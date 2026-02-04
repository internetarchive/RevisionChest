use bzip2::read::BzDecoder;
use chrono::Local;
use clap::Parser;
use crossbeam_channel::{Receiver, Sender};
use postgres::{NoTls};
use r2d2_postgres::PostgresConnectionManager;
use quick_xml::events::Event;
use quick_xml::reader::Reader;
use rayon::prelude::*;
use rusqlite::{params, Connection};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::sync::{Arc, RwLock};
use url::Url;

#[derive(Debug, Clone)]
enum DbMessage {
    SiteInfo {
        domain: String,
    },
    Page {
        id: u64,
        ns: i32,
        title: String,
    },
    Revision {
        rev_id: u64,
        parent_rev_id: Option<u64>,
        page_id: u64,
        file_path: String,
        offset_begin: u64,
        offset_end: u64,
        timestamp: String,
    },
}

fn process_pg_batch(
    pool: &r2d2::Pool<PostgresConnectionManager<NoTls>>,
    batch: Vec<DbMessage>,
    domain_id_atomic: &Arc<std::sync::atomic::AtomicI32>,
    domain_label_cached: &Arc<RwLock<Option<String>>>,
) {
    let mut retry_count = 0;
    let max_retries = 5;

    while retry_count < max_retries {
        let mut client = pool.get().expect("Failed to get connection from pool");
        let mut tx = client.transaction().expect("Failed to start transaction");

        let domain_id = domain_id_atomic.load(std::sync::atomic::Ordering::SeqCst);

        if domain_id == 0 {
            return;
        }

        let domain_label = {
            let lock = domain_label_cached.read().unwrap();
            lock.clone().expect("Domain label should be cached by now")
        };

        let mut rev_data = Vec::new();
        let mut page_data = Vec::new();
        let mut bundle_cache: HashMap<String, i32> = HashMap::new();

        tx.execute(
            "CREATE TEMP TABLE temp_pages (
                id INTEGER,
                ns INTEGER,
                title TEXT
            ) ON COMMIT DROP",
            &[],
        ).expect("Failed to create temp_pages");

        let mut success = true;
        for msg in &batch {
            match msg {
                DbMessage::Page { id, ns, title } => {
                    let row = format!("{}\t{}\t{}\n", id, ns, title);
                    page_data.extend_from_slice(row.as_bytes());
                }
                DbMessage::Revision {
                    rev_id,
                    parent_rev_id,
                    page_id,
                    file_path,
                    offset_begin,
                    offset_end,
                    timestamp,
                } => {
                    let bundle_id = if let Some(&id) = bundle_cache.get(file_path) {
                        id
                    } else {
                        let bundle_id_res = tx.query_one(
                            "INSERT INTO revision_bundles (file_path)
                             VALUES ($1)
                             ON CONFLICT (file_path) DO UPDATE SET file_path = EXCLUDED.file_path
                             RETURNING id",
                            &[file_path],
                        );

                        match bundle_id_res {
                            Ok(row) => {
                                let id: i32 = row.get(0);
                                bundle_cache.insert(file_path.clone(), id);
                                id
                            }
                            Err(e) => {
                                eprintln!("Error upserting bundle: {}. Retrying...", e);
                                success = false;
                                break;
                            }
                        }
                    };

                    let parent_id = parent_rev_id.map(|id| id as i64);
                    let row = format!(
                        "{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                        *rev_id as i64,
                        *page_id as i32,
                        bundle_id,
                        *offset_begin as i32,
                        *offset_end as i32,
                        parent_id.map(|id| id.to_string()).unwrap_or_else(|| "\\N".to_string()),
                        timestamp
                    );
                    rev_data.extend_from_slice(row.as_bytes());
                }
                _ => {}
            }
        }

        if !success {
            retry_count += 1;
            std::thread::sleep(std::time::Duration::from_millis(100 * retry_count));
            continue;
        }

        if !page_data.is_empty() {
            let page_res = (|| {
                let mut writer = tx.copy_in("COPY temp_pages (id, ns, title) FROM STDIN")?;
                writer.write_all(&page_data)?;
                writer.finish()?;

                tx.execute(
                    "INSERT INTO documents (title)
                     SELECT title FROM temp_pages
                     ON CONFLICT DO NOTHING",
                    &[],
                )?;

                tx.execute(
                    &format!(
                        "INSERT INTO web_resources (url, numeric_page_id, numeric_namespace_id, domain_id, instance_of_document)
                         SELECT 'https://' || $1 || '/w/index.php?curid=' || id, id, ns, $2, NULL
                         FROM temp_pages
                         ON CONFLICT (url) DO UPDATE SET
                            numeric_page_id = EXCLUDED.numeric_page_id,
                            numeric_namespace_id = EXCLUDED.numeric_namespace_id,
                            domain_id = EXCLUDED.domain_id"
                    ),
                    &[&domain_label, &domain_id],
                )?;

                tx.execute(
                    "UPDATE web_resources wr
                     SET instance_of_document = d.id
                     FROM documents d
                     JOIN temp_pages tp ON d.title = tp.title
                     WHERE wr.numeric_page_id = tp.id
                     AND wr.domain_id = $1
                     AND wr.instance_of_document IS NULL",
                    &[&domain_id],
                )?;

                Ok::<(), Box<dyn std::error::Error>>(())
            })();

            if let Err(e) = page_res {
                eprintln!("COPY pages failed: {}. Retrying...", e);
                retry_count += 1;
                std::thread::sleep(std::time::Duration::from_millis(100 * retry_count));
                continue;
            }
        }

        if !rev_data.is_empty() {
            let copy_res = (|| {
                let mut writer = tx.copy_in("COPY revisions (revision_id, page_id, found_in_bundle, offset_begin, offset_end, parent_revision_id, revision_timestamp) FROM STDIN")?;
                writer.write_all(&rev_data)?;
                writer.finish()?;
                Ok::<(), Box<dyn std::error::Error>>(())
            })();

            if let Err(e) = copy_res {
                eprintln!("COPY revisions failed: {}. Retrying...", e);
                retry_count += 1;
                std::thread::sleep(std::time::Duration::from_millis(100 * retry_count));
                continue;
            }
        }

        match tx.commit() {
            Ok(_) => {
                return;
            }
            Err(e) => {
                eprintln!("Transaction commit failed: {}. Retrying...", e);
                retry_count += 1;
                std::thread::sleep(std::time::Duration::from_millis(100 * retry_count));
            }
        }
    }
    panic!("Failed to process batch after {} retries", max_retries);
}

fn db_worker(rx: Receiver<DbMessage>, db_path: PathBuf) {
    dotenvy::dotenv().ok();

    if std::env::var("DATABASE").unwrap_or_default() == "postgres" {
        let host = std::env::var("DB_HOST").expect("DB_HOST not set");
        let port = std::env::var("DB_PORT").expect("DB_PORT not set");
        let name = std::env::var("DB_NAME").expect("DB_NAME not set");
        let user = std::env::var("DB_USER").expect("DB_USER not set");
        let pass = std::env::var("DB_PASS").expect("DB_PASS not set");

        let conn_str = format!("postgresql://{}:{}@{}:{}/{}", user, pass, host, port, name);
        let manager = PostgresConnectionManager::new(conn_str.parse().unwrap(), NoTls);
        let pool = r2d2::Pool::builder()
            .max_size(8) // Multiple connections for parallel sub-workers
            .build(manager)
            .expect("Failed to create pool");

        // Verify tables
        {
            let mut client = pool.get().expect("Failed to get connection");
            let tables = [
                "documents", "web_resources", "domains", "revision_bundles", "revisions"
            ];
            let rows = client
                .query(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = ANY($1)",
                    &[&&tables[..]],
                )
                .expect("Failed to check for table existence");
            
            let existing_tables: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
            for table in &tables {
                if !existing_tables.contains(&table.to_string()) {
                    panic!("Required table '{}' is missing", table);
                }
            }
        }

        let domain_id = Arc::new(std::sync::atomic::AtomicI32::new(0));
        let domain_label_cached = Arc::new(RwLock::new(None));

        let mut pending_messages: Vec<DbMessage> = Vec::new();
        let mut sub_worker_txs: Vec<Sender<Vec<DbMessage>>> = Vec::new();
        let mut sub_worker_handles = Vec::new();

        for _ in 0..4 {
            let (tx, sub_rx) = crossbeam_channel::bounded::<Vec<DbMessage>>(10);
            let pool_clone = pool.clone();
            let domain_id_clone = domain_id.clone();
            let domain_label_clone = domain_label_cached.clone();

            let handle = thread::spawn(move || {
                while let Ok(batch) = sub_rx.recv() {
                    process_pg_batch(&pool_clone, batch, &domain_id_clone, &domain_label_clone);
                }
            });
            sub_worker_txs.push(tx);
            sub_worker_handles.push(handle);
        }

        let mut current_sub_worker = 0;
        let mut batch = Vec::with_capacity(10000);

        while let Ok(msg) = rx.recv() {
            match msg {
                DbMessage::SiteInfo { domain } => {
                    let mut client = pool.get().expect("Failed to get connection");
                    let mut tx = client.transaction().expect("Failed to start transaction");

                    tx.execute(
                        "INSERT INTO domains (value)
                         VALUES ($1)
                         ON CONFLICT (value) DO NOTHING",
                        &[&domain],
                    ).expect("Failed to insert domain");

                    let domain_id_val: i32 = tx.query_one(
                        "SELECT id FROM domains WHERE value = $1",
                        &[&domain],
                    ).expect("Failed to fetch domain id").get(0);

                    domain_id.store(domain_id_val, std::sync::atomic::Ordering::SeqCst);

                    {
                        let mut lock = domain_label_cached.write().unwrap();
                        *lock = Some(domain.clone());
                    }

                    tx.commit().expect("Failed to commit siteinfo tx");

                    // Process pending
                    let pending = std::mem::take(&mut pending_messages);
                    if !pending.is_empty() {
                        for chunk in pending.chunks(10000) {
                            sub_worker_txs[current_sub_worker].send(chunk.to_vec()).ok();
                            current_sub_worker = (current_sub_worker + 1) % sub_worker_txs.len();
                        }
                    }
                }
                _ => {
                    if domain_id.load(std::sync::atomic::Ordering::SeqCst) == 0 {
                        if pending_messages.len() > 100_000 {
                            panic!("Too many pending messages without SiteInfo");
                        }
                        pending_messages.push(msg);
                    } else {
                        batch.push(msg);
                        if batch.len() >= 10000 {
                            sub_worker_txs[current_sub_worker].send(std::mem::take(&mut batch)).ok();
                            current_sub_worker = (current_sub_worker + 1) % sub_worker_txs.len();
                        }
                    }
                }
            }
        }
        if !batch.is_empty() {
            sub_worker_txs[current_sub_worker].send(batch).ok();
        }

        // Wait for workers to finish
        drop(sub_worker_txs);
        for handle in sub_worker_handles {
            handle.join().ok();
        }
        
        // After workers are done, return
        return;
    }

    // SQLite path (unchanged for now as the focus is PostgreSQL performance)
    let mut conn = Connection::open(db_path).expect("Failed to open SQLite database");
    conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA synchronous = NORMAL;
         CREATE TABLE IF NOT EXISTS pages (
             id INTEGER PRIMARY KEY,
             ns INTEGER,
             title TEXT
         );
         CREATE TABLE IF NOT EXISTS files (
             id INTEGER PRIMARY KEY AUTOINCREMENT,
             path TEXT UNIQUE
         );
         CREATE TABLE IF NOT EXISTS revisions (
             rev_id INTEGER PRIMARY KEY,
             page_id INTEGER,
             file_id INTEGER,
             offset_begin INTEGER,
             offset_end INTEGER
         );",
    )
    .expect("Failed to initialize SQLite database");

    let mut file_cache: HashMap<String, i64> = HashMap::new();
    let mut batch = Vec::with_capacity(10000);

    while let Ok(msg) = rx.recv() {
        batch.push(msg);
        if batch.len() >= 10000 || (rx.is_empty() && !batch.is_empty()) {
            let tx = conn.transaction().expect("Failed to start SQLite transaction");
            for m in batch.drain(..) {
                match m {
                    DbMessage::SiteInfo { .. } => {}
                    DbMessage::Page { id, ns, title } => {
                        tx.execute(
                            "INSERT OR IGNORE INTO pages (id, ns, title) VALUES (?1, ?2, ?3)",
                            params![id, ns, title],
                        )
                        .ok();
                    }
                    DbMessage::Revision {
                        rev_id,
                        page_id,
                        file_path,
                        offset_begin,
                        offset_end,
                        ..
                    } => {
                        let file_id = if let Some(&id) = file_cache.get(&file_path) {
                            id
                        } else {
                            tx.execute(
                                "INSERT OR IGNORE INTO files (path) VALUES (?1)",
                                params![file_path],
                            )
                            .ok();
                            let id: i64 = tx
                                .query_row(
                                    "SELECT id FROM files WHERE path = ?1",
                                    params![file_path],
                                    |row| row.get(0),
                                )
                                .expect("Failed to get file id");
                            file_cache.insert(file_path, id);
                            id
                        };

                        tx.execute(
                            "INSERT OR IGNORE INTO revisions (rev_id, page_id, file_id, offset_begin, offset_end) VALUES (?1, ?2, ?3, ?4, ?5)",
                            params![rev_id, page_id, file_id, offset_begin, offset_end],
                        ).ok();
                    }
                }
            }
            tx.commit().expect("Failed to commit SQLite transaction");
        }
    }
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Wikipedia dump file (.bz2 or .7z)
    input: Option<PathBuf>,

    /// Directory containing Wikipedia dump files
    #[arg(short = 'd')]
    input_dir: Option<PathBuf>,

    /// Output directory for .mwrev.zst files
    #[arg(short = 'o')]
    output_dir: Option<PathBuf>,

    /// Namespaces to include, comma-separated (e.g., --namespace=0,118)
    #[arg(long, value_delimiter = ',')]
    namespace: Option<Vec<String>>,

    /// SQLite database file
    #[arg(long, default_value = "index.db")]
    db: PathBuf,

    /// Override or specify the wiki domain (e.g., en.wikipedia.org)
    #[arg(long)]
    domain: Option<String>,
}

fn process_file(
    input_path: &Path,
    output_dir: Option<&Path>,
    allowed_namespaces: &Option<Vec<String>>,
    db_tx: &Sender<DbMessage>,
    file_index: usize,
    total_files: usize,
    skip_siteinfo: bool,
) -> io::Result<()> {
    let filename = input_path.file_name().unwrap().to_string_lossy();
    eprintln!(
        "[{}] Starting {} ({}/{})",
        Local::now().format("%Y-%m-%d %H:%M:%S"),
        filename,
        file_index,
        total_files
    );

    let input_reader: Box<dyn Read> = if input_path.extension().map_or(false, |ext| ext == "7z") {
        let mut reader = sevenz_rust::SevenZReader::open(input_path, sevenz_rust::Password::empty())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        
        let mut first_file_content = Vec::new();
        reader.for_each_entries(|entry, reader| {
            if entry.is_directory() {
                return Ok(true);
            }
            reader.read_to_end(&mut first_file_content)?;
            Ok(false) // stop after first file
        }).map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        
        Box::new(io::Cursor::new(first_file_content))
    } else {
        let file = File::open(input_path)?;
        Box::new(BzDecoder::new(file))
    };

    let writer: Box<dyn Write> = if let Some(out_dir) = output_dir {
        fs::create_dir_all(out_dir)?;
        let mut out_name = filename.to_string();
        if out_name.ends_with(".bz2") {
            out_name = out_name.strip_suffix(".bz2").unwrap().to_string();
        } else if out_name.ends_with(".7z") {
            out_name = out_name.strip_suffix(".7z").unwrap().to_string();
        }
        out_name.push_str(".mwrev.zst");
        let out_path = out_dir.join(out_name);
        let file = File::create(out_path)?;
        Box::new(zstd::Encoder::new(file, 0)?.auto_finish())
    } else {
        Box::new(io::stdout())
    };

    struct TrackingWriter<W: Write> {
        inner: W,
        offset: u64,
    }
    impl<W: Write> Write for TrackingWriter<W> {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            let n = self.inner.write(buf)?;
            self.offset += n as u64;
            Ok(n)
        }
        fn flush(&mut self) -> io::Result<()> {
            self.inner.flush()
        }
    }
    let mut writer = TrackingWriter {
        inner: writer,
        offset: 0,
    };

    let mut reader = Reader::from_reader(BufReader::new(input_reader));
    reader.config_mut().trim_text(true);

    let mut buf = Vec::new();
    let mut current_page_id = String::new();
    let mut current_ns = String::new();
    let mut current_title = String::new();
    
    let mut current_tag = String::new();
    let mut in_page = false;
    let mut in_revision = false;
    let mut in_contributor = false;
    let mut in_siteinfo = false;

    // Revision metadata
    let mut rev_id = String::new();
    let mut parent_rev_id = String::new();
    let mut timestamp = String::new();
    let mut user_id = String::new();
    let mut text = String::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).into_owned();
                match name.as_str() {
                    "siteinfo" => in_siteinfo = true,
                    "page" => {
                        in_page = true;
                        current_title.clear();
                    }
                    "revision" => {
                        if let Some(allowed) = allowed_namespaces {
                            if !allowed.contains(&current_ns) {
                                in_revision = false;
                            } else {
                                in_revision = true;
                            }
                        } else {
                            in_revision = true;
                        }

                        if in_revision {
                            if !current_page_id.is_empty() {
                                db_tx
                                    .send(DbMessage::Page {
                                        id: current_page_id.parse().unwrap_or(0),
                                        ns: current_ns.parse().unwrap_or(0),
                                        title: current_title.clone(),
                                    })
                                    .ok();
                            }
                            rev_id.clear();
                            parent_rev_id.clear();
                            timestamp.clear();
                            user_id.clear();
                            text.clear();
                        }
                    }
                    "contributor" => in_contributor = true,
                    _ => {}
                }
                current_tag = name;
            }
            Ok(Event::End(ref e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).into_owned();
                match name.as_str() {
                    "siteinfo" => in_siteinfo = false,
                    "page" => {
                        in_page = false;
                        current_page_id.clear();
                        current_ns.clear();
                        current_title.clear();
                    }
                    "revision" => {
                        if in_revision {
                            in_revision = false;
                            let display_user_id = if user_id.is_empty() { "0" } else { &user_id };

                            writeln!(
                                writer,
                                "# page_id={} ns={} rev_id={} parent_rev_id={} timestamp={} user_id={}",
                                current_page_id, current_ns, rev_id, parent_rev_id, timestamp, display_user_id
                            )?;
                            
                            let offset_begin = writer.offset;
                            for line in text.lines() {
                                writeln!(writer, " {}", line)?;
                            }
                            let offset_end = writer.offset;

                            let out_name = if output_dir.is_some() {
                                let mut out_name = filename.to_string();
                                if out_name.ends_with(".bz2") {
                                    out_name = out_name.strip_suffix(".bz2").unwrap().to_string();
                                } else if out_name.ends_with(".7z") {
                                    out_name = out_name.strip_suffix(".7z").unwrap().to_string();
                                }
                                out_name.push_str(".mwrev.zst");
                                out_name
                            } else {
                                "stdout".to_string()
                            };

                            db_tx.send(DbMessage::Revision {
                                rev_id: rev_id.parse().unwrap_or(0),
                                parent_rev_id: parent_rev_id.parse().ok(),
                                page_id: current_page_id.parse().unwrap_or(0),
                                file_path: out_name,
                                offset_begin,
                                offset_end,
                                timestamp: timestamp.clone(),
                            }).ok();

                            rev_id.clear();
                            parent_rev_id.clear();
                            timestamp.clear();
                            user_id.clear();
                            text.clear();
                        }
                    }
                    "contributor" => in_contributor = false,
                    _ => {}
                }
                current_tag.clear();
            }
            Ok(Event::Text(ref e)) => {
                let content = e.unescape().unwrap_or_default().into_owned();
                match current_tag.as_str() {
                    "base" => {
                        if in_siteinfo && !skip_siteinfo {
                            if let Ok(url) = Url::parse(&content) {
                                if let Some(domain) = url.domain() {
                                    db_tx.send(DbMessage::SiteInfo { domain: domain.to_string() }).ok();
                                }
                            }
                        }
                    }
                    "title" => {
                        if in_page {
                            current_title = content;
                        }
                    }
                    "id" => {
                        if in_contributor {
                            user_id = content;
                        } else if in_revision {
                            rev_id = content;
                        } else if in_page {
                            current_page_id = content;
                        }
                    }
                    "ns" => {
                        if in_page {
                            current_ns = content;
                        }
                    }
                    "parentid" => {
                        if in_revision {
                            parent_rev_id = content;
                        }
                    }
                    "timestamp" => {
                        if in_revision {
                            timestamp = content;
                        }
                    }
                    "text" => {
                        if in_revision {
                            text = content;
                        }
                    }
                    _ => {}
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => {
                eprintln!("Error at position {}: {:?}", reader.buffer_position(), e);
                break;
            }
            _ => {}
        }
        buf.clear();
    }

    eprintln!(
        "[{}] Finished {} ({}/{})",
        Local::now().format("%Y-%m-%d %H:%M:%S"),
        filename,
        file_index,
        total_files
    );
    Ok(())
}

fn main() -> io::Result<()> {
    let args = Args::parse();
    let (db_tx, db_rx) = crossbeam_channel::bounded(10000);
    let mut db_path = args.db.clone();

    if let Some(ref output_dir) = args.output_dir {
        if db_path.is_relative() {
            db_path = output_dir.join(db_path);
        }
    }

    let db_thread = thread::spawn(move || {
        db_worker(db_rx, db_path);
    });

    let skip_siteinfo = if let Some(domain) = args.domain.clone() {
        db_tx.send(DbMessage::SiteInfo { domain }).ok();
        true
    } else {
        false
    };

    if let Some(input_dir) = args.input_dir {
        let output_dir = args.output_dir.as_deref().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "-o is required when using -d")
        })?;

        let mut entries: Vec<PathBuf> = fs::read_dir(input_dir)?
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .filter(|path| {
                path.is_file() && (path.extension() == Some("bz2".as_ref()) || path.extension() == Some("7z".as_ref()))
            })
            .collect();

        entries.sort();

        let total_files = entries.len();
        let started_count = std::sync::atomic::AtomicUsize::new(0);

        entries.into_par_iter().for_each(|path| {
            let file_index = started_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            if let Err(e) = process_file(&path, Some(output_dir), &args.namespace, &db_tx, file_index, total_files, skip_siteinfo) {
                eprintln!("Error processing {:?}: {}", path, e);
            }
        });
    } else if let Some(input_path) = args.input {
        process_file(
            &input_path,
            args.output_dir.as_deref(),
            &args.namespace,
            &db_tx,
            1,
            1,
            skip_siteinfo,
        )?;
    } else {
        eprintln!("Usage: RevisionChest <wikipedia_dump.xml.bz2|7z> OR -d <input_dir> -o <output_dir>");
        std::process::exit(1);
    }

    drop(db_tx);
    db_thread.join().expect("Database thread panicked");

    Ok(())
}
