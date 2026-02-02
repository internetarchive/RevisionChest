use bzip2::read::BzDecoder;
use chrono::Local;
use clap::Parser;
use crossbeam_channel::{Receiver, Sender};
use postgres::{Client, NoTls, Transaction};
use quick_xml::events::Event;
use quick_xml::reader::Reader;
use rayon::prelude::*;
use rusqlite::{params, Connection};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::thread;
use url::Url;

fn process_pg_page(tx: &mut Transaction, id: u64, ns: i32, title: String, container_id: i32, entity_id: i32) {
    let domain_label = tx
        .query_one("SELECT label FROM concepts WHERE id = $1", &[&container_id])
        .expect("Failed to get domain label")
        .get::<_, String>(0);
    let page_label = format!("{}:{}", domain_label, id);

    // page_concept_id: label like "{domain}:{page_id}"
    let row = tx.query_opt("SELECT id FROM concepts WHERE label = $1", &[&page_label])
        .expect("Failed to check for page concept");
    let concept_id: i32 = match row {
        Some(r) => r.get(0),
        None => {
            let insert_row = tx.query_one("INSERT INTO concepts (label) VALUES ($1) RETURNING id", &[&page_label])
                .expect("Failed to insert page concept");
            insert_row.get(0)
        }
    };

    tx.execute(
        "INSERT INTO documents (id, title, numeric_page_id, numeric_namespace_id, has_container)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (numeric_page_id, has_container) DO UPDATE SET
           title = EXCLUDED.title,
           numeric_namespace_id = EXCLUDED.numeric_namespace_id",
        &[&concept_id, &title, &(id as i64), &ns, &container_id],
    )
    .expect("Failed to upsert document");

    // Fetch the canonical concept id for this document (it may be the existing row’s id)
    let doc_concept_id: i32 = tx
        .query_one(
            "SELECT id FROM documents WHERE numeric_page_id = $1 AND has_container = $2",
            &[&(id as i64), &container_id],
        )
        .expect("Failed to fetch canonical document concept id")
        .get(0);

    // web_resource_concept_id: label like "{domain}:{page_id} (on web)"
    let web_label = format!("{} (on web)", page_label);
    let row = tx.query_opt("SELECT id FROM concepts WHERE label = $1", &[&web_label])
        .expect("Failed to check for web resource concept");
    let web_concept_id: i32 = match row {
        Some(r) => r.get(0),
        None => {
            let insert_row = tx.query_one("INSERT INTO concepts (label) VALUES ($1) RETURNING id", &[&web_label])
                .expect("Failed to insert web resource concept");
            insert_row.get(0)
        }
    };
    let web_url = format!("https://{}/w/index.php?curid={}", domain_label, id);

    tx.execute(
        "INSERT INTO web_resources (id, url, instance_of_document, domain, is_archive_of)
         VALUES ($1, $2, $3, $4, NULL)
         ON CONFLICT (url) DO UPDATE SET
           instance_of_document = EXCLUDED.instance_of_document,
           domain = EXCLUDED.domain",
        &[&web_concept_id, &web_url, &doc_concept_id, &entity_id],
    )
    .expect("Failed to upsert web resource");
}

fn process_pg_revision(
    tx: &mut Transaction,
    rev_id: u64,
    parent_rev_id: Option<u64>,
    page_id: u64,
    file_path: String,
    offset_begin: u64,
    offset_end: u64,
    timestamp: String,
    bundle_cache: &mut HashMap<String, i32>,
) {
    let bundle_id = if let Some(&id) = bundle_cache.get(&file_path) {
        id
    } else {
        let id: i32 = tx.query_one(
            "INSERT INTO revision_bundles (file_path)
             VALUES ($1)
             ON CONFLICT (file_path) DO UPDATE SET file_path = EXCLUDED.file_path
             RETURNING id",
            &[&file_path],
        )
        .expect("Failed to upsert revision bundle")
        .get(0);
        bundle_cache.insert(file_path, id);
        id
    };

    tx.execute(
        "INSERT INTO revisions (
          revision_id, page_id, found_in_bundle, offset_begin, offset_end, parent_revision_id, revision_timestamp
        ) VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (revision_id) DO UPDATE SET
          page_id = EXCLUDED.page_id,
          found_in_bundle = EXCLUDED.found_in_bundle,
          offset_begin = EXCLUDED.offset_begin,
          offset_end = EXCLUDED.offset_end,
          parent_revision_id = EXCLUDED.parent_revision_id,
          revision_timestamp = EXCLUDED.revision_timestamp",
        &[
            &(rev_id as i64),
            &(page_id as i64),
            &bundle_id,
            &(offset_begin as i64),
            &(offset_end as i64),
            &parent_rev_id.map(|id| id as i64),
            &timestamp,
        ],
    )
    .expect("Failed to upsert revision");
}

#[derive(Debug)]
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

fn db_worker(rx: Receiver<DbMessage>, db_path: PathBuf) {
    dotenvy::dotenv().ok();

    let mut pg_client = if std::env::var("DATABASE").unwrap_or_default() == "postgres" {
        let host = std::env::var("DB_HOST").expect("DB_HOST not set");
        let port = std::env::var("DB_PORT").expect("DB_PORT not set");
        let name = std::env::var("DB_NAME").expect("DB_NAME not set");
        let user = std::env::var("DB_USER").expect("DB_USER not set");
        let pass = std::env::var("DB_PASS").expect("DB_PASS not set");

        let conn_str = format!("postgresql://{}:{}@{}:{}/{}", user, pass, host, port, name);
        let mut client = Client::connect(&conn_str, NoTls).expect("Failed to connect to PostgreSQL");

        // Verify that tables exist instead of creating them
        let tables = ["concepts", "documents", "revision_bundles", "revisions", "web_resources", "domains"];
        for table in &tables {
            let row_exists: bool = client
                .query_one(
                    "SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = $1
                    )",
                    &[table],
                )
                .expect("Failed to check for table existence")
                .get(0);

            if !row_exists {
                panic!("Required table '{}' is missing in PostgreSQL database", table);
            }
        }

        Some(client)
    } else {
        None
    };

    let mut sqlite_conn = if pg_client.is_none() {
        let conn = Connection::open(db_path).expect("Failed to open SQLite database");
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
        Some(conn)
    } else {
        None
    };

    let mut domain_container_concept_id = None;
    let mut domain_entity_concept_id = None;
    let mut bundle_cache: HashMap<String, i32> = HashMap::new();
    let mut file_cache: HashMap<String, i64> = HashMap::new();
    let mut pending_messages: Vec<DbMessage> = Vec::new();

    let mut batch = Vec::with_capacity(10000);
    while let Ok(msg) = rx.recv() {
        batch.push(msg);
        if batch.len() >= 10000 || (rx.is_empty() && !batch.is_empty()) {
            if let Some(ref mut client) = pg_client {
                let mut tx = client.transaction().expect("Failed to start PG transaction");
                let current_batch: Vec<DbMessage> = batch.drain(..).collect();
                for m in current_batch {
                    match m {
                        DbMessage::SiteInfo { domain } => {
                            // domain_container_concept_id: label like "{domain}"
                            let row = tx.query_opt("SELECT id FROM concepts WHERE label = $1", &[&domain])
                                .expect("Failed to check for domain container concept");
                            let container_id: i32 = match row {
                                Some(r) => r.get(0),
                                None => {
                                    let insert_row = tx.query_one("INSERT INTO concepts (label) VALUES ($1) RETURNING id", &[&domain])
                                        .expect("Failed to insert domain container concept");
                                    insert_row.get(0)
                                }
                            };
                            domain_container_concept_id = Some(container_id);

                            // domain_entity_concept_id: label like "{domain} (domain)"
                            let entity_label = format!("{} (domain)", domain);
                            let row = tx.query_opt("SELECT id FROM concepts WHERE label = $1", &[&entity_label])
                                .expect("Failed to check for domain entity concept");
                            let entity_id: i32 = match row {
                                Some(r) => r.get(0),
                                None => {
                                    let insert_row = tx.query_one("INSERT INTO concepts (label) VALUES ($1) RETURNING id", &[&entity_label])
                                        .expect("Failed to insert domain entity concept");
                                    insert_row.get(0)
                                }
                            };
                            domain_entity_concept_id = Some(entity_id);

                            // Upsert domains row
                            tx.execute(
                                "INSERT INTO domains (id, value, top_level_domain, parent_domain, for_container)
                                 VALUES ($1, $2, $3, $4, $5)
                                 ON CONFLICT (value) DO UPDATE SET
                                   id = EXCLUDED.id,
                                   top_level_domain = COALESCE(EXCLUDED.top_level_domain, domains.top_level_domain),
                                   parent_domain = COALESCE(EXCLUDED.parent_domain, domains.parent_domain),
                                   for_container = COALESCE(EXCLUDED.for_container, domains.for_container)",
                                &[&entity_id, &domain, &Option::<String>::None, &Option::<i32>::None, &container_id],
                            ).expect("Failed to upsert domain");

                            // Process pending messages now that we have siteinfo
                            let pending = std::mem::take(&mut pending_messages);
                            for pm in pending {
                                match pm {
                                    DbMessage::Page { id, ns, title } => {
                                        process_pg_page(&mut tx, id, ns, title, domain_container_concept_id.unwrap(), domain_entity_concept_id.unwrap());
                                    }
                                    DbMessage::Revision { rev_id, parent_rev_id, page_id, file_path, offset_begin, offset_end, timestamp } => {
                                        process_pg_revision(&mut tx, rev_id, parent_rev_id, page_id, file_path, offset_begin, offset_end, timestamp, &mut bundle_cache);
                                    }
                                    _ => {}
                                }
                            }
                        }
                        DbMessage::Page { id, ns, title } => {
                            if let (Some(container_id), Some(entity_id)) = (domain_container_concept_id, domain_entity_concept_id) {
                                process_pg_page(&mut tx, id, ns, title, container_id, entity_id);
                            } else {
                                pending_messages.push(DbMessage::Page { id, ns, title });
                            }
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
                            if domain_container_concept_id.is_some() {
                                process_pg_revision(&mut tx, rev_id, parent_rev_id, page_id, file_path, offset_begin, offset_end, timestamp, &mut bundle_cache);
                            } else {
                                pending_messages.push(DbMessage::Revision {
                                    rev_id,
                                    parent_rev_id,
                                    page_id,
                                    file_path,
                                    offset_begin,
                                    offset_end,
                                    timestamp,
                                });
                            }
                        }
                    }
                }
                tx.commit().expect("Failed to commit PG transaction");
            } else if let Some(ref mut conn) = sqlite_conn {
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
}

fn process_file(
    input_path: &Path,
    output_dir: Option<&Path>,
    allowed_namespaces: &Option<Vec<String>>,
    db_tx: &Sender<DbMessage>,
) -> io::Result<()> {
    let filename = input_path.file_name().unwrap().to_string_lossy();
    eprintln!("[{}] Starting {}", Local::now().format("%Y-%m-%d %H:%M:%S"), filename);

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
                        if in_siteinfo {
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

    eprintln!("[{}] Finished {}", Local::now().format("%Y-%m-%d %H:%M:%S"), filename);
    Ok(())
}

fn main() -> io::Result<()> {
    let args = Args::parse();
    let (db_tx, db_rx) = crossbeam_channel::unbounded();
    let mut db_path = args.db.clone();

    if let Some(ref output_dir) = args.output_dir {
        if db_path.is_relative() {
            db_path = output_dir.join(db_path);
        }
    }

    let db_thread = thread::spawn(move || {
        db_worker(db_rx, db_path);
    });

    if let Some(input_dir) = args.input_dir {
        let output_dir = args.output_dir.as_deref().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "-o is required when using -d")
        })?;

        let entries: Vec<PathBuf> = fs::read_dir(input_dir)?
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .filter(|path| {
                path.is_file() && (path.extension() == Some("bz2".as_ref()) || path.extension() == Some("7z".as_ref()))
            })
            .collect();

        entries.into_par_iter().for_each(|path| {
            if let Err(e) = process_file(&path, Some(output_dir), &args.namespace, &db_tx) {
                eprintln!("Error processing {:?}: {}", path, e);
            }
        });
    } else if let Some(input_path) = args.input {
        process_file(
            &input_path,
            args.output_dir.as_deref(),
            &args.namespace,
            &db_tx,
        )?;
    } else {
        eprintln!("Usage: RevisionChest <wikipedia_dump.xml.bz2|7z> OR -d <input_dir> -o <output_dir>");
        std::process::exit(1);
    }

    drop(db_tx);
    db_thread.join().expect("Database thread panicked");

    Ok(())
}
