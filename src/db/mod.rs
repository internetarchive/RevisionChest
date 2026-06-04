pub mod postgres;
pub mod sqlite;

use std::collections::HashMap;
use std::path::{PathBuf};
use std::sync::{Arc, RwLock};
use std::thread;
use crossbeam_channel::{Receiver, Sender};
use r2d2_postgres::PostgresConnectionManager;
use rusqlite::{params, Connection, OptionalExtension};
use crate::models::DbMessage;
use self::postgres::process_pg_batch;
use self::sqlite::process_sqlite_batch;
use ::postgres::NoTls;

pub fn db_worker(rx: Receiver<DbMessage>, db_path: PathBuf) {
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
                "containers", "documents", "web_resources", "domains", "revision_bundles", "revisions"
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
        let language_code_cached = Arc::new(RwLock::new(None));
        let bundle_cache: Arc<RwLock<HashMap<String, i32>>> = Arc::new(RwLock::new(HashMap::new()));
        let page_title_cache: Arc<RwLock<HashMap<u64, String>>> = Arc::new(RwLock::new(HashMap::new()));

        let mut pending_messages: Vec<DbMessage> = Vec::new();
        let mut sub_worker_txs: Vec<Sender<Vec<DbMessage>>> = Vec::new();
        let mut sub_worker_handles = Vec::new();

        for _ in 0..4 {
            let (tx, sub_rx) = crossbeam_channel::bounded::<Vec<DbMessage>>(10);
            let pool_clone = pool.clone();
            let domain_id_clone = domain_id.clone();
            let domain_label_clone = domain_label_cached.clone();
            let language_code_clone = language_code_cached.clone();
            let bundle_cache_clone = bundle_cache.clone();
            let page_title_cache_clone = page_title_cache.clone();

            let handle = thread::spawn(move || {
                while let Ok(batch) = sub_rx.recv() {
                    process_pg_batch(&pool_clone, batch, &domain_id_clone, &domain_label_clone, &language_code_clone, &bundle_cache_clone, &page_title_cache_clone);
                }
            });
            sub_worker_txs.push(tx);
            sub_worker_handles.push(handle);
        }

        let mut current_sub_worker = 0;
        let mut batch = Vec::with_capacity(10000);

        while let Ok(msg) = rx.recv() {
            match msg {
                DbMessage::SiteInfo { domain, language_code } => {
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
                    if let Some(lc) = language_code {
                        let mut lock = language_code_cached.write().unwrap();
                        *lock = Some(lc);
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

    // SQLite path
    let mut conn = Connection::open(db_path).expect("Failed to open SQLite database");
    conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA synchronous = NORMAL;
         CREATE TABLE IF NOT EXISTS containers (
             id INTEGER PRIMARY KEY AUTOINCREMENT,
             label TEXT UNIQUE,
             wikidata_id INTEGER UNIQUE,
             librarybase_id INTEGER UNIQUE
         );
         CREATE TABLE IF NOT EXISTS domains (
             id INTEGER PRIMARY KEY AUTOINCREMENT,
             value TEXT UNIQUE,
             top_level_domain TEXT,
             parent_domain INTEGER,
             for_container INTEGER,
             internet_domains_id INTEGER UNIQUE,
             FOREIGN KEY(parent_domain) REFERENCES domains(id),
             FOREIGN KEY(for_container) REFERENCES containers(id)
         );
         CREATE TABLE IF NOT EXISTS documents (
             id INTEGER PRIMARY KEY AUTOINCREMENT,
             language_code TEXT,
             has_container INTEGER,
             part_of_larger_work INTEGER,
             title TEXT,
             wikidata_id INTEGER UNIQUE,
             librarybase_id INTEGER UNIQUE,
             FOREIGN KEY(has_container) REFERENCES containers(id),
             FOREIGN KEY(part_of_larger_work) REFERENCES documents(id)
         );
         CREATE TABLE IF NOT EXISTS web_resources (
             id INTEGER PRIMARY KEY AUTOINCREMENT,
             url TEXT,
             url_hash CHAR(32) NOT NULL UNIQUE,
             instance_of_document INTEGER,
             availability_status INTEGER,
             is_archive_of INTEGER,
             domain_id INTEGER,
             numeric_page_id INTEGER,
             numeric_namespace_id INTEGER,
             FOREIGN KEY(instance_of_document) REFERENCES documents(id),
             FOREIGN KEY(is_archive_of) REFERENCES web_resources(id),
             FOREIGN KEY(domain_id) REFERENCES domains(id)
         );
         CREATE TABLE IF NOT EXISTS revision_bundles (
             id INTEGER PRIMARY KEY AUTOINCREMENT,
             file_path TEXT UNIQUE
         );
         CREATE TABLE IF NOT EXISTS revisions (
             revision_id INTEGER PRIMARY KEY,
             page_id INTEGER,
             found_in_bundle INTEGER,
             offset_begin INTEGER,
             length INTEGER,
             parent_revision_id INTEGER,
             revision_timestamp TEXT,
             FOREIGN KEY(found_in_bundle) REFERENCES revision_bundles(id)
         );",
    )
    .expect("Failed to initialize SQLite database");

    let mut bundle_cache: HashMap<String, i64> = HashMap::new();
    let mut page_title_cache: HashMap<u64, String> = HashMap::new();
    let mut domain_id: Option<i64> = None;
    let mut domain_label: Option<String> = None;
    let mut language_code: Option<String> = None;
    let mut batch = Vec::with_capacity(10000);
    let mut pending_messages: Vec<DbMessage> = Vec::new();

    while let Ok(msg) = rx.recv() {
        match msg {
            DbMessage::SiteInfo { domain, language_code: lang } => {
                let tx = conn.transaction().expect("Failed to start SQLite transaction");
                tx.execute(
                    "INSERT OR IGNORE INTO domains (value) VALUES (?1)",
                    params![domain],
                ).expect("Failed to insert domain");

                let id: i64 = tx.query_row(
                    "SELECT id FROM domains WHERE value = ?1",
                    params![domain],
                    |row| row.get(0),
                ).expect("Failed to fetch domain id");

                domain_id = Some(id);
                domain_label = Some(domain);
                if lang.is_some() {
                    language_code = lang;
                }
                tx.commit().expect("Failed to commit SiteInfo transaction");

                // Process pending messages
                let pending = std::mem::take(&mut pending_messages);
                for m in pending {
                    batch.push(m);
                    if batch.len() >= 10000 {
                        process_sqlite_batch(&conn, &mut batch, &mut bundle_cache, &mut page_title_cache, domain_id, domain_label.as_deref(), language_code.as_deref());
                    }
                }
            }
            _ => {
                if domain_id.is_none() {
                    pending_messages.push(msg);
                } else {
                    batch.push(msg);
                    if batch.len() >= 10000 {
                        process_sqlite_batch(&conn, &mut batch, &mut bundle_cache, &mut page_title_cache, domain_id, domain_label.as_deref(), language_code.as_deref());
                    }
                }
            }
        }
    }

    if !batch.is_empty() {
        process_sqlite_batch(&conn, &mut batch, &mut bundle_cache, &mut page_title_cache, domain_id, domain_label.as_deref(), language_code.as_deref());
    }
}

pub fn get_latest_timestamp(db_path: PathBuf) -> Option<String> {
    dotenvy::dotenv().ok();

    if std::env::var("DATABASE").unwrap_or_default() == "postgres" {
        let host = std::env::var("DB_HOST").expect("DB_HOST not set");
        let port = std::env::var("DB_PORT").expect("DB_PORT not set");
        let name = std::env::var("DB_NAME").expect("DB_NAME not set");
        let user = std::env::var("DB_USER").expect("DB_USER not set");
        let pass = std::env::var("DB_PASS").expect("DB_PASS not set");

        let conn_str = format!("postgresql://{}:{}@{}:{}/{}", user, pass, host, port, name);
        let mut client = ::postgres::Client::connect(&conn_str, NoTls).ok()?;
        
        let row = client.query_one("SELECT MAX(revision_timestamp) FROM revisions", &[]).ok()?;
        let ts: Option<String> = row.get(0);
        return ts;
    }

    let conn = Connection::open(db_path).ok()?;
    let ts: Option<String> = conn.query_row(
        "SELECT MAX(revision_timestamp) FROM revisions",
        [],
        |row| row.get(0),
    ).optional().ok().flatten();
    
    ts
}

pub fn filter_existing_revisions(db_path: PathBuf, rev_ids: Vec<u64>) -> Vec<u64> {
    if rev_ids.is_empty() {
        return Vec::new();
    }

    dotenvy::dotenv().ok();

    if std::env::var("DATABASE").unwrap_or_default() == "postgres" {
        let host = std::env::var("DB_HOST").expect("DB_HOST not set");
        let port = std::env::var("DB_PORT").expect("DB_PORT not set");
        let name = std::env::var("DB_NAME").expect("DB_NAME not set");
        let user = std::env::var("DB_USER").expect("DB_USER not set");
        let pass = std::env::var("DB_PASS").expect("DB_PASS not set");

        let conn_str = format!("postgresql://{}:{}@{}:{}/{}", user, pass, host, port, name);
        let mut client = match ::postgres::Client::connect(&conn_str, NoTls) {
            Ok(c) => c,
            Err(_) => return rev_ids,
        };

        let ids_i64: Vec<i64> = rev_ids.iter().map(|&id| id as i64).collect();
        let rows = match client.query(
            "SELECT revision_id FROM revisions WHERE revision_id = ANY($1) AND found_in_bundle IS NOT NULL",
            &[&ids_i64],
        ) {
            Ok(r) => r,
            Err(_) => return rev_ids,
        };

        let existing_ids: std::collections::HashSet<u64> = rows.iter().map(|r| r.get::<_, i64>(0) as u64).collect();
        return rev_ids.into_iter().filter(|id| !existing_ids.contains(id)).collect();
    }

    let conn = match Connection::open(db_path) {
        Ok(c) => c,
        Err(_) => return rev_ids,
    };

    let mut existing_ids = std::collections::HashSet::new();
    
    // Process in chunks to avoid SQLite parameter limits if rev_ids is huge, 
    // though here it's likely 500 at most from recentchanges.
    for chunk in rev_ids.chunks(500) {
        let placeholders: String = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let query = format!("SELECT revision_id FROM revisions WHERE revision_id IN ({}) AND found_in_bundle IS NOT NULL", placeholders);
        let params_vec: Vec<i64> = chunk.iter().map(|&id| id as i64).collect();
        let mut stmt = match conn.prepare(&query) {
            Ok(s) => s,
            Err(_) => continue,
        };
        let rows = match stmt.query_map(rusqlite::params_from_iter(params_vec), |row| row.get::<_, i64>(0)) {
            Ok(r) => r,
            Err(_) => continue,
        };
        for id_res in rows {
            if let Ok(id) = id_res {
                existing_ids.insert(id as u64);
            }
        }
    }

    rev_ids.into_iter().filter(|id| !existing_ids.contains(id)).collect()
}
