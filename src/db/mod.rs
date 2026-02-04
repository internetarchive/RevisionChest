pub mod postgres;
pub mod sqlite;

use std::collections::HashMap;
use std::path::{PathBuf};
use std::sync::{Arc, RwLock};
use std::thread;
use crossbeam_channel::{Receiver, Sender};
use r2d2_postgres::PostgresConnectionManager;
use rusqlite::{params, Connection};
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

    // SQLite path
    let mut conn = Connection::open(db_path).expect("Failed to open SQLite database");
    conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA synchronous = NORMAL;
         CREATE TABLE IF NOT EXISTS domains (
             id INTEGER PRIMARY KEY AUTOINCREMENT,
             value TEXT UNIQUE
         );
         CREATE TABLE IF NOT EXISTS documents (
             id INTEGER PRIMARY KEY AUTOINCREMENT,
             title TEXT UNIQUE
         );
         CREATE TABLE IF NOT EXISTS web_resources (
             id INTEGER PRIMARY KEY AUTOINCREMENT,
             url TEXT UNIQUE,
             numeric_page_id INTEGER,
             numeric_namespace_id INTEGER,
             domain_id INTEGER,
             instance_of_document INTEGER,
             FOREIGN KEY(domain_id) REFERENCES domains(id),
             FOREIGN KEY(instance_of_document) REFERENCES documents(id)
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
             offset_end INTEGER,
             parent_revision_id INTEGER,
             revision_timestamp TEXT,
             FOREIGN KEY(found_in_bundle) REFERENCES revision_bundles(id)
         );",
    )
    .expect("Failed to initialize SQLite database");

    let mut bundle_cache: HashMap<String, i64> = HashMap::new();
    let mut domain_id: Option<i64> = None;
    let mut domain_label: Option<String> = None;
    let mut batch = Vec::with_capacity(10000);
    let mut pending_messages: Vec<DbMessage> = Vec::new();

    while let Ok(msg) = rx.recv() {
        match msg {
            DbMessage::SiteInfo { domain } => {
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
                tx.commit().expect("Failed to commit SiteInfo transaction");

                // Process pending messages
                let pending = std::mem::take(&mut pending_messages);
                for m in pending {
                    batch.push(m);
                    if batch.len() >= 10000 {
                        process_sqlite_batch(&conn, &mut batch, &mut bundle_cache, domain_id, domain_label.as_deref());
                    }
                }
            }
            _ => {
                if domain_id.is_none() {
                    pending_messages.push(msg);
                } else {
                    batch.push(msg);
                    if batch.len() >= 10000 {
                        process_sqlite_batch(&conn, &mut batch, &mut bundle_cache, domain_id, domain_label.as_deref());
                    }
                }
            }
        }
    }

    if !batch.is_empty() {
        process_sqlite_batch(&conn, &mut batch, &mut bundle_cache, domain_id, domain_label.as_deref());
    }
}
