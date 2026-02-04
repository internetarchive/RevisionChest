use std::collections::HashMap;
use std::io::Write;
use std::sync::{Arc, RwLock};
use std::thread;
use r2d2_postgres::PostgresConnectionManager;
use crate::models::DbMessage;
use crate::utils::sanitize_copy_text;

pub fn process_pg_batch(
    pool: &r2d2::Pool<PostgresConnectionManager<postgres::NoTls>>,
    batch: Vec<DbMessage>,
    domain_id_atomic: &Arc<std::sync::atomic::AtomicI32>,
    domain_label_cached: &Arc<RwLock<Option<String>>>,
    bundle_cache: &Arc<RwLock<HashMap<String, i32>>>,
    page_title_cache: &Arc<RwLock<HashMap<u64, String>>>,
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

        // Sort batch to avoid deadlocks in revision_bundles upserts
        let mut sorted_batch = batch.clone();
        sorted_batch.sort_by(|a, b| {
            match (a, b) {
                (DbMessage::Revision { file_path: f1, .. }, DbMessage::Revision { file_path: f2, .. }) => f1.cmp(f2),
                (DbMessage::Page { .. }, DbMessage::Revision { .. }) => std::cmp::Ordering::Less,
                (DbMessage::Revision { .. }, DbMessage::Page { .. }) => std::cmp::Ordering::Greater,
                _ => std::cmp::Ordering::Equal,
            }
        });

        let mut rev_data = Vec::new();
        let mut page_data = Vec::new();
        let mut seen_pages = std::collections::HashSet::new();

        tx.execute(
            "CREATE TEMP TABLE temp_pages (
                id INTEGER,
                ns INTEGER,
                title TEXT
            ) ON COMMIT DROP",
            &[],
        ).expect("Failed to create temp_pages");

        let mut success = true;
        for msg in &sorted_batch {
            match msg {
                DbMessage::Page { id, ns, title } => {
                    if seen_pages.insert(*id) {
                        let row = format!("{}\t{}\t{}\n", *id as i32, ns, sanitize_copy_text(title));
                        page_data.extend_from_slice(row.as_bytes());
                    }
                    {
                        let mut lock = page_title_cache.write().unwrap();
                        lock.insert(*id, title.clone());
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
                    let cached_id = {
                        let lock = bundle_cache.read().unwrap();
                        lock.get(file_path).copied()
                    };

                    let bundle_id = if let Some(id) = cached_id {
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
                                {
                                    let mut lock = bundle_cache.write().unwrap();
                                    lock.insert(file_path.clone(), id);
                                }
                                id
                            }
                            Err(e) => {
                                if let Some(db_err) = e.as_db_error() {
                                    if db_err.code() == &postgres::error::SqlState::T_R_SERIALIZATION_FAILURE || 
                                       db_err.code() == &postgres::error::SqlState::T_R_DEADLOCK_DETECTED {
                                        // Silent retry for concurrency issues
                                        success = false;
                                        break;
                                    }
                                }
                                eprintln!("Error upserting bundle (file_path: {}): {:?}. Retrying...", file_path, e);
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
                        sanitize_copy_text(timestamp)
                    );
                    rev_data.extend_from_slice(row.as_bytes());
                }
                DbMessage::Finalize => {
                    let cached = {
                        let mut lock = page_title_cache.write().unwrap();
                        std::mem::take(&mut *lock)
                    };

                    if !cached.is_empty() {
                        let mut finalize_page_data = Vec::new();
                        for (id, title) in &cached {
                            let row = format!("{}\t{}\n", *id as i32, sanitize_copy_text(title));
                            finalize_page_data.extend_from_slice(row.as_bytes());
                        }

                        let finalize_res = (|| {
                            tx.execute(
                                "CREATE TEMP TABLE temp_finalize_pages (
                                    id INTEGER,
                                    title TEXT
                                ) ON COMMIT DROP",
                                &[],
                            )?;

                            let mut writer = tx.copy_in("COPY temp_finalize_pages (id, title) FROM STDIN")?;
                            writer.write_all(&finalize_page_data)?;
                            writer.finish()?;

                            tx.execute(
                                "INSERT INTO documents (title)
                                 SELECT DISTINCT title FROM temp_finalize_pages
                                 ON CONFLICT DO NOTHING",
                                &[],
                            )?;

                            tx.execute(
                                "UPDATE web_resources
                                 SET instance_of_document = d.id
                                 FROM temp_finalize_pages tfp
                                 JOIN documents d ON d.title = tfp.title
                                 WHERE web_resources.numeric_page_id = tfp.id 
                                   AND web_resources.instance_of_document IS NULL",
                                &[],
                            )?;

                            Ok::<(), Box<dyn std::error::Error>>(())
                        })();

                        if let Err(e) = finalize_res {
                            eprintln!("Finalize pages failed: {:?}. Retrying...", e);
                            success = false;
                            break;
                        }
                    }
                }
                _ => {}
            }
        }

        if !success {
            retry_count += 1;
            thread::sleep(std::time::Duration::from_millis(100 * retry_count));
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
                         SELECT 'https://' || $1 || '/w/index.php?curid=' || tp.id, tp.id, tp.ns, $2, NULL
                         FROM temp_pages tp
                         ON CONFLICT (url) DO UPDATE SET
                            numeric_page_id = EXCLUDED.numeric_page_id,
                            numeric_namespace_id = EXCLUDED.numeric_namespace_id,
                            domain_id = EXCLUDED.domain_id"
                    ),
                    &[&domain_label, &domain_id],
                )?;

                Ok::<(), Box<dyn std::error::Error>>(())
            })();

            if let Err(e) = page_res {
                eprintln!("COPY pages failed: {:?}. Retrying...", e);
                retry_count += 1;
                thread::sleep(std::time::Duration::from_millis(100 * retry_count));
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
                eprintln!("COPY revisions failed: {:?}. Retrying...", e);
                retry_count += 1;
                thread::sleep(std::time::Duration::from_millis(100 * retry_count));
                continue;
            }
        }

        match tx.commit() {
            Ok(_) => {
                return;
            }
            Err(e) => {
                eprintln!("Transaction commit failed: {:?}. Retrying...", e);
                retry_count += 1;
                thread::sleep(std::time::Duration::from_millis(100 * retry_count));
            }
        }
    }
    panic!("Failed to process batch after {} retries", max_retries);
}
