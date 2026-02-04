use std::collections::HashMap;
use rusqlite::{params, Connection};
use crate::models::DbMessage;

pub fn process_sqlite_batch(
    conn: &Connection,
    batch: &mut Vec<DbMessage>,
    bundle_cache: &mut HashMap<String, i64>,
    page_title_cache: &mut HashMap<u64, String>,
    domain_id: Option<i64>,
    domain_label: Option<&str>,
) {
    let tx = conn.unchecked_transaction().expect("Failed to start SQLite transaction");

    for m in batch.drain(..) {
        match m {
            DbMessage::Page { id, ns, title } => {
                if let (Some(did), Some(dlabel)) = (domain_id, domain_label) {
                    let url = format!("https://{}/w/index.php?curid={}", dlabel, id);
                    tx.execute(
                        "INSERT INTO web_resources (url, numeric_page_id, numeric_namespace_id, domain_id, instance_of_document)
                         VALUES (?1, ?2, ?3, ?4, NULL)
                         ON CONFLICT (url) DO UPDATE SET
                            numeric_page_id = EXCLUDED.numeric_page_id,
                            numeric_namespace_id = EXCLUDED.numeric_namespace_id,
                            domain_id = EXCLUDED.domain_id",
                        params![url, id as i64, ns, did],
                    ).ok();
                }
                page_title_cache.insert(id, title);
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
                let b_id = if let Some(&id) = bundle_cache.get(&file_path) {
                    id
                } else {
                    tx.execute(
                        "INSERT OR IGNORE INTO revision_bundles (file_path) VALUES (?1)",
                        params![file_path],
                    ).ok();
                    let id: i64 = tx.query_row(
                        "SELECT id FROM revision_bundles WHERE file_path = ?1",
                        params![file_path],
                        |row| row.get(0),
                    ).expect("Failed to get bundle id");
                    bundle_cache.insert(file_path, id);
                    id
                };

                tx.execute(
                    "INSERT OR IGNORE INTO revisions (revision_id, page_id, found_in_bundle, offset_begin, offset_end, parent_revision_id, revision_timestamp)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    params![rev_id as i64, page_id as i64, b_id, offset_begin as i64, offset_end as i64, parent_rev_id.map(|id| id as i64), timestamp],
                ).ok();
            }
            DbMessage::Finalize => {
                for (page_id, title) in page_title_cache.drain() {
                    tx.execute(
                        "INSERT OR IGNORE INTO documents (title) VALUES (?1)",
                        params![title],
                    ).ok();
                    tx.execute(
                        "UPDATE web_resources 
                         SET instance_of_document = (SELECT id FROM documents WHERE title = ?1)
                         WHERE numeric_page_id = ?2 AND instance_of_document IS NULL",
                        params![title, page_id as i64],
                    ).ok();
                }
            }
            _ => {}
        }
    }
    tx.commit().expect("Failed to commit SQLite batch transaction");
}
