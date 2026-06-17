use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::Path;
use chrono::{Utc, Duration, Local};
use crossbeam_channel::Sender;
use reqwest::blocking::Client;
use serde_json::Value;
use moka::sync::Cache;
use crate::models::DbMessage;
use crate::args::SyncArgs;
use crate::db::filter_existing_revisions;
use crate::utils::parse_timestamp_utc;

pub fn run_sync(args: SyncArgs, db_tx: Sender<DbMessage>, last_ts_str: Option<String>) -> io::Result<()> {
    let mut db_path = args.db.clone();
    if db_path.is_relative() {
        db_path = args.output_dir.join(db_path);
    }

    let interval_mins = args.interval
        .or_else(|| std::env::var("SYNC_INTERVAL").ok().and_then(|v| v.parse().ok()))
        .unwrap_or(10);

    let mut current_last_ts = if let Some(ts_str) = last_ts_str {
        let now = Utc::now();
        let ts = parse_timestamp_utc(&ts_str)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("Failed to parse timestamp {}: {}", ts_str, e)))?;
        
        if now - ts > Duration::days(30) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("The most recent timestamp in the database ({}) is more than 30 days old. Sync is not possible because Wikipedia's recent changes are only kept for 30 days.", ts_str)
            ));
        }

        ts - Duration::hours(24)
    } else {
        Utc::now() - Duration::hours(24)
    };

    let user_agent = format!("{}/{} ({})", 
        std::env::var("UA_APP_NAME").unwrap_or_else(|_| "RevisionChest".to_string()),
        std::env::var("UA_APP_VERSION").unwrap_or_else(|_| "0.1.0".to_string()),
        std::env::var("UA_CONTACT_INFO").unwrap_or_else(|_| "https://github.com/yourusername/RevisionChest".to_string())
    );

    let client = Client::builder()
        .user_agent(user_agent)
        .build()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

    let rc_url = format!("https://{}/w/api.php", args.domain);
    let namespaces = args.namespace.as_ref().map(|ns| ns.join("|"));

    let processed_cache: Cache<u64, ()> = Cache::builder()
        .time_to_live(std::time::Duration::from_secs(25 * 3600))
        .build();

    loop {
        let mut rc_continue: Option<String> = None;
        let mut batch_start_ts = current_last_ts;

        loop {
            let start_ts_str = batch_start_ts.format("%Y-%m-%dT%H:%M:%SZ").to_string();
            eprintln!(
                "[{}] Polling recent changes from {} (starting from {})",
                Local::now().format("%Y-%m-%d %H:%M:%S"),
                args.domain,
                start_ts_str
            );
            let mut params = vec![
                ("action", "query"),
                ("list", "recentchanges"),
                ("rcstart", &start_ts_str),
                ("rcdir", "newer"),
                ("rclimit", "500"),
                ("rcprop", "ids|timestamp|title|userid|ns"),
                ("format", "json"),
                ("formatversion", "2"),
            ];
            
            if let Some(ref ns) = namespaces {
                params.push(("rcnamespace", ns));
            }

            if let Some(ref c) = rc_continue {
                params.push(("rccontinue", c));
            }

            let resp = client.get(&rc_url)
                .query(&params)
                .send()
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
                .json::<Value>()
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

            let recent_changes = resp["query"]["recentchanges"].as_array();
            
            if let Some(rcs) = recent_changes {
                if rcs.is_empty() {
                    break;
                }

                // Update batch_start_ts to the timestamp of the last item in this batch
                if let Some(last_rc) = rcs.last() {
                    if let Some(last_ts_str) = last_rc["timestamp"].as_str() {
                        if let Ok(last_ts) = parse_timestamp_utc(last_ts_str) {
                            batch_start_ts = last_ts;
                            current_last_ts = batch_start_ts;
                        }
                    }
                }

                let rev_ids: Vec<u64> = rcs.iter()
                    .filter_map(|rc| rc["revid"].as_u64())
                    .collect();

    let rev_ids = if args.no_db {
        rev_ids
    } else {
        filter_existing_revisions(db_path.clone(), rev_ids)
    };

                let rev_ids: Vec<u64> = rev_ids.into_iter()
                    .filter(|id| !processed_cache.contains_key(id))
                    .collect();

                // Store page info for indexing
                for rc in rcs {
                    if let (Some(id), Some(ns), Some(title)) = (rc["pageid"].as_u64(), rc["ns"].as_i64(), rc["title"].as_str()) {
                        let ns_i32 = ns as i32;
                        if let Some(allowed) = &args.namespace {
                            if !allowed.contains(&ns_i32.to_string()) {
                                continue;
                            }
                        }
                        db_tx.send(DbMessage::Page {
                            id,
                            ns: ns_i32,
                            title: title.to_string(),
                        }).ok();
                    }
                }

                for chunk in rev_ids.chunks(50) {
                    eprintln!(
                        "[{}] Fetching content for {} revisions",
                        Local::now().format("%Y-%m-%d %H:%M:%S"),
                        chunk.len()
                    );
                    fetch_and_process_revisions(&client, &rc_url, chunk, &args.output_dir, &db_tx, &args.namespace)?;
                    for &id in chunk {
                        processed_cache.insert(id, ());
                    }
                }
            } else {
                break;
            }

            if let Some(c) = resp["continue"]["rccontinue"].as_str() {
                rc_continue = Some(c.to_string());
            } else {
                break;
            }
        }

        if !args.ongoing {
            break;
        }

        eprintln!(
            "[{}] Sync cycle complete. Sleeping for {} minutes...",
            Local::now().format("%Y-%m-%d %H:%M:%S"),
            interval_mins
        );
        std::thread::sleep(std::time::Duration::from_secs(interval_mins * 60));
    }

    Ok(())
}

fn fetch_and_process_revisions(
    client: &Client,
    api_url: &str,
    rev_ids: &[u64],
    output_dir: &Path,
    db_tx: &Sender<DbMessage>,
    allowed_namespaces: &Option<Vec<String>>,
) -> io::Result<()> {
    if rev_ids.is_empty() {
        return Ok(());
    }
    let ids_str = rev_ids.iter().map(|id| id.to_string()).collect::<Vec<_>>().join("|");
    
    let resp = client.get(api_url)
        .query(&[
            ("action", "query"),
            ("prop", "revisions"),
            ("revids", &ids_str),
            ("rvprop", "ids|timestamp|user|content|userid|flags"),
            ("rvslots", "main"),
            ("format", "json"),
            ("formatversion", "2"),
        ])
        .send()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
        .json::<Value>()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

    let pages = if resp["query"]["pages"].is_array() {
        resp["query"]["pages"].as_array().map(|a| a.iter().collect::<Vec<_>>())
    } else {
        resp["query"]["pages"].as_object().map(|o| o.values().collect::<Vec<_>>())
    };

    if let Some(pages) = pages {
        for page in pages {
            let ns = page["ns"].as_i64().unwrap_or(0) as i32;
            let page_id = page["pageid"].as_u64().unwrap_or(0);
            
            if let Some(allowed) = allowed_namespaces {
                if !allowed.contains(&ns.to_string()) {
                    continue;
                }
            }

            let revisions = page["revisions"].as_array();
            if let Some(revisions) = revisions {
                for rev in revisions {
                    process_single_revision(rev, ns, page_id, output_dir, db_tx)?;
                }
            }
        }
    }

    Ok(())
}

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

fn process_single_revision(
    rev_json: &Value,
    ns: i32,
    page_id: u64,
    output_dir: &Path,
    db_tx: &Sender<DbMessage>
) -> io::Result<()> {
    let rev_id = rev_json["revid"].as_u64().unwrap_or(0);
    let parent_rev_id = rev_json["parentid"].as_u64();
    let timestamp = rev_json["timestamp"].as_str().unwrap_or("");
    let user_id = rev_json["userid"].as_u64().unwrap_or(0);
    let content = rev_json["slots"]["main"]["content"].as_str().unwrap_or("");

    if rev_id == 0 || timestamp.is_empty() {
        return Ok(());
    }

    // Daily rotation
    let date_str = &timestamp[..10]; // YYYY-MM-DD
    let rc_dir = output_dir.join("recentchanges");
    fs::create_dir_all(&rc_dir)?;
    
    let filename = format!("{}.mwrev.zst", date_str);
    let file_path = rc_dir.join(&filename);
    let relative_path = format!("recentchanges/{}", filename);

    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&file_path)?;

    // We need to use a zstd encoder. Since we are appending, it's a bit tricky with zstd 
    // because it's frame-based. Appending a new frame is valid.
    let mut encoder = zstd::Encoder::new(file, 0)?;
    encoder.include_contentsize(false)?;
    
    let mut writer = TrackingWriter {
        inner: encoder,
        offset: 0, // This offset won't be absolute in the file if it already existed
    };

    // To get absolute offset, we need to know the current file size
    let current_file_size = fs::metadata(&file_path)?.len();

    writeln!(
        writer,
        "# page_id={} ns={} rev_id={} parent_rev_id={} timestamp={} user_id={}",
        page_id, ns, rev_id, parent_rev_id.unwrap_or(0), timestamp, user_id
    )?;
    
    let offset_begin = current_file_size + writer.offset;
    for line in content.lines() {
        writeln!(writer, " {}", line)?;
    }
    writer.flush()?;
    let encoder = writer.inner;
    encoder.finish()?;
    
    let length = (fs::metadata(&file_path)?.len()) - current_file_size;

    db_tx.send(DbMessage::Revision {
        rev_id,
        parent_rev_id,
        page_id,
        file_path: relative_path,
        offset_begin,
        length,
        timestamp: timestamp.to_string(),
    }).ok();

    Ok(())
}
