//! `sync`: poll `list=recentchanges`, fetch the new revisions' content and
//! append them to daily bundles under `<output>/recentchanges/`.

use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::io;
use std::path::Path;

use chrono::{DateTime, Duration, Utc};
use moka::sync::Cache;
use mwrev::{BundleWriter, RevisionHeader};
use reqwest::blocking::Client;
use revisionchest_meta::{MetaEvent, MetaSender};
use serde_json::Value;

use crate::args::SyncArgs;
use crate::log;

/// Decides which revision IDs are already recorded (Postgres when configured).
pub type AlreadyRecorded = Box<dyn Fn(&[u64]) -> HashSet<u64> + Send>;

fn parse_ts(ts: &str) -> Option<DateTime<Utc>> {
    let n = revisionchest_meta::timestamp::normalize_utc(ts)?;
    DateTime::parse_from_rfc3339(&n)
        .ok()
        .map(|d| d.with_timezone(&Utc))
}

fn other<E: std::fmt::Display>(e: E) -> io::Error {
    io::Error::other(e.to_string())
}

/// GETs `url` with `params` and decodes JSON, retrying a response that does
/// not decode (a truncated or HTML error body) with exponential backoff.
fn request_with_retry(
    client: &Client,
    url: &str,
    params: &[(&str, &str)],
    max_retries: u32,
) -> io::Result<Value> {
    for attempt in 0..=max_retries {
        let response = client.get(url).query(params).send().map_err(other)?;
        match response.json::<Value>() {
            Ok(v) => return Ok(v),
            Err(e) if attempt < max_retries => {
                let delay = std::time::Duration::from_secs(2u64.pow(attempt.min(20)));
                log(&format!(
                    "Failed to decode response (attempt {}/{}): {e}. Retrying in {delay:?}",
                    attempt + 1,
                    max_retries + 1
                ));
                std::thread::sleep(delay);
            }
            Err(e) => {
                return Err(other(format!(
                    "failed to decode response after {} attempts: {e}",
                    max_retries + 1
                )));
            }
        }
    }
    unreachable!("loop returns on the last attempt")
}

/// Runs one sync pass, or loops when `--ongoing`.
///
/// `latest_recorded` is the newest revision timestamp already stored (from the
/// Postgres or Parquet metadata); polling starts `--overlap` minutes (default
/// 60) before it, or 24 hours ago when nothing is recorded, or at `--since`.
pub fn run_sync(
    args: &SyncArgs,
    meta: &MetaSender,
    latest_recorded: Option<String>,
    already: AlreadyRecorded,
) -> io::Result<()> {
    let interval_mins = args
        .interval
        .or_else(|| {
            std::env::var("SYNC_INTERVAL")
                .ok()
                .and_then(|v| v.parse().ok())
        })
        .unwrap_or(10);
    let overlap_mins = args
        .overlap
        .or_else(|| {
            std::env::var("SYNC_OVERLAP")
                .ok()
                .and_then(|v| v.parse().ok())
        })
        .unwrap_or(60);

    let mut cursor: DateTime<Utc> = match (&args.since, &latest_recorded) {
        (Some(s), _) => parse_ts(s).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("--since {s:?} is not a timestamp"),
            )
        })?,
        (None, Some(ts)) => {
            let ts = parse_ts(ts).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("stored timestamp {ts:?} is not a timestamp"),
                )
            })?;
            if Utc::now() - ts > Duration::days(30) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "the most recent recorded revision ({ts}) is more than 30 days old; recent changes only go back 30 days — rebuild from a dump instead"
                    ),
                ));
            }
            ts - Duration::minutes(overlap_mins as i64)
        }
        (None, None) => Utc::now() - Duration::hours(24),
    };

    let user_agent = format!(
        "{}/{} ({})",
        std::env::var("UA_APP_NAME").unwrap_or_else(|_| "RevisionChest".into()),
        std::env::var("UA_APP_VERSION").unwrap_or_else(|_| env!("CARGO_PKG_VERSION").into()),
        std::env::var("UA_CONTACT_INFO")
            .or_else(|_| std::env::var("CONTACT_EMAIL"))
            .unwrap_or_else(|_| "https://github.com/internetarchive/RevisionChest".into())
    );
    let client = Client::builder()
        .user_agent(user_agent)
        .build()
        .map_err(other)?;
    let api = format!("https://{}/w/api.php", args.domain);
    let namespaces = args.namespace.as_ref().map(|ns| {
        ns.iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("|")
    });
    let processed: Cache<u64, ()> = Cache::builder()
        .time_to_live(std::time::Duration::from_secs(25 * 3600))
        .build();

    loop {
        let mut rc_continue: Option<String> = None;
        loop {
            let start = cursor.format("%Y-%m-%dT%H:%M:%SZ").to_string();
            log(&format!(
                "Polling recent changes from {} (starting from {start})",
                args.domain
            ));
            let mut params = vec![
                ("action", "query"),
                ("list", "recentchanges"),
                ("rcstart", start.as_str()),
                ("rcdir", "newer"),
                ("rclimit", "500"),
                ("rcprop", "ids|timestamp|title|userid|ns"),
                ("format", "json"),
                ("formatversion", "2"),
            ];
            if let Some(ns) = &namespaces {
                params.push(("rcnamespace", ns));
            }
            if let Some(c) = &rc_continue {
                params.push(("rccontinue", c));
            }
            let resp = request_with_retry(&client, &api, &params, args.max_retries)?;

            let Some(rcs) = resp["query"]["recentchanges"].as_array() else {
                break;
            };
            if rcs.is_empty() {
                break;
            }
            if let Some(ts) = rcs
                .last()
                .and_then(|rc| rc["timestamp"].as_str())
                .and_then(parse_ts)
            {
                cursor = ts;
            }

            for rc in rcs {
                if let (Some(id), Some(ns), Some(title)) = (
                    rc["pageid"].as_u64(),
                    rc["ns"].as_i64(),
                    rc["title"].as_str(),
                ) {
                    if args
                        .namespace
                        .as_ref()
                        .is_none_or(|list| list.contains(&ns))
                    {
                        meta.send(MetaEvent::Page {
                            id,
                            ns,
                            title: title.to_string(),
                        });
                    }
                }
            }

            let wanted: Vec<u64> = rcs
                .iter()
                .filter_map(|rc| rc["revid"].as_u64())
                .filter(|id| !processed.contains_key(id))
                .collect();
            let recorded = already(&wanted);
            let wanted: Vec<u64> = wanted
                .into_iter()
                .filter(|id| !recorded.contains(id))
                .collect();

            for chunk in wanted.chunks(50) {
                log(&format!("Fetching content for {} revisions", chunk.len()));
                fetch_and_append(
                    &client,
                    &api,
                    chunk,
                    &args.output_dir,
                    args.namespace.as_deref(),
                    meta,
                    args.max_retries,
                )?;
                for &id in chunk {
                    processed.insert(id, ());
                }
            }
            meta.send(MetaEvent::Finalize);

            match resp["continue"]["rccontinue"].as_str() {
                Some(c) => rc_continue = Some(c.to_string()),
                None => break,
            }
        }

        if !args.ongoing {
            break;
        }
        log(&format!(
            "Sync cycle complete. Sleeping for {interval_mins} minutes"
        ));
        std::thread::sleep(std::time::Duration::from_secs(interval_mins * 60));
    }
    Ok(())
}

struct Fetched {
    header: RevisionHeader,
    text: String,
}

fn fetch_and_append(
    client: &Client,
    api: &str,
    rev_ids: &[u64],
    output_dir: &Path,
    namespaces: Option<&[i64]>,
    meta: &MetaSender,
    max_retries: u32,
) -> io::Result<()> {
    if rev_ids.is_empty() {
        return Ok(());
    }
    let ids = rev_ids
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("|");
    let params: Vec<(&str, &str)> = vec![
        ("action", "query"),
        ("prop", "revisions"),
        ("revids", ids.as_str()),
        ("rvprop", "ids|timestamp|user|content|userid|flags"),
        ("rvslots", "main"),
        ("format", "json"),
        ("formatversion", "2"),
    ];
    let resp = request_with_retry(client, api, &params, max_retries)?;
    let by_day = parse_revisions(&resp, namespaces);
    append_revisions(by_day, output_dir, meta)
}

/// The revisions in a `prop=revisions` (formatversion=2) response, grouped by
/// UTC day so each daily bundle is appended once per batch.
fn parse_revisions(resp: &Value, namespaces: Option<&[i64]>) -> BTreeMap<String, Vec<Fetched>> {
    let pages: Vec<&Value> = match &resp["query"]["pages"] {
        Value::Array(a) => a.iter().collect(),
        Value::Object(o) => o.values().collect(),
        _ => Vec::new(),
    };

    let mut by_day: BTreeMap<String, Vec<Fetched>> = BTreeMap::new();
    for page in pages {
        let ns = page["ns"].as_i64().unwrap_or(0);
        let page_id = page["pageid"].as_u64().unwrap_or(0);
        if !namespaces.is_none_or(|list| list.contains(&ns)) {
            continue;
        }
        for rev in page["revisions"].as_array().into_iter().flatten() {
            let rev_id = rev["revid"].as_u64().unwrap_or(0);
            let timestamp = rev["timestamp"].as_str().unwrap_or("");
            if rev_id == 0 || timestamp.len() < 10 {
                continue;
            }
            let text = rev["slots"]["main"]["content"]
                .as_str()
                .unwrap_or("")
                .to_string();
            by_day
                .entry(timestamp[..10].to_string())
                .or_default()
                .push(Fetched {
                    header: RevisionHeader {
                        page_id,
                        ns,
                        rev_id,
                        parent_rev_id: rev["parentid"].as_u64().filter(|p| *p != 0),
                        timestamp: timestamp.to_string(),
                        user_id: rev["userid"].as_u64().unwrap_or(0),
                    },
                    text,
                });
        }
    }

    by_day
}

/// Appends the revisions to `<output>/recentchanges/<day>.mwrev.zst` and reports where each landed.
fn append_revisions(
    by_day: BTreeMap<String, Vec<Fetched>>,
    output_dir: &Path,
    meta: &MetaSender,
) -> io::Result<()> {
    let rc_dir = output_dir.join("recentchanges");
    fs::create_dir_all(&rc_dir)?;
    for (day, revisions) in by_day {
        let file_name = format!("{day}.mwrev.zst");
        let relative = format!("recentchanges/{file_name}");
        let mut writer = BundleWriter::append(&rc_dir.join(&file_name))?;
        for f in &revisions {
            let loc = writer.write_revision(&f.header, &f.text)?;
            meta.send(MetaEvent::Revision {
                rev_id: f.header.rev_id,
                parent_rev_id: f.header.parent_rev_id,
                page_id: f.header.page_id,
                file_path: relative.clone(),
                offset_begin: loc.offset_begin,
                length: loc.length,
                timestamp: f.header.timestamp.clone(),
            });
        }
        writer.finish()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use revisionchest_meta::{MetaSink, SinkError};
    use std::sync::{Arc, Mutex};

    struct Collect(Arc<Mutex<Vec<MetaEvent>>>);
    impl MetaSink for Collect {
        fn handle(&mut self, e: MetaEvent) -> Result<(), SinkError> {
            self.0.lock().unwrap().push(e);
            Ok(())
        }
        fn finish(self: Box<Self>) -> Result<(), SinkError> {
            Ok(())
        }
    }

    fn api_response(revs: &[(u64, u64, Option<u64>, &str, &str)]) -> Value {
        // (page_id, rev_id, parent, timestamp, text), all in page 7 / ns 0 unless page_id differs
        let mut pages = serde_json::Map::new();
        for (page_id, rev_id, parent, ts, text) in revs {
            let page = pages.entry(page_id.to_string()).or_insert_with(|| serde_json::json!({"pageid": page_id, "ns": 0, "title": format!("P{page_id}"), "revisions": []}));
            page["revisions"]
                .as_array_mut()
                .unwrap()
                .push(serde_json::json!({
                    "revid": rev_id, "parentid": parent.unwrap_or(0), "timestamp": ts, "userid": 42,
                    "slots": {"main": {"content": text, "contentmodel": "wikitext"}}
                }));
        }
        serde_json::json!({"query": {"pages": pages.values().cloned().collect::<Vec<_>>()}})
    }

    #[test]
    fn appended_offsets_locate_each_revision_across_batches_and_days() {
        let dir = tempfile::tempdir().unwrap();
        let events = Arc::new(Mutex::new(Vec::new()));
        let (meta, handle) = revisionchest_meta::spawn(Box::new(Collect(events.clone())), 16);

        let batch1 = api_response(&[
            (
                7,
                100,
                None,
                "2026-10-18T23:59:00Z",
                "late on the 18th\nsecond line",
            ),
            (
                7,
                101,
                Some(100),
                "2026-10-19T00:00:10Z",
                "first on the 19th",
            ),
            (8, 200, None, "2026-10-19T01:00:00Z", "another page\n"),
        ]);
        let batch2 = api_response(&[(
            7,
            102,
            Some(101),
            "2026-10-19T02:00:00Z",
            "a later batch appends a new frame",
        )]);
        append_revisions(parse_revisions(&batch1, None), dir.path(), &meta).unwrap();
        append_revisions(parse_revisions(&batch2, None), dir.path(), &meta).unwrap();
        drop(meta);
        handle.join().unwrap();

        let events = events.lock().unwrap();
        let revs: Vec<_> = events
            .iter()
            .filter_map(|e| match e {
                MetaEvent::Revision {
                    rev_id,
                    file_path,
                    offset_begin,
                    length,
                    ..
                } => Some((*rev_id, file_path.clone(), *offset_begin, *length)),
                _ => None,
            })
            .collect();
        assert_eq!(
            revs.iter().map(|r| r.0).collect::<Vec<_>>(),
            vec![100, 101, 200, 102]
        );
        assert_eq!(revs[0].1, "recentchanges/2026-10-18.mwrev.zst");
        assert_eq!(revs[1].1, "recentchanges/2026-10-19.mwrev.zst");

        // Every recorded location must read back exactly that revision's text lines,
        // including the one appended as a second zstd frame.
        let expected = [
            (100u64, " late on the 18th\n second line\n"),
            (101, " first on the 19th\n"),
            (200, " another page\n"),
            (102, " a later batch appends a new frame\n"),
        ];
        for ((rev_id, file_path, off, len), (exp_id, exp_text)) in revs.iter().zip(expected) {
            assert_eq!(*rev_id, exp_id);
            let bytes = mwrev::read_at(&dir.path().join(file_path), *off, *len).unwrap();
            assert_eq!(String::from_utf8(bytes).unwrap(), exp_text, "rev {rev_id}");
        }
        // and the daily bundle reads back as three revisions in order
        let ids: Vec<u64> =
            mwrev::open_bundle(&dir.path().join("recentchanges/2026-10-19.mwrev.zst"))
                .unwrap()
                .map(|r| r.unwrap().0.rev_id)
                .collect();
        assert_eq!(ids, vec![101, 200, 102]);
    }

    #[test]
    fn namespace_filter_and_bad_rows() {
        let mut resp = api_response(&[(7, 1, None, "2026-10-19T00:00:00Z", "x")]);
        resp["query"]["pages"][0]["ns"] = serde_json::json!(118);
        assert!(parse_revisions(&resp, Some(&[0])).is_empty());
        assert_eq!(parse_revisions(&resp, Some(&[0, 118])).len(), 1);
        let resp = api_response(&[(7, 0, None, "", "x")]);
        assert!(parse_revisions(&resp, None).is_empty());
    }
}
