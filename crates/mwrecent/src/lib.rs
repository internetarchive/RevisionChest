//! Following a MediaWiki wiki by its recent changes: the `list=recentchanges` query, and a
//! batch of rows read the way the Wikidata Query Service's updater read them before it had
//! an event stream.
//!
//! A [`Poller`] holds the follower's position: the newest timestamp it has seen, the
//! continuation while it has one, and the `rcid`s it remembers. [`Poller::query`] builds
//! the next request, and [`Poller::read`] takes the response apart:
//!
//! - every row moves the position forward, used or not;
//! - a row whose `rcid` was seen within the tail window is skipped;
//! - with [`Coalesce::ByTitle`] (the default), the rest are coalesced to one per title, the
//!   highest revision winning, and a log row (revision 0: a deletion, restoration, move,
//!   merge or suppression) always staying; with [`Coalesce::Off`], every row is kept, in
//!   the order the wiki returned them, which is what an archive of every revision needs.
//!
//! Near the present (within the settle window) the query reads by time from ten seconds
//! before the position instead of by continuation, since rows can be committed late with
//! earlier timestamps. A full batch of nothing new moves the position on one second. The
//! caller runs the tail pass by asking for a [`Poller::tail_query`] once caught up.
//!
//! The crate is pure: it sends nothing. Which wiki, which namespaces, how to send a
//! request and where to store the position are the caller's. Timestamps are seconds since
//! the epoch; [`iso`] and [`parse_iso`] convert the wiki's ISO 8601 strings.
//!
//! ```
//! use mwrecent::{Coalesce, Poller, Windows};
//!
//! let start = mwrecent::parse_iso("2026-10-04T00:00:00Z").unwrap();
//! let mut poller = Poller::new(start, vec![0], 500, Windows::default()).coalesce(Coalesce::Off);
//! let now = start + 86_400;
//! let url = format!("https://example.org/w/api.php?{}", poller.query(now, true).to_query_string());
//! # let _ = url;
//! // … send it, then:
//! let body = r#"{"query":{"recentchanges":[
//!     {"rcid":1,"type":"edit","ns":0,"title":"Example","revid":10,"pageid":3,"timestamp":"2026-10-04T00:00:01Z"},
//!     {"rcid":2,"type":"edit","ns":0,"title":"Example","revid":11,"pageid":3,"timestamp":"2026-10-04T00:00:02Z"}
//! ]}}"#;
//! let batch = poller.read(body, false).unwrap();
//! assert_eq!(batch.changes.len(), 2);
//! assert_eq!(poller.time, start + 2);
//! ```

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, VecDeque};
use std::fmt::Write as _;

use serde::Deserialize;

/// A follower's windows, in seconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Windows {
    /// Within this of now, read by time, backed off by [`BACKOFF`].
    pub settle: u64,
    /// How far behind the head the tail pass re-reads, and how long seen `rcid`s are
    /// remembered. Zero turns the tail pass off.
    pub tail: u64,
}

impl Default for Windows {
    fn default() -> Self {
        Windows {
            settle: 120,
            tail: 900,
        }
    }
}

/// How far a settling read backs off from the position, in seconds.
pub const BACKOFF: u64 = 10;

/// What [`Poller::read`] does with several rows about one title.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Coalesce {
    /// One change per title: the highest revision wins, and a log row always stays. For a
    /// follower that reads each page's current state.
    #[default]
    ByTitle,
    /// Every row is a change, in the wiki's order. For an archive of every revision.
    Off,
}

/// A `list=recentchanges` request's parameters, in query order.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Query {
    /// The parameters.
    pub params: Vec<(String, String)>,
}

impl Query {
    /// The parameters as a query string, percent-encoding what needs it.
    #[must_use]
    pub fn to_query_string(&self) -> String {
        self.params
            .iter()
            .map(|(k, v)| format!("{}={}", encode(k), encode(v)))
            .collect::<Vec<_>>()
            .join("&")
    }
}

fn encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'|' => {
                out.push(b as char);
            }
            _ => {
                let _ = write!(out, "%{b:02X}");
            }
        }
    }
    out
}

/// One row of `list=recentchanges`, as the API serializes it with
/// `rcprop=title|ids|timestamp|loginfo`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[non_exhaustive]
pub struct Row {
    /// The change ID.
    pub rcid: u64,
    /// `edit`, `new`, `log`, `external`, `categorize`.
    #[serde(rename = "type")]
    pub kind: String,
    /// The namespace number.
    #[serde(default)]
    pub ns: Option<i64>,
    /// The title, with its namespace prefix.
    #[serde(default)]
    pub title: String,
    /// The revision, 0 for a log entry.
    #[serde(default)]
    pub revid: u64,
    /// The page ID; on a deletion's log row, the deleted page's; 0 for a log row about
    /// no page.
    #[serde(default)]
    pub pageid: u64,
    /// The timestamp, ISO 8601.
    #[serde(default)]
    pub timestamp: String,
    /// `delete`, `move`, `merge`, `suppress`, … on a log row.
    #[serde(default)]
    pub logtype: Option<String>,
    /// The log action, `delete`, `restore`, `move`, `merge`, …
    #[serde(default)]
    pub logaction: Option<String>,
    /// A move's or merge's parameters.
    #[serde(default)]
    pub logparams: Option<serde_json::Value>,
    /// The log entry's ID, on a log row.
    #[serde(default)]
    pub logid: Option<u64>,
}

/// The response, as far as the follower reads it.
#[derive(Debug, Clone, Deserialize)]
struct Response {
    #[serde(default)]
    #[serde(rename = "continue")]
    cont: Option<BTreeMap<String, String>>,
    query: Option<ResponseQuery>,
}

#[derive(Debug, Clone, Deserialize)]
struct ResponseQuery {
    #[serde(default)]
    recentchanges: Vec<Row>,
}

/// One change to act on: a row the batch kept.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Change {
    /// The change ID of the row kept; under [`Coalesce::ByTitle`], the winning row's.
    pub rcid: u64,
    /// The title.
    pub title: String,
    /// The namespace.
    pub ns: i64,
    /// The row's revision (under [`Coalesce::ByTitle`], the highest the batch saw for the
    /// title), or `None` for a log row, which means "read the page's state whatever it is".
    pub revid: Option<u64>,
    /// The page's ID, which a deletion's log row also carries; `None` where a row gave 0.
    pub pageid: Option<u64>,
    /// The row's time, seconds since the epoch.
    pub time: u64,
    /// The log row's `(type, action, params)`, if it is one.
    pub log: Option<(String, String, Option<serde_json::Value>)>,
    /// The log entry's ID, on a log row whose response carried one (`rcprop=loginfo`).
    pub logid: Option<u64>,
}

/// What a batch yielded.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct Batch {
    /// The changes: under [`Coalesce::ByTitle`], in the order their titles were first
    /// seen; under [`Coalesce::Off`], in the wiki's order.
    pub changes: Vec<Change>,
    /// Rows the response carried, including the ones skipped.
    pub rows: usize,
    /// Rows skipped as already seen.
    pub repeated: usize,
    /// Whether the response carried a continuation.
    pub more: bool,
}

/// A follower's position and memory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Poller {
    /// The newest timestamp seen, seconds since the epoch.
    pub time: u64,
    /// The continuation, while there is one.
    pub cont: Option<BTreeMap<String, String>>,
    /// The `rcid`s seen, with their times, oldest first.
    seen: VecDeque<(u64, u64)>,
    windows: Windows,
    namespaces: Vec<i64>,
    limit: u32,
    coalesce: Coalesce,
}

impl Poller {
    /// A poller starting at `time`, over `namespaces` (every namespace when empty), asking
    /// for `limit` rows per request (500 anonymously, 5,000 with `apihighlimits`),
    /// coalescing by title.
    #[must_use]
    pub fn new(time: u64, namespaces: Vec<i64>, limit: u32, windows: Windows) -> Self {
        Poller {
            time,
            cont: None,
            seen: VecDeque::new(),
            windows,
            namespaces,
            limit,
            coalesce: Coalesce::default(),
        }
    }

    /// Restores a poller from a stored position: its time, continuation and seen IDs.
    /// Coalescing is not part of the position; set it again with [`Poller::coalesce`].
    #[must_use]
    pub fn restore(
        time: u64,
        cont: Option<BTreeMap<String, String>>,
        seen: impl IntoIterator<Item = (u64, u64)>,
        namespaces: Vec<i64>,
        limit: u32,
        windows: Windows,
    ) -> Self {
        let mut p = Poller::new(time, namespaces, limit, windows);
        p.cont = cont;
        p.seen = seen.into_iter().collect();
        p
    }

    /// The poller, with rows about one title coalesced or not.
    #[must_use]
    pub fn coalesce(mut self, coalesce: Coalesce) -> Self {
        self.coalesce = coalesce;
        self
    }

    /// Whether rows about one title are coalesced.
    #[must_use]
    pub fn coalescing(&self) -> Coalesce {
        self.coalesce
    }

    /// The seen `rcid`s with their times, for storing.
    #[must_use]
    pub fn seen(&self) -> Vec<(u64, u64)> {
        self.seen.iter().copied().collect()
    }

    /// The windows.
    #[must_use]
    pub fn windows(&self) -> Windows {
        self.windows
    }

    /// Whether the position is within the settle window of `now`.
    #[must_use]
    pub fn settling(&self, now: u64) -> bool {
        self.time + self.windows.settle >= now
    }

    fn base(&self, log_info: bool) -> Vec<(String, String)> {
        let mut prop = "title|ids|timestamp".to_string();
        if log_info {
            prop.push_str("|loginfo");
        }
        let namespaces = self
            .namespaces
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("|");
        let mut params: Vec<(String, String)> = [
            ("action", "query".to_string()),
            ("list", "recentchanges".into()),
            ("format", "json".into()),
            ("formatversion", "2".into()),
            ("rcdir", "newer".into()),
            ("rcprop", prop),
            ("rctype", "edit|new|log".into()),
            ("rcnamespace", namespaces),
            ("rclimit", self.limit.to_string()),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();
        if self.namespaces.is_empty() {
            params.retain(|(k, _)| k != "rcnamespace");
        }
        params
    }

    /// The next request: by continuation while there is one and the position is not
    /// settling, else by time, backed off when settling. `log_info` adds `loginfo` to
    /// `rcprop`, for the log rows' type, action and parameters.
    #[must_use]
    pub fn query(&self, now: u64, log_info: bool) -> Query {
        let mut params = self.base(log_info);
        match (&self.cont, self.settling(now)) {
            (Some(c), false) => {
                params.extend(c.iter().map(|(k, v)| (k.clone(), v.clone())));
            }
            (_, settling) => {
                let from = if settling {
                    self.time.saturating_sub(BACKOFF)
                } else {
                    self.time
                };
                params.push(("rcstart".into(), iso(from)));
            }
        }
        Query { params }
    }

    /// The tail pass's request: by time from `tail` seconds behind the head, independent
    /// of the continuation. `None` when the tail pass is off or the position has not
    /// caught up to within the tail window.
    #[must_use]
    pub fn tail_query(&self, now: u64, log_info: bool) -> Option<Query> {
        if self.windows.tail == 0 || self.time + self.windows.tail < now {
            return None;
        }
        let mut params = self.base(log_info);
        params.push(("rcstart".into(), iso(now.saturating_sub(self.windows.tail))));
        Some(Query { params })
    }

    /// Reads a response, moving the position, remembering `rcid`s and, under
    /// [`Coalesce::ByTitle`], coalescing by title. `tail` marks a tail pass, whose rows
    /// never move the position or its continuation.
    ///
    /// # Errors
    ///
    /// When the body is not a `list=recentchanges` response.
    pub fn read(&mut self, body: &str, tail: bool) -> Result<Batch, ReadError> {
        let resp: Response = serde_json::from_str(body).map_err(|e| ReadError(e.to_string()))?;
        let rows = resp.query.map(|q| q.recentchanges).unwrap_or_default();
        let mut batch = Batch {
            rows: rows.len(),
            more: resp.cont.is_some(),
            ..Batch::default()
        };
        let mut kept: Vec<Change> = Vec::new();
        let mut index: BTreeMap<String, usize> = BTreeMap::new();
        let mut newest = self.time;
        for row in rows {
            let Some(t) = parse_iso(&row.timestamp) else {
                continue;
            };
            newest = newest.max(t);
            let Some(ns) = row.ns else {
                continue;
            };
            if self.seen.iter().any(|(id, _)| *id == row.rcid) {
                batch.repeated += 1;
                continue;
            }
            self.seen.push_back((row.rcid, t));
            let is_log = row.kind == "log";
            let change = Change {
                rcid: row.rcid,
                logid: if is_log { row.logid } else { None },
                title: row.title,
                ns,
                revid: (!is_log).then_some(row.revid),
                pageid: (row.pageid != 0).then_some(row.pageid),
                time: t,
                log: is_log.then(|| {
                    (
                        row.logtype.unwrap_or_default(),
                        row.logaction.unwrap_or_default(),
                        row.logparams,
                    )
                }),
            };
            if self.coalesce == Coalesce::Off {
                kept.push(change);
                continue;
            }
            match index.get(&change.title) {
                None => {
                    index.insert(change.title.clone(), kept.len());
                    kept.push(change);
                }
                Some(&i) => {
                    let old = &mut kept[i];
                    // Every row about a title names the same page; keep one that says.
                    old.pageid = old.pageid.or(change.pageid);
                    // A log row always stays; otherwise the highest revision wins.
                    let replace = match (old.revid, change.revid) {
                        (None, _) => false,
                        (Some(_), None) => true,
                        (Some(a), Some(b)) => b > a,
                    };
                    if replace {
                        let pageid = change.pageid.or(old.pageid);
                        *old = Change { pageid, ..change };
                    }
                }
            }
        }
        if !tail {
            if kept.is_empty()
                && batch.rows > 0
                && usize::try_from(self.limit).is_ok_and(|l| batch.rows >= l)
                && resp.cont.is_none()
            {
                // A full batch of repeats: move on a second, as the query service did.
                newest = newest.max(self.time + 1);
            }
            self.time = newest;
            self.cont = resp.cont;
        }
        // Forget IDs older than the tail window behind the newest time seen.
        let horizon = self.time.saturating_sub(self.windows.tail.max(BACKOFF * 2));
        while self.seen.front().is_some_and(|(_, t)| *t < horizon) {
            self.seen.pop_front();
        }
        batch.changes = kept;
        Ok(batch)
    }
}

/// A response the follower could not read.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("recentchanges: {0}")]
pub struct ReadError(pub String);

/// Seconds since the epoch as `YYYY-MM-DDTHH:MM:SSZ`.
#[must_use]
pub fn iso(t: u64) -> String {
    let days = t / 86_400;
    let rem = t % 86_400;
    let (y, m, d) = civil_from_days(i64::try_from(days).unwrap_or(i64::MAX));
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z",
        rem / 3600,
        (rem % 3600) / 60,
        rem % 60
    )
}

/// `YYYY-MM-DDTHH:MM:SSZ` as seconds since the epoch.
#[must_use]
pub fn parse_iso(text: &str) -> Option<u64> {
    let text = text.trim().trim_end_matches('Z');
    let (date, time) = text.split_once('T')?;
    let mut dates = date.split('-').map(|x| x.parse::<i64>().ok());
    let (year, month, day) = (dates.next()??, dates.next()??, dates.next()??);
    let mut times = time.split(':').map(|x| x.parse::<u64>().ok());
    let (hour, minute, second) = (times.next()??, times.next()??, times.next()??);
    let days = days_from_civil(year, month, day);
    let secs = i64::try_from(hour * 3600 + minute * 60 + second).ok()?;
    u64::try_from(days * 86_400 + secs).ok()
}

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(rcid: u64, kind: &str, title: &str, revid: u64, ts: &str) -> String {
        format!(
            r#"{{"rcid":{rcid},"type":"{kind}","ns":860,"title":"{title}","revid":{revid},"pageid":1,"timestamp":"{ts}"{}}}"#,
            if kind == "log" {
                r#","logtype":"delete","logaction":"delete""#
            } else {
                ""
            }
        )
    }

    fn body(rows: &[String], cont: Option<&str>) -> String {
        let c = cont.map_or(String::new(), |c| {
            format!(r#""continue":{{"rccontinue":"{c}","continue":"-||"}},"#)
        });
        format!(r#"{{{c}"query":{{"recentchanges":[{}]}}}}"#, rows.join(","))
    }

    #[test]
    fn times_round_trip() {
        let t = parse_iso("2026-10-04T12:34:56Z").unwrap();
        assert_eq!(iso(t), "2026-10-04T12:34:56Z");
        assert_eq!(parse_iso("1970-01-01T00:00:00Z"), Some(0));
    }

    #[test]
    fn a_query_continues_in_the_past_and_reads_by_time_near_the_present() {
        let start = parse_iso("2026-10-04T00:00:00Z").unwrap();
        let now = start + 86_400 * 10;
        let mut p = Poller::new(start, vec![860, 862], 500, Windows::default());
        let q = p.query(now, false);
        assert!(
            q.to_query_string()
                .contains("rcstart=2026-10-04T00%3A00%3A00Z")
        );
        assert!(q.to_query_string().contains("rcnamespace=860|862"));
        assert!(q.to_query_string().contains("rctype=edit|new|log"));

        let b = p
            .read(
                &body(
                    &[row(1, "edit", "Item:Q8", 10, "2026-10-05T00:00:00Z")],
                    Some("20261005000000|1"),
                ),
                false,
            )
            .unwrap();
        assert_eq!(b.changes.len(), 1);
        assert!(b.more);
        assert_eq!(p.time, parse_iso("2026-10-05T00:00:00Z").unwrap());
        let q = p.query(now, false).to_query_string();
        assert!(q.contains("rccontinue=20261005000000|1"), "{q}");
        assert!(!q.contains("rcstart"));

        // Near the present: by time, ten seconds back, continuation ignored.
        let q = p.query(p.time + 30, false).to_query_string();
        assert!(q.contains("rcstart=2026-10-04T23%3A59%3A50Z"), "{q}");
        assert!(!q.contains("rccontinue"));
    }

    #[test]
    fn rows_are_deduped_by_rcid_and_coalesced_by_title_with_log_rows_winning() {
        let t0 = parse_iso("2026-10-04T00:00:00Z").unwrap();
        let mut p = Poller::new(t0, vec![860], 500, Windows::default());
        assert_eq!(p.coalescing(), Coalesce::ByTitle);
        let first = body(
            &[
                row(1, "edit", "Item:Q8", 10, "2026-10-04T00:00:01Z"),
                row(2, "edit", "Item:Q8", 12, "2026-10-04T00:00:02Z"),
                row(3, "edit", "Item:Q9", 20, "2026-10-04T00:00:03Z"),
                row(4, "log", "Item:Q9", 0, "2026-10-04T00:00:04Z"),
                row(5, "edit", "Item:Q10", 30, "2026-10-04T00:00:05Z"),
                row(6, "edit", "Item:Q10", 29, "2026-10-04T00:00:06Z"),
            ],
            None,
        );
        let b = p.read(&first, false).unwrap();
        assert_eq!(b.rows, 6);
        assert_eq!(b.repeated, 0);
        let got: Vec<(u64, String, Option<u64>)> = b
            .changes
            .iter()
            .map(|c| (c.rcid, c.title.clone(), c.revid))
            .collect();
        assert_eq!(
            got,
            [
                (2, "Item:Q8".into(), Some(12)),
                (4, "Item:Q9".into(), None),
                (5, "Item:Q10".into(), Some(30)),
            ]
        );
        assert_eq!(b.changes[1].log.as_ref().unwrap().0, "delete");

        // The same rows again, with one new one: only the new one is kept.
        let again = body(
            &[
                row(2, "edit", "Item:Q8", 12, "2026-10-04T00:00:02Z"),
                row(4, "log", "Item:Q9", 0, "2026-10-04T00:00:04Z"),
                row(7, "new", "Item:Q11", 40, "2026-10-04T00:00:07Z"),
            ],
            None,
        );
        let b = p.read(&again, false).unwrap();
        assert_eq!(b.repeated, 2);
        assert_eq!(b.changes.len(), 1);
        assert_eq!(b.changes[0].title, "Item:Q11");
        assert_eq!(p.time, t0 + 7);
    }

    #[test]
    fn without_coalescing_every_new_row_is_kept_in_the_wikis_order() {
        let t0 = parse_iso("2026-10-04T00:00:00Z").unwrap();
        let mut p = Poller::new(t0, vec![860], 500, Windows::default()).coalesce(Coalesce::Off);
        let first = body(
            &[
                row(1, "edit", "Item:Q8", 10, "2026-10-04T00:00:01Z"),
                row(2, "edit", "Item:Q8", 12, "2026-10-04T00:00:02Z"),
                row(3, "log", "Item:Q8", 0, "2026-10-04T00:00:03Z"),
                row(4, "edit", "Item:Q8", 11, "2026-10-04T00:00:04Z"),
            ],
            None,
        );
        let b = p.read(&first, false).unwrap();
        let got: Vec<(u64, Option<u64>)> = b.changes.iter().map(|c| (c.rcid, c.revid)).collect();
        assert_eq!(
            got,
            [(1, Some(10)), (2, Some(12)), (3, None), (4, Some(11))]
        );
        assert_eq!(b.changes[2].log.as_ref().unwrap().1, "delete");

        // Duplicates are still dropped by rcid, and a tail pass finds only what is late.
        let late = body(
            &[
                row(2, "edit", "Item:Q8", 12, "2026-10-04T00:00:02Z"),
                row(5, "edit", "Item:Q8", 13, "2026-10-04T00:00:03Z"),
            ],
            None,
        );
        let b = p.read(&late, true).unwrap();
        assert_eq!(b.repeated, 1);
        assert_eq!(b.changes.len(), 1);
        assert_eq!(b.changes[0].revid, Some(13));
        assert_eq!(p.time, t0 + 4, "a tail pass does not move the position");
    }

    #[test]
    fn a_change_keeps_the_page_id_its_rows_name() {
        let t0 = parse_iso("2026-10-04T00:00:00Z").unwrap();
        let mut p = Poller::new(t0, vec![860], 500, Windows::default());
        let page = |r: String, id: u64| r.replace(r#""pageid":1"#, &format!(r#""pageid":{id}"#));
        let b = p
            .read(
                &body(
                    &[
                        // A deletion's log row names the deleted page, as Wikidata's do.
                        page(row(1, "log", "Item:Q9", 0, "2026-10-04T00:00:01Z"), 90),
                        // An edit then a log row that names no page: the edit's stays.
                        page(row(2, "edit", "Item:Q8", 10, "2026-10-04T00:00:02Z"), 80),
                        page(row(3, "log", "Item:Q8", 0, "2026-10-04T00:00:03Z"), 0),
                        // A log row that names no page, then an edit that does.
                        page(row(4, "log", "Item:Q7", 0, "2026-10-04T00:00:04Z"), 0),
                        page(row(5, "edit", "Item:Q7", 11, "2026-10-04T00:00:05Z"), 70),
                        page(row(6, "log", "Item:Q6", 0, "2026-10-04T00:00:06Z"), 0),
                    ],
                    None,
                ),
                false,
            )
            .unwrap();
        let pages: Vec<_> = b
            .changes
            .iter()
            .map(|c| (c.title.as_str(), c.pageid))
            .collect();
        assert_eq!(
            pages,
            [
                ("Item:Q9", Some(90)),
                ("Item:Q8", Some(80)),
                ("Item:Q7", Some(70)),
                ("Item:Q6", None)
            ]
        );
    }

    #[test]
    fn a_tail_pass_moves_nothing_and_a_full_batch_of_repeats_moves_a_second() {
        let t0 = parse_iso("2026-10-04T00:00:00Z").unwrap();
        let mut p = Poller::new(t0, vec![860], 2, Windows::default());
        let rows = [
            row(1, "edit", "Item:Q8", 10, "2026-10-04T00:00:01Z"),
            row(2, "edit", "Item:Q9", 11, "2026-10-04T00:00:01Z"),
        ];
        p.read(&body(&rows, None), false).unwrap();
        assert_eq!(p.time, t0 + 1);
        // Tail pass: repeats, and the position stays.
        let b = p.read(&body(&rows, None), true).unwrap();
        assert_eq!(b.repeated, 2);
        assert_eq!(p.time, t0 + 1);
        // Head pass, a full batch of repeats, no continuation: one second on.
        p.read(&body(&rows, None), false).unwrap();
        assert_eq!(p.time, t0 + 2);
        assert!(p.tail_query(t0 + 100, false).is_some());
        assert!(p.tail_query(t0 + 100_000, false).is_none(), "not caught up");
    }

    #[test]
    fn seen_ids_are_forgotten_beyond_the_tail_window() {
        let t0 = parse_iso("2026-10-04T00:00:00Z").unwrap();
        let w = Windows {
            settle: 120,
            tail: 60,
        };
        let mut p = Poller::new(t0, vec![860], 500, w);
        p.read(
            &body(
                &[row(1, "edit", "Item:Q8", 10, "2026-10-04T00:00:01Z")],
                None,
            ),
            false,
        )
        .unwrap();
        p.read(
            &body(
                &[row(2, "edit", "Item:Q8", 11, "2026-10-04T00:05:00Z")],
                None,
            ),
            false,
        )
        .unwrap();
        assert_eq!(p.seen().len(), 1, "rcid 1 is older than the window");
        let restored = Poller::restore(p.time, p.cont.clone(), p.seen(), vec![860], 500, w);
        assert_eq!(restored, p);
    }

    #[test]
    fn no_namespaces_means_every_namespace_and_log_rows_carry_their_id() {
        let t0 = parse_iso("2026-10-04T00:00:00Z").unwrap();
        let mut p = Poller::new(t0, vec![], 500, Windows::default()).coalesce(Coalesce::Off);
        let q = p.query(t0 + 86_400, true).to_query_string();
        assert!(!q.contains("rcnamespace"), "{q}");
        assert!(q.contains("rcprop=title|ids|timestamp|loginfo"), "{q}");
        let log = row(1, "log", "Item:Q9", 0, "2026-10-04T00:00:01Z")
            .replace(r#""logtype""#, r#""logid":77,"logtype""#);
        let edit = row(2, "edit", "Item:Q9", 10, "2026-10-04T00:00:02Z");
        let b = p.read(&body(&[log, edit], None), false).unwrap();
        assert_eq!(b.changes[0].logid, Some(77));
        assert_eq!(b.changes[1].logid, None);
    }

    #[test]
    fn a_continuation_passes_through_whatever_its_keys() {
        let t0 = parse_iso("2026-10-04T00:00:00Z").unwrap();
        let mut cont = BTreeMap::new();
        cont.insert("rccontinue".to_string(), "20261004000000|5".to_string());
        cont.insert("grccontinue".to_string(), "x y".to_string());
        let p = Poller::restore(t0, Some(cont), [], vec![0], 500, Windows::default());
        let q = p.query(t0 + 86_400, false).to_query_string();
        assert!(q.contains("rccontinue=20261004000000|5"), "{q}");
        assert!(q.contains("grccontinue=x%20y"), "{q}");
    }
}
