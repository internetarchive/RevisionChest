//! `sync`: follow a wiki's recent changes with `mwrecent`, fetch the new
//! revisions' content and append them to daily bundles under
//! `<output>/recentchanges/`, recording log events as they come.
//!
//! **Correctness.** Each source of error has its own mechanism, in place of a
//! fixed overlap:
//!
//! - *Late rows*: `mwrecent` reads by time near the present (the settle
//!   window) and re-reads the recent past once caught up (the tail pass),
//!   dropping rows it has seen by `rcid`.
//! - *Restarts*: a cycle's bundle frames, its metadata and the poller's
//!   position commit together. The position (`recentchanges/position.json`)
//!   holds the poller's time, continuation and seen `rcid`s, each bundle's
//!   committed length, and, while a cycle runs, the Parquet file it is
//!   writing. On start, a cycle that did not commit is rolled forward: the
//!   revisions in the bundles past their committed length are reported again
//!   from the bundles, a frame cut off part way is cut back, the unfinished
//!   Parquet file is removed, and the poller resumes from the committed
//!   position. Nothing is fetched or appended twice. A cycle still behind
//!   after `--commit-every` minutes stops and commits, so a long catch-up
//!   is rolled forward from its last commit, not from its start.
//! - *Gaps*: a position older than the wiki keeps recent changes
//!   (`--rc-max-age`), and older than its oldest retained change, cannot be
//!   resumed without missing revisions. `sync` refuses to start;
//!   `--accept-gap` resumes from the oldest change and records the gap in
//!   `recentchanges/gaps.jsonl`.
//! - *Later changes*: deletions, suppressions, revision-visibility changes,
//!   restorations, moves and merges are recorded as log events; bundles are
//!   never rewritten, and readers derive tombstones from the events.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use moka::sync::Cache;
use mwrecent::{Change, Coalesce, Poller, Windows};
use mwrev::{BundleWriter, RevisionHeader};
use reqwest::blocking::Client;
use revisionchest_meta::{MetaEvent, MetaSender};
use serde_json::{Value, json};

use crate::args::SyncArgs;
use crate::log;

/// Decides which revision IDs are already recorded (Postgres or Parquet).
pub type AlreadyRecorded = Box<dyn Fn(&[u64]) -> HashSet<u64> + Send>;

/// Rows per `list=recentchanges` request (the anonymous limit).
const LIMIT: u32 = 500;
/// Revisions per `prop=revisions` request.
const FETCH_CHUNK: usize = 50;
/// The position file's name, in `<output>/recentchanges/`.
pub const POSITION_FILE: &str = "position.json";
/// Where accepted gaps are recorded, one JSON object per line.
pub const GAPS_FILE: &str = "gaps.jsonl";

fn other<E: std::fmt::Display>(e: E) -> io::Error {
    io::Error::other(e.to_string())
}

/// A wiki's Action API: one GET, its body returned as text.
pub trait Wiki {
    /// Sends `params` and returns the response body.
    ///
    /// # Errors
    ///
    /// When the request fails.
    fn get(&self, params: &[(String, String)]) -> io::Result<String>;
}

/// The Action API over HTTP, retrying with exponential backoff both a request
/// that fails (a connection refused or dropped, a body cut off while it was
/// read) and a response that is not JSON (a truncated or HTML error body).
pub struct HttpWiki {
    client: Client,
    api: String,
    max_retries: u32,
}

impl HttpWiki {
    /// The API of `https://{domain}/w/api.php`, with the User-Agent the
    /// Wikimedia policy asks for.
    ///
    /// # Errors
    ///
    /// When the HTTP client cannot be built.
    pub fn new(domain: &str, max_retries: u32) -> io::Result<Self> {
        let user_agent = format!(
            "{}/{} ({})",
            std::env::var("UA_APP_NAME").unwrap_or_else(|_| "RevisionChest".into()),
            std::env::var("UA_APP_VERSION").unwrap_or_else(|_| env!("CARGO_PKG_VERSION").into()),
            std::env::var("UA_CONTACT_INFO")
                .or_else(|_| std::env::var("CONTACT_EMAIL"))
                .unwrap_or_else(|_| "https://github.com/internetarchive/RevisionChest".into())
        );
        Self::with_api(
            format!("https://{domain}/w/api.php"),
            &user_agent,
            max_retries,
        )
    }

    /// The API at `api`, sending `user_agent`.
    ///
    /// # Errors
    ///
    /// When the HTTP client cannot be built.
    pub fn with_api(api: String, user_agent: &str, max_retries: u32) -> io::Result<Self> {
        let client = Client::builder()
            .user_agent(user_agent)
            .build()
            .map_err(other)?;
        Ok(Self {
            client,
            api,
            max_retries,
        })
    }
}

impl Wiki for HttpWiki {
    fn get(&self, params: &[(String, String)]) -> io::Result<String> {
        for attempt in 0..=self.max_retries {
            let failure = match self
                .client
                .get(&self.api)
                .query(params)
                .send()
                .and_then(reqwest::blocking::Response::text)
            {
                Ok(body) => match serde_json::from_str::<Value>(&body) {
                    Ok(_) => return Ok(body),
                    Err(e) => format!("failed to decode response: {e}"),
                },
                // a transport error ends no cycle: a dropped connection or a body cut
                // off part way is as transient as a truncated one
                Err(e) => format!("request failed: {e}"),
            };
            if attempt == self.max_retries {
                return Err(other(format!(
                    "{failure} (after {} attempts)",
                    self.max_retries + 1
                )));
            }
            let delay = std::time::Duration::from_secs(2u64.pow(attempt.min(20)));
            log(&format!(
                "{failure} (attempt {}/{}). Retrying in {delay:?}",
                attempt + 1,
                self.max_retries + 1
            ));
            std::thread::sleep(delay);
        }
        unreachable!("loop returns on the last attempt")
    }
}

/// Minutes between polls when `--ongoing`: `--interval`, else `SYNC_INTERVAL`, else 10.
pub fn interval_mins(args: &SyncArgs) -> u64 {
    args.interval
        .or_else(|| {
            std::env::var("SYNC_INTERVAL")
                .ok()
                .and_then(|v| v.parse().ok())
        })
        .unwrap_or(10)
}

/// The settle and tail windows, from `--settle` and `--tail` (seconds).
pub fn windows(args: &SyncArgs) -> Windows {
    let d = Windows::default();
    Windows {
        settle: args.settle.unwrap_or(d.settle),
        tail: args.tail.unwrap_or(d.tail),
    }
}

/// Where a sync with no position starts: `--since`, else the newest recorded
/// revision less the tail window, else 24 hours before `now`.
///
/// # Errors
///
/// When `--since` or the recorded timestamp is not a timestamp.
pub fn start_time(args: &SyncArgs, latest_recorded: Option<&str>, now: u64) -> io::Result<u64> {
    let parse = |s: &str, what: &str| {
        revisionchest_meta::timestamp::normalize_utc(s)
            .and_then(|n| mwrecent::parse_iso(&n))
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("{what} {s:?} is not a timestamp"),
                )
            })
    };
    Ok(match (&args.since, latest_recorded) {
        (Some(s), _) => parse(s, "--since")?,
        (None, Some(ts)) => parse(ts, "stored timestamp")?.saturating_sub(windows(args).tail),
        (None, None) => now.saturating_sub(24 * 3600),
    })
}

/// A bundle's committed length: compressed bytes, decompressed bytes.
type Committed = (u64, u64);

/// The position file's content.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Position {
    time: u64,
    cont: Option<BTreeMap<String, String>>,
    seen: Vec<(u64, u64)>,
    bundles: BTreeMap<String, Committed>,
    pending_parquet: Option<PathBuf>,
}

impl Position {
    fn to_json(&self) -> Value {
        json!({
            "version": 1,
            "time": mwrecent::iso(self.time),
            "continue": self.cont,
            "seen": self.seen,
            "bundles": self.bundles,
            "pending_parquet": self.pending_parquet,
        })
    }

    fn from_json(v: &Value) -> Option<Self> {
        if v["version"].as_u64() != Some(1) {
            return None;
        }
        let time = mwrecent::parse_iso(v["time"].as_str()?)?;
        let cont = serde_json::from_value(v["continue"].clone()).ok()?;
        let seen = serde_json::from_value(v["seen"].clone()).ok()?;
        let bundles = serde_json::from_value(v["bundles"].clone()).ok()?;
        let pending_parquet = v["pending_parquet"].as_str().map(PathBuf::from);
        Some(Self {
            time,
            cont,
            seen,
            bundles,
            pending_parquet,
        })
    }
}

/// Writes `value` to `path` through a temporary file and a rename, so a
/// reader sees the old file or the new one, never a torn write.
fn write_atomic(path: &Path, value: &Value) -> io::Result<()> {
    let tmp = path.with_extension("json.tmp");
    fs::write(&tmp, serde_json::to_vec_pretty(value).map_err(other)?)?;
    fs::File::open(&tmp)?.sync_all()?;
    fs::rename(&tmp, path)
}

/// The decompressed length of a bundle: its sidecar, else measured.
fn decompressed_len(path: &Path) -> io::Result<u64> {
    match fs::read_to_string(mwrev::writer_sidecar_path(path)) {
        Ok(s) => s.trim().parse().map_err(other),
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            Ok(mwrev::recover::read_tail(path, 0, 0)?.decompressed_len)
        }
        Err(e) => Err(e),
    }
}

/// The bundles in `dir`, by file name.
fn bundle_files(dir: &Path) -> io::Result<Vec<String>> {
    let mut names = Vec::new();
    if dir.is_dir() {
        for entry in fs::read_dir(dir)? {
            let name = entry?.file_name().to_string_lossy().into_owned();
            if name.ends_with(".mwrev.zst") {
                names.push(name);
            }
        }
    }
    names.sort();
    Ok(names)
}

/// The state a sync carries from one cycle to the next.
///
/// A cycle ([`Syncer::cycle`]) polls recent changes from the position up to
/// now, fetches the new revisions and reports them and the log events. The
/// caller gives each cycle its own metadata sink: it calls [`Syncer::begin`]
/// before the cycle, naming the Parquet file the sink writes, and
/// [`Syncer::commit`] once the sink has finished, which commits the cycle.
pub struct Syncer<'a> {
    args: &'a SyncArgs,
    wiki: Box<dyn Wiki + 'a>,
    now: Box<dyn Fn() -> u64 + 'a>,
    poller: Poller,
    dir: PathBuf,
    committed: BTreeMap<String, Committed>,
    touched: BTreeSet<String>,
    processed: Cache<u64, ()>,
    already: AlreadyRecorded,
    /// Metadata of a cycle that did not commit, reported again from the
    /// bundles at the start of the next.
    recovered: Vec<MetaEvent>,
}

impl<'a> Syncer<'a> {
    /// Sets up a sync: from the committed position when there is one (rolling
    /// an interrupted cycle forward), else from [`start_time`]; then checks
    /// the position against the oldest change the wiki still has.
    ///
    /// # Errors
    ///
    /// On a gap without `--accept-gap`, an unreadable position file, a bundle
    /// shorter than its committed length, or a failed request.
    pub fn new(
        args: &'a SyncArgs,
        wiki: Box<dyn Wiki + 'a>,
        now: Box<dyn Fn() -> u64 + 'a>,
        latest_recorded: Option<String>,
        already: AlreadyRecorded,
    ) -> io::Result<Self> {
        let dir = args.output_dir.join("recentchanges");
        fs::create_dir_all(&dir)?;
        let namespaces = args.namespace.clone().unwrap_or_default();
        let processed: Cache<u64, ()> = Cache::builder()
            .time_to_live(std::time::Duration::from_secs(25 * 3600))
            .build();

        let position_path = dir.join(POSITION_FILE);
        let stored = match fs::read(&position_path) {
            Ok(bytes) => {
                let v: Value = serde_json::from_slice(&bytes).map_err(other)?;
                Some(Position::from_json(&v).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "{} is not a position this version reads",
                            position_path.display()
                        ),
                    )
                })?)
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => None,
            Err(e) => return Err(e),
        };

        let mut recovered = Vec::new();
        let mut touched = BTreeSet::new();
        let (poller, committed) = match stored {
            Some(pos) => {
                let committed =
                    Self::roll_forward(&dir, &pos, &mut recovered, &mut touched, &processed)?;
                let poller = Poller::restore(
                    pos.time,
                    pos.cont.clone(),
                    pos.seen.clone(),
                    namespaces.clone(),
                    LIMIT,
                    windows(args),
                );
                (poller, committed)
            }
            None => {
                let start = start_time(args, latest_recorded.as_deref(), now())?;
                let mut committed = BTreeMap::new();
                for name in bundle_files(&dir)? {
                    let path = dir.join(&name);
                    committed.insert(name, (fs::metadata(&path)?.len(), decompressed_len(&path)?));
                }
                let poller = Poller::new(start, namespaces.clone(), LIMIT, windows(args));
                (poller, committed)
            }
        };

        let mut syncer = Self {
            args,
            wiki,
            now,
            poller: poller.coalesce(Coalesce::Off),
            dir,
            committed,
            touched,
            processed,
            already,
            recovered,
        };
        syncer.check_gap()?;
        syncer.save(None)?;
        Ok(syncer)
    }

    /// Rolls an interrupted cycle forward: reports again the revisions written
    /// past each bundle's committed length, cuts back a frame cut off part
    /// way, and removes the Parquet file the cycle was writing.
    fn roll_forward(
        dir: &Path,
        pos: &Position,
        recovered: &mut Vec<MetaEvent>,
        touched: &mut BTreeSet<String>,
        processed: &Cache<u64, ()>,
    ) -> io::Result<BTreeMap<String, Committed>> {
        if let Some(p) = &pos.pending_parquet {
            match fs::remove_file(p) {
                Ok(()) => log(&format!(
                    "Removed {}, written by a cycle that did not commit",
                    p.display()
                )),
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
        }
        let mut committed = pos.bundles.clone();
        for name in bundle_files(dir)? {
            let path = dir.join(&name);
            let (c, d) = committed.get(&name).copied().unwrap_or((0, 0));
            let len = fs::metadata(&path)?.len();
            if len == c {
                continue;
            }
            let tail = mwrev::recover::read_tail(&path, c, d)?;
            if tail.incomplete {
                log(&format!(
                    "{name}: cutting back a frame cut off part way ({} bytes)",
                    len - tail.compressed_len
                ));
            }
            mwrev::recover::truncate(&path, tail.compressed_len, tail.decompressed_len)?;
            if !tail.revisions.is_empty() {
                log(&format!(
                    "{name}: {} revisions written by a cycle that did not commit; reporting them again",
                    tail.revisions.len()
                ));
            }
            let file_path = format!("recentchanges/{name}");
            for (h, loc) in tail.revisions {
                processed.insert(h.rev_id, ());
                recovered.push(MetaEvent::Revision {
                    rev_id: h.rev_id,
                    parent_rev_id: h.parent_rev_id,
                    page_id: h.page_id,
                    file_path: file_path.clone(),
                    offset_begin: loc.offset_begin,
                    length: loc.length,
                    timestamp: h.timestamp,
                });
            }
            // What was rolled forward commits with the next cycle.
            committed.insert(name.clone(), (c, d));
            touched.insert(name);
        }
        Ok(committed)
    }

    /// Refuses a position the wiki no longer keeps changes for: older than
    /// `--rc-max-age` days before now, and older than the oldest change the wiki
    /// still has. Within the window, an older position only means nothing
    /// changed in between. `--accept-gap` moves the position to the oldest
    /// change and records the gap.
    fn check_gap(&mut self) -> io::Result<()> {
        let mut params = vec![
            ("action", "query"),
            ("list", "recentchanges"),
            ("rcdir", "newer"),
            ("rclimit", "1"),
            ("rcprop", "timestamp"),
            ("format", "json"),
            ("formatversion", "2"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect::<Vec<_>>();
        if let Some(ns) = &self.args.namespace {
            let ns = ns
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("|");
            params.push(("rcnamespace".into(), ns));
        }
        let body = self.wiki.get(&params)?;
        let v: Value = serde_json::from_str(&body).map_err(other)?;
        let Some(oldest) = v["query"]["recentchanges"][0]["timestamp"]
            .as_str()
            .and_then(mwrecent::parse_iso)
        else {
            return Ok(()); // nothing retained, so nothing can be missed
        };
        let position = self.poller.time;
        let retained_since = (self.now)().saturating_sub(self.args.rc_max_age * 86_400);
        if oldest <= position || position >= retained_since {
            return Ok(());
        }
        let (from, to) = (mwrecent::iso(position), mwrecent::iso(oldest));
        if !self.args.accept_gap {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "gap: the sync's position is {from}, but the oldest change {} still has is {to}; \
                     the changes between are gone. Rebuild that window from a dump, or pass \
                     --accept-gap to resume from {to} and record the gap",
                    self.args.domain
                ),
            ));
        }
        log(&format!("Accepting a gap from {from} to {to}"));
        let record = json!({"from": from, "to": to, "recorded": mwrecent::iso((self.now)())});
        let mut line = serde_json::to_string(&record).map_err(other)?;
        line.push('\n');
        use std::io::Write as _;
        fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.dir.join(GAPS_FILE))?
            .write_all(line.as_bytes())?;
        self.poller = Poller::new(
            oldest,
            self.args.namespace.clone().unwrap_or_default(),
            LIMIT,
            windows(self.args),
        )
        .coalesce(Coalesce::Off);
        Ok(())
    }

    fn save(&self, pending_parquet: Option<&Path>) -> io::Result<()> {
        // Absolute, so a restart from another working directory still finds the file.
        let pending_parquet = pending_parquet.map(std::path::absolute).transpose()?;
        let pos = Position {
            time: self.poller.time,
            cont: self.poller.cont.clone(),
            seen: self.poller.seen(),
            bundles: self.committed.clone(),
            pending_parquet,
        };
        write_atomic(&self.dir.join(POSITION_FILE), &pos.to_json())
    }

    /// Before a cycle: records the Parquet file its sink writes, so a cycle
    /// that does not commit can be rolled forward without duplicating rows.
    ///
    /// # Errors
    ///
    /// When the position cannot be written.
    pub fn begin(&self, parquet: Option<&Path>) -> io::Result<()> {
        self.save(parquet)
    }

    /// After the cycle's sink has finished: commits the bundles' lengths and
    /// the poller's position.
    ///
    /// # Errors
    ///
    /// When a bundle cannot be measured or the position cannot be written.
    pub fn commit(&mut self) -> io::Result<()> {
        for name in std::mem::take(&mut self.touched) {
            let path = self.dir.join(&name);
            self.committed
                .insert(name, (fs::metadata(&path)?.len(), decompressed_len(&path)?));
        }
        self.save(None)
    }

    /// One pass: reports what an interrupted cycle wrote, then polls recent
    /// changes from the position until caught up, runs the tail pass, and
    /// fetches and appends the revisions not yet recorded. With a `budget`
    /// (seconds), a pass still behind once it has run that long stops after
    /// the batch in hand, without the tail pass; the caller commits it and
    /// starts the next from the continuation. Returns whether it caught up.
    ///
    /// # Errors
    ///
    /// When a request, a response or a bundle write fails. Nothing is committed
    /// then, and the next start rolls forward what was written.
    pub fn cycle(&mut self, meta: &MetaSender, budget: Option<u64>) -> io::Result<bool> {
        if !self.recovered.is_empty() {
            for ev in std::mem::take(&mut self.recovered) {
                meta.send(ev);
            }
            meta.send(MetaEvent::Finalize);
        }
        let now = (self.now)();
        loop {
            log(&format!(
                "Polling recent changes from {} (from {})",
                self.args.domain,
                mwrecent::iso(self.poller.time)
            ));
            let query = self.poller.query(now, true);
            let body = self.wiki.get(&query.params)?;
            let batch = self.poller.read(&body, false).map_err(other)?;
            self.handle(&batch.changes, meta)?;
            if !batch.more {
                break;
            }
            if budget.is_some_and(|b| (self.now)().saturating_sub(now) >= b) {
                log(&format!(
                    "Behind after {} s, at {}: committing, then continuing",
                    (self.now)().saturating_sub(now),
                    mwrecent::iso(self.poller.time)
                ));
                return Ok(false);
            }
        }
        if let Some(query) = self.poller.tail_query(now, true) {
            let body = self.wiki.get(&query.params)?;
            let batch = self.poller.read(&body, true).map_err(other)?;
            if !batch.changes.is_empty() {
                log(&format!(
                    "Tail pass: {} changes committed late",
                    batch.changes.len()
                ));
            }
            self.handle(&batch.changes, meta)?;
        }
        Ok(true)
    }

    /// Reports the pages and log events of a batch, and fetches and appends
    /// its revisions not yet recorded.
    fn handle(&mut self, changes: &[Change], meta: &MetaSender) -> io::Result<()> {
        let mut wanted = Vec::new();
        for c in changes {
            if let Some(id) = c.pageid {
                meta.send(MetaEvent::Page {
                    id,
                    ns: c.ns,
                    title: c.title.clone(),
                });
            }
            match (&c.log, c.revid) {
                (Some((log_type, log_action, params)), _) => meta.send(MetaEvent::Log {
                    rcid: c.rcid,
                    log_id: c.logid,
                    log_type: log_type.clone(),
                    log_action: log_action.clone(),
                    page_id: c.pageid,
                    ns: c.ns,
                    title: c.title.clone(),
                    timestamp: mwrecent::iso(c.time),
                    params: params.as_ref().map(ToString::to_string),
                }),
                (None, Some(id)) if !self.processed.contains_key(&id) => wanted.push(id),
                _ => {}
            }
        }
        let recorded = (self.already)(&wanted);
        let mut once = HashSet::new();
        wanted.retain(|id| !recorded.contains(id) && once.insert(*id));
        for chunk in wanted.chunks(FETCH_CHUNK) {
            log(&format!("Fetching content for {} revisions", chunk.len()));
            let by_day = self.fetch(chunk)?;
            let names = append_revisions(by_day, &self.args.output_dir, meta)?;
            self.touched.extend(names);
            for &id in chunk {
                self.processed.insert(id, ());
            }
        }
        meta.send(MetaEvent::Finalize);
        Ok(())
    }

    fn fetch(&self, rev_ids: &[u64]) -> io::Result<BTreeMap<String, Vec<Fetched>>> {
        let ids = rev_ids
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("|");
        let params: Vec<(String, String)> = [
            ("action", "query"),
            ("prop", "revisions"),
            ("revids", ids.as_str()),
            ("rvprop", "ids|timestamp|user|content|userid|flags"),
            ("rvslots", "main"),
            ("format", "json"),
            ("formatversion", "2"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let body = self.wiki.get(&params)?;
        let resp: Value = serde_json::from_str(&body).map_err(other)?;
        Ok(parse_revisions(&resp, self.args.namespace.as_deref()))
    }
}

struct Fetched {
    header: RevisionHeader,
    text: String,
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
            let main = &rev["slots"]["main"];
            // A hidden revision comes back with `texthidden` and no content.
            let text_hidden = main["texthidden"].as_bool().unwrap_or(false)
                || rev["texthidden"].as_bool().unwrap_or(false);
            let text = main["content"].as_str().unwrap_or("").to_string();
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
                        text_hidden,
                    },
                    text,
                });
        }
    }

    by_day
}

/// Appends the revisions to `<output>/recentchanges/<day>.mwrev.zst`, reports
/// where each landed, and returns the bundles' file names.
fn append_revisions(
    by_day: BTreeMap<String, Vec<Fetched>>,
    output_dir: &Path,
    meta: &MetaSender,
) -> io::Result<Vec<String>> {
    let rc_dir = output_dir.join("recentchanges");
    fs::create_dir_all(&rc_dir)?;
    let mut names = Vec::new();
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
        writer.finish()?.sync_all()?;
        names.push(file_name);
    }
    Ok(names)
}

#[cfg(test)]
mod tests;
