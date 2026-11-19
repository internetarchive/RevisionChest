//! The Postgres sink: writes into the Wikipedia Citations Database schema.
//!
//! Tables touched (Stage 0 of the WCD pipeline, see
//! `docs/design/proposed_data_model.md` in the monorepo):
//!
//! | table | what |
//! |---|---|
//! | `domains` | the site, inserted if absent (`value` = domain) |
//! | `revision_bundles` | one row per bundle file (`file_path`) |
//! | `revisions` | every revision: `(revision_id, domain_id, page_id, parent_revision_id, revision_timestamp TIMESTAMP, found_in_bundle, offset_begin, length)` |
//! | `page_metadata` | `(domain_id, page_id, page_title, page_namespace)` |
//! | `web_resources` | the article's curid URL (`url`, `url_hash`, `numeric_page_id`, `numeric_namespace_id`, `domain_id`) |
//!
//! Nothing is written to `documents`: pages are identified by
//! `(domain_id, page_id)`, not by title.
//!
//! Events are batched (10 000 per transaction by default) and written by a
//! few worker connections through `COPY` into temp tables followed by
//! `INSERT … ON CONFLICT`. Deadlocks and serialization failures are retried.

use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Sender, channel};
use std::sync::{Arc, RwLock};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use postgres::NoTls;
use postgres::error::SqlState;
use r2d2_postgres::PostgresConnectionManager;

use crate::{MetaEvent, MetaSink, SinkError};

type Pool = r2d2::Pool<PostgresConnectionManager<NoTls>>;

/// Connection settings.
#[derive(Debug, Clone)]
pub struct PostgresConfig {
    /// A libpq connection string or URL.
    pub url: String,
    /// Writer threads (and pool size).
    pub workers: usize,
    /// Events per transaction.
    pub batch_size: usize,
}

impl PostgresConfig {
    /// From the environment: `DATABASE_URL`, or the historical
    /// `DB_HOST`/`DB_PORT`/`DB_NAME`/`DB_USER`/`DB_PASS` set. `None` when neither
    /// is configured.
    #[must_use]
    pub fn from_env() -> Option<Self> {
        let url = match std::env::var("DATABASE_URL") {
            Ok(u) if !u.is_empty() => u,
            _ => {
                let get = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
                let (host, port, name, user, pass) = (
                    get("DB_HOST")?,
                    get("DB_PORT")?,
                    get("DB_NAME")?,
                    get("DB_USER")?,
                    get("DB_PASS")?,
                );
                format!("postgresql://{user}:{pass}@{host}:{port}/{name}")
            }
        };
        Some(Self {
            url,
            workers: 4,
            batch_size: 10_000,
        })
    }
}

/// The required tables; checked once at start so a wrong database fails fast.
const REQUIRED_TABLES: [&str; 5] = [
    "domains",
    "revision_bundles",
    "revisions",
    "page_metadata",
    "web_resources",
];

/// What a writer thread receives: the domain the rows belong to, and the rows.
struct Batch {
    domain_id: i32,
    domain: String,
    events: Vec<MetaEvent>,
}

/// The sink. See the module documentation.
pub struct PostgresSink {
    pool: Pool,
    domain: Option<(i32, String)>,
    before_domain: Vec<MetaEvent>,
    batch: Vec<MetaEvent>,
    batch_size: usize,
    workers: Vec<(Sender<Batch>, JoinHandle<()>)>,
    next_worker: usize,
    failed: Arc<AtomicBool>,
}

impl PostgresSink {
    /// Connects, verifies the tables and starts the writers.
    pub fn connect(cfg: &PostgresConfig) -> Result<Self, SinkError> {
        let manager = PostgresConnectionManager::new(
            cfg.url
                .parse()
                .map_err(|e: postgres::Error| SinkError::Other(format!("DATABASE_URL: {e}")))?,
            NoTls,
        );
        let pool = r2d2::Pool::builder()
            .max_size(cfg.workers as u32 + 1)
            .build(manager)
            .map_err(|e| SinkError::Other(format!("postgres pool: {e}")))?;
        {
            let mut c = pool.get().map_err(|e| SinkError::Other(e.to_string()))?;
            let rows = c
                .query(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = ANY($1)",
                    &[&&REQUIRED_TABLES[..]],
                )
                .map_err(|e| SinkError::Other(e.to_string()))?;
            let present: HashSet<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
            if let Some(missing) = REQUIRED_TABLES.iter().find(|t| !present.contains(**t)) {
                return Err(SinkError::Other(format!(
                    "required table '{missing}' is missing; create the WCD schema first (wcd init-db)"
                )));
            }
        }
        Ok(Self {
            pool,
            domain: None,
            before_domain: Vec::new(),
            batch: Vec::with_capacity(cfg.batch_size),
            batch_size: cfg.batch_size,
            workers: Vec::with_capacity(cfg.workers),
            next_worker: 0,
            failed: Arc::new(AtomicBool::new(false)),
        })
        .map(|mut s| {
            s.start_workers(cfg.workers);
            s
        })
    }

    fn start_workers(&mut self, n: usize) {
        let bundle_cache: Arc<RwLock<HashMap<String, i32>>> = Arc::new(RwLock::new(HashMap::new()));
        for i in 0..n.max(1) {
            let (tx, rx) = channel::<Batch>();
            let pool = self.pool.clone();
            let cache = bundle_cache.clone();
            let failed = self.failed.clone();
            let handle = thread::Builder::new()
                .name(format!("pg-writer-{i}"))
                .spawn(move || {
                    while let Ok(b) = rx.recv() {
                        if let Err(e) =
                            write_batch(&pool, b.domain_id, &b.domain, &b.events, &cache)
                        {
                            eprintln!("postgres writer: {e}");
                            failed.store(true, Ordering::SeqCst);
                        }
                    }
                })
                .expect("spawn postgres writer");
            self.workers.push((tx, handle));
        }
    }

    fn dispatch(&mut self) {
        if self.batch.is_empty() {
            return;
        }
        let Some((id, name)) = &self.domain else {
            return;
        };
        let batch = Batch {
            domain_id: *id,
            domain: name.clone(),
            events: std::mem::take(&mut self.batch),
        };
        let (tx, _) = &self.workers[self.next_worker];
        let _ = tx.send(batch);
        self.next_worker = (self.next_worker + 1) % self.workers.len();
    }

    fn set_domain(&mut self, domain: &str) -> Result<(), SinkError> {
        let mut c = self
            .pool
            .get()
            .map_err(|e| SinkError::Other(e.to_string()))?;
        let mut tx = c
            .transaction()
            .map_err(|e| SinkError::Other(e.to_string()))?;
        tx.execute(
            "INSERT INTO domains (value) VALUES ($1) ON CONFLICT (value) DO NOTHING",
            &[&domain],
        )
        .map_err(|e| SinkError::Other(e.to_string()))?;
        let id: i32 = tx
            .query_one("SELECT id FROM domains WHERE value = $1", &[&domain])
            .map_err(|e| SinkError::Other(e.to_string()))?
            .get(0);
        tx.commit().map_err(|e| SinkError::Other(e.to_string()))?;
        self.domain = Some((id, domain.to_string()));
        let pending = std::mem::take(&mut self.before_domain);
        for ev in pending {
            self.push(ev);
        }
        Ok(())
    }

    fn push(&mut self, ev: MetaEvent) {
        self.batch.push(ev);
        if self.batch.len() >= self.batch_size {
            self.dispatch();
        }
    }
}

impl MetaSink for PostgresSink {
    fn handle(&mut self, event: MetaEvent) -> Result<(), SinkError> {
        match event {
            MetaEvent::SiteInfo { domain, .. } => {
                if self.domain.as_ref().is_some_and(|(_, d)| *d == domain) {
                    return Ok(());
                }
                self.dispatch(); // flush rows of the previous domain
                self.set_domain(&domain)
            }
            MetaEvent::Finalize => {
                self.dispatch();
                Ok(())
            }
            ev => {
                if self.domain.is_none() {
                    if self.before_domain.len() >= 1_000_000 {
                        return Err(SinkError::Other(
                            "a million events and no SiteInfo: pass --domain".into(),
                        ));
                    }
                    self.before_domain.push(ev);
                } else {
                    self.push(ev);
                }
                Ok(())
            }
        }
    }

    fn finish(mut self: Box<Self>) -> Result<(), SinkError> {
        if !self.before_domain.is_empty() {
            return Err(SinkError::Other(format!(
                "{} events were never written: no site domain was seen (pass --domain)",
                self.before_domain.len()
            )));
        }
        self.dispatch();
        let workers = std::mem::take(&mut self.workers);
        for (tx, handle) in workers {
            drop(tx);
            let _ = handle.join();
        }
        if self.failed.load(Ordering::SeqCst) {
            return Err(SinkError::Other(
                "one or more batches failed; see messages above".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug)]
enum WriteError {
    Pg(postgres::Error),
    Io(std::io::Error),
}
impl From<postgres::Error> for WriteError {
    fn from(e: postgres::Error) -> Self {
        WriteError::Pg(e)
    }
}
impl From<std::io::Error> for WriteError {
    fn from(e: std::io::Error) -> Self {
        WriteError::Io(e)
    }
}
impl std::fmt::Display for WriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WriteError::Pg(e) => match e.as_db_error() {
                Some(d) => write!(f, "{} ({})", d.message(), d.code().code()),
                None => write!(f, "{e}"),
            },
            WriteError::Io(e) => write!(f, "copy: {e}"),
        }
    }
}

fn is_retryable(e: &WriteError) -> bool {
    match e {
        WriteError::Pg(e) => e.as_db_error().is_some_and(|d| {
            *d.code() == SqlState::T_R_SERIALIZATION_FAILURE
                || *d.code() == SqlState::T_R_DEADLOCK_DETECTED
        }),
        WriteError::Io(_) => false,
    }
}

/// Escapes a value for `COPY … FROM STDIN` text format.
fn copy_text(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\\' => out.push_str("\\\\"),
            '\0' => {}
            c => out.push(c),
        }
    }
    out
}

fn url_hash(url: &str) -> String {
    format!("{:x}", md5::compute(url))
}

const MAX_RETRIES: u64 = 5;

fn write_batch(
    pool: &Pool,
    domain_id: i32,
    domain: &str,
    batch: &[MetaEvent],
    bundle_cache: &Arc<RwLock<HashMap<String, i32>>>,
) -> Result<(), SinkError> {
    // Pages before revisions, each in key order, so concurrent writers take
    // locks in the same order.
    let mut sorted: Vec<&MetaEvent> = batch
        .iter()
        .filter(|e| matches!(e, MetaEvent::Page { .. } | MetaEvent::Revision { .. }))
        .collect();
    sorted.sort_by(|a, b| match (a, b) {
        (MetaEvent::Page { id: x, .. }, MetaEvent::Page { id: y, .. }) => x.cmp(y),
        (
            MetaEvent::Revision {
                file_path: x,
                rev_id: rx,
                ..
            },
            MetaEvent::Revision {
                file_path: y,
                rev_id: ry,
                ..
            },
        ) => x.cmp(y).then(rx.cmp(ry)),
        (MetaEvent::Page { .. }, _) => std::cmp::Ordering::Less,
        _ => std::cmp::Ordering::Greater,
    });

    let mut attempt = 0u64;
    loop {
        match try_write(pool, domain_id, domain, &sorted, bundle_cache) {
            Ok(()) => return Ok(()),
            Err(e) if attempt < MAX_RETRIES && is_retryable(&e) => {
                attempt += 1;
                eprintln!("postgres: {e} — retry {attempt}/{MAX_RETRIES}");
                thread::sleep(Duration::from_millis(100 * attempt));
            }
            Err(e) => return Err(SinkError::Other(e.to_string())),
        }
    }
}

fn try_write(
    pool: &Pool,
    domain_id: i32,
    domain: &str,
    sorted: &[&MetaEvent],
    bundle_cache: &Arc<RwLock<HashMap<String, i32>>>,
) -> Result<(), WriteError> {
    let mut client = pool
        .get()
        .map_err(|e| WriteError::Io(std::io::Error::other(e.to_string())))?;
    let mut tx = client.transaction()?;

    tx.batch_execute(
        "CREATE TEMP TABLE IF NOT EXISTS temp_pages (
            page_id INTEGER, ns INTEGER, title TEXT, url TEXT, url_hash CHAR(32)
        ) ON COMMIT DROP;
        CREATE TEMP TABLE IF NOT EXISTS temp_revisions (
            revision_id BIGINT, page_id INTEGER, found_in_bundle INTEGER, offset_begin BIGINT,
            length BIGINT, parent_revision_id BIGINT, revision_timestamp TEXT
        ) ON COMMIT DROP;
        TRUNCATE temp_pages, temp_revisions;",
    )?;

    let mut page_rows = Vec::new();
    let mut rev_rows = Vec::new();
    let mut seen_pages = HashSet::new();
    let mut new_bundles: Vec<(String, i32)> = Vec::new();

    for ev in sorted {
        match ev {
            MetaEvent::Page { id, ns, title } => {
                if seen_pages.insert(*id) {
                    let url = format!("https://{domain}/w/index.php?curid={id}");
                    page_rows.extend_from_slice(
                        format!(
                            "{}\t{}\t{}\t{}\t{}\n",
                            *id as i64,
                            ns,
                            copy_text(title),
                            url,
                            url_hash(&url)
                        )
                        .as_bytes(),
                    );
                }
            }
            MetaEvent::Revision {
                rev_id,
                parent_rev_id,
                page_id,
                file_path,
                offset_begin,
                length,
                timestamp,
            } => {
                let cached = bundle_cache.read().unwrap().get(file_path).copied();
                let bundle_id = match cached.or_else(|| {
                    new_bundles
                        .iter()
                        .find(|(p, _)| p == file_path)
                        .map(|(_, id)| *id)
                }) {
                    Some(id) => id,
                    None => {
                        let id: i32 = match tx.query_opt("SELECT id FROM revision_bundles WHERE file_path = $1", &[file_path])? {
                            Some(row) => row.get(0),
                            None => tx
                                .query_one(
                                    "INSERT INTO revision_bundles (file_path) VALUES ($1)
                                     ON CONFLICT (file_path) DO UPDATE SET file_path = EXCLUDED.file_path RETURNING id",
                                    &[file_path],
                                )?
                                .get(0),
                        };
                        new_bundles.push((file_path.clone(), id));
                        id
                    }
                };
                rev_rows.extend_from_slice(
                    format!(
                        "{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                        *rev_id as i64,
                        *page_id as i64,
                        bundle_id,
                        offset_begin,
                        length,
                        parent_rev_id.map_or_else(|| "\\N".to_string(), |p| p.to_string()),
                        copy_text(timestamp)
                    )
                    .as_bytes(),
                );
            }
            _ => {}
        }
    }

    if !page_rows.is_empty() {
        let mut w = tx.copy_in("COPY temp_pages (page_id, ns, title, url, url_hash) FROM STDIN")?;
        w.write_all(&page_rows)?;
        w.finish()?;
        tx.execute(
            "INSERT INTO page_metadata (domain_id, page_id, page_title, page_namespace)
             SELECT $1, page_id, title, ns FROM temp_pages
             ON CONFLICT (domain_id, page_id) DO UPDATE SET
                page_title = EXCLUDED.page_title, page_namespace = EXCLUDED.page_namespace",
            &[&domain_id],
        )?;
        tx.execute(
            "INSERT INTO web_resources (url, url_hash, numeric_page_id, numeric_namespace_id, domain_id)
             SELECT url, url_hash, page_id, ns, $1 FROM temp_pages
             ON CONFLICT (url_hash) DO UPDATE SET
                url = EXCLUDED.url, numeric_page_id = EXCLUDED.numeric_page_id,
                numeric_namespace_id = EXCLUDED.numeric_namespace_id, domain_id = EXCLUDED.domain_id",
            &[&domain_id],
        )?;
    }

    if !rev_rows.is_empty() {
        let mut w = tx.copy_in("COPY temp_revisions (revision_id, page_id, found_in_bundle, offset_begin, length, parent_revision_id, revision_timestamp) FROM STDIN")?;
        w.write_all(&rev_rows)?;
        w.finish()?;
        tx.execute(
            "INSERT INTO revisions (revision_id, domain_id, page_id, found_in_bundle, offset_begin, length, parent_revision_id, revision_timestamp)
             SELECT revision_id, $1, page_id, found_in_bundle, offset_begin, length, parent_revision_id, revision_timestamp::timestamp
             FROM temp_revisions
             ON CONFLICT (revision_id) DO NOTHING",
            &[&domain_id],
        )?;
    }

    tx.commit()?;
    if !new_bundles.is_empty() {
        let mut cache = bundle_cache.write().unwrap();
        for (p, id) in new_bundles {
            cache.insert(p, id);
        }
    }
    Ok(())
}

/// `MAX(revision_timestamp)` as `YYYY-MM-DDTHH:MM:SSZ`, or `None` when empty.
#[must_use]
pub fn latest_timestamp(cfg: &PostgresConfig) -> Option<String> {
    let mut c = postgres::Client::connect(&cfg.url, NoTls).ok()?;
    let row = c.query_one("SELECT to_char(MAX(revision_timestamp), 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') FROM revisions", &[]).ok()?;
    row.get::<_, Option<String>>(0)
}

/// Which of `rev_ids` are already recorded with a bundle location.
#[must_use]
pub fn existing_revisions(cfg: &PostgresConfig, rev_ids: &[u64]) -> HashSet<u64> {
    let Ok(mut c) = postgres::Client::connect(&cfg.url, NoTls) else {
        return HashSet::new();
    };
    let ids: Vec<i64> = rev_ids.iter().map(|&i| i as i64).collect();
    c.query("SELECT revision_id FROM revisions WHERE revision_id = ANY($1) AND found_in_bundle IS NOT NULL", &[&ids])
        .map(|rows| rows.iter().map(|r| r.get::<_, i64>(0) as u64).collect())
        .unwrap_or_default()
}
