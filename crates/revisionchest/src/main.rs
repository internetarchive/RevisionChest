//! RevisionChest: MediaWiki XML dumps → `.mwrev.zst` revision bundles, plus
//! the metadata that says where every revision landed.
//!
//! The work is done by three library crates — `mwdump` (reading dumps),
//! `mwrev` (the bundle format) and `revisionchest-meta` (metadata sinks) —
//! and this binary only drives them.

mod args;
mod build;
mod sync;

use std::collections::HashSet;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use chrono::Local;
use clap::Parser;
use rayon::prelude::*;
use revisionchest_meta::parquet::ParquetSink;
use revisionchest_meta::{MetaEvent, MetaSink, NullSink, SinkError};

use crate::args::{Args, Commands, MetaArgs};

pub(crate) fn log(msg: &str) {
    eprintln!("[{}] {}", Local::now().format("%Y-%m-%d %H:%M:%S"), msg);
}

/// Several sinks fed the same events.
struct Tee(Vec<Box<dyn MetaSink>>);

impl MetaSink for Tee {
    fn handle(&mut self, event: MetaEvent) -> Result<(), SinkError> {
        for s in &mut self.0 {
            s.handle(event.clone())?;
        }
        Ok(())
    }
    fn finish(self: Box<Self>) -> Result<(), SinkError> {
        for s in self.0 {
            s.finish()?;
        }
        Ok(())
    }
}

#[cfg(feature = "postgres")]
fn postgres_config(meta: &MetaArgs) -> Option<revisionchest_meta::postgres::PostgresConfig> {
    let wanted = meta.postgres || std::env::var("DATABASE").is_ok_and(|v| v == "postgres");
    if !wanted {
        return None;
    }
    match revisionchest_meta::postgres::PostgresConfig::from_env() {
        Some(cfg) => Some(cfg),
        None => {
            eprintln!(
                "error: --postgres given but neither DATABASE_URL nor DB_HOST/DB_PORT/DB_NAME/DB_USER/DB_PASS is set"
            );
            std::process::exit(2);
        }
    }
}

#[cfg(not(feature = "postgres"))]
fn postgres_config(meta: &MetaArgs) -> Option<()> {
    if meta.postgres {
        eprintln!(
            "error: this build has no Postgres support (built without the `postgres` feature)"
        );
        std::process::exit(2);
    }
    None
}

/// Parquet target for a run: a file as given, or a new per-run file inside a
/// directory (`sync` keeps every run's — and every `--ongoing` cycle's —
/// metadata that way). An existing file in the directory is never reused.
fn parquet_target(path: &Path) -> PathBuf {
    if path.is_dir() {
        let stem = format!("revisionchest-{}", Local::now().format("%Y%m%dT%H%M%S"));
        let mut target = path.join(format!("{stem}.parquet"));
        let mut n = 1;
        while target.exists() {
            target = path.join(format!("{stem}-{n}.parquet"));
            n += 1;
        }
        target
    } else {
        path.to_path_buf()
    }
}

fn make_sink(meta: &MetaArgs) -> io::Result<Box<dyn MetaSink>> {
    let target = meta.parquet.as_deref().map(parquet_target);
    make_sink_at(meta, target.as_deref())
}

/// The sink, writing Parquet to `target` (chosen by the caller, who may need
/// to know it beforehand).
fn make_sink_at(meta: &MetaArgs, target: Option<&Path>) -> io::Result<Box<dyn MetaSink>> {
    let mut sinks: Vec<Box<dyn MetaSink>> = Vec::new();
    if let Some(target) = target {
        if let Some(parent) = target.parent().filter(|d| !d.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent)?;
        }
        sinks.push(Box::new(
            ParquetSink::create(target).map_err(io::Error::other)?,
        ));
        log(&format!("Metadata → {}", target.display()));
    }
    #[cfg(feature = "postgres")]
    if let Some(cfg) = postgres_config(meta) {
        sinks.push(Box::new(
            revisionchest_meta::postgres::PostgresSink::connect(&cfg).map_err(io::Error::other)?,
        ));
        log("Metadata → Postgres");
    }
    #[cfg(not(feature = "postgres"))]
    let _ = postgres_config(meta);
    Ok(match sinks.len() {
        0 => {
            if !meta.no_db {
                eprintln!("note: no --parquet or --postgres given; only bundles will be written");
            }
            Box::new(NullSink)
        }
        1 => sinks.pop().unwrap(),
        _ => Box::new(Tee(sinks)),
    })
}

fn main() -> io::Result<()> {
    dotenvy::dotenv().ok();
    let args = Args::parse();

    match args.command {
        Commands::Build(b) => {
            if let Some(jobs) = b.jobs {
                rayon::ThreadPoolBuilder::new()
                    .num_threads(jobs)
                    .build_global()
                    .ok();
            }
            let sink = make_sink(&b.meta)?;
            let (meta, handle) = revisionchest_meta::spawn(sink, 10_000);
            if let Some(domain) = &b.domain {
                meta.send(MetaEvent::SiteInfo {
                    domain: domain.clone(),
                    language_code: None,
                });
            }

            let result = if let Some(dir) = &b.input_dir {
                let out = b.output_dir.as_deref().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "-o is required with -d")
                })?;
                let mut files = build::list_dumps(dir)?;
                build::largest_first(&mut files);
                let total = files.len();
                let started = AtomicUsize::new(0);
                let failures: Vec<(PathBuf, io::Error)> = files
                    .into_iter()
                    // A shared queue in that order: rayon would otherwise split
                    // the Vec into per-thread ranges and ignore the ordering.
                    .par_bridge()
                    .filter_map(|path| {
                        let n = started.fetch_add(1, Ordering::SeqCst) + 1;
                        build::build_file(
                            &path,
                            Some(out),
                            b.namespace.as_deref(),
                            b.domain.as_deref(),
                            &meta,
                            (n, total),
                        )
                        .err()
                        .map(|e| (path, e))
                    })
                    .collect();
                for (p, e) in &failures {
                    eprintln!("error: {}: {e}", p.display());
                }
                if failures.is_empty() {
                    Ok(())
                } else {
                    Err(io::Error::other(format!(
                        "{} of {total} dump files failed",
                        failures.len()
                    )))
                }
            } else if let Some(input) = &b.input {
                build::build_file(
                    input,
                    b.output_dir.as_deref(),
                    b.namespace.as_deref(),
                    b.domain.as_deref(),
                    &meta,
                    (1, 1),
                )
            } else {
                eprintln!(
                    "usage: RevisionChest build <dump.xml[.bz2|.gz|.7z]> [-o DIR] | build -d DUMP_DIR -o DIR"
                );
                std::process::exit(2);
            };

            drop(meta);
            let sink_result = handle.join();
            result?;
            sink_result.map_err(io::Error::other)?;
        }
        Commands::Sync(s) => {
            // Where to resume from, and how to skip what is already recorded.
            #[allow(unused_mut)]
            let mut latest: Option<String> = None;
            #[allow(unused_mut)]
            let mut already: sync::AlreadyRecorded = Box::new(|_ids: &[u64]| HashSet::new());
            #[cfg(feature = "postgres")]
            if let Some(cfg) = postgres_config(&s.meta) {
                latest = revisionchest_meta::postgres::latest_timestamp(&cfg);
                let cfg2 = cfg.clone();
                already = Box::new(move |ids: &[u64]| {
                    revisionchest_meta::postgres::existing_revisions(&cfg2, ids)
                });
            }
            if latest.is_none() {
                if let Some(p) = &s.meta.parquet {
                    latest = latest_parquet_timestamp(p);
                    // Without a position, sync starts before the newest recorded
                    // revision; what the Parquet metadata already records since
                    // then is not fetched again.
                    let now = chrono::Utc::now().timestamp().max(0) as u64;
                    let start = sync::start_time(&s, latest.as_deref(), now)?;
                    let recorded = parquet_revisions_since(p, &mwrecent::iso(start));
                    already = Box::new(move |ids: &[u64]| {
                        ids.iter()
                            .copied()
                            .filter(|id| recorded.contains(id))
                            .collect()
                    });
                }
            }
            if s.overlap.is_some() {
                log("--overlap is replaced by the committed position and --tail; ignoring it");
            }

            // Each cycle gets its own sink, finished before the next poll, so an
            // --ongoing sync leaves complete metadata on disk after every cycle
            // (a Parquet file has no footer, and cannot be read, until closed).
            // A single --parquet file would be truncated by every cycle.
            if s.ongoing {
                if let Some(p) = s.meta.parquet.as_ref().filter(|p| !p.is_dir()) {
                    eprintln!(
                        "error: --ongoing writes one metadata file per cycle; --parquet must be an existing directory, not {}",
                        p.display()
                    );
                    std::process::exit(2);
                }
            }
            let wiki = Box::new(sync::HttpWiki::new(&s.domain, s.max_retries)?);
            let now = Box::new(|| chrono::Utc::now().timestamp().max(0) as u64);
            let mut syncer = sync::Syncer::new(&s, wiki, now, latest, already)?;
            loop {
                let target = s.meta.parquet.as_deref().map(parquet_target);
                syncer.begin(target.as_deref())?;
                let sink = make_sink_at(&s.meta, target.as_deref())?;
                let (meta, handle) = revisionchest_meta::spawn(sink, 10_000);
                meta.send(MetaEvent::SiteInfo {
                    domain: s.domain.clone(),
                    language_code: None,
                });
                let result = syncer.cycle(&meta);
                drop(meta);
                let sink_result = handle.join();
                result?;
                sink_result.map_err(io::Error::other)?;
                // The bundles and the metadata are written: commit the cycle.
                syncer.commit()?;

                if !s.ongoing {
                    break;
                }
                let interval = sync::interval_mins(&s);
                log(&format!(
                    "Sync cycle complete. Sleeping for {interval} minutes"
                ));
                std::thread::sleep(std::time::Duration::from_secs(interval * 60));
            }
        }
    }
    Ok(())
}

/// The `*.parquet` files of a path: the file, or every one in the directory.
fn parquet_files(path: &Path) -> Vec<PathBuf> {
    if path.is_dir() {
        std::fs::read_dir(path)
            .into_iter()
            .flatten()
            .filter_map(Result::ok)
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|x| x == "parquet"))
            .collect()
    } else {
        vec![path.to_path_buf()]
    }
}

/// The revisions the Parquet metadata records at or after `since`.
fn parquet_revisions_since(path: &Path, since: &str) -> HashSet<u64> {
    parquet_files(path)
        .iter()
        .flat_map(|p| revisionchest_meta::parquet::revisions_since(p, since))
        .collect()
}

/// Latest revision timestamp across a Parquet file, or every `*.parquet` in a directory.
fn latest_parquet_timestamp(path: &Path) -> Option<String> {
    use revisionchest_meta::parquet::latest_timestamp;
    if path.is_dir() {
        std::fs::read_dir(path)
            .ok()?
            .filter_map(Result::ok)
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|x| x == "parquet"))
            .filter_map(|p| latest_timestamp(&p))
            .max()
    } else {
        latest_timestamp(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parquet_target_in_a_directory_never_reuses_a_file() {
        let dir = tempfile::tempdir().unwrap();
        let mut seen = HashSet::new();
        for _ in 0..3 {
            // Three runs within the same second: each must get a new file.
            let t = parquet_target(dir.path());
            assert!(!t.exists(), "{} already exists", t.display());
            assert_eq!(t.parent(), Some(dir.path()));
            assert_eq!(t.extension().and_then(|e| e.to_str()), Some("parquet"));
            std::fs::write(&t, b"").unwrap();
            assert!(seen.insert(t));
        }
    }

    #[test]
    fn parquet_target_for_a_file_is_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("meta.parquet");
        assert_eq!(parquet_target(&file), file);
    }
}
