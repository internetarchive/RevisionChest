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

/// Parquet target for a run: a file as given, or a per-run file inside a
/// directory (`sync` keeps every run's metadata that way).
fn parquet_target(path: &Path) -> PathBuf {
    if path.is_dir() {
        path.join(format!(
            "revisionchest-{}.parquet",
            Local::now().format("%Y%m%dT%H%M%S")
        ))
    } else {
        path.to_path_buf()
    }
}

fn make_sink(meta: &MetaArgs) -> io::Result<Box<dyn MetaSink>> {
    let mut sinks: Vec<Box<dyn MetaSink>> = Vec::new();
    if let Some(p) = &meta.parquet {
        let target = parquet_target(p);
        if let Some(parent) = target.parent().filter(|d| !d.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent)?;
        }
        sinks.push(Box::new(
            ParquetSink::create(&target).map_err(io::Error::other)?,
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
                let files = build::list_dumps(dir)?;
                let total = files.len();
                let started = AtomicUsize::new(0);
                let failures: Vec<(PathBuf, io::Error)> = files
                    .into_par_iter()
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
                }
            }

            let sink = make_sink(&s.meta)?;
            let (meta, handle) = revisionchest_meta::spawn(sink, 10_000);
            meta.send(MetaEvent::SiteInfo {
                domain: s.domain.clone(),
                language_code: None,
            });
            let result = sync::run_sync(&s, &meta, latest, already);
            drop(meta);
            let sink_result = handle.join();
            result?;
            sink_result.map_err(io::Error::other)?;
        }
    }
    Ok(())
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
