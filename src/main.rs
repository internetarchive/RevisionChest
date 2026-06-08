mod models;
mod args;
mod utils;
mod db;
mod processor;
mod sync;
mod parquet_writer;

use std::fs;
use std::io;
use std::path::{PathBuf};
use std::thread;
use clap::Parser;
use rayon::prelude::*;

use crate::models::DbMessage;
use crate::args::{Args, Commands};
use crate::db::{db_worker, get_latest_timestamp};
use crate::processor::process_file;
use crate::sync::run_sync;
use crate::parquet_writer::{write_parquet_batch, merge_parquet_files, get_latest_timestamp_from_parquet, parquet_worker};

fn main() -> io::Result<()> {
    let args = Args::parse();
    
    match args.command {
        Commands::Build(build_args) => {
            if let Some(jobs) = build_args.jobs {
                rayon::ThreadPoolBuilder::new()
                    .num_threads(jobs)
                    .build_global()
                    .unwrap_or_else(|e| {
                        eprintln!("Warning: Failed to initialize thread pool with {} threads: {}", jobs, e);
                    });
            }
            let (db_tx, db_rx) = crossbeam_channel::bounded(10000);
            let mut db_path = build_args.db.clone();

            if let Some(ref output_dir) = build_args.output_dir {
                if db_path.is_relative() {
                    db_path = output_dir.join(db_path);
                }
            }

            let no_db = build_args.no_db || build_args.parquet.is_some();
            let db_thread = thread::spawn(move || {
                if no_db {
                    while let Ok(_) = db_rx.recv() {}
                } else {
                    db_worker(db_rx, db_path);
                }
            });

            let skip_siteinfo = if let Some(domain) = build_args.domain.clone() {
                db_tx.send(DbMessage::SiteInfo { 
                    domain,
                    language_code: None, // Could potentially be inferred from domain if we wanted
                }).ok();
                true
            } else {
                false
            };

            if let Some(input_dir) = build_args.input_dir {
                let output_dir = build_args.output_dir.as_deref().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "-o is required when using -d")
                })?;

                let mut entries: Vec<PathBuf> = fs::read_dir(input_dir)?
                    .filter_map(|entry| entry.ok())
                    .map(|entry| entry.path())
                    .filter(|path| {
                        path.is_file() && (
                            path.extension() == Some("bz2".as_ref()) || 
                            path.extension() == Some("7z".as_ref()) ||
                            path.extension() == Some("xml".as_ref())
                        )
                    })
                    .collect();

                entries.sort();

                let total_files = entries.len();
                let started_count = std::sync::atomic::AtomicUsize::new(0);

                let parquet_results: Vec<PathBuf> = entries.into_par_iter().enumerate().map(|(idx, path)| {
                    let file_index = started_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                    let messages = match process_file(&path, Some(output_dir), &build_args.namespace, &db_tx, file_index, total_files, skip_siteinfo) {
                        Ok(m) => m,
                        Err(e) => {
                            eprintln!("Error processing {:?}: {}", path, e);
                            Vec::new()
                        }
                    };
                    db_tx.send(DbMessage::Finalize).ok();

                    if let Some(ref parquet_path) = build_args.parquet {
                        let mut thread_parquet = parquet_path.clone();
                        thread_parquet.set_extension(format!("{}.tmp.parquet", idx));
                        if let Err(e) = write_parquet_batch(&thread_parquet, &messages) {
                            eprintln!("Error writing parquet batch for {:?}: {}", path, e);
                        }
                        Some(thread_parquet)
                    } else {
                        None
                    }
                }).filter_map(|p| p).collect();

                if let Some(ref parquet_path) = build_args.parquet {
                    if let Err(e) = merge_parquet_files(&parquet_results, parquet_path) {
                        eprintln!("Error merging parquet files: {}", e);
                    }
                    for p in parquet_results {
                        fs::remove_file(p).ok();
                    }
                }
            } else if let Some(input_path) = build_args.input {
                let messages = process_file(
                    &input_path,
                    build_args.output_dir.as_deref(),
                    &build_args.namespace,
                    &db_tx,
                    1,
                    1,
                    skip_siteinfo,
                )?;
                db_tx.send(DbMessage::Finalize).ok();

                if let Some(ref parquet_path) = build_args.parquet {
                    if let Err(e) = write_parquet_batch(parquet_path, &messages) {
                        eprintln!("Error writing parquet file: {}", e);
                    }
                }
            } else {
                eprintln!("Usage: RevisionChest build <wikipedia_dump.xml.bz2|7z|xml> OR build -d <input_dir> -o <output_dir>");
                std::process::exit(1);
            }

            drop(db_tx.clone());
            db_tx.send(DbMessage::Finalize).ok();
            drop(db_tx);
            db_thread.join().expect("Database thread panicked");
        }
        Commands::Sync(sync_args) => {
            if let Some(jobs) = sync_args.jobs {
                rayon::ThreadPoolBuilder::new()
                    .num_threads(jobs)
                    .build_global()
                    .unwrap_or_else(|e| {
                        eprintln!("Warning: Failed to initialize thread pool with {} threads: {}", jobs, e);
                    });
            }
            let (db_tx, db_rx) = crossbeam_channel::bounded(10000);
            let mut db_path = sync_args.db.clone();

            if db_path.is_relative() {
                db_path = sync_args.output_dir.join(db_path);
            }

            let mut last_ts = get_latest_timestamp(db_path.clone());
            if last_ts.is_none() {
                if let Some(ref p_path) = sync_args.parquet {
                    last_ts = get_latest_timestamp_from_parquet(p_path);
                }
            }

            let no_db = sync_args.no_db || sync_args.parquet.is_some();
            let parquet_path = sync_args.parquet.clone();
            let db_path_for_worker = db_path.clone();
            let db_thread = thread::spawn(move || {
                if no_db {
                    if let Some(path) = parquet_path {
                        parquet_worker(db_rx, path);
                    } else {
                        while let Ok(_) = db_rx.recv() {}
                    }
                } else {
                    db_worker(db_rx, db_path_for_worker);
                }
            });

            db_tx.send(DbMessage::SiteInfo { 
                domain: sync_args.domain.clone(),
                language_code: None,
            }).ok();

            if let Err(e) = run_sync(sync_args, db_tx.clone(), last_ts) {
                eprintln!("Error during sync: {}", e);
            }

            db_tx.send(DbMessage::Finalize).ok();
            drop(db_tx);
            db_thread.join().expect("Database thread panicked");
        }
    }

    Ok(())
}
