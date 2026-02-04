mod models;
mod args;
mod utils;
mod db;
mod processor;

use std::fs;
use std::io;
use std::path::{PathBuf};
use std::thread;
use clap::Parser;
use rayon::prelude::*;

use crate::models::DbMessage;
use crate::args::Args;
use crate::db::db_worker;
use crate::processor::process_file;

fn main() -> io::Result<()> {
    let args = Args::parse();
    let (db_tx, db_rx) = crossbeam_channel::bounded(10000);
    let mut db_path = args.db.clone();

    if let Some(ref output_dir) = args.output_dir {
        if db_path.is_relative() {
            db_path = output_dir.join(db_path);
        }
    }

    let db_thread = thread::spawn(move || {
        db_worker(db_rx, db_path);
    });

    let skip_siteinfo = if let Some(domain) = args.domain.clone() {
        db_tx.send(DbMessage::SiteInfo { domain }).ok();
        true
    } else {
        false
    };

    if let Some(input_dir) = args.input_dir {
        let output_dir = args.output_dir.as_deref().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "-o is required when using -d")
        })?;

        let mut entries: Vec<PathBuf> = fs::read_dir(input_dir)?
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .filter(|path| {
                path.is_file() && (path.extension() == Some("bz2".as_ref()) || path.extension() == Some("7z".as_ref()))
            })
            .collect();

        entries.sort();

        let total_files = entries.len();
        let started_count = std::sync::atomic::AtomicUsize::new(0);

        entries.into_par_iter().for_each(|path| {
            let file_index = started_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            if let Err(e) = process_file(&path, Some(output_dir), &args.namespace, &db_tx, file_index, total_files, skip_siteinfo) {
                eprintln!("Error processing {:?}: {}", path, e);
            }
        });
    } else if let Some(input_path) = args.input {
        process_file(
            &input_path,
            args.output_dir.as_deref(),
            &args.namespace,
            &db_tx,
            1,
            1,
            skip_siteinfo,
        )?;
    } else {
        eprintln!("Usage: RevisionChest <wikipedia_dump.xml.bz2|7z> OR -d <input_dir> -o <output_dir>");
        std::process::exit(1);
    }

    drop(db_tx);
    db_thread.join().expect("Database thread panicked");

    Ok(())
}
