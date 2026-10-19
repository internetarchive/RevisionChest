//! Where RevisionChest records what it wrote.
//!
//! Reading a dump produces a stream of [`MetaEvent`]s — the site, each page's
//! title and namespace, and for each revision the bundle it landed in with
//! its offset and length. A [`MetaSink`] consumes that stream:
//!
//! - [`parquet::ParquetSink`] writes one Parquet file (`type, page_id, ns,
//!   title, rev_id, parent_rev_id, file_path, offset_begin, length,
//!   timestamp`), the hand-off to a pipeline's own loader;
//! - [`postgres::PostgresSink`] (feature `postgres`) writes straight into the
//!   Wikipedia Citations Database schema: `domains`, `revision_bundles`,
//!   `revisions`, `page_metadata` and the article `web_resources` rows.
//! - [`NullSink`] discards everything.
//!
//! Sinks run on their own thread behind a bounded channel ([`spawn`]), so a
//! slow database never stalls the dump readers for long and a fast one is
//! fed in batches.

#![forbid(unsafe_code)]

pub mod timestamp;

#[cfg(feature = "parquet")]
pub mod parquet;
#[cfg(feature = "postgres")]
pub mod postgres;

use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::thread::{self, JoinHandle};

/// One fact about what was processed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetaEvent {
    /// The site the following pages belong to. Sent once, first.
    SiteInfo {
        /// The site's domain, e.g. `en.wikipedia.org`.
        domain: String,
        /// Language code if known.
        language_code: Option<String>,
    },
    /// A page's title and namespace.
    Page {
        /// Page ID.
        id: u64,
        /// Namespace number.
        ns: i64,
        /// Full title.
        title: String,
    },
    /// A revision and where its text was written.
    Revision {
        /// Revision ID.
        rev_id: u64,
        /// Parent revision ID.
        parent_rev_id: Option<u64>,
        /// Page ID.
        page_id: u64,
        /// Bundle path, relative to the output directory (`enwiki-…-p1p857.mwrev.zst`,
        /// `recentchanges/2026-10-19.mwrev.zst`).
        file_path: String,
        /// Decompressed byte offset of the text in the bundle (see `mwrev`).
        offset_begin: u64,
        /// Decompressed byte length of the text.
        length: u64,
        /// Timestamp as the source gave it.
        timestamp: String,
    },
    /// A unit of input (one dump file, one sync batch) is complete; sinks may
    /// flush.
    Finalize,
}

/// A consumer of [`MetaEvent`]s.
pub trait MetaSink: Send {
    /// Handles one event. Errors are reported and the sink keeps going where
    /// it can; a sink that cannot continue should panic on its own thread.
    fn handle(&mut self, event: MetaEvent) -> Result<(), SinkError>;
    /// Called once after the last event.
    fn finish(self: Box<Self>) -> Result<(), SinkError>;
}

/// A sink failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SinkError {
    /// I/O.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// Anything else, with context.
    #[error("{0}")]
    Other(String),
}

impl From<Box<dyn std::error::Error + Send + Sync>> for SinkError {
    fn from(e: Box<dyn std::error::Error + Send + Sync>) -> Self {
        SinkError::Other(e.to_string())
    }
}

/// A sink that discards everything.
pub struct NullSink;

impl MetaSink for NullSink {
    fn handle(&mut self, _event: MetaEvent) -> Result<(), SinkError> {
        Ok(())
    }
    fn finish(self: Box<Self>) -> Result<(), SinkError> {
        Ok(())
    }
}

/// Sending half of a running sink.
#[derive(Clone)]
pub struct MetaSender(SyncSender<MetaEvent>);

impl MetaSender {
    /// Sends an event; a sink that has gone away is ignored (its error was
    /// already reported on its own thread).
    pub fn send(&self, event: MetaEvent) {
        let _ = self.0.send(event);
    }
}

/// A running sink thread.
pub struct SinkHandle {
    join: JoinHandle<Result<(), SinkError>>,
}

impl SinkHandle {
    /// Waits for the sink to drain and finish. Call after every
    /// [`MetaSender`] clone has been dropped.
    pub fn join(self) -> Result<(), SinkError> {
        self.join
            .join()
            .unwrap_or_else(|_| Err(SinkError::Other("sink thread panicked".into())))
    }
}

/// Runs `sink` on its own thread behind a bounded channel of `capacity` events.
pub fn spawn(sink: Box<dyn MetaSink>, capacity: usize) -> (MetaSender, SinkHandle) {
    let (tx, rx): (SyncSender<MetaEvent>, Receiver<MetaEvent>) = sync_channel(capacity);
    let join = thread::Builder::new()
        .name("revisionchest-meta".into())
        .spawn(move || {
            let mut sink = sink;
            while let Ok(ev) = rx.recv() {
                if let Err(e) = sink.handle(ev) {
                    eprintln!("metadata sink: {e}");
                }
            }
            sink.finish()
        })
        .expect("spawn sink thread");
    (MetaSender(tx), SinkHandle { join })
}
