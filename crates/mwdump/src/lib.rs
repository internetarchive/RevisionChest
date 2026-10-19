//! Streaming reader for MediaWiki XML dumps.
//!
//! A dump (`pages-meta-history`, `pages-articles`, a `Special:Export` file,
//! anything `dumpBackup.php` writes in export format 0.10/0.11) is read one
//! XML event at a time and turned into a stream of [`Event`]s: the site
//! information, then for every page a [`Event::PageStart`], each of its
//! [`Event::Revision`]s as they are encountered, and an [`Event::PageEnd`].
//! Nothing larger than one revision is ever held in memory, which is what
//! lets a full-history dump of a page with a hundred thousand revisions be
//! read on an ordinary machine.
//!
//! ```no_run
//! use mwdump::{open, Event};
//!
//! for event in open(std::path::Path::new("enwiki-pages-meta-history1.xml.bz2"))? {
//!     match event? {
//!         Event::PageStart(page) => println!("page {} {}", page.id, page.title),
//!         Event::Revision(rev) => println!("  rev {} ({} bytes)", rev.id, rev.text.len()),
//!         Event::PageEnd { .. } | Event::SiteInfo(_) => {}
//!     }
//! }
//! # Ok::<(), mwdump::DumpError>(())
//! ```
//!
//! [`open`] picks the decompressor from the file extension (`.bz2`, `.gz`,
//! `.7z`, or none); every path streams, including 7z. [`DumpReader::new`]
//! takes any `BufRead` when the caller has its own source.

#![forbid(unsafe_code)]

mod open;
mod reader;

pub use open::{open, open_path};
pub use reader::{DumpReader, Event, Namespace, PageHeader, Revision, SiteInfo};

/// Why a dump could not be read.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DumpError {
    /// Reading the underlying file or stream failed.
    #[error("read: {0}")]
    Io(#[from] std::io::Error),
    /// The XML is malformed.
    #[error("xml at byte {position}: {source}")]
    Xml {
        /// Byte offset into the (decompressed) document.
        position: u64,
        #[source]
        source: quick_xml::Error,
    },
    /// The XML is well-formed but is not a dump this reader understands.
    #[error("malformed dump: {0}")]
    Malformed(String),
    /// The 7z archive could not be opened or read.
    #[error("7z: {0}")]
    SevenZ(String),
}
