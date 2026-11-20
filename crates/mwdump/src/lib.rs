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
//!         _ => {} // the site information, page ends, and any later kind of event
//!     }
//! }
//! # Ok::<(), mwdump::DumpError>(())
//! ```
//!
//! [`open`] picks the decompressor from the file extension (`.bz2`, `.gz`,
//! `.7z`, or none); every path streams, including 7z. [`DumpReader::new`]
//! takes any `BufRead` when the caller has its own source.
//! [`DumpReader::with_namespaces`] drops pages outside a namespace list
//! without decoding their revisions.
//!
//! [`DumpReader::pages`] collects the events into whole pages, each with all
//! its revisions, for callers that work a page at a time.
//!
//! A revision carries what the export hid (text, comment, contributor) and,
//! for a dump in schema 0.11, its further content slots. [`Writer`] writes
//! the same shape back out, page by page and revision by revision, as
//! MediaWiki 1.43's `XmlDumpWriter` writes it.

#![forbid(unsafe_code)]

mod open;
mod pages;
mod reader;
mod writer;

pub use open::{open, open_path};
pub use pages::{Page, Pages};
pub use reader::{DumpReader, Event, Namespace, PageHeader, Revision, SiteInfo, Slot};
pub use writer::Writer;

/// The export schema version [`Writer`] writes.
pub const SCHEMA_VERSION: &str = "0.11";
/// The XML namespace of that schema.
pub const SCHEMA_NAMESPACE: &str = "http://www.mediawiki.org/xml/export-0.11/";

/// MediaWiki's `sha1` of a revision's text: the SHA-1 digest in base 36, left-padded with
/// zeros to 31 characters, as `Wikimedia\base_convert` writes it (`rev_sha1`).
#[must_use]
pub fn sha1_base36(text: &str) -> String {
    use sha1::{Digest as _, Sha1};
    base36(&Sha1::digest(text.as_bytes()))
}

/// A big-endian byte string as a base-36 number, 31 digits wide.
fn base36(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut number: Vec<u8> = bytes.to_vec();
    let mut out = Vec::with_capacity(31);
    while number.iter().any(|&b| b != 0) {
        let mut carry = 0u32;
        for b in &mut number {
            let value = (carry << 8) | u32::from(*b);
            *b = u8::try_from(value / 36).unwrap_or(0);
            carry = value % 36;
        }
        out.push(DIGITS[carry as usize]);
    }
    while out.len() < 31 {
        out.push(b'0');
    }
    out.reverse();
    String::from_utf8(out).unwrap_or_default()
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha1_as_mediawiki_writes_it() {
        // Librarybase's `User:Database import`, revision 1096.
        assert_eq!(
            sha1_base36(
                "Edits attributed to this account have been imported using BatchIngestion."
            ),
            "jvd0zndg8rmpjno1ro5g8a78mno8uu3"
        );
        assert_eq!(sha1_base36("").len(), 31);
        assert_eq!(base36(&[0; 20]), "0".repeat(31));
    }
}
