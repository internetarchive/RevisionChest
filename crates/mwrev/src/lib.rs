//! The `.mwrev.zst` revision bundle format.
//!
//! A bundle is a zstd-compressed text stream. Each revision is one header
//! line followed by its text with every line prefixed by a single space:
//!
//! ```text
//! # page_id=101 ns=0 rev_id=1001 parent_rev_id= timestamp=2004-01-01T00:00:00Z user_id=7
//!  '''Example Island''' is an island.
//!
//!  == References ==
//! # page_id=101 ns=0 rev_id=1002 parent_rev_id=1001 timestamp=2005-06-15T12:00:00Z user_id=0
//!  …
//! ```
//!
//! `parent_rev_id` is empty for a page's first revision; `user_id` is `0`
//! when the contributor is anonymous or hidden. A revision whose text the
//! source hid (revision deletion, suppression) has no text lines and a
//! trailing ` text_hidden=1` on its header, so a reader can tell "hidden"
//! from "blank"; the key is absent otherwise. The text is written line by
//! line (`str::lines`), so a trailing newline is not preserved and `\r\n`
//! becomes `\n`.
//!
//! **Offsets.** A revision's location is `(bundle, offset_begin, length)`,
//! where `offset_begin` is the byte offset of the first text line *in the
//! decompressed stream of the whole bundle* and `length` the number of
//! decompressed bytes of the text lines (prefixes and newlines included).
//! A bundle may consist of several zstd frames (an appended-to bundle does);
//! offsets still count from the start of the file's decompressed content.
//! [`BundleWriter::append`] keeps that invariant through a sidecar file.
//!
//! **Compression.** Frames are written at zstd's default level with
//! long-distance matching and a 128 MiB window ([`WINDOW_LOG`] = 27), so a
//! revision can reuse the previous revisions of its page even when they are
//! megabytes long. That is zstd's default decoder limit: any zstd reader
//! decodes a bundle with default settings, and bundles written before this
//! (2 MiB window) read the same way.
//!
//! [`BundleReader`] streams a bundle back as `(header, text)` pairs; it is
//! the reference parser for this format (the Python pipeline has an
//! equivalent in `wcd_pipeline.extract`).

#![forbid(unsafe_code)]

mod header;
mod reader;
pub mod recover;
mod writer;

pub use header::{HeaderError, RevisionHeader};
pub use reader::{BundleReader, open_bundle, read_at};
pub use writer::{
    BundleWriter, Location, SIDECAR_SUFFIX, WINDOW_LOG, sidecar_path as writer_sidecar_path,
};
