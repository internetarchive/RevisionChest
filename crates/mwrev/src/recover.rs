//! Recovering a bundle that was being appended to when its writer stopped.
//!
//! An appending writer ([`BundleWriter::append`](crate::BundleWriter::append))
//! adds one zstd frame per batch. A caller that records how long the bundle was
//! after its last batch it committed (compressed and decompressed) can, after a
//! crash, read what was written past that point with [`read_tail`]: the
//! revisions in the complete frames, at the offsets the writer gave them, so
//! their metadata can be written again without fetching or appending anything.
//! A frame cut off part way is not counted; [`truncate`] cuts the bundle back
//! to its complete frames and rewrites the length sidecar.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;

use crate::writer::sidecar_path;
use crate::{Location, RevisionHeader};

/// What follows a committed prefix of a bundle.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Tail {
    /// Compressed length of the bundle through its last complete frame.
    pub compressed_len: u64,
    /// Decompressed length of the bundle through its last complete frame.
    pub decompressed_len: u64,
    /// The revisions in the complete frames past the prefix, in order, with
    /// where their text lies.
    pub revisions: Vec<(RevisionHeader, Location)>,
    /// Whether bytes follow the last complete frame (a frame cut off part way).
    pub incomplete: bool,
}

/// Reads the complete zstd frames that follow byte `compressed_from` of a
/// bundle whose first `compressed_from` bytes decompress to `decompressed_from`
/// bytes.
///
/// # Errors
///
/// When the file cannot be read, is shorter than `compressed_from`, or a
/// complete frame does not decode or holds a malformed header.
pub fn read_tail(path: &Path, compressed_from: u64, decompressed_from: u64) -> io::Result<Tail> {
    let mut file = File::open(path)?;
    let len = file.metadata()?.len();
    if len < compressed_from {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "{} is {len} bytes, shorter than the {compressed_from} committed",
                path.display()
            ),
        ));
    }
    file.seek(SeekFrom::Start(compressed_from))?;
    let mut rest = Vec::new();
    file.read_to_end(&mut rest)?;

    // The complete frames: each one's size is in its header and block headers.
    let mut complete = 0usize;
    while complete < rest.len() {
        match zstd::zstd_safe::find_frame_compressed_size(&rest[complete..]) {
            Ok(n) if n > 0 && complete + n <= rest.len() => complete += n,
            _ => break,
        }
    }
    let text = zstd::stream::decode_all(&rest[..complete])?;

    let mut revisions = Vec::new();
    let mut pos = 0usize;
    let mut current: Option<(RevisionHeader, Location)> = None;
    while pos < text.len() {
        let end = text[pos..]
            .iter()
            .position(|&b| b == b'\n')
            .map_or(text.len(), |i| pos + i + 1);
        let line = &text[pos..end];
        if line.first() == Some(&b'#') {
            if let Some(done) = current.take() {
                revisions.push(done);
            }
            let header = std::str::from_utf8(line)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
                .trim_end_matches('\n');
            let header = RevisionHeader::parse(header)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let location = Location {
                offset_begin: decompressed_from + end as u64,
                length: 0,
            };
            current = Some((header, location));
        } else if let Some((_, location)) = current.as_mut() {
            location.length += line.len() as u64;
        }
        pos = end;
    }
    if let Some(done) = current {
        revisions.push(done);
    }

    Ok(Tail {
        compressed_len: compressed_from + complete as u64,
        decompressed_len: decompressed_from + text.len() as u64,
        revisions,
        incomplete: complete < rest.len(),
    })
}

/// Cuts a bundle back to `compressed_len` bytes and records `decompressed_len`
/// in its sidecar, so the next [`BundleWriter::append`](crate::BundleWriter::append)
/// continues from there.
///
/// # Errors
///
/// When the file or its sidecar cannot be written.
pub fn truncate(path: &Path, compressed_len: u64, decompressed_len: u64) -> io::Result<()> {
    let file = OpenOptions::new().write(true).open(path)?;
    file.set_len(compressed_len)?;
    file.sync_all()?;
    std::fs::write(sidecar_path(path), format!("{decompressed_len}\n"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BundleWriter;

    fn header(rev_id: u64, text_hidden: bool) -> RevisionHeader {
        RevisionHeader {
            page_id: 7,
            ns: 0,
            rev_id,
            parent_rev_id: None,
            timestamp: "2026-10-19T00:00:00Z".into(),
            user_id: 1,
            text_hidden,
        }
    }

    #[test]
    fn the_tail_gives_the_offsets_the_writer_gave() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("b.mwrev.zst");
        let mut w = BundleWriter::append(&path).unwrap();
        w.write_revision(&header(1, false), "committed").unwrap();
        w.finish().unwrap();
        let committed = std::fs::metadata(&path).unwrap().len();
        let committed_d: u64 = std::fs::read_to_string(sidecar_path(&path))
            .unwrap()
            .trim()
            .parse()
            .unwrap();

        let mut expected = Vec::new();
        let mut w = BundleWriter::append(&path).unwrap();
        expected.push(w.write_revision(&header(2, false), "two\nlines").unwrap());
        expected.push(w.write_revision(&header(3, true), "").unwrap());
        w.finish().unwrap();
        let mut w = BundleWriter::append(&path).unwrap();
        expected.push(
            w.write_revision(&header(4, false), "a later frame")
                .unwrap(),
        );
        w.finish().unwrap();

        let tail = read_tail(&path, committed, committed_d).unwrap();
        assert!(!tail.incomplete);
        assert_eq!(tail.compressed_len, std::fs::metadata(&path).unwrap().len());
        let ids: Vec<u64> = tail.revisions.iter().map(|(h, _)| h.rev_id).collect();
        assert_eq!(ids, [2, 3, 4]);
        assert!(tail.revisions[1].0.text_hidden);
        let got: Vec<Location> = tail.revisions.iter().map(|(_, l)| *l).collect();
        assert_eq!(got, expected);
        for (_, l) in &tail.revisions[..1] {
            let bytes = crate::read_at(&path, l.offset_begin, l.length).unwrap();
            assert_eq!(bytes, b" two\n lines\n");
        }
    }

    #[test]
    fn a_cut_off_frame_is_not_counted_and_is_truncated() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("b.mwrev.zst");
        let mut w = BundleWriter::append(&path).unwrap();
        w.write_revision(&header(1, false), "committed").unwrap();
        w.finish().unwrap();
        let committed = std::fs::metadata(&path).unwrap().len();
        let committed_d = std::fs::read_to_string(sidecar_path(&path))
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        let mut w = BundleWriter::append(&path).unwrap();
        w.write_revision(&header(2, false), "complete").unwrap();
        w.finish().unwrap();
        let good = std::fs::metadata(&path).unwrap().len();
        // A frame cut off part way: a copy of the last frame, short of its last bytes.
        let bytes = std::fs::read(&path).unwrap();
        let frame = &bytes[usize::try_from(committed).unwrap()..];
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        std::io::Write::write_all(&mut f, &frame[..frame.len() - 3]).unwrap();
        drop(f);

        let tail = read_tail(&path, committed, committed_d).unwrap();
        assert!(tail.incomplete);
        assert_eq!(tail.compressed_len, good);
        assert_eq!(tail.revisions.len(), 1);
        truncate(&path, tail.compressed_len, tail.decompressed_len).unwrap();
        let ids: Vec<u64> = crate::open_bundle(&path)
            .unwrap()
            .map(|r| r.unwrap().0.rev_id)
            .collect();
        assert_eq!(ids, [1, 2]);
        // and appending continues at the right offset
        let mut w = BundleWriter::append(&path).unwrap();
        let l = w.write_revision(&header(3, false), "after").unwrap();
        w.finish().unwrap();
        assert_eq!(
            crate::read_at(&path, l.offset_begin, l.length).unwrap(),
            b" after\n"
        );
    }
}
