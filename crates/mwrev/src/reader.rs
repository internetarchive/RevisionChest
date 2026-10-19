//! Reading bundles back.

use std::fs::File;
use std::io::{self, BufRead, BufReader, Read};
use std::path::Path;

use zstd::stream::read::Decoder;

use crate::RevisionHeader;

/// Streams `(header, text)` pairs out of a bundle. All zstd frames are read.
///
/// Text lines are joined with `\n` and the single leading space of each is
/// removed, which inverts [`BundleWriter::write_revision`](crate::BundleWriter::write_revision)
/// up to the trailing newline the writer does not keep. Lines that are neither
/// a header nor space-prefixed are skipped, as the Python reader does.
pub struct BundleReader<R: BufRead> {
    lines: io::Lines<R>,
    pending: Option<RevisionHeader>,
    done: bool,
}

impl BundleReader<BufReader<Decoder<'static, BufReader<File>>>> {
    /// Opens a `.mwrev.zst` file.
    pub fn open(path: &Path) -> io::Result<Self> {
        let dec = Decoder::new(File::open(path)?)?;
        Ok(Self::from_decompressed(BufReader::with_capacity(
            1 << 20,
            dec,
        )))
    }
}

impl<R: BufRead> BundleReader<R> {
    /// Wraps an already decompressed stream.
    pub fn from_decompressed(inner: R) -> Self {
        Self {
            lines: inner.lines(),
            pending: None,
            done: false,
        }
    }

    fn read_next(&mut self) -> io::Result<Option<(RevisionHeader, String)>> {
        // Find the header of the next revision (the one left over from the
        // previous call, or the first header line in the stream).
        let header = match self.pending.take() {
            Some(h) => h,
            None => loop {
                match self.lines.next() {
                    None => return Ok(None),
                    Some(line) => {
                        let line = line?;
                        if line.starts_with('#') {
                            break RevisionHeader::parse(&line)
                                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                        }
                    }
                }
            },
        };
        let mut text = String::new();
        let mut first = true;
        loop {
            match self.lines.next() {
                None => break,
                Some(line) => {
                    let line = line?;
                    if let Some(rest) = line.strip_prefix(' ') {
                        if !first {
                            text.push('\n');
                        }
                        text.push_str(rest);
                        first = false;
                    } else if line.starts_with('#') {
                        self.pending = Some(
                            RevisionHeader::parse(&line)
                                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
                        );
                        break;
                    }
                    // anything else is skipped
                }
            }
        }
        Ok(Some((header, text)))
    }
}

impl<R: BufRead> Iterator for BundleReader<R> {
    type Item = io::Result<(RevisionHeader, String)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        match self.read_next() {
            Ok(Some(item)) => Some(Ok(item)),
            Ok(None) => {
                self.done = true;
                None
            }
            Err(e) => {
                self.done = true;
                Some(Err(e))
            }
        }
    }
}

/// Opens a bundle file as a [`BundleReader`].
pub fn open_bundle(
    path: &Path,
) -> io::Result<BundleReader<BufReader<Decoder<'static, BufReader<File>>>>> {
    BundleReader::open(path)
}

/// Reads the decompressed bytes of a bundle at `offset_begin..offset_begin+length`
/// by decompressing from the start; a convenience for tooling and tests, not a
/// random-access path for production.
pub fn read_at(path: &Path, offset_begin: u64, length: u64) -> io::Result<Vec<u8>> {
    let mut dec = Decoder::new(File::open(path)?)?;
    io::copy(&mut (&mut dec).take(offset_begin), &mut io::sink())?;
    let mut out = Vec::with_capacity(length as usize);
    dec.take(length).read_to_end(&mut out)?;
    Ok(out)
}
