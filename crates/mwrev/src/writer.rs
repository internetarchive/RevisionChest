//! Writing bundles.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use zstd::stream::write::Encoder;

use crate::RevisionHeader;

/// Suffix of the sidecar that records a bundle's decompressed length, so that
/// [`BundleWriter::append`] can continue the offset count without
/// decompressing the bundle.
pub const SIDECAR_SUFFIX: &str = ".len";

/// Where a revision's text landed in a bundle (decompressed byte offsets).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Location {
    /// Offset of the first text line, counted from the start of the bundle's
    /// decompressed content.
    pub offset_begin: u64,
    /// Decompressed bytes of the text lines (space prefixes and newlines included).
    pub length: u64,
}

/// Writes revisions to a zstd stream (or, with [`BundleWriter::plain`], an
/// uncompressed one), counting decompressed bytes.
pub struct BundleWriter<W: Write> {
    enc: Option<Counting<Sink<W>>>,
    /// Decompressed bytes that preceded this writer's frame (append mode).
    base: u64,
    /// Sidecar to update on finish, with the total decompressed length.
    sidecar: Option<PathBuf>,
}

impl BundleWriter<File> {
    /// Creates (truncates) a bundle file.
    pub fn create(path: &Path) -> io::Result<Self> {
        Self::new(File::create(path)?)
    }

    /// Opens a bundle file for appending a new zstd frame.
    ///
    /// Offsets continue from the bundle's existing decompressed length, read
    /// from the `<path>.len` sidecar if present and otherwise measured by
    /// decompressing the file once. On [`finish`](Self::finish) the sidecar
    /// is rewritten with the new total.
    pub fn append(path: &Path) -> io::Result<Self> {
        let sidecar = sidecar_path(path);
        let base = match std::fs::read_to_string(&sidecar) {
            Ok(s) => s.trim().parse::<u64>().map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("{}: {e}", sidecar.display()),
                )
            })?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => match File::open(path) {
                Ok(f) => decompressed_len(f)?,
                Err(e) if e.kind() == io::ErrorKind::NotFound => 0,
                Err(e) => return Err(e),
            },
            Err(e) => return Err(e),
        };
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let mut w = Self::new(file)?;
        w.base = base;
        w.sidecar = Some(sidecar);
        Ok(w)
    }
}

impl<W: Write> BundleWriter<W> {
    /// Wraps any writer; the zstd frame is finished by [`finish`](Self::finish)
    /// (or on drop, with errors ignored).
    pub fn new(inner: W) -> io::Result<Self> {
        let enc = Encoder::new(inner, 0)?; // 0 = zstd's default level
        Ok(Self {
            enc: Some(Counting {
                inner: Sink::Zstd(enc),
                offset: 0,
            }),
            base: 0,
            sidecar: None,
        })
    }

    /// Writes the bundle text uncompressed (for stdout and tests). Offsets are
    /// then plain byte offsets into the output.
    pub fn plain(inner: W) -> Self {
        Self {
            enc: Some(Counting {
                inner: Sink::Plain(inner),
                offset: 0,
            }),
            base: 0,
            sidecar: None,
        }
    }

    /// Decompressed bytes written so far, offsets of any preceding frames included.
    #[must_use]
    pub fn offset(&self) -> u64 {
        self.base + self.enc.as_ref().map_or(0, |e| e.offset)
    }

    /// Writes one revision: the header line, then each line of `text`
    /// prefixed with a space. Returns where the text lines landed.
    pub fn write_revision(&mut self, header: &RevisionHeader, text: &str) -> io::Result<Location> {
        let enc = self.enc.as_mut().expect("writer already finished");
        writeln!(enc, "{header}")?;
        let offset_begin = self.base + enc.offset;
        for line in text.lines() {
            enc.write_all(b" ")?;
            enc.write_all(line.as_bytes())?;
            enc.write_all(b"\n")?;
        }
        let length = self.base + enc.offset - offset_begin;
        Ok(Location {
            offset_begin,
            length,
        })
    }

    /// Finishes the zstd frame, flushes, updates the sidecar (append mode) and
    /// returns the underlying writer.
    pub fn finish(mut self) -> io::Result<W> {
        let counting = self.enc.take().expect("writer already finished");
        let total = self.base + counting.offset;
        let mut inner = match counting.inner {
            Sink::Zstd(enc) => enc.finish()?,
            Sink::Plain(w) => w,
        };
        inner.flush()?;
        if let Some(sidecar) = self.sidecar.take() {
            std::fs::write(&sidecar, format!("{total}\n"))?;
        }
        Ok(inner)
    }
}

impl<W: Write> Drop for BundleWriter<W> {
    fn drop(&mut self) {
        if let Some(mut counting) = self.enc.take() {
            if let Sink::Zstd(enc) = &mut counting.inner {
                let _ = enc.do_finish();
            }
        }
    }
}

/// Path of the sidecar for a bundle.
#[must_use]
pub fn sidecar_path(bundle: &Path) -> PathBuf {
    let mut s = bundle.as_os_str().to_owned();
    s.push(SIDECAR_SUFFIX);
    PathBuf::from(s)
}

/// Decompressed length of an existing bundle (all frames).
fn decompressed_len<R: Read>(r: R) -> io::Result<u64> {
    let mut dec = zstd::stream::read::Decoder::new(r)?;
    io::copy(&mut dec, &mut io::sink())
}

enum Sink<W: Write> {
    Zstd(Encoder<'static, W>),
    Plain(W),
}

impl<W: Write> Write for Sink<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Sink::Zstd(e) => e.write(buf),
            Sink::Plain(w) => w.write(buf),
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        match self {
            Sink::Zstd(e) => e.flush(),
            Sink::Plain(w) => w.flush(),
        }
    }
}

struct Counting<W: Write> {
    inner: W,
    offset: u64,
}

impl<W: Write> Write for Counting<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.offset += n as u64;
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}
