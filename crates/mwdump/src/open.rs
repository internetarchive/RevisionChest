//! Opening dump files: decompression chosen by extension, every path streaming.

use std::fs::File;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::path::Path;
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::thread;

use crate::{DumpError, DumpReader};

/// A boxed, decompressed, buffered byte stream.
pub type DumpStream = Box<dyn BufRead + Send>;

const BUF: usize = 1 << 20;

/// Opens a dump file and returns its decompressed byte stream.
///
/// `.bz2` (multi-stream, as Wikimedia writes them), `.gz` (multi-member),
/// `.7z` (the archive's first file, decompressed on a worker thread and
/// streamed through a bounded channel, so the dump is never held in memory
/// whole) and anything else as plain XML.
pub fn open_path(path: &Path) -> Result<DumpStream, DumpError> {
    let name = path.to_string_lossy();
    if name.ends_with(".7z") {
        return Ok(Box::new(BufReader::with_capacity(
            BUF,
            SevenZStream::open(path)?,
        )));
    }
    let file = File::open(path)?;
    Ok(if name.ends_with(".bz2") {
        Box::new(BufReader::with_capacity(
            BUF,
            bzip2::read::MultiBzDecoder::new(file),
        ))
    } else if name.ends_with(".gz") {
        Box::new(BufReader::with_capacity(
            BUF,
            flate2::read::MultiGzDecoder::new(file),
        ))
    } else {
        Box::new(BufReader::with_capacity(BUF, file))
    })
}

/// Opens a dump file as an event reader. See [`open_path`] for the formats.
pub fn open(path: &Path) -> Result<DumpReader<DumpStream>, DumpError> {
    Ok(DumpReader::new(open_path(path)?))
}

/// The first file of a 7z archive, read on a worker thread.
///
/// `sevenz-rust` hands the entry's reader to a callback rather than returning
/// it, so the decompression runs on its own thread and the bytes cross a
/// bounded channel of 1 MiB chunks; back-pressure keeps at most a few
/// chunks in flight.
struct SevenZStream {
    rx: Receiver<io::Result<Vec<u8>>>,
    chunk: Vec<u8>,
    pos: usize,
    finished: bool,
}

impl SevenZStream {
    fn open(path: &Path) -> Result<Self, DumpError> {
        let mut archive = sevenz_rust::SevenZReader::open(path, sevenz_rust::Password::empty())
            .map_err(|e| DumpError::SevenZ(e.to_string()))?;
        let (tx, rx) = sync_channel::<io::Result<Vec<u8>>>(4);
        thread::Builder::new()
            .name("mwdump-7z".into())
            .spawn(move || {
                let tx2 = tx.clone();
                let result = archive.for_each_entries(|entry, reader| {
                    if entry.is_directory() {
                        return Ok(true);
                    }
                    let mut sink = ChunkSender {
                        tx: tx2.clone(),
                        buf: Vec::with_capacity(BUF),
                    };
                    io::copy(reader, &mut sink)?;
                    sink.flush()?;
                    Ok(false) // the first file only
                });
                if let Err(e) = result {
                    // The consumer may already be gone; ignore a failed send.
                    let _ = tx.send(Err(io::Error::other(e.to_string())));
                }
                // Dropping `tx` closes the channel: end of stream.
            })
            .map_err(DumpError::Io)?;
        Ok(Self {
            rx,
            chunk: Vec::new(),
            pos: 0,
            finished: false,
        })
    }
}

impl Read for SevenZStream {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        loop {
            if self.pos < self.chunk.len() {
                let n = (self.chunk.len() - self.pos).min(out.len());
                out[..n].copy_from_slice(&self.chunk[self.pos..self.pos + n]);
                self.pos += n;
                return Ok(n);
            }
            if self.finished {
                return Ok(0);
            }
            match self.rx.recv() {
                Ok(Ok(chunk)) => {
                    self.chunk = chunk;
                    self.pos = 0;
                }
                Ok(Err(e)) => {
                    self.finished = true;
                    return Err(e);
                }
                Err(_) => {
                    self.finished = true;
                    return Ok(0);
                }
            }
        }
    }
}

/// `Write` end of the channel: accumulates a chunk and sends it when full.
struct ChunkSender {
    tx: SyncSender<io::Result<Vec<u8>>>,
    buf: Vec<u8>,
}

impl Write for ChunkSender {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.buf.extend_from_slice(data);
        if self.buf.len() >= BUF {
            self.flush()?;
        }
        Ok(data.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let chunk = std::mem::replace(&mut self.buf, Vec::with_capacity(BUF));
        self.tx
            .send(Ok(chunk))
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "7z consumer went away"))
    }
}
