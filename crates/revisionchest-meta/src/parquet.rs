//! The Parquet sink and reader helpers.
//!
//! One row per event, `type` = `page` or `revision`; page rows carry `ns` and
//! `title`, revision rows the bundle location and timestamp. Rows are written
//! in row groups as they arrive, so memory stays bounded however large the
//! input.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;

use crate::timestamp::normalize_utc;
use crate::{MetaEvent, MetaSink, SinkError};

/// Rows per row group.
pub const ROW_GROUP: usize = 100_000;

/// The file schema.
#[must_use]
pub fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("type", DataType::Utf8, false),
        Field::new("page_id", DataType::UInt64, true),
        Field::new("ns", DataType::Int64, true),
        Field::new("title", DataType::Utf8, true),
        Field::new("rev_id", DataType::UInt64, true),
        Field::new("parent_rev_id", DataType::UInt64, true),
        Field::new("file_path", DataType::Utf8, true),
        Field::new("offset_begin", DataType::UInt64, true),
        Field::new("length", DataType::UInt64, true),
        Field::new("timestamp", DataType::Utf8, true),
    ]))
}

#[derive(Default)]
struct Columns {
    types: Vec<&'static str>,
    page_ids: Vec<Option<u64>>,
    nss: Vec<Option<i64>>,
    titles: Vec<Option<String>>,
    rev_ids: Vec<Option<u64>>,
    parent_rev_ids: Vec<Option<u64>>,
    file_paths: Vec<Option<String>>,
    offset_begins: Vec<Option<u64>>,
    lengths: Vec<Option<u64>>,
    timestamps: Vec<Option<String>>,
}

impl Columns {
    fn len(&self) -> usize {
        self.types.len()
    }

    fn push(&mut self, event: &MetaEvent) {
        match event {
            MetaEvent::Page { id, ns, title } => {
                self.types.push("page");
                self.page_ids.push(Some(*id));
                self.nss.push(Some(*ns));
                self.titles.push(Some(title.clone()));
                self.rev_ids.push(None);
                self.parent_rev_ids.push(None);
                self.file_paths.push(None);
                self.offset_begins.push(None);
                self.lengths.push(None);
                self.timestamps.push(None);
            }
            MetaEvent::Revision {
                rev_id,
                parent_rev_id,
                page_id,
                file_path,
                offset_begin,
                length,
                timestamp,
            } => {
                self.types.push("revision");
                self.page_ids.push(Some(*page_id));
                self.nss.push(None);
                self.titles.push(None);
                self.rev_ids.push(Some(*rev_id));
                self.parent_rev_ids.push(*parent_rev_id);
                self.file_paths.push(Some(file_path.clone()));
                self.offset_begins.push(Some(*offset_begin));
                self.lengths.push(Some(*length));
                self.timestamps.push(Some(timestamp.clone()));
            }
            MetaEvent::SiteInfo { .. } | MetaEvent::Finalize => {}
        }
    }

    fn into_batch(self, schema: Arc<Schema>) -> Result<RecordBatch, SinkError> {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(self.types)),
            Arc::new(UInt64Array::from(self.page_ids)),
            Arc::new(Int64Array::from(self.nss)),
            Arc::new(StringArray::from(self.titles)),
            Arc::new(UInt64Array::from(self.rev_ids)),
            Arc::new(UInt64Array::from(self.parent_rev_ids)),
            Arc::new(StringArray::from(self.file_paths)),
            Arc::new(UInt64Array::from(self.offset_begins)),
            Arc::new(UInt64Array::from(self.lengths)),
            Arc::new(StringArray::from(self.timestamps)),
        ];
        RecordBatch::try_new(schema, columns).map_err(|e| SinkError::Other(e.to_string()))
    }
}

/// Writes events to one Parquet file.
pub struct ParquetSink {
    writer: Option<ArrowWriter<File>>,
    schema: Arc<Schema>,
    pending: Columns,
    path: PathBuf,
}

impl ParquetSink {
    /// Creates (truncates) the file.
    pub fn create(path: &Path) -> Result<Self, SinkError> {
        let schema = schema();
        let file = File::create(path)?;
        let writer = ArrowWriter::try_new(
            file,
            schema.clone(),
            Some(WriterProperties::builder().build()),
        )
        .map_err(|e| SinkError::Other(e.to_string()))?;
        Ok(Self {
            writer: Some(writer),
            schema,
            pending: Columns::default(),
            path: path.to_path_buf(),
        })
    }

    fn flush(&mut self) -> Result<(), SinkError> {
        if self.pending.len() == 0 {
            return Ok(());
        }
        let batch = std::mem::take(&mut self.pending).into_batch(self.schema.clone())?;
        self.writer
            .as_mut()
            .expect("writer open")
            .write(&batch)
            .map_err(|e| SinkError::Other(format!("{}: {e}", self.path.display())))
    }
}

impl MetaSink for ParquetSink {
    fn handle(&mut self, event: MetaEvent) -> Result<(), SinkError> {
        self.pending.push(&event);
        if self.pending.len() >= ROW_GROUP {
            self.flush()?;
        }
        Ok(())
    }

    fn finish(mut self: Box<Self>) -> Result<(), SinkError> {
        self.flush()?;
        if let Some(w) = self.writer.take() {
            w.close().map_err(|e| SinkError::Other(e.to_string()))?;
        }
        Ok(())
    }
}

/// Writes a batch of events to a new file in one go (used by tests and by
/// callers that already hold a file's events).
pub fn write_batch(path: &Path, events: &[MetaEvent]) -> Result<(), SinkError> {
    let mut sink = Box::new(ParquetSink::create(path)?);
    for e in events {
        sink.handle(e.clone())?;
    }
    sink.finish()
}

/// The latest revision timestamp in a metadata file, normalized to
/// `YYYY-MM-DDTHH:MM:SSZ`; `None` when the file is missing or has no revisions.
#[must_use]
pub fn latest_timestamp(path: &Path) -> Option<String> {
    let file = File::open(path).ok()?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .ok()?
        .build()
        .ok()?;
    let mut max_ts: Option<String> = None;
    for batch in reader.flatten() {
        let Some(col) = batch.column_by_name("timestamp") else {
            continue;
        };
        let Some(arr) = col.as_any().downcast_ref::<StringArray>() else {
            continue;
        };
        for i in 0..arr.len() {
            if arr.is_null(i) {
                continue;
            }
            if let Some(n) = normalize_utc(arr.value(i)) {
                if max_ts.as_ref().is_none_or(|m| n > *m) {
                    max_ts = Some(n);
                }
            }
        }
    }
    max_ts
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latest_timestamp_normalizes_before_comparing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("m.parquet");
        let rev = |rev_id, ts: &str| MetaEvent::Revision {
            rev_id,
            parent_rev_id: None,
            page_id: 10,
            file_path: "recentchanges/2026-05-31.mwrev.zst".into(),
            offset_begin: 0,
            length: 1,
            timestamp: ts.into(),
        };
        write_batch(
            &path,
            &[
                rev(1, "2026-05-31T23:59:54Z"),
                rev(2, "2026-05-31 23:59:55"),
                MetaEvent::Page {
                    id: 10,
                    ns: 0,
                    title: "T".into(),
                },
            ],
        )
        .unwrap();
        assert_eq!(
            latest_timestamp(&path).as_deref(),
            Some("2026-05-31T23:59:55Z")
        );
    }
}
