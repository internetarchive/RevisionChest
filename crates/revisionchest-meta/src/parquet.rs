//! The Parquet sink and reader helpers.
//!
//! One row per event, `type` = `page`, `revision` or `log`; page rows carry
//! `ns` and `title`, revision rows the bundle location and timestamp, and log
//! rows the page, timestamp, `rcid`, `log_id`, `log_type`, `log_action` and
//! `log_params` (JSON). The log columns are null on other rows. Rows are written
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
        Field::new("rcid", DataType::UInt64, true),
        Field::new("log_id", DataType::UInt64, true),
        Field::new("log_type", DataType::Utf8, true),
        Field::new("log_action", DataType::Utf8, true),
        Field::new("log_params", DataType::Utf8, true),
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
    rcids: Vec<Option<u64>>,
    log_ids: Vec<Option<u64>>,
    log_types: Vec<Option<String>>,
    log_actions: Vec<Option<String>>,
    log_params: Vec<Option<String>>,
}

impl Columns {
    fn len(&self) -> usize {
        self.types.len()
    }

    fn push_no_log(&mut self) {
        self.rcids.push(None);
        self.log_ids.push(None);
        self.log_types.push(None);
        self.log_actions.push(None);
        self.log_params.push(None);
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
                self.push_no_log();
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
                self.push_no_log();
            }
            MetaEvent::Log {
                rcid,
                log_id,
                log_type,
                log_action,
                page_id,
                ns,
                title,
                timestamp,
                params,
            } => {
                self.types.push("log");
                self.page_ids.push(*page_id);
                self.nss.push(Some(*ns));
                self.titles.push(Some(title.clone()));
                self.rev_ids.push(None);
                self.parent_rev_ids.push(None);
                self.file_paths.push(None);
                self.offset_begins.push(None);
                self.lengths.push(None);
                self.timestamps.push(Some(timestamp.clone()));
                self.rcids.push(Some(*rcid));
                self.log_ids.push(*log_id);
                self.log_types.push(Some(log_type.clone()));
                self.log_actions.push(Some(log_action.clone()));
                self.log_params.push(params.clone());
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
            Arc::new(UInt64Array::from(self.rcids)),
            Arc::new(UInt64Array::from(self.log_ids)),
            Arc::new(StringArray::from(self.log_types)),
            Arc::new(StringArray::from(self.log_actions)),
            Arc::new(StringArray::from(self.log_params)),
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

/// Every revision row of a metadata file, as `(rev_id, timestamp)` with the
/// timestamp normalized to `YYYY-MM-DDTHH:MM:SSZ`; empty when the file is
/// missing or unreadable.
fn revision_rows(path: &Path) -> Vec<(u64, String)> {
    let Ok(file) = File::open(path) else {
        return Vec::new();
    };
    let Ok(reader) = ParquetRecordBatchReaderBuilder::try_new(file).and_then(|b| b.build()) else {
        return Vec::new();
    };
    let mut rows = Vec::new();
    for batch in reader.flatten() {
        let (Some(types), Some(ids), Some(ts)) = (
            batch
                .column_by_name("type")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>().cloned()),
            batch
                .column_by_name("rev_id")
                .and_then(|c| c.as_any().downcast_ref::<UInt64Array>().cloned()),
            batch
                .column_by_name("timestamp")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>().cloned()),
        ) else {
            continue;
        };
        for i in 0..batch.num_rows() {
            if types.value(i) != "revision" || ids.is_null(i) || ts.is_null(i) {
                continue;
            }
            if let Some(n) = normalize_utc(ts.value(i)) {
                rows.push((ids.value(i), n));
            }
        }
    }
    rows
}

/// The latest revision timestamp in a metadata file, normalized to
/// `YYYY-MM-DDTHH:MM:SSZ`; `None` when the file is missing or has no revisions.
#[must_use]
pub fn latest_timestamp(path: &Path) -> Option<String> {
    revision_rows(path).into_iter().map(|(_, ts)| ts).max()
}

/// The revisions a metadata file records at or after `since`
/// (`YYYY-MM-DDTHH:MM:SSZ`), so a sync that starts before its newest recorded
/// revision does not fetch them again.
#[must_use]
pub fn revisions_since(path: &Path, since: &str) -> std::collections::HashSet<u64> {
    revision_rows(path)
        .into_iter()
        .filter(|(_, ts)| ts.as_str() >= since)
        .map(|(id, _)| id)
        .collect()
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

    #[test]
    fn log_rows_are_written_and_do_not_count_as_revisions() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("m.parquet");
        write_batch(
            &path,
            &[
                MetaEvent::Revision {
                    rev_id: 5,
                    parent_rev_id: None,
                    page_id: 10,
                    file_path: "recentchanges/2026-05-31.mwrev.zst".into(),
                    offset_begin: 0,
                    length: 1,
                    timestamp: "2026-05-31T10:00:00Z".into(),
                },
                MetaEvent::Log {
                    rcid: 99,
                    log_id: Some(7),
                    log_type: "delete".into(),
                    log_action: "revision".into(),
                    page_id: Some(10),
                    ns: 0,
                    title: "T".into(),
                    timestamp: "2026-05-31T11:00:00Z".into(),
                    params: Some(r#"{"ids":[5]}"#.into()),
                },
            ],
        )
        .unwrap();
        assert_eq!(
            latest_timestamp(&path).as_deref(),
            Some("2026-05-31T10:00:00Z"),
            "a log row is not a revision"
        );
        assert_eq!(
            revisions_since(&path, "2026-05-31T09:00:00Z"),
            [5].into_iter().collect()
        );
        assert!(revisions_since(&path, "2026-05-31T10:00:01Z").is_empty());

        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap())
            .unwrap()
            .build()
            .unwrap();
        let batch = reader.flatten().next().unwrap();
        let col = |n: &str| batch.column_by_name(n).unwrap().clone();
        let types = col("type");
        let types = types.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(types.value(1), "log");
        let params = col("log_params");
        let params = params.as_any().downcast_ref::<StringArray>().unwrap();
        assert!(params.is_null(0));
        assert_eq!(params.value(1), r#"{"ids":[5]}"#);
        let rcids = col("rcid");
        let rcids = rcids.as_any().downcast_ref::<UInt64Array>().unwrap();
        assert_eq!(rcids.value(1), 99);
    }
}
