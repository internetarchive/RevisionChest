use std::fs::File;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use arrow::array::{Array, ArrayRef, Int64Array, StringArray, UInt64Array, RecordBatchReader};
use arrow::record_batch::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use crate::models::DbMessage;
use crate::utils::normalize_timestamp_utc;

pub fn get_latest_timestamp_from_parquet(path: &Path) -> Option<String> {
    let file = File::open(path).ok()?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file).ok()?.build().ok()?;
    let mut max_ts: Option<String> = None;

    for batch in reader {
        if let Ok(batch) = batch {
            if let Some(ts_col) = batch.column_by_name("timestamp") {
                let ts_array = ts_col.as_any().downcast_ref::<StringArray>()?;
                for i in 0..ts_array.len() {
                    if !ts_array.is_null(i) {
                        let val = ts_array.value(i).to_string();
                        if let Ok(normalized) = normalize_timestamp_utc(&val) {
                            if max_ts.as_ref().is_none_or(|current| normalized > *current) {
                                max_ts = Some(normalized);
                            }
                        }
                    }
                }
            }
        }
    }
    max_ts
}

pub fn write_parquet_batch(path: &Path, messages: &[DbMessage]) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
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
    ]));

    let mut types = Vec::new();
    let mut page_ids = Vec::new();
    let mut nss = Vec::new();
    let mut titles = Vec::new();
    let mut rev_ids = Vec::new();
    let mut parent_rev_ids = Vec::new();
    let mut file_paths = Vec::new();
    let mut offset_begins = Vec::new();
    let mut lengths = Vec::new();
    let mut timestamps = Vec::new();

    for msg in messages {
        match msg {
            DbMessage::Page { id, ns, title } => {
                types.push(Some("page"));
                page_ids.push(Some(*id));
                nss.push(Some(*ns as i64));
                titles.push(Some(title.as_str()));
                rev_ids.push(None);
                parent_rev_ids.push(None);
                file_paths.push(None);
                offset_begins.push(None);
                lengths.push(None);
                timestamps.push(None);
            }
            DbMessage::Revision {
                rev_id,
                parent_rev_id,
                page_id,
                file_path,
                offset_begin,
                length,
                timestamp,
            } => {
                types.push(Some("revision"));
                page_ids.push(Some(*page_id));
                nss.push(None);
                titles.push(None);
                rev_ids.push(Some(*rev_id));
                parent_rev_ids.push(*parent_rev_id);
                file_paths.push(Some(file_path.as_str()));
                offset_begins.push(Some(*offset_begin));
                lengths.push(Some(*length));
                timestamps.push(Some(timestamp.as_str()));
            }
            _ => {}
        }
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(types)),
        Arc::new(UInt64Array::from(page_ids)),
        Arc::new(Int64Array::from(nss)),
        Arc::new(StringArray::from(titles)),
        Arc::new(UInt64Array::from(rev_ids)),
        Arc::new(UInt64Array::from(parent_rev_ids)),
        Arc::new(StringArray::from(file_paths)),
        Arc::new(UInt64Array::from(offset_begins)),
        Arc::new(UInt64Array::from(lengths)),
        Arc::new(StringArray::from(timestamps)),
    ];

    let batch = RecordBatch::try_new(schema.clone(), columns)?;
    let file = File::create(path)?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;

    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}

pub fn merge_parquet_files(input_paths: &[PathBuf], output_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    if input_paths.is_empty() {
        return Ok(());
    }

    let file = File::create(output_path)?;
    let first_file = File::open(&input_paths[0])?;
    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(first_file)?.build()?;
    let schema = reader.schema();

    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;

    for path in input_paths {
        let input_file = File::open(path)?;
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(input_file)?.build()?;
        for batch in reader {
            writer.write(&batch?)?;
        }
    }

    writer.close()?;
    Ok(())
}

pub fn parquet_worker(rx: crossbeam_channel::Receiver<DbMessage>, path: PathBuf) {
    let mut messages = Vec::new();
    while let Ok(msg) = rx.recv() {
        if matches!(msg, DbMessage::Finalize) {
            break;
        }
        messages.push(msg);

        // Periodically flush to Parquet
        if messages.len() >= 1000 {
            // NOTE: Parquet files are typically written once. 
            // For a long-running sync, we'd ideally append to a table or write multiple files.
            // However, the recommendation suggested writing in batches.
            // Since `write_parquet_batch` creates a new file, we'll just keep it in memory for now
            // or we would need to implement a more complex rolling file strategy.
            // The recommendation's `parquet_worker` had `write_parquet_batch(&path, &messages).ok();`
            // but that would overwrite the file each time.
            // Let's stick to the simplest interpretation for now: collect and write at the end, 
            // OR we can write a new file per batch.
            // Given the original recommendation: "write_parquet_batch(&path, &messages).ok();"
            // Let's improve it slightly by only writing at the end or when finalized, 
            // or use a temporary file strategy if we really want batches.
        }
    }
    if !messages.is_empty() {
        write_parquet_batch(&path, &messages).ok();
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::models::DbMessage;

    use super::{get_latest_timestamp_from_parquet, write_parquet_batch};

    #[test]
    fn latest_timestamp_from_parquet_normalizes_before_comparing() {
        let unique = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let path = std::env::temp_dir().join(format!("revisionchest-{unique}.parquet"));
        let messages = vec![
            DbMessage::Revision {
                rev_id: 1,
                parent_rev_id: None,
                page_id: 10,
                file_path: "recentchanges/2026-05-31.mwrev.zst".to_string(),
                offset_begin: 0,
                length: 100,
                timestamp: "2026-05-31T23:59:54Z".to_string(),
            },
            DbMessage::Revision {
                rev_id: 2,
                parent_rev_id: Some(1),
                page_id: 10,
                file_path: "recentchanges/2026-05-31.mwrev.zst".to_string(),
                offset_begin: 100,
                length: 120,
                timestamp: "2026-05-31 23:59:55".to_string(),
            },
        ];

        write_parquet_batch(&path, &messages).unwrap();

        let latest = get_latest_timestamp_from_parquet(&path);
        fs::remove_file(&path).ok();

        assert_eq!(latest.as_deref(), Some("2026-05-31T23:59:55Z"));
    }
}
