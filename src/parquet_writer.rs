use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use arrow::array::{ArrayRef, Int64Array, StringArray, UInt64Array, RecordBatchReader};
use arrow::record_batch::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use crate::models::DbMessage;

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

use std::path::PathBuf;
