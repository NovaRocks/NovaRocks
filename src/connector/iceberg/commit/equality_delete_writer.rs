// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Minimal v2/v3-compatible equality-delete Parquet writer.
//!
//! The writer receives an Arrow batch that already contains only equality-key
//! columns. It attaches Iceberg field-id metadata to the Parquet schema, writes
//! one delete file under the caller-provided staging directory, and returns a
//! [`super::types::WrittenFile`] with `content = EqualityDeletes` and
//! `equality_ids` populated for `RowDeltaCommit`.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use arrow::record_batch::RecordBatch;
use iceberg::io::FileIO;
use iceberg::spec::{DataContentType, DataFileFormat, Struct};
use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use uuid::Uuid;

use super::types::WrittenFile;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EqualityDeleteColumn {
    pub name: String,
    pub field_id: i32,
    pub data_type: DataType,
    pub nullable: bool,
}

pub async fn write_equality_delete_file(
    file_io: &FileIO,
    staging_dir: &str,
    partition_spec_id: i32,
    columns: Vec<EqualityDeleteColumn>,
    batch: RecordBatch,
) -> Result<Option<WrittenFile>, String> {
    if batch.num_rows() == 0 {
        return Ok(None);
    }
    let schema = equality_delete_schema(&columns)?;
    let batch = rewrap_batch_with_equality_schema(batch, schema.clone())?;
    let path = format!(
        "{staging_dir}/equality-delete-{:08x}-{}.parquet",
        0,
        Uuid::new_v4()
    );
    let bytes = encode_equality_delete_parquet(schema, &batch)?;
    let file_size = bytes.len() as u64;
    write_bytes_via_file_io(file_io, &path, bytes).await?;

    Ok(Some(WrittenFile {
        path,
        format: DataFileFormat::Parquet,
        content: DataContentType::EqualityDeletes,
        partition_values: Struct::empty(),
        partition_spec_id,
        record_count: batch.num_rows() as u64,
        file_size_in_bytes: file_size,
        split_offsets: vec![],
        column_sizes: HashMap::new(),
        value_counts: HashMap::new(),
        null_value_counts: HashMap::new(),
        lower_bounds: HashMap::new(),
        upper_bounds: HashMap::new(),
        key_metadata: None,
        referenced_data_file: None,
        equality_ids: Some(columns.iter().map(|c| c.field_id).collect()),
        first_row_id: None,
        content_offset: None,
        content_size_in_bytes: None,
        cardinality: None,
    }))
}

fn equality_delete_schema(columns: &[EqualityDeleteColumn]) -> Result<ArrowSchemaRef, String> {
    if columns.is_empty() {
        return Err("equality-delete writer requires at least one equality column".to_string());
    }
    let fields = columns
        .iter()
        .map(|column| {
            let mut metadata = HashMap::new();
            metadata.insert(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                column.field_id.to_string(),
            );
            Field::new(&column.name, column.data_type.clone(), column.nullable)
                .with_metadata(metadata)
        })
        .collect::<Vec<_>>();
    Ok(Arc::new(ArrowSchema::new(fields)))
}

fn rewrap_batch_with_equality_schema(
    batch: RecordBatch,
    schema: ArrowSchemaRef,
) -> Result<RecordBatch, String> {
    if batch.num_columns() != schema.fields().len() {
        return Err(format!(
            "equality-delete batch column count mismatch: expected {}, got {}",
            schema.fields().len(),
            batch.num_columns()
        ));
    }
    for (idx, field) in schema.fields().iter().enumerate() {
        let actual = batch.column(idx).data_type();
        if actual != field.data_type() {
            return Err(format!(
                "equality-delete column `{}` type mismatch: expected {:?}, got {:?}",
                field.name(),
                field.data_type(),
                actual
            ));
        }
    }
    let columns = (0..batch.num_columns())
        .map(|idx| Arc::clone(batch.column(idx)) as ArrayRef)
        .collect::<Vec<_>>();
    RecordBatch::try_new(schema, columns)
        .map_err(|e| format!("equality-delete RecordBatch::try_new failed: {e}"))
}

fn encode_equality_delete_parquet(
    schema: ArrowSchemaRef,
    batch: &RecordBatch,
) -> Result<Vec<u8>, String> {
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut buf = Vec::with_capacity(batch.num_rows() * batch.num_columns() * 16 + 1024);
    {
        let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props))
            .map_err(|e| format!("ArrowWriter::try_new failed for equality-delete: {e}"))?;
        writer
            .write(batch)
            .map_err(|e| format!("ArrowWriter::write failed for equality-delete: {e}"))?;
        writer
            .close()
            .map_err(|e| format!("ArrowWriter::close failed for equality-delete: {e}"))?;
    }
    Ok(buf)
}

async fn write_bytes_via_file_io(
    file_io: &FileIO,
    path: &str,
    bytes: Vec<u8>,
) -> Result<(), String> {
    let output = file_io
        .new_output(path)
        .map_err(|e| format!("FileIO::new_output({path}) failed: {e}"))?;
    let mut w = output
        .writer()
        .await
        .map_err(|e| format!("FileIO::writer({path}) failed: {e}"))?;
    w.write(bytes.into())
        .await
        .map_err(|e| format!("write equality-delete bytes to {path} failed: {e}"))?;
    w.close()
        .await
        .map_err(|e| format!("close equality-delete output {path} failed: {e}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, Int32Array, StringArray};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    use super::*;

    fn columns() -> Vec<EqualityDeleteColumn> {
        vec![
            EqualityDeleteColumn {
                name: "id".to_string(),
                field_id: 1,
                data_type: DataType::Int32,
                nullable: false,
            },
            EqualityDeleteColumn {
                name: "category".to_string(),
                field_id: 2,
                data_type: DataType::Utf8,
                nullable: true,
            },
        ]
    }

    #[test]
    fn schema_has_iceberg_field_ids() {
        let schema = equality_delete_schema(&columns()).expect("schema");

        assert_eq!(
            schema.field(0).metadata().get(PARQUET_FIELD_ID_META_KEY),
            Some(&"1".to_string())
        );
        assert_eq!(
            schema.field(1).metadata().get(PARQUET_FIELD_ID_META_KEY),
            Some(&"2".to_string())
        );
    }

    #[test]
    fn encode_round_trips_values() {
        let columns = columns();
        let schema = equality_delete_schema(&columns).expect("schema");
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![2, 4])),
                Arc::new(StringArray::from(vec![Some("B"), None])),
            ],
        )
        .expect("batch");

        let bytes = encode_equality_delete_parquet(schema, &batch).expect("encode");
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
            .expect("reader builder")
            .build()
            .expect("reader");
        let batches = reader.collect::<Result<Vec<_>, _>>().expect("read");

        assert_eq!(batches.len(), 1);
        let ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("id column");
        assert_eq!(ids.values(), &[2, 4]);
        let categories = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("category column");
        assert_eq!(categories.value(0), "B");
        assert!(categories.is_null(1));
    }
}
