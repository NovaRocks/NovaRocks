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

use crate::iceberg::io::FileIO;
use crate::iceberg::spec::{DataContentType, DataFileFormat, Struct};
use arrow::array::{Array, ArrayRef, Int8Array, Int16Array, Int32Builder};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use arrow::record_batch::RecordBatch;
use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use uuid::Uuid;

use super::frozen_write::scalar_integer_storage_schema;
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
    // Validate the declared input before changing only its storage carrier.
    let batch = rewrap_batch_with_equality_schema(batch, schema.clone())?;
    let schema = scalar_integer_storage_schema(&schema);
    let batch = equality_delete_storage_batch(batch, schema.clone())?;
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
        nan_value_counts: HashMap::new(),
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

/// Equality-delete keys retain their exact declared input signature. The only
/// carrier change owned here is lossless signed I8/I16 to standard Iceberg INT.
fn equality_delete_storage_batch(
    batch: RecordBatch,
    schema: ArrowSchemaRef,
) -> Result<RecordBatch, String> {
    let columns = batch
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(column, field)| match (column.data_type(), field.data_type()) {
            (DataType::Int8, DataType::Int32) => {
                let values = column
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .ok_or_else(|| "equality-delete Int8 carrier downcast failed".to_string())?;
                let mut builder = Int32Builder::with_capacity(values.len());
                for value in values.iter() {
                    builder.append_option(value.map(i32::from));
                }
                Ok(Arc::new(builder.finish()) as ArrayRef)
            }
            (DataType::Int16, DataType::Int32) => {
                let values = column
                    .as_any()
                    .downcast_ref::<Int16Array>()
                    .ok_or_else(|| "equality-delete Int16 carrier downcast failed".to_string())?;
                let mut builder = Int32Builder::with_capacity(values.len());
                for value in values.iter() {
                    builder.append_option(value.map(i32::from));
                }
                Ok(Arc::new(builder.finish()) as ArrayRef)
            }
            (actual, target) if actual == target => Ok(Arc::clone(column)),
            (actual, target) => Err(format!(
                "unsupported equality-delete storage conversion for `{}`: {actual:?} to {target:?}",
                field.name()
            )),
        })
        .collect::<Result<Vec<_>, String>>()?;
    RecordBatch::try_new(schema, columns)
        .map_err(|error| format!("build equality-delete storage batch failed: {error}"))
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
    fn local_file_io(location: &str) -> FileIO {
        use novarocks_fs::{FsAccessResolver, TokioFileIoRuntime, TokioFileTaskSpawner};
        let runtime = tokio::runtime::Handle::current();
        let binding = crate::access_binding::IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime)),
        );
        crate::fs_io::build_file_io_for_location(location, binding)
    }

    #[tokio::test]
    async fn equality_delete_public_writer_stores_narrow_keys_as_int32_with_exact_ids_and_nulls() {
        use arrow::array::{BinaryArray, Int8Array, Int16Array};
        use parquet::basic::{LogicalType, Type as PhysicalType};
        let dir = tempfile::tempdir().unwrap();
        let location = format!("file://{}", dir.path().display());
        let columns = vec![
            EqualityDeleteColumn {
                name: "tiny".to_string(),
                field_id: 11,
                data_type: DataType::Int8,
                nullable: true,
            },
            EqualityDeleteColumn {
                name: "small".to_string(),
                field_id: 27,
                data_type: DataType::Int16,
                nullable: true,
            },
            EqualityDeleteColumn {
                name: "raw".to_string(),
                field_id: 99,
                data_type: DataType::Binary,
                nullable: true,
            },
        ];
        // Actual input is still the frozen SQL I8/I16 domain, not pre-cast I32.
        let input_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("tiny", DataType::Int8, true),
            Field::new("small", DataType::Int16, true),
            Field::new("raw", DataType::Binary, true),
        ]));
        let batch = RecordBatch::try_new(
            input_schema,
            vec![
                Arc::new(Int8Array::from(vec![Some(-128), None, Some(127)])),
                Arc::new(Int16Array::from(vec![Some(-32768), Some(32767), None])),
                Arc::new(BinaryArray::from(vec![
                    Some(&[0xff, 0][..]),
                    None,
                    Some(&[][..]),
                ])),
            ],
        )
        .unwrap();
        let written = write_equality_delete_file(
            &local_file_io(&location),
            &format!("{location}/data"),
            7,
            columns,
            batch,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(written.content, DataContentType::EqualityDeletes);
        assert_eq!(written.partition_spec_id, 7);
        assert_eq!(written.record_count, 3);
        assert_eq!(written.equality_ids, Some(vec![11, 27, 99]));
        let builder = ParquetRecordBatchReaderBuilder::try_new(
            std::fs::File::open(
                written
                    .path
                    .strip_prefix("file://")
                    .unwrap_or(&written.path),
            )
            .unwrap(),
        )
        .unwrap();
        for (index, id) in [11, 27].into_iter().enumerate() {
            let column = &builder.parquet_schema().columns()[index];
            assert_eq!(column.physical_type(), PhysicalType::INT32);
            assert!(column.self_type().get_basic_info().has_id());
            assert_eq!(column.self_type().get_basic_info().id(), id);
            assert!(!matches!(
                column.logical_type_ref(),
                Some(LogicalType::Integer {
                    bit_width: 8 | 16,
                    ..
                })
            ));
            assert_eq!(builder.schema().field(index).data_type(), &DataType::Int32);
            assert!(builder.schema().field(index).is_nullable());
            assert_eq!(
                builder
                    .schema()
                    .field(index)
                    .metadata()
                    .get(PARQUET_FIELD_ID_META_KEY),
                Some(&id.to_string())
            );
        }
        assert_eq!(builder.schema().field(2).data_type(), &DataType::Binary);
        assert_eq!(
            builder
                .schema()
                .field(2)
                .metadata()
                .get(PARQUET_FIELD_ID_META_KEY),
            Some(&"99".to_string())
        );
        let batches = builder
            .build()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(batches.len(), 1);
        let tiny = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let small = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(
            tiny.iter().collect::<Vec<_>>(),
            vec![Some(-128), None, Some(127)]
        );
        assert_eq!(
            small.iter().collect::<Vec<_>>(),
            vec![Some(-32768), Some(32767), None]
        );
        let raw = batches[0]
            .column(2)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(raw.value(0), &[0xff, 0]);
        assert!(raw.is_null(1));
        assert_eq!(raw.value(2), &[] as &[u8]);
    }

    #[tokio::test]
    async fn equality_delete_public_writer_rejects_input_signature_before_storage_conversion() {
        use arrow::array::{Int8Array, Int16Array};
        let dir = tempfile::tempdir().unwrap();
        let location = format!("file://{}", dir.path().display());
        let io = local_file_io(&location);
        let columns = vec![EqualityDeleteColumn {
            name: "key".to_string(),
            field_id: 11,
            data_type: DataType::Int8,
            nullable: true,
        }];
        for values in [
            Arc::new(Int32Array::from(vec![127])) as ArrayRef,
            Arc::new(Int16Array::from(vec![127])) as ArrayRef,
        ] {
            let schema = Arc::new(ArrowSchema::new(vec![Field::new(
                "key",
                values.data_type().clone(),
                true,
            )]));
            let batch = RecordBatch::try_new(schema, vec![values]).unwrap();
            let error = write_equality_delete_file(
                &io,
                &format!("{location}/data"),
                7,
                columns.clone(),
                batch,
            )
            .await
            .err()
            .expect("wider caller input must not bypass the frozen I8 declaration");
            assert!(error.contains("type mismatch: expected Int8"), "{error}");
            assert!(
                !dir.path().join("data").exists(),
                "rejected input must not publish an output"
            );
        }
        let columns = vec![EqualityDeleteColumn {
            name: "key".to_string(),
            field_id: 11,
            data_type: DataType::Int8,
            nullable: false,
        }];
        let batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![Field::new(
                "key",
                DataType::Int8,
                true,
            )])),
            vec![Arc::new(Int8Array::from(vec![None::<i8>]))],
        )
        .unwrap();
        let error = write_equality_delete_file(&io, &format!("{location}/data"), 7, columns, batch)
            .await
            .err()
            .expect("NULL must not satisfy a required input field");
        assert!(error.contains("RecordBatch::try_new failed"), "{error}");
        assert!(!dir.path().join("data").exists());
    }
}
