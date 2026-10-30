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
#[cfg(test)]
use std::sync::Arc;

use crate::iceberg::io::FileIO;
use crate::iceberg::spec::{DataContentType, DataFileFormat, Struct};
use arrow::datatypes::SchemaRef as ArrowSchemaRef;
use arrow::record_batch::RecordBatch;
use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use uuid::Uuid;

use super::types::WrittenFile;
use super::write_adaptation::adapt_batch;
use super::write_stack::equality_schema::EqualityDeleteFileTarget;

use super::data_writer::StagedPathJournal;

#[cfg(test)]
pub(crate) async fn write_equality_delete_file(
    file_io: &FileIO,
    staging_dir: &str,
    partition_spec_id: i32,
    target: EqualityDeleteFileTarget,
    batch: RecordBatch,
) -> Result<Option<WrittenFile>, String> {
    write_equality_delete_file_with_journal(
        file_io,
        staging_dir,
        partition_spec_id,
        target,
        batch,
        None,
    )
    .await
}

pub(crate) async fn write_equality_delete_file_journaled(
    file_io: &FileIO,
    staging_dir: &str,
    partition_spec_id: i32,
    target: EqualityDeleteFileTarget,
    batch: RecordBatch,
    journal: &StagedPathJournal,
) -> Result<Option<WrittenFile>, String> {
    write_equality_delete_file_with_journal(
        file_io,
        staging_dir,
        partition_spec_id,
        target,
        batch,
        Some(journal),
    )
    .await
}

async fn write_equality_delete_file_with_journal(
    file_io: &FileIO,
    staging_dir: &str,
    partition_spec_id: i32,
    target: EqualityDeleteFileTarget,
    batch: RecordBatch,
    journal: Option<&StagedPathJournal>,
) -> Result<Option<WrittenFile>, String> {
    if batch.num_rows() == 0 {
        return Ok(None);
    }
    let schema = target.arrow_schema;
    if schema.fields().is_empty() {
        return Err("equality-delete writer requires at least one equality column".to_string());
    }
    let equality_ids = schema
        .fields()
        .iter()
        .map(|field| {
            field
                .metadata()
                .get(PARQUET_FIELD_ID_META_KEY)
                .ok_or_else(|| {
                    format!(
                        "equality-delete field `{}` has no Iceberg field ID",
                        field.name()
                    )
                })?
                .parse::<i32>()
                .map_err(|error| {
                    format!(
                        "invalid equality-delete field ID for `{}`: {error}",
                        field.name()
                    )
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let batch = adapt_batch(&batch, &schema)?;
    let path = format!(
        "{staging_dir}/equality-delete-{:08x}-{}.parquet",
        0,
        Uuid::new_v4()
    );
    let bytes = encode_equality_delete_parquet(schema, &batch)?;
    let file_size = bytes.len() as u64;
    if let Some(journal) = journal {
        journal.record(&path);
    }
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
        equality_ids: Some(equality_ids),
        first_row_id: None,
        content_offset: None,
        content_size_in_bytes: None,
        cardinality: None,
    }))
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

    use crate::iceberg::arrow::schema_to_arrow_schema;
    use crate::iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use arrow::array::{ArrayRef, Int8Array};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};

    fn target(fields: Vec<crate::iceberg::spec::NestedFieldRef>) -> EqualityDeleteFileTarget {
        let schema = Schema::builder().with_fields(fields).build().unwrap();
        EqualityDeleteFileTarget {
            arrow_schema: Arc::new(schema_to_arrow_schema(&schema).unwrap()),
        }
    }

    fn common_target() -> EqualityDeleteFileTarget {
        target(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(2, "category", Type::Primitive(PrimitiveType::String)).into(),
        ])
    }

    #[test]
    fn schema_has_iceberg_field_ids() {
        let schema = common_target().arrow_schema;

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
        let schema = common_target().arrow_schema;
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
        let target = target(vec![
            NestedField::optional(11, "tiny", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(27, "small", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(99, "raw", Type::Primitive(PrimitiveType::Binary)).into(),
        ]);
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
            target,
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
                Some(LogicalType::Integer(value)) if matches!(value.bit_width, 8 | 16)
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
        assert_eq!(
            builder.schema().field(2).data_type(),
            &DataType::LargeBinary
        );
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
            .downcast_ref::<arrow::array::LargeBinaryArray>()
            .unwrap();
        assert_eq!(raw.value(0), &[0xff, 0]);
        assert!(raw.is_null(1));
        assert_eq!(raw.value(2), &[] as &[u8]);
    }

    #[tokio::test]
    async fn equality_delete_public_writer_rejects_unsupported_and_required_null_before_io() {
        use arrow::array::Int64Array;
        let dir = tempfile::tempdir().unwrap();
        let location = format!("file://{}", dir.path().display());
        let io = local_file_io(&location);
        for (values, required, expected) in [
            (
                Arc::new(Int64Array::from(vec![127])) as ArrayRef,
                false,
                "unsupported Iceberg write adaptation",
            ),
            (
                Arc::new(Int8Array::from(vec![None::<i8>])) as ArrayRef,
                true,
                "contains NULL",
            ),
        ] {
            let input = Arc::new(ArrowSchema::new(vec![Field::new(
                "key",
                values.data_type().clone(),
                true,
            )]));
            let batch = RecordBatch::try_new(input, vec![values]).unwrap();
            let field = if required {
                NestedField::required(11, "key", Type::Primitive(PrimitiveType::Int))
            } else {
                NestedField::optional(11, "key", Type::Primitive(PrimitiveType::Int))
            };
            let error = write_equality_delete_file(
                &io,
                &format!("{location}/data"),
                7,
                target(vec![field.into()]),
                batch,
            )
            .await
            .expect_err("reject before writing");
            assert!(error.contains(expected), "{error}");
            assert!(!dir.path().join("data").exists());
        }
    }

    #[tokio::test]
    async fn equality_delete_sdk_target_preserves_timezone_and_dictionary_values() {
        use arrow::array::{DictionaryArray, LargeStringArray, TimestampMicrosecondArray};
        use arrow::datatypes::{Int32Type, TimeUnit};
        use parquet::basic::LogicalType;
        let dir = tempfile::tempdir().unwrap();
        let location = format!("file://{}", dir.path().display());
        let target = target(vec![
            NestedField::optional(71, "ts", Type::Primitive(PrimitiveType::Timestamptz)).into(),
            NestedField::optional(72, "s", Type::Primitive(PrimitiveType::String)).into(),
        ]);
        let dictionary: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![Some(1), None, Some(0)]),
                Arc::new(LargeStringArray::from(vec!["alpha", "東京"])) as ArrayRef,
            )
            .unwrap(),
        );
        let batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
                Field::new("s", dictionary.data_type().clone(), true),
            ])),
            vec![
                Arc::new(TimestampMicrosecondArray::from(vec![
                    Some(-1234567),
                    None,
                    Some(42),
                ])),
                dictionary,
            ],
        )
        .unwrap();
        let written = write_equality_delete_file(
            &local_file_io(&location),
            &format!("{location}/data"),
            0,
            target,
            batch,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(written.equality_ids, Some(vec![71, 72]));
        let reader = ParquetRecordBatchReaderBuilder::try_new(
            std::fs::File::open(written.path.strip_prefix("file://").unwrap()).unwrap(),
        )
        .unwrap();
        assert!(matches!(
            reader.parquet_schema().columns()[0].logical_type_ref(),
            Some(LogicalType::Timestamp(value)) if value.is_adjusted_to_u_t_c
        ));
        assert_eq!(
            reader.parquet_schema().columns()[0]
                .self_type()
                .get_basic_info()
                .id(),
            71
        );
        let batches = reader
            .build()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let timestamps = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(
            timestamps.iter().collect::<Vec<_>>(),
            vec![Some(-1234567), None, Some(42)]
        );
        let strings = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            strings.iter().collect::<Vec<_>>(),
            vec![Some("東京"), None, Some("alpha")]
        );
    }
}
