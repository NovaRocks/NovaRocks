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

//! Local Parquet I/O helpers: read a schema-shaped batch from a parquet file,
//! cast/normalize batches to match the table schema, and write batches back
//! to disk.

use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

pub(crate) fn read_local_parquet_data(
    path: &Path,
    target_schema: &SchemaRef,
) -> Result<RecordBatch, String> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let file =
        std::fs::File::open(path).map_err(|e| format!("open local parquet file failed: {e}"))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| format!("read local parquet metadata failed: {e}"))?;
    let reader = builder
        .build()
        .map_err(|e| format!("build local parquet reader failed: {e}"))?;
    let mut batches = Vec::new();
    for batch_result in reader {
        let batch = batch_result.map_err(|e| format!("read local parquet batch failed: {e}"))?;
        batches.push(batch);
    }
    let batch = concat_or_empty_batches(target_schema, batches)?;
    cast_batch_to_schema(&batch, target_schema)
}

fn concat_or_empty_batches(
    target_schema: &SchemaRef,
    batches: Vec<RecordBatch>,
) -> Result<RecordBatch, String> {
    if let Some(first) = batches.first() {
        arrow::compute::concat_batches(&first.schema(), batches.iter())
            .map_err(|e| format!("concat standalone batches failed: {e}"))
    } else {
        let arrays = target_schema
            .fields()
            .iter()
            .map(|field| arrow::array::new_empty_array(field.data_type()))
            .collect::<Vec<_>>();
        RecordBatch::try_new(target_schema.clone(), arrays)
            .map_err(|e| format!("build empty standalone batch failed: {e}"))
    }
}

/// Normalize `Map` entries so that the `entries` struct field is non-nullable
/// (a Parquet schema requirement). Traverses nested lists/structs too.
pub(crate) fn normalize_map_entries_nullability(data_type: &DataType) -> DataType {
    match data_type {
        DataType::Map(entries_field, ordered) => {
            let inner = normalize_map_entries_nullability(entries_field.data_type());
            let new_field = Arc::new(Field::new(entries_field.name(), inner, false));
            DataType::Map(new_field, *ordered)
        }
        DataType::List(field) => {
            let inner = normalize_map_entries_nullability(field.data_type());
            let new_field = Arc::new(Field::new(field.name(), inner, field.is_nullable()));
            DataType::List(new_field)
        }
        DataType::LargeList(field) => {
            let inner = normalize_map_entries_nullability(field.data_type());
            let new_field = Arc::new(Field::new(field.name(), inner, field.is_nullable()));
            DataType::LargeList(new_field)
        }
        DataType::FixedSizeList(field, size) => {
            let inner = normalize_map_entries_nullability(field.data_type());
            let new_field = Arc::new(Field::new(field.name(), inner, field.is_nullable()));
            DataType::FixedSizeList(new_field, *size)
        }
        DataType::Struct(fields) => {
            let new_fields = fields
                .iter()
                .map(|field| {
                    Arc::new(Field::new(
                        field.name(),
                        normalize_map_entries_nullability(field.data_type()),
                        field.is_nullable(),
                    ))
                })
                .collect::<Vec<_>>();
            DataType::Struct(new_fields.into())
        }
        other => other.clone(),
    }
}

/// Cast a RecordBatch to match a target schema (column-by-column cast).
pub(crate) fn cast_batch_to_schema(
    batch: &RecordBatch,
    target_schema: &std::sync::Arc<arrow::datatypes::Schema>,
) -> Result<RecordBatch, String> {
    if batch.schema().fields().len() != target_schema.fields().len() {
        return Err(format!(
            "INSERT SELECT column count mismatch: source={}, target={}",
            batch.schema().fields().len(),
            target_schema.fields().len()
        ));
    }
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (idx, target_field) in target_schema.fields().iter().enumerate() {
        let source_col = batch.column(idx);
        if source_col.data_type() == target_field.data_type() {
            columns.push(source_col.clone());
        } else {
            let casted = cast_array_for_local_schema(source_col, target_field).map_err(|e| {
                format!(
                    "cast column {} from {:?} to {:?} failed: {e}",
                    target_field.name(),
                    source_col.data_type(),
                    target_field.data_type()
                )
            })?;
            columns.push(casted);
        }
    }
    RecordBatch::try_new(target_schema.clone(), columns)
        .map_err(|e| format!("rebuild insert-select batch failed: {e}"))
}

fn cast_list_struct_to_map_for_local_schema(
    source_col: &ArrayRef,
    target_entries: &Arc<Field>,
    ordered: bool,
) -> Result<ArrayRef, String> {
    use arrow::array::{ListArray, MapArray, StructArray};
    use arrow_buffer::OffsetBuffer;

    let list = source_col
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| "failed to downcast ListArray".to_string())?;
    let values = if list.values().data_type() == target_entries.data_type() {
        list.values().clone()
    } else {
        novarocks_execution::exec::expr::cast_with_special_rules(
            list.values(),
            target_entries.data_type(),
        )?
    };
    let entries = values
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| "failed to cast LIST values to STRUCT for MAP rebuild".to_string())?
        .clone();
    Ok(Arc::new(MapArray::new(
        target_entries.clone(),
        OffsetBuffer::new(list.value_offsets().to_vec().into()),
        entries,
        list.nulls().cloned(),
        ordered,
    )) as ArrayRef)
}

fn cast_array_for_local_schema(
    source_col: &ArrayRef,
    target_field: &arrow::datatypes::FieldRef,
) -> Result<ArrayRef, String> {
    use arrow::array::{Array, BinaryArray, LargeBinaryArray, LargeStringArray, StringArray};
    use arrow::datatypes::DataType;

    fn bytes_to_text(bytes: &[u8]) -> String {
        // Some older local Parquet files expose VARCHAR payloads as Binary.
        // Preserve valid UTF-8 as text; mapping every byte through Latin-1
        // corrupts multi-byte UTF-8 values such as Chinese characters.  Invalid
        // binary payloads still need a lossless String carrier for the legacy
        // local BINARY/VARBINARY representation, so retain the Latin-1 mapping
        // only as that fallback.
        String::from_utf8(bytes.to_vec())
            .unwrap_or_else(|_| bytes.iter().map(|byte| char::from(*byte)).collect())
    }

    fn is_numeric_datetime_source(data_type: &DataType) -> bool {
        matches!(
            data_type,
            DataType::Boolean
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Float32
                | DataType::Float64
                | DataType::Decimal128(_, _)
                | DataType::FixedSizeBinary(16)
        )
    }

    match (source_col.data_type(), target_field.data_type()) {
        // Standalone local tables currently map SQL BINARY/VARBINARY columns to Utf8.
        // Preserve payload bytes explicitly instead of relying on Arrow's UTF-8 cast rules.
        (DataType::Binary, DataType::Utf8) => {
            let arr = source_col
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| "failed to downcast BinaryArray".to_string())?;
            Ok(Arc::new(StringArray::from(
                (0..arr.len())
                    .map(|row| (!arr.is_null(row)).then(|| bytes_to_text(arr.value(row))))
                    .collect::<Vec<_>>(),
            )) as ArrayRef)
        }
        (DataType::LargeBinary, DataType::Utf8) => {
            let arr = source_col
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| "failed to downcast LargeBinaryArray".to_string())?;
            Ok(Arc::new(StringArray::from(
                (0..arr.len())
                    .map(|row| (!arr.is_null(row)).then(|| bytes_to_text(arr.value(row))))
                    .collect::<Vec<_>>(),
            )) as ArrayRef)
        }
        (DataType::Binary, DataType::LargeUtf8) => {
            let arr = source_col
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| "failed to downcast BinaryArray".to_string())?;
            Ok(Arc::new(LargeStringArray::from(
                (0..arr.len())
                    .map(|row| (!arr.is_null(row)).then(|| bytes_to_text(arr.value(row))))
                    .collect::<Vec<_>>(),
            )) as ArrayRef)
        }
        (DataType::LargeBinary, DataType::LargeUtf8) => {
            let arr = source_col
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| "failed to downcast LargeBinaryArray".to_string())?;
            Ok(Arc::new(LargeStringArray::from(
                (0..arr.len())
                    .map(|row| (!arr.is_null(row)).then(|| bytes_to_text(arr.value(row))))
                    .collect::<Vec<_>>(),
            )) as ArrayRef)
        }
        (source_type, DataType::Date32) if is_numeric_datetime_source(source_type) => {
            novarocks_execution::exec::expr::cast_with_special_rules(
                source_col,
                target_field.data_type(),
            )
        }
        (source_type, DataType::Timestamp(_, _)) if is_numeric_datetime_source(source_type) => {
            novarocks_execution::exec::expr::cast_with_special_rules(
                source_col,
                target_field.data_type(),
            )
        }
        (_, DataType::FixedSizeBinary(width))
            if *width == novarocks_types::largeint::LARGEINT_BYTE_WIDTH =>
        {
            novarocks_execution::exec::expr::cast_with_special_rules(
                source_col,
                target_field.data_type(),
            )
        }
        (DataType::List(source_field), DataType::Map(target_entries, ordered))
            if matches!(source_field.data_type(), DataType::Struct(_)) =>
        {
            cast_list_struct_to_map_for_local_schema(source_col, target_entries, *ordered)
        }
        (_, DataType::List(_) | DataType::Struct(_) | DataType::Map(_, _)) => {
            novarocks_execution::exec::expr::cast_with_special_rules(
                source_col,
                target_field.data_type(),
            )
        }
        _ => arrow::compute::cast(source_col, target_field.data_type()).map_err(|e| format!("{e}")),
    }
}

fn parquet_storage_type_for_local_batch(data_type: &DataType) -> DataType {
    match data_type {
        DataType::Map(entries_field, _) => DataType::List(Arc::new(Field::new(
            "item",
            entries_field.data_type().clone(),
            true,
        ))),
        other => other.clone(),
    }
}

fn encode_array_for_local_parquet_storage(array: &ArrayRef) -> Result<ArrayRef, String> {
    use arrow::array::{ListArray, MapArray};
    use arrow_buffer::OffsetBuffer;

    match array.data_type() {
        DataType::Map(_, _) => {
            let map = array
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(|| "failed to downcast MapArray".to_string())?;
            Ok(Arc::new(ListArray::new(
                Arc::new(Field::new(
                    "item",
                    DataType::Struct(map.entries().fields().clone()),
                    true,
                )),
                OffsetBuffer::new(map.value_offsets().to_vec().into()),
                Arc::new(map.entries().clone()) as ArrayRef,
                map.nulls().cloned(),
            )) as ArrayRef)
        }
        _ => Ok(array.clone()),
    }
}

fn normalize_local_parquet_batch(batch: &RecordBatch) -> Result<RecordBatch, String> {
    let mut changed = false;
    let mut fields = Vec::with_capacity(batch.num_columns());
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (field, column) in batch.schema().fields().iter().zip(batch.columns().iter()) {
        let encoded = encode_array_for_local_parquet_storage(column)?;
        if !Arc::ptr_eq(column, &encoded) {
            changed = true;
        }
        let storage_type = parquet_storage_type_for_local_batch(field.data_type());
        if &storage_type != field.data_type() {
            changed = true;
        }
        fields.push(
            Field::new(field.name(), storage_type, field.is_nullable())
                .with_metadata(field.metadata().clone()),
        );
        columns.push(encoded);
    }
    if !changed {
        return Ok(batch.clone());
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|e| format!("build local parquet storage batch failed: {e}"))
}

/// Write a RecordBatch to a parquet file at the given path.
pub(crate) fn write_parquet_to_path(path: &Path, batch: &RecordBatch) -> Result<(), String> {
    use parquet::arrow::ArrowWriter;

    let batch = normalize_local_parquet_batch(batch)?;
    let file = std::fs::File::create(path)
        .map_err(|e| format!("create local parquet file failed: {e}"))?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None)
        .map_err(|e| format!("create local parquet writer failed: {e}"))?;
    writer
        .write(&batch)
        .map_err(|e| format!("write local parquet batch failed: {e}"))?;
    writer
        .close()
        .map_err(|e| format!("close local parquet writer failed: {e}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BinaryArray, Int32Array, MapArray, StringArray, StructArray};
    use arrow_buffer::OffsetBuffer;

    fn single_entry_map_batch() -> RecordBatch {
        let entry_fields = vec![
            Arc::new(Field::new("key", DataType::Int32, false)),
            Arc::new(Field::new("value", DataType::Utf8, true)),
        ];
        let entries = StructArray::new(
            entry_fields.clone().into(),
            vec![
                Arc::new(Int32Array::from(vec![7])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("value")])) as ArrayRef,
            ],
            None,
        );
        let entries_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(entry_fields.into()),
            false,
        ));
        let map = Arc::new(MapArray::new(
            entries_field.clone(),
            OffsetBuffer::new(vec![0, 1].into()),
            entries,
            None,
            false,
        )) as ArrayRef;
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "m",
                DataType::Map(entries_field, false),
                true,
            )])),
            vec![map],
        )
        .expect("build map batch")
    }

    #[test]
    fn normalize_map_entries_nullability_recurses_through_nested_collections() {
        let entries = Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Arc::new(Field::new("key", DataType::Int32, false)),
                    Arc::new(Field::new("value", DataType::Utf8, true)),
                ]
                .into(),
            ),
            true,
        ));
        let nested = DataType::List(Arc::new(Field::new(
            "items",
            DataType::Map(entries, false),
            true,
        )));

        let DataType::List(items) = normalize_map_entries_nullability(&nested) else {
            panic!("expected LIST");
        };
        let DataType::Map(entries, ordered) = items.data_type() else {
            panic!("expected nested MAP");
        };
        assert!(!ordered);
        assert!(!entries.is_nullable());
        assert!(items.is_nullable());
    }

    #[test]
    fn cast_batch_to_schema_preserves_latin1_binary_payload() {
        let payload = [0x00, 0x7f, 0x80, 0xff];
        let source = RecordBatch::try_from_iter(vec![(
            "payload",
            Arc::new(BinaryArray::from_vec(vec![payload.as_slice()])) as ArrayRef,
        )])
        .expect("build source batch");
        let target = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Utf8,
            false,
        )]));

        let casted = cast_batch_to_schema(&source, &target).expect("cast binary payload");
        let values = casted
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("StringArray");
        assert_eq!(
            values
                .value(0)
                .chars()
                .map(|ch| ch as u32)
                .collect::<Vec<_>>(),
            vec![0x00, 0x7f, 0x80, 0xff]
        );
    }

    #[test]
    fn cast_batch_to_schema_preserves_utf8_binary_text() {
        let source = RecordBatch::try_from_iter(vec![(
            "text",
            Arc::new(BinaryArray::from_vec(vec!["中文".as_bytes()])) as ArrayRef,
        )])
        .expect("build source batch");
        let target = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, false)]));

        let casted = cast_batch_to_schema(&source, &target).expect("cast UTF-8 binary text");
        let values = casted
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("StringArray");
        assert_eq!(values.value(0), "中文");
    }

    #[test]
    fn cast_batch_to_schema_relaxes_map_key_nullability() {
        let source = single_entry_map_batch();
        let target_entries = Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Arc::new(Field::new("key", DataType::Int32, true)),
                    Arc::new(Field::new("value", DataType::Utf8, true)),
                ]
                .into(),
            ),
            false,
        ));
        let target = Arc::new(Schema::new(vec![Field::new(
            "m",
            DataType::Map(target_entries, false),
            true,
        )]));

        let casted = cast_batch_to_schema(&source, &target).expect("cast map batch");
        let casted_schema = casted.schema();
        let DataType::Map(entries, _) = casted_schema.field(0).data_type() else {
            panic!("expected MAP");
        };
        let DataType::Struct(fields) = entries.data_type() else {
            panic!("expected MAP entries STRUCT");
        };
        assert!(fields[0].is_nullable());
    }

    #[test]
    fn local_parquet_round_trip_preserves_non_null_map_keys() {
        let source = single_entry_map_batch();
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("map_round_trip.parquet");

        write_parquet_to_path(&path, &source).expect("write local parquet");
        let round_tripped =
            read_local_parquet_data(&path, &source.schema()).expect("read local parquet");
        let map = round_tripped
            .column(0)
            .as_any()
            .downcast_ref::<MapArray>()
            .expect("MapArray");
        let keys = map
            .entries()
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Int32Array");
        assert_eq!(keys.null_count(), 0);
        assert_eq!(keys.value(0), 7);
        let round_tripped_schema = round_tripped.schema();
        let DataType::Map(entries, _) = round_tripped_schema.field(0).data_type() else {
            panic!("expected MAP");
        };
        assert!(!entries.is_nullable());
    }
}
