//! Parquet I/O for local (on-disk) tables: read a column-set from a parquet
//! file, cast/normalize batches to match the table schema, and write batches
//! back to disk.
//!
//! This module also owns the small string-to-date/datetime parsing helpers
//! used while coercing literal rows into typed arrow arrays for local-parquet
//! writes.

use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use super::ColumnDef;
use crate::standalone::engine::iceberg_glue::concat_or_empty_batches;

pub(crate) fn read_local_parquet_data(
    path: &Path,
    columns: &[ColumnDef],
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
    let batch = concat_or_empty_batches(columns, batches)?;
    let target_schema = Arc::new(Schema::new(
        columns
            .iter()
            .map(|c| Field::new(&c.name, c.data_type.clone(), c.nullable))
            .collect::<Vec<_>>(),
    ));
    cast_batch_to_schema(&batch, &target_schema)
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

pub(crate) fn parse_date_string_to_days(s: &str) -> Result<i32, String> {
    use chrono::NaiveDate;
    let date = NaiveDate::parse_from_str(s.trim(), "%Y-%m-%d")
        .map_err(|e| format!("invalid date literal `{s}`: {e}"))?;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
    Ok((date - epoch).num_days() as i32)
}

pub(crate) fn parse_datetime_string_to_micros(s: &str) -> Result<i64, String> {
    use chrono::NaiveDateTime;
    let s = s.trim();
    // Try datetime first, then date-only
    if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        return Ok(dt.and_utc().timestamp_micros());
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f") {
        return Ok(dt.and_utc().timestamp_micros());
    }
    if let Ok(d) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        let dt = d.and_hms_opt(0, 0, 0).expect("midnight");
        return Ok(dt.and_utc().timestamp_micros());
    }
    Err(format!("invalid datetime literal `{s}`"))
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
        crate::exec::expr::cast_with_special_rules(&list.values(), target_entries.data_type())?
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

    fn encode_bytes(bytes: &[u8]) -> String {
        bytes.iter().map(|b| char::from(*b)).collect()
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
                    .map(|row| (!arr.is_null(row)).then(|| encode_bytes(arr.value(row))))
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
                    .map(|row| (!arr.is_null(row)).then(|| encode_bytes(arr.value(row))))
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
                    .map(|row| (!arr.is_null(row)).then(|| encode_bytes(arr.value(row))))
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
                    .map(|row| (!arr.is_null(row)).then(|| encode_bytes(arr.value(row))))
                    .collect::<Vec<_>>(),
            )) as ArrayRef)
        }
        (source_type, DataType::Date32) if is_numeric_datetime_source(source_type) => {
            crate::exec::expr::cast_with_special_rules(source_col, target_field.data_type())
        }
        (source_type, DataType::Timestamp(_, _)) if is_numeric_datetime_source(source_type) => {
            crate::exec::expr::cast_with_special_rules(source_col, target_field.data_type())
        }
        (_, DataType::FixedSizeBinary(width))
            if *width == crate::common::largeint::LARGEINT_BYTE_WIDTH =>
        {
            crate::exec::expr::cast_with_special_rules(source_col, target_field.data_type())
        }
        (DataType::List(source_field), DataType::Map(target_entries, ordered))
            if matches!(source_field.data_type(), DataType::Struct(_)) =>
        {
            cast_list_struct_to_map_for_local_schema(source_col, target_entries, *ordered)
        }
        (_, DataType::List(_) | DataType::Struct(_) | DataType::Map(_, _)) => {
            crate::exec::expr::cast_with_special_rules(source_col, target_field.data_type())
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
