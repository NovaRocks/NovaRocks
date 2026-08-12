// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements.  See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.

//! Iceberg equality-delete decoding and row visibility.
//!
//! Equality delete files are Iceberg physical facts.  The connector owns their
//! parquet decoding, field-ID matching, and scalar-key comparison; consumers
//! receive only the resulting row visibility decision.

use std::collections::HashSet;

use arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array, Float32Array,
    Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray,
    LargeStringArray, RecordBatch, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use novarocks_fs::{FileProjection, FileReadContext, FsAccessHandle};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::delete_file::{IcebergDeleteFileSpec, IcebergFileContent, IcebergFileFormat};
use crate::file_reader::read_parquet_batches;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum EqualityValue {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(u64),
    Utf8(String),
    Binary(Vec<u8>),
    Decimal128(i128, i8),
    Date32(i32),
    Date64(i64),
    Timestamp(i64, TimeUnit),
}

/// Decoded keys and authoritative Iceberg field references from one equality
/// delete file.  Its representation remains provider-private.
#[derive(Clone, Debug)]
pub struct EqualityDeleteSet {
    columns: Vec<EqualityColumnRef>,
    keys: HashSet<Vec<EqualityValue>>,
}

#[derive(Clone, Debug)]
struct EqualityColumnRef {
    name: String,
    field_id: Option<i32>,
}

/// Load equality delete files under the exact reader context that owns
/// process-local execution, deadline, and cancellation.
pub fn load_equality_delete_sets_with_context(
    specs: &[IcebergDeleteFileSpec],
    access: &FsAccessHandle,
    context: &FileReadContext,
) -> Result<Vec<EqualityDeleteSet>, String> {
    let mut sets = Vec::new();
    for spec in specs {
        if spec.file_content != IcebergFileContent::EqualityDeletes {
            continue;
        }
        if spec.file_format != IcebergFileFormat::Parquet
            || spec.content_offset.is_some()
            || spec.content_size_in_bytes.is_some()
        {
            return Err(format!(
                "iceberg equality-delete file {} has unsupported physical layout",
                spec.path
            ));
        }
        let batches = read_parquet_batches(
            access,
            &spec.path,
            spec.length,
            FileProjection::All,
            context.clone(),
        )?;
        sets.push(equality_delete_set_from_record_batches(
            &spec.path,
            batches.into_iter().map(|file_batch| file_batch.batch),
        )?);
    }
    Ok(sets)
}

/// Construct a provider equality-delete set from provider-decoded batches.
fn equality_delete_set_from_record_batches<I>(
    path: &str,
    batches: I,
) -> Result<EqualityDeleteSet, String>
where
    I: IntoIterator<Item = RecordBatch>,
{
    let batches = batches.into_iter().collect::<Vec<_>>();
    let Some(schema) = batches.first().map(RecordBatch::schema) else {
        return Ok(EqualityDeleteSet {
            columns: Vec::new(),
            keys: HashSet::new(),
        });
    };
    if schema.fields().is_empty() {
        return Err(format!(
            "iceberg equality-delete file {path} has no equality columns"
        ));
    }
    let columns = schema
        .fields()
        .iter()
        .map(|field| equality_column_ref(field.as_ref()))
        .collect::<Result<Vec<_>, _>>()?;
    let mut keys = HashSet::new();
    for batch in batches {
        if batch.num_columns() != columns.len() {
            return Err(format!(
                "equality-delete batch from {path} has {} columns, expected {}",
                batch.num_columns(),
                columns.len()
            ));
        }
        for row in 0..batch.num_rows() {
            keys.insert(equality_key_for_row(&batch, row, &columns)?);
        }
    }
    Ok(EqualityDeleteSet { columns, keys })
}

/// Return a keep mask only when at least one row is deleted.
pub fn equality_delete_keep_mask(
    batch: &RecordBatch,
    sets: &[EqualityDeleteSet],
) -> Result<Option<Vec<bool>>, String> {
    if sets.is_empty() || batch.num_rows() == 0 {
        return Ok(None);
    }
    let mut keep = Vec::with_capacity(batch.num_rows());
    let mut deleted_count = 0usize;
    for row in 0..batch.num_rows() {
        let deleted = row_matches_any_equality_delete(batch, row, sets)?;
        if deleted {
            deleted_count += 1;
        }
        keep.push(!deleted);
    }
    if deleted_count == 0 {
        return Ok(None);
    }
    Ok(Some(keep))
}

pub fn equality_delete_row_is_deleted(
    batch: &RecordBatch,
    row: usize,
    sets: &[EqualityDeleteSet],
) -> Result<bool, String> {
    if sets.is_empty() {
        return Ok(false);
    }
    row_matches_any_equality_delete(batch, row, sets)
}

fn row_matches_any_equality_delete(
    batch: &RecordBatch,
    row: usize,
    sets: &[EqualityDeleteSet],
) -> Result<bool, String> {
    for set in sets {
        let key = equality_key_for_row(batch, row, &set.columns)?;
        if set.keys.contains(&key) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn equality_key_for_row(
    batch: &RecordBatch,
    row: usize,
    columns: &[EqualityColumnRef],
) -> Result<Vec<EqualityValue>, String> {
    let schema = batch.schema();
    let mut key = Vec::with_capacity(columns.len());
    for column in columns {
        let idx = find_equality_column_index(schema.as_ref(), column)?;
        key.push(equality_value(batch.column(idx).as_ref(), row)?);
    }
    Ok(key)
}

fn equality_column_ref(field: &Field) -> Result<EqualityColumnRef, String> {
    Ok(EqualityColumnRef {
        name: field.name().to_ascii_lowercase(),
        field_id: parse_parquet_field_id(field)?,
    })
}

fn find_equality_column_index(
    schema: &Schema,
    column: &EqualityColumnRef,
) -> Result<usize, String> {
    if let Some(target_field_id) = column.field_id {
        let mut schema_has_field_ids = false;
        for (idx, field) in schema.fields().iter().enumerate() {
            let field_id = parse_parquet_field_id(field.as_ref())?;
            schema_has_field_ids |= field_id.is_some();
            if field_id == Some(target_field_id) {
                return Ok(idx);
            }
        }
        if schema_has_field_ids {
            return Err(equality_column_missing_error(schema, column));
        }
    }

    schema
        .fields()
        .iter()
        .position(|field| field.name().eq_ignore_ascii_case(&column.name))
        .ok_or_else(|| equality_column_missing_error(schema, column))
}

fn equality_column_missing_error(schema: &Schema, column: &EqualityColumnRef) -> String {
    let field_id = column
        .field_id
        .map(|id| format!(" field_id={id}"))
        .unwrap_or_default();
    format!(
        "equality-delete column `{}`{} is not available in data batch schema {:?}",
        column.name,
        field_id,
        schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
    )
}

fn parse_parquet_field_id(field: &Field) -> Result<Option<i32>, String> {
    let Some(raw) = field.metadata().get(PARQUET_FIELD_ID_META_KEY) else {
        return Ok(None);
    };
    raw.parse::<i32>().map(Some).map_err(|error| {
        format!(
            "invalid parquet field_id metadata: field={} key={} value={} error={}",
            field.name(),
            PARQUET_FIELD_ID_META_KEY,
            raw,
            error
        )
    })
}

fn equality_value(array: &dyn Array, row: usize) -> Result<EqualityValue, String> {
    if array.is_null(row) {
        return Ok(EqualityValue::Null);
    }
    match array.data_type() {
        DataType::Boolean => Ok(EqualityValue::Bool(
            array_as::<BooleanArray>(array)?.value(row),
        )),
        DataType::Int8 => Ok(EqualityValue::I64(i64::from(
            array_as::<Int8Array>(array)?.value(row),
        ))),
        DataType::Int16 => Ok(EqualityValue::I64(i64::from(
            array_as::<Int16Array>(array)?.value(row),
        ))),
        DataType::Int32 => Ok(EqualityValue::I64(i64::from(
            array_as::<Int32Array>(array)?.value(row),
        ))),
        DataType::Int64 => Ok(EqualityValue::I64(
            array_as::<Int64Array>(array)?.value(row),
        )),
        DataType::UInt8 => Ok(EqualityValue::U64(u64::from(
            array_as::<UInt8Array>(array)?.value(row),
        ))),
        DataType::UInt16 => Ok(EqualityValue::U64(u64::from(
            array_as::<UInt16Array>(array)?.value(row),
        ))),
        DataType::UInt32 => Ok(EqualityValue::U64(u64::from(
            array_as::<UInt32Array>(array)?.value(row),
        ))),
        DataType::UInt64 => Ok(EqualityValue::U64(
            array_as::<UInt64Array>(array)?.value(row),
        )),
        DataType::Float32 => Ok(EqualityValue::F64(
            f64::from(array_as::<Float32Array>(array)?.value(row)).to_bits(),
        )),
        DataType::Float64 => Ok(EqualityValue::F64(
            array_as::<Float64Array>(array)?.value(row).to_bits(),
        )),
        DataType::Utf8 => Ok(EqualityValue::Utf8(
            array_as::<StringArray>(array)?.value(row).to_string(),
        )),
        DataType::LargeUtf8 => Ok(EqualityValue::Utf8(
            array_as::<LargeStringArray>(array)?.value(row).to_string(),
        )),
        DataType::Binary => Ok(EqualityValue::Binary(
            array_as::<BinaryArray>(array)?.value(row).to_vec(),
        )),
        DataType::LargeBinary => Ok(EqualityValue::Binary(
            array_as::<LargeBinaryArray>(array)?.value(row).to_vec(),
        )),
        DataType::Decimal128(_, scale) => Ok(EqualityValue::Decimal128(
            array_as::<Decimal128Array>(array)?.value(row),
            *scale,
        )),
        DataType::Date32 => Ok(EqualityValue::Date32(
            array_as::<Date32Array>(array)?.value(row),
        )),
        DataType::Date64 => Ok(EqualityValue::Date64(
            array_as::<Date64Array>(array)?.value(row),
        )),
        DataType::Timestamp(TimeUnit::Second, _) => Ok(EqualityValue::Timestamp(
            array_as::<TimestampSecondArray>(array)?.value(row),
            TimeUnit::Second,
        )),
        DataType::Timestamp(TimeUnit::Millisecond, _) => Ok(EqualityValue::Timestamp(
            array_as::<TimestampMillisecondArray>(array)?.value(row),
            TimeUnit::Millisecond,
        )),
        DataType::Timestamp(TimeUnit::Microsecond, _) => Ok(EqualityValue::Timestamp(
            array_as::<TimestampMicrosecondArray>(array)?.value(row),
            TimeUnit::Microsecond,
        )),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => Ok(EqualityValue::Timestamp(
            array_as::<TimestampNanosecondArray>(array)?.value(row),
            TimeUnit::Nanosecond,
        )),
        other => Err(format!(
            "unsupported equality-delete column type for row filtering: {other:?}"
        )),
    }
}

fn array_as<T: 'static>(array: &dyn Array) -> Result<&T, String> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        format!(
            "array downcast failed for equality-delete filtering: {:?}",
            array.data_type()
        )
    })
}
