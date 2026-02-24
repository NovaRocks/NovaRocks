//! StarRocks native write-side helpers.
//!
//! This module keeps format-level logic close to native format code:
//! - Sort rows by tablet sort key before segment encoding.
//! - Build minimal `SegmentMetadataPB` (`sort_key_min/max`, `num_rows`).

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Decimal256Array,
    FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, LargeBinaryArray, LargeStringArray, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow::compute::{SortColumn, SortOptions, lexsort_to_indices, take};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use arrow_buffer::i256;
use chrono::{DateTime, NaiveDate};

use crate::common::largeint;
use crate::service::grpc_client::proto::starrocks::{
    ColumnPb, KeysType, PScalarType, PTypeDesc, PTypeNode, SegmentMetadataPb, TabletSchemaPb,
    TuplePb, VariantPb, VariantTypePb,
};
use crate::types::TPrimitiveType;

const TYPE_NODE_SCALAR: i32 = 0;
const DATE32_UNIX_EPOCH_DAY_OFFSET: i32 = 719_163; // 1970-01-01 in proleptic Gregorian days

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SortKeyValueType {
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    Float,
    Double,
    Boolean,
    Date,
    Datetime,
    LargeInt,
    Varchar,
    Decimal { primitive: i32, scale: i8 },
}

pub fn sort_batch_for_native_write(
    batch: &RecordBatch,
    tablet_schema: &TabletSchemaPb,
) -> Result<RecordBatch, String> {
    let aligned_batch = align_batch_columns_to_schema(batch, tablet_schema)?;
    if aligned_batch.num_rows() <= 1 {
        return Ok(aligned_batch);
    }
    validate_keys_type_for_native_write(tablet_schema)?;
    let sort_key_indexes = resolve_sort_key_indexes(tablet_schema, aligned_batch.num_columns())?;

    let mut columns = Vec::with_capacity(sort_key_indexes.len() + 1);
    for col_idx in sort_key_indexes {
        columns.push(SortColumn {
            values: aligned_batch.column(col_idx).clone(),
            options: Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
        });
    }

    // Append row ordinal to enforce deterministic stable ordering for equal sort keys.
    let row_ordinal =
        UInt64Array::from_iter_values((0..aligned_batch.num_rows()).map(|v| v as u64));
    columns.push(SortColumn {
        values: Arc::new(row_ordinal),
        options: Some(SortOptions {
            descending: false,
            nulls_first: true,
        }),
    });

    let indices = lexsort_to_indices(&columns, None)
        .map_err(|e| format!("sort batch by sort key failed: {e}"))?;
    let mut sorted_columns = Vec::with_capacity(aligned_batch.num_columns());
    for col_idx in 0..aligned_batch.num_columns() {
        let sorted = take(aligned_batch.column(col_idx).as_ref(), &indices, None).map_err(|e| {
            format!(
                "reorder column by sorted indices failed: column_index={}, error={}",
                col_idx, e
            )
        })?;
        sorted_columns.push(sorted);
    }
    RecordBatch::try_new(aligned_batch.schema(), sorted_columns)
        .map_err(|e| format!("build sorted record batch failed: {e}"))
}

pub fn build_single_segment_metadata(
    sorted_batch: &RecordBatch,
    tablet_schema: &TabletSchemaPb,
) -> Result<SegmentMetadataPb, String> {
    let sorted_batch = align_batch_columns_to_schema(sorted_batch, tablet_schema)?;
    if sorted_batch.num_rows() == 0 {
        return Err("cannot build segment metadata from empty batch".to_string());
    }
    let sort_key_indexes = resolve_sort_key_indexes(tablet_schema, sorted_batch.num_columns())?;
    let sort_key_min = build_sort_key_tuple(&sorted_batch, tablet_schema, &sort_key_indexes, 0)?;
    let sort_key_max = build_sort_key_tuple(
        &sorted_batch,
        tablet_schema,
        &sort_key_indexes,
        sorted_batch.num_rows() - 1,
    )?;
    Ok(SegmentMetadataPb {
        sort_key_min: Some(sort_key_min),
        sort_key_max: Some(sort_key_max),
        num_rows: Some(sorted_batch.num_rows() as i64),
    })
}

fn is_positional_generated_field_name(field_name: &str, index: usize) -> bool {
    field_name
        .strip_prefix("col_")
        .and_then(|suffix| suffix.parse::<usize>().ok())
        .is_some_and(|generated_index| generated_index == index)
}

fn align_batch_columns_to_schema(
    batch: &RecordBatch,
    tablet_schema: &TabletSchemaPb,
) -> Result<RecordBatch, String> {
    if tablet_schema.column.is_empty() {
        return Ok(batch.clone());
    }
    if batch.num_columns() < tablet_schema.column.len() {
        return Err(format!(
            "batch/schema column mismatch for native writer: batch_columns={} schema_columns={}",
            batch.num_columns(),
            tablet_schema.column.len()
        ));
    }

    let mut name_to_batch_idx = HashMap::new();
    for (idx, field) in batch.schema().fields().iter().enumerate() {
        name_to_batch_idx.insert(field.name().to_ascii_lowercase(), idx);
    }

    let mut selected_indices = Vec::with_capacity(tablet_schema.column.len());
    for (schema_idx, schema_col) in tablet_schema.column.iter().enumerate() {
        let schema_name = schema_col
            .name
            .as_deref()
            .unwrap_or("")
            .trim()
            .to_ascii_lowercase();

        let batch_schema = batch.schema();
        let indexed_field = batch_schema.fields().get(schema_idx);
        let index_matches_name =
            indexed_field.is_some_and(|field| field.name().eq_ignore_ascii_case(&schema_name));
        let index_is_generated = indexed_field
            .is_some_and(|field| is_positional_generated_field_name(field.name(), schema_idx));

        let batch_idx = if index_matches_name || schema_name.is_empty() || index_is_generated {
            schema_idx
        } else if let Some(idx) = name_to_batch_idx.get(&schema_name) {
            *idx
        } else if schema_idx < batch.num_columns() {
            schema_idx
        } else {
            return Err(format!(
                "schema column not found in batch for native writer: schema_index={}, schema_name={}",
                schema_idx, schema_name
            ));
        };
        selected_indices.push(batch_idx);
    }

    let identity = selected_indices
        .iter()
        .enumerate()
        .all(|(idx, selected)| idx == *selected);
    let names_aligned = selected_indices
        .iter()
        .enumerate()
        .all(|(schema_idx, batch_idx)| {
            let schema_name = tablet_schema.column[schema_idx]
                .name
                .as_deref()
                .unwrap_or("")
                .trim();
            schema_name.is_empty()
                || batch
                    .schema()
                    .field(*batch_idx)
                    .name()
                    .eq_ignore_ascii_case(schema_name)
        });
    if identity && names_aligned {
        return Ok(batch.clone());
    }

    let mut aligned_columns = Vec::with_capacity(selected_indices.len());
    let mut aligned_fields = Vec::with_capacity(selected_indices.len());
    for (schema_idx, batch_idx) in selected_indices.iter().enumerate() {
        aligned_columns.push(batch.column(*batch_idx).clone());
        let source_field = batch.schema().field(*batch_idx).as_ref().clone();
        let schema_name = tablet_schema.column[schema_idx]
            .name
            .as_deref()
            .unwrap_or("")
            .trim();
        let aligned_field = if schema_name.is_empty() {
            source_field
        } else {
            source_field.with_name(schema_name.to_string())
        };
        aligned_fields.push(Arc::new(aligned_field));
    }

    let aligned_schema = Arc::new(Schema::new_with_metadata(
        aligned_fields,
        batch.schema().metadata().clone(),
    ));
    RecordBatch::try_new(aligned_schema, aligned_columns)
        .map_err(|e| format!("build schema-aligned record batch failed: {e}"))
}

fn validate_keys_type_for_native_write(tablet_schema: &TabletSchemaPb) -> Result<(), String> {
    let keys_type_raw = tablet_schema
        .keys_type
        .ok_or_else(|| "tablet schema missing keys_type for native write".to_string())?;
    let keys_type = KeysType::try_from(keys_type_raw).map_err(|_| {
        format!(
            "unknown keys_type in tablet schema for native write: {}",
            keys_type_raw
        )
    })?;
    let _ = keys_type;
    Ok(())
}

fn resolve_sort_key_indexes(
    tablet_schema: &TabletSchemaPb,
    output_columns: usize,
) -> Result<Vec<usize>, String> {
    if tablet_schema.sort_key_idxes.is_empty() {
        return Err("tablet schema missing sort_key_idxes for native write".to_string());
    }
    let mut indexes = Vec::with_capacity(tablet_schema.sort_key_idxes.len());
    for idx in &tablet_schema.sort_key_idxes {
        let idx_usize = usize::try_from(*idx)
            .map_err(|_| format!("invalid sort_key_idx in tablet schema: {}", idx))?;
        if idx_usize >= output_columns {
            return Err(format!(
                "sort_key_idx out of range in tablet schema: idx={} output_columns={}",
                idx_usize, output_columns
            ));
        }
        indexes.push(idx_usize);
    }
    Ok(indexes)
}

fn build_sort_key_tuple(
    batch: &RecordBatch,
    tablet_schema: &TabletSchemaPb,
    sort_key_indexes: &[usize],
    row_idx: usize,
) -> Result<TuplePb, String> {
    let mut values = Vec::with_capacity(sort_key_indexes.len());
    for col_idx in sort_key_indexes {
        let schema_col = tablet_schema.column.get(*col_idx).ok_or_else(|| {
            format!(
                "sort key column index out of range in tablet schema: idx={} columns={}",
                col_idx,
                tablet_schema.column.len()
            )
        })?;
        let array = batch.column(*col_idx);
        let value_type = parse_sort_key_value_type(schema_col)?;
        let variant = build_variant_for_value(array, schema_col, value_type, *col_idx, row_idx)?;
        values.push(variant);
    }
    Ok(TuplePb { values })
}

fn build_variant_for_value(
    array: &ArrayRef,
    schema_col: &ColumnPb,
    value_type: SortKeyValueType,
    col_idx: usize,
    row_idx: usize,
) -> Result<VariantPb, String> {
    let type_desc = build_scalar_type_desc(schema_col, value_type)?;
    if array.is_null(row_idx) {
        return Ok(VariantPb {
            r#type: Some(type_desc),
            value: None,
            variant_type: Some(VariantTypePb::NullValue as i32),
        });
    }
    let value = match value_type {
        SortKeyValueType::TinyInt
        | SortKeyValueType::SmallInt
        | SortKeyValueType::Int
        | SortKeyValueType::BigInt
        | SortKeyValueType::LargeInt => {
            extract_integral_sort_key_value(array, row_idx, col_idx)?.to_string()
        }
        SortKeyValueType::Boolean => {
            let typed = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    format!(
                        "sort-key type mismatch: expected Boolean array at column {}",
                        col_idx
                    )
                })?;
            if typed.value(row_idx) {
                "1".to_string()
            } else {
                "0".to_string()
            }
        }
        SortKeyValueType::Float => {
            let typed = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| {
                    format!(
                        "sort-key type mismatch: expected Float32 array at column {}",
                        col_idx
                    )
                })?;
            typed.value(row_idx).to_string()
        }
        SortKeyValueType::Double => {
            let typed = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    format!(
                        "sort-key type mismatch: expected Float64 array at column {}",
                        col_idx
                    )
                })?;
            typed.value(row_idx).to_string()
        }
        SortKeyValueType::Varchar => extract_varchar_sort_key_value(array, row_idx, col_idx)?,
        SortKeyValueType::Date => {
            let typed = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| {
                    format!(
                        "sort-key type mismatch: expected Date32 array at column {}",
                        col_idx
                    )
                })?;
            format_date32_sort_key_value(typed.value(row_idx))?
        }
        SortKeyValueType::Datetime => extract_datetime_sort_key_value(array, row_idx, col_idx)?,
        SortKeyValueType::Decimal { primitive, scale } => {
            if primitive == TPrimitiveType::DECIMAL256.0 {
                let typed = array
                    .as_any()
                    .downcast_ref::<Decimal256Array>()
                    .ok_or_else(|| {
                        format!(
                            "sort-key type mismatch: expected Decimal256 array at column {}",
                            col_idx
                        )
                    })?;
                format_decimal256_sort_key_value(typed.value(row_idx), scale)
            } else {
                let typed = array
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .ok_or_else(|| {
                        format!(
                            "sort-key type mismatch: expected Decimal128 array at column {}",
                            col_idx
                        )
                    })?;
                format_decimal_sort_key_value(typed.value(row_idx), scale)
            }
        }
    };

    Ok(VariantPb {
        r#type: Some(type_desc),
        value: Some(value),
        variant_type: Some(VariantTypePb::NormalValue as i32),
    })
}

fn parse_sort_key_value_type(col: &ColumnPb) -> Result<SortKeyValueType, String> {
    let type_name = col.r#type.trim().to_ascii_uppercase();
    let base_type = type_name.split('(').next().unwrap_or(type_name.as_str());
    match base_type {
        "TINYINT" => Ok(SortKeyValueType::TinyInt),
        "SMALLINT" => Ok(SortKeyValueType::SmallInt),
        "INT" => Ok(SortKeyValueType::Int),
        "BIGINT" => Ok(SortKeyValueType::BigInt),
        "LARGEINT" => Ok(SortKeyValueType::LargeInt),
        "FLOAT" => Ok(SortKeyValueType::Float),
        "DOUBLE" => Ok(SortKeyValueType::Double),
        "BOOLEAN" => Ok(SortKeyValueType::Boolean),
        "DATE" | "DATE_V2" => Ok(SortKeyValueType::Date),
        "DATETIME" | "DATETIME_V2" | "TIMESTAMP" => Ok(SortKeyValueType::Datetime),
        "CHAR" | "VARCHAR" | "STRING" | "BINARY" | "VARBINARY" => Ok(SortKeyValueType::Varchar),
        "DECIMAL32" => parse_decimal_sort_key_value_type(col, TPrimitiveType::DECIMAL32.0),
        "DECIMAL64" => parse_decimal_sort_key_value_type(col, TPrimitiveType::DECIMAL64.0),
        "DECIMAL128" => parse_decimal_sort_key_value_type(col, TPrimitiveType::DECIMAL128.0),
        "DECIMAL256" => parse_decimal_sort_key_value_type(col, TPrimitiveType::DECIMAL256.0),
        "DECIMAL" => parse_decimal_sort_key_value_type(col, TPrimitiveType::DECIMAL.0),
        "DECIMALV2" => parse_decimal_sort_key_value_type(col, TPrimitiveType::DECIMALV2.0),
        other => Err(format!(
            "unsupported sort-key schema type for segment metadata writer: {}",
            other
        )),
    }
}

fn build_scalar_type_desc(
    col: &ColumnPb,
    value_type: SortKeyValueType,
) -> Result<PTypeDesc, String> {
    let primitive = match value_type {
        SortKeyValueType::Boolean => TPrimitiveType::BOOLEAN.0,
        SortKeyValueType::TinyInt => TPrimitiveType::TINYINT.0,
        SortKeyValueType::SmallInt => TPrimitiveType::SMALLINT.0,
        SortKeyValueType::Int => TPrimitiveType::INT.0,
        SortKeyValueType::BigInt => TPrimitiveType::BIGINT.0,
        SortKeyValueType::LargeInt => TPrimitiveType::LARGEINT.0,
        SortKeyValueType::Float => TPrimitiveType::FLOAT.0,
        SortKeyValueType::Double => TPrimitiveType::DOUBLE.0,
        SortKeyValueType::Date => TPrimitiveType::DATE.0,
        SortKeyValueType::Datetime => TPrimitiveType::DATETIME.0,
        SortKeyValueType::Decimal { primitive, .. } => primitive,
        SortKeyValueType::Varchar => {
            let type_name = col.r#type.trim().to_ascii_uppercase();
            let base_type = type_name.split('(').next().unwrap_or(type_name.as_str());
            match base_type {
                "CHAR" => TPrimitiveType::CHAR.0,
                "STRING" => TPrimitiveType::VARCHAR.0,
                "VARCHAR" => TPrimitiveType::VARCHAR.0,
                "BINARY" => TPrimitiveType::BINARY.0,
                "VARBINARY" => TPrimitiveType::VARBINARY.0,
                other => {
                    return Err(format!(
                        "unsupported textual schema type for segment metadata writer: {}",
                        other
                    ));
                }
            }
        }
    };
    Ok(PTypeDesc {
        types: vec![PTypeNode {
            r#type: TYPE_NODE_SCALAR,
            scalar_type: Some(PScalarType {
                r#type: primitive,
                len: col.length,
                precision: col.precision,
                scale: col.frac,
            }),
            struct_fields: Vec::new(),
        }],
    })
}

fn parse_decimal_sort_key_value_type(
    col: &ColumnPb,
    primitive: i32,
) -> Result<SortKeyValueType, String> {
    let raw_scale = col
        .frac
        .ok_or_else(|| format!("decimal sort-key column missing scale: {}", col.r#type))?;
    let scale = i8::try_from(raw_scale).map_err(|_| {
        format!(
            "decimal sort-key column scale overflows i8: type={} scale={}",
            col.r#type, raw_scale
        )
    })?;
    if scale < 0 {
        return Err(format!(
            "decimal sort-key column has negative scale: type={} scale={}",
            col.r#type, scale
        ));
    }
    Ok(SortKeyValueType::Decimal { primitive, scale })
}

fn extract_integral_sort_key_value(
    array: &ArrayRef,
    row_idx: usize,
    col_idx: usize,
) -> Result<i128, String> {
    match array.data_type() {
        DataType::Int8 => {
            let typed = array.as_any().downcast_ref::<Int8Array>().ok_or_else(|| {
                format!(
                    "sort-key type mismatch: expected integral array at column {} (actual={:?})",
                    col_idx,
                    array.data_type()
                )
            })?;
            Ok(typed.value(row_idx) as i128)
        }
        DataType::Int16 => {
            let typed = array.as_any().downcast_ref::<Int16Array>().ok_or_else(|| {
                format!(
                    "sort-key type mismatch: expected integral array at column {} (actual={:?})",
                    col_idx,
                    array.data_type()
                )
            })?;
            Ok(typed.value(row_idx) as i128)
        }
        DataType::Int32 => {
            let typed = array.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                format!(
                    "sort-key type mismatch: expected integral array at column {} (actual={:?})",
                    col_idx,
                    array.data_type()
                )
            })?;
            Ok(typed.value(row_idx) as i128)
        }
        DataType::Int64 => {
            let typed = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                format!(
                    "sort-key type mismatch: expected integral array at column {} (actual={:?})",
                    col_idx,
                    array.data_type()
                )
            })?;
            Ok(typed.value(row_idx) as i128)
        }
        DataType::UInt8 => {
            let typed = array.as_any().downcast_ref::<UInt8Array>().ok_or_else(|| {
                format!(
                    "sort-key type mismatch: expected integral array at column {} (actual={:?})",
                    col_idx,
                    array.data_type()
                )
            })?;
            Ok(typed.value(row_idx) as i128)
        }
        DataType::UInt16 => {
            let typed = array.as_any().downcast_ref::<UInt16Array>().ok_or_else(|| {
                format!(
                    "sort-key type mismatch: expected integral array at column {} (actual={:?})",
                    col_idx,
                    array.data_type()
                )
            })?;
            Ok(typed.value(row_idx) as i128)
        }
        DataType::UInt32 => {
            let typed = array.as_any().downcast_ref::<UInt32Array>().ok_or_else(|| {
                format!(
                    "sort-key type mismatch: expected integral array at column {} (actual={:?})",
                    col_idx,
                    array.data_type()
                )
            })?;
            Ok(typed.value(row_idx) as i128)
        }
        DataType::UInt64 => {
            let typed = array.as_any().downcast_ref::<UInt64Array>().ok_or_else(|| {
                format!(
                    "sort-key type mismatch: expected integral array at column {} (actual={:?})",
                    col_idx,
                    array.data_type()
                )
            })?;
            Ok(typed.value(row_idx) as i128)
        }
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            let typed = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| {
                    format!(
                        "sort-key type mismatch: expected LARGEINT FixedSizeBinary array at column {}",
                        col_idx
                    )
                })?;
            largeint::i128_from_be_bytes(typed.value(row_idx)).map_err(|e| {
                format!(
                    "decode LARGEINT sort-key value failed: column={}, row={}, error={}",
                    col_idx, row_idx, e
                )
            })
        }
        other => Err(format!(
            "sort-key type mismatch: expected integral/LARGEINT array at column {} (actual={:?})",
            col_idx, other
        )),
    }
}

fn extract_varchar_sort_key_value(
    array: &ArrayRef,
    row_idx: usize,
    col_idx: usize,
) -> Result<String, String> {
    if let Some(typed) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(typed.value(row_idx).to_string());
    }
    if let Some(typed) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(typed.value(row_idx).to_string());
    }
    if let Some(typed) = array.as_any().downcast_ref::<BinaryArray>() {
        return Ok(String::from_utf8_lossy(typed.value(row_idx)).to_string());
    }
    if let Some(typed) = array.as_any().downcast_ref::<LargeBinaryArray>() {
        return Ok(String::from_utf8_lossy(typed.value(row_idx)).to_string());
    }
    Err(format!(
        "sort-key type mismatch: expected textual array at column {} (actual={:?})",
        col_idx,
        array.data_type()
    ))
}

fn extract_datetime_sort_key_value(
    array: &ArrayRef,
    row_idx: usize,
    col_idx: usize,
) -> Result<String, String> {
    let micros = match array.data_type() {
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, _) => {
            let typed = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| {
                    format!(
                        "sort-key type mismatch: expected timestamp array at column {}",
                        col_idx
                    )
                })?;
            typed.value(row_idx)
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, _) => {
            let typed = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| {
                    format!(
                        "sort-key type mismatch: expected timestamp array at column {}",
                        col_idx
                    )
                })?;
            typed.value(row_idx).saturating_mul(1_000)
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Second, _) => {
            let typed = array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .ok_or_else(|| {
                    format!(
                        "sort-key type mismatch: expected timestamp array at column {}",
                        col_idx
                    )
                })?;
            typed.value(row_idx).saturating_mul(1_000_000)
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, _) => {
            let typed = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| {
                    format!(
                        "sort-key type mismatch: expected timestamp array at column {}",
                        col_idx
                    )
                })?;
            typed.value(row_idx) / 1_000
        }
        other => {
            return Err(format!(
                "sort-key type mismatch: expected timestamp array at column {} (actual={:?})",
                col_idx, other
            ));
        }
    };
    format_datetime_sort_key_value(micros)
}

fn format_date32_sort_key_value(days_since_epoch: i32) -> Result<String, String> {
    let days_from_ce = DATE32_UNIX_EPOCH_DAY_OFFSET
        .checked_add(days_since_epoch)
        .ok_or_else(|| {
            format!(
                "date32 day overflow when formatting sort-key value: {}",
                days_since_epoch
            )
        })?;
    let date = NaiveDate::from_num_days_from_ce_opt(days_from_ce).ok_or_else(|| {
        format!(
            "invalid date32 value for sort-key formatting: {}",
            days_since_epoch
        )
    })?;
    Ok(date.format("%Y-%m-%d").to_string())
}

fn format_datetime_sort_key_value(unix_micros: i64) -> Result<String, String> {
    let dt = DateTime::from_timestamp_micros(unix_micros).ok_or_else(|| {
        format!(
            "invalid unix micros for datetime sort-key formatting: {}",
            unix_micros
        )
    })?;
    Ok(dt.naive_utc().format("%Y-%m-%d %H:%M:%S%.6f").to_string())
}

fn format_decimal_sort_key_value(unscaled: i128, scale: i8) -> String {
    if scale <= 0 {
        return unscaled.to_string();
    }
    let mut digits = unscaled.unsigned_abs().to_string();
    let scale = scale as usize;
    if digits.len() <= scale {
        let mut padded = String::with_capacity(scale + 1);
        for _ in 0..=(scale - digits.len()) {
            padded.push('0');
        }
        padded.push_str(&digits);
        digits = padded;
    }
    let split = digits.len() - scale;
    let sign = if unscaled < 0 { "-" } else { "" };
    format!("{sign}{}.{}", &digits[..split], &digits[split..])
}

fn format_decimal256_sort_key_value(unscaled: i256, scale: i8) -> String {
    if scale <= 0 {
        return unscaled.to_string();
    }
    let negative = unscaled.is_negative();
    let abs = if negative {
        unscaled.checked_neg().unwrap_or(unscaled)
    } else {
        unscaled
    };
    let mut digits = abs.to_string();
    let scale = scale as usize;
    if digits.len() <= scale {
        let mut padded = String::with_capacity(scale + 1);
        for _ in 0..=(scale - digits.len()) {
            padded.push('0');
        }
        padded.push_str(&digits);
        digits = padded;
    }
    let split = digits.len() - scale;
    let sign = if negative { "-" } else { "" };
    format!("{sign}{}.{}", &digits[..split], &digits[split..])
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        Date32Array, Decimal128Array, Decimal256Array, Int16Array, Int32Array, Int64Array,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use arrow_buffer::i256;

    use super::{align_batch_columns_to_schema, build_single_segment_metadata};
    use crate::common::largeint;
    use crate::service::grpc_client::proto::starrocks::{
        ColumnPb, KeysType, TabletSchemaPb, VariantTypePb,
    };
    use crate::types::TPrimitiveType;

    fn one_bigint_tablet_schema() -> TabletSchemaPb {
        TabletSchemaPb {
            keys_type: Some(KeysType::DupKeys as i32),
            column: vec![ColumnPb {
                unique_id: 0,
                name: Some("c1".to_string()),
                r#type: "BIGINT".to_string(),
                is_key: Some(true),
                aggregation: None,
                is_nullable: Some(true),
                default_value: None,
                precision: None,
                frac: None,
                length: None,
                index_length: None,
                is_bf_column: None,
                referenced_column_id: None,
                referenced_column: None,
                has_bitmap_index: None,
                visible: None,
                children_columns: Vec::new(),
                is_auto_increment: Some(false),
                agg_state_desc: None,
            }],
            num_short_key_columns: Some(1),
            num_rows_per_row_block: None,
            bf_fpp: None,
            next_column_unique_id: Some(1),
            deprecated_is_in_memory: None,
            deprecated_id: None,
            compression_type: None,
            sort_key_idxes: vec![0],
            schema_version: Some(0),
            sort_key_unique_ids: vec![0],
            table_indices: Vec::new(),
            compression_level: None,
            id: Some(1),
        }
    }

    fn one_date_tablet_schema() -> TabletSchemaPb {
        TabletSchemaPb {
            keys_type: Some(KeysType::DupKeys as i32),
            column: vec![ColumnPb {
                unique_id: 0,
                name: Some("d1".to_string()),
                r#type: "DATE".to_string(),
                is_key: Some(true),
                aggregation: None,
                is_nullable: Some(true),
                default_value: None,
                precision: None,
                frac: None,
                length: None,
                index_length: None,
                is_bf_column: None,
                referenced_column_id: None,
                referenced_column: None,
                has_bitmap_index: None,
                visible: None,
                children_columns: Vec::new(),
                is_auto_increment: Some(false),
                agg_state_desc: None,
            }],
            num_short_key_columns: Some(1),
            num_rows_per_row_block: None,
            bf_fpp: None,
            next_column_unique_id: Some(1),
            deprecated_is_in_memory: None,
            deprecated_id: None,
            compression_type: None,
            sort_key_idxes: vec![0],
            schema_version: Some(0),
            sort_key_unique_ids: vec![0],
            table_indices: Vec::new(),
            compression_level: None,
            id: Some(1),
        }
    }

    fn one_decimal32_tablet_schema() -> TabletSchemaPb {
        TabletSchemaPb {
            keys_type: Some(KeysType::DupKeys as i32),
            column: vec![ColumnPb {
                unique_id: 0,
                name: Some("d32".to_string()),
                r#type: "DECIMAL32".to_string(),
                is_key: Some(true),
                aggregation: None,
                is_nullable: Some(true),
                default_value: None,
                precision: Some(2),
                frac: Some(1),
                length: None,
                index_length: None,
                is_bf_column: None,
                referenced_column_id: None,
                referenced_column: None,
                has_bitmap_index: None,
                visible: None,
                children_columns: Vec::new(),
                is_auto_increment: Some(false),
                agg_state_desc: None,
            }],
            num_short_key_columns: Some(1),
            num_rows_per_row_block: None,
            bf_fpp: None,
            next_column_unique_id: Some(1),
            deprecated_is_in_memory: None,
            deprecated_id: None,
            compression_type: None,
            sort_key_idxes: vec![0],
            schema_version: Some(0),
            sort_key_unique_ids: vec![0],
            table_indices: Vec::new(),
            compression_level: None,
            id: Some(1),
        }
    }

    fn one_decimal256_tablet_schema() -> TabletSchemaPb {
        TabletSchemaPb {
            keys_type: Some(KeysType::DupKeys as i32),
            column: vec![ColumnPb {
                unique_id: 0,
                name: Some("d256".to_string()),
                r#type: "DECIMAL256".to_string(),
                is_key: Some(true),
                aggregation: None,
                is_nullable: Some(true),
                default_value: None,
                precision: Some(40),
                frac: Some(1),
                length: None,
                index_length: None,
                is_bf_column: None,
                referenced_column_id: None,
                referenced_column: None,
                has_bitmap_index: None,
                visible: None,
                children_columns: Vec::new(),
                is_auto_increment: Some(false),
                agg_state_desc: None,
            }],
            num_short_key_columns: Some(1),
            num_rows_per_row_block: None,
            bf_fpp: None,
            next_column_unique_id: Some(1),
            deprecated_is_in_memory: None,
            deprecated_id: None,
            compression_type: None,
            sort_key_idxes: vec![0],
            schema_version: Some(0),
            sort_key_unique_ids: vec![0],
            table_indices: Vec::new(),
            compression_level: None,
            id: Some(1),
        }
    }

    fn one_tinyint_tablet_schema() -> TabletSchemaPb {
        TabletSchemaPb {
            keys_type: Some(KeysType::DupKeys as i32),
            column: vec![ColumnPb {
                unique_id: 0,
                name: Some("t1".to_string()),
                r#type: "TINYINT".to_string(),
                is_key: Some(true),
                aggregation: None,
                is_nullable: Some(true),
                default_value: None,
                precision: None,
                frac: None,
                length: None,
                index_length: None,
                is_bf_column: None,
                referenced_column_id: None,
                referenced_column: None,
                has_bitmap_index: None,
                visible: None,
                children_columns: Vec::new(),
                is_auto_increment: Some(false),
                agg_state_desc: None,
            }],
            num_short_key_columns: Some(1),
            num_rows_per_row_block: None,
            bf_fpp: None,
            next_column_unique_id: Some(1),
            deprecated_is_in_memory: None,
            deprecated_id: None,
            compression_type: None,
            sort_key_idxes: vec![0],
            schema_version: Some(0),
            sort_key_unique_ids: vec![0],
            table_indices: Vec::new(),
            compression_level: None,
            id: Some(1),
        }
    }

    fn one_largeint_tablet_schema() -> TabletSchemaPb {
        TabletSchemaPb {
            keys_type: Some(KeysType::DupKeys as i32),
            column: vec![ColumnPb {
                unique_id: 0,
                name: Some("l1".to_string()),
                r#type: "LARGEINT".to_string(),
                is_key: Some(true),
                aggregation: None,
                is_nullable: Some(true),
                default_value: None,
                precision: None,
                frac: None,
                length: None,
                index_length: None,
                is_bf_column: None,
                referenced_column_id: None,
                referenced_column: None,
                has_bitmap_index: None,
                visible: None,
                children_columns: Vec::new(),
                is_auto_increment: Some(false),
                agg_state_desc: None,
            }],
            num_short_key_columns: Some(1),
            num_rows_per_row_block: None,
            bf_fpp: None,
            next_column_unique_id: Some(1),
            deprecated_is_in_memory: None,
            deprecated_id: None,
            compression_type: None,
            sort_key_idxes: vec![0],
            schema_version: Some(0),
            sort_key_unique_ids: vec![0],
            table_indices: Vec::new(),
            compression_level: None,
            id: Some(1),
        }
    }

    #[test]
    fn segment_meta_alignment_keeps_positional_generated_columns() {
        let schema = TabletSchemaPb {
            keys_type: Some(KeysType::DupKeys as i32),
            column: vec![
                ColumnPb {
                    unique_id: 1,
                    name: Some("col_1".to_string()),
                    r#type: "INT".to_string(),
                    is_key: Some(true),
                    is_nullable: Some(true),
                    ..Default::default()
                },
                ColumnPb {
                    unique_id: 2,
                    name: Some("col_2".to_string()),
                    r#type: "INT".to_string(),
                    is_key: Some(false),
                    is_nullable: Some(true),
                    ..Default::default()
                },
                ColumnPb {
                    unique_id: 3,
                    name: Some("col_3".to_string()),
                    r#type: "INT".to_string(),
                    is_key: Some(false),
                    is_nullable: Some(true),
                    ..Default::default()
                },
            ],
            num_short_key_columns: Some(1),
            sort_key_idxes: vec![0],
            sort_key_unique_ids: vec![1],
            ..Default::default()
        };
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("col_0", DataType::Int32, true),
                Field::new("col_1", DataType::Int32, true),
                Field::new("col_2", DataType::Int32, true),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![Some(1), Some(4)])),
                Arc::new(Int32Array::from(vec![Some(2), Some(5)])),
                Arc::new(Int32Array::from(vec![Some(3), Some(6)])),
            ],
        )
        .expect("build generated-name batch");

        let aligned =
            align_batch_columns_to_schema(&batch, &schema).expect("align generated-name batch");
        let c0 = aligned
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("aligned column0");
        let c1 = aligned
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("aligned column1");
        let c2 = aligned
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("aligned column2");

        assert_eq!(c0.value(0), 1);
        assert_eq!(c1.value(0), 2);
        assert_eq!(c2.value(0), 3);
        assert_eq!(aligned.schema().field(0).name(), "col_1");
        assert_eq!(aligned.schema().field(1).name(), "col_2");
        assert_eq!(aligned.schema().field(2).name(), "col_3");
    }

    #[test]
    fn segment_meta_uses_normal_variant_for_non_null_sort_key() {
        let schema = Arc::new(Schema::new(vec![Field::new("c1", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![Some(1_i64), Some(9_i64)]))],
        )
        .expect("build record batch");
        let segment_meta =
            build_single_segment_metadata(&batch, &one_bigint_tablet_schema()).expect("meta");
        let min = segment_meta
            .sort_key_min
            .as_ref()
            .expect("sort_key_min")
            .values
            .first()
            .expect("min value");
        let max = segment_meta
            .sort_key_max
            .as_ref()
            .expect("sort_key_max")
            .values
            .first()
            .expect("max value");
        assert_eq!(min.variant_type, Some(VariantTypePb::NormalValue as i32));
        assert_eq!(min.value.as_deref(), Some("1"));
        assert_eq!(max.variant_type, Some(VariantTypePb::NormalValue as i32));
        assert_eq!(max.value.as_deref(), Some("9"));
    }

    #[test]
    fn segment_meta_uses_null_variant_for_null_sort_key() {
        let schema = Arc::new(Schema::new(vec![Field::new("c1", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![None, Some(8_i64)]))],
        )
        .expect("build record batch");
        let segment_meta =
            build_single_segment_metadata(&batch, &one_bigint_tablet_schema()).expect("meta");
        let min = segment_meta
            .sort_key_min
            .as_ref()
            .expect("sort_key_min")
            .values
            .first()
            .expect("min value");
        assert_eq!(min.variant_type, Some(VariantTypePb::NullValue as i32));
        assert!(min.value.is_none());
    }

    #[test]
    fn segment_meta_formats_date_sort_key_values() {
        let schema = Arc::new(Schema::new(vec![Field::new("d1", DataType::Date32, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Date32Array::from(vec![Some(0_i32), Some(2_i32)]))],
        )
        .expect("build record batch");
        let segment_meta =
            build_single_segment_metadata(&batch, &one_date_tablet_schema()).expect("meta");
        let min = segment_meta
            .sort_key_min
            .as_ref()
            .expect("sort_key_min")
            .values
            .first()
            .expect("min value");
        let max = segment_meta
            .sort_key_max
            .as_ref()
            .expect("sort_key_max")
            .values
            .first()
            .expect("max value");
        assert_eq!(min.variant_type, Some(VariantTypePb::NormalValue as i32));
        assert_eq!(min.value.as_deref(), Some("1970-01-01"));
        assert_eq!(max.value.as_deref(), Some("1970-01-03"));
        let primitive = min
            .r#type
            .as_ref()
            .and_then(|t| t.types.first())
            .and_then(|n| n.scalar_type.as_ref())
            .map(|s| s.r#type)
            .expect("primitive type");
        assert_eq!(primitive, TPrimitiveType::DATE.0);
    }

    #[test]
    fn segment_meta_formats_decimal_sort_key_values() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "d32",
            DataType::Decimal128(2, 1),
            true,
        )]));
        let values = Decimal128Array::from(vec![Some(12_i128), Some(20_i128)])
            .with_precision_and_scale(2, 1)
            .expect("decimal array");
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(values)]).expect("build record batch");
        let segment_meta =
            build_single_segment_metadata(&batch, &one_decimal32_tablet_schema()).expect("meta");
        let min = segment_meta
            .sort_key_min
            .as_ref()
            .expect("sort_key_min")
            .values
            .first()
            .expect("min value");
        let max = segment_meta
            .sort_key_max
            .as_ref()
            .expect("sort_key_max")
            .values
            .first()
            .expect("max value");
        assert_eq!(min.variant_type, Some(VariantTypePb::NormalValue as i32));
        assert_eq!(min.value.as_deref(), Some("1.2"));
        assert_eq!(max.value.as_deref(), Some("2.0"));
        let primitive = min
            .r#type
            .as_ref()
            .and_then(|t| t.types.first())
            .and_then(|n| n.scalar_type.as_ref())
            .map(|s| s.r#type)
            .expect("primitive type");
        assert_eq!(primitive, TPrimitiveType::DECIMAL32.0);
    }

    #[test]
    fn segment_meta_formats_decimal256_sort_key_values() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "d256",
            DataType::Decimal256(40, 1),
            true,
        )]));
        let values =
            Decimal256Array::from(vec![Some(i256::from_i128(12)), Some(i256::from_i128(20))])
                .with_precision_and_scale(40, 1)
                .expect("decimal256 array");
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(values)]).expect("build record batch");
        let segment_meta =
            build_single_segment_metadata(&batch, &one_decimal256_tablet_schema()).expect("meta");
        let min = segment_meta
            .sort_key_min
            .as_ref()
            .expect("sort_key_min")
            .values
            .first()
            .expect("min value");
        let max = segment_meta
            .sort_key_max
            .as_ref()
            .expect("sort_key_max")
            .values
            .first()
            .expect("max value");
        assert_eq!(min.variant_type, Some(VariantTypePb::NormalValue as i32));
        assert_eq!(min.value.as_deref(), Some("1.2"));
        assert_eq!(max.value.as_deref(), Some("2.0"));
        let primitive = min
            .r#type
            .as_ref()
            .and_then(|t| t.types.first())
            .and_then(|n| n.scalar_type.as_ref())
            .map(|s| s.r#type)
            .expect("primitive type");
        assert_eq!(primitive, TPrimitiveType::DECIMAL256.0);
    }

    #[test]
    fn segment_meta_accepts_wider_integral_array_for_tinyint_schema() {
        let schema = Arc::new(Schema::new(vec![Field::new("t1", DataType::Int16, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int16Array::from(vec![Some(1_i16), Some(7_i16)]))],
        )
        .expect("build record batch");
        let segment_meta =
            build_single_segment_metadata(&batch, &one_tinyint_tablet_schema()).expect("meta");
        let min = segment_meta
            .sort_key_min
            .as_ref()
            .expect("sort_key_min")
            .values
            .first()
            .expect("min value");
        assert_eq!(min.value.as_deref(), Some("1"));
    }

    #[test]
    fn segment_meta_supports_largeint_sort_key_schema() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "l1",
            DataType::FixedSizeBinary(largeint::LARGEINT_BYTE_WIDTH),
            true,
        )]));
        let values = largeint::array_from_i128(&[Some(3_i128), Some(11_i128)]).expect("array");
        let batch = RecordBatch::try_new(schema, vec![values]).expect("build record batch");
        let segment_meta =
            build_single_segment_metadata(&batch, &one_largeint_tablet_schema()).expect("meta");
        let min = segment_meta
            .sort_key_min
            .as_ref()
            .expect("sort_key_min")
            .values
            .first()
            .expect("min value");
        let max = segment_meta
            .sort_key_max
            .as_ref()
            .expect("sort_key_max")
            .values
            .first()
            .expect("max value");
        assert_eq!(min.value.as_deref(), Some("3"));
        assert_eq!(max.value.as_deref(), Some("11"));
        let primitive = max
            .r#type
            .as_ref()
            .and_then(|t| t.types.first())
            .and_then(|n| n.scalar_type.as_ref())
            .map(|s| s.r#type)
            .expect("primitive type");
        assert_eq!(primitive, TPrimitiveType::LARGEINT.0);
    }
}
