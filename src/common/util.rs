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
use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Decimal256Array,
    FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, LargeBinaryArray, LargeListArray, ListArray, MapArray, StringArray, StructArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use arrow_buffer::i256;
use chrono::{DateTime, Datelike, NaiveDate};
use std::cmp::Ordering;

use crate::common::largeint;
use crate::exec::variant::VariantValue;
use crate::types;

const FIELD_META_PRIMITIVE_TYPE: &str = "novarocks.primitive_type";
const FIELD_META_PRIMITIVE_JSON: &str = "JSON";

fn format_date32_for_mysql(days: i32) -> String {
    let Some(date) = NaiveDate::from_num_days_from_ce_opt(719163 + days) else {
        return NaiveDate::from_ymd_opt(1970, 1, 1)
            .unwrap()
            .format("%Y-%m-%d")
            .to_string();
    };

    // StarRocks renders the zero-date sentinel as 0000-00-00.
    if date.year() == -1 && date.month() == 11 && date.day() == 30 {
        "0000-00-00".to_string()
    } else {
        date.format("%Y-%m-%d").to_string()
    }
}

pub(crate) fn mysql_text_row_from_arrays(
    columns: &[ArrayRef],
    row: usize,
) -> Result<Vec<u8>, String> {
    mysql_text_row_from_arrays_with_primitives(columns, row, None)
}

pub(crate) fn mysql_text_row_from_arrays_with_primitives(
    columns: &[ArrayRef],
    row: usize,
    primitive_types: Option<&[types::TPrimitiveType]>,
) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    for (idx, col) in columns.iter().enumerate() {
        if col.is_null(row) {
            out.push(0xFB);
            continue;
        }
        let primitive = primitive_types
            .and_then(|prims| prims.get(idx))
            .copied()
            .unwrap_or(types::TPrimitiveType::INVALID_TYPE);
        match col.data_type() {
            DataType::Null => out.push(0xFB),
            DataType::Boolean => {
                let arr = col
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| "failed to downcast to BooleanArray".to_string())?;
                let bytes = if arr.value(row) { b"1" } else { b"0" };
                append_lenenc_string(&mut out, bytes);
            }
            DataType::Int64 => {
                let arr = col
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| "failed to downcast to Int64Array".to_string())?;
                append_lenenc_string(&mut out, arr.value(row).to_string().as_bytes());
            }
            DataType::Int32 => {
                let arr = col
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or_else(|| "failed to downcast to Int32Array".to_string())?;
                append_lenenc_string(&mut out, arr.value(row).to_string().as_bytes());
            }
            DataType::Int16 => {
                let arr = col
                    .as_any()
                    .downcast_ref::<Int16Array>()
                    .ok_or_else(|| "failed to downcast to Int16Array".to_string())?;
                append_lenenc_string(&mut out, arr.value(row).to_string().as_bytes());
            }
            DataType::Int8 => {
                let arr = col
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .ok_or_else(|| "failed to downcast to Int8Array".to_string())?;
                append_lenenc_string(&mut out, arr.value(row).to_string().as_bytes());
            }
            DataType::Float64 => {
                let arr = col
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| "failed to downcast to Float64Array".to_string())?;
                append_lenenc_string(&mut out, arr.value(row).to_string().as_bytes());
            }
            DataType::Float32 => {
                let arr = col
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .ok_or_else(|| "failed to downcast to Float32Array".to_string())?;
                append_lenenc_string(&mut out, arr.value(row).to_string().as_bytes());
            }
            DataType::Utf8 => {
                let arr = col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| "failed to downcast to StringArray".to_string())?;
                append_lenenc_string(&mut out, arr.value(row).as_bytes());
            }
            DataType::Date32 => {
                let arr = col
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .ok_or_else(|| "failed to downcast to Date32Array".to_string())?;
                let days = arr.value(row);
                let date_str = format_date32_for_mysql(days);
                append_lenenc_string(&mut out, date_str.as_bytes());
            }
            DataType::Timestamp(unit, tz) => {
                let tz = tz.as_deref();
                let ts = match unit {
                    TimeUnit::Second => {
                        let arr = col
                            .as_any()
                            .downcast_ref::<TimestampSecondArray>()
                            .ok_or_else(|| {
                                "failed to downcast to TimestampSecondArray".to_string()
                            })?;
                        let seconds = arr.value(row);
                        format_timestamp_with_primitive(TimeUnit::Second, seconds, tz, primitive)
                    }
                    TimeUnit::Millisecond => {
                        let arr = col
                            .as_any()
                            .downcast_ref::<TimestampMillisecondArray>()
                            .ok_or_else(|| {
                                "failed to downcast to TimestampMillisecondArray".to_string()
                            })?;
                        let millis = arr.value(row);
                        format_timestamp_with_primitive(
                            TimeUnit::Millisecond,
                            millis,
                            tz,
                            primitive,
                        )
                    }
                    TimeUnit::Microsecond => {
                        let arr = col
                            .as_any()
                            .downcast_ref::<TimestampMicrosecondArray>()
                            .ok_or_else(|| {
                                "failed to downcast to TimestampMicrosecondArray".to_string()
                            })?;
                        let micros = arr.value(row);
                        format_timestamp_with_primitive(
                            TimeUnit::Microsecond,
                            micros,
                            tz,
                            primitive,
                        )
                    }
                    TimeUnit::Nanosecond => {
                        let arr = col
                            .as_any()
                            .downcast_ref::<TimestampNanosecondArray>()
                            .ok_or_else(|| {
                                "failed to downcast to TimestampNanosecondArray".to_string()
                            })?;
                        let nanos = arr.value(row);
                        format_timestamp_with_primitive(TimeUnit::Nanosecond, nanos, tz, primitive)
                    }
                };
                append_lenenc_string(&mut out, ts.as_bytes());
            }
            DataType::Decimal128(_, scale) => {
                let arr = col
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .ok_or_else(|| "failed to downcast to Decimal128Array".to_string())?;
                let value = format_decimal(arr.value(row), *scale);
                append_lenenc_string(&mut out, value.as_bytes());
            }
            DataType::Decimal256(_, scale) => {
                let arr = col
                    .as_any()
                    .downcast_ref::<Decimal256Array>()
                    .ok_or_else(|| "failed to downcast to Decimal256Array".to_string())?;
                let value = format_decimal256(arr.value(row), *scale);
                append_lenenc_string(&mut out, value.as_bytes());
            }
            DataType::Binary => {
                let arr = col
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| "failed to downcast to BinaryArray".to_string())?;
                append_lenenc_string(&mut out, arr.value(row));
            }
            DataType::FixedSizeBinary(width) => {
                let arr = col
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .ok_or_else(|| "failed to downcast to FixedSizeBinaryArray".to_string())?;
                if primitive == types::TPrimitiveType::LARGEINT
                    || *width == largeint::LARGEINT_BYTE_WIDTH
                {
                    let value = largeint::i128_from_be_bytes(arr.value(row))?;
                    append_lenenc_string(&mut out, value.to_string().as_bytes());
                } else {
                    append_lenenc_string(&mut out, arr.value(row));
                }
            }
            DataType::LargeBinary => {
                let arr = col
                    .as_any()
                    .downcast_ref::<LargeBinaryArray>()
                    .ok_or_else(|| "failed to downcast to LargeBinaryArray".to_string())?;
                let json =
                    VariantValue::from_serialized(arr.value(row)).and_then(|v| v.to_json_local());
                match json {
                    Ok(text) => append_lenenc_string(&mut out, text.as_bytes()),
                    Err(_) => out.push(0xFB),
                }
            }
            DataType::List(_)
            | DataType::LargeList(_)
            | DataType::Map(_, _)
            | DataType::Struct(_) => {
                let value = format_mysql_container_value(col, row)?;
                append_lenenc_string(&mut out, value.as_bytes());
            }
            other => {
                return Err(format!(
                    "unsupported array type in mysql_text_row_from_arrays: {:?}",
                    other
                ));
            }
        }
    }
    Ok(out)
}

fn format_mysql_container_value(col: &ArrayRef, row: usize) -> Result<String, String> {
    if col.is_null(row) {
        return Ok("null".to_string());
    }
    match col.data_type() {
        DataType::Null => Ok("null".to_string()),
        DataType::Boolean => {
            let arr = col
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| "failed to downcast to BooleanArray".to_string())?;
            Ok(if arr.value(row) { "1" } else { "0" }.to_string())
        }
        DataType::Int64 => {
            let arr = col
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "failed to downcast to Int64Array".to_string())?;
            Ok(arr.value(row).to_string())
        }
        DataType::Int32 => {
            let arr = col
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "failed to downcast to Int32Array".to_string())?;
            Ok(arr.value(row).to_string())
        }
        DataType::Int16 => {
            let arr = col
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| "failed to downcast to Int16Array".to_string())?;
            Ok(arr.value(row).to_string())
        }
        DataType::Int8 => {
            let arr = col
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| "failed to downcast to Int8Array".to_string())?;
            Ok(arr.value(row).to_string())
        }
        DataType::Float64 => {
            let arr = col
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| "failed to downcast to Float64Array".to_string())?;
            Ok(arr.value(row).to_string())
        }
        DataType::Float32 => {
            let arr = col
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| "failed to downcast to Float32Array".to_string())?;
            Ok(arr.value(row).to_string())
        }
        DataType::Utf8 => {
            let arr = col
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "failed to downcast to StringArray".to_string())?;
            Ok(quote_mysql_container_string(arr.value(row)))
        }
        DataType::Date32 => {
            let arr = col
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| "failed to downcast to Date32Array".to_string())?;
            let days = arr.value(row);
            let date_str = format_date32_for_mysql(days);
            Ok(quote_mysql_container_string(&date_str))
        }
        DataType::Timestamp(unit, tz) => {
            let tz = tz.as_deref();
            let ts = match unit {
                TimeUnit::Second => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<TimestampSecondArray>()
                        .ok_or_else(|| "failed to downcast to TimestampSecondArray".to_string())?;
                    let seconds = arr.value(row);
                    format_timestamp(TimeUnit::Second, seconds, tz)
                }
                TimeUnit::Millisecond => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .ok_or_else(|| {
                            "failed to downcast to TimestampMillisecondArray".to_string()
                        })?;
                    let millis = arr.value(row);
                    format_timestamp(TimeUnit::Millisecond, millis, tz)
                }
                TimeUnit::Microsecond => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .ok_or_else(|| {
                            "failed to downcast to TimestampMicrosecondArray".to_string()
                        })?;
                    let micros = arr.value(row);
                    format_timestamp(TimeUnit::Microsecond, micros, tz)
                }
                TimeUnit::Nanosecond => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .ok_or_else(|| {
                            "failed to downcast to TimestampNanosecondArray".to_string()
                        })?;
                    let nanos = arr.value(row);
                    format_timestamp(TimeUnit::Nanosecond, nanos, tz)
                }
            };
            Ok(quote_mysql_container_string(&ts))
        }
        DataType::Decimal128(_, scale) => {
            let arr = col
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| "failed to downcast to Decimal128Array".to_string())?;
            Ok(format_decimal(arr.value(row), *scale))
        }
        DataType::Decimal256(_, scale) => {
            let arr = col
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| "failed to downcast to Decimal256Array".to_string())?;
            Ok(format_decimal256(arr.value(row), *scale))
        }
        DataType::Binary => {
            let arr = col
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| "failed to downcast to BinaryArray".to_string())?;
            Ok(quote_mysql_container_string(&String::from_utf8_lossy(
                arr.value(row),
            )))
        }
        DataType::FixedSizeBinary(width) => {
            let arr = col
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| "failed to downcast to FixedSizeBinaryArray".to_string())?;
            if *width == largeint::LARGEINT_BYTE_WIDTH {
                let value = largeint::i128_from_be_bytes(arr.value(row))?;
                Ok(value.to_string())
            } else {
                Ok(quote_mysql_container_string(&String::from_utf8_lossy(
                    arr.value(row),
                )))
            }
        }
        DataType::LargeBinary => {
            let arr = col
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| "failed to downcast to LargeBinaryArray".to_string())?;
            let json =
                VariantValue::from_serialized(arr.value(row)).and_then(|v| v.to_json_local());
            Ok(json.unwrap_or_else(|_| "null".to_string()))
        }
        DataType::List(_) => {
            let arr = col
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| "failed to downcast to ListArray".to_string())?;
            let offsets = arr.value_offsets();
            let start = offsets[row] as usize;
            let end = offsets[row + 1] as usize;
            let values = arr.values();
            let item_is_json = list_item_json_semantic(arr.data_type());
            let mut out = String::from("[");
            for i in start..end {
                if i > start {
                    out.push(',');
                }
                if item_is_json {
                    out.push_str(&format_mysql_container_json_value(&values, i)?);
                } else {
                    out.push_str(&format_mysql_container_value(&values, i)?);
                }
            }
            out.push(']');
            Ok(out)
        }
        DataType::LargeList(_) => {
            let arr = col
                .as_any()
                .downcast_ref::<LargeListArray>()
                .ok_or_else(|| "failed to downcast to LargeListArray".to_string())?;
            let offsets = arr.value_offsets();
            let start = offsets[row] as usize;
            let end = offsets[row + 1] as usize;
            let values = arr.values();
            let item_is_json = list_item_json_semantic(arr.data_type());
            let mut out = String::from("[");
            for i in start..end {
                if i > start {
                    out.push(',');
                }
                if item_is_json {
                    out.push_str(&format_mysql_container_json_value(&values, i)?);
                } else {
                    out.push_str(&format_mysql_container_value(&values, i)?);
                }
            }
            out.push(']');
            Ok(out)
        }
        DataType::Map(_, _) => {
            let arr = col
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(|| "failed to downcast to MapArray".to_string())?;
            let offsets = arr.offsets();
            let start = offsets[row] as usize;
            let end = offsets[row + 1] as usize;
            let keys = arr.keys();
            let values = arr.values();
            let map_value_is_json = map_value_json_semantic(arr.data_type());
            let mut out = String::from("{");
            let mut entry_indices: Vec<usize> = (start..end).collect();
            if !matches!(keys.data_type(), DataType::Utf8) {
                sort_map_entry_indices(keys, &mut entry_indices)?;
            }
            for (idx, entry_idx) in entry_indices.into_iter().enumerate() {
                if idx > 0 {
                    out.push(',');
                }
                let key = format_mysql_container_value(keys, entry_idx)?;
                let value = if map_value_is_json {
                    format_mysql_container_json_value(values, entry_idx)?
                } else {
                    format_mysql_container_value(values, entry_idx)?
                };
                out.push_str(&key);
                out.push(':');
                out.push_str(&value);
            }
            out.push('}');
            Ok(out)
        }
        DataType::Struct(_) => {
            let arr = col
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| "failed to downcast to StructArray".to_string())?;
            let fields = arr.fields();
            let mut out = String::from("{");
            for (idx, field) in fields.iter().enumerate() {
                if idx > 0 {
                    out.push(',');
                }
                out.push_str(&quote_mysql_container_string(field.name()));
                out.push(':');
                let child = arr.column(idx);
                out.push_str(&format_mysql_container_value(child, row)?);
            }
            out.push('}');
            Ok(out)
        }
        other => Err(format!(
            "unsupported array type in mysql_text_row_from_arrays: {:?}",
            other
        )),
    }
}

fn sort_map_entry_indices(keys: &ArrayRef, indices: &mut [usize]) -> Result<(), String> {
    for i in 1..indices.len() {
        let mut j = i;
        while j > 0 {
            let ord = compare_map_keys_ordered(keys, indices[j - 1], indices[j])?;
            if ord.is_gt() {
                indices.swap(j - 1, j);
                j -= 1;
            } else {
                break;
            }
        }
    }
    Ok(())
}

fn compare_map_keys_ordered(
    keys: &ArrayRef,
    left: usize,
    right: usize,
) -> Result<Ordering, String> {
    match (keys.is_null(left), keys.is_null(right)) {
        (true, true) => return Ok(Ordering::Equal),
        (true, false) => return Ok(Ordering::Less),
        (false, true) => return Ok(Ordering::Greater),
        (false, false) => {}
    }

    match keys.data_type() {
        DataType::Int8 => {
            let arr = keys
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| "failed to downcast map key to Int8Array".to_string())?;
            Ok(arr.value(left).cmp(&arr.value(right)))
        }
        DataType::Int16 => {
            let arr = keys
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| "failed to downcast map key to Int16Array".to_string())?;
            Ok(arr.value(left).cmp(&arr.value(right)))
        }
        DataType::Int32 => {
            let arr = keys
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "failed to downcast map key to Int32Array".to_string())?;
            Ok(arr.value(left).cmp(&arr.value(right)))
        }
        DataType::Int64 => {
            let arr = keys
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "failed to downcast map key to Int64Array".to_string())?;
            Ok(arr.value(left).cmp(&arr.value(right)))
        }
        DataType::Float32 => {
            let arr = keys
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| "failed to downcast map key to Float32Array".to_string())?;
            Ok(arr
                .value(left)
                .partial_cmp(&arr.value(right))
                .unwrap_or(Ordering::Equal))
        }
        DataType::Float64 => {
            let arr = keys
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| "failed to downcast map key to Float64Array".to_string())?;
            Ok(arr
                .value(left)
                .partial_cmp(&arr.value(right))
                .unwrap_or(Ordering::Equal))
        }
        DataType::Boolean => {
            let arr = keys
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| "failed to downcast map key to BooleanArray".to_string())?;
            Ok(arr.value(left).cmp(&arr.value(right)))
        }
        DataType::Utf8 => {
            let arr = keys
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "failed to downcast map key to StringArray".to_string())?;
            Ok(arr.value(left).cmp(arr.value(right)))
        }
        DataType::Date32 => {
            let arr = keys
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| "failed to downcast map key to Date32Array".to_string())?;
            Ok(arr.value(left).cmp(&arr.value(right)))
        }
        DataType::Decimal128(_, _) => {
            let arr = keys
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| "failed to downcast map key to Decimal128Array".to_string())?;
            Ok(arr.value(left).cmp(&arr.value(right)))
        }
        DataType::Decimal256(_, _) => {
            let arr = keys
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| "failed to downcast map key to Decimal256Array".to_string())?;
            Ok(arr.value(left).cmp(&arr.value(right)))
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let arr = keys
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .ok_or_else(|| "failed to downcast map key to TimestampSecondArray".to_string())?;
            Ok(arr.value(left).cmp(&arr.value(right)))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let arr = keys
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| {
                    "failed to downcast map key to TimestampMillisecondArray".to_string()
                })?;
            Ok(arr.value(left).cmp(&arr.value(right)))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let arr = keys
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| {
                    "failed to downcast map key to TimestampMicrosecondArray".to_string()
                })?;
            Ok(arr.value(left).cmp(&arr.value(right)))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let arr = keys
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| {
                    "failed to downcast map key to TimestampNanosecondArray".to_string()
                })?;
            Ok(arr.value(left).cmp(&arr.value(right)))
        }
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            let arr = keys
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| "failed to downcast map key to FixedSizeBinaryArray".to_string())?;
            let left_value = largeint::i128_from_be_bytes(arr.value(left))
                .map_err(|e| format!("map key LARGEINT decode failed: {}", e))?;
            let right_value = largeint::i128_from_be_bytes(arr.value(right))
                .map_err(|e| format!("map key LARGEINT decode failed: {}", e))?;
            Ok(left_value.cmp(&right_value))
        }
        _ => {
            let left_value = format_mysql_container_value(keys, left)?;
            let right_value = format_mysql_container_value(keys, right)?;
            Ok(left_value.cmp(&right_value))
        }
    }
}

fn quote_mysql_container_string(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for ch in value.chars() {
        if ch == '"' || ch == '\\' {
            out.push('\\');
        }
        out.push(ch);
    }
    out.push('"');
    out
}

fn quote_mysql_json_container_string(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('\'');
    for ch in value.chars() {
        if ch == '\'' || ch == '\\' {
            out.push('\\');
        }
        out.push(ch);
    }
    out.push('\'');
    out
}

fn map_value_json_semantic(data_type: &DataType) -> bool {
    let DataType::Map(entries, _) = data_type else {
        return false;
    };
    let DataType::Struct(fields) = entries.data_type() else {
        return false;
    };
    let Some(value_field) = fields.get(1) else {
        return false;
    };
    value_field
        .metadata()
        .get(FIELD_META_PRIMITIVE_TYPE)
        .is_some_and(|v| v == FIELD_META_PRIMITIVE_JSON)
}

fn list_item_json_semantic(data_type: &DataType) -> bool {
    match data_type {
        DataType::List(item) | DataType::LargeList(item) => item
            .metadata()
            .get(FIELD_META_PRIMITIVE_TYPE)
            .is_some_and(|v| v == FIELD_META_PRIMITIVE_JSON),
        _ => false,
    }
}

fn format_mysql_container_json_value(col: &ArrayRef, row: usize) -> Result<String, String> {
    if col.is_null(row) {
        return Ok("null".to_string());
    }
    match col.data_type() {
        DataType::Utf8 => {
            let arr = col
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "failed to downcast to StringArray".to_string())?;
            Ok(quote_mysql_json_container_string(arr.value(row)))
        }
        DataType::LargeBinary => {
            let arr = col
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| "failed to downcast to LargeBinaryArray".to_string())?;
            let text =
                VariantValue::from_serialized(arr.value(row)).and_then(|v| v.to_json_local());
            Ok(quote_mysql_json_container_string(
                &text.unwrap_or_else(|_| "null".to_string()),
            ))
        }
        _ => format_mysql_container_value(col, row),
    }
}

fn format_timestamp(unit: TimeUnit, value: i64, tz: Option<&str>) -> String {
    let timestamp_str = match unit {
        TimeUnit::Second => {
            let dt = DateTime::from_timestamp(value, 0)
                .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());
            dt.naive_utc().format("%Y-%m-%d %H:%M:%S").to_string()
        }
        TimeUnit::Millisecond => {
            let seconds = value / 1000;
            let nanos = ((value % 1000) * 1_000_000) as u32;
            let dt = DateTime::from_timestamp(seconds, nanos)
                .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());
            dt.naive_utc().format("%Y-%m-%d %H:%M:%S%.3f").to_string()
        }
        TimeUnit::Microsecond => {
            let seconds = value.div_euclid(1_000_000);
            let micros = value.rem_euclid(1_000_000) as u32;
            let nanos = micros * 1000;
            let dt = DateTime::from_timestamp(seconds, nanos)
                .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());
            if micros == 0 {
                dt.naive_utc().format("%Y-%m-%d %H:%M:%S").to_string()
            } else {
                dt.naive_utc().format("%Y-%m-%d %H:%M:%S%.6f").to_string()
            }
        }
        TimeUnit::Nanosecond => {
            let seconds = value / 1_000_000_000;
            let nanos = (value % 1_000_000_000) as u32;
            let dt = DateTime::from_timestamp(seconds, nanos)
                .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());
            dt.naive_utc().format("%Y-%m-%d %H:%M:%S%.9f").to_string()
        }
    };
    if let Some(tz) = tz {
        format!("{} {}", timestamp_str, tz)
    } else {
        timestamp_str
    }
}

fn format_time_duration(unit: TimeUnit, value: i64) -> String {
    let total_micros = match unit {
        TimeUnit::Second => value.saturating_mul(1_000_000),
        TimeUnit::Millisecond => value.saturating_mul(1_000),
        TimeUnit::Microsecond => value,
        TimeUnit::Nanosecond => value / 1_000,
    };
    let sign = if total_micros < 0 { "-" } else { "" };
    let abs = total_micros.unsigned_abs() as u64;
    let hour = abs / 3_600_000_000;
    let minute = (abs % 3_600_000_000) / 60_000_000;
    let second = (abs % 60_000_000) / 1_000_000;
    let micros = abs % 1_000_000;
    if micros == 0 {
        format!("{sign}{hour:02}:{minute:02}:{second:02}")
    } else {
        format!("{sign}{hour:02}:{minute:02}:{second:02}.{micros:06}")
    }
}

fn format_timestamp_with_primitive(
    unit: TimeUnit,
    value: i64,
    tz: Option<&str>,
    primitive: types::TPrimitiveType,
) -> String {
    if primitive == types::TPrimitiveType::TIME {
        return format_time_duration(unit, value);
    }
    format_timestamp(unit, value, tz)
}

fn format_decimal(unscaled: i128, scale: i8) -> String {
    let scale = scale as i32;
    if scale <= 0 {
        return unscaled.to_string();
    }

    let unscaled_str = unscaled.abs().to_string();
    let scale_usize = scale as usize;

    if unscaled_str.len() <= scale_usize {
        let padded = format!("{:0>width$}", unscaled_str, width = scale_usize);
        if unscaled < 0 {
            format!("-0.{}", padded)
        } else {
            format!("0.{}", padded)
        }
    } else {
        let split_pos = unscaled_str.len() - scale_usize;
        let integer_part = &unscaled_str[..split_pos];
        let fractional_part = &unscaled_str[split_pos..];
        if unscaled < 0 {
            format!("-{}.{}", integer_part, fractional_part)
        } else {
            format!("{}.{}", integer_part, fractional_part)
        }
    }
}

fn format_decimal256(unscaled: i256, scale: i8) -> String {
    let scale = scale as i32;
    if scale <= 0 {
        return unscaled.to_string();
    }

    let negative = unscaled.is_negative();
    let abs = if negative {
        unscaled.checked_neg().unwrap_or(unscaled)
    } else {
        unscaled
    };
    let abs_str = abs.to_string();
    let scale_usize = scale as usize;

    if abs_str.len() <= scale_usize {
        let padded = format!("{:0>width$}", abs_str, width = scale_usize);
        if negative {
            format!("-0.{}", padded)
        } else {
            format!("0.{}", padded)
        }
    } else {
        let split_pos = abs_str.len() - scale_usize;
        let integer_part = &abs_str[..split_pos];
        let fractional_part = &abs_str[split_pos..];
        if negative {
            format!("-{}.{}", integer_part, fractional_part)
        } else {
            format!("{}.{}", integer_part, fractional_part)
        }
    }
}

fn append_lenenc_string(out: &mut Vec<u8>, bytes: &[u8]) {
    append_lenenc_int(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}

fn append_lenenc_int(out: &mut Vec<u8>, v: u64) {
    if v < 251 {
        out.push(v as u8);
        return;
    }
    if v < 0x10000 {
        out.push(0xFC);
        out.push((v & 0xFF) as u8);
        out.push(((v >> 8) & 0xFF) as u8);
        return;
    }
    if v < 0x1000000 {
        out.push(0xFD);
        out.push((v & 0xFF) as u8);
        out.push(((v >> 8) & 0xFF) as u8);
        out.push(((v >> 16) & 0xFF) as u8);
        return;
    }
    out.push(0xFE);
    for i in 0..8 {
        out.push(((v >> (8 * i)) & 0xFF) as u8);
    }
}

#[cfg(test)]
mod tests {
    use super::format_timestamp;
    use arrow::datatypes::TimeUnit;

    #[test]
    fn format_timestamp_microsecond_omits_zero_fraction() {
        assert_eq!(
            format_timestamp(TimeUnit::Microsecond, 0, None),
            "1970-01-01 00:00:00"
        );
    }

    #[test]
    fn format_timestamp_microsecond_keeps_non_zero_fraction() {
        assert_eq!(
            format_timestamp(TimeUnit::Microsecond, 1, None),
            "1970-01-01 00:00:00.000001"
        );
    }
}
