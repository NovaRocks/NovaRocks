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
    Array, ArrayRef, BooleanArray, Date32Array, Date32Builder, Decimal128Array,
    FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, ListArray, MapArray, StringArray, StructArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, TimeUnit};
use std::cmp::Ordering;
use std::sync::Arc;

use crate::common::largeint;
use crate::exec::expr::function::date::common::{
    naive_to_date32, naive_to_timestamp_micros, parse_date, parse_datetime,
};

pub(super) fn row_index(row: usize, len: usize) -> usize {
    if len == 1 { 0 } else { row }
}

pub(super) fn cast_output(
    out: ArrayRef,
    output_type: Option<&DataType>,
    fn_name: &str,
) -> Result<ArrayRef, String> {
    let Some(target) = output_type else {
        return Ok(out);
    };
    if out.data_type() == target {
        return Ok(out);
    }
    cast(&out, target).map_err(|e| format!("{}: failed to cast output: {}", fn_name, e))
}

fn cast_utf8_to_date32(array: &ArrayRef) -> Result<ArrayRef, String> {
    let arr = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "failed to downcast to StringArray".to_string())?;
    let mut builder = Date32Builder::new();
    for i in 0..arr.len() {
        if arr.is_null(i) {
            builder.append_null();
            continue;
        }
        let raw = arr.value(i).trim();
        let days = if let Some(date) = parse_date(raw) {
            naive_to_date32(date)
        } else if let Some(dt) = parse_datetime(raw) {
            naive_to_date32(dt.date())
        } else {
            return Err(format!("invalid date literal '{}'", arr.value(i)));
        };
        builder.append_value(days);
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn cast_utf8_to_timestamp(array: &ArrayRef, target_type: &DataType) -> Result<ArrayRef, String> {
    let arr = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "failed to downcast to StringArray".to_string())?;
    let mut values = Vec::with_capacity(arr.len());
    for i in 0..arr.len() {
        if arr.is_null(i) {
            values.push(None);
            continue;
        }
        let raw = arr.value(i).trim();
        let micros = if let Some(dt) = parse_datetime(raw) {
            naive_to_timestamp_micros(dt)
        } else if let Some(date) = parse_date(raw) {
            let midnight = date
                .and_hms_opt(0, 0, 0)
                .ok_or_else(|| format!("invalid timestamp literal '{}'", arr.value(i)))?;
            naive_to_timestamp_micros(midnight)
        } else {
            return Err(format!("invalid timestamp literal '{}'", arr.value(i)));
        };
        values.push(Some(micros));
    }
    let micros_array = Arc::new(TimestampMicrosecondArray::from(values)) as ArrayRef;
    if matches!(
        target_type,
        DataType::Timestamp(TimeUnit::Microsecond, None)
    ) {
        return Ok(micros_array);
    }
    cast(&micros_array, target_type).map_err(|e| e.to_string())
}

fn format_decimal(unscaled: i128, scale: i8) -> String {
    if scale <= 0 {
        return unscaled.to_string();
    }
    let scale = scale as usize;
    let abs = unscaled.abs().to_string();
    if abs.len() <= scale {
        let frac = format!("{:0>width$}", abs, width = scale);
        if unscaled < 0 {
            format!("-0.{}", frac)
        } else {
            format!("0.{}", frac)
        }
    } else {
        let split = abs.len() - scale;
        let integer = &abs[..split];
        let frac = &abs[split..];
        if unscaled < 0 {
            format!("-{}.{}", integer, frac)
        } else {
            format!("{}.{}", integer, frac)
        }
    }
}

fn cast_decimal_to_utf8(array: &ArrayRef) -> Result<ArrayRef, String> {
    let (arr, scale) = match array.data_type() {
        DataType::Decimal128(_, scale) => (
            array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| "failed to downcast Decimal128Array".to_string())?,
            *scale,
        ),
        other => {
            return Err(format!(
                "cast_decimal_to_utf8 expects Decimal128Array, got {:?}",
                other
            ));
        }
    };
    let mut out = Vec::with_capacity(arr.len());
    for row in 0..arr.len() {
        if arr.is_null(row) {
            out.push(None);
        } else {
            out.push(Some(format_decimal(arr.value(row), scale)));
        }
    }
    Ok(Arc::new(StringArray::from(out)) as ArrayRef)
}

pub(super) fn cast_with_special_rules(
    array: &ArrayRef,
    target_type: &DataType,
    fn_name: &str,
) -> Result<ArrayRef, String> {
    if array.data_type() == target_type {
        return Ok(array.clone());
    }
    let casted = match (array.data_type(), target_type) {
        (DataType::Utf8, DataType::Date32) => cast_utf8_to_date32(array),
        (DataType::Utf8, DataType::Timestamp(_, None)) => {
            cast_utf8_to_timestamp(array, target_type)
        }
        (DataType::Decimal128(_, _), DataType::Utf8) => cast_decimal_to_utf8(array),
        (DataType::Decimal128(_, _), DataType::Decimal128(_, _)) => {
            crate::exec::expr::cast::cast_with_special_rules(array, target_type)
        }
        _ => cast(array, target_type).map_err(|e| e.to_string()),
    }
    .map_err(|e| {
        format!(
            "{}: failed to cast {:?} -> {:?}: {}",
            fn_name,
            array.data_type(),
            target_type,
            e
        )
    })?;
    Ok(casted)
}

pub(super) fn compare_value_to_target(
    values: &ArrayRef,
    value_idx: usize,
    targets: &ArrayRef,
    target_row: usize,
) -> Result<bool, String> {
    let target_idx = row_index(target_row, targets.len());
    compare_values_with_null(values, value_idx, targets, target_idx, false)
}

pub(super) fn compare_values_at(
    left: &ArrayRef,
    left_idx: usize,
    right: &ArrayRef,
    right_idx: usize,
) -> Result<bool, String> {
    compare_values_with_null(left, left_idx, right, right_idx, false)
}

pub(super) fn compare_values_with_null(
    left: &ArrayRef,
    left_idx: usize,
    right: &ArrayRef,
    right_idx: usize,
    null_equals_null: bool,
) -> Result<bool, String> {
    if left.is_null(left_idx) || right.is_null(right_idx) {
        return Ok(null_equals_null && left.is_null(left_idx) && right.is_null(right_idx));
    }
    if left.data_type() != right.data_type() {
        return Err(format!(
            "array compare type mismatch: {:?} vs {:?}",
            left.data_type(),
            right.data_type()
        ));
    }

    match left.data_type() {
        DataType::Int8 => {
            let l = left.as_any().downcast_ref::<Int8Array>().unwrap();
            let r = right.as_any().downcast_ref::<Int8Array>().unwrap();
            Ok(l.value(left_idx) == r.value(right_idx))
        }
        DataType::Int16 => {
            let l = left.as_any().downcast_ref::<Int16Array>().unwrap();
            let r = right.as_any().downcast_ref::<Int16Array>().unwrap();
            Ok(l.value(left_idx) == r.value(right_idx))
        }
        DataType::Int32 => {
            let l = left.as_any().downcast_ref::<Int32Array>().unwrap();
            let r = right.as_any().downcast_ref::<Int32Array>().unwrap();
            Ok(l.value(left_idx) == r.value(right_idx))
        }
        DataType::Int64 => {
            let l = left.as_any().downcast_ref::<Int64Array>().unwrap();
            let r = right.as_any().downcast_ref::<Int64Array>().unwrap();
            Ok(l.value(left_idx) == r.value(right_idx))
        }
        DataType::Float32 => {
            let l = left.as_any().downcast_ref::<Float32Array>().unwrap();
            let r = right.as_any().downcast_ref::<Float32Array>().unwrap();
            Ok(l.value(left_idx) == r.value(right_idx))
        }
        DataType::Float64 => {
            let l = left.as_any().downcast_ref::<Float64Array>().unwrap();
            let r = right.as_any().downcast_ref::<Float64Array>().unwrap();
            Ok(l.value(left_idx) == r.value(right_idx))
        }
        DataType::Boolean => {
            let l = left.as_any().downcast_ref::<BooleanArray>().unwrap();
            let r = right.as_any().downcast_ref::<BooleanArray>().unwrap();
            Ok(l.value(left_idx) == r.value(right_idx))
        }
        DataType::Utf8 => {
            let l = left.as_any().downcast_ref::<StringArray>().unwrap();
            let r = right.as_any().downcast_ref::<StringArray>().unwrap();
            Ok(l.value(left_idx) == r.value(right_idx))
        }
        DataType::Date32 => {
            let l = left.as_any().downcast_ref::<Date32Array>().unwrap();
            let r = right.as_any().downcast_ref::<Date32Array>().unwrap();
            Ok(l.value(left_idx) == r.value(right_idx))
        }
        DataType::Decimal128(_, _) => {
            let l = left.as_any().downcast_ref::<Decimal128Array>().unwrap();
            let r = right.as_any().downcast_ref::<Decimal128Array>().unwrap();
            Ok(l.value(left_idx) == r.value(right_idx))
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None) => {
            let l = left
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .unwrap();
            let r = right
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .unwrap();
            Ok(l.value(left_idx) == r.value(right_idx))
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None) => {
            let l = left
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            let r = right
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            Ok(l.value(left_idx) == r.value(right_idx))
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None) => {
            let l = left
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            let r = right
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            Ok(l.value(left_idx) == r.value(right_idx))
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None) => {
            let l = left
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            let r = right
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            Ok(l.value(left_idx) == r.value(right_idx))
        }
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            let l = left
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| "failed to downcast left to FixedSizeBinaryArray".to_string())?;
            let r = right
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| "failed to downcast right to FixedSizeBinaryArray".to_string())?;
            let lv = largeint::i128_from_be_bytes(l.value(left_idx))
                .map_err(|e| format!("array compare LARGEINT decode failed: {}", e))?;
            let rv = largeint::i128_from_be_bytes(r.value(right_idx))
                .map_err(|e| format!("array compare LARGEINT decode failed: {}", e))?;
            Ok(lv == rv)
        }
        DataType::List(_) => compare_list_values(left, left_idx, right, right_idx),
        DataType::Struct(_) => compare_struct_values(left, left_idx, right, right_idx),
        DataType::Map(_, _) => compare_map_values(left, left_idx, right, right_idx),
        other => Err(format!("array compare unsupported type: {:?}", other)),
    }
}

fn compare_list_values(
    left: &ArrayRef,
    left_idx: usize,
    right: &ArrayRef,
    right_idx: usize,
) -> Result<bool, String> {
    let l = left
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| "failed to downcast left to ListArray".to_string())?;
    let r = right
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| "failed to downcast right to ListArray".to_string())?;

    let l_offsets = l.value_offsets();
    let r_offsets = r.value_offsets();
    let l_start = l_offsets[left_idx] as usize;
    let l_end = l_offsets[left_idx + 1] as usize;
    let r_start = r_offsets[right_idx] as usize;
    let r_end = r_offsets[right_idx + 1] as usize;
    let l_len = l_end.saturating_sub(l_start);
    let r_len = r_end.saturating_sub(r_start);
    if l_len != r_len {
        return Ok(false);
    }

    let l_values = l.values();
    let r_values = r.values();
    for offset in 0..l_len {
        if !compare_values_with_null(
            &l_values,
            l_start + offset,
            &r_values,
            r_start + offset,
            true,
        )? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn compare_struct_values(
    left: &ArrayRef,
    left_idx: usize,
    right: &ArrayRef,
    right_idx: usize,
) -> Result<bool, String> {
    let l = left
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| "failed to downcast left to StructArray".to_string())?;
    let r = right
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| "failed to downcast right to StructArray".to_string())?;
    if l.num_columns() != r.num_columns() {
        return Ok(false);
    }
    for col_idx in 0..l.num_columns() {
        if !compare_values_with_null(
            l.column(col_idx),
            left_idx,
            r.column(col_idx),
            right_idx,
            true,
        )? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn compare_map_values(
    left: &ArrayRef,
    left_idx: usize,
    right: &ArrayRef,
    right_idx: usize,
) -> Result<bool, String> {
    let l = left
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| "failed to downcast left to MapArray".to_string())?;
    let r = right
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| "failed to downcast right to MapArray".to_string())?;

    let l_offsets = l.value_offsets();
    let r_offsets = r.value_offsets();
    let l_start = l_offsets[left_idx] as usize;
    let l_end = l_offsets[left_idx + 1] as usize;
    let r_start = r_offsets[right_idx] as usize;
    let r_end = r_offsets[right_idx + 1] as usize;
    let l_len = l_end.saturating_sub(l_start);
    let r_len = r_end.saturating_sub(r_start);
    if l_len != r_len {
        return Ok(false);
    }

    let l_keys = l.keys();
    let r_keys = r.keys();
    let l_values = l.values();
    let r_values = r.values();
    for offset in 0..l_len {
        let li = l_start + offset;
        let ri = r_start + offset;
        if !compare_values_with_null(&l_keys, li, &r_keys, ri, true)? {
            return Ok(false);
        }
        if !compare_values_with_null(&l_values, li, &r_values, ri, true)? {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(super) fn compare_values_ordered(
    values: &ArrayRef,
    left_idx: usize,
    right_idx: usize,
) -> Result<Ordering, String> {
    if values.is_null(left_idx) || values.is_null(right_idx) {
        return Err("array ordered compare does not accept null indices".to_string());
    }
    match values.data_type() {
        DataType::Int8 => {
            let arr = values.as_any().downcast_ref::<Int8Array>().unwrap();
            Ok(arr.value(left_idx).cmp(&arr.value(right_idx)))
        }
        DataType::Int16 => {
            let arr = values.as_any().downcast_ref::<Int16Array>().unwrap();
            Ok(arr.value(left_idx).cmp(&arr.value(right_idx)))
        }
        DataType::Int32 => {
            let arr = values.as_any().downcast_ref::<Int32Array>().unwrap();
            Ok(arr.value(left_idx).cmp(&arr.value(right_idx)))
        }
        DataType::Int64 => {
            let arr = values.as_any().downcast_ref::<Int64Array>().unwrap();
            Ok(arr.value(left_idx).cmp(&arr.value(right_idx)))
        }
        DataType::Float32 => {
            let arr = values.as_any().downcast_ref::<Float32Array>().unwrap();
            Ok(arr
                .value(left_idx)
                .partial_cmp(&arr.value(right_idx))
                .unwrap_or(Ordering::Equal))
        }
        DataType::Float64 => {
            let arr = values.as_any().downcast_ref::<Float64Array>().unwrap();
            Ok(arr
                .value(left_idx)
                .partial_cmp(&arr.value(right_idx))
                .unwrap_or(Ordering::Equal))
        }
        DataType::Boolean => {
            let arr = values.as_any().downcast_ref::<BooleanArray>().unwrap();
            Ok(arr.value(left_idx).cmp(&arr.value(right_idx)))
        }
        DataType::Utf8 => {
            let arr = values.as_any().downcast_ref::<StringArray>().unwrap();
            Ok(arr.value(left_idx).cmp(arr.value(right_idx)))
        }
        DataType::Date32 => {
            let arr = values.as_any().downcast_ref::<Date32Array>().unwrap();
            Ok(arr.value(left_idx).cmp(&arr.value(right_idx)))
        }
        DataType::Decimal128(_, _) => {
            let arr = values.as_any().downcast_ref::<Decimal128Array>().unwrap();
            Ok(arr.value(left_idx).cmp(&arr.value(right_idx)))
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None) => {
            let arr = values
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .unwrap();
            Ok(arr.value(left_idx).cmp(&arr.value(right_idx)))
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None) => {
            let arr = values
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            Ok(arr.value(left_idx).cmp(&arr.value(right_idx)))
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None) => {
            let arr = values
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            Ok(arr.value(left_idx).cmp(&arr.value(right_idx)))
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None) => {
            let arr = values
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            Ok(arr.value(left_idx).cmp(&arr.value(right_idx)))
        }
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            let arr = values
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| "failed to downcast to FixedSizeBinaryArray".to_string())?;
            let left = largeint::i128_from_be_bytes(arr.value(left_idx))
                .map_err(|e| format!("array ordered compare LARGEINT decode failed: {}", e))?;
            let right = largeint::i128_from_be_bytes(arr.value(right_idx))
                .map_err(|e| format!("array ordered compare LARGEINT decode failed: {}", e))?;
            Ok(left.cmp(&right))
        }
        other => Err(format!(
            "array ordered compare unsupported type: {:?}",
            other
        )),
    }
}
