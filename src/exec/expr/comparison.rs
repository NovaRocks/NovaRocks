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
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Builder, ListArray, StringArray, StructArray,
    TimestampMicrosecondBuilder,
};
use arrow::compute::cast;
use arrow::compute::kernels::boolean::not;
use arrow::compute::kernels::cmp::{eq, gt, gt_eq, lt, lt_eq, neq};
use arrow::datatypes::DataType;
use chrono::{Datelike, NaiveDate, NaiveDateTime};
use std::cmp::Ordering;
use std::sync::Arc;

fn parse_date32_value(value: &str) -> Result<i32, String> {
    const UNIX_EPOCH_DAY_OFFSET: i32 = 719163;
    if let Ok(date) = NaiveDate::parse_from_str(value, "%Y-%m-%d") {
        return Ok(date.num_days_from_ce() - UNIX_EPOCH_DAY_OFFSET);
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S") {
        return Ok(dt.date().num_days_from_ce() - UNIX_EPOCH_DAY_OFFSET);
    }
    Err(format!("invalid date literal '{}'", value))
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
        } else {
            let days = parse_date32_value(arr.value(i))?;
            builder.append_value(days);
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn parse_timestamp_micro_value(value: &str) -> Result<i64, String> {
    if let Ok(dt) = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f") {
        return Ok(dt.and_utc().timestamp_micros());
    }
    if let Ok(date) = NaiveDate::parse_from_str(value, "%Y-%m-%d") {
        let dt = date
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| format!("invalid timestamp literal '{}'", value))?;
        return Ok(dt.and_utc().timestamp_micros());
    }
    Err(format!("invalid timestamp literal '{}'", value))
}

fn cast_utf8_to_timestamp_micro(array: &ArrayRef) -> Result<ArrayRef, String> {
    let arr = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "failed to downcast to StringArray".to_string())?;
    let mut builder = TimestampMicrosecondBuilder::new();
    for i in 0..arr.len() {
        if arr.is_null(i) {
            builder.append_null();
        } else {
            let micros = parse_timestamp_micro_value(arr.value(i))?;
            builder.append_value(micros);
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn cast_utf8_to_timestamp(array: &ArrayRef, target: &DataType) -> Result<ArrayRef, String> {
    match target {
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None) => {
            cast_utf8_to_timestamp_micro(array)
        }
        DataType::Timestamp(_, _) => cast(array, target).map_err(|e| e.to_string()),
        other => Err(format!("unsupported timestamp target type: {:?}", other)),
    }
}

fn compare_scalar_non_null(
    left: &ArrayRef,
    left_idx: usize,
    right: &ArrayRef,
    right_idx: usize,
) -> Result<Ordering, String> {
    if left.data_type() != right.data_type() {
        return Err(format!(
            "list scalar compare type mismatch: {:?} vs {:?}",
            left.data_type(),
            right.data_type()
        ));
    }
    match left.data_type() {
        DataType::Boolean => {
            let l = left
                .as_any()
                .downcast_ref::<arrow::array::BooleanArray>()
                .ok_or_else(|| "failed to downcast left to BooleanArray".to_string())?;
            let r = right
                .as_any()
                .downcast_ref::<arrow::array::BooleanArray>()
                .ok_or_else(|| "failed to downcast right to BooleanArray".to_string())?;
            Ok(l.value(left_idx).cmp(&r.value(right_idx)))
        }
        DataType::Int8 => {
            let l = left
                .as_any()
                .downcast_ref::<arrow::array::Int8Array>()
                .ok_or_else(|| "failed to downcast left to Int8Array".to_string())?;
            let r = right
                .as_any()
                .downcast_ref::<arrow::array::Int8Array>()
                .ok_or_else(|| "failed to downcast right to Int8Array".to_string())?;
            Ok(l.value(left_idx).cmp(&r.value(right_idx)))
        }
        DataType::Int16 => {
            let l = left
                .as_any()
                .downcast_ref::<arrow::array::Int16Array>()
                .ok_or_else(|| "failed to downcast left to Int16Array".to_string())?;
            let r = right
                .as_any()
                .downcast_ref::<arrow::array::Int16Array>()
                .ok_or_else(|| "failed to downcast right to Int16Array".to_string())?;
            Ok(l.value(left_idx).cmp(&r.value(right_idx)))
        }
        DataType::Int32 => {
            let l = left
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .ok_or_else(|| "failed to downcast left to Int32Array".to_string())?;
            let r = right
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .ok_or_else(|| "failed to downcast right to Int32Array".to_string())?;
            Ok(l.value(left_idx).cmp(&r.value(right_idx)))
        }
        DataType::Int64 => {
            let l = left
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .ok_or_else(|| "failed to downcast left to Int64Array".to_string())?;
            let r = right
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .ok_or_else(|| "failed to downcast right to Int64Array".to_string())?;
            Ok(l.value(left_idx).cmp(&r.value(right_idx)))
        }
        DataType::Float32 => {
            let l = left
                .as_any()
                .downcast_ref::<arrow::array::Float32Array>()
                .ok_or_else(|| "failed to downcast left to Float32Array".to_string())?;
            let r = right
                .as_any()
                .downcast_ref::<arrow::array::Float32Array>()
                .ok_or_else(|| "failed to downcast right to Float32Array".to_string())?;
            Ok(l.value(left_idx)
                .partial_cmp(&r.value(right_idx))
                .unwrap_or(Ordering::Equal))
        }
        DataType::Float64 => {
            let l = left
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .ok_or_else(|| "failed to downcast left to Float64Array".to_string())?;
            let r = right
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .ok_or_else(|| "failed to downcast right to Float64Array".to_string())?;
            Ok(l.value(left_idx)
                .partial_cmp(&r.value(right_idx))
                .unwrap_or(Ordering::Equal))
        }
        DataType::Utf8 => {
            let l = left
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .ok_or_else(|| "failed to downcast left to StringArray".to_string())?;
            let r = right
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .ok_or_else(|| "failed to downcast right to StringArray".to_string())?;
            Ok(l.value(left_idx).cmp(r.value(right_idx)))
        }
        DataType::Date32 => {
            let l = left
                .as_any()
                .downcast_ref::<arrow::array::Date32Array>()
                .ok_or_else(|| "failed to downcast left to Date32Array".to_string())?;
            let r = right
                .as_any()
                .downcast_ref::<arrow::array::Date32Array>()
                .ok_or_else(|| "failed to downcast right to Date32Array".to_string())?;
            Ok(l.value(left_idx).cmp(&r.value(right_idx)))
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None) => {
            let l = left
                .as_any()
                .downcast_ref::<arrow::array::TimestampSecondArray>()
                .ok_or_else(|| "failed to downcast left to TimestampSecondArray".to_string())?;
            let r = right
                .as_any()
                .downcast_ref::<arrow::array::TimestampSecondArray>()
                .ok_or_else(|| "failed to downcast right to TimestampSecondArray".to_string())?;
            Ok(l.value(left_idx).cmp(&r.value(right_idx)))
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None) => {
            let l = left
                .as_any()
                .downcast_ref::<arrow::array::TimestampMillisecondArray>()
                .ok_or_else(|| {
                    "failed to downcast left to TimestampMillisecondArray".to_string()
                })?;
            let r = right
                .as_any()
                .downcast_ref::<arrow::array::TimestampMillisecondArray>()
                .ok_or_else(|| {
                    "failed to downcast right to TimestampMillisecondArray".to_string()
                })?;
            Ok(l.value(left_idx).cmp(&r.value(right_idx)))
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None) => {
            let l = left
                .as_any()
                .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
                .ok_or_else(|| {
                    "failed to downcast left to TimestampMicrosecondArray".to_string()
                })?;
            let r = right
                .as_any()
                .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
                .ok_or_else(|| {
                    "failed to downcast right to TimestampMicrosecondArray".to_string()
                })?;
            Ok(l.value(left_idx).cmp(&r.value(right_idx)))
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None) => {
            let l = left
                .as_any()
                .downcast_ref::<arrow::array::TimestampNanosecondArray>()
                .ok_or_else(|| "failed to downcast left to TimestampNanosecondArray".to_string())?;
            let r = right
                .as_any()
                .downcast_ref::<arrow::array::TimestampNanosecondArray>()
                .ok_or_else(|| {
                    "failed to downcast right to TimestampNanosecondArray".to_string()
                })?;
            Ok(l.value(left_idx).cmp(&r.value(right_idx)))
        }
        DataType::Decimal128(_, _) => {
            let l = left
                .as_any()
                .downcast_ref::<arrow::array::Decimal128Array>()
                .ok_or_else(|| "failed to downcast left to Decimal128Array".to_string())?;
            let r = right
                .as_any()
                .downcast_ref::<arrow::array::Decimal128Array>()
                .ok_or_else(|| "failed to downcast right to Decimal128Array".to_string())?;
            Ok(l.value(left_idx).cmp(&r.value(right_idx)))
        }
        other => Err(format!("list scalar compare unsupported type: {:?}", other)),
    }
}

fn compare_value_recursive(
    left: &ArrayRef,
    left_idx: usize,
    right: &ArrayRef,
    right_idx: usize,
) -> Result<Option<Ordering>, String> {
    if left.data_type() != right.data_type() {
        return Err(format!(
            "list compare type mismatch: {:?} vs {:?}",
            left.data_type(),
            right.data_type()
        ));
    }
    if left.is_null(left_idx) || right.is_null(right_idx) {
        return Ok(None);
    }
    if matches!(left.data_type(), DataType::List(_)) {
        let l = left
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| "failed to downcast left to ListArray".to_string())?;
        let r = right
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| "failed to downcast right to ListArray".to_string())?;
        return compare_list_rows(l, left_idx, r, right_idx);
    }
    if matches!(left.data_type(), DataType::Struct(_)) {
        let l = left
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| "failed to downcast left to StructArray".to_string())?;
        let r = right
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| "failed to downcast right to StructArray".to_string())?;
        return compare_struct_rows(l, left_idx, r, right_idx);
    }
    compare_scalar_non_null(left, left_idx, right, right_idx).map(Some)
}

fn compare_value_recursive_in_list(
    left: &ArrayRef,
    left_idx: usize,
    right: &ArrayRef,
    right_idx: usize,
) -> Result<Ordering, String> {
    if left.data_type() != right.data_type() {
        return Err(format!(
            "list compare type mismatch: {:?} vs {:?}",
            left.data_type(),
            right.data_type()
        ));
    }
    let left_is_null = left.is_null(left_idx);
    let right_is_null = right.is_null(right_idx);
    match (left_is_null, right_is_null) {
        (true, true) => return Ok(Ordering::Equal),
        (true, false) => return Ok(Ordering::Greater),
        (false, true) => return Ok(Ordering::Less),
        (false, false) => {}
    }
    if matches!(left.data_type(), DataType::List(_)) {
        let l = left
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| "failed to downcast left to ListArray".to_string())?;
        let r = right
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| "failed to downcast right to ListArray".to_string())?;
        return compare_list_rows_non_null(l, left_idx, r, right_idx);
    }
    compare_scalar_non_null(left, left_idx, right, right_idx)
}

fn compare_list_rows_non_null(
    left: &ListArray,
    left_row: usize,
    right: &ListArray,
    right_row: usize,
) -> Result<Ordering, String> {
    let left_offsets = left.value_offsets();
    let right_offsets = right.value_offsets();
    let left_start = left_offsets[left_row] as usize;
    let left_end = left_offsets[left_row + 1] as usize;
    let right_start = right_offsets[right_row] as usize;
    let right_end = right_offsets[right_row + 1] as usize;
    let left_len = left_end.saturating_sub(left_start);
    let right_len = right_end.saturating_sub(right_start);
    let min_len = left_len.min(right_len);

    let left_values = left.values();
    let right_values = right.values();
    for idx in 0..min_len {
        let l_idx = left_start + idx;
        let r_idx = right_start + idx;
        let ord = compare_value_recursive_in_list(&left_values, l_idx, &right_values, r_idx)?;
        if ord != Ordering::Equal {
            return Ok(ord);
        }
    }
    Ok(left_len.cmp(&right_len))
}

fn compare_list_rows(
    left: &ListArray,
    left_row: usize,
    right: &ListArray,
    right_row: usize,
) -> Result<Option<Ordering>, String> {
    if left.is_null(left_row) || right.is_null(right_row) {
        return Ok(None);
    }
    compare_list_rows_non_null(left, left_row, right, right_row).map(Some)
}

fn compare_struct_rows(
    left: &StructArray,
    left_row: usize,
    right: &StructArray,
    right_row: usize,
) -> Result<Option<Ordering>, String> {
    if left.is_null(left_row) || right.is_null(right_row) {
        return Ok(None);
    }
    if left.columns().len() != right.columns().len() {
        return Err(format!(
            "struct compare field count mismatch: {} vs {}",
            left.columns().len(),
            right.columns().len()
        ));
    }

    let mut has_unknown = false;
    for (left_col, right_col) in left.columns().iter().zip(right.columns()) {
        match compare_value_recursive(left_col, left_row, right_col, right_row)? {
            Some(Ordering::Equal) => {}
            Some(ord) => return Ok(Some(ord)),
            None => has_unknown = true,
        }
    }

    if has_unknown {
        Ok(None)
    } else {
        Ok(Some(Ordering::Equal))
    }
}

fn eq_value_recursive_null_safe(
    left: &ArrayRef,
    left_idx: usize,
    right: &ArrayRef,
    right_idx: usize,
) -> Result<bool, String> {
    if left.data_type() != right.data_type() {
        return Err(format!(
            "null-safe eq type mismatch: {:?} vs {:?}",
            left.data_type(),
            right.data_type()
        ));
    }
    let left_is_null = left.is_null(left_idx);
    let right_is_null = right.is_null(right_idx);
    if left_is_null || right_is_null {
        return Ok(left_is_null && right_is_null);
    }
    if matches!(left.data_type(), DataType::List(_)) {
        let l = left
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| "failed to downcast left to ListArray".to_string())?;
        let r = right
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| "failed to downcast right to ListArray".to_string())?;
        return eq_list_rows_null_safe(l, left_idx, r, right_idx);
    }
    if matches!(left.data_type(), DataType::Struct(_)) {
        let l = left
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| "failed to downcast left to StructArray".to_string())?;
        let r = right
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| "failed to downcast right to StructArray".to_string())?;
        return eq_struct_rows_null_safe(l, left_idx, r, right_idx);
    }
    Ok(compare_scalar_non_null(left, left_idx, right, right_idx)? == Ordering::Equal)
}

fn eq_list_rows_null_safe(
    left: &ListArray,
    left_row: usize,
    right: &ListArray,
    right_row: usize,
) -> Result<bool, String> {
    let left_is_null = left.is_null(left_row);
    let right_is_null = right.is_null(right_row);
    if left_is_null || right_is_null {
        return Ok(left_is_null && right_is_null);
    }

    let left_offsets = left.value_offsets();
    let right_offsets = right.value_offsets();
    let left_start = left_offsets[left_row] as usize;
    let left_end = left_offsets[left_row + 1] as usize;
    let right_start = right_offsets[right_row] as usize;
    let right_end = right_offsets[right_row + 1] as usize;
    let left_len = left_end.saturating_sub(left_start);
    let right_len = right_end.saturating_sub(right_start);
    if left_len != right_len {
        return Ok(false);
    }

    let left_values = left.values();
    let right_values = right.values();
    for idx in 0..left_len {
        let l_idx = left_start + idx;
        let r_idx = right_start + idx;
        if !eq_value_recursive_null_safe(&left_values, l_idx, &right_values, r_idx)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn eq_struct_rows_null_safe(
    left: &StructArray,
    left_row: usize,
    right: &StructArray,
    right_row: usize,
) -> Result<bool, String> {
    let left_is_null = left.is_null(left_row);
    let right_is_null = right.is_null(right_row);
    if left_is_null || right_is_null {
        return Ok(left_is_null && right_is_null);
    }
    if left.columns().len() != right.columns().len() {
        return Err(format!(
            "null-safe eq struct field count mismatch: {} vs {}",
            left.columns().len(),
            right.columns().len()
        ));
    }

    for (left_col, right_col) in left.columns().iter().zip(right.columns()) {
        if !eq_value_recursive_null_safe(left_col, left_row, right_col, right_row)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn eval_null_safe_eq(left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef, String> {
    if left.data_type() != right.data_type() {
        return Err(format!(
            "null-safe eq type mismatch: {:?} vs {:?}",
            left.data_type(),
            right.data_type()
        ));
    }
    let out_len = left.len().max(right.len());
    if out_len == 0 {
        return Ok(Arc::new(BooleanArray::from(Vec::<Option<bool>>::new())));
    }

    let mut builder = arrow::array::BooleanBuilder::new();
    for row in 0..out_len {
        let left_row = if left.len() == 1 { 0 } else { row };
        let right_row = if right.len() == 1 { 0 } else { row };
        if left_row >= left.len() || right_row >= right.len() {
            return Err(format!(
                "null-safe eq row out of bounds: left_len={} right_len={} row={}",
                left.len(),
                right.len(),
                row
            ));
        }
        let equals = eq_value_recursive_null_safe(left, left_row, right, right_row)?;
        builder.append_value(equals);
    }
    Ok(Arc::new(builder.finish()))
}

fn eval_nested_compare<F>(
    left: &ArrayRef,
    right: &ArrayRef,
    predicate: F,
) -> Result<ArrayRef, String>
where
    F: Fn(Ordering) -> bool,
{
    if left.data_type() != right.data_type() {
        return Err(format!(
            "nested compare type mismatch: {:?} vs {:?}",
            left.data_type(),
            right.data_type()
        ));
    }
    let out_len = left.len().max(right.len());
    if out_len == 0 {
        return Ok(Arc::new(arrow::array::BooleanArray::from(Vec::<
            Option<bool>,
        >::new())));
    }
    let mut builder = arrow::array::BooleanBuilder::new();
    for row in 0..out_len {
        let left_row = if left.len() == 1 { 0 } else { row };
        let right_row = if right.len() == 1 { 0 } else { row };
        if left_row >= left.len() || right_row >= right.len() {
            return Err(format!(
                "nested compare row out of bounds: left_len={} right_len={} row={}",
                left.len(),
                right.len(),
                row
            ));
        }
        match compare_value_recursive(left, left_row, right, right_row)? {
            Some(ord) => builder.append_value(predicate(ord)),
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

// Helper function to normalize types for comparison
fn normalize_comparison_types(
    left: ArrayRef,
    right: ArrayRef,
) -> Result<(ArrayRef, ArrayRef), String> {
    let left_type = left.data_type();
    let right_type = right.data_type();

    // If types match, no conversion needed
    if left_type == right_type {
        return Ok((left, right));
    }

    // Handle date vs string by casting string to Date32.
    if matches!(left_type, DataType::Date32) && matches!(right_type, DataType::Utf8) {
        let right_date = cast_utf8_to_date32(&right)?;
        return Ok((left, right_date));
    }
    if matches!(left_type, DataType::Utf8) && matches!(right_type, DataType::Date32) {
        let left_date = cast_utf8_to_date32(&left)?;
        return Ok((left_date, right));
    }

    // Handle timestamp vs string by casting string to the timestamp type.
    if matches!(left_type, DataType::Timestamp(_, _)) && matches!(right_type, DataType::Utf8) {
        let right_ts = cast_utf8_to_timestamp(&right, left_type)?;
        return Ok((left, right_ts));
    }
    if matches!(left_type, DataType::Utf8) && matches!(right_type, DataType::Timestamp(_, _)) {
        let left_ts = cast_utf8_to_timestamp(&left, right_type)?;
        return Ok((left_ts, right));
    }

    // Handle integer type mismatches by casting to Int64.
    let is_int = |dt: &DataType| {
        matches!(
            dt,
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
        )
    };

    if is_int(left_type) && is_int(right_type) {
        let left_64 = if matches!(left_type, DataType::Int64) {
            left
        } else {
            cast(&left, &DataType::Int64).map_err(|e| e.to_string())?
        };
        let right_64 = if matches!(right_type, DataType::Int64) {
            right
        } else {
            cast(&right, &DataType::Int64).map_err(|e| e.to_string())?
        };
        return Ok((left_64, right_64));
    }

    // Handle float type mismatches by casting to Float64
    let is_float = |dt: &DataType| matches!(dt, DataType::Float32 | DataType::Float64);

    // Handle mixed integer/float numeric comparisons by casting both sides to Float64.
    // StarRocks allows expressions like `abs(1 - 2) = 0` even when one side is inferred
    // as floating point and the other side is integer.
    if (is_int(left_type) && is_float(right_type)) || (is_float(left_type) && is_int(right_type)) {
        let left_64 = if matches!(left_type, DataType::Float64) {
            left
        } else {
            cast(&left, &DataType::Float64).map_err(|e| e.to_string())?
        };
        let right_64 = if matches!(right_type, DataType::Float64) {
            right
        } else {
            cast(&right, &DataType::Float64).map_err(|e| e.to_string())?
        };
        return Ok((left_64, right_64));
    }

    if is_float(left_type) && is_float(right_type) {
        let left_64 = if matches!(left_type, DataType::Float64) {
            left
        } else {
            cast(&left, &DataType::Float64).map_err(|e| e.to_string())?
        };
        let right_64 = if matches!(right_type, DataType::Float64) {
            right
        } else {
            cast(&right, &DataType::Float64).map_err(|e| e.to_string())?
        };
        return Ok((left_64, right_64));
    }

    // Handle decimal type mismatches by casting to a common decimal type (StarRocks-aligned).
    //
    // Arrow comparison kernels require identical data types, but StarRocks allows comparing
    // decimal values with different precisions/scales by coercing both sides to a compatible
    // decimal type:
    //   scale = max(s1, s2)
    //   precision = max(p1-s1, p2-s2) + scale
    if let (DataType::Decimal128(lp, ls), DataType::Decimal128(rp, rs)) = (left_type, right_type) {
        let target_scale: i8 = (*ls).max(*rs);
        let lhs_int_digits: i16 = (*lp as i16) - (*ls as i16);
        let rhs_int_digits: i16 = (*rp as i16) - (*rs as i16);
        let int_digits: i16 = lhs_int_digits.max(rhs_int_digits).max(0);
        let target_precision: i16 = int_digits + (target_scale as i16);
        if target_precision <= 0 {
            return Err("invalid decimal precision for comparison".to_string());
        }
        if target_precision > 38 {
            return Err(format!(
                "decimal comparison precision overflow: lhs={:?} rhs={:?} => target=Decimal128({}, {})",
                left_type, right_type, target_precision, target_scale
            ));
        }
        let target_type = DataType::Decimal128(target_precision as u8, target_scale);
        let left_cast = if left_type == &target_type {
            left
        } else {
            cast(&left, &target_type).map_err(|e| e.to_string())?
        };
        let right_cast = if right_type == &target_type {
            right
        } else {
            cast(&right, &target_type).map_err(|e| e.to_string())?
        };
        return Ok((left_cast, right_cast));
    }

    // If no conversion possible, return error
    Err(format!(
        "Cannot compare incompatible types: {:?} vs {:?}",
        left_type, right_type
    ))
}

// Arrow vectorized versions
pub fn eval_eq(
    arena: &ExprArena,
    left: ExprId,
    right: ExprId,
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let l = arena.eval(left, chunk)?;
    let r = arena.eval(right, chunk)?;
    let (l_norm, r_norm) = normalize_comparison_types(l, r)?;
    if matches!(l_norm.data_type(), DataType::List(_) | DataType::Struct(_)) {
        return eval_nested_compare(&l_norm, &r_norm, |ord| ord == Ordering::Equal);
    }
    let result = eq(&l_norm, &r_norm).map_err(|e| e.to_string())?;
    Ok(Arc::new(result))
}

pub fn eval_eq_for_null(
    arena: &ExprArena,
    left: ExprId,
    right: ExprId,
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let l = arena.eval(left, chunk)?;
    let r = arena.eval(right, chunk)?;
    let (l_norm, r_norm) = normalize_comparison_types(l, r)?;
    eval_null_safe_eq(&l_norm, &r_norm)
}

pub fn eval_ne(
    arena: &ExprArena,
    left: ExprId,
    right: ExprId,
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let l = arena.eval(left, chunk)?;
    let r = arena.eval(right, chunk)?;
    let (l_norm, r_norm) = normalize_comparison_types(l, r)?;
    if matches!(l_norm.data_type(), DataType::List(_) | DataType::Struct(_)) {
        return eval_nested_compare(&l_norm, &r_norm, |ord| ord != Ordering::Equal);
    }
    let result = neq(&l_norm, &r_norm).map_err(|e| e.to_string())?;
    Ok(Arc::new(result))
}

pub fn eval_lt(
    arena: &ExprArena,
    left: ExprId,
    right: ExprId,
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let l = arena.eval(left, chunk)?;
    let r = arena.eval(right, chunk)?;
    let (l_norm, r_norm) = normalize_comparison_types(l, r)?;
    if matches!(l_norm.data_type(), DataType::List(_) | DataType::Struct(_)) {
        return eval_nested_compare(&l_norm, &r_norm, |ord| ord == Ordering::Less);
    }
    let result = lt(&l_norm, &r_norm).map_err(|e| e.to_string())?;
    Ok(Arc::new(result))
}

pub fn eval_le(
    arena: &ExprArena,
    left: ExprId,
    right: ExprId,
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let l = arena.eval(left, chunk)?;
    let r = arena.eval(right, chunk)?;
    let (l_norm, r_norm) = normalize_comparison_types(l, r)?;
    if matches!(l_norm.data_type(), DataType::List(_) | DataType::Struct(_)) {
        return eval_nested_compare(&l_norm, &r_norm, |ord| {
            ord == Ordering::Less || ord == Ordering::Equal
        });
    }
    let result = lt_eq(&l_norm, &r_norm).map_err(|e| e.to_string())?;
    Ok(Arc::new(result))
}

pub fn eval_gt(
    arena: &ExprArena,
    left: ExprId,
    right: ExprId,
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let l = arena.eval(left, chunk)?;
    let r = arena.eval(right, chunk)?;
    let (l_norm, r_norm) = normalize_comparison_types(l, r)?;
    if matches!(l_norm.data_type(), DataType::List(_) | DataType::Struct(_)) {
        return eval_nested_compare(&l_norm, &r_norm, |ord| ord == Ordering::Greater);
    }
    let result = gt(&l_norm, &r_norm).map_err(|e| e.to_string())?;
    Ok(Arc::new(result))
}

pub fn eval_ge(
    arena: &ExprArena,
    left: ExprId,
    right: ExprId,
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let l = arena.eval(left, chunk)?;
    let r = arena.eval(right, chunk)?;
    let (l_norm, r_norm) = normalize_comparison_types(l, r)?;
    if matches!(l_norm.data_type(), DataType::List(_) | DataType::Struct(_)) {
        return eval_nested_compare(&l_norm, &r_norm, |ord| {
            ord == Ordering::Greater || ord == Ordering::Equal
        });
    }
    let result = gt_eq(&l_norm, &r_norm).map_err(|e| e.to_string())?;
    Ok(Arc::new(result))
}

pub fn eval_and(
    arena: &ExprArena,
    left: ExprId,
    right: ExprId,
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let l = arena.eval(left, chunk)?;
    let r = arena.eval(right, chunk)?;
    let l_bool = l
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| "AND left operand must be boolean".to_string())?;
    let r_bool = r
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| "AND right operand must be boolean".to_string())?;
    // Arrow's boolean kernels treat NULLs as "propagate NULL" in some cases, which does not match
    // SQL three-valued logic (3VL). In SQL:
    //   FALSE AND NULL = FALSE
    //   TRUE  AND NULL = NULL
    //   NULL  AND FALSE = FALSE
    //   NULL  AND TRUE  = NULL
    // We implement 3VL explicitly to match StarRocks semantics for WHERE predicates.
    let mut builder = arrow::array::BooleanBuilder::new();
    for i in 0..l_bool.len() {
        let l_is_null = l_bool.is_null(i);
        let r_is_null = r_bool.is_null(i);
        match (l_is_null, r_is_null) {
            (false, false) => builder.append_value(l_bool.value(i) && r_bool.value(i)),
            // FALSE dominates AND even when the other side is NULL.
            (false, true) => {
                if !l_bool.value(i) {
                    builder.append_value(false);
                } else {
                    builder.append_null();
                }
            }
            (true, false) => {
                if !r_bool.value(i) {
                    builder.append_value(false);
                } else {
                    builder.append_null();
                }
            }
            (true, true) => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

pub fn eval_or(
    arena: &ExprArena,
    left: ExprId,
    right: ExprId,
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let l = arena.eval(left, chunk)?;
    let r = arena.eval(right, chunk)?;
    let l_bool = l
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| "OR left operand must be boolean".to_string())?;
    let r_bool = r
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| "OR right operand must be boolean".to_string())?;
    // SQL three-valued logic (3VL):
    //   TRUE  OR NULL = TRUE
    //   FALSE OR NULL = NULL
    //   NULL  OR TRUE = TRUE
    //   NULL  OR FALSE = NULL
    let mut builder = arrow::array::BooleanBuilder::new();
    for i in 0..l_bool.len() {
        let l_is_null = l_bool.is_null(i);
        let r_is_null = r_bool.is_null(i);
        match (l_is_null, r_is_null) {
            (false, false) => builder.append_value(l_bool.value(i) || r_bool.value(i)),
            // TRUE dominates OR even when the other side is NULL.
            (false, true) => {
                if l_bool.value(i) {
                    builder.append_value(true);
                } else {
                    builder.append_null();
                }
            }
            (true, false) => {
                if r_bool.value(i) {
                    builder.append_value(true);
                } else {
                    builder.append_null();
                }
            }
            (true, true) => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

pub fn eval_not(arena: &ExprArena, child: ExprId, chunk: &Chunk) -> Result<ArrayRef, String> {
    let v = arena.eval(child, chunk)?;

    if let Some(b) = v.as_any().downcast_ref::<BooleanArray>() {
        let result = not(b).map_err(|e| e.to_string())?;
        return Ok(Arc::new(result));
    }

    let mut builder = arrow::array::BooleanBuilder::new();
    match v.data_type() {
        DataType::Int8 => {
            let a = v
                .as_any()
                .downcast_ref::<arrow::array::Int8Array>()
                .ok_or_else(|| "NOT operand type mismatch".to_string())?;
            for i in 0..a.len() {
                if a.is_null(i) {
                    builder.append_null();
                } else {
                    builder.append_value(a.value(i) == 0);
                }
            }
        }
        DataType::Int16 => {
            let a = v
                .as_any()
                .downcast_ref::<arrow::array::Int16Array>()
                .ok_or_else(|| "NOT operand type mismatch".to_string())?;
            for i in 0..a.len() {
                if a.is_null(i) {
                    builder.append_null();
                } else {
                    builder.append_value(a.value(i) == 0);
                }
            }
        }
        DataType::Int32 => {
            let a = v
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .ok_or_else(|| "NOT operand type mismatch".to_string())?;
            for i in 0..a.len() {
                if a.is_null(i) {
                    builder.append_null();
                } else {
                    builder.append_value(a.value(i) == 0);
                }
            }
        }
        DataType::Int64 => {
            let a = v
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .ok_or_else(|| "NOT operand type mismatch".to_string())?;
            for i in 0..a.len() {
                if a.is_null(i) {
                    builder.append_null();
                } else {
                    builder.append_value(a.value(i) == 0);
                }
            }
        }
        DataType::Float32 => {
            let a = v
                .as_any()
                .downcast_ref::<arrow::array::Float32Array>()
                .ok_or_else(|| "NOT operand type mismatch".to_string())?;
            for i in 0..a.len() {
                if a.is_null(i) {
                    builder.append_null();
                } else {
                    builder.append_value(a.value(i) == 0.0);
                }
            }
        }
        DataType::Float64 => {
            let a = v
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .ok_or_else(|| "NOT operand type mismatch".to_string())?;
            for i in 0..a.len() {
                if a.is_null(i) {
                    builder.append_null();
                } else {
                    builder.append_value(a.value(i) == 0.0);
                }
            }
        }
        DataType::Decimal128(_, _) => {
            let a = v
                .as_any()
                .downcast_ref::<arrow::array::Decimal128Array>()
                .ok_or_else(|| "NOT operand type mismatch".to_string())?;
            for i in 0..a.len() {
                if a.is_null(i) {
                    builder.append_null();
                } else {
                    builder.append_value(a.value(i) == 0);
                }
            }
        }
        other => {
            return Err(format!(
                "NOT operand must be boolean or numeric, got {:?}",
                other
            ));
        }
    }
    Ok(Arc::new(builder.finish()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::chunk::field_with_slot_id;
    use crate::exec::expr::{ExprNode, LiteralValue};
    use arrow::array::{
        BooleanArray, Decimal128Array, Int32Array, Int64Array, ListArray, StringArray, StructArray,
    };
    use arrow::datatypes::{Field, Fields, Schema};
    use arrow::record_batch::RecordBatch;

    fn create_test_chunk_int(values: Vec<i64>) -> Chunk {
        let array = Arc::new(Int64Array::from(values)) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![field_with_slot_id(
            Field::new("col0", DataType::Int64, false),
            SlotId::new(1),
        )]));
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
        Chunk::new(batch)
    }

    fn create_test_chunk_i64_nullable(left: Vec<Option<i64>>, right: Vec<Option<i64>>) -> Chunk {
        let left = Arc::new(Int64Array::from(left)) as ArrayRef;
        let right = Arc::new(Int64Array::from(right)) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![
            field_with_slot_id(Field::new("l", DataType::Int64, true), SlotId::new(1)),
            field_with_slot_id(Field::new("r", DataType::Int64, true), SlotId::new(2)),
        ]));
        let batch = RecordBatch::try_new(schema, vec![left, right]).unwrap();
        Chunk::new(batch)
    }

    fn create_test_chunk_bool(l: Vec<Option<bool>>, r: Vec<Option<bool>>) -> Chunk {
        let l = Arc::new(BooleanArray::from(l)) as ArrayRef;
        let r = Arc::new(BooleanArray::from(r)) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![
            field_with_slot_id(Field::new("l", DataType::Boolean, true), SlotId::new(1)),
            field_with_slot_id(Field::new("r", DataType::Boolean, true), SlotId::new(2)),
        ]));
        let batch = RecordBatch::try_new(schema, vec![l, r]).unwrap();
        Chunk::new(batch)
    }

    fn create_test_chunk_list_i64(left: ListArray, right: ListArray, list_type: DataType) -> Chunk {
        let left = Arc::new(left) as ArrayRef;
        let right = Arc::new(right) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![
            field_with_slot_id(Field::new("l", list_type.clone(), true), SlotId::new(1)),
            field_with_slot_id(Field::new("r", list_type, true), SlotId::new(2)),
        ]));
        let batch = RecordBatch::try_new(schema, vec![left, right]).unwrap();
        Chunk::new(batch)
    }

    fn create_test_chunk_struct_i32(
        left: StructArray,
        right: StructArray,
        struct_type: DataType,
    ) -> Chunk {
        let left = Arc::new(left) as ArrayRef;
        let right = Arc::new(right) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![
            field_with_slot_id(Field::new("l", struct_type.clone(), true), SlotId::new(1)),
            field_with_slot_id(Field::new("r", struct_type, true), SlotId::new(2)),
        ]));
        let batch = RecordBatch::try_new(schema, vec![left, right]).unwrap();
        Chunk::new(batch)
    }

    #[test]
    fn test_eq_integers() {
        let mut arena = ExprArena::default();
        let lit5 = arena.push(ExprNode::Literal(LiteralValue::Int64(5)));
        let lit5_dup = arena.push(ExprNode::Literal(LiteralValue::Int64(5)));

        let chunk = create_test_chunk_int(vec![1]);

        let result = eval_eq(&arena, lit5, lit5_dup, &chunk).unwrap();
        let result_arr = result.as_any().downcast_ref::<BooleanArray>().unwrap();

        assert_eq!(result_arr.value(0), true);
    }

    #[test]
    fn test_ne_integers() {
        let mut arena = ExprArena::default();
        let lit5 = arena.push(ExprNode::Literal(LiteralValue::Int64(5)));
        let lit3 = arena.push(ExprNode::Literal(LiteralValue::Int64(3)));

        let chunk = create_test_chunk_int(vec![1]);

        let result = eval_ne(&arena, lit5, lit3, &chunk).unwrap();
        let result_arr = result.as_any().downcast_ref::<BooleanArray>().unwrap();

        assert_eq!(result_arr.value(0), true);
    }

    #[test]
    fn test_eq_for_null_integers() {
        let mut arena = ExprArena::default();
        let l = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int64);
        let r = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Int64);
        let expr = arena.push_typed(ExprNode::EqForNull(l, r), DataType::Boolean);

        let chunk = create_test_chunk_i64_nullable(
            vec![None, Some(1), Some(2), None],
            vec![None, Some(1), None, Some(2)],
        );
        let out = arena.eval(expr, &chunk).unwrap();
        let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(out.len(), 4);
        assert_eq!(out.value(0), true);
        assert_eq!(out.value(1), true);
        assert_eq!(out.value(2), false);
        assert_eq!(out.value(3), false);
        assert!(!out.is_null(0));
        assert!(!out.is_null(1));
        assert!(!out.is_null(2));
        assert!(!out.is_null(3));
    }

    #[test]
    fn test_lt_integers() {
        let mut arena = ExprArena::default();
        let lit3 = arena.push(ExprNode::Literal(LiteralValue::Int64(3)));
        let lit5 = arena.push(ExprNode::Literal(LiteralValue::Int64(5)));

        let chunk = create_test_chunk_int(vec![1]);

        let result = eval_lt(&arena, lit3, lit5, &chunk).unwrap();
        let result_arr = result.as_any().downcast_ref::<BooleanArray>().unwrap();

        assert_eq!(result_arr.value(0), true);
    }

    #[test]
    fn test_gt_integers() {
        let mut arena = ExprArena::default();
        let lit10 = arena.push(ExprNode::Literal(LiteralValue::Int64(10)));
        let lit5 = arena.push(ExprNode::Literal(LiteralValue::Int64(5)));

        let chunk = create_test_chunk_int(vec![1]);

        let result = eval_gt(&arena, lit10, lit5, &chunk).unwrap();
        let result_arr = result.as_any().downcast_ref::<BooleanArray>().unwrap();

        assert_eq!(result_arr.value(0), true);
    }

    #[test]
    fn test_and_logic() {
        let mut arena = ExprArena::default();
        let lit5 = arena.push(ExprNode::Literal(LiteralValue::Int64(5)));
        let lit3 = arena.push(ExprNode::Literal(LiteralValue::Int64(3)));
        let lit10 = arena.push(ExprNode::Literal(LiteralValue::Int64(10)));

        let chunk = create_test_chunk_int(vec![1]);

        // (5 > 3) AND (5 < 10) should be true
        let gt_expr = arena.push(ExprNode::Gt(lit5, lit3));
        let lt_expr = arena.push(ExprNode::Lt(lit5, lit10));

        let result = eval_and(&arena, gt_expr, lt_expr, &chunk).unwrap();
        let result_arr = result.as_any().downcast_ref::<BooleanArray>().unwrap();

        assert_eq!(result_arr.value(0), true);
    }

    #[test]
    fn test_or_logic() {
        let mut arena = ExprArena::default();
        let lit5 = arena.push(ExprNode::Literal(LiteralValue::Int64(5)));
        let lit3 = arena.push(ExprNode::Literal(LiteralValue::Int64(3)));
        let lit10 = arena.push(ExprNode::Literal(LiteralValue::Int64(10)));

        let chunk = create_test_chunk_int(vec![1]);

        // (5 < 3) OR (5 < 10) should be true
        let lt1_expr = arena.push(ExprNode::Lt(lit5, lit3));
        let lt2_expr = arena.push(ExprNode::Lt(lit5, lit10));

        let result = eval_or(&arena, lt1_expr, lt2_expr, &chunk).unwrap();
        let result_arr = result.as_any().downcast_ref::<BooleanArray>().unwrap();

        assert_eq!(result_arr.value(0), true);
    }

    #[test]
    fn test_and_or_sql_three_valued_logic_with_nulls() {
        // SQL WHERE uses three-valued logic (3VL), so we must treat NULLs as "unknown":
        // - NULL OR TRUE  => TRUE
        // - NULL AND FALSE => FALSE
        let mut arena = ExprArena::default();
        let l = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Boolean);
        let r = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Boolean);
        let and_expr = arena.push_typed(ExprNode::And(l, r), DataType::Boolean);
        let or_expr = arena.push_typed(ExprNode::Or(l, r), DataType::Boolean);

        let chunk = create_test_chunk_bool(vec![None], vec![Some(true)]);
        let out = arena.eval(or_expr, &chunk).unwrap();
        let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(out.value(0), true);

        let chunk = create_test_chunk_bool(vec![None], vec![Some(false)]);
        let out = arena.eval(and_expr, &chunk).unwrap();
        let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(out.value(0), false);
    }

    #[test]
    fn test_gt_decimal_with_different_precision_same_scale() {
        let left = Arc::new(
            Decimal128Array::from(vec![Some(123_i128), Some(50_i128)])
                .with_precision_and_scale(7, 2)
                .unwrap(),
        ) as ArrayRef;
        let right = Arc::new(
            Decimal128Array::from(vec![Some(99_i128), Some(500_i128)])
                .with_precision_and_scale(4, 2)
                .unwrap(),
        ) as ArrayRef;

        let mut arena = ExprArena::default();
        let l = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Decimal128(7, 2));
        let r = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Decimal128(4, 2));
        let expr = arena.push_typed(ExprNode::Gt(l, r), DataType::Boolean);

        let chunk = {
            let schema = Arc::new(Schema::new(vec![
                field_with_slot_id(
                    Field::new("l", DataType::Decimal128(7, 2), true),
                    SlotId::new(1),
                ),
                field_with_slot_id(
                    Field::new("r", DataType::Decimal128(4, 2), true),
                    SlotId::new(2),
                ),
            ]));
            let batch = RecordBatch::try_new(schema, vec![left, right]).unwrap();
            Chunk::new(batch)
        };

        let out = arena.eval(expr, &chunk).unwrap();
        let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
        // 1.23 > 0.99 => true
        assert_eq!(out.value(0), true);
        // 0.50 > 5.00 => false
        assert_eq!(out.value(1), false);
    }

    #[test]
    fn test_compare_timestamp_and_utf8_literal() {
        let ts_arr = Arc::new(arrow::array::TimestampMicrosecondArray::from(vec![
            Some(1_704_067_200_000_000_i64), // 2024-01-01 00:00:00
        ])) as ArrayRef;
        let str_arr = Arc::new(StringArray::from(vec![Some("0001-01-01 00:00:00")])) as ArrayRef;

        let mut arena = ExprArena::default();
        let l = arena.push_typed(
            ExprNode::SlotId(SlotId::new(1)),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
        );
        let r = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Utf8);
        let expr = arena.push_typed(ExprNode::Gt(l, r), DataType::Boolean);

        let schema = Arc::new(Schema::new(vec![
            field_with_slot_id(
                Field::new(
                    "l",
                    DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                    true,
                ),
                SlotId::new(1),
            ),
            field_with_slot_id(Field::new("r", DataType::Utf8, true), SlotId::new(2)),
        ]));
        let batch = RecordBatch::try_new(schema, vec![ts_arr, str_arr]).unwrap();
        let chunk = Chunk::new(batch);

        let out = arena.eval(expr, &chunk).unwrap();
        let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(out.value(0), true);
    }

    #[test]
    fn test_eq_list_arrays() {
        let mut arena = ExprArena::default();
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let left = ListArray::from_iter_primitive::<arrow::datatypes::Int64Type, _, _>(vec![
            Some(vec![Some(22), Some(11), Some(33)]),
            Some(vec![Some(22), Some(11), Some(44)]),
            None,
        ]);
        let right = ListArray::from_iter_primitive::<arrow::datatypes::Int64Type, _, _>(vec![
            Some(vec![Some(22), Some(11), Some(33)]),
            Some(vec![Some(22), Some(11), Some(33)]),
            None,
        ]);
        let chunk = create_test_chunk_list_i64(left, right, list_type.clone());

        let l = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), list_type.clone());
        let r = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), list_type);
        let expr = arena.push_typed(ExprNode::Eq(l, r), DataType::Boolean);
        let out = arena.eval(expr, &chunk).unwrap();
        let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(out.value(0), true);
        assert_eq!(out.value(1), false);
        assert!(out.is_null(2));
    }

    #[test]
    fn test_eq_for_null_list_arrays() {
        let mut arena = ExprArena::default();
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let left = ListArray::from_iter_primitive::<arrow::datatypes::Int64Type, _, _>(vec![
            None,
            Some(vec![Some(22), Some(11), Some(33)]),
            Some(vec![Some(22), None, Some(33)]),
            Some(vec![Some(22), None, Some(33)]),
            Some(vec![Some(22), Some(11), Some(33)]),
        ]);
        let right = ListArray::from_iter_primitive::<arrow::datatypes::Int64Type, _, _>(vec![
            None,
            Some(vec![Some(22), Some(11), Some(33)]),
            Some(vec![Some(22), None, Some(33)]),
            Some(vec![Some(22), Some(11), Some(33)]),
            None,
        ]);
        let chunk = create_test_chunk_list_i64(left, right, list_type.clone());

        let l = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), list_type.clone());
        let r = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), list_type);
        let expr = arena.push_typed(ExprNode::EqForNull(l, r), DataType::Boolean);
        let out = arena.eval(expr, &chunk).unwrap();
        let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(out.value(0), true);
        assert_eq!(out.value(1), true);
        assert_eq!(out.value(2), true);
        assert_eq!(out.value(3), false);
        assert_eq!(out.value(4), false);
    }

    #[test]
    fn test_gt_list_arrays() {
        let mut arena = ExprArena::default();
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let left = ListArray::from_iter_primitive::<arrow::datatypes::Int64Type, _, _>(vec![
            Some(vec![Some(22), Some(11), Some(44)]),
            Some(vec![Some(22), Some(11), Some(33)]),
        ]);
        let right = ListArray::from_iter_primitive::<arrow::datatypes::Int64Type, _, _>(vec![
            Some(vec![Some(22), Some(11), Some(33)]),
            Some(vec![Some(22), Some(11), Some(33)]),
        ]);
        let chunk = create_test_chunk_list_i64(left, right, list_type.clone());

        let l = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), list_type.clone());
        let r = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), list_type);
        let expr = arena.push_typed(ExprNode::Gt(l, r), DataType::Boolean);
        let out = arena.eval(expr, &chunk).unwrap();
        let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(out.value(0), true);
        assert_eq!(out.value(1), false);
    }

    #[test]
    fn test_eq_struct_arrays() {
        let mut arena = ExprArena::default();
        let fields = Fields::from(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
        ]);
        let struct_type = DataType::Struct(fields.clone());

        let left = StructArray::new(
            fields.clone(),
            vec![
                Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)])) as ArrayRef,
                Arc::new(Int32Array::from(vec![Some(1), Some(1), Some(1)])) as ArrayRef,
            ],
            None,
        );
        let right = StructArray::new(
            fields,
            vec![
                Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)])) as ArrayRef,
                Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(1)])) as ArrayRef,
            ],
            None,
        );
        let chunk = create_test_chunk_struct_i32(left, right, struct_type.clone());

        let l = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), struct_type.clone());
        let r = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), struct_type);
        let expr = arena.push_typed(ExprNode::Eq(l, r), DataType::Boolean);
        let out = arena.eval(expr, &chunk).unwrap();
        let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(out.value(0), true);
        assert_eq!(out.value(1), false);
        assert_eq!(out.value(2), true);
    }
}
