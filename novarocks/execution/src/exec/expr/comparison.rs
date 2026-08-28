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
use crate::exec::chunk::type_compatibility::{check_exact, retag_column};
use crate::exec::expr::cast::cast_with_special_rules;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Builder, ListArray, MapArray, StringArray, StructArray,
    TimestampMicrosecondBuilder,
};
use arrow::compute::cast;
use arrow::compute::kernels::boolean::not;
use arrow::compute::kernels::cmp::{eq, gt, gt_eq, lt, lt_eq, neq};
use arrow::datatypes::{DataType, Field, Fields};
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
    if !comparison_data_types_match(left.data_type(), right.data_type()) {
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
    if !comparison_data_types_match(left.data_type(), right.data_type()) {
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
    if matches!(left.data_type(), DataType::Map(_, _)) {
        let l = left
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| "failed to downcast left to MapArray".to_string())?;
        let r = right
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| "failed to downcast right to MapArray".to_string())?;
        return compare_map_rows(l, left_idx, r, right_idx);
    }
    compare_scalar_non_null(left, left_idx, right, right_idx).map(Some)
}

fn compare_value_recursive_in_list(
    left: &ArrayRef,
    left_idx: usize,
    right: &ArrayRef,
    right_idx: usize,
) -> Result<Ordering, String> {
    if !comparison_data_types_match(left.data_type(), right.data_type()) {
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
    if matches!(left.data_type(), DataType::Struct(_)) {
        let l = left
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| "failed to downcast left to StructArray".to_string())?;
        let r = right
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| "failed to downcast right to StructArray".to_string())?;
        return compare_struct_rows_non_null(l, left_idx, r, right_idx);
    }
    if matches!(left.data_type(), DataType::Map(_, _)) {
        let l = left
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| "failed to downcast left to MapArray".to_string())?;
        let r = right
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| "failed to downcast right to MapArray".to_string())?;
        return compare_map_rows_non_null(l, left_idx, r, right_idx);
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
        let ord = compare_value_recursive_in_list(left_values, l_idx, right_values, r_idx)?;
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

fn compare_struct_rows_non_null(
    left: &StructArray,
    left_row: usize,
    right: &StructArray,
    right_row: usize,
) -> Result<Ordering, String> {
    if left.columns().len() != right.columns().len() {
        return Err(format!(
            "struct compare field count mismatch: {} vs {}",
            left.columns().len(),
            right.columns().len()
        ));
    }

    for (left_col, right_col) in left.columns().iter().zip(right.columns()) {
        let ord = compare_value_recursive_in_list(left_col, left_row, right_col, right_row)?;
        if ord != Ordering::Equal {
            return Ok(ord);
        }
    }

    Ok(Ordering::Equal)
}

fn compare_map_rows_non_null(
    left: &MapArray,
    left_row: usize,
    right: &MapArray,
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

    let left_keys = left.keys();
    let right_keys = right.keys();
    let left_values = left.values();
    let right_values = right.values();
    for idx in 0..min_len {
        let left_idx = left_start + idx;
        let right_idx = right_start + idx;
        let key_ord = compare_value_recursive_in_list(left_keys, left_idx, right_keys, right_idx)?;
        if key_ord != Ordering::Equal {
            return Ok(key_ord);
        }
        let value_ord =
            compare_value_recursive_in_list(left_values, left_idx, right_values, right_idx)?;
        if value_ord != Ordering::Equal {
            return Ok(value_ord);
        }
    }

    Ok(left_len.cmp(&right_len))
}

fn compare_map_rows(
    left: &MapArray,
    left_row: usize,
    right: &MapArray,
    right_row: usize,
) -> Result<Option<Ordering>, String> {
    if left.is_null(left_row) || right.is_null(right_row) {
        return Ok(None);
    }
    compare_map_rows_non_null(left, left_row, right, right_row).map(Some)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NestedEq {
    Equal,
    NotEqual,
    Unknown,
}

fn option_eq_to_nested(value: Option<bool>) -> NestedEq {
    match value {
        Some(true) => NestedEq::Equal,
        Some(false) => NestedEq::NotEqual,
        None => NestedEq::Unknown,
    }
}

fn eq_value_recursive(
    left: &ArrayRef,
    left_idx: usize,
    right: &ArrayRef,
    right_idx: usize,
) -> Result<Option<bool>, String> {
    if !comparison_data_types_match(left.data_type(), right.data_type()) {
        return Err(format!(
            "eq type mismatch: {:?} vs {:?}",
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
        return eq_list_rows(l, left_idx, r, right_idx);
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
        return eq_struct_rows(l, left_idx, r, right_idx);
    }
    if matches!(left.data_type(), DataType::Map(_, _)) {
        let l = left
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| "failed to downcast left to MapArray".to_string())?;
        let r = right
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| "failed to downcast right to MapArray".to_string())?;
        return eq_map_rows(l, left_idx, r, right_idx);
    }
    Ok(Some(
        compare_scalar_non_null(left, left_idx, right, right_idx)? == Ordering::Equal,
    ))
}

fn eq_value_recursive_nested(
    left: &ArrayRef,
    left_idx: usize,
    right: &ArrayRef,
    right_idx: usize,
) -> Result<NestedEq, String> {
    if !comparison_data_types_match(left.data_type(), right.data_type()) {
        return Err(format!(
            "nested eq type mismatch: {:?} vs {:?}",
            left.data_type(),
            right.data_type()
        ));
    }
    let left_is_null = left.is_null(left_idx);
    let right_is_null = right.is_null(right_idx);
    match (left_is_null, right_is_null) {
        (true, true) => return Ok(NestedEq::Equal),
        (true, false) | (false, true) => return Ok(NestedEq::Unknown),
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
        return Ok(option_eq_to_nested(eq_list_rows(
            l, left_idx, r, right_idx,
        )?));
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
        return Ok(option_eq_to_nested(eq_struct_rows(
            l, left_idx, r, right_idx,
        )?));
    }
    if matches!(left.data_type(), DataType::Map(_, _)) {
        let l = left
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| "failed to downcast left to MapArray".to_string())?;
        let r = right
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| "failed to downcast right to MapArray".to_string())?;
        return Ok(option_eq_to_nested(eq_map_rows(l, left_idx, r, right_idx)?));
    }
    Ok(
        if compare_scalar_non_null(left, left_idx, right, right_idx)? == Ordering::Equal {
            NestedEq::Equal
        } else {
            NestedEq::NotEqual
        },
    )
}

fn eq_list_rows(
    left: &ListArray,
    left_row: usize,
    right: &ListArray,
    right_row: usize,
) -> Result<Option<bool>, String> {
    if left.is_null(left_row) || right.is_null(right_row) {
        return Ok(None);
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
        return Ok(Some(false));
    }

    let left_values = left.values();
    let right_values = right.values();
    let mut has_unknown = false;
    for idx in 0..left_len {
        let l_idx = left_start + idx;
        let r_idx = right_start + idx;
        match eq_value_recursive_nested(left_values, l_idx, right_values, r_idx)? {
            NestedEq::Equal => {}
            NestedEq::NotEqual => return Ok(Some(false)),
            NestedEq::Unknown => has_unknown = true,
        }
    }

    Ok(if has_unknown { None } else { Some(true) })
}

fn eq_struct_rows(
    left: &StructArray,
    left_row: usize,
    right: &StructArray,
    right_row: usize,
) -> Result<Option<bool>, String> {
    if left.is_null(left_row) || right.is_null(right_row) {
        return Ok(None);
    }
    if left.columns().len() != right.columns().len() {
        return Err(format!(
            "eq struct field count mismatch: {} vs {}",
            left.columns().len(),
            right.columns().len()
        ));
    }

    let mut has_unknown = false;
    for (left_col, right_col) in left.columns().iter().zip(right.columns()) {
        match eq_value_recursive_nested(left_col, left_row, right_col, right_row)? {
            NestedEq::Equal => {}
            NestedEq::NotEqual => return Ok(Some(false)),
            NestedEq::Unknown => has_unknown = true,
        }
    }

    Ok(if has_unknown { None } else { Some(true) })
}

fn eq_map_rows(
    left: &MapArray,
    left_row: usize,
    right: &MapArray,
    right_row: usize,
) -> Result<Option<bool>, String> {
    if left.is_null(left_row) || right.is_null(right_row) {
        return Ok(None);
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
        return Ok(Some(false));
    }

    let left_keys = left.keys();
    let right_keys = right.keys();
    let left_values = left.values();
    let right_values = right.values();
    let mut has_unknown = false;
    for idx in 0..left_len {
        let left_idx = left_start + idx;
        let right_idx = right_start + idx;
        match eq_value_recursive_nested(left_keys, left_idx, right_keys, right_idx)? {
            NestedEq::Equal => {}
            NestedEq::NotEqual => return Ok(Some(false)),
            NestedEq::Unknown => has_unknown = true,
        }
        match eq_value_recursive_nested(left_values, left_idx, right_values, right_idx)? {
            NestedEq::Equal => {}
            NestedEq::NotEqual => return Ok(Some(false)),
            NestedEq::Unknown => has_unknown = true,
        }
    }

    Ok(if has_unknown { None } else { Some(true) })
}

fn eq_value_recursive_null_safe(
    left: &ArrayRef,
    left_idx: usize,
    right: &ArrayRef,
    right_idx: usize,
) -> Result<bool, String> {
    if !comparison_data_types_match(left.data_type(), right.data_type()) {
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
    if matches!(left.data_type(), DataType::Map(_, _)) {
        let l = left
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| "failed to downcast left to MapArray".to_string())?;
        let r = right
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| "failed to downcast right to MapArray".to_string())?;
        return eq_map_rows_null_safe(l, left_idx, r, right_idx);
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
        if !eq_value_recursive_null_safe(left_values, l_idx, right_values, r_idx)? {
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

fn eq_map_rows_null_safe(
    left: &MapArray,
    left_row: usize,
    right: &MapArray,
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

    let left_keys = left.keys();
    let right_keys = right.keys();
    let left_values = left.values();
    let right_values = right.values();
    for idx in 0..left_len {
        let left_idx = left_start + idx;
        let right_idx = right_start + idx;
        if !eq_value_recursive_null_safe(left_keys, left_idx, right_keys, right_idx)? {
            return Ok(false);
        }
        if !eq_value_recursive_null_safe(left_values, left_idx, right_values, right_idx)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn eval_null_safe_eq(left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef, String> {
    if !comparison_data_types_match(left.data_type(), right.data_type()) {
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
    if !comparison_data_types_match(left.data_type(), right.data_type()) {
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

fn eval_nested_eq(left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef, String> {
    if !comparison_data_types_match(left.data_type(), right.data_type()) {
        return Err(format!(
            "nested eq type mismatch: {:?} vs {:?}",
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
                "nested eq row out of bounds: left_len={} right_len={} row={}",
                left.len(),
                right.len(),
                row
            ));
        }
        match eq_value_recursive(left, left_row, right, right_row)? {
            Some(value) => builder.append_value(value),
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn eval_nested_ne(left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef, String> {
    if !comparison_data_types_match(left.data_type(), right.data_type()) {
        return Err(format!(
            "nested ne type mismatch: {:?} vs {:?}",
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
                "nested ne row out of bounds: left_len={} right_len={} row={}",
                left.len(),
                right.len(),
                row
            ));
        }
        match eq_value_recursive(left, left_row, right, right_row)? {
            Some(value) => builder.append_value(!value),
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
    if comparison_data_types_match(left_type, right_type) {
        return Ok((left, right));
    }

    if let Some(normalized) = normalize_nested_metadata_only_types(left.clone(), right.clone())? {
        return Ok(normalized);
    }

    if let Some(normalized) = normalize_nested_comparison_types(left.clone(), right.clone())? {
        return Ok(normalized);
    }

    if let Some(normalized) = normalize_dictionary_comparison_types(left.clone(), right.clone())? {
        return Ok(normalized);
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

    // Numeric / decimal: delegate the type decision to the single authority,
    // then materialize the cast on both arrays. String / temporal pairs were
    // already handled above; everything else is incompatible.
    match novarocks_types::comparison_common_type(left_type, right_type)? {
        Some(target) => {
            let left_cast = if left_type == &target {
                left
            } else {
                cast_with_special_rules(&left, &target)?
            };
            let right_cast = if right_type == &target {
                right
            } else {
                cast_with_special_rules(&right, &target)?
            };
            Ok((left_cast, right_cast))
        }
        None => Err(format!(
            "Cannot compare incompatible types: {:?} vs {:?}",
            left_type, right_type
        )),
    }
}

fn comparison_data_types_match(left: &DataType, right: &DataType) -> bool {
    if left == right {
        return true;
    }
    match (left, right) {
        (DataType::List(left_item), DataType::List(right_item))
        | (DataType::LargeList(left_item), DataType::LargeList(right_item)) => {
            comparison_fields_match(left_item, right_item)
        }
        (
            DataType::FixedSizeList(left_item, left_size),
            DataType::FixedSizeList(right_item, right_size),
        ) => left_size == right_size && comparison_fields_match(left_item, right_item),
        (DataType::Struct(left_fields), DataType::Struct(right_fields)) => {
            left_fields.len() == right_fields.len()
                && left_fields
                    .iter()
                    .zip(right_fields.iter())
                    .all(|(left_field, right_field)| {
                        comparison_fields_match(left_field, right_field)
                    })
        }
        (
            DataType::Map(left_entries, left_ordered),
            DataType::Map(right_entries, right_ordered),
        ) => left_ordered == right_ordered && comparison_fields_match(left_entries, right_entries),
        _ => false,
    }
}

fn comparison_fields_match(
    left: &arrow::datatypes::Field,
    right: &arrow::datatypes::Field,
) -> bool {
    left.name() == right.name()
        && left.is_nullable() == right.is_nullable()
        && comparison_data_types_match(left.data_type(), right.data_type())
}

fn normalize_nested_metadata_only_types(
    left: ArrayRef,
    right: ArrayRef,
) -> Result<Option<(ArrayRef, ArrayRef)>, String> {
    if !matches!(
        left.data_type(),
        DataType::List(_) | DataType::Struct(_) | DataType::Map(_, _)
    ) || !matches!(
        right.data_type(),
        DataType::List(_) | DataType::Struct(_) | DataType::Map(_, _)
    ) {
        return Ok(None);
    }
    if check_exact(left.data_type(), right.data_type()).is_ok() {
        let right = retag_column(&right, left.data_type()).map_err(|m| format!("{m:?}"))?;
        let left = retag_column(&left, right.data_type()).map_err(|m| format!("{m:?}"))?;
        return Ok(Some((left, right)));
    }
    if check_exact(right.data_type(), left.data_type()).is_ok() {
        let left = retag_column(&left, right.data_type()).map_err(|m| format!("{m:?}"))?;
        let right = retag_column(&right, left.data_type()).map_err(|m| format!("{m:?}"))?;
        return Ok(Some((left, right)));
    }
    Ok(None)
}

fn same_nested_kind(left: &DataType, right: &DataType) -> bool {
    matches!(
        (left, right),
        (DataType::List(_), DataType::List(_))
            | (DataType::Struct(_), DataType::Struct(_))
            | (DataType::Map(_, _), DataType::Map(_, _))
    )
}

fn nested_comparison_field(left: &Field, right: &Field) -> Result<Option<Field>, String> {
    let Some(data_type) = nested_comparison_target_type(left.data_type(), right.data_type())?
    else {
        return Ok(None);
    };
    Ok(Some(
        Field::new(
            left.name().clone(),
            data_type,
            left.is_nullable() || right.is_nullable(),
        )
        .with_metadata(left.metadata().clone()),
    ))
}

fn nested_comparison_target_type(
    left: &DataType,
    right: &DataType,
) -> Result<Option<DataType>, String> {
    if left == right {
        return Ok(Some(left.clone()));
    }
    if !same_nested_kind(left, right) {
        return novarocks_types::comparison_common_type(left, right);
    }
    match (left, right) {
        (DataType::List(left_field), DataType::List(right_field)) => {
            let Some(field) = nested_comparison_field(left_field, right_field)? else {
                return Ok(None);
            };
            Ok(Some(DataType::List(Arc::new(field))))
        }
        (DataType::Struct(left_fields), DataType::Struct(right_fields)) => {
            if left_fields.len() != right_fields.len() {
                return Ok(None);
            }
            let mut fields = Vec::with_capacity(left_fields.len());
            for (left_field, right_field) in left_fields.iter().zip(right_fields.iter()) {
                let Some(field) = nested_comparison_field(left_field, right_field)? else {
                    return Ok(None);
                };
                fields.push(field);
            }
            Ok(Some(DataType::Struct(Fields::from(fields))))
        }
        (DataType::Map(left_entries, left_ordered), DataType::Map(right_entries, _)) => {
            let Some(entries) = nested_comparison_field(left_entries, right_entries)? else {
                return Ok(None);
            };
            Ok(Some(DataType::Map(Arc::new(entries), *left_ordered)))
        }
        _ => Ok(None),
    }
}

fn normalize_nested_comparison_types(
    left: ArrayRef,
    right: ArrayRef,
) -> Result<Option<(ArrayRef, ArrayRef)>, String> {
    if !same_nested_kind(left.data_type(), right.data_type()) {
        return Ok(None);
    }
    let Some(target_type) = nested_comparison_target_type(left.data_type(), right.data_type())?
    else {
        return Ok(None);
    };
    let left = if left.data_type() == &target_type {
        left
    } else {
        cast_with_special_rules(&left, &target_type)?
    };
    let right = if right.data_type() == &target_type {
        right
    } else {
        cast_with_special_rules(&right, &target_type)?
    };
    Ok(Some((left, right)))
}

fn is_string_or_null_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Null
    )
}

fn c1_dictionary_string_value_type(data_type: &DataType) -> Option<DataType> {
    match data_type {
        DataType::Dictionary(key_type, value_type)
            if matches!(key_type.as_ref(), DataType::Int32)
                && matches!(value_type.as_ref(), DataType::Utf8 | DataType::LargeUtf8) =>
        {
            Some(value_type.as_ref().clone())
        }
        _ => None,
    }
}

fn normalize_dictionary_comparison_types(
    left: ArrayRef,
    right: ArrayRef,
) -> Result<Option<(ArrayRef, ArrayRef)>, String> {
    let left_value_type = c1_dictionary_string_value_type(left.data_type());
    let right_value_type = c1_dictionary_string_value_type(right.data_type());
    match (left_value_type, right_value_type) {
        (None, None) => Ok(None),
        (Some(value_type), None) => {
            if !is_string_or_null_type(right.data_type()) {
                return Ok(None);
            }
            let right = if right.data_type() == &value_type {
                right
            } else {
                cast(&right, &value_type).map_err(|e| e.to_string())?
            };
            Ok(Some((left, right)))
        }
        (None, Some(value_type)) => {
            if !is_string_or_null_type(left.data_type()) {
                return Ok(None);
            }
            let left = if left.data_type() == &value_type {
                left
            } else {
                cast(&left, &value_type).map_err(|e| e.to_string())?
            };
            Ok(Some((left, right)))
        }
        (Some(left_value_type), Some(right_value_type)) => {
            if left_value_type != right_value_type {
                return Err(format!(
                    "Cannot compare dictionary arrays with different value types: {:?} vs {:?}",
                    left_value_type, right_value_type
                ));
            }
            Ok(Some((left, right)))
        }
    }
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
    if matches!(
        l_norm.data_type(),
        DataType::List(_) | DataType::Struct(_) | DataType::Map(_, _)
    ) {
        return eval_nested_eq(&l_norm, &r_norm);
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
    if matches!(
        l_norm.data_type(),
        DataType::List(_) | DataType::Struct(_) | DataType::Map(_, _)
    ) {
        return eval_nested_ne(&l_norm, &r_norm);
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
    if matches!(
        l_norm.data_type(),
        DataType::List(_) | DataType::Struct(_) | DataType::Map(_, _)
    ) {
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
    if matches!(
        l_norm.data_type(),
        DataType::List(_) | DataType::Struct(_) | DataType::Map(_, _)
    ) {
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
    if matches!(
        l_norm.data_type(),
        DataType::List(_) | DataType::Struct(_) | DataType::Map(_, _)
    ) {
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
    if matches!(
        l_norm.data_type(),
        DataType::List(_) | DataType::Struct(_) | DataType::Map(_, _)
    ) {
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
    use crate::exec::expr::{ExprNode, LiteralValue};
    use arrow::array::{
        BooleanArray, Decimal128Array, DictionaryArray, Int32Array, Int32Builder, Int64Array,
        Int64Builder, LargeStringDictionaryBuilder, ListArray, MapArray, MapBuilder, MapFieldNames,
        PrimitiveDictionaryBuilder, StringArray, StructArray,
    };
    use arrow::buffer::{NullBuffer, OffsetBuffer};
    use arrow::datatypes::{Field, Fields, Int8Type, Int32Type, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_types::SlotId;
    use novarocks_types::largeint;
    use std::collections::HashMap;

    fn create_test_chunk_int(values: Vec<i64>) -> Chunk {
        let array = Arc::new(Int64Array::from(values)) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col0",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
        {
            let batch = batch;
            let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                batch.schema().as_ref(),
                &[SlotId::new(1)],
            )
            .expect("chunk schema");
            Chunk::new_with_chunk_schema(batch, chunk_schema)
        }
    }

    fn create_test_chunk_i64_nullable(left: Vec<Option<i64>>, right: Vec<Option<i64>>) -> Chunk {
        let left = Arc::new(Int64Array::from(left)) as ArrayRef;
        let right = Arc::new(Int64Array::from(right)) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![
            Field::new("l", DataType::Int64, true),
            Field::new("r", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(schema, vec![left, right]).unwrap();
        {
            let batch = batch;
            let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                batch.schema().as_ref(),
                &[SlotId::new(1), SlotId::new(2)],
            )
            .expect("chunk schema");
            Chunk::new_with_chunk_schema(batch, chunk_schema)
        }
    }

    fn create_test_chunk_two_arrays(left: ArrayRef, right: ArrayRef) -> Chunk {
        let schema = Arc::new(Schema::new(vec![
            Field::new("l", left.data_type().clone(), true),
            Field::new("r", right.data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(schema, vec![left, right]).unwrap();
        let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[SlotId::new(1), SlotId::new(2)],
        )
        .expect("chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    }

    fn create_test_chunk_bool(l: Vec<Option<bool>>, r: Vec<Option<bool>>) -> Chunk {
        let l = Arc::new(BooleanArray::from(l)) as ArrayRef;
        let r = Arc::new(BooleanArray::from(r)) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![
            Field::new("l", DataType::Boolean, true),
            Field::new("r", DataType::Boolean, true),
        ]));
        let batch = RecordBatch::try_new(schema, vec![l, r]).unwrap();
        {
            let batch = batch;
            let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                batch.schema().as_ref(),
                &[SlotId::new(1), SlotId::new(2)],
            )
            .expect("chunk schema");
            Chunk::new_with_chunk_schema(batch, chunk_schema)
        }
    }

    fn create_test_chunk_dict_status(values: Vec<Option<&str>>) -> Chunk {
        let array =
            Arc::new(values.into_iter().collect::<DictionaryArray<Int32Type>>()) as ArrayRef;
        create_test_chunk_status_array(array)
    }

    fn create_test_chunk_large_dict_status(values: Vec<Option<&str>>) -> Chunk {
        let mut builder = LargeStringDictionaryBuilder::<Int32Type>::new();
        for value in values {
            match value {
                Some(value) => {
                    builder.append(value).unwrap();
                }
                None => builder.append_null(),
            }
        }
        create_test_chunk_status_array(Arc::new(builder.finish()) as ArrayRef)
    }

    fn create_test_chunk_i8_dict_status(values: Vec<Option<&str>>) -> Chunk {
        let array = Arc::new(values.into_iter().collect::<DictionaryArray<Int8Type>>()) as ArrayRef;
        create_test_chunk_status_array(array)
    }

    fn create_test_chunk_i32_dict_i32_status(values: Vec<Option<i32>>) -> Chunk {
        let mut builder = PrimitiveDictionaryBuilder::<Int32Type, Int32Type>::new();
        for value in values {
            match value {
                Some(value) => builder.append_value(value),
                None => builder.append_null(),
            }
        }
        create_test_chunk_status_array(Arc::new(builder.finish()) as ArrayRef)
    }

    fn create_test_chunk_status_array(array: ArrayRef) -> Chunk {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "status",
            array.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
        let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[SlotId::new(1)],
        )
        .expect("chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    }

    fn bool_values(array: &ArrayRef) -> Vec<Option<bool>> {
        let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
        (0..array.len())
            .map(|idx| (!array.is_null(idx)).then(|| array.value(idx)))
            .collect()
    }

    fn chunk_from_arrays(columns: Vec<(SlotId, &'static str, ArrayRef)>) -> Chunk {
        let fields = columns
            .iter()
            .map(|(_, name, array)| Field::new(*name, array.data_type().clone(), true))
            .collect::<Vec<_>>();
        let arrays = columns
            .iter()
            .map(|(_, _, array)| array.clone())
            .collect::<Vec<_>>();
        let slot_ids = columns.iter().map(|(slot, _, _)| *slot).collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema, arrays).unwrap();
        let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &slot_ids,
        )
        .unwrap();
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    }

    fn list_i64_with_field_id(field_id: &str) -> ArrayRef {
        let field = Arc::new(Field::new("item", DataType::Int64, true).with_metadata(
            HashMap::from([("PARQUET:field_id".to_string(), field_id.to_string())]),
        ));
        Arc::new(ListArray::new(
            field,
            OffsetBuffer::new(vec![0, 2, 4].into()),
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            None::<NullBuffer>,
        ))
    }

    fn list_decimal_26_2() -> ArrayRef {
        let field = Arc::new(Field::new("item", DataType::Decimal128(26, 2), true));
        Arc::new(ListArray::new(
            field,
            OffsetBuffer::new(vec![0, 2, 4].into()),
            Arc::new(
                Decimal128Array::from(vec![100_i128, 200_i128, 300_i128, 400_i128])
                    .with_precision_and_scale(26, 2)
                    .unwrap(),
            ),
            None::<NullBuffer>,
        ))
    }

    fn list_utf8_numeric_text() -> ArrayRef {
        let field = Arc::new(Field::new("item", DataType::Utf8, true));
        Arc::new(ListArray::new(
            field,
            OffsetBuffer::new(vec![0, 2, 4].into()),
            Arc::new(StringArray::from(vec!["1.00", "2.00", "3.00", "4.00"])),
            None::<NullBuffer>,
        ))
    }

    fn create_test_chunk_list_i64(left: ListArray, right: ListArray, list_type: DataType) -> Chunk {
        let left = Arc::new(left) as ArrayRef;
        let right = Arc::new(right) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![
            Field::new("l", list_type.clone(), true),
            Field::new("r", list_type, true),
        ]));
        let batch = RecordBatch::try_new(schema, vec![left, right]).unwrap();
        {
            let batch = batch;
            let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                batch.schema().as_ref(),
                &[SlotId::new(1), SlotId::new(2)],
            )
            .expect("chunk schema");
            Chunk::new_with_chunk_schema(batch, chunk_schema)
        }
    }

    fn create_test_chunk_struct_i32(
        left: StructArray,
        right: StructArray,
        struct_type: DataType,
    ) -> Chunk {
        let left = Arc::new(left) as ArrayRef;
        let right = Arc::new(right) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![
            Field::new("l", struct_type.clone(), true),
            Field::new("r", struct_type, true),
        ]));
        let batch = RecordBatch::try_new(schema, vec![left, right]).unwrap();
        {
            let batch = batch;
            let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                batch.schema().as_ref(),
                &[SlotId::new(1), SlotId::new(2)],
            )
            .expect("chunk schema");
            Chunk::new_with_chunk_schema(batch, chunk_schema)
        }
    }

    fn create_test_map_array(rows: &[Option<&[(i32, i64)]>]) -> MapArray {
        let mut builder = MapBuilder::new(
            Some(MapFieldNames {
                entry: "entries".to_string(),
                key: "key".to_string(),
                value: "value".to_string(),
            }),
            Int32Builder::new(),
            Int64Builder::new(),
        );
        for row in rows {
            match row {
                Some(entries) => {
                    for (key, value) in *entries {
                        builder.keys().append_value(*key);
                        builder.values().append_value(*value);
                    }
                    builder.append(true).unwrap();
                }
                None => builder.append(false).unwrap(),
            }
        }
        builder.finish()
    }

    type NullableMapRow<'a> = Option<&'a [(i32, Option<i64>)]>;

    fn create_test_map_array_nullable_values(rows: &[NullableMapRow<'_>]) -> MapArray {
        let mut builder = MapBuilder::new(
            Some(MapFieldNames {
                entry: "entries".to_string(),
                key: "key".to_string(),
                value: "value".to_string(),
            }),
            Int32Builder::new(),
            Int64Builder::new(),
        );
        for row in rows {
            match row {
                Some(entries) => {
                    for (key, value) in *entries {
                        builder.keys().append_value(*key);
                        match value {
                            Some(value) => builder.values().append_value(*value),
                            None => builder.values().append_null(),
                        }
                    }
                    builder.append(true).unwrap();
                }
                None => builder.append(false).unwrap(),
            }
        }
        builder.finish()
    }

    fn create_test_chunk_map_i32_i64(left: MapArray, right: MapArray, map_type: DataType) -> Chunk {
        let left = Arc::new(left) as ArrayRef;
        let right = Arc::new(right) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![
            Field::new("l", map_type.clone(), true),
            Field::new("r", map_type, true),
        ]));
        let batch = RecordBatch::try_new(schema, vec![left, right]).unwrap();
        {
            let batch = batch;
            let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                batch.schema().as_ref(),
                &[SlotId::new(1), SlotId::new(2)],
            )
            .expect("chunk schema");
            Chunk::new_with_chunk_schema(batch, chunk_schema)
        }
    }

    #[test]
    fn test_eq_integers() {
        let mut arena = ExprArena::default();
        let lit5 = arena.push(ExprNode::Literal(LiteralValue::Int64(5)));
        let lit5_dup = arena.push(ExprNode::Literal(LiteralValue::Int64(5)));

        let chunk = create_test_chunk_int(vec![1]);

        let result = eval_eq(&arena, lit5, lit5_dup, &chunk).unwrap();
        let result_arr = result.as_any().downcast_ref::<BooleanArray>().unwrap();

        assert!(result_arr.value(0));
    }

    #[test]
    fn test_ne_integers() {
        let mut arena = ExprArena::default();
        let lit5 = arena.push(ExprNode::Literal(LiteralValue::Int64(5)));
        let lit3 = arena.push(ExprNode::Literal(LiteralValue::Int64(3)));

        let chunk = create_test_chunk_int(vec![1]);

        let result = eval_ne(&arena, lit5, lit3, &chunk).unwrap();
        let result_arr = result.as_any().downcast_ref::<BooleanArray>().unwrap();

        assert!(result_arr.value(0));
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
        assert!(out.value(0));
        assert!(out.value(1));
        assert!(!out.value(2));
        assert!(!out.value(3));
        assert!(!out.is_null(0));
        assert!(!out.is_null(1));
        assert!(!out.is_null(2));
        assert!(!out.is_null(3));
    }

    #[test]
    fn dictionary_utf8_eq_literal_uses_logical_values() {
        let chunk =
            create_test_chunk_dict_status(vec![Some("PAID"), Some("PENDING"), None, Some("PAID")]);
        let mut arena = ExprArena::default();
        let slot = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Utf8);
        let lit = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("PAID".to_string())),
            DataType::Utf8,
        );
        let expr = arena.push_typed(ExprNode::Eq(slot, lit), DataType::Boolean);

        let result = arena.eval(expr, &chunk).expect("dictionary eq");

        assert_eq!(
            bool_values(&result),
            vec![Some(true), Some(false), None, Some(true)]
        );
    }

    #[test]
    fn nested_eq_ignores_arrow_field_metadata() {
        let left_array = list_i64_with_field_id("6");
        let right_array = list_i64_with_field_id("7");
        let chunk = chunk_from_arrays(vec![
            (SlotId::new(1), "l", left_array.clone()),
            (SlotId::new(2), "r", right_array.clone()),
        ]);
        let mut arena = ExprArena::default();
        let left_slot = arena.push_typed(
            ExprNode::SlotId(SlotId::new(1)),
            left_array.data_type().clone(),
        );
        let right_slot = arena.push_typed(
            ExprNode::SlotId(SlotId::new(2)),
            right_array.data_type().clone(),
        );
        let expr = arena.push_typed(ExprNode::Eq(left_slot, right_slot), DataType::Boolean);

        let result = arena
            .eval(expr, &chunk)
            .expect("metadata-only nested type difference");

        assert_eq!(bool_values(&result), vec![Some(true), Some(true)]);
    }

    #[test]
    fn nested_ne_casts_string_items_to_decimal_items() {
        let left_array = list_decimal_26_2();
        let right_array = list_utf8_numeric_text();
        let chunk = chunk_from_arrays(vec![
            (SlotId::new(1), "l", left_array.clone()),
            (SlotId::new(2), "r", right_array.clone()),
        ]);
        let mut arena = ExprArena::default();
        let left_slot = arena.push_typed(
            ExprNode::SlotId(SlotId::new(1)),
            left_array.data_type().clone(),
        );
        let right_slot = arena.push_typed(
            ExprNode::SlotId(SlotId::new(2)),
            right_array.data_type().clone(),
        );
        let expr = arena.push_typed(ExprNode::Ne(left_slot, right_slot), DataType::Boolean);

        let result = arena
            .eval(expr, &chunk)
            .expect("nested comparison should cast string items to decimal");

        assert_eq!(bool_values(&result), vec![Some(false), Some(false)]);
    }

    #[test]
    fn dictionary_utf8_ordering_literal_uses_logical_values() {
        let chunk = create_test_chunk_dict_status(vec![Some("a"), Some("c"), None, Some("b")]);
        let mut arena = ExprArena::default();
        let slot = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Utf8);
        let lit = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("b".to_string())),
            DataType::Utf8,
        );
        let expr = arena.push_typed(ExprNode::Lt(slot, lit), DataType::Boolean);

        let result = arena.eval(expr, &chunk).expect("dictionary lt");

        assert_eq!(
            bool_values(&result),
            vec![Some(true), Some(false), None, Some(false)]
        );
    }

    #[test]
    fn dictionary_utf8_missing_comparison_operators_use_logical_values() {
        type DictionaryComparisonCase = (
            &'static str,
            fn(ExprId, ExprId) -> ExprNode,
            Vec<Option<bool>>,
        );
        let cases: [DictionaryComparisonCase; 4] = [
            (
                "ne",
                ExprNode::Ne,
                vec![Some(true), Some(true), None, Some(false)],
            ),
            (
                "le",
                ExprNode::Le,
                vec![Some(true), Some(false), None, Some(true)],
            ),
            (
                "gt",
                ExprNode::Gt,
                vec![Some(false), Some(true), None, Some(false)],
            ),
            (
                "ge",
                ExprNode::Ge,
                vec![Some(false), Some(true), None, Some(true)],
            ),
        ];

        for (name, make_expr, expected) in cases {
            let chunk = create_test_chunk_dict_status(vec![Some("a"), Some("c"), None, Some("b")]);
            let mut arena = ExprArena::default();
            let slot = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Utf8);
            let lit = arena.push_typed(
                ExprNode::Literal(LiteralValue::Utf8("b".to_string())),
                DataType::Utf8,
            );
            let expr = arena.push_typed(make_expr(slot, lit), DataType::Boolean);

            let result = arena
                .eval(expr, &chunk)
                .unwrap_or_else(|err| panic!("dictionary {name}: {err}"));

            assert_eq!(bool_values(&result), expected, "{name}");
        }
    }

    #[test]
    fn dictionary_utf8_largeutf8_value_type_uses_logical_values() {
        let chunk = create_test_chunk_large_dict_status(vec![Some("PAID"), Some("PENDING"), None]);
        let mut arena = ExprArena::default();
        let slot = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::LargeUtf8);
        let lit = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("PAID".to_string())),
            DataType::LargeUtf8,
        );
        let expr = arena.push_typed(ExprNode::Eq(slot, lit), DataType::Boolean);

        let result = arena.eval(expr, &chunk).expect("dictionary LargeUtf8 eq");

        assert_eq!(bool_values(&result), vec![Some(true), Some(false), None]);
    }

    #[test]
    fn dictionary_utf8_reversed_literal_comparison_uses_logical_values() {
        let chunk = create_test_chunk_dict_status(vec![Some("a"), Some("c"), None, Some("b")]);
        let mut arena = ExprArena::default();
        let lit = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("b".to_string())),
            DataType::Utf8,
        );
        let slot = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Utf8);
        let expr = arena.push_typed(ExprNode::Gt(lit, slot), DataType::Boolean);

        let result = arena.eval(expr, &chunk).expect("dictionary reversed gt");

        assert_eq!(
            bool_values(&result),
            vec![Some(true), Some(false), None, Some(false)]
        );
    }

    #[test]
    fn dictionary_utf8_numeric_literal_does_not_cast_to_string() {
        let chunk = create_test_chunk_dict_status(vec![Some("1")]);
        let mut arena = ExprArena::default();
        let slot = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Utf8);
        let lit = arena.push_typed(ExprNode::Literal(LiteralValue::Int32(1)), DataType::Int32);
        let expr = arena.push_typed(ExprNode::Eq(slot, lit), DataType::Boolean);

        let err = arena
            .eval(expr, &chunk)
            .expect_err("numeric literal should not match C1 path");

        assert!(
            err.contains("Cannot compare incompatible types"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn dictionary_utf8_non_int32_key_does_not_match_c1_path() {
        let chunk = create_test_chunk_i8_dict_status(vec![Some("PAID")]);
        let mut arena = ExprArena::default();
        let slot = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Utf8);
        let lit = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("PAID".to_string())),
            DataType::Utf8,
        );
        let expr = arena.push_typed(ExprNode::Eq(slot, lit), DataType::Boolean);

        let err = arena
            .eval(expr, &chunk)
            .expect_err("non-Int32 dictionary key should fall back");

        assert!(
            err.contains("Cannot compare incompatible types"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn dictionary_utf8_non_string_dictionary_does_not_match_c1_path() {
        let chunk = create_test_chunk_i32_dict_i32_status(vec![Some(1)]);
        let mut arena = ExprArena::default();
        let slot = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let lit = arena.push_typed(ExprNode::Literal(LiteralValue::Int32(1)), DataType::Int32);
        let expr = arena.push_typed(ExprNode::Eq(slot, lit), DataType::Boolean);

        let err = arena
            .eval(expr, &chunk)
            .expect_err("non-string dictionary should fall back");

        assert!(
            err.contains("Cannot compare incompatible types"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_lt_integers() {
        let mut arena = ExprArena::default();
        let lit3 = arena.push(ExprNode::Literal(LiteralValue::Int64(3)));
        let lit5 = arena.push(ExprNode::Literal(LiteralValue::Int64(5)));

        let chunk = create_test_chunk_int(vec![1]);

        let result = eval_lt(&arena, lit3, lit5, &chunk).unwrap();
        let result_arr = result.as_any().downcast_ref::<BooleanArray>().unwrap();

        assert!(result_arr.value(0));
    }

    #[test]
    fn test_gt_integers() {
        let mut arena = ExprArena::default();
        let lit10 = arena.push(ExprNode::Literal(LiteralValue::Int64(10)));
        let lit5 = arena.push(ExprNode::Literal(LiteralValue::Int64(5)));

        let chunk = create_test_chunk_int(vec![1]);

        let result = eval_gt(&arena, lit10, lit5, &chunk).unwrap();
        let result_arr = result.as_any().downcast_ref::<BooleanArray>().unwrap();

        assert!(result_arr.value(0));
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

        assert!(result_arr.value(0));
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

        assert!(result_arr.value(0));
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
        assert!(out.value(0));

        let chunk = create_test_chunk_bool(vec![None], vec![Some(false)]);
        let out = arena.eval(and_expr, &chunk).unwrap();
        let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(!out.value(0));
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
                Field::new("l", DataType::Decimal128(7, 2), true),
                Field::new("r", DataType::Decimal128(4, 2), true),
            ]));
            let batch = RecordBatch::try_new(schema, vec![left, right]).unwrap();
            {
                let batch = batch;
                let chunk_schema =
                    crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                        batch.schema().as_ref(),
                        &[SlotId::new(1), SlotId::new(2)],
                    )
                    .expect("chunk schema");
                Chunk::new_with_chunk_schema(batch, chunk_schema)
            }
        };

        let out = arena.eval(expr, &chunk).unwrap();
        let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
        // 1.23 > 0.99 => true
        assert!(out.value(0));
        // 0.50 > 5.00 => false
        assert!(!out.value(1));
    }

    #[test]
    fn normalize_promotes_decimal_overflow_to_decimal256() {
        // common precision 40 > 38 -> both sides become Decimal256(40, 10)
        let left: ArrayRef = std::sync::Arc::new(
            Decimal128Array::from(vec![1_i128])
                .with_precision_and_scale(30, 0)
                .unwrap(),
        );
        let right: ArrayRef = std::sync::Arc::new(
            Decimal128Array::from(vec![1_i128])
                .with_precision_and_scale(30, 10)
                .unwrap(),
        );
        let (l, r) = normalize_comparison_types(left, right).expect("should promote, not error");
        assert_eq!(l.data_type(), &DataType::Decimal256(40, 10));
        assert_eq!(r.data_type(), &DataType::Decimal256(40, 10));
    }

    #[test]
    fn normalize_int_mismatch_still_int64() {
        use arrow::array::{Int32Array, Int64Array};
        let left: ArrayRef = std::sync::Arc::new(Int32Array::from(vec![1]));
        let right: ArrayRef = std::sync::Arc::new(Int64Array::from(vec![1_i64]));
        let (l, r) = normalize_comparison_types(left, right).unwrap();
        assert_eq!(l.data_type(), &DataType::Int64);
        assert_eq!(r.data_type(), &DataType::Int64);
    }

    #[test]
    fn bool_integer_comparison_uses_numeric_values() {
        let chunk = create_test_chunk_two_arrays(
            Arc::new(BooleanArray::from(vec![Some(true), Some(false)])) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(1_i64), Some(1_i64)])) as ArrayRef,
        );
        let mut arena = ExprArena::default();
        let l = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Boolean);
        let r = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Int64);
        let eq_expr = arena.push_typed(ExprNode::Eq(l, r), DataType::Boolean);
        let lt_expr = arena.push_typed(ExprNode::Lt(l, r), DataType::Boolean);

        let eq_out = arena.eval(eq_expr, &chunk).unwrap();
        let lt_out = arena.eval(lt_expr, &chunk).unwrap();

        assert_eq!(bool_values(&eq_out), vec![Some(true), Some(false)]);
        assert_eq!(bool_values(&lt_out), vec![Some(false), Some(true)]);
    }

    #[test]
    fn comparison_with_null_operand_returns_nulls() {
        let chunk = create_test_chunk_two_arrays(
            Arc::new(Int64Array::from(vec![Some(1_i64), Some(2_i64)])) as ArrayRef,
            arrow::array::new_null_array(&DataType::Null, 2),
        );
        let mut arena = ExprArena::default();
        let l = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int64);
        let r = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Null);
        let expr = arena.push_typed(ExprNode::Eq(l, r), DataType::Boolean);

        let out = arena.eval(expr, &chunk).unwrap();

        assert_eq!(bool_values(&out), vec![None, None]);
    }

    #[test]
    fn largeint_integer_comparison_uses_largeint_values() {
        let chunk = create_test_chunk_two_arrays(
            largeint::array_from_i128(&[Some(9_223_372_036_854_775_808_i128), Some(5_i128)])
                .unwrap(),
            Arc::new(Int64Array::from(vec![
                Some(9_223_372_036_854_775_807_i64),
                Some(10_i64),
            ])) as ArrayRef,
        );
        let mut arena = ExprArena::default();
        let largeint_type = DataType::FixedSizeBinary(largeint::LARGEINT_BYTE_WIDTH);
        let l = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), largeint_type);
        let r = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Int64);
        let expr = arena.push_typed(ExprNode::Gt(l, r), DataType::Boolean);

        let out = arena.eval(expr, &chunk).unwrap();

        assert_eq!(bool_values(&out), vec![Some(true), Some(false)]);
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
            Field::new(
                "l",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                true,
            ),
            Field::new("r", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(schema, vec![ts_arr, str_arr]).unwrap();
        let chunk = {
            let batch = batch;
            let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                batch.schema().as_ref(),
                &[SlotId::new(1), SlotId::new(2)],
            )
            .expect("chunk schema");
            Chunk::new_with_chunk_schema(batch, chunk_schema)
        };

        let out = arena.eval(expr, &chunk).unwrap();
        let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(out.value(0));
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
        assert!(out.value(0));
        assert!(!out.value(1));
        assert!(out.is_null(2));
    }

    #[test]
    fn test_eq_list_arrays_ignores_item_field_metadata() {
        let mut arena = ExprArena::default();
        let left_item = Arc::new(Field::new("item", DataType::Int64, true).with_metadata(
            HashMap::from([("PARQUET:field_id".to_string(), "6".to_string())]),
        ));
        let right_item = Arc::new(Field::new("item", DataType::Int64, true).with_metadata(
            HashMap::from([("PARQUET:field_id".to_string(), "7".to_string())]),
        ));
        let left_type = DataType::List(left_item.clone());
        let right_type = DataType::List(right_item.clone());
        let left = ListArray::try_new(
            left_item,
            OffsetBuffer::new(vec![0_i32, 3, 6].into()),
            Arc::new(Int64Array::from(vec![22_i64, 11, 33, 22, 11, 44])) as ArrayRef,
            None,
        )
        .expect("left list");
        let right = ListArray::try_new(
            right_item,
            OffsetBuffer::new(vec![0_i32, 3, 6].into()),
            Arc::new(Int64Array::from(vec![22_i64, 11, 33, 22, 11, 33])) as ArrayRef,
            None,
        )
        .expect("right list");
        let chunk = create_test_chunk_two_arrays(Arc::new(left), Arc::new(right));

        let l = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), left_type);
        let r = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), right_type);
        let expr = arena.push_typed(ExprNode::Eq(l, r), DataType::Boolean);
        let out = arena.eval(expr, &chunk).unwrap();
        let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(out.value(0));
        assert!(!out.value(1));
    }

    #[test]
    fn test_eq_list_arrays_with_nested_nulls() {
        let mut arena = ExprArena::default();
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let left = ListArray::from_iter_primitive::<arrow::datatypes::Int64Type, _, _>(vec![
            Some(vec![Some(22), None, Some(33)]),
            Some(vec![Some(22), None, Some(44)]),
            Some(vec![Some(22), None, Some(33)]),
        ]);
        let right = ListArray::from_iter_primitive::<arrow::datatypes::Int64Type, _, _>(vec![
            Some(vec![Some(22), None, Some(33)]),
            Some(vec![Some(22), None, Some(33)]),
            Some(vec![Some(22), Some(11), Some(33)]),
        ]);
        let chunk = create_test_chunk_list_i64(left, right, list_type.clone());

        let l = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), list_type.clone());
        let r = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), list_type);
        let expr = arena.push_typed(ExprNode::Eq(l, r), DataType::Boolean);
        let out = arena.eval(expr, &chunk).unwrap();
        let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(out.value(0));
        assert!(!out.value(1));
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
        assert!(out.value(0));
        assert!(out.value(1));
        assert!(out.value(2));
        assert!(!out.value(3));
        assert!(!out.value(4));
    }

    #[test]
    fn test_ne_list_arrays_with_nested_nulls() {
        let mut arena = ExprArena::default();
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let left = ListArray::from_iter_primitive::<arrow::datatypes::Int64Type, _, _>(vec![
            Some(vec![Some(22), None, Some(33)]),
            Some(vec![Some(22), None, Some(44)]),
            Some(vec![Some(22), None, Some(33)]),
        ]);
        let right = ListArray::from_iter_primitive::<arrow::datatypes::Int64Type, _, _>(vec![
            Some(vec![Some(22), None, Some(33)]),
            Some(vec![Some(22), None, Some(33)]),
            Some(vec![Some(22), Some(11), Some(33)]),
        ]);
        let chunk = create_test_chunk_list_i64(left, right, list_type.clone());

        let l = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), list_type.clone());
        let r = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), list_type);
        let expr = arena.push_typed(ExprNode::Ne(l, r), DataType::Boolean);
        let out = arena.eval(expr, &chunk).unwrap();
        let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(!out.value(0));
        assert!(out.value(1));
        assert!(out.is_null(2));
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
        assert!(out.value(0));
        assert!(!out.value(1));
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
                Arc::new(Int32Array::from(vec![
                    Some(1),
                    Some(2),
                    Some(3),
                    None,
                    None,
                ])) as ArrayRef,
                Arc::new(Int32Array::from(vec![
                    Some(1),
                    Some(1),
                    Some(1),
                    Some(4),
                    Some(5),
                ])) as ArrayRef,
            ],
            None,
        );
        let right = StructArray::new(
            fields,
            vec![
                Arc::new(Int32Array::from(vec![
                    Some(1),
                    Some(2),
                    Some(3),
                    None,
                    Some(5),
                ])) as ArrayRef,
                Arc::new(Int32Array::from(vec![
                    Some(1),
                    Some(2),
                    Some(1),
                    Some(4),
                    Some(5),
                ])) as ArrayRef,
            ],
            None,
        );
        let chunk = create_test_chunk_struct_i32(left, right, struct_type.clone());

        let l = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), struct_type.clone());
        let r = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), struct_type);
        let expr = arena.push_typed(ExprNode::Eq(l, r), DataType::Boolean);
        let out = arena.eval(expr, &chunk).unwrap();
        let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(out.value(0));
        assert!(!out.value(1));
        assert!(out.value(2));
        assert!(out.value(3));
        assert!(out.is_null(4));
    }

    #[test]
    fn test_eq_map_arrays() {
        let mut arena = ExprArena::default();
        let entries_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Int32, false),
                Field::new("value", DataType::Int64, true),
            ])),
            false,
        ));
        let map_type = DataType::Map(entries_field, false);
        let left =
            create_test_map_array(&[Some(&[(0, 10), (1, 11)]), Some(&[(0, 10), (1, 11)]), None]);
        let right =
            create_test_map_array(&[Some(&[(0, 10), (1, 11)]), Some(&[(0, 10), (1, 12)]), None]);
        let chunk = create_test_chunk_map_i32_i64(left, right, map_type.clone());

        let l = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), map_type.clone());
        let r = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), map_type);
        let expr = arena.push_typed(ExprNode::Eq(l, r), DataType::Boolean);
        let out = arena.eval(expr, &chunk).unwrap();
        let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(out.value(0));
        assert!(!out.value(1));
        assert!(out.is_null(2));
    }

    #[test]
    fn test_eq_map_arrays_with_nested_null_values() {
        let mut arena = ExprArena::default();
        let entries_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Int32, false),
                Field::new("value", DataType::Int64, true),
            ])),
            false,
        ));
        let map_type = DataType::Map(entries_field, false);
        let left = create_test_map_array_nullable_values(&[
            Some(&[(0, Some(10)), (1, None)]),
            Some(&[(0, Some(10)), (1, None)]),
            Some(&[(0, Some(10)), (1, None)]),
        ]);
        let right = create_test_map_array_nullable_values(&[
            Some(&[(0, Some(10)), (1, None)]),
            Some(&[(0, Some(10)), (1, Some(11))]),
            Some(&[(0, Some(10)), (1, Some(12))]),
        ]);
        let chunk = create_test_chunk_map_i32_i64(left, right, map_type.clone());

        let l = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), map_type.clone());
        let r = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), map_type);
        let expr = arena.push_typed(ExprNode::Eq(l, r), DataType::Boolean);
        let out = arena.eval(expr, &chunk).unwrap();
        let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(out.value(0));
        assert!(out.is_null(1));
        assert!(out.is_null(2));
    }

    #[test]
    fn test_eq_for_null_map_arrays() {
        let mut arena = ExprArena::default();
        let entries_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Int32, false),
                Field::new("value", DataType::Int64, true),
            ])),
            false,
        ));
        let map_type = DataType::Map(entries_field, false);
        let left =
            create_test_map_array(&[None, Some(&[(0, 10), (1, 11)]), Some(&[(0, 10), (1, 11)])]);
        let right =
            create_test_map_array(&[None, Some(&[(0, 10), (1, 11)]), Some(&[(0, 10), (1, 12)])]);
        let chunk = create_test_chunk_map_i32_i64(left, right, map_type.clone());

        let l = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), map_type.clone());
        let r = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), map_type);
        let expr = arena.push_typed(ExprNode::EqForNull(l, r), DataType::Boolean);
        let out = arena.eval(expr, &chunk).unwrap();
        let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(out.value(0));
        assert!(out.value(1));
        assert!(!out.value(2));
    }

    // IV3-7 Task 12: UTF-8 literal coercion against nanosecond timestamp columns must
    // preserve sub-microsecond precision, so that '...05.000000001' is not silently
    // truncated to '...05.000000' before comparison.
    #[test]
    fn normalize_utf8_vs_nanosecond_preserves_sub_microsecond() {
        use arrow::array::TimestampNanosecondArray;

        // Build a nanosecond timestamp column value: 2024-01-02 03:04:05.000000001
        // 1704164645_000000001 ns from epoch
        let ts_val = chrono::NaiveDateTime::parse_from_str(
            "2024-01-02 03:04:05.000000001",
            "%Y-%m-%d %H:%M:%S%.f",
        )
        .unwrap()
        .and_utc()
        .timestamp_nanos_opt()
        .unwrap();

        let ns_array = Arc::new(TimestampNanosecondArray::from(vec![Some(ts_val)])) as ArrayRef;
        // The literal is the same timestamp as a string.
        let lit_array = Arc::new(StringArray::from(vec![Some(
            "2024-01-02 03:04:05.000000001",
        )])) as ArrayRef;

        // normalize_comparison_types should coerce the string to nanoseconds.
        let (left, right) = normalize_comparison_types(ns_array, lit_array).unwrap();
        assert_eq!(
            left.data_type(),
            right.data_type(),
            "types must match after normalization"
        );
        let right_ns = right
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("right must be TimestampNanosecondArray after coercion");
        // The coerced literal must equal the original nanosecond value exactly,
        // including the sub-microsecond digit '1'. If truncated to microseconds,
        // right_ns.value(0) % 1000 == 0 and this assertion fails.
        assert_eq!(
            right_ns.value(0) % 1_000,
            1,
            "sub-microsecond digit lost in coercion: got ns_value={}",
            right_ns.value(0)
        );
        assert_eq!(
            right_ns.value(0),
            ts_val,
            "coerced literal does not equal the expected nanosecond value"
        );
    }
}
