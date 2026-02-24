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
use super::super::*;
use arrow::array::{
    BooleanArray, BooleanBuilder, Date32Array, Decimal128Array, Decimal256Array,
    FixedSizeBinaryArray, Float32Array, Float32Builder, Float64Array, Float64Builder, Int8Array,
    Int8Builder, Int16Array, Int16Builder, Int32Array, Int32Builder, Int64Array, Int64Builder,
    ListArray, MapArray, StringArray, StringBuilder, StructArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer, i256};
use chrono::{DateTime, NaiveDate};
use std::cmp::Ordering;

use crate::common::largeint;
const UNIX_EPOCH_DAY_OFFSET: i32 = 719163;

fn date32_to_naive(days: i32) -> Option<NaiveDate> {
    NaiveDate::from_num_days_from_ce_opt(UNIX_EPOCH_DAY_OFFSET + days)
}

pub(in crate::exec::expr::agg) fn build_bool_array(
    offset: usize,
    group_states: &[AggStatePtr],
) -> Result<ArrayRef, String> {
    let mut builder = BooleanBuilder::new();
    for &base in group_states {
        let state = unsafe { &*((base as *mut u8).add(offset) as *const BoolState) };
        if state.has_value {
            builder.append_value(state.value);
        } else {
            builder.append_null();
        }
    }
    Ok(Arc::new(builder.finish()))
}

pub(in crate::exec::expr::agg) fn build_utf8_array(
    offset: usize,
    group_states: &[AggStatePtr],
) -> Result<ArrayRef, String> {
    let mut builder = StringBuilder::new();
    for &base in group_states {
        let state = unsafe { &*((base as *mut u8).add(offset) as *const Utf8State) };
        match &state.value {
            Some(v) => builder.append_value(v),
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

pub(in crate::exec::expr::agg) fn build_date32_array(
    offset: usize,
    group_states: &[AggStatePtr],
) -> Result<ArrayRef, String> {
    let mut values = Vec::with_capacity(group_states.len());
    for &base in group_states {
        let state = unsafe { &*((base as *mut u8).add(offset) as *const I32State) };
        values.push(state.has_value.then_some(state.value));
    }
    Ok(Arc::new(Date32Array::from(values)))
}

pub(in crate::exec::expr::agg) fn build_timestamp_array(
    offset: usize,
    group_states: &[AggStatePtr],
    output_type: &DataType,
) -> Result<ArrayRef, String> {
    let (unit, tz) = match output_type {
        DataType::Timestamp(unit, tz) => (unit.clone(), tz.as_deref().map(|s| s.to_string())),
        other => return Err(format!("timestamp output type mismatch: {:?}", other)),
    };
    let mut values = Vec::with_capacity(group_states.len());
    for &base in group_states {
        let state = unsafe { &*((base as *mut u8).add(offset) as *const I64State) };
        values.push(state.has_value.then_some(state.value));
    }
    let array: ArrayRef = match unit {
        TimeUnit::Second => {
            let array = TimestampSecondArray::from(values);
            if let Some(tz) = tz {
                Arc::new(array.with_timezone(tz))
            } else {
                Arc::new(array)
            }
        }
        TimeUnit::Millisecond => {
            let array = TimestampMillisecondArray::from(values);
            if let Some(tz) = tz {
                Arc::new(array.with_timezone(tz))
            } else {
                Arc::new(array)
            }
        }
        TimeUnit::Microsecond => {
            let array = TimestampMicrosecondArray::from(values);
            if let Some(tz) = tz {
                Arc::new(array.with_timezone(tz))
            } else {
                Arc::new(array)
            }
        }
        TimeUnit::Nanosecond => {
            let array = TimestampNanosecondArray::from(values);
            if let Some(tz) = tz {
                Arc::new(array.with_timezone(tz))
            } else {
                Arc::new(array)
            }
        }
    };
    Ok(array)
}

pub(in crate::exec::expr::agg) fn build_decimal128_array(
    offset: usize,
    group_states: &[AggStatePtr],
    output_type: &DataType,
) -> Result<ArrayRef, String> {
    let (precision, scale) = match output_type {
        DataType::Decimal128(precision, scale) => (*precision, *scale),
        other => return Err(format!("decimal output type mismatch: {:?}", other)),
    };
    let mut values = Vec::with_capacity(group_states.len());
    for &base in group_states {
        let state = unsafe { &*((base as *mut u8).add(offset) as *const I128State) };
        values.push(state.has_value.then_some(state.value));
    }
    let array = Decimal128Array::from(values)
        .with_precision_and_scale(precision, scale)
        .map_err(|e| e.to_string())?;
    Ok(Arc::new(array))
}

pub(in crate::exec::expr::agg) fn build_decimal256_array(
    offset: usize,
    group_states: &[AggStatePtr],
    output_type: &DataType,
) -> Result<ArrayRef, String> {
    let (precision, scale) = match output_type {
        DataType::Decimal256(precision, scale) => (*precision, *scale),
        other => return Err(format!("decimal256 output type mismatch: {:?}", other)),
    };
    let mut values = Vec::with_capacity(group_states.len());
    for &base in group_states {
        let state = unsafe { &*((base as *mut u8).add(offset) as *const I256State) };
        values.push(state.has_value.then_some(state.value));
    }
    let array = Decimal256Array::from(values)
        .with_precision_and_scale(precision, scale)
        .map_err(|e| e.to_string())?;
    Ok(Arc::new(array))
}

pub(in crate::exec::expr::agg) fn build_largeint_array(
    offset: usize,
    group_states: &[AggStatePtr],
) -> Result<ArrayRef, String> {
    let mut values = Vec::with_capacity(group_states.len());
    for &base in group_states {
        let state = unsafe { &*((base as *mut u8).add(offset) as *const I128State) };
        values.push(state.has_value.then_some(state.value));
    }
    largeint::array_from_i128(&values)
}

#[derive(Clone, Debug)]
pub(crate) enum AggScalarValue {
    Bool(bool),
    Int64(i64),
    Float64(f64),
    Utf8(String),
    Date32(i32),
    Timestamp(i64),
    Decimal128(i128),
    Decimal256(i256),
    Struct(Vec<Option<AggScalarValue>>),
    Map(Vec<(Option<AggScalarValue>, Option<AggScalarValue>)>),
    List(Vec<Option<AggScalarValue>>),
}

pub(crate) fn scalar_from_array(
    array: &ArrayRef,
    row: usize,
) -> Result<Option<AggScalarValue>, String> {
    match array.data_type() {
        DataType::Boolean => {
            let arr = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| "failed to downcast to BooleanArray".to_string())?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(AggScalarValue::Bool(arr.value(row))))
            }
        }
        DataType::Int8 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| "failed to downcast to Int8Array".to_string())?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(AggScalarValue::Int64(arr.value(row) as i64)))
            }
        }
        DataType::Int16 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| "failed to downcast to Int16Array".to_string())?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(AggScalarValue::Int64(arr.value(row) as i64)))
            }
        }
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "failed to downcast to Int32Array".to_string())?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(AggScalarValue::Int64(arr.value(row) as i64)))
            }
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "failed to downcast to Int64Array".to_string())?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(AggScalarValue::Int64(arr.value(row))))
            }
        }
        DataType::Float32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| "failed to downcast to Float32Array".to_string())?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(AggScalarValue::Float64(arr.value(row) as f64)))
            }
        }
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| "failed to downcast to Float64Array".to_string())?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(AggScalarValue::Float64(arr.value(row))))
            }
        }
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "failed to downcast to StringArray".to_string())?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(AggScalarValue::Utf8(arr.value(row).to_string())))
            }
        }
        DataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| "failed to downcast to Date32Array".to_string())?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(AggScalarValue::Date32(arr.value(row))))
            }
        }
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .ok_or_else(|| "failed to downcast to TimestampSecondArray".to_string())?;
                if arr.is_null(row) {
                    Ok(None)
                } else {
                    Ok(Some(AggScalarValue::Timestamp(arr.value(row))))
                }
            }
            TimeUnit::Millisecond => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .ok_or_else(|| "failed to downcast to TimestampMillisecondArray".to_string())?;
                if arr.is_null(row) {
                    Ok(None)
                } else {
                    Ok(Some(AggScalarValue::Timestamp(arr.value(row))))
                }
            }
            TimeUnit::Microsecond => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(|| "failed to downcast to TimestampMicrosecondArray".to_string())?;
                if arr.is_null(row) {
                    Ok(None)
                } else {
                    Ok(Some(AggScalarValue::Timestamp(arr.value(row))))
                }
            }
            TimeUnit::Nanosecond => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .ok_or_else(|| "failed to downcast to TimestampNanosecondArray".to_string())?;
                if arr.is_null(row) {
                    Ok(None)
                } else {
                    Ok(Some(AggScalarValue::Timestamp(arr.value(row))))
                }
            }
        },
        DataType::Decimal128(_, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| "failed to downcast to Decimal128Array".to_string())?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(AggScalarValue::Decimal128(arr.value(row))))
            }
        }
        DataType::Decimal256(_, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| "failed to downcast to Decimal256Array".to_string())?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(AggScalarValue::Decimal256(arr.value(row))))
            }
        }
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            let arr = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| "failed to downcast to FixedSizeBinaryArray".to_string())?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                let v = largeint::value_at(arr, row)?;
                Ok(Some(AggScalarValue::Decimal128(v)))
            }
        }
        DataType::List(_item) => {
            let arr = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| "failed to downcast to ListArray".to_string())?;
            if arr.is_null(row) {
                return Ok(None);
            }
            let offsets = arr.value_offsets();
            let start = offsets[row] as usize;
            let end = offsets[row + 1] as usize;
            let values = arr.values();
            let mut out = Vec::with_capacity(end.saturating_sub(start));
            for idx in start..end {
                out.push(scalar_from_array(&values, idx)?);
            }
            Ok(Some(AggScalarValue::List(out)))
        }
        DataType::Struct(fields) => {
            let arr = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| "failed to downcast to StructArray".to_string())?;
            if arr.is_null(row) {
                return Ok(None);
            }
            let mut out = Vec::with_capacity(fields.len());
            for col in arr.columns() {
                out.push(scalar_from_array(col, row)?);
            }
            Ok(Some(AggScalarValue::Struct(out)))
        }
        DataType::Map(_, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(|| "failed to downcast to MapArray".to_string())?;
            if arr.is_null(row) {
                return Ok(None);
            }
            let offsets = arr.value_offsets();
            let start = offsets[row] as usize;
            let end = offsets[row + 1] as usize;
            let keys = arr.keys();
            let values = arr.values();
            let mut out = Vec::with_capacity(end.saturating_sub(start));
            for idx in start..end {
                out.push((
                    scalar_from_array(&keys, idx)?,
                    scalar_from_array(&values, idx)?,
                ));
            }
            Ok(Some(AggScalarValue::Map(out)))
        }
        other => Err(format!("unsupported scalar type: {:?}", other)),
    }
}

pub(in crate::exec::expr::agg) fn scalar_to_string(
    value: &AggScalarValue,
    data_type: &DataType,
) -> Result<String, String> {
    match value {
        AggScalarValue::Bool(v) => Ok(if *v { "1".to_string() } else { "0".to_string() }),
        AggScalarValue::Int64(v) => Ok(v.to_string()),
        AggScalarValue::Float64(v) => Ok(v.to_string()),
        AggScalarValue::Utf8(v) => Ok(v.clone()),
        AggScalarValue::Date32(v) => {
            let date = date32_to_naive(*v).ok_or_else(|| "invalid date32 value".to_string())?;
            Ok(date.format("%Y-%m-%d").to_string())
        }
        AggScalarValue::Timestamp(v) => match data_type {
            DataType::Timestamp(unit, tz) => Ok(format_timestamp(*unit, *v, tz.as_deref())),
            _ => Ok(v.to_string()),
        },
        AggScalarValue::Decimal128(v) => match data_type {
            DataType::Decimal128(_, scale) => Ok(format_decimal(*v, *scale as i8)),
            _ => Ok(v.to_string()),
        },
        AggScalarValue::Decimal256(v) => match data_type {
            DataType::Decimal256(_, scale) => Ok(format_decimal256(*v, *scale as i8)),
            _ => Ok(v.to_string()),
        },
        AggScalarValue::Struct(items) => {
            let mut rendered = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    Some(v) => rendered.push(scalar_to_string(v, data_type)?),
                    None => rendered.push("NULL".to_string()),
                }
            }
            Ok(format!("{{{}}}", rendered.join(",")))
        }
        AggScalarValue::Map(items) => {
            let mut rendered = Vec::with_capacity(items.len());
            for (k, v) in items {
                let key = match k {
                    Some(k) => scalar_to_string(k, data_type)?,
                    None => "NULL".to_string(),
                };
                let value = match v {
                    Some(v) => scalar_to_string(v, data_type)?,
                    None => "NULL".to_string(),
                };
                rendered.push(format!("{}:{}", key, value));
            }
            Ok(format!("{{{}}}", rendered.join(",")))
        }
        AggScalarValue::List(items) => {
            let mut rendered = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    Some(v) => rendered.push(scalar_to_string(v, data_type)?),
                    None => rendered.push("NULL".to_string()),
                }
            }
            Ok(format!("[{}]", rendered.join(",")))
        }
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
            let seconds = value / 1_000;
            let nanos = ((value % 1_000) * 1_000_000) as u32;
            let dt = DateTime::from_timestamp(seconds, nanos)
                .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());
            dt.naive_utc().format("%Y-%m-%d %H:%M:%S%.3f").to_string()
        }
        TimeUnit::Microsecond => {
            let seconds = value / 1_000_000;
            let nanos = ((value % 1_000_000) * 1_000) as u32;
            let dt = DateTime::from_timestamp(seconds, nanos)
                .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());
            dt.naive_utc().format("%Y-%m-%d %H:%M:%S%.6f").to_string()
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

pub(crate) fn compare_scalar_values(
    left: &AggScalarValue,
    right: &AggScalarValue,
) -> Result<Ordering, String> {
    match (left, right) {
        (AggScalarValue::Bool(l), AggScalarValue::Bool(r)) => Ok(l.cmp(r)),
        (AggScalarValue::Int64(l), AggScalarValue::Int64(r)) => Ok(l.cmp(r)),
        (AggScalarValue::Float64(l), AggScalarValue::Float64(r)) => l
            .partial_cmp(r)
            .ok_or_else(|| "float comparison is not ordered".to_string()),
        (AggScalarValue::Utf8(l), AggScalarValue::Utf8(r)) => Ok(l.cmp(r)),
        (AggScalarValue::Date32(l), AggScalarValue::Date32(r)) => Ok(l.cmp(r)),
        (AggScalarValue::Timestamp(l), AggScalarValue::Timestamp(r)) => Ok(l.cmp(r)),
        (AggScalarValue::Decimal128(l), AggScalarValue::Decimal128(r)) => Ok(l.cmp(r)),
        (AggScalarValue::Decimal256(l), AggScalarValue::Decimal256(r)) => Ok(l.cmp(r)),
        (AggScalarValue::Struct(l), AggScalarValue::Struct(r)) => {
            let min_len = l.len().min(r.len());
            for idx in 0..min_len {
                let ord = compare_optional_scalar_values(&l[idx], &r[idx])?;
                if !ord.is_eq() {
                    return Ok(ord);
                }
            }
            Ok(l.len().cmp(&r.len()))
        }
        (AggScalarValue::Map(l), AggScalarValue::Map(r)) => {
            let min_len = l.len().min(r.len());
            for idx in 0..min_len {
                let (lk, lv) = &l[idx];
                let (rk, rv) = &r[idx];
                let key_ord = compare_optional_scalar_values(lk, rk)?;
                if !key_ord.is_eq() {
                    return Ok(key_ord);
                }
                let value_ord = compare_optional_scalar_values(lv, rv)?;
                if !value_ord.is_eq() {
                    return Ok(value_ord);
                }
            }
            Ok(l.len().cmp(&r.len()))
        }
        (AggScalarValue::List(l), AggScalarValue::List(r)) => {
            let min_len = l.len().min(r.len());
            for idx in 0..min_len {
                let ord = compare_optional_scalar_values(&l[idx], &r[idx])?;
                if !ord.is_eq() {
                    return Ok(ord);
                }
            }
            Ok(l.len().cmp(&r.len()))
        }
        _ => Err("scalar comparison type mismatch".to_string()),
    }
}

fn compare_optional_scalar_values(
    left: &Option<AggScalarValue>,
    right: &Option<AggScalarValue>,
) -> Result<Ordering, String> {
    match (left, right) {
        (None, None) => Ok(Ordering::Equal),
        (None, Some(_)) => Ok(Ordering::Less),
        (Some(_), None) => Ok(Ordering::Greater),
        (Some(l), Some(r)) => compare_scalar_values(l, r),
    }
}

pub(crate) fn build_scalar_array(
    output_type: &DataType,
    values: Vec<Option<AggScalarValue>>,
) -> Result<ArrayRef, String> {
    match output_type {
        DataType::Boolean => {
            let mut builder = BooleanBuilder::new();
            for value in values {
                match value {
                    Some(AggScalarValue::Bool(v)) => builder.append_value(v),
                    None => builder.append_null(),
                    _ => return Err("scalar output type mismatch for Boolean".to_string()),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int8 => {
            let mut builder = Int8Builder::new();
            for value in values {
                match value {
                    Some(AggScalarValue::Int64(v)) => {
                        let v = i8::try_from(v).map_err(|_| "int8 overflow".to_string())?;
                        builder.append_value(v);
                    }
                    None => builder.append_null(),
                    _ => return Err("scalar output type mismatch for Int8".to_string()),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int16 => {
            let mut builder = Int16Builder::new();
            for value in values {
                match value {
                    Some(AggScalarValue::Int64(v)) => {
                        let v = i16::try_from(v).map_err(|_| "int16 overflow".to_string())?;
                        builder.append_value(v);
                    }
                    None => builder.append_null(),
                    _ => return Err("scalar output type mismatch for Int16".to_string()),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int32 => {
            let mut builder = Int32Builder::new();
            for value in values {
                match value {
                    Some(AggScalarValue::Int64(v)) => {
                        let v = i32::try_from(v).map_err(|_| "int32 overflow".to_string())?;
                        builder.append_value(v);
                    }
                    None => builder.append_null(),
                    _ => return Err("scalar output type mismatch for Int32".to_string()),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int64 => {
            let mut builder = Int64Builder::new();
            for value in values {
                match value {
                    Some(AggScalarValue::Int64(v)) => builder.append_value(v),
                    None => builder.append_null(),
                    _ => return Err("scalar output type mismatch for Int64".to_string()),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float32 => {
            let mut builder = Float32Builder::new();
            for value in values {
                match value {
                    Some(AggScalarValue::Float64(v)) => builder.append_value(v as f32),
                    None => builder.append_null(),
                    _ => return Err("scalar output type mismatch for Float32".to_string()),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float64 => {
            let mut builder = Float64Builder::new();
            for value in values {
                match value {
                    Some(AggScalarValue::Float64(v)) => builder.append_value(v),
                    None => builder.append_null(),
                    _ => return Err("scalar output type mismatch for Float64".to_string()),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Utf8 => {
            let mut builder = StringBuilder::new();
            for value in values {
                match value {
                    Some(AggScalarValue::Utf8(v)) => builder.append_value(v),
                    None => builder.append_null(),
                    _ => return Err("scalar output type mismatch for Utf8".to_string()),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Date32 => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                match value {
                    Some(AggScalarValue::Date32(v)) => out.push(Some(v)),
                    None => out.push(None),
                    _ => return Err("scalar output type mismatch for Date32".to_string()),
                }
            }
            Ok(Arc::new(Date32Array::from(out)))
        }
        DataType::Timestamp(unit, tz) => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                match value {
                    Some(AggScalarValue::Timestamp(v)) => out.push(Some(v)),
                    None => out.push(None),
                    _ => return Err("scalar output type mismatch for Timestamp".to_string()),
                }
            }
            let tz = tz.as_deref().map(|s| s.to_string());
            let array: ArrayRef = match unit {
                TimeUnit::Second => {
                    let array = TimestampSecondArray::from(out);
                    if let Some(tz) = tz {
                        Arc::new(array.with_timezone(tz))
                    } else {
                        Arc::new(array)
                    }
                }
                TimeUnit::Millisecond => {
                    let array = TimestampMillisecondArray::from(out);
                    if let Some(tz) = tz {
                        Arc::new(array.with_timezone(tz))
                    } else {
                        Arc::new(array)
                    }
                }
                TimeUnit::Microsecond => {
                    let array = TimestampMicrosecondArray::from(out);
                    if let Some(tz) = tz {
                        Arc::new(array.with_timezone(tz))
                    } else {
                        Arc::new(array)
                    }
                }
                TimeUnit::Nanosecond => {
                    let array = TimestampNanosecondArray::from(out);
                    if let Some(tz) = tz {
                        Arc::new(array.with_timezone(tz))
                    } else {
                        Arc::new(array)
                    }
                }
            };
            Ok(array)
        }
        DataType::Decimal128(precision, scale) => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                match value {
                    Some(AggScalarValue::Decimal128(v)) => out.push(Some(v)),
                    None => out.push(None),
                    _ => return Err("scalar output type mismatch for Decimal128".to_string()),
                }
            }
            let array = Decimal128Array::from(out)
                .with_precision_and_scale(*precision, *scale)
                .map_err(|e| e.to_string())?;
            Ok(Arc::new(array))
        }
        DataType::Decimal256(precision, scale) => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                match value {
                    Some(AggScalarValue::Decimal256(v)) => out.push(Some(v)),
                    None => out.push(None),
                    _ => return Err("scalar output type mismatch for Decimal256".to_string()),
                }
            }
            let array = Decimal256Array::from(out)
                .with_precision_and_scale(*precision, *scale)
                .map_err(|e| e.to_string())?;
            Ok(Arc::new(array))
        }
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                match value {
                    Some(AggScalarValue::Decimal128(v)) => out.push(Some(v)),
                    None => out.push(None),
                    _ => return Err("scalar output type mismatch for LargeInt".to_string()),
                }
            }
            largeint::array_from_i128(&out)
        }
        DataType::List(item) => {
            let mut flat_values = Vec::new();
            let mut offsets = Vec::with_capacity(values.len() + 1);
            offsets.push(0_i32);
            let mut current: i64 = 0;
            for value in values {
                match value {
                    Some(AggScalarValue::List(items)) => {
                        current += i64::try_from(items.len())
                            .map_err(|_| "list item length overflow".to_string())?;
                        if current > i32::MAX as i64 {
                            return Err("list offset overflow".to_string());
                        }
                        flat_values.extend(items);
                        offsets.push(current as i32);
                    }
                    None => offsets.push(current as i32),
                    _ => return Err("scalar output type mismatch for List".to_string()),
                }
            }
            let child = build_scalar_array(item.data_type(), flat_values)?;
            let out =
                ListArray::try_new(item.clone(), OffsetBuffer::new(offsets.into()), child, None)
                    .map_err(|e| format!("list output build failed: {}", e))?;
            Ok(Arc::new(out))
        }
        DataType::Struct(fields) => {
            let mut field_values: Vec<Vec<Option<AggScalarValue>>> =
                vec![Vec::with_capacity(values.len()); fields.len()];
            let mut nulls = NullBufferBuilder::new(values.len());
            for value in values {
                match value {
                    Some(AggScalarValue::Struct(items)) => {
                        if items.len() != fields.len() {
                            return Err(format!(
                                "scalar output struct field count mismatch: expected {} got {}",
                                fields.len(),
                                items.len()
                            ));
                        }
                        nulls.append_non_null();
                        for (idx, item) in items.into_iter().enumerate() {
                            field_values[idx].push(item);
                        }
                    }
                    None => {
                        nulls.append_null();
                        for values in field_values.iter_mut() {
                            values.push(None);
                        }
                    }
                    _ => return Err("scalar output type mismatch for Struct".to_string()),
                }
            }
            let mut columns = Vec::with_capacity(fields.len());
            for (field, values) in fields.iter().zip(field_values.into_iter()) {
                columns.push(build_scalar_array(field.data_type(), values)?);
            }
            Ok(Arc::new(StructArray::new(
                fields.clone(),
                columns,
                nulls.finish(),
            )))
        }
        DataType::Map(field, ordered) => {
            let DataType::Struct(entry_fields) = field.data_type() else {
                return Err("scalar output MAP entries type must be STRUCT".to_string());
            };
            if entry_fields.len() != 2 {
                return Err("scalar output MAP entries must have 2 fields".to_string());
            }
            let mut key_values = Vec::<Option<AggScalarValue>>::new();
            let mut value_values = Vec::<Option<AggScalarValue>>::new();
            let mut offsets = Vec::with_capacity(values.len() + 1);
            offsets.push(0_i32);
            let mut current: i64 = 0;
            let mut nulls = NullBufferBuilder::new(values.len());

            for value in values {
                match value {
                    Some(AggScalarValue::Map(items)) => {
                        nulls.append_non_null();
                        for (k, v) in items {
                            key_values.push(k);
                            value_values.push(v);
                            current += 1;
                            if current > i32::MAX as i64 {
                                return Err("map offset overflow".to_string());
                            }
                        }
                        offsets.push(current as i32);
                    }
                    None => {
                        nulls.append_null();
                        offsets.push(current as i32);
                    }
                    _ => return Err("scalar output type mismatch for Map".to_string()),
                }
            }

            let keys = build_scalar_array(entry_fields[0].data_type(), key_values)?;
            let values = build_scalar_array(entry_fields[1].data_type(), value_values)?;
            let entries = StructArray::new(entry_fields.clone(), vec![keys, values], None);
            let out = MapArray::try_new(
                field.clone(),
                OffsetBuffer::new(offsets.into()),
                entries,
                nulls.finish(),
                *ordered,
            )
            .map_err(|e| format!("map output build failed: {}", e))?;
            Ok(Arc::new(out))
        }
        other => Err(format!("unsupported scalar output type: {:?}", other)),
    }
}
