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
use crate::common::largeint;
use crate::exec::chunk::Chunk;
use crate::exec::expr::decimal::pow10_i256;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBuilder, Date32Array, Date32Builder, Decimal128Array,
    Decimal256Array, Float32Array, Float32Builder, Float64Array, Float64Builder, Int8Array,
    Int8Builder, Int16Array, Int16Builder, Int32Array, Int32Builder, Int64Array, Int64Builder,
    LargeBinaryArray, ListArray, MapArray, StringArray, StringBuilder, StructArray,
    TimestampMicrosecondArray, TimestampMicrosecondBuilder, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array, make_array, new_null_array,
};
use arrow::compute::{cast, take};
use arrow::datatypes::{DataType, Field, Fields, TimeUnit};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer, i256};
use chrono::{DateTime, Datelike, Local, NaiveDate, NaiveDateTime, NaiveTime, Offset, Timelike};
use serde_json::Value as JsonValue;
use std::sync::Arc;

use crate::exec::variant::{
    VariantValue, is_variant_null, variant_to_bool, variant_to_date_days,
    variant_to_datetime_micros, variant_to_f64, variant_to_i64, variant_to_string,
    variant_to_time_micros,
};
const UNIX_EPOCH_DAY_OFFSET: i32 = 719163;
const FIELD_META_PRIMITIVE_TYPE: &str = "novarocks.primitive_type";
const FIELD_META_PRIMITIVE_JSON: &str = "JSON";

fn date32_to_date_literal(days: i32) -> Result<i32, String> {
    let date = NaiveDate::from_num_days_from_ce_opt(UNIX_EPOCH_DAY_OFFSET + days)
        .ok_or_else(|| format!("invalid Date32 value {days}"))?;
    Ok(date.year() * 10000 + date.month() as i32 * 100 + date.day() as i32)
}

fn standardize_date_literal(value: i64) -> Option<i64> {
    const YY_PART_YEAR: i64 = 70;
    if value <= 0 {
        return None;
    }
    if value >= 10000101000000 {
        if value > 99999999999999 {
            return None;
        }
        return Some(value);
    }
    if value < 101 {
        return None;
    }
    if value <= (YY_PART_YEAR - 1) * 10000 + 1231 {
        return Some((value + 20000000) * 1000000);
    }
    if value < YY_PART_YEAR * 10000 + 101 {
        return None;
    }
    if value <= 991231 {
        return Some((value + 19000000) * 1000000);
    }
    if value < 10000101 {
        return None;
    }
    if value <= 99991231 {
        return Some(value * 1000000);
    }
    if value < 101000000 {
        return None;
    }
    if value <= (YY_PART_YEAR - 1) * 10000000000 + 1231235959 {
        return Some(value + 20000000000000);
    }
    if value < YY_PART_YEAR * 10000000000 + 101000000 {
        return None;
    }
    if value <= 991231235959 {
        return Some(value + 19000000000000);
    }
    Some(value)
}

fn date_literal_to_date32(value: i64) -> Result<i32, String> {
    let standardized =
        standardize_date_literal(value).ok_or_else(|| format!("invalid date literal {value}"))?;
    let date_part = standardized / 1_000_000;
    let time_part = standardized % 1_000_000;
    let year = (date_part / 10000) as i32;
    let month = ((date_part / 100) % 100) as u32;
    let day = (date_part % 100) as u32;
    let hour = (time_part / 10000) as i32;
    let minute = ((time_part / 100) % 100) as i32;
    let second = (time_part % 100) as i32;
    if hour > 23 || minute > 59 || second > 59 {
        return Err(format!("invalid date literal {value}"));
    }
    let date = NaiveDate::from_ymd_opt(year, month, day)
        .ok_or_else(|| format!("invalid date literal {value}"))?;
    Ok(date.num_days_from_ce() - UNIX_EPOCH_DAY_OFFSET)
}

fn pow10_i128(scale: u32) -> Option<i128> {
    let mut out: i128 = 1;
    for _ in 0..scale {
        out = out.checked_mul(10)?;
    }
    Some(out)
}

fn decimal128_to_i64_literal(value: i128, scale: i8) -> Option<i64> {
    let integral = if scale >= 0 {
        let divisor = pow10_i128(scale as u32)?;
        value / divisor
    } else {
        let factor = pow10_i128((-scale) as u32)?;
        value.checked_mul(factor)?
    };
    i64::try_from(integral).ok()
}

fn decimal128_to_i128_literal(value: i128, scale: i8) -> Option<i128> {
    if scale >= 0 {
        let divisor = pow10_i128(scale as u32)?;
        Some(value / divisor)
    } else {
        let factor = pow10_i128((-scale) as u32)?;
        value.checked_mul(factor)
    }
}

fn decimal256_to_i128_literal(value: i256, scale: i8) -> Option<i128> {
    let integral = if scale >= 0 {
        let divisor = pow10_i256(scale as usize).ok()?;
        value.checked_div(divisor)?
    } else {
        let factor = pow10_i256((-scale) as usize).ok()?;
        value.checked_mul(factor)?
    };
    integral.to_i128()
}

fn format_decimal_with_scale(unscaled: i128, scale: i8) -> String {
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
        let int_part = &abs[..split];
        let frac_part = &abs[split..];
        if unscaled < 0 {
            format!("-{}.{}", int_part, frac_part)
        } else {
            format!("{}.{}", int_part, frac_part)
        }
    }
}

fn format_decimal256_with_scale(unscaled: i256, scale: i8) -> String {
    if scale <= 0 {
        return unscaled.to_string();
    }
    let scale = scale as usize;
    let negative = unscaled.is_negative();
    let abs = if negative {
        unscaled.checked_neg().unwrap_or(unscaled)
    } else {
        unscaled
    };
    let abs_str = abs.to_string();
    if abs_str.len() <= scale {
        let frac = format!("{:0>width$}", abs_str, width = scale);
        if negative {
            format!("-0.{}", frac)
        } else {
            format!("0.{}", frac)
        }
    } else {
        let split = abs_str.len() - scale;
        let int_part = &abs_str[..split];
        let frac_part = &abs_str[split..];
        if negative {
            format!("-{}.{}", int_part, frac_part)
        } else {
            format!("{}.{}", int_part, frac_part)
        }
    }
}

fn numeric_largeint_literal_at(array: &ArrayRef, row: usize) -> Result<Option<i128>, String> {
    if array.is_null(row) {
        return Ok(None);
    }
    match array.data_type() {
        DataType::Boolean => {
            let arr = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| "failed to downcast to BooleanArray".to_string())?;
            Ok(Some(if arr.value(row) { 1 } else { 0 }))
        }
        DataType::Int8 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| "failed to downcast to Int8Array".to_string())?;
            Ok(Some(arr.value(row) as i128))
        }
        DataType::Int16 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| "failed to downcast to Int16Array".to_string())?;
            Ok(Some(arr.value(row) as i128))
        }
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "failed to downcast to Int32Array".to_string())?;
            Ok(Some(arr.value(row) as i128))
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "failed to downcast to Int64Array".to_string())?;
            Ok(Some(arr.value(row) as i128))
        }
        DataType::UInt8 => {
            let arr = array
                .as_any()
                .downcast_ref::<UInt8Array>()
                .ok_or_else(|| "failed to downcast to UInt8Array".to_string())?;
            Ok(Some(arr.value(row) as i128))
        }
        DataType::UInt16 => {
            let arr = array
                .as_any()
                .downcast_ref::<UInt16Array>()
                .ok_or_else(|| "failed to downcast to UInt16Array".to_string())?;
            Ok(Some(arr.value(row) as i128))
        }
        DataType::UInt32 => {
            let arr = array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| "failed to downcast to UInt32Array".to_string())?;
            Ok(Some(arr.value(row) as i128))
        }
        DataType::UInt64 => {
            let arr = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| "failed to downcast to UInt64Array".to_string())?;
            Ok(Some(arr.value(row) as i128))
        }
        DataType::Float32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| "failed to downcast to Float32Array".to_string())?;
            let value = arr.value(row) as f64;
            if !value.is_finite() || value < i128::MIN as f64 || value > i128::MAX as f64 {
                Ok(None)
            } else {
                Ok(Some(value.trunc() as i128))
            }
        }
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| "failed to downcast to Float64Array".to_string())?;
            let value = arr.value(row);
            if !value.is_finite() || value < i128::MIN as f64 || value > i128::MAX as f64 {
                Ok(None)
            } else {
                Ok(Some(value.trunc() as i128))
            }
        }
        DataType::Decimal128(_, scale) => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| "failed to downcast to Decimal128Array".to_string())?;
            Ok(decimal128_to_i128_literal(arr.value(row), *scale))
        }
        other => Err(format!(
            "unsupported numeric LARGEINT source type: {:?}",
            other
        )),
    }
}

fn cast_numeric_to_largeint_binary_array(array: &ArrayRef) -> Result<ArrayRef, String> {
    let mut values = Vec::with_capacity(array.len());
    for row in 0..array.len() {
        values.push(numeric_largeint_literal_at(array, row)?);
    }
    largeint::array_from_i128(&values)
}

fn datetime_literal_to_naive_datetime(value: i64) -> Option<NaiveDateTime> {
    let standardized = standardize_date_literal(value)?;
    let date_part = standardized / 1_000_000;
    let time_part = standardized % 1_000_000;

    let year = (date_part / 10_000) as i32;
    let month = ((date_part / 100) % 100) as u32;
    let day = (date_part % 100) as u32;
    let hour = (time_part / 10_000) as u32;
    let minute = ((time_part / 100) % 100) as u32;
    let second = (time_part % 100) as u32;

    let date = NaiveDate::from_ymd_opt(year, month, day)?;
    let time = NaiveTime::from_hms_opt(hour, minute, second)?;
    Some(date.and_time(time))
}

fn is_numeric_datetime_source(ty: &DataType) -> bool {
    matches!(
        ty,
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

fn numeric_datetime_literal_at(array: &ArrayRef, row: usize) -> Result<Option<i64>, String> {
    if array.is_null(row) {
        return Ok(None);
    }
    match array.data_type() {
        DataType::Boolean => {
            let arr = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| "failed to downcast to BooleanArray".to_string())?;
            Ok(Some(if arr.value(row) { 1 } else { 0 }))
        }
        DataType::Int8 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| "failed to downcast to Int8Array".to_string())?;
            Ok(Some(arr.value(row) as i64))
        }
        DataType::Int16 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| "failed to downcast to Int16Array".to_string())?;
            Ok(Some(arr.value(row) as i64))
        }
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "failed to downcast to Int32Array".to_string())?;
            Ok(Some(arr.value(row) as i64))
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "failed to downcast to Int64Array".to_string())?;
            Ok(Some(arr.value(row)))
        }
        DataType::UInt8 => {
            let arr = array
                .as_any()
                .downcast_ref::<UInt8Array>()
                .ok_or_else(|| "failed to downcast to UInt8Array".to_string())?;
            Ok(Some(arr.value(row) as i64))
        }
        DataType::UInt16 => {
            let arr = array
                .as_any()
                .downcast_ref::<UInt16Array>()
                .ok_or_else(|| "failed to downcast to UInt16Array".to_string())?;
            Ok(Some(arr.value(row) as i64))
        }
        DataType::UInt32 => {
            let arr = array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| "failed to downcast to UInt32Array".to_string())?;
            Ok(Some(arr.value(row) as i64))
        }
        DataType::UInt64 => {
            let arr = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| "failed to downcast to UInt64Array".to_string())?;
            let value = arr.value(row);
            Ok((value <= i64::MAX as u64).then_some(value as i64))
        }
        DataType::Float32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| "failed to downcast to Float32Array".to_string())?;
            let value = arr.value(row) as f64;
            if !value.is_finite() || value < i64::MIN as f64 || value > i64::MAX as f64 {
                Ok(None)
            } else {
                Ok(Some(value.trunc() as i64))
            }
        }
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| "failed to downcast to Float64Array".to_string())?;
            let value = arr.value(row);
            if !value.is_finite() || value < i64::MIN as f64 || value > i64::MAX as f64 {
                Ok(None)
            } else {
                Ok(Some(value.trunc() as i64))
            }
        }
        DataType::Decimal128(_, scale) => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| "failed to downcast to Decimal128Array".to_string())?;
            Ok(decimal128_to_i64_literal(arr.value(row), *scale))
        }
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            let arr =
                largeint::as_fixed_size_binary_array(array, "cast LARGEINT to DATETIME source")?;
            let value = largeint::value_at(arr, row)?;
            Ok(i64::try_from(value).ok())
        }
        other => Err(format!(
            "unsupported numeric datetime source type: {:?}",
            other
        )),
    }
}

fn cast_numeric_to_date32_array(array: &ArrayRef) -> Result<ArrayRef, String> {
    let mut builder = Date32Builder::new();
    for row in 0..array.len() {
        let value = numeric_datetime_literal_at(array, row)?;
        let days = value
            .and_then(datetime_literal_to_naive_datetime)
            .map(|dt| dt.date().num_days_from_ce() - UNIX_EPOCH_DAY_OFFSET);
        match days {
            Some(v) => builder.append_value(v),
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn cast_numeric_to_timestamp_array(
    array: &ArrayRef,
    target_type: &DataType,
) -> Result<ArrayRef, String> {
    let mut micros = Vec::with_capacity(array.len());
    for row in 0..array.len() {
        let value = numeric_datetime_literal_at(array, row)?;
        let micros_value = value
            .and_then(datetime_literal_to_naive_datetime)
            .map(|dt| dt.and_utc().timestamp_micros());
        micros.push(micros_value);
    }
    let micro_array = Arc::new(TimestampMicrosecondArray::from(micros)) as ArrayRef;
    if micro_array.data_type() == target_type {
        return Ok(micro_array);
    }
    cast(micro_array.as_ref(), target_type).map_err(|e| {
        format!(
            "CAST failed: from {:?} to {:?}: {}",
            micro_array.data_type(),
            target_type,
            e
        )
    })
}

fn cast_date32_to_float64(arr: &Date32Array) -> Result<ArrayRef, String> {
    let mut builder = Float64Builder::new();
    for i in 0..arr.len() {
        if arr.is_null(i) {
            builder.append_null();
            continue;
        }
        let literal = date32_to_date_literal(arr.value(i))?;
        builder.append_value(literal as f64);
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn cast_date32_to_float32(arr: &Date32Array) -> Result<ArrayRef, String> {
    let mut builder = Float32Builder::new();
    for i in 0..arr.len() {
        if arr.is_null(i) {
            builder.append_null();
            continue;
        }
        let literal = date32_to_date_literal(arr.value(i))?;
        builder.append_value(literal as f32);
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn cast_float64_to_date32(arr: &Float64Array) -> Result<ArrayRef, String> {
    let mut builder = Date32Builder::new();
    for i in 0..arr.len() {
        if arr.is_null(i) {
            builder.append_null();
            continue;
        }
        let value = arr.value(i);
        if !value.is_finite() {
            return Err(format!("invalid date literal {value}"));
        }
        let literal = value as i64;
        let days = date_literal_to_date32(literal)?;
        builder.append_value(days);
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn cast_float32_to_date32(arr: &Float32Array) -> Result<ArrayRef, String> {
    let mut builder = Date32Builder::new();
    for i in 0..arr.len() {
        if arr.is_null(i) {
            builder.append_null();
            continue;
        }
        let value = arr.value(i) as f64;
        if !value.is_finite() {
            return Err(format!("invalid date literal {value}"));
        }
        let literal = value as i64;
        let days = date_literal_to_date32(literal)?;
        builder.append_value(days);
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn sanitize_non_finite_float64(arr: &Float64Array) -> ArrayRef {
    let mut builder = Float64Builder::new();
    for i in 0..arr.len() {
        if arr.is_null(i) {
            builder.append_null();
            continue;
        }
        let value = arr.value(i);
        if value.is_nan() {
            builder.append_null();
        } else {
            builder.append_value(value);
        }
    }
    Arc::new(builder.finish()) as ArrayRef
}

fn sanitize_non_finite_float32(arr: &Float32Array) -> ArrayRef {
    let mut builder = Float32Builder::new();
    for i in 0..arr.len() {
        if arr.is_null(i) {
            builder.append_null();
            continue;
        }
        let value = arr.value(i);
        if value.is_nan() {
            builder.append_null();
        } else {
            builder.append_value(value);
        }
    }
    Arc::new(builder.finish()) as ArrayRef
}

fn cast_float64_to_float32_preserve_non_finite(arr: &Float64Array) -> ArrayRef {
    let mut builder = Float32Builder::new();
    for i in 0..arr.len() {
        if arr.is_null(i) {
            builder.append_null();
        } else {
            builder.append_value(arr.value(i) as f32);
        }
    }
    Arc::new(builder.finish()) as ArrayRef
}

fn cast_float32_to_float64_preserve_non_finite(arr: &Float32Array) -> ArrayRef {
    let mut builder = Float64Builder::new();
    for i in 0..arr.len() {
        if arr.is_null(i) {
            builder.append_null();
        } else {
            builder.append_value(arr.value(i) as f64);
        }
    }
    Arc::new(builder.finish()) as ArrayRef
}

fn sanitize_non_finite_cast_result(
    array: ArrayRef,
    target_type: &DataType,
) -> Result<ArrayRef, String> {
    match target_type {
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| "failed to downcast to Float64Array".to_string())?;
            Ok(sanitize_non_finite_float64(arr))
        }
        DataType::Float32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| "failed to downcast to Float32Array".to_string())?;
            Ok(sanitize_non_finite_float32(arr))
        }
        _ => Ok(array),
    }
}

fn is_integral_target_type(target_type: &DataType) -> bool {
    matches!(
        target_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
    )
}

fn integral_target_name(target_type: &DataType) -> &'static str {
    match target_type {
        DataType::Int8 => "TINYINT",
        DataType::Int16 => "SMALLINT",
        DataType::Int32 => "INT",
        DataType::Int64 => "BIGINT",
        DataType::UInt8 => "TINYINT UNSIGNED",
        DataType::UInt16 => "SMALLINT UNSIGNED",
        DataType::UInt32 => "INT UNSIGNED",
        DataType::UInt64 => "BIGINT UNSIGNED",
        _ => "INTEGER",
    }
}

fn first_float_to_int_overflow_value(
    source: &ArrayRef,
    casted: &ArrayRef,
    target_type: &DataType,
) -> Result<Option<f64>, String> {
    if source.len() != casted.len() || !is_integral_target_type(target_type) {
        return Ok(None);
    }

    match source.data_type() {
        DataType::Float32 => {
            let arr = source
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| "failed to downcast to Float32Array".to_string())?;
            for i in 0..arr.len() {
                if !arr.is_null(i) && casted.is_null(i) {
                    return Ok(Some(arr.value(i) as f64));
                }
            }
            Ok(None)
        }
        DataType::Float64 => {
            let arr = source
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| "failed to downcast to Float64Array".to_string())?;
            for i in 0..arr.len() {
                if !arr.is_null(i) && casted.is_null(i) {
                    return Ok(Some(arr.value(i)));
                }
            }
            Ok(None)
        }
        _ => Ok(None),
    }
}

fn has_decimal_to_decimal_overflow(source: &ArrayRef, casted: &ArrayRef) -> bool {
    if source.len() != casted.len() {
        return false;
    }
    let source_is_decimal = matches!(
        source.data_type(),
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _)
    );
    let target_is_decimal = matches!(
        casted.data_type(),
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _)
    );
    if !source_is_decimal || !target_is_decimal {
        return false;
    }
    for row in 0..source.len() {
        if !source.is_null(row) && casted.is_null(row) {
            return true;
        }
    }
    false
}

fn parse_time_string_to_seconds(raw: &str) -> Option<i64> {
    let raw = raw.trim();
    if raw.is_empty() || raw.contains('+') || raw.starts_with('-') {
        return None;
    }
    let mut parts = raw.split(':');
    let hour = parts.next()?.trim().parse::<i64>().ok()?;
    let minute = parts.next()?.trim().parse::<i64>().ok()?;
    let second = parts.next()?.trim().parse::<i64>().ok()?;
    if parts.next().is_some()
        || hour < 0
        || minute < 0
        || second < 0
        || minute >= 60
        || second >= 60
    {
        return None;
    }
    hour.checked_mul(3600)?
        .checked_add(minute.checked_mul(60)?)?
        .checked_add(second)
}

fn parse_datetime_string_to_seconds(raw: &str) -> Option<i64> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }
    let dt = chrono::NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S").ok()?;
    let t = dt.time();
    Some((t.hour() as i64) * 3600 + (t.minute() as i64) * 60 + t.second() as i64)
}

fn parse_time_integer_to_seconds(value: i64) -> Option<i64> {
    if value < 0 {
        return None;
    }
    if value < 100 {
        return Some(value);
    }
    let hour = value / 10000;
    let minute = (value / 100) % 100;
    let second = value % 100;
    if minute >= 60 || second >= 60 {
        return None;
    }
    hour.checked_mul(3600)?
        .checked_add(minute.checked_mul(60)?)?
        .checked_add(second)
}

fn parse_time_float_to_seconds(value: f64) -> Option<i64> {
    if !value.is_finite() {
        return None;
    }
    parse_time_integer_to_seconds(value.trunc() as i64)
}

fn seconds_from_timestamp(unit: &TimeUnit, value: i64) -> Option<i64> {
    let micros = match unit {
        TimeUnit::Second => value.checked_mul(1_000_000)?,
        TimeUnit::Millisecond => value.checked_mul(1_000)?,
        TimeUnit::Microsecond => value,
        TimeUnit::Nanosecond => value / 1_000,
    };
    let seconds = micros.div_euclid(1_000_000);
    let sub_micros = micros.rem_euclid(1_000_000) as u32;
    let dt = DateTime::from_timestamp(seconds, sub_micros * 1000)?;
    let t = dt.naive_utc().time();
    Some((t.hour() as i64) * 3600 + (t.minute() as i64) * 60 + t.second() as i64)
}

fn seconds_to_target_timestamp_array(
    seconds: Vec<Option<i64>>,
    target_type: &DataType,
) -> Result<ArrayRef, String> {
    let mut micros = Vec::with_capacity(seconds.len());
    for value in seconds {
        let v = value.and_then(|s| s.checked_mul(1_000_000));
        micros.push(v);
    }
    let array = Arc::new(TimestampMicrosecondArray::from(micros)) as ArrayRef;
    if matches!(target_type, DataType::Timestamp(TimeUnit::Microsecond, _)) {
        return Ok(array);
    }
    cast(&array, target_type).map_err(|e| {
        format!(
            "CAST failed: from {:?} to {:?}: {}",
            array.data_type(),
            target_type,
            e
        )
    })
}

fn parse_varchar_to_boolean_starrocks(value: &str) -> Option<bool> {
    // StarRocks BE first parses VARCHAR as int32; when that succeeds, non-zero is true.
    // If integer parsing fails, it falls back to strict boolean text parsing.
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Ok(v) = trimmed.parse::<i32>() {
        return Some(v != 0);
    }
    if trimmed.eq_ignore_ascii_case("true") {
        return Some(true);
    }
    if trimmed.eq_ignore_ascii_case("false") {
        return Some(false);
    }
    None
}

fn cast_utf8_to_boolean_array(arr: &StringArray) -> ArrayRef {
    let mut builder = BooleanBuilder::new();
    for i in 0..arr.len() {
        if arr.is_null(i) {
            builder.append_null();
            continue;
        }
        match parse_varchar_to_boolean_starrocks(arr.value(i)) {
            Some(v) => builder.append_value(v),
            None => builder.append_null(),
        }
    }
    Arc::new(builder.finish()) as ArrayRef
}

fn format_float64_for_varchar(value: f64) -> String {
    if value == 0.0 {
        return "0".to_string();
    }
    if value.is_nan() {
        return "nan".to_string();
    }
    if value.is_infinite() {
        return if value.is_sign_negative() {
            "-inf".to_string()
        } else {
            "inf".to_string()
        };
    }
    let mut buf = ryu::Buffer::new();
    let formatted = buf.format(value);
    normalize_float_string_for_varchar(formatted)
}

fn format_float32_for_varchar(value: f32) -> String {
    if value == 0.0 {
        return "0".to_string();
    }
    if value.is_nan() {
        return "nan".to_string();
    }
    if value.is_infinite() {
        return if value.is_sign_negative() {
            "-inf".to_string()
        } else {
            "inf".to_string()
        };
    }
    let mut buf = ryu::Buffer::new();
    let formatted = buf.format(value);
    normalize_float_string_for_varchar(formatted)
}

fn normalize_float_string_for_varchar(formatted: &str) -> String {
    let stripped = formatted.strip_suffix(".0").unwrap_or(formatted);
    if let Some(exp_pos) = stripped.find('e') {
        let mut out = String::with_capacity(stripped.len() + 1);
        out.push_str(&stripped[..=exp_pos]);
        if let Some(sign_or_digit) = stripped.as_bytes().get(exp_pos + 1) {
            if *sign_or_digit == b'+' || *sign_or_digit == b'-' {
                out.push_str(&stripped[exp_pos + 1..]);
            } else {
                out.push('+');
                out.push_str(&stripped[exp_pos + 1..]);
            }
        }
        out
    } else {
        stripped.to_string()
    }
}

fn cast_float64_to_utf8_array(arr: &Float64Array) -> ArrayRef {
    let mut builder = StringBuilder::new();
    for i in 0..arr.len() {
        if arr.is_null(i) {
            builder.append_null();
            continue;
        }
        builder.append_value(format_float64_for_varchar(arr.value(i)));
    }
    Arc::new(builder.finish()) as ArrayRef
}

fn cast_float32_to_utf8_array(arr: &Float32Array) -> ArrayRef {
    let mut builder = StringBuilder::new();
    for i in 0..arr.len() {
        if arr.is_null(i) {
            builder.append_null();
            continue;
        }
        builder.append_value(format_float32_for_varchar(arr.value(i)));
    }
    Arc::new(builder.finish()) as ArrayRef
}

fn decimal_precision_limit(precision: u8) -> Option<i128> {
    if precision == 0 {
        return Some(1);
    }
    pow10_i128(precision as u32)
}

fn decimal_value_within_precision(value: i128, precision: u8) -> bool {
    let Some(limit) = decimal_precision_limit(precision) else {
        return false;
    };
    value > -limit && value < limit
}

fn decimal256_precision_limit(precision: u8) -> Option<i256> {
    if precision == 0 {
        return Some(i256::ONE);
    }
    pow10_i256(precision as usize).ok()
}

fn decimal256_value_within_precision(value: i256, precision: u8) -> bool {
    let Some(limit) = decimal256_precision_limit(precision) else {
        return false;
    };
    let Some(neg_limit) = limit.checked_neg() else {
        return false;
    };
    value > neg_limit && value < limit
}

fn cast_float_to_decimal_with_rounding(
    len: usize,
    mut value_at: impl FnMut(usize) -> Option<f64>,
    precision: u8,
    scale: i8,
) -> Result<ArrayRef, String> {
    let scale_factor_f64 = if scale >= 0 {
        let factor = pow10_i128(scale as u32).ok_or_else(|| {
            format!(
                "decimal scale overflow while casting float to DECIMAL: scale={}",
                scale
            )
        })?;
        factor as f64
    } else {
        let factor = pow10_i128((-scale) as u32).ok_or_else(|| {
            format!(
                "decimal scale overflow while casting float to DECIMAL: scale={}",
                scale
            )
        })?;
        1.0 / (factor as f64)
    };
    let effective_precision = if precision <= 18 { 18 } else { precision };
    let abs_limit = decimal_precision_limit(effective_precision).ok_or_else(|| {
        format!(
            "decimal precision overflow while casting float to DECIMAL: precision={}",
            effective_precision
        )
    })?;

    let mut values: Vec<Option<i128>> = Vec::with_capacity(len);
    for row in 0..len {
        let Some(v) = value_at(row) else {
            values.push(None);
            continue;
        };
        if !v.is_finite() {
            values.push(None);
            continue;
        }

        // Match StarRocks DecimalV3Cast::from_float: nearest integer with half-up behavior.
        let delta = if v >= 0.0 { 0.5 } else { -0.5 };
        let scaled = v * scale_factor_f64 + delta;
        if !scaled.is_finite() {
            values.push(None);
            continue;
        }

        let unscaled_f = scaled.trunc();
        if unscaled_f > (i128::MAX as f64) || unscaled_f < (i128::MIN as f64) {
            values.push(None);
            continue;
        }
        let unscaled = unscaled_f as i128;
        if unscaled.abs() >= abs_limit {
            values.push(None);
            continue;
        }
        values.push(Some(unscaled));
    }

    let wide = Decimal128Array::from(values)
        .with_precision_and_scale(38, scale)
        .map_err(|e| e.to_string())?;
    retag_decimal_array(&wide, precision, scale)
}

fn format_timestamp_for_varchar(unit: &TimeUnit, value: i64, tz: Option<&str>) -> String {
    let timestamp_str = match unit {
        TimeUnit::Second => {
            let dt = DateTime::from_timestamp(value, 0)
                .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());
            dt.naive_utc().format("%Y-%m-%d %H:%M:%S").to_string()
        }
        TimeUnit::Millisecond => {
            let seconds = value.div_euclid(1_000);
            let millis = value.rem_euclid(1_000) as u32;
            let dt = DateTime::from_timestamp(seconds, millis * 1_000_000)
                .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());
            if millis == 0 {
                dt.naive_utc().format("%Y-%m-%d %H:%M:%S").to_string()
            } else {
                dt.naive_utc().format("%Y-%m-%d %H:%M:%S%.3f").to_string()
            }
        }
        TimeUnit::Microsecond => {
            let seconds = value.div_euclid(1_000_000);
            let micros = value.rem_euclid(1_000_000) as u32;
            let dt = DateTime::from_timestamp(seconds, micros * 1_000)
                .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());
            if micros == 0 {
                dt.naive_utc().format("%Y-%m-%d %H:%M:%S").to_string()
            } else {
                dt.naive_utc().format("%Y-%m-%d %H:%M:%S%.6f").to_string()
            }
        }
        TimeUnit::Nanosecond => {
            let seconds = value.div_euclid(1_000_000_000);
            let nanos = value.rem_euclid(1_000_000_000) as u32;
            let dt = DateTime::from_timestamp(seconds, nanos)
                .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());
            if nanos == 0 {
                dt.naive_utc().format("%Y-%m-%d %H:%M:%S").to_string()
            } else {
                dt.naive_utc().format("%Y-%m-%d %H:%M:%S%.9f").to_string()
            }
        }
    };
    if let Some(tz) = tz {
        format!("{timestamp_str} {tz}")
    } else {
        timestamp_str
    }
}

fn cast_timestamp_to_utf8_array(
    array: &ArrayRef,
    unit: &TimeUnit,
    tz: Option<&str>,
) -> Result<ArrayRef, String> {
    let mut builder = StringBuilder::new();
    match unit {
        TimeUnit::Second => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .ok_or_else(|| "failed to downcast to TimestampSecondArray".to_string())?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    builder.append_null();
                } else {
                    builder.append_value(format_timestamp_for_varchar(unit, arr.value(i), tz));
                }
            }
        }
        TimeUnit::Millisecond => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| "failed to downcast to TimestampMillisecondArray".to_string())?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    builder.append_null();
                } else {
                    builder.append_value(format_timestamp_for_varchar(unit, arr.value(i), tz));
                }
            }
        }
        TimeUnit::Microsecond => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| "failed to downcast to TimestampMicrosecondArray".to_string())?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    builder.append_null();
                } else {
                    builder.append_value(format_timestamp_for_varchar(unit, arr.value(i), tz));
                }
            }
        }
        TimeUnit::Nanosecond => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| "failed to downcast to TimestampNanosecondArray".to_string())?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    builder.append_null();
                } else {
                    builder.append_value(format_timestamp_for_varchar(unit, arr.value(i), tz));
                }
            }
        }
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn field_has_json_semantics(field: Option<&Field>) -> bool {
    let Some(field) = field else {
        return false;
    };
    field
        .metadata()
        .get(FIELD_META_PRIMITIVE_TYPE)
        .is_some_and(|v| v == FIELD_META_PRIMITIVE_JSON)
}

fn format_json_value_for_cast(value: &JsonValue, out: &mut String) -> Result<(), String> {
    match value {
        JsonValue::Null => out.push_str("null"),
        JsonValue::Bool(v) => out.push_str(if *v { "true" } else { "false" }),
        JsonValue::Number(v) => out.push_str(&v.to_string()),
        JsonValue::String(v) => {
            let escaped = serde_json::to_string(v)
                .map_err(|e| format!("json stringify failed while cast: {e}"))?;
            out.push_str(&escaped);
        }
        JsonValue::Array(items) => {
            out.push('[');
            for (idx, item) in items.iter().enumerate() {
                if idx > 0 {
                    out.push_str(", ");
                }
                format_json_value_for_cast(item, out)?;
            }
            out.push(']');
        }
        JsonValue::Object(map) => {
            out.push('{');
            let mut keys = map.keys().collect::<Vec<_>>();
            keys.sort_unstable();
            for (idx, key) in keys.iter().enumerate() {
                if idx > 0 {
                    out.push_str(", ");
                }
                let escaped_key = serde_json::to_string(key)
                    .map_err(|e| format!("json stringify key failed while cast: {e}"))?;
                out.push_str(&escaped_key);
                out.push_str(": ");
                let child = map
                    .get(*key)
                    .ok_or_else(|| "json object key missing while cast".to_string())?;
                format_json_value_for_cast(child, out)?;
            }
            out.push('}');
        }
    }
    Ok(())
}

fn json_value_to_normalized_text(value: &JsonValue) -> Result<String, String> {
    let mut out = String::new();
    format_json_value_for_cast(value, &mut out)?;
    Ok(out)
}

fn json_value_to_varchar(value: &JsonValue) -> Result<Option<String>, String> {
    match value {
        JsonValue::Null => Ok(None),
        JsonValue::String(v) => Ok(Some(v.clone())),
        _ => Ok(Some(json_value_to_normalized_text(value)?)),
    }
}

fn parse_utf8_json_rows(arr: &StringArray) -> Vec<Option<JsonValue>> {
    let mut out = Vec::with_capacity(arr.len());
    for row in 0..arr.len() {
        if arr.is_null(row) {
            out.push(None);
            continue;
        }
        out.push(serde_json::from_str::<JsonValue>(arr.value(row)).ok());
    }
    out
}

fn cast_optional_utf8_to_target(
    values: Vec<Option<String>>,
    target_type: &DataType,
) -> Result<ArrayRef, String> {
    let source = Arc::new(StringArray::from(values)) as ArrayRef;
    if matches!(target_type, DataType::Utf8) {
        return Ok(source);
    }
    cast_with_special_rules(&source, target_type)
}

fn is_integral_or_largeint_target_type(target_type: &DataType) -> bool {
    is_integral_target_type(target_type)
        || matches!(
            target_type,
            DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH
        )
}

fn json_number_to_integral_string(value: &serde_json::Number) -> Option<String> {
    if let Some(v) = value.as_i64() {
        return Some(v.to_string());
    }
    if let Some(v) = value.as_u64() {
        return Some(v.to_string());
    }
    let v = value.as_f64()?;
    if !v.is_finite() || v > i128::MAX as f64 || v < i128::MIN as f64 {
        return None;
    }
    Some((v.trunc() as i128).to_string())
}

fn cast_json_values_to_list(
    values: &[Option<JsonValue>],
    target_field: Arc<Field>,
) -> Result<ArrayRef, String> {
    let mut flat_values = Vec::<Option<JsonValue>>::new();
    let mut offsets = Vec::with_capacity(values.len() + 1);
    offsets.push(0_i32);
    let mut current: i64 = 0;
    let mut null_builder = NullBufferBuilder::new(values.len());

    for value in values {
        match value {
            Some(JsonValue::Array(items)) => {
                current = current
                    .checked_add(items.len() as i64)
                    .ok_or_else(|| "CAST list offset overflow".to_string())?;
                if current > i32::MAX as i64 {
                    return Err("CAST list offset overflow".to_string());
                }
                for item in items {
                    flat_values.push(Some(item.clone()));
                }
                null_builder.append_non_null();
                offsets.push(current as i32);
            }
            _ => {
                null_builder.append_null();
                offsets.push(current as i32);
            }
        }
    }

    let cast_values = cast_json_values_to_target(
        &flat_values,
        target_field.data_type(),
        Some(target_field.as_ref()),
    )?;
    let out = ListArray::new(
        target_field,
        OffsetBuffer::new(offsets.into()),
        cast_values,
        null_builder.finish(),
    );
    Ok(Arc::new(out) as ArrayRef)
}

fn cast_json_values_to_struct(
    values: &[Option<JsonValue>],
    target_fields: Fields,
) -> Result<ArrayRef, String> {
    let field_count = target_fields.len();
    let mut field_values = vec![Vec::<Option<JsonValue>>::with_capacity(values.len()); field_count];
    let mut struct_nulls = NullBufferBuilder::new(values.len());

    for value in values {
        match value {
            Some(JsonValue::Array(items)) => {
                for (idx, field_buf) in field_values.iter_mut().enumerate() {
                    field_buf.push(items.get(idx).cloned());
                }
                struct_nulls.append_non_null();
            }
            Some(JsonValue::Object(map)) => {
                for (idx, field) in target_fields.iter().enumerate() {
                    field_values[idx].push(map.get(field.name()).cloned());
                }
                struct_nulls.append_non_null();
            }
            _ => {
                for field_buf in &mut field_values {
                    field_buf.push(None);
                }
                struct_nulls.append_null();
            }
        }
    }

    let mut casted_fields = Vec::with_capacity(field_count);
    for (idx, field) in target_fields.iter().enumerate() {
        casted_fields.push(cast_json_values_to_target(
            &field_values[idx],
            field.data_type(),
            Some(field.as_ref()),
        )?);
    }
    let out = StructArray::new(target_fields, casted_fields, struct_nulls.finish());
    Ok(Arc::new(out) as ArrayRef)
}

fn cast_json_values_to_map(
    values: &[Option<JsonValue>],
    entries_field: Arc<Field>,
    ordered: bool,
) -> Result<ArrayRef, String> {
    let DataType::Struct(entry_fields) = entries_field.data_type() else {
        return Err("CAST MAP target entries field must be STRUCT".to_string());
    };
    if entry_fields.len() != 2 {
        return Err("CAST MAP target entries struct must have 2 fields".to_string());
    }
    let key_field = entry_fields[0].clone();
    let value_field = entry_fields[1].clone();

    let mut key_values = Vec::<Option<String>>::new();
    let mut value_values = Vec::<Option<JsonValue>>::new();
    let mut offsets = Vec::with_capacity(values.len() + 1);
    offsets.push(0_i32);
    let mut current: i64 = 0;
    let mut map_nulls = NullBufferBuilder::new(values.len());

    for value in values {
        match value {
            Some(JsonValue::Object(map)) => {
                for (key, child) in map.iter() {
                    key_values.push(Some(key.clone()));
                    value_values.push(Some(child.clone()));
                }
                current = current
                    .checked_add(map.len() as i64)
                    .ok_or_else(|| "CAST MAP offset overflow".to_string())?;
                if current > i32::MAX as i64 {
                    return Err("CAST MAP offset overflow".to_string());
                }
                map_nulls.append_non_null();
                offsets.push(current as i32);
            }
            _ => {
                map_nulls.append_null();
                offsets.push(current as i32);
            }
        }
    }

    let casted_keys = cast_optional_utf8_to_target(key_values, key_field.data_type())?;
    let casted_values = cast_json_values_to_target(
        &value_values,
        value_field.data_type(),
        Some(value_field.as_ref()),
    )?;
    let entries = StructArray::new(entry_fields.clone(), vec![casted_keys, casted_values], None);
    let map = MapArray::new(
        entries_field,
        OffsetBuffer::new(offsets.into()),
        entries,
        map_nulls.finish(),
        ordered,
    );
    Ok(Arc::new(map) as ArrayRef)
}

fn cast_json_values_to_target(
    values: &[Option<JsonValue>],
    target_type: &DataType,
    target_field: Option<&Field>,
) -> Result<ArrayRef, String> {
    match target_type {
        DataType::Utf8 => {
            let json_semantic = field_has_json_semantics(target_field);
            let mut builder = StringBuilder::new();
            for value in values {
                match value {
                    None => builder.append_null(),
                    Some(v) if json_semantic => {
                        builder.append_value(json_value_to_normalized_text(v)?);
                    }
                    Some(v) => match json_value_to_varchar(v)? {
                        Some(text) => builder.append_value(text),
                        None => builder.append_null(),
                    },
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::List(target_field) => cast_json_values_to_list(values, target_field.clone()),
        DataType::Struct(target_fields) => {
            cast_json_values_to_struct(values, target_fields.clone())
        }
        DataType::Map(entries_field, ordered) => {
            cast_json_values_to_map(values, entries_field.clone(), *ordered)
        }
        _ => {
            let mut scalar_strings = Vec::with_capacity(values.len());
            for value in values {
                let text = match value {
                    None => None,
                    Some(v) if v.is_null() => None,
                    Some(JsonValue::Bool(v))
                        if is_integral_or_largeint_target_type(target_type) =>
                    {
                        Some(if *v { "1" } else { "0" }.to_string())
                    }
                    Some(JsonValue::Number(v))
                        if is_integral_or_largeint_target_type(target_type) =>
                    {
                        json_number_to_integral_string(v)
                    }
                    Some(v) if v.is_string() => v.as_str().map(|s| s.to_string()),
                    Some(v) => Some(json_value_to_normalized_text(v)?),
                };
                scalar_strings.push(text);
            }
            cast_optional_utf8_to_target(scalar_strings, target_type)
        }
    }
}

fn cast_utf8_json_to_target(array: &ArrayRef, target_type: &DataType) -> Result<ArrayRef, String> {
    let arr = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "failed to downcast to StringArray".to_string())?;
    let parsed = parse_utf8_json_rows(arr);
    cast_json_values_to_target(&parsed, target_type, None)
}

fn cast_struct_to_struct(array: &ArrayRef, target_fields: &Fields) -> Result<ArrayRef, String> {
    let source = array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| "failed to downcast to StructArray".to_string())?;
    let source_fields = match array.data_type() {
        DataType::Struct(fields) => fields,
        other => {
            return Err(format!(
                "CAST failed: source is not STRUCT, got {:?}",
                other
            ));
        }
    };
    if source_fields.len() != target_fields.len() {
        return Err(format!(
            "CAST STRUCT field count mismatch: source={} target={}",
            source_fields.len(),
            target_fields.len()
        ));
    }

    let mut casted_columns = Vec::with_capacity(target_fields.len());
    let mut out_fields = target_fields.iter().cloned().collect::<Vec<_>>();
    for (idx, target_field) in target_fields.iter().enumerate() {
        let source_col = source.column(idx).clone();
        let casted = if source_col.data_type() == target_field.data_type() {
            source_col
        } else if source_col.null_count() == source_col.len() {
            new_null_array(target_field.data_type(), source_col.len())
        } else {
            cast_with_special_rules(&source_col, target_field.data_type())?
        };
        if casted.data_type() != target_field.data_type() {
            out_fields[idx] = Arc::new(Field::new(
                target_field.name(),
                casted.data_type().clone(),
                target_field.is_nullable(),
            ));
        }
        casted_columns.push(casted);
    }

    let out = StructArray::new(
        Fields::from(out_fields),
        casted_columns,
        source.nulls().cloned(),
    );
    Ok(Arc::new(out) as ArrayRef)
}

fn cast_map_to_utf8_array(array: &ArrayRef) -> Result<ArrayRef, String> {
    let source = array
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| "failed to downcast to MapArray".to_string())?;
    let (entries_field, ordered) = match source.data_type() {
        DataType::Map(entries_field, ordered) => (entries_field.clone(), *ordered),
        other => {
            return Err(format!("CAST failed: source is not MAP, got {:?}", other));
        }
    };
    let DataType::Struct(entry_fields) = entries_field.data_type() else {
        return Err("CAST MAP source entries field must be STRUCT".to_string());
    };
    if entry_fields.len() != 2 {
        return Err("CAST MAP source entries struct must have 2 fields".to_string());
    }
    let (sorted_offsets, sorted_indices) =
        crate::exec::expr::function::map::sorted_map_offsets_and_indices(source)?;
    let sorted_index_array = UInt32Array::from(sorted_indices);
    let sorted_keys = take(source.keys().as_ref(), &sorted_index_array, None)
        .map_err(|e| format!("CAST MAP failed to reorder keys: {e}"))?;
    let sorted_values = take(source.values().as_ref(), &sorted_index_array, None)
        .map_err(|e| format!("CAST MAP failed to reorder values: {e}"))?;
    let entries = StructArray::new(entry_fields.clone(), vec![sorted_keys, sorted_values], None);
    let sorted_map = MapArray::try_new(
        entries_field,
        sorted_offsets,
        entries,
        source.nulls().cloned(),
        ordered,
    )
    .map_err(|e| format!("CAST MAP failed to rebuild sorted map: {e}"))?;
    cast(&sorted_map, &DataType::Utf8).map_err(|e| e.to_string())
}

fn cast_map_to_map(
    array: &ArrayRef,
    target_entries: &Arc<Field>,
    ordered: bool,
) -> Result<ArrayRef, String> {
    let source = array
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| "failed to downcast to MapArray".to_string())?;
    let DataType::Struct(target_entry_fields) = target_entries.data_type() else {
        return Err("CAST MAP target entries type must be STRUCT".to_string());
    };
    if target_entry_fields.len() != 2 {
        return Err("CAST MAP target entries struct must have 2 fields".to_string());
    }
    let target_key_field = target_entry_fields[0].clone();
    let target_value_field = target_entry_fields[1].clone();

    let source_entries = source.entries();
    let source_keys = source_entries.column(0).clone();
    let source_values = source_entries.column(1).clone();

    let casted_keys = if source_keys.data_type() == target_key_field.data_type() {
        source_keys
    } else if source_keys.null_count() == source_keys.len() {
        new_null_array(target_key_field.data_type(), source_keys.len())
    } else {
        cast_with_special_rules(&source_keys, target_key_field.data_type())?
    };
    let casted_values = if source_values.data_type() == target_value_field.data_type() {
        source_values
    } else if source_values.null_count() == source_values.len() {
        new_null_array(target_value_field.data_type(), source_values.len())
    } else {
        cast_with_special_rules(&source_values, target_value_field.data_type())?
    };

    let mut out_entry_fields = target_entry_fields.clone();
    if casted_keys.null_count() > 0 && !target_key_field.is_nullable() {
        let mut adjusted = out_entry_fields.iter().cloned().collect::<Vec<_>>();
        adjusted[0] = Arc::new(Field::new(
            target_key_field.name(),
            target_key_field.data_type().clone(),
            true,
        ));
        out_entry_fields = Fields::from(adjusted);
    }
    if casted_values.data_type() != target_value_field.data_type() {
        let mut adjusted = out_entry_fields.iter().cloned().collect::<Vec<_>>();
        adjusted[1] = Arc::new(Field::new(
            target_value_field.name(),
            casted_values.data_type().clone(),
            target_value_field.is_nullable(),
        ));
        out_entry_fields = Fields::from(adjusted);
    }

    let out_entries = StructArray::new(
        out_entry_fields.clone(),
        vec![casted_keys, casted_values],
        None,
    );
    let out_entries_field = Arc::new(Field::new(
        target_entries.name(),
        DataType::Struct(out_entry_fields),
        target_entries.is_nullable(),
    ));
    let out = MapArray::new(
        out_entries_field,
        OffsetBuffer::new(source.value_offsets().to_vec().into()),
        out_entries,
        source.nulls().cloned(),
        ordered,
    );
    Ok(Arc::new(out) as ArrayRef)
}

pub(crate) fn cast_with_special_rules(
    array: &ArrayRef,
    target_type: &DataType,
) -> Result<ArrayRef, String> {
    if array.data_type() == target_type {
        return Ok(array.clone());
    }
    match (array.data_type(), target_type) {
        (source, DataType::Date32) if is_numeric_datetime_source(source) => {
            cast_numeric_to_date32_array(array)
        }
        (source, DataType::Timestamp(_, _)) if is_numeric_datetime_source(source) => {
            cast_numeric_to_timestamp_array(array, target_type)
        }
        (source, DataType::FixedSizeBinary(width))
            if *width == largeint::LARGEINT_BYTE_WIDTH && is_numeric_datetime_source(source) =>
        {
            cast_numeric_to_largeint_binary_array(array)
        }
        (DataType::Utf8, DataType::FixedSizeBinary(width))
            if *width == largeint::LARGEINT_BYTE_WIDTH =>
        {
            cast_utf8_to_largeint_binary(array)
        }
        (DataType::Utf8, DataType::Boolean) => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "failed to downcast to StringArray".to_string())?;
            Ok(cast_utf8_to_boolean_array(arr))
        }
        (DataType::Float64, DataType::Decimal128(precision, scale)) => {
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| "failed to downcast to Float64Array".to_string())?;
            cast_float_to_decimal_with_rounding(
                arr.len(),
                |row| {
                    if arr.is_null(row) {
                        None
                    } else {
                        Some(arr.value(row))
                    }
                },
                *precision,
                *scale,
            )
        }
        (DataType::Float32, DataType::Decimal128(precision, scale)) => {
            let arr = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| "failed to downcast to Float32Array".to_string())?;
            cast_float_to_decimal_with_rounding(
                arr.len(),
                |row| {
                    if arr.is_null(row) {
                        None
                    } else {
                        Some(arr.value(row) as f64)
                    }
                },
                *precision,
                *scale,
            )
        }
        (DataType::Float64, DataType::Utf8) => {
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| "failed to downcast to Float64Array".to_string())?;
            Ok(cast_float64_to_utf8_array(arr))
        }
        (DataType::Float32, DataType::Utf8) => {
            let arr = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| "failed to downcast to Float32Array".to_string())?;
            Ok(cast_float32_to_utf8_array(arr))
        }
        (
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64,
            DataType::Decimal128(target_precision, target_scale),
        ) => cast_integral_to_decimal128_relaxed(array, *target_precision, *target_scale),
        (DataType::Boolean, DataType::Decimal128(precision, scale)) => {
            cast_boolean_to_decimal128_array(array, *precision, *scale)
        }
        (DataType::Boolean, DataType::Decimal256(precision, scale)) => {
            cast_boolean_to_decimal256_array(array, *precision, *scale)
        }
        (DataType::Decimal128(_, source_scale), DataType::Utf8) => {
            cast_decimal_to_utf8_array(array, *source_scale)
        }
        (DataType::Decimal256(_, source_scale), DataType::Utf8) => {
            cast_decimal256_to_utf8_array(array, *source_scale)
        }
        (DataType::Decimal256(_, source_scale), DataType::Boolean) => {
            cast_decimal256_to_boolean(array, *source_scale)
        }
        (DataType::Decimal256(_, source_scale), DataType::Int8) => {
            cast_decimal256_to_int8(array, *source_scale)
        }
        (DataType::Decimal256(_, source_scale), DataType::Int16) => {
            cast_decimal256_to_int16(array, *source_scale)
        }
        (DataType::Decimal256(_, source_scale), DataType::Int32) => {
            cast_decimal256_to_int32(array, *source_scale)
        }
        (DataType::Decimal256(_, source_scale), DataType::Int64) => {
            cast_decimal256_to_int64(array, *source_scale)
        }
        (DataType::Decimal256(_, source_scale), DataType::FixedSizeBinary(width))
            if *width == largeint::LARGEINT_BYTE_WIDTH =>
        {
            cast_decimal256_to_largeint_binary(array, *source_scale)
        }
        (DataType::FixedSizeBinary(width), DataType::Int8)
            if *width == largeint::LARGEINT_BYTE_WIDTH =>
        {
            cast_largeint_binary_to_int8(array)
        }
        (DataType::FixedSizeBinary(width), DataType::Int16)
            if *width == largeint::LARGEINT_BYTE_WIDTH =>
        {
            cast_largeint_binary_to_int16(array)
        }
        (DataType::FixedSizeBinary(width), DataType::Int32)
            if *width == largeint::LARGEINT_BYTE_WIDTH =>
        {
            cast_largeint_binary_to_int32(array)
        }
        (DataType::FixedSizeBinary(width), DataType::Int64)
            if *width == largeint::LARGEINT_BYTE_WIDTH =>
        {
            cast_largeint_binary_to_int64(array)
        }
        (DataType::Timestamp(unit, tz), DataType::Utf8) => {
            cast_timestamp_to_utf8_array(array, unit, tz.as_deref())
        }
        (DataType::Utf8, DataType::Decimal128(_, _)) => {
            cast_utf8_to_decimal_with_empty_as_null(array, target_type)
        }
        (
            DataType::Decimal128(_, source_scale),
            DataType::Decimal128(target_precision, target_scale),
        ) => {
            cast_decimal_to_decimal_relaxed(array, *source_scale, *target_precision, *target_scale)
        }
        (
            DataType::Decimal256(_, source_scale),
            DataType::Decimal256(target_precision, target_scale),
        ) => cast_decimal256_to_decimal256_relaxed(
            array,
            *source_scale,
            *target_precision,
            *target_scale,
        ),
        (DataType::FixedSizeBinary(width), DataType::Utf8)
            if *width == largeint::LARGEINT_BYTE_WIDTH =>
        {
            cast_largeint_binary_to_utf8(array)
        }
        (DataType::Map(_, _), DataType::Utf8) => cast_map_to_utf8_array(array),
        (DataType::FixedSizeBinary(width), DataType::Boolean)
            if *width == largeint::LARGEINT_BYTE_WIDTH =>
        {
            cast_largeint_binary_to_boolean(array)
        }
        (DataType::Utf8, DataType::List(target_field)) => {
            if let Some(out) = try_cast_utf8_json_array_to_list(array, target_field.clone())? {
                Ok(out)
            } else {
                cast(array.as_ref(), target_type).map_err(|e| e.to_string())
            }
        }
        (DataType::Utf8, DataType::Struct(_)) | (DataType::Utf8, DataType::Map(_, _)) => {
            cast_utf8_json_to_target(array, target_type)
        }
        (DataType::List(_), DataType::List(target_field)) => {
            let list = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| "failed to downcast to ListArray".to_string())?;
            // Empty list values can be safely retagged to any target item type.
            // This matches StarRocks behavior for casts around empty array literals.
            let cast_values = if list.values().is_empty() {
                arrow::array::new_empty_array(target_field.data_type())
            } else if list.values().null_count() == list.values().len() {
                // Preserve all-null list literals while adapting the target item type.
                arrow::array::new_null_array(target_field.data_type(), list.values().len())
            } else {
                cast_with_special_rules(&list.values(), target_field.data_type())?
            };
            let out = ListArray::new(
                target_field.clone(),
                OffsetBuffer::new(list.value_offsets().to_vec().into()),
                cast_values,
                list.nulls().cloned(),
            );
            Ok(Arc::new(out) as ArrayRef)
        }
        (DataType::Struct(_), DataType::Struct(target_fields)) => {
            cast_struct_to_struct(array, target_fields)
        }
        (DataType::Map(_, _), DataType::Map(target_entries, ordered)) => {
            cast_map_to_map(array, target_entries, *ordered)
        }
        _ => cast(array.as_ref(), target_type).map_err(|e| e.to_string()),
    }
}

fn cast_utf8_to_decimal_with_empty_as_null(
    child_array: &ArrayRef,
    target_type: &DataType,
) -> Result<ArrayRef, String> {
    let arr = child_array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "failed to downcast to StringArray".to_string())?;

    let mut builder = StringBuilder::new();
    for i in 0..arr.len() {
        if arr.is_null(i) {
            builder.append_null();
            continue;
        }
        let value = arr.value(i);
        if value.trim().is_empty() {
            builder.append_null();
        } else {
            builder.append_value(value);
        }
    }

    let normalized = Arc::new(builder.finish()) as ArrayRef;
    cast(normalized.as_ref(), target_type).map_err(|e| e.to_string())
}

fn cast_decimal_to_utf8_array(child_array: &ArrayRef, scale: i8) -> Result<ArrayRef, String> {
    let arr = child_array
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| "failed to downcast to Decimal128Array".to_string())?;

    let mut builder = StringBuilder::new();
    for row in 0..arr.len() {
        if arr.is_null(row) {
            builder.append_null();
            continue;
        }
        builder.append_value(format_decimal_with_scale(arr.value(row), scale));
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn cast_decimal256_to_utf8_array(child_array: &ArrayRef, scale: i8) -> Result<ArrayRef, String> {
    let arr = child_array
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .ok_or_else(|| "failed to downcast to Decimal256Array".to_string())?;

    let mut builder = StringBuilder::new();
    for row in 0..arr.len() {
        if arr.is_null(row) {
            builder.append_null();
            continue;
        }
        builder.append_value(format_decimal256_with_scale(arr.value(row), scale));
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn cast_boolean_to_decimal256_array(
    child_array: &ArrayRef,
    precision: u8,
    scale: i8,
) -> Result<ArrayRef, String> {
    let arr = child_array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| "failed to downcast to BooleanArray".to_string())?;

    let factor = if scale == 0 {
        None
    } else {
        Some(
            pow10_i256(scale.unsigned_abs() as usize)
                .map_err(|e| format!("decimal scale overflow while casting BOOLEAN: {e}"))?,
        )
    };

    let mut out = Vec::with_capacity(arr.len());
    for row in 0..arr.len() {
        if arr.is_null(row) {
            out.push(None);
            continue;
        }
        let mut value = if arr.value(row) {
            i256::from_i128(1)
        } else {
            i256::ZERO
        };
        if let Some(factor) = factor {
            if scale > 0 {
                value = value
                    .checked_mul(factor)
                    .ok_or_else(|| "decimal overflow while casting BOOLEAN".to_string())?;
            } else {
                value = value
                    .checked_div(factor)
                    .ok_or_else(|| "decimal overflow while casting BOOLEAN".to_string())?;
            }
        }
        out.push(Some(value));
    }

    let array = Decimal256Array::from(out)
        .with_precision_and_scale(precision, scale)
        .map_err(|e| e.to_string())?;
    Ok(Arc::new(array))
}

fn try_cast_utf8_json_array_to_list(
    child_array: &ArrayRef,
    target_field: Arc<Field>,
) -> Result<Option<ArrayRef>, String> {
    let arr = child_array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "failed to downcast to StringArray".to_string())?;

    let mut parsed_rows = Vec::<Option<Vec<Option<String>>>>::with_capacity(arr.len());
    for row in 0..arr.len() {
        if arr.is_null(row) {
            parsed_rows.push(None);
            continue;
        }
        let value: JsonValue = match serde_json::from_str(arr.value(row)) {
            Ok(v) => v,
            Err(_) => return Ok(None),
        };
        match value {
            JsonValue::Null => {
                parsed_rows.push(None);
            }
            JsonValue::Array(items) => {
                let mut row_values = Vec::with_capacity(items.len());
                for item in items {
                    if item.is_null() {
                        row_values.push(None);
                    } else {
                        let normalized = if let Some(text) = item.as_str() {
                            text.to_string()
                        } else {
                            item.to_string()
                        };
                        row_values.push(Some(normalized));
                    }
                }
                parsed_rows.push(Some(row_values));
            }
            _ => return Ok(None),
        }
    }

    let mut flat_values = Vec::<Option<String>>::new();
    let mut offsets = Vec::with_capacity(arr.len() + 1);
    offsets.push(0_i32);
    let mut current: i64 = 0;
    let mut null_builder = NullBufferBuilder::new(arr.len());

    for parsed in parsed_rows {
        match parsed {
            None => {
                null_builder.append_null();
                offsets.push(current as i32);
            }
            Some(values) => {
                current = current
                    .checked_add(values.len() as i64)
                    .ok_or_else(|| "CAST list offset overflow".to_string())?;
                if current > i32::MAX as i64 {
                    return Err("CAST list offset overflow".to_string());
                }
                flat_values.extend(values);
                null_builder.append_non_null();
                offsets.push(current as i32);
            }
        }
    }

    let string_values = Arc::new(StringArray::from(flat_values)) as ArrayRef;
    let cast_values = if string_values.data_type() == target_field.data_type() {
        string_values
    } else {
        cast_with_special_rules(&string_values, target_field.data_type())?
    };
    let out = ListArray::new(
        target_field.clone(),
        OffsetBuffer::new(offsets.into()),
        cast_values,
        null_builder.finish(),
    );
    Ok(Some(Arc::new(out) as ArrayRef))
}

fn retag_decimal_array(
    array: &Decimal128Array,
    precision: u8,
    scale: i8,
) -> Result<ArrayRef, String> {
    let data = array
        .to_data()
        .into_builder()
        .data_type(DataType::Decimal128(precision, scale))
        .build()
        .map_err(|e| e.to_string())?;
    Ok(make_array(data))
}

fn retag_decimal256_array(
    array: &Decimal256Array,
    precision: u8,
    scale: i8,
) -> Result<ArrayRef, String> {
    let data = array
        .to_data()
        .into_builder()
        .data_type(DataType::Decimal256(precision, scale))
        .build()
        .map_err(|e| e.to_string())?;
    Ok(make_array(data))
}

fn fits_i128_precision(value: i128, precision: u8) -> bool {
    value.unsigned_abs().to_string().len() <= usize::from(precision)
}

fn cast_integral_to_decimal128_relaxed(
    child_array: &ArrayRef,
    target_precision: u8,
    target_scale: i8,
) -> Result<ArrayRef, String> {
    let upscale = if target_scale > 0 {
        Some(
            pow10_i128(target_scale as u32)
                .ok_or_else(|| "decimal scale overflow while casting integral".to_string())?,
        )
    } else {
        None
    };
    let downscale = if target_scale < 0 {
        Some(
            pow10_i128((-target_scale) as u32)
                .ok_or_else(|| "decimal scale overflow while casting integral".to_string())?,
        )
    } else {
        None
    };

    let mut values = Vec::with_capacity(child_array.len());
    for row in 0..child_array.len() {
        if child_array.is_null(row) {
            values.push(None);
            continue;
        }
        let mut value = match child_array.data_type() {
            DataType::Int8 => child_array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| "failed to downcast to Int8Array".to_string())?
                .value(row) as i128,
            DataType::Int16 => child_array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| "failed to downcast to Int16Array".to_string())?
                .value(row) as i128,
            DataType::Int32 => child_array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "failed to downcast to Int32Array".to_string())?
                .value(row) as i128,
            DataType::Int64 => child_array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "failed to downcast to Int64Array".to_string())?
                .value(row) as i128,
            other => {
                return Err(format!(
                    "integral to DECIMAL cast unsupported source type: {:?}",
                    other
                ));
            }
        };

        if let Some(factor) = upscale {
            let Some(scaled) = value.checked_mul(factor) else {
                values.push(None);
                continue;
            };
            value = scaled;
        } else if let Some(factor) = downscale {
            value /= factor;
        }

        // StarRocks keeps a BIGINT-compatible window for narrow DECIMAL targets.
        // For wider targets, honor FE-declared precision to allow large scaled BIGINTs
        // (for example BIGINT -> DECIMAL(27,1)).
        let effective_precision = if target_precision <= 18 {
            19
        } else {
            target_precision
        };
        if !fits_i128_precision(value, effective_precision) {
            values.push(None);
            continue;
        }
        values.push(Some(value));
    }

    let wide = Decimal128Array::from(values)
        .with_precision_and_scale(38, target_scale)
        .map_err(|e| e.to_string())?;
    retag_decimal_array(&wide, target_precision, target_scale)
}

fn cast_decimal_to_decimal_relaxed(
    child_array: &ArrayRef,
    source_scale: i8,
    target_precision: u8,
    target_scale: i8,
) -> Result<ArrayRef, String> {
    let arr = child_array
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| "failed to downcast to Decimal128Array".to_string())?;
    let mut values = Vec::with_capacity(arr.len());
    for row in 0..arr.len() {
        if arr.is_null(row) {
            values.push(None);
            continue;
        }
        let mut value = arr.value(row);
        if source_scale < target_scale {
            let factor = pow10_i128((target_scale - source_scale) as u32)
                .ok_or_else(|| "decimal scale overflow while casting DECIMAL".to_string())?;
            let Some(scaled) = value.checked_mul(factor) else {
                values.push(None);
                continue;
            };
            value = scaled;
        } else if source_scale > target_scale {
            let factor = pow10_i128((source_scale - target_scale) as u32)
                .ok_or_else(|| "decimal scale overflow while casting DECIMAL".to_string())?;
            let quotient = value / factor;
            let remainder = value % factor;
            let needs_round = remainder.abs().saturating_mul(2) >= factor;
            value = if needs_round {
                let carry = if value < 0 { -1 } else { 1 };
                let Some(rounded) = quotient.checked_add(carry) else {
                    values.push(None);
                    continue;
                };
                rounded
            } else {
                quotient
            };
        }
        // Keep downscale casts (higher scale -> lower scale) even when the rounded
        // integer part exceeds declared target precision, to match StarRocks
        // decimal regression behavior. Enforce precision only for same-scale and
        // upscale casts where overflow should still null out. For DECIMAL32/64
        // families (precision <= 18), StarRocks allows wider integral parts in
        // array-literal cast paths, so use an 18-digit effective precision window.
        let enforce_precision = source_scale <= target_scale;
        let effective_target_precision = if target_precision <= 18 {
            18
        } else {
            target_precision
        };
        if enforce_precision && !decimal_value_within_precision(value, effective_target_precision) {
            values.push(None);
            continue;
        }
        values.push(Some(value));
    }

    // Build with max precision first, then retag to FE-declared precision/scale.
    let wide = Decimal128Array::from(values)
        .with_precision_and_scale(38, target_scale)
        .map_err(|e| e.to_string())?;
    retag_decimal_array(&wide, target_precision, target_scale)
}

fn cast_decimal256_to_decimal256_relaxed(
    child_array: &ArrayRef,
    source_scale: i8,
    target_precision: u8,
    target_scale: i8,
) -> Result<ArrayRef, String> {
    let arr = child_array
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .ok_or_else(|| "failed to downcast to Decimal256Array".to_string())?;

    let upscale = if source_scale < target_scale {
        Some(
            pow10_i256((target_scale - source_scale) as usize)
                .map_err(|e| format!("decimal scale overflow while casting DECIMAL: {e}"))?,
        )
    } else {
        None
    };
    let downscale = if source_scale > target_scale {
        Some(
            pow10_i256((source_scale - target_scale) as usize)
                .map_err(|e| format!("decimal scale overflow while casting DECIMAL: {e}"))?,
        )
    } else {
        None
    };

    let mut values = Vec::with_capacity(arr.len());
    for row in 0..arr.len() {
        if arr.is_null(row) {
            values.push(None);
            continue;
        }

        let mut value = arr.value(row);
        if let Some(factor) = upscale {
            let Some(scaled) = value.checked_mul(factor) else {
                values.push(None);
                continue;
            };
            value = scaled;
        } else if let Some(factor) = downscale {
            let quotient = value / factor;
            let remainder = value % factor;
            let remainder_abs = if remainder < i256::ZERO {
                let Some(abs) = remainder.checked_neg() else {
                    values.push(None);
                    continue;
                };
                abs
            } else {
                remainder
            };
            let doubled = match remainder_abs.checked_mul(i256::from_i128(2)) {
                Some(v) => v,
                None => {
                    values.push(None);
                    continue;
                }
            };
            let needs_round = doubled >= factor;
            value = if needs_round {
                let carry = if value < i256::ZERO {
                    i256::from_i128(-1)
                } else {
                    i256::from_i128(1)
                };
                let Some(rounded) = quotient.checked_add(carry) else {
                    values.push(None);
                    continue;
                };
                rounded
            } else {
                quotient
            };
        }
        // StarRocks keeps DECIMAL256 downscale casts (higher scale -> lower scale)
        // even when the rounded integer part exceeds the declared target precision.
        // Enforce precision bounds for same-scale/upscale casts, but keep downscale
        // values to match FE regression expectations.
        let enforce_precision = source_scale <= target_scale;
        if enforce_precision && !decimal256_value_within_precision(value, target_precision) {
            values.push(None);
            continue;
        }
        values.push(Some(value));
    }

    let wide = Decimal256Array::from(values);
    retag_decimal256_array(&wide, target_precision, target_scale)
}

fn cast_largeint_binary_to_decimal(
    child_array: &ArrayRef,
    precision: u8,
    scale: i8,
) -> Result<ArrayRef, String> {
    let arr = largeint::as_fixed_size_binary_array(child_array, "cast LARGEINT to DECIMAL")?;
    let mut values = Vec::with_capacity(arr.len());
    let multiplier = if scale >= 0 {
        Some(
            10_i128
                .checked_pow(scale as u32)
                .ok_or_else(|| format!("decimal scale overflow: {scale}"))?,
        )
    } else {
        None
    };
    let divisor = if scale < 0 {
        Some(
            10_i128
                .checked_pow((-scale) as u32)
                .ok_or_else(|| format!("decimal scale overflow: {scale}"))?,
        )
    } else {
        None
    };

    for row in 0..arr.len() {
        if arr.is_null(row) {
            values.push(None);
            continue;
        }
        let mut value = largeint::value_at(arr, row)?;
        if let Some(m) = multiplier {
            let Some(scaled) = value.checked_mul(m) else {
                values.push(None);
                continue;
            };
            value = scaled;
        } else if let Some(d) = divisor {
            value /= d;
        }
        let enforce_precision = precision <= 18;
        if enforce_precision && !decimal_value_within_precision(value, precision) {
            values.push(None);
            continue;
        }
        values.push(Some(value));
    }

    let out = Decimal128Array::from(values)
        .with_precision_and_scale(precision, scale)
        .map_err(|e| e.to_string())?;
    Ok(Arc::new(out) as ArrayRef)
}

fn cast_largeint_binary_to_decimal256(
    child_array: &ArrayRef,
    precision: u8,
    scale: i8,
) -> Result<ArrayRef, String> {
    let arr = largeint::as_fixed_size_binary_array(child_array, "cast LARGEINT to DECIMAL256")?;
    let mut values = Vec::with_capacity(arr.len());
    let multiplier = if scale >= 0 {
        Some(
            pow10_i256(scale as usize)
                .map_err(|e| format!("decimal scale overflow while casting LARGEINT: {e}"))?,
        )
    } else {
        None
    };
    let divisor = if scale < 0 {
        Some(
            pow10_i256((-scale) as usize)
                .map_err(|e| format!("decimal scale overflow while casting LARGEINT: {e}"))?,
        )
    } else {
        None
    };

    for row in 0..arr.len() {
        if arr.is_null(row) {
            values.push(None);
            continue;
        }
        let mut value = i256::from_i128(largeint::value_at(arr, row)?);
        if let Some(m) = multiplier {
            let Some(scaled) = value.checked_mul(m) else {
                values.push(None);
                continue;
            };
            value = scaled;
        } else if let Some(d) = divisor {
            let Some(scaled) = value.checked_div(d) else {
                values.push(None);
                continue;
            };
            value = scaled;
        }
        if !decimal256_value_within_precision(value, precision) {
            values.push(None);
            continue;
        }
        values.push(Some(value));
    }

    let out = Decimal256Array::from(values)
        .with_precision_and_scale(precision, scale)
        .map_err(|e| e.to_string())?;
    Ok(Arc::new(out) as ArrayRef)
}

fn decimal256_integral_values(arr: &Decimal256Array, source_scale: i8) -> Vec<Option<i128>> {
    let mut values = Vec::with_capacity(arr.len());
    for row in 0..arr.len() {
        if arr.is_null(row) {
            values.push(None);
            continue;
        }
        values.push(decimal256_to_i128_literal(arr.value(row), source_scale));
    }
    values
}

fn cast_decimal256_to_boolean(
    child_array: &ArrayRef,
    _source_scale: i8,
) -> Result<ArrayRef, String> {
    let arr = child_array
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .ok_or_else(|| "failed to downcast to Decimal256Array".to_string())?;
    let mut out = Vec::with_capacity(arr.len());
    for row in 0..arr.len() {
        if arr.is_null(row) {
            out.push(None);
            continue;
        }
        out.push(Some(arr.value(row) != i256::ZERO));
    }
    Ok(Arc::new(BooleanArray::from(out)) as ArrayRef)
}

fn cast_decimal256_to_int8(child_array: &ArrayRef, source_scale: i8) -> Result<ArrayRef, String> {
    let arr = child_array
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .ok_or_else(|| "failed to downcast to Decimal256Array".to_string())?;
    let out = decimal256_integral_values(arr, source_scale)
        .into_iter()
        .map(|v| v.and_then(|n| i8::try_from(n).ok()))
        .collect::<Vec<_>>();
    Ok(Arc::new(Int8Array::from(out)) as ArrayRef)
}

fn cast_decimal256_to_int16(child_array: &ArrayRef, source_scale: i8) -> Result<ArrayRef, String> {
    let arr = child_array
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .ok_or_else(|| "failed to downcast to Decimal256Array".to_string())?;
    let out = decimal256_integral_values(arr, source_scale)
        .into_iter()
        .map(|v| v.and_then(|n| i16::try_from(n).ok()))
        .collect::<Vec<_>>();
    Ok(Arc::new(Int16Array::from(out)) as ArrayRef)
}

fn cast_decimal256_to_int32(child_array: &ArrayRef, source_scale: i8) -> Result<ArrayRef, String> {
    let arr = child_array
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .ok_or_else(|| "failed to downcast to Decimal256Array".to_string())?;
    let out = decimal256_integral_values(arr, source_scale)
        .into_iter()
        .map(|v| v.and_then(|n| i32::try_from(n).ok()))
        .collect::<Vec<_>>();
    Ok(Arc::new(Int32Array::from(out)) as ArrayRef)
}

fn cast_decimal256_to_int64(child_array: &ArrayRef, source_scale: i8) -> Result<ArrayRef, String> {
    let arr = child_array
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .ok_or_else(|| "failed to downcast to Decimal256Array".to_string())?;
    let out = decimal256_integral_values(arr, source_scale)
        .into_iter()
        .map(|v| v.and_then(|n| i64::try_from(n).ok()))
        .collect::<Vec<_>>();
    Ok(Arc::new(Int64Array::from(out)) as ArrayRef)
}

fn cast_decimal256_to_largeint_binary(
    child_array: &ArrayRef,
    source_scale: i8,
) -> Result<ArrayRef, String> {
    let arr = child_array
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .ok_or_else(|| "failed to downcast to Decimal256Array".to_string())?;
    let values = decimal256_integral_values(arr, source_scale);
    largeint::array_from_i128(&values)
}

fn cast_boolean_to_decimal128_array(
    child_array: &ArrayRef,
    precision: u8,
    scale: i8,
) -> Result<ArrayRef, String> {
    let arr = child_array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| "failed to downcast to BooleanArray".to_string())?;
    let factor = if scale == 0 {
        None
    } else {
        Some(
            pow10_i128(scale.unsigned_abs() as u32)
                .ok_or_else(|| format!("decimal scale overflow while casting BOOLEAN: {scale}"))?,
        )
    };
    let mut out = Vec::with_capacity(arr.len());
    for row in 0..arr.len() {
        if arr.is_null(row) {
            out.push(None);
            continue;
        }
        let mut value = if arr.value(row) { 1_i128 } else { 0_i128 };
        if let Some(factor) = factor {
            if scale > 0 {
                let Some(scaled) = value.checked_mul(factor) else {
                    out.push(None);
                    continue;
                };
                value = scaled;
            } else {
                value /= factor;
            }
        }
        if !decimal_value_within_precision(value, precision) {
            out.push(None);
            continue;
        }
        out.push(Some(value));
    }
    let array = Decimal128Array::from(out)
        .with_precision_and_scale(precision, scale)
        .map_err(|e| e.to_string())?;
    Ok(Arc::new(array))
}

fn cast_largeint_binary_to_float64(child_array: &ArrayRef) -> Result<ArrayRef, String> {
    let arr = largeint::as_fixed_size_binary_array(child_array, "cast LARGEINT to DOUBLE")?;
    let mut out = Float64Builder::new();
    for row in 0..arr.len() {
        if arr.is_null(row) {
            out.append_null();
            continue;
        }
        out.append_value(largeint::value_at(arr, row)? as f64);
    }
    Ok(Arc::new(out.finish()) as ArrayRef)
}

fn cast_largeint_binary_to_int8(child_array: &ArrayRef) -> Result<ArrayRef, String> {
    let arr = largeint::as_fixed_size_binary_array(child_array, "cast LARGEINT to TINYINT")?;
    let mut out = Int8Builder::new();
    for row in 0..arr.len() {
        if arr.is_null(row) {
            out.append_null();
            continue;
        }
        let value = largeint::value_at(arr, row)?;
        if value < i8::MIN as i128 || value > i8::MAX as i128 {
            out.append_null();
            continue;
        }
        out.append_value(value as i8);
    }
    Ok(Arc::new(out.finish()) as ArrayRef)
}

fn cast_largeint_binary_to_int16(child_array: &ArrayRef) -> Result<ArrayRef, String> {
    let arr = largeint::as_fixed_size_binary_array(child_array, "cast LARGEINT to SMALLINT")?;
    let mut out = Int16Builder::new();
    for row in 0..arr.len() {
        if arr.is_null(row) {
            out.append_null();
            continue;
        }
        let value = largeint::value_at(arr, row)?;
        if value < i16::MIN as i128 || value > i16::MAX as i128 {
            out.append_null();
            continue;
        }
        out.append_value(value as i16);
    }
    Ok(Arc::new(out.finish()) as ArrayRef)
}

fn cast_largeint_binary_to_int32(child_array: &ArrayRef) -> Result<ArrayRef, String> {
    let arr = largeint::as_fixed_size_binary_array(child_array, "cast LARGEINT to INT")?;
    let mut out = Int32Builder::new();
    for row in 0..arr.len() {
        if arr.is_null(row) {
            out.append_null();
            continue;
        }
        let value = largeint::value_at(arr, row)?;
        if value < i32::MIN as i128 || value > i32::MAX as i128 {
            out.append_null();
            continue;
        }
        out.append_value(value as i32);
    }
    Ok(Arc::new(out.finish()) as ArrayRef)
}

fn cast_largeint_binary_to_int64(child_array: &ArrayRef) -> Result<ArrayRef, String> {
    let arr = largeint::as_fixed_size_binary_array(child_array, "cast LARGEINT to BIGINT")?;
    let mut out = Int64Builder::new();
    for row in 0..arr.len() {
        if arr.is_null(row) {
            out.append_null();
            continue;
        }
        let value = largeint::value_at(arr, row)?;
        if value < i64::MIN as i128 || value > i64::MAX as i128 {
            out.append_null();
            continue;
        }
        out.append_value(value as i64);
    }
    Ok(Arc::new(out.finish()) as ArrayRef)
}

fn cast_largeint_binary_to_float32(child_array: &ArrayRef) -> Result<ArrayRef, String> {
    let arr = largeint::as_fixed_size_binary_array(child_array, "cast LARGEINT to FLOAT")?;
    let mut out = Float32Builder::new();
    for row in 0..arr.len() {
        if arr.is_null(row) {
            out.append_null();
            continue;
        }
        out.append_value(largeint::value_at(arr, row)? as f32);
    }
    Ok(Arc::new(out.finish()) as ArrayRef)
}

fn cast_largeint_binary_to_boolean(child_array: &ArrayRef) -> Result<ArrayRef, String> {
    let arr = largeint::as_fixed_size_binary_array(child_array, "cast LARGEINT to BOOLEAN")?;
    let mut out = BooleanBuilder::new();
    for row in 0..arr.len() {
        if arr.is_null(row) {
            out.append_null();
            continue;
        }
        out.append_value(largeint::value_at(arr, row)? != 0);
    }
    Ok(Arc::new(out.finish()) as ArrayRef)
}

fn parse_utf8_to_largeint(value: &str) -> Option<i128> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    trimmed.parse::<i128>().ok()
}

fn cast_utf8_to_largeint_binary(child_array: &ArrayRef) -> Result<ArrayRef, String> {
    let arr = child_array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "failed to downcast to StringArray".to_string())?;
    let mut values = Vec::with_capacity(arr.len());
    for row in 0..arr.len() {
        if arr.is_null(row) {
            values.push(None);
            continue;
        }
        values.push(parse_utf8_to_largeint(arr.value(row)));
    }
    largeint::array_from_i128(&values)
}

fn cast_largeint_binary_to_utf8(child_array: &ArrayRef) -> Result<ArrayRef, String> {
    let arr = largeint::as_fixed_size_binary_array(child_array, "cast LARGEINT to VARCHAR")?;
    let mut builder = StringBuilder::new();
    for row in 0..arr.len() {
        if arr.is_null(row) {
            builder.append_null();
            continue;
        }
        let value = largeint::value_at(arr, row)?;
        builder.append_value(value.to_string());
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

pub fn eval(
    arena: &ExprArena,
    cast_expr: ExprId,
    child: ExprId,
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let target_type = arena
        .data_type(cast_expr)
        .ok_or_else(|| "CAST missing target data type".to_string())?
        .clone();

    let child_array = arena.eval(child, chunk)?;
    if child_array.data_type() == &target_type {
        return Ok(child_array);
    }

    if let DataType::LargeBinary = child_array.data_type() {
        let variant_arr = child_array
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .ok_or_else(|| "failed to downcast to LargeBinaryArray".to_string())?;
        let casted = cast_variant_array(variant_arr, &target_type).map_err(|e| {
            format!(
                "CAST failed: from {:?} to {:?}: {e}",
                child_array.data_type(),
                target_type
            )
        });
        return casted.and_then(|array| {
            sanitize_non_finite_cast_result(array, &target_type).map_err(|e| {
                format!(
                    "CAST failed: from {:?} to {:?}: {e}",
                    child_array.data_type(),
                    target_type
                )
            })
        });
    }

    match (child_array.data_type(), &target_type) {
        (DataType::Utf8, DataType::Boolean) => {
            let arr = child_array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "failed to downcast to StringArray".to_string())?;
            return Ok(cast_utf8_to_boolean_array(arr));
        }
        (DataType::Date32, DataType::Float64) => {
            let arr = child_array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| "failed to downcast to Date32Array".to_string())?;
            let casted = cast_date32_to_float64(arr).map_err(|e| {
                format!(
                    "CAST failed: from {:?} to {:?}: {e}",
                    child_array.data_type(),
                    target_type
                )
            });
            return casted.and_then(|array| {
                sanitize_non_finite_cast_result(array, &target_type).map_err(|e| {
                    format!(
                        "CAST failed: from {:?} to {:?}: {e}",
                        child_array.data_type(),
                        target_type
                    )
                })
            });
        }
        (DataType::Date32, DataType::Float32) => {
            let arr = child_array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| "failed to downcast to Date32Array".to_string())?;
            let casted = cast_date32_to_float32(arr).map_err(|e| {
                format!(
                    "CAST failed: from {:?} to {:?}: {e}",
                    child_array.data_type(),
                    target_type
                )
            });
            return casted.and_then(|array| {
                sanitize_non_finite_cast_result(array, &target_type).map_err(|e| {
                    format!(
                        "CAST failed: from {:?} to {:?}: {e}",
                        child_array.data_type(),
                        target_type
                    )
                })
            });
        }
        (DataType::Float64, DataType::Float32) => {
            let arr = child_array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| "failed to downcast to Float64Array".to_string())?;
            return Ok(cast_float64_to_float32_preserve_non_finite(arr));
        }
        (DataType::Float32, DataType::Float64) => {
            let arr = child_array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| "failed to downcast to Float32Array".to_string())?;
            return Ok(cast_float32_to_float64_preserve_non_finite(arr));
        }
        (DataType::Float64, DataType::Date32) => {
            let arr = child_array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| "failed to downcast to Float64Array".to_string())?;
            let casted = cast_float64_to_date32(arr).map_err(|e| {
                format!(
                    "CAST failed: from {:?} to {:?}: {e}",
                    child_array.data_type(),
                    target_type
                )
            });
            return casted.and_then(|array| {
                sanitize_non_finite_cast_result(array, &target_type).map_err(|e| {
                    format!(
                        "CAST failed: from {:?} to {:?}: {e}",
                        child_array.data_type(),
                        target_type
                    )
                })
            });
        }
        (DataType::Float32, DataType::Date32) => {
            let arr = child_array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| "failed to downcast to Float32Array".to_string())?;
            let casted = cast_float32_to_date32(arr).map_err(|e| {
                format!(
                    "CAST failed: from {:?} to {:?}: {e}",
                    child_array.data_type(),
                    target_type
                )
            });
            return casted.and_then(|array| {
                sanitize_non_finite_cast_result(array, &target_type).map_err(|e| {
                    format!(
                        "CAST failed: from {:?} to {:?}: {e}",
                        child_array.data_type(),
                        target_type
                    )
                })
            });
        }
        (DataType::FixedSizeBinary(width), DataType::Decimal128(precision, scale))
            if *width == largeint::LARGEINT_BYTE_WIDTH =>
        {
            let casted = cast_largeint_binary_to_decimal(&child_array, *precision, *scale)
                .map_err(|e| {
                    format!(
                        "CAST failed: from {:?} to {:?}: {e}",
                        child_array.data_type(),
                        target_type
                    )
                })?;
            return sanitize_non_finite_cast_result(casted, &target_type).map_err(|e| {
                format!(
                    "CAST failed: from {:?} to {:?}: {e}",
                    child_array.data_type(),
                    target_type
                )
            });
        }
        (DataType::FixedSizeBinary(width), DataType::Decimal256(precision, scale))
            if *width == largeint::LARGEINT_BYTE_WIDTH =>
        {
            let casted = cast_largeint_binary_to_decimal256(&child_array, *precision, *scale)
                .map_err(|e| {
                    format!(
                        "CAST failed: from {:?} to {:?}: {e}",
                        child_array.data_type(),
                        target_type
                    )
                })?;
            return sanitize_non_finite_cast_result(casted, &target_type).map_err(|e| {
                format!(
                    "CAST failed: from {:?} to {:?}: {e}",
                    child_array.data_type(),
                    target_type
                )
            });
        }
        (DataType::FixedSizeBinary(width), DataType::Float64)
            if *width == largeint::LARGEINT_BYTE_WIDTH =>
        {
            let casted = cast_largeint_binary_to_float64(&child_array).map_err(|e| {
                format!(
                    "CAST failed: from {:?} to {:?}: {e}",
                    child_array.data_type(),
                    target_type
                )
            })?;
            return sanitize_non_finite_cast_result(casted, &target_type).map_err(|e| {
                format!(
                    "CAST failed: from {:?} to {:?}: {e}",
                    child_array.data_type(),
                    target_type
                )
            });
        }
        (DataType::FixedSizeBinary(width), DataType::Float32)
            if *width == largeint::LARGEINT_BYTE_WIDTH =>
        {
            let casted = cast_largeint_binary_to_float32(&child_array).map_err(|e| {
                format!(
                    "CAST failed: from {:?} to {:?}: {e}",
                    child_array.data_type(),
                    target_type
                )
            })?;
            return sanitize_non_finite_cast_result(casted, &target_type).map_err(|e| {
                format!(
                    "CAST failed: from {:?} to {:?}: {e}",
                    child_array.data_type(),
                    target_type
                )
            });
        }
        _ => {}
    }

    if matches!(child_array.data_type(), DataType::Utf8)
        && matches!(target_type, DataType::Decimal128(_, _))
    {
        let casted =
            cast_utf8_to_decimal_with_empty_as_null(&child_array, &target_type).map_err(|e| {
                format!(
                    "CAST failed: from {:?} to {:?}: {e}",
                    child_array.data_type(),
                    target_type
                )
            })?;
        return sanitize_non_finite_cast_result(casted, &target_type).map_err(|e| {
            format!(
                "CAST failed: from {:?} to {:?}: {e}",
                child_array.data_type(),
                target_type
            )
        });
    }

    let casted = cast_with_special_rules(&child_array, &target_type).map_err(|e| {
        format!(
            "CAST failed: from {:?} to {:?}: {e}",
            child_array.data_type(),
            target_type
        )
    })?;
    if arena.allow_throw_exception() {
        if let Some(value) = first_float_to_int_overflow_value(&child_array, &casted, &target_type)?
        {
            return Err(format!(
                "Expr evaluate meet error: CAST failed: from {:?} to {:?}: {} conflict with range of {}",
                child_array.data_type(),
                target_type,
                value,
                integral_target_name(&target_type)
            ));
        }
        if has_decimal_to_decimal_overflow(&child_array, &casted) {
            return Err(
                "Expr evaluate meet error: The type cast from decimal to decimal overflows"
                    .to_string(),
            );
        }
    }
    sanitize_non_finite_cast_result(casted, &target_type).map_err(|e| {
        format!(
            "CAST failed: from {:?} to {:?}: {e}",
            child_array.data_type(),
            target_type
        )
    })
}

fn eval_time_internal(
    arena: &ExprArena,
    cast_expr: ExprId,
    child: ExprId,
    chunk: &Chunk,
    source_is_datetime: bool,
) -> Result<ArrayRef, String> {
    let target_type = arena
        .data_type(cast_expr)
        .ok_or_else(|| "CAST missing target data type".to_string())?
        .clone();
    if !matches!(target_type, DataType::Timestamp(_, _)) {
        return Err(format!(
            "CAST failed: TIME target must be timestamp-compatible, got {:?}",
            target_type
        ));
    }

    let child_array = arena.eval(child, chunk)?;
    if matches!(
        arena.node(child),
        Some(
            crate::exec::expr::ExprNode::CastTime(_)
                | crate::exec::expr::ExprNode::CastTimeFromDatetime(_)
        )
    ) {
        if child_array.data_type() == &target_type {
            return Ok(child_array);
        }
        return cast(&child_array, &target_type).map_err(|e| {
            format!(
                "CAST failed: from {:?} to {:?}: {}",
                child_array.data_type(),
                target_type,
                e
            )
        });
    }

    let mut seconds = Vec::with_capacity(child_array.len());
    match child_array.data_type() {
        DataType::Utf8 => {
            let arr = child_array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "failed to downcast to StringArray".to_string())?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    seconds.push(None);
                } else {
                    let raw = arr.value(i);
                    let parsed = if source_is_datetime {
                        parse_datetime_string_to_seconds(raw)
                    } else {
                        parse_time_string_to_seconds(raw)
                    };
                    seconds.push(parsed);
                }
            }
        }
        DataType::Boolean => {
            let arr = child_array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| "failed to downcast to BooleanArray".to_string())?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    seconds.push(None);
                } else {
                    seconds.push(parse_time_integer_to_seconds(if arr.value(i) {
                        1
                    } else {
                        0
                    }));
                }
            }
        }
        DataType::Int8 => {
            let arr = child_array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| "failed to downcast to Int8Array".to_string())?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    seconds.push(None);
                } else {
                    seconds.push(parse_time_integer_to_seconds(arr.value(i) as i64));
                }
            }
        }
        DataType::Int16 => {
            let arr = child_array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| "failed to downcast to Int16Array".to_string())?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    seconds.push(None);
                } else {
                    seconds.push(parse_time_integer_to_seconds(arr.value(i) as i64));
                }
            }
        }
        DataType::Int32 => {
            let arr = child_array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "failed to downcast to Int32Array".to_string())?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    seconds.push(None);
                } else {
                    seconds.push(parse_time_integer_to_seconds(arr.value(i) as i64));
                }
            }
        }
        DataType::Int64 => {
            let arr = child_array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "failed to downcast to Int64Array".to_string())?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    seconds.push(None);
                } else {
                    seconds.push(parse_time_integer_to_seconds(arr.value(i)));
                }
            }
        }
        DataType::Float32 => {
            let arr = child_array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| "failed to downcast to Float32Array".to_string())?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    seconds.push(None);
                } else {
                    seconds.push(parse_time_float_to_seconds(arr.value(i) as f64));
                }
            }
        }
        DataType::Float64 => {
            let arr = child_array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| "failed to downcast to Float64Array".to_string())?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    seconds.push(None);
                } else {
                    seconds.push(parse_time_float_to_seconds(arr.value(i)));
                }
            }
        }
        DataType::Decimal128(_, scale) => {
            let arr = child_array
                .as_any()
                .downcast_ref::<arrow::array::Decimal128Array>()
                .ok_or_else(|| "failed to downcast to Decimal128Array".to_string())?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    seconds.push(None);
                    continue;
                }
                let mut value = arr.value(i);
                if *scale > 0 {
                    let factor = 10_i128.pow(*scale as u32);
                    value /= factor;
                }
                seconds.push(parse_time_integer_to_seconds(value as i64));
            }
        }
        DataType::Date32 => {
            let arr = child_array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| "failed to downcast to Date32Array".to_string())?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    seconds.push(None);
                } else {
                    seconds.push(Some(0));
                }
            }
        }
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => {
                let arr = child_array
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .ok_or_else(|| "failed to downcast to TimestampSecondArray".to_string())?;
                for i in 0..arr.len() {
                    if arr.is_null(i) {
                        seconds.push(None);
                    } else {
                        seconds.push(seconds_from_timestamp(unit, arr.value(i)));
                    }
                }
            }
            TimeUnit::Millisecond => {
                let arr = child_array
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .ok_or_else(|| "failed to downcast to TimestampMillisecondArray".to_string())?;
                for i in 0..arr.len() {
                    if arr.is_null(i) {
                        seconds.push(None);
                    } else {
                        seconds.push(seconds_from_timestamp(unit, arr.value(i)));
                    }
                }
            }
            TimeUnit::Microsecond => {
                let arr = child_array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(|| "failed to downcast to TimestampMicrosecondArray".to_string())?;
                for i in 0..arr.len() {
                    if arr.is_null(i) {
                        seconds.push(None);
                    } else {
                        seconds.push(seconds_from_timestamp(unit, arr.value(i)));
                    }
                }
            }
            TimeUnit::Nanosecond => {
                let arr = child_array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .ok_or_else(|| "failed to downcast to TimestampNanosecondArray".to_string())?;
                for i in 0..arr.len() {
                    if arr.is_null(i) {
                        seconds.push(None);
                    } else {
                        seconds.push(seconds_from_timestamp(unit, arr.value(i)));
                    }
                }
            }
        },
        _ => {
            let casted = cast(child_array.as_ref(), &target_type).map_err(|e| {
                format!(
                    "CAST failed: from {:?} to {:?}: {e}",
                    child_array.data_type(),
                    target_type
                )
            })?;
            return sanitize_non_finite_cast_result(casted, &target_type).map_err(|e| {
                format!(
                    "CAST failed: from {:?} to {:?}: {e}",
                    child_array.data_type(),
                    target_type
                )
            });
        }
    }

    seconds_to_target_timestamp_array(seconds, &target_type)
}

pub fn eval_time(
    arena: &ExprArena,
    cast_expr: ExprId,
    child: ExprId,
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_time_internal(arena, cast_expr, child, chunk, false)
}

pub fn eval_time_from_datetime(
    arena: &ExprArena,
    cast_expr: ExprId,
    child: ExprId,
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_time_internal(arena, cast_expr, child, chunk, true)
}

fn cast_variant_array(
    variant_arr: &LargeBinaryArray,
    target_type: &DataType,
) -> Result<ArrayRef, String> {
    let tz = Local::now().offset().fix();
    match target_type {
        DataType::Boolean => {
            let mut builder = BooleanBuilder::new();
            for row in 0..variant_arr.len() {
                let value = match parse_variant(variant_arr, row) {
                    Some(v) => v,
                    None => {
                        builder.append_null();
                        continue;
                    }
                };
                if is_variant_null(&value).unwrap_or(true) {
                    builder.append_null();
                    continue;
                }
                match variant_to_bool(&value) {
                    Ok(v) => builder.append_value(v),
                    Err(_) => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Int8 => {
            let mut builder = Int8Builder::new();
            for row in 0..variant_arr.len() {
                let value = match parse_variant(variant_arr, row) {
                    Some(v) => v,
                    None => {
                        builder.append_null();
                        continue;
                    }
                };
                if is_variant_null(&value).unwrap_or(true) {
                    builder.append_null();
                    continue;
                }
                match variant_to_i64(&value) {
                    Ok(v) => builder.append_value(v as i8),
                    Err(_) => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Int16 => {
            let mut builder = Int16Builder::new();
            for row in 0..variant_arr.len() {
                let value = match parse_variant(variant_arr, row) {
                    Some(v) => v,
                    None => {
                        builder.append_null();
                        continue;
                    }
                };
                if is_variant_null(&value).unwrap_or(true) {
                    builder.append_null();
                    continue;
                }
                match variant_to_i64(&value) {
                    Ok(v) => builder.append_value(v as i16),
                    Err(_) => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Int32 => {
            let mut builder = Int32Builder::new();
            for row in 0..variant_arr.len() {
                let value = match parse_variant(variant_arr, row) {
                    Some(v) => v,
                    None => {
                        builder.append_null();
                        continue;
                    }
                };
                if is_variant_null(&value).unwrap_or(true) {
                    builder.append_null();
                    continue;
                }
                match variant_to_i64(&value) {
                    Ok(v) => builder.append_value(v as i32),
                    Err(_) => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Int64 => {
            let mut builder = Int64Builder::new();
            for row in 0..variant_arr.len() {
                let value = match parse_variant(variant_arr, row) {
                    Some(v) => v,
                    None => {
                        builder.append_null();
                        continue;
                    }
                };
                if is_variant_null(&value).unwrap_or(true) {
                    builder.append_null();
                    continue;
                }
                match variant_to_i64(&value) {
                    Ok(v) => builder.append_value(v),
                    Err(_) => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Float32 => {
            let mut builder = Float32Builder::new();
            for row in 0..variant_arr.len() {
                let value = match parse_variant(variant_arr, row) {
                    Some(v) => v,
                    None => {
                        builder.append_null();
                        continue;
                    }
                };
                if is_variant_null(&value).unwrap_or(true) {
                    builder.append_null();
                    continue;
                }
                match variant_to_f64(&value) {
                    Ok(v) if v.is_finite() => builder.append_value(v as f32),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Float64 => {
            let mut builder = Float64Builder::new();
            for row in 0..variant_arr.len() {
                let value = match parse_variant(variant_arr, row) {
                    Some(v) => v,
                    None => {
                        builder.append_null();
                        continue;
                    }
                };
                if is_variant_null(&value).unwrap_or(true) {
                    builder.append_null();
                    continue;
                }
                match variant_to_f64(&value) {
                    Ok(v) if v.is_finite() => builder.append_value(v),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Utf8 => {
            let mut builder = StringBuilder::new();
            for row in 0..variant_arr.len() {
                let value = match parse_variant(variant_arr, row) {
                    Some(v) => v,
                    None => {
                        builder.append_null();
                        continue;
                    }
                };
                if is_variant_null(&value).unwrap_or(true) {
                    builder.append_null();
                    continue;
                }
                match variant_to_string(&value) {
                    Ok(v) => builder.append_value(v),
                    Err(_) => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Date32 => {
            let mut builder = Date32Builder::new();
            for row in 0..variant_arr.len() {
                let value = match parse_variant(variant_arr, row) {
                    Some(v) => v,
                    None => {
                        builder.append_null();
                        continue;
                    }
                };
                if is_variant_null(&value).unwrap_or(true) {
                    builder.append_null();
                    continue;
                }
                match variant_to_date_days(&value, tz) {
                    Ok(v) => builder.append_value(v),
                    Err(_) => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            let mut builder = TimestampMicrosecondBuilder::new();
            for row in 0..variant_arr.len() {
                let value = match parse_variant(variant_arr, row) {
                    Some(v) => v,
                    None => {
                        builder.append_null();
                        continue;
                    }
                };
                if is_variant_null(&value).unwrap_or(true) {
                    builder.append_null();
                    continue;
                }
                let micros = match variant_to_datetime_micros(&value, tz)
                    .or_else(|_| variant_to_time_micros(&value, tz))
                {
                    Ok(v) => v,
                    Err(_) => {
                        builder.append_null();
                        continue;
                    }
                };
                builder.append_value(micros);
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        _ => Err("CAST from VARIANT is not supported".to_string()),
    }
}

fn parse_variant(variant_arr: &LargeBinaryArray, row: usize) -> Option<VariantValue> {
    if variant_arr.is_null(row) {
        return None;
    }
    VariantValue::from_serialized(variant_arr.value(row)).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::ids::SlotId;
    use crate::common::largeint;
    use crate::exec::chunk::{Chunk, field_with_slot_id};
    use crate::exec::expr::{ExprArena, ExprNode, LiteralValue};
    use arrow::array::{
        ArrayRef, Decimal128Array, Decimal256Array, FixedSizeBinaryArray, Int8Array, Int32Array,
        Int64Array, StructArray,
    };
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn chunk_len_1() -> Chunk {
        let array = Arc::new(Int64Array::from(vec![1])) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![field_with_slot_id(
            Field::new("dummy", DataType::Int64, false),
            SlotId::new(1),
        )]));
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
        Chunk::new(batch)
    }

    fn days_since_epoch(year: i32, month: u32, day: u32) -> i32 {
        let date = NaiveDate::from_ymd_opt(year, month, day).unwrap();
        date.num_days_from_ce() - UNIX_EPOCH_DAY_OFFSET
    }

    #[test]
    fn test_date32_to_float64_literal() {
        let days = days_since_epoch(1999, 1, 2);
        let arr = Date32Array::from(vec![Some(days)]);
        let out = cast_date32_to_float64(&arr).unwrap();
        let out = out.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(out.value(0), 19990102.0);
    }

    #[test]
    fn test_float64_to_date32_literal() {
        let arr = Float64Array::from(vec![Some(19990102.9)]);
        let out = cast_float64_to_date32(&arr).unwrap();
        let out = out.as_any().downcast_ref::<Date32Array>().unwrap();
        assert_eq!(out.value(0), days_since_epoch(1999, 1, 2));
    }

    #[test]
    fn test_sanitize_non_finite_float64_to_null() {
        let input = Arc::new(Float64Array::from(vec![
            Some(1.5),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
            Some(f64::NAN),
            None,
        ])) as ArrayRef;
        let out = sanitize_non_finite_cast_result(input, &DataType::Float64).unwrap();
        let out = out.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(out.value(0), 1.5);
        assert_eq!(out.value(1), f64::INFINITY);
        assert_eq!(out.value(2), f64::NEG_INFINITY);
        assert!(out.is_null(3));
        assert!(out.is_null(4));
    }

    #[test]
    fn test_sanitize_non_finite_float32_to_null() {
        let input = Arc::new(Float32Array::from(vec![
            Some(2.5),
            Some(f32::INFINITY),
            Some(f32::NEG_INFINITY),
            Some(f32::NAN),
            None,
        ])) as ArrayRef;
        let out = sanitize_non_finite_cast_result(input, &DataType::Float32).unwrap();
        let out = out.as_any().downcast_ref::<Float32Array>().unwrap();
        assert_eq!(out.value(0), 2.5);
        assert_eq!(out.value(1), f32::INFINITY);
        assert_eq!(out.value(2), f32::NEG_INFINITY);
        assert!(out.is_null(3));
        assert!(out.is_null(4));
    }

    #[test]
    fn test_cast_time_string_parsing_rules() {
        assert_eq!(parse_time_string_to_seconds("00:00:00"), Some(0));
        assert_eq!(parse_time_string_to_seconds("25:00:00"), Some(90_000));
        assert_eq!(parse_time_string_to_seconds("1970-01-01 01:01:01"), None);
        assert_eq!(parse_time_string_to_seconds("+01:02:03"), None);
        assert_eq!(parse_time_string_to_seconds("-01:02:03"), None);
    }

    #[test]
    fn test_cast_time_from_datetime_source_uses_datetime_parser() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();

        let literal = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("1970-01-01 01:01:01".to_string())),
            DataType::Utf8,
        );
        let cast_time = arena.push_typed(
            ExprNode::CastTime(literal),
            DataType::Timestamp(TimeUnit::Microsecond, None),
        );
        let direct = arena.eval(cast_time, &chunk).expect("cast time eval");
        let direct = direct
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("timestamp output");
        assert!(direct.is_null(0));

        let cast_time_from_dt = arena.push_typed(
            ExprNode::CastTimeFromDatetime(literal),
            DataType::Timestamp(TimeUnit::Microsecond, None),
        );
        let from_dt = arena
            .eval(cast_time_from_dt, &chunk)
            .expect("cast time from datetime eval");
        let from_dt = from_dt
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("timestamp output");
        assert!(!from_dt.is_null(0));
        assert_eq!(from_dt.value(0), 3_661_000_000);
    }

    #[test]
    fn test_cast_time_allows_extended_hour_literals() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();

        let literal = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("25:00:00".to_string())),
            DataType::Utf8,
        );
        let cast_time = arena.push_typed(
            ExprNode::CastTime(literal),
            DataType::Timestamp(TimeUnit::Microsecond, None),
        );
        let out = arena.eval(cast_time, &chunk).expect("cast time eval");
        let out = out
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("timestamp output");
        assert_eq!(out.value(0), 90_000_000_000);
    }

    #[test]
    fn test_parse_varchar_to_boolean_starrocks_semantics() {
        assert_eq!(parse_varchar_to_boolean_starrocks("2"), Some(true));
        assert_eq!(parse_varchar_to_boolean_starrocks("-1"), Some(true));
        assert_eq!(parse_varchar_to_boolean_starrocks("+0"), Some(false));
        assert_eq!(parse_varchar_to_boolean_starrocks(" 2"), Some(true));
        assert_eq!(parse_varchar_to_boolean_starrocks("2147483648"), None);
        assert_eq!(parse_varchar_to_boolean_starrocks("true"), Some(true));
        assert_eq!(parse_varchar_to_boolean_starrocks("FALSE"), Some(false));
        assert_eq!(parse_varchar_to_boolean_starrocks("t"), None);
        assert_eq!(parse_varchar_to_boolean_starrocks("f"), None);
        assert_eq!(parse_varchar_to_boolean_starrocks(""), None);
        assert_eq!(parse_varchar_to_boolean_starrocks("   "), None);
    }

    #[test]
    fn test_cast_utf8_to_boolean_array_starrocks_semantics() {
        let input = StringArray::from(vec![
            Some("2"),
            Some("-1"),
            Some("+0"),
            Some("false"),
            Some("TRUE"),
            Some("t"),
            Some("2147483648"),
            Some(""),
            None,
        ]);
        let out = cast_utf8_to_boolean_array(&input);
        let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();

        assert_eq!(out.value(0), true);
        assert_eq!(out.value(1), true);
        assert_eq!(out.value(2), false);
        assert_eq!(out.value(3), false);
        assert_eq!(out.value(4), true);
        assert!(out.is_null(5));
        assert!(out.is_null(6));
        assert!(out.is_null(7));
        assert!(out.is_null(8));
    }

    #[test]
    fn test_cast_decimal_utf8_empty_or_blank_returns_null() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();

        let empty = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("".to_string())),
            DataType::Utf8,
        );
        let blank = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("   ".to_string())),
            DataType::Utf8,
        );
        let zero = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("0".to_string())),
            DataType::Utf8,
        );

        let cast_empty = arena.push_typed(ExprNode::Cast(empty), DataType::Decimal128(10, 2));
        let cast_blank = arena.push_typed(ExprNode::Cast(blank), DataType::Decimal128(10, 2));
        let cast_zero = arena.push_typed(ExprNode::Cast(zero), DataType::Decimal128(10, 2));

        let empty_out = arena.eval(cast_empty, &chunk).unwrap();
        let empty_out = empty_out
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert!(empty_out.is_null(0));

        let blank_out = arena.eval(cast_blank, &chunk).unwrap();
        let blank_out = blank_out
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert!(blank_out.is_null(0));

        let zero_out = arena.eval(cast_zero, &chunk).unwrap();
        let zero_out = zero_out.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert!(!zero_out.is_null(0));
        assert_eq!(zero_out.value(0), 0);
    }

    #[test]
    fn test_cast_decimal_to_utf8_keeps_full_unscaled_value() {
        let wide = Decimal128Array::from(vec![
            Some(10_000_i128),
            Some(20_100_i128),
            Some(100_000_i128),
            Some(-10_000_i128),
            Some(1_100_i128),
            None,
        ])
        .with_precision_and_scale(38, 3)
        .unwrap();
        let data = wide
            .to_data()
            .into_builder()
            .data_type(DataType::Decimal128(4, 3))
            .build()
            .unwrap();
        let arr = make_array(data);

        let out = cast_with_special_rules(&arr, &DataType::Utf8).unwrap();
        let out = out.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(out.value(0), "10.000");
        assert_eq!(out.value(1), "20.100");
        assert_eq!(out.value(2), "100.000");
        assert_eq!(out.value(3), "-10.000");
        assert_eq!(out.value(4), "1.100");
        assert!(out.is_null(5));
    }

    #[test]
    fn test_cast_decimal256_to_utf8_preserves_unscaled_digits() {
        let arr = Arc::new(
            Decimal256Array::from(vec![
                Some(i256::from_i128(12_340)),
                Some(i256::from_i128(-500)),
                None,
            ])
            .with_precision_and_scale(76, 2)
            .unwrap(),
        ) as ArrayRef;
        let out = cast_with_special_rules(&arr, &DataType::Utf8).unwrap();
        let out = out.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(out.value(0), "123.40");
        assert_eq!(out.value(1), "-5.00");
        assert!(out.is_null(2));
    }

    #[test]
    fn test_cast_decimal256_to_largeint_binary_truncates_and_nulls_overflow() {
        let huge = i256::from_string("123456789012345678901234567890123456789012").unwrap();
        let arr = Arc::new(
            Decimal256Array::from(vec![
                Some(i256::from_i128(12_345)), // 123.45 -> 123
                Some(i256::from_i128(-5)),     // -0.05 -> 0
                Some(huge),                    // overflow after scale adjustment
                None,
            ])
            .with_precision_and_scale(50, 2)
            .unwrap(),
        ) as ArrayRef;
        let out = cast_with_special_rules(&arr, &DataType::FixedSizeBinary(16)).unwrap();
        let out = out.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
        assert_eq!(largeint::value_at(out, 0).unwrap(), 123_i128);
        assert_eq!(largeint::value_at(out, 1).unwrap(), 0_i128);
        assert!(out.is_null(2));
        assert!(out.is_null(3));
    }

    #[test]
    fn test_cast_decimal256_to_boolean_and_int64_semantics() {
        let arr = Arc::new(
            Decimal256Array::from(vec![
                Some(i256::from_i128(1)),     // 0.001
                Some(i256::ZERO),             // 0.000
                Some(i256::from_i128(-1)),    // -0.001
                Some(i256::from_i128(1_500)), // 1.500
                None,
            ])
            .with_precision_and_scale(20, 3)
            .unwrap(),
        ) as ArrayRef;

        let bool_out = cast_with_special_rules(&arr, &DataType::Boolean).unwrap();
        let bool_out = bool_out.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(bool_out.value(0), true);
        assert_eq!(bool_out.value(1), false);
        assert_eq!(bool_out.value(2), true);
        assert_eq!(bool_out.value(3), true);
        assert!(bool_out.is_null(4));

        let int_out = cast_with_special_rules(&arr, &DataType::Int64).unwrap();
        let int_out = int_out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(int_out.value(0), 0_i64);
        assert_eq!(int_out.value(1), 0_i64);
        assert_eq!(int_out.value(2), 0_i64);
        assert_eq!(int_out.value(3), 1_i64);
        assert!(int_out.is_null(4));
    }

    #[test]
    fn test_cast_decimal_to_decimal_downscale_rounds_half_up() {
        let arr = Arc::new(
            Decimal128Array::from(vec![
                Some(3_185_i128),  // 0.3185 -> 0.32
                Some(-3_185_i128), // -0.3185 -> -0.32
                Some(3_149_i128),  // 0.3149 -> 0.31
                Some(-3_149_i128), // -0.3149 -> -0.31
            ])
            .with_precision_and_scale(10, 4)
            .unwrap(),
        ) as ArrayRef;
        let out = cast_with_special_rules(&arr, &DataType::Decimal128(10, 2)).unwrap();
        let out = out.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(out.value(0), 32);
        assert_eq!(out.value(1), -32);
        assert_eq!(out.value(2), 31);
        assert_eq!(out.value(3), -31);
    }

    #[test]
    fn test_cast_largeint_binary_to_decimal128() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();

        let lit = arena.push_typed(
            ExprNode::Literal(LiteralValue::LargeInt(9_223_372_036_854_775_808_i128)),
            DataType::FixedSizeBinary(16),
        );
        let cast_expr = arena.push_typed(ExprNode::Cast(lit), DataType::Decimal128(38, 0));

        let out = arena.eval(cast_expr, &chunk).unwrap();
        let out = out.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(out.value(0), 9_223_372_036_854_775_808_i128);
    }

    #[test]
    fn test_cast_int16_to_largeint_binary() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();

        let lit = arena.push_typed(ExprNode::Literal(LiteralValue::Int16(7)), DataType::Int16);
        let cast_expr = arena.push_typed(ExprNode::Cast(lit), DataType::FixedSizeBinary(16));

        let out = arena.eval(cast_expr, &chunk).unwrap();
        let out = out.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
        assert_eq!(largeint::value_at(out, 0).unwrap(), 7_i128);
    }

    #[test]
    fn test_cast_utf8_to_largeint_binary() {
        let arr = Arc::new(StringArray::from(vec![
            Some("42"),
            Some("-17"),
            Some("  9000 "),
            Some("oops"),
            Some(""),
            None,
        ])) as ArrayRef;
        let out = cast_with_special_rules(&arr, &DataType::FixedSizeBinary(16)).unwrap();
        let out = out.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
        assert_eq!(largeint::value_at(out, 0).unwrap(), 42_i128);
        assert_eq!(largeint::value_at(out, 1).unwrap(), -17_i128);
        assert_eq!(largeint::value_at(out, 2).unwrap(), 9000_i128);
        assert!(out.is_null(3));
        assert!(out.is_null(4));
        assert!(out.is_null(5));
    }

    #[test]
    fn test_cast_float_to_decimal_rounds_like_starrocks() {
        let arr = Arc::new(Float64Array::from(vec![
            Some(0.31847),
            Some(0.315),
            Some(-0.315),
            Some(-0.314),
            None,
        ])) as ArrayRef;
        let out = cast_with_special_rules(&arr, &DataType::Decimal128(10, 2)).unwrap();
        let out = out.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(out.value(0), 32);
        assert_eq!(out.value(1), 32);
        assert_eq!(out.value(2), -32);
        assert_eq!(out.value(3), -31);
        assert!(out.is_null(4));
    }

    #[test]
    fn test_cast_float32_to_utf8_uses_compact_representation() {
        let arr = Float32Array::from(vec![
            Some(0.3690040111541748_f32),
            Some(1.5_f32),
            Some(-0.0),
        ]);
        let out = cast_float32_to_utf8_array(&arr);
        let out = out.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(out.value(0), "0.369004");
        assert_eq!(out.value(1), "1.5");
        assert_eq!(out.value(2), "0");
    }

    #[test]
    fn test_cast_float_overflow_respects_allow_throw_exception() {
        let chunk = chunk_len_1();

        let mut arena_default = ExprArena::default();
        let lit_default = arena_default.push_typed(
            ExprNode::Literal(LiteralValue::Float64(2_147_483_648.0)),
            DataType::Float64,
        );
        let cast_default = arena_default.push_typed(ExprNode::Cast(lit_default), DataType::Int32);
        let out = arena_default.eval(cast_default, &chunk).unwrap();
        let out = out.as_any().downcast_ref::<Int32Array>().unwrap();
        assert!(out.is_null(0));

        let mut arena_strict = ExprArena::default();
        arena_strict.set_allow_throw_exception(true);
        let lit_strict = arena_strict.push_typed(
            ExprNode::Literal(LiteralValue::Float64(2_147_483_648.0)),
            DataType::Float64,
        );
        let cast_strict = arena_strict.push_typed(ExprNode::Cast(lit_strict), DataType::Int32);
        let err = arena_strict.eval(cast_strict, &chunk).unwrap_err();
        assert!(err.contains("conflict with range of INT"));
    }

    #[test]
    fn test_cast_struct_decimal_to_int64_fields() {
        let first = Arc::new(
            Decimal128Array::from(vec![
                Some(9_223_372_036_854_775_807_i128),
                Some(-9_223_372_036_854_775_808_i128),
            ])
            .with_precision_and_scale(38, 0)
            .unwrap(),
        ) as ArrayRef;
        let second = Arc::new(Int8Array::from(vec![Some(1_i8), Some(-1_i8)])) as ArrayRef;
        let source_fields = Fields::from(vec![
            Field::new("col1", DataType::Decimal128(38, 0), true),
            Field::new("col2", DataType::Int8, true),
        ]);
        let source =
            Arc::new(StructArray::new(source_fields, vec![first, second], None)) as ArrayRef;

        let target = DataType::Struct(Fields::from(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]));
        let out = cast_with_special_rules(&source, &target).expect("cast struct");
        let out = out.as_any().downcast_ref::<StructArray>().expect("struct");
        let a = out
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("a int64");
        let b = out
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("b int64");
        assert_eq!(a.value(0), 9_223_372_036_854_775_807_i64);
        assert_eq!(a.value(1), -9_223_372_036_854_775_808_i64);
        assert_eq!(b.value(0), 1_i64);
        assert_eq!(b.value(1), -1_i64);
    }

    #[test]
    fn test_cast_largeint_binary_to_int64_overflow_to_null() {
        let source = largeint::array_from_i128(&[
            Some(i64::MAX as i128),
            Some(i64::MIN as i128),
            Some(i64::MAX as i128 + 1),
            None,
        ])
        .expect("largeint source");
        let out = cast_with_special_rules(&source, &DataType::Int64).expect("cast int64");
        let out = out.as_any().downcast_ref::<Int64Array>().expect("int64");
        assert_eq!(out.value(0), i64::MAX);
        assert_eq!(out.value(1), i64::MIN);
        assert!(out.is_null(2));
        assert!(out.is_null(3));
    }

    #[test]
    fn test_cast_struct_largeint_binary_to_int64_fields() {
        let first = largeint::array_from_i128(&[Some(9_i128), Some(-9_i128)]).expect("largeint");
        let second = Arc::new(Int8Array::from(vec![Some(1_i8), Some(-1_i8)])) as ArrayRef;
        let source_fields = Fields::from(vec![
            Field::new(
                "col1",
                DataType::FixedSizeBinary(largeint::LARGEINT_BYTE_WIDTH),
                true,
            ),
            Field::new("col2", DataType::Int8, true),
        ]);
        let source =
            Arc::new(StructArray::new(source_fields, vec![first, second], None)) as ArrayRef;

        let target = DataType::Struct(Fields::from(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]));
        let out = cast_with_special_rules(&source, &target).expect("cast struct");
        let out = out.as_any().downcast_ref::<StructArray>().expect("struct");
        let a = out
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("a int64");
        let b = out
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("b int64");
        assert_eq!(a.value(0), 9_i64);
        assert_eq!(a.value(1), -9_i64);
        assert_eq!(b.value(0), 1_i64);
        assert_eq!(b.value(1), -1_i64);
    }
}
