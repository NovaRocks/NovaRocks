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
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, Date32Array, Decimal128Array, Int32Array, Int64Array,
    StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::DataType;
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, Utc};

use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};

const UNIX_EPOCH_DAY_OFFSET: i32 = 719_163;
const MICROS_PER_SECOND: i64 = 1_000_000;
const MICROS_PER_HOUR: i64 = 3_600_000_000;
const MICROS_PER_DAY: i64 = 86_400_000_000;

pub fn eval_iceberg_identity(
    arena: &ExprArena,
    child: ExprId,
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    arena.eval(child, chunk)
}

pub fn eval_iceberg_void(
    arena: &ExprArena,
    _child: ExprId,
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let data_type = arena.data_type(_child).cloned().unwrap_or(DataType::Null);
    Ok(arrow::array::new_null_array(&data_type, chunk.len()))
}

pub fn eval_iceberg_year(
    arena: &ExprArena,
    child: ExprId,
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let array = arena.eval(child, chunk)?;
    let mut builder = arrow::array::Int64Builder::with_capacity(array.len());
    match array.data_type() {
        DataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| {
                    "iceberg_transform_year expects DATE for Date32 array".to_string()
                })?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let days = arr.value(i);
                let date = NaiveDate::from_num_days_from_ce_opt(UNIX_EPOCH_DAY_OFFSET + days)
                    .ok_or_else(|| format!("invalid Date32 value {days}"))?;
                let years = i64::from(date.year() - 1970);
                builder.append_value(years);
            }
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| {
                    "iceberg_transform_year expects DATETIME for Timestamp array".to_string()
                })?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let micros = arr.value(i);
                let dt = micros_to_naive_datetime(micros)?;
                let years = i64::from(dt.year() - 1970);
                builder.append_value(years);
            }
        }
        other => {
            return Err(format!(
                "iceberg_transform_year unsupported type: {other:?}"
            ));
        }
    }
    Ok(Arc::new(builder.finish()))
}

pub fn eval_iceberg_month(
    arena: &ExprArena,
    child: ExprId,
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let array = arena.eval(child, chunk)?;
    let mut builder = arrow::array::Int64Builder::with_capacity(array.len());
    match array.data_type() {
        DataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| {
                    "iceberg_transform_month expects DATE for Date32 array".to_string()
                })?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let days = arr.value(i);
                let date = NaiveDate::from_num_days_from_ce_opt(UNIX_EPOCH_DAY_OFFSET + days)
                    .ok_or_else(|| format!("invalid Date32 value {days}"))?;
                let years = date.year() - 1970;
                let months = years as i64 * 12 + i64::from(date.month() - 1);
                builder.append_value(months);
            }
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| {
                    "iceberg_transform_month expects DATETIME for Timestamp array".to_string()
                })?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let micros = arr.value(i);
                let dt = micros_to_naive_datetime(micros)?;
                let years = dt.year() - 1970;
                let months = years as i64 * 12 + i64::from(dt.month() - 1);
                builder.append_value(months);
            }
        }
        other => {
            return Err(format!(
                "iceberg_transform_month unsupported type: {other:?}"
            ));
        }
    }
    Ok(Arc::new(builder.finish()))
}

pub fn eval_iceberg_day(
    arena: &ExprArena,
    child: ExprId,
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let array = arena.eval(child, chunk)?;
    let mut builder = arrow::array::Int64Builder::with_capacity(array.len());
    match array.data_type() {
        DataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| "iceberg_transform_day expects DATE for Date32 array".to_string())?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let days = arr.value(i);
                builder.append_value(i64::from(days));
            }
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| {
                    "iceberg_transform_day expects DATETIME for Timestamp array".to_string()
                })?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let micros = arr.value(i);
                let days = micros.div_euclid(MICROS_PER_DAY);
                builder.append_value(days);
            }
        }
        other => {
            return Err(format!("iceberg_transform_day unsupported type: {other:?}"));
        }
    }
    Ok(Arc::new(builder.finish()))
}

pub fn eval_iceberg_hour(
    arena: &ExprArena,
    child: ExprId,
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let array = arena.eval(child, chunk)?;
    let mut builder = arrow::array::Int64Builder::with_capacity(array.len());
    match array.data_type() {
        DataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| {
                    "iceberg_transform_hour expects DATE for Date32 array".to_string()
                })?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let days = i64::from(arr.value(i));
                builder.append_value(days.saturating_mul(24));
            }
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| {
                    "iceberg_transform_hour expects DATETIME for Timestamp array".to_string()
                })?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let micros = arr.value(i);
                let hours = micros.div_euclid(MICROS_PER_HOUR);
                builder.append_value(hours);
            }
        }
        other => {
            return Err(format!(
                "iceberg_transform_hour unsupported type: {other:?}"
            ));
        }
    }
    Ok(Arc::new(builder.finish()))
}

pub fn eval_iceberg_bucket(
    arena: &ExprArena,
    value: ExprId,
    buckets: ExprId,
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let width = extract_const_i64(arena, buckets, chunk)?;
    if width <= 0 {
        return Err(format!(
            "iceberg_transform_bucket width must be > 0, got {width}"
        ));
    }
    let array = arena.eval(value, chunk)?;
    let mut builder = arrow::array::Int32Builder::with_capacity(array.len());
    match array.data_type() {
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "iceberg_transform_bucket expects INT".to_string())?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let v = arr.value(i) as i64;
                let h = murmur3_32(&v.to_le_bytes(), 0);
                let bucket = ((h & 0x7fffffff) as i64 % width) as i32;
                builder.append_value(bucket);
            }
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "iceberg_transform_bucket expects BIGINT".to_string())?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let v = arr.value(i);
                let h = murmur3_32(&v.to_le_bytes(), 0);
                let bucket = ((h & 0x7fffffff) as i64 % width) as i32;
                builder.append_value(bucket);
            }
        }
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "iceberg_transform_bucket expects VARCHAR".to_string())?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let v = arr.value(i).as_bytes();
                let h = murmur3_32(v, 0);
                let bucket = ((h & 0x7fffffff) as i64 % width) as i32;
                builder.append_value(bucket);
            }
        }
        DataType::Binary => {
            let arr = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| "iceberg_transform_bucket expects BINARY".to_string())?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let v = arr.value(i);
                let h = murmur3_32(v, 0);
                let bucket = ((h & 0x7fffffff) as i64 % width) as i32;
                builder.append_value(bucket);
            }
        }
        DataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| "iceberg_transform_bucket expects DATE".to_string())?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let days = i64::from(arr.value(i));
                let h = murmur3_32(&days.to_le_bytes(), 0);
                let bucket = ((h & 0x7fffffff) as i64 % width) as i32;
                builder.append_value(bucket);
            }
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| "iceberg_transform_bucket expects DATETIME".to_string())?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let micros = arr.value(i);
                let h = murmur3_32(&micros.to_le_bytes(), 0);
                let bucket = ((h & 0x7fffffff) as i64 % width) as i32;
                builder.append_value(bucket);
            }
        }
        DataType::Decimal128(_, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| "iceberg_transform_bucket expects DECIMAL".to_string())?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let v = arr.value(i);
                let bytes = decimal_to_bytes(v);
                let h = murmur3_32(&bytes, 0);
                let bucket = ((h & 0x7fffffff) as i64 % width) as i32;
                builder.append_value(bucket);
            }
        }
        other => {
            return Err(format!(
                "iceberg_transform_bucket unsupported type: {other:?}"
            ));
        }
    }
    Ok(Arc::new(builder.finish()))
}

pub fn eval_iceberg_truncate(
    arena: &ExprArena,
    value: ExprId,
    width: ExprId,
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let width = extract_const_i64(arena, width, chunk)?;
    if width <= 0 {
        return Err(format!(
            "iceberg_transform_truncate width must be > 0, got {width}"
        ));
    }
    let array = arena.eval(value, chunk)?;
    match array.data_type() {
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "iceberg_transform_truncate expects INT".to_string())?;
            let mut builder = arrow::array::Int32Builder::with_capacity(arr.len());
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let v = arr.value(i) as i64;
                let truncated = v - ((v % width) + width) % width;
                builder.append_value(truncated as i32);
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "iceberg_transform_truncate expects BIGINT".to_string())?;
            let mut builder = arrow::array::Int64Builder::with_capacity(arr.len());
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let v = arr.value(i);
                let truncated = v - ((v % width) + width) % width;
                builder.append_value(truncated);
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Decimal128(precision, scale) => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| "iceberg_transform_truncate expects DECIMAL".to_string())?;
            let mut builder = arrow::array::Decimal128Builder::with_capacity(arr.len())
                .with_data_type(DataType::Decimal128(*precision, *scale));
            let width = width as i128;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let v = arr.value(i);
                let truncated = v - ((v % width) + width) % width;
                builder.append_value(truncated);
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "iceberg_transform_truncate expects VARCHAR".to_string())?;
            let mut builder = arrow::array::StringBuilder::with_capacity(arr.len(), 0);
            let width = width as usize;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let v = arr.value(i);
                let truncated: String = v.chars().take(width).collect();
                builder.append_value(truncated);
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Binary => {
            let arr = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| "iceberg_transform_truncate expects BINARY".to_string())?;
            let mut builder = arrow::array::BinaryBuilder::with_capacity(arr.len(), 0);
            let width = width as usize;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let v = arr.value(i);
                let end = v.len().min(width);
                builder.append_value(&v[..end]);
            }
            Ok(Arc::new(builder.finish()))
        }
        other => Err(format!(
            "iceberg_transform_truncate unsupported type: {other:?}"
        )),
    }
}

fn extract_const_i64(arena: &ExprArena, expr: ExprId, chunk: &Chunk) -> Result<i64, String> {
    let array = arena.eval(expr, chunk)?;
    match array.data_type() {
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "expected INT for constant argument".to_string())?;
            if arr.is_null(0) {
                return Err("constant argument is NULL".to_string());
            }
            Ok(i64::from(arr.value(0)))
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "expected BIGINT for constant argument".to_string())?;
            if arr.is_null(0) {
                return Err("constant argument is NULL".to_string());
            }
            Ok(arr.value(0))
        }
        other => Err(format!(
            "constant argument must be INT/BIGINT, got {other:?}"
        )),
    }
}

fn micros_to_naive_datetime(micros: i64) -> Result<NaiveDateTime, String> {
    let secs = micros.div_euclid(MICROS_PER_SECOND);
    let rem = micros.rem_euclid(MICROS_PER_SECOND);
    let nanos = (rem as u32) * 1000;
    let dt = DateTime::<Utc>::from_timestamp(secs, nanos)
        .ok_or_else(|| format!("invalid datetime micros {micros}"))?;
    Ok(dt.naive_utc())
}

fn decimal_to_bytes(value: i128) -> Vec<u8> {
    let mut v = value;
    let mut bytes = value.to_le_bytes().to_vec();
    if v < 0 {
        v = !v;
    }
    let mut bit_len = 0;
    while v > 0 {
        v >>= 1;
        bit_len += 1;
    }
    let new_len = (bit_len / 8 + 1).max(1);
    bytes.resize(new_len, 0);
    bytes.reverse();
    bytes
}

fn murmur3_32(data: &[u8], seed: u32) -> u32 {
    const C1: u32 = 0xcc9e2d51;
    const C2: u32 = 0x1b873593;

    let mut hash = seed;
    let mut chunks = data.chunks_exact(4);
    for chunk in &mut chunks {
        let mut k = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        k = k.wrapping_mul(C1);
        k = k.rotate_left(15);
        k = k.wrapping_mul(C2);
        hash ^= k;
        hash = hash.rotate_left(13);
        hash = hash.wrapping_mul(5).wrapping_add(0xe6546b64);
    }

    let rem = chunks.remainder();
    let mut k1 = 0u32;
    match rem.len() {
        3 => {
            k1 ^= (rem[2] as u32) << 16;
            k1 ^= (rem[1] as u32) << 8;
            k1 ^= rem[0] as u32;
        }
        2 => {
            k1 ^= (rem[1] as u32) << 8;
            k1 ^= rem[0] as u32;
        }
        1 => {
            k1 ^= rem[0] as u32;
        }
        _ => {}
    }
    if k1 != 0 {
        k1 = k1.wrapping_mul(C1);
        k1 = k1.rotate_left(15);
        k1 = k1.wrapping_mul(C2);
        hash ^= k1;
    }

    hash ^= data.len() as u32;
    hash ^= hash >> 16;
    hash = hash.wrapping_mul(0x85ebca6b);
    hash ^= hash >> 13;
    hash = hash.wrapping_mul(0xc2b2ae35);
    hash ^= hash >> 16;
    hash
}
