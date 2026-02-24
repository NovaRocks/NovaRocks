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
//! Exact-value IN runtime filters.
//!
//! Responsibilities:
//! - Stores typed IN-filter values and probes rows via exact set-membership semantics.
//! - Provides local filter sets keyed by expr id for fast probe-time lookup.
//!
//! Key exported interfaces:
//! - Types: `RuntimeInFilter`, `LocalRuntimeInFilterSet`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow::compute::filter_record_batch;
use arrow::datatypes::DataType;
use hashbrown::HashSet;

use crate::common::ids::SlotId;
use crate::common::largeint;
use crate::exec::chunk::Chunk;
use crate::exec::expr::LiteralValue;
use crate::exec::node::join::JoinRuntimeFilterSpec;

#[derive(Clone, Debug)]
/// Typed IN runtime filter storing exact candidate values for one expression.
pub(crate) struct RuntimeInFilter {
    filter_id: i32,
    slot_id: SlotId,
    values: RuntimeInFilterValues,
}

#[derive(Clone, Debug)]
pub(in crate::exec::runtime_filter) enum RuntimeInFilterValues {
    Int8(HashSet<i8>),
    Int16(HashSet<i16>),
    Int32(HashSet<i32>),
    Int64(HashSet<i64>),
    LargeInt(HashSet<i128>),
    Float32(HashSet<u32>),
    Float64(HashSet<u64>),
    Bool(HashSet<bool>),
    Utf8(HashSet<String>),
    Date32(HashSet<i32>),
    TimestampSecond(HashSet<i64>),
    TimestampMillisecond(HashSet<i64>),
    TimestampMicrosecond(HashSet<i64>),
    TimestampNanosecond(HashSet<i64>),
    Decimal128 {
        values: HashSet<i128>,
        precision: u8,
        scale: i8,
    },
}

#[derive(Clone, Debug)]
/// Expression-indexed container of locally available runtime IN filters.
pub(crate) struct LocalRuntimeInFilterSet {
    filters: Vec<LocalRuntimeInFilter>,
}

#[derive(Clone, Debug)]
struct LocalRuntimeInFilter {
    filter_id: i32,
    expr_order: usize,
    slot_id: SlotId,
    values: RuntimeInFilterValues,
}

impl RuntimeInFilterValues {
    fn new(data_type: &DataType) -> Result<Self, String> {
        match data_type {
            DataType::Int8 => Ok(Self::Int8(HashSet::new())),
            DataType::Int16 => Ok(Self::Int16(HashSet::new())),
            DataType::Int32 => Ok(Self::Int32(HashSet::new())),
            DataType::Int64 => Ok(Self::Int64(HashSet::new())),
            DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
                Ok(Self::LargeInt(HashSet::new()))
            }
            DataType::Float32 => Ok(Self::Float32(HashSet::new())),
            DataType::Float64 => Ok(Self::Float64(HashSet::new())),
            DataType::Boolean => Ok(Self::Bool(HashSet::new())),
            DataType::Utf8 => Ok(Self::Utf8(HashSet::new())),
            DataType::Date32 => Ok(Self::Date32(HashSet::new())),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Second, _) => {
                Ok(Self::TimestampSecond(HashSet::new()))
            }
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, _) => {
                Ok(Self::TimestampMillisecond(HashSet::new()))
            }
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, _) => {
                Ok(Self::TimestampMicrosecond(HashSet::new()))
            }
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, _) => {
                Ok(Self::TimestampNanosecond(HashSet::new()))
            }
            DataType::Decimal128(precision, scale) => Ok(Self::Decimal128 {
                values: HashSet::new(),
                precision: *precision,
                scale: *scale,
            }),
            other => Err(format!("unsupported runtime in-filter type: {:?}", other)),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            RuntimeInFilterValues::Int8(values) => values.is_empty(),
            RuntimeInFilterValues::Int16(values) => values.is_empty(),
            RuntimeInFilterValues::Int32(values) => values.is_empty(),
            RuntimeInFilterValues::Int64(values) => values.is_empty(),
            RuntimeInFilterValues::LargeInt(values) => values.is_empty(),
            RuntimeInFilterValues::Float32(values) => values.is_empty(),
            RuntimeInFilterValues::Float64(values) => values.is_empty(),
            RuntimeInFilterValues::Bool(values) => values.is_empty(),
            RuntimeInFilterValues::Utf8(values) => values.is_empty(),
            RuntimeInFilterValues::Date32(values) => values.is_empty(),
            RuntimeInFilterValues::TimestampSecond(values) => values.is_empty(),
            RuntimeInFilterValues::TimestampMillisecond(values) => values.is_empty(),
            RuntimeInFilterValues::TimestampMicrosecond(values) => values.is_empty(),
            RuntimeInFilterValues::TimestampNanosecond(values) => values.is_empty(),
            RuntimeInFilterValues::Decimal128 { values, .. } => values.is_empty(),
        }
    }

    fn empty_like(&self) -> Self {
        match self {
            RuntimeInFilterValues::Int8(_) => RuntimeInFilterValues::Int8(HashSet::new()),
            RuntimeInFilterValues::Int16(_) => RuntimeInFilterValues::Int16(HashSet::new()),
            RuntimeInFilterValues::Int32(_) => RuntimeInFilterValues::Int32(HashSet::new()),
            RuntimeInFilterValues::Int64(_) => RuntimeInFilterValues::Int64(HashSet::new()),
            RuntimeInFilterValues::LargeInt(_) => RuntimeInFilterValues::LargeInt(HashSet::new()),
            RuntimeInFilterValues::Float32(_) => RuntimeInFilterValues::Float32(HashSet::new()),
            RuntimeInFilterValues::Float64(_) => RuntimeInFilterValues::Float64(HashSet::new()),
            RuntimeInFilterValues::Bool(_) => RuntimeInFilterValues::Bool(HashSet::new()),
            RuntimeInFilterValues::Utf8(_) => RuntimeInFilterValues::Utf8(HashSet::new()),
            RuntimeInFilterValues::Date32(_) => RuntimeInFilterValues::Date32(HashSet::new()),
            RuntimeInFilterValues::TimestampSecond(_) => {
                RuntimeInFilterValues::TimestampSecond(HashSet::new())
            }
            RuntimeInFilterValues::TimestampMillisecond(_) => {
                RuntimeInFilterValues::TimestampMillisecond(HashSet::new())
            }
            RuntimeInFilterValues::TimestampMicrosecond(_) => {
                RuntimeInFilterValues::TimestampMicrosecond(HashSet::new())
            }
            RuntimeInFilterValues::TimestampNanosecond(_) => {
                RuntimeInFilterValues::TimestampNanosecond(HashSet::new())
            }
            RuntimeInFilterValues::Decimal128 {
                precision, scale, ..
            } => RuntimeInFilterValues::Decimal128 {
                values: HashSet::new(),
                precision: *precision,
                scale: *scale,
            },
        }
    }

    fn insert_array(&mut self, array: &ArrayRef) -> Result<(), String> {
        match self {
            RuntimeInFilterValues::Int8(values) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .ok_or_else(|| "runtime in-filter type mismatch for Int8".to_string())?;
                for i in 0..arr.len() {
                    if arr.is_null(i) {
                        continue;
                    }
                    values.insert(arr.value(i));
                }
                Ok(())
            }
            RuntimeInFilterValues::Int16(values) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Int16Array>()
                    .ok_or_else(|| "runtime in-filter type mismatch for Int16".to_string())?;
                for i in 0..arr.len() {
                    if arr.is_null(i) {
                        continue;
                    }
                    values.insert(arr.value(i));
                }
                Ok(())
            }
            RuntimeInFilterValues::Int32(values) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or_else(|| "runtime in-filter type mismatch for Int32".to_string())?;
                for i in 0..arr.len() {
                    if arr.is_null(i) {
                        continue;
                    }
                    values.insert(arr.value(i));
                }
                Ok(())
            }
            RuntimeInFilterValues::Int64(values) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| "runtime in-filter type mismatch for Int64".to_string())?;
                for i in 0..arr.len() {
                    if arr.is_null(i) {
                        continue;
                    }
                    values.insert(arr.value(i));
                }
                Ok(())
            }
            RuntimeInFilterValues::LargeInt(values) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .ok_or_else(|| "runtime in-filter type mismatch for LargeInt".to_string())?;
                if arr.value_length() != largeint::LARGEINT_BYTE_WIDTH {
                    return Err("runtime in-filter type mismatch for LargeInt".to_string());
                }
                for i in 0..arr.len() {
                    if arr.is_null(i) {
                        continue;
                    }
                    values.insert(largeint::i128_from_be_bytes(arr.value(i))?);
                }
                Ok(())
            }
            RuntimeInFilterValues::Float32(values) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .ok_or_else(|| "runtime in-filter type mismatch for Float32".to_string())?;
                for i in 0..arr.len() {
                    if arr.is_null(i) {
                        continue;
                    }
                    values.insert(arr.value(i).to_bits());
                }
                Ok(())
            }
            RuntimeInFilterValues::Float64(values) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| "runtime in-filter type mismatch for Float64".to_string())?;
                for i in 0..arr.len() {
                    if arr.is_null(i) {
                        continue;
                    }
                    values.insert(arr.value(i).to_bits());
                }
                Ok(())
            }
            RuntimeInFilterValues::Bool(values) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| "runtime in-filter type mismatch for Boolean".to_string())?;
                for i in 0..arr.len() {
                    if arr.is_null(i) {
                        continue;
                    }
                    values.insert(arr.value(i));
                }
                Ok(())
            }
            RuntimeInFilterValues::Utf8(values) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| "runtime in-filter type mismatch for Utf8".to_string())?;
                for i in 0..arr.len() {
                    if arr.is_null(i) {
                        continue;
                    }
                    values.insert(arr.value(i).to_string());
                }
                Ok(())
            }
            RuntimeInFilterValues::Date32(values) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .ok_or_else(|| "runtime in-filter type mismatch for Date32".to_string())?;
                for i in 0..arr.len() {
                    if arr.is_null(i) {
                        continue;
                    }
                    values.insert(arr.value(i));
                }
                Ok(())
            }
            RuntimeInFilterValues::TimestampSecond(values) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .ok_or_else(|| {
                        "runtime in-filter type mismatch for TimestampSecond".to_string()
                    })?;
                for i in 0..arr.len() {
                    if arr.is_null(i) {
                        continue;
                    }
                    values.insert(arr.value(i));
                }
                Ok(())
            }
            RuntimeInFilterValues::TimestampMillisecond(values) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .ok_or_else(|| {
                        "runtime in-filter type mismatch for TimestampMillisecond".to_string()
                    })?;
                for i in 0..arr.len() {
                    if arr.is_null(i) {
                        continue;
                    }
                    values.insert(arr.value(i));
                }
                Ok(())
            }
            RuntimeInFilterValues::TimestampMicrosecond(values) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(|| {
                        "runtime in-filter type mismatch for TimestampMicrosecond".to_string()
                    })?;
                for i in 0..arr.len() {
                    if arr.is_null(i) {
                        continue;
                    }
                    values.insert(arr.value(i));
                }
                Ok(())
            }
            RuntimeInFilterValues::TimestampNanosecond(values) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .ok_or_else(|| {
                        "runtime in-filter type mismatch for TimestampNanosecond".to_string()
                    })?;
                for i in 0..arr.len() {
                    if arr.is_null(i) {
                        continue;
                    }
                    values.insert(arr.value(i));
                }
                Ok(())
            }
            RuntimeInFilterValues::Decimal128 {
                values,
                precision,
                scale,
            } => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .ok_or_else(|| "runtime in-filter type mismatch for Decimal128".to_string())?;
                let DataType::Decimal128(arr_precision, arr_scale) = arr.data_type() else {
                    return Err("runtime in-filter type mismatch for Decimal128".to_string());
                };
                if *arr_precision != *precision || *arr_scale != *scale {
                    return Err("runtime in-filter decimal type mismatch".to_string());
                }
                for i in 0..arr.len() {
                    if arr.is_null(i) {
                        continue;
                    }
                    values.insert(arr.value(i));
                }
                Ok(())
            }
        }
    }

    fn contains(&self, array: &ArrayRef, row: usize) -> Result<bool, String> {
        match self {
            RuntimeInFilterValues::Int8(values) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .ok_or_else(|| "runtime in-filter type mismatch for Int8".to_string())?;
                Ok(values.contains(&arr.value(row)))
            }
            RuntimeInFilterValues::Int16(values) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Int16Array>()
                    .ok_or_else(|| "runtime in-filter type mismatch for Int16".to_string())?;
                Ok(values.contains(&arr.value(row)))
            }
            RuntimeInFilterValues::Int32(values) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or_else(|| "runtime in-filter type mismatch for Int32".to_string())?;
                Ok(values.contains(&arr.value(row)))
            }
            RuntimeInFilterValues::Int64(values) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| "runtime in-filter type mismatch for Int64".to_string())?;
                Ok(values.contains(&arr.value(row)))
            }
            RuntimeInFilterValues::LargeInt(values) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .ok_or_else(|| "runtime in-filter type mismatch for LargeInt".to_string())?;
                if arr.value_length() != largeint::LARGEINT_BYTE_WIDTH {
                    return Err("runtime in-filter type mismatch for LargeInt".to_string());
                }
                let value = largeint::i128_from_be_bytes(arr.value(row))?;
                Ok(values.contains(&value))
            }
            RuntimeInFilterValues::Float32(values) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .ok_or_else(|| "runtime in-filter type mismatch for Float32".to_string())?;
                Ok(values.contains(&arr.value(row).to_bits()))
            }
            RuntimeInFilterValues::Float64(values) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| "runtime in-filter type mismatch for Float64".to_string())?;
                Ok(values.contains(&arr.value(row).to_bits()))
            }
            RuntimeInFilterValues::Bool(values) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| "runtime in-filter type mismatch for Boolean".to_string())?;
                Ok(values.contains(&arr.value(row)))
            }
            RuntimeInFilterValues::Utf8(values) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| "runtime in-filter type mismatch for Utf8".to_string())?;
                Ok(values.contains(arr.value(row)))
            }
            RuntimeInFilterValues::Date32(values) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .ok_or_else(|| "runtime in-filter type mismatch for Date32".to_string())?;
                Ok(values.contains(&arr.value(row)))
            }
            RuntimeInFilterValues::TimestampSecond(values) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .ok_or_else(|| {
                        "runtime in-filter type mismatch for TimestampSecond".to_string()
                    })?;
                Ok(values.contains(&arr.value(row)))
            }
            RuntimeInFilterValues::TimestampMillisecond(values) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .ok_or_else(|| {
                        "runtime in-filter type mismatch for TimestampMillisecond".to_string()
                    })?;
                Ok(values.contains(&arr.value(row)))
            }
            RuntimeInFilterValues::TimestampMicrosecond(values) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(|| {
                        "runtime in-filter type mismatch for TimestampMicrosecond".to_string()
                    })?;
                Ok(values.contains(&arr.value(row)))
            }
            RuntimeInFilterValues::TimestampNanosecond(values) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .ok_or_else(|| {
                        "runtime in-filter type mismatch for TimestampNanosecond".to_string()
                    })?;
                Ok(values.contains(&arr.value(row)))
            }
            RuntimeInFilterValues::Decimal128 {
                values,
                precision,
                scale,
            } => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .ok_or_else(|| "runtime in-filter type mismatch for Decimal128".to_string())?;
                let DataType::Decimal128(arr_precision, arr_scale) = arr.data_type() else {
                    return Err("runtime in-filter type mismatch for Decimal128".to_string());
                };
                if *arr_precision != *precision || *arr_scale != *scale {
                    return Err("runtime in-filter decimal type mismatch".to_string());
                }
                Ok(values.contains(&arr.value(row)))
            }
        }
    }

    fn merge_from(&mut self, other: &RuntimeInFilterValues) -> Result<(), String> {
        match (self, other) {
            (RuntimeInFilterValues::Int8(lhs), RuntimeInFilterValues::Int8(rhs)) => {
                lhs.extend(rhs.iter().copied());
                Ok(())
            }
            (RuntimeInFilterValues::Int16(lhs), RuntimeInFilterValues::Int16(rhs)) => {
                lhs.extend(rhs.iter().copied());
                Ok(())
            }
            (RuntimeInFilterValues::Int32(lhs), RuntimeInFilterValues::Int32(rhs)) => {
                lhs.extend(rhs.iter().copied());
                Ok(())
            }
            (RuntimeInFilterValues::Int64(lhs), RuntimeInFilterValues::Int64(rhs)) => {
                lhs.extend(rhs.iter().copied());
                Ok(())
            }
            (RuntimeInFilterValues::LargeInt(lhs), RuntimeInFilterValues::LargeInt(rhs)) => {
                lhs.extend(rhs.iter().copied());
                Ok(())
            }
            (RuntimeInFilterValues::Float32(lhs), RuntimeInFilterValues::Float32(rhs)) => {
                lhs.extend(rhs.iter().copied());
                Ok(())
            }
            (RuntimeInFilterValues::Float64(lhs), RuntimeInFilterValues::Float64(rhs)) => {
                lhs.extend(rhs.iter().copied());
                Ok(())
            }
            (RuntimeInFilterValues::Bool(lhs), RuntimeInFilterValues::Bool(rhs)) => {
                lhs.extend(rhs.iter().copied());
                Ok(())
            }
            (RuntimeInFilterValues::Utf8(lhs), RuntimeInFilterValues::Utf8(rhs)) => {
                lhs.extend(rhs.iter().cloned());
                Ok(())
            }
            (RuntimeInFilterValues::Date32(lhs), RuntimeInFilterValues::Date32(rhs)) => {
                lhs.extend(rhs.iter().copied());
                Ok(())
            }
            (
                RuntimeInFilterValues::TimestampSecond(lhs),
                RuntimeInFilterValues::TimestampSecond(rhs),
            ) => {
                lhs.extend(rhs.iter().copied());
                Ok(())
            }
            (
                RuntimeInFilterValues::TimestampMillisecond(lhs),
                RuntimeInFilterValues::TimestampMillisecond(rhs),
            ) => {
                lhs.extend(rhs.iter().copied());
                Ok(())
            }
            (
                RuntimeInFilterValues::TimestampMicrosecond(lhs),
                RuntimeInFilterValues::TimestampMicrosecond(rhs),
            ) => {
                lhs.extend(rhs.iter().copied());
                Ok(())
            }
            (
                RuntimeInFilterValues::TimestampNanosecond(lhs),
                RuntimeInFilterValues::TimestampNanosecond(rhs),
            ) => {
                lhs.extend(rhs.iter().copied());
                Ok(())
            }
            (
                RuntimeInFilterValues::Decimal128 {
                    values: lhs,
                    precision: lp,
                    scale: ls,
                },
                RuntimeInFilterValues::Decimal128 {
                    values: rhs,
                    precision: rp,
                    scale: rs,
                },
            ) => {
                if lp != rp || ls != rs {
                    return Err("runtime in-filter decimal type mismatch".to_string());
                }
                lhs.extend(rhs.iter().copied());
                Ok(())
            }
            _ => Err("runtime in-filter type mismatch".to_string()),
        }
    }

    fn min_max_literal(&self) -> Option<(LiteralValue, LiteralValue)> {
        match self {
            RuntimeInFilterValues::Int8(values) => min_max_i64(values.iter().map(|v| *v as i64))
                .map(|(min, max)| (LiteralValue::Int64(min), LiteralValue::Int64(max))),
            RuntimeInFilterValues::Int16(values) => min_max_i64(values.iter().map(|v| *v as i64))
                .map(|(min, max)| (LiteralValue::Int64(min), LiteralValue::Int64(max))),
            RuntimeInFilterValues::Int32(values) => min_max_i64(values.iter().map(|v| *v as i64))
                .map(|(min, max)| (LiteralValue::Int64(min), LiteralValue::Int64(max))),
            RuntimeInFilterValues::Int64(values) => min_max_i64(values.iter().map(|v| *v))
                .map(|(min, max)| (LiteralValue::Int64(min), LiteralValue::Int64(max))),
            RuntimeInFilterValues::LargeInt(values) => min_max_i128(values.iter().copied())
                .map(|(min, max)| (LiteralValue::LargeInt(min), LiteralValue::LargeInt(max))),
            RuntimeInFilterValues::Date32(values) => min_max_i64(values.iter().map(|v| *v as i64))
                .map(|(min, max)| (LiteralValue::Int64(min), LiteralValue::Int64(max))),
            RuntimeInFilterValues::TimestampSecond(values)
            | RuntimeInFilterValues::TimestampMillisecond(values)
            | RuntimeInFilterValues::TimestampMicrosecond(values)
            | RuntimeInFilterValues::TimestampNanosecond(values) => {
                min_max_i64(values.iter().map(|v| *v))
                    .map(|(min, max)| (LiteralValue::Int64(min), LiteralValue::Int64(max)))
            }
            RuntimeInFilterValues::Float32(values) => {
                min_max_f64(values.iter().map(|v| f32::from_bits(*v) as f64))
                    .map(|(min, max)| (LiteralValue::Float64(min), LiteralValue::Float64(max)))
            }
            RuntimeInFilterValues::Float64(values) => {
                min_max_f64(values.iter().map(|v| f64::from_bits(*v)))
                    .map(|(min, max)| (LiteralValue::Float64(min), LiteralValue::Float64(max)))
            }
            RuntimeInFilterValues::Bool(_) => None,
            RuntimeInFilterValues::Utf8(_) => None,
            RuntimeInFilterValues::Decimal128 { .. } => None,
        }
    }
}

fn min_max_i64<I>(iter: I) -> Option<(i64, i64)>
where
    I: IntoIterator<Item = i64>,
{
    let mut it = iter.into_iter();
    let first = it.next()?;
    let mut min = first;
    let mut max = first;
    for v in it {
        if v < min {
            min = v;
        }
        if v > max {
            max = v;
        }
    }
    Some((min, max))
}

fn min_max_i128<I>(iter: I) -> Option<(i128, i128)>
where
    I: IntoIterator<Item = i128>,
{
    let mut it = iter.into_iter();
    let first = it.next()?;
    let mut min = first;
    let mut max = first;
    for v in it {
        if v < min {
            min = v;
        }
        if v > max {
            max = v;
        }
    }
    Some((min, max))
}

fn min_max_f64<I>(iter: I) -> Option<(f64, f64)>
where
    I: IntoIterator<Item = f64>,
{
    let mut min: Option<f64> = None;
    let mut max: Option<f64> = None;
    for v in iter {
        if v.is_nan() {
            continue;
        }
        min = Some(match min {
            Some(cur) => cur.min(v),
            None => v,
        });
        max = Some(match max {
            Some(cur) => cur.max(v),
            None => v,
        });
    }
    match (min, max) {
        (Some(min), Some(max)) => Some((min, max)),
        _ => None,
    }
}

impl RuntimeInFilter {
    pub(in crate::exec::runtime_filter) fn new(
        filter_id: i32,
        slot_id: SlotId,
        values: RuntimeInFilterValues,
    ) -> Self {
        Self {
            filter_id,
            slot_id,
            values,
        }
    }

    pub(crate) fn empty(
        filter_id: i32,
        slot_id: SlotId,
        data_type: &DataType,
    ) -> Result<Self, String> {
        let values = RuntimeInFilterValues::new(data_type)?;
        Ok(Self {
            filter_id,
            slot_id,
            values,
        })
    }

    pub(crate) fn filter_id(&self) -> i32 {
        self.filter_id
    }

    pub(crate) fn slot_id(&self) -> SlotId {
        self.slot_id
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub(crate) fn min_max_literal(&self) -> Option<(LiteralValue, LiteralValue)> {
        self.values.min_max_literal()
    }

    pub(in crate::exec::runtime_filter) fn values(&self) -> &RuntimeInFilterValues {
        &self.values
    }

    pub(in crate::exec::runtime_filter) fn contains(
        &self,
        array: &ArrayRef,
        row: usize,
    ) -> Result<bool, String> {
        self.values.contains(array, row)
    }
}

impl LocalRuntimeInFilterSet {
    pub(crate) fn new(
        specs: &[JoinRuntimeFilterSpec],
        key_arrays: &[ArrayRef],
    ) -> Result<Self, String> {
        let mut filters = Vec::with_capacity(specs.len());
        for spec in specs {
            let Some(array) = key_arrays.get(spec.expr_order) else {
                return Err(format!(
                    "runtime filter {} expects build key index {} but only {} keys are available",
                    spec.filter_id,
                    spec.expr_order,
                    key_arrays.len()
                ));
            };
            let values = RuntimeInFilterValues::new(array.data_type())?;
            filters.push(LocalRuntimeInFilter {
                filter_id: spec.filter_id,
                expr_order: spec.expr_order,
                slot_id: spec.probe_slot_id,
                values,
            });
        }
        Ok(Self { filters })
    }

    pub(crate) fn add_build_arrays(&mut self, key_arrays: &[ArrayRef]) -> Result<(), String> {
        if self.filters.is_empty() {
            return Ok(());
        }
        for filter in &mut self.filters {
            let Some(array) = key_arrays.get(filter.expr_order) else {
                return Err(format!(
                    "runtime filter {} expects build key index {} but only {} keys are available",
                    filter.filter_id,
                    filter.expr_order,
                    key_arrays.len()
                ));
            };
            filter.values.insert_array(array)?;
        }
        Ok(())
    }

    pub(crate) fn into_filters(self) -> Vec<RuntimeInFilter> {
        self.filters
            .into_iter()
            .map(|filter| RuntimeInFilter {
                filter_id: filter.filter_id,
                slot_id: filter.slot_id,
                values: filter.values,
            })
            .collect()
    }
}

impl RuntimeInFilter {
    pub(crate) fn merge_from(&mut self, other: &RuntimeInFilter) -> Result<(), String> {
        if self.filter_id != other.filter_id || self.slot_id != other.slot_id {
            return Err("runtime in-filter metadata mismatch".to_string());
        }
        self.values.merge_from(&other.values)
    }

    pub(crate) fn with_slot_id(&self, slot_id: SlotId) -> Self {
        if self.slot_id == slot_id {
            return self.clone();
        }
        Self {
            filter_id: self.filter_id,
            slot_id,
            values: self.values.clone(),
        }
    }

    pub(crate) fn empty_like(&self) -> Self {
        Self {
            filter_id: self.filter_id,
            slot_id: self.slot_id,
            values: self.values.empty_like(),
        }
    }

    pub(crate) fn filter_chunk_with_array(
        &self,
        array: &ArrayRef,
        chunk: Chunk,
    ) -> Result<Option<Chunk>, String> {
        if self.is_empty() {
            return Ok(Some(chunk));
        }
        if chunk.is_empty() {
            return Ok(Some(chunk));
        }
        if array.len() != chunk.len() {
            return Err("runtime in-filter array length mismatch".to_string());
        }
        let len = chunk.len();
        let mut keep = vec![true; len];
        for row in 0..len {
            if !keep[row] {
                continue;
            }
            if array.is_null(row) {
                keep[row] = false;
                continue;
            }
            if !self.values.contains(array, row)? {
                keep[row] = false;
            }
        }
        if keep.iter().all(|v| *v) {
            return Ok(Some(chunk));
        }
        if keep.iter().all(|v| !*v) {
            return Ok(None);
        }
        let mask = BooleanArray::from(keep);
        let filtered_batch = filter_record_batch(&chunk.batch, &mask).map_err(|e| e.to_string())?;
        Ok(Some(Chunk::new(filtered_batch)))
    }
}
