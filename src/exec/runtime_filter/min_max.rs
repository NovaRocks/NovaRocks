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
//! Runtime min-max filter implementation.
//!
//! Responsibilities:
//! - Tracks typed lower/upper bounds and probes literals/rows against range constraints.
//! - Used for lightweight range pruning when bloom/IN filters are unavailable.
//!
//! Key exported interfaces:
//! - Types: `MinMaxValue`, `RuntimeMinMaxFilter`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::cmp::Ordering;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};

use crate::common::largeint;
use crate::exec::expr::LiteralValue;
use crate::types::TPrimitiveType;

use super::codec::{read_i8, read_i16_le, read_i32_le, read_i64_le, read_u8, read_u64_le};

#[derive(Clone, Debug)]
/// Typed bound values used by runtime min-max filters.
pub(crate) enum MinMaxValue {
    Bool(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    LargeInt(i128),
    Float32(f32),
    Float64(f64),
    Date32(i32),
    Timestamp(i64),
    Utf8(String),
    Decimal128(i128),
}

impl MinMaxValue {
    fn cmp(&self, other: &MinMaxValue) -> Result<Ordering, String> {
        match (self, other) {
            (MinMaxValue::Bool(a), MinMaxValue::Bool(b)) => Ok(a.cmp(b)),
            (MinMaxValue::Int8(a), MinMaxValue::Int8(b)) => Ok(a.cmp(b)),
            (MinMaxValue::Int16(a), MinMaxValue::Int16(b)) => Ok(a.cmp(b)),
            (MinMaxValue::Int32(a), MinMaxValue::Int32(b)) => Ok(a.cmp(b)),
            (MinMaxValue::Int64(a), MinMaxValue::Int64(b)) => Ok(a.cmp(b)),
            (MinMaxValue::LargeInt(a), MinMaxValue::LargeInt(b)) => Ok(a.cmp(b)),
            (MinMaxValue::Float32(a), MinMaxValue::Float32(b)) => a
                .partial_cmp(b)
                .ok_or_else(|| "runtime min/max float32 compare failed".to_string()),
            (MinMaxValue::Float64(a), MinMaxValue::Float64(b)) => a
                .partial_cmp(b)
                .ok_or_else(|| "runtime min/max float64 compare failed".to_string()),
            (MinMaxValue::Date32(a), MinMaxValue::Date32(b)) => Ok(a.cmp(b)),
            (MinMaxValue::Timestamp(a), MinMaxValue::Timestamp(b)) => Ok(a.cmp(b)),
            (MinMaxValue::Utf8(a), MinMaxValue::Utf8(b)) => Ok(a.as_bytes().cmp(b.as_bytes())),
            (MinMaxValue::Decimal128(a), MinMaxValue::Decimal128(b)) => Ok(a.cmp(b)),
            _ => Err("runtime min/max value type mismatch".to_string()),
        }
    }
}

#[derive(Clone, Debug)]
/// Runtime range filter that prunes rows using typed lower/upper bounds.
pub(crate) struct RuntimeMinMaxFilter {
    ltype: TPrimitiveType,
    has_min_max: bool,
    min: MinMaxValue,
    max: MinMaxValue,
}

impl RuntimeMinMaxFilter {
    pub(crate) fn new(
        ltype: TPrimitiveType,
        has_min_max: bool,
        min: MinMaxValue,
        max: MinMaxValue,
    ) -> Self {
        Self {
            ltype,
            has_min_max,
            min,
            max,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn has_min_max(&self) -> bool {
        self.has_min_max
    }

    #[allow(dead_code)]
    pub(crate) fn min(&self) -> &MinMaxValue {
        &self.min
    }

    #[allow(dead_code)]
    pub(crate) fn max(&self) -> &MinMaxValue {
        &self.max
    }

    pub(crate) fn min_max_literal(&self) -> Option<(LiteralValue, LiteralValue)> {
        if !self.has_min_max {
            return None;
        }
        let to_literal = |ltype: TPrimitiveType, value: &MinMaxValue| -> Option<LiteralValue> {
            match (ltype, value) {
                (TPrimitiveType::BOOLEAN, MinMaxValue::Bool(v)) => Some(LiteralValue::Bool(*v)),
                (TPrimitiveType::TINYINT, MinMaxValue::Int8(v)) => Some(LiteralValue::Int8(*v)),
                (TPrimitiveType::SMALLINT, MinMaxValue::Int16(v)) => Some(LiteralValue::Int16(*v)),
                (TPrimitiveType::INT, MinMaxValue::Int32(v)) => Some(LiteralValue::Int32(*v)),
                (TPrimitiveType::BIGINT, MinMaxValue::Int64(v)) => Some(LiteralValue::Int64(*v)),
                (TPrimitiveType::LARGEINT, MinMaxValue::LargeInt(v)) => {
                    Some(LiteralValue::LargeInt(*v))
                }
                (TPrimitiveType::FLOAT, MinMaxValue::Float32(v)) => Some(LiteralValue::Float32(*v)),
                (TPrimitiveType::DOUBLE, MinMaxValue::Float64(v)) => {
                    Some(LiteralValue::Float64(*v))
                }
                (TPrimitiveType::DATE, MinMaxValue::Date32(v)) => Some(LiteralValue::Date32(*v)),
                (t, MinMaxValue::Utf8(v)) if is_utf8_type(&t) => {
                    Some(LiteralValue::Utf8(v.clone()))
                }
                _ => None,
            }
        };
        let min = to_literal(self.ltype, &self.min)?;
        let max = to_literal(self.ltype, &self.max)?;
        Some((min, max))
    }

    pub(crate) fn full_range(ltype: TPrimitiveType) -> Result<Self, String> {
        let (min, max) = full_range_values(&ltype)?;
        Ok(Self::new(ltype, true, min, max))
    }

    pub(crate) fn empty_range(ltype: TPrimitiveType) -> Result<Self, String> {
        let (min, max) = empty_range_values(&ltype)?;
        Ok(Self::new(ltype, true, min, max))
    }

    pub(crate) fn from_arrays(ltype: TPrimitiveType, arrays: &[ArrayRef]) -> Result<Self, String> {
        if arrays.is_empty() {
            return Self::empty_range(ltype);
        }
        let mut min: Option<MinMaxValue> = None;
        let mut max: Option<MinMaxValue> = None;
        for array in arrays {
            match ltype {
                t if t == TPrimitiveType::BOOLEAN => {
                    let arr = as_array::<BooleanArray>(array, "Boolean")?;
                    for i in 0..arr.len() {
                        if arr.is_null(i) {
                            continue;
                        }
                        let value = MinMaxValue::Bool(arr.value(i));
                        update_min_max(value, &mut min, &mut max)?;
                    }
                }
                t if t == TPrimitiveType::TINYINT => {
                    let arr = as_array::<Int8Array>(array, "Int8")?;
                    for i in 0..arr.len() {
                        if arr.is_null(i) {
                            continue;
                        }
                        let value = MinMaxValue::Int8(arr.value(i));
                        update_min_max(value, &mut min, &mut max)?;
                    }
                }
                t if t == TPrimitiveType::SMALLINT => {
                    let arr = as_array::<Int16Array>(array, "Int16")?;
                    for i in 0..arr.len() {
                        if arr.is_null(i) {
                            continue;
                        }
                        let value = MinMaxValue::Int16(arr.value(i));
                        update_min_max(value, &mut min, &mut max)?;
                    }
                }
                t if t == TPrimitiveType::INT => {
                    let arr = as_array::<Int32Array>(array, "Int32")?;
                    for i in 0..arr.len() {
                        if arr.is_null(i) {
                            continue;
                        }
                        let value = MinMaxValue::Int32(arr.value(i));
                        update_min_max(value, &mut min, &mut max)?;
                    }
                }
                t if t == TPrimitiveType::BIGINT => {
                    let arr = as_array::<Int64Array>(array, "Int64")?;
                    for i in 0..arr.len() {
                        if arr.is_null(i) {
                            continue;
                        }
                        let value = MinMaxValue::Int64(arr.value(i));
                        update_min_max(value, &mut min, &mut max)?;
                    }
                }
                t if t == TPrimitiveType::LARGEINT => {
                    let arr = as_array::<FixedSizeBinaryArray>(array, "FixedSizeBinary(16)")?;
                    if arr.value_length() != largeint::LARGEINT_BYTE_WIDTH {
                        return Err(
                            "runtime min/max type mismatch for FixedSizeBinary(16)".to_string()
                        );
                    }
                    for i in 0..arr.len() {
                        if arr.is_null(i) {
                            continue;
                        }
                        let value =
                            MinMaxValue::LargeInt(largeint::i128_from_be_bytes(arr.value(i))?);
                        update_min_max(value, &mut min, &mut max)?;
                    }
                }
                t if t == TPrimitiveType::FLOAT => {
                    let arr = as_array::<Float32Array>(array, "Float32")?;
                    for i in 0..arr.len() {
                        if arr.is_null(i) {
                            continue;
                        }
                        let v = arr.value(i);
                        if v.is_nan() {
                            continue;
                        }
                        let value = MinMaxValue::Float32(v);
                        update_min_max(value, &mut min, &mut max)?;
                    }
                }
                t if t == TPrimitiveType::DOUBLE => {
                    let arr = as_array::<Float64Array>(array, "Float64")?;
                    for i in 0..arr.len() {
                        if arr.is_null(i) {
                            continue;
                        }
                        let v = arr.value(i);
                        if v.is_nan() {
                            continue;
                        }
                        let value = MinMaxValue::Float64(v);
                        update_min_max(value, &mut min, &mut max)?;
                    }
                }
                t if t == TPrimitiveType::DATE => {
                    let arr = as_array::<Date32Array>(array, "Date32")?;
                    for i in 0..arr.len() {
                        if arr.is_null(i) {
                            continue;
                        }
                        let value = MinMaxValue::Date32(arr.value(i));
                        update_min_max(value, &mut min, &mut max)?;
                    }
                }
                t if t == TPrimitiveType::DATETIME || t == TPrimitiveType::TIME => {
                    let (values, unit) = timestamp_values(array)?;
                    for (value, is_null) in values {
                        if is_null {
                            continue;
                        }
                        let value = MinMaxValue::Timestamp(to_microseconds(value, unit));
                        update_min_max(value, &mut min, &mut max)?;
                    }
                }
                t if is_utf8_type(&t) => {
                    let arr = as_array::<StringArray>(array, "Utf8")?;
                    for i in 0..arr.len() {
                        if arr.is_null(i) {
                            continue;
                        }
                        let value = MinMaxValue::Utf8(arr.value(i).to_string());
                        update_min_max(value, &mut min, &mut max)?;
                    }
                }
                t if is_decimal_type(&t) => {
                    let arr = as_array::<Decimal128Array>(array, "Decimal128")?;
                    for i in 0..arr.len() {
                        if arr.is_null(i) {
                            continue;
                        }
                        let value = MinMaxValue::Decimal128(arr.value(i));
                        update_min_max(value, &mut min, &mut max)?;
                    }
                }
                _ => return Err(format!("unsupported runtime min/max type: {:?}", ltype)),
            }
        }
        let Some(min) = min else {
            return Self::empty_range(ltype);
        };
        let Some(max) = max else {
            return Self::empty_range(ltype);
        };
        Ok(Self::new(ltype, true, min, max))
    }

    pub(crate) fn merge_from(&mut self, other: &RuntimeMinMaxFilter) -> Result<(), String> {
        if self.ltype != other.ltype {
            return Err("runtime min/max filter type mismatch".to_string());
        }
        if !self.has_min_max && other.has_min_max {
            self.has_min_max = true;
            self.min = other.min.clone();
            self.max = other.max.clone();
            return Ok(());
        }
        if !other.has_min_max {
            return Ok(());
        }
        if self.min.cmp(&other.min)? == Ordering::Greater {
            self.min = other.min.clone();
        }
        if self.max.cmp(&other.max)? == Ordering::Less {
            self.max = other.max.clone();
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn is_empty_range(&self) -> Result<bool, String> {
        if !self.has_min_max {
            return Ok(false);
        }
        Ok(self.min.cmp(&self.max)? == Ordering::Greater)
    }

    pub(crate) fn apply_to_array(
        &self,
        array: &ArrayRef,
        has_null: bool,
        check_null: bool,
        keep: &mut [bool],
    ) -> Result<(), String> {
        if !self.has_min_max {
            return Ok(());
        }
        if keep.len() != array.len() {
            return Err("runtime min/max selection size mismatch".to_string());
        }
        match self.ltype {
            t if t == TPrimitiveType::BOOLEAN => {
                let (min, max) = self.bool_range()?;
                let arr = as_array::<BooleanArray>(array, "Boolean")?;
                for i in 0..arr.len() {
                    if !keep[i] {
                        continue;
                    }
                    if arr.is_null(i) {
                        if check_null {
                            keep[i] = has_null;
                        }
                        continue;
                    }
                    let v = arr.value(i);
                    if v < min || v > max {
                        keep[i] = false;
                    }
                }
            }
            t if t == TPrimitiveType::TINYINT => {
                let (min, max) = self.i8_range()?;
                let arr = as_array::<Int8Array>(array, "Int8")?;
                for i in 0..arr.len() {
                    if !keep[i] {
                        continue;
                    }
                    if arr.is_null(i) {
                        if check_null {
                            keep[i] = has_null;
                        }
                        continue;
                    }
                    let v = arr.value(i);
                    if v < min || v > max {
                        keep[i] = false;
                    }
                }
            }
            t if t == TPrimitiveType::SMALLINT => {
                let (min, max) = self.i16_range()?;
                let arr = as_array::<Int16Array>(array, "Int16")?;
                for i in 0..arr.len() {
                    if !keep[i] {
                        continue;
                    }
                    if arr.is_null(i) {
                        if check_null {
                            keep[i] = has_null;
                        }
                        continue;
                    }
                    let v = arr.value(i);
                    if v < min || v > max {
                        keep[i] = false;
                    }
                }
            }
            t if t == TPrimitiveType::INT => {
                let (min, max) = self.i32_range()?;
                let arr = as_array::<Int32Array>(array, "Int32")?;
                for i in 0..arr.len() {
                    if !keep[i] {
                        continue;
                    }
                    if arr.is_null(i) {
                        if check_null {
                            keep[i] = has_null;
                        }
                        continue;
                    }
                    let v = arr.value(i);
                    if v < min || v > max {
                        keep[i] = false;
                    }
                }
            }
            t if t == TPrimitiveType::BIGINT => {
                let (min, max) = self.i64_range()?;
                let arr = as_array::<Int64Array>(array, "Int64")?;
                for i in 0..arr.len() {
                    if !keep[i] {
                        continue;
                    }
                    if arr.is_null(i) {
                        if check_null {
                            keep[i] = has_null;
                        }
                        continue;
                    }
                    let v = arr.value(i);
                    if v < min || v > max {
                        keep[i] = false;
                    }
                }
            }
            t if t == TPrimitiveType::LARGEINT => {
                let (min, max) = self.i128_range()?;
                let arr = as_array::<FixedSizeBinaryArray>(array, "FixedSizeBinary(16)")?;
                if arr.value_length() != largeint::LARGEINT_BYTE_WIDTH {
                    return Err("runtime min/max type mismatch for FixedSizeBinary(16)".to_string());
                }
                for i in 0..arr.len() {
                    if !keep[i] {
                        continue;
                    }
                    if arr.is_null(i) {
                        if check_null {
                            keep[i] = has_null;
                        }
                        continue;
                    }
                    let v = largeint::i128_from_be_bytes(arr.value(i))?;
                    if v < min || v > max {
                        keep[i] = false;
                    }
                }
            }
            t if t == TPrimitiveType::FLOAT => {
                let (min, max) = self.f32_range()?;
                let arr = as_array::<Float32Array>(array, "Float32")?;
                for i in 0..arr.len() {
                    if !keep[i] {
                        continue;
                    }
                    if arr.is_null(i) {
                        if check_null {
                            keep[i] = has_null;
                        }
                        continue;
                    }
                    let v = arr.value(i);
                    if v.is_nan() || v < min || v > max {
                        keep[i] = false;
                    }
                }
            }
            t if t == TPrimitiveType::DOUBLE => {
                let (min, max) = self.f64_range()?;
                let arr = as_array::<Float64Array>(array, "Float64")?;
                for i in 0..arr.len() {
                    if !keep[i] {
                        continue;
                    }
                    if arr.is_null(i) {
                        if check_null {
                            keep[i] = has_null;
                        }
                        continue;
                    }
                    let v = arr.value(i);
                    if v.is_nan() || v < min || v > max {
                        keep[i] = false;
                    }
                }
            }
            t if t == TPrimitiveType::DATE => {
                let (min, max) = self.date32_range()?;
                let arr = as_array::<Date32Array>(array, "Date32")?;
                for i in 0..arr.len() {
                    if !keep[i] {
                        continue;
                    }
                    if arr.is_null(i) {
                        if check_null {
                            keep[i] = has_null;
                        }
                        continue;
                    }
                    let v = arr.value(i);
                    if v < min || v > max {
                        keep[i] = false;
                    }
                }
            }
            t if t == TPrimitiveType::DATETIME || t == TPrimitiveType::TIME => {
                let (min, max) = self.timestamp_range()?;
                let (values, unit) = timestamp_values(array)?;
                for (idx, (value, is_null)) in values.iter().enumerate() {
                    if !keep[idx] {
                        continue;
                    }
                    if *is_null {
                        if check_null {
                            keep[idx] = has_null;
                        }
                        continue;
                    }
                    let v = to_microseconds(*value, unit);
                    if v < min || v > max {
                        keep[idx] = false;
                    }
                }
            }
            t if is_utf8_type(&t) => {
                let (min, max) = self.utf8_range()?;
                let arr = as_array::<StringArray>(array, "Utf8")?;
                let min_bytes = min.as_bytes();
                let max_bytes = max.as_bytes();
                for i in 0..arr.len() {
                    if !keep[i] {
                        continue;
                    }
                    if arr.is_null(i) {
                        if check_null {
                            keep[i] = has_null;
                        }
                        continue;
                    }
                    let bytes = arr.value(i).as_bytes();
                    if bytes < min_bytes || bytes > max_bytes {
                        keep[i] = false;
                    }
                }
            }
            t if is_decimal_type(&t) => {
                let (min, max) = self.decimal_range()?;
                let arr = as_array::<Decimal128Array>(array, "Decimal128")?;
                for i in 0..arr.len() {
                    if !keep[i] {
                        continue;
                    }
                    if arr.is_null(i) {
                        if check_null {
                            keep[i] = has_null;
                        }
                        continue;
                    }
                    let v = arr.value(i);
                    if v < min || v > max {
                        keep[i] = false;
                    }
                }
            }
            _ => {
                return Err(format!(
                    "unsupported runtime min/max type: {:?}",
                    self.ltype
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn encode_into(&self, buf: &mut Vec<u8>) -> Result<(), String> {
        buf.push(if self.has_min_max { 1 } else { 0 });
        encode_value(&self.ltype, &self.min, buf)?;
        encode_value(&self.ltype, &self.max, buf)?;
        Ok(())
    }

    pub(crate) fn decode(
        ltype: TPrimitiveType,
        data: &[u8],
        offset: &mut usize,
    ) -> Result<Self, String> {
        let has_min_max = read_u8(data, offset)? != 0;
        let min = decode_value(&ltype, data, offset)?;
        let max = decode_value(&ltype, data, offset)?;
        Ok(Self::new(ltype, has_min_max, min, max))
    }

    fn bool_range(&self) -> Result<(bool, bool), String> {
        match (&self.min, &self.max) {
            (MinMaxValue::Bool(min), MinMaxValue::Bool(max)) => Ok((*min, *max)),
            _ => Err("runtime min/max bool type mismatch".to_string()),
        }
    }

    fn i8_range(&self) -> Result<(i8, i8), String> {
        match (&self.min, &self.max) {
            (MinMaxValue::Int8(min), MinMaxValue::Int8(max)) => Ok((*min, *max)),
            _ => Err("runtime min/max int8 type mismatch".to_string()),
        }
    }

    fn i16_range(&self) -> Result<(i16, i16), String> {
        match (&self.min, &self.max) {
            (MinMaxValue::Int16(min), MinMaxValue::Int16(max)) => Ok((*min, *max)),
            _ => Err("runtime min/max int16 type mismatch".to_string()),
        }
    }

    fn i32_range(&self) -> Result<(i32, i32), String> {
        match (&self.min, &self.max) {
            (MinMaxValue::Int32(min), MinMaxValue::Int32(max)) => Ok((*min, *max)),
            _ => Err("runtime min/max int32 type mismatch".to_string()),
        }
    }

    fn i64_range(&self) -> Result<(i64, i64), String> {
        match (&self.min, &self.max) {
            (MinMaxValue::Int64(min), MinMaxValue::Int64(max)) => Ok((*min, *max)),
            _ => Err("runtime min/max int64 type mismatch".to_string()),
        }
    }

    fn i128_range(&self) -> Result<(i128, i128), String> {
        match (&self.min, &self.max) {
            (MinMaxValue::LargeInt(min), MinMaxValue::LargeInt(max)) => Ok((*min, *max)),
            _ => Err("runtime min/max int128 type mismatch".to_string()),
        }
    }

    fn f32_range(&self) -> Result<(f32, f32), String> {
        match (&self.min, &self.max) {
            (MinMaxValue::Float32(min), MinMaxValue::Float32(max)) => Ok((*min, *max)),
            _ => Err("runtime min/max float32 type mismatch".to_string()),
        }
    }

    fn f64_range(&self) -> Result<(f64, f64), String> {
        match (&self.min, &self.max) {
            (MinMaxValue::Float64(min), MinMaxValue::Float64(max)) => Ok((*min, *max)),
            _ => Err("runtime min/max float64 type mismatch".to_string()),
        }
    }

    fn date32_range(&self) -> Result<(i32, i32), String> {
        match (&self.min, &self.max) {
            (MinMaxValue::Date32(min), MinMaxValue::Date32(max)) => Ok((*min, *max)),
            _ => Err("runtime min/max date32 type mismatch".to_string()),
        }
    }

    fn timestamp_range(&self) -> Result<(i64, i64), String> {
        match (&self.min, &self.max) {
            (MinMaxValue::Timestamp(min), MinMaxValue::Timestamp(max)) => Ok((*min, *max)),
            _ => Err("runtime min/max timestamp type mismatch".to_string()),
        }
    }

    fn utf8_range(&self) -> Result<(&String, &String), String> {
        match (&self.min, &self.max) {
            (MinMaxValue::Utf8(min), MinMaxValue::Utf8(max)) => Ok((min, max)),
            _ => Err("runtime min/max utf8 type mismatch".to_string()),
        }
    }

    fn decimal_range(&self) -> Result<(i128, i128), String> {
        match (&self.min, &self.max) {
            (MinMaxValue::Decimal128(min), MinMaxValue::Decimal128(max)) => Ok((*min, *max)),
            _ => Err("runtime min/max decimal type mismatch".to_string()),
        }
    }
}

fn as_array<'a, T: Array + 'static>(array: &'a ArrayRef, expected: &str) -> Result<&'a T, String> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| format!("runtime min/max type mismatch for {expected}"))
}

fn update_min_max(
    value: MinMaxValue,
    min: &mut Option<MinMaxValue>,
    max: &mut Option<MinMaxValue>,
) -> Result<(), String> {
    match min.as_ref() {
        Some(current) => {
            if current.cmp(&value)? == Ordering::Greater {
                *min = Some(value.clone());
            }
        }
        None => *min = Some(value.clone()),
    }
    match max.as_ref() {
        Some(current) => {
            if current.cmp(&value)? == Ordering::Less {
                *max = Some(value);
            }
        }
        None => *max = Some(value),
    }
    Ok(())
}

fn full_range_values(ltype: &TPrimitiveType) -> Result<(MinMaxValue, MinMaxValue), String> {
    match *ltype {
        TPrimitiveType::BOOLEAN => Ok((MinMaxValue::Bool(false), MinMaxValue::Bool(true))),
        TPrimitiveType::TINYINT => Ok((MinMaxValue::Int8(i8::MIN), MinMaxValue::Int8(i8::MAX))),
        TPrimitiveType::SMALLINT => {
            Ok((MinMaxValue::Int16(i16::MIN), MinMaxValue::Int16(i16::MAX)))
        }
        TPrimitiveType::INT => Ok((MinMaxValue::Int32(i32::MIN), MinMaxValue::Int32(i32::MAX))),
        TPrimitiveType::BIGINT => Ok((MinMaxValue::Int64(i64::MIN), MinMaxValue::Int64(i64::MAX))),
        TPrimitiveType::LARGEINT => Ok((
            MinMaxValue::LargeInt(i128::MIN),
            MinMaxValue::LargeInt(i128::MAX),
        )),
        TPrimitiveType::FLOAT => Ok((
            MinMaxValue::Float32(f32::MIN),
            MinMaxValue::Float32(f32::MAX),
        )),
        TPrimitiveType::DOUBLE => Ok((
            MinMaxValue::Float64(f64::MIN),
            MinMaxValue::Float64(f64::MAX),
        )),
        TPrimitiveType::DATE => Ok((MinMaxValue::Date32(i32::MIN), MinMaxValue::Date32(i32::MAX))),
        TPrimitiveType::DATETIME | TPrimitiveType::TIME => Ok((
            MinMaxValue::Timestamp(i64::MIN),
            MinMaxValue::Timestamp(i64::MAX),
        )),
        t if is_utf8_type(&t) => Ok((
            MinMaxValue::Utf8(String::new()),
            MinMaxValue::Utf8(String::from("\u{10FFFF}")),
        )),
        TPrimitiveType::DECIMAL32 => Ok((
            MinMaxValue::Decimal128(i32::MIN as i128),
            MinMaxValue::Decimal128(i32::MAX as i128),
        )),
        TPrimitiveType::DECIMAL64 => Ok((
            MinMaxValue::Decimal128(i64::MIN as i128),
            MinMaxValue::Decimal128(i64::MAX as i128),
        )),
        TPrimitiveType::DECIMAL128 | TPrimitiveType::DECIMAL | TPrimitiveType::DECIMALV2 => Ok((
            MinMaxValue::Decimal128(i128::MIN),
            MinMaxValue::Decimal128(i128::MAX),
        )),
        _ => Err(format!("unsupported runtime min/max type: {:?}", ltype)),
    }
}

fn empty_range_values(ltype: &TPrimitiveType) -> Result<(MinMaxValue, MinMaxValue), String> {
    match *ltype {
        TPrimitiveType::BOOLEAN => Ok((MinMaxValue::Bool(true), MinMaxValue::Bool(false))),
        TPrimitiveType::TINYINT => Ok((MinMaxValue::Int8(i8::MAX), MinMaxValue::Int8(i8::MIN))),
        TPrimitiveType::SMALLINT => {
            Ok((MinMaxValue::Int16(i16::MAX), MinMaxValue::Int16(i16::MIN)))
        }
        TPrimitiveType::INT => Ok((MinMaxValue::Int32(i32::MAX), MinMaxValue::Int32(i32::MIN))),
        TPrimitiveType::BIGINT => Ok((MinMaxValue::Int64(i64::MAX), MinMaxValue::Int64(i64::MIN))),
        TPrimitiveType::LARGEINT => Ok((
            MinMaxValue::LargeInt(i128::MAX),
            MinMaxValue::LargeInt(i128::MIN),
        )),
        TPrimitiveType::FLOAT => Ok((
            MinMaxValue::Float32(f32::MAX),
            MinMaxValue::Float32(f32::MIN),
        )),
        TPrimitiveType::DOUBLE => Ok((
            MinMaxValue::Float64(f64::MAX),
            MinMaxValue::Float64(f64::MIN),
        )),
        TPrimitiveType::DATE => Ok((MinMaxValue::Date32(i32::MAX), MinMaxValue::Date32(i32::MIN))),
        TPrimitiveType::DATETIME | TPrimitiveType::TIME => Ok((
            MinMaxValue::Timestamp(i64::MAX),
            MinMaxValue::Timestamp(i64::MIN),
        )),
        t if is_utf8_type(&t) => Ok((
            MinMaxValue::Utf8(String::from("\u{10FFFF}")),
            MinMaxValue::Utf8(String::new()),
        )),
        TPrimitiveType::DECIMAL32 => Ok((
            MinMaxValue::Decimal128(i32::MAX as i128),
            MinMaxValue::Decimal128(i32::MIN as i128),
        )),
        TPrimitiveType::DECIMAL64 => Ok((
            MinMaxValue::Decimal128(i64::MAX as i128),
            MinMaxValue::Decimal128(i64::MIN as i128),
        )),
        TPrimitiveType::DECIMAL128 | TPrimitiveType::DECIMAL | TPrimitiveType::DECIMALV2 => Ok((
            MinMaxValue::Decimal128(i128::MAX),
            MinMaxValue::Decimal128(i128::MIN),
        )),
        _ => Err(format!("unsupported runtime min/max type: {:?}", ltype)),
    }
}

fn decode_value(
    ltype: &TPrimitiveType,
    data: &[u8],
    offset: &mut usize,
) -> Result<MinMaxValue, String> {
    match *ltype {
        TPrimitiveType::BOOLEAN => Ok(MinMaxValue::Bool(read_u8(data, offset)? != 0)),
        TPrimitiveType::TINYINT => Ok(MinMaxValue::Int8(read_i8(data, offset)?)),
        TPrimitiveType::SMALLINT => Ok(MinMaxValue::Int16(read_i16_le(data, offset)?)),
        TPrimitiveType::INT => Ok(MinMaxValue::Int32(read_i32_le(data, offset)?)),
        TPrimitiveType::BIGINT => Ok(MinMaxValue::Int64(read_i64_le(data, offset)?)),
        TPrimitiveType::LARGEINT => Ok(MinMaxValue::LargeInt(read_i128_le(data, offset)?)),
        TPrimitiveType::FLOAT => {
            let bits = read_i32_le(data, offset)? as u32;
            Ok(MinMaxValue::Float32(f32::from_bits(bits)))
        }
        TPrimitiveType::DOUBLE => {
            let bits = read_i64_le(data, offset)? as u64;
            Ok(MinMaxValue::Float64(f64::from_bits(bits)))
        }
        TPrimitiveType::DATE => Ok(MinMaxValue::Date32(read_i32_le(data, offset)?)),
        TPrimitiveType::DATETIME | TPrimitiveType::TIME => {
            Ok(MinMaxValue::Timestamp(read_i64_le(data, offset)?))
        }
        t if is_utf8_type(&t) => {
            let len = read_u64_le(data, offset)? as usize;
            if data.len() < *offset + len {
                return Err("runtime min/max data truncated".to_string());
            }
            let bytes = data[*offset..*offset + len].to_vec();
            *offset += len;
            let value = String::from_utf8(bytes)
                .map_err(|_| "runtime min/max utf8 decode failed".to_string())?;
            Ok(MinMaxValue::Utf8(value))
        }
        t if is_decimal_type(&t) => {
            let value = match *ltype {
                TPrimitiveType::DECIMAL32 => read_i32_le(data, offset)? as i128,
                TPrimitiveType::DECIMAL64 => read_i64_le(data, offset)? as i128,
                TPrimitiveType::DECIMAL128
                | TPrimitiveType::DECIMAL
                | TPrimitiveType::DECIMALV2 => read_i128_le(data, offset)?,
                _ => {
                    return Err(format!(
                        "unsupported runtime min/max decimal type: {:?}",
                        ltype
                    ));
                }
            };
            Ok(MinMaxValue::Decimal128(value))
        }
        _ => Err(format!("unsupported runtime min/max type: {:?}", ltype)),
    }
}

fn encode_value(
    ltype: &TPrimitiveType,
    value: &MinMaxValue,
    buf: &mut Vec<u8>,
) -> Result<(), String> {
    match (ltype, value) {
        (t, MinMaxValue::Bool(v)) if *t == TPrimitiveType::BOOLEAN => {
            buf.push(if *v { 1 } else { 0 });
        }
        (t, MinMaxValue::Int8(v)) if *t == TPrimitiveType::TINYINT => {
            buf.push(*v as u8);
        }
        (t, MinMaxValue::Int16(v)) if *t == TPrimitiveType::SMALLINT => {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        (t, MinMaxValue::Int32(v)) if *t == TPrimitiveType::INT => {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        (t, MinMaxValue::Int64(v)) if *t == TPrimitiveType::BIGINT => {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        (t, MinMaxValue::LargeInt(v)) if *t == TPrimitiveType::LARGEINT => {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        (t, MinMaxValue::Float32(v)) if *t == TPrimitiveType::FLOAT => {
            buf.extend_from_slice(&v.to_bits().to_le_bytes());
        }
        (t, MinMaxValue::Float64(v)) if *t == TPrimitiveType::DOUBLE => {
            buf.extend_from_slice(&v.to_bits().to_le_bytes());
        }
        (t, MinMaxValue::Date32(v)) if *t == TPrimitiveType::DATE => {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        (t, MinMaxValue::Timestamp(v))
            if *t == TPrimitiveType::DATETIME || *t == TPrimitiveType::TIME =>
        {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        (t, MinMaxValue::Utf8(v)) if is_utf8_type(t) => {
            let bytes = v.as_bytes();
            buf.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
            buf.extend_from_slice(bytes);
        }
        (t, MinMaxValue::Decimal128(v)) if is_decimal_type(t) => match *t {
            TPrimitiveType::DECIMAL32 => {
                let value = i32::try_from(*v)
                    .map_err(|_| "runtime min/max decimal32 overflow".to_string())?;
                buf.extend_from_slice(&value.to_le_bytes());
            }
            TPrimitiveType::DECIMAL64 => {
                let value = i64::try_from(*v)
                    .map_err(|_| "runtime min/max decimal64 overflow".to_string())?;
                buf.extend_from_slice(&value.to_le_bytes());
            }
            TPrimitiveType::DECIMAL128 | TPrimitiveType::DECIMAL | TPrimitiveType::DECIMALV2 => {
                buf.extend_from_slice(&v.to_le_bytes());
            }
            _ => return Err(format!("unsupported runtime min/max decimal type: {:?}", t)),
        },
        _ => return Err("runtime min/max encode type mismatch".to_string()),
    }
    Ok(())
}

fn read_i128_le(data: &[u8], offset: &mut usize) -> Result<i128, String> {
    if data.len() < *offset + 16 {
        return Err("runtime min/max data truncated".to_string());
    }
    let mut buf = [0u8; 16];
    buf.copy_from_slice(&data[*offset..*offset + 16]);
    *offset += 16;
    Ok(i128::from_le_bytes(buf))
}

fn timestamp_values(array: &ArrayRef) -> Result<(Vec<(i64, bool)>, TimeUnit), String> {
    match array.data_type() {
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => {
                let arr = as_array::<TimestampSecondArray>(array, "TimestampSecond")?;
                Ok((
                    (0..arr.len())
                        .map(|i| (arr.value(i), arr.is_null(i)))
                        .collect(),
                    *unit,
                ))
            }
            TimeUnit::Millisecond => {
                let arr = as_array::<TimestampMillisecondArray>(array, "TimestampMillisecond")?;
                Ok((
                    (0..arr.len())
                        .map(|i| (arr.value(i), arr.is_null(i)))
                        .collect(),
                    *unit,
                ))
            }
            TimeUnit::Microsecond => {
                let arr = as_array::<TimestampMicrosecondArray>(array, "TimestampMicrosecond")?;
                Ok((
                    (0..arr.len())
                        .map(|i| (arr.value(i), arr.is_null(i)))
                        .collect(),
                    *unit,
                ))
            }
            TimeUnit::Nanosecond => {
                let arr = as_array::<TimestampNanosecondArray>(array, "TimestampNanosecond")?;
                Ok((
                    (0..arr.len())
                        .map(|i| (arr.value(i), arr.is_null(i)))
                        .collect(),
                    *unit,
                ))
            }
        },
        _ => Err("runtime min/max timestamp type mismatch".to_string()),
    }
}

fn to_microseconds(value: i64, unit: TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => value.saturating_mul(1_000_000),
        TimeUnit::Millisecond => value.saturating_mul(1_000),
        TimeUnit::Microsecond => value,
        TimeUnit::Nanosecond => value / 1_000,
    }
}

fn is_utf8_type(ltype: &TPrimitiveType) -> bool {
    matches!(
        *ltype,
        TPrimitiveType::CHAR
            | TPrimitiveType::VARCHAR
            | TPrimitiveType::JSON
            | TPrimitiveType::HLL
            | TPrimitiveType::OBJECT
            | TPrimitiveType::PERCENTILE
            | TPrimitiveType::FUNCTION
    )
}

fn is_decimal_type(ltype: &TPrimitiveType) -> bool {
    matches!(
        *ltype,
        TPrimitiveType::DECIMAL
            | TPrimitiveType::DECIMALV2
            | TPrimitiveType::DECIMAL32
            | TPrimitiveType::DECIMAL64
            | TPrimitiveType::DECIMAL128
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{MinMaxValue, RuntimeMinMaxFilter};
    use crate::common::largeint;
    use crate::exec::expr::LiteralValue;
    use crate::types::TPrimitiveType;

    #[test]
    fn test_largeint_min_max_from_arrays() {
        let array = largeint::array_from_i128(&[
            Some(10),
            Some(-3),
            None,
            Some(i128::MAX - 1),
            Some(i128::MIN + 2),
        ])
        .unwrap();
        let filter = RuntimeMinMaxFilter::from_arrays(TPrimitiveType::LARGEINT, &[array]).unwrap();
        match (filter.min(), filter.max()) {
            (MinMaxValue::LargeInt(min), MinMaxValue::LargeInt(max)) => {
                assert_eq!(*min, i128::MIN + 2);
                assert_eq!(*max, i128::MAX - 1);
            }
            (min, max) => panic!("unexpected largeint min/max: min={:?} max={:?}", min, max),
        }
    }

    #[test]
    fn test_largeint_min_max_encode_decode() {
        let filter = RuntimeMinMaxFilter::new(
            TPrimitiveType::LARGEINT,
            true,
            MinMaxValue::LargeInt(i128::MIN + 123),
            MinMaxValue::LargeInt(i128::MAX - 456),
        );
        let mut encoded = Vec::new();
        filter.encode_into(&mut encoded).unwrap();
        let mut offset = 0usize;
        let decoded =
            RuntimeMinMaxFilter::decode(TPrimitiveType::LARGEINT, &encoded, &mut offset).unwrap();
        assert_eq!(offset, encoded.len());
        match decoded.min_max_literal() {
            Some((LiteralValue::LargeInt(min), LiteralValue::LargeInt(max))) => {
                assert_eq!(min, i128::MIN + 123);
                assert_eq!(max, i128::MAX - 456);
            }
            other => panic!("unexpected literal bounds: {:?}", other),
        }
    }

    #[test]
    fn test_largeint_apply_to_array() {
        let filter = RuntimeMinMaxFilter::new(
            TPrimitiveType::LARGEINT,
            true,
            MinMaxValue::LargeInt(0),
            MinMaxValue::LargeInt(10),
        );
        let array =
            largeint::array_from_i128(&[Some(-1), Some(0), Some(8), Some(11), None]).unwrap();
        let mut keep = vec![true; 5];
        filter
            .apply_to_array(&array, false, true, &mut keep)
            .unwrap();
        assert_eq!(keep, vec![false, true, true, false, false]);
    }

    #[test]
    fn test_largeint_apply_to_array_with_null_allowed() {
        let filter = RuntimeMinMaxFilter::new(
            TPrimitiveType::LARGEINT,
            true,
            MinMaxValue::LargeInt(0),
            MinMaxValue::LargeInt(10),
        );
        let array = largeint::array_from_i128(&[Some(3), None]).unwrap();
        let mut keep = vec![true; 2];
        filter
            .apply_to_array(&array, true, true, &mut keep)
            .unwrap();
        assert_eq!(keep, vec![true, true]);
    }

    #[test]
    fn test_largeint_apply_to_array_type_mismatch() {
        let filter = RuntimeMinMaxFilter::new(
            TPrimitiveType::LARGEINT,
            true,
            MinMaxValue::LargeInt(0),
            MinMaxValue::LargeInt(10),
        );
        let mut builder = arrow::array::FixedSizeBinaryBuilder::with_capacity(2, 8);
        builder.append_value([0u8; 8]).unwrap();
        builder.append_value([1u8; 8]).unwrap();
        let wrong_width = Arc::new(builder.finish()) as arrow::array::ArrayRef;
        let mut keep = vec![true; 2];
        let err = filter
            .apply_to_array(&wrong_width, false, true, &mut keep)
            .unwrap_err();
        assert!(err.contains("FixedSizeBinary(16)"));
    }
}
