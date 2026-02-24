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
    Array, ArrayRef, BooleanBuilder, Date32Array, Decimal128Array, Decimal256Array,
    FixedSizeBinaryArray, Float32Builder, Float64Builder, Int8Builder, Int16Builder, Int32Array,
    Int32Builder, Int64Builder, ListArray, StringArray, StringBuilder, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt32Array,
    new_null_array,
};
use arrow::compute::{concat, take};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer, i256};

use crate::common::ids::SlotId;
use crate::common::largeint;
use crate::exec::chunk::field_with_slot_id;
use crate::exec::expr::agg::AggKernelEntry;

use super::key_builder::{
    GroupKeyArrayView, encode_group_key_row, list_int32_row_value, list_utf8_row_value,
};

fn float32_key_equal(left: f32, right: f32) -> bool {
    (left.is_nan() && right.is_nan()) || left == right
}

fn float64_key_equal(left: f64, right: f64) -> bool {
    (left.is_nan() && right.is_nan()) || left == right
}

#[derive(Clone, Debug)]
pub(crate) enum KeyColumn {
    Int8 {
        values: Vec<i8>,
        nulls: Vec<u8>,
    },
    Int16 {
        values: Vec<i16>,
        nulls: Vec<u8>,
    },
    Int32 {
        values: Vec<i32>,
        nulls: Vec<u8>,
    },
    Int64 {
        values: Vec<i64>,
        nulls: Vec<u8>,
    },
    Float32 {
        values: Vec<f32>,
        nulls: Vec<u8>,
    },
    Float64 {
        values: Vec<f64>,
        nulls: Vec<u8>,
    },
    Boolean {
        values: Vec<u8>,
        nulls: Vec<u8>,
    },
    Utf8 {
        offsets: Vec<usize>,
        data: Vec<u8>,
        nulls: Vec<u8>,
    },
    Date32 {
        values: Vec<i32>,
        nulls: Vec<u8>,
    },
    Timestamp {
        values: Vec<i64>,
        nulls: Vec<u8>,
        unit: TimeUnit,
        tz: Option<String>,
    },
    Decimal128 {
        values: Vec<i128>,
        nulls: Vec<u8>,
        precision: u8,
        scale: i8,
    },
    Decimal256 {
        values: Vec<i256>,
        nulls: Vec<u8>,
        precision: u8,
        scale: i8,
    },
    LargeIntBinary {
        values: Vec<i128>,
        nulls: Vec<u8>,
    },
    ListUtf8 {
        values: Vec<Option<Vec<Option<String>>>>,
    },
    ListInt32 {
        values: Vec<Option<Vec<Option<i32>>>>,
    },
    Complex {
        data_type: DataType,
        keys: Vec<Vec<u8>>,
        nulls: Vec<u8>,
        values: Vec<ArrayRef>,
    },
}

impl KeyColumn {
    fn push_int_value<T>(
        values: &mut Vec<T>,
        nulls: &mut Vec<u8>,
        value: Option<i64>,
        type_name: &str,
    ) -> Result<(), String>
    where
        T: TryFrom<i64>,
    {
        if let Some(value) = value {
            let casted = T::try_from(value)
                .map_err(|_| format!("group key {} overflow: {}", type_name, value))?;
            values.push(casted);
            nulls.push(1);
        } else {
            let zero =
                T::try_from(0i64).map_err(|_| format!("group key {} zero overflow", type_name))?;
            values.push(zero);
            nulls.push(0);
        }
        Ok(())
    }

    fn int_value_equals<T>(
        values: &[T],
        nulls: &[u8],
        group_id: usize,
        value: Option<i64>,
        type_name: &str,
    ) -> Result<bool, String>
    where
        T: TryFrom<i64> + PartialEq,
    {
        let stored = values
            .get(group_id)
            .ok_or_else(|| "group key index out of bounds".to_string())?;
        let valid = *nulls
            .get(group_id)
            .ok_or_else(|| "group key index out of bounds".to_string())?
            != 0;
        match value {
            Some(value) => {
                let casted = T::try_from(value)
                    .map_err(|_| format!("group key {} overflow: {}", type_name, value))?;
                Ok(valid && *stored == casted)
            }
            None => Ok(!valid),
        }
    }

    fn largeint_value_at(arr: &FixedSizeBinaryArray, row: usize) -> Result<Option<i128>, String> {
        if arr.is_null(row) {
            return Ok(None);
        }
        largeint::i128_from_be_bytes(arr.value(row))
            .map(Some)
            .map_err(|e| format!("group key LARGEINT decode failed at row {}: {}", row, e))
    }

    pub(crate) fn push_value_from_view(
        &mut self,
        view: &GroupKeyArrayView<'_>,
        row: usize,
    ) -> Result<(), String> {
        match (self, view) {
            (KeyColumn::Int8 { values, nulls }, GroupKeyArrayView::Int(view)) => {
                Self::push_int_value(values, nulls, view.value_at(row), "int8")
            }
            (KeyColumn::Int16 { values, nulls }, GroupKeyArrayView::Int(view)) => {
                Self::push_int_value(values, nulls, view.value_at(row), "int16")
            }
            (KeyColumn::Int32 { values, nulls }, GroupKeyArrayView::Int(view)) => {
                Self::push_int_value(values, nulls, view.value_at(row), "int32")
            }
            (KeyColumn::Int64 { values, nulls }, GroupKeyArrayView::Int(view)) => {
                Self::push_int_value(values, nulls, view.value_at(row), "int64")
            }
            (KeyColumn::Float32 { values, nulls }, GroupKeyArrayView::Float(view)) => {
                if let Some(value) = view.value_at(row) {
                    values.push(value as f32);
                    nulls.push(1);
                } else {
                    values.push(0.0);
                    nulls.push(0);
                }
                Ok(())
            }
            (KeyColumn::Float64 { values, nulls }, GroupKeyArrayView::Float(view)) => {
                if let Some(value) = view.value_at(row) {
                    values.push(value);
                    nulls.push(1);
                } else {
                    values.push(0.0);
                    nulls.push(0);
                }
                Ok(())
            }
            (KeyColumn::Boolean { values, nulls }, GroupKeyArrayView::Boolean(arr)) => {
                if arr.is_null(row) {
                    values.push(0);
                    nulls.push(0);
                } else {
                    values.push(if arr.value(row) { 1 } else { 0 });
                    nulls.push(1);
                }
                Ok(())
            }
            (
                KeyColumn::Utf8 {
                    offsets,
                    data,
                    nulls,
                },
                GroupKeyArrayView::Utf8(arr),
            ) => {
                if arr.is_null(row) {
                    nulls.push(0);
                } else {
                    data.extend_from_slice(arr.value(row).as_bytes());
                    nulls.push(1);
                }
                offsets.push(data.len());
                Ok(())
            }
            (KeyColumn::Date32 { values, nulls }, GroupKeyArrayView::Date32(arr)) => {
                if arr.is_null(row) {
                    values.push(0);
                    nulls.push(0);
                } else {
                    values.push(arr.value(row));
                    nulls.push(1);
                }
                Ok(())
            }
            (
                KeyColumn::Timestamp { values, nulls, .. },
                GroupKeyArrayView::TimestampSecond(arr),
            ) => {
                if arr.is_null(row) {
                    values.push(0);
                    nulls.push(0);
                } else {
                    values.push(arr.value(row));
                    nulls.push(1);
                }
                Ok(())
            }
            (
                KeyColumn::Timestamp { values, nulls, .. },
                GroupKeyArrayView::TimestampMillisecond(arr),
            ) => {
                if arr.is_null(row) {
                    values.push(0);
                    nulls.push(0);
                } else {
                    values.push(arr.value(row));
                    nulls.push(1);
                }
                Ok(())
            }
            (
                KeyColumn::Timestamp { values, nulls, .. },
                GroupKeyArrayView::TimestampMicrosecond(arr),
            ) => {
                if arr.is_null(row) {
                    values.push(0);
                    nulls.push(0);
                } else {
                    values.push(arr.value(row));
                    nulls.push(1);
                }
                Ok(())
            }
            (
                KeyColumn::Timestamp { values, nulls, .. },
                GroupKeyArrayView::TimestampNanosecond(arr),
            ) => {
                if arr.is_null(row) {
                    values.push(0);
                    nulls.push(0);
                } else {
                    values.push(arr.value(row));
                    nulls.push(1);
                }
                Ok(())
            }
            (KeyColumn::Decimal128 { values, nulls, .. }, GroupKeyArrayView::Decimal128(arr)) => {
                if arr.is_null(row) {
                    values.push(0);
                    nulls.push(0);
                } else {
                    values.push(arr.value(row));
                    nulls.push(1);
                }
                Ok(())
            }
            (KeyColumn::Decimal256 { values, nulls, .. }, GroupKeyArrayView::Decimal256(arr)) => {
                if arr.is_null(row) {
                    values.push(i256::ZERO);
                    nulls.push(0);
                } else {
                    values.push(arr.value(row));
                    nulls.push(1);
                }
                Ok(())
            }
            (
                KeyColumn::LargeIntBinary { values, nulls },
                GroupKeyArrayView::LargeIntBinary(arr),
            ) => {
                if let Some(value) = Self::largeint_value_at(arr, row)? {
                    values.push(value);
                    nulls.push(1);
                } else {
                    values.push(0);
                    nulls.push(0);
                }
                Ok(())
            }
            (
                KeyColumn::ListUtf8 { values: stored },
                GroupKeyArrayView::ListUtf8 { list, values },
            ) => {
                stored.push(list_utf8_row_value(list, values, row));
                Ok(())
            }
            (
                KeyColumn::ListInt32 { values: stored },
                GroupKeyArrayView::ListInt32 { list, values },
            ) => {
                stored.push(list_int32_row_value(list, values, row));
                Ok(())
            }
            (
                KeyColumn::Complex {
                    keys,
                    nulls,
                    values,
                    ..
                },
                GroupKeyArrayView::Complex(array),
            ) => {
                if array.is_null(row) {
                    keys.push(Vec::new());
                    nulls.push(0);
                    values.push(new_null_array(array.data_type(), 1));
                    return Ok(());
                }
                let encoded = encode_group_key_row(array, row)?
                    .ok_or_else(|| "complex group key encoded unexpectedly null".to_string())?;
                let row_u32 = u32::try_from(row)
                    .map_err(|_| format!("group key row index overflow: {}", row))?;
                let row_index = UInt32Array::from(vec![row_u32]);
                let single = take(array.as_ref(), &row_index, None)
                    .map_err(|e| format!("take complex group key row failed: {}", e))?;
                keys.push(encoded);
                nulls.push(1);
                values.push(single);
                Ok(())
            }
            _ => Err("group by key type mismatch".to_string()),
        }
    }

    pub(crate) fn value_equals(
        &self,
        group_id: usize,
        view: &GroupKeyArrayView<'_>,
        row: usize,
    ) -> Result<bool, String> {
        match (self, view) {
            (KeyColumn::Int8 { values, nulls }, GroupKeyArrayView::Int(view)) => {
                Self::int_value_equals(values, nulls, group_id, view.value_at(row), "int8")
            }
            (KeyColumn::Int16 { values, nulls }, GroupKeyArrayView::Int(view)) => {
                Self::int_value_equals(values, nulls, group_id, view.value_at(row), "int16")
            }
            (KeyColumn::Int32 { values, nulls }, GroupKeyArrayView::Int(view)) => {
                Self::int_value_equals(values, nulls, group_id, view.value_at(row), "int32")
            }
            (KeyColumn::Int64 { values, nulls }, GroupKeyArrayView::Int(view)) => {
                Self::int_value_equals(values, nulls, group_id, view.value_at(row), "int64")
            }
            (KeyColumn::Float32 { values, nulls }, GroupKeyArrayView::Float(view)) => {
                let stored = values
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?;
                let valid = *nulls
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?
                    != 0;
                match view.value_at(row) {
                    Some(value) => Ok(valid && float32_key_equal(*stored, value as f32)),
                    None => Ok(!valid),
                }
            }
            (KeyColumn::Float64 { values, nulls }, GroupKeyArrayView::Float(view)) => {
                let stored = values
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?;
                let valid = *nulls
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?
                    != 0;
                match view.value_at(row) {
                    Some(value) => Ok(valid && float64_key_equal(*stored, value)),
                    None => Ok(!valid),
                }
            }
            (KeyColumn::Boolean { values, nulls }, GroupKeyArrayView::Boolean(arr)) => {
                let stored = values
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?;
                let valid = *nulls
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?
                    != 0;
                let value = (!arr.is_null(row)).then(|| arr.value(row));
                match value {
                    Some(value) => Ok(valid && (*stored != 0) == value),
                    None => Ok(!valid),
                }
            }
            (
                KeyColumn::Utf8 {
                    offsets,
                    data,
                    nulls,
                },
                GroupKeyArrayView::Utf8(arr),
            ) => {
                let valid = *nulls
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?
                    != 0;
                let value = (!arr.is_null(row)).then(|| arr.value(row));
                match value {
                    Some(value) => {
                        if !valid {
                            return Ok(false);
                        }
                        let start = *offsets
                            .get(group_id)
                            .ok_or_else(|| "group key index out of bounds".to_string())?;
                        let end = *offsets
                            .get(group_id + 1)
                            .ok_or_else(|| "group key index out of bounds".to_string())?;
                        Ok(data
                            .get(start..end)
                            .map_or(false, |bytes| bytes == value.as_bytes()))
                    }
                    None => Ok(!valid),
                }
            }
            (KeyColumn::Date32 { values, nulls }, GroupKeyArrayView::Date32(arr)) => {
                let stored = values
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?;
                let valid = *nulls
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?
                    != 0;
                let value = (!arr.is_null(row)).then(|| arr.value(row));
                match value {
                    Some(value) => Ok(valid && *stored == value),
                    None => Ok(!valid),
                }
            }
            (
                KeyColumn::Timestamp { values, nulls, .. },
                GroupKeyArrayView::TimestampSecond(arr),
            ) => {
                let stored = values
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?;
                let valid = *nulls
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?
                    != 0;
                let value = (!arr.is_null(row)).then(|| arr.value(row));
                match value {
                    Some(value) => Ok(valid && *stored == value),
                    None => Ok(!valid),
                }
            }
            (
                KeyColumn::Timestamp { values, nulls, .. },
                GroupKeyArrayView::TimestampMillisecond(arr),
            ) => {
                let stored = values
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?;
                let valid = *nulls
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?
                    != 0;
                let value = (!arr.is_null(row)).then(|| arr.value(row));
                match value {
                    Some(value) => Ok(valid && *stored == value),
                    None => Ok(!valid),
                }
            }
            (
                KeyColumn::Timestamp { values, nulls, .. },
                GroupKeyArrayView::TimestampMicrosecond(arr),
            ) => {
                let stored = values
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?;
                let valid = *nulls
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?
                    != 0;
                let value = (!arr.is_null(row)).then(|| arr.value(row));
                match value {
                    Some(value) => Ok(valid && *stored == value),
                    None => Ok(!valid),
                }
            }
            (
                KeyColumn::Timestamp { values, nulls, .. },
                GroupKeyArrayView::TimestampNanosecond(arr),
            ) => {
                let stored = values
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?;
                let valid = *nulls
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?
                    != 0;
                let value = (!arr.is_null(row)).then(|| arr.value(row));
                match value {
                    Some(value) => Ok(valid && *stored == value),
                    None => Ok(!valid),
                }
            }
            (KeyColumn::Decimal128 { values, nulls, .. }, GroupKeyArrayView::Decimal128(arr)) => {
                let stored = values
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?;
                let valid = *nulls
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?
                    != 0;
                let value = (!arr.is_null(row)).then(|| arr.value(row));
                match value {
                    Some(value) => Ok(valid && *stored == value),
                    None => Ok(!valid),
                }
            }
            (KeyColumn::Decimal256 { values, nulls, .. }, GroupKeyArrayView::Decimal256(arr)) => {
                let stored = values
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?;
                let valid = *nulls
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?
                    != 0;
                let value = (!arr.is_null(row)).then(|| arr.value(row));
                match value {
                    Some(value) => Ok(valid && *stored == value),
                    None => Ok(!valid),
                }
            }
            (
                KeyColumn::LargeIntBinary { values, nulls },
                GroupKeyArrayView::LargeIntBinary(arr),
            ) => {
                let stored = values
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?;
                let valid = *nulls
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?
                    != 0;
                match Self::largeint_value_at(arr, row)? {
                    Some(value) => Ok(valid && *stored == value),
                    None => Ok(!valid),
                }
            }
            (
                KeyColumn::ListUtf8 { values: stored },
                GroupKeyArrayView::ListUtf8 { list, values },
            ) => {
                let saved = stored
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?;
                let current = list_utf8_row_value(list, values, row);
                Ok(saved == &current)
            }
            (
                KeyColumn::ListInt32 { values: stored },
                GroupKeyArrayView::ListInt32 { list, values },
            ) => {
                let saved = stored
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?;
                let current = list_int32_row_value(list, values, row);
                Ok(saved == &current)
            }
            (KeyColumn::Complex { keys, nulls, .. }, GroupKeyArrayView::Complex(array)) => {
                let valid = *nulls
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?
                    != 0;
                if array.is_null(row) {
                    return Ok(!valid);
                }
                if !valid {
                    return Ok(false);
                }
                let saved = keys
                    .get(group_id)
                    .ok_or_else(|| "group key index out of bounds".to_string())?;
                let current = encode_group_key_row(array, row)?
                    .ok_or_else(|| "complex group key encoded unexpectedly null".to_string())?;
                Ok(saved == &current)
            }
            _ => Err("group by key type mismatch".to_string()),
        }
    }

    pub(crate) fn to_array(&self) -> Result<ArrayRef, String> {
        match self {
            KeyColumn::Int8 { values, nulls } => {
                let mut builder = Int8Builder::new();
                for (value, valid) in values.iter().zip(nulls.iter()) {
                    if *valid != 0 {
                        builder.append_value(*value);
                    } else {
                        builder.append_null();
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            KeyColumn::Int16 { values, nulls } => {
                let mut builder = Int16Builder::new();
                for (value, valid) in values.iter().zip(nulls.iter()) {
                    if *valid != 0 {
                        builder.append_value(*value);
                    } else {
                        builder.append_null();
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            KeyColumn::Int32 { values, nulls } => {
                let mut builder = Int32Builder::new();
                for (value, valid) in values.iter().zip(nulls.iter()) {
                    if *valid != 0 {
                        builder.append_value(*value);
                    } else {
                        builder.append_null();
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            KeyColumn::Int64 { values, nulls } => {
                let mut builder = Int64Builder::new();
                for (value, valid) in values.iter().zip(nulls.iter()) {
                    if *valid != 0 {
                        builder.append_value(*value);
                    } else {
                        builder.append_null();
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            KeyColumn::Float32 { values, nulls } => {
                let mut builder = Float32Builder::new();
                for (value, valid) in values.iter().zip(nulls.iter()) {
                    if *valid != 0 {
                        builder.append_value(*value);
                    } else {
                        builder.append_null();
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            KeyColumn::Float64 { values, nulls } => {
                let mut builder = Float64Builder::new();
                for (value, valid) in values.iter().zip(nulls.iter()) {
                    if *valid != 0 {
                        builder.append_value(*value);
                    } else {
                        builder.append_null();
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            KeyColumn::Boolean { values, nulls } => {
                let mut builder = BooleanBuilder::new();
                for (value, valid) in values.iter().zip(nulls.iter()) {
                    if *valid != 0 {
                        builder.append_value(*value != 0);
                    } else {
                        builder.append_null();
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            KeyColumn::Utf8 {
                offsets,
                data,
                nulls,
            } => {
                let mut builder = StringBuilder::new();
                if offsets.len() < nulls.len() + 1 {
                    return Err("group key offsets out of bounds".to_string());
                }
                for idx in 0..nulls.len() {
                    if nulls[idx] == 0 {
                        builder.append_null();
                        continue;
                    }
                    let start = offsets[idx];
                    let end = offsets[idx + 1];
                    let bytes = data
                        .get(start..end)
                        .ok_or_else(|| "group key utf8 slice out of bounds".to_string())?;
                    let value = std::str::from_utf8(bytes).map_err(|e| e.to_string())?;
                    builder.append_value(value);
                }
                Ok(Arc::new(builder.finish()))
            }
            KeyColumn::Date32 { values, nulls } => {
                let mut out = Vec::with_capacity(values.len());
                for (value, valid) in values.iter().zip(nulls.iter()) {
                    if *valid != 0 {
                        out.push(Some(*value));
                    } else {
                        out.push(None);
                    }
                }
                Ok(Arc::new(Date32Array::from(out)))
            }
            KeyColumn::Timestamp {
                values,
                nulls,
                unit,
                tz,
            } => {
                let mut out = Vec::with_capacity(values.len());
                for (value, valid) in values.iter().zip(nulls.iter()) {
                    if *valid != 0 {
                        out.push(Some(*value));
                    } else {
                        out.push(None);
                    }
                }
                let array: ArrayRef = match unit {
                    TimeUnit::Second => {
                        let array = TimestampSecondArray::from(out);
                        if let Some(tz) = tz {
                            Arc::new(array.with_timezone(tz.clone()))
                        } else {
                            Arc::new(array)
                        }
                    }
                    TimeUnit::Millisecond => {
                        let array = TimestampMillisecondArray::from(out);
                        if let Some(tz) = tz {
                            Arc::new(array.with_timezone(tz.clone()))
                        } else {
                            Arc::new(array)
                        }
                    }
                    TimeUnit::Microsecond => {
                        let array = TimestampMicrosecondArray::from(out);
                        if let Some(tz) = tz {
                            Arc::new(array.with_timezone(tz.clone()))
                        } else {
                            Arc::new(array)
                        }
                    }
                    TimeUnit::Nanosecond => {
                        let array = TimestampNanosecondArray::from(out);
                        if let Some(tz) = tz {
                            Arc::new(array.with_timezone(tz.clone()))
                        } else {
                            Arc::new(array)
                        }
                    }
                };
                Ok(array)
            }
            KeyColumn::Decimal128 {
                values,
                nulls,
                precision,
                scale,
            } => {
                let mut out = Vec::with_capacity(values.len());
                for (value, valid) in values.iter().zip(nulls.iter()) {
                    if *valid != 0 {
                        out.push(Some(*value));
                    } else {
                        out.push(None);
                    }
                }
                let array = Decimal128Array::from(out)
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(|e| e.to_string())?;
                Ok(Arc::new(array))
            }
            KeyColumn::Decimal256 {
                values,
                nulls,
                precision,
                scale,
            } => {
                let mut out = Vec::with_capacity(values.len());
                for (value, valid) in values.iter().zip(nulls.iter()) {
                    if *valid != 0 {
                        out.push(Some(*value));
                    } else {
                        out.push(None);
                    }
                }
                let array = Decimal256Array::from(out)
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(|e| e.to_string())?;
                Ok(Arc::new(array))
            }
            KeyColumn::LargeIntBinary { values, nulls } => {
                let mut out = Vec::with_capacity(values.len());
                for (value, valid) in values.iter().zip(nulls.iter()) {
                    if *valid != 0 {
                        out.push(Some(*value));
                    } else {
                        out.push(None);
                    }
                }
                largeint::array_from_i128(&out)
            }
            KeyColumn::ListUtf8 { values } => {
                let mut flat = Vec::<Option<String>>::new();
                let mut offsets = Vec::with_capacity(values.len() + 1);
                offsets.push(0_i32);
                let mut nulls = NullBufferBuilder::new(values.len());
                let mut current: i64 = 0;
                for value in values {
                    match value {
                        None => {
                            nulls.append_null();
                            offsets.push(current as i32);
                        }
                        Some(items) => {
                            current = current
                                .checked_add(items.len() as i64)
                                .ok_or_else(|| "group key list offset overflow".to_string())?;
                            if current > i32::MAX as i64 {
                                return Err("group key list offset overflow".to_string());
                            }
                            flat.extend(items.iter().cloned());
                            nulls.append_non_null();
                            offsets.push(current as i32);
                        }
                    }
                }
                let item_array = Arc::new(StringArray::from(flat)) as ArrayRef;
                let field = Arc::new(Field::new("item", DataType::Utf8, true));
                let array = ListArray::new(
                    field,
                    OffsetBuffer::new(offsets.into()),
                    item_array,
                    nulls.finish(),
                );
                Ok(Arc::new(array))
            }
            KeyColumn::ListInt32 { values } => {
                let mut flat = Vec::<Option<i32>>::new();
                let mut offsets = Vec::with_capacity(values.len() + 1);
                offsets.push(0_i32);
                let mut nulls = NullBufferBuilder::new(values.len());
                let mut current: i64 = 0;
                for value in values {
                    match value {
                        None => {
                            nulls.append_null();
                            offsets.push(current as i32);
                        }
                        Some(items) => {
                            current = current
                                .checked_add(items.len() as i64)
                                .ok_or_else(|| "group key list offset overflow".to_string())?;
                            if current > i32::MAX as i64 {
                                return Err("group key list offset overflow".to_string());
                            }
                            flat.extend(items.iter().copied());
                            nulls.append_non_null();
                            offsets.push(current as i32);
                        }
                    }
                }
                let item_array = Arc::new(Int32Array::from(flat)) as ArrayRef;
                let field = Arc::new(Field::new("item", DataType::Int32, true));
                let array = ListArray::new(
                    field,
                    OffsetBuffer::new(offsets.into()),
                    item_array,
                    nulls.finish(),
                );
                Ok(Arc::new(array))
            }
            KeyColumn::Complex {
                data_type, values, ..
            } => {
                if values.is_empty() {
                    return Ok(new_null_array(data_type, 0));
                }
                let refs: Vec<&dyn Array> = values.iter().map(|v| v.as_ref()).collect();
                concat(&refs).map_err(|e| format!("concat complex group key failed: {}", e))
            }
        }
    }

    pub(crate) fn data_type(&self) -> DataType {
        match self {
            KeyColumn::Int8 { .. } => DataType::Int8,
            KeyColumn::Int16 { .. } => DataType::Int16,
            KeyColumn::Int32 { .. } => DataType::Int32,
            KeyColumn::Int64 { .. } => DataType::Int64,
            KeyColumn::Float32 { .. } => DataType::Float32,
            KeyColumn::Float64 { .. } => DataType::Float64,
            KeyColumn::Boolean { .. } => DataType::Boolean,
            KeyColumn::Utf8 { .. } => DataType::Utf8,
            KeyColumn::Date32 { .. } => DataType::Date32,
            KeyColumn::Timestamp { unit, tz, .. } => {
                let tz_arc = tz.as_deref().map(Arc::<str>::from);
                DataType::Timestamp(unit.clone(), tz_arc)
            }
            KeyColumn::Decimal128 {
                precision, scale, ..
            } => DataType::Decimal128(*precision, *scale),
            KeyColumn::Decimal256 {
                precision, scale, ..
            } => DataType::Decimal256(*precision, *scale),
            KeyColumn::LargeIntBinary { .. } => {
                DataType::FixedSizeBinary(largeint::LARGEINT_BYTE_WIDTH)
            }
            KeyColumn::ListUtf8 { .. } => {
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
            }
            KeyColumn::ListInt32 { .. } => {
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true)))
            }
            KeyColumn::Complex { data_type, .. } => data_type.clone(),
        }
    }
}

#[allow(dead_code)]
pub(crate) fn key_column_from_array(array: &ArrayRef) -> Result<KeyColumn, String> {
    key_column_from_type(array.data_type())
}

//FIXME:
pub(crate) fn key_column_from_type(data_type: &DataType) -> Result<KeyColumn, String> {
    match data_type {
        DataType::Int8 => Ok(KeyColumn::Int8 {
            values: Vec::new(),
            nulls: Vec::new(),
        }),
        DataType::Int16 => Ok(KeyColumn::Int16 {
            values: Vec::new(),
            nulls: Vec::new(),
        }),
        DataType::Int32 => Ok(KeyColumn::Int32 {
            values: Vec::new(),
            nulls: Vec::new(),
        }),
        DataType::Int64 => Ok(KeyColumn::Int64 {
            values: Vec::new(),
            nulls: Vec::new(),
        }),
        DataType::Float32 => Ok(KeyColumn::Float32 {
            values: Vec::new(),
            nulls: Vec::new(),
        }),
        DataType::Float64 => Ok(KeyColumn::Float64 {
            values: Vec::new(),
            nulls: Vec::new(),
        }),
        DataType::Boolean => Ok(KeyColumn::Boolean {
            values: Vec::new(),
            nulls: Vec::new(),
        }),
        DataType::Utf8 => Ok(KeyColumn::Utf8 {
            offsets: vec![0],
            data: Vec::new(),
            nulls: Vec::new(),
        }),
        DataType::Date32 => Ok(KeyColumn::Date32 {
            values: Vec::new(),
            nulls: Vec::new(),
        }),
        DataType::Timestamp(unit, tz) => {
            let tz_string = tz.as_deref().map(|s| s.to_string());
            Ok(KeyColumn::Timestamp {
                values: Vec::new(),
                nulls: Vec::new(),
                unit: unit.clone(),
                tz: tz_string,
            })
        }
        DataType::Decimal128(precision, scale) => Ok(KeyColumn::Decimal128 {
            values: Vec::new(),
            nulls: Vec::new(),
            precision: *precision,
            scale: *scale,
        }),
        DataType::Decimal256(precision, scale) => Ok(KeyColumn::Decimal256 {
            values: Vec::new(),
            nulls: Vec::new(),
            precision: *precision,
            scale: *scale,
        }),
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            Ok(KeyColumn::LargeIntBinary {
                values: Vec::new(),
                nulls: Vec::new(),
            })
        }
        DataType::List(field) if matches!(field.data_type(), DataType::Utf8) => {
            Ok(KeyColumn::ListUtf8 { values: Vec::new() })
        }
        DataType::List(field) if matches!(field.data_type(), DataType::Int32) => {
            Ok(KeyColumn::ListInt32 { values: Vec::new() })
        }
        DataType::Null => Err("group by type is null".to_string()),
        other => Ok(KeyColumn::Complex {
            data_type: other.clone(),
            keys: Vec::new(),
            nulls: Vec::new(),
            values: Vec::new(),
        }),
    }
}

pub(crate) fn build_output_schema_from_kernels(
    key_columns: &[KeyColumn],
    kernels: &[AggKernelEntry],
    output_intermediate: bool,
    output_slots: &[SlotId],
) -> Result<SchemaRef, String> {
    let mut fields: Vec<Field> = Vec::with_capacity(key_columns.len() + kernels.len());
    for (idx, col) in key_columns.iter().enumerate() {
        let mut field = Field::new(format!("group_{}", idx), col.data_type(), true);
        if let Some(slot_id) = output_slots.get(idx) {
            field = field_with_slot_id(field, *slot_id);
        }
        fields.push(field);
    }
    for (idx, kernel) in kernels.iter().enumerate() {
        let mut field = Field::new(
            format!("agg_{}", idx),
            kernel.output_type(output_intermediate),
            true,
        );
        if let Some(slot_id) = output_slots.get(key_columns.len() + idx) {
            field = field_with_slot_id(field, *slot_id);
        }
        fields.push(field);
    }
    // StarRocks FE may include extra materialized slots in the output tuple (e.g. intermediate /
    // passthrough slots) that are not part of the final operator output schema we construct here.
    // We only require that the provided output_slots cover the columns we actually output.
    if !output_slots.is_empty() && output_slots.len() < fields.len() {
        return Err(format!(
            "aggregate output slot count mismatch: slots={} columns={}",
            output_slots.len(),
            fields.len()
        ));
    }
    Ok(Arc::new(Schema::new(fields)))
}
