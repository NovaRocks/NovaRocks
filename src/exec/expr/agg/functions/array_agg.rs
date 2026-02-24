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
use std::cmp::Ordering;
use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, ListArray, MapArray,
    StringArray, StructArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::datatypes::{DataType, Field, Fields, TimeUnit};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};

use crate::common::largeint;
use crate::exec::node::aggregate::AggFunction;

use super::super::*;
use super::AggregateFunction;
use super::common::compare_scalar_values;

pub(super) struct ArrayAggAgg;

#[derive(Clone, Debug)]
enum ArrayAggValue {
    Bool(bool),
    Int64(i64),
    Float64(f64),
    Utf8(String),
    Date32(i32),
    Timestamp(i64),
    Decimal128(i128),
    Struct(Vec<Option<ArrayAggValue>>),
    List(Vec<Option<ArrayAggValue>>),
}

#[derive(Clone, Debug, Default)]
struct ArrayAggState {
    rows: Vec<Vec<Option<ArrayAggValue>>>,
    distinct_seen: HashSet<Vec<u8>>,
}

fn encode_scalar_key(value: &Option<ArrayAggValue>) -> Vec<u8> {
    fn encode_value(out: &mut Vec<u8>, value: &ArrayAggValue) {
        match value {
            ArrayAggValue::Bool(v) => {
                out.push(1);
                out.push(*v as u8);
            }
            ArrayAggValue::Int64(v) => {
                out.push(2);
                out.extend_from_slice(&v.to_le_bytes());
            }
            ArrayAggValue::Float64(v) => {
                out.push(3);
                out.extend_from_slice(&v.to_bits().to_le_bytes());
            }
            ArrayAggValue::Utf8(v) => {
                out.push(4);
                let len = v.len() as u32;
                out.extend_from_slice(&len.to_le_bytes());
                out.extend_from_slice(v.as_bytes());
            }
            ArrayAggValue::Date32(v) => {
                out.push(5);
                out.extend_from_slice(&v.to_le_bytes());
            }
            ArrayAggValue::Timestamp(v) => {
                out.push(6);
                out.extend_from_slice(&v.to_le_bytes());
            }
            ArrayAggValue::Decimal128(v) => {
                out.push(7);
                out.extend_from_slice(&v.to_le_bytes());
            }
            ArrayAggValue::Struct(items) => {
                out.push(8);
                let len = items.len() as u32;
                out.extend_from_slice(&len.to_le_bytes());
                for item in items {
                    match item {
                        None => out.push(0),
                        Some(v) => {
                            out.push(1);
                            encode_value(out, v);
                        }
                    }
                }
            }
            ArrayAggValue::List(items) => {
                out.push(9);
                let len = items.len() as u32;
                out.extend_from_slice(&len.to_le_bytes());
                for item in items {
                    match item {
                        None => out.push(0),
                        Some(v) => {
                            out.push(1);
                            encode_value(out, v);
                        }
                    }
                }
            }
        }
    }

    match value {
        None => vec![0],
        Some(v) => {
            let mut out = vec![1];
            encode_value(&mut out, v);
            out
        }
    }
}

fn is_distinct_kind(kind: &AggKind) -> bool {
    match kind {
        AggKind::ArrayAgg { is_distinct, .. } => *is_distinct,
        AggKind::ArrayUniqueAgg => true,
        _ => false,
    }
}

fn parse_bool_flag(v: &str) -> Result<bool, String> {
    match v {
        "1" | "true" | "TRUE" => Ok(true),
        "0" | "false" | "FALSE" => Ok(false),
        _ => Err(format!("array_agg metadata bool is invalid: {}", v)),
    }
}

fn parse_bool_list(v: &str) -> Result<Vec<bool>, String> {
    if v.is_empty() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for token in v.split(',') {
        out.push(parse_bool_flag(token)?);
    }
    Ok(out)
}

fn parse_array_agg_metadata(name: &str) -> Result<(String, Vec<bool>, Vec<bool>), String> {
    let base = name.split('|').next().unwrap_or(name).to_string();
    if base != "array_agg" && base != "array_agg_distinct" && base != "array_unique_agg" {
        return Err(format!("unsupported array agg function: {}", name));
    }
    if !name.contains('|') {
        return Ok((base, Vec::new(), Vec::new()));
    }

    let mut is_asc_order = Vec::new();
    let mut nulls_first = Vec::new();
    for token in name.split('|').skip(1) {
        let (k, v) = token
            .split_once('=')
            .ok_or_else(|| format!("array_agg metadata token missing '=': {}", token))?;
        match k {
            "a" => is_asc_order = parse_bool_list(v)?,
            "n" => nulls_first = parse_bool_list(v)?,
            other => {
                return Err(format!("array_agg metadata key '{}' is unsupported", other));
            }
        }
    }
    if is_asc_order.len() != nulls_first.len() {
        return Err(format!(
            "array_agg metadata length mismatch: is_asc_order={} nulls_first={}",
            is_asc_order.len(),
            nulls_first.len()
        ));
    }
    Ok((base, is_asc_order, nulls_first))
}

fn order_by_kind(kind: &AggKind) -> (&[bool], &[bool]) {
    match kind {
        AggKind::ArrayAgg {
            is_asc_order,
            nulls_first,
            ..
        } => (is_asc_order.as_slice(), nulls_first.as_slice()),
        _ => (&[], &[]),
    }
}

fn compare_optional_values(
    left: &Option<ArrayAggValue>,
    right: &Option<ArrayAggValue>,
    asc: bool,
    nulls_first: bool,
) -> Result<Ordering, String> {
    let ord = match (left, right) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => {
            if nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (Some(_), None) => {
            if nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        (Some(left), Some(right)) => {
            let left = to_common_scalar(left);
            let right = to_common_scalar(right);
            let ord = compare_scalar_values(&left, &right)?;
            if asc { ord } else { ord.reverse() }
        }
    };
    Ok(ord)
}

fn to_common_scalar(value: &ArrayAggValue) -> super::common::AggScalarValue {
    match value {
        ArrayAggValue::Bool(v) => super::common::AggScalarValue::Bool(*v),
        ArrayAggValue::Int64(v) => super::common::AggScalarValue::Int64(*v),
        ArrayAggValue::Float64(v) => super::common::AggScalarValue::Float64(*v),
        ArrayAggValue::Utf8(v) => super::common::AggScalarValue::Utf8(v.clone()),
        ArrayAggValue::Date32(v) => super::common::AggScalarValue::Date32(*v),
        ArrayAggValue::Timestamp(v) => super::common::AggScalarValue::Timestamp(*v),
        ArrayAggValue::Decimal128(v) => super::common::AggScalarValue::Decimal128(*v),
        ArrayAggValue::Struct(items) => super::common::AggScalarValue::Struct(
            items
                .iter()
                .map(|item| item.as_ref().map(to_common_scalar))
                .collect(),
        ),
        ArrayAggValue::List(items) => super::common::AggScalarValue::List(
            items
                .iter()
                .map(|item| item.as_ref().map(to_common_scalar))
                .collect(),
        ),
    }
}

fn sort_rows(
    rows: &mut [Vec<Option<ArrayAggValue>>],
    is_asc_order: &[bool],
    nulls_first: &[bool],
) -> Result<(), String> {
    let order_by_num = is_asc_order.len();
    if order_by_num == 0 {
        return Ok(());
    }
    let mut error: Option<String> = None;
    rows.sort_by(|left, right| {
        if error.is_some() {
            return Ordering::Equal;
        }
        for key in 0..order_by_num {
            let col = 1 + key;
            let l = left.get(col).cloned().unwrap_or(None);
            let r = right.get(col).cloned().unwrap_or(None);
            match compare_optional_values(&l, &r, is_asc_order[key], nulls_first[key]) {
                Ok(Ordering::Equal) => continue,
                Ok(ord) => return ord,
                Err(err) => {
                    error = Some(err);
                    return Ordering::Equal;
                }
            }
        }
        Ordering::Equal
    });
    if let Some(err) = error {
        return Err(err);
    }
    Ok(())
}

fn extract_final_values(
    spec: &AggSpec,
    state: &ArrayAggState,
) -> Result<Vec<Option<ArrayAggValue>>, String> {
    let mut rows = state.rows.clone();
    let (is_asc_order, nulls_first) = order_by_kind(&spec.kind);
    sort_rows(&mut rows, is_asc_order, nulls_first)?;

    let distinct = is_distinct_kind(&spec.kind);
    let mut out = Vec::with_capacity(rows.len());
    let mut seen = HashSet::new();
    for row in rows {
        let value = row.first().cloned().unwrap_or(None);
        if distinct {
            let key = encode_scalar_key(&value);
            if seen.insert(key) {
                out.push(value);
            }
        } else {
            out.push(value);
        }
    }
    Ok(out)
}

fn first_field_item_type(field: &Field) -> Result<DataType, String> {
    match field.data_type() {
        DataType::List(item) => Ok(item.data_type().clone()),
        other => Err(format!(
            "array_agg intermediate struct field must be ARRAY, got {:?}",
            other
        )),
    }
}

fn first_item_type_from_update_input(input_type: &DataType) -> Result<DataType, String> {
    match input_type {
        DataType::Struct(fields) => {
            let first = fields.first().ok_or_else(|| {
                "array_agg input struct must contain at least 1 field".to_string()
            })?;
            Ok(first.data_type().clone())
        }
        other => Ok(other.clone()),
    }
}

fn unique_item_type_from_update_input(input_type: &DataType) -> Result<DataType, String> {
    match input_type {
        DataType::List(item) => Ok(item.data_type().clone()),
        DataType::Struct(fields) => {
            let first = fields.first().ok_or_else(|| {
                "array_unique_agg input struct must contain at least 1 field".to_string()
            })?;
            if let DataType::List(item) = first.data_type() {
                Ok(item.data_type().clone())
            } else {
                Ok(first.data_type().clone())
            }
        }
        other => Ok(other.clone()),
    }
}

fn first_item_type_from_intermediate_input(input_type: &DataType) -> Result<DataType, String> {
    match input_type {
        DataType::List(field) => match field.data_type() {
            DataType::Struct(fields) => {
                let first = fields.first().ok_or_else(|| {
                    "array_agg merge ARRAY<STRUCT> input must contain at least 1 struct field"
                        .to_string()
                })?;
                Ok(first.data_type().clone())
            }
            other => Ok(other.clone()),
        },
        DataType::Struct(fields) => {
            let first = fields.first().ok_or_else(|| {
                "array_agg merge struct input must contain at least 1 field".to_string()
            })?;
            first_field_item_type(first)
        }
        other => Err(format!(
            "array_agg merge input must be ARRAY or STRUCT, got {:?}",
            other
        )),
    }
}

fn append_value(state: &mut ArrayAggState, value: Option<ArrayAggValue>, distinct: bool) {
    if distinct {
        let key = encode_scalar_key(&value);
        if state.distinct_seen.insert(key) {
            state.rows.push(vec![value]);
        }
    } else {
        state.rows.push(vec![value]);
    }
}

fn unwrap_update_value_array<'a>(
    spec: &AggSpec,
    array: &'a ArrayRef,
) -> Result<(&'a ArrayRef, Option<&'a StructArray>), String> {
    let Some(wrapper) = array.as_any().downcast_ref::<StructArray>() else {
        return Ok((array, None));
    };
    let Some(expected_item_type) = spec.input_arg_type.as_ref() else {
        return Ok((array, None));
    };
    let first = wrapper.columns().first().ok_or_else(|| {
        "array_agg struct input must contain at least 1 field for value extraction".to_string()
    })?;
    if first.data_type() == expected_item_type {
        Ok((first, Some(wrapper)))
    } else {
        Ok((array, None))
    }
}

fn scalar_from_array(array: &ArrayRef, row: usize) -> Result<Option<ArrayAggValue>, String> {
    match array.data_type() {
        DataType::Boolean => {
            let arr = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| "failed to downcast to BooleanArray".to_string())?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(ArrayAggValue::Bool(arr.value(row))))
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
                Ok(Some(ArrayAggValue::Int64(arr.value(row) as i64)))
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
                Ok(Some(ArrayAggValue::Int64(arr.value(row) as i64)))
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
                Ok(Some(ArrayAggValue::Int64(arr.value(row) as i64)))
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
                Ok(Some(ArrayAggValue::Int64(arr.value(row))))
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
                Ok(Some(ArrayAggValue::Float64(arr.value(row) as f64)))
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
                Ok(Some(ArrayAggValue::Float64(arr.value(row))))
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
                Ok(Some(ArrayAggValue::Utf8(arr.value(row).to_string())))
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
                Ok(Some(ArrayAggValue::Date32(arr.value(row))))
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
                    Ok(Some(ArrayAggValue::Timestamp(arr.value(row))))
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
                    Ok(Some(ArrayAggValue::Timestamp(arr.value(row))))
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
                    Ok(Some(ArrayAggValue::Timestamp(arr.value(row))))
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
                    Ok(Some(ArrayAggValue::Timestamp(arr.value(row))))
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
                Ok(Some(ArrayAggValue::Decimal128(arr.value(row))))
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
                Ok(Some(ArrayAggValue::Decimal128(largeint::value_at(
                    arr, row,
                )?)))
            }
        }
        DataType::Struct(fields) => {
            let arr = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| "failed to downcast to StructArray".to_string())?;
            if arr.is_null(row) {
                return Ok(None);
            }
            if fields.is_empty() {
                return Err("unsupported scalar type: Struct(0) has no fields".to_string());
            }
            let mut values = Vec::with_capacity(arr.num_columns());
            for column in arr.columns() {
                values.push(scalar_from_array(column, row)?);
            }
            Ok(Some(ArrayAggValue::Struct(values)))
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
            let entries = arr.entries();
            let keys = entries.column(0).clone();
            let values = entries.column(1).clone();
            let mut out = Vec::with_capacity(end.saturating_sub(start));
            for idx in start..end {
                let key = scalar_from_array(&keys, idx)?;
                let value = scalar_from_array(&values, idx)?;
                out.push(Some(ArrayAggValue::Struct(vec![key, value])));
            }
            Ok(Some(ArrayAggValue::List(out)))
        }
        DataType::List(_) => {
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
            Ok(Some(ArrayAggValue::List(out)))
        }
        other => Err(format!("unsupported scalar type: {:?}", other)),
    }
}

fn build_scalar_array(
    output_type: &DataType,
    values: Vec<Option<ArrayAggValue>>,
) -> Result<ArrayRef, String> {
    match output_type {
        DataType::Boolean => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                match value {
                    Some(ArrayAggValue::Bool(v)) => out.push(Some(v)),
                    None => out.push(None),
                    _ => return Err("scalar output type mismatch for Boolean".to_string()),
                }
            }
            Ok(Arc::new(BooleanArray::from(out)) as ArrayRef)
        }
        DataType::Int8 => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                match value {
                    Some(ArrayAggValue::Int64(v)) => {
                        let v = i8::try_from(v).map_err(|_| "int8 overflow".to_string())?;
                        out.push(Some(v));
                    }
                    None => out.push(None),
                    _ => return Err("scalar output type mismatch for Int8".to_string()),
                }
            }
            Ok(Arc::new(Int8Array::from(out)) as ArrayRef)
        }
        DataType::Int16 => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                match value {
                    Some(ArrayAggValue::Int64(v)) => {
                        let v = i16::try_from(v).map_err(|_| "int16 overflow".to_string())?;
                        out.push(Some(v));
                    }
                    None => out.push(None),
                    _ => return Err("scalar output type mismatch for Int16".to_string()),
                }
            }
            Ok(Arc::new(Int16Array::from(out)) as ArrayRef)
        }
        DataType::Int32 => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                match value {
                    Some(ArrayAggValue::Int64(v)) => {
                        let v = i32::try_from(v).map_err(|_| "int32 overflow".to_string())?;
                        out.push(Some(v));
                    }
                    None => out.push(None),
                    _ => return Err("scalar output type mismatch for Int32".to_string()),
                }
            }
            Ok(Arc::new(Int32Array::from(out)) as ArrayRef)
        }
        DataType::Int64 => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                match value {
                    Some(ArrayAggValue::Int64(v)) => out.push(Some(v)),
                    None => out.push(None),
                    _ => return Err("scalar output type mismatch for Int64".to_string()),
                }
            }
            Ok(Arc::new(Int64Array::from(out)) as ArrayRef)
        }
        DataType::Float32 => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                match value {
                    Some(ArrayAggValue::Float64(v)) => out.push(Some(v as f32)),
                    None => out.push(None),
                    _ => return Err("scalar output type mismatch for Float32".to_string()),
                }
            }
            Ok(Arc::new(Float32Array::from(out)) as ArrayRef)
        }
        DataType::Float64 => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                match value {
                    Some(ArrayAggValue::Float64(v)) => out.push(Some(v)),
                    None => out.push(None),
                    _ => return Err("scalar output type mismatch for Float64".to_string()),
                }
            }
            Ok(Arc::new(Float64Array::from(out)) as ArrayRef)
        }
        DataType::Utf8 => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                match value {
                    Some(ArrayAggValue::Utf8(v)) => out.push(Some(v)),
                    None => out.push(None),
                    _ => return Err("scalar output type mismatch for Utf8".to_string()),
                }
            }
            Ok(Arc::new(StringArray::from(out)) as ArrayRef)
        }
        DataType::Date32 => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                match value {
                    Some(ArrayAggValue::Date32(v)) => out.push(Some(v)),
                    None => out.push(None),
                    _ => return Err("scalar output type mismatch for Date32".to_string()),
                }
            }
            Ok(Arc::new(Date32Array::from(out)) as ArrayRef)
        }
        DataType::Timestamp(unit, tz) => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                match value {
                    Some(ArrayAggValue::Timestamp(v)) => out.push(Some(v)),
                    None => out.push(None),
                    _ => return Err("scalar output type mismatch for Timestamp".to_string()),
                }
            }
            let tz = tz.as_deref().map(|s| s.to_string());
            let array: ArrayRef = match unit {
                TimeUnit::Second => {
                    let arr = TimestampSecondArray::from(out);
                    if let Some(tz) = tz {
                        Arc::new(arr.with_timezone(tz))
                    } else {
                        Arc::new(arr)
                    }
                }
                TimeUnit::Millisecond => {
                    let arr = TimestampMillisecondArray::from(out);
                    if let Some(tz) = tz {
                        Arc::new(arr.with_timezone(tz))
                    } else {
                        Arc::new(arr)
                    }
                }
                TimeUnit::Microsecond => {
                    let arr = TimestampMicrosecondArray::from(out);
                    if let Some(tz) = tz {
                        Arc::new(arr.with_timezone(tz))
                    } else {
                        Arc::new(arr)
                    }
                }
                TimeUnit::Nanosecond => {
                    let arr = TimestampNanosecondArray::from(out);
                    if let Some(tz) = tz {
                        Arc::new(arr.with_timezone(tz))
                    } else {
                        Arc::new(arr)
                    }
                }
            };
            Ok(array)
        }
        DataType::Decimal128(precision, scale) => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                match value {
                    Some(ArrayAggValue::Decimal128(v)) => out.push(Some(v)),
                    None => out.push(None),
                    _ => return Err("scalar output type mismatch for Decimal128".to_string()),
                }
            }
            let arr = Decimal128Array::from(out)
                .with_precision_and_scale(*precision, *scale)
                .map_err(|e| e.to_string())?;
            Ok(Arc::new(arr) as ArrayRef)
        }
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                match value {
                    Some(ArrayAggValue::Decimal128(v)) => out.push(Some(v)),
                    None => out.push(None),
                    _ => return Err("scalar output type mismatch for LargeInt".to_string()),
                }
            }
            largeint::array_from_i128(&out)
        }
        DataType::Struct(fields) => {
            if fields.is_empty() {
                return Err("array_agg cannot build Struct output with no fields".to_string());
            }
            let mut null_builder = NullBufferBuilder::new(values.len());
            let mut by_field: Vec<Vec<Option<ArrayAggValue>>> = (0..fields.len())
                .map(|_| Vec::with_capacity(values.len()))
                .collect();
            for value in values {
                match value {
                    None => {
                        null_builder.append_null();
                        for field_values in &mut by_field {
                            field_values.push(None);
                        }
                    }
                    Some(ArrayAggValue::Struct(items)) => {
                        if items.len() != fields.len() {
                            return Err(format!(
                                "scalar output type mismatch for Struct: expected {} fields, got {}",
                                fields.len(),
                                items.len()
                            ));
                        }
                        null_builder.append_non_null();
                        for (idx, item) in items.into_iter().enumerate() {
                            by_field[idx].push(item);
                        }
                    }
                    Some(other) => {
                        return Err(format!(
                            "scalar output type mismatch for Struct: got {:?}",
                            other
                        ));
                    }
                }
            }
            let mut columns = Vec::with_capacity(fields.len());
            for (idx, field) in fields.iter().enumerate() {
                columns.push(build_scalar_array(
                    field.data_type(),
                    std::mem::take(&mut by_field[idx]),
                )?);
            }
            let out = StructArray::new(fields.clone(), columns, null_builder.finish());
            Ok(Arc::new(out) as ArrayRef)
        }
        DataType::Map(entries_field, ordered) => {
            let DataType::Struct(entry_fields) = entries_field.data_type() else {
                return Err(format!(
                    "array_agg map entries field must be Struct, got {:?}",
                    entries_field.data_type()
                ));
            };
            if entry_fields.len() != 2 {
                return Err(format!(
                    "array_agg map entries field must contain key/value, got {} fields",
                    entry_fields.len()
                ));
            }

            let mut key_values = Vec::new();
            let mut value_values = Vec::new();
            let mut offsets = Vec::with_capacity(values.len() + 1);
            offsets.push(0_i32);
            let mut current: i64 = 0;
            let mut null_builder = NullBufferBuilder::new(values.len());

            for value in values {
                match value {
                    None => {
                        null_builder.append_null();
                        offsets.push(current as i32);
                    }
                    Some(ArrayAggValue::List(items)) => {
                        null_builder.append_non_null();
                        for item in items {
                            let Some(ArrayAggValue::Struct(entry)) = item else {
                                return Err("scalar output type mismatch for Map entry".to_string());
                            };
                            if entry.len() != 2 {
                                return Err(format!(
                                    "scalar output type mismatch for Map entry field count: expected 2, got {}",
                                    entry.len()
                                ));
                            }
                            key_values.push(entry[0].clone());
                            value_values.push(entry[1].clone());
                            current += 1;
                            if current > i32::MAX as i64 {
                                return Err("array_agg map offset overflow".to_string());
                            }
                        }
                        offsets.push(current as i32);
                    }
                    Some(other) => {
                        return Err(format!(
                            "scalar output type mismatch for Map: got {:?}",
                            other
                        ));
                    }
                }
            }

            let key_array = build_scalar_array(entry_fields[0].data_type(), key_values)?;
            let value_array = build_scalar_array(entry_fields[1].data_type(), value_values)?;
            let entries_fields = if key_array.null_count() > 0 && !entry_fields[0].is_nullable() {
                let mut adjusted = entry_fields.iter().cloned().collect::<Vec<_>>();
                adjusted[0] = Arc::new(Field::new(
                    entry_fields[0].name(),
                    entry_fields[0].data_type().clone(),
                    true,
                ));
                Fields::from(adjusted)
            } else {
                entry_fields.clone()
            };
            let entries =
                StructArray::new(entries_fields.clone(), vec![key_array, value_array], None);
            let entries_field = Arc::new(Field::new(
                entries_field.name(),
                DataType::Struct(entries_fields),
                entries_field.is_nullable(),
            ));
            let out = MapArray::new(
                entries_field,
                OffsetBuffer::new(offsets.into()),
                entries,
                null_builder.finish(),
                *ordered,
            );
            Ok(Arc::new(out) as ArrayRef)
        }
        DataType::List(field) => {
            let mut flattened = Vec::<Option<ArrayAggValue>>::new();
            let mut offsets = Vec::with_capacity(values.len() + 1);
            offsets.push(0_i32);
            let mut current: i64 = 0;
            let mut null_builder = NullBufferBuilder::new(values.len());
            for value in values {
                match value {
                    None => {
                        null_builder.append_null();
                        offsets.push(current as i32);
                    }
                    Some(ArrayAggValue::List(items)) => {
                        current += items.len() as i64;
                        if current > i32::MAX as i64 {
                            return Err("array_agg offset overflow".to_string());
                        }
                        flattened.extend(items);
                        null_builder.append_non_null();
                        offsets.push(current as i32);
                    }
                    Some(other) => {
                        return Err(format!(
                            "scalar output type mismatch for List: got {:?}",
                            other
                        ));
                    }
                }
            }
            let child_values = build_scalar_array(field.data_type(), flattened)?;
            let out = ListArray::new(
                field.clone(),
                OffsetBuffer::new(offsets.into()),
                child_values,
                null_builder.finish(),
            );
            Ok(Arc::new(out) as ArrayRef)
        }
        other => Err(format!("unsupported scalar output type: {:?}", other)),
    }
}

fn merge_input_list_array<'a>(
    array: &'a ArrayRef,
) -> Result<(&'a ListArray, Option<&'a StructArray>), String> {
    if let Some(list) = array.as_any().downcast_ref::<ListArray>() {
        return Ok((list, None));
    }
    if let Some(struct_arr) = array.as_any().downcast_ref::<StructArray>() {
        if struct_arr.num_columns() < 1 {
            return Err(format!(
                "array_agg merge struct input expects at least 1 field, got {}",
                struct_arr.num_columns()
            ));
        }
        let list = struct_arr
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| "array_agg merge struct field[0] must be ListArray".to_string())?;
        return Ok((list, Some(struct_arr)));
    }
    Err("array_agg merge input must be ListArray or StructArray".to_string())
}

impl AggregateFunction for ArrayAggAgg {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        input_type: Option<&DataType>,
        input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        let input_type = input_type.ok_or_else(|| "array_agg input type missing".to_string())?;
        let (base_name, is_asc_order, nulls_first) = parse_array_agg_metadata(func.name.as_str())?;
        let kind = match base_name.as_str() {
            "array_agg" => AggKind::ArrayAgg {
                is_distinct: false,
                is_asc_order,
                nulls_first,
            },
            "array_agg_distinct" => AggKind::ArrayAgg {
                is_distinct: true,
                is_asc_order,
                nulls_first,
            },
            "array_unique_agg" => AggKind::ArrayUniqueAgg,
            other => return Err(format!("unsupported array agg function: {}", other)),
        };

        let item_type = if input_is_intermediate {
            first_item_type_from_intermediate_input(input_type)?
        } else if matches!(kind, AggKind::ArrayUniqueAgg) {
            unique_item_type_from_update_input(input_type)?
        } else {
            first_item_type_from_update_input(input_type)?
        };

        let output_list_type =
            DataType::List(Arc::new(Field::new("item", item_type.clone(), true)));
        let intermediate_type = if input_is_intermediate {
            input_type.clone()
        } else {
            func.types
                .as_ref()
                .and_then(|t| t.intermediate_type.as_ref())
                .cloned()
                .unwrap_or_else(|| output_list_type.clone())
        };

        Ok(AggSpec {
            kind,
            output_type: output_list_type,
            intermediate_type,
            input_arg_type: Some(item_type),
            count_all: false,
        })
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        match kind {
            AggKind::ArrayAgg { .. } | AggKind::ArrayUniqueAgg => (
                std::mem::size_of::<ArrayAggState>(),
                std::mem::align_of::<ArrayAggState>(),
            ),
            other => unreachable!("unexpected kind for array_agg: {:?}", other),
        }
    }

    fn build_input_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "array_agg input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn build_merge_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "array_agg merge input missing".to_string())?;
        let _ = merge_input_list_array(arr)?;
        Ok(AggInputView::Any(arr))
    }

    fn init_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::write(ptr as *mut ArrayAggState, ArrayAggState::default());
        }
    }

    fn drop_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::drop_in_place(ptr as *mut ArrayAggState);
        }
    }

    fn update_batch(
        &self,
        spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        let AggInputView::Any(array) = input else {
            return Err("array_agg batch input type mismatch".to_string());
        };
        if matches!(spec.kind, AggKind::ArrayUniqueAgg) {
            if let Some(list) = array.as_any().downcast_ref::<ListArray>() {
                let values = list.values();
                let offsets = list.value_offsets();
                for (row, &base) in state_ptrs.iter().enumerate() {
                    if list.is_null(row) {
                        continue;
                    }
                    let state =
                        unsafe { &mut *((base as *mut u8).add(offset) as *mut ArrayAggState) };
                    let start = offsets[row] as usize;
                    let end = offsets[row + 1] as usize;
                    for idx in start..end {
                        let value = scalar_from_array(&values, idx)?;
                        append_value(state, value, true);
                    }
                }
                return Ok(());
            }
        }

        let (value_array, wrapper_array) = unwrap_update_value_array(spec, array)?;
        if let Some(wrapper) = wrapper_array {
            for (row, &base) in state_ptrs.iter().enumerate() {
                let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut ArrayAggState) };
                let mut row_values = Vec::with_capacity(wrapper.num_columns());
                if wrapper.is_null(row) {
                    row_values.resize(wrapper.num_columns(), None);
                } else {
                    for col in wrapper.columns() {
                        row_values.push(scalar_from_array(col, row)?);
                    }
                }
                state.rows.push(row_values);
            }
        } else {
            for (row, &base) in state_ptrs.iter().enumerate() {
                let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut ArrayAggState) };
                state.rows.push(vec![scalar_from_array(value_array, row)?]);
            }
        }
        Ok(())
    }

    fn merge_batch(
        &self,
        spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        let AggInputView::Any(array) = input else {
            return Err("array_agg merge input type mismatch".to_string());
        };
        let (first_list, struct_arr) = merge_input_list_array(array)?;
        let first_values = first_list.values();
        let first_offsets = first_list.value_offsets();

        for (row, &base) in state_ptrs.iter().enumerate() {
            if struct_arr.map(|s| s.is_null(row)).unwrap_or(false) || first_list.is_null(row) {
                continue;
            }
            let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut ArrayAggState) };
            let start = first_offsets[row] as usize;
            let end = first_offsets[row + 1] as usize;
            for idx in start..end {
                if matches!(spec.kind, AggKind::ArrayUniqueAgg) {
                    let value = scalar_from_array(&first_values, idx)?;
                    append_value(state, value, true);
                    continue;
                }
                if let Some(struct_arr) = struct_arr {
                    let mut row_values = Vec::with_capacity(struct_arr.num_columns());
                    for (col_idx, col) in struct_arr.columns().iter().enumerate() {
                        let list = col.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                            format!(
                                "array_agg merge struct field[{}] must be ListArray",
                                col_idx
                            )
                        })?;
                        if list.is_null(row) {
                            row_values.push(None);
                            continue;
                        }
                        let offsets = list.value_offsets();
                        if offsets[row] != first_offsets[row]
                            || offsets[row + 1] != first_offsets[row + 1]
                        {
                            return Err(format!(
                                "array_agg merge struct field[{}] offsets mismatch",
                                col_idx
                            ));
                        }
                        row_values.push(scalar_from_array(&list.values(), idx)?);
                    }
                    state.rows.push(row_values);
                } else {
                    state
                        .rows
                        .push(vec![scalar_from_array(&first_values, idx)?]);
                }
            }
        }
        Ok(())
    }

    fn build_array(
        &self,
        spec: &AggSpec,
        offset: usize,
        group_states: &[AggStatePtr],
        output_intermediate: bool,
    ) -> Result<ArrayRef, String> {
        let target_type = if output_intermediate {
            &spec.intermediate_type
        } else {
            &spec.output_type
        };
        match target_type {
            DataType::List(list_field) => {
                let mut flattened = Vec::<Option<ArrayAggValue>>::new();
                let mut out_offsets = Vec::with_capacity(group_states.len() + 1);
                out_offsets.push(0_i32);
                let mut current: i64 = 0;
                for &base in group_states {
                    let state =
                        unsafe { &*((base as *mut u8).add(offset) as *const ArrayAggState) };
                    let values = if output_intermediate {
                        state
                            .rows
                            .iter()
                            .map(|row| row.first().cloned().unwrap_or(None))
                            .collect::<Vec<_>>()
                    } else {
                        extract_final_values(spec, state)?
                    };
                    current += values.len() as i64;
                    if current > i32::MAX as i64 {
                        return Err("array_agg offset overflow".to_string());
                    }
                    out_offsets.push(current as i32);
                    flattened.extend(values);
                }
                let values = build_scalar_array(list_field.data_type(), flattened)?;
                let list_out = ListArray::new(
                    list_field.clone(),
                    OffsetBuffer::new(out_offsets.into()),
                    values,
                    None,
                );
                Ok(Arc::new(list_out) as ArrayRef)
            }
            DataType::Struct(fields) => {
                let first = fields.first().ok_or_else(|| {
                    "array_agg struct target type must contain at least 1 field".to_string()
                })?;
                let DataType::List(first_list_field) = first.data_type() else {
                    return Err(format!(
                        "array_agg struct field[0] must be List, got {:?}",
                        first.data_type()
                    ));
                };
                let mut flattened_by_col: Vec<Vec<Option<ArrayAggValue>>> =
                    (0..fields.len()).map(|_| Vec::new()).collect();
                let mut out_offsets = Vec::with_capacity(group_states.len() + 1);
                out_offsets.push(0_i32);
                let mut current: i64 = 0;

                for &base in group_states {
                    let state =
                        unsafe { &*((base as *mut u8).add(offset) as *const ArrayAggState) };
                    let rows = if output_intermediate {
                        state.rows.clone()
                    } else {
                        extract_final_values(spec, state)?
                            .into_iter()
                            .map(|value| vec![value])
                            .collect()
                    };
                    current += rows.len() as i64;
                    if current > i32::MAX as i64 {
                        return Err("array_agg offset overflow".to_string());
                    }
                    out_offsets.push(current as i32);
                    for row in rows {
                        for col_idx in 0..fields.len() {
                            flattened_by_col[col_idx]
                                .push(row.get(col_idx).cloned().unwrap_or(None));
                        }
                    }
                }

                let mut columns = Vec::with_capacity(fields.len());
                for (idx, field) in fields.iter().enumerate() {
                    let DataType::List(list_field) = field.data_type() else {
                        return Err(format!(
                            "array_agg struct field[{}] must be List, got {:?}",
                            idx,
                            field.data_type()
                        ));
                    };
                    let values = if idx == 0 {
                        build_scalar_array(
                            first_list_field.data_type(),
                            flattened_by_col[idx].clone(),
                        )?
                    } else {
                        build_scalar_array(list_field.data_type(), flattened_by_col[idx].clone())?
                    };
                    let list = ListArray::new(
                        list_field.clone(),
                        OffsetBuffer::new(out_offsets.clone().into()),
                        values,
                        None,
                    );
                    columns.push(Arc::new(list) as ArrayRef);
                }

                Ok(Arc::new(StructArray::new(fields.clone(), columns, None)) as ArrayRef)
            }
            other => Err(format!(
                "array_agg target type must be List or Struct(List...), got {:?}",
                other
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::DataType;
    use arrow_buffer::OffsetBuffer;
    use std::mem::MaybeUninit;

    fn list_i64_type() -> DataType {
        DataType::List(Arc::new(Field::new("item", DataType::Int64, true)))
    }

    fn make_func(name: &str) -> AggFunction {
        AggFunction {
            name: name.to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(crate::exec::node::aggregate::AggTypeSignature {
                intermediate_type: Some(list_i64_type()),
                output_type: Some(list_i64_type()),
                input_arg_type: Some(DataType::Int64),
            }),
        }
    }

    #[test]
    fn test_array_agg_spec() {
        let func = make_func("array_agg");
        let spec = ArrayAggAgg
            .build_spec_from_type(&func, Some(&DataType::Int64), false)
            .unwrap();
        assert!(matches!(spec.kind, AggKind::ArrayAgg { .. }));
        assert_eq!(spec.output_type, list_i64_type());
    }

    #[test]
    fn test_array_agg_collects_values_and_nulls() {
        let func = make_func("array_agg");
        let spec = ArrayAggAgg
            .build_spec_from_type(&func, Some(&DataType::Int64), false)
            .unwrap();

        let values = Arc::new(Int64Array::from(vec![Some(1), None, Some(2)])) as ArrayRef;
        let input = AggInputView::Any(&values);

        let mut state = MaybeUninit::<ArrayAggState>::uninit();
        ArrayAggAgg.init_state(&spec, state.as_mut_ptr() as *mut u8);
        let state_ptr = state.as_mut_ptr() as AggStatePtr;
        let state_ptrs = vec![state_ptr; 3];
        ArrayAggAgg
            .update_batch(&spec, 0, &state_ptrs, &input)
            .unwrap();
        let out = ArrayAggAgg
            .build_array(&spec, 0, &[state_ptr], false)
            .unwrap();
        ArrayAggAgg.drop_state(&spec, state.as_mut_ptr() as *mut u8);

        let out = out.as_any().downcast_ref::<ListArray>().unwrap();
        let values = out.values().as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(values.len(), 3);
        assert_eq!(values.value(0), 1);
        assert!(values.is_null(1));
        assert_eq!(values.value(2), 2);
    }

    #[test]
    fn test_array_agg_distinct_deduplicates_values() {
        let func = make_func("array_agg_distinct");
        let spec = ArrayAggAgg
            .build_spec_from_type(&func, Some(&DataType::Int64), false)
            .unwrap();

        let values = Arc::new(Int64Array::from(vec![
            Some(1),
            Some(1),
            None,
            None,
            Some(2),
        ])) as ArrayRef;
        let input = AggInputView::Any(&values);

        let mut state = MaybeUninit::<ArrayAggState>::uninit();
        ArrayAggAgg.init_state(&spec, state.as_mut_ptr() as *mut u8);
        let state_ptr = state.as_mut_ptr() as AggStatePtr;
        let state_ptrs = vec![state_ptr; 5];
        ArrayAggAgg
            .update_batch(&spec, 0, &state_ptrs, &input)
            .unwrap();
        let out = ArrayAggAgg
            .build_array(&spec, 0, &[state_ptr], false)
            .unwrap();
        ArrayAggAgg.drop_state(&spec, state.as_mut_ptr() as *mut u8);

        let out = out.as_any().downcast_ref::<ListArray>().unwrap();
        let values = out.values().as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(values.len(), 3);
        assert_eq!(values.value(0), 1);
        assert!(values.is_null(1));
        assert_eq!(values.value(2), 2);
    }

    #[test]
    fn test_array_agg_collects_list_items() {
        let func = AggFunction {
            name: "array_agg".to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: None,
        };
        let input_type = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
        let spec = ArrayAggAgg
            .build_spec_from_type(&func, Some(&input_type), false)
            .unwrap();

        let list_values = Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef;
        let input = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Utf8, true)),
            OffsetBuffer::new(vec![0, 2, 3].into()),
            list_values,
            None,
        )) as ArrayRef;
        let input = AggInputView::Any(&input);

        let mut state = MaybeUninit::<ArrayAggState>::uninit();
        ArrayAggAgg.init_state(&spec, state.as_mut_ptr() as *mut u8);
        let state_ptr = state.as_mut_ptr() as AggStatePtr;
        let state_ptrs = vec![state_ptr; 2];
        ArrayAggAgg
            .update_batch(&spec, 0, &state_ptrs, &input)
            .unwrap();
        let out = ArrayAggAgg
            .build_array(&spec, 0, &[state_ptr], false)
            .unwrap();
        ArrayAggAgg.drop_state(&spec, state.as_mut_ptr() as *mut u8);

        let out = out.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(out.len(), 1);
        let nested = out.values().as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(nested.len(), 2);
        let flat = nested
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(flat.len(), 3);
        assert_eq!(flat.value(0), "a");
        assert_eq!(flat.value(1), "b");
        assert_eq!(flat.value(2), "c");
    }

    #[test]
    fn test_array_unique_agg_alias() {
        let func = make_func("array_unique_agg");
        let spec = super::build_spec_from_type(&func, Some(&DataType::Int64), false).unwrap();
        assert!(matches!(spec.kind, AggKind::ArrayUniqueAgg));
    }
}
