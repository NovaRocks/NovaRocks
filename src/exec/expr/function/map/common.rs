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
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Decimal256Array,
    FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, MapArray, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Fields};
use arrow_buffer::OffsetBuffer;
use std::cmp::Ordering;
use std::sync::Arc;

use crate::common::largeint;

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

pub(super) fn compare_key_to_target(
    keys: &ArrayRef,
    key_idx: usize,
    targets: &ArrayRef,
    target_row: usize,
) -> Result<bool, String> {
    let target_idx = row_index(target_row, targets.len());
    compare_keys_at(keys, key_idx, targets, target_idx)
}

pub(super) fn compare_keys_at(
    keys: &ArrayRef,
    key_idx: usize,
    targets: &ArrayRef,
    target_idx: usize,
) -> Result<bool, String> {
    if keys.is_null(key_idx) || targets.is_null(target_idx) {
        return Ok(false);
    }
    if keys.data_type() != targets.data_type() {
        return Err(format!(
            "map key type mismatch: {:?} vs {:?}",
            keys.data_type(),
            targets.data_type()
        ));
    }

    match keys.data_type() {
        DataType::Int8 => {
            let l = keys.as_any().downcast_ref::<Int8Array>().unwrap();
            let r = targets.as_any().downcast_ref::<Int8Array>().unwrap();
            Ok(l.value(key_idx) == r.value(target_idx))
        }
        DataType::Int16 => {
            let l = keys.as_any().downcast_ref::<Int16Array>().unwrap();
            let r = targets.as_any().downcast_ref::<Int16Array>().unwrap();
            Ok(l.value(key_idx) == r.value(target_idx))
        }
        DataType::Int32 => {
            let l = keys.as_any().downcast_ref::<Int32Array>().unwrap();
            let r = targets.as_any().downcast_ref::<Int32Array>().unwrap();
            Ok(l.value(key_idx) == r.value(target_idx))
        }
        DataType::Int64 => {
            let l = keys.as_any().downcast_ref::<Int64Array>().unwrap();
            let r = targets.as_any().downcast_ref::<Int64Array>().unwrap();
            Ok(l.value(key_idx) == r.value(target_idx))
        }
        DataType::Float32 => {
            let l = keys.as_any().downcast_ref::<Float32Array>().unwrap();
            let r = targets.as_any().downcast_ref::<Float32Array>().unwrap();
            Ok(l.value(key_idx) == r.value(target_idx))
        }
        DataType::Float64 => {
            let l = keys.as_any().downcast_ref::<Float64Array>().unwrap();
            let r = targets.as_any().downcast_ref::<Float64Array>().unwrap();
            Ok(l.value(key_idx) == r.value(target_idx))
        }
        DataType::Boolean => {
            let l = keys.as_any().downcast_ref::<BooleanArray>().unwrap();
            let r = targets.as_any().downcast_ref::<BooleanArray>().unwrap();
            Ok(l.value(key_idx) == r.value(target_idx))
        }
        DataType::Utf8 => {
            let l = keys.as_any().downcast_ref::<StringArray>().unwrap();
            let r = targets.as_any().downcast_ref::<StringArray>().unwrap();
            Ok(l.value(key_idx) == r.value(target_idx))
        }
        DataType::Date32 => {
            let l = keys.as_any().downcast_ref::<Date32Array>().unwrap();
            let r = targets.as_any().downcast_ref::<Date32Array>().unwrap();
            Ok(l.value(key_idx) == r.value(target_idx))
        }
        DataType::Decimal128(_, _) => {
            let l = keys.as_any().downcast_ref::<Decimal128Array>().unwrap();
            let r = targets.as_any().downcast_ref::<Decimal128Array>().unwrap();
            Ok(l.value(key_idx) == r.value(target_idx))
        }
        DataType::Decimal256(_, _) => {
            let l = keys.as_any().downcast_ref::<Decimal256Array>().unwrap();
            let r = targets.as_any().downcast_ref::<Decimal256Array>().unwrap();
            Ok(l.value(key_idx) == r.value(target_idx))
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None) => {
            let l = keys
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .unwrap();
            let r = targets
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .unwrap();
            Ok(l.value(key_idx) == r.value(target_idx))
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None) => {
            let l = keys
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            let r = targets
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            Ok(l.value(key_idx) == r.value(target_idx))
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None) => {
            let l = keys
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            let r = targets
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            Ok(l.value(key_idx) == r.value(target_idx))
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None) => {
            let l = keys
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            let r = targets
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            Ok(l.value(key_idx) == r.value(target_idx))
        }
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            let l = keys
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| "failed to downcast key to FixedSizeBinaryArray".to_string())?;
            let r = targets
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| "failed to downcast target to FixedSizeBinaryArray".to_string())?;
            let lv = largeint::i128_from_be_bytes(l.value(key_idx))
                .map_err(|e| format!("map key LARGEINT decode failed: {}", e))?;
            let rv = largeint::i128_from_be_bytes(r.value(target_idx))
                .map_err(|e| format!("map key LARGEINT decode failed: {}", e))?;
            Ok(lv == rv)
        }
        other => Err(format!("map key compare unsupported type: {:?}", other)),
    }
}

pub(super) fn output_list_field(
    output_type: Option<&DataType>,
    default_item_type: &DataType,
    fn_name: &str,
) -> Result<Arc<Field>, String> {
    match output_type {
        Some(DataType::List(field)) => Ok(field.clone()),
        Some(other) => Err(format!(
            "{} output type must be List, got {:?}",
            fn_name, other
        )),
        None => Ok(Arc::new(Field::new(
            "item",
            default_item_type.clone(),
            true,
        ))),
    }
}

pub(crate) fn sorted_map_offsets_and_indices(
    map: &MapArray,
) -> Result<(OffsetBuffer<i32>, Vec<u32>), String> {
    let offsets = map.value_offsets();
    let keys = map.keys();

    let mut sorted_indices: Vec<u32> = Vec::new();
    let mut sorted_offsets: Vec<i32> = Vec::with_capacity(map.len() + 1);
    sorted_offsets.push(0);

    for row in 0..map.len() {
        let current = *sorted_offsets.last().unwrap_or(&0);
        if map.is_null(row) {
            sorted_offsets.push(current);
            continue;
        }

        let start = offsets[row] as usize;
        let end = offsets[row + 1] as usize;
        let mut row_indices: Vec<usize> = (start..end).collect();
        sort_indices_by_key(keys, &mut row_indices)?;

        for idx in row_indices {
            let idx_u32 = u32::try_from(idx)
                .map_err(|_| format!("map entry index overflow while sorting: {}", idx))?;
            sorted_indices.push(idx_u32);
        }
        let next = i32::try_from(sorted_indices.len())
            .map_err(|_| "map offset overflow while sorting entries".to_string())?;
        sorted_offsets.push(next);
    }

    Ok((OffsetBuffer::new(sorted_offsets.into()), sorted_indices))
}

fn sort_indices_by_key(keys: &ArrayRef, indices: &mut [usize]) -> Result<(), String> {
    for i in 1..indices.len() {
        let mut j = i;
        while j > 0 {
            let ord = compare_keys_ordered(keys, indices[j - 1], indices[j])?;
            if ord.is_gt() {
                indices.swap(j - 1, j);
                j -= 1;
            } else {
                break;
            }
        }
    }
    Ok(())
}

fn compare_keys_ordered(keys: &ArrayRef, left: usize, right: usize) -> Result<Ordering, String> {
    match (keys.is_null(left), keys.is_null(right)) {
        (true, true) => return Ok(Ordering::Equal),
        (true, false) => return Ok(Ordering::Less),
        (false, true) => return Ok(Ordering::Greater),
        (false, false) => {}
    }

    match keys.data_type() {
        DataType::Int8 => {
            let arr = keys.as_any().downcast_ref::<Int8Array>().unwrap();
            Ok(arr.value(left).cmp(&arr.value(right)))
        }
        DataType::Int16 => {
            let arr = keys.as_any().downcast_ref::<Int16Array>().unwrap();
            Ok(arr.value(left).cmp(&arr.value(right)))
        }
        DataType::Int32 => {
            let arr = keys.as_any().downcast_ref::<Int32Array>().unwrap();
            Ok(arr.value(left).cmp(&arr.value(right)))
        }
        DataType::Int64 => {
            let arr = keys.as_any().downcast_ref::<Int64Array>().unwrap();
            Ok(arr.value(left).cmp(&arr.value(right)))
        }
        DataType::Float32 => {
            let arr = keys.as_any().downcast_ref::<Float32Array>().unwrap();
            Ok(arr
                .value(left)
                .partial_cmp(&arr.value(right))
                .unwrap_or(Ordering::Equal))
        }
        DataType::Float64 => {
            let arr = keys.as_any().downcast_ref::<Float64Array>().unwrap();
            Ok(arr
                .value(left)
                .partial_cmp(&arr.value(right))
                .unwrap_or(Ordering::Equal))
        }
        DataType::Boolean => {
            let arr = keys.as_any().downcast_ref::<BooleanArray>().unwrap();
            Ok(arr.value(left).cmp(&arr.value(right)))
        }
        DataType::Utf8 => {
            let arr = keys.as_any().downcast_ref::<StringArray>().unwrap();
            Ok(arr.value(left).cmp(arr.value(right)))
        }
        DataType::Date32 => {
            let arr = keys.as_any().downcast_ref::<Date32Array>().unwrap();
            Ok(arr.value(left).cmp(&arr.value(right)))
        }
        DataType::Decimal128(_, _) => {
            let arr = keys.as_any().downcast_ref::<Decimal128Array>().unwrap();
            Ok(arr.value(left).cmp(&arr.value(right)))
        }
        DataType::Decimal256(_, _) => {
            let arr = keys.as_any().downcast_ref::<Decimal256Array>().unwrap();
            Ok(arr.value(left).cmp(&arr.value(right)))
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None) => {
            let arr = keys
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .unwrap();
            Ok(arr.value(left).cmp(&arr.value(right)))
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None) => {
            let arr = keys
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            Ok(arr.value(left).cmp(&arr.value(right)))
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None) => {
            let arr = keys
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            Ok(arr.value(left).cmp(&arr.value(right)))
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None) => {
            let arr = keys
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            Ok(arr.value(left).cmp(&arr.value(right)))
        }
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            let arr = keys
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| "failed to downcast map key to FixedSizeBinaryArray".to_string())?;
            let left_value = largeint::i128_from_be_bytes(arr.value(left))
                .map_err(|e| format!("map key LARGEINT decode failed: {}", e))?;
            let right_value = largeint::i128_from_be_bytes(arr.value(right))
                .map_err(|e| format!("map key LARGEINT decode failed: {}", e))?;
            Ok(left_value.cmp(&right_value))
        }
        other => Err(format!(
            "map key ordered compare unsupported type: {:?}",
            other
        )),
    }
}

pub(super) fn output_map_field(
    output_type: Option<&DataType>,
    key_type: &DataType,
    value_type: &DataType,
    fn_name: &str,
) -> Result<(Arc<Field>, bool), String> {
    match output_type {
        Some(DataType::Map(field, ordered)) => Ok((field.clone(), *ordered)),
        Some(other) => Err(format!(
            "{} output type must be Map, got {:?}",
            fn_name, other
        )),
        None => {
            let entry_fields = Fields::from(vec![
                Arc::new(Field::new("key", key_type.clone(), false)),
                Arc::new(Field::new("value", value_type.clone(), true)),
            ]);
            Ok((
                Arc::new(Field::new("entries", DataType::Struct(entry_fields), false)),
                false,
            ))
        }
    }
}
