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
use std::collections::HashMap;

use arrow::array::{Array, ArrayRef, MapArray};
use arrow::datatypes::DataType;

use crate::exec::node::aggregate::AggFunction;

use super::super::*;
use super::AggregateFunction;
use super::common::{
    AggScalarValue, build_scalar_array, compare_scalar_values, scalar_from_array,
};

pub(super) struct SumMapAgg;

#[derive(Clone, Debug, Default)]
struct SumMapState {
    saw_non_null_map: bool,
    indexes: HashMap<Vec<u8>, usize>,
    entries: Vec<(Option<AggScalarValue>, AggScalarValue)>,
}

impl AggregateFunction for SumMapAgg {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        input_type: Option<&DataType>,
        input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        let input_type = input_type.ok_or_else(|| "sum_map input type missing".to_string())?;
        let output_type = func
            .types
            .as_ref()
            .and_then(|sig| sig.output_type.clone())
            .unwrap_or_else(|| input_type.clone());
        let intermediate_type = func
            .types
            .as_ref()
            .and_then(|sig| sig.intermediate_type.clone())
            .unwrap_or_else(|| output_type.clone());

        let check_type = if input_is_intermediate {
            &intermediate_type
        } else {
            input_type
        };
        if !matches!(check_type, DataType::Map(_, _)) {
            return Err(format!("sum_map expects MAP input, got {:?}", check_type));
        }

        Ok(AggSpec {
            kind: AggKind::SumMap,
            output_type,
            intermediate_type,
            input_arg_type: func.types.as_ref().and_then(|sig| sig.input_arg_type.clone()),
            count_all: false,
        })
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        match kind {
            AggKind::SumMap => (
                std::mem::size_of::<SumMapState>(),
                std::mem::align_of::<SumMapState>(),
            ),
            other => unreachable!("unexpected kind for sum_map: {:?}", other),
        }
    }

    fn build_input_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "sum_map input missing".to_string())?;
        let _ = arr
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| "sum_map input must be MapArray".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn build_merge_view<'a>(
        &self,
        spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        self.build_input_view(spec, array)
    }

    fn init_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::write(ptr as *mut SumMapState, SumMapState::default());
        }
    }

    fn drop_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::drop_in_place(ptr as *mut SumMapState);
        }
    }

    fn update_batch(
        &self,
        _spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        let AggInputView::Any(array) = input else {
            return Err("sum_map input type mismatch".to_string());
        };
        let map = array
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| "sum_map input must be MapArray".to_string())?;
        merge_map_array(offset, state_ptrs, map)
    }

    fn merge_batch(
        &self,
        spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        self.update_batch(spec, offset, state_ptrs, input)
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
        let mut out = Vec::with_capacity(group_states.len());
        for &base in group_states {
            let state = unsafe { &*((base as *mut u8).add(offset) as *const SumMapState) };
            if !state.saw_non_null_map {
                out.push(None);
                continue;
            }
            let mut entries = state
                .entries
                .iter()
                .map(|(k, v)| (k.clone(), Some(v.clone())))
                .collect::<Vec<_>>();
            entries.sort_by(compare_map_entry_keys);
            out.push(Some(AggScalarValue::Map(entries)));
        }
        build_scalar_array(target_type, out)
    }
}

fn merge_map_array(
    offset: usize,
    state_ptrs: &[AggStatePtr],
    map: &MapArray,
) -> Result<(), String> {
    let keys = map.keys().clone();
    let values = map.values().clone();
    let offsets = map.value_offsets();

    for (row, &base) in state_ptrs.iter().enumerate() {
        if map.is_null(row) {
            continue;
        }
        let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut SumMapState) };
        state.saw_non_null_map = true;
        let start = offsets[row] as usize;
        let end = offsets[row + 1] as usize;
        for idx in start..end {
            let Some(value) = scalar_from_array(&values, idx)? else {
                continue;
            };
            let key = scalar_from_array(&keys, idx)?;
            let fp = fingerprint_optional_scalar(&key);
            if let Some(existing_idx) = state.indexes.get(&fp).copied() {
                sum_scalar_in_place(&mut state.entries[existing_idx].1, &value)?;
            } else {
                let insert_idx = state.entries.len();
                state.indexes.insert(fp, insert_idx);
                state.entries.push((key, value));
            }
        }
    }
    Ok(())
}

fn compare_map_entry_keys(
    left: &(Option<AggScalarValue>, Option<AggScalarValue>),
    right: &(Option<AggScalarValue>, Option<AggScalarValue>),
) -> Ordering {
    compare_optional_scalars(&left.0, &right.0).unwrap_or(Ordering::Equal)
}

fn compare_optional_scalars(
    left: &Option<AggScalarValue>,
    right: &Option<AggScalarValue>,
) -> Result<Ordering, String> {
    match (left, right) {
        (None, None) => Ok(Ordering::Equal),
        (None, Some(_)) => Ok(Ordering::Less),
        (Some(_), None) => Ok(Ordering::Greater),
        (Some(left), Some(right)) => compare_scalar_values(left, right),
    }
}

fn sum_scalar_in_place(target: &mut AggScalarValue, incoming: &AggScalarValue) -> Result<(), String> {
    match (target, incoming) {
        (AggScalarValue::Int64(left), AggScalarValue::Int64(right)) => {
            *left = left
                .checked_add(*right)
                .ok_or_else(|| "sum_map int64 overflow".to_string())?;
            Ok(())
        }
        (AggScalarValue::Float64(left), AggScalarValue::Float64(right)) => {
            *left += *right;
            Ok(())
        }
        (AggScalarValue::Decimal128(left), AggScalarValue::Decimal128(right)) => {
            *left = left
                .checked_add(*right)
                .ok_or_else(|| "sum_map decimal128 overflow".to_string())?;
            Ok(())
        }
        (AggScalarValue::Decimal256(left), AggScalarValue::Decimal256(right)) => {
            *left = left
                .checked_add(*right)
                .ok_or_else(|| "sum_map decimal256 overflow".to_string())?;
            Ok(())
        }
        _ => Err("sum_map value type mismatch".to_string()),
    }
}

fn fingerprint_optional_scalar(value: &Option<AggScalarValue>) -> Vec<u8> {
    let mut out = Vec::new();
    match value {
        None => out.push(0),
        Some(value) => {
            out.push(1);
            encode_scalar(&mut out, value);
        }
    }
    out
}

fn encode_scalar(out: &mut Vec<u8>, value: &AggScalarValue) {
    match value {
        AggScalarValue::Bool(v) => {
            out.push(1);
            out.push(if *v { 1 } else { 0 });
        }
        AggScalarValue::Int64(v) => {
            out.push(2);
            out.extend_from_slice(&v.to_le_bytes());
        }
        AggScalarValue::Float64(v) => {
            out.push(3);
            out.extend_from_slice(&v.to_bits().to_le_bytes());
        }
        AggScalarValue::Utf8(v) => {
            out.push(4);
            let len = v.len() as u32;
            out.extend_from_slice(&len.to_le_bytes());
            out.extend_from_slice(v.as_bytes());
        }
        AggScalarValue::Date32(v) => {
            out.push(5);
            out.extend_from_slice(&v.to_le_bytes());
        }
        AggScalarValue::Timestamp(v) => {
            out.push(6);
            out.extend_from_slice(&v.to_le_bytes());
        }
        AggScalarValue::Decimal128(v) => {
            out.push(7);
            out.extend_from_slice(&v.to_le_bytes());
        }
        AggScalarValue::Struct(items) => {
            out.push(8);
            out.extend_from_slice(&(items.len() as u32).to_le_bytes());
            for item in items {
                match item {
                    Some(value) => {
                        out.push(1);
                        encode_scalar(out, value);
                    }
                    None => out.push(0),
                }
            }
        }
        AggScalarValue::Map(items) => {
            out.push(9);
            out.extend_from_slice(&(items.len() as u32).to_le_bytes());
            for (key, value) in items {
                match key {
                    Some(value) => {
                        out.push(1);
                        encode_scalar(out, value);
                    }
                    None => out.push(0),
                }
                match value {
                    Some(value) => {
                        out.push(1);
                        encode_scalar(out, value);
                    }
                    None => out.push(0),
                }
            }
        }
        AggScalarValue::List(items) => {
            out.push(10);
            out.extend_from_slice(&(items.len() as u32).to_le_bytes());
            for item in items {
                match item {
                    Some(value) => {
                        out.push(1);
                        encode_scalar(out, value);
                    }
                    None => out.push(0),
                }
            }
        }
        AggScalarValue::Decimal256(v) => {
            out.push(11);
            let text = v.to_string();
            out.extend_from_slice(&(text.len() as u32).to_le_bytes());
            out.extend_from_slice(text.as_bytes());
        }
    }
}
