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
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, MapArray, NullArray};
use arrow::datatypes::DataType;

use crate::exec::expr::agg::{
    AggregateAllocator, AggregateHashMap, AggregateVec, aggregate_hash_map,
};
use crate::exec::node::aggregate::AggFunction;
use crate::runtime::mem_tracker::{MemTracker, process_mem_tracker};

use super::super::*;
use super::AggregateFunction;
use super::common::{
    AggScalarValue, TrackedAggScalarValue, build_scalar_array, compare_scalar_values,
    tracked_optional_key_fingerprint, tracked_scalar_from_array, tracked_scalar_to_output,
};

pub(super) struct SumMapAgg;

#[derive(Debug)]
struct SumMapState {
    allocator: AggregateAllocator,
    saw_non_null_map: bool,
    indexes: AggregateHashMap<AggregateVec<u8>, usize>,
    entries: AggregateVec<(Option<TrackedAggScalarValue>, TrackedAggScalarValue)>,
}

impl SumMapState {
    fn new(tracker: Arc<MemTracker>) -> Self {
        let allocator = AggregateAllocator::new(tracker);
        Self {
            indexes: aggregate_hash_map(allocator.clone()),
            entries: AggregateVec::new_in(allocator.clone()),
            allocator,
            saw_non_null_map: false,
        }
    }
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
        if !matches!(check_type, DataType::Map(_, _) | DataType::Null) {
            return Err(format!("sum_map expects MAP input, got {:?}", check_type));
        }

        Ok(AggSpec {
            kind: AggKind::SumMap,
            output_type,
            intermediate_type,
            input_arg_type: func
                .types
                .as_ref()
                .and_then(|sig| sig.input_arg_type.clone()),
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
        if matches!(arr.data_type(), DataType::Null)
            && arr.as_any().downcast_ref::<NullArray>().is_some()
        {
            return Ok(AggInputView::Any(arr));
        }
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
            std::ptr::write(
                ptr as *mut SumMapState,
                SumMapState::new(process_mem_tracker()),
            );
        }
    }

    fn init_state_with_tracker(
        &self,
        _spec: &AggSpec,
        ptr: *mut u8,
        tracker: Option<Arc<MemTracker>>,
    ) -> Result<(), String> {
        let tracker = tracker
            .ok_or_else(|| "allocation-tracked sum_map requires a memory tracker".to_string())?;
        unsafe { ptr.cast::<SumMapState>().write(SumMapState::new(tracker)) };
        Ok(())
    }

    fn drop_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::drop_in_place(ptr as *mut SumMapState);
        }
    }

    fn retained_bytes(&self, _spec: &AggSpec, _ptr: *const u8) -> usize {
        0
    }

    fn retained_memory_policy(&self, _spec: &AggSpec) -> RetainedMemoryPolicy {
        RetainedMemoryPolicy::AllocationTracked
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
        if matches!(array.data_type(), DataType::Null)
            && array.as_any().downcast_ref::<NullArray>().is_some()
        {
            return Ok(());
        }
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
                .map(|(key, value)| {
                    Ok((
                        key.as_ref().map(tracked_scalar_to_output).transpose()?,
                        Some(tracked_scalar_to_output(value)?),
                    ))
                })
                .collect::<Result<Vec<_>, String>>()?;
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
            let Some(value) = tracked_scalar_from_array(&values, idx, &state.allocator)? else {
                continue;
            };
            let key = tracked_scalar_from_array(&keys, idx, &state.allocator)?;
            let fp = tracked_optional_key_fingerprint(&key, &state.allocator)?;
            if let Some(existing_idx) = state.indexes.get(&fp).copied() {
                sum_scalar_in_place(&mut state.entries[existing_idx].1, &value)?;
            } else {
                let insert_idx = state.entries.len();
                state.indexes.try_reserve(1).map_err(|_| {
                    state
                        .allocator
                        .allocation_error("reserve sum_map key index")
                })?;
                state
                    .entries
                    .try_reserve(1)
                    .map_err(|_| state.allocator.allocation_error("reserve sum_map entries"))?;
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

fn sum_scalar_in_place(
    target: &mut TrackedAggScalarValue,
    incoming: &TrackedAggScalarValue,
) -> Result<(), String> {
    match (target, incoming) {
        (TrackedAggScalarValue::Int64(left), TrackedAggScalarValue::Int64(right)) => {
            *left = left
                .checked_add(*right)
                .ok_or_else(|| "sum_map int64 overflow".to_string())?;
            Ok(())
        }
        (TrackedAggScalarValue::Float64(left), TrackedAggScalarValue::Float64(right)) => {
            *left += *right;
            Ok(())
        }
        (TrackedAggScalarValue::Decimal128(left), TrackedAggScalarValue::Decimal128(right)) => {
            *left = left
                .checked_add(*right)
                .ok_or_else(|| "sum_map decimal128 overflow".to_string())?;
            Ok(())
        }
        (TrackedAggScalarValue::Decimal256(left), TrackedAggScalarValue::Decimal256(right)) => {
            *left = left
                .checked_add(*right)
                .ok_or_else(|| "sum_map decimal256 overflow".to_string())?;
            Ok(())
        }
        _ => Err("sum_map value type mismatch".to_string()),
    }
}
