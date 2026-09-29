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
    Array, ArrayRef, BinaryArray, BinaryBuilder, Int64Builder, LargeStringArray, StringArray,
    StructArray,
};
use arrow::datatypes::DataType;

use crate::exec::expr::function::object::percentile_functions::payload_bytes_at;
use crate::exec::hll::{HllHandle, HllTargetType};
use crate::exec::node::aggregate::AggFunction;
use crate::exec::sketch_hash::prehash_array_value;
use crate::runtime::mem_tracker::MemTracker;

use super::super::*;
use super::AggregateFunction;

const DEFAULT_LOG_K: u8 = 17;
const DEFAULT_TARGET_TYPE: HllTargetType = HllTargetType::Hll6;

pub(super) struct DsHllAgg;

struct DsHllState {
    handle: Option<HllHandle>,
    retained_charge: AggregateRetainedCharge,
    allocator: AggregateAllocator,
}

impl DsHllState {
    fn new(tracker: Arc<MemTracker>) -> Self {
        let allocator = AggregateAllocator::new(tracker);
        Self {
            handle: None,
            retained_charge: AggregateRetainedCharge::new(allocator.clone()),
            allocator,
        }
    }

    fn ensure_handle(
        &mut self,
        log_k: u8,
        target_type: HllTargetType,
    ) -> Result<&mut HllHandle, String> {
        if self.handle.is_none() {
            let preflight = HllHandle::new_allocation_preflight(log_k, target_type)?;
            let mut reservation = self.retained_charge.reserve_operation(
                preflight.bounds().operation_peak_bytes,
                "reserve ds_hll handle creation",
            )?;
            let (handle, outcome) = HllHandle::new_under_reservation(&preflight, &reservation)?;
            self.retained_charge.reconcile_under_reservation(
                hll_heap_bytes(outcome.current_bytes),
                &mut reservation,
            )?;
            self.handle = Some(handle);
        }
        Ok(self.handle.as_mut().expect("ds_hll handle initialized"))
    }

    fn ensure_handle_from_payload(&mut self, payload: &[u8]) -> Result<&mut HllHandle, String> {
        if self.handle.is_none() {
            let preflight = HllHandle::from_payload_allocation_preflight(payload)?;
            let mut reservation = self.retained_charge.reserve_operation(
                preflight.bounds().operation_peak_bytes,
                "reserve ds_hll payload initialization",
            )?;
            let (handle, outcome) =
                HllHandle::from_payload_under_reservation(payload, &preflight, &reservation)?;
            self.retained_charge.reconcile_under_reservation(
                hll_heap_bytes(outcome.current_bytes),
                &mut reservation,
            )?;
            self.handle = Some(handle);
        }
        Ok(self.handle.as_mut().expect("ds_hll handle initialized"))
    }

    fn update_hash(&mut self, hash: u64) -> Result<(), String> {
        let handle = self
            .handle
            .as_mut()
            .ok_or_else(|| "ds_hll handle is not initialized".to_string())?;
        let preflight = handle.update_hash_allocation_preflight();
        let mut reservation = self.retained_charge.reserve_operation(
            preflight.bounds().additional_headroom_bytes(),
            "reserve ds_hll update",
        )?;
        let outcome = handle.update_hash_under_reservation(hash, &preflight, &reservation)?;
        self.retained_charge
            .reconcile_under_reservation(hll_heap_bytes(outcome.current_bytes), &mut reservation)
    }

    fn merge_payload(&mut self, payload: &[u8]) -> Result<(), String> {
        if self.handle.is_none() {
            self.ensure_handle_from_payload(payload)?;
            return Ok(());
        }
        let handle = self.handle.as_mut().expect("ds_hll handle initialized");
        let preflight = handle.merge_payload_allocation_preflight(payload)?;
        let mut reservation = self.retained_charge.reserve_operation(
            preflight.bounds().additional_headroom_bytes(),
            "reserve ds_hll merge",
        )?;
        let outcome = handle.merge_payload_under_reservation(payload, &preflight, &reservation)?;
        self.retained_charge
            .reconcile_under_reservation(hll_heap_bytes(outcome.current_bytes), &mut reservation)
    }
}

fn hll_heap_bytes(current_allocation_bytes: usize) -> usize {
    current_allocation_bytes.saturating_sub(std::mem::size_of::<HllHandle>())
}

unsafe fn get_state<'a>(ptr: *const u8) -> &'a DsHllState {
    unsafe { &*(ptr as *const DsHllState) }
}

unsafe fn get_state_mut<'a>(ptr: *mut u8) -> &'a mut DsHllState {
    unsafe { &mut *(ptr as *mut DsHllState) }
}

fn parse_target_type(value: &str) -> HllTargetType {
    match value.to_ascii_uppercase().as_str() {
        "HLL_4" => HllTargetType::Hll4,
        "HLL_8" => HllTargetType::Hll8,
        _ => HllTargetType::Hll6,
    }
}

fn parse_log_k(array: &ArrayRef, row: usize, context: &str) -> Result<Option<u8>, String> {
    match array.data_type() {
        DataType::Int8 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Int8Array>()
                .ok_or_else(|| format!("{context}: failed to downcast Int8Array"))?;
            Ok((!arr.is_null(row)).then_some(arr.value(row) as u8))
        }
        DataType::Int16 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Int16Array>()
                .ok_or_else(|| format!("{context}: failed to downcast Int16Array"))?;
            Ok((!arr.is_null(row)).then_some(arr.value(row) as u8))
        }
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .ok_or_else(|| format!("{context}: failed to downcast Int32Array"))?;
            Ok((!arr.is_null(row)).then_some(arr.value(row) as u8))
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .ok_or_else(|| format!("{context}: failed to downcast Int64Array"))?;
            Ok((!arr.is_null(row)).then_some(arr.value(row) as u8))
        }
        other => Err(format!(
            "{context}: ds_hll log_k expects integer input, got {:?}",
            other
        )),
    }
}

fn parse_target_type_array(
    array: &ArrayRef,
    row: usize,
    context: &str,
) -> Result<Option<HllTargetType>, String> {
    match array.data_type() {
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .ok_or_else(|| format!("{context}: failed to downcast StringArray"))?;
            Ok((!arr.is_null(row)).then_some(parse_target_type(arr.value(row))))
        }
        DataType::LargeUtf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::LargeStringArray>()
                .ok_or_else(|| format!("{context}: failed to downcast LargeStringArray"))?;
            Ok((!arr.is_null(row)).then_some(parse_target_type(arr.value(row))))
        }
        DataType::Binary => {
            let arr = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| format!("{context}: failed to downcast BinaryArray"))?;
            Ok((!arr.is_null(row)).then_some(parse_target_type(
                std::str::from_utf8(arr.value(row)).unwrap_or_default(),
            )))
        }
        DataType::Null => Ok(None),
        other => Err(format!(
            "{context}: ds_hll target type expects string input, got {:?}",
            other
        )),
    }
}

fn update_from_struct(
    array: &StructArray,
    offset: usize,
    state_ptrs: &[AggStatePtr],
    context: &str,
) -> Result<(), String> {
    let fields = array.columns();
    if fields.is_empty() {
        return Err(format!("{context}: ds_hll input struct is empty"));
    }
    let values = fields[0].clone();

    for (row, &base) in state_ptrs.iter().enumerate() {
        let lg_k = if fields.len() >= 2 {
            parse_log_k(&fields[1], row, context)?.unwrap_or(DEFAULT_LOG_K)
        } else {
            DEFAULT_LOG_K
        };
        let target_type = if fields.len() >= 3 {
            parse_target_type_array(&fields[2], row, context)?.unwrap_or(DEFAULT_TARGET_TYPE)
        } else {
            DEFAULT_TARGET_TYPE
        };
        let Some(hash) = prehash_array_value(&values, row, context)? else {
            continue;
        };
        let ptr = unsafe { (base as *mut u8).add(offset) };
        let state = unsafe { get_state_mut(ptr) };
        state.ensure_handle(lg_k, target_type)?;
        state.update_hash(hash)?;
    }
    Ok(())
}

fn update_from_raw_array(
    array: &ArrayRef,
    offset: usize,
    state_ptrs: &[AggStatePtr],
    log_k: u8,
    target_type: HllTargetType,
    context: &str,
) -> Result<(), String> {
    for (row, &base) in state_ptrs.iter().enumerate() {
        let Some(hash) = prehash_array_value(array, row, context)? else {
            continue;
        };
        let ptr = unsafe { (base as *mut u8).add(offset) };
        let state = unsafe { get_state_mut(ptr) };
        state.ensure_handle(log_k, target_type)?;
        state.update_hash(hash)?;
    }
    Ok(())
}

fn merge_payload_array(
    array: &ArrayRef,
    offset: usize,
    state_ptrs: &[AggStatePtr],
    context: &str,
) -> Result<(), String> {
    for (row, &base) in state_ptrs.iter().enumerate() {
        let ptr = unsafe { (base as *mut u8).add(offset) };
        let state = unsafe { get_state_mut(ptr) };
        let Some(payload) = payload_bytes_for_merge(array, row, context, state.allocator.clone())?
        else {
            continue;
        };
        state.merge_payload(payload.as_ref())?;
    }
    Ok(())
}

enum MergePayload<'a> {
    Borrowed(&'a [u8]),
    Owned(AggregateVec<u8>),
}

impl AsRef<[u8]> for MergePayload<'_> {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Borrowed(value) => value,
            Self::Owned(value) => value.as_slice(),
        }
    }
}

fn payload_bytes_for_merge<'a>(
    array: &'a ArrayRef,
    row: usize,
    context: &str,
    allocator: AggregateAllocator,
) -> Result<Option<MergePayload<'a>>, String> {
    match array.data_type() {
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| format!("{context}: failed to downcast StringArray"))?;
            if arr.is_null(row) {
                return Ok(None);
            }
            let value = arr.value(row);
            let mut payload = AggregateVec::new_in(allocator.clone());
            payload
                .try_reserve_exact(value.len())
                .map_err(|_| allocator.allocation_error("reserve ds_hll string payload"))?;
            payload.extend(value.chars().map(|ch| ch as u8));
            Ok(Some(MergePayload::Owned(payload)))
        }
        DataType::LargeUtf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| format!("{context}: failed to downcast LargeStringArray"))?;
            if arr.is_null(row) {
                return Ok(None);
            }
            let value = arr.value(row);
            let mut payload = AggregateVec::new_in(allocator.clone());
            payload
                .try_reserve_exact(value.len())
                .map_err(|_| allocator.allocation_error("reserve ds_hll string payload"))?;
            payload.extend(value.chars().map(|ch| ch as u8));
            Ok(Some(MergePayload::Owned(payload)))
        }
        _ => {
            payload_bytes_at(array, row, context).map(|payload| payload.map(MergePayload::Borrowed))
        }
    }
}

impl AggregateFunction for DsHllAgg {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        input_type: Option<&DataType>,
        input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        let Some(_input_type) = input_type else {
            return Err("ds_hll expects input".to_string());
        };

        let fe_output_is_binary = func
            .types
            .as_ref()
            .and_then(|sig| sig.output_type.as_ref())
            .is_some_and(|ty| matches!(ty, DataType::Binary));

        let kind = match canonical_agg_name(func.name.as_str()) {
            "ds_hll_count_distinct_union" => AggKind::DsHllMerge,
            "ds_hll_count_distinct_merge" if !input_is_intermediate && fe_output_is_binary => {
                AggKind::DsHllMerge
            }
            "ds_hll_count_distinct" | "approx_count_distinct_hll_sketch"
                if !input_is_intermediate =>
            {
                AggKind::DsHllHash
            }
            "ds_hll_count_distinct_merge"
            | "ds_hll_count_distinct"
            | "approx_count_distinct_hll_sketch" => AggKind::DsHllCount,
            other => return Err(format!("unsupported ds_hll aggregate function: {}", other)),
        };

        Ok(AggSpec {
            kind: kind.clone(),
            output_type: match kind {
                AggKind::DsHllMerge => DataType::Binary,
                _ => DataType::Int64,
            },
            intermediate_type: DataType::Binary,
            input_arg_type: None,
            count_all: false,
        })
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        match kind {
            AggKind::DsHllHash | AggKind::DsHllMerge | AggKind::DsHllCount => (
                std::mem::size_of::<DsHllState>(),
                std::mem::align_of::<DsHllState>(),
            ),
            other => unreachable!("unexpected ds_hll agg kind: {:?}", other),
        }
    }

    fn build_input_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "ds_hll input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn build_merge_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "ds_hll merge input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn init_state(&self, _spec: &AggSpec, _ptr: *mut u8) {
        panic!("allocation-tracked ds_hll requires tracker-aware initialization");
    }

    fn init_state_with_tracker(
        &self,
        _spec: &AggSpec,
        ptr: *mut u8,
        tracker: Option<Arc<MemTracker>>,
    ) -> Result<(), String> {
        let tracker = tracker.ok_or_else(|| {
            "allocation-tracked ds_hll requires an aggregate memory tracker".to_string()
        })?;
        unsafe {
            std::ptr::write(ptr as *mut DsHllState, DsHllState::new(tracker));
        }
        Ok(())
    }

    fn drop_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::drop_in_place(ptr as *mut DsHllState);
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
        spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        let AggInputView::Any(array) = input else {
            return Err("ds_hll input type mismatch".to_string());
        };
        if let Some(struct_array) = array.as_any().downcast_ref::<StructArray>() {
            return update_from_struct(struct_array, offset, state_ptrs, "ds_hll_count_distinct");
        }
        match &spec.kind {
            AggKind::DsHllHash => update_from_raw_array(
                array,
                offset,
                state_ptrs,
                DEFAULT_LOG_K,
                DEFAULT_TARGET_TYPE,
                "ds_hll_count_distinct",
            ),
            AggKind::DsHllMerge => merge_payload_array(array, offset, state_ptrs, "ds_hll_merge"),
            AggKind::DsHllCount => {
                merge_payload_array(array, offset, state_ptrs, "ds_hll_count_distinct")
            }
            other => Err(format!("unexpected ds_hll aggregate kind: {:?}", other)),
        }
    }

    fn merge_batch(
        &self,
        _spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        let AggInputView::Any(array) = input else {
            return Err("ds_hll merge input type mismatch".to_string());
        };
        merge_payload_array(array, offset, state_ptrs, "ds_hll_merge")
    }

    fn build_array(
        &self,
        spec: &AggSpec,
        offset: usize,
        group_states: &[AggStatePtr],
        output_intermediate: bool,
    ) -> Result<ArrayRef, String> {
        let output_type = if output_intermediate {
            &spec.intermediate_type
        } else {
            &spec.output_type
        };

        match output_type {
            DataType::Binary => {
                let mut builder = BinaryBuilder::new();
                let empty_payload =
                    HllHandle::new_unreserved(DEFAULT_LOG_K, DEFAULT_TARGET_TYPE)?.serialize()?;
                for &base in group_states {
                    let ptr = unsafe { (base as *mut u8).add(offset) };
                    let state = unsafe { get_state(ptr) };
                    let payload = state
                        .handle
                        .as_ref()
                        .map(HllHandle::serialize)
                        .transpose()?
                        .unwrap_or_else(|| empty_payload.clone());
                    builder.append_value(payload);
                }
                Ok(Arc::new(builder.finish()) as ArrayRef)
            }
            DataType::Int64 => {
                let mut builder = Int64Builder::new();
                for &base in group_states {
                    let ptr = unsafe { (base as *mut u8).add(offset) };
                    let value = unsafe { get_state(ptr) }
                        .handle
                        .as_ref()
                        .map(HllHandle::estimate)
                        .transpose()?
                        .unwrap_or(0);
                    builder.append_value(value);
                }
                Ok(Arc::new(builder.finish()) as ArrayRef)
            }
            other => Err(format!(
                "ds_hll output type must be Binary or Int64, got {:?}",
                other
            )),
        }
    }
}

fn canonical_agg_name(name: &str) -> &str {
    name.split_once('|').map(|(base, _)| base).unwrap_or(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tracked_state_charges_retained_hll_heap_and_releases_on_drop() {
        let tracker = MemTracker::new_root("ds-hll-retained-test");
        {
            let mut state = DsHllState::new(Arc::clone(&tracker));
            state
                .ensure_handle(DEFAULT_LOG_K, DEFAULT_TARGET_TYPE)
                .expect("create handle");
            state.update_hash(11).expect("update handle");
            assert_eq!(tracker.current(), state.retained_charge.bytes() as i64);
            assert!(tracker.current() > 0);
        }
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn sparse_to_dense_growth_is_rejected_before_mutating_the_handle() {
        let tracker = MemTracker::new_root("ds-hll-admission-test");
        let mut state = DsHllState::new(Arc::clone(&tracker));
        state
            .ensure_handle(DEFAULT_LOG_K, DEFAULT_TARGET_TYPE)
            .expect("create handle");
        for hash in 0..7 {
            state.update_hash(hash).expect("sparse update");
        }
        let before_estimate = state
            .handle
            .as_ref()
            .expect("handle")
            .estimate()
            .expect("estimate");
        tracker
            .install_limit_once(tracker.current())
            .expect("install exact current limit");

        let error = state.update_hash(7).expect_err("dense allocation rejected");
        assert!(error.contains("ResourceExhausted"));
        assert_eq!(tracker.current(), state.retained_charge.bytes() as i64);
        assert_eq!(
            state
                .handle
                .as_ref()
                .expect("handle")
                .estimate()
                .expect("estimate"),
            before_estimate
        );
    }
    // Use the sealed catalog selection and the production aggregate adapters.
    // Exact estimates below come from the pinned Java 6.2.0 byte-domain oracle.
    fn prepared_binary_hll_parts(
        partitions: Vec<ArrayRef>,
        replay_dense: bool,
    ) -> (i64, Vec<Vec<u8>>, Vec<u8>) {
        use crate::exec::expr::agg::{
            AggStateArena, build_kernel_set, test_builtin_execution_function_set,
        };
        use crate::exec::node::aggregate::AggTypeSignature;
        use arrow::array::Int64Array;
        use arrow::datatypes::Field;
        use novarocks_functions::AggregateInputBatch;
        let functions = test_builtin_execution_function_set();
        let selected = functions
            .catalog()
            .resolve_aggregate_trusted(
                "ds_hll_count_distinct",
                &[DataType::Binary, DataType::Int64],
            )
            .unwrap();
        assert_eq!(
            selected.argument_types,
            vec![DataType::Binary, DataType::Int64]
        );
        assert_eq!(selected.intermediate_type, DataType::Binary);
        assert_eq!(selected.output_type, DataType::Int64);
        let packed_fields = vec![
            Arc::new(Field::new("value", DataType::Binary, true)),
            Arc::new(Field::new("lgk", DataType::Int64, false)),
        ];
        let packed_type = DataType::Struct(packed_fields.clone().into());
        let function = |merge| AggFunction {
            name: "ds_hll_count_distinct".to_string(),
            input_is_intermediate: merge,
            types: Some(AggTypeSignature {
                intermediate_type: Some(DataType::Binary),
                output_type: Some(DataType::Int64),
                input_arg_type: Some(DataType::Binary),
            }),
            ..Default::default()
        };
        let update = build_kernel_set(
            &functions,
            &[function(false)],
            &[Some(packed_type)],
            &[selected.clone()],
        )
        .unwrap();
        let merge = build_kernel_set(
            &functions,
            &[function(true)],
            &[Some(DataType::Binary)],
            &[selected],
        )
        .unwrap();
        let update = &update.entries[0];
        let merge = &merge.entries[0];
        let tracker = MemTracker::new_root("prepared-binary-hll-reference");
        let mut arena = AggStateArena::new(4096);
        arena.try_set_mem_tracker(tracker.clone()).unwrap();
        let mut partials = Vec::new();
        let mut frames = Vec::new();
        for values in partitions {
            let rows = values.len();
            let packed: ArrayRef = Arc::new(StructArray::new(
                packed_fields.clone().into(),
                vec![values, Arc::new(Int64Array::from(vec![10; rows]))],
                None,
            ));
            let pointer = arena.alloc(update.state.size, update.state_align());
            update
                .init_state_with_tracker(pointer, tracker.clone())
                .unwrap();
            update
                .update_batch(
                    &vec![pointer; rows],
                    AggregateInputBatch::try_new(Some(&packed), rows).unwrap(),
                )
                .unwrap();
            let partial = update.build_array(&[pointer], true).unwrap();
            frames.push(
                partial
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap()
                    .value(0)
                    .to_vec(),
            );
            partials.push(partial);
            update.drop_state(pointer);
        }
        let pointer = arena.alloc(merge.state.size, merge.state_align());
        merge
            .init_state_with_tracker(pointer, tracker.clone())
            .unwrap();
        for partial in partials
            .iter()
            .chain(if replay_dense { partials.first() } else { None })
        {
            merge
                .merge_batch(
                    &[pointer],
                    AggregateInputBatch::try_new(Some(partial), 1).unwrap(),
                )
                .unwrap();
        }
        let result = merge.build_array(&[pointer], false).unwrap();
        let payload = merge.build_array(&[pointer], true).unwrap();
        let result = result
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let payload = payload
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(0)
            .to_vec();
        merge.drop_state(pointer);
        drop(arena);
        assert_eq!(
            tracker.current(),
            0,
            "retained heap and state blocks release after the tree exits"
        );
        (result, frames, payload)
    }

    fn binary_values(indices: impl IntoIterator<Item = i32>) -> ArrayRef {
        let mut builder = BinaryBuilder::new();
        for index in indices {
            builder.append_value(format!("value_{index}").as_bytes());
        }
        Arc::new(builder.finish())
    }

    #[test]
    fn ds_hll_prepared_binary_dense_threshold_matches_independent_raw_reference() {
        use datasketches::hll::HllSketch;
        for (count, expected_mode, estimate, integer) in [
            (96, 1, 96.00002264977122, 96),
            (97, 2, 97.00002312660857, 97),
            (100, 2, 100.20239597952975, 100),
        ] {
            let (result, frames, _) =
                prepared_binary_hll_parts(vec![binary_values(1..=count)], false);
            assert_eq!(result, integer);
            assert_eq!(
                frames[0][7] & 3,
                expected_mode,
                "97 unique coupons enter dense mode at lgK10"
            );
            let decoded = HllSketch::deserialize(&frames[0]).unwrap();
            assert!(
                (decoded.estimate() - estimate).abs() < 1e-8,
                "count={count}: {}",
                decoded.estimate()
            );
        }
    }

    #[test]
    fn ds_hll_prepared_binary_partial_merge_and_dense_replay_preserve_the_reference_domain() {
        use datasketches::hll::HllSketch;
        let (result, _, _) = prepared_binary_hll_parts(
            (0..3)
                .map(|part| binary_values((1..=100).filter(move |index| index % 3 == part)))
                .collect(),
            false,
        );
        assert_eq!(
            result, 100,
            "three sparse partials preserve all 100 fixed byte inputs"
        );
        // Each overlapping leaf is dense, so merging loses HIP chronology.
        // The composite estimate is register-derived and replay-idempotent.
        let (result, frames, payload) =
            prepared_binary_hll_parts(vec![binary_values(1..=100), binary_values(1..=100)], true);
        assert_eq!(result, 103);
        assert!(frames.iter().all(|frame| frame[7] & 3 == 2));
        let decoded = HllSketch::deserialize(&payload).unwrap();
        assert!(
            (decoded.estimate() - 102.9590150260783).abs() < 1e-8,
            "{}",
            decoded.estimate()
        );
    }

    #[test]
    fn ds_hll_binary_hash_domain_preserves_zero_invalid_utf8_empty_and_null_bytes() {
        let mut builder = BinaryBuilder::new();
        for bytes in [
            b"".as_slice(),
            &[0],
            &[0xff],
            &[0, 1, 0xff, 0x80],
            b"value_1".as_slice(),
        ] {
            builder.append_value(bytes);
        }
        builder.append_null();
        let values: ArrayRef = Arc::new(builder.finish());
        for (row, expected) in [
            0xd8dfea6585bc9732,
            0xa55b92ce23afa288,
            0xe325594e010c6967,
            0xb37e42d422e8a61a,
            0x6c264bfa909526d5,
        ]
        .into_iter()
        .enumerate()
        {
            assert_eq!(
                prehash_array_value(&values, row, "reference").unwrap(),
                Some(expected)
            );
        }
        assert_eq!(prehash_array_value(&values, 5, "reference").unwrap(), None);
        assert_eq!(prepared_binary_hll_parts(vec![values], false).0, 5);
    }
}
