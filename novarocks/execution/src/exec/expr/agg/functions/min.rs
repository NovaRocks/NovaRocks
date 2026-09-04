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
use arrow::array::{Array, ArrayRef};
use arrow::datatypes::DataType;
use arrow_buffer::i256;
use std::sync::Arc;

use crate::exec::node::aggregate::AggFunction;
use crate::runtime::mem_tracker::MemTracker;
use novarocks_types::largeint;

use super::super::*;
use super::AggregateFunction;
use super::common;

pub(super) struct MinAgg;

fn min_spec_from_type(data_type: &DataType) -> Result<AggSpec, String> {
    let (kind, output_type, intermediate_type) = match data_type {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            (AggKind::MinInt, data_type.clone(), data_type.clone())
        }
        DataType::Float32 | DataType::Float64 => {
            (AggKind::MinFloat, data_type.clone(), data_type.clone())
        }
        DataType::Boolean => (AggKind::MinBool, DataType::Boolean, DataType::Boolean),
        DataType::Utf8 => (AggKind::MinUtf8, DataType::Utf8, DataType::Utf8),
        DataType::Date32 => (AggKind::MinDate32, DataType::Date32, DataType::Date32),
        DataType::Timestamp(unit, tz) => (
            AggKind::MinTimestamp,
            DataType::Timestamp(*unit, tz.clone()),
            DataType::Timestamp(*unit, tz.clone()),
        ),
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => (
            AggKind::MinLargeInt,
            DataType::FixedSizeBinary(*width),
            DataType::FixedSizeBinary(*width),
        ),
        DataType::Decimal128(precision, scale) => (
            AggKind::MinDecimal128,
            DataType::Decimal128(*precision, *scale),
            DataType::Decimal128(*precision, *scale),
        ),
        DataType::Decimal256(precision, scale) => (
            AggKind::MinDecimal256,
            DataType::Decimal256(*precision, *scale),
            DataType::Decimal256(*precision, *scale),
        ),
        other => return Err(format!("min unsupported input type: {:?}", other)),
    };
    Ok(AggSpec {
        kind,
        output_type,
        intermediate_type,
        input_arg_type: None,
        count_all: false,
    })
}

impl AggregateFunction for MinAgg {
    fn build_spec_from_type(
        &self,
        _func: &AggFunction,
        input_type: Option<&DataType>,
        _input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        let data_type = input_type.ok_or_else(|| "min input type missing".to_string())?;
        min_spec_from_type(data_type)
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        match kind {
            AggKind::MinInt | AggKind::MinTimestamp => (
                std::mem::size_of::<I64State>(),
                std::mem::align_of::<I64State>(),
            ),
            AggKind::MinFloat => (
                std::mem::size_of::<F64State>(),
                std::mem::align_of::<F64State>(),
            ),
            AggKind::MinBool => (
                std::mem::size_of::<BoolState>(),
                std::mem::align_of::<BoolState>(),
            ),
            AggKind::MinUtf8 => (
                std::mem::size_of::<common::TrackedUtf8State>(),
                std::mem::align_of::<common::TrackedUtf8State>(),
            ),
            AggKind::MinDate32 => (
                std::mem::size_of::<I32State>(),
                std::mem::align_of::<I32State>(),
            ),
            AggKind::MinLargeInt | AggKind::MinDecimal128 => (
                std::mem::size_of::<I128State>(),
                std::mem::align_of::<I128State>(),
            ),
            AggKind::MinDecimal256 => (
                std::mem::size_of::<I256State>(),
                std::mem::align_of::<I256State>(),
            ),
            other => unreachable!("unexpected kind for min: {:?}", other),
        }
    }

    fn build_input_view<'a>(
        &self,
        spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        match spec.kind {
            AggKind::MinInt => {
                let arr = array
                    .as_ref()
                    .ok_or_else(|| "int input missing".to_string())?;
                Ok(AggInputView::Int(IntArrayView::new(arr)?))
            }
            AggKind::MinFloat => {
                let arr = array
                    .as_ref()
                    .ok_or_else(|| "float input missing".to_string())?;
                Ok(AggInputView::Float(FloatArrayView::new(arr)?))
            }
            AggKind::MinBool => {
                let arr = array
                    .as_ref()
                    .ok_or_else(|| "bool input missing".to_string())?;
                let view = arr
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| "failed to downcast to BooleanArray".to_string())?;
                Ok(AggInputView::Bool(view))
            }
            AggKind::MinUtf8
            | AggKind::MinDate32
            | AggKind::MinTimestamp
            | AggKind::MinDecimal128 => {
                let arr = array
                    .as_ref()
                    .ok_or_else(|| "utf8 input missing".to_string())?;
                Ok(AggInputView::Utf8(Utf8ArrayView::new(arr)?))
            }
            AggKind::MinLargeInt => {
                let arr = array
                    .as_ref()
                    .ok_or_else(|| "largeint input missing".to_string())?;
                Ok(AggInputView::Any(arr))
            }
            AggKind::MinDecimal256 => {
                let arr = array
                    .as_ref()
                    .ok_or_else(|| "decimal256 input missing".to_string())?;
                Ok(AggInputView::Any(arr))
            }
            _ => Err("min input type mismatch".to_string()),
        }
    }

    fn build_merge_view<'a>(
        &self,
        spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        self.build_input_view(spec, array)
    }

    fn init_state(&self, spec: &AggSpec, ptr: *mut u8) {
        match spec.kind {
            AggKind::MinInt | AggKind::MinTimestamp => unsafe {
                std::ptr::write(ptr as *mut I64State, I64State::default());
            },
            AggKind::MinFloat => unsafe {
                std::ptr::write(ptr as *mut F64State, F64State::default());
            },
            AggKind::MinBool => unsafe {
                std::ptr::write(ptr as *mut BoolState, BoolState::default());
            },
            AggKind::MinUtf8 => {
                panic!("allocation-tracked min UTF-8 state requires tracker-aware init");
            }
            AggKind::MinDate32 => unsafe {
                std::ptr::write(ptr as *mut I32State, I32State::default());
            },
            AggKind::MinDecimal128 => unsafe {
                std::ptr::write(ptr as *mut I128State, I128State::default());
            },
            AggKind::MinDecimal256 => unsafe {
                std::ptr::write(ptr as *mut I256State, I256State::default());
            },
            _ => {}
        }
    }

    fn init_state_with_tracker(
        &self,
        spec: &AggSpec,
        ptr: *mut u8,
        tracker: Option<Arc<MemTracker>>,
    ) -> Result<(), String> {
        if matches!(spec.kind, AggKind::MinUtf8) {
            let tracker = tracker.ok_or_else(|| {
                "allocation-tracked min UTF-8 state requires a memory tracker".to_string()
            })?;
            unsafe {
                ptr.cast::<common::TrackedUtf8State>()
                    .write(common::TrackedUtf8State::new(AggregateAllocator::new(
                        tracker,
                    )))
            };
        } else {
            self.init_state(spec, ptr);
        }
        Ok(())
    }

    fn drop_state(&self, spec: &AggSpec, ptr: *mut u8) {
        if matches!(spec.kind, AggKind::MinUtf8) {
            unsafe {
                std::ptr::drop_in_place(ptr as *mut common::TrackedUtf8State);
            }
        }
    }

    fn retained_bytes(&self, _spec: &AggSpec, _ptr: *const u8) -> usize {
        0
    }

    fn retained_memory_policy(&self, spec: &AggSpec) -> RetainedMemoryPolicy {
        if matches!(spec.kind, AggKind::MinUtf8) {
            RetainedMemoryPolicy::AllocationTracked
        } else {
            RetainedMemoryPolicy::FixedZero
        }
    }

    fn update_batch(
        &self,
        spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        match spec.kind {
            AggKind::MinInt => update_min_int(offset, state_ptrs, input),
            AggKind::MinFloat => update_min_float(offset, state_ptrs, input),
            AggKind::MinBool => update_min_bool(offset, state_ptrs, input),
            AggKind::MinUtf8 => update_min_utf8(offset, state_ptrs, input),
            AggKind::MinDate32 => update_min_date32(offset, state_ptrs, input),
            AggKind::MinTimestamp => update_min_timestamp(offset, state_ptrs, input),
            AggKind::MinLargeInt => update_min_largeint(offset, state_ptrs, input),
            AggKind::MinDecimal128 => update_min_decimal128(offset, state_ptrs, input),
            AggKind::MinDecimal256 => update_min_decimal256(offset, state_ptrs, input),
            _ => Err("min update kind mismatch".to_string()),
        }
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
        match spec.kind {
            AggKind::MinInt => build_min_int_array(target_type, offset, group_states),
            AggKind::MinFloat => build_min_float_array(target_type, offset, group_states),
            AggKind::MinBool => common::build_bool_array(offset, group_states),
            AggKind::MinUtf8 => common::build_utf8_array(offset, group_states),
            AggKind::MinDate32 => common::build_date32_array(offset, group_states),
            AggKind::MinTimestamp => {
                common::build_timestamp_array(offset, group_states, target_type)
            }
            AggKind::MinLargeInt => common::build_largeint_array(offset, group_states),
            AggKind::MinDecimal128 => {
                common::build_decimal128_array(offset, group_states, target_type)
            }
            AggKind::MinDecimal256 => {
                common::build_decimal256_array(offset, group_states, target_type)
            }
            _ => Err("min output kind mismatch".to_string()),
        }
    }
}

fn build_min_int_array(
    output_type: &DataType,
    offset: usize,
    group_states: &[AggStatePtr],
) -> Result<ArrayRef, String> {
    let mut values = Vec::with_capacity(group_states.len());
    for &base in group_states {
        let state = unsafe { &*((base as *mut u8).add(offset) as *const I64State) };
        if state.has_value {
            values.push(Some(common::AggScalarValue::Int64(state.value)));
        } else {
            values.push(None);
        }
    }
    common::build_scalar_array(output_type, values)
}

fn build_min_float_array(
    output_type: &DataType,
    offset: usize,
    group_states: &[AggStatePtr],
) -> Result<ArrayRef, String> {
    let mut values = Vec::with_capacity(group_states.len());
    for &base in group_states {
        let state = unsafe { &*((base as *mut u8).add(offset) as *const F64State) };
        if state.has_value {
            values.push(Some(common::AggScalarValue::Float64(state.value)));
        } else {
            values.push(None);
        }
    }
    common::build_scalar_array(output_type, values)
}

fn update_min_int(
    offset: usize,
    state_ptrs: &[AggStatePtr],
    input: &AggInputView,
) -> Result<(), String> {
    match input {
        AggInputView::Int(view) => {
            for (row, &base) in state_ptrs.iter().enumerate() {
                if let Some(v) = view.value_at(row) {
                    let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut I64State) };
                    if !state.has_value || v < state.value {
                        state.has_value = true;
                        state.value = v;
                    }
                }
            }
            Ok(())
        }
        _ => Err("min int input type mismatch".to_string()),
    }
}

fn update_min_float(
    offset: usize,
    state_ptrs: &[AggStatePtr],
    input: &AggInputView,
) -> Result<(), String> {
    match input {
        AggInputView::Float(view) => {
            for (row, &base) in state_ptrs.iter().enumerate() {
                if let Some(v) = view.value_at(row) {
                    let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut F64State) };
                    if !state.has_value || v.total_cmp(&state.value) == std::cmp::Ordering::Less {
                        state.has_value = true;
                        state.value = v;
                    }
                }
            }
            Ok(())
        }
        _ => Err("min float input type mismatch".to_string()),
    }
}

fn update_min_bool(
    offset: usize,
    state_ptrs: &[AggStatePtr],
    input: &AggInputView,
) -> Result<(), String> {
    match input {
        AggInputView::Bool(view) => {
            for (row, &base) in state_ptrs.iter().enumerate() {
                if !view.is_null(row) {
                    let v = view.value(row);
                    let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut BoolState) };
                    if !state.has_value || (!v & state.value) {
                        state.has_value = true;
                        state.value = v;
                    }
                }
            }
            Ok(())
        }
        _ => Err("min bool input type mismatch".to_string()),
    }
}

fn update_min_utf8(
    offset: usize,
    state_ptrs: &[AggStatePtr],
    input: &AggInputView,
) -> Result<(), String> {
    match input {
        AggInputView::Utf8(Utf8ArrayView::Utf8(arr)) => {
            for (row, &base) in state_ptrs.iter().enumerate() {
                if arr.is_null(row) {
                    continue;
                }
                let v = arr.value(row);
                let state = unsafe {
                    &mut *((base as *mut u8).add(offset) as *mut common::TrackedUtf8State)
                };
                match &state.value {
                    None => state.replace(v)?,
                    Some(cur) => {
                        if v.as_bytes() < cur.as_slice() {
                            state.replace(v)?;
                        }
                    }
                }
            }
            Ok(())
        }
        AggInputView::Utf8(_) => Err("min utf8 input type mismatch".to_string()),
        _ => Err("min utf8 input type mismatch".to_string()),
    }
}

fn update_min_date32(
    offset: usize,
    state_ptrs: &[AggStatePtr],
    input: &AggInputView,
) -> Result<(), String> {
    match input {
        AggInputView::Utf8(Utf8ArrayView::Date32(arr)) => {
            for (row, &base) in state_ptrs.iter().enumerate() {
                if arr.is_null(row) {
                    continue;
                }
                let v = arr.value(row);
                let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut I32State) };
                if !state.has_value || v < state.value {
                    state.has_value = true;
                    state.value = v;
                }
            }
            Ok(())
        }
        AggInputView::Utf8(_) => Err("min date32 input type mismatch".to_string()),
        _ => Err("min date32 input type mismatch".to_string()),
    }
}

fn update_min_timestamp(
    offset: usize,
    state_ptrs: &[AggStatePtr],
    input: &AggInputView,
) -> Result<(), String> {
    match input {
        AggInputView::Utf8(view) => {
            for (row, &base) in state_ptrs.iter().enumerate() {
                let v = match view {
                    Utf8ArrayView::TimestampSecond(arr, _) => {
                        (!arr.is_null(row)).then(|| arr.value(row))
                    }
                    Utf8ArrayView::TimestampMillisecond(arr, _) => {
                        (!arr.is_null(row)).then(|| arr.value(row))
                    }
                    Utf8ArrayView::TimestampMicrosecond(arr, _) => {
                        (!arr.is_null(row)).then(|| arr.value(row))
                    }
                    Utf8ArrayView::TimestampNanosecond(arr, _) => {
                        (!arr.is_null(row)).then(|| arr.value(row))
                    }
                    _ => return Err("min timestamp input type mismatch".to_string()),
                };
                if let Some(v) = v {
                    let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut I64State) };
                    if !state.has_value || v < state.value {
                        state.has_value = true;
                        state.value = v;
                    }
                }
            }
            Ok(())
        }
        _ => Err("min timestamp input type mismatch".to_string()),
    }
}

fn update_min_decimal128(
    offset: usize,
    state_ptrs: &[AggStatePtr],
    input: &AggInputView,
) -> Result<(), String> {
    match input {
        AggInputView::Utf8(Utf8ArrayView::Decimal128(arr, _)) => {
            for (row, &base) in state_ptrs.iter().enumerate() {
                if arr.is_null(row) {
                    continue;
                }
                let v = arr.value(row);
                let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut I128State) };
                if !state.has_value || v < state.value {
                    state.has_value = true;
                    state.value = v;
                }
            }
            Ok(())
        }
        AggInputView::Utf8(_) => Err("min decimal input type mismatch".to_string()),
        _ => Err("min decimal input type mismatch".to_string()),
    }
}

fn update_min_largeint(
    offset: usize,
    state_ptrs: &[AggStatePtr],
    input: &AggInputView,
) -> Result<(), String> {
    let AggInputView::Any(array) = input else {
        return Err("min largeint input type mismatch".to_string());
    };
    let arr = largeint::as_fixed_size_binary_array(array, "min largeint input")?;
    for (row, &base) in state_ptrs.iter().enumerate() {
        if arr.is_null(row) {
            continue;
        }
        let v = largeint::value_at(arr, row)?;
        let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut I128State) };
        if !state.has_value || v < state.value {
            state.has_value = true;
            state.value = v;
        }
    }
    Ok(())
}

fn update_min_decimal256(
    offset: usize,
    state_ptrs: &[AggStatePtr],
    input: &AggInputView,
) -> Result<(), String> {
    let AggInputView::Any(array) = input else {
        return Err("min decimal256 input type mismatch".to_string());
    };
    let arr = array
        .as_any()
        .downcast_ref::<arrow::array::Decimal256Array>()
        .ok_or_else(|| "min decimal256 input type mismatch".to_string())?;
    for (row, &base) in state_ptrs.iter().enumerate() {
        if arr.is_null(row) {
            continue;
        }
        let v: i256 = arr.value(row);
        let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut I256State) };
        if !state.has_value || v < state.value {
            state.has_value = true;
            state.value = v;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::MaybeUninit;
    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef, Int32Array, StringArray};
    use arrow::datatypes::DataType;

    use crate::exec::expr::ExprId;
    use crate::exec::expr::agg::{
        AggStateArena, build_kernel_set, test_builtin_execution_function_set,
    };
    use crate::exec::node::aggregate::{AggFunction, AggTypeSignature};
    use crate::runtime::mem_tracker::MemTracker;
    use novarocks_types::largeint;

    #[test]
    fn test_min_int32_aligns_with_signature_type() {
        let func = AggFunction {
            name: "min".to_string(),
            inputs: vec![ExprId(0)],
            input_is_intermediate: false,
            types: Some(AggTypeSignature {
                intermediate_type: Some(DataType::Int32),
                output_type: Some(DataType::Int32),
                input_arg_type: Some(DataType::Int32),
            }),
            ..Default::default()
        };

        let function_set = test_builtin_execution_function_set();
        let selected = function_set
            .catalog()
            .resolve_aggregate_trusted("min", &[DataType::Int32])
            .expect("resolved min");
        let kernels = build_kernel_set(
            &function_set,
            &[func],
            &[Some(DataType::Int32)],
            &[selected],
        )
        .expect("build kernels");
        let kernel = &kernels.entries[0];

        let mut arena = AggStateArena::new(1024);
        let base = arena.alloc(kernels.layout.total_size, kernel.state_align());
        kernel.init_state(base).expect("init state");

        let input = Arc::new(Int32Array::from(vec![Some(7), Some(2), Some(9), None])) as ArrayRef;
        let state_ptrs = vec![base; input.len()];
        kernel
            .update_batch(
                &state_ptrs,
                novarocks_functions::AggregateInputBatch::try_new(Some(&input), input.len())
                    .expect("input batch"),
            )
            .expect("update");

        let out = kernel.build_array(&[base], false).expect("build output");
        let out = out
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 output");
        assert!(!out.is_null(0));
        assert_eq!(out.value(0), 2);
    }

    #[test]
    fn test_min_largeint_aligns_with_signature_type() {
        let func = AggFunction {
            name: "min".to_string(),
            inputs: vec![ExprId(0)],
            input_is_intermediate: false,
            types: Some(AggTypeSignature {
                intermediate_type: Some(DataType::FixedSizeBinary(16)),
                output_type: Some(DataType::FixedSizeBinary(16)),
                input_arg_type: Some(DataType::FixedSizeBinary(16)),
            }),
            ..Default::default()
        };

        let function_set = test_builtin_execution_function_set();
        let selected = function_set
            .catalog()
            .resolve_aggregate_trusted("min", &[DataType::FixedSizeBinary(16)])
            .expect("resolved min");
        let kernels = build_kernel_set(
            &function_set,
            &[func],
            &[Some(DataType::FixedSizeBinary(16))],
            &[selected],
        )
        .expect("build kernels");
        let kernel = &kernels.entries[0];

        let mut arena = AggStateArena::new(1024);
        let base = arena.alloc(kernels.layout.total_size, kernel.state_align());
        kernel.init_state(base).expect("init state");

        let input = largeint::array_from_i128(&[
            Some(-9_223_372_036_854_775_809_i128),
            Some(2_i128),
            Some(9_223_372_036_854_775_808_i128),
            None,
        ])
        .expect("build input");
        let state_ptrs = vec![base; input.len()];
        kernel
            .update_batch(
                &state_ptrs,
                novarocks_functions::AggregateInputBatch::try_new(Some(&input), input.len())
                    .expect("input batch"),
            )
            .expect("update");

        let out = kernel.build_array(&[base], false).expect("build output");
        let out = largeint::as_fixed_size_binary_array(&out, "min largeint output").unwrap();
        assert!(!out.is_null(0));
        let v = largeint::value_at(out, 0).expect("decode output");
        assert_eq!(v, -9_223_372_036_854_775_809_i128);
    }

    #[test]
    fn min_utf8_tracks_replacement_merge_and_drop_exactly() {
        let spec = min_spec_from_type(&DataType::Utf8).unwrap();
        let tracker = MemTracker::new_root("min-utf8-test");
        let mut state = MaybeUninit::<common::TrackedUtf8State>::uninit();
        MinAgg
            .init_state_with_tracker(&spec, state.as_mut_ptr().cast(), Some(Arc::clone(&tracker)))
            .unwrap();
        let state_ptr = state.as_mut_ptr() as AggStatePtr;

        let initial = Some(Arc::new(StringArray::from(vec!["zzzz"])) as ArrayRef);
        let initial_view = MinAgg.build_input_view(&spec, &initial).unwrap();
        MinAgg
            .update_batch(&spec, 0, &[state_ptr], &initial_view)
            .unwrap();
        assert_eq!(tracker.current(), 4);

        let replacement_text = "a-longer-minimum";
        let replacement = Some(Arc::new(StringArray::from(vec![replacement_text])) as ArrayRef);
        let replacement_view = MinAgg.build_merge_view(&spec, &replacement).unwrap();
        MinAgg
            .merge_batch(&spec, 0, &[state_ptr], &replacement_view)
            .unwrap();
        assert_eq!(tracker.current(), replacement_text.len() as i64);

        let output = MinAgg.build_array(&spec, 0, &[state_ptr], false).unwrap();
        let output = output.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(output.value(0), replacement_text);

        MinAgg.drop_state(&spec, state.as_mut_ptr().cast());
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn min_utf8_oom_preserves_prior_value_and_charge() {
        let spec = min_spec_from_type(&DataType::Utf8).unwrap();
        let tracker = MemTracker::new_root("min-utf8-oom-test");
        tracker.install_limit_once(8).unwrap();
        let mut state = MaybeUninit::<common::TrackedUtf8State>::uninit();
        MinAgg
            .init_state_with_tracker(&spec, state.as_mut_ptr().cast(), Some(Arc::clone(&tracker)))
            .unwrap();
        let state_ptr = state.as_mut_ptr() as AggStatePtr;

        let initial = Some(Arc::new(StringArray::from(vec!["zzzzz"])) as ArrayRef);
        let initial_view = MinAgg.build_input_view(&spec, &initial).unwrap();
        MinAgg
            .update_batch(&spec, 0, &[state_ptr], &initial_view)
            .unwrap();
        assert_eq!(tracker.current(), 5);

        // The replacement would retain only 6 bytes, but the exact allocator
        // must admit the 5 + 6 byte replacement peak before releasing the old value.
        let rejected = Some(Arc::new(StringArray::from(vec!["aaaaaa"])) as ArrayRef);
        let rejected_view = MinAgg.build_input_view(&spec, &rejected).unwrap();
        let error = MinAgg
            .update_batch(&spec, 0, &[state_ptr], &rejected_view)
            .unwrap_err();
        assert!(error.contains("ResourceExhausted"));
        assert_eq!(tracker.current(), 5);

        let output = MinAgg.build_array(&spec, 0, &[state_ptr], false).unwrap();
        let output = output.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(output.value(0), "zzzzz");

        MinAgg.drop_state(&spec, state.as_mut_ptr().cast());
        assert_eq!(tracker.current(), 0);
    }
}
