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

pub(super) struct MaxAgg;

fn max_spec_from_type(data_type: &DataType) -> Result<AggSpec, String> {
    let (kind, output_type, intermediate_type) = match data_type {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            (AggKind::MaxInt, data_type.clone(), data_type.clone())
        }
        DataType::Float32 | DataType::Float64 => {
            (AggKind::MaxFloat, data_type.clone(), data_type.clone())
        }
        DataType::Boolean => (AggKind::MaxBool, DataType::Boolean, DataType::Boolean),
        DataType::Utf8 => (AggKind::MaxUtf8, DataType::Utf8, DataType::Utf8),
        DataType::Date32 => (AggKind::MaxDate32, DataType::Date32, DataType::Date32),
        DataType::Timestamp(unit, tz) => (
            AggKind::MaxTimestamp,
            DataType::Timestamp(*unit, tz.clone()),
            DataType::Timestamp(*unit, tz.clone()),
        ),
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => (
            AggKind::MaxLargeInt,
            DataType::FixedSizeBinary(*width),
            DataType::FixedSizeBinary(*width),
        ),
        DataType::Decimal128(precision, scale) => (
            AggKind::MaxDecimal128,
            DataType::Decimal128(*precision, *scale),
            DataType::Decimal128(*precision, *scale),
        ),
        DataType::Decimal256(precision, scale) => (
            AggKind::MaxDecimal256,
            DataType::Decimal256(*precision, *scale),
            DataType::Decimal256(*precision, *scale),
        ),
        other => return Err(format!("max unsupported input type: {:?}", other)),
    };
    Ok(AggSpec {
        kind,
        output_type,
        intermediate_type,
        input_arg_type: None,
        count_all: false,
    })
}

impl AggregateFunction for MaxAgg {
    fn build_spec_from_type(
        &self,
        _func: &AggFunction,
        input_type: Option<&DataType>,
        _input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        let data_type = input_type.ok_or_else(|| "max input type missing".to_string())?;
        max_spec_from_type(data_type)
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        match kind {
            AggKind::MaxInt | AggKind::MaxTimestamp => (
                std::mem::size_of::<I64State>(),
                std::mem::align_of::<I64State>(),
            ),
            AggKind::MaxFloat => (
                std::mem::size_of::<F64State>(),
                std::mem::align_of::<F64State>(),
            ),
            AggKind::MaxBool => (
                std::mem::size_of::<BoolState>(),
                std::mem::align_of::<BoolState>(),
            ),
            AggKind::MaxUtf8 => (
                std::mem::size_of::<common::TrackedUtf8State>(),
                std::mem::align_of::<common::TrackedUtf8State>(),
            ),
            AggKind::MaxDate32 => (
                std::mem::size_of::<I32State>(),
                std::mem::align_of::<I32State>(),
            ),
            AggKind::MaxLargeInt | AggKind::MaxDecimal128 => (
                std::mem::size_of::<I128State>(),
                std::mem::align_of::<I128State>(),
            ),
            AggKind::MaxDecimal256 => (
                std::mem::size_of::<I256State>(),
                std::mem::align_of::<I256State>(),
            ),
            other => unreachable!("unexpected kind for max: {:?}", other),
        }
    }

    fn build_input_view<'a>(
        &self,
        spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        match spec.kind {
            AggKind::MaxInt => {
                let arr = array
                    .as_ref()
                    .ok_or_else(|| "int input missing".to_string())?;
                Ok(AggInputView::Int(IntArrayView::new(arr)?))
            }
            AggKind::MaxFloat => {
                let arr = array
                    .as_ref()
                    .ok_or_else(|| "float input missing".to_string())?;
                Ok(AggInputView::Float(FloatArrayView::new(arr)?))
            }
            AggKind::MaxBool => {
                let arr = array
                    .as_ref()
                    .ok_or_else(|| "bool input missing".to_string())?;
                let view = arr
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| "failed to downcast to BooleanArray".to_string())?;
                Ok(AggInputView::Bool(view))
            }
            AggKind::MaxUtf8
            | AggKind::MaxDate32
            | AggKind::MaxTimestamp
            | AggKind::MaxDecimal128 => {
                let arr = array
                    .as_ref()
                    .ok_or_else(|| "utf8 input missing".to_string())?;
                Ok(AggInputView::Utf8(Utf8ArrayView::new(arr)?))
            }
            AggKind::MaxLargeInt => {
                let arr = array
                    .as_ref()
                    .ok_or_else(|| "largeint input missing".to_string())?;
                Ok(AggInputView::Any(arr))
            }
            AggKind::MaxDecimal256 => {
                let arr = array
                    .as_ref()
                    .ok_or_else(|| "decimal256 input missing".to_string())?;
                Ok(AggInputView::Any(arr))
            }
            _ => Err("max input type mismatch".to_string()),
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
            AggKind::MaxInt | AggKind::MaxTimestamp => unsafe {
                std::ptr::write(ptr as *mut I64State, I64State::default());
            },
            AggKind::MaxFloat => unsafe {
                std::ptr::write(ptr as *mut F64State, F64State::default());
            },
            AggKind::MaxBool => unsafe {
                std::ptr::write(ptr as *mut BoolState, BoolState::default());
            },
            AggKind::MaxUtf8 => {
                panic!("allocation-tracked max UTF-8 state requires tracker-aware init");
            }
            AggKind::MaxDate32 => unsafe {
                std::ptr::write(ptr as *mut I32State, I32State::default());
            },
            AggKind::MaxLargeInt | AggKind::MaxDecimal128 => unsafe {
                std::ptr::write(ptr as *mut I128State, I128State::default());
            },
            AggKind::MaxDecimal256 => unsafe {
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
        if matches!(spec.kind, AggKind::MaxUtf8) {
            let tracker = tracker.ok_or_else(|| {
                "allocation-tracked max UTF-8 state requires a memory tracker".to_string()
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
        if matches!(spec.kind, AggKind::MaxUtf8) {
            unsafe {
                std::ptr::drop_in_place(ptr as *mut common::TrackedUtf8State);
            }
        }
    }

    fn retained_bytes(&self, _spec: &AggSpec, _ptr: *const u8) -> usize {
        0
    }

    fn retained_memory_policy(&self, spec: &AggSpec) -> RetainedMemoryPolicy {
        if matches!(spec.kind, AggKind::MaxUtf8) {
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
            AggKind::MaxInt => update_max_int(offset, state_ptrs, input),
            AggKind::MaxFloat => update_max_float(offset, state_ptrs, input),
            AggKind::MaxBool => update_max_bool(offset, state_ptrs, input),
            AggKind::MaxUtf8 => update_max_utf8(offset, state_ptrs, input),
            AggKind::MaxDate32 => update_max_date32(offset, state_ptrs, input),
            AggKind::MaxTimestamp => update_max_timestamp(offset, state_ptrs, input),
            AggKind::MaxLargeInt => update_max_largeint(offset, state_ptrs, input),
            AggKind::MaxDecimal128 => update_max_decimal128(offset, state_ptrs, input),
            AggKind::MaxDecimal256 => update_max_decimal256(offset, state_ptrs, input),
            _ => Err("max update kind mismatch".to_string()),
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
            AggKind::MaxInt => build_max_int_array(target_type, offset, group_states),
            AggKind::MaxFloat => build_max_float_array(target_type, offset, group_states),
            AggKind::MaxBool => common::build_bool_array(offset, group_states),
            AggKind::MaxUtf8 => common::build_utf8_array(offset, group_states),
            AggKind::MaxDate32 => common::build_date32_array(offset, group_states),
            AggKind::MaxTimestamp => {
                common::build_timestamp_array(offset, group_states, target_type)
            }
            AggKind::MaxLargeInt => common::build_largeint_array(offset, group_states),
            AggKind::MaxDecimal128 => {
                common::build_decimal128_array(offset, group_states, target_type)
            }
            AggKind::MaxDecimal256 => {
                common::build_decimal256_array(offset, group_states, target_type)
            }
            _ => Err("max output kind mismatch".to_string()),
        }
    }
}

fn build_max_int_array(
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

fn build_max_float_array(
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

fn update_max_int(
    offset: usize,
    state_ptrs: &[AggStatePtr],
    input: &AggInputView,
) -> Result<(), String> {
    match input {
        AggInputView::Int(view) => {
            for (row, &base) in state_ptrs.iter().enumerate() {
                if let Some(v) = view.value_at(row) {
                    let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut I64State) };
                    if !state.has_value || v > state.value {
                        state.has_value = true;
                        state.value = v;
                    }
                }
            }
            Ok(())
        }
        _ => Err("max int input type mismatch".to_string()),
    }
}

fn update_max_float(
    offset: usize,
    state_ptrs: &[AggStatePtr],
    input: &AggInputView,
) -> Result<(), String> {
    match input {
        AggInputView::Float(view) => {
            for (row, &base) in state_ptrs.iter().enumerate() {
                if let Some(v) = view.value_at(row) {
                    let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut F64State) };
                    if !state.has_value || v.total_cmp(&state.value) == std::cmp::Ordering::Greater
                    {
                        state.has_value = true;
                        state.value = v;
                    }
                }
            }
            Ok(())
        }
        _ => Err("max float input type mismatch".to_string()),
    }
}

fn update_max_bool(
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
                    if !state.has_value || (v & !state.value) {
                        state.has_value = true;
                        state.value = v;
                    }
                }
            }
            Ok(())
        }
        _ => Err("max bool input type mismatch".to_string()),
    }
}

fn update_max_utf8(
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
                        if v.as_bytes() > cur.as_slice() {
                            state.replace(v)?;
                        }
                    }
                }
            }
            Ok(())
        }
        AggInputView::Utf8(_) => Err("max utf8 input type mismatch".to_string()),
        _ => Err("max utf8 input type mismatch".to_string()),
    }
}

fn update_max_date32(
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
                if !state.has_value || v > state.value {
                    state.has_value = true;
                    state.value = v;
                }
            }
            Ok(())
        }
        AggInputView::Utf8(_) => Err("max date32 input type mismatch".to_string()),
        _ => Err("max date32 input type mismatch".to_string()),
    }
}

fn update_max_timestamp(
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
                    _ => return Err("max timestamp input type mismatch".to_string()),
                };
                if let Some(v) = v {
                    let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut I64State) };
                    if !state.has_value || v > state.value {
                        state.has_value = true;
                        state.value = v;
                    }
                }
            }
            Ok(())
        }
        _ => Err("max timestamp input type mismatch".to_string()),
    }
}

fn update_max_decimal128(
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
                if !state.has_value || v > state.value {
                    state.has_value = true;
                    state.value = v;
                }
            }
            Ok(())
        }
        AggInputView::Utf8(_) => Err("max decimal input type mismatch".to_string()),
        _ => Err("max decimal input type mismatch".to_string()),
    }
}

fn update_max_largeint(
    offset: usize,
    state_ptrs: &[AggStatePtr],
    input: &AggInputView,
) -> Result<(), String> {
    let AggInputView::Any(array) = input else {
        return Err("max largeint input type mismatch".to_string());
    };
    let arr = largeint::as_fixed_size_binary_array(array, "max largeint input")?;
    for (row, &base) in state_ptrs.iter().enumerate() {
        if arr.is_null(row) {
            continue;
        }
        let v = largeint::value_at(arr, row)?;
        let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut I128State) };
        if !state.has_value || v > state.value {
            state.has_value = true;
            state.value = v;
        }
    }
    Ok(())
}

fn update_max_decimal256(
    offset: usize,
    state_ptrs: &[AggStatePtr],
    input: &AggInputView,
) -> Result<(), String> {
    let AggInputView::Any(array) = input else {
        return Err("max decimal256 input type mismatch".to_string());
    };
    let arr = array
        .as_any()
        .downcast_ref::<arrow::array::Decimal256Array>()
        .ok_or_else(|| "max decimal256 input type mismatch".to_string())?;
    for (row, &base) in state_ptrs.iter().enumerate() {
        if arr.is_null(row) {
            continue;
        }
        let v: i256 = arr.value(row);
        let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut I256State) };
        if !state.has_value || v > state.value {
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
    fn test_max_int32_aligns_with_signature_type() {
        let func = AggFunction {
            name: "max".to_string(),
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
            .resolve_aggregate_trusted("max", &[DataType::Int32])
            .expect("resolved max");
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
        assert_eq!(out.value(0), 9);
    }

    #[test]
    fn test_max_largeint_aligns_with_signature_type() {
        let func = AggFunction {
            name: "max".to_string(),
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
            .resolve_aggregate_trusted("max", &[DataType::FixedSizeBinary(16)])
            .expect("resolved max");
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
        let out = largeint::as_fixed_size_binary_array(&out, "max largeint output").unwrap();
        assert!(!out.is_null(0));
        let v = largeint::value_at(out, 0).expect("decode output");
        assert_eq!(v, 9_223_372_036_854_775_808_i128);
    }

    #[test]
    fn max_utf8_tracks_replacement_merge_and_drop_exactly() {
        let spec = max_spec_from_type(&DataType::Utf8).unwrap();
        let tracker = MemTracker::new_root("max-utf8-test");
        let mut state = MaybeUninit::<common::TrackedUtf8State>::uninit();
        MaxAgg
            .init_state_with_tracker(&spec, state.as_mut_ptr().cast(), Some(Arc::clone(&tracker)))
            .unwrap();
        let state_ptr = state.as_mut_ptr() as AggStatePtr;

        let initial = Some(Arc::new(StringArray::from(vec!["a"])) as ArrayRef);
        let initial_view = MaxAgg.build_input_view(&spec, &initial).unwrap();
        MaxAgg
            .update_batch(&spec, 0, &[state_ptr], &initial_view)
            .unwrap();
        assert_eq!(tracker.current(), 1);

        let replacement_text = "zzzz-longer-maximum";
        let replacement = Some(Arc::new(StringArray::from(vec![replacement_text])) as ArrayRef);
        let replacement_view = MaxAgg.build_merge_view(&spec, &replacement).unwrap();
        MaxAgg
            .merge_batch(&spec, 0, &[state_ptr], &replacement_view)
            .unwrap();
        assert_eq!(tracker.current(), replacement_text.len() as i64);

        let output = MaxAgg.build_array(&spec, 0, &[state_ptr], false).unwrap();
        let output = output.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(output.value(0), replacement_text);

        MaxAgg.drop_state(&spec, state.as_mut_ptr().cast());
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn max_utf8_oom_preserves_prior_value_and_charge() {
        let spec = max_spec_from_type(&DataType::Utf8).unwrap();
        let tracker = MemTracker::new_root("max-utf8-oom-test");
        tracker.install_limit_once(8).unwrap();
        let mut state = MaybeUninit::<common::TrackedUtf8State>::uninit();
        MaxAgg
            .init_state_with_tracker(&spec, state.as_mut_ptr().cast(), Some(Arc::clone(&tracker)))
            .unwrap();
        let state_ptr = state.as_mut_ptr() as AggStatePtr;

        let initial = Some(Arc::new(StringArray::from(vec!["aaaaa"])) as ArrayRef);
        let initial_view = MaxAgg.build_input_view(&spec, &initial).unwrap();
        MaxAgg
            .update_batch(&spec, 0, &[state_ptr], &initial_view)
            .unwrap();
        assert_eq!(tracker.current(), 5);

        // The replacement would retain only 6 bytes, but the exact allocator
        // must admit the 5 + 6 byte replacement peak before releasing the old value.
        let rejected = Some(Arc::new(StringArray::from(vec!["zzzzzz"])) as ArrayRef);
        let rejected_view = MaxAgg.build_input_view(&spec, &rejected).unwrap();
        let error = MaxAgg
            .update_batch(&spec, 0, &[state_ptr], &rejected_view)
            .unwrap_err();
        assert!(error.contains("ResourceExhausted"));
        assert_eq!(tracker.current(), 5);

        let output = MaxAgg.build_array(&spec, 0, &[state_ptr], false).unwrap();
        let output = output.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(output.value(0), "aaaaa");

        MaxAgg.drop_state(&spec, state.as_mut_ptr().cast());
        assert_eq!(tracker.current(), 0);
    }
}
