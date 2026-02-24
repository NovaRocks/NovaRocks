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

use crate::common::largeint;
use crate::exec::node::aggregate::AggFunction;

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
            DataType::Timestamp(unit.clone(), tz.clone()),
            DataType::Timestamp(unit.clone(), tz.clone()),
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
                std::mem::size_of::<Utf8State>(),
                std::mem::align_of::<Utf8State>(),
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
            AggKind::MinUtf8 => unsafe {
                std::ptr::write(ptr as *mut Utf8State, Utf8State::default());
            },
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

    fn drop_state(&self, spec: &AggSpec, ptr: *mut u8) {
        if matches!(spec.kind, AggKind::MinUtf8) {
            unsafe {
                std::ptr::drop_in_place(ptr as *mut Utf8State);
            }
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
                    if !state.has_value || v < state.value {
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
        AggInputView::Utf8(view) => match view {
            Utf8ArrayView::Utf8(arr) => {
                for (row, &base) in state_ptrs.iter().enumerate() {
                    if arr.is_null(row) {
                        continue;
                    }
                    let v = arr.value(row);
                    let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut Utf8State) };
                    match &state.value {
                        None => state.value = Some(v.to_string()),
                        Some(cur) => {
                            if v < cur.as_str() {
                                state.value = Some(v.to_string());
                            }
                        }
                    }
                }
                Ok(())
            }
            _ => Err("min utf8 input type mismatch".to_string()),
        },
        _ => Err("min utf8 input type mismatch".to_string()),
    }
}

fn update_min_date32(
    offset: usize,
    state_ptrs: &[AggStatePtr],
    input: &AggInputView,
) -> Result<(), String> {
    match input {
        AggInputView::Utf8(view) => match view {
            Utf8ArrayView::Date32(arr) => {
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
            _ => Err("min date32 input type mismatch".to_string()),
        },
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
        AggInputView::Utf8(view) => match view {
            Utf8ArrayView::Decimal128(arr, _) => {
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
            _ => Err("min decimal input type mismatch".to_string()),
        },
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
    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef, Int32Array};
    use arrow::datatypes::DataType;

    use crate::common::largeint;
    use crate::exec::expr::ExprId;
    use crate::exec::expr::agg::{AggStateArena, build_kernel_set};
    use crate::exec::node::aggregate::{AggFunction, AggTypeSignature};

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
        };

        let kernels = build_kernel_set(&[func], &[Some(DataType::Int32)]).expect("build kernels");
        let kernel = &kernels.entries[0];

        let mut arena = AggStateArena::new(1024);
        let base = arena.alloc(kernels.layout.total_size, kernel.state_align());
        kernel.init_state(base);

        let input = Arc::new(Int32Array::from(vec![Some(7), Some(2), Some(9), None])) as ArrayRef;
        let input_opt = Some(input.clone());
        let view = kernel.build_input_view(&input_opt).expect("build view");
        let state_ptrs = vec![base; input.len()];
        kernel.update_batch(&state_ptrs, &view).expect("update");

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
        };

        let kernels = build_kernel_set(&[func], &[Some(DataType::FixedSizeBinary(16))])
            .expect("build kernels");
        let kernel = &kernels.entries[0];

        let mut arena = AggStateArena::new(1024);
        let base = arena.alloc(kernels.layout.total_size, kernel.state_align());
        kernel.init_state(base);

        let input = largeint::array_from_i128(&[
            Some(-9_223_372_036_854_775_809_i128),
            Some(2_i128),
            Some(9_223_372_036_854_775_808_i128),
            None,
        ])
        .expect("build input");
        let input_opt = Some(input.clone());
        let view = kernel.build_input_view(&input_opt).expect("build view");
        let state_ptrs = vec![base; input.len()];
        kernel.update_batch(&state_ptrs, &view).expect("update");

        let out = kernel.build_array(&[base], false).expect("build output");
        let out = largeint::as_fixed_size_binary_array(&out, "min largeint output").unwrap();
        assert!(!out.is_null(0));
        let v = largeint::value_at(out, 0).expect("decode output");
        assert_eq!(v, -9_223_372_036_854_775_809_i128);
    }
}
