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
use arrow::array::{ArrayRef, Decimal256Array, FixedSizeBinaryArray};
use arrow::datatypes::DataType;
use arrow_buffer::i256;

use crate::common::largeint;
use crate::exec::node::aggregate::AggFunction;

use super::super::*;
use super::AggregateFunction;
use super::common;

pub(super) struct SumAgg;

fn sum_spec_from_type(data_type: &DataType) -> Result<AggSpec, String> {
    match data_type {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => Ok(AggSpec {
            kind: AggKind::SumInt,
            output_type: DataType::Int64,
            intermediate_type: DataType::Int64,
            input_arg_type: None,
            count_all: false,
        }),
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => Ok(AggSpec {
            kind: AggKind::SumLargeInt,
            output_type: DataType::FixedSizeBinary(*width),
            intermediate_type: DataType::FixedSizeBinary(*width),
            input_arg_type: None,
            count_all: false,
        }),
        DataType::Boolean => Ok(AggSpec {
            kind: AggKind::SumInt,
            output_type: DataType::Int64,
            intermediate_type: DataType::Int64,
            input_arg_type: None,
            count_all: false,
        }),
        DataType::Float32 | DataType::Float64 => Ok(AggSpec {
            kind: AggKind::SumFloat,
            output_type: DataType::Float64,
            intermediate_type: DataType::Float64,
            input_arg_type: None,
            count_all: false,
        }),
        DataType::Decimal128(precision, scale) => Ok(AggSpec {
            kind: AggKind::SumDecimal128,
            output_type: DataType::Decimal128(*precision, *scale),
            intermediate_type: DataType::Decimal128(*precision, *scale),
            input_arg_type: None,
            count_all: false,
        }),
        DataType::Decimal256(precision, scale) => Ok(AggSpec {
            kind: AggKind::SumDecimal256,
            output_type: DataType::Decimal256(*precision, *scale),
            intermediate_type: DataType::Decimal256(*precision, *scale),
            input_arg_type: None,
            count_all: false,
        }),
        other => Err(format!("sum unsupported input type: {:?}", other)),
    }
}

impl AggregateFunction for SumAgg {
    fn build_spec_from_type(
        &self,
        _func: &AggFunction,
        input_type: Option<&DataType>,
        _input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        let data_type = input_type.ok_or_else(|| "sum input type missing".to_string())?;
        sum_spec_from_type(data_type)
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        match kind {
            AggKind::SumInt => (
                std::mem::size_of::<SumIntState>(),
                std::mem::align_of::<SumIntState>(),
            ),
            AggKind::SumLargeInt => (
                std::mem::size_of::<I128State>(),
                std::mem::align_of::<I128State>(),
            ),
            AggKind::SumFloat => (
                std::mem::size_of::<SumFloatState>(),
                std::mem::align_of::<SumFloatState>(),
            ),
            AggKind::SumDecimal128 => (
                std::mem::size_of::<SumDecimal128State>(),
                std::mem::align_of::<SumDecimal128State>(),
            ),
            AggKind::SumDecimal256 => (
                std::mem::size_of::<SumDecimal256State>(),
                std::mem::align_of::<SumDecimal256State>(),
            ),
            other => unreachable!("unexpected kind for sum: {:?}", other),
        }
    }

    fn build_input_view<'a>(
        &self,
        spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        match spec.kind {
            AggKind::SumInt => {
                let arr = array
                    .as_ref()
                    .ok_or_else(|| "int input missing".to_string())?;
                if arr.data_type() == &DataType::Boolean {
                    let arr = arr
                        .as_any()
                        .downcast_ref::<arrow::array::BooleanArray>()
                        .ok_or_else(|| "failed to downcast to BooleanArray".to_string())?;
                    Ok(AggInputView::Bool(arr))
                } else {
                    Ok(AggInputView::Int(IntArrayView::new(arr)?))
                }
            }
            AggKind::SumLargeInt => {
                let arr = array
                    .as_ref()
                    .ok_or_else(|| "largeint input missing".to_string())?;
                Ok(AggInputView::Any(arr))
            }
            AggKind::SumFloat => {
                let arr = array
                    .as_ref()
                    .ok_or_else(|| "float input missing".to_string())?;
                Ok(AggInputView::Float(FloatArrayView::new(arr)?))
            }
            AggKind::SumDecimal128 => {
                let arr = array
                    .as_ref()
                    .ok_or_else(|| "utf8 input missing".to_string())?;
                Ok(AggInputView::Utf8(Utf8ArrayView::new(arr)?))
            }
            AggKind::SumDecimal256 => {
                let arr = array
                    .as_ref()
                    .ok_or_else(|| "decimal256 input missing".to_string())?;
                Ok(AggInputView::Any(arr))
            }
            _ => Err("sum input type mismatch".to_string()),
        }
    }

    fn build_merge_view<'a>(
        &self,
        spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        // sum merge uses the same update_* kernels. Its intermediate type matches its output type.
        self.build_input_view(spec, array)
    }

    fn init_state(&self, spec: &AggSpec, ptr: *mut u8) {
        match spec.kind {
            AggKind::SumInt => unsafe {
                std::ptr::write(
                    ptr as *mut SumIntState,
                    SumIntState {
                        sum: 0,
                        has_value: false,
                    },
                );
            },
            AggKind::SumFloat => unsafe {
                std::ptr::write(
                    ptr as *mut SumFloatState,
                    SumFloatState {
                        sum: 0.0,
                        has_value: false,
                    },
                );
            },
            AggKind::SumLargeInt => unsafe {
                std::ptr::write(ptr as *mut I128State, I128State::default());
            },
            AggKind::SumDecimal128 => unsafe {
                std::ptr::write(
                    ptr as *mut SumDecimal128State,
                    SumDecimal128State {
                        sum: i256::ZERO,
                        has_value: false,
                    },
                );
            },
            AggKind::SumDecimal256 => unsafe {
                std::ptr::write(
                    ptr as *mut SumDecimal256State,
                    SumDecimal256State {
                        sum: i256::ZERO,
                        has_value: false,
                    },
                );
            },
            _ => {}
        }
    }

    fn drop_state(&self, _spec: &AggSpec, _ptr: *mut u8) {}

    fn update_batch(
        &self,
        spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        match spec.kind {
            AggKind::SumInt => update_sum_int(offset, state_ptrs, input),
            AggKind::SumLargeInt => update_sum_largeint(offset, state_ptrs, input),
            AggKind::SumFloat => update_sum_float(offset, state_ptrs, input),
            AggKind::SumDecimal128 => update_sum_decimal128(offset, state_ptrs, input),
            AggKind::SumDecimal256 => update_sum_decimal256(offset, state_ptrs, input),
            _ => Err("sum update kind mismatch".to_string()),
        }
    }

    fn merge_batch(
        &self,
        spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        // sum merge == sum update
        self.update_batch(spec, offset, state_ptrs, input)
    }

    fn build_array(
        &self,
        spec: &AggSpec,
        offset: usize,
        group_states: &[AggStatePtr],
        _output_intermediate: bool,
    ) -> Result<ArrayRef, String> {
        match spec.kind {
            AggKind::SumInt => build_sum_int_array(offset, group_states),
            AggKind::SumLargeInt => common::build_largeint_array(offset, group_states),
            AggKind::SumFloat => build_sum_float_array(offset, group_states),
            AggKind::SumDecimal128 => {
                build_sum_decimal128_array(offset, group_states, &spec.output_type)
            }
            AggKind::SumDecimal256 => {
                build_sum_decimal256_array(offset, group_states, &spec.output_type)
            }
            _ => Err("sum output kind mismatch".to_string()),
        }
    }
}

fn update_sum_int(
    offset: usize,
    state_ptrs: &[AggStatePtr],
    input: &AggInputView,
) -> Result<(), String> {
    match input {
        AggInputView::Int(view) => {
            for (row, &base) in state_ptrs.iter().enumerate() {
                if let Some(v) = view.value_at(row) {
                    let state =
                        unsafe { &mut *((base as *mut u8).add(offset) as *mut SumIntState) };
                    state.sum += v;
                    state.has_value = true;
                }
            }
            Ok(())
        }
        AggInputView::Bool(arr) => {
            for (row, &base) in state_ptrs.iter().enumerate() {
                if arr.is_null(row) {
                    continue;
                }
                let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut SumIntState) };
                state.sum += i64::from(arr.value(row));
                state.has_value = true;
            }
            Ok(())
        }
        _ => Err("sum int input type mismatch".to_string()),
    }
}

fn update_sum_float(
    offset: usize,
    state_ptrs: &[AggStatePtr],
    input: &AggInputView,
) -> Result<(), String> {
    match input {
        AggInputView::Float(view) => {
            for (row, &base) in state_ptrs.iter().enumerate() {
                if let Some(v) = view.value_at(row) {
                    let state =
                        unsafe { &mut *((base as *mut u8).add(offset) as *mut SumFloatState) };
                    state.sum += v;
                    state.has_value = true;
                }
            }
            Ok(())
        }
        _ => Err("sum float input type mismatch".to_string()),
    }
}

fn update_sum_largeint(
    offset: usize,
    state_ptrs: &[AggStatePtr],
    input: &AggInputView,
) -> Result<(), String> {
    let AggInputView::Any(array) = input else {
        return Err("sum largeint input type mismatch".to_string());
    };
    let arr = array
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| "sum largeint input downcast failed".to_string())?;
    if arr.value_length() != largeint::LARGEINT_BYTE_WIDTH {
        return Err(format!(
            "sum largeint input width mismatch: expected {}, got {}",
            largeint::LARGEINT_BYTE_WIDTH,
            arr.value_length()
        ));
    }
    for (row, &base) in state_ptrs.iter().enumerate() {
        if arr.is_null(row) {
            continue;
        }
        let value = largeint::i128_from_be_bytes(arr.value(row))?;
        let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut I128State) };
        state.value = state
            .value
            .checked_add(value)
            .ok_or_else(|| "largeint overflow".to_string())?;
        state.has_value = true;
    }
    Ok(())
}

fn update_sum_decimal128(
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
                    let state =
                        unsafe { &mut *((base as *mut u8).add(offset) as *mut SumDecimal128State) };
                    state.sum = state
                        .sum
                        .checked_add(i256::from_i128(arr.value(row)))
                        .ok_or_else(|| "decimal overflow".to_string())?;
                    state.has_value = true;
                }
                Ok(())
            }
            _ => Err("sum decimal input type mismatch".to_string()),
        },
        _ => Err("sum decimal input type mismatch".to_string()),
    }
}

fn update_sum_decimal256(
    offset: usize,
    state_ptrs: &[AggStatePtr],
    input: &AggInputView,
) -> Result<(), String> {
    let AggInputView::Any(array) = input else {
        return Err("sum decimal256 input type mismatch".to_string());
    };
    let arr = array
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .ok_or_else(|| "sum decimal256 input downcast failed".to_string())?;
    for (row, &base) in state_ptrs.iter().enumerate() {
        if arr.is_null(row) {
            continue;
        }
        let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut SumDecimal256State) };
        state.sum = state
            .sum
            .checked_add(arr.value(row))
            .ok_or_else(|| "decimal overflow".to_string())?;
        state.has_value = true;
    }
    Ok(())
}

fn build_sum_int_array(offset: usize, group_states: &[AggStatePtr]) -> Result<ArrayRef, String> {
    let mut builder = Int64Builder::new();
    for &base in group_states {
        let state = unsafe { &*((base as *mut u8).add(offset) as *const SumIntState) };
        if state.has_value {
            builder.append_value(state.sum);
        } else {
            builder.append_null();
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_sum_float_array(offset: usize, group_states: &[AggStatePtr]) -> Result<ArrayRef, String> {
    let mut builder = Float64Builder::new();
    for &base in group_states {
        let state = unsafe { &*((base as *mut u8).add(offset) as *const SumFloatState) };
        if state.has_value {
            builder.append_value(state.sum);
        } else {
            builder.append_null();
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_sum_decimal128_array(
    offset: usize,
    group_states: &[AggStatePtr],
    output_type: &DataType,
) -> Result<ArrayRef, String> {
    let (precision, scale) = match output_type {
        DataType::Decimal128(precision, scale) => (*precision, *scale),
        other => return Err(format!("decimal output type mismatch: {:?}", other)),
    };
    let mut values = Vec::with_capacity(group_states.len());
    for &base in group_states {
        let state = unsafe { &*((base as *mut u8).add(offset) as *const SumDecimal128State) };
        if !state.has_value {
            values.push(None);
            continue;
        }
        let sum = state
            .sum
            .to_i128()
            .ok_or_else(|| "decimal overflow".to_string())?;
        values.push(Some(sum));
    }
    let array = Decimal128Array::from(values)
        .with_precision_and_scale(precision, scale)
        .map_err(|e| e.to_string())?;
    Ok(Arc::new(array))
}

fn build_sum_decimal256_array(
    offset: usize,
    group_states: &[AggStatePtr],
    output_type: &DataType,
) -> Result<ArrayRef, String> {
    let (precision, scale) = match output_type {
        DataType::Decimal256(precision, scale) => (*precision, *scale),
        other => return Err(format!("decimal256 output type mismatch: {:?}", other)),
    };
    let mut values = Vec::with_capacity(group_states.len());
    for &base in group_states {
        let state = unsafe { &*((base as *mut u8).add(offset) as *const SumDecimal256State) };
        values.push(state.has_value.then_some(state.sum));
    }
    let array = Decimal256Array::from(values)
        .with_precision_and_scale(precision, scale)
        .map_err(|e| e.to_string())?;
    Ok(Arc::new(array))
}
