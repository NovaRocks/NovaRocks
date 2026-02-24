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
use arrow::array::{ArrayRef, BinaryArray, Decimal128Array, Decimal256Array, StructArray};
use arrow::datatypes::DataType;
use arrow_buffer::i256;

use crate::exec::node::aggregate::AggFunction;

use super::super::*;
use super::AggregateFunction;
use crate::exec::expr::decimal::{div_round_i128, div_round_i256, pow10_i128, pow10_i256};

pub(super) struct AvgAgg;

fn avg_intermediate_type() -> DataType {
    // StarRocks commonly uses VARBINARY/VARCHAR; we represent it as Utf8 here.
    DataType::Utf8
}

fn avg_decimal_intermediate_type(_precision: u8, _scale: i8) -> DataType {
    // StarRocks commonly uses VARBINARY/VARCHAR; we represent it as Utf8 here.
    DataType::Utf8
}

fn is_count_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    )
}

fn avg_spec_from_input_type(data_type: &DataType) -> Result<AggSpec, String> {
    match data_type {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => Ok(AggSpec {
            kind: AggKind::AvgInt,
            output_type: DataType::Float64,
            intermediate_type: avg_intermediate_type(),
            input_arg_type: None,
            count_all: false,
        }),
        DataType::Float32 | DataType::Float64 => Ok(AggSpec {
            kind: AggKind::AvgFloat,
            output_type: DataType::Float64,
            intermediate_type: avg_intermediate_type(),
            input_arg_type: None,
            count_all: false,
        }),
        DataType::Decimal128(precision, scale) => Ok(AggSpec {
            kind: AggKind::AvgDecimal128,
            output_type: DataType::Decimal128(*precision, *scale),
            intermediate_type: avg_decimal_intermediate_type(*precision, *scale),
            input_arg_type: None,
            count_all: false,
        }),
        DataType::Decimal256(precision, scale) => Ok(AggSpec {
            kind: AggKind::AvgDecimal256,
            output_type: DataType::Decimal256(*precision, *scale),
            intermediate_type: avg_decimal_intermediate_type(*precision, *scale),
            input_arg_type: None,
            count_all: false,
        }),
        other => Err(format!("avg unsupported input type: {:?}", other)),
    }
}

fn avg_spec_from_intermediate_type(
    func: &AggFunction,
    data_type: &DataType,
) -> Result<AggSpec, String> {
    // StarRocks avg intermediate is often a VARBINARY/VARCHAR blob.
    // When the intermediate is opaque (Binary/Utf8), we must rely on FE type signature.
    match data_type {
        DataType::Binary | DataType::Utf8 => {
            let sig = agg_type_signature(func)
                .ok_or_else(|| "avg intermediate type signature missing".to_string())?;
            let output_type = sig
                .output_type
                .as_ref()
                .ok_or_else(|| "avg intermediate output_type signature missing".to_string())?;

            match output_type {
                DataType::Decimal128(p, s) => {
                    let input_arg_type = sig
                        .input_arg_type
                        .clone()
                        .ok_or_else(|| "avg intermediate input_arg_type missing".to_string())?;
                    Ok(AggSpec {
                        kind: AggKind::AvgDecimal128,
                        output_type: DataType::Decimal128(*p, *s),
                        intermediate_type: data_type.clone(),
                        input_arg_type: Some(input_arg_type),
                        count_all: false,
                    })
                }
                DataType::Decimal256(p, s) => {
                    let input_arg_type = sig
                        .input_arg_type
                        .clone()
                        .ok_or_else(|| "avg intermediate input_arg_type missing".to_string())?;
                    Ok(AggSpec {
                        kind: AggKind::AvgDecimal256,
                        output_type: DataType::Decimal256(*p, *s),
                        intermediate_type: data_type.clone(),
                        input_arg_type: Some(input_arg_type),
                        count_all: false,
                    })
                }
                DataType::Float64 => {
                    let arg0 = sig.input_arg_type.as_ref().ok_or_else(|| {
                        "avg intermediate input_arg_type signature is required".to_string()
                    })?;
                    let kind = match arg0 {
                        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                            AggKind::AvgInt
                        }
                        DataType::Float32 | DataType::Float64 => AggKind::AvgFloat,
                        other => {
                            return Err(format!(
                                "avg intermediate input_arg_type unsupported: {:?}",
                                other
                            ));
                        }
                    };
                    Ok(AggSpec {
                        kind,
                        output_type: DataType::Float64,
                        intermediate_type: data_type.clone(),
                        input_arg_type: Some(arg0.clone()),
                        count_all: false,
                    })
                }
                other => Err(format!(
                    "avg intermediate output type unsupported: {:?}",
                    other
                )),
            }
        }
        DataType::Struct(fields) => {
            if fields.len() != 2 {
                return Err("avg intermediate expects 2 fields".to_string());
            }
            let sum_type = fields[0].data_type();
            let count_type = fields[1].data_type();
            if !is_count_type(count_type) {
                return Err(format!(
                    "avg intermediate count type mismatch: {:?}",
                    count_type
                ));
            }
            match sum_type {
                DataType::Float64 | DataType::Float32 => Ok(AggSpec {
                    kind: AggKind::AvgFloat,
                    output_type: DataType::Float64,
                    intermediate_type: data_type.clone(),
                    input_arg_type: None,
                    count_all: false,
                }),
                DataType::Decimal128(precision, scale) => Ok(AggSpec {
                    kind: AggKind::AvgDecimal128,
                    output_type: DataType::Decimal128(*precision, *scale),
                    intermediate_type: data_type.clone(),
                    input_arg_type: None,
                    count_all: false,
                }),
                DataType::Decimal256(precision, scale) => Ok(AggSpec {
                    kind: AggKind::AvgDecimal256,
                    output_type: DataType::Decimal256(*precision, *scale),
                    intermediate_type: data_type.clone(),
                    input_arg_type: None,
                    count_all: false,
                }),
                other => Err(format!(
                    "avg intermediate sum type unsupported: {:?}",
                    other
                )),
            }
        }
        other => Err(format!(
            "avg intermediate unsupported input type: {:?}",
            other
        )),
    }
}

impl AggregateFunction for AvgAgg {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        input_type: Option<&DataType>,
        input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        if input_is_intermediate {
            if let Some(data_type) = input_type {
                avg_spec_from_intermediate_type(func, data_type)
            } else {
                Err("avg intermediate input type missing".to_string())
            }
        } else if let Some(data_type) = input_type {
            avg_spec_from_input_type(data_type)
        } else {
            Err("avg input type missing".to_string())
        }
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        match kind {
            AggKind::AvgInt | AggKind::AvgFloat => (
                std::mem::size_of::<AvgState>(),
                std::mem::align_of::<AvgState>(),
            ),
            AggKind::AvgDecimal128 => (
                std::mem::size_of::<AvgDecimal128State>(),
                std::mem::align_of::<AvgDecimal128State>(),
            ),
            AggKind::AvgDecimal256 => (
                std::mem::size_of::<AvgDecimal256State>(),
                std::mem::align_of::<AvgDecimal256State>(),
            ),
            other => unreachable!("unexpected kind for avg: {:?}", other),
        }
    }

    fn build_input_view<'a>(
        &self,
        spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        match spec.kind {
            AggKind::AvgInt => {
                let arr = array
                    .as_ref()
                    .ok_or_else(|| "int input missing".to_string())?;
                Ok(AggInputView::Int(IntArrayView::new(arr)?))
            }
            AggKind::AvgFloat => {
                let arr = array
                    .as_ref()
                    .ok_or_else(|| "float input missing".to_string())?;
                Ok(AggInputView::Float(FloatArrayView::new(arr)?))
            }
            AggKind::AvgDecimal128 => {
                let arr = array
                    .as_ref()
                    .ok_or_else(|| "utf8 input missing".to_string())?;
                Ok(AggInputView::Utf8(Utf8ArrayView::new(arr)?))
            }
            AggKind::AvgDecimal256 => {
                let arr = array
                    .as_ref()
                    .ok_or_else(|| "decimal256 input missing".to_string())?;
                Ok(AggInputView::Any(arr))
            }
            _ => Err("avg input type mismatch".to_string()),
        }
    }

    fn build_merge_view<'a>(
        &self,
        spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        match spec.kind {
            AggKind::AvgInt | AggKind::AvgFloat => {
                let arr = array
                    .as_ref()
                    .ok_or_else(|| "avg input missing".to_string())?;
                if matches!(arr.data_type(), DataType::Binary) {
                    Ok(AggInputView::Binary(
                        arr.as_any()
                            .downcast_ref::<BinaryArray>()
                            .ok_or_else(|| "failed to downcast to BinaryArray".to_string())?,
                    ))
                } else if matches!(arr.data_type(), DataType::Utf8) {
                    Ok(AggInputView::Utf8(Utf8ArrayView::new(arr)?))
                } else {
                    let struct_arr = arr
                        .as_any()
                        .downcast_ref::<StructArray>()
                        .ok_or_else(|| "failed to downcast to StructArray".to_string())?;
                    if struct_arr.num_columns() != 2 {
                        return Err("avg intermediate expects 2 fields".to_string());
                    }
                    let sum = struct_arr.column(0);
                    let count = struct_arr.column(1);
                    let sum_view = FloatArrayView::new(sum)?;
                    let count_view = IntArrayView::new(count)?;
                    Ok(AggInputView::AvgState(AvgStateView {
                        sums: sum_view,
                        counts: count_view,
                    }))
                }
            }
            AggKind::AvgDecimal128 => {
                let arr = array
                    .as_ref()
                    .ok_or_else(|| "avg input missing".to_string())?;
                if matches!(arr.data_type(), DataType::Binary) {
                    Ok(AggInputView::Binary(
                        arr.as_any()
                            .downcast_ref::<BinaryArray>()
                            .ok_or_else(|| "failed to downcast to BinaryArray".to_string())?,
                    ))
                } else if matches!(arr.data_type(), DataType::Utf8) {
                    Ok(AggInputView::Utf8(Utf8ArrayView::new(arr)?))
                } else {
                    let struct_arr = arr
                        .as_any()
                        .downcast_ref::<StructArray>()
                        .ok_or_else(|| "failed to downcast to StructArray".to_string())?;
                    if struct_arr.num_columns() != 2 {
                        return Err("avg decimal intermediate expects 2 fields".to_string());
                    }
                    let sum = struct_arr.column(0);
                    let count = struct_arr.column(1);
                    let sum_view = sum
                        .as_any()
                        .downcast_ref::<Decimal128Array>()
                        .ok_or_else(|| "failed to downcast to Decimal128Array".to_string())?;
                    let count_view = IntArrayView::new(count)?;
                    Ok(AggInputView::AvgDecimalState(AvgDecimalStateView {
                        sums: sum_view,
                        counts: count_view,
                    }))
                }
            }
            AggKind::AvgDecimal256 => {
                let arr = array
                    .as_ref()
                    .ok_or_else(|| "avg input missing".to_string())?;
                if matches!(arr.data_type(), DataType::Binary) {
                    Ok(AggInputView::Binary(
                        arr.as_any()
                            .downcast_ref::<BinaryArray>()
                            .ok_or_else(|| "failed to downcast to BinaryArray".to_string())?,
                    ))
                } else if matches!(arr.data_type(), DataType::Utf8) {
                    Ok(AggInputView::Utf8(Utf8ArrayView::new(arr)?))
                } else {
                    Ok(AggInputView::Any(arr))
                }
            }
            _ => Err("avg merge input type mismatch".to_string()),
        }
    }

    fn init_state(&self, spec: &AggSpec, ptr: *mut u8) {
        match spec.kind {
            AggKind::AvgInt | AggKind::AvgFloat => unsafe {
                std::ptr::write(ptr as *mut AvgState, AvgState { sum: 0.0, count: 0 });
            },
            AggKind::AvgDecimal128 => unsafe {
                std::ptr::write(
                    ptr as *mut AvgDecimal128State,
                    AvgDecimal128State { sum: 0, count: 0 },
                );
            },
            AggKind::AvgDecimal256 => unsafe {
                std::ptr::write(
                    ptr as *mut AvgDecimal256State,
                    AvgDecimal256State {
                        sum: i256::ZERO,
                        count: 0,
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
            AggKind::AvgInt => update_avg_int(offset, state_ptrs, input),
            AggKind::AvgFloat => update_avg_float(offset, state_ptrs, input),
            AggKind::AvgDecimal128 => {
                let sum_scale =
                    if matches!(spec.intermediate_type, DataType::Binary | DataType::Utf8) {
                        match spec.input_arg_type.as_ref() {
                            Some(DataType::Decimal128(_, scale)) => *scale,
                            other => {
                                return Err(format!(
                                    "avg decimal arg0 type missing/mismatch: {:?}",
                                    other
                                ));
                            }
                        }
                    } else {
                        avg_decimal_sum_scale(&spec.intermediate_type, &spec.output_type)?
                    };
                update_avg_decimal128(offset, state_ptrs, input, sum_scale)
            }
            AggKind::AvgDecimal256 => {
                let sum_scale =
                    if matches!(spec.intermediate_type, DataType::Binary | DataType::Utf8) {
                        match spec.input_arg_type.as_ref() {
                            Some(DataType::Decimal256(_, scale)) => *scale,
                            other => {
                                return Err(format!(
                                    "avg decimal256 arg0 type missing/mismatch: {:?}",
                                    other
                                ));
                            }
                        }
                    } else {
                        avg_decimal_sum_scale(&spec.intermediate_type, &spec.output_type)?
                    };
                update_avg_decimal256(offset, state_ptrs, input, sum_scale)
            }
            _ => Err("avg update kind mismatch".to_string()),
        }
    }

    fn merge_batch(
        &self,
        spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        match spec.kind {
            AggKind::AvgInt | AggKind::AvgFloat => merge_avg(offset, state_ptrs, input),
            AggKind::AvgDecimal128 => merge_avg_decimal128(offset, state_ptrs, input),
            AggKind::AvgDecimal256 => merge_avg_decimal256(offset, state_ptrs, input),
            _ => Err("avg merge kind mismatch".to_string()),
        }
    }

    fn build_array(
        &self,
        spec: &AggSpec,
        offset: usize,
        group_states: &[AggStatePtr],
        output_intermediate: bool,
    ) -> Result<ArrayRef, String> {
        match spec.kind {
            AggKind::AvgInt | AggKind::AvgFloat => {
                if output_intermediate {
                    if matches!(spec.intermediate_type, DataType::Binary) {
                        build_avg_intermediate_binary_array(offset, group_states)
                    } else if matches!(spec.intermediate_type, DataType::Utf8) {
                        build_avg_intermediate_utf8_array(offset, group_states)
                    } else {
                        build_avg_intermediate_array(offset, group_states)
                    }
                } else {
                    build_avg_array(offset, group_states)
                }
            }
            AggKind::AvgDecimal128 => {
                if output_intermediate {
                    build_avg_decimal_intermediate_array(
                        offset,
                        group_states,
                        &spec.intermediate_type,
                    )
                } else {
                    let arg_scale = spec.input_arg_type.as_ref().and_then(|t| match t {
                        DataType::Decimal128(_, s) => Some(*s),
                        _ => None,
                    });
                    build_avg_decimal_array(
                        offset,
                        group_states,
                        &spec.output_type,
                        &spec.intermediate_type,
                        arg_scale,
                    )
                }
            }
            AggKind::AvgDecimal256 => {
                if output_intermediate {
                    build_avg_decimal256_intermediate_array(
                        offset,
                        group_states,
                        &spec.intermediate_type,
                    )
                } else {
                    let arg_scale = spec.input_arg_type.as_ref().and_then(|t| match t {
                        DataType::Decimal256(_, s) => Some(*s),
                        _ => None,
                    });
                    build_avg_decimal256_array(
                        offset,
                        group_states,
                        &spec.output_type,
                        &spec.intermediate_type,
                        arg_scale,
                    )
                }
            }
            _ => Err("avg output kind mismatch".to_string()),
        }
    }
}

fn update_avg_int(
    offset: usize,
    state_ptrs: &[AggStatePtr],
    input: &AggInputView,
) -> Result<(), String> {
    match input {
        AggInputView::Int(view) => {
            for (row, &base) in state_ptrs.iter().enumerate() {
                if let Some(v) = view.value_at(row) {
                    let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut AvgState) };
                    state.sum += v as f64;
                    state.count += 1;
                }
            }
            Ok(())
        }
        _ => Err("avg int input type mismatch".to_string()),
    }
}

fn update_avg_float(
    offset: usize,
    state_ptrs: &[AggStatePtr],
    input: &AggInputView,
) -> Result<(), String> {
    match input {
        AggInputView::Float(view) => {
            for (row, &base) in state_ptrs.iter().enumerate() {
                if let Some(v) = view.value_at(row) {
                    let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut AvgState) };
                    state.sum += v;
                    state.count += 1;
                }
            }
            Ok(())
        }
        _ => Err("avg float input type mismatch".to_string()),
    }
}

fn update_avg_decimal128(
    offset: usize,
    state_ptrs: &[AggStatePtr],
    input: &AggInputView,
    sum_scale: i8,
) -> Result<(), String> {
    match input {
        AggInputView::Utf8(view) => match view {
            Utf8ArrayView::Decimal128(arr, input_scale) => {
                let scale_diff = sum_scale as i32 - *input_scale as i32;
                let factor = if scale_diff == 0 {
                    None
                } else {
                    Some(pow10_i128(scale_diff.abs() as usize)?)
                };
                for (row, &base) in state_ptrs.iter().enumerate() {
                    if arr.is_null(row) {
                        continue;
                    }
                    let mut value = arr.value(row);
                    if let Some(factor) = factor {
                        if scale_diff > 0 {
                            value = value
                                .checked_mul(factor)
                                .ok_or_else(|| "decimal overflow".to_string())?;
                        } else {
                            value /= factor;
                        }
                    }
                    let state =
                        unsafe { &mut *((base as *mut u8).add(offset) as *mut AvgDecimal128State) };
                    state.sum += value;
                    state.count += 1;
                }
                Ok(())
            }
            _ => Err("avg decimal input type mismatch".to_string()),
        },
        _ => Err("avg decimal input type mismatch".to_string()),
    }
}

fn update_avg_decimal256(
    offset: usize,
    state_ptrs: &[AggStatePtr],
    input: &AggInputView,
    sum_scale: i8,
) -> Result<(), String> {
    let AggInputView::Any(array) = input else {
        return Err("avg decimal256 input type mismatch".to_string());
    };
    let (arr, input_scale) = match array.data_type() {
        DataType::Decimal256(_, scale) => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| "failed to downcast to Decimal256Array".to_string())?;
            (arr, *scale)
        }
        other => return Err(format!("avg decimal256 input type mismatch: {:?}", other)),
    };
    let scale_diff = sum_scale as i32 - input_scale as i32;
    let factor = if scale_diff == 0 {
        None
    } else {
        Some(pow10_i256(scale_diff.abs() as usize)?)
    };
    for (row, &base) in state_ptrs.iter().enumerate() {
        if arr.is_null(row) {
            continue;
        }
        let mut value = arr.value(row);
        if let Some(factor) = factor {
            if scale_diff > 0 {
                value = value.wrapping_mul(factor);
            } else {
                value = value
                    .checked_div(factor)
                    .ok_or_else(|| "decimal overflow".to_string())?;
            }
        }
        let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut AvgDecimal256State) };
        state.sum = state.sum.wrapping_add(value);
        state.count += 1;
    }
    Ok(())
}

fn merge_avg(
    offset: usize,
    state_ptrs: &[AggStatePtr],
    input: &AggInputView,
) -> Result<(), String> {
    match input {
        AggInputView::AvgState(view) => {
            for (row, &base) in state_ptrs.iter().enumerate() {
                if let Some((sum, count)) = view.value_at(row) {
                    let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut AvgState) };
                    state.sum += sum;
                    state.count += count;
                }
            }
            Ok(())
        }
        AggInputView::Binary(arr) => {
            for (row, &base) in state_ptrs.iter().enumerate() {
                if arr.is_null(row) {
                    continue;
                }
                let bytes = arr.value(row);
                if bytes.len() != 16 {
                    return Err(format!("invalid avg binary state length: {}", bytes.len()));
                }
                let mut sum_b = [0u8; 8];
                sum_b.copy_from_slice(&bytes[..8]);
                let mut cnt_b = [0u8; 8];
                cnt_b.copy_from_slice(&bytes[8..16]);
                let sum = f64::from_le_bytes(sum_b);
                let count = i64::from_le_bytes(cnt_b);
                let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut AvgState) };
                state.sum += sum;
                state.count += count;
            }
            Ok(())
        }
        AggInputView::Utf8(view) => match view {
            Utf8ArrayView::Utf8(arr) => {
                for (row, &base) in state_ptrs.iter().enumerate() {
                    if arr.is_null(row) {
                        continue;
                    }
                    let s = arr.value(row);
                    let (sum_s, count_s) = s
                        .split_once(',')
                        .ok_or_else(|| format!("invalid avg state '{}': missing ','", s))?;
                    let sum = sum_s
                        .parse::<f64>()
                        .map_err(|e| format!("invalid avg state sum '{}': {}", sum_s, e))?;
                    let count = count_s
                        .parse::<i64>()
                        .map_err(|e| format!("invalid avg state count '{}': {}", count_s, e))?;
                    let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut AvgState) };
                    state.sum += sum;
                    state.count += count;
                }
                Ok(())
            }
            _ => Err("avg intermediate type mismatch".to_string()),
        },
        _ => Err("avg merge input type mismatch".to_string()),
    }
}

fn merge_avg_decimal128(
    offset: usize,
    state_ptrs: &[AggStatePtr],
    input: &AggInputView,
) -> Result<(), String> {
    match input {
        AggInputView::AvgDecimalState(view) => {
            for (row, &base) in state_ptrs.iter().enumerate() {
                if let Some((sum, count)) = view.value_at(row) {
                    let state =
                        unsafe { &mut *((base as *mut u8).add(offset) as *mut AvgDecimal128State) };
                    state.sum += sum;
                    state.count += count;
                }
            }
            Ok(())
        }
        AggInputView::Binary(arr) => {
            for (row, &base) in state_ptrs.iter().enumerate() {
                if arr.is_null(row) {
                    continue;
                }
                let bytes = arr.value(row);
                if bytes.len() != 24 {
                    return Err(format!(
                        "invalid avg decimal binary state length: {}",
                        bytes.len()
                    ));
                }
                let mut sum_b = [0u8; 16];
                sum_b.copy_from_slice(&bytes[..16]);
                let mut cnt_b = [0u8; 8];
                cnt_b.copy_from_slice(&bytes[16..24]);
                let sum = i128::from_le_bytes(sum_b);
                let count = i64::from_le_bytes(cnt_b);
                let state =
                    unsafe { &mut *((base as *mut u8).add(offset) as *mut AvgDecimal128State) };
                state.sum += sum;
                state.count += count;
            }
            Ok(())
        }
        AggInputView::Utf8(view) => match view {
            Utf8ArrayView::Utf8(arr) => {
                for (row, &base) in state_ptrs.iter().enumerate() {
                    if arr.is_null(row) {
                        continue;
                    }
                    let s = arr.value(row);
                    let (sum_s, count_s) = s
                        .split_once(',')
                        .ok_or_else(|| format!("invalid avg decimal state '{}': missing ','", s))?;
                    let sum = sum_s
                        .parse::<i128>()
                        .map_err(|e| format!("invalid avg decimal state sum '{}': {}", sum_s, e))?;
                    let count = count_s.parse::<i64>().map_err(|e| {
                        format!("invalid avg decimal state count '{}': {}", count_s, e)
                    })?;
                    let state =
                        unsafe { &mut *((base as *mut u8).add(offset) as *mut AvgDecimal128State) };
                    state.sum += sum;
                    state.count += count;
                }
                Ok(())
            }
            _ => Err("avg decimal intermediate type mismatch".to_string()),
        },
        _ => Err("avg decimal merge input type mismatch".to_string()),
    }
}

fn merge_avg_decimal256(
    offset: usize,
    state_ptrs: &[AggStatePtr],
    input: &AggInputView,
) -> Result<(), String> {
    match input {
        AggInputView::Any(array) => {
            let struct_arr = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| "failed to downcast to StructArray".to_string())?;
            if struct_arr.num_columns() != 2 {
                return Err("avg decimal256 intermediate expects 2 fields".to_string());
            }
            let sum_arr = struct_arr
                .column(0)
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| "failed to downcast to Decimal256Array".to_string())?;
            let count_view = IntArrayView::new(struct_arr.column(1))?;
            for (row, &base) in state_ptrs.iter().enumerate() {
                if sum_arr.is_null(row) {
                    continue;
                }
                let Some(count) = count_view.value_at(row) else {
                    continue;
                };
                let state =
                    unsafe { &mut *((base as *mut u8).add(offset) as *mut AvgDecimal256State) };
                state.sum = state.sum.wrapping_add(sum_arr.value(row));
                state.count += count;
            }
            Ok(())
        }
        AggInputView::Binary(arr) => {
            for (row, &base) in state_ptrs.iter().enumerate() {
                if arr.is_null(row) {
                    continue;
                }
                let bytes = arr.value(row);
                if bytes.len() != 40 {
                    return Err(format!(
                        "invalid avg decimal256 binary state length: {}",
                        bytes.len()
                    ));
                }
                let mut sum_b = [0u8; 32];
                sum_b.copy_from_slice(&bytes[..32]);
                let mut cnt_b = [0u8; 8];
                cnt_b.copy_from_slice(&bytes[32..40]);
                let sum = i256::from_le_bytes(sum_b);
                let count = i64::from_le_bytes(cnt_b);
                let state =
                    unsafe { &mut *((base as *mut u8).add(offset) as *mut AvgDecimal256State) };
                state.sum = state.sum.wrapping_add(sum);
                state.count += count;
            }
            Ok(())
        }
        AggInputView::Utf8(view) => match view {
            Utf8ArrayView::Utf8(arr) => {
                for (row, &base) in state_ptrs.iter().enumerate() {
                    if arr.is_null(row) {
                        continue;
                    }
                    let s = arr.value(row);
                    let (sum_s, count_s) = s.split_once(',').ok_or_else(|| {
                        format!("invalid avg decimal256 state '{}': missing ','", s)
                    })?;
                    let sum = sum_s.parse::<i256>().map_err(|e| {
                        format!("invalid avg decimal256 state sum '{}': {}", sum_s, e)
                    })?;
                    let count = count_s.parse::<i64>().map_err(|e| {
                        format!("invalid avg decimal256 state count '{}': {}", count_s, e)
                    })?;
                    let state =
                        unsafe { &mut *((base as *mut u8).add(offset) as *mut AvgDecimal256State) };
                    state.sum = state.sum.wrapping_add(sum);
                    state.count += count;
                }
                Ok(())
            }
            _ => Err("avg decimal256 intermediate type mismatch".to_string()),
        },
        _ => Err("avg decimal256 merge input type mismatch".to_string()),
    }
}

fn build_avg_array(offset: usize, group_states: &[AggStatePtr]) -> Result<ArrayRef, String> {
    let mut builder = Float64Builder::new();
    for &base in group_states {
        let state = unsafe { &*((base as *mut u8).add(offset) as *const AvgState) };
        if state.count == 0 {
            builder.append_null();
        } else {
            builder.append_value(state.sum / state.count as f64);
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_avg_intermediate_array(
    offset: usize,
    group_states: &[AggStatePtr],
) -> Result<ArrayRef, String> {
    let mut sum_builder = Float64Builder::new();
    let mut count_builder = Int64Builder::new();
    for &base in group_states {
        let state = unsafe { &*((base as *mut u8).add(offset) as *const AvgState) };
        if state.count == 0 {
            sum_builder.append_null();
            count_builder.append_null();
        } else {
            sum_builder.append_value(state.sum);
            count_builder.append_value(state.count);
        }
    }
    let sum_array = Arc::new(sum_builder.finish()) as ArrayRef;
    let count_array = Arc::new(count_builder.finish()) as ArrayRef;
    let fields: Fields = vec![
        Field::new("sum", DataType::Float64, true),
        Field::new("count", DataType::Int64, true),
    ]
    .into();
    let arrays = vec![sum_array, count_array];
    let struct_array = StructArray::try_new(fields, arrays, None).map_err(|e| e.to_string())?;
    Ok(Arc::new(struct_array))
}

fn build_avg_intermediate_utf8_array(
    offset: usize,
    group_states: &[AggStatePtr],
) -> Result<ArrayRef, String> {
    let mut builder = StringBuilder::new();
    for &base in group_states {
        let state = unsafe { &*((base as *mut u8).add(offset) as *const AvgState) };
        if state.count == 0 {
            builder.append_null();
        } else {
            builder.append_value(format!("{},{}", state.sum, state.count));
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_avg_intermediate_binary_array(
    offset: usize,
    group_states: &[AggStatePtr],
) -> Result<ArrayRef, String> {
    let mut builder = BinaryBuilder::new();
    for &base in group_states {
        let state = unsafe { &*((base as *mut u8).add(offset) as *const AvgState) };
        if state.count == 0 {
            builder.append_null();
            continue;
        }
        let mut buf = [0u8; 16];
        buf[..8].copy_from_slice(&state.sum.to_le_bytes());
        buf[8..].copy_from_slice(&state.count.to_le_bytes());
        builder.append_value(&buf);
    }
    Ok(Arc::new(builder.finish()))
}

fn build_avg_decimal_intermediate_array(
    offset: usize,
    group_states: &[AggStatePtr],
    intermediate_type: &DataType,
) -> Result<ArrayRef, String> {
    if matches!(intermediate_type, DataType::Binary) {
        let mut builder = BinaryBuilder::new();
        for &base in group_states {
            let state = unsafe { &*((base as *mut u8).add(offset) as *const AvgDecimal128State) };
            if state.count == 0 {
                builder.append_null();
                continue;
            }
            let mut buf = [0u8; 24];
            buf[..16].copy_from_slice(&state.sum.to_le_bytes());
            buf[16..].copy_from_slice(&state.count.to_le_bytes());
            builder.append_value(&buf);
        }
        return Ok(Arc::new(builder.finish()));
    }
    if matches!(intermediate_type, DataType::Utf8) {
        let mut builder = StringBuilder::new();
        for &base in group_states {
            let state = unsafe { &*((base as *mut u8).add(offset) as *const AvgDecimal128State) };
            if state.count == 0 {
                builder.append_null();
            } else {
                builder.append_value(format!("{},{}", state.sum, state.count));
            }
        }
        return Ok(Arc::new(builder.finish()));
    }

    let (sum_precision, sum_scale) = avg_decimal_sum_info(intermediate_type, None)?;
    let mut sum_values = Vec::with_capacity(group_states.len());
    let mut count_values = Vec::with_capacity(group_states.len());
    for &base in group_states {
        let state = unsafe { &*((base as *mut u8).add(offset) as *const AvgDecimal128State) };
        if state.count == 0 {
            sum_values.push(None);
            count_values.push(None);
        } else {
            sum_values.push(Some(state.sum));
            count_values.push(Some(state.count));
        }
    }
    let sum_array = Decimal128Array::from(sum_values)
        .with_precision_and_scale(sum_precision, sum_scale)
        .map_err(|e| e.to_string())?;
    let count_array = Int64Array::from(count_values);
    let fields: Fields = vec![
        Field::new("sum", DataType::Decimal128(sum_precision, sum_scale), true),
        Field::new("count", DataType::Int64, true),
    ]
    .into();
    let arrays = vec![
        Arc::new(sum_array) as ArrayRef,
        Arc::new(count_array) as ArrayRef,
    ];
    let struct_array = StructArray::try_new(fields, arrays, None).map_err(|e| e.to_string())?;
    Ok(Arc::new(struct_array))
}

fn build_avg_decimal_array(
    offset: usize,
    group_states: &[AggStatePtr],
    output_type: &DataType,
    intermediate_type: &DataType,
    arg_scale: Option<i8>,
) -> Result<ArrayRef, String> {
    let (out_precision, out_scale) = match output_type {
        DataType::Decimal128(precision, scale) => (*precision, *scale),
        other => return Err(format!("decimal output type mismatch: {:?}", other)),
    };
    let sum_scale = if matches!(intermediate_type, DataType::Binary | DataType::Utf8) {
        arg_scale.ok_or_else(|| "avg decimal arg scale missing".to_string())?
    } else {
        avg_decimal_sum_info(intermediate_type, None)?.1
    };
    let scale_diff = out_scale as i32 - sum_scale as i32;
    let factor = if scale_diff == 0 {
        None
    } else {
        Some(pow10_i128(scale_diff.abs() as usize)?)
    };
    let mut values = Vec::with_capacity(group_states.len());
    for &base in group_states {
        let state = unsafe { &*((base as *mut u8).add(offset) as *const AvgDecimal128State) };
        if state.count == 0 {
            values.push(None);
            continue;
        }
        let mut sum = state.sum;
        if let Some(factor) = factor {
            if scale_diff > 0 {
                sum = sum
                    .checked_mul(factor)
                    .ok_or_else(|| "decimal overflow".to_string())?;
            } else {
                sum /= factor;
            }
        }
        // StarRocks BE uses decimal_div_integer -> DecimalV3Arithmetics::div_round.
        // Use ROUND_HALF_UP instead of truncating division.
        let avg = div_round_i128(sum, state.count as i128);
        values.push(Some(avg));
    }
    let array = Decimal128Array::from(values)
        .with_precision_and_scale(out_precision, out_scale)
        .map_err(|e| e.to_string())?;
    Ok(Arc::new(array))
}

fn build_avg_decimal256_intermediate_array(
    offset: usize,
    group_states: &[AggStatePtr],
    intermediate_type: &DataType,
) -> Result<ArrayRef, String> {
    if matches!(intermediate_type, DataType::Binary) {
        let mut builder = BinaryBuilder::new();
        for &base in group_states {
            let state = unsafe { &*((base as *mut u8).add(offset) as *const AvgDecimal256State) };
            if state.count == 0 {
                builder.append_null();
                continue;
            }
            let mut buf = [0u8; 40];
            buf[..32].copy_from_slice(&state.sum.to_le_bytes());
            buf[32..].copy_from_slice(&state.count.to_le_bytes());
            builder.append_value(&buf);
        }
        return Ok(Arc::new(builder.finish()));
    }
    if matches!(intermediate_type, DataType::Utf8) {
        let mut builder = StringBuilder::new();
        for &base in group_states {
            let state = unsafe { &*((base as *mut u8).add(offset) as *const AvgDecimal256State) };
            if state.count == 0 {
                builder.append_null();
            } else {
                builder.append_value(format!("{},{}", state.sum, state.count));
            }
        }
        return Ok(Arc::new(builder.finish()));
    }

    let (sum_precision, sum_scale) = avg_decimal_sum_info(intermediate_type, None)?;
    let mut sum_values = Vec::with_capacity(group_states.len());
    let mut count_values = Vec::with_capacity(group_states.len());
    for &base in group_states {
        let state = unsafe { &*((base as *mut u8).add(offset) as *const AvgDecimal256State) };
        if state.count == 0 {
            sum_values.push(None);
            count_values.push(None);
        } else {
            sum_values.push(Some(state.sum));
            count_values.push(Some(state.count));
        }
    }
    let sum_array = Decimal256Array::from(sum_values)
        .with_precision_and_scale(sum_precision, sum_scale)
        .map_err(|e| e.to_string())?;
    let count_array = Int64Array::from(count_values);
    let fields: Fields = vec![
        Field::new("sum", DataType::Decimal256(sum_precision, sum_scale), true),
        Field::new("count", DataType::Int64, true),
    ]
    .into();
    let arrays = vec![
        Arc::new(sum_array) as ArrayRef,
        Arc::new(count_array) as ArrayRef,
    ];
    let struct_array = StructArray::try_new(fields, arrays, None).map_err(|e| e.to_string())?;
    Ok(Arc::new(struct_array))
}

fn build_avg_decimal256_array(
    offset: usize,
    group_states: &[AggStatePtr],
    output_type: &DataType,
    intermediate_type: &DataType,
    arg_scale: Option<i8>,
) -> Result<ArrayRef, String> {
    let (out_precision, out_scale) = match output_type {
        DataType::Decimal256(precision, scale) => (*precision, *scale),
        other => return Err(format!("decimal256 output type mismatch: {:?}", other)),
    };
    let sum_scale = if matches!(intermediate_type, DataType::Binary | DataType::Utf8) {
        arg_scale.ok_or_else(|| "avg decimal256 arg scale missing".to_string())?
    } else {
        avg_decimal_sum_info(intermediate_type, None)?.1
    };
    let scale_diff = out_scale as i32 - sum_scale as i32;
    let factor = if scale_diff == 0 {
        None
    } else {
        Some(pow10_i256(scale_diff.abs() as usize)?)
    };

    let mut values = Vec::with_capacity(group_states.len());
    for &base in group_states {
        let state = unsafe { &*((base as *mut u8).add(offset) as *const AvgDecimal256State) };
        if state.count == 0 {
            values.push(None);
            continue;
        }
        let count = i256::from_i128(state.count as i128);
        let avg = if let Some(factor) = factor {
            if scale_diff > 0 {
                // Match StarRocks decimal_div_integer semantics: scale first with wrapping arithmetic.
                let scaled = state.sum.wrapping_mul(factor);
                div_round_i256(scaled, count)?
            } else {
                let scaled = state
                    .sum
                    .checked_div(factor)
                    .ok_or_else(|| "decimal overflow".to_string())?;
                div_round_i256(scaled, count)?
            }
        } else {
            div_round_i256(state.sum, count)?
        };
        values.push(Some(avg));
    }
    let array = Decimal256Array::from(values)
        .with_precision_and_scale(out_precision, out_scale)
        .map_err(|e| e.to_string())?;
    Ok(Arc::new(array))
}
