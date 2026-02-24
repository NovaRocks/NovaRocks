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
use arrow::array::{ArrayRef, BinaryArray, BinaryBuilder, Float64Builder, StringBuilder};
use arrow::datatypes::DataType;

use crate::exec::node::aggregate::AggFunction;

use super::super::*;
use super::AggregateFunction;

pub(super) struct VarStdAgg;

fn kind_from_name(name: &str) -> Option<AggKind> {
    match name {
        "variance" | "variance_pop" | "var_pop" => Some(AggKind::VariancePop),
        "variance_samp" | "var_samp" => Some(AggKind::VarianceSamp),
        "stddev" | "std" | "stddev_pop" => Some(AggKind::StddevPop),
        "stddev_samp" => Some(AggKind::StddevSamp),
        _ => None,
    }
}

fn dev_from_ave_spec_from_input_type(name: &str, data_type: &DataType) -> Result<AggSpec, String> {
    let kind = kind_from_name(name).ok_or_else(|| format!("unsupported agg function: {name}"))?;
    match data_type {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64 => Ok(AggSpec {
            kind,
            output_type: DataType::Float64,
            intermediate_type: DataType::Binary,
            input_arg_type: None,
            count_all: false,
        }),
        other => Err(format!("{} unsupported input type: {:?}", name, other)),
    }
}

fn dev_from_ave_spec_from_intermediate_type(
    func: &AggFunction,
    name: &str,
    data_type: &DataType,
) -> Result<AggSpec, String> {
    let kind = kind_from_name(name).ok_or_else(|| format!("unsupported agg function: {name}"))?;

    match data_type {
        DataType::Binary | DataType::Utf8 => {
            // When the intermediate is opaque, rely on FE signatures.
            let sig = agg_type_signature(func)
                .ok_or_else(|| format!("{name} intermediate type signature missing"))?;
            let output_type = sig
                .output_type
                .as_ref()
                .ok_or_else(|| format!("{name} intermediate output_type signature missing"))?;
            if !matches!(output_type, DataType::Float64) {
                return Err(format!(
                    "{name} intermediate output type unsupported: {:?}",
                    output_type
                ));
            }
            Ok(AggSpec {
                kind,
                output_type: DataType::Float64,
                intermediate_type: data_type.clone(),
                input_arg_type: sig.input_arg_type.clone(),
                count_all: false,
            })
        }
        other => Err(format!(
            "{name} intermediate unsupported input type: {:?}",
            other
        )),
    }
}

impl AggregateFunction for VarStdAgg {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        input_type: Option<&DataType>,
        input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        let name = func.name.as_str();
        if input_is_intermediate {
            let data_type =
                input_type.ok_or_else(|| format!("{name} intermediate input type missing"))?;
            dev_from_ave_spec_from_intermediate_type(func, name, data_type)
        } else {
            let data_type = input_type.ok_or_else(|| format!("{name} input type missing"))?;
            dev_from_ave_spec_from_input_type(name, data_type)
        }
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        match kind {
            AggKind::VariancePop
            | AggKind::VarianceSamp
            | AggKind::StddevPop
            | AggKind::StddevSamp => (
                std::mem::size_of::<DevFromAveState>(),
                std::mem::align_of::<DevFromAveState>(),
            ),
            other => unreachable!("unexpected kind for variance/stddev: {:?}", other),
        }
    }

    fn build_input_view<'a>(
        &self,
        spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "variance/stddev input missing".to_string())?;
        match arr.data_type() {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                Ok(AggInputView::Int(IntArrayView::new(arr)?))
            }
            DataType::Float32 | DataType::Float64 => {
                Ok(AggInputView::Float(FloatArrayView::new(arr)?))
            }
            other => Err(format!(
                "variance/stddev input type mismatch: {:?} for {:?}",
                other, spec.kind
            )),
        }
    }

    fn build_merge_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "variance/stddev merge input missing".to_string())?;
        match arr.data_type() {
            DataType::Binary => arr
                .as_any()
                .downcast_ref::<BinaryArray>()
                .map(AggInputView::Binary)
                .ok_or_else(|| "failed to downcast to BinaryArray".to_string()),
            DataType::Utf8 => Ok(AggInputView::Utf8(Utf8ArrayView::new(arr)?)),
            other => Err(format!(
                "variance/stddev merge input type mismatch: {:?}",
                other
            )),
        }
    }

    fn init_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe { std::ptr::write(ptr as *mut DevFromAveState, DevFromAveState::default()) }
    }

    fn drop_state(&self, _spec: &AggSpec, _ptr: *mut u8) {}

    fn update_batch(
        &self,
        _spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        match input {
            AggInputView::Int(view) => {
                for (row, &base) in state_ptrs.iter().enumerate() {
                    if let Some(v) = view.value_at(row) {
                        update_state(base, offset, v as f64);
                    }
                }
                Ok(())
            }
            AggInputView::Float(view) => {
                for (row, &base) in state_ptrs.iter().enumerate() {
                    if let Some(v) = view.value_at(row) {
                        update_state(base, offset, v);
                    }
                }
                Ok(())
            }
            _ => Err("variance/stddev update input type mismatch".to_string()),
        }
    }

    fn merge_batch(
        &self,
        _spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        match input {
            AggInputView::Binary(arr) => {
                for (row, &base) in state_ptrs.iter().enumerate() {
                    if arr.is_null(row) {
                        continue;
                    }
                    let bytes = arr.value(row);
                    let (mean, m2, count) = parse_binary_state(bytes)?;
                    merge_state(base, offset, mean, m2, count);
                }
                Ok(())
            }
            AggInputView::Utf8(view) => {
                for (row, &base) in state_ptrs.iter().enumerate() {
                    let Some(s) = view.value_at(row) else {
                        continue;
                    };
                    let (mean, m2, count) = parse_utf8_state(&s)?;
                    merge_state(base, offset, mean, m2, count);
                }
                Ok(())
            }
            _ => Err("variance/stddev merge input type mismatch".to_string()),
        }
    }

    fn build_array(
        &self,
        spec: &AggSpec,
        offset: usize,
        group_states: &[AggStatePtr],
        output_intermediate: bool,
    ) -> Result<ArrayRef, String> {
        if output_intermediate {
            match &spec.intermediate_type {
                DataType::Binary => build_intermediate_binary_array(offset, group_states),
                DataType::Utf8 => build_intermediate_utf8_array(offset, group_states),
                other => Err(format!(
                    "variance/stddev intermediate output type unsupported: {:?}",
                    other
                )),
            }
        } else {
            build_final_array(spec, offset, group_states)
        }
    }
}

fn update_state(base: AggStatePtr, offset: usize, v: f64) {
    let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut DevFromAveState) };
    let temp = state.count + 1;
    let delta = v - state.mean;
    let r = delta / (temp as f64);
    state.mean += r;
    state.m2 += (state.count as f64) * delta * r;
    state.count = temp;
}

fn merge_state(base: AggStatePtr, offset: usize, mean: f64, m2: f64, count: i64) {
    if count <= 0 {
        return;
    }
    let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut DevFromAveState) };
    if state.count == 0 {
        state.mean = mean;
        state.m2 = m2;
        state.count = count;
        return;
    }
    let delta = state.mean - mean;
    let count_state = state.count as f64;
    let count_in = count as f64;
    let sum_count = count_state + count_in;
    state.mean = mean + delta * (count_state / sum_count);
    state.m2 = m2 + state.m2 + (delta * delta) * (count_in * count_state / sum_count);
    state.count += count;
}

fn parse_binary_state(bytes: &[u8]) -> Result<(f64, f64, i64), String> {
    if bytes.len() != 24 {
        return Err(format!(
            "variance/stddev intermediate binary size mismatch: expected 24, got {}",
            bytes.len()
        ));
    }
    let mean = f64::from_le_bytes(bytes[0..8].try_into().unwrap());
    let m2 = f64::from_le_bytes(bytes[8..16].try_into().unwrap());
    let count = i64::from_le_bytes(bytes[16..24].try_into().unwrap());
    Ok((mean, m2, count))
}

fn parse_utf8_state(s: &str) -> Result<(f64, f64, i64), String> {
    let mut it = s.split(',');
    let mean = it
        .next()
        .ok_or_else(|| "variance/stddev intermediate utf8 missing mean".to_string())?
        .parse::<f64>()
        .map_err(|e| e.to_string())?;
    let m2 = it
        .next()
        .ok_or_else(|| "variance/stddev intermediate utf8 missing m2".to_string())?
        .parse::<f64>()
        .map_err(|e| e.to_string())?;
    let count = it
        .next()
        .ok_or_else(|| "variance/stddev intermediate utf8 missing count".to_string())?
        .parse::<i64>()
        .map_err(|e| e.to_string())?;
    Ok((mean, m2, count))
}

fn build_intermediate_binary_array(
    offset: usize,
    group_states: &[AggStatePtr],
) -> Result<ArrayRef, String> {
    let mut builder = BinaryBuilder::new();
    for &base in group_states {
        let state = unsafe { &*((base as *mut u8).add(offset) as *const DevFromAveState) };
        if state.count == 0 {
            builder.append_null();
            continue;
        }
        let mut buf = [0u8; 24];
        buf[..8].copy_from_slice(&state.mean.to_le_bytes());
        buf[8..16].copy_from_slice(&state.m2.to_le_bytes());
        buf[16..].copy_from_slice(&state.count.to_le_bytes());
        builder.append_value(&buf);
    }
    Ok(std::sync::Arc::new(builder.finish()))
}

fn build_intermediate_utf8_array(
    offset: usize,
    group_states: &[AggStatePtr],
) -> Result<ArrayRef, String> {
    let mut builder = StringBuilder::new();
    for &base in group_states {
        let state = unsafe { &*((base as *mut u8).add(offset) as *const DevFromAveState) };
        if state.count == 0 {
            builder.append_null();
        } else {
            builder.append_value(format!("{},{},{}", state.mean, state.m2, state.count));
        }
    }
    Ok(std::sync::Arc::new(builder.finish()))
}

fn build_final_array(
    spec: &AggSpec,
    offset: usize,
    group_states: &[AggStatePtr],
) -> Result<ArrayRef, String> {
    let mut builder = Float64Builder::new();
    for &base in group_states {
        let state = unsafe { &*((base as *mut u8).add(offset) as *const DevFromAveState) };
        match spec.kind {
            AggKind::VariancePop => {
                if state.count == 0 {
                    builder.append_null();
                } else {
                    builder.append_value(state.m2 / (state.count as f64));
                }
            }
            AggKind::VarianceSamp => {
                if state.count <= 1 {
                    builder.append_null();
                } else {
                    builder.append_value(state.m2 / ((state.count - 1) as f64));
                }
            }
            AggKind::StddevPop => {
                if state.count == 0 {
                    builder.append_null();
                } else {
                    builder.append_value((state.m2 / (state.count as f64)).sqrt());
                }
            }
            AggKind::StddevSamp => {
                if state.count <= 1 {
                    builder.append_null();
                } else {
                    builder.append_value((state.m2 / ((state.count - 1) as f64)).sqrt());
                }
            }
            _ => return Err("variance/stddev output kind mismatch".to_string()),
        }
    }
    Ok(std::sync::Arc::new(builder.finish()))
}
