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

use arrow::array::{Array, ArrayRef, BinaryBuilder, StructArray};
use arrow::datatypes::DataType;
use serde::{Deserialize, Serialize};

use crate::exec::expr::agg::functions::common::{
    AggScalarValue, build_scalar_array, compare_scalar_values, scalar_from_array,
};
use crate::exec::expr::function::object::percentile_functions::{numeric_value_at, payload_bytes_at};
use crate::exec::node::aggregate::AggFunction;

use super::super::*;
use super::AggregateFunction;

const EXACT_PERCENTILE_MAGIC: u8 = 0xC3;
const EXACT_PERCENTILE_VERSION: u8 = 1;

pub(super) struct PercentilePlaceholderAgg;

#[derive(Clone, Debug, Serialize, Deserialize)]
enum SerializableScalar {
    Int64(i64),
    Float64(f64),
    Utf8(String),
    Date32(i32),
    Timestamp(i64),
    Decimal128(i128),
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct ExactPercentileState {
    rate: Option<f64>,
    values: Vec<SerializableScalar>,
}

fn state_slot(ptr: *mut u8) -> *mut *mut ExactPercentileState {
    ptr as *mut *mut ExactPercentileState
}

unsafe fn get_or_init_state<'a>(ptr: *mut u8) -> &'a mut ExactPercentileState {
    let slot = state_slot(ptr);
    let raw = unsafe { *slot };
    if raw.is_null() {
        let boxed: Box<ExactPercentileState> = Box::default();
        let raw = Box::into_raw(boxed);
        unsafe {
            *slot = raw;
            &mut *raw
        }
    } else {
        unsafe { &mut *raw }
    }
}

unsafe fn get_state<'a>(ptr: *mut u8) -> Option<&'a ExactPercentileState> {
    let raw = unsafe { *state_slot(ptr) };
    if raw.is_null() {
        None
    } else {
        Some(unsafe { &*raw })
    }
}

unsafe fn take_state(ptr: *mut u8) -> Option<Box<ExactPercentileState>> {
    let slot = state_slot(ptr);
    let raw = unsafe { *slot };
    if raw.is_null() {
        None
    } else {
        unsafe {
            *slot = std::ptr::null_mut();
            Some(Box::from_raw(raw))
        }
    }
}

fn canonical_agg_name(name: &str) -> &str {
    name.split_once('|').map(|(base, _)| base).unwrap_or(name)
}

fn encode_state(state: &ExactPercentileState) -> Vec<u8> {
    let payload = serde_json::to_vec(state).expect("serialize exact percentile state");
    let mut out = Vec::with_capacity(2 + payload.len());
    out.push(EXACT_PERCENTILE_MAGIC);
    out.push(EXACT_PERCENTILE_VERSION);
    out.extend_from_slice(&payload);
    out
}

fn decode_state(payload: &[u8]) -> Result<ExactPercentileState, String> {
    if payload.is_empty() {
        return Ok(ExactPercentileState::default());
    }
    if payload.len() < 2 {
        return Err("exact percentile payload too short".to_string());
    }
    if payload[0] != EXACT_PERCENTILE_MAGIC {
        return Err(format!(
            "unsupported exact percentile payload magic: expected=0x{:02x} actual=0x{:02x}",
            EXACT_PERCENTILE_MAGIC, payload[0]
        ));
    }
    if payload[1] != EXACT_PERCENTILE_VERSION {
        return Err(format!(
            "unsupported exact percentile payload version: expected={} actual={}",
            EXACT_PERCENTILE_VERSION, payload[1]
        ));
    }
    serde_json::from_slice(&payload[2..]).map_err(|e| e.to_string())
}

fn apply_rate(state: &mut ExactPercentileState, rate: f64) -> Result<(), String> {
    if !(0.0..=1.0).contains(&rate) {
        return Err("Percentile rate must be between 0 and 1".to_string());
    }
    match state.rate {
        Some(existing) if (existing - rate).abs() > f64::EPSILON => {
            Err(format!(
                "percentile rate mismatch while merging states: existing={} incoming={}",
                existing, rate
            ))
        }
        Some(_) => Ok(()),
        None => {
            state.rate = Some(rate);
            Ok(())
        }
    }
}

fn scalar_to_serializable(value: AggScalarValue) -> Result<SerializableScalar, String> {
    match value {
        AggScalarValue::Int64(v) => Ok(SerializableScalar::Int64(v)),
        AggScalarValue::Float64(v) => Ok(SerializableScalar::Float64(v)),
        AggScalarValue::Utf8(v) => Ok(SerializableScalar::Utf8(v)),
        AggScalarValue::Date32(v) => Ok(SerializableScalar::Date32(v)),
        AggScalarValue::Timestamp(v) => Ok(SerializableScalar::Timestamp(v)),
        AggScalarValue::Decimal128(v) => Ok(SerializableScalar::Decimal128(v)),
        other => Err(format!(
            "unsupported percentile_disc/cont input scalar {:?}",
            other
        )),
    }
}

fn serializable_to_scalar(value: &SerializableScalar) -> AggScalarValue {
    match value {
        SerializableScalar::Int64(v) => AggScalarValue::Int64(*v),
        SerializableScalar::Float64(v) => AggScalarValue::Float64(*v),
        SerializableScalar::Utf8(v) => AggScalarValue::Utf8(v.clone()),
        SerializableScalar::Date32(v) => AggScalarValue::Date32(*v),
        SerializableScalar::Timestamp(v) => AggScalarValue::Timestamp(*v),
        SerializableScalar::Decimal128(v) => AggScalarValue::Decimal128(*v),
    }
}

fn numeric_from_scalar(value: &AggScalarValue, context: &str) -> Result<f64, String> {
    match value {
        AggScalarValue::Int64(v) => Ok(*v as f64),
        AggScalarValue::Float64(v) => Ok(*v),
        AggScalarValue::Date32(v) => Ok(*v as f64),
        AggScalarValue::Timestamp(v) => Ok(*v as f64),
        AggScalarValue::Decimal128(v) => Ok(*v as f64),
        other => Err(format!(
            "{context}: unsupported percentile_cont interpolation input {:?}",
            other
        )),
    }
}

fn scalar_from_numeric(output_type: &DataType, value: f64) -> Result<AggScalarValue, String> {
    match output_type {
        DataType::Float64 => Ok(AggScalarValue::Float64(value)),
        DataType::Date32 => Ok(AggScalarValue::Date32(value as i32)),
        DataType::Timestamp(_, _) => Ok(AggScalarValue::Timestamp(value as i64)),
        other => Err(format!(
            "unsupported percentile_cont output type {:?}",
            other
        )),
    }
}

fn merge_payload_array(
    array: &ArrayRef,
    offset: usize,
    state_ptrs: &[AggStatePtr],
    context: &str,
) -> Result<(), String> {
    for (row, &base) in state_ptrs.iter().enumerate() {
        let Some(payload) = payload_bytes_at(array, row, context)? else {
            continue;
        };
        let incoming = decode_state(payload)?;
        let ptr = unsafe { (base as *mut u8).add(offset) };
        let state = unsafe { get_or_init_state(ptr) };
        if let Some(rate) = incoming.rate {
            apply_rate(state, rate)?;
        }
        state.values.extend(incoming.values);
    }
    Ok(())
}

fn update_from_struct(
    array: &StructArray,
    offset: usize,
    state_ptrs: &[AggStatePtr],
    context: &str,
) -> Result<(), String> {
    let fields = array.columns();
    if fields.len() < 2 {
        return Err(format!(
            "{context}: percentile_disc/cont expects STRUCT(value, rate) input"
        ));
    }
    let values = fields[0].clone();
    let rates = fields[1].clone();
    for (row, &base) in state_ptrs.iter().enumerate() {
        let ptr = unsafe { (base as *mut u8).add(offset) };
        let state = unsafe { get_or_init_state(ptr) };
        if let Some(rate) = numeric_value_at(&rates, row, context)? {
            apply_rate(state, rate)?;
        }
        let Some(value) = scalar_from_array(&values, row)? else {
            continue;
        };
        state.values.push(scalar_to_serializable(value)?);
    }
    Ok(())
}

fn finalize_cont(state: &ExactPercentileState, output_type: &DataType) -> Result<Option<AggScalarValue>, String> {
    if state.values.is_empty() {
        return Ok(None);
    }
    let rate = state.rate.unwrap_or(0.0);
    let mut values: Vec<AggScalarValue> = state.values.iter().map(serializable_to_scalar).collect();
    values.sort_by(|left, right| compare_scalar_values(left, right).unwrap_or(std::cmp::Ordering::Equal));

    if values.len() == 1 || rate == 1.0 {
        return match output_type {
            DataType::Float64 => Ok(Some(AggScalarValue::Float64(
                numeric_from_scalar(values.last().expect("last"), "percentile_cont")?,
            ))),
            _ => Ok(Some(values.last().expect("last").clone())),
        };
    }

    if rate == 0.0 {
        return match output_type {
            DataType::Float64 => Ok(Some(AggScalarValue::Float64(
                numeric_from_scalar(values.first().expect("first"), "percentile_cont")?,
            ))),
            _ => Ok(Some(values.first().expect("first").clone())),
        };
    }

    let u = ((values.len() - 1) as f64) * rate;
    let index = u.floor() as usize;
    let fraction = u - index as f64;
    if fraction == 0.0 {
        return match output_type {
            DataType::Float64 => Ok(Some(AggScalarValue::Float64(
                numeric_from_scalar(&values[index], "percentile_cont")?,
            ))),
            _ => Ok(Some(values[index].clone())),
        };
    }

    let lower = numeric_from_scalar(&values[index], "percentile_cont")?;
    let upper = numeric_from_scalar(&values[index + 1], "percentile_cont")?;
    let interpolated = lower + fraction * (upper - lower);
    scalar_from_numeric(output_type, interpolated).map(Some)
}

fn finalize_disc(state: &ExactPercentileState) -> Result<Option<AggScalarValue>, String> {
    if state.values.is_empty() {
        return Ok(None);
    }
    let rate = state.rate.unwrap_or(0.0);
    let mut values: Vec<AggScalarValue> = state.values.iter().map(serializable_to_scalar).collect();
    values.sort_by(|left, right| compare_scalar_values(left, right).unwrap_or(std::cmp::Ordering::Equal));
    if values.len() == 1 || rate == 1.0 {
        return Ok(values.last().cloned());
    }
    let index = (((values.len() - 1) as f64) * rate).ceil() as usize;
    Ok(values.get(index).cloned())
}

impl AggregateFunction for PercentilePlaceholderAgg {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        input_type: Option<&DataType>,
        _input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        let sig = func
            .types
            .as_ref()
            .ok_or_else(|| "aggregate type signature is required".to_string())?;
        let kind = match canonical_agg_name(func.name.as_str()) {
            "percentile_cont" => AggKind::PercentileCont,
            "percentile_disc" => AggKind::PercentileDisc,
            "percentile_disc_lc" => AggKind::PercentileDiscLc,
            other => return Err(format!("unsupported percentile aggregate function: {}", other)),
        };
        let output_type = sig
            .output_type
            .as_ref()
            .cloned()
            .or_else(|| input_type.cloned())
            .unwrap_or(DataType::Binary);
        let intermediate_type = sig
            .intermediate_type
            .as_ref()
            .cloned()
            .unwrap_or(DataType::Binary);
        Ok(AggSpec {
            kind,
            output_type,
            intermediate_type,
            input_arg_type: sig.input_arg_type.clone(),
            count_all: false,
        })
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        match kind {
            AggKind::PercentileCont | AggKind::PercentileDisc | AggKind::PercentileDiscLc => (
                std::mem::size_of::<*mut ExactPercentileState>(),
                std::mem::align_of::<*mut ExactPercentileState>(),
            ),
            other => unreachable!("unexpected kind for percentile placeholder: {:?}", other),
        }
    }

    fn build_input_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "percentile_disc/cont input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn build_merge_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "percentile_disc/cont merge input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn init_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::write(ptr as *mut *mut ExactPercentileState, std::ptr::null_mut());
        }
    }

    fn drop_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            let _ = take_state(ptr);
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
            return Err("percentile_disc/cont input type mismatch".to_string());
        };
        if let Some(struct_array) = array.as_any().downcast_ref::<StructArray>() {
            update_from_struct(struct_array, offset, state_ptrs, "percentile_disc_cont_update")
        } else {
            merge_payload_array(array, offset, state_ptrs, "percentile_disc_cont_update")
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
            return Err("percentile_disc/cont merge input type mismatch".to_string());
        };
        merge_payload_array(array, offset, state_ptrs, "percentile_disc_cont_merge")
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

        if output_intermediate {
            let mut builder = BinaryBuilder::new();
            for &base in group_states {
                let ptr = unsafe { (base as *mut u8).add(offset) };
                let state = unsafe { get_state(ptr) }.cloned().unwrap_or_default();
                builder.append_value(encode_state(&state));
            }
            return Ok(Arc::new(builder.finish()));
        }

        let mut values = Vec::with_capacity(group_states.len());
        for &base in group_states {
            let ptr = unsafe { (base as *mut u8).add(offset) };
            let value = unsafe { get_state(ptr) }.map_or(Ok(None), |state| match &spec.kind {
                AggKind::PercentileCont => finalize_cont(state, output_type),
                AggKind::PercentileDisc | AggKind::PercentileDiscLc => finalize_disc(state),
                other => Err(format!("unexpected percentile placeholder kind: {:?}", other)),
            })?;
            values.push(value);
        }
        build_scalar_array(output_type, values)
    }
}
