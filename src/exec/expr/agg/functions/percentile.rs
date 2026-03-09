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
use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, Decimal128Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Float64Builder, Int8Array, Int16Array, Int32Array, Int64Array,
    LargeBinaryArray, LargeStringArray, StringArray, StructArray,
};
use arrow::datatypes::DataType;
use std::sync::Arc;

use crate::common::{largeint, percentile};
use crate::exec::expr::function::object::percentile_functions::{
    numeric_value_at, payload_bytes_at,
};
use crate::exec::node::aggregate::AggFunction;

use super::super::*;
use super::AggregateFunction;

pub(super) struct PercentileAgg;

fn state_slot(ptr: *mut u8) -> *mut *mut percentile::PercentileState {
    ptr as *mut *mut percentile::PercentileState
}

unsafe fn get_or_init_state<'a>(ptr: *mut u8) -> &'a mut percentile::PercentileState {
    let slot = state_slot(ptr);
    let raw = unsafe { *slot };
    if raw.is_null() {
        let boxed: Box<percentile::PercentileState> = Box::default();
        let raw = Box::into_raw(boxed);
        unsafe {
            *slot = raw;
            &mut *raw
        }
    } else {
        unsafe { &mut *raw }
    }
}

unsafe fn get_state<'a>(ptr: *mut u8) -> Option<&'a percentile::PercentileState> {
    let raw = unsafe { *state_slot(ptr) };
    if raw.is_null() {
        None
    } else {
        Some(unsafe { &*raw })
    }
}

unsafe fn take_state(ptr: *mut u8) -> Option<Box<percentile::PercentileState>> {
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
        let ptr = unsafe { (base as *mut u8).add(offset) };
        let state = unsafe { get_or_init_state(ptr) };
        percentile::merge_serialized_state_into(state, payload)?;
    }
    Ok(())
}

fn merge_numeric_array(
    array: &ArrayRef,
    offset: usize,
    state_ptrs: &[AggStatePtr],
    context: &str,
) -> Result<(), String> {
    for (row, &base) in state_ptrs.iter().enumerate() {
        let Some(value) = numeric_value_at(array, row, context)? else {
            continue;
        };
        let ptr = unsafe { (base as *mut u8).add(offset) };
        let state = unsafe { get_or_init_state(ptr) };
        percentile::add_value(state, value);
    }
    Ok(())
}

fn merge_struct_array(
    array: &StructArray,
    offset: usize,
    state_ptrs: &[AggStatePtr],
    context: &str,
) -> Result<(), String> {
    let fields = array.columns();
    if fields.len() < 2 {
        return Err(format!(
            "{context}: percentile_approx expects STRUCT(value, quantile[, compression]) input"
        ));
    }
    let values = fields[0].clone();
    let quantiles = fields[1].clone();

    for (row, &base) in state_ptrs.iter().enumerate() {
        let ptr = unsafe { (base as *mut u8).add(offset) };
        let state = unsafe { get_or_init_state(ptr) };

        if let Some(quantile) = numeric_value_at(&quantiles, row, context)? {
            percentile::set_quantile(state, quantile)?;
        }

        match values.data_type() {
            DataType::Binary | DataType::Utf8 | DataType::LargeBinary | DataType::LargeUtf8 => {
                if let Some(payload) = payload_bytes_at(&values, row, context)? {
                    percentile::merge_serialized_state_into(state, payload)?;
                }
            }
            _ => {
                if let Some(value) = numeric_value_at(&values, row, context)? {
                    percentile::add_value(state, value);
                }
            }
        }
    }
    Ok(())
}

fn update_percentile_union(
    array: &ArrayRef,
    offset: usize,
    state_ptrs: &[AggStatePtr],
) -> Result<(), String> {
    merge_payload_array(array, offset, state_ptrs, "percentile_union")
}

fn update_percentile_approx(
    array: &ArrayRef,
    offset: usize,
    state_ptrs: &[AggStatePtr],
) -> Result<(), String> {
    if let Some(struct_array) = array.as_any().downcast_ref::<StructArray>() {
        return merge_struct_array(struct_array, offset, state_ptrs, "percentile_approx");
    }
    match array.data_type() {
        DataType::Binary | DataType::Utf8 | DataType::LargeBinary | DataType::LargeUtf8 => {
            merge_payload_array(array, offset, state_ptrs, "percentile_approx")
        }
        _ => merge_numeric_array(array, offset, state_ptrs, "percentile_approx"),
    }
}

impl AggregateFunction for PercentileAgg {
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

        match canonical_agg_name(func.name.as_str()) {
            "percentile_union" => Ok(AggSpec {
                kind: AggKind::PercentileUnion,
                output_type: sig.output_type.clone().unwrap_or(DataType::Binary),
                intermediate_type: sig.intermediate_type.clone().unwrap_or(DataType::Binary),
                input_arg_type: input_type.cloned(),
                count_all: false,
            }),
            "percentile_approx" => Ok(AggSpec {
                kind: AggKind::PercentileApprox,
                output_type: sig.output_type.clone().unwrap_or(DataType::Float64),
                intermediate_type: sig.intermediate_type.clone().unwrap_or(DataType::Binary),
                input_arg_type: input_type.cloned(),
                count_all: false,
            }),
            other => Err(format!(
                "unsupported percentile aggregate function: {}",
                other
            )),
        }
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        match kind {
            AggKind::PercentileUnion | AggKind::PercentileApprox => (
                std::mem::size_of::<*mut percentile::PercentileState>(),
                std::mem::align_of::<*mut percentile::PercentileState>(),
            ),
            other => unreachable!("unexpected percentile agg kind: {:?}", other),
        }
    }

    fn build_input_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "percentile aggregate input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn build_merge_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "percentile aggregate merge input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn init_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::write(
                ptr as *mut *mut percentile::PercentileState,
                std::ptr::null_mut(),
            );
        }
    }

    fn drop_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            let _ = take_state(ptr);
        }
    }

    fn update_batch(
        &self,
        spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        let AggInputView::Any(array) = input else {
            return Err("percentile aggregate input type mismatch".to_string());
        };
        match spec.kind {
            AggKind::PercentileUnion => update_percentile_union(array, offset, state_ptrs),
            AggKind::PercentileApprox => update_percentile_approx(array, offset, state_ptrs),
            _ => Err(format!(
                "unexpected percentile aggregate kind: {:?}",
                spec.kind
            )),
        }
    }

    fn merge_batch(
        &self,
        spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        let AggInputView::Any(array) = input else {
            return Err("percentile aggregate merge input type mismatch".to_string());
        };
        match spec.kind {
            AggKind::PercentileUnion => update_percentile_union(array, offset, state_ptrs),
            AggKind::PercentileApprox => update_percentile_approx(array, offset, state_ptrs),
            _ => Err(format!(
                "unexpected percentile aggregate kind: {:?}",
                spec.kind
            )),
        }
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
                for &base in group_states {
                    let ptr = unsafe { (base as *mut u8).add(offset) };
                    let state = unsafe { get_state(ptr) }.cloned().unwrap_or_default();
                    builder.append_value(percentile::encode_state(&state));
                }
                Ok(Arc::new(builder.finish()) as ArrayRef)
            }
            DataType::Float64 => {
                let mut builder = Float64Builder::with_capacity(group_states.len());
                for &base in group_states {
                    let ptr = unsafe { (base as *mut u8).add(offset) };
                    let state = unsafe { get_state(ptr) };
                    if let Some(value) =
                        state.and_then(|s| percentile::quantile_from_state(s, None))
                    {
                        builder.append_value(value);
                    } else {
                        builder.append_null();
                    }
                }
                Ok(Arc::new(builder.finish()) as ArrayRef)
            }
            other => Err(format!(
                "percentile aggregate output type must be Binary/Float64, got {:?}",
                other
            )),
        }
    }
}

fn canonical_agg_name(name: &str) -> &str {
    name.split_once('|').map(|(base, _)| base).unwrap_or(name)
}
