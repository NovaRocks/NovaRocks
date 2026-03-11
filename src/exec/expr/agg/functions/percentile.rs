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

use arrow::array::{Array, ArrayRef, BinaryBuilder, StructArray};
use arrow::datatypes::DataType;
use std::sync::Arc;

use crate::common::percentile;
use crate::exec::expr::agg::functions::common::{
    AggScalarValue, build_scalar_array, scalar_from_array,
};
use crate::exec::expr::function::object::percentile_functions::{
    numeric_value_at, payload_bytes_at,
};
use crate::exec::node::aggregate::AggFunction;

use super::super::*;
use super::AggregateFunction;

pub(super) struct PercentileAgg;

fn unweighted_state_slot(ptr: *mut u8) -> *mut *mut percentile::PercentileState {
    ptr as *mut *mut percentile::PercentileState
}

fn weighted_state_slot(ptr: *mut u8) -> *mut *mut percentile::PercentileState {
    ptr as *mut *mut percentile::PercentileState
}

unsafe fn get_or_init_unweighted<'a>(ptr: *mut u8) -> &'a mut percentile::PercentileState {
    let slot = unweighted_state_slot(ptr);
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

unsafe fn get_unweighted<'a>(ptr: *mut u8) -> Option<&'a percentile::PercentileState> {
    let raw = unsafe { *unweighted_state_slot(ptr) };
    if raw.is_null() {
        None
    } else {
        Some(unsafe { &*raw })
    }
}

unsafe fn take_unweighted(ptr: *mut u8) -> Option<Box<percentile::PercentileState>> {
    let slot = unweighted_state_slot(ptr);
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

unsafe fn get_or_init_weighted<'a>(ptr: *mut u8) -> &'a mut percentile::PercentileState {
    let slot = weighted_state_slot(ptr);
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

unsafe fn get_weighted<'a>(ptr: *mut u8) -> Option<&'a percentile::PercentileState> {
    let raw = unsafe { *weighted_state_slot(ptr) };
    if raw.is_null() {
        None
    } else {
        Some(unsafe { &*raw })
    }
}

unsafe fn take_weighted(ptr: *mut u8) -> Option<Box<percentile::PercentileState>> {
    let slot = weighted_state_slot(ptr);
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

fn integer_value_at(array: &ArrayRef, row: usize, context: &str) -> Result<Option<i64>, String> {
    match scalar_from_array(array, row)? {
        Some(AggScalarValue::Int64(v)) => Ok(Some(v)),
        Some(AggScalarValue::Float64(v)) => Ok(Some(v as i64)),
        Some(other) => Err(format!(
            "{context}: percentile weight expects numeric scalar, got {:?}",
            other
        )),
        None => Ok(None),
    }
}

fn validate_quantile(context: &str, quantile: f64) -> Result<(), String> {
    if !(0.0..=1.0).contains(&quantile) {
        return Err(format!(
            "{context}: percentile parameter must be between 0 and 1, got {}",
            quantile
        ));
    }
    Ok(())
}

fn apply_unweighted_quantiles(
    state: &mut percentile::PercentileState,
    array: &ArrayRef,
    row: usize,
    context: &str,
) -> Result<(), String> {
    match scalar_from_array(array, row)? {
        Some(AggScalarValue::Float64(v)) => {
            validate_quantile(context, v)?;
            percentile::set_quantile(state, v)
        }
        Some(AggScalarValue::Int64(v)) => {
            let quantile = v as f64;
            validate_quantile(context, quantile)?;
            percentile::set_quantile(state, quantile)
        }
        Some(AggScalarValue::List(values)) => {
            let mut quantiles = Vec::with_capacity(values.len());
            for (idx, item) in values.into_iter().enumerate() {
                let quantile = match item {
                    Some(AggScalarValue::Float64(v)) => v,
                    Some(AggScalarValue::Int64(v)) => v as f64,
                    None => {
                        return Err(format!(
                            "{context}: percentile array element[{idx}] cannot be null"
                        ));
                    }
                    Some(other) => {
                        return Err(format!(
                            "{context}: percentile array element[{idx}] must be numeric, got {:?}",
                            other
                        ));
                    }
                };
                validate_quantile(context, quantile)?;
                quantiles.push(quantile);
            }
            percentile::set_quantiles(state, quantiles)
        }
        Some(other) => Err(format!(
            "{context}: percentile expects numeric or numeric array, got {:?}",
            other
        )),
        None => Ok(()),
    }
}

fn apply_weighted_quantiles(
    state: &mut percentile::PercentileState,
    array: &ArrayRef,
    row: usize,
    context: &str,
) -> Result<(), String> {
    match scalar_from_array(array, row)? {
        Some(AggScalarValue::Float64(v)) => {
            validate_quantile(context, v)?;
            percentile::set_quantile(state, v)
        }
        Some(AggScalarValue::Int64(v)) => {
            let quantile = v as f64;
            validate_quantile(context, quantile)?;
            percentile::set_quantile(state, quantile)
        }
        Some(AggScalarValue::List(values)) => {
            let mut quantiles = Vec::with_capacity(values.len());
            for (idx, item) in values.into_iter().enumerate() {
                let quantile = match item {
                    Some(AggScalarValue::Float64(v)) => v,
                    Some(AggScalarValue::Int64(v)) => v as f64,
                    None => {
                        return Err(format!(
                            "{context}: percentile array element[{idx}] cannot be null"
                        ));
                    }
                    Some(other) => {
                        return Err(format!(
                            "{context}: percentile array element[{idx}] must be numeric, got {:?}",
                            other
                        ));
                    }
                };
                validate_quantile(context, quantile)?;
                quantiles.push(quantile);
            }
            percentile::set_quantiles(state, quantiles)
        }
        Some(other) => Err(format!(
            "{context}: percentile expects numeric or numeric array, got {:?}",
            other
        )),
        None => Ok(()),
    }
}

fn apply_compression_to_unweighted(
    state: &mut percentile::PercentileState,
    array: &ArrayRef,
    row: usize,
    context: &str,
) -> Result<(), String> {
    if let Some(value) = numeric_value_at(array, row, context)? {
        percentile::set_compression(state, value)?;
    }
    Ok(())
}

fn apply_compression_to_weighted(
    state: &mut percentile::PercentileState,
    array: &ArrayRef,
    row: usize,
    context: &str,
) -> Result<(), String> {
    if let Some(value) = numeric_value_at(array, row, context)? {
        percentile::set_compression(state, value)?;
    }
    Ok(())
}

fn merge_unweighted_payload_array(
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
        let state = unsafe { get_or_init_unweighted(ptr) };
        percentile::merge_serialized_state_into(state, payload)?;
    }
    Ok(())
}

fn merge_weighted_payload_array(
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
        let state = unsafe { get_or_init_weighted(ptr) };
        percentile::merge_serialized_state_into(state, payload)?;
    }
    Ok(())
}

fn update_unweighted_struct(
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
    let compression = if fields.len() >= 3 {
        Some(fields[2].clone())
    } else {
        None
    };

    for (row, &base) in state_ptrs.iter().enumerate() {
        let ptr = unsafe { (base as *mut u8).add(offset) };
        let state = unsafe { get_or_init_unweighted(ptr) };
        apply_unweighted_quantiles(state, &quantiles, row, context)?;
        if let Some(compression) = &compression {
            apply_compression_to_unweighted(state, compression, row, context)?;
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

fn update_weighted_struct(
    array: &StructArray,
    offset: usize,
    state_ptrs: &[AggStatePtr],
    context: &str,
) -> Result<(), String> {
    let fields = array.columns();
    if fields.len() < 3 {
        return Err(format!(
            "{context}: percentile_approx_weighted expects STRUCT(value, weight, quantile[, compression]) input"
        ));
    }
    let values = fields[0].clone();
    let weights = fields[1].clone();
    let quantiles = fields[2].clone();
    let compression = if fields.len() >= 4 {
        Some(fields[3].clone())
    } else {
        None
    };

    for (row, &base) in state_ptrs.iter().enumerate() {
        let ptr = unsafe { (base as *mut u8).add(offset) };
        let state = unsafe { get_or_init_weighted(ptr) };
        apply_weighted_quantiles(state, &quantiles, row, context)?;
        if let Some(compression) = &compression {
            apply_compression_to_weighted(state, compression, row, context)?;
        }
        let Some(value) = numeric_value_at(&values, row, context)? else {
            continue;
        };
        let weight = integer_value_at(&weights, row, context)?.unwrap_or_default();
        if weight < 0 {
            return Err(format!(
                "{context}: percentile weight must be non-negative, got {}",
                weight
            ));
        }
        percentile::add_weighted_value(state, value, weight)?;
    }

    Ok(())
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

        let kind = match canonical_agg_name(func.name.as_str()) {
            "percentile_union" => AggKind::PercentileUnion,
            "percentile_approx" => AggKind::PercentileApprox,
            "percentile_approx_weighted" => AggKind::PercentileApproxWeighted,
            other => {
                return Err(format!(
                    "unsupported percentile aggregate function: {}",
                    other
                ));
            }
        };

        Ok(AggSpec {
            kind,
            output_type: sig.output_type.clone().unwrap_or(DataType::Float64),
            intermediate_type: sig.intermediate_type.clone().unwrap_or(DataType::Binary),
            input_arg_type: input_type.cloned(),
            count_all: false,
        })
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        match kind {
            AggKind::PercentileUnion
            | AggKind::PercentileApprox
            | AggKind::PercentileApproxWeighted => (
                std::mem::size_of::<*mut u8>(),
                std::mem::align_of::<*mut u8>(),
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

    fn init_state(&self, spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            match &spec.kind {
                AggKind::PercentileApproxWeighted => std::ptr::write(
                    ptr as *mut *mut percentile::PercentileState,
                    std::ptr::null_mut(),
                ),
                _ => std::ptr::write(
                    ptr as *mut *mut percentile::PercentileState,
                    std::ptr::null_mut(),
                ),
            }
        }
    }

    fn drop_state(&self, spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            match &spec.kind {
                AggKind::PercentileApproxWeighted => {
                    let _ = take_weighted(ptr);
                }
                _ => {
                    let _ = take_unweighted(ptr);
                }
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
        let AggInputView::Any(array) = input else {
            return Err("percentile aggregate input type mismatch".to_string());
        };
        match &spec.kind {
            AggKind::PercentileApproxWeighted => {
                if let Some(struct_array) = array.as_any().downcast_ref::<StructArray>() {
                    update_weighted_struct(
                        struct_array,
                        offset,
                        state_ptrs,
                        "percentile_approx_weighted",
                    )
                } else {
                    merge_weighted_payload_array(
                        array,
                        offset,
                        state_ptrs,
                        "percentile_approx_weighted",
                    )
                }
            }
            AggKind::PercentileUnion | AggKind::PercentileApprox => {
                if let Some(struct_array) = array.as_any().downcast_ref::<StructArray>() {
                    update_unweighted_struct(struct_array, offset, state_ptrs, "percentile_approx")
                } else {
                    merge_unweighted_payload_array(array, offset, state_ptrs, "percentile_approx")
                }
            }
            other => Err(format!("unexpected percentile aggregate kind: {:?}", other)),
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
        match &spec.kind {
            AggKind::PercentileApproxWeighted => merge_weighted_payload_array(
                array,
                offset,
                state_ptrs,
                "percentile_approx_weighted_merge",
            ),
            _ => {
                merge_unweighted_payload_array(array, offset, state_ptrs, "percentile_approx_merge")
            }
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

        match &spec.kind {
            AggKind::PercentileApproxWeighted => match output_type {
                DataType::Binary => {
                    let mut builder = BinaryBuilder::new();
                    for &base in group_states {
                        let ptr = unsafe { (base as *mut u8).add(offset) };
                        let state = unsafe { get_weighted(ptr) }.cloned().unwrap_or_default();
                        builder.append_value(percentile::encode_state(&state));
                    }
                    Ok(Arc::new(builder.finish()))
                }
                DataType::Float64 => {
                    let mut values = Vec::with_capacity(group_states.len());
                    for &base in group_states {
                        let ptr = unsafe { (base as *mut u8).add(offset) };
                        let value = unsafe { get_weighted(ptr) }
                            .and_then(|state| percentile::quantile_from_state(state, None))
                            .map(AggScalarValue::Float64);
                        values.push(value);
                    }
                    build_scalar_array(output_type, values)
                }
                DataType::List(_) => {
                    let mut values = Vec::with_capacity(group_states.len());
                    for &base in group_states {
                        let ptr = unsafe { (base as *mut u8).add(offset) };
                        let value = unsafe { get_weighted(ptr) }.and_then(|state| {
                            percentile::quantiles_from_state(state).map(|items| {
                                AggScalarValue::List(
                                    items
                                        .into_iter()
                                        .map(|item| Some(AggScalarValue::Float64(item)))
                                        .collect(),
                                )
                            })
                        });
                        values.push(value);
                    }
                    build_scalar_array(output_type, values)
                }
                other => Err(format!(
                    "weighted percentile aggregate output type must be Binary/Float64/List, got {:?}",
                    other
                )),
            },
            _ => match output_type {
                DataType::Binary => {
                    let mut builder = BinaryBuilder::new();
                    for &base in group_states {
                        let ptr = unsafe { (base as *mut u8).add(offset) };
                        let state = unsafe { get_unweighted(ptr) }.cloned().unwrap_or_default();
                        builder.append_value(percentile::encode_state(&state));
                    }
                    Ok(Arc::new(builder.finish()))
                }
                DataType::Float64 => {
                    let mut values = Vec::with_capacity(group_states.len());
                    for &base in group_states {
                        let ptr = unsafe { (base as *mut u8).add(offset) };
                        let value = unsafe { get_unweighted(ptr) }
                            .and_then(|state| percentile::quantile_from_state(state, None))
                            .map(AggScalarValue::Float64);
                        values.push(value);
                    }
                    build_scalar_array(output_type, values)
                }
                DataType::List(_) => {
                    let mut values = Vec::with_capacity(group_states.len());
                    for &base in group_states {
                        let ptr = unsafe { (base as *mut u8).add(offset) };
                        let value = unsafe { get_unweighted(ptr) }.and_then(|state| {
                            percentile::quantiles_from_state(state).map(|items| {
                                AggScalarValue::List(
                                    items
                                        .into_iter()
                                        .map(|item| Some(AggScalarValue::Float64(item)))
                                        .collect(),
                                )
                            })
                        });
                        values.push(value);
                    }
                    build_scalar_array(output_type, values)
                }
                other => Err(format!(
                    "percentile aggregate output type must be Binary/Float64/List, got {:?}",
                    other
                )),
            },
        }
    }
}
