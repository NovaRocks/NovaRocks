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
use std::cmp::Ordering;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, LargeBinaryArray, LargeStringArray, StringArray,
    StructArray,
};
use arrow::datatypes::{DataType, Field};
use arrow_buffer::i256;

use crate::exec::node::aggregate::AggFunction;

use super::super::*;
use super::AggregateFunction;
use super::common::{AggScalarValue, build_scalar_array, compare_scalar_values, scalar_from_array};

pub(super) struct MinMaxNAgg;

#[derive(Clone, Debug, Default)]
struct MinMaxNState {
    initialized: bool,
    limit: usize,
    values: Vec<AggScalarValue>,
}

impl AggregateFunction for MinMaxNAgg {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        input_type: Option<&DataType>,
        input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        let input_type = input_type.ok_or_else(|| "min_n/max_n input type missing".to_string())?;
        let kind = match canonical_agg_name(func.name.as_str()) {
            "min_n" => AggKind::MinN,
            "max_n" => AggKind::MaxN,
            other => return Err(format!("unsupported min/max_n function: {}", other)),
        };

        let output_type = func
            .types
            .as_ref()
            .and_then(|sig| sig.output_type.clone())
            .unwrap_or_else(|| default_output_type(input_type, input_is_intermediate));
        let intermediate_type = func
            .types
            .as_ref()
            .and_then(|sig| sig.intermediate_type.clone())
            .unwrap_or(DataType::Binary);

        if input_is_intermediate {
            if !matches!(
                input_type,
                DataType::Binary | DataType::Utf8 | DataType::LargeBinary | DataType::LargeUtf8
            ) {
                return Err(format!(
                    "min_n/max_n merge input must be binary-like, got {:?}",
                    input_type
                ));
            }
        } else if !matches!(input_type, DataType::Struct(fields) if fields.len() >= 2) {
            return Err(format!(
                "min_n/max_n expects packed STRUCT(value, n), got {:?}",
                input_type
            ));
        }

        Ok(AggSpec {
            kind,
            output_type,
            intermediate_type,
            input_arg_type: func
                .types
                .as_ref()
                .and_then(|sig| sig.input_arg_type.clone()),
            count_all: false,
        })
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        match kind {
            AggKind::MinN | AggKind::MaxN => (
                std::mem::size_of::<MinMaxNState>(),
                std::mem::align_of::<MinMaxNState>(),
            ),
            other => unreachable!("unexpected kind for min/max_n: {:?}", other),
        }
    }

    fn build_input_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "min_n/max_n input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn build_merge_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "min_n/max_n merge input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn init_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::write(ptr as *mut MinMaxNState, MinMaxNState::default());
        }
    }

    fn drop_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::drop_in_place(ptr as *mut MinMaxNState);
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
            return Err("min_n/max_n input type mismatch".to_string());
        };
        let struct_array = array
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| "min_n/max_n input must be StructArray".to_string())?;
        if struct_array.num_columns() < 2 {
            return Err(format!(
                "min_n/max_n input expects at least 2 fields, got {}",
                struct_array.num_columns()
            ));
        }
        let values = struct_array.column(0).clone();
        let limits = struct_array.column(1).clone();
        let keep_smallest = matches!(spec.kind, AggKind::MinN);

        for (row, &base) in state_ptrs.iter().enumerate() {
            let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut MinMaxNState) };
            let limit = parse_limit(&limits, row)?;
            init_limit_if_needed(state, limit)?;
            let Some(value) = scalar_from_array(&values, row)? else {
                continue;
            };
            push_value(state, value, keep_smallest)?;
        }
        Ok(())
    }

    fn merge_batch(
        &self,
        spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        let AggInputView::Any(array) = input else {
            return Err("min_n/max_n merge input type mismatch".to_string());
        };
        let keep_smallest = matches!(spec.kind, AggKind::MinN);
        for (row, &base) in state_ptrs.iter().enumerate() {
            let Some((limit, values)) = decode_payload_at(array, row)? else {
                continue;
            };
            if limit == 0 {
                continue;
            }
            let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut MinMaxNState) };
            init_limit_if_needed(state, limit)?;
            for value in values {
                push_value(state, value, keep_smallest)?;
            }
        }
        Ok(())
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

        match target_type {
            DataType::Binary => {
                let mut builder = BinaryBuilder::new();
                for &base in group_states {
                    let state = unsafe { &*((base as *mut u8).add(offset) as *const MinMaxNState) };
                    builder.append_value(encode_state(state)?);
                }
                Ok(Arc::new(builder.finish()) as ArrayRef)
            }
            DataType::List(_) => {
                let mut values = Vec::with_capacity(group_states.len());
                for &base in group_states {
                    let state = unsafe { &*((base as *mut u8).add(offset) as *const MinMaxNState) };
                    let list = state.values.iter().cloned().map(Some).collect::<Vec<_>>();
                    values.push(Some(AggScalarValue::List(list)));
                }
                build_scalar_array(target_type, values)
            }
            other => Err(format!(
                "min_n/max_n output type must be Binary or List, got {:?}",
                other
            )),
        }
    }
}

fn canonical_agg_name(name: &str) -> &str {
    name.split_once('|').map(|(base, _)| base).unwrap_or(name)
}

fn default_output_type(input_type: &DataType, input_is_intermediate: bool) -> DataType {
    if input_is_intermediate {
        return DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
    }
    if let DataType::Struct(fields) = input_type {
        if let Some(first) = fields.first() {
            return DataType::List(Arc::new(Field::new(
                "item",
                first.data_type().clone(),
                true,
            )));
        }
    }
    DataType::List(Arc::new(Field::new("item", input_type.clone(), true)))
}

fn parse_limit(array: &ArrayRef, row: usize) -> Result<usize, String> {
    let Some(value) = scalar_from_array(array, row)? else {
        return Err("min_n/max_n limit cannot be null".to_string());
    };
    let AggScalarValue::Int64(value) = value else {
        return Err("min_n/max_n limit must be integer".to_string());
    };
    if value <= 0 {
        return Err(format!("min_n/max_n limit must be positive, got {}", value));
    }
    usize::try_from(value).map_err(|_| "min_n/max_n limit overflow".to_string())
}

fn init_limit_if_needed(state: &mut MinMaxNState, limit: usize) -> Result<(), String> {
    if !state.initialized {
        state.initialized = true;
        state.limit = limit;
        return Ok(());
    }
    if state.limit != limit {
        return Err(format!(
            "min_n/max_n limit mismatch while merging states: {} vs {}",
            state.limit, limit
        ));
    }
    Ok(())
}

fn push_value(
    state: &mut MinMaxNState,
    value: AggScalarValue,
    keep_smallest: bool,
) -> Result<(), String> {
    if state.limit == 0 {
        return Ok(());
    }
    state.values.push(value);
    state
        .values
        .sort_by(|left, right| compare_scalar_values(left, right).unwrap_or(Ordering::Equal));
    if !keep_smallest {
        state.values.reverse();
    }
    if state.values.len() > state.limit {
        state.values.truncate(state.limit);
    }
    state
        .values
        .sort_by(|left, right| compare_scalar_values(left, right).unwrap_or(Ordering::Equal));
    Ok(())
}

fn encode_state(state: &MinMaxNState) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    out.extend_from_slice(&(state.limit as u32).to_le_bytes());
    out.extend_from_slice(&(state.values.len() as u32).to_le_bytes());
    for value in &state.values {
        encode_scalar(&mut out, value)?;
    }
    Ok(out)
}

fn decode_payload_at(
    array: &ArrayRef,
    row: usize,
) -> Result<Option<(usize, Vec<AggScalarValue>)>, String> {
    let payload = match array.data_type() {
        DataType::Binary => {
            let array = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| "min_n/max_n merge input must be BinaryArray".to_string())?;
            (!array.is_null(row)).then_some(array.value(row))
        }
        DataType::Utf8 => {
            let array = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "min_n/max_n merge input must be StringArray".to_string())?;
            (!array.is_null(row)).then_some(array.value(row).as_bytes())
        }
        DataType::LargeBinary => {
            let array = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| "min_n/max_n merge input must be LargeBinaryArray".to_string())?;
            (!array.is_null(row)).then_some(array.value(row))
        }
        DataType::LargeUtf8 => {
            let array = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| "min_n/max_n merge input must be LargeStringArray".to_string())?;
            (!array.is_null(row)).then_some(array.value(row).as_bytes())
        }
        other => {
            return Err(format!(
                "min_n/max_n merge input must be binary-like, got {:?}",
                other
            ));
        }
    };
    let Some(payload) = payload else {
        return Ok(None);
    };

    let mut pos = 0usize;
    let limit = read_u32(payload, &mut pos, "limit")? as usize;
    let count = read_u32(payload, &mut pos, "count")? as usize;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        values.push(decode_scalar(payload, &mut pos)?);
    }
    Ok(Some((limit, values)))
}

fn read_u32(bytes: &[u8], pos: &mut usize, label: &str) -> Result<u32, String> {
    need_len(bytes, *pos, 4, label)?;
    let value = u32::from_le_bytes(
        bytes[*pos..*pos + 4]
            .try_into()
            .map_err(|_| format!("min_n/max_n decode {} failed", label))?,
    );
    *pos += 4;
    Ok(value)
}

fn need_len(bytes: &[u8], pos: usize, need: usize, label: &str) -> Result<(), String> {
    if pos + need > bytes.len() {
        return Err(format!(
            "min_n/max_n decode {} overflow: pos={} need={} len={}",
            label,
            pos,
            need,
            bytes.len()
        ));
    }
    Ok(())
}

fn encode_scalar(out: &mut Vec<u8>, value: &AggScalarValue) -> Result<(), String> {
    match value {
        AggScalarValue::Bool(v) => {
            out.push(1);
            out.push(if *v { 1 } else { 0 });
        }
        AggScalarValue::Int64(v) => {
            out.push(2);
            out.extend_from_slice(&v.to_le_bytes());
        }
        AggScalarValue::Float64(v) => {
            out.push(3);
            out.extend_from_slice(&v.to_bits().to_le_bytes());
        }
        AggScalarValue::Utf8(v) => {
            out.push(4);
            let len = u32::try_from(v.len())
                .map_err(|_| "min_n/max_n utf8 length overflow".to_string())?;
            out.extend_from_slice(&len.to_le_bytes());
            out.extend_from_slice(v.as_bytes());
        }
        AggScalarValue::Date32(v) => {
            out.push(5);
            out.extend_from_slice(&v.to_le_bytes());
        }
        AggScalarValue::Timestamp(v) => {
            out.push(6);
            out.extend_from_slice(&v.to_le_bytes());
        }
        AggScalarValue::Decimal128(v) => {
            out.push(7);
            out.extend_from_slice(&v.to_le_bytes());
        }
        AggScalarValue::Decimal256(v) => {
            out.push(11);
            let text = v.to_string();
            let len = u32::try_from(text.len())
                .map_err(|_| "min_n/max_n decimal256 length overflow".to_string())?;
            out.extend_from_slice(&len.to_le_bytes());
            out.extend_from_slice(text.as_bytes());
        }
        other => {
            return Err(format!(
                "min_n/max_n does not support serialized value {:?}",
                other
            ));
        }
    }
    Ok(())
}

fn decode_scalar(bytes: &[u8], pos: &mut usize) -> Result<AggScalarValue, String> {
    need_len(bytes, *pos, 1, "tag")?;
    let tag = bytes[*pos];
    *pos += 1;
    match tag {
        1 => {
            need_len(bytes, *pos, 1, "bool")?;
            let value = bytes[*pos] != 0;
            *pos += 1;
            Ok(AggScalarValue::Bool(value))
        }
        2 => {
            need_len(bytes, *pos, 8, "int64")?;
            let value = i64::from_le_bytes(
                bytes[*pos..*pos + 8]
                    .try_into()
                    .map_err(|_| "min_n/max_n int64 decode failed".to_string())?,
            );
            *pos += 8;
            Ok(AggScalarValue::Int64(value))
        }
        3 => {
            need_len(bytes, *pos, 8, "float64")?;
            let bits = u64::from_le_bytes(
                bytes[*pos..*pos + 8]
                    .try_into()
                    .map_err(|_| "min_n/max_n float64 decode failed".to_string())?,
            );
            *pos += 8;
            Ok(AggScalarValue::Float64(f64::from_bits(bits)))
        }
        4 => {
            let len = read_u32(bytes, pos, "utf8_len")? as usize;
            need_len(bytes, *pos, len, "utf8")?;
            let value = std::str::from_utf8(&bytes[*pos..*pos + len])
                .map_err(|e| format!("min_n/max_n utf8 decode failed: {}", e))?
                .to_string();
            *pos += len;
            Ok(AggScalarValue::Utf8(value))
        }
        5 => {
            need_len(bytes, *pos, 4, "date32")?;
            let value = i32::from_le_bytes(
                bytes[*pos..*pos + 4]
                    .try_into()
                    .map_err(|_| "min_n/max_n date32 decode failed".to_string())?,
            );
            *pos += 4;
            Ok(AggScalarValue::Date32(value))
        }
        6 => {
            need_len(bytes, *pos, 8, "timestamp")?;
            let value = i64::from_le_bytes(
                bytes[*pos..*pos + 8]
                    .try_into()
                    .map_err(|_| "min_n/max_n timestamp decode failed".to_string())?,
            );
            *pos += 8;
            Ok(AggScalarValue::Timestamp(value))
        }
        7 => {
            need_len(bytes, *pos, 16, "decimal128")?;
            let value = i128::from_le_bytes(
                bytes[*pos..*pos + 16]
                    .try_into()
                    .map_err(|_| "min_n/max_n decimal128 decode failed".to_string())?,
            );
            *pos += 16;
            Ok(AggScalarValue::Decimal128(value))
        }
        11 => {
            let len = read_u32(bytes, pos, "decimal256_len")? as usize;
            need_len(bytes, *pos, len, "decimal256")?;
            let text = std::str::from_utf8(&bytes[*pos..*pos + len])
                .map_err(|e| format!("min_n/max_n decimal256 decode failed: {}", e))?;
            *pos += len;
            let value = text
                .parse::<i256>()
                .map_err(|_| "min_n/max_n decimal256 parse failed".to_string())?;
            Ok(AggScalarValue::Decimal256(value))
        }
        other => Err(format!("min_n/max_n decode unknown tag {}", other)),
    }
}
