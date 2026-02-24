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
use arrow::array::{ArrayRef, BinaryArray, BinaryBuilder, StructArray};
use arrow::datatypes::DataType;
use arrow_buffer::i256;

use crate::exec::node::aggregate::AggFunction;

use super::super::*;
use super::AggregateFunction;
use super::common::{AggScalarValue, build_scalar_array, compare_scalar_values, scalar_from_array};

pub(super) struct MaxMinByAgg;

#[derive(Clone, Debug)]
struct MaxMinByState {
    has_key: bool,
    key: AggScalarValue,
    value: Option<AggScalarValue>,
}

impl Default for MaxMinByState {
    fn default() -> Self {
        Self {
            has_key: false,
            key: AggScalarValue::Int64(0),
            value: None,
        }
    }
}

fn is_max_kind(kind: &AggKind) -> bool {
    matches!(kind, AggKind::MaxBy | AggKind::MaxByV2)
}

fn allow_null_value(kind: &AggKind) -> bool {
    matches!(kind, AggKind::MaxByV2 | AggKind::MinByV2)
}

fn kind_from_name(name: &str) -> Option<AggKind> {
    match name {
        "max_by" => Some(AggKind::MaxBy),
        "max_by_v2" => Some(AggKind::MaxByV2),
        "min_by" => Some(AggKind::MinBy),
        "min_by_v2" => Some(AggKind::MinByV2),
        _ => None,
    }
}

fn encode_scalar(value: &Option<AggScalarValue>, buf: &mut Vec<u8>) -> Result<(), String> {
    match value {
        None => {
            buf.push(0);
            Ok(())
        }
        Some(AggScalarValue::Bool(v)) => {
            buf.push(1);
            buf.push(if *v { 1 } else { 0 });
            Ok(())
        }
        Some(AggScalarValue::Int64(v)) => {
            buf.push(2);
            buf.extend_from_slice(&v.to_le_bytes());
            Ok(())
        }
        Some(AggScalarValue::Float64(v)) => {
            buf.push(3);
            buf.extend_from_slice(&v.to_le_bytes());
            Ok(())
        }
        Some(AggScalarValue::Utf8(v)) => {
            buf.push(4);
            let len = u32::try_from(v.len()).map_err(|_| "string too large".to_string())?;
            buf.extend_from_slice(&len.to_le_bytes());
            buf.extend_from_slice(v.as_bytes());
            Ok(())
        }
        Some(AggScalarValue::Date32(v)) => {
            buf.push(5);
            buf.extend_from_slice(&v.to_le_bytes());
            Ok(())
        }
        Some(AggScalarValue::Timestamp(v)) => {
            buf.push(6);
            buf.extend_from_slice(&v.to_le_bytes());
            Ok(())
        }
        Some(AggScalarValue::Decimal128(v)) => {
            buf.push(7);
            buf.extend_from_slice(&v.to_le_bytes());
            Ok(())
        }
        Some(AggScalarValue::Decimal256(v)) => {
            buf.push(11);
            let text = v.to_string();
            let len = u32::try_from(text.len()).map_err(|_| "string too large".to_string())?;
            buf.extend_from_slice(&len.to_le_bytes());
            buf.extend_from_slice(text.as_bytes());
            Ok(())
        }
        Some(AggScalarValue::Struct(_))
        | Some(AggScalarValue::Map(_))
        | Some(AggScalarValue::List(_)) => {
            Err("max_by/min_by does not support complex scalar keys".to_string())
        }
    }
}

fn decode_scalar(input: &mut &[u8]) -> Result<Option<AggScalarValue>, String> {
    if input.is_empty() {
        return Err("max_by/min_by scalar decode failed: empty buffer".to_string());
    }
    let tag = input[0];
    *input = &input[1..];
    match tag {
        0 => Ok(None),
        1 => {
            if input.is_empty() {
                return Err("max_by/min_by bool decode failed".to_string());
            }
            let v = input[0] != 0;
            *input = &input[1..];
            Ok(Some(AggScalarValue::Bool(v)))
        }
        2 => {
            if input.len() < 8 {
                return Err("max_by/min_by int64 decode failed".to_string());
            }
            let v = i64::from_le_bytes(input[..8].try_into().unwrap());
            *input = &input[8..];
            Ok(Some(AggScalarValue::Int64(v)))
        }
        3 => {
            if input.len() < 8 {
                return Err("max_by/min_by float64 decode failed".to_string());
            }
            let v = f64::from_le_bytes(input[..8].try_into().unwrap());
            *input = &input[8..];
            Ok(Some(AggScalarValue::Float64(v)))
        }
        4 => {
            if input.len() < 4 {
                return Err("max_by/min_by utf8 decode failed".to_string());
            }
            let len = u32::from_le_bytes(input[..4].try_into().unwrap()) as usize;
            *input = &input[4..];
            if input.len() < len {
                return Err("max_by/min_by utf8 decode failed".to_string());
            }
            let v = std::str::from_utf8(&input[..len])
                .map_err(|e| e.to_string())?
                .to_string();
            *input = &input[len..];
            Ok(Some(AggScalarValue::Utf8(v)))
        }
        5 => {
            if input.len() < 4 {
                return Err("max_by/min_by date32 decode failed".to_string());
            }
            let v = i32::from_le_bytes(input[..4].try_into().unwrap());
            *input = &input[4..];
            Ok(Some(AggScalarValue::Date32(v)))
        }
        6 => {
            if input.len() < 8 {
                return Err("max_by/min_by timestamp decode failed".to_string());
            }
            let v = i64::from_le_bytes(input[..8].try_into().unwrap());
            *input = &input[8..];
            Ok(Some(AggScalarValue::Timestamp(v)))
        }
        7 => {
            if input.len() < 16 {
                return Err("max_by/min_by decimal128 decode failed".to_string());
            }
            let v = i128::from_le_bytes(input[..16].try_into().unwrap());
            *input = &input[16..];
            Ok(Some(AggScalarValue::Decimal128(v)))
        }
        11 => {
            if input.len() < 4 {
                return Err("max_by/min_by decimal256 decode failed".to_string());
            }
            let len = u32::from_le_bytes(input[..4].try_into().unwrap()) as usize;
            *input = &input[4..];
            if input.len() < len {
                return Err("max_by/min_by decimal256 decode failed".to_string());
            }
            let text = std::str::from_utf8(&input[..len]).map_err(|e| e.to_string())?;
            *input = &input[len..];
            let v = text
                .parse::<i256>()
                .map_err(|_| "max_by/min_by decimal256 decode failed".to_string())?;
            Ok(Some(AggScalarValue::Decimal256(v)))
        }
        _ => Err("max_by/min_by scalar decode failed: unknown tag".to_string()),
    }
}

impl AggregateFunction for MaxMinByAgg {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        input_type: Option<&DataType>,
        input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        let kind = kind_from_name(func.name.as_str())
            .ok_or_else(|| format!("unsupported max_by/min_by: {}", func.name))?;
        let data_type = input_type.ok_or_else(|| "max_by/min_by input type missing".to_string())?;

        if input_is_intermediate {
            let sig = super::super::agg_type_signature(func)
                .ok_or_else(|| "max_by/min_by type signature missing".to_string())?;
            let output_type = sig
                .output_type
                .as_ref()
                .ok_or_else(|| "max_by/min_by output type signature missing".to_string())?;
            return Ok(AggSpec {
                kind,
                output_type: output_type.clone(),
                intermediate_type: data_type.clone(),
                input_arg_type: sig.input_arg_type.clone(),
                count_all: false,
            });
        }

        match data_type {
            DataType::Struct(fields) => {
                if fields.len() != 2 {
                    return Err("max_by/min_by expects 2 arguments".to_string());
                }
                let value_type = fields[0].data_type().clone();
                Ok(AggSpec {
                    kind,
                    output_type: value_type,
                    intermediate_type: DataType::Binary,
                    input_arg_type: None,
                    count_all: false,
                })
            }
            other => Err(format!(
                "max_by/min_by expects struct input, got {:?}",
                other
            )),
        }
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        match kind {
            AggKind::MaxBy | AggKind::MinBy | AggKind::MaxByV2 | AggKind::MinByV2 => (
                std::mem::size_of::<MaxMinByState>(),
                std::mem::align_of::<MaxMinByState>(),
            ),
            other => unreachable!("unexpected kind for max_by/min_by: {:?}", other),
        }
    }

    fn build_input_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "max_by/min_by input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn build_merge_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "max_by/min_by merge input missing".to_string())?;
        let bin = arr
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| "failed to downcast to BinaryArray".to_string())?;
        Ok(AggInputView::Binary(bin))
    }

    fn init_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::write(ptr as *mut MaxMinByState, MaxMinByState::default());
        }
    }

    fn drop_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::drop_in_place(ptr as *mut MaxMinByState);
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
            return Err("max_by/min_by batch input type mismatch".to_string());
        };
        let struct_arr = array
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| "max_by/min_by expects struct input".to_string())?;
        if struct_arr.num_columns() != 2 {
            return Err("max_by/min_by expects 2 arguments".to_string());
        }
        let value_arr = struct_arr.column(0);
        let key_arr = struct_arr.column(1);
        let is_max = is_max_kind(&spec.kind);
        let allow_null = allow_null_value(&spec.kind);

        for (row, &base) in state_ptrs.iter().enumerate() {
            let key = scalar_from_array(key_arr, row)?;
            let Some(key) = key else { continue };
            let value = scalar_from_array(value_arr, row)?;
            if value.is_none() && !allow_null {
                continue;
            }

            let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut MaxMinByState) };
            let should_update = if !state.has_key {
                true
            } else {
                let ordering = compare_scalar_values(&key, &state.key)?;
                if is_max {
                    ordering == std::cmp::Ordering::Greater
                } else {
                    ordering == std::cmp::Ordering::Less
                }
            };
            if should_update {
                state.has_key = true;
                state.key = key;
                state.value = value;
            }
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
        let AggInputView::Binary(arr) = input else {
            return Err("max_by/min_by merge input type mismatch".to_string());
        };
        let is_max = is_max_kind(&spec.kind);
        let allow_null = allow_null_value(&spec.kind);
        for (row, &base) in state_ptrs.iter().enumerate() {
            if arr.is_null(row) {
                continue;
            }
            let bytes = arr.value(row);
            let mut slice = bytes;
            let key = decode_scalar(&mut slice)?
                .ok_or_else(|| "max_by/min_by merge missing key".to_string())?;
            let value = decode_scalar(&mut slice)?;
            if value.is_none() && !allow_null {
                continue;
            }
            let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut MaxMinByState) };
            let should_update = if !state.has_key {
                true
            } else {
                let ordering = compare_scalar_values(&key, &state.key)?;
                if is_max {
                    ordering == std::cmp::Ordering::Greater
                } else {
                    ordering == std::cmp::Ordering::Less
                }
            };
            if should_update {
                state.has_key = true;
                state.key = key;
                state.value = value;
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
        if output_intermediate {
            let mut builder = BinaryBuilder::new();
            for &base in group_states {
                let state = unsafe { &*((base as *mut u8).add(offset) as *const MaxMinByState) };
                if !state.has_key {
                    builder.append_null();
                    continue;
                }
                let mut buf = Vec::new();
                encode_scalar(&Some(state.key.clone()), &mut buf)?;
                encode_scalar(&state.value, &mut buf)?;
                builder.append_value(&buf);
            }
            return Ok(std::sync::Arc::new(builder.finish()));
        }

        let mut values = Vec::with_capacity(group_states.len());
        for &base in group_states {
            let state = unsafe { &*((base as *mut u8).add(offset) as *const MaxMinByState) };
            if state.has_key {
                values.push(state.value.clone());
            } else {
                values.push(None);
            }
        }
        build_scalar_array(&spec.output_type, values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray, StructArray};
    use arrow::datatypes::{DataType, Field, Fields};
    use std::mem::MaybeUninit;

    #[test]
    fn test_max_by_spec() {
        let func = AggFunction {
            name: "max_by".to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(crate::exec::node::aggregate::AggTypeSignature {
                intermediate_type: Some(DataType::Binary),
                output_type: Some(DataType::Int64),
                input_arg_type: None,
            }),
        };
        let struct_type = DataType::Struct(
            vec![
                Field::new("v", DataType::Int64, true),
                Field::new("k", DataType::Int64, true),
            ]
            .into(),
        );
        let spec = MaxMinByAgg
            .build_spec_from_type(&func, Some(&struct_type), false)
            .unwrap();
        assert!(matches!(spec.kind, AggKind::MaxBy));
    }

    #[test]
    fn test_max_min_by_variants() {
        let values = Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef;
        let keys = Arc::new(Int64Array::from(vec![1, 3, 2])) as ArrayRef;
        let fields = vec![
            Field::new("v", DataType::Utf8, true),
            Field::new("k", DataType::Int64, true),
        ];
        let struct_type = DataType::Struct(Fields::from(fields.clone()));
        let struct_arr = StructArray::new(fields.into(), vec![values, keys], None);
        let array_ref = Arc::new(struct_arr) as ArrayRef;
        let input = AggInputView::Any(&array_ref);

        for (name, expected) in [
            ("max_by", "b"),
            ("min_by", "a"),
            ("max_by_v2", "b"),
            ("min_by_v2", "a"),
        ] {
            let func = AggFunction {
                name: name.to_string(),
                inputs: vec![],
                input_is_intermediate: false,
                types: Some(crate::exec::node::aggregate::AggTypeSignature {
                    intermediate_type: Some(DataType::Binary),
                    output_type: Some(DataType::Utf8),
                    input_arg_type: None,
                }),
            };
            let spec = MaxMinByAgg
                .build_spec_from_type(&func, Some(&struct_type), false)
                .unwrap();

            let mut state = MaybeUninit::<MaxMinByState>::uninit();
            MaxMinByAgg.init_state(&spec, state.as_mut_ptr() as *mut u8);
            let state_ptr = state.as_mut_ptr() as AggStatePtr;
            let state_ptrs = vec![state_ptr; 3];
            MaxMinByAgg
                .update_batch(&spec, 0, &state_ptrs, &input)
                .unwrap();
            let out = MaxMinByAgg
                .build_array(&spec, 0, &[state_ptr], false)
                .unwrap();
            MaxMinByAgg.drop_state(&spec, state.as_mut_ptr() as *mut u8);

            let out_arr = out.as_any().downcast_ref::<StringArray>().unwrap();
            assert_eq!(out_arr.value(0), expected);
        }
    }
}
