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

fn encode_scalar_value(value: &AggScalarValue, buf: &mut Vec<u8>) -> Result<(), String> {
    match value {
        AggScalarValue::Bool(v) => {
            buf.push(1);
            buf.push(*v as u8);
        }
        AggScalarValue::Int64(v) => {
            buf.push(2);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        AggScalarValue::Float64(v) => {
            buf.push(3);
            buf.extend_from_slice(&v.to_bits().to_le_bytes());
        }
        AggScalarValue::Utf8(v) => {
            buf.push(4);
            let len = u32::try_from(v.len()).map_err(|_| "string too large".to_string())?;
            buf.extend_from_slice(&len.to_le_bytes());
            buf.extend_from_slice(v.as_bytes());
        }
        AggScalarValue::Date32(v) => {
            buf.push(5);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        AggScalarValue::Timestamp(v) => {
            buf.push(6);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        AggScalarValue::Decimal128(v) => {
            buf.push(7);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        AggScalarValue::Struct(items) => {
            buf.push(8);
            let len =
                u32::try_from(items.len()).map_err(|_| "max_by/min_by struct too large".to_string())?;
            buf.extend_from_slice(&len.to_le_bytes());
            for item in items {
                encode_scalar(item, buf)?;
            }
        }
        AggScalarValue::Map(items) => {
            buf.push(9);
            let len =
                u32::try_from(items.len()).map_err(|_| "max_by/min_by map too large".to_string())?;
            buf.extend_from_slice(&len.to_le_bytes());
            for (key, value) in items {
                encode_scalar(key, buf)?;
                encode_scalar(value, buf)?;
            }
        }
        AggScalarValue::List(items) => {
            buf.push(10);
            let len =
                u32::try_from(items.len()).map_err(|_| "max_by/min_by list too large".to_string())?;
            buf.extend_from_slice(&len.to_le_bytes());
            for item in items {
                encode_scalar(item, buf)?;
            }
        }
        AggScalarValue::Decimal256(v) => {
            buf.push(11);
            let text = v.to_string();
            let len = u32::try_from(text.len()).map_err(|_| "string too large".to_string())?;
            buf.extend_from_slice(&len.to_le_bytes());
            buf.extend_from_slice(text.as_bytes());
        }
    }
    Ok(())
}

fn encode_scalar(value: &Option<AggScalarValue>, buf: &mut Vec<u8>) -> Result<(), String> {
    match value {
        Some(value) => {
            buf.push(1);
            encode_scalar_value(value, buf)
        }
        None => {
            buf.push(0);
            Ok(())
        }
    }
}

fn need_len(input: &[u8], need: usize, label: &str) -> Result<(), String> {
    if input.len() < need {
        Err(format!("max_by/min_by {} decode failed", label))
    } else {
        Ok(())
    }
}

fn read_u32(input: &mut &[u8], label: &str) -> Result<u32, String> {
    need_len(input, 4, label)?;
    let value = u32::from_le_bytes(input[..4].try_into().unwrap());
    *input = &input[4..];
    Ok(value)
}

fn decode_scalar_value(input: &mut &[u8]) -> Result<AggScalarValue, String> {
    need_len(input, 1, "scalar")?;
    let tag = input[0];
    *input = &input[1..];
    match tag {
        1 => {
            need_len(input, 1, "bool")?;
            let v = input[0] != 0;
            *input = &input[1..];
            Ok(AggScalarValue::Bool(v))
        }
        2 => {
            need_len(input, 8, "int64")?;
            let v = i64::from_le_bytes(input[..8].try_into().unwrap());
            *input = &input[8..];
            Ok(AggScalarValue::Int64(v))
        }
        3 => {
            need_len(input, 8, "float64")?;
            let bits = u64::from_le_bytes(input[..8].try_into().unwrap());
            *input = &input[8..];
            Ok(AggScalarValue::Float64(f64::from_bits(bits)))
        }
        4 => {
            let len = read_u32(input, "utf8")? as usize;
            need_len(input, len, "utf8")?;
            let v = std::str::from_utf8(&input[..len])
                .map_err(|e| e.to_string())?
                .to_string();
            *input = &input[len..];
            Ok(AggScalarValue::Utf8(v))
        }
        5 => {
            need_len(input, 4, "date32")?;
            let v = i32::from_le_bytes(input[..4].try_into().unwrap());
            *input = &input[4..];
            Ok(AggScalarValue::Date32(v))
        }
        6 => {
            need_len(input, 8, "timestamp")?;
            let v = i64::from_le_bytes(input[..8].try_into().unwrap());
            *input = &input[8..];
            Ok(AggScalarValue::Timestamp(v))
        }
        7 => {
            need_len(input, 16, "decimal128")?;
            let v = i128::from_le_bytes(input[..16].try_into().unwrap());
            *input = &input[16..];
            Ok(AggScalarValue::Decimal128(v))
        }
        8 => {
            let len = read_u32(input, "struct")? as usize;
            let mut items = Vec::with_capacity(len);
            for _ in 0..len {
                items.push(decode_scalar(input)?);
            }
            Ok(AggScalarValue::Struct(items))
        }
        9 => {
            let len = read_u32(input, "map")? as usize;
            let mut items = Vec::with_capacity(len);
            for _ in 0..len {
                let key = decode_scalar(input)?;
                let value = decode_scalar(input)?;
                items.push((key, value));
            }
            Ok(AggScalarValue::Map(items))
        }
        10 => {
            let len = read_u32(input, "list")? as usize;
            let mut items = Vec::with_capacity(len);
            for _ in 0..len {
                items.push(decode_scalar(input)?);
            }
            Ok(AggScalarValue::List(items))
        }
        11 => {
            let len = read_u32(input, "decimal256")? as usize;
            need_len(input, len, "decimal256")?;
            let text = std::str::from_utf8(&input[..len]).map_err(|e| e.to_string())?;
            *input = &input[len..];
            let v = text
                .parse::<i256>()
                .map_err(|_| "max_by/min_by decimal256 decode failed".to_string())?;
            Ok(AggScalarValue::Decimal256(v))
        }
        _ => Err("max_by/min_by scalar decode failed: unknown tag".to_string()),
    }
}

fn decode_scalar(input: &mut &[u8]) -> Result<Option<AggScalarValue>, String> {
    need_len(input, 1, "scalar")?;
    let has_value = input[0];
    *input = &input[1..];
    match has_value {
        0 => Ok(None),
        1 => decode_scalar_value(input).map(Some),
        _ => Err("max_by/min_by scalar decode failed: invalid null flag".to_string()),
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

    #[test]
    fn test_max_min_by_complex_value_round_trip() {
        let value = Some(AggScalarValue::Struct(vec![
            Some(AggScalarValue::Utf8("tag".to_string())),
            Some(AggScalarValue::List(vec![
                Some(AggScalarValue::Int64(7)),
                None,
                Some(AggScalarValue::Map(vec![(
                    Some(AggScalarValue::Utf8("k".to_string())),
                    Some(AggScalarValue::Utf8("v".to_string())),
                )])),
            ])),
        ]));

        let mut encoded = Vec::new();
        encode_scalar(&value, &mut encoded).unwrap();

        let mut input = encoded.as_slice();
        let decoded = decode_scalar(&mut input).unwrap();
        assert!(input.is_empty());

        let mut reencoded = Vec::new();
        encode_scalar(&decoded, &mut reencoded).unwrap();
        assert_eq!(encoded, reencoded);
    }
}
