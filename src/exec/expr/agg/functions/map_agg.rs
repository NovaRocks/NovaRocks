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
use arrow::array::{ArrayRef, MapArray, StructArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field};
use arrow_buffer::OffsetBuffer;
use std::collections::HashSet;
use std::sync::Arc;

use crate::exec::node::aggregate::AggFunction;

use super::super::*;
use super::AggregateFunction;
use super::common::{AggScalarValue, build_scalar_array, scalar_from_array};

pub(super) struct MapAggAgg;

#[derive(Clone, Debug, Default)]
struct MapAggState {
    seen_keys: HashSet<Vec<u8>>,
    entries: Vec<(AggScalarValue, Option<AggScalarValue>)>,
}

impl AggregateFunction for MapAggAgg {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        input_type: Option<&DataType>,
        input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        let input_type = input_type.ok_or_else(|| "map_agg input type missing".to_string())?;
        let Some(kind) = kind_from_name(func.name.as_str()) else {
            return Err(format!("unsupported map agg function: {}", func.name));
        };

        if input_is_intermediate {
            let output_type = func
                .types
                .as_ref()
                .and_then(|t| t.output_type.clone())
                .unwrap_or_else(|| input_type.clone());
            return Ok(AggSpec {
                kind,
                output_type,
                intermediate_type: input_type.clone(),
                input_arg_type: func.types.as_ref().and_then(|t| t.input_arg_type.clone()),
                count_all: false,
            });
        }

        let DataType::Struct(fields) = input_type else {
            return Err(format!(
                "map_agg expects struct input, got {:?}",
                input_type
            ));
        };
        if fields.len() != 2 {
            return Err("map_agg expects 2 arguments".to_string());
        }

        let output_type = func
            .types
            .as_ref()
            .and_then(|t| t.output_type.clone())
            .unwrap_or_else(|| build_default_map_type(fields[0].clone(), fields[1].clone()));
        let intermediate_type = func
            .types
            .as_ref()
            .and_then(|t| t.intermediate_type.clone())
            .unwrap_or_else(|| output_type.clone());

        Ok(AggSpec {
            kind,
            output_type,
            intermediate_type,
            input_arg_type: func.types.as_ref().and_then(|t| t.input_arg_type.clone()),
            count_all: false,
        })
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        match kind {
            AggKind::MapAgg => (
                std::mem::size_of::<MapAggState>(),
                std::mem::align_of::<MapAggState>(),
            ),
            other => unreachable!("unexpected kind for map_agg: {:?}", other),
        }
    }

    fn build_input_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "map_agg input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn build_merge_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "map_agg merge input missing".to_string())?;
        let _ = arr
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| "map_agg merge input must be MapArray".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn init_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::write(ptr as *mut MapAggState, MapAggState::default());
        }
    }

    fn drop_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::drop_in_place(ptr as *mut MapAggState);
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
            return Err("map_agg batch input type mismatch".to_string());
        };
        let struct_arr = array
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| "map_agg expects struct input".to_string())?;
        if struct_arr.num_columns() != 2 {
            return Err("map_agg expects 2 arguments".to_string());
        }

        let key_arr = struct_arr.column(0).clone();
        let value_arr = struct_arr.column(1).clone();
        for (row, &base) in state_ptrs.iter().enumerate() {
            let Some(key) = scalar_from_array(&key_arr, row)? else {
                continue;
            };
            let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut MapAggState) };
            let key_fp = key_fingerprint(&key);
            if state.seen_keys.insert(key_fp) {
                let value = scalar_from_array(&value_arr, row)?;
                state.entries.push((key, value));
            }
        }
        Ok(())
    }

    fn merge_batch(
        &self,
        _spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        let AggInputView::Any(array) = input else {
            return Err("map_agg merge input type mismatch".to_string());
        };
        let map_arr = array
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| "map_agg merge input must be MapArray".to_string())?;
        let key_arr = map_arr.keys().clone();
        let value_arr = map_arr.values().clone();
        let offsets = map_arr.value_offsets();

        for (row, &base) in state_ptrs.iter().enumerate() {
            if map_arr.is_null(row) {
                continue;
            }
            let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut MapAggState) };
            let start = offsets[row] as usize;
            let end = offsets[row + 1] as usize;
            for idx in start..end {
                let Some(key) = scalar_from_array(&key_arr, idx)? else {
                    continue;
                };
                let key_fp = key_fingerprint(&key);
                if state.seen_keys.insert(key_fp) {
                    let value = scalar_from_array(&value_arr, idx)?;
                    state.entries.push((key, value));
                }
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
        let (map_field, field_defs, ordered) = parse_map_type(target_type)?;
        let key_type = field_defs[0].data_type();
        let value_type = field_defs[1].data_type();

        let mut key_values = Vec::<Option<AggScalarValue>>::new();
        let mut value_values = Vec::<Option<AggScalarValue>>::new();
        let mut offsets = Vec::with_capacity(group_states.len() + 1);
        offsets.push(0_i32);
        let mut current: i64 = 0;
        for &base in group_states {
            let state = unsafe { &*((base as *mut u8).add(offset) as *const MapAggState) };
            for (key, value) in &state.entries {
                key_values.push(Some(key.clone()));
                value_values.push(value.clone());
                current += 1;
                if current > i32::MAX as i64 {
                    return Err("map_agg offset overflow".to_string());
                }
            }
            offsets.push(current as i32);
        }

        let mut out_keys = build_scalar_array(key_type, key_values)?;
        let mut out_values = build_scalar_array(value_type, value_values)?;
        if out_keys.data_type() != field_defs[0].data_type() {
            out_keys = cast(&out_keys, field_defs[0].data_type())
                .map_err(|e| format!("map_agg failed to cast output key: {}", e))?;
        }
        if out_values.data_type() != field_defs[1].data_type() {
            out_values = cast(&out_values, field_defs[1].data_type())
                .map_err(|e| format!("map_agg failed to cast output value: {}", e))?;
        }

        let entries = StructArray::new(field_defs, vec![out_keys, out_values], None);
        let out = MapArray::try_new(
            map_field,
            OffsetBuffer::new(offsets.into()),
            entries,
            None,
            ordered,
        )
        .map_err(|e| format!("map_agg: {}", e))?;
        Ok(Arc::new(out))
    }
}

fn kind_from_name(name: &str) -> Option<AggKind> {
    match name {
        "map_agg" => Some(AggKind::MapAgg),
        _ => None,
    }
}

fn parse_map_type(ty: &DataType) -> Result<(Arc<Field>, arrow::datatypes::Fields, bool), String> {
    let DataType::Map(field, ordered) = ty else {
        return Err(format!("map_agg output type must be MAP, got {:?}", ty));
    };
    let DataType::Struct(fields) = field.data_type().clone() else {
        return Err("map_agg map entries type must be STRUCT".to_string());
    };
    if fields.len() != 2 {
        return Err("map_agg map entries type must have 2 fields".to_string());
    }
    Ok((field.clone(), fields, *ordered))
}

fn build_default_map_type(key_field: Arc<Field>, value_field: Arc<Field>) -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(arrow::datatypes::Fields::from(vec![
                Arc::new(Field::new(
                    "key",
                    key_field.data_type().clone(),
                    key_field.is_nullable(),
                )),
                Arc::new(Field::new(
                    "value",
                    value_field.data_type().clone(),
                    value_field.is_nullable(),
                )),
            ])),
            false,
        )),
        false,
    )
}

fn key_fingerprint(key: &AggScalarValue) -> Vec<u8> {
    fn encode_scalar(out: &mut Vec<u8>, key: &AggScalarValue) {
        match key {
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
                let len = text.len() as u32;
                out.extend_from_slice(&len.to_le_bytes());
                out.extend_from_slice(text.as_bytes());
            }
            AggScalarValue::Struct(items) => {
                out.push(8);
                let len = items.len() as u32;
                out.extend_from_slice(&len.to_le_bytes());
                for item in items {
                    match item {
                        Some(v) => {
                            out.push(1);
                            encode_scalar(out, v);
                        }
                        None => out.push(0),
                    }
                }
            }
            AggScalarValue::Map(items) => {
                out.push(9);
                let len = items.len() as u32;
                out.extend_from_slice(&len.to_le_bytes());
                for (k, v) in items {
                    match k {
                        Some(k) => {
                            out.push(1);
                            encode_scalar(out, k);
                        }
                        None => out.push(0),
                    }
                    match v {
                        Some(v) => {
                            out.push(1);
                            encode_scalar(out, v);
                        }
                        None => out.push(0),
                    }
                }
            }
            AggScalarValue::List(items) => {
                out.push(10);
                let len = items.len() as u32;
                out.extend_from_slice(&len.to_le_bytes());
                for item in items {
                    match item {
                        Some(v) => {
                            out.push(1);
                            encode_scalar(out, v);
                        }
                        None => out.push(0),
                    }
                }
            }
        }
    }

    let mut out = Vec::new();
    match key {
        _ => encode_scalar(&mut out, key),
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, Int64Builder, MapBuilder, StructArray};
    use std::mem::MaybeUninit;

    fn map_type_i64_i64() -> DataType {
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(arrow::datatypes::Fields::from(vec![
                    Arc::new(Field::new("key", DataType::Int64, false)),
                    Arc::new(Field::new("value", DataType::Int64, true)),
                ])),
                false,
            )),
            false,
        )
    }

    #[test]
    fn test_map_agg_spec() {
        let func = AggFunction {
            name: "map_agg".to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(crate::exec::node::aggregate::AggTypeSignature {
                intermediate_type: Some(map_type_i64_i64()),
                output_type: Some(map_type_i64_i64()),
                input_arg_type: None,
            }),
        };
        let input_type = DataType::Struct(arrow::datatypes::Fields::from(vec![
            Arc::new(Field::new("k", DataType::Int64, true)),
            Arc::new(Field::new("v", DataType::Int64, true)),
        ]));
        let spec = MapAggAgg
            .build_spec_from_type(&func, Some(&input_type), false)
            .unwrap();
        assert!(matches!(spec.kind, AggKind::MapAgg));
        assert_eq!(spec.output_type, map_type_i64_i64());
    }

    #[test]
    fn test_map_agg_update_dedups_keep_first() {
        let func = AggFunction {
            name: "map_agg".to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(crate::exec::node::aggregate::AggTypeSignature {
                intermediate_type: Some(map_type_i64_i64()),
                output_type: Some(map_type_i64_i64()),
                input_arg_type: None,
            }),
        };
        let input_type = DataType::Struct(arrow::datatypes::Fields::from(vec![
            Arc::new(Field::new("k", DataType::Int64, true)),
            Arc::new(Field::new("v", DataType::Int64, true)),
        ]));
        let spec = MapAggAgg
            .build_spec_from_type(&func, Some(&input_type), false)
            .unwrap();

        let keys = Arc::new(Int64Array::from(vec![Some(1), Some(1), Some(2), None])) as ArrayRef;
        let vals = Arc::new(Int64Array::from(vec![
            Some(10),
            Some(99),
            Some(20),
            Some(30),
        ])) as ArrayRef;
        let input = Arc::new(StructArray::new(
            arrow::datatypes::Fields::from(vec![
                Arc::new(Field::new("k", DataType::Int64, true)),
                Arc::new(Field::new("v", DataType::Int64, true)),
            ]),
            vec![keys, vals],
            None,
        )) as ArrayRef;

        let view = AggInputView::Any(&input);
        let mut state = MaybeUninit::<MapAggState>::uninit();
        MapAggAgg.init_state(&spec, state.as_mut_ptr() as *mut u8);
        let state_ptr = state.as_mut_ptr() as AggStatePtr;
        let state_ptrs = vec![state_ptr; 4];

        MapAggAgg
            .update_batch(&spec, 0, &state_ptrs, &view)
            .unwrap();
        let out = MapAggAgg
            .build_array(&spec, 0, &[state_ptr], false)
            .unwrap();
        MapAggAgg.drop_state(&spec, state.as_mut_ptr() as *mut u8);

        let out = out.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(out.value_length(0), 2);
        let keys = out.keys().as_any().downcast_ref::<Int64Array>().unwrap();
        let vals = out.values().as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(keys.value(0), 1);
        assert_eq!(vals.value(0), 10);
        assert_eq!(keys.value(1), 2);
        assert_eq!(vals.value(1), 20);
    }

    #[test]
    fn test_map_agg_merge_dedups_keep_first() {
        let func = AggFunction {
            name: "map_agg".to_string(),
            inputs: vec![],
            input_is_intermediate: true,
            types: Some(crate::exec::node::aggregate::AggTypeSignature {
                intermediate_type: Some(map_type_i64_i64()),
                output_type: Some(map_type_i64_i64()),
                input_arg_type: None,
            }),
        };
        let spec = MapAggAgg
            .build_spec_from_type(&func, Some(&map_type_i64_i64()), true)
            .unwrap();

        let mut map_builder = MapBuilder::new(None, Int64Builder::new(), Int64Builder::new());
        map_builder.keys().append_value(1);
        map_builder.values().append_value(10);
        map_builder.keys().append_value(2);
        map_builder.values().append_value(20);
        map_builder.append(true).unwrap();
        map_builder.keys().append_value(2);
        map_builder.values().append_value(99);
        map_builder.keys().append_value(3);
        map_builder.values().append_value(30);
        map_builder.append(true).unwrap();
        let map_array = Arc::new(map_builder.finish()) as ArrayRef;
        let view = AggInputView::Any(&map_array);

        let mut state = MaybeUninit::<MapAggState>::uninit();
        MapAggAgg.init_state(&spec, state.as_mut_ptr() as *mut u8);
        let state_ptr = state.as_mut_ptr() as AggStatePtr;
        let state_ptrs = vec![state_ptr; 2];
        MapAggAgg.merge_batch(&spec, 0, &state_ptrs, &view).unwrap();
        let out = MapAggAgg
            .build_array(&spec, 0, &[state_ptr], false)
            .unwrap();
        MapAggAgg.drop_state(&spec, state.as_mut_ptr() as *mut u8);

        let out = out.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(out.value_length(0), 3);
        let keys = out.keys().as_any().downcast_ref::<Int64Array>().unwrap();
        let vals = out.values().as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(keys.value(0), 1);
        assert_eq!(vals.value(0), 10);
        assert_eq!(keys.value(1), 2);
        assert_eq!(vals.value(1), 20);
        assert_eq!(keys.value(2), 3);
        assert_eq!(vals.value(2), 30);
    }
}
