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
    ArrayRef, BinaryArray, BinaryBuilder, Float64Builder, StringBuilder, StructArray,
};
use arrow::datatypes::DataType;

use crate::exec::node::aggregate::AggFunction;

use super::super::*;
use super::AggregateFunction;

pub(super) struct CovarCorrAgg;

fn kind_from_name(name: &str) -> Option<AggKind> {
    match name {
        "covar_pop" => Some(AggKind::CovarPop),
        "covar_samp" => Some(AggKind::CovarSamp),
        "corr" => Some(AggKind::Corr),
        _ => None,
    }
}

enum NumericView<'a> {
    Int(IntArrayView<'a>),
    Float(FloatArrayView<'a>),
}

impl<'a> NumericView<'a> {
    fn new(array: &'a ArrayRef) -> Result<Self, String> {
        match array.data_type() {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                Ok(Self::Int(IntArrayView::new(array)?))
            }
            DataType::Float32 | DataType::Float64 => Ok(Self::Float(FloatArrayView::new(array)?)),
            other => Err(format!("covar/corr unsupported input type: {:?}", other)),
        }
    }

    fn value_at(&self, row: usize) -> Option<f64> {
        match self {
            NumericView::Int(view) => view.value_at(row).map(|v| v as f64),
            NumericView::Float(view) => view.value_at(row),
        }
    }
}

fn update_covar_state(state: &mut CovarState, x: f64, y: f64) {
    state.count += 1;
    let count = state.count as f64;
    let old_mean_x = state.mean_x;
    let old_mean_y = state.mean_y;
    state.mean_x = old_mean_x + (x - old_mean_x) / count;
    state.mean_y = old_mean_y + (y - old_mean_y) / count;
    state.c2 += (x - old_mean_x) * (y - state.mean_y);
}

fn update_corr_state(state: &mut CorrState, x: f64, y: f64) {
    state.count += 1;
    let count = state.count as f64;
    let old_mean_x = state.mean_x;
    let old_mean_y = state.mean_y;
    state.mean_x = old_mean_x + (x - old_mean_x) / count;
    state.mean_y = old_mean_y + (y - old_mean_y) / count;
    state.c2 += (x - old_mean_x) * (y - state.mean_y);
    state.m2x += (x - old_mean_x) * (x - state.mean_x);
    state.m2y += (y - old_mean_y) * (y - state.mean_y);
}

fn merge_covar_state(state: &mut CovarState, mean_x: f64, mean_y: f64, c2: f64, count: i64) {
    if count == 0 {
        return;
    }
    if state.count == 0 {
        state.mean_x = mean_x;
        state.mean_y = mean_y;
        state.c2 = c2;
        state.count = count;
        return;
    }
    let delta_x = state.mean_x - mean_x;
    let delta_y = state.mean_y - mean_y;
    let sum_count = state.count + count;
    let sum_count_f = sum_count as f64;
    let factor = (state.count as f64) * (count as f64) / sum_count_f;
    state.mean_x = mean_x + delta_x * (state.count as f64 / sum_count_f);
    state.mean_y = mean_y + delta_y * (state.count as f64 / sum_count_f);
    state.c2 = c2 + state.c2 + (delta_x * delta_y) * factor;
    state.count = sum_count;
}

fn merge_corr_state(
    state: &mut CorrState,
    mean_x: f64,
    mean_y: f64,
    c2: f64,
    count: i64,
    m2x: f64,
    m2y: f64,
) {
    if count == 0 {
        return;
    }
    if state.count == 0 {
        state.mean_x = mean_x;
        state.mean_y = mean_y;
        state.c2 = c2;
        state.m2x = m2x;
        state.m2y = m2y;
        state.count = count;
        return;
    }
    let delta_x = state.mean_x - mean_x;
    let delta_y = state.mean_y - mean_y;
    let sum_count = state.count + count;
    let sum_count_f = sum_count as f64;
    let factor = (state.count as f64) * (count as f64) / sum_count_f;
    state.mean_x = mean_x + delta_x * (state.count as f64 / sum_count_f);
    state.mean_y = mean_y + delta_y * (state.count as f64 / sum_count_f);
    state.c2 = c2 + state.c2 + (delta_x * delta_y) * factor;
    state.m2x = m2x + state.m2x + (delta_x * delta_x) * factor;
    state.m2y = m2y + state.m2y + (delta_y * delta_y) * factor;
    state.count = sum_count;
}

fn parse_covar_state(bytes: &[u8]) -> Result<(f64, f64, f64, i64), String> {
    if bytes.len() != 32 {
        return Err(format!(
            "covar intermediate binary size mismatch: expected 32, got {}",
            bytes.len()
        ));
    }
    let mean_x = f64::from_le_bytes(bytes[0..8].try_into().unwrap());
    let mean_y = f64::from_le_bytes(bytes[8..16].try_into().unwrap());
    let c2 = f64::from_le_bytes(bytes[16..24].try_into().unwrap());
    let count = i64::from_le_bytes(bytes[24..32].try_into().unwrap());
    Ok((mean_x, mean_y, c2, count))
}

fn parse_corr_state(bytes: &[u8]) -> Result<(f64, f64, f64, i64, f64, f64), String> {
    if bytes.len() != 48 {
        return Err(format!(
            "corr intermediate binary size mismatch: expected 48, got {}",
            bytes.len()
        ));
    }
    let mean_x = f64::from_le_bytes(bytes[0..8].try_into().unwrap());
    let mean_y = f64::from_le_bytes(bytes[8..16].try_into().unwrap());
    let c2 = f64::from_le_bytes(bytes[16..24].try_into().unwrap());
    let count = i64::from_le_bytes(bytes[24..32].try_into().unwrap());
    let m2x = f64::from_le_bytes(bytes[32..40].try_into().unwrap());
    let m2y = f64::from_le_bytes(bytes[40..48].try_into().unwrap());
    Ok((mean_x, mean_y, c2, count, m2x, m2y))
}

impl AggregateFunction for CovarCorrAgg {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        input_type: Option<&DataType>,
        input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        let name = func.name.as_str();
        let kind = kind_from_name(name)
            .ok_or_else(|| format!("unsupported covar/corr function: {name}"))?;
        let data_type = input_type.ok_or_else(|| format!("{name} input type missing"))?;

        if input_is_intermediate {
            return Ok(AggSpec {
                kind,
                output_type: DataType::Float64,
                intermediate_type: data_type.clone(),
                input_arg_type: None,
                count_all: false,
            });
        }

        match data_type {
            DataType::Struct(fields) => {
                if fields.len() != 2 {
                    return Err(format!("{name} expects 2 arguments"));
                }
                Ok(AggSpec {
                    kind,
                    output_type: DataType::Float64,
                    intermediate_type: DataType::Binary,
                    input_arg_type: None,
                    count_all: false,
                })
            }
            other => Err(format!("{name} expects struct input, got {:?}", other)),
        }
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        match kind {
            AggKind::CovarPop | AggKind::CovarSamp => (
                std::mem::size_of::<CovarState>(),
                std::mem::align_of::<CovarState>(),
            ),
            AggKind::Corr => (
                std::mem::size_of::<CorrState>(),
                std::mem::align_of::<CorrState>(),
            ),
            other => unreachable!("unexpected kind for covar/corr: {:?}", other),
        }
    }

    fn build_input_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "covar/corr input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn build_merge_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "covar/corr merge input missing".to_string())?;
        match arr.data_type() {
            DataType::Binary => {
                let bin = arr
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| "failed to downcast to BinaryArray".to_string())?;
                Ok(AggInputView::Binary(bin))
            }
            DataType::Utf8 => Ok(AggInputView::Utf8(Utf8ArrayView::new(arr)?)),
            other => Err(format!(
                "covar/corr intermediate type mismatch: {:?}",
                other
            )),
        }
    }

    fn init_state(&self, spec: &AggSpec, ptr: *mut u8) {
        match spec.kind {
            AggKind::CovarPop | AggKind::CovarSamp => unsafe {
                std::ptr::write(ptr as *mut CovarState, CovarState::default());
            },
            AggKind::Corr => unsafe {
                std::ptr::write(ptr as *mut CorrState, CorrState::default());
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
        let AggInputView::Any(array) = input else {
            return Err("covar/corr batch input type mismatch".to_string());
        };
        let struct_arr = array
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| "covar/corr expects struct input".to_string())?;
        if struct_arr.num_columns() != 2 {
            return Err("covar/corr expects 2 arguments".to_string());
        }
        let left = struct_arr.column(0);
        let right = struct_arr.column(1);
        let left_view = NumericView::new(left)?;
        let right_view = NumericView::new(right)?;

        for (row, &base) in state_ptrs.iter().enumerate() {
            let Some(x) = left_view.value_at(row) else {
                continue;
            };
            let Some(y) = right_view.value_at(row) else {
                continue;
            };
            match spec.kind {
                AggKind::CovarPop | AggKind::CovarSamp => {
                    let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut CovarState) };
                    update_covar_state(state, x, y);
                }
                AggKind::Corr => {
                    let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut CorrState) };
                    update_corr_state(state, x, y);
                }
                _ => return Err("covar/corr kind mismatch".to_string()),
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
        match input {
            AggInputView::Binary(arr) => {
                for (row, &base) in state_ptrs.iter().enumerate() {
                    if arr.is_null(row) {
                        continue;
                    }
                    let bytes = arr.value(row);
                    match spec.kind {
                        AggKind::CovarPop | AggKind::CovarSamp => {
                            let (mean_x, mean_y, c2, count) = parse_covar_state(bytes)?;
                            let state =
                                unsafe { &mut *((base as *mut u8).add(offset) as *mut CovarState) };
                            merge_covar_state(state, mean_x, mean_y, c2, count);
                        }
                        AggKind::Corr => {
                            let (mean_x, mean_y, c2, count, m2x, m2y) = parse_corr_state(bytes)?;
                            let state =
                                unsafe { &mut *((base as *mut u8).add(offset) as *mut CorrState) };
                            merge_corr_state(state, mean_x, mean_y, c2, count, m2x, m2y);
                        }
                        _ => return Err("covar/corr kind mismatch".to_string()),
                    }
                }
                Ok(())
            }
            AggInputView::Utf8(view) => {
                let mut values = Vec::with_capacity(view.len());
                for i in 0..view.len() {
                    values.push(view.value_at(i));
                }
                for (row, &base) in state_ptrs.iter().enumerate() {
                    let Some(text) = values[row].as_deref() else {
                        continue;
                    };
                    let parts: Vec<&str> = text.split(',').collect();
                    match spec.kind {
                        AggKind::CovarPop | AggKind::CovarSamp => {
                            if parts.len() != 4 {
                                return Err("covar utf8 state expects 4 parts".to_string());
                            }
                            let mean_x = parts[0].parse::<f64>().map_err(|e| e.to_string())?;
                            let mean_y = parts[1].parse::<f64>().map_err(|e| e.to_string())?;
                            let c2 = parts[2].parse::<f64>().map_err(|e| e.to_string())?;
                            let count = parts[3].parse::<i64>().map_err(|e| e.to_string())?;
                            let state =
                                unsafe { &mut *((base as *mut u8).add(offset) as *mut CovarState) };
                            merge_covar_state(state, mean_x, mean_y, c2, count);
                        }
                        AggKind::Corr => {
                            if parts.len() != 6 {
                                return Err("corr utf8 state expects 6 parts".to_string());
                            }
                            let mean_x = parts[0].parse::<f64>().map_err(|e| e.to_string())?;
                            let mean_y = parts[1].parse::<f64>().map_err(|e| e.to_string())?;
                            let c2 = parts[2].parse::<f64>().map_err(|e| e.to_string())?;
                            let count = parts[3].parse::<i64>().map_err(|e| e.to_string())?;
                            let m2x = parts[4].parse::<f64>().map_err(|e| e.to_string())?;
                            let m2y = parts[5].parse::<f64>().map_err(|e| e.to_string())?;
                            let state =
                                unsafe { &mut *((base as *mut u8).add(offset) as *mut CorrState) };
                            merge_corr_state(state, mean_x, mean_y, c2, count, m2x, m2y);
                        }
                        _ => return Err("covar/corr kind mismatch".to_string()),
                    }
                }
                Ok(())
            }
            _ => Err("covar/corr merge input type mismatch".to_string()),
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
            match spec.intermediate_type {
                DataType::Binary => {
                    let mut builder = BinaryBuilder::new();
                    for &base in group_states {
                        match spec.kind {
                            AggKind::CovarPop | AggKind::CovarSamp => {
                                let state = unsafe {
                                    &*((base as *mut u8).add(offset) as *const CovarState)
                                };
                                if state.count == 0 {
                                    builder.append_null();
                                    continue;
                                }
                                let mut buf = [0u8; 32];
                                buf[0..8].copy_from_slice(&state.mean_x.to_le_bytes());
                                buf[8..16].copy_from_slice(&state.mean_y.to_le_bytes());
                                buf[16..24].copy_from_slice(&state.c2.to_le_bytes());
                                buf[24..32].copy_from_slice(&state.count.to_le_bytes());
                                builder.append_value(&buf);
                            }
                            AggKind::Corr => {
                                let state = unsafe {
                                    &*((base as *mut u8).add(offset) as *const CorrState)
                                };
                                if state.count == 0 {
                                    builder.append_null();
                                    continue;
                                }
                                let mut buf = [0u8; 48];
                                buf[0..8].copy_from_slice(&state.mean_x.to_le_bytes());
                                buf[8..16].copy_from_slice(&state.mean_y.to_le_bytes());
                                buf[16..24].copy_from_slice(&state.c2.to_le_bytes());
                                buf[24..32].copy_from_slice(&state.count.to_le_bytes());
                                buf[32..40].copy_from_slice(&state.m2x.to_le_bytes());
                                buf[40..48].copy_from_slice(&state.m2y.to_le_bytes());
                                builder.append_value(&buf);
                            }
                            _ => return Err("covar/corr kind mismatch".to_string()),
                        }
                    }
                    return Ok(std::sync::Arc::new(builder.finish()));
                }
                DataType::Utf8 => {
                    let mut builder = StringBuilder::new();
                    for &base in group_states {
                        match spec.kind {
                            AggKind::CovarPop | AggKind::CovarSamp => {
                                let state = unsafe {
                                    &*((base as *mut u8).add(offset) as *const CovarState)
                                };
                                if state.count == 0 {
                                    builder.append_null();
                                } else {
                                    builder.append_value(format!(
                                        "{},{},{},{}",
                                        state.mean_x, state.mean_y, state.c2, state.count
                                    ));
                                }
                            }
                            AggKind::Corr => {
                                let state = unsafe {
                                    &*((base as *mut u8).add(offset) as *const CorrState)
                                };
                                if state.count == 0 {
                                    builder.append_null();
                                } else {
                                    builder.append_value(format!(
                                        "{},{},{},{},{},{}",
                                        state.mean_x,
                                        state.mean_y,
                                        state.c2,
                                        state.count,
                                        state.m2x,
                                        state.m2y
                                    ));
                                }
                            }
                            _ => return Err("covar/corr kind mismatch".to_string()),
                        }
                    }
                    return Ok(std::sync::Arc::new(builder.finish()));
                }
                _ => {
                    return Err("covar/corr intermediate type mismatch".to_string());
                }
            }
        }

        let mut builder = Float64Builder::new();
        for &base in group_states {
            match spec.kind {
                AggKind::CovarPop => {
                    let state = unsafe { &*((base as *mut u8).add(offset) as *const CovarState) };
                    if state.count == 0 {
                        builder.append_null();
                    } else {
                        builder.append_value(state.c2 / state.count as f64);
                    }
                }
                AggKind::CovarSamp => {
                    let state = unsafe { &*((base as *mut u8).add(offset) as *const CovarState) };
                    if state.count <= 1 {
                        builder.append_null();
                    } else {
                        builder.append_value(state.c2 / (state.count as f64 - 1.0));
                    }
                }
                AggKind::Corr => {
                    let state = unsafe { &*((base as *mut u8).add(offset) as *const CorrState) };
                    if state.count < 2 || state.m2x <= 0.0 || state.m2y <= 0.0 {
                        builder.append_null();
                    } else {
                        builder.append_value(state.c2 / state.m2x.sqrt() / state.m2y.sqrt());
                    }
                }
                _ => return Err("covar/corr kind mismatch".to_string()),
            }
        }
        Ok(std::sync::Arc::new(builder.finish()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, StructArray};
    use arrow::datatypes::{DataType, Field, Fields};
    use std::mem::MaybeUninit;

    #[test]
    fn test_covar_spec() {
        let func = AggFunction {
            name: "covar_pop".to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(crate::exec::node::aggregate::AggTypeSignature {
                intermediate_type: Some(DataType::Binary),
                output_type: Some(DataType::Float64),
                input_arg_type: None,
            }),
        };
        let struct_type = DataType::Struct(
            vec![
                Field::new("x", DataType::Float64, true),
                Field::new("y", DataType::Float64, true),
            ]
            .into(),
        );
        let spec = CovarCorrAgg
            .build_spec_from_type(&func, Some(&struct_type), false)
            .unwrap();
        assert!(matches!(spec.kind, AggKind::CovarPop));
    }

    #[test]
    fn test_covar_pop_and_samp() {
        let values_x = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])) as ArrayRef;
        let values_y = Arc::new(Float64Array::from(vec![2.0, 4.0, 6.0])) as ArrayRef;
        let fields = vec![
            Field::new("x", DataType::Float64, true),
            Field::new("y", DataType::Float64, true),
        ];
        let struct_type = DataType::Struct(Fields::from(fields.clone()));
        let struct_arr = StructArray::new(fields.into(), vec![values_x, values_y], None);
        let array_ref = Arc::new(struct_arr) as ArrayRef;
        let input = AggInputView::Any(&array_ref);

        for (name, expected) in [("covar_pop", 4.0 / 3.0), ("covar_samp", 2.0)] {
            let func = AggFunction {
                name: name.to_string(),
                inputs: vec![],
                input_is_intermediate: false,
                types: Some(crate::exec::node::aggregate::AggTypeSignature {
                    intermediate_type: Some(DataType::Binary),
                    output_type: Some(DataType::Float64),
                    input_arg_type: None,
                }),
            };
            let spec = CovarCorrAgg
                .build_spec_from_type(&func, Some(&struct_type), false)
                .unwrap();

            let mut state = MaybeUninit::<CovarState>::uninit();
            CovarCorrAgg.init_state(&spec, state.as_mut_ptr() as *mut u8);
            let state_ptr = state.as_mut_ptr() as AggStatePtr;
            let state_ptrs = vec![state_ptr; 3];
            CovarCorrAgg
                .update_batch(&spec, 0, &state_ptrs, &input)
                .unwrap();
            let out = CovarCorrAgg
                .build_array(&spec, 0, &[state_ptr], false)
                .unwrap();
            CovarCorrAgg.drop_state(&spec, state.as_mut_ptr() as *mut u8);

            let out_arr = out.as_any().downcast_ref::<Float64Array>().unwrap();
            let got = out_arr.value(0);
            assert!((got - expected).abs() < 1e-9, "name={}", name);
        }
    }

    #[test]
    fn test_corr() {
        let values_x = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])) as ArrayRef;
        let values_y = Arc::new(Float64Array::from(vec![2.0, 4.0, 6.0])) as ArrayRef;
        let fields = vec![
            Field::new("x", DataType::Float64, true),
            Field::new("y", DataType::Float64, true),
        ];
        let struct_type = DataType::Struct(Fields::from(fields.clone()));
        let struct_arr = StructArray::new(fields.into(), vec![values_x, values_y], None);
        let array_ref = Arc::new(struct_arr) as ArrayRef;
        let input = AggInputView::Any(&array_ref);

        let func = AggFunction {
            name: "corr".to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(crate::exec::node::aggregate::AggTypeSignature {
                intermediate_type: Some(DataType::Binary),
                output_type: Some(DataType::Float64),
                input_arg_type: None,
            }),
        };
        let spec = CovarCorrAgg
            .build_spec_from_type(&func, Some(&struct_type), false)
            .unwrap();

        let mut state = MaybeUninit::<CorrState>::uninit();
        CovarCorrAgg.init_state(&spec, state.as_mut_ptr() as *mut u8);
        let state_ptr = state.as_mut_ptr() as AggStatePtr;
        let state_ptrs = vec![state_ptr; 3];
        CovarCorrAgg
            .update_batch(&spec, 0, &state_ptrs, &input)
            .unwrap();
        let out = CovarCorrAgg
            .build_array(&spec, 0, &[state_ptr], false)
            .unwrap();
        CovarCorrAgg.drop_state(&spec, state.as_mut_ptr() as *mut u8);

        let out_arr = out.as_any().downcast_ref::<Float64Array>().unwrap();
        let got = out_arr.value(0);
        assert!((got - 1.0).abs() < 1e-9);
    }
}
