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
use std::collections::HashSet;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, BooleanArray, Decimal128Array, Decimal256Array,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
};
use arrow::datatypes::DataType;
use arrow_buffer::i256;

use crate::exec::node::aggregate::AggFunction;

use super::super::*;
use super::AggregateFunction;

type DistinctSet = HashSet<Vec<u8>>;

pub(super) struct MultiDistinctSumAgg;

fn set_slot(ptr: *mut u8) -> *mut *mut DistinctSet {
    ptr as *mut *mut DistinctSet
}

unsafe fn get_or_init_set<'a>(ptr: *mut u8) -> &'a mut DistinctSet {
    let slot = set_slot(ptr);
    let raw = unsafe { *slot };
    if raw.is_null() {
        let boxed: Box<DistinctSet> = Box::new(HashSet::new());
        let raw = Box::into_raw(boxed);
        unsafe {
            *slot = raw;
            &mut *raw
        }
    } else {
        unsafe { &mut *raw }
    }
}

unsafe fn take_set(ptr: *mut u8) -> Option<Box<DistinctSet>> {
    let slot = set_slot(ptr);
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

unsafe fn get_set<'a>(ptr: *mut u8) -> Option<&'a DistinctSet> {
    let slot = set_slot(ptr);
    let raw = unsafe { *slot };
    if raw.is_null() {
        None
    } else {
        unsafe { Some(&*raw) }
    }
}

fn encode_le<T: Copy>(v: T) -> Vec<u8> {
    unsafe {
        std::slice::from_raw_parts((&v as *const T) as *const u8, std::mem::size_of::<T>()).to_vec()
    }
}

fn serialize_set(set: &DistinctSet) -> Vec<u8> {
    let mut out = Vec::new();
    let count = set.len() as u32;
    out.extend_from_slice(&count.to_le_bytes());
    for v in set.iter() {
        let len = v.len() as u32;
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(v);
    }
    out
}

fn deserialize_set(bytes: &[u8]) -> Result<Vec<Vec<u8>>, String> {
    if bytes.len() < 4 {
        return Err("invalid distinct set encoding".to_string());
    }
    let mut pos = 0usize;
    let count = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
    pos += 4;
    let mut vals = Vec::with_capacity(count);
    for _ in 0..count {
        if pos + 4 > bytes.len() {
            return Err("invalid distinct set encoding".to_string());
        }
        let len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if pos + len > bytes.len() {
            return Err("invalid distinct set encoding".to_string());
        }
        vals.push(bytes[pos..pos + len].to_vec());
        pos += len;
    }
    Ok(vals)
}

fn sum_from_set(
    set: &DistinctSet,
    input_type: &DataType,
    output_type: &DataType,
) -> Result<ArrayRef, String> {
    if set.is_empty() {
        // Return null
        return match output_type {
            DataType::Int64 => Ok(std::sync::Arc::new(Int64Array::from(vec![None]))),
            DataType::Float64 => Ok(std::sync::Arc::new(Float64Array::from(vec![None]))),
            DataType::Decimal128(precision, scale) => {
                let array = Decimal128Array::from(vec![None])
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(|e| e.to_string())?;
                Ok(std::sync::Arc::new(array))
            }
            DataType::Decimal256(precision, scale) => {
                let array = Decimal256Array::from(vec![None])
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(|e| e.to_string())?;
                Ok(std::sync::Arc::new(array))
            }
            other => Err(format!(
                "multi_distinct_sum output type unsupported: {:?}",
                other
            )),
        };
    }

    match output_type {
        DataType::Int64 => {
            let mut sum: i128 = 0;
            for v in set {
                let value = match input_type {
                    DataType::Int8 => i8::from_le_bytes(v[..1].try_into().unwrap()) as i128,
                    DataType::Int16 => i16::from_le_bytes(v[..2].try_into().unwrap()) as i128,
                    DataType::Int32 => i32::from_le_bytes(v[..4].try_into().unwrap()) as i128,
                    DataType::Int64 => i64::from_le_bytes(v[..8].try_into().unwrap()) as i128,
                    DataType::Boolean => i8::from_le_bytes(v[..1].try_into().unwrap()) as i128,
                    other => {
                        return Err(format!(
                            "multi_distinct_sum unsupported input type for int output: {:?}",
                            other
                        ));
                    }
                };
                sum += value;
            }
            let sum_i64 =
                i64::try_from(sum).map_err(|_| "multi_distinct_sum overflow".to_string())?;
            Ok(std::sync::Arc::new(Int64Array::from(vec![Some(sum_i64)])))
        }
        DataType::Float64 => {
            let mut sum = 0.0f64;
            for v in set {
                let value = match input_type {
                    DataType::Float32 => f32::from_le_bytes(v[..4].try_into().unwrap()) as f64,
                    DataType::Float64 => f64::from_le_bytes(v[..8].try_into().unwrap()),
                    other => {
                        return Err(format!(
                            "multi_distinct_sum unsupported input type for float output: {:?}",
                            other
                        ));
                    }
                };
                sum += value;
            }
            Ok(std::sync::Arc::new(Float64Array::from(vec![Some(sum)])))
        }
        DataType::Decimal128(precision, scale) => {
            let mut sum: i128 = 0;
            for v in set {
                let value = match input_type {
                    DataType::Decimal128(_, _) => i128::from_le_bytes(v[..16].try_into().unwrap()),
                    other => {
                        return Err(format!(
                            "multi_distinct_sum unsupported input type for decimal output: {:?}",
                            other
                        ));
                    }
                };
                sum += value;
            }
            let array = Decimal128Array::from(vec![Some(sum)])
                .with_precision_and_scale(*precision, *scale)
                .map_err(|e| e.to_string())?;
            Ok(std::sync::Arc::new(array))
        }
        DataType::Decimal256(precision, scale) => {
            let mut sum = i256::ZERO;
            for v in set {
                let value = match input_type {
                    DataType::Decimal256(_, _) => i256::from_le_bytes(
                        v[..32]
                            .try_into()
                            .map_err(|_| "invalid Decimal256 distinct value bytes".to_string())?,
                    ),
                    other => {
                        return Err(format!(
                            "multi_distinct_sum unsupported input type for decimal output: {:?}",
                            other
                        ));
                    }
                };
                sum = sum
                    .checked_add(value)
                    .ok_or_else(|| "multi_distinct_sum decimal overflow".to_string())?;
            }
            let array = Decimal256Array::from(vec![Some(sum)])
                .with_precision_and_scale(*precision, *scale)
                .map_err(|e| e.to_string())?;
            Ok(std::sync::Arc::new(array))
        }
        other => Err(format!(
            "multi_distinct_sum output type unsupported: {:?}",
            other
        )),
    }
}

impl AggregateFunction for MultiDistinctSumAgg {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        input_type: Option<&DataType>,
        input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        let data_type =
            input_type.ok_or_else(|| "multi_distinct_sum input type missing".to_string())?;

        if input_is_intermediate {
            let sig = super::super::agg_type_signature(func)
                .ok_or_else(|| "multi_distinct_sum type signature missing".to_string())?;
            let output_type = sig
                .output_type
                .as_ref()
                .ok_or_else(|| "multi_distinct_sum output type signature missing".to_string())?;
            return Ok(AggSpec {
                kind: AggKind::MultiDistinctSum,
                output_type: output_type.clone(),
                intermediate_type: data_type.clone(),
                input_arg_type: sig.input_arg_type.clone(),
                count_all: false,
            });
        }

        let output_type = match data_type {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Boolean => DataType::Int64,
            DataType::Float32 | DataType::Float64 => DataType::Float64,
            DataType::Decimal128(precision, scale) => DataType::Decimal128(*precision, *scale),
            DataType::Decimal256(precision, scale) => DataType::Decimal256(*precision, *scale),
            other => {
                return Err(format!(
                    "multi_distinct_sum unsupported input type: {:?}",
                    other
                ));
            }
        };
        Ok(AggSpec {
            kind: AggKind::MultiDistinctSum,
            output_type,
            intermediate_type: DataType::Binary,
            input_arg_type: Some(data_type.clone()),
            count_all: false,
        })
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        match kind {
            AggKind::MultiDistinctSum => (
                std::mem::size_of::<*mut DistinctSet>(),
                std::mem::align_of::<*mut DistinctSet>(),
            ),
            other => unreachable!("unexpected kind for multi_distinct_sum: {:?}", other),
        }
    }

    fn build_input_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "multi_distinct_sum input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn build_merge_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "multi_distinct_sum merge input missing".to_string())?;
        let bin = arr
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| "failed to downcast to BinaryArray".to_string())?;
        Ok(AggInputView::Binary(bin))
    }

    fn init_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::write(ptr as *mut *mut DistinctSet, std::ptr::null_mut());
        }
    }

    fn drop_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            let _ = take_set(ptr);
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
            return Err("multi_distinct_sum batch input type mismatch".to_string());
        };
        for (row, &base) in state_ptrs.iter().enumerate() {
            if array.is_null(row) {
                continue;
            }
            let encoded = match array.data_type() {
                DataType::Int8 => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<Int8Array>()
                        .ok_or_else(|| "failed to downcast to Int8Array".to_string())?;
                    encode_le(arr.value(row))
                }
                DataType::Int16 => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<Int16Array>()
                        .ok_or_else(|| "failed to downcast to Int16Array".to_string())?;
                    encode_le(arr.value(row))
                }
                DataType::Int32 => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .ok_or_else(|| "failed to downcast to Int32Array".to_string())?;
                    encode_le(arr.value(row))
                }
                DataType::Int64 => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| "failed to downcast to Int64Array".to_string())?;
                    encode_le(arr.value(row))
                }
                DataType::Boolean => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or_else(|| "failed to downcast to BooleanArray".to_string())?;
                    encode_le(if arr.value(row) { 1u8 } else { 0u8 })
                }
                DataType::Float32 => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<Float32Array>()
                        .ok_or_else(|| "failed to downcast to Float32Array".to_string())?;
                    encode_le(arr.value(row))
                }
                DataType::Float64 => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .ok_or_else(|| "failed to downcast to Float64Array".to_string())?;
                    encode_le(arr.value(row))
                }
                DataType::Decimal128(_, _) => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<Decimal128Array>()
                        .ok_or_else(|| "failed to downcast to Decimal128Array".to_string())?;
                    encode_le(arr.value(row))
                }
                DataType::Decimal256(_, _) => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<Decimal256Array>()
                        .ok_or_else(|| "failed to downcast to Decimal256Array".to_string())?;
                    arr.value(row).to_le_bytes().to_vec()
                }
                other => {
                    return Err(format!(
                        "multi_distinct_sum unsupported input type: {:?}",
                        other
                    ));
                }
            };
            let set = unsafe { get_or_init_set((base as *mut u8).add(offset)) };
            set.insert(encoded);
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
        let AggInputView::Binary(arr) = input else {
            return Err("multi_distinct_sum merge input type mismatch".to_string());
        };
        for (row, &base) in state_ptrs.iter().enumerate() {
            if arr.is_null(row) {
                continue;
            }
            let vals = deserialize_set(arr.value(row))?;
            let set = unsafe { get_or_init_set((base as *mut u8).add(offset)) };
            for v in vals {
                set.insert(v);
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
                let set = unsafe { get_set((base as *mut u8).add(offset)) };
                let Some(set) = set else {
                    builder.append_null();
                    continue;
                };
                if set.is_empty() {
                    builder.append_null();
                } else {
                    builder.append_value(&serialize_set(set));
                }
            }
            return Ok(std::sync::Arc::new(builder.finish()));
        }

        let input_type = spec
            .input_arg_type
            .as_ref()
            .ok_or_else(|| "multi_distinct_sum input_arg_type missing".to_string())?;

        let mut arrays = Vec::with_capacity(group_states.len());
        for &base in group_states {
            let set = unsafe { get_set((base as *mut u8).add(offset)) };
            let array = if let Some(set) = set {
                sum_from_set(set, input_type, &spec.output_type)?
            } else {
                sum_from_set(&HashSet::new(), input_type, &spec.output_type)?
            };
            arrays.push(array);
        }

        // Each array is a single-value array. Concatenate into a batch.
        let values: Vec<&dyn arrow::array::Array> = arrays.iter().map(|a| a.as_ref()).collect();
        // Use concat to combine single-value arrays.
        arrow::compute::kernels::concat::concat(&values).map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::DataType;
    use std::mem::MaybeUninit;

    #[test]
    fn test_multi_distinct_sum_spec() {
        let func = AggFunction {
            name: "multi_distinct_sum".to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(crate::exec::node::aggregate::AggTypeSignature {
                intermediate_type: Some(DataType::Binary),
                output_type: Some(DataType::Int64),
                input_arg_type: Some(DataType::Int64),
            }),
        };
        let spec = MultiDistinctSumAgg
            .build_spec_from_type(&func, Some(&DataType::Int64), false)
            .unwrap();
        assert!(matches!(spec.kind, AggKind::MultiDistinctSum));
    }

    #[test]
    fn test_multi_distinct_sum_updates() {
        let func = AggFunction {
            name: "multi_distinct_sum".to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(crate::exec::node::aggregate::AggTypeSignature {
                intermediate_type: Some(DataType::Binary),
                output_type: Some(DataType::Int64),
                input_arg_type: Some(DataType::Int64),
            }),
        };
        let spec = MultiDistinctSumAgg
            .build_spec_from_type(&func, Some(&DataType::Int64), false)
            .unwrap();

        let values = Arc::new(Int64Array::from(vec![1, 2, 2, 3])) as ArrayRef;
        let input = AggInputView::Any(&values);

        let mut state = MaybeUninit::<*mut DistinctSet>::uninit();
        MultiDistinctSumAgg.init_state(&spec, state.as_mut_ptr() as *mut u8);
        let state_ptr = state.as_mut_ptr() as AggStatePtr;
        let state_ptrs = vec![state_ptr; 4];
        MultiDistinctSumAgg
            .update_batch(&spec, 0, &state_ptrs, &input)
            .unwrap();
        let out = MultiDistinctSumAgg
            .build_array(&spec, 0, &[state_ptr], false)
            .unwrap();
        MultiDistinctSumAgg.drop_state(&spec, state.as_mut_ptr() as *mut u8);

        let out_arr = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(out_arr.value(0), 6);
    }
}
