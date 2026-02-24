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
use arrow::array::{ArrayRef, BooleanArray};
use arrow::datatypes::DataType;

use crate::exec::node::aggregate::AggFunction;

use super::super::*;
use super::AggregateFunction;

pub(super) struct CountIfAgg;

fn is_supported_merge_input_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    )
}

impl AggregateFunction for CountIfAgg {
    fn build_spec_from_type(
        &self,
        _func: &AggFunction,
        input_type: Option<&DataType>,
        input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        let data_type = input_type.ok_or_else(|| "count_if input type missing".to_string())?;
        if input_is_intermediate {
            if !is_supported_merge_input_type(data_type) {
                return Err(format!(
                    "count_if expects integer intermediate input, got {:?}",
                    data_type
                ));
            }
        } else if !matches!(data_type, DataType::Boolean) {
            return Err(format!(
                "count_if expects boolean input, got {:?}",
                data_type
            ));
        }
        Ok(AggSpec {
            kind: AggKind::CountIf,
            output_type: DataType::Int64,
            intermediate_type: DataType::Int64,
            input_arg_type: None,
            count_all: false,
        })
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        match kind {
            AggKind::CountIf => (std::mem::size_of::<i64>(), std::mem::align_of::<i64>()),
            other => unreachable!("unexpected kind for count_if: {:?}", other),
        }
    }

    fn build_input_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "count_if input missing".to_string())?;
        let arr = arr
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| "failed to downcast to BooleanArray".to_string())?;
        Ok(AggInputView::Bool(arr))
    }

    fn build_merge_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "count_if merge input missing".to_string())?;
        Ok(AggInputView::Int(IntArrayView::new(arr)?))
    }

    fn init_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::write(ptr as *mut i64, 0);
        }
    }

    fn drop_state(&self, _spec: &AggSpec, _ptr: *mut u8) {}

    fn update_batch(
        &self,
        _spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        let AggInputView::Bool(arr) = input else {
            return Err("count_if batch input type mismatch".to_string());
        };
        for (row, &base) in state_ptrs.iter().enumerate() {
            if arr.is_null(row) {
                continue;
            }
            if arr.value(row) {
                let slot = unsafe { &mut *((base as *mut u8).add(offset) as *mut i64) };
                *slot += 1;
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
        let AggInputView::Int(view) = input else {
            return Err("count_if merge input type mismatch".to_string());
        };
        for (row, &base) in state_ptrs.iter().enumerate() {
            let Some(value) = view.value_at(row) else {
                continue;
            };
            let slot = unsafe { &mut *((base as *mut u8).add(offset) as *mut i64) };
            *slot += value;
        }
        Ok(())
    }

    fn build_array(
        &self,
        _spec: &AggSpec,
        offset: usize,
        group_states: &[AggStatePtr],
        _output_intermediate: bool,
    ) -> Result<ArrayRef, String> {
        let mut builder = Int64Builder::new();
        for &base in group_states {
            let state = unsafe { &*((base as *mut u8).add(offset) as *const i64) };
            builder.append_value(*state);
        }
        Ok(std::sync::Arc::new(builder.finish()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::BooleanArray;
    use arrow::datatypes::DataType;
    use std::mem::MaybeUninit;

    #[test]
    fn test_count_if_spec() {
        let func = AggFunction {
            name: "count_if".to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(crate::exec::node::aggregate::AggTypeSignature {
                intermediate_type: None,
                output_type: Some(DataType::Int64),
                input_arg_type: Some(DataType::Boolean),
            }),
        };
        let spec = CountIfAgg
            .build_spec_from_type(&func, Some(&DataType::Boolean), false)
            .unwrap();
        assert!(matches!(spec.kind, AggKind::CountIf));
    }

    #[test]
    fn test_count_if_updates() {
        let func = AggFunction {
            name: "count_if".to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(crate::exec::node::aggregate::AggTypeSignature {
                intermediate_type: None,
                output_type: Some(DataType::Int64),
                input_arg_type: Some(DataType::Boolean),
            }),
        };
        let spec = CountIfAgg
            .build_spec_from_type(&func, Some(&DataType::Boolean), false)
            .unwrap();

        let values = Arc::new(BooleanArray::from(vec![
            Some(true),
            Some(false),
            Some(true),
        ]));
        let input = AggInputView::Bool(values.as_ref());

        let mut state = MaybeUninit::<i64>::uninit();
        CountIfAgg.init_state(&spec, state.as_mut_ptr() as *mut u8);
        let state_ptr = state.as_mut_ptr() as AggStatePtr;
        let state_ptrs = vec![state_ptr; 3];
        CountIfAgg
            .update_batch(&spec, 0, &state_ptrs, &input)
            .unwrap();
        let out = CountIfAgg
            .build_array(&spec, 0, &[state_ptr], false)
            .unwrap();
        CountIfAgg.drop_state(&spec, state.as_mut_ptr() as *mut u8);

        let out_arr = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(out_arr.value(0), 2);
    }

    #[test]
    fn test_count_if_merge_spec_accepts_int64() {
        let func = AggFunction {
            name: "count_if".to_string(),
            inputs: vec![],
            input_is_intermediate: true,
            types: Some(crate::exec::node::aggregate::AggTypeSignature {
                intermediate_type: Some(DataType::Int64),
                output_type: Some(DataType::Int64),
                input_arg_type: Some(DataType::Int8),
            }),
        };
        let spec = CountIfAgg
            .build_spec_from_type(&func, Some(&DataType::Int64), true)
            .unwrap();
        assert!(matches!(spec.kind, AggKind::CountIf));
    }
}
