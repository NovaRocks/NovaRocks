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
use arrow::array::ArrayRef;
use arrow::datatypes::DataType;

use crate::exec::node::aggregate::AggFunction;

use super::super::*;
use super::AggregateFunction;
use super::common::{AggScalarValue, build_scalar_array, scalar_from_array};

pub(super) struct AnyValueAgg;

#[derive(Clone, Debug)]
struct AnyValueState {
    has_value: bool,
    value: Option<AggScalarValue>,
}

impl Default for AnyValueState {
    fn default() -> Self {
        Self {
            has_value: false,
            value: None,
        }
    }
}

impl AggregateFunction for AnyValueAgg {
    fn build_spec_from_type(
        &self,
        _func: &AggFunction,
        input_type: Option<&DataType>,
        _input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        let data_type = input_type.ok_or_else(|| "any_value input type missing".to_string())?;
        Ok(AggSpec {
            kind: AggKind::AnyValue,
            output_type: data_type.clone(),
            intermediate_type: data_type.clone(),
            input_arg_type: None,
            count_all: false,
        })
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        match kind {
            AggKind::AnyValue => (
                std::mem::size_of::<AnyValueState>(),
                std::mem::align_of::<AnyValueState>(),
            ),
            other => unreachable!("unexpected kind for any_value: {:?}", other),
        }
    }

    fn build_input_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "any_value input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn build_merge_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "any_value merge input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn init_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::write(ptr as *mut AnyValueState, AnyValueState::default());
        }
    }

    fn drop_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::drop_in_place(ptr as *mut AnyValueState);
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
            return Err("any_value batch input type mismatch".to_string());
        };
        for (row, &base) in state_ptrs.iter().enumerate() {
            let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut AnyValueState) };
            if state.has_value {
                continue;
            }
            let value = scalar_from_array(array, row)?;
            if value.is_some() {
                state.has_value = true;
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
        // any_value merge is identical to update
        self.update_batch(spec, offset, state_ptrs, input)
    }

    fn build_array(
        &self,
        spec: &AggSpec,
        offset: usize,
        group_states: &[AggStatePtr],
        _output_intermediate: bool,
    ) -> Result<ArrayRef, String> {
        let mut values = Vec::with_capacity(group_states.len());
        for &base in group_states {
            let state = unsafe { &*((base as *mut u8).add(offset) as *const AnyValueState) };
            if state.has_value {
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
    use arrow::array::Int64Array;
    use arrow::datatypes::DataType;
    use std::mem::MaybeUninit;

    #[test]
    fn test_any_value_spec_int() {
        let func = AggFunction {
            name: "any_value".to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(crate::exec::node::aggregate::AggTypeSignature {
                intermediate_type: None,
                output_type: Some(DataType::Int64),
                input_arg_type: Some(DataType::Int64),
            }),
        };
        let spec = AnyValueAgg
            .build_spec_from_type(&func, Some(&DataType::Int64), false)
            .unwrap();
        assert!(matches!(spec.kind, AggKind::AnyValue));
        assert_eq!(spec.output_type, DataType::Int64);
    }

    #[test]
    fn test_any_value_picks_first_non_null() {
        let func = AggFunction {
            name: "any_value".to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(crate::exec::node::aggregate::AggTypeSignature {
                intermediate_type: None,
                output_type: Some(DataType::Int64),
                input_arg_type: Some(DataType::Int64),
            }),
        };
        let spec = AnyValueAgg
            .build_spec_from_type(&func, Some(&DataType::Int64), false)
            .unwrap();

        let values = Arc::new(Int64Array::from(vec![None, Some(10), Some(20)])) as ArrayRef;
        let input = AggInputView::Any(&values);
        let mut state = MaybeUninit::<AnyValueState>::uninit();
        AnyValueAgg.init_state(&spec, state.as_mut_ptr() as *mut u8);
        let state_ptr = state.as_mut_ptr() as AggStatePtr;
        let state_ptrs = vec![state_ptr; 3];

        AnyValueAgg
            .update_batch(&spec, 0, &state_ptrs, &input)
            .unwrap();
        let out = AnyValueAgg
            .build_array(&spec, 0, &[state_ptr], false)
            .unwrap();
        AnyValueAgg.drop_state(&spec, state.as_mut_ptr() as *mut u8);

        let out_arr = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(out_arr.value(0), 10);
    }
}
