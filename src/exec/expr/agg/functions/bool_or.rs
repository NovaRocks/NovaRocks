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
use super::common::build_bool_array;

pub(super) struct BoolOrAgg;

impl AggregateFunction for BoolOrAgg {
    fn build_spec_from_type(
        &self,
        _func: &AggFunction,
        input_type: Option<&DataType>,
        _input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        let data_type = input_type.ok_or_else(|| "bool_or input type missing".to_string())?;
        if !matches!(data_type, DataType::Boolean) {
            return Err(format!(
                "bool_or expects boolean input, got {:?}",
                data_type
            ));
        }
        Ok(AggSpec {
            kind: AggKind::BoolOr,
            output_type: DataType::Boolean,
            intermediate_type: DataType::Boolean,
            input_arg_type: None,
            count_all: false,
        })
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        match kind {
            AggKind::BoolOr => (
                std::mem::size_of::<BoolState>(),
                std::mem::align_of::<BoolState>(),
            ),
            other => unreachable!("unexpected kind for bool_or: {:?}", other),
        }
    }

    fn build_input_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "bool_or input missing".to_string())?;
        let arr = arr
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| "failed to downcast to BooleanArray".to_string())?;
        Ok(AggInputView::Bool(arr))
    }

    fn build_merge_view<'a>(
        &self,
        spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        self.build_input_view(spec, array)
    }

    fn init_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::write(
                ptr as *mut BoolState,
                BoolState {
                    has_value: false,
                    value: false,
                },
            );
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
            return Err("bool_or batch input type mismatch".to_string());
        };
        for (row, &base) in state_ptrs.iter().enumerate() {
            // bool_or over raw input rows should treat NULL as false while preserving
            // the fact that the group has seen at least one row.
            let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut BoolState) };
            state.has_value = true;
            if arr.is_null(row) {
                continue;
            }
            state.value = state.value || arr.value(row);
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
        let AggInputView::Bool(arr) = input else {
            return Err("bool_or merge input type mismatch".to_string());
        };
        for (row, &base) in state_ptrs.iter().enumerate() {
            if arr.is_null(row) {
                continue;
            }
            let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut BoolState) };
            state.has_value = true;
            state.value = state.value || arr.value(row);
        }
        let _ = spec;
        Ok(())
    }

    fn build_array(
        &self,
        _spec: &AggSpec,
        offset: usize,
        group_states: &[AggStatePtr],
        _output_intermediate: bool,
    ) -> Result<ArrayRef, String> {
        build_bool_array(offset, group_states)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, BooleanArray};
    use arrow::datatypes::DataType;
    use std::mem::MaybeUninit;
    use std::sync::Arc;

    #[test]
    fn test_bool_or_spec() {
        let func = AggFunction {
            name: "bool_or".to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(crate::exec::node::aggregate::AggTypeSignature {
                intermediate_type: None,
                output_type: Some(DataType::Boolean),
                input_arg_type: Some(DataType::Boolean),
            }),
        };
        let spec = BoolOrAgg
            .build_spec_from_type(&func, Some(&DataType::Boolean), false)
            .unwrap();
        assert!(matches!(spec.kind, AggKind::BoolOr));
    }

    #[test]
    fn test_bool_or_updates() {
        let func = AggFunction {
            name: "bool_or".to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(crate::exec::node::aggregate::AggTypeSignature {
                intermediate_type: None,
                output_type: Some(DataType::Boolean),
                input_arg_type: Some(DataType::Boolean),
            }),
        };
        let spec = BoolOrAgg
            .build_spec_from_type(&func, Some(&DataType::Boolean), false)
            .unwrap();

        let values = Arc::new(BooleanArray::from(vec![Some(false), Some(true), None]));
        let input = AggInputView::Bool(values.as_ref());

        let mut state = MaybeUninit::<BoolState>::uninit();
        BoolOrAgg.init_state(&spec, state.as_mut_ptr() as *mut u8);
        let state_ptr = state.as_mut_ptr() as AggStatePtr;
        let state_ptrs = vec![state_ptr; 3];
        BoolOrAgg
            .update_batch(&spec, 0, &state_ptrs, &input)
            .unwrap();
        let out = BoolOrAgg
            .build_array(&spec, 0, &[state_ptr], false)
            .unwrap();
        BoolOrAgg.drop_state(&spec, state.as_mut_ptr() as *mut u8);

        let out_arr = out.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(out_arr.value(0), true);
    }

    #[test]
    fn test_boolor_agg_alias() {
        let func = AggFunction {
            name: "boolor_agg".to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(crate::exec::node::aggregate::AggTypeSignature {
                intermediate_type: None,
                output_type: Some(DataType::Boolean),
                input_arg_type: Some(DataType::Boolean),
            }),
        };
        let spec = super::build_spec_from_type(&func, Some(&DataType::Boolean), false).unwrap();
        assert!(matches!(spec.kind, AggKind::BoolOr));
    }

    #[test]
    fn test_bool_or_all_null_rows_return_false() {
        let func = AggFunction {
            name: "bool_or".to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(crate::exec::node::aggregate::AggTypeSignature {
                intermediate_type: None,
                output_type: Some(DataType::Boolean),
                input_arg_type: Some(DataType::Boolean),
            }),
        };
        let spec = BoolOrAgg
            .build_spec_from_type(&func, Some(&DataType::Boolean), false)
            .unwrap();

        let values = Arc::new(BooleanArray::from(vec![None, None]));
        let input = AggInputView::Bool(values.as_ref());

        let mut state = MaybeUninit::<BoolState>::uninit();
        BoolOrAgg.init_state(&spec, state.as_mut_ptr() as *mut u8);
        let state_ptr = state.as_mut_ptr() as AggStatePtr;
        let state_ptrs = vec![state_ptr; 2];
        BoolOrAgg
            .update_batch(&spec, 0, &state_ptrs, &input)
            .unwrap();

        let out = BoolOrAgg
            .build_array(&spec, 0, &[state_ptr], false)
            .unwrap();
        BoolOrAgg.drop_state(&spec, state.as_mut_ptr() as *mut u8);

        let out_arr = out.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(!out_arr.is_null(0));
        assert!(!out_arr.value(0));
    }

    #[test]
    fn test_bool_or_merge_null_intermediate_keeps_empty_state() {
        let func = AggFunction {
            name: "bool_or".to_string(),
            inputs: vec![],
            input_is_intermediate: true,
            types: Some(crate::exec::node::aggregate::AggTypeSignature {
                intermediate_type: Some(DataType::Boolean),
                output_type: Some(DataType::Boolean),
                input_arg_type: Some(DataType::Boolean),
            }),
        };
        let spec = BoolOrAgg
            .build_spec_from_type(&func, Some(&DataType::Boolean), true)
            .unwrap();

        let values = Arc::new(BooleanArray::from(vec![None]));
        let input = AggInputView::Bool(values.as_ref());

        let mut state = MaybeUninit::<BoolState>::uninit();
        BoolOrAgg.init_state(&spec, state.as_mut_ptr() as *mut u8);
        let state_ptr = state.as_mut_ptr() as AggStatePtr;
        BoolOrAgg
            .merge_batch(&spec, 0, &[state_ptr], &input)
            .unwrap();

        let out = BoolOrAgg
            .build_array(&spec, 0, &[state_ptr], false)
            .unwrap();
        BoolOrAgg.drop_state(&spec, state.as_mut_ptr() as *mut u8);

        let out_arr = out.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(out_arr.is_null(0));
    }
}
