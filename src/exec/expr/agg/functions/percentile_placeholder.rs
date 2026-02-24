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
use arrow::array::{ArrayRef, new_null_array};
use arrow::datatypes::DataType;

use crate::exec::node::aggregate::AggFunction;

use super::super::*;
use super::AggregateFunction;

pub(super) struct PercentilePlaceholderAgg;

impl AggregateFunction for PercentilePlaceholderAgg {
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
        let output_type = sig
            .output_type
            .as_ref()
            .cloned()
            .or_else(|| input_type.cloned())
            .unwrap_or(DataType::Binary);
        let intermediate_type = sig
            .intermediate_type
            .as_ref()
            .cloned()
            .unwrap_or_else(|| output_type.clone());
        Ok(AggSpec {
            kind: AggKind::PercentilePlaceholder,
            output_type,
            intermediate_type,
            input_arg_type: sig.input_arg_type.clone(),
            count_all: false,
        })
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        match kind {
            AggKind::PercentilePlaceholder => {
                (std::mem::size_of::<u8>(), std::mem::align_of::<u8>())
            }
            other => unreachable!("unexpected kind for percentile placeholder: {:?}", other),
        }
    }

    fn build_input_view<'a>(
        &self,
        _spec: &AggSpec,
        _array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        Ok(AggInputView::None)
    }

    fn build_merge_view<'a>(
        &self,
        _spec: &AggSpec,
        _array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        Ok(AggInputView::None)
    }

    fn init_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            *ptr = 0;
        }
    }

    fn drop_state(&self, _spec: &AggSpec, _ptr: *mut u8) {}

    fn update_batch(
        &self,
        _spec: &AggSpec,
        _offset: usize,
        _state_ptrs: &[AggStatePtr],
        _input: &AggInputView,
    ) -> Result<(), String> {
        Ok(())
    }

    fn merge_batch(
        &self,
        _spec: &AggSpec,
        _offset: usize,
        _state_ptrs: &[AggStatePtr],
        _input: &AggInputView,
    ) -> Result<(), String> {
        Ok(())
    }

    fn build_array(
        &self,
        spec: &AggSpec,
        _offset: usize,
        group_states: &[AggStatePtr],
        output_intermediate: bool,
    ) -> Result<ArrayRef, String> {
        let output_type = if output_intermediate {
            &spec.intermediate_type
        } else {
            &spec.output_type
        };
        Ok(new_null_array(output_type, group_states.len()))
    }
}
