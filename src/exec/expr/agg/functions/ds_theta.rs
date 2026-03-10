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

pub(super) struct DsThetaAgg;

fn unsupported_message(name: &str) -> String {
    format!(
        "unsupported agg function: {} (Theta sketches require the removed Apache DataSketches C++ backend)",
        name
    )
}

impl AggregateFunction for DsThetaAgg {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        _input_type: Option<&DataType>,
        _input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        Err(unsupported_message(canonical_agg_name(func.name.as_str())))
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        unreachable!("unexpected ds_theta agg kind: {:?}", kind)
    }

    fn build_input_view<'a>(
        &self,
        _spec: &AggSpec,
        _array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        Err(unsupported_message("ds_theta_count_distinct"))
    }

    fn build_merge_view<'a>(
        &self,
        _spec: &AggSpec,
        _array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        Err(unsupported_message("ds_theta_count_distinct"))
    }

    fn init_state(&self, _spec: &AggSpec, _ptr: *mut u8) {}

    fn drop_state(&self, _spec: &AggSpec, _ptr: *mut u8) {}

    fn update_batch(
        &self,
        _spec: &AggSpec,
        _offset: usize,
        _state_ptrs: &[AggStatePtr],
        _input: &AggInputView,
    ) -> Result<(), String> {
        Err(unsupported_message("ds_theta_count_distinct"))
    }

    fn merge_batch(
        &self,
        _spec: &AggSpec,
        _offset: usize,
        _state_ptrs: &[AggStatePtr],
        _input: &AggInputView,
    ) -> Result<(), String> {
        Err(unsupported_message("ds_theta_count_distinct"))
    }

    fn build_array(
        &self,
        _spec: &AggSpec,
        _offset: usize,
        _group_states: &[AggStatePtr],
        _output_intermediate: bool,
    ) -> Result<ArrayRef, String> {
        Err(unsupported_message("ds_theta_count_distinct"))
    }
}

fn canonical_agg_name(name: &str) -> &str {
    name.split_once('|').map(|(base, _)| base).unwrap_or(name)
}
