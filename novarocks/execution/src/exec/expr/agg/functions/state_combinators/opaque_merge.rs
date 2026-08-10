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

//! Opaque Binary-state merge aggregate functions.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BinaryArray, BinaryBuilder};
use arrow::datatypes::DataType;

use crate::exec::node::aggregate::AggFunction;

use super::super::{AggInputView, AggKind, AggSpec, AggStatePtr, AggregateFunction};

type StateUnionFn = fn(&[u8], &[u8]) -> Result<Vec<u8>, String>;

pub(in crate::exec::expr::agg::functions) struct OpaqueStateMergeAgg {
    name: &'static str,
    kind: AggKind,
    union: StateUnionFn,
}

#[derive(Default)]
struct OpaqueStateMergeState {
    state: Vec<u8>,
}

impl OpaqueStateMergeAgg {
    pub(in crate::exec::expr::agg::functions) const fn new(
        name: &'static str,
        kind: AggKind,
        union: StateUnionFn,
    ) -> Self {
        Self { name, kind, union }
    }
}

impl AggregateFunction for OpaqueStateMergeAgg {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        input_type: Option<&DataType>,
        _input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        let input_type = input_type.ok_or_else(|| format!("{} input type missing", self.name))?;
        if input_type != &DataType::Binary {
            return Err(format!(
                "{} input must be Binary, got {:?}",
                self.name, input_type
            ));
        }

        Ok(AggSpec {
            kind: self.kind.clone(),
            output_type: DataType::Binary,
            intermediate_type: DataType::Binary,
            input_arg_type: func.types.as_ref().and_then(|t| t.input_arg_type.clone()),
            count_all: false,
        })
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        if !same_kind(kind, &self.kind) {
            unreachable!("unexpected kind for {}: {:?}", self.name, kind);
        }
        (
            std::mem::size_of::<OpaqueStateMergeState>(),
            std::mem::align_of::<OpaqueStateMergeState>(),
        )
    }

    fn build_input_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        build_binary_state_view(self.name, array)
    }

    fn build_merge_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        build_binary_state_view(self.name, array)
    }

    fn init_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::write(
                ptr as *mut OpaqueStateMergeState,
                OpaqueStateMergeState::default(),
            );
        }
    }

    fn drop_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::drop_in_place(ptr as *mut OpaqueStateMergeState);
        }
    }

    fn update_batch(
        &self,
        spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        self.merge_binary_states(spec, offset, state_ptrs, input)
    }

    fn merge_batch(
        &self,
        spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        self.merge_binary_states(spec, offset, state_ptrs, input)
    }

    fn build_array(
        &self,
        spec: &AggSpec,
        offset: usize,
        group_states: &[AggStatePtr],
        _output_intermediate: bool,
    ) -> Result<ArrayRef, String> {
        if !same_kind(&spec.kind, &self.kind) {
            return Err(format!("{} build array kind mismatch", self.name));
        }
        let mut builder = BinaryBuilder::new();
        for &base in group_states {
            let state = unsafe { &*state_slot(base, offset) };
            builder.append_value(&state.state);
        }
        Ok(Arc::new(builder.finish()))
    }
}

impl OpaqueStateMergeAgg {
    fn merge_binary_states(
        &self,
        spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        if !same_kind(&spec.kind, &self.kind) {
            return Err(format!("{} merge kind mismatch", self.name));
        }
        let AggInputView::Binary(array) = input else {
            return Err(format!("{} merge input type mismatch", self.name));
        };
        for (row, &base) in state_ptrs.iter().enumerate() {
            if array.is_null(row) {
                continue;
            }
            let state = unsafe { &mut *state_slot(base, offset) };
            state.state = (self.union)(&state.state, array.value(row))?;
        }
        Ok(())
    }
}

fn build_binary_state_view<'a>(
    name: &str,
    array: &'a Option<ArrayRef>,
) -> Result<AggInputView<'a>, String> {
    let arr = array
        .as_ref()
        .ok_or_else(|| format!("{name} input missing"))?;
    let binary = arr
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| format!("{name} input must be BinaryArray"))?;
    Ok(AggInputView::Binary(binary))
}

fn state_slot(base: AggStatePtr, offset: usize) -> *mut OpaqueStateMergeState {
    unsafe { (base as *mut u8).add(offset) as *mut OpaqueStateMergeState }
}

fn same_kind(left: &AggKind, right: &AggKind) -> bool {
    std::mem::discriminant(left) == std::mem::discriminant(right)
}
