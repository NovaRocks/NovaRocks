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

pub(super) struct CountAgg;

fn count_fallback(count_all: bool) -> AggSpec {
    AggSpec {
        kind: AggKind::Count,
        output_type: DataType::Int64,
        intermediate_type: DataType::Int64,
        input_arg_type: None,
        count_all,
    }
}

impl AggregateFunction for CountAgg {
    fn build_spec_from_type(
        &self,
        _func: &AggFunction,
        input_type: Option<&DataType>,
        _input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        Ok(count_fallback(input_type.is_none()))
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        match kind {
            AggKind::Count => (std::mem::size_of::<i64>(), std::mem::align_of::<i64>()),
            other => unreachable!("unexpected kind for count: {:?}", other),
        }
    }

    fn build_input_view<'a>(
        &self,
        spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        if spec.count_all {
            Ok(AggInputView::None)
        } else {
            let arr = array
                .as_ref()
                .ok_or_else(|| "count input missing".to_string())?;
            Ok(AggInputView::Any(arr))
        }
    }

    fn build_merge_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "count input missing".to_string())?;
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
        spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        match input {
            AggInputView::None => {
                for &base in state_ptrs {
                    let slot = unsafe { &mut *((base as *mut u8).add(offset) as *mut i64) };
                    *slot += 1;
                }
                Ok(())
            }
            AggInputView::Any(array) => {
                if spec.count_all {
                    for &base in state_ptrs {
                        let slot = unsafe { &mut *((base as *mut u8).add(offset) as *mut i64) };
                        *slot += 1;
                    }
                    return Ok(());
                }
                if array.null_count() == 0 {
                    for &base in state_ptrs {
                        let slot = unsafe { &mut *((base as *mut u8).add(offset) as *mut i64) };
                        *slot += 1;
                    }
                } else {
                    for (row, &base) in state_ptrs.iter().enumerate() {
                        if !array.is_null(row) {
                            let slot = unsafe { &mut *((base as *mut u8).add(offset) as *mut i64) };
                            *slot += 1;
                        }
                    }
                }
                Ok(())
            }
            _ => Err("count batch input type mismatch".to_string()),
        }
    }

    fn merge_batch(
        &self,
        _spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        match input {
            AggInputView::Int(view) => match view {
                IntArrayView::Int64(arr) => {
                    let vals = arr.values();
                    if arr.null_count() == 0 {
                        for (row, &base) in state_ptrs.iter().enumerate() {
                            let slot = unsafe { &mut *((base as *mut u8).add(offset) as *mut i64) };
                            *slot += vals[row];
                        }
                    } else {
                        for (row, &base) in state_ptrs.iter().enumerate() {
                            if !arr.is_null(row) {
                                let slot =
                                    unsafe { &mut *((base as *mut u8).add(offset) as *mut i64) };
                                *slot += vals[row];
                            }
                        }
                    }
                    Ok(())
                }
                IntArrayView::Int32(arr) => {
                    let vals = arr.values();
                    if arr.null_count() == 0 {
                        for (row, &base) in state_ptrs.iter().enumerate() {
                            let slot = unsafe { &mut *((base as *mut u8).add(offset) as *mut i64) };
                            *slot += vals[row] as i64;
                        }
                    } else {
                        for (row, &base) in state_ptrs.iter().enumerate() {
                            if !arr.is_null(row) {
                                let slot =
                                    unsafe { &mut *((base as *mut u8).add(offset) as *mut i64) };
                                *slot += vals[row] as i64;
                            }
                        }
                    }
                    Ok(())
                }
                IntArrayView::Int16(arr) => {
                    let vals = arr.values();
                    if arr.null_count() == 0 {
                        for (row, &base) in state_ptrs.iter().enumerate() {
                            let slot = unsafe { &mut *((base as *mut u8).add(offset) as *mut i64) };
                            *slot += vals[row] as i64;
                        }
                    } else {
                        for (row, &base) in state_ptrs.iter().enumerate() {
                            if !arr.is_null(row) {
                                let slot =
                                    unsafe { &mut *((base as *mut u8).add(offset) as *mut i64) };
                                *slot += vals[row] as i64;
                            }
                        }
                    }
                    Ok(())
                }
                IntArrayView::Int8(arr) => {
                    let vals = arr.values();
                    if arr.null_count() == 0 {
                        for (row, &base) in state_ptrs.iter().enumerate() {
                            let slot = unsafe { &mut *((base as *mut u8).add(offset) as *mut i64) };
                            *slot += vals[row] as i64;
                        }
                    } else {
                        for (row, &base) in state_ptrs.iter().enumerate() {
                            if !arr.is_null(row) {
                                let slot =
                                    unsafe { &mut *((base as *mut u8).add(offset) as *mut i64) };
                                *slot += vals[row] as i64;
                            }
                        }
                    }
                    Ok(())
                }
            },
            _ => Err("count merge batch input type mismatch".to_string()),
        }
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
            let value = unsafe { *((base as *mut u8).add(offset) as *const i64) };
            builder.append_value(value);
        }
        Ok(Arc::new(builder.finish()))
    }
}
