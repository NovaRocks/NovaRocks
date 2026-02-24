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
use arrow::array::builder::ListBuilder;
use arrow::array::{Array, ArrayRef, BooleanArray, BooleanBuilder, ListArray};
use arrow::datatypes::{DataType, Field};

use crate::exec::node::aggregate::AggFunction;

use super::super::*;
use super::AggregateFunction;

pub(super) struct RetentionAgg;

const MAX_CONDITION_SIZE_BIT: u64 = 5;
const MAX_CONDITION_SIZE: u64 = (1 << MAX_CONDITION_SIZE_BIT) - 1;
const BOOL_VALUES: [u64; 31] = [
    1u64 << 63,
    1u64 << 62,
    1u64 << 61,
    1u64 << 60,
    1u64 << 59,
    1u64 << 58,
    1u64 << 57,
    1u64 << 56,
    1u64 << 55,
    1u64 << 54,
    1u64 << 53,
    1u64 << 52,
    1u64 << 51,
    1u64 << 50,
    1u64 << 49,
    1u64 << 48,
    1u64 << 47,
    1u64 << 46,
    1u64 << 45,
    1u64 << 44,
    1u64 << 43,
    1u64 << 42,
    1u64 << 41,
    1u64 << 40,
    1u64 << 39,
    1u64 << 38,
    1u64 << 37,
    1u64 << 36,
    1u64 << 35,
    1u64 << 34,
    1u64 << 33,
];

impl AggregateFunction for RetentionAgg {
    fn build_spec_from_type(
        &self,
        _func: &AggFunction,
        input_type: Option<&DataType>,
        _input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        let data_type = input_type.ok_or_else(|| "retention input type missing".to_string())?;
        match data_type {
            DataType::List(field) if matches!(field.data_type(), DataType::Boolean) => {
                Ok(AggSpec {
                    kind: AggKind::Retention,
                    output_type: DataType::List(Field::new("item", DataType::Boolean, true).into()),
                    intermediate_type: DataType::Int64,
                    input_arg_type: None,
                    count_all: false,
                })
            }
            other => Err(format!(
                "retention expects array<boolean> input, got {:?}",
                other
            )),
        }
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        match kind {
            AggKind::Retention => (std::mem::size_of::<u64>(), std::mem::align_of::<u64>()),
            other => unreachable!("unexpected kind for retention: {:?}", other),
        }
    }

    fn build_input_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "retention input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn build_merge_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "retention merge input missing".to_string())?;
        Ok(AggInputView::Int(IntArrayView::new(arr)?))
    }

    fn init_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::write(ptr as *mut u64, 0);
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
        let AggInputView::Any(array) = input else {
            return Err("retention batch input type mismatch".to_string());
        };
        let list = array
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| "retention expects ListArray".to_string())?;
        let values = list.values();
        let bool_values = values
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| "retention expects list<bool>".to_string())?;
        let offsets = list.offsets();

        for (row, &base) in state_ptrs.iter().enumerate() {
            let start = offsets[row] as usize;
            let end = offsets[row + 1] as usize;
            let mut size = end.saturating_sub(start) as u64;
            if size > MAX_CONDITION_SIZE {
                size = MAX_CONDITION_SIZE;
            }
            let slot = unsafe { &mut *((base as *mut u8).add(offset) as *mut u64) };
            for i in 0..(size as usize) {
                let idx = start + i;
                if !bool_values.is_null(idx) && bool_values.value(idx) {
                    *slot |= BOOL_VALUES[i];
                }
            }
            *slot |= size;
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
            return Err("retention merge input type mismatch".to_string());
        };
        for (row, &base) in state_ptrs.iter().enumerate() {
            let Some(value) = view.value_at(row) else {
                continue;
            };
            let slot = unsafe { &mut *((base as *mut u8).add(offset) as *mut u64) };
            *slot |= value as u64;
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
        let mut builder = ListBuilder::new(BooleanBuilder::new());
        for &base in group_states {
            let state = unsafe { &*((base as *mut u8).add(offset) as *const u64) };
            let size = (state & MAX_CONDITION_SIZE) as usize;
            if size == 0 {
                builder.append(true);
                continue;
            }
            let first = (state & BOOL_VALUES[0]) != 0;
            for idx in 0..size {
                let value = if idx == 0 {
                    first
                } else if first {
                    (state & BOOL_VALUES[idx]) != 0
                } else {
                    false
                };
                builder.values().append_value(value);
            }
            builder.append(true);
        }
        Ok(std::sync::Arc::new(builder.finish()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BooleanBuilder, ListBuilder};
    use arrow::datatypes::DataType;
    use std::mem::MaybeUninit;

    #[test]
    fn test_retention_spec() {
        let func = AggFunction {
            name: "retention".to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(crate::exec::node::aggregate::AggTypeSignature {
                intermediate_type: Some(DataType::Int64),
                output_type: Some(DataType::List(
                    Field::new("item", DataType::Boolean, true).into(),
                )),
                input_arg_type: None,
            }),
        };
        let input_type = DataType::List(Field::new("item", DataType::Boolean, true).into());
        let spec = RetentionAgg
            .build_spec_from_type(&func, Some(&input_type), false)
            .unwrap();
        assert!(matches!(spec.kind, AggKind::Retention));
    }

    #[test]
    fn test_retention_updates() {
        let func = AggFunction {
            name: "retention".to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(crate::exec::node::aggregate::AggTypeSignature {
                intermediate_type: Some(DataType::Int64),
                output_type: Some(DataType::List(
                    Field::new("item", DataType::Boolean, true).into(),
                )),
                input_arg_type: None,
            }),
        };
        let input_type = DataType::List(Field::new("item", DataType::Boolean, true).into());
        let spec = RetentionAgg
            .build_spec_from_type(&func, Some(&input_type), false)
            .unwrap();

        let mut builder = ListBuilder::new(BooleanBuilder::new());
        builder.values().append_value(true);
        builder.values().append_value(false);
        builder.values().append_value(true);
        builder.append(true);
        let list = Arc::new(builder.finish()) as ArrayRef;
        let input = AggInputView::Any(&list);

        let mut state = MaybeUninit::<u64>::uninit();
        RetentionAgg.init_state(&spec, state.as_mut_ptr() as *mut u8);
        let state_ptr = state.as_mut_ptr() as AggStatePtr;
        let state_ptrs = vec![state_ptr; 1];
        RetentionAgg
            .update_batch(&spec, 0, &state_ptrs, &input)
            .unwrap();
        let out = RetentionAgg
            .build_array(&spec, 0, &[state_ptr], false)
            .unwrap();
        RetentionAgg.drop_state(&spec, state.as_mut_ptr() as *mut u8);

        let out_arr = out.as_any().downcast_ref::<ListArray>().unwrap();
        let values = out_arr
            .values()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let start = out_arr.offsets()[0] as usize;
        let end = out_arr.offsets()[1] as usize;
        assert_eq!(end - start, 3);
        assert_eq!(values.value(start), true);
        assert_eq!(values.value(start + 1), false);
        assert_eq!(values.value(start + 2), true);
    }
}
