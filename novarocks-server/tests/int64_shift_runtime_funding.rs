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

//! Original Int64 shipping GLOBAL witnesses, sharing the lawful Native fixture.
#[path = "support/shift_runtime_funding.rs"]
mod shift_support;
use arrow::array::{Array, Int64Array};
use novarocks_execution::runtime::scalar_memory::{
    RuntimeScalarEvaluationFailure, RuntimeScalarMemoryRefusal,
    test_support::{RuntimeScalarFrameForTest, RuntimeScalarWrapperForTest},
};
use novarocks_functions::{EvaluatedArgument, Selection};
use novarocks_memory::lane::global_store;
use shift_support::*;

fn program(
    name: &str,
    binding: &novarocks_execution::runtime::query_memory::QueryMemoryBinding,
) -> std::sync::Arc<novarocks_local_program::LocalProgram> {
    program_typed(name, &arrow::datatypes::DataType::Int64, binding)
}
#[test]
fn original_production_frame_values_keep_account_through_alias_last_free() {
    let _ = novarocks_server::memory_observation::snapshot();
    for name in NAMES {
        let binding = binding();
        let runtime = runtime(&binding);
        let program = program(name, &binding);
        let input = batch(&program, false, 4096);
        let mut frame = RuntimeScalarFrameForTest::try_new(
            program.clone(),
            site(&program),
            runtime.clone(),
            task(&binding),
            Some(binding.clone()),
        )
        .unwrap();
        let output = frame.evaluate(&input).unwrap();
        let output_array = output.as_any().downcast_ref::<Int64Array>().unwrap();
        let expected = match name {
            "bit_shift_left" => -14,
            "bit_shift_right" => -4,
            "bit_shift_right_logical" => ((-7i64) as u64 >> 1) as i64,
            _ => unreachable!(),
        };
        assert_eq!(
            output_array.iter().collect::<Vec<_>>(),
            vec![Some(expected); 4096]
        );
        let backing = output_array.values().inner().clone();
        // SAFETY: the actual Frame returned the standard original shift value
        // allocation, which remains alive via output and this Buffer alias.
        let reference = unsafe { original_shift_tag(&backing) };
        assert_origin(reference, &binding);
        eprintln!(
            "Frame source={name} rows=4096 values_capacity={} record={reference:?} account={}",
            backing.capacity(),
            binding.account().id().get()
        );
        for argument in input.columns() {
            let source = argument
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .inner();
            assert_ne!(source.data_ptr(), backing.data_ptr());
            // SAFETY: this fixture's Int64Array came from its actual standard
            // Vec producer under the same shipping GLOBAL, outside any grant.
            let original = unsafe { original_shift_tag(source) };
            assert_ne!(
                global_store().snapshot_ref(original).unwrap().origin,
                binding.account().id().get()
            );
        }
        let slice = output.slice(10, 3000);
        let sliced_backing = slice
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .inner()
            .clone();
        assert_eq!(backing.data_ptr(), sliced_backing.data_ptr());
        assert_eq!(backing.capacity(), sliced_backing.capacity());
        drop(frame);
        drop(program);
        drop(input);
        drop(output);
        drop(slice);
        assert_origin(reference, &binding);
        drop(sliced_backing);
        assert_origin(reference, &binding);
        runtime.shutdown_driver_execution().unwrap();
        drop(runtime);
        last_free(&binding, vec![backing], vec![reference]);
    }
}
#[test]
fn original_complete_wrapper_nullable_buffers_keep_account_through_alias_last_free() {
    let _ = novarocks_server::memory_observation::snapshot();
    for name in NAMES {
        let binding = binding();
        let runtime = runtime(&binding);
        let program = program(name, &binding);
        let input = batch(&program, true, 4096);
        let mut wrapper = RuntimeScalarWrapperForTest::try_new(
            &program,
            site(&program),
            runtime.clone(),
            task(&binding),
            Some(binding.clone()),
        )
        .unwrap();
        let arguments = [
            EvaluatedArgument::Column(&input.columns()[0]),
            EvaluatedArgument::Column(&input.columns()[1]),
        ];
        let (output, receipt) = wrapper.evaluate(Selection::all(4096), &arguments);
        assert_settled(&receipt);
        let (_, values, errors) = output.unwrap().into_parts();
        assert!(errors.is_empty());
        let array = values.as_any().downcast_ref::<Int64Array>().unwrap();
        let expected = match name {
            "bit_shift_left" => -14,
            "bit_shift_right" => -4,
            "bit_shift_right_logical" => ((-7i64) as u64 >> 1) as i64,
            _ => unreachable!(),
        };
        assert_eq!(
            array.iter().collect::<Vec<_>>(),
            (0..4096)
                .map(|row| if row % 3 == 0 { None } else { Some(expected) })
                .collect::<Vec<_>>()
        );
        assert_eq!(array.null_count(), 4096_usize.div_ceil(3));
        let value_alias = array.values().inner().clone();
        let null_alias = array.nulls().unwrap().buffer().clone();
        assert_ne!(value_alias.data_ptr(), null_alias.data_ptr());
        // SAFETY: both live standard allocations were born in the original
        // complete ScalarV1 wrapper under this target's sole shipping GLOBAL.
        let value_tag = unsafe { original_shift_tag(&value_alias) };
        let null_tag = unsafe { original_shift_tag(&null_alias) };
        eprintln!(
            "Wrapper source={name} rows=4096 values_capacity={} bitmap_capacity={} values_record={value_tag:?} bitmap_record={null_tag:?} workset={} settlement={:?}",
            value_alias.capacity(),
            null_alias.capacity(),
            receipt.workset_bytes.unwrap(),
            receipt.body.settlement
        );
        for tag in [value_tag, null_tag] {
            assert_origin(tag, &binding);
        }
        let slice = values.slice(11, 3000);
        let sliced = slice.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(sliced.values().inner().data_ptr(), value_alias.data_ptr());
        assert_eq!(
            sliced.nulls().unwrap().buffer().data_ptr(),
            null_alias.data_ptr()
        );
        let bitmap_slice_alias = sliced.nulls().unwrap().buffer().clone();
        assert_eq!(bitmap_slice_alias.capacity(), null_alias.capacity());
        // SAFETY: the slice keeps the SAME live original wrapper allocation;
        // the tail uses its base and capacity, never its visible bit offset.
        assert_eq!(unsafe { original_shift_tag(&bitmap_slice_alias) }, null_tag);
        drop(wrapper);
        drop(program);
        drop(input);
        drop(values);
        drop(slice);
        assert_origin(null_tag, &binding);
        drop(bitmap_slice_alias);
        for tag in [value_tag, null_tag] {
            assert_origin(tag, &binding);
        }
        runtime.shutdown_driver_execution().unwrap();
        drop(runtime);
        last_free(
            &binding,
            vec![value_alias, null_alias],
            vec![value_tag, null_tag],
        );
    }
}
#[test]
fn original_complete_wrapper_empty_small_and_missing_keep_existing_policy() {
    let _ = novarocks_server::memory_observation::snapshot();
    for rows in [0, 3] {
        let binding = binding();
        let runtime = runtime(&binding);
        let program = program(NAMES[0], &binding);
        let input = batch(&program, false, rows);
        let mut wrapper = RuntimeScalarWrapperForTest::try_new(
            &program,
            site(&program),
            runtime.clone(),
            task(&binding),
            Some(binding.clone()),
        )
        .unwrap();
        let arguments = [
            EvaluatedArgument::Column(&input.columns()[0]),
            EvaluatedArgument::Column(&input.columns()[1]),
        ];
        let (values, receipt) = wrapper.evaluate(Selection::all(rows), &arguments);
        let (_, values, errors) = values.unwrap().into_parts();
        assert!(errors.is_empty());
        assert_eq!(values.len(), rows);
        eprintln!(
            "Wrapper small rows={rows} workset={} settlement={:?}",
            receipt.workset_bytes.unwrap(),
            receipt.body.settlement
        );
        assert!(receipt.workset_bytes.unwrap() >= 512);
        assert_eq!(receipt.body.settlement.as_ref().unwrap().accepted_live, 0);
        assert_eq!(receipt.body.settlement.as_ref().unwrap().debt, 0);
        assert!(
            values
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .inner()
                .capacity()
                < 512
        );
        let mut missing = RuntimeScalarWrapperForTest::try_new(
            &program,
            site(&program),
            runtime.clone(),
            task(&binding),
            None,
        )
        .unwrap();
        let (failure, refused) = missing.evaluate(Selection::all(rows), &arguments);
        assert_eq!(
            failure.unwrap_err(),
            RuntimeScalarEvaluationFailure::Host(RuntimeScalarMemoryRefusal::MissingQueryMemory)
        );
        assert!(refused.body.settlement.is_none());
        drop(wrapper);
        drop(missing);
        drop(values);
        drop(input);
        drop(program);
        runtime.shutdown_driver_execution().unwrap();
    }
}
