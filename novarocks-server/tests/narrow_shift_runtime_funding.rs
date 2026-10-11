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

//! Shipping GLOBAL: original narrow Frame and complete wrapper allocations.
//! Explicit account retirement is a test protocol, not production exit evidence.
#[path = "support/shift_runtime_funding.rs"]
mod shift_support;
use arrow::{
    array::{Array, ArrayRef, Int8Array, Int16Array, Int32Array, Int64Array},
    buffer::Buffer,
    datatypes::DataType,
};
use novarocks_execution::runtime::scalar_memory::{
    RuntimeScalarEvaluationFailure, RuntimeScalarMemoryRefusal,
    test_support::{RuntimeScalarFrameForTest, RuntimeScalarWrapperForTest},
};
use novarocks_functions::{EvaluatedArgument, Selection};
use novarocks_memory::lane::global_store;
use shift_support::*;

fn expected(name: &str) -> Option<i64> {
    match name {
        "bit_shift_left" => Some(-14),
        "bit_shift_right" => Some(-4),
        "bit_shift_right_logical" => None,
        _ => panic!("installed exact shift name"),
    }
}

#[test]
fn original_nine_frames_keep_fresh_values_and_always_bitmap_through_alias_last_free() {
    let _ = novarocks_server::memory_observation::snapshot();
    for ty in [DataType::Int8, DataType::Int16, DataType::Int32] {
        for name in NAMES {
            let binding = binding();
            let runtime = runtime(&binding);
            let program = program_typed(name, &ty, &binding);
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
            assert_eq!(output.data_type(), &ty);
            assert_eq!(result_rows(&output), vec![expected(name); 4096]);
            let (values, bitmap) = result_backings(&output);
            let bitmap = bitmap.expect("original safe unary_opt always creates validity");
            assert_ne!(values.data_ptr(), bitmap.data_ptr());
            assert_eq!(bitmap.capacity(), 512);
            // SAFETY: both live allocations are the original standard narrow
            // output born under this target's sole shipping GLOBAL.
            let value_tag = unsafe { original_shift_tag(&values) };
            let bitmap_tag = unsafe { original_shift_tag(&bitmap) };
            assert_origin(value_tag, &binding);
            assert_origin(bitmap_tag, &binding);
            eprintln!(
                "Frame source={name} width={ty:?} rows=4096 values_capacity={} bitmap_capacity={} values_record={value_tag:?} bitmap_record={bitmap_tag:?} account={}",
                values.capacity(),
                bitmap.capacity(),
                binding.account().id().get()
            );
            for argument in input.columns() {
                let (source, _) = result_backings(argument);
                assert_ne!(source.data_ptr(), values.data_ptr());
                assert_ne!(source.data_ptr(), bitmap.data_ptr());
                // SAFETY: input came from the shared fixture's standard Vec
                // producer outside any query grant, under the same GLOBAL.
                let input_tag = unsafe { original_shift_tag(&source) };
                assert_ne!(
                    global_store().snapshot_ref(input_tag).unwrap().origin,
                    binding.account().id().get()
                );
            }
            let slice = output.slice(11, 3000);
            let (sliced_values, sliced_bitmap) = result_backings(&slice);
            let sliced_bitmap = sliced_bitmap.unwrap();
            assert_eq!(sliced_values.data_ptr(), values.data_ptr());
            assert_eq!(sliced_values.capacity(), values.capacity());
            assert_eq!(sliced_bitmap.data_ptr(), bitmap.data_ptr());
            assert_eq!(sliced_bitmap.capacity(), bitmap.capacity());
            // SAFETY: slicing retains both SAME original allocations. Tail
            // address is their original base/capacity, not a visible offset.
            assert_eq!(unsafe { original_shift_tag(&sliced_values) }, value_tag);
            assert_eq!(unsafe { original_shift_tag(&sliced_bitmap) }, bitmap_tag);
            drop(frame);
            drop(program);
            drop(input);
            drop(output);
            drop(slice);
            drop(sliced_values);
            drop(sliced_bitmap);
            assert_origin(value_tag, &binding);
            assert_origin(bitmap_tag, &binding);
            runtime.shutdown_driver_execution().unwrap();
            drop(runtime);
            if ty == DataType::Int16 {
                last_free(&binding, vec![bitmap, values], vec![bitmap_tag, value_tag]);
            } else {
                last_free(&binding, vec![values, bitmap], vec![value_tag, bitmap_tag]);
            }
        }
    }
}

#[test]
fn original_nine_complete_nullable_wrappers_keep_both_backings_and_zero_debt() {
    let _ = novarocks_server::memory_observation::snapshot();
    for ty in [DataType::Int8, DataType::Int16, DataType::Int32] {
        for name in NAMES {
            let binding = binding();
            let runtime = runtime(&binding);
            let program = program_typed(name, &ty, &binding);
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
            let (_, output, errors) = output.unwrap().into_parts();
            assert!(errors.is_empty());
            assert_eq!(output.data_type(), &ty);
            assert_eq!(
                result_rows(&output),
                (0..4096)
                    .map(|row| if row % 3 == 0 { None } else { expected(name) })
                    .collect::<Vec<_>>()
            );
            let (values, bitmap) = result_backings(&output);
            let bitmap = bitmap.unwrap();
            // SAFETY: live original standard complete-wrapper allocations
            // born under the same query grant in this sole GLOBAL target.
            let value_tag = unsafe { original_shift_tag(&values) };
            let bitmap_tag = unsafe { original_shift_tag(&bitmap) };
            assert_origin(value_tag, &binding);
            assert_origin(bitmap_tag, &binding);
            eprintln!(
                "Wrapper source={name} width={ty:?} rows=4096 values_capacity={} bitmap_capacity={} workset={} settlement={:?}",
                values.capacity(),
                bitmap.capacity(),
                receipt.workset_bytes.unwrap(),
                receipt.body.settlement
            );
            let slice = output.slice(17, 3000);
            let (slice_values, slice_bitmap) = result_backings(&slice);
            assert_eq!(slice_values.data_ptr(), values.data_ptr());
            assert_eq!(slice_bitmap.as_ref().unwrap().data_ptr(), bitmap.data_ptr());
            drop(wrapper);
            drop(program);
            drop(input);
            drop(output);
            drop(slice);
            drop(slice_values);
            drop(slice_bitmap);
            runtime.shutdown_driver_execution().unwrap();
            drop(runtime);
            last_free(&binding, vec![bitmap, values], vec![bitmap_tag, value_tag]);
        }
    }
}

#[test]
fn original_narrow_empty_small_and_rounding_bands_preserve_nominal_missing_policy() {
    let _ = novarocks_server::memory_observation::snapshot();
    for ty in [DataType::Int8, DataType::Int16, DataType::Int32] {
        for name in NAMES {
            for rows in [0, 3, 63, 64, 65, 127, 128, 129, 511, 512, 513] {
                let binding = binding();
                let runtime = runtime(&binding);
                let program = program_typed(name, &ty, &binding);
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
                let (output, receipt) = wrapper.evaluate(Selection::all(rows), &arguments);
                let (_, output, errors) = output.unwrap().into_parts();
                assert!(errors.is_empty());
                assert_eq!(output.data_type(), &ty);
                assert_eq!(result_rows(&output), vec![expected(name); rows]);
                let settlement = receipt.body.settlement.as_ref().unwrap();
                assert_eq!(settlement.debt, 0);
                assert_eq!(settlement.next_step, Ok(()));
                let (values, bitmap) = result_backings(&output);
                for backing in std::iter::once(&values).chain(bitmap.as_ref()) {
                    if backing.capacity() >= 512 {
                        // SAFETY: actual original standard output, same GLOBAL;
                        // this branch alone has the sampled size-band premise.
                        assert_origin(unsafe { original_shift_tag(backing) }, &binding);
                    }
                }
                if rows <= 3 {
                    assert!(values.capacity() < 512);
                    assert_eq!(settlement.accepted_live, 0);
                }
                eprintln!(
                    "Rounding source={name} width={ty:?} rows={rows} values_capacity={} bitmap_capacity={:?} workset={} settlement={settlement:?}",
                    values.capacity(),
                    bitmap.as_ref().map(Buffer::capacity),
                    receipt.workset_bytes.unwrap()
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
                    RuntimeScalarEvaluationFailure::Host(
                        RuntimeScalarMemoryRefusal::MissingQueryMemory
                    )
                );
                assert!(refused.body.settlement.is_none());
                drop(wrapper);
                drop(missing);
                drop(values);
                drop(bitmap);
                drop(output);
                drop(input);
                drop(program);
                runtime.shutdown_driver_execution().unwrap();
            }
        }
    }
}

#[test]
fn original_sparse_and_broadcast_wrappers_fund_selected_rows_with_original_arguments() {
    let _ = novarocks_server::memory_observation::snapshot();
    let selected_rows: Vec<usize> = (0..8192).step_by(2).collect();
    let selection = Selection::try_sparse(8192, &selected_rows).unwrap();
    for ty in [DataType::Int8, DataType::Int16, DataType::Int32] {
        for name in NAMES {
            for scalar_source in [false, true] {
                let binding = binding();
                let runtime = runtime(&binding);
                let program = program_typed(name, &ty, &binding);
                let input = batch(&program, !scalar_source, 8192);
                let source_scalar = input.columns()[0].slice(0, 1);
                let count_scalar = input.columns()[1].slice(0, 1);
                let arguments = [
                    if scalar_source {
                        EvaluatedArgument::Scalar(&source_scalar)
                    } else {
                        EvaluatedArgument::Column(&input.columns()[0])
                    },
                    EvaluatedArgument::Scalar(&count_scalar),
                ];
                let mut wrapper = RuntimeScalarWrapperForTest::try_new(
                    &program,
                    site(&program),
                    runtime.clone(),
                    task(&binding),
                    Some(binding.clone()),
                )
                .unwrap();
                let (output, receipt) = wrapper.evaluate(selection, &arguments);
                assert_settled(&receipt);
                let (returned_selection, output, errors) = output.unwrap().into_parts();
                assert_eq!(returned_selection, selection);
                assert!(errors.is_empty());
                assert_eq!(output.data_type(), &ty);
                assert_eq!(
                    result_rows(&output),
                    selected_rows
                        .iter()
                        .map(|row| if !scalar_source && row % 3 == 0 {
                            None
                        } else {
                            expected(name)
                        })
                        .collect::<Vec<_>>()
                );
                let (values, bitmap) = result_backings(&output);
                let bitmap = bitmap.unwrap();
                // SAFETY: original standard wrapper backings, same sole GLOBAL;
                // sparse/broadcast arguments do not replace the output producer.
                let value_tag = unsafe { original_shift_tag(&values) };
                let bitmap_tag = unsafe { original_shift_tag(&bitmap) };
                assert_origin(value_tag, &binding);
                assert_origin(bitmap_tag, &binding);
                eprintln!(
                    "Sparse broadcast source={name} width={ty:?} batch_rows=8192 selected_rows=4096 scalar_source={scalar_source} count_scalar=true values_capacity={} bitmap_capacity={} workset={} settlement={:?}",
                    values.capacity(),
                    bitmap.capacity(),
                    receipt.workset_bytes.unwrap(),
                    receipt.body.settlement
                );
                drop(wrapper);
                drop(output);
                drop(source_scalar);
                drop(count_scalar);
                drop(input);
                drop(program);
                runtime.shutdown_driver_execution().unwrap();
                drop(runtime);
                last_free(&binding, vec![values, bitmap], vec![value_tag, bitmap_tag]);
            }
        }
    }
}

fn result_backings(values: &ArrayRef) -> (Buffer, Option<Buffer>) {
    macro_rules! backings {
        ($array:ty) => {{
            let array = values.as_any().downcast_ref::<$array>().unwrap();
            (
                array.values().inner().clone(),
                array.nulls().map(|n| n.buffer().clone()),
            )
        }};
    }
    match values.data_type() {
        DataType::Int8 => backings!(Int8Array),
        DataType::Int16 => backings!(Int16Array),
        DataType::Int32 => backings!(Int32Array),
        DataType::Int64 => backings!(Int64Array),
        _ => panic!("exact shift fixture result"),
    }
}
fn result_rows(values: &ArrayRef) -> Vec<Option<i64>> {
    macro_rules! rows {
        ($array:ty) => {
            values
                .as_any()
                .downcast_ref::<$array>()
                .unwrap()
                .iter()
                .map(|v| v.map(i64::from))
                .collect()
        };
    }
    match values.data_type() {
        DataType::Int8 => rows!(Int8Array),
        DataType::Int16 => rows!(Int16Array),
        DataType::Int32 => rows!(Int32Array),
        DataType::Int64 => rows!(Int64Array),
        _ => panic!("exact shift fixture result"),
    }
}
