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

//! Original bitwise wrappers and Frames under the sole shipping GLOBAL.
//! Explicit fixture retirement does not establish production Work teardown.
#[path = "support/shift_runtime_funding.rs"]
mod support;
use arrow::{
    array::{Array, ArrayRef, Int8Array, Int16Array, Int32Array, Int64Array},
    buffer::Buffer,
    datatypes::DataType,
};
use novarocks_execution::runtime::scalar_memory::{
    RuntimeScalarEvaluationFailure, RuntimeScalarMemoryRefusal,
    test_support::{RuntimeScalarFrameForTest, RuntimeScalarWrapperForTest},
};
use novarocks_functions::{
    ConstantPolicy, ConstantPool, EvaluatedArgument, SelectedValues, Selection,
};
use novarocks_memory::lane::global_store;
use novarocks_type_contract::{
    CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
};
use support::*;

const BITWISE: [&str; 4] = ["bitand", "bitor", "bitxor", "bitnot"];
fn widths() -> [DataType; 4] {
    [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
    ]
}
fn expected(name: &str) -> i64 {
    match name {
        "bitand" => 1,
        "bitor" => -5,
        "bitxor" => -6,
        "bitnot" => 6,
        _ => panic!("exact installed bitwise fixture operation"),
    }
}
fn arity(name: &str) -> usize {
    if name == "bitnot" { 1 } else { 2 }
}
fn array(ty: &DataType, values: Vec<Option<i64>>) -> ArrayRef {
    macro_rules! narrow {
        ($array:ty, $native:ty) => {
            Arc::new(<$array>::from(
                values
                    .into_iter()
                    .map(|v| v.map(|v| <$native>::try_from(v).unwrap()))
                    .collect::<Vec<_>>(),
            )) as ArrayRef
        };
    }
    use std::sync::Arc;
    match ty {
        DataType::Int8 => narrow!(Int8Array, i8),
        DataType::Int16 => narrow!(Int16Array, i16),
        DataType::Int32 => narrow!(Int32Array, i32),
        DataType::Int64 => Arc::new(Int64Array::from(values)),
        _ => panic!("exact signed bitwise fixture carrier"),
    }
}
fn rows(values: &ArrayRef) -> Vec<Option<i64>> {
    macro_rules! read {
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
        DataType::Int8 => read!(Int8Array),
        DataType::Int16 => read!(Int16Array),
        DataType::Int32 => read!(Int32Array),
        DataType::Int64 => read!(Int64Array),
        _ => panic!("exact signed bitwise fixture output"),
    }
}
fn backings(values: &ArrayRef) -> (Buffer, Option<Buffer>) {
    macro_rules! read {
        ($array:ty) => {{
            let array = values.as_any().downcast_ref::<$array>().unwrap();
            (
                array.values().inner().clone(),
                array.nulls().map(|n| n.buffer().clone()),
            )
        }};
    }
    match values.data_type() {
        DataType::Int8 => read!(Int8Array),
        DataType::Int16 => read!(Int16Array),
        DataType::Int32 => read!(Int32Array),
        DataType::Int64 => read!(Int64Array),
        _ => panic!("original standard primitive output only"),
    }
}
fn columns(name: &str, ty: &DataType, n: usize, nullable: bool) -> Vec<ArrayRef> {
    (0..arity(name))
        .map(|argument| {
            array(
                ty,
                (0..n)
                    .map(|row| {
                        let period = if argument == 0 { 3 } else { 5 };
                        if nullable && row % period == 0 {
                            None
                        } else {
                            Some(if argument == 0 { -7 } else { 3 })
                        }
                    })
                    .collect(),
            )
        })
        .collect()
}
struct ConstantControl;
impl PureCompileControl for ConstantControl {
    fn checkpoint(&self, _: CompilePhase, work: u32) -> Result<(), CompileControlError> {
        assert!(work <= 256);
        Ok(())
    }
}
fn pool(ty: &DataType, value: Option<i64>) -> ConstantPool {
    let ty = FunctionValueType::new(ty.clone(), true);
    ConstantPool::try_new(
        std::sync::Arc::new(ty.try_to_field("original-bitwise-selected-pool").unwrap()),
        ty.clone(),
        array(&ty.data_type, vec![Some(1), value]).to_data(),
        ConstantPolicy {
            max_rows: 8,
            max_array_nodes: 8,
            max_logical_elements: 64,
            max_retained_buffer_bytes: 4096,
            max_type_depth: 8,
            max_type_nodes: 64,
            max_dictionary_depth: 4,
            max_metadata_bytes: 1024,
            max_library_validation_work: 4096,
            max_library_validation_bytes: 8192,
        },
        CompilePhase::Validate,
        &ConstantControl,
    )
    .unwrap()
}
fn program(
    name: &str,
    ty: &DataType,
    binding: &novarocks_execution::runtime::query_memory::QueryMemoryBinding,
) -> std::sync::Arc<novarocks_local_program::LocalProgram> {
    program_for_arguments(
        name,
        &vec![FunctionValueType::new(ty.clone(), true); arity(name)],
        &BITWISE,
        binding,
    )
}

#[test]
fn sixteen_original_frames_and_nullable_wrappers_keep_each_backing_in_both_drop_orders() {
    let _ = novarocks_server::memory_observation::snapshot();
    for ty in widths() {
        for name in BITWISE {
            for nullable_wrapper in [false, true] {
                for reverse in [false, true] {
                    let binding = binding();
                    let runtime = runtime_for_names(&binding, &BITWISE);
                    let program = program(name, &ty, &binding);
                    let input =
                        batch_for_arguments(&program, columns(name, &ty, 4096, nullable_wrapper));
                    let (output, frame, wrapper) = if nullable_wrapper {
                        let mut wrapper = RuntimeScalarWrapperForTest::try_new(
                            &program,
                            site(&program),
                            runtime.clone(),
                            task(&binding),
                            Some(binding.clone()),
                        )
                        .unwrap();
                        let args = input
                            .columns()
                            .iter()
                            .map(EvaluatedArgument::Column)
                            .collect::<Vec<_>>();
                        let (result, receipt) = wrapper.evaluate(Selection::all(4096), &args);
                        assert_settled(&receipt);
                        let (_, values, errors) = result.unwrap().into_parts();
                        assert!(errors.is_empty());
                        (values, None, Some(wrapper))
                    } else {
                        let mut frame = RuntimeScalarFrameForTest::try_new(
                            program.clone(),
                            site(&program),
                            runtime.clone(),
                            task(&binding),
                            Some(binding.clone()),
                        )
                        .unwrap();
                        let values = frame.evaluate(&input).unwrap();
                        (values, Some(frame), None)
                    };
                    assert_eq!(output.data_type(), &ty);
                    assert_eq!(
                        rows(&output),
                        (0..4096)
                            .map(|row| {
                                if nullable_wrapper
                                    && (row % 3 == 0 || (arity(name) == 2 && row % 5 == 0))
                                {
                                    None
                                } else {
                                    Some(expected(name))
                                }
                            })
                            .collect::<Vec<_>>()
                    );
                    let (values, bitmap) = backings(&output);
                    assert_eq!(bitmap.is_some(), nullable_wrapper || ty != DataType::Int64);
                    if let Some(bitmap) = &bitmap {
                        assert_ne!(bitmap.data_ptr(), values.data_ptr());
                        assert_eq!(bitmap.capacity(), 512);
                    }
                    let mut aliases = vec![values];
                    aliases.extend(bitmap);
                    // SAFETY: all live buffers are the original standard output
                    // from the installed owner under this target's sole GLOBAL.
                    let mut tags = aliases
                        .iter()
                        .map(|b| unsafe { original_primitive_tag(b) })
                        .collect::<Vec<_>>();
                    for &tag in &tags {
                        assert_origin(tag, &binding);
                    }
                    for source in input.columns() {
                        let (source_values, _) = backings(source);
                        for alias in &aliases {
                            assert_ne!(source_values.data_ptr(), alias.data_ptr());
                        }
                        // SAFETY: fixture inputs are standard Vec producers under
                        // the same GLOBAL, created outside any query grant.
                        let tag = unsafe { original_primitive_tag(&source_values) };
                        assert_ne!(
                            global_store().snapshot_ref(tag).unwrap().origin,
                            binding.account().id().get()
                        );
                    }
                    let slice = output.slice(11, 3000);
                    let (slice_values, slice_bitmap) = backings(&slice);
                    let mut sliced = vec![slice_values];
                    sliced.extend(slice_bitmap);
                    for ((alias, slice), tag) in aliases.iter().zip(&sliced).zip(&tags) {
                        assert_eq!(alias.data_ptr(), slice.data_ptr());
                        assert_eq!(alias.capacity(), slice.capacity());
                        // SAFETY: SAME live original allocation and base/capacity.
                        assert_eq!(unsafe { original_primitive_tag(slice) }, *tag);
                    }
                    drop(frame);
                    drop(wrapper);
                    drop(program);
                    drop(input);
                    drop(output);
                    drop(slice);
                    drop(sliced);
                    for &tag in &tags {
                        assert_origin(tag, &binding);
                    }
                    runtime.shutdown_driver_execution().unwrap();
                    drop(runtime);
                    if reverse {
                        aliases.reverse();
                        tags.reverse();
                    }
                    eprintln!(
                        "Bitwise original source={name} width={ty:?} nullable_wrapper={nullable_wrapper} reverse={reverse} account={}",
                        binding.account().id().get()
                    );
                    last_free(&binding, aliases, tags);
                }
            }
        }
    }
}
#[test]
fn original_bitwise_small_empty_and_rounding_wrappers_keep_missing_binding_nominal() {
    let _ = novarocks_server::memory_observation::snapshot();
    for ty in widths() {
        for name in BITWISE {
            for n in [0, 3, 63, 64, 65, 127, 128, 129, 511, 512, 513] {
                let binding = binding();
                let runtime = runtime_for_names(&binding, &BITWISE);
                let program = program(name, &ty, &binding);
                let input = batch_for_arguments(&program, columns(name, &ty, n, false));
                let args = input
                    .columns()
                    .iter()
                    .map(EvaluatedArgument::Column)
                    .collect::<Vec<_>>();
                let mut wrapper = RuntimeScalarWrapperForTest::try_new(
                    &program,
                    site(&program),
                    runtime.clone(),
                    task(&binding),
                    Some(binding.clone()),
                )
                .unwrap();
                let (result, receipt) = wrapper.evaluate(Selection::all(n), &args);
                let (_, output, errors) = result.unwrap().into_parts();
                assert!(errors.is_empty());
                assert_eq!(rows(&output), vec![Some(expected(name)); n]);
                assert_eq!(output.data_type(), &ty);
                let settlement = receipt.body.settlement.as_ref().unwrap();
                assert_eq!(settlement.debt, 0);
                assert_eq!(settlement.next_step, Ok(()));
                let (values, bitmap) = backings(&output);
                for backing in std::iter::once(&values).chain(bitmap.as_ref()) {
                    if backing.capacity() >= 512 {
                        // SAFETY: original standard output, same sole GLOBAL;
                        // only the sampled size band has a tail premise.
                        assert_origin(unsafe { original_primitive_tag(backing) }, &binding);
                    }
                }
                if n <= 3 {
                    assert!(values.capacity() < 512);
                    assert_eq!(settlement.accepted_live, 0);
                }
                let mut missing = RuntimeScalarWrapperForTest::try_new(
                    &program,
                    site(&program),
                    runtime.clone(),
                    task(&binding),
                    None,
                )
                .unwrap();
                let (result, refusal) = missing.evaluate(Selection::all(n), &args);
                assert_eq!(
                    result.unwrap_err(),
                    RuntimeScalarEvaluationFailure::Host(
                        RuntimeScalarMemoryRefusal::MissingQueryMemory
                    )
                );
                assert!(refusal.body.settlement.is_none());
                eprintln!(
                    "Bitwise rounding source={name} width={ty:?} rows={n} values_capacity={} bitmap_capacity={:?} settlement={settlement:?}",
                    values.capacity(),
                    bitmap.as_ref().map(Buffer::capacity)
                );
                drop(wrapper);
                drop(missing);
                drop(output);
                drop(values);
                drop(bitmap);
                drop(input);
                drop(program);
                runtime.shutdown_driver_execution().unwrap();
            }
        }
    }
}

#[test]
fn original_bitwise_sparse_scalar_and_compact_wrappers_keep_actual_arity_and_selection() {
    let _ = novarocks_server::memory_observation::snapshot();
    let selected_rows = (0..8192).step_by(2).collect::<Vec<_>>();
    let selection = Selection::try_sparse(8192, &selected_rows).unwrap();
    for ty in widths() {
        for name in BITWISE {
            for mode in [0, 1, 2, 3, 4] {
                let binding = binding();
                let runtime = runtime_for_names(&binding, &BITWISE);
                let program = program(name, &ty, &binding);
                let input = batch_for_arguments(&program, columns(name, &ty, 8192, mode != 1));
                let scalars = columns(name, &ty, 1, false);
                let compact = (0..arity(name))
                    .map(|argument| {
                        SelectedValues::try_new(
                            selection,
                            &ty,
                            array(
                                &ty,
                                selected_rows
                                    .iter()
                                    .map(|row| {
                                        let period = if argument == 0 { 3 } else { 5 };
                                        if row % period == 0 {
                                            None
                                        } else {
                                            Some(if argument == 0 { -7 } else { 3 })
                                        }
                                    })
                                    .collect(),
                            ),
                            Box::default(),
                        )
                        .unwrap()
                    })
                    .collect::<Vec<_>>();
                let pools = (0..arity(name))
                    .map(|argument| {
                        pool(
                            &ty,
                            if mode == 4 {
                                None
                            } else {
                                Some(if argument == 0 { -7 } else { 3 })
                            },
                        )
                    })
                    .collect::<Vec<_>>();
                let constants = pools
                    .iter()
                    .map(|pool| pool.value(1).unwrap())
                    .collect::<Vec<_>>();
                let args = (0..arity(name))
                    .map(|argument| match mode {
                        0 => EvaluatedArgument::Column(&input.columns()[argument]),
                        1 => EvaluatedArgument::Scalar(&scalars[argument]),
                        2 => EvaluatedArgument::SelectedColumn(&compact[argument]),
                        3 | 4 => EvaluatedArgument::Constant(&constants[argument]),
                        _ => unreachable!(),
                    })
                    .collect::<Vec<_>>();
                let mut wrapper = RuntimeScalarWrapperForTest::try_new(
                    &program,
                    site(&program),
                    runtime.clone(),
                    task(&binding),
                    Some(binding.clone()),
                )
                .unwrap();
                let (result, receipt) = wrapper.evaluate(selection, &args);
                assert_settled(&receipt);
                let (returned, output, errors) = result.unwrap().into_parts();
                assert_eq!(returned, selection);
                assert!(errors.is_empty());
                assert_eq!(output.data_type(), &ty);
                assert_eq!(
                    rows(&output),
                    selected_rows
                        .iter()
                        .map(|row| {
                            if mode == 4
                                || ((mode == 0 || mode == 2)
                                    && (row % 3 == 0 || (arity(name) == 2 && row % 5 == 0)))
                            {
                                None
                            } else {
                                Some(expected(name))
                            }
                        })
                        .collect::<Vec<_>>()
                );
                let (values, bitmap) = backings(&output);
                let mut aliases = vec![values];
                aliases.extend(bitmap);
                // SAFETY: original complete wrapper standard output, same GLOBAL.
                let tags = aliases
                    .iter()
                    .map(|b| unsafe { original_primitive_tag(b) })
                    .collect::<Vec<_>>();
                for &tag in &tags {
                    assert_origin(tag, &binding);
                }
                drop(wrapper);
                drop(output);
                drop(compact);
                drop(constants);
                drop(pools);
                drop(scalars);
                drop(input);
                drop(program);
                runtime.shutdown_driver_execution().unwrap();
                drop(runtime);
                last_free(&binding, aliases, tags);
            }
        }
    }
}

#[test]
fn shared_original_shift_wrappers_keep_their_exact_fixture_contract() {
    let _ = novarocks_server::memory_observation::snapshot();
    let binding = binding();
    let runtime = runtime(&binding);
    let program = program_typed("bit_shift_left", &DataType::Int64, &binding);
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
    assert_eq!(rows(&output), vec![Some(-14); 4096]);
    let (values, _) = backings(&output);
    // SAFETY: original standard shift output under the sole shipping GLOBAL.
    let tag = unsafe { original_shift_tag(&values) };
    assert_origin(tag, &binding);
    drop(frame);
    drop(output);
    drop(program);
    drop(input);
    runtime.shutdown_driver_execution().unwrap();
    drop(runtime);
    last_free(&binding, vec![values], vec![tag]);
}
