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
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BinaryArray, BooleanArray, Float64Array, Int64Array};
use arrow::datatypes::DataType;

use novarocks_execution::exec::expr::ExprId;
use novarocks_execution::exec::expr::agg;
use novarocks_execution::exec::node::aggregate::{AggFunction, AggTypeSignature};
use novarocks_functions::AggregateInputBatch;

fn build_builtin_kernel_set(
    functions: &[AggFunction],
    input_types: &[Option<DataType>],
    argument_types: &[Vec<DataType>],
) -> Result<agg::AggKernelSet, String> {
    let mut builder = agg::ExecutionFunctionSetBuilder::new();
    novarocks_sql::compiler::contribute_builtin_functions(builder.catalog_builder_mut())
        .map_err(|error| error.to_string())?;
    agg::contribute_builtin_aggregate_implementations(&mut builder)
        .map_err(|error| error.to_string())?;
    let function_set = builder.seal().map_err(|error| error.to_string())?;
    let selected = functions
        .iter()
        .zip(argument_types)
        .map(|(function, argument_types)| {
            function_set
                .catalog()
                .resolve_aggregate_trusted(&function.name, argument_types)
                .map_err(|error| error.to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    agg::build_kernel_set(&function_set, functions, input_types, &selected)
}

fn run_two_phase_i64(name: &str, part1: Vec<Option<i64>>, part2: Vec<Option<i64>>) -> Option<f64> {
    let func = AggFunction {
        name: name.to_string(),
        inputs: vec![ExprId(0)],
        input_is_intermediate: false,
        types: Some(AggTypeSignature {
            intermediate_type: None,
            output_type: Some(DataType::Float64),
            input_arg_type: None,
        }),
        order: Default::default(),
    };

    // Partial aggregation for each partition.
    let input1 = Arc::new(Int64Array::from(part1)) as ArrayRef;
    let input2 = Arc::new(Int64Array::from(part2)) as ArrayRef;

    let arrays1 = [Some(Arc::clone(&input1))];
    let arrays2 = [Some(Arc::clone(&input2))];
    let input_types = vec![Some(DataType::Int64)];
    let kernels = build_builtin_kernel_set(
        std::slice::from_ref(&func),
        &input_types,
        &[vec![DataType::Int64]],
    )
    .unwrap();
    let kernel = &kernels.entries[0];

    let mut arena = agg::AggStateArena::new(64 * 1024);
    let base1 = arena.alloc(kernels.layout.total_size, kernel.state_align());
    let base2 = arena.alloc(kernels.layout.total_size, kernel.state_align());
    kernel.init_state(base1).expect("init first state");
    kernel.init_state(base2).expect("init second state");

    let state_ptrs1 = vec![base1; input1.len()];
    kernel
        .update_batch(
            &state_ptrs1,
            AggregateInputBatch::try_new(arrays1[0].as_ref(), input1.len()).unwrap(),
        )
        .unwrap();

    let state_ptrs2 = vec![base2; input2.len()];
    kernel
        .update_batch(
            &state_ptrs2,
            AggregateInputBatch::try_new(arrays2[0].as_ref(), input2.len()).unwrap(),
        )
        .unwrap();

    // Build intermediate outputs (one row per partition state).
    let intermediate = kernel.build_array(&[base1, base2], true).unwrap();
    let intermediate = intermediate
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("binary intermediate");

    // Final aggregation merges intermediate states.
    let mut func_merge = func;
    func_merge.input_is_intermediate = true;
    let kernels_merge = build_builtin_kernel_set(
        &[func_merge],
        &[Some(intermediate.data_type().clone())],
        &[vec![DataType::Int64]],
    )
    .unwrap();
    let kernel_merge = &kernels_merge.entries[0];

    let base_final = arena.alloc(kernels_merge.layout.total_size, kernel_merge.state_align());
    kernel_merge
        .init_state(base_final)
        .expect("init final state");

    let merge_input = [Some(Arc::new(intermediate.clone()) as ArrayRef)];
    let merge_state_ptrs = vec![base_final; intermediate.len()];
    kernel_merge
        .merge_batch(
            &merge_state_ptrs,
            AggregateInputBatch::try_new(merge_input[0].as_ref(), intermediate.len()).unwrap(),
        )
        .unwrap();

    let out = kernel_merge.build_array(&[base_final], false).unwrap();
    let out = out
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("float64 output");
    if out.is_null(0) {
        None
    } else {
        Some(out.value(0))
    }
}

#[test]
fn test_variance_samp_two_phase() {
    let v = run_two_phase_i64(
        "variance_samp",
        vec![Some(1), Some(2)],
        vec![Some(3), Some(4)],
    )
    .expect("non-null");
    let expected = 5.0 / 3.0;
    assert!((v - expected).abs() < 1e-12, "got {v}, expected {expected}");
}

#[test]
fn test_stddev_samp_two_phase() {
    let v = run_two_phase_i64(
        "stddev_samp",
        vec![Some(1), Some(2)],
        vec![Some(3), Some(4)],
    )
    .expect("non-null");
    let expected = (5.0f64 / 3.0f64).sqrt();
    assert!((v - expected).abs() < 1e-12, "got {v}, expected {expected}");
}

#[test]
fn test_stddev_samp_null_on_single_value() {
    let v = run_two_phase_i64("stddev_samp", vec![Some(7)], vec![]);
    assert!(v.is_none(), "expected NULL, got {v:?}");
}

#[test]
fn test_variance_pop_null_on_empty() {
    let v = run_two_phase_i64("variance_pop", vec![None], vec![None]);
    assert!(v.is_none(), "expected NULL, got {v:?}");
}

#[test]
fn test_std_alias_matches_stddev_pop() {
    let v = run_two_phase_i64("std", vec![Some(1)], vec![Some(2)]).expect("non-null");
    // pop variance([1,2]) = 0.25 -> stddev_pop = 0.5
    assert!((v - 0.5).abs() < 1e-12, "got {v}, expected 0.5");
}

#[test]
fn test_sum_bool_counts_true_as_one() {
    let func = AggFunction {
        name: "sum".to_string(),
        inputs: vec![ExprId(0)],
        input_is_intermediate: false,
        types: Some(AggTypeSignature {
            intermediate_type: Some(DataType::Int64),
            output_type: Some(DataType::Int64),
            input_arg_type: Some(DataType::Boolean),
        }),
        order: Default::default(),
    };

    let input = Arc::new(BooleanArray::from(vec![
        Some(true),
        Some(false),
        None,
        Some(true),
    ])) as ArrayRef;
    let arrays = [Some(Arc::clone(&input))];
    let input_types = vec![Some(DataType::Boolean)];
    let kernels = build_builtin_kernel_set(&[func], &input_types, &[vec![DataType::Boolean]])
        .expect("build kernels");
    let kernel = &kernels.entries[0];

    let mut arena = agg::AggStateArena::new(64 * 1024);
    let base = arena.alloc(kernels.layout.total_size, kernel.state_align());
    kernel.init_state(base).expect("init state");

    let state_ptrs = vec![base; input.len()];
    kernel
        .update_batch(
            &state_ptrs,
            AggregateInputBatch::try_new(arrays[0].as_ref(), input.len()).expect("build input"),
        )
        .expect("update");

    let out = kernel.build_array(&[base], false).expect("build out");
    let out = out
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 output");
    assert_eq!(out.value(0), 2);
}

#[test]
fn test_sum_bool_null_when_all_null() {
    let func = AggFunction {
        name: "sum".to_string(),
        inputs: vec![ExprId(0)],
        input_is_intermediate: false,
        types: Some(AggTypeSignature {
            intermediate_type: Some(DataType::Int64),
            output_type: Some(DataType::Int64),
            input_arg_type: Some(DataType::Boolean),
        }),
        order: Default::default(),
    };

    let input = Arc::new(BooleanArray::from(vec![None, None])) as ArrayRef;
    let arrays = [Some(Arc::clone(&input))];
    let input_types = vec![Some(DataType::Boolean)];
    let kernels = build_builtin_kernel_set(&[func], &input_types, &[vec![DataType::Boolean]])
        .expect("build kernels");
    let kernel = &kernels.entries[0];

    let mut arena = agg::AggStateArena::new(64 * 1024);
    let base = arena.alloc(kernels.layout.total_size, kernel.state_align());
    kernel.init_state(base).expect("init state");

    let state_ptrs = vec![base; input.len()];
    kernel
        .update_batch(
            &state_ptrs,
            AggregateInputBatch::try_new(arrays[0].as_ref(), input.len()).expect("build input"),
        )
        .expect("update");

    let out = kernel.build_array(&[base], false).expect("build out");
    let out = out
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 output");
    assert!(out.is_null(0));
}
