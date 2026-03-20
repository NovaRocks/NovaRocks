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

use novarocks::exec::expr::ExprId;
use novarocks::exec::expr::agg;
use novarocks::exec::node::aggregate::{AggFunction, AggTypeSignature};

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
    };

    // Partial aggregation for each partition.
    let input1 = Arc::new(Int64Array::from(part1)) as ArrayRef;
    let input2 = Arc::new(Int64Array::from(part2)) as ArrayRef;

    let arrays1 = vec![Some(Arc::clone(&input1))];
    let arrays2 = vec![Some(Arc::clone(&input2))];
    let input_types = vec![Some(DataType::Int64)];
    let kernels = agg::build_kernel_set(&[func.clone()], &input_types).unwrap();
    let kernel = &kernels.entries[0];

    let mut arena = agg::AggStateArena::new(64 * 1024);
    let base1 = arena.alloc(kernels.layout.total_size, kernel.state_align());
    let base2 = arena.alloc(kernels.layout.total_size, kernel.state_align());
    kernel.init_state(base1);
    kernel.init_state(base2);

    let view1 = kernel.build_input_view(&arrays1[0]).unwrap();
    let state_ptrs1 = vec![base1; input1.len()];
    kernel.update_batch(&state_ptrs1, &view1).unwrap();

    let view2 = kernel.build_input_view(&arrays2[0]).unwrap();
    let state_ptrs2 = vec![base2; input2.len()];
    kernel.update_batch(&state_ptrs2, &view2).unwrap();

    // Build intermediate outputs (one row per partition state).
    let intermediate = kernel.build_array(&[base1, base2], true).unwrap();
    let intermediate = intermediate
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("binary intermediate");

    // Final aggregation merges intermediate states.
    let mut func_merge = func;
    func_merge.input_is_intermediate = true;
    let kernels_merge =
        agg::build_kernel_set(&[func_merge], &[Some(intermediate.data_type().clone())]).unwrap();
    let kernel_merge = &kernels_merge.entries[0];

    let base_final = arena.alloc(kernels_merge.layout.total_size, kernel_merge.state_align());
    kernel_merge.init_state(base_final);

    let merge_input = vec![Some(Arc::new(intermediate.clone()) as ArrayRef)];
    let merge_view = kernel_merge.build_merge_view(&merge_input[0]).unwrap();
    let merge_state_ptrs = vec![base_final; intermediate.len()];
    kernel_merge
        .merge_batch(&merge_state_ptrs, &merge_view)
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
    };

    let input = Arc::new(BooleanArray::from(vec![
        Some(true),
        Some(false),
        None,
        Some(true),
    ])) as ArrayRef;
    let arrays = vec![Some(Arc::clone(&input))];
    let input_types = vec![Some(DataType::Boolean)];
    let kernels = agg::build_kernel_set(&[func], &input_types).expect("build kernels");
    let kernel = &kernels.entries[0];

    let mut arena = agg::AggStateArena::new(64 * 1024);
    let base = arena.alloc(kernels.layout.total_size, kernel.state_align());
    kernel.init_state(base);

    let view = kernel.build_input_view(&arrays[0]).expect("build view");
    let state_ptrs = vec![base; input.len()];
    kernel.update_batch(&state_ptrs, &view).expect("update");

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
    };

    let input = Arc::new(BooleanArray::from(vec![None, None])) as ArrayRef;
    let arrays = vec![Some(Arc::clone(&input))];
    let input_types = vec![Some(DataType::Boolean)];
    let kernels = agg::build_kernel_set(&[func], &input_types).expect("build kernels");
    let kernel = &kernels.entries[0];

    let mut arena = agg::AggStateArena::new(64 * 1024);
    let base = arena.alloc(kernels.layout.total_size, kernel.state_align());
    kernel.init_state(base);

    let view = kernel.build_input_view(&arrays[0]).expect("build view");
    let state_ptrs = vec![base; input.len()];
    kernel.update_batch(&state_ptrs, &view).expect("update");

    let out = kernel.build_array(&[base], false).expect("build out");
    let out = out
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 output");
    assert!(out.is_null(0));
}
