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

use super::*;
use crate::exec::expr::ExprId;
use crate::exec::expr::agg::{
    AggKernelSet, AggStateArena, build_kernel_set, test_builtin_execution_function_set,
};
use crate::exec::node::aggregate::AggTypeSignature;
use arrow::array::{BinaryArray, Float64Array, Int32Array};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::Field;
use novarocks_functions::AggregateInputBatch;

fn input(values: Vec<Option<f64>>, weights: Vec<Option<i32>>, quantiles: ArrayRef) -> ArrayRef {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Float64Array::from(values)),
        Arc::new(Int32Array::from(weights)),
        quantiles,
    ];
    let fields = ["value", "weight", "quantile"]
        .into_iter()
        .zip(&columns)
        .map(|(name, column)| Arc::new(Field::new(name, column.data_type().clone(), true)))
        .collect::<Vec<_>>();
    Arc::new(StructArray::try_new(fields.into(), columns, None).unwrap())
}

fn scalar_input(values: Vec<Option<f64>>, weights: Vec<Option<i32>>) -> ArrayRef {
    let quantiles = Arc::new(Float64Array::from(vec![0.5; values.len()]));
    input(values, weights, quantiles)
}

fn array_input(value: f64, weight: i32) -> ArrayRef {
    let quantiles = ListArray::try_new(
        Arc::new(Field::new("item", DataType::Float64, true)),
        OffsetBuffer::new(vec![0_i32, 4].into()),
        Arc::new(Float64Array::from(vec![0.0, 0.25, 0.5, 1.0])),
        None,
    )
    .unwrap();
    input(vec![Some(value)], vec![Some(weight)], Arc::new(quantiles))
}

fn kernels(packed_type: &DataType, merge: bool) -> AggKernelSet {
    let DataType::Struct(fields) = packed_type else {
        panic!("packed aggregate input");
    };
    let arguments = fields
        .iter()
        .map(|field| field.data_type().clone())
        .collect::<Vec<_>>();
    let functions = test_builtin_execution_function_set();
    let selected = functions
        .catalog()
        .resolve_aggregate_trusted("percentile_approx_weighted", &arguments)
        .unwrap();
    assert_eq!(selected.argument_types, arguments);
    assert_eq!(selected.intermediate_type, DataType::Binary);
    let expected_output = if matches!(arguments[2], DataType::List(_)) {
        DataType::List(Arc::new(Field::new("item", DataType::Float64, true)))
    } else {
        DataType::Float64
    };
    assert_eq!(selected.output_type, expected_output);
    let function = AggFunction {
        name: "percentile_approx_weighted".to_string(),
        inputs: vec![ExprId(0)],
        input_is_intermediate: merge,
        types: Some(AggTypeSignature {
            intermediate_type: Some(DataType::Binary),
            output_type: Some(expected_output),
            input_arg_type: Some(arguments[0].clone()),
        }),
        ..Default::default()
    };
    let evaluated = if merge {
        DataType::Binary
    } else {
        packed_type.clone()
    };
    build_kernel_set(&functions, &[function], &[Some(evaluated)], &[selected]).unwrap()
}

fn state(
    kernels: &AggKernelSet,
    arena: &mut AggStateArena,
    tracker: &Arc<MemTracker>,
) -> AggStatePtr {
    let kernel = &kernels.entries[0];
    let ptr = arena.alloc(kernels.layout.total_size, kernel.state_align());
    kernel
        .init_state_with_tracker(ptr, Arc::clone(tracker))
        .unwrap();
    ptr
}

fn scalar_result(kernels: &AggKernelSet, ptr: AggStatePtr) -> Option<f64> {
    let result = kernels.entries[0].build_array(&[ptr], false).unwrap();
    assert_eq!(result.data_type(), &DataType::Float64);
    let result = result.as_any().downcast_ref::<Float64Array>().unwrap();
    (!result.is_null(0)).then(|| result.value(0))
}

#[test]
fn prepared_weighted_scalar_singletons_cross_binary_merge_and_roundtrip() {
    let tracker = MemTracker::new_root("weighted-percentile-prepared-scalar");
    let mut arena = AggStateArena::new(4096);
    arena.set_mem_tracker(Arc::clone(&tracker));
    let inputs = [
        scalar_input(
            vec![Some(2.0), None, Some(1000000.0)],
            vec![Some(1), Some(17), Some(0)],
        ),
        scalar_input(vec![Some(3.0), Some(-1000000.0)], vec![Some(2), None]),
        scalar_input(vec![Some(4.0)], vec![Some(3)]),
    ];
    let local = kernels(inputs[0].data_type(), false);
    let merged = kernels(inputs[0].data_type(), true);
    let empty = state(&local, &mut arena, &tracker);
    assert_eq!(scalar_result(&local, empty), None);
    let empty_partial = local.entries[0].build_array(&[empty], true).unwrap();
    local.entries[0].drop_state(empty);
    let mut partials = Vec::new();
    for (index, input) in inputs.iter().enumerate() {
        let ptr = state(&local, &mut arena, &tracker);
        local.entries[0]
            .update_batch(
                &vec![ptr; input.len()],
                AggregateInputBatch::try_new(Some(input), input.len()).unwrap(),
            )
            .unwrap();
        // Each partition has only one positive-weight value, even with NULL/zero controls.
        assert_eq!(scalar_result(&local, ptr), Some([2.0, 3.0, 4.0][index]));
        let partial = local.entries[0].build_array(&[ptr], true).unwrap();
        assert_eq!(partial.data_type(), &DataType::Binary);
        assert!(
            !partial
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .is_null(0)
        );
        partials.push(partial);
        local.entries[0].drop_state(ptr);
    }
    for order in [[0, 1, 2], [2, 1, 0], [1, 0, 2]] {
        let root = state(&merged, &mut arena, &tracker);
        for partial in std::iter::once(&empty_partial).chain(order.iter().map(|&i| &partials[i])) {
            merged.entries[0]
                .merge_batch(
                    &[root],
                    AggregateInputBatch::try_new(Some(partial), 1).unwrap(),
                )
                .unwrap();
        }
        // Centers .5/2/4.5, total6, median index3 => (3*1.5+4*1)/2.5.
        assert_eq!(scalar_result(&merged, root), Some(f64::from(3.4_f32)));
        let serialized = merged.entries[0].build_array(&[root], true).unwrap();
        merged.entries[0].drop_state(root);
        let replay = state(&merged, &mut arena, &tracker);
        merged.entries[0]
            .merge_batch(
                &[replay],
                AggregateInputBatch::try_new(Some(&serialized), 1).unwrap(),
            )
            .unwrap();
        assert_eq!(scalar_result(&merged, replay), Some(f64::from(3.4_f32)));
        merged.entries[0].drop_state(replay);
    }
    drop(arena);
    assert_eq!(
        tracker.current(),
        0,
        "state and arena allocations must be released"
    );
}

#[test]
fn prepared_weighted_array_tuple_preserves_quantile_order_across_merge() {
    let tracker = MemTracker::new_root("weighted-percentile-prepared-array");
    let mut arena = AggStateArena::new(4096);
    arena.set_mem_tracker(Arc::clone(&tracker));
    let first = array_input(2.0, 1);
    let local = kernels(first.data_type(), false);
    let merged = kernels(first.data_type(), true);
    let mut partials = Vec::new();
    for (value, weight) in [(2.0, 1), (3.0, 2), (4.0, 3)] {
        let input = array_input(value, weight);
        let ptr = state(&local, &mut arena, &tracker);
        local.entries[0]
            .update_batch(
                &[ptr],
                AggregateInputBatch::try_new(Some(&input), 1).unwrap(),
            )
            .unwrap();
        partials.push(local.entries[0].build_array(&[ptr], true).unwrap());
        local.entries[0].drop_state(ptr);
    }
    let root = state(&merged, &mut arena, &tracker);
    for partial in partials.iter().rev() {
        merged.entries[0]
            .merge_batch(
                &[root],
                AggregateInputBatch::try_new(Some(partial), 1).unwrap(),
            )
            .unwrap();
    }
    let serialized = merged.entries[0].build_array(&[root], true).unwrap();
    merged.entries[0].drop_state(root);
    let replay = state(&merged, &mut arena, &tracker);
    merged.entries[0]
        .merge_batch(
            &[replay],
            AggregateInputBatch::try_new(Some(&serialized), 1).unwrap(),
        )
        .unwrap();
    let output = merged.entries[0].build_array(&[replay], false).unwrap();
    assert_eq!(
        output.data_type(),
        &DataType::List(Arc::new(Field::new("item", DataType::Float64, true)))
    );
    let output = output.as_any().downcast_ref::<ListArray>().unwrap();
    assert!(!output.is_null(0));
    assert_eq!(output.value_offsets(), &[0, 4]);
    let values = output
        .values()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(values.null_count(), 0);
    // q=.25 index1.5, centers.5/2 => (2*.5+3*1)/1.5 =8/3, stored f32.
    assert_eq!(
        values.values().as_ref(),
        &[2.0, f64::from(8.0_f32 / 3.0), f64::from(3.4_f32), 4.0]
    );
    merged.entries[0].drop_state(replay);
    drop(arena);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn prepared_weighted_q56_int32_input_matches_independent_single_state_cdf() {
    let tracker = MemTracker::new_root("weighted-percentile-prepared-q56");
    let mut arena = AggStateArena::new(4096);
    arena.set_mem_tracker(Arc::clone(&tracker));
    // The original q56 uses DOUBLE c2 and INT c1; c3 TINYINT is not an input.
    let integers = (1_i32..=50000)
        .step_by(3)
        .chain([1, 2, 3, 4])
        .collect::<Vec<_>>();
    assert_eq!(integers.len(), 16671);
    assert_eq!(
        integers.iter().map(|&x| i64::from(x)).sum::<i64>(),
        416675010
    );
    let input = scalar_input(
        integers.iter().map(|&x| Some(f64::from(x))).collect(),
        integers.into_iter().map(Some).collect(),
    );
    let local = kernels(input.data_type(), false);
    let ptr = state(&local, &mut arena, &tracker);
    local.entries[0]
        .update_batch(
            &vec![ptr; input.len()],
            AggregateInputBatch::try_new(Some(&input), input.len()).unwrap(),
        )
        .unwrap();
    // Independent IEEE/CDF calculation for this exact input order, not all distributed trees:
    // stored mass416681600 -> index208340800; centers208305776/208341136.
    let value = scalar_result(&local, ptr).unwrap();
    assert_eq!(value, 35355.9765625);
    assert_eq!(value.trunc(), 35355.0);
    local.entries[0].drop_state(ptr);
    drop(arena);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn prepared_weighted_negative_weight_is_rejected_without_digest_contribution() {
    let tracker = MemTracker::new_root("weighted-percentile-prepared-negative");
    let mut arena = AggStateArena::new(4096);
    arena.set_mem_tracker(Arc::clone(&tracker));
    let input = scalar_input(vec![Some(2.0)], vec![Some(-1)]);
    let local = kernels(input.data_type(), false);
    let ptr = state(&local, &mut arena, &tracker);
    let error = local.entries[0]
        .update_batch(
            &[ptr],
            AggregateInputBatch::try_new(Some(&input), 1).unwrap(),
        )
        .unwrap_err();
    assert!(
        error.contains("percentile weight must be non-negative"),
        "{error}"
    );
    assert_eq!(scalar_result(&local, ptr), None);
    local.entries[0].drop_state(ptr);
    drop(arena);
    assert_eq!(tracker.current(), 0);
}
