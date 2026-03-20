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

use arrow::array::{Array, ArrayRef, Decimal128Array};
use arrow::datatypes::DataType;

use novarocks::exec::expr::ExprId;
use novarocks::exec::expr::agg;
use novarocks::exec::node::aggregate::{AggFunction, AggTypeSignature};

#[test]
fn test_avg_decimal_round_half_up_positive() {
    // avg(0.01, 0.02) = 0.015 -> ROUND_HALF_UP => 0.02
    let input = Arc::new(
        Decimal128Array::from(vec![Some(1_i128), Some(2_i128)])
            .with_precision_and_scale(10, 2)
            .unwrap(),
    ) as ArrayRef;

    let func = AggFunction {
        name: "avg".to_string(),
        inputs: vec![ExprId(0)],
        input_is_intermediate: false,
        types: Some(AggTypeSignature {
            intermediate_type: None,
            output_type: Some(DataType::Decimal128(10, 2)),
            input_arg_type: None,
        }),
    };

    let arrays = vec![Some(input.clone())];
    let input_types = vec![Some(input.data_type().clone())];
    let kernels = agg::build_kernel_set(&[func], &input_types).unwrap();

    let mut arena = agg::AggStateArena::new(64 * 1024);
    let base = arena.alloc(kernels.layout.total_size, kernels.entries[0].state_align());
    kernels.entries[0].init_state(base);

    let view = kernels.entries[0].build_input_view(&arrays[0]).unwrap();
    let state_ptrs = vec![base; 2];
    kernels.entries[0].update_batch(&state_ptrs, &view).unwrap();

    let out = kernels.entries[0].build_array(&[base], false).unwrap();
    let out = out
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .expect("decimal output");
    assert_eq!(out.data_type(), &DataType::Decimal128(10, 2));
    assert_eq!(out.value(0), 2_i128);
}

#[test]
fn test_avg_decimal_round_half_up_negative() {
    // avg(-0.01, -0.02) = -0.015 -> ROUND_HALF_UP (away from zero) => -0.02
    let input = Arc::new(
        Decimal128Array::from(vec![Some(-1_i128), Some(-2_i128)])
            .with_precision_and_scale(10, 2)
            .unwrap(),
    ) as ArrayRef;

    let func = AggFunction {
        name: "avg".to_string(),
        inputs: vec![ExprId(0)],
        input_is_intermediate: false,
        types: Some(AggTypeSignature {
            intermediate_type: None,
            output_type: Some(DataType::Decimal128(10, 2)),
            input_arg_type: None,
        }),
    };

    let arrays = vec![Some(input.clone())];
    let input_types = vec![Some(input.data_type().clone())];
    let kernels = agg::build_kernel_set(&[func], &input_types).unwrap();

    let mut arena = agg::AggStateArena::new(64 * 1024);
    let base = arena.alloc(kernels.layout.total_size, kernels.entries[0].state_align());
    kernels.entries[0].init_state(base);

    let view = kernels.entries[0].build_input_view(&arrays[0]).unwrap();
    let state_ptrs = vec![base; 2];
    kernels.entries[0].update_batch(&state_ptrs, &view).unwrap();

    let out = kernels.entries[0].build_array(&[base], false).unwrap();
    let out = out
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .expect("decimal output");
    assert_eq!(out.data_type(), &DataType::Decimal128(10, 2));
    assert_eq!(out.value(0), -2_i128);
}

#[test]
fn test_avg_intermediate_binary_requires_type_signature() {
    // Strict mode: FE output_type signature is required.
    let func = AggFunction {
        name: "avg".to_string(),
        inputs: vec![ExprId(0)],
        input_is_intermediate: true,
        types: Some(AggTypeSignature {
            intermediate_type: None,
            output_type: None,
            input_arg_type: None,
        }),
    };

    let err = agg::build_kernel_set(&[func], &[Some(DataType::Binary)])
        .expect_err("expected error without output_type signature");

    assert!(
        err.contains("output_type signature"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_avg_intermediate_binary_with_signature_builds() {
    let func = AggFunction {
        name: "avg".to_string(),
        inputs: vec![ExprId(0)],
        input_is_intermediate: true,
        types: Some(AggTypeSignature {
            intermediate_type: Some(DataType::Binary),
            output_type: Some(DataType::Decimal128(10, 2)),
            input_arg_type: Some(DataType::Decimal128(10, 2)),
        }),
    };

    let kernels = agg::build_kernel_set(&[func], &[Some(DataType::Binary)]).unwrap();

    assert_eq!(kernels.entries.len(), 1);
    assert_eq!(
        kernels.entries[0].output_type(false),
        DataType::Decimal128(10, 2)
    );
}
