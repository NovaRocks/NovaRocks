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

#[test]
fn test_avg_decimal_round_half_up_positive() {
    // avg(0.000000000001, 0.000000000002) = 0.0000000000015
    // -> ROUND_HALF_UP at scale 12 => 0.000000000002.
    let input = Arc::new(
        Decimal128Array::from(vec![Some(1_i128), Some(2_i128)])
            .with_precision_and_scale(18, 12)
            .unwrap(),
    ) as ArrayRef;

    let func = AggFunction {
        name: "avg".to_string(),
        inputs: vec![ExprId(0)],
        input_is_intermediate: false,
        types: Some(AggTypeSignature {
            intermediate_type: None,
            output_type: Some(DataType::Decimal128(38, 12)),
            input_arg_type: None,
        }),
        order: Default::default(),
    };

    let arrays = [Some(input.clone())];
    let input_types = vec![Some(input.data_type().clone())];
    let kernels =
        build_builtin_kernel_set(&[func], &input_types, &[vec![input.data_type().clone()]])
            .unwrap();

    let mut arena = agg::AggStateArena::new(64 * 1024);
    let base = arena.alloc(kernels.layout.total_size, kernels.entries[0].state_align());
    kernels.entries[0].init_state(base).expect("init state");

    let state_ptrs = vec![base; 2];
    kernels.entries[0]
        .update_batch(
            &state_ptrs,
            AggregateInputBatch::try_new(arrays[0].as_ref(), 2).unwrap(),
        )
        .unwrap();

    let out = kernels.entries[0].build_array(&[base], false).unwrap();
    let out = out
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .expect("decimal output");
    assert_eq!(out.data_type(), &DataType::Decimal128(38, 12));
    assert_eq!(out.value(0), 2_i128);
}

#[test]
fn test_avg_decimal_round_half_up_negative() {
    // avg(-0.000000000001, -0.000000000002) = -0.0000000000015
    // -> ROUND_HALF_UP (away from zero) at scale 12 => -0.000000000002.
    let input = Arc::new(
        Decimal128Array::from(vec![Some(-1_i128), Some(-2_i128)])
            .with_precision_and_scale(18, 12)
            .unwrap(),
    ) as ArrayRef;

    let func = AggFunction {
        name: "avg".to_string(),
        inputs: vec![ExprId(0)],
        input_is_intermediate: false,
        types: Some(AggTypeSignature {
            intermediate_type: None,
            output_type: Some(DataType::Decimal128(38, 12)),
            input_arg_type: None,
        }),
        order: Default::default(),
    };

    let arrays = [Some(input.clone())];
    let input_types = vec![Some(input.data_type().clone())];
    let kernels =
        build_builtin_kernel_set(&[func], &input_types, &[vec![input.data_type().clone()]])
            .unwrap();

    let mut arena = agg::AggStateArena::new(64 * 1024);
    let base = arena.alloc(kernels.layout.total_size, kernels.entries[0].state_align());
    kernels.entries[0].init_state(base).expect("init state");

    let state_ptrs = vec![base; 2];
    kernels.entries[0]
        .update_batch(
            &state_ptrs,
            AggregateInputBatch::try_new(arrays[0].as_ref(), 2).unwrap(),
        )
        .unwrap();

    let out = kernels.entries[0].build_array(&[base], false).unwrap();
    let out = out
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .expect("decimal output");
    assert_eq!(out.data_type(), &DataType::Decimal128(38, 12));
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
        order: Default::default(),
    };

    let err = build_builtin_kernel_set(
        &[func],
        &[Some(DataType::Binary)],
        &[vec![DataType::Binary]],
    )
    .expect_err("expected error without output_type signature");

    assert!(
        err.contains("output_type signature"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_avg_intermediate_utf8_with_exact_catalog_signature_builds() {
    let func = AggFunction {
        name: "avg".to_string(),
        inputs: vec![ExprId(0)],
        input_is_intermediate: true,
        types: Some(AggTypeSignature {
            intermediate_type: Some(DataType::Utf8),
            output_type: Some(DataType::Decimal128(38, 8)),
            input_arg_type: Some(DataType::Decimal128(10, 2)),
        }),
        order: Default::default(),
    };

    let kernels = build_builtin_kernel_set(
        &[func],
        &[Some(DataType::Utf8)],
        &[vec![DataType::Decimal128(10, 2)]],
    )
    .unwrap();

    assert_eq!(kernels.entries.len(), 1);
    assert_eq!(
        kernels.entries[0].output_type(false),
        DataType::Decimal128(38, 8)
    );
}
