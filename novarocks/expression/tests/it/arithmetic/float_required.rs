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

//! Float arithmetic uses the existing full source/effects compiler and Frame owner.
use super::*;
use arrow::array::{Array, BooleanArray, Float64Array};
use novarocks_expression::testkit::PassThroughCallHost;
use novarocks_expression::{EvaluationInput, ExprEvaluationFailure};
use novarocks_functions::{ScalarInvocationActivation, ScalarInvocationScope};

fn f64_values() -> ArrayRef {
    Arc::new(Float64Array::from(vec![
        Some(1.5),
        Some(-2.25),
        Some(0.0),
        Some(-0.0),
        Some(f64::INFINITY),
        Some(f64::NEG_INFINITY),
        Some(f64::from_bits(0x7ff8_0000_0000_0042)),
        Some(f64::MAX),
        Some(f64::MIN_POSITIVE),
        Some(f64::from_bits(1)),
        None,
        Some(7.0),
    ]))
}

fn ieee_nan_bits(op: BinaryOperator, left: f64, right: f64) -> u64 {
    // IEEE/Rust does not prescribe one universal NaN payload/sign. Use the
    // target's independent Rust f64 primitive for that representation only,
    // preserving the exact-bit check without guessing a host-specific NaN.
    // black_box keeps this a runtime primitive, not a constant-folded NaN.
    // Neither the retired evaluator nor the Functions recipe is an oracle.
    let left = std::hint::black_box(left);
    let right = std::hint::black_box(right);
    let value = match op {
        BinaryOperator::Add => left + right,
        BinaryOperator::Subtract => left - right,
        BinaryOperator::Multiply => left * right,
        BinaryOperator::Divide => left / right,
        BinaryOperator::Modulo => left % right,
        _ => panic!("exact five arithmetic operators only"),
    };
    assert!(value.is_nan(), "the standards oracle is only for NaN cases");
    value.to_bits()
}

fn expected_bits(op: BinaryOperator) -> [Option<u64>; 6] {
    // The selected input rows [0,2,3,6,10,11] are
    // [1.5,+0,-0,qNaN(payload42),NULL,7], paired with themselves.
    // Binary-exact finite arithmetic gives 3, 2.25, 14, 49, 1 and +0.
    // Addition of two -0 values gives -0; subtraction and multiplication
    // of identical signed zeros give +0. NULL input always propagates.
    // The established SQL F64 division rule maps either RHS signed zero
    // to successful NULL, while remainder keeps IEEE invalid-operation NaN.
    // Finite values/zero signs/NULLs below are authored literals, independent
    // of either arithmetic implementation and of the NaN representation oracle.
    let nan = f64::from_bits(0x7ff8_0000_0000_0042);
    let nan = Some(ieee_nan_bits(op, nan, nan));
    match op {
        BinaryOperator::Add => [
            Some(0x4008_0000_0000_0000),
            Some(0),
            Some(0x8000_0000_0000_0000),
            nan,
            None,
            Some(0x402c_0000_0000_0000),
        ],
        BinaryOperator::Subtract => [Some(0), Some(0), Some(0), nan, None, Some(0)],
        BinaryOperator::Multiply => [
            Some(0x4002_0000_0000_0000),
            Some(0),
            Some(0),
            nan,
            None,
            Some(0x4048_8000_0000_0000),
        ],
        BinaryOperator::Divide => [
            Some(0x3ff0_0000_0000_0000),
            None,
            None,
            nan,
            None,
            Some(0x3ff0_0000_0000_0000),
        ],
        BinaryOperator::Modulo => [
            Some(0),
            Some(ieee_nan_bits(op, 0.0, 0.0)),
            Some(ieee_nan_bits(op, -0.0, -0.0)),
            nan,
            None,
            Some(0),
        ],
        _ => panic!("exact five arithmetic operators only"),
    }
}
#[test]
fn float_arithmetic_required_actual_compiler_frame_sparse_bits_and_empty_selection() {
    for op in [
        BinaryOperator::Add,
        BinaryOperator::Subtract,
        BinaryOperator::Multiply,
        BinaryOperator::Divide,
        BinaryOperator::Modulo,
    ] {
        let program = build_program(op, DataType::Float64, DataType::Float64, Wrap::Bare, false);
        let left = f64_values();
        let right = f64_values();
        let batch = RecordBatch::try_new(
            program.input_schema().clone(),
            vec![
                left.clone(),
                right.clone(),
                Arc::new(BooleanArray::from(vec![None; 12])),
            ],
        )
        .unwrap();
        let expected = expected_bits(op);
        for rows in [&[0, 2, 3, 6, 10, 11][..], &[][..]] {
            let selection = Selection::try_sparse(12, rows).unwrap();
            let out = instance(&program)
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &batch,
                        source: None,
                    },
                    ScalarInvocationScope::new(selection, selection),
                    ScalarInvocationActivation::Activated,
                    &Control,
                    &mut PassThroughCallHost,
                )
                .unwrap();
            assert_eq!(out.selection(), selection);
            assert!(out.errors().is_empty());
            let values = out
                .values()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            for (ordinal, row) in rows.iter().copied().enumerate() {
                assert_eq!(
                    values.is_null(ordinal),
                    expected[ordinal].is_none(),
                    "row {row}"
                );
                if let Some(bits) = expected[ordinal] {
                    assert_eq!(values.value(ordinal).to_bits(), bits, "row {row}");
                }
            }
        }
    }
}
#[test]
fn float_arithmetic_required_actual_frame_seven_causes_prefix_and_failed_latch() {
    let program = build_program(
        BinaryOperator::Divide,
        DataType::Float64,
        DataType::Float64,
        Wrap::Bare,
        false,
    );
    let values = (0..320)
        .map(|row| if row % 5 == 0 { None } else { Some(row as f64) })
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        program.input_schema().clone(),
        vec![
            Arc::new(Float64Array::from(values)),
            Arc::new(Float64Array::from(vec![Some(2.0); 320])),
            Arc::new(BooleanArray::from(vec![None; 320])),
        ],
    )
    .unwrap();
    let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
    let out = instance(&program)
        .evaluate(
            EvaluationInput::Batch {
                batch: &batch,
                source: None,
            },
            ScalarInvocationScope::new(Selection::all(320), Selection::all(320)),
            ScalarInvocationActivation::Activated,
            &recorder,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert!(out.errors().is_empty());
    let trace = recorder.trace();
    assert!(trace.contains(&256));
    for stop in 1..=trace.len() {
        for cause in causes() {
            let mut evaluator = instance(&program);
            let control = CallbackControl::new(cause.clone(), stop);
            assert!(
                matches!(evaluator.evaluate(EvaluationInput::Batch { batch: &batch, source: None }, ScalarInvocationScope::new(Selection::all(320), Selection::all(320)), ScalarInvocationActivation::Activated, &control, &mut PassThroughCallHost), Err(ExprEvaluationFailure::Kernel(actual)) if actual==cause)
            );
            assert_eq!(control.trace(), trace[..stop]);
            let after = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
            assert!(matches!(
                evaluator.evaluate(
                    EvaluationInput::Batch {
                        batch: &batch,
                        source: None
                    },
                    ScalarInvocationScope::new(Selection::all(320), Selection::all(320)),
                    ScalarInvocationActivation::Activated,
                    &after,
                    &mut PassThroughCallHost
                ),
                Err(ExprEvaluationFailure::Kernel(KernelFailure::InstanceFailed))
            ));
            assert!(after.trace().is_empty());
        }
    }
}
