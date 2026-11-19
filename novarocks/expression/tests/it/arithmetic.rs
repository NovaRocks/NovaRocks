// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use arrow::{
    array::{
        Array, ArrayRef, BooleanArray, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
    },
    datatypes::DataType,
    record_batch::RecordBatch,
};
use novarocks_expression::testkit::numeric_fixtures::operation;
use novarocks_expression::testkit::numeric_fixtures::{
    CompiledFixture, Control, Wrap, build_program, causes, errors, input, instance,
};
use novarocks_expression::testkit::{CallbackControl, PassThroughCallHost};
use novarocks_expression::{EvaluationInput, ExprEvaluationFailure, StaticExprKind};
use novarocks_expression_contract::BinaryOperator;
use novarocks_functions::{
    KernelFailure, ScalarInvocationActivation, ScalarInvocationScope, Selection,
};
use novarocks_type_contract::{DecimalOverflowPolicy, FunctionValueType};
use std::sync::Arc;

mod float_required;
mod numeric;

fn integral(output: &novarocks_functions::SelectedValues<'_>) -> Vec<Option<i64>> {
    let array = output.values();
    match array.data_type() {
        DataType::Int8 => array
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap()
            .iter()
            .map(|v| v.map(i64::from))
            .collect(),
        DataType::Int16 => array
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap()
            .iter()
            .map(|v| v.map(i64::from))
            .collect(),
        DataType::Int32 => array
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .map(|v| v.map(i64::from))
            .collect(),
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect(),
        _ => panic!("exact frozen integral output"),
    }
}
#[test]
fn five_signed_operators_keep_frozen_mixed_widths_and_sparse_selected_addresses() {
    for (left, right, expected_carrier) in [
        (DataType::Int8, DataType::Int8, DataType::Int8),
        (DataType::Int16, DataType::Int8, DataType::Int16),
        (DataType::Int16, DataType::Int32, DataType::Int32),
        (DataType::Int32, DataType::Int64, DataType::Int64),
    ] {
        for (op, integral_expected) in [
            (BinaryOperator::Add, vec![Some(4), Some(-4), None]),
            (BinaryOperator::Subtract, vec![Some(10), Some(-10), None]),
            (BinaryOperator::Multiply, vec![Some(-21), Some(-21), None]),
            (BinaryOperator::Divide, vec![Some(-2), Some(-2), None]),
            (BinaryOperator::Modulo, vec![Some(1), Some(-1), None]),
        ] {
            let program = build_program(op, left.clone(), right.clone(), Wrap::Bare, true);
            let batch = input(
                &program,
                &[Some(99), Some(7), Some(-7), None, Some(8)],
                &[None, Some(-3), Some(3), Some(0), Some(2)],
                &[None; 5],
            );
            let rows = [1, 2, 3];
            let selection = Selection::try_sparse(5, &rows).unwrap();
            let output = instance(&program)
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
            assert_eq!(output.selection(), selection);
            assert_eq!(output.values().data_type(), &expected_carrier);
            assert_eq!(integral(&output), integral_expected);
            assert!(output.errors().is_empty());
            let unit = &program.unit;
            let actual = unit.root(program.root).unwrap();
            let recipe = unit.recipes().arithmetic(actual.use_id).unwrap();
            assert_eq!(recipe.operator(), operation(op));
            assert_eq!(recipe.left_type().data_type().clone(), left);
            assert_eq!(recipe.right_type().data_type().clone(), right);
            assert!(recipe.allow_throw_exception());
            assert_eq!(
                recipe.decimal_overflow_policy(),
                DecimalOverflowPolicy::ReportError
            );
            assert!(matches!(
                unit.definitions().node(actual.definition).unwrap().kind(),
                StaticExprKind::PreparedArithmetic { .. }
            ));
        }
    }
}

#[test]
fn signed_overflow_and_modulo_faults_use_selected_row_errors_independent_of_allow_flag() {
    for allow in [false, true] {
        for (op, left, right, expected, error_ordinals) in [
            (
                BinaryOperator::Add,
                vec![Some(8), Some(i64::MAX), None, Some(i64::MIN), Some(7)],
                vec![Some(9), Some(1), Some(1), Some(-1), Some(-3)],
                vec![None, None, None, Some(4)],
                vec![0, 2],
            ),
            (
                BinaryOperator::Subtract,
                vec![Some(8), Some(i64::MIN), None, Some(i64::MAX), Some(7)],
                vec![Some(9), Some(1), Some(1), Some(-1), Some(-3)],
                vec![None, None, None, Some(10)],
                vec![0, 2],
            ),
            (
                BinaryOperator::Multiply,
                vec![Some(8), Some(i64::MAX), None, Some(i64::MIN), Some(7)],
                vec![Some(9), Some(2), Some(2), Some(-1), Some(-3)],
                vec![None, None, None, Some(-21)],
                vec![0, 2],
            ),
            (
                BinaryOperator::Modulo,
                vec![Some(8), Some(i64::MIN), None, Some(7), Some(7)],
                vec![Some(9), Some(-1), Some(0), Some(0), Some(-3)],
                vec![Some(0), None, None, Some(1)],
                vec![2],
            ),
        ] {
            let program = build_program(op, DataType::Int64, DataType::Int64, Wrap::Bare, allow);
            let batch = input(&program, &left, &right, &[None; 5]);
            let rows = [1, 2, 3, 4];
            let output = instance(&program)
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &batch,
                        source: None,
                    },
                    {
                        let selection = Selection::try_sparse(5, &rows).unwrap();
                        ScalarInvocationScope::new(selection, selection)
                    },
                    ScalarInvocationActivation::Activated,
                    &Control,
                    &mut PassThroughCallHost,
                )
                .unwrap();
            assert_eq!(integral(&output), expected);
            assert_eq!(errors(&output), error_ordinals);
            assert!(
                output
                    .errors()
                    .iter()
                    .all(|error| !error.message().is_empty())
            );
        }
        let program = build_program(
            BinaryOperator::Divide,
            DataType::Int64,
            DataType::Int64,
            Wrap::Bare,
            allow,
        );
        let batch = input(
            &program,
            &[Some(i64::MIN), Some(7), None, Some(7)],
            &[Some(-1), Some(0), Some(0), None],
            &[None; 4],
        );
        let output = instance(&program)
            .evaluate(
                EvaluationInput::Batch {
                    batch: &batch,
                    source: None,
                },
                ScalarInvocationScope::new(Selection::all(4), Selection::all(4)),
                ScalarInvocationActivation::Activated,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        assert_eq!(integral(&output), vec![None, None, None, None]);
        assert_eq!(errors(&output), vec![0, 1]);
        assert_eq!(
            output.errors()[0].reason(),
            novarocks_functions::DataFailureReason::NumericOutOfRange
        );
        assert_eq!(
            output.errors()[1].reason(),
            novarocks_functions::DataFailureReason::DivisionByZero
        );
    }
}

#[test]
fn narrow_signed_boundaries_reach_original_selected_error_journal() {
    for (carrier, min, max) in [
        (DataType::Int8, i64::from(i8::MIN), i64::from(i8::MAX)),
        (DataType::Int16, i64::from(i16::MIN), i64::from(i16::MAX)),
        (DataType::Int32, i64::from(i32::MIN), i64::from(i32::MAX)),
    ] {
        for allow in [false, true] {
            for (op, left, right, reason) in [
                (
                    BinaryOperator::Add,
                    max,
                    1,
                    novarocks_functions::DataFailureReason::NumericOutOfRange,
                ),
                (
                    BinaryOperator::Subtract,
                    min,
                    1,
                    novarocks_functions::DataFailureReason::NumericOutOfRange,
                ),
                (
                    BinaryOperator::Multiply,
                    max,
                    2,
                    novarocks_functions::DataFailureReason::NumericOutOfRange,
                ),
                (
                    BinaryOperator::Divide,
                    min,
                    -1,
                    novarocks_functions::DataFailureReason::NumericOutOfRange,
                ),
                (
                    BinaryOperator::Divide,
                    7,
                    0,
                    novarocks_functions::DataFailureReason::DivisionByZero,
                ),
                (
                    BinaryOperator::Modulo,
                    7,
                    0,
                    novarocks_functions::DataFailureReason::DivisionByZero,
                ),
            ] {
                let program =
                    build_program(op, carrier.clone(), carrier.clone(), Wrap::Bare, allow);
                let batch = input(
                    &program,
                    &[Some(2), Some(left), None],
                    &[Some(1), Some(right), Some(0)],
                    &[None; 3],
                );
                let selected_rows = [1, 2];
                let selection = Selection::try_sparse(3, &selected_rows).unwrap();
                let output = instance(&program)
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
                assert_eq!(output.values().data_type(), &carrier);
                assert_eq!(output.selection(), selection);
                assert_eq!(integral(&output), vec![None, None]);
                assert_eq!(errors(&output), vec![0]);
                assert_eq!(output.errors()[0].reason(), reason);
            }
        }
    }
}

#[test]
fn literal_broadcast_and_sliced_columns_use_real_compiled_constant_and_original_rows() {
    let program = build_program(
        BinaryOperator::Add,
        DataType::Int64,
        DataType::Int64,
        Wrap::ConstantRight,
        false,
    );
    let backing: ArrayRef = Arc::new(Int64Array::from(vec![
        Some(i64::MAX),
        Some(7),
        None,
        Some(-5),
        Some(i64::MAX),
    ]));
    let schema = program.input_schema().clone();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            backing.slice(1, 3),
            Arc::new(Int64Array::from(vec![Some(i64::MAX); 3])),
            Arc::new(BooleanArray::from(vec![None; 3])),
        ],
    )
    .unwrap();
    let rows = [0, 2];
    let output = instance(&program)
        .evaluate(
            EvaluationInput::Batch {
                batch: &batch,
                source: None,
            },
            {
                let selection = Selection::try_sparse(3, &rows).unwrap();
                ScalarInvocationScope::new(selection, selection)
            },
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert_eq!(integral(&output), vec![Some(9), Some(-3)]);
    assert!(output.errors().is_empty());
}

#[test]
fn eager_required_child_error_survives_parent_strict_null_and_is_null_or_coalesce() {
    for wrap in [Wrap::NullParent, Wrap::IsNull, Wrap::Coalesce] {
        let program = build_program(
            BinaryOperator::Add,
            DataType::Int64,
            DataType::Int64,
            wrap,
            true,
        );
        let batch = input(
            &program,
            &[Some(i64::MAX), None, Some(7)],
            &[Some(1), Some(0), Some(3)],
            &[None; 3],
        );
        let output = instance(&program)
            .evaluate(
                EvaluationInput::Batch {
                    batch: &batch,
                    source: None,
                },
                ScalarInvocationScope::new(Selection::all(3), Selection::all(3)),
                ScalarInvocationActivation::Activated,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        assert_eq!(errors(&output), vec![0]);
        match wrap {
            Wrap::NullParent => assert_eq!(integral(&output), vec![None, None, None]),
            Wrap::Coalesce => assert_eq!(integral(&output), vec![None, Some(71), Some(10)]),
            Wrap::IsNull => assert_eq!(
                output
                    .values()
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                vec![None, Some(true), Some(false)]
            ),
            _ => unreachable!(),
        }
    }
}

#[test]
fn actual_if_and_pure_boolean_domains_control_arithmetic_error_demand() {
    let program = build_program(
        BinaryOperator::Add,
        DataType::Int64,
        DataType::Int64,
        Wrap::If,
        true,
    );
    let batch = input(
        &program,
        &[Some(i64::MAX), Some(i64::MAX), None, Some(7)],
        &[Some(1), Some(1), Some(0), Some(3)],
        &[Some(false), Some(true), Some(true), None],
    );
    let output = instance(&program)
        .evaluate(
            EvaluationInput::Batch {
                batch: &batch,
                source: None,
            },
            ScalarInvocationScope::new(Selection::all(4), Selection::all(4)),
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert_eq!(integral(&output), vec![Some(71), None, None, Some(71)]);
    assert_eq!(errors(&output), vec![1]);
    for wrap in [Wrap::And, Wrap::Or] {
        let program = build_program(
            BinaryOperator::Add,
            DataType::Int64,
            DataType::Int64,
            wrap,
            true,
        );
        let batch = input(
            &program,
            &[Some(i64::MAX), None, Some(7)],
            &[Some(1), Some(0), Some(3)],
            &[None; 3],
        );
        let output = instance(&program)
            .evaluate(
                EvaluationInput::Batch {
                    batch: &batch,
                    source: None,
                },
                ScalarInvocationScope::new(Selection::all(3), Selection::all(3)),
                ScalarInvocationActivation::Activated,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        assert_eq!(
            output
                .values()
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(matches!(wrap, Wrap::Or)); 3]
        );
        assert!(output.errors().is_empty());
    }
}

#[test]
fn actual_arithmetic_rows_preserve_primary_controls_every_quantum_and_failed_instance_latch() {
    let program = build_program(
        BinaryOperator::Add,
        DataType::Int64,
        DataType::Int64,
        Wrap::Coalesce,
        false,
    );
    let left = (0..320)
        .map(|row| match row % 3 {
            0 => Some(i64::MAX),
            1 => None,
            _ => Some(7),
        })
        .collect::<Vec<_>>();
    let batch = input(&program, &left, &[Some(1); 320], &[None; 320]);
    let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
    let output = instance(&program)
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
    assert_eq!(errors(&output).len(), 107);
    let trace = recorder.trace();
    assert!(trace.contains(&256));
    assert!(trace.iter().all(|units| *units <= 256));
    for stop_at in 1..=trace.len() {
        for cause in causes() {
            let mut evaluator = instance(&program);
            let control = CallbackControl::new(cause.clone(), stop_at);
            assert!(
                matches!(evaluator.evaluate(EvaluationInput::Batch { batch: &batch, source: None }, ScalarInvocationScope::new(Selection::all(320), Selection::all(320)), ScalarInvocationActivation::Activated, &control, &mut PassThroughCallHost), Err(ExprEvaluationFailure::Kernel(actual)) if actual == cause)
            );
            assert_eq!(control.trace(), trace[..stop_at]);
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
