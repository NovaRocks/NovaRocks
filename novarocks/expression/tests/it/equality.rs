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

use arrow::{array::BooleanArray, datatypes::DataType};
use novarocks_expression::{
    ComparisonSite, EvaluationInput, ExprEvaluationFailure,
    testkit::{
        CallbackControl, PassThroughCallHost,
        guarded_fixtures::{
            GUARDED_CONTROL as Control, causes,
            equality::{
                EqualityCase, assert_case_recipes, equality_batch, equality_program, float_batch,
                integers, root_use,
            },
        },
    },
};
use novarocks_functions::{
    KernelFailure, ScalarInvocationActivation, ScalarInvocationScope, Selection,
};
use novarocks_type_contract::FunctionValueType;

#[test]
fn ordinary_eq_and_not_eq_preserve_frozen_float_bits_and_nulls_in_sparse_rows() {
    // This is the accepted ordinary bit identity contract, not IEEE ==:
    // equal NaN payloads compare equal, and opposite signed zero differs.
    let nan_a = f64::from_bits(0x7ff8_0000_0000_0001);
    let nan_b = f64::from_bits(0x7ff8_0000_0000_0002);
    let left = vec![
        Some(99.0),
        Some(nan_a),
        Some(nan_a),
        Some(0.0),
        Some(-0.0),
        Some(f64::INFINITY),
        Some(f64::NEG_INFINITY),
        None,
        Some(1.5),
        Some(1.5),
    ];
    let right = vec![
        Some(-99.0),
        Some(nan_a),
        Some(nan_b),
        Some(-0.0),
        Some(-0.0),
        Some(f64::INFINITY),
        Some(f64::INFINITY),
        None,
        None,
        Some(2.5),
    ];
    let rows = [1, 2, 3, 4, 5, 6, 7, 8, 9];
    for kind in [EqualityCase::Eq, EqualityCase::NotEq] {
        let program = equality_program(kind);
        let recipe = program
            .unit
            .recipes()
            .comparison(ComparisonSite::Binary(root_use(&program)))
            .unwrap();
        assert_eq!(
            recipe.left_type(),
            &FunctionValueType::new(DataType::Float64, true)
        );
        assert_eq!(
            recipe.right_type(),
            &FunctionValueType::new(DataType::Float64, true)
        );
        let input = float_batch(&program, left.clone(), right.clone(), vec![None; 10]);
        let selection = Selection::try_sparse(10, &rows).unwrap();
        let output = program
            .instance(&Control)
            .evaluate(
                EvaluationInput::Batch {
                    batch: &input,
                    source: None,
                },
                ScalarInvocationScope::new(selection, selection),
                ScalarInvocationActivation::Activated,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        let eq = [
            Some(true),
            Some(false),
            Some(false),
            Some(true),
            Some(true),
            Some(false),
            None,
            None,
            Some(false),
        ];
        let expected = eq.map(|v| {
            v.map(|v| {
                if matches!(kind, EqualityCase::Eq) {
                    v
                } else {
                    !v
                }
            })
        });
        assert_eq!(output.selection(), selection);
        assert_eq!(
            output
                .values()
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            expected
        );
        assert!(output.errors().is_empty());
    }
}

#[test]
fn simple_case_source_labels_keep_first_match_null_nonmatch_sparse_rows_and_recipe_occurrences() {
    let program = equality_program(EqualityCase::Columns);
    assert_case_recipes(&program, DataType::Float64);
    let input = float_batch(
        &program,
        vec![Some(99.0), Some(2.0), None, Some(4.0), Some(5.0), Some(6.0)],
        vec![Some(0.0), Some(2.0), None, Some(9.0), None, Some(0.0)],
        vec![Some(0.0), Some(2.0), None, Some(4.0), Some(5.0), Some(7.0)],
    );
    let rows = [1, 2, 3, 4, 5];
    let selection = Selection::try_sparse(6, &rows).unwrap();
    let output = program
        .instance(&Control)
        .evaluate(
            EvaluationInput::Batch {
                batch: &input,
                source: None,
            },
            ScalarInvocationScope::new(selection, selection),
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert_eq!(output.selection(), selection);
    assert_eq!(
        integers(&output),
        vec![Some(10), Some(30), Some(20), Some(20), Some(30)]
    );
    assert!(output.errors().is_empty());
}

#[test]
fn simple_case_round_operand_and_label_errors_are_terminal_without_null_match_or_fallthrough() {
    for kind in [EqualityCase::ErrorOperand, EqualityCase::ErrorLabel] {
        let program = equality_program(kind);
        assert_case_recipes(&program, DataType::Decimal128(38, 0));
        let rows = [1, 3, 5, 7];
        let mut source = vec![Some(25); 8];
        let mut label = vec![Some(30); 8];
        if matches!(kind, EqualityCase::ErrorOperand) {
            source[1] = None;
            source[5] = Some(10_i128.pow(38) - 1);
        } else {
            source = vec![Some(30); 8];
            source[1] = None;
            label[3] = Some(25);
            label[5] = Some(10_i128.pow(38) - 1);
            label[7] = None;
        }
        let input = equality_batch(
            &program,
            vec![None; 8],
            vec![None; 8],
            vec![None; 8],
            source,
            label,
        );
        let selection = Selection::try_sparse(8, &rows).unwrap();
        let output = program
            .instance(&Control)
            .evaluate(
                EvaluationInput::Batch {
                    batch: &input,
                    source: None,
                },
                ScalarInvocationScope::new(selection, selection),
                ScalarInvocationActivation::Activated,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        assert_eq!(output.selection(), selection);
        assert_eq!(
            integers(&output),
            if matches!(kind, EqualityCase::ErrorOperand) {
                vec![Some(30), Some(10), None, Some(10)]
            } else {
                vec![Some(30), Some(10), None, Some(20)]
            }
        );
        assert_eq!(output.errors().len(), 1);
        assert_eq!(output.errors()[0].selected_ordinal(), 2);
        assert_eq!(output.selection().row(2), Some(5));
        assert!(output.errors()[0].message().contains("overflow"));
    }
}

#[test]
fn every_actual_simple_case_callback_preserves_all_seven_primary_failures_and_refuses_replay() {
    let program = equality_program(EqualityCase::ErrorLabel);
    let input = equality_batch(
        &program,
        vec![None; 320],
        vec![None; 320],
        vec![None; 320],
        vec![Some(30); 320],
        (0..320)
            .map(|row| match row % 3 {
                0 => None,
                1 => Some(25),
                _ => Some(10_i128.pow(38) - 1),
            })
            .collect(),
    );
    let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
    let output = program
        .instance(&Control)
        .evaluate(
            EvaluationInput::Batch {
                batch: &input,
                source: None,
            },
            ScalarInvocationScope::new(Selection::all(320), Selection::all(320)),
            ScalarInvocationActivation::Activated,
            &recorder,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert_eq!(output.errors().len(), 106);
    assert_eq!(
        integers(&output),
        (0..320)
            .map(|row| match row % 3 {
                0 => Some(20),
                1 => Some(10),
                _ => None,
            })
            .collect::<Vec<_>>()
    );
    let trace = recorder.trace();
    assert!(trace.contains(&256));
    assert!(trace.iter().all(|units| *units <= 256));
    for index in 1..=trace.len() {
        for cause in causes("guarded-origin") {
            let mut evaluator = program.instance(&Control);
            let control = CallbackControl::new(cause.clone(), index);
            assert!(
                matches!(evaluator.evaluate(EvaluationInput::Batch { batch: &input, source: None }, ScalarInvocationScope::new(Selection::all(320), Selection::all(320)), ScalarInvocationActivation::Activated, &control, &mut PassThroughCallHost), Err(ExprEvaluationFailure::Kernel(actual)) if actual == cause)
            );
            assert_eq!(control.trace(), trace[..index]);
            let after = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
            assert!(matches!(
                evaluator.evaluate(
                    EvaluationInput::Batch {
                        batch: &input,
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
