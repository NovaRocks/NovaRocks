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

use novarocks_expression::{
    EvaluationInput, ExprEvaluationFailure, StaticExprKind,
    testkit::{
        CallbackControl, PassThroughCallHost,
        guarded_fixtures::{
            GUARDED_CONTROL as Control, causes,
            unary::{UnaryCase, booleans, max38, unary_batch, unary_program},
        },
    },
};
use novarocks_functions::{
    KernelFailure, ScalarInvocationActivation, ScalarInvocationScope, Selection,
};
use novarocks_type_contract::{ControlShape, EvaluationDemand};

#[test]
fn actual_null_tests_distinguish_success_null_from_round_error_placeholder_on_sparse_rows() {
    for (case, expected) in [
        (UnaryCase::IsNull, vec![Some(false), Some(true), None]),
        (UnaryCase::IsNotNull, vec![Some(true), Some(false), None]),
    ] {
        let program = unary_program(case);
        let input = unary_batch(
            &program,
            vec![Some(true); 8],
            vec![
                Some(max38()),
                Some(25),
                Some(max38()),
                Some(max38()),
                None,
                Some(max38()),
                Some(max38()),
                Some(max38()),
            ],
        );
        let rows = [1, 4, 7];
        let selection = Selection::try_sparse(8, &rows).unwrap();
        let mut instance = program.instance(&Control);
        let output = instance
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
        assert_eq!(booleans(&output), expected);
        assert_eq!(output.selection(), selection);
        assert_eq!(output.errors().len(), 1);
        assert_eq!(output.errors()[0].selected_ordinal(), 2);
        assert_eq!(output.selection().row(2), Some(7));
        assert!(output.errors()[0].message().contains("overflow"));
    }
}

#[test]
fn actual_pure_round_boolean_error_is_suppressed_only_by_later_required_decider() {
    for (case, flags, expected) in [
        (
            UnaryCase::PureAnd,
            vec![Some(false), Some(true), Some(true)],
            vec![Some(false), None, Some(true)],
        ),
        (
            UnaryCase::PureOr,
            vec![Some(true), Some(false), Some(false)],
            vec![Some(true), None, Some(true)],
        ),
    ] {
        let program = unary_program(case);
        let mut values = vec![Some(25); 10];
        values[2] = Some(max38());
        values[7] = Some(max38());
        values[9] = None;
        let mut flag_column = vec![None; 10];
        for (row, flag) in [2, 7, 9].into_iter().zip(flags) {
            flag_column[row] = flag;
        }
        let input = unary_batch(&program, flag_column, values);
        let rows = [2, 7, 9];
        let selection = Selection::try_sparse(10, &rows).unwrap();
        let mut instance = program.instance(&Control);
        let output = instance
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
        assert_eq!(booleans(&output), expected);
        assert_eq!(output.errors().len(), 1);
        assert_eq!(output.errors()[0].selected_ordinal(), 1);
        assert_eq!(output.selection().row(1), Some(7));
        assert!(output.errors()[0].message().contains("overflow"));
    }
}

#[test]
fn actual_not_filter_requires_value_child_and_keeps_nullable_truth_semantics() {
    let program = unary_program(UnaryCase::NotFilter);
    let unit = &program.unit;
    let flow = unit.control_flow();
    let parent = &flow.uses()[&unit.root(program.root).unwrap().use_id];
    assert_eq!(parent.context.demand, EvaluationDemand::TruthOnly);
    assert_eq!(parent.control, ControlShape::Eager);
    assert_eq!(
        flow.uses()[&parent.arguments[0]].context.demand,
        EvaluationDemand::Value
    );
    let definitions = unit.definitions();
    assert!(matches!(
        definitions.node(parent.definition).unwrap().kind(),
        StaticExprKind::Not(_)
    ));
    let child = &flow.uses()[&parent.arguments[0]];
    assert_eq!(child.control, ControlShape::Conjunction);
    assert!(matches!(
        definitions.node(child.definition).unwrap().kind(),
        StaticExprKind::NaryAnd { args } if args.len() == 2
    ));
    for argument in &child.arguments {
        assert_eq!(
            flow.uses()[argument].context.demand,
            EvaluationDemand::Value
        );
    }
    // If NOT propagated its TruthOnly demand into AND, the NULL child row
    // would become FALSE and NOT would incorrectly keep it as TRUE. The
    // actual TRUE literal is compiled to the original ConstantValue owner.
    let input = unary_batch(
        &program,
        vec![
            Some(false),
            Some(true),
            Some(false),
            None,
            Some(true),
            Some(false),
        ],
        vec![None; 6],
    );
    let rows = [1, 3, 5];
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
    assert_eq!(booleans(&output), vec![Some(false), None, Some(true)]);
    assert!(output.errors().is_empty());
}

#[test]
fn every_actual_round_null_rand_boundary_callback_keeps_all_seven_causes_and_no_replay() {
    let program = unary_program(UnaryCase::StateBoundary);
    let input = unary_batch(
        &program,
        vec![Some(true); 320],
        (0..320)
            .map(|row| match row % 3 {
                0 => Some(max38()),
                1 => None,
                _ => Some(25),
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
    assert_eq!(output.errors().len(), 107);
    let trace = recorder.trace();
    assert!(trace.contains(&256));
    assert!(trace.iter().all(|units| *units <= 256));
    for index in 1..=trace.len() {
        for cause in causes("guarded-origin") {
            let mut instance = program.instance(&Control);
            let control = CallbackControl::new(cause.clone(), index);
            assert!(
                matches!(instance.evaluate(EvaluationInput::Batch { batch: &input, source: None }, ScalarInvocationScope::new(Selection::all(320), Selection::all(320)), ScalarInvocationActivation::Activated, &control, &mut PassThroughCallHost), Err(ExprEvaluationFailure::Kernel(actual)) if actual==cause)
            );
            assert_eq!(control.trace(), trace[..index]);
            let after = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
            assert!(matches!(
                instance.evaluate(
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
