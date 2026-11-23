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

use arrow::array::{Decimal128Array, Int64Array};
use novarocks_expression::{
    EvaluationInput, ExprEvaluationFailure, StaticExprKind,
    testkit::{
        PassThroughCallHost,
        guarded_fixtures::{
            CallbackControl, Fixture, GUARDED_CONTROL as Control,
            case::{CaseKind, case_batch, case_program, rand_bits},
            causes,
        },
    },
};
use novarocks_functions::{
    KernelFailure, ScalarInvocationActivation, ScalarInvocationScope, Selection,
};
use novarocks_type_contract::{ControlShape, EvaluationDemand, RegionGuard};

fn assert_case_domains(program: &Fixture, has_else: bool) {
    let unit = &program.unit;
    let flow = unit.control_flow();
    let invocation = &flow.uses()[&unit.root(program.root).unwrap().use_id];
    assert_eq!(
        invocation.control,
        ControlShape::Case {
            simple: false,
            arms: 2,
            has_else
        }
    );
    let definitions = unit.definitions();
    assert!(
        matches!(definitions.node(invocation.definition).unwrap().kind(),StaticExprKind::Case {has_case_expr:false,has_else_expr,children, ..} if *has_else_expr==has_else && children.len()==4+usize::from(has_else))
    );
    for (ordinal, child) in invocation.arguments.iter().enumerate() {
        let actual = &flow.uses()[child];
        assert_eq!(
            actual.context.demand,
            if ordinal < 4 && ordinal % 2 == 0 {
                EvaluationDemand::TruthOnly
            } else {
                EvaluationDemand::Value
            }
        );
        assert_ne!(actual.context.region, invocation.context.region);
        let expected = if ordinal == 4 {
            novarocks_type_contract::GuardKind::CaseElse
        } else if ordinal % 2 == 0 {
            novarocks_type_contract::GuardKind::CaseWhen {
                arm: (ordinal / 2) as u32,
            }
        } else {
            novarocks_type_contract::GuardKind::CaseThen {
                arm: (ordinal / 2) as u32,
            }
        };
        assert_eq!(
            flow.regions()[&actual.context.region].guard,
            Some(RegionGuard {
                owner: invocation.context.use_id,
                kind: expected
            })
        );
    }
}

#[test]
fn searched_case_first_true_wins_and_null_false_when_skip_with_sparse_source_rows() {
    let program = case_program(CaseKind::Priority);
    assert_case_domains(&program, true);
    let input = case_batch(
        &program,
        vec![
            Some(false),
            Some(true),
            Some(false),
            None,
            Some(false),
            Some(false),
            None,
            Some(false),
        ],
        vec![
            Some(false),
            Some(true),
            Some(false),
            Some(true),
            Some(false),
            None,
            Some(false),
            Some(false),
        ],
        vec![Some(25); 8],
    );
    let rows = [1, 3, 5, 6];
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
        output
            .values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(11), Some(22), Some(33), Some(33)]
    );
    assert!(output.errors().is_empty());
}

#[test]
fn searched_case_without_else_returns_successful_null_only_for_unmatched_rows() {
    let program = case_program(CaseKind::NoElse);
    assert_case_domains(&program, false);
    let input = case_batch(
        &program,
        vec![Some(true), None, Some(false)],
        vec![Some(true), None, Some(true)],
        vec![None; 3],
    );
    let output = program
        .instance(&Control)
        .evaluate(
            EvaluationInput::Batch {
                batch: &input,
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
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(11), None, Some(22)]
    );
    assert!(output.errors().is_empty());
}

#[test]
fn searched_case_later_round_when_is_required_only_for_remaining_rows_and_error_cannot_reach_else()
{
    let program = case_program(CaseKind::DeferredRandom);
    assert_case_domains(&program, true);
    let mut first = vec![Some(false); 8];
    first[1] = Some(true);
    first[3] = None;
    let mut values = vec![Some(25); 8];
    values[1] = Some(10_i128.pow(38) - 1);
    values[3] = None;
    values[7] = Some(10_i128.pow(38) - 1);
    let input = case_batch(&program, first, vec![None; 8], values);
    let rows = [1, 3, 5, 7];
    let selection = Selection::try_sparse(8, &rows).unwrap();
    let mut evaluator = program.instance(&Control);
    let output = evaluator
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
    assert_eq!(rand_bits(&output), vec![Some(0), Some(0), Some(0), None]);
    assert_eq!(output.errors().len(), 1);
    assert_eq!(output.errors()[0].selected_ordinal(), 3);
    assert_eq!(output.selection().row(3), Some(7));
    assert!(output.errors()[0].message().contains("overflow"));
    // Neither the first already-decided row nor the failed WHEN row may
    // advance ELSE. Each successful arm advanced its own actual instance once.
    let next = case_batch(
        &program,
        vec![Some(true), Some(false), Some(false)],
        vec![None; 3],
        vec![Some(10_i128.pow(38) - 1), None, Some(25)],
    );
    let output = evaluator
        .evaluate(
            EvaluationInput::Batch {
                batch: &next,
                source: None,
            },
            ScalarInvocationScope::new(Selection::all(3), Selection::all(3)),
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert_eq!(rand_bits(&output), vec![Some(0); 3]);
    assert!(output.errors().is_empty());
}

#[test]
fn searched_case_selected_then_round_error_is_terminal_and_inactive_then_overflow_is_skipped() {
    let program = case_program(CaseKind::ErrorThen);
    let input = case_batch(
        &program,
        vec![Some(true), Some(false), None, Some(true)],
        vec![Some(true), Some(true), Some(false), Some(true)],
        vec![
            Some(10_i128.pow(38) - 1),
            Some(10_i128.pow(38) - 1),
            Some(10_i128.pow(38) - 1),
            Some(25),
        ],
    );
    let output = program
        .instance(&Control)
        .evaluate(
            EvaluationInput::Batch {
                batch: &input,
                source: None,
            },
            ScalarInvocationScope::new(Selection::all(4), Selection::all(4)),
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert_eq!(
        output
            .values()
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![None, Some(71), Some(71), Some(30)]
    );
    assert_eq!(output.errors().len(), 1);
    assert_eq!(output.errors()[0].selected_ordinal(), 0);
    assert!(output.errors()[0].message().contains("overflow"));
}

#[test]
fn every_actual_searched_case_callback_preserves_all_seven_causes_and_refuses_replay() {
    let program = case_program(CaseKind::DeferredRandom);
    let input = case_batch(
        &program,
        (0..320).map(|row| Some(row % 4 == 0)).collect(),
        vec![None; 320],
        (0..320)
            .map(|row| match row % 4 {
                0 | 1 => Some(10_i128.pow(38) - 1),
                2 => None,
                _ => Some(25),
            })
            .collect(),
    );
    let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX, Control);
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
    assert_eq!(output.errors().len(), 80);
    let trace = recorder.trace.lock().unwrap().clone();
    assert!(trace.contains(&256));
    assert!(trace.iter().all(|units| *units <= 256));
    for index in 1..=trace.len() {
        for cause in causes("guarded-origin") {
            let mut evaluator = program.instance(&Control);
            let control = CallbackControl::new(cause.clone(), index, Control);
            assert!(
                matches!(evaluator.evaluate(EvaluationInput::Batch { batch: &input, source: None }, ScalarInvocationScope::new(Selection::all(320), Selection::all(320)), ScalarInvocationActivation::Activated, &control, &mut PassThroughCallHost), Err(ExprEvaluationFailure::Kernel(actual)) if actual==cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..index]);
            let after = CallbackControl::new(KernelFailure::Cancelled, usize::MAX, Control);
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
            assert!(after.trace.lock().unwrap().is_empty());
        }
    }
}
