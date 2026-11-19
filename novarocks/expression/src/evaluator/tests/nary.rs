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

use crate::{
    EvaluationInput, ExprEvaluationFailure,
    testkit::{
        PassThroughCallHost,
        guarded_fixtures::{
            CallbackControl, Fixture, NARY_CONTROL as Control, causes,
            nary::{AND, Connective, OR, Use, batch, dual_demand_program, program, sparse_batch},
        },
    },
};
use arrow::{array::BooleanArray, datatypes::DataType, record_batch::RecordBatch};
use novarocks_functions::{
    KernelFailure, ScalarInvocationActivation, ScalarInvocationScope, Selection,
};
use novarocks_type_contract::{ControlShape, EvaluationDemand};
fn assert_all_nary_evaluation_callbacks(
    program: Fixture,
    input: RecordBatch,
    expected: Option<bool>,
) {
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
    assert!(output.errors().is_empty());
    assert_eq!(
        output
            .values()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![expected; 320],
    );
    let trace = recorder.trace.lock().unwrap().clone();
    assert!(trace.contains(&256));
    assert!(trace.iter().all(|units| *units <= 256));
    for index in 1..=trace.len() {
        for cause in causes("nary-original") {
            let mut evaluator = program.instance(&Control);
            let control = CallbackControl::new(cause.clone(), index, Control);
            assert!(
                matches!(evaluator.evaluate(EvaluationInput::Batch { batch: &input, source: None }, ScalarInvocationScope::new(Selection::all(320), Selection::all(320)), ScalarInvocationActivation::Activated, &control, &mut PassThroughCallHost), Err(ExprEvaluationFailure::Kernel(actual)) if actual == cause)
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
            assert!(evaluator.instances.is_empty());
        }
    }
}

#[test]
fn nullable_nary_three_and_320_have_independent_sql_truth_results_on_sparse_original_rows() {
    for op in [Connective::And, Connective::Or] {
        for width in [3, 320] {
            let program = program(op, Use::Value, width);
            let (input, rows) = sparse_batch(&program);
            let selection = Selection::try_sparse(input.num_rows(), &rows).unwrap();
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
            assert_eq!(output.selection(), selection);
            assert!(output.errors().is_empty());
            let values = output
                .values()
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap();
            let expected = match op {
                Connective::And => AND,
                Connective::Or => OR,
            };
            assert_eq!(values.iter().collect::<Vec<_>>(), expected);
            assert!(evaluator.instances.is_empty());
            // Definitions can be shared by occurrences, but computed values
            // cannot be cached across input batches by definition identity.
            let next = batch(
                &program.unit,
                [
                    vec![Some(false); 2],
                    vec![Some(false); 2],
                    vec![Some(false); 2],
                ],
            );
            let output = evaluator
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &next,
                        source: None,
                    },
                    ScalarInvocationScope::new(Selection::all(2), Selection::all(2)),
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
                vec![Some(false); 2]
            );
            assert!(evaluator.instances.is_empty());
        }
    }
}

#[test]
fn empty_nary_selection_publishes_exact_boolean_empty_without_creating_instances() {
    for op in [Connective::And, Connective::Or] {
        for mode in [Use::Value, Use::TruthOnly] {
            let program = program(op, mode, 320);
            let (input, _) = sparse_batch(&program);
            let rows = [];
            let selection = Selection::try_sparse(input.num_rows(), &rows).unwrap();
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
            assert_eq!(output.selection(), selection);
            assert_eq!(output.values().data_type(), &DataType::Boolean);
            assert_eq!(output.values().len(), 0);
            assert!(output.errors().is_empty());
            assert!(evaluator.instances.is_empty());
        }
    }
}

#[test]
fn every_nary_evaluation_callback_preserves_primary_failure_and_refuses_replay() {
    let program = program(Connective::Or, Use::Value, 3);
    let input = batch(
        &program.unit,
        [
            vec![None; 320],
            vec![Some(false); 320],
            vec![Some(true); 320],
        ],
    );
    assert_all_nary_evaluation_callbacks(program, input, Some(true));
}

#[test]
fn every_nary_later_child_callback_preserves_primary_failure_and_refuses_replay() {
    let program = program(Connective::Or, Use::Value, 3);
    // The actual ordered children are C,A,B. C=false cannot decide OR;
    // A=NULL must remain pending through B=false, leaving a successful NULL.
    // Thus every row crosses both later-child continuation boundaries.
    let input = batch(
        &program.unit,
        [
            vec![None; 320],
            vec![Some(false); 320],
            vec![Some(false); 320],
        ],
    );
    assert_all_nary_evaluation_callbacks(program, input, None);
}

#[test]
fn same_nary_definition_in_if_truth_only_and_value_domains_does_not_cache_null_as_false() {
    let program = dual_demand_program();
    let unit = &program.unit;
    assert_eq!(unit.calls().len(), 1); // Only the actual IF control token.
    let flow = unit.control_flow();
    let invocation = &flow.uses()[&unit.root(program.root).unwrap().use_id];
    assert_eq!(invocation.control, ControlShape::If);
    let condition = &flow.uses()[&invocation.arguments[0]];
    let otherwise = &flow.uses()[&invocation.arguments[2]];
    assert_eq!(condition.definition, otherwise.definition);
    assert_ne!(condition.context.use_id, otherwise.context.use_id);
    assert_ne!(condition.context.region, otherwise.context.region);
    assert_eq!(condition.context.demand, EvaluationDemand::TruthOnly);
    assert_eq!(otherwise.context.demand, EvaluationDemand::Value);
    let input = batch(
        &program.unit,
        [
            vec![Some(true); 6],
            vec![Some(true); 6],
            vec![Some(false), None, Some(true), Some(false), None, Some(true)],
        ],
    );
    let rows = [1, 3, 5];
    let selection = Selection::try_sparse(6, &rows).unwrap();
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
    assert_eq!(output.selection(), selection);
    assert!(output.errors().is_empty());
    assert_eq!(
        output
            .values()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![None, Some(false), Some(true)]
    );
    assert!(evaluator.instances.is_empty());
}
