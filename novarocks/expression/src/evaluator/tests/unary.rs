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

use crate::evaluator::scalar_invocation::{LeafCallObservation, observe_calls};
use crate::{
    CompiledExpressionInstance, EvaluationInput,
    testkit::{
        PassThroughCallHost,
        guarded_fixtures::{
            GUARDED_CONTROL as Control,
            unary::{UnaryCase, booleans, max38, unary_batch, unary_program},
        },
    },
};
use arrow::array::{ArrayRef, Int64Array};
use novarocks_functions::{
    EvaluatedArgument, ScalarInvocationActivation, ScalarInvocationScope, Selection,
};
use std::sync::Arc;

fn next_actual_rand(evaluator: &mut CompiledExpressionInstance) -> i64 {
    let rand = evaluator
        .instances
        .values_mut()
        .find(|instance| instance.contract().function_id().as_str() == "builtin.scalar/rand/v1")
        .unwrap();
    let bound = Arc::new(Int64Array::from(vec![1])) as ArrayRef;
    let arguments = [EvaluatedArgument::Scalar(&bound)];
    let crate::evaluator::scalar_invocation::CallInstance::Kernel(rand) = rand else {
        panic!("actual RAND uses its scalar kernel");
    };
    let output = crate::evaluator::scalar_invocation::call_scalar(
        &mut PassThroughCallHost,
        rand,
        Selection::all(1),
        &arguments,
        &Control,
    )
    .unwrap();
    output
        .values()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}
fn rand_instances(evaluator: &CompiledExpressionInstance) -> usize {
    evaluator
        .instances
        .values()
        .filter(|instance| instance.contract().function_id().as_str() == "builtin.scalar/rand/v1")
        .count()
}

#[test]
fn actual_rand_null_test_state_boundary_promotes_pending_and_skips_failed_parent_rows() {
    let program = unary_program(UnaryCase::StateBoundary);
    let mut evaluator = program.instance(&Control);
    let input = unary_batch(
        &program,
        vec![Some(true); 9],
        vec![
            Some(25),
            Some(max38()),
            Some(25),
            Some(max38()),
            None,
            Some(max38()),
            Some(35),
            Some(max38()),
            Some(max38()),
        ],
    );
    let rows = [2, 4, 6, 8];
    let selection = Selection::try_sparse(9, &rows).unwrap();
    let (output, events) = observe_calls(|| {
        evaluator
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
            .unwrap()
    });
    assert_eq!(
        booleans(&output),
        vec![Some(true), Some(true), Some(true), None]
    );
    assert_eq!(output.errors().len(), 1);
    assert_eq!(output.errors()[0].selected_ordinal(), 3);
    assert_eq!(output.selection().row(3), Some(8));
    assert_eq!(rand_instances(&evaluator), 1);
    // The strict ROUND kernel skips NULL row 4. Row 8 fails before RAND.
    let domains = events
        .into_iter()
        .filter_map(|event| match event {
            LeafCallObservation::Rows { actual, .. } => Some(actual),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(domains, vec![vec![2, 6, 8], vec![2, 6]]);
    assert_eq!(next_actual_rand(&mut evaluator), 0);
    let unit = &program.unit;
    let flow = unit.control_flow();
    let root_use = unit.root(program.root).unwrap().use_id;
    let rand_null = flow.uses()[&root_use].arguments[1];
    let summary = evaluator.effects[&rand_null];
    assert!(
        summary
            .for_use(flow.uses()[&rand_null].context)
            .unwrap()
            .has_instance_state
    );
    assert!(
        !summary
            .for_use(flow.uses()[&rand_null].context)
            .unwrap()
            .permits_boolean_reordering()
    );
}

#[test]
fn all_pending_round_errors_stop_before_rand_instantiation_and_do_not_advance_existing_state() {
    let program = unary_program(UnaryCase::StateBoundary);
    let mut evaluator = program.instance(&Control);
    let failing = unary_batch(&program, vec![Some(true); 2], vec![Some(max38()); 2]);
    let output = evaluator
        .evaluate(
            EvaluationInput::Batch {
                batch: &failing,
                source: None,
            },
            ScalarInvocationScope::new(Selection::all(2), Selection::all(2)),
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert_eq!(booleans(&output), vec![None, None]);
    assert_eq!(output.errors().len(), 2);
    assert_eq!(rand_instances(&evaluator), 0);
    let clean = unary_batch(&program, vec![Some(true)], vec![Some(25)]);
    evaluator
        .evaluate(
            EvaluationInput::Batch {
                batch: &clean,
                source: None,
            },
            ScalarInvocationScope::new(Selection::all(1), Selection::all(1)),
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert_eq!(rand_instances(&evaluator), 1);
    let (output, events) = observe_calls(|| {
        evaluator
            .evaluate(
                EvaluationInput::Batch {
                    batch: &failing,
                    source: None,
                },
                ScalarInvocationScope::new(Selection::all(2), Selection::all(2)),
                ScalarInvocationActivation::Activated,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap()
    });
    assert_eq!(output.errors().len(), 2);
    // Only the failing ROUND call reached a leaf on this batch.
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, LeafCallObservation::Rows { .. }))
            .count(),
        1
    );
    assert_eq!(next_actual_rand(&mut evaluator), 0);
}
