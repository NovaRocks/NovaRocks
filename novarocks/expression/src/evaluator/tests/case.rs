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
    EvaluationInput,
    testkit::{
        PassThroughCallHost,
        guarded_fixtures::{
            GUARDED_CONTROL as Control, SEED_42,
            case::{CaseKind, case_batch, case_program, rand_bits},
        },
    },
};

use novarocks_functions::{ScalarInvocationActivation, ScalarInvocationScope, Selection};

#[test]
fn searched_case_then_and_else_rng_instances_advance_only_actual_matches_across_batches() {
    let program = case_program(CaseKind::Random);
    let input = case_batch(
        &program,
        vec![
            None,
            Some(true),
            None,
            None,
            None,
            Some(false),
            None,
            None,
            None,
            Some(true),
        ],
        vec![
            None,
            Some(true),
            None,
            Some(true),
            None,
            Some(false),
            None,
            None,
            None,
            Some(true),
        ],
        vec![Some(25); 10],
    );
    let rows = [1, 3, 5, 9];
    let selection = Selection::try_sparse(10, &rows).unwrap();
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
    assert_eq!(
        rand_bits(&output),
        vec![
            Some(SEED_42[0]),
            Some(SEED_42[0]),
            Some(SEED_42[0]),
            Some(SEED_42[1])
        ]
    );
    assert!(output.errors().is_empty());
    assert_eq!(evaluator.instances.len(), 3);
    let next = case_batch(
        &program,
        vec![Some(false), Some(true), Some(false)],
        vec![Some(true), Some(true), None],
        vec![Some(25); 3],
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
    assert_eq!(
        rand_bits(&output),
        vec![Some(SEED_42[1]), Some(SEED_42[2]), Some(SEED_42[1])]
    );
    assert!(output.errors().is_empty());
}

#[test]
fn searched_case_empty_selection_does_not_instantiate_any_then_else_or_when_kernel() {
    let program = case_program(CaseKind::DeferredRandom);
    let input = case_batch(
        &program,
        vec![Some(false); 4],
        vec![None; 4],
        vec![Some(10_i128.pow(38) - 1); 4],
    );
    let mut evaluator = program.instance(&Control);
    let rows = [];
    let selection = Selection::try_sparse(4, &rows).unwrap();
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
    assert!(output.values().is_empty());
    assert!(output.errors().is_empty());
    assert!(evaluator.instances.is_empty());
    let clean = case_batch(
        &program,
        vec![Some(true), Some(false), Some(false)],
        vec![None; 3],
        vec![Some(10_i128.pow(38) - 1), None, Some(25)],
    );
    let output = evaluator
        .evaluate(
            EvaluationInput::Batch {
                batch: &clean,
                source: None,
            },
            ScalarInvocationScope::new(Selection::all(3), Selection::all(3)),
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert_eq!(rand_bits(&output), vec![Some(SEED_42[0]); 3]);
}

#[test]
fn searched_case_all_rows_match_first_when_still_runs_then_before_empty_remaining_completion() {
    let program = case_program(CaseKind::Random);
    let input = case_batch(
        &program,
        vec![Some(true); 12],
        vec![Some(true); 12],
        vec![Some(10_i128.pow(38) - 1); 12],
    );
    let rows = [2, 7, 11];
    let selection = Selection::try_sparse(12, &rows).unwrap();
    let mut evaluator = program.instance(&Control);
    // The first WHEN empties the pending row set. Its already matched THEN
    // remains required for every selected row; later WHEN/THEN/ELSE do not.
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
    assert_eq!(
        rand_bits(&output),
        SEED_42[..3].iter().copied().map(Some).collect::<Vec<_>>()
    );
    assert!(output.errors().is_empty());
    assert_eq!(evaluator.instances.len(), 1);
    let next = case_batch(&program, vec![Some(true)], vec![Some(true)], vec![None]);
    let output = evaluator
        .evaluate(
            EvaluationInput::Batch {
                batch: &next,
                source: None,
            },
            ScalarInvocationScope::new(Selection::all(1), Selection::all(1)),
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert_eq!(rand_bits(&output), vec![Some(SEED_42[3])]);
    assert!(output.errors().is_empty());
    assert_eq!(evaluator.instances.len(), 1);
}
