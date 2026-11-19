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
            equality::{
                EqualityCase, assert_case_recipes, equality_program, float_batch, integers,
            },
        },
    },
};
use arrow::datatypes::DataType;
use novarocks_functions::{ScalarInvocationActivation, ScalarInvocationScope, Selection};

#[test]
fn simple_case_rand_operand_is_evaluated_once_per_selected_parent_across_all_when_arms() {
    let program = equality_program(EqualityCase::RandomOperand);
    assert_case_recipes(&program, DataType::Float64);
    let input = float_batch(&program, vec![None; 6], vec![None; 6], vec![None; 6]);
    let rows = [1, 4];
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
    assert_eq!(integers(&output), vec![Some(10), Some(20)]);
    assert!(output.errors().is_empty());
    assert_eq!(evaluator.instances.len(), 1);
    let next = float_batch(&program, vec![None], vec![None], vec![None]);
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
    assert_eq!(integers(&output), vec![Some(30)]);
    assert_eq!(evaluator.instances.len(), 1);
}

#[test]
fn simple_case_rand_when_labels_only_advance_remaining_rows_with_independent_state() {
    let program = equality_program(EqualityCase::RandomLabels);
    assert_case_recipes(&program, DataType::Float64);
    let mut operand = vec![Some(-1.0); 9];
    operand[1] = Some(f64::from_bits(SEED_42[0]));
    operand[5] = Some(f64::from_bits(SEED_42[1]));
    operand[8] = Some(f64::from_bits(SEED_42[1]));
    let input = float_batch(&program, operand, vec![None; 9], vec![None; 9]);
    let rows = [1, 3, 5, 8];
    let selection = Selection::try_sparse(9, &rows).unwrap();
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
    // WHEN1 samples r0..r3; only row1 matches. WHEN2 samples r0..r2
    // over the three remaining rows; only original row5 matches r1.
    assert_eq!(
        integers(&output),
        vec![Some(10), Some(30), Some(20), Some(30)]
    );
    assert!(output.errors().is_empty());
    assert_eq!(evaluator.instances.len(), 2);
    let next = float_batch(
        &program,
        vec![Some(f64::from_bits(SEED_42[3])), Some(-1.0)],
        vec![None; 2],
        vec![None; 2],
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
    // WHEN1 now uses r4,r5; WHEN2 uses its own r3,r4.
    assert_eq!(integers(&output), vec![Some(20), Some(30)]);
    assert!(output.errors().is_empty());
}

#[test]
fn simple_case_empty_selection_does_not_initialize_operand_or_when_rand_instances() {
    for kind in [EqualityCase::RandomOperand, EqualityCase::RandomLabels] {
        let program = equality_program(kind);
        let input = float_batch(&program, vec![None; 4], vec![None; 4], vec![None; 4]);
        let mut evaluator = program.instance(&Control);
        let rows = [];
        let output = evaluator
            .evaluate(
                EvaluationInput::Batch {
                    batch: &input,
                    source: None,
                },
                {
                    let selection = Selection::try_sparse(4, &rows).unwrap();
                    ScalarInvocationScope::new(selection, selection)
                },
                ScalarInvocationActivation::Activated,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        assert!(output.values().is_empty());
        assert!(output.errors().is_empty());
        assert!(evaluator.instances.is_empty());
        let next = float_batch(
            &program,
            vec![Some(f64::from_bits(SEED_42[0]))],
            vec![None],
            vec![None],
        );
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
        assert_eq!(integers(&output), vec![Some(10)]);
    }
}
