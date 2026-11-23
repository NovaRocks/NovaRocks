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

use crate::evaluator::CompiledExpressionInstance;
use crate::{
    EvaluationInput,
    testkit::{
        PassThroughCallHost,
        frame_fixtures::{
            Control, FrameFixture,
            guarded::{Shape, batch, decimal_batch, program},
        },
    },
};
use arrow::array::{Array, Decimal128Array, Int64Array};
use novarocks_functions::{ScalarInvocationActivation, ScalarInvocationScope, Selection};

fn instance(program: &FrameFixture) -> CompiledExpressionInstance {
    CompiledExpressionInstance::try_new(program.unit.clone(), program.root, &Control).unwrap()
}

#[test]
fn guarded_constant_rng_stays_uninstantiated_until_coalesce_really_needs_it() {
    let program = program(Shape::Random);
    let mut evaluator = instance(&program);
    let true_only = batch(&program, vec![Some(true); 3], vec![Some(1); 3]);
    let output = evaluator
        .evaluate(
            EvaluationInput::Batch {
                batch: &true_only,
                source: None,
            },
            ScalarInvocationScope::new(Selection::all(3), Selection::all(3)),
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    let values = output
        .values()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert!(values.iter().all(|value| value == Some(0)));
    assert_eq!(evaluator.instances.len(), 1);
    let false_only = batch(&program, vec![Some(false); 2], vec![None; 2]);
    let output = evaluator
        .evaluate(
            EvaluationInput::Batch {
                batch: &false_only,
                source: None,
            },
            ScalarInvocationScope::new(Selection::all(2), Selection::all(2)),
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    let values = output
        .values()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(values.iter().collect::<Vec<_>>(), vec![Some(0); 2]);
    assert_eq!(evaluator.instances.len(), 2);
}

#[test]
fn empty_guarded_selection_creates_no_kernel_instance_and_does_not_advance_rng() {
    let program = program(Shape::Random);
    let mut evaluator = instance(&program);
    let input = batch(&program, vec![Some(false); 3], vec![None; 3]);
    let rows = [];
    let output = evaluator
        .evaluate(
            EvaluationInput::Batch {
                batch: &input,
                source: None,
            },
            {
                let rows = Selection::try_sparse(3, &rows).unwrap();
                ScalarInvocationScope::new(rows, rows)
            },
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert!(output.values().is_empty());
    assert!(output.errors().is_empty());
    assert!(evaluator.instances.is_empty());
    let one = batch(&program, vec![None], vec![None]);
    let output = evaluator
        .evaluate(
            EvaluationInput::Batch {
                batch: &one,
                source: None,
            },
            ScalarInvocationScope::new(Selection::all(1), Selection::all(1)),
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
            .value(0),
        0
    );
    assert_eq!(evaluator.instances.len(), 1);
}

#[test]
fn only_false_or_null_if_rows_never_instantiate_guarded_round_even_with_valid_overflow_values() {
    let program = program(Shape::Decimal);
    let mut evaluator = instance(&program);
    let input = decimal_batch(
        &program,
        vec![Some(false), None],
        vec![Some(10_i128.pow(38) - 1); 2],
        vec![Some(17); 2],
    );
    let output = evaluator
        .evaluate(
            EvaluationInput::Batch {
                batch: &input,
                source: None,
            },
            ScalarInvocationScope::new(Selection::all(2), Selection::all(2)),
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert!(output.errors().is_empty());
    assert!(evaluator.instances.is_empty());
    assert_eq!(
        output
            .values()
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(17), Some(17)]
    );
}
