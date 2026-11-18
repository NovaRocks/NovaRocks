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
            GUARDED_CONTROL as Control,
            ordered::{Source, ordered_batch, ordered_instance, ordered_program},
        },
    },
};
use arrow::{
    array::{BooleanArray, Float64Array},
    datatypes::DataType,
};
use novarocks_expression_contract::BinaryOperator;
use novarocks_functions::{ScalarInvocationActivation, ScalarInvocationScope, Selection};
use std::sync::Arc;
fn result(output: &novarocks_functions::SelectedValues<'_>) -> Vec<Option<bool>> {
    output
        .values()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .iter()
        .collect()
}

#[test]
fn ordered_eager_right_rand_advances_on_left_null_and_both_instances_keep_independent_batches() {
    let program = ordered_program(BinaryOperator::Lt, DataType::Float64, Source::Random, false);
    let input = ordered_batch(
        &program,
        Arc::new(Float64Array::from(vec![None; 6])),
        Arc::new(Float64Array::from(vec![None; 6])),
        vec![None, Some(false), None, None, Some(true), None],
    );
    let rows = [1, 4];
    let selection = Selection::try_sparse(6, &rows).unwrap();
    let mut evaluator = ordered_instance(&program);
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
    // Left instance gets r0 only; right gets r0,r1 even for left's NULL row.
    assert_eq!(result(&output), vec![None, Some(true)]);
    assert!(output.errors().is_empty());
    assert_eq!(evaluator.instances.len(), 2);
    let next = ordered_batch(
        &program,
        Arc::new(Float64Array::from(vec![None])),
        Arc::new(Float64Array::from(vec![None])),
        vec![Some(true)],
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
    // Independent locked seed42 oracle: r1=0x3fe15e014267f5aa,
    // r2=0x3fe45dec0e3bca26. Skipping right on NULL would compare r1 to r1.
    assert_eq!(result(&output), vec![Some(true)]);
    assert!(output.errors().is_empty());
}
