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
fn ordered_eager_right_rand_visits_left_null_and_both_instances_keep_independent_batches() {
    let program = ordered_program(BinaryOperator::Lt, DataType::Float64, Source::Random, false);
    let input = ordered_batch(
        &program,
        Arc::new(Float64Array::from(vec![None; 6])),
        Arc::new(Float64Array::from(vec![None; 6])),
        vec![
            Some(true),
            Some(false),
            Some(true),
            None,
            Some(true),
            Some(true),
        ],
    );
    let rows = [1, 4];
    let selection = Selection::try_sparse(6, &rows).unwrap();
    let mut evaluator = ordered_instance(&program);
    let empty = Selection::try_sparse(6, &[]).unwrap();
    let (_, empty_calls) = observe_calls(|| {
        evaluator
            .evaluate(
                EvaluationInput::Batch {
                    batch: &input,
                    source: None,
                },
                ScalarInvocationScope::new(empty, empty),
                ScalarInvocationActivation::ValidateOnly,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap()
    });
    assert!(
        empty_calls
            .iter()
            .all(|call| !matches!(call, LeafCallObservation::Rows { .. }))
    );
    assert!(evaluator.instances.is_empty());

    let (output, calls) = observe_calls(|| {
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
    let rows = calls
        .into_iter()
        .filter_map(|call| match call {
            LeafCallObservation::Rows { actual, demand, .. } => Some((actual, demand)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(rows, vec![(vec![4], vec![4]), (vec![1, 4], vec![1, 4])]);
    assert_eq!(result(&output), vec![None, Some(false)]);
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
    assert_eq!(result(&output), vec![Some(false)]);
    assert!(output.errors().is_empty());
}
