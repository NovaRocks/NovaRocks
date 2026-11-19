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

use arrow::array::{Array, Decimal128Array, Float64Array};
use novarocks_expression::{
    EvaluationInput,
    testkit::{
        PassThroughCallHost,
        row_errors::{Control, Shape, batch, instance, program},
    },
};
use novarocks_functions::{ScalarInvocationActivation, ScalarInvocationScope, Selection};

#[test]
fn strict_null_and_real_decimal_overflow_remap_to_parent_selected_ordinal() {
    let program = program(Shape::Single);
    let max38 = 10_i128.pow(38) - 1;
    let input = batch(
        &program,
        vec![Some(123), None, Some(max38), Some(25), Some(15)],
        vec![None; 5],
    );
    let rows = [1, 2, 4];
    let selection = Selection::try_sparse(5, &rows).unwrap();
    let mut evaluator = instance(&program, 0);
    let result = evaluator
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
    let values = result
        .values()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(result.selection(), selection);
    assert_eq!(values.len(), 3);
    assert!(values.is_null(0));
    assert!(values.is_null(1));
    assert_eq!(values.value(2), 20);
    assert_eq!(result.errors().len(), 1);
    assert_eq!(result.errors()[0].selected_ordinal(), 1);
    assert!(result.errors()[0].message().contains("overflow"));
    // Row errors do not poison a successful evaluator instance.
    let clean = batch(&program, vec![Some(25)], vec![None]);
    let next = evaluator
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
    assert!(next.errors().is_empty());
}

#[test]
fn nested_round_keeps_required_child_error_and_successful_null_distinct() {
    let program = program(Shape::Nested);
    let input = batch(
        &program,
        vec![Some(15), None, Some(10_i128.pow(38) - 1), Some(25)],
        vec![None; 4],
    );
    let rows = [1, 2, 3];
    let result = instance(&program, 0)
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
    let values = result
        .values()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert!(values.is_null(0));
    assert!(values.is_null(1));
    assert_eq!(values.value(2), 30);
    assert_eq!(result.errors().len(), 1);
    assert_eq!(result.errors()[0].selected_ordinal(), 1);
}

#[test]
fn eager_rng_child_advances_even_when_parent_strict_digits_are_null() {
    let program = program(Shape::ObservableSibling);
    let mut parent = instance(&program, 0);
    let mut reference = instance(&program, 1);
    let null_digits = batch(&program, vec![None; 2], vec![None; 2]);
    let skipped = parent
        .evaluate(
            EvaluationInput::Batch {
                batch: &null_digits,
                source: None,
            },
            ScalarInvocationScope::new(Selection::all(2), Selection::all(2)),
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert_eq!(skipped.values().null_count(), 2);
    assert!(skipped.errors().is_empty());
    let first_two = reference
        .evaluate(
            EvaluationInput::Batch {
                batch: &null_digits,
                source: None,
            },
            ScalarInvocationScope::new(Selection::all(2), Selection::all(2)),
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert_eq!(first_two.values().null_count(), 0);
    let next = batch(&program, vec![None], vec![Some(15)]);
    let actual = parent
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
    let reference = reference
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
    let source = reference
        .values()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(0);
    // Independent formula for this finite, positive, small-magnitude digit case.
    let expected = (source * 1e15).round() / 1e15;
    assert_eq!(
        actual
            .values()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0)
            .to_bits(),
        expected.to_bits()
    );
    assert!(actual.errors().is_empty());
}
