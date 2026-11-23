// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use super::scalar_rows::{Control, prepare};
use arrow_array::{Array, ArrayRef, Float64Array, ListArray};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::Field;
use novarocks_failure_contract::kernel_reason::INVALID_FUNCTION_ARGUMENT;
use novarocks_functions::{
    DataFailureReason, EvaluatedArgument, FunctionArgument, RowDataError, Selection,
};
use novarocks_type_contract::FunctionValueType;
use std::sync::Arc;

fn list(values: ArrayRef, offsets: Vec<i32>, valid: Option<Vec<bool>>) -> ArrayRef {
    Arc::new(
        ListArray::try_new(
            Arc::new(Field::new("item", values.data_type().clone(), true)),
            OffsetBuffer::new(offsets.into()),
            values,
            valid.map(NullBuffer::from),
        )
        .unwrap(),
    )
}

fn vector(values: &[Option<f64>]) -> ArrayRef {
    list(
        Arc::new(Float64Array::from(values.to_vec())),
        vec![0, i32::try_from(values.len()).unwrap()],
        None,
    )
}

fn evaluate(
    name: &str,
    runtime: [EvaluatedArgument<'_>; 2],
    selection: Selection<'_>,
) -> (Vec<Option<f64>>, Vec<RowDataError>) {
    let arguments = runtime.map(|argument| FunctionArgument::Value {
        value_type: FunctionValueType::from_legacy_declared_projection(
            argument.array().data_type().clone(),
            true,
        ),
        constant: None,
    });
    let mut instance = prepare(name, &arguments);
    let output = instance.evaluate(selection, &runtime, &Control).unwrap();
    (
        output
            .values()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .iter()
            .collect(),
        output.errors().to_vec(),
    )
}

fn pair(name: &str, left: &ArrayRef, right: &ArrayRef) -> (Vec<Option<f64>>, Vec<RowDataError>) {
    evaluate(
        name,
        [
            EvaluatedArgument::Column(left),
            EvaluatedArgument::Column(right),
        ],
        Selection::all(1),
    )
}

fn assert_values(output: (Vec<Option<f64>>, Vec<RowDataError>), expected: &[Option<f64>]) {
    let (values, errors) = output;
    assert!(errors.is_empty(), "{errors:?}");
    assert_eq!(values, expected);
}

#[test]
fn vector_named_functions_preserve_cosine_and_return_euclidean_distance() {
    let base = vector(&[Some(3.0), Some(4.0)]);
    let target = vector(&[Some(6.0), Some(8.0)]);
    for (name, expected) in [("cosine_similarity", 1.0), ("euclidean_distance", 5.0)] {
        assert_values(pair(name, &base, &target), &[Some(expected)]);
    }
}

#[test]
fn vector_final_nonfinite_results_preserve_ieee_values() {
    for name in ["cosine_similarity", "euclidean_distance"] {
        let (values, errors) = pair(
            name,
            &vector(&[Some(f64::INFINITY)]),
            &vector(&[Some(f64::INFINITY)]),
        );
        assert!(errors.is_empty());
        assert!(values[0].unwrap().is_nan());
    }
}

#[test]
fn vector_errors_follow_length_then_null_then_cosine_magnitude() {
    let null = vector(&[None]);
    let empty = vector(&[]);
    let zero = vector(&[Some(0.0)]);
    let one = vector(&[Some(1.0)]);
    assert_values(pair("euclidean_distance", &null, &one), &[Some(1.0)]);
    assert_values(pair("euclidean_distance", &empty, &empty), &[Some(0.0)]);
    assert_values(pair("cosine_similarity", &null, &zero), &[None]);
    for (name, left, right) in [
        ("cosine_similarity", &empty, &empty),
        ("cosine_similarity", &zero, &one),
        ("cosine_similarity", &null, &empty),
        ("euclidean_distance", &null, &empty),
    ] {
        let (values, errors) = pair(name, left, right);
        assert_eq!(values, [None]);
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].selected_ordinal(), 0);
        assert_eq!(
            errors[0].reason(),
            DataFailureReason::Domain(INVALID_FUNCTION_ARGUMENT)
        );
    }

    let outer_null = list(
        Arc::new(Float64Array::from(vec![1.0])),
        vec![0, 1],
        Some(vec![false]),
    );
    for name in ["cosine_similarity", "euclidean_distance"] {
        assert_values(pair(name, &outer_null, &empty), &[None]);
    }
}

#[test]
fn vector_reads_only_selected_reachable_elements() {
    let base = list(
        Arc::new(Float64Array::from(vec![
            None,
            None,
            Some(3.0),
            Some(4.0),
            None,
        ])),
        vec![1, 2, 4],
        None,
    );
    let zero = vector(&[Some(0.0), Some(0.0)]);
    let arguments = [
        EvaluatedArgument::Column(&base),
        EvaluatedArgument::Scalar(&zero),
    ];
    assert_values(
        evaluate(
            "euclidean_distance",
            arguments,
            Selection::try_sparse(2, &[1]).unwrap(),
        ),
        &[Some(5.0)],
    );
    let (values, errors) = evaluate("euclidean_distance", arguments, Selection::all(2));
    assert_eq!(values, [None, Some(5.0)]);
    assert_eq!(errors[0].selected_ordinal(), 0);
    assert_eq!(
        errors[0].reason(),
        DataFailureReason::Domain(INVALID_FUNCTION_ARGUMENT)
    );
}
