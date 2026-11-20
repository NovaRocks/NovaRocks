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
use arrow_array::{Array, ArrayRef, Decimal128Array, Float64Array, ListArray, UInt32Array};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::Field;
use novarocks_functions::{EvaluatedArgument, FunctionArgument, RowDataError, Selection};
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
fn vector_names_preserve_cosine_normalized_dot_and_squared_l2() {
    let base = vector(&[Some(3.0), Some(4.0)]);
    let target = vector(&[Some(6.0), Some(8.0)]);
    for (name, expected) in [
        ("cosine_similarity", 1.0),
        ("approx_cosine_similarity", 1.0),
        ("cosine_similarity_norm", 50.0),
        ("l2_distance", 25.0),
        ("approx_l2_distance", 25.0),
    ] {
        assert_values(pair(name, &base, &target), &[Some(expected)]);
    }

    let zero = vector(&[Some(0.0), Some(0.0)]);
    assert_values(pair("cosine_similarity", &zero, &base), &[Some(0.0)]);
    assert_values(
        pair(
            "cosine_similarity_norm",
            &vector(&[Some(1e16), Some(1.0), Some(-1e16)]),
            &vector(&[Some(1.0); 3]),
        ),
        &[Some(0.0)],
    );
}

#[test]
fn vector_final_nonfinite_values_are_successful_nulls() {
    for (name, base, target) in [
        ("cosine_similarity", f64::INFINITY, 1.0),
        ("cosine_similarity_norm", f64::MAX, 2.0),
        ("l2_distance", f64::MAX, -f64::MAX),
        ("l2_distance", f64::NAN, 0.0),
    ] {
        assert_values(
            pair(name, &vector(&[Some(base)]), &vector(&[Some(target)])),
            &[None],
        );
    }
}

#[test]
fn vector_reports_selected_outer_child_empty_and_dimension_errors() {
    let outer_null = list(
        Arc::new(Float64Array::from(vec![1.0])),
        vec![0, 1],
        Some(vec![false]),
    );
    let required = vector(&[Some(1.0)]);
    let child_null = vector(&[None]);
    for (base, target, error) in [
        (&outer_null, &required, "base array has null value"),
        (&required, &outer_null, "target array has null value"),
        (&child_null, &required, "base array has null value"),
        (&required, &child_null, "target array has null value"),
        (&vector(&[]), &vector(&[]), "requires non-empty arrays"),
        (
            &required,
            &vector(&[Some(1.0), Some(2.0)]),
            "requires equal length arrays",
        ),
    ] {
        let (result, errors) = pair("l2_distance", base, target);
        assert_eq!(result, [None]);
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].selected_ordinal(), 0);
        assert!(errors[0].message().contains(error), "{errors:?}");
    }
}

#[test]
fn vector_reads_only_selected_reachable_elements() {
    // The first selected vector's child includes neither the unused prefix
    // and suffix nor the child of the null-bearing unselected vector row.
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
            "l2_distance",
            arguments,
            Selection::try_sparse(2, &[1]).unwrap(),
        ),
        &[Some(25.0)],
    );
    let (result, errors) = evaluate("l2_distance", arguments, Selection::all(2));
    assert_eq!(result, [None, Some(25.0)]);
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].selected_ordinal(), 0);
    assert!(errors[0].message().contains("base array has null value"));
}

#[test]
fn vector_reads_unsigned_and_scaled_decimal_children() {
    let unsigned_base = list(Arc::new(UInt32Array::from(vec![3, 4])), vec![0, 2], None);
    let unsigned_zero = list(Arc::new(UInt32Array::from(vec![0, 0])), vec![0, 2], None);
    let decimal = |values| {
        list(
            Arc::new(
                Decimal128Array::from(values)
                    .with_precision_and_scale(8, 1)
                    .unwrap(),
            ),
            vec![0, 2],
            None,
        )
    };
    for (base, zero) in [
        (unsigned_base, unsigned_zero),
        (decimal(vec![30i128, 40i128]), decimal(vec![0i128, 0i128])),
    ] {
        assert_values(pair("l2_distance", &base, &zero), &[Some(25.0)]);
    }
}
