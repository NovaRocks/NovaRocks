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
use arrow_array::{
    Array, ArrayRef, Decimal128Array, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array,
};
use arrow_schema::{DataType, Field};
use novarocks_failure_contract::kernel_reason::INVALID_FUNCTION_ARGUMENT;
use novarocks_functions::{
    DataFailureReason, EvaluatedArgument, FunctionArgument, FunctionArgumentType,
    FunctionBindingError, FunctionBindingRequest, FunctionKind, Selection,
    builtin::catalogue::build_builtin_engine_function_catalog,
};
use novarocks_type_contract::FunctionValueType;
use std::sync::Arc;

fn evaluate(
    name: &str,
    arrays: &[ArrayRef],
    rows: Selection<'_>,
) -> (ArrayRef, Vec<novarocks_functions::RowDataError>) {
    let arguments = arrays
        .iter()
        .map(|array| FunctionArgument::Value {
            value_type: FunctionValueType::new(array.data_type().clone(), array.null_count() != 0),
            constant: None,
        })
        .collect::<Vec<_>>();
    let mut instance = prepare(name, &arguments);
    let input = arrays
        .iter()
        .map(EvaluatedArgument::Column)
        .collect::<Vec<_>>();
    let output = instance.evaluate(rows, &input, &Control).unwrap();
    (output.values().clone(), output.errors().to_vec())
}

#[test]
fn narrow_shift_preserves_sign_and_uses_the_declared_width() {
    let value: ArrayRef = Arc::new(Int8Array::from(vec![
        Some(-1),
        Some(-1),
        Some(64),
        Some(-1),
        None,
    ]));
    let count: ArrayRef = Arc::new(Int32Array::from(vec![1, -64, 1, 64, 1]));
    let input = [value, count];
    for (name, expected) in [
        (
            "bitwise_left_shift",
            vec![Some(-2), Some(-1), Some(-128), Some(0), None],
        ),
        (
            "bitwise_right_shift",
            vec![Some(127), Some(-1), Some(32), Some(0), None],
        ),
        (
            "bitwise_right_shift_arithmetic",
            vec![Some(-1), Some(-1), Some(32), Some(-1), None],
        ),
    ] {
        let (values, errors) = evaluate(name, &input, Selection::all(5));
        assert!(errors.is_empty());
        assert_eq!(
            values
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            expected
        );
    }
}

#[test]
fn bitwise_sparse_rows_keep_selected_bigint_addresses() {
    let left: ArrayRef = Arc::new(Int64Array::from(vec![15, 0, -9, -1]));
    let right: ArrayRef = Arc::new(Int64Array::from(vec![3, -1, 64, 32]));
    let rows = Selection::try_sparse(4, &[0, 3]).unwrap();
    let (values, errors) = evaluate("bitwise_and", &[left, right], rows);
    assert!(errors.is_empty());
    assert_eq!(
        values
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[3, 32]
    );
}

#[test]
fn absolute_value_minimum_is_a_selected_numeric_error() {
    for (input, expected) in [
        (
            Arc::new(Int8Array::from(vec![0, i8::MIN, -6])) as ArrayRef,
            DataType::Int8,
        ),
        (
            Arc::new(Int16Array::from(vec![0, i16::MIN, -6])) as ArrayRef,
            DataType::Int16,
        ),
        (
            Arc::new(Int32Array::from(vec![0, i32::MIN, -6])) as ArrayRef,
            DataType::Int32,
        ),
        (
            Arc::new(Int64Array::from(vec![0, i64::MIN, -6])) as ArrayRef,
            DataType::Int64,
        ),
    ] {
        let (values, errors) =
            evaluate("abs", &[input], Selection::try_sparse(3, &[1, 2]).unwrap());
        assert_eq!(values.data_type(), &expected);
        assert!(values.is_null(0) && !values.is_null(1));
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].selected_ordinal(), 0);
        assert_eq!(errors[0].reason(), DataFailureReason::NumericOutOfRange);
    }
}

#[test]
fn numeric_sign_and_integral_rounding_keep_the_trino_type() {
    let decimal: ArrayRef = Arc::new(
        Decimal128Array::from(vec![-154i128, 154])
            .with_precision_and_scale(4, 2)
            .unwrap(),
    );
    for (name, precision, expected) in [
        ("floor", 3, [-2, 1]),
        ("ceiling", 3, [-1, 2]),
        ("sign", 1, [-1, 1]),
    ] {
        let (values, errors) = evaluate(name, std::slice::from_ref(&decimal), Selection::all(2));
        assert!(errors.is_empty());
        assert_eq!(values.data_type(), &DataType::Decimal128(precision, 0));
        assert_eq!(
            values
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .values()
                .as_ref(),
            &expected
        );
    }
    let input: ArrayRef = Arc::new(Float32Array::from(vec![f32::NAN, -0.0, f32::NEG_INFINITY]));
    let (values, errors) = evaluate("sign", &[input], Selection::all(3));
    assert!(errors.is_empty());
    let values = values.as_any().downcast_ref::<Float32Array>().unwrap();
    assert!(values.value(0).is_nan());
    assert_eq!(values.value(1).to_bits(), (-0.0_f32).to_bits());
    assert_eq!(values.value(2), -1.0);
}

#[test]
fn log_base_keeps_order_and_does_not_null_non_finite_results() {
    let base: ArrayRef = Arc::new(Float64Array::from(vec![
        Some(2.0),
        Some(8.0),
        Some(2.0),
        Some(1.0),
        Some(-2.0),
        None,
    ]));
    let value: ArrayRef = Arc::new(Float64Array::from(vec![8.0, 2.0, 0.0, 8.0, 8.0, 8.0]));
    let (values, errors) = evaluate("log", &[base, value], Selection::all(6));
    assert!(errors.is_empty());
    let values = values.as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(values.value(0), 3.0);
    assert!((values.value(1) - 0.33333333333333337).abs() < 1e-15);
    assert_eq!(values.value(2), f64::NEG_INFINITY);
    assert_eq!(values.value(3), f64::INFINITY);
    assert!(values.value(4).is_nan());
    assert!(values.is_null(5));
}

#[test]
fn numeric_mod_uses_the_arithmetic_decimal_and_division_contracts() {
    let left: ArrayRef = Arc::new(
        Decimal128Array::from(vec![1542i128, 1234])
            .with_precision_and_scale(5, 2)
            .unwrap(),
    );
    let right: ArrayRef = Arc::new(
        Decimal128Array::from(vec![2300i128, 0])
            .with_precision_and_scale(4, 3)
            .unwrap(),
    );
    let (values, errors) = evaluate("mod", &[left, right], Selection::all(2));
    assert_eq!(values.data_type(), &DataType::Decimal128(4, 3));
    let values = values.as_any().downcast_ref::<Decimal128Array>().unwrap();
    assert_eq!(values.value(0), 1620);
    assert!(values.is_null(1));
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].reason(), DataFailureReason::DivisionByZero);
    assert_eq!(errors[0].selected_ordinal(), 1);

    let left: ArrayRef = Arc::new(Float32Array::from(vec![1.0]));
    let right: ArrayRef = Arc::new(Float32Array::from(vec![0.0]));
    let (values, errors) = evaluate("mod", &[left, right], Selection::all(1));
    assert!(errors.is_empty());
    assert!(
        values
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .value(0)
            .is_nan()
    );
}

#[test]
fn rand_upper_bound_follows_row_values_and_preserves_all_widths() {
    macro_rules! check {
        ($array:ty) => {
            let input: ArrayRef = Arc::new(<$array>::from(vec![
                Some(-2),
                Some(1),
                None,
                Some(0),
                Some(5),
            ]));
            for name in ["rand", "random"] {
                let rows = Selection::try_sparse(5, &[1, 2, 3, 4]).unwrap();
                let (values, errors) = evaluate(name, std::slice::from_ref(&input), rows);
                let values = values.as_any().downcast_ref::<$array>().unwrap();
                assert_eq!(values.value(0), 0);
                assert!(values.is_null(1) && values.is_null(2));
                assert!((0..5).contains(&values.value(3)));
                assert_eq!(errors.len(), 1);
                assert_eq!(errors[0].selected_ordinal(), 2);
                assert_eq!(
                    errors[0].reason(),
                    DataFailureReason::Domain(INVALID_FUNCTION_ARGUMENT)
                );
            }
        };
    }
    check!(Int8Array);
    check!(Int16Array);
    check!(Int32Array);
    check!(Int64Array);

    let (values, errors) = evaluate("rand", &[], Selection::all(64));
    assert!(errors.is_empty());
    assert!(
        values
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .values()
            .iter()
            .all(|value| (0.0..1.0).contains(value))
    );
}

#[test]
fn trino_numeric_declares_supported_missing_and_retired_signatures() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let bind = |name: &str, types: &[DataType]| {
        let arguments = types
            .iter()
            .map(|ty| FunctionArgument::Value {
                value_type: FunctionValueType::new(ty.clone(), false),
                constant: None,
            })
            .collect::<Vec<_>>();
        catalog.resolve_bound_user(
            name,
            FunctionKind::Scalar,
            FunctionBindingRequest {
                expected_result_type: None,
                arguments: &arguments,
                logical_argument_count: arguments.len(),
            },
            &Control,
        )
    };

    for name in ["sinh", "cosh", "tanh"] {
        let binding = bind(name, &[DataType::Float64]);
        assert!(
            matches!(binding, Err(FunctionBindingError::UnavailableImplementation(overload))
            if overload.as_str().starts_with(&format!("builtin.scalar/{name}/")))
        );
        assert_eq!(
            bind(name, &[]).unwrap_err(),
            FunctionBindingError::NoMatchingOverload
        );
    }
    for name in ["bitand", "bit_shift_left", "l2_distance", "degress", "fmod"] {
        assert_eq!(
            bind(name, &[]).unwrap_err(),
            FunctionBindingError::UnknownFunction
        );
    }
    assert_eq!(
        bind("bitwise_left_shift", &[DataType::Int8, DataType::Int64]).unwrap_err(),
        FunctionBindingError::NoMatchingOverload
    );
    for input in [DataType::Utf8, DataType::Boolean] {
        assert_eq!(
            bind("abs", &[input]).unwrap_err(),
            FunctionBindingError::NoMatchingOverload
        );
    }
    let bound = bind("bitwise_and", &[DataType::Int8, DataType::Int16]).unwrap();
    assert!(bound.selected.argument_types.iter().all(|ty| matches!(
        ty,
        FunctionArgumentType::Value(value) if value.data_type() == &DataType::Int64
    )));

    let integers = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
    let bound = bind("euclidean_distance", &[integers.clone(), integers]).unwrap();
    let doubles = DataType::List(Arc::new(Field::new("item", DataType::Float64, true)));
    assert!(bound.selected.argument_types.iter().all(|ty| matches!(
        ty,
        FunctionArgumentType::Value(value) if value.data_type() == &doubles
    )));
}

#[test]
fn shift_retains_the_original_bounded_allocation_shape() {
    let count: ArrayRef = Arc::new(Int32Array::from(vec![1]));
    for (values, expected_requests) in [
        (Arc::new(Int8Array::from(vec![1])) as ArrayRef, 12),
        (Arc::new(Int64Array::from(vec![1])) as ArrayRef, 7),
    ] {
        let arguments =
            [values.data_type().clone(), DataType::Int32].map(|ty| FunctionArgument::Value {
                value_type: FunctionValueType::new(ty, false),
                constant: None,
            });
        let instance = prepare("bitwise_left_shift", &arguments);
        let profile = instance.invocation_resource_profile().unwrap();
        let inputs = [
            EvaluatedArgument::Scalar(&values),
            EvaluatedArgument::Scalar(&count),
        ];
        let facts = profile.requests_for(Selection::all(513), &inputs).unwrap();
        assert_eq!(facts, profile.requests(513).unwrap());
        assert_eq!(facts.requests(), expected_requests);
    }
}
