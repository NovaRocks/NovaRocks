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

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Float64Array, Int64Array, StringArray};
use novarocks_functions::{
    DataFailureReason, EvaluatedArgument, FunctionArgument, RowDataError, Selection,
};
use novarocks_type_contract::FunctionValueType;

use super::scalar_rows::{Control, prepare};

fn evaluate(name: &str, arrays: &[ArrayRef]) -> (ArrayRef, Box<[RowDataError]>) {
    let rows = arrays[0].len();
    let types: Vec<_> = arrays
        .iter()
        .map(|array| FunctionArgument::Value {
            value_type: FunctionValueType::new(array.data_type().clone(), true),
            constant: None,
        })
        .collect();
    let arguments: Vec<_> = arrays.iter().map(EvaluatedArgument::Column).collect();
    let output = prepare(name, &types)
        .evaluate(Selection::all(rows), &arguments, &Control)
        .unwrap();
    let (_, values, errors) = output.into_parts();
    (values, errors)
}

#[test]
fn bar_accepts_typed_column_arguments_and_preserves_terminal_bytes() {
    let percentages: ArrayRef = Arc::new(Float64Array::from(vec![
        Some(0.5),
        Some(1.0),
        Some(1.0),
        None,
    ]));
    let widths: ArrayRef = Arc::new(Int64Array::from(vec![6, 1, -1, 1]));
    let (values, errors) = evaluate("bar", &[percentages, widths]);
    let bars = values.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(
        bars.iter().collect::<Vec<_>>(),
        [
            Some("\x1b[38;5;196m█\x1b[38;5;208m█\x1b[38;5;220m█\x1b[0m   "),
            Some("\x1b[38;5;16m█\x1b[0m"),
            Some("\x1b[0m"),
            None,
        ]
    );
    assert!(errors.is_empty());
}

#[test]
fn width_bucket_accepts_per_row_bounds_and_authors_the_original_row_errors() {
    let operands: ArrayRef = Arc::new(Float64Array::from(vec![
        Some(-1.0),
        Some(0.0),
        Some(2.0),
        Some(10.0),
        Some(f64::NAN),
        None,
        Some(5.0),
    ]));
    let first_bounds: ArrayRef =
        Arc::new(Float64Array::from(vec![0.0, 0.0, 10.0, 0.0, 0.0, 0.0, 0.0]));
    let second_bounds: ArrayRef = Arc::new(Float64Array::from(vec![
        10.0, 10.0, 0.0, 10.0, 10.0, 10.0, 10.0,
    ]));
    let counts: ArrayRef = Arc::new(Int64Array::from(vec![5, 5, 5, i64::MAX, 5, 5, 0]));
    let (values, errors) = evaluate(
        "width_bucket",
        &[operands, first_bounds, second_bounds, counts],
    );
    let buckets = values.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(
        buckets.iter().collect::<Vec<_>>(),
        [Some(0), Some(1), Some(4), None, None, None, None]
    );
    let invalid = DataFailureReason::Domain(
        novarocks_function_contract::kernel_failure::INVALID_FUNCTION_ARGUMENT,
    );
    assert_eq!(
        errors
            .iter()
            .map(|error| (error.selected_ordinal(), error.reason()))
            .collect::<Vec<_>>(),
        [
            (3, DataFailureReason::NumericOutOfRange),
            (4, invalid),
            (6, invalid),
        ]
    );
}
