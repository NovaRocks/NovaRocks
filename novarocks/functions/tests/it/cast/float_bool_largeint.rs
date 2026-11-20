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

use arrow_array::{ArrayRef, BooleanArray, Float32Array, Float64Array};
use novarocks_functions::{FunctionIntrinsicRowError, Selection};
use novarocks_type_contract::DecimalOverflowPolicy;

use super::evaluate_to_largeint;

#[test]
fn both_float_widths_truncate_and_null_nonfinite_and_unrepresentable_values() {
    let f32_values: ArrayRef = Arc::new(Float32Array::from(vec![
        Some(-7.75_f32),
        Some(7.75),
        Some(-0.75),
        Some(f32::NAN),
        Some(f32::INFINITY),
        Some(f32::NEG_INFINITY),
        Some(i128::MIN as f32),
        Some(i128::MAX as f32),
        Some(-f32::MAX),
        None,
    ]));
    let f64_values: ArrayRef = Arc::new(Float64Array::from(vec![
        Some(-7.75_f64),
        Some(7.75),
        Some(-0.75),
        Some(f64::NAN),
        Some(f64::INFINITY),
        Some(f64::NEG_INFINITY),
        Some(i128::MIN as f64),
        Some(i128::MAX as f64),
        Some(-f64::MAX),
        None,
    ]));
    for values in [f32_values, f64_values] {
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            let (out, errors, effects) = evaluate_to_largeint(&values, Selection::all(10), policy);
            assert_eq!(
                out,
                [
                    Some(-7),
                    Some(7),
                    Some(0),
                    None,
                    None,
                    None,
                    Some(i128::MIN),
                    None,
                    None,
                    None
                ],
            );
            assert!(errors.is_empty());
            assert_eq!(effects, FunctionIntrinsicRowError::NoRowError);
        }
    }
}

#[test]
fn boolean_to_largeint_keeps_zero_one_and_null() {
    let values: ArrayRef = Arc::new(BooleanArray::from(vec![Some(false), Some(true), None]));
    let (out, errors, effects) = evaluate_to_largeint(
        &values,
        Selection::all(3),
        DecimalOverflowPolicy::ReportError,
    );
    assert_eq!(out, [Some(0), Some(1), None]);
    assert!(errors.is_empty());
    assert_eq!(effects, FunctionIntrinsicRowError::NoRowError);
}
