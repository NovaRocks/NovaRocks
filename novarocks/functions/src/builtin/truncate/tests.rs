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

use super::super::round::tests::evaluate;
use crate::Selection;
use arrow_array::{ArrayRef, Decimal128Array, Float64Array, Int32Array};
use arrow_schema::DataType;
use std::sync::Arc;

#[test]
fn truncate_decimal_drops_digits_without_a_float_roundtrip() {
    let exact = 123_456_789_123_456_789_123_255_i128;
    let raw: ArrayRef = Arc::new(
        Decimal128Array::from(vec![Some(exact), Some(-2550), Some(2550), None])
            .with_precision_and_scale(30, 2)
            .unwrap(),
    );
    let digits: ArrayRef = Arc::new(Int32Array::from(vec![1, -1, i32::MIN, 0]));
    let output = evaluate(false, &[raw.clone(), digits], Selection::all(4)).0;
    assert_eq!(output.data_type(), &DataType::Decimal128(30, 2));
    assert_eq!(
        output
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        [Some(exact - 5), Some(-2000), Some(0), None],
    );
    let output = evaluate(false, &[raw], Selection::all(4)).0;
    assert_eq!(output.data_type(), &DataType::Decimal128(28, 0));
    assert_eq!(
        output
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        [Some(exact / 100), Some(-25), Some(25), None],
    );
}

#[test]
fn truncate_double_unary_keeps_floating_shape_and_signed_zero() {
    let input: ArrayRef = Arc::new(Float64Array::from(vec![f64::INFINITY, -0.12, -1.9]));
    let output = evaluate(false, &[input], Selection::all(3)).0;
    let values = output.as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(values.value(0), f64::INFINITY);
    assert_eq!(values.value(1).to_bits(), (-0.0f64).to_bits());
    assert_eq!(values.value(2), -1.0);
}
