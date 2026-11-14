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

use arrow_array::{Array, ArrayRef, Decimal128Array, Decimal256Array};
use arrow_buffer::i256;
use novarocks_functions::legacy_literal::{eval, native_negate_zero};
use novarocks_type_contract::DecimalOverflowPolicy;
use std::sync::Arc;

/// This is an independent original array-author witness. No new fact/helper
/// is used to choose or generate the expected raw output.
#[test]
fn native_negate_original_decimal_full_carrier_outside_precision_is_successful_null() {
    let arrays: [ArrayRef; 2] = [
        Arc::new(
            Decimal128Array::from(vec![Some(10_i128), Some(-10), Some(1), None])
                .with_precision_and_scale(1, 0)
                .unwrap(),
        ),
        Arc::new(
            Decimal256Array::from(vec![
                Some(i256::from_i128(10)),
                Some(i256::from_i128(-10)),
                Some(i256::ONE),
                None,
            ])
            .with_precision_and_scale(1, 0)
            .unwrap(),
        ),
    ];
    for input in arrays {
        assert_eq!(input.null_count(), 1);
        let zero = eval(&native_negate_zero(input.data_type()).unwrap(), input.len()).unwrap();
        let dtype = input.data_type().clone();
        let actual = novarocks_functions::legacy_arithmetic::eval_sub_arrays(
            zero,
            input,
            dtype.clone(),
            false,
            DecimalOverflowPolicy::OutputNull,
        )
        .unwrap();
        assert_eq!(actual.len(), 4);
        assert_eq!(actual.data_type(), &dtype);
        assert!(actual.is_null(0));
        assert!(actual.is_null(1));
        assert!(!actual.is_null(2));
        assert!(actual.is_null(3));
        match actual.data_type() {
            arrow_schema::DataType::Decimal128(_, _) => assert_eq!(
                actual
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .unwrap()
                    .value(2),
                -1
            ),
            arrow_schema::DataType::Decimal256(_, _) => assert_eq!(
                actual
                    .as_any()
                    .downcast_ref::<Decimal256Array>()
                    .unwrap()
                    .value(2),
                i256::MINUS_ONE
            ),
            _ => unreachable!(),
        }
    }
}
