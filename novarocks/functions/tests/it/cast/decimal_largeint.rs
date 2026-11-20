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

use std::{str::FromStr, sync::Arc};

use arrow_array::{ArrayRef, Decimal128Array, Decimal256Array};
use arrow_buffer::i256;
use novarocks_functions::{DataFailureReason, FunctionIntrinsicRowError, Selection};
use novarocks_type_contract::DecimalOverflowPolicy;

use super::evaluate_to_largeint;

#[test]
fn decimal128_scales_truncate_toward_zero_or_multiply_exactly() {
    let positive: ArrayRef = Arc::new(
        Decimal128Array::from(vec![Some(12345), Some(-12345), Some(-99), None])
            .with_precision_and_scale(38, 2)
            .unwrap(),
    );
    let (out, errors, effects) = evaluate_to_largeint(
        &positive,
        Selection::all(4),
        DecimalOverflowPolicy::ReportError,
    );
    assert_eq!(out, [Some(123), Some(-123), Some(0), None]);
    assert!(errors.is_empty());
    assert_eq!(effects, FunctionIntrinsicRowError::NoRowError);

    let negative: ArrayRef = Arc::new(
        Decimal128Array::from(vec![Some(89), Some(-25), Some(0), None])
            .with_precision_and_scale(36, -2)
            .unwrap(),
    );
    let (out, errors, effects) = evaluate_to_largeint(
        &negative,
        Selection::all(4),
        DecimalOverflowPolicy::ReportError,
    );
    assert_eq!(out, [Some(8900), Some(-2500), Some(0), None]);
    assert!(errors.is_empty());
    assert_eq!(effects, FunctionIntrinsicRowError::NoRowError);
}

#[test]
fn decimal256_scaled_signed_extremes_and_overflow_follow_frozen_policy() {
    let wide: ArrayRef = Arc::new(
        Decimal256Array::from(vec![
            Some(i256::from_str("17014118346046923173168730371588410572799").unwrap()),
            Some(i256::from_str("-17014118346046923173168730371588410572899").unwrap()),
            Some(i256::from_str("17014118346046923173168730371588410572800").unwrap()),
            None,
            Some(i256::from_str("-17014118346046923173168730371588410572900").unwrap()),
            Some(i256::ZERO),
        ])
        .with_precision_and_scale(43, 2)
        .unwrap(),
    );
    let rows = [0, 1, 2, 3, 4];
    let selection = Selection::try_sparse(6, &rows).unwrap();
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        let (out, errors, effects) = evaluate_to_largeint(&wide, selection, policy);
        assert_eq!(out, [Some(i128::MAX), Some(i128::MIN), None, None, None]);
        if policy == DecimalOverflowPolicy::ReportError {
            assert_eq!(effects, FunctionIntrinsicRowError::MayRaise);
            assert_eq!(errors.len(), 2);
            for (error, ordinal) in errors.iter().zip([2, 4]) {
                assert_eq!(error.reason(), DataFailureReason::NumericOutOfRange);
                assert_eq!(error.selected_ordinal(), ordinal);
            }
        } else {
            assert_eq!(effects, FunctionIntrinsicRowError::NoRowError);
            assert!(errors.is_empty());
        }
    }
}

#[test]
fn decimal_negative_scale_overflow_retains_zero_and_reports_only_overflow_rows() {
    let narrow: ArrayRef = Arc::new(
        Decimal128Array::from(vec![Some(1), Some(i128::MAX / 10 + 1), None])
            .with_precision_and_scale(38, -1)
            .unwrap(),
    );
    let wide: ArrayRef = Arc::new(
        Decimal256Array::from(vec![Some(i256::ZERO), Some(i256::ONE), None])
            .with_precision_and_scale(1, i8::MIN)
            .unwrap(),
    );
    for (input, expected) in [
        (narrow, [Some(10), None, None]),
        (wide, [Some(0), None, None]),
    ] {
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            let (out, errors, _) = evaluate_to_largeint(&input, Selection::all(3), policy);
            assert_eq!(out, expected);
            if policy == DecimalOverflowPolicy::ReportError {
                assert_eq!(errors.len(), 1);
                assert_eq!(errors[0].selected_ordinal(), 1);
                assert_eq!(errors[0].reason(), DataFailureReason::NumericOutOfRange);
            } else {
                assert!(errors.is_empty());
            }
        }
    }
}
