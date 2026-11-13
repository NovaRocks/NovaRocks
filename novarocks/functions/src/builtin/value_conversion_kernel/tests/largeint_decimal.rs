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

use super::*;
use crate::builtin::value_conversion::resolve_value_conversion;
use arrow_array::{Decimal128Array, Decimal256Array};
use arrow_buffer::i256;
use std::str::FromStr;

fn decimal_prepared(
    target: &FunctionValueType,
    policy: DecimalOverflowPolicy,
) -> Arc<dyn crate::PreparedScalarKernel> {
    super::super::super::value_conversion_owner::prepared_for_test_with_policy(
        &large_type(true),
        target,
        policy,
    )
    .unwrap()
}

#[test]
fn largeint_decimal_exact_pair_checks_nominal_source_target_and_overflow_nullability() {
    let source = large_type(false);
    for target in [
        physical(DataType::Decimal128(38, 0), true),
        physical(DataType::Decimal256(41, 2), false),
        physical(DataType::Decimal128(38, -1), false),
        physical(DataType::Decimal256(1, -76), false),
    ] {
        let selected =
            resolve_value_conversion(&source, &target, &CompileControl::default()).unwrap();
        assert_eq!(
            selected.overload.as_str(),
            super::super::super::value_conversion::LARGEINT_DECIMAL
        );
        assert_eq!(
            selected.result_type,
            crate::FunctionResultType::Scalar(target)
        );
    }
    for source in [
        physical(DataType::FixedSizeBinary(16), false),
        FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            false,
            ValueLogicalType::Uuid,
        )
        .unwrap(),
        physical(DataType::Int64, false),
    ] {
        assert!(
            resolve_value_conversion(
                &source,
                &physical(DataType::Decimal256(41, 2), true),
                &CompileControl::default()
            )
            .is_err()
        );
    }
    assert!(
        resolve_value_conversion(
            &large_type(false),
            &physical(DataType::Decimal128(38, 0), false),
            &CompileControl::default()
        )
        .is_err()
    );
    assert!(
        resolve_value_conversion(
            &large_type(true),
            &physical(DataType::Decimal256(41, 2), false),
            &CompileControl::default()
        )
        .is_err()
    );
}

#[test]
fn largeint_decimal256_keeps_full_signed_extremes_exact_scale_and_sparse_compact_scalar_rows() {
    let source = large_type(true);
    let target = physical(DataType::Decimal256(41, 2), true);
    let input = large(&[Some(7), Some(i128::MAX), None, Some(i128::MIN), Some(-15)]);
    let rows = [1, 2, 3, 4];
    let selection = Selection::try_sparse(5, &rows).unwrap();
    let compact = SelectedValues::try_new(
        selection,
        source.data_type(),
        input.slice(1, 4),
        Box::default(),
    )
    .unwrap();
    let expected: ArrayRef = Arc::new(
        Decimal256Array::from(vec![
            Some(i256::from_str("17014118346046923173168730371588410572700").unwrap()),
            None,
            Some(i256::from_str("-17014118346046923173168730371588410572800").unwrap()),
            Some(i256::from_i128(-1500)),
        ])
        .with_precision_and_scale(41, 2)
        .unwrap(),
    );
    for argument in [
        EvaluatedArgument::Column(&input),
        EvaluatedArgument::SelectedColumn(&compact),
    ] {
        assert_eq!(
            run(&source, &target, argument, selection).to_data(),
            expected.to_data()
        );
    }
    let scalar = input.slice(1, 1);
    let output = run(
        &source,
        &target,
        EvaluatedArgument::Scalar(&scalar),
        Selection::all(3),
    );
    let values = output.as_any().downcast_ref::<Decimal256Array>().unwrap();
    assert_eq!(
        values.values().as_ref(),
        &[i256::from_str("17014118346046923173168730371588410572700").unwrap(); 3]
    );
    let output = run(
        &source,
        &target,
        EvaluatedArgument::Column(&input),
        Selection::try_sparse(5, &[]).unwrap(),
    );
    assert_eq!(output.data_type(), target.data_type());
    assert!(output.is_empty());
}

#[test]
fn largeint_decimal128_enforces_all_precision_and_frozen_overflow_policy_at_selected_ordinals() {
    let target = physical(DataType::Decimal128(38, 0), true);
    let input = large(&[
        Some(0),
        Some(i128::MAX),
        None,
        Some(10_i128.pow(38) - 1),
        Some(i128::MIN),
    ]);
    let rows = [1, 2, 3, 4];
    let selection = Selection::try_sparse(5, &rows).unwrap();
    let arguments = [EvaluatedArgument::Column(&input)];
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        let mut instance =
            ScalarEvaluationInstance::instantiate(decimal_prepared(&target, policy)).unwrap();
        let output = instance
            .evaluate(selection, &arguments, &Control::default())
            .unwrap();
        let expected: ArrayRef = Arc::new(
            Decimal128Array::from(vec![
                None,
                None,
                Some(99_999_999_999_999_999_999_999_999_999_999_999_999_i128),
                None,
            ])
            .with_precision_and_scale(38, 0)
            .unwrap(),
        );
        assert_eq!(output.values().to_data(), expected.to_data());
        let ordinals = output
            .errors()
            .iter()
            .map(|error| error.selected_ordinal())
            .collect::<Vec<_>>();
        assert_eq!(
            ordinals,
            if policy == DecimalOverflowPolicy::ReportError {
                vec![0, 3]
            } else {
                vec![]
            }
        );
    }
}

#[test]
fn largeint_decimal_negative_scales_preserve_integral_truncation_and_zero_extremes() {
    let source = large_type(true);
    let input = large(&[
        Some(-15),
        Some(-16),
        Some(15),
        Some(16),
        Some(995),
        Some(1000),
        None,
    ]);
    let target = physical(DataType::Decimal128(2, -1), true);
    let output = run(
        &source,
        &target,
        EvaluatedArgument::Column(&input),
        Selection::all(7),
    );
    let expected: ArrayRef = Arc::new(
        Decimal128Array::from(vec![
            Some(-1),
            Some(-1),
            Some(1),
            Some(1),
            Some(99),
            None,
            None,
        ])
        .with_precision_and_scale(2, -1)
        .unwrap(),
    );
    assert_eq!(output.to_data(), expected.to_data());
    let input = large(&[Some(i128::MIN), Some(i128::MAX)]);
    let target = physical(DataType::Decimal128(1, -38), true);
    let output = run(
        &source,
        &target,
        EvaluatedArgument::Column(&input),
        Selection::all(2),
    );
    let values = output.as_any().downcast_ref::<Decimal128Array>().unwrap();
    assert_eq!(values.values().as_ref(), &[-1, 1]);
    let target = physical(DataType::Decimal256(1, -76), true);
    let output = run(
        &source,
        &target,
        EvaluatedArgument::Column(&input),
        Selection::all(2),
    );
    let values = output.as_any().downcast_ref::<Decimal256Array>().unwrap();
    assert_eq!(values.values().as_ref(), &[i256::ZERO, i256::ZERO]);
}

#[test]
fn largeint_decimal_every_runtime_callback_preserves_primary_cause_and_failed_latch() {
    let target = physical(DataType::Decimal256(3, 2), true);
    let input = large(&(0..320).map(|v| Some(i128::from(v))).collect::<Vec<_>>());
    let prepared = decimal_prepared(&target, DecimalOverflowPolicy::ReportError);
    let args = [EvaluatedArgument::Column(&input)];
    let baseline = Control::default();
    let mut instance = ScalarEvaluationInstance::instantiate(prepared.clone()).unwrap();
    instance
        .evaluate(Selection::all(320), &args, &baseline)
        .unwrap();
    let trace = baseline.trace.lock().unwrap().clone();
    assert!(trace.contains(&256));
    for at in 0..trace.len() {
        for cause in failures() {
            let control = Control {
                refusal: Some((at, cause.clone())),
                ..Default::default()
            };
            let mut instance = ScalarEvaluationInstance::instantiate(prepared.clone()).unwrap();
            assert_eq!(
                instance
                    .evaluate(Selection::all(320), &args, &control)
                    .unwrap_err(),
                cause
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            assert_eq!(
                instance
                    .evaluate(Selection::all(320), &args, &control)
                    .unwrap_err(),
                KernelFailure::InstanceFailed
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
        }
    }
}
