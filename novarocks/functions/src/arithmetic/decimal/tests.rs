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
use crate::{KernelDiagnostic, KernelEvaluationControl};
use arrow_array::{ArrayRef, Float64Array, builder::FixedSizeBinaryBuilder};
use novarocks_type_contract::arithmetic_result_value_type_with_op;
use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Default)]
struct CompileControl {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for CompileControl {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        trace.push(units);
        if let Some((at, cause)) = self.refusal.clone()
            && trace.len() == at + 1
        {
            Err(cause)
        } else {
            Ok(())
        }
    }
}
#[derive(Default)]
struct EvaluationControl {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for EvaluationControl {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        trace.push(units);
        if let Some((at, cause)) = &self.refusal
            && trace.len() == *at + 1
        {
            Err(cause.clone())
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("coefficient arithmetic never waits")
    }
}
fn ty(data_type: DataType) -> FunctionValueType {
    FunctionValueType::from_legacy_declared_projection(data_type, true)
}
fn largeint_type() -> FunctionValueType {
    FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        true,
        ValueLogicalType::LargeInt,
    )
    .unwrap()
}
fn prepare_controlled(
    operator: ArithmeticOperator,
    left: &FunctionValueType,
    right: &FunctionValueType,
    result: &FunctionValueType,
    policy: DecimalOverflowPolicy,
    allow: bool,
    control: &CompileControl,
) -> Result<Option<DecimalArithmetic>, ArithmeticPrepareError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
    let outcome = DecimalArithmetic::prepare(
        operator,
        left,
        right,
        (result.data_type(), result.logical_type(), result.nullable()),
        policy,
        allow,
        &mut work,
    );
    if outcome
        .as_ref()
        .err()
        .is_some_and(|e| e.control_error().is_some())
    {
        return outcome;
    }
    work.finish()?;
    outcome
}
fn recipe(
    operator: ArithmeticOperator,
    left: &FunctionValueType,
    right: &FunctionValueType,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> DecimalArithmetic {
    let result = arithmetic_result_value_type_with_op(left, right, operator).unwrap();
    prepare_controlled(
        operator,
        left,
        right,
        &result,
        policy,
        allow,
        &CompileControl::default(),
    )
    .unwrap()
    .unwrap()
}
fn decimal128(value: i128, precision: u8, scale: i8) -> ArrayRef {
    Arc::new(
        Decimal128Array::from(vec![value])
            .with_precision_and_scale(precision, scale)
            .unwrap(),
    )
}
fn decimal256(value: i256, precision: u8, scale: i8) -> ArrayRef {
    Arc::new(
        Decimal256Array::from(vec![value])
            .with_precision_and_scale(precision, scale)
            .unwrap(),
    )
}
fn largeint(value: i128) -> ArrayRef {
    let mut builder = FixedSizeBinaryBuilder::new(16);
    builder.append_value(value.to_be_bytes()).unwrap();
    Arc::new(builder.finish())
}
fn signed(value: i64, ty: &DataType) -> ArrayRef {
    match ty {
        DataType::Int8 => Arc::new(Int8Array::from(vec![i8::try_from(value).unwrap()])),
        DataType::Int16 => Arc::new(Int16Array::from(vec![i16::try_from(value).unwrap()])),
        DataType::Int32 => Arc::new(Int32Array::from(vec![i32::try_from(value).unwrap()])),
        DataType::Int64 => Arc::new(Int64Array::from(vec![value])),
        _ => panic!("fixture must author signed input"),
    }
}
fn evaluate_controlled(
    recipe: &DecimalArithmetic,
    left: &ArrayRef,
    right: &ArrayRef,
    control: &EvaluationControl,
) -> Result<ArithmeticRowResult, KernelFailure> {
    KernelEvaluationControl::checkpoint(control, 0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = recipe.evaluate_non_null(left.as_ref(), 0, right.as_ref(), 0, 9, &mut work);
    work.finish()?;
    result
}
fn evaluate(recipe: &DecimalArithmetic, left: &ArrayRef, right: &ArrayRef) -> ArithmeticRowResult {
    recipe.validate_left(left.as_ref()).unwrap();
    recipe.validate_right(right.as_ref()).unwrap();
    evaluate_controlled(recipe, left, right, &EvaluationControl::default()).unwrap()
}

#[test]
fn exact_decimal128_and_each_signed_width_preserve_all_five_coefficient_algorithms() {
    use ArithmeticOperator::{Add, Divide, Modulo, Multiply, Subtract};
    let decimal_type = ty(DataType::Decimal128(18, 2));
    let decimal = decimal128(700, 18, 2);
    for signed_type in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
    ] {
        let integer_type = ty(signed_type.clone());
        let integer = signed(3, &signed_type);
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            for allow in [false, true] {
                for (op, forward, reverse) in [
                    (Add, 1000, 1000),
                    (Subtract, 400, -400),
                    (Multiply, 2100, 2100),
                    (Divide, 233_333_333, 428_571),
                    (Modulo, 100, 300),
                ] {
                    assert_eq!(
                        evaluate(
                            &recipe(op, &decimal_type, &integer_type, policy, allow),
                            &decimal,
                            &integer
                        ),
                        ArithmeticRowResult::Decimal128(forward)
                    );
                    assert_eq!(
                        evaluate(
                            &recipe(op, &integer_type, &decimal_type, policy, allow),
                            &integer,
                            &decimal
                        ),
                        ArithmeticRowResult::Decimal128(reverse)
                    );
                }
            }
        }
    }
    let other_type = ty(DataType::Decimal128(18, 3));
    let other = decimal128(3000, 18, 3);
    for (op, expected) in [
        (Add, 10_000),
        (Subtract, 4000),
        (Multiply, 2_100_000),
        (Divide, 233_333_333),
        (Modulo, 1000),
    ] {
        assert_eq!(
            evaluate(
                &recipe(
                    op,
                    &decimal_type,
                    &other_type,
                    DecimalOverflowPolicy::OutputNull,
                    false
                ),
                &decimal,
                &other
            ),
            ArithmeticRowResult::Decimal128(expected)
        );
    }
}

#[test]
fn decimal_division_final_half_away_and_negative_scale_alignment_are_exact() {
    for (numerator, denominator, expected) in [
        (1, 128, 7813),
        (-1, 128, -7813),
        (1, -128, -7813),
        (-1, -128, 7813),
        (1, 256, 3906),
    ] {
        let recipe = recipe(
            ArithmeticOperator::Divide,
            &ty(DataType::Decimal128(2, 0)),
            &ty(DataType::Decimal128(3, 0)),
            DecimalOverflowPolicy::ReportError,
            true,
        );
        assert_eq!(
            evaluate(
                &recipe,
                &decimal128(numerator, 2, 0),
                &decimal128(denominator, 3, 0)
            ),
            ArithmeticRowResult::Decimal128(expected)
        );
    }
    let recipe = recipe(
        ArithmeticOperator::Add,
        &ty(DataType::Decimal128(2, -2)),
        &ty(DataType::Decimal128(2, -1)),
        DecimalOverflowPolicy::ReportError,
        false,
    );
    assert_eq!(
        evaluate(&recipe, &decimal128(7, 2, -2), &decimal128(3, 2, -1)),
        ArithmeticRowResult::Decimal128(73)
    );
}

#[test]
fn every_decimal_overflow_policy_is_independent_and_allow_adds_only_multiplication_errors() {
    use ArithmeticOperator::{Add, Divide, Modulo, Multiply, Subtract};
    let max = 10_i128.pow(38) - 1;
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        for allow in [false, true] {
            for (op, left, right, right_scale) in [
                (Add, max, 1, 0),
                (Subtract, -max, 1, 0),
                (Multiply, max, 2, 0),
                (Divide, max, 1, 1),
                (Modulo, max, 1, 1),
            ] {
                let recipe = recipe(
                    op,
                    &ty(DataType::Decimal128(38, 0)),
                    &ty(DataType::Decimal128(2, right_scale)),
                    policy,
                    allow,
                );
                let should_error =
                    policy == DecimalOverflowPolicy::ReportError || (op == Multiply && allow);
                assert_eq!(recipe.own_effects().may_raise_row_error, should_error);
                let value = evaluate(
                    &recipe,
                    &decimal128(left, 38, 0),
                    &decimal128(right, 2, right_scale),
                );
                if should_error {
                    let ArithmeticRowResult::RowError(error) = value else {
                        panic!("numeric overflow disappeared")
                    };
                    assert_eq!(error.selected_ordinal(), 9);
                    assert_eq!(error.reason(), crate::DataFailureReason::NumericOutOfRange);
                    let name = match op {
                        Add => "add",
                        Subtract => "sub",
                        Multiply => "mul",
                        Divide => "div",
                        Modulo => "mod",
                    };
                    assert_eq!(
                        error.message(),
                        format!(
                            "Expr evaluate meet error: The '{name}' operation involving decimal values overflows"
                        )
                    );
                } else {
                    assert_eq!(value, ArithmeticRowResult::Null);
                }
            }
        }
    }
}

#[test]
fn unavailable_scale_factors_are_deferred_and_zero_divisors_precede_numeric_overflow() {
    let tiny = ty(DataType::Decimal128(1, -128));
    let fraction = ty(DataType::Decimal128(38, 38));
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        let add = recipe(
            ArithmeticOperator::Add,
            &tiny,
            &ty(DataType::Int64),
            policy,
            true,
        );
        let result = evaluate(&add, &decimal128(0, 1, -128), &signed(0, &DataType::Int64));
        if policy == DecimalOverflowPolicy::ReportError {
            assert!(matches!(result, ArithmeticRowResult::RowError(_)));
        } else {
            assert_eq!(result, ArithmeticRowResult::Null);
        }
        let divide = recipe(ArithmeticOperator::Divide, &fraction, &tiny, policy, true);
        assert_eq!(
            evaluate(&divide, &decimal128(1, 38, 38), &decimal128(0, 1, -128)),
            ArithmeticRowResult::Null
        );
        let modulo = recipe(ArithmeticOperator::Modulo, &tiny, &fraction, policy, true);
        assert_eq!(
            evaluate(&modulo, &decimal128(1, 1, -128), &decimal128(0, 38, 38)),
            ArithmeticRowResult::Null
        );
    }
}

#[test]
fn mixed_largeint_and_decimal_wide_add_sub_preserve_coefficients_and_raw_column_contract() {
    use ArithmeticOperator::{Add, Subtract};
    let integer_type = largeint_type();
    for integer in [i128::MIN, i128::MAX] {
        for scale in [15, 36, -36] {
            let decimal_type = ty(DataType::Decimal128(38, scale));
            let decimal = decimal128(7, 38, scale);
            let integer_array = largeint(integer);
            let factor = 10_i128.pow(scale.unsigned_abs() as u32);
            let decimal_value = if scale < 0 {
                i256::from_i128(7)
                    .checked_mul(i256::from_i128(factor))
                    .unwrap()
            } else {
                i256::from_i128(7)
            };
            let integer_value = if scale > 0 {
                i256::from_i128(integer)
                    .checked_mul(i256::from_i128(factor))
                    .unwrap()
            } else {
                i256::from_i128(integer)
            };
            for op in [Add, Subtract] {
                let forward = if op == Add {
                    decimal_value.checked_add(integer_value)
                } else {
                    decimal_value.checked_sub(integer_value)
                }
                .unwrap();
                let reverse = if op == Add {
                    integer_value.checked_add(decimal_value)
                } else {
                    integer_value.checked_sub(decimal_value)
                }
                .unwrap();
                assert_eq!(
                    evaluate(
                        &recipe(
                            op,
                            &decimal_type,
                            &integer_type,
                            DecimalOverflowPolicy::ReportError,
                            true
                        ),
                        &decimal,
                        &integer_array
                    ),
                    ArithmeticRowResult::Decimal256(forward)
                );
                assert_eq!(
                    evaluate(
                        &recipe(
                            op,
                            &integer_type,
                            &decimal_type,
                            DecimalOverflowPolicy::ReportError,
                            true
                        ),
                        &integer_array,
                        &decimal
                    ),
                    ArithmeticRowResult::Decimal256(reverse)
                );
            }
        }
    }
    let decimal_type = ty(DataType::Decimal256(55, 15));
    let wide = i256::from_string("200000000000000000000000000000000000000").unwrap();
    assert!(wide.to_i128().is_none());
    assert_eq!(
        evaluate(
            &recipe(
                Add,
                &decimal_type,
                &integer_type,
                DecimalOverflowPolicy::ReportError,
                false
            ),
            &decimal256(wide, 55, 15),
            &largeint(1)
        ),
        ArithmeticRowResult::Decimal256(
            wide.checked_add(i256::from_i128(1_000_000_000_000_000))
                .unwrap()
        )
    );
    // Column coefficients outside declared precision remain actual raw inputs;
    // output numeric overflow follows the selected policy, never input rejection.
    let raw_type = ty(DataType::Decimal256(2, 0));
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        let recipe = recipe(Add, &raw_type, &integer_type, policy, true);
        assert_eq!(
            recipe.own_effects().may_raise_row_error,
            policy == DecimalOverflowPolicy::ReportError
        );
        let result = evaluate(&recipe, &decimal256(i256::MAX, 2, 0), &largeint(0));
        if policy == DecimalOverflowPolicy::ReportError {
            assert!(matches!(result, ArithmeticRowResult::RowError(_)));
        } else {
            assert_eq!(result, ArithmeticRowResult::Null);
        }
    }
}

#[test]
fn decimal_recipe_rejects_unlicensed_pairs_and_foreign_standard_classes() {
    let integer = largeint_type();
    let d128 = ty(DataType::Decimal128(18, 2));
    let d256 = ty(DataType::Decimal256(55, 15));
    let result = ty(DataType::Decimal256(56, 15));
    for op in [
        ArithmeticOperator::Multiply,
        ArithmeticOperator::Divide,
        ArithmeticOperator::Modulo,
    ] {
        assert!(
            prepare_controlled(
                op,
                &d256,
                &integer,
                &result,
                DecimalOverflowPolicy::ReportError,
                false,
                &CompileControl::default()
            )
            .is_err()
        );
    }
    for left in [
        ty(DataType::FixedSizeBinary(16)),
        FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            true,
            ValueLogicalType::Uuid,
        )
        .unwrap(),
        ty(DataType::UInt64),
        d256.clone(),
    ] {
        assert!(
            prepare_controlled(
                ArithmeticOperator::Add,
                &left,
                &d128,
                &ty(DataType::Decimal128(38, 2)),
                DecimalOverflowPolicy::OutputNull,
                false,
                &CompileControl::default()
            )
            .is_err()
        );
    }
    let ordinary = recipe(
        ArithmeticOperator::Add,
        &d128,
        &ty(DataType::Int64),
        DecimalOverflowPolicy::OutputNull,
        false,
    );
    assert!(matches!(
        ordinary.validate_left(&Float64Array::from(vec![1.0])),
        Err(KernelFailure::Internal(_))
    ));
    assert!(matches!(
        ordinary.validate_right(&Int32Array::from(vec![1])),
        Err(KernelFailure::Internal(_))
    ));
    let mut wrong =
        arithmetic_result_value_type_with_op(&d128, &ty(DataType::Int64), ArithmeticOperator::Add)
            .unwrap();
    wrong = wrong
        .with_nullable(false)
        .expect("valid function nullability declaration");
    assert!(
        prepare_controlled(
            ArithmeticOperator::Add,
            &d128,
            &ty(DataType::Int64),
            &wrong,
            DecimalOverflowPolicy::OutputNull,
            false,
            &CompileControl::default()
        )
        .is_err()
    );
    assert!(
        prepare_controlled(
            ArithmeticOperator::Add,
            &ty(DataType::Int64),
            &ty(DataType::Int64),
            &ty(DataType::Int64),
            DecimalOverflowPolicy::OutputNull,
            false,
            &CompileControl::default()
        )
        .unwrap()
        .is_none()
    );
}

#[test]
fn original_prepare_and_row_controls_keep_all_primary_prefixes_including_ordinary_error_tail() {
    let left = ty(DataType::Decimal128(1, -128));
    let right = ty(DataType::Int64);
    let result =
        arithmetic_result_value_type_with_op(&left, &right, ArithmeticOperator::Add).unwrap();
    for nullable in [true, false] {
        let mut result = result.clone();
        result = result
            .with_nullable(nullable)
            .expect("valid function nullability declaration");
        let control = CompileControl::default();
        let _ = prepare_controlled(
            ArithmeticOperator::Add,
            &left,
            &right,
            &result,
            DecimalOverflowPolicy::OutputNull,
            false,
            &control,
        );
        let trace = control.trace.into_inner().unwrap();
        assert_eq!(trace[0], 0);
        assert!(trace.last().is_some_and(|units| *units > 0));
        for at in 0..trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = CompileControl {
                    refusal: Some((at, cause.clone())),
                    ..CompileControl::default()
                };
                assert_eq!(
                    prepare_controlled(
                        ArithmeticOperator::Add,
                        &left,
                        &right,
                        &result,
                        DecimalOverflowPolicy::OutputNull,
                        false,
                        &control
                    )
                    .unwrap_err()
                    .control_error(),
                    Some(cause)
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
    let recipe = recipe(
        ArithmeticOperator::Add,
        &ty(DataType::Decimal128(18, 0)),
        &ty(DataType::Int64),
        DecimalOverflowPolicy::OutputNull,
        false,
    );
    for left in [
        decimal128(7, 18, 0),
        Arc::new(Float64Array::from(vec![7.0])) as ArrayRef,
    ] {
        let right = signed(3, &DataType::Int64);
        let control = EvaluationControl::default();
        let _ = evaluate_controlled(&recipe, &left, &right, &control);
        let trace = control.trace.into_inner().unwrap();
        assert!(trace.last().is_some_and(|units| *units > 0));
        for at in 0..trace.len() {
            for cause in [
                KernelFailure::Cancelled,
                KernelFailure::DeadlineExceeded,
                KernelFailure::ResourceExhausted,
                KernelFailure::InvalidProgram(KernelDiagnostic::new("original invalid refusal")),
                KernelFailure::InstanceFailed,
                KernelFailure::Operational(KernelDiagnostic::new("original operational refusal")),
                KernelFailure::Internal(KernelDiagnostic::new("original internal refusal")),
            ] {
                let control = EvaluationControl {
                    refusal: Some((at, cause.clone())),
                    ..EvaluationControl::default()
                };
                assert_eq!(
                    evaluate_controlled(&recipe, &left, &right, &control),
                    Err(cause)
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
}
