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

//! Independent expected values for migrated scalar cast families.
use super::*;
use std::time::Duration;

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("CAST does not wait")
    }
}
fn recipe(
    source: DataType,
    target: DataType,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> PreparedCastRecipe {
    PreparedCastRecipe::try_new(
        CastOperation::Carrier,
        &FunctionValueType::new(source, true),
        &FunctionValueType::new(target, true),
        policy,
        allow,
        &Control,
    )
    .unwrap()
}
fn modes() -> [(DecimalOverflowPolicy, bool); 4] {
    [
        (DecimalOverflowPolicy::OutputNull, false),
        (DecimalOverflowPolicy::OutputNull, true),
        (DecimalOverflowPolicy::ReportError, false),
        (DecimalOverflowPolicy::ReportError, true),
    ]
}
fn check(input: ArrayRef, target: DataType, expected: &[CastRowResult]) {
    for (policy, allow) in modes() {
        let r = recipe(input.data_type().clone(), target.clone(), policy, allow);
        for (row, expected) in expected.iter().enumerate() {
            assert_eq!(
                &r.evaluate_row(EvaluatedArgument::Column(&input), row, row, &Control)
                    .unwrap(),
                expected
            );
        }
    }
}
#[test]
fn decimal_integer_truncates_toward_zero_and_retains_actual_epoch_days() {
    for target in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
    ] {
        let input = Arc::new(
            Decimal128Array::from(vec![Some(12799), Some(-12799), None])
                .with_precision_and_scale(8, 2)
                .unwrap(),
        ) as ArrayRef;
        check(
            input,
            target.clone(),
            &[
                CastRowResult::Signed(127),
                CastRowResult::Signed(-127),
                CastRowResult::Null,
            ],
        );
        let input = Arc::new(
            Decimal256Array::from(vec![
                Some(i256::from_i128(12799)),
                Some(i256::from_i128(-12799)),
                None,
            ])
            .with_precision_and_scale(76, 2)
            .unwrap(),
        ) as ArrayRef;
        check(
            input,
            target,
            &[
                CastRowResult::Signed(127),
                CastRowResult::Signed(-127),
                CastRowResult::Null,
            ],
        );
    }
    let input = Arc::new(Date32Array::from(vec![
        Some(0),
        Some(-1),
        Some(20000),
        None,
    ])) as ArrayRef;
    for target in [DataType::Int32, DataType::Int64] {
        check(
            Arc::clone(&input),
            target,
            &[
                CastRowResult::Signed(0),
                CastRowResult::Signed(-1),
                CastRowResult::Signed(20000),
                CastRowResult::Null,
            ],
        );
    }
    for target in [DataType::Int8, DataType::Int16] {
        assert_eq!(
            PreparedCastRecipe::try_new(
                CastOperation::Carrier,
                &FunctionValueType::new(DataType::Date32, true),
                &FunctionValueType::new(target, true),
                DecimalOverflowPolicy::OutputNull,
                false,
                &Control
            ),
            Err(CastPrepareError::Unsupported)
        );
    }
}
#[test]
fn decimal_integer_overflow_is_policy_bound_not_float_allow_bound() {
    for wide in [false, true] {
        let input: ArrayRef = if wide {
            Arc::new(
                Decimal256Array::from(vec![i256::from_i128(128), i256::from_i128(-129)])
                    .with_precision_and_scale(76, 0)
                    .unwrap(),
            )
        } else {
            Arc::new(
                Decimal128Array::from(vec![128, -129])
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            )
        };
        for (policy, allow) in modes() {
            let r = recipe(input.data_type().clone(), DataType::Int8, policy, allow);
            for row in 0..2 {
                let out = r
                    .evaluate_row(EvaluatedArgument::Column(&input), row, row, &Control)
                    .unwrap();
                if policy == DecimalOverflowPolicy::ReportError {
                    assert!(matches!(out, CastRowResult::RowError(_)));
                } else {
                    assert_eq!(out, CastRowResult::Null);
                }
            }
        }
    }
    let input = Arc::new(
        Decimal128Array::from(vec![12, -12])
            .with_precision_and_scale(8, -2)
            .unwrap(),
    ) as ArrayRef;
    check(
        input,
        DataType::Int64,
        &[CastRowResult::Signed(1200), CastRowResult::Signed(-1200)],
    );
}
#[test]
fn text_parsers_preserve_invalid_null_and_decimal_rounding() {
    let input = Arc::new(StringArray::from(vec![
        Some("1.25"),
        Some("-2.5"),
        Some("bad"),
        Some("NaN"),
        Some("inf"),
        None,
    ])) as ArrayRef;
    check(
        Arc::clone(&input),
        DataType::Float32,
        &[
            CastRowResult::Float32(1.25),
            CastRowResult::Float32(-2.5),
            CastRowResult::Null,
            CastRowResult::Null,
            CastRowResult::Null,
            CastRowResult::Null,
        ],
    );
    check(
        input,
        DataType::Float64,
        &[
            CastRowResult::Float64(1.25),
            CastRowResult::Float64(-2.5),
            CastRowResult::Null,
            CastRowResult::Null,
            CastRowResult::Null,
            CastRowResult::Null,
        ],
    );
    let input = Arc::new(StringArray::from(vec![
        Some("1.25"),
        Some("-1.25"),
        Some(""),
        Some("  "),
        Some("bad"),
        None,
    ])) as ArrayRef;
    check(
        input,
        DataType::Decimal128(8, 1),
        &[
            CastRowResult::Decimal128(13),
            CastRowResult::Decimal128(-13),
            CastRowResult::Null,
            CastRowResult::Null,
            CastRowResult::Null,
            CastRowResult::Null,
        ],
    );
}
#[test]
fn float32_decimal_uses_the_existing_float_author() {
    let input = Arc::new(Float32Array::from(vec![
        Some(1.25),
        Some(-1.25),
        Some(f32::INFINITY),
        None,
    ])) as ArrayRef;
    check(
        input,
        DataType::Decimal128(8, 1),
        &[
            CastRowResult::Decimal128(13),
            CastRowResult::Decimal128(-13),
            CastRowResult::Null,
            CastRowResult::Null,
        ],
    );
}
#[test]
fn decimal256_narrows_with_declared_precision_and_scale() {
    let input = Arc::new(
        Decimal256Array::from(vec![
            Some(i256::from_i128(125)),
            Some(i256::from_i128(-125)),
            None,
        ])
        .with_precision_and_scale(76, 2)
        .unwrap(),
    ) as ArrayRef;
    check(
        input,
        DataType::Decimal128(8, 1),
        &[
            CastRowResult::Decimal128(13),
            CastRowResult::Decimal128(-13),
            CastRowResult::Null,
        ],
    );
    let input = Arc::new(
        Decimal256Array::from(vec![i256::from_i128(1000)])
            .with_precision_and_scale(76, 0)
            .unwrap(),
    ) as ArrayRef;
    for (policy, allow) in modes() {
        let r = recipe(
            input.data_type().clone(),
            DataType::Decimal128(3, 0),
            policy,
            allow,
        );
        let out = r
            .evaluate_row(EvaluatedArgument::Column(&input), 0, 0, &Control)
            .unwrap();
        if policy == DecimalOverflowPolicy::ReportError || allow {
            assert!(matches!(out, CastRowResult::RowError(_)));
        } else {
            assert_eq!(out, CastRowResult::Null);
        }
    }
}
#[test]
fn boolean_decimal_and_decimal256_truth_value_are_independent_of_scale() {
    let input = Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])) as ArrayRef;
    check(
        Arc::clone(&input),
        DataType::Decimal128(8, 2),
        &[
            CastRowResult::Decimal128(100),
            CastRowResult::Decimal128(0),
            CastRowResult::Null,
        ],
    );
    check(
        input,
        DataType::Decimal128(8, -2),
        &[
            CastRowResult::Decimal128(0),
            CastRowResult::Decimal128(0),
            CastRowResult::Null,
        ],
    );
    let input = Arc::new(
        Decimal256Array::from(vec![
            Some(i256::from_i128(1)),
            Some(i256::ZERO),
            Some(i256::from_i128(-1)),
            None,
        ])
        .with_precision_and_scale(76, 20)
        .unwrap(),
    ) as ArrayRef;
    check(
        input,
        DataType::Boolean,
        &[
            CastRowResult::Boolean(true),
            CastRowResult::Boolean(false),
            CastRowResult::Boolean(true),
            CastRowResult::Null,
        ],
    );
}
#[test]
fn extended_recipes_do_not_authorize_nominal_domain_changes() {
    let source =
        FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
            .unwrap();
    let target = FunctionValueType::new(DataType::Decimal128(8, 1), true);
    assert_eq!(
        PreparedCastRecipe::try_new(
            CastOperation::Carrier,
            &source,
            &target,
            DecimalOverflowPolicy::OutputNull,
            false,
            &Control
        ),
        Err(CastPrepareError::Unsupported)
    );
}

#[test]
fn decimal_calendar_reuses_numeric_calendar_grammar() {
    let input = Arc::new(
        Decimal128Array::from(vec![Some(202001019), Some(999999999), None])
            .with_precision_and_scale(18, 1)
            .unwrap(),
    ) as ArrayRef;
    check(
        input,
        DataType::Date32,
        &[
            CastRowResult::Signed(18262),
            CastRowResult::Null,
            CastRowResult::Null,
        ],
    );

    // Decimal coefficients are first truncated to compact numeric calendar
    // literals. 1969-12-31 23:59:59 is -1 epoch second; leap-day
    // 2024-02-29 12:34:56 is exactly 1_709_210_096 epoch seconds.
    let decimal = Arc::new(
        Decimal128Array::from(vec![Some(196912312359599), Some(202402291234569), None])
            .with_precision_and_scale(18, 1)
            .unwrap(),
    ) as ArrayRef;
    for (unit, before_epoch, leap_day) in [
        (TimeUnit::Second, -1, 1_709_210_096),
        (TimeUnit::Millisecond, -1_000, 1_709_210_096_000),
        (TimeUnit::Microsecond, -1_000_000, 1_709_210_096_000_000),
        (
            TimeUnit::Nanosecond,
            -1_000_000_000,
            1_709_210_096_000_000_000,
        ),
    ] {
        check(
            decimal.clone(),
            DataType::Timestamp(unit, None),
            &[
                CastRowResult::Timestamp(before_epoch),
                CastRowResult::Timestamp(leap_day),
                CastRowResult::Null,
            ],
        );
    }
    // Negative decimal scale multiplies the coefficient, rather than treating
    // it as an epoch count. The literal ends in :00, exactly 56 seconds earlier.
    check(
        Arc::new(
            Decimal128Array::from(vec![Some(202402291234), None])
                .with_precision_and_scale(16, -2)
                .unwrap(),
        ),
        DataType::Timestamp(TimeUnit::Microsecond, None),
        &[
            CastRowResult::Timestamp(1_709_210_040_000_000),
            CastRowResult::Null,
        ],
    );
    let fixed =
        crate::largeint::array_from_i128(&[Some(20200101), Some(i64::MAX as i128 + 1), None])
            .unwrap();
    check(
        fixed,
        DataType::Date32,
        &[
            CastRowResult::Signed(18_262),
            CastRowResult::Null,
            CastRowResult::Null,
        ],
    );
    // Year 9999 is a valid numeric calendar date and fits epoch microseconds,
    // but 253_402_300_799 seconds cannot fit an i64 nanosecond carrier.
    let overflow_sources = [
        Arc::new(
            Decimal128Array::from(vec![Some(0), Some(99991231235959), None])
                .with_precision_and_scale(18, 0)
                .unwrap(),
        ) as ArrayRef,
        crate::largeint::array_from_i128(&[Some(0), Some(99991231235959), None]).unwrap(),
    ];
    for input in overflow_sources {
        for (policy, allow) in modes() {
            let r = recipe(
                input.data_type().clone(),
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                policy,
                allow,
            );
            let error = r
                .evaluate_row(EvaluatedArgument::Column(&input), 0, 1, &Control)
                .unwrap();
            let CastRowResult::RowError(error) = error else {
                panic!("valid out-of-range calendar must report its nanosecond row error")
            };
            assert_eq!(error.selected_ordinal(), 0);
            assert_eq!(
                error.message(),
                "CAST failed: numeric datetime literal is out of nanosecond i64 range"
            );
            assert_eq!(
                r.evaluate_row(EvaluatedArgument::Column(&input), 1, 2, &Control)
                    .unwrap(),
                CastRowResult::Null
            );
        }
    }
}

// P05 qualification: exact target limits follow two's-complement integer
// ranges. These constants are not observations of an evaluator.
#[test]
fn p05_decimal_signed_width_endpoints_and_overflow_all_modes() {
    use novarocks_failure_contract::FailureProjection;
    for (target, min, max) in [
        (DataType::Int8, i8::MIN as i128, i8::MAX as i128),
        (DataType::Int16, i16::MIN as i128, i16::MAX as i128),
        (DataType::Int32, i32::MIN as i128, i32::MAX as i128),
        (DataType::Int64, i64::MIN as i128, i64::MAX as i128),
    ] {
        let coefficients = vec![Some(min), Some(max), Some(min - 1), Some(max + 1), None];
        let narrow = Arc::new(
            Decimal128Array::from(coefficients.clone())
                .with_precision_and_scale(38, 0)
                .unwrap(),
        ) as ArrayRef;
        let wide = Arc::new(
            Decimal256Array::from(
                coefficients
                    .iter()
                    .map(|value| value.map(i256::from_i128))
                    .collect::<Vec<_>>(),
            )
            .with_precision_and_scale(76, 0)
            .unwrap(),
        ) as ArrayRef;
        for input in [narrow, wide] {
            for (policy, allow) in modes() {
                let r = recipe(input.data_type().clone(), target.clone(), policy, allow);
                for row in 0..5 {
                    let actual = r
                        .evaluate_row(EvaluatedArgument::Column(&input), row, row, &Control)
                        .unwrap();
                    match row {
                        0 => assert_eq!(actual, CastRowResult::Signed(min as i64)),
                        1 => assert_eq!(actual, CastRowResult::Signed(max as i64)),
                        2 | 3 if policy == DecimalOverflowPolicy::ReportError => {
                            let CastRowResult::RowError(error) = actual else {
                                panic!("finite range overflow is a row error")
                            };
                            assert_eq!(error.selected_ordinal(), row);
                            assert_eq!(
                                error.failure_fact().reason(),
                                crate::DataFailureReason::NumericOutOfRange
                                    .failure_fact()
                                    .reason()
                            );
                        }
                        _ => assert_eq!(actual, CastRowResult::Null),
                    }
                }
            }
        }
    }
}

#[test]
fn p05_float32_precision_policy_and_date_epoch_extrema_are_independent() {
    let input = Arc::new(Float32Array::from(vec![
        Some(12.0),
        Some(1000.0),
        Some(f32::NAN),
        Some(f32::INFINITY),
        None,
    ])) as ArrayRef;
    for (policy, allow) in modes() {
        let r = recipe(DataType::Float32, DataType::Decimal128(3, 0), policy, allow);
        assert_eq!(
            r.evaluate_row(EvaluatedArgument::Column(&input), 0, 0, &Control)
                .unwrap(),
            CastRowResult::Decimal128(12)
        );
        let overflow = r
            .evaluate_row(EvaluatedArgument::Column(&input), 1, 1, &Control)
            .unwrap();
        if policy == DecimalOverflowPolicy::ReportError {
            let CastRowResult::RowError(error) = overflow else {
                panic!("finite decimal precision overflow must report")
            };
            assert_eq!(error.selected_ordinal(), 1);
        } else {
            assert_eq!(overflow, CastRowResult::Null);
        }
        for row in 2..5 {
            assert_eq!(
                r.evaluate_row(EvaluatedArgument::Column(&input), row, row, &Control)
                    .unwrap(),
                CastRowResult::Null
            );
        }
    }
    // DATE uses signed epoch days; it is not a yyyyMMdd conversion.
    let input = Arc::new(Date32Array::from(vec![
        Some(i32::MIN),
        Some(i32::MAX),
        None,
    ])) as ArrayRef;
    for target in [DataType::Int32, DataType::Int64] {
        check(
            input.clone(),
            target,
            &[
                CastRowResult::Signed(i32::MIN as i64),
                CastRowResult::Signed(i32::MAX as i64),
                CastRowResult::Null,
            ],
        );
    }
}

#[test]
fn p05_extended_preparation_types_nullability_and_effects_follow_each_semantic_branch() {
    use novarocks_type_contract::{EvaluationDemand, EvaluationDomainId, ExpressionUseId};
    let context = ExpressionEffectContext {
        use_id: ExpressionUseId::new(1),
        domain: EvaluationDomainId::new(0),
        demand: EvaluationDemand::Value,
    };
    // A tag specifies the independently stated successful-NULL/error contract,
    // not a recipe body's private enum or a runtime-computed expected value.
    for (source, target, contract) in [
        (DataType::Decimal128(8, 2), DataType::Int16, "numeric"),
        (DataType::Decimal256(40, 2), DataType::Int16, "numeric"),
        (DataType::Utf8, DataType::Float64, "text_float"),
        (DataType::Utf8, DataType::Decimal128(8, 1), "text_decimal"),
        (
            DataType::Decimal256(40, 2),
            DataType::Decimal128(8, 1),
            "narrow",
        ),
        (DataType::Date32, DataType::Int64, "exact"),
        (
            DataType::Boolean,
            DataType::Decimal128(8, 2),
            "boolean_decimal",
        ),
        (DataType::Decimal256(40, 2), DataType::Boolean, "exact"),
        (
            DataType::Float32,
            DataType::Decimal128(8, 1),
            "float_decimal",
        ),
    ] {
        for (policy, allow) in modes() {
            let null_possible = match contract {
                "numeric" | "boolean_decimal" => policy == DecimalOverflowPolicy::OutputNull,
                "narrow" => policy == DecimalOverflowPolicy::OutputNull && !allow,
                "exact" => false,
                _ => true,
            };
            let may_raise = match contract {
                "numeric" => policy == DecimalOverflowPolicy::ReportError,
                "text_float" | "exact" => false,
                _ => true,
            };
            for source_nullable in [false, true] {
                for target_nullable in [false, true] {
                    let from = FunctionValueType::new(source.clone(), source_nullable);
                    let to = FunctionValueType::new(target.clone(), target_nullable);
                    let actual = PreparedCastRecipe::try_new(
                        CastOperation::Carrier,
                        &from,
                        &to,
                        policy,
                        allow,
                        &Control,
                    );
                    if !target_nullable && (source_nullable || null_possible) {
                        assert_eq!(actual, Err(CastPrepareError::TypeMismatch));
                    } else {
                        let r = actual.unwrap();
                        assert_eq!(r.source_type(), &from);
                        assert_eq!(r.result_type(), &to);
                        assert_eq!(r.policy(), policy);
                        assert_eq!(r.allow_throw_exception(), allow);
                        assert_eq!(
                            r.own_effects(context)
                                .for_use(context)
                                .unwrap()
                                .may_raise_row_error,
                            may_raise
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn utf8_raw_float_retains_strict_text_and_non_finite_null_policy() {
    let input = Arc::new(StringArray::from(vec![
        Some("7"),
        Some(" 7"),
        Some("7 "),
        Some("\u{a0}7"),
        Some("7\u{2003}"),
        Some("NaN"),
        Some("1e999"),
        None,
    ])) as ArrayRef;
    check(
        input.clone(),
        DataType::Float64,
        &[
            CastRowResult::Float64(7.0),
            CastRowResult::Null,
            CastRowResult::Null,
            CastRowResult::Null,
            CastRowResult::Null,
            CastRowResult::Null,
            CastRowResult::Null,
            CastRowResult::Null,
        ],
    );
    check(
        input,
        DataType::Float32,
        &[
            CastRowResult::Float32(7.0),
            CastRowResult::Null,
            CastRowResult::Null,
            CastRowResult::Null,
            CastRowResult::Null,
            CastRowResult::Null,
            CastRowResult::Null,
            CastRowResult::Null,
        ],
    );
}

#[test]
fn utf8_decimal_retains_its_existing_blank_normalization_and_unicode_trim() {
    let input = Arc::new(StringArray::from(vec![
        Some("\u{a0}1.25\u{2003}"),
        Some("-1.255"),
        Some("1.255"),
        Some("1e2"),
        Some(""),
        Some("\u{3000}"),
        None,
    ])) as ArrayRef;
    check(
        input,
        DataType::Decimal128(5, 2),
        &[
            CastRowResult::Decimal128(125),
            CastRowResult::Decimal128(-126),
            CastRowResult::Decimal128(126),
            CastRowResult::Null,
            CastRowResult::Null,
            CastRowResult::Null,
            CastRowResult::Null,
        ],
    );
}
