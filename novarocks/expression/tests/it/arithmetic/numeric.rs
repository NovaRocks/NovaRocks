// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

// Actual full FVT source facts, compiler recipes, and selected runtime. This
// fixture never derives LargeInt identity from its fixed-width byte carrier.
use super::*;
use arrow::array::{Decimal128Array, Decimal256Array, FixedSizeBinaryBuilder};
use arrow::datatypes::i256;
use novarocks_expression::testkit::PassThroughCallHost;
use novarocks_expression::testkit::checked_constants;
use novarocks_expression::testkit::numeric_fixtures::{
    fixture_constant_policy, signed, typed_program,
};
use novarocks_expression::{EvaluationInput, ExprEvaluationFailure};
use novarocks_functions::PreparedArithmeticRecipe;
use novarocks_functions::{ScalarInvocationActivation, ScalarInvocationScope};
use novarocks_type_contract::ValueLogicalType;
use std::str::FromStr;

fn decimal(precision: u8, scale: i8) -> FunctionValueType {
    FunctionValueType::new(DataType::Decimal128(precision, scale), true)
}
fn largeint() -> FunctionValueType {
    FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        true,
        ValueLogicalType::LargeInt,
    )
    .unwrap()
}
fn d128(ty: &FunctionValueType, values: &[Option<i128>]) -> ArrayRef {
    let DataType::Decimal128(p, s) = ty.data_type() else {
        panic!("exact Decimal128 source")
    };
    // Arrow deliberately accepts coefficients outside declared input precision.
    // Production arithmetic checks its result, not an invented input bound.
    Arc::new(
        Decimal128Array::from(values.to_vec())
            .with_precision_and_scale(*p, *s)
            .unwrap(),
    )
}
fn d256(ty: &FunctionValueType, values: &[Option<i256>]) -> ArrayRef {
    let DataType::Decimal256(p, s) = ty.data_type() else {
        panic!("exact Decimal256 source")
    };
    Arc::new(
        Decimal256Array::from(values.to_vec())
            .with_precision_and_scale(*p, *s)
            .unwrap(),
    )
}
fn large(values: &[Option<i128>]) -> ArrayRef {
    let mut builder = FixedSizeBinaryBuilder::with_capacity(values.len(), 16);
    for value in values {
        if let Some(value) = value {
            builder.append_value(value.to_be_bytes()).unwrap();
        } else {
            builder.append_null();
        }
    }
    Arc::new(builder.finish())
}
fn typed_input(
    program: &CompiledFixture,
    left: ArrayRef,
    right: ArrayRef,
    flags: &[Option<bool>],
) -> RecordBatch {
    RecordBatch::try_new(
        program.input_schema().clone(),
        vec![left, right, Arc::new(BooleanArray::from(flags.to_vec()))],
    )
    .unwrap()
}
fn coefficients(output: &novarocks_functions::SelectedValues<'_>) -> Vec<Option<i128>> {
    output
        .values()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap()
        .iter()
        .collect()
}
fn wide_coefficients(output: &novarocks_functions::SelectedValues<'_>) -> Vec<Option<i256>> {
    output
        .values()
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .unwrap()
        .iter()
        .collect()
}
fn large_values(output: &novarocks_functions::SelectedValues<'_>) -> Vec<Option<i128>> {
    output
        .values()
        .as_any()
        .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
        .unwrap()
        .iter()
        .map(|value| value.map(|bytes| i128::from_be_bytes(bytes.try_into().unwrap())))
        .collect()
}
fn exact_recipe(program: &CompiledFixture) -> &PreparedArithmeticRecipe {
    let unit = &program.unit;
    unit.recipes()
        .arithmetic(unit.root(program.root).unwrap().use_id)
        .unwrap()
}

#[test]
fn decimal_and_signed_five_operators_preserve_exact_scale_and_sparse_coefficients() {
    let left = decimal(6, 2);
    for (right, right_array, expectations) in [
        (
            decimal(4, 1),
            d128(&decimal(4, 1), &[None, Some(25), Some(25), Some(0)]),
            [12_595, 12_095, 308_625, 4_938_000_000, 95],
        ),
        (
            FunctionValueType::new(DataType::Int64, true),
            Arc::new(Int64Array::from(vec![None, Some(2), Some(2), Some(0)])) as ArrayRef,
            [12_545, 12_145, 24_690, 6_172_500_000, 145],
        ),
    ] {
        for (index, op) in [
            BinaryOperator::Add,
            BinaryOperator::Subtract,
            BinaryOperator::Multiply,
            BinaryOperator::Divide,
            BinaryOperator::Modulo,
        ]
        .into_iter()
        .enumerate()
        {
            let program = typed_program(
                op,
                left.clone(),
                right.clone(),
                Wrap::Bare,
                false,
                DecimalOverflowPolicy::ReportError,
                None,
            );
            let batch = typed_input(
                &program,
                d128(&left, &[Some(999_999), Some(12_345), Some(-12_345), None]),
                right_array.clone(),
                &[None; 4],
            );
            let rows = [1, 2, 3];
            let selected = Selection::try_sparse(4, &rows).unwrap();
            let output = instance(&program)
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &batch,
                        source: None,
                    },
                    ScalarInvocationScope::new(selected, selected),
                    ScalarInvocationActivation::Activated,
                    &Control,
                    &mut PassThroughCallHost,
                )
                .unwrap();
            let negative = match (op, &right.data_type()) {
                (BinaryOperator::Add, DataType::Decimal128(..)) => -12_095,
                (BinaryOperator::Subtract, DataType::Decimal128(..)) => -12_595,
                (BinaryOperator::Add, _) => -12_145,
                (BinaryOperator::Subtract, _) => -12_545,
                _ => -expectations[index],
            };
            assert_eq!(
                coefficients(&output),
                vec![Some(expectations[index]), Some(negative), None]
            );
            assert_eq!(output.selection(), selected);
            assert!(output.errors().is_empty());
            let recipe = exact_recipe(&program);
            assert_eq!(recipe.left_type(), &left);
            assert_eq!(recipe.right_type(), &right);
            assert_eq!(
                output.values().data_type().clone(),
                recipe.result_type().data_type().clone()
            );
            assert_eq!(
                recipe.decimal_overflow_policy(),
                DecimalOverflowPolicy::ReportError
            );
            assert!(!recipe.allow_throw_exception());
        }
    }
    // The same exact integral source port accepts all four signed widths.
    for carrier in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
    ] {
        let right = FunctionValueType::new(carrier.clone(), true);
        for (op, expected) in [
            (BinaryOperator::Add, 12_545),
            (BinaryOperator::Subtract, 12_145),
            (BinaryOperator::Multiply, 24_690),
            (BinaryOperator::Divide, 6_172_500_000),
            (BinaryOperator::Modulo, 145),
        ] {
            let program = typed_program(
                op,
                left.clone(),
                right.clone(),
                Wrap::Bare,
                true,
                DecimalOverflowPolicy::OutputNull,
                None,
            );
            let batch = typed_input(
                &program,
                d128(&left, &[Some(12_345)]),
                signed(&carrier, &[Some(2)]),
                &[None],
            );
            let output = instance(&program)
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &batch,
                        source: None,
                    },
                    ScalarInvocationScope::new(Selection::all(1), Selection::all(1)),
                    ScalarInvocationActivation::Activated,
                    &Control,
                    &mut PassThroughCallHost,
                )
                .unwrap();
            assert_eq!(coefficients(&output), vec![Some(expected)]);
            assert!(output.errors().is_empty());
        }
    }
}

#[test]
fn decimal_precision_factor_and_mul_overflow_keep_independent_policies_and_null_precedence() {
    let maximum = 10_i128.pow(38) - 1;
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        for allow in [false, true] {
            for (op, left, right, lvalues, rvalues, expected, raises) in [
                // The first coefficient is outside its input precision; only
                // the result's p=2 bound fails. It is not an input rejection.
                (
                    BinaryOperator::Add,
                    decimal(1, 0),
                    decimal(1, 0),
                    vec![Some(99), None, Some(1)],
                    vec![Some(1), Some(0), Some(1)],
                    vec![None, None, Some(2)],
                    policy == DecimalOverflowPolicy::ReportError,
                ),
                (
                    BinaryOperator::Multiply,
                    decimal(38, 0),
                    decimal(1, 0),
                    vec![Some(maximum), None, Some(1)],
                    vec![Some(2), Some(0), Some(2)],
                    vec![None, None, Some(2)],
                    policy == DecimalOverflowPolicy::ReportError || allow,
                ),
                // Both source scales are legal. Aligning -38 to the shared
                // output scale +38 requires 10^76, which fails in i128;
                // successful input NULLs still skip that numeric failure.
                (
                    BinaryOperator::Add,
                    decimal(1, -38),
                    decimal(38, 38),
                    vec![Some(1), None, Some(1)],
                    vec![Some(1), Some(1), None],
                    vec![None, None, None],
                    policy == DecimalOverflowPolicy::ReportError,
                ),
                // Division's 10^44 factor fails. A zero divisor returns a
                // successful NULL before applying the impossible factor.
                (
                    BinaryOperator::Divide,
                    decimal(38, 0),
                    decimal(38, 38),
                    vec![Some(1), Some(1), None],
                    vec![Some(1), Some(0), Some(1)],
                    vec![None, None, None],
                    policy == DecimalOverflowPolicy::ReportError,
                ),
            ] {
                let program = typed_program(
                    op,
                    left.clone(),
                    right.clone(),
                    Wrap::Bare,
                    allow,
                    policy,
                    None,
                );
                let batch = typed_input(
                    &program,
                    d128(&left, &lvalues),
                    d128(&right, &rvalues),
                    &[None; 3],
                );
                let output = instance(&program)
                    .evaluate(
                        EvaluationInput::Batch {
                            batch: &batch,
                            source: None,
                        },
                        ScalarInvocationScope::new(Selection::all(3), Selection::all(3)),
                        ScalarInvocationActivation::Activated,
                        &Control,
                        &mut PassThroughCallHost,
                    )
                    .unwrap();
                assert_eq!(
                    coefficients(&output),
                    expected,
                    "{op:?}, {policy:?}, allow={allow}"
                );
                assert_eq!(
                    errors(&output),
                    if raises { vec![0] } else { vec![] },
                    "{op:?}, {policy:?}, allow={allow}"
                );
                assert_eq!(exact_recipe(&program).decimal_overflow_policy(), policy);
                assert_eq!(exact_recipe(&program).allow_throw_exception(), allow);
            }
            for op in [BinaryOperator::Divide, BinaryOperator::Modulo] {
                let ty = decimal(6, 2);
                let program =
                    typed_program(op, ty.clone(), ty.clone(), Wrap::Bare, allow, policy, None);
                let batch = typed_input(
                    &program,
                    d128(&ty, &[Some(123), None, Some(123)]),
                    d128(&ty, &[Some(0), Some(0), None]),
                    &[None; 3],
                );
                let output = instance(&program)
                    .evaluate(
                        EvaluationInput::Batch {
                            batch: &batch,
                            source: None,
                        },
                        ScalarInvocationScope::new(Selection::all(3), Selection::all(3)),
                        ScalarInvocationActivation::Activated,
                        &Control,
                        &mut PassThroughCallHost,
                    )
                    .unwrap();
                assert_eq!(coefficients(&output), vec![None; 3]);
                assert!(
                    output.errors().is_empty(),
                    "decimal zero is not signed Mod's row error"
                );
            }
        }
    }
    // Ordinary valid Decimal literal authors retain the same constant shape.
    let ty = decimal(6, 2);
    let program = typed_program(
        BinaryOperator::Add,
        ty.clone(),
        ty.clone(),
        Wrap::ConstantRight,
        false,
        DecimalOverflowPolicy::OutputNull,
        Some(checked_constants::decimal128(
            &ty,
            125,
            fixture_constant_policy(),
        )),
    );
    let batch = typed_input(
        &program,
        d128(&ty, &[Some(250), None, Some(-125)]),
        d128(&ty, &[Some(maximum); 3]),
        &[None; 3],
    );
    let output = instance(&program)
        .evaluate(
            EvaluationInput::Batch {
                batch: &batch,
                source: None,
            },
            ScalarInvocationScope::new(Selection::all(3), Selection::all(3)),
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert_eq!(coefficients(&output), vec![Some(375), None, Some(0)]);
    assert!(output.errors().is_empty());
}

#[test]
fn accurate_largeint_wraps_four_integral_operations_and_divides_fractionally() {
    let ty = largeint();
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        for (op, left, right, expected) in [
            (
                BinaryOperator::Add,
                vec![Some(i128::MAX), Some(i128::MIN), None],
                vec![Some(1), Some(-1), Some(0)],
                vec![Some(i128::MIN), Some(i128::MAX), None],
            ),
            (
                BinaryOperator::Subtract,
                vec![Some(i128::MIN), Some(i128::MAX), None],
                vec![Some(1), Some(-1), Some(0)],
                vec![Some(i128::MAX), Some(i128::MIN), None],
            ),
            (
                BinaryOperator::Multiply,
                vec![Some(i128::MAX), Some(i128::MIN), None],
                vec![Some(2), Some(-1), Some(0)],
                vec![Some(-2), Some(i128::MIN), None],
            ),
            (
                BinaryOperator::Modulo,
                vec![Some(i128::MIN), Some(7), Some(-7), None],
                vec![Some(-1), Some(0), Some(3), Some(0)],
                vec![Some(0), None, Some(-1), None],
            ),
        ] {
            let program = typed_program(op, ty.clone(), ty.clone(), Wrap::Bare, true, policy, None);
            let batch = typed_input(
                &program,
                large(&left),
                large(&right),
                &vec![None; left.len()],
            );
            let output = instance(&program)
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &batch,
                        source: None,
                    },
                    {
                        let selection = Selection::all(left.len());
                        ScalarInvocationScope::new(selection, selection)
                    },
                    ScalarInvocationActivation::Activated,
                    &Control,
                    &mut PassThroughCallHost,
                )
                .unwrap();
            assert_eq!(large_values(&output), expected);
            assert!(output.errors().is_empty());
            assert_eq!(
                exact_recipe(&program).left_type().logical_type(),
                ValueLogicalType::LargeInt
            );
            assert_eq!(
                exact_recipe(&program).result_type().logical_type(),
                ValueLogicalType::LargeInt
            );
        }
    }
    let program = typed_program(
        BinaryOperator::Divide,
        ty.clone(),
        ty.clone(),
        Wrap::Bare,
        false,
        DecimalOverflowPolicy::ReportError,
        None,
    );
    let batch = typed_input(
        &program,
        large(&[Some(7), Some(-7), Some(i128::MIN), None, Some(1)]),
        large(&[Some(2), Some(2), Some(-1), Some(1), Some(0)]),
        &[None; 5],
    );
    let output = instance(&program)
        .evaluate(
            EvaluationInput::Batch {
                batch: &batch,
                source: None,
            },
            ScalarInvocationScope::new(Selection::all(5), Selection::all(5)),
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert_eq!(
        output
            .values()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![
            Some(3.5),
            Some(-3.5),
            Some(170_141_183_460_469_231_731_687_303_715_884_105_728.0),
            None,
            None
        ]
    );
    assert_eq!(
        exact_recipe(&program).result_type().logical_type(),
        ValueLogicalType::Physical
    );
    assert!(output.errors().is_empty());
    // Mixed signed/LargeInt stays an exact source pair on both sides.
    for reversed in [false, true] {
        let signed_type = FunctionValueType::new(DataType::Int64, true);
        let (left, right) = if reversed {
            (signed_type.clone(), ty.clone())
        } else {
            (ty.clone(), signed_type.clone())
        };
        let program = typed_program(
            BinaryOperator::Divide,
            left,
            right,
            Wrap::Bare,
            false,
            DecimalOverflowPolicy::OutputNull,
            None,
        );
        let (left, right) = if reversed {
            (
                Arc::new(Int64Array::from(vec![Some(7)])) as ArrayRef,
                large(&[Some(2)]),
            )
        } else {
            (
                large(&[Some(7)]),
                Arc::new(Int64Array::from(vec![Some(2)])) as ArrayRef,
            )
        };
        let batch = typed_input(&program, left, right, &[None]);
        let output = instance(&program)
            .evaluate(
                EvaluationInput::Batch {
                    batch: &batch,
                    source: None,
                },
                ScalarInvocationScope::new(Selection::all(1), Selection::all(1)),
                ScalarInvocationActivation::Activated,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        assert_eq!(
            output
                .values()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            3.5
        );
        assert!(output.errors().is_empty());
    }
}

#[test]
fn decimal128_and_decimal256_largeint_add_subtract_keep_full_domains_and_slices() {
    let sum = i256::from_str("17014118346046923173168730371588410572823").unwrap();
    let difference = i256::from_str("17014118346046923173168730371588410572577").unwrap();
    for decimal_type in [
        decimal(6, 2),
        FunctionValueType::new(DataType::Decimal256(40, 2), true),
    ] {
        for reversed in [false, true] {
            for op in [BinaryOperator::Add, BinaryOperator::Subtract] {
                let li = largeint();
                let (left_type, right_type) = if reversed {
                    (li, decimal_type.clone())
                } else {
                    (decimal_type.clone(), li)
                };
                let program = typed_program(
                    op,
                    left_type.clone(),
                    right_type.clone(),
                    Wrap::Bare,
                    false,
                    DecimalOverflowPolicy::ReportError,
                    None,
                );
                let decimal_values = match decimal_type.data_type() {
                    DataType::Decimal128(..) => {
                        d128(&decimal_type, &[None, Some(123), Some(123), None, Some(99)])
                    }
                    DataType::Decimal256(..) => d256(
                        &decimal_type,
                        &[
                            None,
                            Some(i256::from_i128(123)),
                            Some(i256::from_i128(123)),
                            None,
                            Some(i256::from_i128(99)),
                        ],
                    ),
                    _ => unreachable!(),
                }
                .slice(1, 3);
                let integer_values =
                    large(&[Some(0), Some(i128::MAX), Some(7), None, Some(0)]).slice(1, 3);
                let (left, right) = if reversed {
                    (integer_values, decimal_values)
                } else {
                    (decimal_values, integer_values)
                };
                let batch = typed_input(&program, left, right, &[None; 3]);
                let output = instance(&program)
                    .evaluate(
                        EvaluationInput::Batch {
                            batch: &batch,
                            source: None,
                        },
                        ScalarInvocationScope::new(Selection::all(3), Selection::all(3)),
                        ScalarInvocationActivation::Activated,
                        &Control,
                        &mut PassThroughCallHost,
                    )
                    .unwrap();
                let expected = match (op, reversed) {
                    (BinaryOperator::Add, _) => vec![Some(sum), Some(i256::from_i128(823)), None],
                    (BinaryOperator::Subtract, false) => {
                        vec![Some(-difference), Some(i256::from_i128(-577)), None]
                    }
                    (BinaryOperator::Subtract, true) => {
                        vec![Some(difference), Some(i256::from_i128(577)), None]
                    }
                    _ => unreachable!(),
                };
                assert_eq!(wide_coefficients(&output), expected);
                assert_eq!(output.values().data_type(), &DataType::Decimal256(42, 2));
                assert_eq!(exact_recipe(&program).left_type(), &left_type);
                assert_eq!(exact_recipe(&program).right_type(), &right_type);
                assert!(output.errors().is_empty());
            }
        }
    }
}

#[test]
fn decimal_guarded_rows_keep_terminal_errors_successful_nulls_and_decisive_pure_regions() {
    let maximum = 10_i128.pow(38) - 1;
    let left = decimal(38, 0);
    let right = decimal(1, 0);
    for wrap in [Wrap::IsNull, Wrap::Coalesce, Wrap::If, Wrap::And, Wrap::Or] {
        let program = typed_program(
            BinaryOperator::Add,
            left.clone(),
            right.clone(),
            wrap,
            true,
            DecimalOverflowPolicy::ReportError,
            None,
        );
        let batch = typed_input(
            &program,
            d128(&left, &[Some(maximum), Some(maximum), None, Some(7)]),
            d128(&right, &[Some(1), Some(1), Some(0), Some(3)]),
            &[Some(false), Some(true), Some(true), None],
        );
        let output = instance(&program)
            .evaluate(
                EvaluationInput::Batch {
                    batch: &batch,
                    source: None,
                },
                ScalarInvocationScope::new(Selection::all(4), Selection::all(4)),
                ScalarInvocationActivation::Activated,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        match wrap {
            Wrap::If => {
                assert_eq!(coefficients(&output), vec![Some(71), None, None, Some(71)]);
                assert_eq!(errors(&output), vec![1]);
            }
            Wrap::Coalesce => {
                assert_eq!(coefficients(&output), vec![None, None, Some(71), Some(10)]);
                assert_eq!(errors(&output), vec![0, 1]);
            }
            Wrap::IsNull => {
                assert_eq!(
                    output
                        .values()
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .unwrap()
                        .iter()
                        .collect::<Vec<_>>(),
                    vec![None, None, Some(true), Some(false)]
                );
                assert_eq!(errors(&output), vec![0, 1]);
            }
            Wrap::And | Wrap::Or => {
                assert_eq!(
                    output
                        .values()
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .unwrap()
                        .iter()
                        .collect::<Vec<_>>(),
                    vec![Some(matches!(wrap, Wrap::Or)); 4]
                );
                assert!(output.errors().is_empty());
            }
            _ => unreachable!(),
        }
    }
}

#[test]
fn decimal_selected_rows_preserve_all_seven_primary_refusals_and_failed_latch() {
    let left = decimal(38, 0);
    let right = decimal(1, 0);
    let program = typed_program(
        BinaryOperator::Multiply,
        left.clone(),
        right.clone(),
        Wrap::Coalesce,
        true,
        DecimalOverflowPolicy::OutputNull,
        None,
    );
    let maximum = 10_i128.pow(38) - 1;
    let rows = (0..320)
        .map(|row| match row % 3 {
            0 => Some(maximum),
            1 => None,
            _ => Some(1),
        })
        .collect::<Vec<_>>();
    let batch = typed_input(
        &program,
        d128(&left, &rows),
        d128(&right, &[Some(2); 320]),
        &[None; 320],
    );
    let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
    let output = instance(&program)
        .evaluate(
            EvaluationInput::Batch {
                batch: &batch,
                source: None,
            },
            ScalarInvocationScope::new(Selection::all(320), Selection::all(320)),
            ScalarInvocationActivation::Activated,
            &recorder,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert_eq!(errors(&output).len(), 107);
    let trace = recorder.trace();
    assert!(trace.contains(&256));
    assert!(trace.iter().all(|units| *units <= 256));
    for stop_at in 1..=trace.len() {
        for cause in causes() {
            let mut evaluator = instance(&program);
            let control = CallbackControl::new(cause.clone(), stop_at);
            assert!(
                matches!(evaluator.evaluate(EvaluationInput::Batch { batch: &batch, source: None }, ScalarInvocationScope::new(Selection::all(320), Selection::all(320)), ScalarInvocationActivation::Activated, &control, &mut PassThroughCallHost), Err(ExprEvaluationFailure::Kernel(actual)) if actual == cause)
            );
            assert_eq!(control.trace(), trace[..stop_at]);
            let after = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
            assert!(matches!(
                evaluator.evaluate(
                    EvaluationInput::Batch {
                        batch: &batch,
                        source: None
                    },
                    ScalarInvocationScope::new(Selection::all(320), Selection::all(320)),
                    ScalarInvocationActivation::Activated,
                    &after,
                    &mut PassThroughCallHost
                ),
                Err(ExprEvaluationFailure::Kernel(KernelFailure::InstanceFailed))
            ));
            assert!(after.trace().is_empty());
        }
    }
}
