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

use arrow::{
    array::{
        ArrayRef, BinaryArray, BooleanArray, Decimal128Array, FixedSizeBinaryBuilder, Float64Array,
        Int64Array, LargeBinaryArray, LargeStringArray, StringArray,
    },
    datatypes::DataType,
};
use novarocks_expression::{
    ComparisonSite, EvaluationInput, ExprEvaluationFailure,
    testkit::{
        PassThroughCallHost,
        guarded_fixtures::{
            CallbackControl, Fixture, GUARDED_CONTROL as Control, causes,
            ordered::{
                Source, ordered_batch, ordered_instance, ordered_program, ordered_value_program,
            },
        },
    },
};
use novarocks_expression_contract::BinaryOperator;
use novarocks_functions::{
    ComparisonOperator, KernelFailure, ScalarInvocationActivation, ScalarInvocationScope, Selection,
};
use novarocks_type_contract::{EvaluationDemand, FunctionValueType};
use std::sync::Arc;
fn operators() -> [(BinaryOperator, ComparisonOperator); 4] {
    [
        (BinaryOperator::Lt, ComparisonOperator::Lt),
        (BinaryOperator::LtEq, ComparisonOperator::Le),
        (BinaryOperator::Gt, ComparisonOperator::Gt),
        (BinaryOperator::GtEq, ComparisonOperator::Ge),
    ]
}
// Relation -1/0/1 is supplied by the independent hand-written oracle below.
fn expected(operator: ComparisonOperator, relations: &[Option<i8>]) -> Vec<Option<bool>> {
    relations
        .iter()
        .map(|relation| {
            relation.map(|r| match operator {
                ComparisonOperator::Lt => r == -1,
                ComparisonOperator::Le => r != 1,
                ComparisonOperator::Gt => r == 1,
                ComparisonOperator::Ge => r != -1,
                _ => unreachable!("four ordered operators only"),
            })
        })
        .collect()
}
fn result(output: &novarocks_functions::SelectedValues<'_>) -> Vec<Option<bool>> {
    output
        .values()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .iter()
        .collect()
}
fn assert_recipe(program: &Fixture, truth: bool, op: ComparisonOperator, ty: DataType) {
    let unit = &program.unit;
    let occurrence = unit.root(program.root).unwrap().use_id;
    let recipe = unit
        .recipes()
        .comparison(ComparisonSite::Binary(occurrence))
        .unwrap();
    assert_eq!(recipe.operator(), op);
    assert_eq!(recipe.left_type().data_type().clone(), ty);
    assert_eq!(recipe.right_type().data_type().clone(), ty);
    let flow = unit.control_flow();
    let invocation = &flow.uses()[&occurrence];
    assert_eq!(
        invocation.context.demand,
        if truth {
            EvaluationDemand::TruthOnly
        } else {
            EvaluationDemand::Value
        }
    );
    for child in &invocation.arguments {
        assert_eq!(flow.uses()[child].context.demand, EvaluationDemand::Value);
    }
}

fn bytes(ty: &DataType, values: &[Option<&[u8]>]) -> ArrayRef {
    match ty {
        DataType::Utf8 => Arc::new(StringArray::from(
            values
                .iter()
                .map(|v| v.map(|b| std::str::from_utf8(b).unwrap()))
                .collect::<Vec<_>>(),
        )),
        DataType::LargeUtf8 => Arc::new(LargeStringArray::from(
            values
                .iter()
                .map(|v| v.map(|b| std::str::from_utf8(b).unwrap()))
                .collect::<Vec<_>>(),
        )),
        DataType::Binary => Arc::new(BinaryArray::from(values.to_vec())),
        DataType::LargeBinary => Arc::new(LargeBinaryArray::from(values.to_vec())),
        _ => unreachable!("four exact byte carriers only"),
    }
}
fn decimal_array(values: Vec<Option<i128>>) -> ArrayRef {
    Arc::new(
        Decimal128Array::from(values)
            .with_precision_and_scale(38, 0)
            .unwrap(),
    )
}
fn fixed16(values: &[Option<i128>]) -> ArrayRef {
    let mut builder = FixedSizeBinaryBuilder::new(16);
    for value in values {
        match value {
            Some(value) => builder.append_value(value.to_be_bytes()).unwrap(),
            None => builder.append_null(),
        }
    }
    Arc::new(builder.finish())
}

#[test]
fn ordered_four_operators_preserve_i64_precision_extremes_sparse_nulls_and_actual_recipe() {
    for (physical, frozen) in operators() {
        let program = ordered_program(physical, DataType::Int64, Source::Columns, false);
        assert_recipe(&program, false, frozen, DataType::Int64);
        let input = ordered_batch(
            &program,
            Arc::new(Int64Array::from(vec![
                Some(99),
                Some(i64::MIN),
                Some(9_007_199_254_740_993),
                Some(i64::MAX),
                Some(-1),
                None,
                Some(1),
            ])),
            Arc::new(Int64Array::from(vec![
                Some(-99),
                Some(i64::MAX),
                Some(9_007_199_254_740_992),
                Some(i64::MAX),
                Some(0),
                Some(1),
                None,
            ])),
            vec![None; 7],
        );
        let rows = [1, 2, 3, 4, 5, 6];
        let selection = Selection::try_sparse(7, &rows).unwrap();
        let output = ordered_instance(&program)
            .evaluate(
                EvaluationInput::Batch {
                    batch: &input,
                    source: None,
                },
                ScalarInvocationScope::new(selection, selection),
                ScalarInvocationActivation::Activated,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        assert_eq!(output.selection(), selection);
        assert_eq!(
            result(&output),
            expected(frozen, &[Some(-1), Some(1), Some(0), Some(-1), None, None])
        );
        assert!(output.errors().is_empty());
    }
}

#[test]
fn ordered_float_total_order_distinguishes_signed_zero_nan_payload_sign_and_infinity() {
    let positive_nan = f64::from_bits(0x7ff8_0000_0000_0001);
    let next_nan = f64::from_bits(0x7ff8_0000_0000_0002);
    let negative_nan = f64::from_bits(0xfff8_0000_0000_0001);
    for (physical, frozen) in operators() {
        let program = ordered_program(physical, DataType::Float64, Source::Columns, false);
        let input = ordered_batch(
            &program,
            Arc::new(Float64Array::from(vec![
                Some(-0.0),
                Some(0.0),
                Some(positive_nan),
                Some(positive_nan),
                Some(negative_nan),
                Some(f64::NEG_INFINITY),
                Some(f64::INFINITY),
                None,
            ])),
            Arc::new(Float64Array::from(vec![
                Some(0.0),
                Some(-0.0),
                Some(positive_nan),
                Some(next_nan),
                Some(f64::NEG_INFINITY),
                Some(f64::INFINITY),
                Some(positive_nan),
                Some(0.0),
            ])),
            vec![None; 8],
        );
        let output = ordered_instance(&program)
            .evaluate(
                EvaluationInput::Batch {
                    batch: &input,
                    source: None,
                },
                ScalarInvocationScope::new(Selection::all(8), Selection::all(8)),
                ScalarInvocationActivation::Activated,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        assert_eq!(
            result(&output),
            expected(
                frozen,
                &[
                    Some(-1),
                    Some(1),
                    Some(0),
                    Some(-1),
                    Some(-1),
                    Some(-1),
                    Some(-1),
                    None
                ]
            )
        );
        assert!(output.errors().is_empty());
    }
}

#[test]
fn ordered_bytes_compare_actual_sliced_common_prefix_unsigned_bytes_and_null_sparse_rows() {
    for ty in [
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Binary,
        DataType::LargeBinary,
    ] {
        let prefix = "a".repeat(2048);
        let later = format!("{prefix}z");
        for (physical, frozen) in operators() {
            let program = ordered_program(physical, ty.clone(), Source::Columns, false);
            let left = bytes(
                &ty,
                &[
                    Some(b"outside"),
                    Some(prefix.as_bytes()),
                    Some(later.as_bytes()),
                    None,
                    Some(if matches!(ty, DataType::Binary | DataType::LargeBinary) {
                        b"\x80".as_slice()
                    } else {
                        b"\0b".as_slice()
                    }),
                    Some(b"same"),
                    Some(b"tail"),
                ],
            )
            .slice(1, 5);
            let right = bytes(
                &ty,
                &[
                    Some(b"unused"),
                    Some(later.as_bytes()),
                    Some(prefix.as_bytes()),
                    Some(b"nonnull"),
                    Some(if matches!(ty, DataType::Binary | DataType::LargeBinary) {
                        b"\x7f".as_slice()
                    } else {
                        b"\0a".as_slice()
                    }),
                    Some(b"same"),
                    Some(b"end"),
                ],
            )
            .slice(1, 5);
            let input = ordered_batch(&program, left, right, vec![None; 5]);
            let dense = ordered_instance(&program)
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &input,
                        source: None,
                    },
                    ScalarInvocationScope::new(Selection::all(5), Selection::all(5)),
                    ScalarInvocationActivation::Activated,
                    &Control,
                    &mut PassThroughCallHost,
                )
                .unwrap();
            assert_eq!(
                result(&dense),
                expected(frozen, &[Some(-1), Some(1), None, Some(1), Some(0)])
            );
            let rows = [0, 2, 3, 4];
            let selection = Selection::try_sparse(5, &rows).unwrap();
            let output = ordered_instance(&program)
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &input,
                        source: None,
                    },
                    ScalarInvocationScope::new(selection, selection),
                    ScalarInvocationActivation::Activated,
                    &Control,
                    &mut PassThroughCallHost,
                )
                .unwrap();
            assert_eq!(
                result(&output),
                expected(frozen, &[Some(-1), None, Some(1), Some(0)])
            );
            assert_eq!(output.selection(), selection);
            assert!(output.errors().is_empty());
        }
    }
}

#[test]
fn ordered_filter_truth_only_root_keeps_value_demand_children_and_successful_null() {
    for (physical, frozen) in operators() {
        let program = ordered_program(physical, DataType::Int64, Source::Columns, true);
        assert_recipe(&program, true, frozen, DataType::Int64);
        let input = ordered_batch(
            &program,
            Arc::new(Int64Array::from(vec![Some(0), None, Some(2)])),
            Arc::new(Int64Array::from(vec![Some(1), Some(0), Some(1)])),
            vec![None; 3],
        );
        let output = ordered_instance(&program)
            .evaluate(
                EvaluationInput::Batch {
                    batch: &input,
                    source: None,
                },
                ScalarInvocationScope::new(Selection::all(3), Selection::all(3)),
                ScalarInvocationActivation::Activated,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        assert_eq!(
            result(&output),
            expected(frozen, &[Some(-1), None, Some(1)])
        );
        assert!(output.errors().is_empty());
    }
}

#[test]
fn ordered_both_round_children_preserve_each_compact_error_cursor_and_eager_right_on_null() {
    let program = ordered_program(
        BinaryOperator::Lt,
        DataType::Decimal128(38, 0),
        Source::Rounded,
        false,
    );
    let max = 10_i128.pow(38) - 1;
    let mut left = vec![Some(25); 10];
    let mut right = vec![Some(30); 10];
    left[1] = Some(max);
    right[3] = Some(max);
    left[5] = Some(max);
    right[5] = Some(max);
    left[9] = None;
    right[9] = Some(max);
    let input = ordered_batch(
        &program,
        decimal_array(left),
        decimal_array(right),
        vec![None; 10],
    );
    let rows = [1, 3, 5, 7, 9];
    let selection = Selection::try_sparse(10, &rows).unwrap();
    let output = ordered_instance(&program)
        .evaluate(
            EvaluationInput::Batch {
                batch: &input,
                source: None,
            },
            ScalarInvocationScope::new(selection, selection),
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert_eq!(result(&output), vec![None, None, None, Some(false), None]);
    assert_eq!(
        output
            .errors()
            .iter()
            .map(|e| e.selected_ordinal())
            .collect::<Vec<_>>(),
        vec![0, 1, 2, 4]
    );
    assert_eq!(output.selection().row(4), Some(9));
    assert!(
        output
            .errors()
            .iter()
            .all(|e| e.message().contains("overflow"))
    );
}

#[test]
fn every_actual_ordered_comparison_callback_keeps_seven_primary_failures_and_refuses_replay() {
    let program = ordered_program(
        BinaryOperator::LtEq,
        DataType::Decimal128(38, 0),
        Source::Rounded,
        false,
    );
    let max = 10_i128.pow(38) - 1;
    let input = ordered_batch(
        &program,
        decimal_array(
            (0..320)
                .map(|row| if row % 3 == 2 { Some(max) } else { Some(25) })
                .collect(),
        ),
        decimal_array(
            (0..320)
                .map(|row| if row % 3 == 1 { Some(max) } else { Some(30) })
                .collect(),
        ),
        vec![None; 320],
    );
    let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX, Control);
    let output = ordered_instance(&program)
        .evaluate(
            EvaluationInput::Batch {
                batch: &input,
                source: None,
            },
            ScalarInvocationScope::new(Selection::all(320), Selection::all(320)),
            ScalarInvocationActivation::Activated,
            &recorder,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert_eq!(output.errors().len(), 213);
    assert_eq!(
        result(&output),
        (0..320)
            .map(|row| if row % 3 == 0 { Some(true) } else { None })
            .collect::<Vec<_>>()
    );
    let trace = recorder.trace.lock().unwrap().clone();
    assert!(trace.contains(&256));
    assert!(trace.iter().all(|units| *units <= 256));
    for index in 1..=trace.len() {
        for cause in causes("guarded-origin") {
            let mut evaluator = ordered_instance(&program);
            let control = CallbackControl::new(cause.clone(), index, Control);
            assert!(
                matches!(evaluator.evaluate(EvaluationInput::Batch { batch: &input, source: None }, ScalarInvocationScope::new(Selection::all(320), Selection::all(320)), ScalarInvocationActivation::Activated, &control, &mut PassThroughCallHost), Err(ExprEvaluationFailure::Kernel(actual)) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..index]);
            let after = CallbackControl::new(KernelFailure::Cancelled, usize::MAX, Control);
            assert!(matches!(
                evaluator.evaluate(
                    EvaluationInput::Batch {
                        batch: &input,
                        source: None
                    },
                    ScalarInvocationScope::new(Selection::all(320), Selection::all(320)),
                    ScalarInvocationActivation::Activated,
                    &after,
                    &mut PassThroughCallHost
                ),
                Err(ExprEvaluationFailure::Kernel(KernelFailure::InstanceFailed))
            ));
            assert!(after.trace.lock().unwrap().is_empty());
        }
    }
}

#[test]
fn ordered_largeint_is_signed_i128_while_same_fixed16_physical_and_uuid_keep_byte_order() {
    let carrier = DataType::FixedSizeBinary(16);
    let domains = [
        FunctionValueType::try_with_logical_type(
            carrier.clone(),
            true,
            novarocks_type_contract::ValueLogicalType::LargeInt,
        )
        .unwrap(),
        FunctionValueType::new(carrier.clone(), true),
        FunctionValueType::try_with_logical_type(
            carrier,
            true,
            novarocks_type_contract::ValueLogicalType::Uuid,
        )
        .unwrap(),
    ];
    for domain in domains {
        for (physical, frozen) in operators() {
            let program = ordered_value_program(physical, domain.clone(), Source::Columns, false);
            let recipe = program
                .unit
                .recipes()
                .comparison(ComparisonSite::Binary(
                    program.unit.root(program.root).unwrap().use_id,
                ))
                .unwrap();
            assert_eq!(recipe.left_type(), &domain);
            assert_eq!(recipe.right_type(), &domain);
            let input = ordered_batch(
                &program,
                fixed16(&[
                    Some(-1),
                    Some(i128::MIN),
                    Some(0),
                    Some(1),
                    Some(i128::MAX),
                    None,
                ]),
                fixed16(&[
                    Some(0),
                    Some(0),
                    Some(1),
                    Some(-1),
                    Some(i128::MIN),
                    Some(0),
                ]),
                vec![None; 6],
            );
            let output = ordered_instance(&program)
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &input,
                        source: None,
                    },
                    ScalarInvocationScope::new(Selection::all(6), Selection::all(6)),
                    ScalarInvocationActivation::Activated,
                    &Control,
                    &mut PassThroughCallHost,
                )
                .unwrap();
            let relations =
                if domain.logical_type() == novarocks_type_contract::ValueLogicalType::LargeInt {
                    [Some(-1), Some(-1), Some(-1), Some(1), Some(1), None]
                } else {
                    // Big-endian raw bytes order ff/80 after 00 and 7f before 80.
                    [Some(1), Some(1), Some(-1), Some(-1), Some(-1), None]
                };
            assert_eq!(result(&output), expected(frozen, &relations));
            assert!(output.errors().is_empty());
        }
    }
}
