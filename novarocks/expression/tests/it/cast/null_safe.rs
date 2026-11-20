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
use arrow::array::{Float32Array, StringArray};
use novarocks_expression::testkit::PassThroughCallHost;
use novarocks_expression::{EvaluationInput, ExprEvaluationFailure};
use novarocks_functions::{ScalarInvocationActivation, ScalarInvocationScope};

fn nullsafe_batch(program: &CompiledFixture, left: ArrayRef, right: ArrayRef) -> RecordBatch {
    RecordBatch::try_new(program.input_schema().clone(), vec![left, right]).unwrap()
}
fn boolean_values(output: &novarocks_functions::SelectedValues<'_>) -> Vec<Option<bool>> {
    output
        .values()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .iter()
        .collect()
}

#[test]
fn floating_nullsafe_keeps_nan_any_nonnull_and_signed_zero_in_both_operand_orders() {
    let left = [
        Some(0.0),
        Some(-0.0),
        Some(f64::from_bits(0x7ff8_0000_0000_0011)),
        Some(19.0),
        None,
        Some(f64::NEG_INFINITY),
        Some(2.0),
        None,
    ];
    let right = [
        Some(-0.0),
        Some(0.0),
        Some(-7.0),
        Some(f64::from_bits(0xfff8_0000_0000_0022)),
        Some(f64::NAN),
        Some(f64::INFINITY),
        Some(3.0),
        None,
    ];
    let expected = vec![
        Some(true),
        Some(true),
        Some(true),
        Some(true),
        Some(false),
        Some(false),
        Some(false),
        Some(true),
    ];
    for ty in [DataType::Float32, DataType::Float64] {
        let program = nullsafe_program(
            FunctionValueType::new(ty.clone(), true),
            NullSafeRight::Column,
        );
        for swapped in [false, true] {
            let arrays = if ty == DataType::Float32 {
                (
                    Arc::new(Float32Array::from(
                        left.iter()
                            .map(|v| {
                                v.map(|v| {
                                    if v.is_nan() {
                                        f32::from_bits(0x7fc0_0011)
                                    } else {
                                        v as f32
                                    }
                                })
                            })
                            .collect::<Vec<_>>(),
                    )) as ArrayRef,
                    Arc::new(Float32Array::from(
                        right
                            .iter()
                            .map(|v| {
                                v.map(|v| {
                                    if v.is_nan() {
                                        f32::from_bits(0xffc0_0022)
                                    } else {
                                        v as f32
                                    }
                                })
                            })
                            .collect::<Vec<_>>(),
                    )) as ArrayRef,
                )
            } else {
                (
                    Arc::new(Float64Array::from(left.to_vec())) as ArrayRef,
                    Arc::new(Float64Array::from(right.to_vec())) as ArrayRef,
                )
            };
            let (left, right) = if swapped {
                (arrays.1, arrays.0)
            } else {
                arrays
            };
            let output = instance(&program)
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &nullsafe_batch(&program, left, right),
                        source: None,
                    },
                    ScalarInvocationScope::new(Selection::all(8), Selection::all(8)),
                    ScalarInvocationActivation::Activated,
                    &Control,
                    &mut PassThroughCallHost,
                )
                .unwrap();
            assert_eq!(boolean_values(&output), expected);
        }
    }
}

#[test]
fn nullsafe_controller_preserves_every_primary_refusal_prefix_and_failed_latch() {
    let program = nullsafe_program(
        FunctionValueType::new(DataType::Int64, true),
        NullSafeRight::Column,
    );
    let batch = nullsafe_batch(
        &program,
        Arc::new(Int64Array::from(vec![Some(7); 320])),
        Arc::new(Int64Array::from(vec![Some(7); 320])),
    );
    let constructor = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
    CompiledExpressionInstance::try_new(root_unit(&program).clone(), program.root, &constructor)
        .unwrap();
    let construction = constructor.trace();
    for stop_at in 1..=construction.len() {
        for cause in causes() {
            let refusal = CallbackControl::new(cause.clone(), stop_at);
            assert!(
                matches!(CompiledExpressionInstance::try_new(root_unit(&program).clone(), program.root, &refusal), Err(actual) if actual == cause)
            );
            assert_eq!(refusal.trace(), construction[..stop_at]);
        }
    }
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
    assert_eq!(boolean_values(&output), vec![Some(true); 320]);
    let trace = recorder.trace();
    assert!(trace.contains(&256));
    assert!(trace.iter().all(|units| *units <= 256));
    for stop_at in 1..=trace.len() {
        for cause in causes() {
            let mut evaluator = instance(&program);
            let refusal = CallbackControl::new(cause.clone(), stop_at);
            assert!(
                matches!(evaluator.evaluate(EvaluationInput::Batch { batch: &batch, source: None }, ScalarInvocationScope::new(Selection::all(320), Selection::all(320)), ScalarInvocationActivation::Activated, &refusal, &mut PassThroughCallHost), Err(ExprEvaluationFailure::Kernel(actual)) if actual == cause)
            );
            assert_eq!(refusal.trace(), trace[..stop_at]);
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

#[test]
fn nullsafe_source_carrier_and_logical_metadata_are_checked_before_selected_null() {
    let program = nullsafe_program(
        FunctionValueType::new(DataType::Int64, true),
        NullSafeRight::Column,
    );
    let schema = program.input_schema();
    let foreign = RecordBatch::try_new(
        Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new(schema.field(0).name(), DataType::Float64, true),
            arrow::datatypes::Field::new(schema.field(1).name(), DataType::Float64, true),
        ])),
        vec![
            Arc::new(Float64Array::from(vec![None])),
            Arc::new(Float64Array::from(vec![None])),
        ],
    )
    .unwrap();
    assert!(matches!(
        instance(&program).evaluate(
            EvaluationInput::Batch {
                batch: &foreign,
                source: None
            },
            ScalarInvocationScope::new(Selection::all(1), Selection::all(1)),
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost
        ),
        Err(ExprEvaluationFailure::Kernel(
            KernelFailure::InvalidProgram(_)
        ))
    ));
    let program = nullsafe_program(
        FunctionValueType::new(DataType::Utf8, true),
        NullSafeRight::Column,
    );
    let fields = program
        .input_schema()
        .fields()
        .iter()
        .map(|field| {
            let mut metadata = field.metadata().clone();
            metadata.insert("nr_logical_type".to_string(), "Json".to_string());
            field.as_ref().clone().with_metadata(metadata)
        })
        .collect::<Vec<_>>();
    let foreign = RecordBatch::try_new(
        Arc::new(arrow::datatypes::Schema::new(fields)),
        vec![
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![None::<&str>])),
        ],
    )
    .unwrap();
    assert!(matches!(
        instance(&program).evaluate(
            EvaluationInput::Batch {
                batch: &foreign,
                source: None
            },
            ScalarInvocationScope::new(Selection::all(1), Selection::all(1)),
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost
        ),
        Err(ExprEvaluationFailure::Kernel(
            KernelFailure::InvalidProgram(_)
        ))
    ));
}

// Conservative retained-source invoice and independent projection ceilings for
// these small fixtures only; this is not a production default or a MEM grant.

#[test]
fn null_safe_equality_never_turns_successful_null_into_a_row_error() {
    use crate::zero_port;
    use novarocks_expression_contract::{BinaryOperator, ExprKind};
    use novarocks_type_contract::SemanticParameters;

    let result = FunctionValueType::new(DataType::Boolean, false);
    for (left, right, expected) in [
        (None, None, true),
        (Some(true), None, false),
        (None, Some(false), false),
        (Some(true), Some(true), true),
    ] {
        let inputs =
            [left, right].map(|value| Arc::new(BooleanArray::from(vec![value])) as ArrayRef);
        let unit = zero_port::primitive(
            inputs,
            result.clone(),
            |children| ExprKind::Binary {
                left: children[0],
                op: BinaryOperator::EqForNull,
                right: children[1],
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                allow_throw_exception: None,
            },
            &SemanticParameters::default(),
        )
        .unwrap();
        let output = zero_port::evaluate(unit);
        assert!(output.errors().is_empty());
        assert_eq!(boolean_values(&output), vec![Some(expected)]);
    }
}
