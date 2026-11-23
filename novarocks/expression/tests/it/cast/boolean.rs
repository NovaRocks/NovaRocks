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

use super::*;
use arrow::array::ArrayRef;
use arrow::datatypes::Field;
use novarocks_expression::testkit::{PassThroughCallHost, numeric_fixtures::compiled_cast};
use novarocks_expression::{EvaluationInput, ExprEvaluationFailure};
use novarocks_functions::{
    CastPrepareError, PreparedCastRecipe, ScalarInvocationActivation, ScalarInvocationScope,
};
use novarocks_type_contract::{ExpressionEffects, TrinoCastMode, ValueLogicalType};

fn profiles() -> Vec<(DataType, DataType)> {
    let numeric = [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
    ];
    std::iter::once((DataType::Boolean, DataType::Boolean))
        .chain(
            numeric
                .iter()
                .cloned()
                .map(|target| (DataType::Boolean, target)),
        )
        .chain(
            numeric
                .iter()
                .cloned()
                .map(|source| (source, DataType::Boolean)),
        )
        .collect()
}
fn bool_values(output: &novarocks_functions::SelectedValues<'_>) -> Vec<Option<bool>> {
    output
        .values()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .iter()
        .collect()
}
fn batch_with_source(
    program: &CompiledFixture,
    source: ArrayRef,
    flags: Option<Vec<Option<bool>>>,
) -> RecordBatch {
    let rows = source.len();
    RecordBatch::try_new(
        program.input_schema().clone(),
        vec![
            source,
            Arc::new(Int64Array::from(vec![Some(42); rows])),
            Arc::new(BooleanArray::from(
                flags.unwrap_or_else(|| vec![Some(true); rows]),
            )),
        ],
    )
    .unwrap()
}
fn source_values(carrier: &DataType) -> ArrayRef {
    let padded: ArrayRef = match carrier {
        DataType::Boolean => Arc::new(BooleanArray::from(vec![
            Some(true),
            Some(false),
            Some(true),
            None,
            Some(true),
            Some(false),
            Some(true),
        ])),
        DataType::Float32 => Arc::new(Float32Array::from(vec![
            Some(9.0),
            Some(-0.0),
            Some(f32::from_bits(0x7fc12345)),
            None,
            Some(f32::INFINITY),
            Some(f32::from_bits(1)),
            Some(9.0),
        ])),
        DataType::Float64 => Arc::new(Float64Array::from(vec![
            Some(9.0),
            Some(-0.0),
            Some(f64::from_bits(0x7ff8123456789abc)),
            None,
            Some(f64::NEG_INFINITY),
            Some(f64::from_bits(1)),
            Some(9.0),
        ])),
        signed_type => {
            let (min, max) = bound(signed_type);
            signed(
                signed_type,
                &[
                    Some(9),
                    Some(min),
                    Some(0),
                    None,
                    Some(max),
                    Some(-1),
                    Some(9),
                ],
            )
        }
    };
    padded.slice(1, 5)
}
fn assert_zero_one(
    output: &novarocks_functions::SelectedValues<'_>,
    target: &DataType,
    expected: &[Option<bool>],
) {
    match target {
        DataType::Boolean => assert_eq!(bool_values(output), expected),
        DataType::Float32 => assert_eq!(
            output
                .values()
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .iter()
                .map(|value| value.map(f32::to_bits))
                .collect::<Vec<_>>(),
            expected
                .iter()
                .map(|value| value.map(|value| if value { 0x3f800000 } else { 0 }))
                .collect::<Vec<_>>()
        ),
        DataType::Float64 => assert_eq!(
            output
                .values()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .iter()
                .map(|value| value.map(f64::to_bits))
                .collect::<Vec<_>>(),
            expected
                .iter()
                .map(|value| value.map(|value| if value { 0x3ff0000000000000 } else { 0 }))
                .collect::<Vec<_>>()
        ),
        _ => assert_eq!(
            signed_values(output),
            expected
                .iter()
                .map(|value| value.map(i64::from))
                .collect::<Vec<_>>()
        ),
    }
}
fn root_context(program: &CompiledFixture) -> (ExpressionUseId, ExpressionEffectContext) {
    let occurrence = cast_root_use(program);
    (
        occurrence,
        root_unit(program).control_flow().uses()[&occurrence].context,
    )
}

#[test]
fn boolean_cast_modes_preserve_typed_values_and_sparse_source_nulls() {
    for (source, target) in profiles() {
        for mode in [TrinoCastMode::Explicit, TrinoCastMode::Try] {
            let source_type = FunctionValueType::new(source.clone(), true);
            let target_type = FunctionValueType::new(target.clone(), true);
            let program = compiled_cast(
                source_type.clone(),
                target_type.clone(),
                Source::Column,
                Wrap::Bare,
                mode,
                false,
            );
            let (occurrence, context) = root_context(&program);
            let recipe = root_unit(&program).recipes().cast(occurrence).unwrap();
            assert_eq!(recipe.source_type(), &source_type);
            assert_eq!(recipe.result_type(), &target_type);
            assert_eq!(recipe.cast_mode(), Some(mode));
            assert_eq!(
                recipe.own_effects(context).for_use(context).unwrap(),
                ExpressionEffects::PURE_VALUE
            );
            let batch = batch_with_source(&program, source_values(&source), None);
            let rows = [0, 1, 2, 4];
            let selection = Selection::try_sparse(5, &rows).unwrap();
            let expected = match source {
                DataType::Boolean => vec![Some(false), Some(true), None, Some(false)],
                DataType::Float32 | DataType::Float64 => {
                    vec![Some(false), Some(true), None, Some(true)]
                }
                _ => vec![Some(true), Some(false), None, Some(true)],
            };
            let mut evaluator = instance(&program);
            for _ in 0..2 {
                let output = evaluator
                    .evaluate(
                        EvaluationInput::Batch {
                            batch: &batch,
                            source: None,
                        },
                        ScalarInvocationScope::new(selection, selection),
                        ScalarInvocationActivation::Activated,
                        &Control,
                        &mut PassThroughCallHost,
                    )
                    .unwrap();
                assert_eq!(output.selection().iter().collect::<Vec<_>>(), rows);
                assert_eq!(output.values().data_type(), &target);
                assert!(output.errors().is_empty());
                assert_zero_one(&output, &target, &expected);
            }
            let output = evaluator
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &batch,
                        source: None,
                    },
                    {
                        let selection = Selection::try_sparse(5, &[]).unwrap();
                        ScalarInvocationScope::new(selection, selection)
                    },
                    ScalarInvocationActivation::Activated,
                    &Control,
                    &mut PassThroughCallHost,
                )
                .unwrap();
            assert!(output.values().is_empty());
            assert_eq!(output.values().data_type(), &target);
            assert!(output.errors().is_empty());
        }
    }
}

#[test]
fn actual_compiled_bool_cast_nonnullable_bool_literal_and_true_constant_broadcast_are_real_producers()
 {
    for target in [
        DataType::Boolean,
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
    ] {
        for allow in [false, true] {
            for policy in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                for mode in [Source::Column, Source::Constant] {
                    let program = compiled(
                        FunctionValueType::new(DataType::Boolean, false),
                        FunctionValueType::new(target.clone(), false),
                        mode,
                        Wrap::Bare,
                        policy,
                        allow,
                    );
                    // The immutable incoming producer is the actual Bool(false)
                    // literal; the constant root is separately authored Bool(true).
                    let batch = batch_with_source(
                        &program,
                        Arc::new(BooleanArray::from(vec![false, true, false, false, true])),
                        None,
                    );
                    let rows = [0, 3, 4];
                    let output = instance(&program)
                        .evaluate(
                            EvaluationInput::Batch {
                                batch: &batch,
                                source: None,
                            },
                            {
                                let selection = Selection::try_sparse(5, &rows).unwrap();
                                ScalarInvocationScope::new(selection, selection)
                            },
                            ScalarInvocationActivation::Activated,
                            &Control,
                            &mut PassThroughCallHost,
                        )
                        .unwrap();
                    let expected = if matches!(mode, Source::Constant) {
                        vec![Some(true); 3]
                    } else {
                        vec![Some(false), Some(false), Some(true)]
                    };
                    assert_zero_one(&output, &target, &expected);
                    assert!(output.errors().is_empty());
                    assert_eq!(output.values().null_count(), 0);
                    assert!(
                        !root_unit(&program)
                            .recipes()
                            .cast(root_context(&program).0)
                            .unwrap()
                            .result_type()
                            .nullable()
                    );
                }
            }
        }
    }
}

#[test]
fn actual_compiled_bool_cast_nonfinite_values_are_not_null_under_coalesce_isnull_or_if() {
    for allow in [false, true] {
        for wrap in [Wrap::Coalesce, Wrap::IsNull, Wrap::If] {
            let program = compiled(
                FunctionValueType::new(DataType::Float64, true),
                FunctionValueType::new(DataType::Boolean, true),
                Source::Column,
                wrap,
                DecimalOverflowPolicy::ReportError,
                allow,
            );
            let source: ArrayRef = Arc::new(Float64Array::from(vec![
                Some(f64::NAN),
                Some(f64::INFINITY),
                Some(-0.0),
                None,
                Some(f64::NEG_INFINITY),
            ]));
            let batch = batch_with_source(
                &program,
                source,
                Some(vec![
                    Some(true),
                    Some(false),
                    Some(true),
                    Some(true),
                    Some(false),
                ]),
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
            assert!(output.errors().is_empty());
            assert_eq!(
                bool_values(&output),
                if matches!(wrap, Wrap::IsNull) {
                    vec![
                        Some(false),
                        Some(false),
                        Some(false),
                        Some(true),
                        Some(false),
                    ]
                } else if matches!(wrap, Wrap::Coalesce) {
                    vec![Some(true), Some(true), Some(false), Some(true), Some(true)]
                } else {
                    vec![Some(true), Some(true), Some(false), None, Some(true)]
                }
            );
        }
    }
}

#[test]
fn boolean_cast_checks_frozen_nullability_and_input_layout() {
    let nullable_source = FunctionValueType::new(DataType::Boolean, true);
    let nonnullable_result = FunctionValueType::new(DataType::Int64, false);
    for mode in [TrinoCastMode::Explicit, TrinoCastMode::Try] {
        assert!(matches!(
            PreparedCastRecipe::try_new_trino(
                &nullable_source,
                &nonnullable_result,
                mode,
                &Control
            ),
            Err(CastPrepareError::TypeMismatch)
        ));
    }
    let program = compiled_cast(
        FunctionValueType::new(DataType::Boolean, false),
        nonnullable_result,
        Source::Column,
        Wrap::Bare,
        TrinoCastMode::Explicit,
        false,
    );
    let wrong: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    // Construct a real but different incoming schema, rather than bypassing
    // Arrow RecordBatch's own column/field compatibility checks.
    let wrong = RecordBatch::try_new(
        Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
            "wrong",
            DataType::Int64,
            false,
        )])),
        vec![wrong],
    )
    .unwrap();
    assert!(matches!(
        instance(&program).evaluate(
            EvaluationInput::Batch {
                batch: &wrong,
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

#[test]
fn uuid_identity_cast_retains_nominal_layout_and_bytes() {
    let uuid = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        true,
        ValueLogicalType::Uuid,
    )
    .unwrap();
    let bytes = [0x84_u8; 16];
    let input = Arc::new(
        arrow::array::FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            [Some(bytes), None].into_iter(),
            16,
        )
        .unwrap(),
    );
    for mode in [TrinoCastMode::Explicit, TrinoCastMode::Try] {
        let program = compiled_cast(
            uuid.clone(),
            uuid.clone(),
            Source::Column,
            Wrap::Bare,
            mode,
            false,
        );
        let recipe = root_unit(&program)
            .recipes()
            .cast(cast_root_use(&program))
            .unwrap();
        assert!(recipe.is_identity());
        assert_eq!(recipe.result_type(), &uuid);
        let batch = batch_with_source(&program, input.clone(), None);
        let output = instance(&program)
            .evaluate(
                EvaluationInput::Batch {
                    batch: &batch,
                    source: None,
                },
                ScalarInvocationScope::new(Selection::all(2), Selection::all(2)),
                ScalarInvocationActivation::Activated,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        assert_eq!(output.values().to_data(), input.to_data());
        assert!(output.errors().is_empty());
    }
}

#[test]
fn actual_compiled_bool_cast_compile_and_runtime_failures_keep_every_original_prefix_and_failed_latch()
 {
    let source = FunctionValueType::new(DataType::Float64, true);
    let target = FunctionValueType::new(DataType::Boolean, true);
    let fixture = cast_fixture(
        source,
        target,
        Source::Column,
        Wrap::Bare,
        DecimalOverflowPolicy::ReportError,
        true,
    )
    .expect("nullable boolean cast is admitted");
    let recorder = CompileCallbacks::new(CompileControlError::Cancelled, usize::MAX);
    let program = fixture.compile(&recorder).unwrap();
    let trace = recorder.trace();
    for at in 1..=trace.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = CompileCallbacks::new(cause.clone(), at);
            assert!(
                matches!(fixture.compile(&control), Err(novarocks_expression::ExprCompileError::Control(actual)) if actual == cause)
            );
            assert_eq!(control.trace(), trace[..at]);
        }
    }
    let batch = batch_with_source(
        &program,
        Arc::new(Float64Array::from(
            (0..320)
                .map(|row| match row % 5 {
                    0 => None,
                    1 => Some(-0.0),
                    2 => Some(f64::NAN),
                    3 => Some(f64::INFINITY),
                    _ => Some(-1.0),
                })
                .collect::<Vec<_>>(),
        )),
        None,
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
    assert!(output.errors().is_empty());
    let trace = recorder.trace();
    assert!(trace.contains(&256));
    for at in 1..=trace.len() {
        for cause in causes() {
            let mut evaluator = instance(&program);
            let control = CallbackControl::new(cause.clone(), at);
            assert!(
                matches!(evaluator.evaluate(EvaluationInput::Batch { batch: &batch, source: None }, ScalarInvocationScope::new(Selection::all(320), Selection::all(320)), ScalarInvocationActivation::Activated, &control, &mut PassThroughCallHost), Err(ExprEvaluationFailure::Kernel(actual)) if actual == cause)
            );
            assert_eq!(control.trace(), trace[..at]);
            let retry = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
            assert!(matches!(
                evaluator.evaluate(
                    EvaluationInput::Batch {
                        batch: &batch,
                        source: None
                    },
                    ScalarInvocationScope::new(Selection::all(320), Selection::all(320)),
                    ScalarInvocationActivation::Activated,
                    &retry,
                    &mut PassThroughCallHost
                ),
                Err(ExprEvaluationFailure::Kernel(KernelFailure::InstanceFailed))
            ));
            assert!(retry.trace().is_empty());
        }
    }
}

// The signed 128-bit representation contract defines these bytes independently
// of CAST. A checked package and the actual identity controller must preserve
// both extrema, NULLs, selection order, and the nominal type.
#[test]
fn actual_compiled_largeint_identity_preserves_extreme_bytes_nulls_and_full_metadata() {
    for (source_null, result_null) in [(false, false), (false, true), (true, true)] {
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            for allow in [false, true] {
                for mode in [Source::Column, Source::Constant] {
                    let source = FunctionValueType::try_with_logical_type(
                        DataType::FixedSizeBinary(16),
                        source_null,
                        ValueLogicalType::LargeInt,
                    )
                    .unwrap();
                    let result = source.with_nullable(result_null).unwrap();
                    let program = compiled(
                        source.clone(),
                        result.clone(),
                        mode,
                        Wrap::Bare,
                        policy,
                        allow,
                    );
                    let recipe = root_unit(&program)
                        .recipes()
                        .cast(root_context(&program).0)
                        .unwrap();
                    assert!(recipe.is_identity());
                    assert_eq!(recipe.source_type(), &source);
                    assert_eq!(recipe.result_type(), &result);
                    let field = recipe.result_type().try_to_field("result").unwrap();
                    assert_eq!(
                        field
                            .metadata()
                            .get(novarocks_type_contract::NR_LOGICAL_TYPE_KEY)
                            .map(String::as_str),
                        Some("largeint")
                    );
                    assert_eq!(field.is_nullable(), result_null);
                    let inputs = [
                        Some(i128::MIN),
                        Some(i128::MAX),
                        Some(-1),
                        Some(0),
                        if source_null { None } else { Some(1) },
                    ];
                    let padded = std::iter::once(Some(17_i128))
                        .chain(inputs)
                        .chain(std::iter::once(Some(23_i128)))
                        .map(|value| value.map(i128::to_be_bytes));
                    let array: ArrayRef = Arc::new(
                        arrow::array::FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                            padded, 16,
                        )
                        .unwrap(),
                    );
                    let batch = batch_with_source(&program, array.slice(1, 5), None);
                    let all = [0, 1, 2, 3, 4];
                    let sparse = [0, 1, 4];
                    let empty = [];
                    for rows in [&all[..], &sparse[..], &empty[..]] {
                        let selection = if rows.len() == 5 {
                            Selection::all(5)
                        } else {
                            Selection::try_sparse(5, rows).unwrap()
                        };
                        let output = instance(&program)
                            .evaluate(
                                EvaluationInput::Batch {
                                    batch: &batch,
                                    source: None,
                                },
                                ScalarInvocationScope::new(selection, selection),
                                ScalarInvocationActivation::Activated,
                                &Control,
                                &mut PassThroughCallHost,
                            )
                            .unwrap();
                        let actual = output
                            .values()
                            .as_any()
                            .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
                            .unwrap();
                        assert_eq!(actual.data_type(), &DataType::FixedSizeBinary(16));
                        assert!(output.errors().is_empty());
                        assert_eq!(actual.len(), rows.len());
                        for (out_row, &source_row) in rows.iter().enumerate() {
                            let expected = if matches!(mode, Source::Constant) {
                                Some(i128::MIN)
                            } else {
                                inputs[source_row]
                            };
                            assert_eq!(actual.is_null(out_row), expected.is_none());
                            if let Some(value) = expected {
                                assert_eq!(actual.value(out_row), value.to_be_bytes().as_slice());
                            }
                        }
                    }
                }
            }
        }
    }
}
