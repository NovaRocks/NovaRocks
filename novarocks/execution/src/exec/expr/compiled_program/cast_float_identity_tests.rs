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

fn bits(array: &ArrayRef) -> Vec<Option<u64>> {
    match array.data_type() {
        DataType::Float32 => array
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .iter()
            .map(|v| v.map(|v| u64::from(v.to_bits())))
            .collect(),
        DataType::Float64 => array
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .iter()
            .map(|v| v.map(f64::to_bits))
            .collect(),
        _ => panic!("exact float result carrier"),
    }
}

fn bit_batch(program: &LocalProgram, source: &DataType, values: &[Option<u64>]) -> RecordBatch {
    let mut padded = vec![Some(0)];
    padded.extend_from_slice(values);
    padded.push(Some(0));
    let array: ArrayRef = match source {
        DataType::Float32 => Arc::new(Float32Array::from(
            padded
                .iter()
                .map(|v| v.map(|v| f32::from_bits(u32::try_from(v).unwrap())))
                .collect::<Vec<_>>(),
        )),
        DataType::Float64 => Arc::new(Float64Array::from(
            padded
                .iter()
                .map(|v| v.map(f64::from_bits))
                .collect::<Vec<_>>(),
        )),
        _ => panic!("actual frozen source"),
    };
    RecordBatch::try_new(
        program.graph().nodes()[1].output_layout().schema().clone(),
        vec![
            array.slice(1, values.len()),
            Arc::new(Int64Array::from(vec![Some(42); values.len()])),
            Arc::new(BooleanArray::from(vec![Some(true); values.len()])),
        ],
    )
    .unwrap()
}

// Literal IEEE payloads include quiet/signaling NaNs. Same-width identity must
// not round-trip through another float width. Cross-width expectations are the
// native conversion on this platform. Finite answers are fixed independent bit
// tables; cross-width NaN payloads follow native as rather than a portable rule.
fn payloads(source: &DataType, target: &DataType) -> (Vec<Option<u64>>, Vec<Option<u64>>) {
    let source_bits = if source == &DataType::Float32 {
        vec![
            0, 0x80000000, 0x7fc12345, 0xffc54321, 0x7f812345, 0x7f800000, 0xff800000, 1,
            0x80000001, 0x00800000, 0x7f7fffff, 0x40600000,
        ]
    } else {
        vec![
            0,
            0x8000000000000000,
            0x7ff8123456789abc,
            0xfff8543212345678,
            0x7ff0123456789abc,
            0x7ff0000000000000,
            0xfff0000000000000,
            1,
            0x8000000000000001,
            0x0010000000000000,
            0x7fefffffffffffff,
            0x400c000000000000,
        ]
    };
    let mut expected_bits = if source == target {
        source_bits.clone()
    } else if source == &DataType::Float32 {
        vec![
            0,
            0x8000000000000000,
            0, // NaN payloads are filled from native conversion below.
            0,
            0,
            0x7ff0000000000000,
            0xfff0000000000000,
            0x36a0000000000000,
            0xb6a0000000000000,
            0x3810000000000000,
            0x47efffffe0000000,
            0x400c000000000000,
        ]
    } else {
        vec![
            0, 0x80000000, 0, 0, 0, 0x7f800000, 0xff800000, 0, 0x80000000, 0, 0x7f800000,
            0x40600000,
        ]
    };
    if source != target {
        for (row, original) in source_bits.iter().enumerate() {
            if let Some(native) = native_cross_nan_bits(source, *original) {
                expected_bits[row] = native;
            }
        }
    }
    let mut source = source_bits.into_iter().map(Some).collect::<Vec<_>>();
    source.push(None);
    let mut expected = expected_bits.into_iter().map(Some).collect::<Vec<_>>();
    expected.push(None);
    (source, expected)
}

fn native_cross_nan_bits(source: &DataType, bits: u64) -> Option<u64> {
    match source {
        DataType::Float32 => {
            let value = f32::from_bits(u32::try_from(bits).unwrap());
            value.is_nan().then(|| (value as f64).to_bits())
        }
        DataType::Float64 => {
            let value = f64::from_bits(bits);
            value.is_nan().then(|| u64::from((value as f32).to_bits()))
        }
        _ => panic!("exact native float source"),
    }
}

fn root_recipe(
    program: &LocalProgram,
) -> (
    &novarocks_functions::PreparedCastRecipe,
    ExpressionEffectContext,
) {
    let occurrence = cast_root_use(&program);
    (
        root_unit(&program).recipes().cast(occurrence).unwrap(),
        root_unit(&program).control_flow().uses()[&occurrence].context,
    )
}

#[test]
fn float_identity_four_profiles_preserve_ieee_bits_slice_sparse_null_and_both_policies() {
    for source in [DataType::Float32, DataType::Float64] {
        for target in [DataType::Float32, DataType::Float64] {
            let (values, expected) = payloads(&source, &target);
            let rows = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12];
            let selection = Selection::try_sparse(values.len(), &rows).unwrap();
            for policy in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                for allow in [false, true] {
                    let program = compiled(
                        FunctionValueType::new(source.clone(), true),
                        FunctionValueType::new(target.clone(), true),
                        Source::Column,
                        Wrap::Bare,
                        policy,
                        allow,
                    );
                    let (recipe, context) = root_recipe(&program);
                    assert!(
                        !recipe
                            .own_effects(context)
                            .for_use(context)
                            .unwrap()
                            .may_raise_row_error
                    );
                    let batch = bit_batch(&program, &source, &values);
                    let mut evaluator = instance(&program);
                    assert!(
                        evaluator
                            .evaluate(
                                &batch,
                                Selection::try_sparse(values.len(), &[]).unwrap(),
                                &Control
                            )
                            .unwrap()
                            .values()
                            .is_empty()
                    );
                    for _ in 0..2 {
                        let output = evaluator.evaluate(&batch, selection, &Control).unwrap();
                        assert_eq!(output.selection(), selection);
                        assert_eq!(output.values().data_type(), &target);
                        assert_eq!(
                            bits(output.values()),
                            rows.map(|row| expected[row]).to_vec()
                        );
                        assert!(output.errors().is_empty());
                    }
                    assert!(evaluator.instances.is_empty());
                }
            }
        }
    }
}

#[test]
fn float_identity_actual_nonnullable_float_sources_and_constant_broadcast_keep_signed_zero() {
    // The F32 producer is an actual F64-to-F32 Cast, not a retagged literal.
    for source in [DataType::Float32, DataType::Float64] {
        for target in [DataType::Float32, DataType::Float64] {
            for allow in [false, true] {
                for mode in [Source::Column, Source::Constant] {
                    let program = compiled(
                        FunctionValueType::new(source.clone(), false),
                        FunctionValueType::new(target.clone(), false),
                        mode,
                        Wrap::Bare,
                        DecimalOverflowPolicy::ReportError,
                        allow,
                    );
                    let values = if source == DataType::Float32 {
                        [
                            Some(0x80000000),
                            Some(0x7f800000),
                            Some(0x7fc12345),
                            Some(0x40600000),
                        ]
                    } else {
                        [
                            Some(0x8000000000000000),
                            Some(0x7ff0000000000000),
                            Some(0x7ff8123456789abc),
                            Some(0x400c000000000000),
                        ]
                    };
                    let batch = bit_batch(&program, &source, &values);
                    let rows = [0, 2, 3];
                    let selection = Selection::try_sparse(4, &rows).unwrap();
                    let mut evaluator = instance(&program);
                    assert!(
                        evaluator
                            .evaluate(&batch, Selection::try_sparse(4, &[]).unwrap(), &Control)
                            .unwrap()
                            .values()
                            .is_empty()
                    );
                    for _ in 0..2 {
                        let output = evaluator.evaluate(&batch, selection, &Control).unwrap();
                        let expected = if matches!(mode, Source::Constant) {
                            vec![
                                Some(if target == DataType::Float32 {
                                    0x80000000
                                } else {
                                    0x8000000000000000
                                });
                                3
                            ]
                        } else if source == target {
                            rows.map(|row| values[row]).to_vec()
                        } else if target == DataType::Float32 {
                            vec![
                                Some(0x80000000),
                                native_cross_nan_bits(&source, values[2].unwrap()),
                                Some(0x40600000),
                            ]
                        } else {
                            vec![
                                Some(0x8000000000000000),
                                native_cross_nan_bits(&source, values[2].unwrap()),
                                Some(0x400c000000000000),
                            ]
                        };
                        assert_eq!(bits(output.values()), expected);
                        assert_eq!(output.selection(), selection);
                        assert_eq!(output.values().null_count(), 0);
                        assert!(output.errors().is_empty());
                    }
                }
            }
        }
    }
}

#[test]
fn float_identity_nonfinite_remains_successful_value_under_real_coalesce_and_isnull() {
    for source in [DataType::Float32, DataType::Float64] {
        for target in [DataType::Float32, DataType::Float64] {
            let (values, expected) = payloads(&source, &target);
            let rows = [2, 5, 6, 10, 12];
            let selection = Selection::try_sparse(values.len(), &rows).unwrap();
            for wrap in [Wrap::Coalesce, Wrap::IsNull] {
                let program = compiled(
                    FunctionValueType::new(source.clone(), true),
                    FunctionValueType::new(target.clone(), true),
                    Source::Column,
                    wrap,
                    DecimalOverflowPolicy::ReportError,
                    true,
                );
                let batch = bit_batch(&program, &source, &values);
                let output = instance(&program)
                    .evaluate(&batch, selection, &Control)
                    .unwrap();
                assert!(output.errors().is_empty());
                if matches!(wrap, Wrap::IsNull) {
                    assert_eq!(
                        output
                            .values()
                            .as_any()
                            .downcast_ref::<BooleanArray>()
                            .unwrap()
                            .iter()
                            .collect::<Vec<_>>(),
                        vec![
                            Some(false),
                            Some(false),
                            Some(false),
                            Some(false),
                            Some(true)
                        ]
                    );
                } else {
                    let mut expected = rows.map(|row| expected[row]).to_vec();
                    expected[4] = Some(if target == DataType::Float32 {
                        0x428e0000
                    } else {
                        0x4051c00000000000
                    });
                    assert_eq!(bits(output.values()), expected);
                }
            }
        }
    }
}

#[test]
fn float_identity_pure_own_effect_keeps_real_round_child_errors_and_compact_journal() {
    for target in [DataType::Float32, DataType::Float64] {
        for allow in [false, true] {
            let (functions, package) = inherited_round_fixture_with_target(allow, target.clone());
            let program = compile(&functions, package, &Control).unwrap();
            let (recipe, context) = root_recipe(&program);
            assert!(
                !recipe
                    .own_effects(context)
                    .for_use(context)
                    .unwrap()
                    .may_raise_row_error
            );
            let mut evaluator = instance(&program);
            let occurrence = context.use_id;
            let summary = evaluator.effects[&occurrence];
            assert!(summary.for_use(context).unwrap().may_raise_row_error);
            let raw = 10_i128.pow(38) - 1;
            let batch = RecordBatch::try_new(
                program.graph().nodes()[1].output_layout().schema().clone(),
                vec![
                    Arc::new(Float64Array::from(vec![
                        Some(7.9),
                        Some(f64::INFINITY),
                        Some(128.0),
                        None,
                        Some(-7.9),
                    ])),
                    Arc::new(
                        Decimal128Array::from(vec![
                            Some(raw),
                            Some(raw),
                            Some(10),
                            None,
                            Some(raw),
                        ])
                        .with_precision_and_scale(38, 0)
                        .unwrap(),
                    ),
                ],
            )
            .unwrap();
            let rows = [1, 2, 3, 4];
            let selection = Selection::try_sparse(5, &rows).unwrap();
            let output = evaluator.evaluate(&batch, selection, &Control).unwrap();
            assert_eq!(errors(&output), vec![0, 3]);
            assert_eq!(
                bits(output.values()),
                vec![
                    None,
                    Some(if target == DataType::Float32 {
                        0x43000000
                    } else {
                        0x4060000000000000
                    }),
                    None,
                    None
                ]
            );
            assert_eq!(output.selection(), selection);
            assert_eq!(output.errors()[0].message(), output.errors()[1].message());
        }
    }
}

#[test]
fn float_identity_original_compile_and_runtime_refusals_keep_exact_prefix_and_failed_latch() {
    let (functions, package) = fixture(
        FunctionValueType::new(DataType::Float64, true),
        FunctionValueType::new(DataType::Float32, true),
        Source::Column,
        Wrap::Coalesce,
        DecimalOverflowPolicy::ReportError,
        true,
    );
    let recorder = CompileCallbacks::new(CompileControlError::Cancelled, usize::MAX);
    let program = compile(&functions, package.clone(), &recorder).unwrap();
    let compile_trace = recorder.trace.lock().unwrap().clone();
    for stop_at in 1..=compile_trace.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let refusal = CompileCallbacks::new(cause.clone(), stop_at);
            assert!(
                matches!(compile(&functions, package.clone(), &refusal), Err(FragmentCompileError::Control(actual)) if actual == cause)
            );
            assert_eq!(*refusal.trace.lock().unwrap(), compile_trace[..stop_at]);
        }
    }
    let values = (0..320)
        .map(|row| match row % 4 {
            0 => Some(f64::INFINITY),
            1 => None,
            2 => Some(f64::MAX),
            _ => Some(-0.0),
        })
        .collect::<Vec<_>>();
    let batch = float_batch(&program, &values);
    let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
    let output = instance(&program)
        .evaluate(&batch, Selection::all(320), &recorder)
        .unwrap();
    assert_eq!(output.values().null_count(), 0);
    assert!(output.errors().is_empty());
    let trace = recorder.trace.lock().unwrap().clone();
    assert!(trace.contains(&256));
    assert!(trace.iter().all(|units| *units <= 256));
    for stop_at in 1..=trace.len() {
        for cause in causes() {
            let mut evaluator = instance(&program);
            let refusal = CallbackControl::new(cause.clone(), stop_at);
            assert!(
                matches!(evaluator.evaluate(&batch, Selection::all(320), &refusal), Err(actual) if actual == cause)
            );
            assert_eq!(*refusal.trace.lock().unwrap(), trace[..stop_at]);
            let after = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
            assert!(matches!(
                evaluator.evaluate(&batch, Selection::all(320), &after),
                Err(KernelFailure::InstanceFailed)
            ));
            assert!(after.trace.lock().unwrap().is_empty());
        }
    }
}
