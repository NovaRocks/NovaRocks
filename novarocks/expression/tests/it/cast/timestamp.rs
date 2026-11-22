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
use arrow::array::{
    StringArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow::datatypes::TimeUnit;
use novarocks_expression::testkit::{PassThroughCallHost, numeric_fixtures::compiled_cast};
use novarocks_expression::{EvaluationInput, ExprEvaluationFailure};
use novarocks_failure_contract::kernel_reason::INVALID_CAST_ARGUMENT;
use novarocks_functions::{
    CastPrepareError, DataFailureReason, PreparedCastRecipe, ScalarInvocationActivation,
    ScalarInvocationScope, SelectedValues,
};
use novarocks_type_contract::TrinoCastMode;

const MICROS: DataType = DataType::Timestamp(TimeUnit::Microsecond, None);
const NANOS: DataType = DataType::Timestamp(TimeUnit::Nanosecond, None);
const MODES: [TrinoCastMode; 2] = [TrinoCastMode::Explicit, TrinoCastMode::Try];

pub(super) fn timestamp_array(unit: TimeUnit, values: Vec<Option<i64>>) -> ArrayRef {
    match unit {
        TimeUnit::Second => Arc::new(TimestampSecondArray::from(values)),
        TimeUnit::Millisecond => Arc::new(TimestampMillisecondArray::from(values)),
        TimeUnit::Microsecond => Arc::new(TimestampMicrosecondArray::from(values)),
        TimeUnit::Nanosecond => Arc::new(TimestampNanosecondArray::from(values)),
    }
}

fn timestamp_values(output: &SelectedValues<'_>) -> Vec<Option<i64>> {
    output
        .values()
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap()
        .iter()
        .collect()
}

fn timestamp_batch(
    program: &CompiledFixture,
    source: ArrayRef,
    flags: &[Option<bool>],
) -> RecordBatch {
    let len = source.len();
    RecordBatch::try_new(
        program.input_schema().clone(),
        vec![
            source,
            Arc::new(Int64Array::from(vec![Some(42); len])),
            Arc::new(BooleanArray::from(flags.to_vec())),
        ],
    )
    .unwrap()
}

#[test]
fn timestamp_narrowing_rounds_signed_half_microseconds_on_selected_rows() {
    let input = [
        Some(99),
        Some(-1001),
        Some(-501),
        Some(-500),
        Some(-499),
        Some(0),
        Some(499),
        Some(500),
        Some(501),
        Some(1001),
        Some(i64::MIN),
        Some(i64::MAX),
        None,
    ];
    let expected = [
        Some(-1),
        Some(-1),
        Some(0),
        Some(0),
        Some(0),
        Some(0),
        Some(1),
        Some(1),
        Some(1),
        Some(-9_223_372_036_854_776),
        Some(9_223_372_036_854_776),
        None,
    ];
    let input = timestamp_array(TimeUnit::Nanosecond, input.to_vec()).slice(1, expected.len());
    for mode in MODES {
        let program = compiled_cast(
            FunctionValueType::new(NANOS, true),
            FunctionValueType::new(MICROS, true),
            Source::Column,
            Wrap::Bare,
            mode,
            false,
        );
        let batch = timestamp_batch(&program, input.clone(), &[Some(true); 12]);
        for rows in [
            &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11][..],
            &[0, 2, 4, 6, 9, 11],
            &[],
        ] {
            let selection = Selection::try_sparse(batch.num_rows(), rows).unwrap();
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
            assert_eq!(output.selection(), selection);
            assert_eq!(output.values().data_type(), &MICROS);
            assert_eq!(
                timestamp_values(&output),
                rows.iter().map(|&row| expected[row]).collect::<Vec<_>>()
            );
            assert!(output.errors().is_empty());
        }
    }
}

#[test]
fn timestamp_narrowing_preserves_required_constant_broadcast() {
    for mode in MODES {
        for (unit, expected) in [(TimeUnit::Microsecond, 71), (TimeUnit::Nanosecond, 0)] {
            let program = compiled_cast(
                FunctionValueType::new(DataType::Timestamp(unit, None), false),
                FunctionValueType::new(MICROS, false),
                Source::Constant,
                Wrap::Bare,
                mode,
                false,
            );
            let source = timestamp_array(unit, vec![Some(99); 5]);
            let batch = timestamp_batch(&program, source, &[Some(true); 5]);
            let selection = Selection::try_sparse(batch.num_rows(), &[1, 4]).unwrap();
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
            assert_eq!(output.selection(), selection);
            assert_eq!(timestamp_values(&output), [Some(expected); 2]);
            assert!(output.errors().is_empty());
        }
    }
}

#[test]
fn timestamp_precision_pairs_without_a_recipe_remain_unsupported() {
    let units = [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ];
    for source in units {
        for target in units {
            if source == target || (source, target) == (TimeUnit::Nanosecond, TimeUnit::Microsecond)
            {
                continue;
            }
            for mode in MODES {
                assert!(
                    matches!(
                        PreparedCastRecipe::try_new_trino(
                            &FunctionValueType::new(DataType::Timestamp(source, None), false),
                            &FunctionValueType::new(DataType::Timestamp(target, None), true),
                            mode,
                            &Control,
                        ),
                        Err(CastPrepareError::Unsupported)
                    ),
                    "{mode:?} {source:?} -> {target:?}"
                );
            }
        }
    }
}

#[test]
fn timestamp_text_cast_errors_and_successful_nulls_survive_control_wrappers() {
    for mode in MODES {
        for wrap in [Wrap::Bare, Wrap::IsNull, Wrap::If, Wrap::Coalesce] {
            let program = compiled_cast(
                FunctionValueType::new(DataType::Utf8, true),
                FunctionValueType::new(MICROS, true),
                Source::Column,
                wrap,
                mode,
                false,
            );
            let source = Arc::new(StringArray::from(vec![
                Some("bad"),
                None,
                Some("1970-01-01 00:00:00.000001"),
                Some("bad"),
            ]));
            let batch = timestamp_batch(
                &program,
                source,
                &[Some(true), Some(true), Some(true), Some(false)],
            );
            let selection = Selection::all(4);
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
            let expected_errors = match (mode, wrap) {
                (TrinoCastMode::Try, _) => vec![],
                (_, Wrap::If) => vec![0],
                _ => vec![0, 3],
            };
            assert_eq!(errors(&output), expected_errors);
            assert!(
                output
                    .errors()
                    .iter()
                    .all(|e| e.reason() == DataFailureReason::Domain(INVALID_CAST_ARGUMENT))
            );
            let tried = mode == TrinoCastMode::Try;
            if matches!(wrap, Wrap::IsNull) {
                let values = output
                    .values()
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap();
                let invalid = tried.then_some(true);
                assert_eq!(
                    values.iter().collect::<Vec<_>>(),
                    [invalid, Some(true), Some(false), invalid]
                );
            } else {
                assert_eq!(
                    timestamp_values(&output),
                    match wrap {
                        Wrap::If => [None, None, Some(1), Some(71)],
                        Wrap::Coalesce if tried => [Some(71), Some(71), Some(1), Some(71)],
                        Wrap::Coalesce => [None, Some(71), Some(1), None],
                        _ => [None, None, Some(1), None],
                    }
                );
            }
        }
    }
}

#[test]
fn timestamp_cast_runtime_checkpoints_keep_original_failures_and_latch() {
    let program = compiled_cast(
        FunctionValueType::new(DataType::Utf8, true),
        FunctionValueType::new(MICROS, true),
        Source::Column,
        Wrap::Coalesce,
        TrinoCastMode::Explicit,
        false,
    );
    let source = Arc::new(StringArray::from_iter(
        [Some("bad"), None]
            .into_iter()
            .chain(std::iter::repeat_n(Some("1970-01-01 00:00:00.000001"), 318)),
    ));
    let batch = timestamp_batch(&program, source, &[Some(true); 320]);
    let selection = Selection::all(320);
    let scope = ScalarInvocationScope::new(selection, selection);
    let input = EvaluationInput::Batch {
        batch: &batch,
        source: None,
    };
    let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
    let output = instance(&program)
        .evaluate(
            input,
            scope,
            ScalarInvocationActivation::Activated,
            &recorder,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert_eq!(errors(&output), [0]);
    assert_eq!(&timestamp_values(&output)[..3], &[None, Some(71), Some(1)]);
    let trace = recorder.trace();
    assert!(trace.contains(&256));
    assert!(trace.iter().all(|units| *units <= 256));
    for stop_at in 1..=trace.len() {
        for cause in causes() {
            let control = CallbackControl::new(cause.clone(), stop_at);
            let mut evaluator = instance(&program);
            assert!(matches!(
                evaluator.evaluate(input, scope, ScalarInvocationActivation::Activated, &control, &mut PassThroughCallHost),
                Err(ExprEvaluationFailure::Kernel(actual)) if actual == cause
            ));
            assert_eq!(control.trace(), trace[..stop_at]);
            let after = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
            assert!(matches!(
                evaluator.evaluate(
                    input,
                    scope,
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
