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
//! Compiled VARCHAR-to-TIME(6) conversion through the standard cast modes.
use super::*;
use arrow::array::{StringArray, Time64MicrosecondArray};
use arrow::datatypes::TimeUnit;
use novarocks_expression::testkit::{PassThroughCallHost, numeric_fixtures::compiled_cast};
use novarocks_expression::{EvaluationInput, ExprEvaluationFailure};
use novarocks_failure_contract::kernel_reason::INVALID_CAST_ARGUMENT;
use novarocks_functions::{DataFailureReason, ScalarInvocationActivation, ScalarInvocationScope};
use novarocks_type_contract::TrinoCastMode;

const TIME: DataType = DataType::Time64(TimeUnit::Microsecond);

pub(super) fn values(array: &ArrayRef) -> Vec<Option<i64>> {
    assert_eq!(
        array.data_type(),
        &DataType::Time64(arrow::datatypes::TimeUnit::Microsecond)
    );
    array
        .as_any()
        .downcast_ref::<Time64MicrosecondArray>()
        .unwrap()
        .iter()
        .collect()
}
pub(super) fn batch(program: &CompiledFixture, source: ArrayRef) -> RecordBatch {
    let n = source.len();
    RecordBatch::try_new(
        program.input_schema().clone(),
        vec![
            source,
            Arc::new(Int64Array::from(vec![42; n])),
            Arc::new(BooleanArray::from(vec![true; n])),
        ],
    )
    .unwrap()
}
#[test]
fn varchar_to_time_keeps_civil_values_and_selected_cast_errors() {
    // The successful values are microseconds since midnight.
    let cases = [
        (Some("00:00:00"), Some(0)),
        (Some("25:00:00"), None),
        (Some("+01:02:03"), None),
        (Some("-01:02:03"), None),
        (Some("01:02:03.456789"), Some(3_723_456_789)),
        (Some("1"), None),
        (Some("+1"), None),
        (Some("-1"), None),
        (Some("12:10"), Some(43_800_000_000)),
        (Some("23:59:60"), None),
        (Some("1970-01-01 01:01:01"), None),
        (Some(" 12 : 34 : 56 "), None),
        (Some(""), None),
        (Some("9223372036854775807:00:00"), None),
        (Some("2562047788015:00:00"), None),
        (None, None),
        (Some("23:59:59.9999995"), Some(0)),
        (Some("01:02:03.4567895"), Some(3_723_456_790)),
        (Some("01:02:03  "), Some(3_723_000_000)),
    ];
    let full: ArrayRef = Arc::new(StringArray::from_iter(cases.iter().map(|(text, _)| *text)));
    for mode in [TrinoCastMode::Explicit, TrinoCastMode::Try] {
        let program = compiled_cast(
            FunctionValueType::new(DataType::Utf8, true),
            FunctionValueType::new(TIME, true),
            Source::Column,
            Wrap::Bare,
            mode,
            false,
        );
        for (source, expected) in [
            (full.clone(), cases.as_slice()),
            (full.slice(1, 7), &cases[1..8]),
            (full.slice(0, 0), &cases[..0]),
        ] {
            let batch = batch(&program, source.clone());
            for rows in [
                (0..source.len()).collect::<Vec<_>>(),
                (0..source.len()).filter(|row| row % 3 == 0).collect(),
                Vec::new(),
            ] {
                let selection = Selection::try_sparse(source.len(), &rows).unwrap();
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
                assert_eq!(
                    values(output.values()),
                    rows.iter().map(|&row| expected[row].1).collect::<Vec<_>>()
                );
                let expected_errors = if mode == TrinoCastMode::Explicit {
                    rows.iter()
                        .enumerate()
                        .filter_map(|(ordinal, &row)| {
                            let (text, value) = expected[row];
                            (text.is_some() && value.is_none()).then_some((
                                ordinal,
                                DataFailureReason::Domain(INVALID_CAST_ARGUMENT),
                            ))
                        })
                        .collect::<Vec<_>>()
                } else {
                    Vec::new()
                };
                assert_eq!(
                    output
                        .errors()
                        .iter()
                        .map(|error| (error.selected_ordinal(), error.reason()))
                        .collect::<Vec<_>>(),
                    expected_errors
                );
            }
        }
    }
}

#[test]
fn text_time_actual_compiler_all_seven_runtime_causes_stop_and_latch_without_replay() {
    for mode in [TrinoCastMode::Explicit, TrinoCastMode::Try] {
        let program = compiled_cast(
            FunctionValueType::new(DataType::Utf8, true),
            FunctionValueType::new(TIME, true),
            Source::Column,
            Wrap::Bare,
            mode,
            false,
        );
        let long = format!("{}:00:00", "0".repeat(64 * 270));
        let source = Arc::new(StringArray::from(vec![
            Some(long.as_str()),
            None,
            Some("+01:02:03"),
        ]));

        let batch = batch(&program, source);
        let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
        let output = instance(&program)
            .evaluate(
                EvaluationInput::Batch {
                    batch: &batch,
                    source: None,
                },
                ScalarInvocationScope::new(Selection::all(3), Selection::all(3)),
                ScalarInvocationActivation::Activated,
                &recorder,
                &mut PassThroughCallHost,
            )
            .unwrap();
        assert_eq!(
            errors(&output),
            if mode == TrinoCastMode::Explicit {
                vec![0, 2]
            } else {
                vec![]
            }
        );
        let trace = recorder.trace();
        assert!(trace.contains(&256));
        assert!(trace.iter().all(|units| *units <= 256));
        for at in 1..=trace.len() {
            for cause in causes() {
                let mut evaluator = instance(&program);
                let control = CallbackControl::new(cause.clone(), at);
                assert!(
                    matches!(evaluator.evaluate(EvaluationInput::Batch { batch: &batch, source: None }, ScalarInvocationScope::new(Selection::all(3), Selection::all(3)), ScalarInvocationActivation::Activated, &control, &mut PassThroughCallHost), Err(ExprEvaluationFailure::Kernel(actual)) if actual == cause)
                );
                assert_eq!(control.trace(), trace[..at]);
                let after = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
                assert!(matches!(
                    evaluator.evaluate(
                        EvaluationInput::Batch {
                            batch: &batch,
                            source: None
                        },
                        ScalarInvocationScope::new(Selection::all(3), Selection::all(3)),
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
}
