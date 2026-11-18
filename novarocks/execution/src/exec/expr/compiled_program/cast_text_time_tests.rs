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
//! LocalCompiler text-to-TIME recipes checked against independent duration/clock tables.
use super::*;
use arrow::array::{
    BooleanArray, Int64Array, LargeStringArray, StringArray, StringViewArray,
    Time64MicrosecondArray,
};
use novarocks_expression::testkit::PassThroughCallHost;
use novarocks_expression::{EvaluationInput, ExprEvaluationFailure};
use novarocks_functions::{ScalarInvocationActivation, ScalarInvocationScope};
// These fixtures contain only authored input data. No retired evaluator is used.
fn input(dtype: &DataType, strings: &[Option<&str>]) -> ArrayRef {
    match dtype {
        DataType::Utf8 => Arc::new(StringArray::from(strings.to_vec())),
        DataType::LargeUtf8 => Arc::new(LargeStringArray::from(strings.to_vec())),
        DataType::Utf8View => Arc::new(StringViewArray::from(strings.to_vec())),
        _ => panic!("fixture requires one actual text carrier"),
    }
}
fn corpus(dtype: &DataType) -> ArrayRef {
    input(
        dtype,
        &[
            Some("00:00:00"),
            Some("25:00:00"),
            Some("+01:02:03"),
            Some("-01:02:03"),
            Some("01:02:03.456789"),
            Some("1"),
            Some("+1"),
            Some("-1"),
            Some("12:10"),
            Some("23:59:60"),
            Some("1970-01-01 01:01:01"),
            Some(" 12 : 34 : 56 "),
            Some(""),
            Some("9223372036854775807:00:00"),
            Some("2562047788015:00:00"),
            None,
        ],
    )
}
fn values(array: &ArrayRef) -> Vec<Option<i64>> {
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
fn expected(dtype: &DataType) -> Vec<Option<i64>> {
    match dtype {
        // UTF8 uses duration grammar: trim fields; unsigned integer hours are
        // unbounded; minutes/seconds are 0..59; checked seconds * 1_000_000.
        // Thus 25h is 90_000s and 12:34:56 is 45_296s. Signs, fractions,
        // incomplete clocks, leap seconds and overflowing durations are NULL.
        DataType::Utf8 => vec![
            Some(0),
            Some(90_000_000_000),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(45_296_000_000),
            None,
            None,
            None,
            None,
        ],
        // Arrow 58.4 Time64Microsecond parsing uses a wall clock, then an i64
        // raw-microsecond fallback. 01:02:03.456789 is 3_723_456_789us;
        // 12:10 is 43_800s and the leap second 23:59:60 is 86_400s.
        // Whole signed numbers are raw micros; whitespace and 25h are invalid.
        // These literals are derived from that grammar, never an evaluator run.
        DataType::LargeUtf8 | DataType::Utf8View => vec![
            Some(0),
            None,
            None,
            None,
            Some(3_723_456_789),
            Some(1),
            Some(1),
            Some(-1),
            Some(43_800_000_000),
            Some(86_400_000_000),
            None,
            None,
            None,
            None,
            None,
            None,
        ],
        _ => panic!("fixture requires one actual text carrier"),
    }
}
fn batch(program: &LocalProgram, source: ArrayRef) -> RecordBatch {
    let n = source.len();
    RecordBatch::try_new(
        program.graph().nodes()[1].output_layout().schema().clone(),
        vec![
            source,
            Arc::new(Int64Array::from(vec![42; n])),
            Arc::new(BooleanArray::from(vec![true; n])),
        ],
    )
    .unwrap()
}
fn check(dtype: DataType) {
    for allow in [false, true] {
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            let program = compiled(
                FunctionValueType::new(dtype.clone(), true),
                FunctionValueType::new(
                    DataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
                    true,
                ),
                Source::Column,
                Wrap::Bare,
                policy,
                allow,
            );
            let expected = expected(&dtype);
            for (source, expected) in [
                (corpus(&dtype), expected.as_slice()),
                (corpus(&dtype).slice(1, 7), &expected[1..8]),
                (corpus(&dtype).slice(0, 0), &expected[0..0]),
            ] {
                let batch = batch(&program, source.clone());
                for rows in [
                    (0..source.len()).collect::<Vec<_>>(),
                    (0..source.len()).filter(|row| row % 3 == 0).collect(),
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
                    assert!(output.errors().is_empty());
                    assert_eq!(
                        values(output.values()),
                        rows.iter().map(|row| expected[*row]).collect::<Vec<_>>()
                    );
                }
            }
        }
    }
}
#[test]
fn text_time_actual_compiler_utf8_exact_profile_all_policies_sparse_empty_slices() {
    check(DataType::Utf8);
}
#[test]
fn text_time_actual_compiler_large_utf8_exact_profile_all_policies_sparse_empty_slices() {
    check(DataType::LargeUtf8);
}
#[test]
fn text_time_actual_compiler_utf8_view_exact_profile_all_policies_sparse_empty_slices() {
    check(DataType::Utf8View);
}

#[test]
fn text_time_actual_compiler_all_seven_runtime_causes_stop_and_latch_without_replay() {
    for dtype in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
        let program = compiled(
            FunctionValueType::new(dtype.clone(), true),
            FunctionValueType::new(
                DataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
                true,
            ),
            Source::Column,
            Wrap::Bare,
            DecimalOverflowPolicy::ReportError,
            true,
        );
        let long = format!("{}:00:00", "0".repeat(777));
        let source = input(&dtype, &[Some(long.as_str()), None, Some("+01:02:03")]);
        let batch = batch(&program, source);
        let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
        assert!(
            instance(&program)
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &batch,
                        source: None
                    },
                    ScalarInvocationScope::new(Selection::all(3), Selection::all(3)),
                    ScalarInvocationActivation::Activated,
                    &recorder,
                    &mut PassThroughCallHost
                )
                .unwrap()
                .errors()
                .is_empty()
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

#[path = "cast_calendar_time_tests.rs"]
mod calendar_time_tests;
