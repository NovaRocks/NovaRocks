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
//! LocalCompiler calendar-to-TIME profiles checked against independent calendar arithmetic.
use super::*;
use arrow::array::{
    Date32Array, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow::datatypes::TimeUnit;
use novarocks_expression::testkit::PassThroughCallHost;
use novarocks_expression::{EvaluationInput, ExprEvaluationFailure};
use novarocks_functions::{ScalarInvocationActivation, ScalarInvocationScope};
fn calendar_corpus(dtype: &DataType) -> ArrayRef {
    if dtype == &DataType::Date32 {
        return Arc::new(Date32Array::from(vec![
            Some(i32::MIN),
            Some(i32::MAX),
            Some(-1),
            Some(0),
            Some(1),
            Some(7),
            Some(-7),
            None,
        ]));
    }
    let DataType::Timestamp(unit, zone) = dtype else {
        panic!("exact calendar profile")
    };
    let multiplier = match unit {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => 1_000,
        TimeUnit::Microsecond => 1_000_000,
        TimeUnit::Nanosecond => 1_000_000_000,
    };
    let values = vec![
        Some(i64::MIN),
        Some(i64::MAX),
        Some(-1),
        Some(0),
        Some(1),
        Some(86_401 * multiplier),
        Some(-86_401 * multiplier),
        None,
    ];
    match unit {
        TimeUnit::Second => {
            Arc::new(TimestampSecondArray::from(values).with_timezone_opt(zone.clone()))
        }
        TimeUnit::Millisecond => {
            Arc::new(TimestampMillisecondArray::from(values).with_timezone_opt(zone.clone()))
        }
        TimeUnit::Microsecond => {
            Arc::new(TimestampMicrosecondArray::from(values).with_timezone_opt(zone.clone()))
        }
        TimeUnit::Nanosecond => {
            Arc::new(TimestampNanosecondArray::from(values).with_timezone_opt(zone.clone()))
        }
    }
}
fn calendar_expected(dtype: &DataType) -> Vec<Option<i64>> {
    // Date-only carriers denote midnight even outside Chrono's date range.
    // Timestamp micros are split by Euclidean division into whole seconds and
    // subsecond residue, then TIME drops that residue. Thus -1ms/-1us denotes
    // the previous day's 23:59:59; timezone metadata does not move the clock.
    // Second/millisecond extrema overflow checked micros, and microsecond
    // extrema exceed the supported calendar range: each becomes NULL.
    match dtype {
        DataType::Date32 => vec![
            Some(0),
            Some(0),
            Some(0),
            Some(0),
            Some(0),
            Some(0),
            Some(0),
            None,
        ],
        DataType::Timestamp(TimeUnit::Second, _) => vec![
            None,
            None,
            Some(86_399_000_000),
            Some(0),
            Some(1_000_000),
            Some(1_000_000),
            Some(86_399_000_000),
            None,
        ],
        DataType::Timestamp(TimeUnit::Millisecond | TimeUnit::Microsecond, _) => vec![
            None,
            None,
            Some(86_399_000_000),
            Some(0),
            Some(0),
            Some(1_000_000),
            Some(86_399_000_000),
            None,
        ],
        // Nanos first truncate toward zero to micros: -1ns becomes zero. At
        // i64 extrema the whole seconds are -9_223_372_037/+9_223_372_036;
        // modulo 86_400 gives 763/85_636 seconds after midnight. Both dates
        // are in range. No runtime cast or old evaluator produces this table.
        DataType::Timestamp(TimeUnit::Nanosecond, _) => vec![
            Some(763_000_000),
            Some(85_636_000_000),
            Some(0),
            Some(0),
            Some(0),
            Some(1_000_000),
            Some(86_399_000_000),
            None,
        ],
        _ => panic!("exact calendar profile"),
    }
}
fn calendar_check(dtype: DataType) {
    for allow in [false, true] {
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            let program = compiled(
                FunctionValueType::new(dtype.clone(), true),
                FunctionValueType::new(DataType::Time64(TimeUnit::Microsecond), true),
                Source::Column,
                Wrap::Bare,
                policy,
                allow,
            );
            let expected = calendar_expected(&dtype);
            for (source, expected) in [
                (calendar_corpus(&dtype), expected.as_slice()),
                (calendar_corpus(&dtype).slice(1, 6), &expected[1..7]),
                (calendar_corpus(&dtype).slice(0, 0), &expected[0..0]),
            ] {
                let batch = batch(&program, source.clone());
                for rows in [
                    (0..source.len()).collect::<Vec<_>>(),
                    (0..source.len()).filter(|r| r % 3 == 0).collect(),
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
                        rows.iter().map(|r| expected[*r]).collect::<Vec<_>>()
                    );
                }
            }
        }
    }
}
fn calendar_timestamp_check(unit: TimeUnit) {
    for zone in [
        None,
        Some("UTC".into()),
        Some("Asia/Shanghai".into()),
        Some("opaque-original-zone".into()),
    ] {
        calendar_check(DataType::Timestamp(unit, zone));
    }
}
#[test]
fn calendar_time_actual_compiler_date() {
    calendar_check(DataType::Date32);
}
#[test]
fn calendar_time_actual_compiler_second() {
    calendar_timestamp_check(TimeUnit::Second);
}
#[test]
fn calendar_time_actual_compiler_millisecond() {
    calendar_timestamp_check(TimeUnit::Millisecond);
}
#[test]
fn calendar_time_actual_compiler_microsecond() {
    calendar_timestamp_check(TimeUnit::Microsecond);
}
#[test]
fn calendar_time_actual_compiler_nanosecond() {
    calendar_timestamp_check(TimeUnit::Nanosecond);
}
#[test]
fn calendar_time_actual_compiler_every_cause_stops_at_actual_callback_and_latches() {
    for dtype in [
        DataType::Date32,
        DataType::Timestamp(TimeUnit::Microsecond, None),
    ] {
        let program = compiled(
            FunctionValueType::new(dtype.clone(), true),
            FunctionValueType::new(DataType::Time64(TimeUnit::Microsecond), true),
            Source::Column,
            Wrap::Bare,
            DecimalOverflowPolicy::ReportError,
            true,
        );
        let batch = batch(&program, calendar_corpus(&dtype));
        let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
        assert!(
            instance(&program)
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &batch,
                        source: None
                    },
                    {
                        let selection = Selection::all(batch.num_rows());
                        ScalarInvocationScope::new(selection, selection)
                    },
                    ScalarInvocationActivation::Activated,
                    &recorder,
                    &mut PassThroughCallHost
                )
                .is_ok()
        );
        let trace = recorder.trace();
        assert!(!trace.is_empty());
        assert!(trace.iter().all(|n| *n <= 256));
        for at in 1..=trace.len() {
            for cause in [
                KernelFailure::Cancelled,
                KernelFailure::DeadlineExceeded,
                KernelFailure::ResourceExhausted,
                KernelFailure::InvalidProgram(KernelDiagnostic::new("calendar invalid")),
                KernelFailure::Internal(KernelDiagnostic::new("calendar internal")),
                KernelFailure::Operational(KernelDiagnostic::new("calendar host")),
                KernelFailure::InstanceFailed,
            ] {
                let control = CallbackControl::new(cause.clone(), at);
                let mut runtime = instance(&program);
                assert_eq!(
                    runtime
                        .evaluate(
                            EvaluationInput::Batch {
                                batch: &batch,
                                source: None
                            },
                            {
                                let selection = Selection::all(batch.num_rows());
                                ScalarInvocationScope::new(selection, selection)
                            },
                            ScalarInvocationActivation::Activated,
                            &control,
                            &mut PassThroughCallHost
                        )
                        .unwrap_err(),
                    ExprEvaluationFailure::Kernel(cause)
                );
                assert_eq!(control.trace(), trace[..at]);
                let saved = control.trace();
                assert_eq!(
                    runtime
                        .evaluate(
                            EvaluationInput::Batch {
                                batch: &batch,
                                source: None
                            },
                            {
                                let selection = Selection::all(batch.num_rows());
                                ScalarInvocationScope::new(selection, selection)
                            },
                            ScalarInvocationActivation::Activated,
                            &control,
                            &mut PassThroughCallHost
                        )
                        .unwrap_err(),
                    ExprEvaluationFailure::Kernel(KernelFailure::InstanceFailed)
                );
                assert_eq!(control.trace(), saved);
            }
        }
    }
}
#[test]
fn calendar_time_exact_successful_null_fact_retains_nonnullable_date_and_nano_domains() {
    use novarocks_functions::{CastOperation, CastPrepareError, PreparedCastRecipe};
    for dtype in [
        DataType::Date32,
        DataType::Timestamp(TimeUnit::Nanosecond, None),
    ] {
        assert!(!novarocks_functions::carrier_cast_can_produce_null(
            &dtype,
            &DataType::Time64(TimeUnit::Microsecond),
            false
        ));
        assert!(
            PreparedCastRecipe::try_new(
                CastOperation::Carrier,
                &FunctionValueType::new(dtype, false),
                &FunctionValueType::new(DataType::Time64(TimeUnit::Microsecond), false),
                DecimalOverflowPolicy::ReportError,
                false,
                &Control
            )
            .is_ok()
        );
    }
    for unit in [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
    ] {
        let dtype = DataType::Timestamp(unit, None);
        assert!(novarocks_functions::carrier_cast_can_produce_null(
            &dtype,
            &DataType::Time64(TimeUnit::Microsecond),
            false
        ));
        assert_eq!(
            PreparedCastRecipe::try_new(
                CastOperation::Carrier,
                &FunctionValueType::new(dtype, false),
                &FunctionValueType::new(DataType::Time64(TimeUnit::Microsecond), false),
                DecimalOverflowPolicy::ReportError,
                false,
                &Control
            ),
            Err(CastPrepareError::TypeMismatch)
        );
    }
}
