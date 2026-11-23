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
//! Compiled unzoned TIMESTAMP(6) to TIME(6) preserves subsecond precision.
use super::text_time::{batch, values};
use super::*;
use arrow::array::TimestampMicrosecondArray;
use arrow::datatypes::TimeUnit;
use novarocks_expression::testkit::{PassThroughCallHost, numeric_fixtures::compiled_cast};
use novarocks_expression::{EvaluationInput, ExprEvaluationFailure};
use novarocks_functions::{
    CastPrepareError, PreparedCastRecipe, ScalarInvocationActivation, ScalarInvocationScope,
};
use novarocks_type_contract::TrinoCastMode;

const TIMESTAMP: DataType = DataType::Timestamp(TimeUnit::Microsecond, None);
const TIME: DataType = DataType::Time64(TimeUnit::Microsecond);
const MODES: [TrinoCastMode; 2] = [TrinoCastMode::Explicit, TrinoCastMode::Try];

fn timestamp_to_time_program(mode: TrinoCastMode) -> CompiledFixture {
    compiled_cast(
        FunctionValueType::new(TIMESTAMP, true),
        FunctionValueType::new(TIME, true),
        Source::Column,
        Wrap::Bare,
        mode,
        false,
    )
}

#[test]
fn timestamp_to_time_retains_subseconds_on_each_side_of_the_epoch() {
    let inputs = [
        Some(i64::MIN),
        Some(i64::MAX),
        Some(-1),
        Some(0),
        Some(1),
        Some(86_401_000_000),
        Some(-86_401_000_000),
        None,
    ];
    // Euclidean day remainders retain the fractional part. In particular one
    // microsecond before the epoch is the last microsecond of the previous day.
    let expected = [
        Some(71_945_224_192),
        Some(14_454_775_807),
        Some(86_399_999_999),
        Some(0),
        Some(1),
        Some(1_000_000),
        Some(86_399_000_000),
        None,
    ];
    let source: ArrayRef = Arc::new(TimestampMicrosecondArray::from_iter(inputs));
    for mode in MODES {
        let program = timestamp_to_time_program(mode);
        for (source, fixed) in [
            (source.clone(), expected.as_slice()),
            (source.slice(1, 7), &expected[1..]),
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
                assert!(output.errors().is_empty());
                assert_eq!(
                    values(output.values()),
                    rows.iter().map(|&row| fixed[row]).collect::<Vec<_>>()
                );
            }
        }
    }
}

#[test]
fn date_to_time_is_rejected_in_both_cast_modes() {
    for mode in MODES {
        assert!(matches!(
            PreparedCastRecipe::try_new_trino(
                &FunctionValueType::new(DataType::Date32, true),
                &FunctionValueType::new(TIME, true),
                mode,
                &Control,
            ),
            Err(CastPrepareError::Illegal)
        ));
    }
}

#[test]
fn calendar_time_actual_compiler_every_cause_stops_at_actual_callback_and_latches() {
    for mode in MODES {
        let program = timestamp_to_time_program(mode);
        let source = Arc::new(TimestampMicrosecondArray::from(vec![
            Some(-1),
            Some(1),
            None,
        ]));
        let batch = batch(&program, source);
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
            for cause in causes() {
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
