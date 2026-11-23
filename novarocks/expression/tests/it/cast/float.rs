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
use novarocks_expression::testkit::PassThroughCallHost;
use novarocks_expression::testkit::numeric_fixtures::{compiled_cast, float_batch};
use novarocks_expression::{EvaluationInput, ExprEvaluationFailure};
use novarocks_failure_contract::kernel_reason::{INVALID_CAST_ARGUMENT, NUMERIC_OUT_OF_RANGE};
use novarocks_functions::{DataFailureReason, ScalarInvocationActivation, ScalarInvocationScope};
use novarocks_type_contract::TrinoCastMode;

#[test]
fn nonnullable_double_cast_preserves_failure_and_try_returns_null() {
    for target in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
    ] {
        for mode in [TrinoCastMode::Explicit, TrinoCastMode::Try] {
            let program = compiled_cast(
                FunctionValueType::new(DataType::Float64, false),
                FunctionValueType::new(target.clone(), mode == TrinoCastMode::Try),
                Source::Column,
                Wrap::Bare,
                mode,
                false,
            );
            let batch = float_batch(
                &program,
                &[
                    Some(126.5),
                    Some(f64::NAN),
                    Some(-127.5),
                    Some(f64::INFINITY),
                ],
            );
            let rows = [0, 1, 2, 3];
            let output = instance(&program)
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &batch,
                        source: None,
                    },
                    {
                        let selection = Selection::try_sparse(4, &rows).unwrap();
                        ScalarInvocationScope::new(selection, selection)
                    },
                    ScalarInvocationActivation::Activated,
                    &Control,
                    &mut PassThroughCallHost,
                )
                .unwrap();
            assert_eq!(
                signed_values(&output),
                vec![Some(127), None, Some(-128), None]
            );
            let expected_errors = match mode {
                TrinoCastMode::Explicit => vec![
                    (1, DataFailureReason::Domain(INVALID_CAST_ARGUMENT)),
                    (3, DataFailureReason::Domain(NUMERIC_OUT_OF_RANGE)),
                ],
                TrinoCastMode::Try => vec![],
                _ => unreachable!(),
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

#[test]
fn float_cast_runtime_every_original_callback_cause_latches_without_retry_including_error_rows() {
    let program = compiled_cast(
        FunctionValueType::new(DataType::Float64, true),
        FunctionValueType::new(DataType::Int8, true),
        Source::Column,
        Wrap::Bare,
        TrinoCastMode::Explicit,
        false,
    );
    let values = (0..320)
        .map(|row| match row % 4 {
            0 => Some(f64::NAN),
            1 => None,
            2 => Some(128.0),
            _ => Some(-7.9),
        })
        .collect::<Vec<_>>();
    let batch = float_batch(&program, &values);
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
    assert_eq!(output.errors().len(), 160);
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
