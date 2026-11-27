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

use super::stateful_call_fixtures::{GUARDED_DIAGNOSTICS, binary_program, binary_values};
use arrow::array::{ArrayRef, StringArray};
use novarocks_expression::testkit::CallbackControl;
use novarocks_expression::{
    CompiledExpressionInstance, EvaluationInput, ExprEvaluationFailure,
    testkit::{HarnessControl as Control, PassThroughCallHost, callback_causes},
};
use novarocks_functions::{
    DataFailureReason, KernelFailure, ScalarInvocationActivation, ScalarInvocationScope, Selection,
};
use std::sync::Arc;

const BYTE_FUNCTIONS: [&str; 3] = ["from_hex", "from_base64", "to_utf8"];

#[test]
fn byte_functions_compile_ordered_strict_calls_and_preserve_selected_values_and_data() {
    use novarocks_type_contract::{ArgumentControl, FunctionNullBehavior};

    let cases = [
        (
            "from_hex",
            [
                Some("fF"),
                Some(""),
                Some("ff00"),
                None,
                Some("00"),
                Some("!"),
            ],
            [
                Some(&b"\xff"[..]),
                Some(&b""[..]),
                Some(&b"\xff\0"[..]),
                None,
                Some(&b"\0"[..]),
                None,
            ],
            Some(5),
        ),
        (
            "from_base64",
            [
                Some("/w=="),
                Some(""),
                Some("/wA="),
                None,
                Some("AA=="),
                Some("!"),
            ],
            [
                Some(&b"\xff"[..]),
                Some(&b""[..]),
                Some(&b"\xff\0"[..]),
                None,
                Some(&b"\0"[..]),
                None,
            ],
            Some(5),
        ),
        (
            "to_utf8",
            [
                Some("é"),
                Some(""),
                Some("abc"),
                None,
                Some("中"),
                Some("\0"),
            ],
            [
                Some("é".as_bytes()),
                Some(&b""[..]),
                Some(&b"abc"[..]),
                None,
                Some("中".as_bytes()),
                Some(&b"\0"[..]),
            ],
            None,
        ),
    ];
    for (name, input, all_expected, malformed_row) in cases {
        let program = binary_program(name);
        let batch = program.batch(vec![Arc::new(StringArray::from(input.to_vec()))]);
        for call in program.unit.calls().values() {
            assert_eq!(
                call.call_contract().effects().null_behavior,
                FunctionNullBehavior::Strict
            );
            assert_eq!(
                call.call_contract().effects().argument_control,
                ArgumentControl::OrderedNull
            );
        }
        for selection in [
            Selection::all(6),
            Selection::try_sparse(6, &[0, 2, 3, 4]).unwrap(),
            Selection::try_sparse(6, &[0, 3, 5]).unwrap(),
            Selection::try_sparse(6, &[]).unwrap(),
        ] {
            let expected: Vec<_> = selection
                .iter()
                .map(|row| all_expected[row].map(<[u8]>::to_vec))
                .collect();
            let mut instance =
                CompiledExpressionInstance::try_new(program.unit.clone(), program.root, &Control)
                    .unwrap();
            let out = instance
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
            assert_eq!(binary_values(out.values()), expected);
            let expected_errors: Vec<_> = selection
                .iter()
                .enumerate()
                .filter_map(|(ordinal, row)| (malformed_row == Some(row)).then_some(ordinal))
                .collect();
            assert_eq!(
                out.errors()
                    .iter()
                    .map(|error| error.selected_ordinal())
                    .collect::<Vec<_>>(),
                expected_errors
            );
            assert!(out.errors().iter().all(|error| {
                error.reason()
                    == DataFailureReason::Domain(
                        novarocks_function_contract::kernel_failure::INVALID_FUNCTION_ARGUMENT,
                    )
            }));
        }
    }
}

#[test]
fn byte_functions_constructor_preserves_every_callback_cause() {
    for name in BYTE_FUNCTIONS {
        let program = binary_program(name);
        let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
        let _ = CompiledExpressionInstance::try_new(program.unit.clone(), program.root, &recorder)
            .unwrap();
        let trace = recorder.trace();
        assert!(!trace.is_empty());
        assert!(trace.iter().all(|n| *n <= 256));
        for index in 1..=trace.len() {
            for cause in callback_causes(GUARDED_DIAGNOSTICS) {
                let control = CallbackControl::new(cause.clone(), index);
                assert!(
                    matches!(CompiledExpressionInstance::try_new(program.unit.clone(),program.root,&control),Err(actual) if actual==cause)
                );
                assert_eq!(control.trace(), trace[..index]);
            }
        }
    }
}
#[test]
fn byte_functions_evaluation_preserves_every_callback_cause_and_latch() {
    for name in BYTE_FUNCTIONS {
        let program = binary_program(name);
        let long = "00ff".repeat(160);
        let input: ArrayRef = Arc::new(StringArray::from(
            (0..321)
                .map(|i| {
                    if i == 2 {
                        Some(long.as_str())
                    } else if i % 4 == 0 {
                        None
                    } else if i % 4 == 1 {
                        Some("")
                    } else {
                        Some("00")
                    }
                })
                .collect::<Vec<_>>(),
        ));
        let batch = program.batch(vec![input]);
        let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
        let mut evaluator =
            CompiledExpressionInstance::try_new(program.unit.clone(), program.root, &Control)
                .unwrap();
        let _ = evaluator
            .evaluate(
                EvaluationInput::Batch {
                    batch: &batch,
                    source: None,
                },
                ScalarInvocationScope::new(Selection::all(321), Selection::all(321)),
                ScalarInvocationActivation::Activated,
                &recorder,
                &mut PassThroughCallHost,
            )
            .unwrap();
        let trace = recorder.trace();
        assert!(trace.contains(&256));
        assert!(trace.iter().all(|n| *n <= 256));
        for index in 1..=trace.len() {
            for cause in callback_causes(GUARDED_DIAGNOSTICS) {
                let mut evaluator = CompiledExpressionInstance::try_new(
                    program.unit.clone(),
                    program.root,
                    &Control,
                )
                .unwrap();
                let control = CallbackControl::new(cause.clone(), index);
                assert!(
                    matches!(evaluator.evaluate(EvaluationInput::Batch { batch: &batch, source: None }, ScalarInvocationScope::new(Selection::all(321), Selection::all(321)), ScalarInvocationActivation::Activated, &control, &mut PassThroughCallHost), Err(ExprEvaluationFailure::Kernel(actual)) if actual==cause)
                );
                assert_eq!(control.trace(), trace[..index]);
                let after = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
                assert!(matches!(
                    evaluator.evaluate(
                        EvaluationInput::Batch {
                            batch: &batch,
                            source: None
                        },
                        ScalarInvocationScope::new(Selection::all(321), Selection::all(321)),
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
