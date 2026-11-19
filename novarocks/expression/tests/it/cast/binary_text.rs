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

//! Actual LocalCompiler Binary-to-Utf8 CAST, native constant source and first-failure contracts.
use super::*;
use arrow::array::{BinaryArray, StringArray};
use novarocks_expression::testkit::PassThroughCallHost;
use novarocks_expression::{EvaluationInput, ExprEvaluationFailure};
use novarocks_functions::{ScalarInvocationActivation, ScalarInvocationScope};
// Literal bytes and UTF8 validity determine these independent values: NUL
// and Unicode survive, an empty sequence is empty text, and 0xff is invalid.
fn input() -> ArrayRef {
    Arc::new(
        BinaryArray::from(vec![
            Some(b"pad".as_slice()),
            Some("你好\0🙂".as_bytes()),
            Some(b"".as_slice()),
            Some(b"\xffa".as_slice()),
            None,
            Some(b"tail".as_slice()),
        ])
        .slice(1, 4),
    )
}
fn binary_batch(program: &CompiledFixture, source: ArrayRef) -> RecordBatch {
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
fn binary_text_actual_compiler_full_profile_all_policies_source_nullability_slice_empty_selected() {
    for nullable in [false, true] {
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            for allow in [false, true] {
                let source = if nullable {
                    input()
                } else {
                    input().slice(0, 3)
                };
                let program = compiled(
                    FunctionValueType::new(DataType::Binary, nullable),
                    FunctionValueType::new(DataType::Utf8, true),
                    Source::Column,
                    Wrap::Bare,
                    policy,
                    allow,
                );
                let batch = binary_batch(&program, source.clone());
                let rows = if nullable { vec![0, 2, 3] } else { vec![0, 2] };
                let selected = Selection::try_sparse(source.len(), &rows).unwrap();
                let result = instance(&program)
                    .evaluate(
                        EvaluationInput::Batch {
                            batch: &batch,
                            source: None,
                        },
                        ScalarInvocationScope::new(selected, selected),
                        ScalarInvocationActivation::Activated,
                        &Control,
                        &mut PassThroughCallHost,
                    )
                    .unwrap();
                assert!(result.errors().is_empty());
                let values = [Some("你好\0🙂"), Some(""), None, None];
                let expected = selected.iter().map(|row| values[row]).collect::<Vec<_>>();
                assert_eq!(
                    result.values().to_data(),
                    StringArray::from(expected).to_data()
                );
                let empty = [];
                let result = instance(&program)
                    .evaluate(
                        EvaluationInput::Batch {
                            batch: &batch,
                            source: None,
                        },
                        {
                            let selection = Selection::try_sparse(source.len(), &empty).unwrap();
                            ScalarInvocationScope::new(selection, selection)
                        },
                        ScalarInvocationActivation::Activated,
                        &Control,
                        &mut PassThroughCallHost,
                    )
                    .unwrap();
                assert_eq!(result.values().data_type(), &DataType::Utf8);
                assert_eq!(result.values().len(), 0);
                assert!(result.errors().is_empty());
                let empty_source = source.slice(0, 0);
                let batch = binary_batch(&program, empty_source);
                let result = instance(&program)
                    .evaluate(
                        EvaluationInput::Batch {
                            batch: &batch,
                            source: None,
                        },
                        ScalarInvocationScope::new(Selection::all(0), Selection::all(0)),
                        ScalarInvocationActivation::Activated,
                        &Control,
                        &mut PassThroughCallHost,
                    )
                    .unwrap();
                assert_eq!(result.values().len(), 0);
            }
        }
    }
}
#[test]
fn binary_text_actual_compiler_native_binary_constant_uses_real_emitted_bytes() {
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        for allow in [false, true] {
            let program = compiled(
                FunctionValueType::new(DataType::Binary, false),
                FunctionValueType::new(DataType::Utf8, true),
                Source::Constant,
                Wrap::Bare,
                policy,
                allow,
            );
            let source: ArrayRef = Arc::new(BinaryArray::from(vec![b"ignored".as_slice(); 4]));
            let batch = binary_batch(&program, source);
            let rows = [1, 3];
            let result = instance(&program)
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
            assert!(result.errors().is_empty());
            assert_eq!(
                result.values().to_data(),
                StringArray::from(vec![Some("native\0producer"), Some("native\0producer")])
                    .to_data()
            );
        }
    }
}
#[test]
fn binary_text_actual_compiler_all_seven_runtime_causes_stop_and_latch_after_actual_byte_quanta() {
    let program = compiled(
        FunctionValueType::new(DataType::Binary, true),
        FunctionValueType::new(DataType::Utf8, true),
        Source::Column,
        Wrap::Bare,
        DecimalOverflowPolicy::OutputNull,
        false,
    );
    let long = vec![b'a'; 777];
    let source: ArrayRef = Arc::new(BinaryArray::from(vec![
        Some(long.as_slice()),
        Some(b"\xff".as_slice()),
        None,
    ]));
    let batch = binary_batch(&program, source);
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
    for at in 1..=trace.len() {
        for cause in causes() {
            let mut evaluator = instance(&program);
            let control = CallbackControl::new(cause.clone(), at);
            assert!(
                matches!(evaluator.evaluate(EvaluationInput::Batch { batch: &batch, source: None }, ScalarInvocationScope::new(Selection::all(3), Selection::all(3)), ScalarInvocationActivation::Activated, &control, &mut PassThroughCallHost), Err(ExprEvaluationFailure::Kernel(actual)) if actual==cause)
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
