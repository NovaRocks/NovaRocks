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

//! Actual compiled List CAST failure/admission witnesses, separate from raw oracles.
use super::observed_list_cast_profile_tests::{input, target_type};
use super::*;
use novarocks_expression::testkit::PassThroughCallHost;
use novarocks_expression::{EvaluationInput, ExprEvaluationFailure};
use novarocks_functions::{ScalarInvocationActivation, ScalarInvocationScope};
fn list_batch(program: &LocalProgram, source: ArrayRef) -> RecordBatch {
    let len = source.len();
    RecordBatch::try_new(
        program.graph().nodes()[1].output_layout().schema().clone(),
        vec![
            source,
            Arc::new(Int64Array::from(vec![42; len])),
            Arc::new(BooleanArray::from(vec![true; len])),
        ],
    )
    .unwrap()
}
#[test]
fn observed_list_cast_actual_compiler_every_callback_seven_causes_no_footer_or_replay() {
    for (null, id) in [(true, None), (false, Some("6")), (false, Some("7"))] {
        let source = input(null);
        let target = target_type(null, id);
        let program = compiled(
            FunctionValueType::new(source.data_type().clone(), true),
            FunctionValueType::new(target, true),
            Source::Column,
            Wrap::Bare,
            DecimalOverflowPolicy::ReportError,
            true,
        );
        let data = list_batch(&program, source);
        for rows in [vec![0, 1, 2, 3], vec![1, 3], Vec::new()] {
            let selection = Selection::try_sparse(4, &rows).unwrap();
            let record = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
            instance(&program)
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &data,
                        source: None,
                    },
                    ScalarInvocationScope::new(selection, selection),
                    ScalarInvocationActivation::Activated,
                    &record,
                    &mut PassThroughCallHost,
                )
                .unwrap();
            let trace = record.trace();
            assert!(trace.iter().all(|n| *n <= 256));
            for stop in 1..=trace.len() {
                for cause in causes() {
                    let mut frame = instance(&program);
                    let control = CallbackControl::new(cause.clone(), stop);
                    assert!(
                        matches!(frame.evaluate(EvaluationInput::Batch { batch: &data, source: None }, ScalarInvocationScope::new(selection, selection), ScalarInvocationActivation::Activated, &control, &mut PassThroughCallHost), Err(ExprEvaluationFailure::Kernel(actual)) if actual==cause)
                    );
                    assert_eq!(control.trace(), trace[..stop]);
                    let after = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
                    assert!(matches!(
                        frame.evaluate(
                            EvaluationInput::Batch {
                                batch: &data,
                                source: None
                            },
                            ScalarInvocationScope::new(selection, selection),
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
}
#[test]
fn observed_list_cast_actual_compiler_nonnullable_child_is_explicit_unsupported_shape_even_empty() {
    let source = input(true);
    let target = DataType::List(Arc::new(arrow::datatypes::Field::new(
        "element",
        DataType::Int32,
        false,
    )));
    let (functions, package) = fixture(
        FunctionValueType::new(source.data_type().clone(), true),
        FunctionValueType::new(target, true),
        Source::Column,
        Wrap::Bare,
        DecimalOverflowPolicy::OutputNull,
        false,
    );
    let error = compile(&functions, package, &Control).unwrap_err();
    assert!(
        error.to_string().contains("implemented exact Physical"),
        "{error}"
    );
    assert!(!matches!(error, FragmentCompileError::Control(_)));
}
