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

//! Actual LocalCompiler strict float-DATE profiles and native source/first-failure contracts.
use super::*;
use arrow::array::{Date32Array, Float32Array, Float64Array};
use novarocks_expression::testkit::PassThroughCallHost;
use novarocks_expression::{EvaluationInput, ExprEvaluationFailure};
use novarocks_functions::{ScalarInvocationActivation, ScalarInvocationScope};
// Pure authored input construction; conversion to f32 defines the source
// carrier, not the expected DATE. No old evaluator is borrowed.
fn input(wide: bool, values: Vec<Option<f64>>) -> ArrayRef {
    if wide {
        Arc::new(Float64Array::from(values))
    } else {
        Arc::new(Float32Array::from(
            values
                .into_iter()
                .map(|v| v.map(|v| v as f32))
                .collect::<Vec<_>>(),
        ))
    }
}
fn float_batch(program: &CompiledFixture, source: ArrayRef) -> RecordBatch {
    let n = source.len();
    let schema = program.input_schema().clone();
    RecordBatch::try_new(
        schema,
        vec![
            source,
            Arc::new(Int64Array::from(vec![42; n])),
            Arc::new(BooleanArray::from(vec![true; n])),
        ],
    )
    .unwrap()
}
fn source_type(wide: bool) -> DataType {
    if wide {
        DataType::Float64
    } else {
        DataType::Float32
    }
}

#[test]
fn float_date_actual_compiler_native_float_constant_negative_zero_is_original_error_not_null() {
    for wide in [false, true] {
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            for allow in [false, true] {
                // Existing actual fixture projects Literal(-0 F64) through the native constant author;
                // F32 also consumes its original explicit narrowing cast, never a guessed scalar ordinal.
                let program = compiled(
                    FunctionValueType::new(source_type(wide), false),
                    FunctionValueType::new(DataType::Date32, true),
                    Source::Constant,
                    Wrap::Bare,
                    policy,
                    allow,
                );
                let batch = float_batch(&program, input(wide, vec![Some(19700102.0); 4]));
                let rows = [1, 3];
                let selection = Selection::try_sparse(4, &rows).unwrap();
                let result = instance(&program)
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
                // Signed zero truncates to the integer date literal 0, which
                // has neither a valid month nor day; it is a row error, not a
                // successful NULL. This diagnostic follows the numeric grammar.
                let message = format!(
                    "CAST failed: from {:?} to Date32: invalid date literal 0",
                    source_type(wide)
                );
                assert_eq!(
                    result.values().to_data(),
                    Date32Array::from(vec![None, None]).to_data()
                );
                assert_eq!(result.errors().len(), 2);
                for (ordinal, error) in result.errors().iter().enumerate() {
                    assert_eq!(error.selected_ordinal(), ordinal);
                    assert_eq!(error.message(), message);
                }
            }
        }
    }
}
#[test]
fn float_date_actual_compiler_all_seven_runtime_causes_stop_and_latch_without_replay() {
    for wide in [false, true] {
        let program = compiled(
            FunctionValueType::new(source_type(wide), true),
            FunctionValueType::new(DataType::Date32, true),
            Source::Column,
            Wrap::Bare,
            DecimalOverflowPolicy::OutputNull,
            false,
        );
        let source = input(wide, vec![Some(19700102.0), Some(f64::NAN), None]);
        let batch = float_batch(&program, source);
        let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
        assert_eq!(
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
                .len(),
            1
        );
        let trace = recorder.trace();
        assert!(!trace.is_empty());
        assert!(trace.iter().all(|n| *n <= 256));
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
}
