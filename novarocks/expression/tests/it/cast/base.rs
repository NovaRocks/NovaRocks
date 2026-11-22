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
use arrow::array::Float32Array;
use novarocks_expression::testkit::PassThroughCallHost;
use novarocks_expression::testkit::numeric_fixtures::{cast_fixture_in_mode, compiled_cast};
use novarocks_expression::{EvaluationInput, ExprEvaluationFailure};
use novarocks_functions::{ScalarInvocationActivation, ScalarInvocationScope};
use novarocks_type_contract::TrinoCastMode;

#[test]
fn truthful_nonnullable_widening_and_try_narrowing_have_different_null_contracts() {
    // The current Physical scalar literal author can produce a nonempty I64
    // source. I8(false) source production needs the later CV/channel slice;
    // neither a retagged I64 literal nor a NULL(false) proves it here.
    let values = [
        Some(i64::MIN),
        Some(-128),
        Some(0),
        Some(127),
        Some(i64::MAX),
    ];
    for target in [DataType::Int64, DataType::Float32, DataType::Float64] {
        let program = compiled_cast(
            FunctionValueType::new(DataType::Int64, false),
            FunctionValueType::new(target.clone(), false),
            Source::Column,
            Wrap::Bare,
            TrinoCastMode::Implicit,
            false,
        );
        let batch = sliced_batch(&program, &values);
        let output = instance(&program)
            .evaluate(
                EvaluationInput::Batch {
                    batch: &batch,
                    source: None,
                },
                {
                    let selection = Selection::all(values.len());
                    ScalarInvocationScope::new(selection, selection)
                },
                ScalarInvocationActivation::Activated,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        assert_eq!(output.values().null_count(), 0);
        match target {
            DataType::Int64 => assert_eq!(signed_values(&output), values.to_vec()),
            DataType::Float32 => {
                let actual = output
                    .values()
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .unwrap();
                for (row, value) in values.iter().enumerate() {
                    assert_eq!(
                        actual.value(row).to_bits(),
                        (value.unwrap() as f32).to_bits()
                    );
                }
            }
            DataType::Float64 => {
                let actual = output
                    .values()
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                for (row, value) in values.iter().enumerate() {
                    assert_eq!(
                        actual.value(row).to_bits(),
                        (value.unwrap() as f64).to_bits()
                    );
                }
            }
            _ => unreachable!(),
        }
    }
    // An out-of-range explicit cast raises an error, while TRY_CAST can return NULL.
    let error = match cast_fixture_in_mode(
        FunctionValueType::new(DataType::Int64, false),
        FunctionValueType::new(DataType::Int8, false),
        Source::Column,
        Wrap::Bare,
        TrinoCastMode::Try,
        false,
    ) {
        Err(error) => error,
        Ok(_) => panic!("narrowing cast was admitted"),
    };
    assert!(!matches!(
        error,
        novarocks_expression::CallAuthorError::Control(_)
    ));
    assert!(matches!(
        error,
        novarocks_expression::CallAuthorError::Capability(_)
    ));
}

#[test]
fn signed_store_assignment_reports_overflow_and_keeps_source_null() {
    use novarocks_failure_contract::kernel_reason::NUMERIC_OUT_OF_RANGE;
    use novarocks_functions::DataFailureReason;

    let program = compiled_cast(
        FunctionValueType::new(DataType::Int64, true),
        FunctionValueType::new(DataType::Int32, true),
        Source::Column,
        Wrap::Bare,
        TrinoCastMode::StoreAssignment,
        false,
    );
    let batch = sliced_batch(&program, &[Some(1), Some(2_147_483_648), None]);
    let selection = Selection::all(3);
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
    assert_eq!(signed_values(&output), [Some(1), None, None]);
    assert_eq!(errors(&output), [1]);
    assert_eq!(
        output.errors()[0].reason(),
        DataFailureReason::Domain(NUMERIC_OUT_OF_RANGE)
    );
}

#[test]
fn cast_joins_real_required_child_errors_and_guarded_control_never_turns_them_into_successful_null()
{
    for wrap in [
        Wrap::Bare,
        Wrap::IsNull,
        Wrap::Coalesce,
        Wrap::If,
        Wrap::And,
        Wrap::Or,
    ] {
        let program = compiled_cast(
            FunctionValueType::new(DataType::Int64, true),
            FunctionValueType::new(DataType::Int8, true),
            Source::Add,
            wrap,
            TrinoCastMode::Try,
            false,
        );
        let batch = input(
            &program,
            &[Some(1), Some(i64::MAX), None, Some(7), Some(i64::MAX)],
            &[Some(42); 5],
            &[None, Some(true), Some(true), Some(true), Some(false)],
        );
        let rows = [1, 2, 3, 4];
        let selection = Selection::try_sparse(5, &rows).unwrap();
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
        match wrap {
            Wrap::And | Wrap::Or => {
                assert!(output.errors().is_empty());
                let values = output
                    .values()
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap();
                assert_eq!(
                    values.iter().collect::<Vec<_>>(),
                    vec![Some(matches!(wrap, Wrap::Or)); 4]
                );
            }
            Wrap::If => {
                assert_eq!(errors(&output), vec![0]);
                assert_eq!(signed_values(&output), vec![None, None, Some(8), Some(71)]);
            }
            Wrap::Coalesce => {
                assert_eq!(errors(&output), vec![0, 3]);
                assert_eq!(signed_values(&output), vec![None, Some(71), Some(8), None]);
            }
            Wrap::Bare => {
                assert_eq!(errors(&output), vec![0, 3]);
                assert_eq!(signed_values(&output), vec![None, None, Some(8), None]);
            }
            Wrap::IsNull => {
                assert_eq!(errors(&output), vec![0, 3]);
                let values = output
                    .values()
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap();
                assert!(values.is_null(0));
                assert!(values.value(1));
                assert!(!values.value(2));
                assert!(values.is_null(3));
            }
            _ => unreachable!(),
        }
        for error in output.errors() {
            assert_eq!(
                error.message(),
                "Arithmetic overflow: Overflow happened on: 9223372036854775807 + 1"
            );
        }
    }
}

#[test]
fn cast_runtime_all_original_failure_categories_stop_at_exact_callback_and_latch_without_replay() {
    let program = compiled_cast(
        FunctionValueType::new(DataType::Int64, true),
        FunctionValueType::new(DataType::Int8, true),
        Source::Column,
        Wrap::Coalesce,
        TrinoCastMode::Explicit,
        true,
    );
    let source = (0..320)
        .map(|row| match row % 3 {
            0 => Some(130),
            1 => None,
            _ => Some(-7),
        })
        .collect::<Vec<_>>();
    let batch = sliced_batch(&program, &source);
    let constructor = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
    CompiledExpressionInstance::try_new(root_unit(&program).clone(), program.root, &constructor)
        .unwrap();
    let constructor_trace = constructor.trace();
    for stop_at in 1..=constructor_trace.len() {
        for cause in causes() {
            let control = CallbackControl::new(cause.clone(), stop_at);
            assert!(
                matches!(CompiledExpressionInstance::try_new(root_unit(&program).clone(), program.root, &control), Err(actual) if actual == cause)
            );
            assert_eq!(control.trace(), constructor_trace[..stop_at]);
        }
    }
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
    assert_eq!(errors(&output), (0..320).step_by(3).collect::<Vec<_>>());
    let trace = recorder.trace();
    assert!(trace.contains(&256));
    assert!(trace.iter().all(|units| *units <= 256));
    for stop_at in 1..=trace.len() {
        for cause in causes() {
            let mut evaluator = instance(&program);
            let control = CallbackControl::new(cause.clone(), stop_at);
            assert!(
                matches!(evaluator.evaluate(EvaluationInput::Batch { batch: &batch, source: None }, ScalarInvocationScope::new(Selection::all(320), Selection::all(320)), ScalarInvocationActivation::Activated, &control, &mut PassThroughCallHost), Err(ExprEvaluationFailure::Kernel(actual)) if actual == cause)
            );
            assert_eq!(control.trace(), trace[..stop_at]);
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
