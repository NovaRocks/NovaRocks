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

use arrow::{
    array::{Array, BooleanArray, Int32Array, Int64Array, ListArray},
    record_batch::RecordBatch,
};
use novarocks_expression::{
    CompiledFilterConjunctionInstance, EvaluationInput, ExprEvaluationFailure,
    testkit::{
        CallbackControl, CountingAllocator, PassThroughCallHost, callback_causes,
        frame_fixtures::{
            Control,
            array::{
                Guard, SourceMode, input, invocation_program, original_source_program,
                sliced_full_child_batch, tracked_instance,
            },
        },
    },
};
use novarocks_functions::{
    KernelFailure, ScalarInvocationActivation as Activation, ScalarInvocationScope, Selection,
};
use std::{convert::Infallible, sync::Arc};

fn data(
    error: ExprEvaluationFailure<Infallible>,
    message: &str,
) -> novarocks_functions::ScalarInvocationData {
    let ExprEvaluationFailure::Data(data) = error else {
        panic!("expected original atomic Data, got {error:?}")
    };
    assert_eq!(data.message(), message);
    data
}
#[test]
fn scalar_invocation_every_original_checkpoint_cause_has_no_tail_and_failed_reentry() {
    let program = invocation_program(3, Guard::Direct);
    let batch = input(&program, vec![Some(true); 320], false);
    let trace_control = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
    let (mut instance, _) = tracked_instance(&program);
    data(
        instance
            .evaluate(
                EvaluationInput::Batch {
                    batch: &batch,
                    source: None,
                },
                ScalarInvocationScope::new(Selection::all(320), Selection::all(320)),
                Activation::Activated,
                &trace_control,
                &mut PassThroughCallHost,
            )
            .unwrap_err(),
        "array_struct_subfield expects 2 to 2 arguments, got 3",
    );
    let trace = trace_control.trace();
    assert!(!trace.is_empty());
    assert!(trace.iter().all(|n| *n <= 256));
    for stop in 1..=trace.len() {
        for cause in callback_causes([
            "guarded-origin-invalid",
            "guarded-origin-internal",
            "guarded-origin-operational",
        ]) {
            let (mut instance, _) = tracked_instance(&program);
            let control = CallbackControl::new(cause.clone(), stop);
            assert!(
                matches!(instance.evaluate(EvaluationInput::Batch { batch: &batch, source: None }, ScalarInvocationScope::new(Selection::all(320), Selection::all(320)), Activation::Activated, &control, &mut PassThroughCallHost),Err(error) if matches!(&error, ExprEvaluationFailure::Kernel(actual) if actual==&cause))
            );
            assert_eq!(control.trace(), trace[..stop]);
            let after = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
            assert!(matches!(
                instance.evaluate(EvaluationInput::Batch { batch: &batch, source: None }, ScalarInvocationScope::new(Selection::all(320), Selection::all(320)), Activation::Activated, &after, &mut PassThroughCallHost),
                Err(error) if matches!(&error, ExprEvaluationFailure::Kernel(KernelFailure::InstanceFailed))
            ));
            assert!(after.trace().is_empty());
        }
    }
}

#[test]
fn scalar_invocation_case_insensitive_result_is_the_same_in_boolean_and_filter_contexts() {
    for (guard, expected) in [(Guard::And, false), (Guard::Or, true)] {
        let program = original_source_program("id", guard, SourceMode::Slot);
        let batch = input(&program, vec![Some(true); 3], false);
        let (mut instance, _) = tracked_instance(&program);
        let output = instance
            .evaluate(
                EvaluationInput::Batch {
                    batch: &batch,
                    source: None,
                },
                ScalarInvocationScope::new(Selection::all(3), Selection::all(3)),
                Activation::Activated,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        assert!(output.errors().is_empty());
        assert_eq!(
            output
                .values()
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(expected); 3]
        );
    }
    let program = original_source_program("id", Guard::Filter, SourceMode::Slot);
    let batch = input(&program, vec![Some(true); 3], false);
    let tracker = Arc::new(CountingAllocator::new(usize::MAX));
    let mut filter = CompiledFilterConjunctionInstance::try_new_with_allocator(
        &[(program.unit.clone(), program.root)],
        &Control,
        Some(tracker.clone()),
    )
    .unwrap();
    let output = filter
        .evaluate(
            EvaluationInput::Batch {
                batch: &batch,
                source: None,
            },
            Selection::all(3),
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert_eq!(
        output
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(false); 3]
    );
    drop(output);
    drop(filter);
    assert_eq!(tracker.outstanding(), 0);
}

#[test]
fn original_domain_computed_child_covers_actual_three_and_projects_only_outer_two() {
    let program = original_source_program("ID", Guard::Direct, SourceMode::Computed);
    let batch = input(&program, vec![Some(true); 6], false);
    let actual = [1, 3, 5];
    let demand = [3, 5];
    let (mut frame, _) = tracked_instance(&program);
    let output = frame
        .evaluate(
            EvaluationInput::Batch {
                batch: &batch,
                source: None,
            },
            novarocks_functions::ScalarInvocationScope::new(
                Selection::try_sparse(6, &actual).unwrap(),
                Selection::try_sparse(6, &demand).unwrap(),
            ),
            Activation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert_eq!(output.selection().iter().collect::<Vec<_>>(), vec![3, 5]);
    assert!(output.errors().is_empty());
    let list = output
        .values()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(list.value_offsets(), &[0, 1, 2]);
    assert_eq!(
        list.values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(7), Some(7)]
    );
}

#[test]
fn original_domain_case_insensitive_computed_child_projects_outer_demand_and_keeps_control_failure()
{
    let program = original_source_program("id", Guard::Direct, SourceMode::Computed);
    let batch = input(&program, vec![Some(true); 6], false);
    let actual = [1, 3, 5];
    let demand = [3, 5];
    let (mut frame, _) = tracked_instance(&program);
    let output = frame
        .evaluate(
            EvaluationInput::Batch {
                batch: &batch,
                source: None,
            },
            novarocks_functions::ScalarInvocationScope::new(
                Selection::try_sparse(6, &actual).unwrap(),
                Selection::try_sparse(6, &demand).unwrap(),
            ),
            Activation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert_eq!(output.selection().iter().collect::<Vec<_>>(), demand);
    assert!(output.errors().is_empty());
    assert_eq!(
        output
            .values()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[7, 7]
    );
    drop(output);
    let stop = CallbackControl::new(KernelFailure::DeadlineExceeded, 1);
    let error = frame
        .evaluate(
            EvaluationInput::Batch {
                batch: &batch,
                source: None,
            },
            ScalarInvocationScope::new(Selection::all(6), Selection::all(6)),
            Activation::Activated,
            &stop,
            &mut PassThroughCallHost,
        )
        .unwrap_err();
    assert_eq!(
        &error,
        &ExprEvaluationFailure::Kernel(KernelFailure::DeadlineExceeded)
    );
    assert_eq!(stop.trace().len(), 1);
    let after = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
    let error = frame
        .evaluate(
            EvaluationInput::Batch {
                batch: &batch,
                source: None,
            },
            ScalarInvocationScope::new(Selection::all(6), Selection::all(6)),
            Activation::Activated,
            &after,
            &mut PassThroughCallHost,
        )
        .unwrap_err();
    assert_eq!(
        &error,
        &ExprEvaluationFailure::Kernel(KernelFailure::InstanceFailed)
    );
    assert!(after.trace().is_empty());
}

#[test]
fn original_domain_if_real_all_true_and_all_false_keep_sliced_full_child_carrier() {
    for (mode, flags) in [
        (SourceMode::IfThen, vec![Some(true), Some(true)]),
        (SourceMode::IfElse, vec![Some(false), Some(false)]),
    ] {
        let program = original_source_program("ID", Guard::Direct, mode);
        let batch = sliced_full_child_batch(&program, flags);
        let (mut frame, _) = tracked_instance(&program);
        let output = frame
            .evaluate(
                EvaluationInput::Batch {
                    batch: &batch,
                    source: None,
                },
                ScalarInvocationScope::new(Selection::all(2), Selection::all(2)),
                Activation::Activated,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        let list = output
            .values()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(
            list.value_offsets(),
            &[2, 4, 5],
            "IF fast path must not turn the original slice into a take/interleave"
        );
        assert_eq!(
            list.values()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(10), Some(20), Some(30), Some(40), Some(50)]
        );
    }
}

#[test]
fn original_domain_if_actual_mixed_mask_filters_even_a_contiguous_true_branch() {
    let program = original_source_program("ID", Guard::Direct, SourceMode::IfThen);
    let batch = sliced_full_child_batch(&program, vec![Some(true), Some(false)]);
    let (mut frame, _) = tracked_instance(&program);
    let output = frame
        .evaluate(
            EvaluationInput::Batch {
                batch: &batch,
                source: None,
            },
            ScalarInvocationScope::new(Selection::all(2), Selection::all(2)),
            Activation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    let list = output
        .values()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(list.value_offsets(), &[0, 2, 2]);
    assert_eq!(
        list.iter().map(|row| row.is_some()).collect::<Vec<_>>(),
        vec![true, false]
    );
    assert_eq!(
        list.values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(30), Some(40)]
    );
}

#[test]
fn original_domain_nested_if_uses_its_immediate_filtered_parent_rows() {
    let success = original_source_program("ID", Guard::Direct, SourceMode::NestedIf);
    let original = input(
        &success,
        vec![Some(true), Some(false), Some(true), Some(true)],
        false,
    );
    let mut columns = original.columns().to_vec();
    columns[2] = Arc::new(Int64Array::from(vec![1, 1, 0, 1]));
    let batch = RecordBatch::try_new(original.schema(), columns).unwrap();
    let (mut frame, _) = tracked_instance(&success);
    let result = frame
        .evaluate(
            EvaluationInput::Batch {
                batch: &batch,
                source: None,
            },
            ScalarInvocationScope::new(Selection::all(4), Selection::all(4)),
            Activation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    let list = result
        .values()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(list.value_offsets(), &[0, 1, 1, 1, 2]);
    assert_eq!(
        list.iter().map(|row| row.is_some()).collect::<Vec<_>>(),
        vec![true, false, false, true]
    );
    assert_eq!(
        list.values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(7), Some(7)]
    );

    let program = original_source_program("id", Guard::IfThen, SourceMode::NestedIf);
    let original = input(
        &program,
        vec![Some(true), Some(false), Some(true), Some(true)],
        false,
    );
    let mut columns = original.columns().to_vec();
    columns[2] = Arc::new(Int64Array::from(vec![1, 1, 0, 1]));
    let batch = RecordBatch::try_new(original.schema(), columns).unwrap();
    let (mut frame, _) = tracked_instance(&program);
    let output = frame
        .evaluate(
            EvaluationInput::Batch {
                batch: &batch,
                source: None,
            },
            ScalarInvocationScope::new(Selection::all(4), Selection::all(4)),
            Activation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    // The outer IF consumes IS NULL(projected), so its result is Boolean.
    assert_eq!(
        output
            .values()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(false), None, Some(true), Some(false)]
    );
    assert!(output.errors().is_empty());
}
