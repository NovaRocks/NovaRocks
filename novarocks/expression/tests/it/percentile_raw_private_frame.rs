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
//! Original private percentile call and its funded evaluator behavior.

use super::stateful_call_fixtures::{
    GUARDED_DIAGNOSTICS, NoTail, RefusingHost, assert_fixture_capacity,
    capacity_refusing_allocator, percentile_arrays, percentile_batch, percentile_program,
};
use arrow::array::Array;
use novarocks_expression::testkit::CountingAllocator;
use novarocks_expression::{
    CompiledExpressionInstance, EvaluationInput, ExprEvaluationFailure,
    testkit::{HarnessControl as Control, PassThroughCallHost, callback_causes},
};
use novarocks_functions::{
    KernelFailure, ScalarInvocationActivation, ScalarInvocationScope, Selection,
};
use std::sync::Arc;

#[test]
fn percentile_raw_private_frame_original_values_full_rowdata_sparse_and_host_drop() {
    let program = percentile_program();
    let batch = percentile_batch(&program);
    let allocator = Arc::new(CountingAllocator::new(4 * 1024 * 1024));
    let mut instance = CompiledExpressionInstance::try_new_with_allocator(
        program.unit.clone(),
        program.root,
        &Control,
        Some(allocator.clone()),
    )
    .unwrap();
    let output = instance
        .evaluate(
            EvaluationInput::Batch {
                batch: &batch,
                source: None,
            },
            ScalarInvocationScope::new(Selection::all(3), Selection::all(3)),
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert_eq!(output.errors().len(), 1);
    assert_eq!(output.errors()[0].selected_ordinal(), 0);
    let arrays = percentile_arrays();
    let expected = novarocks_functions::percentile_approx_raw_core::row(&arrays[0], 0, &arrays[1])
        .unwrap_err();
    assert_eq!(output.errors()[0].message(), expected.to_string());
    let values = output
        .values()
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    assert!(values.is_null(0));
    assert!(values.is_null(1));
    assert_eq!(
        values.value(2).to_bits(),
        novarocks_functions::percentile_approx_raw_core::row(&arrays[0], 2, &arrays[1])
            .unwrap()
            .unwrap()
            .to_bits()
    );
    drop(output);
    let selected = [2];
    let output = instance
        .evaluate(
            EvaluationInput::Batch {
                batch: &batch,
                source: None,
            },
            {
                let selection = Selection::try_sparse(3, &selected).unwrap();
                ScalarInvocationScope::new(selection, selection)
            },
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert!(output.errors().is_empty());
    assert_eq!(output.selection().row(0), Some(2));
    drop(output);
    let output = instance
        .evaluate(
            EvaluationInput::Batch {
                batch: &batch,
                source: None,
            },
            {
                let selection = Selection::try_sparse(3, &[]).unwrap();
                ScalarInvocationScope::new(selection, selection)
            },
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert_eq!(output.values().len(), 0);
    drop(output);
    drop(instance);
    assert_eq!(allocator.outstanding(), 0);
}
#[test]
fn percentile_raw_private_frame_missing_host_and_true_budget_failure_latch() {
    let program = percentile_program();
    let batch = percentile_batch(&program);
    let mut no_host =
        CompiledExpressionInstance::try_new(program.unit.clone(), program.root, &Control).unwrap();
    assert!(matches!(
        no_host.evaluate(
            EvaluationInput::Batch {
                batch: &batch,
                source: None
            },
            ScalarInvocationScope::new(Selection::all(3), Selection::all(3)),
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost
        ),
        Err(ExprEvaluationFailure::Kernel(
            KernelFailure::InvalidProgram(_)
        ))
    ));
    let allocator = capacity_refusing_allocator(1);
    let mut instance = CompiledExpressionInstance::try_new_with_allocator(
        program.unit.clone(),
        program.root,
        &Control,
        Some(allocator.clone()),
    )
    .unwrap();
    let refusal = instance
        .evaluate(
            EvaluationInput::Batch {
                batch: &batch,
                source: None,
            },
            ScalarInvocationScope::new(Selection::all(3), Selection::all(3)),
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap_err();
    assert_fixture_capacity(&refusal, 1);
    assert!(matches!(
        instance.evaluate(
            EvaluationInput::Batch {
                batch: &batch,
                source: None
            },
            ScalarInvocationScope::new(Selection::all(3), Selection::all(3)),
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost
        ),
        Err(ExprEvaluationFailure::Kernel(KernelFailure::InstanceFailed))
    ));
    drop(instance);
    assert_eq!(allocator.outstanding(), 0);
}
#[test]
fn percentile_raw_private_frame_constructor_host_seven_causes_have_no_footer() {
    for cause in callback_causes(GUARDED_DIAGNOSTICS) {
        for fail_metadata in [false, true] {
            let p = percentile_program();
            let batch = percentile_batch(&p);
            let allocator = Arc::new(CountingAllocator::new(2 * 1024 * 1024));
            let authority = Arc::new(RefusingHost {
                actual: allocator.clone(),
                cause: cause.clone(),
                stop: if fail_metadata { usize::MAX } else { 0 },
                fail_metadata,
                requests: std::sync::atomic::AtomicUsize::new(0),
                refused: std::sync::atomic::AtomicBool::new(false),
            });
            let mut instance = CompiledExpressionInstance::try_new_with_allocator(
                p.unit.clone(),
                p.root,
                &Control,
                Some(authority.clone()),
            )
            .unwrap();
            let control = NoTail(&authority, "percentile scalar never waits");
            assert!(
                matches!(instance.evaluate(EvaluationInput::Batch { batch: &batch, source: None }, ScalarInvocationScope::new(Selection::all(3), Selection::all(3)), ScalarInvocationActivation::Activated, &control, &mut PassThroughCallHost), Err(ExprEvaluationFailure::Kernel(actual)) if actual==cause)
            );
            assert!(matches!(
                instance.evaluate(
                    EvaluationInput::Batch {
                        batch: &batch,
                        source: None
                    },
                    ScalarInvocationScope::new(Selection::all(3), Selection::all(3)),
                    ScalarInvocationActivation::Activated,
                    &control,
                    &mut PassThroughCallHost
                ),
                Err(ExprEvaluationFailure::Kernel(KernelFailure::InstanceFailed))
            ));
            drop(instance);
            assert_eq!(allocator.outstanding(), 0);
        }
    }
}
