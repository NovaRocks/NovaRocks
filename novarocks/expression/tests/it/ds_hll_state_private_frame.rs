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
//! Original selected HLL call recipes and their funded evaluator behavior.

use super::stateful_call_fixtures::{
    DsCatalogue, GUARDED_DIAGNOSTICS, NoTail, RefusingHost, assert_fixture_capacity,
    capacity_refusing_allocator, ds_batch, ds_if_batch, ds_if_program, ds_if_row_data_batch,
    ds_if_type, ds_program,
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
fn ds_scalar_private_adapter_frame_every_declared_arity_true_host_and_no_host_refusal() {
    use arrow::array::BinaryArray;
    for count in 1..=3 {
        let p = ds_program(DsCatalogue::Private, count);
        let batch = ds_batch(&p, count);
        let mut no_host =
            CompiledExpressionInstance::try_new(p.unit.clone(), p.root, &Control).unwrap();
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
        let allocator = Arc::new(CountingAllocator::new(2 * 1024 * 1024));
        let mut instance = CompiledExpressionInstance::try_new_with_allocator(
            p.unit.clone(),
            p.root,
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
        let bytes = output
            .values()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert!(bytes.is_null(1));
        assert!(output.errors().is_empty());
        assert_eq!(bytes.value(0)[3], if count == 1 { 17 } else { 10 });
        assert!(allocator.outstanding() > 0);
        drop(output);
        drop(instance);
        assert_eq!(allocator.outstanding(), 0);
    }
}
#[test]
fn ds_scalar_private_adapter_frame_real_tracker_refusal_and_failure_latch() {
    let p = ds_program(DsCatalogue::Private, 1);
    let batch = ds_batch(&p, 1);
    let allocator = capacity_refusing_allocator(1);
    let mut instance = CompiledExpressionInstance::try_new_with_allocator(
        p.unit.clone(),
        p.root,
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
    assert_eq!(allocator.outstanding(), 0);
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
fn ds_scalar_private_adapter_frame_sparse_original_row_identity_and_empty_selection() {
    let p = ds_program(DsCatalogue::Private, 2);
    let batch = ds_batch(&p, 2);
    let allocator = Arc::new(CountingAllocator::new(2 * 1024 * 1024));
    let mut instance = CompiledExpressionInstance::try_new_with_allocator(
        p.unit.clone(),
        p.root,
        &Control,
        Some(allocator.clone()),
    )
    .unwrap();
    let rows = [2];
    let selection = Selection::try_sparse(3, &rows).unwrap();
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
    assert!(out.errors().is_empty());
    assert_eq!(out.selection().row(0), Some(2));
    let before = allocator.outstanding();
    drop(out);
    let out = instance
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
    assert_eq!(out.values().len(), 0);
    assert_eq!(allocator.outstanding(), before);
    drop(out);
    drop(instance);
    assert_eq!(allocator.outstanding(), 0);
}

#[test]
fn ds_scalar_if_type_contract_is_unmaskable_and_numeric_value_is_row_data() {
    use novarocks_failure_contract::{FailureClass, FailureProjection, domain};
    let p = ds_if_program(DsCatalogue::Private, ds_if_type());
    let batch = ds_if_batch(&p);
    let allocator = Arc::new(CountingAllocator::new(4 * 1024 * 1024));
    let mut instance = CompiledExpressionInstance::try_new_with_allocator(
        p.unit.clone(),
        p.root,
        &Control,
        Some(allocator.clone()),
    )
    .unwrap();
    let error = instance
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
    let fact = error.failure_fact();
    assert_eq!(fact.class(), FailureClass::Invariant);
    assert_eq!(fact.reason().domain(), domain::KERNEL.get());
    assert_eq!(fact.reason().code(), 311);
    assert!(
        error
            .to_string()
            .starts_with("ds_hll_count_distinct_state target type expects string input, got List(")
    );
    assert!(error.to_string().len() <= 512);
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

    let p = ds_if_program(DsCatalogue::Private, arrow::datatypes::DataType::Utf8);
    let batch = ds_if_row_data_batch(&p);
    let mut instance = CompiledExpressionInstance::try_new_with_allocator(
        p.unit.clone(),
        p.root,
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
    let error = &output.errors()[0];
    assert_eq!(error.selected_ordinal(), 1);
    assert_eq!(
        error.reason(),
        novarocks_functions::DataFailureReason::NumericOutOfRange
    );
    assert_eq!(
        error.message(),
        "ds_hll_count_distinct_state log_k out of range: 999"
    );
    assert_eq!(output.values().null_count(), 3);
    drop(output);
    let rows = [0, 2];
    let output = instance
        .evaluate(
            EvaluationInput::Batch {
                batch: &batch,
                source: None,
            },
            {
                let selection = Selection::try_sparse(3, &rows).unwrap();
                ScalarInvocationScope::new(selection, selection)
            },
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert!(output.errors().is_empty());
    assert_eq!(output.values().null_count(), 2);
    drop(output);
    drop(instance);
    assert_eq!(allocator.outstanding(), 0);
}
#[test]
fn ds_scalar_private_adapter_frame_all_seven_originating_host_causes_have_no_footer_or_replay() {
    for cause in callback_causes(GUARDED_DIAGNOSTICS) {
        let p = ds_program(DsCatalogue::Private, 1);
        let batch = ds_batch(&p, 1);
        let allocator = Arc::new(CountingAllocator::new(2 * 1024 * 1024));
        let authority = Arc::new(RefusingHost {
            actual: allocator.clone(),
            cause: cause.clone(),
            stop: 1,
            fail_metadata: false,
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
        let control = NoTail(&authority, "DS scalar never waits");
        assert!(
            matches!(instance.evaluate(EvaluationInput::Batch { batch: &batch, source: None }, ScalarInvocationScope::new(Selection::all(3), Selection::all(3)), ScalarInvocationActivation::Activated, &control, &mut PassThroughCallHost), Err(ExprEvaluationFailure::Kernel(actual)) if actual==cause)
        );
        assert_eq!(
            authority
                .requests
                .load(std::sync::atomic::Ordering::Relaxed),
            2
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

#[test]
fn ds_scalar_private_adapter_frame_constructor_host_seven_causes_have_no_footer() {
    for cause in callback_causes(GUARDED_DIAGNOSTICS) {
        for fail_metadata in [false, true] {
            let p = ds_program(DsCatalogue::Private, 1);
            let batch = ds_batch(&p, 1);
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
            let control = NoTail(&authority, "DS scalar never waits");
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
