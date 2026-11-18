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
//! Actual compiler/Frame instance and actual production MemTracker allocator.
use super::*;
use novarocks_expression::testkit::PassThroughCallHost;
use novarocks_expression::{EvaluationInput, ExprEvaluationFailure};
use novarocks_functions::{ScalarInvocationActivation, ScalarInvocationScope};
#[path = "ds_hll_state_private_frame_fixture.rs"]
mod fixture;
use crate::runtime::mem_tracker::MemTracker;
fn host(tracker: Arc<MemTracker>) -> Arc<dyn novarocks_functions::AggregateStateAllocator> {
    crate::runtime::tracked_state_allocator::expression_allocation_host(tracker)
}
#[test]
fn ds_scalar_private_adapter_frame_every_declared_arity_true_host_and_no_host_refusal() {
    use arrow::array::BinaryArray;
    for count in 1..=3 {
        let p = fixture::program(count);
        let batch = fixture::source_batch(&p, count);
        let mut no_host =
            CompiledExpressionInstance::try_new(root_unit(&p).clone(), root(&p).root, &Control)
                .unwrap();
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
        let tracker = MemTracker::new_root("ds-scalar-actual-frame");
        tracker.install_limit_once(2 * 1024 * 1024).unwrap();
        let mut instance = CompiledExpressionInstance::try_new_with_allocator(
            root_unit(&p).clone(),
            root(&p).root,
            &Control,
            Some(host(tracker.clone())),
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
        assert!(tracker.current() > 0);
        drop(output);
        drop(instance);
        assert_eq!(tracker.current(), 0);
    }
}
#[test]
fn ds_scalar_private_adapter_frame_real_tracker_refusal_and_failure_latch() {
    let p = fixture::program(1);
    let batch = fixture::source_batch(&p, 1);
    let tracker = MemTracker::new_root("ds-scalar-actual-refusal");
    tracker.install_limit_once(1).unwrap();
    let mut instance = CompiledExpressionInstance::try_new_with_allocator(
        root_unit(&p).clone(),
        root(&p).root,
        &Control,
        Some(host(tracker.clone())),
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
    crate::runtime_filter::assert_real_tracker_capacity(&refusal, 1);
    assert_eq!(tracker.current(), 0);
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
    assert_eq!(tracker.current(), 0);
}
#[test]
fn ds_scalar_private_adapter_frame_sparse_original_row_identity_and_empty_selection() {
    let p = fixture::program(2);
    let batch = fixture::source_batch(&p, 2);
    let tracker = MemTracker::new_root("ds-scalar-sparse-frame");
    tracker.install_limit_once(2 * 1024 * 1024).unwrap();
    let mut instance = CompiledExpressionInstance::try_new_with_allocator(
        root_unit(&p).clone(),
        root(&p).root,
        &Control,
        Some(host(tracker.clone())),
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
    let before = tracker.current();
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
    assert_eq!(tracker.current(), before);
    drop(out);
    drop(instance);
    assert_eq!(tracker.current(), 0);
}

#[path = "ds_hll_state_private_if_fixture.rs"]
mod if_fixture;
#[test]
fn ds_scalar_if_type_contract_is_unmaskable_and_numeric_value_is_row_data() {
    use novarocks_failure_contract::{FailureClass, FailureProjection, domain};
    let p = if_fixture::program();
    let batch = if_fixture::source_batch(&p);
    let tracker = MemTracker::new_root("ds-scalar-if-shape");
    tracker.install_limit_once(4 * 1024 * 1024).unwrap();
    let mut instance = CompiledExpressionInstance::try_new_with_allocator(
        root_unit(&p).clone(),
        root(&p).root,
        &Control,
        Some(host(tracker.clone())),
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
    assert_eq!(tracker.current(), 0);

    let p = if_fixture::row_data_program();
    let batch = if_fixture::row_data_batch(&p);
    let mut instance = CompiledExpressionInstance::try_new_with_allocator(
        root_unit(&p).clone(),
        root(&p).root,
        &Control,
        Some(host(tracker.clone())),
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
    assert_eq!(tracker.current(), 0);
}
// This test authority delegates every granted byte/block to the actual
// production tracker. Only its explicit second opaque request is refused.
struct RefusingHost {
    actual: Arc<dyn novarocks_functions::AggregateStateAllocator>,
    cause: KernelFailure,
    stop: usize,
    fail_metadata: bool,
    requests: std::sync::atomic::AtomicUsize,
    refused: std::sync::atomic::AtomicBool,
}
impl novarocks_functions::AggregateStateAllocator for RefusingHost {
    fn opaque_allocation_host(
        &self,
    ) -> Option<&dyn novarocks_functions::opaque_memory::OpaqueAllocationHost> {
        Some(self)
    }
    fn allocate(&self, layout: std::alloc::Layout) -> Result<std::ptr::NonNull<u8>, KernelFailure> {
        if self.fail_metadata {
            self.refused
                .store(true, std::sync::atomic::Ordering::Relaxed);
            return Err(self.cause.clone());
        }
        self.actual.allocate(layout)
    }
    unsafe fn release(&self, p: std::ptr::NonNull<u8>, layout: std::alloc::Layout) {
        unsafe { self.actual.release(p, layout) }
    }
}
impl novarocks_functions::opaque_memory::OpaqueAllocationHost for RefusingHost {
    fn reserve_opaque(&self, bytes: usize) -> Result<(), KernelFailure> {
        use std::sync::atomic::Ordering;
        let at = self.requests.fetch_add(1, Ordering::Relaxed);
        assert!(
            at <= self.stop,
            "opaque request after actual originating refusal"
        );
        if at == self.stop {
            self.refused.store(true, Ordering::Relaxed);
            return Err(self.cause.clone());
        }
        self.actual
            .opaque_allocation_host()
            .unwrap()
            .reserve_opaque(bytes)
    }
    fn release_opaque(&self, bytes: usize) {
        self.actual
            .opaque_allocation_host()
            .unwrap()
            .release_opaque(bytes);
    }
}
struct NoTail<'a>(&'a RefusingHost);
impl KernelEvaluationControl for NoTail<'_> {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        assert!(
            !self.0.refused.load(std::sync::atomic::Ordering::Relaxed),
            "control callback after actual host first failure"
        );
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("DS scalar never waits")
    }
}
#[test]
fn ds_scalar_private_adapter_frame_all_seven_originating_host_causes_have_no_footer_or_replay() {
    for cause in causes() {
        let p = fixture::program(1);
        let batch = fixture::source_batch(&p, 1);
        let tracker = MemTracker::new_root("ds-scalar-host-first-cause");
        tracker.install_limit_once(2 * 1024 * 1024).unwrap();
        let authority = Arc::new(RefusingHost {
            actual: host(tracker.clone()),
            cause: cause.clone(),
            stop: 1,
            fail_metadata: false,
            requests: std::sync::atomic::AtomicUsize::new(0),
            refused: std::sync::atomic::AtomicBool::new(false),
        });
        let mut instance = CompiledExpressionInstance::try_new_with_allocator(
            root_unit(&p).clone(),
            root(&p).root,
            &Control,
            Some(authority.clone()),
        )
        .unwrap();
        let control = NoTail(&authority);
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
        assert_eq!(tracker.current(), 0);
    }
}

#[test]
fn ds_scalar_private_adapter_frame_constructor_host_seven_causes_have_no_footer() {
    for cause in causes() {
        for fail_metadata in [false, true] {
            let p = fixture::program(1);
            let batch = fixture::source_batch(&p, 1);
            let tracker = MemTracker::new_root("ds-scalar-constructor-first-cause");
            tracker.install_limit_once(2 * 1024 * 1024).unwrap();
            let authority = Arc::new(RefusingHost {
                actual: host(tracker.clone()),
                cause: cause.clone(),
                stop: if fail_metadata { usize::MAX } else { 0 },
                fail_metadata,
                requests: std::sync::atomic::AtomicUsize::new(0),
                refused: std::sync::atomic::AtomicBool::new(false),
            });
            let mut instance = CompiledExpressionInstance::try_new_with_allocator(
                root_unit(&p).clone(),
                root(&p).root,
                &Control,
                Some(authority.clone()),
            )
            .unwrap();
            let control = NoTail(&authority);
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
            assert_eq!(tracker.current(), 0);
        }
    }
}
