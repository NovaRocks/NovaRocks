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
//! Real compiler/Frame and the original actual production tracker host.
use super::*;
use novarocks_expression::testkit::PassThroughCallHost;
use novarocks_expression::{EvaluationInput, ExprEvaluationFailure};
use novarocks_functions::{ScalarInvocationActivation, ScalarInvocationScope};
#[path = "percentile_raw_private_frame_fixture.rs"]
mod fixture;
use crate::runtime::mem_tracker::MemTracker;
fn host(tracker: Arc<MemTracker>) -> Arc<dyn novarocks_functions::AggregateStateAllocator> {
    crate::runtime::tracked_state_allocator::expression_allocation_host(tracker)
}
#[test]
fn percentile_raw_private_frame_original_values_full_rowdata_sparse_and_host_drop() {
    let program = fixture::program();
    let batch = fixture::source_batch(&program);
    let tracker = MemTracker::new_root("percentile-raw-private-frame");
    tracker.install_limit_once(4 * 1024 * 1024).unwrap();
    let mut instance = CompiledExpressionInstance::try_new_with_allocator(
        root_unit(&program).clone(),
        root(&program).root,
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
    assert_eq!(output.errors()[0].selected_ordinal(), 0);
    let arrays = fixture::source_arrays();
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
    assert_eq!(tracker.current(), 0);
}
#[test]
fn percentile_raw_private_frame_missing_host_and_true_budget_failure_latch() {
    let program = fixture::program();
    let batch = fixture::source_batch(&program);
    let mut no_host = CompiledExpressionInstance::try_new(
        root_unit(&program).clone(),
        root(&program).root,
        &Control,
    )
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
    let tracker = MemTracker::new_root("percentile-raw-refusal");
    tracker.install_limit_once(1).unwrap();
    let mut instance = CompiledExpressionInstance::try_new_with_allocator(
        root_unit(&program).clone(),
        root(&program).root,
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
        panic!("percentile scalar never waits")
    }
}
#[test]
fn percentile_raw_private_frame_constructor_host_seven_causes_have_no_footer() {
    for cause in causes() {
        for fail_metadata in [false, true] {
            let p = fixture::program();
            let batch = fixture::source_batch(&p);
            let tracker = MemTracker::new_root("percentile-raw-constructor-first-cause");
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
