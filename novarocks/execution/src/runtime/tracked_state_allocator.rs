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

//! One runtime-owned adapter for real state blocks and opaque state backing.
//! Each operator installs its own tracker capability; sharing code does not
//! share an instance, quota, control policy or allocator journal.
use crate::runtime::mem_tracker::{MemTracker, MemoryAccountingFailure};
use allocator_api2::alloc::Allocator;
use novarocks_functions::{AggregateStateAllocator, KernelDiagnostic, KernelFailure};
use std::{alloc::Layout, ptr::NonNull, sync::Arc};

/// State backing charged to the operator's memory tracker. A block is charged
/// before it is allocated and released after it is returned, so the tracker
/// never undercounts live backing.
pub(crate) struct TrackedStateAllocator<C = ()> {
    tracker: Arc<MemTracker>,
    _creation: C,
}

impl TrackedStateAllocator {
    pub(crate) fn new(tracker: Arc<MemTracker>) -> Self {
        Self {
            tracker,
            _creation: (),
        }
    }
    /// Read only the original host's immutable tracker ancestry. The caller
    /// must establish the actual instance-host loan before consuming facts.
    pub(crate) fn limit_failure_request_facts(
        &self,
    ) -> Result<
        crate::runtime::mem_tracker::TrackerLimitFailureRequestFacts,
        novarocks_type_contract::ControlResourceError,
    > {
        self.tracker.limit_failure_request_facts()
    }
}

/// Actual expression and aggregate backing share the same tracker adapter.
pub(crate) fn expression_allocation_host(tracker: Arc<MemTracker>) -> Arc<TrackedStateAllocator> {
    Arc::new(TrackedStateAllocator::new(tracker))
}

/// The caller admits this exact Arc<TrackedStateAllocator<C>> before entry.
/// Its private inline keeper survives all real host aliases, including those
/// held by source custody. The host Arc is not exposed through a Weak port.
pub(crate) fn expression_allocation_host_with_creation<C: Send + Sync + 'static>(
    tracker: Arc<MemTracker>,
    creation: C,
) -> Arc<TrackedStateAllocator<C>> {
    Arc::new(TrackedStateAllocator {
        tracker,
        _creation: creation,
    })
}

impl<C: Send + Sync> novarocks_functions::opaque_memory::OpaqueAllocationHost
    for TrackedStateAllocator<C>
{
    fn reserve_opaque(&self, bytes: usize) -> Result<(), KernelFailure> {
        if bytes == 0 {
            return Ok(());
        }
        let bytes = checked_counter_bytes(bytes)?;
        if let Err(source) = self.tracker.consume_and_check_limit_nominal(bytes) {
            self.tracker.release(bytes);
            use novarocks_failure_contract::FailureProjection;
            return Err(KernelFailure::Capacity(
                novarocks_functions::KernelCapacityRefusal::new(
                    source.failure_fact().reason(),
                    source.requested,
                    source.limit,
                    source.committed,
                ),
            ));
        }
        Ok(())
    }
    fn release_opaque(&self, bytes: usize) {
        if bytes != 0 {
            // Successful reservation was represented exactly as i64.
            self.tracker
                .release(i64::try_from(bytes).expect("successful opaque reservation size"));
        }
    }
}
impl<C: Send + Sync> AggregateStateAllocator for TrackedStateAllocator<C> {
    fn opaque_allocation_host(
        &self,
    ) -> Option<&dyn novarocks_functions::opaque_memory::OpaqueAllocationHost> {
        Some(self)
    }

    fn allocate(&self, layout: Layout) -> Result<NonNull<u8>, KernelFailure> {
        if layout.size() == 0 {
            return Err(KernelFailure::Internal(KernelDiagnostic::new(
                "aggregate state block has no size",
            )));
        }
        let bytes = checked_counter_bytes(layout.size())?;
        if let Err(source) = self.tracker.consume_and_check_limit_nominal(bytes) {
            self.tracker.release(bytes);
            use novarocks_failure_contract::FailureProjection;
            return Err(KernelFailure::Capacity(
                novarocks_functions::KernelCapacityRefusal::new(
                    source.failure_fact().reason(),
                    source.requested,
                    source.limit,
                    source.committed,
                ),
            ));
        }
        match allocator_api2::alloc::Global.allocate(layout) {
            Ok(block) => Ok(block.cast()),
            Err(source) => {
                self.tracker.release(bytes);
                Err(KernelFailure::from_domain(
                    novarocks_functions::allocation_failure::AllocationRefusal::new(
                        "allocate runtime state backing",
                        novarocks_functions::allocation_failure::AllocatorRequestRefusal::new(
                            layout, source,
                        ),
                    ),
                ))
            }
        }
    }

    unsafe fn release(&self, block: NonNull<u8>, layout: Layout) {
        // SAFETY: the column returns a block this allocator produced with
        // exactly this layout, after destroying every state inside it.
        unsafe { allocator_api2::alloc::Global.deallocate(block, layout) };
        self.tracker.release(
            i64::try_from(layout.size()).expect("successful state backing reservation size"),
        );
    }
}

#[derive(Debug)]
struct CounterWidthFailure {
    requested: usize,
    source: std::num::TryFromIntError,
}
impl std::fmt::Display for CounterWidthFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "state backing request {} exceeds the tracker counter range: {}",
            self.requested, self.source
        )
    }
}
impl std::error::Error for CounterWidthFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
impl novarocks_failure_contract::FailureProjection for CounterWidthFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureProjection::failure_fact(
            &MemoryAccountingFailure::BytesOutOfRange {
                requested: self.requested,
            },
        )
    }
}
impl novarocks_failure_contract::OuterFailureProjection for CounterWidthFailure {}
fn checked_counter_bytes(requested: usize) -> Result<i64, KernelFailure> {
    i64::try_from(requested)
        .map_err(|source| KernelFailure::from_domain(CounterWidthFailure { requested, source }))
}
#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection, domain};
    use novarocks_functions::opaque_memory::OpaqueAllocationHost;
    #[test]
    fn opaque_request_width_refuses_before_any_charge_and_preserves_conversion_source() {
        let tracker = MemTracker::new_root("runtime state request width");
        let host = TrackedStateAllocator::new(tracker.clone());
        let error = host.reserve_opaque(usize::MAX).unwrap_err();
        let fact = error.failure_fact();
        assert_eq!(fact.class(), FailureClass::Resource);
        assert_eq!(fact.reason().domain(), domain::MEMORY.get());
        assert_eq!(fact.reason().code(), 9);
        assert_eq!(fact.resource().unwrap().requested, Some(usize::MAX as u64));
        assert!(fact.representation().is_none());
        let mut source: &dyn std::error::Error = &error;
        while !source.is::<std::num::TryFromIntError>() {
            source = source.source().expect("actual signed-counter conversion");
        }
        assert_eq!(tracker.current(), 0);
        assert_eq!(tracker.allocated(), 0);
        assert_eq!(tracker.deallocated(), 0);
    }
}
