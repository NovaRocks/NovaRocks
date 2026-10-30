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

use std::alloc::Layout;
use std::ptr::NonNull;
use std::sync::Arc;

use allocator_api2::alloc::{AllocError, Allocator, Global};
use allocator_api2::vec::Vec as AllocVec;

use crate::runtime::mem_tracker::MemTracker;

/// Cloneable allocator shared by heap containers owned by an execution state.
/// It only clones the already-owned tracker and therefore creates no separate
/// per-state accounting allocation. Every successful allocation is charged by
/// its exact `Layout`. A grow
/// reserves the complete replacement allocation before the underlying
/// allocator is invoked, then releases the old layout only after success. This
/// intentionally accounts the conservative `old + new` reallocation peak.
#[derive(Clone, Debug)]
pub(crate) struct AggregateAllocator {
    tracker: Arc<MemTracker>,
}

impl novarocks_functions::aggregate_scalar::ScalarStateAllocator for AggregateAllocator {
    fn scalar_allocation_error<
        E: novarocks_functions::allocation_failure::AllocationRefusalSource,
    >(
        &self,
        _operation: &str,
        source: E,
    ) -> novarocks_functions::KernelFailure {
        self.allocation_error(_operation, source)
    }
}
impl novarocks_functions::exact_percentile_core::ExactPercentileAllocator for AggregateAllocator {
    type ParserReservation = AggregateTransientReservation;
    fn reserve_percentile_transient(
        &self,
        bytes: usize,
        operation: &str,
    ) -> Result<Self::ParserReservation, novarocks_functions::KernelFailure> {
        self.reserve_transient(bytes, operation)
    }
}
#[derive(Clone, Copy, Debug)]
struct AggregateRequestExtent {
    bytes: usize,
}
impl std::fmt::Display for AggregateRequestExtent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "aggregate byte request {} exceeds the signed tracker request extent",
            self.bytes
        )
    }
}
impl std::error::Error for AggregateRequestExtent {}
impl novarocks_failure_contract::FailureProjection for AggregateRequestExtent {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{
            FailureClass, FailureFact, ResourceDimension, ResourceFacts,
        };
        FailureFact::new(
            FailureClass::Resource,
            crate::runtime::fragment::ExecutionReason::AggregateRequestExtent.reason(),
        )
        .with_resource(ResourceFacts {
            request_scope: None,
            constraint_scope: None,
            dimension: ResourceDimension::Bytes,
            requested: u64::try_from(self.bytes).ok(),
            required_growth: None,
            limit: Some(i64::MAX as u64),
            live: None,
            committed: None,
        })
    }
}
impl novarocks_failure_contract::OuterFailureProjection for AggregateRequestExtent {}

impl AggregateAllocator {
    pub(crate) fn new(tracker: Arc<MemTracker>) -> Self {
        Self { tracker }
    }

    /// Use the original tracker author without allocating a refusal message.
    /// Containers still pay their actual Layout, including old + new growth.
    pub(crate) fn new_nominal(tracker: Arc<MemTracker>) -> Self {
        Self { tracker }
    }

    pub(crate) fn allocation_error<
        E: novarocks_functions::allocation_failure::AllocationRefusalSource,
    >(
        &self,
        operation: &str,
        source: E,
    ) -> novarocks_functions::KernelFailure {
        novarocks_functions::KernelFailure::from_domain(
            novarocks_functions::allocation_failure::AllocationRefusal::new(operation, source),
        )
    }

    /// Reserves bounded headroom for a temporary allocation performed by a
    /// library that cannot accept our allocator. The caller must prove the
    /// supplied byte bound covers the library's complete live allocation
    /// graph for the guarded operation.
    pub(crate) fn reserve_transient(
        &self,
        bytes: usize,
        _operation: &str,
    ) -> Result<AggregateTransientReservation, novarocks_functions::KernelFailure> {
        let signed = i64::try_from(bytes).map_err(|_| {
            novarocks_functions::KernelFailure::from_domain(AggregateRequestExtent { bytes })
        })?;
        if let Err(source) = self.tracker.consume_and_check_limit_nominal(signed) {
            self.tracker.release(signed);
            use novarocks_failure_contract::FailureProjection;
            return Err(novarocks_functions::KernelFailure::Capacity(
                novarocks_functions::KernelCapacityRefusal::new(
                    source.failure_fact().reason(),
                    source.requested,
                    source.limit,
                    source.committed,
                ),
            ));
        }
        Ok(AggregateTransientReservation {
            allocator: self.clone(),
            bytes,
        })
    }

    fn try_charge(&self, bytes: usize) -> Result<(), AllocError> {
        if bytes == 0 {
            return Ok(());
        }
        let Ok(bytes) = i64::try_from(bytes) else {
            return Err(AllocError);
        };
        match self.tracker.consume_and_check_limit_nominal(bytes) {
            Ok(()) => Ok(()),
            Err(_) => {
                self.tracker.release(bytes);
                Err(AllocError)
            }
        }
    }

    fn release_charge(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        self.tracker.release(bytes_to_i64(bytes));
    }

    fn allocation_failed(&self, bytes: usize) {
        self.release_charge(bytes);
    }
}

pub(crate) struct AggregateTransientReservation {
    allocator: AggregateAllocator,
    bytes: usize,
}

/// Persistent query-memory charge used only by allocator contract tests.
#[cfg(test)]
pub(crate) struct AggregateRetainedCharge {
    allocator: AggregateAllocator,
    bytes: usize,
}

#[cfg(test)]
impl AggregateRetainedCharge {
    pub(crate) fn new(allocator: AggregateAllocator) -> Self {
        Self {
            allocator,
            bytes: 0,
        }
    }

    pub(crate) fn reserve_operation(
        &self,
        additional_headroom_bytes: usize,
        operation: &str,
    ) -> Result<AggregateTransientReservation, novarocks_functions::KernelFailure> {
        self.allocator
            .reserve_transient(additional_headroom_bytes, operation)
    }

    /// Converts the retained portion of a successful operation reservation
    /// into this persistent charge without charging the tracker twice.
    pub(crate) fn reconcile_under_reservation(
        &mut self,
        new_bytes: usize,
        reservation: &mut AggregateTransientReservation,
    ) -> Result<(), novarocks_functions::KernelFailure> {
        novarocks_functions::opaque_memory::reconcile_opaque_retained(
            &mut self.bytes,
            &mut reservation.bytes,
            new_bytes,
            |bytes| self.allocator.release_charge(bytes),
        )
        .map_err(novarocks_functions::KernelFailure::from_domain)
    }
}

impl Drop for AggregateTransientReservation {
    fn drop(&mut self) {
        self.allocator.release_charge(self.bytes);
    }
}

#[cfg(test)]
impl Drop for AggregateRetainedCharge {
    fn drop(&mut self) {
        self.allocator.release_charge(self.bytes);
    }
}

pub(crate) type AggregateVec<T> = AllocVec<T, AggregateAllocator>;

unsafe impl Allocator for AggregateAllocator {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        self.try_charge(layout.size())?;
        match Global.allocate(layout) {
            Ok(block) => Ok(block),
            Err(error) => {
                self.allocation_failed(layout.size());
                Err(error)
            }
        }
    }

    unsafe fn deallocate(&self, pointer: NonNull<u8>, layout: Layout) {
        unsafe { Global.deallocate(pointer, layout) };
        self.release_charge(layout.size());
    }

    unsafe fn grow(
        &self,
        pointer: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        self.try_charge(new_layout.size())?;
        match unsafe { Global.grow(pointer, old_layout, new_layout) } {
            Ok(block) => {
                self.release_charge(old_layout.size());
                Ok(block)
            }
            Err(error) => {
                self.allocation_failed(new_layout.size());
                Err(error)
            }
        }
    }

    unsafe fn grow_zeroed(
        &self,
        pointer: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        self.try_charge(new_layout.size())?;
        match unsafe { Global.grow_zeroed(pointer, old_layout, new_layout) } {
            Ok(block) => {
                self.release_charge(old_layout.size());
                Ok(block)
            }
            Err(error) => {
                self.allocation_failed(new_layout.size());
                Err(error)
            }
        }
    }

    unsafe fn shrink(
        &self,
        pointer: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        // `realloc` is permitted to allocate the replacement before releasing
        // the old block even when the requested layout is smaller. Reserve
        // that complete replacement peak for the same reason as `grow`.
        self.try_charge(new_layout.size())?;
        match unsafe { Global.shrink(pointer, old_layout, new_layout) } {
            Ok(block) => {
                self.release_charge(old_layout.size());
                Ok(block)
            }
            Err(error) => {
                self.allocation_failed(new_layout.size());
                Err(error)
            }
        }
    }
}

fn bytes_to_i64(bytes: usize) -> i64 {
    i64::try_from(bytes).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordered_topn_owned_nominal_vector_growth_keeps_live_backing_on_refusal() {
        let tracker = MemTracker::new_root("ordered-topn-nominal-growth");
        tracker.install_limit_once(16).unwrap();
        let mut values =
            AggregateVec::<u64>::new_in(AggregateAllocator::new_nominal(Arc::clone(&tracker)));
        values.try_reserve_exact(2).unwrap();
        values.extend_from_slice(&[7, 9]);
        assert_eq!(tracker.current(), 16);
        let capacity = values.capacity();
        assert!(values.try_reserve_exact(1).is_err());
        assert_eq!(values.capacity(), capacity);
        assert_eq!(values.as_slice(), &[7, 9]);
        assert_eq!(tracker.current(), 16);
        drop(values);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn ordered_topn_owned_nominal_shrink_reserves_old_and_new_before_replacement() {
        let tracker = MemTracker::new_root("ordered-topn-nominal-shrink");
        tracker.install_limit_once(8).unwrap();
        let allocator = AggregateAllocator::new_nominal(Arc::clone(&tracker));
        let old = Layout::from_size_align(8, 8).unwrap();
        let replacement = Layout::from_size_align(4, 8).unwrap();
        let block = allocator.allocate(old).unwrap();
        let pointer = NonNull::new(block.as_ptr() as *mut u8).unwrap();
        // SAFETY: this allocator created the block with exactly `old`.
        assert!(unsafe { allocator.shrink(pointer, old, replacement) }.is_err());
        assert_eq!(tracker.current(), 8);
        // SAFETY: failed shrink retained the original pointer and Layout.
        unsafe { allocator.deallocate(pointer, old) };
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn transient_refusal_keeps_original_limit_facts_and_no_tracker_owner() {
        use novarocks_failure_contract::{FailureClass, FailureProjection, domain};
        let tracker = MemTracker::new_root("native transient refusal");
        tracker.install_limit_once(8).unwrap();
        let weak = Arc::downgrade(&tracker);
        let allocator = AggregateAllocator::new(tracker.clone());
        let error = match allocator.reserve_transient(16, "numeric overflow authentication failure")
        {
            Err(error) => error,
            Ok(_) => panic!("request exceeds the original tracker limit"),
        };
        assert_eq!(
            tracker.current(),
            0,
            "failed prospective charge rolls back before publication"
        );
        let fact = error.failure_fact();
        assert_eq!(fact.class(), FailureClass::Resource);
        assert_eq!(fact.reason().domain(), domain::MEMORY.get());
        assert_eq!(fact.reason().code(), 8);
        let resource = fact.resource().unwrap();
        assert_eq!(
            (resource.requested, resource.limit, resource.committed),
            (Some(16), Some(8), Some(16))
        );
        drop(allocator);
        drop(tracker);
        assert!(
            weak.upgrade().is_none(),
            "error facts do not extend the tracker lifetime"
        );
        assert_eq!(error.failure_fact(), fact);
    }

    #[test]
    fn rejects_growth_before_allocation_and_releases_exact_layouts() {
        let tracker = MemTracker::new_root("aggregate-test");
        tracker.install_limit_once(8).unwrap();
        let allocator = AggregateAllocator::new(Arc::clone(&tracker));
        let mut values = AggregateVec::<u64>::new_in(allocator.clone());

        values.try_reserve_exact(1).unwrap();
        values.push(0);
        assert_eq!(tracker.current(), 8);

        let capacity = values.capacity();
        let source = values.try_reserve_exact(1).unwrap_err();
        assert_eq!(values.capacity(), capacity);
        assert_eq!(tracker.current(), 8);
        use novarocks_failure_contract::FailureProjection;
        let error = allocator.allocation_error("grow aggregate vector", source);
        let fact = error.failure_fact();
        assert_eq!(
            fact.class(),
            novarocks_failure_contract::FailureClass::Resource
        );
        assert_eq!(fact.resource().unwrap().requested, Some(16));
        assert_eq!(fact.resource().unwrap().limit, None);
        assert_eq!(fact.resource().unwrap().committed, None);

        drop(values);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn rejects_shrink_reallocation_peak_before_calling_global_allocator() {
        let tracker = MemTracker::new_root("aggregate-shrink-test");
        tracker.install_limit_once(8).unwrap();
        let allocator = AggregateAllocator::new(Arc::clone(&tracker));
        let old_layout = Layout::from_size_align(8, 8).unwrap();
        let new_layout = Layout::from_size_align(4, 8).unwrap();
        let block = allocator.allocate(old_layout).unwrap();
        assert_eq!(tracker.current(), 8);

        let pointer = NonNull::new(block.as_ptr() as *mut u8).unwrap();
        assert!(unsafe { allocator.shrink(pointer, old_layout, new_layout) }.is_err());
        assert_eq!(tracker.current(), 8);

        unsafe { allocator.deallocate(pointer, old_layout) };
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn operation_reservation_transfers_only_retained_growth() {
        let tracker = MemTracker::new_root("aggregate-manual-charge-test");
        let allocator = AggregateAllocator::new(Arc::clone(&tracker));
        {
            let mut retained = AggregateRetainedCharge::new(allocator);
            let mut reservation = retained
                .reserve_operation(128, "manual allocation")
                .expect("reserve peak");
            assert_eq!(tracker.current(), 128);
            retained
                .reconcile_under_reservation(48, &mut reservation)
                .expect("retain successful allocation");
            drop(reservation);
            assert_eq!(tracker.current(), 48);

            let mut reservation = retained
                .reserve_operation(32, "manual shrink")
                .expect("reserve shrink peak");
            retained
                .reconcile_under_reservation(16, &mut reservation)
                .expect("release retained shrink");
            drop(reservation);
            assert_eq!(tracker.current(), 16);
        }
        assert_eq!(tracker.current(), 0);
    }
}
