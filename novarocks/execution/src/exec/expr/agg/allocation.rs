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
use hashbrown::hash_map::DefaultHashBuilder;

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

impl AggregateAllocator {
    pub(crate) fn new(tracker: Arc<MemTracker>) -> Self {
        Self { tracker }
    }

    pub(crate) fn allocation_error(&self, operation: &str) -> String {
        format!(
            "ResourceExhausted: {operation}: aggregate allocation was rejected by memory tracker {} or the system allocator",
            self.tracker.label()
        )
    }

    /// Reserves bounded headroom for a temporary allocation performed by a
    /// library that cannot accept our allocator. The caller must prove the
    /// supplied byte bound covers the library's complete live allocation
    /// graph for the guarded operation.
    pub(crate) fn reserve_transient(
        &self,
        bytes: usize,
        operation: &str,
    ) -> Result<AggregateTransientReservation, String> {
        self.try_charge(bytes)
            .map_err(|_| self.allocation_error(operation))?;
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
        match self.tracker.consume_and_check_limit(bytes) {
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

/// Persistent query-memory charge for a state owned by a library that exposes
/// allocation preflight evidence but cannot accept a Rust allocator. The
/// charge itself is inline and clones only the existing query tracker.
pub(crate) struct AggregateRetainedCharge {
    allocator: AggregateAllocator,
    bytes: usize,
}

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
    ) -> Result<AggregateTransientReservation, String> {
        self.allocator
            .reserve_transient(additional_headroom_bytes, operation)
    }

    /// Converts the retained portion of a successful operation reservation
    /// into this persistent charge without charging the tracker twice.
    pub(crate) fn reconcile_under_reservation(
        &mut self,
        new_bytes: usize,
        reservation: &mut AggregateTransientReservation,
    ) -> Result<(), String> {
        if new_bytes >= self.bytes {
            let growth = new_bytes - self.bytes;
            if growth > reservation.bytes {
                return Err(format!(
                    "aggregate allocation preflight underestimated retained growth: reserved={} growth={growth}",
                    reservation.bytes
                ));
            }
            reservation.bytes -= growth;
        } else {
            self.allocator.release_charge(self.bytes - new_bytes);
        }
        self.bytes = new_bytes;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }
}

impl Drop for AggregateTransientReservation {
    fn drop(&mut self) {
        self.allocator.release_charge(self.bytes);
    }
}

impl Drop for AggregateRetainedCharge {
    fn drop(&mut self) {
        self.allocator.release_charge(self.bytes);
    }
}

pub(crate) type AggregateVec<T> = AllocVec<T, AggregateAllocator>;
pub(crate) type AggregateHashSet<T> = hashbrown::HashSet<T, DefaultHashBuilder, AggregateAllocator>;
pub(crate) type AggregateHashMap<K, V> =
    hashbrown::HashMap<K, V, DefaultHashBuilder, AggregateAllocator>;

pub(crate) fn aggregate_hash_set<T>(allocator: AggregateAllocator) -> AggregateHashSet<T> {
    AggregateHashSet::with_hasher_in(DefaultHashBuilder::default(), allocator)
}

pub(crate) fn aggregate_hash_map<K, V>(allocator: AggregateAllocator) -> AggregateHashMap<K, V> {
    AggregateHashMap::with_hasher_in(DefaultHashBuilder::default(), allocator)
}

pub(crate) fn aggregate_bytes(
    allocator: AggregateAllocator,
    bytes: &[u8],
) -> Result<AggregateVec<u8>, String> {
    let mut value = AggregateVec::new_in(allocator.clone());
    value
        .try_reserve_exact(bytes.len())
        .map_err(|_| allocator.allocation_error("reserve aggregate byte value"))?;
    value.extend_from_slice(bytes);
    Ok(value)
}

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
    fn rejects_growth_before_allocation_and_releases_exact_layouts() {
        let tracker = MemTracker::new_root("aggregate-test");
        tracker.install_limit_once(8).unwrap();
        let allocator = AggregateAllocator::new(Arc::clone(&tracker));
        let mut values = AggregateVec::<u64>::new_in(allocator.clone());

        values.try_reserve_exact(1).unwrap();
        values.push(0);
        assert_eq!(tracker.current(), 8);

        let capacity = values.capacity();
        assert!(values.try_reserve_exact(1).is_err());
        assert_eq!(values.capacity(), capacity);
        assert_eq!(tracker.current(), 8);
        assert!(
            allocator
                .allocation_error("grow aggregate vector")
                .contains("ResourceExhausted")
        );

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
