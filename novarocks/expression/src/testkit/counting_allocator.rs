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

use novarocks_functions::opaque_memory::OpaqueAllocationHost;
use novarocks_functions::{
    AggregateStateAllocator, KernelFailure, UnaccountedAggregateStateAllocator,
};
use std::{
    alloc::Layout,
    ptr::NonNull,
    sync::atomic::{AtomicUsize, Ordering},
};

pub struct CountingAllocator {
    inner: UnaccountedAggregateStateAllocator,
    outstanding: AtomicUsize,
    reserved: AtomicUsize,
    refunded: AtomicUsize,
    limit: usize,
    capacity_reason: Option<novarocks_failure_contract::FailureReason>,
}
impl CountingAllocator {
    pub fn new(limit: usize) -> Self {
        Self {
            inner: UnaccountedAggregateStateAllocator,
            outstanding: AtomicUsize::new(0),
            reserved: AtomicUsize::new(0),
            refunded: AtomicUsize::new(0),
            limit,
            capacity_reason: None,
        }
    }
    pub fn with_capacity_reason(
        limit: usize,
        reason: novarocks_failure_contract::FailureReason,
    ) -> Self {
        let mut allocator = Self::new(limit);
        allocator.capacity_reason = Some(reason);
        allocator
    }

    pub fn outstanding(&self) -> usize {
        self.outstanding.load(Ordering::SeqCst)
    }
    pub fn reservation_totals(&self) -> (usize, usize) {
        (
            self.reserved.load(Ordering::SeqCst),
            self.refunded.load(Ordering::SeqCst),
        )
    }
    fn reserve(&self, bytes: usize) -> Result<(), KernelFailure> {
        self.outstanding
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                current
                    .checked_add(bytes)
                    .filter(|&total| total <= self.limit)
            })
            .map(|_| {
                self.reserved.fetch_add(bytes, Ordering::SeqCst);
            })
            .map_err(|current| {
                let refusal = self.capacity_reason.and_then(|reason| {
                    Some(novarocks_functions::KernelCapacityRefusal::new(
                        reason,
                        u64::try_from(bytes).ok()?,
                        u64::try_from(self.limit).ok()?,
                        u64::try_from(current.checked_add(bytes)?).ok()?,
                    ))
                });
                refusal.map_or(KernelFailure::ResourceExhausted, KernelFailure::Capacity)
            })
    }
    fn refund(&self, bytes: usize) {
        let previous = self.outstanding.fetch_sub(bytes, Ordering::SeqCst);
        self.refunded.fetch_add(bytes, Ordering::SeqCst);
        assert!(
            previous >= bytes,
            "only an outstanding allocation can be released"
        );
    }
}
impl AggregateStateAllocator for CountingAllocator {
    fn opaque_allocation_host(&self) -> Option<&dyn OpaqueAllocationHost> {
        Some(self)
    }
    fn allocate(&self, layout: Layout) -> Result<NonNull<u8>, KernelFailure> {
        self.reserve(layout.size())?;
        match self.inner.allocate(layout) {
            Ok(block) => Ok(block),
            Err(error) => {
                self.refund(layout.size());
                Err(error)
            }
        }
    }
    unsafe fn release(&self, block: NonNull<u8>, layout: Layout) {
        // SAFETY: the caller returns this allocator's block with the exact original layout.
        unsafe {
            self.inner.release(block, layout);
        }
        self.refund(layout.size());
    }
}
impl OpaqueAllocationHost for CountingAllocator {
    fn reserve_opaque(&self, bytes: usize) -> Result<(), KernelFailure> {
        self.reserve(bytes)
    }
    fn release_opaque(&self, bytes: usize) {
        self.refund(bytes);
    }
}
