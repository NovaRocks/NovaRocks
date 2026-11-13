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

//! Test-owned allocation authority for the public expression-frame seam.
//! The original 8 MiB fixture budget and last-release assertion are preserved.
use novarocks_functions::opaque_memory::OpaqueAllocationHost;
use novarocks_functions::{
    AggregateStateAllocator, KernelFailure, UnaccountedAggregateStateAllocator,
};
use std::{
    alloc::Layout,
    ptr::NonNull,
    sync::atomic::{AtomicUsize, Ordering},
};

pub(super) struct CountingAllocator {
    inner: UnaccountedAggregateStateAllocator,
    outstanding: AtomicUsize,
    limit: usize,
}
impl CountingAllocator {
    pub(super) fn new(limit: usize) -> Self {
        Self {
            inner: UnaccountedAggregateStateAllocator,
            outstanding: AtomicUsize::new(0),
            limit,
        }
    }
    pub(super) fn outstanding(&self) -> usize {
        self.outstanding.load(Ordering::SeqCst)
    }
    fn reserve(&self, bytes: usize) -> Result<(), KernelFailure> {
        self.outstanding
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                current
                    .checked_add(bytes)
                    .filter(|&total| total <= self.limit)
            })
            .map(|_| ())
            .map_err(|_| KernelFailure::ResourceExhausted)
    }
    fn refund(&self, bytes: usize) {
        let previous = self.outstanding.fetch_sub(bytes, Ordering::SeqCst);
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
