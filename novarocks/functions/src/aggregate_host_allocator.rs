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

//! Explicit host allocation for state-owned fallible containers.
use crate::{AggregateStateAllocator, KernelFailure};
use allocator_api2::alloc::{AllocError, Allocator};
use std::{
    alloc::Layout,
    fmt,
    ptr::NonNull,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering, fence},
    },
};

// This block is owned by the actual host, once per created aggregate state.
// Allocator clones are thin handles to it; clone never allocates.
struct HostAllocatorInner {
    references: AtomicUsize,
    host: Arc<dyn AggregateStateAllocator>,
    failure: Mutex<Option<KernelFailure>>,
}
pub(super) struct HostAggregateAllocator {
    inner: NonNull<HostAllocatorInner>,
}
// SAFETY: the pointee remains alive while any handle exists; its immutable
// host is Send + Sync and its refcount/journal use atomic/Mutex synchronization.
unsafe impl Send for HostAggregateAllocator {}
unsafe impl Sync for HostAggregateAllocator {}
impl HostAggregateAllocator {
    pub(super) fn try_new(host: Arc<dyn AggregateStateAllocator>) -> Result<Self, KernelFailure> {
        let pointer = host
            .allocate(Layout::new::<HostAllocatorInner>())?
            .cast::<HostAllocatorInner>();
        // SAFETY: the host returned this exact nonzero, aligned Layout and it
        // has not been published or initialized. No fallible work follows.
        unsafe {
            pointer.as_ptr().write(HostAllocatorInner {
                references: AtomicUsize::new(1),
                host,
                failure: Mutex::new(None),
            })
        };
        Ok(Self { inner: pointer })
    }
    /// Typed single-block custody allocation on the same actual host. Unlike
    /// allocator-api2 containers, this path needs no AllocError journal and
    /// never locks its potentially allocating platform Mutex.
    pub(crate) fn allocate_typed_block(
        &self,
        layout: Layout,
    ) -> Result<NonNull<u8>, KernelFailure> {
        self.inner().host.allocate(layout)
    }
    fn inner(&self) -> &HostAllocatorInner {
        // SAFETY: this owning handle contributes one reference, so the block
        // cannot be destroyed for the lifetime of the returned borrow.
        unsafe { self.inner.as_ref() }
    }
    /// Compare actual immutable host identity. This grants no allocation or scope.
    pub(crate) fn has_host_authority(&self, host: &Arc<dyn AggregateStateAllocator>) -> bool {
        Arc::ptr_eq(&self.inner().host, host)
    }
    pub(crate) const fn metadata_allocation_bytes() -> usize {
        Layout::new::<HostAllocatorInner>().size()
    }
    /// Count the one actual metadata block once per state, not per clone.
    pub(super) fn metadata_bytes(&self) -> usize {
        Self::metadata_allocation_bytes()
    }
    /// Borrow the originating allocator refusal without consuming its journal.
    /// serde's custom error carrier must not erase the distinct Kernel cause.
    pub(super) fn recorded_failure(&self) -> Option<KernelFailure> {
        self.inner()
            .failure
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
    }
    pub(super) fn take_recorded_failure(&self) -> Option<KernelFailure> {
        self.inner()
            .failure
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .take()
    }
    pub(super) fn take_failure(&self) -> KernelFailure {
        self.take_recorded_failure()
            .unwrap_or(KernelFailure::ResourceExhausted)
    }
    fn refuse(&self, error: KernelFailure) -> AllocError {
        let mut slot = self
            .inner()
            .failure
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if slot.is_none() {
            *slot = Some(error);
        }
        AllocError
    }
}
impl Clone for HostAggregateAllocator {
    fn clone(&self) -> Self {
        let previous = self.inner().references.fetch_add(1, Ordering::Relaxed);
        // An infallible Clone cannot return an overflow error. Match the Arc
        // guard before overflow can make a live block appear unreferenced.
        if previous > isize::MAX as usize {
            std::process::abort();
        }
        Self { inner: self.inner }
    }
}
impl Drop for HostAggregateAllocator {
    fn drop(&mut self) {
        if self.inner().references.fetch_sub(1, Ordering::Release) != 1 {
            return;
        }
        fence(Ordering::Acquire);
        // Retain the host outside its own metadata block before destroying it.
        // Arc clone is refcount-only; it does not allocate a replacement block.
        let host = Arc::clone(&self.inner().host);
        let pointer = self.inner;
        // SAFETY: the last reference owns destruction exactly once, and no
        // references to the inner block are used after this call.
        unsafe { std::ptr::drop_in_place(pointer.as_ptr()) };
        // SAFETY: exact original block and Layout; host remains alive locally.
        unsafe { host.release(pointer.cast(), Layout::new::<HostAllocatorInner>()) };
    }
}
impl fmt::Debug for HostAggregateAllocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HostAggregateAllocator")
            .finish_non_exhaustive()
    }
}
// SAFETY: every nonzero block is delegated to the same clone-stable host,
// which owns exact allocation/release. Containers preserve the original Layout.
// The default resize methods reserve the replacement before releasing the old.
unsafe impl Allocator for HostAggregateAllocator {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        if layout.size() == 0 {
            let pointer = NonNull::new(layout.align() as *mut u8).expect("nonzero alignment");
            return Ok(NonNull::slice_from_raw_parts(pointer, 0));
        }
        if self
            .inner()
            .failure
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .is_some()
        {
            return Err(AllocError);
        }
        let pointer = self
            .inner()
            .host
            .allocate(layout)
            .map_err(|error| self.refuse(error))?;
        Ok(NonNull::slice_from_raw_parts(pointer, layout.size()))
    }
    unsafe fn deallocate(&self, pointer: NonNull<u8>, layout: Layout) {
        if layout.size() != 0 {
            // SAFETY: exact block and Layout forwarded from this host.
            unsafe { self.inner().host.release(pointer, layout) };
        }
    }
}

impl crate::aggregate_scalar::ScalarStateAllocator for HostAggregateAllocator {
    fn scalar_allocation_error(
        &self,
        _operation: &str,
    ) -> crate::aggregate_scalar::ScalarStateError {
        crate::aggregate_scalar::ScalarStateError::Kernel(self.take_failure())
    }
}

#[cfg(test)]
#[path = "aggregate_host_allocator_tests.rs"]
mod tests;
