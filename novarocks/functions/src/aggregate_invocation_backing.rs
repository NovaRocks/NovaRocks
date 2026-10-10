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

use crate::KernelFailure;
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::kernel_control::internal;
use crate::kernel_input::EvaluationCheckpoints;
use allocator_api2::{alloc::Allocator, vec::Vec as HostVec};
use std::{
    alloc::Layout,
    fmt::{self, Write},
    ops::Deref,
    ptr::NonNull,
    sync::atomic::{AtomicUsize, Ordering, fence},
};
/// A real host-allocated immutable block. No allocation occurs during Clone.
struct SharedBlock<T> {
    references: AtomicUsize,
    allocator: HostAggregateAllocator,
    value: T,
}
pub(crate) struct HostShared<T> {
    pointer: NonNull<SharedBlock<T>>,
}
unsafe impl<T: Send + Sync> Send for HostShared<T> {}
unsafe impl<T: Send + Sync> Sync for HostShared<T> {}
impl<T> HostShared<T> {
    pub(crate) fn try_new(
        value: T,
        allocator: HostAggregateAllocator,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Self, KernelFailure> {
        Self::try_new_with(value, allocator, work, |allocator, layout| {
            allocator
                .allocate(layout)
                .map(|pointer| pointer.cast::<u8>())
                .map_err(|_| allocator.take_failure())
        })
    }
    /// Same holder/refcount/Drop author, with direct typed host failure.
    pub(crate) fn try_new_direct(
        value: T,
        allocator: HostAggregateAllocator,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Self, KernelFailure> {
        Self::try_new_with(value, allocator, work, |allocator, layout| {
            allocator.allocate_typed_block(layout)
        })
    }
    fn try_new_with(
        value: T,
        allocator: HostAggregateAllocator,
        work: &mut EvaluationCheckpoints<'_>,
        allocate: impl FnOnce(&HostAggregateAllocator, Layout) -> Result<NonNull<u8>, KernelFailure>,
    ) -> Result<Self, KernelFailure> {
        work.flush()?;
        let pointer =
            allocate(&allocator, Layout::new::<SharedBlock<T>>())?.cast::<SharedBlock<T>>();
        // SAFETY: allocator admitted this exact nonzero layout and pointer; initialize before publishing.
        unsafe {
            pointer.as_ptr().write(SharedBlock {
                references: AtomicUsize::new(1),
                allocator,
                value,
            });
        }
        let result = Self { pointer };
        work.flush()?;
        Ok(result)
    }
    pub(crate) fn ptr_eq(&self, other: &Self) -> bool {
        self.pointer == other.pointer
    }
    pub(crate) fn block_bytes(&self) -> usize {
        Layout::new::<SharedBlock<T>>().size()
    }
}
impl<T> Deref for HostShared<T> {
    type Target = T;
    fn deref(&self) -> &T {
        // SAFETY: this owner pins the immutable allocation through the entire borrow.
        unsafe { &self.pointer.as_ref().value }
    }
}
impl<T> Clone for HostShared<T> {
    fn clone(&self) -> Self {
        // SAFETY: this handle pins the block; refcount is atomic.
        let previous = unsafe { self.pointer.as_ref() }
            .references
            .fetch_add(1, Ordering::Relaxed);
        if previous > isize::MAX as usize {
            std::process::abort();
        }
        Self {
            pointer: self.pointer,
        }
    }
}
impl<T> Drop for HostShared<T> {
    fn drop(&mut self) {
        // SAFETY: this live reference pins the block.
        if unsafe { self.pointer.as_ref() }
            .references
            .fetch_sub(1, Ordering::Release)
            != 1
        {
            return;
        }
        fence(Ordering::Acquire);
        // Retain the real allocator while dropping its own immutable payload and owner handle.
        let allocator = unsafe { self.pointer.as_ref() }.allocator.clone();
        let _release = SharedBlockRelease::<T> {
            pointer: self.pointer,
            allocator,
        };
        // SAFETY: last reference exclusively owns this initialized object. The
        // release guard also deallocates the block if payload destruction unwinds.
        unsafe {
            std::ptr::drop_in_place(self.pointer.as_ptr());
        }
    }
}
struct SharedBlockRelease<T> {
    pointer: NonNull<SharedBlock<T>>,
    allocator: HostAggregateAllocator,
}
impl<T> Drop for SharedBlockRelease<T> {
    fn drop(&mut self) {
        // SAFETY: this guard uniquely owns the original block after last-reference teardown.
        unsafe {
            self.allocator
                .deallocate(self.pointer.cast(), Layout::new::<SharedBlock<T>>());
        }
    }
}
impl<T: fmt::Debug> fmt::Debug for HostShared<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.deref().fmt(f)
    }
}
/// Whole original diagnostic bytes. Preparation borrows the real instance allocator.
#[derive(Clone, Debug)]
pub(crate) struct HostDiagnostic(HostShared<HostVec<u8, HostAggregateAllocator>>);
impl HostDiagnostic {
    pub(crate) fn prepare(
        allocator: &HostAggregateAllocator,
        work: &mut EvaluationCheckpoints<'_>,
        original_formatter: impl FnOnce(&mut dyn Write) -> fmt::Result,
    ) -> Result<Self, KernelFailure> {
        let mut writer = ObservedHostWriter {
            bytes: HostVec::new_in(allocator.clone()),
            work,
            refusal: None,
        };
        let result = original_formatter(&mut writer);
        if let Some(cause) = writer.refusal {
            return Err(cause);
        }
        if result.is_err() {
            return Err(internal(
                "original diagnostic formatter failed without an observed refusal",
            ));
        }
        let bytes = writer.bytes;
        Ok(Self(HostShared::try_new(bytes, allocator.clone(), work)?))
    }
    pub(crate) fn message(&self) -> &str {
        // SAFETY: only fmt::Write valid UTF8 str fragments are appended during construction.
        unsafe { std::str::from_utf8_unchecked(self.0.as_slice()) }
    }
    pub(crate) fn retained_bytes(&self) -> usize {
        self.0.block_bytes() + self.0.capacity()
    }
    pub(crate) fn ptr_eq(&self, other: &Self) -> bool {
        self.0.ptr_eq(&other.0)
    }
}
struct ObservedHostWriter<'a, 'b> {
    bytes: HostVec<u8, HostAggregateAllocator>,
    work: &'b mut EvaluationCheckpoints<'a>,
    refusal: Option<KernelFailure>,
}
impl Write for ObservedHostWriter<'_, '_> {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        if self.refusal.is_some() {
            return Err(fmt::Error);
        }
        let result = (|| {
            if text.len() > self.bytes.capacity() - self.bytes.len() {
                self.work.flush()?;
                self.bytes
                    .try_reserve(text.len())
                    .map_err(|_| self.bytes.allocator().take_failure())?;
                self.work.flush()?;
            }
            // One real byte copy quantum per step; no second formatter/type decoder.
            for byte in text.bytes() {
                self.bytes.push(byte);
                self.work.step()?;
            }
            Ok::<_, KernelFailure>(())
        })();
        match result {
            Ok(()) => Ok(()),
            Err(cause) => {
                self.refusal = Some(cause);
                Err(fmt::Error)
            }
        }
    }
}
#[cfg(test)]
#[path = "aggregate_invocation_backing_tests.rs"]
mod tests;
