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

//! Exact host-funded allocator metadata and clone lifetime.
use super::*;
use crate::KernelDiagnostic;
use crate::kernel_control::{internal, invalid};
#[derive(Default)]
struct Ledger {
    attempts: usize,
    bytes: usize,
    peak: usize,
    live: Vec<(usize, Layout)>,
    opaque_attempts: usize,
    opaque_releases: usize,
    opaque_bytes: usize,
    opaque_peak: usize,
}
#[derive(Default)]
struct Host {
    ledger: Mutex<Ledger>,
    refusal: Mutex<Option<(usize, KernelFailure)>>,
    opaque_refusal: Mutex<Option<KernelFailure>>,
}
impl AggregateStateAllocator for Host {
    fn opaque_allocation_host(&self) -> Option<&dyn crate::opaque_memory::OpaqueAllocationHost> {
        Some(self)
    }
    fn allocate(&self, layout: Layout) -> Result<NonNull<u8>, KernelFailure> {
        assert_ne!(layout.size(), 0);
        let mut ledger = self.ledger.lock().unwrap();
        let at = ledger.attempts;
        ledger.attempts += 1;
        if let Some((stop, cause)) = &*self.refusal.lock().unwrap() {
            if *stop == at {
                return Err(cause.clone());
            }
        }
        let pointer = NonNull::new(unsafe { std::alloc::alloc(layout) })
            .ok_or(KernelFailure::ResourceExhausted)?;
        ledger.bytes += layout.size();
        ledger.peak = ledger.peak.max(ledger.bytes);
        ledger.live.push((pointer.as_ptr().addr(), layout));
        Ok(pointer)
    }
    unsafe fn release(&self, pointer: NonNull<u8>, layout: Layout) {
        let mut ledger = self.ledger.lock().unwrap();
        let at = ledger
            .live
            .iter()
            .position(|(address, actual)| *address == pointer.as_ptr().addr() && *actual == layout)
            .expect("exact block released once");
        ledger.live.swap_remove(at);
        ledger.bytes -= layout.size();
        unsafe { std::alloc::dealloc(pointer.as_ptr(), layout) };
    }
}
impl crate::opaque_memory::OpaqueAllocationHost for Host {
    fn reserve_opaque(&self, bytes: usize) -> Result<(), KernelFailure> {
        let mut ledger = self.ledger.lock().unwrap();
        ledger.opaque_attempts += 1;
        if let Some(cause) = self.opaque_refusal.lock().unwrap().as_ref() {
            return Err(cause.clone());
        }
        ledger.opaque_bytes += bytes;
        ledger.opaque_peak = ledger.opaque_peak.max(ledger.opaque_bytes);
        Ok(())
    }
    fn release_opaque(&self, bytes: usize) {
        let mut ledger = self.ledger.lock().unwrap();
        ledger.opaque_bytes = ledger.opaque_bytes.checked_sub(bytes).unwrap();
        ledger.opaque_releases += 1;
    }
}
fn arm_refusal(host: &Host, cause: KernelFailure) {
    let next = host.ledger.lock().unwrap().attempts;
    *host.refusal.lock().unwrap() = Some((next, cause));
}
fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        invalid("host original"),
        internal("host original"),
        KernelFailure::Operational(KernelDiagnostic::new("host original")),
        KernelFailure::InstanceFailed,
    ]
}

#[test]
fn metadata_constructor_actual_refusal_preserves_all_seven_causes_without_publication() {
    for cause in causes() {
        let host = Arc::new(Host::default());
        arm_refusal(&host, cause.clone());
        assert!(matches!(HostAggregateAllocator::try_new(host.clone()),Err(e) if e==cause));
        let ledger = host.ledger.lock().unwrap();
        assert_eq!(ledger.attempts, 1);
        assert_eq!(ledger.bytes, 0);
        assert!(ledger.live.is_empty());
    }
}
#[test]
fn metadata_is_one_actual_block_and_clones_release_it_only_after_exact_last_drop() {
    assert_eq!(
        std::mem::size_of::<HostAggregateAllocator>(),
        std::mem::size_of::<usize>()
    );
    let host = Arc::new(Host::default());
    let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
    let metadata = allocator.metadata_bytes();
    assert_eq!(metadata, Layout::new::<HostAllocatorInner>().size());
    let first = allocator.clone();
    let second = first.clone();
    assert_eq!(host.ledger.lock().unwrap().attempts, 1);
    assert_eq!(host.ledger.lock().unwrap().bytes, metadata);
    let layout = Layout::from_size_align(513, 16).unwrap();
    let bytes = allocator.allocate(layout).unwrap().cast::<u8>();
    assert_eq!(host.ledger.lock().unwrap().bytes, metadata + 513);
    drop(allocator);
    drop(first);
    assert_eq!(host.ledger.lock().unwrap().live.len(), 2);
    unsafe { second.deallocate(bytes, layout) };
    assert_eq!(host.ledger.lock().unwrap().bytes, metadata);
    drop(second);
    let ledger = host.ledger.lock().unwrap();
    assert_eq!(ledger.bytes, 0);
    assert!(ledger.live.is_empty());
}
#[test]
fn shared_clone_journal_keeps_actual_cause_but_separate_state_journal_is_isolated() {
    for cause in causes() {
        // Metadata first succeeds; the first container allocation rejects.
        let host = Arc::new(Host::default());
        let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
        arm_refusal(&host, cause.clone());
        let child = allocator.clone();
        assert!(child.allocate(Layout::new::<u64>()).is_err());
        // Another state has a genuinely independent block and journal even
        // while this state's cause is still pending on all its clones.
        let independent = HostAggregateAllocator::try_new(host.clone()).unwrap();
        assert!(independent.inner().failure.lock().unwrap().is_none());
        assert!(!std::ptr::eq(allocator.inner(), independent.inner()));
        let other = independent
            .allocate(Layout::new::<u64>())
            .unwrap()
            .cast::<u8>();
        unsafe { independent.deallocate(other, Layout::new::<u64>()) };
        assert_eq!(allocator.take_failure(), cause);
        // The consumed journal admits retry, rather than preserving stale cause.
        let bytes = child.allocate(Layout::new::<u64>()).unwrap().cast::<u8>();
        unsafe { child.deallocate(bytes, Layout::new::<u64>()) };
        drop(independent);
        drop(child);
        drop(allocator);
        assert_eq!(host.ledger.lock().unwrap().bytes, 0);
    }
}
#[test]
fn zero_sized_allocation_uses_no_host_block_and_does_not_change_metadata_lifetime() {
    let host = Arc::new(Host::default());
    let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
    let layout = Layout::from_size_align(0, 64).unwrap();
    let bytes = allocator.allocate(layout).unwrap().cast::<u8>();
    unsafe { allocator.deallocate(bytes, layout) };
    assert_eq!(host.ledger.lock().unwrap().attempts, 1);
    assert_eq!(
        host.ledger.lock().unwrap().bytes,
        allocator.metadata_bytes()
    );
    drop(allocator);
    assert_eq!(host.ledger.lock().unwrap().bytes, 0);
}

#[cfg(any(
    target_os = "linux",
    all(target_os = "macos", target_pointer_width = "64")
))]
mod covered_journal {
    use super::*;

    #[test]
    fn covered_journal_refusal_precedes_metadata_and_preserves_all_seven_causes() {
        for cause in causes() {
            let host = Arc::new(Host::default());
            *host.opaque_refusal.lock().unwrap() = Some(cause.clone());
            assert!(matches!(
                HostAggregateAllocator::try_new_with_journal(host.clone()),
                Err(actual) if actual == cause
            ));
            let ledger = host.ledger.lock().unwrap();
            assert_eq!(ledger.opaque_attempts, 1);
            assert_eq!(ledger.attempts, 0);
            assert_eq!(ledger.bytes, 0);
            assert_eq!(ledger.opaque_bytes, 0);
            assert!(ledger.live.is_empty());
        }
    }

    #[test]
    fn covered_metadata_refusal_releases_prior_journal_reservation() {
        for cause in causes() {
            let host = Arc::new(Host::default());
            arm_refusal(&host, cause.clone());
            assert!(matches!(
                HostAggregateAllocator::try_new_with_journal(host.clone()),
                Err(actual) if actual == cause
            ));
            let ledger = host.ledger.lock().unwrap();
            assert_eq!(ledger.attempts, 1);
            assert_eq!(ledger.opaque_attempts, 1);
            assert_eq!(ledger.bytes, 0);
            assert_eq!(ledger.opaque_bytes, 0);
            assert!(ledger.live.is_empty());
        }
    }

    #[test]
    fn journal_lease_follows_clones_through_cross_thread_last_drop() {
        let host = Arc::new(Host::default());
        let allocator = HostAggregateAllocator::try_new_with_journal(host.clone()).unwrap();
        let metadata = allocator.metadata_bytes();
        let journal = allocator.journal_backing_bytes();
        let child = allocator.clone();
        let last = child.clone();
        // Exercise the original container journal after the exclusive warm.
        arm_refusal(&host, KernelFailure::DeadlineExceeded);
        assert!(child.allocate(Layout::new::<u64>()).is_err());
        assert_eq!(child.take_failure(), KernelFailure::DeadlineExceeded);
        drop(allocator);
        drop(child);
        {
            let ledger = host.ledger.lock().unwrap();
            assert_eq!(ledger.bytes, metadata);
            assert_eq!(ledger.opaque_bytes, journal);
            assert_eq!(ledger.opaque_peak, journal);
            assert_eq!(ledger.opaque_attempts, 1);
        }
        std::thread::spawn(move || drop(last)).join().unwrap();
        let ledger = host.ledger.lock().unwrap();
        assert_eq!(ledger.bytes, 0);
        assert_eq!(ledger.opaque_bytes, 0);
        assert!(ledger.live.is_empty());
    }

    #[test]
    fn covered_constructor_requires_opaque_capability_even_without_separate_backing() {
        struct TypedOnly(Host);
        impl AggregateStateAllocator for TypedOnly {
            fn allocate(&self, layout: Layout) -> Result<NonNull<u8>, KernelFailure> {
                self.0.allocate(layout)
            }
            unsafe fn release(&self, block: NonNull<u8>, layout: Layout) {
                // SAFETY: this adapter forwards the exact original host block.
                unsafe { self.0.release(block, layout) };
            }
        }
        let host = Arc::new(TypedOnly(Host::default()));
        assert!(matches!(
            HostAggregateAllocator::try_new_with_journal(host.clone()),
            Err(KernelFailure::InvalidProgram(_))
        ));
        assert_eq!(host.0.ledger.lock().unwrap().attempts, 0);
    }
}
