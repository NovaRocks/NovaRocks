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

#![cfg(not(loom))]
mod common;
use novarocks_memory::attribution::{
    AttributingAllocator, ThreadRole, binding, explicit::ExplicitOwner, freeze_thread_roles,
    register_thread_role, set_current_thread_role,
};
use novarocks_memory::lane::{
    RecordRef, RecordStorePin, ResponsibilityClass, UNATTRIBUTED_SHARDS, global_store,
};
use std::alloc::{GlobalAlloc, Layout, System};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
static ARMED: AtomicBool = AtomicBool::new(false);
static UNEXPECTED: AtomicU64 = AtomicU64::new(0);
struct MetadataProbe;
unsafe impl GlobalAlloc for MetadataProbe {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        if ARMED.load(Ordering::Relaxed) {
            UNEXPECTED.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe { System.dealloc(p, l) };
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        if ARMED.load(Ordering::Relaxed) {
            UNEXPECTED.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.realloc(p, l, n) }
    }
}
#[global_allocator]
static GLOBAL: MetadataProbe = MetadataProbe;
#[test]
fn hooks_make_no_nested_metadata_allocations_including_first_tls_access() {
    let a = AttributingAllocator::new(System);
    let o = RecordStorePin::global()
        .acquire(
            7,
            ResponsibilityClass::Query,
            novarocks_memory::lane::RecordRegion::Observation,
        )
        .unwrap();
    let l = Layout::from_size_align(512, 8).unwrap();
    let before = UNEXPECTED.load(Ordering::Relaxed);
    // This unbound allocation is the thread's first attribution TLS access.
    let p = hook_operation(|| unsafe { a.alloc_zeroed(l) });
    assert!(!p.is_null());
    let q = hook_operation(|| unsafe { a.realloc(p, l, 2048) });
    assert!(!q.is_null());
    hook_operation(|| unsafe { a.dealloc(q, Layout::from_size_align(2048, 8).unwrap()) });
    assert_eq!(UNEXPECTED.load(Ordering::Relaxed), before);
    assert_eq!(a.snapshot().live_bytes, 0);

    // Process composition and labels may allocate or lock; finish them before
    // arming the hook probe. The new role branches consume only the TLS number.
    let role = register_thread_role("native_handler").unwrap();
    freeze_thread_roles();
    set_current_thread_role(role).unwrap();
    let authority = common::authority(64 * 1024);
    let closed_account = common::work(&authority);
    closed_account.retire(&common::exited()).unwrap();
    let uncovered = closed_account.create_lane_shards();
    assert!(uncovered.coverage().is_err());
    let uncovered_explicit = uncovered.explicit_owner();

    let before = UNEXPECTED.load(Ordering::Relaxed);
    let unbound = allocation_cycle(&a);
    // SAFETY: this same-thread synchronous assembly retains the real outer
    // owner until exact restoration. Uncovered scopes use only safe APIs.
    let previous = unsafe { binding::install_ambient(o.reference()) };
    let ambient_before_shadow = allocation_cycle(&a);
    let ambient_shadow = uncovered.run(|| allocation_cycle(&a));
    let explicit_wins = uncovered.run(|| {
        // SAFETY: the real explicit owner is held until exact restoration.
        let explicit_previous = unsafe { binding::install_explicit(o.reference()) };
        let cycle = allocation_cycle(&a);
        unsafe { binding::restore_explicit(explicit_previous) };
        cycle
    });
    let ambient_restored = allocation_cycle(&a);
    let explicit_shadow = explicit_allocation_cycle(&a, &uncovered_explicit);
    unsafe { binding::restore_ambient(previous) };
    binding::flush_current();
    assert_eq!(UNEXPECTED.load(Ordering::Relaxed), before);
    for cycle in [unbound, ambient_shadow, explicit_shadow] {
        for reference in [cycle.0, cycle.1] {
            assert_eq!(reference.generation, 0);
            assert_eq!(
                reference.index as usize / UNATTRIBUTED_SHARDS as usize,
                role.index()
            );
        }
        assert_eq!(cycle.0, cycle.1);
    }
    for cycle in [ambient_before_shadow, explicit_wins, ambient_restored] {
        assert_eq!(cycle, (o.reference(), o.reference()));
    }
    assert_eq!(a.snapshot().live_bytes, 0);
    assert_eq!(global_store().faults.snapshot().orphan_events, 0);
    assert_eq!(global_store().faults.snapshot().pinned_slots, 0);
    set_current_thread_role(ThreadRole::UNKNOWN).unwrap();
}

// Arm only the actual allocator operation. Owner creation, scope entry/exit,
// explicit helper bookkeeping, token inspection and assertions stay outside.
fn hook_operation<T>(operation: impl FnOnce() -> T) -> T {
    ARMED.store(true, Ordering::Relaxed);
    let result = operation();
    ARMED.store(false, Ordering::Relaxed);
    result
}

fn allocation_cycle(allocator: &AttributingAllocator<System>) -> (RecordRef, RecordRef) {
    let layout = Layout::from_size_align(512, 8).unwrap();
    // SAFETY: nonzero valid layouts, exact live pointer at each operation, and
    // token reads remain within the successful tagged allocations' storage.
    unsafe {
        let ptr = hook_operation(|| allocator.alloc_zeroed(layout));
        assert!(!ptr.is_null());
        let initial = RecordRef::read(ptr.add(512));
        let grown = hook_operation(|| allocator.realloc(ptr, layout, 2048));
        assert!(!grown.is_null());
        let final_owner = RecordRef::read(grown.add(2048));
        hook_operation(|| allocator.dealloc(grown, Layout::from_size_align(2048, 8).unwrap()));
        (initial, final_owner)
    }
}

fn explicit_allocation_cycle(
    allocator: &AttributingAllocator<System>,
    owner: &ExplicitOwner,
) -> (RecordRef, RecordRef) {
    let initial_layout = Layout::from_size_align(512, 8).unwrap();
    let grown_layout = Layout::from_size_align(2048, 8).unwrap();
    let ptr = owner
        .allocate_with(initial_layout, || {
            // SAFETY: valid nonzero layout, success checked before further access.
            NonNull::new(hook_operation(|| unsafe {
                allocator.alloc_zeroed(initial_layout)
            }))
            .ok_or(())
        })
        .unwrap();
    // SAFETY: the live tagged allocation owns its initialized tail token.
    let initial = unsafe { RecordRef::read(ptr.as_ptr().add(512)) };
    // SAFETY: the old block belongs to this helper; the underlying realloc
    // returns the genuine replacement or leaves the old block unchanged.
    let grown = unsafe {
        owner.resize_with(initial_layout, grown_layout, || {
            NonNull::new(hook_operation(|| {
                allocator.realloc(ptr.as_ptr(), initial_layout, 2048)
            }))
            .ok_or(())
        })
    }
    .unwrap();
    // SAFETY: successful realloc produced the live extended tagged block.
    let final_owner = unsafe { RecordRef::read(grown.as_ptr().add(2048)) };
    // SAFETY: release this helper's exact allocation once using the exact layout.
    unsafe {
        owner.deallocate_with(grown_layout, || {
            hook_operation(|| allocator.dealloc(grown.as_ptr(), grown_layout));
        });
    }
    (initial, final_owner)
}
