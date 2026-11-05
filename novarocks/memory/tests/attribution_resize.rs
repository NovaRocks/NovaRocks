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
use novarocks_memory::attribution::{AttributingAllocator, binding};
use novarocks_memory::lane::{RecordStorePin, ResponsibilityClass, global_store};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
fn layout(size: usize) -> Layout {
    Layout::from_size_align(size, 8).unwrap()
}
fn owner() -> novarocks_memory::lane::RecordOwner {
    RecordStorePin::global()
        .acquire(
            7,
            ResponsibilityClass::Query,
            novarocks_memory::lane::RecordRegion::Observation,
        )
        .unwrap()
}
fn bytes(r: novarocks_memory::lane::RecordRef) -> i64 {
    global_store().snapshot_ref(r).unwrap().tagged_bytes
}

use std::sync::atomic::{AtomicBool, Ordering};
struct Failable {
    fail: AtomicBool,
}
unsafe impl GlobalAlloc for Failable {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        if self.fail.load(Ordering::Relaxed) {
            std::ptr::null_mut()
        } else {
            unsafe { System.alloc(l) }
        }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe { System.dealloc(p, l) };
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        if self.fail.load(Ordering::Relaxed) {
            std::ptr::null_mut()
        } else {
            unsafe { System.realloc(p, l, n) }
        }
    }
}
#[test]
fn all_resize_routes_reconcile_bands_and_preserve_original_token_owner() {
    let a = AttributingAllocator::new(System);
    let x = owner();
    let y = owner();
    let rx = x.reference();
    let ry = y.reference();
    let mut l = layout(100);
    let mut p = unsafe { a.alloc(l) };
    assert!(!p.is_null());
    let old = unsafe { binding::install_ambient(rx) };
    p = unsafe { a.realloc(p, l, 200) };
    assert!(!p.is_null());
    l = layout(200);
    p = unsafe { a.realloc(p, l, 512) };
    assert!(!p.is_null());
    l = layout(512);
    unsafe { binding::restore_ambient(old) };
    assert_eq!(bytes(rx), 520);
    let old = unsafe { binding::install_ambient(ry) };
    p = unsafe { a.realloc(p, l, 1025) };
    assert!(!p.is_null());
    l = layout(1025);
    unsafe { binding::restore_ambient(old) };
    assert_eq!(bytes(rx), 1033);
    assert_eq!(bytes(ry), 0);
    assert_eq!(global_store().snapshot_ref(rx).unwrap().outstanding, 1);
    p = unsafe { a.realloc(p, l, 513) };
    assert!(!p.is_null());
    l = layout(513);
    assert_eq!(bytes(rx), 521);
    p = unsafe { a.realloc(p, l, 128) };
    assert!(!p.is_null());
    l = layout(128);
    assert_eq!(bytes(rx), 0);
    let s = a.band_snapshot();
    assert_eq!(s.small.live_bytes, 128);
    assert_eq!(s.tagged.live_bytes, 0);
    assert_eq!(s.total.live_bytes, 128);
    assert_eq!(s.total.allocations, 1);
    assert_eq!(s.total.reallocations, 5);
    assert_eq!(s.small_deallocations, 0);
    assert_eq!(s.tagged_deallocations, 0);
    assert_eq!(s.total_deallocations, 0);
    assert_eq!(s.total.allocated_total_bytes, 1033);
    assert_eq!(s.total.deallocated_total_bytes, 905);
    unsafe { a.dealloc(p, l) };
    assert_eq!(a.band_snapshot().small_deallocations, 1);
    assert_eq!(a.band_snapshot().tagged_deallocations, 0);
    assert_eq!(a.band_snapshot().total_deallocations, 1);
    assert_eq!(
        a.snapshot().allocated_total_bytes,
        a.snapshot().deallocated_total_bytes
    );
}
#[test]
fn failure_preserves_pointer_contents_token_and_all_facts_in_every_route() {
    for (old_size, new_size) in [(64, 128), (64, 512), (512, 1024), (512, 64)] {
        let a = AttributingAllocator::new(Failable {
            fail: AtomicBool::new(false),
        });
        let o = owner();
        let r = o.reference();
        let previous = unsafe { binding::install_ambient(r) };
        let p = unsafe { a.alloc(layout(old_size)) };
        assert!(!p.is_null());
        unsafe {
            std::ptr::write_bytes(p, 0x7b, old_size);
            binding::restore_ambient(previous)
        };
        let before = global_store().snapshot_ref(r).unwrap();
        let counters = a.band_snapshot();
        a.inner().fail.store(true, Ordering::Relaxed);
        assert!(unsafe { a.realloc(p, layout(old_size), new_size) }.is_null());
        assert_eq!(global_store().snapshot_ref(r).unwrap(), before);
        assert_eq!(a.snapshot().live_bytes, counters.total.live_bytes);
        assert_eq!(
            a.snapshot().allocated_total_bytes,
            counters.total.allocated_total_bytes
        );
        assert_eq!(
            a.snapshot().deallocated_total_bytes,
            counters.total.deallocated_total_bytes
        );
        assert_eq!(a.snapshot().reallocations, 0);
        assert_eq!(a.snapshot().failures, 1);
        assert_eq!(a.band_snapshot().total_deallocations, 0);
        assert!(
            unsafe { std::slice::from_raw_parts(p, old_size) }
                .iter()
                .all(|b| *b == 0x7b)
        );
        if old_size >= 512 {
            assert_eq!(
                unsafe { novarocks_memory::lane::RecordRef::read(p.add(old_size)) },
                r
            );
        }
        unsafe { a.dealloc(p, layout(old_size)) };
    }
}
#[test]
fn checked_resize_overflow_keeps_the_old_allocation_live() {
    let a = AttributingAllocator::new(System);
    let p = unsafe { a.alloc(layout(512)) };
    assert!(!p.is_null());
    let before = a.snapshot();
    assert!(unsafe { a.realloc(p, layout(512), isize::MAX as usize - 7) }.is_null());
    assert_eq!(a.snapshot().live_bytes, before.live_bytes);
    assert_eq!(a.snapshot().failures, 1);
    unsafe { a.dealloc(p, layout(512)) };
}

#[test]
fn failed_alloc_and_zeroed_alloc_publish_no_lane_facts() {
    let a = AttributingAllocator::new(Failable {
        fail: AtomicBool::new(true),
    });
    let o = owner();
    let r = o.reference();
    let before = global_store().snapshot_ref(r).unwrap();
    let previous = unsafe { binding::install_ambient(r) };
    for size in [64, 512] {
        assert!(unsafe { a.alloc(layout(size)) }.is_null());
        assert!(unsafe { a.alloc_zeroed(layout(size)) }.is_null());
    }
    unsafe { binding::restore_ambient(previous) };
    assert_eq!(global_store().snapshot_ref(r).unwrap(), before);
    assert_eq!(a.snapshot().failures, 4);
    assert_eq!(a.band_snapshot().total_deallocations, 0);
    assert_eq!(a.snapshot().allocations, 0);
    assert_eq!(a.snapshot().live_bytes, 0);
}

struct AlwaysMoving;

// SAFETY: every allocation uses System with the exact supplied layout. Resize
// allocates while the old block remains live, copies the required prefix, then
// releases the old block. Failure leaves the old block untouched.
unsafe impl GlobalAlloc for AlwaysMoving {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        unsafe { System.alloc(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe { System.dealloc(p, l) };
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        let Ok(next_layout) = Layout::from_size_align(n, l.align()) else {
            return std::ptr::null_mut();
        };
        let next = unsafe { System.alloc(next_layout) };
        if !next.is_null() {
            unsafe {
                std::ptr::copy_nonoverlapping(p, next, l.size().min(n));
                System.dealloc(p, l);
            }
        }
        next
    }
}

/// One block with spare backing solely to force the in-place allocator contract.
/// Cell deliberately prevents shared concurrent use of this single-block fixture.
/// Its backing size is not an attribution or performance oracle.
struct AlwaysInPlace {
    pointer: Cell<*mut u8>,
    requested: Cell<usize>,
    alignment: Cell<usize>,
    invalid_layout: Cell<bool>,
}
impl AlwaysInPlace {
    const CAPACITY: usize = 4096;
    const fn new() -> Self {
        Self {
            pointer: Cell::new(std::ptr::null_mut()),
            requested: Cell::new(0),
            alignment: Cell::new(0),
            invalid_layout: Cell::new(false),
        }
    }
    fn matches(&self, p: *mut u8, l: Layout) -> bool {
        let matches = self.pointer.get() == p
            && self.requested.get() == l.size()
            && self.alignment.get() == l.align();
        if !matches {
            self.invalid_layout.set(true);
        }
        matches
    }
}
// SAFETY: this single-thread fixture admits one allocation and retains its real
// System backing through every bounded resize. It verifies the wrapper's exact
// current layout and frees with the original backing layout, without unwinding.
unsafe impl GlobalAlloc for AlwaysInPlace {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        if !self.pointer.get().is_null() || l.size() > Self::CAPACITY {
            return std::ptr::null_mut();
        }
        let Ok(backing) = Layout::from_size_align(Self::CAPACITY, l.align()) else {
            return std::ptr::null_mut();
        };
        let p = unsafe { System.alloc(backing) };
        if !p.is_null() {
            self.pointer.set(p);
            self.requested.set(l.size());
            self.alignment.set(l.align());
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        if !self.matches(p, l) {
            return;
        }
        let Ok(backing) = Layout::from_size_align(Self::CAPACITY, l.align()) else {
            return;
        };
        unsafe { System.dealloc(p, backing) };
        self.pointer.set(std::ptr::null_mut());
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        if !self.matches(p, l) || n > Self::CAPACITY {
            return std::ptr::null_mut();
        }
        self.requested.set(n);
        p
    }
}

fn verify_deterministic_resize<A: GlobalAlloc>(
    a: &AttributingAllocator<A>,
    old_size: usize,
    new_size: usize,
    moves: bool,
) {
    let x = owner();
    let y = owner();
    let rx = x.reference();
    let ry = y.reference();
    let old_binding = unsafe { binding::install_ambient(rx) };
    let p = unsafe { a.alloc(layout(old_size)) };
    assert!(!p.is_null());
    unsafe {
        std::ptr::write_bytes(p, 0x7b, old_size);
        binding::restore_ambient(old_binding);
    }
    let old_binding = unsafe { binding::install_ambient(ry) };
    let next = unsafe { a.realloc(p, layout(old_size), new_size) };
    assert!(!next.is_null());
    assert_eq!(
        next != p,
        moves,
        "the fixture must exercise its claimed route"
    );
    assert!(
        unsafe { std::slice::from_raw_parts(next, old_size.min(new_size)) }
            .iter()
            .all(|byte| *byte == 0x7b)
    );
    if new_size >= 512 {
        let expected_owner = if old_size >= 512 { rx } else { ry };
        assert_eq!(
            unsafe { novarocks_memory::lane::RecordRef::read(next.add(new_size)) },
            expected_owner
        );
    }
    unsafe { binding::restore_ambient(old_binding) };
    let tagged = if new_size >= 512 {
        (new_size + 8) as u64
    } else {
        0
    };
    let retains_x = old_size >= 512 && new_size >= 512;
    let acquires_y = old_size < 512 && new_size >= 512;
    for (r, owns) in [(rx, retains_x), (ry, acquires_y)] {
        let facts = global_store().snapshot_ref(r).unwrap();
        assert_eq!(facts.tagged_bytes, if owns { tagged as i64 } else { 0 });
        assert_eq!(facts.outstanding, i64::from(owns));
    }
    let counters = a.band_snapshot();
    assert_eq!(
        counters.total.live_bytes,
        new_size as u64 + u64::from(new_size >= 512) * 8
    );
    assert_eq!(counters.tagged.live_bytes, tagged);
    assert_eq!(
        counters.small.live_bytes,
        if new_size < 512 { new_size as u64 } else { 0 }
    );
    assert_eq!(counters.total.allocations, 1);
    assert_eq!(counters.total.reallocations, 1);
    assert_eq!(counters.total_deallocations, 0);
    unsafe { a.dealloc(next, layout(new_size)) };
    assert_eq!(a.snapshot().live_bytes, 0);
    assert_eq!(
        a.snapshot().allocated_total_bytes,
        a.snapshot().deallocated_total_bytes
    );
    assert_eq!(a.band_snapshot().total_deallocations, 1);
    for r in [rx, ry] {
        let facts = global_store().snapshot_ref(r).unwrap();
        assert_eq!(facts.tagged_bytes, 0);
        assert_eq!(facts.outstanding, 0);
    }
}

#[test]
fn in_place_resize_preserves_contents_owner_and_facts_in_every_route() {
    for (old, new) in [
        (64, 128),
        (128, 64),
        (64, 512),
        (512, 64),
        (512, 1025),
        (1025, 513),
    ] {
        let a = AttributingAllocator::new(AlwaysInPlace::new());
        verify_deterministic_resize(&a, old, new, false);
        assert!(!a.inner().invalid_layout.get());
        assert!(a.inner().pointer.get().is_null());
    }
}

#[test]
fn moving_resize_preserves_contents_owner_and_facts_in_every_route() {
    for (old, new) in [
        (64, 128),
        (128, 64),
        (64, 512),
        (512, 64),
        (512, 1025),
        (1025, 513),
    ] {
        let a = AttributingAllocator::new(AlwaysMoving);
        verify_deterministic_resize(&a, old, new, true);
    }
}
