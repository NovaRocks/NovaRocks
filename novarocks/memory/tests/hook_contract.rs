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

#![expect(
    clippy::disallowed_methods,
    reason = "Integration tests own and join their threads and runtime tasks; see ADR-0218."
)]

mod common;
use common::*;
use novarocks_memory::*;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

// A const TLS cell has no heap allocation or destructor. Counting only this
// thread avoids noise from concurrently running tests and the test harness.
thread_local! {
    static PROBE_ENABLED: Cell<bool> = const { Cell::new(false) };
    static HEAP_CALLS: Cell<u64> = const { Cell::new(0) };
}
struct HookHeapProbe;
#[global_allocator]
static ALLOCATOR: HookHeapProbe = HookHeapProbe;
fn count_heap_call() {
    if PROBE_ENABLED.try_with(Cell::get).unwrap_or(false) {
        let _ = HEAP_CALLS.try_with(|count| count.set(count.get() + 1));
    }
}
// SAFETY: all allocation operations are forwarded unchanged to System. The
// instrumentation accesses only non-allocating, destructor-free TLS cells.
unsafe impl GlobalAlloc for HookHeapProbe {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count_heap_call();
        // SAFETY: caller's valid layout is forwarded unchanged.
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        count_heap_call();
        // SAFETY: caller's valid layout is forwarded unchanged.
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        count_heap_call();
        // SAFETY: caller's live pointer and valid layouts are forwarded.
        unsafe { System.realloc(pointer, layout, size) }
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        count_heap_call();
        // SAFETY: caller's live pointer and matching layout are forwarded.
        unsafe { System.dealloc(pointer, layout) }
    }
}
struct ProbeGuard;
impl Drop for ProbeGuard {
    fn drop(&mut self) {
        PROBE_ENABLED.with(|enabled| enabled.set(false));
    }
}
fn without_heap_calls<R>(operation: impl FnOnce() -> R) -> R {
    HEAP_CALLS.with(|count| count.set(0));
    PROBE_ENABLED.with(|enabled| enabled.set(true));
    let guard = ProbeGuard;
    let result = operation();
    drop(guard);
    assert_eq!(
        HEAP_CALLS.with(Cell::get),
        0,
        "allocator hook allocated, resized or destroyed heap metadata"
    );
    result
}

#[test]
fn stock_hit_miss_threshold_and_over_authorization_hooks_do_not_touch_heap_or_root() {
    let a = authority(32_768);
    let q = work(&a);
    let domain = q.create_domain(1_024).unwrap();
    let root_before = a.root().interactions();
    let mut scope = domain.activate(128, 32).unwrap();
    let first = without_heap_calls(|| scope.record_allocation(64));
    assert_eq!(scope.stock_bytes(), 64);
    assert!(!scope.threshold_triggered());
    let second = without_heap_calls(|| scope.record_allocation(96));
    assert_eq!(scope.stock_bytes(), 0);
    assert!(!scope.threshold_triggered());
    let third = without_heap_calls(|| scope.record_allocation(1_024));
    assert!(scope.threshold_triggered());
    without_heap_calls(|| free(second, 96));
    assert!(
        scope.threshold_triggered(),
        "remote release does not erase a boundary signal"
    );
    assert_eq!(
        a.root().interactions(),
        root_before,
        "hook does not refill the parent/root"
    );
    let receipt = scope.finish();
    assert_eq!(receipt.accepted_live, 1_088);
    assert_eq!(receipt.debt, 64);
    without_heap_calls(|| {
        free(first, 64);
        free(third, 1_024);
    });
}

#[test]
fn remote_and_post_teardown_late_free_do_not_allocate_or_destroy_record_in_hook() {
    let a = authority(32_768);
    let q = work(&a);
    let domain = q.create_domain(512).unwrap();
    let mut scope = domain.activate(512, 0).unwrap();
    let early = scope.record_allocation(256);
    let late = scope.record_allocation(256);
    scope.finish();
    std::thread::spawn(move || without_heap_calls(|| free(early, 256)))
        .join()
        .unwrap();
    q.retire(&exited()).unwrap();
    drop(domain);
    drop(q);
    std::thread::spawn(move || without_heap_calls(|| free(late, 256)))
        .join()
        .unwrap();
    // The final hook only publishes free facts. Core metadata is destroyed
    // later by an explicit non-hook maintenance caller.
    assert_eq!(
        a.pressure_projection().residual_metadata,
        OWNER_METADATA_BYTES
    );
    a.request_maintenance(MaintenanceReason::ExplicitLocalReclaim);
    while !a.maintain(64).complete {}
    assert_eq!(a.pressure_projection().residual_metadata, 0);
}

#[test]
fn zero_byte_publication_and_final_free_use_no_heap_calls_and_retain_an_origin() {
    let a = authority(16_384);
    let q = work(&a);
    let domain = q.create_domain(64).unwrap();
    let mut scope = domain.activate(64, 0).unwrap();
    let origin = without_heap_calls(|| scope.record_allocation(0));
    scope.finish();
    q.retire(&exited()).unwrap();
    drop(domain);
    drop(q);
    a.request_maintenance(MaintenanceReason::ExplicitLocalReclaim);
    while !a.maintain(64).complete {}
    assert_eq!(
        a.pressure_projection().residual_metadata,
        OWNER_METADATA_BYTES
    );
    without_heap_calls(|| free(origin, 0));
    a.request_maintenance(MaintenanceReason::ExplicitLocalReclaim);
    while !a.maintain(64).complete {}
    assert_eq!(a.pressure_projection().residual_metadata, 0);
}
