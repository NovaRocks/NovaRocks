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

#[test]
fn same_and_cross_thread_release_preserve_source_after_owner_drop() {
    let a = std::sync::Arc::new(AttributingAllocator::new(System));
    let o = owner();
    let r = o.reference();
    let previous = unsafe { binding::install_ambient(r) };
    let p = unsafe { a.alloc(layout(512)) };
    assert!(!p.is_null());
    let q = unsafe { a.alloc(layout(513)) };
    assert!(!q.is_null());
    unsafe { binding::restore_ambient(previous) };
    assert_eq!(bytes(r), 1041);
    unsafe { a.dealloc(p, layout(512)) };
    assert_eq!(a.band_snapshot().tagged_deallocations, 1);
    drop(o);
    assert_eq!(global_store().snapshot_ref(r).unwrap().outstanding, 1);
    struct Block(*mut u8);
    unsafe impl Send for Block {}
    impl Block {
        unsafe fn release(self, a: &AttributingAllocator<System>) {
            unsafe { a.dealloc(self.0, layout(513)) };
        }
    }
    let block = Block(q);
    let other = a.clone();
    std::thread::spawn(move || unsafe { block.release(&other) })
        .join()
        .unwrap();
    assert_eq!(bytes(r), 0);
    assert_eq!(global_store().snapshot_ref(r).unwrap().outstanding, 0);
    global_store().reclaim(usize::MAX);
    assert!(global_store().snapshot_ref(r).is_none());
    assert_eq!(a.snapshot().live_bytes, 0);
    assert_eq!(a.band_snapshot().tagged_deallocations, 2);
    assert_eq!(a.band_snapshot().total_deallocations, 2);
}
#[test]
fn tls_destructor_can_release_a_tagged_block() {
    static A: AttributingAllocator<System> = AttributingAllocator::new(System);
    struct ExitBlock(*mut u8);
    impl Drop for ExitBlock {
        fn drop(&mut self) {
            unsafe { A.dealloc(self.0, layout(512)) };
        }
    }
    thread_local! { static EXIT: std::cell::RefCell<Option<ExitBlock>> = const { std::cell::RefCell::new(None) }; }
    let o = owner();
    let r = o.reference();
    std::thread::spawn(move || {
        let previous = unsafe { binding::install_ambient(r) };
        let p = unsafe { A.alloc(layout(512)) };
        assert!(!p.is_null());
        unsafe { binding::restore_ambient(previous) };
        EXIT.with(|cell| *cell.borrow_mut() = Some(ExitBlock(p)));
        drop(o);
    })
    .join()
    .unwrap();
    assert_eq!(bytes(r), 0);
    assert_eq!(A.snapshot().live_bytes, 0);
}
#[test]
fn orphan_tail_releases_storage_without_guessing_an_owner() {
    let a = AttributingAllocator::new(System);
    let o = owner();
    let r = o.reference();
    let old = unsafe { binding::install_ambient(r) };
    let p = unsafe { a.alloc(layout(512)) };
    unsafe { binding::restore_ambient(old) };
    let before = global_store().faults.snapshot().orphan_events;
    // Deliberately corrupt only metadata; inner storage must still be freed.
    unsafe {
        novarocks_memory::lane::RecordRef::NONE.write(p.add(512));
        a.dealloc(p, layout(512));
    }
    assert_eq!(a.snapshot().live_bytes, 0);
    assert_eq!(global_store().faults.snapshot().orphan_events, before + 1);
    assert_eq!(bytes(r), 520);
    // Repair the test's real lost fact using the still-held owner capability.
    unsafe { novarocks_memory::lane::SlotCore::direct(global_store(), r, -520, 0, -1) };
}
