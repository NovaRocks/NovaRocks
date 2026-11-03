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
fn buffered_balance_stays_below_quantum_and_exits_flush() {
    let a = AttributingAllocator::new(System);
    let o = owner();
    let r = o.reference();
    let previous = unsafe { binding::install_ambient(r) };
    let mut pointers = [std::ptr::null_mut(); 1024];
    for p in &mut pointers {
        *p = unsafe { a.alloc(layout(1024)) };
        assert!(!p.is_null());
        assert!(binding::pending_bytes() < novarocks_memory::lane::SLOT_QUANTUM_BYTES);
    }
    assert!(bytes(r) > 0);
    unsafe { binding::restore_ambient(previous) };
    assert_eq!(binding::pending_bytes(), 0);
    assert_eq!(bytes(r), 1024 * 1032);
    for p in pointers {
        unsafe { a.dealloc(p, layout(1024)) };
    }
    assert_eq!(bytes(r), 0);
    assert_eq!(binding::pending_bytes(), 0);
}
#[test]
fn large_event_direct_publishes_and_source_switch_flushes_the_old_slot() {
    let a = AttributingAllocator::new(System);
    let x = owner();
    let y = owner();
    let rx = x.reference();
    let ry = y.reference();
    let previous = unsafe { binding::install_ambient(rx) };
    let p = unsafe { a.alloc(layout(512)) };
    assert!(!p.is_null());
    assert_eq!(bytes(rx), 0);
    let outer = unsafe { binding::install_explicit(ry) };
    let q = unsafe { a.alloc(layout(512)) };
    assert!(!q.is_null());
    assert_eq!(bytes(rx), 520);
    let big = unsafe { a.alloc(layout(1024 * 1024)) };
    assert!(!big.is_null());
    assert_eq!(bytes(ry), 1024 * 1024 + 8);
    unsafe {
        binding::restore_explicit(outer);
        binding::restore_ambient(previous)
    };
    assert_eq!(bytes(ry), 1024 * 1024 + 528);
    unsafe {
        a.dealloc(p, layout(512));
        a.dealloc(q, layout(512));
        a.dealloc(big, layout(1024 * 1024));
    }
    assert_eq!(bytes(rx), 0);
    assert_eq!(bytes(ry), 0);
}
