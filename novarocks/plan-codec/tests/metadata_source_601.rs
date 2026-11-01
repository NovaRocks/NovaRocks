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

//! Independent public Metadata allocation/lifetime facts, not a fitted layout model.
use arrow::datatypes::Metadata;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::collections::HashMap;
use std::sync::Arc;

thread_local! {
    static ACTIVE: Cell<bool> = const { Cell::new(false) };
    static REQUESTS: Cell<usize> = const { Cell::new(0) };
    static CAPTURE_NEXT: Cell<bool> = const { Cell::new(false) };
    static STAGING: Cell<usize> = const { Cell::new(0) };
    static STAGING_FREED: Cell<bool> = const { Cell::new(false) };
}
struct RequestAllocator;
fn allocated(pointer: *mut u8) {
    if pointer.is_null() {
        return;
    }
    let _ = ACTIVE.try_with(|active| {
        if active.get() {
            REQUESTS.with(|count| count.set(count.get() + 1));
            CAPTURE_NEXT.with(|next| {
                if next.replace(false) {
                    STAGING.with(|block| block.set(pointer as usize));
                }
            });
        }
    });
}
fn freed(pointer: *mut u8) {
    let _ = STAGING.try_with(|block| {
        if block.get() != 0 && block.get() == pointer as usize {
            STAGING_FREED.with(|freed| freed.set(true));
        }
    });
}
// Const TLS never allocates, formats, locks, or dereferences a saved address.
// The allocator forwards the exact original request/layout to System.
unsafe impl GlobalAlloc for RequestAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        allocated(pointer);
        pointer
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        allocated(pointer);
        pointer
    }
    unsafe fn realloc(&self, pointer: *mut u8, old: Layout, size: usize) -> *mut u8 {
        let new = unsafe { System.realloc(pointer, old, size) };
        if !new.is_null() && new != pointer {
            freed(pointer);
        }
        allocated(new);
        new
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        freed(pointer);
        unsafe { System.dealloc(pointer, layout) }
    }
}
#[global_allocator]
static ALLOCATOR: RequestAllocator = RequestAllocator;
struct StopTracking;
impl Drop for StopTracking {
    fn drop(&mut self) {
        ACTIVE.with(|active| active.set(false));
        CAPTURE_NEXT.with(|next| next.set(false));
        STAGING.with(|block| block.set(0));
    }
}
fn start() {
    REQUESTS.with(|count| count.set(0));
    ACTIVE.with(|active| active.set(true));
}
fn stop() -> usize {
    ACTIVE.with(|active| active.set(false));
    REQUESTS.with(Cell::get)
}
fn source_map() -> HashMap<String, String> {
    // Warm RandomState before isolating the real staging-table allocation.
    drop(HashMap::<String, String>::with_capacity(1));
    STAGING_FREED.with(|freed| freed.set(false));
    CAPTURE_NEXT.with(|next| next.set(true));
    start();
    let map = HashMap::with_capacity(1 << 16);
    let requests = stop();
    assert_eq!(
        requests, 1,
        "the isolated source reserve must identify one original block"
    );
    assert!(!CAPTURE_NEXT.with(Cell::get));
    assert!(!STAGING_FREED.with(Cell::get));
    map
}

#[test]
fn hashmap_conversion_drops_staging_and_metadata_clones_share_the_actual_owner() {
    let _reset = StopTracking;
    let mut source = source_map();
    source.insert("provider_id".into(), "71".into());
    assert!(source.capacity() >= 1 << 16);
    let metadata = Metadata::from(source);
    assert!(
        STAGING_FREED.with(Cell::get),
        "consumed HashMap buckets are not attached Metadata backing"
    );
    assert_eq!(metadata.get("provider_id").map(String::as_str), Some("71"));
    let weak = Arc::downgrade(metadata.as_arc().unwrap());
    start();
    let mut cloned = std::hint::black_box(metadata.clone());
    let retained_clone = std::hint::black_box(metadata.clone());
    let requests = stop();
    assert_eq!(
        requests, 0,
        "immutable Metadata::clone must not request a copied map"
    );
    assert!(Arc::ptr_eq(
        metadata.as_arc().unwrap(),
        cloned.as_arc().unwrap()
    ));

    // A real mutation is COW, unlike an immutable clone. It preserves the
    // original entry and independently owns its new map allocation.
    let key = String::from("extra");
    let value = String::from("separate");
    start();
    cloned.insert(key, value);
    let requests = stop();
    assert!(requests > 0);
    assert!(!Arc::ptr_eq(
        metadata.as_arc().unwrap(),
        cloned.as_arc().unwrap()
    ));
    assert_eq!(metadata.get("extra"), None);
    drop(cloned);
    assert!(weak.upgrade().is_some());
    drop(metadata);
    assert!(weak.upgrade().is_some());
    assert_eq!(
        retained_clone.get("provider_id").map(String::as_str),
        Some("71")
    );
    drop(retained_clone);
    assert!(weak.upgrade().is_none());
}

#[test]
fn cleared_hashmap_conversion_has_no_attached_allocation_or_clone_request() {
    let _reset = StopTracking;
    let mut source = source_map();
    source.insert("removed".into(), "source".into());
    source.clear();
    assert!(source.capacity() >= 1 << 16);
    assert!(!STAGING_FREED.with(Cell::get));
    start();
    let metadata = Metadata::from(source);
    let cloned = std::hint::black_box(metadata.clone());
    let requests = stop();
    assert_eq!(
        requests, 0,
        "empty conversion and clone must not request an attached tree/Arc"
    );
    assert!(STAGING_FREED.with(Cell::get));
    assert!(metadata.as_arc().is_none());
    assert!(cloned.as_arc().is_none());
}
