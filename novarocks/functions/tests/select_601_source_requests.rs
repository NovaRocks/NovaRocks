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

//! Real public filter allocation requests, separate from the caller's policy.
use arrow_array::{Array, BooleanArray};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
thread_local! {
    static ACTIVE: Cell<bool> = const { Cell::new(false) };
    static MATCHES: Cell<usize> = const { Cell::new(0) };
}
struct RequestAllocator;
fn observe(layout: Layout) {
    if layout.size() == 72 && layout.align() == align_of::<u64>() {
        ACTIVE.with(|active| {
            if active.get() {
                MATCHES.with(|count| count.set(count.get() + 1));
            }
        });
    }
}
// The TLS contains only const-initialized Cells and never allocates, formats,
// locks, or dereferences an allocation. System receives unchanged layouts.
unsafe impl GlobalAlloc for RequestAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        observe(layout);
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        observe(layout);
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, old: Layout, size: usize) -> *mut u8 {
        if let Ok(new) = Layout::from_size_align(size, old.align()) {
            observe(new);
        }
        unsafe { System.realloc(ptr, old, size) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}
#[global_allocator]
static ALLOCATOR: RequestAllocator = RequestAllocator;
#[test]
fn filter_512_selected_bits_requests_nine_u64_words_and_preserves_values() {
    let input = BooleanArray::from((0..1024).map(|row| row % 3 == 0).collect::<Vec<_>>());
    let mask = BooleanArray::from((0..1024).map(|row| row % 2 == 0).collect::<Vec<_>>());
    MATCHES.with(|count| count.set(0));
    ACTIVE.with(|active| active.set(true));
    let result = arrow_select::filter::filter(&input, &mask);
    ACTIVE.with(|active| active.set(false));
    let requests = MATCHES.with(Cell::get);
    let result = result.unwrap();
    assert_eq!(
        requests, 1,
        "filter_bits_compress owns Vec<u64>(512/64+1), not a64-byte bitmap"
    );
    let output = result.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert_eq!(output.len(), 512);
    assert_eq!(
        output.iter().collect::<Vec<_>>(),
        (0..512)
            .map(|row| Some((2 * row) % 3 == 0))
            .collect::<Vec<_>>()
    );
}
