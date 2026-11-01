// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use arrow_schema::{DataType, Field, UnionFields};
use novarocks_type_contract::{
    ControlResourceError,
    owned_resources::{arrow_union::from_iterator_requests, vec},
};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    convert::Infallible,
    sync::Arc,
};
thread_local! {
    static TRACK: Cell<bool> = const { Cell::new(false) };
    static CALLS: Cell<usize> = const { Cell::new(0) };
    static BYTES: Cell<usize> = const { Cell::new(0) };
}
struct Probe;
fn record(bytes: usize) {
    if TRACK.try_with(Cell::get).unwrap_or(false) {
        let _ = CALLS.try_with(|v| v.set(v.get() + 1));
        let _ = BYTES.try_with(|v| v.set(v.get() + bytes));
    }
}
// SAFETY: System receives every original pointer/Layout; TLS integer cells
// perform no allocation and retain no references into the user allocation.
unsafe impl GlobalAlloc for Probe {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        record(size);
        unsafe { System.realloc(ptr, layout, size) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}
#[global_allocator]
static ALLOCATOR: Probe = Probe;
struct Stop;
impl Drop for Stop {
    fn drop(&mut self) {
        TRACK.with(|v| v.set(false));
    }
}
#[test]
fn result_shunt_union_collection_covers_unzip_and_try_new_at_growth_boundaries() {
    for count in [0usize, 1, 3, 4, 5, 7, 8, 9, 15, 16, 17, 127, 128] {
        let input = (0..count)
            .map(|id| (id as i8, Arc::new(Field::new("v", DataType::Int64, true))))
            .collect::<Vec<_>>();
        let facts = from_iterator_requests(count).unwrap();
        let old_scratch =
            vec::original_fresh_push_requests::<(i8, Arc<Field>), ControlResourceError>(
                count,
                &mut || Ok(()),
            )
            .unwrap();
        CALLS.with(|v| v.set(0));
        BYTES.with(|v| v.set(0));
        let stop = Stop;
        TRACK.with(|v| v.set(true));
        let actual = input
            .into_iter()
            .map(Ok::<_, Infallible>)
            .collect::<Result<UnionFields, Infallible>>()
            .unwrap();
        drop(stop);
        let calls = CALLS.with(Cell::get);
        let bytes = BYTES.with(Cell::get);
        assert_eq!(actual.len(), count);
        assert!(
            calls <= facts.scratch_requests_upper + 1,
            "count={count}, calls={calls}"
        );
        assert!(
            bytes <= facts.scratch_bytes_upper + facts.final_arc.size(),
            "count={count}, bytes={bytes}"
        );
        if count == 0 {
            assert_eq!(calls, 1, "an empty UnionFields still owns the Arc header");
            assert_eq!(bytes, facts.final_arc.size());
        } else {
            assert!(
                calls >= 4,
                "two unzip vectors, the try_new vector, and final Arc"
            );
        }
        if count == 1 {
            assert!(
                bytes > old_scratch + facts.final_arc.size(),
                "the former one-Vec quote misses real unzip allocations"
            );
        }
    }
}
