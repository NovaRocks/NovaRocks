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

use arrow_buffer::Buffer;
use novarocks_value_arrays::{
    ByteLayout,
    build::ValueBuilder,
    control::{ValueStructure, global_growth, uncontrolled},
};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    convert::Infallible,
};

thread_local! {
    static TRACK: Cell<bool> = const { Cell::new(false) };
    static TABLE_BYTES: Cell<usize> = const { Cell::new(0) };
    static ARC_BYTES: Cell<usize> = const { Cell::new(0) };
    static ARC_ALIGNMENT: Cell<usize> = const { Cell::new(1) };
    static ARC_CALLS: Cell<usize> = const { Cell::new(0) };
    static TABLE_LIVE: Cell<usize> = const { Cell::new(0) };
    static COEXISTED: Cell<bool> = const { Cell::new(false) };
}
struct Probe;
fn allocated(layout: Layout) {
    if !TRACK.try_with(Cell::get).unwrap_or(false) {
        return;
    }
    if TABLE_BYTES.with(Cell::get) != 0 && layout.size() == TABLE_BYTES.with(Cell::get) {
        TABLE_LIVE.with(|live| live.set(live.get() + 1));
    }
    if layout.size() == ARC_BYTES.with(Cell::get) && layout.align() == ARC_ALIGNMENT.with(Cell::get)
    {
        ARC_CALLS.with(|calls| calls.set(calls.get() + 1));
        if TABLE_LIVE.with(Cell::get) != 0 {
            COEXISTED.with(|value| value.set(true));
        }
    }
}
fn released(layout: Layout) {
    if TRACK.try_with(Cell::get).unwrap_or(false)
        && TABLE_BYTES.with(Cell::get) != 0
        && layout.size() == TABLE_BYTES.with(Cell::get)
    {
        TABLE_LIVE.with(|live| live.set(live.get().saturating_sub(1)));
    }
}
// SAFETY: Requests and pointers are delegated unchanged to System. Recording
// uses only allocation-free thread-local integer cells on the active thread.
unsafe impl GlobalAlloc for Probe {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            allocated(layout);
        }
        pointer
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            allocated(layout);
        }
        pointer
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let result = unsafe { System.realloc(pointer, layout, size) };
        if !result.is_null() {
            released(layout);
            allocated(Layout::from_size_align(size, layout.align()).unwrap());
        }
        result
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        released(layout);
        unsafe { System.dealloc(pointer, layout) }
    }
}
#[global_allocator]
static ALLOCATOR: Probe = Probe;
struct StopTracking;
impl Drop for StopTracking {
    fn drop(&mut self) {
        TRACK.with(|value| value.set(false));
    }
}

#[test]
fn original_view_publication_quotes_the_real_arc_table_once_with_vector_peak() {
    for layout in [ByteLayout::Utf8View, ByteLayout::BinaryView] {
        for count in [64, 0, 1] {
            let sources = (0..count)
                .map(|_| Buffer::from_vec(b"independent long shared region".to_vec()))
                .collect::<Vec<_>>();
            let mut builder = ValueBuilder::new(layout);
            for source in &sources {
                let mut row = builder.begin_row();
                if layout.is_utf8() {
                    row.write_string_region(
                        source,
                        0..source.len(),
                        std::str::from_utf8(source.as_slice()).unwrap(),
                        &mut global_growth,
                        &mut uncontrolled,
                    )
                    .unwrap();
                } else {
                    row.write_binary_region(
                        source,
                        0..source.len(),
                        &mut global_growth,
                        &mut uncontrolled,
                    )
                    .unwrap();
                }
                row.commit(&mut global_growth, &mut uncontrolled).unwrap();
            }
            let payload = Layout::array::<Buffer>(count).unwrap();
            let expected =
                novarocks_type_contract::owned_resources::layout::arc_layout(payload).unwrap();
            TABLE_BYTES.with(|value| value.set(payload.size()));
            ARC_BYTES.with(|value| value.set(expected.size()));
            ARC_ALIGNMENT.with(|value| value.set(expected.align()));
            ARC_CALLS.with(|value| value.set(0));
            TABLE_LIVE.with(|value| value.set(0));
            COEXISTED.with(|value| value.set(false));
            let mut observations = 0;
            let mut observed_before_allocation = false;
            let stop = StopTracking;
            TRACK.with(|value| value.set(true));
            let built = builder
                .finish_structured_observed(&mut global_growth, &mut uncontrolled, &mut |event| {
                    if let ValueStructure::SharedOwner(actual) = event {
                        observations += 1;
                        assert_eq!(actual, expected);
                        observed_before_allocation = ARC_CALLS.with(Cell::get) == 0;
                    }
                    Ok::<_, Infallible>(())
                })
                .unwrap();
            drop(stop);
            assert_eq!(
                observations,
                1,
                "{layout:?}, {count} regions: actual_arc_calls={}, vec_arc_coexisted={}",
                ARC_CALLS.with(Cell::get),
                COEXISTED.with(Cell::get),
            );
            assert!(observed_before_allocation);
            assert_eq!(ARC_CALLS.with(Cell::get), 1, "{layout:?}, {count} regions");
            if count == 64 {
                assert!(
                    COEXISTED.with(Cell::get),
                    "original Vec and new Arc table coexist"
                );
                assert_eq!(
                    TABLE_LIVE.with(Cell::get),
                    0,
                    "temporary Vec actually exited"
                );
            }
            assert_eq!(built.array.len(), count);
            built.array.to_data().validate_full().unwrap();
            drop(built);
            for source in &sources {
                assert_eq!(source.as_slice(), b"independent long shared region");
            }
        }
    }
}
