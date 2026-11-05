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

//! Pins the wrapper sizes `guarded_bytes` charges against the allocations
//! the upstream `bytes` crate actually makes, and the exit order it relies on.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::num::NonZeroUsize;

use novarocks_execution::runtime::task::io::{ResultWriteAdmission, ResultWriteCredit};
use novarocks_result_contract::RootProfileV1;
use novarocks_worker::result_buffer::ResultRetainedBudget;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use novarocks_worker::guarded_bytes::{
    BYTES_MUT_SHARED_HEADER_BYTES, bytes_with_exit_guard, owner_wrapper_bytes,
};

// Same-thread component oracle only. No allocator hook allocates, logs, locks,
// panics, or retains any body bytes. Eight actual allocations are the hard cap.
const PHYSICAL_RECORD_CAP: usize = 8;
#[derive(Clone, Copy, Debug, Default)]
struct PhysicalAllocation {
    pointer: usize,
    bytes: usize,
    align: usize,
    dealloc_returned: bool,
}
impl PhysicalAllocation {
    const EMPTY: Self = Self {
        pointer: 0,
        bytes: 0,
        align: 0,
        dealloc_returned: false,
    };
}
#[derive(Clone, Copy, Debug)]
struct PhysicalProbe {
    selected_size: usize,
    records: [PhysicalAllocation; PHYSICAL_RECORD_CAP],
    used: usize,
    invalid: bool,
}
impl PhysicalProbe {
    const EMPTY: Self = Self {
        selected_size: 0,
        records: [PhysicalAllocation::EMPTY; PHYSICAL_RECORD_CAP],
        used: 0,
        invalid: false,
    };
    fn allocated(&mut self, pointer: *mut u8, layout: Layout) {
        if self.selected_size == 0 || self.selected_size != layout.size() {
            return;
        }
        if pointer.is_null()
            || self.used == PHYSICAL_RECORD_CAP
            || self.records[..self.used]
                .iter()
                .any(|record| record.pointer == pointer as usize && !record.dealloc_returned)
        {
            self.invalid = true;
            return;
        }
        self.records[self.used] = PhysicalAllocation {
            pointer: pointer as usize,
            bytes: layout.size(),
            align: layout.align(),
            dealloc_returned: false,
        };
        self.used += 1;
    }
    fn live_pointer(&self, pointer: *mut u8) -> Option<usize> {
        self.records[..self.used]
            .iter()
            .position(|record| record.pointer == pointer as usize && !record.dealloc_returned)
    }
    fn reallocated(
        &mut self,
        pointer: *mut u8,
        result: *mut u8,
        layout: Layout,
        size: usize,
    ) -> bool {
        let Some(index) = self.live_pointer(pointer) else {
            return false;
        };
        // Reallocation is never accepted as the no-growth proof. Keep enough
        // exact identity to observe the eventual free, including realloc fail.
        self.invalid = true;
        if !result.is_null() {
            self.records[index].pointer = result as usize;
            self.records[index].bytes = size;
            self.records[index].align = layout.align();
        }
        true
    }
    fn deallocated_after_return(&mut self, pointer: *mut u8, layout: Layout) {
        let Some(index) = self.live_pointer(pointer) else {
            return;
        };
        let record = &mut self.records[index];
        if record.bytes != layout.size() || record.align != layout.align() {
            self.invalid = true;
            return;
        }
        record.dealloc_returned = true;
    }
    fn all_deallocated(&self) -> bool {
        !self.invalid
            && self.used > 0
            && self.records[..self.used]
                .iter()
                .all(|record| record.dealloc_returned)
    }
    fn live_alias(&self, pointer: *const u8, bytes: usize) -> usize {
        assert!(
            !self.invalid,
            "physical oracle overflow or invalid allocation"
        );
        assert!(bytes > 0, "empty alias cannot identify backing");
        let start = pointer as usize;
        let end = start.checked_add(bytes).expect("alias address overflow");
        let mut matched = None;
        for (index, record) in self.records[..self.used].iter().enumerate() {
            if !record.dealloc_returned
                && start >= record.pointer
                && end
                    <= record
                        .pointer
                        .checked_add(record.bytes)
                        .expect("backing address overflow")
            {
                assert!(
                    matched.replace(index).is_none(),
                    "ambiguous physical backing"
                );
            }
        }
        matched.expect("actual alias must belong to one captured live allocation")
    }
}

thread_local! {
    static BACKING_PROBE: Cell<PhysicalProbe> = const { Cell::new(PhysicalProbe::EMPTY) };
    static COUNTING: Cell<bool> = const { Cell::new(false) };
    static ALLOCATED: Cell<usize> = const { Cell::new(0) };
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}

struct CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNTING.with(Cell::get) {
            ALLOCATED.with(|bytes| bytes.set(bytes.get() + layout.size()));
            ALLOCATIONS.with(|count| count.set(count.get() + 1));
        }
        let pointer = unsafe { System.alloc(layout) };
        let _ = BACKING_PROBE.try_with(|cell| {
            let mut probe = cell.get();
            probe.allocated(pointer, layout);
            cell.set(probe);
        });
        pointer
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        let _ = BACKING_PROBE.try_with(|cell| {
            let mut probe = cell.get();
            probe.deallocated_after_return(ptr, layout);
            cell.set(probe);
        });
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let result = unsafe { System.realloc(pointer, layout, size) };
        let _ = BACKING_PROBE.try_with(|cell| {
            let mut probe = cell.get();
            probe.reallocated(pointer, result, layout, size);
            cell.set(probe);
        });
        result
    }
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

fn counted<R>(work: impl FnOnce() -> R) -> (R, usize, usize) {
    ALLOCATED.with(|bytes| bytes.set(0));
    ALLOCATIONS.with(|count| count.set(0));
    COUNTING.with(|on| on.set(true));
    let value = work();
    COUNTING.with(|on| on.set(false));
    (
        value,
        ALLOCATED.with(Cell::get),
        ALLOCATIONS.with(Cell::get),
    )
}

struct Flag(Arc<AtomicBool>);

impl Drop for Flag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[test]
fn wrapper_is_the_only_allocation_and_matches_its_constant() {
    let backing = vec![7_u8; 64];
    let released = Arc::new(AtomicBool::new(false));
    let guard = Flag(Arc::clone(&released));
    let (bytes, allocated, allocations) = counted(move || bytes_with_exit_guard(backing, guard));
    assert_eq!(allocations, 1);
    assert_eq!(allocated, owner_wrapper_bytes::<Vec<u8>, Flag>());
    assert_eq!(&bytes[..], &[7_u8; 64][..]);
    drop(bytes);
    assert!(released.load(Ordering::SeqCst));
}

#[test]
fn guard_waits_for_the_last_clone_and_slice() {
    let released = Arc::new(AtomicBool::new(false));
    let bytes = bytes_with_exit_guard(vec![1_u8; 32], Flag(Arc::clone(&released)));
    let clone = bytes.clone();
    let slice = bytes.slice(4..8);
    drop(bytes);
    drop(clone);
    assert!(!released.load(Ordering::SeqCst));
    assert_eq!(&slice[..], &[1_u8; 4][..]);
    drop(slice);
    assert!(released.load(Ordering::SeqCst));
}

#[test]
fn backing_drops_before_its_guard() {
    struct Backing(Arc<AtomicBool>, Vec<u8>);
    impl AsRef<[u8]> for Backing {
        fn as_ref(&self) -> &[u8] {
            &self.1
        }
    }
    impl Drop for Backing {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    struct OrderGuard(Arc<AtomicBool>);
    impl Drop for OrderGuard {
        fn drop(&mut self) {
            assert!(
                self.0.load(Ordering::SeqCst),
                "guard must not run while its backing is live"
            );
        }
    }
    let backing_dropped = Arc::new(AtomicBool::new(false));
    let bytes = bytes_with_exit_guard(
        Backing(Arc::clone(&backing_dropped), vec![0; 8]),
        OrderGuard(Arc::clone(&backing_dropped)),
    );
    drop(bytes);
    assert!(backing_dropped.load(Ordering::SeqCst));
}

#[test]
fn bytes_mut_shared_header_matches_its_constant() {
    let mut buffer = bytes::BytesMut::with_capacity(256);
    buffer.extend_from_slice(&[3_u8; 128]);
    let (frozen, allocated, allocations) = counted(move || {
        let mut buffer = buffer;
        let tail = buffer.split_off(64);
        (buffer.freeze(), tail.freeze())
    });
    assert_eq!(allocations, 1);
    assert_eq!(allocated, BYTES_MUT_SHARED_HEADER_BYTES);
    drop(frozen);
}

struct BackingCapture;
impl BackingCapture {
    fn begin(bytes: usize) -> Self {
        BACKING_PROBE.with(|cell| {
            let mut probe = PhysicalProbe::EMPTY;
            probe.selected_size = bytes;
            cell.set(probe);
        });
        Self
    }
    fn stop(&self) {
        BACKING_PROBE.with(|cell| {
            let mut probe = cell.get();
            probe.selected_size = 0;
            cell.set(probe);
        });
    }
}
impl Drop for BackingCapture {
    fn drop(&mut self) {
        self.stop();
    }
}
fn backing_snapshot() -> PhysicalProbe {
    BACKING_PROBE.with(Cell::get)
}
fn process_credit(budget: &Arc<ResultRetainedBudget>, bytes: usize) -> ResultWriteCredit {
    let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(bytes).unwrap() else {
        panic!("original process budget must grant fixture capacity")
    };
    credit
}
// This owns the original credit, not a replacement release callback. Its Drop
// samples the allocator's post-System.dealloc fact; fields release afterwards.
struct ObservedOriginalCredit {
    _credit: ResultWriteCredit,
    allocation: usize,
    saw_post_dealloc: Arc<AtomicBool>,
}
impl Drop for ObservedOriginalCredit {
    fn drop(&mut self) {
        let state = backing_snapshot();
        self.saw_post_dealloc.store(
            !state.invalid && state.records[self.allocation].dealloc_returned,
            Ordering::SeqCst,
        );
    }
}

#[test]
fn actual_large_backing_short_alias_keeps_original_credit_until_post_dealloc() {
    // Component fixture cap only; no production limit or wallet is changed.
    const PROCESS: usize = 512 * 1024 * 1024;
    let budget = ResultRetainedBudget::new(NonZeroUsize::new(PROCESS).unwrap());
    let released_after_dealloc = Arc::new(AtomicBool::new(false));
    let credit = process_credit(&budget, RootProfileV1::SEGMENT_BYTES);
    let capture = BackingCapture::begin(RootProfileV1::SEGMENT_BYTES);
    let mut backing = Vec::with_capacity(RootProfileV1::SEGMENT_BYTES);
    backing.extend_from_slice(b"abcd");
    capture.stop();
    let before = backing_snapshot();
    assert!(!before.invalid);
    assert_eq!(before.used, 1);
    let allocation = before.live_alias(backing.as_ptr(), backing.len());
    assert_eq!(
        before.records[allocation].pointer,
        backing.as_ptr() as usize
    );
    assert_eq!(before.records[allocation].bytes, backing.capacity());
    assert_eq!(backing.capacity(), credit.bytes());
    let bytes = bytes_with_exit_guard(
        backing,
        ObservedOriginalCredit {
            _credit: credit,
            allocation,
            saw_post_dealloc: Arc::clone(&released_after_dealloc),
        },
    );
    let clone = bytes.clone();
    let short = bytes.slice(1..3);
    let last = short.slice(1..2);
    assert_eq!(
        before.records[allocation].bytes,
        RootProfileV1::SEGMENT_BYTES
    );
    assert_eq!(last.len(), 1);
    assert!(before.records[allocation].bytes >= last.len() * 1024 * 1024);
    let filler = process_credit(&budget, PROCESS - before.records[allocation].bytes);
    drop(bytes);
    drop(clone);
    drop(short);
    assert_eq!(&last[..], b"c");
    assert!(!backing_snapshot().records[allocation].dealloc_returned);
    assert!(!released_after_dealloc.load(Ordering::SeqCst));
    assert!(matches!(
        budget.try_reserve_process(1).unwrap(),
        ResultWriteAdmission::Blocked
    ));
    drop(last);
    let after = backing_snapshot();
    assert!(!after.invalid);
    assert!(after.records[allocation].dealloc_returned);
    assert!(after.all_deallocated());
    assert!(released_after_dealloc.load(Ordering::SeqCst));
    drop(process_credit(&budget, before.records[allocation].bytes));
    drop(filler);
}

#[test]
fn same_size_allocations_preserve_each_original_pointer_and_post_return_fact() {
    let capture = BackingCapture::begin(RootProfileV1::SEGMENT_BYTES);
    let first = vec![1_u8; RootProfileV1::SEGMENT_BYTES];
    let second = vec![2_u8; RootProfileV1::SEGMENT_BYTES];
    capture.stop();
    let before = backing_snapshot();
    assert!(!before.invalid);
    assert_eq!(before.used, 2);
    let first_id = before.live_alias(first.as_ptr(), first.len());
    let second_id = before.live_alias(second.as_ptr(), second.len());
    assert_ne!(first_id, second_id);
    assert_ne!(
        before.records[first_id].pointer,
        before.records[second_id].pointer
    );
    drop(first);
    let middle = backing_snapshot();
    assert!(!middle.invalid);
    assert!(middle.records[first_id].dealloc_returned);
    assert!(!middle.records[second_id].dealloc_returned);
    assert!(!middle.all_deallocated());
    assert_eq!(second[0], 2);
    drop(second);
    assert!(backing_snapshot().all_deallocated());
}

#[test]
fn physical_descriptor_overflow_never_reports_successful_reclamation() {
    // Actual allocations exercise the ninth-record refusal without growing the
    // fixed descriptor array; this is an oracle test, not result-owner proof.
    let capture = BackingCapture::begin(1024);
    let allocations: [Vec<u8>; PHYSICAL_RECORD_CAP + 1] = std::array::from_fn(|_| vec![0_u8; 1024]);
    capture.stop();
    let full = backing_snapshot();
    assert_eq!(full.used, PHYSICAL_RECORD_CAP);
    assert!(full.invalid);
    drop(allocations);
    assert!(!backing_snapshot().all_deallocated());
}
