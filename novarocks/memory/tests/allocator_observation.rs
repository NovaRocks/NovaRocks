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

//! The counting allocator observed as the real process allocator.
//!
//! A unit test can only exercise the wrapper as a value. This target installs
//! it with `#[global_allocator]`, which is the only way to prove the property
//! that matters in production: every allocation the process makes through Rust
//! passes through the counters, including allocations made by code that knows
//! nothing about the memory core.
//!
//! Two kinds of assertion appear here, deliberately:
//!
//! - Against the installed allocator the numbers are *deltas*. The test
//!   harness itself allocates — thread bookkeeping, captured output — and
//!   there is no way to exclude it from a process-wide counter. Where the
//!   harness's own activity is itself counted, an expectation subtracts it and
//!   stays exact; where it cannot be attributed, the signal asserted on is
//!   megabytes against kilobytes of noise and a declared tolerance says so.
//! - Against a locally held wrapper the numbers are *exact*. A value that was
//!   never installed sees only what the test hands it, which is how the
//!   reallocation delta and the failure path are pinned down precisely.

use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::thread;

use novarocks_memory::observe::allocator::{AllocatorSnapshot, CountingAllocator, SHARD_COUNT};
use novarocks_memory::observe::coverage::{
    BlindSpot, CoverageDescriptor, CoverageReport, MeasuredSource, SourceReading,
};

#[global_allocator]
static ALLOCATOR: CountingAllocator<System> = CountingAllocator::new(System);

/// Serialises the tests in this binary.
///
/// The counters are process-wide, so two tests reading them at once would each
/// see the other's allocations. Holding this lock leaves only the harness's
/// own bookkeeping as noise; the blocked test threads are parked and allocate
/// nothing.
static TEST_LOCK: Mutex<()> = Mutex::new(());

/// Bytes of process-wide slack allowed when comparing a live reading against a
/// baseline. Harness bookkeeping is kilobytes; every signal asserted against
/// this tolerance is megabytes.
const HARNESS_NOISE_BYTES: i128 = 1 << 20;

/// Allocation calls of slack allowed on top of the work a test performed:
/// thread spawn bookkeeping and harness output. Asserted against 800 000
/// deliberate allocations, so it still pins the counter to within 0.5%.
const HARNESS_NOISE_ALLOCATIONS: u64 = 4_096;

fn serialise() -> MutexGuard<'static, ()> {
    TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Returns `after - before` as a signed difference, so an assertion can talk
/// about a reading that moved backwards without underflowing.
fn delta(before: u64, after: u64) -> i128 {
    i128::from(after) - i128::from(before)
}

fn layout(size: usize) -> Layout {
    Layout::from_size_align(size, 16).expect("valid layout")
}

/// An inner allocator that refuses exactly one request on demand.
///
/// Needed because a real allocator cannot be made to fail predictably, and the
/// contract under test is precisely that a refusal changes nothing except the
/// failure counter.
struct FailingAllocator {
    fail_next: AtomicBool,
}

impl FailingAllocator {
    const fn new() -> Self {
        Self {
            fail_next: AtomicBool::new(false),
        }
    }

    fn fail_next_request(&self) {
        self.fail_next.store(true, Ordering::Relaxed);
    }

    fn should_fail(&self) -> bool {
        self.fail_next.swap(false, Ordering::Relaxed)
    }
}

// SAFETY: every request is either refused with a null pointer, which
// `GlobalAlloc` explicitly permits, or forwarded unchanged to `System`, so the
// returned pointers obey `System`'s contract.
unsafe impl GlobalAlloc for FailingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if self.should_fail() {
            return ptr::null_mut();
        }
        // SAFETY: `layout` is forwarded unchanged from the caller.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if self.should_fail() {
            return ptr::null_mut();
        }
        // SAFETY: `layout` is forwarded unchanged from the caller.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if self.should_fail() {
            // A refused reallocation must leave the original block untouched,
            // which is what makes "accounting unchanged" the correct answer.
            return ptr::null_mut();
        }
        // SAFETY: pointer and layout are forwarded unchanged from the caller.
        unsafe { System.realloc(pointer, layout, new_size) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: pointer and layout are forwarded unchanged from the caller.
        unsafe { System.dealloc(pointer, layout) };
    }
}

#[test]
fn allocating_raises_live_bytes_and_releasing_returns_to_the_baseline() {
    let _serialised = serialise();
    const SIZE: usize = 8 * 1_024 * 1_024;

    let baseline = ALLOCATOR.snapshot();
    let buffer: Vec<u8> = Vec::with_capacity(SIZE);
    black_box(&buffer);
    let held = ALLOCATOR.snapshot();

    // The cumulative counters are monotonic, so they carry no tolerance: the
    // allocated total must have risen by at least the request.
    let allocated = delta(baseline.allocated_total_bytes, held.allocated_total_bytes);
    assert!(
        allocated >= SIZE as i128,
        "allocated total rose by {allocated}, expected at least {SIZE}: \
         {baseline:?} -> {held:?}"
    );
    assert!(held.allocations > baseline.allocations, "{held:?}");

    // The live reading is a difference of those two totals, so a release by
    // the harness inside the measurement window lowers it. That release is
    // itself counted, which makes the expectation exact rather than
    // approximate: the live rise is the request minus whatever else was freed.
    let released_meanwhile = delta(
        baseline.deallocated_total_bytes,
        held.deallocated_total_bytes,
    );
    let raised = delta(baseline.live_bytes, held.live_bytes);
    assert!(
        raised >= SIZE as i128 - released_meanwhile,
        "live bytes rose by {raised}, expected at least {SIZE} less the \
         {released_meanwhile} bytes freed meanwhile: {baseline:?} -> {held:?}"
    );

    drop(buffer);
    let released = ALLOCATOR.snapshot();
    let residue = delta(baseline.live_bytes, released.live_bytes);
    assert!(
        residue.abs() <= HARNESS_NOISE_BYTES,
        "live bytes did not return to the baseline: off by {residue}: \
         {baseline:?} -> {released:?}"
    );
    assert_eq!(released.failures, baseline.failures, "{released:?}");
}

#[test]
fn a_growing_reallocation_records_only_the_delta() {
    let _serialised = serialise();
    // Held as a value rather than installed, so the counters see nothing but
    // these three calls and the assertions can be exact.
    let counting = CountingAllocator::new(System);
    let small = layout(1_024);

    // SAFETY: the layout has a non-zero size and a valid alignment.
    let pointer = unsafe { counting.alloc(small) };
    assert!(!pointer.is_null());

    // SAFETY: `pointer` was allocated by `counting` with `small`, and 4096 is
    // a valid size for its alignment.
    let grown = unsafe { counting.realloc(pointer, small, 4_096) };
    assert!(!grown.is_null());

    let snapshot = counting.snapshot();
    assert_eq!(snapshot.live_bytes, 4_096, "{snapshot:?}");
    assert_eq!(snapshot.allocated_total_bytes, 4_096, "{snapshot:?}");
    assert_ne!(
        snapshot.allocated_total_bytes,
        1_024 + 4_096,
        "the full new size must not be charged on top of the old block"
    );
    assert_eq!(snapshot.deallocated_total_bytes, 0, "{snapshot:?}");
    assert_eq!(snapshot.allocations, 1, "{snapshot:?}");
    assert_eq!(snapshot.reallocations, 1, "{snapshot:?}");
    assert_eq!(snapshot.failures, 0, "{snapshot:?}");

    // SAFETY: `grown` is the live block, now 4096 bytes at the same alignment.
    unsafe { counting.dealloc(grown, layout(4_096)) };
    let released = counting.snapshot();
    assert_eq!(released.live_bytes, 0, "{released:?}");
    assert_eq!(released.deallocated_total_bytes, 4_096, "{released:?}");
}

#[test]
fn concurrent_allocation_returns_to_the_baseline_and_counts_every_request() {
    let _serialised = serialise();
    const THREADS: usize = 8;
    const OPERATIONS: usize = 100_000;
    const BLOCK: usize = 128;

    let baseline = ALLOCATOR.snapshot();
    let mut workers = Vec::with_capacity(THREADS);
    for _ in 0..THREADS {
        workers.push(thread::spawn(|| {
            for _ in 0..OPERATIONS {
                let block: Vec<u8> = Vec::with_capacity(BLOCK);
                // Escapes the pointer so the allocation cannot be optimised
                // away, which would leave nothing to count.
                black_box(&block);
                drop(block);
            }
        }));
    }
    for worker in workers {
        worker.join().expect("worker thread");
    }
    let after = ALLOCATOR.snapshot();

    let performed = (THREADS * OPERATIONS) as u64;
    let counted = after.allocations - baseline.allocations;
    assert!(
        counted >= performed,
        "counted {counted} allocations, performed at least {performed}"
    );
    assert!(
        counted <= performed + HARNESS_NOISE_ALLOCATIONS,
        "counted {counted} allocations, expected {performed} plus harness noise"
    );
    assert!(
        after.allocated_total_bytes - baseline.allocated_total_bytes >= performed * BLOCK as u64,
        "{baseline:?} -> {after:?}"
    );

    let residue = delta(baseline.live_bytes, after.live_bytes);
    assert!(
        residue.abs() <= HARNESS_NOISE_BYTES,
        "live bytes did not return to the baseline after {performed} paired \
         operations across {THREADS} threads: off by {residue}"
    );
    assert_eq!(after.failures, baseline.failures, "{after:?}");
}

#[test]
fn a_refused_request_counts_a_failure_and_leaves_the_accounting_alone() {
    let _serialised = serialise();
    let counting = CountingAllocator::new(FailingAllocator::new());
    let block = layout(4_096);

    counting.inner().fail_next_request();
    // SAFETY: the layout is valid; the inner allocator answers with null.
    let refused = unsafe { counting.alloc(block) };
    assert!(refused.is_null(), "the inner allocator must refuse");

    let after_refusal = counting.snapshot();
    assert_eq!(after_refusal.failures, 1, "{after_refusal:?}");
    assert_eq!(after_refusal.live_bytes, 0, "{after_refusal:?}");
    assert_eq!(after_refusal.allocated_total_bytes, 0, "{after_refusal:?}");
    assert_eq!(after_refusal.allocations, 0, "{after_refusal:?}");

    // SAFETY: the layout is valid and the inner allocator now succeeds.
    let pointer = unsafe { counting.alloc(block) };
    assert!(!pointer.is_null());
    let granted = counting.snapshot();
    assert_eq!(granted.live_bytes, 4_096, "{granted:?}");

    counting.inner().fail_next_request();
    // SAFETY: `pointer` was allocated by `counting` with `block`; a refused
    // reallocation leaves that block valid, and it is released below.
    let refused_growth = unsafe { counting.realloc(pointer, block, 1 << 20) };
    assert!(refused_growth.is_null(), "the growth must be refused");

    let after_refused_growth = counting.snapshot();
    assert_eq!(after_refused_growth.failures, 2, "{after_refused_growth:?}");
    assert_eq!(
        after_refused_growth.live_bytes, granted.live_bytes,
        "a refused reallocation must not move live bytes"
    );
    assert_eq!(
        after_refused_growth.allocated_total_bytes, granted.allocated_total_bytes,
        "{after_refused_growth:?}"
    );
    assert_eq!(
        after_refused_growth.reallocations, 0,
        "{after_refused_growth:?}"
    );

    // SAFETY: the refusal left the original block live with its own layout.
    unsafe { counting.dealloc(pointer, block) };
    assert_eq!(counting.snapshot().live_bytes, 0);
}

#[test]
fn coverage_lists_the_blind_spots_and_nothing_folds_them_into_a_total() {
    let _serialised = serialise();
    let descriptor = CoverageDescriptor::RUST_GLOBAL_ALLOCATOR;
    assert_eq!(descriptor.source(), MeasuredSource::RustGlobalAllocator);
    assert!(!descriptor.covered_description().is_empty());

    let expected = [
        BlindSpot::DirectSystemAllocation,
        BlindSpot::CustomAllocator,
        BlindSpot::NativeLibrary,
        BlindSpot::DirectMapping,
        BlindSpot::AllocatorRetentionAndFragmentation,
    ];
    assert_eq!(descriptor.blind_spots(), expected.as_slice());
    for spot in descriptor.blind_spots() {
        assert!(!spot.label().is_empty(), "{spot:?}");
        assert!(!spot.description().is_empty(), "{spot:?}");
    }
    assert_eq!(
        BlindSpot::DirectSystemAllocation.measurable_source(),
        None,
        "a bypassed allocator cannot be quantified from inside this process"
    );

    let allocator = ALLOCATOR.snapshot();
    let resident = 64 * 1_024 * 1_024;
    let report = CoverageReport::new(allocator)
        .with_reading(SourceReading::measured(
            MeasuredSource::OperatingSystemResident,
            resident,
        ))
        .with_reading(SourceReading::unknown(MeasuredSource::NativeLibrary));

    assert_eq!(report.readings().len(), 2, "each source keeps its own line");
    assert_eq!(
        report.live_bytes(),
        allocator.live_bytes,
        "an added reading must not change the counted bytes"
    );

    // The only way to a combined figure is for the caller to compute it, which
    // is exactly what no API here offers.
    let hand_rolled_total = report
        .readings()
        .iter()
        .filter_map(|reading| reading.measured_bytes())
        .fold(report.live_bytes(), u64::saturating_add);
    assert_ne!(
        report.live_bytes(),
        hand_rolled_total,
        "the report must not be reporting a folded total already"
    );
    assert_eq!(
        report.readings()[1].measured_bytes(),
        None,
        "an unmeasured source stays unknown instead of contributing zero"
    );
}

#[test]
fn recorded_counting_allocator_shape() {
    let _serialised = serialise();
    let size = size_of::<CountingAllocator<System>>();
    let align = align_of::<CountingAllocator<System>>();
    println!("size_of::<CountingAllocator<System>>() = {size}");
    println!("align_of::<CountingAllocator<System>>() = {align}");
    println!("SHARD_COUNT = {SHARD_COUNT}");
    println!(
        "size_of::<AllocatorSnapshot>() = {}",
        size_of::<AllocatorSnapshot>()
    );

    assert_eq!(SHARD_COUNT, 64);
    assert!(SHARD_COUNT.is_power_of_two(), "sharding selects by mask");
    assert_eq!(ALLOCATOR.shard_count(), SHARD_COUNT);
    // Five 8-byte counters padded to one 64-byte line per shard, and `System`
    // is zero-sized, so the whole observer costs 4 KiB of static memory.
    assert_eq!(size, 64 * SHARD_COUNT);
    assert_eq!(align, 64);
}
