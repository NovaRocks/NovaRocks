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

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::collections::HashSet;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicUsize, Ordering};

use datasketches::hll::{Coupon, HllSketch, HllType};
use novarocks_execution::exec::hll::{HllAccumulator, HllAllocationUpperBounds, HllTargetType};

struct TrackingAllocator;

thread_local! {
    static TRACKING: Cell<bool> = const { Cell::new(false) };
    static RESERVATION_ACTIVE: Cell<bool> = const { Cell::new(false) };
}

static LIVE_DELTA: AtomicIsize = AtomicIsize::new(0);
static PEAK_LIVE_DELTA: AtomicIsize = AtomicIsize::new(0);
static ALLOCATED_BYTES: AtomicUsize = AtomicUsize::new(0);
static ALLOCATION_WITHOUT_RESERVATION: AtomicBool = AtomicBool::new(false);
static TEST_LOCK: Mutex<()> = Mutex::new(());

#[global_allocator]
static ALLOCATOR: TrackingAllocator = TrackingAllocator;

fn tracking_reservation() -> Option<bool> {
    TRACKING.with(|tracking| tracking.get().then(|| RESERVATION_ACTIVE.with(Cell::get)))
}

fn record_live_delta(delta: isize) {
    let current = LIVE_DELTA.fetch_add(delta, Ordering::Relaxed) + delta;
    let mut peak = PEAK_LIVE_DELTA.load(Ordering::Relaxed);
    while current > peak {
        match PEAK_LIVE_DELTA.compare_exchange_weak(
            peak,
            current,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => break,
            Err(observed) => peak = observed,
        }
    }
}

fn record_allocation(size: usize, reservation_active: bool) {
    if !reservation_active {
        ALLOCATION_WITHOUT_RESERVATION.store(true, Ordering::Relaxed);
    }
    ALLOCATED_BYTES.fetch_add(size, Ordering::Relaxed);
    record_live_delta(size as isize);
}

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null()
            && let Some(active) = tracking_reservation()
        {
            record_allocation(layout.size(), active);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
        if tracking_reservation().is_some() {
            record_live_delta(-(layout.size() as isize));
        }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null()
            && let Some(active) = tracking_reservation()
        {
            record_allocation(layout.size(), active);
        }
        pointer
    }

    unsafe fn realloc(&self, pointer: *mut u8, old: Layout, new_size: usize) -> *mut u8 {
        let new_pointer = unsafe { System.realloc(pointer, old, new_size) };
        if !new_pointer.is_null()
            && let Some(active) = tracking_reservation()
        {
            // Treat realloc as old and new storage coexisting to preserve a conservative peak.
            record_allocation(new_size, active);
            record_live_delta(-(old.size() as isize));
        }
        new_pointer
    }
}

struct TrackingScope;

impl Drop for TrackingScope {
    fn drop(&mut self) {
        TRACKING.with(|tracking| tracking.set(false));
        RESERVATION_ACTIVE.with(|active| active.set(false));
    }
}

struct ReservationGuard;

impl ReservationGuard {
    fn acquire() -> Self {
        RESERVATION_ACTIVE.with(|active| {
            assert!(!active.replace(true), "reservation is already active");
        });
        Self
    }
}

impl Drop for ReservationGuard {
    fn drop(&mut self) {
        RESERVATION_ACTIVE.with(|active| {
            assert!(active.replace(false), "reservation was not active");
        });
    }
}

#[derive(Debug)]
struct AllocationMeasurement {
    peak_live_delta: usize,
    allocated_bytes: usize,
    allocation_without_reservation: bool,
}

fn measure_allocations<T>(operation: impl FnOnce() -> T) -> (T, AllocationMeasurement) {
    LIVE_DELTA.store(0, Ordering::Relaxed);
    PEAK_LIVE_DELTA.store(0, Ordering::Relaxed);
    ALLOCATED_BYTES.store(0, Ordering::Relaxed);
    ALLOCATION_WITHOUT_RESERVATION.store(false, Ordering::Relaxed);
    RESERVATION_ACTIVE.with(|active| active.set(false));
    TRACKING.with(|tracking| tracking.set(true));
    let scope = TrackingScope;
    let result = operation();
    drop(scope);
    (
        result,
        AllocationMeasurement {
            peak_live_delta: PEAK_LIVE_DELTA.load(Ordering::Relaxed).max(0) as usize,
            allocated_bytes: ALLOCATED_BYTES.load(Ordering::Relaxed),
            allocation_without_reservation: ALLOCATION_WITHOUT_RESERVATION.load(Ordering::Relaxed),
        },
    )
}

fn with_reservation<T>(operation: impl FnOnce(&ReservationGuard) -> T) -> T {
    let guard = ReservationGuard::acquire();
    let result = operation(&guard);
    assert!(
        RESERVATION_ACTIVE.with(Cell::get),
        "operation must not release the caller-owned reservation"
    );
    // A real caller reconciles the retained charge here while the guard is still live.
    drop(guard);
    result
}

fn target_type(target: HllType) -> HllTargetType {
    match target {
        HllType::Hll4 => HllTargetType::Hll4,
        HllType::Hll6 => HllTargetType::Hll6,
        HllType::Hll8 => HllTargetType::Hll8,
    }
}

fn mixed_value(index: usize, seed: u64) -> u64 {
    let mut value = (index as u64)
        .wrapping_add(seed)
        .wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn payload(target: HllType, lg_k: u8, count: usize, seed: u64) -> Vec<u8> {
    let mut sketch = HllSketch::new(lg_k, target).expect("create input sketch");
    for index in 0..count {
        sketch.update(mixed_value(index, seed));
    }
    sketch.serialize()
}

fn payload_from_hashes(target: HllType, lg_k: u8, hashes: &[u64]) -> Vec<u8> {
    let mut sketch = HllSketch::new(lg_k, target).expect("create input sketch");
    for hash in hashes {
        sketch.update(*hash);
    }
    sketch.serialize()
}

fn unique_hashes(count: usize, seed: u64) -> Vec<u64> {
    let mut coupons = HashSet::with_capacity(count);
    let mut hashes = Vec::with_capacity(count);
    let mut index = 0;
    while hashes.len() < count {
        let hash = mixed_value(index, seed);
        if coupons.insert(Coupon::from_value(hash)) {
            hashes.push(hash);
        }
        index += 1;
    }
    hashes
}

fn noncompact_list_payload() -> Vec<u8> {
    let mut input = payload(HllType::Hll8, 10, 3, 0x1100);
    input[5] &= !8;
    input.resize(8 + 8 * 4, 0);
    input
}

fn noncompact_set_payload() -> Vec<u8> {
    let mut input = payload(HllType::Hll8, 10, 16, 0x2200);
    let capacity = 1usize << input[4];
    input[5] &= !8;
    input.resize(12 + capacity * 4, 0);
    input
}

fn noncompact_hll4_payload(compact: &[u8]) -> Vec<u8> {
    let lg_k = compact[3];
    let aux_count = u32::from_le_bytes(compact[36..40].try_into().expect("aux count")) as usize;
    assert!(aux_count > 0, "fixture must exercise HLL4 AuxMap");
    let mut capacity = 1usize
        << [
            0_u8, 2, 2, 2, 2, 2, 2, 3, 3, 3, 4, 4, 5, 5, 6, 7, 8, 9, 10, 11, 12, 13,
        ][lg_k as usize];
    while 4 * aux_count > 3 * capacity {
        capacity *= 2;
    }
    let packed_end = 40 + (1usize << (lg_k - 1));
    let mut input = compact[..packed_end].to_vec();
    input.extend_from_slice(&compact[packed_end..]);
    input.resize(packed_end + capacity * 4, 0);
    input[4] = capacity.trailing_zeros() as u8;
    input[5] &= !8;
    input
}

fn full_external_list_payload(hashes: &[u64]) -> Vec<u8> {
    assert_eq!(hashes.len(), 8);
    let set = payload_from_hashes(HllType::Hll8, 10, hashes);
    assert_eq!(set[7] & 3, 1, "eight coupons must produce SET locally");
    let mut list = set[..8].to_vec();
    list[0] = 2;
    list[4] = 3;
    list[5] = 8;
    list[6] = 8;
    list[7] = 8;
    list.extend_from_slice(&set[12..]);
    list
}

fn near_full_external_set_payload(hashes: &[u64]) -> Vec<u8> {
    assert_eq!(hashes.len(), 31);
    let mut set = payload_from_hashes(HllType::Hll8, 10, hashes);
    assert_eq!(set[7] & 3, 1, "31 coupons must remain SET locally");
    assert_eq!(set[4], 6, "local SET must have grown to capacity 64");
    set[4] = 5;
    set
}

fn hll4_aux_payload() -> Vec<u8> {
    let mut sketch = HllSketch::new(12, HllType::Hll4).expect("create HLL4 sketch");
    let initial_size = sketch.estimated_size();
    for index in 0..2_000_000 {
        sketch.update(mixed_value(index, 7_640_891_576_956_012_809));
        if sketch.estimated_size() > initial_size + (1 << 11) {
            return sketch.serialize();
        }
    }
    panic!("deterministic HLL4 workload did not reach AuxMap");
}

fn synthetic_hll4_aux_payload(aux_count: usize) -> Vec<u8> {
    assert!((1..=25).contains(&aux_count));
    let lg_k = 12_u8;
    let mut input = payload(HllType::Hll4, lg_k, 512, 0xc000);
    assert_eq!(input[7] & 3, 2, "base fixture must be dense");
    assert_eq!(
        u32::from_le_bytes(input[36..40].try_into().expect("base aux count")),
        0,
        "base fixture must not already contain auxiliary entries"
    );
    let packed_end = 40 + (1usize << (lg_k - 1));
    input.truncate(packed_end);
    let aux_value = input[6].checked_add(15).expect("valid auxiliary value");
    for slot in 0..aux_count {
        let byte = &mut input[40 + slot / 2];
        if slot & 1 == 0 {
            *byte = (*byte & 0xf0) | 0x0f;
        } else {
            *byte = (*byte & 0x0f) | 0xf0;
        }
        let coupon = (u32::from(aux_value) << 26) | slot as u32;
        input.extend_from_slice(&coupon.to_le_bytes());
    }
    input[36..40].copy_from_slice(&(aux_count as u32).to_le_bytes());
    input
}

fn populated_accumulator(target: HllType, lg_k: u8, count: usize, seed: u64) -> HllAccumulator {
    let mut accumulator =
        HllAccumulator::new_unreserved(lg_k, target_type(target)).expect("create accumulator");
    for index in 0..count {
        accumulator
            .update_hash_unreserved(mixed_value(index, seed))
            .expect("update accumulator");
    }
    accumulator
}

fn assert_peak_bound(
    label: &str,
    bounds: HllAllocationUpperBounds,
    measured: &AllocationMeasurement,
) {
    let absolute_peak = bounds.current_bytes + measured.peak_live_delta;
    assert!(
        absolute_peak <= bounds.operation_peak_bytes,
        "{label}: measured peak {absolute_peak} exceeds {bounds:?}; {measured:?}"
    );
    assert_eq!(
        bounds.additional_headroom_bytes(),
        bounds.operation_peak_bytes - bounds.current_bytes
    );
    assert!(
        !measured.allocation_without_reservation,
        "{label}: allocation happened without the caller guard: {measured:?}"
    );
}

fn assert_from_payload_bound(label: &str, input: &[u8]) {
    let (preflight, measured_preflight) =
        measure_allocations(|| HllAccumulator::from_payload_allocation_preflight(input));
    let preflight = preflight.expect("payload preflight");
    assert_eq!(measured_preflight.allocated_bytes, 0, "{label}: preflight");
    let bounds = preflight.bounds();
    assert_eq!(bounds.current_bytes, 0);
    let (result, measured) = measure_allocations(|| {
        with_reservation(|guard| {
            HllAccumulator::from_payload_under_reservation(input, &preflight, guard)
        })
    });
    let (accumulator, outcome) = result.expect("initialize from payload");
    assert_eq!(
        outcome.current_bytes,
        accumulator.current_allocation_upper_bound()
    );
    assert_eq!(outcome.operation_peak_bytes, bounds.operation_peak_bytes);
    assert_peak_bound(label, bounds, &measured);
}

fn assert_merge_bound(label: &str, mut accumulator: HllAccumulator, input: &[u8]) {
    let (preflight, measured_preflight) =
        measure_allocations(|| accumulator.merge_payload_allocation_preflight(input));
    let preflight = preflight.expect("merge preflight");
    assert_eq!(measured_preflight.allocated_bytes, 0, "{label}: preflight");
    let bounds = preflight.bounds();
    let (result, measured) = measure_allocations(|| {
        with_reservation(|guard| {
            accumulator.merge_payload_under_reservation(input, &preflight, guard)
        })
    });
    let outcome = result.expect("merge payload");
    assert_eq!(
        outcome.current_bytes,
        accumulator.current_allocation_upper_bound()
    );
    assert_eq!(outcome.operation_peak_bytes, bounds.operation_peak_bytes);
    assert_peak_bound(label, bounds, &measured);
}

fn reserved_update(accumulator: &mut HllAccumulator, hash: u64) -> HllAllocationUpperBounds {
    let (preflight, measured_preflight) = measure_allocations(|| {
        accumulator
            .update_hash_allocation_preflight()
            .expect("valid HLL update preflight")
    });
    assert_eq!(measured_preflight.allocated_bytes, 0, "update preflight");
    let bounds = preflight.bounds();
    let (result, measured) = measure_allocations(|| {
        with_reservation(|guard| accumulator.update_hash_under_reservation(hash, &preflight, guard))
    });
    let outcome = result.expect("reserved update");
    assert_eq!(
        outcome.current_bytes,
        accumulator.current_allocation_upper_bound()
    );
    assert_peak_bound("update", bounds, &measured);
    outcome
}

#[test]
fn preflight_bounds_cover_modes_targets_aux_and_transitions() {
    let _lock = TEST_LOCK.lock().expect("lock allocation tracker");
    let lg_k = 10;
    for target in [HllType::Hll4, HllType::Hll6, HllType::Hll8] {
        let token = HllAccumulator::new_allocation_preflight(lg_k, target_type(target))
            .expect("new preflight");
        let bounds = token.bounds();
        let (result, measured) = measure_allocations(|| {
            with_reservation(|guard| HllAccumulator::new_under_reservation(&token, guard))
        });
        let (mut accumulator, outcome) = result.expect("create accumulator");
        assert_eq!(
            outcome.current_bytes,
            accumulator.current_allocation_upper_bound()
        );
        assert_peak_bound("new", bounds, &measured);

        let mut observed_sizes = vec![outcome.current_bytes];
        for index in 0..256 {
            let outcome = reserved_update(&mut accumulator, mixed_value(index, 0x7711_0000));
            if observed_sizes.last().copied() != Some(outcome.current_bytes) {
                observed_sizes.push(outcome.current_bytes);
            }
        }
        assert!(
            observed_sizes.len() >= 4,
            "{target:?}: expected list, set growth, and dense sizes: {observed_sizes:?}"
        );

        for (mode, count) in [("list", 1), ("set", 16), ("dense", 4_096)] {
            let input = payload(target, lg_k, count, 0x1234_0000 + count as u64);
            let label = format!("{target:?}-{mode}");
            assert_from_payload_bound(&label, &input);
            assert_merge_bound(
                &label,
                populated_accumulator(target, lg_k, count / 2, 0x9876_0000),
                &input,
            );
        }
    }

    let lower_precision = payload(HllType::Hll6, 8, 4_096, 0x2233_4455);
    assert_merge_bound(
        "dense-downsample",
        populated_accumulator(HllType::Hll8, lg_k, 4_096, 0x6677_8899),
        &lower_precision,
    );
    let aux = hll4_aux_payload();
    assert_from_payload_bound("HLL4-aux", &aux);
    assert_merge_bound(
        "HLL4-aux",
        populated_accumulator(HllType::Hll4, 12, 4_096, 0x55aa_aa55),
        &aux,
    );
    for aux_count in [1, 24, 25] {
        let aux_boundary = synthetic_hll4_aux_payload(aux_count);
        assert_from_payload_bound(&format!("HLL4-aux-{aux_count}"), &aux_boundary);
        assert_merge_bound(
            &format!("HLL4-aux-{aux_count}"),
            populated_accumulator(HllType::Hll4, 12, 4_096, 0x65aa_aa55 + aux_count as u64),
            &aux_boundary,
        );
    }

    // lg_k below 8 skips SET and promotes LIST directly to the union's HLL8 array.
    for direct_lg_k in [4, 5, 7] {
        let mut accumulator = HllAccumulator::new_unreserved(direct_lg_k, HllTargetType::Hll8)
            .expect("create direct-promotion accumulator");
        for index in 0..64 {
            reserved_update(
                &mut accumulator,
                mixed_value(index, 0x4400_0000 + u64::from(direct_lg_k)),
            );
        }
    }
}

#[test]
fn update_preflight_is_live_configuration_sensitive() {
    let _lock = TEST_LOCK.lock().expect("lock allocation tracker");
    let mut small =
        HllAccumulator::new_unreserved(10, HllTargetType::Hll8).expect("small accumulator");
    let small_initial = small
        .update_hash_allocation_preflight()
        .expect("valid HLL update preflight")
        .bounds();
    assert_eq!(
        small_initial.current_bytes,
        small_initial.operation_peak_bytes
    );
    for index in 0..7 {
        small
            .update_hash_unreserved(mixed_value(index, 1))
            .expect("fill list");
    }
    let list_transition = small
        .update_hash_allocation_preflight()
        .expect("valid HLL update preflight")
        .bounds();
    assert!(list_transition.additional_headroom_bytes() < 1_024);

    let huge = HllAccumulator::new_unreserved(21, HllTargetType::Hll8).expect("huge accumulator");
    let huge_initial = huge
        .update_hash_allocation_preflight()
        .expect("valid HLL update preflight")
        .bounds();
    assert_eq!(
        huge_initial.current_bytes,
        huge_initial.operation_peak_bytes
    );
    assert_eq!(huge_initial.current_bytes, small_initial.current_bytes);

    let dense = populated_accumulator(HllType::Hll8, 10, 4_096, 2);
    let dense_update = dense
        .update_hash_allocation_preflight()
        .expect("valid HLL update preflight")
        .bounds();
    assert_eq!(
        dense_update.current_bytes,
        dense_update.operation_peak_bytes
    );
    assert!(dense_update.current_bytes > small_initial.current_bytes);
}

#[test]
fn update_preflight_covers_every_lg_k_representation_transition() {
    let _lock = TEST_LOCK.lock().expect("lock allocation tracker");
    for lg_k in 4_u8..=21 {
        let k = 1usize << lg_k;
        let mut transition_counts = vec![8];
        if lg_k >= 8 {
            let mut capacity = 32;
            loop {
                transition_counts.push(3 * capacity / 4 + 1);
                if capacity == k / 8 {
                    break;
                }
                capacity *= 2;
            }
        }
        let final_transition = *transition_counts.last().expect("transition count");
        let hashes = unique_hashes(
            final_transition + 1,
            0x9000_0000_u64.wrapping_add(u64::from(lg_k)),
        );
        let mut accumulator =
            HllAccumulator::new_unreserved(lg_k, HllTargetType::Hll8).expect("create accumulator");
        for (index, hash) in hashes.into_iter().enumerate() {
            let count = index + 1;
            if transition_counts.contains(&count) || count == final_transition + 1 {
                reserved_update(&mut accumulator, hash);
            } else {
                accumulator
                    .update_hash_unreserved(hash)
                    .expect("populate exact coupon boundary");
            }
        }
        assert_eq!(
            accumulator.current_allocation_upper_bound(),
            std::mem::size_of::<HllAccumulator>() + k,
            "lg_k={lg_k}: final representation must retain exactly the dense HLL8 heap"
        );
    }
}

#[test]
fn lg_k_five_ambiguous_heap_covers_dense_downsample_clone() {
    let _lock = TEST_LOCK.lock().expect("lock allocation tracker");
    let dense_lg_five = payload(HllType::Hll8, 5, 8, 0);
    assert_eq!(dense_lg_five[7] & 3, 2, "fixture must be dense");
    let mut accumulator =
        HllAccumulator::new_unreserved(5, HllTargetType::Hll8).expect("create lg_k=5 accumulator");
    accumulator
        .merge_payload_unreserved(&dense_lg_five)
        .expect("initial dense merge");
    accumulator
        .merge_payload_unreserved(&dense_lg_five)
        .expect("composite merge");
    let serialized = accumulator
        .serialize()
        .expect("serialize composite accumulator");
    let ambiguous_estimate = HllSketch::deserialize(&serialized)
        .expect("deserialize composite accumulator")
        .estimate();
    assert!(
        ambiguous_estimate < 8.0,
        "fixture must reproduce the dense/list estimate ambiguity, got {ambiguous_estimate}"
    );

    let lower_dense = payload(HllType::Hll8, 4, 8, 1);
    assert_eq!(lower_dense[7] & 3, 2, "source fixture must be dense");
    let preflight = accumulator
        .merge_payload_allocation_preflight(&lower_dense)
        .expect("downsample preflight");
    let expected_peak = accumulator.current_allocation_upper_bound()
        + std::mem::size_of::<HllSketch>()
        + (1 << 4)
        + (1 << 4)
        + (1 << 5);
    assert!(
        preflight.bounds().operation_peak_bytes >= expected_peak,
        "ambiguous lg_k=5 profile must cover the dense branch's old-gadget clone"
    );
    let bounds = preflight.bounds();
    let (result, measured) = measure_allocations(|| {
        with_reservation(|guard| {
            accumulator.merge_payload_under_reservation(&lower_dense, &preflight, guard)
        })
    });
    result.expect("downsample merge");
    assert_peak_bound("lg_k=5 dense downsample", bounds, &measured);
}

#[test]
fn external_sparse_capacity_bounds_cover_clone_and_repeated_growth() {
    let _lock = TEST_LOCK.lock().expect("lock allocation tracker");
    let hashes = unique_hashes(49, 0xb000_0000);

    let full_list = full_external_list_payload(&hashes[..8]);
    let mut list_accumulator =
        HllAccumulator::from_payload_unreserved(&full_list).expect("clone full external LIST");
    reserved_update(&mut list_accumulator, hashes[8]);

    let near_full_set = near_full_external_set_payload(&hashes[..31]);
    let mut set_accumulator = HllAccumulator::from_payload_unreserved(&near_full_set)
        .expect("clone near-full external SET");
    let additions = payload_from_hashes(HllType::Hll8, 10, &hashes[31..49]);
    let preflight = set_accumulator
        .merge_payload_allocation_preflight(&additions)
        .expect("near-full SET merge preflight");
    let bounds = preflight.bounds();
    let (result, measured) = measure_allocations(|| {
        with_reservation(|guard| {
            set_accumulator.merge_payload_under_reservation(&additions, &preflight, guard)
        })
    });
    result.expect("merge into near-full external SET");
    assert_peak_bound("near-full external SET repeated growth", bounds, &measured);
    assert_eq!(
        set_accumulator.current_allocation_upper_bound(),
        std::mem::size_of::<HllAccumulator>() + 128 * std::mem::size_of::<u32>(),
        "31 + 18 unique coupons must grow capacity 32 -> 64 -> 128"
    );
}

#[test]
fn allocation_header_enforces_minimum_lengths_and_low_k_set_shape() {
    let _lock = TEST_LOCK.lock().expect("lock allocation tracker");
    let compact_hll4 = hll4_aux_payload();
    let valid_images = [
        ("compact-list", payload(HllType::Hll8, 10, 3, 0x3300)),
        ("updatable-list", noncompact_list_payload()),
        ("compact-set", payload(HllType::Hll8, 10, 16, 0x4400)),
        ("updatable-set", noncompact_set_payload()),
        ("compact-hll4", compact_hll4.clone()),
        ("updatable-hll4", noncompact_hll4_payload(&compact_hll4)),
        ("hll6", payload(HllType::Hll6, 10, 4_096, 0x5500)),
        ("hll8", payload(HllType::Hll8, 10, 4_096, 0x6600)),
    ];
    for (label, input) in valid_images {
        HllAccumulator::from_payload_allocation_preflight(&input)
            .unwrap_or_else(|error| panic!("{label}: valid canonical image rejected: {error}"));

        let mut trailing = input.clone();
        trailing.push(0xa5);
        assert!(
            HllAccumulator::from_payload_allocation_preflight(&trailing).is_ok(),
            "{label}: trailing backing capacity does not change allocation shape"
        );

        let mut truncated = input.clone();
        truncated.pop();
        assert!(
            HllAccumulator::from_payload_allocation_preflight(&truncated).is_err(),
            "{label}: truncated body must be rejected by allocation admission"
        );

        let mut unknown_flag = input.clone();
        unknown_flag[5] |= 0x80;
        assert!(
            HllAccumulator::from_payload_allocation_preflight(&unknown_flag).is_ok(),
            "{label}: reserved flags follow the pinned decoder's tolerant semantics"
        );
    }

    let set = payload(HllType::Hll8, 8, 16, 0xaa00);
    for low_lg_k in 4_u8..8 {
        let mut invalid = set.clone();
        invalid[3] = low_lg_k;
        assert!(
            HllAccumulator::from_payload_allocation_preflight(&invalid).is_err(),
            "lg_k={low_lg_k}: SET must be rejected because no valid lg_arr exists"
        );
    }
}

#[test]
fn stale_and_mismatched_tokens_fail_before_datasketches_mutation() {
    let _lock = TEST_LOCK.lock().expect("lock allocation tracker");
    let mut accumulator =
        HllAccumulator::new_unreserved(10, HllTargetType::Hll8).expect("accumulator");
    let stale = accumulator
        .update_hash_allocation_preflight()
        .expect("valid HLL update preflight");
    accumulator
        .update_hash_unreserved(1_u64)
        .expect("advance accumulator");
    let estimate = accumulator.estimate().expect("estimate");
    let (stale_result, stale_measurement) = measure_allocations(|| {
        with_reservation(|guard| accumulator.update_hash_under_reservation(2_u64, &stale, guard))
    });
    let failure = stale_result.expect_err("a mismatched original preflight must be refused");
    let fact = novarocks_failure_contract::FailureProjection::failure_fact(&failure);
    assert_eq!(
        fact.class(),
        novarocks_failure_contract::FailureClass::Invariant
    );
    assert_eq!(fact.reason().code(), 182);
    assert_eq!(
        stale_measurement.allocated_bytes, 0,
        "typed preflight rejection must not enter allocating SDK work"
    );
    assert_eq!(
        accumulator.estimate().expect("estimate after stale"),
        estimate
    );

    let list_payload = payload(HllType::Hll8, 10, 1, 3);
    let dense_payload = payload(HllType::Hll8, 10, 4_096, 4);
    let mismatched = accumulator
        .merge_payload_allocation_preflight(&list_payload)
        .expect("list preflight");
    let estimate = accumulator.estimate().expect("estimate before mismatch");
    let (mismatch_result, mismatch_measurement) = measure_allocations(|| {
        with_reservation(|guard| {
            accumulator.merge_payload_under_reservation(&dense_payload, &mismatched, guard)
        })
    });
    let failure = mismatch_result.expect_err("a mismatched original preflight must be refused");
    let fact = novarocks_failure_contract::FailureProjection::failure_fact(&failure);
    assert_eq!(
        fact.class(),
        novarocks_failure_contract::FailureClass::Invariant
    );
    assert_eq!(fact.reason().code(), 182);
    assert_eq!(
        mismatch_measurement.allocated_bytes, 0,
        "typed preflight rejection must not enter allocating SDK work"
    );
    assert_eq!(
        accumulator.estimate().expect("estimate after mismatch"),
        estimate
    );

    // Reservation denial is represented by the caller not invoking the operation at all.
    let denied = accumulator
        .update_hash_allocation_preflight()
        .expect("valid HLL update preflight");
    let denied_estimate = accumulator.estimate().expect("estimate before denial");
    assert!(denied.bounds().additional_headroom_bytes() <= 1 << 10);
    let (_, denied_measurement) = measure_allocations(|| {});
    assert_eq!(denied_measurement.allocated_bytes, 0);
    assert_eq!(
        accumulator.estimate().expect("estimate after denial"),
        denied_estimate
    );
}

#[test]
fn malformed_allocation_headers_fail_before_standard_decode() {
    let _lock = TEST_LOCK.lock().expect("lock allocation tracker");
    let valid = payload(HllType::Hll8, 10, 4_096, 5);
    for mutation in 0..6 {
        let mut malformed = valid.clone();
        match mutation {
            0 => malformed.truncate(7),
            1 => malformed[1] = 99,
            2 => malformed[2] = 99,
            3 => malformed[3] = 3,
            4 => malformed[7] = 15,
            5 => malformed.truncate(40),
            _ => unreachable!(),
        }
        assert!(
            HllAccumulator::from_payload_allocation_preflight(&malformed).is_err(),
            "mutation {mutation} must fail allocation admission"
        );
    }

    let mut overflowing_hll4 = payload(HllType::Hll4, 10, 4_096, 6);
    overflowing_hll4[4] = 63;
    overflowing_hll4[5] &= !8;
    overflowing_hll4[36..40].copy_from_slice(&1_u32.to_le_bytes());
    assert!(HllAccumulator::from_payload_allocation_preflight(&overflowing_hll4).is_err());
}
