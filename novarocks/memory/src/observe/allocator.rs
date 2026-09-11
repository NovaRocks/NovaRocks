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

//! Counting global allocator (MEM-1 wave-1 T04).
//!
//! Wraps the selected allocator and counts successful allocations, releases
//! and reallocation deltas. A failure keeps the previous accounting. The
//! callback allocates nothing, waits for nothing and calls no business code.
//! Per-shard accumulation is a contention measure, not thread attribution: it
//! never implies which work owns a byte.
//!
//! # Why a wrapper rather than an accounting allocator
//!
//! The two-tier model needs one number that no owner has to volunteer: the
//! bytes this process actually asked the allocator for. Hard governance knows
//! only what an owner declared, so a process can be honest about `L` and still
//! be over its bound. Wrapping the global allocator closes that gap from the
//! other side — it observes every request that goes through Rust's allocator
//! interface, whether or not anyone accounted for it.
//!
//! It deliberately stops there. It does not ask which task, query or operator
//! a byte belongs to, and it holds no thread-local ownership state, because a
//! thread is not an owner: a pool thread serves many works, an allocation is
//! freed on a different thread than it was made on, and a cache outlives every
//! thread that filled it. Attribution belongs to the accounts in the
//! governance tier; this tier only measures the process. The difference
//! between a reading here and known `L` is therefore a diagnostic — "this much
//! is unaccounted" — never an attribution.
//!
//! # Callback contract
//!
//! An allocator callback runs at arbitrary points, including inside another
//! allocator callback's caller, during thread teardown, and while a panic is
//! unwinding. Every callback in this module therefore:
//!
//! - allocates nothing (no `Vec`, no `String`, no `format!`),
//! - takes no lock and blocks on nothing,
//! - cannot unwind: it performs only relaxed atomic arithmetic, pointer
//!   null-checks and a masked index that is provably in range,
//! - never calls business code, logging, tracing or metrics.
//!
//! Breaking any of those turns an allocation into a deadlock or an infinite
//! recursion, so they are contract, not style.
//!
//! # What a reading means
//!
//! Counters are per-request sizes, not physical bytes. The allocator rounds
//! requests up to size classes, keeps freed pages for reuse and fragments over
//! time, so the process resident set is normally larger than
//! [`AllocatorSnapshot::live_bytes`]. What the wrapper cannot see at all is
//! enumerated as typed data in [`super::coverage`], not hidden in a margin.

use std::alloc::{GlobalAlloc, Layout};
use std::fmt;
use std::ptr;
use std::sync::atomic::{AtomicU64, Ordering};

/// Number of counter shards a [`CountingAllocator`] spreads its writes over.
///
/// Every allocation in the process would otherwise contend on one cache line,
/// which turns the observer itself into the bottleneck it is meant to observe.
/// Sixty-four shards keep the whole counter array at 4 KiB — cheap enough to
/// live in a `static` unconditionally — while giving a large machine one line
/// per hardware thread in the common case.
pub const SHARD_COUNT: usize = 64;

/// Sharding selects with a mask, which is only a valid modulo for a
/// power-of-two shard count. Enforced at compile time so a later retuning
/// cannot silently start indexing out of range.
const _: () = assert!(SHARD_COUNT.is_power_of_two());

/// Mask that reduces a hashed probe to a shard index.
const SHARD_MASK: usize = SHARD_COUNT - 1;

/// Bits of the mixed probe to discard before taking the shard index.
///
/// Multiplicative hashing concentrates entropy in the high bits, so the index
/// is taken from the top of the product rather than the bottom.
const SHARD_SHIFT: u32 = u64::BITS - SHARD_COUNT.trailing_zeros();

/// Granularity, as a power of two, at which stack addresses are treated as one
/// region when picking a shard.
///
/// 64 KiB collapses the frames of one thread onto a single shard in the common
/// case, while thread stacks — allocated megabytes apart — stay on separate
/// regions.
const STACK_REGION_SHIFT: u32 = 16;

/// Odd multiplier (the 64-bit golden-ratio constant) used to mix a probe.
const PROBE_MIX: u64 = 0x9E37_79B9_7F4A_7C15;

/// One shard's counters, padded so neighbouring shards do not share a line.
///
/// The five counters occupy 40 bytes; the alignment pads them to 64 so a write
/// from one thread does not invalidate another thread's shard. 64 is the line
/// size of the targets this engine runs on. It is deliberately not 128: a
/// doubled padding would additionally defeat adjacent-line prefetch pairing on
/// some cores, at the cost of doubling the array, and the array being small
/// enough to sit in a `static` without thought is worth more here than the
/// last increment of isolation.
#[repr(align(64))]
#[derive(Debug, Default)]
struct Shard {
    /// Bytes that entered live accounting: successful `alloc`/`alloc_zeroed`
    /// request sizes plus the growth part of a successful `realloc`.
    allocated_bytes: AtomicU64,
    /// Bytes that left live accounting: `dealloc` layout sizes plus the shrink
    /// part of a successful `realloc`.
    deallocated_bytes: AtomicU64,
    /// Successful `alloc` and `alloc_zeroed` calls. Reallocations are counted
    /// separately so this stays "blocks handed out".
    allocations: AtomicU64,
    /// Successful `realloc` calls, growing or shrinking.
    reallocations: AtomicU64,
    /// Calls that returned null. A failure is a fact worth reporting on its
    /// own: it is the allocator refusing, not memory being used.
    failures: AtomicU64,
}

impl Shard {
    /// Creates a zeroed shard.
    ///
    /// `const` because the whole array is built inside
    /// [`CountingAllocator::new`], which must be usable to initialise a
    /// `static`.
    const fn new() -> Self {
        Self {
            allocated_bytes: AtomicU64::new(0),
            deallocated_bytes: AtomicU64::new(0),
            allocations: AtomicU64::new(0),
            reallocations: AtomicU64::new(0),
            failures: AtomicU64::new(0),
        }
    }
}

/// Picks a shard without allocating, locking or touching thread-local storage.
///
/// The probe is the address of a stack local. It is a good proxy for "which
/// thread is calling" because every thread has its own stack, and it is the
/// only such proxy that is safe here:
///
/// - `thread_local!` is rejected outright. A TLS access can itself allocate on
///   first use, which would recurse into this allocator, and TLS destructors
///   run while allocator activity is still happening, so the slot may already
///   be destroyed when a callback reads it.
/// - `std::thread::current()` is rejected for the same reason: it lazily
///   materialises a per-thread handle, which can allocate.
/// - A shared round-robin atomic is rejected because it reintroduces exactly
///   the contended cache line sharding exists to remove.
///
/// Nothing about correctness depends on the choice being stable. A thread that
/// changes stack depth enough to cross a region boundary, or two threads that
/// hash to the same shard, only change which counters are touched: allocation
/// and release of one block may land on different shards, and
/// [`CountingAllocator::snapshot`] sums all shards, so the totals are
/// unaffected.
#[inline]
fn shard_index() -> usize {
    let probe = 0u8;
    let region = (ptr::from_ref(&probe).addr() >> STACK_REGION_SHIFT) as u64;
    let mixed = region.wrapping_mul(PROBE_MIX);
    // The mask is redundant given the shift but keeps the index provably in
    // range, which is what removes the panicking bounds check from a callback.
    ((mixed >> SHARD_SHIFT) as usize) & SHARD_MASK
}

/// A [`GlobalAlloc`] wrapper that measures what the process asks for.
///
/// Install it as the process allocator to get the observation tier's one
/// number, or hold it as a plain value to measure a specific inner allocator:
/// the trait methods work on a value that was never installed, which is how
/// the failure path is exercised in tests.
///
/// ```ignore
/// use std::alloc::System;
/// use novarocks_memory::observe::allocator::CountingAllocator;
///
/// #[global_allocator]
/// static ALLOCATOR: CountingAllocator<System> = CountingAllocator::new(System);
/// ```
///
/// The wrapper adds two relaxed atomic increments to an allocation and one to
/// a release, on a line that is normally uncontended. It adds no memory per
/// block: nothing is recorded per pointer, which is also why a release is
/// counted from the caller's `Layout` rather than from a remembered size.
pub struct CountingAllocator<A: GlobalAlloc> {
    inner: A,
    shards: [Shard; SHARD_COUNT],
}

impl<A: GlobalAlloc> CountingAllocator<A> {
    /// Wraps `inner`.
    ///
    /// `const` so the wrapper can initialise the `static` a
    /// `#[global_allocator]` requires, which is the only way it can observe
    /// allocations made before `main` runs.
    pub const fn new(inner: A) -> Self {
        Self {
            inner,
            shards: [const { Shard::new() }; SHARD_COUNT],
        }
    }

    /// Returns the wrapped allocator.
    ///
    /// Exposed so a caller can reach an inner allocator's own statistics,
    /// which the coverage report presents as a separate line rather than
    /// folding into these counters.
    pub const fn inner(&self) -> &A {
        &self.inner
    }

    /// Returns the number of counter shards, which is [`SHARD_COUNT`].
    ///
    /// Reported by diagnostics so a reading can be interpreted against the
    /// sharding that produced it.
    pub const fn shard_count(&self) -> usize {
        SHARD_COUNT
    }

    /// Sums every shard into one process reading.
    ///
    /// This is the only way out of the counters, and it deliberately offers no
    /// per-shard view and no peak:
    ///
    /// - A per-shard view invites treating a shard as a thread, which it is
    ///   not: a block may be allocated on one shard and released on another.
    /// - A peak would have to be a single process-wide value, which requires
    ///   serialising every allocation through one compare-and-exchange on one
    ///   cache line — the contention this design removes. A sum of per-shard
    ///   peaks would be a fabricated number, because the shards peaked at
    ///   different instants. So no peak is kept at all; a consumer that wants
    ///   one samples this method on its own cadence and owns the maximum it
    ///   observed.
    ///
    /// Allocation-free and lock-free, so it is safe to call from a diagnostic
    /// path at any time.
    pub fn snapshot(&self) -> AllocatorSnapshot {
        let mut allocated_total_bytes = 0u64;
        let mut deallocated_total_bytes = 0u64;
        let mut allocations = 0u64;
        let mut reallocations = 0u64;
        let mut failures = 0u64;
        for shard in &self.shards {
            // Wrapping rather than checked: a cumulative counter is exact for
            // any lifetime a real process reaches (2^64 bytes is exabytes of
            // cumulative allocation), and a diagnostic read must never panic.
            allocated_total_bytes =
                allocated_total_bytes.wrapping_add(shard.allocated_bytes.load(Ordering::Relaxed));
            deallocated_total_bytes = deallocated_total_bytes
                .wrapping_add(shard.deallocated_bytes.load(Ordering::Relaxed));
            allocations = allocations.wrapping_add(shard.allocations.load(Ordering::Relaxed));
            reallocations = reallocations.wrapping_add(shard.reallocations.load(Ordering::Relaxed));
            failures = failures.wrapping_add(shard.failures.load(Ordering::Relaxed));
        }
        AllocatorSnapshot {
            // Saturating, not wrapping: shards are read one at a time, so a
            // release counted in an already-read shard whose matching
            // allocation lands in a not-yet-read one can make the difference
            // momentarily negative. Reporting zero understates a quiescent
            // truth by the size of that skew; wrapping would report an absurd
            // exabyte figure instead.
            live_bytes: allocated_total_bytes.saturating_sub(deallocated_total_bytes),
            allocated_total_bytes,
            deallocated_total_bytes,
            allocations,
            reallocations,
            failures,
        }
    }

    /// Returns this thread's shard.
    #[inline]
    fn shard(&self) -> &Shard {
        &self.shards[shard_index()]
    }

    /// Records the outcome of an allocation request.
    ///
    /// A null pointer is a refusal: the previous accounting is left exactly as
    /// it was and only the failure counter moves, because no memory came into
    /// existence.
    #[inline]
    fn record_allocation(&self, pointer: *mut u8, size: usize) {
        let shard = self.shard();
        if pointer.is_null() {
            shard.failures.fetch_add(1, Ordering::Relaxed);
            return;
        }
        shard
            .allocated_bytes
            .fetch_add(size as u64, Ordering::Relaxed);
        shard.allocations.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a release of `size` bytes.
    #[inline]
    fn record_release(&self, size: usize) {
        self.shard()
            .deallocated_bytes
            .fetch_add(size as u64, Ordering::Relaxed);
    }

    /// Records the outcome of a reallocation request.
    ///
    /// Only the difference between the old and the new size moves the live
    /// total. A grown block is not a new allocation plus a release of the old
    /// one: charging the full `new_size` and releasing the old size would
    /// double-count a resize as two events and, for the reader of a
    /// mid-transition snapshot, invent bytes that never coexisted.
    #[inline]
    fn record_reallocation(&self, new_pointer: *mut u8, old_size: usize, new_size: usize) {
        let shard = self.shard();
        if new_pointer.is_null() {
            // A refused `realloc` leaves the original block alive and
            // unchanged, so the accounting is already correct as it stands.
            shard.failures.fetch_add(1, Ordering::Relaxed);
            return;
        }
        if new_size >= old_size {
            shard
                .allocated_bytes
                .fetch_add((new_size - old_size) as u64, Ordering::Relaxed);
        } else {
            shard
                .deallocated_bytes
                .fetch_add((old_size - new_size) as u64, Ordering::Relaxed);
        }
        shard.reallocations.fetch_add(1, Ordering::Relaxed);
    }
}

impl<A: GlobalAlloc> fmt::Debug for CountingAllocator<A> {
    /// Formats the current reading rather than the shard array.
    ///
    /// Hand-written instead of derived for two reasons: sixty-four shards of
    /// raw atomics are unreadable, and a derive would demand `A: Debug`, which
    /// would exclude perfectly good allocators from being wrapped.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CountingAllocator")
            .field("shards", &SHARD_COUNT)
            .field("snapshot", &self.snapshot())
            .finish_non_exhaustive()
    }
}

// SAFETY: every method forwards its arguments unchanged to the wrapped
// allocator, which is the only code that creates, resizes or frees memory
// here, so the pointers this wrapper returns and accepts obey exactly the
// contract `A` obeys. The counting that surrounds the forwarding neither reads
// nor writes the allocated memory, and it cannot re-enter the allocator: it
// performs only relaxed atomic arithmetic on inline storage, so it allocates
// nothing, takes no lock, blocks on nothing and cannot unwind.
unsafe impl<A: GlobalAlloc> GlobalAlloc for CountingAllocator<A> {
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: `layout` is forwarded unchanged, so this call inherits the
        // caller's guarantee that it is a valid non-zero-size layout.
        let pointer = unsafe { self.inner.alloc(layout) };
        self.record_allocation(pointer, layout.size());
        pointer
    }

    #[inline]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: as `alloc`; forwarded so the inner allocator keeps whatever
        // zeroing shortcut it has instead of falling back to alloc + memset.
        let pointer = unsafe { self.inner.alloc_zeroed(layout) };
        self.record_allocation(pointer, layout.size());
        pointer
    }

    #[inline]
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: `pointer` and `layout` are forwarded unchanged, so this call
        // inherits the caller's guarantee that the block was allocated by this
        // allocator with `layout`, and that `new_size` is a valid size for
        // `layout.align()`.
        let new_pointer = unsafe { self.inner.realloc(pointer, layout, new_size) };
        self.record_reallocation(new_pointer, layout.size(), new_size);
        new_pointer
    }

    #[inline]
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: `pointer` and `layout` are forwarded unchanged, so this call
        // inherits the caller's guarantee that the block was allocated by this
        // allocator with `layout`. The counting below does not touch the freed
        // memory.
        unsafe { self.inner.dealloc(pointer, layout) };
        // Counted after the release, so the window between the two shows the
        // bytes as still live. Over-reporting live memory for a few
        // instructions is the safe direction for a capacity authority;
        // under-reporting is not.
        self.record_release(layout.size());
    }
}

/// One process-wide reading of a [`CountingAllocator`].
///
/// Kept next to the allocator rather than in [`super::coverage`] because it is
/// the allocator's own output: coverage describes what a reading does and does
/// not include, and depends on this type, not the other way round.
///
/// Every field is a count of requested bytes or of calls. None of them is a
/// physical measurement, and none of them may be added to a governance total:
/// this is the observation tier's number, reported beside `L`, `F` and `O`,
/// never inside them.
///
/// The reading is eventually consistent. The shards are summed one at a time
/// without a global lock, so a snapshot taken while other threads allocate can
/// be internally inconsistent — `live_bytes` may not equal
/// `allocated_total_bytes - deallocated_total_bytes` as observed by any single
/// instant, and `allocations` may already include a block whose bytes are not
/// yet in `allocated_total_bytes`. At a quiescent point the numbers are exact.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AllocatorSnapshot {
    /// Requested bytes currently outstanding: everything allocated and not yet
    /// released, including reallocation growth.
    ///
    /// This is a difference of two sums read without a lock, so under
    /// concurrency it is approximate and biased low by any transient skew. It
    /// is not a peak and cannot be turned into one after the fact.
    pub live_bytes: u64,
    /// Cumulative bytes that entered live accounting over the process
    /// lifetime: successful `alloc`/`alloc_zeroed` sizes plus reallocation
    /// growth. Monotonic, so a difference between two readings is an
    /// allocation rate.
    pub allocated_total_bytes: u64,
    /// Cumulative bytes that left live accounting: `dealloc` sizes plus
    /// reallocation shrink. Monotonic.
    pub deallocated_total_bytes: u64,
    /// Successful `alloc` and `alloc_zeroed` calls, excluding reallocations.
    pub allocations: u64,
    /// Successful `realloc` calls, growing or shrinking.
    pub reallocations: u64,
    /// Calls that returned null. Non-zero means the allocator refused a
    /// request, which is a different event from memory being held.
    pub failures: u64,
}

#[cfg(test)]
mod tests {
    use std::alloc::System;

    use super::*;

    /// Exercises the wrapper as a value rather than as the process allocator,
    /// so a unit test measures exactly its own requests.
    fn wrapper() -> CountingAllocator<System> {
        CountingAllocator::new(System)
    }

    fn layout(size: usize) -> Layout {
        Layout::from_size_align(size, 8).expect("valid layout")
    }

    #[test]
    fn allocation_and_release_move_only_their_own_counters() {
        let allocator = wrapper();
        let layout = layout(4_096);
        // SAFETY: the layout has a non-zero size and a valid alignment, and
        // the block is released once below with the same layout.
        let pointer = unsafe { allocator.alloc(layout) };
        assert!(!pointer.is_null());

        let allocated = allocator.snapshot();
        assert_eq!(allocated.live_bytes, 4_096);
        assert_eq!(allocated.allocated_total_bytes, 4_096);
        assert_eq!(allocated.allocations, 1);
        assert_eq!(allocated.reallocations, 0);
        assert_eq!(allocated.failures, 0);

        // SAFETY: `pointer` came from this allocator with this exact layout
        // and has not been released yet.
        unsafe { allocator.dealloc(pointer, layout) };

        let released = allocator.snapshot();
        assert_eq!(released.live_bytes, 0);
        assert_eq!(released.allocated_total_bytes, 4_096);
        assert_eq!(released.deallocated_total_bytes, 4_096);
        assert_eq!(released.allocations, 1);
    }

    #[test]
    fn zeroed_allocation_is_counted_like_an_allocation() {
        let allocator = wrapper();
        let layout = layout(512);
        // SAFETY: valid non-zero-size layout; released below with the same one.
        let pointer = unsafe { allocator.alloc_zeroed(layout) };
        assert!(!pointer.is_null());
        assert_eq!(allocator.snapshot().allocations, 1);
        // SAFETY: `pointer` came from this allocator with this exact layout.
        unsafe { allocator.dealloc(pointer, layout) };
        assert_eq!(allocator.snapshot().live_bytes, 0);
    }

    #[test]
    fn reallocation_records_the_delta_in_both_directions() {
        let allocator = wrapper();
        let small = layout(1_024);
        // SAFETY: valid non-zero-size layout.
        let pointer = unsafe { allocator.alloc(small) };
        assert!(!pointer.is_null());

        // SAFETY: `pointer` was allocated by this allocator with `small`, and
        // 4096 is a valid size for its alignment.
        let grown = unsafe { allocator.realloc(pointer, small, 4_096) };
        assert!(!grown.is_null());
        let after_growth = allocator.snapshot();
        assert_eq!(after_growth.live_bytes, 4_096);
        assert_eq!(after_growth.allocated_total_bytes, 4_096);
        assert_eq!(after_growth.deallocated_total_bytes, 0);
        assert_eq!(after_growth.allocations, 1);
        assert_eq!(after_growth.reallocations, 1);

        // SAFETY: `grown` was returned by the reallocation above, so its
        // current layout is 4096 bytes at the original alignment.
        let shrunk = unsafe { allocator.realloc(grown, layout(4_096), 256) };
        assert!(!shrunk.is_null());
        let after_shrink = allocator.snapshot();
        assert_eq!(after_shrink.live_bytes, 256);
        assert_eq!(after_shrink.allocated_total_bytes, 4_096);
        assert_eq!(after_shrink.deallocated_total_bytes, 3_840);
        assert_eq!(after_shrink.reallocations, 2);

        // SAFETY: `shrunk` is the live block, now 256 bytes at the original
        // alignment.
        unsafe { allocator.dealloc(shrunk, layout(256)) };
        assert_eq!(allocator.snapshot().live_bytes, 0);
    }

    #[test]
    fn shard_index_stays_inside_the_array() {
        // Recursion changes the stack depth, which is exactly the input the
        // probe reads, so this covers more than one region.
        fn probe_at_depth(depth: usize, seen: &mut Vec<usize>) {
            let index = shard_index();
            assert!(index < SHARD_COUNT);
            seen.push(index);
            if depth > 0 {
                let padding = [0u8; 4_096];
                std::hint::black_box(&padding);
                probe_at_depth(depth - 1, seen);
            }
        }

        let mut seen = Vec::new();
        probe_at_depth(64, &mut seen);
        assert_eq!(seen.len(), 65);
    }

    #[test]
    fn a_release_seen_before_its_allocation_reports_zero_rather_than_wrapping() {
        // Reproduces the skew a concurrent reader can observe: shards are read
        // one at a time, so a release can be visible while the matching
        // allocation is not. The reading must be low, never enormous.
        let allocator = wrapper();
        allocator.shards[0]
            .deallocated_bytes
            .fetch_add(4_096, Ordering::Relaxed);
        let snapshot = allocator.snapshot();
        assert_eq!(snapshot.live_bytes, 0);
        assert_eq!(snapshot.deallocated_total_bytes, 4_096);
    }

    #[test]
    fn debug_reports_the_reading_without_requiring_a_debug_allocator() {
        let allocator = wrapper();
        let rendered = format!("{allocator:?}");
        assert!(rendered.contains("CountingAllocator"), "{rendered}");
        assert!(rendered.contains("shards: 64"), "{rendered}");
    }
}
