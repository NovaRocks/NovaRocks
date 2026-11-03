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

//! Stable System-backed segments. Only control-plane callers allocate/reclaim.
// Design: ADR-0197 (docs/adr/ADR-0197-observation-owner-shards-and-coverage-shadow.md)
use super::{faults::FaultCounters, owner::RecordOwner, record::*, token::RecordRef};
use crate::sync::{AtomicPtr, AtomicU32, Mutex, Ordering};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    collections::VecDeque,
    ptr,
};

pub const MAX_RECORDS: u32 = 1 << 18;
pub const UNATTRIBUTED_SHARDS: u32 = 16;
#[cfg(not(loom))]
pub const THREAD_ROLE_CAPACITY: u32 = 16;
#[cfg(loom)]
pub const THREAD_ROLE_CAPACITY: u32 = 1;
pub const UNATTRIBUTED_RECORDS: u32 = THREAD_ROLE_CAPACITY * UNATTRIBUTED_SHARDS;
pub const REGION_LABELS: [&str; 2] = ["funding", "observation"];
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecordRegion {
    Funding,
    Observation,
}
impl RecordRegion {
    pub const ALL: [Self; 2] = [Self::Funding, Self::Observation];
    pub const fn index(self) -> usize {
        self as usize
    }
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RegionSnapshot {
    pub capacity: u32,
    pub active: u64,
    pub residual_retained: u64,
    pub pending_reclaim: u64,
    pub residual_tagged_bytes: i128,
    pub residual_r1_small_bytes: i128,
}
#[derive(Debug)]
struct Region {
    start: u32,
    end: u32,
    free: Mutex<Vec<u32>>,
    published: AtomicU32,
}
impl Region {
    #[cfg(not(loom))]
    const fn new(start: u32, end: u32) -> Self {
        Self {
            start,
            end,
            free: Mutex::new(Vec::new()),
            published: AtomicU32::new(start),
        }
    }
    #[cfg(loom)]
    fn new(start: u32, end: u32) -> Self {
        Self {
            start,
            end,
            free: Mutex::new(Vec::new()),
            published: AtomicU32::new(start),
        }
    }
}
pub const fn funding_record_capacity(capacity: u32) -> u32 {
    (capacity - UNATTRIBUTED_RECORDS) / 2
}
#[cfg(not(loom))]
pub const SEGMENT_RECORDS: usize = 4096;
// Models use the identical protocol with a smaller backing allocation.
#[cfg(loom)]
pub const SEGMENT_RECORDS: usize = 4;
const SEGMENTS: usize = 64;
struct Segment {
    records: [LaneRecord; SEGMENT_RECORDS],
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CoverageError {
    RecordStoreExhausted { region: RecordRegion },
    RecordStorageUnavailable,
    AccountClosed,
    ScopeRefused,
    BindingUnavailable,
}
#[derive(Debug)]
pub struct RecordStore {
    segments: [AtomicPtr<Segment>; SEGMENTS],
    unattributed: [LaneRecord; UNATTRIBUTED_RECORDS as usize],
    regions: [Region; 2],
    draining: Mutex<VecDeque<RecordRef>>,
    capacity: u32,
    pub faults: FaultCounters,
}
macro_rules! store_initializer {
    ($segments:expr, $unattributed:expr, $capacity:expr) => {
        Self {
            segments: $segments,
            unattributed: $unattributed,
            regions: [
                Region::new(
                    UNATTRIBUTED_RECORDS,
                    UNATTRIBUTED_RECORDS + funding_record_capacity($capacity),
                ),
                Region::new(
                    UNATTRIBUTED_RECORDS + funding_record_capacity($capacity),
                    $capacity,
                ),
            ],
            draining: Mutex::new(VecDeque::new()),
            capacity: $capacity,
            faults: FaultCounters::new(),
        }
    };
}
impl RecordStore {
    #[cfg(not(loom))]
    pub const fn new(capacity: u32) -> Self {
        assert!(capacity >= UNATTRIBUTED_RECORDS && capacity <= MAX_RECORDS);
        store_initializer!(
            [const { AtomicPtr::new(ptr::null_mut()) }; SEGMENTS],
            [const { LaneRecord::unattributed() }; UNATTRIBUTED_RECORDS as usize],
            capacity
        )
    }
    #[cfg(loom)]
    pub fn new(capacity: u32) -> Self {
        assert!(
            capacity >= UNATTRIBUTED_RECORDS && capacity as usize <= SEGMENTS * SEGMENT_RECORDS
        );
        store_initializer!(
            std::array::from_fn(|_| AtomicPtr::new(ptr::null_mut())),
            std::array::from_fn(|_| LaneRecord::unattributed()),
            capacity
        )
    }
    fn record_at(&self, index: u32) -> Option<&LaneRecord> {
        if index < UNATTRIBUTED_RECORDS {
            return Some(&self.unattributed[index as usize]);
        }
        if index >= self.capacity {
            return None;
        }
        let region = &self.regions[self.region_for(index)?.index()];
        if index >= region.published.load(Ordering::Acquire) {
            return None;
        }
        let segment_index = index as usize / SEGMENT_RECORDS;
        let segment = self.segments.get(segment_index)?.load(Ordering::Acquire);
        if segment.is_null() {
            return None;
        }
        // SAFETY: Release-published, fully initialized segments never move. The
        // store outlives this borrow; instance destruction requires exclusive ownership.
        Some(unsafe { &(*segment).records[index as usize % SEGMENT_RECORDS] })
    }
    /// Resolves identity only. This does not create a lifetime capability.
    pub fn resolve(&self, reference: RecordRef) -> Option<&LaneRecord> {
        let record = self.record_at(reference.index)?;
        if record.generation.load(Ordering::Acquire) != reference.generation
            || record.lifetime().claimed()
            || record.generation.load(Ordering::Acquire) != reference.generation
        {
            return None;
        }
        Some(record)
    }
    /// Hook-side identity failure is diagnostic; it never writes a guessed owner.
    pub fn resolve_hook(&self, reference: RecordRef) -> Option<&LaneRecord> {
        let result = self.resolve(reference);
        if result.is_none() {
            self.faults.orphan();
        }
        result
    }
    pub fn unattributed_ref(&self, role: usize, shard: usize) -> RecordRef {
        RecordRef {
            index: (role as u32) * UNATTRIBUTED_SHARDS
                + (shard & (UNATTRIBUTED_SHARDS as usize - 1)) as u32,
            generation: 0,
        }
    }
    fn ensure_segment(&self, index: u32) -> Result<(), CoverageError> {
        let slot = &self.segments[index as usize / SEGMENT_RECORDS];
        if !slot.load(Ordering::Acquire).is_null() {
            return Ok(());
        }
        let layout = Layout::new::<Segment>();
        // SAFETY: this control-plane allocation bypasses the global wrapper and
        // uses the same Layout in the test/model store's destructor.
        let segment = unsafe { System.alloc(layout) }.cast::<Segment>();
        if segment.is_null() {
            return Err(CoverageError::RecordStorageUnavailable);
        }
        for i in 0..SEGMENT_RECORDS {
            // SAFETY: each element lies in allocated, aligned Segment storage;
            // initialize in place to avoid a large stack temporary.
            unsafe {
                ptr::addr_of_mut!((*segment).records)
                    .cast::<LaneRecord>()
                    .add(i)
                    .write(LaneRecord::vacant())
            };
        }
        // Independent region locks can race at a boundary segment. Publish only
        // once; every record is subsequently read through the winning slot.
        if slot
            .compare_exchange(
                ptr::null_mut(),
                segment,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            // SAFETY: the losing segment was never published or used. Its
            // initialized records and exact System Layout are owned solely here.
            unsafe {
                ptr::drop_in_place(segment);
                System.dealloc(segment.cast(), layout);
            }
        }
        Ok(())
    }
    pub(crate) fn acquire_reference(
        &self,
        origin: u64,
        class: ResponsibilityClass,
        region: RecordRegion,
    ) -> Result<RecordRef, CoverageError> {
        self.reclaim(16);
        let pool = &self.regions[region.index()];
        let index = loop {
            let mut free = pool.free.lock().unwrap();
            if let Some(index) = free.pop() {
                break index;
            }
            let index = pool.published.load(Ordering::Relaxed);
            if index >= pool.end {
                self.faults.exhausted();
                return Err(CoverageError::RecordStoreExhausted { region });
            }
            if self.segments[index as usize / SEGMENT_RECORDS]
                .load(Ordering::Acquire)
                .is_null()
            {
                drop(free);
                self.ensure_segment(index)?;
                continue;
            }
            pool.published.store(index + 1, Ordering::Release);
            break index;
        };
        let record = self.record_at(index).expect("published record segment");
        record.reset_for_owner(origin, class);
        Ok(RecordRef {
            index,
            generation: record.generation.load(Ordering::Acquire),
        })
    }
    pub(crate) fn drain(&self, reference: RecordRef) {
        let record = self.resolve(reference).expect("live record owner");
        record.flags.fetch_or(DRAINING, Ordering::Release);
        self.draining.lock().unwrap().push_back(reference);
    }
    /// Confirms reclamation of this exact, previously owner-dropped generation.
    /// A collector may already have reclaimed and reused it. Queue serialization
    /// ensures a claimed record is fully reclaimed before this returns true.
    pub(crate) fn reclaim_exact(&self, reference: RecordRef) -> bool {
        let _draining = self.draining.lock().unwrap();
        !matches!(self.reclaim_reference(reference), ReclaimResult::Pending)
    }
    // The draining lock serializes claim, generation advancement and reuse
    // publication with exact acknowledgements. Stale entries touch no new owner.
    fn reclaim_reference(&self, reference: RecordRef) -> ReclaimResult {
        let Some(record) = self.record_at(reference.index) else {
            return ReclaimResult::Pending;
        };
        let generation = record.generation.load(Ordering::Acquire);
        if generation > reference.generation {
            return ReclaimResult::AlreadyReclaimed;
        }
        if generation != reference.generation {
            return ReclaimResult::Pending;
        }
        if record.lifetime().claimed() {
            // With the draining lock held, a same-generation claim is the
            // completed quarantine of an exhausted generation, never an
            // in-progress reset. It cannot acquire another owner.
            return ReclaimResult::AlreadyReclaimed;
        }
        if record.flags.load(Ordering::Acquire) & DRAINING == 0
            || record
                .state
                .compare_exchange(
                    ZERO_STATE,
                    ZERO_STATE | CLAIMED,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_err()
        {
            return ReclaimResult::Pending;
        }
        if record.tagged.load(Ordering::Acquire) != 0
            || record.r1_small.load(Ordering::Acquire) != 0
        {
            self.faults.reclaim_nonzero();
        }
        if generation == u32::MAX {
            // Permanently quarantine this index rather than wrap into an ABA.
            self.faults.generation_exhausted();
            return ReclaimResult::AlreadyReclaimed;
        }
        // Reuse resets atomic fields only. Observers may still borrow the
        // stable storage, so replacing the complete record would be a race.
        record.tagged.store(0, Ordering::Release);
        record.r1_small.store(0, Ordering::Release);
        record.sequence.store(0, Ordering::Release);
        record.flags.store(0, Ordering::Release);
        record.origin.store(0, Ordering::Release);
        record.generation.store(generation + 1, Ordering::Release);
        self.regions[self
            .region_for(reference.index)
            .expect("dynamic record region")
            .index()]
        .free
        .lock()
        .unwrap()
        .push(reference.index);
        ReclaimResult::Reclaimed
    }
    /// Bounded queue visits. Hooks never call this function or acquire its locks.
    pub fn reclaim(&self, budget: usize) -> usize {
        let mut draining = self.draining.lock().unwrap();
        let visits = budget.min(draining.len());
        let mut reclaimed = 0;
        for _ in 0..visits {
            let reference = draining.pop_front().expect("bounded drain visit");
            match self.reclaim_reference(reference) {
                ReclaimResult::Pending => draining.push_back(reference),
                ReclaimResult::Reclaimed => reclaimed += 1,
                ReclaimResult::AlreadyReclaimed => {}
            }
        }
        reclaimed
    }
    pub const fn capacity(&self) -> u32 {
        self.capacity
    }
    /// Hook-external diagnostic; sampling may overlap owner release/reclaim.
    pub fn draining_len(&self) -> usize {
        self.draining.lock().unwrap().len()
    }
    /// Skips an observation spanning reclamation/reuse. Values within the
    /// same generation remain independent atomic samples, not a transaction.
    pub fn snapshot_ref(&self, reference: RecordRef) -> Option<RecordSnapshot> {
        let record = self.resolve(reference)?;
        let snapshot = record.snapshot();
        if record.lifetime().claimed()
            || record.generation.load(Ordering::Acquire) != reference.generation
        {
            return None;
        }
        Some(snapshot)
    }
    /// Number of positions ever published, including the immortal prefix.
    /// Region ranges may have a hole; this is not a maximum record index.
    pub fn high_water(&self) -> u32 {
        UNATTRIBUTED_RECORDS
            + self
                .regions
                .iter()
                .map(|r| r.published.load(Ordering::Acquire) - r.start)
                .sum::<u32>()
    }
    pub fn region_bounds(&self, region: RecordRegion) -> (u32, u32) {
        let r = &self.regions[region.index()];
        (r.start, r.end)
    }
    pub fn region_for(&self, index: u32) -> Option<RecordRegion> {
        RecordRegion::ALL.into_iter().find(|r| {
            let r = &self.regions[r.index()];
            index >= r.start && index < r.end
        })
    }
    pub fn published_segment_count(&self) -> usize {
        self.segments
            .iter()
            .filter(|s| !s.load(Ordering::Acquire).is_null())
            .count()
    }
    /// Independent samples. Stopped/sealed records with a live owner or facts
    /// are retained, including zero-byte strong handles and R1-only facts.
    /// Draining records with no count/pin are waiting for control-plane reclaim.
    pub fn region_snapshot(&self, region: RecordRegion) -> RegionSnapshot {
        let r = &self.regions[region.index()];
        let mut result = RegionSnapshot {
            capacity: r.end - r.start,
            ..RegionSnapshot::default()
        };
        for index in r.start..r.published.load(Ordering::Acquire) {
            let Some(record) = self.record_at(index) else {
                continue;
            };
            let reference = RecordRef {
                index,
                generation: record.generation.load(Ordering::Acquire),
            };
            let Some(snapshot) = self.snapshot_ref(reference) else {
                continue;
            };
            if snapshot.is_draining() && snapshot.outstanding == 0 && snapshot.slot_pins == 0 {
                result.pending_reclaim += 1;
            } else if snapshot.is_draining()
                || snapshot.flags & PRODUCTION_MASK != 0
                || snapshot.flags & CLASS_MASK
                    == (ResponsibilityClass::Residual as u32) << CLASS_SHIFT
            {
                result.residual_retained += 1;
                result.residual_tagged_bytes += i128::from(snapshot.tagged_bytes);
                result.residual_r1_small_bytes += i128::from(snapshot.r1_small_bytes);
            } else {
                result.active += 1;
            }
        }
        result
    }
    /// Samples atomic fields over the occupied prefix; no coherent heap snapshot
    /// is implied. Immortal unattributed records are included exactly once.
    pub fn records(&self) -> impl Iterator<Item = (RecordRef, &LaneRecord)> {
        let funding = &self.regions[RecordRegion::Funding.index()];
        let observation = &self.regions[RecordRegion::Observation.index()];
        (0..UNATTRIBUTED_RECORDS)
            .chain(funding.start..funding.published.load(Ordering::Acquire))
            .chain(observation.start..observation.published.load(Ordering::Acquire))
            .filter_map(|index| {
                let record = self.record_at(index)?;
                if record.lifetime().claimed() {
                    return None;
                }
                Some((
                    RecordRef {
                        index,
                        generation: record.generation.load(Ordering::Acquire),
                    },
                    record,
                ))
            })
    }
}
enum ReclaimResult {
    Pending,
    Reclaimed,
    AlreadyReclaimed,
}
impl Drop for RecordStore {
    fn drop(&mut self) {
        // The production static never runs Drop. Owned test/model instances
        // release backing only after all owner handles and accesses are gone.
        for slot in &self.segments {
            let segment = slot.load(Ordering::Relaxed);
            if !segment.is_null() {
                // SAFETY: unique store destruction, initialized records, exact layout.
                unsafe {
                    ptr::drop_in_place(segment);
                    System.dealloc(segment.cast(), Layout::new::<Segment>());
                }
            }
        }
    }
}
#[cfg(not(loom))]
static GLOBAL_STORE: RecordStore = RecordStore::new(MAX_RECORDS);
#[cfg(not(loom))]
pub fn global_store() -> &'static RecordStore {
    &GLOBAL_STORE
}

/// Pins the original record store outside hooks. Global points to the
/// process-lifetime store. Cloning Owned shares ownership of the same backing
/// until the last pin drops.
#[derive(Clone, Debug)]
pub enum RecordStorePin {
    #[cfg(not(loom))]
    Global,
    Owned(std::sync::Arc<RecordStore>),
}
impl RecordStorePin {
    #[cfg(not(loom))]
    pub const fn global() -> Self {
        Self::Global
    }
    /// Test/model store. Retain an independent handle until every outstanding
    /// allocation and slot has finished; owner drop alone does not prove that.
    pub fn owned(capacity: u32) -> Self {
        Self::Owned(std::sync::Arc::new(RecordStore::new(capacity)))
    }
    pub fn store(&self) -> &RecordStore {
        match self {
            #[cfg(not(loom))]
            Self::Global => global_store(),
            Self::Owned(store) => store,
        }
    }
    pub fn acquire(
        &self,
        origin: u64,
        class: ResponsibilityClass,
        region: RecordRegion,
    ) -> Result<RecordOwner, CoverageError> {
        let reference = self.store().acquire_reference(origin, class, region)?;
        Ok(RecordOwner {
            store: self.clone(),
            reference,
        })
    }
}

#[cfg(all(test, not(loom)))]
mod exact_reclaim_tests {
    use super::*;
    use crate::lane::SlotCore;

    #[test]
    fn exact_reclaim_requires_owner_drop_and_final_real_count() {
        let store_pin = RecordStorePin::owned(crate::lane::UNATTRIBUTED_RECORDS + 32);
        let store = store_pin.store();
        let owner = store_pin
            .acquire(
                1,
                ResponsibilityClass::Query,
                crate::lane::RecordRegion::Observation,
            )
            .unwrap();
        let reference = owner.reference();
        assert!(!store.reclaim_exact(reference));
        // SAFETY: owner protects one simulated successful allocation fact.
        unsafe { SlotCore::direct(store, reference, 1, 0, 1) };
        drop(owner);
        assert!(!store.reclaim_exact(reference));
        assert!(store.resolve(reference).is_some());
        // SAFETY: this is the matching simulated final free, exactly once.
        unsafe { SlotCore::direct(store, reference, -1, 0, -1) };
        assert!(store.reclaim_exact(reference));
        assert!(store.resolve(reference).is_none());
        assert!(store.reclaim_exact(reference));
        assert_eq!(store.reclaim(1), 0);
        assert!(store.draining.lock().unwrap().is_empty());
    }

    #[test]
    fn exact_reclaim_accepts_a_completed_collector_reclaim() {
        let store_pin = RecordStorePin::owned(crate::lane::UNATTRIBUTED_RECORDS + 32);
        let store = store_pin.store();
        let owner = store_pin
            .acquire(
                1,
                ResponsibilityClass::Query,
                crate::lane::RecordRegion::Observation,
            )
            .unwrap();
        let reference = owner.reference();
        drop(owner);
        assert_eq!(store.reclaim(1), 1);
        assert!(store.reclaim_exact(reference));
        let replacement = store_pin
            .acquire(
                2,
                ResponsibilityClass::Service,
                crate::lane::RecordRegion::Observation,
            )
            .unwrap();
        assert_eq!(replacement.reference().index, reference.index);
        assert!(store.reclaim_exact(reference));
        assert_eq!(replacement.record().origin.load(Ordering::Acquire), 2);
        drop(replacement);
        assert_eq!(store.reclaim(1), 1);
    }

    #[test]
    fn stale_queue_visit_never_reclaims_a_reused_generation() {
        let store_pin = RecordStorePin::owned(crate::lane::UNATTRIBUTED_RECORDS + 96);
        let store = store_pin.store();
        let mut blockers = Vec::new();
        for origin in 1..=16 {
            let owner = store_pin
                .acquire(
                    origin,
                    ResponsibilityClass::Query,
                    crate::lane::RecordRegion::Observation,
                )
                .unwrap();
            let reference = owner.reference();
            // SAFETY: owner protects one simulated outstanding allocation.
            unsafe { SlotCore::direct(store, reference, 1, 0, 1) };
            blockers.push(reference);
            drop(owner);
        }
        let owner = store_pin
            .acquire(
                17,
                ResponsibilityClass::Query,
                crate::lane::RecordRegion::Observation,
            )
            .unwrap();
        let reference = owner.reference();
        drop(owner);
        assert!(store.reclaim_exact(reference));
        // acquire's sixteen bounded visits cannot reach the stale tail entry.
        let replacement = store_pin
            .acquire(
                18,
                ResponsibilityClass::Service,
                crate::lane::RecordRegion::Observation,
            )
            .unwrap();
        let replacement_ref = replacement.reference();
        assert_eq!(replacement_ref.index, reference.index);
        assert!(replacement_ref.generation > reference.generation);
        drop(replacement);
        assert_eq!(store.reclaim(1), 0);
        assert!(store.resolve(replacement_ref).is_some());
        assert!(store.reclaim_exact(replacement_ref));
        for reference in blockers {
            // SAFETY: release each simulated outstanding allocation once.
            unsafe { SlotCore::direct(store, reference, -1, 0, -1) };
        }
        assert_eq!(store.reclaim(32), 16);
        assert!(store.draining.lock().unwrap().is_empty());
    }
}

#[cfg(all(test, not(loom)))]
mod region_tests {
    use super::*;
    #[test]
    fn observation_exhaustion_never_borrows_funding_and_reclaim_preserves_region() {
        let store = RecordStorePin::owned(UNATTRIBUTED_RECORDS + 4);
        let a = store
            .acquire(1, ResponsibilityClass::Query, RecordRegion::Observation)
            .unwrap();
        let b = store
            .acquire(2, ResponsibilityClass::Query, RecordRegion::Observation)
            .unwrap();
        assert_eq!(
            store
                .acquire(3, ResponsibilityClass::Query, RecordRegion::Observation)
                .unwrap_err(),
            CoverageError::RecordStoreExhausted {
                region: RecordRegion::Observation
            }
        );
        let f = store
            .acquire(4, ResponsibilityClass::Query, RecordRegion::Funding)
            .unwrap();
        assert_eq!(
            store.store().region_snapshot(RecordRegion::Funding).active,
            1
        );
        let old = a.reference();
        drop(a);
        assert_eq!(store.store().reclaim(16), 1);
        let c = store
            .acquire(5, ResponsibilityClass::Service, RecordRegion::Observation)
            .unwrap();
        assert_eq!(c.reference().index, old.index);
        assert_eq!(c.reference().generation, old.generation + 1);
        assert_eq!(
            store.store().region_for(c.reference().index),
            Some(RecordRegion::Observation)
        );
        drop((b, c, f));
        assert_eq!(store.store().reclaim(16), 3);
    }
    #[test]
    fn boundary_segment_is_shared_without_replacing_issued_record_addresses() {
        let store = RecordStorePin::owned(UNATTRIBUTED_RECORDS + 4);
        let f = store
            .acquire(1, ResponsibilityClass::Query, RecordRegion::Funding)
            .unwrap();
        let address = std::ptr::from_ref(f.record());
        let o = store
            .acquire(2, ResponsibilityClass::Query, RecordRegion::Observation)
            .unwrap();
        assert_eq!(store.store().published_segment_count(), 1);
        assert_eq!(
            address,
            std::ptr::from_ref(store.store().resolve(f.reference()).unwrap())
        );
        assert_eq!(
            store.store().region_for(o.reference().index),
            Some(RecordRegion::Observation)
        );
    }
    #[test]
    fn active_owner_configuration_fits_only_the_funding_region() {
        let capacity = funding_record_capacity(MAX_RECORDS);
        let mut config = crate::AuthorityConfig::new(1024, 1024, 0);
        config.max_active_owners = capacity;
        assert!(config.validate().is_ok());
        config.max_active_owners = capacity + 1;
        assert_eq!(
            config.validate(),
            Err(crate::ConfigError::OwnersExceedFundingRecords {
                owners: capacity + 1,
                funding_records: capacity
            })
        );
    }
}
