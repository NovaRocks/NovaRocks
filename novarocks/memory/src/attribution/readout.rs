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

//! Independent observation samples. No sampling operation belongs in an allocator hook.
use super::{
    BandSnapshot,
    band::{ATTRIBUTION_THRESHOLD_BYTES, ATTRIBUTION_TOKEN_BYTES},
    roles::{RoleLabels, thread_role_labels},
};
use crate::lane::faults::FaultSnapshot;
use crate::lane::record::{CLASS_MASK, CLASS_SHIFT, PRODUCTION_MASK};
use crate::lane::store::SEGMENT_RECORDS;
use crate::lane::{
    LaneRecord, RecordRef, RecordRegion, RecordSnapshot, RecordStore, RegionSnapshot,
    SLOT_QUANTUM_BYTES, THREAD_ROLE_CAPACITY, UNATTRIBUTED_SHARDS,
};
use std::time::{SystemTime, UNIX_EPOCH};

pub const CLASS_LABELS: [&str; 4] = ["query", "residual", "service", "unattributed"];
pub const PRODUCTION_LABELS: [&str; 3] = ["producing", "sealed", "stopped"];
/// Per-class signed observations. Negative intermediate samples are permitted
/// by pinned batching, and must not be hidden as zero live physical memory.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ClassFacts {
    pub tagged_bytes: i128,
    pub r1_small_bytes: i128,
}
/// One bounded-prefix sample, not a coherent heap snapshot or settlement proof.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AttributionSnapshot {
    pub process: BandSnapshot,
    /// Indices follow CLASS_LABELS; unattributed tagged bytes remain explicit.
    pub classified: [ClassFacts; 3],
    pub unattributed_tagged_bytes: i128,
    /// Role indices are issued by the bounded process table. Absent labels are
    /// unregistered roles, not another spelling of `unknown`.
    pub thread_roles: RoleLabels,
    pub unattributed_by_role: [i128; THREAD_ROLE_CAPACITY as usize],
    /// Indices follow RecordRegion::ALL / REGION_LABELS. These are independent
    /// samples, not a simultaneous partition of every other sampled field.
    pub regions: [RegionSnapshot; 2],
    pub ledger_blind_spot_bytes: i128,
    pub tagged_reconciliation_bytes: i128,
    pub records: [[u64; 3]; 4],
    pub record_capacity: u32,
    pub record_high_water: u32,
    pub draining_records: usize,
    /// Requested backing of published record segments; excludes inline
    /// records/control storage and is not resident size.
    pub record_segment_requested_bytes: u64,
    /// The reporting authority's observation-control storage estimate. None
    /// means no authority supplied it. S1 never adds this diagnostic to C.
    pub observation_metadata_bytes: Option<u64>,
    pub faults: FaultSnapshot,
    pub attribution_threshold_bytes: usize,
    pub token_bytes: usize,
    pub batch_threshold_bytes: u64,
    /// Q times independently sampled pins, excluding in-flight allocations.
    /// This is neither an instantaneous nor a concurrent mathematical bound.
    pub slot_balance_estimate_excluding_in_flight_bytes: u64,
    pub sampled_at_unix_millis: Option<u64>,
    pub sequence_sum: u64,
}
impl Default for AttributionSnapshot {
    fn default() -> Self {
        let mut thread_roles = [None; THREAD_ROLE_CAPACITY as usize];
        thread_roles[0] = Some("unknown");
        Self {
            process: BandSnapshot::default(),
            classified: [ClassFacts::default(); 3],
            unattributed_tagged_bytes: 0,
            thread_roles,
            unattributed_by_role: [0; THREAD_ROLE_CAPACITY as usize],
            regions: [RegionSnapshot::default(); 2],
            ledger_blind_spot_bytes: 0,
            tagged_reconciliation_bytes: 0,
            records: [[0; 3]; 4],
            record_capacity: 0,
            record_high_water: 0,
            draining_records: 0,
            record_segment_requested_bytes: 0,
            observation_metadata_bytes: None,
            faults: FaultSnapshot::default(),
            attribution_threshold_bytes: ATTRIBUTION_THRESHOLD_BYTES,
            token_bytes: ATTRIBUTION_TOKEN_BYTES,
            batch_threshold_bytes: SLOT_QUANTUM_BYTES,
            slot_balance_estimate_excluding_in_flight_bytes: 0,
            sampled_at_unix_millis: None,
            sequence_sum: 0,
        }
    }
}
impl AttributionSnapshot {
    /// Scans both published region prefixes and the immortal records. Every
    /// record is revalidated through snapshot_ref; raw borrowed fields are never
    /// sampled across reclamation/reuse. Class/bytes remain independent samples.
    pub fn sample(store: &RecordStore, process: BandSnapshot) -> Self {
        let mut result = Self {
            process,
            record_capacity: store.capacity(),
            record_high_water: store.high_water(),
            draining_records: store.draining_len(),
            faults: store.faults.snapshot(),
            thread_roles: thread_role_labels(),
            regions: RecordRegion::ALL.map(|region| store.region_snapshot(region)),
            sampled_at_unix_millis: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .ok()
                .and_then(|d| u64::try_from(d.as_millis()).ok()),
            ..Self::default()
        };
        for (reference, _) in store.records() {
            if let Some(snapshot) = store.snapshot_ref(reference) {
                result.add_record(reference, snapshot);
            }
        }
        result.unattributed_tagged_bytes = result.unattributed_by_role.iter().sum();
        let tagged_sum = result
            .classified
            .iter()
            .map(|f| f.tagged_bytes)
            .sum::<i128>()
            + result.unattributed_tagged_bytes;
        let small_sum = result
            .classified
            .iter()
            .map(|f| f.r1_small_bytes)
            .sum::<i128>();
        result.tagged_reconciliation_bytes = i128::from(process.tagged.live_bytes) - tagged_sum;
        result.ledger_blind_spot_bytes = i128::from(process.small.live_bytes) - small_sum;
        result.slot_balance_estimate_excluding_in_flight_bytes = result
            .faults
            .pinned_slots
            .saturating_mul(SLOT_QUANTUM_BYTES);
        let segments = store.published_segment_count();
        result.record_segment_requested_bytes =
            (segments * SEGMENT_RECORDS * std::mem::size_of::<LaneRecord>()) as u64;
        result
    }
    fn add_record(&mut self, reference: RecordRef, record: RecordSnapshot) {
        let class = ((record.flags & CLASS_MASK) >> CLASS_SHIFT) as usize;
        let production = (record.flags & PRODUCTION_MASK).min(2) as usize;
        self.records[class][production] += 1;
        self.sequence_sum = self.sequence_sum.wrapping_add(record.sequence);
        if class == 3 {
            // Unattributed records are the immortal role-major prefix. The
            // generation/flags sampled above retain the existing identity checks.
            let role = reference.index as usize / UNATTRIBUTED_SHARDS as usize;
            self.unattributed_by_role[role] += i128::from(record.tagged_bytes);
        } else {
            self.classified[class].tagged_bytes += i128::from(record.tagged_bytes);
            self.classified[class].r1_small_bytes += i128::from(record.r1_small_bytes);
        }
    }
    #[must_use]
    pub fn with_observation_metadata_bytes(mut self, bytes: u64) -> Self {
        self.observation_metadata_bytes = Some(bytes);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lane::{RecordStorePin, ResponsibilityClass, UNATTRIBUTED_RECORDS};
    use crate::sync::Ordering;
    #[test]
    fn classified_facts_blind_spot_and_reconciliation_are_sampled_separately() {
        let store = RecordStorePin::owned(crate::lane::UNATTRIBUTED_RECORDS + 8);
        let query = store
            .acquire(
                1,
                ResponsibilityClass::Query,
                crate::lane::RecordRegion::Observation,
            )
            .unwrap();
        let residual = store
            .acquire(
                2,
                ResponsibilityClass::Residual,
                crate::lane::RecordRegion::Observation,
            )
            .unwrap();
        let service = store
            .acquire(
                3,
                ResponsibilityClass::Service,
                crate::lane::RecordRegion::Observation,
            )
            .unwrap();
        // Fixture fields remain protected by live owners; these are readout
        // samples, not lifetime publications or simulated allocation counts.
        for (owner, tagged, small, state) in [
            (&query, 520, 20, 0),
            (&residual, 1032, 30, 1),
            (&service, 2048, 40, 2),
        ] {
            owner.record().tagged.store(tagged, Ordering::Release);
            owner.record().r1_small.store(small, Ordering::Release);
            owner.record().flags.fetch_or(state, Ordering::Release);
            owner.record().sequence.store(7, Ordering::Release);
        }
        store
            .store()
            .resolve(store.store().unattributed_ref(0, 0))
            .unwrap()
            .tagged
            .store(100, Ordering::Release);
        let mut counted = BandSnapshot::default();
        counted.small.live_bytes = 200;
        counted.tagged.live_bytes = 3700;
        let sample = AttributionSnapshot::sample(store.store(), counted)
            .with_observation_metadata_bytes(1234);
        assert_eq!(
            sample.classified[0],
            ClassFacts {
                tagged_bytes: 520,
                r1_small_bytes: 20
            }
        );
        assert_eq!(sample.classified[1].tagged_bytes, 1032);
        assert_eq!(sample.classified[2].r1_small_bytes, 40);
        assert_eq!(sample.unattributed_tagged_bytes, 100);
        assert_eq!(sample.unattributed_by_role[0], 100);
        assert_eq!(sample.unattributed_by_role.iter().sum::<i128>(), 100);
        assert_eq!(sample.thread_roles[0], Some("unknown"));
        assert_eq!(sample.regions[RecordRegion::Funding.index()].capacity, 4);
        assert_eq!(
            sample.regions[RecordRegion::Observation.index()].capacity,
            4
        );
        assert_eq!(sample.ledger_blind_spot_bytes, 110);
        assert_eq!(sample.tagged_reconciliation_bytes, 0);
        assert_eq!(sample.records[0], [1, 0, 0]);
        assert_eq!(sample.records[1], [0, 1, 0]);
        assert_eq!(sample.records[2], [0, 0, 1]);
        assert_eq!(sample.records[3], [u64::from(UNATTRIBUTED_RECORDS), 0, 0]);
        assert_eq!(sample.record_capacity, UNATTRIBUTED_RECORDS + 8);
        assert_eq!(sample.record_high_water, UNATTRIBUTED_RECORDS + 3);
        assert_eq!(sample.sequence_sum, 21);
        assert_eq!(sample.observation_metadata_bytes, Some(1234));
        assert!(sample.sampled_at_unix_millis.is_some());
        assert_eq!(
            sample.record_segment_requested_bytes,
            (SEGMENT_RECORDS * std::mem::size_of::<LaneRecord>()) as u64
        );
        // Restore diagnostic fixture fields before owner reclamation.
        for owner in [&query, &residual, &service] {
            owner.record().tagged.store(0, Ordering::Release);
            owner.record().r1_small.store(0, Ordering::Release);
        }
    }
    #[test]
    fn signed_samples_faults_and_pin_estimates_do_not_claim_settlement() {
        let store = RecordStorePin::owned(crate::lane::UNATTRIBUTED_RECORDS + 2);
        let query = store
            .acquire(
                1,
                ResponsibilityClass::Query,
                crate::lane::RecordRegion::Observation,
            )
            .unwrap();
        query.record().tagged.store(-520, Ordering::Release);
        query.record().r1_small.store(10, Ordering::Release);
        store.store().faults.binding_failed();
        store.store().faults.scope_refused();
        store.store().faults.orphan();
        store.store().faults.pin_added();
        let sample = AttributionSnapshot::sample(store.store(), BandSnapshot::default());
        assert_eq!(sample.classified[0].tagged_bytes, -520);
        assert_eq!(sample.tagged_reconciliation_bytes, 520);
        assert_eq!(sample.ledger_blind_spot_bytes, -10);
        assert_eq!(sample.faults.binding_failures, 1);
        assert_eq!(sample.faults.scope_refusals, 1);
        assert_eq!(sample.faults.orphan_events, 1);
        assert_eq!(
            sample.slot_balance_estimate_excluding_in_flight_bytes,
            SLOT_QUANTUM_BYTES
        );
        assert_eq!(sample.observation_metadata_bytes, None);
        query.record().tagged.store(0, Ordering::Release);
        query.record().r1_small.store(0, Ordering::Release);
        store.store().faults.pin_removed();
    }
    #[test]
    fn reclaimed_generation_is_not_observed_as_the_previous_record() {
        let store = RecordStorePin::owned(crate::lane::UNATTRIBUTED_RECORDS + 2);
        let first = store
            .acquire(
                1,
                ResponsibilityClass::Query,
                crate::lane::RecordRegion::Observation,
            )
            .unwrap();
        let old = first.reference();
        drop(first);
        assert_eq!(store.store().draining_len(), 1);
        assert_eq!(store.store().reclaim(1), 1);
        let second = store
            .acquire(
                2,
                ResponsibilityClass::Service,
                crate::lane::RecordRegion::Observation,
            )
            .unwrap();
        assert_ne!(old, second.reference());
        assert!(store.store().snapshot_ref(old).is_none());
        let sample = AttributionSnapshot::sample(store.store(), BandSnapshot::default());
        assert_eq!(sample.records[0], [0; 3]);
        assert_eq!(sample.records[2], [1, 0, 0]);
        assert_eq!(sample.draining_records, 0);
    }
    #[test]
    fn region_composition_includes_small_only_facts_and_zero_byte_strong_owners() {
        use crate::lane::{RecordRegion, SlotCore};

        let store = RecordStorePin::owned(UNATTRIBUTED_RECORDS + 16);
        let active_funding = store
            .acquire(1, ResponsibilityClass::Query, RecordRegion::Funding)
            .unwrap();
        let active_observation = store
            .acquire(2, ResponsibilityClass::Service, RecordRegion::Observation)
            .unwrap();
        let retained_small = store
            .acquire(3, ResponsibilityClass::Query, RecordRegion::Observation)
            .unwrap();
        let retained_tagged = store
            .acquire(4, ResponsibilityClass::Residual, RecordRegion::Funding)
            .unwrap();
        let retained_handle = store
            .acquire(5, ResponsibilityClass::Service, RecordRegion::Observation)
            .unwrap();
        let pending = store
            .acquire(6, ResponsibilityClass::Query, RecordRegion::Funding)
            .unwrap();
        let small_reference = retained_small.reference();
        let tagged_reference = retained_tagged.reference();
        let small_payload = Box::new([0u8; 128]);
        let tagged_payload = Box::new([0u8; 520]);
        std::hint::black_box(&small_payload);
        std::hint::black_box(&tagged_payload);
        // SAFETY: each box is a real, distinct allocation of the stated request
        // size; the independent owners are retained through fact publication.
        unsafe {
            SlotCore::direct(store.store(), small_reference, 0, 128, 1);
            SlotCore::direct(store.store(), tagged_reference, 520, 0, 1);
        }
        retained_small.record().stop();
        drop(retained_small);
        // The source owner is gone: only the real R1-size allocation retains
        // this observation record; there is no tagged byte or slot pin.
        let small_only = store.store().snapshot_ref(small_reference).unwrap();
        assert!(small_only.is_draining());
        assert_eq!(small_only.outstanding, 1);
        assert_eq!(small_only.slot_pins, 0);
        assert_eq!(small_only.tagged_bytes, 0);
        assert_eq!(small_only.r1_small_bytes, 128);
        retained_handle.record().seal();
        // This owner has no outstanding allocation/pin and now awaits reclaim.
        // No later acquire is allowed to reclaim it before the sample.
        drop(pending);

        let sample = AttributionSnapshot::sample(store.store(), BandSnapshot::default());
        assert_eq!(
            sample.regions[RecordRegion::Funding.index()],
            RegionSnapshot {
                capacity: 8,
                active: 1,
                residual_retained: 1,
                pending_reclaim: 1,
                residual_tagged_bytes: 520,
                residual_r1_small_bytes: 0,
            }
        );
        assert_eq!(
            sample.regions[RecordRegion::Observation.index()],
            RegionSnapshot {
                capacity: 8,
                active: 1,
                residual_retained: 2,
                pending_reclaim: 0,
                residual_tagged_bytes: 0,
                residual_r1_small_bytes: 128,
            }
        );
        assert_eq!(
            sample.regions.iter().map(|r| r.capacity).sum::<u32>() + UNATTRIBUTED_RECORDS,
            sample.record_capacity
        );
        assert_eq!(
            sample.records[3].iter().sum::<u64>(),
            u64::from(UNATTRIBUTED_RECORDS)
        );
        assert_eq!(sample.record_high_water, UNATTRIBUTED_RECORDS + 6);
        // The observation prefix begins beyond the aggregate occupied count.
        assert!(active_observation.reference().index >= sample.record_high_water);
        assert_eq!(sample.classified[2].tagged_bytes, 0);
        assert_eq!(sample.classified[1].tagged_bytes, 520);

        drop(small_payload);
        drop(tagged_payload);
        // SAFETY: the two genuine allocations were just freed, exactly once.
        unsafe {
            SlotCore::direct(store.store(), small_reference, 0, -128, -1);
            SlotCore::direct(store.store(), tagged_reference, -520, 0, -1);
        }
        let released_small = store.store().snapshot_ref(small_reference).unwrap();
        assert!(released_small.is_draining());
        assert_eq!(released_small.outstanding, 0);
        assert_eq!(released_small.r1_small_bytes, 0);
        let released = AttributionSnapshot::sample(store.store(), BandSnapshot::default());
        assert_eq!(
            released.regions[RecordRegion::Observation.index()].pending_reclaim,
            1
        );
        drop((retained_tagged, retained_handle));
        assert_eq!(store.store().reclaim(usize::MAX), 4);
        let settled = AttributionSnapshot::sample(store.store(), BandSnapshot::default());
        for region in settled.regions {
            assert_eq!(region.active, 1);
            assert_eq!(region.residual_retained, 0);
            assert_eq!(region.pending_reclaim, 0);
            assert_eq!(region.residual_tagged_bytes, 0);
            assert_eq!(region.residual_r1_small_bytes, 0);
        }
        drop((active_funding, active_observation));
        assert_eq!(store.store().reclaim(usize::MAX), 2);
    }
    #[test]
    fn a_real_zero_balance_slot_pin_retains_its_region_until_flush() {
        use crate::lane::SlotCore;

        let store = RecordStorePin::owned(UNATTRIBUTED_RECORDS + 4);
        for region in RecordRegion::ALL {
            let owner = store
                .acquire(9, ResponsibilityClass::Query, region)
                .unwrap();
            let reference = owner.reference();
            let payload = Box::new([0u8; 520]);
            std::hint::black_box(&payload);
            let mut slot = SlotCore::new();
            // SAFETY: the real distinct block exists, its owner is retained,
            // and this sole slot gains the pin before buffering the +1 fact.
            unsafe { slot.add(store.store(), reference, 520, 0, 1, true) };
            drop(payload);
            // SAFETY: that exact allocation has now been freed once. Its
            // buffered positive fact and the sole slot pin protect this -1.
            unsafe { slot.add(store.store(), reference, -520, 0, -1, true) };
            assert_eq!(slot.pending_bytes(), 0);
            assert!(!slot.is_empty());
            drop(owner);

            // The source strong owner and physical allocation are both gone.
            // The slot alone keeps this exact generation alive for its flush.
            let pin_only = store.store().snapshot_ref(reference).unwrap();
            assert!(pin_only.is_draining());
            assert_eq!(pin_only.outstanding, 0);
            assert_eq!(pin_only.slot_pins, 1);
            assert_eq!(pin_only.tagged_bytes, 0);
            assert_eq!(pin_only.r1_small_bytes, 0);
            let retained = AttributionSnapshot::sample(store.store(), BandSnapshot::default());
            assert_eq!(
                retained.regions[region.index()],
                RegionSnapshot {
                    capacity: 2,
                    active: 0,
                    residual_retained: 1,
                    pending_reclaim: 0,
                    residual_tagged_bytes: 0,
                    residual_r1_small_bytes: 0,
                }
            );
            assert_eq!(retained.faults.pinned_slots, 1);
            assert_eq!(store.store().reclaim(usize::MAX), 0);

            // SAFETY: the same retained store and the unique slot pin remain;
            // flush performs its final record access before releasing the pin.
            unsafe { slot.flush(store.store()) };
            assert!(slot.is_empty());
            // Never access the saved token/record after the final unpin. These
            // observations use the store's independent control-plane sampler.
            let pending = AttributionSnapshot::sample(store.store(), BandSnapshot::default());
            assert_eq!(pending.regions[region.index()].residual_retained, 0);
            assert_eq!(pending.regions[region.index()].pending_reclaim, 1);
            assert_eq!(pending.faults.pinned_slots, 0);
            assert_eq!(store.store().reclaim(usize::MAX), 1);
            let reclaimed = AttributionSnapshot::sample(store.store(), BandSnapshot::default());
            assert_eq!(reclaimed.regions[region.index()].residual_retained, 0);
            assert_eq!(reclaimed.regions[region.index()].pending_reclaim, 0);
        }
    }
}
