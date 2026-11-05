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

//! Stable allocation facts independent of funding and query lifetime.
pub mod faults;
pub mod handle;
pub mod owner;
pub mod record;
pub(crate) mod registry;
pub use registry::OBSERVATION_LANE_METADATA_BYTES;
pub mod shards;
pub use shards::{CoverageGap, LaneShards, ShardSnapshot, ShardsSnapshot};
pub mod slot;
pub mod store;
pub mod token;
pub use handle::{FactToken, Lane};
pub use owner::RecordOwner;
/// Frozen requested-size boundary, shared by wrappers, helpers and fact routing.
pub const ATTRIBUTION_THRESHOLD_BYTES: usize = 512;
pub use record::{LaneRecord, LifetimeState, ProductionState, RecordSnapshot, ResponsibilityClass};
pub use slot::{SLOT_QUANTUM_BYTES, SlotCore};
#[cfg(not(loom))]
pub use store::global_store;
pub use store::{CoverageError, MAX_RECORDS, RecordStore, RecordStorePin, UNATTRIBUTED_SHARDS};
pub use token::RecordRef;
pub const LANE_SHARDS: usize = if cfg!(loom) { 2 } else { 16 };
pub use store::{
    REGION_LABELS, RecordRegion, RegionSnapshot, THREAD_ROLE_CAPACITY, UNATTRIBUTED_RECORDS,
};
#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    #[test]
    fn bounded_store_and_generation_reuse() {
        let store = RecordStorePin::owned(crate::lane::UNATTRIBUTED_RECORDS + 4);
        let first = store
            .acquire(
                1,
                ResponsibilityClass::Service,
                crate::lane::RecordRegion::Observation,
            )
            .unwrap();
        let second = store
            .acquire(
                2,
                ResponsibilityClass::Query,
                crate::lane::RecordRegion::Observation,
            )
            .unwrap();
        assert_eq!(
            store
                .acquire(
                    3,
                    ResponsibilityClass::Query,
                    crate::lane::RecordRegion::Observation
                )
                .unwrap_err(),
            CoverageError::RecordStoreExhausted {
                region: crate::lane::RecordRegion::Observation
            }
        );
        let old = first.reference();
        drop(first);
        assert_eq!(store.store().reclaim(1), 1);
        assert!(store.store().resolve(old).is_none());
        let replacement = store
            .acquire(
                3,
                ResponsibilityClass::Query,
                crate::lane::RecordRegion::Observation,
            )
            .unwrap();
        let next = replacement.reference();
        assert_eq!(old.index, next.index);
        assert_eq!(old.generation + 1, next.generation);
        // SAFETY: stale identity is rejected before obtaining any record access.
        unsafe { SlotCore::direct(store.store(), old, 50, 0, 1) };
        assert_eq!(replacement.record().snapshot().outstanding, 0);
        assert_eq!(store.store().faults.snapshot().orphan_events, 1);
        drop(second);
        drop(replacement);
        assert_eq!(store.store().reclaim(8), 2);
    }
    #[test]
    fn draining_waits_for_negative_count_and_slot_pin_to_converge() {
        let store = RecordStorePin::owned(crate::lane::UNATTRIBUTED_RECORDS + 2);
        let owner = store
            .acquire(
                42,
                ResponsibilityClass::Query,
                crate::lane::RecordRegion::Observation,
            )
            .unwrap();
        let reference = owner.reference();
        let mut slot = SlotCore::new();
        // SAFETY: live owner pins positive publication and the store is retained.
        unsafe { slot.add(store.store(), reference, 600, 0, 1, true) };
        drop(owner);
        assert_eq!(store.store().reclaim(1), 0);
        // SAFETY: this distinct allocation remains protected by the slot's
        // pending publication/pin even though its count is not flushed yet.
        unsafe { SlotCore::direct(store.store(), reference, -600, 0, -1) };
        let life = store.store().resolve(reference).unwrap().lifetime();
        assert_eq!((life.outstanding(), life.pins()), (-1, 1));
        assert_eq!(store.store().reclaim(1), 0);
        // SAFETY: flush releases this slot's unique pin as its last record access.
        unsafe { slot.flush(store.store()) };
        assert!(slot.is_empty());
        assert_eq!(store.store().reclaim(1), 1);
        assert!(store.store().snapshot_ref(reference).is_none());
        assert_eq!(store.store().faults.snapshot().pinned_slots, 0);
    }
    #[test]
    fn quantum_flush_and_direct_path_publish_real_counts() {
        let store = RecordStorePin::owned(crate::lane::UNATTRIBUTED_RECORDS + 2);
        let owner = store
            .acquire(
                1,
                ResponsibilityClass::Service,
                crate::lane::RecordRegion::Observation,
            )
            .unwrap();
        let reference = owner.reference();
        let mut slot = SlotCore::new();
        let half = (SLOT_QUANTUM_BYTES / 2) as i64;
        // SAFETY: owner remains live throughout both allocations and releases.
        unsafe {
            slot.add(store.store(), reference, half, 0, 1, true);
            assert!(!slot.is_empty());
            assert_eq!(owner.record().snapshot().outstanding, 0);
            slot.add(store.store(), reference, half, 0, 1, true);
            assert!(slot.is_empty());
            assert_eq!(owner.record().snapshot().outstanding, 2);
            slot.add(store.store(), reference, -half, 0, -1, false);
            slot.add(store.store(), reference, -half, 0, -1, false);
            slot.add(
                store.store(),
                reference,
                SLOT_QUANTUM_BYTES as i64,
                0,
                1,
                true,
            );
            assert!(slot.is_empty());
            SlotCore::direct(
                store.store(),
                reference,
                -(SLOT_QUANTUM_BYTES as i64),
                0,
                -1,
            );
        }
        let snapshot = owner.record().snapshot();
        assert_eq!((snapshot.tagged_bytes, snapshot.outstanding), (0, 0));
        drop(owner);
        assert_eq!(store.store().reclaim(1), 1);
    }
    #[test]
    fn owned_store_outlives_owner_and_last_allocation() {
        let store = RecordStorePin::owned(crate::lane::UNATTRIBUTED_RECORDS + 2);
        let owner = store
            .acquire(
                7,
                ResponsibilityClass::Service,
                crate::lane::RecordRegion::Observation,
            )
            .unwrap();
        let reference = owner.reference();
        // SAFETY: owner protects publication; retained store protects backing.
        unsafe { SlotCore::direct(store.store(), reference, 600, 0, 1) };
        drop(owner);
        assert_eq!(store.store().reclaim(1), 0);
        // SAFETY: genuine outstanding allocation, freed exactly once.
        unsafe { SlotCore::direct(store.store(), reference, -600, 0, -1) };
        assert_eq!(store.store().reclaim(1), 1);
        drop(store);
    }
}
