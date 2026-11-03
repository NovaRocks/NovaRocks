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

//! Models execute the production SlotCore, lifetime word and reclaim CAS.
//! Stable System backing has a separate Miri boundary. No copied token is
//! accessed after its final free except a known-invalid generation probe.
use crate::lane::{RecordStorePin, ResponsibilityClass, SlotCore};
use loom::{model::Builder, thread};

fn model(f: impl Fn() + Send + Sync + 'static) {
    let mut builder = Builder::new();
    builder.max_threads = 4;
    builder.preemption_bound = Some(2);
    builder.max_branches = 20_000;
    builder.max_permutations = None;
    builder.max_duration = None;
    builder.checkpoint_file = None;
    builder.check(f);
}
#[test]
fn l1_remote_free_before_flush_never_reclaims_a_pinned_record() {
    model(|| {
        let store = RecordStorePin::owned(crate::lane::UNATTRIBUTED_RECORDS + 2);
        let owner = store
            .acquire(
                1,
                ResponsibilityClass::Query,
                crate::lane::RecordRegion::Observation,
            )
            .unwrap();
        let reference = owner.reference();
        let producer_store = store.clone();
        let remote_store = store.clone();
        let collector_store = store.clone();
        let (publish, receive) = loom::sync::mpsc::channel();
        let producer = thread::spawn(move || {
            let mut slot = SlotCore::new();
            // SAFETY: owner protects initial publication; slot protects delayed facts.
            unsafe { slot.add(producer_store.store(), reference, 600, 0, 1, true) };
            drop(owner);
            publish.send(reference).unwrap();
            thread::yield_now();
            // SAFETY: this slot owns its unique pin until the final update.
            unsafe { slot.flush(producer_store.store()) };
        });
        let remote = thread::spawn(move || {
            let reference = receive.recv().unwrap();
            // SAFETY: this is the allocation published by the producer. Its
            // buffered count is protected by the producer's independent slot pin.
            unsafe { SlotCore::direct(remote_store.store(), reference, -600, 0, -1) };
        });
        let collector = thread::spawn(move || collector_store.store().reclaim(1));
        producer.join().unwrap();
        remote.join().unwrap();
        let reclaimed = collector.join().unwrap();
        assert_eq!(reclaimed + store.store().reclaim(1), 1);
        assert!(store.store().resolve(reference).is_none());
        let faults = store.store().faults.snapshot();
        assert_eq!(
            (
                faults.orphan_events,
                faults.reclaim_nonzero_events,
                faults.pinned_slots
            ),
            (0, 0, 0)
        );
    });
}
#[test]
fn l2_resize_keeps_its_allocation_count_while_another_allocation_frees() {
    model(|| {
        let store = RecordStorePin::owned(crate::lane::UNATTRIBUTED_RECORDS + 2);
        let owner = store
            .acquire(
                1,
                ResponsibilityClass::Service,
                crate::lane::RecordRegion::Observation,
            )
            .unwrap();
        let reference = owner.reference();
        // SAFETY: owner protects publication of two independent allocations.
        unsafe { SlotCore::direct(store.store(), reference, 1200, 0, 2) };
        drop(owner);
        let resize_store = store.clone();
        let free_store = store.clone();
        let collector_store = store.clone();
        let resize = thread::spawn(move || {
            // SAFETY: the first allocation remains live throughout resize.
            unsafe { SlotCore::direct(resize_store.store(), reference, 128, 0, 0) };
        });
        let free = thread::spawn(move || {
            // SAFETY: exactly the second allocation is freed once.
            unsafe { SlotCore::direct(free_store.store(), reference, -600, 0, -1) };
        });
        let collector = thread::spawn(move || collector_store.store().reclaim(1));
        resize.join().unwrap();
        free.join().unwrap();
        assert_eq!(collector.join().unwrap(), 0);
        let snapshot = store.store().snapshot_ref(reference).unwrap();
        assert_eq!((snapshot.tagged_bytes, snapshot.outstanding), (728, 1));
        // SAFETY: resized first allocation is now freed exactly once.
        unsafe { SlotCore::direct(store.store(), reference, -728, 0, -1) };
        assert_eq!(store.store().reclaim(1), 1);
    });
}
#[test]
fn l5_true_token_access_prevents_reuse_then_old_generation_is_rejected() {
    model(|| {
        let store = RecordStorePin::owned(crate::lane::UNATTRIBUTED_RECORDS + 2);
        let owner = store
            .acquire(
                1,
                ResponsibilityClass::Service,
                crate::lane::RecordRegion::Observation,
            )
            .unwrap();
        let old = owner.reference();
        // SAFETY: owner protects publication of the allocation handed to reader.
        unsafe { SlotCore::direct(store.store(), old, 600, 0, 1) };
        drop(owner);
        let reader_store = store.clone();
        let collector_store = store.clone();
        let (resolved, receive_resolved) = loom::sync::mpsc::channel();
        let (checked, receive_checked) = loom::sync::mpsc::channel();
        let reader = thread::spawn(move || {
            // A genuine, unfreed allocation remains the access capability
            // across this deliberate resolve/write gap.
            assert!(reader_store.store().resolve(old).is_some());
            resolved.send(()).unwrap();
            receive_checked.recv().unwrap();
            // SAFETY: the allocation is still outstanding and is freed once.
            unsafe { SlotCore::direct(reader_store.store(), old, -600, 0, -1) };
        });
        let collector = thread::spawn(move || {
            receive_resolved.recv().unwrap();
            assert_eq!(collector_store.store().reclaim(1), 0);
            checked.send(()).unwrap();
        });
        let observer_store = store.clone();
        let observer = thread::spawn(move || {
            // A diagnostic observer has no allocation access capability. It
            // may only sample the stable atomic storage, never publish deltas.
            if let Some(snapshot) = observer_store.store().snapshot_ref(old) {
                assert_eq!(snapshot.generation, old.generation);
                assert_eq!(snapshot.origin, 1);
            }
        });
        reader.join().unwrap();
        collector.join().unwrap();
        assert_eq!(store.store().reclaim(1), 1);
        let new_owner = store
            .acquire(
                2,
                ResponsibilityClass::Query,
                crate::lane::RecordRegion::Observation,
            )
            .unwrap();
        let new = new_owner.reference();
        assert_eq!((new.index, new.generation), (old.index, old.generation + 1));
        observer.join().unwrap();
        // SAFETY: old is now known-invalid; resolution refuses before any write.
        unsafe { SlotCore::direct(store.store(), old, 99, 0, 1) };
        assert_eq!(new_owner.record().snapshot().outstanding, 0);
        assert_eq!(store.store().faults.snapshot().orphan_events, 1);
        drop(new_owner);
        assert_eq!(store.store().reclaim(1), 1);
    });
}

#[test]
fn l4_query_handoff_late_free_flush_and_control_reclaim_keep_one_record() {
    model(|| {
        use crate::{AccountKind, AuthorityConfig, ExternalRef, MemoryAuthority, TeardownEvidence};
        let mut config = AuthorityConfig::new(65_536, 65_536, 0);
        config.max_accounts = 2;
        config.max_active_owners = 1;
        config.metadata_budget_bytes = 4_096;
        let authority = loom::sync::Arc::new(MemoryAuthority::new(config).unwrap());
        let query = authority
            .create_account(AccountKind::Work, ExternalRef::NONE)
            .unwrap();
        let lane = query.create_lane().unwrap();
        let reference = lane.reference();
        let store = authority.shared.record_store.clone();
        let producer_store = store.clone();
        let release_store = store.clone();
        let (send, receive) = loom::sync::mpsc::channel();
        let (published, await_publication) = loom::sync::mpsc::channel();
        let producer = thread::spawn(move || {
            let mut slot = SlotCore::new();
            // SAFETY: held owner publishes a successful allocation; an outer
            // service step's pending slot retains its independent lifetime pin.
            unsafe {
                slot.add(producer_store.store(), reference, 600, 0, 1, true);
            }
            published.send(()).unwrap();
            drop(lane);
            send.send(reference).unwrap();
            thread::yield_now();
            // SAFETY: this exact pending slot uniquely owns its final pin.
            unsafe {
                slot.flush(producer_store.store());
            }
        });
        let retiring = authority.clone();
        let retire = thread::spawn(move || {
            await_publication.recv().unwrap();
            query
                .retire(&TeardownEvidence {
                    tasks_exited: true,
                    operators_destroyed: true,
                    io: &[],
                    now_ns: 1,
                })
                .unwrap();
            assert_eq!(
                retiring
                    .shared
                    .record_store
                    .store()
                    .resolve(reference)
                    .unwrap()
                    .responsibility_class(),
                ResponsibilityClass::Residual
            );
            retiring.maintain(32);
        });
        let free = thread::spawn(move || {
            let reference = receive.recv().unwrap();
            // SAFETY: this is the producer's genuine distinct allocation,
            // released once; source slot/count retains backing to final access.
            unsafe {
                SlotCore::direct(release_store.store(), reference, -600, 0, -1);
            }
        });
        producer.join().unwrap();
        retire.join().unwrap();
        free.join().unwrap();
        authority.maintain(32);
        assert!(store.store().snapshot_ref(reference).is_none());
        let faults = store.store().faults.snapshot();
        assert_eq!(
            (
                faults.orphan_events,
                faults.reclaim_nonzero_events,
                faults.pinned_slots,
                faults.residual_growth_events
            ),
            (0, 0, 0, 0)
        );
    });
}

#[test]
fn l3_r1_cross_band_publication_remote_free_and_last_owner_preserve_lifetime() {
    model(|| {
        let store = RecordStorePin::owned(crate::lane::UNATTRIBUTED_RECORDS + 2);
        let owner = store
            .acquire(
                3,
                ResponsibilityClass::Query,
                crate::lane::RecordRegion::Observation,
            )
            .unwrap();
        let reference = owner.reference();
        // Two proven R1 small blocks: one will resize, the other releases remotely.
        unsafe { SlotCore::direct(store.store(), reference, 0, 72, 2) };
        let producer_store = store.clone();
        let free_store = store.clone();
        let gc_store = store.clone();
        let (send, receive) = loom::sync::mpsc::channel();
        let producer = thread::spawn(move || {
            let mut slot = SlotCore::new();
            // SAFETY: owner independently protects each side of the successful
            // 64→512→128 resize, including its transient count-neutral gap.
            unsafe {
                slot.add(producer_store.store(), reference, 520, 0, 1, true);
                slot.add(producer_store.store(), reference, 0, -64, -1, true);
                slot.add(producer_store.store(), reference, -520, 0, -1, true);
                slot.add_transfer(producer_store.store(), reference, 0, 128, 1, true);
            }
            drop(owner);
            send.send(reference).unwrap();
            thread::yield_now();
            // SAFETY: this unique slot owns the pin through its final flush.
            unsafe { slot.flush(producer_store.store()) };
        });
        let free = thread::spawn(move || {
            // SAFETY: the distinct original 8-byte block is genuinely outstanding.
            unsafe { SlotCore::direct(free_store.store(), reference, 0, -8, -1) };
            let reference = receive.recv().unwrap();
            // SAFETY: final resized block is released once after publication
            // handoff; a source pin may still carry its count/byte obligation.
            unsafe { SlotCore::direct(free_store.store(), reference, 0, -128, -1) };
        });
        let gc = thread::spawn(move || {
            gc_store.store().reclaim(1);
        });
        producer.join().unwrap();
        free.join().unwrap();
        gc.join().unwrap();
        store.store().reclaim(1);
        assert!(store.store().snapshot_ref(reference).is_none());
        let faults = store.store().faults.snapshot();
        assert_eq!(
            (
                faults.orphan_events,
                faults.reclaim_nonzero_events,
                faults.pinned_slots
            ),
            (0, 0, 0)
        );
    });
}

#[test]
fn l6_regions_publish_one_boundary_segment_and_never_replace_addresses() {
    use crate::lane::{RecordRegion, UNATTRIBUTED_RECORDS};
    model(|| {
        // Both first dynamic positions are in segment 4, with distinct region
        // locks. There is no backing yet when the two threads start.
        let store = RecordStorePin::owned(UNATTRIBUTED_RECORDS + 4);
        let funding = store.clone();
        let observation = store.clone();
        let f = thread::spawn(move || {
            let owner = funding
                .acquire(1, ResponsibilityClass::Query, RecordRegion::Funding)
                .unwrap();
            let address = std::ptr::from_ref(owner.record()).addr();
            (owner, address)
        });
        let o = thread::spawn(move || {
            let owner = observation
                .acquire(2, ResponsibilityClass::Service, RecordRegion::Observation)
                .unwrap();
            let address = std::ptr::from_ref(owner.record()).addr();
            (owner, address)
        });
        let f = f.join().unwrap();
        let o = o.join().unwrap();
        assert_eq!(store.store().published_segment_count(), 1);
        for (owner, address) in [&f, &o] {
            assert_eq!(
                *address,
                std::ptr::from_ref(store.store().resolve(owner.reference()).unwrap()).addr()
            );
        }
        assert_eq!(f.0.record().snapshot().origin, 1);
        assert_eq!(o.0.record().snapshot().origin, 2);
    });
}
