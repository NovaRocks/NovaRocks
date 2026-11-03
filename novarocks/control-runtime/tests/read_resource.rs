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

#![cfg(not(loom))]

use novarocks_control_runtime::{
    completion::ReportSlot,
    exit_custody::{ExitCustody, ExitCustodyPhase, ExitObligation},
    read_resource::{ReadResourceError, ReadResourceStore},
    reported_work::ReportedWork,
    transaction::{RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    completion::{ConsumerKind, Consumption},
    identity::{ConsumerId, DimensionId, Generation, SlotId, WorkId},
    owned_storage::OwnedSlotNeed,
    scope::{WorkAdmission, WorkKind},
};
use std::{
    mem::ManuallyDrop,
    sync::{
        Arc, Barrier, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

fn generation() -> Generation {
    Generation::new(1).unwrap()
}
fn dimension() -> DimensionId {
    DimensionId::new(1).unwrap()
}
fn slot() -> SlotId {
    SlotId::new(1).unwrap()
}
struct Fixture {
    ledger: RuntimeLedger,
    scope: ScopeHandle,
    report: ReportSlot,
    exit_custody: ExitCustody,
    authority: Option<ExitObligation>,
    work: WorkId,
}
fn fixture() -> Fixture {
    let resources = ResourceVector::new([(dimension(), 1)]).unwrap();
    let ledger = RuntimeLedger::new(resources.clone());
    let scope = ledger.open_scope_next(None, generation()).unwrap();
    let (work, mut capacity) = scope
        .reserve_next(
            generation(),
            ReservationResources::new(
                resources,
                OwnedSlotNeed {
                    work_headers: 1,
                    completions: 1,
                    captures: 1,
                    ..OwnedSlotNeed::default()
                },
            ),
            WorkKind::Business,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Business)
        .unwrap();
    let work_id = work.id();
    let report = ReportSlot::reserve_next(
        &mut capacity,
        generation(),
        2,
        [
            (
                ConsumerId::new(1).unwrap(),
                ConsumerKind::Owner {
                    scope: scope.id(),
                    generation: generation(),
                },
            ),
            (ConsumerId::new(2).unwrap(), ConsumerKind::Observer),
        ],
    )
    .unwrap();
    let (exit_custody, authority) =
        ExitCustody::from_admitted(ReportedWork::try_new(work, capacity, report.clone()).unwrap());
    Fixture {
        ledger,
        scope,
        report,
        exit_custody,
        authority: Some(authority),
        work: work_id,
    }
}
fn finish(f: Fixture, authority: ExitObligation) {
    authority.exited().unwrap();
    assert_eq!(f.exit_custody.phase(), ExitCustodyPhase::Exited);
    assert_eq!(f.ledger.used(dimension()), 1);
    f.report.consume(ConsumerId::new(1).unwrap()).unwrap();
    assert!(!f.exit_custody.try_retire().unwrap());
    assert_eq!(f.ledger.used(dimension()), 1);
    f.report.consume(ConsumerId::new(2).unwrap()).unwrap();
    assert!(f.exit_custody.try_retire().unwrap());
    assert_eq!(f.ledger.used(dimension()), 0);
    assert_eq!(f.ledger.reap_scope_ready(f.scope.id(), 1).unwrap(), 1);
    f.scope.close_external().unwrap();
    f.ledger.remove_scope(f.scope.id()).unwrap();
    assert!(f.ledger.is_empty());
}

#[test]
fn a_read_transfers_the_original_paid_authority_and_reclaim_does_not_sign_exit() {
    let mut f = fixture();
    let before = f.scope.snapshot().unwrap();
    let store = ReadResourceStore::new(f.scope.clone(), 1);
    let owner = store.insert(slot(), &f.scope, generation(), 11u64).unwrap();
    let zero = store.inventory(f.scope.id(), generation(), None, 0);
    assert_eq!(zero.scanned, 0);
    assert!(zero.rows.is_empty() && !zero.complete);
    let filtered = store.inventory(f.scope.id(), Generation::new(2).unwrap(), None, 1);
    assert_eq!(filtered.scanned, 1);
    assert_eq!(filtered.next_after, Some(slot()));
    assert!(filtered.rows.is_empty());
    let inventory = store.inventory(f.scope.id(), generation(), None, 1);
    assert_eq!(inventory.scanned, 1);
    assert_eq!(inventory.rows.len(), 1);
    assert!(inventory.rows[0].owner_present && !inventory.rows[0].sealed);
    let address = inventory.rows[0].address.clone();
    assert_eq!(
        (address.id(), address.scope(), address.generation()),
        (slot(), f.scope.id(), generation())
    );
    assert!(matches!(
        store.recover_owner(&address),
        Err(ReadResourceError::OwnerPresent)
    ));
    let tail = store.inventory(f.scope.id(), generation(), inventory.next_after, 1);
    assert!(tail.complete && tail.rows.is_empty());
    assert_eq!(tail.scanned, 0);
    assert_eq!(
        store
            .insert(slot(), &f.scope, generation(), 22u64)
            .unwrap_err(),
        (ReadResourceError::Duplicate, 22)
    );
    assert_eq!(
        store
            .insert(SlotId::new(2).unwrap(), &f.scope, generation(), 33u64)
            .unwrap_err(),
        (ReadResourceError::Full, 33)
    );
    let (error, owner) = owner.try_take().unwrap_err();
    assert_eq!(error, ReadResourceError::NotSealed);
    let view = owner.view();
    let alias = view.clone();
    let mut loan = alias.loan(f.authority.take().unwrap()).unwrap();
    assert_eq!(*loan.value(), 11);
    assert_eq!(f.exit_custody.phase(), ExitCustodyPhase::Admitted);
    assert_eq!(f.scope.snapshot().unwrap(), before);
    loan.start_original().unwrap();
    assert_eq!(f.exit_custody.phase(), ExitCustodyPhase::Started);
    assert_eq!(f.scope.snapshot().unwrap().work, before.work);
    store.seal_address(&address).unwrap();
    assert!(matches!(
        store.recover_owner(&address),
        Err(ReadResourceError::OwnerPresent)
    ));
    owner.seal().unwrap();
    assert_eq!(
        *loan.value(),
        11,
        "sealing cannot revoke an actual existing loan"
    );
    let authority = loan.reclaim();
    assert_eq!(f.exit_custody.phase(), ExitCustodyPhase::Started);
    assert!(!f.exit_custody.body_exited() && !f.exit_custody.captures_exited());
    assert_eq!(f.ledger.used(dimension()), 1);
    let (error, authority) = view.loan(authority).unwrap_err();
    assert_eq!(error, ReadResourceError::Sealed);
    assert_eq!(owner.try_take().unwrap(), 11);
    assert!(store.is_empty());
    assert_eq!(f.scope.snapshot().unwrap().work, before.work);
    drop((alias, view, store));
    finish(f, authority);
}

#[test]
fn a_sealed_owner_waits_for_the_last_arc_alias_of_the_same_actual_loan() {
    let mut f = fixture();
    let before = f.scope.snapshot().unwrap();
    let store = ReadResourceStore::new(f.scope.clone(), 1);
    let owner = store.insert(slot(), &f.scope, generation(), 37u64).unwrap();
    let loan = Arc::new(owner.view().loan(f.authority.take().unwrap()).unwrap());
    let actual_alias = loan.clone();
    let entered = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let worker_entered = entered.clone();
    let worker_release = release.clone();
    let worker = std::thread::spawn(move || {
        let observed = *actual_alias.value();
        worker_entered.wait();
        worker_release.wait();
        let loan =
            Arc::try_unwrap(actual_alias).expect("the last actual alias reclaims the same loan");
        (observed, loan.reclaim())
    });
    entered.wait();
    owner.seal().unwrap();
    let mut premature_value = None;
    let (first_error, mut owner) = match owner.try_take() {
        Err((error, original)) => (Some(error), Some(original)),
        Ok(value) => {
            premature_value = Some(value);
            (None, None)
        }
    };
    drop(loan);
    let second_error = match owner.take() {
        Some(original) => match original.try_take() {
            Err((error, original)) => {
                owner = Some(original);
                Some(error)
            }
            Ok(value) => {
                premature_value = Some(value);
                None
            }
        },
        None => None,
    };
    let during = f.scope.snapshot().unwrap();
    let changed_before_release = store.change_version();
    // Release and join the actual worker before evaluating the oracle,
    // including a regression that would have taken the resource too early.
    release.wait();
    let (observed, authority) = worker.join().unwrap();
    let notified = store.change_version() > changed_before_release;
    let phase = f.exit_custody.phase();
    let value = match owner {
        Some(owner) => owner.try_take().unwrap(),
        None => premature_value.unwrap(),
    };
    assert!(store.is_empty());
    drop(store);
    finish(f, authority);
    assert_eq!(first_error, Some(ReadResourceError::Busy));
    assert_eq!(second_error, Some(ReadResourceError::Busy));
    assert_eq!(during, before);
    assert_eq!(observed, 37);
    assert!(notified);
    assert_eq!(phase, ExitCustodyPhase::Admitted);
    assert_eq!(value, 37);
}

#[test]
fn reused_numeric_resource_identity_never_gives_an_old_view_the_new_payload() {
    let mut f = fixture();
    let store = ReadResourceStore::new(f.scope.clone(), 1);
    let old = store.insert(slot(), &f.scope, generation(), 3u64).unwrap();
    let old_view = old.view();
    let address = store.inventory(f.scope.id(), generation(), None, 1).rows[0]
        .address
        .clone();
    old.seal().unwrap();
    assert_eq!(old.try_take().unwrap(), 3);
    let next = store.insert(slot(), &f.scope, generation(), 9u64).unwrap();
    let next_view = next.view();
    assert!(
        store.inventory(f.scope.id(), generation(), None, 1).rows[0].owner_present,
        "the consumed old owner's Drop cannot mark the replacement owner absent"
    );
    assert!(matches!(
        store.recover_owner(&address),
        Err(ReadResourceError::WrongIdentity)
    ));
    assert_eq!(
        store.seal_address(&address),
        Err(ReadResourceError::WrongIdentity)
    );
    let (error, authority) = old_view.loan(f.authority.take().unwrap()).unwrap_err();
    assert_eq!(error, ReadResourceError::WrongIdentity);
    assert_eq!(f.exit_custody.phase(), ExitCustodyPhase::Admitted);
    let loan = next_view.loan(authority).unwrap();
    assert_eq!(*loan.value(), 9);
    let authority = loan.reclaim();
    next.seal().unwrap();
    assert_eq!(next.try_take().unwrap(), 9);
    drop(store);
    // Metadata views do not keep the original store or its root anchor alive.
    let (error, authority) = next_view.loan(authority).unwrap_err();
    assert_eq!(error, ReadResourceError::StoreGone);
    drop((old_view, next_view));
    finish(f, authority);
}

#[test]
fn foreign_ledger_authority_is_returned_even_when_every_numeric_identity_matches() {
    let mut f = fixture();
    let mut foreign = fixture();
    assert_eq!(f.scope.id(), foreign.scope.id());
    assert_eq!(f.work, foreign.work);
    assert_eq!(f.report.id(), foreign.report.id());
    let store = ReadResourceStore::new(f.scope.clone(), 1);
    let owner = store.insert(slot(), &f.scope, generation(), 17u64).unwrap();
    let address = store.inventory(f.scope.id(), generation(), None, 1).rows[0]
        .address
        .clone();
    let other_store = ReadResourceStore::<u64>::new(f.scope.clone(), 1);
    assert!(matches!(
        other_store.recover_owner(&address),
        Err(ReadResourceError::WrongStore)
    ));
    assert_eq!(
        other_store.seal_address(&address),
        Err(ReadResourceError::WrongStore)
    );
    drop(other_store);
    let (error, authority) = owner
        .view()
        .loan(foreign.authority.take().unwrap())
        .unwrap_err();
    assert_eq!(error, ReadResourceError::WrongAuthority);
    assert_eq!(foreign.exit_custody.phase(), ExitCustodyPhase::Admitted);
    assert_eq!(foreign.ledger.used(dimension()), 1);
    let (error, returned) = store
        .insert(SlotId::new(2).unwrap(), &foreign.scope, generation(), 19u64)
        .unwrap_err();
    assert_eq!((error, returned), (ReadResourceError::WrongLedger, 19));
    owner.seal().unwrap();
    assert_eq!(owner.try_take().unwrap(), 17);
    drop(store);
    let original = f.authority.take().unwrap();
    finish(f, original);
    finish(foreign, authority);
}

#[test]
fn recovering_a_dropped_original_owner_is_unique_and_still_waits_for_actual_loans() {
    let mut f = fixture();
    let before = f.scope.snapshot().unwrap();
    let store = ReadResourceStore::new(f.scope.clone(), 1);
    let owner = store.insert(slot(), &f.scope, generation(), 97u64).unwrap();
    let view = owner.view();
    let address = store.inventory(f.scope.id(), generation(), None, 1).rows[0]
        .address
        .clone();
    let loan = view.loan(f.authority.take().unwrap()).unwrap();
    store.seal_address(&address).unwrap();
    drop(owner);
    let inventory = store.inventory(f.scope.id(), generation(), None, 1);
    assert!(!inventory.rows[0].owner_present && inventory.rows[0].sealed);
    assert_eq!(*loan.value(), 97);
    assert_eq!(f.scope.snapshot().unwrap(), before);
    let race = Arc::new(Barrier::new(3));
    let workers: Vec<_> = (0..2)
        .map(|_| {
            let store = store.clone();
            let address = address.clone();
            let race = race.clone();
            std::thread::spawn(move || {
                race.wait();
                store.recover_owner(&address)
            })
        })
        .collect();
    race.wait();
    // Keep every actual returned owner alive until both attempts returned;
    // an ordinary owner Drop must not open a new recovery during the oracle.
    let results: Vec<_> = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect();
    let mut recovered = Vec::new();
    let mut refusals = Vec::new();
    for result in results {
        match result {
            Ok(owner) => recovered.push(owner),
            Err(error) => refusals.push(error),
        }
    }
    let recovered_count = recovered.len();
    if recovered.is_empty() {
        // No original metadata authority is available to prove disposal.
        // Preserve this finite actual inventory instead of inventing S8 exit.
        let _retained_original = ManuallyDrop::new((f, store, loan, view));
        assert_ne!(
            recovered_count, 0,
            "one failure owner must recover the original row"
        );
        return;
    }
    let owner = recovered.remove(0);
    drop(recovered);
    owner.seal().unwrap();
    let (error, owner) = owner.try_take().unwrap_err();
    assert_eq!(error, ReadResourceError::Busy);
    assert_eq!(*loan.value(), 97);
    let authority = loan.reclaim();
    assert_eq!(owner.try_take().unwrap(), 97);
    assert!(store.is_empty());
    assert_eq!(f.scope.snapshot().unwrap(), before);
    assert_eq!(f.exit_custody.phase(), ExitCustodyPhase::Admitted);
    assert_eq!(f.ledger.used(dimension()), 1);
    for consumer in [1, 2] {
        assert_eq!(
            f.report
                .consumption(ConsumerId::new(consumer).unwrap())
                .unwrap(),
            Consumption::Pending
        );
    }
    drop((store, view));
    finish(f, authority);
    assert_eq!(recovered_count, 1);
    assert_eq!(refusals, vec![ReadResourceError::OwnerPresent]);
}

struct DropProbe {
    drops: Arc<AtomicUsize>,
    disposal_thread: Arc<Mutex<Option<std::thread::ThreadId>>>,
    inspect: Box<dyn Fn() + Send + Sync>,
}
impl Drop for DropProbe {
    fn drop(&mut self) {
        (self.inspect)();
        *self.disposal_thread.lock().unwrap() = Some(std::thread::current().id());
        self.drops.fetch_add(1, Ordering::AcqRel);
    }
}
impl std::fmt::Debug for DropProbe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("original read payload")
    }
}

#[test]
fn opaque_payload_drop_belongs_to_the_host_after_take_outside_the_store_lock() {
    let mut f = fixture();
    let drops = Arc::new(AtomicUsize::new(0));
    let disposal_thread = Arc::new(Mutex::new(None));
    let store: ReadResourceStore<DropProbe> = ReadResourceStore::new(f.scope.clone(), 1);
    let metadata = store.clone();
    let owner = store
        .insert(
            slot(),
            &f.scope,
            generation(),
            DropProbe {
                drops: drops.clone(),
                disposal_thread: disposal_thread.clone(),
                inspect: Box::new(move || assert!(metadata.is_empty())),
            },
        )
        .unwrap();
    let view = owner.view();
    let loan = view.clone().loan(f.authority.take().unwrap()).unwrap();
    owner.seal().unwrap();
    let authority = loan.reclaim();
    drop(view);
    assert_eq!(drops.load(Ordering::Acquire), 0);
    let address = store.inventory(f.scope.id(), generation(), None, 1).rows[0]
        .address
        .clone();
    drop(owner);
    assert_eq!(drops.load(Ordering::Acquire), 0);
    let owner = store.recover_owner(&address).unwrap();
    let original = owner.try_take().unwrap();
    assert_eq!(drops.load(Ordering::Acquire), 0);
    assert_eq!(f.ledger.used(dimension()), 1);
    drop(original);
    assert_eq!(drops.load(Ordering::Acquire), 1);
    assert_eq!(
        *disposal_thread.lock().unwrap(),
        Some(std::thread::current().id())
    );
    drop(store);
    finish(f, authority);
}

#[test]
fn losing_a_loan_releases_only_its_pin_and_keeps_the_original_role_unknown_and_paid() {
    let mut f = fixture();
    let drops = Arc::new(AtomicUsize::new(0));
    let disposal_thread = Arc::new(Mutex::new(None));
    let store = ReadResourceStore::new(f.scope.clone(), 1);
    let owner = store
        .insert(
            slot(),
            &f.scope,
            generation(),
            DropProbe {
                drops: drops.clone(),
                disposal_thread,
                inspect: Box::new(|| {}),
            },
        )
        .unwrap();
    let loan = owner.view().loan(f.authority.take().unwrap()).unwrap();
    owner.seal().unwrap();
    drop(loan);
    assert_eq!(f.exit_custody.phase(), ExitCustodyPhase::Unresolved);
    assert!(!f.exit_custody.body_exited() && !f.exit_custody.captures_exited());
    assert!(!f.exit_custody.try_retire().unwrap());
    assert_eq!(f.ledger.used(dimension()), 1);
    for consumer in [1, 2] {
        assert_eq!(
            f.report
                .consumption(ConsumerId::new(consumer).unwrap())
                .unwrap(),
            Consumption::Pending
        );
    }
    assert_eq!(drops.load(Ordering::Acquire), 0);
    // The pin actually ended, so the original host may dispose its payload.
    // That is independent of the lost IO authority's Unknown exit/report.
    drop(owner.try_take().unwrap());
    assert_eq!(drops.load(Ordering::Acquire), 1);
    assert!(store.is_empty());
    drop(store);
    let _retained_original = ManuallyDrop::new(f);
}
