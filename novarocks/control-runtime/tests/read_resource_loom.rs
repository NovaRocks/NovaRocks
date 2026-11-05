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

#![cfg(loom)]

//! Models the actual row pins and store/seal locks with two preemptions.
//! Store/View standard Arc/Weak lifetime is deliberately kept alive here;
//! its internal Weak algorithm is not modeled. No Worker R admission, RF,
//! Native IO or failure-owner cleanup is claimed by these Common models.

use loom::{
    sync::{Arc, mpsc},
    thread,
};
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
    identity::{ConsumerId, DimensionId, Generation, SlotId},
    owned_storage::OwnedSlotNeed,
    scope::{WorkAdmission, WorkKind},
};

fn model(run: impl Fn() + Send + Sync + 'static) {
    let mut model = loom::model::Builder::new();
    model.preemption_bound = Some(2);
    model.max_branches = 20_000;
    model.check(run);
}
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
    }
}
fn assert_original_unexited(f: &Fixture) {
    assert_eq!(f.exit_custody.phase(), ExitCustodyPhase::Admitted);
    assert!(!f.exit_custody.body_exited() && !f.exit_custody.captures_exited());
    assert_eq!(f.ledger.used(dimension()), 1);
    for consumer in [1, 2] {
        assert_eq!(
            f.report
                .consumption(ConsumerId::new(consumer).unwrap())
                .unwrap(),
            Consumption::Pending
        );
    }
}
fn finish(f: Fixture, authority: ExitObligation) {
    // Only the actual caller supplies this original positive exit. Neither a
    // released row pin nor a successful take can sign it on the caller's behalf.
    assert_original_unexited(&f);
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
fn sealing_and_original_loan_acquisition_have_one_order_without_early_role_exit() {
    model(|| {
        let mut f = fixture();
        let original = f.scope.snapshot().unwrap();
        let store = ReadResourceStore::new(f.scope.clone(), 1);
        let owner = store.insert(slot(), &f.scope, generation(), 41u64).unwrap();
        let view = owner.view();
        let authority = f.authority.take().unwrap();
        let reader = thread::spawn(move || view.loan(authority));
        owner.seal().unwrap();
        let (owner, authority) = match reader.join().unwrap() {
            Ok(loan) => {
                // If the actual loan won the seal race, it remains readable.
                assert_eq!(*loan.value(), 41);
                let (error, original_owner) = owner.try_take().unwrap_err();
                assert_eq!(error, ReadResourceError::Busy);
                (original_owner, loan.reclaim())
            }
            Err((error, authority)) => {
                assert_eq!(error, ReadResourceError::Sealed);
                (owner, authority)
            }
        };
        assert_original_unexited(&f);
        assert_eq!(f.scope.snapshot().unwrap(), original);
        assert_eq!(owner.try_take().unwrap(), 41);
        assert!(store.is_empty());
        drop(store);
        finish(f, authority);
    });
}

#[test]
fn the_last_actual_loan_alias_races_host_take_but_cannot_retire_its_paid_role() {
    model(|| {
        let mut f = fixture();
        let original = f.scope.snapshot().unwrap();
        let store = ReadResourceStore::new(f.scope.clone(), 1);
        let owner = store.insert(slot(), &f.scope, generation(), 73u64).unwrap();
        let loan = Arc::new(owner.view().loan(f.authority.take().unwrap()).unwrap());
        let actual_alias = loan.clone();
        let (release, released) = mpsc::channel();
        let reader = thread::spawn(move || {
            released.recv().unwrap();
            let observed = *actual_alias.value();
            let original = Arc::try_unwrap(actual_alias)
                .expect("the caller released the other actual alias before signaling");
            (observed, original.reclaim())
        });
        owner.seal().unwrap();
        let (error, owner) = owner.try_take().unwrap_err();
        assert_eq!(error, ReadResourceError::Busy);
        // The channel orders only outer-alias release. The subsequent actual
        // row-pin reclaim races the host's real try_take below.
        drop(loan);
        release.send(()).unwrap();
        let attempted = owner.try_take();
        let (observed, authority) = reader.join().unwrap();
        let value = match attempted {
            Ok(value) => value,
            Err((error, owner)) => {
                assert_eq!(error, ReadResourceError::Busy);
                owner.try_take().unwrap()
            }
        };
        assert_eq!(observed, 73);
        assert_eq!(value, 73);
        assert_original_unexited(&f);
        assert_eq!(f.scope.snapshot().unwrap(), original);
        assert!(store.is_empty());
        drop(store);
        finish(f, authority);
    });
}

#[test]
fn concurrent_recovery_returns_one_original_owner_and_preserves_existing_read_pins() {
    model(|| {
        let mut f = fixture();
        let original = f.scope.snapshot().unwrap();
        let store = ReadResourceStore::new(f.scope.clone(), 1);
        let owner = store.insert(slot(), &f.scope, generation(), 97u64).unwrap();
        let loan = owner.view().loan(f.authority.take().unwrap()).unwrap();
        owner.seal().unwrap();
        let inventory = store.inventory(f.scope.id(), generation(), None, 1);
        assert_eq!(inventory.scanned, 1);
        assert_eq!(inventory.rows.len(), 1);
        let entry = inventory.rows.into_iter().next().unwrap();
        assert!(entry.owner_present && entry.sealed);
        assert!(matches!(
            store.recover_owner(&entry.address),
            Err(ReadResourceError::OwnerPresent)
        ));
        assert_original_unexited(&f);
        // Owner Drop relinquishes metadata ownership only. The store and the
        // already issued read loan still own the actual original row.
        drop(owner);
        let address = entry.address;
        let other_address = address.clone();
        let other_store = store.clone();
        let first = thread::spawn(move || other_store.recover_owner(&other_address));
        let other_store = store.clone();
        let second = thread::spawn(move || other_store.recover_owner(&address));
        // A successful result remains alive in its real join/result holder;
        // it cannot be dropped between the two competing recovery attempts.
        let first = first.join().unwrap();
        let second = second.join().unwrap();
        let owner = match (first, second) {
            (Ok(owner), Err(ReadResourceError::OwnerPresent))
            | (Err(ReadResourceError::OwnerPresent), Ok(owner)) => owner,
            other => panic!("the original row has one active recovery owner: {other:?}"),
        };
        let (error, owner) = owner.try_take().unwrap_err();
        assert_eq!(error, ReadResourceError::Busy);
        assert_eq!(*loan.value(), 97);
        assert_original_unexited(&f);
        assert_eq!(f.scope.snapshot().unwrap(), original);
        let authority = loan.reclaim();
        assert_eq!(owner.try_take().unwrap(), 97);
        assert!(store.is_empty());
        assert_original_unexited(&f);
        drop(store);
        finish(f, authority);
    });
}
