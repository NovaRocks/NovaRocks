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

//! Actual prepaid Source roles, ordinary partial refusal and true header exit.

use super::*;
use novarocks_control_runtime_core::{
    capacity::{CapacityError, ResourceVector},
    owned_storage::{OwnedSlotKind, OwnedSlotNeed, OwnedStorageError},
    scope::WorkAdmission,
};

struct Fixture {
    ledger: RuntimeLedger,
    scope: ScopeHandle,
    parent: WorkObligation,
    payer: Option<ReservationObligation>,
}
impl Fixture {
    fn new(headers: usize, completions: usize, captures: usize) -> Self {
        let resources = ReservationResources::new(
            ResourceVector::default(),
            OwnedSlotNeed {
                work_headers: headers,
                completions,
                captures,
                ..OwnedSlotNeed::default()
            },
        );
        let ledger = RuntimeLedger::new(ResourceVector::default());
        let generation = Generation::new(1).unwrap();
        let scope = ledger.open_scope_next(None, generation).unwrap();
        let (mut parent, payer) = scope
            .reserve_next(
                generation,
                resources,
                WorkKind::Business,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Business)
            .unwrap();
        parent.start().unwrap();
        Self {
            ledger,
            scope,
            parent,
            payer: Some(payer),
        }
    }
    fn remaining(&self) -> OwnedSlotNeed {
        self.payer
            .as_ref()
            .unwrap()
            .resources()
            .unwrap()
            .owned_slots
    }
    fn finish(mut self) {
        self.scope.close_external().unwrap();
        self.parent.body_exited().unwrap();
        self.parent.captures_exited().unwrap();
        self.payer.as_mut().unwrap().finish().unwrap();
        self.ledger.reap_scope_ready(self.scope.id(), 16).unwrap();
        self.ledger.remove_scope(self.scope.id()).unwrap();
        assert!(self.ledger.is_empty());
    }
}
fn retire(partial: &mut SourceSlotIssuePartial, ledger: &RuntimeLedger) {
    for _ in 0..8 {
        if partial.try_retire(ledger).unwrap() {
            return;
        }
    }
    panic!("the original finite unused source headers must retire");
}
fn missing(error: &SourceSlotIssueError, kind: OwnedSlotKind) -> bool {
    matches!(error, SourceSlotIssueError::Runtime(RuntimeError::Capacity(
        CapacityError::OwnedStorage(OwnedStorageError::MissingPosition(actual))
    )) if *actual == kind)
}

#[test]
fn issuing_and_returning_whole_slots_keeps_each_original_work_until_its_true_reaping() {
    let mut f = Fixture::new(3, 2, 2);
    let before = f.scope.snapshot().unwrap().work;
    let slots = reserve_source_slots::<2>(
        &f.scope,
        &f.parent,
        (f.scope.id(), Generation::new(1).unwrap()),
        SourceSlotPayer::Retained(f.payer.as_mut().unwrap()),
    )
    .unwrap();
    for slot in &slots {
        assert_eq!(
            slot.reported().bound_storage_profile().unwrap(),
            SOURCE_SLOT_STORAGE.resources().unwrap().owned_slots
        );
    }
    let after_issue = f.scope.snapshot().unwrap().work;
    assert!(f.remaining().is_empty());
    let mut partial = SourceSlotIssuePartial::from_admissions(&f.scope, slots);
    let same_scope = partial.scope().same_scope(&f.scope);
    let original_headers = partial
        .slots
        .iter()
        .all(|slot| !slot.retirement.is_retired());
    let first = partial.try_retire(&f.ledger).unwrap();
    let remaining_header = partial
        .slots
        .iter()
        .filter(|slot| !slot.retirement.is_retired())
        .count();
    let still_live = f.scope.snapshot().unwrap().work;
    retire(&mut partial, &f.ledger);
    let after_retirement = f.scope.snapshot().unwrap().work;
    let idempotent = partial.try_retire(&f.ledger).unwrap()
        && f.scope.snapshot().unwrap().work == after_retirement;
    f.finish();
    assert_eq!(before, 1);
    assert_eq!(after_issue, 3);
    assert!(same_scope && original_headers && !first && idempotent);
    assert_eq!(
        remaining_header, 1,
        "one visit reaps only one actual header"
    );
    assert_eq!(still_live, 2);
    assert_eq!(after_retirement, 1, "only the actual parent remains");
}

#[test]
fn a_later_report_refusal_preserves_the_first_original_and_unsplit_payer_inventory() {
    let mut f = Fixture::new(3, 1, 2);
    let refused = reserve_source_slots::<2>(
        &f.scope,
        &f.parent,
        (f.scope.id(), Generation::new(1).unwrap()),
        SourceSlotPayer::Retained(f.payer.as_mut().unwrap()),
    )
    .unwrap_err();
    assert!(missing(&refused.error, OwnedSlotKind::Completion));
    let mut partial = refused.partial;
    assert_eq!(
        partial.slots.len(),
        1,
        "refused atomic admission creates no phantom Work"
    );
    assert_eq!(f.scope.snapshot().unwrap().work, 2);
    assert_eq!(
        f.scope.snapshot().unwrap().storage,
        1,
        "only the first actual report exists"
    );
    assert_eq!(
        f.remaining(),
        OwnedSlotNeed {
            work_headers: 1,
            captures: 1,
            ..OwnedSlotNeed::default()
        }
    );
    retire(&mut partial, &f.ledger);
    assert_eq!(f.scope.snapshot().unwrap().work, 1);
    assert_eq!(f.scope.snapshot().unwrap().storage, 0);
    f.finish();
}

#[test]
fn a_later_header_refusal_retains_only_the_actual_admitted_holder_without_fabricating_storage() {
    let mut f = Fixture::new(2, 2, 2);
    let refused = reserve_source_slots::<2>(
        &f.scope,
        &f.parent,
        (f.scope.id(), Generation::new(1).unwrap()),
        SourceSlotPayer::Retained(f.payer.as_mut().unwrap()),
    )
    .unwrap_err();
    assert!(missing(&refused.error, OwnedSlotKind::WorkHeader));
    let mut partial = refused.partial;
    assert_eq!(partial.slots.len(), 1);
    assert!(
        partial
            .slots
            .iter()
            .all(|slot| !slot.retirement.is_retired())
    );
    assert_eq!(f.scope.snapshot().unwrap().work, 2);
    let remaining = f.remaining();
    assert_eq!(
        remaining,
        OwnedSlotNeed {
            completions: 1,
            captures: 1,
            ..OwnedSlotNeed::default()
        }
    );
    retire(&mut partial, &f.ledger);
    assert_eq!(f.scope.snapshot().unwrap().work, 1);
    assert!(partial.try_retire(&f.ledger).unwrap());
    assert_eq!(f.remaining(), remaining);
    f.finish();
}

#[test]
fn a_pure_source_pool_moves_all_unused_positions_without_a_dummy_unit_or_extra_header() {
    let mut f = Fixture::new(3, 2, 2);
    let mut pool = Some(
        f.payer
            .as_mut()
            .unwrap()
            .split_next(
                &SOURCE_SLOT_STORAGE
                    .checked_mul(2)
                    .unwrap()
                    .resources()
                    .unwrap(),
            )
            .unwrap(),
    );
    let original_pool_id = pool.as_ref().unwrap().id();
    let slots = reserve_source_slots::<2>(
        &f.scope,
        &f.parent,
        (f.scope.id(), Generation::new(1).unwrap()),
        SourceSlotPayer::Pool(&mut pool),
    )
    .unwrap();
    assert!(pool.as_ref().unwrap().is_released());
    assert!(pool.as_ref().unwrap().resources().unwrap().is_empty());
    assert!(f.remaining().is_empty());
    assert_eq!(f.scope.snapshot().unwrap().work, 3);
    assert!(
        slots
            .iter()
            .all(|slot| slot.reported().work().scope().same_scope(&f.scope))
    );
    let mut partial = SourceSlotIssuePartial::from_admissions(&f.scope, slots);
    retire(&mut partial, &f.ledger);
    assert!(partial.try_retire(&f.ledger).unwrap());
    assert_eq!(f.scope.snapshot().unwrap().work, 1);
    assert!(pool.as_ref().unwrap().is_released());
    assert!(original_pool_id.get() > f.payer.as_ref().unwrap().id().get());
    f.finish();
}

#[test]
fn an_absent_source_pool_refuses_without_issuing_a_replacement_role_or_storage() {
    let f = Fixture::new(1, 0, 0);
    let mut pool = None;
    let before = f.scope.snapshot().unwrap();
    let refused = reserve_source_slots::<1>(
        &f.scope,
        &f.parent,
        (f.scope.id(), Generation::new(1).unwrap()),
        SourceSlotPayer::Pool(&mut pool),
    )
    .unwrap_err();
    assert!(matches!(
        refused.error,
        SourceSlotIssueError::StorageUnavailable
    ));
    let mut partial = refused.partial;
    assert!(partial.is_empty());
    assert_eq!(f.scope.snapshot().unwrap(), before);
    assert!(pool.is_none());
    assert!(partial.try_retire(&f.ledger).unwrap());
    f.finish();
}
