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

//! Real original payment and unused Source Work cleanup, not bank detach,
//! producer disposal, Native execution or complete failed-owner recovery.

use super::*;
use crate::control_admission::Dimension;
use novarocks_control_runtime::lease::WorkLease;
use novarocks_control_runtime_core::scope::WorkAdmission;

struct Fixture {
    ledger: RuntimeLedger,
    scope: ScopeHandle,
    parent: WorkLease,
    payer: Option<CapacityBundle>,
}
impl Fixture {
    fn new(units: usize, reports: usize) -> Self {
        let resources = SOURCE_SLOT_STORAGE
            .checked_mul(units)
            .unwrap()
            .resources()
            .unwrap();
        let ledger = RuntimeLedger::new(resources.clone(), 32, 1);
        let generation = Generation::new(1).unwrap();
        let scope = ledger
            .open_scope_next(None, generation, 16, reports)
            .unwrap();
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
    fn charges(&self) -> [u64; 3] {
        [Dimension::Work, Dimension::Completions, Dimension::Captures]
            .map(|dimension| self.ledger.used(dimension.id()))
    }
    fn finish(mut self) {
        self.scope.close_external().unwrap();
        self.parent.body_exited().unwrap();
        self.parent.captures_exited().unwrap();
        self.ledger.reap_scope_ready(self.scope.id(), 16).unwrap();
        if let Some(payer) = &mut self.payer
            && !payer.is_released()
        {
            let remaining = payer.resources().unwrap();
            payer.release(&remaining).unwrap();
        }
        self.ledger.remove_scope(self.scope.id()).unwrap();
        assert!(self.ledger.is_empty());
    }
}
fn retire(partial: &mut SourceSlotIssuePartial, ledger: &RuntimeLedger) {
    // The fixture admits at most three source holders, plus this same parent.
    for _ in 0..8 {
        if partial.try_retire(ledger).unwrap() {
            return;
        }
    }
    panic!("the original finite unused source headers must retire");
}

#[test]
fn issuing_and_returning_whole_slots_moves_one_payment_until_each_original_work_reaps() {
    let mut f = Fixture::new(3, 8);
    let before = f.charges();
    let parent = f.payer.as_ref().unwrap().id();
    let slots = reserve_source_slots::<2>(
        &f.scope,
        parent,
        Generation::new(1).unwrap(),
        SourceSlotPayer::Retained(f.payer.as_mut().unwrap()),
    )
    .unwrap();
    let after_issue = f.charges();
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
    let still_paid = f.charges();
    retire(&mut partial, &f.ledger);
    let after_retirement = f.charges();
    let idempotent = partial.try_retire(&f.ledger).unwrap() && f.charges() == after_retirement;
    f.finish();
    assert_eq!(before, [3; 3]);
    assert_eq!(
        after_issue, before,
        "slot admission moves already paid units without acquiring more capacity"
    );
    assert!(same_scope && original_headers && !first && idempotent);
    assert_eq!(remaining_header, 1, "one call reaps only one actual header");
    assert_eq!(
        still_paid, [2; 3],
        "the other admitted slot remains paid until its own Work reaps"
    );
    assert_eq!(
        after_retirement, [1; 3],
        "only the two original slot fees were returned"
    );
}

#[test]
fn a_later_report_refusal_preserves_the_first_paid_slot_and_the_unsplit_payer() {
    let mut f = Fixture::new(3, 1);
    let parent = f.payer.as_ref().unwrap().id();
    let refused = reserve_source_slots::<2>(
        &f.scope,
        parent,
        Generation::new(1).unwrap(),
        SourceSlotPayer::Retained(f.payer.as_mut().unwrap()),
    )
    .unwrap_err();
    let runtime_refusal = matches!(refused.error, SourceSlotIssueError::Runtime(_));
    let mut partial = refused.partial;
    let paid_originals = partial
        .slots
        .iter()
        .filter(|slot| slot.storage.is_some())
        .count();
    let before_cleanup = f.charges();
    let parent_remaining = f.payer.as_ref().unwrap().resources().unwrap();
    retire(&mut partial, &f.ledger);
    let after_cleanup = f.charges();
    f.finish();
    assert!(runtime_refusal);
    assert_eq!(paid_originals, 1);
    assert_eq!(
        parent_remaining,
        SOURCE_SLOT_STORAGE
            .checked_mul(2)
            .unwrap()
            .resources()
            .unwrap()
    );
    assert_eq!(before_cleanup, [3; 3]);
    assert_eq!(after_cleanup, [2; 3]);
}

#[test]
fn a_later_split_refusal_keeps_the_last_unpaid_reported_holder_without_fabricating_credit() {
    let mut f = Fixture::new(2, 8);
    let parent = f.payer.as_ref().unwrap().id();
    let refused = reserve_source_slots::<2>(
        &f.scope,
        parent,
        Generation::new(1).unwrap(),
        SourceSlotPayer::Retained(f.payer.as_mut().unwrap()),
    )
    .unwrap_err();
    let runtime_refusal = matches!(refused.error, SourceSlotIssueError::Runtime(_));
    let mut partial = refused.partial;
    let holders = partial.slots.len();
    let payments = partial
        .slots
        .iter()
        .filter(|slot| slot.storage.is_some())
        .count();
    let all_original = partial
        .slots
        .iter()
        .all(|slot| !slot.retirement.is_retired());
    let before_cleanup = f.charges();
    let parent_remaining = f.payer.as_ref().unwrap().resources().unwrap();
    retire(&mut partial, &f.ledger);
    let after_cleanup = f.charges();
    let idempotent = partial.try_retire(&f.ledger).unwrap() && f.charges() == after_cleanup;
    f.finish();
    assert!(runtime_refusal && all_original && idempotent);
    assert_eq!((holders, payments), (2, 1));
    assert_eq!(before_cleanup, [2; 3]);
    assert_eq!(parent_remaining, SOURCE_SLOT_STORAGE.resources().unwrap());
    assert_eq!(after_cleanup, [1; 3]);
}

#[test]
fn a_pure_source_pool_moves_its_last_whole_bundle_without_a_dummy_unit_or_extra_header() {
    let mut f = Fixture::new(2, 8);
    let parent = f.payer.as_ref().unwrap().id();
    let mut pool = f.payer.take();
    let original_pool_id = pool.as_ref().unwrap().id();
    let before = f.charges();
    let slots = reserve_source_slots::<2>(
        &f.scope,
        parent,
        Generation::new(1).unwrap(),
        SourceSlotPayer::Pool(&mut pool),
    )
    .unwrap();
    let exhausted_normally = pool.is_none();
    let after_issue = f.charges();
    let mut partial = SourceSlotIssuePartial::from_admissions(&f.scope, slots);
    let last_is_same_bundle = partial.slots[1].storage.as_ref().unwrap().id() == original_pool_id;
    retire(&mut partial, &f.ledger);
    let after_cleanup = f.charges();
    let idempotent = partial.try_retire(&f.ledger).unwrap() && f.charges() == after_cleanup;
    f.finish();
    assert!(exhausted_normally && last_is_same_bundle && idempotent);
    assert_eq!(before, [2; 3]);
    assert_eq!(after_issue, before);
    assert_eq!(after_cleanup, [0; 3]);
}

#[test]
fn an_absent_source_pool_refuses_without_issuing_a_replacement_role_or_payment() {
    let f = Fixture::new(1, 8);
    let parent = f.payer.as_ref().unwrap().id();
    let mut pool = None;
    let before = f.scope.snapshot().unwrap();
    let refused = reserve_source_slots::<1>(
        &f.scope,
        parent,
        Generation::new(1).unwrap(),
        SourceSlotPayer::Pool(&mut pool),
    )
    .unwrap_err();
    let unavailable = matches!(refused.error, SourceSlotIssueError::StorageUnavailable);
    let mut partial = refused.partial;
    let no_originals = partial.is_empty();
    let unchanged = f.scope.snapshot().unwrap() == before && pool.is_none();
    let retired = partial.try_retire(&f.ledger).unwrap();
    f.finish();
    assert!(unavailable && no_originals && unchanged && retired);
}
