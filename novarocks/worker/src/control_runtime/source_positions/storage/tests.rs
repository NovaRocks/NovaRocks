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

//! Actual original ledger payment and reported Work; this primitive test does
//! not exercise the parent bank's detach or generation-replacement gates.

use super::*;
use crate::control_admission::Dimension;
use novarocks_control_runtime::{
    completion::ReportSlot,
    exit_role::{ExitRole, ExitRolePhase},
    lease::WorkLease,
    transaction::{RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::ResourceVector,
    completion::{ConsumerKind, Consumption},
    identity::{ConsumerId, Generation},
    scope::{WorkAdmission, WorkKind},
};

struct Fixture {
    ledger: RuntimeLedger,
    scope: ScopeHandle,
    payer: WorkLease,
    payment: CapacityBundle,
}
impl Fixture {
    fn new(units: usize) -> Self {
        let need = SOURCE_SLOT_STORAGE.checked_mul(units).unwrap();
        let ledger = RuntimeLedger::new(need.resources().unwrap(), 16, 1);
        let generation = Generation::new(1).unwrap();
        let scope = ledger.open_scope_next(None, generation, 8, 8).unwrap();
        let (mut payer, payment) = scope
            .reserve_next(
                generation,
                need.resources().unwrap(),
                WorkKind::Business,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Business)
            .unwrap();
        payer.start().unwrap();
        Self {
            ledger,
            scope,
            payer,
            payment,
        }
    }
    fn reported(&self) -> ReportedWork {
        let generation = self.scope.gates().unwrap().generation;
        let guard = self
            .scope
            .reserve_child_next(
                generation,
                self.payment.id(),
                ResourceVector::default(),
                WorkKind::Cleanup,
            )
            .unwrap();
        let report = ReportSlot::reserve_next(
            &self.scope,
            generation,
            guard.work_id(),
            2,
            [
                (
                    ConsumerId::new(1).unwrap(),
                    ConsumerKind::Owner {
                        scope: self.scope.id(),
                        generation,
                    },
                ),
                (ConsumerId::new(2).unwrap(), ConsumerKind::Observer),
            ],
        )
        .unwrap();
        let (work, zero) = guard.bind(WorkKind::Cleanup).unwrap();
        ReportedWork::try_new(work, zero, report).unwrap()
    }
    fn charges(&self) -> [u64; 3] {
        [Dimension::Work, Dimension::Completions, Dimension::Captures]
            .map(|dimension| self.ledger.used(dimension.id()))
    }
    fn finish(mut self) {
        self.scope.close_external().unwrap();
        self.payer.body_exited().unwrap();
        self.payer.captures_exited().unwrap();
        self.ledger.reap_scope_ready(self.scope.id(), 8).unwrap();
        if !self.payment.is_released() {
            self.payment
                .release(&self.payment.resources().unwrap())
                .unwrap();
        }
        self.ledger.remove_scope(self.scope.id()).unwrap();
        assert!(self.ledger.is_empty());
    }
}
fn unused(reported: ReportedWork) {
    let (role, authority) = ExitRole::from_admitted(reported);
    // No callback or capture was attached to this exact original role.
    authority.withdraw_unpublished_and_exit().unwrap();
    assert!(role.try_retire().unwrap());
}

#[test]
fn original_source_payment_survives_report_consumers_and_work_reaping_until_explicit_release() {
    let mut f = Fixture::new(2);
    let reported = f.reported();
    let report = reported.report().clone();
    let retirement = reported.work().retirement_observer();
    let before = f.charges();
    let payment = f
        .payment
        .split_next(&SOURCE_SLOT_STORAGE.resources().unwrap())
        .unwrap();
    let admission = SourceSlotAdmission::from_paid(reported, payment).unwrap();
    let after_admission = f.charges();
    let (reported, mut storage) = admission.into_slot_parts();
    let (role, mut authority) = ExitRole::from_admitted(reported);
    authority.start().unwrap();
    authority.exited().unwrap();
    report.consume(ConsumerId::new(1).unwrap()).unwrap();
    let observer_pending =
        report.consumption(ConsumerId::new(2).unwrap()).unwrap() == Consumption::Pending;
    let before_observer = role.try_retire().unwrap();
    report.consume(ConsumerId::new(2).unwrap()).unwrap();
    let role_retired = role.try_retire().unwrap() && role.phase() == ExitRolePhase::Retired;
    f.ledger.reap_scope_ready(f.scope.id(), 8).unwrap();
    let work_reaped = retirement.is_retired();
    let after_generation_exit = f.charges();
    // The test supplies only the storage primitive's explicit release. It
    // cannot claim that a bank detach/alias/foreign-destructor gate was proved.
    storage.release().unwrap();
    let after_release = f.charges();
    storage.release().unwrap();
    let idempotent = f.charges() == after_release && storage.into_payment().is_none();
    f.finish();

    assert_eq!(before, [2; 3]);
    assert_eq!(
        after_admission, before,
        "handoff acquires no additional credit"
    );
    assert!(observer_pending && !before_observer && role_retired && work_reaped);
    assert_eq!(
        after_generation_exit, before,
        "the zero-role exit cannot refund SourceSlot storage"
    );
    assert_eq!(after_release, [1; 3]);
    assert!(idempotent);
}

#[test]
fn invalid_storage_returns_both_original_owners_without_mutation() {
    for invalid in [
        StorageNeed {
            work: 1,
            ..StorageNeed::default()
        },
        SOURCE_SLOT_STORAGE.checked_mul(2).unwrap(),
    ] {
        let mut f = Fixture::new(3);
        let reported = f.reported();
        let work = reported.work().id();
        let report = reported.report().clone();
        let mut payment = f.payment.split_next(&invalid.resources().unwrap()).unwrap();
        let payment_id = payment.id();
        let before = f.charges();
        let refused = SourceSlotAdmission::from_paid(reported, payment).unwrap_err();
        let typed = matches!(refused.error, SourceSlotStorageError::InvalidStorage);
        let originals = refused.reported.work().id() == work
            && refused.reported.report().same_slot(&report)
            && refused.storage.id() == payment_id;
        let unchanged = f.charges() == before;
        payment = refused.storage;
        unused(refused.reported);
        f.ledger.reap_scope_ready(f.scope.id(), 8).unwrap();
        payment.release(&payment.resources().unwrap()).unwrap();
        f.finish();

        assert!(typed && originals && unchanged);
    }
}

#[test]
fn foreign_ledger_with_matching_numeric_identities_cannot_take_the_original_payment() {
    let mut payer = Fixture::new(2);
    let other = Fixture::new(2);
    let local_reported = payer.reported();
    let reported = other.reported();
    let same_numbers = local_reported.work().id() == reported.work().id()
        && local_reported.work().scope().id() == reported.work().scope().id();
    let work = reported.work().id();
    let report = reported.report().clone();
    let payment = payer
        .payment
        .split_next(&SOURCE_SLOT_STORAGE.resources().unwrap())
        .unwrap();
    let payment_id = payment.id();
    let before = payer.charges();
    let refused = SourceSlotAdmission::from_paid(reported, payment).unwrap_err();
    let typed = matches!(refused.error, SourceSlotStorageError::Runtime(_));
    let originals = refused.reported.work().id() == work
        && refused.reported.report().same_slot(&report)
        && refused.storage.id() == payment_id;
    let unchanged = payer.charges() == before && other.charges() == [2; 3];
    let mut payment = refused.storage;
    unused(refused.reported);
    unused(local_reported);
    payment.release(&payment.resources().unwrap()).unwrap();
    payer.finish();
    other.finish();

    assert!(same_numbers && typed && originals && unchanged);
}

#[test]
fn explicit_finite_protocol_model_has_no_production_payment_or_refund() {
    let f = Fixture::new(2);
    let before = f.charges();
    let (reported, mut storage) =
        SourceSlotAdmission::protocol_model(f.reported()).into_slot_parts();
    unused(reported);
    storage.release().unwrap();
    let unchanged = f.charges() == before && storage.into_payment().is_none();
    f.finish();
    assert!(
        unchanged,
        "an explicit unpaid model cannot demonstrate quota release"
    );
}

#[test]
fn unexposed_paid_admission_returns_the_same_reported_work_and_unique_payment_for_cleanup() {
    let mut f = Fixture::new(2);
    let reported = f.reported();
    let work = reported.work().id();
    let report = reported.report().clone();
    let payment = f
        .payment
        .split_next(&SOURCE_SLOT_STORAGE.resources().unwrap())
        .unwrap();
    let payment_id = payment.id();
    let admission = SourceSlotAdmission::from_paid(reported, payment).unwrap();
    let (reported, payment) = admission.into_originals();
    let mut payment = payment.expect("a production admission retains its actual payment");
    let originals = reported.work().id() == work
        && reported.report().same_slot(&report)
        && payment.id() == payment_id;
    unused(reported);
    f.ledger.reap_scope_ready(f.scope.id(), 8).unwrap();
    let still_paid = f.charges() == [2; 3];
    payment.release(&payment.resources().unwrap()).unwrap();
    let refunded = f.charges() == [1; 3];
    f.finish();
    assert!(originals && still_paid && refunded);
}
