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

//! Actual Source roles bind their own Header/report/capture from one parent.

use super::*;
use novarocks_control_runtime::{
    exit_role::{ExitRole, ExitRolePhase},
    lease::{ReservationBundle, WorkLease},
    transaction::{RoleSpec, RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    completion::{ConsumerKind, Consumption},
    identity::{ConsumerId, Generation},
    owned_storage::OwnedSlotNeed,
    scope::{WorkAdmission, WorkKind},
};

struct Fixture {
    ledger: RuntimeLedger,
    scope: ScopeHandle,
    payer: WorkLease,
    payment: ReservationBundle,
}
impl Fixture {
    fn new(units: usize) -> Self {
        let generation = Generation::new(1).unwrap();
        let ledger = RuntimeLedger::new(ResourceVector::default());
        let scope = ledger.open_scope_next(None, generation).unwrap();
        let mut resources = SOURCE_SLOT_STORAGE
            .checked_mul(units)
            .unwrap()
            .resources()
            .unwrap();
        resources.owned_slots.work_headers += 1; // The actual parent has its own header.
        let (mut payer, payment) = scope
            .reserve_next(
                generation,
                resources,
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
    fn reported(&mut self, captures: usize) -> ReportedWork {
        let generation = self.scope.gates().unwrap().generation;
        self.scope
            .reserve_child_from(
                &mut self.payment,
                &self.payer,
                RoleSpec {
                    generation,
                    kind: WorkKind::Cleanup,
                    resources: ReservationResources::new(
                        ResourceVector::default(),
                        OwnedSlotNeed {
                            work_headers: 1,
                            completions: 1,
                            captures,
                            ..OwnedSlotNeed::default()
                        },
                    ),
                    consumers: vec![
                        (
                            ConsumerId::new(1).unwrap(),
                            ConsumerKind::Owner {
                                scope: self.scope.id(),
                                generation,
                            },
                        ),
                        (ConsumerId::new(2).unwrap(), ConsumerKind::Observer),
                    ],
                },
            )
            .unwrap()
    }
    fn finish(mut self) {
        self.scope.close_external().unwrap();
        self.payer.body_exited().unwrap();
        self.payer.captures_exited().unwrap();
        self.payment.finish().unwrap();
        self.ledger.reap_scope_ready(self.scope.id(), 8).unwrap();
        self.ledger.remove_scope(self.scope.id()).unwrap();
        assert!(self.ledger.is_empty());
    }
}
fn unused(reported: ReportedWork) {
    let (role, authority) = ExitRole::from_admitted(reported);
    authority.withdraw_unpublished_and_exit().unwrap();
    assert!(role.try_retire().unwrap());
}

#[test]
fn original_source_storage_follows_actual_report_consumers_and_work_reaping() {
    let mut f = Fixture::new(2);
    let reported = f.reported(1);
    let report = reported.report().clone();
    let retirement = reported.work().retirement_observer();
    let original = reported.work().reference();
    let remaining = f.payment.resources().unwrap();
    let admission = SourceSlotAdmission::from_admitted(reported).unwrap();
    assert_eq!(
        f.payment.resources().unwrap(),
        remaining,
        "handoff does not allocate a second payment"
    );
    let reported = admission.into_reported();
    assert_eq!(reported.work().reference(), original);
    let (role, mut authority) = ExitRole::from_admitted(reported);
    authority.start().unwrap();
    authority.exited().unwrap();
    report.consume(ConsumerId::new(1).unwrap()).unwrap();
    assert_eq!(
        report.consumption(ConsumerId::new(2).unwrap()).unwrap(),
        Consumption::Pending
    );
    assert!(!role.try_retire().unwrap());
    assert!(!retirement.is_retired());
    report.consume(ConsumerId::new(2).unwrap()).unwrap();
    assert!(role.try_retire().unwrap());
    assert_eq!(role.phase(), ExitRolePhase::Retired);
    f.scope.reap_ready(8).unwrap();
    assert!(retirement.is_retired());
    assert_eq!(
        f.payment.resources().unwrap(),
        remaining,
        "actual bound source storage is never refunded to unused parent inventory"
    );
    f.finish();
}

#[test]
fn invalid_storage_returns_the_original_reported_owner_without_mutation() {
    for captures in [0, 2] {
        let mut f = Fixture::new(3);
        let reported = f.reported(captures);
        let original = reported.work().reference();
        let report = reported.report().clone();
        let before = f.scope.snapshot().unwrap();
        let unused_storage = f.payment.resources().unwrap();
        let refused = SourceSlotAdmission::from_admitted(reported).unwrap_err();
        assert!(matches!(
            refused.error,
            SourceSlotStorageError::InvalidStorage
        ));
        assert_eq!(refused.reported.work().reference(), original);
        assert!(refused.reported.report().same_slot(&report));
        assert_eq!(f.scope.snapshot().unwrap(), before);
        assert_eq!(f.payment.resources().unwrap(), unused_storage);
        unused(refused.reported);
        f.scope.reap_ready(8).unwrap();
        f.finish();
    }
}

#[test]
fn successful_admission_returns_the_same_unique_reported_owner() {
    let mut f = Fixture::new(2);
    let reported = f.reported(1);
    let original = reported.work().reference();
    let report = reported.report().clone();
    let before = f.scope.snapshot().unwrap();
    let admission = SourceSlotAdmission::from_admitted(reported).unwrap();
    let reported = admission.into_reported();
    assert_eq!(reported.work().reference(), original);
    assert!(reported.report().same_slot(&report));
    assert_eq!(f.scope.snapshot().unwrap(), before);
    assert_eq!(
        reported.bound_storage_profile().unwrap(),
        SOURCE_SLOT_STORAGE.resources().unwrap().owned_slots
    );
    unused(reported);
    f.scope.reap_ready(8).unwrap();
    f.finish();
}
