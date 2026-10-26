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
    exit_role::{ExitAuthority, ExitRole, ExitRoleError, ExitRolePhase},
    transaction::{RuntimeError, RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::ResourceVector,
    completion::{CompletionError, ConsumerKind},
    identity::{ConsumerId, DimensionId, Generation, WorkId},
    scope::{WorkAdmission, WorkKind},
};
fn generation() -> Generation {
    Generation::new(1).unwrap()
}
fn dimension() -> DimensionId {
    DimensionId::new(1).unwrap()
}
fn resources(amount: u64) -> ResourceVector {
    ResourceVector::new([(dimension(), amount)]).unwrap()
}
struct Fixture {
    ledger: RuntimeLedger,
    scope: ScopeHandle,
    report: ReportSlot,
    role: ExitRole,
    authority: Option<ExitAuthority>,
    owner: ConsumerId,
    observer: ConsumerId,
    work: WorkId,
}
fn fixture() -> Fixture {
    let ledger = RuntimeLedger::new(resources(1), 1, 1);
    let scope = ledger.open_scope_next(None, generation(), 1, 1).unwrap();
    let (work, capacity) = scope
        .reserve_next(
            generation(),
            resources(1),
            WorkKind::Cleanup,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Cleanup)
        .unwrap();
    let owner = ConsumerId::new(1).unwrap();
    let observer = ConsumerId::new(2).unwrap();
    let report = ReportSlot::reserve_next(
        &scope,
        generation(),
        work.id(),
        2,
        [
            (
                owner,
                ConsumerKind::Owner {
                    scope: scope.id(),
                    generation: generation(),
                },
            ),
            (observer, ConsumerKind::Observer),
        ],
    )
    .unwrap();
    let id = work.id();
    let (role, authority) = ExitRole::from_admitted(work, capacity, report.clone());
    Fixture {
        ledger,
        scope,
        report,
        role,
        authority: Some(authority),
        owner,
        observer,
        work: id,
    }
}
fn retire(f: &Fixture) {
    f.report.consume(f.owner).unwrap();
    assert!(
        !f.role.try_retire().unwrap(),
        "the observer retains its independent leg"
    );
    assert_eq!(f.ledger.used(dimension()), 1);
    f.report.consume(f.observer).unwrap();
    assert!(f.role.try_retire().unwrap());
    assert_eq!(f.role.phase(), ExitRolePhase::Retired);
    assert!(
        !f.role.try_retire().unwrap(),
        "retirement returns capacity only once"
    );
    assert_eq!(f.ledger.used(dimension()), 0);
    assert_eq!(f.ledger.reap_scope_ready(f.scope.id(), 1).unwrap(), 1);
    f.scope.close_external().unwrap();
    f.ledger.remove_scope(f.scope.id()).unwrap();
    assert!(f.ledger.is_empty());
}

#[test]
fn the_original_unused_holder_supplies_positive_exit_without_starting_a_body() {
    let mut f = fixture();
    assert_eq!(f.role.phase(), ExitRolePhase::Admitted);
    assert!(!f.role.body_exited());
    assert!(!f.role.captures_exited());
    assert!(!f.role.try_retire().unwrap());
    f.authority.take().unwrap().exited().unwrap();
    assert_eq!(f.role.phase(), ExitRolePhase::Exited);
    assert!(f.role.body_exited());
    assert!(f.role.captures_exited());
    assert_eq!(f.ledger.used(dimension()), 1);
    retire(&f);
}

#[test]
fn original_authority_starts_once_and_waits_for_actual_capture_exit() {
    let mut f = fixture();
    let authority = f.authority.as_mut().unwrap();
    authority.start().unwrap();
    assert_eq!(
        authority.start(),
        Err(ExitRoleError::InvalidPhase(ExitRolePhase::Started))
    );
    assert_eq!(f.role.phase(), ExitRolePhase::Started);
    assert!(!f.role.try_retire().unwrap());
    assert!(
        f.report.consume(f.owner).is_err(),
        "start is not an exit report"
    );
    f.authority.take().unwrap().exited().unwrap();
    retire(&f);
}

#[test]
fn authority_drop_records_unresolved_and_retains_original_custody_after_all_handles_vanish() {
    for started in [false, true] {
        let mut f = fixture();
        if started {
            f.authority.as_mut().unwrap().start().unwrap();
        }
        drop(f.authority.take());
        assert_eq!(f.role.phase(), ExitRolePhase::Unresolved);
        assert!(!f.role.body_exited());
        assert!(!f.role.captures_exited());
        assert!(!f.role.try_retire().unwrap());
        assert!(f.report.consume(f.owner).is_err());
        assert_eq!(f.ledger.unresolved_work(), vec![(f.scope.id(), f.work)]);
        let ledger = f.ledger.clone();
        let scope = f.scope.clone();
        drop(f);
        assert_eq!(ledger.used(dimension()), 1);
        assert_eq!(scope.snapshot().unwrap().work, 1);
        assert_eq!(scope.snapshot().unwrap().storage, 1);
        assert_eq!(ledger.unresolved_work().len(), 1);
        scope.close_external().unwrap();
        assert!(!scope.snapshot().unwrap().join_ready);
    }
}

#[test]
fn original_cleanup_exit_after_scope_failure_preserves_the_observers_consumption_leg() {
    let mut f = fixture();
    f.scope.fail().unwrap();
    f.report
        .abandon_failed_owner(f.owner, &f.scope, generation())
        .unwrap();
    assert!(
        f.report
            .abandon_failed_owner(f.observer, &f.scope, generation())
            .is_err()
    );
    assert!(!f.role.try_retire().unwrap());
    f.authority.as_mut().unwrap().start().unwrap();
    f.authority.take().unwrap().exited().unwrap();
    assert_eq!(f.role.phase(), ExitRolePhase::Exited);
    assert!(
        !f.role.try_retire().unwrap(),
        "failure cannot abandon another consumer"
    );
    assert_eq!(f.ledger.used(dimension()), 1);
    f.report.consume(f.observer).unwrap();
    assert!(f.role.try_retire().unwrap());
    f.ledger.reap_scope_ready(f.scope.id(), 1).unwrap();
    f.ledger.remove_scope(f.scope.id()).unwrap();
    assert!(f.ledger.is_empty());
}

#[test]
fn body_and_report_exit_cannot_release_capacity_while_a_derived_child_is_live() {
    for reaping_budget in [1, 2] {
        let ledger = RuntimeLedger::new(resources(2), 4, 2);
        let scope = ledger.open_scope_next(None, generation(), 2, 1).unwrap();
        let contender = ledger.open_scope_next(None, generation(), 1, 1).unwrap();
        let (parent, capacity) = scope
            .reserve_next(
                generation(),
                resources(1),
                WorkKind::Cleanup,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Cleanup)
            .unwrap();
        let (mut child, mut child_capacity) = scope
            .reserve_next(
                generation(),
                resources(1),
                WorkKind::Cleanup,
                WorkAdmission::Derived(parent.id()),
            )
            .unwrap()
            .bind(WorkKind::Cleanup)
            .unwrap();
        child.start().unwrap();
        let consumer = ConsumerId::new(1).unwrap();
        let report = ReportSlot::reserve_next(
            &scope,
            generation(),
            parent.id(),
            1,
            [(consumer, ConsumerKind::Observer)],
        )
        .unwrap();
        let (role, authority) = ExitRole::from_admitted(parent, capacity, report.clone());
        authority.exited().unwrap();
        report.consume(consumer).unwrap();
        assert!(!role.try_retire().unwrap());
        assert_eq!(ledger.used(dimension()), 2);
        assert!(
            contender
                .reserve_next(
                    generation(),
                    resources(1),
                    WorkKind::Business,
                    WorkAdmission::External
                )
                .is_err()
        );
        let version = ledger.change_version();
        child.body_exited().unwrap();
        child.captures_exited().unwrap();
        assert!(
            ledger.change_version() > version,
            "child progress wakes the original finisher"
        );
        child_capacity.release(&resources(1)).unwrap();
        assert_eq!(ledger.used(dimension()), 1);
        assert!(
            !role.try_retire().unwrap(),
            "the child record still retains its parent"
        );
        assert_eq!(
            ledger.reap_scope_ready(scope.id(), reaping_budget).unwrap(),
            reaping_budget
        );
        assert!(
            role.try_retire().unwrap(),
            "works even if the pure parent record was already reaped"
        );
        assert_eq!(ledger.used(dimension()), 0);
        assert_eq!(
            ledger.reap_scope_ready(scope.id(), 1).unwrap(),
            2 - reaping_budget
        );
        let reusable = contender
            .reserve_next(
                generation(),
                resources(1),
                WorkKind::Business,
                WorkAdmission::External,
            )
            .unwrap();
        drop(reusable);
        scope.close_external().unwrap();
        contender.close_external().unwrap();
        ledger.remove_scope(scope.id()).unwrap();
        ledger.remove_scope(contender.id()).unwrap();
        assert!(ledger.is_empty());
    }
}

#[test]
fn a_partial_exit_observation_failure_keeps_the_real_lease_report_and_capacity_unresolved() {
    let mut f = fixture();
    // Fault injection: violating the reserved-only report contract must not
    // turn a partially accepted body observation into capture exit or a refund.
    f.report.fill().unwrap();
    let result = f.authority.take().unwrap().exited();
    assert_eq!(
        result,
        Err(ExitRoleError::Runtime(RuntimeError::Completion(
            CompletionError::AlreadyFilled
        )))
    );
    assert_eq!(f.role.phase(), ExitRolePhase::Unresolved);
    assert!(f.role.body_exited());
    assert!(!f.role.captures_exited());
    assert!(!f.role.try_retire().unwrap());
    assert_eq!(f.ledger.used(dimension()), 1);
    assert_eq!(f.ledger.unresolved_work(), vec![(f.scope.id(), f.work)]);
    assert_eq!(f.scope.snapshot().unwrap().work, 1);
    assert_eq!(f.scope.snapshot().unwrap().storage, 1);
}
