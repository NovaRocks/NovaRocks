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
    history::HistoryCapture,
    lease::WorkRetirementObserver,
    transaction::{RuntimeError, RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::ResourceVector,
    completion::{CompletionError, ConsumerKind, Consumption},
    history::HistoryFact,
    identity::{ConsumerId, DimensionId, Generation, WorkId},
    scope::{ScopeError, WorkAdmission, WorkKind},
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
    retirement: WorkRetirementObserver,
    history: HistoryCapture,
}
fn fixture() -> Fixture {
    fixture_with_preobserved_body(false)
}
fn fixture_with_preobserved_body(body_exited: bool) -> Fixture {
    fixture_with_kind_and_preobserved_body(WorkKind::Cleanup, body_exited)
}
fn fixture_with_kind_and_preobserved_body(kind: WorkKind, body_exited: bool) -> Fixture {
    let history = HistoryCapture::new(64);
    let ledger = RuntimeLedger::with_history(resources(1), 1, 1, history.clone());
    let scope = ledger.open_scope_next(None, generation(), 1, 1).unwrap();
    let (mut work, capacity) = scope
        .reserve_next(generation(), resources(1), kind, WorkAdmission::External)
        .unwrap()
        .bind(kind)
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
    let retirement = work.retirement_observer();
    if body_exited {
        work.body_exited().unwrap();
    }
    let (role, authority) = ExitRole::from_admitted(
        novarocks_control_runtime::reported_work::ReportedWork::try_new(
            work,
            capacity,
            report.clone(),
        )
        .unwrap(),
    );
    Fixture {
        ledger,
        scope,
        report,
        role,
        authority: Some(authority),
        owner,
        observer,
        work: id,
        retirement,
        history,
    }
}
#[test]
fn a_reusable_position_waits_for_the_actual_reaper_after_role_and_report_retirement() {
    let mut f = fixture();
    assert!(!f.retirement.is_retired());
    f.authority.take().unwrap().exited().unwrap();
    f.report.consume(f.owner).unwrap();
    f.report.consume(f.observer).unwrap();
    assert!(f.role.try_retire().unwrap());
    assert_eq!(f.role.phase(), ExitRolePhase::Retired);
    assert_eq!(f.ledger.used(dimension()), 0);
    assert!(
        !f.retirement.is_retired(),
        "returning capacity and report storage does not remove its Work header"
    );
    assert_eq!(f.ledger.reap_scope_ready(f.scope.id(), 0).unwrap(), 0);
    assert!(!f.retirement.is_retired());
    assert_eq!(f.ledger.reap_scope_ready(f.scope.id(), 1).unwrap(), 1);
    assert!(f.retirement.is_retired());
    f.scope.close_external().unwrap();
    f.ledger.remove_scope(f.scope.id()).unwrap();
    assert!(
        f.retirement.is_retired(),
        "the original bound identity remains retired after scope removal"
    );
    assert!(f.ledger.is_empty());
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

fn retire_revoked(f: &Fixture) {
    assert_eq!(f.role.phase(), ExitRolePhase::Exited);
    assert!(f.role.body_exited());
    assert!(f.role.captures_exited());
    assert!(f.role.actual_exit_ready().unwrap());
    for consumer in [f.owner, f.observer] {
        assert_eq!(
            f.report.consume(consumer),
            Err(RuntimeError::Completion(CompletionError::NotFilled)),
            "a revoked unused report was never published to either consumer"
        );
    }
    assert!(f.role.try_retire().unwrap());
    assert_eq!(f.ledger.used(dimension()), 0);
    assert!(!f.retirement.is_retired());
    assert_eq!(f.ledger.reap_scope_ready(f.scope.id(), 1).unwrap(), 1);
    assert!(f.retirement.is_retired());
    f.scope.close_external().unwrap();
    f.ledger.remove_scope(f.scope.id()).unwrap();
    assert!(f.ledger.is_empty());
    let history = f.history.drain(64);
    assert!(history.complete);
    assert_eq!(history.remaining, 0);
    assert!(history.entries.iter().any(|entry| matches!(
        entry.fact,
        HistoryFact::BodyExited { work, .. } if work == f.work
    )));
    assert!(history.entries.iter().any(|entry| matches!(
        entry.fact,
        HistoryFact::CapturesExited { work, .. } if work == f.work
    )));
    assert!(history.entries.iter().all(|entry| !matches!(
        entry.fact,
        HistoryFact::Started { .. }
            | HistoryFact::SlotFilled { .. }
            | HistoryFact::CompletionConsumed { .. }
            | HistoryFact::SlotAbandoned { .. }
    )));
}

#[test]
fn unused_revocation_retires_the_original_report_without_fictitious_consumers() {
    let mut f = fixture();
    f.authority
        .take()
        .unwrap()
        .revoke_unused_and_exit()
        .unwrap();
    retire_revoked(&f);
}

#[test]
fn scope_failure_does_not_prevent_real_unused_holder_revocation_and_exit() {
    let mut f = fixture();
    f.scope.fail().unwrap();
    f.authority
        .take()
        .unwrap()
        .revoke_unused_and_exit()
        .unwrap();
    retire_revoked(&f);
}

#[test]
fn started_authority_cannot_revoke_its_original_reserved_report_as_unused() {
    let mut f = fixture();
    f.authority.as_mut().unwrap().start().unwrap();
    assert_eq!(
        f.authority.take().unwrap().revoke_unused_and_exit(),
        Err(ExitRoleError::InvalidPhase(ExitRolePhase::Started))
    );
    // Consuming a refused authority does not replace it or prove its exit.
    assert_eq!(f.role.phase(), ExitRolePhase::Unresolved);
    assert!(!f.role.body_exited());
    assert!(!f.role.captures_exited());
    assert!(!f.role.actual_exit_ready().unwrap());
    assert!(!f.role.try_retire().unwrap());
    assert_eq!(f.ledger.used(dimension()), 1);
    assert_eq!(f.ledger.unresolved_work(), vec![(f.scope.id(), f.work)]);
    // Reserved is preserved on rejection, rather than silently revoked.
    f.report.fill().unwrap();
}

#[test]
fn published_or_already_revoked_reports_refuse_unused_exit_and_keep_all_custody() {
    for filled in [true, false] {
        let mut f = fixture();
        if filled {
            f.report.fill().unwrap();
        } else {
            f.report.revoke_unused().unwrap();
        }
        assert_eq!(
            f.authority.take().unwrap().revoke_unused_and_exit(),
            Err(ExitRoleError::Runtime(RuntimeError::Completion(
                CompletionError::InvalidRevocation
            )))
        );
        assert_eq!(f.role.phase(), ExitRolePhase::Unresolved);
        assert!(!f.role.body_exited());
        assert!(!f.role.captures_exited());
        assert!(!f.role.actual_exit_ready().unwrap());
        assert!(!f.role.try_retire().unwrap());
        assert_eq!(f.ledger.used(dimension()), 1);
        assert_eq!(f.ledger.unresolved_work(), vec![(f.scope.id(), f.work)]);
        assert_eq!(f.scope.snapshot().unwrap().work, 1);
        assert_eq!(f.scope.snapshot().unwrap().storage, 1);
        assert_eq!(f.ledger.reap_scope_ready(f.scope.id(), 1).unwrap(), 0);
    }
}

#[test]
fn error_after_unused_report_revocation_retains_unknown_work_report_and_capacity() {
    // Fault injection: the admission owner violated from_admitted's original
    // role contract by supplying Work whose body observation already occurred.
    let mut f = fixture_with_preobserved_body(true);
    assert_eq!(
        f.authority.take().unwrap().revoke_unused_and_exit(),
        Err(ExitRoleError::Runtime(RuntimeError::Scope(
            novarocks_control_runtime_core::scope::ScopeError::InvalidTransition
        )))
    );
    assert_eq!(f.role.phase(), ExitRolePhase::Unresolved);
    assert!(!f.role.body_exited());
    assert!(!f.role.captures_exited());
    assert!(!f.role.actual_exit_ready().unwrap());
    assert!(!f.role.try_retire().unwrap());
    assert_eq!(f.ledger.used(dimension()), 1);
    assert_eq!(f.ledger.unresolved_work(), vec![(f.scope.id(), f.work)]);
    assert_eq!(f.scope.snapshot().unwrap().work, 1);
    assert_eq!(f.scope.snapshot().unwrap().storage, 1);
    assert!(!f.report.try_retire().unwrap());
    assert_eq!(
        f.report.fill(),
        Err(RuntimeError::Completion(CompletionError::AlreadyFilled)),
        "a partially revoked report cannot be refilled or rolled back"
    );
    assert_eq!(f.ledger.reap_scope_ready(f.scope.id(), 1).unwrap(), 0);
}

#[test]
fn unused_revocation_still_waits_for_real_derived_child_exit_and_reaping() {
    for reaping_budget in [1, 2] {
        let ledger = RuntimeLedger::new(resources(2), 2, 1);
        let scope = ledger.open_scope_next(None, generation(), 2, 1).unwrap();
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
        let retirement = parent.retirement_observer();
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
        let report = ReportSlot::reserve_next(
            &scope,
            generation(),
            parent.id(),
            2,
            [
                (ConsumerId::new(1).unwrap(), ConsumerKind::Observer),
                (ConsumerId::new(2).unwrap(), ConsumerKind::Observer),
            ],
        )
        .unwrap();
        let (role, authority) = ExitRole::from_admitted(
            novarocks_control_runtime::reported_work::ReportedWork::try_new(
                parent, capacity, report,
            )
            .unwrap(),
        );
        authority.revoke_unused_and_exit().unwrap();
        assert!(!role.actual_exit_ready().unwrap());
        assert!(!role.try_retire().unwrap());
        assert_eq!(ledger.used(dimension()), 2);
        child.body_exited().unwrap();
        child.captures_exited().unwrap();
        child_capacity.release(&resources(1)).unwrap();
        assert!(!role.actual_exit_ready().unwrap());
        assert!(!role.try_retire().unwrap());
        assert_eq!(ledger.used(dimension()), 1);
        assert_eq!(
            ledger.reap_scope_ready(scope.id(), reaping_budget).unwrap(),
            reaping_budget
        );
        assert!(role.actual_exit_ready().unwrap());
        assert!(role.try_retire().unwrap());
        assert_eq!(ledger.used(dimension()), 0);
        assert_eq!(
            ledger.reap_scope_ready(scope.id(), 1).unwrap(),
            2 - reaping_budget
        );
        assert!(retirement.is_retired());
        scope.close_external().unwrap();
        ledger.remove_scope(scope.id()).unwrap();
        assert!(ledger.is_empty());
    }
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
        let (role, authority) = ExitRole::from_admitted(
            novarocks_control_runtime::reported_work::ReportedWork::try_new(
                parent,
                capacity,
                report.clone(),
            )
            .unwrap(),
        );
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

#[test]
fn an_old_generation_cannot_start_cleanup_after_failure_or_release_its_original_custody() {
    let mut f = fixture();
    let failed_generation = f.scope.fail().unwrap();
    assert!(failed_generation > generation());
    assert_eq!(
        f.authority
            .as_mut()
            .unwrap()
            .start_in_generation(generation()),
        Err(ExitRoleError::Runtime(RuntimeError::Scope(
            ScopeError::GenerationMismatch
        )))
    );
    assert_eq!(f.role.phase(), ExitRolePhase::Admitted);
    assert!(!f.role.body_exited() && !f.role.captures_exited());
    assert!(!f.role.actual_exit_ready().unwrap());
    assert!(!f.role.try_retire().unwrap());
    assert_eq!(f.ledger.used(dimension()), 1);
    assert_eq!(f.report.consumption(f.owner).unwrap(), Consumption::Pending);
    assert_eq!(
        f.report.consumption(f.observer).unwrap(),
        Consumption::Pending
    );
    // The exact never-run original may now withdraw. Failure supplied no exit,
    // report consumption or refund; the authority supplies the real unused exit.
    f.authority
        .take()
        .unwrap()
        .revoke_unused_and_exit()
        .unwrap();
    retire_revoked(&f);
}

#[test]
fn generation_guarded_start_obeys_business_stop_while_current_cleanup_still_runs() {
    let mut business = fixture_with_kind_and_preobserved_body(WorkKind::Business, false);
    business.scope.request_stop().unwrap();
    assert_eq!(
        business
            .authority
            .as_mut()
            .unwrap()
            .start_in_generation(generation()),
        Err(ExitRoleError::Runtime(RuntimeError::Scope(
            ScopeError::Stopped
        )))
    );
    assert_eq!(business.role.phase(), ExitRolePhase::Admitted);
    assert!(!business.role.body_exited() && !business.role.captures_exited());
    assert_eq!(business.ledger.used(dimension()), 1);
    business
        .authority
        .take()
        .unwrap()
        .revoke_unused_and_exit()
        .unwrap();
    retire_revoked(&business);

    let mut cleanup = fixture();
    cleanup.scope.request_stop().unwrap();
    let current = cleanup.scope.fail().unwrap();
    cleanup
        .authority
        .as_mut()
        .unwrap()
        .start_in_generation(current)
        .unwrap();
    assert_eq!(cleanup.role.phase(), ExitRolePhase::Started);
    assert!(!cleanup.role.body_exited() && !cleanup.role.captures_exited());
    cleanup.authority.take().unwrap().exited().unwrap();
    assert_eq!(
        cleanup.report.consumption(cleanup.owner).unwrap(),
        Consumption::Pending
    );
    assert_eq!(
        cleanup.report.consumption(cleanup.observer).unwrap(),
        Consumption::Pending
    );
    retire(&cleanup);
}

#[test]
fn a_start_that_won_before_failure_keeps_real_exit_independent_from_core_consumption() {
    let mut f = fixture_with_kind_and_preobserved_body(WorkKind::Business, false);
    f.authority
        .as_mut()
        .unwrap()
        .start_in_generation(generation())
        .unwrap();
    assert_eq!(f.role.phase(), ExitRolePhase::Started);
    f.scope.fail().unwrap();
    assert_eq!(f.role.phase(), ExitRolePhase::Started);
    assert!(!f.role.body_exited() && !f.role.captures_exited());
    assert_eq!(f.ledger.used(dimension()), 1);
    f.authority.take().unwrap().exited().unwrap();
    assert_eq!(f.role.phase(), ExitRolePhase::Exited);
    assert!(f.role.actual_exit_ready().unwrap());
    assert_eq!(f.report.consumption(f.owner).unwrap(), Consumption::Pending);
    assert_eq!(
        f.report.consumption(f.observer).unwrap(),
        Consumption::Pending
    );
    assert!(!f.role.try_retire().unwrap());
    // This is an explicit failed owner's consumption-leg abandonment; no Core
    // event commit is fabricated by the start, failure, or actual host exit.
    f.report
        .abandon_failed_owner(f.owner, &f.scope, generation())
        .unwrap();
    assert_eq!(
        f.report.consumption(f.owner).unwrap(),
        Consumption::AbandonedByOwnerFailure
    );
    assert!(!f.role.try_retire().unwrap());
    assert_eq!(f.ledger.used(dimension()), 1);
    f.report.consume(f.observer).unwrap();
    assert!(f.role.try_retire().unwrap());
    assert_eq!(f.ledger.used(dimension()), 0);
    assert_eq!(f.ledger.reap_scope_ready(f.scope.id(), 1).unwrap(), 1);
    f.scope.close_external().unwrap();
    f.ledger.remove_scope(f.scope.id()).unwrap();
    assert!(f.ledger.is_empty());
}
