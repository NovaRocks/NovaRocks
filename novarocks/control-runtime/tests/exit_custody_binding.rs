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
    actor::{Actor, ActorConfig, ActorStep},
    actor_turn::{ActorRuntime, ActorTurnError},
    completion::ReportSlot,
    continuation::{ContinuationSlot, InvocationResult},
    exit_custody::ExitCustody,
    obligation::{ReservationObligation, WorkObligation},
    reported_work::{ReportedWork, ReportedWorkBindingError},
    transaction::{RuntimeError, RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    completion::{ConsumerKind, Consumption},
    identity::{ActorId, ConsumerId, DimensionId, Generation, ReservationId, ScopeId, Tick},
    mailbox::InputEnvelope,
    owned_storage::OwnedSlotNeed,
    scope::{WorkAdmission, WorkKind},
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
fn generation() -> Generation {
    Generation::new(1).unwrap()
}
fn dimension() -> DimensionId {
    DimensionId::new(1).unwrap()
}
fn consumer() -> ConsumerId {
    ConsumerId::new(1).unwrap()
}
fn resources(amount: u64) -> ResourceVector {
    ResourceVector::new([(dimension(), amount)]).unwrap()
}
fn role_resources(amount: u64, completions: usize) -> ReservationResources {
    ReservationResources::new(
        resources(amount),
        OwnedSlotNeed {
            work_headers: 1,
            completions,
            captures: 1,
            ..OwnedSlotNeed::default()
        },
    )
}
fn ledger(amount: u64) -> RuntimeLedger {
    RuntimeLedger::new(resources(amount))
}
fn scope(ledger: &RuntimeLedger) -> ScopeHandle {
    ledger.open_scope_next(None, generation()).unwrap()
}
struct Original {
    work: WorkObligation,
    capacity: ReservationObligation,
    report: ReportSlot,
}
fn original(scope: &ScopeHandle, amount: u64, kind: WorkKind) -> Original {
    let (work, mut capacity) = scope
        .reserve_next(
            generation(),
            role_resources(amount, 1),
            kind,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(kind)
        .unwrap();
    let report = ReportSlot::reserve_next(
        &mut capacity,
        generation(),
        1,
        [(consumer(), ConsumerKind::Observer)],
    )
    .unwrap();
    Original {
        work,
        capacity,
        report,
    }
}
fn finish_exit(reported: ReportedWork) {
    let report = reported.report().clone();
    let (role, authority) = ExitCustody::from_admitted(reported);
    authority.exited().unwrap();
    assert!(!role.try_retire().unwrap());
    report.consume(consumer()).unwrap();
    assert!(role.try_retire().unwrap());
}
fn finish_continuation(reported: ReportedWork) {
    let report = reported.report().clone();
    let (slot, token) = ContinuationSlot::<usize>::from_admitted(reported);
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    token
        .register(Box::new(move |value| {
            assert_eq!(value, 29);
            count.fetch_add(1, Ordering::SeqCst);
        }))
        .unwrap_or_else(|_| panic!("one actual registration"));
    slot.publish_barrier(29).unwrap();
    assert_eq!(slot.invoke().unwrap(), InvocationResult::Exited);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(!slot.try_retire().unwrap());
    report.consume(consumer()).unwrap();
    assert!(slot.try_retire().unwrap());
}
fn finish_scopes(ledger: &RuntimeLedger, scopes: &[&ScopeHandle]) {
    for scope in scopes {
        ledger.reap_scope_ready(scope.id(), 16).unwrap();
        scope.close_external().unwrap();
        ledger.remove_scope(scope.id()).unwrap();
    }
    assert!(ledger.is_empty());
}
#[test]
fn a_different_scope_running_capacity_is_refused_and_returned_without_quota_release() {
    let ledger = ledger(3);
    let first = scope(&ledger);
    let second = scope(&ledger);
    let Original {
        work: a,
        capacity: capacity_a,
        report: report_a,
    } = original(&first, 1, WorkKind::Cleanup);
    let Original {
        mut work,
        capacity,
        report,
    } = original(&second, 2, WorkKind::Cleanup);
    work.start().unwrap();
    let a_id = a.id();
    let b_id = capacity.id();
    let same_report = report_a.clone();
    let before_a = first.snapshot().unwrap();
    let before_b = second.snapshot().unwrap();
    let refusal = ReportedWork::try_new(a, capacity, report_a).unwrap_err();
    assert_eq!(refusal.error, ReportedWorkBindingError::WrongScope);
    assert_eq!(refusal.work.id(), a_id);
    assert_eq!(refusal.capacity.id(), b_id);
    assert!(refusal.report.same_slot(&same_report));
    assert_eq!(ledger.used(dimension()), 3);
    assert!(!work.actual_exit_ready().unwrap());
    assert_eq!(first.snapshot().unwrap().work, before_a.work);
    assert_eq!(first.snapshot().unwrap().storage, before_a.storage);
    assert_eq!(second.snapshot().unwrap().work, before_b.work);
    assert_eq!(second.snapshot().unwrap().storage, before_b.storage);
    assert!(ledger.unresolved_work().is_empty());
    let restored_a = ReportedWork::try_new(refusal.work, capacity_a, refusal.report).unwrap();
    let restored_b = ReportedWork::try_new(work, refusal.capacity, report).unwrap();
    finish_exit(restored_a);
    assert_eq!(
        ledger.used(dimension()),
        2,
        "the actual running B retains all of its quota"
    );
    finish_exit(restored_b);
    finish_scopes(&ledger, &[&first, &second]);
}
#[test]
fn same_scope_different_work_capacity_cannot_bind_to_either_custodian() {
    let ledger = ledger(2);
    let scope = scope(&ledger);
    let Original {
        work: a,
        capacity: capacity_a,
        report: report_a,
    } = original(&scope, 1, WorkKind::Settlement);
    let Original {
        work: b,
        capacity: capacity_b,
        report: report_b,
    } = original(&scope, 1, WorkKind::Cleanup);
    let refusal = ReportedWork::try_new(a, capacity_b, report_a).unwrap_err();
    assert_eq!(
        refusal.error,
        ReportedWorkBindingError::CapacityWorkMismatch
    );
    assert_eq!(ledger.used(dimension()), 2);
    assert_eq!(scope.snapshot().unwrap().unresolved, 0);
    finish_continuation(ReportedWork::try_new(refusal.work, capacity_a, refusal.report).unwrap());
    assert_eq!(ledger.used(dimension()), 1);
    finish_exit(ReportedWork::try_new(b, refusal.capacity, report_b).unwrap());
    finish_scopes(&ledger, &[&scope]);
}
#[test]
fn two_real_ledgers_with_the_same_numeric_ids_cannot_share_capacity_or_report() {
    for foreign_report in [false, true] {
        let first = ledger(1);
        let second = ledger(1);
        let scope_a = scope(&first);
        let scope_b = scope(&second);
        let Original {
            work: a,
            capacity: capacity_a,
            report: report_a,
        } = original(&scope_a, 1, WorkKind::Cleanup);
        let Original {
            work: b,
            capacity: capacity_b,
            report: report_b,
        } = original(&scope_b, 1, WorkKind::Settlement);
        assert_eq!(a.id(), b.id());
        assert_eq!(capacity_a.id(), capacity_b.id());
        assert_eq!(report_a.id(), report_b.id());
        assert!(!report_a.same_slot(&report_b));
        let (restored_a, restored_b) = if foreign_report {
            let refusal = ReportedWork::try_new(a, capacity_a, report_b).unwrap_err();
            assert_eq!(refusal.error, ReportedWorkBindingError::DifferentLedger);
            (
                ReportedWork::try_new(refusal.work, refusal.capacity, report_a).unwrap(),
                ReportedWork::try_new(b, capacity_b, refusal.report).unwrap(),
            )
        } else {
            let refusal = ReportedWork::try_new(a, capacity_b, report_a).unwrap_err();
            assert_eq!(refusal.error, ReportedWorkBindingError::DifferentLedger);
            (
                ReportedWork::try_new(refusal.work, capacity_a, refusal.report).unwrap(),
                ReportedWork::try_new(b, refusal.capacity, report_b).unwrap(),
            )
        };
        assert_eq!(first.used(dimension()), 1);
        assert_eq!(second.used(dimension()), 1);
        assert!(first.unresolved_work().is_empty());
        assert!(second.unresolved_work().is_empty());
        finish_exit(restored_a);
        finish_continuation(restored_b);
        finish_scopes(&first, &[&scope_a]);
        finish_scopes(&second, &[&scope_b]);
    }
}
#[test]
fn a_report_from_another_scope_or_producer_is_refused_and_original_owners_recover() {
    for cross_scope in [false, true] {
        let ledger = ledger(2);
        let scope_a = scope(&ledger);
        let scope_b = cross_scope.then(|| scope(&ledger));
        let Original {
            work: a,
            capacity: capacity_a,
            report: report_a,
        } = original(&scope_a, 1, WorkKind::Settlement);
        let Original {
            work: b,
            capacity: capacity_b,
            report: report_b,
        } = original(scope_b.as_ref().unwrap_or(&scope_a), 1, WorkKind::Cleanup);
        let original_report_b = report_b.clone();
        let refusal = ReportedWork::try_new(a, capacity_a, report_b).unwrap_err();
        assert_eq!(
            refusal.error,
            if cross_scope {
                ReportedWorkBindingError::WrongScope
            } else {
                ReportedWorkBindingError::ReportProducerMismatch
            }
        );
        assert!(refusal.report.same_slot(&original_report_b));
        assert_eq!(ledger.used(dimension()), 2);
        assert_eq!(
            report_a.consumption(consumer()).unwrap(),
            Consumption::Pending
        );
        assert_eq!(
            refusal.report.consumption(consumer()).unwrap(),
            Consumption::Pending
        );
        finish_continuation(
            ReportedWork::try_new(refusal.work, refusal.capacity, report_a).unwrap(),
        );
        finish_exit(ReportedWork::try_new(b, capacity_b, refusal.report).unwrap());
        if let Some(scope_b) = &scope_b {
            finish_scopes(&ledger, &[&scope_a, scope_b]);
        } else {
            finish_scopes(&ledger, &[&scope_a]);
        }
    }
}
#[test]
fn a_real_split_bundle_is_accepted_by_its_actual_work_not_the_initial_reservation_id() {
    let ledger = ledger(3);
    let scope = scope(&ledger);
    let Original {
        work,
        mut capacity,
        report,
    } = original(&scope, 3, WorkKind::Cleanup);
    let split = capacity
        .split(
            ReservationId::new(1000).unwrap(),
            &ReservationResources::new(
                resources(1),
                OwnedSlotNeed {
                    captures: 1,
                    ..OwnedSlotNeed::default()
                },
            ),
        )
        .unwrap();
    assert_ne!(split.id(), capacity.id());
    let reported = ReportedWork::try_new(work, split, report).unwrap();
    assert_eq!(ledger.used(dimension()), 3);
    finish_exit(reported);
    assert_eq!(
        ledger.used(dimension()),
        2,
        "the separately owned original remainder is still charged"
    );
    capacity
        .release_quota(&capacity.resources().unwrap().quota)
        .unwrap();
    capacity.finish().unwrap();
    finish_scopes(&ledger, &[&scope]);
}
#[test]
fn a_legal_transfer_after_scope_failure_keeps_the_actual_binding_despite_generation_change() {
    let ledger = ledger(4);
    let first = scope(&ledger);
    let second = scope(&ledger);
    let Original {
        mut work,
        mut capacity,
        report,
    } = original(&first, 3, WorkKind::Cleanup);
    let Original {
        work: mut target,
        capacity: mut remainder,
        report: target_report,
    } = original(&second, 1, WorkKind::Cleanup);
    work.bind_owned_captures(&mut capacity, 1).unwrap();
    target.bind_owned_captures(&mut remainder, 1).unwrap();
    let mut route = capacity
        .split_next(&ReservationResources::new(
            resources(3),
            OwnedSlotNeed::default(),
        ))
        .unwrap();
    target.start().unwrap();
    second.fail().unwrap();
    route.transfer(&target).unwrap();
    // Transfer records the receiving scope's new generation. The original
    // target report retains its admitted generation and still belongs here.
    let target_role = ReportedWork::try_new(target, route, target_report).unwrap();
    assert_eq!(target_role.work().scope().id(), second.id());
    work.start().unwrap();
    work.body_exited().unwrap();
    work.captures_exited_with_report(&report).unwrap();
    report.body_exited().unwrap();
    report.captures_exited().unwrap();
    report.consume(consumer()).unwrap();
    assert!(report.try_retire().unwrap());
    capacity.finish().unwrap();
    assert_eq!(ledger.used(dimension()), 4);
    finish_exit(target_role);
    assert_eq!(ledger.used(dimension()), 1);
    remainder
        .release_quota(&remainder.resources().unwrap().quota)
        .unwrap();
    remainder.finish().unwrap();
    finish_scopes(&ledger, &[&first, &second]);
}

#[test]
fn a_retired_report_slot_cannot_be_reissued_explicitly_in_another_scope() {
    let ledger = ledger(1);
    let first = scope(&ledger);
    let original = original(&first, 1, WorkKind::Cleanup);
    let stale_report = original.report.clone();
    let old_id = stale_report.id();
    finish_exit(ReportedWork::try_new(original.work, original.capacity, original.report).unwrap());
    finish_scopes(&ledger, &[&first]);

    let second = scope(&ledger);
    let (work, mut capacity) = second
        .reserve_next(
            generation(),
            role_resources(1, 2),
            WorkKind::Cleanup,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Cleanup)
        .unwrap();
    let (unexpected, rejection) = match ReportSlot::reserve(
        &mut capacity,
        old_id,
        generation(),
        1,
        [(consumer(), ConsumerKind::Observer)],
    ) {
        Ok(report) => (Some(report), None),
        Err(error) => (None, Some(error)),
    };
    let report = ReportSlot::reserve_next(
        &mut capacity,
        generation(),
        1,
        [(consumer(), ConsumerKind::Observer)],
    )
    .unwrap();
    let fresh_id = report.id();
    let same_original_identity = stale_report.same_slot(&report);
    let (role, authority) =
        ExitCustody::from_admitted(ReportedWork::try_new(work, capacity, report.clone()).unwrap());
    authority.exited().unwrap();
    // Clean the erroneous accepted position after the same real Work exited,
    // so the red case also leaves no false unresolved or leaked test fixture.
    if let Some(unexpected) = unexpected {
        unexpected.revoke_unused().unwrap();
        unexpected.body_exited().unwrap();
        unexpected.captures_exited().unwrap();
        assert!(unexpected.try_retire().unwrap());
    }
    report.consume(consumer()).unwrap();
    assert!(role.try_retire().unwrap());
    finish_scopes(&ledger, &[&second]);
    assert!(fresh_id.get() > old_id.get());
    assert!(!same_original_identity);
    assert_eq!(
        rejection,
        Some(novarocks_control_runtime::transaction::RuntimeError::DuplicateSlot)
    );
}

#[derive(Debug)]
struct SlotIdentityActor {
    marker: u64,
}
impl Actor for SlotIdentityActor {
    type Input = ();
    type Effect = ();
    fn timer_input(_now: Tick) {}
    fn handle(
        &mut self,
        _now: Tick,
        _input: InputEnvelope,
        _value: (),
        _step: &mut ActorStep<'_, ()>,
    ) {
    }
    fn next_deadline(&self) -> Option<Tick> {
        None
    }
    fn owner_failed(
        _scope: ScopeId,
        _generation: Generation,
        _input: InputEnvelope,
        _step: &mut ActorStep<'_, ()>,
    ) {
    }
}
fn slot_identity_actor_config() -> ActorConfig {
    ActorConfig {
        mailbox: [1; 4],
        max_batches: 1,
        max_effects_per_step: 1,
        max_dependencies_per_effect: 0,
        turn_budget: 8,
        max_step_work: 1,
    }
}

#[test]
fn a_retired_report_slot_cannot_be_reissued_as_an_actor_attachment() {
    let ledger = ledger(1);
    let first = scope(&ledger);
    let original = original(&first, 1, WorkKind::Cleanup);
    let old_id = original.report.id();
    finish_exit(ReportedWork::try_new(original.work, original.capacity, original.report).unwrap());
    finish_scopes(&ledger, &[&first]);

    let runtime = ActorRuntime::new();
    let second = scope(&ledger);
    let before = second.snapshot().unwrap();
    let actor = ActorId::new(1).unwrap();
    let (core, rejection, refusal_left_no_custody, next_scope) = match runtime.register(
        actor,
        second.clone(),
        old_id,
        SlotIdentityActor { marker: 71 },
        slot_identity_actor_config(),
    ) {
        Err((error, core)) => {
            let after = second.snapshot().unwrap();
            let unchanged = after.work == before.work
                && after.storage == before.storage
                && after.unresolved == before.unresolved
                && ledger.used(dimension()) == 0
                && runtime.is_empty();
            (core, Some(error), unchanged, second)
        }
        Ok(()) => {
            // A broken cross-class gate must still return the actual Core
            // through normal actor removal before the final red assertion.
            second.close_external().unwrap();
            let core = runtime.remove_actor(actor).unwrap();
            finish_scopes(&ledger, &[&second]);
            (core, None, false, scope(&ledger))
        }
    };
    let returned_original_core = core.marker == 71;
    let next_actor = ActorId::new(2).unwrap();
    let fresh_id = runtime
        .register_next(
            next_actor,
            next_scope.clone(),
            core,
            slot_identity_actor_config(),
        )
        .unwrap();
    next_scope.close_external().unwrap();
    let core = runtime.remove_actor(next_actor).unwrap();
    finish_scopes(&ledger, &[&next_scope]);
    assert!(runtime.is_empty());
    assert!(fresh_id.get() > old_id.get());
    assert!(returned_original_core);
    assert_eq!(core.marker, 71);
    assert!(refusal_left_no_custody);
    assert_eq!(
        rejection,
        Some(ActorTurnError::Runtime(RuntimeError::DuplicateSlot))
    );
}

#[test]
fn a_removed_actor_attachment_cannot_be_reissued_as_a_report_slot() {
    let ledger = ledger(1);
    let runtime = ActorRuntime::new();
    let first = scope(&ledger);
    let actor = ActorId::new(1).unwrap();
    let old_id = runtime
        .register_next(
            actor,
            first.clone(),
            SlotIdentityActor { marker: 83 },
            slot_identity_actor_config(),
        )
        .unwrap();
    first.close_external().unwrap();
    let core = runtime.remove_actor(actor).unwrap();
    let actually_detached = first.snapshot().unwrap().retirement_ready && runtime.is_empty();
    finish_scopes(&ledger, &[&first]);

    let second = scope(&ledger);
    let (work, mut capacity) = second
        .reserve_next(
            generation(),
            role_resources(1, 2),
            WorkKind::Cleanup,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Cleanup)
        .unwrap();
    let before = second.snapshot().unwrap();
    let (unexpected, rejection) = match ReportSlot::reserve(
        &mut capacity,
        old_id,
        generation(),
        1,
        [(consumer(), ConsumerKind::Observer)],
    ) {
        Ok(report) => (Some(report), None),
        Err(error) => (None, Some(error)),
    };
    let after = second.snapshot().unwrap();
    let refusal_left_no_custody = after.work == before.work
        && after.storage == before.storage
        && after.unresolved == before.unresolved
        && ledger.used(dimension()) == 1
        && runtime.is_empty();
    let report = ReportSlot::reserve_next(
        &mut capacity,
        generation(),
        1,
        [(consumer(), ConsumerKind::Observer)],
    )
    .unwrap();
    let fresh_id = report.id();
    let (role, authority) =
        ExitCustody::from_admitted(ReportedWork::try_new(work, capacity, report.clone()).unwrap());
    authority.exited().unwrap();
    // The incorrect accepted report stays owned until its same actual Work
    // exits; only then can this red-case fixture retire the unused position.
    if let Some(unexpected) = unexpected {
        unexpected.revoke_unused().unwrap();
        unexpected.body_exited().unwrap();
        unexpected.captures_exited().unwrap();
        assert!(unexpected.try_retire().unwrap());
    }
    report.consume(consumer()).unwrap();
    assert!(role.try_retire().unwrap());
    finish_scopes(&ledger, &[&second]);
    assert!(actually_detached);
    assert_eq!(core.marker, 83);
    assert!(fresh_id.get() > old_id.get());
    assert!(refusal_left_no_custody);
    assert_eq!(rejection, Some(RuntimeError::DuplicateSlot));
}
