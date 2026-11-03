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

//! Models the production custody locks with two preemptions. Host capture
//! disposal is supplied by the original authority, not simulated by a timeout.
use loom::thread;
use novarocks_control_runtime::{
    completion::ReportSlot,
    exit_custody::{ExitCustody, ExitCustodyPhase},
    transaction::{RuntimeError, RuntimeLedger},
};
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    completion::{CompletionError, ConsumerKind},
    identity::{ConsumerId, DimensionId, Generation},
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
fn resources() -> ResourceVector {
    ResourceVector::new([(dimension(), 1)]).unwrap()
}

fn role_resources() -> ReservationResources {
    ReservationResources::new(
        resources(),
        OwnedSlotNeed {
            work_headers: 1,
            completions: 1,
            captures: 1,
            ..OwnedSlotNeed::default()
        },
    )
}

#[test]
fn actual_exit_and_independent_consumption_cannot_reuse_capacity_early() {
    model(|| {
        let ledger = RuntimeLedger::new(resources());
        let scope = ledger.open_scope_next(None, generation()).unwrap();
        let (work, mut capacity) = scope
            .reserve_next(
                generation(),
                role_resources(),
                WorkKind::Cleanup,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Cleanup)
            .unwrap();
        let consumer = ConsumerId::new(1).unwrap();
        let report = ReportSlot::reserve_next(
            &mut capacity,
            generation(),
            1,
            [(consumer, ConsumerKind::Observer)],
        )
        .unwrap();
        let (original_exit_custody, mut authority) = ExitCustody::from_admitted(
            novarocks_control_runtime::reported_work::ReportedWork::try_new(
                work,
                capacity,
                report.clone(),
            )
            .unwrap(),
        );
        authority.start().unwrap();
        scope.close_external().unwrap();
        let finishing = original_exit_custody.clone();
        let exit = thread::spawn(move || {
            authority.exited().unwrap();
            finishing.try_retire().unwrap()
        });
        let consuming = report.clone();
        let consumed = thread::spawn(move || consuming.consume(consumer));
        let retired = exit.join().unwrap();
        match consumed.join().unwrap() {
            Ok(()) => (),
            Err(RuntimeError::Completion(CompletionError::NotFilled)) => {
                report.consume(consumer).unwrap()
            }
            other => panic!("unexpected consumer fact: {other:?}"),
        }
        if !retired {
            assert_eq!(ledger.used(dimension()), 1);
            assert!(original_exit_custody.try_retire().unwrap());
        }
        assert_eq!(original_exit_custody.phase(), ExitCustodyPhase::Retired);
        assert!(original_exit_custody.body_exited());
        assert!(original_exit_custody.captures_exited());
        assert_eq!(ledger.used(dimension()), 0);
        assert_eq!(ledger.reap_scope_ready(scope.id(), 1).unwrap(), 1);
        ledger.remove_scope(scope.id()).unwrap();
        assert!(ledger.is_empty());
    });
}

#[test]
fn scope_failure_abandons_only_the_owner_leg_and_never_the_original_exit() {
    model(|| {
        let ledger = RuntimeLedger::new(resources());
        let scope = ledger.open_scope_next(None, generation()).unwrap();
        let (work, mut capacity) = scope
            .reserve_next(
                generation(),
                role_resources(),
                WorkKind::Cleanup,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Cleanup)
            .unwrap();
        let owner = ConsumerId::new(1).unwrap();
        let observer = ConsumerId::new(2).unwrap();
        let report = ReportSlot::reserve_next(
            &mut capacity,
            generation(),
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
        let (original_exit_custody, mut authority) = ExitCustody::from_admitted(
            novarocks_control_runtime::reported_work::ReportedWork::try_new(
                work,
                capacity,
                report.clone(),
            )
            .unwrap(),
        );
        let exit = thread::spawn(move || {
            authority.start().unwrap();
            authority.exited().unwrap();
        });
        let failing = scope.clone();
        let abandoning = report.clone();
        let fail = thread::spawn(move || {
            failing.fail().unwrap();
            abandoning
                .abandon_failed_owner(owner, &failing, generation())
                .unwrap();
        });
        exit.join().unwrap();
        fail.join().unwrap();
        assert_eq!(original_exit_custody.phase(), ExitCustodyPhase::Exited);
        assert!(!original_exit_custody.try_retire().unwrap());
        assert_eq!(ledger.used(dimension()), 1);
        assert!(
            report
                .abandon_failed_owner(observer, &scope, generation())
                .is_err()
        );
        report.consume(observer).unwrap();
        assert!(original_exit_custody.try_retire().unwrap());
        assert_eq!(ledger.reap_scope_ready(scope.id(), 1).unwrap(), 1);
        ledger.remove_scope(scope.id()).unwrap();
        assert!(ledger.is_empty());
    });
}

#[test]
fn parent_physical_exit_waits_for_its_actual_child_but_not_for_report_consumption() {
    model(|| {
        let ledger = RuntimeLedger::new(ResourceVector::new([(dimension(), 2)]).unwrap());
        let scope = ledger.open_scope_next(None, generation()).unwrap();
        let (parent_work, mut parent_capacity) = scope
            .reserve_next(
                generation(),
                role_resources(),
                WorkKind::Business,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Business)
            .unwrap();
        let (child_work, mut child_capacity) = scope
            .reserve_child_next(
                generation(),
                parent_capacity.id(),
                role_resources(),
                WorkKind::Cleanup,
            )
            .unwrap()
            .bind(WorkKind::Cleanup)
            .unwrap();
        let consumer = ConsumerId::new(1).unwrap();
        let parent_report = ReportSlot::reserve_next(
            &mut parent_capacity,
            generation(),
            1,
            [(consumer, ConsumerKind::Observer)],
        )
        .unwrap();
        let child_report = ReportSlot::reserve_next(
            &mut child_capacity,
            generation(),
            1,
            [(consumer, ConsumerKind::Observer)],
        )
        .unwrap();
        let (parent, mut parent_authority) = ExitCustody::from_admitted(
            novarocks_control_runtime::reported_work::ReportedWork::try_new(
                parent_work,
                parent_capacity,
                parent_report.clone(),
            )
            .unwrap(),
        );
        let (child, mut child_authority) = ExitCustody::from_admitted(
            novarocks_control_runtime::reported_work::ReportedWork::try_new(
                child_work,
                child_capacity,
                child_report.clone(),
            )
            .unwrap(),
        );
        parent_authority.start().unwrap();
        child_authority.start().unwrap();
        parent_authority.exited().unwrap();
        assert!(!parent.actual_exit_ready().unwrap());
        scope.close_external().unwrap();
        let reaping = ledger.clone();
        let owner = scope.id();
        let exiting = thread::spawn(move || {
            child_authority.exited().unwrap();
            reaping.reap_scope_ready(owner, 2).unwrap();
        });
        let checking = parent.clone();
        let actual_child = child.clone();
        let observing = thread::spawn(move || {
            if checking.actual_exit_ready().unwrap() {
                assert!(actual_child.actual_exit_ready().unwrap());
            }
            assert!(!checking.try_retire().unwrap());
        });
        exiting.join().unwrap();
        observing.join().unwrap();
        assert!(parent.actual_exit_ready().unwrap());
        assert!(!parent.try_retire().unwrap());
        assert_eq!(
            ledger.used(dimension()),
            2,
            "physical exit does not consume either report"
        );
        assert!(!scope.snapshot().unwrap().retirement_ready);
        parent_report.consume(consumer).unwrap();
        child_report.consume(consumer).unwrap();
        assert!(parent.try_retire().unwrap());
        assert!(child.try_retire().unwrap());
        ledger.remove_scope(scope.id()).unwrap();
        assert!(ledger.is_empty());
    });
}

#[test]
fn owned_return_publish_take_and_retire_use_the_original_slot_and_versioned_predicate() {
    model(|| {
        let ledger = RuntimeLedger::new(resources());
        let scope = ledger.open_scope_next(None, generation()).unwrap();
        let (work, mut capacity) = scope
            .reserve_next(
                generation(),
                role_resources(),
                WorkKind::Cleanup,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Cleanup)
            .unwrap();
        let consumer = ConsumerId::new(1).unwrap();
        let report = ReportSlot::reserve_next(
            &mut capacity,
            generation(),
            1,
            [(consumer, ConsumerKind::Observer)],
        )
        .unwrap();
        let (sender, mut reader) = report.claim_owned_return::<usize>().unwrap();
        let (original_exit_custody, authority) = ExitCustody::from_admitted(
            novarocks_control_runtime::reported_work::ReportedWork::try_new(
                work,
                capacity,
                report.clone(),
            )
            .unwrap(),
        );
        authority.exited().unwrap();
        report.consume(consumer).unwrap();
        assert!(
            !original_exit_custody.try_retire().unwrap(),
            "the original return is still reserved"
        );
        let producer = thread::spawn(move || sender.publish(23).unwrap());
        let waiting_ledger = ledger.clone();
        let taking = thread::spawn(move || {
            loop {
                // Sampling after the predicate would permit a lost wake.
                let observed = waiting_ledger.change_version();
                if let Some(value) = reader.try_take().unwrap() {
                    assert_eq!(value, 23);
                    assert!(reader.try_take().unwrap().is_none());
                    break;
                }
                waiting_ledger.wait_after(observed);
            }
        });
        let retiring = original_exit_custody.clone();
        let retirement = thread::spawn(move || retiring.try_retire().unwrap());
        producer.join().unwrap();
        taking.join().unwrap();
        let retired = retirement.join().unwrap();
        if !retired {
            assert!(original_exit_custody.try_retire().unwrap());
        }
        assert_eq!(original_exit_custody.phase(), ExitCustodyPhase::Retired);
        assert_eq!(ledger.used(dimension()), 0);
        ledger.reap_scope_ready(scope.id(), 1).unwrap();
        scope.close_external().unwrap();
        ledger.remove_scope(scope.id()).unwrap();
        assert!(ledger.is_empty());
    });
}
#[test]
fn revoked_original_return_reader_drop_can_race_actual_report_retirement() {
    model(|| {
        let ledger = RuntimeLedger::new(resources());
        let scope = ledger.open_scope_next(None, generation()).unwrap();
        let (work, mut capacity) = scope
            .reserve_next(
                generation(),
                role_resources(),
                WorkKind::Cleanup,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Cleanup)
            .unwrap();
        let consumer = ConsumerId::new(1).unwrap();
        let report = ReportSlot::reserve_next(
            &mut capacity,
            generation(),
            1,
            [(consumer, ConsumerKind::Observer)],
        )
        .unwrap();
        let (sender, reader) = report.claim_owned_return::<usize>().unwrap();
        let (original_exit_custody, authority) = ExitCustody::from_admitted(
            novarocks_control_runtime::reported_work::ReportedWork::try_new(
                work,
                capacity,
                report.clone(),
            )
            .unwrap(),
        );
        scope.close_external().unwrap();
        sender.revoke_unused().unwrap();
        authority.exited().unwrap();
        report.consume(consumer).unwrap();
        let dropping = thread::spawn(move || drop(reader));
        let retiring = original_exit_custody.clone();
        let retired = thread::spawn(move || retiring.try_retire().unwrap());
        dropping.join().unwrap();
        assert!(retired.join().unwrap());
        ledger.reap_scope_ready(scope.id(), 1).unwrap();
        ledger.remove_scope(scope.id()).unwrap();
        assert!(ledger.is_empty());
    });
}
#[test]
fn pure_original_return_loss_stays_unknown_against_fill_take_and_retirement() {
    // The actual wrapper's Unknown payload deliberately retains an Arc self
    // pin and has no positive cleanup API. Model its shared production pure
    // state here; std integration separately observes real opaque retention.
    model(|| {
        use novarocks_control_runtime_core::{
            completion::{CompletionSlot, ReturnState},
            identity::{ScopeId, SlotId, WorkId},
        };
        let consumer = ConsumerId::new(1).unwrap();
        let mut slot = CompletionSlot::new(
            SlotId::new(1).unwrap(),
            ScopeId::new(1).unwrap(),
            generation(),
            WorkId::new(1).unwrap(),
            1,
            [(consumer, ConsumerKind::Observer)],
        )
        .unwrap();
        slot.claim_return().unwrap();
        slot.fill().unwrap();
        slot.observe_body_exit();
        slot.observe_captures_exit();
        slot.consume(consumer).unwrap();
        let slot = loom::sync::Arc::new(loom::sync::Mutex::new(slot));
        let filling = slot.clone();
        let producer = thread::spawn(move || {
            let mut slot = filling.lock().unwrap();
            let _ = slot.fill_return();
            assert!(!slot.retirable());
        });
        let losing = slot.clone();
        let lost = thread::spawn(move || assert!(losing.lock().unwrap().lose_return()));
        producer.join().unwrap();
        lost.join().unwrap();
        let mut slot = slot.lock().unwrap();
        assert_eq!(slot.return_state(), ReturnState::Unknown);
        assert!(!slot.retirable());
        assert!(slot.fill_return().is_err());
        assert!(slot.take_return().is_err());
        assert!(slot.revoke_return_unused().is_err());
        assert!(!slot.lose_return());
    });
}

#[test]
fn mismatched_carrier_refusal_keeps_the_other_running_owners_capacity() {
    model(|| {
        use novarocks_control_runtime::reported_work::{ReportedWork, ReportedWorkBindingError};
        let ledger = RuntimeLedger::new(ResourceVector::new([(dimension(), 2)]).unwrap());
        let scope = ledger.open_scope_next(None, generation()).unwrap();
        let (a, mut capacity_a) = scope
            .reserve_next(
                generation(),
                role_resources(),
                WorkKind::Cleanup,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Cleanup)
            .unwrap();
        let (mut b, mut capacity_b) = scope
            .reserve_next(
                generation(),
                role_resources(),
                WorkKind::Cleanup,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Cleanup)
            .unwrap();
        let consumer = ConsumerId::new(1).unwrap();
        let report_a = ReportSlot::reserve_next(
            &mut capacity_a,
            generation(),
            1,
            [(consumer, ConsumerKind::Observer)],
        )
        .unwrap();
        let report_b = ReportSlot::reserve_next(
            &mut capacity_b,
            generation(),
            1,
            [(consumer, ConsumerKind::Observer)],
        )
        .unwrap();
        b.start().unwrap();
        let (release, wait) = loom::sync::mpsc::channel();
        let running = thread::spawn(move || {
            wait.recv().unwrap();
            b
        });
        // Preserve all handles even if a broken validator incorrectly accepts
        // the mix, so every actual thread is joined before the final verdict.
        let (a, capacity_b, report_a, rejected) =
            match ReportedWork::try_new(a, capacity_b, report_a) {
                Err(refusal) => (
                    refusal.work,
                    refusal.capacity,
                    refusal.report,
                    Some(refusal.error),
                ),
                Ok(carrier) => {
                    let (a, capacity_b, report_a) = carrier.into_parts();
                    (a, capacity_b, report_a, None)
                }
            };
        let still_paid_while_b_runs = ledger.used(dimension()) == 2;
        release.send(()).unwrap();
        let b = running.join().unwrap();
        let (first, first_authority) = ExitCustody::from_admitted(
            ReportedWork::try_new(a, capacity_a, report_a.clone()).unwrap(),
        );
        let (second, second_authority) = ExitCustody::from_admitted(
            ReportedWork::try_new(b, capacity_b, report_b.clone()).unwrap(),
        );
        first_authority.exited().unwrap();
        report_a.consume(consumer).unwrap();
        assert!(first.try_retire().unwrap());
        assert_eq!(ledger.used(dimension()), 1);
        second_authority.exited().unwrap();
        report_b.consume(consumer).unwrap();
        assert!(second.try_retire().unwrap());
        ledger.reap_scope_ready(scope.id(), 2).unwrap();
        scope.close_external().unwrap();
        ledger.remove_scope(scope.id()).unwrap();
        assert!(ledger.is_empty());
        assert!(still_paid_while_b_runs);
        assert_eq!(
            rejected,
            Some(ReportedWorkBindingError::CapacityWorkMismatch)
        );
    });
}

#[test]
fn original_authority_child_scope_and_parent_stop_keep_one_actual_link_order() {
    model(|| {
        use novarocks_control_runtime::exit_custody::ExitCustodyError;
        use novarocks_control_runtime::reported_work::ReportedWork;
        use novarocks_control_runtime_core::scope::ScopeError;
        let paid = ResourceVector::new([(dimension(), 3)]).unwrap();
        let ledger = RuntimeLedger::new(paid.clone());
        let scope = ledger.open_scope_next(None, generation()).unwrap();
        let (work, mut capacity) = scope
            .reserve_next(
                generation(),
                ReservationResources::new(
                    paid,
                    OwnedSlotNeed {
                        work_headers: 3,
                        completions: 1,
                        captures: 1,
                        ..OwnedSlotNeed::default()
                    },
                ),
                WorkKind::Step,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Step)
            .unwrap();
        let consumer = ConsumerId::new(1).unwrap();
        let report = ReportSlot::reserve_next(
            &mut capacity,
            generation(),
            1,
            [(consumer, ConsumerKind::Observer)],
        )
        .unwrap();
        let (original_exit_custody, mut authority) = ExitCustody::from_admitted(
            ReportedWork::try_new(work, capacity, report.clone()).unwrap(),
        );
        authority.start().unwrap();
        scope.close_external().unwrap();
        ledger.close_admission();
        let creating = thread::spawn(move || {
            let child = authority.open_child_scope_with_root(
                generation(),
                WorkKind::Business,
                ReservationResources::work_only(),
            );
            (authority, child)
        });
        let stopping = scope.clone();
        let stop = thread::spawn(move || stopping.request_stop().unwrap());
        let (authority, child) = creating.join().unwrap();
        stop.join().unwrap();
        authority.exited().unwrap();
        report.consume(consumer).unwrap();
        let mut stopped_refusal = false;
        let mut blocked_by_child = false;
        match child {
            Ok((child, root)) => {
                blocked_by_child = !original_exit_custody.actual_exit_ready().unwrap()
                    && !original_exit_custody.try_retire().unwrap()
                    && ledger.used(dimension()) == 3;
                let (mut work, mut capacity) = root.bind(WorkKind::Business).unwrap();
                // A real admitted but never-run neutral child is actually
                // disposed; parent stop is not used as its exit observation.
                child.close_external().unwrap();
                work.body_exited().unwrap();
                work.captures_exited().unwrap();
                ledger.reap_scope_ready(child.id(), 1).unwrap();
                capacity.finish().unwrap();
                ledger.remove_scope(child.id()).unwrap();
                ledger.reap_scope_ready(scope.id(), 1).unwrap();
            }
            Err(ExitCustodyError::Runtime(RuntimeError::Scope(ScopeError::Stopped))) => {
                stopped_refusal =
                    scope.snapshot().unwrap().work == 1 && ledger.used(dimension()) == 3;
            }
            Err(error) => panic!("unexpected actual child admission result: {error}"),
        }
        assert!(original_exit_custody.try_retire().unwrap());
        ledger.reap_scope_ready(scope.id(), 1).unwrap();
        ledger.remove_scope(scope.id()).unwrap();
        assert!(blocked_by_child || stopped_refusal);
        assert_eq!(ledger.used(dimension()), 0);
        assert!(ledger.is_empty());
    });
}

#[test]
fn generation_guarded_start_and_scope_failure_have_one_order_without_inventing_exit() {
    model(|| {
        let ledger = RuntimeLedger::new(resources());
        let scope = ledger.open_scope_next(None, generation()).unwrap();
        // Keep the real supervisor separate from its already-paid source
        // child. Root failure races the child's getter start, not its cleanup.
        let (mut parent, mut parent_capacity) = scope
            .reserve_next(
                generation(),
                ReservationResources::work_only(),
                WorkKind::Step,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Step)
            .unwrap();
        parent.start().unwrap();
        let (child, guard) = ledger
            .open_child_scope_with_root(&parent, generation(), WorkKind::Cleanup, role_resources())
            .unwrap();
        let (work, mut capacity) = guard.bind(WorkKind::Cleanup).unwrap();
        let observer = ConsumerId::new(1).unwrap();
        let report = ReportSlot::reserve_next(
            &mut capacity,
            generation(),
            1,
            [(observer, ConsumerKind::Observer)],
        )
        .unwrap();
        let (original_exit_custody, mut authority) = ExitCustody::from_admitted(
            novarocks_control_runtime::reported_work::ReportedWork::try_new(
                work,
                capacity,
                report.clone(),
            )
            .unwrap(),
        );
        scope.close_external().unwrap();
        let original_parent = scope.clone();
        let starting = thread::spawn(move || {
            let result = authority.start_in_parent_generation(&original_parent, generation());
            (authority, result)
        });
        let failing_scope = scope.clone();
        let failing = thread::spawn(move || failing_scope.fail().unwrap());
        let (mut authority, started) = starting.join().unwrap();
        assert!(failing.join().unwrap() > generation());
        match started {
            Ok(()) => assert_eq!(original_exit_custody.phase(), ExitCustodyPhase::Started),
            Err(novarocks_control_runtime::exit_custody::ExitCustodyError::Runtime(
                RuntimeError::Scope(
                    novarocks_control_runtime_core::scope::ScopeError::GenerationMismatch,
                ),
            )) => assert_eq!(original_exit_custody.phase(), ExitCustodyPhase::Admitted),
            other => panic!("unexpected guarded-start result: {other:?}"),
        }
        assert!(!original_exit_custody.body_exited());
        assert!(!original_exit_custody.captures_exited());
        assert!(!original_exit_custody.actual_exit_ready().unwrap());
        assert_eq!(ledger.used(dimension()), 1);
        // Root failure never supplies child exit. If its getter was refused,
        // this same original Cleanup authority can still start real disposal.
        if original_exit_custody.phase() == ExitCustodyPhase::Admitted {
            authority.start().unwrap();
        }
        parent.body_exited().unwrap();
        parent.captures_exited().unwrap();
        assert!(!parent.actual_exit_ready().unwrap());
        authority.exited().unwrap();
        assert!(!original_exit_custody.try_retire().unwrap());
        assert_eq!(ledger.used(dimension()), 1);
        report.consume(observer).unwrap();
        assert!(original_exit_custody.try_retire().unwrap());
        assert_eq!(ledger.reap_scope_ready(child.id(), 1).unwrap(), 1);
        child.close_external().unwrap();
        ledger.remove_scope(child.id()).unwrap();
        ledger.reap_scope_ready(scope.id(), 2).unwrap();
        assert!(parent.actual_exit_ready().unwrap());
        parent_capacity.finish().unwrap();
        ledger.remove_scope(scope.id()).unwrap();
        assert!(ledger.is_empty());
    });
}
