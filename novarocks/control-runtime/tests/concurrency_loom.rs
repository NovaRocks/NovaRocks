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

//! These models run the production mutex/claim/report algorithms with at most
//! two preemptions. They target the named boundaries; they are not a proof of
//! arbitrary scheduling or host I/O behavior.

use loom::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
};
use novarocks_control_runtime::{
    actor::{Actor, ActorConfig, StepContext},
    completion::ReportSlot,
    driver::ActorRuntime,
    transaction::{RuntimeError, RuntimeLedger},
};
use novarocks_control_runtime_core::{
    capacity::{ReservationRequest, ResourceVector},
    completion::{CompletionError, ConsumerKind},
    identity::*,
    mailbox::{InputClass, InputEnvelope},
    scope::{WorkAdmission, WorkKind},
};
fn model(f: impl Fn() + Send + Sync + 'static) {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.max_branches = 20_000;
    builder.check(f);
}
fn sid(n: u64) -> ScopeId {
    ScopeId::new(n).unwrap()
}
fn wid(n: u64) -> WorkId {
    WorkId::new(n).unwrap()
}
fn aid(n: u64) -> ActorId {
    ActorId::new(n).unwrap()
}
fn generation() -> Generation {
    Generation::new(1).unwrap()
}
fn resources(n: u64) -> ResourceVector {
    ResourceVector::new([
        (DimensionId::new(1).unwrap(), n),
        (DimensionId::new(2).unwrap(), n),
    ])
    .unwrap()
}
fn request(scope_index: u64) -> ReservationRequest {
    ReservationRequest {
        id: ReservationId::new(scope_index).unwrap(),
        generation: generation(),
        guard: wid(1),
        kind: WorkKind::Business,
        resources: resources(1),
        admission: WorkAdmission::External,
    }
}
#[test]
fn multi_dimension_competition_is_atomic_with_two_preemptions() {
    model(|| {
        let ledger = RuntimeLedger::new(resources(1), 4, 2);
        let first = ledger.open_scope(sid(1), None, generation(), 4, 4).unwrap();
        let second = ledger.open_scope(sid(2), None, generation(), 4, 4).unwrap();
        let one = thread::spawn(move || {
            first
                .reserve(request(1))
                .and_then(|guard| guard.bind(WorkKind::Business))
                .ok()
        });
        let two = thread::spawn(move || {
            second
                .reserve(request(2))
                .and_then(|guard| guard.bind(WorkKind::Business))
                .ok()
        });
        let values = [one.join().unwrap(), two.join().unwrap()];
        assert_eq!(values.iter().filter(|v| v.is_some()).count(), 1);
        assert_eq!(ledger.used(DimensionId::new(1).unwrap()), 1);
        assert_eq!(ledger.used(DimensionId::new(2).unwrap()), 1);
        for (mut work, mut bundle) in values.into_iter().flatten() {
            work.body_exited().unwrap();
            work.captures_exited().unwrap();
            bundle.release(&resources(1)).unwrap();
        }
        ledger.reap_ready(2).unwrap();
        assert_eq!(ledger.used(DimensionId::new(1).unwrap()), 0);
    });
}
#[test]
fn close_and_reserve_have_one_order_with_two_preemptions() {
    model(|| {
        let ledger = RuntimeLedger::new(resources(1), 4, 1);
        let scope = ledger.open_scope(sid(1), None, generation(), 4, 4).unwrap();
        let closer = scope.clone();
        let reserver = scope.clone();
        let close = thread::spawn(move || closer.close_external().unwrap());
        let reserve = thread::spawn(move || {
            reserver
                .reserve(request(1))
                .and_then(|guard| guard.bind(WorkKind::Business))
                .ok()
        });
        close.join().unwrap();
        if let Some((mut work, mut bundle)) = reserve.join().unwrap() {
            assert!(!scope.snapshot().unwrap().join_ready);
            work.start().unwrap();
            work.body_exited().unwrap();
            work.captures_exited().unwrap();
            bundle.release(&resources(1)).unwrap();
        }
        ledger.reap_ready(4).unwrap();
        assert!(scope.snapshot().unwrap().retirement_ready);
    });
}
#[test]
fn report_publication_and_last_capture_are_distinct_with_two_preemptions() {
    model(|| {
        let ledger = RuntimeLedger::new(resources(1), 4, 1);
        let scope = ledger.open_scope(sid(1), None, generation(), 4, 4).unwrap();
        let mut work = scope
            .admit_work(
                generation(),
                wid(1),
                WorkKind::Business,
                WorkAdmission::External,
            )
            .unwrap();
        let consumer = ConsumerId::new(1).unwrap();
        let report = ReportSlot::reserve(
            &scope,
            SlotId::new(1).unwrap(),
            generation(),
            wid(1),
            1,
            [(consumer, ConsumerKind::Observer)],
        )
        .unwrap();
        let payload = Arc::new(AtomicUsize::new(0));
        let captured = Arc::new(AtomicBool::new(false));
        let consumed = Arc::new(AtomicBool::new(false));
        let producer_report = report.clone();
        let producer_payload = payload.clone();
        let producer_captured = captured.clone();
        let producer = thread::spawn(move || {
            producer_payload.store(7, Ordering::Relaxed);
            producer_report.fill().unwrap();
            producer_report.body_exited().unwrap();
            thread::yield_now();
            producer_captured.store(true, Ordering::Relaxed);
            producer_report.captures_exited().unwrap();
            work.body_exited().unwrap();
            work.captures_exited().unwrap();
        });
        let reader_report = report.clone();
        let reader_payload = payload.clone();
        let reader_captured = captured.clone();
        let reader_consumed = consumed.clone();
        let reader = thread::spawn(move || match reader_report.consume(consumer) {
            Ok(()) => {
                assert_eq!(reader_payload.load(Ordering::Relaxed), 7);
                reader_consumed.store(true, Ordering::Relaxed);
                if reader_report.try_retire().unwrap() {
                    assert!(reader_captured.load(Ordering::Relaxed));
                }
            }
            Err(RuntimeError::Completion(CompletionError::NotFilled)) => {}
            Err(error) => panic!("unexpected report failure: {error}"),
        });
        producer.join().unwrap();
        reader.join().unwrap();
        if !consumed.load(Ordering::Relaxed) {
            report.consume(consumer).unwrap();
        }
        if scope.snapshot().unwrap().storage != 0 {
            assert!(report.try_retire().unwrap());
        }
        scope.close_external().unwrap();
        ledger.reap_ready(4).unwrap();
        assert!(scope.snapshot().unwrap().retirement_ready);
    });
}
struct Counter {
    seen: Vec<u64>,
}
impl Actor for Counter {
    type Input = u64;
    type Effect = ();
    fn timer_input(now: Tick) -> u64 {
        now.0
    }
    fn handle(
        &mut self,
        _now: Tick,
        _input: InputEnvelope,
        value: u64,
        _step: &mut StepContext<'_, ()>,
    ) {
        self.seen.push(value);
    }
    fn next_deadline(&self) -> Option<Tick> {
        None
    }
    fn owner_failed(
        _scope: ScopeId,
        _generation: Generation,
        _input: InputEnvelope,
        _step: &mut StepContext<'_, ()>,
    ) {
    }
}
#[test]
fn enqueue_racing_claim_release_cannot_lose_readiness_with_two_preemptions() {
    model(|| {
        let ledger = RuntimeLedger::new(resources(1), 4, 1);
        let scope = ledger.open_scope(sid(1), None, generation(), 4, 4).unwrap();
        let runtime = ActorRuntime::new(1);
        let config = ActorConfig {
            mailbox: [4; 4],
            max_batches: 4,
            max_effects_per_step: 1,
            max_dependencies_per_effect: 0,
            turn_budget: 8,
            max_step_work: 1,
        };
        runtime
            .register(
                aid(1),
                scope.clone(),
                SlotId::new(1).unwrap(),
                Counter { seen: vec![] },
                config,
            )
            .unwrap_or_else(|_| panic!("register pure actor"));
        let first = runtime
            .reserve_input(aid(1), aid(2), InputClass::Ordinary)
            .unwrap()
            .fill(1)
            .unwrap();
        let second = runtime
            .reserve_input(aid(1), aid(2), InputClass::Ordinary)
            .unwrap();
        let second_id = second.id();
        let poll_runtime = runtime.clone();
        let poller = thread::spawn(move || poll_runtime.poll_one(Tick(1)).unwrap());
        let writer = thread::spawn(move || second.fill(2).unwrap());
        poller.join().unwrap();
        writer.join().unwrap();
        for _ in 0..3 {
            runtime.poll_one(Tick(2)).unwrap();
        }
        assert_eq!(runtime.snapshot(aid(1)).unwrap().batches, 2);
        runtime.retire_input(aid(1), first).unwrap();
        runtime.retire_input(aid(1), second_id).unwrap();
        scope.close_external().unwrap();
        while let Some(ready) = runtime.claim_effect_actor() {
            runtime.release_effect_actor(ready, false).unwrap();
        }
        assert_eq!(runtime.remove_actor(aid(1)).unwrap().seen, vec![1, 2]);
        ledger.remove_scope(sid(1)).unwrap();
    });
}

#[test]
fn capacity_release_report_cannot_precede_its_release_with_two_preemptions() {
    model(|| {
        let ledger = RuntimeLedger::new(resources(1), 4, 1);
        let scope = ledger.open_scope(sid(1), None, generation(), 4, 4).unwrap();
        let (mut work, mut bundle) = scope
            .reserve(request(1))
            .unwrap()
            .bind(WorkKind::Business)
            .unwrap();
        let consumer = ConsumerId::new(1).unwrap();
        let report = ReportSlot::reserve(
            &scope,
            SlotId::new(1).unwrap(),
            generation(),
            wid(1),
            1,
            [(consumer, ConsumerKind::Observer)],
        )
        .unwrap();
        let producer_report = report.clone();
        let release = thread::spawn(move || {
            bundle
                .release_with_report(&resources(1), &producer_report)
                .unwrap();
        });
        let reader_report = report.clone();
        let reader_ledger = ledger.clone();
        let consumed = Arc::new(AtomicBool::new(false));
        let reader_consumed = consumed.clone();
        let read = thread::spawn(move || match reader_report.consume(consumer) {
            Ok(()) => {
                assert_eq!(reader_ledger.used(DimensionId::new(1).unwrap()), 0);
                reader_consumed.store(true, Ordering::Relaxed);
            }
            Err(RuntimeError::Completion(CompletionError::NotFilled)) => {}
            Err(error) => panic!("unexpected release report failure: {error}"),
        });
        release.join().unwrap();
        read.join().unwrap();
        if !consumed.load(Ordering::Relaxed) {
            report.consume(consumer).unwrap();
        }
        work.body_exited().unwrap();
        work.captures_exited().unwrap();
        report.body_exited().unwrap();
        report.captures_exited().unwrap();
        assert!(report.try_retire().unwrap());
        scope.close_external().unwrap();
        ledger.reap_ready(1).unwrap();
        assert!(scope.snapshot().unwrap().retirement_ready);
    });
}

struct SnapshotActor;
impl Actor for SnapshotActor {
    type Input = ();
    type Effect = ();
    fn timer_input(_now: Tick) {}
    fn handle(
        &mut self,
        _now: Tick,
        input: InputEnvelope,
        _value: (),
        step: &mut StepContext<'_, ()>,
    ) {
        step.effect(
            novarocks_control_runtime_core::effect::EffectDescriptor {
                id: ActionId::new(1).unwrap(),
                work: wid(input.input.get()),
                kind: novarocks_control_runtime_core::effect::EffectKind::Publication,
                dependencies: vec![],
            },
            (),
        )
        .unwrap();
    }
    fn next_deadline(&self) -> Option<Tick> {
        None
    }
    fn owner_failed(
        _scope: ScopeId,
        _generation: Generation,
        _input: InputEnvelope,
        _step: &mut StepContext<'_, ()>,
    ) {
    }
}
#[test]
fn snapshot_commit_and_scope_failure_have_one_publication_order_with_two_preemptions() {
    model(|| {
        use novarocks_control_runtime::driver::DriverError;
        use novarocks_control_runtime::publication::PublishedSnapshot;
        use novarocks_control_runtime_core::effect::EffectError;
        let ledger = RuntimeLedger::new(resources(1), 4, 1);
        let scope = ledger.open_scope(sid(1), None, generation(), 4, 4).unwrap();
        let runtime = ActorRuntime::new(1);
        let config = ActorConfig {
            mailbox: [4; 4],
            max_batches: 4,
            max_effects_per_step: 1,
            max_dependencies_per_effect: 0,
            turn_budget: 8,
            max_step_work: 1,
        };
        runtime
            .register(
                aid(1),
                scope.clone(),
                SlotId::new(1).unwrap(),
                SnapshotActor,
                config,
            )
            .unwrap_or_else(|_| panic!("register actor"));
        let input = runtime
            .reserve_input(aid(1), aid(2), InputClass::Ordinary)
            .unwrap()
            .fill(())
            .unwrap();
        runtime.poll_one(Tick(1)).unwrap();
        let ticket = runtime
            .take_effect(aid(1), input, ActionId::new(1).unwrap())
            .unwrap();
        let snapshot = PublishedSnapshot::new(&scope, Arc::new(0usize));
        let writer_snapshot = snapshot.clone();
        let writer_runtime = runtime.clone();
        let writer = thread::spawn(move || {
            let committed =
                match writer_runtime.commit_snapshot(&ticket, &writer_snapshot, Arc::new(1)) {
                    Ok(old) => {
                        assert_eq!(*old, 0);
                        true
                    }
                    Err((DriverError::Effect(EffectError::StaleGeneration), value)) => {
                        assert_eq!(*value, 1);
                        false
                    }
                    Err((error, _)) => panic!("unexpected publication rejection: {error}"),
                };
            (ticket, committed)
        });
        let failed_scope = scope.clone();
        let failure = thread::spawn(move || failed_scope.fail().unwrap());
        let (ticket, committed) = writer.join().unwrap();
        failure.join().unwrap();
        assert_eq!(*snapshot.read(), usize::from(committed));
        runtime.effect_completed(ticket).unwrap();
        runtime.retire_input(aid(1), input).unwrap();
        while let Some(actor) = runtime.claim_effect_actor() {
            runtime.release_effect_actor(actor, false).unwrap();
        }
        runtime.remove_actor(aid(1)).unwrap();
        ledger.remove_scope(sid(1)).unwrap();
    });
}

#[test]
fn prepaid_role_fill_and_retirement_have_one_reuse_order_after_close_with_two_preemptions() {
    model(|| {
        use loom::sync::Mutex;
        use novarocks_control_runtime::driver::DriverError;
        let ledger = RuntimeLedger::new(resources(1), 4, 1);
        let scope = ledger.open_scope(sid(1), None, generation(), 4, 4).unwrap();
        let runtime = ActorRuntime::new(1);
        let config = ActorConfig {
            mailbox: [1; 4],
            max_batches: 2,
            max_effects_per_step: 1,
            max_dependencies_per_effect: 0,
            turn_budget: 8,
            max_step_work: 1,
        };
        runtime
            .register(
                aid(1),
                scope.clone(),
                SlotId::new(1).unwrap(),
                Counter { seen: vec![] },
                config,
            )
            .unwrap_or_else(|_| panic!("register actor"));
        let role = runtime
            .reserve_input(aid(1), aid(2), InputClass::Ordinary)
            .unwrap()
            .into_role()
            .unwrap();
        let first = role.fill(1).unwrap();
        runtime.poll_one(Tick(1)).unwrap();
        runtime.close_admission();
        let role = Arc::new(Mutex::new(role));
        let writer_role = role.clone();
        let retire_runtime = runtime.clone();
        let writer = thread::spawn(move || writer_role.lock().unwrap().fill(2));
        let retire = thread::spawn(move || retire_runtime.retire_input(aid(1), first).unwrap());
        let filled = writer.join().unwrap();
        retire.join().unwrap();
        let second = match filled {
            Ok(input) => input,
            Err((DriverError::RoleBusy, value)) => role.lock().unwrap().fill(value).unwrap(),
            Err((error, _)) => panic!("unexpected role fill: {error}"),
        };
        assert!(second > first);
        runtime.poll_one(Tick(2)).unwrap();
        runtime.retire_input(aid(1), second).unwrap();
        role.lock().unwrap().close_after_exit().unwrap();
        scope.close_external().unwrap();
        for _ in 0..3 {
            runtime.poll_one(Tick(2)).unwrap();
        }
        while let Some(actor) = runtime.claim_effect_actor() {
            runtime.release_effect_actor(actor, false).unwrap();
        }
        assert_eq!(runtime.remove_actor(aid(1)).unwrap().seen, vec![1, 2]);
        ledger.remove_scope(sid(1)).unwrap();
    });
}

#[test]
fn concurrent_id_selection_and_admission_never_reorder_the_owner_watermark() {
    model(|| {
        let ledger = RuntimeLedger::new(resources(2), 2, 1);
        let scope = ledger.open_scope_next(None, generation(), 2, 2).unwrap();
        let mut threads = vec![];
        for _ in 0..2 {
            let scope = scope.clone();
            threads.push(thread::spawn(move || {
                let (work, bundle) = scope
                    .reserve_next(
                        generation(),
                        resources(1),
                        WorkKind::Business,
                        WorkAdmission::External,
                    )
                    .unwrap()
                    .bind(WorkKind::Business)
                    .unwrap();
                let report =
                    ReportSlot::reserve_next(&scope, generation(), work.id(), 0, []).unwrap();
                (work, bundle, report)
            }));
        }
        let [one, two]: [_; 2] = threads
            .into_iter()
            .map(|t| t.join().unwrap())
            .collect::<Vec<_>>()
            .try_into()
            .ok()
            .unwrap();
        assert_ne!(one.0.id(), two.0.id());
        assert_ne!(one.1.id(), two.1.id());
        assert_ne!(one.2.id(), two.2.id());
        assert_eq!(scope.snapshot().unwrap().work, 2);
        assert_eq!(scope.snapshot().unwrap().storage, 2);
        for (mut work, mut bundle, report) in [one, two] {
            report.revoke_unused().unwrap();
            report.body_exited().unwrap();
            report.captures_exited().unwrap();
            assert!(report.try_retire().unwrap());
            work.body_exited().unwrap();
            work.captures_exited().unwrap();
            bundle.release(&resources(1)).unwrap();
        }
        assert_eq!(ledger.reap_ready(2).unwrap(), 2);
        scope.close_external().unwrap();
        ledger.remove_scope(scope.id()).unwrap();
        assert!(ledger.is_empty());
    });
}

#[test]
fn input_reservation_racing_removal_cannot_reopen_a_stale_node_with_two_preemptions() {
    model(|| {
        use novarocks_control_runtime::driver::DriverError;
        let ledger = RuntimeLedger::new(resources(0), 1, 1);
        let scope = ledger.open_scope_next(None, generation(), 1, 1).unwrap();
        let runtime = ActorRuntime::new(1);
        runtime
            .register_next(
                aid(1),
                scope.clone(),
                Counter { seen: Vec::new() },
                ActorConfig {
                    mailbox: [1; 4],
                    max_batches: 1,
                    max_effects_per_step: 1,
                    max_dependencies_per_effect: 0,
                    turn_budget: 8,
                    max_step_work: 1,
                },
            )
            .unwrap_or_else(|(error, _)| panic!("model registration: {error:?}"));
        scope.close_external().unwrap();
        let admitting = runtime.clone();
        let producer = thread::spawn(move || {
            match admitting.reserve_input(aid(1), aid(2), InputClass::Ordinary) {
                Ok(permit) => permit.revoke_unused().unwrap(),
                Err(DriverError::Closed | DriverError::UnknownActor) => (),
                Err(error) => panic!("unexpected admission result: {error:?}"),
            }
        });
        let retiring = runtime.clone();
        let removal = thread::spawn(move || retiring.remove_actor(aid(1)));
        producer.join().unwrap();
        let actor = match removal.join().unwrap() {
            Ok(actor) => actor,
            Err(DriverError::ActorNotRetired) => runtime.remove_actor(aid(1)).unwrap(),
            Err(error) => panic!("unexpected retirement result: {error:?}"),
        };
        assert!(actor.seen.is_empty());
        ledger.remove_scope(scope.id()).unwrap();
        assert!(runtime.is_empty());
        assert!(ledger.is_empty());
    });
}

#[test]
fn reservation_parent_exit_and_unused_child_rollback_share_one_retirement_order() {
    model(|| {
        use novarocks_control_runtime_core::{capacity::CapacityError, scope::ScopeError};
        let ledger = RuntimeLedger::new(resources(2), 4, 1);
        let scope = ledger.open_scope_next(None, generation(), 4, 4).unwrap();
        let (mut parent, mut capacity) = scope
            .reserve_next(
                generation(),
                resources(1),
                WorkKind::Business,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Business)
            .unwrap();
        let parent_reservation = capacity.id();
        scope.close_external().unwrap();
        ledger.close_admission();
        let exiting = thread::spawn(move || {
            parent.body_exited().unwrap();
            parent.captures_exited().unwrap();
            parent
        });
        let deriving = scope.clone();
        let child = thread::spawn(move || {
            match deriving.reserve_child_next(
                generation(),
                parent_reservation,
                resources(1),
                WorkKind::Business,
            ) {
                Ok(unbound) => drop(unbound),
                Err(RuntimeError::Capacity(CapacityError::Scope(
                    ScopeError::InvalidTransition,
                ))) => (),
                Err(error) => panic!("unexpected parent admission result: {error}"),
            }
        });
        let parent = exiting.join().unwrap();
        child.join().unwrap();
        assert!(parent.actual_exit_ready().unwrap());
        assert_eq!(ledger.reap_scope_ready(scope.id(), 1).unwrap(), 1);
        capacity.release(&resources(1)).unwrap();
        ledger.remove_scope(scope.id()).unwrap();
        assert!(ledger.is_empty());
    });
}

struct ClosingTimerActor {
    seen: usize,
}
impl Actor for ClosingTimerActor {
    type Input = ();
    type Effect = ();
    fn timer_input(_now: Tick) {}
    fn handle(
        &mut self,
        _now: Tick,
        _input: InputEnvelope,
        _value: (),
        _step: &mut StepContext<'_, ()>,
    ) {
        self.seen += 1;
        thread::yield_now();
    }
    fn next_deadline(&self) -> Option<Tick> {
        Some(Tick(1))
    }
    fn owner_failed(
        _scope: ScopeId,
        _generation: Generation,
        _input: InputEnvelope,
        _step: &mut StepContext<'_, ()>,
    ) {
    }
}
#[test]
fn timer_source_close_and_advance_preserve_real_pending_input_with_two_preemptions() {
    model(|| {
        use novarocks_control_runtime::driver::TimerSourceState;
        let ledger = RuntimeLedger::new(resources(1), 4, 1);
        let scope = ledger.open_scope_next(None, generation(), 4, 4).unwrap();
        let (mut work, mut capacity) = scope
            .reserve_next(
                generation(),
                resources(1),
                WorkKind::Step,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Step)
            .unwrap();
        work.start().unwrap();
        let runtime = ActorRuntime::new(1);
        runtime
            .register_next(
                aid(1),
                scope.clone(),
                ClosingTimerActor { seen: 0 },
                ActorConfig {
                    mailbox: [2; 4],
                    max_batches: 2,
                    max_effects_per_step: 1,
                    max_dependencies_per_effect: 0,
                    turn_budget: 8,
                    max_step_work: 1,
                },
            )
            .unwrap_or_else(|_| panic!("actual timer actor registration"));
        let mut role = runtime
            .reserve_input(aid(1), aid(2), InputClass::Control)
            .unwrap()
            .into_role()
            .unwrap();
        let advancing = runtime.clone();
        let producer = thread::spawn(move || advancing.advance_time(Tick(1), 1).unwrap());
        let closing = runtime.clone();
        let closer = thread::spawn(move || closing.close_timer_source(aid(1)).unwrap());
        let filled = producer.join().unwrap();
        closer.join().unwrap();
        let inputs = runtime.input_inventory(aid(1), None, 2).unwrap();
        let observed = runtime.timer_source_state(aid(1)).unwrap();
        runtime.poll_one(Tick(1)).unwrap();
        for input in &inputs {
            runtime.retire_input(aid(1), input.input).unwrap();
        }
        let drained = runtime.timer_source_state(aid(1)).unwrap();
        let deadline = runtime.next_deadline();
        // Closing the builtin source neither closes this original role nor
        // turns external drain into fresh admission.
        ledger.close_admission();
        scope.close_external().unwrap();
        runtime.close_actor_admission(aid(1)).unwrap();
        runtime.close_admission();
        let next = role.fill(()).unwrap();
        runtime.poll_one(Tick(2)).unwrap();
        runtime.retire_input(aid(1), next).unwrap();
        role.close_after_exit().unwrap();
        while let Some(actor) = runtime.claim_effect_actor() {
            runtime.release_effect_actor(actor, false).unwrap();
        }
        runtime.seal_actor(aid(1)).unwrap();
        work.body_exited().unwrap();
        work.captures_exited().unwrap();
        ledger.reap_scope_ready(scope.id(), 1).unwrap();
        let actual = runtime.remove_actor(aid(1)).unwrap();
        capacity.release(&resources(1)).unwrap();
        ledger.remove_scope(scope.id()).unwrap();
        assert_eq!(filled, inputs.len());
        assert_eq!(
            observed,
            inputs
                .first()
                .map_or(TimerSourceState::ClosedDrained, |input| {
                    TimerSourceState::ClosedPending(input.input)
                })
        );
        assert_eq!(drained, TimerSourceState::ClosedDrained);
        assert_eq!(deadline, None);
        assert_eq!(actual.seen, filled + 1);
        assert!(runtime.is_empty());
        assert!(ledger.is_empty());
    });
}

#[test]
fn split_next_and_reserve_next_share_the_production_identity_transaction() {
    model(|| {
        let ledger = RuntimeLedger::new(resources(5), 3, 1);
        let scope = ledger.open_scope_next(None, generation(), 2, 0).unwrap();
        let (mut original_work, mut original_capacity) = scope
            .reserve_next(
                generation(),
                resources(4),
                WorkKind::Step,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Step)
            .unwrap();
        original_work.start().unwrap();
        let original_id = original_capacity.id();
        let split = thread::spawn(move || {
            let split = original_capacity.split_next(&resources(1));
            (original_capacity, split)
        });
        let reserving_scope = scope.clone();
        let reserve = thread::spawn(move || {
            reserving_scope
                .reserve_next(
                    generation(),
                    resources(1),
                    WorkKind::Step,
                    WorkAdmission::External,
                )
                .and_then(|guard| guard.bind(WorkKind::Step))
        });
        let (mut original_capacity, split) = split.join().unwrap();
        let reserved = reserve.join().unwrap();
        let split_error = split.as_ref().err().copied();
        let reserve_error = reserved.as_ref().err().copied();
        let split_id = split.as_ref().ok().map(|capacity| capacity.id());
        let reserved_id = reserved.as_ref().ok().map(|(_, capacity)| capacity.id());
        let committed = [
            ledger.used(DimensionId::new(1).unwrap()),
            ledger.used(DimensionId::new(2).unwrap()),
        ];
        let work_count = scope.snapshot().unwrap().work;
        // The split owns no new Work. Both real metadata holders have returned
        // before their original body/capture exit and capacity release.
        original_work.body_exited().unwrap();
        original_work.captures_exited().unwrap();
        original_capacity
            .release(&original_capacity.resources().unwrap())
            .unwrap();
        if let Ok(mut capacity) = split {
            capacity.release(&capacity.resources().unwrap()).unwrap();
        }
        let mut retired_work = 1;
        if let Ok((mut work, mut capacity)) = reserved {
            work.start().unwrap();
            work.body_exited().unwrap();
            work.captures_exited().unwrap();
            capacity.release(&capacity.resources().unwrap()).unwrap();
            retired_work += 1;
        }
        scope.close_external().unwrap();
        let reaped = ledger.reap_scope_ready(scope.id(), 2).unwrap();
        ledger.remove_scope(scope.id()).unwrap();
        assert!(ledger.is_empty());
        assert_eq!(split_error, None);
        assert_eq!(reserve_error, None);
        assert_eq!(committed, [5, 5]);
        assert_eq!(work_count, 2);
        assert_eq!(reaped, retired_work);
        assert_ne!(split_id, reserved_id);
        assert!(split_id.unwrap().get() > original_id.get());
        assert!(reserved_id.unwrap().get() > original_id.get());
    });
}

#[test]
fn admitted_neutral_input_and_scope_failure_share_the_actual_author_transaction() {
    model(|| {
        use novarocks_control_runtime::driver::DriverError;
        let ledger = RuntimeLedger::new(resources(1), 1, 1);
        let scope = ledger.open_scope_next(None, generation(), 1, 1).unwrap();
        let (mut work, mut capacity) = scope
            .reserve_next(
                generation(),
                resources(1),
                WorkKind::Settlement,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Settlement)
            .unwrap();
        work.start().unwrap();
        let runtime = ActorRuntime::new(1);
        runtime
            .register_next(
                aid(1),
                scope.clone(),
                Counter { seen: Vec::new() },
                ActorConfig {
                    mailbox: [1; 4],
                    max_batches: 1,
                    max_effects_per_step: 1,
                    max_dependencies_per_effect: 0,
                    turn_budget: 8,
                    max_step_work: 1,
                },
            )
            .unwrap_or_else(|_| panic!("actual admitted-input actor registration"));
        ledger.close_admission();
        scope.close_external().unwrap();
        scope.request_stop().unwrap();
        runtime.close_actor_admission(aid(1)).unwrap();
        runtime.close_admission();
        let producing = runtime.clone();
        let producer = thread::spawn(move || {
            let result = match producing.reserve_admitted_input(
                aid(1),
                aid(2),
                InputClass::Control,
                &work,
            ) {
                // No payload was ever bound. This retires only the actual
                // unused input position, never the author's Work or captures.
                Ok(permit) => permit.revoke_unused(),
                Err(error) => Err(error),
            };
            (work, result)
        });
        let failing = scope.clone();
        let failure = thread::spawn(move || failing.fail().unwrap());
        let (mut work, result) = producer.join().unwrap();
        failure.join().unwrap();
        let inputs = runtime.snapshot(aid(1)).unwrap().inputs;
        let actual_work = scope.snapshot().unwrap().work;
        let committed = [
            ledger.used(DimensionId::new(1).unwrap()),
            ledger.used(DimensionId::new(2).unwrap()),
        ];
        runtime.seal_actor(aid(1)).unwrap();
        work.body_exited().unwrap();
        work.captures_exited().unwrap();
        let reaped = ledger.reap_scope_ready(scope.id(), 1).unwrap();
        let actor = runtime.remove_actor(aid(1)).unwrap();
        capacity.release(&capacity.resources().unwrap()).unwrap();
        ledger.remove_scope(scope.id()).unwrap();
        assert!(matches!(result, Ok(()) | Err(DriverError::ActorFailed)));
        assert_eq!(inputs, 0);
        assert_eq!(actual_work, 1);
        assert_eq!(committed, [1, 1]);
        assert_eq!(reaped, 1);
        assert!(actor.seen.is_empty());
        assert!(runtime.is_empty());
        assert!(ledger.is_empty());
    });
}

// These actors own real admitted Work. Emitting and receiving remain neutral;
// the models exercise the production driver rather than a second mailbox model.
#[derive(Debug)]
enum EffectForwardInput {
    Emit,
    Value(u64),
}
#[derive(Debug)]
struct EffectForwardActor {
    work: WorkId,
    seen: Vec<u64>,
}
impl Actor for EffectForwardActor {
    type Input = EffectForwardInput;
    type Effect = ();
    fn timer_input(_: Tick) -> Self::Input {
        unreachable!("this model has no builtin timer")
    }
    fn handle(
        &mut self,
        _: Tick,
        _: InputEnvelope,
        value: Self::Input,
        step: &mut StepContext<'_, ()>,
    ) {
        match value {
            EffectForwardInput::Emit => step
                .effect(
                    novarocks_control_runtime_core::effect::EffectDescriptor {
                        id: ActionId::new(1).unwrap(),
                        work: self.work,
                        kind: novarocks_control_runtime_core::effect::EffectKind::Publication,
                        dependencies: vec![],
                    },
                    (),
                )
                .unwrap(),
            EffectForwardInput::Value(value) => self.seen.push(value),
        }
    }
    fn next_deadline(&self) -> Option<Tick> {
        None
    }
    fn owner_failed(_: ScopeId, _: Generation, _: InputEnvelope, _: &mut StepContext<'_, ()>) {}
}
struct EffectForwardOwner {
    actor: ActorId,
    scope: novarocks_control_runtime::transaction::ScopeHandle,
    work: novarocks_control_runtime::lease::WorkLease,
    capacity: novarocks_control_runtime::lease::CapacityBundle,
}
struct EffectForwardFixture {
    ledger: RuntimeLedger,
    runtime: ActorRuntime<EffectForwardActor>,
    owners: Vec<EffectForwardOwner>,
}
impl EffectForwardFixture {
    fn new() -> Self {
        let ledger = RuntimeLedger::new(resources(2), 2, 2);
        let runtime = ActorRuntime::new(2);
        let mut owners = Vec::with_capacity(2);
        for actor in [aid(1), aid(2)] {
            let scope = ledger.open_scope_next(None, generation(), 1, 1).unwrap();
            let (mut work, capacity) = scope
                .reserve_next(
                    generation(),
                    resources(1),
                    WorkKind::Settlement,
                    WorkAdmission::External,
                )
                .unwrap()
                .bind(WorkKind::Settlement)
                .unwrap();
            work.start().unwrap();
            runtime
                .register_next(
                    actor,
                    scope.clone(),
                    EffectForwardActor {
                        work: work.id(),
                        seen: vec![],
                    },
                    ActorConfig {
                        mailbox: [2; 4],
                        max_batches: 2,
                        max_effects_per_step: 1,
                        max_dependencies_per_effect: 0,
                        turn_budget: 8,
                        max_step_work: 1,
                    },
                )
                .unwrap_or_else(|(error, _)| panic!("actual forwarding model actor: {error:?}"));
            owners.push(EffectForwardOwner {
                actor,
                scope,
                work,
                capacity,
            });
        }
        Self {
            ledger,
            runtime,
            owners,
        }
    }
    fn ticket(&self, actor: ActorId) -> novarocks_control_runtime::driver::EffectTicket<()> {
        let input = self
            .runtime
            .reserve_input(actor, actor, InputClass::Ordinary)
            .unwrap()
            .fill(EffectForwardInput::Emit)
            .unwrap();
        self.runtime.poll_one(Tick(1)).unwrap();
        self.runtime
            .take_effect(actor, input, ActionId::new(1).unwrap())
            .unwrap()
    }
    fn role(
        &self,
        target: ActorId,
        source: ActorId,
    ) -> novarocks_control_runtime::driver::InputRole<EffectForwardActor> {
        self.runtime
            .reserve_input(target, source, InputClass::Control)
            .unwrap()
            .into_role()
            .unwrap()
    }
    fn finish(
        mut self,
        tickets: Vec<novarocks_control_runtime::driver::EffectTicket<()>>,
        mut roles: Vec<(
            ActorId,
            novarocks_control_runtime::driver::InputRole<EffectForwardActor>,
        )>,
    ) -> Vec<Vec<u64>> {
        for ticket in tickets {
            let (actor, input) = (ticket.actor, ticket.input);
            self.runtime.effect_completed(ticket).unwrap();
            self.runtime.retire_input(actor, input).unwrap();
        }
        // Scheduler order is independent of this owner vector. Drain the real
        // callback queues before retiring any of their exact input receipts.
        for _ in 0..roles.len() {
            self.runtime.poll_one(Tick(2)).unwrap();
        }
        for (target, role) in &mut roles {
            if let Some(input) = role.in_flight().unwrap() {
                self.runtime.retire_input(*target, input).unwrap();
            }
            role.close_after_exit().unwrap();
        }
        for owner in &mut self.owners {
            self.runtime.close_actor_admission(owner.actor).unwrap();
            owner.scope.close_external().unwrap();
            owner.work.body_exited().unwrap();
            owner.work.captures_exited().unwrap();
            self.ledger.reap_scope_ready(owner.scope.id(), 1).unwrap();
        }
        while let Some(actor) = self.runtime.claim_effect_actor() {
            self.runtime.release_effect_actor(actor, false).unwrap();
        }
        let mut seen = Vec::with_capacity(2);
        for owner in &mut self.owners {
            self.runtime.seal_actor(owner.actor).unwrap();
            seen.push(self.runtime.remove_actor(owner.actor).unwrap().seen);
            owner.capacity.release(&resources(1)).unwrap();
            self.ledger.remove_scope(owner.scope.id()).unwrap();
        }
        assert!(self.runtime.is_empty());
        assert!(self.ledger.is_empty());
        seen
    }
}

#[test]
fn effect_input_forwarding_and_source_failure_have_one_ledger_order_with_two_preemptions() {
    model(|| {
        use novarocks_control_runtime::driver::DriverError;
        use novarocks_control_runtime_core::effect::EffectError;
        let f = EffectForwardFixture::new();
        let ticket = f.ticket(aid(1));
        let role = f.role(aid(2), aid(1));
        // Existing positions can drain, without admitting a new role or Work.
        f.owners[1].scope.close_external().unwrap();
        f.owners[1].scope.request_stop().unwrap();
        f.runtime.close_actor_admission(aid(2)).unwrap();
        f.runtime.close_admission();
        let producing = f.runtime.clone();
        let forward = thread::spawn(move || {
            let result = producing.fill_from_effect(&ticket, &role, EffectForwardInput::Value(53));
            (ticket, role, result)
        });
        let source = f.owners[0].scope.clone();
        let fail = thread::spawn(move || source.fail().unwrap());
        let (ticket, role, result) = forward.join().unwrap();
        fail.join().unwrap();
        let receipt = role.in_flight().unwrap();
        let current_ticket = f.runtime.validate_effect_ticket(&ticket);
        let paid = [
            f.ledger.used(DimensionId::new(1).unwrap()),
            f.ledger.used(DimensionId::new(2).unwrap()),
        ];
        let seen = f.finish(vec![ticket], vec![(aid(2), role)]);
        // Both branches preserve the same neutral value: it is either in the
        // actual target actor or returned in the original refusal, never lost.
        match result {
            Ok(input) => {
                assert_eq!(receipt, Some(input));
                assert_eq!(seen, vec![vec![], vec![53]]);
            }
            Err((
                DriverError::Effect(EffectError::StaleGeneration),
                EffectForwardInput::Value(value),
            )) => {
                assert_eq!(value, 53);
                assert_eq!(receipt, None);
                assert_eq!(seen, vec![vec![], vec![]]);
            }
            other => panic!("unexpected actual forwarding order: {other:?}"),
        }
        assert_eq!(
            current_ticket,
            Err(DriverError::Effect(EffectError::StaleGeneration))
        );
        assert_eq!(paid, [2, 2]);
    });
}

#[test]
fn opposite_effect_input_forwards_share_actor_lock_order_with_two_preemptions() {
    model(|| {
        let f = EffectForwardFixture::new();
        let first_ticket = f.ticket(aid(1));
        let second_ticket = f.ticket(aid(2));
        let to_second = f.role(aid(2), aid(1));
        let to_first = f.role(aid(1), aid(2));
        let first_runtime = f.runtime.clone();
        let first = thread::spawn(move || {
            let result = first_runtime.fill_from_effect(
                &first_ticket,
                &to_second,
                EffectForwardInput::Value(61),
            );
            (first_ticket, to_second, result)
        });
        let second_runtime = f.runtime.clone();
        let second = thread::spawn(move || {
            let result = second_runtime.fill_from_effect(
                &second_ticket,
                &to_first,
                EffectForwardInput::Value(62),
            );
            (second_ticket, to_first, result)
        });
        let (first_ticket, to_second, first_result) = first.join().unwrap();
        let (second_ticket, to_first, second_result) = second.join().unwrap();
        let first_receipt = to_second.in_flight().unwrap();
        let second_receipt = to_first.in_flight().unwrap();
        let first_valid = f.runtime.validate_effect_ticket(&first_ticket);
        let second_valid = f.runtime.validate_effect_ticket(&second_ticket);
        let seen = f.finish(
            vec![first_ticket, second_ticket],
            vec![(aid(2), to_second), (aid(1), to_first)],
        );
        assert_eq!(first_receipt, Some(first_result.unwrap()));
        assert_eq!(second_receipt, Some(second_result.unwrap()));
        assert_eq!(first_valid, Ok(()));
        assert_eq!(second_valid, Ok(()));
        assert_eq!(seen, vec![vec![62], vec![61]]);
    });
}

mod staged_scope_stop_model {
    use super::*;
    use novarocks_control_runtime::history::HistoryCapture;
    use novarocks_control_runtime_core::{
        effect::{EffectDescriptor, EffectKind},
        history::HistoryFact,
        scope::ScopeError,
    };

    struct StopActor {
        work: WorkId,
    }
    impl Actor for StopActor {
        type Input = ();
        type Effect = ();
        fn timer_input(_: Tick) {}
        fn handle(&mut self, _: Tick, _: InputEnvelope, _: (), step: &mut StepContext<'_, ()>) {
            step.stop_producing();
            step.effect(
                EffectDescriptor {
                    id: ActionId::new(1).unwrap(),
                    work: self.work,
                    kind: EffectKind::Publication,
                    dependencies: vec![],
                },
                (),
            )
            .unwrap();
        }
        fn next_deadline(&self) -> Option<Tick> {
            None
        }
        fn owner_failed(_: ScopeId, _: Generation, _: InputEnvelope, _: &mut StepContext<'_, ()>) {}
    }

    #[test]
    fn actual_step_stop_commit_and_business_start_share_the_original_ledger_with_two_preemptions() {
        model(|| {
            let history = HistoryCapture::new(256);
            let ledger = RuntimeLedger::with_history(resources(3), 3, 1, history.clone());
            let scope = ledger.open_scope_next(None, generation(), 3, 1).unwrap();
            let mut roles = Vec::with_capacity(3);
            for kind in [WorkKind::Settlement, WorkKind::Business, WorkKind::Cleanup] {
                roles.push(
                    scope
                        .reserve_next(generation(), resources(1), kind, WorkAdmission::External)
                        .unwrap()
                        .bind(kind)
                        .unwrap(),
                );
            }
            let (mut metadata, mut metadata_capacity) = roles.remove(0);
            let (business, mut business_capacity) = roles.remove(0);
            let (mut cleanup, mut cleanup_capacity) = roles.remove(0);
            let business_id = business.id();
            metadata.start().unwrap();
            let runtime = ActorRuntime::with_history(1, history.clone());
            runtime
                .register_next(
                    aid(1),
                    scope.clone(),
                    StopActor {
                        work: metadata.id(),
                    },
                    ActorConfig {
                        mailbox: [1; 4],
                        max_batches: 1,
                        max_effects_per_step: 1,
                        max_dependencies_per_effect: 0,
                        turn_budget: 8,
                        max_step_work: 1,
                    },
                )
                .unwrap_or_else(|(error, _)| {
                    panic!("actual staged-stop model registration: {error:?}")
                });
            let input = runtime
                .reserve_input(aid(1), aid(1), InputClass::Control)
                .unwrap()
                .fill(())
                .unwrap();
            let polling = runtime.clone();
            let poll = thread::spawn(move || polling.poll_one(Tick(1)));
            let start = thread::spawn(move || {
                let mut business = business;
                let result = business.start();
                (business, result)
            });
            let polled = poll.join().unwrap();
            let (mut business, started) = start.join().unwrap();
            let gates = scope.gates().unwrap();
            let custody = runtime.input_custody(aid(1), input).unwrap().unwrap();
            let business_exited = business.actual_exit_ready().unwrap();
            let cleanup_started = cleanup.start();
            let paid = [
                ledger.used(DimensionId::new(1).unwrap()),
                ledger.used(DimensionId::new(2).unwrap()),
            ];
            let work_count = scope.snapshot().unwrap().work;

            // The real batch still owns its publication effect. Neither stop
            // nor a rejected start can acknowledge that body or retire its input.
            let effect = runtime
                .take_effect(aid(1), input, ActionId::new(1).unwrap())
                .unwrap();
            runtime.effect_completed(effect).unwrap();
            runtime.retire_input(aid(1), input).unwrap();
            runtime.close_actor_admission(aid(1)).unwrap();
            scope.close_external().unwrap();
            for work in [&mut metadata, &mut business, &mut cleanup] {
                work.body_exited().unwrap();
                work.captures_exited().unwrap();
            }
            ledger.reap_scope_ready(scope.id(), 3).unwrap();
            while let Some(actor) = runtime.claim_effect_actor() {
                runtime.release_effect_actor(actor, false).unwrap();
            }
            runtime.seal_actor(aid(1)).unwrap();
            let _retired_actor = runtime.remove_actor(aid(1)).unwrap();
            for capacity in [
                &mut metadata_capacity,
                &mut business_capacity,
                &mut cleanup_capacity,
            ] {
                capacity.release(&resources(1)).unwrap();
            }
            ledger.remove_scope(scope.id()).unwrap();
            let observed = history.drain(256);
            assert!(observed.complete);
            assert!(runtime.is_empty());
            assert!(ledger.is_empty());

            // History comes from the production transactions themselves. It
            // proves the admitted Business start either preceded the committed
            // gate or was refused; there is no separate model decision state.
            let stop: Vec<_> = observed.entries.iter().filter(|entry| matches!(
                entry.fact, HistoryFact::ScopeStopped { scope: owner } if owner == scope.id()
            )).map(|entry| entry.ordinal).collect();
            let start: Vec<_> = observed
                .entries
                .iter()
                .filter(|entry| {
                    matches!(
                        entry.fact, HistoryFact::Started { scope: owner, work }
                            if owner == scope.id() && work == business_id
                    )
                })
                .map(|entry| entry.ordinal)
                .collect();
            assert_eq!(stop.len(), 1);
            match started {
                Ok(()) => {
                    assert_eq!(start.len(), 1);
                    assert!(start[0] < stop[0]);
                }
                Err(RuntimeError::Scope(ScopeError::Stopped)) => assert!(start.is_empty()),
                other => panic!("unexpected actual Business start: {other:?}"),
            }
            assert_eq!(polled, Ok(true));
            assert!(gates.stopped && !gates.failed && !gates.external_open);
            assert_eq!(gates.generation, generation());
            assert_eq!(
                custody.phase,
                novarocks_control_runtime::driver::InputPhase::Committed
            );
            assert_eq!(cleanup_started, Ok(()));
            assert!(!business_exited);
            assert_eq!(paid, [3, 3]);
            assert_eq!(work_count, 3);
            assert!(observed.entries.iter().any(|entry| matches!(entry.fact,
                HistoryFact::InputConsumed { input: actual, .. } if actual == input)));
            assert!(observed.entries.iter().any(|entry| matches!(entry.fact,
                HistoryFact::BatchCommitted { input: actual, .. } if actual == input)));
        });
    }
}
