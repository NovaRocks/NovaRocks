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
