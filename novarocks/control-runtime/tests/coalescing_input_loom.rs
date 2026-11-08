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

//! These models run the real standing-input and handoff synchronization. The
//! bounds cover the named competitions, not arbitrary executor scheduling.

use novarocks_control_runtime::{
    actor::{Actor, ActorConfig, ActorStep},
    actor_turn::ActorRuntime,
    coalescing_input::{CoalescingInput, Merge},
    transaction::RuntimeLedger,
};
use novarocks_control_runtime_core::{
    capacity::ResourceVector,
    identity::{ActorId, Generation, ScopeId, Tick},
    mailbox::{InputClass, InputEnvelope},
};
fn actor() -> ActorId {
    ActorId::new(1).unwrap()
}
#[derive(Debug)]
struct Signal(u64);
impl Merge for Signal {
    fn merge(&mut self, newer: Self) {
        self.0 += newer.0;
    }
    fn unit() -> Self {
        Self(1)
    }
}
#[derive(Debug, Default)]
struct Recorder(Vec<u64>);
impl Actor for Recorder {
    type Input = u64;
    type Effect = ();
    fn timer_input(_: Tick) -> u64 {
        unreachable!("no deadline")
    }
    fn handle(&mut self, _: Tick, _: InputEnvelope, value: u64, _: &mut ActorStep<'_, ()>) {
        self.0.push(value);
    }
    fn next_deadline(&self) -> Option<Tick> {
        None
    }
    fn owner_failed(_: ScopeId, _: Generation, _: InputEnvelope, _: &mut ActorStep<'_, ()>) {
        unreachable!()
    }
}
fn fixture() -> (
    RuntimeLedger,
    novarocks_control_runtime::transaction::ScopeHandle,
    ActorRuntime<Recorder>,
    CoalescingInput<Recorder, Signal>,
) {
    let ledger = RuntimeLedger::new(ResourceVector::default());
    let scope = ledger
        .open_scope_next(None, Generation::new(1).unwrap())
        .unwrap();
    let runtime = ActorRuntime::new();
    runtime
        .register_next(
            actor(),
            scope.clone(),
            Recorder::default(),
            ActorConfig {
                mailbox: [1; 4],
                max_batches: 2,
                max_effects_per_step: 1,
                max_dependencies_per_effect: 0,
                turn_budget: 8,
                max_step_work: 1,
            },
        )
        .unwrap();
    let standing = runtime
        .reserve_input(actor(), ActorId::new(2).unwrap(), InputClass::Ordinary)
        .unwrap()
        .into_standing_input()
        .unwrap();
    (
        ledger,
        scope,
        runtime,
        CoalescingInput::new(standing, |v| v.0),
    )
}
fn finish(
    ledger: RuntimeLedger,
    scope: novarocks_control_runtime::transaction::ScopeHandle,
    runtime: ActorRuntime<Recorder>,
    input: CoalescingInput<Recorder, Signal>,
) -> Vec<u64> {
    input.close_after_exit().unwrap();
    runtime.close_actor_admission(actor()).unwrap();
    scope.close_external().unwrap();
    while let Some(turn) = runtime.claim_host_actor() {
        turn.release(false).unwrap();
    }
    runtime.seal_actor(actor()).unwrap();
    let seen = runtime.remove_actor(actor()).unwrap().0;
    ledger.remove_scope(scope.id()).unwrap();
    assert!(runtime.is_empty());
    assert!(ledger.is_empty());
    seen
}

use loom::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
};
use novarocks_control_runtime::{
    completion::ReportSlot,
    exit_custody::{ExitCustody, ExitCustodyPhase},
    exit_work::{GatedNotStarted, GatedOutcome, GatedWork, HandoffCell, PanicSink, Withdrawal},
    reported_work::ReportedWork,
};
use novarocks_control_runtime_core::{
    capacity::ReservationResources,
    completion::ConsumerKind,
    identity::ConsumerId,
    owned_storage::OwnedSlotNeed,
    scope::{WorkAdmission, WorkKind},
};
use std::{
    future::{Future, pending, ready},
    task::{Context, Poll, Wake, Waker},
};
fn model(run: impl Fn() + Send + Sync + 'static) {
    let mut model = loom::model::Builder::new();
    model.preemption_bound = Some(2);
    model.max_branches = 20_000;
    model.check(run);
}
#[test]
fn notify_and_actual_retirement_never_lose_an_update_or_double_fill() {
    model(|| {
        let (ledger, scope, runtime, input) = fixture();
        let first = input.notify(Signal(1)).unwrap().unwrap();
        runtime.poll_one(Tick(1)).unwrap();
        let notifier = input.clone();
        let notify = thread::spawn(move || notifier.notify(Signal(2)).unwrap());
        let retired = input.clone();
        let retire_runtime = runtime.clone();
        let retire = thread::spawn(move || {
            retire_runtime.retire_input(actor(), first).unwrap();
            retired.after_retire().unwrap()
        });
        let filled = [notify.join().unwrap(), retire.join().unwrap()];
        assert_eq!(filled.iter().filter(|v| v.is_some()).count(), 1);
        let second = input.in_flight().unwrap().unwrap();
        assert!(second > first);
        runtime.poll_one(Tick(2)).unwrap();
        runtime.retire_input(actor(), second).unwrap();
        assert_eq!(input.after_retire().unwrap(), None);
        assert_eq!(finish(ledger, scope, runtime, input), vec![1, 2]);
    });
}
fn work() -> (
    RuntimeLedger,
    novarocks_control_runtime::transaction::ScopeHandle,
    ExitCustody,
    novarocks_control_runtime::exit_custody::ExitObligation,
    ReportSlot,
) {
    let ledger = RuntimeLedger::new(ResourceVector::default());
    let generation = Generation::new(1).unwrap();
    let scope = ledger.open_scope_next(None, generation).unwrap();
    let (work, mut capacity) = scope
        .reserve_next(
            generation,
            ReservationResources::new(
                ResourceVector::default(),
                OwnedSlotNeed {
                    work_headers: 1,
                    completions: 1,
                    captures: 1,
                    ..Default::default()
                },
            ),
            WorkKind::Business,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Business)
        .unwrap();
    let report = ReportSlot::reserve_next(
        &mut capacity,
        generation,
        1,
        [(ConsumerId::new(1).unwrap(), ConsumerKind::Observer)],
    )
    .unwrap();
    let (custody, obligation) =
        ExitCustody::from_admitted(ReportedWork::try_new(work, capacity, report.clone()).unwrap());
    (ledger, scope, custody, obligation, report)
}
fn retire_work(
    ledger: RuntimeLedger,
    scope: novarocks_control_runtime::transaction::ScopeHandle,
    custody: ExitCustody,
    report: ReportSlot,
    started: bool,
) {
    assert_eq!(custody.phase(), ExitCustodyPhase::Exited);
    if started {
        report.consume(ConsumerId::new(1).unwrap()).unwrap();
    }
    assert!(custody.try_retire().unwrap());
    ledger.reap_scope_ready(scope.id(), 1).unwrap();
    scope.close_external().unwrap();
    ledger.remove_scope(scope.id()).unwrap();
    assert!(ledger.is_empty());
}
struct CountWake(Arc<AtomicUsize>);
impl Wake for CountWake {
    fn wake(self: std::sync::Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[test]
fn withdrawal_and_actual_start_choose_exactly_one_outcome() {
    model(|| {
        let (ledger, scope, custody, obligation, report) = work();
        let handoff = Arc::new(HandoffCell::new());
        let withdraw_cell = handoff.clone();
        let withdrawing = thread::spawn(move || withdraw_cell.withdraw());
        let running = thread::spawn(move || {
            let mut future = Box::pin(
                GatedWork::new(obligation, (), handoff, PanicSink::new()).run(
                    Generation::new(1).unwrap(),
                    ready(()),
                    |(), ()| ready(7),
                ),
            );
            future
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
        });
        let withdrawn = withdrawing.join().unwrap();
        let outcome = running.join().unwrap();
        let started = match (withdrawn, outcome) {
            (
                Withdrawal::Withdrawn,
                Poll::Ready(Ok(GatedOutcome::NotStarted(GatedNotStarted::Withdrawn))),
            ) => false,
            (Withdrawal::TooLate, Poll::Ready(Ok(GatedOutcome::Completed(7)))) => true,
            other => panic!("handoff did not have exactly one winner: {other:?}"),
        };
        retire_work(ledger, scope, custody, report, started);
    });
}
#[test]
fn withdrawal_racing_pending_poll_always_wakes_or_is_observed() {
    model(|| {
        let (ledger, scope, custody, obligation, report) = work();
        let handoff = Arc::new(HandoffCell::new());
        let wakes = Arc::new(AtomicUsize::new(0));
        let observed = wakes.clone();
        let mut future = Box::pin(
            GatedWork::new(obligation, (), handoff.clone(), PanicSink::new()).run(
                Generation::new(1).unwrap(),
                pending::<()>(),
                |(), ()| ready(()),
            ),
        );
        let polling = thread::spawn(move || {
            let waker = Waker::from(std::sync::Arc::new(CountWake(observed)));
            let first = future.as_mut().poll(&mut Context::from_waker(&waker));
            (future, first)
        });
        let withdrawing = thread::spawn(move || handoff.withdraw());
        assert_eq!(withdrawing.join().unwrap(), Withdrawal::Withdrawn);
        let (mut future, first) = polling.join().unwrap();
        if first.is_pending() {
            assert_eq!(wakes.load(Ordering::SeqCst), 1);
            assert!(matches!(
                future
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop())),
                Poll::Ready(Ok(GatedOutcome::NotStarted(GatedNotStarted::Withdrawn)))
            ));
        } else {
            assert!(matches!(
                first,
                Poll::Ready(Ok(GatedOutcome::NotStarted(GatedNotStarted::Withdrawn)))
            ));
        }
        drop(future);
        retire_work(ledger, scope, custody, report, false);
    });
}
