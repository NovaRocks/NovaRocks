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
    actor::{Actor, ActorConfig, StepContext},
    driver::{ActorRuntime, FailureSourceState, InputPhase},
    history::HistoryCapture,
    lease::{ReservationBundle, WorkLease},
    transaction::{RuntimeError, RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    effect::{EffectDescriptor, EffectKind, EffectState},
    history::HistoryFact,
    identity::{ActionId, ActorId, DimensionId, Generation, InputId, ScopeId, ScopedWorkRef, Tick},
    mailbox::{InputClass, InputEnvelope},
    owned_storage::OwnedSlotNeed,
    scope::{ScopeError, WorkAdmission, WorkKind},
};
use std::sync::{Arc, Barrier};

struct Pause {
    entered: Barrier,
    resume: Barrier,
}
impl Pause {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: Barrier::new(2),
            resume: Barrier::new(2),
        })
    }
}
#[derive(Clone, Copy, Debug)]
enum Command {
    Stop,
    StopPaused,
    StopThenPanic,
    Continue,
}
struct Controller {
    work: ScopedWorkRef,
    pause: Option<Arc<Pause>>,
}
impl Actor for Controller {
    type Input = Command;
    type Effect = ();
    fn timer_input(_: Tick) -> Command {
        unreachable!("no timer in this fixture")
    }
    fn handle(
        &mut self,
        _: Tick,
        _: InputEnvelope,
        command: Command,
        step: &mut StepContext<'_, ()>,
    ) {
        if !matches!(command, Command::Continue) {
            step.stop_producing();
        }
        if matches!(command, Command::StopPaused) {
            // Test-only scheduling control, never a production Core callback.
            let pause = self.pause.as_ref().unwrap();
            pause.entered.wait();
            pause.resume.wait();
        }
        if matches!(command, Command::StopThenPanic) {
            panic!("controlled panic before the staged gate can commit");
        }
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
fn actor() -> ActorId {
    ActorId::new(1).unwrap()
}
fn generation() -> Generation {
    Generation::new(1).unwrap()
}
fn dimension() -> DimensionId {
    DimensionId::new(1).unwrap()
}
fn resources(n: u64) -> ResourceVector {
    ResourceVector::new([(dimension(), n)]).unwrap()
}
struct Admitted {
    work: WorkLease,
    capacity: ReservationBundle,
}
impl Admitted {
    fn reserve(scope: &ScopeHandle, kind: WorkKind) -> Self {
        let (work, capacity) = scope
            .reserve_next(
                generation(),
                ReservationResources {
                    quota: resources(1),
                    owned_slots: OwnedSlotNeed {
                        work_headers: 1,
                        ..OwnedSlotNeed::default()
                    },
                },
                kind,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(kind)
            .unwrap();
        Self { work, capacity }
    }
}
struct Fixture {
    runtime: ActorRuntime<Controller>,
    ledger: RuntimeLedger,
    history: HistoryCapture,
    scope: ScopeHandle,
    metadata: Admitted,
    business: Admitted,
    cleanup: Admitted,
}
impl Fixture {
    fn new(pause: Option<Arc<Pause>>) -> Self {
        let history = HistoryCapture::new(256);
        let ledger = RuntimeLedger::with_history(resources(3), history.clone());
        let scope = ledger.open_scope_next(None, generation()).unwrap();
        let mut metadata = Admitted::reserve(&scope, WorkKind::Settlement);
        let business = Admitted::reserve(&scope, WorkKind::Business);
        let cleanup = Admitted::reserve(&scope, WorkKind::Cleanup);
        metadata.work.start().unwrap();
        let runtime = ActorRuntime::with_history(history.clone());
        runtime
            .register_supervised_bound(
                actor(),
                scope.clone(),
                Controller {
                    work: metadata.work.reference(),
                    pause,
                },
                ActorConfig {
                    mailbox: [1; 4],
                    max_batches: 1,
                    max_effects_per_step: 1,
                    max_dependencies_per_effect: 0,
                    turn_budget: 8,
                    max_step_work: 1,
                },
                &cleanup.work,
            )
            .unwrap_or_else(|(error, _)| panic!("actual controller registration: {error:?}"));
        Self {
            runtime,
            ledger,
            history,
            scope,
            metadata,
            business,
            cleanup,
        }
    }
    fn send(&self, command: Command) -> InputId {
        self.runtime
            .reserve_input(actor(), actor(), InputClass::Control)
            .unwrap()
            .fill(command)
            .unwrap()
    }
    fn finish(mut self, input: InputId) -> Vec<HistoryFact> {
        let payload = self.runtime.take_failure_payload(actor()).unwrap();
        drop(payload);
        let original = self.runtime.input_custody(actor(), input).unwrap().unwrap();
        if original.phase == InputPhase::Committed {
            for action in self.runtime.ready_effects(actor(), input, 1).unwrap() {
                let ticket = self.runtime.take_effect(actor(), input, action).unwrap();
                self.runtime.effect_completed(ticket).unwrap();
            }
        } else {
            assert_eq!(original.phase, InputPhase::AbandonedByOwnerFailure);
        }
        self.runtime.retire_input(actor(), input).unwrap();
        if let FailureSourceState::ClosedPending(supervisor) =
            self.runtime.failure_source_state(actor()).unwrap()
        {
            // The empty static failure projection has its own original batch.
            assert!(
                self.runtime
                    .ready_effects(actor(), supervisor, 1)
                    .unwrap()
                    .is_empty()
            );
            self.runtime.retire_input(actor(), supervisor).unwrap();
        }
        self.runtime.close_actor_admission(actor()).unwrap();
        self.scope.close_external().unwrap();
        for admitted in [&mut self.metadata, &mut self.business, &mut self.cleanup] {
            // Every remaining fixture body/holder is now actually unused or
            // has returned; no host capture or report is inferred from stop.
            admitted.work.body_exited().unwrap();
            admitted.work.captures_exited().unwrap();
        }
        self.ledger.reap_scope_ready(self.scope.id(), 3).unwrap();
        while let Some(original_host_turn) = self.runtime.claim_host_actor() {
            original_host_turn.release(false).unwrap();
        }
        self.runtime.seal_actor(actor()).unwrap();
        drop(self.runtime.remove_actor(actor()).unwrap());
        for admitted in [&mut self.metadata, &mut self.business, &mut self.cleanup] {
            admitted.capacity.release_quota(&resources(1)).unwrap();
            admitted.capacity.finish().unwrap();
        }
        self.ledger.remove_scope(self.scope.id()).unwrap();
        assert!(self.runtime.is_empty());
        assert!(self.ledger.is_empty());
        let history = self.history.drain(256);
        assert!(history.complete);
        history
            .entries
            .into_iter()
            .map(|entry| entry.fact)
            .collect()
    }
}
fn stop_count(history: &[HistoryFact]) -> usize {
    history
        .iter()
        .filter(|fact| matches!(fact, HistoryFact::ScopeStopped { .. }))
        .count()
}

#[test]
fn a_committed_stop_rejects_queued_business_before_effect_dispatch_but_allows_cleanup() {
    let mut f = Fixture::new(None);
    let input = f.send(Command::Stop);
    f.runtime.poll_one(Tick(1)).unwrap();
    let gates = f.scope.gates().unwrap();
    let custody = f.runtime.input_custody(actor(), input).unwrap().unwrap();
    let effects = f.runtime.effect_inventory(actor(), input, None, 1).unwrap();
    let business = f.business.work.start();
    let cleanup = f.cleanup.work.start();
    let paid = f.ledger.used(dimension());
    let work = f.scope.snapshot().unwrap().work;
    let history = f.finish(input);
    assert!(gates.stopped && !gates.external_open && !gates.failed);
    assert_eq!(gates.generation, generation());
    assert_eq!(custody.phase, InputPhase::Committed);
    assert_eq!(effects.len(), 1);
    assert_eq!(effects[0].1, EffectState::Pending);
    assert_eq!(business, Err(RuntimeError::Scope(ScopeError::Stopped)));
    assert_eq!(cleanup, Ok(()));
    assert_eq!(paid, 3);
    assert_eq!(work, 3);
    assert_eq!(stop_count(&history), 1);
    assert!(history.iter().any(
        |fact| matches!(fact, HistoryFact::InputConsumed { input: actual, .. } if *actual == input)
    ));
    assert!(history.iter().any(
        |fact| matches!(fact, HistoryFact::BatchCommitted { input: actual, .. } if *actual == input)
    ));
}

#[test]
fn a_staged_stop_is_not_visible_before_commit_and_an_already_started_body_remains_owned() {
    let pause = Pause::new();
    let mut f = Fixture::new(Some(pause.clone()));
    let input = f.send(Command::StopPaused);
    let runtime = f.runtime.clone();
    let poller = std::thread::spawn(move || runtime.poll_one(Tick(1)));
    pause.entered.wait();
    let before = f.scope.gates().unwrap();
    let started = f.business.work.start();
    let before_input = f.runtime.input_custody(actor(), input).unwrap().unwrap();
    pause.resume.wait();
    let polled = poller.join().unwrap();
    let after = f.scope.gates().unwrap();
    let actual_exit = f.business.work.actual_exit_ready().unwrap();
    let paid = f.ledger.used(dimension());
    let history = f.finish(input);
    assert_eq!(polled, Ok(true));
    assert!(!before.stopped && before.external_open);
    assert_eq!(before_input.phase, InputPhase::Consuming);
    assert_eq!(started, Ok(()));
    assert!(after.stopped);
    assert!(!actual_exit);
    assert_eq!(paid, 3);
    assert_eq!(stop_count(&history), 1);
}

#[test]
fn a_normal_step_without_a_directive_keeps_the_existing_scope_start_policy() {
    let mut f = Fixture::new(None);
    let input = f.send(Command::Continue);
    f.runtime.poll_one(Tick(1)).unwrap();
    let gates = f.scope.gates().unwrap();
    let business = f.business.work.start();
    let history = f.finish(input);
    assert!(!gates.stopped && gates.external_open && !gates.failed);
    assert_eq!(business, Ok(()));
    assert_eq!(stop_count(&history), 0);
}

#[test]
fn a_panicking_step_discards_its_directive_and_only_the_original_failure_path_stops_scope() {
    let mut f = Fixture::new(None);
    let input = f.send(Command::StopThenPanic);
    f.runtime.poll_one(Tick(1)).unwrap();
    let gates = f.scope.gates().unwrap();
    let custody = f.runtime.input_custody(actor(), input).unwrap().unwrap();
    let business = f.business.work.start();
    let history = f.finish(input);
    assert!(gates.failed && gates.stopped && !gates.external_open);
    assert_ne!(gates.generation, generation());
    assert_eq!(custody.phase, InputPhase::AbandonedByOwnerFailure);
    assert_eq!(business, Err(RuntimeError::Scope(ScopeError::Stopped)));
    assert_eq!(stop_count(&history), 0);
    assert_eq!(
        history
            .iter()
            .filter(|fact| matches!(fact, HistoryFact::OwnerFailed { .. }))
            .count(),
        1
    );
    assert!(!history.iter().any(
        |fact| matches!(fact, HistoryFact::InputConsumed { input: actual, .. } if *actual == input)
    ));
}
