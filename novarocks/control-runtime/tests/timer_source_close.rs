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
    driver::{ActorRuntime, InputRole, TimerSourceState},
    lease::{CapacityBundle, WorkLease},
    transaction::{RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::ResourceVector,
    identity::*,
    mailbox::{InputClass, InputEnvelope},
    scope::{WorkAdmission, WorkKind},
};
use std::sync::{Arc, Condvar, Mutex, OnceLock};

fn actor() -> ActorId {
    ActorId::new(1).unwrap()
}
fn generation() -> Generation {
    Generation::new(1).unwrap()
}
fn dimension() -> DimensionId {
    DimensionId::new(1).unwrap()
}
fn paid() -> ResourceVector {
    ResourceVector::new([(dimension(), 1)]).unwrap()
}
fn config() -> ActorConfig {
    ActorConfig {
        mailbox: [2; 4],
        max_batches: 4,
        max_effects_per_step: 1,
        max_dependencies_per_effect: 0,
        turn_budget: 8,
        max_step_work: 1,
    }
}
struct Fixture<A: Actor> {
    runtime: ActorRuntime<A>,
    ledger: RuntimeLedger,
    scope: ScopeHandle,
    work: WorkLease,
    capacity: CapacityBundle,
    roles: Vec<InputRole<A>>,
}
impl<A: Actor> Fixture<A> {
    fn new(value: A) -> Self {
        let ledger = RuntimeLedger::new(paid(), 4, 1);
        let scope = ledger.open_scope_next(None, generation(), 4, 4).unwrap();
        let (mut work, capacity) = scope
            .reserve_next(
                generation(),
                paid(),
                WorkKind::Step,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Step)
            .unwrap();
        work.start().unwrap();
        let runtime = ActorRuntime::new(1);
        runtime
            .register_next(actor(), scope.clone(), value, config())
            .unwrap_or_else(|(error, _)| panic!("actual neutral actor registration: {error:?}"));
        Self {
            runtime,
            ledger,
            scope,
            work,
            capacity,
            roles: vec![],
        }
    }
    fn role(&mut self) {
        self.roles.push(
            self.runtime
                .reserve_input(actor(), ActorId::new(2).unwrap(), InputClass::Control)
                .unwrap()
                .into_role()
                .unwrap(),
        );
    }
    fn finish(mut self) -> A {
        // Cleanup runs before causal assertions, including if an old wrong
        // timer rule delivered an extra real Tick. No input is guessed away.
        self.runtime.close_actor_admission(actor()).unwrap();
        self.scope.close_external().unwrap();
        self.runtime.poll_one(Tick(100)).unwrap();
        for input in self.runtime.input_inventory(actor(), None, 4).unwrap() {
            self.runtime.retire_input(actor(), input.input).unwrap();
        }
        for role in &mut self.roles {
            role.close_after_exit().unwrap();
        }
        while let Some(id) = self.runtime.claim_effect_actor() {
            self.runtime.release_effect_actor(id, false).unwrap();
        }
        self.runtime.seal_actor(actor()).unwrap();
        self.work.body_exited().unwrap();
        self.work.captures_exited().unwrap();
        self.ledger.reap_scope_ready(self.scope.id(), 1).unwrap();
        let value = self.runtime.remove_actor(actor()).unwrap();
        self.capacity
            .release(&self.capacity.resources().unwrap())
            .unwrap();
        self.ledger.remove_scope(self.scope.id()).unwrap();
        assert!(self.runtime.is_empty());
        assert!(self.ledger.is_empty());
        value
    }
}
#[derive(Default)]
struct PauseState {
    entered: bool,
    released: bool,
}
#[derive(Default)]
struct Pause {
    state: Mutex<PauseState>,
    changed: Condvar,
}
impl Pause {
    fn enter(&self) {
        let mut state = self.state.lock().unwrap();
        state.entered = true;
        self.changed.notify_all();
        while !state.released {
            state = self.changed.wait(state).unwrap();
        }
    }
    fn wait_entered(&self) {
        let mut state = self.state.lock().unwrap();
        while !state.entered {
            state = self.changed.wait(state).unwrap();
        }
    }
    fn release(&self) {
        self.state.lock().unwrap().released = true;
        self.changed.notify_all();
    }
}
static SELECTED_TIMER: OnceLock<Arc<Pause>> = OnceLock::new();
struct SelectedActor;
impl Actor for SelectedActor {
    type Input = ();
    type Effect = ();
    fn timer_input(_now: Tick) {
        SELECTED_TIMER.get().unwrap().enter();
    }
    fn handle(
        &mut self,
        _now: Tick,
        _input: InputEnvelope,
        _value: (),
        _step: &mut StepContext<'_, ()>,
    ) {
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
fn source_close_withdraws_a_selected_tick_before_its_actual_input_fill() {
    let pause = Arc::new(Pause::default());
    assert!(SELECTED_TIMER.set(pause.clone()).is_ok());
    let f = Fixture::new(SelectedActor);
    let advancing = f.runtime.clone();
    let producer = std::thread::spawn(move || advancing.advance_time(Tick(1), 1).unwrap());
    pause.wait_entered(); // TimerIndex already selected the original node.
    let closed = f.runtime.close_timer_source(actor()).unwrap();
    let retained_fee = f.ledger.used(dimension());
    let work_still_live = !f.work.actual_exit_ready().unwrap();
    pause.release();
    let filled = producer.join().unwrap();
    let inputs = f.runtime.input_inventory(actor(), None, 2).unwrap().len();
    let state = f.runtime.timer_source_state(actor()).unwrap();
    let deadline = f.runtime.next_deadline();
    f.finish();
    assert_eq!(closed, TimerSourceState::ClosedDrained);
    assert_eq!(filled, 0);
    assert_eq!(inputs, 0);
    assert_eq!(state, TimerSourceState::ClosedDrained);
    assert_eq!(deadline, None);
    assert_eq!(retained_fee, 1);
    assert!(work_still_live, "source closure is not Work exit");
}
struct ReturningDeadline {
    pause: Arc<Pause>,
    deadline: Option<Tick>,
}
impl Actor for ReturningDeadline {
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
        self.pause.enter();
        self.deadline = Some(Tick(10));
    }
    fn next_deadline(&self) -> Option<Tick> {
        self.deadline
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
fn source_close_prevents_an_already_started_core_turn_from_rearming_its_deadline() {
    let pause = Arc::new(Pause::default());
    let mut f = Fixture::new(ReturningDeadline {
        pause: pause.clone(),
        deadline: None,
    });
    f.role();
    let input = f.roles[0].fill(()).unwrap();
    let running = f.runtime.clone();
    let poller = std::thread::spawn(move || running.poll_one(Tick(1)).unwrap());
    pause.wait_entered();
    let closed = f.runtime.close_timer_source(actor()).unwrap();
    pause.release();
    let did_run = poller.join().unwrap();
    let deadline = f.runtime.next_deadline();
    let still_in_flight = f.roles[0].in_flight().unwrap();
    let filled = f.runtime.advance_time(Tick(10), 1).unwrap();
    // A real extra Tick under the old rule is drained by finish as well.
    let fee = f.ledger.used(dimension());
    f.finish();
    assert!(did_run);
    assert_eq!(closed, TimerSourceState::ClosedDrained);
    assert_eq!(deadline, None);
    assert_eq!(still_in_flight, Some(input));
    assert_eq!(filled, 0);
    assert_eq!(fee, 1);
}
struct PlainActor {
    seen: Vec<u64>,
}
impl Actor for PlainActor {
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
fn source_close_keeps_a_filled_tick_pending_until_actual_retirement_and_roles_still_drain() {
    let mut f = Fixture::new(PlainActor { seen: vec![] });
    f.role();
    f.ledger.close_admission();
    f.scope.close_external().unwrap();
    f.runtime.close_actor_admission(actor()).unwrap();
    f.runtime.close_admission();
    let state_before = f.runtime.timer_source_state(actor()).unwrap();
    let filled = f.runtime.advance_time(Tick(1), 1).unwrap();
    let timer = f.runtime.input_inventory(actor(), None, 1).unwrap()[0].input;
    let closed = f.runtime.close_timer_source(actor()).unwrap();
    let role_input = f.roles[0].fill(7).unwrap();
    f.runtime.poll_one(Tick(1)).unwrap();
    let after_core = f.runtime.timer_source_state(actor()).unwrap();
    f.runtime.retire_input(actor(), timer).unwrap();
    let after_retirement = f.runtime.timer_source_state(actor()).unwrap();
    let role_still_live = f.roles[0].in_flight().unwrap();
    f.runtime.retire_input(actor(), role_input).unwrap();
    let repeated = f.runtime.close_timer_source(actor()).unwrap();
    let work_still_live = !f.work.actual_exit_ready().unwrap();
    let actual = f.finish();
    assert_eq!(
        state_before,
        TimerSourceState::Open,
        "external closure does not close builtin timer"
    );
    assert_eq!(filled, 1);
    assert_eq!(closed, TimerSourceState::ClosedPending(timer));
    assert_eq!(after_core, TimerSourceState::ClosedPending(timer));
    assert_eq!(after_retirement, TimerSourceState::ClosedDrained);
    assert_eq!(repeated, TimerSourceState::ClosedDrained);
    assert_eq!(role_still_live, Some(role_input));
    assert!(actual.seen.contains(&1) && actual.seen.contains(&7));
    assert!(work_still_live);
}
