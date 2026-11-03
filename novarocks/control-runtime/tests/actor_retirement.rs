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
    transaction::RuntimeLedger,
};
use novarocks_control_runtime_core::{
    capacity::ResourceVector,
    identity::{ActorId, Generation, ScopeId, Tick},
    mailbox::{InputClass, InputEnvelope},
};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::Duration;

fn actor() -> ActorId {
    ActorId::new(1).unwrap()
}
fn config() -> ActorConfig {
    ActorConfig {
        mailbox: [4; 4],
        max_batches: 4,
        max_effects_per_step: 1,
        max_dependencies_per_effect: 0,
        turn_budget: 8,
        max_step_work: 1,
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
fn quiet<A: Actor>(runtime: &ActorRuntime<A>) {
    while let Some(original_host_turn) = runtime.claim_host_actor() {
        original_host_turn.release(false).unwrap();
    }
}
#[test]
fn one_actor_closes_new_positions_but_drains_both_prepaid_input_kinds_before_seal() {
    let ledger = RuntimeLedger::new(ResourceVector::default());
    let scope = ledger
        .open_scope_next(None, Generation::new(1).unwrap())
        .unwrap();
    let other = ledger
        .open_scope_next(None, Generation::new(1).unwrap())
        .unwrap();
    let runtime = ActorRuntime::new();
    runtime
        .register_next(actor(), scope.clone(), Recorder::default(), config())
        .unwrap();
    let peer = ActorId::new(2).unwrap();
    runtime
        .register_next(peer, other.clone(), Recorder::default(), config())
        .unwrap();
    let original = runtime
        .reserve_input(actor(), peer, InputClass::Ordinary)
        .unwrap();
    let mut role = runtime
        .reserve_input(actor(), peer, InputClass::Lifecycle)
        .unwrap()
        .into_standing_input()
        .unwrap();
    runtime.close_actor_admission(actor()).unwrap();
    assert_eq!(
        runtime
            .reserve_input(actor(), peer, InputClass::Ordinary)
            .err(),
        Some(ActorTurnError::Closed)
    );
    assert_eq!(
        runtime.seal_actor(actor()),
        Err(ActorTurnError::ActorNotRetired)
    );
    let input = original.fill(1).unwrap();
    let lifecycle = role.fill(2).unwrap();
    runtime.poll_one(Tick(1)).unwrap();
    runtime.retire_input(actor(), input).unwrap();
    runtime.retire_input(actor(), lifecycle).unwrap();
    role.close_after_exit().unwrap();
    quiet(&runtime);
    runtime.seal_actor(actor()).unwrap();
    assert!(runtime.snapshot(actor()).unwrap().sealed);
    // A closed actor does not close its peer or the process admission gate.
    let input = runtime
        .reserve_input(peer, actor(), InputClass::Ordinary)
        .unwrap()
        .fill(3)
        .unwrap();
    runtime.poll_one(Tick(1)).unwrap();
    runtime.retire_input(peer, input).unwrap();
    quiet(&runtime);
    scope.close_external().unwrap();
    other.close_external().unwrap();
    assert_eq!(runtime.remove_actor(actor()).unwrap().0, vec![1, 2]);
    assert_eq!(runtime.remove_actor(peer).unwrap().0, vec![3]);
    ledger.remove_scope(scope.id()).unwrap();
    ledger.remove_scope(other.id()).unwrap();
    assert!(runtime.is_empty());
    assert!(ledger.is_empty());
}

#[derive(Default)]
struct Pause {
    selected: bool,
    released: bool,
}
static TIMER_PAUSE: OnceLock<Arc<(Mutex<Pause>, Condvar)>> = OnceLock::new();
#[derive(Debug)]
struct PausedTimer;
impl Actor for PausedTimer {
    type Input = ();
    type Effect = ();
    fn timer_input(_: Tick) {
        // Deliberately pause after the real timer selection and node lookup.
        // A production thread can be descheduled at exactly this boundary.
        let pause = TIMER_PAUSE.get().unwrap();
        let mut state = pause.0.lock().unwrap();
        state.selected = true;
        pause.1.notify_all();
        while !state.released {
            state = pause.1.wait(state).unwrap();
        }
    }
    fn handle(&mut self, _: Tick, _: InputEnvelope, _: (), _: &mut ActorStep<'_, ()>) {
        panic!("a retired timer cannot execute");
    }
    fn next_deadline(&self) -> Option<Tick> {
        Some(Tick(1))
    }
    fn owner_failed(_: ScopeId, _: Generation, _: InputEnvelope, _: &mut ActorStep<'_, ()>) {
        panic!("retirement cannot fail a healthy owner");
    }
}
struct TimerThread {
    handle: Option<std::thread::JoinHandle<Result<usize, ActorTurnError>>>,
    pause: Arc<(Mutex<Pause>, Condvar)>,
}
impl TimerThread {
    fn release(&mut self) -> std::thread::Result<Result<usize, ActorTurnError>> {
        self.pause.0.lock().unwrap().released = true;
        self.pause.1.notify_all();
        self.handle.take().unwrap().join()
    }
}
impl Drop for TimerThread {
    fn drop(&mut self) {
        if self.handle.is_some() {
            let _ = self.release();
        }
    }
}
#[test]
fn an_already_selected_timer_cannot_resurrect_a_removed_actor_or_poison_the_poller() {
    let pause = Arc::new((Mutex::new(Pause::default()), Condvar::new()));
    assert!(TIMER_PAUSE.set(pause.clone()).is_ok());
    let ledger = RuntimeLedger::new(ResourceVector::default());
    let scope = ledger
        .open_scope_next(None, Generation::new(1).unwrap())
        .unwrap();
    let runtime = ActorRuntime::new();
    runtime
        .register_next(actor(), scope.clone(), PausedTimer, config())
        .unwrap();
    let ticking = runtime.clone();
    let mut thread = TimerThread {
        handle: Some(std::thread::spawn(move || ticking.advance_time(Tick(1), 1))),
        pause: pause.clone(),
    };
    let (state, timeout) = pause
        .1
        .wait_timeout_while(pause.0.lock().unwrap(), Duration::from_secs(2), |state| {
            !state.selected
        })
        .unwrap();
    let selected = state.selected;
    drop(state);
    assert!(
        selected && !timeout.timed_out(),
        "timer reached its original node before retirement"
    );
    runtime.close_actor_admission(actor()).unwrap();
    runtime.seal_actor(actor()).unwrap();
    scope.close_external().unwrap();
    runtime.remove_actor(actor()).unwrap();
    ledger.remove_scope(scope.id()).unwrap();
    assert_eq!(thread.release().unwrap().unwrap(), 0);
    assert!(runtime.is_empty());
    assert!(ledger.is_empty());
}
