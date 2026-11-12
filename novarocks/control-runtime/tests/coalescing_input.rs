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
    fixture_with_class(InputClass::Ordinary)
}
fn fixture_with_class(
    class: InputClass,
) -> (
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
        .reserve_input(actor(), ActorId::new(2).unwrap(), class)
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

#[test]
fn in_flight_updates_merge_and_refill_only_after_actual_retirement() {
    let (ledger, scope, runtime, input) = fixture();
    let first = input.notify(Signal(1)).unwrap().unwrap();
    assert_eq!(input.notify(Signal(2)).unwrap(), None);
    assert_eq!(input.notify(Signal(3)).unwrap(), None);
    assert_eq!(input.in_flight().unwrap(), Some(first));
    runtime.poll_one(Tick(1)).unwrap();
    runtime.retire_input(actor(), first).unwrap();
    let second = input.after_retire().unwrap().unwrap();
    assert!(second > first);
    runtime.poll_one(Tick(2)).unwrap();
    runtime.retire_input(actor(), second).unwrap();
    assert_eq!(input.after_retire().unwrap(), None);
    assert_eq!(finish(ledger, scope, runtime, input), vec![1, 5]);
}
#[test]
fn waker_coalesces_and_close_discards_updates_without_releasing_a_live_input() {
    let (ledger, scope, runtime, input) = fixture();
    let waker = input.waker();
    waker.wake_by_ref();
    let first = input.in_flight().unwrap().unwrap();
    waker.wake_by_ref();
    waker.wake_by_ref();
    assert_eq!(
        input.close_after_exit(),
        Err(ActorTurnError::StandingInputBusy)
    );
    assert_eq!(input.notify(Signal(8)), Err(ActorTurnError::Closed));
    waker.wake_by_ref();
    runtime.poll_one(Tick(1)).unwrap();
    runtime.retire_input(actor(), first).unwrap();
    assert_eq!(input.after_retire().unwrap(), None);
    drop(waker);
    assert_eq!(finish(ledger, scope, runtime, input), vec![1]);
}

#[test]
fn busy_refill_retains_the_original_update_and_waits_for_real_retirement() {
    let (ledger, scope, runtime, input) = fixture();
    let first = input.notify(Signal(1)).unwrap().unwrap();
    assert_eq!(input.notify(Signal(2)).unwrap(), None);
    // An erroneous early host callback cannot lose the update or duplicate a
    // live standing occurrence; the actual retire below remains authoritative.
    assert_eq!(input.after_retire(), Err(ActorTurnError::StandingInputBusy));
    assert_eq!(input.notify(Signal(3)).unwrap(), None);
    assert_eq!(input.in_flight().unwrap(), Some(first));
    runtime.poll_one(Tick(1)).unwrap();
    runtime.retire_input(actor(), first).unwrap();
    let second = input.after_retire().unwrap().unwrap();
    runtime.poll_one(Tick(2)).unwrap();
    runtime.retire_input(actor(), second).unwrap();
    input.after_retire().unwrap();
    assert_eq!(finish(ledger, scope, runtime, input), vec![1, 5]);
}

#[test]
fn original_source_can_choose_a_prepaid_lifecycle_position() {
    let (ledger, scope, runtime, input) = fixture_with_class(InputClass::Lifecycle);
    let first = input.notify(Signal(1)).unwrap().unwrap();
    input.notify(Signal(2)).unwrap();
    runtime.poll_one(Tick(1)).unwrap();
    runtime.retire_input(actor(), first).unwrap();
    let second = input.after_retire().unwrap().unwrap();
    runtime.poll_one(Tick(2)).unwrap();
    runtime.retire_input(actor(), second).unwrap();
    input.after_retire().unwrap();
    assert_eq!(finish(ledger, scope, runtime, input), vec![1, 2]);
}
