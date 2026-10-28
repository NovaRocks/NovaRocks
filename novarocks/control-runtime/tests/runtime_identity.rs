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
    driver::ActorRuntime,
    transaction::{RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::ResourceVector,
    identity::{ActorId, Generation, ScopeId, Tick},
    mailbox::InputEnvelope,
};

#[derive(Debug)]
struct Neutral;
impl Actor for Neutral {
    type Input = ();
    type Effect = ();
    fn timer_input(_: Tick) {
        unreachable!("no timer in this fixture")
    }
    fn handle(&mut self, _: Tick, _: InputEnvelope, _: (), _: &mut StepContext<'_, ()>) {}
    fn next_deadline(&self) -> Option<Tick> {
        None
    }
    fn owner_failed(_: ScopeId, _: Generation, _: InputEnvelope, _: &mut StepContext<'_, ()>) {
        unreachable!("no failing actor in this fixture")
    }
}

fn actor() -> ActorId {
    ActorId::new(1).unwrap()
}
fn registered_runtime() -> (ActorRuntime<Neutral>, RuntimeLedger, ScopeHandle) {
    let ledger = RuntimeLedger::new(ResourceVector::default(), 1, 1);
    let scope = ledger
        .open_scope_next(None, Generation::new(1).unwrap(), 1, 1)
        .unwrap();
    let runtime = ActorRuntime::new(1);
    runtime
        .register_next(
            actor(),
            scope.clone(),
            Neutral,
            ActorConfig {
                mailbox: [1; 4],
                max_batches: 1,
                max_effects_per_step: 1,
                max_dependencies_per_effect: 0,
                turn_budget: 8,
                max_step_work: 1,
            },
        )
        .unwrap();
    (runtime, ledger, scope)
}
fn finish(runtime: &ActorRuntime<Neutral>, ledger: RuntimeLedger, scope: ScopeHandle) {
    runtime.close_actor_admission(actor()).unwrap();
    scope.close_external().unwrap();
    runtime.seal_actor(actor()).unwrap();
    runtime.remove_actor(actor()).unwrap();
    ledger.remove_scope(scope.id()).unwrap();
    assert!(runtime.is_empty());
    assert!(ledger.is_empty());
}

#[test]
fn cloned_handles_identify_the_same_runtime_instance() {
    let original = ActorRuntime::<Neutral>::new(1);
    let cloned = original.clone();
    assert!(original.same_runtime(&original));
    assert!(original.same_runtime(&cloned));
    assert!(cloned.same_runtime(&original));
    // Instance identity is independent of current admission permissions.
    original.close_admission();
    assert!(original.same_runtime(&cloned));
}

#[test]
fn matching_actor_and_scope_numbers_do_not_identify_independent_runtimes() {
    let (left, left_ledger, left_scope) = registered_runtime();
    let (right, right_ledger, right_scope) = registered_runtime();
    let same_scope_number = left_scope.id() == right_scope.id();
    let same_instance = left.same_runtime(&right);
    let reverse_same_instance = right.same_runtime(&left);
    // Both actors and their real attachment slots retire before assertions.
    finish(&left, left_ledger, left_scope);
    finish(&right, right_ledger, right_scope);
    assert!(same_scope_number);
    assert!(!same_instance);
    assert!(!reverse_same_instance);
}
