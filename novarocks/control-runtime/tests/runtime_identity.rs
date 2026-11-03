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
    actor_turn::ActorRuntime,
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
    fn handle(&mut self, _: Tick, _: InputEnvelope, _: (), _: &mut ActorStep<'_, ()>) {}
    fn next_deadline(&self) -> Option<Tick> {
        None
    }
    fn owner_failed(_: ScopeId, _: Generation, _: InputEnvelope, _: &mut ActorStep<'_, ()>) {
        unreachable!("no failing actor in this fixture")
    }
}

fn actor() -> ActorId {
    ActorId::new(1).unwrap()
}
fn registered_runtime() -> (ActorRuntime<Neutral>, RuntimeLedger, ScopeHandle) {
    let ledger = RuntimeLedger::new(ResourceVector::default());
    let scope = ledger
        .open_scope_next(None, Generation::new(1).unwrap())
        .unwrap();
    let runtime = ActorRuntime::new();
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
    let original = ActorRuntime::<Neutral>::new();
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

#[derive(Debug)]
struct FailingIdentityActor;
impl Actor for FailingIdentityActor {
    type Input = bool;
    type Effect = ();
    fn timer_input(_: Tick) -> bool {
        unreachable!("no timer in this identity scenario")
    }
    fn handle(&mut self, _: Tick, _: InputEnvelope, fail: bool, _: &mut ActorStep<'_, ()>) {
        assert!(!fail, "the original identity owner fails");
    }
    fn next_deadline(&self) -> Option<Tick> {
        None
    }
    fn owner_failed(_: ScopeId, _: Generation, _: InputEnvelope, _: &mut ActorStep<'_, ()>) {
        // This actor owns no I/O, publication, callback or Work. Its original
        // neutral inputs and panic payload still require actual custody exit.
    }
}

#[test]
fn failed_owner_input_handles_cannot_access_positions_in_a_replacement_actor() {
    use novarocks_control_runtime::actor_turn::ActorTurnError;
    use novarocks_control_runtime_core::mailbox::InputClass;

    let ledger = RuntimeLedger::new(ResourceVector::default());
    let runtime = ActorRuntime::new();
    let config = ActorConfig {
        mailbox: [8; 4],
        max_batches: 2,
        max_effects_per_step: 1,
        max_dependencies_per_effect: 0,
        turn_budget: 8,
        max_step_work: 1,
    };
    let original_scope = ledger
        .open_scope_next(None, Generation::new(1).unwrap())
        .unwrap();
    runtime
        .register_next(
            actor(),
            original_scope.clone(),
            FailingIdentityActor,
            config,
        )
        .unwrap();
    let reserve = || {
        runtime
            .reserve_input(actor(), actor(), InputClass::Control)
            .unwrap()
    };
    let stale_fill = reserve();
    let stale_convert = reserve();
    let stale_revoke = reserve();
    let mut stale_role = reserve().into_standing_input().unwrap();
    let failed = reserve().fill(true).unwrap();
    assert!(runtime.poll_one(Tick(1)).unwrap());
    assert!(runtime.snapshot(actor()).unwrap().failed);
    for id in [stale_fill.id(), stale_convert.id(), stale_revoke.id()] {
        runtime.abandon_failed_input(actor(), id).unwrap();
        runtime.retire_input(actor(), id).unwrap();
    }
    // The failed source has actually ended. Its failure custodian closes the
    // lost role, while its old Rust handle can still be held by that custodian.
    runtime
        .close_failed_standing_input_after_exit(actor(), stale_role.id())
        .unwrap();
    runtime.retire_input(actor(), failed).unwrap();
    for input in runtime.pending_batches(actor(), None, 2).unwrap() {
        runtime.retire_input(actor(), input).unwrap();
    }
    drop(runtime.take_failure_payload(actor()).unwrap().unwrap());
    runtime.close_actor_admission(actor()).unwrap();
    let host = runtime.claim_host_actor().unwrap();
    host.retire_actor().unwrap();
    ledger.remove_scope(original_scope.id()).unwrap();

    let replacement_scope = ledger
        .open_scope_next(None, Generation::new(1).unwrap())
        .unwrap();
    runtime
        .register_next(
            actor(),
            replacement_scope.clone(),
            FailingIdentityActor,
            config,
        )
        .unwrap();
    let current_fill = reserve();
    let current_convert = reserve();
    let current_revoke = reserve();
    let mut current_role = reserve().into_standing_input().unwrap();
    assert_eq!(stale_fill.id(), current_fill.id());
    assert_eq!(stale_convert.id(), current_convert.id());
    assert_eq!(stale_revoke.id(), current_revoke.id());
    assert_eq!(stale_role.id(), current_role.id());
    let current = runtime.snapshot(actor()).unwrap();
    assert_eq!(
        stale_fill.fill(false).unwrap_err(),
        (ActorTurnError::EffectTicketOriginMismatch, false)
    );
    assert_eq!(
        stale_convert.into_standing_input().unwrap_err().0,
        ActorTurnError::EffectTicketOriginMismatch
    );
    assert_eq!(
        stale_revoke.revoke_unused(),
        Err(ActorTurnError::EffectTicketOriginMismatch)
    );
    assert_eq!(
        stale_role.author().unwrap_err(),
        ActorTurnError::EffectTicketOriginMismatch
    );
    assert_eq!(
        stale_role.in_flight(),
        Err(ActorTurnError::EffectTicketOriginMismatch)
    );
    assert_eq!(
        stale_role.fill(false).unwrap_err(),
        (ActorTurnError::EffectTicketOriginMismatch, false)
    );
    assert_eq!(
        stale_role.close_after_exit(),
        Err(ActorTurnError::EffectTicketOriginMismatch)
    );
    assert_eq!(runtime.snapshot(actor()).unwrap(), current);
    current_fill.revoke_unused().unwrap();
    current_convert.revoke_unused().unwrap();
    current_revoke.revoke_unused().unwrap();
    current_role.close_after_exit().unwrap();
    runtime.close_actor_admission(actor()).unwrap();
    replacement_scope.close_external().unwrap();
    runtime.seal_actor(actor()).unwrap();
    runtime.remove_actor(actor()).unwrap();
    ledger.remove_scope(replacement_scope.id()).unwrap();
    assert!(runtime.is_empty());
    assert!(ledger.is_empty());
}
