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

use super::*;
use crate::{
    lease::{CapacityBundle, WorkLease},
    transaction::RuntimeLedger,
};
use novarocks_control_runtime_core::{
    capacity::ResourceVector, identity::DimensionId, scope::WorkAdmission,
};

#[derive(Debug)]
struct Original(WorkId);
impl Actor for Original {
    type Input = u64;
    type Effect = u64;
    fn timer_input(_: Tick) -> u64 {
        unreachable!("this actor has no timer")
    }
    fn handle(&mut self, _: Tick, _: InputEnvelope, value: u64, step: &mut StepContext<'_, u64>) {
        step.effect(
            EffectDescriptor {
                id: ActionId::new(1).unwrap(),
                work: self.0,
                kind: EffectKind::Publication,
                dependencies: vec![],
            },
            value,
        )
        .unwrap();
    }
    fn next_deadline(&self) -> Option<Tick> {
        None
    }
    fn owner_failed(_: ScopeId, _: Generation, _: InputEnvelope, _: &mut StepContext<'_, u64>) {
        unreachable!("the pure actor does not panic")
    }
}

fn paid() -> ResourceVector {
    ResourceVector::new([(DimensionId::new(1).unwrap(), 2)]).unwrap()
}
fn actor() -> ActorId {
    ActorId::new(1).unwrap()
}
struct Fixture {
    ledger: RuntimeLedger,
    runtime: ActorRuntime<Original>,
    scope: ScopeHandle,
    registration: ActorRegistration,
    work: WorkLease,
    capacity: CapacityBundle,
    input: InputId,
}
impl Fixture {
    fn new(ledger: RuntimeLedger, runtime: ActorRuntime<Original>) -> (Self, EffectTicket<u64>) {
        let generation = Generation::new(1).unwrap();
        let scope = ledger.open_scope_next(None, generation, 8, 8).unwrap();
        let (mut work, capacity) = scope
            .reserve_next(
                generation,
                paid(),
                WorkKind::Cleanup,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Cleanup)
            .unwrap();
        work.start().unwrap();
        let registration = runtime
            .register_supervised_bound(
                actor(),
                scope.clone(),
                Original(work.id()),
                ActorConfig {
                    mailbox: [1; 4],
                    max_batches: 1,
                    max_effects_per_step: 1,
                    max_dependencies_per_effect: 0,
                    turn_budget: 8,
                    max_step_work: 1,
                },
                &work,
            )
            .unwrap();
        let input = runtime
            .reserve_input(actor(), actor(), InputClass::Ordinary)
            .unwrap()
            .fill(17)
            .unwrap();
        runtime.poll_one(Tick(1)).unwrap();
        let ticket = runtime
            .take_effect(actor(), input, ActionId::new(1).unwrap())
            .unwrap();
        (
            Self {
                ledger,
                runtime,
                scope,
                registration,
                work,
                capacity,
                input,
            },
            ticket,
        )
    }
    fn independent() -> (Self, EffectTicket<u64>) {
        Self::new(RuntimeLedger::new(paid(), 8, 8), ActorRuntime::new(1))
    }
    fn finish(mut self, ticket: EffectTicket<u64>) {
        self.runtime
            .effect_completed_for(&self.registration, ticket)
            .unwrap();
        self.runtime.retire_input(actor(), self.input).unwrap();
        self.scope.close_external().unwrap();
        self.work.body_exited().unwrap();
        self.work.captures_exited().unwrap();
        self.ledger.reap_scope_ready(self.scope.id(), 1).unwrap();
        while let Some(original_host_turn) = self.runtime.claim_host_actor() {
            original_host_turn.release(false).unwrap();
        }
        self.runtime.remove_actor(actor()).unwrap();
        self.capacity
            .release(&self.capacity.resources().unwrap())
            .unwrap();
        self.ledger.remove_scope(self.scope.id()).unwrap();
        assert!(self.runtime.is_empty());
        assert!(self.ledger.is_empty());
    }
}

#[test]
fn retired_registration_cannot_complete_reused_actor_in_the_same_runtime() {
    let ledger = RuntimeLedger::new(paid(), 8, 8);
    let runtime = ActorRuntime::new(1);
    let (old, old_ticket) = Fixture::new(ledger.clone(), runtime.clone());
    let witness = old.registration.clone();
    old.finish(old_ticket);
    let (current, ticket) = Fixture::new(ledger, runtime);
    let different_scope = witness.scope_id() != current.registration.scope_id();
    let different_attachment = witness.attachment() != current.registration.attachment();
    let expected_origin = ticket.origin.clone();
    let expected_input = ticket.input;
    let refusal = current
        .runtime
        .effect_completed_for(&witness, ticket)
        .unwrap_err();
    let same_ticket = Arc::ptr_eq(&expected_origin, &refusal.ticket.origin)
        && refusal.ticket.input == expected_input
        && refusal.ticket.effect == 17;
    let error = refusal.error;
    current.finish(refusal.ticket);
    assert_eq!(witness.actor(), actor());
    assert!(different_scope && different_attachment);
    assert_eq!(error, DriverError::EffectTicketOriginMismatch);
    assert!(same_ticket);
}

#[test]
fn foreign_registration_with_equal_numeric_metadata_preserves_the_original_ticket() {
    let (first, ticket) = Fixture::independent();
    let (second, second_ticket) = Fixture::independent();
    let numeric_collision = first.registration.actor() == second.registration.actor()
        && first.registration.attachment() == second.registration.attachment()
        && first.registration.scope_id() == second.registration.scope_id();
    // Use the ticket's actual runtime: the additional registered-owner check
    // must refuse even though the runtime's ordinary origin check would pass.
    let refusal = first
        .runtime
        .effect_completed_for(&second.registration, ticket)
        .unwrap_err();
    let error = refusal.error;
    let original_value = refusal.ticket.effect;
    first.finish(refusal.ticket);
    second.finish(second_ticket);
    assert!(numeric_collision);
    assert_eq!(error, DriverError::EffectTicketOriginMismatch);
    assert_eq!(original_value, 17);
}

#[test]
fn original_registration_can_complete_a_physical_fact_after_scope_failure() {
    let (fixture, ticket) = Fixture::independent();
    let original_generation = ticket.generation;
    fixture.scope.fail().unwrap();
    let failed_generation = fixture.scope.snapshot().unwrap().generation;
    let current_publication = fixture.runtime.validate_effect_ticket(&ticket);
    fixture.finish(ticket);
    assert_ne!(original_generation, failed_generation);
    assert_eq!(
        current_publication,
        Err(DriverError::Effect(EffectError::StaleGeneration))
    );
}
