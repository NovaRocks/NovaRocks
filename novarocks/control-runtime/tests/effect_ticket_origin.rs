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
    driver::{ActorRuntime, DriverError, EffectTicket},
    lease::{ReservationBundle, WorkLease},
    publication::{PublishedReply, PublishedSnapshot},
    transaction::{RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    effect::{EffectDescriptor, EffectKind},
    identity::*,
    mailbox::{InputClass, InputEnvelope},
    owned_storage::OwnedSlotNeed,
    scope::{WorkAdmission, WorkKind},
};
use std::sync::Arc;

#[derive(Debug)]
struct Publisher {
    work: ScopedWorkRef,
}
impl Actor for Publisher {
    type Input = u64;
    type Effect = u64;
    fn timer_input(now: Tick) -> u64 {
        now.0
    }
    fn handle(
        &mut self,
        _now: Tick,
        _input: InputEnvelope,
        value: u64,
        step: &mut StepContext<'_, u64>,
    ) {
        step.effect(
            EffectDescriptor {
                id: ActionId::new(1).unwrap(),
                work: self.work,
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
    fn owner_failed(
        _scope: ScopeId,
        _generation: Generation,
        _input: InputEnvelope,
        _step: &mut StepContext<'_, u64>,
    ) {
    }
}
fn actor() -> ActorId {
    ActorId::new(1).unwrap()
}
struct Fixture {
    ledger: RuntimeLedger,
    scope: ScopeHandle,
    runtime: ActorRuntime<Publisher>,
    input: InputId,
    work: WorkLease,
    capacity: ReservationBundle,
}
impl Fixture {
    fn new() -> (Self, EffectTicket<u64>) {
        let ledger = RuntimeLedger::new(ResourceVector::new([]).unwrap());
        let scope = ledger
            .open_scope_next(None, Generation::new(1).unwrap())
            .unwrap();
        let (mut work, capacity) = scope
            .reserve_next(
                Generation::new(1).unwrap(),
                ReservationResources {
                    quota: ResourceVector::new([]).unwrap(),
                    owned_slots: OwnedSlotNeed {
                        work_headers: 1,
                        ..OwnedSlotNeed::default()
                    },
                },
                WorkKind::Business,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Business)
            .unwrap();
        work.start().unwrap();
        let runtime = ActorRuntime::new();
        runtime
            .register_next(
                actor(),
                scope.clone(),
                Publisher {
                    work: work.reference(),
                },
                ActorConfig {
                    mailbox: [2; 4],
                    max_batches: 2,
                    max_effects_per_step: 1,
                    max_dependencies_per_effect: 1,
                    turn_budget: 8,
                    max_step_work: 1,
                },
            )
            .unwrap();
        let input = runtime
            .reserve_input(actor(), actor(), InputClass::Ordinary)
            .unwrap()
            .fill(7)
            .unwrap();
        runtime.poll_one(Tick(1)).unwrap();
        let ticket = runtime
            .take_effect(actor(), input, ActionId::new(1).unwrap())
            .unwrap();
        (
            Self {
                ledger,
                scope,
                runtime,
                input,
                work,
                capacity,
            },
            ticket,
        )
    }
    fn finish(self, ticket: EffectTicket<u64>) {
        self.runtime.effect_completed(ticket).unwrap();
        self.finish_completed();
    }
    fn finish_completed(mut self) {
        self.runtime.retire_input(actor(), self.input).unwrap();
        self.scope.close_external().unwrap();
        self.work.body_exited().unwrap();
        self.work.captures_exited().unwrap();
        self.ledger.reap_scope_ready(self.scope.id(), 1).unwrap();
        while let Some(original_host_turn) = self.runtime.claim_host_actor() {
            original_host_turn.release(false).unwrap();
        }
        self.runtime.remove_actor(actor()).unwrap();
        self.capacity.finish().unwrap();
        self.ledger.remove_scope(self.scope.id()).unwrap();
    }
}

#[test]
fn same_numeric_runtime_records_cannot_borrow_foreign_publication_authority() {
    let (first, ticket) = Fixture::new();
    let (second, second_ticket) = Fixture::new();
    assert_eq!(ticket.actor, second_ticket.actor);
    assert_eq!(ticket.input, second_ticket.input);
    assert_eq!(ticket.generation, second_ticket.generation);
    assert_eq!(ticket.descriptor, second_ticket.descriptor);
    let snapshot = PublishedSnapshot::new(&second.scope, Arc::new(0u64));
    let reply = PublishedReply::new(&second.scope);
    let next_snapshot = Arc::new(99u64);
    let next_reply = Arc::new(100u64);
    let snapshot_result = second
        .runtime
        .commit_snapshot(&ticket, &snapshot, next_snapshot.clone());
    let reply_result = second
        .runtime
        .commit_reply(&ticket, &reply, next_reply.clone());
    let validation = second.runtime.validate_effect_ticket(&ticket);
    let reservation = second
        .runtime
        .take_step_reservation(&ticket, ReservationId::new(1).unwrap())
        .err();
    let failure = second.runtime.claim_failure_publication(&ticket).err();
    let external = second.runtime.observe_external_publication(&ticket);
    let snapshot_value = *snapshot.read();
    let reply_committed = reply.is_committed();
    // Even the old wrong rule can be cleaned honestly: its mistaken snapshot
    // publication leaves B Published, and completion accepts Started or Published.
    first.finish(ticket);
    second.finish(second_ticket);
    let (error, returned_snapshot) = snapshot_result.unwrap_err();
    assert_eq!(error, DriverError::EffectTicketOriginMismatch);
    assert!(Arc::ptr_eq(&returned_snapshot, &next_snapshot));
    let (error, returned_reply) = reply_result.unwrap_err();
    assert_eq!(error, DriverError::EffectTicketOriginMismatch);
    assert!(Arc::ptr_eq(&returned_reply, &next_reply));
    assert_eq!(validation, Err(DriverError::EffectTicketOriginMismatch));
    assert_eq!(reservation, Some(DriverError::EffectTicketOriginMismatch));
    assert_eq!(failure, Some(DriverError::EffectTicketOriginMismatch));
    assert_eq!(external, Err(DriverError::EffectTicketOriginMismatch));
    assert_eq!(snapshot_value, 0);
    assert!(!reply_committed);
}

#[test]
fn refused_foreign_completion_returns_original_ticket_to_its_actual_runtime() {
    let (first, ticket) = Fixture::new();
    let (second, second_ticket) = Fixture::new();
    match second.runtime.effect_completed(ticket) {
        Err(refusal) => {
            let error = refusal.error;
            let original_effect = refusal.ticket.effect;
            first
                .runtime
                .validate_effect_ticket(&refusal.ticket)
                .unwrap();
            second
                .runtime
                .validate_effect_ticket(&second_ticket)
                .unwrap();
            first.finish(refusal.ticket);
            second.finish(second_ticket);
            assert_eq!(error, DriverError::EffectTicketOriginMismatch);
            assert_eq!(original_effect, 7);
        }
        Ok(()) => {
            // A broken consuming API has destroyed A's unique authority; it
            // cannot be reconstructed to fake cleanup. Keep that one bounded
            // fixture, clean the actually completed B, then fail normally rather
            // than accepting a strict Inbox::drop abort as the regression signal.
            std::mem::forget(first);
            drop(second_ticket);
            second.finish_completed();
            panic!("foreign completion consumed the original runtime's unique ticket");
        }
    }
}

#[test]
fn current_validation_does_not_reject_original_late_external_fact_or_exit() {
    let (fixture, ticket) = Fixture::new();
    fixture.scope.fail().unwrap();
    let current = fixture.runtime.validate_effect_ticket(&ticket);
    let external = fixture.runtime.observe_external_publication(&ticket);
    fixture.finish(ticket);
    assert_eq!(current, Err(DriverError::FailurePublicationRequired));
    assert_eq!(external, Ok(()));
}
