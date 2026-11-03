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
    obligation::{ReservationObligation, WorkObligation},
    transaction::RuntimeLedger,
};
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    identity::DimensionId,
    owned_storage::OwnedSlotNeed,
    scope::WorkAdmission,
};

#[derive(Debug)]
struct Original(ScopedWorkRef);
impl Actor for Original {
    type Input = u64;
    type Effect = u64;
    fn timer_input(_: Tick) -> u64 {
        unreachable!("this actor has no timer")
    }
    fn handle(&mut self, _: Tick, _: InputEnvelope, value: u64, step: &mut ActorStep<'_, u64>) {
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
    fn owner_failed(_: ScopeId, _: Generation, _: InputEnvelope, _: &mut ActorStep<'_, u64>) {
        unreachable!("the pure actor does not panic")
    }
}

fn paid() -> ResourceVector {
    ResourceVector::new([(DimensionId::new(1).unwrap(), 2)]).unwrap()
}
fn work_resources(quota: ResourceVector) -> ReservationResources {
    ReservationResources {
        quota,
        owned_slots: OwnedSlotNeed {
            work_headers: 1,
            ..OwnedSlotNeed::default()
        },
    }
}
fn request_resources() -> ReservationResources {
    ReservationResources {
        quota: paid(),
        owned_slots: OwnedSlotNeed {
            work_headers: 1,
            inputs: 1,
            batches: 1,
            effects: 1,
            replies: 1,
            ..OwnedSlotNeed::default()
        },
    }
}
fn actor() -> ActorId {
    ActorId::new(1).unwrap()
}
struct Fixture {
    ledger: RuntimeLedger,
    runtime: ActorRuntime<Original>,
    scope: ScopeHandle,
    registration: ActorRegistration,
    work: WorkObligation,
    capacity: ReservationObligation,
    input: InputId,
}
impl Fixture {
    fn new(ledger: RuntimeLedger, runtime: ActorRuntime<Original>) -> (Self, EffectTicket<u64>) {
        let generation = Generation::new(1).unwrap();
        let scope = ledger.open_scope_next(None, generation).unwrap();
        let (mut work, capacity) = scope
            .reserve_next(
                generation,
                work_resources(paid()),
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
                Original(work.reference()),
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
        Self::new(RuntimeLedger::new(paid()), ActorRuntime::new())
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
            .release_quota(&self.capacity.resources().unwrap().quota)
            .unwrap();
        self.capacity.finish().unwrap();
        self.ledger.remove_scope(self.scope.id()).unwrap();
        assert!(self.runtime.is_empty());
        assert!(self.ledger.is_empty());
    }
}

#[test]
fn retired_registration_cannot_complete_reused_actor_in_the_same_runtime() {
    let ledger = RuntimeLedger::new(paid());
    let runtime = ActorRuntime::new();
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
    assert_eq!(error, ActorTurnError::EffectTicketOriginMismatch);
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
    assert_eq!(error, ActorTurnError::EffectTicketOriginMismatch);
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
        Err(ActorTurnError::FailurePublicationRequired)
    );
}

#[derive(Debug)]
enum RetainedInput {
    Read(u64),
    Tick,
    Expired(u64),
}
#[derive(Debug)]
struct RetainedActor {
    deadline: Option<Tick>,
    consumed_expiry: Option<u64>,
}
impl Actor for RetainedActor {
    type Input = RetainedInput;
    type Effect = u64;
    fn timer_input(_: Tick) -> Self::Input {
        RetainedInput::Tick
    }
    fn handle(
        &mut self,
        _: Tick,
        _: InputEnvelope,
        input: Self::Input,
        step: &mut ActorStep<'_, Self::Effect>,
    ) {
        match input {
            RetainedInput::Read(value) => step
                .effect(
                    EffectDescriptor {
                        id: ActionId::new(1).unwrap(),
                        work: step.input_work().expect("current original request"),
                        kind: EffectKind::Reply,
                        dependencies: vec![],
                    },
                    value,
                )
                .unwrap(),
            RetainedInput::Tick => {
                self.deadline = None;
                assert!(step.input_work().is_none());
                step.metadata(73).unwrap();
            }
            RetainedInput::Expired(original) => self.consumed_expiry = Some(original),
        }
    }
    fn next_deadline(&self) -> Option<Tick> {
        self.deadline
    }
    fn owner_failed(_: ScopeId, _: Generation, _: InputEnvelope, _: &mut ActorStep<'_, u64>) {
        unreachable!("this pure protocol does not panic")
    }
}

#[test]
fn retained_registration_releases_execution_and_keeps_current_request_and_expiry_custody() {
    let generation = Generation::new(1).unwrap();
    let ledger = RuntimeLedger::new(paid());
    let runtime = ActorRuntime::new();
    let process = ledger.open_scope_next(None, generation).unwrap();
    let (mut control_work, mut control_capacity) = process
        .reserve_next(
            generation,
            work_resources(ResourceVector::default()),
            WorkKind::Cleanup,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Cleanup)
        .unwrap();
    control_work.start().unwrap();
    let control = ActorId::new(2).unwrap();
    let config = ActorConfig {
        mailbox: [1; 4],
        max_batches: 1,
        max_effects_per_step: 1,
        max_dependencies_per_effect: 0,
        turn_budget: 8,
        max_step_work: 1,
    };
    let control_registration = runtime
        .register_supervised_bound(
            control,
            process.clone(),
            RetainedActor {
                deadline: None,
                consumed_expiry: None,
            },
            config,
            &control_work,
        )
        .unwrap();
    let execution = ledger
        .open_scope_next(
            Some(crate::transaction::ScopeParentAdmission {
                scope: process.id(),
                generation,
                kind: WorkKind::Cleanup,
                admission: WorkAdmission::Derived(control_work.id()),
            }),
            generation,
        )
        .unwrap();
    let (mut original, mut original_capacity) = execution
        .reserve_next(
            generation,
            work_resources(paid()),
            WorkKind::Cleanup,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Cleanup)
        .unwrap();
    original.start().unwrap();
    let registration = runtime
        .register_supervised_bound(
            actor(),
            execution.clone(),
            RetainedActor {
                deadline: Some(Tick(10)),
                consumed_expiry: None,
            },
            config,
            &original,
        )
        .unwrap();
    let witness = registration.clone();
    runtime.notify_host_actor(&registration).unwrap();
    let mut host = runtime.claim_host_actor().unwrap();
    assert_eq!(
        host.detach_execution(&process),
        Err(ActorTurnError::ActorNotRetired)
    );
    execution.close_external().unwrap();
    original.body_exited().unwrap();
    original.captures_exited().unwrap();
    ledger.reap_scope_ready(execution.id(), 1).unwrap();
    original_capacity.release_quota(&paid()).unwrap();
    original_capacity.finish().unwrap();
    host.detach_execution(&process).unwrap();
    assert!(registration.same_registration(&witness));
    assert_eq!(registration.scope_id(), execution.id());
    assert!(!registration.matches_scope(&execution));
    runtime.validate_registration(&witness).unwrap();
    host.release(false).unwrap();
    ledger.remove_scope(execution.id()).unwrap();
    ledger.reap_scope_ready(process.id(), 1).unwrap();
    assert_eq!(ledger.used(DimensionId::new(1).unwrap()), 0);
    assert!(matches!(
        runtime.reserve_input(actor(), actor(), InputClass::Ordinary),
        Err(ActorTurnError::RetainedInputRequired)
    ));

    let (mut request, mut request_capacity) = process
        .reserve_child_next(
            generation,
            control_capacity.id(),
            request_resources(),
            WorkKind::Settlement,
        )
        .unwrap()
        .bind(WorkKind::Settlement)
        .unwrap();
    request.start().unwrap();
    let permit = runtime
        .reserve_retained_input(&registration, actor(), InputClass::Ordinary, &request)
        .unwrap();
    let reply = crate::publication::PublishedReply::new_for_input(&permit.author().unwrap());
    let input = permit.fill(RetainedInput::Read(29)).unwrap();
    runtime.poll_one(Tick(2)).unwrap();
    let ticket = runtime
        .take_effect(actor(), input, ActionId::new(1).unwrap())
        .unwrap();
    assert_eq!(ticket.descriptor.work, request.reference());
    runtime.commit_reply(&ticket, &reply, Arc::new(29)).unwrap();
    assert_eq!(reply.take_once().as_deref(), Some(&29));
    runtime.effect_completed_for(&registration, ticket).unwrap();
    runtime.retire_input(actor(), input).unwrap();
    request.body_exited().unwrap();
    request.captures_exited().unwrap();
    ledger.reap_scope_ready(process.id(), 1).unwrap();
    request_capacity.release_quota(&paid()).unwrap();
    request_capacity.finish().unwrap();

    assert_eq!(runtime.advance_time(Tick(10), 2).unwrap(), 1);
    let tick = runtime.pending_timer_input(actor()).unwrap().unwrap();
    runtime.poll_one(Tick(10)).unwrap();
    let mut host = runtime.claim_host_actor().unwrap();
    assert_eq!(host.actor(), actor());
    let notice = host.take_metadata(tick).unwrap().unwrap();
    assert_eq!(*notice.value(), 73);
    assert_eq!(
        runtime.retire_input(actor(), tick),
        Err(ActorTurnError::MetadataStillPending)
    );
    let delivery = runtime
        .forward_metadata_for(
            &registration,
            &control_registration,
            &notice,
            InputClass::Lifecycle,
            RetainedInput::Expired(*notice.value()),
        )
        .unwrap();
    runtime.poll_one(Tick(10)).unwrap();
    assert!(
        delivery
            .target_registration()
            .same_registration(&control_registration)
    );
    assert_eq!(delivery.consumption(), MetadataConsumption::Consumed);
    runtime
        .metadata_completed_for(&registration, notice)
        .unwrap();
    runtime.retire_input(actor(), tick).unwrap();
    runtime.close_actor_admission(actor()).unwrap();
    // This original host turn still owns its scheduler claim. Its atomic
    // retire_actor path seals under that claim after checking all custody.
    // The process's actual Control work is still running. A retained Context
    // has no execution join debt to it, and its original expiry was consumed.
    let retired = host.retire_actor().unwrap();
    assert!(retired.deadline.is_none());
    assert!(!process.join_ready().unwrap());

    // The actual Control consumption queued its own host-ready notice.
    // Consume that real host turn before using the unclaimed seal API.
    while let Some(turn) = runtime.claim_host_actor() {
        assert_eq!(turn.actor(), control);
        turn.release(false).unwrap();
    }
    runtime.close_actor_admission(control).unwrap();
    runtime.seal_actor(control).unwrap();
    process.close_external().unwrap();
    control_work.body_exited().unwrap();
    control_work.captures_exited().unwrap();
    ledger.reap_scope_ready(process.id(), 1).unwrap();
    let control = runtime.remove_actor(control).unwrap();
    assert_eq!(control.consumed_expiry, Some(73));
    control_capacity.finish().unwrap();
    ledger.remove_scope(process.id()).unwrap();
    assert!(runtime.is_empty());
    assert!(ledger.is_empty());
}
