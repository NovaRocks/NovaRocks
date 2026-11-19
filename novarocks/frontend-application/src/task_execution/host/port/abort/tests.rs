// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.
use super::*;
use crate::{
    control_runtime::{actor::TestActor, logical::adapter::LogicalEffectEnvelope},
    task_execution::operation::OperationDispatchResult,
};
use novarocks_attempt_protocol::{
    input::SendGeneration,
    port_projection::{AbortProjectionError, project_abort_settlement},
};
use novarocks_control_runtime::{
    actor::{ActorConfig, ActorStep},
    actor_turn::{FailureSourceState, TimerSourceState},
    obligation::{ReservationObligation, WorkObligation},
    transaction::{RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    effect::{EffectDescriptor, EffectKind},
    identity::{ActionId, ActorId, Generation, ReservationId, ScopedWorkRef, Tick},
    mailbox::{InputClass, InputEnvelope},
    owned_storage::OwnedSlotNeed,
};
use novarocks_coordination_contract::logical_port::AbortWorkerClosure;
use novarocks_coordination_contract::{IdSeed, OperationIds, logical_port as lp};
use novarocks_execution_contract::{
    AbortCause, AbortParticipation, OperationOutcome, ParticipationReceipt, ParticipationState,
};
use novarocks_logical_execution_protocol::{LogicalActionId, effect::LogicalEffect};
use novarocks_types::{AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId};
use std::num::NonZeroU64;
fn action(n: u64) -> AttemptActionId {
    AttemptActionId::new(NonZeroU64::new(n).unwrap())
}
fn operation(n: u128) -> TaskOperationId {
    OperationIds::new(IdSeed::new(n, 17)).issue()
}
fn part(n: u128) -> ParticipationRef {
    ParticipationRef::new(
        QueryExecutionId::new(QueryId::new(41, 43), AttemptId::new(1).unwrap()).unwrap(),
        FrontendProcessId::try_from_bytes(operation(10).to_bytes()).unwrap(),
        BackendProcessId::try_from_bytes(operation(n).to_bytes()).unwrap(),
    )
}
fn activation() -> AttemptActivationIdentity {
    AttemptActivationIdentity::from_parts(71, part(11).query_execution_id(), 8)
}
fn request() -> AbortParticipation {
    AbortParticipation::new(operation(21), part(11), AbortCause::QueryFailed)
}
fn key(n: u64) -> SendKey {
    SendKey {
        operation: request().envelope().operation_id(),
        generation: SendGeneration::new(action(n)),
    }
}
fn acknowledgement(
    outcome: OperationOutcome,
    state: ParticipationState,
) -> OperationAcknowledgement {
    OperationAcknowledgement::worker_receipt(
        key(5).operation,
        OperationKind::AbortParticipation,
        outcome,
        AckPayload::Participation(ParticipationReceipt::new(part(11), state)),
    )
}
type Received = Arc<Mutex<Vec<novarocks_coordination_contract::LogicalToAttempt>>>;
struct Carrier {
    work: ScopedWorkRef,
    reservation: ReservationId,
    received: Received,
    hold_received_effect: bool,
}
impl TestActor for Carrier {
    fn handle(
        &mut self,
        _: Tick,
        _: InputEnvelope,
        input: FrontendInput,
        step: &mut ActorStep<'_, FrontendEffect>,
    ) {
        assert!(step.charge(1));
        match input {
            FrontendInput::Test(n) => {
                let body = LogicalHostEffect::Protocol(Box::new(LogicalEffect {
                    action: LogicalActionId::new(NonZeroU64::new(40 + n).unwrap()),
                    body: LogicalEffectBody::AttemptPort(Box::new(lp::LogicalToAttempt {
                        activation: activation(),
                        body: lp::LogicalToAttemptBody::Abort {
                            action: action(60 + n),
                            request: request(),
                        },
                    })),
                }));
                step.effect(
                    EffectDescriptor {
                        id: ActionId::new(1).unwrap(),
                        work: self.work,
                        kind: EffectKind::Settlement,
                        dependencies: Vec::new(),
                    },
                    FrontendEffect::Logical(Arc::new(LogicalEffectEnvelope {
                        body,
                        reservation: self.reservation,
                    })),
                )
                .unwrap();
            }
            FrontendInput::Attempt(input) => {
                let AttemptInput::Port(port) = *input else {
                    panic!("original Abort Control port input");
                };
                self.received.lock().unwrap().push(port);
                if self.hold_received_effect {
                    // A real kernel financial effect, not a Worker/Abort RPC
                    // success or physical Job exit. It must settle separately.
                    step.effect(
                        EffectDescriptor {
                            id: ActionId::new(1).unwrap(),
                            work: self.work,
                            kind: EffectKind::Settlement,
                            dependencies: Vec::new(),
                        },
                        FrontendEffect::Test { value: 1 },
                    )
                    .unwrap();
                }
            }
            _ => panic!("unexpected input in original Abort carrier fixture"),
        }
    }
}
struct Original {
    scope: ScopeHandle,
    work: WorkObligation,
    payer: ReservationObligation,
    actor: ActorRegistration,
}
struct OriginalFixture {
    ledger: RuntimeLedger,
    runtime: ActorRuntime<FrontendActor>,
    lp: Original,
    ap: Original,
    ingress: Arc<AttemptAbortIngress>,
    received: Received,
}
impl OriginalFixture {
    fn new() -> Self {
        Self::with_received_effect(false)
    }
    fn with_received_effect(hold_received_effect: bool) -> Self {
        let ledger = RuntimeLedger::new(ResourceVector::default());
        let runtime = ActorRuntime::new();
        let received = Arc::new(Mutex::new(Vec::new()));
        let make = |id| {
            let scope = ledger
                .open_scope_next(None, Generation::new(1).unwrap())
                .unwrap();
            let (mut work, payer) = scope
                .reserve_next(
                    Generation::new(1).unwrap(),
                    ReservationResources::new(
                        ResourceVector::default(),
                        OwnedSlotNeed {
                            work_headers: 1,
                            ..Default::default()
                        },
                    ),
                    novarocks_control_runtime_core::scope::WorkKind::Settlement,
                    novarocks_control_runtime_core::scope::WorkAdmission::External,
                )
                .unwrap()
                .bind(novarocks_control_runtime_core::scope::WorkKind::Settlement)
                .unwrap();
            work.start().unwrap();
            let actor = runtime
                .register_bound(
                    ActorId::new(id).unwrap(),
                    scope.clone(),
                    FrontendActor::Test(Box::new(Carrier {
                        work: work.reference(),
                        reservation: payer.id(),
                        received: received.clone(),
                        hold_received_effect: id == 2 && hold_received_effect,
                    })),
                    ActorConfig {
                        mailbox: [8; 4],
                        max_batches: 8,
                        max_effects_per_step: 1,
                        max_dependencies_per_effect: 0,
                        turn_budget: 12,
                        // The original Core turn charges one unit and emits
                        // one effect, which independently costs one unit.
                        max_step_work: 2,
                    },
                )
                .unwrap_or_else(|_| panic!("original LP/AP runtime registration"));
            Original {
                scope,
                work,
                payer,
                actor,
            }
        };
        let lp = make(1);
        let ap = make(2);
        let position = runtime
            .reserve_admitted_input_for(&ap.actor, lp.actor.actor(), InputClass::Control, &ap.work)
            .unwrap()
            .into_standing_input()
            .unwrap_or_else(|_| panic!("original AP Control position"));
        let ingress = Arc::new(AttemptAbortIngress::new(
            lp.actor.clone(),
            ap.actor.clone(),
            activation(),
            [(part(11), position)],
        ));
        Self {
            ledger,
            runtime,
            lp,
            ap,
            ingress,
            received,
        }
    }
    fn ticket(&self, actor: ActorId, n: u64) -> EffectTicket<FrontendEffect> {
        let input = self
            .runtime
            .reserve_input(actor, actor, InputClass::Ordinary)
            .unwrap()
            .fill(FrontendInput::Test(n))
            .unwrap();
        assert!(self.runtime.poll_one(Tick(0)).unwrap());
        self.runtime
            .take_effect(actor, input, ActionId::new(1).unwrap())
            .unwrap()
    }
    fn complete(&self, ticket: EffectTicket<FrontendEffect>) {
        let actor = ticket.actor;
        let input = ticket.input;
        self.runtime.effect_completed(ticket).unwrap();
        self.runtime.retire_input(actor, input).unwrap();
    }
    fn close(&mut self) {
        let mut retirement = self.ingress.retirement();
        for _ in 0..4 {
            match retirement.progress().unwrap() {
                AbortIngressRetirementProgress::Complete => break,
                AbortIngressRetirementProgress::Advanced => {}
                AbortIngressRetirementProgress::Blocked => {
                    panic!("actual Abort input/effects were not consumed")
                }
            }
        }
        assert_eq!(
            retirement.progress().unwrap(),
            AbortIngressRetirementProgress::Complete
        );
        for original in [&mut self.lp, &mut self.ap] {
            original.work.body_exited().unwrap();
            original.work.captures_exited().unwrap();
            original.payer.finish().unwrap();
            self.ledger
                .reap_scope_ready(original.scope.id(), 32)
                .unwrap();
            self.runtime
                .close_actor_admission(original.actor.actor())
                .unwrap();
            assert_eq!(
                self.runtime
                    .close_timer_source(original.actor.actor())
                    .unwrap(),
                TimerSourceState::ClosedDrained
            );
        }
        while let Some(turn) = self.runtime.claim_host_actor() {
            turn.release(false).unwrap();
        }
        for original in [&self.lp, &self.ap] {
            let actor = original.actor.actor();
            self.runtime.seal_actor(actor).unwrap();
            assert_eq!(
                self.runtime.failure_source_state(actor).unwrap(),
                FailureSourceState::ClosedDrained
            );
            original.scope.close_external().unwrap();
            drop(self.runtime.remove_actor(actor).unwrap());
            self.ledger
                .reap_scope_ready(original.scope.id(), 32)
                .unwrap();
            self.ledger.remove_scope(original.scope.id()).unwrap();
        }
        assert!(self.runtime.is_empty());
        assert!(self.ledger.is_empty());
    }
}
/// Keep every actual Actor, Scope, checked Work and input role together if an
/// assertion unwinds. An unresolved fixture is retained as UNKNOWN; neither
/// dropping the runtime nor guessing cleanup is a substitute for actual exit.
struct Fixture {
    original: Option<OriginalFixture>,
}
impl Fixture {
    fn new() -> Self {
        Self {
            original: Some(OriginalFixture::new()),
        }
    }
    fn close(mut self) {
        self.original.as_mut().unwrap().close();
        drop(self.original.take());
    }
}
impl std::ops::Deref for Fixture {
    type Target = OriginalFixture;
    fn deref(&self) -> &Self::Target {
        self.original.as_ref().unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if std::thread::panicking()
            && let Some(original) = self.original.take()
        {
            static ORIGINAL_UNKNOWN: OnceLock<Mutex<Vec<OriginalFixture>>> = OnceLock::new();
            ORIGINAL_UNKNOWN
                .get_or_init(|| Mutex::new(Vec::new()))
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(original);
        }
    }
}
#[test]
fn original_lp_ticket_fills_control_synchronously_without_waiting_for_ap_consumption() {
    let f = Fixture::new();
    let ticket = f.ticket(f.lp.actor.actor(), 1);
    let input = f.ingress.deliver(&f.runtime, &ticket).unwrap();
    assert!(f.received.lock().unwrap().is_empty());
    f.complete(ticket);
    assert!(f.runtime.poll_one(Tick(0)).unwrap());
    let received = f.received.lock().unwrap();
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].activation, activation());
    assert!(
        matches!(&received[0].body,novarocks_coordination_contract::LogicalToAttemptBody::Abort{action:a,request:actual}if *a==action(61)&&actual.envelope()==request().envelope())
    );
    drop(received);
    assert!(
        f.runtime
            .input_custody(f.ap.actor.actor(), input)
            .unwrap()
            .is_some()
    );
    f.close();
}
#[test]
fn occupied_original_control_position_is_not_a_second_queue_or_a_replacement_role() {
    let f = Fixture::new();
    let first = f.ticket(f.lp.actor.actor(), 1);
    let second = f.ticket(f.lp.actor.actor(), 2);
    f.ingress.deliver(&f.runtime, &first).unwrap();
    f.complete(first);
    assert!(matches!(
        f.ingress.deliver(&f.runtime, &second),
        Err(AbortPortError::Runtime(ActorTurnError::StandingInputBusy))
    ));
    // Only the original AP input is actually consumed before reusing its role.
    for _ in 0..3 {
        if !f.runtime.poll_one(Tick(0)).unwrap() {
            break;
        }
    }
    f.ingress.deliver(&f.runtime, &second).unwrap();
    f.complete(second);
    assert!(f.runtime.poll_one(Tick(0)).unwrap());
    assert_eq!(f.received.lock().unwrap().len(), 2);
    f.close();
}
#[test]
fn closed_abort_ingress_refuses_the_current_ticket_without_consuming_it() {
    let f = Fixture::new();
    let ticket = f.ticket(f.lp.actor.actor(), 1);
    f.ingress.close_external();
    assert_eq!(
        f.ingress.deliver(&f.runtime, &ticket),
        Err(AbortPortError::Closed)
    );
    assert!(f.received.lock().unwrap().is_empty());
    f.complete(ticket);
    f.close();
}
#[test]
fn ap_actor_identity_cannot_impersonate_the_exact_logical_owner() {
    let f = Fixture::new();
    let ticket = f.ticket(f.ap.actor.actor(), 1);
    assert_eq!(
        f.ingress.deliver(&f.runtime, &ticket),
        Err(AbortPortError::WrongOwner)
    );
    f.complete(ticket);
    assert!(f.received.lock().unwrap().is_empty());
    f.close();
}
#[test]
fn source_failure_fences_the_current_ticket_without_filling_a_control_input() {
    let f = Fixture::new();
    let ticket = f.ticket(f.lp.actor.actor(), 1);
    f.lp.scope.fail().unwrap();
    assert_eq!(
        f.ingress.deliver(&f.runtime, &ticket),
        Err(AbortPortError::Runtime(
            ActorTurnError::FailurePublicationRequired
        ))
    );
    f.complete(ticket);
    assert!(f.received.lock().unwrap().is_empty());
    f.close();
}
#[test]
fn only_original_closing_worker_receipts_write_the_same_isolation_table_and_epoch() {
    let (rows, epoch) = (Arc::new(Mutex::new(BTreeSet::new())), watch::channel(0).0);
    let closed = ClosedParticipations::from_original(rows.clone(), epoch.clone());
    let mut watch = epoch.subscribe();
    for (outcome, state) in [
        (OperationOutcome::Accepted, ParticipationState::Active),
        (OperationOutcome::OwnerFailed, ParticipationState::Aborting),
    ] {
        let slot = AbortReceiptSlot::new(key(5), part(11), closed.clone());
        slot.record(&acknowledgement(outcome, state)).unwrap();
        assert!(!closed.contains(part(11)));
        assert!(!watch.has_changed().unwrap());
    }
    let unknown = AbortReceiptSlot::new(key(6), part(11), closed.clone());
    unknown
        .record(&OperationAcknowledgement::from_dispatch_result(
            key(6).operation,
            OperationKind::AbortParticipation,
            OperationDispatchResult::TransportUnknown,
            AckPayload::None,
        ))
        .unwrap();
    assert!(!closed.contains(part(11)));
    assert!(!watch.has_changed().unwrap());
    let original = AbortReceiptSlot::new(key(7), part(11), closed.clone());
    original
        .record(&acknowledgement(
            OperationOutcome::Accepted,
            ParticipationState::TerminalRetained,
        ))
        .unwrap();
    assert!(rows.lock().unwrap().contains(&part(11)));
    assert!(watch.has_changed().unwrap());
    assert_eq!(*watch.borrow_and_update(), 1);
    assert!(Arc::ptr_eq(&rows, &closed.rows()));
    assert_eq!(*closed.epoch().borrow(), 1);
}
#[test]
fn exact_physical_generation_can_project_old_worker_closure_without_losing_newer_unknown() {
    let closed = ClosedParticipations::new();
    let table = AbortReceiptTable::new(activation());
    let older = Arc::new(AbortReceiptSlot::new(key(5), part(11), closed.clone()));
    let newer = Arc::new(AbortReceiptSlot::new(key(6), part(11), closed.clone()));
    table.register(&older).unwrap();
    table.register(&newer).unwrap();
    newer
        .record(&OperationAcknowledgement::from_dispatch_result(
            key(6).operation,
            OperationKind::AbortParticipation,
            OperationDispatchResult::TransportUnknown,
            AckPayload::None,
        ))
        .unwrap();
    older
        .record(&acknowledgement(
            OperationOutcome::Accepted,
            ParticipationState::Aborting,
        ))
        .unwrap();
    let port = novarocks_attempt_protocol::port_output::AttemptToLogical {
        activation: activation(),
        action: action(31),
        body: novarocks_attempt_protocol::port_output::AttemptToLogicalBody::AbortSettlement {
            abort_action: action(21),
            generation: action(6),
            receipt_generation: Some(action(5)),
            participation: part(11),
            operation_id: key(5).operation,
            settlement: novarocks_attempt_protocol::port_output::AbortIssueSettlement::Worker(
                OperationOutcome::Accepted,
            ),
        },
    };
    let fact = project_abort_settlement(&port, &mut |p, op, g| table.resolve(p, op, g)).unwrap();
    assert!(
        matches!(fact,lp::AttemptToLogicalBody::AbortSettlement{abort_action:a,settlement:lp::AbortIssueSettlement::Worker{closure:AbortWorkerClosure::Aborting,..},..}if a==action(21))
    );
    assert!(
        table
            .resolve(part(11), key(6).operation, action(6))
            .is_none()
    );
    assert!(!table.retire(key(5)));
    drop(older);
    assert!(table.retire(key(5)));
    assert_eq!(
        project_abort_settlement(&port, &mut |p, op, g| table.resolve(p, op, g)).unwrap_err(),
        AbortProjectionError::MissingOriginalReceipt
    );
}
#[test]
fn pair_binding_rejects_foreign_actual_registrations_with_equal_public_numbers() {
    let first = Fixture::new();
    let foreign = Fixture::new();
    assert_eq!(first.lp.actor.actor(), foreign.lp.actor.actor());
    assert_eq!(first.ap.actor.actor(), foreign.ap.actor.actor());
    assert!(
        first
            .ingress
            .matches_pair(&first.lp.actor, &first.ap.actor, activation())
    );
    assert!(
        !first
            .ingress
            .matches_pair(&foreign.lp.actor, &first.ap.actor, activation())
    );
    assert!(
        !first
            .ingress
            .matches_pair(&first.lp.actor, &foreign.ap.actor, activation())
    );
    let ticket = foreign.ticket(foreign.lp.actor.actor(), 1);
    assert_eq!(
        first.ingress.deliver(&foreign.runtime, &ticket),
        Err(AbortPortError::WrongOwner)
    );
    foreign.complete(ticket);
    first.close();
    foreign.close();
}

#[test]
fn normal_abort_occurrence_retirement_waits_for_real_consumption_and_effects_and_keeps_same_role() {
    let f = Fixture {
        original: Some(OriginalFixture::with_received_effect(true)),
    };
    let ticket = f.ticket(f.lp.actor.actor(), 1);
    let input = f.ingress.deliver(&f.runtime, &ticket).unwrap();
    let role = f
        .runtime
        .input_custody(f.ap.actor.actor(), input)
        .unwrap()
        .unwrap()
        .standing_input_id
        .unwrap();
    f.complete(ticket);
    assert_eq!(
        f.ingress.retire_ready_occurrence_after(None).unwrap(),
        Some((part(11), false)),
        "a queued actual Abort input cannot be returned"
    );
    assert!(
        f.runtime
            .input_custody(f.ap.actor.actor(), input)
            .unwrap()
            .is_some()
    );
    assert!(f.received.lock().unwrap().is_empty());
    assert!(f.runtime.poll_one(Tick(0)).unwrap());
    assert_eq!(f.received.lock().unwrap().len(), 1);
    assert!(matches!(
        f.runtime.input_retirement_status(f.ap.actor.actor(), input),
        Err(ActorTurnError::EffectsRemain)
    ));
    let held = f
        .runtime
        .take_effect(f.ap.actor.actor(), input, ActionId::new(1).unwrap())
        .unwrap();
    assert_eq!(
        f.ingress.retire_ready_occurrence_after(None).unwrap(),
        Some((part(11), false)),
        "an actually consumed input still owes its original financial effect"
    );
    f.runtime.effect_completed(held).unwrap();
    assert_eq!(
        f.ingress.retire_ready_occurrence_after(None).unwrap(),
        Some((part(11), true))
    );
    assert!(
        f.runtime
            .input_custody(f.ap.actor.actor(), input)
            .unwrap()
            .is_none()
    );
    let original = f
        .runtime
        .standing_input_inventory(f.ap.actor.actor(), None, 16)
        .unwrap();
    assert!(
        original
            .iter()
            .any(|row| row.standing_input_id == role && row.in_flight.is_none())
    );
    assert!(!f.ingress.closed.load(Ordering::Acquire));
    assert_eq!(
        f.ingress
            .retire_ready_occurrence_after(Some(part(11)))
            .unwrap(),
        Some((part(11), false)),
        "an idle standing is retained without inventing retirement progress"
    );
    let ticket = f.ticket(f.lp.actor.actor(), 2);
    let next = f.ingress.deliver(&f.runtime, &ticket).unwrap();
    assert_ne!(next, input);
    assert_eq!(
        f.runtime
            .input_custody(f.ap.actor.actor(), next)
            .unwrap()
            .unwrap()
            .standing_input_id,
        Some(role)
    );
    f.complete(ticket);
    assert!(f.runtime.poll_one(Tick(0)).unwrap());
    assert_eq!(f.received.lock().unwrap().len(), 2);
    let held = f
        .runtime
        .take_effect(f.ap.actor.actor(), next, ActionId::new(1).unwrap())
        .unwrap();
    f.runtime.effect_completed(held).unwrap();
    assert_eq!(
        f.ingress
            .retire_ready_occurrence_after(Some(part(11)))
            .unwrap(),
        Some((part(11), true))
    );
    f.close();
}

#[test]
fn returning_the_original_abort_position_wakes_its_exhausted_retirement_pass() {
    let f = Fixture {
        original: Some(OriginalFixture::with_received_effect(true)),
    };
    let ticket = f.ticket(f.lp.actor.actor(), 1);
    let FrontendEffect::Logical(envelope) = &ticket.effect else {
        panic!("same original logical effect");
    };
    let LogicalHostEffect::Protocol(effect) = &envelope.body else {
        panic!("same original protocol effect");
    };
    let LogicalEffectBody::AttemptPort(message) = &effect.body else {
        panic!("same original Abort message");
    };
    let slot = f.ingress.slots.get(&part(11)).unwrap();
    let position = slot.position.lock().unwrap().take().unwrap();
    let mut loan = PositionLoan {
        slot,
        position: Some(position),
    };
    let input = f
        .runtime
        .fill_from_effect(
            &ticket,
            loan.position.as_ref().unwrap(),
            FrontendInput::Attempt(Box::new(AttemptInput::Port(message.as_ref().clone()))),
        )
        .unwrap_or_else(|_| panic!("the same original Abort input is queued"));
    f.complete(ticket);
    assert!(f.runtime.poll_one(Tick(0)).unwrap());
    let effect = f
        .runtime
        .take_effect(f.ap.actor.actor(), input, ActionId::new(1).unwrap())
        .unwrap();
    f.runtime.effect_completed(effect).unwrap();
    assert!(
        f.runtime
            .input_retirement_status(f.ap.actor.actor(), input)
            .is_ok()
    );
    assert_eq!(
        f.ingress.retire_ready_occurrence_after(None).unwrap(),
        Some((part(11), false)),
        "the real ready occurrence cannot be returned while its position is loaned"
    );
    // Consume the prior actual notifications after that blocked finite visit.
    // This component hosts the kernel directly; no Source or Worker exit is claimed.
    for _ in 0..4 {
        let Some(turn) = f.runtime.claim_host_actor() else {
            break;
        };
        turn.release(false).unwrap();
    }
    assert!(f.runtime.claim_host_actor().is_none());
    loan.restore_and_notify(&f.runtime, &f.ap.actor).unwrap();
    let turn = f
        .runtime
        .claim_host_actor()
        .expect("actual position return wakes its same AP");
    assert!(turn.was_notified());
    assert!(turn.registration().same_registration(&f.ap.actor));
    assert_eq!(
        f.ingress.retire_ready_occurrence_after(None).unwrap(),
        Some((part(11), true))
    );
    turn.release(false).unwrap();
    drop(loan);
    f.close();
}
