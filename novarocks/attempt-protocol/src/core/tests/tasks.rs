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
    AttemptCore,
    capacity::ReservationToken,
    effect::{EffectClass, Publication},
    execution::ack_phase::{admission_intent, dispatch_admission, queue_establish},
    input::{AttemptInput, AttemptInputClaim, QueuedInputGrantRef, SendGeneration},
};
use novarocks_coordination_contract::{
    AttemptAuthorization, AttemptAuthorizationDecision, AttemptAuthorizationRef,
    AttemptToLogicalBody,
};
use novarocks_execution_contract::{
    AdmissionTicketId, LeaseSequence, LeaseValidFor, OperationEnvelope, OperationOutcome,
    ParticipationAdmissionTicketReceipt,
};

struct DispatchTurn<'a> {
    budget: Units,
    reservation_target: &'a mut Option<BackendProcessId>,
}

impl StepContext for DispatchTurn<'_> {
    fn charge(&mut self, units: usize) -> bool {
        self.budget.charge(units)
    }

    fn reserve_queue_stage(
        &mut self,
        lane: DispatchLane,
        control_progress: bool,
        backend: BackendProcessId,
        records: usize,
        queued_bytes: usize,
    ) -> QueueStage {
        assert_eq!(
            (lane, control_progress, records, queued_bytes),
            (DispatchLane::Lifecycle, false, 1, 256)
        );
        assert_eq!(self.reservation_target.take(), Some(backend));
        QueueStage::Granted(ReservationToken(id(50)))
    }
}

#[derive(Debug, Eq, PartialEq)]
struct DispatchTrace {
    publication_revision: u64,
    send: (OperationEnvelope, QueuedInputGrantRef, ReservationToken),
    settled_admission: (
        AttemptAuthorizationRef,
        TaskOperationId,
        ParticipationAdmissionTicketReceipt,
    ),
    retired_authorization: Option<TaskOperationId>,
    round_result: Option<Result<(), AttemptFailure>>,
    remaining_credits: [usize; 2],
    deadline: Option<MonotonicInstant>,
}

fn drive_dispatch_cut(budget: usize) -> (DispatchTrace, bool) {
    let start = start();
    let now = start.initial_now;
    let sources = start.sources;
    let activation = start.activation.identity;
    let mut core = Core::new(start).unwrap();
    let (target, observed) = {
        let mut participants = core.execution.graph().participations().copied();
        (participants.next().unwrap(), participants.next().unwrap())
    };
    let action = |n| AttemptActionId::new(id(n));
    let token = |n| AttemptRequestToken::new(id(n));
    let qa = |token, decision| LogicalToAttempt {
        activation,
        body: LogicalToAttemptBody::AuthorizationReply {
            token,
            decision: Ok(decision),
        },
    };

    let authorize = |core: &mut Core, intent: &OperationIntent, n, decision| {
        let request = core
            .authorize_candidate(intent, token(n), action(n))
            .unwrap();
        assert!(matches!(request, AuthorizationStep::Request(_)));
        let reply = core.apply_qa_reply(qa(token(n), decision)).unwrap();
        assert!(
            matches!(reply, QaReply::Authorized(operation) if operation == intent.operation_id())
        );
    };

    // Keep a queued, authorized Establish and a second backend's Native reply
    // outstanding when the two runs enter the same original round.
    let request = admission_intent(&mut core.execution, target);
    let ticket = dispatch_admission(&mut core.execution, request, 1, OperationOutcome::Accepted);
    core.execution.apply_dispatched_reply(&ticket).1.unwrap();

    let request = admission_intent(&mut core.execution, observed);
    let admission = OperationIntent::AcquireParticipationAdmissionTicket(request);
    let operation = admission.operation_id();
    let original_issue = AttemptAuthorizationRef::new(id(15));
    authorize(
        &mut core,
        &admission,
        15,
        AttemptAuthorizationDecision::AdmissionStarted(original_issue),
    );
    let observation =
        dispatch_admission(&mut core.execution, request, 2, OperationOutcome::Accepted);
    core.authorizations
        .note_sent(&admission, observation.generation);
    let original_ticket = ParticipationAdmissionTicketReceipt::new(
        AdmissionTicketId::try_from_bytes([2; 16]).unwrap(),
        observed,
        request.valid_for(),
    );

    let grant = QueuedInputGrantRef::new(id(20), SendGeneration::new(action(20)));
    let intent = queue_establish(&mut core.execution, target, grant);
    let establish_authorization = AttemptAuthorizationRef::new(id(20));
    authorize(
        &mut core,
        &intent,
        20,
        AttemptAuthorizationDecision::EstablishAuthorized(establish_authorization),
    );

    let claim = |source, n| AttemptInputClaim {
        source,
        input: AttemptInputId(id(n)),
        native_cut: Some(AttemptSourceCut {
            source: sources.native,
            tail: None,
        }),
        selected_cut: None,
    };
    let mut reservation_target = Some(target.backend_process_id());
    let mut step = |core: &mut Core, claim, input| {
        core.handle(
            now,
            claim,
            input,
            &mut DispatchTurn {
                budget: Units(budget),
                reservation_target: &mut reservation_target,
            },
        )
        .unwrap()
    };
    let bootstrap = step(&mut core, claim(sources.actor, 1), AttemptInput::Bootstrap);
    assert_eq!(bootstrap.selection, InputSelection::Prefer(sources.native));
    assert!(bootstrap.effect.is_none());
    let receipt_claim = AttemptInputClaim {
        native_cut: Some(AttemptSourceCut {
            source: sources.native,
            tail: Some(AttemptInputId(id(2))),
        }),
        ..claim(sources.native, 2)
    };
    let receipt_step = step(
        &mut core,
        receipt_claim,
        AttemptInput::Executor(ExecutorFact::Operation(observation)),
    );
    assert_eq!(
        receipt_step.selection,
        InputSelection::Only(sources.observation)
    );
    assert!(receipt_step.effect.is_none());
    let common_claim = AttemptInputClaim {
        selected_cut: Some(AttemptSourceCut {
            source: sources.observation,
            tail: None,
        }),
        ..claim(sources.actor, 101)
    };
    for claim in [common_claim, claim(sources.actor, 102)] {
        assert!(step(&mut core, claim, AttemptInput::Timer).effect.is_none());
    }

    let publication = step(&mut core, claim(sources.actor, 103), AttemptInput::Timer);
    assert_eq!(
        publication.selection,
        InputSelection::Only(sources.publication)
    );
    assert_eq!(core.next_deadline(), None);
    let Some(AttemptEffect {
        id: publication,
        class: EffectClass::Publication,
        body: EffectBody::Publish(Publication::ProtocolBatch(batch)),
    }) = publication.effect
    else {
        panic!("expected the original protocol publication before dispatch")
    };
    assert_eq!(batch.owner_revision, 0);
    assert_eq!(
        batch.initial_membership.as_deref(),
        Some(&ProtocolMembership::default())
    );
    assert!(batch.changes.is_empty());
    assert!(batch.selected_failure.is_none());
    assert!(!batch.root_success_sealed);

    let mut next = step(
        &mut core,
        claim(sources.publication, 3),
        AttemptInput::Executor(ExecutorFact::PublicationApplied {
            effect: publication,
            result: Ok(PublicationApplied::ProtocolBatch { published: true }),
        }),
    );
    let yielded = next.effect.is_none();
    if yielded {
        assert_eq!(next.selection, InputSelection::Only(sources.actor));
        assert_eq!(core.next_deadline(), Some(now));
        assert_eq!(core.execution.dispatcher().queued_items(), 1);
        assert_eq!(core.execution.dispatcher().in_flight_operations(), 0);
        next = step(&mut core, claim(sources.actor, 104), AttemptInput::Timer);
    }
    assert!(next.round_result.is_none());
    let Some(AttemptEffect {
        class: EffectClass::Business,
        body: EffectBody::SendBatch {
            batch: send,
            reservation,
        },
        ..
    }) = next.effect
    else {
        panic!("expected the original queued Establish")
    };
    assert_eq!(send.backend, target.backend_process_id());
    assert_eq!(send.lane, DispatchLane::Lifecycle);
    assert!(!send.control_progress);
    assert_eq!((send.queued_at, send.queued_bytes), (now, 256));
    assert_eq!(send.queued_inputs.as_ref(), [grant]);
    let [
        OperationIntent::EstablishParticipation {
            envelope,
            participation,
            ticket,
            initial_lease_sequence,
            initial_lease_valid_for,
            material,
            queued_bytes,
        },
    ] = send.operations.as_ref()
    else {
        panic!("expected one original Establish operation")
    };
    assert_eq!(*envelope, intent.envelope());
    assert_eq!(*participation, target);
    assert_eq!(*ticket, AdmissionTicketId::try_from_bytes([1; 16]).unwrap());
    assert_eq!(*initial_lease_sequence, LeaseSequence::INITIAL);
    assert_eq!(
        *initial_lease_valid_for,
        LeaseValidFor::new(Duration::from_secs(30)).unwrap()
    );
    assert_eq!((*material, *queued_bytes), (PayloadRef(id(90)), 256));
    assert_eq!(reservation, ReservationToken(id(50)));
    let send = (*envelope, send.queued_inputs[0], reservation);

    let next = step(&mut core, claim(sources.actor, 105), AttemptInput::Timer);
    assert_eq!(next.selection, InputSelection::Only(sources.qa));
    assert_eq!(core.next_deadline(), None);
    assert!(next.round_result.is_none());
    let Some(AttemptEffect {
        id: settlement_effect,
        class: EffectClass::Settlement,
        body: EffectBody::Port(port),
    }) = next.effect
    else {
        panic!("expected original QA settlement after the queued send")
    };
    assert_eq!(port.activation, activation);
    assert_eq!(port.action, settlement_effect);
    let AttemptToLogicalBody::AuthorizationRequest {
        token: settlement_token,
        request:
            AttemptAuthorization::SettleAdmission {
                issue,
                participation,
                operation_id,
                outcome: Some(OperationOutcome::Accepted),
                receipt: Some(ticket),
                prior_transport_unknown: false,
            },
    } = port.body
    else {
        panic!("expected exact original Worker receipt in the settlement")
    };
    assert_eq!(
        (issue, participation, operation_id, ticket),
        (original_issue, observed, operation, original_ticket)
    );

    // Correlate each response with the effect emitted in its own run. Resuming
    // the tail can allocate unused action IDs; operation and grant IDs are exact.
    let finished = step(
        &mut core,
        claim(sources.qa, 4),
        AttemptInput::Port(qa(
            settlement_token,
            AttemptAuthorizationDecision::AdmissionGranted,
        )),
    );
    assert_eq!(finished.retired_authorization, Some(operation));
    assert_eq!(finished.round_result, Some(Ok(())));
    assert_eq!(finished.selection, InputSelection::Prefer(sources.native));
    assert!(finished.effect.is_none());
    assert_eq!(core.next_deadline(), None);
    let dispatcher = core.execution.dispatcher();
    assert_eq!(
        (
            dispatcher.queued_items(),
            dispatcher.queued_bytes(),
            dispatcher.in_flight_operations()
        ),
        (0, 0, 1)
    );
    let remaining_credits = [target, observed]
        .map(|part| dispatcher.lane_in_flight(part.backend_process_id(), DispatchLane::Lifecycle));
    assert_eq!(remaining_credits, [1, 0]);
    assert_eq!(reservation_target, None);

    (
        DispatchTrace {
            publication_revision: batch.owner_revision,
            send,
            settled_admission: (issue, operation_id, ticket),
            retired_authorization: finished.retired_authorization,
            round_result: finished.round_result,
            remaining_credits,
            deadline: core.next_deadline(),
        },
        yielded,
    )
}

#[test]
fn dispatch_continuation_preserves_original_effects_and_settlement_order() {
    // Nine units can apply a Timer, enter dispatch and emit a single already
    // selected member, but cannot also scan the original backend rotation.
    let (resumed, yielded) = drive_dispatch_cut(9);
    let (uninterrupted, large_budget_yielded) = drive_dispatch_cut(4096);
    assert!(yielded);
    assert!(!large_budget_yielded);
    assert_eq!(resumed, uninterrupted);
}
