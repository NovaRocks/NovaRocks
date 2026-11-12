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
    core::round_tail::RoundTailStep,
    effect::{EffectClass, Publication, PumpKind, QueuedInputPayload, TaskDomainIntent},
    execution::{
        CreateGrantProgress, EdgeAdmissionProgress,
        ack_phase::{admission_intent, dispatch_admission, queue_establish},
    },
    input::{
        AckPayload, AttemptInput, AttemptInputClaim, OperationReply, OperationReplyResult,
        QueuedInputGrantRef, SendGeneration, SplitDeliveryRef, UpdateAdmission,
    },
    split_delivery::{AcceptedPlanNode, SplitDeliverySettlement},
};
use novarocks_coordination_contract::{
    AttemptAuthorization, AttemptAuthorizationDecision, AttemptAuthorizationRef,
    AttemptToLogicalBody,
};
use novarocks_execution_contract::{
    AdmissionTicketId, CancelReason, CreateTaskReceipt, DomainProgression,
    ExchangeDeliveryRequirement, ExchangeEdgeId, LeaseReceipt, LeaseSequence, LeaseValidFor,
    OperationEnvelope, OperationOutcome, ParticipationAdmissionTicketReceipt, ParticipationReceipt,
    ParticipationState, PlanNodeId, PlanNodeSplitReceipt, SafeDetail, SplitOffer, SplitSequence,
    SplitWatermark, TaskDomainReceipt, TaskFailure, UpdateTaskReceipt,
    failure::{FailureClass, FailureFact, FailureReason},
    operation::OperationResult,
};
use novarocks_types::{TaskId, UniqueId};
use std::num::NonZeroU16;

#[test]
fn original_backend_preparation_capacity_must_cover_the_configured_deployment_window() {
    let mut start = start();
    let window = start.plan.deployment_window.get();
    let positions = start
        .plan
        .preparation_positions
        .values_mut()
        .next()
        .unwrap();
    *positions = std::num::NonZeroUsize::new(window - 1).unwrap();
    assert!(matches!(
        Core::new(start),
        Err(AttemptFailure::TaskProtocol(error))
            if matches!(error.as_ref(), TaskExecutionError::Schedule(message)
                if message == "frozen backend preparation capacities must cover the configured deployment window for every exact participant")
    ));
}

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
    let reply = step(
        &mut core,
        claim(sources.qa, 4),
        AttemptInput::Port(qa(
            settlement_token,
            AttemptAuthorizationDecision::AdmissionGranted,
        )),
    );
    assert_eq!(reply.retired_authorization, Some(operation));
    assert!(reply.round_result.is_none() && reply.effect.is_none());
    assert_eq!(reply.selection, InputSelection::Only(sources.actor));
    assert_eq!(core.next_deadline(), Some(now));
    let finished = step(&mut core, claim(sources.actor, 106), AttemptInput::Timer);
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
            retired_authorization: reply.retired_authorization,
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

fn authorize_queued_establish(
    core: &mut Core,
    participation: ParticipationRef,
    sequence: u64,
) -> (
    TaskOperationId,
    AttemptAuthorizationRef,
    QueuedInputGrantRef,
) {
    let action = AttemptActionId::new(id(sequence));
    let token = AttemptRequestToken::new(id(sequence));
    let guard = AttemptAuthorizationRef::new(id(sequence));
    let grant = QueuedInputGrantRef::new(id(sequence), SendGeneration::new(action));
    let intent = queue_establish(&mut core.execution, participation, grant);
    assert!(matches!(
        core.authorize_candidate(&intent, token, action).unwrap(),
        AuthorizationStep::Request(AttemptEffect {
            body: EffectBody::EstablishAuthorization { .. },
            ..
        })
    ));
    let result = core
        .apply_qa_reply(LogicalToAttempt {
            activation: core.activation,
            body: LogicalToAttemptBody::AuthorizationReply {
                token,
                decision: Ok(AttemptAuthorizationDecision::EstablishAuthorized(guard)),
            },
        })
        .unwrap();
    let operation = intent.operation_id();
    assert!(matches!(result, QaReply::Authorized(original) if original == operation));
    (operation, guard, grant)
}

#[test]
fn closing_gate_settles_two_unused_establish_guards_on_separate_timer_inputs() {
    use crate::terminal::GatePreparation;
    use novarocks_coordination_contract::{AttemptIssueSettlement, AttemptStopReason};

    let start = start();
    let now = start.initial_now;
    let sources = start.sources;
    let activation = start.activation.identity;
    let mut core = Core::new(start).unwrap();
    let (first, second) = {
        let mut participations = core.execution.graph().participations().copied();
        (
            participations.next().unwrap(),
            participations.next().unwrap(),
        )
    };
    let fixture_order = [(20, second), (21, first)];
    for (sequence, participation) in fixture_order {
        let admission = admission_intent(&mut core.execution, participation);
        let ticket = dispatch_admission(
            &mut core.execution,
            admission,
            sequence,
            OperationOutcome::Accepted,
        );
        core.execution.apply_dispatched_reply(&ticket).1.unwrap();
    }
    let mut original_guards = fixture_order.map(|(sequence, participation)| {
        let (operation, guard, _) = authorize_queued_establish(&mut core, participation, sequence);
        (operation, guard, participation)
    });
    original_guards.sort_unstable_by_key(|(operation, _, _)| *operation);
    assert_eq!(core.execution.dispatcher().queued_items(), 2);
    assert_eq!(core.execution.dispatcher().in_flight_operations(), 0);

    let mut next_input = 1;
    let mut step = |core: &mut Core, source, input| {
        let claim = AttemptInputClaim {
            source,
            input: AttemptInputId(id(next_input)),
            native_cut: Some(AttemptSourceCut {
                source: sources.native,
                tail: None,
            }),
            selected_cut: None,
        };
        next_input += 1;
        core.handle(now, claim, input, &mut Units(4096)).unwrap()
    };
    step(&mut core, sources.actor, AttemptInput::Bootstrap);
    let stopped = step(
        &mut core,
        sources.actor,
        AttemptInput::Port(LogicalToAttempt {
            activation,
            body: LogicalToAttemptBody::Stop {
                reason: AttemptStopReason::Client,
                failure: AttemptFailure::TaskProtocol(Box::new(schedule("original client stop"))),
            },
        }),
    );
    assert!(stopped.stop_producing);

    for (expected_operation, expected_guard, expected_participation) in original_guards {
        let closing = step(&mut core, sources.actor, AttemptInput::Timer);
        assert_eq!(closing.selection, InputSelection::Only(sources.qa));
        assert_eq!(core.next_deadline(), None);
        let Some(AttemptEffect {
            class: EffectClass::Settlement,
            body: EffectBody::Port(port),
            ..
        }) = closing.effect
        else {
            panic!("expected the next original unused Establish");
        };
        let AttemptToLogicalBody::EstablishSettlement {
            token,
            authorization,
            participation,
            operation_id,
            settlement: AttemptIssueSettlement::DefinitelyUnsent,
        } = port.body
        else {
            panic!("the original unused guard must settle definitely unsent");
        };
        assert_eq!(port.activation, activation);
        assert_eq!(
            (operation_id, authorization, participation),
            (expected_operation, expected_guard, expected_participation)
        );
        let replied = step(
            &mut core,
            sources.qa,
            AttemptInput::Port(LogicalToAttempt {
                activation,
                body: LogicalToAttemptBody::EstablishSettlementReply {
                    token,
                    authorization,
                    result: Ok(()),
                },
            }),
        );
        assert!(
            replied.effect.is_none(),
            "the QA input must retire before borrowing its result position again"
        );
        assert_eq!(replied.selection, InputSelection::Only(sources.actor));
        assert_eq!(core.next_deadline(), Some(now));
        assert_eq!(core.authorizations.preparation(), GatePreparation::Applying);
    }
    let closed = step(&mut core, sources.actor, AttemptInput::Timer);
    assert!(closed.effect.is_none());
    assert_eq!(
        core.authorizations.preparation(),
        GatePreparation::Applied { ready: true }
    );
    assert_eq!(core.execution.dispatcher().queued_items(), 2);
    assert_eq!(core.execution.dispatcher().in_flight_operations(), 0);
}

#[test]
fn cleanup_settles_unused_establish_and_waits_for_emitted_establish_withdrawal() {
    use crate::{input::WithdrawalOutcome, terminal::GatePreparation};
    use novarocks_coordination_contract::{
        AttemptConvergenceMode, AttemptIssueSettlement, AttemptToLogical,
    };

    let start = start();
    let now = start.initial_now;
    let sources = start.sources;
    let activation = start.activation.identity;
    let mut core = Core::new(start).unwrap();
    let (emitted, unused) = {
        let mut participants = core.execution.graph().participations().copied();
        (participants.next().unwrap(), participants.next().unwrap())
    };
    for (participation, sequence) in [(emitted, 1), (unused, 2)] {
        let admission = admission_intent(&mut core.execution, participation);
        let ticket = dispatch_admission(
            &mut core.execution,
            admission,
            sequence,
            OperationOutcome::Accepted,
        );
        core.execution.apply_dispatched_reply(&ticket).1.unwrap();
    }
    let (emitted_operation, emitted_guard, emitted_grant) =
        authorize_queued_establish(&mut core, emitted, 20);
    let mut next_input = 1;
    let mut reservation_target = Some(emitted.backend_process_id());
    let mut step = |core: &mut Core, source, input| {
        let claim = AttemptInputClaim {
            source,
            input: AttemptInputId(id(next_input)),
            native_cut: Some(AttemptSourceCut {
                source: sources.native,
                tail: None,
            }),
            selected_cut: Some(AttemptSourceCut {
                source: sources.observation,
                tail: None,
            }),
        };
        next_input += 1;
        core.handle(
            now,
            claim,
            input,
            &mut DispatchTurn {
                budget: Units(4096),
                reservation_target: &mut reservation_target,
            },
        )
        .unwrap()
    };
    let publication_applied = |effect| {
        AttemptInput::Executor(ExecutorFact::PublicationApplied {
            effect,
            result: Ok(PublicationApplied::ProtocolBatch { published: true }),
        })
    };
    let expect_unsent = |port: AttemptToLogical<AttemptFailure>, expected| {
        let AttemptToLogicalBody::EstablishSettlement {
            token,
            authorization,
            participation,
            operation_id,
            settlement: AttemptIssueSettlement::DefinitelyUnsent,
        } = port.body
        else {
            panic!("expected definitely-unsent Establish settlement");
        };
        assert_eq!((authorization, participation, operation_id), expected);
        token
    };
    let settlement_reply = |token, authorization| {
        AttemptInput::Port(LogicalToAttempt {
            activation,
            body: LogicalToAttemptBody::EstablishSettlementReply {
                token,
                authorization,
                result: Ok(()),
            },
        })
    };

    step(&mut core, sources.actor, AttemptInput::Bootstrap);
    for _ in 0..3 {
        assert!(
            step(&mut core, sources.actor, AttemptInput::Timer)
                .effect
                .is_none()
        );
    }
    let Some(AttemptEffect {
        id: publication,
        body: EffectBody::Publish(Publication::ProtocolBatch(_)),
        ..
    }) = step(&mut core, sources.actor, AttemptInput::Timer).effect
    else {
        panic!("expected the original publication before dispatch");
    };
    let Some(AttemptEffect {
        id: send,
        class: EffectClass::Business,
        body: EffectBody::SendBatch { batch, .. },
    }) = step(
        &mut core,
        sources.publication,
        publication_applied(publication),
    )
    .effect
    else {
        panic!("expected one emitted Establish batch");
    };
    assert_eq!(batch.queued_inputs.as_ref(), [emitted_grant]);
    assert!(matches!(
        batch.operations.as_ref(),
        [operation] if operation.operation_id() == emitted_operation
    ));
    assert_eq!(
        step(&mut core, sources.actor, AttemptInput::Timer).round_result,
        Some(Ok(()))
    );
    let (unused_operation, unused_guard, unused_grant) =
        authorize_queued_establish(&mut core, unused, 21);
    assert_eq!(core.execution.dispatcher().queued_items(), 1);

    let closing = step(
        &mut core,
        sources.actor,
        AttemptInput::Port(LogicalToAttempt {
            activation,
            body: LogicalToAttemptBody::BeginConvergence {
                mode: AttemptConvergenceMode::Cleanup,
                remote_deadline: now.saturating_add(Duration::from_secs(5)),
                computation_ready: None,
                retain_diagnostics: false,
            },
        }),
    );
    assert!(closing.stop_producing);
    let Some(AttemptEffect {
        id: withdrawal,
        class: EffectClass::Cleanup,
        body: EffectBody::Withdraw { effect },
    }) = step(&mut core, sources.actor, AttemptInput::Timer).effect
    else {
        panic!("expected withdrawal of the emitted batch");
    };
    assert_eq!(effect, send);

    let closing = step(&mut core, sources.actor, AttemptInput::Timer);
    let Some(AttemptEffect {
        class: EffectClass::Settlement,
        body: EffectBody::Port(port),
        ..
    }) = closing.effect
    else {
        panic!("the unused guard must settle without a transport report");
    };
    let token = expect_unsent(port, (unused_guard, unused, unused_operation));
    assert_eq!(closing.selection, InputSelection::Only(sources.qa));
    let stale = step(
        &mut core,
        sources.qa,
        settlement_reply(token, emitted_guard),
    );
    assert_eq!(stale.selection, InputSelection::Only(sources.qa));
    assert!(stale.effect.is_none());
    let replied = step(&mut core, sources.qa, settlement_reply(token, unused_guard));
    assert!(replied.effect.is_none());
    assert_eq!(replied.selection, InputSelection::Only(sources.actor));
    assert_eq!(core.authorizations.preparation(), GatePreparation::Applying);
    let waiting = step(&mut core, sources.actor, AttemptInput::Timer);
    assert!(waiting.effect.is_none());
    assert_eq!(
        core.authorizations.preparation(),
        GatePreparation::Applied { ready: false }
    );

    assert_eq!(core.execution.dispatcher().queued_items(), 1);

    // A report for the Withdraw request itself cannot retire the SendBatch.
    core.apply_transport_withdrawal(
        now,
        &ExecutorFact::Withdrawn {
            effect: withdrawal,
            outcome: WithdrawalOutcome::DefinitelyUnsent,
        },
        |_| panic!("Establish does not settle an Abort"),
    )
    .unwrap();
    assert!(!core.authorizations.close_needs_drive());
    assert_eq!(
        core.execution
            .dispatcher()
            .lane_in_flight(emitted.backend_process_id(), DispatchLane::Lifecycle),
        1
    );
    let actual = step(
        &mut core,
        sources.transport,
        AttemptInput::Executor(ExecutorFact::Withdrawn {
            effect: send,
            outcome: WithdrawalOutcome::DefinitelyUnsent,
        }),
    );
    assert!(actual.effect.is_none());
    assert_eq!(core.execution.dispatcher().in_flight_operations(), 0);
    assert!(core.authorizations.close_needs_drive());

    let mut next = (sources.actor, AttemptInput::Timer);
    let mut returned_unused_grant = false;
    let mut settled_emitted_guard = false;
    for _ in 0..32 {
        let progress = step(&mut core, next.0, next.1);
        next = (sources.actor, AttemptInput::Timer);
        if let Some(effect) = progress.effect {
            match effect.body {
                EffectBody::Publish(Publication::ProtocolBatch(_)) => {
                    next = (sources.publication, publication_applied(effect.id));
                }
                EffectBody::ReleaseCreationPayload { .. } => {}
                EffectBody::ReleaseQueuedInput { grant } => {
                    assert_eq!(grant, unused_grant);
                    assert!(!std::mem::replace(&mut returned_unused_grant, true));
                }
                EffectBody::Port(port) => {
                    assert_eq!(effect.class, EffectClass::Settlement);
                    let token = expect_unsent(port, (emitted_guard, emitted, emitted_operation));
                    assert!(!std::mem::replace(&mut settled_emitted_guard, true));
                    next = (sources.qa, settlement_reply(token, emitted_guard));
                }
                body => panic!("unexpected cleanup effect: {body:?}"),
            }
        }
        if returned_unused_grant
            && settled_emitted_guard
            && core.authorizations.preparation() == (GatePreparation::Applied { ready: true })
        {
            break;
        }
    }
    assert!(returned_unused_grant);
    assert!(settled_emitted_guard);
    assert_eq!(
        core.authorizations.preparation(),
        GatePreparation::Applied { ready: true }
    );
    assert_eq!(core.execution.dispatcher().queued_items(), 0);
    assert_eq!(core.execution.dispatcher().in_flight_operations(), 0);
}

struct ExchangeTurn {
    blocked_create: Option<BackendProcessId>,
    blocked_update: Option<BackendProcessId>,
    refused: usize,
}

impl StepContext for ExchangeTurn {
    fn charge(&mut self, _: usize) -> bool {
        true
    }

    fn reserve_queue_stage(
        &mut self,
        lane: DispatchLane,
        _: bool,
        backend: BackendProcessId,
        _: usize,
        _: usize,
    ) -> QueueStage {
        if lane == DispatchLane::Create && self.blocked_create == Some(backend) {
            QueueStage::TargetFull(backend)
        } else if lane == DispatchLane::Update && self.blocked_update == Some(backend) {
            self.refused += 1;
            QueueStage::ProcessFull
        } else {
            QueueStage::Granted(ReservationToken(id(900)))
        }
    }
}

struct DrainTrace {
    core: Core,
    producer: TaskIdentity,
    middle: Vec<TaskIdentity>,
    transport: ExchangeTurn,
    next: u64,
    replies: Vec<OperationObservation>,
    openings: BTreeMap<ParticipationRef, NonZeroU64>,
    creates: BTreeMap<TaskIdentity, (OperationIntent, SendGeneration)>,
    closes: BTreeMap<TaskIdentity, usize>,
    close_sends: usize,
    producer_opens: usize,
    cancels: Vec<(TaskIdentity, CancelReason)>,
    quiesces: Vec<ParticipationRef>,
    split_notices: Vec<EffectBody>,
    split_sends: Vec<(TaskIdentity, PlanNodeId, SplitOffer)>,
}

fn worker_accepted(
    operation: &OperationIntent,
    generation: SendGeneration,
    payload: Option<AckPayload>,
) -> OperationObservation {
    OperationObservation {
        generation,
        reply: OperationReply {
            operation_id: operation.operation_id(),
            kind: operation.kind(),
            result: OperationReplyResult::Worker(OperationResult::Verdict {
                outcome: OperationOutcome::Accepted,
                detail: None,
                acknowledgement: payload,
            }),
        },
    }
}

impl DrainTrace {
    fn new() -> Self {
        let mut trace = Self::with_dispatched_creates();
        let identities: Vec<_> = trace.creates.keys().copied().collect();
        for identity in identities {
            trace.acknowledge_create(TaskStatus::created(identity).with_installed());
        }
        trace.pump();
        assert!(trace.closes.is_empty());
        trace.transport.blocked_update = Some(trace.producer.backend_process_id());
        trace
    }

    fn with_dispatched_creates() -> Self {
        let trace = Self::with_chain(ExchangeDeliveryRequirement::DemandDriven);
        assert_eq!(trace.creates.len(), 4);
        trace
    }

    fn with_chain(delivery: ExchangeDeliveryRequirement) -> Self {
        let mut start = start();
        let strict = delivery == ExchangeDeliveryRequirement::MustDrain;
        let graph = &mut start.plan.graph;
        let root = graph.root;
        let mut rows = graph.tasks.values();
        let root_part = rows.next().unwrap().participation;
        let middle = rows.next().unwrap().identity;
        let leaf = rows.next().unwrap();
        let producer = leaf.identity;
        let producer_part = leaf.participation;
        // Match the original three-stage chain. Root and leaf share a BE;
        // each middle destination lives on a distinct other BE.
        let root = TaskIdentity::new(
            root.query_execution_id(),
            root.stage_id(),
            root.task_id(),
            producer.backend_process_id(),
        );
        graph.root = root;
        let root_task = graph.tasks.get_mut(&root.task_id()).unwrap();
        root_task.identity = root;
        root_task.participation = producer_part;
        let mut middle_tasks = vec![middle];
        if strict {
            start.plan.establish_material.remove(&root_part);
            start
                .plan
                .admission_epochs
                .remove(&root_part.backend_process_id());
            start
                .plan
                .preparation_positions
                .remove(&root_part.backend_process_id());
            graph
                .tasks
                .get_mut(&producer.task_id())
                .unwrap()
                .split_plan_nodes = Box::from([PlanNodeId::new(5).unwrap()]);
        } else {
            let sibling = TaskIdentity::new(
                root.query_execution_id(),
                middle.stage_id(),
                TaskId::new(4).unwrap(),
                root_part.backend_process_id(),
            );
            let mut sibling_task = graph.tasks[&middle.task_id()].clone();
            sibling_task.identity = sibling;
            sibling_task.participation = root_part;
            sibling_task.task_key = UniqueId::new(4, 1);
            sibling_task.creation.resource = PayloadRef(id(4));
            graph.tasks.insert(sibling.task_id(), sibling_task);
            middle_tasks.push(sibling);
        }
        graph.stages.insert(root.stage_id(), Box::from([root]));
        graph
            .stages
            .insert(middle.stage_id(), middle_tasks.clone().into_boxed_slice());
        let root_edge = graph
            .edges
            .get_mut(&ExchangeEdgeId::new(1).unwrap())
            .unwrap();
        root_edge.delivery = delivery;
        root_edge.producers = middle_tasks.clone().into_boxed_slice();
        root_edge.destinations = Box::from([root]);
        let leaf_edge = graph
            .edges
            .get_mut(&ExchangeEdgeId::new(2).unwrap())
            .unwrap();
        leaf_edge.delivery = delivery;
        leaf_edge.destinations = middle_tasks.clone().into_boxed_slice();

        let tasks: Vec<_> = graph.tasks.values().map(|task| task.identity).collect();
        let core = Core::new(start).unwrap();
        let mut trace = Self {
            core,
            producer,
            middle: middle_tasks,
            transport: ExchangeTurn {
                blocked_create: strict.then_some(producer.backend_process_id()),
                blocked_update: None,
                refused: 0,
            },
            next: 100,
            replies: Vec::new(),
            openings: BTreeMap::new(),
            creates: BTreeMap::new(),
            closes: BTreeMap::new(),
            close_sends: 0,
            producer_opens: 0,
            cancels: Vec::new(),
            quiesces: Vec::new(),
            split_notices: Vec::new(),
            split_sends: Vec::new(),
        };
        trace.dispatch_creates(tasks);
        trace
    }

    fn action(&mut self) -> AttemptActionId {
        self.next += 1;
        AttemptActionId::new(id(self.next))
    }

    fn dispatch_creates(&mut self, tasks: Vec<TaskIdentity>) {
        let participants: Vec<ParticipationRef> = self
            .core
            .execution
            .graph()
            .participations()
            .copied()
            .collect();
        for (index, participation) in participants.into_iter().enumerate() {
            let execution = &mut self.core.execution;
            let request = admission_intent(execution, participation);
            let ticket = dispatch_admission(
                execution,
                request,
                index as u64 + 1,
                OperationOutcome::Accepted,
            );
            execution.apply_dispatched_reply(&ticket).1.unwrap();

            let action = self.action();
            let grant = QueuedInputGrantRef::new(id(self.next), SendGeneration::new(action));
            let intent = queue_establish(&mut self.core.execution, participation, grant);
            let OperationIntent::EstablishParticipation {
                initial_lease_sequence,
                initial_lease_valid_for,
                ..
            } = intent
            else {
                panic!("expected original Establish");
            };
            let send = self.action();
            let execution = &mut self.core.execution;
            let BatchDispatch::Ready(AttemptEffect {
                body: EffectBody::SendBatch { batch, .. },
                ..
            }) = execution
                .advance_send_batch(&mut self.core.dispatch, send, &mut self.transport, |_| {
                    Ok(())
                })
                .unwrap()
            else {
                panic!("expected fixture Establish dispatch");
            };
            assert_eq!(batch.queued_inputs.as_ref(), [grant]);
            execution.observe_send_started(send);
            execution
                .establish_send_started(participation, intent.operation_id())
                .unwrap();
            let active = ParticipationReceipt::new(participation, ParticipationState::Active)
                .with_lease(LeaseReceipt::new(
                    initial_lease_sequence,
                    initial_lease_valid_for,
                    Duration::from_secs(9),
                ));
            execution
                .apply_dispatched_reply(&worker_accepted(
                    &intent,
                    grant.generation(),
                    Some(AckPayload::Participation(active)),
                ))
                .1
                .unwrap();
            let now = execution.monotonic_now();
            self.core
                .subscriptions
                .activate(now, participation)
                .unwrap();
        }

        for task in tasks {
            let action = self.action();
            let Some(AttemptEffect {
                body:
                    EffectBody::AdmitQueuedInput {
                        operation,
                        generation,
                        ..
                    },
                ..
            }) = self
                .core
                .execution
                .begin_create_admission(task, action)
                .unwrap()
            else {
                panic!("expected exact original Create admission for {task}");
            };
            let grant = QueuedInputGrantRef::new(id(self.next), generation);
            assert!(matches!(
                self.core
                    .execution
                    .observe_create_grant(operation.operation_id(), grant)
                    .unwrap(),
                CreateGrantProgress::Queued
            ));
        }
        self.run_round(None);
    }

    fn acknowledge_create(&mut self, status: TaskStatus) {
        let identity = status.identity();
        let (request, generation) = &self.creates[&identity];
        self.replies.push(worker_accepted(
            request,
            *generation,
            Some(AckPayload::Create(Box::new(CreateTaskReceipt::new(
                identity,
                Vec::new(),
                status,
            )))),
        ));
    }

    fn begin_prefix(&mut self) {
        let replies = std::mem::take(&mut self.replies);
        let now = self.core.execution.monotonic_now();
        let source = self.core.sources.native;
        let first = self.next + 1;
        self.next += replies.len() as u64;
        let tail = (!replies.is_empty()).then(|| AttemptInputId(id(self.next)));
        self.core
            .begin_native_prefix(now, AttemptSourceCut { source, tail })
            .unwrap();
        for (offset, reply) in replies.into_iter().enumerate() {
            let action = self
                .core
                .apply_native(
                    now,
                    source,
                    AttemptInputId(id(first + offset as u64)),
                    &ExecutorFact::Operation(reply),
                    |_, _| panic!("there is no actor Abort in this trace"),
                )
                .unwrap();
            assert!(action.is_none());
        }
    }

    fn run_round(&mut self, observations: Option<ObservationBatch>) -> bool {
        self.begin_prefix();
        let now = self.core.execution.monotonic_now();
        if let Some(observations) = observations {
            assert!(
                self.core
                    .apply_observations(now, self.core.sources.observation, observations)
                    .unwrap()
                    .is_none()
            );
        }
        self.core.begin_round_tail(true).unwrap();
        for _ in 0..64 {
            let action = self.action();
            match self
                .core
                .advance_round_tail(
                    now,
                    action,
                    AttemptRequestToken::new(id(self.next)),
                    &mut self.transport,
                )
                .unwrap()
            {
                RoundTailStep::Complete(result) => {
                    result.unwrap();
                    return !self.replies.is_empty();
                }
                RoundTailStep::Effect(AttemptEffect {
                    id,
                    body: EffectBody::Publish(Publication::ProtocolBatch(_)),
                    ..
                }) => {
                    self.core.protocol_publication_applied(
                        id,
                        Ok(PublicationApplied::ProtocolBatch { published: true }),
                    );
                }
                RoundTailStep::Effect(AttemptEffect {
                    id,
                    body: EffectBody::SendBatch { batch, .. },
                    ..
                }) => {
                    self.core.execution.observe_send_started(id);
                    for (operation, grant) in batch.operations.iter().zip(&batch.queued_inputs) {
                        let generation = grant.generation();
                        let payload = match operation {
                            OperationIntent::CreateTask { identity, .. } => {
                                assert!(
                                    self.creates
                                        .insert(*identity, (operation.clone(), generation))
                                        .is_none(),
                                    "the original Create was sent again for {identity}"
                                );
                                continue;
                            }
                            OperationIntent::UpdateTask {
                                identity, domains, ..
                            } => Some(self.update_receipt(*identity, domains)),
                            OperationIntent::CancelTask(cancel) => {
                                self.cancels.push((cancel.identity(), cancel.reason()));
                                None
                            }
                            OperationIntent::QuiesceParticipation(request) => {
                                self.quiesces.push(request.participation());
                                continue;
                            }
                            _ => panic!("unexpected operation {operation:?}"),
                        };
                        self.replies
                            .push(worker_accepted(operation, generation, payload));
                    }
                }
                RoundTailStep::Effect(AttemptEffect {
                    body: EffectBody::OpenSubscription(request),
                    ..
                }) => {
                    assert_eq!(
                        self.openings
                            .insert(request.participation, request.generation),
                        None
                    );
                }
                RoundTailStep::Effect(AttemptEffect {
                    body: EffectBody::AdjustReadWindow(request),
                    ..
                }) => {
                    assert_eq!(self.openings[&request.participation], request.generation);
                }
                RoundTailStep::Effect(AttemptEffect {
                    body:
                        EffectBody::ReleaseCreationPayload { .. }
                        | EffectBody::ReleaseQueuedInput { .. },
                    ..
                }) => {}
                RoundTailStep::Effect(AttemptEffect {
                    body:
                        body @ (EffectBody::PumpUpdateAdmitted { .. }
                        | EffectBody::PumpOperationReleased { .. }
                        | EffectBody::PumpOperationSettled { .. }),
                    ..
                }) => self.split_notices.push(body),
                other => panic!("unexpected original round step {other:?}"),
            }
        }
        panic!("exchange round did not settle");
    }

    fn update_receipt(
        &mut self,
        identity: TaskIdentity,
        domains: &[TaskDomainIntent],
    ) -> AckPayload {
        let receipt = match domains {
            [TaskDomainIntent::OpenExchangeEdges { version, edges }] => {
                if identity == self.producer {
                    assert_eq!(edges.as_slice(), [ExchangeEdgeId::new(2).unwrap()]);
                    self.producer_opens += 1;
                }
                TaskDomainReceipt::OpenExchangeEdges {
                    accepted_version: *version,
                    opened: edges.clone(),
                    progression: DomainProgression::Apply,
                }
            }
            [
                TaskDomainIntent::CloseExchangeDestination {
                    version,
                    edge,
                    destination,
                },
            ] => {
                self.close_sends += 1;
                if identity == self.producer {
                    assert_eq!(*edge, ExchangeEdgeId::new(2).unwrap());
                    *self.closes.entry(*destination).or_default() += 1;
                }
                TaskDomainReceipt::CloseExchangeDestination {
                    accepted_version: *version,
                    progression: DomainProgression::Apply,
                }
            }
            [TaskDomainIntent::SplitAssignment { node, offer, .. }] => {
                self.split_sends.push((identity, *node, *offer));
                let (_, last) = offer
                    .range()
                    .expect("the strict-tail fixture sends a split batch");
                let watermark = SplitWatermark::empty().apply_batch(last, offer.no_more_splits());
                TaskDomainReceipt::SplitAssignment {
                    nodes: vec![PlanNodeSplitReceipt::new(*node, watermark).with_queued_splits(1)],
                    progression: DomainProgression::Apply,
                }
            }
            _ => panic!("unexpected exchange input {domains:?}"),
        };
        AckPayload::Update(UpdateTaskReceipt::new(identity, vec![receipt]))
    }

    fn pump(&mut self) {
        let tasks: Vec<_> = self
            .core
            .execution
            .graph()
            .tasks()
            .map(|task| task.identity())
            .collect();
        for _ in 0..16 {
            let mut progressed = self.run_round(None);
            // Use the production edge input and task-update candidates after
            // the tail, preserving the same cut before each dispatch.
            let mut after = None;
            while let Some(producer) = self.core.execution.edge_producer_after(after) {
                let action = self.action();
                match self
                    .core
                    .execution
                    .advance_edge_admission(producer, action)
                    .unwrap()
                {
                    EdgeAdmissionProgress::Admitting(effect) => {
                        self.grant_update_or_cancel(effect);
                        progressed = true;
                        after = Some(producer);
                    }
                    EdgeAdmissionProgress::Discarded => progressed = true,
                    EdgeAdmissionProgress::Blocked => after = Some(producer),
                }
            }
            for &task in &tasks {
                if let Some(cancel) = self.core.execution.next_task_cancel_intent(task).unwrap() {
                    let action = self.action();
                    let effect = self
                        .core
                        .execution
                        .begin_lifecycle_admission(cancel, action)
                        .unwrap();
                    self.grant_update_or_cancel(effect);
                    progressed = true;
                }
                let action = self.action();
                let (queued, replay) = self
                    .core
                    .execution
                    .advance_update_send(task, action)
                    .unwrap();
                assert!(replay.is_none());
                progressed |= queued;
            }
            if !progressed {
                return;
            }
        }
        panic!("exchange dispatch did not become quiet");
    }

    fn grant_update_or_cancel(&mut self, effect: AttemptEffect) {
        let EffectBody::AdmitQueuedInput {
            operation,
            generation,
            ..
        } = effect.body
        else {
            panic!("expected exact edge or cancel admission");
        };
        let grant = QueuedInputGrantRef::new(id(self.next), generation);
        let result = self
            .core
            .execution
            .observe_lifecycle_grant(operation.operation_id(), grant)
            .unwrap();
        assert!(matches!(
            (operation, result),
            (
                OperationIntent::UpdateTask { .. },
                CreateGrantProgress::Waiting
            ) | (OperationIntent::CancelTask(_), CreateGrantProgress::Queued)
        ));
    }

    fn authorize_close(&mut self) {
        self.core
            .execution
            .stand_down_task_normally(self.middle[0])
            .unwrap();
    }
}

#[test]
fn an_accepted_create_without_its_receipt_is_refused() {
    use novarocks_execution_contract::OperationKind;

    let mut trace = DrainTrace::with_dispatched_creates();
    let task = trace.producer;
    let (request, generation) = &trace.creates[&task];
    let execution = &mut trace.core.execution;
    let credit_before_reply = execution.dispatcher().in_flight_operations();
    let reply = worker_accepted(request, *generation, None);

    let (effect, outcome) = execution.apply_dispatched_reply(&reply);
    assert_eq!(
        outcome,
        Err(TaskExecutionError::MissingReceipt(
            OperationKind::CreateTask
        ))
    );
    assert!(effect.is_none());
    assert_eq!(
        execution.dispatcher().in_flight_operations(),
        credit_before_reply - 1
    );
    let task = execution.task(task.task_id()).unwrap();
    assert!(!task.create_ownership_proven());
    assert!(task.status().is_none());
}

fn permutations<T: Copy>(events: &mut [T], position: usize, traces: &mut Vec<Vec<T>>) {
    if position == events.len() {
        traces.push(events.to_vec());
        return;
    }
    for next in position..events.len() {
        events.swap(position, next);
        permutations(events, position + 1, traces);
        events.swap(position, next);
    }
}

#[test]
fn original_authorized_close_and_create_reply_permutations() {
    #[derive(Clone, Copy, Debug)]
    enum Event {
        AuthorizeClose,
        AcceptedAck,
        Installed,
        NeededSiblingReady,
        QueueAvailable,
    }

    let mut traces = Vec::new();
    permutations(
        &mut [
            Event::AuthorizeClose,
            Event::AcceptedAck,
            Event::Installed,
            Event::NeededSiblingReady,
            Event::QueueAvailable,
        ],
        0,
        &mut traces,
    );

    let (mut closes, mut opens, mut late_ack) = (0, 0, 0);
    for events in &traces {
        let mut trace = DrainTrace::with_dispatched_creates();
        let closed = trace.middle[0];
        let needed = trace.middle[1];
        let participation = trace
            .core
            .execution
            .graph()
            .task(closed.task_id())
            .unwrap()
            .participation();
        trace.acknowledge_create(TaskStatus::created(trace.producer).with_installed());
        trace.transport.blocked_update = Some(trace.producer.backend_process_id());
        trace.pump();
        let now = trace.core.execution.monotonic_now();
        let installed = ReceivedObservation {
            participation,
            generation: trace.openings[&participation],
            received_at: now,
            opening: ObservationOpening {
                started_at: now,
                opened_at: Some(now),
            },
            event: ObservationEvent::Fact {
                source_revision: None,
                fact: ObservationFact::Status(
                    TaskStatus::try_new(
                        closed,
                        TaskStatusVersion::new(2).unwrap(),
                        TaskState::Planned,
                        None,
                        TaskOutputFacts::default(),
                    )
                    .unwrap()
                    .with_installed(),
                ),
            },
        };

        let mut authorized = false;
        for event in events {
            match event {
                Event::AuthorizeClose => {
                    authorized = true;
                    trace.authorize_close();
                    trace.authorize_close();
                }
                Event::AcceptedAck => {
                    late_ack += usize::from(authorized);
                    trace.acknowledge_create(TaskStatus::created(closed));
                }
                Event::Installed => {
                    trace.run_round(Some(
                        ObservationBatch::new(vec![installed.clone(), installed.clone()]).unwrap(),
                    ));
                }
                Event::NeededSiblingReady => {
                    trace.acknowledge_create(TaskStatus::created(needed).with_installed());
                }
                Event::QueueAvailable => trace.transport.blocked_update = None,
            }
            trace.pump();
            if authorized {
                let action = trace.action();
                assert!(
                    trace
                        .core
                        .execution
                        .begin_create_admission(closed, action)
                        .unwrap()
                        .is_none(),
                    "{events:?}: local close restored A Create"
                );
            }
        }

        assert_eq!(
            trace.closes,
            BTreeMap::from([(closed, 1)]),
            "{events:?}: lost or duplicated A close, or B was closed"
        );
        assert_eq!(trace.producer_opens, 1, "{events:?}: A blocked necessary B");
        for (target, reason) in trace.cancels {
            assert_eq!(
                (target, reason),
                (closed, CancelReason::UpstreamNoLongerNeeded),
                "{events:?}: unexpected cancellation"
            );
        }
        assert!(
            !trace
                .core
                .execution
                .task(closed.task_id())
                .unwrap()
                .create_pending(),
            "{events:?}: replay eligibility revived"
        );
        let sequence = SplitSequence::new(1).unwrap();
        let (disposition, intent) = trace
            .core
            .execution
            .prepare_task_update(
                closed,
                TaskDomainIntent::SplitAssignment {
                    node: PlanNodeId::new(5).unwrap(),
                    offer: SplitOffer::batch(sequence, sequence, false).unwrap(),
                    payload: PayloadRef(id(500)),
                    queued_bytes: 128,
                },
            )
            .unwrap();
        assert_eq!(
            disposition,
            UpdateAdmission::DiscardedNoLongerNeeded,
            "{events:?}: late ownership restored ordinary input"
        );
        assert!(intent.is_none(), "{events:?}: discarded input was prepared");
        closes += trace.closes.values().sum::<usize>();
        opens += trace.producer_opens;
    }
    assert_eq!(traces.len(), 120);
    assert_eq!((closes, opens, late_ack), (120, 120, 60));
}

#[test]
fn original_normal_drain_and_backpressure_permutations() {
    #[derive(Clone, Copy, Debug)]
    enum Event {
        AuthorizeClose,
        QueueAvailable,
        NormalDrain,
        DuplicateAuthorization,
    }

    let mut traces = Vec::new();
    permutations(
        &mut [
            Event::AuthorizeClose,
            Event::QueueAvailable,
            Event::NormalDrain,
            Event::DuplicateAuthorization,
        ],
        0,
        &mut traces,
    );
    let mut delivered = 0;
    for events in &traces {
        let mut trace = DrainTrace::new();
        let mut available = false;
        for event in events {
            match event {
                Event::AuthorizeClose | Event::DuplicateAuthorization => trace.authorize_close(),
                Event::QueueAvailable => {
                    available = true;
                    trace.transport.blocked_update = None;
                }
                Event::NormalDrain => trace.core.execution.begin_normal_drain(),
            }
            trace.pump();
            if !available {
                assert!(trace.closes.is_empty(), "{events:?}");
            }
        }
        assert_eq!(
            trace.closes,
            BTreeMap::from([(trace.middle[0], 1), (trace.middle[1], 1)]),
            "{events:?}"
        );
        assert_eq!(
            trace.transport.refused > 0,
            !matches!(events[0], Event::QueueAvailable),
            "{events:?}: a close preceding capacity must face real dispatch refusal"
        );
        delivered += trace.closes.values().sum::<usize>();
    }
    assert_eq!((traces.len(), delivered), (24, 48));

    let failure = TerminationDetail::Failed(TaskFailure::new(
        FailureFact::new(
            FailureClass::Data,
            FailureReason::new(
                NonZeroU16::new(u16::MAX).unwrap(),
                NonZeroU16::new(1).unwrap(),
            ),
        ),
        SafeDetail::truncating("exploration failure"),
    ));
    for close_first in [false, true] {
        let mut trace = DrainTrace::new();
        if close_first {
            trace.authorize_close();
        }
        let identity = trace.middle[0];
        let participation = trace
            .core
            .execution
            .graph()
            .task(identity.task_id())
            .unwrap()
            .participation();
        // Observe Running, Failing and Failed in the actual subscription's
        // first generation, preserving the Worker status order.
        let now = trace.core.execution.monotonic_now();
        let generation = trace.openings[&participation];
        let mut observations = Vec::new();
        for (version, state, termination) in [
            (2, TaskState::Running, None),
            (3, TaskState::Failing, Some(failure.clone())),
            (4, TaskState::Failed, Some(failure.clone())),
        ] {
            let status = TaskStatus::try_new(
                identity,
                TaskStatusVersion::new(version).unwrap(),
                state,
                termination,
                TaskOutputFacts::default(),
            )
            .unwrap();
            observations.push(ReceivedObservation {
                participation,
                generation,
                received_at: now,
                opening: ObservationOpening {
                    started_at: now,
                    opened_at: Some(now),
                },
                event: ObservationEvent::Fact {
                    source_revision: None,
                    fact: ObservationFact::Status(status),
                },
            });
        }
        trace.run_round(Some(ObservationBatch::new(observations).unwrap()));
        if !close_first {
            trace.authorize_close();
        }
        assert_eq!(
            trace.core.execution.failure_cause(),
            Some(&failure),
            "close_first={close_first}"
        );
    }
}

#[test]
fn strict_drain_preserves_original_create_split_and_late_exchange_open() {
    let mut trace = DrainTrace::with_chain(ExchangeDeliveryRequirement::MustDrain);
    let leaf = trace.producer;
    let middle = trace.middle[0];
    let root = trace.core.execution.graph().root_identity();
    assert_eq!(trace.creates.keys().copied().collect::<Vec<_>>(), [middle]);
    assert_eq!(trace.core.execution.dispatcher().queued_items(), 2);

    trace.core.execution.begin_normal_drain();
    assert!(!trace.core.execution.normal_drain_started());
    let pump = PayloadRef(id(700));
    let payload = PayloadRef(id(701));
    let delivery = SplitDeliveryRef(id(702));
    let node = PlanNodeId::new(5).unwrap();
    let sequence = SplitSequence::new(1).unwrap();
    let offer = SplitOffer::batch(sequence, sequence, true).unwrap();
    trace.core.execution.bind_split_pump(Some(pump));
    assert!(matches!(
        trace.core.execution.split_pump_lifecycle(true),
        Some(EffectBody::StartPump { pump: original, kind: PumpKind::SplitAssignment })
            if original == pump
    ));
    trace
        .core
        .execution
        .accept_split_delivery(
            pump,
            delivery,
            leaf,
            Box::from([TaskDomainIntent::SplitAssignment {
                node,
                offer,
                payload,
                queued_bytes: 128,
            }]),
        )
        .unwrap();
    let action = trace.action();
    let effect = trace
        .core
        .execution
        .prepare_split_delivery(leaf, action)
        .unwrap()
        .unwrap();
    let EffectBody::AdmitQueuedInput {
        operation: OperationIntent::UpdateTask {
            envelope, identity, ..
        },
        payload: QueuedInputPayload::NewTaskUpdate(original),
        ..
    } = &effect.body
    else {
        panic!("expected the strict producer's original split admission");
    };
    let split_operation = envelope.operation_id();
    assert_eq!((*identity, *original), (leaf, payload));
    trace.grant_update_or_cancel(effect);
    trace.pump();
    assert!(trace.split_sends.is_empty());
    assert!(trace.close_sends == 0 && trace.cancels.is_empty());

    trace.transport.blocked_create = None;
    trace.pump();
    assert!(trace.creates.contains_key(&leaf));
    assert!(trace.creates.contains_key(&root));
    for task in [root, leaf] {
        trace.acknowledge_create(TaskStatus::created(task).with_installed());
    }
    trace.pump();
    assert_eq!(trace.split_sends, [(leaf, node, offer)]);
    assert_eq!(
        trace.producer_opens, 0,
        "the strict destination still lacks Installed"
    );
    assert!(!trace.core.execution.normal_drain_started());
    assert!(trace.close_sends == 0 && trace.cancels.is_empty());
    assert!(
        matches!(
            trace.split_notices.as_slice(),
            [
                EffectBody::PumpUpdateAdmitted { pump: p1, delivery: d1, settlement: None },
                EffectBody::PumpOperationReleased { pump: p2, delivery: d2, operation: o2 },
                EffectBody::PumpOperationSettled {
                    pump: p3, delivery: d3, operation: o3,
                    settlement: SplitDeliverySettlement::Accepted(nodes),
                },
            ] if [*p1, *p2, *p3] == [pump; 3]
                && [*d1, *d2, *d3] == [delivery; 3]
                && [*o2, *o3] == [split_operation; 2]
                && nodes.as_ref() == [AcceptedPlanNode {
                    plan_node_id: 5, accepted_through_sequence: 1, no_more_splits: true, queued_splits: 1,
                }]
        ),
        "{:?}",
        trace.split_notices
    );

    trace.acknowledge_create(TaskStatus::created(middle).with_installed());
    trace.pump();
    assert_eq!(trace.producer_opens, 1);
    assert!(!trace.core.execution.normal_drain_started());
    assert!(trace.close_sends == 0 && trace.cancels.is_empty());

    let participants: Vec<_> = trace
        .core
        .execution
        .graph()
        .participations()
        .copied()
        .collect();
    for task in [root, middle, leaf] {
        let participation = trace
            .core
            .execution
            .graph()
            .task(task.task_id())
            .unwrap()
            .participation();
        let now = trace.core.execution.monotonic_now();
        let observations = [
            (2, TaskState::Running, false),
            (3, TaskState::Flushing, false),
            (4, TaskState::Finished, true),
        ]
        .map(|(version, state, complete)| ReceivedObservation {
            participation,
            generation: trace.openings[&participation],
            received_at: now,
            opening: ObservationOpening {
                started_at: now,
                opened_at: Some(now),
            },
            event: ObservationEvent::Fact {
                source_revision: None,
                fact: ObservationFact::Status(
                    TaskStatus::try_new(
                        task,
                        TaskStatusVersion::new(version).unwrap(),
                        state,
                        None,
                        TaskOutputFacts::new(complete),
                    )
                    .unwrap(),
                ),
            },
        });
        trace.run_round(Some(ObservationBatch::new(observations.into()).unwrap()));
        assert_eq!(trace.core.execution.normal_drain_started(), task == leaf);
        if task != leaf {
            for &participation in &participants {
                assert!(
                    trace
                        .core
                        .execution
                        .next_quiesce_intent(participation)
                        .is_none()
                );
            }
        }
    }
    for &participation in &participants {
        let operation = trace
            .core
            .execution
            .next_quiesce_intent(participation)
            .unwrap();
        let action = trace.action();
        let effect = trace
            .core
            .execution
            .begin_lifecycle_admission(operation, action)
            .unwrap();
        let EffectBody::AdmitQueuedInput {
            operation,
            generation,
            ..
        } = effect.body
        else {
            panic!("expected the exact normal Quiesce admission after all strict terminals");
        };
        let grant = QueuedInputGrantRef::new(id(trace.next), generation);
        assert!(matches!(
            trace
                .core
                .execution
                .observe_lifecycle_grant(operation.operation_id(), grant)
                .unwrap(),
            CreateGrantProgress::Queued
        ));
    }
    trace.run_round(None);
    trace.quiesces.sort_unstable();
    assert_eq!(trace.quiesces, participants);
    assert!(trace.close_sends == 0 && trace.cancels.is_empty());
    assert_eq!(trace.core.execution.failure_cause(), None);
}
