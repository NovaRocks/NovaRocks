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
    effect::{EffectClass, Publication, TaskDomainIntent},
    execution::{
        CreateGrantProgress, EdgeAdmissionProgress,
        ack_phase::{admission_intent, dispatch_admission, queue_establish},
    },
    input::{
        AckPayload, AttemptInput, AttemptInputClaim, OperationReply, OperationReplyResult,
        QueuedInputGrantRef, SendGeneration,
    },
};
use novarocks_coordination_contract::{
    AttemptAuthorization, AttemptAuthorizationDecision, AttemptAuthorizationRef,
    AttemptToLogicalBody,
};
use novarocks_execution_contract::{
    AdmissionTicketId, CreateTaskReceipt, DomainProgression, ExchangeDeliveryRequirement,
    ExchangeEdgeId, LeaseReceipt, LeaseSequence, LeaseValidFor, OperationEnvelope,
    OperationOutcome, ParticipationAdmissionTicketReceipt, ParticipationReceipt,
    ParticipationState, SafeDetail, TaskDomainReceipt, TaskFailure, UpdateTaskReceipt,
    failure::{FailureClass, FailureFact, FailureReason},
    operation::OperationResult,
};
use novarocks_types::{TaskId, UniqueId};
use std::num::NonZeroU16;

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

struct ExchangeTurn {
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
        if lane == DispatchLane::Update && self.blocked_update == Some(backend) {
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
    middle: [TaskIdentity; 2],
    transport: ExchangeTurn,
    next: u64,
    replies: Vec<OperationObservation>,
    openings: BTreeMap<ParticipationRef, NonZeroU64>,
    closes: BTreeMap<TaskIdentity, usize>,
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
        let mut start = start();
        let graph = &mut start.plan.graph;
        let root = graph.root;
        let mut rows = graph.tasks.values();
        let root_part = rows.next().unwrap().participation;
        let middle = rows.next().unwrap().identity;
        let leaf = rows.next().unwrap();
        let producer = leaf.identity;
        let producer_part = leaf.participation;
        // Match the original three-stage chain. Root and leaf share a BE;
        // the middle's two exact destinations live on distinct other BEs.
        let root = TaskIdentity::new(
            root.query_execution_id(),
            root.stage_id(),
            root.task_id(),
            producer.backend_process_id(),
        );
        let sibling = TaskIdentity::new(
            root.query_execution_id(),
            middle.stage_id(),
            TaskId::new(4).unwrap(),
            root_part.backend_process_id(),
        );
        graph.root = root;
        let root_task = graph.tasks.get_mut(&root.task_id()).unwrap();
        root_task.identity = root;
        root_task.participation = producer_part;
        let mut sibling_task = graph.tasks[&middle.task_id()].clone();
        sibling_task.identity = sibling;
        sibling_task.participation = root_part;
        sibling_task.task_key = UniqueId::new(4, 1);
        sibling_task.creation.resource = PayloadRef(id(4));
        graph.tasks.insert(sibling.task_id(), sibling_task);
        graph.stages.insert(root.stage_id(), Box::from([root]));
        graph
            .stages
            .insert(middle.stage_id(), Box::from([middle, sibling]));
        let root_edge = graph
            .edges
            .get_mut(&ExchangeEdgeId::new(1).unwrap())
            .unwrap();
        root_edge.producers = Box::from([middle, sibling]);
        root_edge.destinations = Box::from([root]);
        let leaf_edge = graph
            .edges
            .get_mut(&ExchangeEdgeId::new(2).unwrap())
            .unwrap();
        leaf_edge.delivery = ExchangeDeliveryRequirement::DemandDriven;
        leaf_edge.destinations = Box::from([middle, sibling]);

        let core = Core::new(start).unwrap();
        let mut trace = Self {
            core,
            producer,
            middle: [middle, sibling],
            transport: ExchangeTurn {
                blocked_update: None,
                refused: 0,
            },
            next: 100,
            replies: Vec::new(),
            openings: BTreeMap::new(),
            closes: BTreeMap::new(),
        };
        trace.prepare_installed_tasks([root, middle, producer, sibling]);
        trace.pump();
        assert!(trace.closes.is_empty());
        trace.transport.blocked_update = Some(producer.backend_process_id());
        trace
    }

    fn action(&mut self) -> AttemptActionId {
        self.next += 1;
        AttemptActionId::new(id(self.next))
    }

    fn prepare_installed_tasks(&mut self, tasks: [TaskIdentity; 4]) {
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
        assert_eq!(self.replies.len(), tasks.len());
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
                        let payload = match operation {
                            OperationIntent::CreateTask { identity, .. } => {
                                Some(AckPayload::Create(Box::new(CreateTaskReceipt::new(
                                    *identity,
                                    Vec::new(),
                                    TaskStatus::created(*identity).with_installed(),
                                ))))
                            }
                            OperationIntent::UpdateTask {
                                identity, domains, ..
                            } => Some(self.update_receipt(*identity, domains)),
                            OperationIntent::CancelTask(_) => None,
                            _ => panic!("unexpected operation {operation:?}"),
                        };
                        self.replies
                            .push(worker_accepted(operation, grant.generation(), payload));
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
                if identity == self.producer {
                    assert_eq!(*edge, ExchangeEdgeId::new(2).unwrap());
                    *self.closes.entry(*destination).or_default() += 1;
                }
                TaskDomainReceipt::CloseExchangeDestination {
                    accepted_version: *version,
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
fn original_normal_drain_and_backpressure_permutations() {
    #[derive(Clone, Copy, Debug)]
    enum Event {
        AuthorizeClose,
        QueueAvailable,
        NormalDrain,
        DuplicateAuthorization,
    }

    fn permutations(events: &mut [Event], position: usize, traces: &mut Vec<Vec<Event>>) {
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
