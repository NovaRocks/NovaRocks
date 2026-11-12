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

use super::tests::logical_execution_with_abort_port;
use crate::{
    control_runtime::transport_withdrawal::AttemptEffectEnvelope,
    native::task_transport::wire::{AttemptWireFacts, encode_operation},
    query_execution::{
        lifecycle_plan::QueryCredentialLeases, native_execution_adapter::LogicalAbortRouter,
    },
    task_execution::{
        bridge::{
            abort::{AbortSettlementOwner, AttemptAbortIngress},
            qa::{AttemptQaBridge, AttemptQaReply},
        },
        host::{
            adapter::tests::start,
            custody::tests::WithdrawalInbox,
            transport::{AttemptEstablishMaterial, FrozenSendRequests, PreparedSendBatch},
        },
        sources::AttemptEstablishFacts,
    },
};
use novarocks_attempt_protocol::{
    AttemptCore, AttemptStep, Core, InputSelection,
    capacity::{QueueStage, ReservationToken, StepContext},
    config::DispatchLane,
    effect::{AttemptEffect, EffectBody, OperationIntent as ProtocolIntent, Publication},
    error::TaskExecutionError as ProtocolError,
    failure::AttemptFailure,
    input::{
        AckPayload, AttemptInput, AttemptInputClaim, AttemptInputId, AttemptSourceCut,
        AttemptSourceId, ExecutorFact, LocalDispatchResult, OperationObservation, OperationReply,
        OperationReplyResult, PublicationApplied, QueuedInputGrantRef, SendGeneration,
    },
    operation::{IngressRejection, OperationDispatchResult},
    start::{AttemptSources, AttemptStart},
};
use novarocks_control_runtime::{actor_turn::InputPhase, transaction::RuntimeLedger};
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    identity::Generation,
    mailbox::InputClass,
    scope::{WorkAdmission, WorkKind},
};
use novarocks_coordination_contract::{
    AttemptActionId, AttemptAuthorization, AttemptAuthorizationDecision, AttemptIssueSettlement,
    AttemptToLogicalBody, LogicalToAttemptBody, MonotonicInstant,
};
use novarocks_execution_contract::{
    AbortCause, AdmissionTicketId, LeaseReceipt, OperationKind, OperationOutcome,
    ParticipationAdmissionTicketReceipt, ParticipationReceipt, ParticipationRef,
    ParticipationState, TaskOperationId, operation::OperationResult,
};
use novarocks_query_application::coordination::{
    EstablishIssueError, EstablishIssueState, EstablishWorkerSettlement,
    LogicalExecutionActorError, LogicalExecutionJoinReadiness,
};
use novarocks_types::BackendProcessId;
use std::{num::NonZeroU64, sync::Arc, time::Duration};

fn positive(value: u64) -> NonZeroU64 {
    NonZeroU64::new(value).unwrap()
}

// The actual Core and QA bridge own decisions and authorizations. The test
// supplies queue references and the ordered Native source facts.
struct Turn;
impl StepContext for Turn {
    fn charge(&mut self, _: usize) -> bool {
        true
    }

    fn reserve_queue_stage(
        &mut self,
        _: DispatchLane,
        control: bool,
        _: BackendProcessId,
        records: usize,
        _: usize,
    ) -> QueueStage {
        assert!(!control);
        assert_eq!(records, 1);
        QueueStage::Granted(ReservationToken(positive(39)))
    }
}

struct Trace {
    core: Core,
    sources: AttemptSources,
    now: MonotonicInstant,
    sequence: u64,
    pending: Option<AttemptStep>,
}

impl Trace {
    fn new(start: AttemptStart) -> Self {
        Self {
            sources: start.sources,
            now: start.initial_now,
            core: Core::new(start).unwrap(),
            sequence: 0,
            pending: None,
        }
    }

    fn step(&mut self, source: AttemptSourceId, input: AttemptInput) -> AttemptStep {
        let tail = (source == self.sources.native).then_some(self.sequence + 1);
        self.step_with_native_tail(source, input, tail)
    }

    fn step_with_native_tail(
        &mut self,
        source: AttemptSourceId,
        input: AttemptInput,
        tail: Option<u64>,
    ) -> AttemptStep {
        self.sequence += 1;
        self.core
            .handle(
                self.now,
                AttemptInputClaim {
                    source,
                    input: AttemptInputId(positive(self.sequence)),
                    native_cut: Some(AttemptSourceCut {
                        source: self.sources.native,
                        tail: tail.map(|id| AttemptInputId(positive(id))),
                    }),
                    selected_cut: Some(AttemptSourceCut {
                        source: self.sources.observation,
                        tail: None,
                    }),
                },
                input,
                &mut Turn,
            )
            .unwrap()
    }

    fn drive(&mut self) -> AttemptStep {
        let mut next = self.pending.take();
        for _ in 0..24 {
            let mut step = next.take().unwrap_or_else(|| {
                assert!(self.core.next_deadline().is_some_and(|due| due <= self.now));
                self.step(self.sources.actor, AttemptInput::Timer)
            });
            match step.effect.take() {
                Some(AttemptEffect {
                    id: effect,
                    body: EffectBody::Publish(Publication::ProtocolBatch(_)),
                    ..
                }) => {
                    assert!(!step.round_result.is_some_and(|result| result.is_err()));
                    next = Some(self.step(
                        self.sources.publication,
                        AttemptInput::Executor(ExecutorFact::PublicationApplied {
                            effect,
                            result: Ok(PublicationApplied::ProtocolBatch { published: true }),
                        }),
                    ));
                }
                effect => {
                    step.effect = effect;
                    if step.effect.is_some()
                        || step.round_result.as_ref().is_some_and(Result::is_err)
                    {
                        return step;
                    }
                }
            }
        }
        panic!("expected an original Core effect or round failure");
    }

    fn effect(&mut self) -> AttemptEffect {
        self.drive().effect.expect("original Core effect")
    }

    fn reply(&mut self, reply: AttemptQaReply) -> AttemptStep {
        self.step(self.sources.qa, AttemptInput::Port(reply.reply))
    }

    fn wait_native(&mut self) {
        for _ in 0..16 {
            let turn = self.step(self.sources.actor, AttemptInput::Timer);
            assert!(turn.effect.is_none(), "{turn:?}");
            if turn.selection == InputSelection::Prefer(self.sources.native) {
                return;
            }
        }
        panic!("the original round did not return to its Native source");
    }

    fn dispatch(
        &mut self,
        admission: AttemptEffect,
        record: u64,
    ) -> (AttemptEffect, SendGeneration) {
        let EffectBody::AdmitQueuedInput {
            operation,
            generation,
            ..
        } = admission.body
        else {
            panic!("expected a Core-issued queue request");
        };
        let grant = QueuedInputGrantRef::new(positive(record), generation);
        self.wait_native();
        let accepted = self.step(
            self.sources.transport,
            AttemptInput::Executor(ExecutorFact::QueuedInputGranted {
                operation: operation.operation_id(),
                grant,
                queued_bytes: operation.queued_bytes(),
            }),
        );
        assert!(accepted.effect.is_none(), "{accepted:?}");
        let send = self.effect();
        let EffectBody::SendBatch { batch, .. } = &send.body else {
            panic!("expected Core dispatch of the original operation");
        };
        assert_eq!(batch.queued_inputs.as_ref(), [grant]);
        assert_eq!(batch.operations.len(), 1);
        assert_eq!(batch.operations[0].envelope(), operation.envelope());
        self.wait_native();
        (send, generation)
    }

    fn observation(
        &mut self,
        send: AttemptActionId,
        generation: SendGeneration,
        operation: TaskOperationId,
        kind: OperationKind,
        establish_started: Option<ParticipationRef>,
        result: OperationReplyResult,
    ) {
        let tail = Some(self.sequence + if establish_started.is_some() { 3 } else { 2 });
        let mut started = vec![ExecutorFact::SendStarted {
            effect: send,
            started_at: self.now,
        }];
        if let Some(participation) = establish_started {
            started.push(ExecutorFact::EstablishRpcStarted {
                effect: send,
                operation,
                participation,
                started_at: self.now,
            });
        }
        for fact in started {
            let turn =
                self.step_with_native_tail(self.sources.native, AttemptInput::Executor(fact), tail);
            assert!(turn.effect.is_none(), "{turn:?}");
            assert_eq!(turn.selection, InputSelection::Only(self.sources.native));
        }
        assert!(self.pending.is_none());
        self.pending = Some(self.step(
            self.sources.native,
            AttemptInput::Executor(ExecutorFact::Operation(OperationObservation {
                generation,
                reply: OperationReply {
                    operation_id: operation,
                    kind,
                    result,
                },
            })),
        ));
    }

    fn assert_dispatch_rejected(&mut self, kind: OperationKind) {
        let step = self.drive();
        assert_eq!(
            step.round_result,
            Some(Err(AttemptFailure::TaskProtocol(Box::new(
                ProtocolError::DispatchRejected {
                    kind,
                    result: OperationDispatchResult::IngressRejected(
                        IngressRejection::WaitingCapacity
                    ),
                }
            )))),
            "{step:?}"
        );
    }
}

fn local(result: LocalDispatchResult) -> OperationReplyResult {
    OperationReplyResult::Local {
        result,
        failure: None,
        protocol_failure: None,
        detail: None,
    }
}

fn applied(outcome: OperationOutcome, acknowledgement: AckPayload) -> OperationReplyResult {
    OperationReplyResult::Worker(OperationResult::Verdict {
        outcome,
        detail: None,
        acknowledgement: Some(acknowledgement),
    })
}

#[derive(Clone, Copy)]
enum Case {
    EstablishIdempotentReplay,
    EstablishWaitingCapacity,
    AdmissionUnknownThenWaitingCapacity,
}

async fn run_original_authority(case: Case) {
    let mut frozen = start();
    let participation = *frozen.plan.establish_material.keys().next().unwrap();
    let router = LogicalAbortRouter::new();
    let route = router
        .reserve_route(participation.query_execution_id())
        .unwrap();
    let (mut logical, drive) =
        logical_execution_with_abort_port(participation, router.clone()).await;
    let activation = drive.identity();
    frozen.activation.identity = activation;
    frozen.plan.authorization_limits = drive.authorization_limits();
    let compatibility = frozen.plan.compatibility;
    let facts = AttemptEstablishFacts::freeze(
        Default::default(),
        [(participation.backend_process_id(), Default::default())],
        Default::default(),
        &QueryCredentialLeases::empty(),
    )
    .unwrap();
    let mut next_payload = positive(2);
    let (quotes, material) =
        AttemptEstablishMaterial::project(&facts, [participation], &mut next_payload).unwrap();
    frozen.plan.establish_material = quotes;
    let material = Arc::new(material);
    let requests = FrozenSendRequests::with_establish(material.clone());
    let mut bridge = AttemptQaBridge::new(Arc::new(drive), compatibility, material);
    let wire = AttemptWireFacts {
        native_compatibility_id: compatibility,
    };
    let mut trace = Trace::new(frozen);
    trace.step(trace.sources.actor, AttemptInput::Bootstrap);
    trace.step(trace.sources.actor, AttemptInput::StartProtocol);
    let EffectBody::Port(begin) = trace.effect().body else {
        panic!("expected original Admission authorization");
    };
    let AttemptToLogicalBody::AuthorizationRequest {
        request: AttemptAuthorization::BeginAdmission(admission),
        ..
    } = begin.body
    else {
        panic!("expected the original Admission request");
    };
    assert_eq!(admission.participation(), participation);
    let reply = bridge.handle_port(&begin).await.unwrap();
    let LogicalToAttemptBody::AuthorizationReply {
        decision: Ok(AttemptAuthorizationDecision::AdmissionStarted(issue)),
        ..
    } = reply.reply.body
    else {
        panic!("QA must issue the original Admission");
    };
    assert!(reply.failure.is_none());
    let queue = trace.reply(reply).effect.unwrap();
    let (first, first_generation) = trace.dispatch(queue, 71);
    let admission_operation = admission.envelope().operation_id();
    let admission_kind = OperationKind::AcquireParticipationAdmissionTicket;
    let refusal = || {
        local(LocalDispatchResult::IngressRejected(
            IngressRejection::WaitingCapacity,
        ))
    };
    let prepared_wire = |send| {
        let prepared =
            PreparedSendBatch::resolve(AttemptEffectEnvelope::new(send), &requests).unwrap();
        assert_eq!(prepared.operations().len(), 1);
        encode_operation(&prepared.operations()[0], &wire).unwrap()
    };
    let replay_admission = matches!(case, Case::AdmissionUnknownThenWaitingCapacity);

    let ticket = ParticipationAdmissionTicketReceipt::new(
        AdmissionTicketId::try_from_bytes([0x71; 16]).unwrap(),
        participation,
        admission.valid_for(),
    );
    trace.observation(
        first.id,
        first_generation,
        admission_operation,
        admission_kind,
        None,
        if replay_admission {
            local(LocalDispatchResult::TransportUnknown)
        } else {
            applied(
                OperationOutcome::Accepted,
                AckPayload::AdmissionTicket(ticket),
            )
        },
    );
    if replay_admission {
        let queue = trace.effect();
        let (second, second_generation) = trace.dispatch(queue, 72);
        assert!(second_generation > first_generation);
        assert_eq!(prepared_wire(first), prepared_wire(second.clone()));
        trace.observation(
            second.id,
            second_generation,
            admission_operation,
            admission_kind,
            None,
            refusal(),
        );
    }
    let EffectBody::Port(settlement) = trace.effect().body else {
        panic!("expected the original Admission settlement");
    };
    assert!(matches!(
        settlement.body,
        AttemptToLogicalBody::AuthorizationRequest {
            request: AttemptAuthorization::SettleAdmission {
                issue: original, participation: exact, operation_id, outcome, receipt, prior_transport_unknown,
            }, ..
        } if (original, exact, operation_id) == (issue, participation, admission_operation)
            && prior_transport_unknown == replay_admission
            && if replay_admission { outcome.is_none() && receipt.is_none() }
               else { outcome == Some(OperationOutcome::Accepted) && receipt == Some(ticket) }
    ));
    let reply = bridge.handle_port(&settlement).await.unwrap();
    if replay_admission {
        assert!(matches!(
            &reply.failure,
            Some((
                _,
                crate::task_execution::error::TaskExecutionError::Actor(
                    LogicalExecutionActorError::Establish(EstablishIssueError::AdmissionRejected),
                )
            ))
        ));
        assert!(matches!(
            reply.reply.body,
            LogicalToAttemptBody::AuthorizationReply {
                decision: Err(_),
                ..
            }
        ));
        trace.reply(reply);
        trace.assert_dispatch_rejected(admission_kind);
        assert!(
            logical
                .establish_snapshot(participation)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            logical.actor_snapshot().await.unwrap().establish_error,
            Some(EstablishIssueError::AdmissionRejected)
        );
    } else {
        assert!(reply.failure.is_none());
        assert!(matches!(
            reply.reply.body,
            LogicalToAttemptBody::AuthorizationReply {
                decision: Ok(AttemptAuthorizationDecision::AdmissionGranted),
                ..
            }
        ));
        trace.reply(reply);
        let EffectBody::EstablishAuthorization { operation, request } = trace.effect().body else {
            panic!("expected the original Establish authorization");
        };
        let operation_id = operation.operation_id();
        let kind = OperationKind::UpdateParticipation;
        let reply = bridge.authorize(&operation, &request).await.unwrap();
        let LogicalToAttemptBody::AuthorizationReply {
            decision: Ok(AttemptAuthorizationDecision::EstablishAuthorized(first_guard)),
            ..
        } = reply.reply.body
        else {
            panic!("QA must authorize the original Establish");
        };
        assert!(reply.failure.is_none());
        let queue = trace.reply(reply).effect.unwrap();
        let (first, generation) = trace.dispatch(queue, 73);
        let replay_establish = matches!(case, Case::EstablishIdempotentReplay);
        trace.observation(
            first.id,
            generation,
            operation_id,
            kind,
            Some(participation),
            if replay_establish {
                local(LocalDispatchResult::TransportUnknown)
            } else {
                refusal()
            },
        );
        let settle = async |trace: &mut Trace, bridge: &mut AttemptQaBridge, guard, expected| {
            let effect = trace.effect();
            let EffectBody::Port(port) = effect.body else {
                panic!("expected the original Establish settlement: {effect:?}");
            };
            assert!(matches!(
                port.body,
                AttemptToLogicalBody::EstablishSettlement {
                    authorization, participation: exact, operation_id: id, settlement, ..
                } if (authorization, exact, id, settlement) == (guard, participation, operation_id, expected)
            ));
            let reply = bridge.handle_port(&port).await.unwrap();
            assert!(reply.failure.is_none());
            assert!(matches!(
                reply.reply.body,
                LogicalToAttemptBody::EstablishSettlementReply { authorization, result: Ok(()), .. }
                    if authorization == guard
            ));
            trace.reply(reply)
        };
        if replay_establish {
            settle(
                &mut trace,
                &mut bridge,
                first_guard,
                AttemptIssueSettlement::TransportUnknown,
            )
            .await;
            trace.now = trace.now.saturating_add(Duration::from_secs(1));
            let EffectBody::Port(port) = trace.effect().body else {
                panic!("expected original Establish reauthorization");
            };
            assert!(matches!(
                port.body,
                AttemptToLogicalBody::AuthorizationRequest {
                    request: AttemptAuthorization::ReauthorizeEstablish { participation: exact, operation_id: id }, ..
                } if (exact, id) == (participation, operation_id)
            ));
            let reply = bridge.handle_port(&port).await.unwrap();
            let LogicalToAttemptBody::AuthorizationReply {
                decision: Ok(AttemptAuthorizationDecision::EstablishAuthorized(second_guard)),
                ..
            } = reply.reply.body
            else {
                panic!("QA must reauthorize the same Establish");
            };
            assert!(reply.failure.is_none());
            assert_ne!(first_guard, second_guard);
            let queue = trace.reply(reply).effect.unwrap();
            assert!(matches!(
                trace.effect().body,
                EffectBody::AdmitQueuedInput {
                    operation: ProtocolIntent::CreateTask { .. },
                    ..
                }
            ));
            let (second, second_generation) = trace.dispatch(queue, 74);
            assert!(second_generation > generation);
            assert_eq!(prepared_wire(first), prepared_wire(second.clone()));
            let ProtocolIntent::EstablishParticipation {
                initial_lease_sequence,
                initial_lease_valid_for,
                ..
            } = operation
            else {
                panic!("expected frozen Establish lease");
            };
            let receipt = ParticipationReceipt::new(participation, ParticipationState::Active)
                .with_lease(LeaseReceipt::new(
                    initial_lease_sequence,
                    initial_lease_valid_for,
                    initial_lease_valid_for.get(),
                ));
            trace.observation(
                second.id,
                second_generation,
                operation_id,
                kind,
                Some(participation),
                applied(
                    OperationOutcome::Idempotent,
                    AckPayload::Participation(receipt),
                ),
            );
            // The applied Establish opens the original observation source
            // before the round settles QA. Leave that request pending here.
            assert!(matches!(
                trace.effect().body,
                EffectBody::OpenSubscription(request) if request.participation == participation
            ));
            // The one definitive operation receipt settles both retained QA
            // guards, in original observer order. It is not a second receipt.
            let settlement = AttemptIssueSettlement::Worker(OperationOutcome::Idempotent);
            let late = settle(&mut trace, &mut bridge, first_guard, settlement).await;
            assert_eq!(late.retired_authorization, None);
            let current = settle(&mut trace, &mut bridge, second_guard, settlement).await;
            assert_eq!(current.retired_authorization, Some(operation_id));
        } else {
            let refused = settle(
                &mut trace,
                &mut bridge,
                first_guard,
                AttemptIssueSettlement::PreWorkerRejected,
            )
            .await;
            assert_eq!(refused.retired_authorization, Some(operation_id));
            trace.assert_dispatch_rejected(kind);
        }
        let expected = if replay_establish {
            EstablishIssueState::WorkerSettled(EstablishWorkerSettlement::Applied)
        } else {
            EstablishIssueState::PreWorkerRejected
        };
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Some(snapshot) = logical.establish_snapshot(participation).await.unwrap()
                    && snapshot.state() == expected
                {
                    assert_eq!(snapshot.identity().unwrap().operation_id(), operation_id);
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    drop(bridge);
    let generation = Generation::new(1).unwrap();
    let ledger = RuntimeLedger::new(ResourceVector::default());
    let scope = ledger.open_scope_next(None, generation).unwrap();
    let inbox = WithdrawalInbox::new(&scope);
    let (mut work, mut payer) = scope
        .reserve_next(
            generation,
            ReservationResources::work_only(),
            WorkKind::Settlement,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Settlement)
        .unwrap();
    work.start().unwrap();
    let actor = inbox.registration().actor();
    let runtime = inbox.runtime();
    let standing = runtime
        .reserve_admitted_input_for(inbox.registration(), actor, InputClass::Control, &work)
        .unwrap()
        .into_standing_input()
        .unwrap();
    let standing_id = standing.id();
    let ingress = Arc::new(AttemptAbortIngress::new(
        inbox.registration().clone(),
        activation,
        [(participation, standing)],
        router.capacity_author(),
        None,
    ));
    route.publish(ingress.clone());
    let owner = AbortSettlementOwner::new(ingress.clone());
    logical.abandon_running_attempt();
    let (input, AttemptInput::Port(abort)) = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(abort) = inbox.try_commit_owner_control() {
                break abort;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("QA retained the original participation's Abort debt") else {
        panic!("expected the original QA Control port");
    };
    let LogicalToAttemptBody::Abort { request, .. } = abort.body else {
        panic!("expected the original QA Abort");
    };
    assert_eq!(abort.activation, activation);
    assert_eq!(request.participation(), participation);
    assert_eq!(request.cause(), AbortCause::QueryFailed);
    let snapshot = logical
        .stand_down_snapshot(participation)
        .await
        .unwrap()
        .unwrap();
    let original = snapshot.identity().unwrap();
    assert_eq!(original.activation(), activation);
    assert_eq!(original.participation(), participation);
    assert_eq!(original.operation_id(), request.envelope().operation_id());
    let custody = runtime.input_custody(actor, input).unwrap().unwrap();
    assert_eq!(custody.standing_input_id, Some(standing_id));
    assert_eq!(custody.class, InputClass::Control);
    assert_eq!(custody.phase, InputPhase::Committed);
    assert!(!matches!(
        logical.join_readiness().await.unwrap(),
        LogicalExecutionJoinReadiness::Ready
    ));

    // Stop the test-owned route after observing the original Abort issue.
    let mut drained = owner.drain_failed();
    assert!(drained.errors.is_empty());
    assert!(
        drained
            .retirements
            .pop_front()
            .unwrap()
            .retire_ready()
            .unwrap()
    );
    assert!(drained.retirements.is_empty());
    assert!(runtime.input_custody(actor, input).unwrap().is_none());
    logical
        .observe_worker_process_replaced(participation)
        .await
        .unwrap();
    logical
        .finish_until(std::time::Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    drop(route);
    work.body_exited().unwrap();
    work.captures_exited().unwrap();
    assert!(ingress.close_after_exit().unwrap());
    payer.finish().unwrap();
    scope.close_external().unwrap();
    ledger.reap_scope_ready(scope.id(), 8).unwrap();
    inbox.finish();
    ledger.remove_scope(scope.id()).unwrap();
    assert!(ledger.is_empty());
}

#[tokio::test]
async fn original_establish_replays_to_an_idempotent_receipt_through_qa() {
    run_original_authority(Case::EstablishIdempotentReplay).await;
}

#[tokio::test]
async fn pre_worker_establish_refusal_preserves_ticket_abort_debt() {
    run_original_authority(Case::EstablishWaitingCapacity).await;
}

#[tokio::test]
async fn pre_worker_admission_replay_refusal_preserves_unknown_abort_debt() {
    run_original_authority(Case::AdmissionUnknownThenWaitingCapacity).await;
}
