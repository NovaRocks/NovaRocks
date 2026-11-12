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
    control_runtime::{actor::FrontendInput, transport_withdrawal::AttemptEffectEnvelope},
    native::task_transport::wire::{AttemptWireFacts, encode_operation},
    query_execution::{
        lifecycle_plan::QueryCredentialLeases, native_execution_adapter::LogicalAbortRouter,
    },
    task_execution::{
        bridge::{
            abort::{AbortSettlementOwner, AttemptAbortIngress},
            qa::{AttemptQaBridge, AttemptQaReply},
            terminal::{TerminalOutbox, TerminalPublication},
        },
        error::TaskExecutionError as FrontendError,
        host::{
            adapter::tests::start,
            custody::tests::WithdrawalInbox,
            publication::OriginalHostFailure,
            transport::{AttemptEstablishMaterial, FrozenSendRequests, PreparedSendBatch, SendKey},
        },
        intent::{AckPayload, OperationAcknowledgement},
        operation::{
            IngressRejection as NativeIngressRejection,
            OperationDispatchResult as NativeDispatchResult,
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
        AttemptInput, AttemptInputClaim, AttemptInputId, AttemptSourceCut, AttemptSourceId,
        ExecutorFact, MembershipObservation, PublicationApplied, QueuedInputGrantRef,
        SendGeneration,
    },
    operation::{IngressRejection, OperationDispatchResult},
    start::{AttemptSources, AttemptStart},
};
use novarocks_control_runtime::{
    actor_turn::InputPhase,
    transaction::{RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    identity::{Generation, InputId},
    mailbox::InputClass,
    scope::{WorkAdmission, WorkKind},
};
use novarocks_coordination_contract::{
    AttemptActionId, AttemptAuthorization, AttemptAuthorizationDecision,
    AttemptAuthorizationFailure, AttemptAuthorizationRef, AttemptIssueSettlement,
    AttemptRequestToken, AttemptTerminal, AttemptToLogicalBody, LogicalToAttemptBody,
    MonotonicInstant,
};
use novarocks_execution_contract::{
    AbortCause, AdmissionTicketId, LeaseReceipt, OperationKind, OperationOutcome,
    ParticipationAdmissionTicketReceipt, ParticipationReceipt, ParticipationRef,
    ParticipationState, SafeDetail, TaskOperationId,
};
use novarocks_query_application::{
    api::{NativeAttemptTerminal, ResultRowCarrier, ResultSchema},
    coordination::{
        EstablishIssueError, EstablishIssueState, EstablishWorkerSettlement,
        LogicalExecutionActorConfig, LogicalExecutionActorError, LogicalExecutionJoinReadiness,
        NativeAttemptDrive, ReplacementQualificationEffectAdmission,
        ReplacementQualificationEffectPort, ReplacementQualificationRequest,
    },
    execution_error::ExecutionFailureSite,
    test_support::LogicalExecutionTestHarness,
};
use novarocks_result_contract::{ClientRowProfile, RootOutputKind, RootProfileV1};
use novarocks_types::BackendProcessId;
use novarocks_workload_control::{
    ResourceConfig, Stage, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
};
use std::{
    num::{NonZeroU32, NonZeroU64, NonZeroUsize},
    sync::Arc,
    time::Duration,
};

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
        assert_ne!(source, self.sources.native);
        self.sequence += 1;
        self.step_at(source, input, AttemptInputId(positive(self.sequence)), None)
    }

    fn step_at(
        &mut self,
        source: AttemptSourceId,
        input: AttemptInput,
        input_id: AttemptInputId,
        tail: Option<AttemptInputId>,
    ) -> AttemptStep {
        self.core
            .handle(
                self.now,
                AttemptInputClaim {
                    source,
                    input: input_id,
                    native_cut: Some(AttemptSourceCut {
                        source: self.sources.native,
                        tail,
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

    fn native(&mut self, input: InputId, tail: InputId, fact: ExecutorFact) -> AttemptStep {
        self.step_at(
            self.sources.native,
            AttemptInput::Executor(fact),
            AttemptInputId(positive(input.get())),
            Some(AttemptInputId(positive(tail.get()))),
        )
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
        inbox: &WithdrawalInbox,
        scope: &ScopeHandle,
        send: AttemptActionId,
        generation: SendGeneration,
        establish_started: Option<ParticipationRef>,
        raw: OperationAcknowledgement,
    ) {
        let operation = raw.operation_id();
        let (mut work, mut payer) = scope
            .reserve_next(
                Generation::new(1).unwrap(),
                ReservationResources::work_only(),
                WorkKind::Settlement,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Settlement)
            .unwrap();
        work.start().unwrap();
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
        let starters: Vec<_> = started
            .into_iter()
            .map(|fact| {
                inbox
                    .reserve_command(&work)
                    .fill(FrontendInput::Attempt(Box::new(AttemptInput::Executor(
                        fact,
                    ))))
                    .unwrap()
            })
            .collect();
        let key = SendKey {
            operation,
            generation,
        };
        let mut receipt = inbox.reserve_native_receipt(&work, key, raw.kind());
        receipt.record(key, raw.clone(), None, None).unwrap();
        let report = receipt.submit().unwrap();
        let runtime = inbox.runtime();
        let actor = inbox.registration().actor();
        let custody = runtime.input_custody(actor, report).unwrap().unwrap();
        assert_eq!(custody.source, inbox.source());
        assert_eq!(custody.class, InputClass::Lifecycle);
        assert_eq!(custody.phase, InputPhase::Filled);
        assert!(custody.standing_input_id.is_some());
        for input in starters {
            let turn = self.native(input, report, inbox.commit(input));
            assert!(turn.effect.is_none(), "{turn:?}");
            assert_eq!(turn.selection, InputSelection::Only(self.sources.native));
            runtime.retire_input(actor, input).unwrap();
        }
        assert!(self.pending.is_none());
        let fact = inbox.commit(report);
        assert_eq!(
            runtime.input_custody(actor, report).unwrap().unwrap().phase,
            InputPhase::Committed,
        );
        self.pending = Some(self.native(report, report, fact));
        runtime.retire_input(actor, report).unwrap();
        work.body_exited().unwrap();
        work.captures_exited().unwrap();
        let retained = inbox.retire_native_receipt_for_test(&mut receipt);
        assert_eq!(retained, raw);
        payer.finish().unwrap();
        assert!(runtime.input_custody(actor, report).unwrap().is_none());
    }

    fn process_sample(
        &mut self,
        effect: AttemptEffect,
        participation: ParticipationRef,
        observation: MembershipObservation,
    ) -> (AttemptRequestToken, AttemptStep) {
        let EffectBody::ObserveProcess {
            request,
            participation: original,
        } = effect.body
        else {
            panic!("expected an exact original process observation: {effect:?}");
        };
        assert_eq!(original, participation);
        let step = self.step(
            self.sources.process,
            AttemptInput::Executor(ExecutorFact::ProcessSnapshot {
                request,
                participation,
                sampled_at: self.now,
                observation,
            }),
        );
        (request, step)
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

fn waiting_capacity(operation: TaskOperationId, kind: OperationKind) -> OperationAcknowledgement {
    OperationAcknowledgement::from_dispatch_result(
        operation,
        kind,
        NativeDispatchResult::IngressRejected(NativeIngressRejection::WaitingCapacity),
        AckPayload::None,
    )
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Case {
    EstablishIdempotentReplay,
    EstablishReplayIdentityMismatch,
    EstablishReplayBudgetExhausted,
    EstablishWaitingCapacity,
    AdmissionUnknownThenWaitingCapacity,
}

#[derive(Debug)]
struct NoSuccessor(tokio::sync::watch::Sender<u64>);

impl ReplacementQualificationEffectPort for NoSuccessor {
    fn subscribe_capacity(&self) -> tokio::sync::watch::Receiver<u64> {
        self.0.subscribe()
    }

    fn try_reserve(
        &self,
        _: &ReplacementQualificationRequest,
    ) -> ReplacementQualificationEffectAdmission {
        panic!("these Native observation cuts must not start a successor")
    }
}

async fn recovery_actor(
    participation: ParticipationRef,
    router: Arc<LogicalAbortRouter>,
) -> (LogicalExecutionTestHarness, NativeAttemptDrive) {
    let workload = WorkloadControl::try_new(
        WorkloadConfig::default(),
        ResourceConfig {
            total_bytes: 1 << 20,
            control_bytes: 1 << 12,
            per_scope_bytes: 1 << 18,
        },
    )
    .unwrap();
    workload.mark_ready().unwrap();
    let work = workload
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let stage = work.owner.scope().try_acquire(Stage::Execution).unwrap();
    let profile = ClientRowProfile::try_new(
        RootProfileV1::SEGMENT_BYTES,
        RootProfileV1::ROW_PAYLOAD_BYTES,
    )
    .unwrap();
    let execution = participation.query_execution_id();
    let two = NonZeroUsize::new(2).unwrap();
    let config = LogicalExecutionActorConfig::read_only_pre_visibility_recovery_rows(
        execution,
        two,
        vec![participation],
        two,
        two,
        NonZeroU32::new(2).unwrap(),
        Arc::new(NoSuccessor(tokio::sync::watch::channel(0).0)),
        work.owner,
        stage,
        Duration::from_secs(30),
        ResultSchema::new(Vec::new()),
        NonZeroUsize::MIN,
        ResultRowCarrier::relayed(RootOutputKind::ClientRows, Some(profile)).unwrap(),
    )
    .unwrap()
    .with_abort_participation_effect_port(router, two);
    let mut logical = LogicalExecutionTestHarness::install(
        tokio::runtime::Handle::current(),
        config,
        execution,
        vec![participation],
    )
    .unwrap();
    logical.activate_initial().await.unwrap();
    let drive = logical.native_attempt_drive();
    (logical, drive)
}

async fn run_original_authority(case: Case) {
    let mut frozen = start();
    let participation = *frozen.plan.establish_material.keys().next().unwrap();
    let router = LogicalAbortRouter::new();
    let route = router
        .reserve_route(participation.query_execution_id())
        .unwrap();
    let (mut logical, drive) = if matches!(
        case,
        Case::EstablishReplayIdentityMismatch | Case::EstablishReplayBudgetExhausted
    ) {
        recovery_actor(participation, router.clone()).await
    } else {
        logical_execution_with_abort_port(participation, router.clone()).await
    };
    let generation = Generation::new(1).unwrap();
    let ledger = RuntimeLedger::new(ResourceVector::default());
    let scope = ledger.open_scope_next(None, generation).unwrap();
    let inbox = WithdrawalInbox::new(&scope);
    // Align the paid reporter Source with this Core's Native role.
    std::mem::swap(&mut frozen.sources.native, &mut frozen.sources.publication);
    assert_eq!(frozen.sources.native.0.get(), inbox.source().get());
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
    let ticket_horizon = trace.now.saturating_add(ticket.valid_for().get());
    trace.observation(
        &inbox,
        &scope,
        first.id,
        first_generation,
        None,
        if replay_admission {
            OperationAcknowledgement::transport_unknown(admission_operation, admission_kind)
        } else {
            OperationAcknowledgement::worker_receipt(
                admission_operation,
                admission_kind,
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
            &inbox,
            &scope,
            second.id,
            second_generation,
            None,
            waiting_capacity(admission_operation, admission_kind),
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
        let replay_establish = case != Case::EstablishWaitingCapacity;
        trace.observation(
            &inbox,
            &scope,
            first.id,
            generation,
            Some(participation),
            if replay_establish {
                OperationAcknowledgement::transport_unknown(operation_id, kind)
            } else {
                waiting_capacity(operation_id, kind)
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
            trace.now = trace.now.saturating_add(Duration::from_secs(1));
            let settlement = match case {
                Case::EstablishReplayBudgetExhausted => {
                    trace.observation(
                        &inbox,
                        &scope,
                        second.id,
                        second_generation,
                        Some(participation),
                        OperationAcknowledgement::transport_unknown(operation_id, kind),
                    );
                    let unknown = settle(
                        &mut trace,
                        &mut bridge,
                        second_guard,
                        AttemptIssueSettlement::TransportUnknown,
                    )
                    .await;
                    assert!(unknown.effect.is_none());
                    finish_exhausted_replay(
                        &mut trace,
                        &mut bridge,
                        participation,
                        operation_id,
                        ticket_horizon,
                        [first_guard, second_guard],
                    )
                    .await;
                    None
                }
                Case::EstablishReplayIdentityMismatch => {
                    let raw = OperationAcknowledgement::worker_receipt(
                        operation_id,
                        kind,
                        OperationOutcome::IdentityMismatch,
                        AckPayload::None,
                    )
                    .with_detail(Some(SafeDetail::truncating(
                        "original Establish replay identity mismatch",
                    )));
                    let rejection = raw.worker_header().unwrap();
                    trace.observation(
                        &inbox,
                        &scope,
                        second.id,
                        second_generation,
                        Some(participation),
                        raw,
                    );
                    Some((OperationOutcome::IdentityMismatch, Some(rejection)))
                }
                Case::EstablishIdempotentReplay => {
                    let ProtocolIntent::EstablishParticipation {
                        initial_lease_sequence,
                        initial_lease_valid_for,
                        ..
                    } = operation
                    else {
                        panic!("expected frozen Establish lease");
                    };
                    let receipt =
                        ParticipationReceipt::new(participation, ParticipationState::Active)
                            .with_lease(LeaseReceipt::new(
                                initial_lease_sequence,
                                initial_lease_valid_for,
                                initial_lease_valid_for.get(),
                            ));
                    trace.observation(
                        &inbox,
                        &scope,
                        second.id,
                        second_generation,
                        Some(participation),
                        OperationAcknowledgement::worker_receipt(
                            operation_id,
                            kind,
                            OperationOutcome::Idempotent,
                            AckPayload::Participation(receipt),
                        ),
                    );
                    // Applied Establish opens this Source before the original QA settlements.
                    assert!(matches!(
                        trace.effect().body,
                        EffectBody::OpenSubscription(request) if request.participation == participation
                    ));
                    Some((OperationOutcome::Idempotent, None))
                }
                _ => unreachable!("the original Establish replay cases"),
            };
            if let Some((outcome, rejection)) = settlement {
                // One definitive receipt settles the original late guard and current guard.
                let settlement = AttemptIssueSettlement::Worker(outcome);
                let late = settle(&mut trace, &mut bridge, first_guard, settlement).await;
                assert_eq!(late.retired_authorization, None);
                let current = settle(&mut trace, &mut bridge, second_guard, settlement).await;
                assert_eq!(current.retired_authorization, Some(operation_id));
                if let Some(rejection) = rejection {
                    finish_fenced_replay(
                        &mut trace,
                        &mut bridge,
                        participation,
                        operation_id,
                        rejection,
                    )
                    .await;
                }
            }
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
        let expected = match case {
            Case::EstablishIdempotentReplay => {
                EstablishIssueState::WorkerSettled(EstablishWorkerSettlement::Applied)
            }
            Case::EstablishReplayIdentityMismatch => EstablishIssueState::WorkerSettled(
                EstablishWorkerSettlement::Rejected(OperationOutcome::IdentityMismatch),
            ),
            Case::EstablishReplayBudgetExhausted => EstablishIssueState::TransportUnknown,
            Case::EstablishWaitingCapacity => EstablishIssueState::PreWorkerRejected,
            Case::AdmissionUnknownThenWaitingCapacity => unreachable!("no Establish was attempted"),
        };
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Some(snapshot) = logical.establish_snapshot(participation).await.unwrap()
                    && snapshot.state() == expected
                {
                    let exact = snapshot.identity().unwrap();
                    assert_eq!(exact.operation_id(), operation_id);
                    assert_eq!(exact.participation(), participation);
                    assert_eq!(exact.activation(), activation);
                    assert_eq!(snapshot.ticket(), ticket);
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    drop(bridge);
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

async fn publish_failure(
    trace: &mut Trace,
    activation: novarocks_coordination_contract::AttemptActivationIdentity,
    original: Option<(
        novarocks_attempt_protocol::failure::HostFailureRef,
        FrontendError,
    )>,
) -> novarocks_query_application::api::NativeAttemptPreparationFailure {
    let EffectBody::Port(port) = trace.effect().body else {
        panic!("the original terminal follows the completed recovery observation");
    };
    assert_eq!(port.activation, activation);
    assert!(matches!(
        port.body,
        AttemptToLogicalBody::AttemptTerminal(AttemptTerminal::Failed(_))
    ));
    let outbox = TerminalOutbox::new(activation);
    let mut publication = TerminalPublication::new(port);
    if let Some((reference, failure)) = original {
        assert_eq!(publication.host_reference(), Some(reference));
        publication.attach_host_failure(reference, OriginalHostFailure::Task(failure));
    } else {
        assert!(publication.host_reference().is_none());
    }
    assert!(outbox.publish(Box::new(publication)).is_ok());
    let NativeAttemptTerminal::Failed(failure) = outbox.terminal().await.unwrap() else {
        panic!("replay recovery cannot become success");
    };
    failure
}

async fn finish_fenced_replay(
    trace: &mut Trace,
    bridge: &mut AttemptQaBridge,
    participation: ParticipationRef,
    operation_id: TaskOperationId,
    rejection: novarocks_execution_contract::operation::OperationResult<std::convert::Infallible>,
) {
    let original =
        AttemptFailure::TaskProtocol(Box::new(ProtocolError::PreReadyEstablishRejected {
            participation,
            operation_id,
            rejection: rejection.clone(),
        }));
    let expected = FrontendError::PreReadyEstablishRejected {
        participation,
        operation_id,
        rejection,
    }
    .preparation_input();
    let round = trace.drive();
    assert_eq!(round.round_result, Some(Err(original.clone())));
    let effect = round.effect.unwrap_or_else(|| trace.effect());
    let EffectBody::Port(permission) = effect.body else {
        panic!("the exact replay refusal requests QA replacement permission");
    };
    assert!(matches!(
        &permission.body,
        AttemptToLogicalBody::PreReadyReplacementQuery { refusal, .. } if refusal == &original
    ));
    let reply = bridge.handle_port(&permission).await.unwrap();
    assert!(reply.failure.is_none());
    assert!(matches!(
        reply.reply.body,
        LogicalToAttemptBody::PreReadyReplacementReply {
            may_observe: true,
            ..
        }
    ));
    let sample = trace
        .reply(reply)
        .effect
        .expect("positive QA permission opens the exact process sample");
    let replacement = BackendProcessId::new_v7();
    assert_ne!(replacement, participation.backend_process_id());
    let (_, observed) = trace.process_sample(
        sample,
        participation,
        MembershipObservation::ExactReplacement(replacement),
    );
    assert!(trace.pending.is_none());
    trace.pending = Some(observed);
    let failure = publish_failure(trace, permission.activation, None).await;
    assert_eq!(failure.input(), &expected);
    let witness = failure.pre_ready_replacement().unwrap();
    assert_eq!(witness.participation(), participation);
    assert_eq!(witness.operation_id(), operation_id);
    assert_eq!(witness.replacement_process(), replacement);
}

async fn finish_exhausted_replay(
    trace: &mut Trace,
    bridge: &mut AttemptQaBridge,
    participation: ParticipationRef,
    operation_id: TaskOperationId,
    horizon: MonotonicInstant,
    guards: [AttemptAuthorizationRef; 2],
) {
    let EffectBody::Port(port) = trace.effect().body else {
        panic!("the third carrier must ask the original QA budget before admission");
    };
    assert!(matches!(
        port.body,
        AttemptToLogicalBody::AuthorizationRequest {
            request: AttemptAuthorization::ReauthorizeEstablish { participation: exact, operation_id: operation },
            ..
        } if (exact, operation) == (participation, operation_id)
    ));
    let mut reply = bridge.handle_port(&port).await.unwrap();
    let original = reply.failure.take().expect("original QA budget failure");
    assert!(matches!(
        original.1,
        FrontendError::Actor(LogicalExecutionActorError::Establish(
            EstablishIssueError::AuthorizationBudgetExhausted,
        ))
    ));
    let LogicalToAttemptBody::AuthorizationReply {
        decision:
            Err(AttemptAuthorizationFailure::EstablishBudgetExhausted(AttemptFailure::Host(projected))),
        ..
    } = &reply.reply.body
    else {
        panic!("real QA budget refusal retains its original Host error");
    };
    assert_eq!(projected.original, original.0);
    let exhausted = trace.reply(reply);
    assert!(exhausted.effect.is_none());
    let first = trace.effect();
    let (earlier, waiting) =
        trace.process_sample(first, participation, MembershipObservation::Unobserved);
    assert!(waiting.effect.is_none() && !waiting.stop_producing);
    assert_eq!(
        waiting.selection,
        InputSelection::Prefer(trace.sources.native)
    );
    assert!(trace.now < horizon);
    assert_eq!(trace.core.next_deadline(), Some(horizon));

    trace.now = horizon;
    let final_sample = trace.effect();
    let (actual, observed) = trace.process_sample(
        final_sample,
        participation,
        MembershipObservation::ExactOriginal,
    );
    assert_ne!(actual, earlier);
    assert!(observed.effect.is_none());

    for guard in guards {
        let EffectBody::Port(detach) = trace.effect().body else {
            panic!("the same original late guards must detach before terminal");
        };
        assert!(
            matches!(detach.body, AttemptToLogicalBody::DetachEstablish {
            authorization, participation: exact, operation_id: operation, ..
        } if (authorization, exact, operation) == (guard, participation, operation_id))
        );
        let reply = bridge.handle_port(&detach).await.unwrap();
        assert!(reply.failure.is_none());
        assert!(matches!(reply.reply.body,
            LogicalToAttemptBody::EstablishDetachReply { authorization, result: Ok(()), .. }
            if authorization == guard));
        let applied = trace.reply(reply);
        assert!(applied.effect.is_none());
    }

    let failure = publish_failure(trace, port.activation, Some(original)).await;
    let backend = participation.backend_process_id();
    assert_eq!(
        failure.input(),
        &FrontendError::PreReadyEstablishTransportUnknown { backend }.preparation_input()
    );
    assert_eq!(
        failure.input().site(),
        ExecutionFailureSite::Backend { process: backend }
    );
    assert!(failure.pre_ready_replacement().is_none());
}

#[tokio::test]
async fn original_establish_replay_identity_mismatch_keeps_exact_replacement_fence() {
    run_original_authority(Case::EstablishReplayIdentityMismatch).await;
}

#[tokio::test]
async fn original_establish_unknown_replay_exhausts_real_qa_budget_at_original_ticket_horizon() {
    run_original_authority(Case::EstablishReplayBudgetExhausted).await;
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
