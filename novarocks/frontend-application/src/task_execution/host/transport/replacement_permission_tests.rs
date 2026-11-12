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

mod create;

use super::{AttemptEstablishMaterial, AttemptProcessObserver};
use crate::{
    control_runtime::membership::FrozenMember,
    query_execution::{
        lifecycle_plan::QueryCredentialLeases, native_execution_adapter::LogicalAbortRouter,
    },
    task_execution::{
        bridge::{
            control::AttemptOwnerControls,
            qa::AttemptQaBridge,
            terminal::{TerminalOutbox, TerminalPublication},
        },
        host::{adapter::tests::start, custody::tests::WithdrawalInbox},
        intent::{AckPayload, OperationAcknowledgement},
        sources::AttemptEstablishFacts,
    },
};
use novarocks_attempt_protocol::{
    AttemptCore, AttemptStep, Core, InputSelection,
    capacity::{QueueStage, ReservationToken, StepContext},
    config::DispatchLane,
    effect::{AttemptEffect, EffectBody, OperationIntent, Publication},
    error::TaskExecutionError,
    failure::AttemptFailure,
    input::{
        AttemptInput, AttemptInputClaim, AttemptInputId, AttemptSourceCut, AttemptSourceId,
        ExecutorFact, MembershipObservation, MembershipView, PublicationApplied,
        QueuedInputGrantRef, SendGeneration,
    },
    start::{AttemptSources, AttemptStart},
};
use novarocks_control_runtime::{actor_turn::Clock, transaction::RuntimeLedger};
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    identity::{Generation, Tick},
    mailbox::InputClass,
    scope::{WorkAdmission, WorkKind},
};
use novarocks_coordination_contract::{
    AttemptActivationIdentity, AttemptFailureClass, AttemptRequestToken, AttemptTerminal,
    AttemptToLogical, AttemptToLogicalBody, ExecutionEffect, LogicalToAttemptBody,
    MonotonicInstant,
};
use novarocks_execution_contract::{
    AdmissionTicketId, OperationKind, OperationOutcome, ParticipationAdmissionTicketReceipt,
    ParticipationRef, RuntimeEndpoint, SafeDetail,
};
use novarocks_query_application::{
    api::{
        BackendProcessObservation, BackendProcessObservationPort, BackendTopologyError,
        NativeAttemptTerminal, ResultRowCarrier, ResultSchema,
    },
    coordination::{
        EstablishIssueState, EstablishWorkerSettlement, LogicalExecutionActorConfig,
        ReplacementQualificationEffectAdmission, ReplacementQualificationEffectPort,
        ReplacementQualificationRequest,
    },
    execution_error::ExecutionFailureSite,
    test_support::LogicalExecutionTestHarness,
};
use novarocks_result_contract::{ClientRowProfile, RootOutputKind, RootProfileV1};
use novarocks_types::BackendProcessId;
use novarocks_workload_control::{
    CancellationReason, CancellationView, ResourceConfig, Stage, WorkCancellationRequester,
    WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
};
use std::{
    num::{NonZeroU32, NonZeroU64, NonZeroUsize},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

fn id(value: u64) -> NonZeroU64 {
    NonZeroU64::new(value).unwrap()
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
        panic!("the original Native observation must not start a successor")
    }
}

#[derive(Default)]
struct SampleClock(AtomicU64);

impl SampleClock {
    fn set_seconds(&self, seconds: u64) {
        self.0.store(seconds * 1_000_000_000, Ordering::SeqCst);
    }

    fn instant(&self) -> MonotonicInstant {
        MonotonicInstant::from_origin(Duration::from_nanos(self.now().0))
    }
}

impl Clock for SampleClock {
    fn now(&self) -> Tick {
        Tick(self.0.load(Ordering::SeqCst))
    }
}

struct ProcessProbe {
    original: ParticipationRef,
    endpoint: RuntimeEndpoint,
    replacement: BackendProcessId,
    replaced: AtomicBool,
    reads: AtomicU64,
}

impl BackendProcessObservationPort for ProcessProbe {
    fn observe_process_at_endpoint(
        &self,
        expected: BackendProcessId,
        endpoint: &RuntimeEndpoint,
    ) -> Result<BackendProcessObservation, BackendTopologyError> {
        assert_eq!(expected, self.original.backend_process_id());
        assert_eq!(endpoint, &self.endpoint);
        self.reads.fetch_add(1, Ordering::SeqCst);
        if self.replaced.load(Ordering::SeqCst) {
            Ok(BackendProcessObservation::Replaced {
                current_process: self.replacement,
            })
        } else {
            Ok(BackendProcessObservation::Unobservable)
        }
    }
}

struct Turn;

impl StepContext for Turn {
    fn charge(&mut self, _: usize) -> bool {
        true
    }

    fn reserve_queue_stage(
        &mut self,
        lane: DispatchLane,
        control: bool,
        _: BackendProcessId,
        records: usize,
        _: usize,
    ) -> QueueStage {
        assert!(matches!(
            lane,
            DispatchLane::Lifecycle | DispatchLane::Create
        ));
        assert!(!control);
        assert_eq!(records, 1);
        QueueStage::Granted(ReservationToken(id(55)))
    }
}

type Port = AttemptToLogical<AttemptFailure>;

struct Cut {
    core: Core,
    sources: AttemptSources,
    clock: Arc<SampleClock>,
    qa: AttemptQaBridge,
    next: u64,
    round_failure: Option<AttemptFailure>,
}

impl Cut {
    fn new(start: AttemptStart, clock: Arc<SampleClock>, qa: AttemptQaBridge) -> Self {
        let sources = start.sources;
        let mut cut = Self {
            core: <Core as AttemptCore>::new(start).unwrap(),
            sources,
            clock,
            qa,
            next: 100,
            round_failure: None,
        };
        let boot = cut.actor(AttemptInput::Bootstrap);
        assert!(boot.effect.is_none());
        assert_eq!(boot.selection, InputSelection::Prefer(sources.native));
        cut
    }

    fn step(&mut self, source: AttemptSourceId, input: AttemptInput) -> AttemptStep {
        self.step_at(source, input, None)
    }

    fn step_at(
        &mut self,
        source: AttemptSourceId,
        input: AttemptInput,
        original: Option<AttemptInputId>,
    ) -> AttemptStep {
        self.next += 1;
        let input_id = original.unwrap_or(AttemptInputId(id(self.next)));
        self.core
            .handle(
                self.clock.instant(),
                AttemptInputClaim {
                    source,
                    input: input_id,
                    native_cut: Some(AttemptSourceCut {
                        source: self.sources.native,
                        tail: (source == self.sources.native).then_some(input_id),
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

    fn actor(&mut self, input: AttemptInput) -> AttemptStep {
        self.step(self.sources.actor, input)
    }

    fn native(&mut self, fact: ExecutorFact) -> AttemptStep {
        self.step(self.sources.native, AttemptInput::Executor(fact))
    }

    fn native_before_queued_reply(&mut self, fact: ExecutorFact) -> AttemptStep {
        self.next += 1;
        let input = AttemptInputId(id(self.next));
        let tail = AttemptInputId(id(self.next + 1));
        let source = self.sources.native;
        self.core
            .handle(
                self.clock.instant(),
                AttemptInputClaim {
                    source,
                    input,
                    native_cut: Some(AttemptSourceCut {
                        source,
                        tail: Some(tail),
                    }),
                    selected_cut: Some(AttemptSourceCut {
                        source: self.sources.observation,
                        tail: None,
                    }),
                },
                AttemptInput::Executor(fact),
                &mut Turn,
            )
            .unwrap()
    }

    async fn advance(&mut self, mut step: AttemptStep) -> Option<AttemptEffect> {
        for _ in 0..96 {
            assert!(step.abort_settlement.is_none());
            if let Some(Err(error)) = step.round_result {
                assert!(self.round_failure.replace(error).is_none());
            }
            if let Some(effect) = step.effect {
                match &effect.body {
                    EffectBody::Publish(Publication::ProtocolBatch(_)) => {
                        step = self.step(
                            self.sources.publication,
                            AttemptInput::Executor(ExecutorFact::PublicationApplied {
                                effect: effect.id,
                                result: Ok(PublicationApplied::ProtocolBatch { published: true }),
                            }),
                        );
                        continue;
                    }
                    EffectBody::EstablishAuthorization { operation, request } => {
                        let reply = self.qa.authorize(operation, request).await.unwrap();
                        assert!(reply.failure.is_none());
                        step = self.step(self.sources.qa, AttemptInput::Port(reply.reply));
                        continue;
                    }
                    EffectBody::Port(port)
                        if matches!(
                            port.body,
                            AttemptToLogicalBody::AuthorizationRequest { .. }
                                | AttemptToLogicalBody::EstablishSettlement { .. }
                                | AttemptToLogicalBody::DetachEstablish { .. }
                        ) =>
                    {
                        let reply = self.qa.handle_port(port).await.unwrap();
                        assert!(reply.failure.is_none());
                        step = self.step(self.sources.qa, AttemptInput::Port(reply.reply));
                        continue;
                    }
                    EffectBody::ReleaseCreationPayload { .. } => {}
                    _ => return Some(effect),
                }
            }
            if self
                .core
                .next_deadline()
                .is_none_or(|deadline| deadline > self.clock.instant())
            {
                return None;
            }
            step = self.actor(AttemptInput::Timer);
        }
        panic!("original Core did not reach its next external effect or deadline");
    }

    async fn send(
        &mut self,
        admitted: AttemptEffect,
    ) -> (
        OperationIntent,
        SendGeneration,
        novarocks_coordination_contract::AttemptActionId,
    ) {
        let EffectBody::AdmitQueuedInput {
            operation,
            generation,
            lane,
            ..
        } = admitted.body
        else {
            panic!("one exact queue admission: {admitted:?}");
        };
        assert_eq!(lane, operation.lane());
        assert_eq!(admitted.id, generation.admission_effect());
        let grant =
            QueuedInputGrantRef::new(id(2000 + generation.admission_effect().get()), generation);
        let timer = self.actor(AttemptInput::Timer);
        assert!(self.advance(timer).await.is_none());
        let granted = self.step(
            self.sources.transport,
            AttemptInput::Executor(ExecutorFact::QueuedInputGranted {
                operation: operation.operation_id(),
                grant,
                queued_bytes: operation.queued_bytes(),
            }),
        );
        let send = self.advance(granted).await.unwrap();
        let EffectBody::SendBatch { batch, .. } = send.body else {
            panic!("one original SendBatch");
        };
        assert_eq!(batch.queued_inputs.as_ref(), [grant]);
        assert_eq!(batch.operations.len(), 1);
        assert_eq!(batch.operations[0].operation_id(), operation.operation_id());
        let timer = self.actor(AttemptInput::Timer);
        assert!(self.advance(timer).await.is_none());
        (operation, generation, send.id)
    }

    async fn permission(&mut self, port: Port, expected: bool) -> AttemptStep {
        let AttemptToLogicalBody::PreReadyReplacementQuery { token, .. } = port.body else {
            panic!("original QA permission port");
        };
        let reply = self.qa.handle_port(&port).await.unwrap();
        assert!(reply.failure.is_none());
        assert_eq!(reply.reply.activation, port.activation);
        assert!(matches!(
            reply.reply.body,
            LogicalToAttemptBody::PreReadyReplacementReply { token: actual, may_observe }
                if actual == token && may_observe == expected
        ));
        self.step(self.sources.qa, AttemptInput::Port(reply.reply))
    }
}

fn terminal_port(effect: AttemptEffect) -> Port {
    let EffectBody::Port(port) = effect.body else {
        panic!("the next external effect must be the original terminal, got {effect:?}");
    };
    assert!(matches!(
        port.body,
        AttemptToLogicalBody::AttemptTerminal(AttemptTerminal::Failed(_))
    ));
    port
}

fn sample(
    cut: &mut Cut,
    step: AttemptStep,
    observer: &AttemptProcessObserver,
    participation: ParticipationRef,
) -> (AttemptRequestToken, AttemptStep) {
    let Some(AttemptEffect {
        body:
            EffectBody::ObserveProcess {
                request,
                participation: exact,
            },
        ..
    }) = step.effect
    else {
        panic!("positive QA permission must acquire one exact process sample");
    };
    assert_eq!(participation, exact);
    assert_eq!(step.selection, InputSelection::Only(cut.sources.process));
    let fact = observer.read(request, exact).unwrap();
    assert!(matches!(
        fact,
        ExecutorFact::ProcessSnapshot { request: original, participation: part, sampled_at, .. }
            if original == request && part == exact && sampled_at == cut.clock.instant()
    ));
    (
        request,
        cut.step(cut.sources.process, AttemptInput::Executor(fact)),
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Case {
    Replacement,
    NoRecovery,
    StopBeforePermission,
    StopAfterSample,
}

#[tokio::test]
async fn original_establish_refusal_observes_only_with_qa_permission_and_keeps_its_identity() {
    for case in [
        Case::Replacement,
        Case::NoRecovery,
        Case::StopBeforePermission,
        Case::StopAfterSample,
    ] {
        tokio::time::timeout(Duration::from_secs(3), rejected_establish(case))
            .await
            .unwrap();
    }
}

async fn rejected_establish(case: Case) {
    let mut start = start();
    let participation = *start.plan.establish_material.keys().next().unwrap();
    let execution = participation.query_execution_id();
    let compatibility = start.plan.compatibility;
    let facts = AttemptEstablishFacts::freeze(
        Default::default(),
        [(participation.backend_process_id(), Default::default())],
        Default::default(),
        &QueryCredentialLeases::empty(),
    )
    .unwrap();
    let mut next_payload = id(500);
    let (quotes, material) =
        AttemptEstablishMaterial::project(&facts, [participation], &mut next_payload).unwrap();
    start.plan.establish_material = quotes;

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
    let scope = work.owner.scope();
    let cancellation = scope.cancellation().unwrap();
    let requester = work.owner.cancellation_requester();
    let stage = scope.try_acquire(Stage::Execution).unwrap();
    let roster = vec![participation];
    let two = NonZeroUsize::new(2).unwrap();
    let config = if case == Case::NoRecovery {
        LogicalExecutionActorConfig::single_attempt_completion(
            execution,
            ExecutionEffect::None,
            two,
            roster.clone(),
            two,
            two,
            work.owner,
            stage,
        )
    } else {
        let profile = ClientRowProfile::try_new(
            RootProfileV1::SEGMENT_BYTES,
            RootProfileV1::ROW_PAYLOAD_BYTES,
        )
        .unwrap();
        LogicalExecutionActorConfig::read_only_pre_visibility_recovery_rows(
            execution,
            two,
            roster.clone(),
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
    }
    .unwrap()
    .with_abort_participation_effect_port(LogicalAbortRouter::new(), two);
    let mut logical = LogicalExecutionTestHarness::install(
        tokio::runtime::Handle::current(),
        config,
        execution,
        roster,
    )
    .unwrap();
    let activation = logical.activate_initial().await.unwrap();
    let drive = Arc::new(logical.native_attempt_drive());
    start.activation.identity = activation;
    start.plan.authorization_limits = drive.authorization_limits();
    let bridge = AttemptQaBridge::new(drive, compatibility, Arc::new(material));
    let clock = Arc::new(SampleClock::default());
    let mut cut = Cut::new(start, clock.clone(), bridge);

    let endpoint = RuntimeEndpoint::new("127.0.0.1", 19091).unwrap();
    let probe = Arc::new(ProcessProbe {
        original: participation,
        endpoint: endpoint.clone(),
        replacement: BackendProcessId::new_v7(),
        replaced: AtomicBool::new(false),
        reads: AtomicU64::new(0),
    });
    let observer = AttemptProcessObserver::new(
        probe.clone(),
        Arc::from([FrozenMember {
            participation,
            endpoint,
        }]),
        clock.clone(),
    );
    let outbox = TerminalOutbox::new(activation);

    let begun = cut.actor(AttemptInput::StartProtocol);
    let admitted = cut.advance(begun).await.unwrap();
    let (operation, generation, sent) = cut.send(admitted).await;
    let OperationIntent::AcquireParticipationAdmissionTicket(admission) = operation else {
        panic!("the original admission must precede Establish");
    };
    let started = cut.native(ExecutorFact::SendStarted {
        effect: sent,
        started_at: clock.instant(),
    });
    assert!(cut.advance(started).await.is_none());
    let ticket = ParticipationAdmissionTicketReceipt::new(
        AdmissionTicketId::try_from_bytes([9; 16]).unwrap(),
        participation,
        admission.valid_for(),
    );
    let admitted = OperationAcknowledgement::worker_receipt(
        admission.envelope().operation_id(),
        OperationKind::AcquireParticipationAdmissionTicket,
        OperationOutcome::Accepted,
        AckPayload::AdmissionTicket(ticket),
    )
    .attempt_observation(admission.envelope().operation_id(), generation, None, None)
    .unwrap();
    let got_ticket = cut.native(ExecutorFact::Operation(admitted));
    let establish = cut.advance(got_ticket).await.unwrap();
    let (operation, generation, sent) = cut.send(establish).await;
    let operation_id = operation.operation_id();
    assert!(
        matches!(operation, OperationIntent::EstablishParticipation { participation: exact, ticket: original, .. }
        if exact == participation && original == ticket.ticket_id())
    );
    // Both original facts can already be queued when AP claims the Native
    // prefix. Do not advance candidates between RPC start and its refusal.
    let started = cut.native_before_queued_reply(ExecutorFact::EstablishRpcStarted {
        effect: sent,
        operation: operation_id,
        participation,
        started_at: clock.instant(),
    });
    assert!(started.effect.is_none() && started.round_result.is_none());
    assert_eq!(started.selection, InputSelection::Only(cut.sources.native));

    clock.set_seconds(1);
    let raw = OperationAcknowledgement::worker_receipt(
        operation_id,
        OperationKind::UpdateParticipation,
        OperationOutcome::IdentityMismatch,
        AckPayload::None,
    )
    .with_detail(Some(SafeDetail::truncating(
        "original Establish process mismatch",
    )));
    let rejection = raw.worker_header().unwrap();
    let original =
        AttemptFailure::TaskProtocol(Box::new(TaskExecutionError::PreReadyEstablishRejected {
            participation,
            operation_id,
            rejection: rejection.clone(),
        }));
    let expected = crate::task_execution::error::TaskExecutionError::PreReadyEstablishRejected {
        participation,
        operation_id,
        rejection,
    }
    .preparation_input();
    let refused = cut.native(ExecutorFact::Operation(
        raw.attempt_observation(operation_id, generation, None, None)
            .unwrap(),
    ));
    let permission = cut.advance(refused).await.unwrap();
    assert_eq!(cut.round_failure, Some(original.clone()));
    let EffectBody::Port(permission) = permission.body else {
        panic!("the actual AP refusal requests original QA permission");
    };
    assert_eq!(permission.activation, activation);
    assert!(
        matches!(&permission.body, AttemptToLogicalBody::PreReadyReplacementQuery { refusal, .. } if refusal == &original)
    );

    loop {
        if let Some(snapshot) = logical.establish_snapshot(participation).await.unwrap()
            && snapshot.state()
                == EstablishIssueState::WorkerSettled(EstablishWorkerSettlement::Rejected(
                    OperationOutcome::IdentityMismatch,
                ))
        {
            let exact = snapshot.identity().unwrap();
            assert_eq!(exact.activation(), activation);
            assert_eq!(exact.operation_id(), operation_id);
            assert_eq!(exact.participation(), participation);
            assert_eq!(snapshot.ticket(), ticket);
            break;
        }
        tokio::task::yield_now().await;
    }

    assert_eq!(logical.actor_snapshot().await.unwrap().conclusion, None);
    if case == Case::NoRecovery {
        assert!(
            !logical
                .native_attempt_drive()
                .may_observe_pre_ready_replacement(&expected)
                .await
                .unwrap()
        );
    }
    if case == Case::StopBeforePermission {
        stop(&requester, &cancellation, activation, &mut cut);
    }
    let positive = matches!(case, Case::Replacement | Case::StopAfterSample);
    let permitted = cut.permission(permission, positive).await;
    let effect = if positive {
        let (first, current) = sample(&mut cut, permitted, &observer, participation);
        assert!(current.effect.is_none());
        assert_eq!(probe.reads.load(Ordering::SeqCst), 1);
        let original_horizon = MonotonicInstant::ORIGIN.saturating_add(ticket.valid_for().get());
        assert_eq!(cut.core.next_deadline(), Some(original_horizon));
        {
            let mut terminal = Box::pin(outbox.terminal());
            assert!(futures::poll!(terminal.as_mut()).is_pending());
        }
        clock.set_seconds(2);
        if case == Case::StopAfterSample {
            let stopped = stop(&requester, &cancellation, activation, &mut cut);
            cut.advance(stopped).await.unwrap()
        } else {
            probe.replaced.store(true, Ordering::SeqCst);
            // A topology revision wakes the receiver; only its new exact
            // endpoint sample can attach a replacement witness.
            let changed = cut.step(
                cut.sources.membership,
                AttemptInput::Membership(MembershipView {
                    revision: 1,
                    processes: [(
                        participation,
                        MembershipObservation::ExactReplacement(probe.replacement),
                    )]
                    .into(),
                }),
            );
            let EffectBody::Port(query) = cut.advance(changed).await.unwrap().body else {
                panic!("the same pending refusal must recheck original QA permission");
            };
            assert!(
                matches!(&query.body, AttemptToLogicalBody::PreReadyReplacementQuery { refusal, .. } if refusal == &original)
            );
            let permitted = cut.permission(query, true).await;
            let (second, sampled) = sample(&mut cut, permitted, &observer, participation);
            assert_ne!(first, second);
            cut.advance(sampled).await.unwrap()
        }
    } else {
        assert!(!matches!(
            permitted.effect,
            Some(AttemptEffect {
                body: EffectBody::ObserveProcess { .. },
                ..
            })
        ));
        cut.advance(permitted).await.unwrap()
    };
    let terminal = terminal_port(effect);
    assert_eq!(terminal.activation, activation);
    let original_port = TerminalPublication::new(terminal);
    assert!(original_port.host_reference().is_none());
    assert!(outbox.publish(Box::new(original_port)).is_ok());
    let NativeAttemptTerminal::Failed(failure) = outbox.terminal().await.unwrap() else {
        panic!("the exact original refusal must remain the Native terminal");
    };
    assert_eq!(failure.input(), &expected);
    assert_eq!(failure.class(), AttemptFailureClass::ContractViolation);
    assert_eq!(
        failure.input().site(),
        ExecutionFailureSite::Operation {
            participation,
            operation_id,
            kind: OperationKind::UpdateParticipation,
        }
    );
    if case == Case::Replacement {
        let witness = failure.pre_ready_replacement().unwrap();
        assert_eq!(witness.participation(), participation);
        assert_eq!(witness.operation_id(), operation_id);
        assert_eq!(witness.replacement_process(), probe.replacement);
        assert_eq!(probe.reads.load(Ordering::SeqCst), 2);
    } else {
        assert!(failure.pre_ready_replacement().is_none());
        assert_eq!(
            probe.reads.load(Ordering::SeqCst),
            u64::from(case == Case::StopAfterSample)
        );
    }
    assert_eq!(
        outbox.terminal().await.unwrap(),
        NativeAttemptTerminal::Failed(failure)
    );

    drop(cut);
    logical.abandon_running_attempt();
    while logical
        .stand_down_snapshot(participation)
        .await
        .unwrap()
        .is_none()
    {
        tokio::task::yield_now().await;
    }
    logical
        .observe_worker_process_replaced(participation)
        .await
        .unwrap();
    drop(requester);
    drop(cancellation);
    work.business.release();
    logical
        .finish_until(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    scope.wait_released().await;
    workload.close_admission();
    assert!(workload.shutdown().is_ok());
}

fn stop(
    requester: &WorkCancellationRequester,
    cancellation: &CancellationView,
    activation: AttemptActivationIdentity,
    cut: &mut Cut,
) -> AttemptStep {
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
    assert_eq!(cut.sources.actor.0.get(), actor.get());
    let runtime = inbox.runtime();
    let control = || {
        runtime
            .reserve_admitted_input_for(inbox.registration(), actor, InputClass::Control, &work)
            .unwrap()
            .into_standing_input()
            .unwrap()
    };
    let mut controls = AttemptOwnerControls::new(activation, control(), control());

    requester.request(CancellationReason::Requested).unwrap();
    let reason = cancellation
        .reason()
        .expect("original cancellation is visible");
    let stop_id = controls.stop_for_cancellation(reason).unwrap().unwrap();
    let (actual, input) = inbox.try_commit_owner_control().unwrap();
    assert_eq!(actual, stop_id);
    assert!(matches!(&input, AttemptInput::Port(port)
        if port.activation == activation && matches!(port.body, LogicalToAttemptBody::Stop { .. })));
    let stopped = cut.step_at(
        cut.sources.actor,
        input,
        Some(AttemptInputId(id(stop_id.get()))),
    );
    assert!(stopped.stop_producing && stopped.effect.is_none() && stopped.round_result.is_none());
    runtime.retire_input(actor, stop_id).unwrap();

    work.body_exited().unwrap();
    work.captures_exited().unwrap();
    controls.close_after_exit().unwrap();
    payer.finish().unwrap();
    scope.close_external().unwrap();
    ledger.reap_scope_ready(scope.id(), 8).unwrap();
    inbox.finish();
    ledger.remove_scope(scope.id()).unwrap();
    assert!(ledger.is_empty());
    stopped
}
