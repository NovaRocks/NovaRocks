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
    query_execution::lifecycle_plan::QueryCredentialLeases,
    task_execution::{
        host::adapter::tests::start,
        intent::{AckPayload, OperationAcknowledgement},
        sources::AttemptEstablishFacts,
    },
};
use novarocks_attempt_protocol::port_output::{
    AttemptTerminal, AttemptToLogical, AttemptToLogicalBody,
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
use novarocks_control_runtime::actor_turn::Clock;
use novarocks_control_runtime_core::identity::Tick;
use novarocks_coordination_contract::{
    AttemptActivationIdentity, AttemptFailureClass, AttemptRequestToken, LogicalToAttemptBody,
    MonotonicInstant,
};
use novarocks_execution_contract::{
    AdmissionTicketId, OperationKind, OperationOutcome, ParticipationAdmissionTicketReceipt,
    ParticipationRef, RuntimeEndpoint, SafeDetail,
};
use novarocks_query_application::{
    api::{BackendProcessObservation, BackendProcessObservationPort, BackendTopologyError},
    execution_error::ExecutionFailureSite,
};
use novarocks_types::BackendProcessId;
use novarocks_workload_control::CancellationReason;
use std::{
    num::NonZeroU64,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

fn id(value: u64) -> NonZeroU64 {
    NonZeroU64::new(value).unwrap()
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
    owner: PermissionOwner,
    material: Arc<AttemptEstablishMaterial>,
    compatibility: novarocks_types::NativeCompatibilityId,
    next: u64,
    round_failure: Option<AttemptFailure>,
}

impl Cut {
    fn new(
        start: AttemptStart,
        clock: Arc<SampleClock>,
        owner: PermissionOwner,
        material: Arc<AttemptEstablishMaterial>,
    ) -> Self {
        let compatibility = start.plan.compatibility;
        let sources = start.sources;
        let mut cut = Self {
            core: <Core as AttemptCore>::new(start).unwrap(),
            sources,
            clock,
            owner,
            material,
            compatibility,
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
                        let AttemptToLogicalBody::AuthorizationRequest {
                            request: authorization,
                            ..
                        } = request.body
                        else {
                            panic!("original Establish request");
                        };
                        let canonical = self
                            .material
                            .for_authorization(operation, authorization)
                            .unwrap();
                        let reply = self.exchange(request).await;
                        // Canonical lookup remains the very same Arc after LP reply.
                        let same = self
                            .material
                            .for_authorization(operation, authorization)
                            .unwrap();
                        assert!(Arc::ptr_eq(&canonical, &same));
                        step = self.step(self.sources.qa, AttemptInput::Port(reply));
                        continue;
                    }
                    EffectBody::Port(port)
                        if matches!(
                            port.body,
                            AttemptToLogicalBody::AuthorizationRequest { .. }
                                | AttemptToLogicalBody::EstablishSettlement { .. }
                        ) =>
                    {
                        let reply = self.exchange(port).await;
                        step = self.step(self.sources.qa, AttemptInput::Port(reply));
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
        let grant = QueuedInputGrantRef::new(
            id(2000 + generation.admission_effect().get()),
            generation,
            1,
        );
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

    async fn exchange(&mut self, port: &Port) -> novarocks_coordination_contract::LogicalToAttempt {
        let projected = novarocks_attempt_protocol::port_projection::project_attempt_message(
            port,
            &mut |_| panic!("this original refusal owns no Host failure reference"),
            &mut |p, op| self.material.authorization_facts(p, op, self.compatibility),
            &mut |_, _, _| panic!("these cases have no original Abort receipt"),
        )
        .unwrap();
        let reply = self.owner.exchange(projected, self.clock.instant()).await;
        match &reply.body {
            LogicalToAttemptBody::AuthorizationReply { decision, .. } => assert!(decision.is_ok()),
            LogicalToAttemptBody::EstablishSettlementReply { result, .. } => {
                assert!(result.is_ok())
            }
            LogicalToAttemptBody::PreReadyReplacementReply { .. } => {}
            _ => panic!("only original request replies: {reply:?}"),
        }
        reply
    }
    fn assert_original_settlement(
        &self,
        p: ParticipationRef,
        op: novarocks_execution_contract::TaskOperationId,
        settlement: novarocks_coordination_contract::AttemptIssueSettlement,
        ticket: ParticipationAdmissionTicketReceipt,
    ) {
        use novarocks_coordination_contract::logical_port::{
            AttemptAuthorization as A, AttemptToLogicalBody as B,
        };
        let (reference, token) = self
            .owner
            .sent
            .iter()
            .find_map(|m| match m.body {
                B::EstablishSettlement {
                    token,
                    authorization,
                    participation,
                    operation_id,
                    settlement: actual,
                } if participation == p && operation_id == op && actual == settlement => {
                    Some((authorization, token))
                }
                _ => None,
            })
            .expect("same original AP settlement crossed the actual current-ticket port");
        assert!(self.owner.replies.iter().any(|m| matches!(&m.body,
            LogicalToAttemptBody::EstablishSettlementReply { token: actual, authorization, result: Ok(()) }
                if *actual == token && *authorization == reference)));
        let original = self
            .owner
            .sent
            .iter()
            .find_map(|m| match &m.body {
                B::AuthorizationRequest {
                    request:
                        A::Establish {
                            participation,
                            operation_id,
                            ticket_id,
                            semantics,
                            native_compatibility_id,
                        },
                    ..
                } if *participation == p && *operation_id == op => {
                    Some((*ticket_id, *semantics, *native_compatibility_id))
                }
                _ => None,
            })
            .unwrap();
        let expected = self
            .material
            .authorization_facts(p, op, self.compatibility)
            .unwrap();
        assert_eq!(
            original,
            (ticket.ticket_id(), expected.semantics, self.compatibility)
        );
        self.owner.assert_unconcluded();
    }
    async fn permission(&mut self, port: Port, expected: bool) -> AttemptStep {
        let AttemptToLogicalBody::PreReadyReplacementQuery { token, .. } = port.body else {
            panic!("original QA permission port");
        };
        let reply = self.exchange(&port).await;
        assert_eq!(reply.activation, port.activation);
        assert!(matches!(
            reply.body,
            LogicalToAttemptBody::PreReadyReplacementReply { token: actual, may_observe }
                if actual == token && may_observe == expected
        ));
        self.step(self.sources.qa, AttemptInput::Port(reply))
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

    let owner = PermissionOwner::new(&mut start, case != Case::NoRecovery).await;
    let activation = start.activation.identity;
    let clock = Arc::new(SampleClock::default());
    let mut cut = Cut::new(start, clock.clone(), owner, Arc::new(material));

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

    cut.assert_original_settlement(
        participation,
        operation_id,
        novarocks_coordination_contract::AttemptIssueSettlement::Worker(
            OperationOutcome::IdentityMismatch,
        ),
        ticket,
    );
    if case == Case::StopBeforePermission {
        stop(&mut cut).await;
    }
    let positive = matches!(case, Case::Replacement | Case::StopAfterSample);
    let permitted = cut.permission(permission, positive).await;
    let effect = if positive {
        let (first, current) = sample(&mut cut, permitted, &observer, participation);
        assert!(current.effect.is_none());
        assert_eq!(probe.reads.load(Ordering::SeqCst), 1);
        let original_horizon = MonotonicInstant::ORIGIN.saturating_add(ticket.valid_for().get());
        assert_eq!(cut.core.next_deadline(), Some(original_horizon));
        assert!(cut.round_failure.is_some());
        cut.owner.assert_unconcluded();
        clock.set_seconds(2);
        if case == Case::StopAfterSample {
            let stopped = stop(&mut cut).await;
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
    let projected = novarocks_attempt_protocol::port_projection::project_attempt_message(
        &terminal,
        &mut |_| panic!("no Host failure reference"),
        &mut |_, _| panic!("terminal is not an Establish authorization"),
        &mut |_, _, _| panic!("terminal is not an Abort receipt"),
    )
    .unwrap();
    let novarocks_coordination_contract::logical_port::AttemptToLogicalBody::AttemptTerminal(
        novarocks_coordination_contract::logical_port::AttemptTerminal::Failed(failure),
    ) = &projected.body
    else {
        panic!("the exact original refusal stays the passive terminal");
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
    // The final terminal is one owned passive message, with no mutable old
    // outbox/watch to reread or reconstruct through a compatibility API.
    cut.owner.finish().await;
}

async fn stop(cut: &mut Cut) -> AttemptStep {
    cut.owner.advance_to(cut.clock.instant());
    let (input, original) = cut.owner.stop().await;
    let stopped = cut.step_at(
        cut.sources.owner,
        AttemptInput::Port(original),
        Some(AttemptInputId(id(input.get()))),
    );
    assert!(stopped.stop_producing && stopped.effect.is_none() && stopped.round_result.is_none());
    stopped
}

/// The independently driven AP Core still selects every original message.
/// This existing scripted AP carries it under a real current ticket to the
/// real LP Core; it does not decide the permission or invent a Worker reply.
struct PermissionOwner {
    fixture: crate::fixtures::FrontendFixture,
    logical: crate::fixtures::LogicalCase,
    attempt: crate::fixtures::AttemptCase,
    sent: Vec<novarocks_coordination_contract::logical_port::AttemptToLogical>,
    replies: Vec<novarocks_coordination_contract::LogicalToAttempt>,
    now: MonotonicInstant,
}
impl PermissionOwner {
    async fn new(start: &mut AttemptStart, recovery: bool) -> Self {
        use crate::fixtures::{AttemptScript, FrontendFixture, LogicalScript};
        let mut initial = LogicalScript::start(700, 0, 2);
        initial.identity = start.activation.identity.execution().query_id();
        initial.actor_instance = start.activation.identity.actor();
        initial.first_attempt.activation = start.activation.identity;
        initial.first_attempt.participations =
            start.plan.establish_material.keys().copied().collect();
        initial.policy.recovery = if recovery {
            novarocks_coordination_contract::RecoveryMode::RestartAttemptBeforeVisibility
        } else {
            novarocks_coordination_contract::RecoveryMode::NoRecovery
        };
        initial.budgets.replacement_reservation_valid_for =
            recovery.then_some(Duration::from_secs(30));
        initial.budgets.establish_per_participation =
            start.plan.authorization_limits.establish_per_participation;
        initial.budgets.abort_per_participation =
            start.plan.authorization_limits.abort_per_participation;
        // This test never opens client visibility. The original permission
        // predicate reads recovery/effect/budget/deadline/visibility state,
        // not the output carrier. Actual Client/Ready wiring is separate.
        let mut fixture = FrontendFixture::new();
        let logical = fixture.install_logical(initial, LogicalScript::default());
        let attempt = fixture
            .activate_attempt(
                &logical,
                start.activation.identity,
                AttemptScript::default(),
            )
            .await;
        start.sources.actor = AttemptSourceId(id(attempt.registration.actor().get()));
        start.sources.owner = AttemptSourceId(id(logical.registration.actor().get()));
        // These are the pure AP driver's external source descriptors, not
        // registrations or newly granted runtime input positions.
        let source = |n: u64| AttemptSourceId(id(10_000 + n));
        start.sources.publication = source(1);
        start.sources.qa = source(2);
        start.sources.process = source(3);
        start.sources.native = source(4);
        start.sources.observation = source(5);
        start.sources.capacity = source(6);
        start.sources.membership = source(7);
        start.sources.subscription = source(8);
        start.sources.transport = source(9);
        Self {
            fixture,
            logical,
            attempt,
            sent: Vec::new(),
            replies: Vec::new(),
            now: MonotonicInstant::ORIGIN,
        }
    }
    fn activation(&self) -> AttemptActivationIdentity {
        self.attempt.activation
    }
    fn advance_to(&mut self, now: MonotonicInstant) {
        assert!(now >= self.now);
        self.fixture
            .advance_time(now.saturating_duration_since(self.now));
        self.now = now;
    }
    async fn turn(&mut self) {
        self.fixture
            .progress(
                crate::fixtures::ProgressSelection::All,
                crate::fixtures::ProgressBudget::default(),
            )
            .await;
    }
    fn assert_unconcluded(&self) {
        use novarocks_logical_execution_protocol::effect::{
            ConsumerEffect, LogicalEffectBody, NativeEffect,
        };
        let actual = self.fixture.observe(&self.logical);
        assert!(!actual.failed);
        assert!(!actual.effects.iter().any(|e| matches!(
            e,
            LogicalEffectBody::Consumer(
                ConsumerEffect::PublishFailure(_) | ConsumerEffect::PublishCompletion
            ) | LogicalEffectBody::Native(
                NativeEffect::PrepareSuccessor { .. } | NativeEffect::DispatchQualification { .. }
            )
        )));
    }
    async fn exchange(
        &mut self,
        message: novarocks_coordination_contract::logical_port::AttemptToLogical,
        now: MonotonicInstant,
    ) -> novarocks_coordination_contract::LogicalToAttempt {
        use novarocks_coordination_contract::logical_port::AttemptToLogicalBody as Body;
        let token = match &message.body {
            Body::AuthorizationRequest { token, .. }
            | Body::EstablishSettlement { token, .. }
            | Body::PreReadyReplacementQuery { token, .. } => *token,
            _ => panic!("only the original request family"),
        };
        assert_eq!(message.activation, self.activation());
        self.advance_to(now);
        let action = message.action;
        self.sent.push(message.clone());
        self.attempt.script.enqueue_original(message);
        for _ in 0..256 {
            if let Some(reply) =
                self.attempt
                    .script
                    .received()
                    .into_iter()
                    .find(|m| match &m.body {
                        LogicalToAttemptBody::AuthorizationReply { token: actual, .. }
                        | LogicalToAttemptBody::EstablishSettlementReply {
                            token: actual, ..
                        }
                        | LogicalToAttemptBody::PreReadyReplacementReply {
                            token: actual, ..
                        } => *actual == token,
                        _ => false,
                    })
            {
                assert!(self.attempt.script.published().contains(&action));
                assert_eq!(reply.activation, self.activation());
                self.replies.push(reply.clone());
                return reply;
            }
            self.turn().await;
        }
        panic!(
            "actual current-ticket LP request did not produce its exact reply; sent={:?}, received={:?}",
            self.sent,
            self.attempt.script.received()
        );
    }
    async fn stop(
        &mut self,
    ) -> (
        novarocks_control_runtime_core::identity::InputId,
        novarocks_coordination_contract::LogicalToAttempt,
    ) {
        self.logical
            .request_cancel(CancellationReason::Requested)
            .unwrap();
        for _ in 0..256 {
            if let Some(original) = self
                .attempt
                .script
                .received_inputs()
                .into_iter()
                .find(|(_, m)| matches!(m.body, LogicalToAttemptBody::Stop { .. }))
            {
                assert!(matches!(&original.1.body, LogicalToAttemptBody::Stop {
                    reason: novarocks_coordination_contract::AttemptStopReason::Client,
                    failure,
                } if failure.input().site() == ExecutionFailureSite::Coordinator));
                let LogicalToAttemptBody::Stop { failure, .. } = &original.1.body else {
                    unreachable!()
                };
                assert_eq!(
                    failure.input().fact(),
                    novarocks_execution_contract::failure::FailureProjection::failure_fact(
                        &novarocks_coordination_contract::QueryFailureReason::Cancelled
                    )
                );
                return original;
            }
            self.turn().await;
        }
        panic!("the original Scope latch did not reach the actual LP/AP Stop port");
    }
    async fn finish(mut self) {
        use novarocks_coordination_contract::{
            AttemptConvergenceClosure, AttemptParticipationConvergence,
            logical_port::AttemptToLogicalBody,
        };
        use novarocks_logical_execution_protocol::effect::{ConsumerEffect, LogicalEffectBody};
        // The pure AP decision probe has already ended. Close the SAME actual
        // carrier through the original Scope cancellation and explicit script
        // facts. A script fact is never claimed to be a real remote RPC exit.
        self.stop().await;
        let roster = self.logical.start.first_attempt.participations.to_vec();
        let closure = self
            .attempt
            .script
            .enqueue(AttemptToLogicalBody::ConvergenceProgress {
                local_exited: false,
                closure: Some(Arc::new(AttemptConvergenceClosure::Evidence(
                    roster
                        .into_iter()
                        .map(|p| (p, AttemptParticipationConvergence::WorkerProcessReplaced))
                        .collect(),
                ))),
                lifecycle_runtime_filters: None,
            });
        let local = self
            .attempt
            .script
            .enqueue(AttemptToLogicalBody::ConvergenceProgress {
                local_exited: true,
                closure: None,
                lifecycle_runtime_filters: None,
            });
        self.logical.script.run.open();
        let mut concluded = false;
        for _ in 0..256 {
            self.turn().await;
            let actual = self.fixture.observe(&self.logical);
            if self.attempt.script.published().contains(&closure)
                && self.attempt.script.published().contains(&local)
                && actual.effects.iter().any(|e| {
                    matches!(
                        e,
                        LogicalEffectBody::Consumer(ConsumerEffect::PublishFailure(_))
                    )
                })
            {
                concluded = true;
                break;
            }
        }
        assert!(
            concluded,
            "actual cancellation and both explicit closure facts must be consumed before Registry close"
        );
        assert_eq!(
            self.attempt
                .script
                .received()
                .iter()
                .filter(|m| matches!(m.body, LogicalToAttemptBody::Stop { .. }))
                .count(),
            1,
            "the same original cancellation produces one Stop, even after a repeated request"
        );
        self.fixture.finish().await;
        assert!(
            self.attempt.exited(),
            "the real AP Actor/Scope must exit independently of its scripted local fact"
        );
        let history =
            crate::fixtures::history::CapturedHistory::drain(&self.fixture.history(), usize::MAX);
        history
            .check_completed_runtime(self.fixture.runtime(), self.fixture.ledger())
            .unwrap();
    }
}
