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
    control_runtime::{
        actor::{FrontendActor, FrontendActorKind, FrontendEffect, FrontendInput},
        host::{
            EffectExecutor, EffectSource, FrontendLanePolicy, HostBinding, HostRegistry,
            PendingHandoff, PreparedEffectJob, SourceProgress,
        },
        metrics::FrontendHostObservations,
        transport_withdrawal::AttemptEffectEnvelope,
    },
    native::task_transport::tests::Loopback,
    task_execution::{
        clock::ProcessMonotonicClock,
        host::{
            failure_lifecycle::FailureLifecycle,
            failure_publication::{FailureContext, FailurePublications},
            failure_supervisor::{FailureCleanupPort, FailureCleanupRequest, FailureSupervisor},
        },
        intent::OperationAcknowledgement,
    },
};
use novarocks_attempt_protocol::{
    capacity::ReservationToken,
    effect::{
        AttemptEffect, DispatchBatch, EffectBody, EffectClass, OperationIntent as ProtocolIntent,
        QueuedInputPayload,
    },
    input::{AttemptInput, ExecutorFact, QueuedInputGrantRef, SendGeneration},
    operation::OperationDispatchResult as PassiveDispatch,
};
use novarocks_control_runtime::{
    actor::ActorConfig,
    actor_turn::{
        ActorHostTurn, ActorRuntime, EffectTicket, InputPhase, ProcessClock,
        ReservedInputObligation,
    },
    causal_input::CausalInputObligation,
    history::HistoryCapture,
    obligation::WorkObligation,
    transaction::RuntimeError,
};
use novarocks_control_runtime_core::{
    capacity::CapacityError,
    history::HistoryFact,
    identity::{ActorId, InputId, Tick},
    mailbox::InputClass,
    scope::ScopeError,
};
use novarocks_coordination_contract::{
    AttemptActionId, AttemptActivationIdentity, MonotonicInstant,
};
use novarocks_execution_contract::{
    AbortCause, AbortParticipation, OperationOutcome, ParticipationRef,
};
use novarocks_types::FrontendProcessId;

type Position = CausalInputObligation<FrontendActor>;
type CanonicalReports = Arc<Mutex<VecDeque<(transport::SendKey, OperationAcknowledgement)>>>;

struct ScopeOwner {
    scope: ScopeHandle,
    parent: WorkObligation,
    payer: ReservationObligation,
    reports: CanonicalReports,
    closing: bool,
    exited: bool,
    paid: bool,
}

impl EffectSource for ScopeOwner {
    fn prepay_result(&mut self, _: &FrontendEffect) -> Option<Position> {
        unreachable!("the original Native Source owns all test effects")
    }

    fn bind_job(
        &mut self,
        _: &ActorRuntime<FrontendActor>,
        _: &ActorHostTurn<FrontendActor>,
        _: &EffectTicket<FrontendEffect>,
        _: Position,
    ) -> PreparedEffectJob {
        unreachable!("the original Native Source owns all test jobs")
    }

    fn return_unclaimed_result(&mut self, _: Position) {
        unreachable!()
    }

    fn core_consumed(&mut self, _: &[InputId]) {}

    fn retain_transport_failure(&mut self, original: Box<dyn std::any::Any + Send>) {
        std::panic::resume_unwind(original);
    }

    fn retain_transport_release(&mut self, original: Box<dyn std::any::Any + Send>) {
        let report = original
            .downcast::<transport::SendReportReturn>()
            .unwrap_or_else(|original| std::panic::resume_unwind(original));
        let transport::SendReportReturn::Receipt {
            key,
            receipt,
            delivery_error: None,
            ..
        } = *report
        else {
            panic!("expected the original delivered Native receipt");
        };
        assert!(receipt.unpublished.is_none());
        assert_eq!(receipt.original.operation_id(), key.operation);
        self.reports
            .lock()
            .unwrap()
            .push_back((key, receipt.original));
    }

    fn owner_failed_pending(&mut self, original: PendingHandoff) {
        std::panic::resume_unwind(Box::new(original));
    }

    fn close_external(&mut self) {
        self.scope.close_external().unwrap();
        self.closing = true;
    }

    fn result_admission_closed(&self) -> bool {
        self.closing
    }

    fn progress_retirement(
        &mut self,
        _: &ActorRuntime<FrontendActor>,
        _: &ActorHostTurn<FrontendActor>,
        _: Tick,
        budget: usize,
    ) -> SourceProgress {
        let mut units = 0;
        if self.closing && budget != 0 {
            if !self.exited {
                self.parent.body_exited().unwrap();
                self.parent.captures_exited().unwrap();
                self.exited = true;
                units += 1;
            }
            units += self.scope.reap_ready(budget - units).unwrap();
            if !self.paid && units < budget {
                match self.payer.finish() {
                    Ok(()) => {
                        self.paid = true;
                        units += 1;
                    }
                    Err(RuntimeError::Capacity(CapacityError::Scope(
                        ScopeError::ChildrenRemain,
                    ))) => {}
                    Err(error) => panic!("original scope could not finish: {error:?}"),
                }
            }
        }
        let retired = self.paid && self.scope.join_ready().unwrap();
        SourceProgress {
            units,
            progressed: units != 0,
            more: units != 0 && !retired,
            retired,
        }
    }
}

struct NoFailure;
impl FailureCleanupPort for NoFailure {
    fn abort_once(
        &mut self,
        _: AbortParticipation,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        panic!("the original Host must stay healthy")
    }
    fn finish(
        self: Box<Self>,
        _: FailureCleanupRequest,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        panic!("the original Host must stay healthy")
    }
}

struct OnlyNative;
impl EffectExecutor for OnlyNative {
    fn handoff(&self, original: PreparedEffectJob) -> Result<(), PreparedEffectJob> {
        Err(original)
    }
}

struct SendTrace {
    admit: Arc<AttemptEffectEnvelope>,
    send: Arc<AttemptEffectEnvelope>,
    guard: StepRegistration,
    grant: QueuedInputGrantRef,
    key: transport::SendKey,
}

fn paid_send(
    roles: &Roles,
    payer: ReservationId,
    protocol: &ProtocolIntent,
    generation_ordinal: u64,
) -> SendTrace {
    let id = |value| AttemptActionId::new(NonZeroU64::new(value).unwrap());
    let generation_id = SendGeneration::new(id(generation_ordinal));
    let key = transport::SendKey {
        operation: protocol.operation_id(),
        generation: generation_id,
    };
    let grant =
        QueuedInputGrantRef::new(NonZeroU64::new(generation_ordinal).unwrap(), generation_id);
    let backend = protocol.backend_process_id();
    let bytes = protocol.queued_bytes();
    let guard = roles
        .scope
        .reserve_child_next(
            generation(),
            payer,
            transport::send_queue_resources(
                &roles.layout,
                backend,
                TransportCapacityLane::Control,
                1,
                bytes,
            )
            .unwrap(),
            WorkKind::Settlement,
        )
        .unwrap();
    let reservation = NonZeroU64::new(guard.reservation_id().get()).unwrap();
    let admit = AttemptEffectEnvelope::new(AttemptEffect {
        id: generation_id.admission_effect(),
        class: EffectClass::Settlement,
        body: EffectBody::AdmitQueuedInput {
            operation: protocol.clone(),
            generation: generation_id,
            payload: QueuedInputPayload::AlreadyRetained,
            lane: protocol.lane(),
        },
    });
    let send = AttemptEffectEnvelope::new(AttemptEffect {
        id: id(generation_ordinal + 1),
        class: EffectClass::Settlement,
        body: EffectBody::SendBatch {
            batch: DispatchBatch {
                backend,
                lane: protocol.lane(),
                control_progress: true,
                operations: Box::new([protocol.clone()]),
                queued_bytes: bytes,
                queued_at: MonotonicInstant::ORIGIN,
                queued_inputs: Box::new([grant]),
            },
            reservation: ReservationToken(reservation),
        },
    });
    SendTrace {
        admit,
        send,
        guard,
        grant,
        key,
    }
}

#[tokio::test]
async fn native_apply_results_and_both_original_joins_retire_while_ordinary_stays_filled() {
    let history = HistoryCapture::new(4096);
    let roles = Roles::with_history(Some(history.clone()));
    let mut peer = Loopback::start().await;
    let mut wc = Governed::for_native_origin(Some(&peer.control_endpoint));
    let handle = Handle::current();
    let mut data = FrontendDataRuntime::new(handle.clone());
    let backend = BackendProcessId::new_v7();
    let execution =
        QueryExecutionId::new(QueryId::new(0x7a5c, 7), AttemptId::new(1).unwrap()).unwrap();
    let activation = AttemptActivationIdentity::from_parts(1, execution, 1);
    let participation = ParticipationRef::new(execution, FrontendProcessId::new_v7(), backend);
    peer.expect_transport_unknown();
    peer.accept_abort(participation);
    let protocol = ProtocolIntent::AbortParticipation(AbortParticipation::new(
        TaskOperationId::new_v7(),
        participation,
        AbortCause::QueryFailed,
    ));

    let (mut parent, payer) = roles
        .scope
        .reserve_next(
            generation(),
            ReservationResources::work_only(),
            WorkKind::Cleanup,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Cleanup)
        .unwrap();
    parent.start().unwrap();
    let (mut ordinary_work, mut ordinary_payer) = roles
        .scope
        .reserve_next(
            generation(),
            ReservationResources::work_only(),
            WorkKind::Business,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Business)
        .unwrap();
    ordinary_work.start().unwrap();
    let first = paid_send(&roles, payer.id(), &protocol, 1);
    let replay = paid_send(&roles, payer.id(), &protocol, 3);
    let sends = [
        (
            first.key,
            first.grant,
            first.send.protocol.id,
            first.guard.work_id(),
            true,
        ),
        (
            replay.key,
            replay.grant,
            replay.send.protocol.id,
            replay.guard.work_id(),
            false,
        ),
    ];
    let mailbox = [10, 8, 3, 8];
    let failure_work = roles.cleanup(parent.id());
    let inbox = custody::tests::WithdrawalInbox::with_failure_work(
        &roles.scope,
        [
            (first.admit, None),
            (first.send, Some(first.guard)),
            (replay.admit, None),
            (replay.send, Some(replay.guard)),
        ],
        mailbox,
        Some(failure_work.work()),
    );
    let runtime = inbox.runtime();
    let registration = inbox.registration();
    let actor = registration.actor();
    let commands: [[_; 2]; 2] =
        std::array::from_fn(|_| std::array::from_fn(|_| inbox.reserve_command(&parent)));
    let standing: [_; 3] = std::array::from_fn(|n| {
        runtime
            .reserve_admitted_input_for(
                registration,
                ActorId::new(n as u64 + 9).unwrap(),
                InputClass::Ordinary,
                &ordinary_work,
            )
            .unwrap()
            .into_standing_input()
            .unwrap()
    });
    let admission = transport::AdmissionSource::new(
        &parent,
        &payer,
        generation(),
        observer(),
        wc.authority(),
        [
            VecDeque::new(),
            VecDeque::from([inbox.reserve_result(&parent)]),
        ],
    )
    .unwrap_or_else(|_| panic!("original paid admission source"));
    let send_idle = [
        VecDeque::new(),
        VecDeque::from([inbox.reserve_result(&parent)]),
    ];
    let report_author = roles.cleanup(parent.id());
    let context = transport::SendContext {
        registration: registration.clone(),
        ledger: roles.ledger.clone(),
        scope: roles.scope.clone(),
        layout: roles.layout.clone(),
        transport: TransportBudget::DEFAULT,
        authority: wc.authority(),
        wire: crate::native::task_transport::wire::AttemptWireFacts {
            native_compatibility_id: novarocks_types::NativeCompatibilityId::new([1; 32]),
        },
        client: Client::for_endpoint(
            peer.control_endpoint.native_endpoint().clone(),
            NativeEndpointDomain::BackendControl,
            backend,
            data.clone(),
        ),
        apply: data.apply_send_port().clone(),
        clock: Arc::new(ProcessMonotonicClock::default()),
        observer: observer(),
    };
    let supervisor = FailureSupervisor::new(
        runtime.clone(),
        registration.clone(),
        failure_work,
        observer(),
        handle.clone(),
    );
    let host = HostRegistry::new(
        runtime.clone(),
        roles.ledger.clone(),
        Arc::new(FrontendHostObservations::new(Arc::new(
            ProcessClock::default(),
        ))),
    );
    let retirement = custody::TransportRetirements::new(
        runtime.clone(),
        registration.clone(),
        handle.clone(),
        1,
    );
    let reports = CanonicalReports::default();
    let mut exit = host
        .register(HostBinding {
            scope: roles.scope.clone(),
            registration: registration.clone(),
            kind: FrontendActorKind::Test,
            config: ActorConfig {
                mailbox,
                max_batches: 4,
                max_effects_per_step: 1,
                max_dependencies_per_effect: 0,
                turn_budget: 8,
                max_step_work: 1,
            },
            policy: FrontendLanePolicy::attempt(1),
            source: Box::new(transport::AttemptTransportSource::new(
                runtime.clone(),
                registration.clone(),
                inbox.source(),
                roles.scope.clone(),
                admission,
                [None, None, None, None, None, None],
                None,
                transport::AttemptSendSource {
                    requests: transport::FrozenSendRequests::default(),
                    contexts: Arc::new(BTreeMap::from([(backend, context)])),
                    report_author,
                    idle: send_idle,
                },
                (
                    FailureContext {
                        lifecycle: FailureLifecycle::freeze([participation].into_iter()),
                        publications: Arc::new(FailurePublications::new(
                            registration.clone(),
                            activation,
                            &roles.scope,
                        )),
                    },
                    transport::AttemptFailureBinding {
                        supervisor,
                        port: Box::new(NoFailure),
                        abort_drain: None,
                    },
                ),
                Box::new(ScopeOwner {
                    scope: roles.scope.clone(),
                    parent,
                    payer,
                    reports: reports.clone(),
                    closing: false,
                    exited: false,
                    paid: false,
                }),
            )),
            executor: Arc::new(transport::AttemptTransportExecutor {
                native: transport::ActorTransportExecutor(handle),
                other: Arc::new(OnlyNative),
            }),
            transport: Some(retirement),
            startup: None,
            capacity: None,
            membership: None,
            owner_controls: None,
            ordered_observations: None,
        })
        .unwrap_or_else(|_| panic!("the original Native Host registration"));

    inbox.select_command_source_for_test(&ordinary_work, true);
    let filled = standing.each_ref().map(|position| {
        let input = position
            .fill(FrontendInput::Attempt(Box::new(
                AttemptInput::CapacityReleased,
            )))
            .unwrap();
        (position.id(), input)
    });
    let assert_held = || {
        for (ordinal, (standing_id, input)) in filled.into_iter().enumerate() {
            let custody = runtime.input_custody(actor, input).unwrap().unwrap();
            assert_eq!(custody.phase, InputPhase::Filled);
            assert_eq!(custody.class, InputClass::Ordinary);
            assert_eq!(custody.source, ActorId::new(ordinal as u64 + 9).unwrap());
            assert_eq!(custody.standing_input_id, Some(standing_id));
            assert!(!custody.runtime_metadata);
        }
        assert_eq!(
            runtime
                .input_inventory(actor, None, 32)
                .unwrap()
                .into_iter()
                .filter(|row| row.class == InputClass::Ordinary && row.phase == InputPhase::Filled)
                .count(),
            mailbox[InputClass::Ordinary.index()],
        );
    };
    let host_turn = || {
        while let Some(turn) = runtime.claim_host_actor() {
            host.drive(Tick(1), turn);
        }
    };
    let submit = |command: ReservedInputObligation<FrontendActor>| {
        assert_held();
        let input = command.fill(FrontendInput::Test(1)).unwrap();
        assert!(runtime.poll_one(Tick(1)).unwrap());
        assert_held();
        input
    };
    let lifecycle_ready = || {
        runtime
            .input_inventory(actor, None, 32)
            .unwrap()
            .into_iter()
            .any(|row| row.class == InputClass::Lifecycle && row.phase == InputPhase::Filled)
    };
    let wait = Duration::from_secs(3);
    for ([admit, send], (key, grant, send_action, work, unknown)) in commands.into_iter().zip(sends)
    {
        let command = submit(admit);
        let admitted = tokio::time::timeout(wait, async {
            loop {
                drive(&mut wc.driver);
                host_turn();
                assert_held();
                if lifecycle_ready() {
                    let (_, fact) = inbox
                        .try_commit_next()
                        .expect("an original paid admission result");
                    assert_held();
                    break fact;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            matches!(admitted, ExecutorFact::QueuedInputGranted { operation, grant: actual, .. }
            if operation == key.operation && actual == grant)
        );
        runtime.retire_input(actor, command).unwrap();

        let command = submit(send);
        let mut started = false;
        tokio::time::timeout(wait, async {
            loop {
                drive(&mut wc.driver);
                host_turn();
                assert_held();
                if lifecycle_ready() {
                    let (_, fact) = inbox
                        .try_commit_next()
                        .expect("original Native send progress or receipt");
                    assert_held();
                    match fact {
                        ExecutorFact::SendStarted { effect, .. } => {
                            assert_eq!(effect, send_action);
                            assert!(!std::mem::replace(&mut started, true));
                        }
                        ExecutorFact::Operation(observed) => {
                            assert!(started);
                            assert_eq!(observed.generation, key.generation);
                            assert_eq!(observed.reply.operation_id(), key.operation);
                            if unknown {
                                assert_eq!(
                                    observed.reply.dispatch_result(),
                                    PassiveDispatch::TransportUnknown
                                );
                            } else {
                                assert_eq!(
                                    observed.reply.worker_outcome(),
                                    Some(OperationOutcome::Accepted)
                                );
                            }
                            break;
                        }
                        other => panic!("unexpected original Native fact: {other:?}"),
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        runtime.retire_input(actor, command).unwrap();
        let (returned, original) = tokio::time::timeout(wait, async {
            loop {
                drive(&mut wc.driver);
                host_turn();
                assert_held();
                if let Some(original) = reports.lock().unwrap().pop_front() {
                    break original;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(returned, key);
        assert_eq!(original.kind(), protocol.kind());
        assert_eq!(
            original.worker_outcome(),
            (!unknown).then_some(OperationOutcome::Accepted)
        );
        if unknown {
            assert_eq!(
                original.dispatch_result(),
                crate::task_execution::operation::OperationDispatchResult::TransportUnknown
            );
        }
        drop(original);
        tokio::time::timeout(wait, async {
            while roles.scope.work_lifecycle(work).unwrap().is_some() {
                drive(&mut wc.driver);
                host_turn();
                assert_held();
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    assert_held();
    assert!(reports.lock().unwrap().is_empty());
    let wire = peer.applied_requests();
    assert_eq!(wire.len(), 2);
    assert_eq!(wire[0], wire[1]);
    assert_eq!(peer.control_requests(), 2);
    assert_eq!(
        roles
            .ledger
            .try_capacity_dimensions([TOTAL_ITEMS, TOTAL_BYTES])
            .unwrap()
            .unwrap()
            .used,
        [0, 0]
    );

    let receipt = history.drain(4096);
    assert!(
        receipt.complete && receipt.remaining == 0,
        "{:?}",
        receipt.error
    );
    let scope_id = roles.scope.id();
    let mut previous_retired = None;
    for (_, _, _, work, _) in sends {
        let only = |predicate: fn(
            &HistoryFact,
            novarocks_control_runtime_core::identity::ScopeId,
            WorkId,
        ) -> bool| {
            let entries: Vec<_> = receipt
                .entries
                .iter()
                .filter(|entry| predicate(&entry.fact, scope_id, work))
                .collect();
            let [entry] = entries.as_slice() else {
                panic!("expected one original lifecycle fact: {work:?}")
            };
            entry.ordinal
        };
        let started = only(
            |fact, scope_id, work_id| matches!(fact, HistoryFact::Started { scope, work } if (*scope, *work) == (scope_id, work_id)),
        );
        let body = only(
            |fact, scope_id, work_id| matches!(fact, HistoryFact::BodyExited { scope, work } if (*scope, *work) == (scope_id, work_id)),
        );
        let captures = only(
            |fact, scope_id, work_id| matches!(fact, HistoryFact::CapturesExited { scope, work } if (*scope, *work) == (scope_id, work_id)),
        );
        let retired = only(
            |fact, scope_id, work_id| matches!(fact, HistoryFact::WorkRetired { scope, work } if (*scope, *work) == (scope_id, work_id)),
        );
        assert!(started < body && body < captures && captures < retired);
        assert!(previous_retired.is_none_or(|previous| previous < started));
        previous_retired = Some(retired);
    }

    // Release these same original positions only after both Native joins.
    inbox.select_command_source_for_test(&ordinary_work, false);
    for (_, input) in filled {
        let state = || runtime.input_custody(actor, input).unwrap().unwrap().phase;
        if state() == InputPhase::Filled {
            assert!(runtime.poll_one(Tick(1)).unwrap());
        }
        assert_eq!(state(), InputPhase::Committed);
        runtime.retire_input(actor, input).unwrap();
    }
    ordinary_work.body_exited().unwrap();
    ordinary_work.captures_exited().unwrap();
    for mut position in standing {
        position.close_after_exit().unwrap();
    }
    ordinary_payer.finish().unwrap();
    runtime.close_actor_admission(actor).unwrap();
    runtime.close_timer_source(actor).unwrap();
    exit.request_close().unwrap();
    tokio::time::timeout(wait, async {
        while !host.is_empty() {
            drive(&mut wc.driver);
            host_turn();
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    exit.wait().await.unwrap();
    assert!(runtime.is_empty() && roles.ledger.is_empty());
    let deadline = Instant::now() + wait;
    data.drain_original_apply_until(deadline).await.unwrap();
    data.drain_original_subscriptions_until(deadline)
        .await
        .unwrap();
    data.drain_native_outgoing_until(deadline).await.unwrap();
    assert!(peer.settle().await);
    assert!(wc.finish());
}
