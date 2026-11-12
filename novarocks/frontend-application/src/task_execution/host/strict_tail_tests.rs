// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use super::{
    adapter::tests::start, custody::tests::WithdrawalInbox, publication::PublicationRecord,
};
use crate::task_execution::{
    bridge::{
        control::AttemptOwnerControls,
        terminal::{TerminalOutbox, TerminalPublication},
    },
    completion::WriteVerdict,
    intent::{AckPayload, OperationAcknowledgement},
    protocol_facts::{AttemptProtocolFactsPublisher, WriteProtocolTracker},
    tests::delivery_chain_graph,
};
use novarocks_attempt_protocol::{
    AttemptCore, AttemptStep, Core, InputSelection,
    capacity::{QueueStage, ReservationToken, StepContext},
    config::DispatchLane,
    effect::{AttemptEffect, EffectBody},
    error::TaskExecutionError as ProtocolError,
    failure::AttemptFailure,
    input::{
        AttemptInput, AttemptInputClaim, AttemptInputId, AttemptSourceCut, AttemptSourceId,
        ExecutorFact, ObservationBatch, ObservationEvent, ObservationFact, ObservationOpening,
        PublicationApplied, QueuedInputGrantRef, ReceivedObservation, SendGeneration,
    },
    start::{AttemptSources, FrozenEstablish, PayloadRef},
};
use novarocks_control_runtime::transaction::RuntimeLedger;
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    identity::Generation,
    mailbox::InputClass,
    scope::{WorkAdmission, WorkKind},
};
use novarocks_coordination_contract::{
    AbortIssueSettlement, AttemptActionId, AttemptActivationIdentity, AttemptRequestToken,
    AttemptStopReason, AttemptTerminal, AttemptToLogical, AttemptToLogicalBody, ExecutionEffect,
    LogicalToAttempt, LogicalToAttemptBody, MonotonicInstant, ResultParticipationWithdrawal,
    SelectedTaskTermination,
};
use novarocks_execution_contract::{
    AbortCause, AbortParticipation, AdmissionEpochCapability, ExchangeDeliveryRequirement,
    OperationKind, OperationOutcome, ParticipationRef, SafeDetail, TaskFailure, TaskFailurePhase,
    TaskIdentity, TaskOperationId, TaskOutputFacts, TaskState, TaskStatus, TaskStatusVersion,
    TerminationDetail, failure::FailureProjection,
};
use novarocks_physical_plan::PlanVersionId;
use novarocks_query_application::{
    api::{NativeAttemptTerminal, PlanSeal, QueryExecutionError},
    coordination::{
        AcceptedRootControlPort, AcceptedRootControlRequest, LogicalExecutionActorConfig,
        accepted_root_status_projection_with_control_port,
    },
    execution_error::{ExecutionFailureInput, ExecutionFailureSite, QueryFailureReason},
    session_outcome::governed_cancellation_failure_input,
    test_support::LogicalExecutionTestHarness,
};
use novarocks_result_contract::{PerKind, SummaryKind};
use novarocks_types::BackendProcessId;
use novarocks_workload_control::{
    CancellationReason, ResourceConfig, Stage, WorkCancellationRequestOutcome, WorkClass,
    WorkRequest, WorkloadConfig, WorkloadControl,
};
use std::{
    collections::BTreeSet,
    num::{NonZeroU64, NonZeroUsize},
    sync::{Arc, Mutex},
    task::Poll,
    time::{Duration, Instant},
};

#[derive(Debug, Default)]
struct SealIngress(Mutex<Option<AcceptedRootControlRequest>>);

impl AcceptedRootControlPort for SealIngress {
    fn enqueue_root_control(
        &self,
        request: AcceptedRootControlRequest,
    ) -> Result<(), AcceptedRootControlRequest> {
        assert!(request.terminal_control_root().is_none());
        assert!(self.0.lock().unwrap().replace(request).is_none());
        Ok(())
    }
}

struct Units {
    remaining: usize,
    queue: Option<BackendProcessId>,
}

impl StepContext for Units {
    fn charge(&mut self, units: usize) -> bool {
        let Some(remaining) = self.remaining.checked_sub(units) else {
            return false;
        };
        self.remaining = remaining;
        true
    }

    fn reserve_queue_stage(
        &mut self,
        lane: DispatchLane,
        control: bool,
        backend: BackendProcessId,
        records: usize,
        _: usize,
    ) -> QueueStage {
        assert_eq!(
            Some(backend),
            self.queue.take(),
            "the exact fixture Abort queue"
        );
        assert_eq!(lane, DispatchLane::Lifecycle);
        assert!(control);
        assert_eq!(records, 1);
        QueueStage::Granted(ReservationToken(id(9001)))
    }
}

fn id(value: u64) -> NonZeroU64 {
    NonZeroU64::new(value).unwrap()
}

struct PendingSealReply {
    action: AttemptActionId,
    token: AttemptRequestToken,
    withdrawals: Arc<[ResultParticipationWithdrawal]>,
}

struct StrictTail {
    core: Core,
    sources: AttemptSources,
    input_id: u64,
    publication: PublicationRecord,
    terminal: Arc<TerminalOutbox>,
    queue: Option<BackendProcessId>,
}

impl StrictTail {
    fn input(&mut self, source: AttemptSourceId, input: AttemptInput) -> AttemptStep {
        self.input_id += 1;
        let input_id = AttemptInputId(id(self.input_id));
        self.core
            .handle(
                MonotonicInstant::ORIGIN,
                AttemptInputClaim {
                    source,
                    input: input_id,
                    native_cut: Some(AttemptSourceCut {
                        source: self.sources.native,
                        tail: (source == self.sources.native).then_some(input_id),
                    }),
                    selected_cut: Some(AttemptSourceCut {
                        source: self.sources.observation,
                        tail: (source == self.sources.observation).then_some(input_id),
                    }),
                },
                input,
                &mut Units {
                    remaining: 128,
                    queue: self.queue,
                },
            )
            .unwrap()
    }

    fn applied(&mut self, effect: AttemptActionId, applied: PublicationApplied) -> AttemptStep {
        self.input(
            self.sources.publication,
            AttemptInput::Executor(ExecutorFact::PublicationApplied {
                effect,
                result: Ok(applied),
            }),
        )
    }

    fn observe(&mut self, observations: Vec<ReceivedObservation>) -> AttemptStep {
        self.input(
            self.sources.observation,
            AttemptInput::Executor(ExecutorFact::Observations(
                ObservationBatch::new(observations).unwrap(),
            )),
        )
    }

    // Keep the original Seal delivery at its AwaitSeal boundary.
    fn drive(&mut self, step: AttemptStep) -> Option<PendingSealReply> {
        let terminal = self.terminal.clone();
        let effect = self.advance(
            step,
            |round| assert_eq!(round, Ok(())),
            |port| {
                assert!(
                    terminal
                        .publish(Box::new(TerminalPublication::new(port)))
                        .is_ok()
                )
            },
        )?;
        let EffectBody::Port(port) = effect.body else {
            panic!("unexpected Native work in the strict-tail fixture: {effect:?}");
        };
        let AttemptToLogicalBody::SealResult { token, result } = port.body else {
            panic!("expected the original Seal reply");
        };
        Some(PendingSealReply {
            action: effect.id,
            token,
            withdrawals: result.expect("the exact Root success is sealed"),
        })
    }

    fn advance(
        &mut self,
        mut step: AttemptStep,
        mut observe_round: impl FnMut(Result<(), AttemptFailure>),
        mut observe_terminal: impl FnMut(AttemptToLogical<AttemptFailure>),
    ) -> Option<AttemptEffect> {
        for _ in 0..32 {
            if let Some(result) = step.round_result {
                observe_round(result);
            }
            if let Some(effect) = step.effect {
                match effect.body {
                    EffectBody::Publish(publication) => {
                        assert_eq!(
                            step.selection,
                            InputSelection::Only(self.sources.publication)
                        );
                        let applied = self.publication.publish(&publication, None).unwrap();
                        step = self.applied(effect.id, applied);
                        continue;
                    }
                    EffectBody::Port(port)
                        if matches!(port.body, AttemptToLogicalBody::AttemptTerminal(_)) =>
                    {
                        observe_terminal(port);
                    }
                    EffectBody::Port(ref port)
                        if matches!(port.body, AttemptToLogicalBody::SealResult { .. }) =>
                    {
                        assert_eq!(
                            step.selection,
                            InputSelection::Only(self.sources.publication)
                        );
                        return Some(effect);
                    }
                    EffectBody::ReleaseCreationPayload { .. } => {}
                    _ => return Some(effect),
                }
            }
            match step.selection {
                InputSelection::Only(source) if source == self.sources.actor => {
                    step = self.input(source, AttemptInput::Timer);
                }
                InputSelection::Only(source)
                    if source == self.sources.observation
                        && self.core.next_deadline() == Some(MonotonicInstant::ORIGIN) =>
                {
                    step = self.input(self.sources.actor, AttemptInput::Timer);
                }
                InputSelection::Only(source) if source == self.sources.observation => return None,
                InputSelection::Prefer(source) if source == self.sources.native => return None,
                selection => panic!("unexpected strict-tail continuation: {selection:?}"),
            }
        }
        panic!("the original strict-tail continuation did not finish");
    }

    fn issue_residual_abort(
        &mut self,
        activation: AttemptActivationIdentity,
        participation: ParticipationRef,
    ) -> (TaskOperationId, SendGeneration) {
        let operation = TaskOperationId::new_v7();
        let step = self.input(
            self.sources.actor,
            AttemptInput::Port(LogicalToAttempt {
                activation,
                body: LogicalToAttemptBody::Abort {
                    action: AttemptActionId::new(id(9002)),
                    request: AbortParticipation::new(
                        operation,
                        participation,
                        AbortCause::QueryFailed,
                    ),
                },
            }),
        );
        assert!(step.effect.is_none());
        let no_terminal = |port| panic!("the Native input has yet to settle: {port:?}");
        let successful = |round| assert_eq!(round, Ok(()));
        let timer = self.input(self.sources.actor, AttemptInput::Timer);
        let admit = self.advance(timer, successful, no_terminal).unwrap();
        let EffectBody::AdmitQueuedInput {
            operation: original,
            generation,
            ..
        } = admit.body
        else {
            panic!("Core must admit the original residual operation");
        };
        assert_eq!(original.operation_id(), operation);
        let waiting = self.input(self.sources.actor, AttemptInput::Timer);
        assert!(self.advance(waiting, successful, no_terminal).is_none());
        let grant = QueuedInputGrantRef::new(id(9003), generation, 1);
        self.queue = Some(participation.backend_process_id());
        let granted = self.input(
            self.sources.transport,
            AttemptInput::Executor(ExecutorFact::QueuedInputGranted {
                operation,
                grant,
                queued_bytes: original.queued_bytes(),
            }),
        );
        let send = self.advance(granted, successful, no_terminal).unwrap();
        self.queue = None;
        assert!(matches!(send.body, EffectBody::SendBatch { ref batch, .. }
            if batch.queued_inputs.as_ref() == [grant]));
        let waiting = self.input(self.sources.actor, AttemptInput::Timer);
        assert!(self.advance(waiting, successful, no_terminal).is_none());
        let started = self.input(
            self.sources.native,
            AttemptInput::Executor(ExecutorFact::SendStarted {
                effect: send.id,
                started_at: MonotonicInstant::ORIGIN,
            }),
        );
        assert!(self.advance(started, successful, no_terminal).is_none());
        (operation, generation)
    }

    fn residual_abort_failure(
        &mut self,
        participation: ParticipationRef,
        (operation, generation): (TaskOperationId, SendGeneration),
    ) -> (AttemptStep, AttemptFailure) {
        let original = OperationAcknowledgement::worker_receipt(
            operation,
            OperationKind::AbortParticipation,
            OperationOutcome::OwnerFailed,
            AckPayload::None,
        )
        .with_detail(Some(SafeDetail::truncating(
            "original residual Native Abort owner failed",
        )));
        let observation = original
            .attempt_observation(operation, generation, None, None)
            .unwrap();
        let step = self.input(
            self.sources.native,
            AttemptInput::Executor(ExecutorFact::Operation(observation)),
        );
        assert!(step.stop_producing);
        assert!(matches!(&step.abort_settlement,
            Some(AttemptEffect {
                body: EffectBody::Port(AttemptToLogical {
                    body: AttemptToLogicalBody::AbortSettlement {
                        abort_action, generation: original, participation: exact, operation_id,
                        settlement: AbortIssueSettlement::FailedClosed, ..
                    }, ..
                }), ..
            }) if abort_action.get() == 9002 && *original == generation.admission_effect()
                && *exact == participation && *operation_id == operation));
        let failure = AttemptFailure::TaskProtocol(Box::new(ProtocolError::OperationFailed {
            task: None,
            participation,
            operation_id: operation,
            kind: OperationKind::AbortParticipation,
            rejection: original.worker_header().unwrap(),
        }));
        (step, failure)
    }
}

fn apply_governed_stop(
    tail: &mut StrictTail,
    activation: AttemptActivationIdentity,
    reason: CancellationReason,
) -> AttemptStep {
    let generation = Generation::new(1).unwrap();
    let ledger = RuntimeLedger::new(ResourceVector::default());
    let scope = ledger.open_scope_next(None, generation).unwrap();
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
    let inbox = WithdrawalInbox::new(&scope);
    let standing = || {
        inbox
            .runtime()
            .reserve_admitted_input_for(
                inbox.registration(),
                inbox.registration().actor(),
                InputClass::Control,
                &work,
            )
            .unwrap()
            .into_standing_input()
            .unwrap()
    };
    let mut controls = AttemptOwnerControls::new(activation, standing(), standing());
    let input = controls
        .stop_for_cancellation(reason.clone())
        .unwrap()
        .unwrap();
    let (committed, stop) = inbox.try_commit_owner_control().unwrap();
    assert_eq!(committed, input);
    assert!(matches!(
        &stop,
        AttemptInput::Port(LogicalToAttempt {
            activation: actual,
            body: LogicalToAttemptBody::Stop {
                reason: AttemptStopReason::Client,
                ..
            },
        }) if *actual == activation
    ));
    let step = tail.input(tail.sources.actor, stop);
    assert!(step.stop_producing);
    assert_eq!(controls.stop_for_cancellation(reason).unwrap(), None);
    assert!(inbox.try_commit_owner_control().is_none());
    controls.close_after_exit().unwrap();
    work.body_exited().unwrap();
    work.captures_exited().unwrap();
    payer.finish().unwrap();
    scope.reap_ready(8).unwrap();
    scope.close_external().unwrap();
    inbox.finish();
    ledger.remove_scope(scope.id()).unwrap();
    assert!(ledger.is_empty());
    step
}

fn finished(task: TaskIdentity) -> TaskStatus {
    TaskStatus::try_new(
        task,
        TaskStatusVersion::new(4).unwrap(),
        TaskState::Finished,
        None,
        TaskOutputFacts::new(true),
    )
    .unwrap()
}

#[derive(Clone, Copy)]
enum TailCase {
    StrictCompletion { writer_first: bool },
    StrictSourceFailure,
    StrictCancellation,
    OrdinaryCompletion,
    SealVsNativeFailure { seal_first: bool },
}

#[tokio::test]
async fn strict_tail_keeps_native_completion_behind_both_original_task_terminals() {
    run_tail_cases([true, false].map(|writer_first| TailCase::StrictCompletion { writer_first }))
        .await;
}

#[tokio::test]
async fn strict_tail_preserves_original_source_failure_and_governed_cancellation() {
    run_tail_cases([TailCase::StrictSourceFailure, TailCase::StrictCancellation]).await;
}

#[tokio::test]
async fn demand_driven_tail_completes_at_original_seal_with_live_siblings() {
    run_tail_cases([TailCase::OrdinaryCompletion]).await;
}

#[tokio::test]
async fn original_root_seal_reply_and_native_advance_failure_follow_their_cut_order() {
    run_tail_cases([true, false].map(|seal_first| TailCase::SealVsNativeFailure { seal_first }))
        .await;
}

async fn run_tail_cases(cases: impl IntoIterator<Item = TailCase>) {
    for case in cases {
        let strict = !matches!(
            case,
            TailCase::OrdinaryCompletion | TailCase::SealVsNativeFailure { .. }
        );
        let graph = delivery_chain_graph(if strict {
            ExchangeDeliveryRequirement::MustDrain
        } else {
            ExchangeDeliveryRequirement::DemandDriven
        });
        let root = graph.root_identity();
        let execution = root.query_execution_id();
        let writer = graph
            .tasks()
            .find(|task| task.identity().stage_id().get() == 2)
            .unwrap()
            .identity();
        let summary = graph
            .tasks()
            .find(|task| task.identity().stage_id().get() == 1)
            .unwrap()
            .identity();
        assert_ne!(writer, summary);
        let plan = PlanSeal::Version(PlanVersionId::try_new([197; 16]).unwrap());
        let writers = strict.then_some(writer).into_iter().collect();
        let summaries = PerKind {
            write_counts: strict.then(|| BTreeSet::from([summary])),
            delete_verification: None,
        };
        let (publisher, mut facts) =
            AttemptProtocolFactsPublisher::for_graph(plan, &graph, &writers, &summaries).unwrap();

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
        let requester = work.owner.cancellation_requester();
        let cancellation = scope.cancellation().unwrap();
        let stage = scope.try_acquire(Stage::Execution).unwrap();
        let config = LogicalExecutionActorConfig::single_attempt_completion(
            execution,
            ExecutionEffect::None,
            NonZeroUsize::new(8).unwrap(),
            Vec::new(),
            NonZeroUsize::MIN,
            NonZeroUsize::MIN,
            work.owner,
            stage,
        )
        .unwrap();
        let mut logical = LogicalExecutionTestHarness::install(
            tokio::runtime::Handle::current(),
            config,
            execution,
            Vec::new(),
        )
        .unwrap();
        logical.activate_initial().await.unwrap();
        let drive = logical.native_attempt_drive();
        let activation = drive.identity();
        let control = Arc::new(SealIngress::default());
        let (sender, statuses) =
            accepted_root_status_projection_with_control_port(root, control.clone());
        let terminal = TerminalOutbox::new(activation);

        let mut payload = NonZeroU64::MIN;
        let (graph, _creation_backing) = graph.into_attempt_graph(&mut payload).unwrap();
        let participations = graph
            .tasks
            .values()
            .map(|task| task.participation)
            .collect::<BTreeSet<_>>();
        let mut start = start();
        start.activation.identity = activation;
        start.plan.authorization_limits = drive.authorization_limits();
        start.plan.graph = graph;
        start.plan.writers = writers;
        start.plan.summary_sources = summaries;
        start.plan.establish_material = participations
            .iter()
            .map(|&participation| {
                let material = PayloadRef(payload);
                payload = payload.checked_add(1).unwrap();
                (
                    participation,
                    FrozenEstablish {
                        material,
                        queued_bytes: 256,
                    },
                )
            })
            .collect();
        start.plan.admission_epochs = participations
            .iter()
            .map(|participation| {
                (
                    participation.backend_process_id(),
                    AdmissionEpochCapability::try_from_bytes([0x61; 16]).unwrap(),
                )
            })
            .collect();
        start.plan.preparation_positions = participations
            .iter()
            .map(|participation| {
                (
                    participation.backend_process_id(),
                    start.plan.deployment_window,
                )
            })
            .collect();

        let now = start.initial_now;
        assert_eq!(now, MonotonicInstant::ORIGIN);
        let sources = start.sources;
        let mut core = <Core as AttemptCore>::new(start).unwrap();
        let generations = participations
            .iter()
            .enumerate()
            .map(|(index, &participation)| {
                let identity = id(index as u64 + 100);
                let generation = core
                    .open_subscription_for_test(
                        participation,
                        AttemptActionId::new(identity),
                        AttemptRequestToken::new(identity),
                        &mut Units {
                            remaining: 128,
                            queue: None,
                        },
                    )
                    .unwrap();
                (participation, generation)
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        let observation = |status: TaskStatus| {
            let participation = participations
                .iter()
                .copied()
                .find(|part| part.backend_process_id() == status.identity().backend_process_id())
                .unwrap();
            ReceivedObservation {
                participation,
                generation: generations[&participation],
                received_at: now,
                opening: ObservationOpening {
                    started_at: now,
                    opened_at: Some(now),
                },
                event: ObservationEvent::Fact {
                    source_revision: None,
                    fact: ObservationFact::Status(status),
                },
            }
        };
        let mut tail = StrictTail {
            core,
            sources,
            input_id: 0,
            terminal: terminal.clone(),
            queue: None,
            publication: PublicationRecord::new(activation, sender, publisher, None, None),
        };
        let started = tail.input(sources.actor, AttemptInput::Bootstrap);
        assert_eq!(started.selection, InputSelection::Prefer(sources.native));
        let initial = tail.observe(vec![
            observation(finished(root)),
            observation(TaskStatus::created(writer)),
            observation(TaskStatus::created(summary)),
        ]);
        assert!(tail.drive(initial).is_none());
        let initial = facts.take_changes(activation, root, plan).unwrap().unwrap();
        assert!(!initial.root_success_sealed());
        for sibling in [writer, summary] {
            assert!(initial.tasks().iter().any(|change| {
                change.identity() == sibling
                    && change.status().is_some_and(|status| !status.is_terminal())
            }));
        }

        let finish = async move |mut logical: LogicalExecutionTestHarness, drive| {
            drop(drive);
            work.business.release();
            logical
                .finish_until(Instant::now() + Duration::from_secs(2))
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_secs(2), scope.wait_released())
                .await
                .unwrap();
            workload.close_admission();
            assert!(workload.shutdown().is_ok());
        };
        let residual_participation = participations
            .iter()
            .copied()
            .find(|part| part.backend_process_id() == root.backend_process_id())
            .unwrap();
        let residual = matches!(case, TailCase::SealVsNativeFailure { .. })
            .then(|| tail.issue_residual_abort(activation, residual_participation));
        let mut seal = statuses.begin_success_seal_request().unwrap();
        let token = AttemptRequestToken::new(id(1000));
        let seal_request = || {
            AttemptInput::Port(LogicalToAttempt {
                activation,
                body: LogicalToAttemptBody::SealRequest { token },
            })
        };
        if matches!(case, TailCase::SealVsNativeFailure { seal_first: false }) {
            let (step, failure) =
                tail.residual_abort_failure(residual_participation, residual.unwrap());
            let mut observed_round = None;
            assert!(
                tail.advance(
                    step,
                    |result| {
                        if let Err(original) = result {
                            assert!(observed_round.replace(original).is_none());
                        }
                    },
                    |port| {
                        assert!(matches!(
                            &port.body,
                            AttemptToLogicalBody::AttemptTerminal(AttemptTerminal::Failed(actual))
                                if actual == &failure
                        ));
                        assert!(
                            terminal
                                .publish(Box::new(TerminalPublication::new(port)))
                                .is_ok()
                        );
                    },
                )
                .is_none()
            );
            assert_eq!(observed_round, Some(failure));
            let mut terminal_wait = Box::pin(terminal.terminal());
            let Poll::Ready(Ok(NativeAttemptTerminal::Failed(native))) =
                futures::poll!(terminal_wait.as_mut())
            else {
                panic!("the Native error preceded Seal acceptance");
            };
            assert_eq!(
                native.input().site(),
                ExecutionFailureSite::Operation {
                    participation: residual_participation,
                    operation_id: residual.unwrap().0,
                    kind: OperationKind::AbortParticipation,
                }
            );
            assert!(matches!(
                seal.try_recv(),
                Err(tokio::sync::oneshot::error::TryRecvError::Empty)
            ));
            let requested = tail.input(sources.observation, seal_request());
            assert!(requested.root_control_processed.is_some());
            let reply = tail
                .advance(
                    requested,
                    |round| assert_eq!(round, Ok(())),
                    |port| panic!("the original Native terminal was already published: {port:?}"),
                )
                .unwrap();
            let EffectBody::Port(port) = reply.body else {
                panic!("the original Seal request requires its reply");
            };
            use novarocks_attempt_protocol::root_control::RootSealRefusal;
            let refusal = RootSealRefusal::AttemptCleanup.message();
            assert!(matches!(
                &port.body,
                AttemptToLogicalBody::SealResult { token: actual, result: Err(failure) }
                    if *actual == token && *failure == AttemptFailure::TaskProtocol(Box::new(ProtocolError::Schedule(refusal.into())))
            ));
            let projected = QueryExecutionError::from_execution(
                crate::task_execution::error::TaskExecutionError::Schedule(refusal.into())
                    .preparation_input(),
            );
            control
                .0
                .lock()
                .unwrap()
                .take()
                .unwrap()
                .reject(projected.clone());
            let applied = tail.applied(reply.id, PublicationApplied::SealResult);
            assert!(tail.drive(applied).is_none());
            assert_eq!(seal.await.unwrap(), Err(projected));
            if let Some(delta) = facts.take_changes(activation, root, plan).unwrap() {
                assert!(!delta.root_success_sealed());
            }
            finish(logical, drive).await;
            continue;
        }
        let step = tail.input(sources.observation, seal_request());
        assert!(step.root_control_processed.is_some());
        let reply = tail
            .drive(step)
            .expect("the Root seal is independent of strict Task terminal facts");
        assert_eq!(reply.token, token);
        if strict {
            assert!(reply.withdrawals.is_empty());
        } else {
            let participant = participations
                .iter()
                .copied()
                .find(|part| part.backend_process_id() == writer.backend_process_id())
                .unwrap();
            assert_eq!(
                reply.withdrawals.as_ref(),
                &[ResultParticipationWithdrawal::new(
                    activation,
                    root,
                    participant
                )]
            );
        }
        let mut terminal_wait = Box::pin(terminal.terminal());
        let paused = tail.input(sources.actor, AttemptInput::Timer);
        assert!(paused.effect.is_none() && paused.round_result.is_none());
        assert_eq!(paused.selection, InputSelection::Only(sources.publication));
        let foreign = tail.applied(
            AttemptActionId::new(id(9000)),
            PublicationApplied::SealResult,
        );
        assert!(foreign.effect.is_none() && foreign.round_result.is_none());
        assert_eq!(foreign.selection, InputSelection::Only(sources.publication));
        assert!(futures::poll!(terminal_wait.as_mut()).is_pending());
        assert!(matches!(
            seal.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));

        let request = control
            .0
            .lock()
            .unwrap()
            .take()
            .expect("the original QA request");
        request
            .accept(
                tail.publication.take_root_for_seal().unwrap(),
                reply.withdrawals.to_vec(),
            )
            .unwrap();
        let applied = tail.applied(reply.action, PublicationApplied::SealResult);
        if matches!(case, TailCase::SealVsNativeFailure { seal_first: true }) {
            let mut completed = None;
            assert!(
                tail.advance(
                    applied,
                    |round| assert_eq!(round, Ok(())),
                    |port| {
                        assert!(matches!(
                            port.body,
                            AttemptToLogicalBody::AttemptTerminal(AttemptTerminal::Completed)
                        ));
                        assert!(completed.replace(port).is_none());
                    },
                )
                .is_none()
            );
            // The real Core has selected the original terminal, but the Host
            // has not delivered it and the QA caller has not consumed its Seal reply.
            assert!(futures::poll!(terminal_wait.as_mut()).is_pending());
            let (step, failure) =
                tail.residual_abort_failure(residual_participation, residual.unwrap());
            let mut observed_round = None;
            assert!(
                tail.advance(
                    step,
                    |result| {
                        if let Err(original) = result {
                            assert!(observed_round.replace(original).is_none());
                        }
                    },
                    |port| panic!("the residual error replaced the original terminal: {port:?}"),
                )
                .is_none()
            );
            assert_eq!(observed_round, Some(failure));
            assert!(
                terminal
                    .publish(Box::new(TerminalPublication::new(completed.unwrap())))
                    .is_ok()
            );
            assert!(matches!(
                futures::poll!(terminal_wait.as_mut()),
                Poll::Ready(Ok(NativeAttemptTerminal::Completed))
            ));
        } else {
            assert!(tail.drive(applied).is_none());
        }
        seal.await.unwrap().unwrap();
        let sealed = facts.take_changes(activation, root, plan).unwrap().unwrap();
        assert!(sealed.root_success_sealed());
        assert!(sealed.selected_failure().is_none());
        if strict {
            assert!(futures::poll!(terminal_wait.as_mut()).is_pending());
        }

        match case {
            TailCase::StrictCompletion { writer_first } => {
                let mut tracker = WriteProtocolTracker::from_initial(&initial).unwrap();
                assert!(tracker.execution_verdict().is_pending());
                tracker.observe(&sealed).unwrap();
                assert!(tracker.execution_verdict().is_pending());
                let (first, last, remaining_kind) = if writer_first {
                    (writer, summary, Some(SummaryKind::WriteCounts))
                } else {
                    (summary, writer, None)
                };
                let first = tail.observe(vec![observation(finished(first))]);
                assert!(tail.drive(first).is_none());
                let delta = facts.take_changes(activation, root, plan).unwrap().unwrap();
                tracker.observe(&delta).unwrap();
                assert_eq!(
                    tracker.execution_verdict(),
                    WriteVerdict::TaskNotFinished {
                        task: last,
                        state: TaskState::Planned,
                        summary_kind: remaining_kind,
                    }
                );
                assert!(futures::poll!(terminal_wait.as_mut()).is_pending());

                let last = tail.observe(vec![observation(finished(last))]);
                assert!(tail.drive(last).is_none());
                let delta = facts.take_changes(activation, root, plan).unwrap().unwrap();
                tracker.observe(&delta).unwrap();
                assert_eq!(tracker.execution_verdict(), WriteVerdict::Complete);
                assert!(matches!(
                    tokio::time::timeout(Duration::from_secs(2), terminal_wait)
                        .await
                        .unwrap()
                        .unwrap(),
                    NativeAttemptTerminal::Completed
                ));
            }
            TailCase::StrictSourceFailure => {
                let termination = TerminationDetail::Failed(TaskFailure::new_in_phase(
                    QueryFailureReason::SplitAssignmentFailed.failure_fact(),
                    SafeDetail::truncating("original strict source failed after root seal"),
                    TaskFailurePhase::Execution,
                ));
                let selected = SelectedTaskTermination::new(summary, termination.clone());
                let step = tail.observe(vec![observation(
                    TaskStatus::try_new(
                        summary,
                        TaskStatusVersion::new(2).unwrap(),
                        TaskState::Failing,
                        Some(termination),
                        TaskOutputFacts::default(),
                    )
                    .unwrap()
                    .with_installed(),
                )]);
                assert!(tail.drive(step).is_none());
                let delta = facts.take_changes(activation, root, plan).unwrap().unwrap();
                assert!(delta.root_success_sealed());
                assert_eq!(delta.selected_failure(), Some(&selected));
                assert!(delta.tasks().iter().any(|change| {
                    change.identity() == summary
                        && change.status().is_some_and(|status| {
                            status.state() == TaskState::Failing
                                && status.version() == TaskStatusVersion::new(2).unwrap()
                                && status.installed()
                        })
                }));
                let Poll::Ready(Ok(NativeAttemptTerminal::Failed(failure))) =
                    futures::poll!(terminal_wait.as_mut())
                else {
                    panic!("strict source failure must settle before either sibling finishes");
                };
                assert_eq!(
                    failure.input(),
                    &ExecutionFailureInput::from_selected_task(&selected).unwrap()
                );
                assert_eq!(
                    failure.input().site(),
                    ExecutionFailureSite::Task {
                        identity: summary,
                        phase: TaskFailurePhase::Execution,
                    }
                );
            }
            TailCase::StrictCancellation => {
                assert_eq!(
                    requester
                        .request_with_outcome(CancellationReason::Requested)
                        .unwrap(),
                    WorkCancellationRequestOutcome::Requested
                );
                let reason = tokio::time::timeout(Duration::from_secs(2), cancellation.cancelled())
                    .await
                    .unwrap();
                assert_eq!(reason, CancellationReason::Requested);
                let expected = governed_cancellation_failure_input(reason.clone());
                let stop = apply_governed_stop(&mut tail, activation, reason);
                assert!(tail.drive(stop).is_none());
                let Poll::Ready(Ok(NativeAttemptTerminal::Failed(failure))) =
                    futures::poll!(terminal_wait.as_mut())
                else {
                    panic!("governed cancellation must settle before either sibling finishes");
                };
                assert_eq!(failure.input(), &expected);
                assert_eq!(failure.input().site(), ExecutionFailureSite::Coordinator);
                assert_eq!(
                    failure.input().fact(),
                    QueryFailureReason::Cancelled.failure_fact()
                );
                if let Some(delta) = facts.take_changes(activation, root, plan).unwrap() {
                    assert!(delta.root_success_sealed());
                    assert!(delta.selected_failure().is_none());
                }
            }
            TailCase::SealVsNativeFailure { .. } => {}
            TailCase::OrdinaryCompletion => {
                assert!(matches!(
                    futures::poll!(terminal_wait.as_mut()),
                    Poll::Ready(Ok(NativeAttemptTerminal::Completed))
                ));
                assert!(sealed.tasks().iter().all(|change| {
                    ![writer, summary].contains(&change.identity())
                        || change.status().is_some_and(|status| !status.is_terminal())
                }));
            }
        }

        finish(logical, drive).await;
    }
}
