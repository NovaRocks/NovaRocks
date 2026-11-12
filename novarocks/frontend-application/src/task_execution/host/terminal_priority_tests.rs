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

use super::{
    adapter::tests::start,
    custody::tests::WithdrawalInbox,
    observations::StreamKey,
    publication::OriginalHostFailure,
    subscriptions::{
        NativeSubscriptionFailure, NativeSubscriptionFailureCustody, SubscriptionFailureCustody,
        SubscriptionFailureRecord,
    },
};
use crate::{
    native::task_transport::wire::CoveredEventValidationError,
    query_execution::native_execution_adapter::LogicalAbortRouter,
    task_execution::bridge::{
        control::AttemptOwnerControls,
        terminal::{TerminalOutbox, TerminalPublication},
    },
};
use novarocks_attempt_protocol::{
    AttemptCore, AttemptStep, Core, InputSelection,
    capacity::{QueueStage, StepContext},
    config::DispatchLane,
    effect::EffectBody,
    error::{ParticipantObservationFailure, TaskExecutionError},
    failure::{AttemptFailure, AttemptFailureSite},
    input::{
        AttemptInput, AttemptInputClaim, AttemptInputId, AttemptSourceCut, AttemptSourceId,
        ExecutorFact, ObservationBatch, ObservationEvent, ObservationFact, ObservationFailure,
        ObservationFailureKind, ObservationOpening, ReceivedObservation,
    },
    start::{AttemptSources, AttemptStart},
};
use novarocks_control_runtime::transaction::RuntimeLedger;
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    identity::Generation,
    mailbox::InputClass,
    scope::{WorkAdmission, WorkKind},
};
use novarocks_coordination_contract::{
    AttemptActionId, AttemptActivationIdentity, AttemptFailureClass, AttemptRequestToken,
    AttemptTerminal, AttemptToLogical, AttemptToLogicalBody, ExecutionEffect, LogicalToAttemptBody,
    MonotonicInstant,
};
use novarocks_execution_contract::{
    SafeDetail, TaskFailure, TaskFailurePhase, TaskIdentity, TaskOutputFacts, TaskState,
    TaskStatus, TaskStatusVersion, TerminationDetail, failure::FailureProjection,
};
use novarocks_query_application::{
    api::{NativeAttemptPreparationFailure, NativeAttemptTerminal},
    coordination::{LogicalConclusion, LogicalExecutionActorConfig},
    execution_error::{ExecutionFailureInput, ExecutionFailureSite, QueryFailureReason},
    test_support::LogicalExecutionTestHarness,
};
use novarocks_types::{BackendProcessId, TaskId};
use novarocks_workload_control::{
    BusinessPermit, CancellationReason, CancellationView, ResourceConfig, Stage,
    WorkCancellationRequester, WorkClass, WorkRequest, WorkScope, WorkloadConfig, WorkloadControl,
};
use std::{
    num::{NonZeroU64, NonZeroUsize},
    time::{Duration, Instant},
};

fn id(value: u64) -> NonZeroU64 {
    NonZeroU64::new(value).unwrap()
}

struct QaAttempt {
    logical: LogicalExecutionTestHarness,
    scope: WorkScope,
    business: BusinessPermit,
    workload: WorkloadControl,
    cancellation: CancellationView,
    requester: WorkCancellationRequester,
}

impl QaAttempt {
    async fn activate(start: &mut AttemptStart) -> Self {
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
        let execution = start.plan.graph.root.query_execution_id();
        let participations: Vec<_> = start.plan.establish_material.keys().copied().collect();
        let config = LogicalExecutionActorConfig::single_attempt_completion(
            execution,
            ExecutionEffect::None,
            NonZeroUsize::new(8).unwrap(),
            participations.clone(),
            NonZeroUsize::MIN,
            NonZeroUsize::MIN,
            work.owner,
            stage,
        )
        .unwrap()
        .with_abort_participation_effect_port(LogicalAbortRouter::new(), NonZeroUsize::MIN);
        let mut logical = LogicalExecutionTestHarness::install(
            tokio::runtime::Handle::current(),
            config,
            execution,
            participations,
        )
        .unwrap();
        start.activation.identity = logical.activate_initial().await.unwrap();
        let drive = logical.native_attempt_drive();
        start.plan.authorization_limits = drive.authorization_limits();
        drop(drive);
        Self {
            logical,
            scope,
            business: work.business,
            workload,
            cancellation,
            requester,
        }
    }

    async fn finish(self) {
        let Self {
            mut logical,
            scope,
            business,
            workload,
            cancellation,
            requester,
        } = self;
        drop(requester);
        drop(cancellation);
        business.release();
        logical
            .finish_until(Instant::now() + Duration::from_secs(2))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), scope.wait_released())
            .await
            .unwrap();
        workload.close_admission();
        assert!(workload.shutdown().is_ok());
    }
}

struct Units(usize);

impl StepContext for Units {
    fn charge(&mut self, units: usize) -> bool {
        let Some(remaining) = self.0.checked_sub(units) else {
            return false;
        };
        self.0 = remaining;
        true
    }

    fn reserve_queue_stage(
        &mut self,
        _: DispatchLane,
        _: bool,
        _: BackendProcessId,
        _: usize,
        _: usize,
    ) -> QueueStage {
        panic!("this fixture does not issue Native operations")
    }
}

type TerminalPort = AttemptToLogical<AttemptFailure>;

struct ReceiverCut {
    core: Core,
    sources: AttemptSources,
    now: MonotonicInstant,
    next: u64,
    budget: usize,
}

impl ReceiverCut {
    fn new(start: AttemptStart, budget: usize) -> Self {
        let sources = start.sources;
        let now = start.initial_now;
        let mut cut = Self {
            core: <Core as AttemptCore>::new(start).unwrap(),
            sources,
            now,
            next: 100,
            budget,
        };
        let boot = cut.input(sources.actor, AttemptInput::Bootstrap, None);
        assert_eq!(boot.selection, InputSelection::Prefer(sources.native));
        cut
    }

    fn input(
        &mut self,
        source: AttemptSourceId,
        input: AttemptInput,
        original: Option<AttemptInputId>,
    ) -> AttemptStep {
        self.next += 1;
        let input_id = original.unwrap_or(AttemptInputId(id(self.next)));
        self.core
            .handle(
                self.now,
                AttemptInputClaim {
                    source,
                    input: input_id,
                    native_cut: Some(AttemptSourceCut {
                        source: self.sources.native,
                        tail: None,
                    }),
                    selected_cut: Some(AttemptSourceCut {
                        source: self.sources.observation,
                        tail: (source == self.sources.observation).then_some(input_id),
                    }),
                },
                input,
                &mut Units(self.budget),
            )
            .unwrap()
    }

    fn finish_terminal(
        &mut self,
        mut step: AttemptStep,
        expected_round: &AttemptFailure,
    ) -> TerminalPort {
        let mut round_seen = false;
        for _ in 0..32 {
            if let Some(round) = step.round_result {
                assert!(!round_seen, "one result for the original receiver round");
                assert_eq!(round, Err(expected_round.clone()));
                round_seen = true;
            }
            if let Some(effect) = step.effect {
                match effect.body {
                    EffectBody::Port(port) => {
                        assert!(round_seen);
                        assert!(matches!(
                            port.body,
                            AttemptToLogicalBody::AttemptTerminal(AttemptTerminal::Failed(_))
                        ));
                        return port;
                    }
                    EffectBody::ReleaseCreationPayload { .. } => {}
                    other => panic!("unexpected effect at receiver terminal: {other:?}"),
                }
            }
            assert_eq!(step.selection, InputSelection::Only(self.sources.actor));
            step = self.input(self.sources.actor, AttemptInput::Timer, None);
        }
        panic!("the original receiver cut did not publish its terminal");
    }
}

fn task_failure(identity: TaskIdentity, detail: &str) -> TaskStatus {
    TaskStatus::try_new_with_installed(
        identity,
        TaskStatusVersion::new(2).unwrap(),
        TaskState::Failing,
        Some(TerminationDetail::Failed(TaskFailure::new_in_phase(
            QueryFailureReason::PreparationFailed.failure_fact(),
            SafeDetail::truncating(detail),
            TaskFailurePhase::Preparation,
        ))),
        TaskOutputFacts::new(false),
        false,
    )
    .unwrap()
}

fn status_event(status: TaskStatus) -> ObservationEvent {
    ObservationEvent::Fact {
        source_revision: None,
        fact: ObservationFact::Status(status),
    }
}

async fn original_projection(
    outbox: &TerminalOutbox,
    port: TerminalPort,
    retained: Option<&SubscriptionFailureRecord>,
) -> NativeAttemptPreparationFailure {
    let mut publication = TerminalPublication::new(port);
    if let Some(record) = retained {
        let reference = publication.host_reference().unwrap();
        assert_eq!(reference, record.reference());
        let original = record.take(reference).unwrap();
        publication.attach_host_failure(reference, OriginalHostFailure::Subscription(original));
        assert!(record.take(reference).is_none());
    } else {
        assert!(publication.host_reference().is_none());
    }
    assert!(outbox.publish(Box::new(publication)).is_ok());
    let NativeAttemptTerminal::Failed(original) =
        tokio::time::timeout(Duration::from_secs(2), outbox.terminal())
            .await
            .unwrap()
            .unwrap()
    else {
        panic!("the original Native terminal is a failure");
    };
    assert_eq!(
        outbox.terminal().await.unwrap(),
        NativeAttemptTerminal::Failed(original.clone())
    );
    original
}

#[derive(Clone, Copy)]
enum CompetingObservation {
    OrdinaryLoss {
        connection_loss: bool,
        observation_first: bool,
    },
    ProcessMismatch,
    UnknownCoveredTask {
        cancel_while_receiving: bool,
    },
}

async fn competing_observation(case: CompetingObservation) {
    let mut start = start();
    start.plan.coordination.status_subscription_error_budget = 1;
    let root = start.plan.graph.root;
    let participation = *start.plan.establish_material.keys().next().unwrap();
    let mut qa = QaAttempt::activate(&mut start).await;
    let activation = start.activation.identity;
    let outbox = TerminalOutbox::new(activation);
    let cancel_while_receiving = matches!(
        case,
        CompetingObservation::UnknownCoveredTask {
            cancel_while_receiving: true
        }
    );
    // Charge the primary input and the first original frame before yielding.
    let mut cut = ReceiverCut::new(start, if cancel_while_receiving { 2 } else { 128 });
    let open = AttemptActionId::new(id(1000));
    let generation = cut
        .core
        .open_subscription_for_test(
            participation,
            open,
            AttemptRequestToken::new(id(1000)),
            &mut Units(128),
        )
        .unwrap();
    let key = StreamKey {
        participation,
        generation,
    };
    let record = SubscriptionFailureRecord::new(open, key);
    let detail = match case {
        CompetingObservation::UnknownCoveredTask { .. } => "original Task before invalid frame",
        _ => "same-cut original Task failure",
    };
    let original_task = status_event(task_failure(root, detail));
    let foreign = TaskIdentity::new(
        root.query_execution_id(),
        root.stage_id(),
        TaskId::new(u32::MAX).unwrap(),
        root.backend_process_id(),
    );
    let (competing, round_error, observation_first) = match case {
        CompetingObservation::OrdinaryLoss {
            connection_loss,
            observation_first,
        } => {
            let (event, state) = if connection_loss {
                (
                    ObservationEvent::Failed(ObservationFailure {
                        kind: ObservationFailureKind::RecoverableTransport {
                            observed_connection_loss: true,
                        },
                        cause: record.retain_native_failure(
                            key,
                            NativeSubscriptionFailure::Read(tonic::Status::from_error(Box::new(
                                std::io::Error::from(std::io::ErrorKind::ConnectionReset),
                            ))),
                        ),
                    }),
                    ParticipantObservationFailure::ConnectionLost,
                )
            } else {
                (
                    ObservationEvent::Closed,
                    ParticipantObservationFailure::Transport("budget_exhausted"),
                )
            };
            (
                event,
                TaskExecutionError::ParticipantUnobservable {
                    backend: participation.backend_process_id(),
                    state,
                },
                observation_first,
            )
        }
        CompetingObservation::ProcessMismatch => {
            let failure = record.conflicting(key, CoveredEventValidationError::ProcessMismatch);
            assert_eq!(
                failure.cause.site,
                AttemptFailureSite::Participation(participation)
            );
            (
                ObservationEvent::Failed(failure),
                TaskExecutionError::NativeTransport(failure.cause),
                false,
            )
        }
        CompetingObservation::UnknownCoveredTask { .. } => (
            status_event(task_failure(foreign, detail)),
            TaskExecutionError::UnknownOperation,
            false,
        ),
    };
    let mut events = [original_task, competing];
    if observation_first {
        events.swap(0, 1);
    }
    let frames = events
        .into_iter()
        .map(|event| ReceivedObservation {
            participation,
            generation,
            received_at: cut.now,
            opening: ObservationOpening {
                started_at: cut.now,
                opened_at: Some(cut.now),
            },
            event,
        })
        .collect();
    let step = cut.input(
        cut.sources.observation,
        AttemptInput::Executor(ExecutorFact::Observations(
            ObservationBatch::new(frames).unwrap(),
        )),
        None,
    );
    let step = if cancel_while_receiving {
        assert_eq!(step.selection, InputSelection::Only(cut.sources.actor));
        assert!(step.effect.is_none() && step.round_result.is_none());
        assert!(cut.core.selected_task_failure_for_test().is_some());
        stop_during_original_receiver_pass(&qa, activation, &mut cut);
        cut.budget = 128;
        cut.input(cut.sources.actor, AttemptInput::Timer, None)
    } else {
        step
    };
    let round_failure = AttemptFailure::TaskProtocol(Box::new(round_error));
    let terminal = cut.finish_terminal(step, &round_failure);
    assert_eq!(terminal.activation, activation);
    let selected = cut.core.selected_task_failure_for_test().unwrap();
    let selected_input = ExecutionFailureInput::from_selected_task(selected).unwrap();
    assert_eq!(
        selected_input.fact(),
        QueryFailureReason::PreparationFailed.failure_fact()
    );
    assert_eq!(
        selected_input.site(),
        ExecutionFailureSite::Task {
            identity: root,
            phase: TaskFailurePhase::Preparation
        }
    );
    assert_eq!(selected_input.detail().as_str(), detail);

    let task_wins = matches!(case, CompetingObservation::OrdinaryLoss { .. });
    let expected_terminal = if task_wins {
        AttemptFailure::SelectedTask(selected.clone())
    } else {
        round_failure
    };
    assert!(matches!(
        &terminal.body,
        AttemptToLogicalBody::AttemptTerminal(AttemptTerminal::Failed(actual)) if actual == &expected_terminal
    ));
    let retained = matches!(case, CompetingObservation::ProcessMismatch).then_some(record.as_ref());
    let failure = original_projection(&outbox, terminal, retained).await;
    if task_wins {
        assert_eq!(failure.input(), &selected_input);
        assert_eq!(failure.class(), AttemptFailureClass::ExecutionFailure);
    } else {
        assert_eq!(
            failure.input().fact(),
            QueryFailureReason::PreparationProtocol.failure_fact()
        );
        assert_eq!(failure.input().site(), ExecutionFailureSite::Coordinator);
        assert_eq!(failure.class(), AttemptFailureClass::ContractViolation);
    }
    if matches!(
        case,
        CompetingObservation::OrdinaryLoss {
            connection_loss: true,
            ..
        }
    ) {
        assert!(!record.is_terminal_candidate());
        assert!(record.take(record.reference()).is_some());
    }
    if cancel_while_receiving {
        qa.logical.abandon_running_attempt();
    } else {
        assert_eq!(
            qa.logical
                .fail_original_running_attempt(failure.error().clone())
                .await
                .unwrap(),
            LogicalConclusion::Failed
        );
        assert_eq!(
            qa.logical.actor_snapshot().await.unwrap().conclusion,
            Some(LogicalConclusion::Failed)
        );
    }
    qa.finish().await;
}

#[tokio::test]
async fn original_task_failure_precedes_same_cut_ordinary_loss_in_both_orders() {
    for connection_loss in [false, true] {
        for observation_first in [false, true] {
            competing_observation(CompetingObservation::OrdinaryLoss {
                connection_loss,
                observation_first,
            })
            .await;
        }
    }
}

#[tokio::test]
async fn original_subscription_identity_error_precedes_same_cut_task_failure() {
    competing_observation(CompetingObservation::ProcessMismatch).await;
}

#[tokio::test]
async fn later_bad_covered_frame_wins_without_erasing_the_accepted_task_failure() {
    competing_observation(CompetingObservation::UnknownCoveredTask {
        cancel_while_receiving: false,
    })
    .await;
}

#[tokio::test]
async fn a_definitive_receiver_error_wins_with_cancellation_visible_in_the_same_pass() {
    competing_observation(CompetingObservation::UnknownCoveredTask {
        cancel_while_receiving: true,
    })
    .await;
}

fn stop_during_original_receiver_pass(
    qa: &QaAttempt,
    activation: AttemptActivationIdentity,
    cut: &mut ReceiverCut,
) {
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

    qa.requester.request(CancellationReason::Requested).unwrap();
    let reason = qa
        .cancellation
        .reason()
        .expect("original cancellation is visible");
    assert_eq!(reason, CancellationReason::Requested);
    let stop_id = controls.stop_for_cancellation(reason).unwrap().unwrap();
    let (input, stop) = inbox.try_commit_owner_control().unwrap();
    assert_eq!(input, stop_id);
    assert!(matches!(&stop, AttemptInput::Port(port)
        if port.activation == activation && matches!(port.body, LogicalToAttemptBody::Stop { .. })));
    let stopped = cut.input(
        cut.sources.actor,
        stop,
        Some(AttemptInputId(id(stop_id.get()))),
    );
    assert!(stopped.stop_producing && stopped.effect.is_none() && stopped.round_result.is_none());
    assert_eq!(stopped.selection, InputSelection::Only(cut.sources.actor));

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
}
