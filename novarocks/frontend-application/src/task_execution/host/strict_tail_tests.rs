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
    protocol_facts::{AttemptProtocolFactsPublisher, WriteProtocolTracker},
    tests::delivery_chain_graph,
};
use novarocks_attempt_protocol::{
    AttemptCore, AttemptStep, Core, InputSelection,
    capacity::{QueueStage, StepContext},
    config::DispatchLane,
    effect::EffectBody,
    input::{
        AttemptInput, AttemptInputClaim, AttemptInputId, AttemptSourceCut, AttemptSourceId,
        ExecutorFact, ObservationBatch, ObservationEvent, ObservationFact, ObservationOpening,
        PublicationApplied, ReceivedObservation,
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
    AttemptActionId, AttemptActivationIdentity, AttemptRequestToken, AttemptStopReason,
    AttemptTerminal, AttemptToLogicalBody, ExecutionEffect, LogicalToAttempt, LogicalToAttemptBody,
    MonotonicInstant, ResultParticipationWithdrawal, SelectedTaskTermination,
};
use novarocks_execution_contract::{
    AdmissionEpochCapability, ExchangeDeliveryRequirement, SafeDetail, TaskFailure,
    TaskFailurePhase, TaskIdentity, TaskOutputFacts, TaskState, TaskStatus, TaskStatusVersion,
    TerminationDetail, failure::FailureProjection,
};
use novarocks_physical_plan::PlanVersionId;
use novarocks_query_application::{
    api::{NativeAttemptTerminal, PlanSeal},
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
        panic!("this fixture has not started the Native transport")
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
                        tail: None,
                    }),
                    selected_cut: Some(AttemptSourceCut {
                        source: self.sources.observation,
                        tail: (source == self.sources.observation).then_some(input_id),
                    }),
                },
                input,
                &mut Units(128),
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

    // Deliver only the existing publication results and terminal bridge port.
    // Seal delivery is held by the test at its original AwaitSeal boundary.
    fn drive(&mut self, mut step: AttemptStep) -> Option<PendingSealReply> {
        for _ in 0..32 {
            if let Some(result) = step.round_result.as_ref() {
                assert_eq!(result, &Ok(()));
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
                    EffectBody::Port(port) => {
                        if let AttemptToLogicalBody::SealResult { token, result } = port.body {
                            assert_eq!(
                                step.selection,
                                InputSelection::Only(self.sources.publication)
                            );
                            return Some(PendingSealReply {
                                action: effect.id,
                                token,
                                withdrawals: result.expect("the exact Root success is sealed"),
                            });
                        }
                        assert!(matches!(
                            port.body,
                            AttemptToLogicalBody::AttemptTerminal(
                                AttemptTerminal::Completed | AttemptTerminal::Failed(_)
                            )
                        ));
                        assert!(
                            self.terminal
                                .publish(Box::new(TerminalPublication::new(port)))
                                .is_ok()
                        );
                    }
                    EffectBody::ReleaseCreationPayload { .. } => {}
                    other => panic!("unexpected Native work in the strict-tail fixture: {other:?}"),
                }
            }
            match step.selection {
                InputSelection::Only(source) if source == self.sources.actor => {
                    step = self.input(source, AttemptInput::Timer);
                }
                InputSelection::Only(source) if source == self.sources.observation => return None,
                InputSelection::Prefer(source) if source == self.sources.native => return None,
                selection => panic!("unexpected strict-tail continuation: {selection:?}"),
            }
        }
        panic!("the original strict-tail continuation did not finish");
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

async fn run_tail_cases(cases: impl IntoIterator<Item = TailCase>) {
    for case in cases {
        let strict = !matches!(case, TailCase::OrdinaryCompletion);
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
                        &mut Units(128),
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

        let mut seal = statuses.begin_success_seal_request().unwrap();
        let token = AttemptRequestToken::new(id(1000));
        let step = tail.input(
            sources.observation,
            AttemptInput::Port(LogicalToAttempt {
                activation,
                body: LogicalToAttemptBody::SealRequest { token },
            }),
        );
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
        assert!(tail.drive(applied).is_none());
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
    }
}
