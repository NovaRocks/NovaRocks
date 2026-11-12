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

use super::{adapter::tests::start, publication::PublicationRecord};
use crate::task_execution::{
    bridge::terminal::{TerminalOutbox, TerminalPublication},
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
use novarocks_coordination_contract::{
    AttemptActionId, AttemptRequestToken, AttemptTerminal, AttemptToLogicalBody, ExecutionEffect,
    LogicalToAttempt, LogicalToAttemptBody, MonotonicInstant, ResultParticipationWithdrawal,
};
use novarocks_execution_contract::{
    AdmissionEpochCapability, ExchangeDeliveryRequirement, TaskIdentity, TaskOutputFacts,
    TaskState, TaskStatus, TaskStatusVersion,
};
use novarocks_physical_plan::PlanVersionId;
use novarocks_query_application::{
    api::{NativeAttemptTerminal, PlanSeal},
    coordination::{
        AcceptedRootControlPort, AcceptedRootControlRequest, LogicalExecutionActorConfig,
        accepted_root_status_projection_with_control_port,
    },
    test_support::LogicalExecutionTestHarness,
};
use novarocks_result_contract::{PerKind, SummaryKind};
use novarocks_types::BackendProcessId;
use novarocks_workload_control::{
    ResourceConfig, Stage, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
};
use std::{
    collections::BTreeSet,
    num::{NonZeroU64, NonZeroUsize},
    sync::{Arc, Mutex},
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
                            AttemptToLogicalBody::AttemptTerminal(AttemptTerminal::Completed)
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

#[tokio::test]
async fn strict_tail_keeps_native_completion_behind_both_original_task_terminals() {
    for writer_first in [true, false] {
        let graph = delivery_chain_graph(ExchangeDeliveryRequirement::MustDrain);
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
        let writers = BTreeSet::from([writer]);
        let summaries = PerKind {
            write_counts: Some(BTreeSet::from([summary])),
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
        let mut tracker = WriteProtocolTracker::from_initial(&initial).unwrap();
        assert!(tracker.execution_verdict().is_pending());

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
        assert!(reply.withdrawals.is_empty());
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
        tracker.observe(&sealed).unwrap();
        assert!(tracker.execution_verdict().is_pending());
        assert!(futures::poll!(terminal_wait.as_mut()).is_pending());

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
