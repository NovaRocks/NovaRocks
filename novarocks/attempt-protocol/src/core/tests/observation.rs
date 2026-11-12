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
    execution::{
        CreateGrantProgress,
        ack_phase::{admission_intent, dispatch_admission, queue_establish},
    },
    input::{
        AckPayload, AttemptInput, AttemptInputClaim, DiagnosticRead, OperationReply,
        OperationReplyResult, QueuedInputGrantRef, SendGeneration,
    },
};
use novarocks_coordination_contract::{
    AttemptComputationCut, SelectedTaskTermination, StatusObservation,
};
use novarocks_execution_contract::{
    CancelReason, CreateTaskReceipt, ExchangeDeliveryRequirement, LeaseReceipt, OperationKind,
    OperationOutcome, ParticipationReceipt, ParticipationState, SafeDetail, TaskConvergenceReceipt,
    TaskConvergenceVersion, TaskFailure, TaskFailurePhase, TaskResourceFacts,
    failure::{FailureClass, FailureFact, FailureReason},
    operation::OperationResult,
    root_result::RootResultEnd,
};
use std::num::NonZeroU16;

struct OriginalSend(BackendProcessId, DispatchLane);

impl StepContext for OriginalSend {
    fn charge(&mut self, _: usize) -> bool {
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
            (backend, lane, control, records),
            (self.0, self.1, false, 1)
        );
        QueueStage::Granted(ReservationToken(id(1)))
    }
}

struct TerminalTrace {
    core: Core,
    next: u64,
}

impl TerminalTrace {
    fn new() -> Self {
        Self {
            core: Core::new(start()).unwrap(),
            next: 100,
        }
    }

    fn next_id(&mut self) -> NonZeroU64 {
        self.next += 1;
        id(self.next)
    }

    fn dispatch(
        &mut self,
        operation: &OperationIntent,
        grant: QueuedInputGrantRef,
        backend: BackendProcessId,
        lane: DispatchLane,
    ) {
        let effect = AttemptActionId::new(self.next_id());
        let BatchDispatch::Ready(AttemptEffect {
            body: EffectBody::SendBatch { batch, .. },
            ..
        }) = self
            .core
            .execution
            .advance_send_batch(
                &mut self.core.dispatch,
                effect,
                &mut OriginalSend(backend, lane),
                |_| Ok(()),
            )
            .unwrap()
        else {
            panic!("expected original {lane:?} dispatch");
        };
        assert_eq!(batch.operations.len(), 1);
        assert_eq!(batch.operations[0].operation_id(), operation.operation_id());
        assert_eq!(batch.queued_inputs.as_ref(), [grant]);
        self.core.execution.observe_send_started(effect);
    }

    fn start_create(&mut self, identity: TaskIdentity) -> (TaskOperationId, SendGeneration) {
        let participation = self
            .core
            .execution
            .graph()
            .task(identity.task_id())
            .unwrap()
            .participation();
        let backend = participation.backend_process_id();
        let record = self.next_id();
        let request = admission_intent(&mut self.core.execution, participation);
        let ticket = dispatch_admission(
            &mut self.core.execution,
            request,
            record.get(),
            OperationOutcome::Accepted,
        );
        self.core
            .execution
            .apply_dispatched_reply(&ticket)
            .1
            .unwrap();

        let record = self.next_id();
        let establish_grant =
            QueuedInputGrantRef::new(record, SendGeneration::new(AttemptActionId::new(record)));
        let establish = queue_establish(&mut self.core.execution, participation, establish_grant);
        self.dispatch(
            &establish,
            establish_grant,
            backend,
            DispatchLane::Lifecycle,
        );
        self.core
            .execution
            .establish_send_started(participation, establish.operation_id())
            .unwrap();

        let OperationIntent::EstablishParticipation {
            initial_lease_sequence,
            initial_lease_valid_for,
            ..
        } = &establish
        else {
            panic!("expected original Establish");
        };
        let active = ParticipationReceipt::new(participation, ParticipationState::Active)
            .with_lease(LeaseReceipt::new(
                *initial_lease_sequence,
                *initial_lease_valid_for,
                Duration::from_secs(30),
            ));
        let reply = OperationObservation {
            generation: establish_grant.generation(),
            reply: OperationReply {
                operation_id: establish.operation_id(),
                kind: establish.kind(),
                result: OperationReplyResult::Worker(OperationResult::Verdict {
                    outcome: OperationOutcome::Accepted,
                    detail: None,
                    acknowledgement: Some(AckPayload::Participation(active)),
                }),
            },
        };
        self.core
            .execution
            .apply_dispatched_reply(&reply)
            .1
            .unwrap();

        let record = self.next_id();
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
            .begin_create_admission(identity, AttemptActionId::new(record))
            .unwrap()
        else {
            panic!("expected original Create admission");
        };
        let grant = QueuedInputGrantRef::new(record, generation);
        assert!(matches!(
            self.core
                .execution
                .observe_create_grant(operation.operation_id(), grant)
                .unwrap(),
            CreateGrantProgress::Queued
        ));
        self.dispatch(&operation, grant, backend, DispatchLane::Create);
        (operation.operation_id(), generation)
    }

    fn handle_input(
        &mut self,
        now: MonotonicInstant,
        source: AttemptSourceId,
        input: AttemptInput,
    ) -> AttemptStep {
        let id = AttemptInputId(self.next_id());
        let sources = self.core.sources;
        self.core
            .handle(
                now,
                AttemptInputClaim {
                    source,
                    input: id,
                    native_cut: Some(AttemptSourceCut {
                        source: sources.native,
                        tail: (source == sources.native).then_some(id),
                    }),
                    selected_cut: Some(AttemptSourceCut {
                        source: sources.observation,
                        tail: (source == sources.observation).then_some(id),
                    }),
                },
                input,
                &mut Units(128),
            )
            .unwrap()
    }

    fn finish_observation_round(
        &mut self,
        now: MonotonicInstant,
        mut pending: AttemptStep,
    ) -> (Result<(), AttemptFailure>, Vec<AttemptEffect>) {
        use crate::effect::Publication;
        let sources = self.core.sources;
        let mut effects = Vec::new();
        for _ in 0..32 {
            let applied = pending.effect.take().and_then(|effect| {
                let applied = match &effect.body {
                    EffectBody::Publish(Publication::ProtocolBatch(_)) => {
                        Some(PublicationApplied::ProtocolBatch { published: true })
                    }
                    EffectBody::Publish(Publication::RootStatus(_)) => {
                        Some(PublicationApplied::RootStatus)
                    }
                    _ => None,
                };
                let id = effect.id;
                effects.push(effect);
                applied.map(|applied| (id, applied))
            });
            if let Some(result) = pending.round_result {
                return (result, effects);
            }
            let (source, input) = match (pending.selection, applied) {
                (InputSelection::Only(source), Some((effect, applied)))
                    if source == sources.publication =>
                {
                    (
                        source,
                        AttemptInput::Executor(ExecutorFact::PublicationApplied {
                            effect,
                            result: Ok(applied),
                        }),
                    )
                }
                (InputSelection::Only(source), None)
                    if source == sources.actor || source == sources.observation =>
                {
                    (sources.actor, AttemptInput::Timer)
                }
                selection => panic!("unexpected observation continuation: {selection:?}"),
            };
            pending = self.handle_input(now, source, input);
        }
        panic!("the original observation round did not finish");
    }

    fn status(&self, task: TaskIdentity) -> Option<&TaskStatus> {
        self.core.execution.task(task.task_id()).unwrap().status()
    }

    fn apply(
        &mut self,
        receipt: Option<OperationObservation>,
        observation: Option<ReceivedObservation>,
    ) -> Result<(), AttemptFailure> {
        let input = AttemptInputId(self.next_id());
        let now = self.core.execution.monotonic_now();
        let source = self.core.sources.native;
        self.core
            .begin_native_prefix(
                now,
                AttemptSourceCut {
                    source,
                    tail: receipt.as_ref().map(|_| input),
                },
            )
            .unwrap();
        if let Some(receipt) = receipt {
            assert!(
                self.core
                    .apply_native(
                        now,
                        source,
                        input,
                        &ExecutorFact::Operation(receipt),
                        |_, _| { panic!("a Task Create never invokes the actor Abort observer") }
                    )
                    .unwrap()
                    .is_none()
            );
        }
        if let Some(observation) = observation {
            assert!(
                self.core
                    .apply_observations(
                        now,
                        self.core.sources.observation,
                        ObservationBatch::new(vec![observation]).unwrap()
                    )
                    .unwrap()
                    .is_none()
            );
        }
        self.core.begin_ack_settlement(None).unwrap();
        match self
            .core
            .advance_ack_settlement(
                AttemptRequestToken::new(input.0),
                AttemptActionId::new(input.0),
                &mut Units(1),
            )
            .unwrap()
        {
            AckSettlementProgress::Complete(result) => result,
            other => panic!("expected a settled Task observation cut, got {other:?}"),
        }
    }

    fn apply_snapshot(
        &mut self,
        stream: &ReceivedObservation,
        status: &TaskStatus,
    ) -> Result<(), AttemptFailure> {
        let mut observation = stream.clone();
        let ObservationEvent::Fact {
            fact: ObservationFact::Status(snapshot),
            ..
        } = &mut observation.event
        else {
            panic!("expected the original Task status stream");
        };
        assert_eq!(snapshot.identity(), status.identity());
        *snapshot = status.clone();
        self.apply(None, Some(observation))
    }

    fn open_diagnostic_window(&mut self) {
        let now = self.core.execution.monotonic_now();
        let deadline = now.saturating_add(Duration::from_secs(1));
        let ready = AttemptComputationCut {
            activation: self.core.activation,
            root: self.core.execution.graph().root_identity(),
            cut: now,
            diagnostic_deadline: deadline,
            convergence_deadline: now.saturating_add(Duration::from_secs(10)),
            end: RootResultEnd {
                sequence: id(1),
                output_rows: 1,
            },
        };
        self.core
            .execution
            .with_diagnostics(|collector, _| collector.begin(ready))
            .unwrap();
        assert!(!self.core.execution.diagnostics().read_ready());
    }

    fn expect_diagnostic_reads(&mut self, expected: &[TaskIdentity]) {
        for &task in expected {
            let request = AttemptRequestToken::new(self.next_id());
            self.core.execution.with_diagnostics(|collector, _| {
                let read = collector
                    .next_read(request, &mut Units(1))
                    .expect("one original terminal candidate");
                assert_eq!(read.task, task);
                assert_eq!(read.request, request);
                assert!(
                    collector
                        .result(
                            request,
                            task,
                            DiagnosticRead::Unavailable(OperationOutcome::Gone)
                        )
                        .unwrap()
                        .is_none()
                );
                collector.actual_exit(request).unwrap();
            });
        }
        assert!(!self.core.execution.diagnostics().read_ready());
    }
}

fn create_reply(
    (operation_id, generation): (TaskOperationId, SendGeneration),
    status: TaskStatus,
) -> OperationObservation {
    OperationObservation {
        generation,
        reply: OperationReply {
            operation_id,
            kind: OperationKind::CreateTask,
            result: OperationReplyResult::Worker(OperationResult::Verdict {
                outcome: OperationOutcome::Accepted,
                detail: None,
                acknowledgement: Some(AckPayload::Create(Box::new(CreateTaskReceipt::new(
                    status.identity(),
                    Vec::new(),
                    status,
                )))),
            }),
        },
    }
}

fn status_frame(trace: &mut TerminalTrace, status: TaskStatus) -> ReceivedObservation {
    let participation = trace
        .core
        .execution
        .graph()
        .task(status.identity().task_id())
        .unwrap()
        .participation();
    let now = trace.core.execution.monotonic_now();
    trace
        .core
        .subscriptions
        .activate(now, participation)
        .unwrap();
    let next = trace.next_id();
    let SubscriptionStep::Effect(AttemptEffect {
        body: EffectBody::OpenSubscription(request),
        ..
    }) = trace
        .core
        .subscriptions
        .advance(
            &trace.core.execution,
            now,
            AttemptActionId::new(next),
            AttemptRequestToken::new(next),
            &mut Units(10),
        )
        .unwrap()
    else {
        panic!("expected the original subscription opening");
    };
    assert_eq!(request.participation, participation);
    ReceivedObservation {
        participation,
        generation: request.generation,
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
}

fn originating_failure(detail: &str, phase: TaskFailurePhase) -> TerminationDetail {
    TerminationDetail::Failed(TaskFailure::new_in_phase(
        FailureFact::new(
            FailureClass::Data,
            FailureReason::new(
                NonZeroU16::new(u16::MAX).unwrap(),
                NonZeroU16::new(1).unwrap(),
            ),
        ),
        SafeDetail::new(detail).unwrap(),
        phase,
    ))
}

#[test]
fn first_terminal_create_reply_and_stream_duplicate_queue_one_diagnostic_read() {
    for preparation_failure in [false, true] {
        let mut trace = TerminalTrace::new();
        let task = trace
            .core
            .execution
            .graph()
            .tasks()
            .last()
            .unwrap()
            .identity();
        let create = trace.start_create(task);
        let (state, cause, installed) = if preparation_failure {
            (
                TaskState::Failed,
                Some(originating_failure(
                    "original preparation failure",
                    TaskFailurePhase::Preparation,
                )),
                false,
            )
        } else {
            (TaskState::Finished, None, true)
        };
        let output = TaskOutputFacts::new(!preparation_failure);
        let terminal = TaskStatus::try_new_with_installed(
            task,
            TaskStatusVersion::FIRST,
            state,
            cause.clone(),
            output,
            installed,
        )
        .unwrap();
        let mut observation = status_frame(&mut trace, terminal.clone());
        let receipt = create_reply(create, terminal.clone());
        let prior = trace.core.execution.accepted_protocol_revision();
        trace.open_diagnostic_window();

        trace.apply(Some(receipt.clone()), None).unwrap();
        assert!(trace.core.execution.diagnostics().read_ready());
        let revision = trace.core.execution.accepted_protocol_revision();
        assert!(revision > prior);
        assert_eq!(trace.status(task), Some(&terminal));

        // The current Core consumes physical generations. A repeated result
        // for the retired send is stale and cannot act as a second author.
        trace.apply(Some(receipt), None).unwrap();
        trace.apply(None, Some(observation.clone())).unwrap();
        assert_eq!(trace.core.execution.accepted_protocol_revision(), revision);

        let changed_metrics = TaskStatus::try_new_with_installed(
            task,
            TaskStatusVersion::new(2).unwrap(),
            state,
            cause,
            output,
            installed,
        )
        .unwrap()
        .with_resources(TaskResourceFacts::empty().with_cpu_time(Duration::from_millis(7)));
        observation.event = ObservationEvent::Fact {
            source_revision: None,
            fact: ObservationFact::Status(changed_metrics),
        };
        assert!(matches!(
            trace.apply(None, Some(observation)),
            Err(AttemptFailure::TaskProtocol(error))
                if matches!(*error, TaskExecutionError::Observation(StatusObservation::TerminalOverwrite))
        ));
        assert_eq!(trace.core.execution.accepted_protocol_revision(), revision);
        assert_eq!(trace.status(task), Some(&terminal));
        trace.expect_diagnostic_reads(&[task]);
    }
}

#[test]
fn fatal_current_subscription_fails_the_attempt_with_its_original_participation() {
    use crate::{
        failure::{AttemptFailureSite, HostFailureKind, HostFailureRef, HostProjectedFailure},
        input::{ObservationFailure, ObservationFailureKind},
    };
    use novarocks_coordination_contract::{
        AttemptFailureClass, AttemptTerminal, AttemptToLogicalBody,
    };

    let mut trace = TerminalTrace::new();
    let now = trace.core.execution.monotonic_now();
    let sources = trace.core.sources;
    let root = trace.core.execution.graph().root_identity();
    let peer = trace
        .core
        .execution
        .graph()
        .tasks()
        .map(|task| task.identity())
        .find(|task| task.backend_process_id() != root.backend_process_id())
        .expect("the original non-root backend");
    let participation = trace
        .core
        .execution
        .graph()
        .participation_on_backend(peer.backend_process_id())
        .unwrap();
    let create = trace.start_create(peer);
    let installed = TaskStatus::created(peer).with_installed();
    trace
        .apply(Some(create_reply(create, installed.clone())), None)
        .unwrap();
    assert!(
        trace
            .core
            .execution
            .owner(participation)
            .unwrap()
            .establish_acknowledged()
    );

    // Keep the original installed participation and its real subscription
    // controller, while driving each subsequent input through the Core.
    trace.handle_input(now, sources.actor, AttemptInput::Bootstrap);
    trace
        .core
        .subscriptions
        .activate(now, participation)
        .unwrap();
    let open = |trace: &mut TerminalTrace, at| {
        let step = trace.handle_input(at, sources.actor, AttemptInput::Timer);
        let (result, effects) = trace.finish_observation_round(at, step);
        assert_eq!(result, Ok(()));
        let mut opens = effects.into_iter().filter_map(|effect| {
            let EffectBody::OpenSubscription(request) = effect.body else {
                return None;
            };
            assert_eq!(request.participation, participation);
            Some((effect.id, request.generation))
        });
        let original = opens.next().expect("one original Open effect");
        assert!(opens.next().is_none());
        original
    };
    let observe = |trace: &mut TerminalTrace, at, frame| {
        let input = AttemptInput::Executor(ExecutorFact::Observations(
            ObservationBatch::new(vec![frame]).unwrap(),
        ));
        let step = trace.handle_input(at, sources.observation, input);
        trace.finish_observation_round(at, step)
    };
    let failure = |open: AttemptActionId, kind, class| ObservationFailure {
        kind,
        cause: HostProjectedFailure {
            original: HostFailureRef(id(open.get())),
            kind: HostFailureKind::NativeTransport,
            class,
            site: AttemptFailureSite::Participation(participation),
        },
    };
    let frame = |at, opened_at, generation, failure| ReceivedObservation {
        participation,
        generation,
        received_at: at,
        opening: ObservationOpening {
            started_at: opened_at,
            opened_at: Some(opened_at),
        },
        event: ObservationEvent::Failed(failure),
    };

    let (original_open, original_generation) = open(&mut trace, now);
    let recoverable = failure(
        original_open,
        ObservationFailureKind::RecoverableTransport {
            observed_connection_loss: true,
        },
        AttemptFailureClass::ExecutionFailure,
    );
    let (result, effects) = observe(
        &mut trace,
        now,
        frame(now, now, original_generation, recoverable),
    );
    assert_eq!(result, Ok(()));
    assert!(trace.core.terminal.terminal().is_none());
    assert!(effects.iter().any(|effect| matches!(
        &effect.body,
        EffectBody::CloseSubscription { participation: original, generation }
            if *original == participation && *generation == original_generation
    )));
    assert!(trace.core.next_subscription_deadline().is_none());

    let exited = trace.handle_input(
        now,
        sources.subscription,
        AttemptInput::Executor(ExecutorFact::SubscriptionExited {
            effect: original_open,
            participation,
            generation: original_generation,
        }),
    );
    assert_eq!(trace.finish_observation_round(now, exited).0, Ok(()));
    let reopen_at = trace
        .core
        .next_subscription_deadline()
        .expect("the original retry deadline");
    assert!(reopen_at > now);
    let (current_open, current_generation) = open(&mut trace, reopen_at);
    assert_eq!(current_generation.get(), original_generation.get() + 1);
    assert_ne!(current_open, original_open);

    // A late copy from the retired generation cannot decide this attempt.
    assert_eq!(
        observe(
            &mut trace,
            reopen_at,
            frame(reopen_at, now, original_generation, recoverable)
        )
        .0,
        Ok(())
    );
    assert!(trace.core.terminal.terminal().is_none());

    let fatal = failure(
        current_open,
        ObservationFailureKind::ProcessMismatch,
        AttemptFailureClass::ContractViolation,
    );
    let expected =
        AttemptFailure::TaskProtocol(Box::new(TaskExecutionError::NativeTransport(fatal.cause)));
    let (result, mut effects) = observe(
        &mut trace,
        reopen_at,
        frame(reopen_at, reopen_at, current_generation, fatal),
    );
    assert_eq!(result, Err(expected.clone()));
    let terminal = AttemptTerminal::Failed(expected);
    for _ in 0..4 {
        if trace.core.terminal.terminal().is_some() {
            break;
        }
        let next = trace.handle_input(reopen_at, sources.actor, AttemptInput::Timer);
        effects.extend(next.effect);
    }
    assert_eq!(trace.core.terminal.terminal(), Some(&terminal));
    assert_eq!(
        effects
            .iter()
            .filter(|effect| matches!(
                &effect.body,
                EffectBody::Port(port)
                    if port.activation == trace.core.activation
                        && matches!(&port.body, AttemptToLogicalBody::AttemptTerminal(original)
                            if original == &terminal)
            ))
            .count(),
        1
    );
    assert!(effects.iter().all(|effect| !matches!(
        &effect.body,
        EffectBody::SendBatch { .. } | EffectBody::ObserveProcess { .. }
    )));
    assert_eq!(trace.status(peer), Some(&installed));
    assert!(!trace.core.execution.task_actual_stopped(peer));
    assert!(
        trace
            .core
            .execution
            .participation_convergence_receipt(participation)
            .is_none()
    );
}

#[test]
fn covered_exhaustion_follows_required_unproven_task_and_exact_terminal_control() {
    use crate::{
        error::ParticipantObservationFailure,
        failure::{AttemptFailureSite, HostFailureKind, HostFailureRef, HostProjectedFailure},
        input::{ObservationFailure, ObservationFailureKind},
    };
    use novarocks_coordination_contract::{
        AttemptFailureClass, AttemptTerminal, AttemptToLogicalBody,
    };

    for connection_lost in [false, true] {
        for stood_down in [false, true] {
            let mut start = start();
            start.plan.coordination.status_subscription_error_budget = 1;
            for edge in start.plan.graph.edges.values_mut() {
                edge.delivery = ExchangeDeliveryRequirement::DemandDriven;
            }
            let now = start.initial_now;
            let sources = start.sources;
            let activation = start.activation.identity;
            let mut trace = TerminalTrace {
                core: Core::new(start).unwrap(),
                next: 100,
            };
            let root = trace.core.execution.graph().root_identity();
            let leaf = trace
                .core
                .execution
                .graph()
                .tasks()
                .last()
                .unwrap()
                .identity();
            assert_ne!(leaf.backend_process_id(), root.backend_process_id());
            let participation = trace
                .core
                .execution
                .graph()
                .participation_on_backend(leaf.backend_process_id())
                .unwrap();
            let root_participation = trace
                .core
                .execution
                .graph()
                .participation_on_backend(root.backend_process_id())
                .unwrap();
            trace.start_create(leaf);
            if stood_down {
                trace.core.execution.stand_down_task_normally(leaf).unwrap();
                trace.core.execution.stand_down_task_normally(leaf).unwrap();
            }
            let task = trace.core.execution.task(leaf.task_id()).unwrap();
            assert!(!task.create_ownership_proven());
            assert!(!task.is_terminal());
            assert_eq!(
                trace
                    .core
                    .execution
                    .requires_task_observation(participation),
                !stood_down
            );
            assert!(
                trace
                    .core
                    .execution
                    .requires_task_observation(root_participation)
            );

            trace.handle_input(now, sources.actor, AttemptInput::Bootstrap);
            trace
                .core
                .subscriptions
                .activate(now, participation)
                .unwrap();
            let step = trace.handle_input(now, sources.actor, AttemptInput::Timer);
            let (round, effects) = trace.finish_observation_round(now, step);
            assert_eq!(round, Ok(()));
            let (open, generation) = effects
                .into_iter()
                .find_map(|effect| {
                    let EffectBody::OpenSubscription(request) = effect.body else {
                        return None;
                    };
                    assert_eq!(request.participation, participation);
                    Some((effect.id, request.generation))
                })
                .expect("the original leaf subscription opens");

            let (event, state) = if connection_lost {
                (
                    ObservationEvent::Failed(ObservationFailure {
                        kind: ObservationFailureKind::RecoverableTransport {
                            observed_connection_loss: true,
                        },
                        cause: HostProjectedFailure {
                            original: HostFailureRef(id(open.get())),
                            kind: HostFailureKind::NativeTransport,
                            class: AttemptFailureClass::ExecutionFailure,
                            site: AttemptFailureSite::Participation(participation),
                        },
                    }),
                    ParticipantObservationFailure::ConnectionLost,
                )
            } else {
                (
                    ObservationEvent::Closed,
                    ParticipantObservationFailure::Transport("budget_exhausted"),
                )
            };
            let input = AttemptInput::Executor(ExecutorFact::Observations(
                ObservationBatch::new(vec![ReceivedObservation {
                    participation,
                    generation,
                    received_at: now,
                    opening: ObservationOpening {
                        started_at: now,
                        opened_at: Some(now),
                    },
                    event,
                }])
                .unwrap(),
            ));
            let expected = AttemptFailure::TaskProtocol(Box::new(
                TaskExecutionError::ParticipantUnobservable {
                    backend: leaf.backend_process_id(),
                    state,
                },
            ));
            let step = trace.handle_input(now, sources.observation, input);
            let (mut round, mut effects) = trace.finish_observation_round(now, step);
            assert!(!trace.core.execution.covered_gap_pending(participation));
            if stood_down {
                assert_eq!(round, Ok(()));
                assert!(trace.core.terminal.terminal().is_none());
                assert!(effects.iter().any(|effect| matches!(
                    &effect.body,
                    EffectBody::CloseSubscription { participation: closed, generation: closing }
                        if *closed == participation && *closing == generation
                )));

                // The earlier exhausted row matters again if this exact
                // attempt now needs authoritative root terminal control.
                let token = AttemptRequestToken::new(trace.next_id());
                let demand = trace.handle_input(
                    now,
                    sources.observation,
                    AttemptInput::Port(LogicalToAttempt {
                        activation,
                        body: LogicalToAttemptBody::TerminalControlDemand { token, root },
                    }),
                );
                assert!(demand.root_control_processed.is_some());
                assert!(matches!(
                    demand.effect.as_ref(),
                    Some(AttemptEffect { body: EffectBody::Port(port), .. })
                        if port.activation == activation && matches!(
                            &port.body, AttemptToLogicalBody::TerminalControlResult {
                                token: original, result: Ok(()),
                            } if *original == token
                        )
                ));
                (round, effects) = trace.finish_observation_round(now, demand);
            }
            assert_eq!(round, Err(expected.clone()));
            let terminal = AttemptTerminal::Failed(expected);
            for _ in 0..4 {
                if trace.core.terminal.terminal().is_some() {
                    break;
                }
                let step = trace.handle_input(now, sources.actor, AttemptInput::Timer);
                effects.extend(step.effect);
            }
            assert_eq!(trace.core.terminal.terminal(), Some(&terminal));
            assert!(effects.iter().any(|effect| matches!(
                &effect.body,
                EffectBody::Port(port)
                    if port.activation == activation && matches!(
                        &port.body, AttemptToLogicalBody::AttemptTerminal(original)
                            if original == &terminal
                    )
            )));
            let task = trace.core.execution.task(leaf.task_id()).unwrap();
            assert!(!task.create_ownership_proven());
            assert!(!task.is_terminal());
            assert!(!trace.core.execution.task_actual_stopped(leaf));
            assert!(
                trace
                    .core
                    .execution
                    .participation_convergence_receipt(participation)
                    .is_none()
            );
        }
    }
}

#[test]
fn stream_first_terminal_order_survives_duplicates_and_late_create_reply() {
    let mut trace = TerminalTrace::new();
    let mut order: Vec<_> = trace
        .core
        .execution
        .graph()
        .tasks()
        .map(|task| task.identity())
        .collect();
    order.reverse();
    assert!(order[0].task_id() > order[1].task_id());
    let creates: Vec<_> = order.iter().map(|&task| trace.start_create(task)).collect();
    for (&task, &create) in order.iter().zip(&creates).skip(1) {
        trace
            .apply(
                Some(create_reply(
                    create,
                    TaskStatus::created(task).with_installed(),
                )),
                None,
            )
            .unwrap();
    }
    trace.open_diagnostic_window();
    let stream_first = order[0];
    for (&task, &create) in order.iter().zip(&creates) {
        let terminal = TaskStatus::try_new(
            task,
            TaskStatusVersion::new(3).unwrap(),
            TaskState::Finished,
            None,
            TaskOutputFacts::new(true),
        )
        .unwrap();
        let observation = status_frame(&mut trace, terminal.clone());
        let prior = trace.core.execution.accepted_protocol_revision();
        trace.apply(None, Some(observation.clone())).unwrap();
        let revision = trace.core.execution.accepted_protocol_revision();
        assert_eq!(revision, prior + 1);
        trace.apply(None, Some(observation)).unwrap();
        assert_eq!(trace.core.execution.accepted_protocol_revision(), revision);

        if task == stream_first {
            assert!(
                trace
                    .core
                    .execution
                    .task(task.task_id())
                    .unwrap()
                    .has_released_operation()
            );
            let reply = create_reply(create, TaskStatus::created(task).with_installed());
            trace.apply(Some(reply.clone()), None).unwrap();
            assert!(
                !trace
                    .core
                    .execution
                    .task(task.task_id())
                    .unwrap()
                    .has_released_operation()
            );
            trace.apply(Some(reply), None).unwrap();
            assert_eq!(trace.core.execution.accepted_protocol_revision(), revision);
            assert_eq!(trace.status(task), Some(&terminal));
        }
    }
    let revision = trace.core.execution.accepted_protocol_revision();
    trace.apply(None, None).unwrap();
    assert_eq!(trace.core.execution.accepted_protocol_revision(), revision);
    trace.expect_diagnostic_reads(&order);
}

#[test]
fn live_metrics_and_derived_refinement_queue_only_the_first_terminal() {
    let mut trace = TerminalTrace::new();
    let task = trace
        .core
        .execution
        .graph()
        .tasks()
        .last()
        .unwrap()
        .identity();
    let derived = TerminationDetail::Aborted(AbortCause::PeerTaskFailed);
    let live_status = |version| {
        TaskStatus::try_new(
            task,
            TaskStatusVersion::new(version).unwrap(),
            TaskState::Aborting,
            Some(derived.clone()),
            TaskOutputFacts::default(),
        )
        .unwrap()
        .with_resources(TaskResourceFacts::empty().with_cpu_time(Duration::from_millis(version)))
    };
    let stream = status_frame(&mut trace, live_status(2));
    trace.open_diagnostic_window();
    for version in [2, 3] {
        let live = live_status(version);
        let revision = trace.core.execution.accepted_protocol_revision();
        trace.apply_snapshot(&stream, &live).unwrap();
        assert_eq!(trace.status(task), Some(&live));
        assert!(trace.core.execution.accepted_protocol_revision() > revision);
        assert!(!trace.core.execution.diagnostics().read_ready());
    }
    assert_eq!(
        trace.core.execution.selected_task_failure(),
        Some(&SelectedTaskTermination::new(task, derived)),
    );

    let failure = originating_failure("original runtime failure", TaskFailurePhase::Execution);
    let terminal = TaskStatus::try_new(
        task,
        TaskStatusVersion::new(9).unwrap(),
        TaskState::Failed,
        Some(failure.clone()),
        TaskOutputFacts::default(),
    )
    .unwrap();
    trace.apply_snapshot(&stream, &terminal).unwrap();
    assert_eq!(trace.status(task), Some(&terminal));
    assert!(trace.core.execution.diagnostics().read_ready());
    let selected = SelectedTaskTermination::new(task, failure);
    assert_eq!(
        trace.core.execution.selected_task_failure(),
        Some(&selected)
    );
    let revision = trace.core.execution.accepted_protocol_revision();

    trace.apply_snapshot(&stream, &terminal).unwrap();
    assert_eq!(trace.core.execution.accepted_protocol_revision(), revision);
    let overwrite = TaskStatus::try_new(
        task,
        TaskStatusVersion::new(10).unwrap(),
        TaskState::Failed,
        Some(originating_failure(
            "replacement runtime failure",
            TaskFailurePhase::Execution,
        )),
        TaskOutputFacts::default(),
    )
    .unwrap();
    assert!(matches!(
        trace.apply_snapshot(&stream, &overwrite),
        Err(AttemptFailure::TaskProtocol(error))
            if matches!(*error, TaskExecutionError::Observation(StatusObservation::TerminalOverwrite))
    ));
    assert_eq!(trace.core.execution.accepted_protocol_revision(), revision);
    assert_eq!(trace.status(task), Some(&terminal));
    assert_eq!(
        trace.core.execution.selected_task_failure(),
        Some(&selected)
    );
    trace.expect_diagnostic_reads(&[task]);
}

#[test]
fn attempt_failure_refinement_queues_only_the_new_terminal_task() {
    let mut trace = TerminalTrace::new();
    let (peer, origin) = {
        let mut tasks = trace.core.execution.graph().tasks();
        let _root = tasks.next().unwrap();
        let origin = tasks.next().unwrap().identity();
        let peer = tasks.next().unwrap().identity();
        (peer, origin)
    };
    let peer_failure = TerminationDetail::Aborted(AbortCause::PeerTaskFailed);
    let peer_terminal = TaskStatus::try_new(
        peer,
        TaskStatusVersion::new(2).unwrap(),
        TaskState::Aborted,
        Some(peer_failure.clone()),
        TaskOutputFacts::default(),
    )
    .unwrap();
    let original_failure =
        originating_failure("different task origin", TaskFailurePhase::Execution);
    let origin_terminal = TaskStatus::try_new(
        origin,
        TaskStatusVersion::new(2).unwrap(),
        TaskState::Failed,
        Some(original_failure.clone()),
        TaskOutputFacts::default(),
    )
    .unwrap();
    let peer_stream = status_frame(&mut trace, peer_terminal.clone());
    let origin_stream = status_frame(&mut trace, origin_terminal.clone());
    trace.open_diagnostic_window();

    trace.apply_snapshot(&peer_stream, &peer_terminal).unwrap();
    assert_eq!(trace.status(peer), Some(&peer_terminal));
    assert_eq!(trace.status(origin), None);
    assert!(trace.core.execution.diagnostics().read_ready());
    assert_eq!(
        trace.core.execution.selected_task_failure(),
        Some(&SelectedTaskTermination::new(peer, peer_failure)),
    );
    let first_revision = trace.core.execution.accepted_protocol_revision();

    trace
        .apply_snapshot(&origin_stream, &origin_terminal)
        .unwrap();
    assert_eq!(trace.status(origin), Some(&origin_terminal));
    let selected = SelectedTaskTermination::new(origin, original_failure);
    assert_eq!(
        trace.core.execution.selected_task_failure(),
        Some(&selected)
    );
    let selected_revision = trace.core.execution.accepted_protocol_revision();
    assert!(selected_revision > first_revision);

    for (stream, terminal) in [
        (&peer_stream, &peer_terminal),
        (&origin_stream, &origin_terminal),
    ] {
        trace.apply_snapshot(stream, terminal).unwrap();
        assert_eq!(trace.status(terminal.identity()), Some(terminal));
        assert_eq!(
            trace.core.execution.accepted_protocol_revision(),
            selected_revision
        );
        assert_eq!(
            trace.core.execution.selected_task_failure(),
            Some(&selected)
        );
    }
    trace.expect_diagnostic_reads(&[peer, origin]);
}

#[test]
fn create_identity_mismatch_asks_qa_about_original_process_replacement() {
    use crate::input::MembershipObservation;
    use novarocks_coordination_contract::{AttemptTerminal, AttemptToLogicalBody};

    let replacement = crate::test_support::backend(99);
    let positive = MembershipObservation::ExactReplacement(replacement);
    for sample in [
        None,
        Some((positive, false)),
        Some((MembershipObservation::ExactOriginal, true)),
        Some((MembershipObservation::Unobserved, true)),
        Some((positive, true)),
    ] {
        let mut trace = TerminalTrace::new();
        let task = trace.core.execution.graph().root_identity();
        let participation = trace
            .core
            .execution
            .task(task.task_id())
            .unwrap()
            .participation();
        let sources = trace.core.sources;
        let activation = trace.core.activation;
        let now = trace.core.execution.monotonic_now();
        // TerminalTrace starts Establish and receives a 30-second effective lease.
        let lease = Duration::from_secs(30);
        let (operation_id, generation) = trace.start_create(task);
        trace.handle_input(now, sources.actor, AttemptInput::Bootstrap);

        let rejection = OperationResult::Verdict {
            outcome: OperationOutcome::IdentityMismatch,
            detail: Some(SafeDetail::new("original Worker refusal").unwrap()),
            acknowledgement: None,
        };
        let refusal = AttemptFailure::TaskProtocol(Box::new(TaskExecutionError::OperationFailed {
            task: Some(task),
            participation,
            operation_id,
            kind: OperationKind::CreateTask,
            rejection: rejection.clone(),
        }));
        let step = trace.handle_input(
            now,
            sources.native,
            AttemptInput::Executor(ExecutorFact::Operation(OperationObservation {
                generation,
                reply: OperationReply {
                    operation_id,
                    kind: OperationKind::CreateTask,
                    result: OperationReplyResult::Worker(rejection.map_ack(|never| match never {})),
                },
            })),
        );
        let (round, effects) = trace.finish_observation_round(now, step);
        assert_eq!(round, Err(refusal.clone()));
        let [
            AttemptEffect {
                body: EffectBody::Port(port),
                ..
            },
        ] = effects.as_slice()
        else {
            panic!("the failed Native prefix must issue the original QA replacement question");
        };
        let AttemptToLogicalBody::PreReadyReplacementQuery {
            token,
            refusal: asked,
        } = &port.body
        else {
            panic!("expected the original QA replacement question");
        };
        assert_eq!(port.activation, activation);
        assert_eq!(asked, &refusal);
        assert_eq!(
            trace.core.execution.rejected_create_observation_remaining(
                task,
                participation,
                operation_id
            ),
            Some(lease)
        );
        assert!(trace.core.terminal.terminal().is_none());

        let permission = trace.handle_input(
            now,
            sources.qa,
            AttemptInput::Port(LogicalToAttempt {
                activation,
                body: LogicalToAttemptBody::PreReadyReplacementReply {
                    token: *token,
                    may_observe: sample.is_some(),
                },
            }),
        );
        let mut expected = refusal.clone();
        let (mut step, at) = if let Some((observation, at_horizon)) = sample {
            let Some(AttemptEffect {
                body:
                    EffectBody::ObserveProcess {
                        request,
                        participation: observed,
                    },
                ..
            }) = permission.effect
            else {
                panic!("allowed replacement must ask for the exact process")
            };
            assert_eq!(observed, participation);
            assert_eq!(permission.selection, InputSelection::Only(sources.process));
            assert!(trace.core.terminal.terminal().is_none());
            let elapsed = if at_horizon {
                lease
            } else {
                lease - Duration::from_secs(1)
            };
            let at = now.saturating_add(elapsed);
            if !at_horizon {
                expected = AttemptFailure::PreReadyReplacement {
                    refusal: Box::new(refusal),
                    participation,
                    operation_id,
                    kind: OperationKind::CreateTask,
                    replacement,
                };
            }
            let step = trace.handle_input(
                at,
                sources.process,
                AttemptInput::Executor(ExecutorFact::ProcessSnapshot {
                    request,
                    participation,
                    sampled_at: at,
                    observation,
                }),
            );
            (step, at)
        } else {
            assert!(permission.effect.is_none());
            (permission, now)
        };

        let terminal = (0..8)
            .find_map(|_| {
                if let Some(AttemptEffect {
                    body: EffectBody::Port(port),
                    ..
                }) = step.effect.take()
                {
                    return Some(port);
                }
                assert_eq!(step.selection, InputSelection::Only(sources.actor));
                step = trace.handle_input(at, sources.actor, AttemptInput::Timer);
                None
            })
            .expect("the original attempt must publish its terminal refusal");
        assert_eq!(terminal.activation, activation);
        assert!(matches!(
            terminal.body,
            AttemptToLogicalBody::AttemptTerminal(AttemptTerminal::Failed(failure)) if failure == expected
        ));
    }
}

#[test]
fn root_read_projection_waits_for_exact_installation_not_create_reply() {
    use crate::{
        AttemptCore,
        effect::Publication,
        input::{AttemptInput, AttemptInputClaim, LocalDispatchResult},
    };
    use std::cell::Cell;

    for (accepted_first, finished_before_reply) in [(true, false), (false, false), (false, true)] {
        let mut trace = TerminalTrace::new();
        let root = trace.core.execution.graph().root_identity();
        let create = trace.start_create(root);
        let uninstalled = TaskStatus::created(root);
        let stream = status_frame(&mut trace, uninstalled.clone());
        let core = &mut trace.core;
        let now = core.execution.monotonic_now();
        let sources = core.sources;
        let next = Cell::new(1);
        let step = |core: &mut Core, source, input| {
            let current = AttemptInputId(id(next.get()));
            next.set(next.get() + 1);
            core.handle(
                now,
                AttemptInputClaim {
                    source,
                    input: current,
                    native_cut: Some(AttemptSourceCut {
                        source: sources.native,
                        tail: (source == sources.native).then_some(current),
                    }),
                    selected_cut: Some(AttemptSourceCut {
                        source: sources.observation,
                        tail: (source == sources.observation).then_some(current),
                    }),
                },
                input,
                &mut Units(128),
            )
            .unwrap()
        };
        let finish = |core: &mut Core, mut pending: AttemptStep| {
            let mut roots = Vec::new();
            for _ in 0..24 {
                let publication = pending.effect.take().and_then(|effect| {
                    let applied = match effect.body {
                        EffectBody::Publish(Publication::RootStatus(projection)) => {
                            assert_eq!(projection.failure, AttemptRootFailure::None);
                            roots.push(projection.status);
                            PublicationApplied::RootStatus
                        }
                        EffectBody::Publish(Publication::ProtocolBatch(_)) => {
                            PublicationApplied::ProtocolBatch { published: true }
                        }
                        EffectBody::ReleaseQueuedInput { .. }
                        | EffectBody::ReleaseCreationPayload { .. } => return None,
                        EffectBody::AdjustReadWindow(request) => {
                            assert_eq!(request.generation, stream.generation);
                            return None;
                        }
                        other => {
                            panic!("unexpected effect in root-read readiness round: {other:?}")
                        }
                    };
                    Some((effect.id, applied))
                });
                let (source, input) = match (pending.selection, publication) {
                    (InputSelection::Only(source), Some((effect, applied)))
                        if source == sources.publication =>
                    {
                        (
                            source,
                            AttemptInput::Executor(ExecutorFact::PublicationApplied {
                                effect,
                                result: Ok(applied),
                            }),
                        )
                    }
                    (InputSelection::Only(source), None)
                        if source == sources.actor || source == sources.observation =>
                    {
                        (sources.actor, AttemptInput::Timer)
                    }
                    (InputSelection::Prefer(source), None) if source == sources.native => {
                        assert_eq!(pending.round_result, Some(Ok(())));
                        return roots;
                    }
                    selection => panic!("unexpected root-read readiness selection: {selection:?}"),
                };
                pending = step(core, source, input);
            }
            panic!("root-read readiness round did not complete");
        };
        let observe = |core: &mut Core, status: TaskStatus| {
            let frame = ReceivedObservation {
                event: ObservationEvent::Fact {
                    source_revision: None,
                    fact: ObservationFact::Status(status),
                },
                ..stream.clone()
            };
            let pending = step(
                core,
                sources.observation,
                AttemptInput::Executor(ExecutorFact::Observations(
                    ObservationBatch::new(vec![frame]).unwrap(),
                )),
            );
            finish(core, pending)
        };
        let reply = |core: &mut Core, reply| {
            let pending = step(
                core,
                sources.native,
                AttemptInput::Executor(ExecutorFact::Operation(reply)),
            );
            finish(core, pending)
        };
        step(core, sources.actor, AttemptInput::Bootstrap);

        // Neither a Create Accepted receipt nor an uninstalled stream
        // snapshot can release the production root projection to the reader.
        let before = if accepted_first {
            reply(core, create_reply(create, uninstalled.clone()))
        } else {
            observe(core, uninstalled.clone())
        };
        assert!(before.is_empty());
        let task = core.execution.task(root.task_id()).unwrap();
        assert_eq!(task.status(), Some(&uninstalled));
        assert_eq!(task.create_ownership_proven(), accepted_first);
        assert!(!task.installed());

        // A covered snapshot can carry Finished across skipped versions.
        // Both that and an Installed Planned root make the original result
        // capability visible, including while the original Create is in flight.
        let installed = TaskStatus::try_new_with_installed(
            root,
            TaskStatusVersion::new(if finished_before_reply { 3 } else { 2 }).unwrap(),
            if finished_before_reply {
                TaskState::Finished
            } else {
                TaskState::Planned
            },
            None,
            TaskOutputFacts::new(finished_before_reply),
            true,
        )
        .unwrap();
        let projected = observe(core, installed.clone());
        assert_eq!(projected.as_slice(), std::slice::from_ref(&installed));
        let task = core.execution.task(root.task_id()).unwrap();
        assert!(task.create_ownership_proven());
        assert!(task.installed());
        assert_eq!(task.has_released_operation(), !accepted_first);

        if !accepted_first {
            let late = if finished_before_reply {
                create_reply(create, uninstalled)
            } else {
                OperationObservation {
                    generation: create.1,
                    reply: OperationReply {
                        operation_id: create.0,
                        kind: OperationKind::CreateTask,
                        result: OperationReplyResult::Local {
                            result: LocalDispatchResult::TransportUnknown,
                            failure: None,
                            protocol_failure: None,
                            detail: None,
                        },
                    },
                }
            };
            let after = reply(core, late);
            assert!(after.iter().all(|status| status == &installed));
            let task = core.execution.task(root.task_id()).unwrap();
            assert_eq!(task.status(), Some(&installed));
            assert!(!task.has_released_operation());
            assert!(task.create_ownership_proven());
            assert!(!task.create_result_is_unresolved());
            assert_eq!(core.root_projection().unwrap().status, installed);
        }
    }
}

#[test]
fn pending_seal_recovers_only_after_exact_terminal_and_new_catch_up() {
    let mut trace = TerminalTrace::new();
    let root = trace.core.execution.graph().root_identity();
    let leaf = trace
        .core
        .execution
        .graph()
        .edges()
        .find(|edge| edge.delivery() == ExchangeDeliveryRequirement::MustDrain)
        .unwrap()
        .producers()[0];
    let create = trace.start_create(leaf);
    trace
        .apply(
            Some(create_reply(
                create,
                TaskStatus::created(leaf).with_installed(),
            )),
            None,
        )
        .unwrap();
    let core = &mut trace.core;
    let start = core.execution.monotonic_now();
    let at = |seconds| start.saturating_add(Duration::from_secs(seconds));
    let source = core.observation_source;
    core.begin_native_prefix(
        start,
        AttemptSourceCut {
            source: core.sources.native,
            tail: None,
        },
    )
    .unwrap();
    let root_finished = frame(core, root, TaskState::Finished, None);
    let participation = core
        .execution
        .graph()
        .task(leaf.task_id())
        .unwrap()
        .participation();
    core.subscriptions.activate(start, participation).unwrap();

    let advance = |core: &mut Core, now, action: AttemptActionId| {
        core.advance_subscription(
            now,
            action,
            AttemptRequestToken::new(id(200)),
            &mut Units(16),
        )
        .unwrap()
    };
    let open = |core: &mut Core, now, action| {
        let SubscriptionStep::Effect(AttemptEffect {
            body: EffectBody::OpenSubscription(request),
            ..
        }) = advance(core, now, action)
        else {
            panic!("expected the original leaf subscription to open");
        };
        assert_eq!(request.participation, participation);
        request
    };
    let observe = |core: &mut Core, now, items| {
        core.apply_observations(now, source, ObservationBatch::new(items).unwrap())
            .unwrap()
    };
    let stream = |opened, generation| {
        move |received_at, event| ReceivedObservation {
            participation,
            generation,
            received_at,
            opening: ObservationOpening {
                started_at: opened,
                opened_at: Some(opened),
            },
            event,
        }
    };
    let fact = |fact| ObservationEvent::Fact {
        source_revision: None,
        fact,
    };
    let complete = |generation: NonZeroU64, initial_cut| {
        fact(ObservationFact::CatchUpComplete {
            generation: generation.get(),
            initial_cut,
        })
    };
    let close = |core: &mut Core, now, action, opened, generation| {
        assert!(matches!(
            advance(core, now, action),
            SubscriptionStep::Effect(AttemptEffect {
                body: EffectBody::CloseSubscription { participation: closed, generation: closing },
                ..
            }) if closed == participation && closing == generation
        ));
        core.apply_subscription_input(
            now,
            &ExecutorFact::SubscriptionExited {
                effect: opened,
                participation,
                generation,
            },
        )
        .unwrap();
    };

    let action = |value| AttemptActionId::new(id(value));
    let first = open(core, start, action(201));
    let initial = stream(start, first.generation);
    assert!(
        observe(
            core,
            start,
            vec![
                root_finished,
                initial(start, fact(ObservationFact::Gone(leaf))),
                initial(start, complete(first.generation, 1)),
            ],
        )
        .is_none()
    );
    let deadline = Some(at(30));
    assert_eq!(core.execution.covered_recovery_deadline(), deadline);
    assert!(seal(core).unwrap().seal.is_none());
    assert!(core.root.pending());

    // The gap schedules reconciliation of the original subscription. Only
    // its actual exit permits the next generation to open.
    close(core, start, action(202), action(201), first.generation);
    let second = open(core, start, action(203));
    assert_eq!(second.generation.get(), first.generation.get() + 1);
    let negative = stream(start, second.generation);
    assert!(
        observe(
            core,
            at(1),
            vec![
                negative(at(1), fact(ObservationFact::Unknown(leaf))),
                negative(
                    at(1),
                    fact(ObservationFact::TaskConvergence(
                        TaskConvergenceReceipt::actual_stopped(leaf, TaskConvergenceVersion::FIRST),
                    )),
                ),
                negative(at(1), complete(second.generation, 2)),
                negative(
                    at(1),
                    fact(ObservationFact::Bookmark {
                        generation: second.generation.get(),
                        sequence: 1,
                        covered_prefix: 2,
                        source_cut: 2,
                    }),
                ),
                negative(at(1), ObservationEvent::Closed),
            ],
        )
        .is_none()
    );
    assert!(core.execution.task_actual_stopped(leaf));
    assert!(!core.execution.covered_observation_ready());
    assert_eq!(core.execution.covered_recovery_deadline(), deadline);
    assert!(core.root.pending());

    close(core, at(1), action(204), action(203), second.generation);
    let third = open(core, at(2), action(205));
    assert_eq!(third.generation.get(), second.generation.get() + 1);
    assert!(third.task_convergence_cursors.iter().any(|cursor| {
        cursor.identity() == leaf && cursor.current_version() == Some(TaskConvergenceVersion::FIRST)
    }));
    let recovered = stream(at(2), third.generation);
    let canceled = |version, state| {
        TaskStatus::try_new(
            leaf,
            TaskStatusVersion::new(version).unwrap(),
            state,
            Some(TerminationDetail::Canceled(
                CancelReason::UpstreamNoLongerNeeded,
            )),
            TaskOutputFacts::default(),
        )
        .unwrap()
    };
    let terminal = canceled(3, TaskState::Canceled);
    assert!(
        observe(
            core,
            at(29),
            vec![
                recovered(
                    at(29),
                    fact(ObservationFact::Status(canceled(2, TaskState::Canceling))),
                ),
                recovered(at(29), fact(ObservationFact::Status(terminal.clone()))),
            ],
        )
        .is_none()
    );
    assert_eq!(
        core.execution.task(leaf.task_id()).unwrap().status(),
        Some(&terminal)
    );
    assert_eq!(core.execution.covered_recovery_deadline(), deadline);
    assert!(core.root.pending());

    let cut = observe(
        core,
        at(29),
        vec![recovered(at(29), complete(third.generation, 3))],
    )
    .expect("the newer complete catch-up settles the original seal");
    assert_eq!(cut.settlement.token, AttemptRequestToken::new(id(90)));
    assert!(cut.settlement.result.is_ok());
    let projection = cut.root_before_seal.expect("the original root projection");
    assert_eq!(projection.status.identity(), root);
    assert_eq!(projection.status.state(), TaskState::Finished);
    assert!(projection.status.output().responsibility_complete());
    assert_eq!(projection.failure, AttemptRootFailure::None);
    assert!(core.execution.covered_observation_ready());
    assert_eq!(core.execution.covered_recovery_deadline(), None);
    assert!(!core.root.pending());
    assert!(core.root.sealed());
    assert!(!core.execution.normal_drain_started());
}
