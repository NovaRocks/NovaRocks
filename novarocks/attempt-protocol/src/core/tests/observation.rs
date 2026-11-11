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
    capacity::ReservationToken,
    execution::{
        CreateGrantProgress,
        ack_phase::{admission_intent, dispatch_admission, queue_establish},
    },
    input::{
        AckPayload, DiagnosticRead, OperationReply, OperationReplyResult, QueuedInputGrantRef,
        SendGeneration,
    },
};
use novarocks_coordination_contract::{AttemptComputationCut, StatusObservation};
use novarocks_execution_contract::{
    CreateTaskReceipt, LeaseReceipt, OperationKind, OperationOutcome, ParticipationReceipt,
    ParticipationState, SafeDetail, TaskFailure, TaskFailurePhase, TaskResourceFacts,
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
                Some(TerminationDetail::Failed(TaskFailure::new_in_phase(
                    FailureFact::new(
                        FailureClass::Data,
                        FailureReason::new(
                            NonZeroU16::new(u16::MAX).unwrap(),
                            NonZeroU16::new(1).unwrap(),
                        ),
                    ),
                    SafeDetail::new("original preparation failure").unwrap(),
                    TaskFailurePhase::Preparation,
                ))),
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
