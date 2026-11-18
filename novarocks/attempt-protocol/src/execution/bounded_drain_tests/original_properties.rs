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

//! Original status, cursor, and revision properties at the protocol owner.
//! These tests use passive Worker facts, not physical transport or exit proof.

use super::*;
use crate::input::{AckPayload, OperationObservation, OperationReply, OperationReplyResult};
use novarocks_execution_contract::{
    CreateTaskReceipt, OperationOutcome, SafeDetail, TaskFailure, operation::OperationResult,
};

fn send_original_create(
    execution: &mut QueryTaskExecution,
    task: TaskIdentity,
) -> (TaskOperationId, QueuedInputGrantRef) {
    let part = execution.task(task.task_id()).unwrap().participation();
    let establish = prepare_test_establish(execution, part);
    let OperationIntent::EstablishParticipation {
        initial_lease_sequence,
        initial_lease_valid_for,
        ..
    } = establish
    else {
        panic!("the original owner authors Establish");
    };
    let establish_grant = grant(898);
    execution
        .dispatcher
        .enqueue_reserved(establish.clone(), execution.now, establish_grant)
        .unwrap();
    execution.operation_targets.insert(
        establish.operation_id(),
        OperationTarget::Participation(part),
    );
    let batch = execution.dispatcher.take_batch().unwrap();
    assert_eq!(batch.operations.len(), 1);
    assert_eq!(batch.operations[0].operation_id(), establish.operation_id());
    execution
        .dispatcher
        .accept_emitted(&batch, grant(899).generation().admission_effect())
        .unwrap();
    execution
        .establish_send_started(part, establish.operation_id())
        .unwrap();
    execution
        .apply_dispatched_reply(&OperationObservation {
            generation: establish_grant.generation(),
            reply: OperationReply {
                operation_id: establish.operation_id(),
                kind: OperationKind::UpdateParticipation,
                result: OperationReplyResult::Worker(OperationResult::Verdict {
                    outcome: OperationOutcome::Accepted,
                    detail: None,
                    acknowledgement: Some(AckPayload::Participation(
                        novarocks_execution_contract::ParticipationReceipt::new(
                            part,
                            novarocks_execution_contract::ParticipationState::Active,
                        )
                        .with_lease(
                            novarocks_execution_contract::LeaseReceipt::new(
                                initial_lease_sequence,
                                initial_lease_valid_for,
                                Duration::from_secs(9),
                            ),
                        ),
                    )),
                }),
            },
        })
        .1
        .unwrap();
    let original = grant(900);
    let operation = enqueue_create(execution, task, original);
    let batch = execution.dispatcher.take_batch().unwrap();
    assert_eq!(batch.operations.len(), 1);
    assert_eq!(batch.operations[0].operation_id(), operation);
    assert_eq!(batch.queued_inputs.as_ref(), [original]);
    execution
        .dispatcher
        .accept_emitted(&batch, grant(901).generation().admission_effect())
        .unwrap();
    (operation, original)
}

fn create_receipt(
    operation: TaskOperationId,
    original: QueuedInputGrantRef,
    status: TaskStatus,
) -> OperationObservation {
    OperationObservation {
        generation: original.generation(),
        reply: OperationReply {
            operation_id: operation,
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

#[test]
fn original_late_create_receipt_settles_without_regressing_a_newer_running_snapshot() {
    let mut execution = many_tasks(2);
    let task = identity(&execution, 2);
    let (operation, original) = send_original_create(&mut execution, task);
    let running = TaskStatus::try_new(
        task,
        TaskStatusVersion::new(2).unwrap(),
        TaskState::Running,
        None,
        TaskOutputFacts::default(),
    )
    .unwrap();
    let mut report = StatusReport::default();
    execution.apply_published(&running, &mut report).unwrap();
    assert_eq!(report.accepted, 1);
    let receipt = create_receipt(operation, original, TaskStatus::created(task));
    execution.apply_dispatched_reply(&receipt).1.unwrap();
    let held = execution.task(task.task_id()).unwrap();
    assert_eq!(held.state(), crate::remote_task::RemoteTaskState::Created);
    assert_eq!(held.status(), Some(&running));
    assert_eq!(held.cursor().current_version(), Some(running.version()));
    assert!(held.create_ownership_proven());
    assert!(!held.create_pending());
    assert_eq!(execution.dispatcher.in_flight_operations(), 0);
}

#[test]
fn original_catch_up_preserves_accepted_causes_and_only_refines_derived_failure() {
    let original = || {
        TerminationDetail::Failed(TaskFailure::new(
            novarocks_execution_contract::failure::FailureFact::new(
                novarocks_execution_contract::failure::FailureClass::Data,
                novarocks_execution_contract::failure::FailureReason::new(
                    std::num::NonZeroU16::new(u16::MAX).unwrap(),
                    std::num::NonZeroU16::new(1).unwrap(),
                ),
            ),
            SafeDetail::new("original source").unwrap(),
        ))
    };
    let another = || {
        TerminationDetail::Failed(TaskFailure::new(
            novarocks_execution_contract::failure::FailureFact::new(
                novarocks_execution_contract::failure::FailureClass::Data,
                novarocks_execution_contract::failure::FailureReason::new(
                    std::num::NonZeroU16::new(u16::MAX).unwrap(),
                    std::num::NonZeroU16::new(1).unwrap(),
                ),
            ),
            SafeDetail::new("another source").unwrap(),
        ))
    };
    for (held_cause, proposed, legal) in [
        (
            TerminationDetail::Canceled(CancelReason::UpstreamNoLongerNeeded),
            original(),
            false,
        ),
        (
            TerminationDetail::Canceled(CancelReason::UpstreamNoLongerNeeded),
            TerminationDetail::Aborted(AbortCause::LeaseExpired),
            false,
        ),
        (
            TerminationDetail::Canceled(CancelReason::UpstreamNoLongerNeeded),
            TerminationDetail::Canceled(CancelReason::UpstreamNoLongerNeeded),
            true,
        ),
        (
            TerminationDetail::Aborted(AbortCause::QueryFailed),
            original(),
            false,
        ),
        (original(), another(), false),
        (
            TerminationDetail::Aborted(AbortCause::PeerTaskFailed),
            original(),
            true,
        ),
        (
            TerminationDetail::Aborted(AbortCause::PeerTaskFailed),
            TerminationDetail::Canceled(CancelReason::UpstreamNoLongerNeeded),
            false,
        ),
    ] {
        let mut task = crate::test_support::remote(1);
        let state = |cause: &TerminationDetail, terminal| match (cause, terminal) {
            (TerminationDetail::Canceled(_), false) => TaskState::Canceling,
            (TerminationDetail::Canceled(_), true) => TaskState::Canceled,
            (TerminationDetail::Aborted(_), false) => TaskState::Aborting,
            (TerminationDetail::Aborted(_), true) => TaskState::Aborted,
            (TerminationDetail::Failed(_), false) => TaskState::Failing,
            (TerminationDetail::Failed(_), true) => TaskState::Failed,
        };
        let held = TaskStatus::try_new(
            task.identity(),
            TaskStatusVersion::new(2).unwrap(),
            state(&held_cause, false),
            Some(held_cause),
            TaskOutputFacts::default(),
        )
        .unwrap();
        assert_eq!(
            task.observe_status(&held).unwrap(),
            StatusObservation::Accept
        );
        let later = TaskStatus::try_new(
            task.identity(),
            TaskStatusVersion::new(9).unwrap(),
            state(&proposed, true),
            Some(proposed),
            TaskOutputFacts::default(),
        )
        .unwrap();
        if legal {
            assert_eq!(
                task.observe_status(&later).unwrap(),
                StatusObservation::Accept
            );
            assert_eq!(task.status(), Some(&later));
            assert!(task.is_terminal());
        } else {
            assert_eq!(
                task.observe_status(&later),
                Err(TaskExecutionError::Observation(
                    StatusObservation::ConclusionOverwrite
                ))
            );
            assert_eq!(task.status(), Some(&held));
            assert_eq!(task.cursor().current_version(), Some(held.version()));
            assert!(!task.is_terminal());
        }
    }
}

#[test]
fn original_forward_status_gap_is_accepted_by_the_same_execution_owner() {
    let mut execution = crate::test_support::execution_with_strict_input();
    let task = identity(&execution, 3);
    let status = |version, state| {
        TaskStatus::try_new(
            task,
            TaskStatusVersion::new(version).unwrap(),
            state,
            None,
            TaskOutputFacts::new(true),
        )
        .unwrap()
    };
    for (version, state) in [(1, TaskState::Planned), (5, TaskState::Finished)] {
        let snapshot = status(version, state);
        let mut report = StatusReport::default();
        execution.apply_published(&snapshot, &mut report).unwrap();
        assert_eq!(report.accepted, 1);
        let held = execution.task(task.task_id()).unwrap();
        assert_eq!(held.task_state(), state);
        assert_eq!(held.status(), Some(&snapshot));
        assert_eq!(held.cursor().current_version(), Some(snapshot.version()));
    }
}

#[test]
fn original_owner_revision_changes_for_create_status_but_not_send_replay_or_idle() {
    let mut execution = many_tasks(2);
    let task = identity(&execution, 2);
    assert_eq!(execution.accepted_protocol_revision(), 0);
    let (operation, original) = send_original_create(&mut execution, task);
    assert_eq!(execution.accepted_protocol_revision(), 0);
    let status = TaskStatus::created(task).with_installed();
    execution
        .apply_dispatched_reply(&create_receipt(operation, original, status.clone()))
        .1
        .unwrap();
    let revision = execution.accepted_protocol_revision();
    assert!(
        revision > 0,
        "the original Create snapshot changes protocol projection"
    );
    let mut replay = StatusReport::default();
    execution.apply_published(&status, &mut replay).unwrap();
    assert_eq!(replay.accepted, 0);
    assert_eq!(execution.accepted_protocol_revision(), revision);
    let mut idle = false;
    for _ in 0..16 {
        let progressed = execution.advance_edge_derivation(&mut Turn(128)).unwrap();
        assert_eq!(execution.accepted_protocol_revision(), revision);
        if !progressed {
            idle = true;
            break;
        }
    }
    assert!(
        idle,
        "the finite original edge cursor reaches its idle fold"
    );
    assert!(!execution.advance_edge_derivation(&mut Turn(128)).unwrap());
    assert_eq!(execution.accepted_protocol_revision(), revision);
}
