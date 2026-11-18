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

//! Definitive original Create replies must emit the exact payload return.
//! Rich backing and actual Drop remain with the independently tested FA Source.

use super::*;
use crate::{
    effect::{EffectBody, QueuedInputPayload},
    input::{AckPayload, LocalDispatchResult, OperationObservation, OperationReplyResult},
};
use novarocks_execution_contract::{
    CreateTaskReceipt, OperationOutcome, operation::OperationResult,
};

fn original_create_release(outcome: OperationOutcome) {
    let mut execution = many_tasks(1);
    let task = identity(&execution, 1);
    let part = execution.task(task.task_id()).unwrap().participation();
    begin_test_establish(&mut execution, part);
    let first = grant(1);
    let operation = enqueue_create(&mut execution, task, first);
    let batch = execution.dispatcher.take_batch().unwrap();
    let [
        OperationIntent::CreateTask {
            identity: original_task,
            creation: payload,
            ..
        },
    ] = batch.operations.as_ref()
    else {
        panic!("one original Create");
    };
    assert_eq!(*original_task, task);
    let payload = *payload;
    assert_eq!(batch.queued_inputs.as_ref(), [first]);
    execution
        .dispatcher
        .accept_emitted(&batch, grant(11).generation().admission_effect())
        .unwrap();
    assert!(
        execution
            .acknowledge_current_send(
                first.generation(),
                &OperationAcknowledgement {
                    operation_id: operation,
                    kind: OperationKind::CreateTask,
                    result: OperationReplyResult::Local {
                        result: LocalDispatchResult::TransportUnknown,
                        failure: None,
                        protocol_failure: None,
                        detail: None,
                    },
                }
            )
            .unwrap()
    );
    assert!(execution.task(task.task_id()).unwrap().create_pending());
    assert!(
        execution.advance_task_input_cleanup().is_none(),
        "Unknown cannot return the original payload"
    );

    let second = grant(2);
    let EffectBody::AdmitQueuedInput {
        operation: replay,
        generation,
        payload: QueuedInputPayload::AlreadyRetained,
        ..
    } = execution
        .begin_create_admission(task, second.generation().admission_effect())
        .unwrap()
        .unwrap()
        .body
    else {
        panic!("the same logical Create must reuse retained backing");
    };
    assert_eq!(generation, second.generation());
    assert_eq!(replay.operation_id(), operation);
    assert!(
        matches!(replay, OperationIntent::CreateTask { identity: actual, creation, .. }
        if actual == task && creation == payload)
    );
    assert!(matches!(
        execution.observe_create_grant(operation, second).unwrap(),
        CreateGrantProgress::Queued
    ));
    let batch = execution.dispatcher.take_batch().unwrap();
    assert_eq!(batch.operations.len(), 1);
    assert_eq!(batch.operations[0].operation_id(), operation);
    assert_eq!(batch.queued_inputs.as_ref(), [second]);
    execution
        .dispatcher
        .accept_emitted(&batch, grant(12).generation().admission_effect())
        .unwrap();
    let accepted_payload = || {
        AckPayload::Create(Box::new(CreateTaskReceipt::new(
            task,
            Vec::new(),
            TaskStatus::created(task),
        )))
    };
    let result = execution
        .apply_dispatched_reply(&OperationObservation {
            generation: second.generation(),
            reply: OperationAcknowledgement {
                operation_id: operation,
                kind: OperationKind::CreateTask,
                result: OperationReplyResult::Worker(OperationResult::Verdict {
                    outcome,
                    detail: None,
                    acknowledgement: (outcome == OperationOutcome::Accepted).then(accepted_payload),
                }),
            },
        })
        .1;
    if outcome == OperationOutcome::Accepted {
        result.unwrap();
        assert_eq!(
            execution.task(task.task_id()).unwrap().state(),
            crate::remote_task::RemoteTaskState::Created
        );
    } else {
        assert!(matches!(result, Err(TaskExecutionError::OperationFailed {
            task: Some(actual), participation, operation_id, kind: OperationKind::CreateTask, rejection,
        }) if actual == task && participation == part && operation_id == operation
            && rejection.outcome() == OperationOutcome::InvalidStateOrRequest));
        assert_eq!(
            execution.task(task.task_id()).unwrap().state(),
            crate::remote_task::RemoteTaskState::Terminal
        );
    }
    assert!(!execution.task(task.task_id()).unwrap().create_pending());
    assert_eq!(execution.dispatcher.in_flight_operations(), 0);
    let mut releases = Vec::new();
    for _ in 0..8 {
        match execution.advance_task_input_cleanup() {
            Some(EffectBody::ReleaseCreationPayload { task, payload }) => {
                releases.push((task, payload))
            }
            Some(other) => panic!("unexpected original Create cleanup: {other:?}"),
            None => break,
        }
    }
    assert_eq!(releases, [(task, payload)]);
    assert!(execution.advance_task_input_cleanup().is_none());
    assert!(execution.create_candidate_for_task(task).unwrap().is_none());
    assert!(execution.dispatcher.take_batch().is_none());
    let late = execution.acknowledge(&OperationAcknowledgement {
        operation_id: operation,
        kind: OperationKind::CreateTask,
        result: OperationReplyResult::Worker(OperationResult::Verdict {
            outcome: OperationOutcome::Accepted,
            detail: None,
            acknowledgement: Some(accepted_payload()),
        }),
    });
    assert!(matches!(late, Err(TaskExecutionError::UnknownOperation)));
    assert!(!execution.task(task.task_id()).unwrap().create_pending());
    assert!(execution.create_candidate_for_task(task).unwrap().is_none());
    assert!(execution.advance_task_input_cleanup().is_none());
}

#[test]
fn original_accepted_create_returns_its_exact_payload_once_after_unknown_replay() {
    original_create_release(OperationOutcome::Accepted);
}

#[test]
fn original_refused_create_returns_its_exact_payload_and_late_acceptance_cannot_revive_it() {
    original_create_release(OperationOutcome::InvalidStateOrRequest);
}
