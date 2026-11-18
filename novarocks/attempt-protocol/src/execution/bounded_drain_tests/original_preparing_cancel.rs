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

//! The original Preparing input is withdrawn by accepted cancellation snapshots.
//! Grant references are passive AP facts; no physical release or exit is claimed.

use super::*;
use crate::effect::{EffectBody, QueuedInputPayload};
use novarocks_execution_contract::{SplitOffer, SplitSequence};

#[test]
fn original_preparing_split_observes_canceling_two_then_canceled_three_without_new_admission() {
    let node = PlanNodeId::new(7).unwrap();
    let mut execution = many_tasks_with_split_node(1, Some(node));
    let task = identity(&execution, 1);
    let payload = PayloadRef(NonZeroU64::new(101).unwrap());
    let sequence = SplitSequence::new(1).unwrap();
    let update = TaskDomainUpdate::SplitAssignment {
        node,
        offer: SplitOffer::batch(sequence, sequence, false).unwrap(),
        payload,
        queued_bytes: 16,
    };
    let (admission, original) = execution.prepare_task_update(task, update.clone()).unwrap();
    assert_eq!(admission, UpdateAdmission::Preparing);
    let original = original.unwrap();
    let operation = original.operation_id();
    let input = grant(5);
    let action = input.generation().admission_effect();
    let effect = execution
        .begin_initial_update_admission(
            original,
            action,
            QueuedInputPayload::NewTaskUpdate(payload),
        )
        .unwrap();
    assert_eq!(effect.id, action);
    assert!(matches!(effect.body, EffectBody::AdmitQueuedInput {
        operation: OperationIntent::UpdateTask { identity, .. }, generation,
        payload: QueuedInputPayload::NewTaskUpdate(actual), ..
    } if identity == task && generation == input.generation() && actual == payload));
    assert!(
        execution
            .task(task.task_id())
            .unwrap()
            .input_preparation_pending()
    );
    assert_eq!(execution.task(task.task_id()).unwrap().pending_updates(), 0);
    assert!(execution.has_lifecycle_admission(operation));
    for (version, state) in [(2, TaskState::Canceling), (3, TaskState::Canceled)] {
        let status = TaskStatus::try_new(
            task,
            TaskStatusVersion::new(version).unwrap(),
            state,
            Some(TerminationDetail::Canceled(
                CancelReason::UpstreamNoLongerNeeded,
            )),
            TaskOutputFacts::default(),
        )
        .unwrap();
        let mut report = StatusReport::default();
        execution.apply_published(&status, &mut report).unwrap();
        assert_eq!(report.accepted, 1);
        let held = execution.task(task.task_id()).unwrap();
        assert_eq!(held.status(), Some(&status));
        assert_eq!(held.task_state(), state);
        assert_eq!(held.cursor().current_version(), Some(status.version()));
        assert_eq!(held.pending_updates(), 0);
    }
    assert!(execution.task(task.task_id()).unwrap().is_terminal());
    assert!(matches!(execution.advance_task_input_cleanup(),
        Some(EffectBody::Withdraw { effect }) if effect == action));
    assert!(
        execution.has_lifecycle_admission(operation),
        "the original admission still owes its result"
    );
    assert!(
        matches!(execution.observe_lifecycle_grant(operation, input).unwrap(),
        CreateGrantProgress::Release(original) if original == input)
    );
    assert!(!execution.has_lifecycle_admission(operation));
    assert!(!execution.tracks_pending_update_grant(input));
    let held = execution.task(task.task_id()).unwrap();
    assert!(!held.input_preparation_pending());
    assert_eq!(held.pending_updates(), 0);
    assert_eq!(held.discarded_updates(), 1);
    let (admission, request) = execution.prepare_task_update(task, update).unwrap();
    assert_eq!(admission, UpdateAdmission::DiscardedNoLongerNeeded);
    assert!(request.is_none());
    assert_eq!(execution.dispatcher.queued_items(), 0);
    assert_eq!(execution.dispatcher.in_flight_operations(), 0);
    let mut more = true;
    for _ in 0..8 {
        match execution.advance_task_input_cleanup() {
            Some(EffectBody::ReleaseCreationPayload { task: original, .. }) => {
                assert_eq!(original, task)
            }
            Some(other) => {
                panic!("the returned original grant must not be refunded twice: {other:?}")
            }
            None => {
                more = false;
                break;
            }
        }
    }
    assert!(!more, "the original finite cleanup reaches idle");
    assert!(execution.advance_task_input_cleanup().is_none());
    assert!(execution.dispatcher.take_batch().is_none());
}
