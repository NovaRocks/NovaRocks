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

//! Original split admission at the AP owner; no physical transport proof.

use super::*;
use crate::{
    effect::QueuedInputPayload,
    input::{AckPayload, OperationObservation, OperationReplyResult},
};
use novarocks_execution_contract::{
    DomainProgression, PlanNodeSplitReceipt, SplitOffer, SplitSequence, SplitWatermark,
    TaskDomainReceipt, UpdateTaskReceipt, operation::OperationResult,
};

#[test]
fn a_terminal_marker_after_the_original_task_took_its_splits_is_admitted() {
    let node = PlanNodeId::new(7).unwrap();
    let mut execution = many_tasks_with_split_node(1, Some(node));
    let task = identity(&execution, 1);
    execution
        .apply_published(
            &TaskStatus::try_new(
                task,
                TaskStatusVersion::new(2).unwrap(),
                TaskState::Running,
                None,
                TaskOutputFacts::default(),
            )
            .unwrap(),
            &mut StatusReport::default(),
        )
        .unwrap();
    let first = SplitSequence::new(1).unwrap();
    let mut watermark = SplitWatermark::empty();
    let mut operations = BTreeSet::new();
    for (index, no_more) in [false, true, false].into_iter().enumerate() {
        let payload = PayloadRef(NonZeroU64::new(100 + index as u64).unwrap());
        let offer = SplitOffer::batch(first, first, no_more).unwrap();
        let (disposition, intent) = execution
            .prepare_task_update(
                task,
                TaskDomainUpdate::SplitAssignment {
                    node,
                    offer,
                    payload,
                    queued_bytes: 16,
                },
            )
            .unwrap();
        assert_eq!(disposition, UpdateAdmission::Preparing);
        let intent = intent.expect("the original producer's same range is admissible");
        let operation = intent.operation_id();
        assert!(operations.insert(operation));
        let original = grant(10 + index as u64);
        execution
            .begin_initial_update_admission(
                intent,
                original.generation().admission_effect(),
                QueuedInputPayload::NewTaskUpdate(payload),
            )
            .unwrap();
        assert!(matches!(
            execution
                .observe_lifecycle_grant(operation, original)
                .unwrap(),
            CreateGrantProgress::Waiting,
        ));
        let (queued, replay) = execution
            .advance_update_send(task, original.generation().admission_effect())
            .unwrap();
        assert!(queued && replay.is_none());
        let batch = execution.dispatcher.take_batch().unwrap();
        assert_eq!(batch.operations.len(), 1);
        assert_eq!(batch.operations[0].operation_id(), operation);
        assert_eq!(batch.queued_inputs.as_ref(), [original]);
        execution
            .dispatcher
            .accept_emitted(&batch, original.generation().admission_effect())
            .unwrap();
        watermark = watermark.apply_batch(first, no_more);
        let reply = OperationObservation {
            generation: original.generation(),
            reply: OperationAcknowledgement {
                operation_id: operation,
                kind: OperationKind::UpdateTask,
                result: OperationReplyResult::Worker(OperationResult::Verdict {
                    outcome: novarocks_execution_contract::OperationOutcome::Accepted,
                    detail: None,
                    acknowledgement: Some(AckPayload::Update(UpdateTaskReceipt::new(
                        task,
                        vec![TaskDomainReceipt::SplitAssignment {
                            nodes: vec![
                                PlanNodeSplitReceipt::new(node, watermark).with_queued_splits(0),
                            ],
                            progression: if index == 2 {
                                DomainProgression::Idempotent
                            } else {
                                DomainProgression::Apply
                            },
                        }],
                    ))),
                }),
            },
        };
        let (action, result) = execution.apply_dispatched_reply(&reply);
        assert!(action.is_none());
        result.unwrap();
        // Reapplying this same physical result cannot advance the original
        // operation or undo the seal retained by its accepted receipt.
        execution.apply_dispatched_reply(&reply).1.unwrap();
        assert_eq!(execution.dispatcher.in_flight_operations(), 0);
        assert_eq!(execution.task(task.task_id()).unwrap().pending_updates(), 0);
        let crate::split_delivery::SplitDeliverySettlement::Accepted(nodes) =
            crate::split_delivery::settle(&reply.reply)
        else {
            panic!("the exact original accepted split receipt")
        };
        assert_eq!(nodes[0].plan_node_id, node.get());
        assert_eq!(nodes[0].accepted_through_sequence, first.get());
        assert_eq!(nodes[0].no_more_splits, index != 0);
        assert_eq!(nodes[0].queued_splits, 0);
    }
    assert_eq!(operations.len(), 3);
    assert!(execution.failure_cause().is_none());
}
