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

//! The original split-producer delivery verdict, selected once by the core.
use crate::error::TaskExecutionError;
use crate::input::{AckPayload, OperationReply, UpdateAdmission};
use crate::operation::{FrontendAction, frontend_action};
use novarocks_execution_contract::{
    DomainProgression, SplitSequence, TaskDomainReceipt, TaskIdentity, UpdateTaskReceipt,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AcceptedPlanNode {
    pub plan_node_id: i32,
    pub accepted_through_sequence: u64,
    pub no_more_splits: bool,
    pub queued_splits: u64,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SplitDeliverySettlement {
    Accepted(Box<[AcceptedPlanNode]>),
    Unknown(String),
    DestinationFinished(String),
    Rejected { reason: String, detail: String },
    Local(String),
}

pub fn admission(
    task: TaskIdentity,
    result: Result<UpdateAdmission, &TaskExecutionError>,
) -> Option<SplitDeliverySettlement> {
    use SplitDeliverySettlement as S;
    match result {
        Ok(UpdateAdmission::Queued | UpdateAdmission::Preparing) => None,
        Ok(UpdateAdmission::DiscardedNoLongerNeeded) => Some(S::DestinationFinished(format!(
            "the original owner no longer needs input for task {}",
            task.task_id()
        ))),
        Ok(UpdateAdmission::DiscardedTerminal) => Some(S::Local(format!(
            "task update discarded: task {} is terminal",
            task.task_id()
        ))),
        Err(error) => Some(S::Local(format!(
            "task update refused by the frontend: {error}"
        ))),
    }
}

/// Called only after the core matched the original UpdateTask operation.
/// Missing queue depth is rejected; it never becomes a report of zero.
pub fn settle(ack: &OperationReply) -> SplitDeliverySettlement {
    use SplitDeliverySettlement as S;
    if ack.is_applied() {
        match applied_nodes(ack.payload()) {
            Ok(nodes) => S::Accepted(nodes),
            Err(detail) => S::Local(detail),
        }
    } else if matches!(
        frontend_action(ack.dispatch_result()),
        FrontendAction::RetryExactRequest
    ) {
        S::Unknown("task update transport outcome is unknown".to_owned())
    } else if matches!(
        frontend_action(ack.dispatch_result()),
        FrontendAction::StopSendingAndReconcile | FrontendAction::Settled
    ) {
        S::DestinationFinished(format!(
            "the task update was answered with {:?}",
            ack.worker_outcome()
        ))
    } else {
        let rejection = ack.worker_outcome().map_or_else(
            || format!("{:?}", ack.dispatch_result()),
            |outcome| format!("{outcome:?}"),
        );
        S::Rejected {
            reason: rejection.clone(),
            detail: format!("the task update was answered with {rejection}"),
        }
    }
}

fn applied_nodes(payload: &AckPayload) -> Result<Box<[AcceptedPlanNode]>, String> {
    let AckPayload::Update(receipt) = payload else {
        return Err("an applied task update carried no update receipt".to_owned());
    };
    let nodes = split_receipt_nodes(receipt)?;
    if nodes.is_empty() {
        return Err("an applied split assignment carried no split receipt".to_owned());
    }
    Ok(nodes)
}

fn split_receipt_nodes(receipt: &UpdateTaskReceipt) -> Result<Box<[AcceptedPlanNode]>, String> {
    let mut accepted = Vec::new();
    for domain in receipt.domains() {
        let TaskDomainReceipt::SplitAssignment { nodes, progression } = domain else {
            continue;
        };
        if !matches!(
            progression,
            DomainProgression::Apply | DomainProgression::Idempotent
        ) {
            return Err(format!(
                "an applied split assignment reported progression {progression:?}"
            ));
        }
        for node in nodes {
            // A watermark this bridge admitted is nonzero, so zero is the
            // exact encoding of "accepted nothing" in the driver's own field
            // and can never be mistaken for a sequence it issued.
            let accepted_through_sequence = node
                .watermark()
                .accepted_through()
                .map_or(0, SplitSequence::get);
            // A queue depth the backend did not report is not a queue depth of
            // zero: reporting zero would tell the driver this task has nothing
            // outstanding and disable the backpressure it uses to stop
            // pulling.
            let queued_splits = node.queued_splits().ok_or_else(|| {
                format!(
                    "the split receipt of plan node {} reports no queue depth",
                    node.node()
                )
            })?;
            accepted.push(AcceptedPlanNode {
                plan_node_id: node.node().get(),
                accepted_through_sequence,
                no_more_splits: node.watermark().no_more_splits(),
                queued_splits,
            });
        }
    }
    Ok(accepted.into_boxed_slice())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::OperationReplyResult;
    use novarocks_execution_contract::{
        OperationKind, OperationOutcome, PlanNodeId, PlanNodeSplitReceipt, SplitWatermark,
        UpdateTaskReceipt, operation::OperationResult,
    };
    use novarocks_types::{AttemptId, QueryExecutionId, QueryId, StageId, TaskId};

    fn reply(depth: Option<u64>) -> OperationReply {
        let task = TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(51, 52), AttemptId::new(1).unwrap()).unwrap(),
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            crate::test_support::backend(1),
        );
        let mut node = PlanNodeSplitReceipt::new(
            PlanNodeId::new(7).unwrap(),
            SplitWatermark::empty().apply_batch(SplitSequence::new(171).unwrap(), true),
        );
        if let Some(depth) = depth {
            node = node.with_queued_splits(depth);
        }
        OperationReply {
            operation_id: crate::ids::OperationIds::new(crate::ids::IdSeed::new(1, 100)).issue(),
            kind: OperationKind::UpdateTask,
            result: OperationReplyResult::Worker(OperationResult::Verdict {
                outcome: OperationOutcome::Accepted,
                detail: None,
                acknowledgement: Some(AckPayload::Update(UpdateTaskReceipt::new(
                    task,
                    vec![TaskDomainReceipt::SplitAssignment {
                        nodes: vec![node],
                        progression: DomainProgression::Apply,
                    }],
                ))),
            }),
        }
    }

    #[test]
    fn an_applied_split_without_reported_depth_cannot_resume_the_producer() {
        let result = settle(&reply(None));
        assert!(
            matches!(result, SplitDeliverySettlement::Local(ref detail) if detail.contains("reports no queue depth"))
        );
    }

    #[test]
    fn accepted_split_retains_the_original_watermark_seal_and_nonzero_backlog() {
        let result = settle(&reply(Some(29)));
        assert_eq!(
            result,
            SplitDeliverySettlement::Accepted(
                vec![AcceptedPlanNode {
                    plan_node_id: 7,
                    accepted_through_sequence: 171,
                    no_more_splits: true,
                    queued_splits: 29
                }]
                .into_boxed_slice()
            )
        );
    }
}
