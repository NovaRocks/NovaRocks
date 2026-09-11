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

//! The split-assignment domain adapter.
//!
//! Runtime split delivery already has one owner: the per-round
//! `SplitAssignmentDriver`, which allocates sequences, retains the immutable
//! request across an unknown outcome, keeps one update in flight per task,
//! and classifies every failure by type. None of that is reimplemented here.
//! This module only translates between the two owners' vocabularies, so the
//! task protocol addresses the same tasks and reaches the same retry verdict
//! as the driver does.

use std::collections::BTreeMap;

use novarocks_execution::task_execution::PlanNodeId;
use novarocks_query_application::coordination::FrontendAction;
use novarocks_types::UniqueId;
use novarocks_types::identity::TaskId;

use super::graph::TaskGraph;
use crate::query_execution::split_assignment::SplitAssignmentDriverError;

/// Stable Task-protocol address for one split-assignment destination.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct TaskAssignmentTarget {
    pub(crate) identity: novarocks_execution::task_execution::TaskIdentity,
    pub(crate) fragment_instance_id: UniqueId,
}

/// The driver's addresses for every task of one plan node.
///
/// The driver addresses a task by its fragment instance id, which is exactly
/// the task protocol's kernel key. Building its target set from the graph is
/// therefore an address translation, not a second placement decision.
#[allow(
    dead_code,
    reason = "The task protocol is not routed into production yet; the adapter is exercised by this module's tests until the transport cutover lands."
)]
pub(crate) fn assignment_targets(graph: &TaskGraph, node: PlanNodeId) -> Vec<TaskAssignmentTarget> {
    graph
        .tasks()
        .filter(|task| task.split_plan_nodes().contains(&node))
        .map(|task| TaskAssignmentTarget {
            identity: task.identity(),
            fragment_instance_id: task.fragment_instance_id(),
        })
        .collect()
}

/// The task each driver address names, keyed by fragment instance id.
///
/// This is the same translation as [`assignment_targets`] read backwards, and
/// it lives beside it for that reason: a lookup written next to a caller could
/// disagree with this module about which task one driver target names. The
/// graph builder already refuses two tasks that derive one kernel key, so
/// collecting here cannot silently merge two tasks into one address.
#[allow(
    dead_code,
    reason = "The task protocol is not routed into production yet; the adapter is exercised by this module's tests until the transport cutover lands."
)]
pub(crate) fn task_kernel_index(graph: &TaskGraph) -> BTreeMap<UniqueId, TaskId> {
    graph
        .tasks()
        .map(|task| (task.fragment_instance_id(), task.task_id()))
        .collect()
}

/// What the frontend does about one split-delivery failure.
#[allow(
    dead_code,
    reason = "The task protocol is not routed into production yet; the adapter is exercised by this module's tests until the transport cutover lands."
)]
pub(crate) fn delivery_action(error: &SplitAssignmentDriverError) -> FrontendAction {
    match error {
        SplitAssignmentDriverError::Transport { .. } => FrontendAction::RetryExactRequest,
        SplitAssignmentDriverError::Closed => FrontendAction::StopSendingAndReconcile,
        SplitAssignmentDriverError::DeliveryInProgress => FrontendAction::FailAttempt,
        SplitAssignmentDriverError::Rejected { .. }
        | SplitAssignmentDriverError::Assignment(_)
        | SplitAssignmentDriverError::NoAdmittedTask { .. }
        | SplitAssignmentDriverError::SplitSource { .. } => FrontendAction::FailAttempt,
    }
}
