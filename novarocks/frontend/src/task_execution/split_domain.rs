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

use novarocks_execution::task_execution::{FrontendAction, PlanNodeId};

use super::graph::TaskGraph;
use crate::query_execution::split_assignment::{AssignmentTarget, SplitAssignmentDriverError};

/// The driver's addresses for every task of one plan node.
///
/// The driver addresses a task by its fragment instance id, which is exactly
/// the task protocol's kernel key. Building its target set from the graph is
/// therefore an address translation, not a second placement decision.
#[allow(
    dead_code,
    reason = "The task protocol is not routed into production yet; the adapter is exercised by this module's tests until the transport cutover lands."
)]
pub(crate) fn assignment_targets(graph: &TaskGraph, node: PlanNodeId) -> Vec<AssignmentTarget> {
    graph
        .tasks()
        .filter(|task| task.split_plan_nodes().contains(&node))
        .map(|task| AssignmentTarget {
            backend_idx: task.backend_idx(),
            fragment_instance_id: task.fragment_instance_id(),
        })
        .collect()
}

/// What the frontend does about one split-delivery failure.
#[allow(
    dead_code,
    reason = "The task protocol is not routed into production yet; the adapter is exercised by this module's tests until the transport cutover lands."
)]
pub(crate) fn delivery_action(error: &SplitAssignmentDriverError) -> FrontendAction {
    error.as_operation_outcome().frontend_action()
}
