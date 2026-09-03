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

//! One stage's owner, and the edge-open decision that spans stages.
//!
//! A stage's aggregate state is derived, never invented: it comes from the
//! neutral `derive_stage_state` over this stage's complete, frozen task set.
//! That is what stops one early finisher in a multi-task stage from reading
//! as `FLUSHING` and cancelling children that the stage's other tasks still
//! need.

use std::collections::{BTreeMap, BTreeSet};

use novarocks_execution::task_execution::{
    CancelReason, ExchangeEdgeId, StageRef, StageState, TaskIdentity, TaskState,
    derive_stage_state, parent_released_children,
};
use novarocks_sql::plan_read::FragmentId;
use novarocks_types::identity::{StageId, TaskId};

use super::error::TaskExecutionError;
use super::graph::TaskGraph;
use super::intent::OperationIntent;
use super::remote_task::{RemoteTask, RemoteTaskState};

/// The frontend's owner of one stage.
#[derive(Debug)]
pub struct StageExecution {
    stage: StageRef,
    fragment_id: FragmentId,
    root_task: Option<TaskId>,
    tasks: BTreeMap<TaskId, RemoteTask>,
}

impl StageExecution {
    /// Freezes one stage over its complete task set.
    pub fn new(
        stage: StageRef,
        fragment_id: FragmentId,
        root_task: Option<TaskId>,
        tasks: BTreeMap<TaskId, RemoteTask>,
    ) -> Result<Self, TaskExecutionError> {
        if tasks.is_empty() {
            return Err(TaskExecutionError::Schedule(format!(
                "stage {} has no task",
                stage.stage_id()
            )));
        }
        if let Some(root) = root_task
            && !tasks.contains_key(&root)
        {
            return Err(TaskExecutionError::Schedule(format!(
                "stage {} does not contain its declared root task {root}",
                stage.stage_id()
            )));
        }
        Ok(Self {
            stage,
            fragment_id,
            root_task,
            tasks,
        })
    }

    pub const fn stage(&self) -> StageRef {
        self.stage
    }

    pub const fn stage_id(&self) -> StageId {
        self.stage.stage_id()
    }

    pub const fn fragment_id(&self) -> FragmentId {
        self.fragment_id
    }

    pub const fn root_task(&self) -> Option<TaskId> {
        self.root_task
    }

    pub fn task(&self, task_id: TaskId) -> Option<&RemoteTask> {
        self.tasks.get(&task_id)
    }

    pub fn task_mut(&mut self, task_id: TaskId) -> Option<&mut RemoteTask> {
        self.tasks.get_mut(&task_id)
    }

    pub fn tasks(&self) -> impl ExactSizeIterator<Item = (&TaskId, &RemoteTask)> + '_ {
        self.tasks.iter()
    }

    pub fn tasks_mut(&mut self) -> impl ExactSizeIterator<Item = (&TaskId, &mut RemoteTask)> + '_ {
        self.tasks.iter_mut()
    }

    /// Whether every task of this frozen set has been created.
    ///
    /// This is the input the neutral derivation needs to refuse a terminal or
    /// flushing aggregate over a task that does not exist on its backend yet.
    pub fn scheduling_complete(&self) -> bool {
        self.tasks
            .values()
            .all(|task| !matches!(task.state(), RemoteTaskState::Creating))
    }

    pub fn task_states(&self) -> Vec<TaskState> {
        self.tasks.values().map(RemoteTask::task_state).collect()
    }

    /// This stage's derived aggregate state.
    pub fn state(&self) -> StageState {
        derive_stage_state(self.scheduling_complete(), &self.task_states())
            .expect("a validated stage has at least one task")
    }

    /// Whether this stage has stopped needing what its producers send.
    pub fn released_children(&self) -> bool {
        parent_released_children(self.state())
    }

    pub fn all_terminal(&self) -> bool {
        self.tasks.values().all(RemoteTask::is_terminal)
    }

    pub fn all_output_released(&self) -> bool {
        // Only a task that FINISHED owes an output responsibility. That state
        // is constructible on the backend precisely when the responsibility is
        // complete, so requiring the fact there is a real check.
        //
        // A task that terminated any other way -- canceled because it was no
        // longer needed, aborted, or failed -- has no such responsibility to
        // complete and will never publish it. Requiring it of every task made
        // the attempt drain wait out its whole budget on every query with an
        // early-terminating branch: a cross or semi join that stops reading, a
        // non-root task stood down normally. The tasks were terminal, their
        // resources released by that terminal, and the frontend sat waiting
        // fifteen seconds for a fact that could not arrive.
        self.tasks
            .values()
            .all(|task| task.task_state() != TaskState::Finished || task.output_released())
    }

    /// Stands down every task of this stage except the query's root.
    ///
    /// There is one normal reason, and this is the only thing that uses it:
    /// anything else is a failure or an abort, which take their own paths and
    /// can never be dressed up as a success-compatible cancellation.
    pub fn cancel_non_root_tasks(&mut self) -> Vec<OperationIntent> {
        let root = self.root_task;
        self.tasks
            .iter_mut()
            .filter(|(task_id, _)| Some(**task_id) != root)
            .filter_map(|(_, task)| task.cancel_intent(CancelReason::UpstreamNoLongerNeeded))
            .collect()
    }
}

/// The cross-stage edge-open decision.
///
/// An edge is opened once every frozen destination of that edge has
/// acknowledged its own creation. The destinations of one edge live in the
/// consumer stage, so this cannot be a stage-local decision; what stays
/// stage-local is the wire, because the resulting fact is recorded on the
/// producer's own `RemoteTask` and only leaves once that producer is created.
#[derive(Debug)]
pub struct EdgeOpenTracker {
    awaiting: BTreeMap<ExchangeEdgeId, BTreeSet<TaskIdentity>>,
    producers: BTreeMap<ExchangeEdgeId, Vec<TaskIdentity>>,
    edges_of_destination: BTreeMap<TaskIdentity, Vec<ExchangeEdgeId>>,
    decided: BTreeSet<ExchangeEdgeId>,
}

impl EdgeOpenTracker {
    pub fn from_graph(graph: &TaskGraph) -> Self {
        let mut awaiting = BTreeMap::<ExchangeEdgeId, BTreeSet<TaskIdentity>>::new();
        let mut producers = BTreeMap::<ExchangeEdgeId, Vec<TaskIdentity>>::new();
        let mut edges_of_destination = BTreeMap::<TaskIdentity, Vec<ExchangeEdgeId>>::new();
        for edge in graph.edges() {
            awaiting.insert(
                edge.edge_id(),
                edge.destinations().iter().copied().collect(),
            );
            producers.insert(edge.edge_id(), edge.producers().to_vec());
            for &destination in edge.destinations() {
                edges_of_destination
                    .entry(destination)
                    .or_default()
                    .push(edge.edge_id());
            }
        }
        Self {
            awaiting,
            producers,
            edges_of_destination,
            decided: BTreeSet::new(),
        }
    }

    /// Records one task's create acknowledgement.
    ///
    /// Returns the edges whose complete destination set is now acknowledged,
    /// which are exactly the edges that may be opened.
    pub fn note_created(&mut self, destination: TaskIdentity) -> Vec<ExchangeEdgeId> {
        let mut newly_decided = Vec::new();
        for edge_id in self
            .edges_of_destination
            .get(&destination)
            .into_iter()
            .flatten()
        {
            let Some(pending) = self.awaiting.get_mut(edge_id) else {
                continue;
            };
            pending.remove(&destination);
            if pending.is_empty() && self.decided.insert(*edge_id) {
                newly_decided.push(*edge_id);
            }
        }
        newly_decided
    }

    pub fn producers_of(&self, edge_id: ExchangeEdgeId) -> &[TaskIdentity] {
        self.producers
            .get(&edge_id)
            .map_or(&[], |producers| producers.as_slice())
    }

    pub fn is_decided(&self, edge_id: ExchangeEdgeId) -> bool {
        self.decided.contains(&edge_id)
    }
}
