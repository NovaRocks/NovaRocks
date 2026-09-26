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

use crate::query_execution::artifact::FragmentId;
use novarocks_execution::task_execution::{
    CancelReason, ExchangeEdgeId, StageRef, TaskIdentity, TaskState,
};
use novarocks_query_application::coordination::{
    StageState, derive_stage_state, parent_released_children,
};
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

    pub(crate) fn tasks_mut(
        &mut self,
    ) -> impl ExactSizeIterator<Item = (&TaskId, &mut RemoteTask)> + '_ {
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
/// An edge is decided once every frozen destination is either ready or has
/// explicitly withdrawn its need for input. The destinations of one edge live
/// in the consumer stage, so this cannot be a stage-local decision; what stays
/// stage-local is the wire, because the resulting fact is recorded on the
/// producer's own `RemoteTask` and only leaves once that producer is created.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EdgeReadyDecision {
    edge_id: ExchangeEdgeId,
    normally_closed: Vec<TaskIdentity>,
}

impl EdgeReadyDecision {
    pub const fn edge_id(&self) -> ExchangeEdgeId {
        self.edge_id
    }

    /// Frozen destinations that must be closed on the producer before the
    /// edge's remaining destinations receive send permission.
    pub fn normally_closed(&self) -> &[TaskIdentity] {
        &self.normally_closed
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EdgeNormalCloseEffect {
    edge_id: ExchangeEdgeId,
    destination: TaskIdentity,
}

impl EdgeNormalCloseEffect {
    pub const fn edge_id(&self) -> ExchangeEdgeId {
        self.edge_id
    }

    pub const fn destination(&self) -> TaskIdentity {
        self.destination
    }
}

/// The two independent producer effects of an exact no-more-input decision.
/// Closing a destination is immediate; deciding the full edge may happen
/// before, during, or after this call according to its other destinations.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct EdgeNormalCloseResolution {
    closed: Vec<EdgeNormalCloseEffect>,
    opened: Vec<EdgeReadyDecision>,
}

impl EdgeNormalCloseResolution {
    pub fn closed(&self) -> &[EdgeNormalCloseEffect] {
        &self.closed
    }

    pub fn opened(&self) -> &[EdgeReadyDecision] {
        &self.opened
    }
}

#[derive(Debug)]
pub struct EdgeOpenTracker {
    awaiting: BTreeMap<ExchangeEdgeId, BTreeSet<TaskIdentity>>,
    normally_closed: BTreeMap<ExchangeEdgeId, BTreeSet<TaskIdentity>>,
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
            normally_closed: BTreeMap::new(),
            producers,
            edges_of_destination,
            decided: BTreeSet::new(),
        }
    }

    /// Records one task's create acknowledgement. A previous normal close is
    /// monotonic: a late acknowledgement cannot revive that destination.
    ///
    /// Returns edges whose complete destination set is now ready or normally
    /// closed, with the exact closed members to project before opening.
    pub fn note_created(&mut self, destination: TaskIdentity) -> Vec<EdgeReadyDecision> {
        self.resolve_destination(destination, false).opened
    }

    /// Consumes an exact no-more-input authorization for one frozen Task.
    /// This closes its sending need even when its Create outcome is unknown;
    /// it does not claim that the Task was never installed or actually stopped.
    pub fn note_normally_closed(&mut self, destination: TaskIdentity) -> EdgeNormalCloseResolution {
        self.resolve_destination(destination, true)
    }

    fn resolve_destination(
        &mut self,
        destination: TaskIdentity,
        normally_closed: bool,
    ) -> EdgeNormalCloseResolution {
        let mut resolution = EdgeNormalCloseResolution::default();
        for edge_id in self
            .edges_of_destination
            .get(&destination)
            .into_iter()
            .flatten()
        {
            let Some(pending) = self.awaiting.get_mut(edge_id) else {
                continue;
            };
            if normally_closed
                && self
                    .normally_closed
                    .entry(*edge_id)
                    .or_default()
                    .insert(destination)
            {
                resolution.closed.push(EdgeNormalCloseEffect {
                    edge_id: *edge_id,
                    destination,
                });
            }
            pending.remove(&destination);
            if pending.is_empty() && self.decided.insert(*edge_id) {
                resolution.opened.push(EdgeReadyDecision {
                    edge_id: *edge_id,
                    normally_closed: self
                        .normally_closed
                        .get(edge_id)
                        .into_iter()
                        .flatten()
                        .copied()
                        .collect(),
                });
            }
        }
        resolution
    }

    pub fn producers_of(&self, edge_id: ExchangeEdgeId) -> &[TaskIdentity] {
        self.producers
            .get(&edge_id)
            .map_or(&[], |producers| producers.as_slice())
    }
}

#[cfg(test)]
mod destination_close_tests {
    use super::*;
    use novarocks_types::identity::{AttemptId, BackendProcessId, QueryExecutionId, QueryId};

    fn identity(task: u32) -> TaskIdentity {
        TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(1, 3), AttemptId::new(1).unwrap()).unwrap(),
            StageId::new(1).unwrap(),
            TaskId::new(task).unwrap(),
            BackendProcessId::new_v7(),
        )
    }

    #[test]
    fn closing_one_destination_keeps_its_sibling_eligible_to_open() {
        let edge = ExchangeEdgeId::new(1).unwrap();
        let producer = identity(1);
        let target = identity(2);
        let sibling = identity(3);
        let mut tracker = EdgeOpenTracker {
            awaiting: BTreeMap::from([(edge, BTreeSet::from([target, sibling]))]),
            normally_closed: BTreeMap::new(),
            producers: BTreeMap::from([(edge, vec![producer])]),
            edges_of_destination: BTreeMap::from([(target, vec![edge]), (sibling, vec![edge])]),
            decided: BTreeSet::new(),
        };

        let closure = tracker.note_normally_closed(target);
        assert_eq!(closure.closed().len(), 1);
        assert_eq!(closure.closed()[0].destination(), target);
        assert!(closure.opened().is_empty());
        let opened = tracker.note_created(sibling);
        assert_eq!(opened.len(), 1);
        assert_eq!(opened[0].edge_id(), edge);
        assert_eq!(opened[0].normally_closed(), &[target]);
        assert!(
            tracker.note_created(target).is_empty(),
            "late creation cannot reopen the closed destination"
        );
        assert_eq!(tracker.producers_of(edge), &[producer]);
    }

    /// Exhaust every word up to length six, including repeated observations.
    /// These are inputs to the production reducers, not a second transition
    /// model: the oracle checks output safety and frozen membership directly.
    #[test]
    fn destination_reducer_bounded_interleavings() {
        use novarocks_execution::task_execution::{
            SafeDetail, TaskFailure, TaskFailureCategory, TerminationDetail,
        };
        use novarocks_query_application::coordination::{RootReadFacts, TerminationLatch};

        #[derive(Clone, Copy, Debug)]
        enum Event {
            ReadyA,
            ReadyB,
            CloseA,
            CloseB,
            FailureA,
            FailureB,
        }
        const EVENTS: [Event; 6] = [
            Event::ReadyA,
            Event::ReadyB,
            Event::CloseA,
            Event::CloseB,
            Event::FailureA,
            Event::FailureB,
        ];
        const DEPTH: usize = 6;
        let edge = ExchangeEdgeId::new(1).unwrap();
        let producer = identity(1);
        let destinations = [identity(2), identity(3)];
        let failures = ["destination A failed", "destination B failed"].map(|detail| {
            TerminationDetail::Failed(TaskFailure::new(
                TaskFailureCategory::Execution,
                SafeDetail::truncating(detail),
            ))
        });
        let mut traces = 0usize;
        let mut transitions = 0usize;
        let mut states = BTreeSet::new();
        let mut closing_unknown_hits = 0usize;
        let mut late_ready_hits = 0usize;
        let mut failure_after_close_hits = 0usize;
        for length in 0..=DEPTH {
            for mut word in 0..EVENTS.len().pow(length as u32) {
                let mut trace = Vec::with_capacity(length);
                for _ in 0..length {
                    trace.push(EVENTS[word % EVENTS.len()]);
                    word /= EVENTS.len();
                }
                traces += 1;
                let mut tracker = EdgeOpenTracker {
                    awaiting: BTreeMap::from([(edge, BTreeSet::from(destinations))]),
                    normally_closed: BTreeMap::new(),
                    producers: BTreeMap::from([(edge, vec![producer])]),
                    edges_of_destination: destinations
                        .into_iter()
                        .map(|task| (task, vec![edge]))
                        .collect(),
                    decided: BTreeSet::new(),
                };
                let mut failure = TerminationLatch::open();
                let mut ready_seen = BTreeSet::new();
                let mut close_seen = BTreeSet::new();
                let mut close_emitted = BTreeSet::new();
                let mut opens = 0usize;
                for (step, event) in trace.iter().enumerate() {
                    transitions += 1;
                    let (closed, opened) = match event {
                        Event::ReadyA | Event::ReadyB => {
                            let task = destinations[usize::from(matches!(event, Event::ReadyB))];
                            if close_seen.contains(&task) {
                                late_ready_hits += 1;
                            }
                            ready_seen.insert(task);
                            (Vec::new(), tracker.note_created(task))
                        }
                        Event::CloseA | Event::CloseB => {
                            let task = destinations[usize::from(matches!(event, Event::CloseB))];
                            if !ready_seen.contains(&task) {
                                closing_unknown_hits += 1;
                            }
                            close_seen.insert(task);
                            let resolution = tracker.note_normally_closed(task);
                            (resolution.closed, resolution.opened)
                        }
                        Event::FailureA | Event::FailureB => {
                            if !close_seen.is_empty() {
                                failure_after_close_hits += 1;
                            }
                            failure.latch(
                                failures[usize::from(matches!(event, Event::FailureB))].clone(),
                            );
                            (Vec::new(), Vec::new())
                        }
                    };
                    let counterexample =
                        format!("trace={trace:?}, step={step}, tracker={tracker:?}");
                    for effect in closed {
                        assert_eq!(effect.edge_id(), edge, "{counterexample}");
                        assert!(
                            close_seen.contains(&effect.destination()),
                            "{counterexample}"
                        );
                        assert!(
                            close_emitted.insert(effect.destination()),
                            "duplicate close: {counterexample}"
                        );
                    }
                    for decision in opened {
                        opens += 1;
                        assert_eq!(opens, 1, "duplicate open: {counterexample}");
                        assert_eq!(decision.edge_id(), edge, "{counterexample}");
                        let emitted_closed = decision
                            .normally_closed()
                            .iter()
                            .copied()
                            .collect::<BTreeSet<_>>();
                        assert_eq!(
                            emitted_closed, close_seen,
                            "lost closed destination: {counterexample}"
                        );
                        assert!(
                            destinations
                                .iter()
                                .all(|task| ready_seen.contains(task) || close_seen.contains(task)),
                            "unresolved sibling opened: {counterexample}"
                        );
                    }
                    let actual_closed = tracker
                        .normally_closed
                        .get(&edge)
                        .cloned()
                        .unwrap_or_default();
                    assert_eq!(
                        actual_closed, close_seen,
                        "closed destination revived: {counterexample}"
                    );
                    assert_eq!(
                        close_emitted, close_seen,
                        "missing exact close effect: {counterexample}"
                    );
                    let all_resolved = destinations
                        .iter()
                        .all(|task| ready_seen.contains(task) || close_seen.contains(task));
                    assert_eq!(
                        tracker.decided.contains(&edge),
                        all_resolved,
                        "eligible sibling blocked: {counterexample}"
                    );
                    if failure.is_latched() {
                        assert!(
                            !failure.cause().unwrap().is_success_compatible(),
                            "failure disguised: {counterexample}"
                        );
                        assert!(
                            !RootReadFacts::new(TaskState::Finished, true, failure.is_latched())
                                .client_visible_completion(),
                            "failure accepted as success: {counterexample}"
                        );
                    }
                    // Record reachable production reducer state, independent of
                    // trace counters and the safety oracle's observation sets.
                    let pending = &tracker.awaiting[&edge];
                    states.insert((
                        pending.contains(&destinations[0]),
                        pending.contains(&destinations[1]),
                        actual_closed.contains(&destinations[0]),
                        actual_closed.contains(&destinations[1]),
                        tracker.decided.contains(&edge),
                        failure.cause().map(|cause| format!("{cause:?}")),
                    ));
                }
            }
        }
        assert_eq!(
            traces,
            (0..=DEPTH)
                .map(|depth| EVENTS.len().pow(depth as u32))
                .sum::<usize>()
        );
        assert!(states.len() > 1 && transitions > 0);
        assert!(closing_unknown_hits > 0 && late_ready_hits > 0 && failure_after_close_hits > 0);
        eprintln!(
            "destination exploration: depth={DEPTH}, alphabet={}, traces={traces}, transitions={transitions}, states={}, closing_unknown_hits={closing_unknown_hits}, late_ready_hits={late_ready_hits}, failure_after_close_hits={failure_after_close_hits}",
            EVENTS.len(),
            states.len()
        );
    }
}
