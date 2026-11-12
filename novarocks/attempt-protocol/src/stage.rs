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

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::{Deref, DerefMut};

use crate::completion_facts::{StageState, derive_stage_state, parent_released_children};
use novarocks_execution_contract::{
    CancelReason, ExchangeEdgeId, StageRef, TaskIdentity, TaskState,
};
use novarocks_types::identity::StageId;
use novarocks_types::identity::TaskId;

use super::error::TaskExecutionError;
use super::graph::TaskGraph;
use super::remote_task::{RemoteTask, RemoteTaskState};
use crate::effect::OperationIntent;
use crate::ids::OperationIds;

const TASK_STATES: [TaskState; 10] = [
    TaskState::Planned,
    TaskState::Running,
    TaskState::Flushing,
    TaskState::Finished,
    TaskState::Canceling,
    TaskState::Canceled,
    TaskState::Aborting,
    TaskState::Aborted,
    TaskState::Failing,
    TaskState::Failed,
];
fn state_index(state: TaskState) -> usize {
    match state {
        TaskState::Planned => 0,
        TaskState::Running => 1,
        TaskState::Flushing => 2,
        TaskState::Finished => 3,
        TaskState::Canceling => 4,
        TaskState::Canceled => 5,
        TaskState::Aborting => 6,
        TaskState::Aborted => 7,
        TaskState::Failing => 8,
        TaskState::Failed => 9,
    }
}
#[derive(Clone, Copy, Debug, Default)]
struct StageCounts {
    states: [usize; TASK_STATES.len()],
    creating: usize,
}
#[derive(Clone, Copy, Eq, PartialEq)]
struct TaskSnapshot {
    state: TaskState,
    creating: bool,
}
impl TaskSnapshot {
    fn of(task: &RemoteTask) -> Self {
        Self {
            state: task.task_state(),
            creating: task.state() == RemoteTaskState::Creating,
        }
    }
}
impl StageCounts {
    fn state(self) -> StageState {
        let mut present = [TaskState::Planned; TASK_STATES.len()];
        let mut used = 0;
        for (index, state) in TASK_STATES.iter().enumerate() {
            if self.states[index] != 0 {
                present[used] = *state;
                used += 1;
            }
        }
        derive_stage_state(self.creating == 0, &present[..used])
            .expect("a validated stage has at least one task")
    }
    fn add(&mut self, snapshot: TaskSnapshot) {
        self.states[state_index(snapshot.state)] += 1;
        self.creating += usize::from(snapshot.creating);
    }
    fn remove(&mut self, snapshot: TaskSnapshot) {
        self.states[state_index(snapshot.state)] = self.states[state_index(snapshot.state)]
            .checked_sub(1)
            .expect("original Task is counted once");
        self.creating = self
            .creating
            .checked_sub(usize::from(snapshot.creating))
            .expect("original creating Task count");
    }
}
/// A pure local mutation boundary. It updates only aggregate facts and owns no
/// resource, callback, runtime capability or foreign destructor.
pub struct TaskMutation<'a> {
    task: &'a mut RemoteTask,
    counts: &'a Cell<StageCounts>,
    before: TaskSnapshot,
    observations: Option<(StageId, &'a demand::StageDemandObservations)>,
}
impl<'a> TaskMutation<'a> {
    fn new(task: &'a mut RemoteTask, counts: &'a Cell<StageCounts>) -> Self {
        let before = TaskSnapshot::of(task);
        Self {
            task,
            counts,
            before,
            observations: None,
        }
    }
    fn observed(
        task: &'a mut RemoteTask,
        counts: &'a Cell<StageCounts>,
        stage: StageId,
        observations: &'a demand::StageDemandObservations,
    ) -> Self {
        let mut guard = Self::new(task, counts);
        guard.observations = Some((stage, observations));
        guard
    }
}
impl Deref for TaskMutation<'_> {
    type Target = RemoteTask;
    fn deref(&self) -> &Self::Target {
        self.task
    }
}
impl DerefMut for TaskMutation<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.task
    }
}
impl Drop for TaskMutation<'_> {
    fn drop(&mut self) {
        let after = TaskSnapshot::of(self.task);
        if after != self.before {
            let mut counts = self.counts.get();
            // Another Task guard may already have updated these counts. The
            // before predicate belongs to current counts at this Drop, never
            // to the stage snapshot when this particular guard was created.
            let released_before = parent_released_children(counts.state());
            counts.remove(self.before);
            counts.add(after);
            let released_after = parent_released_children(counts.state());
            self.counts.set(counts);
            if released_before != released_after
                && let Some((stage, observations)) = self.observations
            {
                observations.stage_changed(stage, released_after);
            }
        }
    }
}

/// The frontend's owner of one stage.
#[derive(Debug)]
pub struct StageExecution {
    stage: StageRef,
    root_task: Option<TaskId>,
    tasks: BTreeMap<TaskId, RemoteTask>,
    counts: Cell<StageCounts>,
}

impl StageExecution {
    /// Freezes one stage over its complete task set.
    pub fn new(
        stage: StageRef,
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
        let mut counts = StageCounts::default();
        for task in tasks.values() {
            counts.add(TaskSnapshot::of(task));
        }
        Ok(Self {
            stage,
            root_task,
            tasks,
            counts: Cell::new(counts),
        })
    }

    pub const fn root_task(&self) -> Option<TaskId> {
        self.root_task
    }

    pub fn task(&self, task_id: TaskId) -> Option<&RemoteTask> {
        self.tasks.get(&task_id)
    }

    pub(crate) fn task_after(&self, after: Option<TaskId>) -> Option<&RemoteTask> {
        match after {
            Some(after) => self
                .tasks
                .range((std::ops::Bound::Excluded(after), std::ops::Bound::Unbounded))
                .next(),
            None => self.tasks.first_key_value(),
        }
        .map(|(_, task)| task)
    }
    pub fn task_mut(&mut self, task_id: TaskId) -> Option<TaskMutation<'_>> {
        let task = self.tasks.get_mut(&task_id)?;
        Some(TaskMutation::new(task, &self.counts))
    }

    pub(crate) fn task_mut_observed<'a>(
        &'a mut self,
        task_id: TaskId,
        observations: &'a demand::StageDemandObservations,
    ) -> Option<TaskMutation<'a>> {
        if !observations.binds_stage(self.stage) {
            return None;
        }
        let task = self.tasks.get_mut(&task_id)?;
        Some(TaskMutation::observed(
            task,
            &self.counts,
            self.stage.stage_id(),
            observations,
        ))
    }
    #[cfg(test)]
    pub(crate) fn tasks_mut_observed<'a>(
        &'a mut self,
        observations: &'a demand::StageDemandObservations,
    ) -> impl ExactSizeIterator<Item = (&'a TaskId, TaskMutation<'a>)> + 'a {
        let counts = &self.counts;
        assert!(
            observations.binds_stage(self.stage),
            "original stage observation binding"
        );
        let stage = self.stage.stage_id();
        self.tasks.iter_mut().map(move |(id, task)| {
            (
                id,
                TaskMutation::observed(task, counts, stage, observations),
            )
        })
    }

    pub fn tasks(&self) -> impl ExactSizeIterator<Item = (&TaskId, &RemoteTask)> + '_ {
        self.tasks.iter()
    }

    /// Whether every task of this frozen set has been created.
    ///
    /// This is the input the neutral derivation needs to refuse a terminal or
    /// flushing aggregate over a task that does not exist on its backend yet.
    pub fn scheduling_complete(&self) -> bool {
        self.counts.get().creating == 0
    }

    pub fn task_states(&self) -> Vec<TaskState> {
        self.tasks.values().map(RemoteTask::task_state).collect()
    }

    /// This stage's derived aggregate state.
    pub fn state(&self) -> StageState {
        // Contains/all predicates depend on presence, never multiplicity.
        // This fixed vocabulary projection reuses the original reducer without
        // walking the stage's frozen Task set on every incoming observation.
        self.counts.get().state()
    }

    /// Whether this stage has stopped needing what its producers send.
    pub fn released_children(&self) -> bool {
        parent_released_children(self.state())
    }

    #[cfg(test)]
    pub fn all_terminal(&self) -> bool {
        self.tasks.values().all(RemoteTask::is_terminal)
    }

    #[cfg(test)]
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

    /// Emit normal cancellation only for Tasks whose original demand owner
    /// already authorized stand-down. Required input/output obligations can
    /// refuse that authorization even after a parent stage releases demand.
    pub fn cancel_normally_stood_down_tasks(
        &mut self,
        ids: &mut OperationIds,
    ) -> Vec<OperationIntent> {
        let root = self.root_task;
        self.tasks
            .iter_mut()
            .filter(|(task_id, task)| Some(**task_id) != root && task.normally_stood_down())
            .filter_map(|(_, task)| task.cancel_intent(CancelReason::UpstreamNoLongerNeeded, ids))
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
    #[cfg(test)]
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

/// One frozen edge member's monotonic change. Producer fanout is deliberately
/// separate: the core charges each exact destination/producer effect before
/// expanding it, and emits known close work before a pending open.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct EdgeMemberResolution {
    pub(crate) closed: bool,
    pub(crate) newly_opened: bool,
}

#[derive(Debug)]
pub struct EdgeOpenTracker {
    must_drain: BTreeSet<ExchangeEdgeId>,
    drain_tasks: BTreeSet<TaskIdentity>,
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
        let must_drain = graph
            .edges()
            .filter(|edge| {
                edge.delivery()
                    == novarocks_execution_contract::ExchangeDeliveryRequirement::MustDrain
            })
            .map(|edge| edge.edge_id())
            .collect();
        let drain_tasks = graph
            .edges()
            .filter(|edge| {
                edge.delivery()
                    == novarocks_execution_contract::ExchangeDeliveryRequirement::MustDrain
            })
            .flat_map(|edge| edge.producers().iter().chain(edge.destinations()))
            .copied()
            .collect();
        Self {
            must_drain,
            drain_tasks,
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

    pub(crate) fn destination_edge_count(&self, destination: TaskIdentity) -> usize {
        self.edges_of_destination
            .get(&destination)
            .map_or(0, Vec::len)
    }

    pub(crate) fn destination_edge(
        &self,
        destination: TaskIdentity,
        index: usize,
    ) -> Option<ExchangeEdgeId> {
        self.edges_of_destination
            .get(&destination)?
            .get(index)
            .copied()
    }

    pub(crate) fn producer_at(&self, edge: ExchangeEdgeId, index: usize) -> Option<TaskIdentity> {
        self.producers.get(&edge)?.get(index).copied()
    }

    /// The caller charges this one member before advancing its graph cursor.
    /// Frozen edge iteration is ordered, so the incidence check is logarithmic
    /// and does not scan all edges or copy an entire producer/destination set.
    pub(crate) fn resolve_edge_member(
        &mut self,
        destination: TaskIdentity,
        edge: ExchangeEdgeId,
        normally_closed: bool,
    ) -> Result<EdgeMemberResolution, TaskExecutionError> {
        if !self
            .edges_of_destination
            .get(&destination)
            .is_some_and(|edges| edges.binary_search(&edge).is_ok())
        {
            return Err(TaskExecutionError::UnknownOperation);
        }
        if normally_closed && self.must_drain.contains(&edge) {
            return Ok(EdgeMemberResolution::default());
        }
        let pending = self
            .awaiting
            .get_mut(&edge)
            .ok_or(TaskExecutionError::UnknownOperation)?;
        let closed = normally_closed
            && self
                .normally_closed
                .entry(edge)
                .or_default()
                .insert(destination);
        pending.remove(&destination);
        Ok(EdgeMemberResolution {
            closed,
            newly_opened: pending.is_empty() && self.decided.insert(edge),
        })
    }

    fn resolve_destination(
        &mut self,
        destination: TaskIdentity,
        normally_closed: bool,
    ) -> EdgeNormalCloseResolution {
        let mut resolution = EdgeNormalCloseResolution::default();
        for index in 0..self.destination_edge_count(destination) {
            let edge_id = self
                .destination_edge(destination, index)
                .expect("frozen destination incidence");
            let delta = self
                .resolve_edge_member(destination, edge_id, normally_closed)
                .expect("same frozen edge member");
            if delta.closed {
                resolution.closed.push(EdgeNormalCloseEffect {
                    edge_id,
                    destination,
                });
            }
            if delta.newly_opened {
                resolution.opened.push(EdgeReadyDecision {
                    edge_id,
                    normally_closed: self
                        .normally_closed
                        .get(&edge_id)
                        .into_iter()
                        .flatten()
                        .copied()
                        .collect(),
                });
            }
        }
        resolution
    }

    pub(crate) fn requires_drain(&self, task: TaskIdentity) -> bool {
        self.drain_tasks.contains(&task)
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
    use novarocks_types::identity::{AttemptId, QueryExecutionId, QueryId};

    fn identity(task: u32) -> TaskIdentity {
        TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(1, 3), AttemptId::new(1).unwrap()).unwrap(),
            StageId::new(1).unwrap(),
            TaskId::new(task).unwrap(),
            crate::test_support::backend(u64::from(task)),
        )
    }

    #[test]
    fn closing_one_destination_keeps_its_sibling_eligible_to_open() {
        let edge = ExchangeEdgeId::new(1).unwrap();
        let producer = identity(1);
        let target = identity(2);
        let sibling = identity(3);
        let mut tracker = EdgeOpenTracker {
            must_drain: BTreeSet::new(),
            drain_tasks: BTreeSet::new(),
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

    #[test]
    fn strict_streams_ignore_demand_withdrawal_and_wait_for_installation() {
        let ordinary = ExchangeEdgeId::new(1).unwrap();
        let strict = ExchangeEdgeId::new(2).unwrap();
        let producer = identity(1);
        let target = identity(2);
        let mut tracker = EdgeOpenTracker {
            must_drain: BTreeSet::from([strict]),
            drain_tasks: BTreeSet::from([producer, target]),
            awaiting: BTreeMap::from([
                (ordinary, BTreeSet::from([target])),
                (strict, BTreeSet::from([target])),
            ]),
            normally_closed: BTreeMap::new(),
            producers: BTreeMap::from([(ordinary, vec![producer]), (strict, vec![producer])]),
            edges_of_destination: BTreeMap::from([(target, vec![ordinary, strict])]),
            decided: BTreeSet::new(),
        };
        let withdrawal = tracker.note_normally_closed(target);
        assert_eq!(
            withdrawal
                .closed()
                .iter()
                .map(|effect| effect.edge_id())
                .collect::<Vec<_>>(),
            [ordinary]
        );
        assert_eq!(
            withdrawal
                .opened()
                .iter()
                .map(|effect| effect.edge_id())
                .collect::<Vec<_>>(),
            [ordinary]
        );
        assert!(tracker.awaiting[&strict].contains(&target));
        assert!(!tracker.normally_closed.contains_key(&strict));
        let installed = tracker.note_created(target);
        assert_eq!(
            installed
                .iter()
                .map(|effect| effect.edge_id())
                .collect::<Vec<_>>(),
            [strict]
        );
        assert!(installed[0].normally_closed().is_empty());
        assert!(tracker.note_normally_closed(target).closed().is_empty());
        assert!(tracker.requires_drain(producer));
        assert!(tracker.requires_drain(target));
    }

    /// Exhaust every word up to length six, including repeated observations.
    /// These are inputs to the production reducers, not a second transition
    /// model: the oracle checks output safety and frozen membership directly.
    #[test]
    fn destination_reducer_bounded_interleavings() {
        use crate::conclusion::TerminationLatch;
        use novarocks_execution_contract::{SafeDetail, TaskFailure, TerminationDetail};

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
                novarocks_execution_contract::failure::FailureFact::new(
                    novarocks_execution_contract::failure::FailureClass::Data,
                    novarocks_execution_contract::failure::FailureReason::new(
                        std::num::NonZeroU16::new(u16::MAX).unwrap(),
                        std::num::NonZeroU16::new(1).unwrap(),
                    ),
                ),
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
                    must_drain: BTreeSet::new(),
                    drain_tasks: BTreeSet::new(),
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
                            let index = usize::from(matches!(event, Event::FailureB));
                            failure.latch(destinations[index], failures[index].clone());
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
                        // Accepted-root sealing rejects this same latched-failure
                        // property in the real result-owner tests.
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

#[cfg(test)]
mod aggregate_tests {
    use super::*;

    #[test]
    fn actual_task_mutation_returns_the_original_stage_counts_on_scope_exit() {
        use novarocks_execution_contract::{TaskOutputFacts, TaskStatus, TaskStatusVersion};
        let task = crate::test_support::remote(1);
        let identity = task.identity();
        let mut stage = StageExecution::new(
            StageRef::new(identity.query_execution_id(), identity.stage_id()),
            Some(identity.task_id()),
            BTreeMap::from([(identity.task_id(), task)]),
        )
        .unwrap();
        assert!(!stage.scheduling_complete());
        let running = TaskStatus::try_new(
            identity,
            TaskStatusVersion::new(2).unwrap(),
            TaskState::Running,
            None,
            TaskOutputFacts::default(),
        )
        .unwrap();
        stage
            .task_mut(identity.task_id())
            .unwrap()
            .observe_status(&running)
            .unwrap();
        assert!(stage.scheduling_complete());
        assert_eq!(stage.state(), StageState::Running);
        let flushing = TaskStatus::try_new(
            identity,
            TaskStatusVersion::new(3).unwrap(),
            TaskState::Flushing,
            None,
            TaskOutputFacts::default(),
        )
        .unwrap();
        stage
            .task_mut(identity.task_id())
            .unwrap()
            .observe_status(&flushing)
            .unwrap();
        assert_eq!(stage.state(), StageState::Flushing);
        let finished = TaskStatus::try_new(
            identity,
            TaskStatusVersion::new(4).unwrap(),
            TaskState::Finished,
            None,
            TaskOutputFacts::new(true),
        )
        .unwrap();
        stage
            .task_mut(identity.task_id())
            .unwrap()
            .observe_status(&finished)
            .unwrap();
        assert_eq!(stage.state(), StageState::Finished);
        assert_eq!(
            Some(stage.state()),
            derive_stage_state(stage.scheduling_complete(), &stage.task_states())
        );
    }

    #[test]
    fn bounded_stage_histogram_preserves_the_original_reducer_for_every_short_multiset() {
        for length in 1..=5 {
            for mut word in 0..TASK_STATES.len().pow(length) {
                let mut states = Vec::new();
                let mut counts = StageCounts::default();
                for _ in 0..length {
                    let state = TASK_STATES[word % TASK_STATES.len()];
                    word /= TASK_STATES.len();
                    states.push(state);
                    counts.add(TaskSnapshot {
                        state,
                        creating: false,
                    });
                }
                for scheduling_complete in [false, true] {
                    counts.creating = usize::from(!scheduling_complete);
                    assert_eq!(
                        Some(counts.state()),
                        derive_stage_state(scheduling_complete, &states),
                        "states={states:?}, complete={scheduling_complete}"
                    );
                }
            }
        }
    }
}

pub(crate) mod derivation;

pub(crate) mod demand;
