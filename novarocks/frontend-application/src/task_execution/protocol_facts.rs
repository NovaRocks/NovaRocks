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

//! One activation's bounded, coalesced accepted Task-protocol projection.

use super::completion::{WriteCompletionTracker, WriteVerdict};
use super::error::TaskExecutionError;
use super::execution::QueryTaskExecution;
use super::graph::TaskGraph;
use novarocks_coordination_contract::{AttemptActivationIdentity, SelectedTaskTermination};
use novarocks_execution_contract::{TaskIdentity, TaskStatus};
use novarocks_query_application::api::PlanSeal;
use novarocks_query_application::coordination::NativeAttemptDrive;
use novarocks_types::{QueryExecutionId, TaskId};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use tokio::sync::watch;

#[derive(Debug)]
struct FrozenAttemptProtocol {
    execution: QueryExecutionId,
    plan: PlanSeal,
    root: TaskIdentity,
    tasks: Box<[TaskIdentity]>,
    indices: BTreeMap<TaskId, usize>,
    writers: BTreeSet<TaskIdentity>,
    summary_sources: BTreeSet<TaskIdentity>,
}
impl FrozenAttemptProtocol {
    fn from_graph(
        plan: PlanSeal,
        graph: &TaskGraph,
        writers: &BTreeSet<TaskIdentity>,
        summary_sources: &BTreeSet<TaskIdentity>,
    ) -> Result<Self, TaskExecutionError> {
        Self::from_tasks(
            plan,
            graph.root_identity(),
            graph.tasks().map(|task| task.identity()).collect(),
            writers.clone(),
            summary_sources.clone(),
        )
    }
    fn from_tasks(
        plan: PlanSeal,
        root: TaskIdentity,
        tasks: Box<[TaskIdentity]>,
        writers: BTreeSet<TaskIdentity>,
        summary_sources: BTreeSet<TaskIdentity>,
    ) -> Result<Self, TaskExecutionError> {
        let indices = tasks
            .iter()
            .enumerate()
            .map(|(index, task)| (task.task_id(), index))
            .collect::<BTreeMap<_, _>>();
        let identities = tasks.iter().copied().collect::<BTreeSet<_>>();
        if indices.len() != tasks.len()
            || !identities.contains(&root)
            || !writers.is_subset(&identities)
            || !summary_sources.is_subset(&identities)
            || tasks
                .iter()
                .any(|task| task.query_execution_id() != root.query_execution_id())
        {
            return Err(protocol_error(
                "frozen protocol Tasks, writers, or summary sources do not match one graph",
            ));
        }
        Ok(Self {
            execution: root.query_execution_id(),
            plan,
            root,
            tasks,
            indices,
            writers,
            summary_sources,
        })
    }
    fn index(&self, identity: TaskIdentity) -> Result<usize, TaskExecutionError> {
        self.indices
            .get(&identity.task_id())
            .copied()
            .filter(|&index| self.tasks[index] == identity)
            .ok_or_else(|| protocol_error("accepted protocol fact names a foreign Task"))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AttemptProtocolBinding {
    activation: AttemptActivationIdentity,
    root: TaskIdentity,
    plan: PlanSeal,
}
impl AttemptProtocolBinding {
    fn verify(
        self,
        activation: AttemptActivationIdentity,
        root: TaskIdentity,
        plan: PlanSeal,
    ) -> Result<(), TaskExecutionError> {
        if self.activation != activation || self.root != root || self.plan != plan {
            return Err(protocol_error(
                "protocol facts belong to another activation, root or plan",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
struct Slot {
    status: Option<Arc<TaskStatus>>,
    // One exact accepted witness, not an event history. Coalescing may not
    // hide an earlier writer report before the consumer inspects this Task.
    writer_witness: Option<Arc<TaskStatus>>,
}
#[derive(Debug)]
struct SlotStore {
    binding: Option<AttemptProtocolBinding>,
    revision: u64,
    slots: Box<[Slot]>,
    dirty: BTreeSet<usize>,
    failure: Option<SelectedTaskTermination>,
    #[cfg(test)]
    root_success_sealed: bool,
}
#[derive(Debug)]
struct Shared {
    frozen: Arc<FrozenAttemptProtocol>,
    store: Mutex<SlotStore>,
}

/// Latest exact status of one changed frozen slot. Missing stays explicit.
#[derive(Debug)]
pub(crate) struct AcceptedTaskProtocolChange {
    index: usize,
    identity: TaskIdentity,
    status: Option<Arc<TaskStatus>>,
    writer_witness: Option<Arc<TaskStatus>>,
}
impl AcceptedTaskProtocolChange {
    pub(crate) const fn index(&self) -> usize {
        self.index
    }
    pub(crate) const fn identity(&self) -> TaskIdentity {
        self.identity
    }
    pub(crate) fn status(&self) -> Option<&TaskStatus> {
        self.status.as_deref()
    }
}

/// Full first read, then only coalesced changed slots. All metadata is sampled
/// under the same private lock and belongs to this one exact revision.
#[derive(Debug)]
pub(crate) struct AcceptedAttemptProtocolChanges {
    frozen: Arc<FrozenAttemptProtocol>,
    binding: AttemptProtocolBinding,
    revision: u64,
    initial: bool,
    tasks: Box<[AcceptedTaskProtocolChange]>,
    failure: Option<SelectedTaskTermination>,
    #[cfg(test)]
    root_success_sealed: bool,
}
impl AcceptedAttemptProtocolChanges {
    pub(crate) const fn revision(&self) -> u64 {
        self.revision
    }
    pub(crate) const fn is_initial(&self) -> bool {
        self.initial
    }
    pub(crate) fn tasks(&self) -> &[AcceptedTaskProtocolChange] {
        &self.tasks
    }
    pub(crate) fn selected_failure(&self) -> Option<&SelectedTaskTermination> {
        self.failure.as_ref()
    }
    #[cfg(test)]
    pub(crate) const fn root_success_sealed(&self) -> bool {
        self.root_success_sealed
    }
}

/// Exact source identity retained by a domain's incremental protocol observer.
/// Correlation values cannot substitute for this original frozen allocation.
#[derive(Clone, Debug)]
pub(crate) struct AttemptProtocolProvenance {
    frozen: Arc<FrozenAttemptProtocol>,
    binding: AttemptProtocolBinding,
}
impl AttemptProtocolProvenance {
    pub(crate) fn from_initial(
        changes: &AcceptedAttemptProtocolChanges,
    ) -> Result<Self, TaskExecutionError> {
        if !changes.is_initial() {
            return Err(protocol_error(
                "domain observation requires the original complete first read",
            ));
        }
        Ok(Self {
            frozen: Arc::clone(&changes.frozen),
            binding: changes.binding,
        })
    }
    pub(crate) fn verify(
        &self,
        changes: &AcceptedAttemptProtocolChanges,
    ) -> Result<(), TaskExecutionError> {
        if !Arc::ptr_eq(&self.frozen, &changes.frozen) || self.binding != changes.binding {
            return Err(protocol_error(
                "domain protocol changes belong to another original source",
            ));
        }
        Ok(())
    }
    pub(crate) fn tasks(&self) -> &[TaskIdentity] {
        &self.frozen.tasks
    }
}

/// One serial TaskRound writer. No consumer can publish or rebind it.
#[derive(Debug)]
pub(super) struct AttemptProtocolFactsPublisher {
    shared: Arc<Shared>,
    activation: Option<AttemptActivationIdentity>,
    last_owner_revision: Option<u64>,
    last_sealed: bool,
    revisions: watch::Sender<u64>,
}
/// Move-only: exactly one consumer drains the bounded coalesced dirty set.
/// It retains its real current facts/tracker outside the transfer lock.
#[derive(Debug)]
pub(crate) struct AttemptProtocolFactsReceiver {
    shared: Arc<Shared>,
    revisions: watch::Receiver<u64>,
    last_read: Option<u64>,
}
struct Update {
    index: usize,
    status: Option<Arc<TaskStatus>>,
    writer_witness: Option<Arc<TaskStatus>>,
}
impl AttemptProtocolFactsPublisher {
    pub(super) fn for_graph(
        plan: PlanSeal,
        graph: &TaskGraph,
        writers: &BTreeSet<TaskIdentity>,
        summary_sources: &BTreeSet<TaskIdentity>,
    ) -> Result<(Self, AttemptProtocolFactsReceiver), TaskExecutionError> {
        Ok(Self::pair(FrozenAttemptProtocol::from_graph(
            plan,
            graph,
            writers,
            summary_sources,
        )?))
    }
    fn pair(frozen: FrozenAttemptProtocol) -> (Self, AttemptProtocolFactsReceiver) {
        let slots = (0..frozen.tasks.len()).map(|_| Slot::default()).collect();
        let shared = Arc::new(Shared {
            frozen: Arc::new(frozen),
            store: Mutex::new(SlotStore {
                binding: None,
                revision: 0,
                slots,
                dirty: BTreeSet::new(),
                failure: None,
                #[cfg(test)]
                root_success_sealed: false,
            }),
        });
        let (revisions, receiver) = watch::channel(0);
        (
            Self {
                shared: Arc::clone(&shared),
                activation: None,
                last_owner_revision: None,
                last_sealed: false,
                revisions,
            },
            AttemptProtocolFactsReceiver {
                shared,
                revisions: receiver,
                last_read: None,
            },
        )
    }
    pub(super) fn bind(&mut self, drive: &NativeAttemptDrive) -> Result<(), TaskExecutionError> {
        self.bind_identity(drive.identity())
    }

    pub(super) fn verify_computation_ready(
        &self,
        ready: novarocks_query_application::api::ComputationReadyToken,
    ) -> Result<(), TaskExecutionError> {
        if self.activation != Some(ready.activation()) || self.shared.frozen.root != ready.root() {
            return Err(protocol_error(
                "computation cut differs from the original Native drive and root",
            ));
        }
        Ok(())
    }
    fn bind_identity(
        &mut self,
        activation: AttemptActivationIdentity,
    ) -> Result<(), TaskExecutionError> {
        if activation.execution() != self.shared.frozen.execution
            || self.activation.is_some_and(|held| held != activation)
        {
            return Err(protocol_error(
                "Native drive differs from this frozen protocol activation",
            ));
        }
        self.activation = Some(activation);
        Ok(())
    }
    fn unchanged(&self, owner_revision: u64, sealed: bool) -> bool {
        self.last_owner_revision == Some(owner_revision) && self.last_sealed == sealed
    }
    pub(super) fn publish(
        &mut self,
        execution: &mut QueryTaskExecution,
        sealed: bool,
    ) -> Result<bool, TaskExecutionError> {
        // A never-run dormant owner must still converge without publishing a
        // guessed binding or an empty-success view.
        if self.activation.is_none() {
            return Ok(false);
        }
        let owner_revision = execution.accepted_protocol_revision();
        if self.unchanged(owner_revision, sealed) {
            return Ok(false);
        }
        if execution.graph().execution_id() != self.shared.frozen.execution
            || execution.graph().root_identity() != self.shared.frozen.root
        {
            return Err(protocol_error(
                "protocol publisher was installed on a different Task graph",
            ));
        }
        let changed = execution.accepted_protocol_task_changes();
        let mut updates = Vec::with_capacity(changed.len());
        for &task_id in changed {
            let task = execution.task(task_id).ok_or_else(|| {
                protocol_error("changed accepted Task is absent from its frozen graph")
            })?;
            let writer_witness = execution.accepted_protocol_writer_witness(task_id).cloned();
            let status = task.status().map(|status| {
                if let Some(witness) = writer_witness.as_ref()
                    && witness.version() == status.version()
                {
                    Arc::clone(witness)
                } else {
                    Arc::new(status.clone())
                }
            });
            updates.push(Update {
                index: self.shared.frozen.index(task.identity())?,
                status,
                writer_witness,
            });
        }
        let published = self.publish_updates(
            owner_revision,
            updates,
            execution.selected_task_failure(),
            sealed,
        )?;
        execution.clear_accepted_protocol_task_changes();
        Ok(published)
    }
    fn publish_updates(
        &mut self,
        owner_revision: u64,
        updates: Vec<Update>,
        failure: Option<&SelectedTaskTermination>,
        sealed: bool,
    ) -> Result<bool, TaskExecutionError> {
        if self.unchanged(owner_revision, sealed) {
            return Ok(false);
        }
        let activation = self.activation.ok_or_else(|| {
            protocol_error("protocol facts are not bound to the actual Native drive")
        })?;
        if self
            .last_owner_revision
            .is_some_and(|previous| owner_revision < previous)
        {
            return Err(protocol_error("accepted protocol owner revision regressed"));
        }
        if let Some(failure) = failure {
            self.shared.frozen.index(failure.identity())?;
        }
        for update in &updates {
            let expected = self
                .shared
                .frozen
                .tasks
                .get(update.index)
                .ok_or_else(|| protocol_error("protocol update exceeds the frozen slot set"))?;
            if update
                .status
                .as_ref()
                .is_some_and(|status| status.identity() != *expected)
                || update.writer_witness.as_ref().is_some_and(|status| {
                    status.identity() != *expected || status.writer().is_none()
                })
            {
                return Err(protocol_error(
                    "protocol update contradicts its exact frozen Task slot",
                ));
            }
        }
        let revision = {
            let mut store = self
                .shared
                .store
                .lock()
                .map_err(|_| protocol_error("Task protocol slot transfer lock was poisoned"))?;
            let revision = store
                .revision
                .checked_add(1)
                .ok_or_else(|| protocol_error("accepted protocol revision exhausted"))?;
            for update in updates {
                let slot = &mut store.slots[update.index];
                slot.status = update.status;
                if slot.writer_witness.is_none() {
                    slot.writer_witness = update.writer_witness;
                }
                store.dirty.insert(update.index);
            }
            store.binding = Some(AttemptProtocolBinding {
                activation,
                root: self.shared.frozen.root,
                plan: self.shared.frozen.plan,
            });
            store.failure = failure.cloned();
            #[cfg(test)]
            {
                store.root_success_sealed = sealed;
            }
            store.revision = revision;
            revision
        };
        self.last_owner_revision = Some(owner_revision);
        self.last_sealed = sealed;
        self.revisions.send_replace(revision);
        Ok(true)
    }
}
impl AttemptProtocolFactsReceiver {
    pub(crate) fn take_changes(
        &mut self,
        activation: AttemptActivationIdentity,
        root: TaskIdentity,
        plan: PlanSeal,
    ) -> Result<Option<AcceptedAttemptProtocolChanges>, TaskExecutionError> {
        let mut store = self
            .shared
            .store
            .lock()
            .map_err(|_| protocol_error("Task protocol slot transfer lock was poisoned"))?;
        let Some(binding) = store.binding else {
            return Ok(None);
        };
        // Validate before touching the only consumer's dirty frontier.
        binding.verify(activation, root, plan)?;
        if self.last_read == Some(store.revision) {
            return Ok(None);
        }
        let initial = self.last_read.is_none();
        let indices = if initial {
            (0..store.slots.len()).collect::<Vec<_>>()
        } else {
            store.dirty.iter().copied().collect()
        };
        let tasks = indices
            .into_iter()
            .map(|index| {
                let slot = &store.slots[index];
                AcceptedTaskProtocolChange {
                    index,
                    identity: self.shared.frozen.tasks[index],
                    status: slot.status.clone(),
                    writer_witness: slot.writer_witness.clone(),
                }
            })
            .collect();
        let changes = AcceptedAttemptProtocolChanges {
            frozen: Arc::clone(&self.shared.frozen),
            binding,
            revision: store.revision,
            initial,
            tasks,
            failure: store.failure.clone(),
            #[cfg(test)]
            root_success_sealed: store.root_success_sealed,
        };
        store.dirty.clear();
        self.last_read = Some(store.revision);
        Ok(Some(changes))
    }
    pub(crate) async fn changed(
        &mut self,
        activation: AttemptActivationIdentity,
        root: TaskIdentity,
        plan: PlanSeal,
    ) -> Result<AcceptedAttemptProtocolChanges, TaskExecutionError> {
        loop {
            if let Some(changes) = self.take_changes(activation, root, plan)? {
                return Ok(changes);
            }
            // No lock or borrowed data crosses this await. Duplicate/coalesced
            // notification wakes simply recheck the scalar store revision.
            self.revisions
                .changed()
                .await
                .map_err(|_| protocol_error("the exact Task protocol fact publisher exited"))?;
        }
    }
}

/// Consumer-owned adapter around the original writer rules. It applies only
/// transferred deltas and never holds the protocol lock during validation.
#[derive(Debug)]
pub(crate) struct WriteProtocolTracker {
    frozen: Arc<FrozenAttemptProtocol>,
    binding: AttemptProtocolBinding,
    revision: u64,
    failure: Option<SelectedTaskTermination>,
    tracker: WriteCompletionTracker,
}
impl WriteProtocolTracker {
    pub(crate) fn from_initial(
        changes: &AcceptedAttemptProtocolChanges,
    ) -> Result<Self, TaskExecutionError> {
        if !changes.initial {
            return Err(protocol_error(
                "writer observation requires the original complete first read",
            ));
        }
        let mut tracker = Self {
            frozen: Arc::clone(&changes.frozen),
            binding: changes.binding,
            revision: 0,
            failure: None,
            tracker: WriteCompletionTracker::try_new(
                changes.frozen.root,
                changes.frozen.writers.iter().copied(),
                changes.frozen.summary_sources.iter().copied(),
            )?,
        };
        tracker.observe(changes)?;
        Ok(tracker)
    }
    pub(crate) fn observe(
        &mut self,
        changes: &AcceptedAttemptProtocolChanges,
    ) -> Result<(), TaskExecutionError> {
        if !Arc::ptr_eq(&self.frozen, &changes.frozen)
            || self.binding != changes.binding
            || changes.revision < self.revision
        {
            return Err(protocol_error(
                "writer protocol changes belong to another source or an older revision",
            ));
        }
        if changes.revision == self.revision {
            return Ok(());
        }
        for task in &changes.tasks {
            // Preserve the original undeclared-writer check even if a
            // contradictory later metric snapshot omitted its earlier facts.
            if let Some(witness) = &task.writer_witness {
                self.tracker.observe_status(witness);
            }
            if let Some(status) = &task.status {
                self.tracker.observe_status(status);
            }
        }
        self.failure = changes.failure.clone();
        self.revision = changes.revision;
        Ok(())
    }
    pub(crate) fn selected_failure(&self) -> Option<&SelectedTaskTermination> {
        self.failure.as_ref()
    }
    pub(crate) fn execution_verdict(&self) -> WriteVerdict {
        self.tracker.execution_verdict(self.failure.is_some())
    }
}
fn protocol_error(message: &'static str) -> TaskExecutionError {
    TaskExecutionError::Schedule(message.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_execution_contract::{
        AbortCause, TaskOutputFacts, TaskState, TaskStatusVersion, TaskWriterFacts,
        TerminationDetail,
    };
    use novarocks_physical_plan::PlanVersionId;
    use novarocks_types::{AttemptId, BackendProcessId, QueryId, StageId};
    fn identity(task: u32) -> TaskIdentity {
        TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(91, 7), AttemptId::new(1).unwrap()).unwrap(),
            StageId::new(1).unwrap(),
            TaskId::new(task).unwrap(),
            BackendProcessId::try_from_bytes([
                1, 1, 1, 1, 1, 1, 0x71, 1, 0x81, 1, 1, 1, 1, 1, 1, 1,
            ])
            .unwrap(),
        )
    }
    fn plan() -> PlanSeal {
        PlanSeal::Version(PlanVersionId::try_new([2; 16]).unwrap())
    }
    fn activation(generation: u64) -> AttemptActivationIdentity {
        AttemptActivationIdentity::from_parts(71, identity(1).query_execution_id(), generation)
    }
    fn status(task: u32, version: u64) -> TaskStatus {
        TaskStatus::try_new(
            identity(task),
            TaskStatusVersion::new(version).unwrap(),
            TaskState::Finished,
            None,
            TaskOutputFacts::new(true),
        )
        .unwrap()
    }
    fn running(task: u32, version: u64) -> TaskStatus {
        TaskStatus::try_new(
            identity(task),
            TaskStatusVersion::new(version).unwrap(),
            TaskState::Running,
            None,
            TaskOutputFacts::default(),
        )
        .unwrap()
    }
    fn pair(writers: &[u32]) -> (AttemptProtocolFactsPublisher, AttemptProtocolFactsReceiver) {
        pair_with_summary_sources(writers, &[])
    }
    fn pair_with_summary_sources(
        writers: &[u32],
        summary_sources: &[u32],
    ) -> (AttemptProtocolFactsPublisher, AttemptProtocolFactsReceiver) {
        AttemptProtocolFactsPublisher::pair(
            FrozenAttemptProtocol::from_tasks(
                plan(),
                identity(1),
                [identity(1), identity(2), identity(3)].into(),
                writers.iter().map(|task| identity(*task)).collect(),
                summary_sources.iter().map(|task| identity(*task)).collect(),
            )
            .unwrap(),
        )
    }
    fn update(task: u32, status: TaskStatus) -> Update {
        let status = Arc::new(status);
        Update {
            index: task as usize - 1,
            writer_witness: status.writer().map(|_| Arc::clone(&status)),
            status: Some(status),
        }
    }
    fn take(receiver: &mut AttemptProtocolFactsReceiver) -> AcceptedAttemptProtocolChanges {
        receiver
            .take_changes(activation(1), identity(1), plan())
            .unwrap()
            .unwrap()
    }
    #[test]
    fn summary_sources_are_an_independent_frozen_task_obligation() {
        let (mut publisher, mut receiver) = pair_with_summary_sources(&[2], &[3]);
        publisher.bind_identity(activation(1)).unwrap();
        publisher
            .publish_updates(
                1,
                vec![update(1, status(1, 2)), update(2, status(2, 2))],
                None,
                false,
            )
            .unwrap();
        let mut tracker = WriteProtocolTracker::from_initial(&take(&mut receiver)).unwrap();
        assert_eq!(
            tracker.execution_verdict(),
            WriteVerdict::TaskNotFinished {
                task: identity(3),
                state: TaskState::Planned,
            }
        );
        publisher
            .publish_updates(2, vec![update(3, running(3, 2))], None, false)
            .unwrap();
        publisher
            .publish_updates(3, vec![update(3, status(3, 3))], None, false)
            .unwrap();
        tracker.observe(&take(&mut receiver)).unwrap();
        assert_eq!(tracker.execution_verdict(), WriteVerdict::Complete);
    }

    #[test]
    fn summary_source_membership_does_not_authorize_writer_facts() {
        let (mut publisher, mut receiver) = pair_with_summary_sources(&[2], &[3]);
        publisher.bind_identity(activation(1)).unwrap();
        publisher
            .publish_updates(
                1,
                vec![
                    update(1, status(1, 2)),
                    update(2, status(2, 2)),
                    update(3, running(3, 2).with_writer(TaskWriterFacts::empty())),
                ],
                None,
                false,
            )
            .unwrap();
        publisher
            .publish_updates(2, vec![update(3, status(3, 3))], None, false)
            .unwrap();
        let tracker = WriteProtocolTracker::from_initial(&take(&mut receiver)).unwrap();
        assert_eq!(
            tracker.execution_verdict(),
            WriteVerdict::UndeclaredWriter(identity(3))
        );
    }

    #[test]
    fn frozen_summary_sources_must_belong_to_the_exact_task_graph() {
        assert!(
            FrozenAttemptProtocol::from_tasks(
                plan(),
                identity(1),
                [identity(1), identity(2)].into(),
                [identity(2)].into(),
                [identity(3)].into(),
            )
            .is_err()
        );
        let original = identity(2);
        let foreign = TaskIdentity::new(
            original.query_execution_id(),
            original.stage_id(),
            original.task_id(),
            BackendProcessId::new_v7(),
        );
        assert!(
            FrozenAttemptProtocol::from_tasks(
                plan(),
                identity(1),
                [identity(1), identity(2)].into(),
                [identity(2)].into(),
                [foreign].into(),
            )
            .is_err()
        );
    }

    #[test]
    fn stale_binding_cannot_rebind_or_consume_the_current_dirty_frontier() {
        let (mut publisher, mut receiver) = pair(&[2]);
        assert!(publisher.publish_updates(0, vec![], None, false).is_err());
        publisher.bind_identity(activation(1)).unwrap();
        publisher.publish_updates(0, vec![], None, false).unwrap();
        assert!(
            receiver
                .take_changes(activation(2), identity(1), plan())
                .is_err()
        );
        assert!(
            receiver
                .take_changes(activation(1), identity(2), plan())
                .is_err()
        );
        assert!(
            receiver
                .take_changes(
                    activation(1),
                    identity(1),
                    PlanSeal::Version(PlanVersionId::try_new([3; 16]).unwrap())
                )
                .is_err()
        );
        assert_eq!(take(&mut receiver).tasks.len(), 3);
        assert!(publisher.bind_identity(activation(2)).is_err());
    }
    #[test]
    fn first_read_covers_missing_writers_and_deltas_preserve_undeclared_evidence() {
        let (mut publisher, mut receiver) = pair(&[2]);
        publisher.bind_identity(activation(1)).unwrap();
        publisher
            .publish_updates(1, vec![update(1, status(1, 2))], None, false)
            .unwrap();
        let initial = take(&mut receiver);
        let mut tracker = WriteProtocolTracker::from_initial(&initial).unwrap();
        assert_eq!(
            tracker.execution_verdict(),
            WriteVerdict::TaskNotFinished {
                task: identity(2),
                state: TaskState::Planned
            }
        );
        publisher
            .publish_updates(
                2,
                vec![
                    update(2, status(2, 2)),
                    update(3, running(3, 2).with_writer(TaskWriterFacts::empty())),
                ],
                None,
                false,
            )
            .unwrap();
        // Coalescing cannot erase the exact accepted writer witness.
        publisher
            .publish_updates(3, vec![update(3, status(3, 3))], None, false)
            .unwrap();
        let delta = take(&mut receiver);
        assert_eq!(delta.tasks.len(), 2);
        tracker.observe(&delta).unwrap();
        assert_eq!(
            tracker.execution_verdict(),
            WriteVerdict::UndeclaredWriter(identity(3))
        );
        let (mut publisher, mut receiver) = pair(&[]);
        publisher.bind_identity(activation(1)).unwrap();
        publisher.publish_updates(0, vec![], None, false).unwrap();
        assert!(WriteProtocolTracker::from_initial(&take(&mut receiver)).is_err());
    }
    #[test]
    fn failure_only_refinement_and_true_seal_are_in_the_same_revision_as_changes() {
        let (mut publisher, mut receiver) = pair(&[2]);
        publisher.bind_identity(activation(1)).unwrap();
        publisher
            .publish_updates(
                1,
                vec![update(1, status(1, 2)), update(2, status(2, 2))],
                None,
                false,
            )
            .unwrap();
        let mut tracker = WriteProtocolTracker::from_initial(&take(&mut receiver)).unwrap();
        assert!(tracker.execution_verdict() == WriteVerdict::Complete);
        publisher.publish_updates(1, vec![], None, true).unwrap();
        let sealed = take(&mut receiver);
        assert!(sealed.tasks.is_empty());
        assert!(sealed.root_success_sealed());
        let derived = SelectedTaskTermination::new(
            identity(3),
            TerminationDetail::Aborted(AbortCause::PeerTaskFailed),
        );
        publisher
            .publish_updates(2, vec![], Some(&derived), true)
            .unwrap();
        let origin = SelectedTaskTermination::new(
            identity(3),
            TerminationDetail::Aborted(AbortCause::QueryFailed),
        );
        publisher
            .publish_updates(3, vec![], Some(&origin), true)
            .unwrap();
        let delta = take(&mut receiver);
        assert!(delta.tasks.is_empty());
        assert_eq!(delta.selected_failure(), Some(&origin));
        assert!(delta.root_success_sealed());
        tracker.observe(&delta).unwrap();
        assert_eq!(tracker.execution_verdict(), WriteVerdict::AttemptFailed);
    }
    #[test]
    fn idle_has_no_slot_copy_and_many_changes_coalesce_to_one_latest_slot() {
        let (mut publisher, mut receiver) = pair(&[2]);
        publisher.bind_identity(activation(1)).unwrap();
        publisher.publish_updates(0, vec![], None, false).unwrap();
        take(&mut receiver);
        assert!(!publisher.publish_updates(0, vec![], None, false).unwrap());
        assert!(
            receiver
                .take_changes(activation(1), identity(1), plan())
                .unwrap()
                .is_none()
        );
        for revision in 1..=10 {
            publisher
                .publish_updates(
                    revision,
                    vec![update(2, running(2, revision + 1))],
                    None,
                    false,
                )
                .unwrap();
        }
        let delta = take(&mut receiver);
        assert_eq!(delta.tasks.len(), 1);
        assert_eq!(
            delta.tasks[0].status().unwrap().version(),
            TaskStatusVersion::new(11).unwrap()
        );
        assert!(publisher.shared.store.lock().unwrap().dirty.is_empty());
    }
    #[test]
    fn statistics_observer_keeps_complete_frontier_and_original_success_rules() {
        use crate::query_execution::statistics::completion::StatisticsProtocolTracker;
        use novarocks_execution_contract::CancelReason;
        let (mut publisher, mut receiver) = pair(&[]);
        publisher.bind_identity(activation(1)).unwrap();
        publisher
            .publish_updates(1, vec![update(1, status(1, 2))], None, false)
            .unwrap();
        let initial = take(&mut receiver);
        let mut tracker = StatisticsProtocolTracker::from_initial(&initial).unwrap();
        assert!(!tracker.all_terminal());
        assert!(!tracker.all_success_compatible());
        let canceling = TaskStatus::try_new(
            identity(2),
            TaskStatusVersion::new(2).unwrap(),
            TaskState::Canceling,
            Some(TerminationDetail::Canceled(
                CancelReason::UpstreamNoLongerNeeded,
            )),
            TaskOutputFacts::default(),
        )
        .unwrap();
        publisher
            .publish_updates(
                2,
                vec![update(2, canceling), update(3, status(3, 2))],
                None,
                false,
            )
            .unwrap();
        tracker.observe(&take(&mut receiver)).unwrap();
        assert!(!tracker.all_terminal());
        let cancelled = TaskStatus::try_new(
            identity(2),
            TaskStatusVersion::new(3).unwrap(),
            TaskState::Canceled,
            Some(TerminationDetail::Canceled(
                CancelReason::UpstreamNoLongerNeeded,
            )),
            TaskOutputFacts::default(),
        )
        .unwrap();
        publisher
            .publish_updates(3, vec![update(2, cancelled)], None, false)
            .unwrap();
        let compatible = take(&mut receiver);
        tracker.observe(&compatible).unwrap();
        tracker.observe(&compatible).unwrap();
        assert!(tracker.all_terminal());
        assert!(tracker.all_success_compatible());
        assert!(tracker.observe(&initial).is_err());
        let failure = SelectedTaskTermination::new(
            identity(3),
            TerminationDetail::Aborted(AbortCause::QueryFailed),
        );
        publisher
            .publish_updates(4, vec![], Some(&failure), false)
            .unwrap();
        tracker.observe(&take(&mut receiver)).unwrap();
        assert!(tracker.all_terminal());
        assert!(!tracker.all_success_compatible());
        assert_eq!(tracker.selected_failure(), Some(&failure));
        let (mut foreign, mut foreign_receiver) = pair(&[]);
        foreign.bind_identity(activation(1)).unwrap();
        foreign.publish_updates(0, vec![], None, false).unwrap();
        assert!(tracker.observe(&take(&mut foreign_receiver)).is_err());
    }

    #[test]
    fn statistics_incompatible_terminal_tasks_refuse_without_a_selected_failure() {
        use crate::query_execution::statistics::completion::StatisticsProtocolTracker;
        use novarocks_execution_contract::{SafeDetail, TaskFailure};

        for termination in [
            TerminationDetail::Aborted(AbortCause::QueryFailed),
            TerminationDetail::Failed(TaskFailure::new(
                novarocks_execution_contract::failure::FailureFact::new(
                    novarocks_execution_contract::failure::FailureClass::Resource,
                    novarocks_execution_contract::failure::FailureReason::new(
                        std::num::NonZeroU16::new(u16::MAX).unwrap(),
                        std::num::NonZeroU16::new(5).unwrap(),
                    ),
                ),
                SafeDetail::new("memory pool exhausted").unwrap(),
            )),
        ] {
            let (mut publisher, mut receiver) = pair(&[]);
            publisher.bind_identity(activation(1)).unwrap();
            publisher
                .publish_updates(
                    1,
                    vec![
                        update(1, status(1, 2)),
                        update(2, running(2, 2)),
                        update(3, status(3, 2)),
                    ],
                    None,
                    false,
                )
                .unwrap();
            let mut tracker =
                StatisticsProtocolTracker::from_initial(&take(&mut receiver)).unwrap();
            assert!(!tracker.all_terminal());
            assert!(!tracker.all_success_compatible());

            let state = match &termination {
                TerminationDetail::Aborted(_) => TaskState::Aborted,
                TerminationDetail::Failed(_) => TaskState::Failed,
                _ => unreachable!("only incompatible terminal cases"),
            };
            let terminal = TaskStatus::try_new(
                identity(2),
                TaskStatusVersion::new(3).unwrap(),
                state,
                Some(termination.clone()),
                TaskOutputFacts::default(),
            )
            .unwrap();
            publisher
                .publish_updates(2, vec![update(2, terminal)], None, false)
                .unwrap();
            let terminal = take(&mut receiver);
            assert_eq!(terminal.tasks().len(), 1);
            assert_eq!(terminal.tasks()[0].identity(), identity(2));
            assert_eq!(
                terminal.tasks()[0].status().unwrap().termination(),
                Some(&termination),
            );
            tracker.observe(&terminal).unwrap();
            assert!(tracker.all_terminal());
            assert_eq!(tracker.selected_failure(), None);
            assert!(!tracker.all_success_compatible());
        }
    }

    #[test]
    fn replacement_pair_cannot_update_an_original_consumer_tracker() {
        let (mut first, mut original) = pair(&[2]);
        first.bind_identity(activation(1)).unwrap();
        first.publish_updates(0, vec![], None, false).unwrap();
        let mut tracker = WriteProtocolTracker::from_initial(&take(&mut original)).unwrap();
        let (mut next, mut replacement) = pair(&[2]);
        next.bind_identity(activation(1)).unwrap();
        next.publish_updates(0, vec![], None, false).unwrap();
        assert!(tracker.observe(&take(&mut replacement)).is_err());
    }
}
