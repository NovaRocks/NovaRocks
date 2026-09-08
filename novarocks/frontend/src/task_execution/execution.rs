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

//! The composition root of one query execution attempt's task protocol.
//!
//! It owns the graph, one stage execution per stage, one query context owner
//! per context, the bounded dispatcher, the cross-stage edge-open decision,
//! and the serial status runner. Everything it does is driven by one thread
//! at a time: `pump` produces intents and releases bounded batches,
//! `acknowledge` settles one released operation, and `apply_status` applies
//! what the intake queued. Nothing here opens a connection.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use novarocks_execution::task_execution::{
    AbortCause, AttemptDrainFacts, DispatchBudget, GoneObservation, LatchOutcome, OperationKind,
    QueryContextRef, StageState, StatusObservation, TaskDomainUpdate, TaskIdentity,
    TaskOperationId, TaskState, TaskStatus, TaskStatusCursor, TerminationDetail, TerminationLatch,
    TransportBudget, UpdateQueryContext, parent_released_children,
};
use novarocks_types::identity::{StageId, TaskId};

use super::clock::TaskProtocolClock;
use super::completion::{ReadCompletionTracker, ReadVerdict};
use novarocks_proto_codec::lifecycle::terminal::QueryTerminalProfileContributionTelemetry;
use novarocks_types::identity::BackendProcessId;

use super::context_owner::{ContextEstablishSource, QueryContextOwner, ReleaseSettlement};
use super::dispatch::{ExpiredOperation, OperationDispatcher};
use super::error::TaskExecutionError;
use super::graph::TaskGraph;
use super::intent::{DispatchBatch, OperationAcknowledgement, OperationIntent, TaskOperationSink};
use super::remote_task::{
    CreateSettlement, RemoteTask, TaskTerminalReport, UpdateAdmission, UpdateSettlement,
};
use super::stage::{EdgeOpenTracker, StageExecution};
use super::status_intake::{StatusEvent, StatusIntake};

/// Which owner settles one released operation.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum OperationTarget {
    Task {
        stage: StageId,
        task: TaskId,
    },
    Context(QueryContextRef),
    /// One shared-domain advance whose progression belongs to an attempt-local
    /// owner outside this state machine.
    ///
    /// The credential domain is the only such owner: the frontend is the
    /// credential principal, so the epoch it advances is minted from a provider
    /// this state machine must not know about. Settling one here releases the
    /// dispatch permit and nothing else; the owner learns the outcome through
    /// the runner's acknowledgement-observer seam, which sees every
    /// acknowledgement before it is settled.
    ContextDomain(QueryContextRef),
}

/// What one pump released.
#[derive(Clone, Debug, Default)]
pub struct PumpReport {
    pub batches: usize,
    pub operations: usize,
    /// Operations the dispatcher dropped for outliving their queue residence
    /// bound.
    ///
    /// Deliberately observational, and deliberately without a consumer that
    /// decides anything. It reads like an unread verdict -- it is not: the
    /// owner that minted the operation is still waiting for its outcome and
    /// re-mints it on its own retry, so dropping the queued copy is what gives
    /// that retry a clean slot. Failing the attempt here instead was measured
    /// as `distributed-resilience` 16/16 -> 4/16, because an acknowledgement
    /// deliberately dropped by a fault is exactly the case whose recovery is a
    /// replay of a queued operation.
    pub expired: Vec<ExpiredOperation>,
}

/// What applying intake changed.
#[derive(Clone, Debug, Default)]
pub struct StatusReport {
    pub accepted: usize,
    pub ignored: usize,
    pub terminal: Vec<TaskTerminalReport>,
    /// The status transport must be resubscribed with the per-task cursors.
    pub resubscribe: bool,
}

/// The release-carried runtime-filter contributions of one attempt.
///
/// The observable supply point of this loop: `contributions().len()` against
/// `contexts()` says whether the carrier is being fed at all. A query that ran
/// runtime filters and reports zero contributions over three contexts is a
/// carrier that is not wired, which no per-part test can show.
#[derive(Clone, Debug, PartialEq)]
pub struct ReleasedRuntimeFilterContributions {
    contributions: Vec<(BackendProcessId, QueryTerminalProfileContributionTelemetry)>,
    complete: bool,
    contexts: usize,
}

impl ReleasedRuntimeFilterContributions {
    pub fn contributions(
        &self,
    ) -> &[(BackendProcessId, QueryTerminalProfileContributionTelemetry)] {
        &self.contributions
    }

    /// Whether every context this attempt placed has answered its release.
    pub const fn is_complete(&self) -> bool {
        self.complete
    }

    /// How many query contexts this attempt placed.
    pub const fn contexts(&self) -> usize {
        self.contexts
    }
}

/// The task protocol of one query execution attempt.
#[derive(Debug)]
pub struct QueryTaskExecution {
    graph: TaskGraph,
    stages: BTreeMap<StageId, StageExecution>,
    stage_of_task: BTreeMap<TaskId, StageId>,
    owners: BTreeMap<QueryContextRef, QueryContextOwner>,
    dispatcher: OperationDispatcher,
    edges: EdgeOpenTracker,
    clock: Arc<dyn TaskProtocolClock>,
    sink: Arc<dyn TaskOperationSink>,
    intake: StatusIntake,
    operation_targets: BTreeMap<TaskOperationId, OperationTarget>,
    status_reconciliations: BTreeSet<QueryContextRef>,
    failure: TerminationLatch,
    read: ReadCompletionTracker,
    drained_tasks: BTreeSet<TaskId>,
    released_outputs: BTreeSet<TaskId>,
    released_children_of: BTreeSet<StageId>,
}

impl QueryTaskExecution {
    /// Builds the attempt's owners from its frozen graph.
    pub fn new(
        graph: TaskGraph,
        budget: DispatchBudget,
        transport: TransportBudget,
        clock: Arc<dyn TaskProtocolClock>,
        sink: Arc<dyn TaskOperationSink>,
        intake: StatusIntake,
    ) -> Result<Self, TaskExecutionError> {
        let edges = EdgeOpenTracker::from_graph(&graph);
        let root_task = graph.root_task();
        let mut owners = BTreeMap::<QueryContextRef, QueryContextOwner>::new();
        let mut tasks_per_context = BTreeMap::<QueryContextRef, usize>::new();
        for task in graph.tasks() {
            *tasks_per_context.entry(task.context()).or_default() += 1;
        }
        for (&context, &tasks) in &tasks_per_context {
            owners.insert(context, QueryContextOwner::new(context, tasks));
        }

        let mut dispatcher = OperationDispatcher::new(budget, transport);
        let (graph, descriptors) = graph.into_descriptors();
        let mut stage_tasks = BTreeMap::<StageId, BTreeMap<TaskId, RemoteTask>>::new();
        let mut stage_of_task = BTreeMap::<TaskId, StageId>::new();
        for (task_id, descriptor) in descriptors {
            let node = graph
                .task(task_id)
                .ok_or_else(|| TaskExecutionError::Schedule(format!("task {task_id} is absent")))?;
            dispatcher.register_task(node.identity().backend_process_id())?;
            stage_of_task.insert(task_id, node.stage_id());
            stage_tasks.entry(node.stage_id()).or_default().insert(
                task_id,
                RemoteTask::new(descriptor, node.context(), Vec::new())?,
            );
        }

        let mut stages = BTreeMap::<StageId, StageExecution>::new();
        for (stage_id, tasks) in stage_tasks {
            let node = graph.stage(stage_id).ok_or_else(|| {
                TaskExecutionError::Schedule(format!("stage {stage_id} is absent"))
            })?;
            let root = tasks.contains_key(&root_task).then_some(root_task);
            stages.insert(
                stage_id,
                StageExecution::new(node.stage(), node.fragment_id(), root, tasks)?,
            );
        }

        let read = ReadCompletionTracker::new(graph.root_identity());
        Ok(Self {
            graph,
            stages,
            stage_of_task,
            owners,
            dispatcher,
            edges,
            clock,
            sink,
            intake,
            operation_targets: BTreeMap::new(),
            status_reconciliations: BTreeSet::new(),
            failure: TerminationLatch::open(),
            read,
            drained_tasks: BTreeSet::new(),
            released_outputs: BTreeSet::new(),
            released_children_of: BTreeSet::new(),
        })
    }

    pub const fn graph(&self) -> &TaskGraph {
        &self.graph
    }

    pub const fn dispatcher(&self) -> &OperationDispatcher {
        &self.dispatcher
    }

    pub const fn intake(&self) -> &StatusIntake {
        &self.intake
    }

    pub fn stage(&self, stage_id: StageId) -> Option<&StageExecution> {
        self.stages.get(&stage_id)
    }

    pub fn owner(&self, context: QueryContextRef) -> Option<&QueryContextOwner> {
        self.owners.get(&context)
    }

    /// Takes contexts whose task-operation acknowledgement requires an
    /// immediate status resubscription from the frontend's held cursors.
    pub fn take_status_reconciliations(&mut self) -> BTreeSet<QueryContextRef> {
        std::mem::take(&mut self.status_reconciliations)
    }

    /// Every backend's sealed runtime-filter observation, as its release
    /// reported it, with whether the set is complete.
    ///
    /// A context contributes an entry only if its release settled *and*
    /// carried a contribution. The two reasons an entry is missing are not the
    /// same fact and are not collapsed here: a context that released without
    /// one installed no participant on that backend, which is an ordinary
    /// query with no runtime filter there; a context that never released has
    /// an answer still outstanding. `is_complete` is the second question, and
    /// it is what stops an outstanding release from reading as an absent
    /// filter.
    pub fn released_runtime_filter_contributions(&self) -> ReleasedRuntimeFilterContributions {
        ReleasedRuntimeFilterContributions {
            contributions: self
                .owners
                .iter()
                .filter_map(|(context, owner)| {
                    owner
                        .runtime_filter_contribution()
                        .map(|telemetry| (context.backend_process_id(), telemetry.clone()))
                })
                .collect(),
            complete: self.owners.values().all(QueryContextOwner::is_released),
            contexts: self.owners.len(),
        }
    }

    pub fn task(&self, task_id: TaskId) -> Option<&RemoteTask> {
        self.stage_of_task
            .get(&task_id)
            .and_then(|stage_id| self.stages.get(stage_id))
            .and_then(|stage| stage.task(task_id))
    }

    pub fn stage_states(&self) -> BTreeMap<StageId, StageState> {
        self.stages
            .iter()
            .map(|(&stage_id, stage)| (stage_id, stage.state()))
            .collect()
    }

    /// Produces every intent the owners currently have and releases bounded
    /// batches to the sink.
    ///
    /// Order matters only for latency, never for correctness: every owner
    /// refuses to hand out a request twice, so the same call made in a
    /// different order releases the same set.
    pub fn pump(
        &mut self,
        establish: &dyn ContextEstablishSource,
    ) -> Result<PumpReport, TaskExecutionError> {
        let now = self.clock.now();
        let mut report = PumpReport {
            expired: self.dispatcher.drain_expired(now),
            ..PumpReport::default()
        };

        let mut lifecycle = Vec::<(OperationTarget, OperationIntent)>::new();
        for (&context, owner) in &mut self.owners {
            if !owner.needs_establish() {
                continue;
            }
            if let Some(intent) = owner.establish_intent(establish.facts_for(context)?, now)? {
                lifecycle.push((OperationTarget::Context(context), intent));
            }
        }
        let mut work = Vec::<(OperationTarget, OperationIntent)>::new();
        for (&stage_id, stage) in &mut self.stages {
            for (&task_id, task) in stage.tasks_mut() {
                let target = OperationTarget::Task {
                    stage: stage_id,
                    task: task_id,
                };
                if let Some(intent) = task.create_intent() {
                    work.push((target, intent));
                }
                if let Some(intent) = task.next_update_intent()? {
                    work.push((target, intent));
                }
            }
        }
        for (&context, owner) in &mut self.owners {
            if let Some(intent) = owner.renew_intent(now)? {
                lifecycle.push((OperationTarget::Context(context), intent));
            }
            if let Some(intent) = owner.release_intent(now) {
                lifecycle.push((OperationTarget::Context(context), intent));
            }
        }

        // Lifecycle intents are admitted first so a create burst cannot fill
        // the shared queue bound ahead of a renewal or a release.
        for (target, intent) in lifecycle.into_iter().chain(work) {
            self.operation_targets.insert(intent.operation_id(), target);
            self.dispatcher.enqueue(intent, now)?;
        }

        while let Some(batch) = self.dispatcher.take_batch() {
            report.batches += 1;
            report.operations += batch.operations().len();
            self.sink.submit(&batch);
        }
        Ok(report)
    }

    /// Records one domain fact for one task.
    ///
    /// This is the seam the split-assignment and runtime-filter owners use.
    /// The fact reaches the wire only when the task itself may send.
    pub fn enqueue_task_update(
        &mut self,
        task_id: TaskId,
        update: TaskDomainUpdate,
    ) -> Result<UpdateAdmission, TaskExecutionError> {
        let stage_id = *self
            .stage_of_task
            .get(&task_id)
            .ok_or(TaskExecutionError::UnknownOperation)?;
        self.stages
            .get_mut(&stage_id)
            .and_then(|stage| stage.task_mut(task_id))
            .ok_or(TaskExecutionError::UnknownOperation)?
            .enqueue_update(update)
    }

    /// Releases one shared-domain advance an attempt-local owner minted.
    ///
    /// This is the seam the credential rotation owner sends through. It takes
    /// only an advance: an establish and a renewal belong to the context owner,
    /// which mints their lease sequences, and letting a domain owner send one
    /// would put two producers on the lease progression.
    ///
    /// The operation is enqueued rather than sent, so it obeys the same bounded
    /// per-backend dispatch as everything else and cannot jump a queue that is
    /// already full of this backend's creates.
    pub fn enqueue_context_domain(
        &mut self,
        request: UpdateQueryContext,
    ) -> Result<TaskOperationId, TaskExecutionError> {
        let UpdateQueryContext::AdvanceDomain(advance) = &request else {
            return Err(TaskExecutionError::Schedule(
                "only a shared-domain advance may be sent by a domain owner".to_owned(),
            ));
        };
        let context = advance.context();
        if !self.owners.contains_key(&context) {
            return Err(TaskExecutionError::UnknownOperation);
        }
        let operation_id = advance.envelope().operation_id();
        let now = self.clock.now();
        self.operation_targets
            .insert(operation_id, OperationTarget::ContextDomain(context));
        self.dispatcher
            .enqueue(OperationIntent::UpdateQueryContext(Arc::new(request)), now)?;
        Ok(operation_id)
    }

    /// Forces one context down, ahead of everything queued for it.
    pub fn abort_context(
        &mut self,
        context: QueryContextRef,
        cause: AbortCause,
    ) -> Result<Option<DispatchBatch>, TaskExecutionError> {
        let now = self.clock.now();
        let Some(intent) = self
            .owners
            .get_mut(&context)
            .and_then(|owner| owner.abort_intent(cause))
        else {
            return Ok(None);
        };
        self.operation_targets
            .insert(intent.operation_id(), OperationTarget::Context(context));
        self.dispatcher.enqueue_priority(intent, now)?;
        let batch = self.dispatcher.take_batch();
        if let Some(batch) = &batch {
            self.sink.submit(batch);
        }
        Ok(batch)
    }

    /// Settles one released operation.
    pub fn acknowledge(
        &mut self,
        ack: &OperationAcknowledgement,
    ) -> Result<(), TaskExecutionError> {
        let target = self
            .operation_targets
            .remove(&ack.operation_id())
            .ok_or(TaskExecutionError::UnknownOperation)?;
        self.dispatcher.settle(ack.operation_id())?;
        match target {
            OperationTarget::Task { stage, task } => self.acknowledge_task(stage, task, ack),
            OperationTarget::Context(context) => self.acknowledge_context(context, ack),
            // The permit is already released above. The verdict belongs to the
            // domain owner, which read this acknowledgement from the observer
            // seam before the runner got here, so applying it a second time
            // would be a second authority over the same progression.
            OperationTarget::ContextDomain(_) => Ok(()),
        }
    }

    fn acknowledge_task(
        &mut self,
        stage_id: StageId,
        task_id: TaskId,
        ack: &OperationAcknowledgement,
    ) -> Result<(), TaskExecutionError> {
        let stage = self
            .stages
            .get_mut(&stage_id)
            .ok_or(TaskExecutionError::UnknownOperation)?;
        let task = stage
            .task_mut(task_id)
            .ok_or(TaskExecutionError::UnknownOperation)?;
        let context = task.context();
        match ack.kind() {
            OperationKind::CreateTask => {
                let identity = task.identity();
                let context = task.context();
                let settlement = task.on_create_ack(ack)?;
                if !matches!(settlement, CreateSettlement::Created) {
                    if let CreateSettlement::FailedClosed(outcome) = settlement {
                        return Err(TaskExecutionError::OperationFailed {
                            kind: OperationKind::CreateTask,
                            outcome,
                            detail: ack.detail().map(|d| d.as_str().to_owned()),
                        });
                    }
                    // A genuinely unknown outcome leaves the identical request
                    // to be handed out again by the next pump.
                    return Ok(());
                }
                if let Some(owner) = self.owners.get_mut(&context) {
                    owner.note_create_acknowledged();
                }
                self.open_ready_edges(identity)?;
                self.settle_task_drain(stage_id, task_id);
                Ok(())
            }
            OperationKind::UpdateTask => match task.on_update_ack(ack)? {
                UpdateSettlement::FailedClosed(outcome) => {
                    Err(TaskExecutionError::OperationFailed {
                        kind: OperationKind::UpdateTask,
                        outcome,
                        detail: ack.detail().map(|d| d.as_str().to_owned()),
                    })
                }
                UpdateSettlement::AwaitingTerminalStatus => {
                    self.status_reconciliations.insert(context);
                    Ok(())
                }
                _ => Ok(()),
            },
            OperationKind::CancelTask => task.on_cancel_ack(ack),
            kind => Err(TaskExecutionError::MissingReceipt(kind)),
        }
    }

    fn acknowledge_context(
        &mut self,
        context: QueryContextRef,
        ack: &OperationAcknowledgement,
    ) -> Result<(), TaskExecutionError> {
        let now = self.clock.now();
        let owner = self
            .owners
            .get_mut(&context)
            .ok_or(TaskExecutionError::UnknownOperation)?;
        match ack.kind() {
            OperationKind::UpdateQueryContext => owner.on_context_ack(ack),
            OperationKind::ReleaseQueryContext => {
                owner
                    .on_release_ack(ack, now)
                    .and_then(|settlement| match settlement {
                        ReleaseSettlement::FailedClosed(outcome) => {
                            Err(TaskExecutionError::OperationFailed {
                                kind: OperationKind::ReleaseQueryContext,
                                outcome,
                                detail: ack.detail().map(|d| d.as_str().to_owned()),
                            })
                        }
                        _ => Ok(()),
                    })
            }
            OperationKind::AbortQueryContext => owner.on_abort_ack(ack),
            kind => Err(TaskExecutionError::MissingReceipt(kind)),
        }
    }

    /// Records the edge-open decision every edge this destination completed.
    ///
    /// The fact is recorded on each producer's own task. A producer that is
    /// still `Creating` queues it, so the decision may be made early while the
    /// wire request still waits for that producer's own acknowledgement.
    fn open_ready_edges(&mut self, destination: TaskIdentity) -> Result<(), TaskExecutionError> {
        for edge_id in self.edges.note_created(destination) {
            let producers = self.edges.producers_of(edge_id).to_vec();
            tracing::debug!(
                edge = %edge_id,
                destination = %destination,
                producers = producers.len(),
                "exchange edge decided; recording the open on every producer"
            );
            for producer in producers {
                // A producer this decision cannot reach never opens its edge,
                // and its sink then waits for permission for the rest of the
                // query. Losing that silently is what makes the resulting hang
                // unattributable, so each miss is reported.
                let Some(stage) = self.stages.get_mut(&producer.stage_id()) else {
                    tracing::warn!(
                        edge = %edge_id,
                        producer = %producer,
                        "edge open cannot reach a producer whose stage is absent"
                    );
                    continue;
                };
                let Some(task) = stage.task_mut(producer.task_id()) else {
                    tracing::warn!(
                        edge = %edge_id,
                        producer = %producer,
                        "edge open cannot reach a producer absent from its own stage"
                    );
                    continue;
                };
                task.enqueue_edge_open(edge_id)?;
                tracing::debug!(
                    edge = %edge_id,
                    producer = %producer,
                    "edge open recorded on its producer"
                );
            }
        }
        Ok(())
    }

    /// Applies what the intake queued, then propagates one layer of stage
    /// release.
    pub fn apply_status(&mut self, max_events: usize) -> Result<StatusReport, TaskExecutionError> {
        let mut report = StatusReport::default();
        let events = {
            let Some(mut runner) = self.intake.try_enter() else {
                return Ok(report);
            };
            report.resubscribe = runner.take_observation_loss();
            runner.drain(max_events)
        };
        for event in events {
            match event {
                StatusEvent::Published(status) => {
                    self.apply_published(&status, &mut report)?;
                }
                StatusEvent::Gone(identity) => {
                    let Some(task) = self.task_by_identity(identity) else {
                        continue;
                    };
                    if matches!(task.observe_gone(), GoneObservation::TerminalNeverObserved) {
                        return Err(TaskExecutionError::Observation(
                            StatusObservation::TerminalOverwrite,
                        ));
                    }
                }
            }
        }
        self.propagate_stage_release()?;
        Ok(report)
    }

    fn apply_published(
        &mut self,
        status: &TaskStatus,
        report: &mut StatusReport,
    ) -> Result<(), TaskExecutionError> {
        let identity = status.identity();
        let Some((stage_id, task_id)) = self.locate(identity) else {
            return Err(TaskExecutionError::UnknownOperation);
        };
        let stage = self
            .stages
            .get_mut(&stage_id)
            .expect("the stage was just located");
        let task = stage.task_mut(task_id).expect("the task was just located");
        match task.observe_status(status)? {
            StatusObservation::Accept => report.accepted += 1,
            _ => report.ignored += 1,
        }
        if let Some(terminal) = task.terminal_report() {
            if let Some(detail) = &terminal.termination
                && !detail.is_success_compatible()
                && matches!(self.failure.latch(detail.clone()), LatchOutcome::Won)
            {
                // The first cause wins and is the one every observer sees.
            }
            report.terminal.push(terminal);
        }
        self.settle_task_drain(stage_id, task_id);
        Ok(())
    }

    /// Publishes one task's drain facts to its context owner, once each.
    fn settle_task_drain(&mut self, stage_id: StageId, task_id: TaskId) {
        let Some(stage) = self.stages.get(&stage_id) else {
            return;
        };
        let Some(task) = stage.task(task_id) else {
            return;
        };
        let context = task.context();
        let terminal = task.is_terminal();
        // Only a task that FINISHED owes an output responsibility; that state
        // is constructible on the backend precisely when the responsibility is
        // complete. A task that terminated any other way -- canceled because
        // it was no longer needed, aborted, or failed -- has none to complete
        // and will never publish one.
        //
        // The owner's counter has to agree with that, or it never reaches
        // `expected_tasks`, `locally_drained` stays false, and the release is
        // never even requested: the context then holds its backend resources
        // until the lease expires while the frontend waits out its whole drain
        // budget. This is the per-context half of the same fact the
        // attempt-level `all_output_released` checks; the two are derived
        // separately on purpose, so both have to say it.
        let output_settled =
            terminal && (task.task_state() != TaskState::Finished || task.output_released());
        let Some(owner) = self.owners.get_mut(&context) else {
            return;
        };
        if terminal && self.drained_tasks.insert(task_id) {
            owner.note_task_drained();
        }
        if output_settled && self.released_outputs.insert(task_id) {
            owner.note_output_released();
        }
    }

    /// Cancels the children of every stage that has stopped consuming.
    ///
    /// Exactly one layer per call: a child only releases its own producers
    /// once its own derived state says it has stopped consuming, which needs
    /// its tasks to have actually stood down.
    fn propagate_stage_release(&mut self) -> Result<(), TaskExecutionError> {
        let released = self
            .stages
            .iter()
            .filter(|(stage_id, stage)| {
                parent_released_children(stage.state())
                    && !self.released_children_of.contains(stage_id)
            })
            .map(|(&stage_id, _)| stage_id)
            .collect::<Vec<_>>();
        let now = self.clock.now();
        for stage_id in released {
            self.released_children_of.insert(stage_id);
            let children = self.graph.producer_stages(stage_id).collect::<Vec<_>>();
            for child in children {
                let Some(stage) = self.stages.get_mut(&child) else {
                    continue;
                };
                let intents = stage.cancel_non_root_tasks();
                for intent in intents {
                    let task = match &intent {
                        OperationIntent::CancelTask(request) => request.identity().task_id(),
                        _ => continue,
                    };
                    self.operation_targets.insert(
                        intent.operation_id(),
                        OperationTarget::Task { stage: child, task },
                    );
                    self.dispatcher.enqueue(intent, now)?;
                }
            }
        }
        Ok(())
    }

    /// Records one packet the root result data plane delivered.
    ///
    /// This is the frontend's own evidence about its result stream, and it is
    /// the only thing that can satisfy the end-of-stream half of a read
    /// completion. A backend's claim that its output responsibility is
    /// complete is a different fact, published on a different channel, and it
    /// cannot substitute for what this process received.
    pub fn consume_root_result_packet(
        &mut self,
        root: TaskIdentity,
        packet_sequence: u64,
        end_of_stream: bool,
    ) -> Result<(), TaskExecutionError> {
        self.read
            .consume_packet(root, packet_sequence, end_of_stream)
    }

    /// Whether the client may be told the read is complete, and why not when
    /// it may not.
    ///
    /// This deliberately does not wait for upstream tasks to finish standing
    /// down, and it does not wait for any context to be released. Draining is
    /// internal resource closure and never gates a completion that has
    /// already been linearized.
    pub fn read_completion(&self) -> ReadVerdict {
        let root_state = self
            .task_by_identity_ref(self.read.root())
            .map_or(TaskState::Planned, RemoteTask::task_state);
        self.read.verdict(root_state, self.failure.is_latched())
    }

    /// Whether the client may be told the read is complete.
    pub fn client_visible_completion(&self) -> bool {
        self.read_completion().is_complete()
    }

    /// Whether this frontend consumed the end of the root result stream.
    pub fn root_end_of_stream_observed(&self) -> bool {
        self.read.end_of_stream_observed()
    }

    /// Whether the whole attempt has drained.
    pub fn attempt_drained(&self) -> bool {
        self.attempt_drain_facts().drained()
    }

    /// The three facts a drain waits on, separately.
    ///
    /// A conjunction that fails has to be able to say which conjunct failed.
    /// Reporting only "not drained" cost a full cluster run to narrow the one
    /// time it mattered, and the three have entirely different causes: tasks
    /// not terminal is the backends still working, output not released is this
    /// frontend not having consumed what they produced, and contexts not
    /// released is the release operation itself outstanding.
    pub fn attempt_drain_facts(&self) -> AttemptDrainFacts {
        AttemptDrainFacts::new(
            self.stages.values().all(StageExecution::all_terminal),
            self.stages
                .values()
                .all(StageExecution::all_output_released),
            self.owners.values().all(QueryContextOwner::is_released),
        )
    }

    /// Where every task of one context has been observed to.
    ///
    /// This is what a subscription starts from, so a transport that dropped
    /// resumes rather than replaying a task's whole version history.
    pub fn status_cursors(&self, context: QueryContextRef) -> Vec<TaskStatusCursor> {
        self.stages
            .values()
            .flat_map(StageExecution::tasks)
            .filter(|(_, task)| task.context() == context)
            .map(|(_, task)| task.cursor())
            .collect()
    }

    /// The first termination cause of this attempt, if one was latched.
    pub fn failure_cause(&self) -> Option<&TerminationDetail> {
        self.failure.cause()
    }

    fn locate(&self, identity: TaskIdentity) -> Option<(StageId, TaskId)> {
        let stage_id = *self.stage_of_task.get(&identity.task_id())?;
        if stage_id != identity.stage_id() {
            return None;
        }
        let stage = self.stages.get(&stage_id)?;
        let task = stage.task(identity.task_id())?;
        task.identity().verify_matches(identity).ok()?;
        Some((stage_id, identity.task_id()))
    }

    fn task_by_identity(&mut self, identity: TaskIdentity) -> Option<&mut RemoteTask> {
        let (stage_id, task_id) = self.locate(identity)?;
        self.stages.get_mut(&stage_id)?.task_mut(task_id)
    }

    fn task_by_identity_ref(&self, identity: TaskIdentity) -> Option<&RemoteTask> {
        let (stage_id, task_id) = self.locate(identity)?;
        self.stages.get(&stage_id)?.task(task_id)
    }
}
