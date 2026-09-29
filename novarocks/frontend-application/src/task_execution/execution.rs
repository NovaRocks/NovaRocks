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

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;

use novarocks_execution::task_execution::{
    AbortCause, AdmissionEpochCapability, CancelReason, ExchangeEdgeId, OperationKind,
    QueryContextRef, TaskDomainUpdate, TaskIdentity, TaskOperationId, TaskState, TaskStatus,
    TaskStatusCursor, TerminationDetail, UpdateQueryContext,
};
use novarocks_execution::task_execution::{
    QueryContextConvergenceCursor, QueryContextConvergenceReceipt, QuiesceQueryContextReceipt,
    TaskConvergenceCursor, TaskConvergenceReceipt,
};
use novarocks_query_application::coordination::{
    AttemptDrainFacts, DispatchBudget, DispatchLane, GoneObservation, LatchOutcome,
    MonotonicInstant, OperationDispatchResult, ReplacementWorkerAdmissionEvidence, StageState,
    StatusObservation, TerminationLatch, parent_released_children,
};
use novarocks_task_codec::TransportBudget;
use novarocks_task_codec::operation::{
    CoveredStatusStreamEvent, CoveredStatusStreamFact, DecodedCoveredSubscription,
    QuiesceObservationCursor,
};
use novarocks_types::NativeCompatibilityId;
use novarocks_types::identity::{StageId, TaskId};

use super::clock::TaskProtocolClock;
use super::completion::{ReadCompletionTracker, ReadVerdict};
use novarocks_proto_codec::lifecycle::terminal::QueryTerminalProfileContributionTelemetry;
use novarocks_query_application::coordination::AcceptedRootControlRequest;
use novarocks_types::identity::BackendProcessId;

use super::context_owner::{ContextEstablishSource, QueryContextOwner, ReleaseSettlement};
use super::dispatch::{DispatchOperationState, LocalQueueCapacity, OperationDispatcher};
use super::error::{CapacityBound, TaskExecutionError};
use super::graph::TaskGraph;
use super::intent::{
    DispatchBatch, OperationAcknowledgement, OperationIntent, TaskOperationQueueAdmission,
    TaskOperationQueuePermit, TaskOperationQueueRequest, TaskOperationSink, TaskOperationSubmit,
};
use super::remote_task::{
    CreateSettlement, RemoteTask, TaskTerminalReport, UpdateAdmission, UpdateSettlement,
};
use super::stage::{EdgeOpenTracker, EdgeReadyDecision, StageExecution};
use super::status_intake::{
    ObservationFrame, ObservationIntakeEntry, StatusEvent, StatusIntake, StatusIntakeEntry,
};

#[derive(Debug, Default)]
struct CoveredContextObservation {
    generation: u64,
    initial_complete: bool,
    bookmark_sequence: u64,
    applied_prefix: u64,
    source_cut: u64,
    actual_stopped: BTreeMap<TaskIdentity, TaskConvergenceReceipt>,
    context_convergence: Option<QueryContextConvergenceReceipt>,
    quiesce: Option<QuiesceQueryContextReceipt>,
    /// An accepted fact that could not establish its task's required terminal
    /// evidence. Only a later complete catch-up with positive facts clears it.
    missing_terminal: BTreeSet<TaskIdentity>,
    gap_generation: Option<u64>,
    gap_since: Option<MonotonicInstant>,
    coverage_debt: Option<(u64, MonotonicInstant)>,
}

/// A frozen edge decision waiting for its producer's bounded process queue.
/// The decision stays here until the producer has retained the domain intent.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum EdgeControlEffect {
    Close {
        producer: TaskIdentity,
        edge: ExchangeEdgeId,
        destination: TaskIdentity,
    },
    Open {
        producer: TaskIdentity,
        edge: ExchangeEdgeId,
    },
}

impl EdgeControlEffect {
    const fn producer(self) -> TaskIdentity {
        match self {
            Self::Close { producer, .. } | Self::Open { producer, .. } => producer,
        }
    }
}

impl CoveredContextObservation {
    fn begin_generation(&mut self, generation: u64) {
        if generation <= self.generation {
            return;
        }
        self.generation = generation;
        self.initial_complete = false;
        self.bookmark_sequence = 0;
        self.applied_prefix = 0;
        self.source_cut = 0;
    }

    fn note_status_gap(&mut self, identity: TaskIdentity, now: MonotonicInstant) {
        self.missing_terminal.insert(identity);
        self.gap_generation.get_or_insert(self.generation);
        self.gap_since.get_or_insert(now);
    }

    fn update_coverage_debt(&mut self, now: MonotonicInstant) {
        if self
            .coverage_debt
            .is_some_and(|(target, _)| self.applied_prefix >= target)
        {
            self.coverage_debt = None;
        }
        if self.source_cut > self.applied_prefix && self.coverage_debt.is_none() {
            self.coverage_debt = Some((self.source_cut, now));
        }
    }

    fn gap_pending(&self) -> bool {
        self.gap_generation.is_some()
    }
}

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
    /// An actor-owned Abort projected into this attempt's Native lifecycle
    /// lane. The dispatcher owns only carrier capacity; TaskRound settles the
    /// actor's exact effect from the same acknowledgement intake.
    ActorAbort,
}

/// One owner transition waiting to become a dispatcher entry.
///
/// A task-domain update may already carry the process reservation acquired
/// before it entered `RemoteTask::pending`; every other candidate obtains its
/// reservation in the same transaction that releases its owner marker.
#[derive(Debug)]
/// One candidate of an admission pass.
///
/// A minted candidate is an intent its owner already handed out; a pass that
/// does not admit it gives it back through `rollback_unsent`, exactly as a
/// refused reservation always has. A create or update candidate is only a
/// position: the task is asked what the send would cost, and nothing is taken
/// from it -- no create frozen, no pending update or its reservation removed --
/// until admission holds the capacity for it.
enum AdmissionCandidate {
    /// Boxed so that the many create and update positions of one pass stay
    /// small; only lifecycle operations are minted ahead of admission.
    Minted {
        target: OperationTarget,
        intent: Box<OperationIntent>,
    },
    Create {
        stage: StageId,
        task: TaskId,
    },
    Update {
        stage: StageId,
        task: TaskId,
    },
}

impl AdmissionCandidate {
    fn minted(target: OperationTarget, intent: OperationIntent) -> Self {
        Self::Minted {
            target,
            intent: Box::new(intent),
        }
    }
}

/// Why an admission pass stopped before its last candidate.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum AdmissionPassEnd {
    /// A process-wide transport window was full.
    ProcessTransport,
    /// This attempt's own total queue bound was full.
    AttemptQueue,
}

/// How one admission pass went.
///
/// A full target skipped only its own later candidates; a pass end skipped
/// everything after it. Both leave every skipped candidate exactly where it
/// was, so the next pass resumes in the same order.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AdmissionPass {
    pub admitted: usize,
    pub full_targets: BTreeSet<BackendProcessId>,
    pub ended: Option<AdmissionPassEnd>,
    /// Whether a skipped candidate was left unadmitted. A caller that must
    /// know every candidate went out -- a stage release -- reads this.
    pub skipped: bool,
}

impl AdmissionPass {
    const fn complete(&self) -> bool {
        !self.skipped
    }
}

/// What one candidate's admission did.
enum CandidateAdmission {
    Admitted,
    /// The candidate had nothing to send after all.
    Nothing,
    TargetFull,
    DeploymentWindowFull,
    Ended(AdmissionPassEnd),
}

/// What one pump released.
#[derive(Debug, Default)]
pub struct PumpReport {
    pub batches: usize,
    pub operations: usize,
    /// How this pump's admission pass went.
    pub admission: AdmissionPass,
}

/// What applying intake changed.
#[derive(Debug, Default)]
pub struct StatusReport {
    pub accepted: usize,
    pub ignored: usize,
    pub terminal: Vec<TaskTerminalReport>,
    /// The status transport must be resubscribed with the per-task cursors.
    pub resubscribe: bool,
    /// The first success-seal request in the same intake order. Entries after
    /// it remain queued as residual status work.
    pub root_control: Option<AcceptedRootControlRequest>,
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
    covered_observation_active: bool,
    covered_observation: BTreeMap<QueryContextRef, CoveredContextObservation>,
    inbound_producers: BTreeMap<QueryContextRef, BTreeSet<TaskIdentity>>,
    tasks_by_context: BTreeMap<QueryContextRef, Vec<TaskIdentity>>,
    deployment_window_limits: BTreeMap<BackendProcessId, usize>,
    deployment_window: BTreeMap<BackendProcessId, BTreeSet<TaskIdentity>>,
    /// The frozen graph bounds this by sum(producers * (destinations + 1)):
    /// the edge tracker emits each close member and each open only once.
    edge_control_effects: VecDeque<EdgeControlEffect>,
    operation_targets: BTreeMap<TaskOperationId, OperationTarget>,
    expired_actor_aborts: Vec<TaskOperationId>,
    status_reconciliations: BTreeSet<QueryContextRef>,
    covered_reconciliations: BTreeSet<QueryContextRef>,
    normal_drain_started: bool,
    terminal_cleanup_started: bool,
    failure: TerminationLatch,
    read: ReadCompletionTracker,
    required_evidence_deadlines: BTreeMap<TaskIdentity, MonotonicInstant>,
    required_result_terminal_control_deadline: Option<MonotonicInstant>,
    drained_tasks: BTreeSet<TaskId>,
    released_outputs: BTreeSet<TaskId>,
    released_children_of: BTreeSet<StageId>,
}

/// What happened when an urgent context abort reached transport admission.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum AbortSubmission {
    /// The context owner had already minted or completed its abort.
    NoIntent,
    /// The transport owns this abort and will publish its acknowledgement.
    Accepted,
    /// The abort never entered an owner or dispatcher queue. The
    /// process-level transport supervisor owns the capacity-change wake that
    /// will schedule its next turn.
    Backpressured,
}

/// Real dispatcher and process-transport capacity reserved for one exact
/// actor-owned Abort preview.
#[derive(Debug)]
pub(crate) struct ActorAbortReservation {
    operation_id: TaskOperationId,
    context: QueryContextRef,
    queue_permit: Box<dyn TaskOperationQueuePermit>,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum ActorAbortDispatchState {
    Queued,
    InFlight,
}

/// How an acknowledgement relates to an exact context operation replay which
/// has not crossed the current generation's transport boundary yet.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum QueuedContextAcknowledgement {
    NotQueued,
    IgnoreOlderTransportUnknown,
    ApplyDefinitive,
}

impl QueryTaskExecution {
    /// Classifies a context acknowledgement against the dispatch owner before
    /// acknowledgement observers consume it.
    ///
    /// After an unknown outcome, an exact replay may already be queued when an
    /// older generation answers. Another unknown fact says nothing about the
    /// queued generation and is ignored. A definitive Worker answer closes the
    /// exact operation and must cancel that definitely-unsent replay before the
    /// domain owner applies the answer.
    pub(crate) fn classify_queued_context_acknowledgement(
        &self,
        acknowledgement: &OperationAcknowledgement,
    ) -> Result<QueuedContextAcknowledgement, TaskExecutionError> {
        if !matches!(
            acknowledgement.kind(),
            OperationKind::AcquireQueryContextAdmissionTicket | OperationKind::UpdateQueryContext
        ) || !matches!(
            self.operation_targets.get(&acknowledgement.operation_id()),
            Some(OperationTarget::Context(_))
        ) {
            return Ok(QueuedContextAcknowledgement::NotQueued);
        }
        if self
            .dispatcher
            .operation_state(acknowledgement.operation_id())?
            != DispatchOperationState::Queued
        {
            return Ok(QueuedContextAcknowledgement::NotQueued);
        }
        Ok(
            if acknowledgement.dispatch_result() != OperationDispatchResult::TransportUnknown {
                QueuedContextAcknowledgement::ApplyDefinitive
            } else {
                QueuedContextAcknowledgement::IgnoreOlderTransportUnknown
            },
        )
    }

    /// Cancels the current definitely-unsent replay and applies the definitive
    /// settled acknowledgement which arrived from an older transport generation.
    pub(crate) fn acknowledge_queued_context_replay(
        &mut self,
        acknowledgement: &OperationAcknowledgement,
    ) -> Result<(), TaskExecutionError> {
        if acknowledgement.dispatch_result() == OperationDispatchResult::TransportUnknown {
            return Err(TaskExecutionError::Schedule(
                "a queued context replay can be closed only by a definitive acknowledgement"
                    .to_owned(),
            ));
        }
        let operation_id = acknowledgement.operation_id();
        let target = *self
            .operation_targets
            .get(&operation_id)
            .ok_or(TaskExecutionError::UnknownOperation)?;
        let OperationTarget::Context(context) = target else {
            return Err(TaskExecutionError::UnknownOperation);
        };
        let intent = self.dispatcher.cancel_queued(operation_id)?;
        if intent.operation_id() != operation_id
            || intent.kind() != acknowledgement.kind()
            || intent.backend_process_id() != context.backend_process_id()
        {
            return Err(TaskExecutionError::Schedule(
                "queued context replay differs from its definitive acknowledgement address"
                    .to_owned(),
            ));
        }
        self.operation_targets.remove(&operation_id);
        self.acknowledge_context(context, acknowledgement)
    }

    /// Builds the attempt's owners from its frozen graph.
    pub fn new(
        graph: TaskGraph,
        budget: DispatchBudget,
        transport: TransportBudget,
        native_compatibility_id: NativeCompatibilityId,
        admission_epochs: &BTreeMap<BackendProcessId, AdmissionEpochCapability>,
        preparing_positions: &BTreeMap<BackendProcessId, usize>,
        clock: Arc<dyn TaskProtocolClock>,
        sink: Arc<dyn TaskOperationSink>,
        intake: StatusIntake,
    ) -> Result<Self, TaskExecutionError> {
        let participants = graph
            .contexts()
            .map(|context| context.backend_process_id())
            .collect::<BTreeSet<_>>();
        if preparing_positions.keys().copied().collect::<BTreeSet<_>>() != participants
            || preparing_positions
                .values()
                .any(|&positions| positions < budget.create_permits())
        {
            return Err(TaskExecutionError::Schedule(
                "frozen backend preparation capacities must cover the configured deployment window for every exact participant"
                    .to_owned(),
            ));
        }
        let deployment_window_limits = preparing_positions
            .iter()
            .map(|(&process, _)| (process, budget.create_permits()))
            .collect();
        let edges = EdgeOpenTracker::from_graph(&graph);
        let mut exchange_destinations = BTreeMap::<
            TaskId,
            Vec<(
                novarocks_execution::task_execution::ExchangeEdgeId,
                TaskIdentity,
            )>,
        >::new();
        let mut inbound_producers = BTreeMap::<QueryContextRef, BTreeSet<TaskIdentity>>::new();
        for edge in graph.edges() {
            for destination in edge.destinations() {
                let context = graph
                    .task(destination.task_id())
                    .ok_or(TaskExecutionError::UnknownOperation)?
                    .context();
                inbound_producers
                    .entry(context)
                    .or_default()
                    .extend(edge.producers().iter().copied());
            }
            for producer in edge.producers() {
                let destinations = exchange_destinations.entry(producer.task_id()).or_default();
                destinations.extend(
                    edge.destinations()
                        .iter()
                        .copied()
                        .map(|destination| (edge.edge_id(), destination)),
                );
            }
        }
        let root_task = graph.root_task();
        let mut owners = BTreeMap::<QueryContextRef, QueryContextOwner>::new();
        let mut tasks_per_context = BTreeMap::<QueryContextRef, usize>::new();
        let mut tasks_by_context = BTreeMap::<QueryContextRef, Vec<TaskIdentity>>::new();
        for task in graph.tasks() {
            *tasks_per_context.entry(task.context()).or_default() += 1;
            tasks_by_context
                .entry(task.context())
                .or_default()
                .push(task.identity());
        }
        for (&context, &tasks) in &tasks_per_context {
            let admission_epoch_capability = admission_epochs
                .get(&context.backend_process_id())
                .copied()
                .ok_or_else(|| {
                    TaskExecutionError::Schedule(format!(
                        "backend {} has no frozen admission epoch capability",
                        context.backend_process_id()
                    ))
                })?;
            owners.insert(
                context,
                QueryContextOwner::new(
                    context,
                    tasks,
                    native_compatibility_id,
                    admission_epoch_capability,
                ),
            );
        }

        let mut dispatcher = OperationDispatcher::new(budget, transport);
        let (graph, seeds) = graph.into_seeds();
        let mut stage_tasks = BTreeMap::<StageId, BTreeMap<TaskId, RemoteTask>>::new();
        let mut stage_of_task = BTreeMap::<TaskId, StageId>::new();
        for (task_id, seed) in seeds {
            let node = graph
                .task(task_id)
                .ok_or_else(|| TaskExecutionError::Schedule(format!("task {task_id} is absent")))?;
            if seed.identity() != node.identity() || seed.context() != node.context() {
                return Err(TaskExecutionError::Schedule(format!(
                    "task {task_id} creation seed names another task or context"
                )));
            }
            dispatcher.register_task(node.identity().backend_process_id())?;
            stage_of_task.insert(task_id, node.stage_id());
            stage_tasks.entry(node.stage_id()).or_default().insert(
                task_id,
                RemoteTask::new(
                    seed,
                    exchange_destinations.remove(&task_id).unwrap_or_default(),
                )?,
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
        let covered_observation = graph
            .contexts()
            .copied()
            .map(|context| (context, CoveredContextObservation::default()))
            .collect();
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
            covered_observation_active: false,
            covered_observation,
            inbound_producers,
            tasks_by_context,
            deployment_window_limits,
            deployment_window: BTreeMap::new(),
            edge_control_effects: VecDeque::new(),
            operation_targets: BTreeMap::new(),
            expired_actor_aborts: Vec::new(),
            status_reconciliations: BTreeSet::new(),
            covered_reconciliations: BTreeSet::new(),
            normal_drain_started: false,
            terminal_cleanup_started: false,
            failure: TerminationLatch::open(),
            read,
            required_evidence_deadlines: BTreeMap::new(),
            required_result_terminal_control_deadline: None,
            drained_tasks: BTreeSet::new(),
            released_outputs: BTreeSet::new(),
            released_children_of: BTreeSet::new(),
        })
    }

    /// Installs the complete admission set acquired by a qualified
    /// replacement. Missing, duplicate, or foreign evidence is rejected
    /// before the first Task-protocol turn.
    pub(crate) fn adopt_replacement_admissions(
        &mut self,
        admissions: Box<[ReplacementWorkerAdmissionEvidence]>,
    ) -> Result<(), TaskExecutionError> {
        let expected = self.owners.keys().copied().collect::<BTreeSet<_>>();
        let actual = admissions
            .iter()
            .map(ReplacementWorkerAdmissionEvidence::context)
            .collect::<BTreeSet<_>>();
        if admissions.len() != actual.len() || actual != expected {
            return Err(TaskExecutionError::Schedule(
                "qualified replacement admission set differs from the Task manifest contexts"
                    .to_owned(),
            ));
        }
        let now = self.clock.now();
        for admission in admissions.iter() {
            self.owners
                .get_mut(&admission.context())
                .expect("the complete replacement admission set was validated")
                .adopt_replacement_admission(admission.receipt(), now)?;
        }
        Ok(())
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

    pub(crate) fn take_covered_reconciliations(&mut self) -> BTreeSet<QueryContextRef> {
        std::mem::take(&mut self.covered_reconciliations)
    }

    pub(crate) fn take_expired_actor_aborts(&mut self) -> Vec<TaskOperationId> {
        std::mem::take(&mut self.expired_actor_aborts)
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
        self.refresh_deployment_window();
        let now = self.clock.now();
        let expired = self.dispatcher.drain_expired(now);
        let mut first_expiry = None;
        for expired in expired {
            let operation_id = expired.operation_id();
            let kind = expired.kind();
            let waited = expired.waited();
            let target = self
                .operation_targets
                .remove(&operation_id)
                .ok_or(TaskExecutionError::UnknownOperation)?;
            // The dispatcher never accepted this operation, so no remote
            // effect is possible. Roll back its owner marker before failing
            // the attempt; cleanup can then mint the cancellation it owes.
            if target == OperationTarget::ActorAbort {
                self.expired_actor_aborts.push(operation_id);
            } else {
                self.rollback_unsent(target, operation_id);
            }
            first_expiry.get_or_insert(TaskExecutionError::QueueResidenceExpired {
                operation_id,
                kind,
                waited,
            });
        }
        if let Some(error) = first_expiry {
            return Err(error);
        }
        let mut report = PumpReport::default();
        if self.terminal_cleanup_started {
            // Terminal cleanup forbids new normal work, but the actor can
            // still enqueue Abort effects for contexts the Worker holds.
            // Those exact, capacity-reserved effects must cross transport.
            self.submit_queued_batches(&mut report)?;
            return Ok(report);
        }

        self.flush_edge_control_effects()?;

        let mut lifecycle = Vec::<AdmissionCandidate>::new();
        if !self.normal_drain_started {
            for (&context, owner) in &mut self.owners {
                if let Some(intent) = owner.admission_intent(now)? {
                    lifecycle.push(AdmissionCandidate::minted(
                        OperationTarget::Context(context),
                        intent,
                    ));
                }
                if !owner.needs_establish() {
                    continue;
                }
                if let Some(intent) = owner.establish_intent(establish.facts_for(context)?, now)? {
                    lifecycle.push(AdmissionCandidate::minted(
                        OperationTarget::Context(context),
                        intent,
                    ));
                }
            }
            for (&stage_id, stage) in &mut self.stages {
                for (&task_id, task) in stage.tasks_mut() {
                    if task.normally_stood_down()
                        && task.create_ownership_proven()
                        && let Some(intent) =
                            task.cancel_intent(CancelReason::UpstreamNoLongerNeeded)
                    {
                        lifecycle.push(AdmissionCandidate::minted(
                            OperationTarget::Task {
                                stage: stage_id,
                                task: task_id,
                            },
                            intent,
                        ));
                    }
                }
            }
        }
        // Creates and updates are positions only. A task is asked what its
        // send would cost when the pass reaches it, so a create that waits
        // behind a full target is neither frozen nor re-measured.
        let mut work = Vec::<AdmissionCandidate>::new();
        if !self.normal_drain_started {
            for (&stage_id, stage) in &self.stages {
                for &task_id in stage.tasks().map(|(task_id, _)| task_id) {
                    work.push(AdmissionCandidate::Create {
                        stage: stage_id,
                        task: task_id,
                    });
                    work.push(AdmissionCandidate::Update {
                        stage: stage_id,
                        task: task_id,
                    });
                }
            }
        } else {
            // Exact no-more-input effects remain live during normal drain.
            // Ordinary updates were discarded when the drain started.
            for (&stage_id, stage) in &self.stages {
                for &task_id in stage.tasks().map(|(task_id, _)| task_id) {
                    work.push(AdmissionCandidate::Update {
                        stage: stage_id,
                        task: task_id,
                    });
                }
            }
        }
        let release_ready = self
            .graph
            .contexts()
            .copied()
            .map(|context| {
                Ok::<_, TaskExecutionError>((
                    context,
                    self.receiver_inputs_stopped(context)?
                        && self.context_tasks_stopped(context)?,
                ))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        for (&context, owner) in &mut self.owners {
            if self.normal_drain_started {
                if let Some(intent) = owner.quiesce_intent() {
                    lifecycle.push(AdmissionCandidate::minted(
                        OperationTarget::Context(context),
                        intent,
                    ));
                }
            }
            if let Some(intent) = owner.renew_intent(now)? {
                lifecycle.push(AdmissionCandidate::minted(
                    OperationTarget::Context(context),
                    intent,
                ));
            }
            if release_ready.get(&context) == Some(&true)
                && let Some(intent) = owner.release_intent(now)
            {
                lifecycle.push(AdmissionCandidate::minted(
                    OperationTarget::Context(context),
                    intent,
                ));
            }
        }

        // Lifecycle intents are admitted first so a create burst cannot fill
        // the shared queue bound ahead of a renewal or a release.
        let candidates = lifecycle.into_iter().chain(work).collect::<VecDeque<_>>();
        report.admission = self.enqueue_candidates(candidates, now)?;

        self.submit_queued_batches(&mut report)?;
        Ok(report)
    }

    fn submit_queued_batches(&mut self, report: &mut PumpReport) -> Result<(), TaskExecutionError> {
        while let Some(batch) = self.dispatcher.take_batch() {
            let acceptance = batch.acceptance();
            let operations = batch.operations().len();
            match self.sink.try_submit(batch) {
                TaskOperationSubmit::Accepted => {
                    self.dispatcher.accept(acceptance)?;
                    report.batches += 1;
                    report.operations += operations;
                }
                TaskOperationSubmit::Backpressured(batch) => {
                    self.dispatcher.restore_backpressured(batch);
                    // A process-level supervisor wake will schedule another
                    // turn when capacity changes. Retrying in this turn would
                    // only rediscover the same full hard bound and spin.
                    break;
                }
                TaskOperationSubmit::Rejected { batch, reason } => {
                    self.rollback_rejected_batch(batch)?;
                    return Err(TaskExecutionError::Schedule(reason));
                }
            }
        }
        Ok(())
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
        if self.normal_drain_started || self.terminal_cleanup_started {
            return Err(TaskExecutionError::Schedule(
                "attempt drain rejects new task updates".to_owned(),
            ));
        }
        let stage_id = *self
            .stage_of_task
            .get(&task_id)
            .ok_or(TaskExecutionError::UnknownOperation)?;
        let backend = self
            .stages
            .get(&stage_id)
            .and_then(|stage| stage.task(task_id))
            .ok_or(TaskExecutionError::UnknownOperation)?
            .identity()
            .backend_process_id();
        let request = TaskOperationQueueRequest::task_update(backend, &update);
        self.dispatcher.validate_queue_request(request)?;
        let queue_permit = self
            .reserve_process_request(request)
            .ok_or_else(|| Self::process_queue_backpressure(request))?;
        self.stages
            .get_mut(&stage_id)
            .and_then(|stage| stage.task_mut(task_id))
            .ok_or(TaskExecutionError::UnknownOperation)?
            .enqueue_update(update, queue_permit)
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
        if self.normal_drain_started || self.terminal_cleanup_started {
            return Err(TaskExecutionError::Schedule(
                "attempt drain rejects new context-domain advances".to_owned(),
            ));
        }
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
        let intent = OperationIntent::UpdateQueryContext(Arc::new(request));
        let queue_permit = self
            .reserve_process_queue(&intent)?
            .ok_or_else(|| Self::process_queue_backpressure(intent.queue_request()))?;
        self.dispatcher
            .enqueue_reserved(intent, now, queue_permit)?;
        self.operation_targets
            .insert(operation_id, OperationTarget::ContextDomain(context));
        Ok(operation_id)
    }

    /// Reserves the two capacities an actor-owned Abort needs before its
    /// bounded adapter slot is released.
    ///
    /// The preview remains in the adapter while this runs. A successful
    /// result owns a real process queue permit, and the serial dispatcher has
    /// proved that the exact backend's lifecycle lane can advance now. The
    /// adapter slot is never treated as either authority.
    pub(crate) fn try_reserve_actor_abort(
        &self,
        intent: &OperationIntent,
    ) -> Result<Option<ActorAbortReservation>, TaskExecutionError> {
        let OperationIntent::AbortQueryContext(request) = intent else {
            return Err(TaskExecutionError::Schedule(
                "actor Abort intake preview is not an AbortQueryContext intent".to_owned(),
            ));
        };
        if !self.owners.contains_key(&request.context()) {
            return Err(TaskExecutionError::Schedule(format!(
                "actor Abort names context {} outside this attempt",
                request.context()
            )));
        }
        if self.operation_targets.contains_key(&intent.operation_id()) {
            return Err(TaskExecutionError::Schedule(format!(
                "actor Abort operation {} is already owned by this attempt",
                intent.operation_id()
            )));
        }
        if !self.dispatcher.priority_capacity_available(intent)? {
            return Ok(None);
        }
        let Some(queue_permit) = self.reserve_process_queue(intent)? else {
            return Ok(None);
        };
        Ok(Some(ActorAbortReservation {
            operation_id: intent.operation_id(),
            context: request.context(),
            queue_permit,
        }))
    }

    /// Commits one actor-owned Abort after its exact adapter effect is taken.
    pub(crate) fn enqueue_actor_abort(
        &mut self,
        intent: OperationIntent,
        reservation: ActorAbortReservation,
    ) -> Result<(), TaskExecutionError> {
        let OperationIntent::AbortQueryContext(request) = &intent else {
            return Err(TaskExecutionError::Schedule(
                "actor Abort carrier is not an AbortQueryContext intent".to_owned(),
            ));
        };
        if intent.operation_id() != reservation.operation_id
            || request.context() != reservation.context
        {
            return Err(TaskExecutionError::Schedule(
                "actor Abort carrier differs from its reserved preview".to_owned(),
            ));
        }
        let now = self.clock.now();
        self.dispatcher
            .enqueue_priority_reserved(intent, now, reservation.queue_permit)?;
        if self
            .operation_targets
            .insert(reservation.operation_id, OperationTarget::ActorAbort)
            .is_some()
        {
            return Err(TaskExecutionError::Schedule(
                "actor Abort operation ownership was replaced".to_owned(),
            ));
        }
        Ok(())
    }

    pub(crate) fn actor_abort_dispatch_state(
        &self,
        operation_id: TaskOperationId,
    ) -> Result<ActorAbortDispatchState, TaskExecutionError> {
        if self.operation_targets.get(&operation_id) != Some(&OperationTarget::ActorAbort) {
            return Err(TaskExecutionError::UnknownOperation);
        }
        match self.dispatcher.operation_state(operation_id)? {
            DispatchOperationState::Queued => Ok(ActorAbortDispatchState::Queued),
            DispatchOperationState::InFlight => Ok(ActorAbortDispatchState::InFlight),
            DispatchOperationState::Absent => Err(TaskExecutionError::UnknownOperation),
        }
    }

    /// Cancels a definitely-unsent actor Abort after another generation's
    /// definitive receipt closed the exact operation.
    pub(crate) fn cancel_queued_actor_abort(
        &mut self,
        operation_id: TaskOperationId,
    ) -> Result<(), TaskExecutionError> {
        if self.actor_abort_dispatch_state(operation_id)? != ActorAbortDispatchState::Queued {
            return Err(TaskExecutionError::UnknownOperation);
        }
        let intent = self.dispatcher.cancel_queued(operation_id)?;
        if intent.kind() != OperationKind::AbortQueryContext {
            return Err(TaskExecutionError::Schedule(
                "actor Abort dispatcher target retained a non-Abort carrier".to_owned(),
            ));
        }
        self.operation_targets.remove(&operation_id);
        Ok(())
    }

    /// Applies the positive closure fact from a definitive actor-owned Abort
    /// receipt. The actor owns the Abort issue and settlement; this method
    /// only projects that already-validated Worker fact to the matching
    /// context owner so normal attempt convergence can observe it.
    pub(crate) fn observe_actor_abort_context_closed(
        &mut self,
        context: QueryContextRef,
    ) -> Result<(), TaskExecutionError> {
        let owner = self
            .owners
            .get_mut(&context)
            .ok_or(TaskExecutionError::UnknownOperation)?;
        owner.observe_actor_abort_closure();
        Ok(())
    }

    /// Keeps observation and context closure live after a successful root
    /// seal while stopping unsent task and input work. In-flight operations
    /// may still settle; only exact destination-close controls may replay.
    pub(crate) fn begin_normal_drain(&mut self) {
        if self.normal_drain_started {
            return;
        }
        let destinations = self
            .graph
            .tasks()
            .map(|task| task.identity())
            .collect::<Vec<_>>();
        for destination in destinations {
            self.stand_down_task_normally(destination)
                .expect("the frozen task graph owns every destination");
        }
        self.normal_drain_started = true;
        let unsent_inputs = self
            .operation_targets
            .iter()
            .filter_map(|(&operation_id, &target)| {
                let queued_kind = self.dispatcher.queued_operation_kind(operation_id);
                (matches!(queued_kind, Some(OperationKind::CreateTask))
                    || (queued_kind == Some(OperationKind::UpdateTask)
                        && !self.dispatcher.queued_destination_close(operation_id))
                    || (target != OperationTarget::ActorAbort
                        && matches!(target, OperationTarget::ContextDomain(_))
                        && queued_kind.is_some()))
                .then_some((operation_id, target))
            })
            .collect::<Vec<_>>();
        for (operation_id, target) in unsent_inputs {
            let _ = self.dispatcher.cancel_queued(operation_id);
            self.operation_targets.remove(&operation_id);
            self.rollback_unsent(target, operation_id);
        }
    }

    /// Applies one exact local no-more-input authorization. An unknown Create
    /// outcome is kept for Context fencing, but cannot keep this destination
    /// in the producer's opening barrier or revive its normal input need.
    pub(crate) fn stand_down_task_normally(
        &mut self,
        identity: TaskIdentity,
    ) -> Result<(), TaskExecutionError> {
        let task = self
            .task_by_identity_ref(identity)
            .ok_or(TaskExecutionError::UnknownOperation)?;
        if task
            .status()
            .and_then(TaskStatus::termination)
            .is_none_or(|detail| detail.is_success_compatible())
        {
            self.close_destination_normally(identity);
        }
        let queued_inputs = self
            .operation_targets
            .iter()
            .filter_map(|(&operation_id, &target)| {
                let OperationTarget::Task { stage, task } = target else {
                    return None;
                };
                if stage != identity.stage_id() || task != identity.task_id() {
                    return None;
                }
                let kind = self.dispatcher.queued_operation_kind(operation_id);
                (kind == Some(OperationKind::CreateTask)
                    || (kind == Some(OperationKind::UpdateTask)
                        && !self.dispatcher.queued_destination_close(operation_id)))
                .then_some((operation_id, target))
            })
            .collect::<Vec<_>>();
        for (operation_id, target) in queued_inputs {
            let _ = self.dispatcher.cancel_queued(operation_id)?;
            self.operation_targets.remove(&operation_id);
            self.rollback_unsent(target, operation_id);
        }
        self.task_by_identity(identity)
            .ok_or(TaskExecutionError::UnknownOperation)?
            .begin_normal_stand_down();
        Ok(())
    }

    /// Stops the normal attempt lifecycle once the logical query has reached
    /// a terminal outcome. It preserves in-flight requests and actor Abort
    /// effects, because their Worker facts may still be useful for cleanup,
    /// while preventing them from causing a successor admission, establish,
    /// renewal, create, or update.
    pub(crate) fn begin_terminal_cleanup(&mut self) {
        if self.terminal_cleanup_started {
            return;
        }
        self.terminal_cleanup_started = true;
        self.deployment_window.clear();
        for stage in self.stages.values_mut() {
            for (_, task) in stage.tasks_mut() {
                task.discard_creation_after_attempt_failure();
            }
        }
        for owner in self.owners.values_mut() {
            owner.begin_terminal_cleanup();
        }
        self.status_reconciliations.clear();
        let normal_queued = self
            .operation_targets
            .iter()
            .filter_map(|(&operation_id, &target)| {
                (target != OperationTarget::ActorAbort
                    && self.dispatcher.operation_state(operation_id).ok()
                        == Some(DispatchOperationState::Queued))
                .then_some((operation_id, target))
            })
            .collect::<Vec<_>>();
        for (operation_id, target) in normal_queued {
            let _ = self.dispatcher.cancel_queued(operation_id);
            self.operation_targets.remove(&operation_id);
            self.rollback_unsent(target, operation_id);
        }
    }

    /// Forces one context down, ahead of everything queued for it.
    pub fn abort_context(
        &mut self,
        context: QueryContextRef,
        cause: AbortCause,
    ) -> Result<AbortSubmission, TaskExecutionError> {
        let now = self.clock.now();
        let Some(intent) = self
            .owners
            .get_mut(&context)
            .and_then(|owner| owner.abort_intent(cause))
        else {
            return Ok(AbortSubmission::NoIntent);
        };
        let operation_id = intent.operation_id();
        let Some(queue_permit) = self.reserve_process_queue(&intent)? else {
            self.rollback_unsent(OperationTarget::Context(context), operation_id);
            return Ok(AbortSubmission::Backpressured);
        };
        if let Err(error) = self
            .dispatcher
            .enqueue_priority_reserved(intent, now, queue_permit)
        {
            self.rollback_unsent(OperationTarget::Context(context), operation_id);
            return Err(error);
        }
        self.operation_targets
            .insert(operation_id, OperationTarget::Context(context));
        let Some(batch) = self
            .dispatcher
            .take_priority_lane_batch(context.backend_process_id(), DispatchLane::Lifecycle)
        else {
            return Ok(AbortSubmission::Backpressured);
        };
        let acceptance = batch.acceptance();
        match self.sink.try_submit(batch) {
            TaskOperationSubmit::Accepted => {
                self.dispatcher.accept(acceptance)?;
                Ok(AbortSubmission::Accepted)
            }
            TaskOperationSubmit::Backpressured(batch) => {
                self.dispatcher.restore_backpressured(batch);
                Ok(AbortSubmission::Backpressured)
            }
            TaskOperationSubmit::Rejected { batch, reason } => {
                self.rollback_rejected_batch(batch)?;
                Err(TaskExecutionError::Schedule(reason))
            }
        }
    }

    /// Settles one released operation.
    pub fn acknowledge(
        &mut self,
        ack: &OperationAcknowledgement,
    ) -> Result<(), TaskExecutionError> {
        let target = *self
            .operation_targets
            .get(&ack.operation_id())
            .ok_or(TaskExecutionError::UnknownOperation)?;
        self.dispatcher.settle(ack.operation_id())?;
        self.operation_targets.remove(&ack.operation_id());
        match target {
            OperationTarget::Task { stage, task } => self.acknowledge_task(stage, task, ack),
            OperationTarget::Context(context) => self.acknowledge_context(context, ack),
            // The permit is already released above. The verdict belongs to the
            // domain owner, which read this acknowledgement from the observer
            // seam before the runner got here, so applying it a second time
            // would be a second authority over the same progression.
            OperationTarget::ContextDomain(_) => Ok(()),
            OperationTarget::ActorAbort => Ok(()),
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
                let was_owned = task.create_ownership_proven();
                let context_established = self
                    .owners
                    .get(&context)
                    .is_some_and(QueryContextOwner::establish_acknowledged);
                let settlement = task.on_create_ack(ack, self.clock.now(), context_established)?;
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
                if !was_owned && task.create_ownership_proven() {
                    if let Some(owner) = self.owners.get_mut(&context) {
                        owner.note_create_owned();
                    }
                }
                self.reconcile_destination_input(identity)?;
                self.settle_task_drain(stage_id, task_id);
                self.accept_task_failure(identity);
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
            OperationKind::AcquireQueryContextAdmissionTicket => owner.on_admission_ack(ack, now),
            OperationKind::UpdateQueryContext => {
                owner.on_context_ack(ack)?;
                if owner.establish_acknowledged() {
                    for stage in self.stages.values_mut() {
                        for (_, task) in stage.tasks_mut() {
                            if task.context() == context {
                                task.on_context_established();
                            }
                        }
                    }
                }
                Ok(())
            }
            OperationKind::QuiesceQueryContext => {
                let receipt = owner.on_quiesce_ack(ack)?;
                if let Some(receipt) = receipt {
                    self.apply_quiesce_membership(context, &receipt)?;
                }
                Ok(())
            }
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

    fn apply_quiesce_membership(
        &mut self,
        context: QueryContextRef,
        receipt: &novarocks_execution::task_execution::QuiesceQueryContextReceipt,
    ) -> Result<(), TaskExecutionError> {
        let accepted = receipt
            .accepted_tasks()
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        if accepted.len() != receipt.accepted_tasks().len() {
            return Err(TaskExecutionError::DomainReceipt(
                "quiesce membership contains duplicate task identities".to_owned(),
            ));
        }
        for &identity in &accepted {
            let Some(task) = self.task_by_identity(identity) else {
                return Err(TaskExecutionError::DomainReceipt(format!(
                    "quiesce membership names an unscheduled task {identity}"
                )));
            };
            if task.context() != context {
                return Err(TaskExecutionError::DomainReceipt(format!(
                    "quiesce membership names task {identity} in another context"
                )));
            }
        }
        let mut drained = Vec::new();
        for (&stage_id, stage) in &mut self.stages {
            for (task_id, task) in stage.tasks_mut() {
                if task.context() != context {
                    continue;
                }
                let was_fenced_out = task.fenced_out();
                let became_owned =
                    task.on_quiesce_membership(accepted.contains(&task.identity()))?;
                let owner = self
                    .owners
                    .get_mut(&context)
                    .ok_or(TaskExecutionError::UnknownOperation)?;
                if became_owned {
                    owner.note_create_owned();
                } else if task.fenced_out() && !was_fenced_out {
                    owner.note_create_fenced_out();
                }
                drained.push((stage_id, *task_id));
            }
        }
        for (stage_id, task_id) in drained {
            self.settle_task_drain(stage_id, task_id);
        }
        Ok(())
    }

    /// Applies an exact transport send fact on the same serial owner as ACKs.
    pub(crate) fn establish_send_started(
        &mut self,
        context: QueryContextRef,
        operation_id: TaskOperationId,
    ) -> Result<(), TaskExecutionError> {
        self.owners
            .get_mut(&context)
            .ok_or(TaskExecutionError::UnknownOperation)?
            .on_establish_send_started(operation_id)
    }

    /// Records the edge-open decision every edge this destination completed.
    ///
    /// The fact is recorded on each producer's own task. A producer that is
    /// still `Creating` queues it, so the decision may be made early while the
    /// wire request still waits for that producer's own acknowledgement.
    fn open_ready_edges(&mut self, destination: TaskIdentity) -> Result<(), TaskExecutionError> {
        let decisions = self.edges.note_created(destination);
        self.apply_open_decisions(decisions);
        Ok(())
    }

    fn apply_open_decisions(&mut self, decisions: Vec<EdgeReadyDecision>) {
        if self.normal_drain_started {
            return;
        }
        for decision in decisions {
            let edge_id = decision.edge_id();
            let producers = self.edges.producers_of(edge_id).to_vec();
            tracing::debug!(
                edge = %edge_id,
                producers = producers.len(),
                "exchange edge decided; recording the open on every producer"
            );
            for producer in producers {
                self.edge_control_effects
                    .push_back(EdgeControlEffect::Open {
                        producer,
                        edge: edge_id,
                    });
            }
        }
    }

    /// Projects a receiver's exact no-more-input decision to each frozen
    /// producer before an edge-level open can follow it in that Task's update
    /// sequence. Closing one destination does not close its siblings.
    fn close_destination_normally(&mut self, destination: TaskIdentity) {
        let resolution = self.edges.note_normally_closed(destination);
        for effect in resolution.closed() {
            let producers = self.edges.producers_of(effect.edge_id()).to_vec();
            for producer in producers {
                self.edge_control_effects
                    .push_back(EdgeControlEffect::Close {
                        producer,
                        edge: effect.edge_id(),
                        destination: effect.destination(),
                    });
            }
        }
        self.apply_open_decisions(resolution.opened().to_vec());
    }

    /// Moves frozen edge effects to the original per-task domain owner only
    /// after the process transport has reserved their bounded queue capacity.
    /// Backpressure leaves the exact decision in this graph-bounded queue.
    /// One full producer cannot block independent producer processes, while
    /// every producer retains its own close-before-open order.
    fn flush_edge_control_effects(&mut self) -> Result<(), TaskExecutionError> {
        let mut blocked_producers = BTreeSet::new();
        let pass_len = self.edge_control_effects.len();
        for _ in 0..pass_len {
            let Some(effect) = self.edge_control_effects.pop_front() else {
                break;
            };
            if self.normal_drain_started && matches!(effect, EdgeControlEffect::Open { .. }) {
                continue;
            }
            let producer = effect.producer();
            if blocked_producers.contains(&producer) {
                self.edge_control_effects.push_back(effect);
                continue;
            }
            let task = self
                .task_by_identity_ref(producer)
                .ok_or(TaskExecutionError::UnknownOperation)?;
            if task.fenced_out() || self.task_actual_stopped(producer) {
                continue;
            }
            if task.is_terminal() && self.covered_observation_active {
                blocked_producers.insert(producer);
                self.edge_control_effects.push_back(effect);
                continue;
            }
            let update = match effect {
                EdgeControlEffect::Close {
                    edge, destination, ..
                } => task.prepare_destination_close(edge, destination)?,
                EdgeControlEffect::Open { edge, .. } => task.prepare_edge_open(edge)?,
            };
            let request =
                TaskOperationQueueRequest::task_update(producer.backend_process_id(), &update);
            self.dispatcher.validate_queue_request(request)?;
            let Some(permit) = self.reserve_process_request(request) else {
                blocked_producers.insert(producer);
                self.edge_control_effects.push_back(effect);
                continue;
            };
            self.task_by_identity(producer)
                .ok_or(TaskExecutionError::UnknownOperation)?
                .enqueue_update(update, permit)?;
        }
        Ok(())
    }

    fn reconcile_destination_input(
        &mut self,
        destination: TaskIdentity,
    ) -> Result<(), TaskExecutionError> {
        if self.normal_drain_started || self.terminal_cleanup_started {
            return Ok(());
        }
        let task = self
            .task_by_identity_ref(destination)
            .ok_or(TaskExecutionError::UnknownOperation)?;
        if !task.installed() {
            return Ok(());
        }
        if let Some(status) = task.status()
            && status.is_terminal()
        {
            if status
                .termination()
                .is_none_or(|detail| detail.is_success_compatible())
            {
                self.close_destination_normally(destination);
            }
        } else {
            self.open_ready_edges(destination)?;
        }
        self.flush_edge_control_effects()
    }

    /// Builds a reconnect request only from facts the serial owner has
    /// applied, never from transport receipt or an unconsumed intake slot.
    pub(crate) fn covered_subscription_request(
        &self,
        context: QueryContextRef,
        generation: NonZeroU64,
    ) -> Result<DecodedCoveredSubscription, TaskExecutionError> {
        let state = self
            .covered_observation
            .get(&context)
            .ok_or(TaskExecutionError::UnknownOperation)?;
        let request = DecodedCoveredSubscription {
            context,
            generation,
            status_cursors: self.status_cursors(context),
            task_convergence_cursors: self
                .graph
                .tasks()
                .filter(|task| task.context() == context)
                .map(|task| {
                    let identity = task.identity();
                    state.actual_stopped.get(&identity).map_or_else(
                        || TaskConvergenceCursor::unobserved(identity),
                        |receipt| TaskConvergenceCursor::at(identity, receipt.version()),
                    )
                })
                .collect(),
            context_cursor: state
                .context_convergence
                .map(|receipt| QueryContextConvergenceCursor::at(context, receipt.version())),
            quiesce_cursor: state
                .quiesce
                .as_ref()
                .map(|receipt| QuiesceObservationCursor {
                    context,
                    fence_version: NonZeroU64::new(receipt.fence_version()),
                }),
            required_identities: self
                .graph
                .tasks()
                .filter(|task| task.context() == context)
                .map(|task| task.identity())
                .collect(),
        };
        novarocks_task_codec::operation::encode_covered_subscribe_task_status(&request)
            .map_err(|error| TaskExecutionError::Schedule(error.to_string()))?;
        Ok(request)
    }

    pub(crate) fn next_covered_generation(
        &self,
        context: QueryContextRef,
    ) -> Result<NonZeroU64, TaskExecutionError> {
        let observed = self
            .covered_observation
            .get(&context)
            .ok_or(TaskExecutionError::UnknownOperation)?
            .generation;
        let next = observed.checked_add(1).ok_or_else(|| {
            TaskExecutionError::Schedule("covered stream generation exhausted".to_owned())
        })?;
        NonZeroU64::new(next).ok_or_else(|| {
            TaskExecutionError::Schedule("covered stream generation must be nonzero".to_owned())
        })
    }

    /// Only a registered local observation gap affects the read success cut.
    /// Ordinary reconnect or an unrelated Context's catch-up does not.
    pub(crate) fn covered_observation_ready(&self) -> bool {
        self.covered_observation
            .values()
            .all(|state| !state.gap_pending())
    }

    pub(crate) fn covered_gap_contexts(&self) -> impl Iterator<Item = QueryContextRef> + '_ {
        self.covered_observation
            .iter()
            .filter_map(|(&context, state)| state.gap_pending().then_some(context))
    }

    pub(crate) fn covered_recovery_expired(&self) -> bool {
        let now = self.clock.now();
        self.covered_observation.values().any(|state| {
            state.gap_pending()
                && [state.gap_since, state.coverage_debt.map(|(_, since)| since)]
                    .into_iter()
                    .flatten()
                    .any(|since| now.has_reached(since.saturating_add(Duration::from_secs(30))))
        })
    }

    pub(crate) fn activate_covered_observation(&mut self) {
        assert!(!self.covered_observation_active);
        self.covered_observation_active = true;
    }

    pub(crate) fn covered_observation_active(&self) -> bool {
        self.covered_observation_active
    }

    pub(crate) fn task_actual_stopped(&self, identity: TaskIdentity) -> bool {
        self.task_by_identity_ref(identity)
            .and_then(|task| self.covered_observation.get(&task.context()))
            .is_some_and(|state| state.actual_stopped.contains_key(&identity))
    }

    /// A receiver Context may release only after every frozen inbound sender
    /// that the Worker actually accepted has stopped. A Task fenced out by
    /// its own complete Quiesce receipt has no remote sender to wait for.
    fn receiver_inputs_stopped(
        &self,
        context: QueryContextRef,
    ) -> Result<bool, TaskExecutionError> {
        if !self.covered_observation_active {
            return Ok(true);
        }
        for &producer in self.inbound_producers.get(&context).into_iter().flatten() {
            let task = self
                .task_by_identity_ref(producer)
                .ok_or(TaskExecutionError::UnknownOperation)?;
            if !task.fenced_out() && !self.task_actual_stopped(producer) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Keep this Context's own stopped facts until dependent receivers have
    /// used them. A terminal status describes the Task's result, not the end
    /// of its local exchange and resource responsibilities.
    fn context_tasks_stopped(&self, context: QueryContextRef) -> Result<bool, TaskExecutionError> {
        if !self.covered_observation_active {
            return Ok(true);
        }
        for &identity in self.tasks_by_context.get(&context).into_iter().flatten() {
            let remote = self
                .task_by_identity_ref(identity)
                .ok_or(TaskExecutionError::UnknownOperation)?;
            if !remote.fenced_out() && !self.task_actual_stopped(identity) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Applies frames that the new intake registered before a success seal in
    /// one order. A frame from an older generation still contributes its
    /// positive entity fact; it cannot advance the current coverage watermark.
    pub(crate) fn apply_covered_entries(
        &mut self,
        entries: Vec<ObservationIntakeEntry>,
    ) -> Result<StatusReport, TaskExecutionError> {
        let mut report = StatusReport::default();
        for entry in entries {
            match entry {
                ObservationIntakeEntry::Frame(ObservationFrame::Covered {
                    context,
                    generation,
                    event,
                }) => self.apply_covered_frame(context, generation, event, &mut report)?,
                ObservationIntakeEntry::Frame(_) => {
                    return Err(TaskExecutionError::Schedule(
                        "legacy observation entered the covered intake".to_owned(),
                    ));
                }
                ObservationIntakeEntry::RootControl(request) => {
                    report.root_control = Some(request);
                    break;
                }
                #[cfg(test)]
                ObservationIntakeEntry::TestSeal => break,
            }
        }
        self.propagate_stage_release()?;
        Ok(report)
    }

    fn apply_covered_frame(
        &mut self,
        context: QueryContextRef,
        generation: u64,
        event: CoveredStatusStreamEvent,
        report: &mut StatusReport,
    ) -> Result<(), TaskExecutionError> {
        if generation == 0 {
            return Err(TaskExecutionError::Schedule(
                "covered observation generation must be nonzero".to_owned(),
            ));
        }
        let now = self.clock.now();
        let state = self
            .covered_observation
            .get_mut(&context)
            .ok_or(TaskExecutionError::UnknownOperation)?;
        state.begin_generation(generation);
        let gap_before = state.gap_pending();
        let current_generation = generation == state.generation;
        if current_generation && event.source_revision.is_some() && !state.initial_complete {
            return Err(TaskExecutionError::Schedule(
                "live covered observation preceded its catch-up boundary".to_owned(),
            ));
        }
        if current_generation
            && state.initial_complete
            && event.source_revision.is_none()
            && !matches!(
                &event.fact,
                CoveredStatusStreamFact::CatchUpComplete(_) | CoveredStatusStreamFact::Bookmark(_)
            )
        {
            return Err(TaskExecutionError::Schedule(
                "catch-up fact followed the completed covered boundary".to_owned(),
            ));
        }
        match event.fact {
            CoveredStatusStreamFact::Status(status) => {
                let identity = status.identity();
                self.verify_covered_task(context, identity)?;
                self.apply_published(&status, report)?;
                if status.is_terminal() {
                    self.covered_observation
                        .get_mut(&context)
                        .expect("validated context")
                        .missing_terminal
                        .remove(&identity);
                }
            }
            CoveredStatusStreamFact::Gone(identity) => {
                self.verify_covered_task(context, identity)?;
                if !current_generation {
                    report.ignored += 1;
                    return Ok(());
                }
                let has_terminal = self
                    .task_by_identity_ref(identity)
                    .and_then(RemoteTask::status)
                    .is_some_and(TaskStatus::is_terminal);
                if has_terminal {
                    let task = self.task_by_identity(identity).expect("validated task");
                    if matches!(task.observe_gone(), GoneObservation::TerminalNeverObserved) {
                        self.covered_observation
                            .get_mut(&context)
                            .unwrap()
                            .note_status_gap(identity, now);
                    }
                } else {
                    self.covered_observation
                        .get_mut(&context)
                        .unwrap()
                        .note_status_gap(identity, now);
                }
                report.ignored += 1;
            }
            CoveredStatusStreamFact::TaskConvergence(receipt) => {
                self.verify_covered_task(context, receipt.identity())?;
                let state = self.covered_observation.get_mut(&context).unwrap();
                if let Some(previous) = state.actual_stopped.get(&receipt.identity()) {
                    if previous != &receipt {
                        return Err(TaskExecutionError::Schedule(
                            "Task actual-stopped receipt changed after application".to_owned(),
                        ));
                    }
                } else {
                    state.actual_stopped.insert(receipt.identity(), receipt);
                }
                report.ignored += 1;
            }
            CoveredStatusStreamFact::ContextConvergence(receipt) => {
                if receipt.context() != context {
                    return Err(TaskExecutionError::Schedule(
                        "Context convergence observation names another context".to_owned(),
                    ));
                }
                let state = self.covered_observation.get_mut(&context).unwrap();
                if let Some(previous) = state.context_convergence {
                    if previous != receipt {
                        return Err(TaskExecutionError::Schedule(
                            "Context convergence receipt changed after application".to_owned(),
                        ));
                    }
                } else {
                    state.context_convergence = Some(receipt);
                }
                report.ignored += 1;
            }
            CoveredStatusStreamFact::Quiesce(receipt) => {
                if receipt.context() != context {
                    return Err(TaskExecutionError::Schedule(
                        "Quiesce observation names another context".to_owned(),
                    ));
                }
                if let Some(previous) = &self.covered_observation[&context].quiesce
                    && (previous.fence_version() != receipt.fence_version()
                        || previous.accepted_tasks() != receipt.accepted_tasks())
                {
                    return Err(TaskExecutionError::Schedule(
                        "Quiesce observation changed its accepted membership".to_owned(),
                    ));
                }
                self.apply_quiesce_membership(context, &receipt)?;
                self.owners
                    .get_mut(&context)
                    .expect("validated context")
                    .observe_quiesce(&receipt)?;
                self.covered_observation.get_mut(&context).unwrap().quiesce = Some(receipt);
                report.ignored += 1;
            }
            CoveredStatusStreamFact::StatusUnchanged(identity) => {
                self.verify_covered_task(context, identity)?;
                if current_generation
                    && self
                        .task_by_identity_ref(identity)
                        .and_then(RemoteTask::status)
                        .is_none()
                {
                    return Err(TaskExecutionError::Schedule(
                        "covered StatusUnchanged has no applied status cursor".to_owned(),
                    ));
                }
                report.ignored += 1;
            }
            CoveredStatusStreamFact::TaskConvergenceUnchanged(identity) => {
                self.verify_covered_task(context, identity)?;
                if current_generation
                    && !self.covered_observation[&context]
                        .actual_stopped
                        .contains_key(&identity)
                {
                    return Err(TaskExecutionError::Schedule(
                        "covered TaskConvergenceUnchanged has no applied convergence cursor"
                            .to_owned(),
                    ));
                }
                report.ignored += 1;
            }
            CoveredStatusStreamFact::Unknown(identity) => {
                self.verify_covered_task(context, identity)?;
                // The source's initial cut can precede an Accepted ACK that
                // this serial owner applied before the Unknown frame arrived.
                // This absence at an older cut cannot retract ownership.
                report.ignored += 1;
            }
            CoveredStatusStreamFact::CatchUpComplete(marker) => {
                if marker.generation != generation {
                    return Err(TaskExecutionError::Schedule(
                        "catch-up boundary names another stream generation".to_owned(),
                    ));
                }
                if current_generation {
                    let state = self.covered_observation.get_mut(&context).unwrap();
                    if state.initial_complete {
                        return Err(TaskExecutionError::Schedule(
                            "covered catch-up boundary repeated".to_owned(),
                        ));
                    }
                    state.initial_complete = true;
                    state.applied_prefix = state.applied_prefix.max(marker.initial_cut);
                    state.source_cut = state.source_cut.max(marker.initial_cut);
                    if state.missing_terminal.is_empty()
                        && state.gap_generation.is_some_and(|gap| generation > gap)
                    {
                        state.gap_generation = None;
                        state.gap_since = None;
                    }
                }
                report.ignored += 1;
            }
            CoveredStatusStreamFact::Bookmark(marker) => {
                if marker.generation != generation {
                    return Err(TaskExecutionError::Schedule(
                        "bookmark names another stream generation".to_owned(),
                    ));
                }
                if current_generation {
                    let state = self.covered_observation.get_mut(&context).unwrap();
                    if marker.sequence <= state.bookmark_sequence {
                        report.ignored += 1;
                        return Ok(());
                    }
                    if marker.covered_prefix > marker.source_cut
                        || marker.source_cut < state.source_cut
                        || marker.covered_prefix < state.applied_prefix
                        || (!state.initial_complete && marker.covered_prefix != 0)
                    {
                        return Err(TaskExecutionError::Schedule(
                            "covered bookmark regressed or crossed an unfinished catch-up"
                                .to_owned(),
                        ));
                    }
                    state.bookmark_sequence = marker.sequence;
                    state.source_cut = marker.source_cut;
                    state.applied_prefix = marker.covered_prefix;
                }
                report.ignored += 1;
            }
        }
        self.covered_observation
            .get_mut(&context)
            .unwrap()
            .update_coverage_debt(now);
        if !gap_before && self.covered_observation[&context].gap_pending() {
            self.covered_reconciliations.insert(context);
        }
        Ok(())
    }

    fn verify_covered_task(
        &self,
        context: QueryContextRef,
        identity: TaskIdentity,
    ) -> Result<(), TaskExecutionError> {
        identity
            .verify_query_context(context)
            .map_err(TaskExecutionError::Identity)?;
        self.locate(identity)
            .ok_or(TaskExecutionError::UnknownOperation)?;
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
            runner.drain_ordered(max_events)
        };
        for event in events {
            match event {
                StatusIntakeEntry::Status(StatusEvent::Published(status)) => {
                    self.apply_published(&status, &mut report)?;
                }
                StatusIntakeEntry::Status(StatusEvent::Gone(identity)) => {
                    let Some(task) = self.task_by_identity(identity) else {
                        continue;
                    };
                    if matches!(task.observe_gone(), GoneObservation::TerminalNeverObserved) {
                        return Err(TaskExecutionError::Observation(
                            StatusObservation::TerminalOverwrite,
                        ));
                    }
                }
                StatusIntakeEntry::ObservationLoss => {
                    report.resubscribe = true;
                }
                StatusIntakeEntry::RootControl(request) => {
                    report.root_control = Some(request);
                    break;
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
        let was_owned = task.create_ownership_proven();
        match task.observe_status(status)? {
            StatusObservation::Accept => report.accepted += 1,
            _ => report.ignored += 1,
        }
        let became_owned = !was_owned && task.create_ownership_proven();
        let installed = task.installed();
        if became_owned && let Some(owner) = self.owners.get_mut(&task.context()) {
            owner.note_create_owned();
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
        if installed {
            self.reconcile_destination_input(identity)?;
        }
        self.settle_task_drain(stage_id, task_id);
        self.accept_task_failure(identity);
        Ok(())
    }

    fn accept_task_failure(&mut self, identity: TaskIdentity) {
        let detail = self
            .task_by_identity_ref(identity)
            .and_then(|task| task.status())
            .and_then(|status| status.termination())
            .filter(|detail| !detail.is_success_compatible())
            .cloned();
        if let Some(detail) = detail {
            self.failure.latch(detail);
            // Freeze normal admission in the fact's own serial turn. Returning
            // W cannot authorize another Create after the attempt has failed.
            self.begin_terminal_cleanup();
        }
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

    /// Cancels a producer only after all of its consumers have stopped.
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
            let children = self.graph.producer_stages(stage_id).collect::<Vec<_>>();
            let mut candidates = VecDeque::new();
            for child in children {
                // A multicast producer can feed several consumer stages.
                // One finished branch releases only its own need; cancelling
                // the producer then would strand another branch without EOS.
                let all_consumers_released = self
                    .graph
                    .edges()
                    .filter(|edge| edge.producer_stage() == child)
                    .all(|edge| {
                        self.stages
                            .get(&edge.consumer_stage())
                            .is_some_and(StageExecution::released_children)
                    });
                if !all_consumers_released {
                    continue;
                }
                let Some(stage) = self.stages.get(&child) else {
                    continue;
                };
                let identities = stage
                    .tasks()
                    .filter(|(task_id, _)| Some(**task_id) != stage.root_task())
                    .map(|(_, task)| task.identity())
                    .collect::<Vec<_>>();
                for identity in identities {
                    self.stand_down_task_normally(identity)?;
                }
                let intents = self
                    .stages
                    .get_mut(&child)
                    .expect("the child stage was just located")
                    .cancel_non_root_tasks();
                for intent in intents {
                    let identity = match &intent {
                        OperationIntent::CancelTask(request) => request.identity(),
                        _ => continue,
                    };
                    candidates.push_back(AdmissionCandidate::minted(
                        OperationTarget::Task {
                            stage: child,
                            task: identity.task_id(),
                        },
                        intent,
                    ));
                }
            }
            // Only a pass that admitted every cancel this release owes closes
            // the release: a cancel skipped behind a full target is still
            // owed, and its stage is asked again on the next turn.
            if self.enqueue_candidates(candidates, now)?.complete() {
                self.released_children_of.insert(stage_id);
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
            .consume_packet(root, packet_sequence, end_of_stream)?;
        if end_of_stream {
            self.require_terminal_evidence(std::iter::once(root))?;
        }
        Ok(())
    }

    /// Registers exact terminal evidence required after the consumer's result
    /// proof arrives. Repeated declarations cannot renew an earlier budget;
    /// unrelated Context coverage does not register any requirement.
    pub(crate) fn require_terminal_evidence(
        &mut self,
        identities: impl IntoIterator<Item = TaskIdentity>,
    ) -> Result<(), TaskExecutionError> {
        let deadline = self.clock.now().saturating_add(Duration::from_secs(30));
        for identity in identities {
            if self.task_by_identity_ref(identity).is_none() {
                return Err(TaskExecutionError::UnknownOperation);
            }
            self.required_evidence_deadlines
                .entry(identity)
                .or_insert(deadline);
        }
        Ok(())
    }

    pub(crate) fn require_result_terminal_control(
        &mut self,
        root: TaskIdentity,
    ) -> Result<(), TaskExecutionError> {
        if root != self.graph.root_identity() {
            return Err(TaskExecutionError::UnknownOperation);
        }
        let deadline = self.clock.now().saturating_add(Duration::from_secs(30));
        self.required_result_terminal_control_deadline
            .get_or_insert(deadline);
        Ok(())
    }

    pub(crate) fn result_terminal_control_required(&self) -> bool {
        self.required_result_terminal_control_deadline.is_some()
    }

    pub(crate) fn required_result_terminal_control_expired(&self) -> bool {
        self.required_result_terminal_control_deadline
            .is_some_and(|deadline| {
                self.clock.now().has_reached(deadline)
                    && self.failure.cause().is_none_or(|cause| cause.is_derived())
            })
    }

    pub(crate) fn required_terminal_evidence_expired(&self) -> Option<TaskIdentity> {
        let now = self.clock.now();
        self.required_evidence_deadlines
            .iter()
            .find_map(|(&identity, &deadline)| {
                let task = self.task_by_identity_ref(identity)?;
                if task.is_terminal() {
                    return None;
                }
                // A newly required consumer cannot grant an existing source
                // debt a fresh recovery window. Only this exact Task's Context
                // is relevant; unrelated coverage never gates read success.
                let debt_expired = self
                    .covered_observation
                    .get(&task.context())
                    .and_then(|state| state.coverage_debt)
                    .is_some_and(|(_, since)| {
                        now.has_reached(since.saturating_add(Duration::from_secs(30)))
                    });
                (now.has_reached(deadline) || debt_expired).then_some(identity)
            })
    }

    #[cfg(test)]
    pub(crate) fn required_root_evidence_expired(&self) -> bool {
        self.required_evidence_deadlines
            .get(&self.read.root())
            .is_some_and(|&deadline| {
                self.clock.now().has_reached(deadline)
                    && !self
                        .task_by_identity_ref(self.read.root())
                        .is_some_and(RemoteTask::is_terminal)
            })
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

    /// Whether the exact Worker context acknowledged release after all of its
    /// local Tasks became terminal. This is a positive per-context stop fact;
    /// absence remains unknown and must not be inferred from transport loss.
    pub(crate) fn context_released(&self, context: QueryContextRef) -> bool {
        self.owners
            .get(&context)
            .is_some_and(QueryContextOwner::is_released)
    }

    /// A context absent from the Worker for the entire attempt can close
    /// during cancellation without waiting for local planned Tasks to report
    /// terminal status: none of them could have been created there.
    pub(crate) fn context_never_established(&self, context: QueryContextRef) -> bool {
        self.owners
            .get(&context)
            .is_some_and(QueryContextOwner::never_attempted_establish)
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

    /// Admits candidates in order, one serial pass.
    ///
    /// Each candidate is checked against this attempt's own queue bounds and
    /// then reserved against process transport, and only then is its request
    /// made to exist and queued; nothing else touches the queues in between,
    /// so the reservation it was admitted with is exactly the one it holds.
    ///
    /// A full target -- this attempt's queue for that backend, or process
    /// transport's window for it -- skips that target's later candidates and
    /// lets every other target advance; a target's own order is never
    /// crossed. A full attempt total or a full process-wide window ends the
    /// pass, because no other target could be admitted either. A request that
    /// could never fit any queue is an error, not backpressure.
    fn enqueue_candidates(
        &mut self,
        candidates: VecDeque<AdmissionCandidate>,
        now: novarocks_query_application::coordination::MonotonicInstant,
    ) -> Result<AdmissionPass, TaskExecutionError> {
        let mut pass = AdmissionPass::default();
        for candidate in candidates {
            let backend = self.candidate_backend(&candidate)?;
            if pass.ended.is_some() || pass.full_targets.contains(&backend) {
                self.skip_candidate(candidate, &mut pass);
                continue;
            }
            match self.admit_candidate(candidate, now)? {
                CandidateAdmission::Admitted => pass.admitted += 1,
                CandidateAdmission::Nothing => {}
                CandidateAdmission::DeploymentWindowFull => pass.skipped = true,
                CandidateAdmission::TargetFull => {
                    pass.full_targets.insert(backend);
                    pass.skipped = true;
                }
                CandidateAdmission::Ended(end) => {
                    pass.ended = Some(end);
                    pass.skipped = true;
                }
            }
        }
        Ok(pass)
    }

    fn candidate_backend(
        &self,
        candidate: &AdmissionCandidate,
    ) -> Result<BackendProcessId, TaskExecutionError> {
        match candidate {
            AdmissionCandidate::Minted { intent, .. } => Ok(intent.backend_process_id()),
            AdmissionCandidate::Create { stage, task }
            | AdmissionCandidate::Update { stage, task } => Ok(self
                .stages
                .get(stage)
                .and_then(|stage| stage.task(*task))
                .ok_or(TaskExecutionError::UnknownOperation)?
                .identity()
                .backend_process_id()),
        }
    }

    /// Leaves one candidate exactly where it was.
    ///
    /// A minted candidate returns to its owner; a create or update position
    /// was never taken. Only a position that genuinely had something to send
    /// counts as a skip, so a stage release still learns whether every cancel
    /// it owes went out.
    fn skip_candidate(&mut self, candidate: AdmissionCandidate, pass: &mut AdmissionPass) {
        match candidate {
            AdmissionCandidate::Minted { target, intent } => {
                self.rollback_unsent(target, intent.operation_id());
                pass.skipped = true;
            }
            AdmissionCandidate::Create { stage, task } => {
                if self
                    .stages
                    .get(&stage)
                    .and_then(|stage| stage.task(task))
                    .is_some_and(RemoteTask::create_pending)
                {
                    pass.skipped = true;
                }
            }
            AdmissionCandidate::Update { stage, task } => {
                if self
                    .stages
                    .get(&stage)
                    .and_then(|stage| stage.task(task))
                    .is_some_and(|task| task.update_candidate().is_some())
                {
                    pass.skipped = true;
                }
            }
        }
    }

    /// Checks this attempt's own queue bounds for one request, then reserves
    /// it against process transport.
    fn reserve_candidate(
        &self,
        request: TaskOperationQueueRequest,
        holds_queue_permit: bool,
    ) -> Result<
        Result<Option<Box<dyn TaskOperationQueuePermit>>, CandidateAdmission>,
        TaskExecutionError,
    > {
        self.dispatcher.validate_queue_request(request)?;
        match self.dispatcher.local_capacity(request) {
            LocalQueueCapacity::Fits => {}
            LocalQueueCapacity::TargetFull => return Ok(Err(CandidateAdmission::TargetFull)),
            LocalQueueCapacity::AttemptFull => {
                return Ok(Err(CandidateAdmission::Ended(
                    AdmissionPassEnd::AttemptQueue,
                )));
            }
        }
        if holds_queue_permit {
            return Ok(Ok(None));
        }
        Ok(match self.sink.try_reserve_queue(request) {
            TaskOperationQueueAdmission::Admitted(permit) => Ok(Some(permit)),
            TaskOperationQueueAdmission::TargetFull => Err(CandidateAdmission::TargetFull),
            TaskOperationQueueAdmission::ProcessFull => Err(CandidateAdmission::Ended(
                AdmissionPassEnd::ProcessTransport,
            )),
        })
    }

    fn refresh_deployment_window(&mut self) {
        let stages = &self.stages;
        self.deployment_window.retain(|_, identities| {
            identities.retain(|identity| {
                stages
                    .get(&identity.stage_id())
                    .and_then(|stage| stage.task(identity.task_id()))
                    .is_some_and(RemoteTask::needs_deployment_window)
            });
            !identities.is_empty()
        });
    }

    fn admit_candidate(
        &mut self,
        candidate: AdmissionCandidate,
        now: novarocks_query_application::coordination::MonotonicInstant,
    ) -> Result<CandidateAdmission, TaskExecutionError> {
        match candidate {
            AdmissionCandidate::Minted { target, intent } => {
                let operation_id = intent.operation_id();
                if let Err(error) = self.dispatcher.validate_operation_carrier(&intent) {
                    self.rollback_unsent(target, operation_id);
                    return Err(error);
                }
                let permit = match self.reserve_candidate(intent.queue_request(), false) {
                    Ok(Ok(permit)) => permit.expect("a minted candidate reserves its own permit"),
                    Ok(Err(refusal)) => {
                        self.rollback_unsent(target, operation_id);
                        return Ok(refusal);
                    }
                    Err(error) => {
                        self.rollback_unsent(target, operation_id);
                        return Err(error);
                    }
                };
                if let Err(error) = self.dispatcher.enqueue_reserved(*intent, now, permit) {
                    self.rollback_unsent(target, operation_id);
                    return Err(error);
                }
                self.operation_targets.insert(operation_id, target);
                Ok(CandidateAdmission::Admitted)
            }
            AdmissionCandidate::Create { stage, task } => {
                let context = self
                    .stages
                    .get(&stage)
                    .and_then(|stage| stage.task(task))
                    .ok_or(TaskExecutionError::UnknownOperation)?
                    .context();
                if !self
                    .owners
                    .get(&context)
                    .is_some_and(QueryContextOwner::may_pipeline_first_create)
                {
                    return Ok(CandidateAdmission::Nothing);
                }
                let identity = self
                    .stages
                    .get(&stage)
                    .and_then(|stage| stage.task(task))
                    .ok_or(TaskExecutionError::UnknownOperation)?
                    .identity();
                let positions = self.deployment_window.get(&identity.backend_process_id());
                if positions.is_some_and(|positions| {
                    !positions.contains(&identity)
                        && positions.len()
                            >= self.deployment_window_limits[&identity.backend_process_id()]
                }) {
                    return Ok(CandidateAdmission::DeploymentWindowFull);
                }
                let remote = self
                    .stages
                    .get_mut(&stage)
                    .and_then(|stage| stage.task_mut(task))
                    .ok_or(TaskExecutionError::UnknownOperation)?;
                let Some(candidate) = remote.create_candidate(now)? else {
                    return Ok(CandidateAdmission::Nothing);
                };
                self.dispatcher
                    .validate_plan_carrier_bytes(candidate.plan_carrier_bytes())?;
                let permit = match self.reserve_candidate(candidate.request(), false)? {
                    Ok(permit) => permit.expect("a create reserves its own permit"),
                    Err(refusal) => return Ok(refusal),
                };
                let remote = self
                    .stages
                    .get_mut(&stage)
                    .and_then(|stage| stage.task_mut(task))
                    .ok_or(TaskExecutionError::UnknownOperation)?;
                // A freeze failure drops the permit it was admitted with.
                let intent = remote.release_create(now)?;
                let target = OperationTarget::Task { stage, task };
                if let Err(error) = self.dispatcher.enqueue_reserved(intent, now, permit) {
                    self.rollback_unsent(target, candidate.operation_id());
                    return Err(error);
                }
                self.operation_targets
                    .insert(candidate.operation_id(), target);
                self.deployment_window
                    .entry(identity.backend_process_id())
                    .or_default()
                    .insert(identity);
                Ok(CandidateAdmission::Admitted)
            }
            AdmissionCandidate::Update { stage, task } => {
                let Some(candidate) = self
                    .stages
                    .get(&stage)
                    .and_then(|stage| stage.task(task))
                    .ok_or(TaskExecutionError::UnknownOperation)?
                    .update_candidate()
                else {
                    return Ok(CandidateAdmission::Nothing);
                };
                let reserved = match self
                    .reserve_candidate(candidate.request(), candidate.holds_queue_permit())?
                {
                    Ok(reserved) => reserved,
                    Err(refusal) => return Ok(refusal),
                };
                let remote = self
                    .stages
                    .get_mut(&stage)
                    .and_then(|stage| stage.task_mut(task))
                    .ok_or(TaskExecutionError::UnknownOperation)?;
                let Some((intent, held)) = remote.next_update_intent()? else {
                    return Err(TaskExecutionError::Schedule(format!(
                        "task {task} withdrew the update admission just priced"
                    )));
                };
                // Exactly one reservation moves with the update: the one it
                // was queued with, or the one this pass just made.
                let permit = match (held, reserved) {
                    (Some(held), None) => held,
                    (None, Some(reserved)) => reserved,
                    _ => {
                        return Err(TaskExecutionError::Schedule(format!(
                            "task {task} update reservation ownership changed during admission"
                        )));
                    }
                };
                let operation_id = intent.operation_id();
                let target = OperationTarget::Task { stage, task };
                if let Err(error) = self.dispatcher.enqueue_reserved(intent, now, permit) {
                    self.rollback_unsent(target, operation_id);
                    return Err(error);
                }
                self.operation_targets.insert(operation_id, target);
                Ok(CandidateAdmission::Admitted)
            }
        }
    }

    /// Releases a batch which a pre-transport gate proved definitely unsent.
    ///
    /// `take_batch` already removed its queue counters. Dropping the attached
    /// process permits releases capacity; rolling back each owner marker lets
    /// attempt convergence mint only the cleanup work it still owes.
    fn rollback_rejected_batch(&mut self, batch: DispatchBatch) -> Result<(), TaskExecutionError> {
        let (operations, queue_permits) = batch.into_queue_parts();
        drop(queue_permits);
        for operation in operations {
            let operation_id = operation.operation_id();
            let target = self
                .operation_targets
                .remove(&operation_id)
                .ok_or(TaskExecutionError::UnknownOperation)?;
            self.rollback_unsent(target, operation_id);
        }
        Ok(())
    }

    fn rollback_unsent(&mut self, target: OperationTarget, operation_id: TaskOperationId) {
        match target {
            OperationTarget::Task { stage, task } => {
                if let Some(task) = self
                    .stages
                    .get_mut(&stage)
                    .and_then(|stage| stage.task_mut(task))
                {
                    task.rollback_unsent(operation_id);
                }
            }
            OperationTarget::Context(context) => {
                if let Some(owner) = self.owners.get_mut(&context) {
                    owner.rollback_unsent(operation_id);
                }
            }
            OperationTarget::ContextDomain(_) => {}
            OperationTarget::ActorAbort => {}
        }
    }

    fn reserve_process_queue(
        &self,
        intent: &OperationIntent,
    ) -> Result<Option<Box<dyn TaskOperationQueuePermit>>, TaskExecutionError> {
        self.dispatcher.validate_operation_carrier(intent)?;
        Ok(self.reserve_process_request(intent.queue_request()))
    }

    /// Reserves one single-operation request against process transport.
    ///
    /// The single-operation owners -- a domain update, an actor Abort, a
    /// cleanup Abort -- have no successor candidate a full target could
    /// reorder, so both refusals mean the same thing to them: not now. Each
    /// keeps its own retry or failure rule for that answer.
    fn reserve_process_request(
        &self,
        request: TaskOperationQueueRequest,
    ) -> Option<Box<dyn TaskOperationQueuePermit>> {
        match self.sink.try_reserve_queue(request) {
            TaskOperationQueueAdmission::Admitted(permit) => Some(permit),
            TaskOperationQueueAdmission::TargetFull | TaskOperationQueueAdmission::ProcessFull => {
                None
            }
        }
    }

    fn process_queue_backpressure(request: TaskOperationQueueRequest) -> TaskExecutionError {
        CapacityBound::ProcessTransportQueue {
            lane: request.lane(),
            bytes: request.queued_bytes(),
        }
        .into()
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

#[cfg(test)]
mod covered_progress_tests {
    use super::*;

    #[test]
    fn coverage_debt_keeps_first_target_and_age_across_cuts_and_generations() {
        let mut state = CoveredContextObservation::default();
        state.begin_generation(1);
        state.source_cut = 10;
        state.applied_prefix = 4;
        state.update_coverage_debt(MonotonicInstant::ORIGIN);
        state.source_cut = 20;
        state.update_coverage_debt(MonotonicInstant::from_origin(Duration::from_secs(29)));
        assert_eq!(state.coverage_debt, Some((10, MonotonicInstant::ORIGIN)));
        state.begin_generation(2);
        state.update_coverage_debt(MonotonicInstant::from_origin(Duration::from_secs(30)));
        assert_eq!(state.coverage_debt, Some((10, MonotonicInstant::ORIGIN)));
        state.source_cut = 25;
        state.applied_prefix = 10;
        let settled = MonotonicInstant::from_origin(Duration::from_secs(31));
        state.update_coverage_debt(settled);
        assert_eq!(state.coverage_debt, Some((25, settled)));
        state.applied_prefix = 25;
        state.update_coverage_debt(settled);
        assert_eq!(state.coverage_debt, None);
    }
}
