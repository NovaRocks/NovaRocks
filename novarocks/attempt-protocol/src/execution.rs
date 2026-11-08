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
//! It owns the graph, one stage execution per stage, one participation owner
//! per participation, the bounded dispatcher, the cross-stage edge-open decision,
//! and the serial status runner. Everything it does is driven by one thread
//! at a time: `pump` produces intents and releases bounded batches,
//! `acknowledge` settles one released operation, and `apply_status` applies
//! what the intake queued. Nothing here opens a connection.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;

use crate::conclusion::{LatchOutcome, TerminationLatch};
use crate::config::DispatchBudget;
use crate::config::FrozenTransportLimits as TransportBudget;
use crate::effect::{DispatchBatch, OperationIntent, TaskDomainIntent as TaskDomainUpdate};
use crate::ids::OperationIds;
use crate::input::{
    ObservationBatch, ObservationEvent, OperationReply as OperationAcknowledgement,
    ReceivedObservation,
};
use crate::input::{
    ObservationFact as CoveredStatusStreamFact, QueuedInputGrantRef, UpdateAdmission,
};
use crate::intent::TaskOperationQueueRequest;
use crate::lease::RequestedLeaseDurations;
use crate::observation::{
    QuiesceCursor as QuiesceObservationCursor, SubscriptionRequest as DecodedCoveredSubscription,
};
use crate::operation::OperationDispatchResult;
use crate::status_validation::GoneObservation;
use novarocks_coordination_contract::AttemptInheritedAdmission as ReplacementWorkerAdmissionEvidence;
use novarocks_coordination_contract::{MonotonicInstant, StatusObservation};
use novarocks_execution_contract::{
    AdmissionEpochCapability, CancelReason, ExchangeEdgeId, OperationKind, ParticipationRef,
    TaskIdentity, TaskOperationId, TaskState, TaskStatus, TaskStatusCursor, TerminationDetail,
};
use novarocks_execution_contract::{
    ParticipationConvergenceCursor, ParticipationConvergenceReceipt, QuiesceParticipationReceipt,
    TaskConvergenceCursor, TaskConvergenceReceipt,
};
use novarocks_types::NativeCompatibilityId;
use novarocks_types::identity::{StageId, TaskId};

use crate::start::PayloadRef;
use novarocks_coordination_contract::AttemptRequestToken as AcceptedRootControlRequest;
use novarocks_types::identity::BackendProcessId;

use super::dispatch::{DispatchOperationState, LocalQueueCapacity, OperationDispatcher};
use super::error::TaskExecutionError;
use super::graph::TaskGraph;
use super::participation_owner::{
    ParticipationEstablishFacts, ParticipationOwner, ReleaseSettlement,
};
use super::remote_task::{CreateSettlement, RemoteTask, TaskTerminalReport, UpdateSettlement};
use super::stage::{EdgeOpenTracker, StageExecution};

struct CoveredStatusStreamEvent {
    source_revision: Option<u64>,
    fact: CoveredStatusStreamFact,
}

#[derive(Debug, Default)]
struct CoveredParticipationObservation {
    generation: u64,
    initial_complete: bool,
    bookmark_sequence: u64,
    applied_prefix: u64,
    source_cut: u64,
    actual_stopped: BTreeMap<TaskIdentity, TaskConvergenceReceipt>,
    participation_convergence: Option<ParticipationConvergenceReceipt>,
    quiesce: Option<QuiesceParticipationReceipt>,
    /// An accepted fact that could not establish its task's required terminal
    /// evidence. Only a later complete catch-up with positive facts clears it.
    missing_terminal: BTreeSet<TaskIdentity>,
    gap_generation: Option<u64>,
    gap_since: Option<MonotonicInstant>,
    coverage_debt: Option<(u64, MonotonicInstant)>,
}

use crate::stage::derivation::{
    DerivedEdgeEffect as EdgeControlEffect, EdgeDerivationStep, EdgeDerivations,
};

/// The original graph row keeps its exact generated input and paid ticket
/// through a capacity wait. No process physical position is held while Joint
/// admission is waiting, and polling never removes this row from its owner.
#[derive(Debug)]
struct PendingEdgeControlEffect {
    effect: EdgeControlEffect,
    input: Option<TaskDomainUpdate>,
    preparation: Option<TaskOperationId>,
}
impl From<EdgeControlEffect> for PendingEdgeControlEffect {
    fn from(effect: EdgeControlEffect) -> Self {
        Self {
            effect,
            input: None,
            preparation: None,
        }
    }
}

impl CoveredParticipationObservation {
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

    fn recovery_deadline(&self) -> Option<MonotonicInstant> {
        if !self.gap_pending() {
            return None;
        }
        [self.gap_since, self.coverage_debt.map(|(_, since)| since)]
            .into_iter()
            .flatten()
            .min()
            .map(|since| since.saturating_add(Duration::from_secs(30)))
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
    Participation(ParticipationRef),
    /// One shared-domain advance whose progression belongs to an attempt-local
    /// owner outside this state machine.
    ///
    /// The credential domain is the only such owner: the frontend is the
    /// credential principal, so the epoch it advances is minted from a provider
    /// this state machine must not know about. Settling one here releases the
    /// dispatch permit and nothing else; the owner learns the outcome through
    /// the runner's acknowledgement-observer seam, which sees every
    /// acknowledgement before it is settled.
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
/// `participations()` says whether the carrier is being fed at all. A query that ran
/// runtime filters and reports zero contributions over three participations is a
/// carrier that is not wired, which no per-part test can show.
#[derive(Clone, Debug, PartialEq)]
pub struct ReleasedRuntimeFilterContributions {
    contributions: Vec<(BackendProcessId, PayloadRef)>,
    complete: bool,
    #[cfg(test)]
    participations: usize,
}

impl ReleasedRuntimeFilterContributions {
    pub fn contributions(&self) -> &[(BackendProcessId, PayloadRef)] {
        &self.contributions
    }

    /// Whether every participation this attempt placed has answered its release.
    pub const fn is_complete(&self) -> bool {
        self.complete
    }

    #[cfg(test)]
    pub const fn participations(&self) -> usize {
        self.participations
    }
}

#[derive(Debug)]
struct NormalDrainCursor {
    after: Option<TaskId>,
}
#[derive(Debug)]
enum TerminalCleanupCursor {
    Tasks(Option<TaskId>),
    Owners(Option<ParticipationRef>),
    Queued(Option<TaskOperationId>),
    Reconciliations,
}

#[derive(Clone, Copy, Debug)]
enum StagePropagationCursor {
    Children {
        parent: StageId,
        after: Option<StageId>,
    },
    Consumers {
        parent: StageId,
        producer: StageId,
    },
    Tasks {
        parent: StageId,
        producer: StageId,
        after: Option<TaskId>,
    },
}
#[derive(Debug, Default)]
struct StageParentDebt {
    scan_done: bool,
    scan_cancelled: bool,
    retry_unproven: bool,
    rescan_requested: bool,
    cancels: usize,
}
#[derive(Clone, Copy, Debug)]
struct StageCancelDebt {
    parent: StageId,
    request: novarocks_execution_contract::CancelTask,
}

/// The task protocol of one query execution attempt.
#[derive(Debug)]
pub struct QueryTaskExecution {
    graph: TaskGraph,
    stages: BTreeMap<StageId, StageExecution>,
    stage_of_task: BTreeMap<TaskId, StageId>,
    owners: BTreeMap<ParticipationRef, ParticipationOwner>,
    diagnostic_sources: crate::diagnostics::sources::DiagnosticSourcesState,
    dispatcher: OperationDispatcher,
    edges: EdgeOpenTracker,
    derived_edges: EdgeDerivations,
    stage_demand: crate::stage::demand::StageDemandObservations,
    stage_propagation: Option<StagePropagationCursor>,
    stage_parent_debts: BTreeMap<StageId, StageParentDebt>,
    stage_cancel_debts: BTreeMap<TaskOperationId, StageCancelDebt>,
    now: MonotonicInstant,
    ids: OperationIds,
    covered_observation_active: bool,
    covered_observation: BTreeMap<ParticipationRef, CoveredParticipationObservation>,
    covered_gaps: BTreeSet<ParticipationRef>,
    covered_gap_deadlines: BTreeSet<(MonotonicInstant, ParticipationRef)>,
    strict_pending: BTreeSet<TaskIdentity>,
    inbound_producers: BTreeMap<ParticipationRef, BTreeSet<TaskIdentity>>,
    tasks_by_participation: BTreeMap<ParticipationRef, Vec<TaskIdentity>>,
    deployment_window_limits: BTreeMap<BackendProcessId, usize>,
    deployment_window: BTreeMap<BackendProcessId, BTreeSet<TaskIdentity>>,
    /// The frozen graph bounds this by sum(producers * (destinations + 1)):
    /// the edge tracker emits each close member and each open only once.
    edge_control_effects: VecDeque<PendingEdgeControlEffect>,
    operation_targets: BTreeMap<TaskOperationId, OperationTarget>,
    pending_candidates: VecDeque<AdmissionCandidate>,
    released_native_inputs: VecDeque<QueuedInputGrantRef>,
    expired_actor_aborts: Vec<TaskOperationId>,
    status_reconciliations: BTreeSet<ParticipationRef>,
    terminal_retired_reconciliations: BTreeSet<ParticipationRef>,
    covered_reconciliations: BTreeSet<ParticipationRef>,
    normal_drain_requested: bool,
    normal_drain_started: bool,
    normal_drain_cursor: Option<NormalDrainCursor>,
    profile_release_deadline: Option<MonotonicInstant>,
    terminal_cleanup_started: bool,
    terminal_cleanup_cursor: Option<TerminalCleanupCursor>,
    failure: TerminationLatch,
    accepted_protocol_revision: u64,
    // One ordinal per frozen Task, appended only on its first accepted terminal.
    // Diagnostic readers keep their own cursor; no idle graph scan is needed.
    accepted_terminal_tasks: Vec<TaskIdentity>,
    accepted_terminal_seen: BTreeSet<TaskIdentity>,
    accepted_protocol_dirty_tasks: BTreeSet<TaskId>,
    accepted_protocol_writer_evidence: BTreeMap<TaskId, Arc<TaskStatus>>,
    required_result_terminal_control_deadline: Option<MonotonicInstant>,
    // First local terminal order, including terminal without TaskStatus.
    // This 1-based graph frontier is not an observation/source sequence.
    drained_tasks: BTreeMap<TaskId, usize>,
    stage_demand_cuts: BTreeMap<StageId, usize>,
    withdrawal_materialized: BTreeSet<TaskId>,
    released_outputs: BTreeSet<TaskId>,
    released_children_of: BTreeSet<StageId>,
}

/// Real dispatcher and process-transport capacity reserved for one exact
/// actor-owned Abort preview.
#[derive(Debug)]
pub(crate) struct ActorAbortReservation {
    operation_id: TaskOperationId,
    participation: ParticipationRef,
    queue_permit: QueuedInputGrantRef,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum ActorAbortDispatchState {
    Queued,
    InFlight,
}

/// How an acknowledgement relates to an exact participation operation replay which
/// has not crossed the current generation's transport boundary yet.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum QueuedParticipationAcknowledgement {
    NotQueued,
    IgnoreOlderTransportUnknown,
    ApplyDefinitive,
}

impl QueryTaskExecution {
    /// Classifies a participation acknowledgement against the dispatch owner before
    /// acknowledgement observers consume it.
    ///
    /// After an unknown outcome, an exact replay may already be queued when an
    /// older generation answers. Another unknown fact says nothing about the
    /// queued generation and is ignored. A definitive Worker answer closes the
    /// exact operation and must cancel that definitely-unsent replay before the
    /// domain owner applies the answer.
    pub(crate) fn classify_queued_participation_acknowledgement(
        &self,
        acknowledgement: &OperationAcknowledgement,
    ) -> Result<QueuedParticipationAcknowledgement, TaskExecutionError> {
        if !matches!(
            acknowledgement.kind(),
            OperationKind::AcquireParticipationAdmissionTicket | OperationKind::UpdateParticipation
        ) || !matches!(
            self.operation_targets.get(&acknowledgement.operation_id()),
            Some(OperationTarget::Participation(_))
        ) {
            return Ok(QueuedParticipationAcknowledgement::NotQueued);
        }
        if self
            .dispatcher
            .operation_state(acknowledgement.operation_id())?
            != DispatchOperationState::Queued
        {
            return Ok(QueuedParticipationAcknowledgement::NotQueued);
        }
        Ok(
            if acknowledgement.dispatch_result() != OperationDispatchResult::TransportUnknown {
                QueuedParticipationAcknowledgement::ApplyDefinitive
            } else {
                QueuedParticipationAcknowledgement::IgnoreOlderTransportUnknown
            },
        )
    }

    /// Cancels the current definitely-unsent replay and applies the definitive
    /// settled acknowledgement which arrived from an older transport generation.
    pub(crate) fn acknowledge_queued_participation_replay(
        &mut self,
        acknowledgement: &OperationAcknowledgement,
    ) -> Result<(), TaskExecutionError> {
        if acknowledgement.dispatch_result() == OperationDispatchResult::TransportUnknown {
            return Err(TaskExecutionError::Schedule(
                "a queued participation replay can be closed only by a definitive acknowledgement"
                    .to_owned(),
            ));
        }
        let operation_id = acknowledgement.operation_id();
        let target = *self
            .operation_targets
            .get(&operation_id)
            .ok_or(TaskExecutionError::UnknownOperation)?;
        let OperationTarget::Participation(participation) = target else {
            return Err(TaskExecutionError::UnknownOperation);
        };
        let intent = self.dispatcher.cancel_queued(operation_id)?;
        if intent.operation_id() != operation_id
            || intent.kind() != acknowledgement.kind()
            || intent.backend_process_id() != participation.backend_process_id()
        {
            return Err(TaskExecutionError::Schedule(
                "queued participation replay differs from its definitive acknowledgement address"
                    .to_owned(),
            ));
        }
        self.operation_targets.remove(&operation_id);
        self.acknowledge_participation(participation, acknowledgement)
    }

    /// Builds the attempt's owners from its frozen graph.
    pub fn new(
        graph: TaskGraph,
        budget: DispatchBudget,
        transport: TransportBudget,
        native_compatibility_id: NativeCompatibilityId,
        admission_epochs: &BTreeMap<BackendProcessId, AdmissionEpochCapability>,
        preparing_positions: &BTreeMap<BackendProcessId, usize>,
        now: MonotonicInstant,
        ids: OperationIds,
        requested_leases: RequestedLeaseDurations,
    ) -> Result<Self, TaskExecutionError> {
        Self::new_with_split_profile_source(
            graph,
            budget,
            transport,
            native_compatibility_id,
            admission_epochs,
            preparing_positions,
            now,
            ids,
            requested_leases,
            None,
        )
    }

    /// Binds the exact original split profile Source during construction.
    /// None positively declares that this attempt has no split Source.
    pub(crate) fn new_with_split_profile_source(
        graph: TaskGraph,
        budget: DispatchBudget,
        transport: TransportBudget,
        native_compatibility_id: NativeCompatibilityId,
        admission_epochs: &BTreeMap<BackendProcessId, AdmissionEpochCapability>,
        preparing_positions: &BTreeMap<BackendProcessId, usize>,
        now: MonotonicInstant,
        mut ids: OperationIds,
        requested_leases: RequestedLeaseDurations,
        split_source: Option<PayloadRef>,
    ) -> Result<Self, TaskExecutionError> {
        let diagnostic_sources = crate::diagnostics::sources::DiagnosticSourcesState::new(
            graph.participations().copied(),
            split_source,
        );
        let participants = graph
            .participations()
            .map(|participation| participation.backend_process_id())
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
            Vec<(novarocks_execution_contract::ExchangeEdgeId, TaskIdentity)>,
        >::new();
        let mut inbound_producers = BTreeMap::<ParticipationRef, BTreeSet<TaskIdentity>>::new();
        for edge in graph.edges() {
            for destination in edge.destinations() {
                let participation = graph
                    .task(destination.task_id())
                    .ok_or(TaskExecutionError::UnknownOperation)?
                    .participation();
                inbound_producers
                    .entry(participation)
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
        let mut owners = BTreeMap::<ParticipationRef, ParticipationOwner>::new();
        let mut tasks_per_participation = BTreeMap::<ParticipationRef, usize>::new();
        let mut tasks_by_participation = BTreeMap::<ParticipationRef, Vec<TaskIdentity>>::new();
        for task in graph.tasks() {
            *tasks_per_participation
                .entry(task.participation())
                .or_default() += 1;
            tasks_by_participation
                .entry(task.participation())
                .or_default()
                .push(task.identity());
        }
        for (&participation, &tasks) in &tasks_per_participation {
            let admission_epoch_capability = admission_epochs
                .get(&participation.backend_process_id())
                .copied()
                .ok_or_else(|| {
                    TaskExecutionError::Schedule(format!(
                        "backend {} has no frozen admission epoch capability",
                        participation.backend_process_id()
                    ))
                })?;
            owners.insert(
                participation,
                ParticipationOwner::new(
                    participation,
                    tasks,
                    native_compatibility_id,
                    admission_epoch_capability,
                    requested_leases,
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
            dispatcher.register_task(node.identity().backend_process_id())?;
            stage_of_task.insert(task_id, node.stage_id());
            stage_tasks.entry(node.stage_id()).or_default().insert(
                task_id,
                RemoteTask::new(
                    node,
                    seed,
                    graph
                        .edges()
                        .filter(|edge| edge.producers().contains(&node.identity()))
                        .map(|edge| edge.edge_id())
                        .collect(),
                    &mut ids,
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
            stages.insert(stage_id, StageExecution::new(node.stage(), root, tasks)?);
        }

        let strict_pending = graph
            .tasks()
            .map(|task| task.identity())
            .filter(|&identity| edges.requires_drain(identity))
            .collect();
        let covered_observation = graph
            .participations()
            .copied()
            .map(|participation| (participation, CoveredParticipationObservation::default()))
            .collect();
        let stage_demand = crate::stage::demand::StageDemandObservations::new(&graph);
        Ok(Self {
            graph,
            stages,
            stage_of_task,
            owners,
            diagnostic_sources,
            dispatcher,
            edges,
            derived_edges: EdgeDerivations::default(),
            stage_demand,
            stage_propagation: None,
            stage_parent_debts: BTreeMap::new(),
            stage_cancel_debts: BTreeMap::new(),
            now,
            ids,
            covered_observation_active: false,
            covered_observation,
            covered_gaps: BTreeSet::new(),
            covered_gap_deadlines: BTreeSet::new(),
            strict_pending,
            inbound_producers,
            tasks_by_participation,
            deployment_window_limits,
            deployment_window: BTreeMap::new(),
            edge_control_effects: VecDeque::new(),
            operation_targets: BTreeMap::new(),
            pending_candidates: VecDeque::new(),
            released_native_inputs: VecDeque::new(),
            expired_actor_aborts: Vec::new(),
            status_reconciliations: BTreeSet::new(),
            terminal_retired_reconciliations: BTreeSet::new(),
            covered_reconciliations: BTreeSet::new(),
            normal_drain_requested: false,
            normal_drain_started: false,
            normal_drain_cursor: None,
            profile_release_deadline: None,
            terminal_cleanup_started: false,
            terminal_cleanup_cursor: None,
            failure: TerminationLatch::open(),
            accepted_protocol_revision: 0,
            accepted_terminal_tasks: Vec::new(),
            accepted_terminal_seen: BTreeSet::new(),
            accepted_protocol_dirty_tasks: BTreeSet::new(),
            accepted_protocol_writer_evidence: BTreeMap::new(),
            required_result_terminal_control_deadline: None,
            drained_tasks: BTreeMap::new(),
            stage_demand_cuts: BTreeMap::new(),
            withdrawal_materialized: BTreeSet::new(),
            released_outputs: BTreeSet::new(),
            released_children_of: BTreeSet::new(),
        })
    }

    /// Installs the complete admission set acquired by a qualified
    /// replacement. Missing, duplicate, or foreign evidence is rejected
    /// before the first Task-protocol turn.
    pub(crate) fn adopt_replacement_admissions(
        &mut self,
        admissions: &[ReplacementWorkerAdmissionEvidence],
    ) -> Result<(), TaskExecutionError> {
        let expected = self.owners.keys().copied().collect::<BTreeSet<_>>();
        let actual = admissions
            .iter()
            .map(|evidence| evidence.participation)
            .collect::<BTreeSet<_>>();
        if admissions.len() != actual.len() || actual != expected {
            return Err(TaskExecutionError::Schedule(
                "qualified replacement admission set differs from the Task manifest participations"
                    .to_owned(),
            ));
        }
        let now = self.now;
        for admission in admissions.iter() {
            self.owners
                .get_mut(&admission.participation)
                .expect("the complete replacement admission set was validated")
                .adopt_replacement_admission(admission.receipt, now)?;
        }
        Ok(())
    }

    pub const fn graph(&self) -> &TaskGraph {
        &self.graph
    }

    pub(crate) fn set_now(&mut self, now: MonotonicInstant) -> Result<(), TaskExecutionError> {
        if now < self.now {
            return Err(TaskExecutionError::Schedule(
                "attempt time regressed".into(),
            ));
        }
        self.now = now;
        Ok(())
    }
    pub(crate) fn issue_operation_id(&mut self) -> TaskOperationId {
        self.ids.issue()
    }

    pub(crate) fn monotonic_now(&self) -> MonotonicInstant {
        self.now
    }

    pub(crate) fn retain_profile_diagnostics_until(&mut self, deadline: MonotonicInstant) {
        self.profile_release_deadline = Some(
            self.profile_release_deadline
                .map_or(deadline, |old| old.min(deadline)),
        );
    }

    pub(crate) fn release_profile_diagnostics(&mut self) {
        self.profile_release_deadline = None;
    }

    #[cfg(test)]
    pub const fn dispatcher(&self) -> &OperationDispatcher {
        &self.dispatcher
    }

    pub fn stage(&self, stage_id: StageId) -> Option<&StageExecution> {
        self.stages.get(&stage_id)
    }

    pub fn owner(&self, participation: ParticipationRef) -> Option<&ParticipationOwner> {
        self.owners.get(&participation)
    }

    /// Read an exact pre-ready participation's original admission horizon using
    /// the same Task clock that accepted its receipt, never an actor clock.
    pub(crate) fn pre_ready_observation_remaining(
        &self,
        backend: BackendProcessId,
    ) -> Option<Duration> {
        self.owners
            .iter()
            .find(|(participation, _)| participation.backend_process_id() == backend)
            .and_then(|(_, owner)| owner.pre_ready_observation_remaining(self.now))
    }

    pub(crate) fn rejected_establish_observation_remaining(
        &self,
        participation: ParticipationRef,
        operation_id: TaskOperationId,
    ) -> Option<Duration> {
        self.owners
            .get(&participation)?
            .rejected_establish_observation_remaining(operation_id, self.now)
    }

    /// Only the Task that accepted this exact Create refusal retains its
    /// snapshot. A participation/op correlation cannot manufacture that evidence.
    pub(crate) fn rejected_create_observation_remaining(
        &self,
        participation: ParticipationRef,
        operation_id: TaskOperationId,
    ) -> Option<Duration> {
        self.graph
            .tasks()
            .filter(|task| task.participation() == participation)
            .find_map(|task| {
                self.task_by_identity_ref(task.identity())?
                    .rejected_create_observation_remaining(operation_id, self.now)
            })
    }

    /// Takes participations whose task-operation acknowledgement requires an
    /// immediate status resubscription from the frontend's held cursors.
    pub fn take_status_reconciliations(&mut self) -> BTreeSet<ParticipationRef> {
        std::mem::take(&mut self.status_reconciliations)
    }

    pub(crate) fn take_covered_reconciliations(&mut self) -> BTreeSet<ParticipationRef> {
        std::mem::take(&mut self.covered_reconciliations)
    }

    pub(crate) fn take_expired_actor_aborts(&mut self) -> Vec<TaskOperationId> {
        std::mem::take(&mut self.expired_actor_aborts)
    }

    /// Every backend's sealed runtime-filter observation, as its release
    /// reported it, with whether the set is complete.
    ///
    /// A participation contributes an entry only if its release settled *and*
    /// carried a contribution. The two reasons an entry is missing are not the
    /// same fact and are not collapsed here: a participation that released without
    /// one installed no participant on that backend, which is an ordinary
    /// query with no runtime filter there; a participation that never released has
    /// an answer still outstanding. `is_complete` is the second question, and
    /// it is what stops an outstanding release from reading as an absent
    /// filter.
    pub fn released_runtime_filter_contributions(&self) -> ReleasedRuntimeFilterContributions {
        ReleasedRuntimeFilterContributions {
            contributions: self
                .owners
                .iter()
                .filter_map(|(participation, owner)| {
                    owner
                        .runtime_filter_contribution()
                        .map(|telemetry| (participation.backend_process_id(), telemetry.clone()))
                })
                .collect(),
            complete: self.owners.values().all(ParticipationOwner::is_released),
            #[cfg(test)]
            participations: self.owners.len(),
        }
    }

    pub(crate) const fn accepted_protocol_revision(&self) -> u64 {
        self.accepted_protocol_revision
    }

    pub(crate) fn accepted_terminal_tasks(&self) -> &[TaskIdentity] {
        &self.accepted_terminal_tasks
    }

    pub(crate) fn accepted_protocol_task_changes(&self) -> &BTreeSet<TaskId> {
        &self.accepted_protocol_dirty_tasks
    }

    pub(crate) fn clear_accepted_protocol_task_changes(&mut self) {
        self.accepted_protocol_dirty_tasks.clear();
    }

    pub(crate) fn accepted_protocol_writer_witness(
        &self,
        task: TaskId,
    ) -> Option<&Arc<TaskStatus>> {
        self.accepted_protocol_writer_evidence.get(&task)
    }

    fn note_accepted_protocol_task(
        &mut self,
        identity: TaskIdentity,
    ) -> Result<(), TaskExecutionError> {
        let task_id = identity.task_id();
        if !self
            .accepted_protocol_writer_evidence
            .contains_key(&task_id)
            && let Some(status) = self
                .task(task_id)
                .and_then(RemoteTask::status)
                .filter(|status| status.writer().is_some())
        {
            // One original accepted writer witness per frozen Task. A later
            // coalesced status cannot erase the undeclared-writer evidence.
            let witness = Arc::new(status.clone());
            self.accepted_protocol_writer_evidence
                .insert(task_id, witness);
        }
        if self
            .task(task_id)
            .and_then(RemoteTask::status)
            .is_some_and(|status| status.is_terminal())
            && self.accepted_terminal_seen.insert(identity)
        {
            self.accepted_terminal_tasks.push(identity);
        }
        self.accepted_protocol_dirty_tasks.insert(task_id);
        self.note_accepted_protocol_change()
    }

    fn note_accepted_protocol_change(&mut self) -> Result<(), TaskExecutionError> {
        self.accepted_protocol_revision = self
            .accepted_protocol_revision
            .checked_add(1)
            .ok_or_else(|| {
                TaskExecutionError::Schedule(
                    "accepted protocol owner revision exhausted".to_owned(),
                )
            })?;
        Ok(())
    }

    fn latch_accepted_task_failure(
        &mut self,
        identity: TaskIdentity,
        detail: TerminationDetail,
    ) -> Result<(), TaskExecutionError> {
        match self.failure.latch(identity, detail) {
            LatchOutcome::Won | LatchOutcome::Refined(_) => self.note_accepted_protocol_change(),
            LatchOutcome::Lost(_) => Ok(()),
        }
    }

    pub fn task(&self, task_id: TaskId) -> Option<&RemoteTask> {
        self.stage_of_task
            .get(&task_id)
            .and_then(|stage_id| self.stages.get(stage_id))
            .and_then(|stage| stage.task(task_id))
    }

    /// Produces every intent the owners currently have and releases bounded
    /// batches to the sink.
    ///
    /// Order matters only for latency, never for correctness: every owner
    /// refuses to hand out a request twice, so the same call made in a
    /// different order releases the same set.

    fn expire_queued_operations(
        &mut self,
        now: MonotonicInstant,
    ) -> Result<(), TaskExecutionError> {
        let expired = self.dispatcher.drain_expired(now);
        let mut first_expiry = None;
        for expired in expired {
            let (intent, waited, original_grant) = expired.into_parts();
            let operation_id = intent.operation_id();
            let kind = intent.kind();
            self.released_native_inputs.push_back(original_grant);
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
        Ok(())
    }

    /// Dispatch only previously authorized actor Abort effects while ordinary
    /// admission is paused. Queue deadlines, permits and settlements remain
    /// with the same dispatcher and transport owners.

    /// Records one domain fact for one task.
    ///
    /// This is the seam the split-assignment and runtime-filter owners use.
    /// The fact reaches the wire only when the task itself may send.

    /// Reserves the two capacities an actor-owned Abort needs before its
    /// bounded adapter slot is released.
    ///
    /// The preview remains in the adapter while this runs. A successful
    /// result owns a real process queue permit, and the serial dispatcher has
    /// proved that the exact backend's lifecycle lane can advance now. The
    /// adapter slot is never treated as either authority.

    /// Commits one actor-owned Abort after its exact adapter effect is taken.

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
        if intent.kind() != OperationKind::AbortParticipation {
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
    /// participation owner so normal attempt convergence can observe it.
    pub(crate) fn observe_actor_abort_participation_closed(
        &mut self,
        participation: ParticipationRef,
    ) -> Result<(), TaskExecutionError> {
        let owner = self
            .owners
            .get_mut(&participation)
            .ok_or(TaskExecutionError::UnknownOperation)?;
        owner.observe_actor_abort_closure();
        self.refresh_release_diagnostic(participation)
    }

    /// Requests normal closure after the root seal. Strict producers and
    /// consumers keep their original admission and input owners until their
    /// Task terminals arrive; result demand cannot quiesce required work.
    pub(crate) fn begin_normal_drain(&mut self) {
        self.normal_drain_requested = true;
        self.try_begin_normal_drain();
    }

    pub(crate) fn normal_drain_started(&self) -> bool {
        self.normal_drain_started
    }

    pub(crate) fn root_seal_facts(
        &self,
        current_turn_failed: bool,
    ) -> crate::root_control::SealFacts {
        use crate::root_control::{FailureReadiness, RootCompletion, SealFacts};
        let root = self.graph.root_identity();
        let status = self.task_by_identity_ref(root).and_then(RemoteTask::status);
        let root = match status {
            Some(status) if status.identity() == root && status.state() == TaskState::Finished => {
                RootCompletion::ExactFinished
            }
            Some(status) if status.is_terminal() => RootCompletion::TerminalWithoutSuccess,
            _ => RootCompletion::Pending,
        };
        let failure = match self.failure_cause() {
            None => FailureReadiness::Empty,
            Some(cause) if cause.is_derived() => FailureReadiness::DerivedPending,
            Some(_) => FailureReadiness::Authoritative,
        };
        SealFacts {
            root,
            failure,
            current_turn_failed,
            terminal_control_required: self.result_terminal_control_required(),
            coverage_ready: !self.covered_observation_active || self.covered_observation_ready(),
            coverage_exhausted: self.covered_recovery_expired(),
            normal_drain_can_start: !self.terminal_cleanup_started
                && failure == FailureReadiness::Empty
                && !current_turn_failed
                && self.strict_pending.is_empty(),
        }
    }

    /// Logical no-more-input is immediate. Physical graph cleanup follows a
    /// charged cursor; the core must check this gate before every candidate
    /// and every queue-to-send handoff, including during a partial scan.
    fn try_begin_normal_drain(&mut self) {
        if !self.normal_drain_requested
            || self.normal_drain_started
            || self.terminal_cleanup_started
            || self.failure_cause().is_some()
            || !self.strict_pending.is_empty()
        {
            return;
        }
        self.normal_drain_started = true;
        self.normal_drain_cursor = Some(NormalDrainCursor { after: None });
    }
    pub(crate) fn advance_normal_drain(
        &mut self,
        context: &mut dyn crate::capacity::StepContext,
    ) -> Result<bool, TaskExecutionError> {
        self.try_begin_normal_drain();
        let Some(cursor) = self.normal_drain_cursor.as_ref() else {
            return Ok(false);
        };
        if !context.charge(1) {
            return Ok(false);
        }
        let next = self
            .graph
            .task_after(cursor.after)
            .map(|task| task.identity());
        match next {
            Some(identity) => {
                self.stand_down_task_normally(identity)?;
                self.normal_drain_cursor
                    .as_mut()
                    .expect("same original scan")
                    .after = Some(identity.task_id());
            }
            None => self.normal_drain_cursor = None,
        }
        Ok(true)
    }
    pub(crate) fn has_pending_normal_drain(&self) -> bool {
        self.normal_drain_cursor.is_some()
    }

    /// The immediate admission/send gate does not depend on the physical
    /// cleanup cursor reaching this Task. Exact destination-close controls
    /// survive normal withdrawal; terminal cleanup suppresses all Task input.
    fn task_input_allowed(
        &self,
        identity: TaskIdentity,
        destination_close: bool,
    ) -> Result<bool, TaskExecutionError> {
        let task = self
            .task_by_identity_ref(identity)
            .ok_or(TaskExecutionError::UnknownOperation)?;
        Ok(!self.terminal_cleanup_started
            && (destination_close
                || (!self.normal_drain_started
                    && !self.stage_demand_withdraws(identity)
                    && !task.normally_stood_down())))
    }
    /// Checks the original immutable input, never a caller-supplied class.
    /// The concrete driver uses this before both Native admission and dispatch.
    pub(crate) fn task_operation_allowed(
        &self,
        operation: &OperationIntent,
    ) -> Result<bool, TaskExecutionError> {
        match operation {
            OperationIntent::CreateTask { identity, .. } => {
                self.task_input_allowed(*identity, false)
            }
            OperationIntent::UpdateTask { identity, .. } => {
                self.task_input_allowed(*identity, operation.requires_control_progress())
            }
            _ => Err(TaskExecutionError::Schedule(
                "Task input gate received a non-Task-input operation".into(),
            )),
        }
    }
    /// Original external split/filter entry. Drain rejects NEW updates; this
    /// differs from an already admitted input's late grant disposition, and
    /// from the original internal exact-close continuation kept during drain.
    pub(crate) fn prepare_task_update(
        &mut self,
        identity: TaskIdentity,
        update: TaskDomainUpdate,
    ) -> Result<(UpdateAdmission, Option<OperationIntent>), TaskExecutionError> {
        if self.normal_drain_started || self.terminal_cleanup_started {
            return Err(TaskExecutionError::Schedule(
                "attempt drain rejects new task updates".into(),
            ));
        }
        self.apply_prior_withdrawal(identity)?;
        let (stage_id, task_id) = self
            .locate(identity)
            .ok_or(TaskExecutionError::UnknownOperation)?;
        self.stages
            .get_mut(&stage_id)
            .expect("located stage")
            .task_mut_observed(task_id, &self.stage_demand)
            .expect("located Task")
            .begin_input_preparation(update, &mut self.ids)
    }
    /// A late original grant observes the logical cut even if the graph scan
    /// has not reached its Task. The same grant is returned exactly once by
    /// that Task's ordinary cleanup leg, never dropped as a passive reference.
    pub(crate) fn complete_task_input_preparation(
        &mut self,
        identity: TaskIdentity,
        operation: TaskOperationId,
        grant: QueuedInputGrantRef,
    ) -> Result<UpdateAdmission, TaskExecutionError> {
        self.apply_prior_withdrawal(identity)?;
        if self.terminal_cleanup_started {
            self.task_by_identity(identity)
                .ok_or(TaskExecutionError::UnknownOperation)?
                .discard_creation_after_attempt_failure();
        } else if self.normal_drain_started {
            self.stand_down_task_normally(identity)?;
        }
        self.task_by_identity(identity)
            .ok_or(TaskExecutionError::UnknownOperation)?
            .complete_input_preparation(operation, grant)
    }

    /// Materialize an already accepted global normal cut on this exact
    /// Task BEFORE accepting a later primary fact that could discard input.
    /// This preserves the first disposition and the original ACK priority,
    /// regardless of whether the background cursor has reached this row.
    fn stage_demand_withdraws(&self, identity: TaskIdentity) -> bool {
        let Some(&frontier) = self.stage_demand_cuts.get(&identity.stage_id()) else {
            return false;
        };
        if self
            .stages
            .get(&identity.stage_id())
            .and_then(StageExecution::root_task)
            == Some(identity.task_id())
        {
            return false;
        }
        !self.edges.requires_drain(identity)
            || self
                .drained_tasks
                .get(&identity.task_id())
                .is_some_and(|&ordinal| ordinal <= frontier)
    }
    fn apply_prior_withdrawal(&mut self, identity: TaskIdentity) -> Result<(), TaskExecutionError> {
        if !self.withdrawal_materialized.contains(&identity.task_id())
            && (self.normal_drain_started || self.stage_demand_withdraws(identity))
        {
            self.stand_down_task_normally(identity)?;
        }
        Ok(())
    }
    /// Current proof linearizes one original producer-stage authorization.
    /// Task fanout materializes this accepted cut. Consumer regression cannot
    /// revoke it; a later strict terminal cannot enter its original frontier.
    fn accept_stage_demand_cut(
        &mut self,
        parent: StageId,
        producer: StageId,
    ) -> Result<(), TaskExecutionError> {
        if self.terminal_cleanup_started
            || self.released_children_of.contains(&parent)
            || !self.graph.is_producer_stage(parent, producer)
            || self.stage_demand.producer() != Some(producer)
            || self.stage_demand.all_consumers_released() != Some(true)
        {
            return Err(TaskExecutionError::Schedule(
                "stage demand cut lacks its original current consumer proof".into(),
            ));
        }
        self.stage_demand_cuts
            .insert(producer, self.drained_tasks.len());
        self.stage_demand.finish();
        Ok(())
    }

    /// Applies one exact local no-more-input authorization. An unknown Create
    /// outcome is kept for Participation fencing, but cannot keep this destination
    /// in the producer's opening barrier or revive its normal input need.
    pub(crate) fn stand_down_task_normally(
        &mut self,
        identity: TaskIdentity,
    ) -> Result<(), TaskExecutionError> {
        let task = self
            .task_by_identity_ref(identity)
            .ok_or(TaskExecutionError::UnknownOperation)?;
        // Strict participants finish their own inputs and outputs. A demand
        // release cannot suppress an unsent Create/update or cancel that work.
        if self.edges.requires_drain(identity) && !task.is_terminal() {
            return Ok(());
        }
        if task
            .status()
            .and_then(TaskStatus::termination)
            .is_none_or(|detail| detail.is_success_compatible())
        {
            self.close_destination_normally(identity);
        }
        let operations = self
            .task_by_identity_ref(identity)
            .expect("same original Task")
            .withdrawal_operation_ids();
        let expected = OperationTarget::Task {
            stage: identity.stage_id(),
            task: identity.task_id(),
        };
        for operation_id in operations.into_iter().flatten() {
            let kind = self.dispatcher.queued_operation_kind(operation_id);
            if kind != Some(OperationKind::CreateTask)
                && (kind != Some(OperationKind::UpdateTask)
                    || self.dispatcher.queued_destination_close(operation_id))
            {
                continue;
            }
            if self.operation_targets.get(&operation_id) != Some(&expected) {
                return Err(TaskExecutionError::Schedule(
                    "original Task withdrawal differs from its exact queued operation owner".into(),
                ));
            }
            self.dispatcher.cancel_queued(operation_id)?;
            self.operation_targets.remove(&operation_id);
            self.rollback_unsent(expected, operation_id);
        }
        self.task_by_identity(identity)
            .ok_or(TaskExecutionError::UnknownOperation)?
            .begin_normal_stand_down();
        self.settle_task_drain(identity.stage_id(), identity.task_id());
        self.withdrawal_materialized.insert(identity.task_id());
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
        // Retire only debt present at this cut. A later in-flight ACK may
        // create a new original reconciliation which must remain observable.
        self.terminal_retired_reconciliations = std::mem::take(&mut self.status_reconciliations);
        self.normal_drain_cursor = None;
        if let Some(cursor) = self.stage_propagation.take() {
            let parent = match cursor {
                StagePropagationCursor::Children { parent, .. }
                | StagePropagationCursor::Consumers { parent, .. }
                | StagePropagationCursor::Tasks { parent, .. } => parent,
            };
            self.stage_parent_debts
                .get_mut(&parent)
                .expect("same active parent traversal")
                .scan_cancelled = true;
            self.finish_stage_parent_if_ready(parent);
        }
        self.stage_demand.finish();
        self.terminal_cleanup_cursor = Some(TerminalCleanupCursor::Tasks(None));
    }
    pub(crate) fn has_pending_terminal_cleanup(&self) -> bool {
        self.terminal_cleanup_cursor.is_some()
    }
    /// One original table row per charge. The terminal gate above already
    /// prevents future business issuance; no large collection is cleared or
    /// destroyed in the input that accepted the failure.
    pub(crate) fn advance_terminal_cleanup(
        &mut self,
        context: &mut dyn crate::capacity::StepContext,
    ) -> Result<bool, TaskExecutionError> {
        let Some(cursor) = self.terminal_cleanup_cursor.as_ref() else {
            return Ok(false);
        };
        if !context.charge(1) {
            return Ok(false);
        }
        match *cursor {
            TerminalCleanupCursor::Tasks(after) => {
                if let Some(identity) = self.graph.task_after(after).map(|task| task.identity()) {
                    self.apply_prior_withdrawal(identity)?;
                    self.task_by_identity(identity)
                        .expect("frozen Task")
                        .discard_creation_after_attempt_failure();
                    if let Some(positions) = self
                        .deployment_window
                        .get_mut(&identity.backend_process_id())
                    {
                        positions.remove(&identity);
                        if positions.is_empty() {
                            self.deployment_window
                                .remove(&identity.backend_process_id());
                        }
                    }
                    self.terminal_cleanup_cursor =
                        Some(TerminalCleanupCursor::Tasks(Some(identity.task_id())));
                } else {
                    self.terminal_cleanup_cursor = Some(TerminalCleanupCursor::Owners(None));
                }
            }
            TerminalCleanupCursor::Owners(after) => {
                let next = match after {
                    Some(after) => self
                        .owners
                        .range_mut((std::ops::Bound::Excluded(after), std::ops::Bound::Unbounded))
                        .next(),
                    None => self.owners.iter_mut().next(),
                };
                if let Some((&participation, owner)) = next {
                    owner.begin_terminal_cleanup();
                    self.terminal_cleanup_cursor =
                        Some(TerminalCleanupCursor::Owners(Some(participation)));
                } else {
                    self.terminal_cleanup_cursor = Some(TerminalCleanupCursor::Queued(None));
                }
            }
            TerminalCleanupCursor::Queued(after) => {
                let next = match after {
                    Some(after) => self
                        .operation_targets
                        .range((std::ops::Bound::Excluded(after), std::ops::Bound::Unbounded))
                        .next(),
                    None => self.operation_targets.first_key_value(),
                }
                .map(|(&operation, &target)| (operation, target));
                if let Some((operation, target)) = next {
                    if target != OperationTarget::ActorAbort
                        && self.dispatcher.operation_state(operation)?
                            == DispatchOperationState::Queued
                    {
                        self.dispatcher.cancel_queued(operation)?;
                        self.operation_targets.remove(&operation);
                        self.rollback_unsent(target, operation);
                    }
                    self.terminal_cleanup_cursor =
                        Some(TerminalCleanupCursor::Queued(Some(operation)));
                } else {
                    self.terminal_cleanup_cursor = Some(TerminalCleanupCursor::Reconciliations);
                }
            }
            TerminalCleanupCursor::Reconciliations => {
                if self.terminal_retired_reconciliations.pop_first().is_none() {
                    self.terminal_cleanup_cursor = None;
                }
            }
        }
        Ok(true)
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
            OperationTarget::Participation(participation) => {
                self.acknowledge_participation(participation, ack)
            }
            // The permit is already released above. The verdict belongs to the
            // domain owner, which read this acknowledgement from the observer
            // seam before the runner got here, so applying it a second time
            // would be a second authority over the same progression.
            OperationTarget::ActorAbort => Ok(()),
        }
    }

    fn acknowledge_task(
        &mut self,
        stage_id: StageId,
        task_id: TaskId,
        ack: &OperationAcknowledgement,
    ) -> Result<(), TaskExecutionError> {
        let identity = self
            .stages
            .get(&stage_id)
            .and_then(|stage| stage.task(task_id))
            .ok_or(TaskExecutionError::UnknownOperation)?
            .identity();
        self.apply_prior_withdrawal(identity)?;
        let result = self.acknowledge_task_inner(stage_id, task_id, ack);
        // Local FailedClosed can become terminal without a TaskStatus or Task
        // failure latch. Count it even on Err before the outer failure policy
        // selects its original cause or a sealed result's residual cleanup.
        self.settle_task_drain(stage_id, task_id);
        result
    }

    fn acknowledge_task_inner(
        &mut self,
        stage_id: StageId,
        task_id: TaskId,
        ack: &OperationAcknowledgement,
    ) -> Result<(), TaskExecutionError> {
        let stage = self
            .stages
            .get_mut(&stage_id)
            .ok_or(TaskExecutionError::UnknownOperation)?;
        let mut task = stage
            .task_mut_observed(task_id, &self.stage_demand)
            .ok_or(TaskExecutionError::UnknownOperation)?;
        let participation = task.participation();
        match ack.kind() {
            OperationKind::CreateTask => {
                let identity = task.identity();
                let participation = task.participation();
                let was_owned = task.create_ownership_proven();
                let participation_established = self
                    .owners
                    .get(&participation)
                    .is_some_and(ParticipationOwner::establish_acknowledged);
                let observation_horizon = self
                    .owners
                    .get(&participation)
                    .and_then(ParticipationOwner::create_refusal_observation_horizon);
                let previous_status = task.status().map(TaskStatus::version);
                let settlement = task.on_create_ack(
                    ack,
                    self.now,
                    participation_established,
                    observation_horizon,
                )?;
                let accepted_status_changed =
                    previous_status != task.status().map(TaskStatus::version);
                if !matches!(settlement, CreateSettlement::Created) {
                    if let CreateSettlement::FailedClosed(_) = settlement {
                        return Err(TaskExecutionError::OperationFailed {
                            participation,
                            operation_id: ack.operation_id(),
                            kind: OperationKind::CreateTask,
                            rejection: ack
                                .worker_header()
                                .expect("a checked Worker verdict has its original branch"),
                        });
                    }
                    // A genuinely unknown outcome leaves the identical request
                    // to be handed out again by the next pump.
                    return Ok(());
                }
                if !was_owned
                    && task.create_ownership_proven()
                    && let Some(owner) = self.owners.get_mut(&participation)
                {
                    owner.note_create_owned();
                }
                drop(task);
                // An applied Create ACK can adopt its original TaskStatus;
                // this path does not contribute to StatusReport.accepted.
                if accepted_status_changed {
                    self.note_accepted_protocol_task(identity)?;
                }
                self.reconcile_destination_input(identity)?;
                self.settle_task_drain(stage_id, task_id);
                self.accept_task_failure(identity)?;
                Ok(())
            }
            OperationKind::UpdateTask => match task.on_update_ack(ack)? {
                UpdateSettlement::FailedClosed(_) => Err(TaskExecutionError::OperationFailed {
                    participation,
                    operation_id: ack.operation_id(),
                    kind: OperationKind::UpdateTask,
                    rejection: ack
                        .worker_header()
                        .expect("a checked Worker verdict has its original branch"),
                }),
                UpdateSettlement::AwaitingTerminalStatus => {
                    self.status_reconciliations.insert(participation);
                    Ok(())
                }
                _ => Ok(()),
            },
            OperationKind::CancelTask => task.on_cancel_ack(ack),
            kind => Err(TaskExecutionError::MissingReceipt(kind)),
        }
    }

    fn acknowledge_participation(
        &mut self,
        participation: ParticipationRef,
        ack: &OperationAcknowledgement,
    ) -> Result<(), TaskExecutionError> {
        let outcome = self.acknowledge_participation_inner(participation, ack);
        let diagnostic = self.refresh_release_diagnostic(participation);
        // FailedClosed is still an accepted closure observation. Preserve the
        // original protocol error if both validations reject the same input.
        outcome.and(diagnostic)
    }

    fn refresh_release_diagnostic(
        &mut self,
        participation: ParticipationRef,
    ) -> Result<(), TaskExecutionError> {
        let owner = self
            .owners
            .get(&participation)
            .ok_or(TaskExecutionError::UnknownOperation)?;
        self.diagnostic_sources.observe_release(
            participation,
            owner.is_released(),
            owner.runtime_filter_contribution().copied(),
        )
    }

    pub(crate) fn diagnostic_sources(
        &self,
    ) -> &crate::diagnostics::sources::DiagnosticSourcesState {
        &self.diagnostic_sources
    }

    pub(crate) fn observe_split_diagnostic(
        &mut self,
        source: PayloadRef,
        observation: crate::publication::SplitDiagnosticObservation,
    ) -> Result<(), TaskExecutionError> {
        self.diagnostic_sources.observe_split(source, observation)
    }

    fn acknowledge_participation_inner(
        &mut self,
        participation: ParticipationRef,
        ack: &OperationAcknowledgement,
    ) -> Result<(), TaskExecutionError> {
        let now = self.now;
        let owner = self
            .owners
            .get_mut(&participation)
            .ok_or(TaskExecutionError::UnknownOperation)?;
        match ack.kind() {
            OperationKind::AcquireParticipationAdmissionTicket => owner.on_admission_ack(ack, now),
            OperationKind::UpdateParticipation => {
                owner.on_participation_ack(ack)?;
                if owner.establish_acknowledged() {
                    for stage in self.stages.values_mut() {
                        for (_, mut task) in stage.tasks_mut_observed(&self.stage_demand) {
                            if task.participation() == participation {
                                task.on_participation_established();
                            }
                        }
                    }
                }
                Ok(())
            }
            OperationKind::QuiesceParticipation => {
                let receipt = owner.on_quiesce_ack(ack)?;
                if let Some(receipt) = receipt {
                    self.apply_quiesce_membership(participation, &receipt)?;
                }
                Ok(())
            }
            OperationKind::ReleaseParticipation => {
                owner
                    .on_release_ack(ack, now)
                    .and_then(|settlement| match settlement {
                        ReleaseSettlement::FailedClosed(_) => {
                            Err(TaskExecutionError::OperationFailed {
                                participation,
                                operation_id: ack.operation_id(),
                                kind: OperationKind::ReleaseParticipation,
                                rejection: ack
                                    .worker_header()
                                    .expect("a checked Worker verdict has its original branch"),
                            })
                        }
                        _ => Ok(()),
                    })
            }
            kind => Err(TaskExecutionError::MissingReceipt(kind)),
        }
    }

    fn apply_quiesce_membership(
        &mut self,
        participation: ParticipationRef,
        receipt: &novarocks_execution_contract::QuiesceParticipationReceipt,
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
            if task.participation() != participation {
                return Err(TaskExecutionError::DomainReceipt(format!(
                    "quiesce membership names task {identity} in another participation"
                )));
            }
        }
        let count = self
            .tasks_by_participation
            .get(&participation)
            .ok_or(TaskExecutionError::UnknownOperation)?
            .len();
        for index in 0..count {
            let identity = self.tasks_by_participation[&participation][index];
            self.apply_prior_withdrawal(identity)?;
            let (result, was_fenced_out, fenced_out) = {
                let mut task = self
                    .task_by_identity(identity)
                    .ok_or(TaskExecutionError::UnknownOperation)?;
                let was_fenced_out = task.fenced_out();
                let result = task.on_quiesce_membership(accepted.contains(&identity));
                (result, was_fenced_out, task.fenced_out())
            };
            // The local terminal aggregate survives an early error; physical
            // convergence and the outer business failure remain separate.
            self.settle_task_drain(identity.stage_id(), identity.task_id());
            let became_owned = result?;
            let owner = self
                .owners
                .get_mut(&participation)
                .ok_or(TaskExecutionError::UnknownOperation)?;
            if became_owned {
                owner.note_create_owned();
            } else if fenced_out && !was_fenced_out {
                owner.note_create_fenced_out();
            }
        }
        Ok(())
    }

    /// Applies an exact transport send fact on the same serial owner as ACKs.
    pub(crate) fn establish_send_started(
        &mut self,
        participation: ParticipationRef,
        operation_id: TaskOperationId,
    ) -> Result<(), TaskExecutionError> {
        self.owners
            .get_mut(&participation)
            .ok_or(TaskExecutionError::UnknownOperation)?
            .on_establish_send_started(operation_id)
    }

    /// Records the edge-open decision every edge this destination completed.
    ///
    /// The fact is recorded on each producer's own task. A producer that is
    /// still `Creating` queues it, so the decision may be made early while the
    /// wire request still waits for that producer's own acknowledgement.
    fn open_ready_edges(&mut self, destination: TaskIdentity) -> Result<(), TaskExecutionError> {
        self.derived_edges.created(destination);
        Ok(())
    }

    fn close_destination_normally(&mut self, destination: TaskIdentity) {
        self.derived_edges.normally_closed(destination);
    }

    pub(crate) fn has_pending_edge_derivation(&self) -> bool {
        self.derived_edges.has_pending()
    }

    /// Retains any produced fact in this same graph owner. A caller resumes
    /// on remaining work, never re-enqueues a returned copy of an effect.
    pub(crate) fn advance_edge_derivation(
        &mut self,
        context: &mut dyn crate::capacity::StepContext,
    ) -> Result<bool, TaskExecutionError> {
        let step = self.derived_edges.advance(
            &mut self.edges,
            !self.normal_drain_started && !self.terminal_cleanup_started,
            context,
        )?;
        match step {
            EdgeDerivationStep::Idle | EdgeDerivationStep::BudgetExhausted => Ok(false),
            EdgeDerivationStep::Progress => Ok(true),
            EdgeDerivationStep::Effect(effect) => {
                self.edge_control_effects.push_back(effect.into());
                Ok(true)
            }
        }
    }

    /// Moves frozen edge effects to the original per-task domain owner only
    /// after the process transport has reserved their bounded queue capacity.
    /// Backpressure leaves the exact decision in this graph-bounded queue.
    /// One full producer cannot block independent producer processes, while
    /// every producer retains its own close-before-open order.

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
                // Strict edges ignored the demand withdrawal above. This
                // installed Task still supplies their real opening fact.
                self.open_ready_edges(destination)?;
            }
        } else {
            self.open_ready_edges(destination)?;
        }
        Ok(())
    }

    /// Builds a reconnect request only from facts the serial owner has
    /// applied, never from transport receipt or an unconsumed intake slot.
    pub(crate) fn covered_subscription_request(
        &self,
        participation: ParticipationRef,
        generation: NonZeroU64,
    ) -> Result<DecodedCoveredSubscription, TaskExecutionError> {
        let state = self
            .covered_observation
            .get(&participation)
            .ok_or(TaskExecutionError::UnknownOperation)?;
        let request = DecodedCoveredSubscription {
            participation,
            generation,
            status_cursors: self.status_cursors(participation).into_boxed_slice(),
            task_convergence_cursors: self
                .graph
                .tasks()
                .filter(|task| task.participation() == participation)
                .map(|task| {
                    let identity = task.identity();
                    state.actual_stopped.get(&identity).map_or_else(
                        || TaskConvergenceCursor::unobserved(identity),
                        |receipt| TaskConvergenceCursor::at(identity, receipt.version()),
                    )
                })
                .collect(),
            participation_cursor: state.participation_convergence.map(|receipt| {
                ParticipationConvergenceCursor::at(participation, receipt.version())
            }),
            quiesce_cursor: state
                .quiesce
                .as_ref()
                .map(|receipt| QuiesceObservationCursor {
                    participation,
                    fence_version: NonZeroU64::new(receipt.fence_version()),
                }),
            required_identities: self
                .graph
                .tasks()
                .filter(|task| task.participation() == participation)
                .map(|task| task.identity())
                .collect(),
        };
        Ok(request)
    }

    pub(crate) fn next_covered_generation(
        &self,
        participation: ParticipationRef,
    ) -> Result<NonZeroU64, TaskExecutionError> {
        let observed = self
            .covered_observation
            .get(&participation)
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
    /// Ordinary reconnect or an unrelated Participation's catch-up does not.
    pub(crate) fn covered_observation_ready(&self) -> bool {
        self.covered_gaps.is_empty()
    }

    pub(crate) fn covered_gap_participations(&self) -> impl Iterator<Item = ParticipationRef> + '_ {
        self.covered_observation
            .iter()
            .filter_map(|(&participation, state)| state.gap_pending().then_some(participation))
    }

    pub(crate) fn covered_recovery_expired(&self) -> bool {
        self.covered_gap_deadlines
            .first()
            .is_some_and(|&(deadline, _)| self.now.has_reached(deadline))
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
            .and_then(|task| self.covered_observation.get(&task.participation()))
            .is_some_and(|state| state.actual_stopped.contains_key(&identity))
    }

    /// A receiver Participation may release only after every frozen inbound sender
    /// that the Worker actually accepted has stopped. A Task fenced out by
    /// its own complete Quiesce receipt has no remote sender to wait for.
    fn receiver_inputs_stopped(
        &self,
        participation: ParticipationRef,
    ) -> Result<bool, TaskExecutionError> {
        if !self.covered_observation_active {
            return Ok(true);
        }
        for &producer in self
            .inbound_producers
            .get(&participation)
            .into_iter()
            .flatten()
        {
            let task = self
                .task_by_identity_ref(producer)
                .ok_or(TaskExecutionError::UnknownOperation)?;
            if !task.fenced_out() && !self.task_actual_stopped(producer) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Keep this Participation's own stopped facts until dependent receivers have
    /// used them. A terminal status describes the Task's result, not the end
    /// of its local exchange and resource responsibilities.
    fn participation_tasks_stopped(
        &self,
        participation: ParticipationRef,
    ) -> Result<bool, TaskExecutionError> {
        if !self.covered_observation_active {
            return Ok(true);
        }
        for &identity in self
            .tasks_by_participation
            .get(&participation)
            .into_iter()
            .flatten()
        {
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

    /// Entity facts can enter this owner only after the current/old generation's
    /// immutable opening witness has passed the subscription policy. The
    /// value moves here without cloning its decoded frame on the poller.
    pub(crate) fn apply_covered_observation(
        &mut self,
        accepted: crate::observation::controller::AcceptedObservation,
    ) -> Result<StatusReport, TaskExecutionError> {
        let observation = accepted.into_observation();
        let ObservationEvent::Fact {
            source_revision,
            fact,
        } = observation.event
        else {
            return Err(TaskExecutionError::Schedule(
                "subscription policy admitted a non-entity observation".into(),
            ));
        };
        let mut report = StatusReport::default();
        self.apply_covered_frame(
            observation.participation,
            observation.generation.get(),
            CoveredStatusStreamEvent {
                source_revision,
                fact,
            },
            &mut report,
        )?;
        Ok(report)
    }

    fn apply_covered_frame(
        &mut self,
        participation: ParticipationRef,
        generation: u64,
        event: CoveredStatusStreamEvent,
        report: &mut StatusReport,
    ) -> Result<(), TaskExecutionError> {
        if generation == 0 {
            return Err(TaskExecutionError::Schedule(
                "covered observation generation must be nonzero".to_owned(),
            ));
        }
        let now = self.now;
        let state = self
            .covered_observation
            .get_mut(&participation)
            .ok_or(TaskExecutionError::UnknownOperation)?;
        state.begin_generation(generation);
        let gap_before = state.gap_pending();
        let deadline_before = state.recovery_deadline();
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
                CoveredStatusStreamFact::CatchUpComplete { .. }
                    | CoveredStatusStreamFact::Bookmark { .. }
            )
        {
            return Err(TaskExecutionError::Schedule(
                "catch-up fact followed the completed covered boundary".to_owned(),
            ));
        }
        match event.fact {
            CoveredStatusStreamFact::Status(status) => {
                let identity = status.identity();
                self.verify_covered_task(participation, identity)?;
                self.apply_published(&status, report)?;
                if status.is_terminal() {
                    self.covered_observation
                        .get_mut(&participation)
                        .expect("validated participation")
                        .missing_terminal
                        .remove(&identity);
                }
            }
            CoveredStatusStreamFact::Gone(identity) => {
                self.verify_covered_task(participation, identity)?;
                if !current_generation {
                    report.ignored += 1;
                    return Ok(());
                }
                let has_terminal = self
                    .task_by_identity_ref(identity)
                    .and_then(RemoteTask::status)
                    .is_some_and(TaskStatus::is_terminal);
                if has_terminal {
                    let task = self.task_by_identity_ref(identity).expect("validated task");
                    if matches!(task.observe_gone(), GoneObservation::TerminalNeverObserved) {
                        self.covered_observation
                            .get_mut(&participation)
                            .unwrap()
                            .note_status_gap(identity, now);
                    }
                } else {
                    self.covered_observation
                        .get_mut(&participation)
                        .unwrap()
                        .note_status_gap(identity, now);
                }
                report.ignored += 1;
            }
            CoveredStatusStreamFact::TaskConvergence(receipt) => {
                self.verify_covered_task(participation, receipt.identity())?;
                let state = self.covered_observation.get_mut(&participation).unwrap();
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
            CoveredStatusStreamFact::ParticipationConvergence(receipt) => {
                if receipt.participation() != participation {
                    return Err(TaskExecutionError::Schedule(
                        "Participation convergence observation names another participation"
                            .to_owned(),
                    ));
                }
                let state = self.covered_observation.get_mut(&participation).unwrap();
                if let Some(previous) = state.participation_convergence {
                    if previous != receipt {
                        return Err(TaskExecutionError::Schedule(
                            "Participation convergence receipt changed after application"
                                .to_owned(),
                        ));
                    }
                } else {
                    state.participation_convergence = Some(receipt);
                }
                report.ignored += 1;
            }
            CoveredStatusStreamFact::Quiesce(receipt) => {
                if receipt.participation() != participation {
                    return Err(TaskExecutionError::Schedule(
                        "Quiesce observation names another participation".to_owned(),
                    ));
                }
                if let Some(previous) = &self.covered_observation[&participation].quiesce
                    && (previous.fence_version() != receipt.fence_version()
                        || previous.accepted_tasks() != receipt.accepted_tasks())
                {
                    return Err(TaskExecutionError::Schedule(
                        "Quiesce observation changed its accepted membership".to_owned(),
                    ));
                }
                self.apply_quiesce_membership(participation, &receipt)?;
                self.owners
                    .get_mut(&participation)
                    .expect("validated participation")
                    .observe_quiesce(&receipt)?;
                self.covered_observation
                    .get_mut(&participation)
                    .unwrap()
                    .quiesce = Some(receipt);
                report.ignored += 1;
            }
            CoveredStatusStreamFact::StatusUnchanged(identity) => {
                self.verify_covered_task(participation, identity)?;
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
                self.verify_covered_task(participation, identity)?;
                if current_generation
                    && !self.covered_observation[&participation]
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
                self.verify_covered_task(participation, identity)?;
                // The source's initial cut can precede an Accepted ACK that
                // this serial owner applied before the Unknown frame arrived.
                // This absence at an older cut cannot retract ownership.
                report.ignored += 1;
            }
            CoveredStatusStreamFact::CatchUpComplete {
                generation: marker_generation,
                initial_cut,
            } => {
                if marker_generation != generation {
                    return Err(TaskExecutionError::Schedule(
                        "catch-up boundary names another stream generation".to_owned(),
                    ));
                }
                if current_generation {
                    let state = self.covered_observation.get_mut(&participation).unwrap();
                    if state.initial_complete {
                        return Err(TaskExecutionError::Schedule(
                            "covered catch-up boundary repeated".to_owned(),
                        ));
                    }
                    state.initial_complete = true;
                    state.applied_prefix = state.applied_prefix.max(initial_cut);
                    state.source_cut = state.source_cut.max(initial_cut);
                    if state.missing_terminal.is_empty()
                        && state.gap_generation.is_some_and(|gap| generation > gap)
                    {
                        state.gap_generation = None;
                        state.gap_since = None;
                    }
                }
                report.ignored += 1;
            }
            CoveredStatusStreamFact::Bookmark {
                generation: marker_generation,
                sequence,
                covered_prefix,
                source_cut,
            } => {
                if marker_generation != generation {
                    return Err(TaskExecutionError::Schedule(
                        "bookmark names another stream generation".to_owned(),
                    ));
                }
                if current_generation {
                    let state = self.covered_observation.get_mut(&participation).unwrap();
                    if sequence <= state.bookmark_sequence {
                        report.ignored += 1;
                        return Ok(());
                    }
                    if covered_prefix > source_cut
                        || source_cut < state.source_cut
                        || covered_prefix < state.applied_prefix
                        || (!state.initial_complete && covered_prefix != 0)
                    {
                        return Err(TaskExecutionError::Schedule(
                            "covered bookmark regressed or crossed an unfinished catch-up"
                                .to_owned(),
                        ));
                    }
                    state.bookmark_sequence = sequence;
                    state.source_cut = source_cut;
                    state.applied_prefix = covered_prefix;
                }
                report.ignored += 1;
            }
        }
        self.covered_observation
            .get_mut(&participation)
            .unwrap()
            .update_coverage_debt(now);
        let state = &self.covered_observation[&participation];
        if state.gap_pending() {
            self.covered_gaps.insert(participation);
            if !gap_before {
                self.covered_reconciliations.insert(participation);
            }
        } else {
            self.covered_gaps.remove(&participation);
        }
        let deadline_after = state.recovery_deadline();
        if deadline_before != deadline_after {
            if let Some(deadline) = deadline_before {
                self.covered_gap_deadlines
                    .remove(&(deadline, participation));
            }
            if let Some(deadline) = deadline_after {
                self.covered_gap_deadlines.insert((deadline, participation));
            }
        }
        Ok(())
    }

    fn verify_covered_task(
        &self,
        participation: ParticipationRef,
        identity: TaskIdentity,
    ) -> Result<(), TaskExecutionError> {
        identity
            .verify_participation(participation)
            .map_err(TaskExecutionError::Identity)?;
        self.locate(identity)
            .ok_or(TaskExecutionError::UnknownOperation)?;
        Ok(())
    }

    /// Applies what the intake queued, then propagates one layer of stage
    /// release.

    fn apply_published(
        &mut self,
        status: &TaskStatus,
        report: &mut StatusReport,
    ) -> Result<(), TaskExecutionError> {
        let identity = status.identity();
        self.apply_prior_withdrawal(identity)?;
        let result = self.apply_published_inner(status, report);
        if self.locate(identity).is_some() {
            self.settle_task_drain(identity.stage_id(), identity.task_id());
        }
        result
    }

    fn apply_published_inner(
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
        let mut task = stage
            .task_mut_observed(task_id, &self.stage_demand)
            .expect("the task was just located");
        let was_owned = task.create_ownership_proven();
        let accepted = match task.observe_status(status)? {
            StatusObservation::Accept => {
                report.accepted += 1;
                true
            }
            _ => {
                report.ignored += 1;
                false
            }
        };
        let became_owned = !was_owned && task.create_ownership_proven();
        let installed = task.installed();
        if became_owned && let Some(owner) = self.owners.get_mut(&task.participation()) {
            owner.note_create_owned();
        }
        let terminal = task.terminal_report();
        drop(task);
        if accepted {
            self.note_accepted_protocol_task(identity)?;
        }
        if let Some(terminal) = terminal {
            if let Some(detail) = &terminal.termination
                && !detail.is_success_compatible()
            {
                self.latch_accepted_task_failure(identity, detail.clone())?;
            }
            report.terminal.push(terminal);
        }
        if installed {
            self.reconcile_destination_input(identity)?;
        }
        self.settle_task_drain(stage_id, task_id);
        self.accept_task_failure(identity)?;
        Ok(())
    }

    fn accept_task_failure(&mut self, identity: TaskIdentity) -> Result<(), TaskExecutionError> {
        let detail = self
            .task_by_identity_ref(identity)
            .and_then(|task| task.status())
            .and_then(|status| status.termination())
            .filter(|detail| !detail.is_success_compatible())
            .cloned();
        if let Some(detail) = detail {
            self.latch_accepted_task_failure(identity, detail)?;
            // Freeze normal admission in the fact's own serial turn. Returning
            // W cannot authorize another Create after the attempt has failed.
            self.begin_terminal_cleanup();
        }
        Ok(())
    }

    /// Publishes one task's drain facts to its participation owner, once each.
    fn settle_task_drain(&mut self, stage_id: StageId, task_id: TaskId) {
        let Some(stage) = self.stages.get(&stage_id) else {
            return;
        };
        let Some(task) = stage.task(task_id) else {
            return;
        };
        let participation = task.participation();
        let terminal = task.is_terminal();
        if !task.needs_deployment_window() {
            let backend = task.identity().backend_process_id();
            if let Some(positions) = self.deployment_window.get_mut(&backend) {
                positions.remove(&task.identity());
                if positions.is_empty() {
                    self.deployment_window.remove(&backend);
                }
            }
        }
        if terminal {
            self.strict_pending.remove(&task.identity());
        }
        // Only a task that FINISHED owes an output responsibility; that state
        // is constructible on the backend precisely when the responsibility is
        // complete. A task that terminated any other way -- canceled because
        // it was no longer needed, aborted, or failed -- has none to complete
        // and will never publish one.
        //
        // The owner's counter has to agree with that, or it never reaches
        // `expected_tasks`, `locally_drained` stays false, and the release is
        // never even requested: the participation then holds its backend resources
        // until the lease expires while the frontend waits out its whole drain
        // budget. This is the per-participation half of the same fact the
        // attempt-level `all_output_released` checks; the two are derived
        // separately on purpose, so both have to say it.
        let output_settled =
            terminal && (task.task_state() != TaskState::Finished || task.output_released());
        let Some(owner) = self.owners.get_mut(&participation) else {
            return;
        };
        if terminal {
            let next = self
                .drained_tasks
                .len()
                .checked_add(1)
                .expect("frozen Task terminal ordinal");
            if let std::collections::btree_map::Entry::Vacant(entry) =
                self.drained_tasks.entry(task_id)
            {
                entry.insert(next);
                owner.note_task_drained();
            }
        }
        if output_settled && self.released_outputs.insert(task_id) {
            owner.note_output_released();
        }
    }

    /// Original stage-demand derivation, charged one Stage/consumer/Task at a
    /// time. The accepted stage cut precedes fanout and remains independent
    /// of Native admission debt, so Full cannot occupy the unique proof or
    /// block another producer's eligibility traversal.
    pub(crate) fn has_pending_stage_propagation(&self) -> bool {
        !self.terminal_cleanup_started
            && (self.stage_propagation.is_some() || self.stage_demand.has_pending_parents())
    }
    pub(crate) fn propagate_stage_release(
        &mut self,
        context: &mut dyn crate::capacity::StepContext,
    ) -> Result<bool, TaskExecutionError> {
        if self.terminal_cleanup_started {
            return Ok(false);
        }
        if let Some(StagePropagationCursor::Consumers { .. }) = self.stage_propagation
            && let Some(consumer) = self.stage_demand.next_consumer()
        {
            let released = self
                .stages
                .get(&consumer)
                .ok_or(TaskExecutionError::UnknownOperation)?
                .released_children();
            return self.stage_demand.check_next(consumer, released, context);
        }
        if !self.has_pending_stage_propagation() || !context.charge(1) {
            return Ok(false);
        }
        match self.stage_propagation {
            None => {
                let parent = self
                    .stage_demand
                    .next_parent()
                    .expect("real coalesced Stage change");
                if let Some(debt) = self.stage_parent_debts.get_mut(&parent) {
                    // Do not lose this actual new release while the old pass
                    // waits on a different Cancel's original Native input.
                    debt.rescan_requested = true;
                } else if !self.released_children_of.contains(&parent)
                    && self
                        .stages
                        .get(&parent)
                        .is_some_and(StageExecution::released_children)
                {
                    self.stage_parent_debts
                        .insert(parent, StageParentDebt::default());
                    self.stage_propagation = Some(StagePropagationCursor::Children {
                        parent,
                        after: None,
                    });
                }
            }
            Some(StagePropagationCursor::Children { parent, after }) => {
                if let Some(producer) = self.graph.producer_stage_after(parent, after) {
                    self.stage_demand.begin(producer)?;
                    self.stage_propagation =
                        Some(StagePropagationCursor::Consumers { parent, producer });
                } else {
                    self.stage_parent_debts
                        .get_mut(&parent)
                        .expect("original parent traversal")
                        .scan_done = true;
                    self.finish_stage_parent_if_ready(parent);
                    self.stage_propagation = None;
                }
            }
            Some(StagePropagationCursor::Consumers { parent, producer }) => {
                match self.stage_demand.all_consumers_released() {
                    Some(true) => {
                        self.accept_stage_demand_cut(parent, producer)?;
                        self.stage_propagation = Some(StagePropagationCursor::Tasks {
                            parent,
                            producer,
                            after: None,
                        });
                    }
                    Some(false) => {
                        if !self.stage_demand_cuts.contains_key(&producer) {
                            self.stage_parent_debts
                                .get_mut(&parent)
                                .expect("same original parent")
                                .retry_unproven = true;
                        }
                        self.stage_demand.finish();
                        self.stage_propagation = Some(StagePropagationCursor::Children {
                            parent,
                            after: Some(producer),
                        });
                    }
                    None => {
                        return Err(TaskExecutionError::Schedule(
                            "stage proof traversal lost its next consumer".into(),
                        ));
                    }
                }
            }
            Some(StagePropagationCursor::Tasks {
                parent,
                producer,
                after,
            }) => {
                let stage = self
                    .stages
                    .get(&producer)
                    .ok_or(TaskExecutionError::UnknownOperation)?;
                let next = stage.task_after(after).map(RemoteTask::identity);
                if let Some(identity) = next {
                    // Root exclusion precedes both physical no-more and Cancel.
                    if stage.root_task() != Some(identity.task_id()) {
                        self.apply_prior_withdrawal(identity)?;
                        let intent = {
                            let stage = self
                                .stages
                                .get_mut(&producer)
                                .expect("frozen producer stage");
                            let mut task = stage
                                .task_mut_observed(identity.task_id(), &self.stage_demand)
                                .expect("frozen producer Task");
                            task.normally_stood_down()
                                .then(|| {
                                    task.cancel_intent(
                                        CancelReason::UpstreamNoLongerNeeded,
                                        &mut self.ids,
                                    )
                                })
                                .flatten()
                        };
                        if let Some(intent) = intent {
                            let operation = intent.operation_id();
                            if let Err(error) = self.dispatcher.validate_operation_carrier(&intent)
                            {
                                self.rollback_unsent(
                                    OperationTarget::Task {
                                        stage: producer,
                                        task: identity.task_id(),
                                    },
                                    operation,
                                );
                                return Err(error);
                            }
                            let OperationIntent::CancelTask(request) = intent else {
                                unreachable!("original normal Cancel author")
                            };
                            assert!(
                                self.stage_cancel_debts
                                    .insert(operation, StageCancelDebt { parent, request })
                                    .is_none(),
                                "unique original Cancel operation"
                            );
                            self.stage_parent_debts
                                .get_mut(&parent)
                                .expect("same original parent traversal")
                                .cancels += 1;
                        }
                    }
                    self.stage_propagation = Some(StagePropagationCursor::Tasks {
                        parent,
                        producer,
                        after: Some(identity.task_id()),
                    });
                } else {
                    self.stage_propagation = Some(StagePropagationCursor::Children {
                        parent,
                        after: Some(producer),
                    });
                }
            }
        }
        Ok(true)
    }
    fn finish_stage_parent_if_ready(&mut self, parent: StageId) {
        if self
            .stage_parent_debts
            .get(&parent)
            .is_some_and(|debt| (debt.scan_done || debt.scan_cancelled) && debt.cancels == 0)
        {
            let debt = self
                .stage_parent_debts
                .remove(&parent)
                .expect("same original parent debt");
            if debt.scan_cancelled {
                return;
            }
            if !debt.retry_unproven {
                self.released_children_of.insert(parent);
            } else if debt.rescan_requested
                && self
                    .stages
                    .get(&parent)
                    .is_some_and(StageExecution::released_children)
            {
                self.stage_demand.revisit_parent(parent);
            }
        }
    }
    /// A read-only original request position. Full leaves this SAME request
    /// and parent debt here. The concrete driver correlates any outstanding
    /// Native preparation; repeatedly reading this value cannot mint Work.
    pub(crate) fn next_stage_cancel_admission(
        &self,
        after: Option<TaskOperationId>,
    ) -> Option<(TaskOperationId, novarocks_execution_contract::CancelTask)> {
        if self.terminal_cleanup_started {
            None
        } else {
            self.stage_cancel_after(after)
        }
    }
    pub(crate) fn next_stage_cancel_withdrawal(
        &self,
        after: Option<TaskOperationId>,
    ) -> Option<(TaskOperationId, novarocks_execution_contract::CancelTask)> {
        if self.terminal_cleanup_started {
            self.stage_cancel_after(after)
        } else {
            None
        }
    }
    fn stage_cancel_after(
        &self,
        after: Option<TaskOperationId>,
    ) -> Option<(TaskOperationId, novarocks_execution_contract::CancelTask)> {
        let next = match after {
            Some(after) => self
                .stage_cancel_debts
                .range((std::ops::Bound::Excluded(after), std::ops::Bound::Unbounded))
                .next(),
            None => self.stage_cancel_debts.first_key_value(),
        };
        next.map(|(&operation, debt)| (operation, debt.request))
    }
    /// Settles logical stage debt only AFTER this exact original Cancel was
    /// admitted into its canonical dispatcher with the original Native grant.
    /// This proves neither transport handoff nor RPC/capture actual exit.
    pub(crate) fn stage_cancel_queued(
        &mut self,
        operation: TaskOperationId,
    ) -> Result<(), TaskExecutionError> {
        let debt = self
            .stage_cancel_debts
            .get(&operation)
            .ok_or(TaskExecutionError::UnknownOperation)?;
        let identity = debt.request.identity();
        if self.dispatcher.operation_state(operation)? != DispatchOperationState::Queued
            || self.dispatcher.queued_operation_kind(operation) != Some(OperationKind::CancelTask)
            || self.operation_targets.get(&operation)
                != Some(&OperationTarget::Task {
                    stage: identity.stage_id(),
                    task: identity.task_id(),
                })
        {
            return Err(TaskExecutionError::Schedule(
                "stage Cancel has not entered its original dispatcher position".into(),
            ));
        }
        self.settle_stage_cancel_debt(operation)
    }
    /// Cleanup consumes the original withdrawal result; a passive operation
    /// identity alone cannot erase an in-flight Native preparation or grant.
    pub(crate) fn stage_cancel_unused(
        &mut self,
        operation: TaskOperationId,
        outcome: crate::input::WithdrawalOutcome,
    ) -> Result<(), TaskExecutionError> {
        if !self.terminal_cleanup_started
            || outcome != crate::input::WithdrawalOutcome::DefinitelyUnsent
            || self.dispatcher.operation_state(operation)? != DispatchOperationState::Absent
        {
            return Err(TaskExecutionError::Schedule(
                "stage Cancel retirement lacks its original definitely-unsent closure".into(),
            ));
        }
        let debt = self
            .stage_cancel_debts
            .get(&operation)
            .ok_or(TaskExecutionError::UnknownOperation)?;
        let identity = debt.request.identity();
        self.rollback_unsent(
            OperationTarget::Task {
                stage: identity.stage_id(),
                task: identity.task_id(),
            },
            operation,
        );
        self.settle_stage_cancel_debt(operation)
    }
    fn settle_stage_cancel_debt(
        &mut self,
        operation: TaskOperationId,
    ) -> Result<(), TaskExecutionError> {
        let debt = self
            .stage_cancel_debts
            .remove(&operation)
            .ok_or(TaskExecutionError::UnknownOperation)?;
        let parent = self
            .stage_parent_debts
            .get_mut(&debt.parent)
            .ok_or(TaskExecutionError::UnknownOperation)?;
        parent.cancels = parent
            .cancels
            .checked_sub(1)
            .expect("same original parent Cancel debt");
        self.finish_stage_parent_if_ready(debt.parent);
        Ok(())
    }

    pub(crate) fn require_result_terminal_control(
        &mut self,
        root: TaskIdentity,
    ) -> Result<(), TaskExecutionError> {
        if root != self.graph.root_identity() {
            return Err(TaskExecutionError::UnknownOperation);
        }
        let deadline = self.now.saturating_add(Duration::from_secs(30));
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
                self.now.has_reached(deadline)
                    && self.failure.cause().is_none_or(|cause| cause.is_derived())
            })
    }

    /// Test observation of accepted Task/output/Release facts, never actual exit.
    #[cfg(test)]
    pub fn attempt_drained(&self) -> bool {
        self.stages.values().all(StageExecution::all_terminal)
            && self
                .stages
                .values()
                .all(StageExecution::all_output_released)
            && self.owners.values().all(ParticipationOwner::is_released)
    }

    /// The exact immutable Worker proof applied by the original covered owner.
    /// Abort/Release acknowledgements settle control operations; they do not
    /// prove that preparation, root payload or asynchronous work really exited.
    pub(crate) fn participation_convergence_receipt(
        &self,
        participation: ParticipationRef,
    ) -> Option<ParticipationConvergenceReceipt> {
        self.covered_observation
            .get(&participation)?
            .participation_convergence
    }

    #[cfg(test)]
    pub(crate) fn worker_participations_stopped(&self) -> bool {
        self.graph.participations().all(|&participation| {
            self.participation_convergence_receipt(participation)
                .is_some()
        })
    }

    /// A participation absent from the Worker for the entire attempt can close
    /// during cancellation without waiting for local planned Tasks to report
    /// terminal status: none of them could have been created there.
    pub(crate) fn participation_never_established(&self, participation: ParticipationRef) -> bool {
        self.owners
            .get(&participation)
            .is_some_and(ParticipationOwner::never_attempted_establish)
    }

    /// Where every task of one participation has been observed to.
    ///
    /// This is what a subscription starts from, so a transport that dropped
    /// resumes rather than replaying a task's whole version history.
    pub fn status_cursors(&self, participation: ParticipationRef) -> Vec<TaskStatusCursor> {
        self.stages
            .values()
            .flat_map(StageExecution::tasks)
            .filter(|(_, task)| task.participation() == participation)
            .map(|(_, task)| task.cursor())
            .collect()
    }

    /// The first termination cause of this attempt, if one was latched.
    pub fn failure_cause(&self) -> Option<&TerminationDetail> {
        self.failure.cause()
    }

    pub(crate) fn selected_task_failure(
        &self,
    ) -> Option<&novarocks_coordination_contract::SelectedTaskTermination> {
        self.failure.selected()
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

    /// Leaves one candidate exactly where it was.
    ///
    /// A minted candidate returns to its owner; a create or update position
    /// was never taken. Only a position that genuinely had something to send
    /// counts as a skip, so a stage release still learns whether every cancel
    /// it owes went out.

    /// Checks this attempt's own queue bounds for one request, then reserves
    /// it against process transport.

    /// Retain the same pure owner decisions until the core's bounded driver
    /// can request the original native input grant. No host callback runs here.
    fn enqueue_candidates(
        &mut self,
        candidates: VecDeque<AdmissionCandidate>,
        _now: MonotonicInstant,
    ) -> Result<AdmissionPass, TaskExecutionError> {
        for candidate in &candidates {
            if let AdmissionCandidate::Minted { intent, .. } = candidate {
                self.dispatcher.validate_operation_carrier(intent)?;
            }
        }
        let admitted = candidates.len();
        self.pending_candidates.extend(candidates);
        Ok(AdmissionPass {
            admitted,
            ..AdmissionPass::default()
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
                if let Some(mut task) = self
                    .stages
                    .get_mut(&stage)
                    .and_then(|stage| stage.task_mut_observed(task, &self.stage_demand))
                {
                    task.rollback_unsent(operation_id);
                }
            }
            OperationTarget::Participation(participation) => {
                if let Some(owner) = self.owners.get_mut(&participation) {
                    owner.rollback_unsent(operation_id);
                }
            }
            OperationTarget::ActorAbort => {}
        }
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

    fn task_by_identity(
        &mut self,
        identity: TaskIdentity,
    ) -> Option<crate::stage::TaskMutation<'_>> {
        let (stage_id, task_id) = self.locate(identity)?;
        self.stages
            .get_mut(&stage_id)?
            .task_mut_observed(task_id, &self.stage_demand)
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
        let mut state = CoveredParticipationObservation::default();
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

#[cfg(test)]
mod primary_terminal_tests {
    use super::*;
    use crate::input::{OperationReply, OperationReplyResult};
    use novarocks_execution_contract::{OperationOutcome, TaskOutputFacts, TaskStatusVersion};
    fn execution() -> QueryTaskExecution {
        crate::test_support::execution_with_strict_input()
    }
    #[test]
    fn failed_closed_create_updates_strict_terminal_without_inventing_task_failure() {
        let mut execution = execution();
        assert_eq!(execution.strict_pending.len(), 2);
        let identity = execution
            .graph
            .task(TaskId::new(3).unwrap())
            .unwrap()
            .identity();
        let intent = execution
            .task_by_identity(identity)
            .unwrap()
            .release_create(MonotonicInstant::ORIGIN)
            .unwrap();
        let reply = OperationReply {
            operation_id: intent.operation_id(),
            kind: OperationKind::CreateTask,
            result: OperationReplyResult::Worker(
                novarocks_execution_contract::operation::OperationResult::Verdict {
                    outcome: OperationOutcome::ResourceExhausted,
                    detail: None,
                    acknowledgement: None,
                },
            ),
        };
        assert!(matches!(
            execution.acknowledge_task(identity.stage_id(), identity.task_id(), &reply),
            Err(TaskExecutionError::OperationFailed { .. })
        ));
        assert!(execution.task(identity.task_id()).unwrap().is_terminal());
        assert!(execution.failure_cause().is_none());
        assert_eq!(execution.strict_pending.len(), 1);
        assert!(!execution.strict_pending.contains(&identity));
        // A separate original business-failure gate still blocks a new seal.
        assert!(execution.root_seal_facts(true).current_turn_failed);
    }
    #[test]
    fn accepted_terminal_returns_deployment_position_and_strict_count_in_the_same_input() {
        let mut execution = execution();
        let identity = execution
            .graph
            .task(TaskId::new(2).unwrap())
            .unwrap()
            .identity();
        execution
            .deployment_window
            .entry(identity.backend_process_id())
            .or_default()
            .insert(identity);
        let status = TaskStatus::try_new(
            identity,
            TaskStatusVersion::new(3).unwrap(),
            TaskState::Finished,
            None,
            TaskOutputFacts::new(true),
        )
        .unwrap();
        execution
            .apply_published(&status, &mut StatusReport::default())
            .unwrap();
        assert!(!execution.strict_pending.contains(&identity));
        assert!(
            !execution
                .deployment_window
                .contains_key(&identity.backend_process_id())
        );
        assert_eq!(execution.strict_pending.len(), 1);
    }
}

#[cfg(test)]
mod diagnostic_release_tests {
    use super::*;
    use crate::input::OperationReplyResult;
    use novarocks_execution_contract::{OperationOutcome, operation::OperationResult};

    #[test]
    fn rejected_admission_retains_original_error_and_updates_release_cut_in_same_input() {
        let mut execution = crate::test_support::execution_with_strict_input();
        let parts = execution
            .graph
            .participations()
            .copied()
            .collect::<Vec<_>>();
        for (index, &part) in parts.iter().enumerate() {
            let intent = execution
                .owners
                .get_mut(&part)
                .unwrap()
                .admission_intent(execution.now, &mut execution.ids)
                .unwrap()
                .unwrap();
            let reply = OperationAcknowledgement {
                operation_id: intent.operation_id(),
                kind: intent.kind(),
                result: OperationReplyResult::Worker(OperationResult::Verdict {
                    outcome: OperationOutcome::ResourceExhausted,
                    detail: None,
                    acknowledgement: None,
                }),
            };
            assert!(
                matches!(execution.acknowledge_participation(part,&reply),Err(TaskExecutionError::OperationFailed { participation,.. }) if participation==part)
            );
            assert!(execution.owner(part).unwrap().is_released());
            assert_eq!(
                execution.diagnostic_sources().all_released(),
                index + 1 == parts.len()
            );
        }
        assert!(
            execution
                .released_runtime_filter_contributions()
                .is_complete()
        );
    }
    #[test]
    fn actor_abort_positive_closure_updates_same_original_diagnostic_release_counter() {
        let mut execution = crate::test_support::execution_with_strict_input();
        let parts = execution
            .graph
            .participations()
            .copied()
            .collect::<Vec<_>>();
        for part in parts {
            execution
                .observe_actor_abort_participation_closed(part)
                .unwrap();
            execution
                .observe_actor_abort_participation_closed(part)
                .unwrap();
        }
        assert!(execution.diagnostic_sources().all_released());
        assert!(
            execution
                .released_runtime_filter_contributions()
                .is_complete()
        );
    }
}

#[cfg(test)]
mod bounded_drain_tests {
    use super::*;
    use crate::{
        capacity::{QueueStage, StepContext},
        config::DispatchLane,
        effect::FrozenContentRef,
        start::{FrozenCreation, FrozenExchangeEdge, FrozenTask, FrozenTaskGraph},
    };
    use novarocks_execution_contract::{
        AbortCause, AbortParticipation, ContentFingerprint, DomainVersion,
        ExchangeDeliveryRequirement, ExchangeEdgeId, PlanNodeId, TaskOutputFacts,
        TaskStatusVersion,
    };
    use novarocks_types::UniqueId;
    struct Turn(usize);
    impl StepContext for Turn {
        fn charge(&mut self, units: usize) -> bool {
            if units > self.0 {
                false
            } else {
                self.0 -= units;
                true
            }
        }
        fn reserve_queue_stage(
            &mut self,
            _: DispatchLane,
            _: bool,
            _: BackendProcessId,
            _: usize,
            _: usize,
        ) -> QueueStage {
            panic!("pure cleanup cannot mint transport capacity")
        }
    }
    fn grant(value: u64) -> QueuedInputGrantRef {
        QueuedInputGrantRef(NonZeroU64::new(value).unwrap())
    }
    fn many_tasks(count: u32) -> QueryTaskExecution {
        let seed = crate::test_support::remote(1);
        let mut tasks = BTreeMap::new();
        let mut stages = BTreeMap::new();
        let mut identities = Vec::new();
        for n in 1..=count {
            let id = TaskId::new(n).unwrap();
            let stage = StageId::new(n).unwrap();
            let identity = TaskIdentity::new(
                seed.identity().query_execution_id(),
                stage,
                id,
                seed.identity().backend_process_id(),
            );
            tasks.insert(
                id,
                FrozenTask {
                    identity,
                    participation: seed.participation(),
                    task_key: UniqueId::new(i64::from(n), 1),
                    fragment_id: n as i32,
                    split_plan_nodes: Box::new([]),
                    creation: FrozenCreation {
                        resource: PayloadRef(NonZeroU64::new(u64::from(n)).unwrap()),
                        queued_bytes: 256,
                        plan_carrier_bytes: 0,
                        descriptor_encoded_bytes: 0,
                    },
                },
            );
            stages.insert(stage, Box::from([identity]));
            identities.push(identity);
        }
        let edges = if count >= 2 {
            let edge = ExchangeEdgeId::new(1).unwrap();
            BTreeMap::from([(
                edge,
                FrozenExchangeEdge {
                    edge,
                    producer_stage: identities[1].stage_id(),
                    consumer_stage: identities[0].stage_id(),
                    destination_node: PlanNodeId::new(1).unwrap(),
                    delivery: ExchangeDeliveryRequirement::DemandDriven,
                    producers: Box::from([identities[1]]),
                    destinations: Box::from([identities[0]]),
                },
            )])
        } else {
            BTreeMap::new()
        };
        crate::test_support::execution_with_graph(
            TaskGraph::from_frozen(FrozenTaskGraph {
                root: identities[0],
                tasks,
                stages,
                edges,
            })
            .unwrap(),
        )
    }
    fn identity(execution: &QueryTaskExecution, value: u32) -> TaskIdentity {
        execution
            .task(TaskId::new(value).unwrap())
            .unwrap()
            .identity()
    }
    fn enqueue_create(
        execution: &mut QueryTaskExecution,
        identity: TaskIdentity,
        original_grant: QueuedInputGrantRef,
    ) -> TaskOperationId {
        let now = execution.now;
        let intent = execution
            .task_by_identity(identity)
            .unwrap()
            .release_create(now)
            .unwrap();
        let operation = intent.operation_id();
        execution
            .dispatcher
            .enqueue_reserved(intent, now, original_grant)
            .unwrap();
        execution.operation_targets.insert(
            operation,
            OperationTarget::Task {
                stage: identity.stage_id(),
                task: identity.task_id(),
            },
        );
        operation
    }
    fn finish(execution: &mut QueryTaskExecution, identity: TaskIdentity) {
        let status = TaskStatus::try_new(
            identity,
            TaskStatusVersion::new(3).unwrap(),
            TaskState::Finished,
            None,
            TaskOutputFacts::new(true),
        )
        .unwrap();
        execution
            .apply_published(&status, &mut StatusReport::default())
            .unwrap();
    }
    #[test]
    fn normal_drain_seals_input_immediately_and_visits_one_frozen_task_per_unit() {
        let mut execution = many_tasks(257);
        execution.begin_normal_drain();
        assert!(execution.normal_drain_started());
        for task in execution.graph.tasks() {
            assert!(
                !execution
                    .task_input_allowed(task.identity(), false)
                    .unwrap()
            );
            assert!(execution.task_input_allowed(task.identity(), true).unwrap());
            assert!(
                !execution
                    .task(task.task_id())
                    .unwrap()
                    .normally_stood_down()
            );
        }
        assert!(!execution.advance_normal_drain(&mut Turn(0)).unwrap());
        let mut steps = 0;
        while execution.has_pending_normal_drain() {
            let mut turn = Turn(1);
            assert!(execution.advance_normal_drain(&mut turn).unwrap());
            assert_eq!(turn.0, 0);
            steps += 1;
            execution.begin_normal_drain(); // An identical request cannot restart the cursor.
            assert!(steps <= 258);
        }
        assert_eq!(steps, 258);
        assert!(execution.graph.tasks().all(|task| {
            execution
                .task(task.task_id())
                .unwrap()
                .normally_stood_down()
        }));
        let mut idle = Turn(1);
        assert!(!execution.advance_normal_drain(&mut idle).unwrap());
        assert_eq!(idle.0, 1);
    }
    #[test]
    fn strict_live_gate_waits_for_actual_primary_terminals_without_empty_timer_continuation() {
        let mut execution = crate::test_support::execution_with_strict_input();
        execution.begin_normal_drain();
        assert!(!execution.normal_drain_started());
        assert!(!execution.has_pending_normal_drain());
        let strict = identity(&execution, 2);
        let create = enqueue_create(&mut execution, strict, grant(1));
        execution.stand_down_task_normally(strict).unwrap();
        assert!(
            !execution
                .task(strict.task_id())
                .unwrap()
                .normally_stood_down()
        );
        assert_eq!(
            execution.dispatcher.operation_state(create).unwrap(),
            DispatchOperationState::Queued
        );
        let other = identity(&execution, 3);
        finish(&mut execution, strict);
        assert!(!execution.advance_normal_drain(&mut Turn(1)).unwrap());
        finish(&mut execution, other);
        assert!(execution.advance_normal_drain(&mut Turn(1)).unwrap());
        assert!(execution.normal_drain_started());
        while execution.has_pending_normal_drain() {
            execution.advance_normal_drain(&mut Turn(1)).unwrap();
        }
        assert_eq!(
            execution.dispatcher.operation_state(create).unwrap(),
            DispatchOperationState::Absent
        );
        assert_eq!(execution.dispatcher.take_released_grant(), Some(grant(1)));
    }
    #[test]
    fn exact_task_withdrawal_cancels_own_create_and_ordinary_update_but_preserves_close_and_sibling()
     {
        for destination_close in [false, true] {
            let mut execution = many_tasks(3);
            let task = identity(&execution, 2);
            let root = identity(&execution, 1);
            let sibling = identity(&execution, 3);
            let own_create = enqueue_create(&mut execution, task, grant(1));
            let sibling_create = enqueue_create(&mut execution, sibling, grant(2));
            let running = TaskStatus::try_new(
                task,
                TaskStatusVersion::new(2).unwrap(),
                TaskState::Running,
                None,
                TaskOutputFacts::default(),
            )
            .unwrap();
            execution
                .apply_published(&running, &mut StatusReport::default())
                .unwrap();
            let update = if destination_close {
                TaskDomainUpdate::CloseExchangeDestination {
                    version: DomainVersion::FIRST,
                    edge: ExchangeEdgeId::new(1).unwrap(),
                    destination: root,
                }
            } else {
                TaskDomainUpdate::TaskDynamicFilter {
                    version: DomainVersion::new(1).unwrap(),
                    payload: FrozenContentRef {
                        resource: PayloadRef(NonZeroU64::new(90).unwrap()),
                        fingerprint: ContentFingerprint::from_bytes([9; 16]),
                        queued_bytes: 16,
                    },
                }
            };
            let operation = execution.ids.issue();
            execution
                .task_by_identity(task)
                .unwrap()
                .enqueue_update(operation, update, grant(3))
                .unwrap();
            let (intent, original_grant) = execution
                .task_by_identity(task)
                .unwrap()
                .next_update_intent()
                .unwrap()
                .unwrap();
            assert_eq!(intent.operation_id(), operation);
            execution
                .dispatcher
                .enqueue_reserved(intent, execution.now, original_grant.unwrap())
                .unwrap();
            execution.operation_targets.insert(
                operation,
                OperationTarget::Task {
                    stage: task.stage_id(),
                    task: task.task_id(),
                },
            );
            execution.stand_down_task_normally(task).unwrap();
            assert!(
                execution
                    .task(task.task_id())
                    .unwrap()
                    .normally_stood_down()
            );
            assert_eq!(
                execution.dispatcher.operation_state(own_create).unwrap(),
                DispatchOperationState::Absent
            );
            assert_eq!(
                execution
                    .dispatcher
                    .operation_state(sibling_create)
                    .unwrap(),
                DispatchOperationState::Queued
            );
            assert_eq!(
                execution.dispatcher.operation_state(operation).unwrap(),
                if destination_close {
                    DispatchOperationState::Queued
                } else {
                    DispatchOperationState::Absent
                }
            );
            let mut returned = Vec::new();
            while let Some(grant) = execution.dispatcher.take_released_grant() {
                returned.push(grant);
            }
            returned.sort_by_key(|grant| grant.0);
            assert_eq!(
                returned,
                if destination_close {
                    vec![grant(1)]
                } else {
                    vec![grant(1), grant(3)]
                }
            );
        }
    }
    #[test]
    fn accepted_failure_does_not_traverse_other_tasks_before_bounded_cleanup() {
        let mut execution = many_tasks(64);
        let tasks = execution
            .graph
            .tasks()
            .map(|task| task.identity())
            .collect::<Vec<_>>();
        let operations = tasks
            .iter()
            .enumerate()
            .map(|(index, &task)| enqueue_create(&mut execution, task, grant(index as u64 + 1)))
            .collect::<Vec<_>>();
        for task in &tasks {
            execution
                .deployment_window
                .entry(task.backend_process_id())
                .or_default()
                .insert(*task);
        }
        execution
            .status_reconciliations
            .insert(execution.task(tasks[0].task_id()).unwrap().participation());
        let failed = TaskStatus::try_new(
            tasks[0],
            TaskStatusVersion::new(2).unwrap(),
            TaskState::Aborted,
            Some(TerminationDetail::Aborted(AbortCause::QueryFailed)),
            TaskOutputFacts::default(),
        )
        .unwrap();
        execution
            .apply_published(&failed, &mut StatusReport::default())
            .unwrap();
        assert!(execution.terminal_cleanup_started);
        assert!(execution.has_pending_terminal_cleanup());
        assert!(
            execution
                .task(tasks[63].task_id())
                .unwrap()
                .create_pending()
        );
        assert_eq!(execution.dispatcher.queued_items(), 64);
        for task in &tasks {
            assert!(!execution.task_input_allowed(*task, false).unwrap());
            assert!(!execution.task_input_allowed(*task, true).unwrap());
        }
        assert!(!execution.advance_terminal_cleanup(&mut Turn(0)).unwrap());
        let mut steps = 0;
        while execution.has_pending_terminal_cleanup() {
            let mut turn = Turn(1);
            assert!(execution.advance_terminal_cleanup(&mut turn).unwrap());
            assert_eq!(turn.0, 0);
            steps += 1;
            execution.begin_terminal_cleanup(); // Same failure cannot reset progress.
            assert!(steps <= 140);
        }
        assert!(steps >= 64 + 64);
        for operation in operations {
            assert_eq!(
                execution.dispatcher.operation_state(operation).unwrap(),
                DispatchOperationState::Absent
            );
        }
        assert!(execution.deployment_window.is_empty());
        assert!(execution.status_reconciliations.is_empty());
        assert!(
            !execution
                .task(tasks[63].task_id())
                .unwrap()
                .create_pending()
        );
        let mut grants = Vec::new();
        while let Some(grant) = execution.dispatcher.take_released_grant() {
            grants.push(grant);
        }
        grants.sort_by_key(|grant| grant.0);
        assert_eq!(grants, (1..=64).map(grant).collect::<Vec<_>>());
        let mut idle = Turn(1);
        assert!(!execution.advance_terminal_cleanup(&mut idle).unwrap());
        assert_eq!(idle.0, 1);
    }
    #[test]
    fn terminal_cleanup_keeps_original_in_flight_and_actor_abort_but_reclaims_only_unsent_grant() {
        let mut execution = many_tasks(3);
        let first = identity(&execution, 1);
        let second = identity(&execution, 2);
        let inflight = enqueue_create(&mut execution, first, grant(1));
        let batch = execution.dispatcher.take_batch().unwrap();
        execution.dispatcher.accept(batch.acceptance()).unwrap();
        let unsent = enqueue_create(&mut execution, second, grant(2));
        let participation = execution.task(first.task_id()).unwrap().participation();
        let abort = execution.ids.issue();
        let intent = OperationIntent::AbortParticipation(AbortParticipation::new(
            abort,
            participation,
            AbortCause::QueryFailed,
        ));
        execution
            .dispatcher
            .enqueue_reserved(intent, execution.now, grant(3))
            .unwrap();
        execution
            .operation_targets
            .insert(abort, OperationTarget::ActorAbort);
        execution.begin_terminal_cleanup();
        while execution.has_pending_terminal_cleanup() {
            execution.advance_terminal_cleanup(&mut Turn(1)).unwrap();
        }
        assert_eq!(
            execution.dispatcher.operation_state(inflight).unwrap(),
            DispatchOperationState::InFlight
        );
        assert_eq!(
            execution.dispatcher.operation_state(unsent).unwrap(),
            DispatchOperationState::Absent
        );
        assert_eq!(
            execution.dispatcher.operation_state(abort).unwrap(),
            DispatchOperationState::Queued
        );
        assert_eq!(execution.dispatcher.take_released_grant(), Some(grant(2)));
        assert_eq!(execution.dispatcher.take_released_grant(), None);
        assert_eq!(batch.queued_inputs.as_ref(), &[grant(1)]);
    }
    #[test]
    fn original_late_native_grant_observes_failure_cut_before_cleanup_cursor_reaches_its_task() {
        let mut execution = many_tasks(64);
        let task = identity(&execution, 64);
        let update = TaskDomainUpdate::TaskDynamicFilter {
            version: DomainVersion::FIRST,
            payload: FrozenContentRef {
                resource: PayloadRef(NonZeroU64::new(100).unwrap()),
                fingerprint: ContentFingerprint::from_bytes([1; 16]),
                queued_bytes: 16,
            },
        };
        let (admission, intent) = execution.prepare_task_update(task, update.clone()).unwrap();
        assert_eq!(admission, UpdateAdmission::Preparing);
        let operation = intent.unwrap().operation_id();
        execution.begin_terminal_cleanup();
        assert!(execution.task(task.task_id()).unwrap().create_pending());
        assert_eq!(
            execution
                .complete_task_input_preparation(task, operation, grant(5))
                .unwrap(),
            UpdateAdmission::DiscardedTerminal
        );
        assert_eq!(
            execution
                .task_by_identity(task)
                .unwrap()
                .take_released_grant(),
            Some(grant(5))
        );
        assert_eq!(
            execution
                .task_by_identity(task)
                .unwrap()
                .take_released_grant(),
            None
        );
        assert!(
            matches!(execution.prepare_task_update(task,update),Err(TaskExecutionError::Schedule(message)) if message=="attempt drain rejects new task updates")
        );
        assert!(execution.has_pending_terminal_cleanup());
    }
    #[test]
    fn first_normal_cut_is_preserved_when_failure_and_late_grant_overtake_its_task_cursor() {
        for same_task in [false, true] {
            let mut execution = many_tasks(64);
            let task = identity(&execution, 64);
            let update = TaskDomainUpdate::TaskDynamicFilter {
                version: DomainVersion::FIRST,
                payload: FrozenContentRef {
                    resource: PayloadRef(NonZeroU64::new(100).unwrap()),
                    fingerprint: ContentFingerprint::from_bytes([1; 16]),
                    queued_bytes: 16,
                },
            };
            let (_, intent) = execution.prepare_task_update(task, update).unwrap();
            let operation = intent.unwrap().operation_id();
            execution.begin_normal_drain();
            let failed_task = if same_task {
                task
            } else {
                identity(&execution, 1)
            };
            let failed = TaskStatus::try_new(
                failed_task,
                TaskStatusVersion::new(2).unwrap(),
                TaskState::Aborted,
                Some(TerminationDetail::Aborted(AbortCause::QueryFailed)),
                TaskOutputFacts::default(),
            )
            .unwrap();
            execution
                .apply_published(&failed, &mut StatusReport::default())
                .unwrap();
            assert!(execution.terminal_cleanup_started);
            assert_eq!(
                execution
                    .complete_task_input_preparation(task, operation, grant(8))
                    .unwrap(),
                UpdateAdmission::DiscardedNoLongerNeeded
            );
            assert_eq!(
                execution
                    .task_by_identity(task)
                    .unwrap()
                    .take_released_grant(),
                Some(grant(8))
            );
            assert!(execution.failure_cause().is_some());
        }
    }
    #[test]
    fn late_create_refusal_observes_original_normal_cut_before_physical_task_cleanup() {
        use crate::input::{LocalDispatchResult, OperationReply, OperationReplyResult};
        let mut execution = many_tasks(64);
        let task = identity(&execution, 64);
        let intent = execution
            .task_by_identity(task)
            .unwrap()
            .release_create(MonotonicInstant::ORIGIN)
            .unwrap();
        execution.begin_normal_drain();
        let reply = OperationReply {
            operation_id: intent.operation_id(),
            kind: OperationKind::CreateTask,
            result: OperationReplyResult::Local {
                result: LocalDispatchResult::NonWorkerRejected,
                failure: None,
                protocol_failure: None,
                detail: None,
            },
        };
        execution
            .acknowledge_task(task.stage_id(), task.task_id(), &reply)
            .unwrap();
        assert!(!execution.task(task.task_id()).unwrap().create_pending());
        assert!(
            execution
                .task(task.task_id())
                .unwrap()
                .normally_stood_down()
        );
    }
    #[test]
    fn terminal_cleanup_discards_only_pre_cut_reconciliation_not_later_inflight_ack_debt() {
        use crate::input::{OperationReply, OperationReplyResult};
        use novarocks_execution_contract::{OperationOutcome, operation::OperationResult};
        let mut execution = many_tasks(3);
        let task = identity(&execution, 2);
        let running = TaskStatus::try_new(
            task,
            TaskStatusVersion::new(2).unwrap(),
            TaskState::Running,
            None,
            TaskOutputFacts::default(),
        )
        .unwrap();
        execution
            .apply_published(&running, &mut StatusReport::default())
            .unwrap();
        let update = TaskDomainUpdate::TaskDynamicFilter {
            version: DomainVersion::FIRST,
            payload: FrozenContentRef {
                resource: PayloadRef(NonZeroU64::new(100).unwrap()),
                fingerprint: ContentFingerprint::from_bytes([1; 16]),
                queued_bytes: 16,
            },
        };
        let operation = execution.ids.issue();
        execution
            .task_by_identity(task)
            .unwrap()
            .enqueue_update(operation, update, grant(4))
            .unwrap();
        execution
            .task_by_identity(task)
            .unwrap()
            .next_update_intent()
            .unwrap()
            .unwrap();
        let part = execution.task(task.task_id()).unwrap().participation();
        execution.status_reconciliations.insert(part);
        execution.begin_terminal_cleanup();
        let reply = OperationReply {
            operation_id: operation,
            kind: OperationKind::UpdateTask,
            result: OperationReplyResult::Worker(OperationResult::Verdict {
                outcome: OperationOutcome::TerminalRejected,
                detail: None,
                acknowledgement: None,
            }),
        };
        execution
            .acknowledge_task(task.stage_id(), task.task_id(), &reply)
            .unwrap();
        while execution.has_pending_terminal_cleanup() {
            execution.advance_terminal_cleanup(&mut Turn(1)).unwrap();
        }
        assert_eq!(
            execution.take_status_reconciliations(),
            BTreeSet::from([part])
        );
    }
}

#[cfg(test)]
mod stage_demand_cut_tests {
    use super::*;
    use crate::{
        capacity::{QueueStage, StepContext},
        config::DispatchLane,
        effect::FrozenContentRef,
    };
    use novarocks_execution_contract::{
        AbortCause, ContentFingerprint, DomainVersion, TaskOutputFacts, TaskStatusVersion,
    };
    struct Turn(usize);
    impl StepContext for Turn {
        fn charge(&mut self, n: usize) -> bool {
            if n > self.0 {
                false
            } else {
                self.0 -= n;
                true
            }
        }
        fn reserve_queue_stage(
            &mut self,
            _: DispatchLane,
            _: bool,
            _: BackendProcessId,
            _: usize,
            _: usize,
        ) -> QueueStage {
            panic!("stage proof grants no transport capacity")
        }
    }
    fn stage(n: u32) -> StageId {
        StageId::new(n).unwrap()
    }
    fn identity(execution: &QueryTaskExecution, n: u32) -> TaskIdentity {
        execution.task(TaskId::new(n).unwrap()).unwrap().identity()
    }
    fn apply(
        execution: &mut QueryTaskExecution,
        task: TaskIdentity,
        version: u64,
        state: TaskState,
    ) {
        let termination = match state {
            TaskState::Aborting | TaskState::Aborted => {
                Some(TerminationDetail::Aborted(AbortCause::QueryFailed))
            }
            TaskState::Canceling | TaskState::Canceled => Some(TerminationDetail::Canceled(
                CancelReason::UpstreamNoLongerNeeded,
            )),
            _ => None,
        };
        let status = TaskStatus::try_new(
            task,
            TaskStatusVersion::new(version).unwrap(),
            state,
            termination,
            TaskOutputFacts::new(state == TaskState::Finished),
        )
        .unwrap();
        execution
            .apply_published(&status, &mut StatusReport::default())
            .unwrap();
    }
    fn proof(execution: &QueryTaskExecution, producer: u32) {
        execution.stage_demand.begin(stage(producer)).unwrap();
        while let Some(consumer) = execution.stage_demand.next_consumer() {
            let released = execution.stages[&consumer].released_children();
            assert!(
                execution
                    .stage_demand
                    .check_next(consumer, released, &mut Turn(1))
                    .unwrap()
            );
        }
    }
    fn update() -> TaskDomainUpdate {
        TaskDomainUpdate::TaskDynamicFilter {
            version: DomainVersion::FIRST,
            payload: FrozenContentRef {
                resource: PayloadRef(NonZeroU64::new(99).unwrap()),
                fingerprint: ContentFingerprint::from_bytes([1; 16]),
                queued_bytes: 16,
            },
        }
    }
    #[test]
    fn original_stage_counts_keep_already_checked_consumer_truth_current() {
        let mut execution =
            crate::test_support::execution_with_graph(crate::stage::demand::tests::graph());
        let first = identity(&execution, 1);
        let other = identity(&execution, 3);
        execution.stage_demand.begin(stage(2)).unwrap();
        execution
            .stage_demand
            .check_next(stage(1), false, &mut Turn(1))
            .unwrap();
        apply(&mut execution, first, 2, TaskState::Flushing);
        apply(&mut execution, other, 2, TaskState::Flushing);
        execution
            .stage_demand
            .check_next(stage(3), true, &mut Turn(1))
            .unwrap();
        assert_eq!(execution.stage_demand.all_consumers_released(), Some(true));
        apply(&mut execution, first, 3, TaskState::Canceling);
        assert_eq!(execution.stage_demand.all_consumers_released(), Some(false));
        assert!(
            execution
                .accept_stage_demand_cut(stage(1), stage(2))
                .is_err()
        );
    }
    #[test]
    fn accepted_stage_cut_survives_later_failure_before_any_physical_task_fanout() {
        let mut execution =
            crate::test_support::execution_with_graph(crate::stage::demand::tests::graph());
        let task = identity(&execution, 2);
        let first = identity(&execution, 1);
        let other = identity(&execution, 3);
        let (_, intent) = execution.prepare_task_update(task, update()).unwrap();
        let operation = intent.unwrap().operation_id();
        apply(&mut execution, first, 2, TaskState::Flushing);
        apply(&mut execution, other, 2, TaskState::Flushing);
        proof(&execution, 2);
        execution
            .accept_stage_demand_cut(stage(1), stage(2))
            .unwrap();
        assert!(
            !execution
                .task(task.task_id())
                .unwrap()
                .normally_stood_down()
        );
        assert!(!execution.task_input_allowed(task, false).unwrap());
        assert_eq!(execution.stage_demand.producer(), None);
        apply(&mut execution, other, 3, TaskState::Aborting);
        assert!(execution.terminal_cleanup_started);
        assert_eq!(
            execution
                .complete_task_input_preparation(
                    task,
                    operation,
                    QueuedInputGrantRef(NonZeroU64::new(8).unwrap())
                )
                .unwrap(),
            UpdateAdmission::DiscardedNoLongerNeeded
        );
        assert!(execution.withdrawal_materialized.contains(&task.task_id()));
    }
    #[test]
    fn strict_task_uses_original_local_terminal_frontier_and_duplicate_settlement_does_not_rewrite_it()
     {
        let mut execution = crate::test_support::execution_with_graph(
            crate::stage::demand::tests::graph_variant(true, false),
        );
        let task = identity(&execution, 2);
        let first = identity(&execution, 1);
        let other = identity(&execution, 3);
        apply(&mut execution, first, 2, TaskState::Flushing);
        apply(&mut execution, other, 2, TaskState::Flushing);
        proof(&execution, 2);
        execution
            .accept_stage_demand_cut(stage(1), stage(2))
            .unwrap();
        assert!(!execution.stage_demand_withdraws(task));
        assert!(execution.task_input_allowed(task, false).unwrap());
        apply(&mut execution, task, 2, TaskState::Finished);
        let ordinal = execution.drained_tasks[&task.task_id()];
        assert_eq!(ordinal, 1);
        assert!(!execution.stage_demand_withdraws(task)); // Later terminal cannot enter the old cut.
        execution.settle_task_drain(task.stage_id(), task.task_id());
        assert_eq!(execution.drained_tasks[&task.task_id()], ordinal);
        proof(&execution, 2);
        execution
            .accept_stage_demand_cut(stage(3), stage(2))
            .unwrap();
        assert!(execution.stage_demand_withdraws(task)); // A distinct eligible parent pass has its own later cut.
    }
    #[test]
    fn producer_stage_cut_excludes_root_before_task_withdrawal_and_new_external_updates_observe_nonroot_cut()
     {
        let mut execution = crate::test_support::execution_with_graph(
            crate::stage::demand::tests::graph_variant(false, true),
        );
        let root = execution.graph.root_identity();
        let first = identity(&execution, 1);
        let other = identity(&execution, 3);
        apply(&mut execution, first, 2, TaskState::Flushing);
        apply(&mut execution, other, 2, TaskState::Flushing);
        proof(&execution, 2);
        execution
            .accept_stage_demand_cut(stage(1), stage(2))
            .unwrap();
        execution.apply_prior_withdrawal(root).unwrap();
        assert!(!execution.stage_demand_withdraws(root));
        assert!(execution.task_input_allowed(root, false).unwrap());
        assert!(!execution.withdrawal_materialized.contains(&root.task_id()));
        assert!(
            !execution
                .task(root.task_id())
                .unwrap()
                .normally_stood_down()
        );
        let mut normal =
            crate::test_support::execution_with_graph(crate::stage::demand::tests::graph());
        let task = identity(&normal, 2);
        let first = identity(&normal, 1);
        let other = identity(&normal, 3);
        apply(&mut normal, first, 2, TaskState::Flushing);
        apply(&mut normal, other, 2, TaskState::Flushing);
        proof(&normal, 2);
        normal.accept_stage_demand_cut(stage(1), stage(2)).unwrap();
        let (disposition, intent) = normal.prepare_task_update(task, update()).unwrap();
        assert_eq!(disposition, UpdateAdmission::DiscardedNoLongerNeeded);
        assert!(intent.is_none());
    }
    #[test]
    fn remote_normal_flag_is_not_a_receipt_for_query_side_exact_withdrawal() {
        let mut execution =
            crate::test_support::execution_with_graph(crate::stage::demand::tests::graph());
        let task = identity(&execution, 2);
        let first = identity(&execution, 1);
        let other = identity(&execution, 3);
        let intent = execution
            .task_by_identity(task)
            .unwrap()
            .release_create(MonotonicInstant::ORIGIN)
            .unwrap();
        let operation = intent.operation_id();
        execution
            .dispatcher
            .enqueue_reserved(
                intent,
                execution.now,
                QueuedInputGrantRef(NonZeroU64::new(9).unwrap()),
            )
            .unwrap();
        execution.operation_targets.insert(
            operation,
            OperationTarget::Task {
                stage: task.stage_id(),
                task: task.task_id(),
            },
        );
        execution
            .task_by_identity(task)
            .unwrap()
            .begin_normal_stand_down(); // Original role-local flag alone.
        assert!(!execution.withdrawal_materialized.contains(&task.task_id()));
        apply(&mut execution, first, 2, TaskState::Flushing);
        apply(&mut execution, other, 2, TaskState::Flushing);
        proof(&execution, 2);
        execution
            .accept_stage_demand_cut(stage(1), stage(2))
            .unwrap();
        execution.apply_prior_withdrawal(task).unwrap();
        assert_eq!(
            execution.dispatcher.operation_state(operation).unwrap(),
            DispatchOperationState::Absent
        );
        assert!(execution.withdrawal_materialized.contains(&task.task_id()));
    }
    #[test]
    fn multiple_task_guards_report_current_stage_predicate_at_their_actual_drop_order() {
        let mut execution = crate::test_support::execution_with_graph(
            crate::stage::demand::tests::graph_two_task_consumer(),
        );
        let first = identity(&execution, 1);
        let sibling = identity(&execution, 5);
        let other = identity(&execution, 3);
        apply(&mut execution, first, 2, TaskState::Flushing);
        apply(&mut execution, sibling, 2, TaskState::Running);
        apply(&mut execution, other, 2, TaskState::Flushing);
        proof(&execution, 2);
        assert_eq!(execution.stage_demand.all_consumers_released(), Some(false));
        let observations = &execution.stage_demand;
        let mut guards = execution
            .stages
            .get_mut(&stage(1))
            .unwrap()
            .tasks_mut_observed(observations);
        let (_, mut first_guard) = guards.next().unwrap();
        let (_, mut sibling_guard) = guards.next().unwrap();
        let failed = TaskStatus::try_new(
            first,
            TaskStatusVersion::new(3).unwrap(),
            TaskState::Aborting,
            Some(TerminationDetail::Aborted(AbortCause::QueryFailed)),
            TaskOutputFacts::default(),
        )
        .unwrap();
        first_guard.observe_status(&failed).unwrap();
        let flushing = TaskStatus::try_new(
            sibling,
            TaskStatusVersion::new(3).unwrap(),
            TaskState::Flushing,
            None,
            TaskOutputFacts::default(),
        )
        .unwrap();
        sibling_guard.observe_status(&flushing).unwrap();
        drop(sibling_guard); // Counts still contain the first guard's old Flushing.
        assert_eq!(observations.all_consumers_released(), Some(true));
        drop(first_guard); // Current counts true -> false, although this guard was created while false.
        assert_eq!(observations.all_consumers_released(), Some(false));
    }
    fn drive(execution: &mut QueryTaskExecution) {
        let mut steps = 0;
        while execution.has_pending_stage_propagation() {
            let mut turn = Turn(1);
            assert!(execution.propagate_stage_release(&mut turn).unwrap());
            assert_eq!(turn.0, 0);
            steps += 1;
            assert!(steps < 200, "real graph-bounded continuation");
        }
    }
    fn queue_cancel(
        execution: &mut QueryTaskExecution,
        operation: TaskOperationId,
        request: novarocks_execution_contract::CancelTask,
        grant: u64,
    ) {
        execution
            .dispatcher
            .enqueue_reserved(
                OperationIntent::CancelTask(request),
                execution.now,
                QueuedInputGrantRef(NonZeroU64::new(grant).unwrap()),
            )
            .unwrap();
        let identity = request.identity();
        execution.operation_targets.insert(
            operation,
            OperationTarget::Task {
                stage: identity.stage_id(),
                task: identity.task_id(),
            },
        );
        execution.stage_cancel_queued(operation).unwrap();
    }
    #[test]
    fn false_producer_proof_yields_to_sibling_and_parent_waits_for_original_cancel_admission() {
        let mut execution =
            crate::test_support::execution_with_graph(crate::stage::demand::tests::graph());
        let first = identity(&execution, 1);
        let blocked = identity(&execution, 2);
        let sibling = identity(&execution, 4);
        apply(&mut execution, blocked, 2, TaskState::Running);
        apply(&mut execution, sibling, 2, TaskState::Running);
        apply(&mut execution, first, 2, TaskState::Flushing);
        assert!(!execution.propagate_stage_release(&mut Turn(0)).unwrap());
        drive(&mut execution);
        assert!(!execution.stage_demand_cuts.contains_key(&stage(2)));
        assert!(
            !execution
                .task(blocked.task_id())
                .unwrap()
                .normally_stood_down()
        );
        assert!(
            execution
                .task(sibling.task_id())
                .unwrap()
                .normally_stood_down()
        );
        assert!(!execution.released_children_of.contains(&stage(1)));
        let (operation, request) = execution.stage_cancel_after(None).unwrap();
        assert_eq!(request.identity(), sibling);
        for _ in 0..100 {
            assert_eq!(execution.stage_cancel_after(None).unwrap().0, operation);
        }
        assert!(execution.stage_cancel_queued(operation).is_err());
        let mut idle = Turn(1);
        assert!(!execution.propagate_stage_release(&mut idle).unwrap());
        assert_eq!(idle.0, 1);
        queue_cancel(&mut execution, operation, request, 8);
        assert!(!execution.released_children_of.contains(&stage(1))); // Producer2 still needs a later real release.
        assert!(!execution.stage_parent_debts.contains_key(&stage(1)));
        assert!(execution.stage_cancel_after(None).is_none());
        assert_eq!(
            execution.task(sibling.task_id()).unwrap().task_state(),
            TaskState::Running
        );
    }
    #[test]
    fn blocked_original_cancel_keeps_same_identity_while_other_producer_and_parent_progress() {
        let mut execution =
            crate::test_support::execution_with_graph(crate::stage::demand::tests::graph());
        let first = identity(&execution, 1);
        let other = identity(&execution, 3);
        let producer = identity(&execution, 2);
        let sibling = identity(&execution, 4);
        apply(&mut execution, producer, 2, TaskState::Running);
        apply(&mut execution, sibling, 2, TaskState::Running);
        apply(&mut execution, first, 2, TaskState::Flushing);
        apply(&mut execution, other, 2, TaskState::Flushing);
        drive(&mut execution);
        let (blocked, original) = execution.stage_cancel_after(None).unwrap();
        assert_eq!(original.identity(), producer);
        let (healthy, request) = execution.stage_cancel_after(Some(blocked)).unwrap();
        assert_eq!(request.identity(), sibling);
        assert!(execution.stage_cancel_after(Some(healthy)).is_none());
        assert!(!execution.released_children_of.contains(&stage(1)));
        queue_cancel(&mut execution, healthy, request, 9);
        assert_eq!(execution.stage_cancel_after(None).unwrap().0, blocked);
        assert!(execution.released_children_of.contains(&stage(3))); // Second parent owns no duplicate Cancel.
        drive(&mut execution);
        assert_eq!(
            execution
                .stage_cancel_after(None)
                .unwrap()
                .1
                .envelope()
                .operation_id(),
            original.envelope().operation_id()
        );
        queue_cancel(&mut execution, blocked, original, 10);
        assert!(execution.released_children_of.contains(&stage(1)));
    }
    #[test]
    fn upstream_stage_waits_for_actual_derived_flushing_instead_of_cancel_or_cancel_ack() {
        use crate::input::{OperationReply, OperationReplyResult};
        use novarocks_execution_contract::{OperationOutcome, operation::OperationResult};
        let mut execution =
            crate::test_support::execution_with_graph(crate::stage::demand::tests::graph_chain());
        let root = identity(&execution, 1);
        let middle = identity(&execution, 2);
        let upstream = identity(&execution, 3);
        apply(&mut execution, middle, 2, TaskState::Running);
        apply(&mut execution, upstream, 2, TaskState::Running);
        apply(&mut execution, root, 2, TaskState::Flushing);
        drive(&mut execution);
        assert!(
            execution
                .task(middle.task_id())
                .unwrap()
                .normally_stood_down()
        );
        assert!(
            !execution
                .task(upstream.task_id())
                .unwrap()
                .normally_stood_down()
        );
        let (operation, request) = execution.stage_cancel_after(None).unwrap();
        queue_cancel(&mut execution, operation, request, 11);
        let batch = execution.dispatcher.take_batch().unwrap();
        execution.dispatcher.accept(batch.acceptance()).unwrap();
        let reply = OperationReply {
            operation_id: operation,
            kind: OperationKind::CancelTask,
            result: OperationReplyResult::Worker(OperationResult::Verdict {
                outcome: OperationOutcome::Accepted,
                detail: None,
                acknowledgement: None,
            }),
        };
        execution.acknowledge(&reply).unwrap();
        drive(&mut execution);
        assert_eq!(
            execution.task(middle.task_id()).unwrap().task_state(),
            TaskState::Running
        );
        assert!(
            !execution
                .task(upstream.task_id())
                .unwrap()
                .normally_stood_down()
        );
        apply(&mut execution, middle, 3, TaskState::Flushing);
        drive(&mut execution);
        assert!(
            execution
                .task(upstream.task_id())
                .unwrap()
                .normally_stood_down()
        );
    }
    #[test]
    fn physical_stage_fanout_excludes_root_and_late_cleanup_requires_original_definitely_unsent_fact()
     {
        let mut root_execution = crate::test_support::execution_with_graph(
            crate::stage::demand::tests::graph_variant(false, true),
        );
        let first = identity(&root_execution, 1);
        let other = identity(&root_execution, 3);
        let root = root_execution.graph.root_identity();
        apply(&mut root_execution, first, 2, TaskState::Flushing);
        apply(&mut root_execution, other, 2, TaskState::Flushing);
        drive(&mut root_execution);
        assert!(
            !root_execution
                .task(root.task_id())
                .unwrap()
                .normally_stood_down()
        );
        assert!(
            !root_execution
                .withdrawal_materialized
                .contains(&root.task_id())
        );
        assert!(root_execution.stage_cancel_after(None).is_none());
        let mut execution =
            crate::test_support::execution_with_graph(crate::stage::demand::tests::graph());
        let first = identity(&execution, 1);
        let sibling = identity(&execution, 4);
        apply(&mut execution, sibling, 2, TaskState::Running);
        apply(&mut execution, first, 2, TaskState::Flushing);
        drive(&mut execution);
        let (operation, request) = execution.stage_cancel_after(None).unwrap();
        assert!(
            execution
                .stage_cancel_unused(operation, crate::input::WithdrawalOutcome::DefinitelyUnsent)
                .is_err()
        );
        execution.begin_terminal_cleanup();
        assert!(!execution.has_pending_stage_propagation());
        assert!(
            execution
                .stage_cancel_unused(operation, crate::input::WithdrawalOutcome::TooLate)
                .is_err()
        );
        assert_eq!(
            execution
                .stage_cancel_after(None)
                .unwrap()
                .1
                .envelope()
                .operation_id(),
            request.envelope().operation_id()
        );
        execution
            .stage_cancel_unused(operation, crate::input::WithdrawalOutcome::DefinitelyUnsent)
            .unwrap();
        assert!(execution.stage_cancel_after(None).is_none());
        assert!(
            execution
                .stage_cancel_unused(operation, crate::input::WithdrawalOutcome::DefinitelyUnsent)
                .is_err()
        );
    }
    #[test]
    fn cleanup_opens_only_original_withdrawal_debt_and_state_remains_send_between_poller_threads() {
        fn assert_send<T: Send>() {}
        assert_send::<QueryTaskExecution>();
        let mut execution =
            crate::test_support::execution_with_graph(crate::stage::demand::tests::graph());
        let parent = identity(&execution, 1);
        let sibling = identity(&execution, 4);
        apply(&mut execution, sibling, 2, TaskState::Running);
        apply(&mut execution, parent, 2, TaskState::Flushing);
        drive(&mut execution);
        let (operation, _) = execution.next_stage_cancel_admission(None).unwrap();
        assert!(execution.next_stage_cancel_withdrawal(None).is_none());
        execution.begin_terminal_cleanup();
        assert!(execution.next_stage_cancel_admission(None).is_none());
        assert_eq!(
            execution.next_stage_cancel_withdrawal(None).unwrap().0,
            operation
        );
        assert!(
            execution
                .accept_stage_demand_cut(stage(1), stage(4))
                .is_err()
        );
    }
    #[test]
    fn observed_task_mutation_rejects_foreign_original_stage_binding_before_mutating_any_row() {
        let graph = crate::stage::demand::tests::graph();
        let mut execution = crate::test_support::execution_with_graph(graph);
        let foreign =
            crate::test_support::execution_with_graph(crate::stage::demand::tests::graph());
        let task = identity(&execution, 1);
        let source = &execution.stage_demand;
        assert!(
            execution
                .stages
                .get_mut(&stage(1))
                .unwrap()
                .task_mut_observed(task.task_id(), source)
                .is_some()
        );
        let foreign_stage = novarocks_execution_contract::StageRef::new(
            novarocks_types::QueryExecutionId::new(
                novarocks_types::QueryId::new(99, 100),
                novarocks_types::AttemptId::new(1).unwrap(),
            )
            .unwrap(),
            stage(1),
        );
        assert!(!foreign.stage_demand.binds_stage(foreign_stage));
        let mut tasks = BTreeMap::new();
        tasks.insert(task.task_id(), crate::test_support::remote(1));
        let mut owner = StageExecution::new(foreign_stage, None, tasks).unwrap();
        assert!(
            owner
                .task_mut_observed(task.task_id(), &execution.stage_demand)
                .is_none()
        );
        assert_eq!(
            owner.task(task.task_id()).unwrap().task_state(),
            TaskState::Planned
        );
    }
    #[test]
    fn stage_cursor_preserves_unproven_producer_after_consumer_falls_and_releases_again() {
        let mut execution =
            crate::test_support::execution_with_graph(crate::stage::demand::tests::graph_chain());
        let parent = identity(&execution, 1);
        let producer = identity(&execution, 2);
        apply(&mut execution, producer, 2, TaskState::Running);
        apply(&mut execution, parent, 2, TaskState::Flushing);
        for _ in 0..3 {
            assert!(execution.propagate_stage_release(&mut Turn(1)).unwrap());
        }
        assert_eq!(execution.stage_demand.all_consumers_released(), Some(true));
        apply(&mut execution, parent, 3, TaskState::Canceling);
        drive(&mut execution);
        assert!(!execution.stage_demand_cuts.contains_key(&stage(2)));
        apply(&mut execution, parent, 4, TaskState::Canceled);
        drive(&mut execution);
        assert!(execution.stage_demand_cuts.contains_key(&stage(2)));
        assert_eq!(
            execution.stage_cancel_after(None).unwrap().1.identity(),
            producer
        );
    }
    #[test]
    fn stage_cursor_preserves_new_release_while_original_sibling_cancel_is_waiting_for_admission() {
        let mut execution =
            crate::test_support::execution_with_graph(crate::stage::demand::tests::graph());
        let parent = identity(&execution, 1);
        let producer = identity(&execution, 2);
        let other = identity(&execution, 3);
        let sibling = identity(&execution, 4);
        apply(&mut execution, producer, 2, TaskState::Running);
        apply(&mut execution, sibling, 2, TaskState::Running);
        apply(&mut execution, parent, 2, TaskState::Flushing);
        drive(&mut execution);
        let (original, request) = execution.stage_cancel_after(None).unwrap();
        assert_eq!(request.identity(), sibling);
        apply(&mut execution, parent, 3, TaskState::Canceling);
        apply(&mut execution, other, 2, TaskState::Flushing);
        drive(&mut execution); // Other parent cannot authorize P yet.
        apply(&mut execution, parent, 4, TaskState::Canceled);
        drive(&mut execution); // Real release arrives during the old Full.
        assert_eq!(execution.stage_cancel_after(None).unwrap().0, original);
        queue_cancel(&mut execution, original, request, 20);
        drive(&mut execution);
        assert!(execution.stage_demand_cuts.contains_key(&stage(2)));
        let (next, next_request) = execution.stage_cancel_after(None).unwrap();
        assert_ne!(next, original);
        assert_eq!(next_request.identity(), producer);
        assert!(execution.stage_cancel_after(Some(next)).is_none());
    }
    #[test]
    fn stage_cursor_preserves_original_cancel_closure_when_terminal_interrupts_partial_parent_fanout()
     {
        let mut execution =
            crate::test_support::execution_with_graph(crate::stage::demand::tests::graph());
        let parent = identity(&execution, 1);
        let other = identity(&execution, 3);
        let producer = identity(&execution, 2);
        apply(&mut execution, producer, 2, TaskState::Running);
        apply(&mut execution, parent, 2, TaskState::Flushing);
        apply(&mut execution, other, 2, TaskState::Flushing);
        while execution.stage_cancel_after(None).is_none() {
            assert!(execution.propagate_stage_release(&mut Turn(1)).unwrap());
        }
        let (operation, _) = execution.stage_cancel_after(None).unwrap();
        assert!(!execution.stage_parent_debts[&stage(1)].scan_done);
        execution.begin_terminal_cleanup();
        assert!(execution.stage_parent_debts.contains_key(&stage(1)));
        execution
            .stage_cancel_unused(operation, crate::input::WithdrawalOutcome::DefinitelyUnsent)
            .unwrap();
        assert!(!execution.stage_parent_debts.contains_key(&stage(1)));
        assert!(!execution.released_children_of.contains(&stage(1))); // Interrupted traversal is not normal completion.
        assert!(!execution.has_pending_stage_propagation());
    }
}
