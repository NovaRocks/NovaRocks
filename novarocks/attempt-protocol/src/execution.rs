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

use crate::completion_facts::parent_released_children;
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

/// The task protocol of one query execution attempt.
#[derive(Debug)]
pub struct QueryTaskExecution {
    graph: TaskGraph,
    stages: BTreeMap<StageId, StageExecution>,
    stage_of_task: BTreeMap<TaskId, StageId>,
    owners: BTreeMap<ParticipationRef, ParticipationOwner>,
    dispatcher: OperationDispatcher,
    edges: EdgeOpenTracker,
    derived_edges: EdgeDerivations,
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
    covered_reconciliations: BTreeSet<ParticipationRef>,
    normal_drain_requested: bool,
    normal_drain_started: bool,
    profile_release_deadline: Option<MonotonicInstant>,
    terminal_cleanup_started: bool,
    failure: TerminationLatch,
    accepted_protocol_revision: u64,
    // One ordinal per frozen Task, appended only on its first accepted terminal.
    // Diagnostic readers keep their own cursor; no idle graph scan is needed.
    accepted_terminal_tasks: Vec<TaskIdentity>,
    accepted_terminal_seen: BTreeSet<TaskIdentity>,
    accepted_protocol_dirty_tasks: BTreeSet<TaskId>,
    accepted_protocol_writer_evidence: BTreeMap<TaskId, Arc<TaskStatus>>,
    required_result_terminal_control_deadline: Option<MonotonicInstant>,
    drained_tasks: BTreeSet<TaskId>,
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
        mut ids: OperationIds,
        requested_leases: RequestedLeaseDurations,
    ) -> Result<Self, TaskExecutionError> {
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
        Ok(Self {
            graph,
            stages,
            stage_of_task,
            owners,
            dispatcher,
            edges,
            derived_edges: EdgeDerivations::default(),
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
            covered_reconciliations: BTreeSet::new(),
            normal_drain_requested: false,
            normal_drain_started: false,
            profile_release_deadline: None,
            terminal_cleanup_started: false,
            failure: TerminationLatch::open(),
            accepted_protocol_revision: 0,
            accepted_terminal_tasks: Vec::new(),
            accepted_terminal_seen: BTreeSet::new(),
            accepted_protocol_dirty_tasks: BTreeSet::new(),
            accepted_protocol_writer_evidence: BTreeMap::new(),
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
        Ok(())
    }

    /// Requests normal closure after the root seal. Strict producers and
    /// consumers keep their original admission and input owners until their
    /// Task terminals arrive; result demand cannot quiesce required work.
    pub(crate) fn begin_normal_drain(&mut self) {
        self.normal_drain_requested = true;
        self.advance_normal_drain();
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

    fn advance_normal_drain(&mut self) {
        if !self.normal_drain_requested
            || self.normal_drain_started
            || self.terminal_cleanup_started
            || self.failure_cause().is_some()
            || !self.strict_pending.is_empty()
        {
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
                        && !self.dispatcher.queued_destination_close(operation_id)))
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
            for (_, mut task) in stage.tasks_mut() {
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
            .task_mut(task_id)
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
                        for (_, mut task) in stage.tasks_mut() {
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
        let mut task = stage.task_mut(task_id).expect("the task was just located");
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
        let now = self.now;
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
                    .cancel_normally_stood_down_tasks(&mut self.ids);
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
                    .and_then(|stage| stage.task_mut(task))
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
