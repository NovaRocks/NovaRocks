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

//! One context's pure protocol authority. Capacity is synchronously reserved
//! in the same actor step; host work and publication are ordered effects.
use crate::control::EstablishSemanticFacts;
use crate::creation::RequestContext;
use crate::domain::{
    ContextDomainInput, CredentialState, QueryContextDomains, TaskDomainInput, TaskDomains,
};
use crate::observation::CoveredSubscriptionRequest;
use crate::{
    conclusion::{adjudicate, terminal_state, terminating_state},
    creation::{AdmissionNeed, AdmissionPort, AdmissionRefusal, CreationCost, PayloadId},
    lease::{InstalledLease, LeaseBounds, LeaseProgression, MonotonicInstant},
    proposal_summary::{CutId, Proposal, ProposalSummary, SourceId},
    task::{CreationPhase, TaskRecord},
};
use novarocks_control_runtime_core::identity::{Generation, InputId, ReservationId, WorkId};
use novarocks_execution_contract::operation::DomainFailureOutcome;
use novarocks_execution_contract::{
    AbortCause, AdmissionTicketId, LeaseSequence, LeaseValidFor, OperationOutcome, QueryContextRef,
    QueryContextState, TaskConclusion, TaskIdentity, TaskOperationId, TaskState, TerminationDetail,
};
use novarocks_execution_contract::{
    CancelReason, DomainProgression, DomainVersion, ExchangeEdgeId, OperationDomainFailure,
    PlanNodeId, TaskOutputFacts, TaskResourceFacts,
};
use novarocks_execution_contract::{
    DynamicFilterAdvertisement, FINAL_TASK_INFO_MAX_OPERATORS, FinalTaskInfo, OperatorStatistics,
    TaskStatus, TaskStatusVersion, TaskWriterFacts,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

mod reply;
pub use reply::{ContextReplyBody, ContextRequestSettlement};

mod terminal;
pub use terminal::{TerminalIntent, TerminalObservation, TerminalStatistics};

/// The complete original Establish identity selected by the Root. Mutable
/// Context domains never replace this record during replay or lease changes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ContextEstablishment {
    pub operation: TaskOperationId,
    pub ticket: AdmissionTicketId,
    pub payload: PayloadId,
    pub semantics: EstablishSemanticFacts,
    /// The Root's actual election time is also the initial lease origin.
    pub elected_at: MonotonicInstant,
}

/// Actual neutral facts supplied by the successful materialization owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ContextMaterialized {
    pub initial_credential_resource: u64,
}

/// Correlation with the original Root request; this probe creates no new RPC.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EstablishReplayRequest {
    pub request: RequestContext,
    pub ticket: AdmissionTicketId,
    pub payload: PayloadId,
    pub semantics: EstablishSemanticFacts,
}

/// The credential owner measured material against this exact current resource.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EstablishCredentialComparison {
    pub current: CredentialState,
    pub same_material: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EstablishReplayResult {
    AwaitMaterialization,
    CompareCredential { expected: CredentialState },
    Decided(OperationOutcome),
}

#[derive(Clone, Copy, Debug)]
pub struct ContextConfig {
    pub max_tasks: usize,
    pub max_creation_waiters: usize,
    pub max_pending_requests: usize,
    pub max_preparing: usize,
    pub max_task_bytes: u64,
    /// Maximum charged work in one actor step. This core uses one-record
    /// continuation slices; the bound includes internal class probes, not storage.
    pub step_budget: usize,
    pub lease_bounds: LeaseBounds,
    pub metric_publish_interval: std::time::Duration,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CoreError {
    TimeRegressed,
    WrongIdentity,
    GenerationExhausted,
    InvalidConfiguration,
    UnknownTask,
    IllegalFact,
    VersionExhausted,
}
/// The four pre-existing preparation body/publication roles. A hold is
/// admitted before its author may publish a failure that starts cleanup; it
/// exits only after original invocation, capture disposal and publication exit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PreparationExitRole {
    Admission,
    Preparation,
    Input,
    Tail,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PhysicalFact {
    LoaderExited,
    PreparationBodyExited,
    PreparationExited,
    ActualStopped,
    OutputReleased,
    ResourcesConverged,
    RootIdle,
}
#[derive(Clone, Debug, Default)]
pub struct TaskShape {
    pub split_nodes: BTreeSet<PlanNodeId>,
    pub edges: BTreeSet<ExchangeEdgeId>,
    pub destinations: BTreeSet<(ExchangeEdgeId, TaskIdentity)>,
}
#[derive(Debug)]
struct PendingTaskUpdate {
    request: InputId,
    payload: PayloadId,
    transaction: crate::domain::TaskDomainTransaction,
    host_started: bool,
    completing: bool,
    refusal: Option<DomainRefusal>,
}
#[derive(Debug)]
struct PendingContextUpdate {
    request: InputId,
    domains: QueryContextDomains,
    update: ContextDomainInput,
    progression: DomainProgression,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DomainRefusal {
    pub outcome: DomainFailureOutcome,
    pub failure: OperationDomainFailure,
}
impl DomainRefusal {
    fn policy(conflict: novarocks_execution_contract::DomainConflict) -> Self {
        use novarocks_execution_contract::{
            DomainConflict, SafeDetail,
            failure::{FailureClass, FailureFact, FailureReason, domain},
        };
        let code = match conflict {
            DomainConflict::SameTokenDifferentContent => 60,
            DomainConflict::Gap => 61,
            DomainConflict::AfterSeal => 62,
            DomainConflict::UnknownMember => 63,
            DomainConflict::NotMonotonic => 64,
        };
        Self {
            outcome: DomainFailureOutcome::DomainConflict,
            failure: OperationDomainFailure::new(
                FailureFact::new(
                    FailureClass::Invariant,
                    FailureReason::new(domain::WORKER, std::num::NonZeroU16::new(code).unwrap()),
                ),
                SafeDetail::truncating(&conflict.to_string()),
            ),
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RootDrainVerdict {
    Published(u64),
    AlreadyTerminal(TaskState),
    Illegal,
    VersionExhausted,
}
/// A neutral RPC body. Its original request token is supplied only by the
/// actor consuming the admitted input; wire operation IDs never join RPCs.
#[derive(Debug)]
pub enum ContextCommand {
    Cancel {
        task: TaskIdentity,
        reason: CancelReason,
    },
    UpdateTask {
        task: TaskIdentity,
        payload: PayloadId,
        updates: Vec<TaskDomainInput>,
    },
    UpdateContext {
        payload: PayloadId,
        update: ContextDomainInput,
    },
    RootDrained {
        task: TaskIdentity,
    },
    GetFinalInfo {
        task: TaskIdentity,
    },
    GetDynamicFilters {
        task: TaskIdentity,
        acknowledged: Option<DomainVersion>,
    },
    Subscribe {
        generation: u64,
        position: CoveredSubscriptionRequest,
    },
    Create {
        task: TaskIdentity,
        payload: PayloadId,
    },
    Renew {
        sequence: LeaseSequence,
        valid_for: LeaseValidFor,
    },
    Quiesce,
    Abort {
        cause: AbortCause,
    },
    Release,
}
impl ContextCommand {
    pub fn task(&self) -> Option<TaskIdentity> {
        match self {
            Self::Create { task, .. }
            | Self::Cancel { task, .. }
            | Self::UpdateTask { task, .. }
            | Self::RootDrained { task }
            | Self::GetFinalInfo { task }
            | Self::GetDynamicFilters { task, .. } => Some(*task),
            _ => None,
        }
    }
}
#[expect(
    clippy::large_enum_variant,
    reason = "The finite preparation failure cut carries two neutral bounded projections of one source; neither owns a callback or confidential resource."
)]
#[derive(Debug)]
pub enum ContextInput {
    /// Select one currently runnable internal slice; never an RPC request.
    NextInternal,
    /// Transport-only neutral body. `handle_request` binds its original token;
    /// the lifecycle-only `handle` entry rejects this wrapper.
    Request(ContextCommand),
    /// The owning frontend process was fenced. The original lifecycle role
    /// binds the target Context; no RPC token or directory epoch is supplied.
    OwnerFenced,
    /// The driver has consumed this Task's already-admitted summary/cut
    /// predecessors. Source/capture actual exit is a separate obligation.
    ApplyTaskClose {
        task: TaskIdentity,
        generation: Generation,
    },
    ContinueTaskDomain {
        task: TaskIdentity,
    },
    ContinueRequestReplies {
        operation: InputId,
    },
    RequestCompleted {
        operation: InputId,
        outcome: OperationOutcome,
    },
    PreparationBodyFailure {
        task: TaskIdentity,
        preparing: Proposal,
        executing: Proposal,
    },
    ContinueQuiesce,
    Summary {
        task: TaskIdentity,
        generation: Generation,
        cut: Option<CutId>,
        summary: ProposalSummary,
    },
    PreparationExitHold {
        task: TaskIdentity,
        role: PreparationExitRole,
        held: bool,
    },
    OutputFacts {
        task: TaskIdentity,
        facts: TaskOutputFacts,
    },
    /// The original terminal action observed successful production. This
    /// stores one bounded intent without accepting success or claiming exit.
    /// `None` leaves output responsibility with the original result owner.
    CompletionIntent {
        task: TaskIdentity,
        completed_output: Option<TaskOutputFacts>,
    },
    /// Facts co-observed by one original terminal action. Its own actual
    /// exit remains the separate CompletionExited input below.
    TerminalObservation {
        task: TaskIdentity,
        observation: TerminalObservation,
    },
    /// The original terminal action, its captures and its fact have actually
    /// exited after receiving the runtime's real retired-resource proof.
    /// Neither a Task terminal nor preparation whole-exit supplies this fact.
    CompletionExited {
        task: TaskIdentity,
    },
    BindTaskShape {
        task: TaskIdentity,
        shape: TaskShape,
    },
    TaskUpdateApplied {
        request: InputId,
        task: TaskIdentity,
        result: Result<Vec<Option<u64>>, DomainRefusal>,
    },
    ContextUpdateApplied {
        request: InputId,
        result: Result<(), DomainRefusal>,
    },
    DynamicFilterPublished {
        task: TaskIdentity,
        version: DomainVersion,
        payload: PayloadId,
        domain_count: u32,
    },
    OperatorStatistics {
        task: TaskIdentity,
        statistics: Vec<OperatorStatistics>,
        truncated: bool,
    },
    Metrics {
        task: TaskIdentity,
        facts: TaskResourceFacts,
        writer: Option<TaskWriterFacts>,
    },
    MaterializationFinished {
        result: Result<ContextMaterialized, AdmissionRefusal>,
    },
    /// The original admitted M returned all captures without invoking the
    /// materialization I/O because the Context's release fence had already won.
    MaterializationWithdrawn,
    /// A pure entity-owner probe. Its original Root R owns waiting, comparison
    /// custody, deadline handling and the single external reply.
    ProbeEstablish {
        context: QueryContextRef,
        request: EstablishReplayRequest,
        comparison: Option<EstablishCredentialComparison>,
    },
    Loaded {
        task: TaskIdentity,
        result: Result<CreationCost, AdmissionRefusal>,
    },
    ContinueReplies {
        task: TaskIdentity,
    },
    Installed {
        task: TaskIdentity,
    },
    Running {
        task: TaskIdentity,
    },
    Flushing {
        task: TaskIdentity,
    },
    Proposal {
        task: TaskIdentity,
        proposal: Proposal,
    },
    Adjudicate {
        task: TaskIdentity,
    },
    /// The admitted role's causal prefix has reached this same Task FIFO.
    /// Work and callback custody remain outside the pure protocol owner.
    Settle {
        task: TaskIdentity,
        work: WorkId,
    },
    Finish {
        task: TaskIdentity,
    },
    /// Completion of an already handed-off termination, preserving only
    /// positive independent output facts from the original terminal action.
    /// Its proposal/source precedes this event in the same Task causal FIFO.
    FinishWithOutput {
        task: TaskIdentity,
        output: Option<TaskOutputFacts>,
    },
    Physical {
        task: TaskIdentity,
        fact: PhysicalFact,
    },
    HostReleased,
    ContinueClose,
    Tick,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ContextEffect {
    /// Coalescible scheduling metadata. The Core retains every actual cursor.
    WakeInternal,
    EstablishProbed {
        context: QueryContextRef,
        request: RequestContext,
        result: EstablishReplayResult,
        /// Time of this owner observation. Only Decided is a final verdict.
        decided_at: MonotonicInstant,
    },
    /// This is the Context's actual decision after its lease/close check,
    /// rather than proof that the materialization host returned successfully.
    EstablishmentDecided {
        context: QueryContextRef,
        /// Protocol close epoch, not the runtime scope incarnation.
        close_epoch: Generation,
        decision: ContextEstablishmentDecision,
    },
    /// Admit one finite Task-close role and order it after this Task's cuts;
    /// another Task's pending predecessor cannot block the context scan.
    ApplyTaskClose {
        task: TaskIdentity,
        generation: Generation,
    },
    CancelLoader {
        task: TaskIdentity,
        generation: Generation,
    },
    ContinueTaskDomain {
        task: TaskIdentity,
    },
    ContinueRequestReplies {
        operation: InputId,
    },
    ContinueQuiesce,
    WaitQuiesce {
        request: InputId,
    },
    WakeQuiesce,
    SummaryConsumed {
        task: TaskIdentity,
        generation: Generation,
        cut: Option<CutId>,
        diagnostics: crate::proposal_summary::Diagnostics,
    },
    PublishTaskConvergence {
        task: TaskIdentity,
        version: u64,
        snapshot: crate::convergence::TaskConvergenceSnapshot,
    },
    ApplyTaskDomains {
        request: InputId,
        task: TaskIdentity,
        payload: PayloadId,
        progressions: Arc<Vec<DomainProgression>>,
    },
    WaitTaskDomain {
        request: InputId,
        task: TaskIdentity,
        payload: PayloadId,
        updates: Vec<TaskDomainInput>,
    },
    WakeTaskDomains {
        task: TaskIdentity,
    },
    ApplyContextDomain {
        request: InputId,
        payload: PayloadId,
        progression: DomainProgression,
    },
    WaitContextDomain {
        request: InputId,
        payload: PayloadId,
        update: ContextDomainInput,
    },
    WakeContextDomains,
    /// Pin the selected version through the original request's paid read role.
    /// This is synchronous metadata preparation, never a getter or wire action.
    PrepareReadPayload {
        request: InputId,
        task: TaskIdentity,
        version: DomainVersion,
        payload: PayloadId,
    },
    Subscribe {
        request: InputId,
        generation: u64,
        position: Arc<CoveredSubscriptionRequest>,
    },
    PublishMetrics {
        task: TaskIdentity,
        version: u64,
        facts: TaskResourceFacts,
        writer: Option<TaskWriterFacts>,
        snapshot: TaskStatus,
    },
    LoadInput {
        task: TaskIdentity,
        payload: PayloadId,
        reservation: ReservationId,
    },
    EnqueuePreparation {
        task: TaskIdentity,
        payload: PayloadId,
        reservation: ReservationId,
    },
    PublishTask {
        task: TaskIdentity,
        version: u64,
        state: TaskState,
        installed: bool,
        snapshot: TaskStatus,
        convergence: crate::convergence::TaskConvergenceSnapshot,
    },
    PublishConclusion {
        task: TaskIdentity,
        generation: u64,
        conclusion: TaskConclusion,
        source: Option<SourceId>,
    },
    SettlementBarrier {
        task: TaskIdentity,
        work: WorkId,
    },
    DisposeSource {
        task: TaskIdentity,
        source: SourceId,
    },
    StopTask {
        task: TaskIdentity,
        generation: Generation,
    },
    SealRoot {
        context: QueryContextRef,
        generation: Generation,
    },
    CloseExternalAdmission {
        context: QueryContextRef,
        generation: Generation,
    },
    ContinueClose {
        generation: Generation,
    },
    ContinueReplies {
        task: TaskIdentity,
    },
    ReleaseHost {
        context: QueryContextRef,
        generation: Generation,
    },
    RetireTask {
        task: TaskIdentity,
        active: ReservationId,
        preparation: Option<ReservationId>,
        convergence: crate::convergence::TaskConvergenceSnapshot,
    },
    RetainContext {
        context: QueryContextRef,
    },
}
#[derive(Debug, Default)]
pub struct ContextStep {
    // Consumed before this step is handed to the runtime. These are pure
    // decisions, not independently dispatched body-publication actions.
    decisions: Vec<(InputId, Arc<ContextReplyBody>)>,
    pub effects: Vec<ContextEffect>,
    pub requests: Vec<ContextRequestSettlement>,
    /// Distinct protocol records visited by this slice. Fixed-size summary
    /// fields and ordered map lookups are local O(1)/O(log N) decisions.
    pub records_visited: usize,
}
impl ContextStep {
    fn reply(&mut self, request: InputId, body: ContextReplyBody) {
        self.decisions.push((request, Arc::new(body)));
    }
    /// Total admission/verification cost, including each output. A runtime
    /// adapter that calls Turn::effect for each effect/reply separately charges
    /// only `records_visited` as additional pure work; never charge this total
    /// and then debit the same outputs a second time.
    pub fn charged_work(&self) -> usize {
        self.effects.len() + self.requests.len() + self.records_visited
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RequestSettlement {
    pub request: RequestContext,
    pub result: RequestResult,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestResult {
    Outcome(OperationOutcome),
    /// Only this reply role expires. The owner still holds the admitted work.
    TimeoutWhileAccepted,
}
/// The actual Context step authors both lifecycle state and the complete
/// operation decision. Host success alone is never evidence of Active.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContextEstablishmentDecision {
    pub state: QueryContextState,
    pub outcome: Result<OperationOutcome, DomainRefusal>,
}
#[derive(Clone, Debug)]
struct PendingReply {
    body: Arc<ContextReplyBody>,
    settled_at: MonotonicInstant,
}
#[derive(Debug)]
struct PendingQuiesce {
    request: InputId,
    cursor: Option<TaskIdentity>,
    accepted: Vec<TaskIdentity>,
}
#[derive(Debug)]
struct QuiesceDecision {
    receipt: Arc<novarocks_execution_contract::QuiesceQueryContextReceipt>,
    decided_at: MonotonicInstant,
}
#[derive(Clone, Copy, Debug)]
struct Closing {
    cursor: Option<TaskIdentity>,
    cause: AbortCause,
    request: Option<InputId>,
    scan_finished: bool,
}
#[derive(Clone, Copy, Debug)]
enum InternalClass {
    TaskDomain,
    RequestReplies,
    CreationReplies,
    Quiesce,
    Close,
    Maintenance,
}
impl InternalClass {
    const ALL: [Self; 6] = [
        Self::TaskDomain,
        Self::RequestReplies,
        Self::CreationReplies,
        Self::Quiesce,
        Self::Close,
        Self::Maintenance,
    ];
    const fn index(self) -> usize {
        self as usize
    }
}
#[derive(Debug)]
pub struct ContextCore {
    identity: QueryContextRef,
    generation: Generation,
    establishment: ContextEstablishment,
    config: ContextConfig,
    state: QueryContextState,
    now: MonotonicInstant,
    lease: InstalledLease,
    tasks: BTreeMap<TaskIdentity, TaskRecord>,
    spent: usize,
    preparing: usize,
    unretired_tasks: usize,
    closing: Option<Closing>,
    close_applied: bool,
    scheduled_closes: usize,
    abort_waiters: BTreeSet<InputId>,
    host_releasing: bool,
    release_request: Option<InputId>,
    task_domains: BTreeMap<TaskIdentity, TaskDomains>,
    pending_task_updates: BTreeMap<TaskIdentity, PendingTaskUpdate>,
    context_domains: QueryContextDomains,
    pending_context_update: Option<PendingContextUpdate>,
    dynamic_filters: BTreeMap<TaskIdentity, (DomainVersion, PayloadId)>,
    final_info: BTreeMap<TaskIdentity, Arc<FinalTaskInfo>>,
    metrics: BTreeMap<TaskIdentity, TaskResourceFacts>,
    metric_deadlines: BTreeSet<(MonotonicInstant, TaskIdentity)>,
    metric_due: BTreeMap<TaskIdentity, MonotonicInstant>,
    pending_quiesce: Option<PendingQuiesce>,
    quiesce_waiters: BTreeSet<InputId>,
    quiesce_decision: Option<QuiesceDecision>,
    requests: BTreeMap<InputId, RequestContext>,
    request_tasks: BTreeMap<InputId, TaskIdentity>,
    request_deadlines: BTreeSet<(MonotonicInstant, InputId)>,
    pending_replies: BTreeMap<InputId, PendingReply>,
    // Ready indexes contain only keys into the original protocol owners.
    // Awaiting host returns never occupy a runnable class. Task identities
    // are cumulative and bounded by max_tasks; each rotating cursor is one
    // key, not a lazy queue of stale membership or a second Task owner.
    runnable_task_domains: BTreeSet<TaskIdentity>,
    creation_reply_tasks: BTreeSet<TaskIdentity>,
    internal_cursor: usize,
    domain_cursor: Option<TaskIdentity>,
    creation_reply_cursor: Option<TaskIdentity>,
}
impl ContextCore {
    /// One slice visits at most one task/domain member/request/catch-up member.
    /// A conclusion cut has at most two candidates and four diagnostics. Its
    /// largest batch is the two-cut Summary input: 3 lease-close effects +
    /// 5 predecessor effects + 5 supplied-summary effects + 1 cut receipt +
    /// 1 record visit = 15. Request-producing inputs have only one such cut;
    /// reply fanout and every variable-size traversal use a continuation.
    pub const MAX_BODY_WORK: usize = 15;
    /// Effects and replies are outputs; class probes need no output slots.
    pub const MAX_STEP_ACTIONS: usize = 15;
    pub const INTERNAL_CLASSES: usize = InternalClass::ALL.len();
    /// One existing body plus at most one probe of each internal class.
    pub const MAX_STEP_WORK: usize = Self::MAX_BODY_WORK + Self::INTERNAL_CLASSES;
    pub fn max_step_work(&self) -> usize {
        Self::MAX_STEP_WORK
    }

    pub fn new(
        identity: QueryContextRef,
        generation: Generation,
        establishment: ContextEstablishment,
        config: ContextConfig,
    ) -> Result<Self, CoreError> {
        if config.max_tasks == 0
            || config.max_creation_waiters == 0
            || config.max_pending_requests == 0
            || config.max_preparing == 0
            || config.step_budget != Self::MAX_STEP_WORK
        {
            return Err(CoreError::InvalidConfiguration);
        }
        let now = establishment.elected_at;
        let lease = InstalledLease::install_initial(
            establishment.semantics.initial_lease_valid_for,
            config.lease_bounds,
            now,
        );
        Ok(Self {
            identity,
            generation,
            establishment,
            config,
            state: QueryContextState::Establishing,
            now,
            lease,
            tasks: BTreeMap::new(),
            spent: 0,
            preparing: 0,
            unretired_tasks: 0,
            closing: None,
            close_applied: false,
            scheduled_closes: 0,
            abort_waiters: BTreeSet::new(),
            host_releasing: false,
            release_request: None,
            task_domains: BTreeMap::new(),
            pending_task_updates: BTreeMap::new(),
            context_domains: QueryContextDomains::default(),
            pending_context_update: None,
            dynamic_filters: BTreeMap::new(),
            final_info: BTreeMap::new(),
            metrics: BTreeMap::new(),
            metric_deadlines: BTreeSet::new(),
            metric_due: BTreeMap::new(),
            pending_quiesce: None,
            quiesce_waiters: BTreeSet::new(),
            quiesce_decision: None,
            requests: BTreeMap::new(),
            request_tasks: BTreeMap::new(),
            request_deadlines: BTreeSet::new(),
            pending_replies: BTreeMap::new(),
            runnable_task_domains: BTreeSet::new(),
            creation_reply_tasks: BTreeSet::new(),
            internal_cursor: 0,
            domain_cursor: None,
            creation_reply_cursor: None,
        })
    }
    pub fn identity(&self) -> QueryContextRef {
        self.identity
    }
    pub const fn establishment(&self) -> ContextEstablishment {
        self.establishment
    }
    /// Accepted neutral domain facts, never host material or an access right.
    pub const fn current_credential(&self) -> Option<CredentialState> {
        self.context_domains.credential()
    }
    pub fn state(&self) -> QueryContextState {
        self.state
    }
    pub fn generation(&self) -> Generation {
        self.generation
    }
    pub fn task(&self, task: TaskIdentity) -> Option<&TaskRecord> {
        self.tasks.get(&task)
    }
    /// The public observer projection excludes Creating and rejected entities.
    pub fn status(&self, task: TaskIdentity) -> Option<&TaskStatus> {
        self.tasks
            .get(&task)
            .filter(|record| record.accepted())
            .map(|record| &record.published)
    }
    pub fn tasks(&self) -> impl Iterator<Item = &TaskRecord> {
        self.tasks.values()
    }
    pub fn spent_identities(&self) -> usize {
        self.spent
    }
    pub fn next_deadline(&self) -> Option<MonotonicInstant> {
        matches!(
            self.state,
            QueryContextState::Establishing
                | QueryContextState::Active
                | QueryContextState::Quiescing
        )
        .then_some(self.lease.expires_at())
        .into_iter()
        .chain(self.metric_deadlines.first().map(|(at, _)| *at))
        .chain(self.request_deadlines.first().map(|(at, _)| *at))
        .min()
    }
    /// Pure readiness, not permission to start a host job or to consume a reply.
    /// Future deadlines and owners awaiting a host return do not cause a wake.
    pub fn has_internal_work(&self) -> bool {
        !self.runnable_task_domains.is_empty()
            || !self.pending_replies.is_empty()
            || !self.creation_reply_tasks.is_empty()
            || self.pending_quiesce.is_some()
            || (self.quiesce_decision.is_some() && !self.quiesce_waiters.is_empty())
            || self.closing.is_some_and(|closing| {
                !closing.scan_finished || (self.close_applied && !self.abort_waiters.is_empty())
            })
            || self.next_deadline().is_some_and(|at| at <= self.now)
    }
    fn next_internal(&mut self) -> (ContextInput, usize) {
        use std::ops::Bound::{Excluded, Unbounded};
        for offset in 0..Self::INTERNAL_CLASSES {
            let class =
                InternalClass::ALL[(self.internal_cursor + offset) % Self::INTERNAL_CLASSES];
            let input = match class {
                InternalClass::TaskDomain => self
                    .domain_cursor
                    .and_then(|cursor| {
                        self.runnable_task_domains
                            .range((Excluded(cursor), Unbounded))
                            .next()
                    })
                    .or_else(|| self.runnable_task_domains.first())
                    .map(|&task| ContextInput::ContinueTaskDomain { task }),
                // Original tokens, not wire operation IDs. Always select the
                // lowest ready token; higher-token churn cannot pass it.
                InternalClass::RequestReplies => self
                    .pending_replies
                    .first_key_value()
                    .map(|(&operation, _)| ContextInput::ContinueRequestReplies { operation }),
                InternalClass::CreationReplies => self
                    .creation_reply_cursor
                    .and_then(|cursor| {
                        self.creation_reply_tasks
                            .range((Excluded(cursor), Unbounded))
                            .next()
                    })
                    .or_else(|| self.creation_reply_tasks.first())
                    .map(|&task| ContextInput::ContinueReplies { task }),
                InternalClass::Quiesce => (self.pending_quiesce.is_some()
                    || (self.quiesce_decision.is_some() && !self.quiesce_waiters.is_empty()))
                .then_some(ContextInput::ContinueQuiesce),
                InternalClass::Close => self
                    .closing
                    .filter(|closing| {
                        !closing.scan_finished
                            || (self.close_applied && !self.abort_waiters.is_empty())
                    })
                    .map(|_| ContextInput::ContinueClose),
                InternalClass::Maintenance => self
                    .next_deadline()
                    .is_some_and(|at| at <= self.now)
                    .then_some(ContextInput::Tick),
            };
            if let Some(input) = input {
                self.internal_cursor = (class.index() + 1) % Self::INTERNAL_CLASSES;
                match input {
                    ContextInput::ContinueTaskDomain { task } => self.domain_cursor = Some(task),
                    ContextInput::ContinueReplies { task } => {
                        self.creation_reply_cursor = Some(task)
                    }
                    _ => {}
                }
                return (input, offset + 1);
            }
        }
        (ContextInput::NextInternal, Self::INTERNAL_CLASSES)
    }
    /// RPC entry used by the actor consuming this exact admitted input.
    /// Entity/domain rules decide replay; operation IDs never join reply roles.
    pub fn handle_request(
        &mut self,
        now: MonotonicInstant,
        request: RequestContext,
        command: ContextCommand,
        capacity: &mut impl AdmissionPort,
    ) -> Result<ContextStep, CoreError> {
        if now < self.now {
            return Err(CoreError::TimeRegressed);
        }
        if self.requests.contains_key(&request.token) {
            return Err(CoreError::WrongIdentity);
        }
        self.now = now;
        let task = command.task();
        if now.has_reached(request.deadline) {
            let body = match &command {
                ContextCommand::Create { task, .. } => match self.tasks.get(task) {
                    Some(record) => match &record.creation_refusal {
                        Some(AdmissionRefusal::Domain(outcome, failure)) => {
                            ContextReplyBody::Domain(DomainRefusal {
                                outcome: *outcome,
                                failure: failure.clone(),
                            })
                        }
                        _ => ContextReplyBody::Verdict(match record.pending_reply_outcome {
                            Some(OperationOutcome::Accepted) => {
                                RequestResult::Outcome(OperationOutcome::Idempotent)
                            }
                            Some(outcome) => RequestResult::Outcome(outcome),
                            None => RequestResult::TimeoutWhileAccepted,
                        }),
                    },
                    None => ContextReplyBody::Verdict(RequestResult::Outcome(
                        OperationOutcome::OperationTimedOut,
                    )),
                },
                _ => ContextReplyBody::Verdict(RequestResult::Outcome(
                    OperationOutcome::OperationTimedOut,
                )),
            };
            return Ok(ContextStep {
                records_visited: 1,
                requests: vec![ContextRequestSettlement::new(request, task, Arc::new(body))],
                ..ContextStep::default()
            });
        }
        if self.requests.len() >= self.config.max_pending_requests {
            return Ok(ContextStep {
                records_visited: 0,
                requests: vec![ContextRequestSettlement::verdict(
                    request,
                    task,
                    RequestResult::Outcome(OperationOutcome::ResourceExhausted),
                )],
                ..ContextStep::default()
            });
        }
        if let Some(task) = task {
            self.request_tasks.insert(request.token, task);
        }
        self.requests.insert(request.token, request);
        self.request_deadlines
            .insert((request.deadline, request.token));
        self.handle_bound(
            now,
            ContextInput::Request(command),
            Some(request.token),
            capacity,
        )
    }
    pub fn handle(
        &mut self,
        now: MonotonicInstant,
        input: ContextInput,
        capacity: &mut impl AdmissionPort,
    ) -> Result<ContextStep, CoreError> {
        if matches!(input, ContextInput::Request(_)) {
            return Err(CoreError::WrongIdentity);
        }
        self.handle_bound(now, input, None, capacity)
    }
    fn handle_bound(
        &mut self,
        now: MonotonicInstant,
        input: ContextInput,
        request_token: Option<InputId>,
        capacity: &mut impl AdmissionPort,
    ) -> Result<ContextStep, CoreError> {
        if now < self.now {
            return Err(CoreError::TimeRegressed);
        }
        if let ContextInput::ProbeEstablish { context, .. } = &input
            && *context != self.identity
        {
            return Err(CoreError::WrongIdentity);
        }
        self.now = now;
        let mut step = ContextStep::default();
        // A normal quiesce that already won establishment keeps the legacy
        // completion verdict, even when this return reaches the lease deadline.
        // A preceding Tick may already have upgraded it; never undo that fact.
        let quiesced_materialization = self.state == QueryContextState::Quiescing
            && matches!(
                &input,
                ContextInput::MaterializationFinished { .. }
                    | ContextInput::MaterializationWithdrawn
            );
        // Other inputs still check current time before handling their fact.
        if !quiesced_materialization
            && matches!(
                self.state,
                QueryContextState::Establishing
                    | QueryContextState::Active
                    | QueryContextState::Quiescing
            )
            && now.has_reached(self.lease.expires_at())
        {
            self.begin_abort(None, AbortCause::LeaseExpired, &mut step)?;
        }
        // Time and the lease fence are applied once. Selection never recurses
        // through handle and never repeats the public request admission path.
        let (input, internal_probes) = if matches!(input, ContextInput::NextInternal) {
            self.next_internal()
        } else {
            (input, 0)
        };
        match input {
            ContextInput::NextInternal => {}
            ContextInput::Request(command) => self.handle_command(
                now,
                request_token.ok_or(CoreError::WrongIdentity)?,
                command,
                capacity,
                &mut step,
            )?,
            ContextInput::OwnerFenced => {
                self.begin_abort(None, AbortCause::QueryFailed, &mut step)?;
            }
            ContextInput::ApplyTaskClose { task, generation } => {
                if generation != self.generation {
                    return Err(CoreError::WrongIdentity);
                }
                let record = self.tasks.get(&task).ok_or(CoreError::UnknownTask)?;
                if record.scheduled_close_generation != Some(generation)
                    && record.applied_close_generation != generation
                {
                    return Err(CoreError::IllegalFact);
                }
                self.apply_close_to(task, &mut step)?;
                self.try_finish_completion_intent(task, &mut step)?;
            }
            ContextInput::ContinueTaskDomain { task } => {
                self.advance_task_domain(task, &mut step)?
            }
            ContextInput::RequestCompleted { operation, outcome } => {
                self.complete_request(
                    operation,
                    Arc::new(ContextReplyBody::Verdict(RequestResult::Outcome(outcome))),
                    &mut step,
                );
            }
            ContextInput::ContinueRequestReplies { operation } => {
                self.flush_request_replies(operation, &mut step)
            }
            ContextInput::PreparationBodyFailure {
                task,
                preparing,
                executing,
            } => {
                self.flush(task, &mut step)?;
                if preparing.source != executing.source || preparing.evidence != executing.evidence
                {
                    return Err(CoreError::IllegalFact);
                }
                let (
                    TaskConclusion::Terminated(TerminationDetail::Failed(prepare)),
                    TaskConclusion::Terminated(TerminationDetail::Failed(execute)),
                ) = (&preparing.conclusion, &executing.conclusion)
                else {
                    return Err(CoreError::IllegalFact);
                };
                if prepare.phase() != novarocks_execution_contract::TaskFailurePhase::Preparation
                    || execute.phase() != novarocks_execution_contract::TaskFailurePhase::Execution
                    || prepare.fact() != execute.fact()
                    || prepare.detail() != execute.detail()
                {
                    return Err(CoreError::IllegalFact);
                }
                let record = self.tasks.get_mut(&task).ok_or(CoreError::UnknownTask)?;
                let chosen = if record.installed {
                    executing
                } else {
                    preparing
                };
                if let Some(source) = record.proposals.push(chosen) {
                    step.effects
                        .push(ContextEffect::DisposeSource { task, source });
                }
                self.flush(task, &mut step)?;
                self.try_finish_completion_intent(task, &mut step)?;
            }
            ContextInput::ContinueQuiesce => self.advance_quiesce(&mut step)?,
            ContextInput::Summary {
                task,
                generation,
                cut,
                summary,
            } => {
                let record = self.tasks.get(&task).ok_or(CoreError::UnknownTask)?;
                // Runtime must deliver sealed Task segments before the next
                // Task predecessor. A generation already crossed by this
                // Task is a causal-order violation, never a late new winner.
                if generation != record.applied_close_generation {
                    return Err(CoreError::WrongIdentity);
                }
                self.flush(task, &mut step)?;
                self.consume_summary(task, summary, &mut step)?;
                self.try_finish_completion_intent(task, &mut step)?;
                step.effects.push(ContextEffect::SummaryConsumed {
                    task,
                    generation,
                    cut,
                    diagnostics: self
                        .tasks
                        .get(&task)
                        .ok_or(CoreError::UnknownTask)?
                        .diagnostics
                        .clone(),
                });
            }
            ContextInput::PreparationExitHold { task, role, held } => {
                self.flush(task, &mut step)?;
                let t = self.tasks.get_mut(&task).ok_or(CoreError::UnknownTask)?;
                if (held && t.actual_stopped) || t.preparation_exit_holds[role as usize] == held {
                    return Err(CoreError::IllegalFact);
                }
                t.preparation_exit_holds[role as usize] = held;
                let before = t.convergence.snapshot().version();
                Self::apply_runtime_exit_receipts(t, before, &mut step)?;
                self.try_finish_completion_intent(task, &mut step)?;
                self.retire_if_ready(task, &mut step)?;
            }
            ContextInput::OutputFacts { task, facts } => {
                self.flush(task, &mut step)?;
                let record = self.tasks.get_mut(&task).ok_or(CoreError::UnknownTask)?;
                if record.output.responsibility_complete() && !facts.responsibility_complete() {
                    return Err(CoreError::IllegalFact);
                }
                record.output = facts;
                self.try_finish_completion_intent(task, &mut step)?;
            }
            ContextInput::CompletionIntent {
                task,
                completed_output,
            } => {
                self.flush(task, &mut step)?;
                let record = self.tasks.get_mut(&task).ok_or(CoreError::UnknownTask)?;
                if !record.accepted()
                    || !record.installed
                    || completed_output.is_some_and(|facts| !facts.responsibility_complete())
                {
                    return Err(CoreError::IllegalFact);
                }
                if let Some(facts) = completed_output {
                    record.output = facts;
                }
                record.completion_success_pending = !record.state.is_terminal();
                self.try_finish_completion_intent(task, &mut step)?;
            }
            ContextInput::TerminalObservation { task, observation } => {
                self.observe_terminal(task, observation, &mut step)?;
            }
            ContextInput::CompletionExited { task } => {
                self.flush(task, &mut step)?;
                let record = self.tasks.get_mut(&task).ok_or(CoreError::UnknownTask)?;
                if !record.accepted() {
                    return Err(CoreError::IllegalFact);
                }
                record.runtime_actual_stopped_received = true;
                record.runtime_resources_converged_received = true;
                let before = record.convergence.snapshot().version();
                Self::apply_runtime_exit_receipts(record, before, &mut step)?;
                self.try_finish_completion_intent(task, &mut step)?;
                self.retire_if_ready(task, &mut step)?;
            }
            ContextInput::BindTaskShape { task, shape } => {
                let record = self.tasks.get(&task).ok_or(CoreError::UnknownTask)?;
                if record.phase != CreationPhase::Loading || self.task_domains.contains_key(&task) {
                    return Err(CoreError::IllegalFact);
                }
                self.task_domains.insert(
                    task,
                    TaskDomains::from_frozen_shape(
                        shape.split_nodes,
                        shape.edges,
                        shape.destinations,
                    ),
                );
            }
            ContextInput::TaskUpdateApplied {
                request,
                task,
                result,
            } => self.task_update_applied(request, task, result, &mut step)?,
            ContextInput::ContextUpdateApplied { request, result } => {
                let pending = self
                    .pending_context_update
                    .take()
                    .ok_or(CoreError::IllegalFact)?;
                if pending.request != request {
                    self.pending_context_update = Some(pending);
                    return Err(CoreError::WrongIdentity);
                }
                match result {
                    Ok(()) => {
                        self.context_domains = pending.domains;
                        step.reply(
                            request,
                            ContextReplyBody::ContextDomain {
                                outcome: if pending.progression == DomainProgression::Apply {
                                    OperationOutcome::Accepted
                                } else {
                                    OperationOutcome::Idempotent
                                },
                                receipt: self
                                    .context_domains
                                    .receipt(pending.update, pending.progression),
                            },
                        );
                    }
                    Err(failure) => step.reply(request, ContextReplyBody::Domain(failure)),
                }
                step.effects.push(ContextEffect::WakeContextDomains);
            }
            ContextInput::DynamicFilterPublished {
                task,
                version,
                payload,
                domain_count,
            } => {
                self.flush(task, &mut step)?;
                if self
                    .dynamic_filters
                    .get(&task)
                    .is_some_and(|(accepted, _)| *accepted > version)
                {
                    return Err(CoreError::IllegalFact);
                }
                self.dynamic_filters.insert(task, (version, payload));
                let record = self.tasks.get_mut(&task).ok_or(CoreError::UnknownTask)?;
                if !record.state.is_terminal() {
                    record.filter_advertisement =
                        Some(DynamicFilterAdvertisement::new(version, domain_count));
                    record.bump_version();
                    Self::publish(record, &mut step)?;
                }
                self.try_finish_completion_intent(task, &mut step)?;
            }
            ContextInput::OperatorStatistics {
                task,
                statistics,
                truncated,
            } => {
                self.flush(task, &mut step)?;
                let record = self.tasks.get_mut(&task).ok_or(CoreError::UnknownTask)?;
                if !record.state.is_terminal() && !record.statistics_sealed {
                    if statistics.len() > FINAL_TASK_INFO_MAX_OPERATORS {
                        return Err(CoreError::IllegalFact);
                    }
                    record.operator_statistics_truncated |= truncated;
                    record.operator_statistics = statistics;
                }
                self.try_finish_completion_intent(task, &mut step)?;
            }
            ContextInput::Metrics {
                task,
                facts,
                writer,
            } => {
                self.flush(task, &mut step)?;
                if self.metrics.get(&task) != Some(&facts)
                    || writer.is_some_and(|value| self.tasks[&task].writer != Some(value))
                {
                    self.metrics.insert(task, facts);
                    let record = self.tasks.get_mut(&task).ok_or(CoreError::UnknownTask)?;
                    if !record.state.is_terminal() {
                        record.resources = facts;
                        if writer.is_some() {
                            record.writer = writer;
                        }
                        let due = self.metric_due.entry(task).or_insert(now);
                        if now.has_reached(*due) {
                            record.bump_version();
                            let snapshot = Self::compose(record)?;
                            record.published = snapshot.clone();
                            step.effects.push(ContextEffect::PublishMetrics {
                                task,
                                version: record.version,
                                facts,
                                writer: record.writer,
                                snapshot,
                            });
                            *due = now.saturating_add(self.config.metric_publish_interval);
                        } else {
                            self.metric_deadlines.insert((*due, task));
                        }
                    }
                }
                self.try_finish_completion_intent(task, &mut step)?;
            }
            ContextInput::MaterializationWithdrawn => {
                if !matches!(
                    self.state,
                    QueryContextState::Quiescing
                        | QueryContextState::Aborting
                        | QueryContextState::Releasing
                        | QueryContextState::TerminalRetained
                ) {
                    return Err(CoreError::IllegalFact);
                }
                step.effects.push(ContextEffect::EstablishmentDecided {
                    context: self.identity,
                    close_epoch: self.generation,
                    decision: ContextEstablishmentDecision {
                        state: self.state,
                        outcome: Ok(self.closed_outcome()),
                    },
                });
            }
            ContextInput::MaterializationFinished { result } => {
                if result.as_ref().err().is_some_and(|refusal| {
                    matches!(
                        refusal.outcome(),
                        OperationOutcome::Accepted | OperationOutcome::Idempotent
                    )
                }) {
                    return Err(CoreError::IllegalFact);
                }
                let outcome = if quiesced_materialization {
                    Ok(OperationOutcome::ContextTerminalReceipt)
                } else {
                    match result {
                        Ok(materialized) => {
                            if self.state == QueryContextState::Establishing {
                                let initial = self.establishment.semantics;
                                self.context_domains = QueryContextDomains::install_initial(
                                    initial.catalog,
                                    initial.shared_filter,
                                    CredentialState {
                                        lease: initial.credential_lease,
                                        epoch: initial.credential_epoch,
                                        resource: materialized.initial_credential_resource,
                                    },
                                );
                                self.state = QueryContextState::Active;
                            }
                            Ok(if self.state == QueryContextState::Active {
                                OperationOutcome::Accepted
                            } else {
                                self.closed_outcome()
                            })
                        }
                        Err(refusal) => {
                            // Preserve an existing first-wins closing cause.
                            // The operation's domain facts remain independent.
                            if self.state != QueryContextState::Releasing {
                                self.begin_abort(None, AbortCause::QueryFailed, &mut step)?;
                            }
                            match refusal {
                                AdmissionRefusal::Verdict(outcome) => Ok(outcome),
                                AdmissionRefusal::Domain(outcome, failure) => {
                                    Err(DomainRefusal { outcome, failure })
                                }
                            }
                        }
                    }
                };
                step.effects.push(ContextEffect::EstablishmentDecided {
                    context: self.identity,
                    close_epoch: self.generation,
                    decision: ContextEstablishmentDecision {
                        state: self.state,
                        outcome,
                    },
                });
            }
            ContextInput::ProbeEstablish {
                context: _,
                request,
                comparison,
            } => {
                let result = self.probe_establish(request, comparison)?;
                step.effects.push(ContextEffect::EstablishProbed {
                    context: self.identity,
                    request: request.request,
                    result,
                    decided_at: now,
                });
            }
            ContextInput::Loaded { task, result } => {
                self.loaded(task, result, capacity, &mut step)?
            }
            ContextInput::ContinueReplies { task } => {
                self.reply_creation(task, &mut step)?;
                self.retire_if_ready(task, &mut step)?;
            }
            ContextInput::Installed { task } => {
                self.flush(task, &mut step)?;
                let t = self.tasks.get_mut(&task).ok_or(CoreError::UnknownTask)?;
                if t.phase != CreationPhase::Accepted {
                    return Err(CoreError::IllegalFact);
                }
                if !t.installed {
                    if t.state.is_terminal()
                        || matches!(t.conclusion.accepted.as_ref(),Some(TaskConclusion::Terminated(TerminationDetail::Failed(failure))) if failure.phase()==novarocks_execution_contract::TaskFailurePhase::Preparation)
                    {
                        return Err(CoreError::IllegalFact);
                    }
                    t.installed = true;
                    t.bump_version();
                    Self::publish(t, &mut step)?;
                }
                self.try_finish_completion_intent(task, &mut step)?;
            }
            ContextInput::Running { task } | ContextInput::Flushing { task } => {
                let target = if matches!(input, ContextInput::Running { .. }) {
                    TaskState::Running
                } else {
                    TaskState::Flushing
                };
                self.flush(task, &mut step)?;
                let t = self.tasks.get_mut(&task).ok_or(CoreError::UnknownTask)?;
                if !t.installed {
                    return Err(CoreError::IllegalFact);
                }
                if t.conclusion
                    .accepted
                    .as_ref()
                    .is_none_or(|v| matches!(v, TaskConclusion::Succeeded))
                {
                    match crate::lifecycle::classify_task_transition(t.state, target) {
                        crate::lifecycle::TaskTransition::Apply => {
                            t.state = target;
                            t.bump_version();
                            Self::publish(t, &mut step)?;
                        }
                        crate::lifecycle::TaskTransition::SameState => {}
                        _ => return Err(CoreError::IllegalFact),
                    }
                }
                self.try_finish_completion_intent(task, &mut step)?;
            }
            ContextInput::Proposal { task, proposal } => {
                let t = self.tasks.get_mut(&task).ok_or(CoreError::UnknownTask)?;
                if matches!(proposal.conclusion, TaskConclusion::Succeeded)
                    && (!t.actual_stopped || !t.output.responsibility_complete())
                {
                    return Err(CoreError::IllegalFact);
                }
                if let Some(source) = t.proposals.push(proposal) {
                    step.effects
                        .push(ContextEffect::DisposeSource { task, source });
                }
            }
            ContextInput::Adjudicate { task } => {
                self.flush(task, &mut step)?;
                self.try_finish_completion_intent(task, &mut step)?;
            }
            ContextInput::Settle { task, work } => {
                self.flush(task, &mut step)?;
                self.try_finish_completion_intent(task, &mut step)?;
                step.effects
                    .push(ContextEffect::SettlementBarrier { task, work });
            }
            ContextInput::Finish { task } => {
                self.flush(task, &mut step)?;
                self.finish_task(task, &mut step)?;
            }
            ContextInput::FinishWithOutput { task, output } => {
                self.flush(task, &mut step)?;
                let record = self.tasks.get_mut(&task).ok_or(CoreError::UnknownTask)?;
                if let Some(facts) = output {
                    if !facts.responsibility_complete() {
                        return Err(CoreError::IllegalFact);
                    }
                    record.output = facts;
                }
                self.finish_task(task, &mut step)?;
            }
            ContextInput::Physical { task, fact } => {
                self.flush(task, &mut step)?;
                let t = self.tasks.get_mut(&task).ok_or(CoreError::UnknownTask)?;
                let before = t.convergence.snapshot().version();
                match fact {
                    PhysicalFact::LoaderExited => t.loader_exited = true,
                    PhysicalFact::PreparationBodyExited => t.preparation_body_exited = true,
                    PhysicalFact::PreparationExited => {
                        if !t.preparation_exited && t.preparation_reservation.is_some() {
                            self.preparing -= 1;
                        }
                        t.preparation_exited = true;
                    }
                    PhysicalFact::ActualStopped => t.runtime_actual_stopped_received = true,
                    PhysicalFact::OutputReleased => {
                        t.convergence
                            .note_output_released()
                            .map_err(|_| CoreError::IllegalFact)?;
                        t.output_released = true;
                    }
                    PhysicalFact::ResourcesConverged => {
                        if !t.runtime_actual_stopped_received {
                            return Err(CoreError::IllegalFact);
                        }
                        t.runtime_resources_converged_received = true;
                    }
                    PhysicalFact::RootIdle => t.root_idle = true,
                }
                Self::apply_runtime_exit_receipts(t, before, &mut step)?;
                self.try_finish_completion_intent(task, &mut step)?;
                self.retire_if_ready(task, &mut step)?;
            }
            ContextInput::HostReleased => {
                if !self.host_releasing {
                    return Err(CoreError::IllegalFact);
                }
                self.host_releasing = false;
                self.state = QueryContextState::TerminalRetained;
                if let Some(request) = self.release_request.take() {
                    step.reply(
                        request,
                        ContextReplyBody::Verdict(RequestResult::Outcome(
                            OperationOutcome::Accepted,
                        )),
                    );
                }
                step.effects.push(ContextEffect::RetainContext {
                    context: self.identity,
                });
            }
            ContextInput::ContinueClose => self.advance_close(&mut step)?,
            ContextInput::Tick => {
                self.expire_requests(&mut step);
                if !step.requests.is_empty() {
                    // The request expiry owns this variable-record slice.
                } else if self.closing.is_some_and(|closing| !closing.scan_finished) {
                    self.advance_close(&mut step)?;
                } else {
                    for _ in 0..1 {
                        let Some((due, task)) = self.metric_deadlines.first().copied() else {
                            break;
                        };
                        if now < due {
                            break;
                        }
                        self.metric_deadlines.pop_first();
                        self.flush(task, &mut step)?;
                        let record = self.tasks.get_mut(&task).ok_or(CoreError::UnknownTask)?;
                        if !record.state.is_terminal() {
                            record.bump_version();
                            let facts = self.metrics[&task];
                            record.resources = facts;
                            let snapshot = Self::compose(record)?;
                            record.published = snapshot.clone();
                            step.effects.push(ContextEffect::PublishMetrics {
                                task,
                                version: record.version,
                                facts,
                                writer: record.writer,
                                snapshot,
                            });
                            self.metric_due.insert(
                                task,
                                now.saturating_add(self.config.metric_publish_interval),
                            );
                        }
                        self.try_finish_completion_intent(task, &mut step)?;
                    }
                }
            }
        }
        if self.state == QueryContextState::Aborting
            && self.close_applied
            && self.unretired_tasks == 0
            && !self.host_releasing
        {
            self.host_releasing = true;
            step.effects.push(ContextEffect::ReleaseHost {
                context: self.identity,
                generation: self.generation,
            });
        }
        let decisions_visited = self.settle_request_decisions(&mut step);
        step.records_visited =
            1 + decisions_visited + usize::from(!step.requests.is_empty()) + internal_probes;
        Ok(step)
    }
    fn handle_command(
        &mut self,
        now: MonotonicInstant,
        request: InputId,
        command: ContextCommand,
        capacity: &mut impl AdmissionPort,
        step: &mut ContextStep,
    ) -> Result<(), CoreError> {
        match command {
            ContextCommand::Cancel { task, reason } => {
                if self.task_request_cut(request, task, step)? {
                    let record = self.tasks.get_mut(&task).expect("cut located task");
                    if !record.state.is_terminal() {
                        record.proposals.push(Proposal {
                            conclusion: TaskConclusion::Terminated(TerminationDetail::Canceled(
                                reason,
                            )),
                            evidence: None,
                            source: None,
                            diagnostic: None,
                        });
                        self.flush(task, step)?;
                        self.try_finish_completion_intent(task, step)?;
                    }
                    step.reply(
                        request,
                        ContextReplyBody::Verdict(RequestResult::Outcome(
                            OperationOutcome::Accepted,
                        )),
                    );
                }
            }
            ContextCommand::UpdateTask {
                task,
                payload,
                updates,
            } => self.update_task(request, task, payload, updates, step)?,
            ContextCommand::UpdateContext { payload, update } => {
                self.update_context(request, payload, update, step)?
            }
            ContextCommand::RootDrained { task } => self.root_drained(request, task, step)?,
            ContextCommand::GetFinalInfo { task } => {
                if self.task_request_cut(request, task, step)? {
                    self.try_finish_completion_intent(task, step)?;
                    if !self.tasks[&task].state.is_terminal() {
                        step.reply(
                            request,
                            ContextReplyBody::Verdict(RequestResult::Outcome(
                                OperationOutcome::InvalidStateOrRequest,
                            )),
                        );
                    } else if let Some(info) = self.final_info.get(&task) {
                        step.reply(request, ContextReplyBody::FinalInfo(info.clone()));
                    } else {
                        step.reply(
                            request,
                            ContextReplyBody::Verdict(RequestResult::Outcome(
                                OperationOutcome::Idempotent,
                            )),
                        );
                    }
                }
            }
            ContextCommand::GetDynamicFilters { task, acknowledged } => {
                if self.task_request_cut(request, task, step)? {
                    self.try_finish_completion_intent(task, step)?;
                    if let Some((version, payload)) = self
                        .dynamic_filters
                        .get(&task)
                        .filter(|_| self.tasks[&task].phase != CreationPhase::Retained)
                    {
                        step.effects.push(ContextEffect::PrepareReadPayload {
                            request,
                            task,
                            version: *version,
                            payload: *payload,
                        });
                        step.reply(
                            request,
                            ContextReplyBody::ReadPayload {
                                version: *version,
                                payload: *payload,
                                outcome: if acknowledged.is_none_or(|ack| ack < *version) {
                                    OperationOutcome::Accepted
                                } else {
                                    OperationOutcome::Idempotent
                                },
                            },
                        );
                    } else {
                        step.reply(
                            request,
                            ContextReplyBody::Verdict(RequestResult::Outcome(
                                OperationOutcome::Idempotent,
                            )),
                        );
                    }
                }
            }
            ContextCommand::Subscribe {
                generation,
                position,
            } => {
                // Per-target validation belongs to ObservationSource's bounded
                // initial-cut cursor, before it may publish CatchUpComplete.
                if generation == 0
                    || position.status.len() > self.config.max_tasks
                    || position.convergence.len() > self.config.max_tasks
                    || position.required.len() > self.config.max_tasks
                {
                    step.reply(
                        request,
                        ContextReplyBody::Verdict(RequestResult::Outcome(
                            OperationOutcome::InvalidStateOrRequest,
                        )),
                    );
                } else {
                    step.effects.push(ContextEffect::Subscribe {
                        request,
                        generation,
                        position: Arc::new(position),
                    });
                }
            }
            ContextCommand::Create { task, payload } => {
                self.create(request, task, payload, capacity, step)?
            }
            ContextCommand::Renew {
                sequence,
                valid_for,
            } => {
                let outcome = if !matches!(
                    self.state,
                    QueryContextState::Active | QueryContextState::Quiescing
                ) {
                    self.closed_outcome()
                } else {
                    match self.lease.classify_renewal(sequence, valid_for) {
                        LeaseProgression::Apply { .. } => {
                            self.lease = self.lease.renew(
                                sequence,
                                valid_for,
                                self.config.lease_bounds,
                                now,
                            );
                            OperationOutcome::Accepted
                        }
                        LeaseProgression::Idempotent { .. } | LeaseProgression::Stale { .. } => {
                            OperationOutcome::Idempotent
                        }
                        LeaseProgression::Conflict { .. } | LeaseProgression::Gap { .. } => {
                            OperationOutcome::DomainConflict
                        }
                    }
                };
                step.reply(
                    request,
                    ContextReplyBody::Verdict(RequestResult::Outcome(outcome)),
                );
            }
            ContextCommand::Quiesce => {
                if !matches!(
                    self.state,
                    QueryContextState::Establishing
                        | QueryContextState::Active
                        | QueryContextState::Quiescing
                ) {
                    step.reply(
                        request,
                        ContextReplyBody::Verdict(RequestResult::Outcome(self.closed_outcome())),
                    );
                } else if self.pending_quiesce.is_some() {
                    // Only the entity owns this membership scan. Each original
                    // RPC retains a separate bounded reply position.
                    self.quiesce_waiters.insert(request);
                } else if self.state == QueryContextState::Quiescing
                    && let Some(decision) = self.quiesce_decision.as_ref()
                {
                    step.reply(
                        request,
                        ContextReplyBody::Quiesce {
                            outcome: OperationOutcome::Idempotent,
                            receipt: decision.receipt.clone(),
                        },
                    );
                } else {
                    if matches!(
                        self.state,
                        QueryContextState::Establishing | QueryContextState::Active
                    ) {
                        self.state = QueryContextState::Quiescing;
                        step.effects.push(ContextEffect::CloseExternalAdmission {
                            context: self.identity,
                            generation: self.generation,
                        });
                    }
                    self.pending_quiesce = Some(PendingQuiesce {
                        request,
                        cursor: None,
                        accepted: Vec::new(),
                    });
                    step.effects.push(ContextEffect::ContinueQuiesce);
                }
            }
            ContextCommand::Abort { cause } => self.begin_abort(Some(request), cause, step)?,
            ContextCommand::Release => {
                if self.state == QueryContextState::TerminalRetained {
                    step.reply(
                        request,
                        ContextReplyBody::Verdict(RequestResult::Outcome(
                            OperationOutcome::ContextTerminalReceipt,
                        )),
                    );
                } else if self.state != QueryContextState::Quiescing
                    || self.closing.is_some()
                    || self.quiesce_decision.is_none()
                    || self.pending_quiesce.is_some()
                    || self.unretired_tasks != 0
                    || !self.pending_task_updates.is_empty()
                    || self.pending_context_update.is_some()
                {
                    step.reply(
                        request,
                        ContextReplyBody::Verdict(RequestResult::Outcome(
                            OperationOutcome::ReleaseNotReady,
                        )),
                    );
                } else {
                    self.state = QueryContextState::Releasing;
                    self.release_request = Some(request);
                    self.host_releasing = true;
                    step.effects.push(ContextEffect::ReleaseHost {
                        context: self.identity,
                        generation: self.generation,
                    });
                }
            }
        }
        Ok(())
    }
    fn compose(task: &TaskRecord) -> Result<TaskStatus, CoreError> {
        let termination = match task.conclusion.accepted.as_ref() {
            Some(TaskConclusion::Terminated(detail)) => Some(detail.clone()),
            _ => None,
        };
        let mut snapshot = TaskStatus::try_new_with_installed(
            task.identity,
            TaskStatusVersion::new(task.version).map_err(|_| CoreError::VersionExhausted)?,
            task.state,
            termination,
            task.output,
            task.installed,
        )
        .map_err(|_| CoreError::IllegalFact)?
        .with_resources(task.resources);
        if let Some(writer) = task.writer {
            snapshot = snapshot.with_writer(writer);
        }
        if let Some(filter) = task.filter_advertisement {
            snapshot = snapshot.with_dynamic_filters(filter);
        }
        Ok(snapshot)
    }
    fn publish(task: &mut TaskRecord, step: &mut ContextStep) -> Result<(), CoreError> {
        if !task.accepted() {
            return Err(CoreError::IllegalFact);
        }
        let snapshot = Self::compose(task)?;
        task.published = snapshot.clone();
        step.effects.push(ContextEffect::PublishTask {
            task: task.identity,
            version: task.version,
            state: task.state,
            installed: task.installed,
            snapshot,
            convergence: task.convergence.snapshot(),
        });
        Ok(())
    }
    fn closed_outcome(&self) -> OperationOutcome {
        if self
            .closing
            .is_some_and(|c| c.cause == AbortCause::LeaseExpired)
        {
            OperationOutcome::LeaseExpired
        } else {
            OperationOutcome::ContextTerminalReceipt
        }
    }
    fn create(
        &mut self,
        request: InputId,
        task: TaskIdentity,
        payload: PayloadId,
        capacity: &mut impl AdmissionPort,
        step: &mut ContextStep,
    ) -> Result<(), CoreError> {
        if task.verify_query_context(self.identity).is_err() {
            step.reply(
                request,
                ContextReplyBody::Verdict(RequestResult::Outcome(
                    OperationOutcome::IdentityMismatch,
                )),
            );
            return Ok(());
        }
        if let Some(t) = self.tasks.get_mut(&task) {
            if t.phase == CreationPhase::Loading {
                if t.waiting_replies.contains(&request) {
                    return Ok(());
                }
                if t.waiting_replies.len() >= self.config.max_creation_waiters {
                    step.reply(
                        request,
                        ContextReplyBody::Verdict(RequestResult::Outcome(
                            OperationOutcome::ResourceExhausted,
                        )),
                    );
                } else {
                    t.waiting_replies.insert(request);
                }
            } else if let Some(refusal) = &t.creation_refusal {
                Self::refusal_reply(request, refusal.clone(), step);
            } else {
                step.reply(
                    request,
                    ContextReplyBody::Verdict(RequestResult::Outcome(
                        if t.pending_reply_outcome
                            .is_some_and(|outcome| outcome != OperationOutcome::Accepted)
                        {
                            t.pending_reply_outcome
                                .expect("retained creation verdict exists")
                        } else {
                            OperationOutcome::Idempotent
                        },
                    )),
                );
            }
            return Ok(());
        }
        let rejection = if self.state != QueryContextState::Active {
            Some(if self.state == QueryContextState::Establishing {
                OperationOutcome::NotReady
            } else {
                self.closed_outcome()
            })
        } else if self.spent >= self.config.max_tasks {
            Some(OperationOutcome::ResourceExhausted)
        } else {
            None
        };
        if let Some(outcome) = rejection {
            step.reply(
                request,
                ContextReplyBody::Verdict(RequestResult::Outcome(outcome)),
            );
            return Ok(());
        }
        match capacity.try_reserve(AdmissionNeed::ActiveTask {
            context: self.identity,
            task,
            generation: self.generation,
        }) {
            Err(refusal) => Self::refusal_reply(request, refusal, step),
            Ok(reservation) => {
                self.tasks.insert(
                    task,
                    TaskRecord::loading(task, payload, reservation, request, self.generation),
                );
                self.spent += 1;
                self.unretired_tasks += 1;
                step.effects.push(ContextEffect::LoadInput {
                    task,
                    payload,
                    reservation,
                });
            }
        }
        Ok(())
    }
    fn loaded(
        &mut self,
        task: TaskIdentity,
        result: Result<CreationCost, AdmissionRefusal>,
        capacity: &mut impl AdmissionPort,
        step: &mut ContextStep,
    ) -> Result<(), CoreError> {
        self.flush(task, step)?;
        let t = self.tasks.get(&task).ok_or(CoreError::UnknownTask)?;
        if t.phase != CreationPhase::Loading {
            return Err(CoreError::IllegalFact);
        }
        let cost = result.and_then(|c| {
            c.preparation_bytes()
                .filter(|b| *b <= self.config.max_task_bytes)
                .map(|bytes| (c, bytes))
                .ok_or(AdmissionRefusal::Verdict(
                    OperationOutcome::ResourceExhausted,
                ))
        });
        let preparing = self.preparing;
        let reserved = if self.state != QueryContextState::Active {
            Err(AdmissionRefusal::Verdict(self.closed_outcome()))
        } else {
            cost.and_then(|(c, bytes)| {
                if preparing >= self.config.max_preparing {
                    return Err(AdmissionRefusal::Verdict(OperationOutcome::PreparationBusy));
                }
                capacity.try_reserve(AdmissionNeed::Preparation {
                    context: self.identity,
                    task,
                    active: t.active_reservation,
                    generation: self.generation,
                    preparing_in_context: preparing,
                    bytes,
                    inbound_records: c.inbound_records,
                    inbound_bytes: c.inbound_bytes,
                })
            })
        };
        let t = self.tasks.get_mut(&task).expect("checked task");
        let outcome = match reserved {
            Ok(reservation) => {
                t.phase = CreationPhase::Accepted;
                t.preparation_reservation = Some(reservation);
                self.preparing += 1;
                OperationOutcome::Accepted
            }
            Err(refusal) => {
                let outcome = refusal.outcome();
                t.creation_refusal = Some(refusal);
                t.phase = CreationPhase::Rejected;
                // No preparation/runtime holder was ever admitted. Only the
                // original loader/captures still owe a real exit fact.
                t.preparation_body_exited = true;
                t.preparation_exited = true;
                t.actual_stopped = true;
                t.output_released = true;
                t.resources_converged = true;
                t.root_idle = true;
                t.runtime_actual_stopped_received = true;
                t.runtime_resources_converged_received = true;
                t.convergence
                    .note_actual_stopped()
                    .map_err(|_| CoreError::IllegalFact)?;
                t.convergence
                    .note_output_released()
                    .map_err(|_| CoreError::IllegalFact)?;
                t.convergence
                    .note_resources_converged()
                    .map_err(|_| CoreError::IllegalFact)?;
                t.convergence
                    .note_conclusion_stable()
                    .map_err(|_| CoreError::IllegalFact)?;
                outcome
            }
        };
        t.pending_reply_outcome = Some(outcome);
        t.creation_decided_at = Some(self.now);
        if outcome == OperationOutcome::Accepted {
            Self::publish(t, step)?;
            // The runtime must bind this action's started dependency to the
            // preceding publication's actual commit/completion evidence.
            step.effects.push(ContextEffect::EnqueuePreparation {
                task,
                payload: t.payload,
                reservation: t
                    .preparation_reservation
                    .expect("accepted preparation reservation"),
            });
        }
        self.reply_creation(task, step)?;
        self.retire_if_ready(task, step)?;
        Ok(())
    }
    fn refusal_reply(request: InputId, refusal: AdmissionRefusal, step: &mut ContextStep) {
        match refusal {
            AdmissionRefusal::Verdict(outcome) => step.reply(
                request,
                ContextReplyBody::Verdict(RequestResult::Outcome(outcome)),
            ),
            AdmissionRefusal::Domain(outcome, failure) => step.reply(
                request,
                ContextReplyBody::Domain(DomainRefusal { outcome, failure }),
            ),
        }
    }
    fn reply_creation(
        &mut self,
        task: TaskIdentity,
        step: &mut ContextStep,
    ) -> Result<(), CoreError> {
        let t = self.tasks.get_mut(&task).ok_or(CoreError::UnknownTask)?;
        t.pending_reply_outcome.ok_or(CoreError::IllegalFact)?;
        let decided_at = t.creation_decided_at.ok_or(CoreError::IllegalFact)?;
        let reply = t.waiting_replies.pop_first().map(|request| {
            let body = Self::creation_reply_body(t);
            (request, body)
        });
        if !t.waiting_replies.is_empty() {
            self.creation_reply_tasks.insert(task);
            step.effects.push(ContextEffect::ContinueReplies { task });
        } else {
            self.creation_reply_tasks.remove(&task);
        }
        if let Some((request, body)) = reply {
            self.complete_request_at(request, Arc::new(body), decided_at, step);
        }
        Ok(())
    }
    fn creation_reply_body(task: &TaskRecord) -> ContextReplyBody {
        match &task.creation_refusal {
            Some(AdmissionRefusal::Domain(outcome, failure)) => {
                ContextReplyBody::Domain(DomainRefusal {
                    outcome: *outcome,
                    failure: failure.clone(),
                })
            }
            _ => ContextReplyBody::Verdict(RequestResult::Outcome(
                task.pending_reply_outcome
                    .expect("creation reply has an entity decision"),
            )),
        }
    }
    fn retire_if_ready(
        &mut self,
        task: TaskIdentity,
        step: &mut ContextStep,
    ) -> Result<(), CoreError> {
        let t = self.tasks.get_mut(&task).ok_or(CoreError::UnknownTask)?;
        if t.phase != CreationPhase::Retained
            && t.retirement_ready()
            && !self.pending_task_updates.contains_key(&task)
        {
            t.convergence.retire().map_err(|_| CoreError::IllegalFact)?;
            t.phase = CreationPhase::Retained;
            self.unretired_tasks -= 1;
            step.effects.push(ContextEffect::RetireTask {
                task,
                active: t.active_reservation,
                preparation: t.preparation_reservation,
                convergence: t.convergence.snapshot(),
            });
        }
        Ok(())
    }
    /// A single original completion intent has no admission or conclusion
    /// authority of its own. Try it only after the causal prefix was flushed
    /// and after applying each independent stop/output/publication-hold fact.
    fn try_finish_completion_intent(
        &mut self,
        task: TaskIdentity,
        step: &mut ContextStep,
    ) -> Result<(), CoreError> {
        let record = self.tasks.get_mut(&task).ok_or(CoreError::UnknownTask)?;
        if record.state.is_terminal() {
            record.completion_success_pending = false;
            return Ok(());
        }
        if !record.completion_success_pending || !record.actual_stopped {
            return Ok(());
        }
        if !matches!(
            record.conclusion.accepted,
            Some(TaskConclusion::Terminated(_))
        ) {
            if !record.output.responsibility_complete() {
                return Ok(());
            }
            // The same strict preconditions as a direct Succeeded proposal
            // apply. This is the original intent's first legal handoff, not a
            // success accepted before physical stop and applied retroactively.
            record.proposals.push(Proposal {
                conclusion: TaskConclusion::Succeeded,
                evidence: None,
                source: None,
                diagnostic: None,
            });
            self.flush(task, step)?;
        }
        // A winning cancellation/failure is completed as itself. In
        // particular a stopped canceled root need not finish producing output
        // merely to settle a successful-production intent that lost the race.
        self.finish_task(task, step)
    }
    fn finish_task(&mut self, task: TaskIdentity, step: &mut ContextStep) -> Result<(), CoreError> {
        let record = self.tasks.get_mut(&task).ok_or(CoreError::UnknownTask)?;
        let accepted = record
            .conclusion
            .accepted
            .as_ref()
            .ok_or(CoreError::IllegalFact)?;
        if matches!(accepted, TaskConclusion::Succeeded)
            && (!record.actual_stopped || !record.output.responsibility_complete())
        {
            return Err(CoreError::IllegalFact);
        }
        if !record.state.is_terminal() {
            if record.version == u64::MAX {
                return Err(CoreError::VersionExhausted);
            }
            record.state = terminal_state(accepted);
            record.conclusion.sealed = true;
            record
                .convergence
                .note_conclusion_stable()
                .map_err(|_| CoreError::IllegalFact)?;
            record.bump_version();
            Self::publish(record, step)?;
            self.final_info.insert(
                task,
                Arc::new(
                    FinalTaskInfo::try_new(
                        task,
                        record.published.clone(),
                        std::mem::take(&mut record.operator_statistics),
                        record.operator_statistics_truncated,
                    )
                    .map_err(|_| CoreError::IllegalFact)?,
                ),
            );
        }
        record.completion_success_pending = false;
        self.retire_if_ready(task, step)
    }
    fn flush(&mut self, task: TaskIdentity, step: &mut ContextStep) -> Result<(), CoreError> {
        let t = self.tasks.get_mut(&task).ok_or(CoreError::UnknownTask)?;
        let summary = std::mem::take(&mut t.proposals);
        self.consume_summary(task, summary, step)
    }
    fn consume_summary(
        &mut self,
        task: TaskIdentity,
        summary: ProposalSummary,
        step: &mut ContextStep,
    ) -> Result<(), CoreError> {
        let t = self.tasks.get_mut(&task).ok_or(CoreError::UnknownTask)?;
        if !t.accepted() {
            return if summary.is_empty() {
                Ok(())
            } else {
                Err(CoreError::IllegalFact)
            };
        }
        if let Some(publication) = adjudicate(summary, &mut t.conclusion, &mut t.diagnostics) {
            if matches!(publication.conclusion, TaskConclusion::Succeeded)
                && (!t.actual_stopped || !t.output.responsibility_complete())
            {
                return Err(CoreError::IllegalFact);
            }
            if matches!(
                publication.conclusion,
                TaskConclusion::Terminated(TerminationDetail::Failed(_))
            ) && publication.source.is_none()
            {
                return Err(CoreError::IllegalFact);
            }
            if publication.changed {
                step.effects.push(ContextEffect::PublishConclusion {
                    task,
                    generation: publication.generation,
                    conclusion: publication.conclusion.clone(),
                    source: publication.source,
                });
            }
            for source in publication.released {
                step.effects
                    .push(ContextEffect::DisposeSource { task, source });
            }
            if let Some(state) = terminating_state(&publication.conclusion)
                && publication.changed
                && !t.state.is_terminal()
            {
                t.state = state;
                t.bump_version();
                Self::publish(t, step)?;
                step.effects.push(ContextEffect::StopTask {
                    task,
                    generation: self.generation,
                });
            }
        }
        Ok(())
    }
    fn begin_abort(
        &mut self,
        request: Option<InputId>,
        cause: AbortCause,
        step: &mut ContextStep,
    ) -> Result<(), CoreError> {
        if matches!(
            self.state,
            QueryContextState::TerminalRetained | QueryContextState::Gone
        ) {
            if let Some(request) = request {
                step.reply(
                    request,
                    ContextReplyBody::Verdict(RequestResult::Outcome(
                        OperationOutcome::ContextTerminalReceipt,
                    )),
                );
            }
            return Ok(());
        }
        if let Some(closing) = self.closing {
            if let Some(request) = request {
                if self.close_applied {
                    step.reply(
                        request,
                        ContextReplyBody::Verdict(RequestResult::Outcome(
                            OperationOutcome::Idempotent,
                        )),
                    );
                } else if closing.request != Some(request) && !self.abort_waiters.contains(&request)
                {
                    // Only live reply roles contribute a later Abort waiter.
                    // The RPC admission bound already owns this storage; no
                    // additional protocol refusal is introduced here.
                    self.abort_waiters.insert(request);
                }
            }
            return Ok(());
        }
        self.generation = self
            .generation
            .next()
            .ok_or(CoreError::GenerationExhausted)?;
        self.state = QueryContextState::Aborting;
        self.closing = Some(Closing {
            cursor: None,
            cause,
            request,
            scan_finished: false,
        });
        self.close_applied = false;
        step.effects.push(ContextEffect::CloseExternalAdmission {
            context: self.identity,
            generation: self.generation,
        });
        step.effects.push(ContextEffect::SealRoot {
            context: self.identity,
            generation: self.generation,
        });
        step.effects.push(ContextEffect::ContinueClose {
            generation: self.generation,
        });
        Ok(())
    }
    fn apply_close_to(
        &mut self,
        task: TaskIdentity,
        step: &mut ContextStep,
    ) -> Result<(), CoreError> {
        let Some(closing) = self.closing else {
            return Err(CoreError::IllegalFact);
        };
        let t = self.tasks.get_mut(&task).ok_or(CoreError::UnknownTask)?;
        if let Some(scheduled) = t.scheduled_close_generation.take() {
            if scheduled != self.generation {
                return Err(CoreError::WrongIdentity);
            }
            self.scheduled_closes = self
                .scheduled_closes
                .checked_sub(1)
                .ok_or(CoreError::IllegalFact)?;
        }
        if t.applied_close_generation != self.generation {
            t.applied_close_generation = self.generation;
            if !t.accepted() {
                // Creating never entered the public Task protocol. Only its
                // input role is cancelled; original captures still owe exit.
                if !t.loader_exited {
                    step.effects.push(ContextEffect::CancelLoader {
                        task,
                        generation: self.generation,
                    });
                }
            } else {
                let proposal = Proposal {
                    conclusion: TaskConclusion::Terminated(TerminationDetail::Aborted(
                        closing.cause,
                    )),
                    evidence: None,
                    source: None,
                    diagnostic: None,
                };
                if let Some(source) = t.proposals.push(proposal) {
                    step.effects
                        .push(ContextEffect::DisposeSource { task, source });
                }
                self.flush(task, step)?;
            }
        }
        self.finish_close_if_ready(step);
        Ok(())
    }
    fn finish_close_if_ready(&mut self, step: &mut ContextStep) {
        let Some(closing) = self.closing.as_mut() else {
            return;
        };
        if self.close_applied || !closing.scan_finished || self.scheduled_closes != 0 {
            return;
        }
        self.close_applied = true;
        if let Some(request) = closing.request.take() {
            step.reply(
                request,
                ContextReplyBody::Verdict(RequestResult::Outcome(OperationOutcome::Accepted)),
            );
        }
        if !self.abort_waiters.is_empty() {
            step.effects.push(ContextEffect::ContinueClose {
                generation: self.generation,
            });
        }
    }
    fn advance_close(&mut self, step: &mut ContextStep) -> Result<(), CoreError> {
        use std::ops::Bound::{Excluded, Unbounded};
        let Some(mut closing) = self.closing else {
            return Ok(());
        };
        if self.close_applied {
            if let Some(request) = self.abort_waiters.pop_first() {
                step.reply(
                    request,
                    ContextReplyBody::Verdict(RequestResult::Outcome(OperationOutcome::Idempotent)),
                );
            }
            if !self.abort_waiters.is_empty() {
                step.effects.push(ContextEffect::ContinueClose {
                    generation: self.generation,
                });
            }
            return Ok(());
        }
        if closing.scan_finished {
            self.finish_close_if_ready(step);
            return Ok(());
        }
        let next = match closing.cursor {
            Some(cursor) => self.tasks.range_mut((Excluded(cursor), Unbounded)),
            None => self.tasks.range_mut(..),
        }
        .next();
        if let Some((task, record)) = next {
            closing.cursor = Some(*task);
            if record.applied_close_generation != self.generation
                && record.scheduled_close_generation.is_none()
            {
                record.scheduled_close_generation = Some(self.generation);
                self.scheduled_closes += 1;
                step.effects.push(ContextEffect::ApplyTaskClose {
                    task: *task,
                    generation: self.generation,
                });
            }
        }
        let more = closing.cursor.is_some_and(|cursor| {
            self.tasks
                .range((Excluded(cursor), Unbounded))
                .next()
                .is_some()
        });
        closing.scan_finished = !more;
        self.closing = Some(closing);
        if more {
            step.effects.push(ContextEffect::ContinueClose {
                generation: self.generation,
            });
        }
        self.finish_close_if_ready(step);
        Ok(())
    }
    fn task_request_cut(
        &mut self,
        request: InputId,
        task: TaskIdentity,
        step: &mut ContextStep,
    ) -> Result<bool, CoreError> {
        let outcome = if task.verify_query_context(self.identity).is_err() {
            Some(OperationOutcome::IdentityMismatch)
        } else if !self.tasks.contains_key(&task) {
            Some(OperationOutcome::InvalidStateOrRequest)
        } else {
            None
        };
        if let Some(outcome) = outcome {
            step.reply(
                request,
                ContextReplyBody::Verdict(RequestResult::Outcome(outcome)),
            );
            return Ok(false);
        }
        self.flush(task, step)?;
        if self.tasks[&task].phase == CreationPhase::Loading {
            step.reply(
                request,
                ContextReplyBody::Verdict(RequestResult::Outcome(
                    OperationOutcome::InvalidStateOrRequest,
                )),
            );
            return Ok(false);
        }
        Ok(true)
    }
    fn update_task(
        &mut self,
        request: InputId,
        task: TaskIdentity,
        payload: PayloadId,
        updates: Vec<TaskDomainInput>,
        step: &mut ContextStep,
    ) -> Result<(), CoreError> {
        if !matches!(
            self.state,
            QueryContextState::Active | QueryContextState::Quiescing
        ) {
            step.reply(
                request,
                ContextReplyBody::Verdict(RequestResult::Outcome(self.closed_outcome())),
            );
            return Ok(());
        }
        if !self.task_request_cut(request, task, step)? {
            return Ok(());
        }
        self.try_finish_completion_intent(task, step)?;
        if self.tasks[&task].state.is_terminal() {
            step.reply(
                request,
                ContextReplyBody::Verdict(RequestResult::Outcome(
                    OperationOutcome::TerminalRejected,
                )),
            );
            return Ok(());
        }
        if self.pending_task_updates.contains_key(&task) {
            step.effects.push(ContextEffect::WaitTaskDomain {
                request,
                task,
                payload,
                updates,
            });
            return Ok(());
        }
        let Some(domains) = self.task_domains.remove(&task) else {
            step.reply(
                request,
                ContextReplyBody::Verdict(RequestResult::Outcome(
                    OperationOutcome::InvalidStateOrRequest,
                )),
            );
            return Ok(());
        };
        self.runnable_task_domains.insert(task);
        self.pending_task_updates.insert(
            task,
            PendingTaskUpdate {
                request,
                payload,
                transaction: crate::domain::TaskDomainTransaction::new(domains, updates),
                host_started: false,
                completing: false,
                refusal: None,
            },
        );
        step.effects
            .push(ContextEffect::ContinueTaskDomain { task });
        Ok(())
    }
    fn task_update_applied(
        &mut self,
        request: InputId,
        task: TaskIdentity,
        result: Result<Vec<Option<u64>>, DomainRefusal>,
        step: &mut ContextStep,
    ) -> Result<(), CoreError> {
        self.flush(task, step)?;
        self.try_finish_completion_intent(task, step)?;
        let pending = self
            .pending_task_updates
            .get_mut(&task)
            .ok_or(CoreError::IllegalFact)?;
        if pending.request != request || !pending.host_started || pending.completing {
            return Err(CoreError::WrongIdentity);
        }
        match result {
            Ok(queued) => {
                if !pending.transaction.begin_completion(queued) {
                    return Err(CoreError::IllegalFact);
                }
            }
            Err(refusal) => {
                pending.refusal = Some(refusal);
                // The conflict is only a journal state marker; the original
                // typed host failure remains the externally reported fact.
                pending
                    .transaction
                    .rollback(novarocks_execution_contract::DomainConflict::NotMonotonic);
            }
        }
        pending.completing = true;
        self.runnable_task_domains.insert(task);
        step.effects
            .push(ContextEffect::ContinueTaskDomain { task });
        Ok(())
    }
    fn advance_task_domain(
        &mut self,
        task: TaskIdentity,
        step: &mut ContextStep,
    ) -> Result<(), CoreError> {
        use crate::domain::TaskDomainTransactionProgress as P;
        self.flush(task, step)?;
        self.try_finish_completion_intent(task, step)?;
        let mut pending = self
            .pending_task_updates
            .remove(&task)
            .ok_or(CoreError::IllegalFact)?;
        if pending.host_started && !pending.completing {
            self.pending_task_updates.insert(task, pending);
            return Err(CoreError::IllegalFact);
        }
        self.runnable_task_domains.remove(&task);
        match pending.transaction.advance() {
            P::Pending => {
                self.runnable_task_domains.insert(task);
                self.pending_task_updates.insert(task, pending);
                step.effects
                    .push(ContextEffect::ContinueTaskDomain { task });
            }
            P::Ready => {
                pending.host_started = true;
                step.effects.push(ContextEffect::ApplyTaskDomains {
                    request: pending.request,
                    task,
                    payload: pending.payload,
                    progressions: pending.transaction.progressions(),
                });
                self.pending_task_updates.insert(task, pending);
            }
            P::RolledBack(conflict) => {
                let (domains, _, _) = pending.transaction.into_result();
                self.task_domains.insert(task, domains);
                step.reply(
                    pending.request,
                    ContextReplyBody::Domain(
                        pending
                            .refusal
                            .unwrap_or_else(|| DomainRefusal::policy(conflict)),
                    ),
                );
                step.effects.push(ContextEffect::WakeTaskDomains { task });
            }
            P::Complete => {
                let (domains, receipts, applied) = pending.transaction.into_result();
                self.task_domains.insert(task, domains);
                step.reply(
                    pending.request,
                    ContextReplyBody::TaskDomain {
                        outcome: if applied {
                            OperationOutcome::Accepted
                        } else {
                            OperationOutcome::Idempotent
                        },
                        receipts: Arc::new(receipts),
                    },
                );
                step.effects.push(ContextEffect::WakeTaskDomains { task });
            }
        }
        Ok(())
    }
    fn probe_establish(
        &self,
        candidate: EstablishReplayRequest,
        comparison: Option<EstablishCredentialComparison>,
    ) -> Result<EstablishReplayResult, CoreError> {
        match self.state {
            QueryContextState::Quiescing | QueryContextState::TerminalRetained => {
                return Ok(EstablishReplayResult::Decided(
                    OperationOutcome::ContextTerminalReceipt,
                ));
            }
            QueryContextState::Aborting | QueryContextState::Releasing => {
                return Ok(EstablishReplayResult::Decided(
                    OperationOutcome::InvalidStateOrRequest,
                ));
            }
            QueryContextState::Establishing | QueryContextState::Active => {}
            _ => return Err(CoreError::IllegalFact),
        }
        if candidate.request.operation != self.establishment.operation
            || candidate.ticket != self.establishment.ticket
            || candidate.semantics != self.establishment.semantics
        {
            return Ok(EstablishReplayResult::Decided(
                OperationOutcome::ContextConflict,
            ));
        }
        // A new carrier's payload identity is not part of entity equality.
        if self.state == QueryContextState::Establishing {
            return Ok(EstablishReplayResult::AwaitMaterialization);
        }
        let current = self
            .context_domains
            .credential()
            .ok_or(CoreError::IllegalFact)?;
        let Some(comparison) = comparison.filter(|fact| fact.current == current) else {
            return Ok(EstablishReplayResult::CompareCredential { expected: current });
        };
        Ok(EstablishReplayResult::Decided(
            if candidate.semantics.credential_lease == current.lease
                && candidate.semantics.credential_epoch == current.epoch
                && comparison.same_material
            {
                OperationOutcome::Idempotent
            } else {
                OperationOutcome::ContextConflict
            },
        ))
    }
    fn update_context(
        &mut self,
        request: InputId,
        payload: PayloadId,
        update: ContextDomainInput,
        step: &mut ContextStep,
    ) -> Result<(), CoreError> {
        if !matches!(
            self.state,
            QueryContextState::Active | QueryContextState::Quiescing
        ) {
            step.reply(
                request,
                ContextReplyBody::Verdict(RequestResult::Outcome(self.closed_outcome())),
            );
            return Ok(());
        }
        if self.pending_context_update.is_some() {
            step.effects.push(ContextEffect::WaitContextDomain {
                request,
                payload,
                update,
            });
            return Ok(());
        }
        let mut domains = self.context_domains.clone();
        let progression = match domains.apply(update) {
            Ok(value) => value,
            Err(conflict) => {
                step.reply(
                    request,
                    ContextReplyBody::Domain(DomainRefusal::policy(conflict)),
                );
                return Ok(());
            }
        };
        self.pending_context_update = Some(PendingContextUpdate {
            request,
            domains,
            update,
            progression,
        });
        step.effects.push(ContextEffect::ApplyContextDomain {
            request,
            payload,
            progression,
        });
        Ok(())
    }
    fn root_drained(
        &mut self,
        request: InputId,
        task: TaskIdentity,
        step: &mut ContextStep,
    ) -> Result<(), CoreError> {
        if !self.task_request_cut(request, task, step)? {
            return Ok(());
        }
        let record = self.tasks.get_mut(&task).expect("located root");
        let result = match crate::lifecycle::classify_root_drain(record.state) {
            crate::lifecycle::RootDrainAction::Illegal => RootDrainVerdict::Illegal,
            crate::lifecycle::RootDrainAction::AlreadyTerminal => {
                record.output = TaskOutputFacts::new(true);
                RootDrainVerdict::AlreadyTerminal(record.state)
            }
            crate::lifecycle::RootDrainAction::Finish
            | crate::lifecycle::RootDrainAction::RecordOnly => {
                if record.version == u64::MAX {
                    RootDrainVerdict::VersionExhausted
                } else {
                    record.output = TaskOutputFacts::new(true);
                    let before = record.version;
                    self.try_finish_completion_intent(task, step)?;
                    let record = self.tasks.get_mut(&task).expect("located root");
                    // A finished completion intent already published this
                    // exact output fact with the final status. Otherwise
                    // retain the existing output-only publication behavior.
                    if record.version == before {
                        record.bump_version();
                        Self::publish(record, step)?;
                    }
                    RootDrainVerdict::Published(record.version)
                }
            }
        };
        step.reply(request, ContextReplyBody::RootDrain(result));
        self.retire_if_ready(task, step)?;
        Ok(())
    }
    fn apply_runtime_exit_receipts(
        task: &mut TaskRecord,
        before: u64,
        step: &mut ContextStep,
    ) -> Result<(), CoreError> {
        let runtime_unheld = !task.preparation_exit_holds.iter().any(|held| *held);
        if runtime_unheld && task.runtime_actual_stopped_received {
            task.convergence
                .note_actual_stopped()
                .map_err(|_| CoreError::IllegalFact)?;
            task.actual_stopped = true;
        }
        if runtime_unheld && task.runtime_resources_converged_received {
            task.convergence
                .note_resources_converged()
                .map_err(|_| CoreError::IllegalFact)?;
            task.resources_converged = true;
        }
        let snapshot = task.convergence.snapshot();
        if snapshot.version() != before {
            step.effects.push(ContextEffect::PublishTaskConvergence {
                task: task.identity,
                version: snapshot.version(),
                snapshot,
            });
        }
        Ok(())
    }
    fn advance_quiesce(&mut self, step: &mut ContextStep) -> Result<(), CoreError> {
        use std::ops::Bound::{Excluded, Unbounded};
        let Some(mut pending) = self.pending_quiesce.take() else {
            if let Some(request) = self.quiesce_waiters.pop_first() {
                let decision = self
                    .quiesce_decision
                    .as_ref()
                    .ok_or(CoreError::IllegalFact)?;
                let receipt = decision.receipt.clone();
                let decided_at = decision.decided_at;

                self.complete_request_at(
                    request,
                    Arc::new(ContextReplyBody::Quiesce {
                        outcome: OperationOutcome::Idempotent,
                        receipt,
                    }),
                    decided_at,
                    step,
                );
                if !self.quiesce_waiters.is_empty() {
                    step.effects.push(ContextEffect::ContinueQuiesce);
                }
            }
            return Ok(());
        };
        let records = match pending.cursor {
            Some(cursor) => self.tasks.range((Excluded(cursor), Unbounded)),
            None => self.tasks.range(..),
        };
        for (identity, record) in records.take(1) {
            pending.cursor = Some(*identity);
            if record.pending_reply_outcome == Some(OperationOutcome::Accepted) {
                pending.accepted.push(*identity);
            }
        }
        let more = pending.cursor.is_some_and(|cursor| {
            self.tasks
                .range((Excluded(cursor), Unbounded))
                .next()
                .is_some()
        });
        if more {
            self.pending_quiesce = Some(pending);
            step.effects.push(ContextEffect::ContinueQuiesce);
        } else {
            let outcome = if self.quiesce_decision.is_some() {
                OperationOutcome::Idempotent
            } else {
                OperationOutcome::Accepted
            };
            let receipt = Arc::new(
                novarocks_execution_contract::QuiesceQueryContextReceipt::new(
                    self.identity,
                    1,
                    pending.accepted,
                    self.state,
                ),
            );
            self.quiesce_decision = Some(QuiesceDecision {
                receipt: receipt.clone(),
                decided_at: self.now,
            });

            self.complete_request_at(
                pending.request,
                Arc::new(ContextReplyBody::Quiesce { outcome, receipt }),
                self.now,
                step,
            );
            if !self.quiesce_waiters.is_empty() {
                step.effects.push(ContextEffect::ContinueQuiesce);
            }
        }
        Ok(())
    }
    fn complete_request(
        &mut self,
        token: InputId,
        body: Arc<ContextReplyBody>,
        step: &mut ContextStep,
    ) {
        self.complete_request_at(token, body, self.now, step);
    }
    fn complete_request_at(
        &mut self,
        token: InputId,
        body: Arc<ContextReplyBody>,
        settled_at: MonotonicInstant,
        step: &mut ContextStep,
    ) {
        if !self.requests.contains_key(&token) {
            return;
        }
        self.pending_replies
            .entry(token)
            .or_insert(PendingReply { body, settled_at });
        self.flush_request_replies(token, step);
    }
    fn flush_request_replies(&mut self, token: InputId, step: &mut ContextStep) {
        if !step.requests.is_empty() {
            return;
        }
        let Some(reply) = self.pending_replies.remove(&token) else {
            return;
        };
        let task = self.request_tasks.get(&token).copied();
        let request = self
            .remove_request(token)
            .expect("pending reply has its original request");
        step.requests
            .push(if reply.settled_at.has_reached(request.deadline) {
                ContextRequestSettlement::verdict(
                    request,
                    task,
                    RequestResult::TimeoutWhileAccepted,
                )
            } else {
                ContextRequestSettlement::new(request, task, reply.body)
            });
    }
    fn remove_request(&mut self, token: InputId) -> Option<RequestContext> {
        let request = self.requests.remove(&token)?;
        self.request_deadlines.remove(&(request.deadline, token));
        self.pending_replies.remove(&token);
        self.abort_waiters.remove(&token);
        self.quiesce_waiters.remove(&token);
        if let Some(task) = self.request_tasks.remove(&token)
            && let Some(record) = self.tasks.get_mut(&task)
        {
            // Remove only this RPC waiter, never the entity's admitted work.
            record.waiting_replies.remove(&token);
            if record.waiting_replies.is_empty() {
                self.creation_reply_tasks.remove(&task);
            }
        }
        Some(request)
    }
    fn expire_requests(&mut self, step: &mut ContextStep) {
        let Some((at, token)) = self.request_deadlines.first().copied() else {
            return;
        };
        if self.now < at {
            return;
        }
        // A complete body decided before the deadline remains its original
        // fact even if bounded delivery reaches this token on a later turn.
        if self
            .pending_replies
            .get(&token)
            .is_some_and(|reply| reply.settled_at < at)
        {
            self.flush_request_replies(token, step);
            return;
        }
        // The entity may have decided all its original waiters while bounded
        // fanout has not yet selected this one. Membership, not same Task or
        // operation identity, grants access to that original decision time.
        let creation = self.request_tasks.get(&token).and_then(|task| {
            let record = self.tasks.get(task)?;
            if !record.waiting_replies.contains(&token) {
                return None;
            }
            let decided_at = record.creation_decided_at.filter(|decided| *decided < at)?;
            Some((Self::creation_reply_body(record), decided_at))
        });
        let quiesce = || {
            if !self.quiesce_waiters.contains(&token) {
                return None;
            }
            let decision = self
                .quiesce_decision
                .as_ref()
                .filter(|decision| decision.decided_at < at)?;
            Some((
                ContextReplyBody::Quiesce {
                    outcome: OperationOutcome::Idempotent,
                    receipt: decision.receipt.clone(),
                },
                decision.decided_at,
            ))
        };
        if let Some((body, decided_at)) = creation.or_else(quiesce) {
            self.complete_request_at(token, Arc::new(body), decided_at, step);
            return;
        }
        let task = self.request_tasks.get(&token).copied();
        let request = self
            .remove_request(token)
            .expect("deadline has its original request");
        step.requests.push(ContextRequestSettlement::verdict(
            request,
            task,
            RequestResult::TimeoutWhileAccepted,
        ));
    }
    fn settle_request_decisions(&mut self, step: &mut ContextStep) -> usize {
        let decisions = std::mem::take(&mut step.decisions);
        let visited = decisions.len();
        for (token, body) in decisions {
            self.complete_request(token, body, step);
        }
        if let Some((&operation, _)) = self.pending_replies.first_key_value() {
            step.effects
                .push(ContextEffect::ContinueRequestReplies { operation });
        }
        visited
    }
}

#[cfg(test)]
#[path = "context/internal_tests.rs"]
mod internal_tests;
