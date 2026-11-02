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
use novarocks_control_runtime_core::identity::{Generation, InputId, ReservationId, ScopedWorkRef};
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
    TaskStatus, TaskWriterFacts,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

mod initial_creation;
pub(crate) use initial_creation::InitialCreation;

mod answers;
mod compact;
mod domain_waiters;
mod requests;

pub use compact::ContextCore;
mod reply;
pub use reply::{ContextAcknowledgement, ContextReplyBody, ContextRequestSettlement};

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

/// A finite original Context-close decision, frozen before its Task prefix is sealed.
/// Normal quiescence leaves the Context's abort cause and protocol epoch unchanged.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskCloseIntent {
    Quiesce,
    Abort(AbortCause),
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EstablishReplayResult {
    AwaitMaterialization,
    CompareCredential { expected: CredentialState },
    Decided(ContextAcknowledgement),
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
    pub request_horizon: crate::lease::RequestHorizon,
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
    /// The original Context owns this exact Root after real encoder exit.
    /// Retained data and read aliases remain funded there; this is not an ACK.
    RootTransferredToContext,
    /// The same real Root has closed issuance, disposed retiring output and
    /// reached complete physical idle. This also releases its output custody.
    RootRetentionReleased,
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
    blocked: Option<OperationOutcome>,
}
#[derive(Debug)]
struct PendingContextUpdate {
    request: InputId,
    payload: PayloadId,
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
    GetFinalInfo {
        task: TaskIdentity,
    },
    GetDynamicFilters {
        task: TaskIdentity,
        acknowledged: Option<DomainVersion>,
    },
    /// Register the original read R while its Task's causal admission is
    /// closed and the original neutral read seal has not yet been published.
    AwaitTaskReadSeal {
        task: TaskIdentity,
        command: TaskReadCommand,
    },
    Subscribe {
        generation: u64,
        position: CoveredSubscriptionRequest,
    },
    Create {
        task: TaskIdentity,
        payload: PayloadId,
    },
    /// Read the original entity/rejection after business intake has closed.
    /// This cannot elect a Task, capture its input, or reserve preparation.
    InspectCreation {
        task: TaskIdentity,
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
            | Self::InspectCreation { task }
            | Self::Cancel { task, .. }
            | Self::UpdateTask { task, .. }
            | Self::GetFinalInfo { task }
            | Self::GetDynamicFilters { task, .. }
            | Self::AwaitTaskReadSeal { task, .. } => Some(*task),
            _ => None,
        }
    }
}
/// A neutral read continuation for an already registered original R. It
/// carries no request admission, Task borrowing or read-seal authority.
#[derive(Clone, Copy, Debug)]
pub enum TaskReadCommand {
    FinalInfo,
    DynamicFilters { acknowledged: Option<DomainVersion> },
}
impl TaskReadCommand {
    pub fn into_command(self, task: TaskIdentity) -> ContextCommand {
        match self {
            Self::FinalInfo => ContextCommand::GetFinalInfo { task },
            Self::DynamicFilters { acknowledged } => {
                ContextCommand::GetDynamicFilters { task, acknowledged }
            }
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
    /// The original never-Accepted Task owner has actually destroyed its
    /// captures and returned its payment. Only the Context self-source emits it.
    RejectedCreationRetired {
        task: TaskIdentity,
        payload: PayloadId,
    },
    /// Transport-only neutral body. `handle_request` binds its original token;
    /// the lifecycle-only `handle` entry rejects this wrapper.
    Request(ContextCommand),
    /// The host's original paid self-source resumes this same waiting R only
    /// after observing its Task's actual neutral read seal.
    ResumeTaskRead {
        request: InputId,
        task: TaskIdentity,
    },
    /// A channel-locked End ACK already belongs to this original R. Its
    /// reply correlation is supplied by the actor, not by another RPC.
    RootEndAcknowledged {
        task: TaskIdentity,
    },
    /// The owning frontend process was fenced. The original lifecycle role
    /// binds the target Context; no RPC token or directory epoch is supplied.
    OwnerFenced,
    /// The driver has consumed this Task's already-admitted summary/cut
    /// predecessors. Source/capture actual exit is a separate obligation.
    ApplyTaskClose {
        task: TaskIdentity,
        generation: Generation,
        intent: TaskCloseIntent,
    },
    ContinueTaskDomain {
        task: TaskIdentity,
    },
    ContinueContextDomain,
    ContextCredentialCompared {
        request: InputId,
        expected: CredentialState,
        result: Result<bool, DomainRefusal>,
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
    /// The same original Loader reports its complete frozen neutral shape and
    /// initial domains. No independent host-side domain authority is created.
    LoadedTask {
        task: TaskIdentity,
        result: Result<CreationCost, AdmissionRefusal>,
        shape: TaskShape,
        initial_domains: Vec<TaskDomainInput>,
    },
    ContinueReplies {
        task: TaskIdentity,
    },
    Installed {
        task: TaskIdentity,
    },
    /// Real queued depths from applying the Core's original initial plan.
    InstalledTask {
        task: TaskIdentity,
        initial_queued: Vec<Option<u64>>,
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
        work: ScopedWorkRef,
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
    /// Actual Task ownership, source/cut reports and captures have retired.
    /// Only the original P06 custody owner may publish this neutral fact.
    TaskExecutionCustodyRetired {
        task: TaskIdentity,
        payload: PayloadId,
        close_epoch: Generation,
    },
    /// Actual execution custody is gone. Visible terminal state or an RPC
    /// deadline cannot supply this independent physical handoff.
    ExecutionCustodyRetired {
        context: QueryContextRef,
        close_epoch: Generation,
        origin: crate::retention::RetentionOrigin,
    },
    ContinueClose,
    ContinueCompaction,
    Tick,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ContextEffect {
    /// The original Context's own retention horizon elapsed. This is no
    /// physical host-removal proof; Control continues its actual cleanup.
    Expired {
        context: QueryContextRef,
        close_epoch: Generation,
    },
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
        intent: TaskCloseIntent,
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
    PublishContextConvergence {
        /// This terminal decision's protocol epoch, carried into the later
        /// independent observation of actual execution custody retirement.
        close_epoch: Generation,
        receipt: novarocks_execution_contract::QueryContextConvergenceReceipt,
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
    },
    WakeContextDomains,
    RecompareContextCredential {
        request: InputId,
        payload: PayloadId,
        expected: CredentialState,
    },
    DomainRequestCompleted {
        request: InputId,
        payload: PayloadId,
        task: Option<TaskIdentity>,
    },
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
    /// The original measured Create was refused before preparation admission.
    /// Dispose its original loader-owned input on the physical cleanup host.
    DisposeLoadedInput {
        task: TaskIdentity,
        payload: PayloadId,
    },
    EnqueuePreparation {
        task: TaskIdentity,
        payload: PayloadId,
        reservation: ReservationId,
        initial_progressions: Arc<Vec<DomainProgression>>,
        initial_pending: bool,
    },
    /// Complete the original Accepted preparation's neutral initial plan.
    /// This grants no second Work, reservation, or Task acceptance.
    InitialPreparationReady {
        task: TaskIdentity,
        progressions: Arc<Vec<DomainProgression>>,
        refusal: Option<novarocks_execution_contract::DomainConflict>,
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
        work: ScopedWorkRef,
    },
    DisposeSource {
        task: TaskIdentity,
        source: SourceId,
    },
    /// The logical conclusion is sealed. Close actual execution borrowers and
    /// finalize rich local sources before waiting for whole preparation exit.
    /// This grants no physical convergence or capacity-release fact.
    FinalizeTaskExecution {
        task: TaskIdentity,
        /// Frozen by this exact terminal decision; later close cannot grant it.
        normally_stopped: bool,
    },
    StopTask {
        task: TaskIdentity,
        generation: Generation,
    },
    SealRoot {
        context: QueryContextRef,
        generation: Generation,
        reason: novarocks_execution_contract::root_lifetime::RootRetentionClose,
    },
    CloseExternalAdmission {
        context: QueryContextRef,
        generation: Generation,
        /// The original close winner; None is normal Quiesce, not Abort.
        reason: Option<AbortCause>,
    },
    /// The bounded scan emitted exactly this many original Task close cuts.
    /// Physical fencing waits for their actual host handoff, not this hint alone.
    CloseFanoutPrepared {
        context: QueryContextRef,
        generation: Generation,
        reason: Option<AbortCause>,
        targets: usize,
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
        /// The original Task close epoch, independent of a later Context fence.
        close_epoch: Generation,
        /// Frozen entity decision; Retained alone does not prove non-admission.
        never_accepted: bool,
        payload: PayloadId,
        state: TaskState,
        active: ReservationId,
        preparation: Option<ReservationId>,
        convergence: crate::convergence::TaskConvergenceSnapshot,
    },
    RetainContext {
        context: QueryContextRef,
        close_epoch: Generation,
        inventory: crate::retention::RetainedContextInventory,
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
/// An actual Worker lookup refusal before any Task command is admitted.
/// This fact creates no Task, Context, cancellation latch or reply owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskControlRefusal {
    NotOwned,
}
impl TaskControlRefusal {
    pub const fn outcome(self) -> OperationOutcome {
        match self {
            Self::NotOwned => OperationOutcome::InvalidStateOrRequest,
        }
    }
    pub const fn cancel_safe_detail(self) -> &'static str {
        match self {
            Self::NotOwned => "cancel names a task this backend does not own",
        }
    }
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
    pub acknowledgement: Option<ContextAcknowledgement>,
    pub decided_at: MonotonicInstant,
}
#[derive(Clone, Debug)]
struct PendingReply {
    body: Arc<ContextReplyBody>,
    settled_at: MonotonicInstant,
}
#[derive(Debug)]
struct PendingQuiesce {
    request: InputId,
    generation: Generation,
    cursor: Option<TaskIdentity>,
    accepted: Vec<TaskIdentity>,
    scheduled: usize,
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
    scheduled: usize,
}
#[derive(Clone, Copy, Debug)]
enum InternalClass {
    TaskDomain,
    RequestReplies,
    CreationReplies,
    Quiesce,
    Close,
    Maintenance,
    Compaction,
}
impl InternalClass {
    const ALL: [Self; 7] = [
        Self::TaskDomain,
        Self::RequestReplies,
        Self::CreationReplies,
        Self::Quiesce,
        Self::Close,
        Self::Maintenance,
        Self::Compaction,
    ];
    const fn index(self) -> usize {
        self as usize
    }
}
#[derive(Debug)]
struct LiveContextCore {
    identity: QueryContextRef,
    generation: Generation,
    establishment: ContextEstablishment,
    config: ContextConfig,
    state: QueryContextState,
    now: MonotonicInstant,
    lease: InstalledLease,
    tasks: BTreeMap<TaskIdentity, TaskRecord>,
    decided_tasks: BTreeMap<TaskIdentity, crate::retention::DecidedTask>,
    custody_retired: BTreeSet<TaskIdentity>,
    retirement_origin: Option<crate::retention::RetentionOrigin>,
    retention_expired: bool,
    materialization_decided: bool,
    spent: usize,
    preparing: usize,
    unretired_tasks: usize,
    closing: Option<Closing>,
    close_applied: bool,
    close_decision: Option<(ContextAcknowledgement, MonotonicInstant)>,
    scheduled_closes: usize,
    abort_waiters: BTreeSet<InputId>,
    host_releasing: bool,
    release_request: Option<InputId>,
    task_domains: BTreeMap<TaskIdentity, TaskDomains>,
    pending_task_updates: BTreeMap<TaskIdentity, PendingTaskUpdate>,
    context_domains: QueryContextDomains,
    pending_context_update: Option<PendingContextUpdate>,
    task_domain_waiters:
        BTreeMap<TaskIdentity, std::collections::VecDeque<domain_waiters::TaskDomainWaiter>>,
    context_domain_waiters: std::collections::VecDeque<domain_waiters::ContextDomainWaiter>,
    context_domain_comparison: Option<domain_waiters::ContextDomainComparison>,
    prefer_context_domain: bool,
    dynamic_filters: BTreeMap<TaskIdentity, (DomainVersion, PayloadId)>,
    final_info: BTreeMap<TaskIdentity, Arc<FinalTaskInfo>>,
    metrics: BTreeMap<TaskIdentity, TaskResourceFacts>,
    metric_deadlines: BTreeSet<(MonotonicInstant, TaskIdentity)>,
    metric_due: BTreeMap<TaskIdentity, MonotonicInstant>,
    pending_quiesce: Option<PendingQuiesce>,
    quiesce_waiters: BTreeSet<InputId>,
    quiesce_decision: Option<QuiesceDecision>,
    replies: requests::RequestState,
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
impl LiveContextCore {
    /// One slice visits at most one task/domain member/request/catch-up member.
    /// A conclusion cut has at most two candidates and four diagnostics. Its
    /// largest batch is the two-cut Summary input: 3 lease-close effects +
    /// 5 predecessor effects + 5 supplied-summary effects + 1 cut receipt +
    /// 1 record visit = 15. Request-producing inputs have only one such cut;
    /// reply fanout and every variable-size traversal use a continuation.
    pub const MAX_BODY_WORK: usize = 15;
    /// One body output batch plus the actual representation-swap notice.
    /// Class probes need no output slots.
    pub const MAX_STEP_ACTIONS: usize = 15 + 1;
    pub const INTERNAL_CLASSES: usize = InternalClass::ALL.len();
    /// One existing body, each internal-class probe, and the actual swap's
    /// record visit plus its one notice effect. The public wrapper and this
    /// live constructor validate the same complete step budget.
    pub const MAX_STEP_WORK: usize = Self::MAX_BODY_WORK + Self::INTERNAL_CLASSES + 2;

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
            decided_tasks: BTreeMap::new(),
            custody_retired: BTreeSet::new(),
            retirement_origin: None,
            retention_expired: false,
            materialization_decided: false,
            spent: 0,
            preparing: 0,
            unretired_tasks: 0,
            closing: None,
            close_applied: false,
            close_decision: None,
            scheduled_closes: 0,
            abort_waiters: BTreeSet::new(),
            host_releasing: false,
            release_request: None,
            task_domains: BTreeMap::new(),
            pending_task_updates: BTreeMap::new(),
            context_domains: QueryContextDomains::default(),
            pending_context_update: None,
            task_domain_waiters: BTreeMap::new(),
            context_domain_waiters: std::collections::VecDeque::new(),
            context_domain_comparison: None,
            prefer_context_domain: false,
            dynamic_filters: BTreeMap::new(),
            final_info: BTreeMap::new(),
            metrics: BTreeMap::new(),
            metric_deadlines: BTreeSet::new(),
            metric_due: BTreeMap::new(),
            pending_quiesce: None,
            quiesce_waiters: BTreeSet::new(),
            quiesce_decision: None,
            replies: requests::RequestState::default(),
            runnable_task_domains: BTreeSet::new(),
            creation_reply_tasks: BTreeSet::new(),
            internal_cursor: 0,
            domain_cursor: None,
            creation_reply_cursor: None,
        })
    }
    /// Accepted neutral domain facts, never host material or an access right.
    /// The public observer projection excludes Creating and rejected entities.
    pub fn status(&self, task: TaskIdentity) -> Option<&TaskStatus> {
        self.tasks
            .get(&task)
            .filter(|record| record.accepted())
            .map(|record| &record.published)
            .or_else(|| self.decided_tasks.get(&task).map(|record| &record.status))
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
        .chain(self.replies.request_deadlines.first().map(|(at, _)| *at))
        .chain(
            self.retirement_origin
                .filter(|_| !self.retention_expired)
                .map(|origin| origin.expires_at()),
        )
        .min()
    }
    /// Pure readiness, not permission to start a host job or to consume a reply.
    /// Future deadlines and owners awaiting a host return do not cause a wake.
    pub fn has_internal_work(&self) -> bool {
        !self.custody_retired.is_empty()
            || self.context_domain_runnable()
            || !self.runnable_task_domains.is_empty()
            || !self.replies.pending_replies.is_empty()
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
                InternalClass::TaskDomain => {
                    let task = self
                        .domain_cursor
                        .and_then(|cursor| {
                            self.runnable_task_domains
                                .range((Excluded(cursor), Unbounded))
                                .next()
                        })
                        .or_else(|| self.runnable_task_domains.first())
                        .copied();
                    if self.context_domain_runnable()
                        && (self.prefer_context_domain || task.is_none())
                    {
                        self.prefer_context_domain = false;
                        Some(ContextInput::ContinueContextDomain)
                    } else {
                        self.prefer_context_domain = true;
                        task.map(|task| ContextInput::ContinueTaskDomain { task })
                    }
                }
                // Original tokens, not wire operation IDs. Always select the
                // lowest ready token; higher-token churn cannot pass it.
                InternalClass::RequestReplies => self
                    .replies
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
                InternalClass::Compaction => {
                    (!self.custody_retired.is_empty()).then_some(ContextInput::ContinueCompaction)
                }
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
        if self.replies.requests.contains_key(&request.token) {
            return Err(CoreError::WrongIdentity);
        }
        self.now = now;
        let task = command.task();
        let refused_domain = match &command {
            ContextCommand::UpdateTask { task, payload, .. } => {
                Some(ContextEffect::DomainRequestCompleted {
                    request: request.token,
                    payload: *payload,
                    task: Some(*task),
                })
            }
            ContextCommand::UpdateContext { payload, .. } => {
                Some(ContextEffect::DomainRequestCompleted {
                    request: request.token,
                    payload: *payload,
                    task: None,
                })
            }
            _ => None,
        };
        if self
            .retirement_origin
            .is_some_and(|origin| origin.has_expired(now))
        {
            let mut step = ContextStep::default();
            self.expire_retention(&mut step);
            step.requests.push(ContextRequestSettlement::verdict(
                request,
                task,
                RequestResult::Outcome(OperationOutcome::Gone),
            ));
            step.effects.extend(refused_domain);
            step.records_visited = 1;
            return Ok(step);
        }
        if now.has_reached(request.deadline) {
            let body = answers::AnswerView::from_live(self).expired_request(&command);
            return Ok(ContextStep {
                records_visited: 1,
                requests: vec![ContextRequestSettlement::new(request, task, Arc::new(body))],
                effects: refused_domain.into_iter().collect(),
                ..ContextStep::default()
            });
        }
        if self.replies.requests.len() >= self.config.max_pending_requests {
            return Err(CoreError::IllegalFact);
        }
        if let Some(task) = task {
            self.replies.request_tasks.insert(request.token, task);
        }
        self.replies.requests.insert(request.token, request);
        self.replies
            .request_deadlines
            .insert((request.deadline, request.token));
        self.handle_bound(
            now,
            ContextInput::Request(command),
            Some(request.token),
            None,
            capacity,
        )
    }
    pub fn handle(
        &mut self,
        now: MonotonicInstant,
        input: ContextInput,
        capacity: &mut impl AdmissionPort,
    ) -> Result<ContextStep, CoreError> {
        if matches!(
            input,
            ContextInput::Request(_) | ContextInput::RootEndAcknowledged { .. }
        ) {
            return Err(CoreError::WrongIdentity);
        }
        self.handle_bound(now, input, None, None, capacity)
    }
    /// Commit an already accepted channel fact through its original input.
    /// The data wait deadline cannot revoke an ACK; it is not business
    /// request admission. Common consumes this exact input once, and this
    /// step returns its one correlated reply after the actual publications.
    pub fn handle_root_end_acknowledged(
        &mut self,
        now: MonotonicInstant,
        original: RequestContext,
        task: TaskIdentity,
        capacity: &mut impl AdmissionPort,
    ) -> Result<ContextStep, CoreError> {
        if self.replies.requests.contains_key(&original.token) {
            return Err(CoreError::WrongIdentity);
        }
        self.handle_bound(
            now,
            ContextInput::RootEndAcknowledged { task },
            None,
            Some(original),
            capacity,
        )
    }
    fn handle_bound(
        &mut self,
        now: MonotonicInstant,
        input: ContextInput,
        request_token: Option<InputId>,
        root_ack_request: Option<RequestContext>,
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
            ContextInput::ResumeTaskRead { request, task } => {
                if let Some((original, command)) = self.replies.take_task_read(request, task) {
                    if self
                        .retirement_origin
                        .is_some_and(|origin| origin.has_expired(now))
                    {
                        self.complete_request(
                            request,
                            Arc::new(ContextReplyBody::Verdict(RequestResult::Outcome(
                                OperationOutcome::Gone,
                            ))),
                            &mut step,
                        );
                    } else if now.has_reached(original.deadline) {
                        self.complete_request(
                            request,
                            Arc::new(ContextReplyBody::Verdict(
                                RequestResult::TimeoutWhileAccepted,
                            )),
                            &mut step,
                        );
                    } else {
                        self.handle_command(
                            now,
                            request,
                            command.into_command(task),
                            capacity,
                            &mut step,
                        )?;
                    }
                }
            }
            ContextInput::RootEndAcknowledged { task } => {
                let original = root_ack_request.ok_or(CoreError::WrongIdentity)?;
                let body = self.root_end_acknowledged(task, &mut step)?;
                // No request_deadlines entry: the owner fact already happened.
                // This is the same bounded reply correlation, not another R.
                step.requests.push(ContextRequestSettlement::new(
                    original,
                    Some(task),
                    Arc::new(body),
                ));
                step.records_visited += 1;
            }
            ContextInput::RejectedCreationRetired { task, payload } => {
                self.begin_rejected_creation_retirement(task, payload, &mut step)?;
            }
            ContextInput::OwnerFenced => {
                self.begin_abort(None, AbortCause::QueryFailed, &mut step)?;
            }
            ContextInput::ApplyTaskClose {
                task,
                generation,
                intent,
            } => {
                let record = self.tasks.get(&task).ok_or(CoreError::UnknownTask)?;
                match intent {
                    TaskCloseIntent::Quiesce => {
                        if record.scheduled_quiesce_generation != Some(generation)
                            && record.applied_quiesce_generation != Some(generation)
                        {
                            return Err(CoreError::IllegalFact);
                        }
                    }
                    TaskCloseIntent::Abort(cause) => {
                        if generation != self.generation
                            || self.closing.is_none_or(|closing| closing.cause != cause)
                        {
                            return Err(CoreError::WrongIdentity);
                        }
                        if record.scheduled_close_generation != Some(generation)
                            && record.applied_close_generation != generation
                        {
                            return Err(CoreError::IllegalFact);
                        }
                    }
                }
                self.apply_close_to(task, generation, intent, &mut step)?;
                self.try_finish_completion_intent(task, &mut step)?;
                // Both original close inputs remain independent retirement gates.
                self.retire_if_ready(task, &mut step)?;
            }
            ContextInput::ContinueTaskDomain { task } => {
                self.advance_task_domain(task, capacity, &mut step)?
            }
            ContextInput::ContinueContextDomain => self.advance_context_domain_waiter(&mut step)?,
            ContextInput::ContextCredentialCompared {
                request,
                expected,
                result,
            } => {
                self.context_credential_compared(request, expected, result, &mut step)?;
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
                if record.phase != CreationPhase::Loading
                    || self.task_domains.contains_key(&task)
                    || record.initial_creation.is_some()
                {
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
                        let outcome = if pending.progression == DomainProgression::Apply {
                            OperationOutcome::Accepted
                        } else {
                            OperationOutcome::Idempotent
                        };
                        let receipt = self
                            .context_domains
                            .receipt(pending.update, pending.progression);
                        let mut acknowledgement = self.context_acknowledgement(outcome);
                        acknowledgement.receipt = Arc::new(
                            acknowledgement
                                .receipt
                                .as_ref()
                                .clone()
                                .with_domains(vec![receipt.clone()]),
                        );
                        step.reply(
                            request,
                            ContextReplyBody::ContextDomain {
                                outcome,
                                receipt,
                                acknowledgement,
                            },
                        );
                    }
                    Err(failure) => {
                        // A host refusal cannot prove that its shared-domain
                        // call had no external prefix. Fence this Context
                        // before returning the original refusal; never keep
                        // using a host whose state may have advanced.
                        self.begin_abort(None, AbortCause::QueryFailed, &mut step)?;
                        step.reply(request, ContextReplyBody::Domain(failure));
                    }
                }
                step.effects.push(ContextEffect::DomainRequestCompleted {
                    request,
                    payload: pending.payload,
                    task: None,
                });
                step.effects.push(ContextEffect::WakeContextDomains);
            }
            ContextInput::DynamicFilterPublished {
                task,
                version,
                payload,
                domain_count,
            } => {
                self.flush(task, &mut step)?;
                let record = self.tasks.get_mut(&task).ok_or(CoreError::UnknownTask)?;
                if !record.state.is_terminal() {
                    if !record.accepted() {
                        return Err(CoreError::IllegalFact);
                    }
                    if self
                        .dynamic_filters
                        .get(&task)
                        .is_some_and(|(accepted, _)| *accepted > version)
                    {
                        return Err(CoreError::IllegalFact);
                    }
                    self.dynamic_filters.insert(task, (version, payload));
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
                    if !record.accepted() {
                        return Err(CoreError::IllegalFact);
                    }
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
                if !self.tasks[&task].accepted() {
                    return Err(CoreError::IllegalFact);
                }
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
                            self.metric_deadlines.remove(&(*due, task));
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
                self.materialization_decided = true;
                step.effects.push(ContextEffect::EstablishmentDecided {
                    context: self.identity,
                    close_epoch: self.generation,
                    decision: ContextEstablishmentDecision {
                        state: self.state,
                        outcome: Ok(self.closed_outcome()),
                        acknowledgement: Some(
                            self.establishment_acknowledgement(self.closed_outcome()),
                        ),
                        decided_at: self.now,
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
                self.materialization_decided = true;
                step.effects.push(ContextEffect::EstablishmentDecided {
                    context: self.identity,
                    close_epoch: self.generation,
                    decision: ContextEstablishmentDecision {
                        state: self.state,
                        acknowledgement: outcome
                            .as_ref()
                            .ok()
                            .map(|outcome| self.establishment_acknowledgement(*outcome)),
                        outcome,
                        decided_at: self.now,
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
                if self
                    .tasks
                    .get(&task)
                    .is_some_and(|record| record.initial_creation.is_some())
                {
                    return Err(CoreError::IllegalFact);
                }
                self.loaded(task, result, Arc::new(Vec::new()), capacity, &mut step)?
            }
            ContextInput::LoadedTask {
                task,
                result,
                shape,
                initial_domains,
            } => {
                self.loaded_task(task, result, shape, initial_domains, capacity, &mut step)?;
            }
            ContextInput::ContinueReplies { task } => {
                self.reply_creation(task, &mut step)?;
                self.retire_if_ready(task, &mut step)?;
            }
            ContextInput::Installed { task } => {
                if self
                    .tasks
                    .get(&task)
                    .is_some_and(|record| record.initial_creation.is_some())
                {
                    return Err(CoreError::IllegalFact);
                }
                self.validate_installation(task, &mut step)?;
                self.publish_installation(task, &mut step)?;
            }
            ContextInput::InstalledTask {
                task,
                initial_queued,
            } => {
                self.installed_task(task, initial_queued, &mut step)?;
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
                    PhysicalFact::RootTransferredToContext => {
                        t.root_transferred = true;
                        t.convergence
                            .note_output_transferred()
                            .map_err(|_| CoreError::IllegalFact)?;
                    }
                    PhysicalFact::RootRetentionReleased => {
                        t.root_idle = true;
                        t.convergence
                            .note_output_released()
                            .map_err(|_| CoreError::IllegalFact)?;
                        t.output_released = true;
                    }
                }
                Self::apply_runtime_exit_receipts(t, before, &mut step)?;
                self.resume_initial_after_preparation_exit(task, &mut step)?;
                self.try_finish_completion_intent(task, &mut step)?;
                self.retire_if_ready(task, &mut step)?;
            }
            ContextInput::TaskExecutionCustodyRetired {
                task,
                payload,
                close_epoch,
            } => {
                self.note_task_custody_retired(task, payload, close_epoch, &mut step)?;
            }
            ContextInput::ExecutionCustodyRetired {
                context,
                close_epoch,
                origin,
            } => {
                if context != self.identity
                    || close_epoch != self.generation
                    || origin.retired_at() > now
                {
                    return Err(CoreError::WrongIdentity);
                }
                if self.state != QueryContextState::TerminalRetained
                    || !self.materialization_decided
                {
                    return Err(CoreError::IllegalFact);
                }
                if origin
                    != crate::retention::RetentionOrigin::new(
                        origin.retired_at(),
                        self.config.request_horizon,
                    )
                {
                    return Err(CoreError::IllegalFact);
                }
                if self
                    .retirement_origin
                    .is_some_and(|original| original != origin)
                {
                    return Err(CoreError::IllegalFact);
                }
                self.retirement_origin = Some(origin);
            }
            ContextInput::ContinueCompaction => self.advance_compaction(&mut step)?,
            ContextInput::HostReleased => {
                if !self.host_releasing || self.unretired_tasks != 0 {
                    return Err(CoreError::IllegalFact);
                }
                self.host_releasing = false;
                self.state = QueryContextState::TerminalRetained;
                if let Some(request) = self.release_request.take() {
                    step.reply(
                        request,
                        self.release_reply(novarocks_execution_contract::ReleaseOutcome::Released),
                    );
                }
                // The original HostReleased fact follows actual Task/host
                // retirement and the same Context admission fence.
                step.effects.push(ContextEffect::PublishContextConvergence {
                    close_epoch: self.generation,
                    receipt: novarocks_execution_contract::QueryContextConvergenceReceipt::new(
                        self.identity,
                        novarocks_execution_contract::QueryContextConvergenceVersion::FIRST,
                        novarocks_execution_contract::QueryContextConvergenceState::WorkerStoppedAndContextFenced,
                    ),
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
            && self.pending_context_update.is_none()
            && self.context_domain_comparison.is_none()
            && self.context_domain_waiters.is_empty()
        {
            self.host_releasing = true;
            step.effects.push(ContextEffect::ReleaseHost {
                context: self.identity,
                generation: self.generation,
            });
        }
        self.expire_retention(&mut step);
        let decisions_visited = self.settle_request_decisions(&mut step);
        step.records_visited +=
            1 + decisions_visited + usize::from(!step.requests.is_empty()) + internal_probes;
        Ok(step)
    }
    fn handle_command(
        &mut self,
        now: MonotonicInstant,
        request: InputId,
        mut command: ContextCommand,
        capacity: &mut impl AdmissionPort,
        step: &mut ContextStep,
    ) -> Result<(), CoreError> {
        // Waiting is not a business read, including after Task/Context
        // compaction. Only a validated continuation may select that read.
        if let ContextCommand::AwaitTaskReadSeal { task, command } = command {
            if let Some(outcome) = answers::AnswerView::from_live(self).task_read_refusal(task)? {
                step.reply(
                    request,
                    ContextReplyBody::Verdict(RequestResult::Outcome(outcome)),
                );
            } else {
                self.replies.await_task_read(request, command)?;
            }
            return Ok(());
        }
        if self.state == QueryContextState::TerminalRetained {
            if let ContextCommand::Cancel { task, .. }
            | ContextCommand::GetFinalInfo { task }
            | ContextCommand::GetDynamicFilters { task, .. } = &command
                && self.tasks.contains_key(task)
            {
                if !self.task_request_cut(request, *task, step)? {
                    return Ok(());
                }
                self.try_finish_completion_intent(*task, step)?;
            }
            answers::AnswerView::from_live(self)
                .terminal_command(request, &mut command)?
                .emit(request, step);
            return Ok(());
        }
        if let Some(answer) =
            answers::AnswerView::from_live(self).decided_task_command(request, &command)?
        {
            answer.emit(request, step);
            return Ok(());
        }
        match command {
            ContextCommand::AwaitTaskReadSeal { .. } => {
                unreachable!("registered before read dispatch")
            }
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
                        ContextReplyBody::Task {
                            outcome: OperationOutcome::Accepted,
                            status: Arc::new(Self::compose(&self.tasks[&task])?),
                        },
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
                    // Intake commits only this original R. Catch-up coverage
                    // and cursor validation belong to ObservationSource.
                    step.reply(
                        request,
                        ContextReplyBody::Verdict(RequestResult::Outcome(
                            OperationOutcome::Accepted,
                        )),
                    );
                }
            }
            ContextCommand::Create { task, payload } => {
                self.create(request, task, payload, capacity, step)?
            }
            ContextCommand::InspectCreation { task } => {
                self.inspect_creation(request, task, step)?
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
                    ContextReplyBody::Context(self.context_acknowledgement(outcome)),
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
                            reason: None,
                        });
                    }
                    self.pending_quiesce = Some(PendingQuiesce {
                        request,
                        generation: self.generation,
                        cursor: None,
                        accepted: Vec::new(),
                        scheduled: 0,
                    });
                    step.effects.push(ContextEffect::ContinueQuiesce);
                }
            }
            ContextCommand::Abort { cause } => self.begin_abort(Some(request), cause, step)?,
            ContextCommand::Release => {
                if self.state != QueryContextState::Quiescing
                    || self.closing.is_some()
                    || self.quiesce_decision.is_none()
                    || self.pending_quiesce.is_some()
                    || self.unretired_tasks != 0
                    || !self.pending_task_updates.is_empty()
                    || self.pending_context_update.is_some()
                    || self.context_domain_comparison.is_some()
                    || !self.context_domain_waiters.is_empty()
                {
                    step.reply(
                        request,
                        self.release_reply(novarocks_execution_contract::ReleaseOutcome::NotReady),
                    );
                } else {
                    self.state = QueryContextState::Releasing;
                    self.release_request = Some(request);
                    self.host_releasing = true;
                    step.effects.push(ContextEffect::SealRoot {
                        context: self.identity,
                        generation: self.generation,
                        reason: novarocks_execution_contract::root_lifetime::RootRetentionClose::ContextReleased,
                    });
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
        task.compose_status()
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
    fn context_acknowledgement(&self, outcome: OperationOutcome) -> ContextAcknowledgement {
        answers::AnswerView::from_live(self).acknowledgement(outcome)
    }
    fn establishment_acknowledgement(&self, outcome: OperationOutcome) -> ContextAcknowledgement {
        answers::AnswerView::from_live(self).establishment_acknowledgement(outcome)
    }
    fn release_reply(
        &self,
        release: novarocks_execution_contract::ReleaseOutcome,
    ) -> ContextReplyBody {
        answers::AnswerView::from_live(self).release(release)
    }
    fn closed_outcome(&self) -> OperationOutcome {
        answers::AnswerView::from_live(self).closed_outcome()
    }
    /// Preserve Create's exact identity/entity priority for the business and
    /// read-only entries. A closed Context still owes existing Task replays.
    fn replay_creation(
        &mut self,
        request: InputId,
        task: TaskIdentity,
        step: &mut ContextStep,
    ) -> Result<bool, CoreError> {
        if task.verify_query_context(self.identity).is_err() {
            step.reply(
                request,
                ContextReplyBody::Verdict(RequestResult::Outcome(
                    OperationOutcome::IdentityMismatch,
                )),
            );
            return Ok(true);
        }
        if let Some(t) = self.tasks.get_mut(&task) {
            if t.phase == CreationPhase::Loading {
                if t.waiting_replies.contains(&request) {
                    return Ok(true);
                }
                if t.waiting_replies.len() >= self.config.max_creation_waiters {
                    return Err(CoreError::IllegalFact);
                }
                t.waiting_replies.insert(request);
            } else if let Some(refusal) = &t.creation_refusal {
                Self::refusal_reply(request, refusal.clone(), step);
            } else {
                let body = if t
                    .pending_reply_outcome
                    .is_some_and(|outcome| outcome != OperationOutcome::Accepted)
                {
                    Self::creation_reply_body(t)
                } else {
                    ContextReplyBody::Create {
                        outcome: OperationOutcome::Idempotent,
                        receipt: Arc::new(
                            novarocks_execution_contract::CreateTaskReceipt::from_shared(
                                t.identity,
                                t.initial_domain_receipts.clone(),
                                Self::compose(t)?,
                            ),
                        ),
                    }
                };
                step.reply(request, body);
            }
            return Ok(true);
        }
        Ok(false)
    }
    fn inspect_creation(
        &mut self,
        request: InputId,
        task: TaskIdentity,
        step: &mut ContextStep,
    ) -> Result<(), CoreError> {
        if self.replay_creation(request, task, step)? {
            return Ok(());
        }
        let outcome = if matches!(
            self.state,
            QueryContextState::Active | QueryContextState::Establishing
        ) {
            OperationOutcome::NotReady
        } else {
            self.closed_outcome()
        };
        step.reply(
            request,
            ContextReplyBody::Verdict(RequestResult::Outcome(outcome)),
        );
        Ok(())
    }
    fn create(
        &mut self,
        request: InputId,
        task: TaskIdentity,
        payload: PayloadId,
        capacity: &mut impl AdmissionPort,
        step: &mut ContextStep,
    ) -> Result<(), CoreError> {
        if self.replay_creation(request, task, step)? {
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
        initial_progressions: Arc<Vec<DomainProgression>>,
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
                .ok_or(AdmissionRefusal::Verdict(
                    OperationOutcome::ResourceExhausted,
                ))
        });
        let preparing = self.preparing;
        let reserved = if self.state != QueryContextState::Active {
            Err(AdmissionRefusal::Verdict(self.closed_outcome()))
        } else {
            cost.and_then(|bytes| {
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
                // The decision survives RPC timeout/removal. Its original
                // loader child still owns opaque input and must really Drop
                // before the independent LoaderExited author can publish.
                step.effects.push(ContextEffect::DisposeLoadedInput {
                    task,
                    payload: t.payload,
                });
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
            // The same batch transfers its actual Accepted reservation.
            // Initial-domain classification may finish later; the original
            // queued holder parks until that neutral plan is ready.
            step.effects.push(ContextEffect::EnqueuePreparation {
                task,
                payload: t.payload,
                reservation: t
                    .preparation_reservation
                    .expect("accepted preparation reservation"),
                initial_progressions,
                initial_pending: t.initial_creation.is_some(),
            });
        }
        t.creation_reply = Some(Arc::new(match &t.creation_refusal {
            Some(AdmissionRefusal::Domain(outcome, failure)) => {
                ContextReplyBody::Domain(DomainRefusal {
                    outcome: *outcome,
                    failure: failure.clone(),
                })
            }
            _ if outcome == OperationOutcome::Accepted => ContextReplyBody::Create {
                outcome,
                receipt: Arc::new(
                    novarocks_execution_contract::CreateTaskReceipt::from_shared(
                        t.identity,
                        t.initial_domain_receipts.clone(),
                        Self::compose(t)?,
                    ),
                ),
            },
            _ => ContextReplyBody::Verdict(RequestResult::Outcome(outcome)),
        }));
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
        task.creation_reply
            .as_deref()
            .expect("original complete entity Create decision")
            .clone()
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
            && !self.task_domain_waiters.contains_key(&task)
            && t.initial_creation.is_none()
            && t.scheduled_close_generation.is_none()
            && t.scheduled_quiesce_generation.is_none()
        {
            t.convergence.retire().map_err(|_| CoreError::IllegalFact)?;
            t.phase = CreationPhase::Retained;
            self.unretired_tasks -= 1;
            step.effects.push(ContextEffect::RetireTask {
                task,
                close_epoch: t.applied_close_generation,
                never_accepted: !t.accepted(),
                payload: t.payload,
                state: t.state,
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
        let normal_cancellation = matches!(
            accepted,
            TaskConclusion::Terminated(TerminationDetail::Canceled(_))
        );
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
            let normally_stopped = record.installed
                && record.normal_close_validated
                && self.closing.is_none()
                && matches!(
                    self.state,
                    QueryContextState::Active
                        | QueryContextState::Quiescing
                        | QueryContextState::Releasing
                )
                && (record.state == TaskState::Finished
                    || (record.state == TaskState::Canceled && normal_cancellation));
            Self::publish(record, step)?;
            step.effects.push(ContextEffect::FinalizeTaskExecution {
                task,
                normally_stopped,
            });
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
                if !t.stop_effect_issued {
                    t.stop_effect_issued = true;
                    step.effects.push(ContextEffect::StopTask {
                        task,
                        generation: self.generation,
                    });
                }
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
                    ContextReplyBody::Context(
                        self.context_acknowledgement(OperationOutcome::ContextTerminalReceipt),
                    ),
                );
            }
            return Ok(());
        }
        if let Some(closing) = self.closing {
            if let Some(request) = request {
                if self.close_applied {
                    step.reply(
                        request,
                        ContextReplyBody::Context(
                            self.context_acknowledgement(OperationOutcome::Idempotent),
                        ),
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
            scheduled: 0,
        });
        self.close_applied = false;
        step.effects.push(ContextEffect::CloseExternalAdmission {
            context: self.identity,
            generation: self.generation,
            reason: Some(cause),
        });
        step.effects.push(ContextEffect::SealRoot {
            context: self.identity,
            generation: self.generation,
            reason: if cause == AbortCause::LeaseExpired {
                novarocks_execution_contract::root_lifetime::RootRetentionClose::LeaseExpired
            } else {
                novarocks_execution_contract::root_lifetime::RootRetentionClose::ContextAborted
            },
        });
        step.effects.push(ContextEffect::ContinueClose {
            generation: self.generation,
        });
        Ok(())
    }
    fn apply_close_to(
        &mut self,
        task: TaskIdentity,
        generation: Generation,
        intent: TaskCloseIntent,
        step: &mut ContextStep,
    ) -> Result<(), CoreError> {
        let t = self.tasks.get_mut(&task).ok_or(CoreError::UnknownTask)?;
        let conclusion = match intent {
            TaskCloseIntent::Quiesce => {
                if t.applied_quiesce_generation == Some(generation) {
                    return Ok(());
                }
                if t.scheduled_quiesce_generation.take() != Some(generation) {
                    return Err(CoreError::IllegalFact);
                }
                t.applied_quiesce_generation = Some(generation);
                TaskConclusion::Terminated(TerminationDetail::Canceled(
                    CancelReason::UpstreamNoLongerNeeded,
                ))
            }
            TaskCloseIntent::Abort(cause) => {
                if let Some(scheduled) = t.scheduled_close_generation.take() {
                    if scheduled != generation {
                        return Err(CoreError::WrongIdentity);
                    }
                    self.scheduled_closes = self
                        .scheduled_closes
                        .checked_sub(1)
                        .ok_or(CoreError::IllegalFact)?;
                }
                if t.applied_close_generation == generation {
                    self.finish_close_if_ready(step);
                    return Ok(());
                }
                t.applied_close_generation = generation;
                TaskConclusion::Terminated(TerminationDetail::Aborted(cause))
            }
        };
        if !t.accepted() {
            // Unaccepted loading owns no public Task conclusion or runtime exit.
            if !t.loader_exited {
                step.effects
                    .push(ContextEffect::CancelLoader { task, generation });
            }
        } else {
            let proposal = Proposal {
                conclusion,
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
        let request = closing.request.take();
        let acknowledgement = self.context_acknowledgement(OperationOutcome::Accepted);
        self.close_decision = Some((acknowledgement.clone(), self.now));
        if let Some(request) = request {
            self.complete_request_at(
                request,
                Arc::new(ContextReplyBody::Context(acknowledgement)),
                self.now,
                step,
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
            if (record.state.is_terminal()
                || (record.phase == CreationPhase::Retained
                    && record.convergence.snapshot().retired()))
                && record.scheduled_close_generation.is_none()
            {
                // Its conclusion is already sealed. The Context fence closes
                // new business, while original Task summaries and cleanup keep
                // their actual causal generation until consumed. Do not revive
                // an exited Task Work or pretend a new close prefix applied.
                // Context-retained Root closure is its separate host duty.
            } else if record.applied_close_generation != self.generation
                && record.scheduled_close_generation.is_none()
            {
                record.scheduled_close_generation = Some(self.generation);
                self.scheduled_closes += 1;
                closing.scheduled += 1;
                step.effects.push(ContextEffect::ApplyTaskClose {
                    task: *task,
                    generation: self.generation,
                    intent: TaskCloseIntent::Abort(closing.cause),
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
        if !more {
            step.effects.push(ContextEffect::CloseFanoutPrepared {
                context: self.identity,
                generation: self.generation,
                reason: Some(closing.cause),
                targets: closing.scheduled,
            });
        }
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
            Some(TaskControlRefusal::NotOwned.outcome())
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
        if !self.tasks[&task].accepted() {
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
            step.effects.push(ContextEffect::DomainRequestCompleted {
                request,
                payload,
                task: Some(task),
            });
            return Ok(());
        }
        if !self.task_request_cut(request, task, step)? {
            step.effects.push(ContextEffect::DomainRequestCompleted {
                request,
                payload,
                task: Some(task),
            });
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
            step.effects.push(ContextEffect::DomainRequestCompleted {
                request,
                payload,
                task: Some(task),
            });
            return Ok(());
        }
        if self.pending_task_updates.contains_key(&task)
            || self.tasks[&task].initial_creation.is_some()
            || self.task_domain_waiters.contains_key(&task)
        {
            self.task_domain_waiters.entry(task).or_default().push_back(
                domain_waiters::TaskDomainWaiter::Request {
                    request,
                    payload,
                    updates,
                },
            );
            step.effects.push(ContextEffect::WaitTaskDomain {
                request,
                task,
                payload,
            });
            self.refresh_task_domain_waiter(task);
            return Ok(());
        }
        self.start_task_domain(request, task, payload, updates, step)
    }
    fn start_task_domain(
        &mut self,
        request: InputId,
        task: TaskIdentity,
        payload: PayloadId,
        updates: Vec<TaskDomainInput>,
        step: &mut ContextStep,
    ) -> Result<(), CoreError> {
        let Some(domains) = self.task_domains.remove(&task) else {
            step.reply(
                request,
                ContextReplyBody::Verdict(RequestResult::Outcome(
                    OperationOutcome::InvalidStateOrRequest,
                )),
            );
            step.effects.push(ContextEffect::DomainRequestCompleted {
                request,
                payload,
                task: Some(task),
            });
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
                blocked: None,
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
        capacity: &mut impl AdmissionPort,
        step: &mut ContextStep,
    ) -> Result<(), CoreError> {
        if self
            .tasks
            .get(&task)
            .is_some_and(|record| record.rejected_retirement_observed)
        {
            return self.advance_rejected_creation_retirement(task, step);
        }
        if self
            .tasks
            .get(&task)
            .is_some_and(|record| record.initial_creation.is_some())
        {
            self.advance_initial_creation(task, capacity, step)?;
            self.refresh_task_domain_waiter(task);
            return Ok(());
        }
        if !self.pending_task_updates.contains_key(&task) {
            return self.advance_task_domain_waiter(task, step);
        }
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
        if !pending.host_started
            && pending.blocked.is_none()
            && (!matches!(
                self.state,
                QueryContextState::Active | QueryContextState::Quiescing
            ) || self.tasks[&task].state.is_terminal()
                || self.tasks[&task].conclusion.accepted.is_some())
        {
            pending.blocked = Some(
                if self.tasks[&task].state.is_terminal()
                    || self.tasks[&task].conclusion.accepted.is_some()
                {
                    OperationOutcome::TerminalRejected
                } else {
                    self.closed_outcome()
                },
            );
            pending
                .transaction
                .rollback(novarocks_execution_contract::DomainConflict::NotMonotonic);
        }
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
                let body = match pending.blocked {
                    Some(outcome) => ContextReplyBody::Verdict(RequestResult::Outcome(outcome)),
                    None => ContextReplyBody::Domain(
                        pending
                            .refusal
                            .unwrap_or_else(|| DomainRefusal::policy(conflict)),
                    ),
                };
                step.reply(pending.request, body);
                self.complete_task_domain_source(task, pending.request, pending.payload);
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
                self.complete_task_domain_source(task, pending.request, pending.payload);
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
        if self
            .retirement_origin
            .is_some_and(|origin| origin.has_expired(self.now))
        {
            return Ok(EstablishReplayResult::Decided(
                self.establishment_acknowledgement(OperationOutcome::Gone),
            ));
        }
        match self.state {
            QueryContextState::Quiescing | QueryContextState::TerminalRetained => {
                return Ok(EstablishReplayResult::Decided(
                    self.establishment_acknowledgement(OperationOutcome::ContextTerminalReceipt),
                ));
            }
            QueryContextState::Aborting | QueryContextState::Releasing => {
                return Ok(EstablishReplayResult::Decided(
                    self.establishment_acknowledgement(OperationOutcome::InvalidStateOrRequest),
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
                self.establishment_acknowledgement(OperationOutcome::ContextConflict),
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
            self.establishment_acknowledgement(
                if candidate.semantics.credential_lease == current.lease
                    && candidate.semantics.credential_epoch == current.epoch
                    && comparison.same_material
                {
                    OperationOutcome::Idempotent
                } else {
                    OperationOutcome::ContextConflict
                },
            ),
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
            step.effects.push(ContextEffect::DomainRequestCompleted {
                request,
                payload,
                task: None,
            });
            return Ok(());
        }
        if self.pending_context_update.is_some()
            || self.context_domain_comparison.is_some()
            || !self.context_domain_waiters.is_empty()
        {
            self.context_domain_waiters
                .push_back(domain_waiters::ContextDomainWaiter {
                    request,
                    payload,
                    update,
                });
            step.effects
                .push(ContextEffect::WaitContextDomain { request, payload });
            return Ok(());
        }
        self.start_context_domain(request, payload, update, step)
    }
    fn start_context_domain(
        &mut self,
        request: InputId,
        payload: PayloadId,
        update: ContextDomainInput,
        step: &mut ContextStep,
    ) -> Result<(), CoreError> {
        if self.recompare_context_credential(request, payload, update, step)? {
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
                step.effects.push(ContextEffect::DomainRequestCompleted {
                    request,
                    payload,
                    task: None,
                });
                return Ok(());
            }
        };
        self.pending_context_update = Some(PendingContextUpdate {
            request,
            payload,
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
    fn root_end_acknowledged(
        &mut self,
        task: TaskIdentity,
        step: &mut ContextStep,
    ) -> Result<ContextReplyBody, CoreError> {
        if task.verify_query_context(self.identity).is_err() {
            return Ok(ContextReplyBody::Verdict(RequestResult::Outcome(
                OperationOutcome::IdentityMismatch,
            )));
        }
        if let Some(record) = self.decided_tasks.get_mut(&task) {
            record.output = TaskOutputFacts::new(true);
            return Ok(ContextReplyBody::RootDrain(
                RootDrainVerdict::AlreadyTerminal(record.status.state()),
            ));
        }
        if !self.tasks.contains_key(&task) {
            return Ok(ContextReplyBody::Verdict(RequestResult::Outcome(
                OperationOutcome::InvalidStateOrRequest,
            )));
        }
        self.flush(task, step)?;
        if !self.tasks[&task].accepted() {
            return Ok(ContextReplyBody::Verdict(RequestResult::Outcome(
                OperationOutcome::InvalidStateOrRequest,
            )));
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
        self.retire_if_ready(task, step)?;
        Ok(ContextReplyBody::RootDrain(result))
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
        if let Some(identity) = self.next_task_identity(pending.cursor) {
            pending.cursor = Some(identity);
            if let Some(record) = self.tasks.get_mut(&identity) {
                if record.pending_reply_outcome == Some(OperationOutcome::Accepted) {
                    pending.accepted.push(identity);
                    if self.closing.is_none()
                        && !record.state.is_terminal()
                        && record.phase != CreationPhase::Retained
                        && record.scheduled_quiesce_generation.is_none()
                        && record.applied_quiesce_generation.is_none()
                    {
                        record.scheduled_quiesce_generation = Some(self.generation);
                        pending.scheduled += 1;
                        step.effects.push(ContextEffect::ApplyTaskClose {
                            task: identity,
                            generation: self.generation,
                            intent: TaskCloseIntent::Quiesce,
                        });
                    }
                }
            } else {
                // Actual exit removes execution state, never accepted membership.
                pending.accepted.push(identity);
            }
        }
        let more = pending
            .cursor
            .is_some_and(|cursor| self.next_task_identity(Some(cursor)).is_some());
        if more {
            self.pending_quiesce = Some(pending);
            step.effects.push(ContextEffect::ContinueQuiesce);
        } else {
            step.effects.push(ContextEffect::CloseFanoutPrepared {
                context: self.identity,
                generation: pending.generation,
                reason: None,
                targets: pending.scheduled,
            });
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
        let before = step.requests.len();
        self.replies.complete(token, body, settled_at, step);
        self.cleanup_emitted_request(before, step);
    }
    fn cleanup_emitted_request(&mut self, before: usize, step: &ContextStep) {
        if let Some(reply) = step.requests.get(before) {
            self.cleanup_request_membership(reply.request.token, reply.task);
        }
    }
    fn cleanup_request_membership(&mut self, token: InputId, task: Option<TaskIdentity>) {
        self.abort_waiters.remove(&token);
        self.quiesce_waiters.remove(&token);
        if let Some(task) = task
            && let Some(record) = self.tasks.get_mut(&task)
        {
            record.waiting_replies.remove(&token);
            if record.waiting_replies.is_empty() {
                self.creation_reply_tasks.remove(&task);
            }
        }
    }
    fn flush_request_replies(&mut self, token: InputId, step: &mut ContextStep) {
        let before = step.requests.len();
        self.replies.flush(token, step);
        self.cleanup_emitted_request(before, step);
    }
    fn expire_requests(&mut self, step: &mut ContextStep) {
        let Some((at, token)) = self.replies.request_deadlines.first().copied() else {
            return;
        };
        if self.now < at {
            return;
        }
        // A complete body decided before the deadline remains its original
        // fact even if bounded delivery reaches this token on a later turn.
        if self
            .replies
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
        let creation = self.replies.request_tasks.get(&token).and_then(|task| {
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
        let aborted = || {
            if !self.abort_waiters.contains(&token) {
                return None;
            }
            let (acknowledgement, decided_at) = self
                .close_decision
                .as_ref()
                .filter(|(_, decided_at)| *decided_at < at)?;
            let mut acknowledgement = acknowledgement.clone();
            acknowledgement.outcome = OperationOutcome::Idempotent;
            Some((ContextReplyBody::Context(acknowledgement), *decided_at))
        };
        if let Some((body, decided_at)) = creation.or_else(quiesce).or_else(aborted) {
            self.complete_request_at(token, Arc::new(body), decided_at, step);
            return;
        }
        let before = step.requests.len();
        self.replies.expire(self.now, step);
        self.cleanup_emitted_request(before, step);
    }
    fn settle_request_decisions(&mut self, step: &mut ContextStep) -> usize {
        let decisions = std::mem::take(&mut step.decisions);
        let visited = decisions.len();
        for (token, body) in decisions {
            self.complete_request(token, body, step);
        }
        if let Some((&operation, _)) = self.replies.pending_replies.first_key_value() {
            step.effects
                .push(ContextEffect::ContinueRequestReplies { operation });
        }
        visited
    }
}

#[cfg(test)]
#[path = "context/internal_tests.rs"]
mod internal_tests;

#[path = "context/rejected_creation.rs"]
mod rejected_creation;
