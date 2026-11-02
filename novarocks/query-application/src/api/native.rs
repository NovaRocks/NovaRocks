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

//! Closed Native capabilities consumed by the query application.
//!
//! The role adapter may observe one immutable execution description and bind
//! dormant Native owners to it. Private acceptance tickets prevent a value
//! already bound for one request from being accepted as another request's
//! return. The role adapter must compare its own template seal through the
//! request's borrowed matcher before using that template; it never receives
//! coordination authority.

use std::collections::BTreeSet;
use std::fmt;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;

use novarocks_execution_contract::QueryContextRef;
use novarocks_types::identity::{BackendProcessId, QueryExecutionId};
pub use novarocks_workload_control::CancellationView;
use novarocks_workload_control::WorkId;

use super::ExecutionSchedulingFacts;
use super::{QueryExecutionError, QueryExecutionErrorKind, StatementNativeHealthFailure};
use crate::admitted_query_context::QueryResultCapacityBinding;
use crate::coordination::{
    AbortQueryContextEffectPort, AcceptedRootStatusSource, AttemptSchedule, NativeAttemptDrive,
    ReplacementQualificationEffectPort, ReplacementWorkerAdmissionEvidence,
};
use crate::preparation::FrozenExecutionDescription;
use novarocks_coordination_contract::{AttemptFailureClass, RecoveryMode};

/// Runtime failure while opening the role-local Native execution session.
///
/// The closed constructors preserve the query application's four open-time
/// runtime outcomes. Ticket or binding failures use
/// [`NativeExecutionContractError`] instead.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalNativeOpenFailure {
    error: QueryExecutionError,
}

impl LogicalNativeOpenFailure {
    pub fn cancelled(message: impl Into<Arc<str>>) -> Self {
        Self::new(QueryExecutionErrorKind::Cancelled, message)
    }

    pub fn deadline_exceeded(message: impl Into<Arc<str>>) -> Self {
        Self::new(QueryExecutionErrorKind::DeadlineExceeded, message)
    }

    pub fn rejected(message: impl Into<Arc<str>>) -> Self {
        Self::new(QueryExecutionErrorKind::Rejected, message)
    }

    pub fn failed(message: impl Into<Arc<str>>) -> Self {
        Self::new(QueryExecutionErrorKind::Failed, message)
    }

    fn new(kind: QueryExecutionErrorKind, message: impl Into<Arc<str>>) -> Self {
        Self {
            error: QueryExecutionError::new(kind, message),
        }
    }

    pub const fn error(&self) -> &QueryExecutionError {
        &self.error
    }
}

impl fmt::Display for LogicalNativeOpenFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(formatter)
    }
}

impl std::error::Error for LogicalNativeOpenFailure {}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum LogicalNativeOpenError {
    Contract(NativeExecutionContractError),
    Runtime(LogicalNativeOpenFailure),
}

impl From<NativeExecutionContractError> for LogicalNativeOpenError {
    fn from(error: NativeExecutionContractError) -> Self {
        Self::Contract(error)
    }
}

impl From<LogicalNativeOpenFailure> for LogicalNativeOpenError {
    fn from(error: LogicalNativeOpenFailure) -> Self {
        Self::Runtime(error)
    }
}

impl fmt::Display for LogicalNativeOpenError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Contract(error) => error.fmt(formatter),
            Self::Runtime(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for LogicalNativeOpenError {}

/// Placement constraint retained from a failed attempt's typed transport verdict.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum NativeAttemptTopologyRequirement {
    #[default]
    LiveSnapshot,
    /// Successor placement selects other eligible processes from the live snapshot.
    ExcludeProcess(BackendProcessId),
}

/// Independent exact process-replacement evidence retained beside an original
/// pre-ready Establish or Create refusal. Native must have correlated a real
/// Worker IdentityMismatch with its original operation and Task/context; QA
/// retains the parent Establish and current recovery policy. The producer must
/// also observe a different process at its frozen endpoint within the ORIGINAL
/// observation horizon. This value does not claim
/// that the wire identified a particular IdentityField, grant retry, or prove exit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreReadyProcessReplacementWitness {
    // Rare, immutable diagnostic facts share one allocation across the original
    // failure carriers. This holds no permit, source, or resource authority.
    facts: Arc<PreReadyProcessReplacementFacts>,
}

#[derive(Debug, Eq, PartialEq)]
struct PreReadyProcessReplacementFacts {
    context: novarocks_execution_contract::QueryContextRef,
    operation_id: novarocks_execution_contract::TaskOperationId,
    kind: novarocks_execution_contract::OperationKind,
    replacement: BackendProcessId,
    refusal_fact: novarocks_failure_contract::FailureFact,
}
impl PreReadyProcessReplacementWitness {
    pub fn try_new(
        input: &crate::execution_error::ExecutionFailureInput,
        context: novarocks_execution_contract::QueryContextRef,
        operation_id: novarocks_execution_contract::TaskOperationId,
        replacement: BackendProcessId,
    ) -> Result<Self, &'static str> {
        let Some((original_context, original_operation, kind)) = Self::refusal_operation(input)
        else {
            return Err("pre-ready replacement evidence has no exact original refusal");
        };
        if original_context != context
            || original_operation != operation_id
            || replacement == context.backend_process_id()
        {
            return Err("pre-ready replacement evidence does not match its original refusal");
        }
        Ok(Self {
            facts: Arc::new(PreReadyProcessReplacementFacts {
                context,
                operation_id,
                kind,
                replacement,
                refusal_fact: input.fact(),
            }),
        })
    }
    pub(crate) fn refusal_operation(
        input: &crate::execution_error::ExecutionFailureInput,
    ) -> Option<(
        QueryContextRef,
        novarocks_execution_contract::TaskOperationId,
        novarocks_execution_contract::OperationKind,
    )> {
        use crate::execution_error::{
            ExecutionFailureSite, TASK_PROTOCOL_IDENTITY_MISMATCH_REASON,
        };
        use novarocks_execution_contract::OperationKind;
        use novarocks_failure_contract::{FailureClass, FailureEvidence};
        if input.fact().class() != FailureClass::Invariant
            || input.fact().evidence() != FailureEvidence::Originating
            || input.fact().reason() != TASK_PROTOCOL_IDENTITY_MISMATCH_REASON
        {
            return None;
        }
        match input.site() {
            ExecutionFailureSite::Operation {
                context,
                operation_id,
                kind: kind @ (OperationKind::UpdateQueryContext | OperationKind::CreateTask),
            } => Some((context, operation_id, kind)),
            _ => None,
        }
    }
    pub fn context(&self) -> novarocks_execution_contract::QueryContextRef {
        self.facts.context
    }
    pub fn operation_id(&self) -> novarocks_execution_contract::TaskOperationId {
        self.facts.operation_id
    }
    pub fn replacement_process(&self) -> BackendProcessId {
        self.facts.replacement
    }
    pub(crate) fn matches_input(
        &self,
        input: &crate::execution_error::ExecutionFailureInput,
    ) -> bool {
        input.fact() == self.facts.refusal_fact
            && input.site()
                == (crate::execution_error::ExecutionFailureSite::Operation {
                    context: self.facts.context,
                    operation_id: self.facts.operation_id,
                    kind: self.facts.kind,
                })
    }
}

/// Runtime failure while preparing one physical attempt.
///
/// Coordination consumes the attempt failure class for recovery policy and
/// retains the query error for the caller-visible terminal result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeAttemptPreparationFailure {
    error: QueryExecutionError,
    pre_ready_replacement: Option<PreReadyProcessReplacementWitness>,
}
impl NativeAttemptPreparationFailure {
    pub fn from_input(input: crate::execution_error::ExecutionFailureInput) -> Self {
        Self {
            error: QueryExecutionError::from_execution(input),
            pre_ready_replacement: None,
        }
    }
    pub fn input(&self) -> &crate::execution_error::ExecutionFailureInput {
        self.error
            .execution_failure()
            .expect("typed constructor retains its original failure input")
    }
    pub fn attach_pre_ready_replacement(
        &mut self,
        witness: PreReadyProcessReplacementWitness,
    ) -> Result<(), &'static str> {
        if self.pre_ready_replacement.is_some() || !witness.matches_input(self.input()) {
            return Err("pre-ready replacement witness belongs to another original failure");
        }
        self.pre_ready_replacement = Some(witness);
        Ok(())
    }
    pub const fn pre_ready_replacement(&self) -> Option<&PreReadyProcessReplacementWitness> {
        self.pre_ready_replacement.as_ref()
    }
    pub fn topology_requirement(&self) -> NativeAttemptTopologyRequirement {
        self.pre_ready_replacement.as_ref().map_or_else(
            || self.input().topology_requirement(),
            |witness| {
                NativeAttemptTopologyRequirement::ExcludeProcess(
                    witness.context().backend_process_id(),
                )
            },
        )
    }
    pub fn class(&self) -> AttemptFailureClass {
        self.input().attempt_class()
    }
    pub const fn error(&self) -> &QueryExecutionError {
        &self.error
    }
}

impl fmt::Display for NativeAttemptPreparationFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(formatter)
    }
}

impl std::error::Error for NativeAttemptPreparationFailure {}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum NativeAttemptPreparationError {
    Contract(NativeExecutionContractError),
    Runtime(NativeAttemptPreparationFailure),
}

impl From<NativeExecutionContractError> for NativeAttemptPreparationError {
    fn from(error: NativeExecutionContractError) -> Self {
        Self::Contract(error)
    }
}

impl From<NativeAttemptPreparationFailure> for NativeAttemptPreparationError {
    fn from(error: NativeAttemptPreparationFailure) -> Self {
        Self::Runtime(error)
    }
}

impl fmt::Display for NativeAttemptPreparationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Contract(error) => error.fmt(formatter),
            Self::Runtime(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for NativeAttemptPreparationError {}

/// Bind one role-local Native execution into its original stable Session.
/// Asynchronous preparation separately borrows that same stable source.
pub trait LogicalExecutionNativePort: fmt::Debug + Send + Sync + 'static {
    /// Pure local binding. Refusal preserves both original stable slots;
    /// success installs the same Session before any caller notification.
    fn bind_in(
        &self,
        source: &mut Option<LogicalNativeOpenRequest>,
        target: &mut Option<LogicalNativeSession>,
    ) -> Result<(), LogicalNativeOpenError>;
}

/// Move-only role-local seed for one logical Native execution.
///
/// A seed contains the immutable Native template/factory and process effect
/// capabilities selected by the sole application finalizer. It must not
/// contain a work scope, execution or attempt identity, topology, lease,
/// credential, or attempt-local split source. Those values are supplied only
/// after the supervisor binds this seed to its exact logical open ticket.
///
/// The seed is deliberately not cloneable and has no public decomposition.
/// A process-wide [`LogicalExecutionNativePort`] can consume it only as part
/// of the [`LogicalNativeOpenRequest`] that owns the exact ticket.
///
/// ```compile_fail
/// use novarocks_query_application::api::NativeLogicalExecutionSeed;
/// fn duplicate(seed: NativeLogicalExecutionSeed) {
///     let _copy = seed.clone();
/// }
/// ```
///
/// ```compile_fail
/// use novarocks_query_application::api::NativeLogicalExecutionSeed;
/// let _detached_constructor = NativeLogicalExecutionSeed::new;
/// ```
pub struct NativeLogicalExecutionSeed {
    plan: crate::api::PlanSeal,
    aborts: Arc<dyn AbortQueryContextEffectPort>,
    replacements: Option<Arc<dyn ReplacementQualificationEffectPort>>,
    attempts: Box<dyn NativeAttemptPreparationPort>,
}

impl fmt::Debug for NativeLogicalExecutionSeed {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NativeLogicalExecutionSeed")
            .field("has_replacement_port", &self.replacements.is_some())
            .finish_non_exhaustive()
    }
}

impl NativeLogicalExecutionSeed {
    /// Called only by the executable request constructor after it has consumed
    /// the frozen description. A role adapter cannot mint a detached seed or
    /// substitute a caller-declared plan seal.
    pub(crate) fn issue(
        plan: crate::api::PlanSeal,
        aborts: Arc<dyn AbortQueryContextEffectPort>,
        replacements: Option<Arc<dyn ReplacementQualificationEffectPort>>,
        attempts: impl NativeAttemptPreparationPort,
    ) -> Self {
        Self {
            plan,
            aborts,
            replacements,
            attempts: Box::new(attempts),
        }
    }

    pub(crate) const fn plan(&self) -> crate::api::PlanSeal {
        self.plan
    }
}

/// Preparing one physical attempt returns a dormant owner. It must not submit
/// tasks before the query application installs the logical runtime.
pub type NativeAttemptPreparationFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), NativeAttemptPreparationError>> + Send + 'a>>;

pub trait NativeAttemptPreparationPort: fmt::Debug + Send + 'static {
    /// The original Session owns this Work and this port outside the polled
    /// Future. Store every partial owner before another fallible operation.
    fn prepare_in<'a>(
        &'a mut self,
        work: &'a mut NativeAttemptPreparationWork,
    ) -> NativeAttemptPreparationFuture<'a>;
}

/// Future that activates one already ticket-bound Native attempt.
pub type NativeAttemptActivationFuture<'a> = Pin<
    Box<
        dyn Future<Output = Result<ActivatedNativeAttempt, NativeAttemptActivationFailure>>
            + Send
            + 'a,
    >,
>;

/// Borrowed execution future polled while the supervisor retains the active
/// Native owner.
pub type NativeAttemptRunFuture<'a> =
    Pin<Box<dyn Future<Output = NativeAttemptTerminal> + Send + 'a>>;

/// Borrowed convergence future for a dormant Native owner.
/// It resolves only after every resource the owner may have made live has
/// converged. Dropping the future or catching a poll panic leaves the owner
/// with its supervisor. Ordinary cancellation may resume that cleanup;
/// an original typed health error or captured panic instead retains this same
/// owner as unproven, without re-polling it into a fabricated later success.
pub type NativeAttemptConvergenceFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), StatementNativeHealthFailure>> + Send + 'a>>;

/// Positive evidence which closes one exact active query context.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NativeContextConvergence {
    context: QueryContextRef,
    kind: NativeContextConvergenceKind,
}

impl NativeContextConvergence {
    pub const fn never_established(context: QueryContextRef) -> Self {
        Self {
            context,
            kind: NativeContextConvergenceKind::NeverEstablished,
        }
    }

    pub const fn worker_stopped_and_context_fenced(context: QueryContextRef) -> Self {
        Self {
            context,
            kind: NativeContextConvergenceKind::WorkerStoppedAndContextFenced,
        }
    }

    pub const fn worker_process_replaced(context: QueryContextRef) -> Self {
        Self {
            context,
            kind: NativeContextConvergenceKind::WorkerProcessReplaced,
        }
    }

    pub const fn context(self) -> QueryContextRef {
        self.context
    }

    pub const fn kind(self) -> NativeContextConvergenceKind {
        self.kind
    }
}

/// Why the residual responsibility for one exact context may be retired.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeContextConvergenceKind {
    NeverEstablished,
    WorkerStoppedAndContextFenced,
    WorkerProcessReplaced,
}

/// Closed convergence facts returned by an active Native owner.
///
/// `AllWorkersStoppedAndContextsFenced` is the compact form for owners which
/// positively observed the whole attempt drain. The per-context form is used
/// when an exact process replacement closes only part of an attempt. An
/// never-established context has no Worker task or context responsibility.
/// An unobservable Worker is deliberately not representable as convergence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NativeAttemptConvergence {
    AllWorkersStoppedAndContextsFenced,
    Contexts(Box<[NativeContextConvergence]>),
}

impl NativeAttemptConvergence {
    pub const fn all_workers_stopped_and_contexts_fenced() -> Self {
        Self::AllWorkersStoppedAndContextsFenced
    }

    pub fn contexts(contexts: impl Into<Box<[NativeContextConvergence]>>) -> Self {
        Self::Contexts(contexts.into())
    }
}

/// Borrowed convergence future for an active Native owner. Resolution carries
/// local closure plus remote evidence or finite tracking termination. No caller
/// may time out/drop this whole future and infer local exit. An unprovable exit
/// returns a typed health failure while the original owner remains supervised.
/// The original remote-observation budget is frozen at the Native terminal
/// cut, before cleanup/handoff may yield. It never times out local holders.
/// Reentry may only keep or shorten that deadline (for real role shutdown),
/// never refresh it. Started operations and closure receipts remain in this
/// same owner when its borrowed future is dropped and reentered.
#[derive(Clone)]
pub struct NativeActiveConvergenceRequest {
    cancellation: CancellationView,
    absolute_remote_deadline: std::time::Instant,
    computation_ready: Option<super::ComputationReadyToken>,
}
impl NativeActiveConvergenceRequest {
    pub fn new(
        cancellation: CancellationView,
        absolute_remote_deadline: std::time::Instant,
    ) -> Self {
        Self {
            cancellation,
            absolute_remote_deadline,
            computation_ready: None,
        }
    }
    pub fn cancellation(&self) -> &CancellationView {
        &self.cancellation
    }
    pub const fn remote_observation_deadline(&self) -> std::time::Instant {
        self.absolute_remote_deadline
    }

    /// The actor's original accepted computation fact. This is neither a
    /// Native Task success declaration nor permission to publish output.
    pub const fn computation_ready(&self) -> Option<super::ComputationReadyToken> {
        self.computation_ready
    }

    pub(crate) fn with_computation_ready(
        mut self,
        token: Option<super::ComputationReadyToken>,
    ) -> Self {
        self.computation_ready = token;
        self
    }

    pub(crate) fn with_remote_observation_deadline(mut self, deadline: std::time::Instant) -> Self {
        self.absolute_remote_deadline = self.absolute_remote_deadline.min(deadline);
        self
    }
}

/// Both variants require original FE-local holders to have actually exited or
/// returned caller capabilities to their same pre-existing process records.
/// Unknown is only the end of remote tracking, never WorkerStopped/RF evidence.
#[derive(Clone, Debug)]
pub enum NativeActiveConvergenceClosure {
    Evidence(NativeAttemptConvergence),
    RemoteTrackingEnded {
        exact_contexts: Box<[QueryContextRef]>,
    },
}

pub type NativeActiveAttemptConvergenceFuture<'a> = Pin<
    Box<
        dyn Future<
                Output = Result<
                    NativeActiveConvergenceClosure,
                    super::StatementNativeHealthFailure,
                >,
            > + Send
            + 'a,
    >,
>;

/// Move-only role owner for a prepared attempt that has not started any Task.
///
/// Before binding, the owner has no asynchronous convergence responsibility.
/// The query supervisor calls `activate` only after the logical actor is
/// atomically installed. The borrowed future leaves this owner in the
/// supervisor even when polling panics; the supervisor then runs its original
/// `converge` once, retaining both owner and cause as unproven after a panic.
/// Successful activation transfers all asynchronous responsibility to
/// the returned active owner and leaves the dormant owner synchronously
/// drop-safe.
pub trait DormantNativeAttemptOwner: fmt::Debug + Send + 'static {
    /// Only the controlled role adapter can supply a live, exact placement preference.
    /// It is absent from ordinary builds and never travels on the Native wire.
    #[cfg(novarocks_dev_controls)]
    fn test_preferred_backend_process(
        &self,
        _execution: QueryExecutionId,
    ) -> Result<Option<BackendProcessId>, String> {
        Ok(None)
    }

    /// Exact process identities from the role-local topology snapshot retained
    /// by this dormant owner. Query Application derives placement only from
    /// this view, so activation cannot substitute a later snapshot while
    /// keeping the already-issued schedule.
    fn eligible_backends(&self) -> &[BackendProcessId];

    /// Install the exact successor manifest's complete dependency union before
    /// Query Application may issue any replacement qualification RPC. The
    /// original dormant owner retains its paid proposal, wait and reply across
    /// Pending, cancellation and unwind. This does not activate Tasks or return
    /// Worker admission evidence. Later activation consumes the same manifest.
    fn poll_bind_qualification(
        &mut self,
        schedule: &AttemptSchedule,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), NativeAttemptActivationFailure>>;

    fn activate<'a>(
        &'a mut self,
        schedule: &'a AttemptSchedule,
        replacement_admissions: Option<Box<[ReplacementWorkerAdmissionEvidence]>>,
        cancellation: CancellationView,
    ) -> NativeAttemptActivationFuture<'a>;

    fn converge<'a>(
        &'a mut self,
        cancellation: CancellationView,
    ) -> NativeAttemptConvergenceFuture<'a>;
}

/// Opaque role owner for an attempt whose Native work may now be live.
///
/// Query Application retains the same owner while borrowed run/converge
/// futures are polled. Its original runtime registration retains custody until
/// actual local closure plus exact remote evidence/tracking termination. A
/// typed unprovable exit or converge panic keeps that same Box and obligation
/// under process supervision until explicit process exit.
pub trait ActiveNativeAttemptOwner: fmt::Debug + Send + 'static {
    fn run<'a>(
        &'a mut self,
        drive: &'a NativeAttemptDrive,
        cancellation: CancellationView,
    ) -> NativeAttemptRunFuture<'a>;

    fn converge<'a>(
        &'a mut self,
        request: NativeActiveConvergenceRequest,
    ) -> NativeActiveAttemptConvergenceFuture<'a>;
}

/// Accepted active Native owner. Wrapper construction keeps role-local owner
/// details out of coordination while preserving a single move-only owner.
pub struct ActivatedNativeAttempt {
    owner: Box<dyn ActiveNativeAttemptOwner>,
    rows: Option<NativeRowsAttemptRuntime>,
}

impl fmt::Debug for ActivatedNativeAttempt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActivatedNativeAttempt")
            .field("has_rows_runtime", &self.rows.is_some())
            .finish_non_exhaustive()
    }
}

impl ActivatedNativeAttempt {
    pub fn completion(owner: impl ActiveNativeAttemptOwner) -> Self {
        Self {
            owner: Box::new(owner),
            rows: None,
        }
    }

    /// Rows relayed as Backend-encoded root items, without decode.
    pub fn relayed_rows(
        owner: impl ActiveNativeAttemptOwner,
        binding: crate::coordination::RootRelayBinding,
        statuses: AcceptedRootStatusSource,
    ) -> Self {
        Self {
            owner: Box::new(owner),
            rows: Some(NativeRowsAttemptRuntime { binding, statuses }),
        }
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        Box<dyn ActiveNativeAttemptOwner>,
        Option<NativeRowsAttemptRuntime>,
    ) {
        (self.owner, self.rows)
    }
}

/// Move-only role runtime for the exact root result stream of one activated
/// attempt. Native retains transport and Task status internals; the query
/// supervisor receives only the closed pump inputs it owns.
pub(crate) struct NativeRowsAttemptRuntime {
    pub(crate) binding: crate::coordination::RootRelayBinding,
    pub(crate) statuses: AcceptedRootStatusSource,
}

/// Typed activation failure. The supervisor still retains the exact dormant
/// owner and must consume it through `converge`.
pub struct NativeAttemptActivationFailure {
    failure: NativeAttemptPreparationFailure,
}

impl fmt::Debug for NativeAttemptActivationFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NativeAttemptActivationFailure")
            .field("failure", &self.failure)
            .finish_non_exhaustive()
    }
}

impl NativeAttemptActivationFailure {
    pub const fn new(failure: NativeAttemptPreparationFailure) -> Self {
        Self { failure }
    }

    pub const fn failure(&self) -> &NativeAttemptPreparationFailure {
        &self.failure
    }

    pub(crate) fn into_failure(self) -> NativeAttemptPreparationFailure {
        self.failure
    }
}

/// Physical terminal observed while the supervisor still owns the active
/// attempt. The owner must subsequently be consumed through `converge`.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum NativeAttemptTerminal {
    Completed,
    Failed(NativeAttemptPreparationFailure),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum NativeExecutionContractError {
    ForeignLogicalSeedPlan,
    ForeignLogicalOpenTicket,
    ForeignAttemptTicket,
    DifferentLogicalQuery,
    EmptyEligibleBackends,
    DuplicateEligibleBackend,
    ExcludedBackendProcess,
    MissingScanWorkFact,
    DuplicateScanWorkFact,
    ForeignScanWorkFact,
    MissingReplacementPort,
    UnexpectedReplacementPort,
    AttemptPreparationRetired,
}

impl NativeExecutionContractError {
    fn safe_message(&self) -> &'static str {
        match self {
            Self::ForeignLogicalSeedPlan => {
                "Native logical execution seed belongs to another sealed preparation plan"
            }
            Self::ForeignLogicalOpenTicket => {
                "Native logical session was bound from a different open request"
            }
            Self::ForeignAttemptTicket => {
                "prepared Native attempt was bound from a different prepare request"
            }
            Self::DifferentLogicalQuery => {
                "Native attempt does not belong to the opened logical query"
            }
            Self::AttemptPreparationRetired => {
                "Native attempt preparation has retired for this logical execution"
            }
            Self::EmptyEligibleBackends => "Native attempt has no eligible backend process",
            Self::DuplicateEligibleBackend => {
                "Native attempt repeats an eligible backend process identity"
            }
            Self::ExcludedBackendProcess => {
                "Native attempt includes the backend process excluded by its prepare request"
            }
            Self::MissingScanWorkFact => {
                "Native attempt scan work does not cover every sealed scan"
            }
            Self::DuplicateScanWorkFact => "Native attempt repeats a sealed scan work fact",
            Self::ForeignScanWorkFact => "Native attempt scan work belongs to another sealed plan",
            Self::MissingReplacementPort => {
                "recoverable logical execution requires a replacement qualification port"
            }
            Self::UnexpectedReplacementPort => {
                "non-recoverable logical execution must not install a replacement qualification port"
            }
        }
    }

    pub fn safe_detail(&self) -> novarocks_execution_contract::SafeDetail {
        novarocks_execution_contract::SafeDetail::truncating(self.safe_message())
    }
}

impl novarocks_execution_contract::failure::FailureProjection for NativeExecutionContractError {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::{
            FailureClass, FailureFact, FailureReason, domain,
        };
        let code = match self {
            Self::ForeignLogicalSeedPlan => 110,
            Self::ForeignLogicalOpenTicket => 111,
            Self::ForeignAttemptTicket => 112,
            Self::DifferentLogicalQuery => 113,
            Self::EmptyEligibleBackends => 114,
            Self::DuplicateEligibleBackend => 115,
            Self::ExcludedBackendProcess => 116,
            Self::MissingScanWorkFact => 117,
            Self::DuplicateScanWorkFact => 118,
            Self::ForeignScanWorkFact => 119,
            Self::MissingReplacementPort => 120,
            Self::UnexpectedReplacementPort => 121,
            Self::AttemptPreparationRetired => 122,
        };
        let reason = FailureReason::new(domain::QUERY, std::num::NonZeroU16::new(code).unwrap());
        if matches!(self, Self::AttemptPreparationRetired) {
            FailureFact::stop_observation(reason)
        } else {
            FailureFact::new(FailureClass::Invariant, reason)
        }
    }
}

impl fmt::Display for NativeExecutionContractError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.safe_message())
    }
}

impl std::error::Error for NativeExecutionContractError {}

struct LogicalNativeTicket {
    initial_execution: QueryExecutionId,
    description: Arc<FrozenExecutionDescription>,
    work_id: WorkId,
    cancellation: CancellationView,
}

impl fmt::Debug for LogicalNativeTicket {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LogicalNativeTicket")
            .field("initial_execution", &self.initial_execution)
            .field("work", &self.work_id)
            .finish_non_exhaustive()
    }
}

/// Move-only request issued by the query application for one logical Native
/// session.
///
/// The private ticket cannot be duplicated or manufactured by a role adapter.
///
/// ```compile_fail
/// use novarocks_query_application::api::LogicalNativeOpenRequest;
/// fn duplicate(request: LogicalNativeOpenRequest) {
///     let _copy = request.clone();
/// }
/// ```
///
/// ```compile_fail
/// use novarocks_query_application::api::LogicalNativeOpenRequest;
/// fn steal_ticket(request: LogicalNativeOpenRequest) {
///     let _ticket = request.ticket;
/// }
/// ```
pub struct LogicalNativeOpenRequest {
    ticket: Arc<LogicalNativeTicket>,
    seed: NativeLogicalExecutionSeed,
}

impl fmt::Debug for LogicalNativeOpenRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LogicalNativeOpenRequest")
            .field("initial_execution", &self.ticket.initial_execution)
            .finish_non_exhaustive()
    }
}

impl LogicalNativeOpenRequest {
    pub fn initial_execution(&self) -> QueryExecutionId {
        self.ticket.initial_execution
    }

    pub fn description(&self) -> &FrozenExecutionDescription {
        self.ticket.description.as_ref()
    }

    pub fn work_id(&self) -> WorkId {
        self.ticket.work_id
    }

    pub fn cancellation(&self) -> CancellationView {
        self.ticket.cancellation.clone()
    }

    /// Consumes the only request ticket and binds the role-local attempt port
    /// to its immutable logical description.
    pub fn bind(self) -> Result<LogicalNativeSession, NativeExecutionContractError> {
        match (
            self.ticket.description.recovery(),
            self.seed.replacements.is_some(),
        ) {
            (RecoveryMode::RestartAttemptBeforeVisibility, false) => {
                return Err(NativeExecutionContractError::MissingReplacementPort);
            }
            (RecoveryMode::NoRecovery, true) => {
                return Err(NativeExecutionContractError::UnexpectedReplacementPort);
            }
            _ => {}
        }
        Ok(LogicalNativeSession {
            ticket: self.ticket,
            aborts: self.seed.aborts,
            replacements: self.seed.replacements,
            attempts: Some(NativeAttemptPreparationState {
                port: self.seed.attempts,
                result_capacity: None,
            }),
            pending_work: None,
            pending_acceptance: None,
            retiring: false,
        })
    }

    pub fn bind_in(
        source: &mut Option<Self>,
        target: &mut Option<LogicalNativeSession>,
    ) -> Result<(), NativeExecutionContractError> {
        if target.is_some() || source.is_none() {
            return Err(NativeExecutionContractError::ForeignLogicalOpenTicket);
        }
        let request = source.as_ref().unwrap();
        match (
            request.ticket.description.recovery(),
            request.seed.replacements.is_some(),
        ) {
            (RecoveryMode::RestartAttemptBeforeVisibility, false) => {
                return Err(NativeExecutionContractError::MissingReplacementPort);
            }
            (RecoveryMode::NoRecovery, true) => {
                return Err(NativeExecutionContractError::UnexpectedReplacementPort);
            }
            _ => {}
        }
        // The same pure checks have completed before any original source move.
        *target = Some(
            source
                .take()
                .unwrap()
                .bind()
                .expect("validated pure Native binding"),
        );
        Ok(())
    }

    pub(crate) fn issue(
        initial_execution: QueryExecutionId,
        description: Arc<FrozenExecutionDescription>,
        work_id: WorkId,
        cancellation: CancellationView,
        seed: NativeLogicalExecutionSeed,
    ) -> (Self, LogicalNativeOpenAcceptance) {
        let ticket = Arc::new(LogicalNativeTicket {
            initial_execution,
            description,
            work_id,
            cancellation,
        });
        (
            Self {
                ticket: Arc::clone(&ticket),
                seed,
            },
            LogicalNativeOpenAcceptance { ticket },
        )
    }
}

#[derive(Debug)]
pub struct LogicalNativeSession {
    ticket: Arc<LogicalNativeTicket>,
    aborts: Arc<dyn AbortQueryContextEffectPort>,
    replacements: Option<Arc<dyn ReplacementQualificationEffectPort>>,
    attempts: Option<NativeAttemptPreparationState>,
    pending_work: Option<NativeAttemptPreparationWork>,
    pending_acceptance: Option<NativeAttemptPreparationAcceptance>,
    retiring: bool,
}

/// Explicit preparation custody. It is never shared by the logical ticket or
/// the opaque Native seed, and retires before remote convergence begins.
struct NativeAttemptPreparationState {
    port: Box<dyn NativeAttemptPreparationPort>,
    result_capacity: Option<QueryResultCapacityBinding>,
}
impl fmt::Debug for NativeAttemptPreparationState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NativeAttemptPreparationState")
            .field("port", &self.port)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub(crate) struct LogicalNativeOpenAcceptance {
    ticket: Arc<LogicalNativeTicket>,
}

impl LogicalNativeOpenAcceptance {
    pub(crate) fn validate(
        &self,
        session: &LogicalNativeSession,
    ) -> Result<(), NativeExecutionContractError> {
        if !Arc::ptr_eq(&self.ticket, &session.ticket) {
            return Err(NativeExecutionContractError::ForeignLogicalOpenTicket);
        }
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn accept(
        self,
        session: LogicalNativeSession,
    ) -> Result<LogicalNativeSession, NativeExecutionContractError> {
        if !Arc::ptr_eq(&self.ticket, &session.ticket) {
            return Err(NativeExecutionContractError::ForeignLogicalOpenTicket);
        }
        Ok(session)
    }
}

impl LogicalNativeSession {
    pub(crate) fn streamed_computation_cut_handle(
        &self,
    ) -> Option<crate::admitted_query_context::StreamedComputationCutHandle> {
        self.attempts
            .as_ref()?
            .result_capacity
            .as_ref()?
            .streamed_computation_cut_handle()
    }
    pub(crate) fn set_result_capacity_in(
        &mut self,
        capacity: &mut Option<QueryResultCapacityBinding>,
    ) {
        let target = &mut self
            .attempts
            .as_mut()
            .expect("original installed preparation")
            .result_capacity;
        assert!(target.is_none());
        *target = capacity.take();
    }

    pub(crate) fn issue_attempt(
        &self,
        execution: QueryExecutionId,
    ) -> Result<
        (
            NativeAttemptPreparationRequest,
            NativeAttemptPreparationAcceptance,
        ),
        NativeExecutionContractError,
    > {
        let preparation = self
            .attempts
            .as_ref()
            .ok_or(NativeExecutionContractError::AttemptPreparationRetired)?;
        if execution.query_id() != self.ticket.initial_execution.query_id() {
            return Err(NativeExecutionContractError::DifferentLogicalQuery);
        }
        let attempt_ticket = Arc::new(AttemptTicket);
        Ok((
            NativeAttemptPreparationRequest {
                logical_ticket: Arc::clone(&self.ticket),
                attempt_ticket: Arc::clone(&attempt_ticket),
                execution,
                topology_requirement: NativeAttemptTopologyRequirement::LiveSnapshot,
                result_capacity: preparation.result_capacity.clone(),
            },
            NativeAttemptPreparationAcceptance::new(&self.ticket, attempt_ticket, execution),
        ))
    }

    pub(crate) fn begin_preparation(
        &mut self,
        execution: QueryExecutionId,
        requirement: NativeAttemptTopologyRequirement,
    ) -> Result<(), NativeExecutionContractError> {
        if self.pending_work.is_some() || self.pending_acceptance.is_some() || self.retiring {
            return Err(NativeExecutionContractError::ForeignAttemptTicket);
        }
        let (request, acceptance) = self.issue_attempt(execution)?;
        self.pending_work = Some(NativeAttemptPreparationWork::new(
            request.require_topology(requirement),
        ));
        self.pending_acceptance = Some(acceptance);
        Ok(())
    }

    pub(crate) fn prepare_pending(&mut self) -> NativeAttemptPreparationFuture<'_> {
        match (self.attempts.as_mut(), self.pending_work.as_mut()) {
            (Some(attempts), Some(work)) => attempts.port.prepare_in(work),
            _ => Box::pin(async {
                Err(NativeExecutionContractError::AttemptPreparationRetired.into())
            }),
        }
    }

    pub(crate) fn accept_pending_in(
        &mut self,
        dormant: &mut NativeDormantCustody,
    ) -> Result<NativePreparedFacts, NativeExecutionContractError> {
        if dormant.owner.is_some() || dormant.drop_unproven() {
            return Err(NativeExecutionContractError::ForeignAttemptTicket);
        }
        let work = self
            .pending_work
            .as_mut()
            .ok_or(NativeExecutionContractError::ForeignAttemptTicket)?;
        let prepared = work
            .prepared
            .as_ref()
            .ok_or(NativeExecutionContractError::ForeignAttemptTicket)?;
        self.pending_acceptance
            .as_ref()
            .ok_or(NativeExecutionContractError::ForeignAttemptTicket)?
            .validate(prepared)?;
        // Every identity/destination check precedes all moves. The owner is
        // installed in its original stable slot before dropping ticket aliases.
        let prepared = work.prepared.take().unwrap();
        let PreparedNativeAttempt {
            logical_ticket,
            attempt_ticket,
            execution,
            eligible_backends,
            scheduling,
            owner,
        } = prepared;
        dormant.owner = Some(owner);
        dormant.convergence_completed = false;
        let facts = NativePreparedFacts {
            execution,
            eligible_backends,
            scheduling,
        };
        drop((logical_ticket, attempt_ticket));
        self.pending_acceptance.take();
        self.pending_work.take();
        Ok(facts)
    }

    /// This is actual synchronous destruction of the first-party preparation
    /// body, not a receipt. A panic leaves retiring=true and the original
    /// Source cannot report local exit. Whole Session exit is separate.
    pub(crate) fn retire_preparation(&mut self) {
        if self.retiring {
            return;
        }
        self.retiring = true;
        drop(self.pending_work.take());
        drop(self.pending_acceptance.take());
        drop(self.attempts.take());
        self.retiring = false;
    }

    pub(crate) fn preparation_drop_unproven(&self) -> bool {
        self.retiring
    }

    #[cfg(test)]
    async fn prepare(
        &mut self,
        request: NativeAttemptPreparationRequest,
    ) -> Result<PreparedNativeAttempt, NativeAttemptPreparationError> {
        if self.attempts.is_none() {
            return Err(NativeExecutionContractError::AttemptPreparationRetired.into());
        }
        assert!(self.pending_work.is_none());
        self.pending_work = Some(NativeAttemptPreparationWork::new(request));
        self.prepare_pending().await?;
        let prepared = self.pending_work.as_mut().unwrap().prepared.take().unwrap();
        self.pending_work.take();
        Ok(prepared)
    }

    pub(crate) fn abort_effect_port(&self) -> Arc<dyn AbortQueryContextEffectPort> {
        Arc::clone(&self.aborts)
    }

    pub(crate) fn replacement_effect_port(
        &self,
    ) -> Option<Arc<dyn ReplacementQualificationEffectPort>> {
        self.replacements.as_ref().map(Arc::clone)
    }
}

#[derive(Debug)]
struct AttemptTicket;

/// Placement preference for runtime work; it does not constrain correctness.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeSplitPlacement {
    SingleUnit,
    Spread,
}

/// Native-owned work cardinality for one exact sealed scan occurrence.
///
/// This is an input to Query Application scheduling, not a placement result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeScanWork {
    /// The exact static work set is empty. The containing fragment still
    /// retains one Task so its non-scan operators and exchange obligations
    /// have an execution owner.
    Empty,
    RuntimeSplits {
        placement: RuntimeSplitPlacement,
    },
    FrozenUnits {
        count: NonZeroUsize,
    },
}

/// Move-only request for one exact physical execution attempt.
///
/// ```compile_fail
/// use novarocks_query_application::api::NativeAttemptPreparationRequest;
/// fn duplicate(request: NativeAttemptPreparationRequest) {
///     let _copy = request.clone();
/// }
/// ```
pub struct NativeAttemptPreparationRequest {
    topology_requirement: NativeAttemptTopologyRequirement,
    logical_ticket: Arc<LogicalNativeTicket>,
    attempt_ticket: Arc<AttemptTicket>,
    execution: QueryExecutionId,
    result_capacity: Option<QueryResultCapacityBinding>,
}

impl fmt::Debug for NativeAttemptPreparationRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NativeAttemptPreparationRequest")
            .field("execution", &self.execution)
            .finish_non_exhaustive()
    }
}

impl NativeAttemptPreparationRequest {
    /// Borrows the same runtime binding retained by the admitted start owner.
    /// Factories may retain it only in this attempt's concrete runtime owners.
    pub fn result_capacity(&self) -> Option<&QueryResultCapacityBinding> {
        self.result_capacity.as_ref()
    }

    pub const fn topology_requirement(&self) -> NativeAttemptTopologyRequirement {
        self.topology_requirement
    }

    pub(crate) fn require_topology(
        mut self,
        requirement: NativeAttemptTopologyRequirement,
    ) -> Self {
        self.topology_requirement = requirement;
        self
    }

    pub const fn execution(&self) -> QueryExecutionId {
        self.execution
    }

    pub fn description(&self) -> &FrozenExecutionDescription {
        self.logical_ticket.description.as_ref()
    }

    /// Compare the request with the role adapter's own immutable Native
    /// template. The opaque seal is never returned to the adapter, so it
    /// cannot replace this proof with a caller-declared identity.
    pub fn matches_plan_seal(&self, template_plan: crate::api::PlanSeal) -> bool {
        self.logical_ticket.description.plan_identity() == template_plan
    }

    pub fn work_id(&self) -> WorkId {
        self.logical_ticket.work_id
    }

    pub fn cancellation(&self) -> CancellationView {
        self.logical_ticket.cancellation.clone()
    }

    /// Consumes this exact prepare ticket and seals Native-owned scheduling
    /// inputs. Query Application alone derives the context and Task manifest.
    pub fn bind(
        self,
        scheduling: ExecutionSchedulingFacts,
        owner: impl DormantNativeAttemptOwner,
    ) -> Result<PreparedNativeAttempt, NativeExecutionContractError> {
        let eligible_backends = owner.eligible_backends().to_vec();
        validate_prepared_inputs(
            self.logical_ticket.description.as_ref(),
            &eligible_backends,
            &scheduling,
        )?;
        if let NativeAttemptTopologyRequirement::ExcludeProcess(excluded) =
            self.topology_requirement
            && eligible_backends.contains(&excluded)
        {
            return Err(NativeExecutionContractError::ExcludedBackendProcess);
        }
        Ok(PreparedNativeAttempt {
            logical_ticket: self.logical_ticket,
            attempt_ticket: self.attempt_ticket,
            execution: self.execution,
            eligible_backends: eligible_backends.into_boxed_slice(),
            scheduling,
            owner: Box::new(owner),
        })
    }
}

/// Stable work space of the one original Session request. It grants no new
/// Work, identity, claim, network capability or scheduling authority.
#[derive(Debug)]
pub struct NativeAttemptPreparationWork {
    request: Option<NativeAttemptPreparationRequest>,
    prepared: Option<PreparedNativeAttempt>,
    // Actual unit fixtures use these same finite source fields. Production
    // binds from its concrete original port's stable source fields below.
    #[cfg(test)]
    staged_owner: Option<Box<dyn DormantNativeAttemptOwner>>,
    #[cfg(test)]
    staged_scheduling: Option<ExecutionSchedulingFacts>,
}
impl std::ops::Deref for NativeAttemptPreparationWork {
    type Target = NativeAttemptPreparationRequest;
    fn deref(&self) -> &Self::Target {
        self.request
            .as_ref()
            .expect("original request not yet bound")
    }
}
impl NativeAttemptPreparationWork {
    fn new(request: NativeAttemptPreparationRequest) -> Self {
        Self {
            request: Some(request),
            prepared: None,
            #[cfg(test)]
            staged_owner: None,
            #[cfg(test)]
            staged_scheduling: None,
        }
    }
    pub fn bind_in(
        &mut self,
        scheduling: &mut Option<ExecutionSchedulingFacts>,
        owner: &mut Option<Box<dyn DormantNativeAttemptOwner>>,
    ) -> Result<(), NativeExecutionContractError> {
        Self::bind_original_in(&mut self.request, &mut self.prepared, scheduling, owner)
    }
    fn bind_original_in(
        request: &mut Option<NativeAttemptPreparationRequest>,
        prepared: &mut Option<PreparedNativeAttempt>,
        scheduling: &mut Option<ExecutionSchedulingFacts>,
        owner: &mut Option<Box<dyn DormantNativeAttemptOwner>>,
    ) -> Result<(), NativeExecutionContractError> {
        if prepared.is_some() || request.is_none() || owner.is_none() || scheduling.is_none() {
            return Err(NativeExecutionContractError::ForeignAttemptTicket);
        }
        let original = request.as_ref().unwrap();
        let eligible = owner.as_ref().unwrap().eligible_backends().to_vec();
        validate_prepared_inputs(
            original.description(),
            &eligible,
            scheduling.as_ref().unwrap(),
        )?;
        if let NativeAttemptTopologyRequirement::ExcludeProcess(excluded) =
            original.topology_requirement
            && eligible.contains(&excluded)
        {
            return Err(NativeExecutionContractError::ExcludedBackendProcess);
        }
        let original = request.take().unwrap();
        *prepared = Some(PreparedNativeAttempt {
            logical_ticket: original.logical_ticket,
            attempt_ticket: original.attempt_ticket,
            execution: original.execution,
            eligible_backends: eligible.into_boxed_slice(),
            scheduling: scheduling.take().unwrap(),
            owner: owner.take().unwrap(),
        });
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn stage_fixture_owner(&mut self, owner: impl DormantNativeAttemptOwner) {
        assert!(self.staged_owner.is_none() && self.prepared.is_none());
        self.staged_owner = Some(Box::new(owner));
    }
    #[cfg(test)]
    pub(crate) fn bind_staged(
        &mut self,
        scheduling: ExecutionSchedulingFacts,
    ) -> Result<(), NativeExecutionContractError> {
        assert!(self.staged_scheduling.is_none());
        self.staged_scheduling = Some(scheduling);
        Self::bind_original_in(
            &mut self.request,
            &mut self.prepared,
            &mut self.staged_scheduling,
            &mut self.staged_owner,
        )
    }
}

pub(crate) enum DormantConvergenceFailure {
    Reported(StatementNativeHealthFailure),
    PollPanic(Box<dyn std::any::Any + Send>),
}

#[derive(Default)]
pub(crate) struct NativeDormantCustody {
    owner: Option<Box<dyn DormantNativeAttemptOwner>>,
    retiring: bool,
    activation_panic: Option<Box<dyn std::any::Any + Send>>,
    convergence_failure: Option<DormantConvergenceFailure>,
    convergence_completed: bool,
}
impl fmt::Debug for NativeDormantCustody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Never invoke an opaque cause's formatting or a foreign owner's Debug.
        f.debug_struct("NativeDormantCustody")
            .field("has_owner", &self.owner.is_some())
            .field("retiring", &self.retiring)
            .field("activation_panicked", &self.activation_panic.is_some())
            .field("convergence_failed", &self.convergence_failure.is_some())
            .field("convergence_completed", &self.convergence_completed)
            .finish()
    }
}
pub(crate) type DormantActivationParts<'a> = (
    &'a mut (dyn DormantNativeAttemptOwner + 'static),
    &'a mut Option<Box<dyn std::any::Any + Send>>,
);

impl NativeDormantCustody {
    pub(crate) fn as_mut(&mut self) -> &mut dyn DormantNativeAttemptOwner {
        self.owner
            .as_mut()
            .expect("same stable dormant owner")
            .as_mut()
    }
    /// Pure split borrow of this original owner and its empty receiving slot.
    pub(crate) fn activation_parts(&mut self) -> Option<DormantActivationParts<'_>> {
        if self.retiring || self.has_failure() || self.convergence_completed {
            return None;
        }
        Some((self.owner.as_mut()?.as_mut(), &mut self.activation_panic))
    }
    /// An activation panic still permits the original cleanup once. A typed
    /// health failure or poll panic in cleanup forbids restarting that source.
    pub(crate) fn convergence_parts(
        &mut self,
    ) -> Option<(
        &mut (dyn DormantNativeAttemptOwner + 'static),
        &mut Option<DormantConvergenceFailure>,
    )> {
        if self.retiring || self.convergence_failure.is_some() || self.convergence_completed {
            return None;
        }
        Some((self.owner.as_mut()?.as_mut(), &mut self.convergence_failure))
    }
    pub(crate) fn convergence_completed(&self) -> bool {
        self.convergence_completed
    }
    pub(crate) fn record_convergence_completed(&mut self) {
        self.convergence_completed = true;
    }
    pub(crate) fn has_failure(&self) -> bool {
        self.activation_panic.is_some() || self.convergence_failure.is_some()
    }
    pub(crate) fn activation_panic(&self) -> Option<&(dyn std::any::Any + Send)> {
        self.activation_panic.as_deref()
    }
    pub(crate) fn convergence_panic(&self) -> Option<&(dyn std::any::Any + Send)> {
        match self.convergence_failure.as_ref()? {
            DormantConvergenceFailure::PollPanic(original) => Some(original.as_ref()),
            DormantConvergenceFailure::Reported(_) => None,
        }
    }
    pub(crate) fn convergence_failure(&self) -> Option<&StatementNativeHealthFailure> {
        match self.convergence_failure.as_ref()? {
            DormantConvergenceFailure::Reported(original) => Some(original),
            DormantConvergenceFailure::PollPanic(_) => None,
        }
    }
    #[must_use = "an unproven original dormant owner must remain in custody"]
    pub(crate) fn retire(&mut self) -> Result<(), ()> {
        if self.drop_unproven() {
            return Err(());
        }
        self.retiring = true;
        drop(self.owner.take());
        self.retiring = false;
        Ok(())
    }
    pub(crate) fn drop_unproven(&self) -> bool {
        self.retiring || self.has_failure()
    }
    #[cfg(test)]
    pub(crate) fn test_owner(owner: impl DormantNativeAttemptOwner) -> Self {
        Self {
            owner: Some(Box::new(owner)),
            ..Self::default()
        }
    }
}
#[derive(Debug)]
pub(crate) struct NativePreparedFacts {
    pub(crate) execution: QueryExecutionId,
    pub(crate) eligible_backends: Box<[BackendProcessId]>,
    pub(crate) scheduling: ExecutionSchedulingFacts,
}

/// Ticketed dormant attempt returned across the Native port.
///
/// Its fields and split remain private to the query application. A role adapter
/// may construct this value only by consuming the exact prepare request.
///
/// ```compile_fail
/// use novarocks_query_application::api::PreparedNativeAttempt;
/// fn take_coordination_authority(prepared: PreparedNativeAttempt) {
///     let _parts = prepared.into_parts();
/// }
/// ```
pub struct PreparedNativeAttempt {
    logical_ticket: Arc<LogicalNativeTicket>,
    attempt_ticket: Arc<AttemptTicket>,
    execution: QueryExecutionId,
    eligible_backends: Box<[BackendProcessId]>,
    scheduling: ExecutionSchedulingFacts,
    owner: Box<dyn DormantNativeAttemptOwner>,
}

impl fmt::Debug for PreparedNativeAttempt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedNativeAttempt")
            .field("execution", &self.execution)
            .field("eligible_backends", &self.eligible_backends)
            .field("scheduling", &self.scheduling)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
#[derive(Debug)]
pub(crate) struct PreparedNativeAttemptParts {
    pub(crate) execution: QueryExecutionId,
    pub(crate) eligible_backends: Box<[BackendProcessId]>,
    pub(crate) scheduling: ExecutionSchedulingFacts,
    pub(crate) owner: Box<dyn DormantNativeAttemptOwner>,
}

#[derive(Debug)]
pub(crate) struct NativeAttemptPreparationAcceptance {
    logical_ticket: Arc<LogicalNativeTicket>,
    attempt_ticket: Arc<AttemptTicket>,
    execution: QueryExecutionId,
}

impl NativeAttemptPreparationAcceptance {
    fn new(
        logical_ticket: &Arc<LogicalNativeTicket>,
        attempt_ticket: Arc<AttemptTicket>,
        execution: QueryExecutionId,
    ) -> Self {
        Self {
            logical_ticket: Arc::clone(logical_ticket),
            attempt_ticket,
            execution,
        }
    }

    fn validate(
        &self,
        prepared: &PreparedNativeAttempt,
    ) -> Result<(), NativeExecutionContractError> {
        if !Arc::ptr_eq(&self.logical_ticket, &prepared.logical_ticket)
            || !Arc::ptr_eq(&self.attempt_ticket, &prepared.attempt_ticket)
            || self.execution != prepared.execution
        {
            return Err(NativeExecutionContractError::ForeignAttemptTicket);
        }
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn accept(
        self,
        prepared: PreparedNativeAttempt,
    ) -> Result<PreparedNativeAttemptParts, NativeExecutionContractError> {
        if !Arc::ptr_eq(&self.logical_ticket, &prepared.logical_ticket)
            || !Arc::ptr_eq(&self.attempt_ticket, &prepared.attempt_ticket)
            || self.execution != prepared.execution
        {
            return Err(NativeExecutionContractError::ForeignAttemptTicket);
        }
        Ok(PreparedNativeAttemptParts {
            execution: prepared.execution,
            eligible_backends: prepared.eligible_backends,
            scheduling: prepared.scheduling,
            owner: prepared.owner,
        })
    }
}

fn validate_prepared_inputs(
    description: &FrozenExecutionDescription,
    eligible_backends: &[BackendProcessId],
    scheduling: &ExecutionSchedulingFacts,
) -> Result<(), NativeExecutionContractError> {
    if eligible_backends.is_empty() {
        return Err(NativeExecutionContractError::EmptyEligibleBackends);
    }
    if eligible_backends
        .iter()
        .copied()
        .collect::<BTreeSet<_>>()
        .len()
        != eligible_backends.len()
    {
        return Err(NativeExecutionContractError::DuplicateEligibleBackend);
    }
    let expected = description
        .scan_identities()
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let mut actual = BTreeSet::new();
    for fragment in &scheduling.fragments {
        for scan in &fragment.scans {
            if !expected.contains(&scan.scan) {
                return Err(NativeExecutionContractError::ForeignScanWorkFact);
            }
            if !actual.insert(scan.scan) {
                return Err(NativeExecutionContractError::DuplicateScanWorkFact);
            }
        }
    }
    if actual != expected {
        return Err(NativeExecutionContractError::MissingScanWorkFact);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{
        FragmentSchedulingFacts, PlanScanIdentity, QueryExecutionKind, ScanSchedulingFacts,
    };
    use crate::coordination::{
        PermanentlyBackpressuredAbortEffectPort, ReplacementQualificationEffectAdmission,
        ReplacementQualificationRequest,
    };
    use crate::execution_error::{ExecutionFailureInput, ExecutionFailureSite, QueryFailureReason};
    use crate::preparation::{
        ExecutionResourceRequirements, FrozenCostEstimate, FrozenEstimateUnknownReason,
        OutputContract,
    };
    use novarocks_coordination_contract::ExecutionEffect;
    use novarocks_execution_contract::{SafeDetail, failure::FailureProjection};
    use novarocks_types::identity::{AttemptId, BackendProcessId, QueryId};
    use novarocks_workload_control::{
        ResourceConfig, RootWork, WorkClass, WorkRequest, WorkScope, WorkloadConfig,
        WorkloadControl,
    };
    use tokio::sync::watch;

    #[derive(Debug)]
    struct Dormant {
        tag: u8,
        eligible_backends: Vec<BackendProcessId>,
    }

    impl Dormant {
        fn new(tag: u8, eligible_backends: Vec<BackendProcessId>) -> Self {
            Self {
                tag,
                eligible_backends,
            }
        }
    }

    #[derive(Debug)]
    struct ActiveDormant(u8);

    impl ActiveNativeAttemptOwner for ActiveDormant {
        fn run<'a>(
            &'a mut self,
            _drive: &'a NativeAttemptDrive,
            _cancellation: CancellationView,
        ) -> NativeAttemptRunFuture<'a> {
            let _tag = self.0;
            Box::pin(async { NativeAttemptTerminal::Completed })
        }

        fn converge<'a>(
            &'a mut self,
            _request: NativeActiveConvergenceRequest,
        ) -> NativeActiveAttemptConvergenceFuture<'a> {
            Box::pin(async {
                Ok(NativeActiveConvergenceClosure::Evidence(
                    NativeAttemptConvergence::all_workers_stopped_and_contexts_fenced(),
                ))
            })
        }
    }

    impl DormantNativeAttemptOwner for Dormant {
        fn poll_bind_qualification(
            &mut self,
            _schedule: &crate::coordination::AttemptSchedule,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), crate::api::NativeAttemptActivationFailure>> {
            // This fixture owns no external dependency or qualification transport.
            std::task::Poll::Ready(Ok(()))
        }

        fn eligible_backends(&self) -> &[BackendProcessId] {
            &self.eligible_backends
        }

        fn activate<'a>(
            &'a mut self,
            _schedule: &'a AttemptSchedule,
            _replacement_admissions: Option<Box<[ReplacementWorkerAdmissionEvidence]>>,
            _cancellation: CancellationView,
        ) -> NativeAttemptActivationFuture<'a> {
            Box::pin(async move { Ok(ActivatedNativeAttempt::completion(ActiveDormant(self.tag))) })
        }

        fn converge<'a>(
            &'a mut self,
            _cancellation: CancellationView,
        ) -> NativeAttemptConvergenceFuture<'a> {
            Box::pin(async { Ok(()) })
        }
    }

    fn process_bytes(seed: u8) -> [u8; 16] {
        [
            0, 0, 0, 0, 0, seed, 0x70, seed, 0x80, 0, 0, 0, 0, 0, 0, seed,
        ]
    }

    fn execution(query: i64, attempt: u64) -> QueryExecutionId {
        QueryExecutionId::new(
            QueryId::new(query, query + 1),
            AttemptId::new(attempt).unwrap(),
        )
        .unwrap()
    }

    fn fixture_version() -> [u8; 16] {
        static NEXT_VERSION: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let sequence = NEXT_VERSION.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut version = [91; 16];
        version[8..].copy_from_slice(&sequence.to_be_bytes());
        version
    }

    fn description(recovery: RecoveryMode) -> Arc<FrozenExecutionDescription> {
        let plan = crate::completed_plan_fixture::completed_noop_plan(fixture_version());
        Arc::new(
            FrozenExecutionDescription::for_completed_plan(
                QueryExecutionKind::Maintenance,
                plan.candidate().clone(),
                Vec::new(),
                OutputContract::CompletionOnly,
                ExecutionEffect::None,
                recovery,
                Vec::new(),
                FrozenCostEstimate::unknown(FrozenEstimateUnknownReason::NotProjected),
                ExecutionResourceRequirements::unknown(FrozenEstimateUnknownReason::NotProjected),
            )
            .unwrap(),
        )
    }

    fn seed_for_description(
        description: &FrozenExecutionDescription,
        aborts: Arc<dyn AbortQueryContextEffectPort>,
        replacements: Option<Arc<dyn ReplacementQualificationEffectPort>>,
        attempts: impl NativeAttemptPreparationPort,
    ) -> NativeLogicalExecutionSeed {
        NativeLogicalExecutionSeed::issue(
            description.plan_identity(),
            aborts,
            replacements,
            attempts,
        )
    }

    struct Governance {
        _control: WorkloadControl,
        root: Option<RootWork>,
    }

    impl Governance {
        fn new() -> Self {
            let control = WorkloadControl::try_new(
                WorkloadConfig::default(),
                ResourceConfig {
                    total_bytes: 1024,
                    control_bytes: 128,
                    per_scope_bytes: 896,
                },
            )
            .unwrap();
            control.mark_ready().unwrap();
            let root = control
                .try_begin_root(WorkRequest::new(WorkClass::Query))
                .unwrap();
            Self {
                _control: control,
                root: Some(root),
            }
        }

        fn scope(&self) -> WorkScope {
            self.root.as_ref().unwrap().owner.scope()
        }
    }

    impl Drop for Governance {
        fn drop(&mut self) {
            let root = self.root.take().unwrap();
            root.owner.complete();
            root.business.release();
        }
    }

    fn issue(
        execution: QueryExecutionId,
        recovery: RecoveryMode,
    ) -> (
        Governance,
        LogicalNativeOpenRequest,
        LogicalNativeOpenAcceptance,
    ) {
        let governance = Governance::new();
        let scope = governance.scope();
        let description = description(recovery);
        let replacements = match recovery {
            RecoveryMode::NoRecovery => None,
            RecoveryMode::RestartAttemptBeforeVisibility => {
                Some(BackpressuredReplacementPort::shared())
            }
        };
        let (request, acceptance) = LogicalNativeOpenRequest::issue(
            execution,
            Arc::clone(&description),
            scope.id(),
            scope.cancellation().unwrap(),
            seed_for_description(
                description.as_ref(),
                PermanentlyBackpressuredAbortEffectPort::shared(),
                replacements,
                NoopPreparationPort,
            ),
        );
        (governance, request, acceptance)
    }

    fn issue_description(
        execution: QueryExecutionId,
        description: Arc<FrozenExecutionDescription>,
    ) -> (
        Governance,
        LogicalNativeOpenRequest,
        LogicalNativeOpenAcceptance,
    ) {
        let governance = Governance::new();
        let scope = governance.scope();
        let (request, acceptance) = LogicalNativeOpenRequest::issue(
            execution,
            Arc::clone(&description),
            scope.id(),
            scope.cancellation().unwrap(),
            seed_for_description(
                description.as_ref(),
                PermanentlyBackpressuredAbortEffectPort::shared(),
                None,
                NoopPreparationPort,
            ),
        );
        (governance, request, acceptance)
    }

    fn scan_description() -> Arc<FrozenExecutionDescription> {
        let candidate = crate::completed_plan_fixture::completed_scan_candidate(fixture_version());
        let scan = PlanScanIdentity::new(
            crate::api::PlanSeal::Version(candidate.plan().version()),
            1,
            0,
        );
        Arc::new(
            FrozenExecutionDescription::for_completed_plan(
                QueryExecutionKind::Maintenance,
                candidate,
                vec![scan],
                OutputContract::CompletionOnly,
                ExecutionEffect::None,
                RecoveryMode::NoRecovery,
                Vec::new(),
                FrozenCostEstimate::unknown(FrozenEstimateUnknownReason::NotProjected),
                ExecutionResourceRequirements::unknown(FrozenEstimateUnknownReason::NotProjected),
            )
            .unwrap(),
        )
    }

    fn bind_no_recovery(
        request: LogicalNativeOpenRequest,
    ) -> Result<LogicalNativeSession, NativeExecutionContractError> {
        request.bind()
    }

    fn issue_with_seed(
        execution: QueryExecutionId,
        description: Arc<FrozenExecutionDescription>,
        seed: NativeLogicalExecutionSeed,
    ) -> (
        Governance,
        LogicalNativeOpenRequest,
        LogicalNativeOpenAcceptance,
    ) {
        let governance = Governance::new();
        let scope = governance.scope();
        let (request, acceptance) = LogicalNativeOpenRequest::issue(
            execution,
            description,
            scope.id(),
            scope.cancellation().unwrap(),
            seed,
        );
        (governance, request, acceptance)
    }

    #[derive(Debug)]
    struct BackpressuredReplacementPort {
        capacity: watch::Sender<u64>,
    }

    impl BackpressuredReplacementPort {
        fn shared() -> Arc<dyn ReplacementQualificationEffectPort> {
            let (capacity, _) = watch::channel(0);
            Arc::new(Self { capacity })
        }
    }

    impl ReplacementQualificationEffectPort for BackpressuredReplacementPort {
        fn subscribe_capacity(&self) -> watch::Receiver<u64> {
            self.capacity.subscribe()
        }

        fn try_reserve(
            &self,
            _request: &ReplacementQualificationRequest,
        ) -> ReplacementQualificationEffectAdmission {
            ReplacementQualificationEffectAdmission::Backpressured
        }
    }

    fn prepared_inputs() -> Vec<BackendProcessId> {
        let backend = BackendProcessId::try_from_bytes(process_bytes(2)).unwrap();
        vec![backend]
    }

    #[test]
    fn open_acceptance_rejects_a_session_from_another_request() {
        let (_first_governance, first, first_acceptance) =
            issue(execution(1, 1), RecoveryMode::NoRecovery);
        let (_second_governance, second, _) = issue(execution(2, 1), RecoveryMode::NoRecovery);
        let session = bind_no_recovery(second).unwrap();
        assert!(matches!(
            first_acceptance.accept(session),
            Err(NativeExecutionContractError::ForeignLogicalOpenTicket)
        ));
        drop(first);
    }

    #[test]
    fn executable_request_rejects_a_cross_seal_seed_for_identical_no_scan_plans() {
        let first_description = description(RecoveryMode::NoRecovery);
        let second_description = description(RecoveryMode::NoRecovery);
        let foreign_seed = NativeLogicalExecutionSeed::issue(
            second_description.plan_identity(),
            PermanentlyBackpressuredAbortEffectPort::shared(),
            None,
            NoopPreparationPort,
        );
        assert!(matches!(
            crate::api::QueryExecutionRequest::try_from_parts(
                first_description.as_ref().clone(),
                foreign_seed
            ),
            Err(NativeExecutionContractError::ForeignLogicalSeedPlan)
        ));
    }

    #[test]
    fn attempt_request_matches_only_its_exact_no_scan_template_seal() {
        let first = description(RecoveryMode::NoRecovery);
        let second = description(RecoveryMode::NoRecovery);
        assert_ne!(first.plan_identity(), second.plan_identity());
        let (_governance, open, acceptance) =
            issue_description(execution(21, 1), Arc::clone(&first));
        let session = acceptance.accept(open.bind().unwrap()).unwrap();
        let (request, _) = session.issue_attempt(execution(21, 1)).unwrap();

        assert!(request.matches_plan_seal(first.plan_identity()));
        assert!(!request.matches_plan_seal(second.plan_identity()));
    }

    #[test]
    fn attempt_acceptance_rejects_cross_request_splicing() {
        let (_governance, open, open_acceptance) = issue(execution(1, 1), RecoveryMode::NoRecovery);
        let session = open_acceptance
            .accept(bind_no_recovery(open).unwrap())
            .unwrap();
        let (first, first_acceptance) = session.issue_attempt(execution(1, 1)).unwrap();
        let (second, _) = session.issue_attempt(execution(1, 2)).unwrap();
        let backends = prepared_inputs();
        let prepared = second
            .bind(scheduling_over(&[]), Dormant::new(2, backends))
            .unwrap();
        assert!(matches!(
            first_acceptance.accept(prepared),
            Err(NativeExecutionContractError::ForeignAttemptTicket)
        ));
        drop(first);
    }

    #[test]
    fn accepted_attempt_preserves_exact_inputs_without_accepting_a_manifest() {
        let (_governance, open, open_acceptance) = issue(execution(1, 1), RecoveryMode::NoRecovery);
        let session = open_acceptance
            .accept(bind_no_recovery(open).unwrap())
            .unwrap();
        let exact_execution = execution(1, 2);
        let (request, acceptance) = session.issue_attempt(exact_execution).unwrap();
        assert!(std::ptr::eq(
            request.description(),
            session.ticket.description.as_ref()
        ));
        assert_eq!(request.work_id(), session.ticket.work_id);
        let backends = prepared_inputs();
        let dormant = Dormant::new(7, backends.clone());
        assert_eq!(dormant.tag, 7);
        let prepared = request.bind(scheduling_over(&[]), dormant).unwrap();
        let parts = acceptance.accept(prepared).unwrap();
        assert_eq!(parts.execution, exact_execution);
        assert_eq!(parts.eligible_backends.as_ref(), backends);
        assert!(
            parts
                .scheduling
                .fragments
                .iter()
                .all(|fragment| fragment.scans.is_empty())
        );
        assert!(format!("{:?}", parts.owner).contains("tag: 7"));
    }

    #[tokio::test]
    async fn mutable_attempt_port_preserves_work_description_and_exact_ticket() {
        let aborts = PermanentlyBackpressuredAbortEffectPort::shared();
        let replacements = BackpressuredReplacementPort::shared();
        let description = description(RecoveryMode::RestartAttemptBeforeVisibility);
        let (_governance, open, open_acceptance) = issue_with_seed(
            execution(1, 1),
            Arc::clone(&description),
            seed_for_description(
                description.as_ref(),
                Arc::clone(&aborts),
                Some(Arc::clone(&replacements)),
                EchoPreparationPort { calls: 0 },
            ),
        );
        let mut session = open_acceptance.accept(open.bind().unwrap()).unwrap();
        assert!(Arc::ptr_eq(&session.abort_effect_port(), &aborts));
        assert!(Arc::ptr_eq(
            &session.replacement_effect_port().unwrap(),
            &replacements
        ));

        let exact_execution = execution(1, 2);
        let (request, acceptance) = session.issue_attempt(exact_execution).unwrap();
        assert_eq!(request.work_id(), session.ticket.work_id);
        let prepared = session.prepare(request).await.unwrap();
        let parts = acceptance.accept(prepared).unwrap();
        assert_eq!(parts.execution, exact_execution);
        assert_eq!(parts.eligible_backends.as_ref(), prepared_inputs());
    }

    #[test]
    fn session_bind_requires_the_recovery_modes_exact_port_shape() {
        let no_recovery_description = description(RecoveryMode::NoRecovery);
        let (_governance, no_recovery, _) = issue_with_seed(
            execution(1, 1),
            Arc::clone(&no_recovery_description),
            seed_for_description(
                no_recovery_description.as_ref(),
                PermanentlyBackpressuredAbortEffectPort::shared(),
                Some(BackpressuredReplacementPort::shared()),
                NoopPreparationPort,
            ),
        );
        assert!(matches!(
            no_recovery.bind(),
            Err(NativeExecutionContractError::UnexpectedReplacementPort)
        ));

        let recoverable_description = description(RecoveryMode::RestartAttemptBeforeVisibility);
        let (_governance, recoverable, _) = issue_with_seed(
            execution(2, 1),
            Arc::clone(&recoverable_description),
            seed_for_description(
                recoverable_description.as_ref(),
                PermanentlyBackpressuredAbortEffectPort::shared(),
                None,
                NoopPreparationPort,
            ),
        );
        assert!(matches!(
            recoverable.bind(),
            Err(NativeExecutionContractError::MissingReplacementPort)
        ));
    }

    #[test]
    fn session_rejects_an_attempt_from_another_logical_query() {
        let (_governance, open, acceptance) = issue(execution(1, 1), RecoveryMode::NoRecovery);
        let session = acceptance.accept(bind_no_recovery(open).unwrap()).unwrap();
        assert!(matches!(
            session.issue_attempt(execution(2, 1)),
            Err(NativeExecutionContractError::DifferentLogicalQuery)
        ));
    }

    #[test]
    fn runtime_failures_preserve_open_outcome_and_attempt_classification() {
        let open = LogicalNativeOpenFailure::deadline_exceeded("open timed out");
        assert_eq!(
            open.error().kind(),
            QueryExecutionErrorKind::DeadlineExceeded
        );
        assert_eq!(open.error().message(), "open timed out");

        let backend = prepared_inputs()[0];
        let input = ExecutionFailureInput::new(
            QueryFailureReason::BackendLost.failure_fact(),
            SafeDetail::truncating("worker disappeared"),
            ExecutionFailureSite::Backend { process: backend },
        );
        let attempt = NativeAttemptPreparationFailure::from_input(input.clone());
        assert_eq!(attempt.input(), &input);
        assert_eq!(
            attempt.topology_requirement(),
            NativeAttemptTopologyRequirement::ExcludeProcess(backend),
        );
        assert_eq!(
            attempt.class(),
            AttemptFailureClass::RecoverableInfrastructure
        );
        assert_eq!(attempt.error().kind(), QueryExecutionErrorKind::Failed);
        assert_eq!(attempt.error().message(), "worker disappeared");
    }

    #[test]
    fn native_contract_facts_are_authored_by_the_closed_binding_domain() {
        use novarocks_execution_contract::failure::{FailureClass, FailureEvidence, domain};
        for (error, code) in [
            (NativeExecutionContractError::ForeignLogicalSeedPlan, 110),
            (NativeExecutionContractError::ForeignLogicalOpenTicket, 111),
            (NativeExecutionContractError::ForeignAttemptTicket, 112),
            (NativeExecutionContractError::DifferentLogicalQuery, 113),
            (NativeExecutionContractError::EmptyEligibleBackends, 114),
            (NativeExecutionContractError::DuplicateEligibleBackend, 115),
            (NativeExecutionContractError::ExcludedBackendProcess, 116),
            (NativeExecutionContractError::MissingScanWorkFact, 117),
            (NativeExecutionContractError::DuplicateScanWorkFact, 118),
            (NativeExecutionContractError::ForeignScanWorkFact, 119),
            (NativeExecutionContractError::MissingReplacementPort, 120),
            (NativeExecutionContractError::UnexpectedReplacementPort, 121),
            (NativeExecutionContractError::AttemptPreparationRetired, 122),
        ] {
            let fact = error.failure_fact();
            assert_eq!(fact.reason().domain(), domain::QUERY.get());
            assert_eq!(fact.reason().code(), code);
            assert_eq!(fact.resource(), None);
            assert_eq!(fact.representation(), None);
            if error == NativeExecutionContractError::AttemptPreparationRetired {
                assert_eq!(fact.class(), FailureClass::Control);
                assert_eq!(fact.evidence(), FailureEvidence::StopObservation);
            } else {
                assert_eq!(fact.class(), FailureClass::Invariant);
                assert_eq!(fact.evidence(), FailureEvidence::Originating);
            }
            assert_eq!(error.safe_detail().as_str(), error.to_string());
        }
    }

    #[test]
    fn prepared_attempt_rejects_empty_and_duplicate_eligible_backends() {
        let exact = execution(1, 1);
        let (_governance, open, acceptance) = issue(exact, RecoveryMode::NoRecovery);
        let session = acceptance.accept(bind_no_recovery(open).unwrap()).unwrap();

        let backends = prepared_inputs();
        let (request, _) = session.issue_attempt(exact).unwrap();
        assert!(matches!(
            request.bind(scheduling_over(&[]), Dormant::new(1, Vec::new())),
            Err(NativeExecutionContractError::EmptyEligibleBackends)
        ));

        let (request, _) = session.issue_attempt(exact).unwrap();
        assert!(matches!(
            request.bind(
                scheduling_over(&[]),
                Dormant::new(1, vec![backends[0], backends[0]]),
            ),
            Err(NativeExecutionContractError::DuplicateEligibleBackend)
        ));
    }

    #[test]
    fn prepare_binding_rejects_excluded_process_and_retains_the_original_funded_owner() {
        use novarocks_workload_control::{AllocationCharge, LocalResourceAuthority, ResourceClass};

        struct FundedDormant {
            backend: BackendProcessId,
            _data: Arc<Vec<u8>>,
            _charge: AllocationCharge,
        }
        impl fmt::Debug for FundedDormant {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_struct("FundedDormant")
                    .field("backend", &self.backend)
                    .finish_non_exhaustive()
            }
        }
        impl DormantNativeAttemptOwner for FundedDormant {
            fn poll_bind_qualification(
                &mut self,
                _schedule: &crate::coordination::AttemptSchedule,
                _cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Result<(), crate::api::NativeAttemptActivationFailure>>
            {
                // This fixture owns no external dependency or qualification transport.
                std::task::Poll::Ready(Ok(()))
            }

            fn eligible_backends(&self) -> &[BackendProcessId] {
                std::slice::from_ref(&self.backend)
            }
            fn activate<'a>(
                &'a mut self,
                _schedule: &'a AttemptSchedule,
                _replacement_admissions: Option<Box<[ReplacementWorkerAdmissionEvidence]>>,
                _cancellation: CancellationView,
            ) -> NativeAttemptActivationFuture<'a> {
                Box::pin(async { panic!("binding-only fixture must not activate") })
            }
            fn converge<'a>(
                &'a mut self,
                _cancellation: CancellationView,
            ) -> NativeAttemptConvergenceFuture<'a> {
                Box::pin(async { Ok(()) })
            }
        }
        fn funded(
            backend: BackendProcessId,
            resources: &LocalResourceAuthority,
            scope: &WorkScope,
        ) -> (FundedDormant, std::sync::Weak<Vec<u8>>) {
            let mut reservation = resources.reserve(scope, 64, ResourceClass::Data).unwrap();
            let charge = reservation.charge(64).unwrap();
            let data = Arc::new(vec![0x5a; 64]);
            let weak = Arc::downgrade(&data);
            (
                FundedDormant {
                    backend,
                    _data: data,
                    _charge: charge,
                },
                weak,
            )
        }

        // Both original fixture identities exist before any refusal or failure.
        let excluded = prepared_inputs()[0];
        let alternate = BackendProcessId::try_from_bytes(process_bytes(3)).unwrap();
        let exact = execution(1, 1);
        let (governance, open, acceptance) = issue(exact, RecoveryMode::NoRecovery);
        let scope = governance.scope();
        let resources = governance._control.resources().unwrap();
        let session = acceptance.accept(bind_no_recovery(open).unwrap()).unwrap();
        let (request, _) = session.issue_attempt(exact).unwrap();
        let (bad_owner, bad_data) = funded(excluded, &resources, &scope);
        assert_eq!(resources.snapshot().data_used_bytes, 64);
        let error = match request
            .require_topology(NativeAttemptTopologyRequirement::ExcludeProcess(excluded))
            .bind(scheduling_over(&[]), bad_owner)
        {
            Err(error) => error,
            Ok(_) => panic!("the exact excluded process must be refused"),
        };
        assert_eq!(error, NativeExecutionContractError::ExcludedBackendProcess);
        assert_eq!(error.failure_fact().reason().code(), 116);
        assert_eq!(
            error.failure_fact().class(),
            novarocks_execution_contract::failure::FailureClass::Invariant
        );
        assert_eq!(error.failure_fact().resource(), None);
        assert_eq!(
            error.safe_detail().as_str(),
            "Native attempt includes the backend process excluded by its prepare request"
        );
        assert!(bad_data.upgrade().is_none());
        assert_eq!(resources.snapshot().data_used_bytes, 0);

        let (request, accepted) = session.issue_attempt(exact).unwrap();
        let (owner, data) = funded(alternate, &resources, &scope);
        let prepared = request
            .require_topology(NativeAttemptTopologyRequirement::ExcludeProcess(excluded))
            .bind(scheduling_over(&[]), owner)
            .unwrap();
        assert_eq!(resources.snapshot().data_used_bytes, 64);
        assert_eq!(data.upgrade().unwrap().as_slice(), &[0x5a; 64]);
        let prepared = accepted.accept(prepared).unwrap();
        assert_eq!(prepared.eligible_backends.as_ref(), &[alternate]);
        assert_eq!(resources.snapshot().data_used_bytes, 64);
        assert!(data.upgrade().is_some());
        drop(prepared);
        assert!(data.upgrade().is_none());
        assert_eq!(resources.snapshot().data_used_bytes, 0);
    }

    /// Scheduling facts naming exactly these scans, with one fragment per
    /// scan. Nothing here schedules; the cover is what is under test.
    fn scheduling_over(scans: &[(PlanScanIdentity, NativeScanWork)]) -> ExecutionSchedulingFacts {
        ExecutionSchedulingFacts {
            topological_fragment_order: vec![1],
            execution_anchor_fragment_id: 1,
            fragments: vec![FragmentSchedulingFacts {
                fragment_id: 1,
                placement: novarocks_physical_plan::StagePlacementPolicy::Singleton,
                scans: scans
                    .iter()
                    .map(|&(scan, work)| ScanSchedulingFacts { scan, work })
                    .collect(),
            }],
            edges: Vec::new(),
        }
    }

    #[test]
    fn prepared_attempt_requires_exact_sealed_scan_work_cover() {
        let exact = execution(7, 1);
        let description = scan_description();
        let expected_scan = description.scan_identities()[0];
        let (_governance, open, acceptance) = issue_description(exact, description);
        let session = acceptance.accept(bind_no_recovery(open).unwrap()).unwrap();
        let backend = prepared_inputs();

        let (missing, _) = session.issue_attempt(exact).unwrap();
        assert!(matches!(
            missing.bind(scheduling_over(&[]), Dormant::new(1, backend.clone())),
            Err(NativeExecutionContractError::MissingScanWorkFact)
        ));

        let fact = (
            expected_scan,
            NativeScanWork::RuntimeSplits {
                placement: RuntimeSplitPlacement::Spread,
            },
        );
        let (duplicate, _) = session.issue_attempt(exact).unwrap();
        assert!(matches!(
            duplicate.bind(
                scheduling_over(&[fact, fact]),
                Dormant::new(1, backend.clone())
            ),
            Err(NativeExecutionContractError::DuplicateScanWorkFact)
        ));

        let foreign_description = scan_description();
        let foreign = (
            foreign_description.scan_identities()[0],
            NativeScanWork::RuntimeSplits {
                placement: RuntimeSplitPlacement::SingleUnit,
            },
        );
        let (foreign_request, _) = session.issue_attempt(exact).unwrap();
        assert!(matches!(
            foreign_request.bind(scheduling_over(&[foreign]), Dormant::new(1, backend)),
            Err(NativeExecutionContractError::ForeignScanWorkFact)
        ));
    }

    #[derive(Debug)]
    struct NoopPreparationPort;

    #[tokio::test]
    async fn retired_preparation_refuses_new_and_previously_issued_tickets() {
        use std::sync::atomic::{AtomicU64, Ordering};

        #[derive(Debug)]
        struct ObservedPreparationPort {
            calls: Arc<AtomicU64>,
            exits: Arc<AtomicU64>,
        }
        impl NativeAttemptPreparationPort for ObservedPreparationPort {
            fn prepare_in<'a>(
                &'a mut self,
                _request: &'a mut crate::api::NativeAttemptPreparationWork,
            ) -> NativeAttemptPreparationFuture<'a> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { Err(NativeExecutionContractError::ForeignAttemptTicket.into()) })
            }
        }
        impl Drop for ObservedPreparationPort {
            fn drop(&mut self) {
                self.exits.fetch_add(1, Ordering::SeqCst);
            }
        }

        let calls = Arc::new(AtomicU64::new(0));
        let exits = Arc::new(AtomicU64::new(0));
        let description = description(RecoveryMode::NoRecovery);
        let seed = seed_for_description(
            &description,
            PermanentlyBackpressuredAbortEffectPort::shared(),
            None,
            ObservedPreparationPort {
                calls: Arc::clone(&calls),
                exits: Arc::clone(&exits),
            },
        );
        let (_governance, open, acceptance) = issue_with_seed(execution(1, 1), description, seed);
        let mut session = acceptance.accept(open.bind().unwrap()).unwrap();
        let (old_request, _) = session.issue_attempt(execution(1, 1)).unwrap();

        session.retire_preparation();
        session.retire_preparation();
        assert_eq!(exits.load(Ordering::SeqCst), 1);
        let retired = match session.issue_attempt(execution(1, 2)) {
            Err(error) => error,
            Ok(_) => panic!("retired preparation cannot issue a new request"),
        };
        assert_eq!(
            retired,
            NativeExecutionContractError::AttemptPreparationRetired
        );
        assert_eq!(
            retired.failure_fact().evidence(),
            novarocks_execution_contract::failure::FailureEvidence::StopObservation
        );
        assert_eq!(retired.failure_fact().reason().code(), 122);
        assert!(matches!(
            session.prepare(old_request).await,
            Err(NativeAttemptPreparationError::Contract(
                NativeExecutionContractError::AttemptPreparationRetired
            ))
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    impl NativeAttemptPreparationPort for NoopPreparationPort {
        fn prepare_in<'a>(
            &'a mut self,
            _request: &'a mut crate::api::NativeAttemptPreparationWork,
        ) -> NativeAttemptPreparationFuture<'a> {
            Box::pin(async { Err(NativeExecutionContractError::ForeignAttemptTicket.into()) })
        }
    }

    #[derive(Debug)]
    struct EchoPreparationPort {
        calls: usize,
    }

    impl NativeAttemptPreparationPort for EchoPreparationPort {
        fn prepare_in<'a>(
            &'a mut self,
            request: &'a mut crate::api::NativeAttemptPreparationWork,
        ) -> NativeAttemptPreparationFuture<'a> {
            self.calls += 1;
            assert_eq!(self.calls, 1);
            let _exact_execution = request.execution();
            let backends = prepared_inputs();
            request.stage_fixture_owner(Dormant::new(9, backends));
            Box::pin(async move {
                request
                    .bind_staged(scheduling_over(&[]))
                    .map_err(Into::into)
            })
        }
    }

    #[derive(Debug)]
    struct RetryableBorrowedConvergence {
        polls: usize,
    }

    impl DormantNativeAttemptOwner for RetryableBorrowedConvergence {
        fn poll_bind_qualification(
            &mut self,
            _schedule: &crate::coordination::AttemptSchedule,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), crate::api::NativeAttemptActivationFailure>> {
            // This fixture owns no external dependency or qualification transport.
            std::task::Poll::Ready(Ok(()))
        }

        fn eligible_backends(&self) -> &[BackendProcessId] {
            &[]
        }

        fn activate<'a>(
            &'a mut self,
            _schedule: &'a AttemptSchedule,
            _replacement_admissions: Option<Box<[ReplacementWorkerAdmissionEvidence]>>,
            _cancellation: CancellationView,
        ) -> NativeAttemptActivationFuture<'a> {
            Box::pin(async { panic!("test owner is never activated") })
        }

        fn converge<'a>(
            &'a mut self,
            _cancellation: CancellationView,
        ) -> NativeAttemptConvergenceFuture<'a> {
            Box::pin(async move {
                self.polls += 1;
                if self.polls == 1 {
                    std::future::pending::<()>().await;
                }
                Ok(())
            })
        }
    }

    #[tokio::test]
    async fn cancelling_a_convergence_future_retains_the_owner_for_retry() {
        let governance = Governance::new();
        let cancellation = governance.scope().cancellation().unwrap();
        let mut owner = RetryableBorrowedConvergence { polls: 0 };
        let mut first = owner.converge(cancellation.clone());

        tokio::select! {
            biased;
            _ = &mut first => panic!("first convergence must remain pending"),
            _ = tokio::task::yield_now() => {}
        }
        drop(first);
        assert_eq!(owner.polls, 1);

        owner.converge(cancellation).await.unwrap();
        assert_eq!(owner.polls, 2);
    }

    #[test]
    fn silent_open_refusal_keeps_the_same_original_input_and_empty_target() {
        let (_governance, open, _acceptance) = issue(execution(301, 1), RecoveryMode::NoRecovery);
        let mut source = Some(open);
        let original = Arc::as_ptr(&source.as_ref().unwrap().ticket);
        let mut target = None;
        LogicalNativeOpenRequest::bind_in(&mut source, &mut target).unwrap();
        assert!(source.is_none());
        assert_eq!(Arc::as_ptr(&target.as_ref().unwrap().ticket), original);
        let (_other_governance, other, _other_acceptance) =
            issue(execution(302, 1), RecoveryMode::NoRecovery);
        let mut source = Some(other);
        let before = Arc::as_ptr(&source.as_ref().unwrap().ticket);
        assert!(LogicalNativeOpenRequest::bind_in(&mut source, &mut target).is_err());
        assert_eq!(Arc::as_ptr(&source.as_ref().unwrap().ticket), before);
        assert_eq!(Arc::as_ptr(&target.as_ref().unwrap().ticket), original);
    }

    #[test]
    fn silent_attempt_validation_refusal_retains_original_request_owner_and_schedule() {
        let (_governance, open, acceptance) = issue(execution(303, 1), RecoveryMode::NoRecovery);
        let session = acceptance.accept(open.bind().unwrap()).unwrap();
        let (request, _) = session.issue_attempt(execution(303, 1)).unwrap();
        let ticket = Arc::as_ptr(&request.attempt_ticket);
        let mut work = NativeAttemptPreparationWork::new(request);
        let mut owner: Option<Box<dyn DormantNativeAttemptOwner>> =
            Some(Box::new(Dormant::new(8, vec![])));
        let original: *const dyn DormantNativeAttemptOwner = owner.as_ref().unwrap().as_ref();
        let mut schedule = Some(scheduling_over(&[]));
        assert!(matches!(
            work.bind_in(&mut schedule, &mut owner),
            Err(NativeExecutionContractError::EmptyEligibleBackends)
        ));
        assert!(work.prepared.is_none() && schedule.is_some());
        assert_eq!(
            Arc::as_ptr(&work.request.as_ref().unwrap().attempt_ticket),
            ticket
        );
        assert!(std::ptr::addr_eq(
            owner.as_ref().unwrap().as_ref() as *const dyn DormantNativeAttemptOwner,
            original
        ));
    }
}

/// Shared typed fixture for the QA carrier, pump, and recovery boundary tests.
#[cfg(test)]
pub(crate) fn pre_ready_identity_refusal_test_fixture(
    context: novarocks_execution_contract::QueryContextRef,
) -> (
    crate::execution_error::ExecutionFailureInput,
    PreReadyProcessReplacementWitness,
    NativeAttemptPreparationFailure,
) {
    pre_ready_identity_refusal_test_fixture_for_kind(
        context,
        novarocks_execution_contract::OperationKind::UpdateQueryContext,
    )
}

#[cfg(test)]
pub(crate) fn pre_ready_identity_refusal_test_fixture_for_kind(
    context: novarocks_execution_contract::QueryContextRef,
    kind: novarocks_execution_contract::OperationKind,
) -> (
    crate::execution_error::ExecutionFailureInput,
    PreReadyProcessReplacementWitness,
    NativeAttemptPreparationFailure,
) {
    use crate::execution_error::{
        ExecutionFailureInput, ExecutionFailureSite, TASK_PROTOCOL_IDENTITY_MISMATCH_REASON,
    };
    use novarocks_execution_contract::{SafeDetail, TaskOperationId};
    use novarocks_failure_contract::{FailureClass, FailureFact};
    let operation_id = TaskOperationId::new_v7();
    let input = ExecutionFailureInput::new(
        FailureFact::new(
            FailureClass::Invariant,
            TASK_PROTOCOL_IDENTITY_MISMATCH_REASON,
        ),
        SafeDetail::truncating("backend process id mismatch"),
        ExecutionFailureSite::Operation {
            context,
            operation_id,
            kind,
        },
    );
    let witness = PreReadyProcessReplacementWitness::try_new(
        &input,
        context,
        operation_id,
        BackendProcessId::new_v7(),
    )
    .unwrap();
    let mut failure = NativeAttemptPreparationFailure::from_input(input.clone());
    failure
        .attach_pre_ready_replacement(witness.clone())
        .unwrap();
    (input, witness, failure)
}

#[cfg(test)]
mod pre_ready_replacement_evidence_tests {
    use super::*;
    #[test]
    fn exact_replacement_evidence_keeps_original_refusal_and_rejects_foreign_input() {
        use crate::execution_error::{
            ExecutionFailureInput, ExecutionFailureSite, QueryFailureReason,
        };
        use novarocks_execution_contract::{
            QueryContextRef, SafeDetail, failure::FailureProjection,
        };
        use novarocks_types::{AttemptId, FrontendProcessId, QueryId};
        let execution =
            QueryExecutionId::new(QueryId::new(29, 31), AttemptId::new(1).unwrap()).unwrap();
        let context = QueryContextRef::new(
            execution,
            FrontendProcessId::new_v7(),
            BackendProcessId::new_v7(),
        );
        let (input, witness, failure) = pre_ready_identity_refusal_test_fixture(context);
        assert_eq!(failure.input(), &input);
        assert_eq!(failure.class(), AttemptFailureClass::ContractViolation);
        assert_eq!(failure.error().execution_failure(), Some(&input));
        assert_eq!(failure.pre_ready_replacement(), Some(&witness));
        assert_eq!(
            failure.topology_requirement(),
            NativeAttemptTopologyRequirement::ExcludeProcess(context.backend_process_id())
        );
        assert!(
            PreReadyProcessReplacementWitness::try_new(
                &input,
                context,
                witness.operation_id(),
                context.backend_process_id()
            )
            .is_err()
        );
        assert!(
            PreReadyProcessReplacementWitness::try_new(
                &input,
                context,
                novarocks_execution_contract::TaskOperationId::new_v7(),
                witness.replacement_process()
            )
            .is_err()
        );
        let foreign = ExecutionFailureInput::new(
            input.fact(),
            input.detail().clone(),
            ExecutionFailureSite::Backend {
                process: context.backend_process_id(),
            },
        );
        assert!(
            PreReadyProcessReplacementWitness::try_new(
                &foreign,
                context,
                witness.operation_id(),
                witness.replacement_process()
            )
            .is_err()
        );
        let ordinary = ExecutionFailureInput::new(
            QueryFailureReason::ResultProtocol.failure_fact(),
            SafeDetail::truncating("another protocol failure"),
            ExecutionFailureSite::Coordinator,
        );
        let mut ordinary_failure = NativeAttemptPreparationFailure::from_input(ordinary.clone());
        assert!(
            ordinary_failure
                .attach_pre_ready_replacement(witness)
                .is_err()
        );
        assert_eq!(ordinary_failure.input(), &ordinary);
        assert!(ordinary_failure.pre_ready_replacement().is_none());
    }
    #[test]
    fn create_replacement_witness_preserves_kind_and_rejects_cross_kind_input() {
        use crate::execution_error::{ExecutionFailureInput, ExecutionFailureSite};
        use novarocks_execution_contract::{OperationKind, QueryContextRef};
        let execution = QueryExecutionId::new(
            novarocks_types::QueryId::new(29, 33),
            novarocks_types::AttemptId::new(1).unwrap(),
        )
        .unwrap();
        let context = QueryContextRef::new(
            execution,
            novarocks_types::FrontendProcessId::new_v7(),
            BackendProcessId::new_v7(),
        );
        let (input, witness, failure) =
            pre_ready_identity_refusal_test_fixture_for_kind(context, OperationKind::CreateTask);
        assert_eq!(failure.input(), &input);
        assert_eq!(failure.class(), AttemptFailureClass::ContractViolation);
        assert_eq!(
            PreReadyProcessReplacementWitness::refusal_operation(&input),
            Some((context, witness.operation_id(), OperationKind::CreateTask))
        );
        let other = ExecutionFailureInput::new(
            input.fact(),
            input.detail().clone(),
            ExecutionFailureSite::Operation {
                context,
                operation_id: witness.operation_id(),
                kind: OperationKind::UpdateQueryContext,
            },
        );
        assert!(!witness.matches_input(&other));
        let unrelated = ExecutionFailureInput::new(
            input.fact(),
            input.detail().clone(),
            ExecutionFailureSite::Operation {
                context,
                operation_id: witness.operation_id(),
                kind: OperationKind::UpdateTask,
            },
        );
        assert!(
            PreReadyProcessReplacementWitness::try_new(
                &unrelated,
                context,
                witness.operation_id(),
                witness.replacement_process()
            )
            .is_err()
        );
    }
}
