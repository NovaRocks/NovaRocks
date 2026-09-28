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
use super::{QueryExecutionError, QueryExecutionErrorKind};
use crate::coordination::{
    AbortQueryContextEffectPort, AcceptedRootStatusSource, AttemptFailureClass, AttemptSchedule,
    NativeAttemptDrive, RecoveryMode, ReplacementQualificationEffectPort,
    ReplacementWorkerAdmissionEvidence, RootResultPumpBinding,
};
use crate::preparation::FrozenExecutionDescription;

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

/// Runtime failure while preparing one physical attempt.
///
/// Coordination consumes the attempt failure class for recovery policy and
/// retains the query error for the caller-visible terminal result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeAttemptPreparationFailure {
    topology_requirement: NativeAttemptTopologyRequirement,
    class: AttemptFailureClass,
    error: QueryExecutionError,
}

impl NativeAttemptPreparationFailure {
    pub const fn new(class: AttemptFailureClass, error: QueryExecutionError) -> Self {
        Self {
            class,
            error,
            topology_requirement: NativeAttemptTopologyRequirement::LiveSnapshot,
        }
    }

    pub const fn with_topology_requirement(
        mut self,
        requirement: NativeAttemptTopologyRequirement,
    ) -> Self {
        self.topology_requirement = requirement;
        self
    }

    pub const fn topology_requirement(&self) -> NativeAttemptTopologyRequirement {
        self.topology_requirement
    }

    pub const fn class(&self) -> AttemptFailureClass {
        self.class
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

/// Opening one role-local Native execution may perform bounded asynchronous
/// setup, but it cannot change the frozen logical description.
pub type LogicalNativeOpenFuture = Pin<
    Box<dyn Future<Output = Result<LogicalNativeSession, LogicalNativeOpenError>> + Send + 'static>,
>;

/// Role-composed entry to the Native execution application.
pub trait LogicalExecutionNativePort: fmt::Debug + Send + Sync + 'static {
    fn open(&self, request: LogicalNativeOpenRequest) -> LogicalNativeOpenFuture;
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
pub type NativeAttemptPreparationFuture = Pin<
    Box<
        dyn Future<Output = Result<PreparedNativeAttempt, NativeAttemptPreparationError>>
            + Send
            + 'static,
    >,
>;

/// Per-logical-execution Native attempt preparation.
pub trait NativeAttemptPreparationPort: fmt::Debug + Send + 'static {
    fn prepare(
        &mut self,
        request: NativeAttemptPreparationRequest,
    ) -> NativeAttemptPreparationFuture;
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
/// with its supervisor, and a later call must resume the same idempotent
/// convergence.
pub type NativeAttemptConvergenceFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

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
/// positive closure evidence for every scheduled context. Dropping the future
/// or catching a poll panic retains the exact owner for a later idempotent
/// retry.
pub type NativeActiveAttemptConvergenceFuture<'a> =
    Pin<Box<dyn Future<Output = NativeAttemptConvergence> + Send + 'a>>;

/// Move-only role owner for a prepared attempt that has not started any Task.
///
/// Before binding, the owner has no asynchronous convergence responsibility.
/// The query supervisor calls `activate` only after the logical actor is
/// atomically installed. The borrowed future leaves this owner in the
/// supervisor even when polling panics; the supervisor then consumes it
/// through `converge`. Success transfers all asynchronous responsibility to
/// the returned active owner and leaves the dormant owner synchronously
/// drop-safe.
pub trait DormantNativeAttemptOwner: fmt::Debug + Send + 'static {
    /// Exact process identities from the role-local topology snapshot retained
    /// by this dormant owner. Query Application derives placement only from
    /// this view, so activation cannot substitute a later snapshot while
    /// keeping the already-issued schedule.
    fn eligible_backends(&self) -> &[BackendProcessId];

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
/// Query Application retains the owner while the borrowed run future is
/// polled. A terminal outcome or panic is followed by consuming `converge`.
pub trait ActiveNativeAttemptOwner: fmt::Debug + Send + 'static {
    fn run<'a>(
        &'a mut self,
        drive: &'a NativeAttemptDrive,
        cancellation: CancellationView,
    ) -> NativeAttemptRunFuture<'a>;

    fn converge<'a>(
        &'a mut self,
        cancellation: CancellationView,
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

    pub fn rows(
        owner: impl ActiveNativeAttemptOwner,
        binding: RootResultPumpBinding,
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
    pub(crate) binding: RootResultPumpBinding,
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
    MissingScanWorkFact,
    DuplicateScanWorkFact,
    ForeignScanWorkFact,
    MissingReplacementPort,
    UnexpectedReplacementPort,
}

impl fmt::Display for NativeExecutionContractError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
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
            Self::EmptyEligibleBackends => "Native attempt has no eligible backend process",
            Self::DuplicateEligibleBackend => {
                "Native attempt repeats an eligible backend process identity"
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
        };
        formatter.write_str(message)
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
            attempts: self.seed.attempts,
        })
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
    attempts: Box<dyn NativeAttemptPreparationPort>,
}

#[derive(Debug)]
pub(crate) struct LogicalNativeOpenAcceptance {
    ticket: Arc<LogicalNativeTicket>,
}

impl LogicalNativeOpenAcceptance {
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
            },
            NativeAttemptPreparationAcceptance::new(&self.ticket, attempt_ticket, execution),
        ))
    }

    pub(crate) fn prepare(
        &mut self,
        request: NativeAttemptPreparationRequest,
    ) -> NativeAttemptPreparationFuture {
        self.attempts.prepare(request)
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

/// Native-owned work cardinality for one exact sealed scan occurrence.
///
/// This is an input to Query Application scheduling, not a placement result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeScanWork {
    /// The exact static work set is empty. The containing fragment still
    /// retains one Task so its non-scan operators and exchange obligations
    /// have an execution owner.
    Empty,
    RuntimeSplits,
    WholeRelation,
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
        ExecutionEffect, PermanentlyBackpressuredAbortEffectPort,
        ReplacementQualificationEffectAdmission, ReplacementQualificationRequest,
    };
    use crate::preparation::{
        ExecutionResourceRequirements, FrozenCostEstimate, FrozenEstimateUnknownReason,
        OutputContract,
    };
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
            _cancellation: CancellationView,
        ) -> NativeActiveAttemptConvergenceFuture<'a> {
            Box::pin(async { NativeAttemptConvergence::all_workers_stopped_and_contexts_fenced() })
        }
    }

    impl DormantNativeAttemptOwner for Dormant {
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
            Box::pin(async {})
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

        let attempt = NativeAttemptPreparationFailure::new(
            AttemptFailureClass::RecoverableInfrastructure,
            QueryExecutionError::new(QueryExecutionErrorKind::Failed, "worker disappeared"),
        );
        assert_eq!(
            attempt.class(),
            AttemptFailureClass::RecoverableInfrastructure
        );
        assert_eq!(attempt.error().kind(), QueryExecutionErrorKind::Failed);
        assert_eq!(attempt.error().message(), "worker disappeared");
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

    /// Scheduling facts naming exactly these scans, with one fragment per
    /// scan. Nothing here schedules; the cover is what is under test.
    fn scheduling_over(scans: &[(PlanScanIdentity, NativeScanWork)]) -> ExecutionSchedulingFacts {
        ExecutionSchedulingFacts {
            topological_fragment_order: vec![1],
            execution_anchor_fragment_id: 1,
            fragments: vec![FragmentSchedulingFacts {
                fragment_id: 1,
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

        let fact = (expected_scan, NativeScanWork::RuntimeSplits);
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
            NativeScanWork::WholeRelation,
        );
        let (foreign_request, _) = session.issue_attempt(exact).unwrap();
        assert!(matches!(
            foreign_request.bind(scheduling_over(&[foreign]), Dormant::new(1, backend)),
            Err(NativeExecutionContractError::ForeignScanWorkFact)
        ));
    }

    #[derive(Debug)]
    struct NoopPreparationPort;

    impl NativeAttemptPreparationPort for NoopPreparationPort {
        fn prepare(
            &mut self,
            _request: NativeAttemptPreparationRequest,
        ) -> NativeAttemptPreparationFuture {
            Box::pin(async { Err(NativeExecutionContractError::ForeignAttemptTicket.into()) })
        }
    }

    #[derive(Debug)]
    struct EchoPreparationPort {
        calls: usize,
    }

    impl NativeAttemptPreparationPort for EchoPreparationPort {
        fn prepare(
            &mut self,
            request: NativeAttemptPreparationRequest,
        ) -> NativeAttemptPreparationFuture {
            self.calls += 1;
            assert_eq!(self.calls, 1);
            let _exact_execution = request.execution();
            let backends = prepared_inputs();
            Box::pin(async move {
                request
                    .bind(scheduling_over(&[]), Dormant::new(9, backends))
                    .map_err(Into::into)
            })
        }
    }

    #[derive(Debug)]
    struct RetryableBorrowedConvergence {
        polls: usize,
    }

    impl DormantNativeAttemptOwner for RetryableBorrowedConvergence {
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

        owner.converge(cancellation).await;
        assert_eq!(owner.polls, 2);
    }
}
