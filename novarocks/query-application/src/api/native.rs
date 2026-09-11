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

use novarocks_sql::planning::query_execution::{SealedPreparationPlanId, SealedScanIdentity};
use novarocks_types::identity::{BackendProcessId, QueryExecutionId};
pub use novarocks_workload_control::CancellationView;
use novarocks_workload_control::WorkId;

use super::{QueryExecutionError, QueryExecutionErrorKind};
use crate::coordination::{
    AbortQueryContextEffectPort, AttemptFailureClass, AttemptSchedule, NativeAttemptDrive,
    RecoveryMode, ReplacementQualificationEffectPort,
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

/// Runtime failure while preparing one physical attempt.
///
/// Coordination consumes the attempt failure class for recovery policy and
/// retains the query error for the caller-visible terminal result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeAttemptPreparationFailure {
    class: AttemptFailureClass,
    error: QueryExecutionError,
}

impl NativeAttemptPreparationFailure {
    pub const fn new(class: AttemptFailureClass, error: QueryExecutionError) -> Self {
        Self { class, error }
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
    plan_seal: SealedPreparationPlanId,
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
        plan_seal: SealedPreparationPlanId,
        aborts: Arc<dyn AbortQueryContextEffectPort>,
        replacements: Option<Arc<dyn ReplacementQualificationEffectPort>>,
        attempts: impl NativeAttemptPreparationPort,
    ) -> Self {
        Self {
            plan_seal,
            aborts,
            replacements,
            attempts: Box::new(attempts),
        }
    }

    pub(crate) const fn plan_seal(&self) -> SealedPreparationPlanId {
        self.plan_seal
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

/// Borrowed convergence future for either a dormant or active Native owner.
/// It resolves only after every resource the owner may have made live has
/// converged. For an active owner, resolution is positive evidence that every
/// scheduled context has stopped accepting work for this attempt and its
/// physical work has stopped, so the supervisor may record exact Registry
/// convergence. Dropping the future or catching a poll panic leaves the owner
/// with its supervisor, and a later call must resume the same idempotent
/// convergence.
pub type NativeAttemptConvergenceFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

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
    ) -> NativeAttemptConvergenceFuture<'a>;
}

/// Accepted active Native owner. Wrapper construction keeps role-local owner
/// details out of coordination while preserving a single move-only owner.
pub struct ActivatedNativeAttempt {
    owner: Box<dyn ActiveNativeAttemptOwner>,
}

impl fmt::Debug for ActivatedNativeAttempt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActivatedNativeAttempt")
            .finish_non_exhaustive()
    }
}

impl ActivatedNativeAttempt {
    pub fn new(owner: impl ActiveNativeAttemptOwner) -> Self {
        Self {
            owner: Box::new(owner),
        }
    }

    pub(crate) fn into_owner(self) -> Box<dyn ActiveNativeAttemptOwner> {
        self.owner
    }
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

/// One scan's exact Native work fact, bound to the opaque SQL plan seal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NativeScanWorkFact {
    scan: SealedScanIdentity,
    work: NativeScanWork,
}

impl NativeScanWorkFact {
    pub const fn new(scan: SealedScanIdentity, work: NativeScanWork) -> Self {
        Self { scan, work }
    }

    pub const fn scan(&self) -> SealedScanIdentity {
        self.scan
    }

    pub const fn work(&self) -> NativeScanWork {
        self.work
    }
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
    pub const fn execution(&self) -> QueryExecutionId {
        self.execution
    }

    pub fn description(&self) -> &FrozenExecutionDescription {
        self.logical_ticket.description.as_ref()
    }

    /// Compare the request with the role adapter's own immutable Native
    /// template. The opaque seal is never returned to the adapter, so it
    /// cannot replace this proof with a caller-declared identity.
    pub fn matches_plan_seal(&self, template_plan_seal: SealedPreparationPlanId) -> bool {
        self.logical_ticket.description.plan_seal() == template_plan_seal
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
        scan_work: Vec<NativeScanWorkFact>,
        owner: impl DormantNativeAttemptOwner,
    ) -> Result<PreparedNativeAttempt, NativeExecutionContractError> {
        let eligible_backends = owner.eligible_backends().to_vec();
        validate_prepared_inputs(
            self.logical_ticket.description.as_ref(),
            &eligible_backends,
            &scan_work,
        )?;
        Ok(PreparedNativeAttempt {
            logical_ticket: self.logical_ticket,
            attempt_ticket: self.attempt_ticket,
            execution: self.execution,
            eligible_backends: eligible_backends.into_boxed_slice(),
            scan_work: scan_work.into_boxed_slice(),
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
    scan_work: Box<[NativeScanWorkFact]>,
    owner: Box<dyn DormantNativeAttemptOwner>,
}

impl fmt::Debug for PreparedNativeAttempt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedNativeAttempt")
            .field("execution", &self.execution)
            .field("eligible_backends", &self.eligible_backends)
            .field("scan_work", &self.scan_work)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub(crate) struct PreparedNativeAttemptParts {
    pub(crate) execution: QueryExecutionId,
    pub(crate) eligible_backends: Box<[BackendProcessId]>,
    pub(crate) scan_work: Box<[NativeScanWorkFact]>,
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
            scan_work: prepared.scan_work,
            owner: prepared.owner,
        })
    }
}

fn validate_prepared_inputs(
    description: &FrozenExecutionDescription,
    eligible_backends: &[BackendProcessId],
    scan_work: &[NativeScanWorkFact],
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
        .scans()
        .iter()
        .map(|scan| scan.scan_identity())
        .collect::<BTreeSet<_>>();
    let mut actual = BTreeSet::new();
    for fact in scan_work {
        if !expected.contains(&fact.scan) {
            return Err(NativeExecutionContractError::ForeignScanWorkFact);
        }
        if !actual.insert(fact.scan) {
            return Err(NativeExecutionContractError::DuplicateScanWorkFact);
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
    use crate::api::QueryExecutionKind;
    use crate::coordination::{
        ExecutionEffect, PermanentlyBackpressuredAbortEffectPort,
        ReplacementQualificationEffectAdmission, ReplacementQualificationRequest,
    };
    use crate::preparation::{
        ExecutionResourceRequirements, FrozenCostEstimate, FrozenEstimateUnknownReason,
        FrozenExecutionDescriptionDraft,
    };
    use novarocks_sql::planning::query_execution::SealedPreparationPlan;
    use novarocks_sql::test_support::{
        NativePreparationFixture, NativeScanFixture, native_preparation_plan, native_scan_plan,
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
        ) -> NativeAttemptConvergenceFuture<'a> {
            Box::pin(async {})
        }
    }

    impl DormantNativeAttemptOwner for Dormant {
        fn eligible_backends(&self) -> &[BackendProcessId] {
            &self.eligible_backends
        }

        fn activate<'a>(
            &'a mut self,
            _schedule: &'a AttemptSchedule,
            _cancellation: CancellationView,
        ) -> NativeAttemptActivationFuture<'a> {
            Box::pin(async move { Ok(ActivatedNativeAttempt::new(ActiveDormant(self.tag))) })
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

    fn description(recovery: RecoveryMode) -> Arc<FrozenExecutionDescription> {
        let plan = native_preparation_plan(NativePreparationFixture::MissingResultOutput).unwrap();
        Arc::new(
            FrozenExecutionDescription::try_freeze(FrozenExecutionDescriptionDraft::new(
                QueryExecutionKind::Maintenance,
                SealedPreparationPlan::seal(plan),
                None,
                ExecutionEffect::None,
                recovery,
                Vec::new(),
                FrozenCostEstimate::unknown(FrozenEstimateUnknownReason::NotProjected),
                ExecutionResourceRequirements::unknown(FrozenEstimateUnknownReason::NotProjected),
            ))
            .unwrap(),
        )
    }

    fn seed_for_description(
        description: &FrozenExecutionDescription,
        aborts: Arc<dyn AbortQueryContextEffectPort>,
        replacements: Option<Arc<dyn ReplacementQualificationEffectPort>>,
        attempts: impl NativeAttemptPreparationPort,
    ) -> NativeLogicalExecutionSeed {
        NativeLogicalExecutionSeed::issue(description.plan_seal(), aborts, replacements, attempts)
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
        Arc::new(
            FrozenExecutionDescription::try_freeze(crate::preparation::tests::scan_draft(
                native_scan_plan(NativeScanFixture::ConnectorRead).unwrap(),
                ExecutionEffect::None,
                RecoveryMode::NoRecovery,
            ))
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
    fn open_acceptance_rejects_a_seed_bound_to_another_plan_seal() {
        let first_description = scan_description();
        let second_description = scan_description();
        assert_ne!(
            first_description.scans()[0].scan_identity(),
            second_description.scans()[0].scan_identity(),
            "the fixture must carry distinct opaque plan seals"
        );
        let (_first_governance, first, first_acceptance) =
            issue_description(execution(11, 1), first_description);
        let (_second_governance, second, _) =
            issue_description(execution(12, 1), second_description);

        let foreign_session = second.bind().unwrap();
        assert!(matches!(
            first_acceptance.accept(foreign_session),
            Err(NativeExecutionContractError::ForeignLogicalOpenTicket)
        ));
        drop(first);
    }

    #[test]
    fn executable_request_rejects_a_cross_seal_seed_for_identical_no_scan_plans() {
        let first_plan = SealedPreparationPlan::seal(
            native_preparation_plan(NativePreparationFixture::MissingResultOutput).unwrap(),
        );
        let second_plan = SealedPreparationPlan::seal(
            native_preparation_plan(NativePreparationFixture::MissingResultOutput).unwrap(),
        );
        let second_description =
            FrozenExecutionDescription::try_freeze(FrozenExecutionDescriptionDraft::new(
                QueryExecutionKind::Maintenance,
                second_plan,
                None,
                ExecutionEffect::None,
                RecoveryMode::NoRecovery,
                Vec::new(),
                FrozenCostEstimate::unknown(FrozenEstimateUnknownReason::NotProjected),
                ExecutionResourceRequirements::unknown(FrozenEstimateUnknownReason::NotProjected),
            ))
            .unwrap();
        let foreign_seed = NativeLogicalExecutionSeed::issue(
            second_description.plan_seal(),
            PermanentlyBackpressuredAbortEffectPort::shared(),
            None,
            NoopPreparationPort,
        );
        let first_description =
            FrozenExecutionDescription::try_freeze(FrozenExecutionDescriptionDraft::new(
                QueryExecutionKind::Maintenance,
                first_plan,
                None,
                ExecutionEffect::None,
                RecoveryMode::NoRecovery,
                Vec::new(),
                FrozenCostEstimate::unknown(FrozenEstimateUnknownReason::NotProjected),
                ExecutionResourceRequirements::unknown(FrozenEstimateUnknownReason::NotProjected),
            ))
            .unwrap();

        assert!(matches!(
            crate::api::QueryExecutionRequest::try_from_parts(first_description, foreign_seed),
            Err(NativeExecutionContractError::ForeignLogicalSeedPlan)
        ));
    }

    #[test]
    fn attempt_request_matches_only_its_exact_no_scan_template_seal() {
        let first = description(RecoveryMode::NoRecovery);
        let second = description(RecoveryMode::NoRecovery);
        assert_ne!(first.plan_seal(), second.plan_seal());
        let (_governance, open, acceptance) =
            issue_description(execution(21, 1), Arc::clone(&first));
        let session = acceptance.accept(open.bind().unwrap()).unwrap();
        let (request, _) = session.issue_attempt(execution(21, 1)).unwrap();

        assert!(request.matches_plan_seal(first.plan_seal()));
        assert!(!request.matches_plan_seal(second.plan_seal()));
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
        let prepared = second.bind(Vec::new(), Dormant::new(2, backends)).unwrap();
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
        let prepared = request.bind(Vec::new(), dormant).unwrap();
        let parts = acceptance.accept(prepared).unwrap();
        assert_eq!(parts.execution, exact_execution);
        assert_eq!(parts.eligible_backends.as_ref(), backends);
        assert!(parts.scan_work.is_empty());
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
            request.bind(Vec::new(), Dormant::new(1, Vec::new())),
            Err(NativeExecutionContractError::EmptyEligibleBackends)
        ));

        let (request, _) = session.issue_attempt(exact).unwrap();
        assert!(matches!(
            request.bind(Vec::new(), Dormant::new(1, vec![backends[0], backends[0]]),),
            Err(NativeExecutionContractError::DuplicateEligibleBackend)
        ));
    }

    #[test]
    fn prepared_attempt_requires_exact_sealed_scan_work_cover() {
        let exact = execution(7, 1);
        let description = scan_description();
        let expected_scan = description.scans()[0].scan_identity();
        let (_governance, open, acceptance) = issue_description(exact, description);
        let session = acceptance.accept(bind_no_recovery(open).unwrap()).unwrap();
        let backend = prepared_inputs();

        let (missing, _) = session.issue_attempt(exact).unwrap();
        assert!(matches!(
            missing.bind(Vec::new(), Dormant::new(1, backend.clone())),
            Err(NativeExecutionContractError::MissingScanWorkFact)
        ));

        let fact = NativeScanWorkFact::new(expected_scan, NativeScanWork::RuntimeSplits);
        let (duplicate, _) = session.issue_attempt(exact).unwrap();
        assert!(matches!(
            duplicate.bind(vec![fact, fact], Dormant::new(1, backend.clone())),
            Err(NativeExecutionContractError::DuplicateScanWorkFact)
        ));

        let foreign_description = scan_description();
        let foreign = NativeScanWorkFact::new(
            foreign_description.scans()[0].scan_identity(),
            NativeScanWork::WholeRelation,
        );
        let (foreign_request, _) = session.issue_attempt(exact).unwrap();
        assert!(matches!(
            foreign_request.bind(vec![foreign], Dormant::new(1, backend)),
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
                    .bind(Vec::new(), Dormant::new(9, backends))
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
