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
//! return. Matching the concrete owner to the frozen template remains a trusted
//! role-adapter obligation; the adapter never receives coordination authority.

use std::collections::BTreeSet;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use novarocks_execution_contract::{QueryContextRef, TaskIdentity};
use novarocks_types::identity::QueryExecutionId;
use novarocks_workload_control::WorkScope;

use super::{QueryExecutionError, QueryExecutionErrorKind};
use crate::coordination::{
    AbortQueryContextEffectPort, AttemptFailureClass, RecoveryMode,
    ReplacementQualificationEffectPort,
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

/// Move-only role owner for a prepared attempt that has not started any Task.
///
/// Before binding, the owner must have no asynchronous convergence
/// responsibility. Dropping it must synchronously release every dormant
/// resource. Activation and post-activation abandonment belong to the
/// supervisor transition introduced by T08-C2.
pub trait DormantNativeAttemptOwner: fmt::Debug + Send + 'static {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum NativeExecutionContractError {
    ForeignLogicalOpenTicket,
    ForeignAttemptTicket,
    DifferentLogicalQuery,
    EmptyContextSet,
    ContextExecutionMismatch,
    DuplicateContext,
    MultipleFrontendProcesses,
    DuplicateBackendContext,
    RootExecutionMismatch,
    RootContextMissing,
    MissingReplacementPort,
    UnexpectedReplacementPort,
}

impl fmt::Display for NativeExecutionContractError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::ForeignLogicalOpenTicket => {
                "Native logical session was bound from a different open request"
            }
            Self::ForeignAttemptTicket => {
                "prepared Native attempt was bound from a different prepare request"
            }
            Self::DifferentLogicalQuery => {
                "Native attempt does not belong to the opened logical query"
            }
            Self::EmptyContextSet => "Native attempt context set must not be empty",
            Self::ContextExecutionMismatch => {
                "Native attempt context does not match the exact execution"
            }
            Self::DuplicateContext => "Native attempt context set contains a duplicate",
            Self::MultipleFrontendProcesses => {
                "Native attempt contexts name multiple frontend processes"
            }
            Self::DuplicateBackendContext => {
                "Native attempt contains multiple contexts for one backend process"
            }
            Self::RootExecutionMismatch => "Native root task does not match the exact execution",
            Self::RootContextMissing => {
                "Native root task backend has no context in the exact attempt"
            }
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
    work: WorkScope,
}

impl fmt::Debug for LogicalNativeTicket {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LogicalNativeTicket")
            .field("initial_execution", &self.initial_execution)
            .field("work", &self.work.id())
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

    pub fn work(&self) -> &WorkScope {
        &self.ticket.work
    }

    /// Consumes the only request ticket and binds the role-local attempt port
    /// to its immutable logical description.
    pub fn bind(
        self,
        aborts: Arc<dyn AbortQueryContextEffectPort>,
        replacements: Option<Arc<dyn ReplacementQualificationEffectPort>>,
        attempts: impl NativeAttemptPreparationPort,
    ) -> Result<LogicalNativeSession, NativeExecutionContractError> {
        match (self.ticket.description.recovery(), replacements.is_some()) {
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
            aborts,
            replacements,
            attempts: Box::new(attempts),
        })
    }

    pub(crate) fn issue(
        initial_execution: QueryExecutionId,
        description: Arc<FrozenExecutionDescription>,
        work: WorkScope,
    ) -> (Self, LogicalNativeOpenAcceptance) {
        let ticket = Arc::new(LogicalNativeTicket {
            initial_execution,
            description,
            work,
        });
        (
            Self {
                ticket: Arc::clone(&ticket),
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

    pub fn work(&self) -> &WorkScope {
        &self.logical_ticket.work
    }

    /// Consumes this exact prepare ticket and seals the dormant role owner to
    /// its execution, context manifest, and root task.
    pub fn bind(
        self,
        contexts: Vec<QueryContextRef>,
        root: TaskIdentity,
        owner: impl DormantNativeAttemptOwner,
    ) -> Result<PreparedNativeAttempt, NativeExecutionContractError> {
        validate_attempt_manifest(self.execution, &contexts, root)?;
        Ok(PreparedNativeAttempt {
            logical_ticket: self.logical_ticket,
            attempt_ticket: self.attempt_ticket,
            execution: self.execution,
            contexts: contexts.into_boxed_slice(),
            root,
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
    contexts: Box<[QueryContextRef]>,
    root: TaskIdentity,
    owner: Box<dyn DormantNativeAttemptOwner>,
}

impl fmt::Debug for PreparedNativeAttempt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedNativeAttempt")
            .field("execution", &self.execution)
            .field("contexts", &self.contexts)
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub(crate) struct PreparedNativeAttemptParts {
    pub(crate) execution: QueryExecutionId,
    pub(crate) contexts: Box<[QueryContextRef]>,
    pub(crate) root: TaskIdentity,
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
            contexts: prepared.contexts,
            root: prepared.root,
            owner: prepared.owner,
        })
    }
}

fn validate_attempt_manifest(
    execution: QueryExecutionId,
    contexts: &[QueryContextRef],
    root: TaskIdentity,
) -> Result<(), NativeExecutionContractError> {
    if root.query_execution_id() != execution {
        return Err(NativeExecutionContractError::RootExecutionMismatch);
    }
    let Some(first) = contexts.first() else {
        return Err(NativeExecutionContractError::EmptyContextSet);
    };
    let frontend = first.frontend_process_id();
    let mut exact_contexts = BTreeSet::new();
    let mut backends = BTreeSet::new();
    for &context in contexts {
        if context.query_execution_id() != execution {
            return Err(NativeExecutionContractError::ContextExecutionMismatch);
        }
        if !exact_contexts.insert(context) {
            return Err(NativeExecutionContractError::DuplicateContext);
        }
        if context.frontend_process_id() != frontend {
            return Err(NativeExecutionContractError::MultipleFrontendProcesses);
        }
        if !backends.insert(context.backend_process_id()) {
            return Err(NativeExecutionContractError::DuplicateBackendContext);
        }
    }
    if !backends.contains(&root.backend_process_id()) {
        return Err(NativeExecutionContractError::RootContextMissing);
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
    use novarocks_sql::test_support::{NativePreparationFixture, native_preparation_plan};
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, FrontendProcessId, QueryId, StageId, TaskId,
    };
    use novarocks_workload_control::{
        ResourceConfig, RootWork, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
    };
    use tokio::sync::watch;

    #[derive(Debug)]
    struct Dormant(u8);

    impl DormantNativeAttemptOwner for Dormant {}

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
        let (request, acceptance) =
            LogicalNativeOpenRequest::issue(execution, description(recovery), governance.scope());
        (governance, request, acceptance)
    }

    fn bind_no_recovery(
        request: LogicalNativeOpenRequest,
    ) -> Result<LogicalNativeSession, NativeExecutionContractError> {
        request.bind(
            PermanentlyBackpressuredAbortEffectPort::shared(),
            None,
            NoopPreparationPort,
        )
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

    fn manifest(
        execution: QueryExecutionId,
    ) -> (
        Vec<QueryContextRef>,
        TaskIdentity,
        FrontendProcessId,
        BackendProcessId,
    ) {
        let frontend = FrontendProcessId::try_from_bytes(process_bytes(1)).unwrap();
        let backend = BackendProcessId::try_from_bytes(process_bytes(2)).unwrap();
        let context = QueryContextRef::new(execution, frontend, backend);
        let root = TaskIdentity::new(
            execution,
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            backend,
        );
        (vec![context], root, frontend, backend)
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
    fn attempt_acceptance_rejects_cross_request_splicing() {
        let (_governance, open, open_acceptance) = issue(execution(1, 1), RecoveryMode::NoRecovery);
        let session = open_acceptance
            .accept(bind_no_recovery(open).unwrap())
            .unwrap();
        let (first, first_acceptance) = session.issue_attempt(execution(1, 1)).unwrap();
        let (second, _) = session.issue_attempt(execution(1, 2)).unwrap();
        let (contexts, root, _, _) = manifest(execution(1, 2));
        let prepared = second.bind(contexts, root, Dormant(2)).unwrap();
        assert!(matches!(
            first_acceptance.accept(prepared),
            Err(NativeExecutionContractError::ForeignAttemptTicket)
        ));
        drop(first);
    }

    #[test]
    fn accepted_attempt_preserves_the_exact_manifest_and_owner() {
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
        assert_eq!(request.work().id(), session.ticket.work.id());
        let (contexts, root, _, _) = manifest(exact_execution);
        let dormant = Dormant(7);
        assert_eq!(dormant.0, 7);
        let prepared = request.bind(contexts.clone(), root, dormant).unwrap();
        let parts = acceptance.accept(prepared).unwrap();
        assert_eq!(parts.execution, exact_execution);
        assert_eq!(parts.contexts.as_ref(), contexts);
        assert_eq!(parts.root, root);
        assert_eq!(format!("{:?}", parts.owner), "Dormant(7)");
    }

    #[tokio::test]
    async fn mutable_attempt_port_preserves_work_description_and_exact_ticket() {
        let (_governance, open, open_acceptance) = issue(
            execution(1, 1),
            RecoveryMode::RestartAttemptBeforeVisibility,
        );
        let aborts = PermanentlyBackpressuredAbortEffectPort::shared();
        let replacements = BackpressuredReplacementPort::shared();
        let mut session = open_acceptance
            .accept(
                open.bind(
                    Arc::clone(&aborts),
                    Some(Arc::clone(&replacements)),
                    EchoPreparationPort { calls: 0 },
                )
                .unwrap(),
            )
            .unwrap();
        assert!(Arc::ptr_eq(&session.abort_effect_port(), &aborts));
        assert!(Arc::ptr_eq(
            &session.replacement_effect_port().unwrap(),
            &replacements
        ));

        let exact_execution = execution(1, 2);
        let (request, acceptance) = session.issue_attempt(exact_execution).unwrap();
        assert_eq!(request.work().id(), session.ticket.work.id());
        let prepared = session.prepare(request).await.unwrap();
        let parts = acceptance.accept(prepared).unwrap();
        assert_eq!(parts.execution, exact_execution);
        assert_eq!(parts.root.query_execution_id(), exact_execution);
    }

    #[test]
    fn session_bind_requires_the_recovery_modes_exact_port_shape() {
        let (_governance, no_recovery, _) = issue(execution(1, 1), RecoveryMode::NoRecovery);
        assert!(matches!(
            no_recovery.bind(
                PermanentlyBackpressuredAbortEffectPort::shared(),
                Some(BackpressuredReplacementPort::shared()),
                NoopPreparationPort,
            ),
            Err(NativeExecutionContractError::UnexpectedReplacementPort)
        ));

        let (_governance, recoverable, _) = issue(
            execution(2, 1),
            RecoveryMode::RestartAttemptBeforeVisibility,
        );
        assert!(matches!(
            recoverable.bind(
                PermanentlyBackpressuredAbortEffectPort::shared(),
                None,
                NoopPreparationPort,
            ),
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
    fn attempt_manifest_rejects_wrong_execution_duplicate_and_missing_root_context() {
        let exact = execution(1, 1);
        let (_governance, open, acceptance) = issue(exact, RecoveryMode::NoRecovery);
        let session = acceptance.accept(bind_no_recovery(open).unwrap()).unwrap();

        let (request, _) = session.issue_attempt(exact).unwrap();
        let (_, wrong_root, _, _) = manifest(execution(1, 2));
        assert!(matches!(
            request.bind(Vec::new(), wrong_root, Dormant(1)),
            Err(NativeExecutionContractError::RootExecutionMismatch)
        ));

        let (contexts, root, _, _) = manifest(exact);
        let (request, _) = session.issue_attempt(exact).unwrap();
        assert!(matches!(
            request.bind(vec![contexts[0], contexts[0]], root, Dormant(1)),
            Err(NativeExecutionContractError::DuplicateContext)
        ));

        let (request, _) = session.issue_attempt(exact).unwrap();
        let other_backend = BackendProcessId::try_from_bytes(process_bytes(3)).unwrap();
        let wrong_root = TaskIdentity::new(
            exact,
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            other_backend,
        );
        assert!(matches!(
            request.bind(contexts, wrong_root, Dormant(1)),
            Err(NativeExecutionContractError::RootContextMissing)
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
            let exact_execution = request.execution();
            let (contexts, root, _, _) = manifest(exact_execution);
            Box::pin(async move { request.bind(contexts, root, Dormant(9)).map_err(Into::into) })
        }
    }
}
