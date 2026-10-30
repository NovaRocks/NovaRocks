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

//! Product ownership for process-local statistics collection jobs.
//!
//! A job is not a query attempt and neither is a provider publication.  The
//! three identities deliberately remain distinct, even when the first product
//! implementation performs one collection attempt and one publication.
//!
//! This crate owns the statistics business state machine.  It only consumes
//! the query-application and workload-control contracts; Native encoding,
//! MySQL adaptation, and connector-private publication mechanics stay outside
//! this crate.

use novarocks_query_application::api::{
    LocalExecutionDisposition, LogicalExecutionExitObserver, LogicalExecutionExitReceipt,
};
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use novarocks_query_application::admitted_query_context::QueryResultCapacityBinding;
use novarocks_workload_control::{QueryConcurrencyPermit, WorkOwner, WorkScope};
#[cfg(test)]
use novarocks_workload_control::{WorkClass, WorkRequest};
use tokio::sync::Notify;
use uuid::Uuid;

mod job_service;

pub use job_service::{StatisticsJobRuntime, StatisticsJobService};

pub const MAX_ACTIVE_OR_QUEUED_STATISTICS_JOBS: usize = 1024;
pub const MAX_RECENT_TERMINAL_STATISTICS_JOBS: usize = 4096;

/// Stable identity of a statistics business request in this frontend process.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StatisticsJobId(Uuid);

impl StatisticsJobId {
    pub fn new_v7() -> Self {
        Self(Uuid::now_v7())
    }

    pub const fn as_uuid(self) -> Uuid {
        self.0
    }

    /// Reconstitutes a user-supplied display identity for lookup only. Job
    /// creation always mints a fresh UUIDv7 through `new_v7`.
    pub const fn from_uuid(value: Uuid) -> Self {
        Self(value)
    }
}

/// The logical query launched to collect one statistics job's artifacts.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StatisticsLogicalExecutionId(Uuid);

impl StatisticsLogicalExecutionId {
    pub fn new_v7() -> Self {
        Self(Uuid::now_v7())
    }

    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

/// A concrete attempt of the logical collection query.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StatisticsQueryAttemptId(Uuid);

impl StatisticsQueryAttemptId {
    pub fn new_v7() -> Self {
        Self(Uuid::now_v7())
    }

    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

/// Idempotency identity for the one provider publication operation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StatisticsPublicationId(Uuid);

impl StatisticsPublicationId {
    pub fn new_v7() -> Self {
        Self(Uuid::now_v7())
    }

    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsTarget {
    pub catalog: Arc<str>,
    pub namespace: Arc<str>,
    pub table: Arc<str>,
    /// Exact provider object identity captured at submission.  A query
    /// attempt rebinds this identity; it must not silently select a same-name
    /// replacement.
    pub object_id: Arc<[u8]>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StatisticsColumns {
    All,
    Explicit(Arc<[Arc<str>]>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsJobCreate {
    pub target: StatisticsTarget,
    pub columns: StatisticsColumns,
    pub submitted_at_ms: i64,
}

/// Non-terminal phases are business phases, not query-execution states.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatisticsJobPhase {
    Submitted,
    Preparing,
    Collecting,
    Publishing,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatisticsJobConclusion {
    Succeeded,
    Failed,
    Stale,
    Cancelled,
    CommitUnknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatisticsJobState {
    Active(StatisticsJobPhase),
    Terminal(StatisticsJobConclusion),
}

impl StatisticsJobState {
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Terminal(_))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatisticsPublicationFact {
    NotStarted,
    /// The Provider completed without applying an external mutation.
    NoOp,
    KnownUncommitted,
    KnownCommitted,
    CommitUnknown,
}

/// Exact business terminal classification for a provider publication.
///
/// This is diagnostics for the product conclusion, never an authorization to
/// retry or reconcile an external effect.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatisticsPublicationTerminal {
    KnownUncommitted,
    KnownCommittedFinalization,
    CommitUnknown,
}

/// Actual runtime convergence must not be inferred from a business conclusion.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StatisticsConvergence {
    pub collection_stopped: bool,
    pub execution_resources_released: bool,
    pub provider_session_closed: bool,
}

impl StatisticsConvergence {
    pub const fn is_complete(self) -> bool {
        self.collection_stopped && self.execution_resources_released && self.provider_session_closed
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsFailure {
    pub message: Arc<str>,
    pub compile_control: Option<novarocks_type_contract::CompileControlError>,
}

#[cfg(test)]
fn test_publication_failure(message: &str) -> StatisticsPublicationFailure {
    use novarocks_failure_contract::FailureProjection;
    let original = novarocks_type_contract::CompileControlError::ResourceExhausted;
    StatisticsPublicationFailure::capture(
        original.failure_fact(),
        Some(original.provenance()),
        &message,
    )
}

/// One bounded observation, not the originating Provider resource owner.
/// The actual Session keeps that original cause until its real close.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct StatisticsPublicationFailure {
    fact: novarocks_failure_contract::FailureFact,
    compile_control: Option<novarocks_type_contract::CompileControlProvenance>,
    diagnostic: [u8; novarocks_failure_contract::DIAGNOSTIC_MAX_BYTES],
    diagnostic_len: u16,
}
impl StatisticsPublicationFailure {
    pub const PUBLIC_MESSAGE_BYTES: usize =
        "statistics publication failed (reason 65535:65535)".len();
    pub const PUBLIC_KIND_BYTES: usize = "FINALIZATION".len();
    pub fn public_message(&self) -> String {
        use fmt::Write;
        let mut message = String::with_capacity(Self::PUBLIC_MESSAGE_BYTES);
        write!(&mut message, "{self}").expect("fixed String write");
        debug_assert!(message.len() <= Self::PUBLIC_MESSAGE_BYTES);
        message
    }

    /// Called outside repository locks while borrowing the actual Provider
    /// outcome. Formatting is diagnostic only and cannot change its facts.
    pub fn capture(
        fact: novarocks_failure_contract::FailureFact,
        compile_control: Option<novarocks_type_contract::CompileControlProvenance>,
        diagnostic: &dyn fmt::Display,
    ) -> Self {
        struct Writer<'a> {
            bytes: &'a mut [u8],
            len: usize,
        }
        impl fmt::Write for Writer<'_> {
            fn write_str(&mut self, text: &str) -> fmt::Result {
                let mut len = text.len().min(self.bytes.len() - self.len);
                while !text.is_char_boundary(len) {
                    len -= 1;
                }
                self.bytes[self.len..self.len + len].copy_from_slice(&text.as_bytes()[..len]);
                self.len += len;
                if len == text.len() {
                    Ok(())
                } else {
                    Err(fmt::Error)
                }
            }
        }
        let mut record = Self {
            fact,
            compile_control,
            diagnostic: [0; novarocks_failure_contract::DIAGNOSTIC_MAX_BYTES],
            diagnostic_len: 0,
        };
        let mut writer = Writer {
            bytes: &mut record.diagnostic,
            len: 0,
        };
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = fmt::write(&mut writer, format_args!("{diagnostic}"));
        }));
        record.diagnostic_len = writer.len as u16;
        record
    }
    pub const fn fact(&self) -> novarocks_failure_contract::FailureFact {
        self.fact
    }
    pub const fn compile_control(
        &self,
    ) -> Option<novarocks_type_contract::CompileControlProvenance> {
        self.compile_control
    }
    /// Private process diagnostics. Never pass this text to SHOW or SQL errors.
    pub fn local_diagnostic(&self) -> &str {
        std::str::from_utf8(&self.diagnostic[..usize::from(self.diagnostic_len)])
            .expect("bounded formatter copied complete UTF-8 characters")
    }
}
impl fmt::Display for StatisticsPublicationFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "statistics publication failed (reason {})",
            self.fact.reason()
        )
    }
}
impl fmt::Debug for StatisticsPublicationFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Even incidental Debug does not publish the private diagnostic.
        f.debug_struct("StatisticsPublicationFailure")
            .field("fact", &self.fact)
            .field("compile_control", &self.compile_control)
            .finish()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsJob {
    pub id: StatisticsJobId,
    pub target: StatisticsTarget,
    pub columns: StatisticsColumns,
    pub state: StatisticsJobState,
    pub logical_execution_id: Option<StatisticsLogicalExecutionId>,
    pub query_attempt_id: Option<StatisticsQueryAttemptId>,
    pub publication_id: StatisticsPublicationId,
    pub publication: StatisticsPublicationFact,
    /// The provider publication refusal, independent of cleanup diagnostics.
    pub publication_failure: Option<StatisticsPublicationFailure>,
    /// Necessary finalization preserves the actual Applied or NoOp effect.
    pub publication_finalization_failure: Option<StatisticsPublicationFailure>,
    /// Optional GC/close diagnostics never change the selected business result.
    pub publication_cleanup_failure: Option<StatisticsPublicationFailure>,
    pub convergence: StatisticsConvergence,
    pub cancel_requested: bool,
    pub failure: Option<StatisticsFailure>,
    pub submitted_at_ms: i64,
    pub updated_at_ms: i64,
    pub completed_at_ms: Option<i64>,
}

impl StatisticsJob {
    fn new(request: StatisticsJobCreate) -> Self {
        Self {
            id: StatisticsJobId::new_v7(),
            target: request.target,
            columns: request.columns,
            state: StatisticsJobState::Active(StatisticsJobPhase::Submitted),
            logical_execution_id: None,
            query_attempt_id: None,
            publication_id: StatisticsPublicationId::new_v7(),
            publication: StatisticsPublicationFact::NotStarted,
            publication_failure: None,
            publication_finalization_failure: None,
            publication_cleanup_failure: None,
            convergence: StatisticsConvergence::default(),
            cancel_requested: false,
            failure: None,
            submitted_at_ms: request.submitted_at_ms,
            updated_at_ms: request.submitted_at_ms,
            completed_at_ms: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatisticsRepositoryErrorKind {
    NotFound,
    Conflict,
    Capacity,
    InvalidTransition,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsRepositoryError {
    kind: StatisticsRepositoryErrorKind,
    message: Arc<str>,
}

impl StatisticsRepositoryError {
    pub const fn kind(&self) -> StatisticsRepositoryErrorKind {
        self.kind
    }

    fn new(kind: StatisticsRepositoryErrorKind, message: impl Into<Arc<str>>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

impl fmt::Display for StatisticsRepositoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for StatisticsRepositoryError {}

/// Move-only receipt for one independently admitted statistics business root.
/// All three inputs must name the exact host/root; no capacity is acquired here.
pub struct StatisticsJobAdmission {
    #[cfg(novarocks_dev_controls)]
    test_statement_nomination:
        Option<Arc<novarocks_failpoint::target_binding::StatementWorkNomination>>,
    owner: WorkOwner,
    query_concurrency: QueryConcurrencyPermit,
    result_capacity: Option<QueryResultCapacityBinding>,
    root_payload: Option<novarocks_workload_control::joint_admission::RetainedPayloadGrant>,
}
impl StatisticsJobAdmission {
    pub fn scope(&self) -> WorkScope {
        self.owner.scope()
    }
    pub fn into_originals(self) -> StatisticsJobOriginals {
        StatisticsJobOriginals {
            owner: Some(self.owner),
            query_concurrency: Some(self.query_concurrency),
            result_capacity: self.result_capacity,
            root_payload: self.root_payload,
            #[cfg(novarocks_dev_controls)]
            test_statement_nomination: self.test_statement_nomination,
        }
    }
    pub fn try_new(
        owner: WorkOwner,
        query_concurrency: QueryConcurrencyPermit,
        window: novarocks_workload_control::ResultWindowGrant,
    ) -> Result<Self, StatisticsRepositoryError> {
        let scope = owner.scope();
        if !query_concurrency.is_for_scope(&scope)
            || window.class() != novarocks_workload_control::ResultWindowClass::Internal
        {
            return Err(StatisticsRepositoryError::new(
                StatisticsRepositoryErrorKind::Conflict,
                "statistics admission requires its exact query permit and Internal window",
            ));
        }
        window
            .check_backing_total(
                novarocks_workload_control::ResultCapacityConfig::V1.all_objects_bytes[2],
            )
            .map_err(work_error)?;
        let result_capacity = QueryResultCapacityBinding::try_new(&scope, window.retain_alias())
            .map_err(work_error)?;
        Ok(Self {
            owner,
            query_concurrency,
            result_capacity: Some(result_capacity),
            root_payload: None,
            #[cfg(novarocks_dev_controls)]
            test_statement_nomination: None,
        })
    }

    /// Adopt the three exact already-paid originals into the stable admission
    /// receiver. No ledger grant or fallback window is minted here. Both
    /// refusal and success are callback-free; caller keeps the source slots
    /// outside any cancellable publisher before exposing the job.
    pub fn install_internal_in(
        owner: &mut Option<WorkOwner>,
        query_concurrency: &mut Option<QueryConcurrencyPermit>,
        payload: &mut Option<novarocks_workload_control::joint_admission::RetainedPayloadGrant>,
        destination: &mut Option<Self>,
    ) -> Result<(), StatisticsRepositoryError> {
        if destination.is_some() {
            return Err(StatisticsRepositoryError::new(
                StatisticsRepositoryErrorKind::Conflict,
                "statistics admission receiver is occupied",
            ));
        }
        let original = owner.as_ref().ok_or_else(|| {
            StatisticsRepositoryError::new(
                StatisticsRepositoryErrorKind::Conflict,
                "statistics Root owner is absent",
            )
        })?;
        let scope = original.scope();
        let permit = query_concurrency.as_ref().ok_or_else(|| {
            StatisticsRepositoryError::new(
                StatisticsRepositoryErrorKind::Conflict,
                "statistics query permit is absent",
            )
        })?;
        if !permit.is_for_scope(&scope) {
            return Err(StatisticsRepositoryError::new(
                StatisticsRepositoryErrorKind::Conflict,
                "statistics query permit belongs to a foreign Root",
            ));
        }
        let grant = payload.as_ref().ok_or_else(|| {
            StatisticsRepositoryError::new(
                StatisticsRepositoryErrorKind::Conflict,
                "statistics original Root payload is absent",
            )
        })?;
        let result_capacity =
            QueryResultCapacityBinding::from_internal_payload(&scope, &scope, grant)
                .map_err(work_error)?;
        *destination = Some(Self {
            owner: owner.take().expect("checked original Root"),
            query_concurrency: query_concurrency.take().expect("checked original permit"),
            result_capacity: Some(result_capacity),
            root_payload: payload.take(),
            #[cfg(novarocks_dev_controls)]
            test_statement_nomination: None,
        });
        Ok(())
    }

    /// Preserve only the explicitly originating ANALYZE statement's evidence.
    /// This carries no cancellation, capacity, execution binding or new grant.
    #[cfg(novarocks_dev_controls)]
    pub fn with_test_statement_nomination(
        mut self,
        nomination: Arc<novarocks_failpoint::target_binding::StatementWorkNomination>,
    ) -> Result<Self, StatisticsRepositoryError> {
        self.install_test_statement_nomination_in(nomination)?;
        Ok(self)
    }

    /// Preserve the actual receiver when nomination validation refuses it.
    #[cfg(novarocks_dev_controls)]
    pub fn install_test_statement_nomination_in(
        &mut self,
        nomination: Arc<novarocks_failpoint::target_binding::StatementWorkNomination>,
    ) -> Result<(), StatisticsRepositoryError> {
        nomination.validate().map_err(|error| {
            StatisticsRepositoryError::new(
                StatisticsRepositoryErrorKind::Conflict,
                error.to_string(),
            )
        })?;
        if self
            .test_statement_nomination
            .as_ref()
            .is_some_and(|current| current != &nomination)
        {
            return Err(StatisticsRepositoryError::new(
                StatisticsRepositoryErrorKind::Conflict,
                "statistics admission has a foreign statement nomination",
            ));
        }
        self.test_statement_nomination = Some(nomination);
        Ok(())
    }
}

/// Stable slots consumed by the original background host. None is not an
/// exit fact: the repository retains the exact observer after these move.
pub struct StatisticsJobOriginals {
    pub owner: Option<WorkOwner>,
    pub query_concurrency: Option<QueryConcurrencyPermit>,
    pub result_capacity: Option<QueryResultCapacityBinding>,
    pub root_payload: Option<novarocks_workload_control::joint_admission::RetainedPayloadGrant>,
    #[cfg(novarocks_dev_controls)]
    pub test_statement_nomination:
        Option<Arc<novarocks_failpoint::target_binding::StatementWorkNomination>>,
}

/// A phase's cancellation/attribution scope and the independently owned job's
/// original capacity are separate facts. Preparation may exit before collect.
#[cfg(test)]
struct StatisticsAttemptContext {
    #[cfg(novarocks_dev_controls)]
    test_statement_nomination:
        Mutex<Option<Arc<novarocks_failpoint::target_binding::StatementWorkNomination>>>,
    stage_scope: WorkScope,
    root_result_capacity: QueryResultCapacityBinding,
}
#[cfg(test)]
impl StatisticsAttemptContext {
    /// Move the one preparation nomination into its actual collection request.
    #[cfg(novarocks_dev_controls)]
    pub fn take_test_statement_nomination(
        &self,
    ) -> Result<
        Option<Arc<novarocks_failpoint::target_binding::StatementWorkNomination>>,
        StatisticsRepositoryError,
    > {
        self.test_statement_nomination
            .lock()
            .map(|mut nomination| nomination.take())
            .map_err(|_| {
                StatisticsRepositoryError::new(
                    StatisticsRepositoryErrorKind::Conflict,
                    "statistics preparation nomination lock poisoned",
                )
            })
    }

    pub fn stage_scope(&self) -> &WorkScope {
        &self.stage_scope
    }
    pub fn root_result_capacity(&self) -> &QueryResultCapacityBinding {
        &self.root_result_capacity
    }
}

struct LiveJob {
    cancellation: novarocks_workload_control::WorkCancellationRequester,
    transferred: bool,
    exit: Option<LogicalExecutionExitObserver>,
    exit_failure: Option<novarocks_query_application::api::QueryExecutionError>,
    retained_exit: Option<LogicalExecutionExitReceipt>,
    #[cfg(novarocks_dev_controls)]
    test_statement_nomination:
        Option<Arc<novarocks_failpoint::target_binding::StatementWorkNomination>>,
    job: StatisticsJob,
    /// The business root remains owned until actual runtime convergence, not
    /// merely until a terminal job conclusion becomes observable.
    owner: Option<WorkOwner>,
    /// An independently submitted statistics job is warehouse compute. Its
    /// permit lasts through actual job convergence, alongside the root owner.
    query_concurrency: Option<QueryConcurrencyPermit>,
    result_capacity: Option<QueryResultCapacityBinding>,
    // Same original positive capability; stage views carry retention only.
    root_payload: Option<novarocks_workload_control::joint_admission::RetainedPayloadGrant>,
    stage_computation:
        Option<novarocks_workload_control::joint_admission::InternalComputationGrant>,
}

// The original terminal cut takes these inline values under the repository
// lock, then performs every actual Drop/complete outside it. This is one
// record's original custody, not another registry or deferred release queue.
struct RetiredJobCapacity {
    query_concurrency: Option<QueryConcurrencyPermit>,
    result_capacity: Option<QueryResultCapacityBinding>,
    payload: Option<novarocks_workload_control::joint_admission::RetainedPayloadGrant>,
    computation: Option<novarocks_workload_control::joint_admission::InternalComputationGrant>,
    owner: Option<WorkOwner>,
}
impl RetiredJobCapacity {
    fn finish(self) {
        let Self {
            query_concurrency,
            result_capacity,
            payload,
            computation,
            owner,
        } = self;
        drop(computation);
        drop(query_concurrency);
        drop(result_capacity);
        drop(payload);
        if let Some(owner) = owner {
            owner.complete();
        }
    }
}
impl LiveJob {
    fn take_capacity(&mut self) -> RetiredJobCapacity {
        RetiredJobCapacity {
            query_concurrency: self.query_concurrency.take(),
            result_capacity: self.result_capacity.take(),
            payload: self.root_payload.take(),
            computation: self.stage_computation.take(),
            owner: self.owner.take(),
        }
    }
}

#[derive(Default)]
struct RuntimeState {
    active: HashMap<StatisticsJobId, LiveJob>,
    terminal: VecDeque<LiveJob>,
    exit_cursor: Option<StatisticsJobId>,
}

/// Bounded, process-local observation of statistics jobs.  It deliberately
/// has no persistence/restart recovery protocol.
#[derive(Clone, Default)]
pub struct StatisticsJobRepository {
    state: Arc<Mutex<RuntimeState>>,
    changed: Arc<Notify>,
}

/// Only the original repository notification, not a capacity or release right.
/// Its Drop is inert; the stable receiver must unlock before calling notify.
#[must_use = "Notify the original service after releasing receiver locks"]
pub struct StatisticsJobNotification {
    changed: Arc<Notify>,
}
impl StatisticsJobNotification {
    pub fn notify(self) {
        self.changed.notify_waiters();
    }
}

impl StatisticsJobRepository {
    pub fn new() -> Self {
        Self::default()
    }

    /// Synchronously publish an already-paid typed Root into this original
    /// bounded LiveJob table. Failure leaves the entire source unchanged;
    /// success has no callback between taking it and installing its receiver.
    pub fn install_internal_job_in(
        &self,
        request: StatisticsJobCreate,
        source: &mut Option<StatisticsJobAdmission>,
    ) -> Result<(StatisticsJob, StatisticsJobNotification), StatisticsRepositoryError> {
        let original = source.as_ref().ok_or_else(|| {
            StatisticsRepositoryError::new(
                StatisticsRepositoryErrorKind::Conflict,
                "statistics admission source is absent",
            )
        })?;
        if original.root_payload.is_none()
            || !original
                .result_capacity
                .as_ref()
                .is_some_and(QueryResultCapacityBinding::is_root_backed_internal)
        {
            return Err(StatisticsRepositoryError::new(
                StatisticsRepositoryErrorKind::Conflict,
                "stable Root receiver requires its original typed payload",
            ));
        }
        let job = StatisticsJob::new(request);
        let mut state = self.lock()?;
        if state.active.len() >= MAX_ACTIVE_OR_QUEUED_STATISTICS_JOBS {
            return Err(StatisticsRepositoryError::new(
                StatisticsRepositoryErrorKind::Capacity,
                "statistics runtime active or queued job capacity exhausted",
            ));
        }
        if state.active.contains_key(&job.id)
            || state.terminal.iter().any(|entry| entry.job.id == job.id)
        {
            return Err(StatisticsRepositoryError::new(
                StatisticsRepositoryErrorKind::Conflict,
                "statistics job identity is already installed",
            ));
        }
        let StatisticsJobAdmission {
            owner,
            query_concurrency,
            result_capacity,
            root_payload,
            #[cfg(novarocks_dev_controls)]
            test_statement_nomination,
        } = source.take().expect("checked original admission source");
        state.active.insert(
            job.id,
            LiveJob {
                cancellation: owner.cancellation_requester(),
                transferred: false,
                exit: None,
                exit_failure: None,
                retained_exit: None,
                job: job.clone(),
                owner: Some(owner),
                query_concurrency: Some(query_concurrency),
                result_capacity,
                root_payload,
                stage_computation: None,
                #[cfg(novarocks_dev_controls)]
                test_statement_nomination,
            },
        );
        let notification = StatisticsJobNotification {
            changed: Arc::clone(&self.changed),
        };
        drop(state);
        Ok((job, notification))
    }

    /// Submit a statistics job with its exact whole-window admission receipt.
    pub async fn create_admitted(
        &self,
        request: StatisticsJobCreate,
        admission: StatisticsJobAdmission,
    ) -> Result<StatisticsJob, StatisticsRepositoryError> {
        self.create_now_admitted(request, admission)
    }

    fn create_now_admitted(
        &self,
        request: StatisticsJobCreate,
        admission: StatisticsJobAdmission,
    ) -> Result<StatisticsJob, StatisticsRepositoryError> {
        let StatisticsJobAdmission {
            owner,
            query_concurrency,
            result_capacity,
            root_payload,
            #[cfg(novarocks_dev_controls)]
            test_statement_nomination,
        } = admission;
        let mut state = self.lock()?;
        if state.active.len() >= MAX_ACTIVE_OR_QUEUED_STATISTICS_JOBS {
            return Err(StatisticsRepositoryError::new(
                StatisticsRepositoryErrorKind::Capacity,
                "statistics runtime active or queued job capacity exhausted",
            ));
        }
        let job = StatisticsJob::new(request);
        state.active.insert(
            job.id,
            LiveJob {
                cancellation: owner.cancellation_requester(),
                transferred: false,
                exit: None,
                exit_failure: None,
                retained_exit: None,
                #[cfg(novarocks_dev_controls)]
                test_statement_nomination,
                job: job.clone(),
                owner: Some(owner),
                query_concurrency: Some(query_concurrency),
                result_capacity,
                root_payload,
                stage_computation: None,
            },
        );
        drop(state);
        self.changed.notify_waiters();
        Ok(job)
    }

    /// Borrow this original stable Root's narrow growth source. Its body/Q
    /// must be constructed after this repository method returns and unlocks.
    pub fn root_stage_source(
        &self,
        id: StatisticsJobId,
        stage: &WorkOwner,
    ) -> Result<
        novarocks_workload_control::joint_admission::RootStageSource,
        StatisticsRepositoryError,
    > {
        let state = self.lock()?;
        let entry = state.active.get(&id).ok_or_else(|| Self::not_found(id))?;
        if entry.job.state.is_terminal() {
            return Err(Self::not_found(id));
        }
        let owner = entry.owner.as_ref().ok_or_else(|| Self::not_found(id))?;
        let payload = entry.root_payload.as_ref().ok_or_else(|| {
            StatisticsRepositoryError::new(
                StatisticsRepositoryErrorKind::Conflict,
                "statistics job has no original typed Root payload",
            )
        })?;
        payload
            .root_stage_source(&owner.scope(), stage)
            .map_err(work_error)
    }

    /// Both Root grant and compute receiver remain in this exact original job.
    /// The WC receipt is silent; notify its returned token only after unlock.
    pub fn install_root_stage_in(
        &self,
        id: StatisticsJobId,
        stage: &WorkOwner,
        ready: &mut novarocks_workload_control::joint_admission::RootStageReady,
    ) -> Result<novarocks_workload_control::WorkloadProgressNotification, StatisticsRepositoryError>
    {
        let mut state = self.lock()?;
        let entry = state
            .active
            .get_mut(&id)
            .ok_or_else(|| Self::not_found(id))?;
        let owner = entry.owner.as_ref().ok_or_else(|| Self::not_found(id))?;
        let payload = entry.root_payload.as_ref().ok_or_else(|| {
            StatisticsRepositoryError::new(
                StatisticsRepositoryErrorKind::Conflict,
                "statistics original Root payload is absent",
            )
        })?;
        ready
            .install_in(&owner.scope(), payload, stage, &mut entry.stage_computation)
            .map_err(work_error)
    }

    /// Silent move into the caller's already-stable original consumer slot.
    /// No source is removed on refusal and no callback runs on either outcome.
    pub fn transfer_stage_computation_in(
        &self,
        id: StatisticsJobId,
        stage: &WorkOwner,
        destination: &mut Option<
            novarocks_workload_control::joint_admission::InternalComputationGrant,
        >,
    ) -> Result<(), StatisticsRepositoryError> {
        if destination.is_some() {
            return Err(StatisticsRepositoryError::new(
                StatisticsRepositoryErrorKind::Conflict,
                "statistics stage receiver is occupied",
            ));
        }
        let mut state = self.lock()?;
        let entry = state
            .active
            .get_mut(&id)
            .ok_or_else(|| Self::not_found(id))?;
        if entry.stage_computation.is_none() {
            return Err(StatisticsRepositoryError::new(
                StatisticsRepositoryErrorKind::Conflict,
                "statistics stage computation is not installed",
            ));
        }
        if !entry
            .stage_computation
            .as_ref()
            .expect("checked original stage grant")
            .is_for_scope(&stage.scope())
        {
            return Err(StatisticsRepositoryError::new(
                StatisticsRepositoryErrorKind::Conflict,
                "statistics computation belongs to another actual stage owner",
            ));
        }
        *destination = entry.stage_computation.take();
        Ok(())
    }

    pub async fn get(
        &self,
        id: StatisticsJobId,
    ) -> Result<Option<StatisticsJob>, StatisticsRepositoryError> {
        let state = self.lock()?;
        Ok(state
            .active
            .get(&id)
            .map(|entry| entry.job.clone())
            .or_else(|| {
                state
                    .terminal
                    .iter()
                    .find(|entry| entry.job.id == id)
                    .map(|entry| entry.job.clone())
            }))
    }

    pub async fn wait_for_conclusion(
        &self,
        id: StatisticsJobId,
    ) -> Result<StatisticsJob, StatisticsRepositoryError> {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let job = self.get(id).await?.ok_or_else(|| Self::not_found(id))?;
            if job.state.is_terminal() {
                return Ok(job);
            }
            notified.await;
        }
    }

    pub async fn list(&self) -> Result<Vec<StatisticsJob>, StatisticsRepositoryError> {
        let state = self.lock()?;
        let mut jobs = state
            .active
            .values()
            .map(|entry| entry.job.clone())
            .chain(state.terminal.iter().map(|entry| entry.job.clone()))
            .collect::<Vec<_>>();
        jobs.sort_by_key(|job| (job.submitted_at_ms, job.id));
        Ok(jobs)
    }

    pub async fn claim_next(
        &self,
        at_ms: i64,
    ) -> Result<Option<StatisticsJob>, StatisticsRepositoryError> {
        let mut state = self.lock()?;
        let id = state
            .active
            .values()
            .filter(|entry| {
                entry.job.state == StatisticsJobState::Active(StatisticsJobPhase::Submitted)
                    && !entry.job.cancel_requested
            })
            .min_by_key(|entry| (entry.job.submitted_at_ms, entry.job.id))
            .map(|entry| entry.job.id);
        let Some(id) = id else {
            return Ok(None);
        };
        let entry = state.active.get_mut(&id).expect("selected job exists");
        entry.job.state = StatisticsJobState::Active(StatisticsJobPhase::Preparing);
        entry.job.logical_execution_id = Some(StatisticsLogicalExecutionId::new_v7());
        entry.job.query_attempt_id = Some(StatisticsQueryAttemptId::new_v7());
        entry.job.updated_at_ms = at_ms;
        let job = entry.job.clone();
        drop(state);
        self.changed.notify_waiters();
        Ok(Some(job))
    }

    /// Cancellation is an intent while collection/publication is active.  It
    /// never manufactures a stopped or released fact. A queued submission has
    /// not started any external operation, so it can conclude immediately.
    pub async fn request_cancel(
        &self,
        id: StatisticsJobId,
        at_ms: i64,
    ) -> Result<StatisticsJob, StatisticsRepositoryError> {
        self.request_cancel_now(id, at_ms)
    }

    /// Deliver cancellation even if no fresh wall-clock observation is available.
    /// Existing observation timestamps are retained; no time is invented.
    pub async fn request_cancel_without_time(
        &self,
        id: StatisticsJobId,
    ) -> Result<StatisticsJob, StatisticsRepositoryError> {
        self.request_cancel_with_time(id, None)
    }

    pub(crate) fn request_cancel_now(
        &self,
        id: StatisticsJobId,
        at_ms: i64,
    ) -> Result<StatisticsJob, StatisticsRepositoryError> {
        self.request_cancel_with_time(id, Some(at_ms))
    }

    fn request_cancel_with_time(
        &self,
        id: StatisticsJobId,
        at_ms: Option<i64>,
    ) -> Result<StatisticsJob, StatisticsRepositoryError> {
        let mut state = self.lock()?;
        let mut entry = state
            .active
            .remove(&id)
            .ok_or_else(|| Self::not_found(id))?;
        entry.job.cancel_requested = true;
        if let Some(at_ms) = at_ms {
            entry.job.updated_at_ms = at_ms;
        }
        if entry.job.state == StatisticsJobState::Active(StatisticsJobPhase::Submitted) {
            entry.job.state = StatisticsJobState::Terminal(StatisticsJobConclusion::Cancelled);
            entry.job.failure = Some(StatisticsFailure {
                compile_control: None,
                message: Arc::from("statistics job cancelled before collection"),
            });
            entry.job.completed_at_ms = at_ms;
            entry.job.convergence = StatisticsConvergence {
                collection_stopped: true,
                execution_resources_released: true,
                provider_session_closed: true,
            };
            #[cfg(novarocks_dev_controls)]
            drop(entry.test_statement_nomination.take());
            let retired = entry.take_capacity();
            let job = entry.job.clone();
            state.terminal.push_back(entry);
            Self::trim_terminal(&mut state);
            drop(state);
            retired.finish();
            self.changed.notify_waiters();
            return Ok(job);
        }
        let requester = entry.cancellation.clone();
        let job = entry.job.clone();
        state.active.insert(id, entry);
        drop(state);
        let _ = requester.request(novarocks_workload_control::CancellationReason::Requested);
        self.changed.notify_waiters();
        Ok(job)
    }

    /// Closes admission to the process-local runner by requesting cancellation
    /// of every active root. Queued jobs have no provider session or query
    /// work, so they are terminally cancelled here; running jobs retain their
    /// root until their executor reports actual convergence.
    pub(crate) fn request_stop_for_process_exit(
        &self,
        at_ms: i64,
    ) -> Result<(), StatisticsRepositoryError> {
        let mut state = self.lock()?;
        let mut retired = Vec::with_capacity(state.active.len());
        let mut queued = Vec::new();
        let mut cancellation_requesters = Vec::new();
        for (id, entry) in &mut state.active {
            entry.job.cancel_requested = true;
            entry.job.updated_at_ms = at_ms;
            if entry.job.state == StatisticsJobState::Active(StatisticsJobPhase::Submitted) {
                queued.push(*id);
            } else {
                cancellation_requesters.push(entry.cancellation.clone());
            }
        }
        for id in queued {
            let mut entry = state.active.remove(&id).expect("queued job exists");
            entry.job.state = StatisticsJobState::Terminal(StatisticsJobConclusion::Cancelled);
            entry.job.failure = Some(StatisticsFailure {
                compile_control: None,
                message: Arc::from("statistics job cancelled during process shutdown"),
            });
            entry.job.completed_at_ms = Some(at_ms);
            entry.job.convergence = StatisticsConvergence {
                collection_stopped: true,
                execution_resources_released: true,
                provider_session_closed: true,
            };
            #[cfg(novarocks_dev_controls)]
            drop(entry.test_statement_nomination.take());
            retired.push(entry.take_capacity());
            state.terminal.push_back(entry);
        }
        Self::trim_terminal(&mut state);
        drop(state);
        for capacity in retired {
            capacity.finish();
        }
        for requester in cancellation_requesters {
            let _ = requester.request(novarocks_workload_control::CancellationReason::Requested);
        }
        self.changed.notify_waiters();
        Ok(())
    }

    pub async fn phase(
        &self,
        id: StatisticsJobId,
        expected: StatisticsJobPhase,
        next: StatisticsJobPhase,
        at_ms: i64,
    ) -> Result<StatisticsJob, StatisticsRepositoryError> {
        self.phase_now(id, expected, next, at_ms)
    }

    fn phase_now(
        &self,
        id: StatisticsJobId,
        expected: StatisticsJobPhase,
        next: StatisticsJobPhase,
        at_ms: i64,
    ) -> Result<StatisticsJob, StatisticsRepositoryError> {
        let mut state = self.lock()?;
        let entry = state
            .active
            .get_mut(&id)
            .ok_or_else(|| Self::not_found(id))?;
        if entry.job.state != StatisticsJobState::Active(expected) {
            return Err(StatisticsRepositoryError::new(
                StatisticsRepositoryErrorKind::Conflict,
                format!(
                    "statistics job {:?} is not in expected phase {expected:?}",
                    id.as_uuid()
                ),
            ));
        }
        if !matches!(
            (expected, next),
            (
                StatisticsJobPhase::Preparing,
                StatisticsJobPhase::Collecting
            ) | (
                StatisticsJobPhase::Collecting,
                StatisticsJobPhase::Publishing
            )
        ) {
            return Err(StatisticsRepositoryError::new(
                StatisticsRepositoryErrorKind::InvalidTransition,
                "statistics business phases cannot be skipped or replayed",
            ));
        }
        entry.job.state = StatisticsJobState::Active(next);
        entry.job.updated_at_ms = at_ms;
        let job = entry.job.clone();
        drop(state);
        self.changed.notify_waiters();
        Ok(job)
    }

    pub async fn conclude(
        &self,
        id: StatisticsJobId,
        expected: StatisticsJobPhase,
        conclusion: StatisticsJobConclusion,
        publication: StatisticsPublicationFact,
        failure: Option<StatisticsFailure>,
        at_ms: i64,
    ) -> Result<StatisticsJob, StatisticsRepositoryError> {
        self.conclude_now(
            id,
            expected,
            conclusion,
            publication,
            failure,
            None,
            None,
            None,
            at_ms,
        )
    }

    fn conclude_now(
        &self,
        id: StatisticsJobId,
        expected: StatisticsJobPhase,
        conclusion: StatisticsJobConclusion,
        publication: StatisticsPublicationFact,
        failure: Option<StatisticsFailure>,
        publication_failure: Option<StatisticsPublicationFailure>,
        finalization_failure: Option<StatisticsPublicationFailure>,
        cleanup_failure: Option<StatisticsPublicationFailure>,
        at_ms: i64,
    ) -> Result<StatisticsJob, StatisticsRepositoryError> {
        let mut state = self.lock()?;
        let mut entry = state
            .active
            .remove(&id)
            .ok_or_else(|| Self::not_found(id))?;
        if entry.job.state != StatisticsJobState::Active(expected) {
            state.active.insert(id, entry);
            return Err(StatisticsRepositoryError::new(
                StatisticsRepositoryErrorKind::Conflict,
                "statistics job conclusion conflicts with its current business phase",
            ));
        }
        if matches!(conclusion, StatisticsJobConclusion::CommitUnknown)
            != matches!(publication, StatisticsPublicationFact::CommitUnknown)
        {
            state.active.insert(id, entry);
            return Err(StatisticsRepositoryError::new(
                StatisticsRepositoryErrorKind::InvalidTransition,
                "CommitUnknown conclusion must retain the provider's unknown publication fact",
            ));
        }
        entry.job.state = StatisticsJobState::Terminal(conclusion);
        entry.job.publication = publication;
        entry.job.failure = failure;
        entry.job.publication_failure = publication_failure;
        entry.job.publication_finalization_failure = finalization_failure;
        entry.job.publication_cleanup_failure = cleanup_failure;
        entry.job.updated_at_ms = at_ms;
        entry.job.completed_at_ms = Some(at_ms);
        state.terminal.push_back(entry);
        Self::trim_terminal(&mut state);
        let job = state
            .terminal
            .back()
            .expect("terminal job inserted")
            .job
            .clone();
        drop(state);
        self.changed.notify_waiters();
        Ok(job)
    }

    /// Records actual stop/release facts after a business conclusion.  A
    /// terminal state cannot be used as a synthetic release notification.
    pub async fn record_convergence(
        &self,
        id: StatisticsJobId,
        convergence: StatisticsConvergence,
        at_ms: i64,
    ) -> Result<StatisticsJob, StatisticsRepositoryError> {
        let mut retired = None;
        let mut state = self.lock()?;
        let entry = if state.active.contains_key(&id) {
            state
                .active
                .get_mut(&id)
                .expect("checked active job exists")
        } else {
            state
                .terminal
                .iter_mut()
                .find(|entry| entry.job.id == id)
                .ok_or_else(|| Self::not_found(id))?
        };
        entry.job.convergence.collection_stopped |= convergence.collection_stopped;
        entry.job.convergence.execution_resources_released |=
            convergence.execution_resources_released;
        entry.job.convergence.provider_session_closed |= convergence.provider_session_closed;
        entry.job.updated_at_ms = at_ms;
        #[cfg(novarocks_dev_controls)]
        if entry.job.convergence.is_complete() {
            drop(entry.test_statement_nomination.take());
        }
        if entry.job.state.is_terminal() && entry.job.convergence.is_complete() {
            retired = Some(entry.take_capacity());
        }
        let job = entry.job.clone();
        drop(state);
        if let Some(retired) = retired {
            retired.finish();
        }
        self.changed.notify_waiters();
        Ok(job)
    }

    /// The existing sequential process worker polls these original observers.
    /// Each observer is taken out of the repository mutex before foreign poll
    /// and receipt Drop. One finite pass visits the existing bounded records.
    pub(crate) fn poll_exits(
        &self,
        cx: &mut Context<'_>,
        at_ms: i64,
    ) -> Poll<Result<(), StatisticsRepositoryError>> {
        let (id, mut observer, more) = {
            let mut state = self.lock()?;
            let next = state
                .active
                .values()
                .chain(state.terminal.iter())
                .filter(|e| {
                    e.exit.is_some() && state.exit_cursor.is_none_or(|after| e.job.id > after)
                })
                .map(|e| e.job.id)
                .min();
            let Some(id) = next else {
                state.exit_cursor = None;
                return Poll::Pending;
            };
            state.exit_cursor = Some(id);
            let more = state
                .active
                .values()
                .chain(state.terminal.iter())
                .any(|e| e.exit.is_some() && e.job.id > id);
            let original = find_entry_mut(&mut state, id)?
                .exit
                .take()
                .expect("selected exact observer");
            if !more {
                state.exit_cursor = None;
            }
            (id, Some(original), more)
        };
        let result = observer.as_mut().unwrap().poll_exit(cx);
        let mut retired = None;
        let mut changed = false;
        {
            let mut state = self.lock()?;
            let entry = find_entry_mut(&mut state, id)?;
            match result {
                Poll::Pending => entry.exit = observer.take(),
                Poll::Ready(Ok(actual)) => {
                    if actual.local_disposition() == LocalExecutionDisposition::ActualSourcesExited
                    {
                        entry.job.convergence = StatisticsConvergence {
                            collection_stopped: true,
                            execution_resources_released: true,
                            provider_session_closed: true,
                        };
                        entry.job.updated_at_ms = at_ms;
                        retired = Some(actual);
                    } else {
                        // Return to an original paid owner is not actual exit.
                        entry.retained_exit = Some(actual);
                    }
                    changed = true;
                }
                Poll::Ready(Err(error)) => {
                    entry.exit_failure = Some(error);
                    changed = true;
                }
            }
        }
        drop(retired);
        drop(observer);
        if more {
            cx.waker().wake_by_ref();
        }
        if changed {
            self.changed.notify_waiters();
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, RuntimeState>, StatisticsRepositoryError> {
        self.state.lock().map_err(|_| {
            StatisticsRepositoryError::new(
                StatisticsRepositoryErrorKind::Conflict,
                "statistics runtime lock poisoned",
            )
        })
    }

    fn not_found(id: StatisticsJobId) -> StatisticsRepositoryError {
        StatisticsRepositoryError::new(
            StatisticsRepositoryErrorKind::NotFound,
            format!(
                "statistics job {:?} is not in this frontend process",
                id.as_uuid()
            ),
        )
    }

    fn trim_terminal(state: &mut RuntimeState) {
        // Keep unresolved responsibility and actual-exit custody even when
        // recent history exceeds its retention target. These terminal tails
        // do not consume active or queued business admission positions.
        while state.terminal.len() > MAX_RECENT_TERMINAL_STATISTICS_JOBS {
            let Some(index) = state.terminal.iter().position(|entry| {
                entry.job.convergence.is_complete()
                    && entry.exit.is_none()
                    && entry.exit_failure.is_none()
                    && entry.retained_exit.is_none()
            }) else {
                break;
            };
            state.terminal.remove(index);
        }
    }
}

/// One original job handoff. The role adapter mounts the original admission,
/// installs its actual observer through Progress, then awaits only NeededReady.
/// The product worker never owns a second runner or a per-phase execution root.
pub trait StatisticsAttemptExecutor: Send + Sync + 'static {
    fn execute<'a>(
        &'a self,
        job: StatisticsJob,
        admission: StatisticsJobAdmission,
        progress: StatisticsJobProgress,
    ) -> Pin<Box<dyn Future<Output = Result<(), StatisticsAttemptError>> + Send + 'a>>;
}

/// Product-owned phase and business result reducer. It observes no transport
/// facts and does not manufacture actual-exit evidence from business success.
#[derive(Clone)]
pub struct StatisticsJobProgress {
    repository: StatisticsJobRepository,
    id: StatisticsJobId,
}
impl StatisticsJobProgress {
    pub fn prepared(&self, at_ms: i64) -> Result<(), StatisticsRepositoryError> {
        self.repository
            .phase_now(
                self.id,
                StatisticsJobPhase::Preparing,
                StatisticsJobPhase::Collecting,
                at_ms,
            )
            .map(|_| ())
    }
    pub fn collected(&self, at_ms: i64) -> Result<(), StatisticsRepositoryError> {
        self.repository
            .phase_now(
                self.id,
                StatisticsJobPhase::Collecting,
                StatisticsJobPhase::Publishing,
                at_ms,
            )
            .map(|_| ())
    }
    /// The source slots stay unchanged on refusal. No callback occurs while
    /// the sole original observer is moved into this existing job record.
    pub fn install_exit_in(
        &self,
        source: &mut Option<LogicalExecutionExitObserver>,
    ) -> Result<(), StatisticsRepositoryError> {
        let mut state = self.repository.lock()?;
        // The same bank may select business Ready immediately after start;
        // its exact original record can already be in the terminal deque.
        let entry = find_entry_mut(&mut state, self.id)?;
        if !entry.transferred || entry.exit.is_some() || source.is_none() {
            return Err(work_error(novarocks_workload_control::WorkError::Conflict));
        }
        entry.exit = source.take();
        Ok(())
    }
    /// No background role was mounted. The adapter returns the unchanged
    /// original Root only after its startup proof has stopped; no Provider F
    /// is allowed before mount. Complete/drop all originals outside the table.
    pub fn retire_unmounted(
        &self,
        mut originals: StatisticsJobOriginals,
        at_ms: i64,
    ) -> Result<(), StatisticsRepositoryError> {
        let owner = originals
            .owner
            .take()
            .ok_or_else(|| work_error(novarocks_workload_control::WorkError::Conflict))?;
        {
            let state = self.repository.lock()?;
            let entry = state
                .active
                .get(&self.id)
                .or_else(|| state.terminal.iter().find(|e| e.job.id == self.id))
                .ok_or_else(|| StatisticsJobRepository::not_found(self.id))?;
            if entry.exit.is_some() || entry.retained_exit.is_some() {
                return Err(work_error(novarocks_workload_control::WorkError::Conflict));
            }
        }
        drop(originals);
        owner.complete_after_terminal_cancel_settled();
        {
            let mut state = self.repository.lock()?;
            let entry = find_entry_mut(&mut state, self.id)?;
            entry.job.convergence = StatisticsConvergence {
                collection_stopped: true,
                execution_resources_released: true,
                provider_session_closed: true,
            };
            entry.job.updated_at_ms = at_ms;
        }
        self.repository.changed.notify_waiters();
        Ok(())
    }
    pub fn record_publication_cleanup_failure(
        &self,
        failure: StatisticsPublicationFailure,
    ) -> Result<(), StatisticsRepositoryError> {
        let mut state = self.repository.lock()?;
        let entry = find_entry_mut(&mut state, self.id)?;
        if entry.job.publication_cleanup_failure.is_some() {
            return Ok(());
        }
        entry.job.publication_cleanup_failure = Some(failure);
        drop(state);
        self.repository.changed.notify_waiters();
        Ok(())
    }
    pub fn finish(
        &self,
        result: Result<StatisticsPublicationOutcome, StatisticsAttemptError>,
        at_ms: i64,
    ) -> Result<(), StatisticsRepositoryError> {
        let phase = {
            let state = self.repository.lock()?;
            let entry = state
                .active
                .get(&self.id)
                .ok_or_else(|| StatisticsJobRepository::not_found(self.id))?;
            let StatisticsJobState::Active(phase) = entry.job.state else {
                unreachable!()
            };
            phase
        };
        let (
            conclusion,
            publication,
            failure,
            publication_failure,
            finalization_failure,
            cleanup_failure,
        ) = publication_result(result);
        self.repository.conclude_now(
            self.id,
            phase,
            conclusion,
            publication,
            failure,
            publication_failure,
            finalization_failure,
            cleanup_failure,
            at_ms,
        )?;
        Ok(())
    }
}

/// Only legal publication combinations cross the product boundary. Pre-dispatch
/// failure is StatisticsAttemptError, never a fabricated publication verdict.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StatisticsPublicationOutcome {
    NoOp {
        finalization_failure: Option<StatisticsPublicationFailure>,
        cleanup_failure: Option<StatisticsPublicationFailure>,
    },
    KnownCommitted {
        finalization_failure: Option<StatisticsPublicationFailure>,
        cleanup_failure: Option<StatisticsPublicationFailure>,
    },
    KnownUncommitted {
        failure: StatisticsPublicationFailure,
        cleanup_failure: Option<StatisticsPublicationFailure>,
    },
    CommitUnknown {
        failure: StatisticsPublicationFailure,
    },
}
impl StatisticsPublicationOutcome {
    pub const fn fact(&self) -> StatisticsPublicationFact {
        match self {
            Self::NoOp { .. } => StatisticsPublicationFact::NoOp,
            Self::KnownCommitted { .. } => StatisticsPublicationFact::KnownCommitted,
            Self::KnownUncommitted { .. } => StatisticsPublicationFact::KnownUncommitted,
            Self::CommitUnknown { .. } => StatisticsPublicationFact::CommitUnknown,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StatisticsAttemptError {
    Failed(StatisticsFailure),
    Stale(StatisticsFailure),
    Cancelled(StatisticsFailure),
}

impl StatisticsAttemptError {
    fn conclusion(&self) -> StatisticsJobConclusion {
        match self {
            Self::Failed(_) => StatisticsJobConclusion::Failed,
            Self::Stale(_) => StatisticsJobConclusion::Stale,
            Self::Cancelled(_) => StatisticsJobConclusion::Cancelled,
        }
    }

    fn failure(&self) -> StatisticsFailure {
        match self {
            Self::Failed(error) | Self::Stale(error) | Self::Cancelled(error) => error.clone(),
        }
    }
}

/// Executes queued process-local jobs without turning an observer detach into
/// job cancellation.  It has no retry/recovery loop: a `CommitUnknown` job is
/// terminal and is never made eligible for another provider publication.
pub struct StatisticsWorker {
    repository: StatisticsJobRepository,
    executor: Arc<dyn StatisticsAttemptExecutor>,
}

impl StatisticsWorker {
    pub fn new(
        repository: StatisticsJobRepository,
        executor: Arc<dyn StatisticsAttemptExecutor>,
    ) -> Self {
        Self {
            repository,
            executor,
        }
    }
    pub async fn run_one(
        &self,
        at_ms: i64,
    ) -> Result<Option<StatisticsJob>, StatisticsRepositoryError> {
        let Some(job) = self.repository.claim_next(at_ms).await? else {
            return Ok(None);
        };
        let admission = {
            let mut state = self.repository.lock()?;
            let entry = state.active.get_mut(&job.id).expect("claimed original job");
            if entry.transferred {
                return Err(work_error(novarocks_workload_control::WorkError::Conflict));
            }
            entry.transferred = true;
            StatisticsJobAdmission {
                owner: entry.owner.take().expect("original Root"),
                query_concurrency: entry.query_concurrency.take().expect("original permit"),
                result_capacity: entry.result_capacity.take(),
                root_payload: entry.root_payload.take(),
                #[cfg(novarocks_dev_controls)]
                test_statement_nomination: entry.test_statement_nomination.take(),
            }
        };
        let progress = StatisticsJobProgress {
            repository: self.repository.clone(),
            id: job.id,
        };
        let result = self
            .executor
            .execute(job.clone(), admission, progress.clone())
            .await;
        if let Err(error) = result {
            // A host refusal must not overwrite the domain's earlier NeededReady.
            if self
                .repository
                .get(job.id)
                .await?
                .is_some_and(|job| !job.state.is_terminal())
            {
                progress.finish(Err(error), at_ms)?;
            }
        }
        self.repository.get(job.id).await
    }
}

fn publication_result(
    result: Result<StatisticsPublicationOutcome, StatisticsAttemptError>,
) -> (
    StatisticsJobConclusion,
    StatisticsPublicationFact,
    Option<StatisticsFailure>,
    Option<StatisticsPublicationFailure>,
    Option<StatisticsPublicationFailure>,
    Option<StatisticsPublicationFailure>,
) {
    match result {
        Ok(StatisticsPublicationOutcome::NoOp {
            finalization_failure,
            cleanup_failure,
        }) => (
            if finalization_failure.is_some() {
                StatisticsJobConclusion::Failed
            } else {
                StatisticsJobConclusion::Succeeded
            },
            StatisticsPublicationFact::NoOp,
            None,
            None,
            finalization_failure,
            cleanup_failure,
        ),
        Ok(StatisticsPublicationOutcome::KnownCommitted {
            finalization_failure,
            cleanup_failure,
        }) => (
            if finalization_failure.is_some() {
                StatisticsJobConclusion::Failed
            } else {
                StatisticsJobConclusion::Succeeded
            },
            StatisticsPublicationFact::KnownCommitted,
            None,
            None,
            finalization_failure,
            cleanup_failure,
        ),
        Ok(StatisticsPublicationOutcome::CommitUnknown { failure }) => (
            StatisticsJobConclusion::CommitUnknown,
            StatisticsPublicationFact::CommitUnknown,
            None,
            Some(failure),
            None,
            None,
        ),
        Ok(StatisticsPublicationOutcome::KnownUncommitted {
            failure,
            cleanup_failure,
        }) => (
            StatisticsJobConclusion::Failed,
            StatisticsPublicationFact::KnownUncommitted,
            None,
            Some(failure),
            None,
            cleanup_failure,
        ),
        Err(error) => (
            error.conclusion(),
            StatisticsPublicationFact::NotStarted,
            Some(error.failure()),
            None,
            None,
            None,
        ),
    }
}

fn find_entry_mut(
    state: &mut RuntimeState,
    id: StatisticsJobId,
) -> Result<&mut LiveJob, StatisticsRepositoryError> {
    if state.active.contains_key(&id) {
        return Ok(state.active.get_mut(&id).unwrap());
    }
    state
        .terminal
        .iter_mut()
        .find(|entry| entry.job.id == id)
        .ok_or_else(|| StatisticsJobRepository::not_found(id))
}

fn work_error(error: novarocks_workload_control::WorkError) -> StatisticsRepositoryError {
    StatisticsRepositoryError::new(
        StatisticsRepositoryErrorKind::Conflict,
        format!("statistics child work scope admission failed: {error}"),
    )
}

#[cfg(test)]
trait StatisticsPhaseFixture: Send + Sync + 'static {
    fn prepare(
        &self,
        job: &StatisticsJob,
        context: &StatisticsAttemptContext,
    ) -> Result<(), StatisticsAttemptError>;
    fn collect(
        &self,
        job: &StatisticsJob,
        context: &StatisticsAttemptContext,
    ) -> Result<(), StatisticsAttemptError>;
    fn publish(
        &self,
        job: &StatisticsJob,
        context: &StatisticsAttemptContext,
    ) -> Result<StatisticsPublicationOutcome, StatisticsAttemptError>;
}
#[cfg(test)]
impl<T: StatisticsPhaseFixture> StatisticsAttemptExecutor for T {
    fn execute<'a>(
        &'a self,
        job: StatisticsJob,
        admission: StatisticsJobAdmission,
        progress: StatisticsJobProgress,
    ) -> Pin<Box<dyn Future<Output = Result<(), StatisticsAttemptError>> + Send + 'a>> {
        Box::pin(async move {
            let mut original = admission.into_originals();
            let root = original.owner.as_ref().unwrap().scope();
            let capacity = original.result_capacity.as_ref().unwrap().clone();
            let failed = |error: StatisticsRepositoryError| {
                StatisticsAttemptError::Failed(StatisticsFailure {
                    compile_control: None,
                    message: Arc::from(error.to_string()),
                })
            };
            let work = |error: novarocks_workload_control::WorkError| {
                StatisticsAttemptError::Cancelled(StatisticsFailure {
                    compile_control: None,
                    message: Arc::from(error.to_string()),
                })
            };
            let result = (|| {
                let prepare = root
                    .child(WorkRequest::new(WorkClass::Statistics))
                    .map_err(work)?;
                let context = StatisticsAttemptContext {
                    stage_scope: prepare.scope(),
                    root_result_capacity: capacity.clone(),
                    #[cfg(novarocks_dev_controls)]
                    test_statement_nomination: Mutex::new(
                        original.test_statement_nomination.take(),
                    ),
                };
                let prepared = self.prepare(&job, &context);
                drop(context);
                prepare.complete();
                prepared?;
                progress.prepared(job.updated_at_ms).map_err(failed)?;
                let collect = root
                    .child(WorkRequest::new(WorkClass::Statistics))
                    .map_err(work)?;
                let context = StatisticsAttemptContext {
                    stage_scope: collect.scope(),
                    root_result_capacity: capacity.clone(),
                    #[cfg(novarocks_dev_controls)]
                    test_statement_nomination: Mutex::new(None),
                };
                let collected = self.collect(&job, &context);
                drop(context);
                collect.complete();
                collected?;
                progress.collected(job.updated_at_ms).map_err(failed)?;
                let publish = root
                    .child(WorkRequest::new(WorkClass::Statistics))
                    .map_err(work)?;
                let context = StatisticsAttemptContext {
                    stage_scope: publish.scope(),
                    root_result_capacity: capacity.clone(),
                    #[cfg(novarocks_dev_controls)]
                    test_statement_nomination: Mutex::new(None),
                };
                let result = self.publish(&job, &context);
                drop(context);
                publish.complete();
                result
            })();
            drop(capacity);
            progress
                .retire_unmounted(original, job.updated_at_ms)
                .map_err(failed)?;
            progress.finish(result, job.updated_at_ms).map_err(failed)?;
            Ok(())
        })
    }
}

#[cfg(test)]
#[path = "tests_process_runtime.rs"]
mod process_runtime_tests;

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    use novarocks_workload_control::{ResourceConfig, WorkloadConfig, WorkloadControl};

    use super::*;

    #[test]
    fn bounded_publication_observation_keeps_facts_but_not_original_custody() {
        use novarocks_failure_contract::{
            FailureClass, FailureFact, FailureProjection, FailureReason, FailureRowPosition,
            OuterFailureProjection, ResourceDimension, ResourceFacts,
        };
        use novarocks_type_contract::{CompileControlError, CompileControlProvenance};
        use std::num::NonZeroU16;
        #[derive(Debug)]
        struct Original {
            fact: FailureFact,
            dropped: Arc<AtomicUsize>,
        }
        impl fmt::Display for Original {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("private-token=secret;")?;
                for _ in 0..1024 {
                    f.write_str("原")?;
                }
                Ok(())
            }
        }
        impl std::error::Error for Original {}
        impl FailureProjection for Original {
            fn failure_fact(&self) -> FailureFact {
                self.fact
            }
        }
        impl OuterFailureProjection for Original {}
        impl Drop for Original {
            fn drop(&mut self) {
                self.dropped.fetch_add(1, Ordering::SeqCst);
            }
        }
        let fact = FailureFact::new(
            FailureClass::Resource,
            FailureReason::new(
                NonZeroU16::new(u16::MAX).unwrap(),
                NonZeroU16::new(u16::MAX).unwrap(),
            ),
        )
        .with_resource(ResourceFacts {
            request_scope: None,
            constraint_scope: None,
            dimension: ResourceDimension::Bytes,
            requested: Some(99),
            required_growth: Some(9),
            limit: Some(90),
            live: Some(81),
            committed: Some(3),
        })
        .with_row_position(FailureRowPosition {
            selected_ordinal: 7,
            batch_row: 11,
        });
        let dropped = Arc::new(AtomicUsize::new(0));
        let original = CompileControlError::from_domain(Original {
            fact,
            dropped: dropped.clone(),
        });
        let record = StatisticsPublicationFailure::capture(
            original.failure_fact(),
            Some(original.provenance()),
            &original,
        );
        assert_eq!(record.fact(), fact);
        assert_eq!(
            record.compile_control(),
            Some(CompileControlProvenance::Source(fact))
        );
        assert!(record.local_diagnostic().contains("private-token=secret"));
        assert!(
            record.local_diagnostic().len() <= novarocks_failure_contract::DIAGNOSTIC_MAX_BYTES
        );
        assert!(record.local_diagnostic().ends_with('原'));
        assert!(!record.public_message().contains("secret"));
        assert!(!format!("{record:?}").contains("secret"));
        assert!(
            record.public_message().len() <= StatisticsPublicationFailure::PUBLIC_MESSAGE_BYTES
        );
        let historical = record;
        drop(original);
        assert_eq!(
            dropped.load(Ordering::SeqCst),
            1,
            "history must not pin the Provider tree"
        );
        assert_eq!(historical.fact(), fact);
        assert_eq!(
            historical.compile_control(),
            Some(CompileControlProvenance::Source(fact))
        );
    }

    #[test]
    fn diagnostic_formatter_panic_cannot_change_publication_facts() {
        use novarocks_failure_contract::FailureProjection;
        struct PanickingDiagnostic;
        impl fmt::Display for PanickingDiagnostic {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("private-prefix")?;
                panic!("diagnostic formatter failure")
            }
        }
        let original = novarocks_type_contract::CompileControlError::DeadlineExceeded;
        let record = StatisticsPublicationFailure::capture(
            original.failure_fact(),
            Some(original.provenance()),
            &PanickingDiagnostic,
        );
        assert_eq!(record.fact(), original.failure_fact());
        assert_eq!(record.compile_control(), Some(original.provenance()));
        assert_eq!(record.local_diagnostic(), "private-prefix");
        assert!(!record.public_message().contains("private"));
    }

    pub(super) fn create(at_ms: i64) -> StatisticsJobCreate {
        StatisticsJobCreate {
            target: StatisticsTarget {
                catalog: Arc::from("iceberg"),
                namespace: Arc::from("db"),
                table: Arc::from("orders"),
                object_id: Arc::from(&b"orders-v1"[..]),
            },
            columns: StatisticsColumns::All,
            submitted_at_ms: at_ms,
        }
    }

    pub(super) async fn root() -> StatisticsJobAdmission {
        root_with_capacity().await.0
    }

    async fn root_with_capacity() -> (
        StatisticsJobAdmission,
        novarocks_workload_control::ResultCapacityHandle,
    ) {
        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 16 * 1024 * 1024,
                control_bytes: 1024 * 1024,
                per_scope_bytes: 8 * 1024 * 1024,
            },
        )
        .expect("control");
        let capacity = control
            .configure_result_capacity(novarocks_workload_control::ResultCapacityConfig::V1)
            .expect("capacity");
        control.mark_ready().expect("ready");
        let root = control
            .root_admission()
            .begin_warehouse_root(WorkRequest::new(WorkClass::Statistics))
            .expect("root");
        let (permit, window) = root
            .owner
            .scope()
            .admit_query_with_result(novarocks_workload_control::ResultWindowClass::Internal)
            .unwrap()
            .await
            .unwrap();
        (
            StatisticsJobAdmission::try_new(root.owner, permit, window).unwrap(),
            capacity,
        )
    }

    pub(super) async fn retain_unknown_terminal_jobs(
        repository: &StatisticsJobRepository,
        count: usize,
    ) -> Vec<(StatisticsJob, WorkScope)> {
        let mut retained = Vec::with_capacity(count);
        for index in 0..count {
            let admission = root().await;
            let scope = admission.scope();
            let job = repository
                .create_admitted(create(index as i64), admission)
                .await
                .unwrap();
            assert_eq!(repository.claim_next(1).await.unwrap().unwrap().id, job.id);
            repository
                .phase(
                    job.id,
                    StatisticsJobPhase::Preparing,
                    StatisticsJobPhase::Collecting,
                    2,
                )
                .await
                .unwrap();
            repository
                .phase(
                    job.id,
                    StatisticsJobPhase::Collecting,
                    StatisticsJobPhase::Publishing,
                    3,
                )
                .await
                .unwrap();
            let terminal = repository
                .conclude(
                    job.id,
                    StatisticsJobPhase::Publishing,
                    StatisticsJobConclusion::CommitUnknown,
                    StatisticsPublicationFact::CommitUnknown,
                    Some(StatisticsFailure {
                        compile_control: None,
                        message: Arc::from("original publication outcome remains unknown"),
                    }),
                    4,
                )
                .await
                .unwrap();
            assert_eq!(terminal.convergence, StatisticsConvergence::default());
            scope.check().unwrap();
            retained.push((terminal, scope));
        }
        retained
    }

    pub(super) async fn close_local_terminal_fixtures(
        repository: &StatisticsJobRepository,
        retained: Vec<(StatisticsJob, WorkScope)>,
    ) {
        // These repository fixtures installed no Native or Provider operation.
        // Close their original local holders only after all custody assertions.
        for (job, scope) in retained {
            repository
                .record_convergence(
                    job.id,
                    StatisticsConvergence {
                        collection_stopped: true,
                        execution_resources_released: true,
                        provider_session_closed: true,
                    },
                    5,
                )
                .await
                .unwrap();
            assert!(matches!(
                scope.check(),
                Err(novarocks_workload_control::WorkError::Released)
            ));
        }
    }

    #[tokio::test]
    async fn terminal_history_trim_retains_unknown_responsibility_without_completing_it() {
        let repository = StatisticsJobRepository::new();
        let retained = retain_unknown_terminal_jobs(&repository, 1).await;
        let (original, scope) = &retained[0];
        let mut first_completed = None;
        for index in 0..MAX_RECENT_TERMINAL_STATISTICS_JOBS {
            let queued = repository
                .create_admitted(create(index as i64 + 10), root().await)
                .await
                .unwrap();
            first_completed.get_or_insert(queued.id);
            let cancelled = repository.request_cancel(queued.id, 20).await.unwrap();
            assert!(cancelled.convergence.is_complete());
        }
        assert_eq!(
            repository.get(original.id).await.unwrap().as_ref(),
            Some(original)
        );
        assert!(
            repository
                .get(first_completed.unwrap())
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            repository.list().await.unwrap().len(),
            MAX_RECENT_TERMINAL_STATISTICS_JOBS
        );
        scope.check().unwrap();
        {
            let state = repository.lock().unwrap();
            let tail = state
                .terminal
                .iter()
                .find(|entry| entry.job.id == original.id)
                .unwrap();
            assert!(tail.owner.is_some() && tail.query_concurrency.is_some());
            assert!(tail.result_capacity.is_some());
            assert_eq!(tail.job.convergence, StatisticsConvergence::default());
        }
        close_local_terminal_fixtures(&repository, retained).await;
    }

    #[tokio::test]
    async fn queued_job_cancel_returns_only_its_own_holder_and_last_alias_releases_capacity() {
        let (admission, capacity) = root_with_capacity().await;
        let retained = admission.result_capacity.clone();
        let repository = StatisticsJobRepository::new();
        let job = repository
            .create_admitted(create(1), admission)
            .await
            .unwrap();
        assert_eq!(capacity.snapshot().held_positions, [0, 0, 1, 0]);
        let cancelled = repository.request_cancel(job.id, 2).await.unwrap();
        assert!(cancelled.convergence.is_complete());
        assert_eq!(capacity.snapshot().held_positions, [0, 0, 1, 0]);
        assert!(
            repository
                .lock()
                .unwrap()
                .terminal
                .back()
                .unwrap()
                .result_capacity
                .is_none()
        );
        drop(retained);
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }

    #[tokio::test]
    async fn admission_refuses_a_foreign_host_query_permit_even_when_root_ids_match() {
        use novarocks_workload_control::{ResultCapacityConfig, ResultWindowClass};
        let make = || {
            let control = WorkloadControl::try_new(
                WorkloadConfig::default(),
                ResourceConfig {
                    total_bytes: 1024 * 1024,
                    control_bytes: 1024,
                    per_scope_bytes: 1024 * 1024 - 1024,
                },
            )
            .unwrap();
            let capacity = control
                .configure_result_capacity(ResultCapacityConfig::V1)
                .unwrap();
            control.mark_ready().unwrap();
            (control, capacity)
        };
        let (control, capacity) = make();
        let (foreign, foreign_capacity) = make();
        let first = control
            .root_admission()
            .begin_warehouse_root(WorkRequest::new(WorkClass::Statistics))
            .unwrap();
        let second = foreign
            .root_admission()
            .begin_warehouse_root(WorkRequest::new(WorkClass::Statistics))
            .unwrap();
        assert_eq!(first.owner.scope().id(), second.owner.scope().id());
        let (permit, window) = first
            .owner
            .scope()
            .admit_query_with_result(ResultWindowClass::Internal)
            .unwrap()
            .await
            .unwrap();
        let (foreign_permit, foreign_window) = second
            .owner
            .scope()
            .admit_query_with_result(ResultWindowClass::Internal)
            .unwrap()
            .await
            .unwrap();
        let refused = StatisticsJobAdmission::try_new(first.owner, foreign_permit, window)
            .err()
            .unwrap();
        assert_eq!(refused.kind(), StatisticsRepositoryErrorKind::Conflict);
        drop(permit);
        drop(foreign_window);
        second.owner.complete();
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        assert_eq!(foreign_capacity.snapshot().held_positions, [0; 4]);
    }

    struct RecordingExecutor {
        publications: AtomicUsize,
        finalization_fails: bool,
    }

    struct BlockingExecutor {
        started: mpsc::Sender<()>,
        release: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    }

    impl StatisticsAttemptExecutor for BlockingExecutor {
        fn execute<'a>(
            &'a self,
            job: StatisticsJob,
            admission: StatisticsJobAdmission,
            progress: StatisticsJobProgress,
        ) -> Pin<Box<dyn Future<Output = Result<(), StatisticsAttemptError>> + Send + 'a>> {
            Box::pin(async move {
                let original = admission.into_originals();
                let root = original.owner.as_ref().unwrap().scope();
                self.started
                    .send(())
                    .expect("test observes actual preparation");
                let release = self.release.lock().unwrap().take().unwrap();
                release.await.expect("test releases original preparation");
                let result = match root.check() {
                    Err(error) => Err(StatisticsAttemptError::Cancelled(StatisticsFailure {
                        compile_control: None,
                        message: Arc::from(error.to_string()),
                    })),
                    Ok(()) => {
                        progress.prepared(job.updated_at_ms).unwrap();
                        progress.collected(job.updated_at_ms).unwrap();
                        Ok(StatisticsPublicationOutcome::KnownCommitted {
                            finalization_failure: None,
                            cleanup_failure: None,
                        })
                    }
                };
                progress
                    .retire_unmounted(original, job.updated_at_ms)
                    .unwrap();
                progress.finish(result, job.updated_at_ms).unwrap();
                Ok(())
            })
        }
    }

    impl StatisticsPhaseFixture for RecordingExecutor {
        fn prepare(
            &self,
            _job: &StatisticsJob,
            context: &StatisticsAttemptContext,
        ) -> Result<(), StatisticsAttemptError> {
            context
                .root_result_capacity()
                .scope()
                .check()
                .expect("job root remains live across phases");
            assert_eq!(
                context.root_result_capacity().class(),
                novarocks_workload_control::ResultWindowClass::Internal
            );
            assert!(
                !context
                    .root_result_capacity()
                    .window_alias()
                    .is_for_scope(context.stage_scope())
            );
            context.stage_scope().check().map_err(|error| {
                StatisticsAttemptError::Failed(StatisticsFailure {
                    compile_control: None,
                    message: Arc::from(error.to_string()),
                })
            })
        }

        fn collect(
            &self,
            _job: &StatisticsJob,
            context: &StatisticsAttemptContext,
        ) -> Result<(), StatisticsAttemptError> {
            context
                .root_result_capacity()
                .scope()
                .check()
                .expect("job root remains live across phases");
            assert_eq!(
                context.root_result_capacity().class(),
                novarocks_workload_control::ResultWindowClass::Internal
            );
            assert!(
                !context
                    .root_result_capacity()
                    .window_alias()
                    .is_for_scope(context.stage_scope())
            );
            context.stage_scope().check().map_err(|error| {
                StatisticsAttemptError::Failed(StatisticsFailure {
                    compile_control: None,
                    message: Arc::from(error.to_string()),
                })
            })
        }

        fn publish(
            &self,
            _job: &StatisticsJob,
            context: &StatisticsAttemptContext,
        ) -> Result<StatisticsPublicationOutcome, StatisticsAttemptError> {
            context
                .root_result_capacity()
                .scope()
                .check()
                .expect("job root remains live across phases");
            assert_eq!(
                context.root_result_capacity().class(),
                novarocks_workload_control::ResultWindowClass::Internal
            );
            assert!(
                !context
                    .root_result_capacity()
                    .window_alias()
                    .is_for_scope(context.stage_scope())
            );
            context.stage_scope().check().map_err(|error| {
                StatisticsAttemptError::Failed(StatisticsFailure {
                    compile_control: None,
                    message: Arc::from(error.to_string()),
                })
            })?;
            self.publications.fetch_add(1, Ordering::SeqCst);
            if self.finalization_fails {
                Ok(StatisticsPublicationOutcome::KnownCommitted {
                    cleanup_failure: None,
                    finalization_failure: Some(test_publication_failure(
                        "accelerator finalization failed",
                    )),
                })
            } else {
                Ok(StatisticsPublicationOutcome::CommitUnknown {
                    failure: crate::test_publication_failure(
                        "original provider publication unknown",
                    ),
                })
            }
        }
    }

    #[tokio::test]
    async fn job_query_attempt_and_publication_have_distinct_v7_identities() {
        let repository = StatisticsJobRepository::new();
        let job = repository
            .create_admitted(create(1), root().await)
            .await
            .expect("create");
        let claimed = repository.claim_next(2).await.expect("claim").expect("job");
        assert_ne!(
            job.id.as_uuid(),
            claimed.logical_execution_id.unwrap().as_uuid()
        );
        assert_ne!(
            job.id.as_uuid(),
            claimed.query_attempt_id.unwrap().as_uuid()
        );
        assert_ne!(
            job.publication_id.as_uuid(),
            claimed.query_attempt_id.unwrap().as_uuid()
        );
        for id in [
            job.id.as_uuid(),
            claimed.logical_execution_id.unwrap().as_uuid(),
            claimed.query_attempt_id.unwrap().as_uuid(),
            job.publication_id.as_uuid(),
        ] {
            assert_eq!(id.get_version_num(), 7);
        }
    }

    #[tokio::test]
    async fn submitted_is_not_success_and_an_observer_has_no_cancellation_authority() {
        let repository = StatisticsJobRepository::new();
        let job = repository
            .create_admitted(create(1), root().await)
            .await
            .expect("create");
        assert_eq!(
            job.state,
            StatisticsJobState::Active(StatisticsJobPhase::Submitted)
        );
        assert!(!job.state.is_terminal());
        assert_eq!(job.publication, StatisticsPublicationFact::NotStarted);
    }

    #[tokio::test]
    async fn job_service_owns_submission_listing_and_cancellation() {
        let service = StatisticsJobService::new();
        let submitted = service
            .submit_admitted(create(1), root().await)
            .await
            .expect("submit");
        assert_eq!(service.list().await.expect("list"), vec![submitted.clone()]);

        let cancelled = service
            .request_cancel(submitted.id, 2)
            .await
            .expect("request cancellation");
        assert_eq!(cancelled.id, submitted.id);
        assert_eq!(
            cancelled.state,
            StatisticsJobState::Terminal(StatisticsJobConclusion::Cancelled)
        );
    }

    #[tokio::test]
    async fn runtime_returns_submission_before_its_background_attempt_concludes() {
        let service = StatisticsJobService::new();
        let (started, started_rx) = mpsc::channel();
        let (release, release_rx) = tokio::sync::oneshot::channel();
        let runtime = StatisticsJobRuntime::start(
            service.clone(),
            Arc::new(BlockingExecutor {
                started,
                release: Mutex::new(Some(release_rx)),
            }),
            tokio::runtime::Handle::current(),
        );

        let submitted = runtime
            .submit_admitted(create(1), root().await)
            .await
            .expect("submit");
        tokio::task::spawn_blocking(move || {
            started_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("background preparation started")
        })
        .await
        .expect("observe preparation");
        let observed = service
            .list()
            .await
            .expect("list")
            .into_iter()
            .find(|job| job.id == submitted.id)
            .expect("submitted job remains observable");
        assert_eq!(
            observed.state,
            StatisticsJobState::Active(StatisticsJobPhase::Preparing)
        );

        release.send(()).expect("release preparation");
        let terminal = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let job = service
                    .list()
                    .await
                    .expect("list")
                    .into_iter()
                    .find(|job| job.id == submitted.id)
                    .expect("job remains retained");
                if job.state.is_terminal() {
                    return job;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("background job reaches terminal");
        assert_eq!(
            terminal.state,
            StatisticsJobState::Terminal(StatisticsJobConclusion::Succeeded)
        );
        runtime
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .expect("shutdown worker");
    }

    #[tokio::test]
    async fn runtime_cancellation_reaches_an_active_attempt_root() {
        let service = StatisticsJobService::new();
        let (started, started_rx) = mpsc::channel();
        let (release, release_rx) = tokio::sync::oneshot::channel();
        let runtime = StatisticsJobRuntime::start(
            service.clone(),
            Arc::new(BlockingExecutor {
                started,
                release: Mutex::new(Some(release_rx)),
            }),
            tokio::runtime::Handle::current(),
        );

        let submitted = runtime
            .submit_admitted(create(1), root().await)
            .await
            .expect("submit");
        tokio::task::spawn_blocking(move || {
            started_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("background preparation started")
        })
        .await
        .expect("observe preparation");
        runtime
            .request_cancel(submitted.id, 2)
            .await
            .expect("request cancellation");
        release.send(()).expect("release preparation");

        let terminal = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let job = service
                    .list()
                    .await
                    .expect("list")
                    .into_iter()
                    .find(|job| job.id == submitted.id)
                    .expect("job remains retained");
                // A business conclusion is deliberately visible before its
                // resource-release facts are all observed. This test is
                // specifically about cancellation reaching the active root,
                // so wait for the independent convergence record rather than
                // treating the first terminal observation as synthetic proof
                // of cleanup.
                if job.state.is_terminal() && job.convergence.is_complete() {
                    return job;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancelled background job reaches terminal");
        assert_eq!(
            terminal.state,
            StatisticsJobState::Terminal(StatisticsJobConclusion::Cancelled)
        );
        assert!(terminal.convergence.is_complete());
        runtime
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .expect("shutdown worker");
    }

    #[tokio::test]
    async fn process_stop_cancels_the_active_root_and_waits_for_convergence() {
        let service = StatisticsJobService::new();
        let (started, started_rx) = mpsc::channel();
        let (release, release_rx) = tokio::sync::oneshot::channel();
        let runtime = StatisticsJobRuntime::start(
            service.clone(),
            Arc::new(BlockingExecutor {
                started,
                release: Mutex::new(Some(release_rx)),
            }),
            tokio::runtime::Handle::current(),
        );

        let submitted = runtime
            .submit_admitted(create(1), root().await)
            .await
            .expect("submit");
        tokio::task::spawn_blocking(move || {
            started_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("background preparation started")
        })
        .await
        .expect("observe preparation");

        runtime.request_stop_for_process_exit();
        release.send(()).expect("release preparation");
        runtime
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .expect("shutdown worker");

        let terminal = service
            .list()
            .await
            .expect("list")
            .into_iter()
            .find(|job| job.id == submitted.id)
            .expect("job remains retained");
        assert_eq!(
            terminal.state,
            StatisticsJobState::Terminal(StatisticsJobConclusion::Cancelled)
        );
        assert!(terminal.convergence.is_complete());
    }

    #[tokio::test]
    async fn process_stop_rejects_new_submission_without_creating_a_job() {
        let service = StatisticsJobService::new();
        let runtime = StatisticsJobRuntime::start(
            service.clone(),
            Arc::new(RecordingExecutor {
                publications: AtomicUsize::new(0),
                finalization_fails: false,
            }),
            tokio::runtime::Handle::current(),
        );

        runtime.request_stop_for_process_exit();
        let error = runtime
            .submit_admitted(create(1), root().await)
            .await
            .expect_err("stopping worker rejects admission");
        assert_eq!(error.kind(), StatisticsRepositoryErrorKind::Conflict);
        assert!(service.list().await.expect("list").is_empty());
        runtime
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .expect("shutdown worker");
    }

    #[tokio::test]
    async fn process_stop_converges_a_queued_root_without_dispatching_it() {
        let service = StatisticsJobService::new();
        let queued = service
            .submit_admitted(create(1), root().await)
            .await
            .expect("queue job");
        let runtime = StatisticsJobRuntime::start(
            service.clone(),
            Arc::new(RecordingExecutor {
                publications: AtomicUsize::new(0),
                finalization_fails: false,
            }),
            tokio::runtime::Handle::current(),
        );

        runtime.request_stop_for_process_exit();
        runtime
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .expect("shutdown worker");

        let terminal = service
            .list()
            .await
            .expect("list")
            .into_iter()
            .find(|job| job.id == queued.id)
            .expect("queued job remains retained");
        assert_eq!(
            terminal.state,
            StatisticsJobState::Terminal(StatisticsJobConclusion::Cancelled)
        );
        assert!(terminal.convergence.is_complete());
        assert!(terminal.query_attempt_id.is_none());
    }

    #[tokio::test]
    async fn commit_unknown_is_terminal_and_never_republished() {
        let repository = StatisticsJobRepository::new();
        let executor = Arc::new(RecordingExecutor {
            publications: AtomicUsize::new(0),
            finalization_fails: false,
        });
        let worker = StatisticsWorker::new(repository.clone(), executor.clone());
        let job = repository
            .create_admitted(create(1), root().await)
            .await
            .expect("create");
        let terminal = worker.run_one(2).await.expect("run").expect("terminal");
        assert_eq!(
            terminal.state,
            StatisticsJobState::Terminal(StatisticsJobConclusion::CommitUnknown)
        );
        assert_eq!(
            terminal.publication,
            StatisticsPublicationFact::CommitUnknown
        );
        assert_eq!(executor.publications.load(Ordering::SeqCst), 1);
        assert!(worker.run_one(3).await.expect("idle").is_none());
        assert_eq!(executor.publications.load(Ordering::SeqCst), 1);
        assert_eq!(
            repository.get(job.id).await.expect("get").unwrap().state,
            terminal.state
        );
    }

    #[tokio::test]
    async fn known_committed_finalization_failure_keeps_commit_fact() {
        let repository = StatisticsJobRepository::new();
        let executor = Arc::new(RecordingExecutor {
            publications: AtomicUsize::new(0),
            finalization_fails: true,
        });
        let worker = StatisticsWorker::new(repository.clone(), executor);
        let job = repository
            .create_admitted(create(1), root().await)
            .await
            .expect("create");
        let terminal = worker.run_one(2).await.expect("run").expect("terminal");
        assert_eq!(
            terminal.state,
            StatisticsJobState::Terminal(StatisticsJobConclusion::Failed)
        );
        assert_eq!(
            terminal.publication,
            StatisticsPublicationFact::KnownCommitted
        );
        assert_eq!(
            terminal.publication_finalization_failure,
            Some(test_publication_failure("accelerator finalization failed"))
        );
        assert!(terminal.convergence.execution_resources_released);
        assert!(terminal.convergence.provider_session_closed);
        assert!(
            repository
                .get(job.id)
                .await
                .expect("get")
                .unwrap()
                .convergence
                .is_complete()
        );
    }

    #[tokio::test]
    async fn stale_is_a_reachable_business_conclusion() {
        struct StaleExecutor;

        impl StatisticsPhaseFixture for StaleExecutor {
            fn prepare(
                &self,
                _job: &StatisticsJob,
                _context: &StatisticsAttemptContext,
            ) -> Result<(), StatisticsAttemptError> {
                Err(StatisticsAttemptError::Stale(StatisticsFailure {
                    compile_control: None,
                    message: Arc::from("captured table object was replaced"),
                }))
            }

            fn collect(
                &self,
                _job: &StatisticsJob,
                _context: &StatisticsAttemptContext,
            ) -> Result<(), StatisticsAttemptError> {
                unreachable!("stale preparation must not start collection")
            }

            fn publish(
                &self,
                _job: &StatisticsJob,
                _context: &StatisticsAttemptContext,
            ) -> Result<StatisticsPublicationOutcome, StatisticsAttemptError> {
                unreachable!("stale preparation must not publish")
            }
        }

        let repository = StatisticsJobRepository::new();
        let job = repository
            .create_admitted(create(1), root().await)
            .await
            .expect("create");
        let terminal = StatisticsWorker::new(repository, Arc::new(StaleExecutor))
            .run_one(2)
            .await
            .expect("run")
            .expect("terminal");
        assert_eq!(
            terminal.state,
            StatisticsJobState::Terminal(StatisticsJobConclusion::Stale)
        );
        assert_eq!(terminal.publication, StatisticsPublicationFact::NotStarted);
        assert_eq!(terminal.id, job.id);
    }

    #[tokio::test]
    async fn terminal_conclusion_does_not_synthesize_resource_convergence() {
        let repository = StatisticsJobRepository::new();
        let job = repository
            .create_admitted(create(1), root().await)
            .await
            .expect("create");
        let claimed = repository.claim_next(2).await.expect("claim").expect("job");
        let terminal = repository
            .conclude(
                claimed.id,
                StatisticsJobPhase::Preparing,
                StatisticsJobConclusion::Failed,
                StatisticsPublicationFact::NotStarted,
                Some(StatisticsFailure {
                    compile_control: None,
                    message: Arc::from("preparation failed"),
                }),
                3,
            )
            .await
            .expect("conclude");
        assert!(!terminal.convergence.collection_stopped);
        assert!(!terminal.convergence.execution_resources_released);
        assert!(!terminal.convergence.provider_session_closed);
        let converged = repository
            .record_convergence(
                job.id,
                StatisticsConvergence {
                    collection_stopped: true,
                    execution_resources_released: true,
                    provider_session_closed: true,
                },
                4,
            )
            .await
            .expect("actual convergence");
        assert!(converged.convergence.is_complete());
    }
    #[tokio::test]
    async fn conclusion_wait_returns_business_before_actual_convergence_releases_root() {
        let repository = StatisticsJobRepository::new();
        let (admission, capacity) = root_with_capacity().await;
        let scope = admission.scope();
        let job = repository
            .create_admitted(create(1), admission)
            .await
            .unwrap();
        let claimed = repository.claim_next(2).await.unwrap().unwrap();
        assert_eq!(
            claimed.state,
            StatisticsJobState::Active(StatisticsJobPhase::Preparing)
        );
        let wait = repository.wait_for_conclusion(job.id);
        tokio::pin!(wait);
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(wait.as_mut().poll(cx).is_pending()))
                .await
        );
        scope.check().unwrap();
        assert_eq!(capacity.snapshot().held_positions, [0, 0, 1, 0]);
        let failure = StatisticsFailure {
            compile_control: None,
            message: Arc::from("failed"),
        };
        let concluded = repository
            .conclude(
                job.id,
                StatisticsJobPhase::Preparing,
                StatisticsJobConclusion::Failed,
                StatisticsPublicationFact::NotStarted,
                Some(failure.clone()),
                3,
            )
            .await
            .unwrap();
        let terminal = std::future::poll_fn(|cx| match wait.as_mut().poll(cx) {
            std::task::Poll::Ready(result) => std::task::Poll::Ready(result.unwrap()),
            std::task::Poll::Pending => {
                panic!("the original business waiter must not wait for resource convergence")
            }
        })
        .await;
        assert_eq!(terminal, concluded);
        assert_eq!(
            terminal.state,
            StatisticsJobState::Terminal(StatisticsJobConclusion::Failed)
        );
        assert_eq!(terminal.publication, StatisticsPublicationFact::NotStarted);
        assert_eq!(terminal.failure, Some(failure.clone()));
        assert_eq!(terminal.convergence, StatisticsConvergence::default());
        scope.check().unwrap();
        assert_eq!(capacity.snapshot().held_positions, [0, 0, 1, 0]);

        let partial = repository
            .record_convergence(
                job.id,
                StatisticsConvergence {
                    collection_stopped: true,
                    execution_resources_released: true,
                    provider_session_closed: false,
                },
                4,
            )
            .await
            .unwrap();
        assert!(partial.convergence.collection_stopped);
        assert!(partial.convergence.execution_resources_released);
        assert!(!partial.convergence.provider_session_closed);
        assert!(!partial.convergence.is_complete());
        assert_eq!(partial.state, terminal.state);
        assert_eq!(partial.failure, Some(failure.clone()));
        assert_eq!(
            repository.wait_for_conclusion(job.id).await.unwrap(),
            partial
        );
        scope.check().unwrap();
        assert_eq!(capacity.snapshot().held_positions, [0, 0, 1, 0]);

        let converged = repository
            .record_convergence(
                job.id,
                StatisticsConvergence {
                    collection_stopped: false,
                    execution_resources_released: false,
                    provider_session_closed: true,
                },
                5,
            )
            .await
            .unwrap();
        assert_eq!(converged.state, terminal.state);
        assert_eq!(converged.failure, Some(failure));
        assert!(converged.convergence.is_complete());
        assert!(matches!(
            scope.check(),
            Err(novarocks_workload_control::WorkError::Released)
        ));
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        assert_eq!(
            repository.wait_for_conclusion(job.id).await.unwrap(),
            converged
        );
    }

    #[tokio::test]
    async fn queued_cancel_wait_returns_only_the_complete_terminal_record() {
        let service = StatisticsJobService::new();
        let job = service
            .submit_admitted(create(1), root().await)
            .await
            .unwrap();
        service.request_cancel(job.id, 2).await.unwrap();
        let terminal = service.wait_for_conclusion(job.id).await.unwrap();
        assert_eq!(
            terminal.state,
            StatisticsJobState::Terminal(StatisticsJobConclusion::Cancelled)
        );
        assert!(terminal.convergence.is_complete());
    }

    #[tokio::test]
    async fn cancellation_without_clock_preserves_observation_time_and_converges() {
        let service = StatisticsJobService::new();
        let job = service
            .submit_admitted(create(17), root().await)
            .await
            .unwrap();
        service.request_cancel_without_time(job.id).await.unwrap();
        let terminal = service.wait_for_conclusion(job.id).await.unwrap();
        assert_eq!(terminal.updated_at_ms, 17);
        assert_eq!(
            terminal.completed_at_ms, None,
            "no completion time is fabricated"
        );
        assert_eq!(
            terminal.state,
            StatisticsJobState::Terminal(StatisticsJobConclusion::Cancelled)
        );
        assert!(terminal.convergence.is_complete());
    }
    #[tokio::test]
    async fn business_completion_allows_next_job_without_inventing_original_exit() {
        struct HoldingExecutor(Mutex<Vec<StatisticsJobOriginals>>);
        impl StatisticsAttemptExecutor for HoldingExecutor {
            fn execute<'a>(
                &'a self,
                job: StatisticsJob,
                admission: StatisticsJobAdmission,
                progress: StatisticsJobProgress,
            ) -> Pin<Box<dyn Future<Output = Result<(), StatisticsAttemptError>> + Send + 'a>>
            {
                Box::pin(async move {
                    // This product fixture retains actual original owners. It
                    // supplies no bank/exit receipt and must remain unresolved.
                    self.0.lock().unwrap().push(admission.into_originals());
                    progress.prepared(job.updated_at_ms).unwrap();
                    progress.collected(job.updated_at_ms).unwrap();
                    progress
                        .finish(
                            Ok(StatisticsPublicationOutcome::KnownCommitted {
                                finalization_failure: None,
                                cleanup_failure: None,
                            }),
                            job.updated_at_ms,
                        )
                        .unwrap();
                    Ok(())
                })
            }
        }
        let repository = StatisticsJobRepository::new();
        let first = repository
            .create_admitted(create(1), root().await)
            .await
            .unwrap();
        let second = repository
            .create_admitted(create(2), root().await)
            .await
            .unwrap();
        let executor = Arc::new(HoldingExecutor(Mutex::new(Vec::new())));
        let worker = StatisticsWorker::new(repository.clone(), executor.clone());
        assert_eq!(worker.run_one(3).await.unwrap().unwrap().id, first.id);
        assert_eq!(worker.run_one(4).await.unwrap().unwrap().id, second.id);
        for id in [first.id, second.id] {
            let business = repository.wait_for_conclusion(id).await.unwrap();
            assert_eq!(
                business.state,
                StatisticsJobState::Terminal(StatisticsJobConclusion::Succeeded)
            );
            assert!(
                !business.convergence.is_complete(),
                "business cannot fabricate original exit"
            );
        }
        {
            let mut state = repository.lock().unwrap();
            StatisticsJobRepository::trim_terminal(&mut state);
            assert_eq!(state.terminal.len(), 2);
            assert!(state.terminal.iter().all(|entry| entry.transferred
                && entry.owner.is_none()
                && !entry.job.convergence.is_complete()));
        }
        let originals = std::mem::take(&mut *executor.0.lock().unwrap());
        for mut original in originals {
            let owner = original.owner.take().unwrap();
            owner.scope().check().unwrap();
            drop(original);
            owner.complete_after_terminal_cancel_settled();
        }
        // Fixture cleanup is not published as a fabricated observer receipt.
        assert!(
            repository
                .list()
                .await
                .unwrap()
                .iter()
                .all(|job| !job.convergence.is_complete())
        );
    }
    #[tokio::test]
    async fn late_cleanup_diagnostic_preserves_business_and_original_convergence() {
        let repository = StatisticsJobRepository::new();
        let created = repository
            .create_admitted(create(1), root().await)
            .await
            .unwrap();
        let job = repository.claim_next(2).await.unwrap().unwrap();
        let progress = StatisticsJobProgress {
            repository: repository.clone(),
            id: job.id,
        };
        progress.prepared(3).unwrap();
        progress.collected(4).unwrap();
        progress
            .finish(
                Ok(StatisticsPublicationOutcome::KnownCommitted {
                    finalization_failure: None,
                    cleanup_failure: None,
                }),
                5,
            )
            .unwrap();
        let before = repository.get(created.id).await.unwrap().unwrap();
        assert!(!before.convergence.is_complete());
        let cleanup = test_publication_failure("late original GC failure");
        progress
            .record_publication_cleanup_failure(cleanup)
            .unwrap();
        progress
            .record_publication_cleanup_failure(cleanup)
            .unwrap();
        let after = repository.get(created.id).await.unwrap().unwrap();
        assert_eq!(
            after.state,
            StatisticsJobState::Terminal(StatisticsJobConclusion::Succeeded)
        );
        assert_eq!(after.publication, StatisticsPublicationFact::KnownCommitted);
        assert_eq!(after.convergence, before.convergence);
        assert!(after.failure.is_none());
        assert_eq!(after.publication_cleanup_failure, Some(cleanup));
        assert_eq!(after.completed_at_ms, before.completed_at_ms);
    }
}

// Refuse flags that would make capability reporting disagree with compiled seams.
#[cfg(any(
    all(novarocks_dev_controls, not(debug_assertions)),
    all(not(novarocks_dev_controls), debug_assertions)
))]
compile_error!("novarocks_dev_controls must match debug_assertions");

#[cfg(test)]
mod internal_root_tests;
