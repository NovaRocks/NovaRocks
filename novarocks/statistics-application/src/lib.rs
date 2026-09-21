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

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex};

use novarocks_workload_control::{
    QueryConcurrencyPermit, WorkClass, WorkOwner, WorkRequest, WorkScope,
};
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
    /// A known commit remains a fact when post-commit projection fails; that
    /// failure must not authorize another provider mutation.
    pub publication_finalization_failure: Option<StatisticsFailure>,
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
            publication_finalization_failure: None,
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

struct LiveJob {
    job: StatisticsJob,
    /// The business root remains owned until actual runtime convergence, not
    /// merely until a terminal job conclusion becomes observable.
    owner: Option<WorkOwner>,
    /// An independently submitted statistics job is warehouse compute. Its
    /// permit lasts through actual job convergence, alongside the root owner.
    query_concurrency: Option<QueryConcurrencyPermit>,
}

#[derive(Default)]
struct RuntimeState {
    active: HashMap<StatisticsJobId, LiveJob>,
    terminal: VecDeque<LiveJob>,
}

/// Bounded, process-local observation of statistics jobs.  It deliberately
/// has no persistence/restart recovery protocol.
#[derive(Clone, Default)]
pub struct StatisticsJobRepository {
    state: Arc<Mutex<RuntimeState>>,
    changed: Arc<Notify>,
}

impl StatisticsJobRepository {
    pub fn new() -> Self {
        Self::default()
    }

    /// Submission consumes the business root.  The observer of an ANALYZE
    /// command therefore cannot detach and accidentally cancel the job root.
    pub async fn create(
        &self,
        request: StatisticsJobCreate,
        owner: WorkOwner,
    ) -> Result<StatisticsJob, StatisticsRepositoryError> {
        self.create_now(request, owner)
    }

    /// Submit a statistics job after it has received the warehouse query
    /// permit. The repository owns both facts until terminal convergence.
    pub async fn create_admitted(
        &self,
        request: StatisticsJobCreate,
        owner: WorkOwner,
        query_concurrency: QueryConcurrencyPermit,
    ) -> Result<StatisticsJob, StatisticsRepositoryError> {
        self.create_now_with_permit(request, owner, Some(query_concurrency))
    }

    pub(crate) fn create_now(
        &self,
        request: StatisticsJobCreate,
        owner: WorkOwner,
    ) -> Result<StatisticsJob, StatisticsRepositoryError> {
        self.create_now_with_permit(request, owner, None)
    }

    fn create_now_with_permit(
        &self,
        request: StatisticsJobCreate,
        owner: WorkOwner,
        query_concurrency: Option<QueryConcurrencyPermit>,
    ) -> Result<StatisticsJob, StatisticsRepositoryError> {
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
                job: job.clone(),
                owner: Some(owner),
                query_concurrency,
            },
        );
        drop(state);
        self.changed.notify_waiters();
        Ok(job)
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

    pub(crate) fn request_cancel_now(
        &self,
        id: StatisticsJobId,
        at_ms: i64,
    ) -> Result<StatisticsJob, StatisticsRepositoryError> {
        let mut state = self.lock()?;
        let mut entry = state
            .active
            .remove(&id)
            .ok_or_else(|| Self::not_found(id))?;
        entry.job.cancel_requested = true;
        entry.job.updated_at_ms = at_ms;
        if entry.job.state == StatisticsJobState::Active(StatisticsJobPhase::Submitted) {
            entry.job.state = StatisticsJobState::Terminal(StatisticsJobConclusion::Cancelled);
            entry.job.failure = Some(StatisticsFailure {
                message: Arc::from("statistics job cancelled before collection"),
            });
            entry.job.completed_at_ms = Some(at_ms);
            entry.job.convergence = StatisticsConvergence {
                collection_stopped: true,
                execution_resources_released: true,
                provider_session_closed: true,
            };
            drop(entry.query_concurrency.take());
            if let Some(owner) = entry.owner.take() {
                owner.complete();
            }
            let job = entry.job.clone();
            state.terminal.push_back(entry);
            Self::trim_terminal(&mut state);
            drop(state);
            self.changed.notify_waiters();
            return Ok(job);
        }
        if let Some(owner) = entry.owner.as_ref() {
            let _ = owner
                .cancellation_requester()
                .request(novarocks_workload_control::CancellationReason::Requested);
        }
        let job = entry.job.clone();
        state.active.insert(id, entry);
        drop(state);
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
        let mut queued = Vec::new();
        let mut cancellation_requesters = Vec::new();
        for (id, entry) in &mut state.active {
            entry.job.cancel_requested = true;
            entry.job.updated_at_ms = at_ms;
            if entry.job.state == StatisticsJobState::Active(StatisticsJobPhase::Submitted) {
                queued.push(*id);
            } else if let Some(owner) = entry.owner.as_ref() {
                cancellation_requesters.push(owner.cancellation_requester());
            }
        }
        for id in queued {
            let mut entry = state.active.remove(&id).expect("queued job exists");
            entry.job.state = StatisticsJobState::Terminal(StatisticsJobConclusion::Cancelled);
            entry.job.failure = Some(StatisticsFailure {
                message: Arc::from("statistics job cancelled during process shutdown"),
            });
            entry.job.completed_at_ms = Some(at_ms);
            entry.job.convergence = StatisticsConvergence {
                collection_stopped: true,
                execution_resources_released: true,
                provider_session_closed: true,
            };
            drop(entry.query_concurrency.take());
            if let Some(owner) = entry.owner.take() {
                owner.complete();
            }
            state.terminal.push_back(entry);
        }
        Self::trim_terminal(&mut state);
        drop(state);
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
        if entry.job.state.is_terminal() && entry.job.convergence.is_complete() {
            drop(entry.query_concurrency.take());
            if let Some(owner) = entry.owner.take() {
                owner.complete();
            }
        }
        let job = entry.job.clone();
        drop(state);
        self.changed.notify_waiters();
        Ok(job)
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
        // Do not evict terminal jobs whose work root still owns convergence
        // responsibility.  Capacity is bounded by refusing later submissions
        // if an implementation cannot converge old jobs.
        while state.terminal.len() > MAX_RECENT_TERMINAL_STATISTICS_JOBS {
            let Some(index) = state
                .terminal
                .iter()
                .position(|entry| entry.owner.is_none())
            else {
                break;
            };
            state.terminal.remove(index);
        }
    }
}

/// Product adapter to the generic query application.  Implementations create
/// a single `QueryExecutionClient` request from the frozen provider facts and
/// consume the exact result before returning from `collect`.
///
/// Every method is a convergence boundary: on either `Ok` or `Err`, the
/// adapter has stopped its phase-local query work and released phase-local
/// resources before returning. `collect` must also close a provider session
/// when it cannot hand its exact artifacts to `publish`. A successful
/// `collect` may retain only the provider publication session needed by the
/// following `publish`; `publish` closes that session before it returns. This
/// makes the worker's convergence records evidence from the phase contract,
/// rather than an inference from the terminal business conclusion.
pub trait StatisticsAttemptExecutor: Send + Sync + 'static {
    fn prepare(&self, job: &StatisticsJob, scope: &WorkScope)
    -> Result<(), StatisticsAttemptError>;

    fn collect(&self, job: &StatisticsJob, scope: &WorkScope)
    -> Result<(), StatisticsAttemptError>;

    fn publish(
        &self,
        job: &StatisticsJob,
        scope: &WorkScope,
    ) -> Result<StatisticsPublicationOutcome, StatisticsAttemptError>;
}

/// Exact provider publication result. A known commit with a finalization
/// failure is neither an uncommitted effect nor an unknown effect.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsPublicationOutcome {
    pub fact: StatisticsPublicationFact,
    pub finalization_failure: Option<StatisticsFailure>,
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
        let root_scope = self.root_scope(job.id)?;
        let preparation = root_scope
            .child(WorkRequest::new(WorkClass::Statistics))
            .map_err(work_error)?;
        let preparation_scope = preparation.scope();
        if let Err(error) = self.executor.prepare(&job, &preparation_scope) {
            preparation.complete();
            return self
                .conclude_error(job, StatisticsJobPhase::Preparing, error, at_ms)
                .await
                .map(Some);
        }
        preparation.complete();
        self.repository
            .phase(
                job.id,
                StatisticsJobPhase::Preparing,
                StatisticsJobPhase::Collecting,
                at_ms,
            )
            .await?;
        let collection = root_scope
            .child(WorkRequest::new(WorkClass::Statistics))
            .map_err(work_error)?;
        let collection_scope = collection.scope();
        if let Err(error) = self.executor.collect(&job, &collection_scope) {
            collection.complete();
            self.repository
                .record_convergence(
                    job.id,
                    StatisticsConvergence {
                        collection_stopped: true,
                        execution_resources_released: true,
                        provider_session_closed: false,
                    },
                    at_ms,
                )
                .await?;
            return self
                .conclude_error(job, StatisticsJobPhase::Collecting, error, at_ms)
                .await
                .map(Some);
        }
        collection.complete();
        self.repository
            .record_convergence(
                job.id,
                StatisticsConvergence {
                    collection_stopped: true,
                    execution_resources_released: true,
                    provider_session_closed: false,
                },
                at_ms,
            )
            .await?;
        self.repository
            .phase(
                job.id,
                StatisticsJobPhase::Collecting,
                StatisticsJobPhase::Publishing,
                at_ms,
            )
            .await?;
        let publication = root_scope
            .child(WorkRequest::new(WorkClass::Statistics))
            .map_err(work_error)?;
        let publication_scope = publication.scope();
        let result = self.executor.publish(&job, &publication_scope);
        publication.complete();
        let (conclusion, publication_fact, failure, finalization_failure) = match result {
            Ok(StatisticsPublicationOutcome {
                fact: StatisticsPublicationFact::KnownCommitted,
                finalization_failure,
            }) => (
                StatisticsJobConclusion::Succeeded,
                StatisticsPublicationFact::KnownCommitted,
                None,
                finalization_failure,
            ),
            Ok(StatisticsPublicationOutcome {
                fact: StatisticsPublicationFact::CommitUnknown,
                ..
            }) => (
                StatisticsJobConclusion::CommitUnknown,
                StatisticsPublicationFact::CommitUnknown,
                Some(StatisticsFailure {
                    message: Arc::from("provider returned an unknown publication outcome"),
                }),
                None,
            ),
            Ok(StatisticsPublicationOutcome {
                fact: StatisticsPublicationFact::KnownUncommitted,
                ..
            }) => (
                StatisticsJobConclusion::Failed,
                StatisticsPublicationFact::KnownUncommitted,
                Some(StatisticsFailure {
                    message: Arc::from("provider publication was not committed"),
                }),
                None,
            ),
            Ok(StatisticsPublicationOutcome {
                fact: StatisticsPublicationFact::NotStarted,
                ..
            }) => (
                StatisticsJobConclusion::Failed,
                StatisticsPublicationFact::NotStarted,
                Some(StatisticsFailure {
                    message: Arc::from("publication executor returned no publication fact"),
                }),
                None,
            ),
            Err(error) => (
                error.conclusion(),
                StatisticsPublicationFact::NotStarted,
                Some(error.failure()),
                None,
            ),
        };
        self.repository
            .conclude(
                job.id,
                StatisticsJobPhase::Publishing,
                conclusion,
                publication_fact,
                failure,
                at_ms,
            )
            .await?;
        if let Some(failure) = finalization_failure {
            self.record_finalization_failure(job.id, failure, at_ms)
                .await?;
        }
        let terminal = self
            .repository
            .record_convergence(
                job.id,
                StatisticsConvergence {
                    collection_stopped: false,
                    execution_resources_released: false,
                    provider_session_closed: true,
                },
                at_ms,
            )
            .await?;
        Ok(Some(terminal))
    }

    fn root_scope(&self, id: StatisticsJobId) -> Result<WorkScope, StatisticsRepositoryError> {
        let state = self.repository.lock()?;
        state
            .active
            .get(&id)
            .map(|entry| entry.owner.as_ref().expect("active job owns root").scope())
            .ok_or_else(|| StatisticsJobRepository::not_found(id))
    }

    async fn conclude_error(
        &self,
        job: StatisticsJob,
        phase: StatisticsJobPhase,
        error: StatisticsAttemptError,
        at_ms: i64,
    ) -> Result<StatisticsJob, StatisticsRepositoryError> {
        self.repository
            .conclude(
                job.id,
                phase,
                error.conclusion(),
                StatisticsPublicationFact::NotStarted,
                Some(error.failure()),
                at_ms,
            )
            .await?;
        self.repository
            .record_convergence(
                job.id,
                StatisticsConvergence {
                    collection_stopped: true,
                    execution_resources_released: true,
                    provider_session_closed: true,
                },
                at_ms,
            )
            .await
    }

    async fn record_finalization_failure(
        &self,
        id: StatisticsJobId,
        failure: StatisticsFailure,
        at_ms: i64,
    ) -> Result<(), StatisticsRepositoryError> {
        let mut state = self.repository.lock()?;
        let entry = state
            .terminal
            .iter_mut()
            .find(|entry| entry.job.id == id)
            .ok_or_else(|| StatisticsJobRepository::not_found(id))?;
        if entry.job.publication != StatisticsPublicationFact::KnownCommitted {
            return Err(StatisticsRepositoryError::new(
                StatisticsRepositoryErrorKind::InvalidTransition,
                "only a known provider commit can retain a finalization failure",
            ));
        }
        entry.job.publication_finalization_failure = Some(failure);
        entry.job.updated_at_ms = at_ms;
        Ok(())
    }
}

fn work_error(error: novarocks_workload_control::WorkError) -> StatisticsRepositoryError {
    StatisticsRepositoryError::new(
        StatisticsRepositoryErrorKind::Conflict,
        format!("statistics child work scope admission failed: {error}"),
    )
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

    fn create(at_ms: i64) -> StatisticsJobCreate {
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

    fn root() -> WorkOwner {
        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 16 * 1024 * 1024,
                control_bytes: 1024 * 1024,
                per_scope_bytes: 8 * 1024 * 1024,
            },
        )
        .expect("control");
        control.mark_ready().expect("ready");
        control
            .try_begin_root(WorkRequest::new(WorkClass::Statistics))
            .expect("root")
            .owner
    }

    struct RecordingExecutor {
        publications: AtomicUsize,
        finalization_fails: bool,
    }

    struct BlockingExecutor {
        started: mpsc::Sender<()>,
        release: Mutex<mpsc::Receiver<()>>,
    }

    impl StatisticsAttemptExecutor for BlockingExecutor {
        fn prepare(
            &self,
            _job: &StatisticsJob,
            scope: &WorkScope,
        ) -> Result<(), StatisticsAttemptError> {
            self.started
                .send(())
                .expect("test observes the background preparation");
            self.release
                .lock()
                .expect("release lock")
                .recv()
                .expect("test releases the background preparation");
            scope.check().map_err(|error| {
                StatisticsAttemptError::Cancelled(StatisticsFailure {
                    message: Arc::from(error.to_string()),
                })
            })
        }

        fn collect(
            &self,
            _job: &StatisticsJob,
            _scope: &WorkScope,
        ) -> Result<(), StatisticsAttemptError> {
            Ok(())
        }

        fn publish(
            &self,
            _job: &StatisticsJob,
            _scope: &WorkScope,
        ) -> Result<StatisticsPublicationOutcome, StatisticsAttemptError> {
            Ok(StatisticsPublicationOutcome {
                fact: StatisticsPublicationFact::KnownCommitted,
                finalization_failure: None,
            })
        }
    }

    impl StatisticsAttemptExecutor for RecordingExecutor {
        fn prepare(
            &self,
            _job: &StatisticsJob,
            scope: &WorkScope,
        ) -> Result<(), StatisticsAttemptError> {
            scope.check().map_err(|error| {
                StatisticsAttemptError::Failed(StatisticsFailure {
                    message: Arc::from(error.to_string()),
                })
            })
        }

        fn collect(
            &self,
            _job: &StatisticsJob,
            scope: &WorkScope,
        ) -> Result<(), StatisticsAttemptError> {
            scope.check().map_err(|error| {
                StatisticsAttemptError::Failed(StatisticsFailure {
                    message: Arc::from(error.to_string()),
                })
            })
        }

        fn publish(
            &self,
            _job: &StatisticsJob,
            scope: &WorkScope,
        ) -> Result<StatisticsPublicationOutcome, StatisticsAttemptError> {
            scope.check().map_err(|error| {
                StatisticsAttemptError::Failed(StatisticsFailure {
                    message: Arc::from(error.to_string()),
                })
            })?;
            self.publications.fetch_add(1, Ordering::SeqCst);
            if self.finalization_fails {
                Ok(StatisticsPublicationOutcome {
                    fact: StatisticsPublicationFact::KnownCommitted,
                    finalization_failure: Some(StatisticsFailure {
                        message: Arc::from("accelerator finalization failed"),
                    }),
                })
            } else {
                Ok(StatisticsPublicationOutcome {
                    fact: StatisticsPublicationFact::CommitUnknown,
                    finalization_failure: None,
                })
            }
        }
    }

    #[tokio::test]
    async fn job_query_attempt_and_publication_have_distinct_v7_identities() {
        let repository = StatisticsJobRepository::new();
        let job = repository.create(create(1), root()).await.expect("create");
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
        let job = repository.create(create(1), root()).await.expect("create");
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
        let submitted = service.submit(create(1), root()).await.expect("submit");
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
        let (release, release_rx) = mpsc::channel();
        let runtime = StatisticsJobRuntime::start(
            service.clone(),
            Arc::new(BlockingExecutor {
                started,
                release: Mutex::new(release_rx),
            }),
            tokio::runtime::Handle::current(),
        );

        let submitted = runtime.submit(create(1), root()).await.expect("submit");
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
        let (release, release_rx) = mpsc::channel();
        let runtime = StatisticsJobRuntime::start(
            service.clone(),
            Arc::new(BlockingExecutor {
                started,
                release: Mutex::new(release_rx),
            }),
            tokio::runtime::Handle::current(),
        );

        let submitted = runtime.submit(create(1), root()).await.expect("submit");
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
        let (release, release_rx) = mpsc::channel();
        let runtime = StatisticsJobRuntime::start(
            service.clone(),
            Arc::new(BlockingExecutor {
                started,
                release: Mutex::new(release_rx),
            }),
            tokio::runtime::Handle::current(),
        );

        let submitted = runtime.submit(create(1), root()).await.expect("submit");
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
            .submit(create(1), root())
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
        let queued = service.submit(create(1), root()).await.expect("queue job");
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
        let job = repository.create(create(1), root()).await.expect("create");
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
        let job = repository.create(create(1), root()).await.expect("create");
        let terminal = worker.run_one(2).await.expect("run").expect("terminal");
        assert_eq!(
            terminal.state,
            StatisticsJobState::Terminal(StatisticsJobConclusion::Succeeded)
        );
        assert_eq!(
            terminal.publication,
            StatisticsPublicationFact::KnownCommitted
        );
        assert_eq!(
            terminal.publication_finalization_failure,
            Some(StatisticsFailure {
                message: Arc::from("accelerator finalization failed"),
            })
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

        impl StatisticsAttemptExecutor for StaleExecutor {
            fn prepare(
                &self,
                _job: &StatisticsJob,
                _scope: &WorkScope,
            ) -> Result<(), StatisticsAttemptError> {
                Err(StatisticsAttemptError::Stale(StatisticsFailure {
                    message: Arc::from("captured table object was replaced"),
                }))
            }

            fn collect(
                &self,
                _job: &StatisticsJob,
                _scope: &WorkScope,
            ) -> Result<(), StatisticsAttemptError> {
                unreachable!("stale preparation must not start collection")
            }

            fn publish(
                &self,
                _job: &StatisticsJob,
                _scope: &WorkScope,
            ) -> Result<StatisticsPublicationOutcome, StatisticsAttemptError> {
                unreachable!("stale preparation must not publish")
            }
        }

        let repository = StatisticsJobRepository::new();
        let job = repository.create(create(1), root()).await.expect("create");
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
        let job = repository.create(create(1), root()).await.expect("create");
        let claimed = repository.claim_next(2).await.expect("claim").expect("job");
        let terminal = repository
            .conclude(
                claimed.id,
                StatisticsJobPhase::Preparing,
                StatisticsJobConclusion::Failed,
                StatisticsPublicationFact::NotStarted,
                Some(StatisticsFailure {
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
}
