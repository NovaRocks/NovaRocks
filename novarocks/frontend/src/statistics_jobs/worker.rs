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

//! Durable ownership for the frontend ANALYZE worker.
//!
//! Job records remain in the repository. This module owns only the global
//! worker lease and the fence validator injected into repository mutations.
//! It never opens writes or starts a restore: an existing coordination plane
//! is respected exactly as it was found.

use std::any::Any;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use novarocks_spi::connector::ExternalMutationEvidence;
use novarocks_spi::state_store::StateStore;
use novarocks_state_store::OperationId;
use novarocks_state_store::coordination::{
    AcquireOutcome, AttemptId, ClockHealth, CoordinationError, CoordinationErrorKind, HolderId,
    IncarnationGate, LeaseClock, LeaseFence, LeaseManager, LeaseSettings, ResourceKey,
};
use uuid::Uuid;

use super::model::{StatisticsJob, StatisticsJobError, StatisticsJobErrorKind, StatisticsJobState};
use super::repository::FenceValidator;
use super::repository::StatisticsJobRepository;

/// One process-wide lease protects all durable ANALYZE attempts for a frontend
/// deployment. It is deliberately not keyed by table or session.
pub const STATISTICS_ANALYZE_WORKER_RESOURCE: &str = "frontend/statistics/analyze-worker/v1";
const STATISTICS_ANALYZE_WORKER_RESOURCE_BYTES: &[u8] = b"frontend/statistics/analyze-worker/v1";

pub const STATISTICS_LEASE_DURATION: Duration = Duration::from_secs(15);
pub const STATISTICS_LEASE_RENEW_INTERVAL: Duration = Duration::from_secs(5);
pub const STATISTICS_MAX_CLOCK_SKEW: Duration = Duration::from_secs(1);
pub const STATISTICS_TAKEOVER_OBSERVATION: Duration = Duration::from_secs(2);
const STATISTICS_WORKER_POLL_INTERVAL: Duration = Duration::from_millis(500);
const MAX_STATISTICS_ATTEMPTS: u32 = 3;
const RETRY_BACKOFF_BASE: Duration = Duration::from_secs(1);
const RETRY_BACKOFF_MAX: Duration = Duration::from_secs(30);

/// An execution error whose retry semantics are explicit. A publish outcome
/// that may have reached the connector is never retried by this worker: its
/// operation ID must be reconciled from the `PUBLISHING` record instead.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsAttemptError {
    pub kind: StatisticsJobErrorKind,
    pub message: String,
    pub transient: bool,
    pub requires_reconcile: bool,
}

impl StatisticsAttemptError {
    pub fn transient(kind: StatisticsJobErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            transient: true,
            requires_reconcile: false,
        }
    }

    pub fn permanent(kind: StatisticsJobErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            transient: false,
            requires_reconcile: false,
        }
    }

    pub fn reconcile(kind: StatisticsJobErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            transient: false,
            requires_reconcile: true,
        }
    }
}

/// Attempt-local collection material. It never crosses a StateStore boundary:
/// only the separately prepared reconciliation evidence is durable.
pub trait StatisticsCollectedAttempt: Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

impl StatisticsCollectedAttempt for () {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Connector-neutral execution owned by the frontend worker. Implementations
/// receive the durable operation ID from `job` and must use it for all
/// external collection/publish reconciliation.
pub trait StatisticsAttemptExecutor: Send + Sync {
    fn collect(
        &self,
        job: &StatisticsJob,
    ) -> Result<Box<dyn StatisticsCollectedAttempt>, StatisticsAttemptError>;

    /// Must be side-effect free. Its result is persisted atomically before
    /// `publish` is ever called.
    fn prepare_publish(
        &self,
        job: &StatisticsJob,
        collected: &dyn StatisticsCollectedAttempt,
    ) -> Result<ExternalMutationEvidence, StatisticsAttemptError>;

    fn publish(
        &self,
        job: &StatisticsJob,
        collected: &dyn StatisticsCollectedAttempt,
        evidence: &ExternalMutationEvidence,
    ) -> Result<(), StatisticsAttemptError>;

    fn reconcile(
        &self,
        job: &StatisticsJob,
        evidence: &ExternalMutationEvidence,
    ) -> Result<(), StatisticsAttemptError>;
}

/// Lifecycle owner for the durable statistics worker task.
pub struct StatisticsAnalyzeWorker {
    stop: Arc<AtomicBool>,
    wakeup: Arc<tokio::sync::Notify>,
    join: Option<tokio::task::JoinHandle<Result<(), String>>>,
}

impl StatisticsAnalyzeWorker {
    pub async fn start(
        runtime: &tokio::runtime::Handle,
        repository: Arc<StatisticsJobRepository>,
        executor: Arc<dyn StatisticsAttemptExecutor>,
    ) -> Result<Self, String> {
        let coordination = StatisticsAnalyzeWorkerCoordination::open(repository.store())
            .await
            .map_err(|error| format!("open statistics worker coordination failed: {error}"))?;
        let stop = Arc::new(AtomicBool::new(false));
        let wakeup = Arc::new(tokio::sync::Notify::new());
        let join = runtime.spawn(run_worker(
            repository,
            Arc::downgrade(&executor),
            coordination,
            Arc::clone(&stop),
            Arc::clone(&wakeup),
        ));
        Ok(Self {
            stop,
            wakeup,
            join: Some(join),
        })
    }

    pub fn wakeup(&self) {
        self.wakeup.notify_one();
    }

    pub fn shutdown(&mut self) -> Result<(), String> {
        self.stop.store(true, Ordering::Release);
        self.wakeup();
        let Some(join) = self.join.take() else {
            return Ok(());
        };
        let joined = if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            tokio::task::block_in_place(|| runtime.block_on(join))
        } else {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| format!("build statistics worker join runtime failed: {error}"))?
                .block_on(join)
        };
        joined.map_err(|error| format!("statistics worker join failed: {error}"))?
    }
}

async fn run_worker(
    repository: Arc<StatisticsJobRepository>,
    executor: Weak<dyn StatisticsAttemptExecutor>,
    coordination: StatisticsAnalyzeWorkerCoordination,
    stop: Arc<AtomicBool>,
    wakeup: Arc<tokio::sync::Notify>,
) -> Result<(), String> {
    loop {
        if stop.load(Ordering::Acquire) || executor.upgrade().is_none() {
            return Ok(());
        }
        match coordination
            .acquire()
            .await
            .map_err(|error| format!("acquire statistics worker lease failed: {error}"))?
        {
            AcquireOutcome::Acquired(mut guard) => {
                let fence = StatisticsAnalyzeWorkerCoordination::fence_validator(guard.fence());
                process_cancellation_requests(&repository, now_unix_millis(), &fence).await?;
                recover_incomplete(&repository, now_unix_millis(), &fence).await?;
                let result = reconcile_publishing(
                    repository.as_ref(),
                    &executor,
                    &mut guard,
                    &fence,
                    stop.as_ref(),
                )
                .await;
                let result = match result {
                    Ok(()) => {
                        process_submitted(
                            repository.as_ref(),
                            &executor,
                            &mut guard,
                            &fence,
                            stop.as_ref(),
                        )
                        .await
                    }
                    Err(error) => Err(error),
                };
                let release = guard.release(OperationId::new_v7()).await;
                result?;
                release
                    .map_err(|error| format!("release statistics worker lease failed: {error}"))?;
            }
            AcquireOutcome::Contended(_) | AcquireOutcome::AwaitingTakeover(_) => {}
        }
        tokio::select! {
            _ = wakeup.notified() => {}
            _ = tokio::time::sleep(STATISTICS_WORKER_POLL_INTERVAL) => {}
        }
    }
}

async fn recover_incomplete(
    repository: &StatisticsJobRepository,
    now_ms: i64,
    fence: &FenceValidator,
) -> Result<(), String> {
    for state in [StatisticsJobState::Preparing, StatisticsJobState::Running] {
        for job in repository
            .list_by_state(state)
            .await
            .map_err(|error| format!("list recoverable statistics jobs failed: {error}"))?
        {
            repository
                .requeue_incomplete(job.job_id, now_ms, fence)
                .await
                .map_err(|error| {
                    format!(
                        "requeue incomplete statistics job {} failed: {error}",
                        job.job_id
                    )
                })?;
        }
    }
    Ok(())
}

async fn process_cancellation_requests(
    repository: &StatisticsJobRepository,
    now_ms: i64,
    fence: &FenceValidator,
) -> Result<(), String> {
    for state in [
        StatisticsJobState::Submitted,
        StatisticsJobState::Preparing,
        StatisticsJobState::Running,
    ] {
        for job in repository
            .list_by_state(state)
            .await
            .map_err(|error| format!("list cancellable statistics jobs failed: {error}"))?
        {
            if !job.cancel_requested {
                continue;
            }
            repository
                .transition(
                    job.job_id,
                    state,
                    StatisticsJobState::Cancelled,
                    now_ms,
                    None,
                    fence,
                )
                .await
                .map_err(|error| {
                    format!(
                        "cancel statistics job {} under worker fence failed: {error}",
                        job.job_id
                    )
                })?;
        }
    }
    Ok(())
}

async fn reconcile_publishing(
    repository: &StatisticsJobRepository,
    executor: &Weak<dyn StatisticsAttemptExecutor>,
    guard: &mut novarocks_state_store::coordination::LeaseGuard,
    fence: &FenceValidator,
    stop: &AtomicBool,
) -> Result<(), String> {
    let mut publishing = repository
        .list_by_state(StatisticsJobState::Publishing)
        .await
        .map_err(|error| format!("list publishing statistics jobs failed: {error}"))?;
    publishing.sort_by_key(|job| job.job_id);
    for job in publishing {
        if stop.load(Ordering::Acquire) {
            return Ok(());
        }
        let Some(executor) = executor.upgrade() else {
            return Ok(());
        };
        let evidence = job.publication_evidence.as_deref().ok_or_else(|| {
            format!(
                "publishing statistics job {} is missing operation evidence",
                job.job_id
            )
        })?;
        let evidence = ExternalMutationEvidence::try_from_wire_v1(evidence)
            .map_err(|error| format!("decode statistics publication evidence: {error}"))?;
        let outcome = run_with_lease_renewal(guard, {
            let executor = Arc::clone(&executor);
            let job = job.clone();
            move || executor.reconcile(&job, &evidence)
        })
        .await;
        match outcome {
            Ok(()) => {
                repository
                    .transition(
                        job.job_id,
                        StatisticsJobState::Publishing,
                        StatisticsJobState::Succeeded,
                        now_unix_millis(),
                        None,
                        fence,
                    )
                    .await
                    .map_err(|error| {
                        format!(
                            "finish reconciled statistics job {} failed: {error}",
                            job.job_id
                        )
                    })?;
            }
            Err(error) if error.requires_reconcile => {
                // The receipt remains uncertain; preserve PUBLISHING and let
                // a future fenced owner retry reconciliation with exactly the
                // same operation ID.
            }
            Err(error) => {
                repository
                    .transition(
                        job.job_id,
                        StatisticsJobState::Publishing,
                        StatisticsJobState::Failed,
                        now_unix_millis(),
                        Some(job_error(&error)),
                        fence,
                    )
                    .await
                    .map_err(|repository_error| {
                        format!(
                            "fail reconciled statistics job {} failed: {repository_error}",
                            job.job_id
                        )
                    })?;
            }
        };
    }
    Ok(())
}

async fn process_submitted(
    repository: &StatisticsJobRepository,
    executor: &Weak<dyn StatisticsAttemptExecutor>,
    guard: &mut novarocks_state_store::coordination::LeaseGuard,
    fence: &FenceValidator,
    stop: &AtomicBool,
) -> Result<(), String> {
    let mut submitted = repository
        .list_by_state(StatisticsJobState::Submitted)
        .await
        .map_err(|error| format!("list submitted statistics jobs failed: {error}"))?;
    submitted.sort_by_key(|job| job.job_id);
    for job in submitted {
        if stop.load(Ordering::Acquire) {
            return Ok(());
        }
        if job
            .retry_not_before_ms
            .is_some_and(|deadline| deadline > now_unix_millis())
        {
            continue;
        }
        let Some(executor) = executor.upgrade() else {
            return Ok(());
        };
        let Some(preparing) = repository
            .claim(job.job_id, now_unix_millis(), fence)
            .await
            .map_err(|error| format!("claim statistics job {} failed: {error}", job.job_id))?
        else {
            continue;
        };
        let running = repository
            .transition(
                preparing.job_id,
                StatisticsJobState::Preparing,
                StatisticsJobState::Running,
                now_unix_millis(),
                None,
                fence,
            )
            .await
            .map_err(|error| {
                format!("start statistics job {} failed: {error}", preparing.job_id)
            })?;
        let collect = run_with_lease_renewal(guard, {
            let executor = Arc::clone(&executor);
            let running = running.clone();
            move || executor.collect(&running)
        })
        .await;
        let collected: Arc<dyn StatisticsCollectedAttempt> = match collect {
            Ok(collected) => Arc::from(collected),
            Err(error) => {
                resolve_collection_error(repository, &running, error, fence).await?;
                continue;
            }
        };
        // An explicit cancellation can still win before PUBLISHING. Once the
        // state crosses that boundary the repository returns a typed conflict.
        let Some(current) = repository
            .get(running.job_id)
            .await
            .map_err(|error| format!("read statistics job {} failed: {error}", running.job_id))?
        else {
            return Err(format!("statistics job {} disappeared", running.job_id));
        };
        if current.state == StatisticsJobState::Cancelled {
            continue;
        }
        if current.cancel_requested {
            repository
                .transition(
                    running.job_id,
                    StatisticsJobState::Running,
                    StatisticsJobState::Cancelled,
                    now_unix_millis(),
                    None,
                    fence,
                )
                .await
                .map_err(|error| {
                    format!(
                        "cancel running statistics job {} failed: {error}",
                        running.job_id
                    )
                })?;
            continue;
        }
        let evidence = run_with_lease_renewal(guard, {
            let executor = Arc::clone(&executor);
            let running = running.clone();
            let collected = Arc::clone(&collected);
            move || executor.prepare_publish(&running, collected.as_ref())
        })
        .await;
        let evidence = match evidence {
            Ok(evidence) => evidence,
            Err(error) => {
                resolve_collection_error(repository, &running, error, fence).await?;
                continue;
            }
        };
        let evidence_wire = evidence
            .try_to_wire_v1()
            .map_err(|error| format!("encode statistics publication evidence: {error}"))?;
        let publishing = repository
            .begin_publishing(running.job_id, now_unix_millis(), evidence_wire, fence)
            .await
            .map_err(|error| {
                format!(
                    "begin publish for statistics job {} failed: {error}",
                    running.job_id
                )
            })?;
        let publish = run_with_lease_renewal(guard, {
            let executor = Arc::clone(&executor);
            let publishing = publishing.clone();
            let collected = Arc::clone(&collected);
            let evidence = evidence.clone();
            move || executor.publish(&publishing, collected.as_ref(), &evidence)
        })
        .await;
        match publish {
            Ok(()) => {
                repository
                    .transition(
                        publishing.job_id,
                        StatisticsJobState::Publishing,
                        StatisticsJobState::Succeeded,
                        now_unix_millis(),
                        None,
                        fence,
                    )
                    .await
                    .map_err(|error| {
                        format!(
                            "finish statistics job {} failed: {error}",
                            publishing.job_id
                        )
                    })?;
            }
            Err(error) if error.requires_reconcile => {
                // Keep PUBLISHING durable. The next fenced worker reconciles
                // with the exact evidence written before external publication.
            }
            Err(error) => {
                repository
                    .transition(
                        publishing.job_id,
                        StatisticsJobState::Publishing,
                        StatisticsJobState::Failed,
                        now_unix_millis(),
                        Some(job_error(&error)),
                        fence,
                    )
                    .await
                    .map_err(|repository_error| {
                        format!(
                            "fail publish for statistics job {} failed: {repository_error}",
                            publishing.job_id
                        )
                    })?;
            }
        }
    }
    Ok(())
}

async fn resolve_collection_error(
    repository: &StatisticsJobRepository,
    job: &StatisticsJob,
    error: StatisticsAttemptError,
    fence: &FenceValidator,
) -> Result<(), String> {
    if error.transient && job.attempt < MAX_STATISTICS_ATTEMPTS {
        let now_ms = now_unix_millis();
        repository
            .retry_running(
                job.job_id,
                now_ms,
                now_ms.saturating_add(retry_backoff(job.attempt).as_millis() as i64),
                fence,
            )
            .await
            .map_err(|repository_error| {
                format!(
                    "schedule retry for statistics job {} failed: {repository_error}",
                    job.job_id
                )
            })?;
    } else {
        repository
            .transition(
                job.job_id,
                StatisticsJobState::Running,
                StatisticsJobState::Failed,
                now_unix_millis(),
                Some(job_error(&error)),
                fence,
            )
            .await
            .map_err(|repository_error| {
                format!(
                    "record collection failure for statistics job {} failed: {repository_error}",
                    job.job_id
                )
            })?;
    }
    Ok(())
}

fn retry_backoff(attempt: u32) -> Duration {
    let exponent = attempt.saturating_sub(1).min(5);
    RETRY_BACKOFF_BASE
        .checked_mul(1_u32 << exponent)
        .unwrap_or(RETRY_BACKOFF_MAX)
        .min(RETRY_BACKOFF_MAX)
}

async fn run_with_lease_renewal<T, F>(
    guard: &mut novarocks_state_store::coordination::LeaseGuard,
    work: F,
) -> Result<T, StatisticsAttemptError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, StatisticsAttemptError> + Send + 'static,
{
    let mut task = tokio::task::spawn_blocking(work);
    loop {
        tokio::select! {
            result = &mut task => {
                return result.map_err(|error| StatisticsAttemptError::permanent(
                    StatisticsJobErrorKind::Internal,
                    format!("statistics attempt task failed: {error}"),
                ))?;
            }
            _ = tokio::time::sleep(guard.renew_after()) => {
                guard.renew(OperationId::new_v7()).await.map_err(|error| {
                    StatisticsAttemptError::reconcile(
                        StatisticsJobErrorKind::Internal,
                        format!("statistics worker lease renewal failed: {error}"),
                    )
                })?;
            }
        }
    }
}

fn job_error(error: &StatisticsAttemptError) -> StatisticsJobError {
    StatisticsJobError {
        kind: error.kind,
        message: error.message.clone(),
    }
}

fn now_unix_millis() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(i64::MAX)
}

/// Production wall/monotonic clock for the statistics worker. Tests inject a
/// deterministic `LeaseClock` directly through `open_with_clock`.
#[derive(Debug)]
pub struct SystemStatisticsLeaseClock {
    monotonic_origin: Instant,
}

impl Default for SystemStatisticsLeaseClock {
    fn default() -> Self {
        Self {
            monotonic_origin: Instant::now(),
        }
    }
}

impl LeaseClock for SystemStatisticsLeaseClock {
    fn wall_time_millis(&self) -> Result<u64, CoordinationError> {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| CoordinationError::clock_unsafe())
            .and_then(|duration| {
                u64::try_from(duration.as_millis()).map_err(|_| CoordinationError::clock_unsafe())
            })
    }

    fn monotonic_time_millis(&self) -> u64 {
        u64::try_from(self.monotonic_origin.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    fn health(&self) -> ClockHealth {
        ClockHealth::Healthy
    }
}

/// Coordination facade used by the durable worker. Opening it may bootstrap a
/// missing coordination record, but never mutates an already bootstrapped
/// incarnation's restore/write mode.
#[derive(Clone)]
pub struct StatisticsAnalyzeWorkerCoordination {
    manager: LeaseManager,
    resource: ResourceKey,
}

impl StatisticsAnalyzeWorkerCoordination {
    pub async fn open(store: Arc<dyn StateStore>) -> Result<Self, CoordinationError> {
        Self::open_with_clock(store, Arc::new(SystemStatisticsLeaseClock::default())).await
    }

    pub async fn open_with_clock(
        store: Arc<dyn StateStore>,
        clock: Arc<dyn LeaseClock>,
    ) -> Result<Self, CoordinationError> {
        let gate = IncarnationGate::new(Arc::clone(&store));
        match gate.load().await {
            Ok(_) => {}
            Err(error) if error.kind() == CoordinationErrorKind::NotBootstrapped => {
                gate.bootstrap(OperationId::new_v7()).await?;
            }
            Err(error) => return Err(error),
        }
        let holder = HolderId::try_from(Bytes::from(Uuid::now_v7().to_string()))?;
        let settings = LeaseSettings::new(
            STATISTICS_LEASE_DURATION,
            STATISTICS_LEASE_RENEW_INTERVAL,
            STATISTICS_MAX_CLOCK_SKEW,
            STATISTICS_TAKEOVER_OBSERVATION,
        )?;
        Ok(Self {
            manager: LeaseManager::new(store, holder, clock, settings)?,
            resource: ResourceKey::try_from(Bytes::from_static(
                STATISTICS_ANALYZE_WORKER_RESOURCE_BYTES,
            ))?,
        })
    }

    pub async fn acquire(&self) -> Result<AcquireOutcome, CoordinationError> {
        self.manager
            .acquire(
                self.resource.clone(),
                AttemptId::try_from(Uuid::now_v7())?,
                OperationId::new_v7(),
            )
            .await
    }

    /// Creates the validator used by repository mutation transactions. The
    /// fence is checked in the same transaction as the job-record CAS and
    /// state-index transition, so a lost worker can never publish a stale job
    /// state after takeover.
    pub fn fence_validator(fence: LeaseFence) -> FenceValidator {
        Arc::new(move |transaction| {
            let fence = fence.clone();
            Box::pin(async move {
                fence
                    .validate_in(transaction)
                    .await
                    .map_err(|error| error.to_string())
            })
        })
    }
}
