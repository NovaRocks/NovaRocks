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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use crate::query_execution::maintenance::{MaintenanceActionOutcome, TableMaintenanceEngine};
use tokio::runtime::{Builder, Handle};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::sleep;

use super::FrontendTableMaintenanceService;
use super::coordination::{
    MaintenanceAcquireOutcome, MaintenanceCoordination, MaintenanceFenceValidator,
    MaintenanceLeaseAttempt,
};
use super::model::{MaintenanceAuthorityV1, OptimizeJob, OptimizeJobOutcome};
use super::now_unix_millis;
use super::repository::{DistributedRewriteOperationRepository, OptimizeJobRepository};

const OPTIMIZE_POLL_INTERVAL: Duration = Duration::from_millis(500);

pub struct OptimizeWorker {
    stop: Arc<AtomicBool>,
    wakeup: Arc<Notify>,
    join: Option<JoinHandle<Result<(), String>>>,
}

/// Executes a claimed OPTIMIZE job after the worker has established durable
/// ownership. Keeping this port separate lets scheduler tests exercise claim,
/// ordering, and shutdown behavior without fabricating a connector write
/// session; production uses the distributed-rewrite implementation below.
pub trait OptimizeJobExecutor: Send + Sync {
    fn execute(
        &self,
        runtime: &Handle,
        engine: &dyn TableMaintenanceEngine,
        job: &OptimizeJob,
        attempt: &MaintenanceLeaseAttempt,
    ) -> Result<MaintenanceActionOutcome, String>;
}

struct DistributedRewriteOptimizeJobExecutor {
    repository: Arc<DistributedRewriteOperationRepository>,
}

impl OptimizeJobExecutor for DistributedRewriteOptimizeJobExecutor {
    fn execute(
        &self,
        runtime: &Handle,
        engine: &dyn TableMaintenanceEngine,
        job: &OptimizeJob,
        attempt: &MaintenanceLeaseAttempt,
    ) -> Result<MaintenanceActionOutcome, String> {
        FrontendTableMaintenanceService::execute_optimize_distributed_rewrite(
            runtime,
            Arc::clone(&self.repository),
            engine,
            job.target.clone(),
            job.job_id,
            attempt.clone(),
        )
    }
}

impl OptimizeWorker {
    pub fn start(
        runtime: &Handle,
        repository: Arc<OptimizeJobRepository>,
        distributed_rewrite_repository: Arc<DistributedRewriteOperationRepository>,
        engine: Weak<dyn TableMaintenanceEngine>,
        coordination: MaintenanceCoordination,
    ) -> Result<Self, String> {
        Self::start_with_executor(
            runtime,
            repository,
            engine,
            Arc::new(DistributedRewriteOptimizeJobExecutor {
                repository: distributed_rewrite_repository,
            }),
            coordination,
        )
    }

    pub fn start_with_executor(
        runtime: &Handle,
        repository: Arc<OptimizeJobRepository>,
        engine: Weak<dyn TableMaintenanceEngine>,
        executor: Arc<dyn OptimizeJobExecutor>,
        coordination: MaintenanceCoordination,
    ) -> Result<Self, String> {
        let stop = Arc::new(AtomicBool::new(false));
        let wakeup = Arc::new(Notify::new());
        let worker_stop = Arc::clone(&stop);
        let worker_wakeup = Arc::clone(&wakeup);
        let worker_runtime = runtime.clone();
        let join = runtime.spawn(async move {
            run_worker(
                worker_runtime,
                repository,
                engine,
                executor,
                coordination,
                worker_stop,
                worker_wakeup,
            )
            .await
        });
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
        let joined = if let Ok(runtime) = Handle::try_current() {
            tokio::task::block_in_place(|| runtime.block_on(join))
        } else {
            Builder::new_current_thread()
                .build()
                .map_err(|error| {
                    format!("build table maintenance worker join runtime failed: {error}")
                })?
                .block_on(join)
        };
        joined.map_err(|error| format!("table maintenance worker join failed: {error}"))?
    }
}

async fn run_worker(
    runtime: Handle,
    repository: Arc<OptimizeJobRepository>,
    engine: Weak<dyn TableMaintenanceEngine>,
    executor: Arc<dyn OptimizeJobExecutor>,
    coordination: MaintenanceCoordination,
    stop: Arc<AtomicBool>,
    wakeup: Arc<Notify>,
) -> Result<(), String> {
    loop {
        if stop.load(Ordering::Acquire) {
            return Ok(());
        }
        if engine.upgrade().is_none() {
            return Ok(());
        }

        // Recovery runs on every poll, not once at startup: a target whose
        // previous holder is still inside its takeover observation window is
        // skipped now and converged by a later round.
        recover_claimed_jobs(repository.as_ref(), &coordination).await?;

        let mut pending = repository
            .list_pending()
            .await
            .map_err(|error| format!("list pending optimize jobs failed: {error}"))?;
        pending.sort_by_key(|job| job.job_id);
        for job in pending {
            if stop.load(Ordering::Acquire) {
                return Ok(());
            }
            let Some(engine) = engine.upgrade() else {
                return Ok(());
            };
            // Per-table authority first. A contended target belongs to another
            // frontend attempt right now, so this worker must not touch its
            // durable record at all.
            let attempt = match coordination.acquire(&job.target).await {
                Ok(MaintenanceAcquireOutcome::Acquired(attempt)) => attempt,
                Ok(
                    MaintenanceAcquireOutcome::Contended(_)
                    | MaintenanceAcquireOutcome::AwaitingTakeover(_),
                ) => continue,
                Err(error) => {
                    return Err(format!(
                        "acquire optimize authority for job {} failed: {error}",
                        job.job_id
                    ));
                }
            };
            let (authority, validator) = match attempt.durable_authority().await {
                Ok(authority) => (authority, attempt.fence_validator()),
                Err(error) => {
                    return Err(format!(
                        "read optimize authority for job {} failed: {error}",
                        job.job_id
                    ));
                }
            };
            let Some(claimed) = repository
                .claim_fenced(
                    job.job_id,
                    now_unix_millis(),
                    authority.clone(),
                    Arc::clone(&validator),
                )
                .await
                .map_err(|error| format!("claim optimize job {} failed: {error}", job.job_id))?
            else {
                continue;
            };
            let executed = execute_claimed_job(
                &runtime,
                repository.as_ref(),
                engine,
                Arc::clone(&executor),
                claimed,
                attempt.clone(),
                authority,
                validator,
            )
            .await;
            release_attempt(&attempt).await;
            executed?;
        }
        if engine.upgrade().is_none() {
            return Ok(());
        }

        tokio::select! {
            _ = wakeup.notified() => {}
            _ = sleep(OPTIMIZE_POLL_INTERVAL) => {}
        }
    }
}

// Design: ADR-0065 (docs/adr/ADR-0065-per-table-maintenance-lease-attempt-authority.md)
/// Converge jobs a previous attempt left RUNNING.
///
/// This replaces the single-frontend restart policy that failed every running
/// job outright. Each job is decided under a freshly acquired attempt, and only
/// on evidence the durable record itself carries:
///
/// * a recorded outcome means the external work is known to have finished, so
///   the job is finalized;
/// * a dispatched child means an external rewrite may have run, so the job
///   fails closed and points at the child that owns the real reconciliation;
/// * neither means nothing was dispatched, so the job returns to PENDING and
///   any frontend may execute it under a new attempt.
///
/// A contended target is skipped: its current holder is still working on it.
async fn recover_claimed_jobs(
    repository: &OptimizeJobRepository,
    coordination: &MaintenanceCoordination,
) -> Result<(), String> {
    let running = repository
        .list_running()
        .await
        .map_err(|error| format!("list running optimize jobs failed: {error}"))?;
    for job in running {
        let attempt = match coordination.acquire(&job.target).await {
            Ok(MaintenanceAcquireOutcome::Acquired(attempt)) => attempt,
            Ok(
                MaintenanceAcquireOutcome::Contended(_)
                | MaintenanceAcquireOutcome::AwaitingTakeover(_),
            ) => continue,
            Err(error) => {
                return Err(format!(
                    "acquire optimize recovery authority for job {} failed: {error}",
                    job.job_id
                ));
            }
        };
        let authority = attempt.durable_authority().await.map_err(|error| {
            format!(
                "read optimize recovery authority for job {} failed: {error}",
                job.job_id
            )
        })?;
        let validator = attempt.fence_validator();
        let job_id = job.job_id;
        if job.outcome.is_some() {
            repository
                .finish_recovered_fenced(job_id, now_unix_millis(), authority, validator)
                .await
                .map_err(|error| {
                    format!("finish recovered optimize job {job_id} failed: {error}")
                })?;
            release_attempt(&attempt).await;
            continue;
        }
        match job.dispatched_child {
            Some(child) => {
                repository
                    .fail_recovered_fenced(
                        job_id,
                        now_unix_millis(),
                        format!(
                            "optimize job dispatched distributed rewrite {child}; its outcome \
                             requires the original exact connector generation"
                        ),
                        authority,
                        validator,
                    )
                    .await
                    .map_err(|error| {
                        format!("fail recovered optimize job {job_id} failed: {error}")
                    })?;
            }
            None => {
                repository
                    .release_undispatched_fenced(job_id, authority, validator)
                    .await
                    .map_err(|error| {
                        format!("release undispatched optimize job {job_id} failed: {error}")
                    })?;
            }
        }
        // The recovery decision is durable. Hand the target back so this same
        // round, or any other frontend, can execute a released job instead of
        // waiting out this attempt's lease.
        release_attempt(&attempt).await;
    }
    Ok(())
}

/// Best-effort lease release. A failed release is not a business failure: the
/// lease expires on its own and CP-1 takeover rules still arbitrate the next
/// acquire.
async fn release_attempt(attempt: &MaintenanceLeaseAttempt) {
    if let Err(error) = attempt.release().await {
        tracing::debug!(%error, "release table maintenance attempt failed");
    }
}

#[allow(clippy::too_many_arguments)]
async fn execute_claimed_job(
    runtime: &Handle,
    repository: &OptimizeJobRepository,
    engine: Arc<dyn TableMaintenanceEngine>,
    executor: Arc<dyn OptimizeJobExecutor>,
    job: OptimizeJob,
    attempt: MaintenanceLeaseAttempt,
    authority: MaintenanceAuthorityV1,
    validator: MaintenanceFenceValidator,
) -> Result<(), String> {
    let job_id = job.job_id;
    let runtime = runtime.clone();
    let job_attempt = attempt.clone();
    let execution = tokio::task::spawn_blocking(move || {
        executor.execute(&runtime, engine.as_ref(), &job, &job_attempt)
    })
    .await
    .map_err(|error| format!("optimize job {job_id} engine task failed: {error}"))
    .and_then(|result| result)
    .and_then(optimize_outcome);

    let outcome = match execution {
        Ok(outcome) => outcome,
        Err(message) => {
            repository
                .fail_fenced(
                    job_id,
                    now_unix_millis(),
                    message,
                    authority,
                    Arc::clone(&validator),
                )
                .await
                .map_err(|error| format!("fail optimize job {job_id} failed: {error}"))?;
            return Ok(());
        }
    };
    repository
        .record_outcome_fenced(job_id, outcome, authority.clone(), Arc::clone(&validator))
        .await
        .map_err(|error| format!("record outcome for optimize job {job_id} failed: {error}"))?;
    repository
        .finish_fenced(job_id, now_unix_millis(), authority, validator)
        .await
        .map_err(|error| format!("finish optimize job {job_id} failed: {error}"))
}

pub(crate) fn optimize_outcome(
    outcome: MaintenanceActionOutcome,
) -> Result<OptimizeJobOutcome, String> {
    let MaintenanceActionOutcome::RewriteDataFiles {
        target_snapshot_id,
        rewritten_data_files_count,
        added_data_files_count,
        removed_delete_files_count,
        output_record_count,
        ..
    } = outcome
    else {
        return Err("optimize worker expected a RewriteDataFiles outcome".to_string());
    };
    Ok(OptimizeJobOutcome {
        target_snapshot_id,
        rewritten_data_files: i64::from(rewritten_data_files_count),
        deleted_data_files: i64::from(removed_delete_files_count),
        added_data_files: i64::from(added_data_files_count),
        output_record_count,
    })
}
