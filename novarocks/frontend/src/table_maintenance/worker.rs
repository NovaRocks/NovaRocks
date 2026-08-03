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

use novarocks::engine::table_maintenance::{MaintenanceActionOutcome, TableMaintenanceEngine};
use tokio::runtime::{Builder, Handle};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::sleep;

use super::FrontendTableMaintenanceService;
use super::model::{OptimizeJob, OptimizeJobOutcome};
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
    ) -> Result<MaintenanceActionOutcome, String> {
        FrontendTableMaintenanceService::execute_optimize_distributed_rewrite(
            runtime,
            Arc::clone(&self.repository),
            engine,
            job.target.clone(),
            job.job_id,
        )
    }
}

impl OptimizeWorker {
    pub fn start(
        runtime: &Handle,
        repository: Arc<OptimizeJobRepository>,
        distributed_rewrite_repository: Arc<DistributedRewriteOperationRepository>,
        engine: Weak<dyn TableMaintenanceEngine>,
    ) -> Result<Self, String> {
        Self::start_with_executor(
            runtime,
            repository,
            engine,
            Arc::new(DistributedRewriteOptimizeJobExecutor {
                repository: distributed_rewrite_repository,
            }),
        )
    }

    pub fn start_with_executor(
        runtime: &Handle,
        repository: Arc<OptimizeJobRepository>,
        engine: Weak<dyn TableMaintenanceEngine>,
        executor: Arc<dyn OptimizeJobExecutor>,
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
    stop: Arc<AtomicBool>,
    wakeup: Arc<Notify>,
) -> Result<(), String> {
    repository
        .reconcile_startup(now_unix_millis())
        .await
        .map_err(|error| format!("reconcile running optimize jobs failed: {error}"))?;

    loop {
        if stop.load(Ordering::Acquire) {
            return Ok(());
        }
        if engine.upgrade().is_none() {
            return Ok(());
        }

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
            let Some(claimed) = repository
                .claim(job.job_id, now_unix_millis())
                .await
                .map_err(|error| format!("claim optimize job {} failed: {error}", job.job_id))?
            else {
                continue;
            };
            execute_claimed_job(
                &runtime,
                repository.as_ref(),
                engine,
                Arc::clone(&executor),
                claimed,
            )
            .await?;
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

async fn execute_claimed_job(
    runtime: &Handle,
    repository: &OptimizeJobRepository,
    engine: Arc<dyn TableMaintenanceEngine>,
    executor: Arc<dyn OptimizeJobExecutor>,
    job: OptimizeJob,
) -> Result<(), String> {
    let job_id = job.job_id;
    let runtime = runtime.clone();
    let execution =
        tokio::task::spawn_blocking(move || executor.execute(&runtime, engine.as_ref(), &job))
            .await
            .map_err(|error| format!("optimize job {job_id} engine task failed: {error}"))
            .and_then(|result| result)
            .and_then(optimize_outcome);

    let outcome = match execution {
        Ok(outcome) => outcome,
        Err(message) => {
            repository
                .fail(job_id, now_unix_millis(), message)
                .await
                .map_err(|error| format!("fail optimize job {job_id} failed: {error}"))?;
            return Ok(());
        }
    };
    repository
        .record_outcome(job_id, outcome)
        .await
        .map_err(|error| format!("record outcome for optimize job {job_id} failed: {error}"))?;
    repository
        .finish(job_id, now_unix_millis())
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
