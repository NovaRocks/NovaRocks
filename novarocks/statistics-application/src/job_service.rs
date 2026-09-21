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

//! Statistics job business operations.

use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::{mpsc, oneshot, watch};

use novarocks_workload_control::{QueryConcurrencyPermit, WorkOwner};

use crate::{
    StatisticsAttemptExecutor, StatisticsJob, StatisticsJobCreate, StatisticsJobId,
    StatisticsJobRepository, StatisticsRepositoryError, StatisticsWorker,
};

/// The process-local owner of statistics job submission and lifecycle lookup.
///
/// SQL, connector target capture, and Native attempt execution are supplied by
/// consumer applications. This service owns the business transition that makes
/// a captured target and a governed root responsibility into a statistics job.
#[derive(Clone)]
pub struct StatisticsJobService {
    repository: StatisticsJobRepository,
}

/// The process-local, event-driven runner for submitted statistics jobs.
///
/// SQL only submits work and never synchronously executes an attempt. The
/// product owns the runner, its wakeup queue, and its stop/join evidence;
/// role composition supplies only the concrete attempt adapter and runtime.
pub struct StatisticsJobRuntime {
    service: StatisticsJobService,
    wake: mpsc::Sender<()>,
    stop: watch::Sender<bool>,
    admission: Mutex<()>,
    completion: Mutex<Option<oneshot::Receiver<()>>>,
    join: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl StatisticsJobService {
    pub fn new() -> Self {
        Self {
            repository: StatisticsJobRepository::new(),
        }
    }

    pub async fn submit(
        &self,
        request: StatisticsJobCreate,
        owner: WorkOwner,
    ) -> Result<StatisticsJob, StatisticsRepositoryError> {
        self.repository.create(request, owner).await
    }

    pub async fn submit_admitted(
        &self,
        request: StatisticsJobCreate,
        owner: WorkOwner,
        query_concurrency: QueryConcurrencyPermit,
    ) -> Result<StatisticsJob, StatisticsRepositoryError> {
        self.repository
            .create_admitted(request, owner, query_concurrency)
            .await
    }

    fn submit_now(
        &self,
        request: StatisticsJobCreate,
        owner: WorkOwner,
    ) -> Result<StatisticsJob, StatisticsRepositoryError> {
        self.repository.create_now(request, owner)
    }

    fn submit_admitted_now(
        &self,
        request: StatisticsJobCreate,
        owner: WorkOwner,
        query_concurrency: QueryConcurrencyPermit,
    ) -> Result<StatisticsJob, StatisticsRepositoryError> {
        self.repository
            .create_now_with_permit(request, owner, Some(query_concurrency))
    }

    pub async fn list(&self) -> Result<Vec<StatisticsJob>, StatisticsRepositoryError> {
        self.repository.list().await
    }

    pub async fn request_cancel(
        &self,
        job_id: StatisticsJobId,
        at_ms: i64,
    ) -> Result<StatisticsJob, StatisticsRepositoryError> {
        self.repository.request_cancel(job_id, at_ms).await
    }

    fn request_cancel_now(
        &self,
        job_id: StatisticsJobId,
        at_ms: i64,
    ) -> Result<StatisticsJob, StatisticsRepositoryError> {
        self.repository.request_cancel_now(job_id, at_ms)
    }

    fn request_stop_for_process_exit(&self, at_ms: i64) -> Result<(), StatisticsRepositoryError> {
        self.repository.request_stop_for_process_exit(at_ms)
    }

    /// Executes one claimed job with the role-supplied Native attempt adapter.
    /// The role owns that adapter; the product service retains the job state
    /// and worker orchestration.
    pub async fn run_one(
        &self,
        executor: Arc<dyn StatisticsAttemptExecutor>,
        at_ms: i64,
    ) -> Result<Option<StatisticsJob>, StatisticsRepositoryError> {
        StatisticsWorker::new(self.repository.clone(), executor)
            .run_one(at_ms)
            .await
    }
}

impl StatisticsJobRuntime {
    pub fn start(
        service: StatisticsJobService,
        executor: Arc<dyn StatisticsAttemptExecutor>,
        runtime: tokio::runtime::Handle,
    ) -> Self {
        let (wake, mut wake_rx) = mpsc::channel(1);
        let (stop, mut stop_rx) = watch::channel(false);
        let (completed, completion) = oneshot::channel();
        let worker_service = service.clone();
        let worker_runtime = runtime.clone();
        let join = runtime.spawn(async move {
            loop {
                tokio::select! {
                    changed = stop_rx.changed() => {
                        if changed.is_err() || *stop_rx.borrow() {
                            break;
                        }
                    }
                    wake = wake_rx.recv() => {
                        if wake.is_none() {
                            break;
                        }
                        loop {
                            if *stop_rx.borrow() {
                                break;
                            }
                            let service = worker_service.clone();
                            let executor = Arc::clone(&executor);
                            let runtime = worker_runtime.clone();
                            let result = tokio::task::spawn_blocking(move || {
                                runtime.block_on(service.run_one(executor, now_ms()))
                            })
                            .await;
                            match result {
                                Ok(Ok(Some(_))) => continue,
                                Ok(Ok(None)) | Ok(Err(_)) | Err(_) => break,
                            }
                        }
                    }
                }
            }
            let _ = completed.send(());
        });
        Self {
            service,
            wake,
            stop,
            admission: Mutex::new(()),
            completion: Mutex::new(Some(completion)),
            join: Mutex::new(Some(join)),
        }
    }

    pub async fn submit(
        &self,
        request: StatisticsJobCreate,
        owner: WorkOwner,
    ) -> Result<StatisticsJob, StatisticsRepositoryError> {
        let _admission = self.admission.lock().map_err(|_| {
            StatisticsRepositoryError::new(
                crate::StatisticsRepositoryErrorKind::Conflict,
                "statistics worker admission lock poisoned",
            )
        })?;
        if *self.stop.borrow() {
            return Err(StatisticsRepositoryError::new(
                crate::StatisticsRepositoryErrorKind::Conflict,
                "statistics worker is stopping",
            ));
        }
        let job = self.service.submit_now(request, owner)?;
        if let Err(mpsc::error::TrySendError::Closed(_)) = self.wake.try_send(()) {
            // Submission must not leave a job that no live process runner can
            // own. The cancellation result is best effort only: the caller
            // still receives the authoritative admission failure.
            let _ = self.service.request_cancel_now(job.id, now_ms());
            return Err(StatisticsRepositoryError::new(
                crate::StatisticsRepositoryErrorKind::Conflict,
                "statistics worker is unavailable",
            ));
        }
        Ok(job)
    }

    pub async fn submit_admitted(
        &self,
        request: StatisticsJobCreate,
        owner: WorkOwner,
        query_concurrency: QueryConcurrencyPermit,
    ) -> Result<StatisticsJob, StatisticsRepositoryError> {
        let _admission = self.admission.lock().map_err(|_| {
            StatisticsRepositoryError::new(
                crate::StatisticsRepositoryErrorKind::Conflict,
                "statistics worker admission lock poisoned",
            )
        })?;
        if *self.stop.borrow() {
            return Err(StatisticsRepositoryError::new(
                crate::StatisticsRepositoryErrorKind::Conflict,
                "statistics worker is stopping",
            ));
        }
        let job = self
            .service
            .submit_admitted_now(request, owner, query_concurrency)?;
        if let Err(mpsc::error::TrySendError::Closed(_)) = self.wake.try_send(()) {
            let _ = self.service.request_cancel_now(job.id, now_ms());
            return Err(StatisticsRepositoryError::new(
                crate::StatisticsRepositoryErrorKind::Conflict,
                "statistics worker is unavailable",
            ));
        }
        Ok(job)
    }

    pub async fn list(&self) -> Result<Vec<StatisticsJob>, StatisticsRepositoryError> {
        self.service.list().await
    }

    pub async fn request_cancel(
        &self,
        job_id: StatisticsJobId,
        at_ms: i64,
    ) -> Result<StatisticsJob, StatisticsRepositoryError> {
        self.service.request_cancel(job_id, at_ms).await
    }

    pub fn request_stop_for_process_exit(&self) {
        let Ok(_admission) = self.admission.lock() else {
            return;
        };
        let _ = self.stop.send(true);
        let _ = self.service.request_stop_for_process_exit(now_ms());
    }

    pub async fn shutdown_until(&self, deadline: Instant) -> Result<(), String> {
        self.request_stop_for_process_exit();
        let mut completion = self
            .completion
            .lock()
            .map_err(|_| "statistics worker completion lock poisoned".to_string())?
            .take()
            .ok_or_else(|| "statistics worker shutdown was already awaited".to_string())?;
        match tokio::time::timeout_at(deadline.into(), &mut completion).await {
            Ok(Ok(())) => {
                let join = self
                    .join
                    .lock()
                    .map_err(|_| "statistics worker join lock poisoned".to_string())?
                    .take()
                    .ok_or_else(|| "statistics worker join was already consumed".to_string())?;
                join.await
                    .map_err(|error| format!("statistics worker panicked: {error}"))
            }
            Ok(Err(_)) => Err("statistics worker completion channel closed".to_string()),
            Err(_) => {
                *self
                    .completion
                    .lock()
                    .map_err(|_| "statistics worker completion lock poisoned".to_string())? =
                    Some(completion);
                Err("statistics worker did not stop before shutdown deadline".to_string())
            }
        }
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}

impl Default for StatisticsJobService {
    fn default() -> Self {
        Self::new()
    }
}
