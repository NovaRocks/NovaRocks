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
// software distributed under the Apache License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Process-owned, one-shot ANALYZE worker.
//!
//! There is no lease, takeover, retry queue, startup scan, or reconciliation
//! pass. Every submission is one fresh attempt owned by this frontend process.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use uuid::Uuid;

use super::application::StatisticsPublicationTerminal;
use super::model::{StatisticsJob, StatisticsJobError, StatisticsJobErrorKind, StatisticsJobState};
use super::repository::StatisticsJobRepository;
use crate::workload_lifecycle::{
    FrontendServingLifecycle, FrontendServingSnapshotReader, FrontendServingState,
    FrontendWorkloadKind,
};

pub const STATISTICS_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(30);

/// A failed attempt is terminal. In particular, no error is retryable and a
/// `CommitUnknown` must not invoke any mutation after the failed publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsAttemptError {
    pub kind: StatisticsJobErrorKind,
    pub message: String,
    pub publication: Option<StatisticsPublicationTerminal>,
}

impl StatisticsAttemptError {
    pub fn permanent(kind: StatisticsJobErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            publication: None,
        }
    }

    pub fn publication(
        terminal: StatisticsPublicationTerminal,
        message: impl Into<String>,
    ) -> Self {
        let kind = match terminal {
            StatisticsPublicationTerminal::KnownUncommitted => StatisticsJobErrorKind::Publish,
            StatisticsPublicationTerminal::KnownCommittedFinalization => {
                StatisticsJobErrorKind::KnownCommittedFinalization
            }
            StatisticsPublicationTerminal::CommitUnknown => StatisticsJobErrorKind::CommitUnknown,
        };
        Self {
            kind,
            message: message.into(),
            publication: Some(terminal),
        }
    }
}

/// Connector-neutral execution owned by the frontend process worker.
pub trait StatisticsAttemptExecutor: Send + Sync {
    fn execute(
        &self,
        job: &StatisticsJob,
        cancellation: crate::common::query_cancellation::QueryCancellationView,
    ) -> Result<(), StatisticsAttemptError>;
}

/// Lifecycle owner for the current process worker task.
pub struct StatisticsAnalyzeWorker {
    repository: StatisticsJobRepository,
    stop: Arc<AtomicBool>,
    join: Option<tokio::task::JoinHandle<Result<(), String>>>,
}

impl StatisticsAnalyzeWorker {
    pub async fn start(
        runtime: &tokio::runtime::Handle,
        repository: StatisticsJobRepository,
        executor: Arc<dyn StatisticsAttemptExecutor>,
        workload_lifecycle: FrontendServingLifecycle,
    ) -> Result<Self, String> {
        let stop = Arc::new(AtomicBool::new(false));
        let join = runtime.spawn(run_worker(
            repository.clone(),
            Arc::downgrade(&executor),
            Arc::clone(&stop),
            workload_lifecycle,
        ));
        Ok(Self {
            repository,
            stop,
            join: Some(join),
        })
    }

    pub fn wakeup(&self) {
        // Repository mutations wake the worker. Keep this method for the
        // application port's explicit post-submit signal.
        self.repository.notify_worker();
    }

    pub fn request_stop(&self) {
        self.stop.store(true, Ordering::Release);
        self.wakeup();
    }

    pub async fn shutdown_until(&mut self, deadline: Instant) -> Result<(), String> {
        self.request_stop();
        let now = now_ms();
        let repository = self.repository.clone();
        tokio::time::timeout_at(deadline.into(), async move {
            repository.cancel_active_for_shutdown(now).await
        })
        .await
        .map_err(|_| {
            "statistics worker cancellation exceeded the shared shutdown deadline".to_string()
        })?
        .map_err(|error| error.to_string())?;
        let Some(join) = self.join.as_mut() else {
            return Ok(());
        };
        let joined = tokio::time::timeout_at(deadline.into(), join)
            .await
            .map_err(|_| {
                "statistics worker did not stop before the shared shutdown deadline".to_string()
            })?;
        self.join.take();
        joined.map_err(|error| format!("statistics worker join failed: {error}"))?
    }
}

async fn run_worker(
    repository: StatisticsJobRepository,
    executor: Weak<dyn StatisticsAttemptExecutor>,
    stop: Arc<AtomicBool>,
    workload_lifecycle: FrontendServingLifecycle,
) -> Result<(), String> {
    loop {
        if stop.load(Ordering::Acquire) {
            return Ok(());
        }
        match workload_lifecycle.frontend_serving_snapshot().serving_state {
            FrontendServingState::Starting => {
                tokio::select! {
                    _ = repository.wait_for_change() => {}
                    _ = tokio::time::sleep(Duration::from_millis(100)) => {}
                }
                continue;
            }
            FrontendServingState::Ready => {}
            FrontendServingState::Draining | FrontendServingState::Stopping => return Ok(()),
        }
        let workload_lease = match workload_lifecycle.try_admit(FrontendWorkloadKind::Background) {
            Ok(lease) => lease,
            Err(_) => return Ok(()),
        };
        let Some(job) = repository
            .claim_next(now_ms())
            .await
            .map_err(|error| error.to_string())?
        else {
            drop(workload_lease);
            tokio::select! {
                _ = repository.wait_for_change() => {}
                _ = tokio::time::sleep(Duration::from_millis(100)) => {}
            }
            continue;
        };
        let Some(executor) = executor.upgrade() else {
            return Ok(());
        };
        run_attempt(
            &repository,
            executor,
            job,
            STATISTICS_ATTEMPT_TIMEOUT,
            workload_lease.cancellation_source().view(),
        )
        .await?;
    }
}

async fn run_attempt(
    repository: &StatisticsJobRepository,
    executor: Arc<dyn StatisticsAttemptExecutor>,
    job: StatisticsJob,
    timeout: Duration,
    cancellation: crate::common::query_cancellation::QueryCancellationView,
) -> Result<(), String> {
    let started = Instant::now();
    if must_stop(repository, job.job_id, started, timeout, &cancellation).await? {
        return cancel(
            repository,
            job.job_id,
            StatisticsJobState::Preparing,
            "statistics job cancelled before collection",
        )
        .await;
    }
    let running = repository
        .transition(
            job.job_id,
            StatisticsJobState::Preparing,
            StatisticsJobState::Running,
            now_ms(),
            None,
        )
        .await
        .map_err(|error| error.to_string())?;
    let attempt_cancellation = crate::common::query_cancellation::QueryCancellationSource::new();
    let execution_cancellation = attempt_cancellation.view();
    let execution_job = running.clone();
    let mut execution = tokio::task::spawn_blocking(move || {
        let job_id = execution_job.job_id;
        let _diagnostic_scope = crate::preparation_diagnostics::enter_product_work(
            format!("statistics-job:{job_id}"),
            format!("statistics-job:{job_id}"),
        );
        executor.execute(&execution_job, execution_cancellation)
    });
    let result = loop {
        tokio::select! {
            joined = &mut execution => {
                break joined.map_err(|error| format!("statistics attempt task failed: {error}"))?;
            }
            _ = tokio::time::sleep(Duration::from_millis(50)) => {
                if cancellation.is_cancelled() {
                    let _ = attempt_cancellation.request(
                        crate::common::query_cancellation::QueryCancellationReason::ServerShutdown,
                    );
                } else if started.elapsed() >= timeout {
                    let _ = attempt_cancellation.request(
                        crate::common::query_cancellation::QueryCancellationReason::DeadlineExceeded {
                            timeout_ms: timeout.as_millis().try_into().unwrap_or(u64::MAX),
                        },
                    );
                } else if repository
                    .cancellation_requested(running.job_id)
                    .await
                    .map_err(|error| error.to_string())?
                {
                    let _ = attempt_cancellation.request(
                        crate::common::query_cancellation::QueryCancellationReason::ExplicitKill {
                            requester_connection_id: 0,
                        },
                    );
                }
            }
        }
    };
    match result {
        Ok(()) => {
            repository
                .transition(
                    running.job_id,
                    StatisticsJobState::Running,
                    StatisticsJobState::Succeeded,
                    now_ms(),
                    None,
                )
                .await
                .map_err(|error| error.to_string())?;
            Ok(())
        }
        Err(error) => {
            if error.publication.is_none()
                && let Some(reason) = attempt_cancellation.view().reason()
            {
                return match reason {
                    crate::common::query_cancellation::QueryCancellationReason::DeadlineExceeded { .. } => {
                        finish_error(
                            repository,
                            running.job_id,
                            StatisticsJobState::Running,
                            StatisticsAttemptError::permanent(
                                StatisticsJobErrorKind::DeadlineExceeded,
                                error.message,
                            ),
                        )
                        .await
                    }
                    _ => cancel(
                        repository,
                        running.job_id,
                        StatisticsJobState::Running,
                        &error.message,
                    )
                    .await,
                };
            }
            finish_error(
                repository,
                running.job_id,
                StatisticsJobState::Running,
                error,
            )
            .await
        }
    }
}

async fn must_stop(
    repository: &StatisticsJobRepository,
    job_id: Uuid,
    started: Instant,
    timeout: Duration,
    cancellation: &crate::common::query_cancellation::QueryCancellationView,
) -> Result<bool, String> {
    if cancellation.is_cancelled() || started.elapsed() >= timeout {
        return Ok(true);
    }
    repository
        .cancellation_requested(job_id)
        .await
        .map_err(|error| error.to_string())
}

async fn cancel(
    repository: &StatisticsJobRepository,
    job_id: Uuid,
    expected: StatisticsJobState,
    message: &str,
) -> Result<(), String> {
    repository
        .transition(
            job_id,
            expected,
            StatisticsJobState::Cancelled,
            now_ms(),
            Some(StatisticsJobError {
                kind: StatisticsJobErrorKind::Cancelled,
                message: message.into(),
            }),
        )
        .await
        .map(|_| ())
        .map_err(|error| error.to_string())
}

async fn finish_error(
    repository: &StatisticsJobRepository,
    job_id: Uuid,
    expected: StatisticsJobState,
    error: StatisticsAttemptError,
) -> Result<(), String> {
    let (next, kind) = match error.publication {
        Some(StatisticsPublicationTerminal::CommitUnknown) => (
            StatisticsJobState::CommitUnknown,
            StatisticsJobErrorKind::CommitUnknown,
        ),
        Some(StatisticsPublicationTerminal::KnownCommittedFinalization) => (
            StatisticsJobState::Succeeded,
            StatisticsJobErrorKind::KnownCommittedFinalization,
        ),
        Some(StatisticsPublicationTerminal::KnownUncommitted) | None => {
            (StatisticsJobState::Failed, error.kind)
        }
    };
    repository
        .transition(
            job_id,
            expected,
            next,
            now_ms(),
            Some(StatisticsJobError {
                kind,
                message: error.message,
            }),
        )
        .await
        .map(|_| ())
        .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::time::{Duration, Instant};

    use super::{
        StatisticsAnalyzeWorker, StatisticsAttemptError, StatisticsAttemptExecutor, run_worker,
    };
    use crate::statistics_jobs::model::StatisticsJob;
    use crate::statistics_jobs::repository::StatisticsJobRepository;
    use crate::workload_lifecycle::FrontendServingLifecycle;

    struct NeverRunExecutor;

    impl StatisticsAttemptExecutor for NeverRunExecutor {
        fn execute(
            &self,
            _job: &StatisticsJob,
            _cancellation: crate::common::query_cancellation::QueryCancellationView,
        ) -> Result<(), StatisticsAttemptError> {
            unreachable!("draining must reject before a statistics attempt starts")
        }
    }

    #[tokio::test]
    async fn draining_rejects_the_worker_before_it_can_claim_an_attempt() {
        let lifecycle = FrontendServingLifecycle::new();
        lifecycle.mark_ready().expect("mark lifecycle ready");
        lifecycle.begin_drain(Duration::from_secs(1));
        let executor: Arc<dyn StatisticsAttemptExecutor> = Arc::new(NeverRunExecutor);

        run_worker(
            StatisticsJobRepository::new(),
            Arc::downgrade(&executor),
            Arc::new(AtomicBool::new(false)),
            lifecycle,
        )
        .await
        .expect("draining worker exits without claiming a job");
    }

    #[tokio::test]
    async fn starting_worker_waits_for_the_serving_lifecycle() {
        let lifecycle = FrontendServingLifecycle::new();
        let repository = StatisticsJobRepository::new();
        let stop = Arc::new(AtomicBool::new(false));
        let executor: Arc<dyn StatisticsAttemptExecutor> = Arc::new(NeverRunExecutor);
        let worker = tokio::spawn(run_worker(
            repository.clone(),
            Arc::downgrade(&executor),
            Arc::clone(&stop),
            lifecycle.clone(),
        ));

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !worker.is_finished(),
            "the statistics worker must not exit while the frontend is starting"
        );

        lifecycle.mark_ready().expect("mark lifecycle ready");
        lifecycle.begin_drain(Duration::from_secs(1));
        repository.notify_worker();
        tokio::time::timeout(Duration::from_secs(1), worker)
            .await
            .expect("worker observes the terminal serving state")
            .expect("worker task joins")
            .expect("worker exits without claiming a job");
    }

    #[tokio::test]
    async fn shared_deadline_retains_the_same_statistics_join_for_retry() {
        let release = Arc::new(tokio::sync::Notify::new());
        let wait = Arc::clone(&release);
        let join = tokio::spawn(async move {
            wait.notified().await;
            Ok(())
        });
        let mut worker = StatisticsAnalyzeWorker {
            repository: StatisticsJobRepository::new(),
            stop: Arc::new(AtomicBool::new(false)),
            join: Some(join),
        };

        let error = worker
            .shutdown_until(Instant::now() + Duration::from_millis(10))
            .await
            .expect_err("blocked statistics worker must respect the shared deadline");
        assert!(error.contains("shared shutdown deadline"));
        assert!(worker.join.is_some());

        release.notify_one();
        worker
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .expect("the retained statistics join remains retryable");
        assert!(worker.join.is_none());
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}
