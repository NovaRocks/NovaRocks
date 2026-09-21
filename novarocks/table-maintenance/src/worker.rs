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

//! Product-owned current-process OPTIMIZE job execution.
//!
//! The product owns job claim, terminal interpretation, cancellation checks,
//! and shutdown cancellation. Role composition supplies only a governed root
//! scope and one exact provider/native dispatch adapter.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use tokio::runtime::Handle;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::runtime::TerminalError;
use crate::{
    AutomaticMaintenanceOutcome, MaintenanceEffectId, MaintenanceTargetRebind, OptimizeJob,
    OptimizeProcessRuntime, optimize_job_outcome_from_action,
};

/// The host-owned scope that accounts for one current-process OPTIMIZE job.
///
/// Dropping this value completes the host's admission bookkeeping. The product
/// reads cancellation but never manufactures a host scope or a second
/// admission authority.
pub trait OptimizeJobScope: Send + Sync {
    fn is_cancelled(&self) -> Result<bool, String>;
}

/// Result of asking the host for one Table Maintenance root scope.
pub enum OptimizeJobAdmission {
    Acquired(Box<dyn OptimizeJobScope>),
    RetryLater,
    Closed,
}

/// Host adapter for role-local admission and resource attribution.
#[async_trait::async_trait]
pub trait OptimizeJobAdmissionPort: Send + Sync {
    async fn begin(&self) -> Result<OptimizeJobAdmission, String>;
}

/// Exact role-local provider/native execution capability for one OPTIMIZE job.
///
/// The product decides when target rebind and provider dispatch may occur. The
/// host validates the frozen physical object identity and consumes the native
/// execution capability; it cannot replace the product's job state machine.
pub trait OptimizeJobExecutionPort: Send + Sync {
    /// Whether the role composition that owns the provider/native capability
    /// still exists. Loss of that owner cancels current-process work instead
    /// of translating host teardown into a provider failure.
    fn is_available(&self) -> bool;

    /// Acquires one exact provider/native execution lease for a claimed job.
    /// The returned value keeps the host capability alive through both target
    /// rebind and dispatch, so host teardown cannot race between them.
    fn acquire(&self) -> Option<Box<dyn OptimizeJobExecution>>;
}

/// One exact provider/native execution lease for a claimed OPTIMIZE job.
pub trait OptimizeJobExecution: Send {
    fn rebind_target(&self, job: &OptimizeJob) -> Result<MaintenanceTargetRebind, String>;

    fn execute(&self, job: &OptimizeJob) -> Result<crate::MaintenanceActionOutcome, TerminalError>;

    /// Executes an MV-owned OPTIMIZE using the identity frozen before job
    /// submission. An adapter without this capability fails before dispatch.
    fn execute_automatic(
        &self,
        _job: &OptimizeJob,
        _effect_id: MaintenanceEffectId,
    ) -> Result<AutomaticMaintenanceOutcome, TerminalError> {
        Err(TerminalError::pre_dispatch_failed(
            "automatic optimize effect identity is unsupported",
        ))
    }
}

/// The sole current-process OPTIMIZE worker owner.
pub struct OptimizeWorker {
    runtime: Arc<OptimizeProcessRuntime>,
    stop: Arc<AtomicBool>,
    wakeup: Arc<Notify>,
    join: Option<JoinHandle<Result<(), String>>>,
}

impl OptimizeWorker {
    pub fn start(
        runtime: &Handle,
        jobs: Arc<OptimizeProcessRuntime>,
        admission: Arc<dyn OptimizeJobAdmissionPort>,
        execution: Arc<dyn OptimizeJobExecutionPort>,
    ) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let wakeup = Arc::new(Notify::new());
        let join = runtime.spawn(run_worker(
            Arc::clone(&jobs),
            admission,
            execution,
            Arc::clone(&stop),
            Arc::clone(&wakeup),
        ));
        Self {
            runtime: jobs,
            stop,
            wakeup,
            join: Some(join),
        }
    }

    pub fn wakeup(&self) {
        self.wakeup.notify_one();
    }

    pub fn request_stop(&self) {
        self.runtime.stop_admission();
        self.stop.store(true, Ordering::Release);
        self.wakeup();
    }

    pub async fn shutdown_until(&mut self, deadline: Instant) -> Result<(), String> {
        self.request_stop();
        let Some(join) = self.join.as_mut() else {
            return Ok(());
        };
        let joined = tokio::time::timeout_at(deadline.into(), join)
            .await
            .map_err(|_| {
                "table maintenance worker did not stop before the shared shutdown deadline"
                    .to_string()
            })?;
        self.join.take();
        joined.map_err(|error| format!("table maintenance worker join failed: {error}"))?
    }

    pub fn has_join_owner(&self) -> bool {
        self.join.is_some()
    }
}

async fn run_worker(
    jobs: Arc<OptimizeProcessRuntime>,
    admission: Arc<dyn OptimizeJobAdmissionPort>,
    execution: Arc<dyn OptimizeJobExecutionPort>,
    stop: Arc<AtomicBool>,
    wakeup: Arc<Notify>,
) -> Result<(), String> {
    loop {
        if stop.load(Ordering::Acquire) {
            cancel_for_shutdown(jobs.as_ref()).await?;
            return Ok(());
        }
        if !execution.is_available() {
            cancel_for_shutdown(jobs.as_ref()).await?;
            return Ok(());
        }
        let scope = match admission.begin().await? {
            OptimizeJobAdmission::Acquired(scope) => scope,
            OptimizeJobAdmission::RetryLater => {
                tokio::select! {
                    _ = wakeup.notified() => {}
                    _ = jobs.wait_for_change() => {}
                    _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {}
                }
                continue;
            }
            OptimizeJobAdmission::Closed => {
                cancel_for_shutdown(jobs.as_ref()).await?;
                return Ok(());
            }
        };
        let claimed = jobs
            .claim_next(now_unix_millis())
            .await
            .map_err(|error| format!("claim current optimize job failed: {error}"))?;
        let Some(job) = claimed else {
            drop(scope);
            tokio::select! {
                _ = wakeup.notified() => {}
                _ = jobs.wait_for_change() => {}
            }
            continue;
        };
        let Some(execution) = execution.acquire() else {
            drop(scope);
            cancel_for_shutdown(jobs.as_ref()).await?;
            return Ok(());
        };
        let result = execute_claimed_job(jobs.as_ref(), execution, scope.as_ref(), job).await;
        drop(scope);
        result?;
    }
}

async fn cancel_for_shutdown(jobs: &OptimizeProcessRuntime) -> Result<(), String> {
    jobs.request_shutdown_cancellation()
        .await
        .map_err(|error| format!("request optimize shutdown cancellation failed: {error}"))
}

async fn execute_claimed_job(
    jobs: &OptimizeProcessRuntime,
    execution: Box<dyn OptimizeJobExecution>,
    scope: &dyn OptimizeJobScope,
    job: OptimizeJob,
) -> Result<(), String> {
    let job_id = job.job_id;
    let initially_cancelled = match scope.is_cancelled() {
        Ok(cancelled) => cancelled,
        Err(error) => {
            return finish(
                jobs,
                job_id,
                Err(TerminalError::pre_dispatch_failed(format!(
                    "read optimize cancellation before target rebind failed: {error}"
                ))),
            )
            .await;
        }
    };
    if initially_cancelled {
        return finish_cancelled(jobs, job_id, "optimize job cancelled before target rebind").await;
    }
    let terminal = match execution.rebind_target(&job) {
        Ok(MaintenanceTargetRebind::Bound) => None,
        Ok(MaintenanceTargetRebind::Replaced) => Some(Err(TerminalError::target_replaced(
            "optimize target was replaced before provider dispatch",
        ))),
        Ok(MaintenanceTargetRebind::Missing) => Some(Err(TerminalError::pre_dispatch_failed(
            "optimize target is missing before provider dispatch",
        ))),
        Err(error) => Some(Err(TerminalError::pre_dispatch_failed(format!(
            "optimize target rebind failed before provider dispatch: {error}"
        )))),
    };
    if let Some(terminal) = terminal {
        return finish(jobs, job_id, terminal).await;
    }
    let scope_cancelled = match scope.is_cancelled() {
        Ok(cancelled) => cancelled,
        Err(error) => {
            return finish(
                jobs,
                job_id,
                Err(TerminalError::pre_dispatch_failed(format!(
                    "read optimize cancellation before provider dispatch failed: {error}"
                ))),
            )
            .await;
        }
    };
    let job_cancelled = match jobs.cancellation_requested(job_id).await {
        Ok(cancelled) => cancelled,
        Err(error) => {
            return finish(
                jobs,
                job_id,
                Err(TerminalError::pre_dispatch_failed(format!(
                    "read optimize job cancellation before provider dispatch failed: {error}"
                ))),
            )
            .await;
        }
    };
    if scope_cancelled || job_cancelled {
        return finish_cancelled(
            jobs,
            job_id,
            "optimize job cancelled before provider dispatch",
        )
        .await;
    }
    let automatic = job.effect_id.is_some();
    let execution = tokio::task::spawn_blocking(move || match job.effect_id {
        Some(effect_id) => execution.execute_automatic(&job, effect_id).map(|outcome| {
            let (action, committed) = match outcome {
                AutomaticMaintenanceOutcome::KnownCommitted(action) => (action, true),
                AutomaticMaintenanceOutcome::NoOpWithoutCommit(action) => (action, false),
            };
            (action, Some(committed))
        }),
        None => execution.execute(&job).map(|action| (action, None)),
    })
    .await;
    let terminal = match execution {
        Ok(Ok((outcome, committed))) => optimize_job_outcome_from_action(outcome)
            .map(|mut outcome| {
                outcome.commit_occurred = committed;
                outcome
            })
            .map_err(|error| {
                if committed == Some(true) {
                    TerminalError::known_committed_finalization_failed(error)
                } else {
                    TerminalError::failed(error)
                }
            }),
        Ok(Err(terminal)) => Err(terminal),
        Err(error) => {
            let message = format!("optimize job {job_id} engine task failed: {error}");
            Err(if automatic {
                TerminalError::commit_unknown(message)
            } else {
                TerminalError::failed(message)
            })
        }
    };
    finish(jobs, job_id, terminal).await
}

async fn finish_cancelled(
    jobs: &OptimizeProcessRuntime,
    job_id: i64,
    message: &'static str,
) -> Result<(), String> {
    finish(
        jobs,
        job_id,
        Err(TerminalError::cancelled_before_dispatch(message)),
    )
    .await
}

async fn finish(
    jobs: &OptimizeProcessRuntime,
    job_id: i64,
    terminal: Result<crate::OptimizeJobOutcome, TerminalError>,
) -> Result<(), String> {
    jobs.finish(job_id, terminal, now_unix_millis())
        .await
        .map(|_| ())
        .map_err(|error| format!("record optimize terminal failed: {error}"))
}

fn now_unix_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use super::*;
    use crate::activity::{MaintenanceActivityFamily, TableMaintenanceActivity};
    use crate::runtime::JobCreate;
    use crate::{MaintenanceActionOutcome, MaintenanceTarget};

    struct TestScope {
        cancelled: bool,
    }

    impl OptimizeJobScope for TestScope {
        fn is_cancelled(&self) -> Result<bool, String> {
            Ok(self.cancelled)
        }
    }

    struct TestAdmission {
        ready: Arc<AtomicBool>,
        closed: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl OptimizeJobAdmissionPort for TestAdmission {
        async fn begin(&self) -> Result<OptimizeJobAdmission, String> {
            if self.closed.load(Ordering::Acquire) {
                return Ok(OptimizeJobAdmission::Closed);
            }
            if !self.ready.load(Ordering::Acquire) {
                return Ok(OptimizeJobAdmission::RetryLater);
            }
            Ok(OptimizeJobAdmission::Acquired(Box::new(TestScope {
                cancelled: false,
            })))
        }
    }

    #[derive(Clone)]
    struct RecordingExecution {
        calls: Arc<Mutex<Vec<&'static str>>>,
        rebind: MaintenanceTargetRebind,
    }

    impl OptimizeJobExecutionPort for RecordingExecution {
        fn is_available(&self) -> bool {
            true
        }

        fn acquire(&self) -> Option<Box<dyn OptimizeJobExecution>> {
            Some(Box::new(self.clone()))
        }
    }

    impl OptimizeJobExecution for RecordingExecution {
        fn rebind_target(&self, _job: &OptimizeJob) -> Result<MaintenanceTargetRebind, String> {
            self.calls.lock().expect("calls lock").push("rebind");
            Ok(self.rebind)
        }

        fn execute(&self, _job: &OptimizeJob) -> Result<MaintenanceActionOutcome, TerminalError> {
            self.calls.lock().expect("calls lock").push("execute");
            Ok(MaintenanceActionOutcome::RewriteDataFiles {
                target_snapshot_id: Some(1),
                rewritten_data_files_count: 1,
                added_data_files_count: None,
                added_delete_files_count: None,
                rewritten_bytes_count: 0,
                failed_data_files_count: 0,
                removed_delete_files_count: 0,
                output_record_count: None,
            })
        }
    }

    fn submit_job(jobs: &OptimizeProcessRuntime) -> impl std::future::Future<Output = i64> + '_ {
        async move {
            let target = MaintenanceTarget {
                catalog: "catalog".to_string(),
                namespace: "namespace".to_string(),
                table: "table".to_string(),
            };
            let permit = TableMaintenanceActivity::default()
                .acquire(&target, MaintenanceActivityFamily::Optimize)
                .expect("permit");
            jobs.submit(
                JobCreate {
                    target,
                    object_id: vec![1],
                    base_snapshot_id: 1,
                    created_at_ms: 1,
                    effect_id: None,
                },
                permit,
            )
            .await
            .expect("submit")
            .job_id
        }
    }

    #[tokio::test]
    async fn worker_waits_for_admission_then_owns_the_complete_job_lifecycle() {
        let jobs = Arc::new(OptimizeProcessRuntime::new());
        let ready = Arc::new(AtomicBool::new(false));
        let closed = Arc::new(AtomicBool::new(false));
        let calls = Arc::new(Mutex::new(Vec::new()));
        let job_id = submit_job(jobs.as_ref()).await;
        let mut worker = OptimizeWorker::start(
            &Handle::current(),
            Arc::clone(&jobs),
            Arc::new(TestAdmission {
                ready: Arc::clone(&ready),
                closed: Arc::clone(&closed),
            }),
            Arc::new(RecordingExecution {
                calls: Arc::clone(&calls),
                rebind: MaintenanceTargetRebind::Bound,
            }),
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            jobs.get(job_id)
                .await
                .expect("job lookup")
                .is_some_and(|job| job.state == crate::runtime::MaintenanceJobState::Pending),
            "the product worker must not claim work before host admission"
        );
        ready.store(true, Ordering::Release);
        worker.wakeup();
        let terminal = jobs
            .wait_for_completion(job_id)
            .await
            .expect("completed job");
        assert_eq!(
            terminal.state,
            crate::runtime::MaintenanceJobState::Finished
        );
        assert_eq!(*calls.lock().expect("calls lock"), ["rebind", "execute"]);
        closed.store(true, Ordering::Release);
        worker.wakeup();
        worker
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .expect("worker shutdown");
    }

    #[tokio::test]
    async fn target_replacement_finishes_without_provider_dispatch() {
        let jobs = Arc::new(OptimizeProcessRuntime::new());
        let ready = Arc::new(AtomicBool::new(true));
        let closed = Arc::new(AtomicBool::new(false));
        let calls = Arc::new(Mutex::new(Vec::new()));
        let job_id = submit_job(jobs.as_ref()).await;
        let mut worker = OptimizeWorker::start(
            &Handle::current(),
            Arc::clone(&jobs),
            Arc::new(TestAdmission { ready, closed }),
            Arc::new(RecordingExecution {
                calls: Arc::clone(&calls),
                rebind: MaintenanceTargetRebind::Replaced,
            }),
        );
        let terminal = jobs
            .wait_for_completion(job_id)
            .await
            .expect("completed job");
        assert_eq!(
            terminal.state,
            crate::runtime::MaintenanceJobState::TargetReplaced
        );
        assert_eq!(*calls.lock().expect("calls lock"), ["rebind"]);
        worker
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .expect("worker shutdown");
    }

    #[tokio::test]
    async fn shared_deadline_retains_the_same_optimize_join_for_retry() {
        let release = Arc::new(Notify::new());
        let wait = Arc::clone(&release);
        let join = tokio::spawn(async move {
            wait.notified().await;
            Ok(())
        });
        let mut worker = OptimizeWorker {
            runtime: Arc::new(OptimizeProcessRuntime::new()),
            stop: Arc::new(AtomicBool::new(false)),
            wakeup: Arc::new(Notify::new()),
            join: Some(join),
        };
        let error = worker
            .shutdown_until(Instant::now() + Duration::from_millis(10))
            .await
            .expect_err("blocked optimize worker must respect the shared deadline");
        assert!(error.contains("shared shutdown deadline"));
        assert!(worker.has_join_owner());
        release.notify_one();
        worker
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .expect("the retained optimize join remains retryable");
        assert!(!worker.has_join_owner());
    }
}
