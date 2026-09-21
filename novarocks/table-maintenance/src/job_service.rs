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

//! Product-owned current-process OPTIMIZE submission and observation.

use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use tokio::runtime::Handle;

use crate::activity::{
    MaintenanceActivityBusy, MaintenanceActivityFamily, MaintenanceActivityPermit,
    TableMaintenanceActivity,
};
use crate::runtime::{JobCreate, JobHandle, MaintenanceJobState, RuntimeErrorKind, TerminalError};
use crate::worker::{OptimizeJobAdmissionPort, OptimizeJobExecutionPort, OptimizeWorker};
use crate::{
    MaintenanceEffectId, MaintenanceTarget, OptimizeJob, OptimizeProcessRuntime, OptimizeSubmission,
};

/// Exact provider facts captured only after the product has the target gate.
///
/// The product persists these opaque values without importing a provider
/// object, a connector handle, or any Native transport type.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapturedOptimizeTarget {
    pub object_id: Vec<u8>,
    pub base_snapshot_id: i64,
}

/// Role-local adapter that captures the current provider binding for an
/// already-gated target.
pub trait OptimizeTargetCapturePort: Send + Sync {
    fn capture(&self, target: &MaintenanceTarget) -> Result<CapturedOptimizeTarget, String>;
}

/// The process-local business owner for maintenance conflict rights and
/// OPTIMIZE job submission, listing, and completion observation.
#[derive(Clone, Default)]
pub struct OptimizeJobService {
    activity: TableMaintenanceActivity,
    runtime: Arc<OptimizeProcessRuntime>,
}

enum WorkerLifecycle {
    NotStarted,
    Started(OptimizeWorker),
    Stopped(Result<(), String>),
}

/// The process-local OPTIMIZE product runtime.
///
/// This owner combines the job ledger with its product worker lifecycle. Role
/// composition supplies the runtime and the narrow admission/execution ports,
/// but it cannot advance, wake, stop, or join jobs itself.
pub struct OptimizeJobRuntime {
    service: OptimizeJobService,
    worker: Mutex<WorkerLifecycle>,
}

impl Default for OptimizeJobRuntime {
    fn default() -> Self {
        Self {
            service: OptimizeJobService::new(),
            worker: Mutex::new(WorkerLifecycle::NotStarted),
        }
    }
}

impl OptimizeJobService {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn runtime(&self) -> Arc<OptimizeProcessRuntime> {
        Arc::clone(&self.runtime)
    }

    pub fn acquire_activity(
        &self,
        target: &MaintenanceTarget,
        family: MaintenanceActivityFamily,
    ) -> Result<MaintenanceActivityPermit, MaintenanceActivityBusy> {
        self.activity.acquire(target, family)
    }

    /// Acquires the shared target gate before requesting provider facts, which
    /// prevents a same-name replacement from crossing a lock-before-binding
    /// window.
    pub async fn submit_optimize(
        &self,
        target: MaintenanceTarget,
        capture: &dyn OptimizeTargetCapturePort,
    ) -> Result<OptimizeSubmission, String> {
        self.submit_optimize_with_effect_id(target, capture, None)
            .await
    }

    pub async fn submit_automatic_optimize(
        &self,
        target: MaintenanceTarget,
        capture: &dyn OptimizeTargetCapturePort,
        effect_id: MaintenanceEffectId,
    ) -> Result<OptimizeSubmission, TerminalError> {
        self.submit_optimize_with_effect_id(target, capture, Some(effect_id))
            .await
            .map_err(TerminalError::pre_dispatch_failed)
    }

    async fn submit_optimize_with_effect_id(
        &self,
        target: MaintenanceTarget,
        capture: &dyn OptimizeTargetCapturePort,
        effect_id: Option<MaintenanceEffectId>,
    ) -> Result<OptimizeSubmission, String> {
        let permit = self
            .acquire_activity(&target, MaintenanceActivityFamily::Optimize)
            .map_err(|_| "an optimize job is already active for this table".to_string())?;
        let captured = capture.capture(&target)?;
        match self
            .runtime
            .submit(
                JobCreate {
                    target,
                    object_id: captured.object_id,
                    base_snapshot_id: captured.base_snapshot_id,
                    created_at_ms: now_unix_millis(),
                    effect_id,
                },
                permit,
            )
            .await
        {
            Ok(job) => Ok(OptimizeSubmission::Submitted { job_id: job.job_id }),
            Err(error) if error.kind() == RuntimeErrorKind::AlreadyActive => {
                Ok(OptimizeSubmission::AlreadyActive)
            }
            Err(error) => Err(format!("create optimize job failed: {error}")),
        }
    }

    pub async fn list(&self) -> Result<Vec<OptimizeJob>, String> {
        self.runtime
            .list()
            .await
            .map_err(|error| format!("list optimize jobs failed: {error}"))
    }

    pub async fn wait_for_completion(
        &self,
        handle: JobHandle,
    ) -> Result<MaintenanceJobState, String> {
        self.runtime
            .wait_for_completion(handle.job_id())
            .await
            .map(|job| job.state)
            .map_err(|error| format!("wait for optimize job failed: {error}"))
    }

    pub async fn wait_for_terminal_record(&self, handle: JobHandle) -> Result<OptimizeJob, String> {
        self.runtime
            .wait_for_completion(handle.job_id())
            .await
            .map_err(|error| format!("wait for optimize job failed: {error}"))
    }

    pub fn stop_admission(&self) {
        self.runtime.stop_admission();
    }
}

impl OptimizeJobRuntime {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn acquire_activity(
        &self,
        target: &MaintenanceTarget,
        family: MaintenanceActivityFamily,
    ) -> Result<MaintenanceActivityPermit, MaintenanceActivityBusy> {
        self.service.acquire_activity(target, family)
    }

    pub fn start(
        &self,
        runtime: &Handle,
        admission: Arc<dyn OptimizeJobAdmissionPort>,
        execution: Arc<dyn OptimizeJobExecutionPort>,
    ) -> Result<(), String> {
        let mut lifecycle = self
            .worker
            .lock()
            .map_err(|error| format!("table maintenance worker lifecycle lock: {error}"))?;
        match &*lifecycle {
            WorkerLifecycle::NotStarted => {
                *lifecycle = WorkerLifecycle::Started(OptimizeWorker::start(
                    runtime,
                    self.service.runtime(),
                    admission,
                    execution,
                ));
                Ok(())
            }
            WorkerLifecycle::Started(_) => {
                Err("table maintenance service is already started".to_string())
            }
            WorkerLifecycle::Stopped(_) => {
                Err("table maintenance service cannot be restarted after shutdown".to_string())
            }
        }
    }

    pub async fn submit_optimize(
        &self,
        target: MaintenanceTarget,
        capture: &dyn OptimizeTargetCapturePort,
    ) -> Result<OptimizeSubmission, String> {
        let submission = self.service.submit_optimize(target, capture).await?;
        self.wakeup_after_submission(submission)
    }

    pub async fn submit_automatic_optimize(
        &self,
        target: MaintenanceTarget,
        capture: &dyn OptimizeTargetCapturePort,
        effect_id: MaintenanceEffectId,
    ) -> Result<OptimizeSubmission, TerminalError> {
        let submission = self
            .service
            .submit_automatic_optimize(target, capture, effect_id)
            .await?;
        self.wakeup_after_submission(submission)
            .map_err(TerminalError::commit_unknown)
    }

    fn wakeup_after_submission(
        &self,
        submission: OptimizeSubmission,
    ) -> Result<OptimizeSubmission, String> {
        if matches!(submission, OptimizeSubmission::Submitted { .. }) {
            self.wakeup_worker()?;
        }
        Ok(submission)
    }

    pub async fn list(&self) -> Result<Vec<OptimizeJob>, String> {
        self.service.list().await
    }

    pub async fn wait_for_completion(
        &self,
        handle: JobHandle,
    ) -> Result<MaintenanceJobState, String> {
        self.service.wait_for_completion(handle).await
    }

    pub async fn wait_for_terminal_record(&self, handle: JobHandle) -> Result<OptimizeJob, String> {
        self.service.wait_for_terminal_record(handle).await
    }

    pub fn stop_admission(&self) {
        self.service.stop_admission();
    }

    pub async fn shutdown_until(&self, deadline: Instant) -> Result<(), String> {
        self.stop_admission();
        let previous = {
            let mut lifecycle = self
                .worker
                .lock()
                .map_err(|error| format!("table maintenance worker lifecycle lock: {error}"))?;
            std::mem::replace(&mut *lifecycle, WorkerLifecycle::Stopped(Ok(())))
        };
        let (next, result) = match previous {
            WorkerLifecycle::NotStarted => (WorkerLifecycle::Stopped(Ok(())), Ok(())),
            WorkerLifecycle::Started(mut worker) => {
                let result = worker.shutdown_until(deadline).await;
                if result.is_err() && worker.has_join_owner() {
                    (WorkerLifecycle::Started(worker), result)
                } else {
                    (WorkerLifecycle::Stopped(result.clone()), result)
                }
            }
            WorkerLifecycle::Stopped(result) => (WorkerLifecycle::Stopped(result.clone()), result),
        };
        let mut lifecycle = self
            .worker
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        *lifecycle = next;
        result
    }

    pub fn request_shutdown_for_process_exit(&self) {
        self.stop_admission();
        let lifecycle = self
            .worker
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let WorkerLifecycle::Started(worker) = &*lifecycle {
            worker.request_stop();
        }
    }

    fn wakeup_worker(&self) -> Result<(), String> {
        let worker = self
            .worker
            .lock()
            .map_err(|error| format!("table maintenance worker lifecycle lock: {error}"))?;
        if let WorkerLifecycle::Started(worker) = &*worker {
            worker.wakeup();
        }
        Ok(())
    }
}

fn now_unix_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use super::*;
    use crate::worker::{OptimizeJobAdmission, OptimizeJobExecution, OptimizeJobScope};
    use crate::{AutomaticMaintenanceOutcome, MaintenanceActionOutcome, MaintenanceTargetRebind};

    struct FixedCapture;

    impl OptimizeTargetCapturePort for FixedCapture {
        fn capture(&self, _target: &MaintenanceTarget) -> Result<CapturedOptimizeTarget, String> {
            Ok(CapturedOptimizeTarget {
                object_id: vec![7],
                base_snapshot_id: 11,
            })
        }
    }

    struct ActiveScope;

    impl OptimizeJobScope for ActiveScope {
        fn is_cancelled(&self) -> Result<bool, String> {
            Ok(false)
        }
    }

    struct ReadyAdmission;

    #[async_trait::async_trait]
    impl OptimizeJobAdmissionPort for ReadyAdmission {
        async fn begin(&self) -> Result<OptimizeJobAdmission, String> {
            Ok(OptimizeJobAdmission::Acquired(Box::new(ActiveScope)))
        }
    }

    struct SuccessfulExecution;

    impl OptimizeJobExecutionPort for SuccessfulExecution {
        fn is_available(&self) -> bool {
            true
        }

        fn acquire(&self) -> Option<Box<dyn OptimizeJobExecution>> {
            Some(Box::new(Self))
        }
    }

    impl OptimizeJobExecution for SuccessfulExecution {
        fn rebind_target(&self, _job: &OptimizeJob) -> Result<MaintenanceTargetRebind, String> {
            Ok(MaintenanceTargetRebind::Bound)
        }

        fn execute(
            &self,
            _job: &OptimizeJob,
        ) -> Result<MaintenanceActionOutcome, crate::runtime::TerminalError> {
            Ok(MaintenanceActionOutcome::RewriteDataFiles {
                target_snapshot_id: Some(1),
                rewritten_data_files_count: 1,
                added_data_files_count: Some(1),
                added_delete_files_count: Some(0),
                rewritten_bytes_count: 1,
                failed_data_files_count: 0,
                removed_delete_files_count: 0,
                output_record_count: Some(1),
            })
        }
    }

    fn target() -> MaintenanceTarget {
        MaintenanceTarget {
            catalog: "catalog".to_string(),
            namespace: "namespace".to_string(),
            table: "table".to_string(),
        }
    }

    #[tokio::test]
    async fn submission_keeps_the_exact_handle_and_shared_target_gate() {
        let service = OptimizeJobService::new();
        let first = service
            .submit_optimize(target(), &FixedCapture)
            .await
            .expect("submit first optimize");
        let handle = first.handle().expect("first submission has a handle");
        assert_eq!(handle.job_id(), first.handle().unwrap().job_id());
        assert!(
            service
                .submit_optimize(target(), &FixedCapture)
                .await
                .expect_err("another caller cannot join the first job")
                .contains("already active")
        );
    }

    #[tokio::test]
    async fn capture_happens_only_after_the_shared_target_gate() {
        struct PanicCapture;
        impl OptimizeTargetCapturePort for PanicCapture {
            fn capture(
                &self,
                _target: &MaintenanceTarget,
            ) -> Result<CapturedOptimizeTarget, String> {
                panic!("capture must not run while target is busy")
            }
        }

        let service = OptimizeJobService::new();
        let _permit = service
            .acquire_activity(&target(), MaintenanceActivityFamily::Cleanup)
            .expect("take target gate");
        assert!(
            service
                .submit_optimize(target(), &PanicCapture)
                .await
                .expect_err("busy is not capture failure")
                .contains("already active")
        );
    }

    #[tokio::test]
    async fn automatic_submit_distinguishes_pre_dispatch_from_queued_wakeup_failure() {
        let service = OptimizeJobService::new();
        let busy_target = target();
        let _permit = service
            .acquire_activity(&busy_target, MaintenanceActivityFamily::Cleanup)
            .expect("take target gate");
        let effect_id = MaintenanceEffectId::from_bytes([8; 16]);
        let error = service
            .submit_automatic_optimize(busy_target, &FixedCapture, effect_id)
            .await
            .expect_err("busy target has not dispatched");
        assert_eq!(error.state, MaintenanceJobState::PreDispatchFailed);

        let runtime = Arc::new(OptimizeJobRuntime::new());
        let poison_runtime = Arc::clone(&runtime);
        let poison = std::thread::spawn(move || {
            let _lifecycle = poison_runtime.worker.lock().expect("lock lifecycle");
            panic!("poison lifecycle lock before wakeup");
        });
        assert!(poison.join().is_err());
        let error = runtime
            .submit_automatic_optimize(target(), &FixedCapture, effect_id)
            .await
            .expect_err("poisoned wakeup follows job submission");
        assert_eq!(error.state, MaintenanceJobState::CommitUnknown);
        let queued = runtime.list().await.expect("queued job remains observable");
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].effect_id, Some(effect_id));
    }

    #[tokio::test]
    async fn product_runtime_owns_submission_wakeup_and_worker_join() {
        let runtime = OptimizeJobRuntime::new();
        runtime
            .start(
                &Handle::current(),
                Arc::new(ReadyAdmission),
                Arc::new(SuccessfulExecution),
            )
            .expect("start product runtime");
        let submission = runtime
            .submit_optimize(target(), &FixedCapture)
            .await
            .expect("submit optimize");
        let handle = submission.handle().expect("submitted job has a handle");
        let terminal =
            tokio::time::timeout(Duration::from_secs(1), runtime.wait_for_completion(handle))
                .await
                .expect("worker completes submitted job")
                .expect("observe job terminal");
        assert_eq!(terminal, MaintenanceJobState::Finished);
        runtime
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .expect("join product worker");
    }

    #[tokio::test]
    async fn automatic_job_dispatches_frozen_effect_id_and_keeps_noop_terminal() {
        struct AutomaticExecution;

        impl OptimizeJobExecutionPort for AutomaticExecution {
            fn is_available(&self) -> bool {
                true
            }

            fn acquire(&self) -> Option<Box<dyn OptimizeJobExecution>> {
                Some(Box::new(Self))
            }
        }

        impl OptimizeJobExecution for AutomaticExecution {
            fn rebind_target(&self, _job: &OptimizeJob) -> Result<MaintenanceTargetRebind, String> {
                Ok(MaintenanceTargetRebind::Bound)
            }

            fn execute(
                &self,
                _job: &OptimizeJob,
            ) -> Result<MaintenanceActionOutcome, crate::runtime::TerminalError> {
                panic!("automatic job must not use the user optimize path")
            }

            fn execute_automatic(
                &self,
                job: &OptimizeJob,
                effect_id: MaintenanceEffectId,
            ) -> Result<AutomaticMaintenanceOutcome, crate::runtime::TerminalError> {
                assert_eq!(job.effect_id, Some(effect_id));
                assert_eq!(effect_id.to_bytes(), [7; 16]);
                Ok(AutomaticMaintenanceOutcome::NoOpWithoutCommit(
                    MaintenanceActionOutcome::RewriteDataFiles {
                        target_snapshot_id: None,
                        rewritten_data_files_count: 0,
                        added_data_files_count: Some(0),
                        added_delete_files_count: Some(0),
                        rewritten_bytes_count: 0,
                        failed_data_files_count: 0,
                        removed_delete_files_count: 0,
                        output_record_count: Some(0),
                    },
                ))
            }
        }

        let runtime = OptimizeJobRuntime::new();
        runtime
            .start(
                &Handle::current(),
                Arc::new(ReadyAdmission),
                Arc::new(AutomaticExecution),
            )
            .expect("start optimize runtime");
        let effect_id = MaintenanceEffectId::from_bytes([7; 16]);
        let submission = runtime
            .submit_automatic_optimize(target(), &FixedCapture, effect_id)
            .await
            .expect("submit automatic optimize");
        let handle = submission.handle().expect("submitted exact job");
        let terminal = tokio::time::timeout(
            Duration::from_secs(1),
            runtime.wait_for_terminal_record(handle),
        )
        .await
        .expect("automatic job finishes")
        .expect("exact terminal record");
        assert_eq!(terminal.handle(), handle);
        assert_eq!(terminal.effect_id, Some(effect_id));
        assert_eq!(terminal.state, MaintenanceJobState::Finished);
        assert_eq!(terminal.outcome.unwrap().commit_occurred, Some(false));
        runtime
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .expect("join product worker");
    }
}
