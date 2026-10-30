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

use crate::{
    StatisticsAttemptExecutor, StatisticsJob, StatisticsJobAdmission, StatisticsJobCreate,
    StatisticsJobId, StatisticsJobRepository, StatisticsRepositoryError, StatisticsWorker,
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
/// SQL submits work; an admitted statement may await its actual conclusion. The
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

    pub async fn submit_admitted(
        &self,
        request: StatisticsJobCreate,
        admission: StatisticsJobAdmission,
    ) -> Result<StatisticsJob, StatisticsRepositoryError> {
        self.repository.create_admitted(request, admission).await
    }

    fn submit_admitted_now(
        &self,
        request: StatisticsJobCreate,
        admission: StatisticsJobAdmission,
    ) -> Result<StatisticsJob, StatisticsRepositoryError> {
        self.repository.create_now_admitted(request, admission)
    }

    pub async fn wait_for_conclusion(
        &self,
        id: StatisticsJobId,
    ) -> Result<StatisticsJob, StatisticsRepositoryError> {
        self.repository.wait_for_conclusion(id).await
    }

    pub async fn list(&self) -> Result<Vec<StatisticsJob>, StatisticsRepositoryError> {
        self.repository.list().await
    }

    /// Cancel without fabricating a fresh wall-clock observation.
    pub async fn request_cancel_without_time(
        &self,
        job_id: StatisticsJobId,
    ) -> Result<StatisticsJob, StatisticsRepositoryError> {
        self.repository.request_cancel_without_time(job_id).await
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
        let join = runtime.spawn(async move {
            let mut pending = false;
            loop {
                if *stop_rx.borrow() { break; }
                if pending {
                    let work = worker_service.run_one(Arc::clone(&executor), now_ms());
                    tokio::pin!(work);
                    loop {
                        tokio::select! {
                            biased;
                            result = &mut work => { pending = matches!(result, Ok(Some(_))); break; }
                            _ = std::future::poll_fn(|cx|worker_service.repository.poll_exits(cx,now_ms())) => {}
                        }
                    }
                    continue;
                }
                tokio::select! {
                    changed = stop_rx.changed() => { if changed.is_err() || *stop_rx.borrow() { break; } }
                    wake = wake_rx.recv() => { if wake.is_none() { break; } pending = true; }
                    _ = std::future::poll_fn(|cx|worker_service.repository.poll_exits(cx,now_ms())) => {}
                }
            }
            // The shared bank retains unresolved actual sources on process
            // stop. Stopping this serial job selector never fabricates exit.
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

    /// Install the already-paid original without consuming it on refusal.
    /// The caller stores the returned identity before notifying either owner.
    pub fn install_internal_job_in(
        &self,
        request: StatisticsJobCreate,
        source: &mut Option<StatisticsJobAdmission>,
    ) -> Result<(StatisticsJob, crate::StatisticsJobNotification), StatisticsRepositoryError> {
        let _admission = self.admission.lock().map_err(|_| {
            StatisticsRepositoryError::new(
                crate::StatisticsRepositoryErrorKind::Conflict,
                "statistics worker admission lock poisoned",
            )
        })?;
        if *self.stop.borrow() || self.wake.is_closed() {
            return Err(StatisticsRepositoryError::new(
                crate::StatisticsRepositoryErrorKind::Conflict,
                "statistics worker is unavailable",
            ));
        }
        self.service
            .repository
            .install_internal_job_in(request, source)
    }

    /// A stopped worker does not undo an installed job or hide its identity.
    /// The repository retains the originals while cancellation settles.
    pub fn notify_installed_job(
        &self,
        id: StatisticsJobId,
        notice: crate::StatisticsJobNotification,
    ) {
        // The installed original must reach its sole private worker even if
        // an unrelated public observation waker panics below. Both calls are
        // outside the admission/repository locks and the identity is stable.
        if let Err(mpsc::error::TrySendError::Closed(_)) = self.wake.try_send(()) {
            let _ = self.service.request_cancel_now(id, now_ms());
        }
        notice.notify();
    }

    pub async fn submit_admitted(
        &self,
        request: StatisticsJobCreate,
        admission: StatisticsJobAdmission,
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
        let job = self.service.submit_admitted_now(request, admission)?;
        if let Err(mpsc::error::TrySendError::Closed(_)) = self.wake.try_send(()) {
            let _ = self.service.request_cancel_now(job.id, now_ms());
            return Err(StatisticsRepositoryError::new(
                crate::StatisticsRepositoryErrorKind::Conflict,
                "statistics worker is unavailable",
            ));
        }
        Ok(job)
    }

    pub async fn wait_for_conclusion(
        &self,
        id: StatisticsJobId,
    ) -> Result<StatisticsJob, StatisticsRepositoryError> {
        self.service.wait_for_conclusion(id).await
    }

    pub async fn list(&self) -> Result<Vec<StatisticsJob>, StatisticsRepositoryError> {
        self.service.list().await
    }

    /// Cancel without fabricating a fresh wall-clock observation.
    pub async fn request_cancel_without_time(
        &self,
        job_id: StatisticsJobId,
    ) -> Result<StatisticsJob, StatisticsRepositoryError> {
        self.service.request_cancel_without_time(job_id).await
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

#[cfg(test)]
mod notification_tests {
    use super::*;
    use crate::{
        StatisticsAttemptError, StatisticsColumns, StatisticsFailure, StatisticsJobConclusion,
        StatisticsJobOriginals, StatisticsJobProgress, StatisticsJobState, StatisticsTarget,
    };
    use novarocks_resource_contract::SharedDependency;
    use novarocks_workload_control::{
        ControlStorageConfig, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
        joint_admission::{
            JointLane, JointProgress, JointProgressDriver, JointResource as R, JointTryBegin,
            ResponsibilityConfig,
        },
    };
    use std::{
        future::Future,
        num::{NonZeroU64, NonZeroUsize},
        panic::{AssertUnwindSafe, catch_unwind},
        pin::Pin,
        sync::atomic::{AtomicBool, AtomicUsize, Ordering},
        task::{Context, Poll, Wake, Waker},
        time::Duration,
    };

    // The same fixed WC substrate supplies a positive Root payload and one
    // real query permit. This is not a Native/Provider capacity certificate.
    fn original_control() -> (WorkloadControl, JointProgressDriver, u64) {
        let storage = ControlStorageConfig::new(4, &[]);
        let metadata = storage.base_charge().unwrap().get(R::MetadataBytes);
        let control = WorkloadControl::try_new_counted(WorkloadConfig {
            root_limit: 4,
            business_limit: 4,
            scope_records_limit: 4,
            query_concurrency_limit: 1,
            ..WorkloadConfig::default()
        })
        .unwrap()
        .owner;
        let (_, driver) = control
            .configure_responsibility(ResponsibilityConfig {
                work_per_turn: NonZeroUsize::new(7).unwrap(),
            })
            .unwrap();
        control.configure_control_storage(storage).unwrap();
        let mut results = novarocks_workload_control::ResultCapacityConfig::V1;
        results.all_objects_bytes[2] = 1024 * 1024;
        control.configure_result_capacity(results).unwrap();
        control.mark_ready().unwrap();
        (control, driver, metadata)
    }
    fn drive_original(driver: &mut JointProgressDriver) {
        for _ in 0..100000 {
            if matches!(driver.step(), JointProgress::Idle(_)) {
                return;
            }
        }
        panic!("original notification component driver did not become idle");
    }

    struct HeldOriginal {
        started: Mutex<Option<oneshot::Sender<StatisticsJobId>>>,
        release: Mutex<Option<oneshot::Receiver<()>>>,
        exited: Arc<AtomicBool>,
    }
    impl StatisticsAttemptExecutor for HeldOriginal {
        fn execute<'a>(
            &'a self,
            job: StatisticsJob,
            admission: StatisticsJobAdmission,
            progress: StatisticsJobProgress,
        ) -> Pin<Box<dyn Future<Output = Result<(), StatisticsAttemptError>> + Send + 'a>> {
            Box::pin(async move {
                let originals: StatisticsJobOriginals = admission.into_originals();
                assert!(
                    originals
                        .query_concurrency
                        .as_ref()
                        .unwrap()
                        .is_for_scope(&originals.owner.as_ref().unwrap().scope())
                );
                let started = self.started.lock().unwrap().take().unwrap();
                let release = self.release.lock().unwrap().take().unwrap();
                started.send(job.id).unwrap();
                release.await.unwrap();
                // This executor mounted no background/Provider/Native source.
                // Retire its actual originals, not a synthetic exit receipt.
                progress.retire_unmounted(originals, now_ms()).unwrap();
                self.exited.store(true, Ordering::SeqCst);
                progress
                    .finish(
                        Err(StatisticsAttemptError::Failed(StatisticsFailure {
                            compile_control: None,
                            message: Arc::from(
                                "controlled executor finished without external dispatch",
                            ),
                        })),
                        now_ms(),
                    )
                    .unwrap();
                Ok(())
            })
        }
    }
    struct PanicObservationWake(Arc<AtomicUsize>);
    impl Wake for PanicObservationWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
            std::panic::panic_any(0x5a91u32);
        }
    }

    #[tokio::test]
    async fn public_observation_panic_cannot_strand_the_installed_original_worker() {
        let (control, mut driver, metadata) = original_control();
        let root = control
            .begin_warehouse_root(WorkRequest::new(WorkClass::Statistics))
            .unwrap();
        let scope = root.owner.scope();
        let permit = scope.admit_query().unwrap().await.unwrap();
        let domains = scope.prepare_root_domains().unwrap();
        let initial = domains
            .initial_payload_source(&scope, JointLane::Custody)
            .unwrap();
        let JointTryBegin::Started(mut proposal) = initial
            .try_begin(
                NonZeroU64::new(metadata.checked_mul(4).unwrap()).unwrap(),
                NonZeroU64::new(128).unwrap(),
            )
            .unwrap()
        else {
            panic!("original initial payload Q slot");
        };
        proposal
            .push_dependency(&SharedDependency::LocalHost)
            .unwrap();
        let mut pending = proposal.submit().unwrap();
        drive_original(&mut driver);
        let mut payload = None;
        let mut cx = Context::from_waker(Waker::noop());
        let Poll::Ready(Ok(notification)) = pending.poll_install_in(&mut cx, &scope, &mut payload)
        else {
            panic!("actual original payload did not install");
        };
        notification.notify();
        drop(pending);
        drive_original(&mut driver);
        let mut owner = Some(root.owner);
        let mut query_permit = Some(permit);
        let mut admission = None;
        StatisticsJobAdmission::install_internal_in(
            &mut owner,
            &mut query_permit,
            &mut payload,
            &mut admission,
        )
        .unwrap();
        let service = StatisticsJobService::new();
        let (started, started_rx) = oneshot::channel();
        let (release, release_rx) = oneshot::channel();
        let exited = Arc::new(AtomicBool::new(false));
        let runtime = StatisticsJobRuntime::start(
            service.clone(),
            Arc::new(HeldOriginal {
                started: Mutex::new(Some(started)),
                release: Mutex::new(Some(release_rx)),
                exited: Arc::clone(&exited),
            }),
            tokio::runtime::Handle::current(),
        );
        let (job, notice) = runtime
            .install_internal_job_in(
                StatisticsJobCreate {
                    target: StatisticsTarget {
                        catalog: Arc::from("iceberg"),
                        namespace: Arc::from("db"),
                        table: Arc::from("t"),
                        object_id: Arc::from(&b"object"[..]),
                    },
                    columns: StatisticsColumns::All,
                    submitted_at_ms: 1,
                },
                &mut admission,
            )
            .unwrap();
        assert!(admission.is_none(), "the original table accepted custody");
        let changed = Arc::clone(&service.repository.changed);
        let mut observation = Box::pin(changed.notified());
        let calls = Arc::new(AtomicUsize::new(0));
        let malicious = Waker::from(Arc::new(PanicObservationWake(Arc::clone(&calls))));
        assert!(
            observation
                .as_mut()
                .poll(&mut Context::from_waker(&malicious))
                .is_pending()
        );
        let panic = catch_unwind(AssertUnwindSafe(|| {
            runtime.notify_installed_job(job.id, notice)
        }))
        .expect_err("the public observation panic must propagate unchanged");
        assert_eq!(panic.downcast_ref::<u32>(), Some(&0x5a91u32));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        drop(observation);
        drop(panic);

        // No second submit, runtime signal, timer retry, or direct run_one.
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), started_rx)
                .await
                .unwrap()
                .unwrap(),
            job.id
        );
        assert_eq!(control.observation().snapshot().admitted_queries, 1);
        assert!(!exited.load(Ordering::SeqCst));
        release.send(()).unwrap();
        let terminal =
            tokio::time::timeout(Duration::from_secs(1), runtime.wait_for_conclusion(job.id))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(
            terminal.state,
            StatisticsJobState::Terminal(StatisticsJobConclusion::Failed)
        );
        assert!(exited.load(Ordering::SeqCst));
        assert_eq!(control.observation().snapshot().admitted_queries, 0);
        runtime
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        control.close_admission();
        drive_original(&mut driver);
        assert!(
            control.shutdown().is_ok(),
            "same original WC must actually drain"
        );
    }
}
