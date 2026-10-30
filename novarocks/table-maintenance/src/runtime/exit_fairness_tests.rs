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

// Real QA background actions produce the observers; this fixture never
// constructs an exit receipt or sets the actual-exited flag.
use super::*;
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::atomic::AtomicUsize;
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};

use novarocks_query_application::admitted_query_context::{
    QueryExecutionContext, QueryResultCapacityBinding,
};
use novarocks_query_application::api::{
    LogicalExecutionNativePort, LogicalNativeOpenError, LogicalNativeOpenRequest,
    LogicalNativeSession, StatementNativeHealthFailure,
};
use novarocks_query_application::coordination::{
    LogicalExecutionRowsConfig, LogicalExecutionSupervisor, LogicalExecutionSupervisorConfig,
};
use novarocks_query_application::session_control::{
    GovernedActionContext, GovernedBackgroundAction, GovernedStatementResidual,
};
use novarocks_query_application::session_error::QueryServiceError;
use novarocks_resource_contract::SharedDependency;
use novarocks_types::identity::{FrontendProcessId, QueryProcessNamespace};
use novarocks_workload_control::joint_admission::{
    JointAdmissionHandle, JointProgress, ResponsibilityConfig,
};
use novarocks_workload_control::{
    ControlStorageConfig, ResourceConfig, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
};

#[derive(Debug)]
struct NoNative;
impl LogicalExecutionNativePort for NoNative {
    fn bind_in(
        &self,
        _: &mut Option<LogicalNativeOpenRequest>,
        _: &mut Option<LogicalNativeSession>,
    ) -> Result<(), LogicalNativeOpenError> {
        panic!("background action must never open a Native attempt");
    }
}
#[derive(Default)]
struct HeldResidual {
    released: AtomicBool,
    dropped: AtomicUsize,
    wake: Mutex<Option<Waker>>,
}
struct Residual(Arc<HeldResidual>);
impl GovernedStatementResidual for Residual {
    fn poll_retirement(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        let next = cx.waker().clone();
        let old = self.0.wake.lock().unwrap().replace(next);
        drop(old);
        if self.0.released.load(Ordering::Acquire) {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}
impl Drop for Residual {
    fn drop(&mut self) {
        self.0.dropped.fetch_add(1, Ordering::SeqCst);
    }
}
struct Action(Option<Box<dyn GovernedStatementResidual>>);
impl GovernedBackgroundAction for Action {
    fn poll_action(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        _: &mut Context<'_>,
    ) -> Poll<Result<(), QueryServiceError>> {
        assert!(owner.cancellation().is_for_scope(owner.scope()));
        Poll::Ready(Ok(()))
    }
    fn handoff_residual_in(
        &mut self,
        target: &mut Option<Box<dyn GovernedStatementResidual>>,
    ) -> Result<(), StatementNativeHealthFailure> {
        assert!(target.is_none());
        *target = self.0.take();
        Ok(())
    }
}
#[derive(Default)]
struct WakeCount {
    count: AtomicUsize,
    notified: tokio::sync::Notify,
}
impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.count.fetch_add(1, Ordering::SeqCst);
        self.notified.notify_one();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.count.fetch_add(1, Ordering::SeqCst);
        self.notified.notify_one();
    }
}
struct Fixture {
    control: WorkloadControl,
    admission: JointAdmissionHandle,
    supervisor: LogicalExecutionSupervisor,
    client: novarocks_query_application::api::QueryExecutionClient,
    stop: tokio::sync::watch::Sender<bool>,
    progress: tokio::task::JoinHandle<()>,
    runtime: ProcessRuntime<String, (), ()>,
    held: Vec<Arc<HeldResidual>>,
    cells: Vec<OriginalJobExit>,
}
impl Fixture {
    fn new() -> Self {
        let roles = [LogicalExecutionSupervisor::logical_role_poll_geometry().unwrap()];
        let mut control = WorkloadControl::try_new(
            WorkloadConfig {
                root_limit: 256,
                business_limit: 256,
                query_concurrency_limit: 256,
                scope_records_limit: 512,
                ..WorkloadConfig::default()
            },
            ResourceConfig {
                total_bytes: 64 << 20,
                control_bytes: 1 << 20,
                per_scope_bytes: 63 << 20,
            },
        )
        .unwrap();
        control
            .configure_result_capacity(novarocks_workload_control::ResultCapacityConfig::V1)
            .unwrap();
        let (admission, mut driver) = control
            .configure_responsibility(ResponsibilityConfig {
                work_per_turn: NonZeroUsize::new(32).unwrap(),
            })
            .unwrap();
        control
            .configure_control_storage(ControlStorageConfig::new(512, &roles))
            .unwrap();
        let (supervisor, client) = LogicalExecutionSupervisor::try_new(
            &mut control,
            0,
            NonZeroUsize::new(32).unwrap(),
            tokio::runtime::Handle::current(),
            Arc::new(NoNative),
            QueryProcessNamespace::new(0x90),
            FrontendProcessId::new_v7(),
            LogicalExecutionSupervisorConfig::new(
                NonZeroUsize::new(8).unwrap(),
                NonZeroUsize::new(2).unwrap(),
                NonZeroUsize::new(2).unwrap(),
                LogicalExecutionRowsConfig::new(
                    NonZeroUsize::new(2).unwrap(),
                    NonZeroU32::new(2).unwrap(),
                    Duration::from_secs(30),
                ),
            ),
        )
        .unwrap();
        control.mark_ready().unwrap();
        let deadlines = control.deadline_expiry_handle();
        let (stop, mut stopped) = tokio::sync::watch::channel(false);
        let progress = tokio::spawn(async move {
            loop {
                if *stopped.borrow() {
                    break;
                }
                deadlines.expire_deadlines();
                driver.recheck_retired_backing();
                match driver.step() {
                    JointProgress::More => tokio::task::yield_now().await,
                    JointProgress::Idle(marker) => tokio::select! {
                        _ = stopped.changed() => {},
                        _ = deadlines.wait_state_change(marker) => {},
                        _ = tokio::time::sleep(Duration::from_millis(1)) => {},
                    },
                }
            }
        });
        Self {
            control,
            admission,
            supervisor,
            client,
            stop,
            progress,
            runtime: ProcessRuntime::new(),
            held: Vec::new(),
            cells: Vec::new(),
        }
    }
    async fn add(&mut self, ordinal: usize) {
        let root = self
            .control
            .try_begin_root(WorkRequest::new(WorkClass::TableMaintenance))
            .unwrap();
        let mut proposal = self.admission.begin_paid_proposal(&root.owner).unwrap();
        proposal
            .begin_first_bind(root.owner.control_claim_view().unwrap())
            .unwrap();
        proposal
            .push_dependency(&SharedDependency::LocalHost)
            .unwrap();
        proposal.end_change().unwrap();
        let batch = proposal.submit().unwrap().await.unwrap();
        assert!(batch.new_claims.is_empty());
        batch
            .adjustment
            .unwrap()
            .install_control(&root.owner, &[])
            .unwrap();
        let scope = root.owner.scope();
        let binding = QueryResultCapacityBinding::from_root_owner(&root.owner).unwrap();
        let mut context = Some(
            QueryExecutionContext::new(
                novarocks_types::ClusterRole::Fe,
                novarocks_query_application::api::BackendTopologySnapshot::try_new(1, Vec::new())
                    .unwrap(),
                None,
                novarocks_query_application::cancellation::QueryCancellationView::governed(
                    scope.cancellation().unwrap(),
                    None,
                ),
                novarocks_sql::compiler::SessionOptimizerSettings::default(),
                novarocks_sql::sql_mode::SqlSemanticSettings::default(),
            )
            .with_result_capacity(binding)
            .unwrap(),
        );
        let mut permit = Some(scope.admit_query().unwrap().await.unwrap());
        let signals = Arc::new(HeldResidual::default());
        let mut action: Option<Box<dyn GovernedBackgroundAction>> =
            Some(Box::new(Action(Some(Box::new(Residual(signals.clone()))))));
        let mut owner = Some(root.owner);
        let mut input = None;
        let mut payload = None;
        self.client
            .prepare_background_action_in(
                &owner,
                &mut context,
                &mut permit,
                &mut payload,
                &mut action,
                &mut input,
            )
            .unwrap();
        let mut ready = None;
        let mut exit = None;
        let tail = self
            .client
            .start_background_action_in(&mut input, &mut owner, &mut ready, &mut exit)
            .unwrap();
        assert!(tail.result().is_ok());
        let job = self
            .runtime
            .submit(
                JobCreate {
                    target: format!("orders-{ordinal}"),
                    effect_id: None,
                    object_id: vec![1],
                    base_snapshot_id: 1,
                    created_at_ms: 1,
                },
                (),
            )
            .await
            .unwrap();
        assert_eq!(
            self.runtime.claim_next(2).await.unwrap().unwrap().job_id,
            job.job_id
        );
        self.runtime
            .install_original_exit_in(job.job_id, &mut exit)
            .unwrap();
        assert!(exit.is_none());
        self.cells.push(
            self.runtime.lock().unwrap().active[&job.job_id]
                .custody
                .original_exit
                .as_ref()
                .unwrap()
                .clone(),
        );
        tail.finish().unwrap();
        let receipt = ready.take().unwrap().await.unwrap();
        drop(receipt);
        root.business.release();
        self.runtime.finish(job.job_id, Ok(()), 3).await.unwrap();
        self.held.push(signals);
    }
    fn release_one(&self, index: usize) {
        let signal = &self.held[index];
        signal.released.store(true, Ordering::Release);
        let wake = signal.wake.lock().unwrap().take();
        if let Some(wake) = wake {
            wake.wake();
        }
    }
    fn release(&self) {
        for index in 0..self.held.len() {
            self.release_one(index);
        }
    }
    async fn finish(mut self) {
        self.release();
        std::future::poll_fn(|cx| {
            let _ = self.runtime.poll_exits(cx);
            if self.runtime.lock().unwrap().exit_headers.is_empty() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
        assert!(self.cells.iter().all(OriginalJobExit::actual_exit_observed));
        assert!(
            self.held
                .iter()
                .all(|s| s.dropped.load(Ordering::SeqCst) == 1)
        );
        {
            let mut state = self.runtime.lock().unwrap();
            ProcessRuntime::<String, (), ()>::prune_locked(&mut state, now_ms());
            assert!(state.terminal.is_empty());
        }
        drop(self.cells);
        drop(self.held);
        self.supervisor
            .shutdown_until(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
        drop(self.client);
        drop(self.supervisor);
        self.control.close_admission();
        loop {
            let revision = self.control.progress_revision();
            if matches!(
                self.control.wait_progress(revision).await,
                novarocks_workload_control::WorkloadProgress::Drained(_)
            ) {
                break;
            }
        }
        self.stop.send_replace(true);
        self.progress.await.unwrap();
        assert!(self.control.shutdown().is_ok());
    }
}

#[tokio::test]
async fn pending_original_observers_are_once_per_round_and_leave_each_product_turn_bounded() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let mut fixture = Fixture::new();
        let count = EXIT_OBSERVATIONS_PER_TURN * 2 + 1;
        for ordinal in 0..count {
            fixture.add(ordinal).await;
        }
        let wake = Arc::new(WakeCount::default());
        let waker = Waker::from(wake.clone());
        let mut cx = Context::from_waker(&waker);
        let mut product_polls = 0;
        for turn in 0..3 {
            let before: usize = fixture
                .cells
                .iter()
                .map(|c| c.polls.load(Ordering::Relaxed))
                .sum();
            assert!(fixture.runtime.poll_exits(&mut cx).is_pending());
            // Same ordering as run_worker: exits are polled immediately
            // before the current Product future gets this turn.
            {
                let mut product = std::pin::pin!(std::future::poll_fn(|_| {
                    product_polls += 1;
                    Poll::<()>::Pending
                }));
                assert!(std::future::Future::poll(product.as_mut(), &mut cx).is_pending());
            }
            let after: usize = fixture
                .cells
                .iter()
                .map(|c| c.polls.load(Ordering::Relaxed))
                .sum();
            assert!(after - before <= EXIT_OBSERVATIONS_PER_TURN);
            assert_eq!(product_polls, turn + 1);
        }
        assert!(
            fixture
                .cells
                .iter()
                .all(|c| c.polls.load(Ordering::Relaxed) == 1)
        );
        assert_eq!(
            wake.count.load(Ordering::SeqCst),
            2,
            "a completed pending round must not self-wake"
        );
        assert_eq!(fixture.runtime.lock().unwrap().exit_headers.len(), count);
        fixture.finish().await;
    })
    .await
    .expect("real observer fairness fixture did not exit");
}

#[tokio::test]
async fn terminal_history_and_header_retire_only_after_the_original_actual_exit_receipt() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut fixture = Fixture::new();
        fixture.add(0).await;
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        assert!(fixture.runtime.poll_exits(&mut cx).is_pending());
        {
            let mut state = fixture.runtime.lock().unwrap();
            ProcessRuntime::<String, (), ()>::prune_locked(&mut state, now_ms());
            assert_eq!(
                state.terminal.len(),
                1,
                "old history cannot evict its real held tail"
            );
            assert_eq!(state.exit_headers.len(), 1);
        }
        assert!(!fixture.cells[0].actual_exit_observed());
        assert_eq!(fixture.held[0].dropped.load(Ordering::SeqCst), 0);
        fixture.finish().await;
    })
    .await
    .expect("real held tail did not actually exit");
}

#[tokio::test]
async fn an_early_original_exit_wake_survives_later_batches_and_repolls_without_another_event() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let mut fixture = Fixture::new();
        let count = EXIT_OBSERVATIONS_PER_TURN * 2 + 1;
        for ordinal in 0..count {
            fixture.add(ordinal).await;
        }
        let wake = Arc::new(WakeCount::default());
        let waker = Waker::from(wake.clone());
        let mut cx = Context::from_waker(&waker);
        assert!(fixture.runtime.poll_exits(&mut cx).is_pending());
        assert_eq!(fixture.cells[0].polls.load(Ordering::Relaxed), 1);
        assert_eq!(wake.count.load(Ordering::SeqCst), 1);
        // Consume the first batch's cooperative notice, then release only the
        // already-visited first real residual. Its QA footer wakes the same
        // original observer; no second release or fabricated Ready is used.
        wake.notified.notified().await;
        fixture.release_one(0);
        wake.notified.notified().await;
        loop {
            // Re-arm only the wake forwarding edge, never poll or consume the
            // original observer. Wait for its real bank footer before running
            // later batches, making the consumed-mid-round notification exact.
            fixture.runtime.exit_wake.register(&waker);
            let returned = {
                let body = fixture.cells[0].original.lock().unwrap();
                let view = body.observer.as_ref().unwrap().view().unwrap();
                view.role_result().is_some() && view.native_preparation_exited()
            };
            if returned {
                break;
            }
            wake.notified.notified().await;
        }
        assert!(fixture.runtime.exit_wake.dirty.load(Ordering::Acquire));
        assert_eq!(fixture.held[0].dropped.load(Ordering::SeqCst), 1);
        assert!(!fixture.cells[0].actual_exit_observed());
        // This executor-driven future starts with batch 2. Batch 3 must
        // schedule the re-observation of id1 itself; the test never manually
        // supplies a fourth poll and all other actual tails stay held.
        let mut turns = 0;
        std::future::poll_fn(|cx| {
            turns += 1;
            let _ = fixture.runtime.poll_exits(cx);
            if fixture.cells[0].actual_exit_observed() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
        assert_eq!(turns, 3);
        assert_eq!(fixture.cells[0].polls.load(Ordering::Relaxed), 2);
        assert!(
            fixture.cells[1..]
                .iter()
                .all(|cell| !cell.actual_exit_observed())
        );
        assert!(
            fixture.held[1..]
                .iter()
                .all(|signal| signal.dropped.load(Ordering::SeqCst) == 0)
        );
        assert_eq!(
            fixture.runtime.lock().unwrap().exit_headers.len(),
            count - 1
        );
        fixture.finish().await;
    })
    .await
    .expect("mid-round actual exit wake was lost");
}

#[tokio::test]
async fn a_new_original_observer_installed_midround_is_polled_without_an_unrelated_release() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let mut fixture = Fixture::new();
        let old_count = EXIT_OBSERVATIONS_PER_TURN * 2 + 1;
        for ordinal in 0..old_count {
            fixture.add(ordinal).await;
        }
        let wake = Arc::new(WakeCount::default());
        let waker = Waker::from(wake.clone());
        let mut cx = Context::from_waker(&waker);
        assert!(fixture.runtime.poll_exits(&mut cx).is_pending());
        assert_eq!(wake.count.load(Ordering::SeqCst), 1);
        wake.notified.notified().await; // Consume the first cooperative notice.
        // Arm the original repository notification, as the worker does around
        // receive. The new job's ordinary change can wake the next old batch,
        // but cannot substitute for making its new exit header dirty.
        let runtime = fixture.runtime.clone();
        {
            let mut changed = std::pin::pin!(runtime.change_notification());
            assert!(std::future::Future::poll(changed.as_mut(), &mut cx).is_pending());
            fixture.add(old_count).await;
        }
        let new_cell = &fixture.cells[old_count];
        assert_eq!(new_cell.polls.load(Ordering::Relaxed), 0);
        assert!(fixture.runtime.exit_wake.dirty.load(Ordering::Acquire));
        // Only the new actual QA residual is released. It has no Runtime exit
        // waker yet; the earlier 65 originals stay held throughout the proof.
        fixture.release_one(old_count);
        let mut turns = 0;
        loop {
            // Every turn requires a real repository/insertion/observer notice
            // or the preceding batch's cooperative wake. There is no manually
            // supplied fourth poll and no unrelated residual release.
            wake.notified.notified().await;
            turns += 1;
            let _ = fixture.runtime.poll_exits(&mut cx);
            if new_cell.actual_exit_observed() {
                break;
            }
        }
        assert!(
            turns >= 5,
            "finish the old round before visiting the new high watermark"
        );
        assert!(new_cell.polls.load(Ordering::Relaxed) >= 1);
        assert_eq!(fixture.held[old_count].dropped.load(Ordering::SeqCst), 1);
        assert!(
            fixture.cells[..old_count]
                .iter()
                .all(|cell| !cell.actual_exit_observed())
        );
        assert!(
            fixture.held[..old_count]
                .iter()
                .all(|signal| signal.dropped.load(Ordering::SeqCst) == 0)
        );
        assert_eq!(
            fixture.runtime.lock().unwrap().exit_headers.len(),
            old_count
        );
        fixture.finish().await;
    })
    .await
    .expect("mid-round original observer insertion wake was lost");
}
