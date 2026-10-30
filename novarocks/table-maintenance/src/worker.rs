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
    /// Move this exact admission into the shared background Source once.
    fn take_originals(&mut self) -> Result<OptimizeJobOriginals, String>;
    fn original_owner(&self) -> Result<&novarocks_workload_control::WorkOwner, String>;
    fn is_cancelled(&self) -> Result<bool, String>;
}

/// Original current-process job slots. A UI waiter never owns these values.
pub struct OptimizeJobOriginals {
    pub owner: Option<novarocks_workload_control::WorkOwner>,
    pub query_concurrency: Option<novarocks_workload_control::QueryConcurrencyPermit>,
    #[cfg(test)]
    pub result_capacity:
        Option<novarocks_query_application::admitted_query_context::QueryResultCapacityBinding>,
    scope: novarocks_workload_control::WorkScope,
}
impl OptimizeJobOriginals {
    #[cfg(test)]
    pub fn new(
        owner: novarocks_workload_control::WorkOwner,
        query_concurrency: novarocks_workload_control::QueryConcurrencyPermit,
        result_capacity: novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
    ) -> Result<Self, String> {
        let scope = owner.scope();
        if !query_concurrency.is_for_scope(&scope)
            || !result_capacity.window_alias().is_for_scope(&scope)
        {
            return Err("optimize originals belong to different Root scopes".into());
        }
        Ok(Self {
            owner: Some(owner),
            query_concurrency: Some(query_concurrency),
            result_capacity: Some(result_capacity),
            scope,
        })
    }
    /// Every scope/permit check precedes the move from the caller's slots.
    pub fn take_in(
        owner: &mut Option<novarocks_workload_control::WorkOwner>,
        permit: &mut Option<novarocks_workload_control::QueryConcurrencyPermit>,
    ) -> Result<Self, String> {
        let scope = owner
            .as_ref()
            .ok_or("Optimize original Root is empty")?
            .scope();
        if !permit.as_ref().is_some_and(|p| p.is_for_scope(&scope)) {
            return Err("Optimize original query permit has a foreign Root".into());
        }
        scope.check().map_err(|e| e.to_string())?;
        Ok(Self {
            owner: owner.take(),
            query_concurrency: permit.take(),
            scope,
            #[cfg(test)]
            result_capacity: None,
        })
    }
    pub fn take_prepared_root_in(
        owner: &mut Option<novarocks_workload_control::WorkOwner>,
    ) -> Result<Self, String> {
        let scope = owner
            .as_ref()
            .ok_or("Optimize prepared Root is empty")?
            .scope();
        scope.check().map_err(|e| e.to_string())?;
        Ok(Self {
            owner: owner.take(),
            query_concurrency: None,
            scope,
            #[cfg(test)]
            result_capacity: None,
        })
    }
    pub fn scope(&self) -> &novarocks_workload_control::WorkScope {
        &self.scope
    }
}
impl Drop for OptimizeJobOriginals {
    fn drop(&mut self) {
        // Before mounting only pure source selection ran. After mounting these
        // owner/permit slots are empty and the common Source owns actual exit.
        #[cfg(test)]
        drop(self.result_capacity.take());
        drop(self.query_concurrency.take());
        if let Some(owner) = self.owner.take() {
            owner.complete();
        }
    }
}

/// A prepared record transfers its original Root rather than acquiring a
/// second query permit. This wrapper is a single-use receiving cell, not an
/// alternative admission path or an owner reconstructed from a result alias.
struct ReceivedOriginalScope {
    original: Option<OptimizeJobOriginals>,
}
impl OptimizeJobScope for ReceivedOriginalScope {
    fn take_originals(&mut self) -> Result<OptimizeJobOriginals, String> {
        self.original
            .take()
            .ok_or_else(|| "Optimize original already mounted".into())
    }
    fn original_owner(&self) -> Result<&novarocks_workload_control::WorkOwner, String> {
        self.original
            .as_ref()
            .and_then(|o| o.owner.as_ref())
            .ok_or_else(|| "Optimize original Root is empty".into())
    }
    fn is_cancelled(&self) -> Result<bool, String> {
        self.original
            .as_ref()
            .ok_or("Optimize original is empty")?
            .scope()
            .cancellation()
            .map(|c| c.reason().is_some())
            .map_err(|e| e.to_string())
    }
}

/// Result of asking the host for one Table Maintenance root scope.
pub enum OptimizeJobAdmission {
    Acquired(Box<dyn OptimizeJobScope>),
    RetryLater,
    Closed,
}

/// Host adapter for role-local admission and resource attribution.
pub type OptimizeJobAdmissionFuture<'a> = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<OptimizeJobAdmission, String>> + Send + 'a>,
>;
pub trait OptimizeJobAdmissionPort: Send + Sync {
    fn begin(&self) -> OptimizeJobAdmissionFuture<'_>;
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
    fn acquire(
        &self,
        owner: &novarocks_workload_control::WorkOwner,
    ) -> Result<Option<Box<dyn OptimizeJobExecution>>, String>;
}

/// One exact provider/native execution lease for a claimed OPTIMIZE job.
pub trait OptimizeJobExecution: Send + Sync + 'static {
    #[cfg(not(test))]
    fn run_original_job<'a>(
        self: Box<Self>,
        jobs: Arc<OptimizeProcessRuntime>,
        originals: OptimizeJobOriginals,
        job: OptimizeJob,
    ) -> crate::product::MaintenanceFuture<'a, Result<crate::OptimizeJobOutcome, TerminalError>>;

    #[cfg(test)]
    fn run_original_job<'a>(
        self: Box<Self>,
        jobs: Arc<OptimizeProcessRuntime>,
        originals: OptimizeJobOriginals,
        job: OptimizeJob,
    ) -> crate::product::MaintenanceFuture<'a, Result<crate::OptimizeJobOutcome, TerminalError>>
    {
        Box::pin(async move {
            let capacity = originals
                .result_capacity
                .as_ref()
                .expect("original test capacity")
                .clone();
            execute_claimed_job(
                jobs.as_ref(),
                self.as_ref(),
                originals.scope(),
                capacity,
                job,
            )
            .await
        })
    }

    /// Exact original publication/no-op author; legacy fixtures may have no
    /// such fact. Never infer an effect from a successful job state.
    fn original_effect<'a>(
        &'a self,
    ) -> crate::product::MaintenanceFuture<
        'a,
        Option<novarocks_coordination_contract::ExecutionEffect>,
    > {
        Box::pin(async move { None })
    }

    fn rebind_target<'a>(
        &'a self,
        job: &'a OptimizeJob,
    ) -> crate::product::MaintenanceFuture<'a, Result<MaintenanceTargetRebind, String>>;

    fn execute<'a>(
        &'a self,
        job: &'a OptimizeJob,
        capacity: &'a novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
    ) -> crate::product::MaintenanceFuture<'a, Result<crate::MaintenanceActionOutcome, TerminalError>>;

    /// Executes an MV-owned OPTIMIZE using the identity frozen before job
    /// submission. An adapter without this capability fails before dispatch.
    fn execute_automatic<'a>(
        &'a self,
        _job: &'a OptimizeJob,
        _effect_id: MaintenanceEffectId,
        _capacity: &'a novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
    ) -> crate::product::MaintenanceFuture<'a, Result<AutomaticMaintenanceOutcome, TerminalError>>
    {
        Box::pin(async move {
            Err(TerminalError::pre_dispatch_failed(
                "automatic optimize effect identity is unsupported",
            ))
        })
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

    pub(crate) fn wakeup_handle(&self) -> Arc<Notify> {
        Arc::clone(&self.wakeup)
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
    let mut waiting = OriginalAdmissionWait::default();
    loop {
        if stop.load(Ordering::Acquire) {
            cancel_for_shutdown(jobs.as_ref()).await?;
            return Ok(());
        }
        if !execution.is_available() {
            cancel_for_shutdown(jobs.as_ref()).await?;
            return Ok(());
        }
        let Some((job, mut scope)) = receive_original_or_admit(
            jobs.as_ref(),
            admission.clone(),
            &mut waiting,
            &stop,
            &wakeup,
        )
        .await?
        else {
            if stop.load(Ordering::Acquire) {
                cancel_for_shutdown(jobs.as_ref()).await?;
                return Ok(());
            }
            continue;
        };
        let job_id = job.job_id;
        let acquisition = match scope.is_cancelled() {
            Ok(true) => {
                // Publish only after the original root/window scope is gone.
                drop(scope);
                finish(
                    jobs.as_ref(),
                    job_id,
                    Err(TerminalError::cancelled_before_dispatch(
                        "optimize job cancelled before execution binding",
                    )),
                )
                .await?;
                continue;
            }
            Ok(false) => scope
                .original_owner()
                .and_then(|owner| execution.acquire(owner)),
            Err(error) => Err(error),
        };
        let execution = match acquisition {
            Ok(Some(execution)) => execution,
            Ok(None) => {
                drop(scope);
                finish(
                    jobs.as_ref(),
                    job_id,
                    Err(TerminalError::pre_dispatch_failed(
                        "optimize execution owner disappeared before provider dispatch",
                    )),
                )
                .await?;
                cancel_for_shutdown(jobs.as_ref()).await?;
                return Ok(());
            }
            Err(error) => {
                drop(scope);
                finish(
                    jobs.as_ref(),
                    job_id,
                    Err(TerminalError::pre_dispatch_failed(format!(
                        "bind optimize execution before target rebind failed: {error}"
                    ))),
                )
                .await?;
                continue;
            }
        };
        // No side effect ran yet. Move the exact original admission before
        // the frontend mounts its Product/Provider Action in the common host.
        let terminal = match scope.take_originals() {
            Ok(originals) => {
                let current = execution.run_original_job(Arc::clone(&jobs), originals, job);
                tokio::pin!(current);
                std::future::poll_fn(|cx| {
                    // The old independent job keeps its exact FIFO ticket
                    // while this already-admitted job executes serially.
                    let _ = waiting.poll(cx);
                    // A negative exit observation never drops or cancels the
                    // currently retained Product F on observer failure.
                    let _ = jobs.poll_exits(cx);
                    std::future::Future::poll(current.as_mut(), cx)
                })
                .await
            }
            Err(error) => Err(TerminalError::pre_dispatch_failed(error)),
        };
        // Design: ADR-0169 (docs/adr/ADR-0169-read-only-hms-and-single-writer-admission.md)
        // Business Ready came from the common Source. Its original residual
        // independently owns actual Native/Provider exit after this job terminal.
        drop(scope);
        finish(jobs.as_ref(), job_id, terminal).await?;
    }
}

type AdmissionFuture = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<OptimizeJobAdmission, String>> + Send>,
>;
#[derive(Default)]
struct OriginalAdmissionWait {
    future: Option<AdmissionFuture>,
    // The affirmative receive precedes original F Drop. It is not inferred
    // from an absent future, a ready event or the query driver's grant state.
    received: Option<Result<OptimizeJobAdmission, String>>,
}
impl OriginalAdmissionWait {
    fn start(&mut self, admission: Arc<dyn OptimizeJobAdmissionPort>) {
        assert!(
            self.future.is_none() && self.received.is_none(),
            "one original Optimize waiting admission"
        );
        self.future = Some(Box::pin(wait_original_admission(admission)));
    }
    fn poll(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<()> {
        if self.received.is_some() {
            return std::task::Poll::Ready(());
        }
        let Some(future) = self.future.as_mut() else {
            return std::task::Poll::Pending;
        };
        let result = std::task::ready!(future.as_mut().poll(cx));
        self.received = Some(result);
        drop(self.future.take());
        std::task::Poll::Ready(())
    }
}
async fn wait_original_admission(
    admission: Arc<dyn OptimizeJobAdmissionPort>,
) -> Result<OptimizeJobAdmission, String> {
    admission.begin().await
}
/// A prepared record uses its original permit; the independent job's one
/// pending ticket stays in this original slot across serial prepared jobs.
async fn receive_original_or_admit(
    jobs: &OptimizeProcessRuntime,
    admission: Arc<dyn OptimizeJobAdmissionPort>,
    waiting: &mut OriginalAdmissionWait,
    stop: &AtomicBool,
    wakeup: &Notify,
) -> Result<Option<(OptimizeJob, Box<dyn OptimizeJobScope>)>, String> {
    loop {
        let changed = jobs.change_notification();
        tokio::pin!(changed);
        changed.as_mut().enable();
        if stop.load(Ordering::Acquire) {
            return Ok(None);
        }
        // Establish the oldest independent job's ONE FIFO ticket before a
        // prepared record can be selected. Continuous prepared arrivals must
        // not prevent that original ticket from ever entering the warehouse.
        if waiting.future.is_none()
            && waiting.received.is_none()
            && jobs.has_worker_pending().map_err(|e| e.to_string())?
        {
            waiting.start(admission.clone());
        }
        // Poll/receive an already established ticket before considering the
        // queue. New arrivals never cancel it, replace it, or mint another Q.
        std::future::poll_fn(|cx| {
            let _ = waiting.poll(cx);
            std::task::Poll::Ready(())
        })
        .await;
        let mut retry = false;
        // A real received grant for the oldest independent job wins before
        // later prepared records. Pending tickets keep their FIFO position;
        // receiving that grant is the bounded opportunity to execute it.
        if let Some(received) = waiting.received.take() {
            match received? {
                OptimizeJobAdmission::Acquired(scope) => {
                    if let Some(job) = jobs
                        .claim_next(now_unix_millis())
                        .await
                        .map_err(|e| e.to_string())?
                    {
                        return Ok(Some((job, scope)));
                    }
                    drop(scope);
                }
                OptimizeJobAdmission::RetryLater => retry = true,
                OptimizeJobAdmission::Closed => {
                    stop.store(true, Ordering::Release);
                    return Ok(None);
                }
            }
        }
        if let Some(mut original) = jobs
            .claim_prepared(now_unix_millis())
            .await
            .map_err(|e| e.to_string())?
        {
            return Ok(Some((
                original.job,
                Box::new(ReceivedOriginalScope {
                    original: original.originals.take(),
                }),
            )));
        }
        if retry {
            tokio::select! {
                _ = wakeup.notified() => {}, _ = &mut changed => {},
                _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {},
            }
            continue;
        }
        let pending = waiting.future.is_some();
        tokio::select! {
            result=std::future::poll_fn(|cx|jobs.poll_exits(cx))=>{result.map_err(|e|e.to_string())?;},
            _=std::future::poll_fn(|cx|waiting.poll(cx)), if pending=>{},
            _=&mut changed=>{},
            _=wakeup.notified()=>{},
        }
    }
}

async fn cancel_for_shutdown(jobs: &OptimizeProcessRuntime) -> Result<(), String> {
    jobs.request_shutdown_cancellation()
        .await
        .map_err(|error| format!("request optimize shutdown cancellation failed: {error}"))
}

pub async fn execute_claimed_job<E: OptimizeJobExecution + ?Sized>(
    jobs: &OptimizeProcessRuntime,
    execution: &E,
    scope: &novarocks_workload_control::WorkScope,
    capacity: novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
    job: OptimizeJob,
) -> Result<crate::OptimizeJobOutcome, TerminalError> {
    let job_id = job.job_id;
    let initially_cancelled = match scope
        .cancellation()
        .map(|view| view.reason().is_some())
        .map_err(|error| error.to_string())
    {
        Ok(cancelled) => cancelled,
        Err(error) => {
            return Err(TerminalError::pre_dispatch_failed(format!(
                "read optimize cancellation before target rebind failed: {error}"
            )));
        }
    };
    if initially_cancelled {
        return Err(TerminalError::cancelled_before_dispatch(
            "optimize job cancelled before target rebind",
        ));
    }
    let terminal = match execution.rebind_target(&job).await {
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
        return terminal;
    }
    let scope_cancelled = match scope
        .cancellation()
        .map(|view| view.reason().is_some())
        .map_err(|error| error.to_string())
    {
        Ok(cancelled) => cancelled,
        Err(error) => {
            return Err(TerminalError::pre_dispatch_failed(format!(
                "read optimize cancellation before provider dispatch failed: {error}"
            )));
        }
    };
    let job_cancelled = match jobs.cancellation_requested(job_id).await {
        Ok(cancelled) => cancelled,
        Err(error) => {
            return Err(TerminalError::pre_dispatch_failed(format!(
                "read optimize job cancellation before provider dispatch failed: {error}"
            )));
        }
    };
    if scope_cancelled || job_cancelled {
        return Err(TerminalError::cancelled_before_dispatch(
            "optimize job cancelled before provider dispatch",
        ));
    }
    let executed = match job.effect_id {
        Some(effect_id) => execution
            .execute_automatic(&job, effect_id, &capacity)
            .await
            .map(|outcome| {
                let (action, committed) = match outcome {
                    AutomaticMaintenanceOutcome::KnownCommitted(action) => (action, true),
                    AutomaticMaintenanceOutcome::NoOpWithoutCommit(action) => (action, false),
                };
                (action, Some(committed))
            }),
        None => match execution.execute(&job, &capacity).await {
            Ok(action) => Ok((
                action,
                execution.original_effect().await.map(|effect| {
                    effect == novarocks_coordination_contract::ExecutionEffect::External
                }),
            )),
            Err(error) => Err(error),
        },
    };
    match executed {
        Ok((outcome, committed)) => optimize_job_outcome_from_action(outcome)
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
        Err(terminal) => Err(terminal),
    }
}

async fn finish(
    jobs: &OptimizeProcessRuntime,
    job_id: i64,
    terminal: Result<crate::OptimizeJobOutcome, TerminalError>,
) -> Result<(), String> {
    let effect = terminal
        .as_ref()
        .ok()
        .and_then(|o| o.commit_occurred)
        .map(|applied| {
            if applied {
                novarocks_coordination_contract::ExecutionEffect::External
            } else {
                novarocks_coordination_contract::ExecutionEffect::None
            }
        });
    jobs.finish_with_effect(job_id, terminal, effect, now_unix_millis())
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
pub(crate) mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use super::*;
    use crate::activity::{MaintenanceActivityFamily, TableMaintenanceActivity};
    use crate::runtime::JobCreate;
    use crate::{MaintenanceActionOutcome, MaintenanceTarget};

    struct TestScope {
        cancelled: bool,
        root: Option<novarocks_workload_control::WorkOwner>,
        permit: Option<novarocks_workload_control::QueryConcurrencyPermit>,
        capacity: novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
    }
    impl Drop for TestScope {
        fn drop(&mut self) {
            drop(self.permit.take());
            if let Some(root) = self.root.take() {
                root.complete();
            }
        }
    }

    impl OptimizeJobScope for TestScope {
        fn take_originals(&mut self) -> Result<OptimizeJobOriginals, String> {
            OptimizeJobOriginals::new(
                self.root.take().ok_or("test Root already moved")?,
                self.permit.take().ok_or("test permit already moved")?,
                self.capacity.clone(),
            )
        }
        fn original_owner(&self) -> Result<&novarocks_workload_control::WorkOwner, String> {
            self.root
                .as_ref()
                .ok_or_else(|| "fixture Root is empty".into())
        }
        fn is_cancelled(&self) -> Result<bool, String> {
            Ok(self.cancelled)
        }
    }

    pub(crate) async fn admitted_scope_fixture() -> Box<dyn OptimizeJobScope> {
        use novarocks_workload_control::{
            ResourceConfig, ResultCapacityConfig, ResultWindowClass, WorkClass, WorkRequest,
            WorkloadConfig, WorkloadControl,
        };
        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024 * 1024,
                control_bytes: 1024,
                per_scope_bytes: 1024 * 1024 - 1024,
            },
        )
        .unwrap();
        control
            .configure_result_capacity(ResultCapacityConfig::V1)
            .unwrap();
        control.mark_ready().unwrap();
        let root = control
            .root_admission()
            .begin_warehouse_root(WorkRequest::new(WorkClass::TableMaintenance))
            .unwrap();
        let (permit, window) = root
            .owner
            .scope()
            .admit_query_with_result(ResultWindowClass::Internal)
            .unwrap()
            .await
            .unwrap();
        let capacity = novarocks_query_application::admitted_query_context::QueryResultCapacityBinding::try_new(&root.owner.scope(), window.retain_alias()).unwrap();
        Box::new(TestScope {
            cancelled: false,
            root: Some(root.owner),
            permit: Some(permit),
            capacity,
        })
    }

    struct TestAdmission {
        ready: Arc<AtomicBool>,
        closed: Arc<AtomicBool>,
    }

    impl OptimizeJobAdmissionPort for TestAdmission {
        fn begin(&self) -> crate::worker::OptimizeJobAdmissionFuture<'_> {
            Box::pin(async move {
                if self.closed.load(Ordering::Acquire) {
                    return Ok(OptimizeJobAdmission::Closed);
                }
                if !self.ready.load(Ordering::Acquire) {
                    return Ok(OptimizeJobAdmission::RetryLater);
                }
                Ok(OptimizeJobAdmission::Acquired(
                    admitted_scope_fixture().await,
                ))
            })
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

        fn acquire(
            &self,
            owner: &novarocks_workload_control::WorkOwner,
        ) -> Result<Option<Box<dyn OptimizeJobExecution>>, String> {
            owner.scope().check().map_err(|error| error.to_string())?;
            Ok(Some(Box::new(self.clone())))
        }
    }

    impl OptimizeJobExecution for RecordingExecution {
        fn rebind_target<'a>(
            &'a self,
            _job: &'a OptimizeJob,
        ) -> crate::product::MaintenanceFuture<'a, Result<MaintenanceTargetRebind, String>>
        {
            Box::pin(async move {
                self.calls.lock().expect("calls lock").push("rebind");
                Ok(self.rebind)
            })
        }

        fn execute<'a>(
            &'a self,
            _job: &'a OptimizeJob,
            capacity: &'a novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
        ) -> crate::product::MaintenanceFuture<'a, Result<MaintenanceActionOutcome, TerminalError>>
        {
            Box::pin(async move {
                assert_eq!(
                    capacity.class(),
                    novarocks_workload_control::ResultWindowClass::Internal
                );
                capacity.scope().check().unwrap();
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
            })
        }
    }

    async fn submit_job(jobs: &OptimizeProcessRuntime) -> i64 {
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
    async fn original_scope_binding_failure_finishes_claim_without_target_rebind() {
        struct RefusedExecution;
        impl OptimizeJobExecutionPort for RefusedExecution {
            fn is_available(&self) -> bool {
                true
            }
            fn acquire(
                &self,
                owner: &novarocks_workload_control::WorkOwner,
            ) -> Result<Option<Box<dyn OptimizeJobExecution>>, String> {
                owner.scope().check().unwrap();
                Err("original attempt binding refused".to_string())
            }
        }
        let jobs = Arc::new(OptimizeProcessRuntime::new());
        let job_id = submit_job(jobs.as_ref()).await;
        let mut worker = OptimizeWorker::start(
            &Handle::current(),
            Arc::clone(&jobs),
            Arc::new(TestAdmission {
                ready: Arc::new(AtomicBool::new(true)),
                closed: Arc::new(AtomicBool::new(false)),
            }),
            Arc::new(RefusedExecution),
        );
        let terminal = jobs
            .wait_for_completion(job_id)
            .await
            .expect("binding refusal is terminal");
        assert_eq!(
            terminal.state,
            crate::runtime::MaintenanceJobState::PreDispatchFailed
        );
        worker
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .expect("worker remains stoppable");
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

    #[tokio::test]
    async fn terminal_publication_follows_actual_scope_release_on_all_execution_paths() {
        struct ReleaseScope {
            jobs: Arc<OptimizeProcessRuntime>,
            job_id: i64,
            released: Arc<AtomicBool>,
            cancelled: bool,
            admitted: Option<Box<dyn OptimizeJobScope>>,
        }
        impl OptimizeJobScope for ReleaseScope {
            fn take_originals(&mut self) -> Result<OptimizeJobOriginals, String> {
                self.admitted
                    .as_mut()
                    .ok_or("release fixture original Root already moved")?
                    .take_originals()
            }
            fn original_owner(&self) -> Result<&novarocks_workload_control::WorkOwner, String> {
                self.admitted
                    .as_ref()
                    .ok_or_else(|| "release fixture scope already dropped".to_string())?
                    .original_owner()
            }
            fn is_cancelled(&self) -> Result<bool, String> {
                Ok(self.cancelled)
            }
        }
        impl Drop for ReleaseScope {
            fn drop(&mut self) {
                // Drop the actual admitted root/permit/window fixture first.
                drop(self.admitted.take());
                // The repository lookup is synchronous under its mutex. Check
                // the ordering inside release, not after a scheduling delay.
                let lookup = self.jobs.get(self.job_id);
                tokio::pin!(lookup);
                let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
                let std::task::Poll::Ready(Ok(Some(job))) = lookup.as_mut().poll(&mut cx) else {
                    panic!("job lookup must be immediate");
                };
                assert_eq!(
                    job.state,
                    crate::runtime::MaintenanceJobState::Running,
                    "terminal was published while scope resources were still live"
                );
                self.released.store(true, Ordering::Release);
            }
        }
        struct ReleaseAdmission {
            scope: Mutex<Option<ReleaseScope>>,
        }

        impl OptimizeJobAdmissionPort for ReleaseAdmission {
            fn begin(&self) -> crate::worker::OptimizeJobAdmissionFuture<'_> {
                Box::pin(async move {
                    Ok(match self.scope.lock().unwrap().take() {
                        Some(scope) => OptimizeJobAdmission::Acquired(Box::new(scope)),
                        None => OptimizeJobAdmission::Closed,
                    })
                })
            }
        }
        for (rebind, cancelled, expected) in [
            (
                MaintenanceTargetRebind::Bound,
                false,
                crate::runtime::MaintenanceJobState::Finished,
            ),
            (
                MaintenanceTargetRebind::Replaced,
                false,
                crate::runtime::MaintenanceJobState::TargetReplaced,
            ),
            (
                MaintenanceTargetRebind::Bound,
                true,
                crate::runtime::MaintenanceJobState::CancelledBeforeDispatch,
            ),
        ] {
            let jobs = Arc::new(OptimizeProcessRuntime::new());
            let job_id = submit_job(&jobs).await;
            let released = Arc::new(AtomicBool::new(false));
            let mut worker = OptimizeWorker::start(
                &Handle::current(),
                Arc::clone(&jobs),
                Arc::new(ReleaseAdmission {
                    scope: Mutex::new(Some(ReleaseScope {
                        jobs: Arc::clone(&jobs),
                        job_id,
                        released: Arc::clone(&released),
                        cancelled,
                        admitted: Some(admitted_scope_fixture().await),
                    })),
                }),
                Arc::new(RecordingExecution {
                    calls: Arc::new(Mutex::new(Vec::new())),
                    rebind,
                }),
            );
            let terminal =
                tokio::time::timeout(Duration::from_secs(1), jobs.wait_for_completion(job_id))
                    .await
                    .expect("terminal release barrier must complete")
                    .unwrap();
            assert_eq!(terminal.state, expected);
            assert!(released.load(Ordering::Acquire));
            worker
                .shutdown_until(Instant::now() + Duration::from_secs(1))
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn original_job_receiving_rejects_foreign_permit_without_consuming_either_owner() {
        use novarocks_workload_control::{WorkClass, WorkRequest, WorkloadConfig, WorkloadControl};
        let control = WorkloadControl::try_new_counted(WorkloadConfig::default())
            .unwrap()
            .owner;
        control.mark_ready().unwrap();
        let a = control
            .root_admission()
            .begin_warehouse_root(WorkRequest::new(WorkClass::TableMaintenance))
            .unwrap();
        let b = control
            .root_admission()
            .begin_warehouse_root(WorkRequest::new(WorkClass::TableMaintenance))
            .unwrap();
        let a_scope = a.owner.scope();
        let b_scope = b.owner.scope();
        let mut a_owner = Some(a.owner);
        let mut b_owner = Some(b.owner);
        let mut b_permit = Some(b_scope.admit_query().unwrap().await.unwrap());
        assert!(OptimizeJobOriginals::take_in(&mut a_owner, &mut b_permit).is_err());
        assert!(a_owner.as_ref().unwrap().is_for_scope(&a_scope));
        assert!(b_permit.as_ref().unwrap().is_for_scope(&b_scope));
        let originals = OptimizeJobOriginals::take_in(&mut b_owner, &mut b_permit).unwrap();
        assert!(b_owner.is_none() && b_permit.is_none());
        assert!(originals.owner.as_ref().unwrap().is_for_scope(&b_scope));
        assert!(matches!(
            originals.scope().result_capacity(),
            Err(novarocks_workload_control::WorkError::NotReady)
        ));
        drop(originals);
        a_owner.take().unwrap().complete();
        control.close_admission();
        assert!(control.shutdown().is_ok());
    }

    struct TicketOnlyScope {
        owner: Option<novarocks_workload_control::WorkOwner>,
        permit: Option<novarocks_workload_control::QueryConcurrencyPermit>,
    }
    impl Drop for TicketOnlyScope {
        fn drop(&mut self) {
            drop(self.permit.take());
            if let Some(owner) = self.owner.take() {
                owner.complete();
            }
        }
    }
    impl OptimizeJobScope for TicketOnlyScope {
        fn take_originals(&mut self) -> Result<OptimizeJobOriginals, String> {
            Err("this fixture never dispatches a Provider job".into())
        }
        fn original_owner(&self) -> Result<&novarocks_workload_control::WorkOwner, String> {
            self.owner
                .as_ref()
                .ok_or_else(|| "fixture owner is empty".into())
        }
        fn is_cancelled(&self) -> Result<bool, String> {
            self.owner
                .as_ref()
                .ok_or("fixture owner is empty")?
                .scope()
                .cancellation()
                .map(|c| c.reason().is_some())
                .map_err(|e| e.to_string())
        }
    }
    struct OriginalTicketAdmission {
        roots: novarocks_workload_control::RootAdmissionHandle,
        calls: Arc<std::sync::atomic::AtomicUsize>,
    }
    impl OptimizeJobAdmissionPort for OriginalTicketAdmission {
        fn begin(&self) -> OptimizeJobAdmissionFuture<'_> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                let root = self
                    .roots
                    .begin_warehouse_root(novarocks_workload_control::WorkRequest::new(
                        novarocks_workload_control::WorkClass::TableMaintenance,
                    ))
                    .map_err(|e| e.to_string())?;
                let permit = root
                    .owner
                    .scope()
                    .admit_query()
                    .map_err(|e| e.to_string())?
                    .await
                    .map_err(|e| e.to_string())?;
                Ok(OptimizeJobAdmission::Acquired(Box::new(TicketOnlyScope {
                    owner: Some(root.owner),
                    permit: Some(permit),
                })))
            })
        }
    }
    #[tokio::test]
    async fn waiting_original_ticket_survives_prepared_handoffs_and_receives_before_successor() {
        use novarocks_workload_control::{
            QueryConcurrencyPermit, ResourceConfig, WorkClass, WorkRequest, WorkloadConfig,
            WorkloadControl,
        };
        use std::task::{Context, Waker};
        let control = WorkloadControl::try_new(
            WorkloadConfig {
                query_concurrency_limit: 1,
                ..WorkloadConfig::default()
            },
            ResourceConfig {
                total_bytes: 32768,
                control_bytes: 1024,
                per_scope_bytes: 31744,
            },
        )
        .unwrap();
        control.mark_ready().unwrap();
        let roots = control.root_admission();
        let sql = roots
            .begin_warehouse_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let mut held = Some(sql.owner.scope().admit_query().unwrap().await.unwrap());
        let mut current = sql.owner;
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut waiting = OriginalAdmissionWait::default();
        waiting.start(Arc::new(OriginalTicketAdmission {
            roots: roots.clone(),
            calls: calls.clone(),
        }));
        let mut cx = Context::from_waker(Waker::noop());
        assert!(waiting.poll(&mut cx).is_pending());
        let queued = control.observation().snapshot().admission_records;
        assert_eq!(queued, 1);
        // These are exact same-State permit handoffs. None cancels or
        // reconstructs the old independent job's original warehouse ticket.
        for _ in 0..3 {
            let child = roots
                .begin_warehouse_root(WorkRequest::new(WorkClass::TableMaintenance))
                .unwrap();
            let mut received = None;
            let notice = QueryConcurrencyPermit::handoff_in(
                &mut held,
                &current.scope(),
                &child.owner,
                &mut received,
            )
            .unwrap();
            notice.notify();
            current.complete();
            current = child.owner;
            held = received;
            assert!(waiting.poll(&mut cx).is_pending());
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert_eq!(control.observation().snapshot().admission_records, queued);
            assert_eq!(control.observation().snapshot().admitted_queries, 1);
        }
        drop(held.take());
        current.complete();
        assert!(waiting.poll(&mut cx).is_ready());
        assert!(waiting.future.is_none());
        assert!(matches!(
            waiting.received.as_ref(),
            Some(Ok(OptimizeJobAdmission::Acquired(_)))
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let next = roots
            .begin_warehouse_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let mut successor = Box::pin(next.owner.scope().admit_query().unwrap());
        assert!(std::future::Future::poll(successor.as_mut(), &mut cx).is_pending());
        // Its original grant was positively received before F Drop and is
        // retained in the waiting slot until the original worker consumes it.
        drop(waiting.received.take());
        let next_permit = successor.await.unwrap();
        drop(next_permit);
        next.owner.complete();
        assert_eq!(control.observation().snapshot().admitted_queries, 0);
    }
}
