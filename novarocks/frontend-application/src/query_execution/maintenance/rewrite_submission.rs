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

//! SQL capture, actual preparation exit, original permit transfer and passive
//! observation of the same Optimize record. No SQL owner is fabricated for BG.
use super::{io_admission::MetadataIoAdmission, *};
use crate::query_execution::kernels::MaintenanceExecutionKernel;
use novarocks_query_application::{
    admitted_query_context::QueryExecutionContext,
    api::StatementNativeHealthFailure,
    session_control::{
        GovernedActionContext, GovernedStatementAction, GovernedStatementResidual,
        StatementActionValue,
    },
    session_error::{QueryServiceError, QueryServiceErrorKind},
};
#[cfg(test)]
use novarocks_spi::connector::ConnectorIoAdmissionPort;
use novarocks_spi::connector::{
    ConnectorDistributedRewriteLease, ConnectorOperationControl, ConnectorRequestContext,
    ConnectorTableJobAdmissionRequest, ConnectorTableJobKind, ConnectorTableObjectCaptureRequest,
    ConnectorTableObjectSelector, ConnectorTableResolution, ProviderMetadataCpuPort,
};
use novarocks_table_maintenance::{
    OptimizeCompletionMode,
    activity::{MaintenanceActivityFamily, MaintenanceActivityPermit},
    product::TableMaintenanceProduct,
    runtime::JobCreate,
    worker::OptimizeJobOriginals,
};
use novarocks_workload_control::{WorkClass, WorkError, WorkOwner, WorkRequest};
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

type PrepareFuture = Pin<Box<dyn Future<Output = Result<(), TerminalError>> + Send>>;
type ObservedEffect = Option<novarocks_coordination_contract::ExecutionEffect>;
type ObserveFuture = Pin<Box<dyn Future<Output = Result<ObservedEffect, TerminalError>> + Send>>;
#[derive(Debug, Default)]
struct SubmitWake(futures::task::AtomicWaker);
impl crate::task_execution::status_intake::StatusIntakeWake for SubmitWake {
    fn wake(&self) {
        self.0.wake()
    }
}
struct SubmissionBody {
    engine: RequestScopedMaintenanceEngine,
    product: Arc<TableMaintenanceProduct>,
    roots: novarocks_workload_control::RootAdmissionHandle,
    lease: ConnectorDistributedRewriteLease,
    target: MaintenanceTarget,
    context: ConnectorRequestContext,
    dependencies: Vec<novarocks_spi::connector::SharedDependency>,
    cpu: Option<Arc<dyn ProviderMetadataCpuPort>>,
    activity: Option<MaintenanceActivityPermit>,
    request: Option<JobCreate<MaintenanceTarget>>,
    originals: Option<OptimizeJobOriginals>,
    pending_owner: Option<WorkOwner>,
    completion: Option<OptimizeCompletionMode>,
    submitted: Option<novarocks_table_maintenance::runtime::JobHandle>,
    #[cfg(novarocks_dev_controls)]
    controls: Option<Arc<crate::table_maintenance::test_controls::MaintenanceTestControls>>,
    #[cfg(novarocks_dev_controls)]
    receipt: Option<crate::table_maintenance::test_controls::SubmissionReceipt>,
}
struct OptimizeSubmissionState {
    source: Arc<tokio::sync::Mutex<SubmissionBody>>,
    io: Option<Arc<MetadataIoAdmission>>,
    origin: novarocks_workload_control::ResultWindowAlias,
    wake: Arc<SubmitWake>,
    prepare: Option<PrepareFuture>,
    prepare_result: Option<Result<(), TerminalError>>,
    ordinary_io_failure: Option<WorkError>,
    prepare_exited: bool,
    observe: Option<ObserveFuture>,
    observed: Option<Result<ObservedEffect, TerminalError>>,
    cpu_closed: bool,
    installed: bool,
    selected: bool,
    disposition: Option<novarocks_spi::connector::LakePublicationDisposition>,
    health: Option<StatementNativeHealthFailure>,
}
struct OptimizeSubmissionAction {
    original: Option<OptimizeSubmissionState>,
}
struct SubmitResidual {
    original: OptimizeSubmissionState,
}
fn service(error: impl ToString) -> QueryServiceError {
    QueryServiceError::new(QueryServiceErrorKind::Internal, error.to_string())
}
fn producer_service(
    error: impl novarocks_failure_contract::FailureProjection
    + std::error::Error
    + Send
    + Sync
    + 'static,
) -> QueryServiceError {
    QueryServiceError::from_failure_cause(
        novarocks_failure_contract::SharedFailureCause::from_static_detail(
            error,
            "Optimize original operation failed",
        ),
    )
}
fn unproven(error: impl std::error::Error + Send + Sync + 'static) -> StatementNativeHealthFailure {
    StatementNativeHealthFailure::actual_exit_unproven(
        "Optimize original preparation exit is unproven",
        Arc::new(error),
    )
}
pub(crate) fn prepare(
    kernel: &MaintenanceExecutionKernel,
    names: &[String],
    context: MaintenanceRequestContext<'_>,
    execution: &QueryExecutionContext,
    connector: &ConnectorRequestContext,
) -> Result<Box<dyn GovernedStatementAction>, TerminalError> {
    let host = kernel
        .service()
        .original_optimize_host()
        .map_err(TerminalError::pre_dispatch_failed)?;
    let engine =
        RequestScopedMaintenanceEngine::new(kernel.clone(), execution.clone(), connector.clone());
    let target = engine
        .resolve_target(names, context)
        .map_err(TerminalError::pre_dispatch_failed)?;
    let capacity = execution.result_capacity().ok_or_else(|| {
        TerminalError::pre_dispatch_failed("Optimize SQL has no original result capacity")
    })?;
    let identity = RequestScopedMaintenanceEngine::target_identity(&target)
        .map_err(TerminalError::pre_dispatch_failed)?;
    let lease = kernel
        .connector_control()
        .acquire_current_distributed_rewrite(&identity.instance_id)
        .map_err(TerminalError::from_original_cause)?;
    let dependencies = lease
        .planning_initial_dependencies()
        .map_err(TerminalError::from_original_cause)?;
    let origin = capacity
        .try_window_alias()
        .map_err(TerminalError::from_original_cause)?;
    let connector = connector.clone();
    let source = Arc::new(tokio::sync::Mutex::new(SubmissionBody {
        engine: RequestScopedMaintenanceEngine::new(
            kernel.clone(),
            execution.clone(),
            connector.clone(),
        ),
        product: host.product,
        roots: host.roots,
        lease,
        target,
        context: connector,
        dependencies,
        cpu: None,
        activity: None,
        request: None,
        originals: None,
        pending_owner: None,
        completion: None,
        submitted: None,
        #[cfg(novarocks_dev_controls)]
        controls: host.controls,
        #[cfg(novarocks_dev_controls)]
        receipt: None,
    }));
    Ok(Box::new(OptimizeSubmissionAction {
        original: Some(OptimizeSubmissionState {
            source,
            io: None,
            origin,
            wake: Arc::new(SubmitWake::default()),
            prepare: None,
            prepare_result: None,
            ordinary_io_failure: None,
            prepare_exited: false,
            observe: None,
            observed: None,
            cpu_closed: false,
            installed: false,
            selected: false,
            disposition: None,
            health: None,
        }),
    }))
}
impl OptimizeSubmissionState {
    fn poll_prepare_exit(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if self.prepare_exited {
            return Poll::Ready(());
        }
        let Some(future) = self.prepare.as_mut() else {
            self.prepare_exited = true;
            return Poll::Ready(());
        };
        let result = std::task::ready!(future.as_mut().poll(cx));
        self.prepare_result = Some(result);
        // Original Work/activity/target/session facts remain in source.
        drop(self.prepare.take());
        self.prepare_exited = true;
        Poll::Ready(())
    }
    fn poll_result(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatementActionValue, QueryServiceError>> {
        self.wake.0.register(cx.waker());
        if !self.origin.is_for_scope(owner.scope()) {
            return Poll::Ready(Err(service("Optimize SQL borrowed another original owner")));
        }
        let scope = owner.scope().clone();
        let original = owner
            .execution_owner_slot()
            .as_ref()
            .ok_or_else(|| service("Optimize SQL original owner is empty"))?;
        if self.io.is_none() {
            let Ok(mut source) = self.source.try_lock() else {
                return Poll::Pending;
            };
            let io =
                MetadataIoAdmission::for_original_source(original, source.dependencies.clone())
                    .map_err(producer_service)?;
            source.context = source.context.clone().with_io_admission(io.clone());
            source.engine = RequestScopedMaintenanceEngine::new(
                source.engine.kernel.clone(),
                source.engine.execution.clone(),
                source.context.clone(),
            );
            self.io = Some(io);
        }
        let io = self.io.as_ref().unwrap().clone();
        if !io.poll_progress(original, cx).map_err(producer_service)? {
            return Poll::Pending;
        }
        if self.prepare.is_none() && !self.prepare_exited {
            let Ok(mut source) = self.source.try_lock() else {
                return Poll::Pending;
            };
            let runtime = source.engine.kernel.rewrite_runtime().map_err(service)?;
            let cpu = runtime
                .execution
                .0
                .provider_creator_cpu()
                .map_err(service)?
                .provider_metadata_cpu_port(
                    &scope,
                    self.origin.try_clone().map_err(service)?,
                    &source.lease,
                    source.engine.execution.cancellation().clone(),
                    runtime.execution.0.runtime().clone(),
                    self.wake.clone(),
                )
                .map_err(service)?;
            source.cpu = Some(cpu);
            drop(source);
            self.prepare = Some(Box::pin(prepare_original_submission(self.source.clone())));
        }
        std::task::ready!(self.poll_prepare_exit(cx));
        if !self.cpu_closed {
            let Ok(source) = self.source.try_lock() else {
                return Poll::Pending;
            };
            if let Some(cpu) = &source.cpu {
                std::task::ready!(cpu.poll_closed(cx)).map_err(producer_service)?;
            }
            self.cpu_closed = true;
        }
        // No original local producer F/CPU or physical metadata request can
        // borrow the SQL permit across this transfer.
        std::task::ready!(poll_preparation_close(
            &io,
            &self.prepare_result,
            &mut self.ordinary_io_failure,
            &mut self.health,
            cx,
        ))?;
        if !self.installed {
            let Ok(mut source) = self.source.try_lock() else {
                return Poll::Pending;
            };
            let mut originals = source
                .originals
                .take()
                .ok_or_else(|| service("Optimize prepared Root is empty"))?;
            // Preserve every original on refusal. The domain record receives
            // the same capability before its existing worker can observe it.
            let transferred = owner.handoff_query_permit_in(
                originals.owner.as_ref().unwrap(),
                &mut originals.query_concurrency,
            );
            if let Err(error) = transferred {
                source.originals = Some(originals);
                return Poll::Ready(Err(service(error)));
            }
            source.originals = Some(originals);
            let request = source
                .request
                .as_ref()
                .expect("one captured Optimize request")
                .clone();
            let product = source.product.clone();
            let SubmissionBody {
                activity,
                originals,
                ..
            } = &mut *source;
            let job = product
                .jobs()
                .install_original_job_in(request, activity, originals)
                .map_err(service)?;
            source.submitted = Some(job.handle());
            drop(source.request.take());
            drop(job);
            self.installed = true;
            #[cfg(novarocks_dev_controls)]
            if let Some(receipt) = source.receipt.take() {
                receipt
                    .bind(source.submitted.as_ref().unwrap().job_id())
                    .map_err(service)?;
            }
            // Stable record first, original handoff notification second.
            let notice = transferred.unwrap();
            drop(source);
            product.jobs().notify_original_job().map_err(service)?;
            notice.notify();
        }
        if self.observe.is_none() && self.observed.is_none() {
            let Ok(source) = self.source.try_lock() else {
                return Poll::Pending;
            };
            if source.completion == Some(OptimizeCompletionMode::Detached) {
                self.observed = Some(Ok(None));
            } else {
                self.observe = Some(Box::pin(observe_original_submission(self.source.clone())));
            }
        }
        if let Some(future) = self.observe.as_mut() {
            self.observed = Some(std::task::ready!(future.as_mut().poll(cx)));
            drop(self.observe.take());
        }
        self.selected = true;
        self.disposition = observed_publication(self.observed.as_ref().unwrap());
        Poll::Ready(
            self.observed
                .take()
                .expect("same Optimize observation")
                .map(|_| StatementActionValue::Ok(novarocks_query_application::session_control::StatementCommandCompletion::new(0)))
                .map_err(super::rewrite_job::computation_service_error),
        )
    }
    fn poll_close(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        self.wake.0.register(cx.waker());
        if let Some(io) = self.io.as_ref()
            && let Err(error) = io.poll_continuing(cx)
        {
            // An unstarted ordinary refusal is retained by the Source;
            // actual quiescence below decides physical exit separately.
            if let Some(failure) = io.health_failure() {
                return Poll::Ready(Err(failure));
            }
            self.ordinary_io_failure.get_or_insert(error);
        }
        std::task::ready!(self.poll_prepare_exit(cx));
        let Ok(mut source) = self.source.try_lock() else {
            return Poll::Pending;
        };
        if let Some(cpu) = &source.cpu {
            std::task::ready!(cpu.poll_closed(cx)).map_err(unproven)?;
        }
        // Observation owns no producer/capacity. A selected original job stays
        // in its same repository and common background Source.
        drop(self.observe.take());
        if !self.installed {
            drop(source.request.take());
            drop(source.activity.take());
            drop(source.originals.take());
            if let Some(owner) = source.pending_owner.take() {
                owner.complete_after_terminal_cancel_settled();
            }
        }
        drop(source);
        if let Some(io) = self.io.as_ref() {
            match io.withdraw_unstarted_in().map_err(unproven)? {
                Some(observed) => {
                    if let Some(failure) = observed.into_ordinary_failure() {
                        self.ordinary_io_failure.get_or_insert(failure);
                    }
                }
                None => {
                    let observed = std::task::ready!(io.poll_actual_quiescent_continuing(cx))?;
                    if let Some(failure) = observed.into_ordinary_failure() {
                        self.ordinary_io_failure.get_or_insert(failure);
                    }
                }
            }
        }
        Poll::Ready(Ok(()))
    }
}
// The result receiver already owns the preparation conclusion. Actual I/O
// exit is a separate prerequisite; its ordinary refusal cannot replace that
// original conclusion with a newly projected cause during close.
fn poll_preparation_close(
    io: &Arc<MetadataIoAdmission>,
    conclusion: &Option<Result<(), TerminalError>>,
    ordinary_failure: &mut Option<WorkError>,
    health: &mut Option<StatementNativeHealthFailure>,
    cx: &mut Context<'_>,
) -> Poll<Result<(), QueryServiceError>> {
    let observed = match std::task::ready!(io.poll_actual_quiescent_continuing(cx)) {
        Ok(observed) => observed,
        Err(failure) => {
            health.get_or_insert(failure);
            return Poll::Ready(Err(service("Optimize original metadata exit is unproven")));
        }
    };
    if let Some(failure) = observed.into_ordinary_failure() {
        ordinary_failure.get_or_insert(failure);
    }
    if let Some(Err(original)) = conclusion {
        return Poll::Ready(Err(super::rewrite_job::computation_service_error(
            original.clone(),
        )));
    }
    if let Some(failure) = ordinary_failure {
        return Poll::Ready(Err(producer_service(failure.clone())));
    }
    match conclusion {
        Some(Ok(())) => Poll::Ready(Ok(())),
        None => Poll::Ready(Err(service("Optimize preparation conclusion is absent"))),
        Some(Err(_)) => unreachable!("the original preparation failure was selected above"),
    }
}

impl GovernedStatementAction for OptimizeSubmissionAction {
    fn poll_action(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatementActionValue, QueryServiceError>> {
        self.original
            .as_mut()
            .expect("original Optimize submission")
            .poll_result(owner, cx)
    }
    fn external_effect_disposition(
        &self,
    ) -> Option<novarocks_query_application::api::StatementExternalEffectDisposition> {
        self.original
            .as_ref()
            .and_then(|s| s.disposition)
            .map(crate::query::external_effect_from_lake)
    }
    fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        self.original.as_ref().and_then(|s| {
            s.health
                .clone()
                .or_else(|| s.io.as_ref().and_then(|io| io.health_failure()))
        })
    }
    fn handoff_residual_in(
        &mut self,
        target: &mut Option<Box<dyn GovernedStatementResidual>>,
    ) -> Result<(), StatementNativeHealthFailure> {
        if target.is_some() {
            return Err(unproven(WorkError::Conflict));
        }
        if let Some(original) = self.original.take() {
            *target = Some(Box::new(SubmitResidual { original }));
        }
        Ok(())
    }
}
impl GovernedStatementResidual for SubmitResidual {
    fn poll_retirement(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        self.original.poll_close(cx)
    }
}
async fn prepare_original_submission(
    source: Arc<tokio::sync::Mutex<SubmissionBody>>,
) -> Result<(), TerminalError> {
    let mut original = source.lock().await;
    let identity = RequestScopedMaintenanceEngine::target_identity(&original.target)
        .map_err(TerminalError::pre_dispatch_failed)?;
    original.activity = Some(
        original
            .product
            .acquire_activity(&original.target, MaintenanceActivityFamily::Optimize)
            .map_err(|error| TerminalError::pre_dispatch_failed(error.to_string()))?,
    );
    original
        .engine
        .reject_direct_rewrite_on_mv_governed(
            &original.target,
            &original.lease.planning_lease(),
            original.cpu.as_ref().unwrap().clone(),
        )
        .await
        .map_err(crate::query_execution::maintenance::rewrite_job::guard_terminal_error)?;
    #[cfg(novarocks_dev_controls)]
    if let Some(controls) = &original.controls
        && let Some(nomination) = original.engine.execution.test_statement_nomination()
    {
        original.receipt = controls
            .begin_submission(nomination.as_ref(), &original.target)
            .map_err(TerminalError::pre_dispatch_failed)?;
    }
    let metadata = original.lease.metadata().clone();
    let completion = metadata
        .admit_table_job(ConnectorTableJobAdmissionRequest {
            table: identity.clone(),
            job: ConnectorTableJobKind::RewriteDataFiles,
            context: original.context.clone(),
        })
        .map_err(TerminalError::from_original_cause)?;
    let (captured, snapshot) = metadata
        .capture_table_object_with_snapshot_cpu(
            ConnectorTableObjectCaptureRequest {
                table: identity.clone(),
                resolution: ConnectorTableResolution::StrictBaseTable,
                selector: ConnectorTableObjectSelector::Current,
                context: original.context.clone(),
            },
            original.cpu.as_ref().unwrap().clone(),
        )
        .await
        .map_err(TerminalError::from_original_cause)?;
    #[cfg(novarocks_dev_controls)]
    if let Some(receipt) = &original.receipt {
        receipt
            .capture(captured.object_id.as_bytes())
            .map_err(TerminalError::pre_dispatch_failed)?;
    }
    let snapshot = snapshot.ok_or_else(|| {
        TerminalError::pre_dispatch_failed("Optimize target has no current snapshot")
    })?;
    original
        .context
        .check_active()
        .map_err(TerminalError::from_original_cause)?;
    let root = original
        .roots
        .begin_warehouse_root(WorkRequest::new(WorkClass::TableMaintenance))
        .map_err(TerminalError::from_original_cause)?;
    original.pending_owner = Some(root.owner);
    original.originals = Some(
        OptimizeJobOriginals::take_prepared_root_in(&mut original.pending_owner)
            .map_err(TerminalError::pre_dispatch_failed)?,
    );
    original.request = Some(JobCreate {
        target: original.target.clone(),
        effect_id: None,
        object_id: captured.object_id.as_bytes().to_vec(),
        base_snapshot_id: snapshot,
        created_at_ms: now_ms(),
    });
    original.completion = Some(match completion {
        novarocks_spi::connector::ConnectorTableJobAdmission::Detached => {
            OptimizeCompletionMode::Detached
        }
        novarocks_spi::connector::ConnectorTableJobAdmission::AwaitTerminal => {
            OptimizeCompletionMode::AwaitTerminal
        }
    });
    Ok(())
}
fn observed_publication(
    result: &Result<ObservedEffect, TerminalError>,
) -> Option<novarocks_spi::connector::LakePublicationDisposition> {
    use novarocks_coordination_contract::ExecutionEffect;
    use novarocks_spi::connector::LakePublicationDisposition as P;
    match result {
        Ok(Some(ExecutionEffect::External)) => Some(P::KnownCommitted),
        Ok(Some(ExecutionEffect::None) | None) => None,
        Err(error) => {
            if error.effect == Some(ExecutionEffect::External) {
                return Some(P::KnownCommitted);
            }
            match error.state {
                novarocks_table_maintenance::runtime::MaintenanceJobState::KnownUncommitted => Some(P::KnownUncommitted),
                novarocks_table_maintenance::runtime::MaintenanceJobState::CommitUnknown => Some(P::CommitUnknown),
                novarocks_table_maintenance::runtime::MaintenanceJobState::KnownCommittedFinalizationFailed
                    if error.effect != Some(ExecutionEffect::None) => Some(P::KnownCommitted),
                _ => None,
            }
        }
    }
}
async fn observe_original_submission(
    source: Arc<tokio::sync::Mutex<SubmissionBody>>,
) -> Result<ObservedEffect, TerminalError> {
    let (jobs, handle, context) = {
        let s = source.lock().await;
        (
            s.product.jobs().service_runtime(),
            *s.submitted.as_ref().unwrap(),
            s.context.clone(),
        )
    };
    observe_original_job(&jobs, handle, &context).await
}
async fn observe_original_job(
    jobs: &novarocks_table_maintenance::OptimizeProcessRuntime,
    handle: novarocks_table_maintenance::runtime::JobHandle,
    context: &ConnectorRequestContext,
) -> Result<ObservedEffect, TerminalError> {
    let wait = jobs.wait_for_completion(handle.job_id());
    tokio::pin!(wait);
    let mut cancelled = false;
    let mut cancel_error = None;
    let terminal = tokio::select! {
        biased;
        terminal = &mut wait => terminal,
        _ = context.stop().stopped() => {
            cancelled = true;
            cancel_error = jobs.request_cancel(handle.job_id()).await.err().map(|e| e.to_string());
            wait.await
        },
        _ = tokio::time::sleep_until(context.deadline().into()) => {
            cancelled = true;
            cancel_error = jobs.request_cancel(handle.job_id()).await.err().map(|e| e.to_string());
            wait.await
        },
    }
    .map_err(|e| TerminalError::failed(e.to_string()))?;
    let effect = terminal.effect;
    crate::table_maintenance::optimize_statement_terminal_result(
        handle,
        terminal,
        cancelled,
        cancel_error,
    )?;
    Ok(effect)
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or_default()
}

#[cfg(test)]
mod observer_effect_tests {
    use super::*;
    use novarocks_coordination_contract::ExecutionEffect;
    use novarocks_spi::connector::{ConnectorStopOwner, LakePublicationDisposition};
    use novarocks_table_maintenance::{
        OptimizeJobOutcome, OptimizeProcessRuntime,
        activity::{MaintenanceActivityFamily, TableMaintenanceActivity},
        runtime::{JobCreate, JobHandle},
    };
    use std::time::{Duration, Instant};

    async fn original_job() -> (OptimizeProcessRuntime, JobHandle) {
        let jobs = OptimizeProcessRuntime::new();
        let target = MaintenanceTarget {
            catalog: "ice".into(),
            namespace: "db".into(),
            table: "orders".into(),
        };
        let activity = TableMaintenanceActivity::default();
        let permit = activity
            .acquire(&target, MaintenanceActivityFamily::Optimize)
            .unwrap();
        let job = jobs
            .submit(
                JobCreate {
                    target,
                    effect_id: None,
                    object_id: vec![1],
                    base_snapshot_id: 1,
                    created_at_ms: 1,
                },
                permit,
            )
            .await
            .unwrap();
        assert_eq!(
            jobs.claim_next(2).await.unwrap().unwrap().job_id,
            job.job_id
        );
        (jobs, job.handle())
    }
    fn outcome(effect: ExecutionEffect) -> OptimizeJobOutcome {
        OptimizeJobOutcome {
            commit_occurred: Some(effect == ExecutionEffect::External),
            target_snapshot_id: Some(if effect == ExecutionEffect::External {
                2
            } else {
                1
            }),
            rewritten_data_files: i64::from(effect == ExecutionEffect::External),
            deleted_data_files: i64::from(effect == ExecutionEffect::External),
            added_data_files: Some(i64::from(effect == ExecutionEffect::External)),
            added_delete_files: Some(0),
            output_record_count: Some(3),
        }
    }
    #[tokio::test]
    async fn original_optimize_waiter_keeps_applied_and_noop_through_late_stop() {
        for effect in [ExecutionEffect::External, ExecutionEffect::None] {
            for poll_before_ready in [false, true] {
                let (jobs, handle) = original_job().await;
                let stop = ConnectorStopOwner::new();
                let context = ConnectorRequestContext::try_new(
                    Instant::now() + Duration::from_secs(30),
                    stop.view(),
                    1024,
                    2048,
                )
                .unwrap();
                let wait = observe_original_job(&jobs, handle, &context);
                tokio::pin!(wait);
                if poll_before_ready {
                    assert!(
                        std::future::poll_fn(|cx| {
                            Poll::Ready(wait.as_mut().poll(cx).is_pending())
                        })
                        .await
                    );
                }
                jobs.finish_with_effect(handle.job_id(), Ok(outcome(effect)), Some(effect), 3)
                    .await
                    .unwrap();
                stop.request_stop();
                let result = tokio::time::timeout(Duration::from_secs(1), wait)
                    .await
                    .unwrap();
                assert_eq!(result.as_ref().unwrap(), &Some(effect));
                assert_eq!(
                    observed_publication(&result),
                    (effect == ExecutionEffect::External)
                        .then_some(LakePublicationDisposition::KnownCommitted)
                );
                assert!(!jobs.cancellation_requested(handle.job_id()).await.unwrap());
            }
        }
    }
    #[tokio::test]
    async fn original_failed_optimize_retains_cumulative_effect_and_exact_unknown() {
        for error in [
            TerminalError::known_committed_finalization_failed("necessary finalization failed"),
            TerminalError::known_committed_finalization_failed(
                "NoOp necessary finalization failed",
            )
            .with_effect(ExecutionEffect::None),
            TerminalError::failed("later step failed").with_effect(ExecutionEffect::External),
            TerminalError::commit_unknown("original publication is unknown"),
            TerminalError::known_uncommitted("original publication was refused"),
        ] {
            let (jobs, handle) = original_job().await;
            let stop = ConnectorStopOwner::new();
            let context = ConnectorRequestContext::try_new(
                Instant::now() + Duration::from_secs(30),
                stop.view(),
                1024,
                2048,
            )
            .unwrap();
            let expected = if error.effect == Some(ExecutionEffect::External) {
                Some(LakePublicationDisposition::KnownCommitted)
            } else if error.state
                == novarocks_table_maintenance::runtime::MaintenanceJobState::CommitUnknown
            {
                Some(LakePublicationDisposition::CommitUnknown)
            } else if error.state == novarocks_table_maintenance::runtime::MaintenanceJobState::KnownCommittedFinalizationFailed {
                None
            } else {
                Some(LakePublicationDisposition::KnownUncommitted)
            };
            jobs.finish_with_effect(handle.job_id(), Err(error.clone()), error.effect, 3)
                .await
                .unwrap();
            let result = observe_original_job(&jobs, handle, &context).await;
            let original = result.as_ref().unwrap_err();
            assert_eq!(original.state, error.state);
            assert_eq!(original.message, error.message);
            assert_eq!(original.effect, error.effect);
            assert_eq!(observed_publication(&result), expected);
        }
    }
}

#[cfg(test)]
#[path = "rewrite_submission/guard_close_tests.rs"]
mod guard_close_tests;
