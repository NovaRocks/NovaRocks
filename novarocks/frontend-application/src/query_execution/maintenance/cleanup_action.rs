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

//! SQL cleanup borrows the original Root, local result ticket and Product.
use super::{RequestScopedMaintenanceEngine, io_admission::MetadataIoAdmission};
use crate::query_execution::kernels::MaintenanceExecutionKernel;
use novarocks_failure_contract::SharedFailureCause;
use novarocks_query_application::{
    admitted_query_context::QueryExecutionContext,
    api::{LocalResultProducer, StatementNativeHealthFailure},
    session_control::{
        GovernedActionContext, GovernedStatementAction, GovernedStatementResidual,
        StatementActionValue,
    },
    session_error::{QueryServiceError, QueryServiceErrorKind},
};
use novarocks_spi::connector::{
    ConnectorCleanupAsyncExecution, ConnectorCleanupAsyncObservation, ConnectorCleanupAsyncStart,
    ConnectorCleanupMaintenanceSession, ConnectorCleanupOperationId, ConnectorControlPlanningPin,
    ConnectorError, ConnectorOriginalResultScope, ConnectorRequestContext,
    LakePublicationDisposition, ProviderMetadataCpuPort, SharedDependency,
};
use novarocks_table_maintenance::{
    MaintenanceActionOutcome, MaintenanceActionRequest, MaintenanceTarget,
    product::{
        DistributedRewriteSession, MaintenanceFuture, RewriteIntent, TableMaintenanceEffectPort,
        TableMaintenanceProduct,
    },
    runtime::TerminalError,
};
use novarocks_workload_control::{ResultWindowAlias, ResultWindowClass, WorkError};
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
type ProductF =
    Pin<Box<dyn Future<Output = Result<MaintenanceActionOutcome, TerminalError>> + Send>>;
#[derive(Default)]
struct Wake {
    waker: futures::task::AtomicWaker,
    observation: std::sync::Mutex<Option<ConnectorCleanupAsyncObservation>>,
}
impl std::fmt::Debug for Wake {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CleanupWake")
    }
}
impl crate::wake::StatusIntakeWake for Wake {
    fn wake(&self) {
        self.waker.wake()
    }
}
struct Body {
    engine: RequestScopedMaintenanceEngine,
    product: Arc<TableMaintenanceProduct>,
    target: MaintenanceTarget,
    older_than: i64,
    cleanup_session: ConnectorCleanupMaintenanceSession,
    planning: ConnectorControlPlanningPin,
    context: ConnectorRequestContext,
    cpu: Option<Arc<dyn ProviderMetadataCpuPort>>,
    original: Option<ConnectorOriginalResultScope>,
    wake: Arc<Wake>,
    start: Option<ConnectorCleanupAsyncStart>,
    session: Option<ConnectorCleanupAsyncExecution>,
}
struct State {
    family: novarocks_result_contract::ClientPresentationFamily,
    body: Arc<tokio::sync::Mutex<Body>>,
    dependencies: Vec<SharedDependency>,
    io: Option<Arc<MetadataIoAdmission>>,
    cpu: Option<Arc<dyn ProviderMetadataCpuPort>>,
    origin: Option<ResultWindowAlias>,
    spark_call: bool,
    local: Option<LocalResultProducer>,
    wake: Arc<Wake>,
    future: Option<ProductF>,
    result: Option<Result<MaintenanceActionOutcome, TerminalError>>,
    ordinary_io_failure: Option<WorkError>,
    product_exited: bool,
    source_closed: bool,
    selected: bool,
}
struct Action {
    state: Option<State>,
}
struct Residual {
    state: State,
}
fn service(error: impl ToString) -> QueryServiceError {
    QueryServiceError::new(QueryServiceErrorKind::Internal, error.to_string())
}
fn original_service(error: WorkError) -> QueryServiceError {
    QueryServiceError::from_failure_cause(SharedFailureCause::from_static_detail(
        error,
        "cleanup original admission failed",
    ))
}
fn health(error: impl std::error::Error + Send + Sync + 'static) -> StatementNativeHealthFailure {
    StatementNativeHealthFailure::actual_exit_unproven(
        "cleanup original source exit is unproven",
        Arc::new(error),
    )
}
pub(crate) fn prepare(
    kernel: &MaintenanceExecutionKernel,
    target: MaintenanceTarget,
    older_than: i64,
    retained_planning: Option<ConnectorControlPlanningPin>,
    spark_call: bool,
    execution: &QueryExecutionContext,
    connector: &ConnectorRequestContext,
    family: novarocks_result_contract::ClientPresentationFamily,
) -> Result<Box<dyn GovernedStatementAction>, TerminalError> {
    let capacity = execution.result_capacity().ok_or_else(|| {
        TerminalError::pre_dispatch_failed("cleanup has no original result capacity")
    })?;
    let origin = capacity
        .try_window_alias()
        .map_err(TerminalError::from_original_cause)?;
    if origin.class() != ResultWindowClass::Local || !origin.is_for_scope(capacity.root_scope()) {
        return Err(TerminalError::pre_dispatch_failed(
            "cleanup requires its original SQL Local declaration",
        ));
    }
    let identity = RequestScopedMaintenanceEngine::target_identity(&target)
        .map_err(TerminalError::pre_dispatch_failed)?;
    let planning = match retained_planning {
        Some(planning) => {
            if planning.binding().descriptor().instance_id != identity.instance_id {
                return Err(TerminalError::pre_dispatch_failed(
                    "cleanup target differs from its retained generation",
                ));
            }
            planning
        }
        None => kernel
            .connector_control()
            .acquire_current(&identity.instance_id)
            .map_err(TerminalError::from_original_cause)?,
    };
    let cleanup_session = kernel
        .connector_control()
        .acquire_exact_cleanup_maintenance(planning.control_runtime_id())
        .map_err(TerminalError::from_original_cause)?;
    let dependencies = cleanup_session
        .initial_dependencies()
        .map_err(TerminalError::from_original_cause)?;
    let product = kernel
        .service()
        .original_optimize_host()
        .map_err(TerminalError::pre_dispatch_failed)?
        .product;
    let wake = Arc::new(Wake::default());
    let body = Arc::new(tokio::sync::Mutex::new(Body {
        engine: RequestScopedMaintenanceEngine::new(
            kernel.clone(),
            execution.clone(),
            connector.clone(),
        ),
        product,
        target,
        older_than,
        cleanup_session,
        planning,
        context: connector.clone(),
        cpu: None,
        original: None,
        wake: wake.clone(),
        start: None,
        session: None,
    }));
    Ok(Box::new(Action {
        state: Some(State {
            family,
            body,
            dependencies,
            io: None,
            cpu: None,
            origin: Some(origin),
            spark_call,
            local: None,
            wake,
            future: None,
            result: None,
            ordinary_io_failure: None,
            product_exited: false,
            source_closed: false,
            selected: false,
        }),
    }))
}
impl State {
    fn poll_product(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if self.product_exited {
            return Poll::Ready(());
        }
        let Some(future) = self.future.as_mut() else {
            return Poll::Ready(());
        };
        let result = std::task::ready!(future.as_mut().poll(cx));
        self.result = Some(result);
        // Result is stable before foreign F Drop. If Drop panics the original
        // Statement Source keeps this WHOLE Action and does not repoll it.
        drop(self.future.take());
        self.product_exited = true;
        Poll::Ready(())
    }
    fn poll_action(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatementActionValue, QueryServiceError>> {
        self.wake.waker.register(cx.waker());
        if self.selected {
            return Poll::Ready(Err(service("cleanup result was already selected")));
        }
        let scope = owner.scope().clone();
        if !self
            .origin
            .as_ref()
            .is_some_and(|origin| origin.is_for_scope(&scope))
        {
            return Poll::Ready(Err(service("cleanup borrowed another original Root")));
        }
        // Freeze the original Local delivery allowance before the first
        // Provider effect; an observed successful terminal is not cancelled by
        // trying to acquire a fresh ticket after the commit.
        if self.spark_call && self.local.is_none() {
            let window = owner
                .result_window_alias()
                .ok_or_else(|| service("cleanup original Local result window is absent"))?;
            self.local = Some(
                LocalResultProducer::try_new(owner.scope(), window, self.family)
                    .map_err(service)?,
            );
        }
        let work = owner
            .execution_owner_slot()
            .as_ref()
            .ok_or_else(|| service("cleanup original WorkOwner is absent"))?;
        if self.io.is_none() {
            self.io = Some(
                MetadataIoAdmission::for_original_source(work, self.dependencies.clone())
                    .map_err(original_service)?,
            );
        }
        let io = self.io.as_ref().unwrap();
        let progress = io.poll_progress(work, cx);
        if self.future.is_none() && !self.product_exited {
            if !*progress
                .as_ref()
                .map_err(|error| original_service(error.clone()))?
            {
                return Poll::Pending;
            }
            let Ok(mut body) = self.body.try_lock() else {
                return Poll::Pending;
            };
            let runtime = body
                .engine
                .kernel
                .rewrite_runtime()
                .map_err(service)?
                .clone();
            let capacity = io.root_capacity().map_err(original_service)?;
            // Retain data under the SAME original Root budget. The SQL Local
            // ticket is only for delivery; residual cleanup never keeps it.
            let execution = body
                .engine
                .execution
                .for_statement_retention(capacity.clone())
                .map_err(original_service)?;
            body.engine = RequestScopedMaintenanceEngine::new(
                body.engine.kernel.clone(),
                execution,
                body.context.clone(),
            );
            let cpu = runtime
                .execution
                .0
                .provider_creator_cpu()
                .map_err(service)?
                .cleanup_metadata_cpu_port(
                    &scope,
                    capacity.try_window_alias().map_err(original_service)?,
                    &body.cleanup_session,
                    body.engine.execution.cancellation().clone(),
                    runtime.execution.0.runtime().clone(),
                    self.wake.clone(),
                )
                .map_err(original_service)?;
            body.context = body.context.clone().with_io_admission(io.clone());
            body.original = Some(
                super::rewrite_capture::borrow_positive_capture(io, &body.context).map_err(
                    |e| {
                        QueryServiceError::from_failure_cause(
                            SharedFailureCause::from_static_detail(
                                e,
                                "cleanup original result source refused",
                            ),
                        )
                    },
                )?,
            );
            body.cpu = Some(cpu.clone());
            self.cpu = Some(cpu);
            drop(body);
            self.future = Some(Box::pin(run_product(self.body.clone())));
        }
        if self.poll_product(cx).is_pending() {
            // Physical I/O is progressed even after a business stop. Its
            // ordinary refusal is not an actual-exit/health verdict.
            if let Err(error) = progress {
                self.ordinary_io_failure.get_or_insert(error);
            }
            return Poll::Pending;
        }
        let result = self
            .result
            .take()
            .ok_or_else(|| service("cleanup original Product result is absent"))?;
        self.selected = true;
        let disposition = self.disposition();
        let outcome = result.map_err(|error| {
            let error = super::rewrite_job::computation_service_error(error);
            match disposition {
                Some(d) => error
                    .attach_external_effect_disposition(crate::query::external_effect_from_lake(d)),
                None => error,
            }
        })?;
        if !self.spark_call {
            return Poll::Ready(Ok(StatementActionValue::Ok(
                novarocks_query_application::session_control::StatementCommandCompletion::empty(),
            )));
        }
        let producer = self
            .local
            .take()
            .ok_or_else(|| service("cleanup positive Local producer is absent"))?;
        Poll::Ready(
            produce_local(producer, outcome)
                .map(StatementActionValue::Local)
                .map_err(service),
        )
    }

    fn disposition(&self) -> Option<LakePublicationDisposition> {
        let observer = self
            .wake
            .observation
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        observer.and_then(|source| source.publication_disposition())
    }
    fn local_health(&self) -> Option<StatementNativeHealthFailure> {
        self.io
            .as_ref()
            .and_then(|io| io.health_failure())
            .or_else(|| {
                self.cpu
                    .as_ref()
                    .and_then(|cpu| cpu.local_failure())
                    .map(|cause| {
                        StatementNativeHealthFailure::actual_exit_unproven(
                            "cleanup original CPU exit is unproven",
                            cause,
                        )
                    })
            })
    }
    fn poll_close(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        self.wake.waker.register(cx.waker());
        let Some(io) = self.io.as_ref() else {
            return Poll::Ready(Ok(()));
        };
        if let Some(observed) = io.withdraw_unstarted_in().map_err(health)? {
            if let Some(failure) = observed.into_ordinary_failure() {
                self.ordinary_io_failure.get_or_insert(failure);
            }
            return Poll::Ready(Ok(()));
        }
        if let Err(error) = io.poll_continuing(cx) {
            self.ordinary_io_failure.get_or_insert(error);
        }
        std::task::ready!(self.poll_product(cx));
        if !self.source_closed {
            let Ok(mut body) = self.body.try_lock() else {
                return Poll::Pending;
            };
            if let Some(source) = &body.session {
                std::task::ready!(source.poll_closed(cx)).map_err(health)?;
            }
            if let Some(cpu) = &body.cpu {
                std::task::ready!(cpu.poll_closed(cx)).map_err(health)?;
            }
            let retired = (
                body.session.take(),
                body.cpu.take(),
                body.start.take(),
                body.original.take(),
            );
            drop(body);
            let observation = self
                .wake
                .observation
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .take();
            // Clear only after actual source/CPU exit, with all foreign Drops
            // outside both receiving locks.
            drop(observation);
            drop(retired);
            drop(self.cpu.take());
            self.source_closed = true;
        }
        let observed = std::task::ready!(
            self.io
                .as_ref()
                .unwrap()
                .poll_actual_quiescent_continuing(cx)
        )?;
        if let Some(failure) = observed.into_ordinary_failure() {
            self.ordinary_io_failure.get_or_insert(failure);
        }
        Poll::Ready(Ok(()))
    }
}
impl GovernedStatementAction for Action {
    fn poll_action(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatementActionValue, QueryServiceError>> {
        self.state.as_mut().unwrap().poll_action(owner, cx)
    }
    fn external_effect_disposition(
        &self,
    ) -> Option<novarocks_query_application::api::StatementExternalEffectDisposition> {
        self.state
            .as_ref()
            .and_then(State::disposition)
            .map(crate::query::external_effect_from_lake)
    }
    fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        self.state.as_ref().and_then(State::local_health)
    }
    fn handoff_residual_in(
        &mut self,
        target: &mut Option<Box<dyn GovernedStatementResidual>>,
    ) -> Result<(), StatementNativeHealthFailure> {
        if target.is_some() {
            return Err(health(WorkError::Conflict));
        }
        if let Some(mut state) = self.state.take() {
            let local_delivery = (state.origin.take(), state.local.take());
            *target = Some(Box::new(Residual { state }));
            // Stable receiver precedes notification/last delivery alias Drop.
            drop(local_delivery);
        }
        Ok(())
    }
}
impl GovernedStatementResidual for Residual {
    fn poll_retirement(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        self.state.poll_close(cx)
    }
}
struct Effects {
    body: Arc<tokio::sync::Mutex<Body>>,
}
async fn run_product(
    body: Arc<tokio::sync::Mutex<Body>>,
) -> Result<MaintenanceActionOutcome, TerminalError> {
    let (product, target, older) = {
        let b = body.lock().await;
        (b.product.clone(), b.target.clone(), b.older_than)
    };
    product
        .execute_user_action(
            &Effects { body },
            MaintenanceActionRequest::RemoveOrphanFiles {
                target,
                older_than_ms: older,
            },
        )
        .await
}
impl TableMaintenanceEffectPort for Effects {
    fn reject_user_action_on_mv<'a>(
        &'a self,
        target: &'a MaintenanceTarget,
    ) -> MaintenanceFuture<'a, Result<(), novarocks_table_maintenance::runtime::TerminalError>>
    {
        Box::pin(reject_mv(self.body.clone(), target))
    }
    fn execute_metadata<'a>(
        &'a self,
        _request: MaintenanceActionRequest,
    ) -> MaintenanceFuture<'a, Result<MaintenanceActionOutcome, String>> {
        Box::pin(async { Err("cleanup Source cannot dispatch a metadata command".into()) })
    }
    fn begin_rewrite<'a>(
        &'a self,
        _target: &'a MaintenanceTarget,
        _intent: RewriteIntent,
    ) -> MaintenanceFuture<'a, Result<Box<dyn DistributedRewriteSession + 'a>, String>> {
        Box::pin(async { Err("cleanup Source cannot dispatch a rewrite".into()) })
    }

    fn execute_cleanup<'a>(
        &'a self,
        _target: &'a MaintenanceTarget,
        _older: i64,
    ) -> MaintenanceFuture<'a, Result<MaintenanceActionOutcome, TerminalError>> {
        Box::pin(execute_cleanup(self.body.clone()))
    }
}
async fn reject_mv(
    original: Arc<tokio::sync::Mutex<Body>>,
    target: &MaintenanceTarget,
) -> Result<(), TerminalError> {
    let body = original.lock().await;
    crate::mv::domain::iceberg_guard::reject_if_iceberg_mv_table_with_governed_planning_pin_and_context_async(body.engine.kernel.mv_storage_observation().as_ref(),&body.planning,body.engine.kernel.mv_readiness().as_ref(),&crate::catalog_application::resolver::TargetBackend{provider_id:body.cleanup_session.descriptor().provider_id.clone(),catalog:target.catalog.clone(),namespace:target.namespace.clone(),table:target.table.clone()},crate::mv::domain::iceberg_guard::IcebergMvUserMutation::Maintenance,body.context.clone(),body.cpu.as_ref().ok_or_else(|| TerminalError::pre_dispatch_failed("cleanup original metadata CPU is absent"))?.clone()).await.map_err(super::rewrite_job::guard_terminal_error)
}

async fn execute_cleanup(
    original: Arc<tokio::sync::Mutex<Body>>,
) -> Result<MaintenanceActionOutcome, TerminalError> {
    let mut body = original.lock().await;
    let Body {
        context,
        target,
        older_than,
        start,
        session,
        cpu,
        cleanup_session,
        original,
        wake,
        ..
    } = &mut *body;
    *start = Some(ConnectorCleanupAsyncStart {
        table: RequestScopedMaintenanceEngine::target_identity(target)?,
        operation_id: ConnectorCleanupOperationId::new(),
        older_than_ms: *older_than,
        context: context.clone(),
        original: original
            .as_ref()
            .ok_or_else(|| {
                TerminalError::pre_dispatch_failed("cleanup original paid scope is absent")
            })?
            .clone(),
    });
    cleanup_session
        .begin_async_in(
            start,
            cpu.as_ref()
                .ok_or_else(|| {
                    TerminalError::pre_dispatch_failed("cleanup original CPU port is absent")
                })?
                .clone(),
            session,
        )
        .map_err(TerminalError::from_original_cause)?;

    let source = session.as_ref().ok_or_else(|| {
        TerminalError::pre_dispatch_failed("cleanup original Source was not installed")
    })?;
    *wake.observation.lock().unwrap_or_else(|p| p.into_inner()) = Some(source.observation());

    let output = std::future::poll_fn(|cx| source.poll_output(cx)).await;
    // Keep the Product guard through actual source exit. An ordinary business
    // failure in the output is not proof that its physical future exited.
    let exit = std::future::poll_fn(|cx| source.poll_closed(cx)).await;
    let (disposition, applied) = source.publication_fact();
    output.map_err(|error| source_terminal(error, disposition, applied))?;
    exit.map_err(TerminalError::from_original_cause)?;
    Ok(MaintenanceActionOutcome::RemoveOrphanFiles {
        orphan_file_locations: Vec::new(),
    })
}

fn source_terminal(
    error: ConnectorError,
    disposition: Option<LakePublicationDisposition>,
    applied: bool,
) -> TerminalError {
    let detail = error.to_string();
    let mut terminal = match disposition {
        Some(LakePublicationDisposition::CommitUnknown) => TerminalError::commit_unknown(detail),
        Some(LakePublicationDisposition::KnownCommitted) => TerminalError::failed(detail),
        Some(LakePublicationDisposition::KnownUncommitted) => {
            TerminalError::known_uncommitted(detail)
        }
        None => TerminalError::pre_dispatch_failed(detail),
    }
    .with_failure_cause(Some(SharedFailureCause::from_static_detail(
        error,
        "cleanup original provider operation failed",
    )));
    if applied {
        terminal = terminal.with_effect(novarocks_coordination_contract::ExecutionEffect::External);
    } else if disposition != Some(LakePublicationDisposition::CommitUnknown) {
        terminal = terminal.with_effect(novarocks_coordination_contract::ExecutionEffect::None);
    }
    terminal
}

fn produce_local(
    producer: LocalResultProducer,
    outcome: MaintenanceActionOutcome,
) -> Result<novarocks_query_application::api::OwnedLocalResult, String> {
    producer.produce(
        || match crate::table_maintenance::result::action_result(outcome)? {
            super::MaintenanceStatementResult::Query(result) => Ok(result),
            _ => Err("cleanup result author returned no rows".into()),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_query_application::admitted_query_context::QueryResultCapacityBinding;
    use novarocks_workload_control::{
        ResultCapacityConfig, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
        WorkloadShutdownError,
    };

    #[test]
    fn cleanup_local_delivery_retires_without_retiring_the_same_root_data() {
        let control = WorkloadControl::try_new_counted(WorkloadConfig::default())
            .unwrap()
            .owner;
        control
            .configure_result_capacity(ResultCapacityConfig::V1)
            .unwrap();
        control.mark_ready().unwrap();
        let (root, local) = control
            .root_admission()
            .try_begin_root_with_result(
                WorkRequest::new(WorkClass::Management),
                ResultWindowClass::Local,
            )
            .unwrap();
        let occupied = control.snapshot().result_windows.held_positions;
        assert_eq!(occupied.iter().sum::<usize>(), 1);
        let retained = QueryResultCapacityBinding::from_root_owner(&root.owner).unwrap();
        assert!(retained.is_root_backed_internal());
        assert_eq!(
            control.snapshot().result_windows.held_positions,
            occupied,
            "root-only cleanup retention occupies no Internal computation position"
        );
        let allowance = retained.window_alias().reserve_payload_bytes(16).unwrap();
        let original_bytes = vec![0u8; 16];
        let producer = LocalResultProducer::try_new(
            &root.owner.scope(),
            local.retain_alias(),
            novarocks_result_contract::ClientPresentationFamily::TrinoJson(
                novarocks_result_contract::TrinoJsonFacts {
                    parametric_datetime: true,
                    variant: true,
                    variant_binary: true,
                },
            ),
        )
        .unwrap();
        let result = produce_local(
            producer,
            MaintenanceActionOutcome::RemoveOrphanFiles {
                orphan_file_locations: vec!["file:///original.parquet".to_owned()],
            },
        )
        .unwrap();
        assert_eq!(result.row_count(), 1);
        assert_eq!(result.columns().len(), 1);
        drop(local);
        drop(result);
        assert_eq!(
            control.snapshot().result_windows.held_positions,
            [0; 4],
            "cleanup residual retention cannot pin the Local delivery window"
        );
        assert_eq!(retained.window_alias().retained_payload_bytes(), 16);
        root.owner.complete();
        root.business.release();
        control.close_admission();
        let stopped = match control.shutdown() {
            Err(error) => error,
            Ok(_) => panic!("original retained data must prevent root drain"),
        };
        let (reason, control) = stopped.into_parts();
        assert_eq!(reason, WorkloadShutdownError::NotDrained);
        drop(original_bytes);
        drop(allowance);
        drop(retained);
        assert!(control.shutdown().is_ok());
    }
}
