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

//! SHOW STATS runs in the original SQL Source, with its exact metadata binding.
use crate::statistics::application;
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
use std::{
    sync::{Arc, Mutex},
    time::Instant,
};

#[derive(Debug, Default)]
struct StatisticsReadWake(futures::task::AtomicWaker);
impl crate::wake::StatusIntakeWake for StatisticsReadWake {
    fn wake(&self) {
        self.0.wake();
    }
}

pub(crate) struct StatisticsReadService {
    target_resolver: Arc<dyn application::StatisticsTargetResolver>,
    blocking: crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor,
    runtime: tokio::runtime::Handle,
    views: Arc<dyn novarocks_query_application::view::ViewService>,
}
impl StatisticsReadService {
    pub(crate) fn new(
        target_resolver: Arc<dyn application::StatisticsTargetResolver>,
        blocking: crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor,
        runtime: tokio::runtime::Handle,
        views: Arc<dyn novarocks_query_application::view::ViewService>,
    ) -> Self {
        Self {
            target_resolver,
            blocking,
            runtime,
            views,
        }
    }
}

use crate::query_execution::maintenance::io_admission::MetadataIoAdmission;
use novarocks_failure_contract::FailureProjection;
use novarocks_query_application::{
    admitted_query_context::{QueryExecutionContext, QueryResultCapacityBinding},
    api::{LocalResultProducer, StatementNativeHealthFailure},
    session_control::{
        GovernedActionContext, GovernedStatementAction, GovernedStatementResidual,
        StatementActionValue,
    },
    session_error::{QueryServiceError, QueryServiceErrorKind},
};
use novarocks_spi::connector::{
    ConnectorInstanceId, ConnectorOperationControl, ConnectorOperationControlView,
    ConnectorOriginalResultScope, ConnectorOriginalResultScopeCheck, ConnectorRequestContext,
    ConnectorStatisticsSession, ConnectorTableIdentity, ConnectorTableRequest,
    ConnectorTableResolution, OriginalResultCheckClass as Class,
    OriginalResultCheckError as ScopeFailure, StatisticsCpuPort, StatisticsSummaryReadOriginal,
    TableStatisticsSummary,
};
use novarocks_workload_control::{ResultWindowAlias, WorkError};
use std::task::{Context, Poll};

struct ReadOriginalCheck {
    control: ConnectorOperationControlView,
    capacity: QueryResultCapacityBinding,
}
impl ConnectorOriginalResultScopeCheck for ReadOriginalCheck {
    fn original_deadline(&self) -> Instant {
        self.control.deadline()
    }
    fn check_active(&self) -> Result<(), ScopeFailure> {
        self.control
            .check_active()
            .map_err(|e| ScopeFailure::new(Class::Control, e))?;
        self.capacity
            .scope()
            .check()
            .map_err(|e| ScopeFailure::new(Class::Admission, e))
    }
}
struct StatisticsReadAction {
    family: novarocks_result_contract::ClientPresentationFamily,
    io: Arc<MetadataIoAdmission>,
    origin: ResultWindowAlias,
    scope: novarocks_workload_control::WorkScope,
    // This exact generation is kept from pure target resolution through close.
    planning: Option<novarocks_spi::connector::ConnectorControlPlanningPin>,
    statistics_session: Option<ConnectorStatisticsSession>,
    target: application::StatisticsTableTarget,
    context: Option<ConnectorRequestContext>,
    blocking: crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor,
    runtime: tokio::runtime::Handle,
    cancellation: novarocks_query_application::cancellation::QueryCancellationView,
    wake: Arc<StatisticsReadWake>,
    cpu: Option<Arc<dyn StatisticsCpuPort>>,
    metadata_cpu: Option<Arc<dyn novarocks_spi::connector::ProviderMetadataCpuPort>>,
    source: Option<Arc<dyn StatisticsSummaryReadOriginal>>,
    future: Option<novarocks_spi::connector::StatisticsSessionFuture<'static, ()>>,
    outcome: Option<Result<TableStatisticsSummary, novarocks_spi::connector::ConnectorError>>,
    receiver: Arc<Mutex<Option<Result<TableStatisticsSummary, ConnectorError>>>>,
    io_failure: Option<WorkError>,
    health_failure: Option<StatementNativeHealthFailure>,
    started: bool,
    read_finished: bool,
    closed: bool,
}
struct StatisticsReadResidual {
    original: StatisticsReadAction,
}
fn service_error(e: impl ToString) -> QueryServiceError {
    QueryServiceError::new(QueryServiceErrorKind::Internal, e.to_string())
}
fn work_error(error: WorkError) -> QueryServiceError {
    QueryServiceError::from_execution_failure(
        novarocks_query_application::execution_error::ExecutionFailureInput::new(
            error.failure_fact(),
            novarocks_execution_contract::SafeDetail::truncating(
                "statistics read original workload refused progress",
            ),
            novarocks_query_application::execution_error::ExecutionFailureSite::Coordinator,
        ),
    )
}
fn connector_error(error: novarocks_spi::connector::ConnectorError) -> QueryServiceError {
    if tracing::enabled!(tracing::Level::DEBUG) {
        tracing::debug!(fact = ?error.failure_fact(), error = %error, "Statistics original read failed");
        // Only closed first-party control authors supply private detail. Do not
        // invoke arbitrary SDK Display or use diagnostic text as machine facts.
        let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(&error);
        for depth in 0..16 {
            let Some(original) = cause else { break };
            if let Some(work) = original.downcast_ref::<WorkError>() {
                tracing::debug!(depth, fact = ?work.failure_fact(), error = %work, "Statistics original read workload refusal");
            }
            if let Some(check) = original.downcast_ref::<ScopeFailure>() {
                tracing::debug!(depth, class = ?check.class(), "Statistics original read scope refusal");
            }
            cause = original.source();
        }
    }
    QueryServiceError::from_failure_cause(
        novarocks_failure_contract::SharedFailureCause::from_static_detail(
            error,
            "statistics read source refused its original request",
        ),
    )
}
impl StatisticsReadService {
    pub(crate) fn prepare_action(
        &self,
        target: application::StatisticsTableTarget,
        execution: &QueryExecutionContext,
        context: &ConnectorRequestContext,
        family: novarocks_result_contract::ClientPresentationFamily,
    ) -> Result<Box<dyn GovernedStatementAction>, application::StatisticsApplicationError> {
        let fail = |e: WorkError| {
            application::StatisticsApplicationError::from_connector_error(
                ConnectorError::compile_interruption(
                    novarocks_type_contract::CompileControlError::Source(
                        novarocks_failure_contract::SharedFailureCause::new(e),
                    ),
                ),
            )
        };
        reject_system_relation(&target)
            .map_err(application::StatisticsApplicationError::from_connector_error)?;
        let views = self
            .views
            .catalog_snapshot()
            .map_err(application::StatisticsApplicationError::new)?;
        if views
            .get(&novarocks_types::naming::TableIdentity::new(
                &target.catalog,
                &target.namespace,
                &target.table,
            ))
            .is_some()
        {
            return Err(
                application::StatisticsApplicationError::from_connector_error(
                    unsupported_relation("views"),
                ),
            );
        }

        let capacity = execution.result_capacity().ok_or_else(|| {
            application::StatisticsApplicationError::new(
                "SHOW STATS FOR requires its original result capacity",
            )
        })?;
        let origin = capacity.try_window_alias().map_err(fail)?;
        let planning = self.target_resolver.planning_pin(&target)?;
        if planning.binding().descriptor().provider_id.as_str() != "iceberg" {
            return Err(
                application::StatisticsApplicationError::from_connector_error(
                    unsupported_relation("non-Iceberg tables"),
                ),
            );
        }

        let statistics_session = planning
            .derive_statistics_lease()
            .map_err(application::StatisticsApplicationError::from_connector_error)?;
        let profile = statistics_session
            .collection_io_profile()
            .map_err(application::StatisticsApplicationError::from_connector_error)?;
        // The same action's inline declaration is responsibility only.
        let metadata = std::mem::size_of::<StatisticsReadAction>() as u64;
        let io =
            MetadataIoAdmission::new(capacity.scope().clone(), profile, metadata).map_err(fail)?;
        let context = context.clone().with_io_admission(io.clone());
        Ok(Box::new(StatisticsReadAction {
            family,
            io,
            origin,
            scope: capacity.scope().clone(),
            planning: Some(planning),
            statistics_session: Some(statistics_session),
            target,
            context: Some(context),
            blocking: self.blocking.clone(),
            runtime: self.runtime.clone(),
            cancellation: execution.cancellation().clone(),
            wake: Arc::new(StatisticsReadWake::default()),
            cpu: None,
            metadata_cpu: None,
            source: None,
            future: None,
            outcome: None,
            receiver: Arc::new(Mutex::new(None)),
            io_failure: None,
            health_failure: None,
            started: false,
            read_finished: false,
            closed: false,
        }))
    }
}
impl StatisticsReadAction {
    fn start(&mut self) -> Result<(), QueryServiceError> {
        let (cpu, metadata) = self
            .blocking
            .provider_creator_cpu()
            .map_err(work_error)?
            .statistics_read_cpu_ports(
                self.origin_scope(),
                self.origin.clone(),
                self.statistics_session.as_ref().unwrap(),
                self.planning.as_ref().unwrap(),
                self.cancellation.clone(),
                self.runtime.clone(),
                self.wake.clone(),
            )
            .map_err(work_error)?;
        self.cpu = Some(cpu.clone());
        self.metadata_cpu = Some(metadata.clone());
        let check = Arc::new(ReadOriginalCheck {
            control: self.context.as_ref().unwrap().original_operation_control(),
            capacity: self.io.root_capacity().map_err(work_error)?,
        });
        let request = ConnectorTableRequest {
            table: ConnectorTableIdentity {
                instance_id: ConnectorInstanceId::parse(&self.target.catalog)
                    .map_err(service_error)?,
                namespace: Arc::from(self.target.namespace.as_str()),
                table: Arc::from(self.target.table.as_str()),
            },
            resolution: ConnectorTableResolution::StrictBaseTable,
            context: self.context.as_ref().unwrap().clone(),
        };
        self.source = Some(
            self.statistics_session
                .as_ref()
                .unwrap()
                .prepare_scoped_summary_read(
                    request,
                    ConnectorOriginalResultScope::from_original(check),
                )
                .map_err(connector_error)?,
        );
        // The whole Source is installed independently before the F exists.
        let source = self.source.as_ref().unwrap().clone();
        let planning = self.planning.as_ref().unwrap().clone();
        let target = self.target.clone();
        let context = self.context.as_ref().unwrap().clone();
        let receiver = self.receiver.clone();
        self.future = Some(Box::pin(async move {
            let result = read_summary(source, cpu, metadata, planning, target, context).await;
            let mut slot = receiver.lock().unwrap_or_else(|poison| poison.into_inner());
            assert!(slot.is_none(), "one original SHOW STATS result");
            *slot = Some(result);
            Ok(())
        }));
        Ok(())
    }
    fn origin_scope(&self) -> &novarocks_workload_control::WorkScope {
        // Original alias identity is checked by prepare and poll_action. CPU
        // admission still authenticates its bound scope and full D itself.
        &self.scope
    }
    fn poll_read(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), QueryServiceError>> {
        self.wake.0.register(cx.waker());
        if self.source.is_none() {
            self.start()?;
        }
        if let Some(future) = self.future.as_mut() {
            let value = std::task::ready!(future.as_mut().poll(cx));
            if let Err(error) = value {
                self.outcome = Some(Err(error));
            }
            self.read_finished = true;
            // No result alias is extracted until actual F destruction finishes.
            drop(self.future.take());
        }
        if let Some(cpu) = self.cpu.as_ref() {
            std::task::ready!(cpu.poll_closed(cx)).map_err(connector_error)?;
        }
        if self.outcome.is_none() {
            self.outcome = self
                .receiver
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .take();
            if self.outcome.is_none() {
                return Poll::Ready(Err(service_error("statistics summary receiver is empty")));
            }
        }
        Poll::Ready(Ok(()))
    }
}
impl GovernedStatementAction for StatisticsReadAction {
    fn poll_action(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatementActionValue, QueryServiceError>> {
        self.wake.0.register(cx.waker());
        if !self.origin.is_for_scope(owner.scope()) {
            return Poll::Ready(Err(service_error("statistics read has a foreign owner")));
        }
        let work = owner
            .execution_owner_slot()
            .as_ref()
            .ok_or_else(|| service_error("statistics read lost original owner"))?;
        if !self.started {
            self.started = self.io.poll_progress(work, cx).map_err(work_error)?;
        } else {
            self.io.poll_continuing(cx).map_err(work_error)?;
        }
        if !self.started {
            return Poll::Pending;
        }
        std::task::ready!(self.poll_read(cx))?;
        let observed = match std::task::ready!(self.io.poll_actual_quiescent_continuing(cx)) {
            Ok(observed) => observed,
            Err(failure) => {
                let error = service_error(failure.detail());
                self.health_failure = Some(failure);
                return Poll::Ready(Err(error));
            }
        };
        if let Some(failure) = observed.into_ordinary_failure() {
            self.io_failure.get_or_insert(failure);
        }
        self.closed = true;
        let evidence = self
            .outcome
            .take()
            .expect("original read evidence receiver")
            .map_err(|error| target_error(error, &self.target))?;
        if let Some(failure) = self.io_failure.as_ref() {
            return Poll::Ready(Err(work_error(failure.clone())));
        }
        let window = owner
            .result_window_alias()
            .ok_or_else(|| service_error("statistics read lost original window"))?;
        let producer = LocalResultProducer::try_new(owner.scope(), window, self.family)
            .map_err(service_error)?;
        Poll::Ready(
            producer
                .produce(|| result_from_summary(&evidence))
                .map(StatementActionValue::Local)
                .map_err(service_error),
        )
    }
    fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        self.health_failure
            .clone()
            .or_else(|| self.io.health_failure())
            .or_else(|| {
                self.cpu
                    .as_ref()
                    .and_then(|cpu| cpu.local_failure())
                    .map(|cause| {
                        StatementNativeHealthFailure::actual_exit_unproven(
                            "statistics read CPU actual exit failed",
                            cause,
                        )
                    })
            })
    }
    fn handoff_residual_in(
        &mut self,
        target: &mut Option<Box<dyn GovernedStatementResidual>>,
    ) -> Result<(), StatementNativeHealthFailure> {
        if target.is_some() {
            return Err(StatementNativeHealthFailure::actual_exit_unproven(
                "statistics read residual is occupied",
                Arc::new(WorkError::Conflict),
            ));
        }
        // Same stable action state, no worker/task or second original owner.
        *target = Some(Box::new(StatisticsReadResidual {
            original: StatisticsReadAction {
                family: self.family,
                io: self.io.clone(),
                origin: self.origin.clone(),
                scope: self.scope.clone(),
                planning: self.planning.take(),
                statistics_session: self.statistics_session.take(),
                target: std::mem::replace(
                    &mut self.target,
                    application::StatisticsTableTarget {
                        catalog: String::new(),
                        namespace: String::new(),
                        table: String::new(),
                    },
                ),
                context: self.context.take(),
                blocking: self.blocking.clone(),
                runtime: self.runtime.clone(),
                cancellation: self.cancellation.clone(),
                wake: self.wake.clone(),
                cpu: self.cpu.take(),
                metadata_cpu: self.metadata_cpu.take(),
                source: self.source.take(),
                future: self.future.take(),
                outcome: self.outcome.take(),
                receiver: self.receiver.clone(),
                io_failure: self.io_failure.take(),
                health_failure: self.health_failure.take(),
                started: self.started,
                read_finished: self.read_finished,
                closed: self.closed,
            },
        }));
        Ok(())
    }
}
impl StatisticsReadAction {
    fn dispose_inputs(&mut self) -> Result<(), StatementNativeHealthFailure> {
        if let Some(source) = self.source.as_ref() {
            source.close_original().map_err(|cause| {
                StatementNativeHealthFailure::actual_exit_unproven(
                    "statistics read input disposal is unproven",
                    Arc::new(cause),
                )
            })?;
        }
        // The same MIO and original source remain in this residual while
        // lease/context destructors run. A panic cannot refund their payload.
        drop((
            self.context.take(),
            self.statistics_session.take(),
            self.planning.take(),
        ));
        Ok(())
    }
}
impl GovernedStatementResidual for StatisticsReadResidual {
    fn poll_retirement(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        let original = &mut self.original;
        original.wake.0.register(cx.waker());
        if let Some(failure) = original.health_failure.as_ref() {
            return Poll::Ready(Err(failure.clone()));
        }
        if original.future.is_some()
            && !original.closed
            && let Err(error) = original.io.poll_continuing(cx)
        {
            original.io_failure.get_or_insert(error);
        }
        if let Some(future) = original.future.as_mut() {
            let result = std::task::ready!(future.as_mut().poll(cx));
            if let Err(error) = result {
                original.outcome = Some(Err(error));
            }
            original.read_finished = true;
            drop(original.future.take());
        }
        if let Some(cpu) = original.cpu.as_ref() {
            std::task::ready!(cpu.poll_closed(cx)).map_err(|e| {
                StatementNativeHealthFailure::actual_exit_unproven(
                    "statistics read CPU actual exit failed",
                    Arc::new(e),
                )
            })?;
        }
        std::task::ready!(poll_read_io_retirement(
            &original.io,
            &mut original.io_failure,
            &mut original.closed,
            cx
        ))?;
        original.dispose_inputs()?;
        original.closed = true;
        Poll::Ready(Ok(()))
    }
}

/// The same source's original bootstrap and request cells prove their own
/// retirement. Business refusal never substitutes for health or actual exit.
fn poll_read_io_retirement(
    io: &Arc<MetadataIoAdmission>,
    ordinary: &mut Option<WorkError>,
    closed: &mut bool,
    cx: &mut Context<'_>,
) -> Poll<Result<(), StatementNativeHealthFailure>> {
    if *closed {
        return Poll::Ready(Ok(()));
    }
    let observed = match io.withdraw_unstarted_in().map_err(|error| {
        StatementNativeHealthFailure::actual_exit_unproven(
            "statistics read bootstrap withdrawal is unproven",
            Arc::new(error),
        )
    })? {
        Some(observed) => observed,
        None => {
            if let Err(error) = io.poll_continuing(cx) {
                ordinary.get_or_insert(error);
            }
            std::task::ready!(io.poll_actual_quiescent_continuing(cx))?
        }
    };
    if let Some(failure) = observed.into_ordinary_failure() {
        ordinary.get_or_insert(failure);
    }
    *closed = true;
    Poll::Ready(Ok(()))
}

fn unsupported_relation(kind: &str) -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::Unsupported,
        format!("SHOW STATS FOR does not support {kind}"),
    )
}
fn reject_system_relation(
    target: &application::StatisticsTableTarget,
) -> Result<(), ConnectorError> {
    if target.catalog.eq_ignore_ascii_case("system")
        || target.namespace.eq_ignore_ascii_case("information_schema")
    {
        return Err(unsupported_relation("system relations"));
    }
    Ok(())
}
fn target_error(
    error: ConnectorError,
    target: &application::StatisticsTableTarget,
) -> QueryServiceError {
    if error.kind() == ConnectorErrorKind::NotFound {
        let user = novarocks_sql::analyze_error::AnalyzeError::unknown_table(
            format!(
                "Table '{}.{}.{}' does not exist",
                target.catalog, target.namespace, target.table
            ),
            novarocks_parser::Span::new(0, 0),
        )
        .to_user_error(None);
        QueryServiceError::from_user_error_with_cause(
            user,
            novarocks_failure_contract::SharedFailureCause::from_static_detail(
                error,
                "statistics target was not found",
            ),
        )
    } else {
        connector_error(error)
    }
}
fn result_from_summary(
    summary: &TableStatisticsSummary,
) -> Result<novarocks_query_application::api::QueryResult, String> {
    let batch = super::show_stats::render_show_stats(summary).map_err(|error| error.to_string())?;
    let columns = batch
        .schema()
        .fields()
        .iter()
        .map(|field| {
            novarocks_query_application::api::ResultField::new(
                field.name().as_str(),
                novarocks_type_contract::FunctionValueType::new(
                    field.data_type().clone(),
                    field.is_nullable(),
                ),
            )
        })
        .collect();
    novarocks_query_application::api::build_arrow_query_result(columns, batch.columns().to_vec())
}
async fn read_summary(
    source: Arc<dyn StatisticsSummaryReadOriginal>,
    cpu: Arc<dyn StatisticsCpuPort>,
    metadata: Arc<dyn novarocks_spi::connector::ProviderMetadataCpuPort>,
    planning: novarocks_spi::connector::ConnectorControlPlanningPin,
    target: application::StatisticsTableTarget,
    context: ConnectorRequestContext,
) -> Result<TableStatisticsSummary, ConnectorError> {
    let mut read = source.clone().read(cpu.clone(), metadata);
    let returned = read.as_mut().await;
    drop(read);
    returned?;
    std::future::poll_fn(|cx| cpu.poll_closed(cx)).await?;
    let mut received = None;
    source.take_result_in(&mut received)?;
    let result = match received.expect("original summary result") {
        Ok(result) => result,
        Err(error) if error.kind() == ConnectorErrorKind::NotFound => {
            // A table miss is not evidence of a view. Ask the original view
            // metadata author through the same exact generation and context.
            if let Some(views) = planning.binding().view_metadata() {
                let request = novarocks_spi::connector::ConnectorViewRequest {
                    view: novarocks_spi::connector::ConnectorViewIdentity {
                        instance_id: planning.binding().descriptor().instance_id.clone(),
                        namespace: target.namespace.as_str().into(),
                        view: target.table.as_str().into(),
                    },
                    context,
                };
                match views.prepare_view(request).await {
                    Ok(prepared) => {
                        drop(prepared);
                        return Err(unsupported_relation("views"));
                    }
                    Err(view_error)
                        if matches!(
                            view_error.kind(),
                            ConnectorErrorKind::NotFound | ConnectorErrorKind::Unsupported
                        ) => {}
                    Err(view_error) => return Err(view_error),
                }
            }
            return Err(error);
        }
        Err(error) => return Err(error),
    };
    let documents = planning.derive_document_storage_lease()?;
    let request = crate::mv::domain::staged_create::keyed_request(
        &documents,
        result.binding.metadata.identity.clone(),
        result.binding.object_id,
        context,
    );
    let view = documents
        .observe_keyed_documents_async(request)
        .await
        .map_err(|error| match error {
            novarocks_spi::connector::document_storage::KeyedDocumentReadError::Invalid(error) => {
                error
            }
            novarocks_spi::connector::document_storage::KeyedDocumentReadError::LegacyFormat {
                ..
            } => unsupported_relation("legacy application documents"),
        })?;
    if crate::mv::domain::iceberg_guard::is_materialized_view_documents(&view) {
        return Err(unsupported_relation("materialized views"));
    }
    Ok(result.summary)
}

#[cfg(test)]
mod tests;
