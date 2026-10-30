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

//! SHOW TABLE STATS runs in the original SQL Source, never the job worker.
use super::*;
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
    ConnectorStatisticsLease, ConnectorTableIdentity, ConnectorTableRequest,
    ConnectorTableResolution, OriginalResultCheckClass as Class,
    OriginalResultCheckError as ScopeFailure, StatisticsCpuPort, StatisticsEvidence,
    StatisticsReadOriginal,
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
    planning: Option<novarocks_spi::connector::ConnectorControlPlanningLease>,
    lease: Option<ConnectorStatisticsLease>,
    target: application::StatisticsTableTarget,
    context: Option<ConnectorRequestContext>,
    blocking: crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor,
    runtime: tokio::runtime::Handle,
    cancellation: novarocks_query_application::cancellation::QueryCancellationView,
    wake: Arc<StatisticsSubmitWake>,
    cpu: Option<Arc<dyn StatisticsCpuPort>>,
    metadata_cpu: Option<Arc<dyn novarocks_spi::connector::ProviderMetadataCpuPort>>,
    source: Option<Arc<dyn StatisticsReadOriginal>>,
    future: Option<novarocks_spi::connector::StatisticsSessionFuture<'static, ()>>,
    outcome: Option<Result<StatisticsEvidence, novarocks_spi::connector::ConnectorError>>,
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
    QueryServiceError::from_execution_failure(
        novarocks_query_application::execution_error::ExecutionFailureInput::new(
            error.failure_fact(),
            novarocks_execution_contract::SafeDetail::truncating(
                "statistics read source refused its original request",
            ),
            novarocks_query_application::execution_error::ExecutionFailureSite::Coordinator,
        ),
    )
}
impl FrontendStatisticsApplicationPort {
    pub(super) fn prepare_read_action(
        &self,
        target: application::StatisticsTableTarget,
        execution: &QueryExecutionContext,
        context: &ConnectorRequestContext,
        family: novarocks_result_contract::ClientPresentationFamily,
    ) -> Result<Box<dyn GovernedStatementAction>, application::StatisticsApplicationError> {
        let fail = |e: WorkError| application::StatisticsApplicationError::new(e.to_string());
        let capacity = execution.result_capacity().ok_or_else(|| {
            application::StatisticsApplicationError::new(
                "SHOW TABLE STATS requires its original result capacity",
            )
        })?;
        let origin = capacity.try_window_alias().map_err(fail)?;
        let planning = self.target_resolver.planning_lease(&target)?;
        let lease = planning
            .derive_statistics_lease()
            .map_err(application::StatisticsApplicationError::from_connector_error)?;
        let profile = lease
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
            lease: Some(lease),
            target,
            context: Some(context),
            blocking: self.blocking.clone(),
            runtime: self.runtime.clone(),
            cancellation: execution.cancellation().clone(),
            wake: Arc::new(StatisticsSubmitWake::default()),
            cpu: None,
            metadata_cpu: None,
            source: None,
            future: None,
            outcome: None,
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
                self.lease.as_ref().unwrap(),
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
            self.lease
                .as_ref()
                .unwrap()
                .prepare_scoped_read(request, ConnectorOriginalResultScope::from_original(check))
                .map_err(connector_error)?,
        );
        // The whole Source is installed independently before the F exists.
        self.future = Some(self.source.as_ref().unwrap().clone().read(cpu, metadata));
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
            self.source
                .as_ref()
                .unwrap()
                .take_result_in(&mut self.outcome)
                .map_err(connector_error)?;
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
            .map_err(connector_error)?;
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
                .produce(|| {
                    let version = evidence.data_version().clone();
                    let rows = evidence
                        .into_metrics()
                        .into_iter()
                        .map(|(metric, state)| {
                            application::statistics_table_stat_view(metric, state, &version)
                        })
                        .collect();
                    let result = crate::statistics::command::statistics_application_result(
                        application::StatisticsApplicationResult::TableStats(rows),
                    )?;
                    let novarocks_query_application::protocol_delivery::QuerySessionOutput::Query(
                        result,
                    ) = result
                    else {
                        return Err("statistics read produced a foreign result".into());
                    };
                    Ok(result)
                })
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
                lease: self.lease.take(),
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
        drop((self.context.take(), self.lease.take(), self.planning.take()));
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
        // No job was dispatched before positive MIO bootstrap. There is no F
        // to cancel/drop or a false worker-exit receipt to manufacture here.
        if !original.started {
            original.dispose_inputs()?;
            return Poll::Ready(Ok(()));
        }
        original.io.poll_continuing(cx).map_err(|e| {
            StatementNativeHealthFailure::actual_exit_unproven(
                "statistics read residual admission failed",
                Arc::new(e),
            )
        })?;
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
        let observed = std::task::ready!(original.io.poll_actual_quiescent_continuing(cx))?;
        if let Some(failure) = observed.into_ordinary_failure() {
            original.io_failure.get_or_insert(failure);
        }
        original.dispose_inputs()?;
        original.closed = true;
        Poll::Ready(Ok(()))
    }
}
