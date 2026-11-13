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

//! ANALYZE runs under its original statement, permit and Internal window.
use super::super::application::{
    StatisticsTableTarget, StatisticsTargetCapture, StatisticsTargetResolver,
};
use super::*;
use novarocks_query_application::session_control::{
    GovernedStatementAction, GovernedStatementResidual, StatementActionValue,
    StatementCommandCompletion,
};
use novarocks_workload_control::ResultWindowClass;

type CaptureFuture = Pin<
    Box<dyn Future<Output = Result<StatisticsTargetCapture, StatisticsApplicationError>> + Send>,
>;

/// Role-local execution capability. It creates one original per-statement
/// Source; it has no job repository, queue or query permit transfer.
pub(crate) trait StatisticsStatementExecutor: Send + Sync {
    fn prepare_statement(
        &self,
        resolver: Arc<dyn StatisticsTargetResolver>,
        target: StatisticsTableTarget,
        columns: StatisticsColumnIntent,
        execution: &QueryExecutionContext,
        context: &ConnectorRequestContext,
    ) -> Result<Box<dyn GovernedStatementAction>, StatisticsApplicationError>;
}
impl StatisticsStatementExecutor for FrontendStatisticsAttemptExecutor {
    fn prepare_statement(
        &self,
        resolver: Arc<dyn StatisticsTargetResolver>,
        target: StatisticsTableTarget,
        columns: StatisticsColumnIntent,
        execution: &QueryExecutionContext,
        context: &ConnectorRequestContext,
    ) -> Result<Box<dyn GovernedStatementAction>, StatisticsApplicationError> {
        let fail = |e: WorkError| {
            StatisticsApplicationError::from_connector_error(
                novarocks_spi::connector::ConnectorError::compile_interruption(
                    novarocks_type_contract::CompileControlError::Source(
                        novarocks_failure_contract::SharedFailureCause::new(e),
                    ),
                ),
            )
        };
        let capacity = execution.result_capacity().ok_or_else(|| {
            StatisticsApplicationError::new("ANALYZE has no original statement capacity")
        })?;
        let origin = capacity.try_window_alias().map_err(fail)?;
        if !origin.is_for_scope(capacity.scope()) || origin.class() != ResultWindowClass::Internal {
            return Err(StatisticsApplicationError::new(
                "ANALYZE requires its original Internal window",
            ));
        }
        let planning = resolver.planning_pin(&target)?;
        let statistics_session = planning
            .derive_statistics_lease()
            .map_err(StatisticsApplicationError::from_connector_error)?;
        let profile = statistics_session
            .collection_io_profile()
            .map_err(StatisticsApplicationError::from_connector_error)?;
        let io = MetadataIoAdmission::new(
            capacity.scope().clone(),
            profile,
            std::mem::size_of::<StatementStatisticsAction>() as u64,
        )
        .map_err(fail)?;
        let input = Arc::new(StatementInput {
            ports: self.ports.clone(),
            resolver,
            target,
            columns,
            execution: execution.clone(),
            context: context.clone().with_io_admission(io.clone()),
            planning,
            statistics_session,
            publication_id: novarocks_spi::connector::LakePublicationId::new_v7(),
        });
        Ok(Box::new(StatementStatisticsAction {
            origin,
            io,
            input: Some(input),
            cpu: None,
            wake: Arc::new(StatisticsWake::default()),
            capture: None,
            captured: None,
            state: None,
            delivered: false,
            io_close_failure: None,
        }))
    }
}
struct StatementInput {
    ports: StatisticsAttemptExecutorPorts,
    resolver: Arc<dyn StatisticsTargetResolver>,
    target: StatisticsTableTarget,
    columns: StatisticsColumnIntent,
    execution: QueryExecutionContext,
    context: ConnectorRequestContext,
    planning: ConnectorControlPlanningPin,
    statistics_session: ConnectorStatisticsSession,
    publication_id: novarocks_spi::connector::LakePublicationId,
}
// Design: ADR-0232 (docs/adr/ADR-0232-statement-owned-maintenance-statistics-and-state-surfaces.md)
struct StatementStatisticsAction {
    origin: novarocks_workload_control::ResultWindowAlias,
    io: Arc<MetadataIoAdmission>,
    input: Option<Arc<StatementInput>>,
    cpu: Option<Arc<dyn StatisticsCpuPort>>,
    wake: Arc<StatisticsWake>,
    capture: Option<CaptureFuture>,
    captured: Option<Result<StatisticsTargetCapture, StatisticsApplicationError>>,
    state: Option<StatisticsState>,
    delivered: bool,
    io_close_failure: Option<WorkError>,
}
struct StatementResidual {
    original: StatementStatisticsAction,
}
fn scope_failure(error: WorkError) -> QueryServiceError {
    QueryServiceError::from_failure_cause(novarocks_failure_contract::SharedFailureCause::new(
        error,
    ))
}
impl StatementStatisticsAction {
    fn poll_start(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), QueryServiceError>> {
        if self.state.is_some() {
            return Poll::Ready(Ok(()));
        }
        self.wake.0.register(cx.waker());
        if !self.origin.is_for_scope(owner.scope()) {
            return Poll::Ready(Err(service_error(
                "ANALYZE changed its original statement Root",
            )));
        }
        let work = owner
            .execution_owner_slot()
            .as_ref()
            .ok_or_else(|| service_error("ANALYZE lost its original execution owner"))?;
        match self.io.poll_progress(work, cx) {
            Ok(true) => {}
            Ok(false) => return Poll::Pending,
            Err(error) => return Poll::Ready(Err(scope_failure(error))),
        }
        let input = self
            .input
            .as_ref()
            .ok_or_else(|| service_error("ANALYZE starting input is empty"))?
            .clone();
        if self.cpu.is_none() {
            let capacity = self.io.root_capacity().map_err(scope_failure)?;
            let runtime = input
                .ports
                .execution_runtime
                .0
                .provider_creator_cpu()
                .map_err(scope_failure)?;
            self.cpu = Some(
                runtime
                    .statistics_cpu_port(
                        owner.scope(),
                        capacity.window_alias(),
                        &input.statistics_session,
                        input.execution.cancellation().clone(),
                        input.ports.runtime.clone(),
                        self.wake.clone(),
                    )
                    .map_err(scope_failure)?,
            );
        }
        if self.capture.is_none() && self.captured.is_none() {
            let source = input.clone();
            let cpu = self.cpu.as_ref().unwrap().clone();
            self.capture = Some(Box::pin(async move {
                source
                    .resolver
                    .capture_table_object_async(
                        &source.target,
                        source.context.clone(),
                        &source.planning,
                        cpu,
                    )
                    .await
            }));
        }
        if let Some(capture) = self.capture.as_mut() {
            self.captured = Some(std::task::ready!(capture.as_mut().poll(cx)));
            drop(self.capture.take());
        }
        if let Some(Err(error)) = &self.captured {
            return Poll::Ready(Err(error.to_query_service_error()));
        }
        owner.scope().check().map_err(scope_failure)?;
        let initial = InitialComputationReservation::new(
            input
                .ports
                .launcher
                .reserve_identity()
                .map_err(service_error)?,
        )
        .map_err(service_error)?;
        let credentials = initial.credentials();
        let context = initial
            .connector_request_context(
                input.context.clone(),
                #[cfg(novarocks_dev_controls)]
                input.execution.test_statement_nomination(),
            )
            .map_err(service_error)?;
        let capacity = self.io.root_capacity().map_err(scope_failure)?;
        let execution = input
            .execution
            .for_statement_retention(capacity)
            .map_err(scope_failure)?;
        let captured = self
            .captured
            .take()
            .unwrap()
            .map_err(|e| e.to_query_service_error())?;
        let relation = crate::query_execution::statistics::StatisticsRelationIdentity::try_new(
            &input.target.catalog,
            &input.target.namespace,
            &input.target.table,
        )
        .map_err(service_error)?;
        let request = StatisticsAttemptRequest {
            operation_id: input.publication_id,
            connector_instance_id: captured.connector_instance_id,
            namespace: captured.namespace,
            table: captured.table,
            object_id: captured.object_id,
            columns: input.columns.clone(),
        };
        #[cfg(novarocks_dev_controls)]
        let observation = execution
            .test_statement_nomination()
            .and_then(|n| input.ports.launcher.resolve_statement_observation(n));
        self.state = Some(StatisticsState {
            ports: input.ports.clone(),
            root: owner.scope().clone(),
            execution,
            context,
            credentials,
            business_result: None,
            captured_binding: Some(captured.binding),
            cleanup_failure: None,
            relation,
            request: Some(request),
            planning_pin: Some(input.planning.clone()),
            statistics_session: Some(input.statistics_session.clone()),
            cpu: self.cpu.as_ref().unwrap().clone(),
            io: self.io.clone(),
            wake: self.wake.clone(),
            session: Arc::new(tokio::sync::Mutex::new(SessionSlot::default())),
            plan: Arc::new(tokio::sync::Mutex::new(PlanSlot::default())),
            provider: FrontendCompletionCustody::new(),
            phase: ActionPhase::Begin,
            pending: None,
            close: None,
            close_done: false,
            cleanup_observed: false,
            computation: StatementComputation::new(
                owner.scope().clone(),
                initial,
                input.ports.launcher.clone(),
                #[cfg(novarocks_dev_controls)]
                observation,
            ),
            native_tail: None,
            selected: false,
            disposition: None,
            failure: None,
            health: None,
            io_close_failure: None,
        });
        Poll::Ready(Ok(()))
    }
    fn poll_statement(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatementActionValue, QueryServiceError>> {
        if self.delivered {
            return Poll::Ready(Err(service_error("ANALYZE result was already delivered")));
        }
        std::task::ready!(self.poll_start(owner, cx))?;
        let state = self.state.as_mut().unwrap();
        std::task::ready!(state.poll_business(owner, cx))?;
        let result = state
            .business_result
            .take()
            .ok_or_else(|| service_error("ANALYZE has no original selected result"))?;
        self.delivered = true;
        if result.is_err() {
            let original = state.session.try_lock().map_err(service_error)?;
            if let Some(error) = &original.binding_failure {
                return Poll::Ready(Err(error.to_query_service_error()));
            }
            if let Some(error) = &original.provider_failure {
                return Poll::Ready(Err(StatisticsApplicationError::from_connector_error(
                    error.clone(),
                )
                .to_query_service_error()));
            }
        }
        let input = self.input.as_ref().unwrap();
        if let Some(error) = StatisticsApplicationError::from_statistics_attempt(
            input.publication_id,
            &input.target,
            &result,
        ) {
            return Poll::Ready(Err(error.to_query_service_error()));
        }
        Poll::Ready(Ok(StatementActionValue::Ok(
            StatementCommandCompletion::no_count(),
        )))
    }
    fn poll_exit(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        self.wake.0.register(cx.waker());
        if let Some(state) = self.state.as_mut() {
            return state.poll_close(cx);
        }
        if let Err(error) = self.io.poll_continuing(cx) {
            self.io_close_failure.get_or_insert(error);
        }
        if let Some(capture) = self.capture.as_mut() {
            self.captured = Some(std::task::ready!(capture.as_mut().poll(cx)));
            drop(self.capture.take());
        }
        if let Some(cpu) = &self.cpu {
            std::task::ready!(cpu.poll_closed(cx))
                .map_err(|e| health("ANALYZE capture CPU actual exit is unproven", Arc::new(e)))?;
        }
        match self
            .io
            .withdraw_unstarted_in()
            .map_err(|e| health("ANALYZE bootstrap withdrawal is unproven", Arc::new(e)))?
        {
            Some(observed) => {
                if let Some(failure) = observed.into_ordinary_failure() {
                    self.io_close_failure.get_or_insert(failure);
                }
            }
            None => {
                let observed = std::task::ready!(self.io.poll_actual_quiescent_continuing(cx))?;
                if let Some(failure) = observed.into_ordinary_failure() {
                    self.io_close_failure.get_or_insert(failure);
                }
            }
        }
        Poll::Ready(Ok(()))
    }
}
impl GovernedStatementAction for StatementStatisticsAction {
    fn poll_action(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatementActionValue, QueryServiceError>> {
        self.poll_statement(owner, cx)
    }
    fn external_effect_disposition(
        &self,
    ) -> Option<novarocks_query_application::api::StatementExternalEffectDisposition> {
        self.state
            .as_ref()
            .and_then(|state| state.disposition)
            .map(crate::query::external_effect_from_lake)
    }
    fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        self.state
            .as_ref()
            .and_then(|state| {
                state
                    .health
                    .clone()
                    .or_else(|| state.computation.local_health_failure())
            })
            .or_else(|| self.io.health_failure())
            .or_else(|| {
                self.cpu
                    .as_ref()
                    .and_then(|cpu| cpu.local_failure())
                    .map(|e| health("ANALYZE CPU actual exit is unproven", e))
            })
    }
    fn handoff_residual_in(
        &mut self,
        target: &mut Option<Box<dyn GovernedStatementResidual>>,
    ) -> Result<(), StatementNativeHealthFailure> {
        if target.is_some() {
            return Err(health(
                "ANALYZE residual receiver is occupied",
                Arc::new(WorkError::Conflict),
            ));
        }
        if let Some(state) = self.state.as_mut() {
            state.seal_credentials();
        }
        let original = Self {
            origin: self.origin.clone(),
            io: self.io.clone(),
            input: self.input.take(),
            cpu: self.cpu.take(),
            wake: self.wake.clone(),
            capture: self.capture.take(),
            captured: self.captured.take(),
            state: self.state.take(),
            delivered: self.delivered,
            io_close_failure: self.io_close_failure.take(),
        };
        *target = Some(Box::new(StatementResidual { original }));
        Ok(())
    }
}
impl GovernedStatementResidual for StatementResidual {
    fn poll_retirement(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        self.original.poll_exit(cx)
    }
}
