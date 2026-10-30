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

//! EXPLAIN runs in the original registered statement, not a second Work role.

use super::read_action::io_source::ReadMetadataIoSource;
use super::read_action::{
    ProviderClose, ReadNativeServices, ReadNativeStageTail, ReadNativeTail, close_provider,
    poll_provider_close,
};
use super::statement_computation::StatementStageAdmission;
use super::*;
use crate::query::compiler::{ExplainPreparationPorts, PreparedAsyncExplain, PreparedAsyncProfile};
use crate::query_execution::completion_facts::FrontendPlanningCpuSource;
use crate::query_execution::logical_execution::OriginalComputationStart;
use novarocks_query_application::admitted_query_context::QueryResultCapacityBinding;
use novarocks_query_application::api::StatementNativeHealthFailure;
use novarocks_query_application::api::{ExecutionHandle, LocalResultProducer, OwnedLocalResult};
use novarocks_query_application::session_control::{
    GovernedActionContext, GovernedQueryStatementOwner, GovernedStatementAction,
    GovernedStatementResidual, StatementActionValue,
};
use novarocks_spi::connector::{
    ConnectorIoAdmissionPort, ConnectorMetadataMaintenanceIoProfile, SharedDependency,
};
use novarocks_state_store_api::StateStoreExecutionRequests;
use novarocks_workload_control::WorkError;
use std::task::{Context, Poll};

type PreparationFuture = Pin<
    Box<
        dyn Future<
                Output = Result<
                    Result<PreparedExplain, QueryServiceError>,
                    StatementNativeHealthFailure,
                >,
            > + Send,
    >,
>;
type DiagnosticFuture =
    Pin<Box<dyn Future<Output = Result<QueryResult, QueryServiceError>> + Send>>;

struct ExplainPreparationInput {
    compiler: FrontendQueryCompiler,
    topology: BackendTopologyService,
    role: ClusterRole,
    optimizer_limit: u64,
    sql: String,
    explain: ast::ExplainQuery,
    state: SessionSqlState,
    deadline: Option<Instant>,
    cancellation: QueryCancellationView,
    scope: novarocks_workload_control::WorkScope,
    window: ResultWindowAlias,
    provider_source: Arc<crate::query_execution::completion_facts::FrontendCompletionCustody>,
    planning_cpu: Arc<FrontendPlanningCpuSource>,
    io: Arc<ReadMetadataIoSource>,
    state_store: Option<Arc<dyn StateStoreExecutionRequests>>,
    #[cfg(novarocks_dev_controls)]
    observation: Option<crate::preparation_diagnostics::StatementObservationHandle>,
}
struct PreparedExplain {
    value: PreparedAsyncExplain,
}
async fn make_explain_preparation(
    input: ExplainPreparationInput,
) -> Result<Result<PreparedExplain, QueryServiceError>, StatementNativeHealthFailure> {
    let prepared: Result<_, QueryServiceError> = async {
        if input.cancellation.is_cancelled() {
            return Err(internal_error(
                "typed statement was cancelled before query compilation began",
            ));
        }
        let topology = input
            .topology
            .snapshot()
            .map_err(|e| internal_error(e.to_string()))?;
        let topology = if topology.targets().is_empty() {
            wait_for_eligible_query_topology_after(
                &input.topology,
                &input.cancellation,
                topology.revision(),
                input
                    .deadline
                    .unwrap_or_else(|| Instant::now() + Duration::from_secs(30)),
            )
            .await?
        } else {
            topology
        };
        let presentation = *input.state.presentation();
        let (catalog, database, execution, mut optimizer, semantics) =
            input.state.into_query_attempt_inputs();
        let semantics = novarocks_sql::sql_mode::statement_sql_semantics(
            &semantics,
            &ParsedStatement::ExplainQuery(input.explain.clone()),
        )
        .map_err(|e| QueryServiceError::from_user_error(e.to_user_error(Some(&input.sql))))?;
        if optimizer.optimizer_query_mem_limit_bytes.is_none() {
            optimizer.optimizer_query_mem_limit_bytes = Some(input.optimizer_limit as f64);
        }
        let options = with_query_hints(
            query_options_from_session_settings(&execution),
            &semantics,
            Some(&input.explain.query),
        );
        let capacity = QueryResultCapacityBinding::try_new(&input.scope, input.window)
            .map_err(|e| internal_error(e.to_string()))?;
        let context = RequestContext::admit(RequestAdmission::new(
            catalog,
            database,
            input.role,
            topology,
            input.deadline,
            input.cancellation,
            optimizer,
            semantics,
            presentation,
        ))
        .with_result_capacity(capacity.clone())
        .map_err(|e| internal_error(e.to_string()))?;
        #[cfg(novarocks_dev_controls)]
        let context = match input
            .observation
            .as_ref()
            .and_then(|handle| handle.nomination())
        {
            Some(nomination) => context
                .with_test_statement_nomination(Arc::new(nomination))
                .map_err(internal_error)?,
            None => context,
        };
        Ok((context, options))
    }
    .await;
    let (context, options) = match prepared {
        Ok(value) => value,
        Err(error) => return Ok(Err(error)),
    };
    let value = input
        .compiler
        .prepare_explain_async(
            &input.explain,
            &context,
            Some(options),
            &input.scope,
            ExplainPreparationPorts {
                provider: &input.provider_source,
                cpu: input.planning_cpu.as_ref(),
                io: input.io,
                state_store: input.state_store,
            },
        )
        .await?;
    Ok(value
        .map(|value| PreparedExplain { value })
        .map_err(|e| routed_execution_service_error(routed_compiler_failure(e, &input.sql))))
}
struct ProfileDiagnosticInput {
    ready: crate::query_execution::internal_consumer::profile::ReadyProfileComputation,
    completion: crate::query_execution::completion::PreparedQueryCompletion,
}
async fn make_profile_diagnostics(
    mut input: ProfileDiagnosticInput,
) -> Result<QueryResult, QueryServiceError> {
    let snapshot = input.ready.wait_diagnostics().await;
    input
        .completion
        .complete_profile_observation(&input.ready, snapshot.as_deref().map_err(|e| *e))
        .map_err(internal_error)
}

struct ExplainStatementAction {
    family: novarocks_result_contract::ClientPresentationFamily,
    // Outside the polled preparation F: async unwind may destroy its fields.
    provider_source: Arc<crate::query_execution::completion_facts::FrontendCompletionCustody>,
    planning_cpu: Arc<FrontendPlanningCpuSource>,
    preparation_health: Option<StatementNativeHealthFailure>,
    provider_close: Option<ProviderClose>,
    provider_close_result: Option<Result<(), StatementNativeHealthFailure>>,
    tail: Option<Box<ReadNativeTail>>,
    bootstrap_ready: bool,
    result_transferred: bool,
    error: Option<QueryServiceError>,
    preparation: Option<PreparationFuture>,
    preparation_started: bool,
    prepared: Option<Result<PreparedExplain, QueryServiceError>>,
    profile: Option<Box<PreparedAsyncProfile>>,
    capacity: Option<QueryResultCapacityBinding>,
    original: OriginalComputationStart,
    execution: Option<Result<ExecutionHandle, QueryExecutionError>>,
    diagnostics: Option<DiagnosticFuture>,
    raw_result: Option<Result<QueryResult, QueryServiceError>>,
    local: Option<OwnedLocalResult>,
    launcher: Arc<dyn LogicalExecutionLauncher>,
    #[cfg(novarocks_dev_controls)]
    observation: Option<crate::preparation_diagnostics::StatementObservationHandle>,
}
impl GovernedStatementAction for ExplainStatementAction {
    fn local_health_failure(
        &self,
    ) -> Option<novarocks_query_application::api::StatementNativeHealthFailure> {
        self.preparation_health
            .clone()
            .or_else(|| self.planning_cpu.local_health_failure())
            .or_else(|| self.provider_source.local_health_failure())
            .or_else(|| {
                self.provider_close_result
                    .as_ref()
                    .and_then(|result| result.as_ref().err().cloned())
            })
            .or_else(|| {
                self.tail.as_ref().and_then(|tail| {
                    tail.health
                        .clone()
                        .or_else(|| tail.services.io().health_failure())
                        .or_else(|| tail.native.stage.local_health_failure())
                })
            })
    }
    fn handoff_residual_in(
        &mut self,
        target: &mut Option<Box<dyn GovernedStatementResidual>>,
    ) -> Result<(), StatementNativeHealthFailure> {
        if self.result_transferred && self.tail.is_some() {
            if target.is_some() {
                return Err(StatementNativeHealthFailure::actual_exit_unproven(
                    "EXPLAIN residual receiver is occupied",
                    Arc::new(WorkError::Conflict),
                ));
            }
            *target = self
                .tail
                .take()
                .map(|tail| tail as Box<dyn GovernedStatementResidual>);
        }
        Ok(())
    }
    fn poll_action(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatementActionValue, QueryServiceError>> {
        if let Some(tail) = self.tail.as_mut() {
            tail.services.poll_state_store(cx, &mut tail.health);
        }
        let polled = self.poll_explain(owner, cx);
        if let Some(tail) = self.tail.as_mut() {
            tail.services.poll_state_store(cx, &mut tail.health);
        }
        let result = match polled {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(result) => result,
        };
        let cancellation =
            QueryCancellationView::governed(owner.cancellation().clone(), owner.timeout_ms());
        let Some(reason) = cancellation.reason() else {
            return Poll::Ready(result);
        };
        // Preserve an already selected execution fact; otherwise retain the
        // original public cancellation policy and its compile-control sidecar.
        Poll::Ready(match result {
            Err(error) if error.execution_failure().is_some() => Err(error),
            Err(error) => {
                Err(cancellation_error(reason).with_compile_control(error.compile_control_error()))
            }
            Ok(value) => {
                drop(value);
                Err(cancellation_error(reason))
            }
        })
    }
}
impl ExplainStatementAction {
    fn poll_explain(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatementActionValue, QueryServiceError>> {
        if self.local_health_failure().is_some() {
            return Poll::Pending;
        }
        if !self.preparation_started && owner.cancellation().reason().is_some() {
            self.error.get_or_insert_with(|| {
                internal_error(
                    "typed statement was cancelled before prepared query execution began",
                )
            });
        }
        if let Some(tail) = self.tail.as_mut() {
            let work = owner
                .execution_owner_slot()
                .as_ref()
                .expect("EXPLAIN retains its original Root");
            match tail.services.io().poll_progress(work, cx) {
                Ok(ready) => self.bootstrap_ready |= ready,
                Err(error) => {
                    self.error.get_or_insert_with(|| {
                        QueryServiceError::from_failure_cause(
                            novarocks_failure_contract::SharedFailureCause::new(error),
                        )
                    });
                }
            }
        }
        if !self.bootstrap_ready && self.error.is_none() {
            return Poll::Pending;
        }
        // Once polled, preparation remains in this Source through its actual
        // return and completed-F Drop, including cancellation and business Err.
        if self.error.is_some() && !self.preparation_started {
            drop(self.preparation.take());
        }
        if let Some(future) = self.preparation.as_mut() {
            self.preparation_started = true;
            match future.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(value)) => self.prepared = Some(value),
                Poll::Ready(Err(health)) => {
                    self.preparation_health = Some(health);
                    return Poll::Pending;
                }
            }
            if self.provider_source.local_health_failure().is_some() {
                return Poll::Pending;
            }
            drop(self.preparation.take());
        }
        match poll_provider_close(
            &mut self.provider_close,
            &mut self.provider_close_result,
            cx,
        ) {
            Poll::Pending | Poll::Ready(Err(_)) => return Poll::Pending,
            Poll::Ready(Ok(())) => {}
        }
        if let Some(prepared) = self.prepared.take() {
            match prepared {
                Err(error) => {
                    self.error.get_or_insert(error);
                }
                Ok(PreparedExplain {
                    value: PreparedAsyncExplain::Lines(lines),
                }) => {
                    self.raw_result = Some(
                        build_string_query_result("Explain String", lines).map_err(internal_error),
                    );
                }
                Ok(PreparedExplain {
                    value: PreparedAsyncExplain::Profile(profile),
                }) => {
                    self.profile = Some(profile);
                }
            }
        }
        if self.error.is_none()
            && let Some(error) = self
                .tail
                .as_ref()
                .and_then(|tail| tail.services.io().original())
                .and_then(|io| io.failure())
        {
            self.error = Some(QueryServiceError::from_failure_cause(
                novarocks_failure_contract::SharedFailureCause::new(error),
            ));
        }
        if self.error.is_none()
            && self
                .profile
                .as_ref()
                .is_some_and(|profile| profile.computation.is_some())
        {
            if owner.cancellation().reason().is_some() {
                self.error = Some(internal_error(
                    "typed statement was cancelled before prepared query execution began",
                ));
            } else {
                let tail = self
                    .tail
                    .as_mut()
                    .expect("Profile has its original metadata and stage Source");
                if !tail.services.state_store_quiescent(cx, &mut tail.health) {
                    return Poll::Pending;
                }
                let io = tail
                    .services
                    .io()
                    .original()
                    .expect("EXPLAIN preparation installed original metadata source");
                let work = owner
                    .execution_owner_slot()
                    .as_ref()
                    .expect("same EXPLAIN Root");
                match tail.native.stage.poll_admit(&io, work, cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(error)) => self.error = Some(error),
                    Poll::Ready(Ok(())) => {}
                }
                if self.error.is_none()
                    && self.capacity.is_none()
                    && let Err(error) = tail
                        .native
                        .stage
                        .install_internal_read_capacity_in(&io, &mut self.capacity)
                {
                    self.error = Some(error);
                }
                if self.error.is_none() {
                    match self.launcher.start_computation_in(
                        &mut self.profile.as_mut().expect("original Profile").computation,
                        tail.native.stage.owner_slot(),
                        self.capacity
                            .as_ref()
                            .expect("original positive Internal stage capacity"),
                        &io,
                        &mut self.original,
                        #[cfg(novarocks_dev_controls)]
                        self.observation.clone(),
                    ) {
                        Err(error) => self.error = Some(governed_query_execution_error(error)),
                        Ok(notification) => {
                            tail.native.observe_start_in(
                                &notification,
                                &mut self.original.exit,
                                &mut tail.health,
                            );
                            if let Err(error) = notification.finish() {
                                self.error = Some(governed_query_execution_error(error));
                            }
                        }
                    }
                }
            }
        }
        if let Some(future) = self.original.ready.as_mut() {
            match future.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(value) => self.execution = Some(value),
            }
            drop(self.original.ready.take());
        }
        if let Some(execution) = self.execution.take() {
            match execution {
                Err(error) => {
                    self.error
                        .get_or_insert_with(|| governed_query_execution_error(error));
                }
                Ok(execution) => {
                    let ready = match execution
                        .ready_token()
                        .copied()
                        .ok_or_else(|| internal_error("Profile has no computation Ready token"))
                        .and_then(|token| {
                            self.profile
                                .as_mut()
                                .expect("started Profile")
                                .output
                                .take_ready(token)
                                .map_err(governed_query_execution_error)
                        }) {
                        Ok(ready) => Some(ready),
                        Err(error) => {
                            self.error.get_or_insert(error);
                            None
                        }
                    };
                    if let Some(ready) = ready {
                        let profile = *self.profile.take().expect("same ready Profile");
                        self.diagnostics =
                            Some(Box::pin(make_profile_diagnostics(ProfileDiagnosticInput {
                                ready,
                                completion: profile.completion,
                            })));
                    }
                    drop(execution);
                }
            }
        }
        if let Some(future) = self.diagnostics.as_mut() {
            match future.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(value) => self.raw_result = Some(value),
            }
            drop(self.diagnostics.take());
        }
        if self.error.is_none()
            && self.local.is_none()
            && let Some(result) = self.raw_result.take()
        {
            match result {
                Err(error) => self.error = Some(error),
                Ok(result) => {
                    let local = owner
                        .result_window_alias()
                        .ok_or_else(|| internal_error("EXPLAIN has no original result window"))
                        .and_then(|window| {
                            LocalResultProducer::try_new(owner.scope(), window, self.family)
                                .map_err(internal_error)
                        })
                        .and_then(|producer| {
                            producer.produce(|| Ok(result)).map_err(internal_error)
                        });
                    match local {
                        Ok(local) => self.local = Some(local),
                        Err(error) => self.error = Some(error),
                    }
                }
            }
        }
        if self.error.is_some() || self.local.is_some() {
            if let Some(tail) = self.tail.as_mut() {
                if !tail.native.is_launched() {
                    drop(self.original.input.take());
                    drop(self.original.request.take());
                    drop(self.profile.take());
                }
                drop(self.capacity.take());
                match tail.poll_business_exit(cx) {
                    Poll::Pending | Poll::Ready(Err(_)) => return Poll::Pending,
                    Poll::Ready(Ok(())) => {}
                }
                if let Some(error) = tail.services.io().original().and_then(|io| io.failure()) {
                    self.error.get_or_insert_with(|| {
                        QueryServiceError::from_failure_cause(
                            novarocks_failure_contract::SharedFailureCause::new(error),
                        )
                    });
                }
            }
            self.result_transferred = true;
            if let Some(error) = self.error.take() {
                return Poll::Ready(Err(error));
            }
            return Poll::Ready(Ok(StatementActionValue::Local(
                self.local.take().expect("sealed original EXPLAIN graph"),
            )));
        }
        Poll::Pending
    }
}

impl FrontendQuerySession {
    pub(super) async fn execute_observed_explain(
        &self,
        sql: String,
        explain: ast::ExplainQuery,
        state: SessionSqlState,
        deadline: Option<Instant>,
        statement: GovernedQueryStatementOwner,
        #[cfg(novarocks_dev_controls)] observation: Option<
            crate::preparation_diagnostics::StatementObservationHandle,
        >,
    ) -> Result<StatementResult, QueryServiceError> {
        let window = match statement.result_window_alias() {
            Some(window) => window,
            None => {
                return Ok(self.governed_typed_error(
                    internal_error("EXPLAIN has no original result window"),
                    statement,
                ));
            }
        };
        self.service
            .query_compiler
            .validate_original_read_runtime()
            .map_err(|error| internal_error(error.to_string()))?;
        let cancellation = QueryCancellationView::governed(
            statement.cancellation().clone(),
            statement.timeout_ms(),
        );
        let state_store = self
            .service
            .state_store
            .as_ref()
            .map(|binding| {
                let owner = binding.for_root(statement.scope().clone())?;
                let requests: Arc<dyn StateStoreExecutionRequests> =
                    Arc::new(owner.execution_requests(binding.transaction_deadline, deadline)?);
                Ok::<_, novarocks_state_store_api::StateStoreExecutionFailure>((owner, requests))
            })
            .transpose()
            .map_err(|cause| {
                QueryServiceError::from_failure_cause(cause)
                    .reconcile_local_stop_observation(&cancellation)
            })?;
        let dependencies = self
            .service
            .state_store
            .as_ref()
            .map_or(&[][..], |binding| binding.blocking_dependency_facts());
        let profile = explain_preparation_profile(dependencies)
            .map_err(|error| internal_error(error.to_string()))?;
        let io = ReadMetadataIoSource::new(statement.scope().clone(), profile.clone());
        io.configure_read_source(&profile)
            .map_err(|error| internal_error(error.to_string()))?;
        let (state_store_owner, state_store_requests) = match state_store {
            Some((owner, requests)) => (Some(owner), Some(requests)),
            None => (None, None),
        };
        let stage = StatementStageAdmission::new(statement.scope().clone(), 0);
        let provider_source =
            crate::query_execution::completion_facts::FrontendCompletionCustody::new();
        let planning_cpu = self.service.query_compiler.new_planning_cpu_source();
        let family = *state.presentation();
        let input = ExplainPreparationInput {
            compiler: self.service.query_compiler.clone(),
            topology: self.service.topology.clone(),
            role: self.service.role,
            optimizer_limit: self.service.optimizer_query_mem_limit_bytes,
            sql,
            explain,
            state,
            deadline,
            cancellation: QueryCancellationView::governed(
                statement.cancellation().clone(),
                statement.timeout_ms(),
            ),
            scope: statement.scope().clone(),
            window,
            provider_source: Arc::clone(&provider_source),
            planning_cpu: Arc::clone(&planning_cpu),
            io: Arc::clone(&io),
            state_store: state_store_requests,
            #[cfg(novarocks_dev_controls)]
            observation: observation.clone(),
        };
        let provider_close = Some(Box::pin(close_provider(
            Arc::clone(&provider_source),
            Arc::clone(&planning_cpu),
        )) as ProviderClose);
        let action = Box::new(ExplainStatementAction {
            family,
            provider_source,
            planning_cpu,
            preparation_health: None,
            provider_close,
            provider_close_result: None,
            tail: Some(Box::new(ReadNativeTail {
                services: ReadNativeServices::new(state_store_owner, io),
                native: ReadNativeStageTail::new(stage),
                health: None,
            })),
            bootstrap_ready: false,
            result_transferred: false,
            error: None,
            preparation: Some(Box::pin(make_explain_preparation(input))),
            preparation_started: false,
            prepared: None,
            profile: None,
            capacity: None,
            original: OriginalComputationStart::default(),
            execution: None,
            diagnostics: None,
            raw_result: None,
            local: None,
            launcher: Arc::clone(&self.service.logical_execution_launcher),
            #[cfg(novarocks_dev_controls)]
            observation,
        });
        self.execute_observed_statement_action(statement, action)
            .await
    }
}

fn explain_preparation_profile(
    dependencies: &[SharedDependency],
) -> Result<ConnectorMetadataMaintenanceIoProfile, WorkError> {
    let mut dependencies = dependencies.to_vec();
    if !dependencies.contains(&SharedDependency::LocalHost) {
        dependencies.push(SharedDependency::LocalHost);
    }
    ConnectorMetadataMaintenanceIoProfile::try_new(
        std::num::NonZeroUsize::new(1).unwrap(),
        std::num::NonZeroU64::new(crate::query_execution::maintenance::io_admission::MetadataIoAdmission::request_metadata_bytes())
            .ok_or(WorkError::ArithmeticOverflow)?,
        std::num::NonZeroU64::new(std::mem::size_of::<ExplainPreparationInput>() as u64)
            .ok_or(WorkError::ArithmeticOverflow)?,
        dependencies,
    ).map_err(|_| WorkError::InvalidConfig("local EXPLAIN preparation source declaration"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_query_application::{
        api::ReservedExecutionIdentity, client_connection::ClientConnectionToken,
        query_control::QueryApplicationControl, session_control::StatementActionReady,
    };
    use novarocks_workload_control::{
        ResourceConfig, ResultCapacityConfig, ResultCapacityHandle, WorkloadConfig, WorkloadControl,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct NoNativeLaunch;
    impl LogicalExecutionLauncher for NoNativeLaunch {
        #[cfg(novarocks_dev_controls)]
        fn resolve_statement_observation(
            &self,
            _: &novarocks_failpoint::target_binding::StatementWorkNomination,
        ) -> Option<crate::preparation_diagnostics::StatementObservationHandle> {
            panic!("this fixture cannot resolve a Native statement observation")
        }

        fn reserve_identity(&self) -> Result<ReservedExecutionIdentity, QueryExecutionError> {
            panic!("Local EXPLAIN cannot reserve Native work")
        }
        fn start_read_in(
            &self,
            _: &mut Option<crate::query_execution::completion::PreparedLogicalRead>,
            _: &mut Option<novarocks_workload_control::WorkOwner>,
            _: &QueryResultCapacityBinding,
            _: &Arc<crate::query::read_action::io_source::ReadMetadataIoSource>,
            _: &mut OriginalComputationStart,
            #[cfg(novarocks_dev_controls)] _: Option<
                crate::preparation_diagnostics::StatementObservationHandle,
            >,
        ) -> Result<
            novarocks_query_application::api::LogicalExecutionStartAttempt,
            QueryExecutionError,
        > {
            panic!("EXPLAIN fixture cannot start ordinary Read")
        }
        fn start_computation_in(
            &self,
            _: &mut Option<crate::query_execution::logical_execution::PreparedLogicalComputation>,
            _: &mut Option<WorkOwner>,
            _: &QueryResultCapacityBinding,
            _: &Arc<crate::query_execution::maintenance::io_admission::MetadataIoAdmission>,
            _: &mut OriginalComputationStart,
            #[cfg(novarocks_dev_controls)] _: Option<
                crate::preparation_diagnostics::StatementObservationHandle,
            >,
        ) -> Result<
            novarocks_query_application::api::LogicalExecutionStartAttempt,
            QueryExecutionError,
        > {
            panic!("Local EXPLAIN cannot start Native work")
        }
    }
    async fn statement() -> (
        WorkloadControl,
        ResultCapacityHandle,
        GovernedQueryStatementOwner,
        novarocks_query_application::session_control::QuerySessionLease,
    ) {
        let control = Arc::new(QueryApplicationControl::default());
        let service = QueryControlService::new(control);
        let session = service
            .register_session(SessionIdentity::new(
                ClientConnectionToken::new(7, 1).unwrap(),
                "root",
            ))
            .unwrap();
        let workload = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024 * 1024,
                control_bytes: 1024,
                per_scope_bytes: 1024 * 1024 - 1024,
            },
        )
        .unwrap();
        let capacity = workload
            .configure_result_capacity(ResultCapacityConfig::V1)
            .unwrap();
        workload.mark_ready().unwrap();
        let owner = service
            .begin_queued_governed_query_statement_with_result(
                session.token(),
                &workload.root_admission(),
                None,
                None,
                None,
                ResultWindowClass::Local,
            )
            .await
            .unwrap();
        (workload, capacity, owner, session)
    }
    fn action(preparation: PreparationFuture) -> ExplainStatementAction {
        ExplainStatementAction {
            family: novarocks_result_contract::ClientPresentationFamily::MysqlText,
            provider_source:
                crate::query_execution::completion_facts::FrontendCompletionCustody::new(),
            planning_cpu: FrontendPlanningCpuSource::new(
                crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor::new(
                    tokio::runtime::Handle::current(),
                ),
            ),
            preparation_health: None,
            provider_close: None,
            provider_close_result: Some(Ok(())),
            tail: None,
            bootstrap_ready: true,
            result_transferred: false,
            error: None,
            preparation: Some(preparation),
            preparation_started: false,
            prepared: None,
            profile: None,
            capacity: None,
            original: OriginalComputationStart::default(),
            execution: None,
            diagnostics: None,
            raw_result: None,
            local: None,
            launcher: Arc::new(NoNativeLaunch),
            #[cfg(novarocks_dev_controls)]
            observation: None,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn prepared_explain_wait_leaves_native_runtime_free_to_poll_first_send() {
        let (_workload, capacity, mut owner, _session) = statement().await;
        let (send, receive) = futures::channel::oneshot::channel();
        let mut action = action(Box::pin(async move {
            receive.await.unwrap();
            Ok(Ok(PreparedExplain {
                value: PreparedAsyncExplain::Lines(vec!["observed".into()]),
            }))
        }));
        let produce = futures::future::poll_fn(|cx| {
            let window = owner.result_window_alias();
            action.poll_action(&mut owner.action_context(window.as_ref()), cx)
        });
        let signal = async move {
            tokio::task::yield_now().await;
            send.send(()).unwrap();
        };
        let (result, ()) = tokio::join!(produce, signal);
        let StatementActionValue::Local(local) = result.unwrap() else {
            panic!("real closed Local output")
        };
        assert_eq!(local.row_count(), 1);
        assert_eq!(capacity.snapshot().held_positions, [0, 1, 0, 0]);
        drop(action);
        drop(local);
        owner.finish();
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }

    #[tokio::test]
    async fn prepared_explain_checks_cancellation_before_dispatch() {
        let (_workload, capacity, mut owner, _session) = statement().await;
        let polls = Arc::new(AtomicUsize::new(0));
        let inside = Arc::clone(&polls);
        let mut action = action(Box::pin(futures::future::poll_fn(move |_| {
            inside.fetch_add(1, Ordering::SeqCst);
            Poll::Pending
        })));
        owner
            .execution_owner_slot()
            .as_ref()
            .unwrap()
            .cancel(novarocks_workload_control::CancellationReason::Requested);
        let result = futures::future::poll_fn(|cx| {
            let window = owner.result_window_alias();
            action.poll_action(&mut owner.action_context(window.as_ref()), cx)
        })
        .await;
        let Err(error) = result else {
            panic!("cancelled preparation cannot deliver a result");
        };
        assert_eq!(error.kind(), QueryServiceErrorKind::Interrupted);
        assert_eq!(
            error.user_error().expect("typed cancellation").message(),
            "Query execution cancellation was requested"
        );
        assert_eq!(
            error
                .execution_failure()
                .expect("original cancellation fact")
                .site(),
            novarocks_query_application::execution_error::ExecutionFailureSite::Coordinator
        );
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        assert_eq!(capacity.snapshot().held_positions, [0, 1, 0, 0]);
        drop(action);
        owner.finish();
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }

    #[tokio::test]
    async fn occupied_protocol_destination_preserves_original_ready_and_owner() {
        let (_workload, _capacity, owner, _session) = statement().await;
        let token = owner.token();
        let mut source = Some(StatementActionReady {
            result: Ok(StatementActionValue::Ok(
                novarocks_query_application::session_control::StatementCommandCompletion::new(0),
            )),
            owner,
        });
        let mut target = Some(StatementResult::Ok);
        assert!(StatementActionReady::transfer_protocol_in(&mut source, &mut target).is_err());
        assert_eq!(source.as_ref().unwrap().owner.token(), token);
        assert!(matches!(target, Some(StatementResult::Ok)));
        target = None;
        StatementActionReady::transfer_protocol_in(&mut source, &mut target).unwrap();
        assert!(source.is_none());
        let StatementResult::GovernedCompletion(result) = target.take().unwrap() else {
            panic!("same governed completion")
        };
        let (completion, mut protocol) = result.into_parts();
        assert_eq!(completion.affected_rows(), 0);
        assert_eq!(
            protocol.complete(),
            novarocks_query_application::session_control::GovernedStatementFinishOutcome::Completed
        );
    }
}
