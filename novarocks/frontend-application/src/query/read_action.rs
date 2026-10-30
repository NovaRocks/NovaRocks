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

//! Ordinary ClientRows preparation and start in the original registered action.
pub(crate) mod io_source;
use super::statement_computation::StatementStageAdmission;
use super::*;
use crate::query_execution::completion_facts::{
    FrontendCompletionCustody, FrontendCompletionPorts, FrontendPlanningCpuSource,
};
use crate::query_execution::logical_execution::OriginalComputationStart;
use crate::state_store::execution::StateStoreOriginalOwner;
use io_source::ReadMetadataIoSource;
use novarocks_query_application::admitted_query_context::QueryResultCapacityBinding;
use novarocks_query_application::api::{
    ExecutionHandle, LocalResultProducer, LogicalExecutionExitObserver,
    LogicalExecutionExitReceipt, StatementNativeHealthFailure,
};
use novarocks_query_application::session_control::{
    GovernedActionContext, GovernedQueryStatementOwner, GovernedStatementAction,
    GovernedStatementResidual, StatementActionValue,
};
use novarocks_spi::connector::{
    ConnectorIoAdmissionPort, ConnectorMetadataMaintenanceIoProfile, SharedDependency,
};
use novarocks_state_store_api::StateStoreExecutionRequests;
use novarocks_workload_control::{WorkError, WorkScope};
use std::task::{Context, Poll};

pub(super) struct ReadPreparationInput {
    pub(super) compiler: FrontendQueryCompiler,
    pub(super) topology: BackendTopologyService,
    pub(super) role: ClusterRole,
    pub(super) optimizer_limit: u64,
    pub(super) sql: Arc<str>,
    pub(super) parsed: ParsedStatement,
    pub(super) state: SessionSqlState,
    pub(super) deadline: Option<Instant>,
    pub(super) cancellation: QueryCancellationView,
    pub(super) scope: WorkScope,
    pub(super) window: ResultWindowAlias,
    pub(super) purpose: FrontendQueryPurpose,
    pub(super) provider: Arc<FrontendCompletionCustody>,
    pub(super) planning_cpu: Arc<FrontendPlanningCpuSource>,
    pub(super) io: Arc<ReadMetadataIoSource>,
    pub(super) state_store: Option<Arc<dyn StateStoreExecutionRequests>>,
    #[cfg(novarocks_dev_controls)]
    pub(super) observation: Option<crate::preparation_diagnostics::StatementObservationHandle>,
    #[cfg(novarocks_dev_controls)]
    pub(super) statement_token: StatementToken,
}
pub(super) async fn prepare_read(
    input: ReadPreparationInput,
) -> Result<Result<PreparedQueryOperation, QueryServiceError>, StatementNativeHealthFailure> {
    let prepared: Result<_, QueryServiceError> = async {
        let parsed = input
            .state
            .substitute_user_variables(input.parsed)
            .map_err(|e| internal_error(e.to_string()))?;
        let topology =
            wait_for_initial_query_topology(&input.topology, &input.cancellation, input.deadline)
                .await?;
        let presentation = *input.state.presentation();
        let (catalog, database, execution, mut optimizer, semantics) =
            input.state.into_query_attempt_inputs();
        let semantics = novarocks_sql::sql_mode::statement_sql_semantics(&semantics, &parsed)
            .map_err(|e| QueryServiceError::from_user_error(e.to_user_error(Some(&input.sql))))?;
        if optimizer.optimizer_query_mem_limit_bytes.is_none() {
            optimizer.optimizer_query_mem_limit_bytes = Some(input.optimizer_limit as f64);
        }
        let options = with_query_hints(
            query_options_from_session_settings(&execution),
            &semantics,
            match &parsed {
                ParsedStatement::Query(q) => Some(q),
                _ => None,
            },
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
        .with_result_capacity(capacity)
        .map_err(|e| internal_error(e.to_string()))?;
        #[cfg(novarocks_dev_controls)]
        let context = match input.observation.as_ref().and_then(|h| h.nomination()) {
            Some(nomination) => context
                .with_test_statement_nomination(Arc::new(nomination))
                .map_err(internal_error)?,
            None => context,
        };
        Ok((parsed, context, options))
    }
    .await;
    let (parsed, context, options) = match prepared {
        Ok(value) => value,
        Err(error) => return Ok(Err(error)),
    };
    #[cfg(novarocks_dev_controls)]
    let rewrite = input
        .observation
        .as_ref()
        .and_then(|h| h.statement_target())
        .map(|(run, nominated, _connection_id)| {
            crate::mv::test_controls::MvRewriteStatementTarget::new(
                input.statement_token,
                nominated,
                run,
            )
        })
        .transpose()
        .map_err(internal_error);
    #[cfg(novarocks_dev_controls)]
    let rewrite = match rewrite {
        Ok(rewrite) => rewrite,
        Err(error) => return Ok(Err(error)),
    };
    let result = input
        .compiler
        .prepare_read_async(
            &parsed,
            input.purpose,
            &context,
            Some(options),
            &input.scope,
            FrontendCompletionPorts {
                provider: &input.provider,
                cpu: input.planning_cpu.as_ref(),
            },
            input.io,
            input.state_store,
            #[cfg(novarocks_dev_controls)]
            rewrite,
        )
        .await?;
    Ok(result.map_err(|e| routed_execution_service_error(routed_compiler_failure(e, &input.sql))))
}
pub(super) async fn close_provider(
    source: Arc<FrontendCompletionCustody>,
    planning_cpu: Arc<FrontendPlanningCpuSource>,
) -> Result<(), StatementNativeHealthFailure> {
    source.close().await?;
    planning_cpu.close().await
}
pub(super) type ProviderClose =
    Pin<Box<dyn Future<Output = Result<(), StatementNativeHealthFailure>> + Send>>;
pub(super) fn poll_provider_close(
    future: &mut Option<ProviderClose>,
    result: &mut Option<Result<(), StatementNativeHealthFailure>>,
    cx: &mut Context<'_>,
) -> Poll<Result<(), StatementNativeHealthFailure>> {
    if let Some(future) = future.as_mut() {
        match future.as_mut().poll(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(value) => *result = Some(value),
        }
        // The result is in the original receiving field before completed F Drop.
    }
    if result.is_none() {
        *result = Some(Err(StatementNativeHealthFailure::actual_exit_unproven(
            "Read Provider close has no original result",
            Arc::new(WorkError::Conflict),
        )));
    }
    drop(future.take());
    Poll::Ready(result.as_ref().unwrap().clone())
}
type Preparation = Pin<
    Box<
        dyn Future<
                Output = Result<
                    Result<PreparedQueryOperation, QueryServiceError>,
                    StatementNativeHealthFailure,
                >,
            > + Send,
    >,
>;

/// The original statement's shared metadata/StateStore ports. A multi-stage
/// SET retains this same packet; a scalar stage never closes these ports.
pub(super) struct ReadNativeServices {
    state_store: Option<StateStoreOriginalOwner>,
    io: Arc<ReadMetadataIoSource>,
}
impl ReadNativeServices {
    pub(super) fn new(
        state_store: Option<StateStoreOriginalOwner>,
        io: Arc<ReadMetadataIoSource>,
    ) -> Self {
        Self { state_store, io }
    }
    pub(super) fn io(&self) -> &Arc<ReadMetadataIoSource> {
        &self.io
    }
    fn retain_state_store_health(
        health: &mut Option<StatementNativeHealthFailure>,
        cause: novarocks_state_store_api::StateStoreExecutionFailure,
    ) {
        if health.is_none() {
            *health = Some(StatementNativeHealthFailure::actual_exit_unproven(
                "Read StateStore original custody failed",
                Arc::new(cause),
            ));
        }
    }
    pub(super) fn poll_state_store(
        &self,
        cx: &mut Context<'_>,
        health: &mut Option<StatementNativeHealthFailure>,
    ) {
        if let Some(source) = self.state_store.as_ref()
            && let Err(cause) = source.poll_progress(cx)
        {
            Self::retain_state_store_health(health, cause);
        }
    }
    pub(super) fn state_store_quiescent(
        &self,
        cx: &mut Context<'_>,
        health: &mut Option<StatementNativeHealthFailure>,
    ) -> bool {
        match self
            .state_store
            .as_ref()
            .map(|source| source.poll_actual_quiescent(cx))
        {
            None | Some(Ok(true)) => true,
            Some(Ok(false)) => false,
            Some(Err(cause)) => {
                Self::retain_state_store_health(health, cause);
                false
            }
        }
    }
    pub(super) fn poll_state_store_closed(
        &mut self,
        cx: &mut Context<'_>,
        health: &mut Option<StatementNativeHealthFailure>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        if let Some(health) = health.as_ref() {
            return Poll::Ready(Err(health.clone()));
        }
        match self
            .state_store
            .as_mut()
            .map(|source| source.poll_closed(cx))
        {
            None | Some(Poll::Ready(Ok(()))) => Poll::Ready(Ok(())),
            Some(Poll::Pending) => Poll::Pending,
            Some(Poll::Ready(Err(cause))) => {
                Self::retain_state_store_health(health, cause);
                Poll::Ready(Err(health.as_ref().unwrap().clone()))
            }
        }
    }
    /// Only the original never-started I/O author can withdraw its proposal.
    /// Call at the whole-statement unstarted cut, never between scalar stages.
    pub(super) fn withdraw_unstarted_io(
        &self,
        health: &mut Option<StatementNativeHealthFailure>,
    ) -> Result<bool, StatementNativeHealthFailure> {
        if let Some(health) = health.as_ref() {
            return Err(health.clone());
        }
        match self.io.original() {
            None => Ok(false),
            Some(io) => match io.withdraw_unstarted_in() {
                Ok(Some(_)) => {
                    if let Some(failure) = io.health_failure() {
                        *health = Some(failure.clone());
                        return Err(failure);
                    }
                    Ok(true)
                }
                Ok(None) => Ok(false),
                Err(error) => {
                    *health = Some(StatementNativeHealthFailure::actual_exit_unproven(
                        "Read initial I/O withdrawal failed",
                        Arc::new(error),
                    ));
                    Err(health.as_ref().unwrap().clone())
                }
            },
        }
    }
    pub(super) fn poll_io_progress(
        &self,
        cx: &mut Context<'_>,
        health: &mut Option<StatementNativeHealthFailure>,
    ) -> Result<(), StatementNativeHealthFailure> {
        if let Some(health) = health.as_ref() {
            return Err(health.clone());
        }
        if let Some(io) = self.io.original() {
            if let Err(error) = io.poll_actual_progress_continuing(cx) {
                *health = Some(StatementNativeHealthFailure::actual_exit_unproven(
                    "Read I/O continuation failed before actual exit",
                    Arc::new(error),
                ));
                return Err(health.as_ref().unwrap().clone());
            }
            if let Some(failure) = io.health_failure() {
                *health = Some(failure.clone());
                return Err(failure);
            }
        }
        Ok(())
    }
    pub(super) fn poll_io_quiescent(
        &self,
        cx: &mut Context<'_>,
        health: &mut Option<StatementNativeHealthFailure>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        if let Some(health) = health.as_ref() {
            return Poll::Ready(Err(health.clone()));
        }
        if let Some(io) = self.io.original() {
            match io.poll_actual_quiescent_continuing(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(failure)) => {
                    *health = Some(failure.clone());
                    return Poll::Ready(Err(failure));
                }
                Poll::Ready(Ok(_)) => {
                    // Ordinary failure remains in the same MIO business slot.
                }
            }
        }
        Poll::Ready(Ok(()))
    }
}

/// This stage's original admission and independent Native exit observation.
/// Its owned read position retires through the pump cut, not through this tail.
pub(super) struct ReadNativeStageTail {
    pub(super) stage: StatementStageAdmission,
    exit: Option<LogicalExecutionExitObserver>,
    receipt: Option<Result<LogicalExecutionExitReceipt, QueryExecutionError>>,
    launched: bool,
}
impl ReadNativeStageTail {
    pub(super) fn new(stage: StatementStageAdmission) -> Self {
        Self {
            stage,
            exit: None,
            receipt: None,
            launched: false,
        }
    }
    pub(super) fn is_launched(&self) -> bool {
        self.launched
    }
    /// Install the exact accepted observer before the caller notifies progress.
    pub(super) fn observe_start_in(
        &mut self,
        attempt: &novarocks_query_application::api::LogicalExecutionStartAttempt,
        original_exit: &mut Option<LogicalExecutionExitObserver>,
        health: &mut Option<StatementNativeHealthFailure>,
    ) {
        if attempt.result().is_ok() {
            self.launched = true;
            self.exit = original_exit.take();
            if self.exit.is_none() {
                *health = Some(StatementNativeHealthFailure::actual_exit_unproven(
                    "accepted Read has no original Native observer",
                    Arc::new(WorkError::Conflict),
                ));
            }
        }
    }
    /// Observe only the original Native stage. This neither closes shared
    /// ports nor retires its stage owner or proves another input's exit.
    pub(super) fn poll_stage_exit(
        &mut self,
        cx: &mut Context<'_>,
        health: &mut Option<StatementNativeHealthFailure>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        if let Some(health) = health.as_ref() {
            return Poll::Ready(Err(health.clone()));
        }
        if self.launched {
            if self.exit.is_none() && self.receipt.is_none() {
                *health = Some(StatementNativeHealthFailure::actual_exit_unproven(
                    "Read start has no original Native exit observer",
                    Arc::new(WorkError::Conflict),
                ));
                return Poll::Ready(Err(health.as_ref().unwrap().clone()));
            }
            if let Some(exit) = self.exit.as_mut() {
                match exit.poll_exit(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(result) => self.receipt = Some(result),
                }
                drop(self.exit.take());
            }
            if self.receipt.as_ref().is_some_and(Result::is_err) {
                *health = Some(StatementNativeHealthFailure::actual_exit_unproven(
                    "Read original Native exit was refused",
                    Arc::new(WorkError::Conflict),
                ));
                return Poll::Ready(Err(health.as_ref().unwrap().clone()));
            }
        }
        Poll::Ready(Ok(()))
    }
}

/// The single Read composes these same packets; their declaration order keeps
/// the original StateStore/I/O/stage/observer/receipt/health Drop sequence.
pub(super) struct ReadNativeTail {
    pub(super) services: ReadNativeServices,
    pub(super) native: ReadNativeStageTail,
    pub(super) health: Option<StatementNativeHealthFailure>,
}
impl ReadNativeTail {
    // Native and MIO retain the same actual-exit order at both original cuts.
    fn poll_required_exit(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        if let Some(health) = &self.health {
            return Poll::Ready(Err(health.clone()));
        }
        let io_withdrawn = if !self.native.is_launched() {
            self.services.withdraw_unstarted_io(&mut self.health)?
        } else {
            false
        };
        if !io_withdrawn {
            self.services.poll_io_progress(cx, &mut self.health)?;
        }
        match self.native.poll_stage_exit(cx, &mut self.health) {
            Poll::Ready(Ok(())) => {}
            result => return result,
        }
        if !io_withdrawn {
            match self.services.poll_io_quiescent(cx, &mut self.health) {
                Poll::Ready(Ok(())) => {}
                result => return result,
            }
        }
        Poll::Ready(Ok(()))
    }
    pub(super) fn poll_business_exit(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        let state_store_quiescent = self.services.state_store_quiescent(cx, &mut self.health);
        match self.poll_required_exit(cx) {
            Poll::Ready(Ok(())) if state_store_quiescent => {
                self.native.stage.retire_unlaunched();
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Ok(())) => Poll::Pending,
            result => result,
        }
    }
    fn poll_exit(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        let state_store_closed = match self.services.poll_state_store_closed(cx, &mut self.health) {
            Poll::Ready(Ok(())) => true,
            Poll::Pending => false,
            Poll::Ready(Err(health)) => return Poll::Ready(Err(health)),
        };
        // Native/MIO still advance while StateStore close is Pending.
        match self.poll_required_exit(cx) {
            Poll::Ready(Ok(())) if state_store_closed => {
                self.native.stage.retire_unlaunched();
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Ok(())) => Poll::Pending,
            result => result,
        }
    }
}
impl GovernedStatementResidual for ReadNativeTail {
    fn poll_retirement(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        self.poll_exit(cx)
    }
}
struct ReadStatementAction {
    family: novarocks_result_contract::ClientPresentationFamily,
    planning_cpu: Arc<FrontendPlanningCpuSource>,
    preparation_health: Option<StatementNativeHealthFailure>,
    provider: Arc<FrontendCompletionCustody>,
    provider_close: Option<ProviderClose>,
    provider_close_result: Option<Result<(), StatementNativeHealthFailure>>,
    local: Option<novarocks_query_application::api::OwnedLocalResult>,
    tail: Option<Box<ReadNativeTail>>,
    preparation: Option<Preparation>,
    prepared: Option<Result<PreparedQueryOperation, QueryServiceError>>,
    read: Option<crate::query_execution::completion::PreparedLogicalRead>,
    capacity: Option<QueryResultCapacityBinding>,
    original: OriginalComputationStart,
    execution: Option<Result<ExecutionHandle, QueryExecutionError>>,
    error: Option<QueryServiceError>,
    completed_preparation: bool,
    preparation_started: bool,
    bootstrap_ready: bool,
    result_transferred: bool,
    launcher: Arc<dyn LogicalExecutionLauncher>,
    #[cfg(novarocks_dev_controls)]
    observation: Option<crate::preparation_diagnostics::StatementObservationHandle>,
}
impl ReadStatementAction {
    fn poll_read(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatementActionValue, QueryServiceError>> {
        if self.preparation_health.is_some() {
            return Poll::Pending;
        }
        let tail = self
            .tail
            .as_mut()
            .expect("original Read tail remains until handoff");
        if tail.health.is_some() {
            return Poll::Pending;
        }
        let work = owner
            .execution_owner_slot()
            .as_ref()
            .expect("Read retains its original Root");
        match tail.services.io().poll_progress(work, cx) {
            Ok(ready) => self.bootstrap_ready |= ready,
            Err(e) => {
                self.error.get_or_insert_with(|| {
                    QueryServiceError::from_failure_cause(
                        novarocks_failure_contract::SharedFailureCause::new(e),
                    )
                });
            }
        }
        if !self.bootstrap_ready && self.error.is_none() {
            return Poll::Pending;
        }
        if self.error.is_none() {
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
                if self.provider.local_health_failure().is_some() {
                    return Poll::Pending;
                }
                self.completed_preparation = true;
                drop(self.preparation.take());
            }
            match poll_provider_close(
                &mut self.provider_close,
                &mut self.provider_close_result,
                cx,
            ) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(_)) => return Poll::Pending,
                Poll::Ready(Ok(())) => {}
            }
            if let Some(prepared) = self.prepared.take() {
                match prepared {
                    Err(error) => self.error = Some(error),
                    Ok(PreparedQueryOperation::LogicalRead(read)) => self.read = Some(read),
                    Ok(PreparedQueryOperation::Immediate(immediate)) => {
                        match immediate.into_result() {
                            StatementResult::Query(result) => {
                                match LocalResultProducer::try_new(
                                    owner.scope(),
                                    owner.result_window_alias().expect("admitted local window"),
                                    self.family,
                                )
                                .and_then(|p| p.produce(|| Ok(result)))
                                {
                                    Ok(result) => self.local = Some(result),
                                    Err(e) => self.error = Some(internal_error(e)),
                                }
                            }
                            _ => {
                                self.error = Some(internal_error(
                                    "Read immediate has a foreign protocol owner",
                                ))
                            }
                        }
                    }
                }
            }
        }
        if self.error.is_none()
            && let Some(error) = tail.services.io().original().and_then(|io| io.failure())
        {
            self.error = Some(QueryServiceError::from_failure_cause(
                novarocks_failure_contract::SharedFailureCause::new(error),
            ));
        }
        if self.error.is_none() && self.local.is_some() {
            match tail.poll_business_exit(cx) {
                Poll::Pending | Poll::Ready(Err(_)) => return Poll::Pending,
                Poll::Ready(Ok(())) => {}
            }
            if let Some(error) = tail.services.io().original().and_then(|io| io.failure()) {
                self.error = Some(QueryServiceError::from_failure_cause(
                    novarocks_failure_contract::SharedFailureCause::new(error),
                ));
            } else {
                self.result_transferred = true;
                return Poll::Ready(Ok(StatementActionValue::Local(
                    self.local.take().expect("closed local Read result"),
                )));
            }
        }
        if self.error.is_none() && self.read.is_some() {
            // Actual physical footer, not an idle projection's final settlement,
            // is the preparation-to-Native cut. Unknown remains in the tail.
            if !tail.services.state_store_quiescent(cx, &mut tail.health) {
                return Poll::Pending;
            }
            let io = tail
                .services
                .io()
                .original()
                .expect("Read CPU body installed original source");
            let work = owner
                .execution_owner_slot()
                .as_ref()
                .expect("original Root");
            match tail.native.stage.poll_admit(&io, work, cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => self.error = Some(e),
                Poll::Ready(Ok(())) => {}
            }
            if self.error.is_none() && self.capacity.is_none() {
                match tail.native.stage.install_client_capacity_in(
                    &io,
                    &owner.result_window_alias().expect("admitted Client window"),
                    &mut self.capacity,
                ) {
                    Ok(()) => {}
                    Err(e) => self.error = Some(e),
                }
            }
            if self.error.is_none() {
                match self.launcher.start_read_in(
                    &mut self.read,
                    tail.native.stage.owner_slot(),
                    self.capacity.as_ref().unwrap(),
                    tail.services.io(),
                    &mut self.original,
                    #[cfg(novarocks_dev_controls)]
                    self.observation.clone(),
                ) {
                    Err(e) => self.error = Some(governed_query_execution_error(e)),
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
        if let Some(future) = self.original.ready.as_mut() {
            match future.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => self.execution = Some(result),
            }
            drop(self.original.ready.take());
        }
        if let Some(execution) = self.execution.take() {
            match execution {
                Ok(execution) => {
                    self.result_transferred = true;
                    drop(self.capacity.take());
                    return Poll::Ready(Ok(StatementActionValue::Rows(execution)));
                }
                Err(e) => self.error = Some(governed_query_execution_error(e)),
            }
        }
        if self.error.is_some() {
            // A preparation F which has ever been polled is not timeout-exit.
            // Continue it and keep its actual Creator source before any close.
            if !self.completed_preparation && !self.preparation_started {
                drop(self.preparation.take());
                self.completed_preparation = true;
            }
            if !self.completed_preparation
                && let Some(future) = self.preparation.as_mut()
            {
                match future.as_mut().poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(result)) => self.prepared = Some(result),
                    Poll::Ready(Err(health)) => {
                        self.preparation_health = Some(health);
                        return Poll::Pending;
                    }
                }
                self.completed_preparation = true;
                drop(self.preparation.take());
            }
            match poll_provider_close(
                &mut self.provider_close,
                &mut self.provider_close_result,
                cx,
            ) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(_)) => return Poll::Pending,
                Poll::Ready(Ok(())) => {}
            }
            if !tail.native.is_launched() {
                // A refused mount left every original input in this action.
                // Dispose those inputs before retiring its unlaunched stage.
                drop(self.local.take());
                drop(self.original.input.take());
                drop(self.original.request.take());
                drop(self.read.take());
                drop(self.capacity.take());
            }
            match tail.poll_business_exit(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(_)) => return Poll::Pending,
                Poll::Ready(Ok(())) => {}
            }
            // Error delivery also hands the same tail to QA. It does not
            // retire an idle StateStore Original or discard its evidence.
            self.result_transferred = true;
            return Poll::Ready(Err(self.error.take().unwrap()));
        }
        Poll::Pending
    }
}
impl GovernedStatementAction for ReadStatementAction {
    fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        self.preparation_health
            .clone()
            .or_else(|| self.planning_cpu.local_health_failure())
            .or_else(|| self.provider.local_health_failure())
            .or_else(|| {
                self.provider_close_result
                    .as_ref()
                    .and_then(|r| r.as_ref().err().cloned())
            })
            .or_else(|| {
                self.tail.as_ref().and_then(|t| {
                    t.services
                        .io()
                        .health_failure()
                        .or_else(|| t.native.stage.local_health_failure())
                        .or_else(|| t.health.clone())
                })
            })
    }
    fn handoff_residual_in(
        &mut self,
        target: &mut Option<Box<dyn GovernedStatementResidual>>,
    ) -> Result<(), StatementNativeHealthFailure> {
        if self.result_transferred {
            if target.is_some() {
                return Err(StatementNativeHealthFailure::actual_exit_unproven(
                    "Read residual receiver is occupied",
                    Arc::new(WorkError::Conflict),
                ));
            }
            *target = Some(self.tail.take().expect("same original Read residual"));
        }
        Ok(())
    }
    fn poll_action(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatementActionValue, QueryServiceError>> {
        // Newly borrowed phases are dispatched in the same poll, including
        // Pending. No borrower wake is needed to discover its own new request.
        if let Some(tail) = self.tail.as_mut() {
            tail.services.poll_state_store(cx, &mut tail.health);
        }
        let result = self.poll_read(owner, cx);
        if let Some(tail) = self.tail.as_mut() {
            tail.services.poll_state_store(cx, &mut tail.health);
        }
        result
    }
}
impl FrontendQuerySession {
    pub(super) async fn execute_observed_read(
        &self,
        sql: Arc<str>,
        parsed: ParsedStatement,
        state: SessionSqlState,
        deadline: Option<Instant>,
        statement: GovernedQueryStatementOwner,
        purpose: FrontendQueryPurpose,
    ) -> Result<StatementResult, QueryServiceError> {
        let window = statement
            .result_window_alias()
            .ok_or_else(|| internal_error("Read has no original result window"))?;
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
        let local = local_preparation_profile_with_dependencies(dependencies)
            .map_err(|error| internal_error(error.to_string()))?;
        let (state_store_owner, state_store_requests) = match state_store {
            Some((owner, requests)) => (Some(owner), Some(requests)),
            None => (None, None),
        };
        let io = ReadMetadataIoSource::new(statement.scope().clone(), local.clone());
        // This same host-local preparation remains a Source through encoding.
        // Exact selected Provider/service facts are added before their effects.
        io.configure_read_source(&local)
            .map_err(|error| internal_error(error.to_string()))?;
        let stage = StatementStageAdmission::new(statement.scope().clone(), 0);
        let stage = if window.class() == novarocks_workload_control::ResultWindowClass::Client {
            stage
                .with_client_window(&window)
                .map_err(|error| internal_error(error.to_string()))?
        } else {
            stage
        };
        let provider = FrontendCompletionCustody::new();
        let planning_cpu = self.service.query_compiler.new_planning_cpu_source();
        #[cfg(novarocks_dev_controls)]
        let observation = self
            .service
            .statement_observation
            .as_ref()
            .and_then(|o| o.bound_statement(statement.token()));
        let family = *state.presentation();
        let input = ReadPreparationInput {
            compiler: self.service.query_compiler.clone(),
            topology: self.service.topology.clone(),
            role: self.service.role,
            optimizer_limit: self.service.optimizer_query_mem_limit_bytes,
            sql,
            parsed,
            state,
            deadline,
            cancellation: QueryCancellationView::governed(
                statement.cancellation().clone(),
                statement.timeout_ms(),
            ),
            scope: statement.scope().clone(),
            window,
            purpose,
            provider: Arc::clone(&provider),
            planning_cpu: Arc::clone(&planning_cpu),
            io: Arc::clone(&io),
            state_store: state_store_requests,
            #[cfg(novarocks_dev_controls)]
            observation: observation.clone(),
            #[cfg(novarocks_dev_controls)]
            statement_token: statement.token(),
        };
        let provider_close = Some(Box::pin(close_provider(
            Arc::clone(&provider),
            Arc::clone(&planning_cpu),
        )) as ProviderClose);
        let action = ReadStatementAction {
            family,
            planning_cpu,
            preparation_health: None,
            provider,
            provider_close,
            provider_close_result: None,
            local: None,
            tail: Some(Box::new(ReadNativeTail {
                services: ReadNativeServices::new(state_store_owner, io),
                native: ReadNativeStageTail::new(stage),
                health: None,
            })),
            preparation: Some(Box::pin(prepare_read(input))),
            prepared: None,
            read: None,
            capacity: None,
            original: OriginalComputationStart::default(),
            execution: None,
            error: None,
            completed_preparation: false,
            preparation_started: false,
            bootstrap_ready: false,
            result_transferred: false,
            launcher: Arc::clone(&self.service.logical_execution_launcher),
            #[cfg(novarocks_dev_controls)]
            observation,
        };
        #[cfg(novarocks_dev_controls)]
        let handoff_statement = statement.token();
        #[cfg(novarocks_dev_controls)]
        let handoff_target = self
            .service
            .statement_observation
            .as_ref()
            .and_then(|o| o.bound_statement(handoff_statement))
            .and_then(|h| h.statement_target());
        let result = self
            .execute_observed_statement_action(statement, Box::new(action))
            .await?;
        #[cfg(novarocks_dev_controls)]
        if let StatementResult::StreamingQuery(stream) = &result
            && let Some(control) = &self.service.result_handoff
        {
            control
                .before_protocol(handoff_statement, handoff_target, stream.failure_view())
                .await
                .map_err(internal_error)?;
        }
        Ok(result)
    }
}
#[cfg(test)]
fn local_preparation_profile() -> Result<ConnectorMetadataMaintenanceIoProfile, WorkError> {
    local_preparation_profile_with_dependencies(&[])
}
pub(super) fn local_preparation_profile_with_dependencies(
    dependencies: &[SharedDependency],
) -> Result<ConnectorMetadataMaintenanceIoProfile, WorkError> {
    use crate::query_execution::maintenance::io_admission::MetadataIoAdmission;
    // These declarations describe the actual single local preparation body;
    // they do not qualify a Provider or reserve an SDK allocation envelope.
    let mut dependencies = dependencies.to_vec();
    if !dependencies.contains(&SharedDependency::LocalHost) {
        dependencies.push(SharedDependency::LocalHost);
    }
    ConnectorMetadataMaintenanceIoProfile::try_new(
        std::num::NonZeroUsize::new(1).unwrap(),
        std::num::NonZeroU64::new(MetadataIoAdmission::request_metadata_bytes())
            .ok_or(WorkError::ArithmeticOverflow)?,
        std::num::NonZeroU64::new(std::mem::size_of::<ReadPreparationInput>() as u64)
            .ok_or(WorkError::ArithmeticOverflow)?,
        dependencies,
    )
    .map_err(|_| WorkError::InvalidConfig("local Read preparation source declaration"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_execution::native_execution_adapter::source_cohort::tests::{finish, fixture};
    use novarocks_workload_control::{WorkClass, WorkRequest};
    use std::{
        num::{NonZeroU64, NonZeroUsize},
        task::Waker,
    };
    fn geometry(io: usize) -> novarocks_spi::connector::ConnectorMetadataMaintenanceIoGeometry {
        novarocks_spi::connector::ConnectorMetadataMaintenanceIoGeometry::new(
            NonZeroUsize::new(io).unwrap(),
            NonZeroU64::new(1).unwrap(),
            NonZeroU64::new(1).unwrap(),
        )
    }
    #[test]
    fn read_preparation_profile_keeps_selected_store_facts_and_local_host() {
        let selected = SharedDependency::Thrift(
            novarocks_resource_contract::ThriftServiceOrigin::from_selected_address(
                "catalog-host:9083",
            )
            .unwrap(),
        );
        let profile = local_preparation_profile_with_dependencies(&[
            selected.clone(),
            SharedDependency::LocalHost,
        ])
        .unwrap();
        assert!(profile.initial_dependencies().contains(&selected));
        assert_eq!(
            profile
                .initial_dependencies()
                .iter()
                .filter(|fact| **fact == SharedDependency::LocalHost)
                .count(),
            1
        );
    }
    #[test]
    fn actual_read_source_accepts_provider_profile_without_a_geometry_quota() {
        let (control, driver, _) = fixture();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let source =
            ReadMetadataIoSource::new(root.owner.scope(), local_preparation_profile().unwrap());
        let actual = ConnectorMetadataMaintenanceIoProfile::from_geometry(
            geometry(2),
            vec![SharedDependency::LocalHost],
        )
        .unwrap();
        source.configure_read_source(&actual).unwrap();
        let original = source.original().expect("same actual local source");
        source.configure_read_source(&actual).unwrap();
        assert!(Arc::ptr_eq(&original, &source.original().unwrap()));
        let mut cx = Context::from_waker(Waker::noop());
        assert!(
            source.poll_read_source_ready(&mut cx).is_pending(),
            "profile acceptance is not Root/source readiness"
        );
        drop(original);
        drop(source);
        finish(control, root, driver);
    }
    #[test]
    fn actual_read_request_cannot_dispatch_before_original_root_source_proof() {
        let (control, driver, _) = fixture();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let source =
            ReadMetadataIoSource::new(root.owner.scope(), local_preparation_profile().unwrap());
        let profile = ConnectorMetadataMaintenanceIoProfile::from_geometry(
            geometry(1),
            vec![SharedDependency::LocalHost],
        )
        .unwrap();
        source.configure_read_source(&profile).unwrap();
        let request = source.request(SharedDependency::LocalHost).unwrap();
        let mut permit = None;
        let mut cx = Context::from_waker(Waker::noop());
        assert!(request.poll_admitted_in(&mut permit, &mut cx).is_pending());
        assert!(permit.is_none());
        assert!(source.poll_read_source_ready(&mut cx).is_pending());
        assert!(
            matches!(
                root.owner.scope().try_request_joint_child(),
                Err(WorkError::Conflict)
            ),
            "empty-D child guard is still enforced"
        );
        drop((request, source));
        finish(control, root, driver);
    }
    #[test]
    fn cancelled_read_bootstrap_withdraws_original_without_manufacturing_health() {
        let (control, driver, _) = fixture();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let scope = root.owner.scope();
        let profile = local_preparation_profile().unwrap();
        let io = ReadMetadataIoSource::new(scope.clone(), profile.clone());
        io.configure_read_source(&profile).unwrap();
        root.owner
            .cancel(novarocks_workload_control::CancellationReason::DeadlineExceeded);
        let mut cx = Context::from_waker(Waker::noop());
        let refused = io.poll_progress(&root.owner, &mut cx).unwrap_err();
        assert_eq!(
            refused,
            WorkError::Cancelled(novarocks_workload_control::CancellationReason::DeadlineExceeded)
        );
        let original = io.original().unwrap();
        let mut tail = ReadNativeTail {
            services: ReadNativeServices::new(None, io),
            native: ReadNativeStageTail::new(StatementStageAdmission::new(scope, 0)),
            health: None,
        };
        assert!(matches!(
            tail.poll_business_exit(&mut cx),
            Poll::Ready(Ok(()))
        ));
        assert_eq!(original.failure(), Some(refused));
        assert!(tail.health.is_none());
        assert!(
            tail.native.receipt.is_none(),
            "an unlaunched Read has no Native footer"
        );
        assert!(matches!(tail.poll_exit(&mut cx), Poll::Ready(Ok(()))));
        assert_eq!(control.snapshot().root_responsibilities, 1);
        drop((original, tail));
        finish(control, root, driver);
    }

    #[test]
    fn stopped_read_keeps_real_request_alias_until_actual_quiescence_without_health() {
        use crate::query_execution::native_execution_adapter::source_cohort::tests::drive;
        let (control, mut driver, _) = fixture();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let scope = root.owner.scope();
        let profile = local_preparation_profile().unwrap();
        let io = ReadMetadataIoSource::new(scope.clone(), profile.clone());
        io.configure_read_source(&profile).unwrap();
        let mut cx = Context::from_waker(Waker::noop());
        let mut ready = false;
        for _ in 0..100 {
            ready = io.poll_progress(&root.owner, &mut cx).unwrap();
            drive(&mut driver);
            if ready {
                break;
            }
        }
        assert!(
            ready,
            "the original MIO actually installed its positive payload"
        );
        let original = io.original().unwrap();
        let selected = SharedDependency::Thrift(
            novarocks_resource_contract::ThriftServiceOrigin::from_selected_address(
                "read-new-route:9083",
            )
            .unwrap(),
        );
        let request = io.request(selected).unwrap();
        root.owner
            .cancel(novarocks_workload_control::CancellationReason::DeadlineExceeded);
        assert!(io.poll_progress(&root.owner, &mut cx).unwrap());
        let refused = original
            .failure()
            .expect("same actual domain-growth refusal");
        assert_eq!(
            refused,
            WorkError::Cancelled(novarocks_workload_control::CancellationReason::DeadlineExceeded)
        );
        let mut permit = None;
        assert!(matches!(
            request.poll_admitted_in(&mut permit, &mut cx),
            Poll::Ready(Err(_))
        ));
        assert!(
            permit.is_none(),
            "the refused request never acquires a physical permit"
        );
        let mut tail = ReadNativeTail {
            services: ReadNativeServices::new(None, io),
            native: ReadNativeStageTail::new(StatementStageAdmission::new(scope, 0)),
            health: None,
        };
        assert!(tail.poll_business_exit(&mut cx).is_pending());
        assert!(tail.health.is_none());
        assert_eq!(control.snapshot().root_responsibilities, 1);
        drop(request);
        assert!(matches!(
            tail.poll_business_exit(&mut cx),
            Poll::Ready(Ok(()))
        ));
        assert!(tail.health.is_none());
        assert_eq!(original.failure(), Some(refused));
        assert!(matches!(tail.poll_exit(&mut cx), Poll::Ready(Ok(()))));
        drop((original, tail));
        finish(control, root, driver);
    }

    #[test]
    fn ordinary_read_tail_cannot_treat_absent_native_observer_as_actual_exit() {
        let (control, driver, _) = fixture();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let mut tail = ReadNativeTail {
            services: ReadNativeServices::new(
                None,
                ReadMetadataIoSource::new(root.owner.scope(), local_preparation_profile().unwrap()),
            ),
            native: ReadNativeStageTail {
                stage: StatementStageAdmission::new(root.owner.scope(), 1),
                exit: None,
                receipt: None,
                launched: true,
            },
            health: None,
        };
        let result = tail.poll_exit(&mut Context::from_waker(Waker::noop()));
        assert!(matches!(result, Poll::Ready(Err(_))));
        assert!(tail.health.is_some());
        assert!(tail.native.receipt.is_none());
        drop(tail);
        finish(control, root, driver);
    }
}

#[cfg(test)]
mod provider_close_tests {
    use super::*;
    use std::task::Waker;
    #[tokio::test]
    async fn uninstalled_provider_still_receives_original_close_before_local_read_exit() {
        let source = FrontendCompletionCustody::new();
        let planning_cpu = FrontendPlanningCpuSource::new(
            crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor::new(
                tokio::runtime::Handle::current(),
            ),
        );
        let mut future =
            Some(Box::pin(close_provider(Arc::clone(&source), planning_cpu)) as ProviderClose);
        let mut receipt = None;
        assert!(matches!(
            poll_provider_close(
                &mut future,
                &mut receipt,
                &mut Context::from_waker(Waker::noop())
            ),
            Poll::Ready(Ok(()))
        ));
        assert!(future.is_none());
        assert!(matches!(receipt, Some(Ok(()))));
        assert!(source.local_health_failure().is_none());
    }
}

#[cfg(test)]
pub(crate) mod error_residual_tests;
