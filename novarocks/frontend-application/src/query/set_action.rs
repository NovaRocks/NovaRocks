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

//! One registered SET action owns every scalar preparation, staged assignment
//! and the final live-state visibility cut. Native cleanup remains a residual.

use super::read_action::{ReadNativeServices, ReadNativeStageTail};
use super::read_action::{ReadPreparationInput, prepare_read};
use super::statement_computation::StatementStageAdmission;
use super::*;
use crate::query_execution::completion_facts::{
    FrontendCompletionCustody, FrontendPlanningCpuSource,
};
use crate::query_execution::logical_execution::OriginalComputationStart;
use novarocks_query_application::admitted_query_context::QueryResultCapacityBinding;
use novarocks_query_application::api::{ExecutionHandle, StatementNativeHealthFailure};
use novarocks_query_application::session_control::{
    GovernedActionContext, GovernedQueryStatementOwner, GovernedStatementAction,
    GovernedStatementResidual, GovernedStatementVisibilitySealOutcome, StatementActionValue,
};
use novarocks_state_store_api::StateStoreExecutionRequests;
use novarocks_types::naming::DEFAULT_DATABASE;
use novarocks_workload_control::WorkError;
use std::task::{Context, Poll};

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
type ProviderClose = Pin<Box<dyn Future<Output = Result<(), StatementNativeHealthFailure>> + Send>>;

/// The existing lightweight typed scalar decoder, polled from the registered
/// statement. Its stream and partial literal remain in this original field.
pub(super) struct GovernedScalarStream {
    stream: novarocks_query_application::api::QueryResultStream,
    schema: Option<novarocks_result_contract::ScalarSchema>,
    schema_started: bool,
    value: Option<String>,
}
impl GovernedScalarStream {
    pub(super) fn new(
        stream: novarocks_query_application::api::QueryResultStream,
        schema: Option<novarocks_result_contract::ScalarSchema>,
    ) -> Self {
        Self {
            stream,
            schema,
            schema_started: false,
            value: None,
        }
    }
    pub(super) fn poll_value(
        &mut self,
        execution: &mut ExecutionHandle,
        cx: &mut Context<'_>,
    ) -> Poll<Result<String, QueryServiceError>> {
        use novarocks_query_application::api::ResultRowCarrier;
        use novarocks_result_contract::{
            BorrowedScalarRecord, InternalResultDomain, RootOutputKind,
        };
        if !self.schema_started {
            let schema = match self.stream.begin_schema() {
                Some(schema) => schema,
                None => {
                    let _ = execution.request_cancel();
                    return Poll::Ready(Err(internal_error(
                        "SET scalar query result has no schema delivery",
                    )));
                }
            };
            if schema.schema().fields().len() != 1 {
                let message = format!(
                    "user variable assignment expected 1 column, got {}",
                    schema.schema().fields().len()
                );
                schema.fail(QueryExecutionError::new(
                    QueryExecutionErrorKind::InvalidRequest,
                    message.clone(),
                ));
                let _ = execution.request_cancel();
                return Poll::Ready(Err(scalar_query_error(message)));
            }
            let relayed = matches!(
                schema.row_carrier(),
                ResultRowCarrier::Relayed {
                    kind: RootOutputKind::InternalFacts(InternalResultDomain::ScalarValueV1),
                    client_rows: None
                }
            );
            if !relayed || self.schema.is_none() {
                let message = "SET scalar query has no matching frozen typed scalar contract";
                schema.fail(QueryExecutionError::new(
                    QueryExecutionErrorKind::InvalidRequest,
                    message,
                ));
                let _ = execution.request_cancel();
                return Poll::Ready(Err(scalar_query_error(message.to_string())));
            }
            self.schema_started = true;
            schema.complete();
        }
        loop {
            // next() only borrows the original stream. Its Pending branch
            // waits on cancellation-safe channel/watch observations, leaving
            // the receiver and every delivery in the same original fields.
            let next = std::pin::pin!(self.stream.next()).poll(cx);
            let delivery = match next {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(Some(delivery))) => delivery,
                Poll::Ready(Ok(None)) => {
                    let _ = execution.request_cancel();
                    return Poll::Ready(Err(internal_error(
                        "SET scalar query stream ended without success EOF",
                    )));
                }
                Poll::Ready(Err(error)) => {
                    let _ = execution.request_cancel();
                    return Poll::Ready(Err(governed_query_execution_error(error)));
                }
            };
            match delivery {
                ResultDelivery::Segment(delivery) => {
                    let decoded = (|| {
                        if delivery.kind()
                            != RootOutputKind::InternalFacts(InternalResultDomain::ScalarValueV1)
                        {
                            return Err("SET scalar query received a foreign root output domain"
                                .to_string());
                        }
                        if self.value.is_some() {
                            return Err(
                                "SET scalar producer returned more than one record".to_string()
                            );
                        }
                        let frozen = self
                            .schema
                            .as_ref()
                            .expect("a typed scalar carrier has its frozen contract");
                        let record = BorrowedScalarRecord::try_decode(frozen, delivery.body())
                            .map_err(|error| format!("SET scalar record: {error}"))?;
                        if delivery.rows() != record.rows() {
                            return Err(
                                "SET scalar record row count differs from its delivery".to_string()
                            );
                        }
                        novarocks_query_application::sql::user_variable::borrowed_scalar_record_to_user_variable_literal(&record)
                    })();
                    match decoded {
                        Ok(literal) => {
                            self.value = Some(literal);
                            delivery.complete();
                        }
                        Err(message) => {
                            delivery.fail(QueryExecutionError::new(
                                QueryExecutionErrorKind::InvalidRequest,
                                message.clone(),
                            ));
                            let _ = execution.request_cancel();
                            return Poll::Ready(Err(scalar_query_error(message)));
                        }
                    }
                }
                ResultDelivery::End(delivery) => {
                    if self.value.is_none() {
                        let message = "SET typed scalar query ended without its required record";
                        delivery.fail(QueryExecutionError::new(
                            QueryExecutionErrorKind::InvalidRequest,
                            message,
                        ));
                        let _ = execution.request_cancel();
                        return Poll::Ready(Err(scalar_query_error(message.to_string())));
                    }
                    delivery.complete();
                    return Poll::Ready(Ok(self
                        .value
                        .take()
                        .expect("the required original scalar record")));
                }
            }
        }
    }
}

/// Per-scalar fields are part of the original whole statement. They introduce
/// neither a statement registration nor another Root or CPU executor.
struct SetScalarStage {
    provider: Arc<FrontendCompletionCustody>,
    preparation: Option<Preparation>,
    prepared: Option<Result<PreparedQueryOperation, QueryServiceError>>,
    preparation_started: bool,
    preparation_exited: bool,
    provider_close: Option<ProviderClose>,
    provider_closed: Option<Result<(), StatementNativeHealthFailure>>,
    read: Option<crate::query_execution::completion::PreparedLogicalRead>,
    capacity: Option<QueryResultCapacityBinding>,
    original: OriginalComputationStart,
    execution: Option<Result<ExecutionHandle, QueryExecutionError>>,
    scalar: Option<GovernedScalarStream>,
    scalar_schema: Option<novarocks_result_contract::ScalarSchema>,
    literal: Option<String>,
    error: Option<QueryServiceError>,
    native: ReadNativeStageTail,
    variable: String,
}

struct ScalarReadPoll<'a> {
    services: &'a ReadNativeServices,
    planning_cpu: &'a FrontendPlanningCpuSource,
    launcher: &'a dyn LogicalExecutionLauncher,
    health: &'a mut Option<StatementNativeHealthFailure>,
    #[cfg(novarocks_dev_controls)]
    observation: Option<crate::preparation_diagnostics::StatementObservationHandle>,
}

impl SetScalarStage {
    fn retire_unlaunched_inputs(&mut self) {
        if !self.native.is_launched() {
            // These inputs never crossed the launcher's accepted frontier.
            // Their real Drop precedes retirement of the unlaunched stage.
            drop(self.original.input.take());
            drop(self.original.request.take());
            drop(self.read.take());
            drop(self.capacity.take());
            drop(self.prepared.take());
        }
    }

    fn health(&self) -> Option<StatementNativeHealthFailure> {
        self.provider
            .local_health_failure()
            .or_else(|| {
                self.provider_closed
                    .as_ref()
                    .and_then(|r| r.as_ref().err().cloned())
            })
            .or_else(|| self.native.stage.local_health_failure())
    }

    fn poll_preparation(
        &mut self,
        planning_cpu: &FrontendPlanningCpuSource,
        health: &mut Option<StatementNativeHealthFailure>,
        cx: &mut Context<'_>,
    ) -> Poll<()> {
        // Never-started preparation has no running body. Once polled, the same
        // future must finish and every original CPU/Provider must prove exit.
        if self.error.is_some() && !self.preparation_started {
            drop(self.preparation.take());
            self.preparation_exited = true;
        }
        if let Some(future) = self.preparation.as_mut() {
            self.preparation_started = true;
            match future.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(result)) => self.prepared = Some(result),
                Poll::Ready(Err(failure)) => {
                    *health = Some(failure);
                    return Poll::Pending;
                }
            }
            if self.provider.local_health_failure().is_some() {
                return Poll::Pending;
            }
            self.preparation_exited = true;
            drop(self.preparation.take());
        }
        if !self.preparation_exited {
            return Poll::Pending;
        }
        if let Some(close) = self.provider_close.as_mut() {
            match close.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => self.provider_closed = Some(result),
            }
            drop(self.provider_close.take());
        }
        match self.provider_closed.as_ref() {
            Some(Ok(())) => {}
            Some(Err(failure)) => {
                *health = Some(failure.clone());
                return Poll::Pending;
            }
            None => return Poll::Pending,
        }
        match planning_cpu.poll_quiescent(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(failure)) => {
                *health = Some(failure);
                Poll::Pending
            }
            Poll::Ready(Ok(())) => Poll::Ready(()),
        }
    }

    fn poll_scalar(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        runtime: ScalarReadPoll<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), QueryServiceError>> {
        let ScalarReadPoll {
            services,
            planning_cpu,
            launcher,
            health,
            #[cfg(novarocks_dev_controls)]
            observation,
        } = runtime;
        if let Some(reason) = owner.cancellation().reason() {
            self.error
                .get_or_insert_with(|| governed_cancellation_error(reason));
        }
        if self.poll_preparation(planning_cpu, health, cx).is_pending() {
            return Poll::Pending;
        }
        if let Some(prepared) = self.prepared.as_ref() {
            match prepared {
                Err(error) => {
                    self.error.get_or_insert_with(|| error.clone());
                }
                Ok(PreparedQueryOperation::LogicalRead(read)) => {
                    // Freeze the scalar contract before the move-only Read
                    // description goes to the unique Native launcher.
                    self.scalar_schema = read.scalar_schema().cloned();
                }
                Ok(PreparedQueryOperation::Immediate(_)) => {}
            }
        }
        if self.error.is_none()
            && let Some(prepared) = self.prepared.take()
        {
            match prepared {
                Ok(PreparedQueryOperation::LogicalRead(read)) => self.read = Some(read),
                Ok(PreparedQueryOperation::Immediate(immediate)) => {
                    self.literal = Some(match immediate.into_result() {
                        StatementResult::Query(result) => novarocks_query_application::sql::user_variable::local_mv_scalar_result_to_user_variable_literal(&result).map_err(scalar_query_error)?,
                        _ => return Poll::Ready(Err(internal_error("SET scalar preparation returned non-query immediate output"))),
                    });
                }
                Err(_) => unreachable!("the retained preparation error was selected above"),
            }
        }
        if self.error.is_none()
            && let Some(error) = services.io().original().and_then(|io| io.failure())
        {
            self.error = Some(QueryServiceError::from_failure_cause(
                novarocks_failure_contract::SharedFailureCause::new(error),
            ));
        }
        if self.error.is_none() && self.read.is_some() {
            if !services.state_store_quiescent(cx, health) {
                return Poll::Pending;
            }
            let io = services
                .io()
                .original()
                .expect("SET preparation installed the original I/O source");
            let work = owner
                .execution_owner_slot()
                .as_ref()
                .expect("the whole SET retains its original Root");
            match self.native.stage.poll_admit(&io, work, cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => self.error = Some(error),
                Poll::Ready(Ok(())) => {}
            }
            if self.error.is_none()
                && self.capacity.is_none()
                && let Err(error) = self
                    .native
                    .stage
                    .install_internal_read_capacity_in(&io, &mut self.capacity)
            {
                self.error = Some(error);
            }
            if self.error.is_none() {
                match launcher.start_read_in(
                    &mut self.read,
                    self.native.stage.owner_slot(),
                    self.capacity
                        .as_ref()
                        .expect("the original Internal computation"),
                    services.io(),
                    &mut self.original,
                    #[cfg(novarocks_dev_controls)]
                    observation,
                ) {
                    Err(error) => self.error = Some(governed_query_execution_error(error)),
                    Ok(notification) => {
                        // The accepted body/exit receiver is stable before its
                        // original notification can invoke any callback.
                        self.native.observe_start_in(
                            &notification,
                            &mut self.original.exit,
                            health,
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
        if self.scalar.is_none() && self.error.is_none() {
            if let Some(Ok(execution)) = self.execution.as_mut() {
                let output = execution.take_output();
                match output {
                    Some(ExecutionOutput::Rows(stream)) => {
                        self.scalar = Some(GovernedScalarStream::new(
                            stream,
                            self.scalar_schema.clone(),
                        ))
                    }
                    Some(ExecutionOutput::Completion) => {
                        self.error = Some(internal_error(
                            "SET scalar query returned completion-only output",
                        ))
                    }
                    None => {
                        self.error = Some(internal_error(
                            "SET scalar query output was already transferred",
                        ))
                    }
                }
            } else if let Some(Err(error)) = self.execution.as_ref() {
                self.error = Some(governed_query_execution_error(error.clone()));
            }
        }
        if self.error.is_none()
            && let (Some(scalar), Some(Ok(execution))) =
                (self.scalar.as_mut(), self.execution.as_mut())
        {
            match scalar.poll_value(execution, cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(literal)) => self.literal = Some(literal),
                Poll::Ready(Err(error)) => self.error = Some(error),
            }
        }
        if let Some(error) = self.error.as_ref() {
            if let Some(Ok(execution)) = self.execution.as_ref() {
                let _ = execution.request_cancel();
            }
            return Poll::Ready(Err(error.clone()));
        }
        if self.literal.is_some() {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}

/// The one whole-SET service/Native residual. Per-scalar computation positions
/// exit at the original pump cut while this same tail keeps data and Unknown.
struct SetNativeTail {
    services: ReadNativeServices,
    planning_cpu: Arc<FrontendPlanningCpuSource>,
    current: Option<SetScalarStage>,
    previous: Vec<SetScalarStage>,
    health: Option<StatementNativeHealthFailure>,
    any_launched: bool,
}
impl SetNativeTail {
    fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        self.health
            .clone()
            .or_else(|| self.planning_cpu.local_health_failure())
            .or_else(|| self.services.io().health_failure())
            .or_else(|| self.current.as_ref().and_then(SetScalarStage::health))
            .or_else(|| self.previous.iter().find_map(SetScalarStage::health))
    }
    fn poll_native_stages(&mut self, cx: &mut Context<'_>) -> bool {
        let mut exited = true;
        for scalar in self.current.iter_mut().chain(self.previous.iter_mut()) {
            if scalar
                .native
                .poll_stage_exit(cx, &mut self.health)
                .is_pending()
            {
                exited = false;
            }
        }
        exited && self.health.is_none()
    }
    fn poll_preparation_exit(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if let Some(scalar) = self.current.as_mut()
            && scalar
                .poll_preparation(&self.planning_cpu, &mut self.health, cx)
                .is_pending()
        {
            return Poll::Pending;
        }
        match self.planning_cpu.poll_quiescent(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(health)) => {
                self.health = Some(health);
                Poll::Pending
            }
            Poll::Ready(Ok(())) => Poll::Ready(()),
        }
    }
}
impl GovernedStatementResidual for SetNativeTail {
    fn poll_retirement(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        self.services.poll_state_store(cx, &mut self.health);
        if let Some(health) = self.local_health_failure() {
            return Poll::Ready(Err(health));
        }
        if self.poll_preparation_exit(cx).is_pending() {
            return Poll::Pending;
        }
        for scalar in self.current.iter_mut().chain(self.previous.iter_mut()) {
            scalar.retire_unlaunched_inputs();
        }
        let store_closed = matches!(
            self.services.poll_state_store_closed(cx, &mut self.health),
            Poll::Ready(Ok(()))
        );
        let mut io_withdrawn = false;
        if !self.any_launched {
            io_withdrawn = self.services.withdraw_unstarted_io(&mut self.health)?;
        }
        if !io_withdrawn {
            self.services.poll_io_progress(cx, &mut self.health)?;
        }
        let native_exited = self.poll_native_stages(cx);
        let io_exited = io_withdrawn
            || matches!(
                self.services.poll_io_quiescent(cx, &mut self.health),
                Poll::Ready(Ok(()))
            );
        let cpu_closed = matches!(self.planning_cpu.poll_closed(cx), Poll::Ready(Ok(())));
        self.services.poll_state_store(cx, &mut self.health);
        if let Some(health) = self.local_health_failure() {
            return Poll::Ready(Err(health));
        }
        if !(store_closed && native_exited && io_exited && cpu_closed) {
            return Poll::Pending;
        }
        for scalar in self.current.iter_mut().chain(self.previous.iter_mut()) {
            scalar.native.stage.retire_unlaunched();
        }
        Poll::Ready(Ok(()))
    }
}

struct SetStatementAction {
    source: Arc<str>,
    set: ast::SetStatement,
    cursor: usize,
    live: Arc<Mutex<SessionSqlState>>,
    staged: Option<SessionSqlState>,
    effects: SessionEffects,
    resolver: SessionCatalogService,
    compiler: FrontendQueryCompiler,
    topology: BackendTopologyService,
    role: ClusterRole,
    optimizer_limit: u64,
    deadline: Option<Instant>,
    state_store: Option<Arc<dyn StateStoreExecutionRequests>>,
    launcher: Arc<dyn LogicalExecutionLauncher>,
    tail: Option<Box<SetNativeTail>>,
    error: Option<QueryServiceError>,
    delivered: bool,
    #[cfg(novarocks_dev_controls)]
    observation: Option<crate::preparation_diagnostics::StatementObservationHandle>,
    #[cfg(novarocks_dev_controls)]
    statement_token: StatementToken,
}

impl SetStatementAction {
    fn poll_set(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatementActionValue, QueryServiceError>> {
        let tail = self.tail.as_mut().expect("the whole SET original residual");
        if tail.local_health_failure().is_some() {
            return Poll::Pending;
        }
        if let Some(reason) = owner.cancellation().reason() {
            self.error
                .get_or_insert_with(|| governed_cancellation_error(reason.clone()));
            if let Some(current) = tail.current.as_mut() {
                current
                    .error
                    .get_or_insert_with(|| governed_cancellation_error(reason));
            }
        }
        let work = owner
            .execution_owner_slot()
            .as_ref()
            .expect("the whole SET Root");
        match tail.services.io().poll_progress(work, cx) {
            Ok(false) if self.error.is_none() => return Poll::Pending,
            Ok(_) => {}
            Err(error) => {
                self.error.get_or_insert_with(|| {
                    QueryServiceError::from_failure_cause(
                        novarocks_failure_contract::SharedFailureCause::new(error),
                    )
                });
            }
        }
        // Only the original registered action clones and mutates the staged
        // state. The client waiter has no state-commit authority.
        if self.staged.is_none() && self.error.is_none() {
            match self.live.lock() {
                Ok(live) => self.staged = Some(live.clone()),
                Err(error) => self.error = Some(poisoned_state(error)),
            }
        }
        if let Some(current) = tail.current.as_mut() {
            match current.poll_scalar(
                owner,
                ScalarReadPoll {
                    services: &tail.services,
                    planning_cpu: &tail.planning_cpu,
                    launcher: self.launcher.as_ref(),
                    health: &mut tail.health,
                    #[cfg(novarocks_dev_controls)]
                    observation: self.observation.clone(),
                },
                cx,
            ) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => {
                    self.error.get_or_insert(error);
                }
                Poll::Ready(Ok(())) => {
                    let literal = current
                        .literal
                        .take()
                        .expect("the scalar's original literal");
                    if let Err(message) = self
                        .staged
                        .as_mut()
                        .expect("original staged state")
                        .set_user_variable(&current.variable, literal)
                    {
                        self.error = Some(QueryServiceError::new(
                            QueryServiceErrorKind::InvalidValue,
                            message,
                        ));
                    } else {
                        self.cursor += 1;
                        tail.any_launched |= current.native.is_launched();
                        tail.previous
                            .push(tail.current.take().expect("same completed scalar"));
                    }
                }
            }
        }
        if let Some(error) = self.error.as_ref() {
            if let Some(current) = tail.current.as_mut() {
                current.error.get_or_insert_with(|| error.clone());
                if let Some(Ok(execution)) = current.execution.as_ref() {
                    let _ = execution.request_cancel();
                }
                tail.any_launched |= current.native.is_launched();
            }
            if tail.poll_preparation_exit(cx).is_pending() {
                return Poll::Pending;
            }
            match tail.planning_cpu.poll_closed(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(health)) => {
                    tail.health = Some(health);
                    return Poll::Pending;
                }
                Poll::Ready(Ok(())) => {}
            }
            self.delivered = true;
            return Poll::Ready(Err(error.clone()));
        }
        while self.cursor < self.set.assignments.len() {
            let assignment = &self.set.assignments[self.cursor];
            let staged = self.staged.as_mut().expect("original staged state");
            if let ast::SetTarget::UserVariable(variable) = &assignment.target {
                match &assignment.value {
                    ast::SetValue::Query(query) => {
                        let parsed = ParsedStatement::Query((**query).clone());
                        let provider = FrontendCompletionCustody::new();
                        let provider_close = Arc::clone(&provider);
                        let input = ReadPreparationInput {
                            compiler: self.compiler.clone(),
                            topology: self.topology.clone(),
                            role: self.role,
                            optimizer_limit: self.optimizer_limit,
                            // The AST spans belong to the whole admitted SET source.
                            sql: Arc::clone(&self.source),
                            parsed,
                            state: staged.clone(),
                            deadline: self.deadline,
                            cancellation: QueryCancellationView::governed(
                                owner.cancellation().clone(),
                                owner.timeout_ms(),
                            ),
                            scope: owner.scope().clone(),
                            window: owner
                                .result_window_alias()
                                .expect("the admitted SET data window"),
                            purpose: FrontendQueryPurpose::ScalarValue,
                            provider: Arc::clone(&provider),
                            planning_cpu: Arc::clone(&tail.planning_cpu),
                            io: Arc::clone(tail.services.io()),
                            state_store: self.state_store.clone(),
                            #[cfg(novarocks_dev_controls)]
                            observation: self.observation.clone(),
                            #[cfg(novarocks_dev_controls)]
                            statement_token: self.statement_token,
                        };
                        tail.current = Some(SetScalarStage {
                            provider,
                            preparation: Some(Box::pin(prepare_read(input))),
                            prepared: None,
                            preparation_started: false,
                            preparation_exited: false,
                            provider_close: Some(Box::pin(
                                async move { provider_close.close().await },
                            )),
                            provider_closed: None,
                            read: None,
                            capacity: None,
                            original: OriginalComputationStart::default(),
                            execution: None,
                            scalar: None,
                            scalar_schema: None,
                            literal: None,
                            error: None,
                            native: ReadNativeStageTail::new(StatementStageAdmission::new(
                                owner.scope().clone(),
                                0,
                            )),
                            variable: variable.value.clone(),
                        });
                        cx.waker().wake_by_ref();
                        return Poll::Pending;
                    }
                    ast::SetValue::Expression(value) => {
                        if let Err(message) =
                            staged.set_user_variable(&variable.value, print_expr(value))
                        {
                            self.error = Some(QueryServiceError::new(
                                QueryServiceErrorKind::InvalidValue,
                                message,
                            ));
                            break;
                        }
                    }
                    ast::SetValue::Words(_) => {
                        self.error = Some(QueryServiceError::new(
                            QueryServiceErrorKind::InvalidValue,
                            "user variable assignment requires an expression",
                        ));
                        break;
                    }
                }
            } else {
                match apply_session_assignment_in(&self.resolver, &self.source, assignment, staged)
                {
                    Ok(change) => self.effects.append(change),
                    Err(error) => {
                        self.error = Some(error);
                        break;
                    }
                }
            }
            self.cursor += 1;
        }
        if self.error.is_some() {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        match tail.planning_cpu.poll_closed(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(health)) => {
                tail.health = Some(health);
                return Poll::Pending;
            }
            Poll::Ready(Ok(())) => {}
        }
        let mut live = match self.live.lock() {
            Ok(live) => live,
            Err(error) => {
                self.delivered = true;
                return Poll::Ready(Err(poisoned_state(error)));
            }
        };
        match owner.seal_success_visibility() {
            GovernedStatementVisibilitySealOutcome::Sealed => {
                // Keep the previous live state in this same original action;
                // no destructor or foreign callback lies inside this move cut.
                std::mem::swap(
                    &mut *live,
                    self.staged.as_mut().expect("all assignments staged"),
                );
                self.delivered = true;
                Poll::Ready(Ok(StatementActionValue::SessionChanges(std::mem::take(
                    &mut self.effects,
                ))))
            }
            GovernedStatementVisibilitySealOutcome::Cancelled(reason) => {
                self.error = Some(governed_cancellation_error(reason));
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            GovernedStatementVisibilitySealOutcome::Stale
            | GovernedStatementVisibilitySealOutcome::Failed => {
                self.error = Some(internal_error(
                    "governed SET query lost its statement generation before state commit",
                ));
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}

impl GovernedStatementAction for SetStatementAction {
    fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        self.tail
            .as_ref()
            .and_then(|tail| tail.local_health_failure())
    }
    fn handoff_residual_in(
        &mut self,
        target: &mut Option<Box<dyn GovernedStatementResidual>>,
    ) -> Result<(), StatementNativeHealthFailure> {
        if self.delivered {
            if target.is_some() {
                return Err(StatementNativeHealthFailure::actual_exit_unproven(
                    "SET residual receiver is occupied",
                    Arc::new(WorkError::Conflict),
                ));
            }
            *target = Some(self.tail.take().expect("the whole original SET residual"));
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
        let result = self.poll_set(owner, cx);
        if let Some(tail) = self.tail.as_mut() {
            tail.services.poll_state_store(cx, &mut tail.health);
        }
        result
    }
}

// The session's ordinary SET route uses this same original policy author.
pub(super) fn apply_session_assignment_in(
    resolver: &SessionCatalogService,
    source: &str,
    assignment: &ast::SetAssignment,
    state: &mut SessionSqlState,
) -> Result<SessionEffects, QueryServiceError> {
    use novarocks_query_application::sql::session::{
        SessionAssignment, session_set_assignment_effects,
    };
    let effects = match session_set_assignment_effects(source, assignment)? {
        SessionAssignment::Effects(effects) => effects,
        SessionAssignment::UserVariable { name, value } => {
            state.set_user_variable(&name, value).map_err(|detail| {
                QueryServiceError::new(QueryServiceErrorKind::InvalidValue, detail)
            })?;
            return Ok(SessionEffects::default());
        }
        SessionAssignment::SelectCatalog(catalog) => {
            let catalog = resolve_catalog_name(resolver, &catalog)?;
            let database_exists =
                catalog.is_some() || resolver.database_exists(state.current_database())?;
            let schema = if catalog.is_none() && !database_exists {
                DEFAULT_DATABASE.to_owned()
            } else {
                state.current_database().to_owned()
            };
            SessionEffects::set_catalog_assignment(catalog, schema)
        }
    };
    state.apply_effects(&effects)?;
    Ok(effects)
}

impl FrontendQuerySession {
    pub(super) async fn execute_observed_set(
        &self,
        source: Arc<str>,
        set: ast::SetStatement,
        deadline: Option<Instant>,
        statement: GovernedQueryStatementOwner,
    ) -> Result<StatementResult, QueryServiceError> {
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
        let profile = super::read_action::local_preparation_profile_with_dependencies(dependencies)
            .map_err(|error| internal_error(error.to_string()))?;
        let io = super::read_action::io_source::ReadMetadataIoSource::new(
            statement.scope().clone(),
            profile.clone(),
        );
        novarocks_spi::connector::ConnectorIoAdmissionPort::configure_read_source(
            io.as_ref(),
            &profile,
        )
        .map_err(|error| internal_error(error.to_string()))?;
        let (state_store_owner, state_store_requests) = match state_store {
            Some((owner, requests)) => (Some(owner), Some(requests)),
            None => (None, None),
        };
        let planning_cpu = self.service.query_compiler.new_planning_cpu_source();
        let action = SetStatementAction {
            source,
            set,
            cursor: 0,
            live: Arc::clone(&self.state),
            staged: None,
            effects: SessionEffects::default(),
            resolver: Arc::clone(&self.service.session_catalog_resolver),
            compiler: self.service.query_compiler.clone(),
            topology: self.service.topology.clone(),
            role: self.service.role,
            optimizer_limit: self.service.optimizer_query_mem_limit_bytes,
            deadline,
            state_store: state_store_requests,
            launcher: Arc::clone(&self.service.logical_execution_launcher),
            tail: Some(Box::new(SetNativeTail {
                services: ReadNativeServices::new(state_store_owner, io),
                planning_cpu,
                current: None,
                previous: Vec::new(),
                health: None,
                any_launched: false,
            })),
            error: None,
            delivered: false,
            #[cfg(novarocks_dev_controls)]
            observation: self
                .service
                .statement_observation
                .as_ref()
                .and_then(|owner| owner.bound_statement(statement.token())),
            #[cfg(novarocks_dev_controls)]
            statement_token: statement.token(),
        };
        self.execute_observed_statement_action(statement, Box::new(action))
            .await
    }
}
