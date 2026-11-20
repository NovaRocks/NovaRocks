// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Typed CREATE TABLE and SET PROPERTIES preparation under the admitted statement.

use super::{
    FrontendQuerySession, QueryCancellationView, QueryServiceError, StatementResult,
    compiler::FrontendQueryCompiler,
    governed_cancellation_error, internal_error, product_command_service_error,
    table_mutation_action::{self, AlterTableMvGuard, PreparedTableMutationInput},
    table_property_computation::{
        LikeColumnsCpuInput, PreparedTrinoTableLike, TrinoTablePropertyCpuInput,
        TrinoTablePropertyOutput, TrinoTablePropertySyntax,
    },
};
use crate::query_execution::{
    completion_facts::FrontendPlanningCpuSource, internal_result_cpu::InternalResultCpu,
    like_source_read::LikeSourceRead, maintenance::io_admission::MetadataIoAdmission,
    planning::sql_cancellation_observation, required_constant_eval::RequiredConstantInterpreter,
    table_exists_source::TableExistsSource,
};
use novarocks_parser::ast::{
    AstVec, ObjectName as ParsedObjectName, StatementProperty, StatementSaveMode, TrinoCreateTable,
};
use novarocks_query_application::{
    admitted_query_context::RequestContext,
    api::{
        CatalogTablePropertyBinding, CatalogTableReadBinding, CommandContext,
        StatementExternalEffectDisposition, StatementNativeHealthFailure,
    },
    session_control::{
        GovernedActionContext, GovernedQueryStatementOwner, GovernedStatementAction,
        GovernedStatementResidual, StatementActionValue, StatementCommandCompletion,
    },
    sql::{
        ProductCommandRouter, dml_admission::DmlAdmissionError, lower_object_name,
        trino_admission::unsupported_statement,
    },
};
use novarocks_sql::{
    compiler::SqlCompileControl,
    semantic::{ObjectName, TableColumnDef},
};
use novarocks_workload_control::WorkError;
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Instant,
};

enum Prepared {
    Like(Vec<TableColumnDef>),
    Table(PreparedTableMutationInput),
    Trino(TrinoTablePropertyOutput),
}
type Preparation = Pin<
    Box<
        dyn Future<
                Output = Result<Result<Prepared, QueryServiceError>, StatementNativeHealthFailure>,
            > + Send,
    >,
>;

struct Tail {
    cpu: Arc<FrontendPlanningCpuSource>,
    preparation: Option<Preparation>,
    prepared: Option<Result<Prepared, QueryServiceError>>,
    read: Option<LikeSourceRead>,
    inner: Option<Box<dyn GovernedStatementAction>>,
    residual: Option<Box<dyn GovernedStatementResidual>>,
    health: Option<StatementNativeHealthFailure>,
}
impl Tail {
    fn health(&self) -> Option<StatementNativeHealthFailure> {
        self.health
            .clone()
            .or_else(|| self.cpu.local_health_failure())
            .or_else(|| {
                self.read
                    .as_ref()
                    .and_then(LikeSourceRead::local_health_failure)
            })
            .or_else(|| {
                self.inner
                    .as_ref()
                    .and_then(|inner| inner.local_health_failure())
            })
    }
    fn poll_turn(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if let Some(future) = self.preparation.as_mut() {
            match future.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(result)) => self.prepared = Some(result),
                Poll::Ready(Err(health)) => self.health = Some(health),
            }
            // receive() already consumed the original completion and retired
            // this job after actual exit. Keep the same Source open for the
            // next finite CPU phase; poll_closed permanently seals it.
            drop(self.preparation.take());
        }
        if self.health().is_some() {
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }
    fn poll_preparation_closed(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if let Some(read) = self.read.as_mut() {
            std::task::ready!(read.poll_close(cx));
        }
        self.poll_cpu_closed(cx)
    }
    fn poll_cpu_closed(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        std::task::ready!(self.poll_turn(cx));
        match self.cpu.poll_closed(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(health)) => {
                self.health = Some(health);
                Poll::Pending
            }
            Poll::Ready(Ok(())) => Poll::Ready(()),
        }
    }
}
impl GovernedStatementResidual for Tail {
    fn poll_retirement(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        if let Some(health) = self.health() {
            return Poll::Ready(Err(health));
        }
        let closed = self.poll_preparation_closed(cx);
        if let Some(health) = self.health() {
            return Poll::Ready(Err(health));
        }
        std::task::ready!(closed);
        match self.residual.as_mut() {
            Some(residual) => residual.poll_retirement(cx),
            None => Poll::Ready(Ok(())),
        }
    }
}

pub(super) enum TablePropertyInput {
    TrinoCreate(TrinoCreateTable),
    TrinoSet {
        name: ParsedObjectName,
        properties: AstVec<StatementProperty>,
    },
}

enum Phase {
    Start,
    Probe,
    TrinoProperties,
    NoOp,
    Read,
    LikeColumns,
    Properties,
}
struct Action {
    source: Arc<str>,
    pending: Option<TablePropertyInput>,
    target: Option<CatalogTableReadBinding>,
    exists: Option<TableExistsSource>,
    continued_io: Option<Arc<MetadataIoAdmission>>,
    trino_like: Option<Box<PreparedTrinoTableLike>>,
    compiler: FrontendQueryCompiler,
    interpreter: Arc<RequiredConstantInterpreter>,
    router: ProductCommandRouter,
    alter_mv_guard: AlterTableMvGuard,
    context: RequestContext,
    command: Option<CommandContext>,
    control: SqlCompileControl,
    cpu: InternalResultCpu,
    cancellation: QueryCancellationView,
    phase: Phase,
    tail: Option<Box<Tail>>,
    error: Option<QueryServiceError>,
    delivered: bool,
}
impl Action {
    fn start_trino_properties(
        &mut self,
        owner: &GovernedActionContext<'_>,
        binding: CatalogTablePropertyBinding,
    ) -> Result<(), QueryServiceError> {
        let syntax = match self.pending.take().expect("one typed table property input") {
            TablePropertyInput::TrinoCreate(statement) => {
                TrinoTablePropertySyntax::Create(statement)
            }
            TablePropertyInput::TrinoSet { properties, .. } => {
                TrinoTablePropertySyntax::Set(properties)
            }
        };
        let window = owner
            .result_window_alias()
            .ok_or_else(|| internal_error("table properties have no original result window"))?;
        let input = TrinoTablePropertyCpuInput {
            source: self.source.clone(),
            syntax,
            binding,
            command: self
                .command
                .as_ref()
                .expect("original table command")
                .clone(),
            context: self.compiler.property_cpu_context(
                self.context.session().current_database().to_owned(),
                self.context.execution().sql_semantics().clone(),
                self.interpreter.clone(),
                self.control.clone(),
                owner.scope().clone(),
                window,
            ),
        };
        let tail = self.tail.as_mut().expect("original table residual");
        let cpu = tail.cpu.clone();
        assert!(tail.preparation.is_none() && tail.prepared.is_none());
        tail.preparation = Some(Box::pin(async move {
            cpu.trino_table_properties(input)
                .await
                .map(|r| r.map(Prepared::Trino))
        }));
        self.phase = Phase::TrinoProperties;
        Ok(())
    }

    fn start_like(&mut self, source: &ObjectName) -> Result<(), QueryServiceError> {
        let binding = self
            .router
            .acquire_table_read_binding(source, &self.context)
            .map_err(product_command_service_error)?;
        let read = LikeSourceRead::try_new(
            binding,
            self.command
                .as_ref()
                .expect("original table command")
                .clone(),
            self.continued_io.take(),
            self.cpu.clone(),
            self.cancellation.clone(),
            self.compiler.connector_runtime().clone(),
        )?;
        self.tail.as_mut().expect("original table residual").read = Some(read);
        self.phase = Phase::Read;
        Ok(())
    }

    fn start_probe(&mut self, name: ObjectName) -> Result<(), QueryServiceError> {
        let target = self
            .router
            .acquire_table_read_binding(&name, &self.context)
            .map_err(product_command_service_error)?;
        let source =
            CatalogTableReadBinding::try_new(target.table().clone(), target.planning().clone())
                .map_err(product_command_service_error)?;
        self.exists = Some(TableExistsSource::try_new(
            source,
            self.command
                .as_ref()
                .expect("original table command")
                .clone(),
            self.cancellation.clone(),
        )?);
        self.target = Some(target);
        self.phase = Phase::Probe;
        Ok(())
    }

    fn poll_probe(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<bool, QueryServiceError>> {
        let probe = self.exists.as_mut().expect("original target probe");
        let result = std::task::ready!(probe.poll_result(owner, cx));
        if matches!(result, Ok(false)) {
            self.continued_io = Some(
                probe
                    .take_completed_io()
                    .expect("successful target probe source"),
            );
        }
        self.exists.take();
        Poll::Ready(result)
    }

    fn advance(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), QueryServiceError>> {
        loop {
            match self.phase {
                Phase::Start => match self.pending.as_ref().expect("one table input") {
                    TablePropertyInput::TrinoCreate(node) => {
                        if node.save_mode == StatementSaveMode::Replace {
                            return Poll::Ready(Err(unsupported_statement(
                                &self.source,
                                node.span,
                                "CREATE OR REPLACE TABLE",
                            )));
                        }
                        self.start_probe(lower_object_name(&node.name))?;
                    }
                    TablePropertyInput::TrinoSet { name, .. } => {
                        let binding = self
                            .router
                            .acquire_table_property_binding(&lower_object_name(name), &self.context)
                            .map_err(product_command_service_error)?;
                        self.start_trino_properties(owner, binding)?;
                    }
                },
                Phase::Probe => {
                    let exists = std::task::ready!(self.poll_probe(owner, cx))?;
                    let Some(TablePropertyInput::TrinoCreate(node)) = self.pending.as_ref() else {
                        unreachable!("same CREATE TABLE for the original target");
                    };
                    let target = self.target.take().expect("original table target");
                    if exists {
                        if node.save_mode == StatementSaveMode::Ignore {
                            self.phase = Phase::NoOp;
                            return Poll::Ready(Ok(()));
                        }
                        let table = target.table();
                        return Poll::Ready(Err(QueryServiceError::from_user_error(
                            DmlAdmissionError::TableAlreadyExists.to_user_error(
                                &self.source,
                                node.span,
                                format!(
                                    "Table '{}.{}.{}' already exists",
                                    table.instance_id.as_str(),
                                    table.namespace,
                                    table.table
                                ),
                            ),
                        )));
                    }
                    let (table, pin) = target.into_parts();
                    let binding = CatalogTablePropertyBinding::try_new(table, pin)
                        .map_err(product_command_service_error)?;
                    self.start_trino_properties(owner, binding)?;
                }
                Phase::NoOp => return Poll::Ready(Ok(())),
                Phase::TrinoProperties => {
                    let tail = self.tail.as_mut().expect("original table residual");
                    std::task::ready!(tail.poll_turn(cx));
                    let result = tail.prepared.take().expect("typed table property result")?;
                    let Prepared::Trino(result) = result else {
                        unreachable!("same typed table property CPU case");
                    };
                    match result {
                        TrinoTablePropertyOutput::Mutation(input) => {
                            tail.prepared = Some(Ok(Prepared::Table(input)));
                            self.phase = Phase::Properties;
                        }
                        TrinoTablePropertyOutput::Like(input) => {
                            self.start_like(input.table.like.as_ref().expect("bound LIKE source"))?;
                            self.trino_like = Some(input);
                        }
                    }
                }
                Phase::Read => {
                    let tail = self.tail.as_mut().expect("same table residual");
                    let metadata = std::task::ready!(
                        tail.read
                            .as_mut()
                            .expect("original LIKE read")
                            .poll_read(owner, cx)
                    )?;
                    let input = LikeColumnsCpuInput {
                        metadata,
                        control: self.control.clone(),
                    };
                    let cpu = tail.cpu.clone();
                    tail.preparation = Some(Box::pin(async move {
                        cpu.like_columns(input).await.map(|r| r.map(Prepared::Like))
                    }));
                    self.phase = Phase::LikeColumns;
                }
                Phase::LikeColumns => {
                    let tail = self.tail.as_mut().expect("same table residual");
                    std::task::ready!(tail.poll_turn(cx));
                    let prepared = tail
                        .prepared
                        .take()
                        .expect("original LIKE column CPU response")?;
                    let Prepared::Like(columns) = prepared else {
                        unreachable!("same finite LIKE CPU case")
                    };
                    let table = self.trino_like.take().expect("original typed LIKE input");
                    let cpu = tail.cpu.clone();
                    tail.preparation = Some(Box::pin(async move {
                        cpu.finish_trino_table_like(table, columns)
                            .await
                            .map(|r| r.map(Prepared::Table))
                    }));
                    self.phase = Phase::Properties;
                }
                Phase::Properties => {
                    let tail = self.tail.as_mut().expect("same table residual");
                    std::task::ready!(tail.poll_cpu_closed(cx));
                    let prepared = tail.prepared.take().expect("original table CPU response")?;
                    let Prepared::Table(prepared) = prepared else {
                        unreachable!("same finite table CPU case")
                    };
                    let continued_io = match tail.read.as_mut() {
                        Some(read) => Some(read.take_completed_io().ok_or_else(|| {
                            internal_error("LIKE read lost its original I/O continuation")
                        })?),
                        None => self.continued_io.take(),
                    };
                    let inner = table_mutation_action::prepare(
                        prepared,
                        continued_io,
                        self.command.take().expect("one original table command"),
                        self.cpu.clone(),
                        self.cancellation.clone(),
                        self.compiler.connector_runtime().clone(),
                        &self.alter_mv_guard,
                    )
                    .map_err(product_command_service_error)?;
                    tail.inner = Some(inner);
                    return Poll::Ready(Ok(()));
                }
            }
        }
    }
}
impl GovernedStatementAction for Action {
    fn external_effect_disposition(&self) -> Option<StatementExternalEffectDisposition> {
        self.tail
            .as_ref()
            .and_then(|tail| tail.inner.as_ref())
            .and_then(|inner| inner.external_effect_disposition())
    }
    fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        self.exists
            .as_ref()
            .and_then(TableExistsSource::local_health_failure)
            .or_else(|| self.tail.as_ref().and_then(|tail| tail.health()))
    }
    fn handoff_residual_in(
        &mut self,
        target: &mut Option<Box<dyn GovernedStatementResidual>>,
    ) -> Result<(), StatementNativeHealthFailure> {
        if !self.delivered {
            return Ok(());
        }
        if target.is_some() {
            return Err(StatementNativeHealthFailure::actual_exit_unproven(
                "table preparation residual receiver is occupied",
                Arc::new(WorkError::Conflict),
            ));
        }
        let tail = self.tail.as_mut().expect("same original table residual");
        if let Some(inner) = tail.inner.as_mut() {
            inner.handoff_residual_in(&mut tail.residual)?;
        }
        *target = Some(self.tail.take().expect("same original table residual"));
        Ok(())
    }
    fn poll_action(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatementActionValue, QueryServiceError>> {
        if self.local_health_failure().is_some() {
            return Poll::Pending;
        }
        if let Some(inner) = self
            .tail
            .as_mut()
            .expect("same table residual")
            .inner
            .as_mut()
        {
            let result = inner.poll_action(owner, cx);
            self.delivered |= result.is_ready();
            return result;
        }
        if let Some(reason) = owner.cancellation().reason() {
            self.error
                .get_or_insert_with(|| governed_cancellation_error(reason));
        }
        if self.error.is_none() {
            match self.advance(owner, cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => self.error = Some(error),
                Poll::Ready(Ok(())) if matches!(self.phase, Phase::NoOp) => {
                    let tail = self.tail.as_mut().expect("original table residual");
                    std::task::ready!(tail.poll_preparation_closed(cx));
                    self.delivered = true;
                    return Poll::Ready(Ok(StatementActionValue::Ok(
                        StatementCommandCompletion::empty(),
                    )));
                }
                Poll::Ready(Ok(())) => {
                    let result = self
                        .tail
                        .as_mut()
                        .expect("same table residual")
                        .inner
                        .as_mut()
                        .expect("prepared original action")
                        .poll_action(owner, cx);
                    self.delivered |= result.is_ready();
                    return result;
                }
            }
        }
        if self.exists.is_some() {
            let _ = std::task::ready!(self.poll_probe(owner, cx));
        }
        std::task::ready!(
            self.tail
                .as_mut()
                .expect("same table residual")
                .poll_preparation_closed(cx)
        );
        self.delivered = true;
        Poll::Ready(Err(self
            .error
            .as_ref()
            .expect("selected original table failure")
            .clone()))
    }
}
impl FrontendQuerySession {
    pub(super) async fn execute_observed_table_properties(
        &self,
        source: Arc<str>,
        pending: TablePropertyInput,
        context: RequestContext,
        command: CommandContext,
        deadline: Option<Instant>,
        statement: GovernedQueryStatementOwner,
    ) -> Result<StatementResult, QueryServiceError> {
        let cpu = match self.service.query_compiler.table_metadata_cpu() {
            Ok(cpu) => cpu,
            Err(error) => {
                return Ok(self.governed_typed_error(
                    QueryServiceError::from_failure_cause(
                        novarocks_failure_contract::SharedFailureCause::new(error),
                    ),
                    statement,
                ));
            }
        };
        let cancellation = QueryCancellationView::governed(
            statement.cancellation().clone(),
            statement.timeout_ms(),
        );
        let action = Action {
            source,
            pending: Some(pending),
            target: None,
            exists: None,
            continued_io: None,
            trino_like: None,
            compiler: self.service.query_compiler.clone(),
            interpreter: self.service.required_constant_interpreter.clone(),
            router: self.service.product_command_router.clone(),
            alter_mv_guard: self.service.alter_mv_guard.clone(),
            context,
            command: Some(command),
            control: SqlCompileControl::new(
                deadline,
                sql_cancellation_observation(cancellation.clone()),
            ),
            cpu,
            cancellation,
            phase: Phase::Start,
            tail: Some(Box::new(Tail {
                cpu: self.service.query_compiler.new_planning_cpu_source(),
                preparation: None,
                prepared: None,
                read: None,
                inner: None,
                residual: None,
                health: None,
            })),
            error: None,
            delivered: false,
        };
        self.execute_observed_statement_action(statement, Box::new(action))
            .await
    }
}
