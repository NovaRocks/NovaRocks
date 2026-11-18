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

//! The original governed CTAS statement and its distinct publication frontiers.
use super::{CtasExecutionInput, PhaseSlot, execution_root, prepare_from_preflight, service};
use crate::{
    connector::mutation::command_error_from_connector,
    query::{
        QueryCancellationView, QueryServiceError, compiler::FrontendQueryCompiler, internal_error,
        product_command_service_error, property_cpu_context::PropertyCpuContext,
        write_action::selected_command_completion,
    },
    query_execution::{
        completion_facts::FrontendPlanningCpuSource,
        dml::ctas::{
            CtasCommand, CtasStorage, CtasTargetBinding, CtasTargetPreflightOutcome,
            PreparedCtasTargetPreflight, TrinoCtasInput,
        },
        kernels::DmlExecutionKernel,
        planning::sql_cancellation_observation,
        required_constant_eval::RequiredConstantInterpreter,
        table_exists_source::TableExistsSource,
    },
};
use novarocks_parser::ast::{StatementSaveMode, TrinoCreateTableAsSelect};
use novarocks_query_application::{
    api::{CommandContext, StatementExternalEffectDisposition, StatementNativeHealthFailure},
    session_control::{
        GovernedActionContext, GovernedStatementAction, GovernedStatementResidual,
        StatementActionValue,
    },
    session_outcome::cancellation_error,
    sql::{
        dml_admission::DmlAdmissionError,
        lower_object_name,
        property_evaluation::{DdlPropertyNamePolicy, table::prepare_trino_ctas},
    },
};
use novarocks_spi::connector::{ConnectorError, ConnectorStagedCreateSession};
use novarocks_sql::compiler::SqlCompileControl;
use novarocks_workload_control::WorkScope;
use std::{
    sync::Arc,
    task::{Context, Poll, ready},
    time::Instant,
};

pub(crate) struct TrinoCtasPropertyInput {
    source: Arc<str>,
    statement: TrinoCreateTableAsSelect,
    staged: ConnectorStagedCreateSession,
    command: CommandContext,
    context: PropertyCpuContext,
}

struct StagedPropertyNames<'a>(&'a ConnectorStagedCreateSession);
impl DdlPropertyNamePolicy for StagedPropertyNames<'_> {
    fn check_unknown_property(&mut self, name: &str) -> Result<(), QueryServiceError> {
        self.0.check_ctas_property_name(name).map_err(connector)
    }
}

impl TrinoCtasPropertyInput {
    pub(crate) fn run(self) -> Result<CtasCommand, QueryServiceError> {
        let Self {
            source,
            statement,
            staged,
            command,
            context,
        } = self;
        let window = command
            .result_window_alias()
            .ok_or_else(|| internal_error("CTAS property CPU has no original command window"))?;
        if !context.window.is_for_scope(&context.scope)
            || !context.window.is_for_scope(command.scope())
            || !context.window.shares_capacity_with(&window)
        {
            return Err(internal_error(
                "CTAS property CPU differs from its original Root/window",
            ));
        }
        let declarations = staged.ctas_property_declarations().map_err(connector)?;
        let table = prepare_trino_ctas(
            &source,
            &statement,
            &declarations,
            &mut StagedPropertyNames(&staged),
            &mut context.constant_port(),
            &context.control,
        )?
        .table;
        let storage = staged
            .prepare_ctas_properties(
                table
                    .properties
                    .into_iter()
                    .map(|(name, value)| (Arc::from(name), value))
                    .collect(),
                table.comment.map(Arc::from),
                command.connector_context(),
            )
            .map_err(connector)?;
        Ok(CtasCommand {
            target_parts: table.name.parts,
            if_not_exists: table.if_not_exists,
            source: statement.query.into_inner(),
            storage: CtasStorage::Evaluated(storage),
        })
    }
}

pub(crate) fn prepare(
    kernel: DmlExecutionKernel,
    input: TrinoCtasInput,
) -> Result<Box<dyn GovernedStatementAction>, QueryServiceError> {
    let root = execution_root(&input.execution.context).map_err(service)?;
    let capacity = input
        .execution
        .context
        .execution()
        .result_capacity()
        .expect("same CTAS capacity");
    let window = input
        .command
        .result_window_alias()
        .ok_or_else(|| internal_error("CTAS target has no original command window"))?;
    if !window.is_for_scope(&root) || !window.shares_capacity_with(&capacity.window_alias()) {
        return Err(internal_error(
            "CTAS target command uses a different Root/window",
        ));
    }
    let binding = kernel
        .retain_governed_ctas_target(
            &lower_object_name(&input.statement.name),
            input.execution.context.session().current_catalog(),
            input.execution.context.session().current_database(),
        )
        .map_err(|error| service(error.into_governed_dml_error(&input.execution.sql)))?;
    let read = TableExistsSource::try_new(
        binding
            .read_binding()
            .map_err(product_command_service_error)?,
        input.command.clone(),
        input.cancellation.clone(),
    )?;
    Ok(Box::new(Action {
        kernel,
        compiler: input.compiler,
        interpreter: input.interpreter,
        root,
        read: Some(read),
        binding: Some(binding),
        statement: Some(input.statement),
        execution: Some(input.execution),
        command: Some(input.command),
        deadline: input.deadline,
        cancellation: input.cancellation,
        preflight: None,
        cpu: None,
        properties: PhaseSlot::default(),
        health: None,
        inner: None,
    }))
}

type PropertyResult = Result<Result<CtasCommand, QueryServiceError>, StatementNativeHealthFailure>;

struct Action {
    kernel: DmlExecutionKernel,
    compiler: FrontendQueryCompiler,
    interpreter: Arc<RequiredConstantInterpreter>,
    root: WorkScope,
    read: Option<TableExistsSource>,
    binding: Option<CtasTargetBinding>,
    statement: Option<TrinoCreateTableAsSelect>,
    execution: Option<CtasExecutionInput>,
    command: Option<CommandContext>,
    deadline: Option<Instant>,
    cancellation: QueryCancellationView,
    preflight: Option<PreparedCtasTargetPreflight>,
    cpu: Option<Arc<FrontendPlanningCpuSource>>,
    properties: PhaseSlot<PropertyResult>,
    health: Option<StatementNativeHealthFailure>,
    inner: Option<Box<dyn GovernedStatementAction>>,
}

impl Action {
    fn admit_target(
        &mut self,
        exists: bool,
    ) -> Result<Option<StatementActionValue>, QueryServiceError> {
        if let Some(reason) = self.cancellation.reason() {
            return Err(cancellation_error(reason));
        }
        let statement = self.statement.take().expect("original CTAS statement");
        let execution = self
            .execution
            .as_ref()
            .expect("original CTAS execution context");
        if exists {
            match statement.save_mode {
                StatementSaveMode::Ignore => {
                    return selected_command_completion(Ok(()), Some(0)).map(Some);
                }
                StatementSaveMode::Fail => {
                    return Err(QueryServiceError::from_user_error(
                        DmlAdmissionError::TableAlreadyExists.to_user_error(
                            &execution.sql,
                            statement.span,
                            format!(
                                "Destination table '{}' already exists",
                                self.binding
                                    .as_ref()
                                    .expect("exact retained CTAS target")
                                    .qualified_name()
                            ),
                        ),
                    ));
                }
                // The pinned analyzer permits REPLACE to continue on an
                // existing target. Our existing CTAS profile refuses it in
                // the original typed admission below, before source analysis.
                StatementSaveMode::Replace => {}
            }
        }
        let control = SqlCompileControl::new(
            self.deadline,
            sql_cancellation_observation(self.cancellation.clone()),
        );
        novarocks_query_application::sql::property_evaluation::table::validate_trino_ctas(
            &execution.sql,
            &statement,
            &control,
        )?;
        let CtasTargetPreflightOutcome::Ready(preflight) = self
            .kernel
            .preflight_governed_ctas_binding(
                self.binding.take().expect("exact retained CTAS target"),
            )
            .map_err(|error| service(error.into_governed_dml_error(&execution.sql)))?;
        let staged = preflight
            .staged_session()
            .map_err(|error| service(error.into_governed_dml_error(&execution.sql)))?;
        let command = self.command.take().expect("original CTAS command");
        let window = command.result_window_alias().ok_or_else(|| {
            internal_error("CTAS property preparation has no original command window")
        })?;
        let context = self.compiler.property_cpu_context(
            execution.context.session().current_database().to_owned(),
            execution.context.execution().sql_semantics().clone(),
            self.interpreter.clone(),
            control,
            command.scope().clone(),
            window,
        );
        let input = TrinoCtasPropertyInput {
            source: execution.sql.clone(),
            statement,
            staged,
            command,
            context,
        };
        let cpu = self.compiler.new_planning_cpu_source();
        let receiving = cpu.clone();
        self.preflight = Some(preflight);
        self.cpu = Some(cpu);
        self.properties
            .start(async move { receiving.ctas_properties(input).await });
        Ok(None)
    }
}

impl GovernedStatementAction for Action {
    fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        self.health
            .clone()
            .or_else(|| self.properties.busy.then(super::health))
            .or_else(|| {
                self.read
                    .as_ref()
                    .and_then(TableExistsSource::local_health_failure)
            })
            .or_else(|| self.cpu.as_ref().and_then(|cpu| cpu.local_health_failure()))
            .or_else(|| {
                self.inner
                    .as_ref()
                    .and_then(|inner| inner.local_health_failure())
            })
    }

    fn external_effect_disposition(&self) -> Option<StatementExternalEffectDisposition> {
        self.inner
            .as_ref()
            .and_then(|inner| inner.external_effect_disposition())
    }

    fn handoff_residual_in(
        &mut self,
        target: &mut Option<Box<dyn GovernedStatementResidual>>,
    ) -> Result<(), StatementNativeHealthFailure> {
        if let Some(inner) = self.inner.as_mut() {
            inner.handoff_residual_in(target)?;
        }
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
        if let Some(read) = self.read.as_mut() {
            let exists = ready!(read.poll_result(owner, cx))?;
            drop(self.read.take());
            if let Some(no_op) = self.admit_target(exists)? {
                return Poll::Ready(Ok(no_op));
            }
        }
        if let Some(cpu) = self.cpu.as_ref() {
            ready!(self.properties.poll_settled(cx));
            if let Err(health) = self
                .properties
                .result
                .as_ref()
                .expect("settled CTAS properties")
            {
                self.health = Some(health.clone());
                return Poll::Pending;
            }
            if let Err(health) = ready!(cpu.poll_closed(cx)) {
                self.health = Some(health);
                return Poll::Pending;
            }
            drop(self.cpu.take());
            let command = self
                .properties
                .take()
                .expect("original CTAS properties had no local fault")?;
            if let Some(reason) = self.cancellation.reason() {
                return Poll::Ready(Err(cancellation_error(reason)));
            }
            self.inner = Some(
                prepare_from_preflight(
                    self.kernel.clone(),
                    command,
                    self.execution.take().expect("original CTAS execution"),
                    self.preflight
                        .take()
                        .expect("original staged CTAS preflight"),
                    self.root.clone(),
                )
                .map_err(service)?,
            );
        }
        self.inner
            .as_mut()
            .expect("original CTAS action")
            .poll_action(owner, cx)
    }
}

fn connector(error: ConnectorError) -> QueryServiceError {
    product_command_service_error(command_error_from_connector(error))
}
