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
            CtasCommand, CtasDescriptionInput, CtasDescriptionResult, CtasFailure,
            CtasTargetBinding, CtasTargetPreflightOutcome, PreparedCtasDescription,
            PreparedCtasTargetPreflight, TrinoCtasInput,
        },
        kernels::DmlExecutionKernel,
        maintenance::io_admission::MetadataIoAdmission,
        planning::sql_cancellation_observation,
        required_constant_eval::RequiredConstantInterpreter,
        table_exists_source::TableExistsSource,
    },
};
use novarocks_parser::{
    Span,
    ast::{StatementSaveMode, TrinoCreateTableAsSelect},
    build,
};
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
        property_evaluation::{
            DdlPropertyNamePolicy,
            table::{prepare_trino_ctas, validate_trino_ctas},
        },
    },
};
use novarocks_spi::connector::{
    ConnectorError, ConnectorRequestContext, ConnectorStagedCreateSession,
    ConnectorStagedCtasProperties, ConnectorTableIdentity, ConnectorTableRequest,
    ConnectorTableResolution,
};
use novarocks_sql::compiler::{SqlCompileControl, SqlDescribedColumn};
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
            storage,
        })
    }
}

pub(crate) async fn describe(
    kernel: &DmlExecutionKernel,
    input: CtasDescriptionInput<'_>,
) -> CtasDescriptionResult {
    let cpu = input.cpu;
    match prepare_description_properties(kernel, input).await {
        Ok(Some(properties)) => {
            let CtasDescriptionProperties {
                input,
                table,
                context,
            } = properties;
            let staged = input.staged.clone();
            Ok(cpu
                .ctas_properties(input)
                .await?
                .map(|command| PreparedCtasDescription {
                    query: command.source,
                    target: Some(CtasDescriptionTarget {
                        staged,
                        table,
                        storage: command.storage,
                        context,
                    }),
                }))
        }
        Ok(None) => Ok(build::values(vec![vec![build::integer(1)]])
            .map_err(|error| internal_error(error.to_string()))
            .map(|body| PreparedCtasDescription {
                query: build::query(body),
                target: None,
            })),
        Err(error) => Ok(Err(error)),
    }
}

struct CtasDescriptionProperties {
    input: TrinoCtasPropertyInput,
    table: ConnectorTableIdentity,
    context: ConnectorRequestContext,
}

pub(crate) struct CtasDescriptionTarget {
    staged: ConnectorStagedCreateSession,
    table: ConnectorTableIdentity,
    storage: Arc<ConnectorStagedCtasProperties>,
    context: ConnectorRequestContext,
}

impl CtasDescriptionTarget {
    pub(crate) fn validate_source(
        &self,
        columns: &[SqlDescribedColumn],
    ) -> Result<(), QueryServiceError> {
        if columns.is_empty() {
            return Err(QueryServiceError::new(
                novarocks_query_application::session_error::QueryServiceErrorKind::InvalidValue,
                "CTAS source has no output columns",
            ));
        }
        let declarations = columns
            .iter()
            .map(|column| {
                let definition =
                    crate::query_execution::dml::iceberg_ctas::source_column_to_table_column_def(
                        &column.name,
                        &column.value_type,
                    )?;
                crate::catalog_application::statement::connector_column(&definition)
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(internal_error)?;
        self.staged
            .validate_ctas_definition(&self.table, &declarations, &self.storage, &self.context)
            .map_err(connector)
    }
}

async fn prepare_description_properties(
    kernel: &DmlExecutionKernel,
    input: CtasDescriptionInput<'_>,
) -> Result<Option<CtasDescriptionProperties>, QueryServiceError> {
    let session = input.request.session();
    let binding = kernel
        .retain_governed_ctas_target(
            &lower_object_name(&input.statement.name),
            session.current_catalog(),
            session.current_database(),
        )
        .map_err(|error| service(error.into_governed_dml_error(&input.source)))?;
    let target = binding
        .read_binding()
        .map_err(product_command_service_error)?;
    let pin = target.planning();
    let connector_context = crate::connector::context_for_planning_pin_typed(pin, input.connector)
        .map_err(connector)?;
    let exists = pin
        .binding()
        .metadata()
        .table_exists_async(ConnectorTableRequest {
            table: target.table().clone(),
            resolution: ConnectorTableResolution::StrictBaseTable,
            context: connector_context.clone(),
        })
        .await
        .map_err(connector)?;
    if let Some(reason) = input.request.execution().cancellation().reason() {
        return Err(cancellation_error(reason));
    }
    if !ctas_needs_source(
        exists,
        input.statement.save_mode,
        &input.source,
        input.statement.span,
        || binding.qualified_name(),
    )? {
        return Ok(None);
    }
    validate_trino_ctas(&input.source, &input.statement, &input.control)?;
    if let Some(span) = input.statement.properties.iter().find_map(|property| {
        input
            .parameters
            .first_in_expression(property.value.as_ref()?)
    }) {
        return Err(QueryServiceError::from_user_error(
            novarocks_sql::analyze_error::AnalyzeError::unsupported_query_shape(
                "DESCRIBE requires a bound CTAS table property value",
                span,
            )
            .to_user_error(Some(&input.source)),
        ));
    }
    let CtasTargetPreflightOutcome::Ready(preflight) = kernel
        .preflight_governed_ctas_binding(binding)
        .map_err(|error| service(error.into_governed_dml_error(&input.source)))?;
    let staged = preflight
        .staged_session()
        .map_err(|error| service(error.into_governed_dml_error(&input.source)))?;
    let window = input
        .command
        .result_window_alias()
        .ok_or_else(|| internal_error("CTAS description has no original command window"))?;
    let context = input.compiler.property_cpu_context(
        session.current_database().to_owned(),
        input.request.execution().sql_semantics().clone(),
        input.interpreter,
        input.control,
        input.command.scope().clone(),
        window,
    );
    Ok(Some(CtasDescriptionProperties {
        input: TrinoCtasPropertyInput {
            source: input.source,
            statement: input.statement,
            staged,
            command: input.command,
            context,
        },
        table: target.table().clone(),
        context: connector_context,
    }))
}

fn ctas_needs_source(
    exists: bool,
    save_mode: StatementSaveMode,
    source: &str,
    span: Span,
    qualified_name: impl FnOnce() -> String,
) -> Result<bool, QueryServiceError> {
    match (exists, save_mode) {
        (true, StatementSaveMode::Ignore) => Ok(false),
        (true, StatementSaveMode::Fail) => Err(QueryServiceError::from_user_error(
            DmlAdmissionError::TableAlreadyExists.to_user_error(
                source,
                span,
                format!("Destination table '{}' already exists", qualified_name()),
            ),
        )),
        _ => Ok(true),
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
    // The exact retained generation's write profile is a pure quote. An
    // unavailable quote still permits the read-only target disposition.
    let (read, io_profile_refusal) = match binding.governed_io_profile(&kernel) {
        Ok(profile) => (read.with_io_profile(profile), None),
        Err(refusal) => (read, Some(refusal)),
    };
    Ok(Box::new(Action {
        kernel,
        compiler: input.compiler,
        interpreter: input.interpreter,
        root,
        read: Some(read),
        io: None,
        io_profile_refusal,
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
    io: Option<Arc<MetadataIoAdmission>>,
    io_profile_refusal: Option<CtasFailure>,
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
        if !ctas_needs_source(
            exists,
            statement.save_mode,
            &execution.sql,
            statement.span,
            || {
                self.binding
                    .as_ref()
                    .expect("exact retained CTAS target")
                    .qualified_name()
            },
        )? {
            return selected_command_completion(Ok(()), Some(0)).map(Some);
        }
        let control = SqlCompileControl::new(
            self.deadline,
            sql_cancellation_observation(self.cancellation.clone()),
        );
        validate_trino_ctas(&execution.sql, &statement, &control)?;
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
            let mut read = self.read.take().expect("original CTAS target probe");
            if let Some(no_op) = self.admit_target(exists)? {
                return Poll::Ready(Ok(no_op));
            }
            if self.io_profile_refusal.is_none() {
                self.io = Some(read.take_completed_io().ok_or_else(|| {
                    internal_error("CTAS target probe lost its original I/O continuation")
                })?);
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
            let execution = self.execution.take().expect("original CTAS execution");
            if let Some(refusal) = self.io_profile_refusal.take() {
                return Poll::Ready(Err(service(
                    refusal.into_governed_dml_error(&execution.sql),
                )));
            }
            self.inner = Some(
                prepare_from_preflight(
                    self.kernel.clone(),
                    command,
                    execution,
                    self.preflight
                        .take()
                        .expect("original staged CTAS preflight"),
                    self.root.clone(),
                    self.io
                        .take()
                        .expect("original profiled CTAS I/O continuation"),
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

#[cfg(test)]
mod tests {
    use super::{StatementSaveMode, ctas_needs_source};
    use novarocks_parser::SYNTHETIC;

    #[test]
    fn prepared_statement_ctas_checks_original_target_before_source() {
        let source = "CREATE TABLE iceberg.db.t AS SELECT * FROM missing_target";
        let target = || "iceberg.db.t".to_owned();
        assert!(
            !ctas_needs_source(true, StatementSaveMode::Ignore, source, SYNTHETIC, target).unwrap()
        );
        assert!(
            ctas_needs_source(false, StatementSaveMode::Ignore, source, SYNTHETIC, target).unwrap()
        );
        let error = ctas_needs_source(true, StatementSaveMode::Fail, source, SYNTHETIC, target)
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Destination table 'iceberg.db.t' already exists")
        );
    }
}
