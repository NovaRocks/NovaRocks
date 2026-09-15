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

//! Frontend-owned SQL session admission and routing boundary.

use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use crate::catalog_application::command::CatalogCommandExecutor;
use crate::catalog_application::iceberg_ref_command::IcebergRefCommandExecutor;
use crate::dml::DmlService;
use crate::mv::command::MvCommandExecutor;
use crate::query::compiler::{FrontendQueryCompiler, FrontendQueryCompilerError};
use crate::query_execution::completion::PreparedQueryOperation;
use crate::query_execution::dml::add_files::AddFilesEngine;
use crate::query_execution::dml::ctas::CtasEngine;
use crate::query_execution::dml::delete::DeleteEngine;
use crate::query_execution::dml::insert::InsertEngine;
use crate::query_execution::dml::mutation::MutationEngine;
use crate::query_execution::dml::truncate::TruncateEngine;
use crate::query_execution::logical_read::LogicalReadLauncher;
use crate::query_execution::maintenance::command::{
    MaintenanceCommandExecutor, MaintenanceReadCommandExecutor,
};
use crate::query_execution::service::QueryExecutionService;
use crate::statistics::command::StatisticsCommandExecutor;
use crate::view::command::ViewCommandExecutor;
use async_trait::async_trait;
use novarocks_parser::{
    ast::{self, Statement as ParsedStatement},
    printer::{print_expr, print_statement},
};
use novarocks_proto_codec::lifecycle::QueryOptions;
use novarocks_proto_models::novarocks;
use novarocks_query_application::admitted_query_context::{RequestAdmission, RequestContext};
use novarocks_query_application::api::{
    BackendCommandExecutor, CatalogCommandConsumer, CommandContext, CommandError, CommandErrorKind,
    CommandFuture, ExecutionOutput, MaintenanceCommandConsumer, MaterializedViewCommand,
    MaterializedViewCommandConsumer, OptionalCommandFuture, QueryExecutionError,
    QueryExecutionErrorKind, ResultDelivery, StatisticsCommandConsumer,
};
use novarocks_query_application::api::{BackendTopologyService, BackendTopologySnapshot};
use novarocks_query_application::api::{
    QueryResult, ResultField as QueryResultColumn, build_string_query_result,
};
use novarocks_query_application::cancellation::{QueryCancellationReason, QueryCancellationView};
use novarocks_query_application::client_connection::ClientConnectionControlPort;
use novarocks_query_application::cpu::{QueryBlockingExecutor, QueryCpuExecutor, QueryCpuRunError};
use novarocks_query_application::protocol_delivery::{
    GovernedCompletionStatementResult, GovernedErrorStatementResult,
    GovernedImmediateStatementResult, QuerySessionOutput as StatementResult,
};
use novarocks_query_application::publication::LakePublicationRuntimePolicy;
use novarocks_query_application::serving_admission::{
    FrontendAdmissionError, FrontendServingAdmission,
};
use novarocks_query_application::session::{
    QuerySession, QuerySessionFactory, QuerySessionOpenRequest,
};
use novarocks_query_application::session_control::{
    GovernedQueryStatementBeginError, QueryControlService, QuerySessionLease, SessionIdentity,
    SessionToken, StatementToken,
};
use novarocks_query_application::session_error::{QueryServiceError, QueryServiceErrorKind};
use novarocks_query_application::session_outcome::{
    cancellation_error, cancellation_requires_statement_fence, governed_cancellation_error,
    governed_execution_error, governed_query_deadline, governed_query_execution_error,
    governed_statement_begin_error, scalar_query_error,
};
use novarocks_query_application::sql::admission::{
    admin_raise_engine_error, requires_lake_publication_deadline, typed_statement_work_class,
    unnegotiated_query_statement,
};
use novarocks_query_application::sql::catalog::SessionCatalogService;
use novarocks_query_application::sql::dml_admission::validate_table_statement_admission;
use novarocks_query_application::sql::kill::execute_kill_statement;
use novarocks_query_application::sql::session::{
    SessionExecutionSettings, SessionSetAssignmentOutcome, SessionSqlState,
    admit_session_set_assignment as admit_query_application_session_set_assignment,
    apply_session_set_assignment as apply_query_application_session_set_assignment,
};
use novarocks_query_application::sql::session_admit::SessionAdmitError;
use novarocks_query_application::sql::user_variable::query_result_to_user_variable_literal;
use novarocks_query_application::sql::{
    ProductCommandRouter, ProductSqlCommand, lower_product_sql_command,
};
use novarocks_query_application::sql::{
    SpecializedStatementRoute, parse_single_statement, query_service_parse_error,
    strip_leading_line_comments,
};
use novarocks_types::ClusterRole;
use novarocks_types::naming::normalize_identifier;
use novarocks_user_error::UserError;
use novarocks_workload_control::WorkError;
use novarocks_workload_control::WorkOwner;
use novarocks_workload_control::{
    LocalResourceAuthority, RootAdmissionHandle, WorkClass, WorkRequest,
};

pub(crate) mod compiler;

const DEFAULT_CATALOG: &str = "default_catalog";

/// Preparation failures retain whether cancellation, rather than a compiler
/// failure, stopped work before the logical-execution handoff.
enum GovernedPreparationError {
    Service(QueryServiceError),
    Cancelled(QueryServiceError),
}

fn command_error(kind: CommandErrorKind, error: impl Into<String>) -> CommandError {
    CommandError::new(kind, error.into())
}

fn execute_product_command_edge<F>(
    executor: QueryBlockingExecutor,
    request_context: RequestContext,
    command_context: CommandContext,
    call: F,
) -> CommandFuture
where
    F: FnOnce(&RequestContext, &CommandContext) -> Result<StatementResult, String> + Send + 'static,
{
    Box::pin(async move {
        if request_context.execution().cancellation().is_cancelled() {
            return Err(command_error(
                CommandErrorKind::Cancelled,
                "product command was cancelled before synchronous admission",
            ));
        }
        command_context.scope().check().map_err(|error| {
            command_error(
                CommandErrorKind::Cancelled,
                format!("product command scope is no longer active: {error}"),
            )
        })?;
        let cancellation = request_context.execution().cancellation().clone();
        let scope = command_context.scope().clone();
        let statement_token = command_context.statement_token();
        executor
            .execute(move || {
                let _diagnostic_scope =
                    crate::preparation_diagnostics::enter_statement(statement_token);
                if cancellation.is_cancelled() {
                    return Err(command_error(
                        CommandErrorKind::Cancelled,
                        "product command was cancelled before synchronous execution began",
                    ));
                }
                scope.check().map_err(|error| {
                    command_error(
                        CommandErrorKind::Cancelled,
                        format!("product command scope is no longer active: {error}"),
                    )
                })?;
                call(&request_context, &command_context)
                    .map_err(|error| command_error(CommandErrorKind::Failed, error))
            })
            .await
            .map_err(|error| command_error(CommandErrorKind::Failed, error))?
    })
}

fn execute_optional_product_command_edge<F>(
    executor: QueryBlockingExecutor,
    request_context: RequestContext,
    command_context: CommandContext,
    call: F,
) -> OptionalCommandFuture
where
    F: FnOnce(&RequestContext, &CommandContext) -> Result<Option<StatementResult>, String>
        + Send
        + 'static,
{
    Box::pin(async move {
        if request_context.execution().cancellation().is_cancelled() {
            return Err(command_error(
                CommandErrorKind::Cancelled,
                "specialized command was cancelled before synchronous admission",
            ));
        }
        command_context.scope().check().map_err(|error| {
            command_error(
                CommandErrorKind::Cancelled,
                format!("specialized command scope is no longer active: {error}"),
            )
        })?;
        let cancellation = request_context.execution().cancellation().clone();
        let scope = command_context.scope().clone();
        let statement_token = command_context.statement_token();
        executor
            .execute(move || {
                let _diagnostic_scope =
                    crate::preparation_diagnostics::enter_statement(statement_token);
                if cancellation.is_cancelled() {
                    return Err(command_error(
                        CommandErrorKind::Cancelled,
                        "specialized command was cancelled before synchronous execution began",
                    ));
                }
                scope.check().map_err(|error| {
                    command_error(
                        CommandErrorKind::Cancelled,
                        format!("specialized command scope is no longer active: {error}"),
                    )
                })?;
                call(&request_context, &command_context)
                    .map_err(|error| command_error(CommandErrorKind::Failed, error))
            })
            .await
            .map_err(|error| command_error(CommandErrorKind::Failed, error))?
    })
}

#[derive(Clone)]
struct FrontendCatalogCommandConsumer {
    catalog: CatalogCommandExecutor,
    view: ViewCommandExecutor,
    iceberg_ref: IcebergRefCommandExecutor,
    executor: QueryBlockingExecutor,
}

impl CatalogCommandConsumer for FrontendCatalogCommandConsumer {
    fn execute(
        &self,
        command: novarocks_sql::semantic::CatalogSqlCommand,
        request_context: RequestContext,
        command_context: CommandContext,
    ) -> CommandFuture {
        let catalog = self.catalog.clone();
        let view = self.view.clone();
        let iceberg_ref = self.iceberg_ref.clone();
        execute_product_command_edge(
            self.executor.clone(),
            request_context,
            command_context,
            move |context, command_context| match command {
                novarocks_sql::semantic::CatalogSqlCommand::DropDatabase { name, .. }
                    if context.session().current_catalog().is_none() && name.parts.len() == 1 =>
                {
                    view.drop_database(DEFAULT_CATALOG, &name.parts[0])?;
                    Ok(StatementResult::Ok)
                }
                novarocks_sql::semantic::CatalogSqlCommand::CreateTable(command) => catalog
                    .execute_table_command(
                        &command,
                        context.session().current_catalog(),
                        context.session().current_database(),
                        command_context.connector_context(),
                    ),
                novarocks_sql::semantic::CatalogSqlCommand::AlterIcebergTable(command)
                    if matches!(
                        &command.action,
                        novarocks_sql::semantic::command::IcebergTableSqlAction::Reference(_)
                    ) =>
                {
                    iceberg_ref.execute_command(
                        &command,
                        context.session().current_database(),
                        command_context.connector_context(),
                    )
                }
                novarocks_sql::semantic::CatalogSqlCommand::AlterIcebergTable(command) => catalog
                    .execute_iceberg_command(
                        &command,
                        context.session().current_catalog(),
                        context.session().current_database(),
                        command_context.connector_context(),
                    ),
                command => catalog.execute_command(
                    &command,
                    context.session().current_catalog(),
                    context.session().current_database(),
                    command_context.connector_context(),
                ),
            },
        )
    }
}

#[derive(Clone)]
struct FrontendStatisticsCommandConsumer {
    statistics: StatisticsCommandExecutor,
    executor: QueryBlockingExecutor,
}

impl StatisticsCommandConsumer for FrontendStatisticsCommandConsumer {
    fn execute(
        &self,
        command: novarocks_sql::semantic::StatisticsSqlCommand,
        request_context: RequestContext,
        command_context: CommandContext,
    ) -> CommandFuture {
        let statistics = self.statistics.clone();
        execute_product_command_edge(
            self.executor.clone(),
            request_context,
            command_context,
            move |context, _command_context| {
                statistics.execute_command(
                    &command,
                    context.session().current_catalog(),
                    context.session().current_database(),
                    Some(context.execution()),
                )
            },
        )
    }
}

#[derive(Clone)]
struct FrontendMaintenanceCommandConsumer {
    maintenance: MaintenanceCommandExecutor,
    maintenance_read: MaintenanceReadCommandExecutor,
    executor: QueryBlockingExecutor,
}

impl MaintenanceCommandConsumer for FrontendMaintenanceCommandConsumer {
    fn execute(
        &self,
        command: novarocks_sql::semantic::MaintenanceSqlCommand,
        request_context: RequestContext,
        command_context: CommandContext,
    ) -> CommandFuture {
        let maintenance = self.maintenance.clone();
        let maintenance_read = self.maintenance_read.clone();
        execute_product_command_edge(
            self.executor.clone(),
            request_context,
            command_context,
            move |context, command_context| match command {
                novarocks_sql::semantic::MaintenanceSqlCommand::ShowOptimize(command) => {
                    maintenance_read.execute_command(
                        &command,
                        context.session().current_catalog(),
                        context.session().current_database(),
                    )
                }
                command => maintenance.execute_command(
                    &command,
                    context.session().current_catalog(),
                    context.session().current_database(),
                    context.execution(),
                    command_context.connector_context(),
                ),
            },
        )
    }
}

/// Builds the role-local product adapters at Frontend role composition.
///
/// Query Application owns the closed command router and selects a consumer
/// after semantic lowering. This factory stays at the outer role boundary so
/// the query session receives an already-composed router rather than choosing
/// or constructing product adapters itself.
pub(crate) fn product_command_router(
    catalog: CatalogCommandExecutor,
    view: ViewCommandExecutor,
    iceberg_ref: IcebergRefCommandExecutor,
    statistics: StatisticsCommandExecutor,
    maintenance: MaintenanceCommandExecutor,
    maintenance_read: MaintenanceReadCommandExecutor,
    executor: QueryBlockingExecutor,
) -> ProductCommandRouter {
    ProductCommandRouter::new(
        Arc::new(FrontendCatalogCommandConsumer {
            catalog,
            view,
            iceberg_ref,
            executor: executor.clone(),
        }),
        Arc::new(FrontendStatisticsCommandConsumer {
            statistics,
            executor: executor.clone(),
        }),
        Arc::new(FrontendMaintenanceCommandConsumer {
            maintenance,
            maintenance_read,
            executor,
        }),
    )
}

#[derive(Clone)]
struct TypedCommandRoute {
    backend: BackendCommandExecutor,
    view: ViewCommandExecutor,
    mv: Arc<dyn MaterializedViewCommandConsumer>,
    mv_call: MvCommandExecutor,
    executor: QueryBlockingExecutor,
}

impl TypedCommandRoute {
    fn new(
        backend: BackendCommandExecutor,
        view: ViewCommandExecutor,
        mv: Arc<dyn MaterializedViewCommandConsumer>,
        mv_call: MvCommandExecutor,
        executor: QueryBlockingExecutor,
    ) -> Self {
        Self {
            backend,
            view,
            mv,
            mv_call,
            executor,
        }
    }
}

impl SpecializedStatementRoute for TypedCommandRoute {
    fn execute_show_backends(
        &self,
        context: &RequestContext,
        command_context: &CommandContext,
    ) -> CommandFuture {
        let backend = self.backend.clone();
        execute_product_command_edge(
            self.executor.clone(),
            context.clone(),
            command_context.clone(),
            move |context, _command_context| {
                backend
                    .show_backends(context.execution().role())
                    .map(StatementResult::Query)
            },
        )
    }

    fn execute_maintenance_call(
        &self,
        statement: &novarocks_parser::ast::CallStatement,
        context: &RequestContext,
        command_context: &CommandContext,
    ) -> OptionalCommandFuture {
        let mv_call = self.mv_call.clone();
        let statement = statement.clone();
        execute_optional_product_command_edge(
            self.executor.clone(),
            context.clone(),
            command_context.clone(),
            move |context, command_context| {
                mv_call.try_execute_typed_call(
                    &statement,
                    context.session().current_database(),
                    command_context.connector_context(),
                )
            },
        )
    }

    fn execute_materialized_view(
        &self,
        statement: &novarocks_parser::ast::MaterializedViewStatement,
        context: &RequestContext,
        command_context: &CommandContext,
    ) -> CommandFuture {
        let mv = Arc::clone(&self.mv);
        let statement = statement.clone();
        execute_product_command_edge(
            self.executor.clone(),
            context.clone(),
            command_context.clone(),
            move |context, command_context| {
                let command = MaterializedViewCommand::new(statement);
                mv.execute(&command, context, command_context)
            },
        )
    }

    fn execute_view(
        &self,
        statement: &novarocks_parser::ast::ViewStatement,
        context: &RequestContext,
        command_context: &CommandContext,
    ) -> CommandFuture {
        let view = self.view.clone();
        let statement = statement.clone();
        execute_product_command_edge(
            self.executor.clone(),
            context.clone(),
            command_context.clone(),
            move |context, command_context| {
                view.execute(
                    &statement,
                    context.session().current_catalog(),
                    context.session().current_database(),
                    command_context.connector_context(),
                )
            },
        )
    }
}

enum RoutedExecutionError {
    Engine(String),
    User(UserError),
    Publication {
        message: String,
        terminal: novarocks_spi::connector::LakePublicationTerminal,
    },
}

fn dml_statement_result(
    result: Result<(), crate::dml::DmlError>,
) -> Result<StatementResult, RoutedExecutionError> {
    dml_result(result).map(|()| StatementResult::Ok)
}

fn dml_result<T>(result: Result<T, crate::dml::DmlError>) -> Result<T, RoutedExecutionError> {
    result.map_err(|error| {
        if let Some(user_error) = error.user_error().cloned() {
            RoutedExecutionError::User(user_error)
        } else if let Some(engine_error_code) = error.engine_error_code() {
            RoutedExecutionError::Engine(format!("[{}] {error}", engine_error_code.as_str()))
        } else if let Some(terminal) = error.publication_terminal().cloned() {
            RoutedExecutionError::Publication {
                message: error.to_string(),
                terminal,
            }
        } else {
            RoutedExecutionError::Engine(error.to_string())
        }
    })
}

#[expect(
    clippy::too_many_arguments,
    reason = "The typed DML boundary keeps one explicit engine per statement family."
)]
fn execute_typed_dml_statement(
    dml: &DmlService,
    insert_engine: &dyn InsertEngine,
    delete_engine: &dyn DeleteEngine,
    mutation_engine: &dyn MutationEngine,
    ctas_engine: &dyn CtasEngine,
    statement: &novarocks_parser::ast::DmlStatement,
    source: &str,
    context: &RequestContext,
    query_options: &QueryOptions,
) -> Result<StatementResult, RoutedExecutionError> {
    use novarocks_parser::ast::DmlStatement;

    match statement {
        DmlStatement::Insert(statement) => dml_statement_result(dml.try_execute_insert(
            insert_engine,
            statement,
            source,
            context,
            Some(query_options),
        )),
        DmlStatement::Delete(statement) => dml_statement_result(
            dml.prepare_delete(
                delete_engine,
                crate::query_execution::dml::delete::DeleteStatement::Predicate(statement),
                source,
                context,
                Some(query_options),
            )
            .and_then(|prepared| dml.execute_prepared_delete(delete_engine, prepared)),
        ),
        DmlStatement::AddEqualityDelete(statement) => dml_statement_result(
            dml.prepare_delete(
                delete_engine,
                crate::query_execution::dml::delete::DeleteStatement::Equality(statement),
                source,
                context,
                Some(query_options),
            )
            .and_then(|prepared| dml.execute_prepared_delete(delete_engine, prepared)),
        ),
        DmlStatement::Update(_) | DmlStatement::Merge(_) => dml_statement_result(
            dml.prepare_typed_mutation(
                mutation_engine,
                statement,
                source,
                context,
                Some(query_options),
            )
            .and_then(|prepared| dml.execute_prepared_mutation(mutation_engine, prepared)),
        ),
        DmlStatement::CreateTableAsSelect(statement) => dml_statement_result(dml.try_execute_ctas(
            ctas_engine,
            statement,
            source,
            context,
            Some(query_options),
        )),
    }
}

/// Executes one declared synchronous command edge after its bounded admission.
///
/// Compilation and protocol delivery must not be captured here. Keeping this
/// edge to one adapter-owned command call makes its queueing, cancellation, and
/// statement-owner handoff explicit.
async fn execute_synchronous_stage<T, F>(
    executor: QueryBlockingExecutor,
    cancellation: QueryCancellationView,
    diagnostic_statement: StatementToken,
    execution_owner: WorkOwner,
    call: F,
) -> Result<(Result<T, RoutedExecutionError>, WorkOwner), String>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, RoutedExecutionError> + Send + 'static,
{
    executor
        .execute(move || {
            let _diagnostic_scope =
                crate::preparation_diagnostics::enter_statement(diagnostic_statement);
            let result = if cancellation.is_cancelled() {
                Err(RoutedExecutionError::Engine(
                    "typed statement was cancelled before synchronous command execution began"
                        .to_owned(),
                ))
            } else {
                call()
            };
            (result, execution_owner)
        })
        .await
}

async fn execute_synchronous_statement<F>(
    executor: QueryBlockingExecutor,
    cancellation: QueryCancellationView,
    diagnostic_statement: StatementToken,
    execution_owner: WorkOwner,
    call: F,
) -> Result<(Result<StatementResult, RoutedExecutionError>, WorkOwner), String>
where
    F: FnOnce() -> Result<StatementResult, RoutedExecutionError> + Send + 'static,
{
    execute_synchronous_stage(
        executor,
        cancellation,
        diagnostic_statement,
        execution_owner,
        call,
    )
    .await
}

/// Runs a DML plan and its external-effect dispatch as distinct governed
/// blocking edges. A successful plan is still pre-dispatch state, so the
/// second edge independently checks cancellation before it can cross the
/// publication boundary.
async fn execute_prepared_dml_statement<P, Prepare, Execute>(
    executor: QueryBlockingExecutor,
    cancellation: QueryCancellationView,
    diagnostic_statement: StatementToken,
    execution_owner: WorkOwner,
    prepare: Prepare,
    execute: Execute,
) -> Result<(Result<StatementResult, RoutedExecutionError>, WorkOwner), String>
where
    P: Send + 'static,
    Prepare: FnOnce() -> Result<P, RoutedExecutionError> + Send + 'static,
    Execute: FnOnce(P) -> Result<StatementResult, RoutedExecutionError> + Send + 'static,
{
    let (prepared, execution_owner) = execute_synchronous_stage(
        executor.clone(),
        cancellation.clone(),
        diagnostic_statement,
        execution_owner,
        prepare,
    )
    .await?;
    match prepared {
        Ok(prepared) => {
            execute_synchronous_stage(
                executor,
                cancellation,
                diagnostic_statement,
                execution_owner,
                move || execute(prepared),
            )
            .await
        }
        Err(error) => Ok((Err(error), execution_owner)),
    }
}

async fn execute_product_statement(
    router: ProductCommandRouter,
    command: ProductSqlCommand,
    request_context: RequestContext,
    command_context: CommandContext,
    execution_owner: WorkOwner,
) -> Result<(Result<StatementResult, RoutedExecutionError>, WorkOwner), String> {
    let result = router
        .execute(command, request_context, command_context)
        .await
        .map_err(|error| RoutedExecutionError::Engine(error.to_string()));
    Ok((result, execution_owner))
}

fn add_files_status(file_count: u32) -> Result<QueryResult, String> {
    build_string_query_result("status", vec![format!("Added {file_count} file(s)")])
}

/// Design: ADR-0012 (docs/adr/ADR-0012-frontend-query-session-router.md)
#[derive(Clone)]
pub struct FrontendQueryService {
    session_catalog_resolver: SessionCatalogService,
    query_compiler: FrontendQueryCompiler,
    command_executor: Arc<dyn SpecializedStatementRoute>,
    product_command_router: ProductCommandRouter,
    query_control: QueryControlService,
    client_connection_control: Arc<dyn ClientConnectionControlPort>,
    query_execution: QueryExecutionService,
    logical_read_launcher: Arc<dyn LogicalReadLauncher>,
    workload_root_admission: RootAdmissionHandle,
    workload_resources: LocalResourceAuthority,
    role: ClusterRole,
    topology: BackendTopologyService,
    dml: Arc<DmlService>,
    insert_engine: Arc<dyn InsertEngine>,
    delete_engine: Arc<dyn DeleteEngine>,
    mutation_engine: Arc<dyn MutationEngine>,
    add_files_engine: Arc<dyn AddFilesEngine>,
    ctas_engine: Arc<dyn CtasEngine>,
    truncate_engine: Arc<dyn TruncateEngine>,
    query_cpu_executor: QueryCpuExecutor,
    query_blocking_executor: QueryBlockingExecutor,
    /// Cost budget frozen from `[runtime]` and handed to statement admission
    /// whenever the session did not set one itself.
    optimizer_query_mem_limit_bytes: u64,
    lake_publication_runtime_policy: LakePublicationRuntimePolicy,
    serving_admission: FrontendServingAdmission,
}

impl FrontendQueryService {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        session_catalog_resolver: SessionCatalogService,
        query_compiler: FrontendQueryCompiler,
        product_command_router: ProductCommandRouter,
        backend_command_executor: BackendCommandExecutor,
        view_command_executor: ViewCommandExecutor,
        mv_command_consumer: Arc<dyn MaterializedViewCommandConsumer>,
        mv_command_executor: MvCommandExecutor,
        query_control: QueryControlService,
        client_connection_control: Arc<dyn ClientConnectionControlPort>,
        query_execution: QueryExecutionService,
        logical_read_launcher: Arc<dyn LogicalReadLauncher>,
        workload_root_admission: RootAdmissionHandle,
        workload_resources: LocalResourceAuthority,
        role: ClusterRole,
        topology: BackendTopologyService,
        dml: Arc<DmlService>,
        insert_engine: Arc<dyn InsertEngine>,
        delete_engine: Arc<dyn DeleteEngine>,
        mutation_engine: Arc<dyn MutationEngine>,
        add_files_engine: Arc<dyn AddFilesEngine>,
        ctas_engine: Arc<dyn CtasEngine>,
        truncate_engine: Arc<dyn TruncateEngine>,
        query_cpu_executor: QueryCpuExecutor,
        query_blocking_executor: QueryBlockingExecutor,
        optimizer_query_mem_limit_bytes: u64,
        lake_publication_runtime_policy: LakePublicationRuntimePolicy,
        serving_admission: FrontendServingAdmission,
    ) -> Self {
        Self {
            session_catalog_resolver,
            query_compiler,
            command_executor: Arc::new(TypedCommandRoute::new(
                backend_command_executor,
                view_command_executor,
                mv_command_consumer,
                mv_command_executor,
                query_blocking_executor.clone(),
            )),
            product_command_router,
            query_control,
            client_connection_control,
            query_execution,
            logical_read_launcher,
            workload_root_admission,
            workload_resources,
            role,
            topology,
            dml,
            insert_engine,
            delete_engine,
            mutation_engine,
            add_files_engine,
            ctas_engine,
            truncate_engine,
            query_cpu_executor,
            query_blocking_executor,
            optimizer_query_mem_limit_bytes,
            lake_publication_runtime_policy,
            serving_admission,
        }
    }
}

impl QuerySessionFactory for FrontendQueryService {
    fn open_session(
        &self,
        request: QuerySessionOpenRequest,
    ) -> Result<Arc<dyn QuerySession>, QueryServiceError> {
        let query_control = self.query_control.clone();
        let identity =
            SessionIdentity::new(request.connection_token(), request.principal().to_string());
        let lease = self
            .serving_admission
            .register_session(|| query_control.register_session(identity))
            .map_err(query_service_admission_error)?
            .map_err(|error| {
                QueryServiceError::new(
                    QueryServiceErrorKind::Internal,
                    format!("register frontend query session failed: {error:?}"),
                )
            })?;
        Ok(Arc::new(FrontendQuerySession {
            service: self.clone(),
            lease: Mutex::new(Some(lease)),
            state: Mutex::new(SessionSqlState::default()),
        }))
    }

    fn cancel_all(&self, reason: QueryCancellationReason) {
        self.query_control.cancel_all(reason);
    }
}

fn query_service_admission_error(error: FrontendAdmissionError) -> QueryServiceError {
    match error {
        FrontendAdmissionError::Draining => QueryServiceError::frontend_draining(),
        FrontendAdmissionError::NotReady { .. } | FrontendAdmissionError::Stopping => {
            QueryServiceError::new(
                QueryServiceErrorKind::Unavailable,
                "frontend is not ready for workload admission",
            )
        }
    }
}

struct FrontendQuerySession {
    service: FrontendQueryService,
    lease: Mutex<Option<QuerySessionLease>>,
    state: Mutex<SessionSqlState>,
}

impl FrontendQuerySession {
    fn governed_statement_begin_error(
        &self,
        error: GovernedQueryStatementBeginError,
    ) -> QueryServiceError {
        if matches!(
            error,
            GovernedQueryStatementBeginError::Admission(WorkError::Closed)
        ) {
            if let Some(admission) = self.service.serving_admission.admission_error() {
                return query_service_admission_error(admission);
            }
        }
        governed_statement_begin_error(error)
    }

    fn token(&self) -> Result<SessionToken, QueryServiceError> {
        self.lease
            .lock()
            .map_err(|_| {
                QueryServiceError::new(
                    QueryServiceErrorKind::Internal,
                    "session lease lock poisoned",
                )
            })?
            .as_ref()
            .map(QuerySessionLease::token)
            .ok_or_else(|| {
                QueryServiceError::new(
                    QueryServiceErrorKind::NoSuchSession,
                    "query session is closed",
                )
            })
    }

    async fn execute_one_statement(
        &self,
        statement: &str,
    ) -> Result<novarocks_query_application::session::QuerySessionStatement, QueryServiceError>
    {
        let trimmed = strip_leading_line_comments(statement.trim().trim_end_matches(';').trim());
        if trimmed.is_empty() {
            return Ok(
                novarocks_query_application::session::QuerySessionStatement::output_owned(
                    StatementResult::Ok,
                ),
            );
        }
        if let Some(error) = admin_raise_engine_error(trimmed)? {
            return Err(error);
        }
        let parsed_statement = parse_single_statement(trimmed)
            .map_err(|error| query_service_parse_error(error, trimmed))?;
        let result = match parsed_statement {
            ParsedStatement::Session(ast::SessionStatement::Set(statement))
                if statement
                    .assignments
                    .iter()
                    .any(|assignment| matches!(assignment.value, ast::SetValue::Query(_))) =>
            {
                self.execute_governed_set(trimmed.to_string(), &statement)
                    .await
            }
            ParsedStatement::Session(statement) => {
                self.execute_session_statement(trimmed, &statement).await
            }
            ParsedStatement::Query(_) => {
                return self
                    .execute_governed_read(trimmed.to_string(), parsed_statement)
                    .await
                    .map(
                        novarocks_query_application::session::QuerySessionStatement::output_owned,
                    );
            }
            statement => {
                self.execute_typed_statement(trimmed.to_string(), statement)
                    .await
            }
        };
        result.map(novarocks_query_application::session::QuerySessionStatement::output_owned)
    }

    async fn execute_session_statement(
        &self,
        source: &str,
        statement: &ast::SessionStatement,
    ) -> Result<StatementResult, QueryServiceError> {
        let token = self.token()?;
        let mut governed = self
            .service
            .query_control
            .begin_governed_statement(
                token,
                &self.service.workload_root_admission,
                WorkClass::Management,
                None,
                None,
            )
            .map_err(|error| self.governed_statement_begin_error(error))?;
        let result = match statement {
            ast::SessionStatement::Set(statement) => {
                for assignment in &statement.assignments {
                    if let Err(error) = self.admit_session_set_assignment(source, assignment) {
                        return Ok(self.governed_typed_error(error, governed));
                    }
                }
                for assignment in &statement.assignments {
                    if let Err(error) = self.apply_session_set_assignment(source, assignment).await {
                        return Ok(self.governed_typed_error(error, governed));
                    }
                }
                Ok(StatementResult::Ok)
            }
            ast::SessionStatement::Use(statement) => {
                let schema = statement.catalog.as_ref().map_or_else(
                    || statement.database.value.clone(),
                    |catalog| format!("{}.{}", catalog.value, statement.database.value),
                );
                let cancellation = QueryCancellationView::governed(
                    governed.cancellation().clone(),
                    governed.timeout_ms(),
                );
                if let Err(error) = self
                    .init_database_with_cancellation(&schema, cancellation)
                    .await
                {
                    return Ok(self.governed_typed_error(error, governed));
                }
                Ok(StatementResult::Ok)
            }
            ast::SessionStatement::Kill(statement) => self.execute_session_kill(source, statement),
            ast::SessionStatement::TransactionControl(statement) => Err(
                QueryServiceError::from_user_error(
                    SessionAdmitError::TransactionUnsupported.to_user_error(
                        source,
                        statement.span,
                        format!(
                            "{} is not supported because NovaRocks only provides statement-level autocommit frontiers",
                            statement.kind.sql()
                        ),
                    ),
                ),
            ),
        };
        match result {
            Ok(StatementResult::Ok) => {
                governed.complete_execution();
                Ok(StatementResult::GovernedCompletion(
                    GovernedCompletionStatementResult::new(
                        self.service.workload_resources.clone(),
                        governed,
                    ),
                ))
            }
            Ok(StatementResult::Query(result)) => {
                governed.complete_execution();
                Ok(StatementResult::GovernedQuery(
                    GovernedImmediateStatementResult::new(
                        result,
                        self.service.workload_resources.clone(),
                        governed,
                    ),
                ))
            }
            Ok(
                StatementResult::GovernedQuery(_)
                | StatementResult::StreamingQuery(_)
                | StatementResult::GovernedCompletion(_)
                | StatementResult::GovernedError(_),
            ) => Ok(self.governed_typed_error(
                internal_error("session statement returned an already-owned protocol result"),
                governed,
            )),
            Err(error) => Ok(self.governed_typed_error(error, governed)),
        }
    }

    fn admit_session_set_assignment(
        &self,
        source: &str,
        assignment: &ast::SetAssignment,
    ) -> Result<(), QueryServiceError> {
        admit_query_application_session_set_assignment(source, assignment)
    }

    async fn apply_session_set_assignment(
        &self,
        source: &str,
        assignment: &ast::SetAssignment,
    ) -> Result<(), QueryServiceError> {
        let mut state = self.state.lock().map_err(poisoned_state)?;
        self.apply_session_set_assignment_to_state(source, assignment, &mut state)
    }

    fn apply_session_set_assignment_to_state(
        &self,
        source: &str,
        assignment: &ast::SetAssignment,
        state: &mut SessionSqlState,
    ) -> Result<(), QueryServiceError> {
        match apply_query_application_session_set_assignment(source, assignment, state)? {
            SessionSetAssignmentOutcome::Applied => Ok(()),
            SessionSetAssignmentOutcome::SelectCatalog(catalog) => {
                self.apply_session_catalog(state, &catalog)
            }
        }
    }

    fn apply_session_catalog(
        &self,
        state: &mut SessionSqlState,
        catalog: &str,
    ) -> Result<(), QueryServiceError> {
        let catalog = resolve_catalog_name(&self.service.session_catalog_resolver, catalog)?;
        let current_database_exists = catalog.is_some()
            || self
                .service
                .session_catalog_resolver
                .database_exists(state.current_database())?;
        state.apply_resolved_catalog(catalog, current_database_exists);
        Ok(())
    }

    fn execute_session_kill(
        &self,
        source: &str,
        statement: &ast::KillStatement,
    ) -> Result<StatementResult, QueryServiceError> {
        let requester = self.token()?;
        execute_kill_statement(
            source,
            statement,
            requester,
            &self.service.query_control,
            self.service.client_connection_control.as_ref(),
        )
    }

    async fn init_database_with_cancellation(
        &self,
        schema: &str,
        cancellation: QueryCancellationView,
    ) -> Result<(), QueryServiceError> {
        let current_catalog = self
            .state
            .lock()
            .map_err(poisoned_state)?
            .current_catalog()
            .map(ToOwned::to_owned);
        let connector_context =
            crate::connector::connector_request_context_for_query(None, cancellation)
                .map_err(internal_error)?;
        let context = resolve_database_context(
            &self.service.session_catalog_resolver,
            current_catalog.as_deref(),
            schema,
            connector_context,
        )
        .await?;
        let mut state = self.state.lock().map_err(poisoned_state)?;
        state.set_resolved_database_context(context.catalog, context.database);
        Ok(())
    }

    async fn prepare_governed_query_operation(
        &self,
        sql: &str,
        parsed_statement: ParsedStatement,
        state: SessionSqlState,
        deadline: Option<Instant>,
        timeout_ms: Option<u64>,
        statement_token: StatementToken,
        cancellation: novarocks_workload_control::CancellationView,
    ) -> Result<PreparedQueryOperation, GovernedPreparationError> {
        let parsed_statement =
            state
                .substitute_user_variables(parsed_statement)
                .map_err(|error| {
                    GovernedPreparationError::Service(internal_error(error.to_string()))
                })?;
        let cancellation = QueryCancellationView::governed(cancellation, timeout_ms);
        let topology =
            wait_for_initial_query_topology(&self.service.topology, &cancellation, deadline)
                .await
                .map_err(|error| governed_preparation_error(error, &cancellation))?;
        let (current_catalog, current_database, execution_settings, mut optimizer_settings) =
            state.into_query_attempt_inputs();
        if optimizer_settings.optimizer_query_mem_limit_bytes.is_none() {
            optimizer_settings.optimizer_query_mem_limit_bytes =
                Some(self.service.optimizer_query_mem_limit_bytes as f64);
        }
        let context = RequestContext::admit(RequestAdmission::new(
            current_catalog,
            current_database,
            self.service.role,
            topology,
            deadline,
            cancellation.clone(),
            optimizer_settings,
        ));
        let query_options = with_query_hints(
            query_options_from_session_settings(&execution_settings),
            match &parsed_statement {
                ParsedStatement::Query(query) => Some(query),
                _ => None,
            },
        );
        let compiler = self.service.query_compiler.clone();
        let prepared = self
            .service
            .query_cpu_executor
            .run_cancellable(cancellation, move || {
                let _diagnostic_scope =
                    crate::preparation_diagnostics::enter_statement(statement_token);
                compiler.prepare_statement(&parsed_statement, &context, Some(query_options))
            })
            .await
            .map_err(|error| match error {
                QueryCpuRunError::Cancelled(reason) => {
                    GovernedPreparationError::Cancelled(cancellation_error(reason))
                }
                QueryCpuRunError::Executor(error) => {
                    GovernedPreparationError::Service(internal_error(error))
                }
            })?;
        match prepared {
            Ok(operation) => Ok(operation),
            Err(FrontendQueryCompilerError::Engine(error)) => {
                Err(GovernedPreparationError::Service(internal_error(error)))
            }
            Err(FrontendQueryCompilerError::Analyze(error)) => {
                Err(GovernedPreparationError::Service(
                    QueryServiceError::from_user_error(error.to_user_error(Some(sql))),
                ))
            }
        }
    }

    async fn execute_governed_set(
        &self,
        source: String,
        set: &ast::SetStatement,
    ) -> Result<StatementResult, QueryServiceError> {
        for assignment in &set.assignments {
            self.admit_session_set_assignment(&source, assignment)?;
        }
        let mut staged_state = self.state.lock().map_err(poisoned_state)?.clone();
        let (deadline, timeout_ms) = governed_query_deadline(&staged_state)?;
        let token = self.token()?;
        let mut statement = self
            .service
            .query_control
            .begin_governed_query_statement(
                token,
                &self.service.workload_root_admission,
                deadline.map(tokio::time::Instant::from_std),
                timeout_ms,
            )
            .map_err(|error| self.governed_statement_begin_error(error))?;
        for assignment in &set.assignments {
            let ast::SetTarget::UserVariable(variable) = &assignment.target else {
                if let Err(error) = self.apply_session_set_assignment_to_state(
                    &source,
                    assignment,
                    &mut staged_state,
                ) {
                    return Ok(self.governed_typed_error(error, statement));
                }
                continue;
            };
            let value = match &assignment.value {
                ast::SetValue::Expression(value) => print_expr(value),
                ast::SetValue::Query(query) => {
                    let parsed = ParsedStatement::Query((**query).clone());
                    let query_source = print_statement(&parsed);
                    match self
                        .evaluate_governed_scalar_query(
                            &query_source,
                            parsed,
                            deadline,
                            timeout_ms,
                            &statement,
                            staged_state.clone(),
                        )
                        .await
                    {
                        Ok(value) => value,
                        Err(error) => {
                            return Ok(self.governed_typed_error(error, statement));
                        }
                    }
                }
                ast::SetValue::Words(_) => {
                    return Ok(self.governed_typed_error(
                        QueryServiceError::new(
                            QueryServiceErrorKind::InvalidValue,
                            "user variable assignment requires an expression",
                        ),
                        statement,
                    ));
                }
            };
            staged_state.set_user_variable(&variable.value, value);
        }
        let mut live_state = self.state.lock().map_err(poisoned_state)?;
        match statement.seal_success_visibility() {
            novarocks_query_application::session_control::GovernedStatementVisibilitySealOutcome::Sealed => {
                *live_state = staged_state;
                drop(live_state);
                statement.complete_execution();
                Ok(StatementResult::GovernedCompletion(
                    GovernedCompletionStatementResult::new(
                        self.service.workload_resources.clone(),
                        statement,
                    ),
                ))
            }
            novarocks_query_application::session_control::GovernedStatementVisibilitySealOutcome::Cancelled(
                reason,
            ) => {
                drop(live_state);
                Ok(self.governed_typed_error(governed_cancellation_error(reason), statement))
            }
            novarocks_query_application::session_control::GovernedStatementVisibilitySealOutcome::Stale => {
                drop(live_state);
                Ok(self.governed_typed_error(
                    internal_error(
                        "governed SET query lost its statement generation before state commit",
                    ),
                    statement,
                ))
            }
        }
    }

    async fn evaluate_governed_scalar_query(
        &self,
        sql: &str,
        parsed: ParsedStatement,
        deadline: Option<Instant>,
        timeout_ms: Option<u64>,
        statement: &novarocks_query_application::session_control::GovernedQueryStatementOwner,
        state: SessionSqlState,
    ) -> Result<String, QueryServiceError> {
        let prepared = self
            .prepare_governed_query_operation(
                sql,
                parsed,
                state,
                deadline,
                timeout_ms,
                statement.token(),
                statement.cancellation().clone(),
            )
            .await
            .map_err(|error| match error {
                GovernedPreparationError::Service(error)
                | GovernedPreparationError::Cancelled(error) => error,
            })?;
        let child = statement
            .scope()
            .child(WorkRequest::new(WorkClass::Query))
            .map_err(|error| {
                QueryServiceError::new(
                    QueryServiceErrorKind::Unavailable,
                    format!("admit SET scalar query child: {error}"),
                )
            })?;
        match prepared {
            PreparedQueryOperation::Immediate(operation) => {
                let result = match operation.into_result() {
                    StatementResult::Query(result) => {
                        query_result_to_user_variable_literal(&result).map_err(scalar_query_error)
                    }
                    _ => Err(internal_error(
                        "SET scalar query preparation returned non-query immediate output",
                    )),
                };
                child.complete();
                result
            }
            PreparedQueryOperation::LogicalRead(read) => {
                let started = self.service.logical_read_launcher.start(read, child).await;
                let mut execution = match started {
                    Ok(execution) => execution,
                    Err(error) => return Err(governed_query_execution_error(error)),
                };
                let output = match execution.take_output() {
                    Some(output) => output,
                    None => {
                        let _ = execution.request_cancel();
                        return Err(internal_error(
                            "SET scalar query output was already transferred",
                        ));
                    }
                };
                let ExecutionOutput::Rows(stream) = output else {
                    let _ = execution.request_cancel();
                    return Err(internal_error(
                        "SET scalar query returned completion-only output",
                    ));
                };
                consume_governed_scalar_stream(&mut execution, stream).await
            }
            PreparedQueryOperation::Distributed(_) => {
                child.complete();
                Err(internal_error(
                    "SET scalar query preparation returned a legacy distributed operation",
                ))
            }
        }
    }

    async fn execute_governed_read(
        &self,
        sql: String,
        parsed_statement: ParsedStatement,
    ) -> Result<StatementResult, QueryServiceError> {
        debug_assert!(matches!(&parsed_statement, ParsedStatement::Query(_)));
        let state = self.state.lock().map_err(poisoned_state)?.clone();
        let (deadline, timeout_ms) = governed_query_deadline(&state)?;
        let token = self.token()?;
        let mut statement = self
            .service
            .query_control
            .begin_governed_query_statement(
                token,
                &self.service.workload_root_admission,
                deadline.map(tokio::time::Instant::from_std),
                timeout_ms,
            )
            .map_err(|error| self.governed_statement_begin_error(error))?;
        let prepared = match self
            .prepare_governed_query_operation(
                &sql,
                parsed_statement,
                state,
                deadline,
                timeout_ms,
                statement.token(),
                statement.cancellation().clone(),
            )
            .await
        {
            Ok(prepared) => prepared,
            Err(GovernedPreparationError::Cancelled(error)) => {
                let _ = statement.finish_unstarted_read_after_cancellation();
                return Err(error);
            }
            Err(GovernedPreparationError::Service(error)) => {
                let _ = statement.finish();
                return Err(error);
            }
        };
        let read = match prepared {
            PreparedQueryOperation::Immediate(operation) => {
                return match operation.into_result() {
                    StatementResult::Query(result) => Ok(StatementResult::GovernedQuery(
                        GovernedImmediateStatementResult::new(
                            result,
                            self.service.workload_resources.clone(),
                            statement,
                        ),
                    )),
                    StatementResult::GovernedQuery(_)
                    | StatementResult::StreamingQuery(_)
                    | StatementResult::GovernedCompletion(_)
                    | StatementResult::GovernedError(_) => {
                        let _ = statement.finish();
                        Err(internal_error(
                            "plain query preparation returned an already-owned protocol result",
                        ))
                    }
                    StatementResult::Ok => {
                        let _ = statement.finish();
                        Err(internal_error(
                            "plain query preparation returned completion-only immediate output",
                        ))
                    }
                };
            }
            PreparedQueryOperation::LogicalRead(read) => read,
            PreparedQueryOperation::Distributed(_) => {
                let _ = statement.finish();
                return Err(internal_error(
                    "plain query preparation returned a legacy distributed operation",
                ));
            }
        };
        let owner = statement
            .take_execution_owner()
            .expect("governed query start transfers its root owner exactly once");
        let execution = match self.service.logical_read_launcher.start(read, owner).await {
            Ok(execution) => execution,
            Err(error) => {
                let completion = statement.finish();
                return Err(governed_execution_error(error, completion));
            }
        };
        novarocks_query_application::protocol_delivery::StreamingStatementResult::try_from_execution(
            execution,
            self.service.workload_resources.clone(),
            statement,
        )
        .map(StatementResult::StreamingQuery)
        .map_err(governed_query_execution_error)
    }

    async fn execute_typed_statement(
        &self,
        sql: String,
        parsed_statement: ParsedStatement,
    ) -> Result<StatementResult, QueryServiceError> {
        reject_plain_query_from_legacy_typed_route(&parsed_statement)?;
        let state = self.state.lock().map_err(poisoned_state)?.clone();
        let parsed_statement = state
            .substitute_user_variables(parsed_statement)
            .map_err(|error| internal_error(error.to_string()))?;
        let query_timeout_secs = state.execution_settings().query_timeout_secs();
        let session_deadline = match query_timeout_secs {
            Some(seconds) => Instant::now()
                .checked_add(Duration::from_secs(seconds))
                .ok_or_else(|| {
                    QueryServiceError::new(
                        QueryServiceErrorKind::Internal,
                        "query deadline exceeds monotonic clock range",
                    )
                })?,
            None => Instant::now(),
        };
        let session_deadline = query_timeout_secs.map(|_| session_deadline);
        let deadline = if requires_lake_publication_deadline(&parsed_statement) {
            Some(
                self.service
                    .lake_publication_runtime_policy
                    .admit_deadline(Instant::now(), session_deadline)
                    .map_err(internal_error)?,
            )
        } else {
            session_deadline
        };
        let timeout_duration =
            deadline.map(|deadline| deadline.saturating_duration_since(Instant::now()));
        let timeout_ms = timeout_duration.map(timeout_message_millis);
        let token = self.token()?;
        let mut statement = self
            .service
            .query_control
            .begin_governed_statement(
                token,
                &self.service.workload_root_admission,
                typed_statement_work_class(&parsed_statement),
                deadline.map(tokio::time::Instant::from_std),
                timeout_ms,
            )
            .map_err(|error| self.governed_statement_begin_error(error))?;
        let cancellation = QueryCancellationView::governed(
            statement.cancellation().clone(),
            statement.timeout_ms(),
        );
        let topology = match self.service.topology.snapshot() {
            Ok(snapshot) => snapshot,
            Err(error) => {
                return Ok(self.governed_typed_error(
                    QueryServiceError::new(QueryServiceErrorKind::Internal, error.to_string()),
                    statement,
                ));
            }
        };
        let topology = if topology.targets().is_empty()
            && matches!(
                parsed_statement,
                ParsedStatement::Query(_)
                    | ParsedStatement::ExplainQuery(_)
                    | ParsedStatement::Dml(_)
            ) {
            let wait_deadline =
                deadline.unwrap_or_else(|| Instant::now() + Duration::from_secs(30));
            let wait_after_revision = topology.revision();
            match wait_for_eligible_query_topology_after(
                &self.service.topology,
                &cancellation,
                wait_after_revision,
                wait_deadline,
            )
            .await
            {
                Ok(snapshot) => snapshot,
                Err(error) => return Ok(self.governed_typed_error(error, statement)),
            }
        } else {
            topology
        };
        let (current_catalog, current_database, execution_settings, mut optimizer_settings) =
            state.into_query_attempt_inputs();
        // A session `SET` wins; otherwise admission freezes the process budget so
        // SQL costing never consults a process-global configuration.
        if optimizer_settings.optimizer_query_mem_limit_bytes.is_none() {
            optimizer_settings.optimizer_query_mem_limit_bytes =
                Some(self.service.optimizer_query_mem_limit_bytes as f64);
        }
        let context = RequestContext::admit(RequestAdmission::new(
            current_catalog,
            current_database,
            self.service.role,
            topology,
            deadline,
            cancellation.clone(),
            optimizer_settings,
        ));
        let compiler = self.service.query_compiler.clone();
        let command_executor = Arc::clone(&self.service.command_executor);
        let product_command_router = self.service.product_command_router.clone();
        let query_execution = self.service.query_execution.clone();
        let dml = Arc::clone(&self.service.dml);
        let insert_engine = Arc::clone(&self.service.insert_engine);
        let delete_engine = Arc::clone(&self.service.delete_engine);
        let mutation_engine = Arc::clone(&self.service.mutation_engine);
        let add_files_engine = Arc::clone(&self.service.add_files_engine);
        let ctas_engine = Arc::clone(&self.service.ctas_engine);
        let truncate_engine = Arc::clone(&self.service.truncate_engine);
        let query_options = with_query_hints(
            query_options_from_session_settings(&execution_settings),
            match &parsed_statement {
                ParsedStatement::ExplainQuery(explain) => Some(&explain.query),
                _ => None,
            },
        );
        let connector_context = match crate::connector::connector_request_context_for_query(
            Some(&query_options),
            cancellation.clone(),
        ) {
            Ok(context) => context,
            Err(error) => {
                return Ok(self.governed_typed_error(internal_error(error), statement));
            }
        };
        let diagnostic_statement = statement.token();
        let command_context = CommandContext::new(
            statement.scope().clone(),
            connector_context,
            diagnostic_statement,
        );
        let execution_owner = statement
            .take_execution_owner()
            .expect("governed typed statement transfers its execution owner exactly once");
        let worker_cancellation = cancellation.clone();
        let synchronous_command_executor = self.service.query_blocking_executor.clone();
        let query_cpu_executor = self.service.query_cpu_executor.clone();
        let mut worker: Pin<Box<dyn Future<Output = _> + Send>> = match parsed_statement {
            statement @ ParsedStatement::ExplainQuery(_) => Box::pin(async move {
                let execution_cancellation = worker_cancellation.clone();
                let (prepared, execution_owner) = query_cpu_executor
                    .run(move || {
                        let _diagnostic_scope =
                            crate::preparation_diagnostics::enter_statement(diagnostic_statement);
                        let result = if worker_cancellation.is_cancelled() {
                            Err(RoutedExecutionError::Engine(
                                "typed statement was cancelled before query compilation began"
                                    .to_owned(),
                            ))
                        } else {
                            compiler
                                .prepare_statement(&statement, &context, Some(query_options))
                                .map_err(|error| match error {
                                    FrontendQueryCompilerError::Engine(error) => {
                                        RoutedExecutionError::Engine(error)
                                    }
                                    FrontendQueryCompilerError::Analyze(error) => {
                                        RoutedExecutionError::User(error.to_user_error(Some(&sql)))
                                    }
                                })
                        };
                        (result, execution_owner)
                    })
                    .await?;
                let result = prepared.and_then(|operation| {
                    if execution_cancellation.is_cancelled() {
                        Err(RoutedExecutionError::Engine(
                            "typed statement was cancelled before prepared query execution began"
                                .to_owned(),
                        ))
                    } else {
                        execute_prepared_query(operation, &query_execution)
                            .map_err(RoutedExecutionError::Engine)
                    }
                });
                Ok((result, execution_owner))
            }),
            ParsedStatement::Dml(novarocks_parser::ast::DmlStatement::Delete(statement)) => {
                let prepare_dml = Arc::clone(&dml);
                let execute_dml = dml;
                let prepare_engine = Arc::clone(&delete_engine);
                let execute_engine = delete_engine;
                let prepare_context = context.clone();
                let prepare_options = query_options.clone();
                Box::pin(execute_prepared_dml_statement(
                    synchronous_command_executor,
                    worker_cancellation,
                    diagnostic_statement,
                    execution_owner,
                    move || {
                        dml_result(prepare_dml.prepare_delete(
                            prepare_engine.as_ref(),
                            crate::query_execution::dml::delete::DeleteStatement::Predicate(
                                &statement,
                            ),
                            &sql,
                            &prepare_context,
                            Some(&prepare_options),
                        ))
                    },
                    move |prepared| {
                        dml_statement_result(
                            execute_dml.execute_prepared_delete(execute_engine.as_ref(), prepared),
                        )
                    },
                ))
            }
            ParsedStatement::Dml(novarocks_parser::ast::DmlStatement::AddEqualityDelete(
                statement,
            )) => {
                let prepare_dml = Arc::clone(&dml);
                let execute_dml = dml;
                let prepare_engine = Arc::clone(&delete_engine);
                let execute_engine = delete_engine;
                let prepare_context = context.clone();
                let prepare_options = query_options.clone();
                Box::pin(execute_prepared_dml_statement(
                    synchronous_command_executor,
                    worker_cancellation,
                    diagnostic_statement,
                    execution_owner,
                    move || {
                        dml_result(prepare_dml.prepare_delete(
                            prepare_engine.as_ref(),
                            crate::query_execution::dml::delete::DeleteStatement::Equality(
                                &statement,
                            ),
                            &sql,
                            &prepare_context,
                            Some(&prepare_options),
                        ))
                    },
                    move |prepared| {
                        dml_statement_result(
                            execute_dml.execute_prepared_delete(execute_engine.as_ref(), prepared),
                        )
                    },
                ))
            }
            ParsedStatement::Dml(
                statement @ (novarocks_parser::ast::DmlStatement::Update(_)
                | novarocks_parser::ast::DmlStatement::Merge(_)),
            ) => {
                let prepare_dml = Arc::clone(&dml);
                let execute_dml = dml;
                let prepare_engine = Arc::clone(&mutation_engine);
                let execute_engine = mutation_engine;
                let prepare_context = context.clone();
                let prepare_options = query_options.clone();
                Box::pin(execute_prepared_dml_statement(
                    synchronous_command_executor,
                    worker_cancellation,
                    diagnostic_statement,
                    execution_owner,
                    move || {
                        dml_result(prepare_dml.prepare_typed_mutation(
                            prepare_engine.as_ref(),
                            &statement,
                            &sql,
                            &prepare_context,
                            Some(&prepare_options),
                        ))
                    },
                    move |prepared| {
                        dml_statement_result(
                            execute_dml
                                .execute_prepared_mutation(execute_engine.as_ref(), prepared),
                        )
                    },
                ))
            }
            ParsedStatement::Dml(statement) => Box::pin(execute_synchronous_statement(
                synchronous_command_executor,
                worker_cancellation,
                diagnostic_statement,
                execution_owner,
                move || {
                    execute_typed_dml_statement(
                        dml.as_ref(),
                        insert_engine.as_ref(),
                        delete_engine.as_ref(),
                        mutation_engine.as_ref(),
                        ctas_engine.as_ref(),
                        &statement,
                        &sql,
                        &context,
                        &query_options,
                    )
                },
            )),
            ParsedStatement::Table(table_statement) => Box::pin(async move {
                let command = validate_table_statement_admission(&table_statement, &sql)
                    .map_err(RoutedExecutionError::User)
                    .and_then(|()| {
                        lower_product_sql_command(&ParsedStatement::Table(table_statement))
                            .map_err(RoutedExecutionError::Engine)
                    })
                    .and_then(|command| {
                        command.ok_or_else(|| {
                            RoutedExecutionError::Engine(
                                "table parser admission did not produce a product command"
                                    .to_string(),
                            )
                        })
                    });
                match command {
                    Ok(command) => {
                        execute_product_statement(
                            product_command_router,
                            command,
                            context,
                            command_context,
                            execution_owner,
                        )
                        .await
                    }
                    Err(error) => Ok((Err(error), execution_owner)),
                }
            }),
            ParsedStatement::Catalog(novarocks_parser::ast::CatalogStatement::TruncateTable(
                statement,
            )) => {
                let command =
                    crate::query_execution::dml::truncate::command_from_typed_statement(&statement);
                let prepare_dml = Arc::clone(&dml);
                let prepare_truncate_engine = Arc::clone(&truncate_engine);
                Box::pin(execute_prepared_dml_statement(
                    synchronous_command_executor,
                    worker_cancellation,
                    diagnostic_statement,
                    execution_owner,
                    move || {
                        prepare_dml
                            .prepare_truncate(
                                prepare_truncate_engine.as_ref(),
                                command,
                                &context,
                                Some(&query_options),
                            )
                            .map_err(|error| RoutedExecutionError::Engine(error.to_string()))
                    },
                    move |prepared| {
                        dml.execute_prepared_truncate(truncate_engine.as_ref(), prepared)
                            .map(|()| StatementResult::Ok)
                            .map_err(|error| RoutedExecutionError::Engine(error.to_string()))
                    },
                ))
            }
            ParsedStatement::Iceberg(novarocks_parser::ast::IcebergStatement::AlterTable(
                statement,
            )) => Box::pin(async move {
                match crate::query_execution::dml::add_files::command_from_typed_statement(
                    &statement,
                ) {
                    Ok(command) => {
                        let prepare_dml = Arc::clone(&dml);
                        let prepare_add_files_engine = Arc::clone(&add_files_engine);
                        execute_prepared_dml_statement(
                            synchronous_command_executor,
                            worker_cancellation,
                            diagnostic_statement,
                            execution_owner,
                            move || {
                                prepare_dml
                                    .prepare_add_files(
                                        prepare_add_files_engine.as_ref(),
                                        command,
                                        &context,
                                        Some(&query_options),
                                    )
                                    .map_err(|error| {
                                        RoutedExecutionError::Engine(error.to_string())
                                    })
                            },
                            move |prepared| {
                                dml.execute_prepared_add_files(add_files_engine.as_ref(), prepared)
                                    .map_err(|error| {
                                        RoutedExecutionError::Engine(error.to_string())
                                    })
                                    .and_then(|count| {
                                        add_files_status(count)
                                            .map(StatementResult::Query)
                                            .map_err(RoutedExecutionError::Engine)
                                    })
                            },
                        )
                        .await
                    }
                    Err(_) => {
                        let command = lower_product_sql_command(&ParsedStatement::Iceberg(
                            novarocks_parser::ast::IcebergStatement::AlterTable(statement),
                        ))
                        .map_err(RoutedExecutionError::Engine)
                        .and_then(|command| {
                            command.ok_or_else(|| {
                                RoutedExecutionError::Engine(
                                    "Iceberg parser admission did not produce a product command"
                                        .to_string(),
                                )
                            })
                        });
                        match command {
                            Ok(command) => {
                                execute_product_statement(
                                    product_command_router,
                                    command,
                                    context,
                                    command_context,
                                    execution_owner,
                                )
                                .await
                            }
                            Err(error) => Ok((Err(error), execution_owner)),
                        }
                    }
                }
            }),
            ParsedStatement::ShowBackends(_) => Box::pin(async move {
                let result = command_executor
                    .execute_show_backends(&context, &command_context)
                    .await
                    .map_err(|error| RoutedExecutionError::Engine(error.to_string()));
                Ok((result, execution_owner))
            }),
            ParsedStatement::Maintenance(novarocks_parser::ast::MaintenanceStatement::Call(
                statement,
            )) => Box::pin(async move {
                let special_result = command_executor
                    .execute_maintenance_call(&statement, &context, &command_context)
                    .await
                    .map_err(|error| RoutedExecutionError::Engine(error.to_string()));
                match special_result {
                    Err(error) => Ok((Err(error), execution_owner)),
                    Ok(Some(result)) => Ok((Ok(result), execution_owner)),
                    Ok(None) => {
                        let command = lower_product_sql_command(&ParsedStatement::Maintenance(
                            novarocks_parser::ast::MaintenanceStatement::Call(statement),
                        ))
                        .map_err(RoutedExecutionError::Engine)
                        .and_then(|command| {
                            command.ok_or_else(|| {
                                RoutedExecutionError::Engine(
                                    "typed statement has no declared product or specialized owner"
                                        .to_string(),
                                )
                            })
                        });
                        match command {
                            Ok(command) => {
                                execute_product_statement(
                                    product_command_router,
                                    command,
                                    context,
                                    command_context,
                                    execution_owner,
                                )
                                .await
                            }
                            Err(error) => Ok((Err(error), execution_owner)),
                        }
                    }
                }
            }),
            ParsedStatement::MaterializedView(statement) => Box::pin(async move {
                let result = command_executor
                    .execute_materialized_view(&statement, &context, &command_context)
                    .await
                    .map_err(|error| RoutedExecutionError::Engine(error.to_string()));
                Ok((result, execution_owner))
            }),
            ParsedStatement::View(statement) => Box::pin(async move {
                let result = command_executor
                    .execute_view(&statement, &context, &command_context)
                    .await
                    .map_err(|error| RoutedExecutionError::Engine(error.to_string()));
                Ok((result, execution_owner))
            }),
            other_statement => Box::pin(async move {
                let command = lower_product_sql_command(&other_statement)
                    .map_err(RoutedExecutionError::Engine)
                    .and_then(|command| {
                        command.ok_or_else(|| {
                            RoutedExecutionError::Engine(
                                "typed statement has no declared product owner".to_string(),
                            )
                        })
                    });
                match command {
                    Ok(command) => {
                        execute_product_statement(
                            product_command_router,
                            command,
                            context,
                            command_context,
                            execution_owner,
                        )
                        .await
                    }
                    Err(error) => Ok((Err(error), execution_owner)),
                }
            }),
        };
        let (result, execution_owner) = if let Some(timeout_duration) = timeout_duration {
            match tokio::time::timeout(timeout_duration, &mut worker).await {
                Ok(result) => result.map_err(internal_error)?,
                Err(_) => {
                    let timeout_ms = timeout_message_millis(timeout_duration);
                    self.cancel_current(QueryCancellationReason::DeadlineExceeded { timeout_ms });
                    // A timeout is not complete until the worker releases the
                    // statement lease. Waiting here also fences Backend abort
                    // acknowledgement before this session admits its next SQL.
                    let (_, execution_owner) = worker.await.map_err(internal_error)?;
                    statement.restore_execution_owner(execution_owner);
                    return Ok(self.governed_typed_error(
                        QueryServiceError::new(
                            QueryServiceErrorKind::Timeout,
                            format!("query timed out after {timeout_ms} ms"),
                        ),
                        statement,
                    ));
                }
            }
        } else {
            // A drain cancellation is injected through the same statement
            // source as ordinary query control. Some blocking execution paths
            // only observe it at their next cooperative boundary, so the
            // protocol owner must not wait indefinitely before returning the
            // first-wins typed cancellation to its already-admitted client.
            loop {
                tokio::select! {
                    result = &mut worker => break result.map_err(internal_error)?,
                    _ = tokio::time::sleep(Duration::from_millis(10)) => {
                        if cancellation.is_cancelled() {
                            let reason = cancellation
                                .reason()
                                .expect("cancelled statement has a reason");
                            if cancellation_requires_statement_fence(&reason) {
                                // KILL QUERY keeps the client connection alive.
                                // Do not return its interrupt until the worker
                                // has released the exact statement generation,
                                // otherwise the next command races that lease
                                // and is rejected as StatementBusy.
                                break (&mut worker)
                                    .await
                                    .map_err(internal_error)?;
                            }
                            return Ok(self.governed_typed_error(
                                cancellation_error(reason),
                                statement,
                            ));
                        }
                    }
                }
            }
        };
        statement.restore_execution_owner(execution_owner);
        if cancellation.is_cancelled() {
            return Ok(self.governed_typed_error(
                cancellation_error(cancellation.reason().expect("cancelled view has a reason")),
                statement,
            ));
        }
        match result {
            Ok(StatementResult::Query(result)) => Ok(StatementResult::GovernedQuery(
                GovernedImmediateStatementResult::new(
                    result,
                    self.service.workload_resources.clone(),
                    statement,
                ),
            )),
            Ok(StatementResult::Ok) => Ok(StatementResult::GovernedCompletion(
                GovernedCompletionStatementResult::new(
                    self.service.workload_resources.clone(),
                    statement,
                ),
            )),
            Ok(
                StatementResult::GovernedQuery(_)
                | StatementResult::StreamingQuery(_)
                | StatementResult::GovernedCompletion(_)
                | StatementResult::GovernedError(_),
            ) => Ok(self.governed_typed_error(
                internal_error("typed statement returned an already-owned protocol result"),
                statement,
            )),
            Err(error) => Ok(self.governed_typed_error(
                match error {
                    RoutedExecutionError::Engine(error) => internal_error(error),
                    RoutedExecutionError::User(error) => QueryServiceError::from_user_error(error),
                    RoutedExecutionError::Publication { message, terminal } => {
                        QueryServiceError::with_publication_terminal(message, terminal)
                    }
                },
                statement,
            )),
        }
    }

    fn governed_typed_error(
        &self,
        error: QueryServiceError,
        statement: novarocks_query_application::session_control::GovernedQueryStatementOwner,
    ) -> StatementResult {
        StatementResult::GovernedError(GovernedErrorStatementResult::new(
            error,
            self.service.workload_resources.clone(),
            statement,
        ))
    }
}

/// A read that stops before its execution owner is transferred has no actor
/// to settle a queued cancellation control. Preserve that fact through every
/// preparation wait, not only through the CPU executor.
fn governed_preparation_error(
    error: QueryServiceError,
    cancellation: &QueryCancellationView,
) -> GovernedPreparationError {
    if cancellation.reason().is_some()
        || matches!(
            error.kind(),
            QueryServiceErrorKind::Interrupted
                | QueryServiceErrorKind::Timeout
                | QueryServiceErrorKind::FrontendDraining
        )
    {
        GovernedPreparationError::Cancelled(error)
    } else {
        GovernedPreparationError::Service(error)
    }
}

fn reject_plain_query_from_legacy_typed_route(
    statement: &ParsedStatement,
) -> Result<(), QueryServiceError> {
    if matches!(statement, ParsedStatement::Query(_)) {
        return Err(internal_error(
            "plain query reached the legacy typed execution route instead of the governed launcher",
        ));
    }
    Ok(())
}

fn with_query_hints(
    query_options: QueryOptions,
    query: Option<&novarocks_parser::ast::Query>,
) -> QueryOptions {
    let mut raw = *query_options.as_proto();
    if let Some(query) = query {
        raw.allow_throw_exception =
            novarocks_sql::admission::query_allows_throw_exception_hint(query);
        if let Some(limit) = novarocks_sql::admission::query_mem_limit_hint(query) {
            raw.query_mem_limit = limit;
        }
    }
    QueryOptions::parse(raw).expect("typed query hints do not invalidate query options")
}

fn execute_prepared_query(
    operation: PreparedQueryOperation,
    query_execution: &QueryExecutionService,
) -> Result<StatementResult, String> {
    match operation {
        PreparedQueryOperation::Immediate(operation) => Ok(operation.into_result()),
        PreparedQueryOperation::LogicalRead(_) => {
            Err("logical read requires the governed Query Application launcher".to_string())
        }
        PreparedQueryOperation::Distributed(operation) => query_execution
            .execute_prepared(operation)
            .map_err(|error| error.to_string()),
    }
}

#[async_trait]
impl QuerySession for FrontendQuerySession {
    async fn init_database(
        &self,
        schema: &str,
    ) -> Result<novarocks_query_application::session::QuerySessionStatement, QueryServiceError>
    {
        let token = self.token()?;
        let mut statement = self
            .service
            .query_control
            .begin_governed_statement(
                token,
                &self.service.workload_root_admission,
                WorkClass::Management,
                None,
                None,
            )
            .map_err(|error| self.governed_statement_begin_error(error))?;
        let cancellation = QueryCancellationView::governed(
            statement.cancellation().clone(),
            statement.timeout_ms(),
        );
        match self
            .init_database_with_cancellation(schema, cancellation)
            .await
        {
            Ok(()) => {
                statement.complete_execution();
                Ok(
                    novarocks_query_application::session::QuerySessionStatement::output_owned(
                        StatementResult::GovernedCompletion(
                            GovernedCompletionStatementResult::new(
                                self.service.workload_resources.clone(),
                                statement,
                            ),
                        ),
                    ),
                )
            }
            Err(error) => Ok(
                novarocks_query_application::session::QuerySessionStatement::output_owned(
                    self.governed_typed_error(error, statement),
                ),
            ),
        }
    }

    async fn execute_batch(
        &self,
        sql: &str,
    ) -> Result<novarocks_query_application::session::QuerySessionStatement, QueryServiceError>
    {
        let Some(statement) = unnegotiated_query_statement(sql)? else {
            return Ok(
                novarocks_query_application::session::QuerySessionStatement::output_owned(
                    StatementResult::Ok,
                ),
            );
        };
        self.execute_statement(statement).await
    }

    async fn execute_statement(
        &self,
        statement: &str,
    ) -> Result<novarocks_query_application::session::QuerySessionStatement, QueryServiceError>
    {
        self.execute_one_statement(statement).await
    }

    fn cancel_current(&self, reason: QueryCancellationReason) {
        let token = self
            .lease
            .lock()
            .ok()
            .and_then(|lease| lease.as_ref().map(QuerySessionLease::token));
        if let Some(token) = token {
            let _ = self
                .service
                .query_control
                .cancel_session_statement(token, reason);
        }
    }

    fn close(&self) {
        self.cancel_current(QueryCancellationReason::ClientDisconnected);
        if let Ok(mut lease) = self.lease.lock() {
            lease.take();
        }
    }
}

impl Drop for FrontendQuerySession {
    fn drop(&mut self) {
        self.close();
    }
}

#[derive(Debug)]
struct DatabaseContext {
    catalog: Option<String>,
    database: String,
}

fn resolve_catalog_name(
    resolver: &SessionCatalogService,
    catalog: &str,
) -> Result<Option<String>, QueryServiceError> {
    let normalized =
        normalize_identifier(catalog).map_err(|error| internal_error(error.to_string()))?;
    if normalized == DEFAULT_CATALOG {
        return Ok(None);
    }
    // Session catalog context is an admission decision, not a local binding
    // lookup: a catalog whose durable attachment is absent is unknown, while one
    // this process has not materialized yet is unavailable.
    resolver.require_external_catalog_ready(&normalized)?;
    Ok(Some(normalized))
}

async fn resolve_database_context(
    resolver: &SessionCatalogService,
    current_catalog: Option<&str>,
    schema: &str,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
) -> Result<DatabaseContext, QueryServiceError> {
    let parts = schema
        .split('.')
        .map(|part| part.trim().trim_matches('`'))
        .collect::<Vec<_>>();
    match parts.as_slice() {
        [database] => {
            let database = normalize_identifier(database)
                .map_err(|error| internal_error(error.to_string()))?;
            match current_catalog {
                Some(catalog) => {
                    if resolver
                        .external_namespace_exists(connector_context, catalog, &database)
                        .await?
                    {
                        Ok(DatabaseContext {
                            catalog: Some(catalog.to_string()),
                            database,
                        })
                    } else {
                        Err(QueryServiceError::new(
                            QueryServiceErrorKind::BadDatabase,
                            format!("unknown database `{schema}`"),
                        ))
                    }
                }
                None => {
                    if resolver.database_exists(&database)? {
                        Ok(DatabaseContext {
                            catalog: None,
                            database,
                        })
                    } else {
                        Err(QueryServiceError::new(
                            QueryServiceErrorKind::BadDatabase,
                            format!("unknown database `{schema}`"),
                        ))
                    }
                }
            }
        }
        [catalog, database] => {
            let catalog = resolve_catalog_name(resolver, catalog)?;
            let database = normalize_identifier(database)
                .map_err(|error| internal_error(error.to_string()))?;
            match catalog {
                Some(catalog) => {
                    if resolver
                        .external_namespace_exists(connector_context, &catalog, &database)
                        .await?
                    {
                        Ok(DatabaseContext {
                            catalog: Some(catalog),
                            database,
                        })
                    } else {
                        Err(QueryServiceError::new(
                            QueryServiceErrorKind::BadDatabase,
                            format!("unknown database `{schema}`"),
                        ))
                    }
                }
                None => {
                    if resolver.database_exists(&database)? {
                        Ok(DatabaseContext {
                            catalog: None,
                            database,
                        })
                    } else {
                        Err(QueryServiceError::new(
                            QueryServiceErrorKind::BadDatabase,
                            format!("unknown database `{schema}`"),
                        ))
                    }
                }
            }
        }
        _ => Err(QueryServiceError::new(
            QueryServiceErrorKind::BadDatabase,
            format!("unknown database `{schema}`; expected `<database>` or `<catalog>.<database>`"),
        )),
    }
}

fn poisoned_state<T>(_error: std::sync::PoisonError<T>) -> QueryServiceError {
    QueryServiceError::new(
        QueryServiceErrorKind::Internal,
        "frontend query session state lock poisoned",
    )
}

fn internal_error(message: impl Into<String>) -> QueryServiceError {
    QueryServiceError::new(QueryServiceErrorKind::Internal, message)
}

/// Keep a user-visible timeout at its admitted millisecond precision. The
/// remaining duration is sampled after deadline admission, so truncating it
/// can turn a one-second timeout into a misleading `999 ms` message.
fn timeout_message_millis(timeout: Duration) -> u64 {
    let millis = timeout.as_nanos().saturating_add(999_999) / 1_000_000;
    u64::try_from(millis).unwrap_or(u64::MAX)
}

async fn consume_governed_scalar_stream(
    execution: &mut novarocks_query_application::api::ExecutionHandle,
    mut stream: novarocks_query_application::api::QueryResultStream,
) -> Result<String, QueryServiceError> {
    let schema = match stream.begin_schema() {
        Some(schema) => schema,
        None => {
            let _ = execution.request_cancel();
            return Err(internal_error(
                "SET scalar query result has no schema delivery",
            ));
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
        return Err(scalar_query_error(message));
    }
    let field = &schema.schema().fields()[0];
    let column = QueryResultColumn::new(
        field.name(),
        field.data_type().clone(),
        field.nullable(),
        field.logical_type().cloned(),
    );
    schema.complete();

    let mut value = None;
    loop {
        let delivery = match stream.next().await {
            Ok(Some(delivery)) => delivery,
            Ok(None) => {
                let _ = execution.request_cancel();
                return Err(internal_error(
                    "SET scalar query stream ended without success EOF",
                ));
            }
            Err(error) => {
                let _ = execution.request_cancel();
                return Err(governed_query_execution_error(error));
            }
        };
        match delivery {
            ResultDelivery::Batch(delivery) => {
                let rows = delivery.batch().num_rows();
                if rows > 1 || (rows == 1 && value.is_some()) {
                    let message = "Subquery returns more than 1 row".to_string();
                    delivery.fail(QueryExecutionError::new(
                        QueryExecutionErrorKind::InvalidRequest,
                        message.clone(),
                    ));
                    let _ = execution.request_cancel();
                    return Err(scalar_query_error(message));
                }
                if rows == 1 {
                    let result = QueryResult {
                        columns: vec![column.clone()],
                        batches: vec![delivery.batch().clone()],
                    };
                    value = match query_result_to_user_variable_literal(&result) {
                        Ok(value) => Some(value),
                        Err(message) => {
                            delivery.fail(QueryExecutionError::new(
                                QueryExecutionErrorKind::InvalidRequest,
                                message.clone(),
                            ));
                            let _ = execution.request_cancel();
                            return Err(scalar_query_error(message));
                        }
                    };
                }
                if let Err(error) = delivery.complete_decoded() {
                    let _ = execution.request_cancel();
                    return Err(governed_query_execution_error(error));
                }
            }
            ResultDelivery::End(delivery) => {
                delivery.complete();
                return Ok(value.unwrap_or_else(|| "null".to_string()));
            }
        }
    }
}

async fn wait_for_initial_query_topology(
    topology: &BackendTopologyService,
    cancellation: &QueryCancellationView,
    statement_deadline: Option<Instant>,
) -> Result<BackendTopologySnapshot, QueryServiceError> {
    const INITIAL_TOPOLOGY_WAIT: Duration = Duration::from_secs(30);

    // Subscribe before the first read. A revision published between the read
    // and `changed` remains pending on this receiver, so an eligible backend
    // cannot be lost behind the planning-stage wait boundary.
    let mut changes = topology.subscribe_changes();
    let now = Instant::now();
    let stage_limit = now.checked_add(INITIAL_TOPOLOGY_WAIT).ok_or_else(|| {
        internal_error("initial backend topology deadline exceeds monotonic clock range")
    })?;
    let stage_deadline = statement_deadline
        .map(|deadline| deadline.min(stage_limit))
        .unwrap_or(stage_limit);
    loop {
        if let Some(reason) = cancellation.reason() {
            return Err(cancellation_error(reason));
        }
        if Instant::now() >= stage_deadline {
            return Err(QueryServiceError::new(
                QueryServiceErrorKind::Timeout,
                "timed out waiting for an eligible backend before query planning",
            ));
        }
        let snapshot = topology
            .snapshot()
            .map_err(|error| internal_error(error.to_string()))?;
        if !snapshot.targets().is_empty() {
            return Ok(snapshot);
        }
        let stage_sleep = tokio::time::sleep_until(tokio::time::Instant::from_std(stage_deadline));
        tokio::pin!(stage_sleep);
        tokio::select! {
            biased;
            reason = cancellation.cancelled() => return Err(cancellation_error(reason)),
            _ = &mut stage_sleep => {
                return Err(QueryServiceError::new(
                    QueryServiceErrorKind::Timeout,
                    "timed out waiting for an eligible backend before query planning",
                ));
            }
            changed = changes.changed() => {
                changed.map_err(|_| QueryServiceError::new(
                    QueryServiceErrorKind::Unavailable,
                    "backend topology change stream closed while waiting for query planning",
                ))?;
            }
        }
    }
}

/// Waits for the replacement round's exact eligibility condition through the
/// topology watch stream. This owns no blocking thread while topology is empty.
async fn wait_for_eligible_query_topology_after(
    topology: &BackendTopologyService,
    cancellation: &QueryCancellationView,
    revision: u64,
    deadline: Instant,
) -> Result<BackendTopologySnapshot, QueryServiceError> {
    let mut changes = topology.subscribe_changes();
    loop {
        if let Some(reason) = cancellation.reason() {
            return Err(cancellation_error(reason));
        }
        if Instant::now() >= deadline {
            return Err(internal_error(format!(
                "timed out waiting for an eligible backend topology revision after {revision}"
            )));
        }
        let snapshot = topology
            .snapshot()
            .map_err(|error| internal_error(error.to_string()))?;
        if snapshot.revision() > revision && !snapshot.targets().is_empty() {
            return Ok(snapshot);
        }
        let sleep = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline));
        tokio::pin!(sleep);
        tokio::select! {
            biased;
            reason = cancellation.cancelled() => return Err(cancellation_error(reason)),
            _ = &mut sleep => return Err(internal_error(format!(
                "timed out waiting for an eligible backend topology revision after {revision}"
            ))),
            changed = changes.changed() => {
                changed.map_err(|_| QueryServiceError::new(
                    QueryServiceErrorKind::Unavailable,
                    "backend topology change stream closed while waiting for an eligible replacement",
                ))?;
            }
        }
    }
}

/// Frontend's one-way projection from Query Application session state into
/// the native query-options DTO consumed at the execution boundary.
fn query_options_from_session_settings(settings: &SessionExecutionSettings) -> QueryOptions {
    QueryOptions::parse(novarocks::QueryOptions {
        group_concat_max_len: Some(settings.group_concat_max_len()),
        query_timeout: settings
            .query_timeout_secs()
            .and_then(|value| value.try_into().ok())
            .unwrap_or_default(),
        pipeline_dop: settings.pipeline_dop().unwrap_or_default(),
        runtime_filter_scan_wait_time_ms: settings.runtime_filter_scan_wait_time_ms(),
        runtime_filter_wait_timeout_ms: settings.runtime_filter_wait_timeout_ms(),
        enable_parquet_reader_page_index: settings.enable_parquet_reader_page_index(),
        enable_scan_datacache: settings.enable_scan_datacache(),
        enable_populate_datacache: settings.enable_populate_datacache(),
        ..Default::default()
    })
    // Session settings never enable spilling, so the Protocol validation
    // performed here cannot reject an internally constructed value.
    .expect("session settings must satisfy the native query-options contract")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::query_execution::dml::delete::{
        DeleteEngine, DeleteOperation, DeletePrepared, DeleteWriteReport, PrepareDeleteRequest,
        PreparedDelete,
    };
    use crate::query_execution::dml::insert::{
        IcebergPreparedInsert, IcebergWriteReport, PrepareIcebergInsert, PreparedIcebergInsert,
        ResolveInsertTarget, ResolvedInsertTarget,
    };
    use crate::query_execution::dml::mutation::{
        MutationEngine, MutationOperation, MutationPrepared, MutationStageOutcome,
        MutationStatementKind, PrepareMutationRequest, PreparedMutation,
    };
    use arrow::{
        array::{Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use novarocks_query_application::admitted_query_context::QueryExecutionContext;
    use novarocks_query_application::api::BackendTopologySnapshot;
    use novarocks_query_application::api::ResultField;
    use novarocks_query_application::cancellation::QueryCancellationSource;
    use novarocks_query_application::sql::{SqlBatchCursor, split_sql_statements};
    use novarocks_query_application::test_support::{
        ResultStreamTestProducer, TestResultDeliveryDisposition,
    };
    use novarocks_sql::compiler::SessionOptimizerSettings;
    use novarocks_types::schema::ColumnDef;
    use novarocks_types::{AttemptId, QueryExecutionId, QueryId};
    use novarocks_workload_control::CancellationReason as WorkCancellationReason;
    use novarocks_workload_control::ResourceConfig;

    fn default_query_options() -> QueryOptions {
        QueryOptions::parse(novarocks_proto_models::novarocks::QueryOptions::default())
            .expect("default wire query options are valid")
    }

    #[test]
    fn frontend_projects_query_application_session_settings_to_native_options() {
        let mut settings = SessionExecutionSettings::default();
        settings.set_query_timeout_secs(17);
        settings.set_group_concat_max_len(-1);
        settings.set_pipeline_dop(4);
        settings
            .set_runtime_filter_scan_wait_time_ms(0)
            .expect("zero is valid");
        settings
            .set_runtime_filter_wait_timeout_ms(3)
            .expect("positive timeout is valid");
        settings.set_enable_parquet_reader_page_index(true);
        settings.set_enable_scan_datacache(true);
        settings.set_enable_populate_datacache(true);

        let options = query_options_from_session_settings(&settings);
        let proto = options.as_proto();
        assert_eq!(proto.group_concat_max_len, Some(-1));
        assert_eq!(proto.query_timeout, 17);
        assert_eq!(proto.pipeline_dop, 4);
        assert_eq!(proto.runtime_filter_scan_wait_time_ms, Some(0));
        assert_eq!(proto.runtime_filter_wait_timeout_ms, Some(3));
        assert!(proto.enable_parquet_reader_page_index);
        assert!(proto.enable_scan_datacache);
        assert!(proto.enable_populate_datacache);
    }

    #[test]
    fn add_files_status_uses_query_application_immediate_result_contract() {
        let result = add_files_status(2).expect("build ADD FILES status");
        assert_eq!(result.columns[0].name(), "status");
        let values = result.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("status text column");
        assert_eq!(values.value(0), "Added 2 file(s)");
    }

    fn scalar_stream_fixture(
        fields: Vec<ResultField>,
    ) -> (
        ResultStreamTestProducer,
        novarocks_query_application::api::ExecutionHandle,
        LocalResourceAuthority,
        novarocks_query_application::test_support::TestResultDeliveryReceipt,
    ) {
        ResultStreamTestProducer::open(
            QueryExecutionId::new(
                QueryId::new(83, 1),
                AttemptId::new(1).expect("test attempt"),
            )
            .expect("test execution"),
            fields,
            1,
            ResourceConfig {
                total_bytes: 1024 * 1024,
                control_bytes: 1024,
                per_scope_bytes: 1024 * 1024 - 1024,
            },
        )
        .expect("open scalar result stream")
    }

    fn scalar_field(nullable: bool) -> ResultField {
        ResultField::new("value", DataType::Int64, nullable, None)
    }

    fn scalar_batch(values: Vec<Option<i64>>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                true,
            )])),
            vec![Arc::new(Int64Array::from(values))],
        )
        .expect("scalar batch")
    }

    #[tokio::test]
    async fn governed_scalar_stream_settles_each_batch_before_eof() {
        let (producer, mut execution, resources, schema_receipt) =
            scalar_stream_fixture(vec![scalar_field(false)]);
        let ExecutionOutput::Rows(stream) = execution.take_output().expect("scalar output") else {
            panic!("expected row output")
        };

        let producer_side = async {
            assert_eq!(
                schema_receipt.wait().await,
                TestResultDeliveryDisposition::Completed
            );
            let batch_receipt = producer
                .enqueue_batch(0, scalar_batch(vec![Some(7)]))
                .await
                .expect("enqueue scalar batch");
            assert_eq!(
                batch_receipt.wait().await,
                TestResultDeliveryDisposition::Completed
            );
            assert_eq!(resources.snapshot().result_credit.held_bytes(), 0);
            let end_receipt = producer.enqueue_end(1).await;
            assert_eq!(
                end_receipt.wait().await,
                TestResultDeliveryDisposition::Completed
            );
            producer.finish();
        };
        let (value, ()) = tokio::join!(
            consume_governed_scalar_stream(&mut execution, stream),
            producer_side
        );

        assert_eq!(value.expect("scalar query succeeds"), "7");
        assert_eq!(resources.snapshot().result_credit.held_bytes(), 0);
    }

    #[tokio::test]
    async fn governed_scalar_stream_maps_empty_and_null_to_null() {
        for (batch, expected) in [(None, "null"), (Some(scalar_batch(vec![None])), "NULL")] {
            let (producer, mut execution, _resources, schema_receipt) =
                scalar_stream_fixture(vec![scalar_field(true)]);
            let ExecutionOutput::Rows(stream) = execution.take_output().expect("scalar output")
            else {
                panic!("expected row output")
            };
            let producer_side = async {
                assert_eq!(
                    schema_receipt.wait().await,
                    TestResultDeliveryDisposition::Completed
                );
                let mut sequence = 0;
                if let Some(batch) = batch {
                    let receipt = producer
                        .enqueue_batch(sequence, batch)
                        .await
                        .expect("enqueue null batch");
                    assert_eq!(
                        receipt.wait().await,
                        TestResultDeliveryDisposition::Completed
                    );
                    sequence += 1;
                }
                let receipt = producer.enqueue_end(sequence).await;
                assert_eq!(
                    receipt.wait().await,
                    TestResultDeliveryDisposition::Completed
                );
                producer.finish();
            };
            let (value, ()) = tokio::join!(
                consume_governed_scalar_stream(&mut execution, stream),
                producer_side
            );
            assert_eq!(value.expect("empty or null scalar succeeds"), expected);
        }
    }

    #[tokio::test]
    async fn governed_scalar_stream_rejects_multiple_rows_and_cancels_execution() {
        let (producer, mut execution, resources, schema_receipt) =
            scalar_stream_fixture(vec![scalar_field(false)]);
        let ExecutionOutput::Rows(stream) = execution.take_output().expect("scalar output") else {
            panic!("expected row output")
        };
        let producer_side = async {
            assert_eq!(
                schema_receipt.wait().await,
                TestResultDeliveryDisposition::Completed
            );
            let receipt = producer
                .enqueue_batch(0, scalar_batch(vec![Some(1), Some(2)]))
                .await
                .expect("enqueue multirow batch");
            assert!(matches!(
                receipt.wait().await,
                TestResultDeliveryDisposition::Failed(_)
            ));
        };
        let (result, ()) = tokio::join!(
            consume_governed_scalar_stream(&mut execution, stream),
            producer_side
        );

        let error = result.expect_err("multirow scalar must fail");
        assert_eq!(error.kind(), QueryServiceErrorKind::InvalidValue);
        assert_eq!(
            producer.cancellation_reason(),
            Some(WorkCancellationReason::Requested)
        );
        assert_eq!(resources.snapshot().result_credit.held_bytes(), 0);
        producer.finish();
    }

    #[tokio::test]
    async fn governed_scalar_stream_rejects_schema_and_fails_its_delivery() {
        let fields = vec![scalar_field(false), scalar_field(false)];
        let (producer, mut execution, _resources, schema_receipt) = scalar_stream_fixture(fields);
        let ExecutionOutput::Rows(stream) = execution.take_output().expect("scalar output") else {
            panic!("expected row output")
        };

        let error = consume_governed_scalar_stream(&mut execution, stream)
            .await
            .expect_err("two-column scalar result must fail");
        assert_eq!(error.kind(), QueryServiceErrorKind::InvalidValue);
        assert!(matches!(
            schema_receipt.wait().await,
            TestResultDeliveryDisposition::Failed(_)
        ));
        assert_eq!(
            producer.cancellation_reason(),
            Some(WorkCancellationReason::Requested)
        );
        producer.finish();
    }

    #[tokio::test]
    async fn governed_scalar_stream_failure_cancels_execution_after_schema_ack() {
        let (producer, mut execution, _resources, schema_receipt) =
            scalar_stream_fixture(vec![scalar_field(false)]);
        let ExecutionOutput::Rows(stream) = execution.take_output().expect("scalar output") else {
            panic!("expected row output")
        };
        producer.fail(QueryExecutionError::new(
            QueryExecutionErrorKind::Failed,
            "test scalar stream failure",
        ));

        let error = consume_governed_scalar_stream(&mut execution, stream)
            .await
            .expect_err("failed scalar result stream must fail");
        assert_eq!(error.kind(), QueryServiceErrorKind::Internal);
        assert_eq!(
            schema_receipt.wait().await,
            TestResultDeliveryDisposition::Completed
        );
        assert_eq!(
            producer.cancellation_reason(),
            Some(WorkCancellationReason::Requested)
        );
        producer.finish();
    }

    struct ChangingTopology {
        snapshot: Mutex<BackendTopologySnapshot>,
        changes: tokio::sync::watch::Sender<u64>,
    }

    impl ChangingTopology {
        fn empty() -> Self {
            let (changes, _) = tokio::sync::watch::channel(0);
            Self {
                snapshot: Mutex::new(BackendTopologySnapshot::empty(0)),
                changes,
            }
        }

        fn publish(&self, snapshot: BackendTopologySnapshot) {
            let revision = snapshot.revision();
            *self.snapshot.lock().expect("test topology lock") = snapshot;
            self.changes.send_replace(revision);
        }
    }

    impl novarocks_query_application::api::BackendTopologyPort for ChangingTopology {
        fn snapshot(
            &self,
        ) -> Result<BackendTopologySnapshot, novarocks_query_application::api::BackendTopologyError>
        {
            Ok(self.snapshot.lock().expect("test topology lock").clone())
        }

        fn subscribe_changes(&self) -> tokio::sync::watch::Receiver<u64> {
            self.changes.subscribe()
        }

        fn validate_snapshot(
            &self,
            expected: &BackendTopologySnapshot,
        ) -> Result<(), novarocks_query_application::api::BackendTopologyValidationError> {
            if &self.snapshot().map_err(
                novarocks_query_application::api::BackendTopologyValidationError::Unavailable,
            )? == expected
            {
                Ok(())
            } else {
                Err(novarocks_query_application::api::BackendTopologyValidationError::ContentChangedWithoutRevision {
                    revision: expected.revision(),
                })
            }
        }

        fn wait_for_eligible_after(
            &self,
            _revision: u64,
            _deadline: Instant,
        ) -> Result<BackendTopologySnapshot, novarocks_query_application::api::BackendTopologyError>
        {
            unreachable!("governed planning uses the event-driven subscription")
        }

        fn record_successful_stage(&self, _backend_idx: usize, _fragment_count: usize) {}
    }

    fn eligible_topology(revision: u64) -> BackendTopologySnapshot {
        use novarocks_execution::task_execution::AdmissionEpochCapability;
        use novarocks_execution_contract::{BackendProcessDescriptor, RuntimeEndpoint};

        let descriptor = BackendProcessDescriptor::try_new(
            novarocks_types::BackendProcessId::new_v7(),
            RuntimeEndpoint::new("127.0.0.1", 9030).expect("test endpoint"),
            "test-deployment",
            "test-build",
            novarocks_types::NativeCompatibilityId::new([0x71; 32]),
        )
        .expect("test descriptor");
        BackendTopologySnapshot::try_new(
            revision,
            vec![novarocks_query_application::api::LiveBackendTarget::new(
                0,
                descriptor,
                AdmissionEpochCapability::try_from_bytes([0x61; 16]).expect("test admission epoch"),
            )],
        )
        .expect("test eligible topology")
    }

    fn test_governed_cancellation() -> (
        novarocks_workload_control::WorkloadControl,
        novarocks_workload_control::RootWork,
        QueryCancellationView,
    ) {
        let workload = novarocks_workload_control::WorkloadControl::try_new(
            novarocks_workload_control::WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024 * 1024,
                control_bytes: 1024,
                per_scope_bytes: 1024 * 1024 - 1024,
            },
        )
        .expect("test workload");
        workload.mark_ready().expect("test workload ready");
        let root = workload
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .expect("test root");
        let cancellation = QueryCancellationView::governed(
            root.owner
                .scope()
                .cancellation()
                .expect("test cancellation"),
            None,
        );
        (workload, root, cancellation)
    }

    #[tokio::test]
    async fn initial_query_topology_wait_reloads_after_revision_event() {
        let topology = Arc::new(ChangingTopology::empty());
        let service: BackendTopologyService = topology.clone();
        let (_workload, root, cancellation) = test_governed_cancellation();
        let published = eligible_topology(1);
        let publish = async {
            tokio::task::yield_now().await;
            topology.publish(published);
        };

        let (observed, ()) = tokio::join!(
            wait_for_initial_query_topology(&service, &cancellation, None),
            publish
        );
        assert_eq!(observed.expect("topology becomes eligible").revision(), 1);
        root.owner.complete();
        root.business.release();
    }

    #[tokio::test]
    async fn initial_query_topology_wait_observes_statement_cancellation() {
        let topology = Arc::new(ChangingTopology::empty());
        topology.publish(eligible_topology(1));
        let service: BackendTopologyService = topology;
        let (_workload, root, cancellation) = test_governed_cancellation();
        root.owner.cancel(WorkCancellationReason::ExplicitKill {
            requester_connection_id: 91,
        });

        let error = wait_for_initial_query_topology(&service, &cancellation, None)
            .await
            .expect_err("cancelled statement cannot keep waiting for topology");
        assert_eq!(error.kind(), QueryServiceErrorKind::Interrupted);
        root.owner.complete();
        root.business.release();
    }

    #[tokio::test]
    async fn replacement_topology_wait_requires_a_new_eligible_revision() {
        let topology = Arc::new(ChangingTopology::empty());
        topology.publish(eligible_topology(1));
        let service: BackendTopologyService = topology.clone();
        let (_workload, root, cancellation) = test_governed_cancellation();
        let publish = async {
            tokio::task::yield_now().await;
            topology.publish(eligible_topology(2));
        };

        let (observed, ()) = tokio::join!(
            wait_for_eligible_query_topology_after(
                &service,
                &cancellation,
                1,
                Instant::now() + Duration::from_secs(1),
            ),
            publish
        );
        assert_eq!(
            observed
                .expect("replacement topology becomes eligible")
                .revision(),
            2
        );
        root.owner.complete();
        root.business.release();
    }

    #[test]
    fn legacy_typed_route_rejects_plain_query_but_keeps_explain() {
        let plain = novarocks_parser::parse("SELECT 1").expect("plain query parses");
        let explain = novarocks_parser::parse("EXPLAIN ANALYZE SELECT 1")
            .expect("explain analyze query parses");

        assert_eq!(plain.len(), 1);
        let error = reject_plain_query_from_legacy_typed_route(&plain[0])
            .expect_err("plain query must use governed launcher");
        assert_eq!(error.kind(), QueryServiceErrorKind::Internal);
        assert_eq!(explain.len(), 1);
        reject_plain_query_from_legacy_typed_route(&explain[0])
            .expect("EXPLAIN remains on the legacy typed execution route");
    }

    #[derive(Default)]
    struct RecordingSpecializedRoute {
        calls: AtomicUsize,
        #[allow(
            dead_code,
            reason = "The fixture retains full execution contexts for targeted router assertions."
        )]
        contexts: Mutex<Vec<QueryExecutionContext>>,
    }

    #[derive(Default)]
    struct RecordingDeleteEngine {
        executions: Mutex<Vec<QueryExecutionContext>>,
        native_encoding_requests: AtomicUsize,
    }

    struct RejectingMutationEngine;

    #[derive(Default)]
    struct RecordingMutationEngine {
        preparations: AtomicUsize,
        native_stage_requests: AtomicUsize,
    }

    struct TestMutationPrepared;

    impl MutationPrepared for TestMutationPrepared {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    impl MutationEngine for RejectingMutationEngine {
        fn prepare_mutation(
            &self,
            _request: PrepareMutationRequest<'_>,
        ) -> Result<PreparedMutation, String> {
            Err("mutation validation failed".to_string())
        }

        fn stage_mutation(
            &self,
            _prepared: &dyn MutationPrepared,
        ) -> Result<MutationStageOutcome, crate::dml::error::DmlExecutionError> {
            unreachable!("rejected mutation must not stage")
        }

        fn finalize_mutation(&self, _prepared: &dyn MutationPrepared) -> Result<(), String> {
            unreachable!("rejected mutation must not finalize")
        }
    }

    impl MutationEngine for RecordingMutationEngine {
        fn prepare_mutation(
            &self,
            request: PrepareMutationRequest<'_>,
        ) -> Result<PreparedMutation, String> {
            self.preparations.fetch_add(1, Ordering::SeqCst);
            Ok(PreparedMutation {
                operation: MutationOperation {
                    publication_id: request.publication_id,
                    kind: MutationStatementKind::Update,
                    catalog: "ice".to_string(),
                    namespace: "db".to_string(),
                    table: "t".to_string(),
                    target_ref: "main".to_string(),
                    attempt_id: "test-mutation".to_string(),
                    base_snapshot_id: None,
                },
                handle: Arc::new(TestMutationPrepared),
                sql_source: request.source.to_string(),
            })
        }

        fn stage_mutation(
            &self,
            _prepared: &dyn MutationPrepared,
        ) -> Result<MutationStageOutcome, crate::dml::error::DmlExecutionError> {
            self.native_stage_requests.fetch_add(1, Ordering::SeqCst);
            Err(crate::dml::error::DmlExecutionError::from(
                "recording mutation stops at the native dispatch edge".to_string(),
            ))
        }

        fn finalize_mutation(&self, _prepared: &dyn MutationPrepared) -> Result<(), String> {
            unreachable!("recording mutation never reaches finalization")
        }
    }

    struct TestDeletePrepared;

    impl DeletePrepared for TestDeletePrepared {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    impl DeleteEngine for RecordingDeleteEngine {
        fn prepare_delete(
            &self,
            request: PrepareDeleteRequest<'_>,
        ) -> Result<PreparedDelete, String> {
            self.executions
                .lock()
                .expect("delete executions")
                .push(request.execution);
            Ok(PreparedDelete {
                operation: DeleteOperation {
                    publication_id: request.publication_id,
                    catalog: "ice".to_string(),
                    namespace: "db".to_string(),
                    table: "t".to_string(),
                    target_ref: "main".to_string(),
                    attempt_id: "test-delete".to_string(),
                    base_snapshot_id: None,
                },
                handle: Arc::new(TestDeletePrepared),
                sql_source: request.source.to_string(),
            })
        }

        fn run_delete(&self, _prepared: &dyn DeletePrepared) -> Result<DeleteWriteReport, String> {
            Ok(DeleteWriteReport::NoOp)
        }

        fn delete_native_encoding<'a>(
            &self,
            _prepared: &'a dyn DeletePrepared,
        ) -> Result<
            crate::query_execution::dml::delete::DeleteNativeEncoding<'a>,
            crate::dml::error::DmlExecutionError,
        > {
            self.native_encoding_requests.fetch_add(1, Ordering::SeqCst);
            Err(crate::dml::error::DmlExecutionError::from(
                "recording DELETE stops at the native dispatch edge".to_string(),
            ))
        }

        fn finalize_delete(&self, _prepared: &dyn DeletePrepared) -> Result<(), String> {
            unreachable!("no-op DELETE must not finalize")
        }
    }

    impl SpecializedStatementRoute for RecordingSpecializedRoute {}

    #[derive(Default)]
    struct RecordingInsertEngine {
        resolve_contexts: Mutex<Vec<QueryExecutionContext>>,
    }

    impl InsertEngine for RecordingInsertEngine {
        fn resolve_target(
            &self,
            request: ResolveInsertTarget,
        ) -> Result<ResolvedInsertTarget, String> {
            self.resolve_contexts
                .lock()
                .expect("resolve contexts")
                .push(request.execution);
            Ok(ResolvedInsertTarget {
                catalog: "ice".to_string(),
                namespace: "db".to_string(),
                table: "t".to_string(),
                columns: vec![ColumnDef {
                    name: "a".to_string(),
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                }],
                planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease::new(
                    Arc::new(novarocks_catalog_application::test_support::test_control_binding(1)),
                    || {},
                ),
                attempt_reservation: None,
            })
        }

        fn prepare_iceberg_write(
            &self,
            _request: PrepareIcebergInsert,
        ) -> Result<PreparedIcebergInsert, String> {
            Err("unexpected Iceberg INSERT".to_string())
        }

        fn run_iceberg_write(
            &self,
            _prepared: &dyn IcebergPreparedInsert,
        ) -> Result<IcebergWriteReport, String> {
            Err("unexpected Iceberg INSERT".to_string())
        }

        fn finalize_iceberg_write(
            &self,
            _prepared: &dyn IcebergPreparedInsert,
        ) -> Result<(), String> {
            Err("unexpected Iceberg INSERT".to_string())
        }
    }

    fn router_test_context(
        topology_revision: u64,
        deadline: Instant,
        cancellation: &QueryCancellationSource,
    ) -> RequestContext {
        RequestContext::admit(RequestAdmission::new(
            None,
            "db".to_string(),
            ClusterRole::Fe,
            BackendTopologySnapshot::empty(topology_revision),
            Some(deadline),
            cancellation.view(),
            SessionOptimizerSettings::default(),
        ))
    }

    /// Test-only router fixture. Production routing is the typed branch in
    /// `execute_typed_statement`; this helper keeps focused unit tests independent of
    /// a running frontend session.
    #[allow(clippy::too_many_arguments)]
    fn execute_frontend_command<C>(
        dml: &DmlService,
        insert_engine: &dyn InsertEngine,
        delete_engine: &dyn DeleteEngine,
        mutation_engine: Option<&dyn MutationEngine>,
        ctas_route: C,
        _command: &dyn SpecializedStatementRoute,
        sql: &str,
        context: &RequestContext,
        query_options: QueryOptions,
    ) -> Result<StatementResult, String>
    where
        C: FnOnce(&str, &RequestContext, &QueryOptions) -> Result<Option<()>, crate::dml::DmlError>,
    {
        use novarocks_parser::ast::{DmlStatement, Statement};

        let parsed = novarocks_parser::parse(sql).map_err(|error| error.to_string())?;
        match parsed.as_slice() {
            [Statement::Dml(DmlStatement::Insert(statement))] => dml
                .try_execute_insert(insert_engine, statement, sql, context, Some(&query_options))
                .map(|()| StatementResult::Ok)
                .map_err(|error| error.to_string()),
            [Statement::Dml(DmlStatement::Delete(statement))] => dml
                .prepare_delete(
                    delete_engine,
                    crate::query_execution::dml::delete::DeleteStatement::Predicate(statement),
                    sql,
                    context,
                    Some(&query_options),
                )
                .and_then(|prepared| dml.execute_prepared_delete(delete_engine, prepared))
                .map(|()| StatementResult::Ok)
                .map_err(|error| error.to_string()),
            [Statement::Dml(DmlStatement::Update(_) | DmlStatement::Merge(_))] => mutation_engine
                .ok_or_else(|| "mutation engine is unavailable".to_string())
                .and_then(|engine| {
                    dml.prepare_typed_mutation(
                        engine,
                        match &parsed[0] {
                            Statement::Dml(statement) => statement,
                            _ => unreachable!(),
                        },
                        sql,
                        context,
                        Some(&query_options),
                    )
                    .and_then(|prepared| dml.execute_prepared_mutation(engine, prepared))
                    .map(|()| StatementResult::Ok)
                    .map_err(|error| error.to_string())
                }),
            [Statement::Dml(DmlStatement::CreateTableAsSelect(_))] => {
                ctas_route(sql, context, &query_options)
                    .map_err(|error| error.to_string())?
                    .map_or_else(
                        || Err("test router has no typed owner for this statement".to_string()),
                        |_| Ok(StatementResult::Ok),
                    )
            }
            _ => Err("test router has no typed owner for this statement".to_string()),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "The test seam preserves the production DML error type."
    )]
    fn not_ctas(
        _sql: &str,
        _context: &RequestContext,
        _query_options: &QueryOptions,
    ) -> Result<Option<()>, crate::dml::DmlError> {
        Ok(None)
    }

    #[test]
    fn sqlx2_application_frontend_router_handles_insert_before_core_command() {
        let engine = RecordingInsertEngine::default();
        let delete_engine = RecordingDeleteEngine::default();
        let command = RecordingSpecializedRoute::default();
        let dml = DmlService::new();
        let cancellation = QueryCancellationSource::new();
        let context =
            router_test_context(41, Instant::now() + Duration::from_secs(30), &cancellation);

        let error = execute_frontend_command(
            &dml,
            &engine,
            &delete_engine,
            None,
            not_ctas,
            &command,
            "INSERT INTO t VALUES (1)",
            &context,
            default_query_options(),
        )
        .expect_err("recording engine rejects the routed INSERT");
        assert!(!error.contains("state store is required"));
        assert_eq!(engine.resolve_contexts.lock().unwrap().len(), 1);
        assert_eq!(command.calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn sqlx2_application_frontend_router_passes_one_request_context_to_dml() {
        let engine = RecordingInsertEngine::default();
        let delete_engine = RecordingDeleteEngine::default();
        let command = RecordingSpecializedRoute::default();
        let dml = DmlService::new();
        let cancellation = QueryCancellationSource::new();
        let deadline = Instant::now() + Duration::from_secs(30);
        let context = router_test_context(73, deadline, &cancellation);

        let error = execute_frontend_command(
            &dml,
            &engine,
            &delete_engine,
            None,
            not_ctas,
            &command,
            "INSERT INTO t VALUES (1)",
            &context,
            default_query_options(),
        )
        .expect_err("recording engine rejects the routed INSERT");
        assert!(!error.contains("state store is required"));

        let resolve = engine.resolve_contexts.lock().unwrap();
        assert_eq!(resolve[0].topology().revision(), 73);
        assert_eq!(resolve[0].deadline(), Some(deadline));
        cancellation.request(QueryCancellationReason::ExplicitKill {
            requester_connection_id: 9,
        });
        assert!(resolve[0].cancellation().is_cancelled());
    }

    #[test]
    fn sqlx2_application_frontend_router_handles_delete_before_core_command() {
        let engine = RecordingInsertEngine::default();
        let delete_engine = RecordingDeleteEngine::default();
        let command = RecordingSpecializedRoute::default();
        let dml = DmlService::new();
        let cancellation = QueryCancellationSource::new();
        let deadline = Instant::now() + Duration::from_secs(30);
        let context = router_test_context(88, deadline, &cancellation);

        let error = execute_frontend_command(
            &dml,
            &engine,
            &delete_engine,
            None,
            not_ctas,
            &command,
            "DELETE FROM t WHERE a = 1",
            &context,
            default_query_options(),
        )
        .expect_err("recording engine rejects the routed DELETE");
        assert!(!error.contains("coordination"));

        let executions = delete_engine.executions.lock().unwrap();
        assert_eq!(executions.len(), 1);
        assert_eq!(executions[0].topology().revision(), 88);
        assert_eq!(executions[0].deadline(), Some(deadline));
        assert_eq!(command.calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn prepared_delete_can_drop_before_native_dispatch() {
        let engine = RecordingDeleteEngine::default();
        let dml = DmlService::new();
        let cancellation = QueryCancellationSource::new();
        let context =
            router_test_context(89, Instant::now() + Duration::from_secs(30), &cancellation);
        let parsed = novarocks_parser::parse("DELETE FROM t WHERE a = 1").expect("DELETE parses");
        let statement = match &parsed[0] {
            novarocks_parser::ast::Statement::Dml(novarocks_parser::ast::DmlStatement::Delete(
                statement,
            )) => statement,
            _ => panic!("expected typed DELETE"),
        };

        let prepared = dml
            .prepare_delete(
                &engine,
                crate::query_execution::dml::delete::DeleteStatement::Predicate(statement),
                "DELETE FROM t WHERE a = 1",
                &context,
                Some(&default_query_options()),
            )
            .expect("preparation is inert");
        assert_eq!(engine.executions.lock().unwrap().len(), 1);
        assert_eq!(engine.native_encoding_requests.load(Ordering::SeqCst), 0);

        drop(prepared);
        assert_eq!(engine.native_encoding_requests.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn prepared_mutation_can_drop_before_native_dispatch() {
        let engine = RecordingMutationEngine::default();
        let dml = DmlService::new();
        let cancellation = QueryCancellationSource::new();
        let context =
            router_test_context(90, Instant::now() + Duration::from_secs(30), &cancellation);
        let parsed =
            novarocks_parser::parse("UPDATE t SET a = 1 WHERE a = 0").expect("UPDATE parses");
        let statement = match &parsed[0] {
            novarocks_parser::ast::Statement::Dml(
                statement @ novarocks_parser::ast::DmlStatement::Update(_),
            ) => statement,
            _ => panic!("expected typed UPDATE"),
        };

        let prepared = dml
            .prepare_typed_mutation(
                &engine,
                statement,
                "UPDATE t SET a = 1 WHERE a = 0",
                &context,
                Some(&default_query_options()),
            )
            .expect("preparation is inert");
        assert_eq!(engine.preparations.load(Ordering::SeqCst), 1);
        assert_eq!(engine.native_stage_requests.load(Ordering::SeqCst), 0);

        drop(prepared);
        assert_eq!(engine.native_stage_requests.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn frontend_router_orders_ctas_before_truncate_and_fallback() {
        let insert = RecordingInsertEngine::default();
        let delete = RecordingDeleteEngine::default();
        let command = RecordingSpecializedRoute::default();
        let dml = DmlService::new();
        let cancellation = QueryCancellationSource::new();
        let context =
            router_test_context(92, Instant::now() + Duration::from_secs(30), &cancellation);
        let ctas_calls = AtomicUsize::new(0);

        execute_frontend_command(
            &dml,
            &insert,
            &delete,
            None,
            |_, _, _| {
                ctas_calls.fetch_add(1, Ordering::SeqCst);
                Ok(Some(()))
            },
            &command,
            "CREATE TABLE ice.db.dst AS SELECT 1",
            &context,
            default_query_options(),
        )
        .expect("frontend CTAS route");

        assert_eq!(ctas_calls.load(Ordering::SeqCst), 1);
        assert_eq!(command.calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn frontend_router_target_errors_never_fall_back() {
        let insert = RecordingInsertEngine::default();
        let delete = RecordingDeleteEngine::default();
        let command = RecordingSpecializedRoute::default();
        let dml = DmlService::new();
        let cancellation = QueryCancellationSource::new();
        let context =
            router_test_context(93, Instant::now() + Duration::from_secs(30), &cancellation);
        let error = execute_frontend_command(
            &dml,
            &insert,
            &delete,
            None,
            |_, _, _| Err(crate::dml::DmlError::executor("CTAS failed")),
            &command,
            "CREATE TABLE ice.db.dst AS SELECT 1",
            &context,
            default_query_options(),
        )
        .unwrap_err();

        assert!(error.contains("CTAS failed"));
        assert_eq!(command.calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn frontend_router_recognized_mutations_never_fall_back_to_core() {
        let insert = RecordingInsertEngine::default();
        let delete = RecordingDeleteEngine::default();
        let mutation = RejectingMutationEngine;
        let command = RecordingSpecializedRoute::default();
        let dml = DmlService::new();
        let cancellation = QueryCancellationSource::new();
        let context =
            router_test_context(95, Instant::now() + Duration::from_secs(30), &cancellation);

        for sql in [
            "UPDATE t SET k = 1",
            "MERGE INTO t USING s ON t.k = s.k WHEN MATCHED THEN UPDATE SET k = s.k",
            "UPDATE information_schema.be_configs SET value = '0'",
        ] {
            let error = execute_frontend_command(
                &dml,
                &insert,
                &delete,
                Some(&mutation),
                not_ctas,
                &command,
                sql,
                &context,
                default_query_options(),
            )
            .expect_err("recognized mutation must terminate frontend route");
            assert!(
                error.contains("mutation validation failed"),
                "error={error}"
            );
        }
        assert_eq!(command.calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn batch_split_preserves_quoted_semicolons_and_statement_order() {
        let statements = split_sql_statements("SET query_timeout = 1; SELECT ';'; SELECT 3")
            .expect("batch must parse");
        assert_eq!(
            statements,
            vec![
                "SET query_timeout = 1".to_string(),
                "SELECT ';'".to_string(),
                "SELECT 3".to_string(),
            ]
        );
    }

    #[test]
    fn batch_split_ignores_semicolons_inside_leading_comments() {
        let statements = split_sql_statements(
            "SET query_timeout=120;\n-- license; users may obtain a copy\nCREATE CATALOG c;",
        )
        .expect("batch must parse");
        assert_eq!(statements.len(), 2);
        assert_eq!(statements[0], "SET query_timeout=120");
        assert!(statements[1].starts_with("-- license;"));
    }

    #[test]
    fn batch_split_rejects_unterminated_quote() {
        let error = split_sql_statements("SELECT 'unterminated").expect_err("must reject");
        assert_eq!(error.kind(), QueryServiceErrorKind::Parse);
    }

    #[test]
    fn leading_line_comments_preserve_the_following_statement() {
        let sql = "-- Licensed under the Apache License\n# suite header\nCREATE CATALOG c";
        assert_eq!(strip_leading_line_comments(sql), "CREATE CATALOG c");
        assert_eq!(strip_leading_line_comments("-- comment only"), "");
    }

    #[test]
    fn cancellation_errors_keep_timeout_distinct_from_interrupts() {
        assert_eq!(
            cancellation_error(QueryCancellationReason::DeadlineExceeded { timeout_ms: 25 }).kind(),
            QueryServiceErrorKind::Timeout
        );
        assert_eq!(
            cancellation_error(QueryCancellationReason::ClientDisconnected).kind(),
            QueryServiceErrorKind::Interrupted
        );
        assert_eq!(
            cancellation_error(QueryCancellationReason::ExplicitKillConnection {
                requester_connection_id: 8
            })
            .kind(),
            QueryServiceErrorKind::Interrupted
        );
    }

    #[test]
    fn preparation_cancellation_errors_retain_the_unstarted_read_boundary() {
        let cancellation =
            novarocks_query_application::cancellation::QueryCancellationSource::new().view();
        assert!(matches!(
            governed_preparation_error(
                QueryServiceError::new(QueryServiceErrorKind::Timeout, "deadline elapsed"),
                &cancellation,
            ),
            GovernedPreparationError::Cancelled(_)
        ));
        assert!(matches!(
            governed_preparation_error(
                QueryServiceError::new(QueryServiceErrorKind::Unavailable, "topology unavailable"),
                &cancellation,
            ),
            GovernedPreparationError::Service(_)
        ));
    }

    #[test]
    fn only_kill_query_fences_the_successor_statement() {
        assert!(cancellation_requires_statement_fence(
            &QueryCancellationReason::ExplicitKill {
                requester_connection_id: 8,
            }
        ));
        for reason in [
            QueryCancellationReason::ExplicitKillConnection {
                requester_connection_id: 8,
            },
            QueryCancellationReason::ClientDisconnected,
            QueryCancellationReason::FrontendDrainDeadlineExceeded { timeout_ms: 10 },
            QueryCancellationReason::ServerShutdown,
        ] {
            assert!(!cancellation_requires_statement_fence(&reason));
        }
    }

    #[test]
    fn draining_admission_uses_the_fixed_retryable_error() {
        let error = query_service_admission_error(FrontendAdmissionError::Draining);
        assert_eq!(error.kind(), QueryServiceErrorKind::FrontendDraining);
        assert_eq!(
            error.message(),
            QueryServiceError::FRONTEND_DRAINING_MESSAGE
        );
    }

    #[test]
    fn batch_cursor_does_not_scan_later_fragments_before_their_turn() {
        let mut cursor = SqlBatchCursor::new("SELECT 1; SELECT 'unterminated");
        assert_eq!(
            cursor.next_fragment().expect("first fragment"),
            Some("SELECT 1")
        );
        assert!(cursor.has_remaining());
        let error = cursor
            .next_fragment()
            .expect_err("later malformed fragment remains deferred");
        assert_eq!(error.kind(), QueryServiceErrorKind::Parse);
    }

    #[test]
    fn timeout_message_rounds_a_sampled_deadline_up_to_milliseconds() {
        assert_eq!(
            timeout_message_millis(Duration::from_nanos(999_999_999)),
            1_000
        );
        assert_eq!(timeout_message_millis(Duration::from_millis(1_000)), 1_000);
    }

    #[test]
    fn sql_batch_splits_on_statement_boundaries() {
        let statements = split_sql_statements(
            "DROP DATABASE IF EXISTS db1 FORCE;\
             CREATE DATABASE db1;\
             USE db1;\
             CREATE TABLE tbl (id int, name string);\
             INSERT INTO tbl VALUES (1, 'a'), (2, 'b');\
             SELECT name FROM tbl WHERE id = 2;",
        )
        .expect("split a well-formed batch");

        assert_eq!(statements.len(), 6);
        assert_eq!(statements[0], "DROP DATABASE IF EXISTS db1 FORCE");
        assert_eq!(statements[5], "SELECT name FROM tbl WHERE id = 2");
    }

    #[test]
    fn sql_batch_ignores_semicolons_inside_quotes_and_comments() {
        // A `;` inside a literal, identifier or comment must not split the
        // batch. Getting this wrong silently truncates a statement instead of
        // failing, so each quoting form is pinned separately.
        let cases = [
            "INSERT INTO t VALUES ('a;b')",
            "INSERT INTO t VALUES (\"a;b\")",
            "SELECT `weird;column` FROM t",
            "SELECT 1 -- trailing ; comment\n",
            "SELECT 1 # trailing ; comment\n",
            "SELECT /* inline ; comment */ 1",
        ];
        for sql in cases {
            let statements =
                split_sql_statements(sql).unwrap_or_else(|error| panic!("split {sql:?}: {error}"));
            assert_eq!(statements.len(), 1, "{sql:?} must stay one statement");
        }
    }

    #[test]
    fn sql_batch_drops_empty_fragments_and_keeps_an_unterminated_tail() {
        assert!(
            split_sql_statements("").expect("empty batch").is_empty(),
            "an empty batch has no statements"
        );
        assert!(
            split_sql_statements(" ;; ; ")
                .expect("separator-only batch")
                .is_empty(),
            "separators alone contribute no statements"
        );

        let statements =
            split_sql_statements("SELECT 1;;SELECT 2").expect("split around an empty fragment");
        assert_eq!(statements, vec!["SELECT 1", "SELECT 2"]);
    }

    #[test]
    fn sql_batch_rejects_an_unterminated_quote() {
        let error = split_sql_statements("SELECT 'unterminated")
            .expect_err("an unterminated literal must fail closed");
        assert_eq!(error.kind(), QueryServiceErrorKind::Parse);
        assert!(
            error.to_string().contains("unterminated quoted string"),
            "unexpected error: {error}"
        );
    }
}
