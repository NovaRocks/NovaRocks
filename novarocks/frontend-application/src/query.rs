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
use crate::query::compiler::{
    FrontendQueryCompiler, FrontendQueryCompilerError, FrontendQueryPurpose,
};
use crate::query_execution::completion::PreparedQueryOperation;
use crate::query_execution::dml::add_files::AddFilesEngine;
use crate::query_execution::dml::ctas::CtasEngine;
use crate::query_execution::dml::delete::DeleteEngine;
use crate::query_execution::dml::insert::InsertEngine;
use crate::query_execution::dml::mutation::MutationEngine;
use crate::query_execution::dml::truncate::TruncateEngine;
use crate::query_execution::logical_execution::LogicalExecutionLauncher;
use crate::query_execution::maintenance::command::{
    MaintenanceCommandExecutor, MaintenanceReadCommandExecutor,
};
use crate::statistics::command::StatisticsCommandExecutor;
use crate::view::command::ViewCommandExecutor;
use async_trait::async_trait;
use novarocks_parser::{
    ast::{self, Statement as ParsedStatement},
    printer::print_expr,
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
use novarocks_query_application::api::{QueryResult, build_string_query_result};
use novarocks_query_application::cancellation::{QueryCancellationReason, QueryCancellationView};
use novarocks_query_application::client_connection::ClientConnectionControlPort;
use novarocks_query_application::cpu::QueryBlockingExecutor;
#[cfg(test)]
use novarocks_query_application::cpu::QueryCpuRunError;
use novarocks_query_application::protocol_delivery::{
    GovernedCompletionStatementResult, GovernedErrorStatementResult,
    GovernedImmediateStatementResult, QuerySessionOutput as StatementResult,
};
use novarocks_query_application::publication::LakePublicationRuntimePolicy;
use novarocks_query_application::serving_admission::{
    FrontendAdmissionError, FrontendServingAdmission,
};
use novarocks_query_application::session::{
    QuerySession, QuerySessionFactory, QuerySessionOpenRequest, StatementSessionRequest,
};
use novarocks_query_application::session_control::{
    GovernedQueryStatementBeginError, GovernedStatementAction, QueryControlService,
    QuerySessionLease, SessionIdentity, SessionToken, StatementToken,
};
use novarocks_query_application::session_effects::SessionEffects;
use novarocks_query_application::session_error::{QueryServiceError, QueryServiceErrorKind};
use novarocks_query_application::session_outcome::{
    cancellation_error, cancellation_requires_statement_fence, governed_cancellation_error,
    governed_query_deadline, governed_query_execution_error, governed_statement_begin_error,
    scalar_query_error,
};
use novarocks_query_application::session_value::StatementSessionError;
use novarocks_query_application::sql::admission::{
    admin_raise_engine_error, requires_lake_publication_deadline, typed_statement_work_class,
    unnegotiated_query_statement,
};
use novarocks_query_application::sql::catalog::SessionCatalogService;
use novarocks_query_application::sql::dml_admission::validate_table_statement_admission;
use novarocks_query_application::sql::kill::execute_control_session_statement;
use novarocks_query_application::sql::session::{
    SessionExecutionSettings, SessionSqlState,
    admit_session_set_assignment as admit_query_application_session_set_assignment,
};
use novarocks_query_application::sql::session_admit::SessionAdmitError;
use novarocks_query_application::sql::{
    ProductCommandRouter, ProductSqlCommand, lower_product_sql_command,
};
use novarocks_query_application::sql::{
    SpecializedStatementRoute, parse_single_statement, query_service_parse_error,
    strip_leading_line_comments,
};
use novarocks_query_application::statement_kind::StatementKind;
use novarocks_type_contract::CompileControlError;
use novarocks_types::ClusterRole;
use novarocks_types::naming::normalize_identifier;
use novarocks_user_error::UserError;
#[cfg(test)]
use novarocks_workload_control::WorkRequest;
use novarocks_workload_control::{
    ResultWindowAlias, ResultWindowClass, RootAdmissionHandle, WorkClass,
};
use novarocks_workload_control::{WorkError, WorkOwner};

pub(crate) mod compiler;
pub(crate) mod ctas_action;
pub(crate) mod delete_action;
mod explain_action;
pub(crate) mod insert_action;
mod maintenance_action;
pub(crate) mod mutation_action;
pub(crate) mod mv_action;
pub(crate) mod mv_read_action;
pub(crate) mod read_action;
mod set_action;
mod statement_action;
pub(crate) mod statement_computation;
pub(crate) mod statement_publication;
pub(crate) mod view_read_action;
pub(crate) mod write_action;

const DEFAULT_CATALOG: &str = "default_catalog";

fn command_error(kind: CommandErrorKind, error: impl Into<String>) -> CommandError {
    CommandError::new(kind, error.into())
}

fn execute_product_command_edge<F, E>(
    executor: QueryBlockingExecutor,
    request_context: RequestContext,
    command_context: CommandContext,
    call: F,
) -> CommandFuture
where
    F: FnOnce(&RequestContext, &CommandContext) -> Result<StatementResult, E> + Send + 'static,
    E: Into<CommandError> + 'static,
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
        executor
            .execute(move || {
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
                let window = command_context.result_window_alias();
                let result = call(&request_context, &command_context).map_err(Into::into);
                // The receipt retains this alias even if the awaiting future
                // disappears while the blocking worker is still producing.
                Ok((result, window))
            })
            .await
            .map_err(|error| command_error(CommandErrorKind::Failed, error))?
            .and_then(|(result, _window)| result)
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
        executor
            .execute(move || {
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
                let window = command_context.result_window_alias();
                let result = call(&request_context, &command_context)
                    .map_err(|error| command_error(CommandErrorKind::Failed, error));
                // The receipt retains this alias even if the awaiting future
                // disappears while the blocking worker is still producing.
                Ok((result, window))
            })
            .await
            .map_err(|error| command_error(CommandErrorKind::Failed, error))?
            .and_then(|(result, _window)| result)
    })
}

#[derive(Clone)]
struct FrontendCatalogCommandConsumer {
    catalog: CatalogCommandExecutor,
    state_store: Option<crate::state_store::execution::StateStoreOriginalBinding>,
    view: ViewCommandExecutor,
    iceberg_ref: IcebergRefCommandExecutor,
    executor: QueryBlockingExecutor,
}

impl CatalogCommandConsumer for FrontendCatalogCommandConsumer {
    fn prepare_statement_action(
        &self,
        command: novarocks_sql::semantic::CatalogSqlCommand,
        _request_context: RequestContext,
        command_context: CommandContext,
    ) -> Result<
        Box<dyn novarocks_query_application::session_control::GovernedStatementAction>,
        CommandError,
    > {
        crate::catalog_application::statement_action::prepare(
            &self.catalog,
            self.state_store.clone(),
            command,
            command_context,
        )
        .map_err(CommandError::from_failure_cause)
    }

    fn execute(
        &self,
        command: novarocks_sql::semantic::CatalogSqlCommand,
        request_context: RequestContext,
        command_context: CommandContext,
    ) -> CommandFuture {
        if matches!(
            command,
            novarocks_sql::semantic::CatalogSqlCommand::CreateCatalog(_)
                | novarocks_sql::semantic::CatalogSqlCommand::DropCatalog { .. }
        ) {
            return Box::pin(async {
                Err(CommandError::new(
                    CommandErrorKind::Conflict,
                    "catalog mutation requires its original registered statement source",
                ))
            });
        }
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
}

impl StatisticsCommandConsumer for FrontendStatisticsCommandConsumer {
    fn prepare_statement_action(
        &self,
        command: novarocks_sql::semantic::StatisticsSqlCommand,
        request_context: RequestContext,
        command_context: CommandContext,
    ) -> Result<
        Box<dyn novarocks_query_application::session_control::GovernedStatementAction>,
        CommandError,
    > {
        self.statistics
            .prepare_statement_action(
                &command,
                request_context.session().current_catalog(),
                request_context.session().current_database(),
                request_context.execution(),
                command_context.connector_context(),
                *request_context.session().presentation(),
            )
            .map_err(|e| CommandError::new(CommandErrorKind::Rejected, e))
    }

    fn execute(
        &self,
        command: novarocks_sql::semantic::StatisticsSqlCommand,
        request_context: RequestContext,
        command_context: CommandContext,
    ) -> CommandFuture {
        let statistics = self.statistics.clone();
        Box::pin(async move {
            if request_context.execution().cancellation().is_cancelled() {
                return Err(command_error(
                    CommandErrorKind::Cancelled,
                    "statistics command was cancelled before asynchronous admission",
                ));
            }
            command_context.scope().check().map_err(|error| {
                command_error(
                    CommandErrorKind::Cancelled,
                    format!("statistics command scope is no longer active: {error}"),
                )
            })?;
            let result = statistics
                .execute_command(
                    &command,
                    request_context.session().current_catalog(),
                    request_context.session().current_database(),
                    Some(request_context.execution()),
                )
                .await
                .map_err(|e| CommandError::new(CommandErrorKind::Rejected, e))?;
            // Keep the admitted command context through the borrowed operation.
            drop(command_context);
            Ok(result)
        })
    }
}

#[derive(Clone)]
struct FrontendMaintenanceCommandConsumer {
    maintenance: MaintenanceCommandExecutor,
    maintenance_read: MaintenanceReadCommandExecutor,
    executor: QueryBlockingExecutor,
    #[cfg(novarocks_dev_controls)]
    statement_observation:
        Option<std::sync::Weak<crate::preparation_diagnostics::StatementObservationOwner>>,
}

impl MaintenanceCommandConsumer for FrontendMaintenanceCommandConsumer {
    fn prepare_statement_action(
        &self,
        command: novarocks_sql::semantic::MaintenanceSqlCommand,
        request_context: RequestContext,
        command_context: CommandContext,
    ) -> Result<
        Box<dyn novarocks_query_application::session_control::GovernedStatementAction>,
        CommandError,
    > {
        #[cfg(novarocks_dev_controls)]
        let observation = self
            .statement_observation
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
            .and_then(|owner| owner.bound_statement(command_context.statement_token()));
        maintenance_action::prepare(
            &self.maintenance,
            &command,
            &request_context,
            &command_context,
            #[cfg(novarocks_dev_controls)]
            observation,
        )
    }

    fn execute(
        &self,
        command: novarocks_sql::semantic::MaintenanceSqlCommand,
        request_context: RequestContext,
        command_context: CommandContext,
    ) -> CommandFuture {
        if crate::query_execution::maintenance::command::is_async_metadata_command(
            &command,
            request_context.session().current_catalog(),
            request_context.session().current_database(),
        )
        .unwrap_or(false)
        {
            return Box::pin(async {
                Err(CommandError::new(
                    CommandErrorKind::Conflict,
                    "metadata maintenance requires the original registered statement source",
                ))
            });
        }
        let maintenance = self.maintenance.clone();
        let maintenance_read = self.maintenance_read.clone();
        execute_product_command_edge(
            self.executor.clone(),
            request_context,
            command_context,
            move |context, command_context| match command {
                novarocks_sql::semantic::MaintenanceSqlCommand::ShowOptimize(command) => {
                    maintenance_read
                        .execute_command(
                            &command,
                            context.session().current_catalog(),
                            context.session().current_database(),
                        )
                        .map_err(CommandError::from)
                }
                command => maintenance
                    .execute_non_metadata_command(
                        &command,
                        context.session().current_catalog(),
                        context.session().current_database(),
                        context.execution(),
                        command_context.connector_context(),
                    )
                    .map_err(maintenance_command_error),
            },
        )
    }
}

/// Project only the domain author's retained verdict into supervisor metadata.
/// The original Lake terminal, target, receipt, and cause remain domain-owned.
pub(crate) const fn external_effect_from_lake(
    disposition: novarocks_spi::connector::LakePublicationDisposition,
) -> novarocks_query_application::api::StatementExternalEffectDisposition {
    use novarocks_query_application::api::StatementExternalEffectDisposition as E;
    use novarocks_spi::connector::LakePublicationDisposition as D;
    match disposition {
        D::KnownUncommitted => E::KnownUncommitted,
        D::CommitUnknown => E::CommitUnknown,
        D::KnownCommitted => E::KnownCommitted,
    }
}

fn maintenance_command_error(
    error: novarocks_table_maintenance::runtime::TerminalError,
) -> CommandError {
    use novarocks_spi::connector::LakePublicationDisposition;
    use novarocks_table_maintenance::runtime::MaintenanceJobState;
    // Only the publication owner's original terminal state provides this fact.
    // A capability refusal cannot prove that a publication was uncommitted.
    let disposition = match error.state {
        MaintenanceJobState::KnownUncommitted => Some(LakePublicationDisposition::KnownUncommitted),
        MaintenanceJobState::CommitUnknown => Some(LakePublicationDisposition::CommitUnknown),
        MaintenanceJobState::KnownCommittedFinalizationFailed => {
            Some(LakePublicationDisposition::KnownCommitted)
        }
        _ => None,
    };
    let control = error.compile_control;
    let command = if let Some(cause) = error.failure_cause {
        CommandError::from_failure_cause(cause)
    } else if let Some(input) = error.execution_failure {
        CommandError::from_execution_failure(input)
    } else {
        match control.clone() {
            Some(control) => CommandError::from_compile_control(control),
            None => CommandError::from(error.message),
        }
    };
    command
        .with_compile_control(control)
        .with_external_effect_disposition(disposition.map(external_effect_from_lake))
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
    state_store: Option<crate::state_store::execution::StateStoreOriginalBinding>,
    #[cfg(novarocks_dev_controls)] statement_observation: Option<
        Arc<crate::preparation_diagnostics::StatementObservationOwner>,
    >,
) -> ProductCommandRouter {
    ProductCommandRouter::new(
        Arc::new(FrontendCatalogCommandConsumer {
            catalog,
            state_store,
            view,
            iceberg_ref,
            executor: executor.clone(),
        }),
        Arc::new(FrontendStatisticsCommandConsumer { statistics }),
        Arc::new(FrontendMaintenanceCommandConsumer {
            maintenance,
            maintenance_read,
            executor,
            #[cfg(novarocks_dev_controls)]
            statement_observation: statement_observation.as_ref().map(Arc::downgrade),
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
    sessions: QueryControlService,
}

impl TypedCommandRoute {
    fn new(
        backend: BackendCommandExecutor,
        view: ViewCommandExecutor,
        mv: Arc<dyn MaterializedViewCommandConsumer>,
        mv_call: MvCommandExecutor,
        executor: QueryBlockingExecutor,
        sessions: QueryControlService,
    ) -> Self {
        Self {
            backend,
            view,
            mv,
            mv_call,
            executor,
            sessions,
        }
    }
}

/// The MySQL `SHOW PROCESSLIST` column set, in MySQL's order.
///
/// `Host`, `db`, and `State` are always NULL: a client's host address belongs
/// to the MySQL listener, the current database is session-local SQL state, and
/// this engine has no per-statement state machine to name. None of the three
/// is registry state, so none is invented here.
fn process_list_columns() -> Vec<novarocks_query_application::api::ResultField> {
    use arrow::datatypes::DataType;
    use novarocks_query_application::api::ResultField;
    use novarocks_type_contract::FunctionValueType;
    [
        ("Id", DataType::Int64, false),
        ("User", DataType::Utf8, false),
        ("Host", DataType::Utf8, true),
        ("db", DataType::Utf8, true),
        ("Command", DataType::Utf8, false),
        ("Time", DataType::Int64, false),
        ("State", DataType::Utf8, true),
        ("Info", DataType::Utf8, true),
    ]
    .into_iter()
    .map(|(name, carrier, nullable)| {
        ResultField::new(name, FunctionValueType::new(carrier, nullable))
    })
    .collect()
}

/// MySQL truncates `Info` here unless the caller asked for `FULL`.
const PROCESS_LIST_INFO_LIMIT: usize = 100;

fn process_list_result(
    processes: Vec<novarocks_query_application::session_control::SessionProcess>,
    full: bool,
) -> Result<QueryResult, String> {
    use novarocks_query_application::api::LocalResultBound;

    // The snapshot shares each statement text with its session. Count the
    // whole result from it before building any array; FULL output can carry
    // every running statement's complete text.
    let info = processes
        .iter()
        .map(|process| {
            process.statement.as_deref().map(|text| {
                if full {
                    std::borrow::Cow::Borrowed(text)
                } else {
                    std::borrow::Cow::Owned(truncate_process_list_info(text))
                }
            })
        })
        .collect::<Vec<_>>();
    let bytes = processes
        .iter()
        .zip(&info)
        .try_fold(0_usize, |total, (process, info)| {
            [
                size_of::<i64>(),
                process.principal.len(),
                0,
                0,
                process.command().len(),
                size_of::<i64>(),
                0,
                info.as_deref().map_or(0, str::len),
            ]
            .into_iter()
            .try_fold(total, |total, cell| {
                total.checked_add(LocalResultBound::cell_bytes(cell))
            })
        })
        .unwrap_or(usize::MAX);
    LocalResultBound::V1.admit(processes.len(), bytes)?;

    let columns = process_list_columns();
    let schema = Arc::new(arrow::datatypes::Schema::new(
        columns
            .iter()
            .map(|field| {
                arrow::datatypes::Field::new(
                    field.name(),
                    field.data_type().clone(),
                    field.nullable(),
                )
            })
            .collect::<Vec<_>>(),
    ));
    let mut ids = arrow::array::Int64Builder::with_capacity(processes.len());
    let mut times = arrow::array::Int64Builder::with_capacity(processes.len());
    for process in &processes {
        ids.append_value(i64::from(process.connection_id));
        times.append_value(i64::try_from(process.elapsed.as_secs()).unwrap_or(i64::MAX));
    }
    let ids = ids.finish();
    let times = times.finish();
    // All requested values/offset/validity storage is covered by the borrowed
    // whole-table preflight above. Iterator constructors cannot infer string
    // bytes and may geometrically replace a nearly full values buffer.
    let users = process_list_text_column(
        processes
            .iter()
            .map(|process| Some(process.principal.as_ref())),
    )?;
    let hosts = process_list_text_column(std::iter::repeat_n(None, processes.len()))?;
    let databases = process_list_text_column(std::iter::repeat_n(None, processes.len()))?;
    let commands =
        process_list_text_column(processes.iter().map(|process| Some(process.command())))?;
    let states = process_list_text_column(std::iter::repeat_n(None, processes.len()))?;
    let info = process_list_text_column(info.iter().map(Option::as_deref))?;

    let batch = arrow::record_batch::RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(ids),
            Arc::new(users),
            Arc::new(hosts),
            Arc::new(databases),
            Arc::new(commands),
            Arc::new(times),
            Arc::new(states),
            Arc::new(info),
        ],
    )
    .map_err(|error| format!("build SHOW PROCESSLIST result: {error}"))?;

    Ok(QueryResult {
        columns,
        batches: vec![batch],
    })
}

/// Only the closed SHOW PROCESSLIST source uses this constructor. Count
/// borrowed cells first; no source alias is installed in the new Arrow graph.
fn process_list_text_column<'a>(
    values: impl ExactSizeIterator<Item = Option<&'a str>> + Clone,
) -> Result<arrow::array::StringArray, String> {
    let bytes = values.clone().flatten().try_fold(0usize, |sum, value| {
        sum.checked_add(value.len())
            .ok_or("SHOW PROCESSLIST text capacity overflows")
    })?;
    let mut builder = arrow::array::StringBuilder::with_capacity(values.len(), bytes);
    for value in values {
        builder.append_option(value);
    }
    Ok(builder.finish())
}

/// Truncates on a character boundary, so a multi-byte statement cannot be cut
/// into invalid UTF-8.
fn truncate_process_list_info(text: &str) -> String {
    if text.chars().count() <= PROCESS_LIST_INFO_LIMIT {
        return text.to_string();
    }
    text.chars().take(PROCESS_LIST_INFO_LIMIT).collect()
}

impl SpecializedStatementRoute for TypedCommandRoute {
    fn execute_show_process_list(
        &self,
        full: bool,
        _context: &RequestContext,
        command_context: &CommandContext,
    ) -> CommandFuture {
        // The registry snapshot is taken here, under its own lock, and the
        // rest is pure projection, so this route needs no blocking executor.
        let window = command_context.result_window_alias();
        let processes = self.sessions.list_processes();
        Box::pin(async move {
            let _window = window;
            process_list_result(processes, full)
                .map(StatementResult::Query)
                .map_err(|error| {
                    novarocks_query_application::api::CommandError::new(
                        novarocks_query_application::api::CommandErrorKind::Failed,
                        error,
                    )
                })
        })
    }

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
                    command_context.principal(),
                    command_context.connector_context(),
                )
            },
        )
    }

    fn prepare_materialized_view_statement_action(
        &self,
        statement: novarocks_parser::ast::MaterializedViewStatement,
        context: RequestContext,
        command_context: CommandContext,
    ) -> Result<Box<dyn GovernedStatementAction>, CommandError> {
        self.mv.prepare_statement_action(
            MaterializedViewCommand::new(statement),
            context,
            command_context,
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
    Application(QueryServiceError),
    Execution {
        input: Box<novarocks_query_application::execution_error::ExecutionFailureInput>,
        terminal: Option<novarocks_spi::connector::LakePublicationTerminal>,
        compile_control: Option<novarocks_type_contract::CompileControlError>,
        external_effect_disposition:
            Option<novarocks_query_application::api::StatementExternalEffectDisposition>,
    },
    ExternalEffect {
        message: String,
        disposition: novarocks_query_application::api::StatementExternalEffectDisposition,
        compile_control: Option<novarocks_type_contract::CompileControlError>,
    },
    Engine(String),
    Control(novarocks_type_contract::CompileControlError),
    User(UserError),
    Publication {
        message: String,
        terminal: novarocks_spi::connector::LakePublicationTerminal,
        compile_control: Option<novarocks_type_contract::CompileControlError>,
    },
}

fn routed_compiler_failure(error: FrontendQueryCompilerError, sql: &str) -> RoutedExecutionError {
    match error {
        FrontendQueryCompilerError::Engine(error) => RoutedExecutionError::Engine(error),
        FrontendQueryCompilerError::Application(error) => RoutedExecutionError::Application(error),
        FrontendQueryCompilerError::Control(error) => RoutedExecutionError::Control(error),
        FrontendQueryCompilerError::Source(source) => {
            RoutedExecutionError::Control(CompileControlError::Source(source))
        }
        FrontendQueryCompilerError::Analyze(error) => {
            RoutedExecutionError::User(error.to_user_error(Some(sql)))
        }
    }
}

/// The distributed coordinator owns domain, root-read and selected-input
/// projection. Keep its rich compile-control sidecar on this routed handoff.
fn routed_distributed_query_failure(
    error: crate::query_execution::contract::DistributedQueryError,
) -> RoutedExecutionError {
    let compile_control = error.compile_control_error();
    RoutedExecutionError::Execution {
        input: Box::new(error.into_execution_failure_input()),
        terminal: None,
        compile_control,
        external_effect_disposition: None,
    }
}

fn dml_result<T>(result: Result<T, crate::dml::DmlError>) -> Result<T, RoutedExecutionError> {
    result.map_err(|mut error| {
        if let Some(original) = error.application_error() {
            let original = match error.publication_terminal() {
                Some(terminal) => original
                    .clone()
                    .attach_publication_terminal(terminal.clone()),
                None => original.clone(),
            };
            return RoutedExecutionError::Application(original);
        }
        if let Some(input) = error.execution_failure().cloned() {
            return RoutedExecutionError::Execution {
                input: Box::new(input),
                terminal: error.publication_terminal().cloned(),
                compile_control: error.compile_control_error(),
                external_effect_disposition: None,
            };
        }
        if let Some(failure) = error.take_cow_failure() {
            // Both production COW callers reach this branch inside the
            // execute_prepared_dml_statement synchronous command closure.
            // Retire the original cause before publishing the worker receipt;
            // its original WorkOwner and result_window remain on that worker.
            let message = failure.retire_on_original_worker(error.publication_terminal());
            return match error.publication_terminal().cloned() {
                Some(terminal) => RoutedExecutionError::Publication {
                    message,
                    terminal,
                    compile_control: None,
                },
                None => RoutedExecutionError::Engine(message),
            };
        }
        if let Some(control) = error.compile_control_error() {
            if let Some(terminal) = error.publication_terminal().cloned() {
                return RoutedExecutionError::Publication {
                    message: error.to_string(),
                    terminal,
                    compile_control: Some(control),
                };
            }
            RoutedExecutionError::Control(control)
        } else if let Some(user_error) = error.user_error().cloned() {
            RoutedExecutionError::User(user_error)
        } else {
            let message = match error.engine_error_code() {
                Some(code) => format!("[{}] {error}", code.as_str()),
                None => error.to_string(),
            };
            match error.publication_terminal().cloned() {
                Some(terminal) => RoutedExecutionError::Publication {
                    message,
                    terminal,
                    compile_control: None,
                },
                None => RoutedExecutionError::Engine(message),
            }
        }
    })
}

fn routed_execution_service_error(error: RoutedExecutionError) -> QueryServiceError {
    match error {
        RoutedExecutionError::Application(error) => error,
        RoutedExecutionError::Execution {
            input,
            terminal,
            compile_control,
            external_effect_disposition,
        } => {
            let mut service = QueryServiceError::from_execution_failure(*input)
                .with_compile_control(compile_control);
            if let Some(disposition) = external_effect_disposition {
                service = service.attach_external_effect_disposition(disposition);
            }
            if let Some(terminal) = terminal {
                service = service.attach_publication_terminal(terminal);
            }
            service
        }
        RoutedExecutionError::ExternalEffect {
            message,
            disposition,
            compile_control,
        } => {
            let service = match compile_control.clone() {
                Some(control) => QueryServiceError::from_compile_control(control),
                None => internal_error(message),
            };
            service
                .with_compile_control(compile_control)
                .attach_external_effect_disposition(disposition)
        }
        RoutedExecutionError::Control(error) => QueryServiceError::from_compile_control(error),
        RoutedExecutionError::Engine(error) => internal_error(error),
        RoutedExecutionError::User(error) => QueryServiceError::from_user_error(error),
        RoutedExecutionError::Publication {
            message,
            terminal,
            compile_control,
        } => QueryServiceError::with_publication_terminal(message, terminal)
            .with_compile_control(compile_control),
    }
}

/// The worker may have selected an execution failure before cleanup outlived
/// the caller deadline. A local timeout cannot replace that accepted input.
/// Publication and rich control facts remain independent sidecars when the
/// original caller cancellation is selected as the fallback.
fn selected_execution_or_worker_fallback(
    result: Result<StatementResult, RoutedExecutionError>,
    fallback: QueryServiceError,
) -> QueryServiceError {
    match result {
        Err(
            error @ (RoutedExecutionError::Application(_) | RoutedExecutionError::Execution { .. }),
        ) => routed_execution_service_error(error),
        Err(RoutedExecutionError::ExternalEffect {
            disposition,
            compile_control,
            ..
        }) => fallback
            .with_compile_control(compile_control)
            .attach_external_effect_disposition(disposition),
        Err(RoutedExecutionError::Publication {
            terminal,
            compile_control,
            ..
        }) => fallback
            .with_compile_control(compile_control)
            .attach_publication_terminal(terminal),
        Err(RoutedExecutionError::Control(control)) => fallback.with_compile_control(Some(control)),
        _ => fallback,
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
    execution_owner: WorkOwner,
    result_window: Option<ResultWindowAlias>,
    call: F,
) -> Result<
    (
        Result<T, RoutedExecutionError>,
        WorkOwner,
        Option<ResultWindowAlias>,
    ),
    String,
>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, RoutedExecutionError> + Send + 'static,
{
    executor
        .execute(move || {
            let result = if cancellation.is_cancelled() {
                Err(RoutedExecutionError::Engine(
                    "typed statement was cancelled before synchronous command execution began"
                        .to_owned(),
                ))
            } else {
                call()
            };
            (result, execution_owner, result_window)
        })
        .await
}

/// Runs a DML plan and its external-effect dispatch as distinct governed
/// blocking edges. A successful plan is still pre-dispatch state, so the
/// second edge independently checks cancellation before it can cross the
/// publication boundary.
async fn execute_prepared_dml_statement<P, Prepare, Execute>(
    executor: QueryBlockingExecutor,
    cancellation: QueryCancellationView,
    execution_owner: WorkOwner,
    result_window: Option<ResultWindowAlias>,
    prepare: Prepare,
    execute: Execute,
) -> Result<(Result<StatementResult, RoutedExecutionError>, WorkOwner), String>
where
    P: Send + 'static,
    Prepare: FnOnce() -> Result<P, RoutedExecutionError> + Send + 'static,
    Execute: FnOnce(P) -> Result<StatementResult, RoutedExecutionError> + Send + 'static,
{
    let (prepared, execution_owner, result_window) = execute_synchronous_stage(
        executor.clone(),
        cancellation.clone(),
        execution_owner,
        result_window,
        prepare,
    )
    .await?;
    match prepared {
        Ok(prepared) => execute_synchronous_stage(
            executor,
            cancellation,
            execution_owner,
            result_window,
            move || execute(prepared),
        )
        .await
        .map(|(result, owner, _window)| (result, owner)),
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
        .map_err(product_command_error);
    Ok((result, execution_owner))
}

pub(crate) fn product_command_service_error(error: CommandError) -> QueryServiceError {
    routed_execution_service_error(product_command_error(error))
}

fn product_command_error(error: CommandError) -> RoutedExecutionError {
    let disposition = error.external_effect_disposition();
    if let Some(cause) = error.failure_cause() {
        // A selected cause and compiler control are independent facts. Keep
        // both in the complete service error across routing and late fallback.
        let mut service = QueryServiceError::from_failure_cause(cause.clone())
            .with_compile_control(error.compile_control_error());
        if let Some(disposition) = disposition {
            service = service.attach_external_effect_disposition(disposition);
        }
        return RoutedExecutionError::Application(service);
    }
    let control = error.compile_control_error();
    if let Some(original) = error.user_error() {
        let mut service =
            QueryServiceError::from_user_error(original.clone()).with_compile_control(control);
        if let Some(disposition) = disposition {
            service = service.attach_external_effect_disposition(disposition);
        }
        return RoutedExecutionError::Application(service);
    }
    let input = error.execution_failure().cloned().or_else(|| {
        // An original Source is already owner-selected, unlike a local caller
        // control fallback. Keep that existing projector and its same fact.
        match (&disposition, &control) {
            (Some(_), Some(control @ novarocks_type_contract::CompileControlError::Source(_))) => {
                QueryServiceError::from_compile_control(control.clone())
                    .execution_failure()
                    .cloned()
            }
            _ => None,
        }
    });
    if let Some(input) = input {
        return RoutedExecutionError::Execution {
            input: Box::new(input),
            terminal: None,
            compile_control: control,
            external_effect_disposition: disposition,
        };
    }
    if let Some(disposition) = disposition {
        return RoutedExecutionError::ExternalEffect {
            message: error.to_string(),
            disposition,
            compile_control: control,
        };
    }
    match control {
        Some(control) => RoutedExecutionError::Control(control),
        None => RoutedExecutionError::Engine(error.to_string()),
    }
}

fn add_files_status(file_count: u32) -> Result<QueryResult, String> {
    build_string_query_result("status", vec![format!("Added {file_count} file(s)")])
}

/// Design: ADR-0012 (docs/adr/ADR-0012-frontend-query-session-router.md)
#[derive(Clone)]
pub struct FrontendQueryService {
    state_store: Option<crate::state_store::execution::StateStoreOriginalBinding>,
    #[cfg(novarocks_dev_controls)]
    statement_observation: Option<Arc<crate::preparation_diagnostics::StatementObservationOwner>>,
    #[cfg(novarocks_dev_controls)]
    result_handoff: Option<Arc<crate::test_session::result_handoff::ResultHandoffControl>>,
    session_catalog_resolver: SessionCatalogService,
    query_compiler: FrontendQueryCompiler,
    command_executor: Arc<dyn SpecializedStatementRoute>,
    product_command_router: ProductCommandRouter,
    query_control: QueryControlService,
    client_connection_control: Arc<dyn ClientConnectionControlPort>,
    logical_execution_launcher: Arc<dyn LogicalExecutionLauncher>,
    workload_root_admission: RootAdmissionHandle,
    role: ClusterRole,
    topology: BackendTopologyService,
    dml: Arc<DmlService>,
    insert_engine: Arc<dyn InsertEngine>,
    delete_engine: Arc<dyn DeleteEngine>,
    mutation_engine: Arc<dyn MutationEngine>,
    add_files_engine: Arc<dyn AddFilesEngine>,
    ctas_engine: Arc<dyn CtasEngine>,
    truncate_engine: Arc<dyn TruncateEngine>,
    query_blocking_executor: QueryBlockingExecutor,
    /// Cost budget frozen from `[runtime]` and handed to statement admission
    /// whenever the session did not set one itself.
    optimizer_query_mem_limit_bytes: u64,
    lake_publication_runtime_policy: LakePublicationRuntimePolicy,
    serving_admission: FrontendServingAdmission,
}

impl FrontendQueryService {
    pub(crate) fn with_state_store_original(
        mut self,
        binding: Option<crate::state_store::execution::StateStoreOriginalBinding>,
    ) -> Self {
        self.state_store = binding;
        self
    }

    #[cfg(novarocks_dev_controls)]
    pub(crate) fn with_statement_observation(
        mut self,
        owner: Option<Arc<crate::preparation_diagnostics::StatementObservationOwner>>,
    ) -> Self {
        self.statement_observation = owner;
        self
    }

    #[cfg(novarocks_dev_controls)]
    pub(crate) fn with_result_handoff(
        mut self,
        control: Option<Arc<crate::test_session::result_handoff::ResultHandoffControl>>,
    ) -> Self {
        self.result_handoff = control;
        self
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "The query processor constructor wires independent application owners and execution policies; a second aggregate would duplicate the composed service contract."
    )]
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
        logical_execution_launcher: Arc<dyn LogicalExecutionLauncher>,
        workload_root_admission: RootAdmissionHandle,
        role: ClusterRole,
        topology: BackendTopologyService,
        dml: Arc<DmlService>,
        insert_engine: Arc<dyn InsertEngine>,
        delete_engine: Arc<dyn DeleteEngine>,
        mutation_engine: Arc<dyn MutationEngine>,
        add_files_engine: Arc<dyn AddFilesEngine>,
        ctas_engine: Arc<dyn CtasEngine>,
        truncate_engine: Arc<dyn TruncateEngine>,
        query_blocking_executor: QueryBlockingExecutor,
        optimizer_query_mem_limit_bytes: u64,
        lake_publication_runtime_policy: LakePublicationRuntimePolicy,
        serving_admission: FrontendServingAdmission,
    ) -> Self {
        Self {
            #[cfg(novarocks_dev_controls)]
            statement_observation: None,
            state_store: None,
            #[cfg(novarocks_dev_controls)]
            result_handoff: None,
            session_catalog_resolver,
            query_compiler,
            command_executor: Arc::new(TypedCommandRoute::new(
                backend_command_executor,
                view_command_executor,
                mv_command_consumer,
                mv_command_executor,
                query_blocking_executor.clone(),
                query_control.clone(),
            )),
            product_command_router,
            query_control,
            client_connection_control,
            logical_execution_launcher,
            workload_root_admission,
            role,
            topology,
            dml,
            insert_engine,
            delete_engine,
            mutation_engine,
            add_files_engine,
            ctas_engine,
            truncate_engine,
            query_blocking_executor,
            optimizer_query_mem_limit_bytes,
            lake_publication_runtime_policy,
            serving_admission,
        }
    }
}

impl QuerySessionFactory for FrontendQueryService {
    // Design: ADR-0203 (docs/adr/ADR-0203-client-result-presentation-and-trino-paged-delivery.md)
    fn open_statement_session(
        &self,
        request: StatementSessionRequest,
    ) -> Result<Arc<dyn QuerySession>, StatementSessionError> {
        let (query_id, principal, value) = request.into_parts();
        let state = SessionSqlState::from_session_value(value)?;
        let identity = SessionIdentity::client_statement(query_id.clone(), Arc::clone(&principal));
        let lease = self
            .serving_admission
            .register_session(|| self.query_control.register_session(identity))
            .map_err(query_service_admission_error)?
            .map_err(|error| {
                internal_error(format!(
                    "register frontend statement session failed: {error:?}"
                ))
            })?;
        Ok(Arc::new(FrontendQuerySession {
            service: self.clone(),
            origin: novarocks_query_application::session_control::SessionOrigin::ClientStatement(
                query_id,
            ),
            lease: Mutex::new(Some(lease)),
            state: Arc::new(Mutex::new(state)),
            statement_ready: statement_action::StatementReadyReceiver::default(),
            principal,
        }))
    }

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
            origin: novarocks_query_application::session_control::SessionOrigin::Connection(
                request.connection_token(),
            ),
            lease: Mutex::new(Some(lease)),
            state: Arc::new(Mutex::new(SessionSqlState::default())),
            statement_ready: statement_action::StatementReadyReceiver::default(),
            principal: Arc::from(request.principal()),
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
    origin: novarocks_query_application::session_control::SessionOrigin,
    service: FrontendQueryService,
    lease: Mutex<Option<QuerySessionLease>>,
    state: Arc<Mutex<SessionSqlState>>,
    statement_ready: statement_action::StatementReadyReceiver,
    /// Who the server authenticated on this connection. A command that records
    /// what someone did takes this, never an argument that claims an identity.
    principal: Arc<str>,
}

/// Statement-origin errors retain the original SQL prefix for exact locations.
/// Connection-origin framing keeps its existing normalization policy.
fn statement_execution_source<'a>(
    origin: &novarocks_query_application::session_control::SessionOrigin,
    original: &'a str,
    legacy_normalized: &'a str,
) -> &'a str {
    match origin {
        novarocks_query_application::session_control::SessionOrigin::ClientStatement(_) => {
            original.trim_end().trim_end_matches(';').trim_end()
        }
        novarocks_query_application::session_control::SessionOrigin::Connection(_) => {
            legacy_normalized
        }
    }
}

impl FrontendQuerySession {
    fn governed_statement_begin_error(
        &self,
        error: GovernedQueryStatementBeginError,
    ) -> QueryServiceError {
        if matches!(
            error,
            GovernedQueryStatementBeginError::Admission(WorkError::Closed)
        ) && let Some(admission) = self.service.serving_admission.admission_error()
        {
            return query_service_admission_error(admission);
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
                    StatementKind::Empty,
                ),
            );
        }
        if let Some(error) = admin_raise_engine_error(trimmed)? {
            return Err(error);
        }
        let source = statement_execution_source(&self.origin, statement, trimmed);
        let parsed_statement = parse_single_statement(source)
            .map_err(|error| query_service_parse_error(error, source))?;
        let kind = StatementKind::from_statement(&parsed_statement);
        let result = match parsed_statement {
            ParsedStatement::Session(ast::SessionStatement::Set(statement))
                if statement
                    .assignments
                    .iter()
                    .any(|assignment| matches!(assignment.value, ast::SetValue::Query(_))) =>
            {
                self.execute_governed_set(source.to_string(), statement)
                    .await
            }
            ParsedStatement::Session(statement) => {
                self.execute_session_statement(source, &statement).await
            }
            ParsedStatement::Query(_) => {
                return self
                    .execute_governed_read(source.to_string(), parsed_statement)
                    .await
                    .map(|output| {
                        novarocks_query_application::session::QuerySessionStatement::output_owned(
                            output, kind,
                        )
                    });
            }
            statement => {
                self.execute_typed_statement(source.to_string(), statement)
                    .await
            }
        };
        result.map(|output| {
            novarocks_query_application::session::QuerySessionStatement::output_owned(output, kind)
        })
    }

    async fn execute_session_statement(
        &self,
        source: &str,
        statement: &ast::SessionStatement,
    ) -> Result<StatementResult, QueryServiceError> {
        let token = self.token()?;
        if let Some(result) = execute_control_session_statement(
            source,
            statement,
            token,
            &self.service.query_control,
            self.service.client_connection_control.as_ref(),
        ) {
            return result;
        }
        let governed = self
            .service
            .query_control
            .begin_governed_statement_with_result(
                token,
                &self.service.workload_root_admission,
                WorkClass::Management,
                None,
                None,
                Some(Arc::from(source)),
                ResultWindowClass::Local,
            )
            .map_err(|error| self.governed_statement_begin_error(error))?;
        let mut effects = SessionEffects::default();
        let result = match statement {
            ast::SessionStatement::Set(statement) => {
                for assignment in &statement.assignments {
                    if let Err(error) = self.admit_session_set_assignment(source, assignment) {
                        return Ok(self.governed_typed_error(error, governed));
                    }
                }
                for assignment in &statement.assignments {
                    match self.apply_session_set_assignment(source, assignment).await {
                        Ok(change) => effects.append(change),
                        Err(error) => return Ok(self.governed_typed_error(error, governed)),
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
                match self.init_database_with_cancellation(&schema, cancellation, &governed).await {
                    Ok(change) => effects.append(change),
                    Err(error) => return Ok(self.governed_typed_error(error, governed)),
                }
                Ok(StatementResult::Ok)
            }
            ast::SessionStatement::Kill(_) => {
                unreachable!("KILL enters the dedicated control route before ordinary admission")
            },
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
            Ok(StatementResult::Ok) => Ok(StatementResult::GovernedCompletion(
                GovernedCompletionStatementResult::with_session_effects(governed, effects),
            )),
            Ok(StatementResult::Query(result)) => {
                // Local delivery may still use transitional LRA credits under
                // this exact scope. Its root exits at the protocol terminal.
                Ok(self.governed_local_result(result, governed))
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

    fn governed_local_result(
        &self,
        result: QueryResult,
        statement: novarocks_query_application::session_control::GovernedQueryStatementOwner,
    ) -> StatementResult {
        let family = match self.state.lock() {
            Ok(state) => *state.presentation(),
            Err(error) => return self.governed_typed_error(poisoned_state(error), statement),
        };
        match GovernedImmediateStatementResult::try_new(result, statement, family) {
            Ok(result) => StatementResult::GovernedQuery(result),
            Err((error, statement)) => self.governed_typed_error(internal_error(error), statement),
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
    ) -> Result<SessionEffects, QueryServiceError> {
        let mut state = self.state.lock().map_err(poisoned_state)?;
        self.apply_session_set_assignment_to_state(source, assignment, &mut state)
    }

    fn apply_session_set_assignment_to_state(
        &self,
        source: &str,
        assignment: &ast::SetAssignment,
        state: &mut SessionSqlState,
    ) -> Result<SessionEffects, QueryServiceError> {
        set_action::apply_session_assignment_in(
            &self.service.session_catalog_resolver,
            source,
            assignment,
            state,
        )
    }

    async fn init_database_with_cancellation(
        &self,
        schema: &str,
        cancellation: QueryCancellationView,
        statement: &novarocks_query_application::session_control::GovernedQueryStatementOwner,
    ) -> Result<SessionEffects, QueryServiceError> {
        let capacity = novarocks_query_application::admitted_query_context::QueryResultCapacityBinding::try_new(
            statement.scope(),
            statement.result_window_alias().ok_or_else(|| {
                internal_error("session namespace lookup requires its admitted result window")
            })?,
        ).map_err(|error| internal_error(error.to_string()))?;
        let current_catalog = self
            .state
            .lock()
            .map_err(poisoned_state)?
            .current_catalog()
            .map(ToOwned::to_owned);
        let connector_context = crate::connector::connector_request_context_for_query_on_runtime(
            self.service.query_compiler.connector_runtime(),
            None,
            cancellation,
        )
        .map_err(internal_error)?;
        let context = resolve_database_context(
            &self.service.session_catalog_resolver,
            current_catalog.as_deref(),
            schema,
            connector_context,
            &capacity,
        )
        .await?;
        let mut state = self.state.lock().map_err(poisoned_state)?;
        let effects = SessionEffects::set_catalog_schema(context.catalog, context.database);
        state.apply_effects(&effects)?;
        Ok(effects)
    }

    async fn execute_governed_set(
        &self,
        source: String,
        set: ast::SetStatement,
    ) -> Result<StatementResult, QueryServiceError> {
        let source: Arc<str> = source.into();
        for assignment in &set.assignments {
            self.admit_session_set_assignment(&source, assignment)?;
        }
        let (deadline, timeout_ms) = {
            let state = self.state.lock().map_err(poisoned_state)?;
            governed_query_deadline(&state)?
        };
        let window_class = if set
            .assignments
            .iter()
            .any(|assignment| matches!(&assignment.value, ast::SetValue::Query(_)))
        {
            ResultWindowClass::Internal
        } else {
            ResultWindowClass::Local
        };
        let token = self.token()?;
        let statement = self
            .service
            .query_control
            .begin_queued_governed_query_statement_with_result(
                token,
                &self.service.workload_root_admission,
                deadline.map(tokio::time::Instant::from_std),
                timeout_ms,
                Some(Arc::clone(&source)),
                window_class,
            )
            .await
            .map_err(|error| self.governed_statement_begin_error(error))?;
        #[cfg(novarocks_dev_controls)]
        if let Some(owner) = &self.service.statement_observation
            && let Err(error) = owner.bind_statement(statement.token(), &source)
        {
            return Ok(self.governed_typed_error(internal_error(error), statement));
        }
        self.execute_observed_set(source, set, deadline, statement)
            .await
    }

    async fn execute_governed_read(
        &self,
        sql: String,
        parsed_statement: ParsedStatement,
    ) -> Result<StatementResult, QueryServiceError> {
        let sql: Arc<str> = sql.into();
        let ParsedStatement::Query(query) = &parsed_statement else {
            return Err(internal_error("read admission requires a query statement"));
        };
        let purpose = FrontendQueryPurpose::client_query(query);
        let (deadline, timeout_ms) = {
            let state = self.state.lock().map_err(poisoned_state)?;
            governed_query_deadline(&state)?
        };
        let token = self.token()?;
        let statement = self
            .service
            .query_control
            .begin_queued_governed_query_statement_with_result(
                token,
                &self.service.workload_root_admission,
                deadline.map(tokio::time::Instant::from_std),
                timeout_ms,
                Some(Arc::clone(&sql)),
                purpose.window_class(),
            )
            .await
            .map_err(|error| self.governed_statement_begin_error(error))?;
        #[cfg(novarocks_dev_controls)]
        if let Some(owner) = &self.service.statement_observation
            && let Err(error) = owner.bind_statement(statement.token(), &sql)
        {
            return Ok(self.governed_typed_error(internal_error(error), statement));
        }

        let state = self.state.lock().map_err(poisoned_state)?.clone();
        self.execute_observed_read(sql, parsed_statement, state, deadline, statement, purpose)
            .await
    }

    async fn execute_typed_statement(
        &self,
        sql: String,
        parsed_statement: ParsedStatement,
    ) -> Result<StatementResult, QueryServiceError> {
        reject_plain_query_from_legacy_typed_route(&parsed_statement)?;
        let query_timeout_secs = self
            .state
            .lock()
            .map_err(poisoned_state)?
            .execution_settings()
            .query_timeout_secs();
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
        let work_class = typed_statement_work_class(&parsed_statement);
        let window_class = typed_statement_result_window_class(&parsed_statement);
        let mut statement = if work_class == WorkClass::Query {
            self.service
                .query_control
                .begin_queued_governed_query_statement_with_result(
                    token,
                    &self.service.workload_root_admission,
                    deadline.map(tokio::time::Instant::from_std),
                    timeout_ms,
                    Some(Arc::from(sql.as_str())),
                    window_class,
                )
                .await
        } else {
            self.service
                .query_control
                .begin_governed_statement_with_result(
                    token,
                    &self.service.workload_root_admission,
                    work_class,
                    deadline.map(tokio::time::Instant::from_std),
                    timeout_ms,
                    Some(Arc::from(sql.as_str())),
                    window_class,
                )
        }
        .map_err(|error| self.governed_statement_begin_error(error))?;
        #[cfg(novarocks_dev_controls)]
        let observation = match self
            .service
            .statement_observation
            .as_ref()
            .map(|owner| owner.bind_statement(statement.token(), &sql))
            .transpose()
        {
            Ok(handle) => handle.flatten(),
            Err(error) => return Ok(self.governed_typed_error(internal_error(error), statement)),
        };
        let state = self.state.lock().map_err(poisoned_state)?.clone();
        let parsed_statement = state
            .substitute_user_variables(parsed_statement)
            .map_err(|error| internal_error(error.to_string()))?;
        novarocks_query_application::sql::admission::admit_persisted_definition_semantics(
            &sql,
            &parsed_statement,
            state.sql_semantics(),
        )?;
        let parsed_statement = match parsed_statement {
            ParsedStatement::ExplainQuery(explain) => {
                return self
                    .execute_observed_explain(
                        sql,
                        explain,
                        state,
                        deadline,
                        statement,
                        #[cfg(novarocks_dev_controls)]
                        observation,
                    )
                    .await;
            }
            other => other,
        };
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
        let presentation = *state.presentation();
        let (
            current_catalog,
            current_database,
            execution_settings,
            mut optimizer_settings,
            sql_semantics,
        ) = state.into_query_attempt_inputs();
        let sql_semantics =
            novarocks_sql::sql_mode::statement_sql_semantics(&sql_semantics, &parsed_statement)
                .map_err(|error| {
                    QueryServiceError::from_user_error(error.to_user_error(Some(&sql)))
                })?;
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
            sql_semantics.clone(),
            presentation,
        ));
        let command_executor = Arc::clone(&self.service.command_executor);
        let product_command_router = self.service.product_command_router.clone();
        let dml = Arc::clone(&self.service.dml);
        let insert_engine = Arc::clone(&self.service.insert_engine);
        let delete_engine = Arc::clone(&self.service.delete_engine);
        let mutation_engine = Arc::clone(&self.service.mutation_engine);
        let add_files_engine = Arc::clone(&self.service.add_files_engine);
        let ctas_engine = Arc::clone(&self.service.ctas_engine);
        let truncate_engine = Arc::clone(&self.service.truncate_engine);
        let window = match statement.result_window_alias() {
            Some(window) => window,
            None => {
                return Ok(self.governed_typed_error(
                    internal_error("typed statement has no admitted result capacity"),
                    statement,
                ));
            }
        };
        let capacity = match novarocks_query_application::admitted_query_context::QueryResultCapacityBinding::try_new(statement.scope(), window) {
            Ok(capacity) => capacity,
            Err(error) => return Ok(self.governed_typed_error(internal_error(error.to_string()), statement)),
        };
        let context = match context.with_result_capacity(capacity) {
            Ok(context) => context,
            Err(error) => {
                return Ok(self.governed_typed_error(internal_error(error.to_string()), statement));
            }
        };
        let query_options = with_query_hints(
            query_options_from_session_settings(&execution_settings),
            &sql_semantics,
            match &parsed_statement {
                ParsedStatement::ExplainQuery(explain) => Some(&explain.query),
                ParsedStatement::Dml(novarocks_parser::ast::DmlStatement::Insert(insert)) => {
                    Some(&insert.source)
                }
                ParsedStatement::Dml(novarocks_parser::ast::DmlStatement::CreateTableAsSelect(
                    ctas,
                )) => Some(&ctas.query),
                _ => None,
            },
        );
        let connector_context =
            match crate::connector::connector_request_context_for_query_on_runtime(
                self.service.query_compiler.connector_runtime(),
                Some(&query_options),
                cancellation.clone(),
            ) {
                Ok(context) => context,
                Err(error) => {
                    return Ok(self.governed_typed_error(internal_error(error), statement));
                }
            };
        let statement_token = statement.token();
        #[cfg(novarocks_dev_controls)]
        #[cfg(novarocks_dev_controls)]
        if let Some(nomination) = observation.as_ref().and_then(|handle| handle.nomination()) {
            let installed =
                connector_context.request_scope_extension_or_insert_with(|| nomination.clone());
            if installed.as_ref() != &nomination {
                return Ok(self.governed_typed_error(
                    internal_error("typed statement nomination conflicts with its request scope"),
                    statement,
                ));
            }
        }
        #[cfg(novarocks_dev_controls)]
        let context = match observation.as_ref().and_then(|handle| handle.nomination()) {
            Some(nomination) => {
                match context.with_test_statement_nomination(Arc::new(nomination)) {
                    Ok(context) => context,
                    Err(error) => {
                        return Ok(self.governed_typed_error(internal_error(error), statement));
                    }
                }
            }
            None => context,
        };
        let command_context = CommandContext::new(
            statement.scope().clone(),
            connector_context,
            statement_token,
            Arc::clone(&self.principal),
        );
        let command_context = match statement.result_window_alias() {
            Some(window) => match command_context.with_result_window(window) {
                Ok(context) => context,
                Err(error) => {
                    return Ok(
                        self.governed_typed_error(internal_error(error.to_string()), statement)
                    );
                }
            },
            None => command_context,
        };
        if let ParsedStatement::View(novarocks_parser::ast::ViewStatement::Show(show)) =
            &parsed_statement
        {
            let action = match self.service.query_compiler.prepare_show_views_action(
                show.clone(),
                context,
                command_context,
            ) {
                Ok(action) => action,
                Err(error) => {
                    return Ok(self.governed_typed_error(
                        routed_execution_service_error(product_command_error(error)),
                        statement,
                    ));
                }
            };
            return self
                .execute_observed_statement_action(statement, action)
                .await;
        }
        if let Ok(Some(ProductSqlCommand::Catalog(
            command @ (novarocks_sql::semantic::CatalogSqlCommand::CreateCatalog(_)
            | novarocks_sql::semantic::CatalogSqlCommand::DropCatalog { .. }),
        ))) = lower_product_sql_command(&parsed_statement)
        {
            let action = match self
                .service
                .product_command_router
                .prepare_catalog_statement_action(command, context, command_context)
            {
                Ok(action) => action,
                Err(error) => {
                    return Ok(self.governed_typed_error(
                        routed_execution_service_error(product_command_error(error)),
                        statement,
                    ));
                }
            };
            return self
                .execute_observed_statement_action(statement, action)
                .await;
        }
        if let Ok(Some(ProductSqlCommand::Statistics(
            command @ (novarocks_sql::semantic::StatisticsSqlCommand::AnalyzeTable { .. }
            | novarocks_sql::semantic::StatisticsSqlCommand::ShowTableStats { .. }),
        ))) = lower_product_sql_command(&parsed_statement)
        {
            let action = match self
                .service
                .product_command_router
                .prepare_statistics_statement_action(command, context, command_context)
            {
                Ok(action) => action,
                Err(error) => {
                    return Ok(self.governed_typed_error(
                        routed_execution_service_error(product_command_error(error)),
                        statement,
                    ));
                }
            };
            return self
                .execute_observed_statement_action(statement, action)
                .await;
        }
        if let Ok(Some(ProductSqlCommand::Maintenance(command))) =
            lower_product_sql_command(&parsed_statement)
            && crate::query_execution::maintenance::command::is_async_metadata_command(
                &command,
                context.session().current_catalog(),
                context.session().current_database(),
            )
            .unwrap_or(false)
        {
            let action = match self
                .service
                .product_command_router
                .prepare_maintenance_statement_action(command, context, command_context)
            {
                Ok(action) => action,
                Err(error) => {
                    return Ok(self.governed_typed_error(
                        routed_execution_service_error(product_command_error(error)),
                        statement,
                    ));
                }
            };
            return self
                .execute_observed_statement_action(statement, action)
                .await;
        }
        if let ParsedStatement::MaterializedView(mv) = &parsed_statement {
            use novarocks_parser::ast::MaterializedViewStatement;
            if matches!(
                mv,
                MaterializedViewStatement::Refresh(_)
                    | MaterializedViewStatement::Show(_)
                    | MaterializedViewStatement::ExplainRefresh(_)
                    | MaterializedViewStatement::Create(_)
                    | MaterializedViewStatement::Drop(_)
                    | MaterializedViewStatement::Alter(_)
            ) {
                let action = match self
                    .service
                    .command_executor
                    .prepare_materialized_view_statement_action(
                        mv.clone(),
                        context,
                        command_context,
                    ) {
                    Ok(action) => action,
                    Err(error) => {
                        return Ok(self.governed_typed_error(
                            routed_execution_service_error(product_command_error(error)),
                            statement,
                        ));
                    }
                };
                return self
                    .execute_observed_statement_action(statement, action)
                    .await;
            }
        }
        if let ParsedStatement::Dml(novarocks_parser::ast::DmlStatement::Insert(insert)) =
            &parsed_statement
        {
            let (command, reference) = match dml.prepare_insert_shape(insert, &sql) {
                Ok(value) => value,
                Err(error) => {
                    return Ok(self.governed_typed_error(
                        routed_execution_service_error(dml_result::<()>(Err(error)).unwrap_err()),
                        statement,
                    ));
                }
            };
            let action = match insert_engine.prepare_statement_action(
                crate::query_execution::dml::insert::InsertStatementInput {
                    command,
                    reference,
                    sql,
                    context,
                    query_options,
                    launcher: Arc::clone(&self.service.logical_execution_launcher),
                    #[cfg(novarocks_dev_controls)]
                    observation,
                },
            ) {
                Ok(action) => action,
                Err(error) => {
                    return Ok(self.governed_typed_error(internal_error(error), statement));
                }
            };
            return self
                .execute_observed_statement_action(statement, action)
                .await;
        }
        let parsed_statement = match parsed_statement {
            ParsedStatement::Dml(
                value @ (novarocks_parser::ast::DmlStatement::Update(_)
                | novarocks_parser::ast::DmlStatement::Merge(_)),
            ) => {
                let lowered = match dml.prepare_mutation_shape(&value, &sql) {
                    Ok(lowered) => lowered,
                    Err(error) => {
                        return Ok(self.governed_typed_error(
                            routed_execution_service_error(
                                dml_result::<()>(Err(error)).unwrap_err(),
                            ),
                            statement,
                        ));
                    }
                };
                let action = match mutation_engine.prepare_statement_action(
                    crate::query_execution::dml::mutation::MutationStatementInput {
                        statement: lowered,
                        sql,
                        context,
                        query_options,
                        launcher: Arc::clone(&self.service.logical_execution_launcher),
                        #[cfg(novarocks_dev_controls)]
                        observation,
                    },
                ) {
                    Ok(action) => action,
                    Err(error) => {
                        return Ok(self.governed_typed_error(internal_error(error), statement));
                    }
                };
                return self
                    .execute_observed_statement_action(statement, action)
                    .await;
            }
            ParsedStatement::Dml(novarocks_parser::ast::DmlStatement::CreateTableAsSelect(
                value,
            )) => {
                let action = match ctas_engine.prepare_statement_action(
                    crate::query_execution::dml::ctas::CtasStatementInput {
                        statement: value,
                        sql,
                        context,
                        query_options,
                        launcher: Arc::clone(&self.service.logical_execution_launcher),
                        #[cfg(novarocks_dev_controls)]
                        observation,
                    },
                ) {
                    Ok(action) => action,
                    Err(error) => {
                        return Ok(self.governed_typed_error(
                            routed_execution_service_error(
                                dml_result::<()>(Err(error)).unwrap_err(),
                            ),
                            statement,
                        ));
                    }
                };
                return self
                    .execute_observed_statement_action(statement, action)
                    .await;
            }
            other => other,
        };
        let (parsed_statement, delete_statement) = match parsed_statement {
            ParsedStatement::Dml(novarocks_parser::ast::DmlStatement::Delete(statement)) => (
                None,
                Some(
                    crate::query_execution::dml::delete::OwnedDeleteStatement::Predicate(statement),
                ),
            ),
            ParsedStatement::Dml(novarocks_parser::ast::DmlStatement::AddEqualityDelete(
                statement,
            )) => (
                None,
                Some(
                    crate::query_execution::dml::delete::OwnedDeleteStatement::Equality(statement),
                ),
            ),
            statement => (Some(statement), None),
        };
        if let Some(delete_statement) = delete_statement {
            use crate::query_execution::dml::delete::{DeleteStatement, OwnedDeleteStatement};
            let borrowed = match &delete_statement {
                OwnedDeleteStatement::Predicate(value) => DeleteStatement::Predicate(value),
                OwnedDeleteStatement::Equality(_) => DeleteStatement::Equality,
            };
            if let Err(error) = dml.admit_delete_shape(borrowed, &sql) {
                return Ok(self.governed_typed_error(
                    routed_execution_service_error(dml_result::<()>(Err(error)).unwrap_err()),
                    statement,
                ));
            }
            let action = match delete_engine.prepare_statement_action(
                crate::query_execution::dml::delete::DeleteStatementInput {
                    statement: delete_statement,
                    sql,
                    context,
                    query_options,
                    launcher: Arc::clone(&self.service.logical_execution_launcher),
                    #[cfg(novarocks_dev_controls)]
                    observation,
                },
            ) {
                Ok(action) => action,
                Err(error) => {
                    return Ok(self.governed_typed_error(internal_error(error), statement));
                }
            };
            return self
                .execute_observed_statement_action(statement, action)
                .await;
        }
        let parsed_statement = parsed_statement.expect("remaining typed statement is retained");
        let producer_window = command_context.result_window_alias();
        let execution_owner = statement
            .take_execution_owner()
            .expect("governed typed statement transfers its execution owner exactly once");
        let worker_cancellation = cancellation.clone();
        let synchronous_command_executor = self.service.query_blocking_executor.clone();
        let mut worker: Pin<Box<dyn Future<Output = _> + Send>> = match parsed_statement {
            ParsedStatement::ExplainQuery(_) => {
                unreachable!("EXPLAIN was transferred to its original statement source")
            }
            ParsedStatement::Dml(novarocks_parser::ast::DmlStatement::Delete(_)) => {
                unreachable!("DELETE was transferred to its original statement action")
            }
            ParsedStatement::Dml(novarocks_parser::ast::DmlStatement::Insert(_)) => {
                unreachable!("INSERT was transferred to its original statement action")
            }
            ParsedStatement::Dml(novarocks_parser::ast::DmlStatement::AddEqualityDelete(_)) => {
                unreachable!("equality DELETE was transferred to its original statement action")
            }
            ParsedStatement::Dml(
                novarocks_parser::ast::DmlStatement::Update(_)
                | novarocks_parser::ast::DmlStatement::Merge(_)
                | novarocks_parser::ast::DmlStatement::CreateTableAsSelect(_),
            ) => {
                unreachable!("mutation or CTAS was transferred to its original statement action")
            }
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
                    execution_owner,
                    producer_window,
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
                            execution_owner,
                            producer_window,
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
            ParsedStatement::ShowProcessList(statement) => Box::pin(async move {
                let result = command_executor
                    .execute_show_process_list(statement.full, &context, &command_context)
                    .await
                    .map_err(|error| RoutedExecutionError::Engine(error.to_string()));
                Ok((result, execution_owner))
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
                    let (result, execution_owner) = worker.await.map_err(internal_error)?;
                    statement.restore_execution_owner(execution_owner);
                    return Ok(self.governed_typed_error(
                        selected_execution_or_worker_fallback(
                            result,
                            cancellation_error(cancellation.reason().unwrap_or(
                                QueryCancellationReason::DeadlineExceeded { timeout_ms },
                            )),
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
                selected_execution_or_worker_fallback(
                    result,
                    cancellation_error(cancellation.reason().expect("cancelled view has a reason")),
                ),
                statement,
            ));
        }
        match result {
            Ok(StatementResult::Query(result)) => Ok(self.governed_local_result(result, statement)),
            Ok(StatementResult::Ok) => Ok(StatementResult::GovernedCompletion(
                GovernedCompletionStatementResult::new(
                    novarocks_query_application::session_control::StatementCommandCompletion::new(
                        0,
                    ),
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
            Err(error) => {
                Ok(self.governed_typed_error(routed_execution_service_error(error), statement))
            }
        }
    }

    fn governed_typed_error(
        &self,
        error: QueryServiceError,
        statement: novarocks_query_application::session_control::GovernedQueryStatementOwner,
    ) -> StatementResult {
        StatementResult::GovernedError(GovernedErrorStatementResult::new(error, statement))
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
    sql_semantics: &novarocks_sql::sql_mode::SqlSemanticSettings,
    query: Option<&novarocks_parser::ast::Query>,
) -> QueryOptions {
    let mut raw = *query_options.as_proto();
    raw.allow_throw_exception = sql_semantics.sql_mode().allow_throw_exception();
    if let Some(query) = query
        && let Some(limit) = novarocks_sql::admission::query_mem_limit_hint(query)
    {
        raw.query_mem_limit = limit;
    }
    QueryOptions::from_proto(raw)
}

#[async_trait]
impl QuerySession for FrontendQuerySession {
    #[cfg(novarocks_dev_controls)]
    fn connection_session(
        &self,
        connection: novarocks_query_application::client_connection::ClientConnectionToken,
    ) -> Option<SessionToken> {
        match &self.origin {
            novarocks_query_application::session_control::SessionOrigin::Connection(original)
                if *original == connection =>
            {
                self.lease
                    .lock()
                    .ok()?
                    .as_ref()
                    .map(QuerySessionLease::token)
            }
            _ => None,
        }
    }

    async fn init_database(
        &self,
        schema: &str,
    ) -> Result<novarocks_query_application::session::QuerySessionStatement, QueryServiceError>
    {
        let token = self.token()?;
        let statement = self
            .service
            .query_control
            .begin_governed_statement_with_result(
                token,
                &self.service.workload_root_admission,
                WorkClass::Management,
                None,
                None,
                None,
                ResultWindowClass::Local,
            )
            .map_err(|error| self.governed_statement_begin_error(error))?;
        let cancellation = QueryCancellationView::governed(
            statement.cancellation().clone(),
            statement.timeout_ms(),
        );
        match self
            .init_database_with_cancellation(schema, cancellation, &statement)
            .await
        {
            Ok(effects) => Ok(
                novarocks_query_application::session::QuerySessionStatement::output_owned(
                    StatementResult::GovernedCompletion(
                        GovernedCompletionStatementResult::with_session_effects(statement, effects),
                    ),
                    StatementKind::Use,
                ),
            ),
            Err(error) => Ok(
                novarocks_query_application::session::QuerySessionStatement::output_owned(
                    self.governed_typed_error(error, statement),
                    StatementKind::Use,
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
                    StatementKind::Empty,
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
    capacity: &novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
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
                        .external_namespace_exists(connector_context, capacity, catalog, &database)
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
                        .external_namespace_exists(connector_context, capacity, &catalog, &database)
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

/// Work classification selects statement admission; the output purpose
/// independently selects its result capacity. Foreground MV refresh and
/// repartition and foreground distributed maintenance rewrites are management
/// commands whose writes produce InternalFacts.
fn typed_statement_result_window_class(statement: &ParsedStatement) -> ResultWindowClass {
    match statement {
        ParsedStatement::Dml(_)
        | ParsedStatement::MaterializedView(ast::MaterializedViewStatement::Refresh(_)) => {
            ResultWindowClass::Internal
        }
        ParsedStatement::ExplainQuery(explain) if explain.format == ast::ExplainFormat::Analyze => {
            ResultWindowClass::Internal
        }
        ParsedStatement::MaterializedView(ast::MaterializedViewStatement::Alter(alter))
            if matches!(
                alter.action,
                ast::MaterializedViewAlterAction::Repartition(_)
            ) =>
        {
            ResultWindowClass::Internal
        }
        ParsedStatement::Maintenance(ast::MaintenanceStatement::Call(call))
            if matches!(call.procedure.parts.as_slice(), [_, namespace, procedure]
                if namespace.value.eq_ignore_ascii_case("system")
                    && (procedure.value.eq_ignore_ascii_case("rewrite_data_files")
                        || procedure.value.eq_ignore_ascii_case("rewrite_position_delete_files"))) =>
        {
            ResultWindowClass::Internal
        }
        _ => ResultWindowClass::Local,
    }
}

#[cfg(test)]
async fn consume_governed_scalar_stream(
    execution: &mut novarocks_query_application::api::ExecutionHandle,
    stream: novarocks_query_application::api::QueryResultStream,
    scalar_schema: Option<&novarocks_result_contract::ScalarSchema>,
) -> Result<String, QueryServiceError> {
    let mut scalar = set_action::GovernedScalarStream::new(stream, scalar_schema.cloned());
    std::future::poll_fn(|cx| scalar.poll_value(execution, cx)).await
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
    QueryOptions::from_proto(novarocks::QueryOptions {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statement_origin_parser_errors_keep_the_original_source_prefix() {
        use novarocks_query_application::{
            query_control::QueryApplicationControl,
            session_control::{QueryControlService, SessionOrigin},
        };
        let control = QueryControlService::new(Arc::new(QueryApplicationControl::new(
            novarocks_types::FrontendProcessId::new_v7(),
        )));
        let origin = SessionOrigin::ClientStatement(control.mint_client_query_id());
        for (sql, line, column) in [
            ("                    SELECT );;", 1, 28),
            ("-- 中文😀 comment\n  SELECT '中😀中', );;", 2, 24),
            ("\n\nSELECT\n '中😀',\n );;", 5, 2),
        ] {
            let legacy = strip_leading_line_comments(sql.trim().trim_end_matches(';').trim());
            let source = statement_execution_source(&origin, sql, legacy);
            assert_eq!(
                source.as_ptr(),
                sql.as_ptr(),
                "the original source prefix stays borrowed"
            );
            let error = parse_single_statement(source)
                .expect_err("actual parser must reject the closing parenthesis");
            let error = query_service_parse_error(error, source);
            let location = error.user_error().unwrap().location().unwrap();
            assert_eq!(
                (location.line(), location.column()),
                (line, column),
                "{sql}"
            );
        }
        let sql = "  -- original prefix\n SELECT '中😀';;;  ";
        let source = statement_execution_source(
            &origin,
            sql,
            strip_leading_line_comments(sql.trim().trim_end_matches(';').trim()),
        );
        assert!(
            parse_single_statement(source).is_ok(),
            "trailing-semicolon tolerance stays intact"
        );
        assert_eq!(source.as_ptr(), sql.as_ptr());
    }

    #[test]
    fn connection_origin_parser_framing_keeps_legacy_normalization() {
        let origin = novarocks_query_application::session_control::SessionOrigin::Connection(
            novarocks_query_application::client_connection::ClientConnectionToken::new(41, 1)
                .unwrap(),
        );
        let sql = "  -- existing connection prefix\n   SELECT );; ";
        let legacy = strip_leading_line_comments(sql.trim().trim_end_matches(';').trim());
        let source = statement_execution_source(&origin, sql, legacy);
        assert_eq!(source, "SELECT )");
        let error = query_service_parse_error(parse_single_statement(source).unwrap_err(), source);
        let location = error.user_error().unwrap().location().unwrap();
        assert_eq!((location.line(), location.column()), (1, 8));
    }

    #[test]
    fn runtime_task_refusal_survives_distributed_dml_and_final_user_projection() {
        use novarocks_failure_contract::FailureProjection;
        let backend = novarocks_types::BackendProcessId::new_v7();
        let query = novarocks_types::QueryExecutionId::new(
            novarocks_types::QueryId::new(31, 32),
            novarocks_types::AttemptId::new(1).unwrap(),
        )
        .unwrap();
        let identity = novarocks_execution_contract::TaskIdentity::new(
            query,
            novarocks_types::StageId::new(1).unwrap(),
            novarocks_types::TaskId::new(1).unwrap(),
            backend,
        );
        for original in [
            novarocks_plan_codec::PhysicalEncodeError::UnsupportedCapability(
                "FULL OUTER hash join".into(),
            ),
            novarocks_plan_codec::PhysicalEncodeError::Invalid("FULL OUTER hash join".into()),
        ] {
            let fact = original.failure_fact();
            let selected = novarocks_coordination_contract::SelectedTaskTermination::new(
                identity,
                novarocks_execution_contract::TerminationDetail::Failed(
                    novarocks_execution_contract::TaskFailure::new_in_phase(
                        fact,
                        novarocks_execution_contract::SafeDetail::truncating(
                            "FULL OUTER hash join",
                        ),
                        novarocks_execution_contract::TaskFailurePhase::Preparation,
                    ),
                ),
            );
            let input = novarocks_query_application::execution_error::ExecutionFailureInput::from_selected_task(&selected).unwrap();
            let error = crate::query_execution::contract::DistributedQueryError::from_execution(
                input.clone(),
            );
            let failure = crate::dml::error::DmlExecutionError::from(error).into_dml_error(None);
            let service =
                routed_execution_service_error(dml_result::<()>(Err(failure)).unwrap_err());
            assert_eq!(service.execution_failure(), Some(&input));
            assert_eq!(service.user_error(), Some(&input.user_error()));
        }
    }

    #[test]
    fn compiled_dml_refusal_preserves_publication_terminal_at_the_client_boundary() {
        use novarocks_failure_contract::FailureProjection;
        use novarocks_spi::connector::{
            LakePublicationDisposition, LakePublicationFamily, LakePublicationId,
            LakePublicationTarget,
        };
        let target =
            LakePublicationTarget::try_new("ice".into(), "db".into(), Some("orders".into()), None)
                .unwrap();
        let mut attempt = crate::dml::attempt::DmlPublicationAttempt::new(
            LakePublicationId::new_v7(),
            LakePublicationFamily::DataMutation,
            target,
            None,
        );
        attempt.terminal_known_uncommitted().unwrap();
        let terminal = attempt.terminal().unwrap().clone();
        for original in [
            novarocks_plan_codec::PhysicalEncodeError::Invalid("post-begin staging failed".into()),
            novarocks_plan_codec::PhysicalEncodeError::UnsupportedCapability(
                "post-begin staging failed".into(),
            ),
        ] {
            let expected = original.failure_fact();
            let failure = crate::dml::error::DmlExecutionError::from_encode(original)
                .into_dml_error(None)
                .with_publication_terminal(terminal.clone());
            let service =
                routed_execution_service_error(dml_result::<()>(Err(failure)).unwrap_err());
            assert_eq!(service.execution_failure().unwrap().fact(), expected);
            assert_eq!(service.publication_terminal(), Some(&terminal));
            assert_eq!(
                service.publication_terminal().unwrap().disposition(),
                LakePublicationDisposition::KnownUncommitted
            );
        }
    }

    #[test]
    fn compiled_dml_refusal_survives_outer_executor_message_wrapping() {
        use novarocks_failure_contract::FailureProjection;
        let original = novarocks_plan_codec::PhysicalEncodeError::UnsupportedCapability(
            "change-stream router".into(),
        );
        let expected = original.failure_fact();
        let failure =
            crate::dml::error::DmlExecutionError::from_encode(original).into_dml_error(None);
        let service = routed_execution_service_error(dml_result::<()>(Err(failure)).unwrap_err());
        assert_eq!(service.execution_failure().unwrap().fact(), expected);
        let Some(novarocks_type_contract::CompileControlError::Source(source)) =
            service.compile_control_error()
        else {
            panic!("retain the original encode source")
        };
        assert!(
            matches!(source.downcast_ref::<novarocks_plan_codec::PhysicalEncodeError>(), Some(novarocks_plan_codec::PhysicalEncodeError::UnsupportedCapability(detail)) if detail == "change-stream router")
        );
    }

    use std::sync::Mutex;

    use arrow::{array::StringArray, datatypes::DataType};
    use novarocks_query_application::api::{BackendTopologySnapshot, ResultField};
    use novarocks_query_application::cancellation::QueryCancellationSource;
    use novarocks_query_application::sql::{SqlBatchCursor, split_sql_statements};
    use novarocks_query_application::test_support::{
        ResultStreamTestProducer, TestResultDeliveryDisposition,
    };
    use novarocks_types::{AttemptId, QueryExecutionId, QueryId};
    use novarocks_workload_control::{
        CancellationReason as WorkCancellationReason, ResourceConfig,
    };

    #[test]
    fn typed_command_result_capacity_follows_output_purpose() {
        let refresh = parse_single_statement("REFRESH MATERIALIZED VIEW mv")
            .expect("parse foreground MV refresh");
        assert_eq!(typed_statement_work_class(&refresh), WorkClass::Management);
        assert_eq!(
            typed_statement_result_window_class(&refresh),
            ResultWindowClass::Internal
        );

        for sql in [
            "INSERT INTO target VALUES (1)",
            "EXPLAIN ANALYZE SELECT 1",
            "CALL ice.system.rewrite_data_files(table => 'db.orders')",
            "CALL ice.system.rewrite_position_delete_files(table => 'db.orders')",
            "CALL ice.SYSTEM.REWRITE_POSITION_DELETE_FILES(table => 'db.orders')",
        ] {
            let statement = parse_single_statement(sql).expect("parse internal output command");
            assert_eq!(
                typed_statement_result_window_class(&statement),
                ResultWindowClass::Internal,
                "{sql}"
            );
        }
        let repartition =
            parse_single_statement("ALTER MATERIALIZED VIEW mv REPARTITION BY (bucket(id, 4))")
                .expect("parse foreground MV repartition");
        assert_eq!(
            typed_statement_work_class(&repartition),
            WorkClass::Management
        );
        assert_eq!(
            typed_statement_result_window_class(&repartition),
            ResultWindowClass::Internal
        );
        for sql in [
            "EXPLAIN SELECT 1",
            "SHOW MATERIALIZED VIEWS",
            "ALTER MATERIALIZED VIEW mv PAUSE REFRESH",
            "CREATE DATABASE result_capacity_test",
            "CALL ice.system.rewrite_manifests(table => 'db.orders')",
            "CALL ice.system.expire_snapshots(table => 'db.orders', retain_last => 1)",
            "CALL ice.other.rewrite_position_delete_files(table => 'db.orders')",
        ] {
            let statement = parse_single_statement(sql).expect("parse local output command");
            assert_eq!(
                typed_statement_result_window_class(&statement),
                ResultWindowClass::Local,
                "{sql}"
            );
        }
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
        novarocks_query_application::test_support::TestResultDeliveryReceipt,
    ) {
        let (producer, execution, _resources, receipt) = ResultStreamTestProducer::open(
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
        .expect("open scalar result stream");
        (producer, execution, receipt)
    }

    #[test]
    fn process_list_truncates_info_only_without_full() {
        use novarocks_query_application::session_control::SessionProcess;
        use std::time::Duration;

        // A statement long enough to cross the limit, with a multi-byte
        // character straddling it: truncating by bytes here would produce
        // invalid UTF-8 rather than a shorter statement.
        let text: String = "SELECT '".to_string() + &"字".repeat(120) + "'";
        let processes = vec![
            SessionProcess {
                connection_id: 1,
                principal: Arc::from("root"),
                elapsed: Duration::from_secs(5),
                statement: None,
            },
            SessionProcess {
                connection_id: 2,
                principal: Arc::from("root"),
                elapsed: Duration::from_secs(0),
                statement: Some(Arc::from(text.as_str())),
            },
        ];

        let truncated = process_list_result(processes.clone(), false).expect("plain result");
        let full = process_list_result(processes, true).expect("full result");
        assert_eq!(truncated.columns.len(), 8);
        assert_eq!(truncated.row_count(), 2);

        fn info(result: &novarocks_query_application::api::QueryResult) -> (bool, String) {
            use arrow::array::Array;

            let column = result.batches[0]
                .column(7)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .expect("Info is a string column")
                .clone();
            (column.is_null(0), column.value(1).to_string())
        }
        let (idle_is_null, truncated_info) = info(&truncated);
        let (_, full_info) = info(&full);
        assert!(idle_is_null, "an idle session reports no statement");
        assert_eq!(truncated_info.chars().count(), PROCESS_LIST_INFO_LIMIT);
        assert!(full_info.chars().count() > PROCESS_LIST_INFO_LIMIT);
        assert_eq!(full_info, text);
    }

    #[test]
    fn full_process_list_preallocates_long_text_without_geometric_value_slack() {
        use arrow::array::{StringArray, StringBuilder};
        use novarocks_query_application::session_control::SessionProcess;

        let text: Arc<str> = Arc::from("x".repeat(65_537));
        let processes = (0..3)
            .map(|connection_id| SessionProcess {
                connection_id,
                principal: Arc::from("root"),
                elapsed: Duration::from_secs(1),
                statement: Some(Arc::clone(&text)),
            })
            .collect();
        let result = process_list_result(processes, true).unwrap();
        let info = result.batches[0]
            .column(7)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let requested = 3 * text.len();
        // The public preallocated builder is the capacity oracle; do not
        // assume Arrow's alignment or allocator rounding in product code.
        let reserved = StringBuilder::with_capacity(3, requested).finish();
        assert_eq!(info.values().capacity(), reserved.values().capacity());
        for row in 0..3 {
            assert_eq!(info.value(row), text.as_ref());
        }
        assert_eq!(result.columns[0].data_type(), &DataType::Int64);
        assert_eq!(result.columns[5].data_type(), &DataType::Int64);
        // This workload exercises the old iterator's replacement buffer,
        // rather than merely restating the current constructor.
        let iterator_built: StringArray = std::iter::repeat_n(Some(text.as_ref()), 3).collect();
        assert!(iterator_built.values().capacity() > info.values().capacity());
    }

    #[test]
    fn full_process_list_beyond_the_local_bound_is_refused_before_building() {
        use novarocks_query_application::api::LocalResultBound;
        use novarocks_query_application::session_control::SessionProcess;
        use std::time::Duration;

        // Every session shares one large running statement; FULL output would
        // carry all of them, the truncated output stays small.
        let statement: Arc<str> = Arc::from("x".repeat(1024 * 1024).as_str());
        let sessions = LocalResultBound::V1.bytes / (1024 * 1024) + 1;
        let processes = (0..sessions)
            .map(|index| SessionProcess {
                connection_id: u32::try_from(index).expect("small id"),
                principal: Arc::from("root"),
                elapsed: Duration::from_secs(1),
                statement: Some(Arc::clone(&statement)),
            })
            .collect::<Vec<_>>();
        let error = process_list_result(processes.clone(), true).expect_err("FULL is too large");
        assert!(error.contains("byte bound"), "{error}");
        assert_eq!(
            process_list_result(processes, false)
                .expect("truncated output")
                .row_count(),
            sessions
        );
    }

    fn scalar_field(nullable: bool) -> ResultField {
        ResultField::new(
            "value",
            novarocks_type_contract::FunctionValueType::new(DataType::Int64, nullable),
        )
    }

    fn relayed_scalar_fixture() -> (
        ResultStreamTestProducer,
        novarocks_query_application::api::ExecutionHandle,
        novarocks_query_application::test_support::TestResultDeliveryReceipt,
        novarocks_types::QueryExecutionId,
        novarocks_result_contract::ScalarSchema,
    ) {
        use novarocks_result_contract::{
            InternalResultDomain, RootOutputKind, ScalarField, ScalarSchema, ScalarValueType,
        };
        let execution_id = novarocks_types::QueryExecutionId::new(
            novarocks_types::QueryId::new(71, 1),
            novarocks_types::AttemptId::new(1).unwrap(),
        )
        .unwrap();
        let schema = ScalarSchema::try_new(ScalarField {
            nullable: true,
            value_type: ScalarValueType::SignedInteger(64),
        })
        .unwrap();
        let (producer, execution, _resources, receipt) =
            ResultStreamTestProducer::open_with_carrier(
                execution_id,
                vec![scalar_field(true)],
                1,
                ResourceConfig {
                    total_bytes: 1 << 20,
                    control_bytes: 1024,
                    per_scope_bytes: (1 << 20) - 1024,
                },
                novarocks_query_application::api::ResultRowCarrier::relayed(
                    RootOutputKind::InternalFacts(InternalResultDomain::ScalarValueV1),
                    None,
                )
                .unwrap(),
            )
            .unwrap();
        (producer, execution, receipt, execution_id, schema)
    }

    fn scalar_root_reply(
        execution_id: novarocks_types::QueryExecutionId,
        schema: &novarocks_result_contract::ScalarSchema,
        leaf: novarocks_result_contract::BorrowedScalarLeaf<'_>,
        sequence: u64,
    ) -> (
        novarocks_execution_contract::root_result::RootResultReply,
        u64,
    ) {
        use novarocks_execution_contract::{
            TaskIdentity,
            root_result::{RootReadOutcome, RootResultData, RootResultReply},
        };
        use novarocks_result_contract::{
            InternalResultDomain, RootOutputKind, RootProfileId, ScalarLeafCursor,
        };
        use novarocks_types::{StageId, TaskId};
        let cursor = ScalarLeafCursor::try_new(schema, leaf).unwrap();
        let rows = cursor.rows();
        let mut body = vec![0; cursor.encoded_len()];
        cursor.copy_range(0, &mut body).unwrap();
        let kind = RootOutputKind::InternalFacts(InternalResultDomain::ScalarValueV1);
        (
            RootResultReply {
                root_task: TaskIdentity::new(
                    execution_id,
                    StageId::new(1).unwrap(),
                    TaskId::new(1).unwrap(),
                    novarocks_types::BackendProcessId::new_v7(),
                ),
                profile: RootProfileId::V1,
                kind,
                accepted_consumed: 0,
                outcome: RootReadOutcome::Data(
                    RootResultData::try_new(
                        kind,
                        std::num::NonZeroU64::new(sequence).unwrap(),
                        bytes::Bytes::from(body),
                        None,
                    )
                    .unwrap(),
                ),
            },
            rows,
        )
    }

    #[tokio::test]
    async fn governed_scalar_stream_consumes_typed_value_null_and_no_rows_after_success_end() {
        use novarocks_result_contract::BorrowedScalarLeaf as V;
        for (leaf, expected) in [
            (V::SignedInteger { bits: 64, value: 7 }, "7"),
            (V::Null, "NULL"),
            (V::NoRows, "null"),
        ] {
            let (producer, mut execution, schema_receipt, execution_id, schema) =
                relayed_scalar_fixture();
            let ExecutionOutput::Rows(stream) = execution.take_output().unwrap() else {
                panic!("rows");
            };
            let capacity = producer.result_capacity();
            let (reply, rows) = scalar_root_reply(execution_id, &schema, leaf, 1);
            let produce = async {
                assert_eq!(
                    schema_receipt.wait().await,
                    TestResultDeliveryDisposition::Completed
                );
                assert_eq!(
                    producer
                        .enqueue_segment(0, reply, None, rows)
                        .await
                        .unwrap()
                        .wait()
                        .await,
                    TestResultDeliveryDisposition::Completed
                );
                assert_eq!(
                    producer.enqueue_end(1).await.wait().await,
                    TestResultDeliveryDisposition::Completed
                );
                assert_eq!(capacity.snapshot().held_positions, [0, 0, 1, 0]);
                producer.finish();
                assert_eq!(capacity.snapshot().held_positions, [0; 4]);
            };
            let (result, ()) = tokio::join!(
                consume_governed_scalar_stream(&mut execution, stream, Some(&schema)),
                produce
            );
            assert_eq!(result.unwrap(), expected);
        }
    }

    #[tokio::test]
    async fn governed_scalar_stream_refuses_missing_duplicate_foreign_and_late_failed_records() {
        use novarocks_result_contract::{BorrowedScalarLeaf, InternalResultDomain, RootOutputKind};
        for fault in ["missing", "duplicate", "foreign", "late"] {
            let (producer, mut execution, schema_receipt, execution_id, schema) =
                relayed_scalar_fixture();
            let ExecutionOutput::Rows(stream) = execution.take_output().unwrap() else {
                panic!("rows");
            };
            let produce = async {
                assert_eq!(
                    schema_receipt.wait().await,
                    TestResultDeliveryDisposition::Completed
                );
                if fault == "missing" {
                    assert!(matches!(
                        producer.enqueue_end(0).await.wait().await,
                        TestResultDeliveryDisposition::Failed(_)
                    ));
                } else {
                    let (mut reply, rows) =
                        scalar_root_reply(execution_id, &schema, BorrowedScalarLeaf::NoRows, 1);
                    if fault == "foreign" {
                        reply.kind = RootOutputKind::InternalFacts(
                            InternalResultDomain::StatisticsArtifactV1,
                        );
                        let novarocks_execution_contract::root_result::RootReadOutcome::Data(data) =
                            &reply.outcome
                        else {
                            panic!("data");
                        };
                        reply.outcome =
                            novarocks_execution_contract::root_result::RootReadOutcome::Data(
                                novarocks_execution_contract::root_result::RootResultData::try_new(
                                    reply.kind,
                                    data.sequence(),
                                    data.body().clone(),
                                    None,
                                )
                                .unwrap(),
                            );
                    }
                    let receipt = producer
                        .enqueue_segment(0, reply, None, rows)
                        .await
                        .unwrap()
                        .wait()
                        .await;
                    if fault == "foreign" {
                        assert!(matches!(receipt, TestResultDeliveryDisposition::Failed(_)));
                    } else {
                        assert_eq!(receipt, TestResultDeliveryDisposition::Completed);
                        if fault == "duplicate" {
                            let (reply, rows) = scalar_root_reply(
                                execution_id,
                                &schema,
                                BorrowedScalarLeaf::NoRows,
                                2,
                            );
                            assert!(matches!(
                                producer
                                    .enqueue_segment(1, reply, None, rows)
                                    .await
                                    .unwrap()
                                    .wait()
                                    .await,
                                TestResultDeliveryDisposition::Failed(_)
                            ));
                        } else {
                            producer.fail(QueryExecutionError::new(
                                QueryExecutionErrorKind::Failed,
                                "late scalar failure",
                            ));
                        }
                    }
                }
                producer.finish();
            };
            let (result, ()) = tokio::join!(
                consume_governed_scalar_stream(&mut execution, stream, Some(&schema)),
                produce
            );
            assert!(result.is_err(), "{fault}");
        }
    }

    #[tokio::test]
    async fn governed_scalar_stream_rejects_foreign_record_row_count_and_cancels_execution() {
        let (producer, mut execution, schema_receipt, execution_id, schema) =
            relayed_scalar_fixture();
        let capacity = producer.result_capacity();
        let ExecutionOutput::Rows(stream) = execution.take_output().unwrap() else {
            panic!("scalar rows");
        };
        let (reply, _) = scalar_root_reply(
            execution_id,
            &schema,
            novarocks_result_contract::BorrowedScalarLeaf::NoRows,
            1,
        );
        let producer_side = async {
            assert_eq!(
                schema_receipt.wait().await,
                TestResultDeliveryDisposition::Completed
            );
            let receipt = producer.enqueue_segment(0, reply, None, 2).await.unwrap();
            assert!(matches!(
                receipt.wait().await,
                TestResultDeliveryDisposition::Failed(_)
            ));
        };
        let (result, ()) = tokio::join!(
            consume_governed_scalar_stream(&mut execution, stream, Some(&schema)),
            producer_side
        );
        let error = result.expect_err("record and delivery row counts must agree");
        assert_eq!(error.kind(), QueryServiceErrorKind::InvalidValue);
        assert_eq!(
            producer.cancellation_reason(),
            Some(WorkCancellationReason::Requested)
        );
        assert_eq!(capacity.snapshot().held_positions, [0, 0, 1, 0]);
        producer.finish();
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }

    #[tokio::test]
    async fn governed_scalar_stream_rejects_schema_and_fails_its_delivery() {
        let fields = vec![scalar_field(false), scalar_field(false)];
        let (producer, mut execution, schema_receipt) = scalar_stream_fixture(fields);
        let ExecutionOutput::Rows(stream) = execution.take_output().expect("scalar output") else {
            panic!("expected row output")
        };

        let error = consume_governed_scalar_stream(&mut execution, stream, None)
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
        let (producer, mut execution, schema_receipt, _execution_id, schema) =
            relayed_scalar_fixture();
        let ExecutionOutput::Rows(stream) = execution.take_output().expect("scalar output") else {
            panic!("expected row output")
        };
        producer.fail(QueryExecutionError::new(
            QueryExecutionErrorKind::Failed,
            "test scalar stream failure",
        ));

        let error = consume_governed_scalar_stream(&mut execution, stream, Some(&schema))
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
            RuntimeEndpoint::new("control-0.test.invalid", 19061).expect("test control endpoint"),
            "test-deployment",
            "test-build",
            novarocks_types::NativeCompatibilityId::new([0x71; 32]),
            4096,
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

    #[test]
    fn prepared_distributed_failure_route_keeps_selected_task_and_root_input() {
        use crate::query_execution::contract::DistributedQueryError;
        use novarocks_execution_contract::{SafeDetail, TaskFailurePhase, TaskIdentity};
        use novarocks_failure_contract::FailureProjection;
        use novarocks_query_application::execution_error::{
            ExecutionFailureInput, ExecutionFailureSite, QueryFailureReason,
        };
        use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
        use novarocks_types::{BackendProcessId, StageId, TaskId};

        let identity = TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(91, 8), AttemptId::new(1).unwrap()).unwrap(),
            StageId::new(2).unwrap(),
            TaskId::new(3).unwrap(),
            BackendProcessId::new_v7(),
        );
        let task_input = ExecutionFailureInput::new(
            ConnectorError::new(ConnectorErrorKind::Internal, "private selected Task cause")
                .failure_fact(),
            SafeDetail::new("native operation failed").unwrap(),
            ExecutionFailureSite::Task {
                identity,
                phase: TaskFailurePhase::Execution,
            },
        );
        let root_input = ExecutionFailureInput::new(
            QueryFailureReason::DeadlineExceeded.failure_fact(),
            SafeDetail::new("root read reached its original deadline").unwrap(),
            ExecutionFailureSite::RootRead { root: identity },
        );
        for (error, expected) in [
            (
                DistributedQueryError::from_execution(task_input.clone()),
                task_input,
            ),
            (
                DistributedQueryError::from_execution(root_input.clone()),
                root_input,
            ),
        ] {
            // Exercise the existing typed carrier's service projection and
            // caller fallback, without driving a Native attempt.
            let routed = routed_distributed_query_failure(error);
            let service = selected_execution_or_worker_fallback(
                Err(routed),
                cancellation_error(QueryCancellationReason::ExplicitKill {
                    requester_session:
                        novarocks_query_application::session_control::ClientSessionKey::new(7)
                            .expect("nonzero fixture requester"),
                }),
            );
            assert_eq!(service.execution_failure(), Some(&expected));
            assert_eq!(service.user_error(), Some(&expected.user_error()));
            assert!(!service.message().contains("private"));
            assert!(service.compile_control_error().is_none());
        }
    }

    #[test]
    fn prepared_distributed_failure_route_uses_original_domain_and_control_projection() {
        use crate::query_execution::contract::{DistributedQueryError, DistributedQueryErrorKind};
        use novarocks_failure_contract::{FailureProjection, SharedFailureCause};
        use novarocks_query_application::execution_error::{
            ExecutionFailureSite, QueryFailureReason,
        };
        use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
        use novarocks_type_contract::owned_resources::layout::LayoutResourceError;

        let source = ConnectorError::new(ConnectorErrorKind::Unavailable, "private provider cause");
        let expected = source.failure_fact();
        let service = routed_execution_service_error(routed_distributed_query_failure(
            DistributedQueryError::from_source(
                DistributedQueryErrorKind::Failed,
                SharedFailureCause::new(source),
            ),
        ));
        let input = service.execution_failure().unwrap();
        assert_eq!(input.fact(), expected);
        assert_eq!(input.site(), ExecutionFailureSite::Coordinator);
        assert!(!service.message().contains("private"));

        let raw = LayoutResourceError::SourceModel;
        let control = CompileControlError::from_domain(raw);
        let service = routed_execution_service_error(routed_distributed_query_failure(
            DistributedQueryError::from_encode(novarocks_plan_codec::PhysicalEncodeError::Control(
                control.clone(),
            )),
        ));
        assert_eq!(
            service.execution_failure().unwrap().fact(),
            control.failure_fact()
        );
        assert_eq!(
            service.execution_failure().unwrap().site(),
            ExecutionFailureSite::Coordinator
        );
        assert_eq!(service.compile_control_error(), Some(control));
        let Some(CompileControlError::Source(source)) = service.compile_control_error() else {
            panic!("the original control source must survive the routed boundary")
        };
        assert_eq!(source.downcast_ref::<LayoutResourceError>(), Some(&raw));

        let service = routed_execution_service_error(routed_distributed_query_failure(
            DistributedQueryError::new(DistributedQueryErrorKind::Failed, "private legacy detail"),
        ));
        assert_eq!(
            service.execution_failure().unwrap().fact(),
            QueryFailureReason::PreparationFailed.failure_fact(),
        );
        assert!(!service.message().contains("private"));
    }

    #[test]
    fn caller_cancellation_keeps_publication_verdict_without_replacing_selected_input() {
        use novarocks_spi::connector::{
            LakePublicationDisposition, LakePublicationFamily, LakePublicationId,
            LakePublicationMarkerHeader, LakePublicationNextAction, LakePublicationTarget,
            LakePublicationTerminal,
        };
        use novarocks_type_contract::owned_resources::layout::LayoutResourceError;

        let control = CompileControlError::from_domain(LayoutResourceError::SourceModel);
        for reason in [
            QueryCancellationReason::DeadlineExceeded { timeout_ms: 1 },
            QueryCancellationReason::ExplicitKill {
                requester_session:
                    novarocks_query_application::session_control::ClientSessionKey::new(7)
                        .expect("nonzero fixture requester"),
            },
        ] {
            for disposition in [
                LakePublicationDisposition::KnownUncommitted,
                LakePublicationDisposition::CommitUnknown,
                LakePublicationDisposition::KnownCommitted,
            ] {
                let terminal = LakePublicationTerminal::new(
                    LakePublicationMarkerHeader::new(
                        LakePublicationId::new_v7(),
                        LakePublicationFamily::Write,
                    ),
                    LakePublicationTarget::try_new(
                        "private-catalog".into(),
                        "private-namespace".into(),
                        Some("private-table".into()),
                        None,
                    )
                    .unwrap(),
                    disposition,
                    if disposition.do_not_retry() {
                        LakePublicationNextAction::InspectPublishedState
                    } else {
                        LakePublicationNextAction::RetryStatement
                    },
                    None,
                );
                let expected = cancellation_error(reason.clone());
                let service = selected_execution_or_worker_fallback(
                    Err(RoutedExecutionError::Publication {
                        message: "private provider diagnostic must not become the cause".into(),
                        terminal: terminal.clone(),
                        compile_control: Some(control.clone()),
                    }),
                    cancellation_error(reason.clone()),
                );
                assert_eq!(service.execution_failure(), expected.execution_failure());
                assert_eq!(service.user_error(), expected.user_error());
                assert_eq!(service.publication_terminal(), Some(&terminal));
                assert_eq!(service.compile_control_error(), Some(control.clone()));
                assert_eq!(
                    service
                        .message()
                        .contains("do_not_retry=true; do not retry statement"),
                    disposition.do_not_retry(),
                );
                assert!(!service.message().contains("private"));
            }
            let service = selected_execution_or_worker_fallback(
                Err(RoutedExecutionError::Control(control.clone())),
                cancellation_error(reason.clone()),
            );
            assert_eq!(
                service.execution_failure(),
                cancellation_error(reason).execution_failure()
            );
            assert_eq!(service.compile_control_error(), Some(control.clone()));
        }
    }

    #[test]
    fn selected_task_execution_survives_dml_publication_and_sql_descriptor() {
        use novarocks_execution_contract::{SafeDetail, TaskFailurePhase, identity::TaskIdentity};
        use novarocks_failure_contract::FailureProjection;
        use novarocks_query_application::execution_error::{
            ExecutionFailureInput, ExecutionFailureSite,
        };
        use novarocks_spi::connector::{
            ConnectorError, ConnectorErrorKind, LakePublicationFamily, LakePublicationId,
            LakePublicationTarget,
        };
        use novarocks_types::{
            AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
        };
        let identity = TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(91, 7), AttemptId::new(1).unwrap()).unwrap(),
            StageId::new(2).unwrap(),
            TaskId::new(3).unwrap(),
            BackendProcessId::new_v7(),
        );
        let source =
            ConnectorError::new(ConnectorErrorKind::Internal, "private provider diagnostic");
        let input = ExecutionFailureInput::new(
            source.failure_fact(),
            SafeDetail::new("Unexpected failure (reason 4:10)").unwrap(),
            ExecutionFailureSite::Task {
                identity,
                phase: TaskFailurePhase::Execution,
            },
        );
        let target = LakePublicationTarget::try_new(
            "catalog".into(),
            "namespace".into(),
            Some("table".into()),
            None,
        )
        .unwrap();
        let mut attempt = crate::dml::attempt::DmlPublicationAttempt::new(
            LakePublicationId::new_v7(),
            LakePublicationFamily::Write,
            target,
            None,
        );
        attempt.terminal_pre_dispatch_uncommitted().unwrap();
        let terminal = attempt.terminal().unwrap().clone();
        let control = novarocks_type_contract::CompileControlError::ResourceExhausted;
        let error = crate::dml::error::DmlExecutionError::from_distributed(
            crate::query_execution::contract::DistributedQueryError::from_execution(input.clone()),
        )
        .into_dml_error(None)
        .with_publication_terminal(terminal.clone())
        .with_compile_control(Some(control.clone()));
        let routed = dml_result::<()>(Err(error)).unwrap_err();
        let service = routed_execution_service_error(routed);
        assert_eq!(service.execution_failure(), Some(&input));
        assert_eq!(service.publication_terminal(), Some(&terminal));
        assert_eq!(service.compile_control_error(), Some(control));
        let descriptor_message = "[query.execution.unexpected] Unexpected failure (reason 4:10)";
        assert_eq!(service.user_error(), Some(&input.user_error()));
        assert_eq!(
            service.user_error().unwrap().to_string(),
            descriptor_message
        );
        assert_eq!(
            service.message(),
            format!(
                "{descriptor_message} (lake publication id={} family=write disposition=KnownUncommitted)",
                terminal.header().publication_id(),
            ),
        );
        assert!(!terminal.do_not_retry());
        assert!(!service.message().contains("do_not_retry"));
        assert!(service.external_effect_disposition().is_none());
        assert!(!service.message().contains("private"));
        assert!(!service.message().contains("Executor:"));
        let delayed = selected_execution_or_worker_fallback(
            Err(RoutedExecutionError::Execution {
                input: Box::new(input.clone()),
                terminal: Some(terminal.clone()),
                compile_control: None,
                external_effect_disposition: None,
            }),
            cancellation_error(QueryCancellationReason::DeadlineExceeded { timeout_ms: 1 }),
        );
        assert_eq!(delayed.execution_failure(), Some(&input));
        assert_eq!(delayed.publication_terminal(), Some(&terminal));
        assert_eq!(delayed.message(), service.message());
        let legacy = selected_execution_or_worker_fallback(
            Err(RoutedExecutionError::Engine("legacy worker stopped".into())),
            cancellation_error(QueryCancellationReason::DeadlineExceeded { timeout_ms: 1 }),
        );
        let fallback =
            cancellation_error(QueryCancellationReason::DeadlineExceeded { timeout_ms: 1 });
        assert_eq!(legacy.execution_failure(), fallback.execution_failure());
        assert!(legacy.execution_failure().is_some());
        assert_eq!(legacy.kind(), QueryServiceErrorKind::Timeout);
        assert_eq!(legacy.message(), fallback.message());
    }

    #[test]
    fn original_control_failure_preserves_publication_verdict_and_is_not_cancellation() {
        use novarocks_failure_contract::FailureProjection;
        use novarocks_spi::connector::{
            LakePublicationFamily, LakePublicationId, LakePublicationTarget,
        };
        use novarocks_type_contract::{
            CompileControlError, owned_resources::layout::LayoutResourceError,
        };
        let target = LakePublicationTarget::try_new(
            "catalog".into(),
            "namespace".into(),
            Some("table".into()),
            None,
        )
        .unwrap();
        let mut attempt = crate::dml::attempt::DmlPublicationAttempt::new(
            LakePublicationId::new_v7(),
            LakePublicationFamily::DataMutation,
            target,
            None,
        );
        attempt.terminal_pre_dispatch_uncommitted().unwrap();
        let terminal = attempt.terminal().unwrap().clone();
        for raw in [
            LayoutResourceError::SourceModel,
            LayoutResourceError::Arithmetic,
        ] {
            let expected = raw.failure_fact();
            let cause = CompileControlError::from_domain(raw);
            let error = crate::dml::error::DmlError::compile_control(cause.clone())
                .with_publication_terminal(terminal.clone());
            let result = dml_result::<()>(Err(error));
            let Err(RoutedExecutionError::Execution {
                input,
                terminal: actual,
                compile_control,
                external_effect_disposition,
            }) = result
            else {
                panic!("the originating input, publication and rich source must survive")
            };
            assert_eq!(input.fact(), expected);
            assert_eq!(
                input.site(),
                novarocks_query_application::execution_error::ExecutionFailureSite::Coordinator
            );
            assert_eq!(actual, Some(terminal.clone()));
            assert_eq!(compile_control, Some(cause.clone()));
            let service = routed_execution_service_error(RoutedExecutionError::Execution {
                input: input.clone(),
                terminal: actual,
                compile_control,
                external_effect_disposition,
            });
            assert_eq!(service.execution_failure(), Some(input.as_ref()));
            assert_eq!(service.user_error(), Some(&input.user_error()));
            assert_ne!(service.kind(), QueryServiceErrorKind::Interrupted);
            assert_ne!(service.kind(), QueryServiceErrorKind::Timeout);
            assert_eq!(service.publication_terminal(), Some(&terminal));
            assert_eq!(service.compile_control_error(), Some(cause.clone()));
            assert_eq!(
                service.compile_control_error().unwrap().failure_fact(),
                expected
            );
            let service = routed_execution_service_error(routed_compiler_failure(
                FrontendQueryCompilerError::Control(cause.clone()),
                "SELECT value",
            ));
            assert_eq!(service.compile_control_error(), Some(cause));
        }
    }

    #[test]
    fn cow_dml_route_keeps_publication_terminal_and_original_client_prefix() {
        use crate::query_execution::dml::mutation_flow::CowFailure;
        use novarocks_spi::connector::{
            ConnectorError, ConnectorErrorKind, LakePublicationFamily, LakePublicationId,
            LakePublicationTarget,
        };
        let target = LakePublicationTarget::try_new(
            "catalog".to_owned(),
            "namespace".to_owned(),
            Some("table".to_owned()),
            None,
        )
        .unwrap();
        let mut attempt = crate::dml::attempt::DmlPublicationAttempt::new(
            LakePublicationId::new_v7(),
            LakePublicationFamily::DataMutation,
            target,
            None,
        );
        attempt.terminal_pre_dispatch_uncommitted().unwrap();
        let terminal = attempt.terminal().unwrap().clone();
        let original = ConnectorError::new(ConnectorErrorKind::Unavailable, "first provider cause")
            .with_retryable_before_progress();
        let legacy_message = crate::dml::error::DmlError::executor(format!(
            "begin connector write session: {original}"
        ))
        .with_publication_terminal(terminal.clone())
        .to_string();
        let error = crate::dml::error::DmlExecutionError::Cow(CowFailure::provider(original))
            .into_dml_error(None)
            .with_publication_terminal(terminal.clone());
        let result = dml_result::<()>(Err(error));
        match result {
            Err(RoutedExecutionError::Publication {
                message,
                terminal: actual,
                compile_control: None,
            }) => {
                assert_eq!(actual, terminal);
                assert_eq!(message, legacy_message);
                let service = QueryServiceError::with_publication_terminal(message, actual);
                assert_eq!(service.kind(), QueryServiceErrorKind::Internal);
                assert_eq!(service.publication_terminal(), Some(&terminal));
            }
            _ => panic!("COW begin must retain the original publication terminal"),
        }
    }

    async fn assert_cow_raw_exit_on_original_worker(abandon_waiter: bool) {
        use crate::query_execution::dml::mutation_flow::CowFailure;
        use novarocks_query_application::cpu::{
            QueryBlockingExecutorConfig, QueryBlockingExecutorOwner,
        };
        use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
        use novarocks_workload_control::{ResultCapacityConfig, WorkloadConfig, WorkloadControl};
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
        let root = workload
            .root_admission()
            .begin_query_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let (permit, window) = root
            .owner
            .scope()
            .admit_query_with_result(ResultWindowClass::Internal)
            .unwrap()
            .await
            .unwrap();
        let cancellation =
            QueryCancellationView::governed(root.owner.scope().cancellation().unwrap(), None);
        let mut blocking = QueryBlockingExecutorOwner::try_new(QueryBlockingExecutorConfig::new(
            std::num::NonZeroUsize::new(1).unwrap(),
            std::num::NonZeroUsize::new(1).unwrap(),
        ))
        .unwrap();
        let executor = blocking.executor();
        let alias = window.retain_alias();
        let gate = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let original_gate = gate.clone();
        let (entered, observed) = tokio::sync::oneshot::channel();
        let calling_thread = std::thread::current().id();
        let failure = CowFailure::provider(ConnectorError::new(
            ConnectorErrorKind::Cancelled,
            "original typed provider failure",
        ))
        .with_exit_probe(move || {
            // CowFailure's final test-only field runs after its raw cause dropped.
            // It holds the original worker before the tuple receipt is published.
            let _ = entered.send(std::thread::current().id());
            let (open, changed) = &*original_gate;
            let (open, _) = changed
                .wait_timeout_while(open.lock().unwrap(), Duration::from_secs(5), |open| !*open)
                .unwrap();
            assert!(
                *open,
                "original cause-exit probe must be released by cleanup"
            );
        });
        let caller = tokio::spawn(async move {
            execute_synchronous_stage(executor, cancellation, root.owner, Some(alias), move || {
                dml_result::<()>(Err(
                    crate::dml::error::DmlExecutionError::Cow(failure).into_dml_error(None)
                ))
            })
            .await
        });
        let exit_thread = tokio::time::timeout(Duration::from_secs(2), observed).await;
        if abandon_waiter {
            caller.abort();
        }
        drop(permit);
        drop(window);
        let held_during_raw_exit = capacity.snapshot().held_positions;
        // Cleanup runs before any assertion, including an observation failure.
        let (open, changed) = &*gate;
        *open.lock().unwrap() = true;
        changed.notify_all();
        let returned = caller.await;
        let mut message = None;
        let mut caller_was_cancelled = false;
        match returned {
            Ok(Ok((result, owner, alias))) => {
                if let Err(RoutedExecutionError::Engine(presentation)) = result {
                    message = Some(presentation);
                }
                owner.complete();
                drop(alias);
            }
            Err(error) => {
                caller_was_cancelled = error.is_cancelled();
            }
            Ok(Err(_)) => {}
        }
        let shutdown = blocking
            .shutdown_until(Instant::now() + Duration::from_secs(2))
            .await;
        assert!(shutdown.is_ok(), "join the original bounded command worker");
        let exit_thread = exit_thread
            .expect("raw exit observed")
            .expect("raw exit sender completes");
        assert_ne!(exit_thread, calling_thread);
        assert_eq!(held_during_raw_exit, [0, 0, 1, 0]);
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        if abandon_waiter {
            assert!(
                caller_was_cancelled,
                "original waiter actually joins as cancelled"
            );
        } else {
            assert_eq!(
                message.as_deref(),
                Some(
                    "Executor: begin connector write session: Cancelled: original typed provider failure"
                )
            );
        }
    }
    #[tokio::test]
    async fn cow_raw_failure_exits_on_original_command_worker_before_receipt() {
        assert_cow_raw_exit_on_original_worker(false).await;
    }
    #[tokio::test]
    async fn abandoned_cow_waiter_cannot_release_original_window_during_raw_exit() {
        assert_cow_raw_exit_on_original_worker(true).await;
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

    async fn assert_abandoned_synchronous_window() {
        use novarocks_query_application::cpu::{
            QueryBlockingExecutorConfig, QueryBlockingExecutorOwner,
        };
        use novarocks_workload_control::{ResultCapacityConfig, WorkloadConfig, WorkloadControl};

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
        let (root, window) = workload
            .root_admission()
            .try_begin_root_with_result(
                WorkRequest::new(WorkClass::Management),
                ResultWindowClass::Local,
            )
            .unwrap();
        let (owner, business) = (root.owner, Some(root.business));
        let cancellation =
            QueryCancellationView::governed(owner.scope().cancellation().unwrap(), None);
        let mut blocking = QueryBlockingExecutorOwner::try_new(QueryBlockingExecutorConfig::new(
            std::num::NonZeroUsize::new(1).unwrap(),
            std::num::NonZeroUsize::new(1).unwrap(),
        ))
        .unwrap();
        let executor = blocking.executor();
        let alias = window.retain_alias();
        let gate = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let worker_gate = Arc::clone(&gate);
        let (started, running) = tokio::sync::oneshot::channel();
        let caller = tokio::spawn(async move {
            let produce = move || {
                started.send(()).unwrap();
                let (open, changed) = &*worker_gate;
                let (open, _) = changed
                    .wait_timeout_while(open.lock().unwrap(), Duration::from_secs(5), |open| !*open)
                    .unwrap();
                assert!(*open, "test worker gate timed out");
                Ok(StatementResult::Ok)
            };
            execute_synchronous_stage(executor, cancellation, owner, Some(alias), produce)
                .await
                .map(|(result, owner, _window)| (result, owner))
        });
        tokio::time::timeout(Duration::from_secs(2), running)
            .await
            .unwrap()
            .unwrap();
        if let Some(business) = business {
            business.release();
        }
        drop(window);
        caller.abort();
        assert!(matches!(caller.await, Err(error) if error.is_cancelled()));
        assert_eq!(capacity.snapshot().held_positions, [0, 1, 0, 0]);
        let (open, changed) = &*gate;
        *open.lock().unwrap() = true;
        changed.notify_all();
        blocking
            .shutdown_until(Instant::now() + Duration::from_secs(2))
            .await
            .unwrap();
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }

    #[tokio::test]
    async fn abandoned_synchronous_command_retains_its_window_until_actual_worker_exit() {
        assert_abandoned_synchronous_window().await;
    }

    async fn assert_cancelled_cpu_window_until_worker_exit(class: ResultWindowClass) {
        use novarocks_query_application::cpu::{QueryCpuExecutorConfig, QueryCpuExecutorOwner};
        use novarocks_workload_control::{ResultCapacityConfig, WorkloadConfig, WorkloadControl};

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
        let root = workload
            .root_admission()
            .begin_query_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let (permit, window) = root
            .owner
            .scope()
            .admit_query_with_result(class)
            .unwrap()
            .await
            .unwrap();
        let source = QueryCancellationSource::new();
        let cancellation = source.view();
        let mut cpu = QueryCpuExecutorOwner::try_new(QueryCpuExecutorConfig::with_idle_keepalive(
            Duration::from_secs(1),
        ))
        .unwrap();
        let executor = cpu.executor();
        let alias = window.retain_alias();
        let gate = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let worker_gate = Arc::clone(&gate);
        let (started, running) = tokio::sync::oneshot::channel();
        let caller = tokio::spawn(async move {
            executor
                .run_cancellable(cancellation, move || {
                    started.send(()).unwrap();
                    let (open, changed) = &*worker_gate;
                    let (open, _) = changed
                        .wait_timeout_while(open.lock().unwrap(), Duration::from_secs(5), |open| {
                            !*open
                        })
                        .unwrap();
                    assert!(*open, "test compiler gate timed out");
                    (StatementResult::Ok, Some(alias))
                })
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), running)
            .await
            .unwrap()
            .unwrap();
        source.request(QueryCancellationReason::ClientDisconnected);
        assert!(matches!(
            caller.await.unwrap(),
            Err(QueryCpuRunError::Cancelled(
                QueryCancellationReason::ClientDisconnected
            ))
        ));
        drop(permit);
        root.owner.complete();
        drop(window);
        let expected = match class {
            ResultWindowClass::Client => [1, 0, 0, 0],
            ResultWindowClass::Local => [0, 1, 0, 0],
            ResultWindowClass::Internal => [0, 0, 1, 0],
            ResultWindowClass::Closing => unreachable!("preparation cannot enter Closing"),
        };
        assert_eq!(capacity.snapshot().held_positions, expected);
        let (open, changed) = &*gate;
        *open.lock().unwrap() = true;
        changed.notify_all();
        cpu.shutdown_until(Instant::now() + Duration::from_secs(2))
            .await
            .unwrap();
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }

    #[tokio::test]
    async fn cancelled_cpu_work_keeps_internal_window_until_worker_exit() {
        assert_cancelled_cpu_window_until_worker_exit(ResultWindowClass::Internal).await;
    }

    #[tokio::test]
    async fn cancelled_cpu_work_keeps_client_window_until_worker_exit() {
        assert_cancelled_cpu_window_until_worker_exit(ResultWindowClass::Client).await;
    }

    #[tokio::test]
    async fn cancelled_cpu_work_keeps_local_window_until_worker_exit() {
        assert_cancelled_cpu_window_until_worker_exit(ResultWindowClass::Local).await;
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
            requester_session: 91,
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
                requester_session:
                    novarocks_query_application::session_control::ClientSessionKey::new(8)
                        .expect("nonzero fixture requester")
            })
            .kind(),
            QueryServiceErrorKind::Interrupted
        );
    }

    #[test]
    fn preparation_errors_keep_timeout_distinct_from_unavailability() {
        let cancellation =
            novarocks_query_application::cancellation::QueryCancellationSource::new().view();
        assert_eq!(
            QueryServiceError::new(QueryServiceErrorKind::Timeout, "deadline elapsed")
                .reconcile_local_stop_observation(&cancellation)
                .kind(),
            QueryServiceErrorKind::Timeout
        );
        assert_eq!(
            QueryServiceError::new(QueryServiceErrorKind::Unavailable, "topology unavailable")
                .reconcile_local_stop_observation(&cancellation)
                .kind(),
            QueryServiceErrorKind::Unavailable
        );
    }

    #[test]
    fn only_kill_query_fences_the_successor_statement() {
        assert!(cancellation_requires_statement_fence(
            &QueryCancellationReason::ExplicitKill {
                requester_session:
                    novarocks_query_application::session_control::ClientSessionKey::new(8)
                        .expect("nonzero fixture requester"),
            }
        ));
        for reason in [
            QueryCancellationReason::ExplicitKillConnection {
                requester_session:
                    novarocks_query_application::session_control::ClientSessionKey::new(8)
                        .expect("nonzero fixture requester"),
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

    #[test]
    fn execution_throw_policy_uses_the_same_effective_mode_as_sql_analysis() {
        let session = novarocks_sql::sql_mode::SqlSemanticSettings::default().with_sql_mode(
            novarocks_sql::sql_mode::SqlMode::from_assignment(
                "GROUP_CONCAT_LEGACY,ALLOW_THROW_EXCEPTION",
            ),
        );
        let statements = novarocks_parser::parse(
            "SELECT /*+ SET_VAR(sql_mode=68719477248) */ /*+ SET_VAR(sql_mode=32) */ \
             /*+ SET_VAR(query_mem_limit=9) */ 1",
        )
        .unwrap();
        let [ParsedStatement::Query(query)] = statements.as_slice() else {
            panic!("query");
        };
        let options = with_query_hints(
            QueryOptions::from_proto(Default::default()),
            &novarocks_sql::sql_mode::query_sql_semantics(&session, query).unwrap(),
            Some(query),
        );
        assert!(!options.as_proto().allow_throw_exception);
        assert_eq!(options.as_proto().query_mem_limit, 9);
        let restored =
            with_query_hints(QueryOptions::from_proto(Default::default()), &session, None);
        assert!(restored.as_proto().allow_throw_exception);
        assert!(session.sql_mode().group_concat_legacy());
    }
}

#[cfg(test)]
mod maintenance_selected_failure_tests {
    use super::*;
    use novarocks_execution_contract::{SafeDetail, TaskFailurePhase, TaskIdentity};
    use novarocks_failure_contract::FailureProjection;
    use novarocks_query_application::execution_error::{
        ExecutionFailureInput, ExecutionFailureSite,
    };
    use novarocks_table_maintenance::runtime::{MaintenanceJobState, TerminalError};
    use novarocks_types::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };

    fn selected_input() -> ExecutionFailureInput {
        let identity = TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(91, 18), AttemptId::new(1).unwrap()).unwrap(),
            StageId::new(2).unwrap(),
            TaskId::new(3).unwrap(),
            BackendProcessId::new_v7(),
        );
        ExecutionFailureInput::new(
            novarocks_local_compiler::FragmentCompileError::Unsupported {
                node: None,
                feature: "provider relation other than a table",
            }
            .failure_fact(),
            SafeDetail::new("local fragment capability admission failed").unwrap(),
            ExecutionFailureSite::Task {
                identity,
                phase: TaskFailurePhase::Preparation,
            },
        )
    }

    #[test]
    fn maintenance_native_selected_input_reaches_sql_without_text_reclassification() {
        let input = selected_input();
        let terminal = TerminalError::known_uncommitted("private native diagnostic")
            .with_execution_failure(Some(input.clone()));
        let service = routed_execution_service_error(product_command_error(
            maintenance_command_error(terminal),
        ));
        assert_eq!(service.execution_failure(), Some(&input));
        assert_eq!(
            service.user_error().unwrap().code(),
            input.user_error().code()
        );
        assert!(service.message().starts_with("[UnsupportedCompiledShape]"));
        assert!(!service.message().contains("private native diagnostic"));
        // The same text without the selected input is not evidence.
        let text = maintenance_command_error(TerminalError::known_uncommitted(
            input.user_error().to_string(),
        ));
        assert_eq!(text.execution_failure(), None);
    }

    #[test]
    fn maintenance_selected_input_and_publication_verdict_both_reach_sql() {
        use novarocks_query_application::api::StatementExternalEffectDisposition;
        for (state, disposition, label) in [
            (
                MaintenanceJobState::CommitUnknown,
                StatementExternalEffectDisposition::CommitUnknown,
                "CommitUnknown",
            ),
            (
                MaintenanceJobState::KnownCommittedFinalizationFailed,
                StatementExternalEffectDisposition::KnownCommitted,
                "KnownCommitted",
            ),
            (
                MaintenanceJobState::KnownUncommitted,
                StatementExternalEffectDisposition::KnownUncommitted,
                "KnownUncommitted",
            ),
        ] {
            let input = selected_input();
            let control = novarocks_type_contract::CompileControlError::ResourceExhausted;
            let mut terminal = TerminalError::known_uncommitted("private publication detail")
                .with_execution_failure(Some(input.clone()))
                .with_compile_control(Some(control.clone()));
            terminal.state = state;
            let command = maintenance_command_error(terminal);
            assert_eq!(command.execution_failure(), Some(&input));
            assert_eq!(command.external_effect_disposition(), Some(disposition));
            let service = routed_execution_service_error(product_command_error(command));
            assert_eq!(service.execution_failure(), Some(&input));
            assert_eq!(service.compile_control_error(), Some(control));
            assert_eq!(service.external_effect_disposition(), Some(disposition));
            assert_eq!(service.publication_terminal(), None); // No invented identity/header.
            assert_eq!(
                service.user_error().unwrap().code(),
                input.user_error().code()
            );
            assert!(service.message().starts_with("[UnsupportedCompiledShape]"));
            assert!(!service.message().contains("private publication detail"));
            assert_eq!(
                service.message().contains("do not retry statement"),
                disposition.do_not_retry()
            );
            if disposition.do_not_retry() {
                assert!(service.message().contains(label));
            }
        }
        // An untyped message is still not evidence of an execution cause.
        let command = maintenance_command_error(TerminalError::commit_unknown(
            "original publication verdict",
        ));
        assert_eq!(command.execution_failure(), None);
        assert_eq!(
            command.external_effect_disposition(),
            Some(StatementExternalEffectDisposition::CommitUnknown)
        );
        assert!(command.to_string().contains("original publication verdict"));
        let service = routed_execution_service_error(product_command_error(command));
        assert_eq!(service.execution_failure(), None);
        assert_eq!(
            service.external_effect_disposition(),
            Some(StatementExternalEffectDisposition::CommitUnknown)
        );
        assert!(service.message().contains("original publication verdict"));
        assert!(service.message().contains("do not retry statement"));
    }
    #[test]
    fn maintenance_untyped_publication_survives_original_caller_control_fallback() {
        use novarocks_query_application::api::StatementExternalEffectDisposition;
        let command =
            maintenance_command_error(TerminalError::commit_unknown("original abort uncertainty"));
        let fallback =
            cancellation_error(QueryCancellationReason::DeadlineExceeded { timeout_ms: 1 });
        let expected = fallback.execution_failure().cloned();
        let service = selected_execution_or_worker_fallback(
            Err(product_command_error(command)),
            fallback.clone(),
        );
        assert_eq!(service.execution_failure(), expected.as_ref());
        assert_eq!(service.kind(), fallback.kind());
        assert_eq!(
            service.external_effect_disposition(),
            Some(StatementExternalEffectDisposition::CommitUnknown)
        );
        assert!(service.message().contains("do not retry statement"));
    }
}

#[cfg(test)]
mod maintenance_failure_cause_tests {
    use super::*;
    use novarocks_failure_contract::{FailureProjection, SharedFailureCause};

    #[test]
    fn neutral_command_effect_preserves_non_lake_cause_without_domain_terminal() {
        use novarocks_query_application::api::StatementExternalEffectDisposition;
        let cause = SharedFailureCause::new(novarocks_workload_control::WorkError::Capacity(
            "original statement input",
        ));
        let fact = cause.failure_fact();
        for disposition in [
            StatementExternalEffectDisposition::KnownUncommitted,
            StatementExternalEffectDisposition::CommitUnknown,
            StatementExternalEffectDisposition::KnownCommitted,
        ] {
            for control in [
                None,
                Some(CompileControlError::Cancelled),
                Some(CompileControlError::DeadlineExceeded),
                Some(CompileControlError::ResourceExhausted),
            ] {
                let command = CommandError::from_failure_cause(cause.clone())
                    .with_compile_control(control.clone())
                    .with_external_effect_disposition(Some(disposition));
                let service = selected_execution_or_worker_fallback(
                    Err(product_command_error(command)),
                    internal_error("late observer failure"),
                );
                assert_eq!(service.failure_cause(), Some(&cause));
                assert_eq!(service.execution_failure().unwrap().fact(), fact);
                assert_eq!(service.external_effect_disposition(), Some(disposition));
                assert!(service.publication_terminal().is_none());
                assert_eq!(service.compile_control_error(), control);
                assert!(!service.message().contains("maintenance publication"));
                assert!(!service.message().contains("lake publication"));
                assert_eq!(
                    service.message().contains("do not retry statement"),
                    disposition.do_not_retry()
                );
            }
        }
    }

    #[test]
    fn command_projection_and_late_fallback_keep_original_source_and_publication() {
        let mutation = novarocks_spi::connector::ConnectorMutationFailure::from_connector(
            novarocks_spi::connector::ConnectorError::compile_interruption(
                CompileControlError::from_domain(novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic),
            ),
        );
        let cause = SharedFailureCause::new(mutation);
        let fact = cause.failure_fact();
        let original = novarocks_table_maintenance::runtime::TerminalError::commit_unknown(
            "publication unknown",
        )
        .with_failure_cause(Some(cause.clone()));
        let routed = product_command_error(maintenance_command_error(original));
        let service =
            selected_execution_or_worker_fallback(Err(routed), internal_error("late stop"));
        assert_eq!(service.failure_cause(), Some(&cause));
        assert_eq!(service.execution_failure().unwrap().fact(), fact);
        assert_eq!(
            service.external_effect_disposition(),
            Some(
                novarocks_query_application::api::StatementExternalEffectDisposition::CommitUnknown
            )
        );
        assert!(service.compile_control_error().is_none());
        assert_eq!(
            std::error::Error::source(&service)
                .unwrap()
                .downcast_ref::<SharedFailureCause>(),
            Some(&cause)
        );
    }
}

#[cfg(test)]
mod mv_guard_application_cause_tests {
    use super::*;
    use novarocks_failure_contract::{FailureProjection, SharedFailureCause};

    #[test]
    fn dml_guard_application_retains_original_qse_and_cause_through_late_stop() {
        let provider = novarocks_spi::connector::ConnectorError::from_domain_static(
            novarocks_spi::connector::ConnectorErrorKind::Interrupted,
            CompileControlError::ResourceExhausted,
            "original guarded DML provider refusal",
        );
        let cause = std::error::Error::source(&provider)
            .unwrap()
            .downcast_ref::<SharedFailureCause>()
            .unwrap()
            .clone();
        let original = crate::mv::domain::readiness::connector_application_error(provider);
        let failure =
            crate::dml::error::DmlExecutionError::from(original.clone()).into_dml_error(None);
        let routed = dml_result::<()>(Err(failure));
        let service = selected_execution_or_worker_fallback(
            routed.map(|_| StatementResult::Ok),
            QueryServiceError::from_compile_control(CompileControlError::Cancelled),
        );
        assert_eq!(service, original);
        assert_eq!(service.failure_cause(), Some(&cause));
        assert_eq!(
            service.execution_failure().unwrap().fact(),
            cause.failure_fact()
        );
    }

    #[test]
    fn dml_guard_plain_policy_message_stays_plain() {
        let original = crate::mv::domain::readiness::application_error(
            "table is a materialized view; use its MV command",
        );
        let failure =
            crate::dml::error::DmlExecutionError::from(original.clone()).into_dml_error(None);
        let service = routed_execution_service_error(dml_result::<()>(Err(failure)).unwrap_err());
        assert_eq!(service, original);
        assert!(service.failure_cause().is_none());
        assert!(service.execution_failure().is_none());
    }
}
#[cfg(test)]
mod product_user_error_projection_tests {
    use super::*;
    #[test]
    fn original_product_descriptor_keeps_its_code_and_independent_effect() {
        let novarocks_sql::compiler::SqlCompileError::Analyze(analyze) =
            novarocks_sql::compiler::SqlCompileError::from(
                novarocks_functions::FunctionBindingError::UnavailableImplementation(
                    novarocks_functions::FunctionOverloadId::try_new(
                        "fixture/product/missing-pure-v1",
                    )
                    .unwrap(),
                ),
            )
        else {
            panic!("the actual analyzer authors the descriptor")
        };
        let original = analyze.to_user_error(None);
        for effect in [None, Some(novarocks_query_application::api::StatementExternalEffectDisposition::KnownCommitted)] {
            let command = CommandError::from_user_error(original.clone())
                .with_external_effect_disposition(effect);
            let selected = routed_execution_service_error(product_command_error(command));
            assert_eq!(selected.user_error(), Some(&original));
            assert_eq!(selected.external_effect_disposition(), effect);
            assert!(selected.compile_control_error().is_none());
        }
    }
}
