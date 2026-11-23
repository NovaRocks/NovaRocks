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
use crate::query_execution::maintenance::command::MaintenanceCommandExecutor;
use crate::statistics::command::StatisticsCommandExecutor;
use crate::view::command::ViewCommandExecutor;
use async_trait::async_trait;
use novarocks_parser::ast;
use novarocks_proto_codec::lifecycle::QueryOptions;
use novarocks_proto_models::novarocks;
use novarocks_query_application::admitted_query_context::{RequestAdmission, RequestContext};
use novarocks_query_application::api::{BackendTopologyService, BackendTopologySnapshot};
use novarocks_query_application::api::{
    CatalogCommandConsumer, CommandContext, CommandError, CommandErrorKind, CommandFuture,
    OptionalCommandFuture, QueryExecutionError, StatisticsCommandConsumer,
};
use novarocks_query_application::api::{QueryResult, build_string_query_result};
use novarocks_query_application::cancellation::{QueryCancellationReason, QueryCancellationView};
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
    QuerySession, QuerySessionFactory, QuerySessionStatement, StatementAdmission,
    StatementOutputShape, StatementSessionRequest,
};
use novarocks_query_application::session_control::{
    GovernedQueryStatementBeginError, GovernedQueryStatementOwner, GovernedStatementAction,
    QueryControlService, QuerySessionLease, SessionIdentity, SessionToken, StatementActionReady,
    StatementActionValue, StatementCommandCompletion, StatementCompletionProjection,
    StatementToken,
};
use novarocks_query_application::session_effects::SessionEffects;
use novarocks_query_application::session_error::{QueryServiceError, QueryServiceErrorKind};
use novarocks_query_application::session_outcome::{
    cancellation_error, cancellation_requires_statement_fence, governed_cancellation_error,
    governed_query_deadline, governed_query_execution_error, governed_statement_begin_error,
};
use novarocks_query_application::session_value::{
    PreparedStatementSnapshot, StatementSessionError,
};
use novarocks_query_application::sql::catalog::SessionCatalogService;
use novarocks_query_application::sql::session::{SessionExecutionSettings, SessionSqlState};
use novarocks_query_application::sql::trino_admission::TrinoSessionAction;
use novarocks_query_application::sql::{
    PreparedMaintenanceCall, ProductCommandRouter, ProductSqlCommand,
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

pub(crate) mod analyze_property_computation;
mod catalog_action;
pub(crate) mod catalog_property_computation;
pub(crate) mod compiler;
pub(crate) mod ctas_action;
pub(crate) mod delete_action;
mod explain_action;
pub(crate) use explain_action::prepare_delete_explain;
pub(crate) use query_session_action::QueryActionEnvironment;
pub(crate) mod insert_action;
mod maintenance_action;
pub(crate) mod mutation_action;
pub(crate) mod mv_action;
pub(crate) mod mv_property_action;
pub(crate) mod mv_read_action;
mod property_action_tail;
mod property_cpu_context;
mod query_session_action;
pub(crate) mod read_action;
pub(super) mod schema_create_action;
pub(crate) mod session_property_computation;
mod set_action;
mod statement_action;
pub(crate) mod statement_computation;
pub(crate) mod statement_publication;
mod table_create_action;
pub(crate) mod table_mutation_action;
pub(crate) mod table_property_computation;
pub(crate) mod trino_statement;
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
    fn acquire_namespace_property_binding(
        &self,
        name: &novarocks_sql::semantic::ObjectName,
        request_context: &RequestContext,
    ) -> Result<novarocks_query_application::api::CatalogNamespacePropertyBinding, CommandError>
    {
        self.catalog
            .acquire_namespace_property_binding(name, request_context)
    }

    fn acquire_table_read_binding(
        &self,
        name: &novarocks_sql::semantic::ObjectName,
        request_context: &RequestContext,
    ) -> Result<novarocks_query_application::api::CatalogTableReadBinding, CommandError> {
        self.catalog
            .acquire_table_read_binding(name, request_context)
    }

    fn acquire_table_property_binding(
        &self,
        name: &novarocks_sql::semantic::ObjectName,
        request_context: &RequestContext,
    ) -> Result<novarocks_query_application::api::CatalogTablePropertyBinding, CommandError> {
        self.catalog
            .acquire_table_property_binding(name, request_context)
    }

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
        if matches!(
            command,
            novarocks_sql::semantic::CatalogSqlCommand::CreateTable(_)
        ) {
            return Box::pin(async {
                Err(CommandError::new(
                    CommandErrorKind::Conflict,
                    "CREATE TABLE requires its original prepared generation",
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
            move |context, command_context| -> Result<StatementResult, CommandError> {
                match command {
                    novarocks_sql::semantic::CatalogSqlCommand::DropDatabase { name, .. }
                        if context.session().current_catalog().is_none()
                            && name.parts.len() == 1 =>
                    {
                        view.drop_database(DEFAULT_CATALOG, &name.parts[0])?;
                        Ok(StatementResult::Ok)
                    }
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
                    novarocks_sql::semantic::CatalogSqlCommand::AlterIcebergTable(command) => {
                        catalog.execute_iceberg_command(
                            &command,
                            context.session().current_catalog(),
                            context.session().current_database(),
                            command_context.connector_context(),
                        )
                    }
                    command => catalog.execute_command(
                        &command,
                        context.session().current_catalog(),
                        context.session().current_database(),
                        command_context.connector_context(),
                    ),
                }
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
        self.statistics.prepare_statement_action(
            &command,
            request_context.session().current_catalog(),
            request_context.session().current_database(),
            request_context.execution(),
            command_context.connector_context(),
            request_context
                .session()
                .presentation()
                .copied()
                .ok_or_else(|| {
                    CommandError::new(
                        CommandErrorKind::Rejected,
                        "statistics client result requires an explicit presentation",
                    )
                })?,
        )
    }

    fn execute(
        &self,
        _command: novarocks_sql::semantic::StatisticsSqlCommand,
        _request_context: RequestContext,
        _command_context: CommandContext,
    ) -> CommandFuture {
        Box::pin(async {
            Err(CommandError::new(
                CommandErrorKind::Conflict,
                "statistics requires its original governed statement source",
            ))
        })
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

pub(crate) fn maintenance_command_error(
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
    executor: QueryBlockingExecutor,
    state_store: Option<crate::state_store::execution::StateStoreOriginalBinding>,
) -> ProductCommandRouter {
    ProductCommandRouter::new(
        Arc::new(FrontendCatalogCommandConsumer {
            catalog,
            state_store,
            view,
            iceberg_ref,
            executor,
        }),
        Arc::new(FrontendStatisticsCommandConsumer { statistics }),
    )
}

#[derive(Clone)]
struct TypedCommandRoute {
    view: ViewCommandExecutor,
    mv_call: MvCommandExecutor,
    executor: QueryBlockingExecutor,
}

impl TypedCommandRoute {
    fn new(
        view: ViewCommandExecutor,
        mv_call: MvCommandExecutor,
        executor: QueryBlockingExecutor,
    ) -> Self {
        Self {
            view,
            mv_call,
            executor,
        }
    }
}

impl TypedCommandRoute {
    fn execute_maintenance_call(
        &self,
        statement: Arc<PreparedMaintenanceCall>,
        context: &RequestContext,
        command_context: &CommandContext,
    ) -> OptionalCommandFuture {
        let mv_call = self.mv_call.clone();
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
            let user = error.to_user_error(Some(sql));
            match error.expression_source_arc() {
                Some(source) => RoutedExecutionError::Application(
                    QueryServiceError::from_user_error(user).with_trino_statement_source(
                        novarocks_query_application::sql::trino_admission::TrinoStatementSource::request(source.clone()),
                    ),
                ),
                None => RoutedExecutionError::User(user),
            }
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

pub(crate) fn procedure_command_error(error: QueryServiceError) -> CommandError {
    let control = error.compile_control_error();
    let command = match (error.user_error(), error.failure_cause()) {
        (Some(user), Some(cause)) => {
            CommandError::from_user_error_with_cause(user.clone(), cause.clone())
        }
        (Some(user), None) => CommandError::from_user_error(user.clone()),
        (None, Some(cause)) => CommandError::from_failure_cause(cause.clone()),
        (None, None) => match control.clone() {
            Some(control) => CommandError::from_compile_control(control),
            None if error.kind() == QueryServiceErrorKind::Unsupported => {
                CommandError::new(CommandErrorKind::Unsupported, error.message())
            }
            None => CommandError::from(error.message().to_owned()),
        },
    };
    command.with_compile_control(control)
}

fn dml_result<T>(result: Result<T, crate::dml::DmlError>) -> Result<T, RoutedExecutionError> {
    result.map_err(|error| {
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

type TypedCommandWorker = Pin<
    Box<
        dyn Future<
                Output = Result<(Result<StatementResult, RoutedExecutionError>, WorkOwner), String>,
            > + Send,
    >,
>;

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
        let service = match error.user_error() {
            Some(user) => {
                QueryServiceError::from_user_error_with_cause(user.clone(), cause.clone())
            }
            None => QueryServiceError::from_failure_cause(cause.clone()),
        };
        let mut service = service.with_compile_control(error.compile_control_error());
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

/// Design: ADR-0012 (docs/adr/ADR-0012-frontend-query-session-router.md)
#[derive(Clone)]
pub struct FrontendQueryService {
    state_store: Option<crate::state_store::execution::StateStoreOriginalBinding>,
    #[cfg(novarocks_dev_controls)]
    statement_observation: Option<Arc<crate::preparation_diagnostics::StatementObservationOwner>>,
    #[cfg(novarocks_dev_controls)]
    result_handoff: Option<Arc<crate::test_session::result_handoff::ResultHandoffControl>>,
    session_catalog_resolver: SessionCatalogService,
    session_properties:
        Arc<novarocks_query_application::sql::session::registry::SessionPropertyRegistry>,
    required_constant_interpreter: Arc<crate::RequiredConstantInterpreter>,
    query_compiler: FrontendQueryCompiler,
    command_executor: Arc<TypedCommandRoute>,
    product_command_router: ProductCommandRouter,
    alter_mv_guard: table_mutation_action::AlterTableMvGuard,
    statistics_command: crate::statistics::command::StatisticsCommandExecutor,
    iceberg_ref_command: IcebergRefCommandExecutor,
    maintenance_command: MaintenanceCommandExecutor,
    query_control: QueryControlService,
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
    statement_clock: crate::application::StatementClock,
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
        session_properties: Arc<
            novarocks_query_application::sql::session::registry::SessionPropertyRegistry,
        >,
        required_constant_interpreter: Arc<crate::RequiredConstantInterpreter>,
        query_compiler: FrontendQueryCompiler,
        product_command_router: ProductCommandRouter,
        alter_mv_guard: table_mutation_action::AlterTableMvGuard,
        statistics_command: crate::statistics::command::StatisticsCommandExecutor,
        iceberg_ref_command: IcebergRefCommandExecutor,
        maintenance_command: MaintenanceCommandExecutor,
        view_command_executor: ViewCommandExecutor,
        mv_command_executor: MvCommandExecutor,
        query_control: QueryControlService,
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
        statement_clock: crate::application::StatementClock,
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
            session_properties,
            required_constant_interpreter,
            query_compiler,
            command_executor: Arc::new(TypedCommandRoute::new(
                view_command_executor,
                mv_command_executor,
                query_blocking_executor.clone(),
            )),
            product_command_router,
            alter_mv_guard,
            statistics_command,
            iceberg_ref_command,
            maintenance_command,
            query_control,
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
            statement_clock,
            lake_publication_runtime_policy,
            serving_admission,
        }
    }

    // Design: ADR-0203 (docs/adr/ADR-0203-client-result-presentation-and-trino-paged-delivery.md)
    pub(crate) fn open_frontend_statement_session(
        &self,
        request: StatementSessionRequest,
    ) -> Result<Arc<FrontendQuerySession>, StatementSessionError> {
        let admission = request.admission();
        let (query_id, principal, value) = request.into_parts();
        let prepared_statements = value.prepared_statements().clone();
        let state = SessionSqlState::from_session_value(
            value,
            &self.session_properties,
            self.session_catalog_resolver.as_ref(),
        )?;
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
            prepared_statements: Mutex::new(prepared_statements),
            statement_ready: statement_action::StatementReadyReceiver::default(),
            principal,
            admission,
            output_shape: Mutex::new(None),
        }))
    }
}

impl QuerySessionFactory for FrontendQueryService {
    fn open_statement_session(
        &self,
        request: StatementSessionRequest,
    ) -> Result<Arc<dyn QuerySession>, StatementSessionError> {
        Ok(self.open_frontend_statement_session(request)?)
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

pub(crate) struct FrontendQuerySession {
    origin: novarocks_query_application::session_control::SessionOrigin,
    service: FrontendQueryService,
    lease: Mutex<Option<QuerySessionLease>>,
    state: Arc<Mutex<SessionSqlState>>,
    prepared_statements: Mutex<PreparedStatementSnapshot>,
    statement_ready: statement_action::StatementReadyReceiver,
    /// Who the server authenticated on this connection. A command that records
    /// what someone did takes this, never an argument that claims an identity.
    principal: Arc<str>,
    admission: novarocks_query_application::session::StatementAdmissionPublisher,
    output_shape: Mutex<Option<novarocks_query_application::session::StatementOutputShape>>,
}

/// Keep the original SQL prefix for exact error locations.
fn statement_execution_source(original: &str) -> &str {
    original.trim_end().trim_end_matches(';').trim_end()
}

impl FrontendQuerySession {
    fn statement_logical_execution_launcher(&self) -> Arc<dyn LogicalExecutionLauncher> {
        let novarocks_query_application::session_control::SessionOrigin::ClientStatement(id) =
            &self.origin;
        crate::query_execution::logical_execution::trace_client_statement(
            Arc::clone(&self.service.logical_execution_launcher),
            id.clone(),
        )
    }

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

    pub(super) async fn execute_trino_session_action(
        &self,
        source: Arc<str>,
        action: TrinoSessionAction,
        parameters: Option<Arc<novarocks_sql::compiler::SqlPreparedParameters>>,
        statement_time: novarocks_type_contract::StatementTimeEnvironment,
    ) -> Result<QuerySessionStatement, QueryServiceError> {
        let (kind, work) = lower_trino_session_action(action);
        let shape = StatementOutputShape::Empty;
        *self.output_shape.lock().map_err(poisoned_state)? = Some(shape);
        self.admission.publish(StatementAdmission { kind, shape });

        let (governed, deadline) =
            self.begin_session_action(&source, matches!(&work, TrinoSessionWork::Mutation(_)))?;
        let output = match work {
            TrinoSessionWork::Use { catalog, schema } => {
                self.execute_session_use(
                    SessionNamespace {
                        catalog: catalog.as_ref(),
                        schema: &schema,
                        source: &source,
                    },
                    governed,
                )
                .await
            }
            TrinoSessionWork::PreparedEffects(effects) => StatementResult::GovernedCompletion(
                GovernedCompletionStatementResult::with_session_effects(governed, effects),
            ),
            TrinoSessionWork::Mutation(mutation) => {
                self.execute_observed_scoped_session_mutation(
                    source,
                    mutation,
                    parameters,
                    deadline,
                    governed,
                    statement_time,
                )
                .await?
            }
        };
        Ok(QuerySessionStatement::output_owned(output, kind))
    }

    fn begin_session_action(
        &self,
        source: &Arc<str>,
        inherited_timeout: bool,
    ) -> Result<(GovernedQueryStatementOwner, Option<Instant>), QueryServiceError> {
        let (deadline, timeout_ms) = if inherited_timeout {
            let state = self.state.lock().map_err(poisoned_state)?;
            governed_query_deadline(&state)?
        } else {
            (None, None)
        };
        let token = self.token()?;
        let governed = self
            .service
            .query_control
            .begin_governed_statement_with_result(
                token,
                &self.service.workload_root_admission,
                WorkClass::Management,
                deadline.map(tokio::time::Instant::from_std),
                timeout_ms,
                Some(Arc::clone(source)),
                ResultWindowClass::Local,
            )
            .map_err(|error| self.governed_statement_begin_error(error))?;
        Ok((governed, deadline))
    }

    async fn execute_session_use(
        &self,
        namespace: SessionNamespace<'_>,
        governed: GovernedQueryStatementOwner,
    ) -> StatementResult {
        let cancellation =
            QueryCancellationView::governed(governed.cancellation().clone(), governed.timeout_ms());
        match self
            .init_database_with_cancellation(namespace, cancellation, &governed)
            .await
        {
            Ok(effects) => StatementResult::GovernedCompletion(
                GovernedCompletionStatementResult::with_session_effects(governed, effects),
            ),
            Err(error) => self.governed_typed_error(error, governed),
        }
    }

    fn governed_local_result(
        &self,
        result: QueryResult,
        statement: novarocks_query_application::session_control::GovernedQueryStatementOwner,
    ) -> StatementResult {
        let family = match self.state.lock() {
            Ok(state) => state.presentation().copied(),
            Err(error) => return self.governed_typed_error(poisoned_state(error), statement),
        };
        let Some(family) = family else {
            return self.governed_typed_error(
                internal_error("client result requires an explicit presentation"),
                statement,
            );
        };
        match GovernedImmediateStatementResult::try_new(result, statement, family) {
            Ok(result) => StatementResult::GovernedQuery(result),
            Err((error, statement)) => self.governed_typed_error(internal_error(error), statement),
        }
    }

    async fn init_database_with_cancellation(
        &self,
        namespace: SessionNamespace<'_>,
        cancellation: QueryCancellationView,
        statement: &GovernedQueryStatementOwner,
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
            namespace,
            connector_context,
            &capacity,
        )
        .await?;
        let mut state = self.state.lock().map_err(poisoned_state)?;
        let effects = SessionEffects::set_catalog_schema(context.catalog, context.database);
        state.apply_effects(&effects)?;
        Ok(effects)
    }

    async fn finish_command_worker(
        &self,
        statement: novarocks_query_application::session_control::GovernedQueryStatementOwner,
        cancellation: QueryCancellationView,
        timeout_duration: Option<Duration>,
        worker: &mut TypedCommandWorker,
    ) -> Result<StatementResult, QueryServiceError> {
        self.finish_command_worker_with_unknown_count(
            statement,
            cancellation,
            timeout_duration,
            worker,
            None,
        )
        .await
    }

    async fn finish_command_worker_with_unknown_count(
        &self,
        mut statement: novarocks_query_application::session_control::GovernedQueryStatementOwner,
        cancellation: QueryCancellationView,
        timeout_duration: Option<Duration>,
        mut worker: &mut TypedCommandWorker,
        count_projection: Option<StatementCompletionProjection>,
    ) -> Result<StatementResult, QueryServiceError> {
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
            Ok(StatementResult::Ok) if count_projection.is_some() => {
                let projection = count_projection.expect("guard selected command count projection");
                // The branch TRUNCATE publication reports a planning summary,
                // not the count of visible rows removed by this DELETE.
                let mut ready = StatementActionReady::new(
                    Ok(StatementActionValue::Ok(
                        StatementCommandCompletion::no_count(),
                    )),
                    statement,
                );
                ready.prepare_completion(projection.shape(), projection.family());
                let mut ready = Some(ready);
                let mut output = None;
                StatementActionReady::transfer_protocol_in(&mut ready, &mut output)
                    .map_err(internal_error)?;
                Ok(output.expect("command completion installed"))
            }
            Ok(StatementResult::Ok) => Ok(StatementResult::GovernedCompletion(
                GovernedCompletionStatementResult::new(
                    novarocks_query_application::session_control::StatementCommandCompletion::empty(
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

fn with_query_sql_semantics(
    query_options: QueryOptions,
    sql_semantics: &novarocks_sql::sql_mode::SqlSemanticSettings,
) -> QueryOptions {
    let mut raw = *query_options.as_proto();
    raw.allow_throw_exception = sql_semantics.sql_mode().allow_throw_exception();
    QueryOptions::from_proto(raw)
}

#[async_trait]
impl QuerySession for FrontendQuerySession {
    async fn execute_statement(
        &self,
        statement: &str,
    ) -> Result<novarocks_query_application::session::QuerySessionStatement, QueryServiceError>
    {
        self.execute_trino_one_statement(Arc::from(statement_execution_source(statement)))
            .await
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

enum TrinoSessionWork {
    Use {
        catalog: Option<ast::Ident>,
        schema: ast::Ident,
    },
    Mutation(session_property_computation::ScopedSessionMutation),
    PreparedEffects(SessionEffects),
}

fn lower_trino_session_action(action: TrinoSessionAction) -> (StatementKind, TrinoSessionWork) {
    use session_property_computation::ScopedSessionMutation;
    match action {
        TrinoSessionAction::Use {
            catalog,
            schema,
            span: _,
        } => (
            StatementKind::Use,
            TrinoSessionWork::Use { catalog, schema },
        ),
        TrinoSessionAction::Set { name, value } => (
            StatementKind::SetSession,
            TrinoSessionWork::Mutation(ScopedSessionMutation::Set { name, value }),
        ),
        TrinoSessionAction::Reset { name, span } => (
            StatementKind::ResetSession,
            TrinoSessionWork::Mutation(ScopedSessionMutation::Reset { name, span }),
        ),
        TrinoSessionAction::Prepare { name, sql } => (
            StatementKind::Prepare,
            TrinoSessionWork::PreparedEffects(SessionEffects::prepare(name, sql)),
        ),
        TrinoSessionAction::DeallocatePrepare { name } => (
            StatementKind::DeallocatePrepare,
            TrinoSessionWork::PreparedEffects(SessionEffects::deallocate_prepare(name)),
        ),
        TrinoSessionAction::SetTimeZone(node) => (
            StatementKind::SetTimeZone,
            TrinoSessionWork::Mutation(ScopedSessionMutation::SetTimeZone(node)),
        ),
    }
}

struct SessionNamespace<'a> {
    catalog: Option<&'a ast::Ident>,
    schema: &'a ast::Ident,
    source: &'a str,
}

fn normalize_session_namespace_part(raw: &str) -> Result<String, QueryServiceError> {
    let normalized = normalize_identifier(raw)
        .map_err(|error| QueryServiceError::new(QueryServiceErrorKind::BadDatabase, error))?;
    if !normalized.eq_ignore_ascii_case(raw) {
        return Err(QueryServiceError::new(
            QueryServiceErrorKind::BadDatabase,
            format!("unsupported identifier `{raw}`"),
        ));
    }
    Ok(normalized)
}

async fn resolve_database_context(
    resolver: &SessionCatalogService,
    current_catalog: Option<&str>,
    namespace: SessionNamespace<'_>,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
    capacity: &novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
) -> Result<DatabaseContext, QueryServiceError> {
    let SessionNamespace {
        catalog,
        schema,
        source,
    } = namespace;
    let start = catalog.map_or(schema.span.start(), |catalog| catalog.span.start());
    let display = source.get(start..schema.span.end()).ok_or_else(|| {
        internal_error("session namespace does not retain its original source range")
    })?;
    let catalog = catalog.map(|catalog| catalog.value.as_str());
    let database = schema.value.as_str();
    let catalog = match catalog {
        Some(catalog) => {
            let normalized = normalize_session_namespace_part(catalog)?;
            if normalized == DEFAULT_CATALOG {
                None
            } else {
                // This is catalog admission, not a process-local lookup.
                resolver.require_external_catalog_ready(&normalized)?;
                Some(normalized)
            }
        }
        None => current_catalog.map(ToOwned::to_owned),
    };
    let database = normalize_session_namespace_part(database)?;
    let exists = match &catalog {
        Some(catalog) => {
            resolver
                .external_namespace_exists(connector_context, capacity, catalog, &database)
                .await?
        }
        None => resolver.database_exists(&database)?,
    };
    if !exists {
        return Err(QueryServiceError::new(
            QueryServiceErrorKind::BadDatabase,
            format!("unknown database `{display}`"),
        ));
    }
    Ok(DatabaseContext { catalog, database })
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
        query_mem_limit: settings.query_mem_limit_bytes().unwrap_or_default(),
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
    use novarocks_query_application::api::ExecutionFailurePresentation;

    #[test]
    fn statement_origin_parser_errors_keep_the_original_source_prefix() {
        for (sql, line, column) in [
            ("                    SELECT );;", 1, 28),
            ("-- 中文😀 comment\n  SELECT '中😀中', );;", 2, 24),
            ("\n\nSELECT\n '中😀',\n );;", 5, 2),
        ] {
            let source = statement_execution_source(sql);
            assert_eq!(
                source.as_ptr(),
                sql.as_ptr(),
                "the original source prefix stays borrowed"
            );
            let error = novarocks_query_application::sql::parse_single_statement(source)
                .expect_err("actual parser must reject the closing parenthesis");
            let error = QueryServiceError::from_user_error(error.to_user_error(source));
            let location = error.user_error().unwrap().location().unwrap();
            assert_eq!(
                (location.line(), location.column()),
                (line, column),
                "{sql}"
            );
        }
        let sql = "  -- original prefix\n SELECT '中😀';;;  ";
        let source = statement_execution_source(sql);
        assert!(
            novarocks_query_application::sql::parse_single_statement(source).is_ok(),
            "trailing-semicolon tolerance stays intact"
        );
        assert_eq!(source.as_ptr(), sql.as_ptr());
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

    use novarocks_query_application::api::BackendTopologySnapshot;
    use novarocks_query_application::cancellation::QueryCancellationSource;
    use novarocks_types::{AttemptId, QueryExecutionId, QueryId};
    use novarocks_workload_control::{
        CancellationReason as WorkCancellationReason, ResourceConfig,
    };

    #[test]
    fn frontend_projects_query_application_session_settings_to_native_options() {
        let mut settings = SessionExecutionSettings::default();
        assert_eq!(
            query_options_from_session_settings(&settings)
                .as_proto()
                .query_mem_limit,
            0
        );
        settings.set_query_mem_limit_bytes(23).unwrap();
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
        assert_eq!(proto.query_mem_limit, 23);
        assert_eq!(proto.pipeline_dop, 4);
        assert_eq!(proto.runtime_filter_scan_wait_time_ms, Some(0));
        assert_eq!(proto.runtime_filter_wait_timeout_ms, Some(3));
        assert!(proto.enable_parquet_reader_page_index);
        assert!(proto.enable_scan_datacache);
        assert!(proto.enable_populate_datacache);
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
    fn timeout_message_rounds_a_sampled_deadline_up_to_milliseconds() {
        assert_eq!(
            timeout_message_millis(Duration::from_nanos(999_999_999)),
            1_000
        );
        assert_eq!(timeout_message_millis(Duration::from_millis(1_000)), 1_000);
    }

    #[test]
    fn execution_throw_policy_uses_the_same_effective_mode_as_sql_analysis() {
        let session = novarocks_sql::sql_mode::SqlSemanticSettings::default().with_sql_mode(
            novarocks_sql::sql_mode::SqlMode::from_assignment(
                "GROUP_CONCAT_LEGACY,ALLOW_THROW_EXCEPTION",
            ),
        );
        let query =
            novarocks_parser::parser::parse_query("WITH SESSION sql_mode = '32' SELECT 1").unwrap();
        let mut execution = SessionExecutionSettings::default();
        execution.set_query_mem_limit_bytes(9).unwrap();
        let options = with_query_sql_semantics(
            query_options_from_session_settings(&execution),
            &novarocks_sql::sql_mode::query_sql_semantics(&session, &query).unwrap(),
        );
        assert!(!options.as_proto().allow_throw_exception);
        assert_eq!(options.as_proto().query_mem_limit, 9);
        let restored =
            with_query_sql_semantics(query_options_from_session_settings(&execution), &session);
        assert!(restored.as_proto().allow_throw_exception);
        assert_eq!(restored.as_proto().query_mem_limit, 9);
        assert!(session.sql_mode().group_concat_legacy());
    }
}

#[cfg(test)]
mod maintenance_selected_failure_tests {
    use super::*;
    use novarocks_execution_contract::{SafeDetail, TaskFailurePhase, TaskIdentity};
    use novarocks_failure_contract::FailureProjection;
    use novarocks_query_application::api::ExecutionFailurePresentation;
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
