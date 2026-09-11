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

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::catalog_application::command::CatalogCommandExecutor;
use crate::catalog_application::iceberg_ref_command::IcebergRefCommandExecutor;
use crate::common::admitted_query_context::{
    LakePublicationRuntimePolicy, RequestAdmission, RequestContext, SessionOptimizerSettings,
};
use crate::common::backend_topology::{BackendTopologyService, BackendTopologySnapshot};
use crate::common::engine_error::EngineError;
use crate::common::query_cancellation::{
    QueryCancellationReason, QueryCancellationSource, QueryCancellationView,
};
use crate::mv::command::MvCommandExecutor;
use crate::query_execution::backend_command::BackendCommandExecutor;
use crate::query_execution::control::{
    ConnectionKillAuthorization, GovernedStatementFinishOutcome, QueryCancelOutcome,
    QueryControlService, QuerySessionLease, SessionIdentity, SessionToken, StatementFinishOutcome,
    StatementToken,
};
use crate::query_execution::dml::add_files::AddFilesEngine;
use crate::query_execution::dml::ctas::CtasEngine;
use crate::query_execution::dml::delete::DeleteEngine;
use crate::query_execution::dml::insert::InsertEngine;
use crate::query_execution::dml::mutation::MutationEngine;
use crate::query_execution::dml::truncate::TruncateEngine;
use crate::query_execution::kernels::SessionCatalogResolver;
use crate::query_execution::logical_read::LogicalReadLauncher;
use crate::query_execution::maintenance::command::{
    MaintenanceCommandExecutor, MaintenanceReadCommandExecutor,
};
use crate::query_execution::service::QueryExecutionService;
use crate::query_execution::{PreparedQueryOperation, StatementResult};
use crate::runtime::query_result::{QueryResult, QueryResultColumn, record_batch_to_chunk};
use crate::runtime::statement_result::GovernedImmediateStatementResult;
use crate::{
    ClientConnectionControlPort, ClientConnectionTerminateOutcome,
    ClientConnectionTerminationReason, QueryServiceError, QueryServiceErrorKind, QuerySession,
    QuerySessionFactory, QuerySessionOpenRequest, SessionExecutionSettings,
};
use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use novarocks_parser::{
    ast::{self, Fold, Statement as ParsedStatement},
    printer::{print_expr, print_statement},
};
use novarocks_proto_codec::lifecycle::QueryOptions;
use novarocks_query_application::api::{
    ExecutionOutput, QueryExecutionError, QueryExecutionErrorKind, ResultDelivery,
};
use novarocks_types::naming::{DEFAULT_DATABASE, normalize_identifier};
use novarocks_types::{ClusterRole, EngineErrorCode};
use novarocks_user_error::UserError;
use novarocks_workload_control::{
    CancellationReason as WorkCancellationReason, LocalResourceAuthority, RootAdmissionHandle,
    WorkClass, WorkRequest,
};
use tokio::task;

use crate::dml::DmlService;
use crate::query::compiler::{FrontendQueryCompiler, FrontendQueryCompilerError};
use crate::statistics::command::StatisticsCommandExecutor;
use crate::view::command::ViewCommandExecutor;
use crate::workload_lifecycle::{
    FrontendAdmissionError, FrontendServingLifecycle, FrontendWorkloadKind, FrontendWorkloadLease,
};

pub(crate) mod compiler;

const DEFAULT_CATALOG: &str = "default_catalog";

pub trait CoreCommandRoute: Send + Sync {
    fn execute_typed(
        &self,
        _statement: &ParsedStatement,
        _context: &RequestContext,
        _query_options: QueryOptions,
    ) -> Result<StatementResult, String> {
        Err("typed command route is unavailable".to_string())
    }
}

#[derive(Clone)]
struct TypedCommandRoute {
    catalog: CatalogCommandExecutor,
    statistics: StatisticsCommandExecutor,
    backend: BackendCommandExecutor,
    view: ViewCommandExecutor,
    iceberg_ref: IcebergRefCommandExecutor,
    mv: MvCommandExecutor,
    maintenance: MaintenanceCommandExecutor,
    maintenance_read: MaintenanceReadCommandExecutor,
}

impl TypedCommandRoute {
    #[expect(
        clippy::too_many_arguments,
        reason = "The command router constructor keeps each typed command owner explicit."
    )]
    fn new(
        catalog: CatalogCommandExecutor,
        statistics: StatisticsCommandExecutor,
        backend: BackendCommandExecutor,
        view: ViewCommandExecutor,
        iceberg_ref: IcebergRefCommandExecutor,
        mv: MvCommandExecutor,
        maintenance: MaintenanceCommandExecutor,
        maintenance_read: MaintenanceReadCommandExecutor,
    ) -> Self {
        Self {
            catalog,
            statistics,
            backend,
            view,
            iceberg_ref,
            mv,
            maintenance,
            maintenance_read,
        }
    }
}

impl CoreCommandRoute for TypedCommandRoute {
    fn execute_typed(
        &self,
        statement: &ParsedStatement,
        context: &RequestContext,
        query_options: QueryOptions,
    ) -> Result<StatementResult, String> {
        match statement {
            ParsedStatement::ShowBackends(statement) => {
                self.backend.execute(statement, context.execution().role())
            }
            ParsedStatement::Statistics(statement) => self.statistics.execute(
                statement,
                context.session().current_catalog(),
                context.session().current_database(),
                Some(context.execution()),
            ),
            ParsedStatement::Catalog(novarocks_parser::ast::CatalogStatement::DropDatabase(
                statement,
            )) if context.session().current_catalog().is_none()
                && statement.name.parts.len() == 1 =>
            {
                self.view
                    .drop_database(DEFAULT_CATALOG, &statement.name.parts[0].value)?;
                Ok(StatementResult::Ok)
            }
            ParsedStatement::Catalog(statement) => {
                let connector_context = crate::connector::connector_request_context_for_query(
                    Some(&query_options),
                    context.execution().cancellation().clone(),
                )?;
                self.catalog.execute_typed(
                    statement,
                    context.session().current_catalog(),
                    context.session().current_database(),
                    &connector_context,
                )
            }
            ParsedStatement::Iceberg(novarocks_parser::ast::IcebergStatement::AlterTable(
                statement,
            )) if matches!(
                &statement.action,
                novarocks_parser::ast::IcebergTableAction::Reference(_)
            ) =>
            {
                let connector_context = crate::connector::connector_request_context_for_query(
                    Some(&query_options),
                    context.execution().cancellation().clone(),
                )?;
                self.iceberg_ref.execute(
                    statement,
                    context.session().current_database(),
                    &connector_context,
                )
            }
            ParsedStatement::Iceberg(novarocks_parser::ast::IcebergStatement::AlterTable(
                statement,
            )) => {
                let connector_context = crate::connector::connector_request_context_for_query(
                    Some(&query_options),
                    context.execution().cancellation().clone(),
                )?;
                self.catalog.execute_iceberg_typed(
                    statement,
                    context.session().current_catalog(),
                    context.session().current_database(),
                    &connector_context,
                )
            }
            ParsedStatement::Maintenance(
                novarocks_parser::ast::MaintenanceStatement::ShowOptimize(statement),
            ) => self.maintenance_read.execute(
                statement,
                context.session().current_catalog(),
                context.session().current_database(),
            ),
            ParsedStatement::Maintenance(novarocks_parser::ast::MaintenanceStatement::Call(
                statement,
            )) => {
                let connector_context = crate::connector::connector_request_context_for_query(
                    Some(&query_options),
                    context.execution().cancellation().clone(),
                )?;
                if let Some(result) = self.mv.try_execute_typed_call(
                    statement,
                    context.session().current_database(),
                    &connector_context,
                )? {
                    return Ok(result);
                }
                self.maintenance.execute(
                    &novarocks_parser::ast::MaintenanceStatement::Call(statement.clone()),
                    context.session().current_catalog(),
                    context.session().current_database(),
                    context.execution(),
                    &connector_context,
                )
            }
            ParsedStatement::Maintenance(statement) => {
                let connector_context = crate::connector::connector_request_context_for_query(
                    Some(&query_options),
                    context.execution().cancellation().clone(),
                )?;
                self.maintenance.execute(
                    statement,
                    context.session().current_catalog(),
                    context.session().current_database(),
                    context.execution(),
                    &connector_context,
                )
            }
            ParsedStatement::MaterializedView(statement) => {
                let connector_context = crate::connector::connector_request_context_for_query(
                    Some(&query_options),
                    context.execution().cancellation().clone(),
                )?;
                self.mv.execute(
                    statement,
                    context.session().current_catalog(),
                    context.session().current_database(),
                    &connector_context,
                    context.execution(),
                )
            }
            ParsedStatement::View(statement) => {
                let connector_context = crate::connector::connector_request_context_for_query(
                    Some(&query_options),
                    context.execution().cancellation().clone(),
                )?;
                self.view.execute(
                    statement,
                    context.session().current_catalog(),
                    context.session().current_database(),
                    &connector_context,
                )
            }
            ParsedStatement::Table(statement) => {
                let connector_context = crate::connector::connector_request_context_for_query(
                    Some(&query_options),
                    context.execution().cancellation().clone(),
                )?;
                self.catalog.execute_table_typed(
                    statement,
                    context.session().current_catalog(),
                    context.session().current_database(),
                    &connector_context,
                )
            }
            ParsedStatement::Dml(_) => Err(
                "typed DDL/DML admission is enabled, but execution routing remains intentionally unconnected until SQLP-5 T10"
                    .to_string(),
            ),
            ParsedStatement::Session(_) => unreachable!(
                "session statements are applied before typed command routing"
            ),
            ParsedStatement::Query(_) | ParsedStatement::ExplainQuery(_) => {
                Err("typed query execution is owned by the query compiler".to_string())
            }
        }
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
    result.map(|()| StatementResult::Ok).map_err(|error| {
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

fn table_statement_admission_error(
    statement: &novarocks_parser::ast::TableStatement,
    source: &str,
) -> Option<UserError> {
    use novarocks_parser::ast::{TablePartition, TableStatement};

    let TableStatement::Create(statement) = statement;
    let unsupported = |span, message| {
        crate::dml::error::AdmitError::CreateTableUnsupportedForm
            .to_user_error(source, span, message)
    };
    if statement.temporary || statement.external {
        return Some(unsupported(
            statement.span,
            "CREATE TABLE does not support TEMPORARY or EXTERNAL tables".to_string(),
        ));
    }
    if let Some(engine) = &statement.engine
        && !engine.value.eq_ignore_ascii_case("iceberg")
    {
        return Some(unsupported(
            engine.span,
            format!("CREATE TABLE does not support ENGINE = {}", engine.value),
        ));
    }
    if let Some(TablePartition::LegacyRange(partition)) = &statement.partition {
        return Some(unsupported(
            partition.span,
            "CREATE TABLE does not support legacy RANGE partition definitions".to_string(),
        ));
    }
    if !statement.order_by.is_empty() {
        return Some(unsupported(
            statement.span,
            "CREATE TABLE does not support ORDER BY".to_string(),
        ));
    }
    None
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
        DmlStatement::Delete(statement) => dml_statement_result(dml.execute_delete(
            delete_engine,
            crate::query_execution::dml::delete::DeleteStatement::Predicate(statement),
            source,
            context,
            Some(query_options),
        )),
        DmlStatement::AddEqualityDelete(statement) => dml_statement_result(dml.execute_delete(
            delete_engine,
            crate::query_execution::dml::delete::DeleteStatement::Equality(statement),
            source,
            context,
            Some(query_options),
        )),
        DmlStatement::Update(_) | DmlStatement::Merge(_) => {
            dml_statement_result(dml.try_execute_typed_mutation(
                mutation_engine,
                statement,
                source,
                context,
                Some(query_options),
            ))
        }
        DmlStatement::CreateTableAsSelect(statement) => dml_statement_result(dml.try_execute_ctas(
            ctas_engine,
            statement,
            source,
            context,
            Some(query_options),
        )),
    }
}

fn add_files_status(file_count: u32) -> Result<QueryResult, String> {
    let column = QueryResultColumn {
        name: "status".to_string(),
        data_type: DataType::Utf8,
        nullable: false,
        logical_type: None,
    };
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "status",
            DataType::Utf8,
            false,
        )])),
        vec![Arc::new(StringArray::from(vec![format!(
            "Added {file_count} file(s)"
        )]))],
    )
    .map_err(|error| format!("build ADD FILES status result failed: {error}"))?;
    Ok(QueryResult {
        columns: vec![column],
        chunks: vec![record_batch_to_chunk(batch)?],
    })
}

fn requires_lake_publication_deadline(statement: &ParsedStatement) -> bool {
    match statement {
        ParsedStatement::Dml(_) | ParsedStatement::Table(_) | ParsedStatement::Iceberg(_) => true,
        ParsedStatement::Catalog(statement) => {
            !matches!(statement, ast::CatalogStatement::ShowCreateTable(_))
        }
        ParsedStatement::Maintenance(statement) => {
            !matches!(statement, ast::MaintenanceStatement::ShowOptimize(_))
        }
        ParsedStatement::MaterializedView(statement) => !matches!(
            statement,
            ast::MaterializedViewStatement::Show(_)
                | ast::MaterializedViewStatement::ExplainRefresh(_)
        ),
        ParsedStatement::View(statement) => !matches!(
            statement,
            ast::ViewStatement::Show(_) | ast::ViewStatement::ShowCreate(_)
        ),
        ParsedStatement::Statistics(statement) => matches!(
            statement,
            ast::StatisticsStatement::AnalyzeTable(_)
                | ast::StatisticsStatement::DropStats(_)
                | ast::StatisticsStatement::DropHistogram(_)
                | ast::StatisticsStatement::DropMultipleColumnsStats(_)
        ),
        ParsedStatement::ShowBackends(_) => false,
        ParsedStatement::Session(_)
        | ParsedStatement::Query(_)
        | ParsedStatement::ExplainQuery(_) => false,
    }
}

/// Design: ADR-0012 (docs/adr/ADR-0012-frontend-query-session-router.md)
#[derive(Clone)]
pub struct FrontendQueryService {
    session_catalog_resolver: SessionCatalogResolver,
    query_compiler: FrontendQueryCompiler,
    command_executor: Arc<dyn CoreCommandRoute>,
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
    /// Cost budget frozen from `[runtime]` and handed to statement admission
    /// whenever the session did not set one itself.
    optimizer_query_mem_limit_bytes: u64,
    lake_publication_runtime_policy: LakePublicationRuntimePolicy,
    serving_lifecycle: FrontendServingLifecycle,
}

impl FrontendQueryService {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        session_catalog_resolver: SessionCatalogResolver,
        query_compiler: FrontendQueryCompiler,
        catalog_command_executor: CatalogCommandExecutor,
        statistics_command_executor: StatisticsCommandExecutor,
        backend_command_executor: BackendCommandExecutor,
        view_command_executor: ViewCommandExecutor,
        iceberg_ref_command_executor: IcebergRefCommandExecutor,
        mv_command_executor: MvCommandExecutor,
        maintenance_command_executor: MaintenanceCommandExecutor,
        maintenance_read_command_executor: MaintenanceReadCommandExecutor,
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
        optimizer_query_mem_limit_bytes: u64,
        lake_publication_runtime_policy: LakePublicationRuntimePolicy,
    ) -> Self {
        Self {
            session_catalog_resolver,
            query_compiler,
            command_executor: Arc::new(TypedCommandRoute::new(
                catalog_command_executor,
                statistics_command_executor,
                backend_command_executor,
                view_command_executor,
                iceberg_ref_command_executor,
                mv_command_executor,
                maintenance_command_executor,
                maintenance_read_command_executor,
            )),
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
            optimizer_query_mem_limit_bytes,
            lake_publication_runtime_policy,
            // A deployable FE must install its shared lifecycle before
            // opening MySQL. Keep an omitted install fail-closed rather than
            // silently creating a second ready admission authority.
            serving_lifecycle: FrontendServingLifecycle::new(),
        }
    }

    /// Installs the process-wide serving owner composed by the FE server.
    ///
    /// The constructor is intentionally fail-closed; deployable composition
    /// must install its shared lifecycle before opening the MySQL listener.
    pub(crate) fn with_serving_lifecycle(mut self, lifecycle: FrontendServingLifecycle) -> Self {
        self.serving_lifecycle = lifecycle;
        self
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
            .serving_lifecycle
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
            active_statements: Mutex::new(Vec::new()),
            state: Mutex::new(FrontendSessionState::default()),
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
        FrontendAdmissionError::SessionRequiresRegistration => QueryServiceError::new(
            QueryServiceErrorKind::Internal,
            "session admission must use lifecycle registration",
        ),
    }
}

#[derive(Clone)]
struct FrontendSessionState {
    current_catalog: Option<String>,
    current_database: String,
    execution_settings: SessionExecutionSettings,
    optimizer_settings: SessionOptimizerSettings,
    user_variables: BTreeMap<String, String>,
}

impl Default for FrontendSessionState {
    fn default() -> Self {
        Self {
            current_catalog: None,
            current_database: DEFAULT_DATABASE.to_string(),
            execution_settings: SessionExecutionSettings::default(),
            optimizer_settings: SessionOptimizerSettings::default(),
            user_variables: BTreeMap::new(),
        }
    }
}

struct FrontendQuerySession {
    service: FrontendQueryService,
    lease: Mutex<Option<QuerySessionLease>>,
    active_statements: Mutex<Vec<FrontendWorkloadLease>>,
    state: Mutex<FrontendSessionState>,
}

impl FrontendQuerySession {
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

    async fn execute_statement(
        &self,
        statement: &str,
        admission: FrontendWorkloadLease,
    ) -> Result<StatementResult, QueryServiceError> {
        let cancellation = admission.cancellation_source();
        let trimmed = strip_leading_line_comments(statement.trim().trim_end_matches(';').trim());
        if trimmed.is_empty() {
            return Ok(StatementResult::Ok);
        }
        if let Some(error) = admin_raise_engine_error(trimmed)? {
            return Err(error);
        }
        let statements = novarocks_parser::parse(trimmed)
            .map_err(|error| QueryServiceError::from_user_error(error.to_user_error(trimmed)))?;
        let [parsed_statement] = statements.as_slice() else {
            return Err(QueryServiceError::new(
                QueryServiceErrorKind::Parse,
                "command admission requires exactly one statement",
            ));
        };
        let result = match parsed_statement {
            ParsedStatement::Session(ast::SessionStatement::Set(statement))
                if statement
                    .assignments
                    .iter()
                    .any(|assignment| matches!(assignment.value, ast::SetValue::Query(_))) =>
            {
                drop(admission);
                return self
                    .execute_governed_set(trimmed.to_string(), statement)
                    .await;
            }
            ParsedStatement::Session(statement) => {
                self.execute_session_statement(trimmed, statement, cancellation)
                    .await
            }
            ParsedStatement::Query(_) => {
                // The process serving latch has completed its readiness check.
                // Plain reads now have one business admission and one active
                // generation, both owned by the workload-backed statement owner.
                drop(admission);
                return self
                    .execute_governed_read(trimmed.to_string(), parsed_statement.clone())
                    .await;
            }
            statement => {
                self.execute_typed_statement(trimmed.to_string(), statement.clone(), cancellation)
                    .await
            }
        };
        if matches!(&result, Ok(StatementResult::Query(_))) {
            let mut active_statements = self.active_statements.lock().map_err(poisoned_state)?;
            // A QueryResult is consumed by the MySQL protocol after this
            // method returns. Retain the admission through that write so an
            // FE drain observes streaming work and can cancel it at deadline.
            active_statements.push(admission);
        }
        result
    }

    fn complete_active_statement(&self) {
        if let Ok(mut active_statements) = self.active_statements.lock() {
            active_statements.clear();
        }
    }

    async fn execute_session_statement(
        &self,
        source: &str,
        statement: &ast::SessionStatement,
        _cancellation: QueryCancellationSource,
    ) -> Result<StatementResult, QueryServiceError> {
        match statement {
            ast::SessionStatement::Set(statement) => {
                for assignment in &statement.assignments {
                    self.admit_session_set_assignment(source, assignment)?;
                }
                for assignment in &statement.assignments {
                    self.apply_session_set_assignment(source, assignment)
                        .await?;
                }
                Ok(StatementResult::Ok)
            }
            ast::SessionStatement::Use(statement) => {
                let schema = statement.catalog.as_ref().map_or_else(
                    || statement.database.value.clone(),
                    |catalog| format!("{}.{}", catalog.value, statement.database.value),
                );
                self.init_database(&schema).await?;
                Ok(StatementResult::Ok)
            }
            ast::SessionStatement::Kill(statement) => self.execute_session_kill(source, statement),
            ast::SessionStatement::TransactionControl(statement) => {
                Err(QueryServiceError::from_user_error(
                    crate::session_error::SessionAdmitError::TransactionUnsupported.to_user_error(
                        source,
                        statement.span,
                        format!(
                            "{} is not supported because NovaRocks only provides statement-level autocommit frontiers",
                            statement.kind.sql()
                        ),
                    ),
                ))
            }
        }
    }

    fn admit_session_set_assignment(
        &self,
        source: &str,
        assignment: &ast::SetAssignment,
    ) -> Result<(), QueryServiceError> {
        let ast::SetTarget::SystemVariable(variable) = &assignment.target else {
            return Ok(());
        };
        if !variable.value.eq_ignore_ascii_case("autocommit") {
            return Ok(());
        }
        match lower_autocommit_setting(&assignment.value)? {
            AutocommitSetting::Enabled => Ok(()),
            AutocommitSetting::Disabled => Err(QueryServiceError::from_user_error(
                crate::session_error::SessionAdmitError::TransactionUnsupported.to_user_error(
                    source,
                    assignment.span,
                    "SET autocommit=0 is not supported because NovaRocks only provides statement-level autocommit frontiers",
                ),
            )),
        }
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
        state: &mut FrontendSessionState,
    ) -> Result<(), QueryServiceError> {
        match &assignment.target {
            ast::SetTarget::UserVariable(variable) => {
                let value = match &assignment.value {
                    ast::SetValue::Expression(value) => print_expr(value),
                    ast::SetValue::Query(_) => {
                        return Err(internal_error(
                            "SET query value bypassed the governed scalar route",
                        ));
                    }
                    ast::SetValue::Words(_) => {
                        return Err(QueryServiceError::new(
                            QueryServiceErrorKind::InvalidValue,
                            "user variable assignment requires an expression",
                        ));
                    }
                };
                state
                    .user_variables
                    .insert(variable.value.to_ascii_lowercase(), value);
                Ok(())
            }
            ast::SetTarget::SystemVariable(variable) => {
                let name = variable.value.to_ascii_lowercase();
                if matches!(assignment.scope, ast::SetScope::Global)
                    && is_known_session_setting(&name)
                {
                    return Err(QueryServiceError::from_user_error(
                        crate::session_error::SessionAdmitError::GlobalScopeUnsupported
                            .to_user_error(
                                source,
                                assignment.span,
                                format!("SET GLOBAL {name} is not supported"),
                            ),
                    ));
                }
                let value = session_setting_value(&assignment.value)?;
                self.apply_session_system_variable(state, &name, &value)
            }
            ast::SetTarget::Catalog { .. } => {
                if matches!(assignment.scope, ast::SetScope::Global) {
                    return Err(QueryServiceError::from_user_error(
                        crate::session_error::SessionAdmitError::GlobalScopeUnsupported
                            .to_user_error(
                                source,
                                assignment.span,
                                "SET GLOBAL CATALOG is not supported",
                            ),
                    ));
                }
                let catalog = session_catalog_value(&assignment.value)?;
                self.apply_session_catalog(state, &catalog)
            }
            ast::SetTarget::Names { .. } | ast::SetTarget::Transaction { .. } => Ok(()),
        }
    }

    fn apply_session_system_variable(
        &self,
        state: &mut FrontendSessionState,
        name: &str,
        value: &str,
    ) -> Result<(), QueryServiceError> {
        if name == "catalog" {
            return self.apply_session_catalog(state, value);
        }
        if name == "autocommit" {
            // `admit_session_set_assignment` has already lowered and accepted
            // only the truthful enabled spelling. Do not turn this into a
            // session state bit: the portable SQL profile has no transaction
            // state to retain across statements.
            debug_assert!(matches!(
                lower_autocommit_value(value),
                Ok(AutocommitSetting::Enabled)
            ));
            return Ok(());
        }
        match name {
            "query_timeout" => {
                let seconds = value.parse::<u64>().map_err(|_| {
                    QueryServiceError::new(
                        QueryServiceErrorKind::InvalidValue,
                        "invalid query_timeout",
                    )
                })?;
                state.execution_settings.set_query_timeout_secs(seconds);
            }
            "group_concat_max_len" => {
                let value = value.parse::<i64>().map_err(|_| {
                    QueryServiceError::new(
                        QueryServiceErrorKind::InvalidValue,
                        "invalid group_concat_max_len",
                    )
                })?;
                state.execution_settings.set_group_concat_max_len(value);
            }
            "pipeline_dop" => {
                let value = value.parse::<i32>().map_err(|_| {
                    QueryServiceError::new(
                        QueryServiceErrorKind::InvalidValue,
                        "invalid pipeline_dop",
                    )
                })?;
                state.execution_settings.set_pipeline_dop(value);
            }
            "enable_parquet_reader_page_index" => {
                state
                    .execution_settings
                    .set_enable_parquet_reader_page_index(parse_bool(value)?);
            }
            "enable_scan_datacache" => {
                state
                    .execution_settings
                    .set_enable_scan_datacache(parse_bool(value)?);
            }
            "enable_populate_datacache" => {
                state
                    .execution_settings
                    .set_enable_populate_datacache(parse_bool(value)?);
            }
            "runtime_filter_scan_wait_time" => {
                let value = value.parse::<i64>().map_err(|_| {
                    QueryServiceError::new(
                        QueryServiceErrorKind::InvalidValue,
                        "invalid runtime_filter_scan_wait_time",
                    )
                })?;
                state
                    .execution_settings
                    .set_runtime_filter_scan_wait_time_ms(value)?;
            }
            "global_runtime_filter_wait_timeout" => {
                let value = value.parse::<i32>().map_err(|_| {
                    QueryServiceError::new(
                        QueryServiceErrorKind::InvalidValue,
                        "invalid global_runtime_filter_wait_timeout",
                    )
                })?;
                state
                    .execution_settings
                    .set_runtime_filter_wait_timeout_ms(value)?;
            }
            "disable_optimizer_rules" | "cbo_disabled_rules" => {
                state.optimizer_settings.set_disabled_rules(
                    value
                        .split(',')
                        .map(str::trim)
                        .filter(|rule| !rule.is_empty())
                        .map(ToOwned::to_owned)
                        .collect(),
                );
            }
            "enable_eliminate_agg" => {
                state
                    .optimizer_settings
                    .set_enable_eliminate_agg(parse_bool(value)?);
            }
            "enable_ukfk_opt" => {
                state
                    .optimizer_settings
                    .set_enable_ukfk_opt(parse_bool(value)?);
            }
            _ => apply_optimizer_session_set(&mut state.optimizer_settings, name, value)?,
        }
        Ok(())
    }

    fn apply_session_catalog(
        &self,
        state: &mut FrontendSessionState,
        catalog: &str,
    ) -> Result<(), QueryServiceError> {
        let catalog = resolve_catalog_name(&self.service.session_catalog_resolver, catalog)?;
        state.current_catalog = catalog;
        if state.current_catalog.is_none()
            && !self
                .service
                .session_catalog_resolver
                .database_exists(&state.current_database)
                .map_err(internal_error)?
        {
            state.current_database = DEFAULT_DATABASE.to_string();
        }
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
            Some(self.service.client_connection_control.as_ref()),
        )
    }

    async fn prepare_governed_query_operation(
        &self,
        sql: &str,
        parsed_statement: ParsedStatement,
        state: FrontendSessionState,
        deadline: Option<Instant>,
        timeout_ms: Option<u64>,
        statement_token: StatementToken,
        cancellation: novarocks_workload_control::CancellationView,
    ) -> Result<PreparedQueryOperation, QueryServiceError> {
        let assignments = state
            .user_variables
            .iter()
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect::<Vec<_>>();
        let parsed_statement = substitute_session_user_variables(parsed_statement, &assignments)
            .map_err(|error| internal_error(error.to_string()))?;
        let cancellation = QueryCancellationView::governed(cancellation, timeout_ms);
        let topology =
            wait_for_initial_query_topology(&self.service.topology, &cancellation, deadline)
                .await?;
        let mut optimizer_settings = state.optimizer_settings;
        if optimizer_settings.optimizer_query_mem_limit_bytes.is_none() {
            optimizer_settings.optimizer_query_mem_limit_bytes =
                Some(self.service.optimizer_query_mem_limit_bytes as f64);
        }
        let context = RequestContext::admit(RequestAdmission::new(
            state.current_catalog,
            state.current_database,
            self.service.role,
            topology,
            deadline,
            cancellation,
            optimizer_settings,
        ));
        let query_options = with_query_hints(
            state.execution_settings.query_options(),
            match &parsed_statement {
                ParsedStatement::Query(query) => Some(query),
                _ => None,
            },
        );
        let compiler = self.service.query_compiler.clone();
        let prepared = task::spawn_blocking(move || {
            let _diagnostic_scope =
                crate::preparation_diagnostics::enter_statement(statement_token);
            compiler.prepare_statement(&parsed_statement, &context, Some(query_options))
        })
        .await
        .map_err(|error| internal_error(error.to_string()))?;
        match prepared {
            Ok(operation) => Ok(operation),
            Err(FrontendQueryCompilerError::Engine(error)) => Err(internal_error(error)),
            Err(FrontendQueryCompilerError::Analyze(error)) => Err(
                QueryServiceError::from_user_error(error.to_user_error(Some(sql))),
            ),
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
            .map_err(governed_statement_begin_error)?;

        for assignment in &set.assignments {
            let ast::SetTarget::UserVariable(variable) = &assignment.target else {
                if let Err(error) = self.apply_session_set_assignment_to_state(
                    &source,
                    assignment,
                    &mut staged_state,
                ) {
                    let _ = statement.finish();
                    return Err(error);
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
                            let _ = statement.finish();
                            return Err(error);
                        }
                    }
                }
                ast::SetValue::Words(_) => {
                    let _ = statement.finish();
                    return Err(QueryServiceError::new(
                        QueryServiceErrorKind::InvalidValue,
                        "user variable assignment requires an expression",
                    ));
                }
            };
            staged_state
                .user_variables
                .insert(variable.value.to_ascii_lowercase(), value);
        }
        let mut live_state = self.state.lock().map_err(poisoned_state)?;
        match statement.seal_success_visibility() {
            crate::query_execution::control::GovernedStatementVisibilitySealOutcome::Sealed => {
                *live_state = staged_state;
                drop(live_state);
                match statement.finish() {
                    GovernedStatementFinishOutcome::Completed => Ok(StatementResult::Ok),
                    GovernedStatementFinishOutcome::Cancelled(reason) => {
                        Err(governed_cancellation_error(reason))
                    }
                    GovernedStatementFinishOutcome::ProtocolFailed
                    | GovernedStatementFinishOutcome::Stale => Err(internal_error(
                        "governed SET query lost its sealed statement generation",
                    )),
                }
            }
            crate::query_execution::control::GovernedStatementVisibilitySealOutcome::Cancelled(
                reason,
            ) => {
                drop(live_state);
                let _ = statement.finish();
                Err(governed_cancellation_error(reason))
            }
            crate::query_execution::control::GovernedStatementVisibilitySealOutcome::Stale => {
                drop(live_state);
                let _ = statement.finish();
                Err(internal_error(
                    "governed SET query lost its statement generation before state commit",
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
        statement: &crate::query_execution::control::GovernedQueryStatementOwner,
        state: FrontendSessionState,
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
            .await?;
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
                        crate::user_variable::query_result_to_user_variable_literal(&result)
                            .map_err(scalar_query_error)
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
            .map_err(governed_statement_begin_error)?;
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
            Err(error) => {
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
                    StatementResult::GovernedQuery(_) | StatementResult::StreamingQuery(_) => {
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
        crate::runtime::statement_result::StreamingStatementResult::try_from_execution(
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
        cancellation_source: QueryCancellationSource,
    ) -> Result<StatementResult, QueryServiceError> {
        reject_plain_query_from_legacy_typed_route(&parsed_statement)?;
        let state = self.state.lock().map_err(poisoned_state)?.clone();
        let assignments = state
            .user_variables
            .iter()
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect::<Vec<_>>();
        let parsed_statement = substitute_session_user_variables(parsed_statement, &assignments)
            .map_err(|error| internal_error(error.to_string()))?;
        let token = self.token()?;
        let mut active = self
            .service
            .query_control
            .begin_statement_with_cancellation(token, cancellation_source)
            .map_err(|error| {
                QueryServiceError::new(
                    QueryServiceErrorKind::Internal,
                    format!("begin statement failed: {error:?}"),
                )
            })?;
        let cancellation = active.cancellation().clone();
        let query_timeout_secs = state.execution_settings.query_timeout_secs();
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
        let topology = match self.service.topology.snapshot() {
            Ok(snapshot) => snapshot,
            Err(error) => {
                let _ = active.finish();
                return Err(QueryServiceError::new(
                    QueryServiceErrorKind::Internal,
                    error.to_string(),
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
            let topology_service = Arc::clone(&self.service.topology);
            match task::spawn_blocking(move || {
                topology_service.wait_for_eligible_after(wait_after_revision, wait_deadline)
            })
            .await
            {
                Ok(Ok(snapshot)) => snapshot,
                Ok(Err(error)) => {
                    let _ = active.finish();
                    return Err(QueryServiceError::new(
                        QueryServiceErrorKind::Internal,
                        error.to_string(),
                    ));
                }
                Err(error) => {
                    let _ = active.finish();
                    return Err(internal_error(error.to_string()));
                }
            }
        } else {
            topology
        };
        let mut optimizer_settings = state.optimizer_settings;
        // A session `SET` wins; otherwise admission freezes the process budget so
        // SQL costing never consults a process-global configuration.
        if optimizer_settings.optimizer_query_mem_limit_bytes.is_none() {
            optimizer_settings.optimizer_query_mem_limit_bytes =
                Some(self.service.optimizer_query_mem_limit_bytes as f64);
        }
        let context = RequestContext::admit(RequestAdmission::new(
            state.current_catalog,
            state.current_database,
            self.service.role,
            topology,
            deadline,
            cancellation.clone(),
            optimizer_settings,
        ));
        let compiler = self.service.query_compiler.clone();
        let command_executor = Arc::clone(&self.service.command_executor);
        let query_execution = self.service.query_execution.clone();
        let dml = Arc::clone(&self.service.dml);
        let insert_engine = Arc::clone(&self.service.insert_engine);
        let delete_engine = Arc::clone(&self.service.delete_engine);
        let mutation_engine = Arc::clone(&self.service.mutation_engine);
        let add_files_engine = Arc::clone(&self.service.add_files_engine);
        let ctas_engine = Arc::clone(&self.service.ctas_engine);
        let truncate_engine = Arc::clone(&self.service.truncate_engine);
        let query_options = with_query_hints(
            state.execution_settings.query_options(),
            match &parsed_statement {
                ParsedStatement::ExplainQuery(explain) => Some(&explain.query),
                _ => None,
            },
        );
        let diagnostic_statement = active.token();
        let mut worker = task::spawn_blocking(move || {
            let _diagnostic_scope =
                crate::preparation_diagnostics::enter_statement(diagnostic_statement);
            let result: Result<StatementResult, RoutedExecutionError> = {
                let statement = parsed_statement;
                if matches!(statement, ParsedStatement::ExplainQuery(_)) {
                    compiler
                        .prepare_statement(&statement, &context, Some(query_options))
                        .and_then(|operation| {
                            execute_prepared_query(operation, &query_execution)
                                .map_err(FrontendQueryCompilerError::Engine)
                        })
                        .map_err(|error| match error {
                            FrontendQueryCompilerError::Engine(error) => {
                                RoutedExecutionError::Engine(error)
                            }
                            FrontendQueryCompilerError::Analyze(error) => {
                                RoutedExecutionError::User(error.to_user_error(Some(&sql)))
                            }
                        })
                } else if let ParsedStatement::Dml(statement) = &statement {
                    execute_typed_dml_statement(
                        dml.as_ref(),
                        insert_engine.as_ref(),
                        delete_engine.as_ref(),
                        mutation_engine.as_ref(),
                        ctas_engine.as_ref(),
                        statement,
                        &sql,
                        &context,
                        &query_options,
                    )
                } else if let ParsedStatement::Table(table_statement) = &statement {
                    if let Some(error) = table_statement_admission_error(table_statement, &sql) {
                        Err(RoutedExecutionError::User(error))
                    } else {
                        command_executor
                            .execute_typed(&statement, &context, query_options)
                            .map_err(RoutedExecutionError::Engine)
                    }
                } else if let ParsedStatement::Catalog(
                    novarocks_parser::ast::CatalogStatement::TruncateTable(statement),
                ) = &statement
                {
                    dml.execute_truncate(
                        truncate_engine.as_ref(),
                        crate::query_execution::dml::truncate::command_from_typed_statement(
                            statement,
                        ),
                        &context,
                        Some(&query_options),
                    )
                    .map(|()| StatementResult::Ok)
                    .map_err(|error| RoutedExecutionError::Engine(error.to_string()))
                } else if let ParsedStatement::Iceberg(
                    novarocks_parser::ast::IcebergStatement::AlterTable(iceberg_statement),
                ) = &statement
                {
                    match crate::query_execution::dml::add_files::command_from_typed_statement(
                        iceberg_statement,
                    ) {
                        Ok(command) => dml
                            .execute_add_files(
                                add_files_engine.as_ref(),
                                command,
                                &context,
                                Some(&query_options),
                            )
                            .map_err(|error| RoutedExecutionError::Engine(error.to_string()))
                            .and_then(|count| {
                                add_files_status(count)
                                    .map(StatementResult::Query)
                                    .map_err(RoutedExecutionError::Engine)
                            }),
                        Err(_) => command_executor
                            .execute_typed(&statement, &context, query_options)
                            .map_err(RoutedExecutionError::Engine),
                    }
                } else {
                    command_executor
                        .execute_typed(&statement, &context, query_options)
                        .map_err(RoutedExecutionError::Engine)
                }
            };
            let completion = active.finish();
            (result, completion)
        });
        let result = if let Some(timeout_duration) = timeout_duration {
            match tokio::time::timeout(timeout_duration, &mut worker).await {
                Ok(result) => result.map_err(|error| internal_error(error.to_string()))?,
                Err(_) => {
                    let timeout_ms = timeout_message_millis(timeout_duration);
                    self.cancel_current(QueryCancellationReason::DeadlineExceeded { timeout_ms });
                    // A timeout is not complete until the worker releases the
                    // statement lease. Waiting here also fences Backend abort
                    // acknowledgement before this session admits its next SQL.
                    let _ = worker.await;
                    return Err(QueryServiceError::new(
                        QueryServiceErrorKind::Timeout,
                        format!("query timed out after {timeout_ms} ms"),
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
                    result = &mut worker => break result.map_err(|error| internal_error(error.to_string()))?,
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
                                    .map_err(|error| internal_error(error.to_string()))?;
                            }
                            return Err(cancellation_error(reason));
                        }
                    }
                }
            }
        };
        let (result, completion) = result;
        match completion {
            StatementFinishOutcome::Cancelled(reason) => Err(cancellation_error(reason)),
            StatementFinishOutcome::Stale if cancellation.is_cancelled() => Err(
                cancellation_error(cancellation.reason().expect("cancelled view has a reason")),
            ),
            StatementFinishOutcome::Completed | StatementFinishOutcome::Stale => {
                result.map_err(|error| match error {
                    RoutedExecutionError::Engine(error) => internal_error(error),
                    RoutedExecutionError::User(error) => QueryServiceError::from_user_error(error),
                    RoutedExecutionError::Publication { message, terminal } => {
                        QueryServiceError::with_publication_terminal(message, terminal)
                    }
                })
            }
        }
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

fn session_setting_value(value: &ast::SetValue) -> Result<String, QueryServiceError> {
    match value {
        ast::SetValue::Expression(value) => Ok(print_expr(value)
            .trim_matches('\'')
            .trim_matches('"')
            .to_string()),
        ast::SetValue::Words(words) => {
            let [ast::SetWord::Ident(value)] = words.as_slice() else {
                return Err(QueryServiceError::new(
                    QueryServiceErrorKind::InvalidValue,
                    "session variable assignment requires an expression",
                ));
            };
            if matches!(value.value.to_ascii_lowercase().as_str(), "on" | "off") {
                Ok(value.value.to_ascii_lowercase())
            } else {
                Err(QueryServiceError::new(
                    QueryServiceErrorKind::InvalidValue,
                    "session variable assignment requires an expression",
                ))
            }
        }
        ast::SetValue::Query(_) => Err(QueryServiceError::new(
            QueryServiceErrorKind::InvalidValue,
            "session variable assignment requires an expression",
        )),
    }
}

/// Closed lowering for the one session setting that would otherwise create a
/// cross-statement publication boundary. Other boolean settings remain on the
/// generic session-value path; autocommit must not silently inherit its
/// permissive/no-op behavior.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AutocommitSetting {
    Enabled,
    Disabled,
}

fn lower_autocommit_setting(value: &ast::SetValue) -> Result<AutocommitSetting, QueryServiceError> {
    let value = session_setting_value(value)?;
    lower_autocommit_value(&value)
}

fn lower_autocommit_value(value: &str) -> Result<AutocommitSetting, QueryServiceError> {
    match value.to_ascii_lowercase().as_str() {
        "1" | "on" | "true" => Ok(AutocommitSetting::Enabled),
        "0" | "off" | "false" => Ok(AutocommitSetting::Disabled),
        _ => Err(QueryServiceError::new(
            QueryServiceErrorKind::InvalidValue,
            format!("invalid autocommit value `{value}`; expected 1, ON, TRUE, 0, OFF, or FALSE"),
        )),
    }
}

fn session_catalog_value(value: &ast::SetValue) -> Result<String, QueryServiceError> {
    if let ast::SetValue::Expression(value) = value {
        return Ok(print_expr(value)
            .trim_matches('\'')
            .trim_matches('"')
            .to_string());
    }
    let ast::SetValue::Words(words) = value else {
        return Err(QueryServiceError::new(
            QueryServiceErrorKind::InvalidValue,
            "SET CATALOG requires a catalog name",
        ));
    };
    let [ast::SetWord::Ident(catalog)] = words.as_slice() else {
        return Err(QueryServiceError::new(
            QueryServiceErrorKind::InvalidValue,
            "SET CATALOG requires a catalog name",
        ));
    };
    Ok(catalog.value.clone())
}

fn is_known_session_setting(name: &str) -> bool {
    matches!(
        name,
        "autocommit"
            | "catalog"
            | "query_timeout"
            | "group_concat_max_len"
            | "pipeline_dop"
            | "enable_parquet_reader_page_index"
            | "enable_scan_datacache"
            | "enable_populate_datacache"
            | "runtime_filter_scan_wait_time"
            | "global_runtime_filter_wait_timeout"
            | "disable_optimizer_rules"
            | "cbo_disabled_rules"
            | "enable_eliminate_agg"
            | "enable_ukfk_opt"
            | "cbo_broadcast_backend_count"
            | "cbo_broadcast_node_mem_budget_bytes"
            | "global_runtime_filter_build_max_size"
            | "global_runtime_filter_build_min_size"
            | "global_runtime_filter_probe_min_size"
            | "global_runtime_filter_probe_min_selectivity"
            | "cbo_max_reorder_node_use_exhaustive"
            | "cbo_max_reorder_node_use_dp"
            | "cbo_max_reorder_node_use_greedy"
            | "cbo_max_reorder_node"
            | "enable_query_rewrite_table_prune"
            | "enable_cbo_table_prune"
            | "enable_table_prune_on_update"
            | "enable_common_subexpr_reuse"
            | "enable_global_runtime_filter"
            | "enable_materialized_view_rewrite"
            | "enable_connector_static_predicate_pushdown"
            | "cbo_enable_dp_join_reorder"
            | "cbo_enable_greedy_join_reorder"
            | "enable_global_runtime_filter_cross_exchange"
    )
}

fn substitute_session_user_variables(
    statement: ParsedStatement,
    assignments: &[(String, String)],
) -> Result<ParsedStatement, String> {
    if assignments.is_empty() {
        return Ok(statement);
    }

    let mut values = BTreeMap::new();
    for (name, value) in assignments {
        let statements = novarocks_parser::parse(&format!("SELECT {value}"))
            .map_err(|error| format!("invalid session user variable {name}: {error}"))?;
        let [ParsedStatement::Query(query)] = statements.as_slice() else {
            return Err(format!("invalid session user variable {name}"));
        };
        let ast::SetExpr::Select(select) = query.body.as_ref() else {
            return Err(format!("invalid session user variable {name}"));
        };
        let [item] = select.projection.as_slice() else {
            return Err(format!("invalid session user variable {name}"));
        };
        let expression = match item {
            ast::SelectItem::UnnamedExpr(expression)
            | ast::SelectItem::ExprWithAlias {
                expr: expression, ..
            } => expression.clone(),
            ast::SelectItem::Wildcard { .. } | ast::SelectItem::QualifiedWildcard { .. } => {
                return Err(format!("invalid session user variable {name}"));
            }
        };
        values.insert(name.to_ascii_lowercase(), expression);
    }

    struct Substituter {
        values: BTreeMap<String, ast::Expr>,
    }

    impl Fold for Substituter {
        fn fold_expr(&mut self, expression: ast::Expr) -> ast::Expr {
            if let ast::Expr::UserVariable(variable) = &expression
                && let Some(value) = self.values.get(&variable.value.to_ascii_lowercase())
            {
                return value.clone();
            }
            ast::fold_expr(self, expression)
        }
    }

    Ok(Substituter { values }.fold_statement(statement))
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
    async fn init_database(&self, schema: &str) -> Result<(), QueryServiceError> {
        let current_catalog = self
            .state
            .lock()
            .map_err(poisoned_state)?
            .current_catalog
            .clone();
        let session_catalog_resolver = self.service.session_catalog_resolver.clone();
        let schema = schema.to_string();
        let context = task::spawn_blocking(move || {
            resolve_database_context(
                &session_catalog_resolver,
                current_catalog.as_deref(),
                &schema,
            )
        })
        .await
        .map_err(|error| internal_error(error.to_string()))??;
        let mut state = self.state.lock().map_err(poisoned_state)?;
        state.current_catalog = context.catalog;
        state.current_database = context.database;
        Ok(())
    }

    async fn execute_batch(&self, sql: &str) -> Result<StatementResult, QueryServiceError> {
        // Match the standalone MySQL session contract: a batch returns its
        // most recent result set even when subsequent DDL/session statements
        // complete successfully. In particular, all-in-one routes through
        // this frontend session before reaching the Stage/Start lifecycle.
        let mut last_query_result = None;
        let mut cursor = SqlBatchCursor::new(sql);
        while cursor.has_remaining() {
            // The lifecycle lease intentionally precedes fragment scanning and
            // parsing. This makes the admission latch the linearization point
            // even for malformed SQL and for later batch fragments.
            let admission = self
                .service
                .serving_lifecycle
                .try_admit(FrontendWorkloadKind::Statement)
                .map_err(query_service_admission_error)?;
            let Some(statement) = cursor.next_fragment()? else {
                break;
            };
            // Empty/comment-only fragments release their just-acquired lease
            // before any session mutation or execution starts.
            if strip_leading_line_comments(statement.trim()).is_empty() {
                continue;
            }
            match self.execute_statement(statement, admission).await? {
                StatementResult::Query(result) => last_query_result = Some(result),
                StatementResult::GovernedQuery(result) => {
                    if cursor.has_nonempty_remaining()? {
                        drop(result);
                        return Err(QueryServiceError::new(
                            QueryServiceErrorKind::Unsupported,
                            "a governed SELECT must be the final nonempty statement in its SQL batch",
                        ));
                    }
                    return Ok(StatementResult::GovernedQuery(result));
                }
                StatementResult::StreamingQuery(result) => {
                    if cursor.has_nonempty_remaining()? {
                        drop(result);
                        return Err(QueryServiceError::new(
                            QueryServiceErrorKind::Unsupported,
                            "a streaming SELECT must be the final nonempty statement in its SQL batch",
                        ));
                    }
                    return Ok(StatementResult::StreamingQuery(result));
                }
                StatementResult::Ok => {}
            }
        }
        Ok(last_query_result
            .map(StatementResult::Query)
            .unwrap_or(StatementResult::Ok))
    }

    fn complete_statement(&self) {
        self.complete_active_statement();
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
        self.complete_active_statement();
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

/// Resolves the frontend-owned KILL semantics after the protocol owner has
/// supplied its transport-neutral connection-control capability.
///
/// Connection lifecycle composition is intentionally outside this helper: the
/// MySQL protocol owner supplies the port only once its registry and runner
/// share the same instance. Until then, a connection KILL cannot be admitted.
fn execute_kill_statement(
    source: &str,
    statement: &ast::KillStatement,
    requester: SessionToken,
    query_control: &QueryControlService,
    connection_control: Option<&dyn ClientConnectionControlPort>,
) -> Result<StatementResult, QueryServiceError> {
    let connection_id = kill_connection_id(statement)?;
    match statement.kind {
        ast::KillKind::Query => match query_control.kill_query(requester, connection_id) {
            QueryCancelOutcome::Requested
            | QueryCancelOutcome::AlreadyRequested(_)
            | QueryCancelOutcome::NoActiveStatement => Ok(StatementResult::Ok),
            QueryCancelOutcome::Failed(error) => Err(internal_error(format!(
                "request governed query cancellation failed: {error}"
            ))),
            QueryCancelOutcome::UnknownSession => Err(no_such_connection_error(connection_id)),
            QueryCancelOutcome::PermissionDenied => Err(kill_denied_error(source, statement)),
        },
        ast::KillKind::Default | ast::KillKind::Connection => {
            let target = match query_control.authorize_connection_kill(requester, connection_id) {
                ConnectionKillAuthorization::Authorized(target) => target,
                ConnectionKillAuthorization::UnknownSession => {
                    return Err(no_such_connection_error(connection_id));
                }
                ConnectionKillAuthorization::PermissionDenied => {
                    return Err(kill_denied_error(source, statement));
                }
            };
            let connection_control = connection_control.ok_or_else(|| {
                QueryServiceError::new(
                    QueryServiceErrorKind::Unavailable,
                    "client connection control is not composed",
                )
            })?;
            match connection_control.terminate(
                target,
                ClientConnectionTerminationReason::ExplicitKillConnection {
                    requester_connection_id: requester.connection_id(),
                },
            ) {
                ClientConnectionTerminateOutcome::Requested
                | ClientConnectionTerminateOutcome::AlreadyTerminating => Ok(StatementResult::Ok),
                ClientConnectionTerminateOutcome::Stale => {
                    Err(no_such_connection_error(connection_id))
                }
            }
        }
    }
}

fn kill_connection_id(statement: &ast::KillStatement) -> Result<u32, QueryServiceError> {
    let ast::LiteralKind::Number(connection_id) = &statement.connection_id.kind else {
        return Err(QueryServiceError::new(
            QueryServiceErrorKind::Parse,
            "KILL requires an integer connection id",
        ));
    };
    connection_id.parse::<u32>().map_err(|_| {
        QueryServiceError::new(
            QueryServiceErrorKind::Parse,
            "KILL requires an integer connection id",
        )
    })
}

fn no_such_connection_error(connection_id: u32) -> QueryServiceError {
    QueryServiceError::new(
        QueryServiceErrorKind::NoSuchSession,
        format!("unknown connection {connection_id}"),
    )
}

fn kill_denied_error(source: &str, statement: &ast::KillStatement) -> QueryServiceError {
    QueryServiceError::from_user_error(
        crate::session_error::SessionAdmitError::KillDenied.to_user_error(
            source,
            statement.span,
            "permission denied to kill connection owned by another principal",
        ),
    )
}

fn resolve_catalog_name(
    resolver: &SessionCatalogResolver,
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
    resolver
        .require_external_catalog_ready(&normalized)
        .map_err(|error| {
            let kind = match error.kind() {
                crate::catalog_application::CatalogApplicationErrorKind::Unavailable => {
                    QueryServiceErrorKind::Unavailable
                }
                _ => QueryServiceErrorKind::BadDatabase,
            };
            QueryServiceError::new(kind, error.to_string())
        })?;
    Ok(Some(normalized))
}

fn resolve_database_context(
    resolver: &SessionCatalogResolver,
    current_catalog: Option<&str>,
    schema: &str,
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
                Some(catalog)
                    if resolver
                        .iceberg_namespace_exists(catalog, &database)
                        .map_err(|error| internal_error(error.to_string()))? =>
                {
                    Ok(DatabaseContext {
                        catalog: Some(catalog.to_string()),
                        database,
                    })
                }
                Some(_) => Err(QueryServiceError::new(
                    QueryServiceErrorKind::BadDatabase,
                    format!("unknown database `{schema}`"),
                )),
                None if resolver
                    .database_exists(&database)
                    .map_err(|error| internal_error(error.to_string()))? =>
                {
                    Ok(DatabaseContext {
                        catalog: None,
                        database,
                    })
                }
                None => Err(QueryServiceError::new(
                    QueryServiceErrorKind::BadDatabase,
                    format!("unknown database `{schema}`"),
                )),
            }
        }
        [catalog, database] => {
            let catalog = resolve_catalog_name(resolver, catalog)?;
            let database = normalize_identifier(database)
                .map_err(|error| internal_error(error.to_string()))?;
            match catalog {
                Some(catalog)
                    if resolver
                        .iceberg_namespace_exists(&catalog, &database)
                        .map_err(|error| internal_error(error.to_string()))? =>
                {
                    Ok(DatabaseContext {
                        catalog: Some(catalog),
                        database,
                    })
                }
                None if resolver
                    .database_exists(&database)
                    .map_err(|error| internal_error(error.to_string()))? =>
                {
                    Ok(DatabaseContext {
                        catalog: None,
                        database,
                    })
                }
                _ => Err(QueryServiceError::new(
                    QueryServiceErrorKind::BadDatabase,
                    format!("unknown database `{schema}`"),
                )),
            }
        }
        _ => Err(QueryServiceError::new(
            QueryServiceErrorKind::BadDatabase,
            format!("unknown database `{schema}`; expected `<database>` or `<catalog>.<database>`"),
        )),
    }
}

#[derive(Clone)]
struct SqlBatchCursor<'a> {
    sql: &'a str,
    offset: usize,
}

impl<'a> SqlBatchCursor<'a> {
    fn new(sql: &'a str) -> Self {
        Self { sql, offset: 0 }
    }

    fn has_remaining(&self) -> bool {
        self.offset < self.sql.len()
    }

    fn has_nonempty_remaining(&self) -> Result<bool, QueryServiceError> {
        let mut probe = self.clone();
        while let Some(fragment) = probe.next_fragment()? {
            let fragment = strip_leading_line_comments(fragment.trim());
            if fragment.is_empty() {
                continue;
            }
            let statements = novarocks_parser::parse(fragment).map_err(|error| {
                QueryServiceError::from_user_error(error.to_user_error(fragment))
            })?;
            if !statements.is_empty() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Returns one raw semicolon-delimited fragment. The cursor never scans a
    /// later fragment, so draining between statements does not parse or admit
    /// work that has not crossed the lifecycle gate yet.
    fn next_fragment(&mut self) -> Result<Option<&'a str>, QueryServiceError> {
        if !self.has_remaining() {
            return Ok(None);
        }
        #[derive(Clone, Copy)]
        enum State {
            Normal,
            SingleQuote,
            DoubleQuote,
            Backtick,
            LineComment,
            BlockComment,
        }

        let start = self.offset;
        let bytes = self.sql.as_bytes();
        let mut index = start;
        let mut state = State::Normal;
        while index < bytes.len() {
            match state {
                State::Normal => match bytes[index] {
                    b'\'' => state = State::SingleQuote,
                    b'"' => state = State::DoubleQuote,
                    b'`' => state = State::Backtick,
                    b'-' if bytes.get(index + 1) == Some(&b'-') => {
                        state = State::LineComment;
                        index += 1;
                    }
                    b'#' => state = State::LineComment,
                    b'/' if bytes.get(index + 1) == Some(&b'*') => {
                        state = State::BlockComment;
                        index += 1;
                    }
                    b';' => {
                        self.offset = index + 1;
                        return Ok(Some(&self.sql[start..index]));
                    }
                    _ => {}
                },
                State::SingleQuote if bytes[index] == b'\'' => state = State::Normal,
                State::DoubleQuote if bytes[index] == b'"' => state = State::Normal,
                State::Backtick if bytes[index] == b'`' => state = State::Normal,
                State::LineComment if bytes[index] == b'\n' => state = State::Normal,
                State::BlockComment
                    if bytes[index] == b'*' && bytes.get(index + 1) == Some(&b'/') =>
                {
                    state = State::Normal;
                    index += 1;
                }
                _ => {}
            }
            index += 1;
        }
        if matches!(
            state,
            State::SingleQuote | State::DoubleQuote | State::Backtick
        ) {
            self.offset = self.sql.len();
            return Err(QueryServiceError::new(
                QueryServiceErrorKind::Parse,
                "unterminated quoted string in SQL batch",
            ));
        }
        self.offset = self.sql.len();
        Ok(Some(&self.sql[start..]))
    }
}

fn split_sql_statements(sql: &str) -> Result<Vec<String>, QueryServiceError> {
    let mut cursor = SqlBatchCursor::new(sql);
    let mut statements = Vec::new();
    while let Some(fragment) = cursor.next_fragment()? {
        let statement = fragment.trim();
        if !statement.is_empty() {
            statements.push(statement.to_string());
        }
    }
    Ok(statements)
}

/// Removes leading whole-line comments while retaining the first SQL token.
/// Script fragments include the repository license header before the SQL
/// statement, so treating the whole fragment as a comment loses the work.
fn strip_leading_line_comments(sql: &str) -> &str {
    let mut remaining = sql.trim();
    loop {
        let Some(newline) = remaining.find('\n') else {
            return if remaining.starts_with("--") || remaining.starts_with('#') {
                ""
            } else {
                remaining
            };
        };
        let line = remaining[..newline].trim();
        if line.is_empty() || line.starts_with("--") || line.starts_with('#') {
            remaining = remaining[newline + 1..].trim_start();
            continue;
        }
        return remaining;
    }
}

fn parse_bool(value: &str) -> Result<bool, QueryServiceError> {
    match value.to_ascii_lowercase().as_str() {
        "1" | "on" | "true" => Ok(true),
        "0" | "off" | "false" => Ok(false),
        _ => Err(QueryServiceError::new(
            QueryServiceErrorKind::InvalidValue,
            format!("invalid boolean value `{value}`"),
        )),
    }
}

fn apply_optimizer_session_set(
    settings: &mut SessionOptimizerSettings,
    name: &str,
    value: &str,
) -> Result<(), QueryServiceError> {
    let parse_bool_value = || parse_bool(value);
    let parse_u64_value = || {
        value.parse::<u64>().map_err(|_| {
            QueryServiceError::new(
                QueryServiceErrorKind::InvalidValue,
                format!("invalid {name}"),
            )
        })
    };
    let parse_f64_value = || {
        value.parse::<f64>().map_err(|_| {
            QueryServiceError::new(
                QueryServiceErrorKind::InvalidValue,
                format!("invalid {name}"),
            )
        })
    };
    let parse_usize_value = || {
        value.parse::<usize>().map_err(|_| {
            QueryServiceError::new(
                QueryServiceErrorKind::InvalidValue,
                format!("invalid {name}"),
            )
        })
    };

    match name {
        "cbo_broadcast_backend_count" => {
            settings.set_broadcast_backend_count(parse_f64_value()?);
        }
        "cbo_broadcast_node_mem_budget_bytes" => {
            settings.cbo_broadcast_node_mem_budget_bytes = Some(parse_f64_value()?);
        }
        "global_runtime_filter_build_max_size" => {
            settings.rf_build_max_bytes = Some(parse_u64_value()?);
        }
        "global_runtime_filter_build_min_size" => {
            settings.rf_build_min_bytes = Some(parse_u64_value()?);
        }
        "global_runtime_filter_probe_min_size" => {
            settings.rf_probe_min_bytes = Some(parse_u64_value()?);
        }
        "global_runtime_filter_probe_min_selectivity" => {
            settings.rf_probe_min_selectivity = Some(parse_f64_value()?);
        }
        "cbo_max_reorder_node_use_exhaustive" => {
            settings.max_reorder_node_use_exhaustive = Some(parse_usize_value()?);
        }
        "cbo_max_reorder_node_use_dp" => {
            settings.max_reorder_node_use_dp = Some(parse_usize_value()?);
        }
        "cbo_max_reorder_node_use_greedy" => {
            settings.max_reorder_node_use_greedy = Some(parse_usize_value()?);
        }
        "cbo_max_reorder_node" => {
            settings.max_reorder_node = Some(parse_usize_value()?);
        }
        "enable_query_rewrite_table_prune" => {
            settings.enable_query_rewrite_table_prune = parse_bool_value()?;
        }
        "enable_cbo_table_prune" => {
            settings.enable_cbo_table_prune = parse_bool_value()?;
        }
        "enable_table_prune_on_update" => {
            settings.enable_table_prune_on_update = parse_bool_value()?;
        }
        "enable_common_subexpr_reuse" => {
            settings.enable_common_subexpr_reuse = Some(parse_bool_value()?);
        }
        "enable_global_runtime_filter" => {
            settings.enable_global_runtime_filter = Some(parse_bool_value()?);
        }
        "enable_materialized_view_rewrite" => {
            settings.enable_materialized_view_rewrite = Some(parse_bool_value()?);
        }
        "enable_connector_static_predicate_pushdown" => {
            settings.enable_connector_static_predicate_pushdown = Some(parse_bool_value()?);
        }
        "cbo_enable_dp_join_reorder" => {
            settings.enable_dp_join_reorder = Some(parse_bool_value()?);
        }
        "cbo_enable_greedy_join_reorder" => {
            settings.enable_greedy_join_reorder = Some(parse_bool_value()?);
        }
        "enable_global_runtime_filter_cross_exchange" => {
            settings.allow_cross_exchange_rf = Some(parse_bool_value()?);
        }
        _ => {}
    }
    Ok(())
}

fn admin_raise_engine_error(sql: &str) -> Result<Option<QueryServiceError>, QueryServiceError> {
    let parts = sql.split_whitespace().collect::<Vec<_>>();
    if !matches!(parts.as_slice(), [admin, raise, engine, error, _]
        if admin.eq_ignore_ascii_case("admin")
            && raise.eq_ignore_ascii_case("raise")
            && engine.eq_ignore_ascii_case("engine")
            && error.eq_ignore_ascii_case("error"))
    {
        return Ok(None);
    }
    let [_, _, _, _, raw_code] = parts.as_slice() else {
        return Err(QueryServiceError::new(
            QueryServiceErrorKind::Parse,
            "expected ADMIN RAISE ENGINE ERROR '<engine_error_code>'",
        ));
    };
    let raw_code = raw_code
        .strip_prefix('\'')
        .and_then(|inner| inner.strip_suffix('\''))
        .or_else(|| {
            raw_code
                .strip_prefix('"')
                .and_then(|inner| inner.strip_suffix('"'))
        })
        .ok_or_else(|| {
            QueryServiceError::new(
                QueryServiceErrorKind::Parse,
                "expected ADMIN RAISE ENGINE ERROR '<engine_error_code>'",
            )
        })?;
    let code = EngineErrorCode::parse(raw_code).ok_or_else(|| {
        QueryServiceError::new(
            QueryServiceErrorKind::Parse,
            format!("unknown engine error code: {raw_code}"),
        )
    })?;
    let error = match code {
        EngineErrorCode::UnsupportedDistributedDmlShape => {
            EngineError::unsupported_distributed_dml_shape(
                "ADMIN RAISE ENGINE ERROR",
                "forced P8 SQL runner error-code smoke",
            )
        }
        EngineErrorCode::IcebergWriteDescriptorMismatch => {
            EngineError::iceberg_write_descriptor_mismatch("forced P8 SQL runner error-code smoke")
        }
        EngineErrorCode::UnsupportedPositionDeleteDescriptor => {
            EngineError::unsupported_position_delete_descriptor(
                "forced position-delete descriptor error-code smoke",
            )
        }
        EngineErrorCode::CommitKnownUncommitted => {
            EngineError::commit_known_uncommitted("forced P8 SQL runner error-code smoke")
        }
        EngineErrorCode::CommitUnknown => {
            EngineError::commit_unknown("forced P8 SQL runner error-code smoke")
        }
        EngineErrorCode::CommitKnownCommittedFinalizeFailed => {
            EngineError::commit_known_committed_finalize_failed(
                "forced P8 SQL runner error-code smoke",
            )
        }
        EngineErrorCode::ProtocolDecodeError => {
            EngineError::protocol_decode("forced P8 SQL runner error-code smoke")
        }
        _ => {
            return Err(QueryServiceError::new(
                QueryServiceErrorKind::Parse,
                format!("unsupported engine error code for ADMIN RAISE ENGINE ERROR: {raw_code}"),
            ));
        }
    };
    Ok(Some(QueryServiceError::new(
        QueryServiceErrorKind::Unsupported,
        error.to_bracketed_user_message(),
    )))
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

fn cancellation_error(reason: QueryCancellationReason) -> QueryServiceError {
    let (kind, message) = match reason {
        QueryCancellationReason::ExecutionCancellationRequested => (
            QueryServiceErrorKind::Interrupted,
            "Query execution cancellation was requested".to_string(),
        ),
        QueryCancellationReason::ExecutionOwnerDropped => (
            QueryServiceErrorKind::Interrupted,
            "Query execution protocol owner was dropped".to_string(),
        ),
        QueryCancellationReason::DeadlineExceeded { timeout_ms } => (
            QueryServiceErrorKind::Timeout,
            format!("query timed out after {timeout_ms} ms"),
        ),
        QueryCancellationReason::FrontendDrainDeadlineExceeded { timeout_ms } => (
            QueryServiceErrorKind::Interrupted,
            format!(
                "FRONTEND_DRAIN_DEADLINE_EXCEEDED: frontend drain deadline exceeded after {timeout_ms} ms"
            ),
        ),
        QueryCancellationReason::ExplicitKill { .. } => (
            QueryServiceErrorKind::Interrupted,
            "Query execution was interrupted".to_string(),
        ),
        QueryCancellationReason::ExplicitKillConnection { .. } => (
            QueryServiceErrorKind::Interrupted,
            "Query execution was interrupted because the connection was killed".to_string(),
        ),
        QueryCancellationReason::ClientDisconnected => (
            QueryServiceErrorKind::Interrupted,
            "Query execution was interrupted because the client disconnected".to_string(),
        ),
        QueryCancellationReason::ServerShutdown => (
            QueryServiceErrorKind::Interrupted,
            "Query execution was interrupted because the server is shutting down".to_string(),
        ),
    };
    QueryServiceError::new(kind, message)
}

fn governed_execution_error(
    error: QueryExecutionError,
    completion: GovernedStatementFinishOutcome,
) -> QueryServiceError {
    match completion {
        GovernedStatementFinishOutcome::Cancelled(reason) => governed_cancellation_error(reason),
        GovernedStatementFinishOutcome::Completed | GovernedStatementFinishOutcome::Stale => {
            governed_query_execution_error(error)
        }
        GovernedStatementFinishOutcome::ProtocolFailed => internal_error(format!(
            "query protocol ownership failed while handling execution error: {error}"
        )),
    }
}

fn governed_query_execution_error(error: QueryExecutionError) -> QueryServiceError {
    let kind = match error.kind() {
        QueryExecutionErrorKind::Cancelled => QueryServiceErrorKind::Interrupted,
        QueryExecutionErrorKind::DeadlineExceeded => QueryServiceErrorKind::Timeout,
        QueryExecutionErrorKind::Rejected => QueryServiceErrorKind::Unavailable,
        QueryExecutionErrorKind::InvalidRequest | QueryExecutionErrorKind::Failed => {
            QueryServiceErrorKind::Internal
        }
        _ => QueryServiceErrorKind::Internal,
    };
    QueryServiceError::new(kind, error.to_string())
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
    let column = QueryResultColumn {
        name: field.name().to_string(),
        data_type: field.data_type().clone(),
        nullable: field.nullable(),
        logical_type: field.logical_type().cloned(),
    };
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
                    let chunk = match record_batch_to_chunk(delivery.batch().clone()) {
                        Ok(chunk) => chunk,
                        Err(message) => {
                            delivery.fail(QueryExecutionError::new(
                                QueryExecutionErrorKind::InvalidRequest,
                                message.clone(),
                            ));
                            let _ = execution.request_cancel();
                            return Err(scalar_query_error(message));
                        }
                    };
                    let result = QueryResult {
                        columns: vec![column.clone()],
                        chunks: vec![chunk],
                    };
                    value = match crate::user_variable::query_result_to_user_variable_literal(
                        &result,
                    ) {
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

fn governed_query_deadline(
    state: &FrontendSessionState,
) -> Result<(Option<Instant>, Option<u64>), QueryServiceError> {
    let timeout_secs = state.execution_settings.query_timeout_secs();
    let deadline = match timeout_secs {
        Some(seconds) => Some(
            Instant::now()
                .checked_add(Duration::from_secs(seconds))
                .ok_or_else(|| internal_error("query deadline exceeds monotonic clock range"))?,
        ),
        None => None,
    };
    Ok((
        deadline,
        timeout_secs.map(|seconds| seconds.saturating_mul(1_000)),
    ))
}

fn governed_statement_begin_error(
    error: crate::query_execution::control::GovernedQueryStatementBeginError,
) -> QueryServiceError {
    QueryServiceError::new(
        QueryServiceErrorKind::Unavailable,
        format!("begin governed query statement failed: {error}"),
    )
}

fn scalar_query_error(message: impl Into<String>) -> QueryServiceError {
    QueryServiceError::new(QueryServiceErrorKind::InvalidValue, message.into())
}

fn governed_cancellation_error(reason: WorkCancellationReason) -> QueryServiceError {
    let (kind, message) = match reason {
        WorkCancellationReason::DeadlineExceeded => (
            QueryServiceErrorKind::Timeout,
            "query deadline exceeded".to_string(),
        ),
        WorkCancellationReason::FrontendDrainDeadlineExceeded => (
            QueryServiceErrorKind::Interrupted,
            "FRONTEND_DRAIN_DEADLINE_EXCEEDED: frontend drain deadline exceeded".to_string(),
        ),
        WorkCancellationReason::ExplicitKill { .. } => (
            QueryServiceErrorKind::Interrupted,
            "Query execution was interrupted".to_string(),
        ),
        WorkCancellationReason::ExplicitKillConnection { .. } => (
            QueryServiceErrorKind::Interrupted,
            "Query execution was interrupted because the connection was killed".to_string(),
        ),
        WorkCancellationReason::ClientDisconnected => (
            QueryServiceErrorKind::Interrupted,
            "Query execution was interrupted because the client disconnected".to_string(),
        ),
        WorkCancellationReason::ServerShutdown => (
            QueryServiceErrorKind::Interrupted,
            "Query execution was interrupted because the server is shutting down".to_string(),
        ),
        WorkCancellationReason::Requested => (
            QueryServiceErrorKind::Interrupted,
            "Query execution cancellation was requested".to_string(),
        ),
        WorkCancellationReason::OwnerDropped => (
            QueryServiceErrorKind::Interrupted,
            "Query execution protocol owner was dropped".to_string(),
        ),
    };
    QueryServiceError::new(kind, message)
}

/// Whether cancellation must release the current statement before the
/// protocol may answer the client.
///
/// Only KILL QUERY preserves the same connection for another command. The
/// disconnect and shutdown forms deliberately retain their bounded-return
/// behavior because no successor statement may be admitted on that session.
fn cancellation_requires_statement_fence(reason: &QueryCancellationReason) -> bool {
    matches!(reason, QueryCancellationReason::ExplicitKill { .. })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::common::admitted_query_context::QueryExecutionContext;
    use crate::common::backend_topology::BackendTopologySnapshot;
    use crate::common::query_cancellation::QueryCancellationSource;
    use crate::query_execution::dml::delete::{
        DeleteEngine, DeleteOperation, DeletePrepared, DeleteWriteReport, PrepareDeleteRequest,
        PreparedDelete,
    };
    use crate::query_execution::dml::insert::{
        IcebergPreparedInsert, IcebergWriteReport, PrepareIcebergInsert, PreparedIcebergInsert,
        ResolveInsertTarget, ResolvedInsertTarget,
    };
    use crate::query_execution::dml::mutation::{
        MutationEngine, MutationPrepared, MutationStageOutcome, PrepareMutationRequest,
        PreparedMutation,
    };
    use arrow::array::Int64Array;
    use novarocks_query_application::api::ResultField;
    use novarocks_query_application::test_support::{
        ResultStreamTestProducer, TestResultDeliveryDisposition,
    };
    use novarocks_types::schema::ColumnDef;
    use novarocks_types::{AttemptId, QueryExecutionId, QueryId};
    use novarocks_workload_control::ResourceConfig;

    fn default_query_options() -> QueryOptions {
        QueryOptions::parse(novarocks_proto_models::novarocks::QueryOptions::default())
            .expect("default wire query options are valid")
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

    impl crate::common::backend_topology::BackendTopologyPort for ChangingTopology {
        fn snapshot(
            &self,
        ) -> Result<BackendTopologySnapshot, crate::common::backend_topology::BackendTopologyError>
        {
            Ok(self.snapshot.lock().expect("test topology lock").clone())
        }

        fn subscribe_changes(&self) -> tokio::sync::watch::Receiver<u64> {
            self.changes.subscribe()
        }

        fn validate_snapshot(
            &self,
            expected: &BackendTopologySnapshot,
        ) -> Result<(), crate::common::backend_topology::BackendTopologyValidationError> {
            if &self.snapshot().map_err(
                crate::common::backend_topology::BackendTopologyValidationError::Unavailable,
            )? == expected
            {
                Ok(())
            } else {
                Err(crate::common::backend_topology::BackendTopologyValidationError::ContentChangedWithoutRevision {
                    revision: expected.revision(),
                })
            }
        }

        fn wait_for_eligible_after(
            &self,
            _revision: u64,
            _deadline: Instant,
        ) -> Result<BackendTopologySnapshot, crate::common::backend_topology::BackendTopologyError>
        {
            unreachable!("governed planning uses the event-driven subscription")
        }

        fn record_successful_stage(&self, _backend_idx: usize, _fragment_count: usize) {}

        fn show_backends(&self) -> Result<QueryResult, String> {
            unreachable!("test topology has no SHOW BACKENDS surface")
        }
    }

    fn eligible_topology(revision: u64) -> BackendTopologySnapshot {
        use novarocks_execution::task_execution::AdmissionEpochCapability;
        use novarocks_proto_codec::lifecycle::QueryControlEndpoint;
        use novarocks_proto_codec::membership::BackendProcessDescriptor;

        let descriptor = BackendProcessDescriptor::new(
            novarocks_types::BackendProcessId::new_v7(),
            QueryControlEndpoint::new("127.0.0.1", 9030).expect("test endpoint"),
            "test-deployment",
            "test-build",
            novarocks_types::NativeCompatibilityId::new([0x71; 32]),
        )
        .expect("test descriptor");
        BackendTopologySnapshot::try_new(
            revision,
            vec![crate::common::backend_topology::LiveBackendTarget::new(
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

    fn parsed_kill(source: &str) -> ast::KillStatement {
        let statements = novarocks_parser::parse(source).expect("KILL statement must parse");
        let [ParsedStatement::Session(ast::SessionStatement::Kill(statement))] =
            statements.as_slice()
        else {
            panic!("expected one KILL session statement");
        };
        statement.clone()
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

    fn register_kill_session(
        control: &QueryControlService,
        connection_id: u32,
        generation: u64,
        principal: &str,
    ) -> QuerySessionLease {
        control
            .register_session(SessionIdentity::new(
                crate::ClientConnectionToken::new(connection_id, generation)
                    .expect("valid test connection token"),
                principal,
            ))
            .expect("register test session")
    }

    struct FixedConnectionControl {
        outcome: ClientConnectionTerminateOutcome,
        calls: Mutex<
            Vec<(
                crate::ClientConnectionToken,
                ClientConnectionTerminationReason,
            )>,
        >,
    }

    impl FixedConnectionControl {
        fn new(outcome: ClientConnectionTerminateOutcome) -> Self {
            Self {
                outcome,
                calls: Mutex::new(Vec::new()),
            }
        }
    }

    impl ClientConnectionControlPort for FixedConnectionControl {
        fn terminate(
            &self,
            target: crate::ClientConnectionToken,
            reason: ClientConnectionTerminationReason,
        ) -> ClientConnectionTerminateOutcome {
            self.calls
                .lock()
                .expect("connection control calls lock")
                .push((target, reason));
            self.outcome
        }
    }

    #[test]
    fn kill_query_treats_an_idle_authorized_target_as_ok() {
        let control = crate::query_control::FrontendQueryControl::service();
        let requester = register_kill_session(&control, 8, 1, "alice");
        let _target = register_kill_session(&control, 7, 1, "alice");
        let source = "KILL QUERY 7";

        let result = execute_kill_statement(
            source,
            &parsed_kill(source),
            requester.token(),
            &control,
            None,
        );

        assert!(matches!(result, Ok(StatementResult::Ok)));
    }

    #[test]
    fn kill_connection_forms_accept_requested_and_already_terminating() {
        for outcome in [
            ClientConnectionTerminateOutcome::Requested,
            ClientConnectionTerminateOutcome::AlreadyTerminating,
        ] {
            for source in ["KILL 7", "KILL CONNECTION 7"] {
                let control = crate::query_control::FrontendQueryControl::service();
                let requester = register_kill_session(&control, 8, 1, "alice");
                let _target = register_kill_session(&control, 7, 11, "alice");
                let connection_control = FixedConnectionControl::new(outcome);

                let result = execute_kill_statement(
                    source,
                    &parsed_kill(source),
                    requester.token(),
                    &control,
                    Some(&connection_control),
                );

                assert!(
                    matches!(result, Ok(StatementResult::Ok)),
                    "{source}: {outcome:?}"
                );
                assert_eq!(
                    connection_control
                        .calls
                        .lock()
                        .expect("connection control calls lock")
                        .as_slice(),
                    &[(
                        crate::ClientConnectionToken::new(7, 11)
                            .expect("valid test connection token"),
                        ClientConnectionTerminationReason::ExplicitKillConnection {
                            requester_connection_id: 8,
                        },
                    )]
                );
            }
        }
    }

    #[test]
    fn kill_connection_stale_target_maps_to_no_such_session() {
        let control = crate::query_control::FrontendQueryControl::service();
        let requester = register_kill_session(&control, 8, 1, "alice");
        let _target = register_kill_session(&control, 7, 1, "alice");
        let connection_control =
            FixedConnectionControl::new(ClientConnectionTerminateOutcome::Stale);
        let source = "KILL CONNECTION 7";

        let error = execute_kill_statement(
            source,
            &parsed_kill(source),
            requester.token(),
            &control,
            Some(&connection_control),
        )
        .expect_err("stale protocol target must be rejected");

        assert_eq!(error.kind(), QueryServiceErrorKind::NoSuchSession);
    }

    #[test]
    fn kill_denial_is_a_typed_admit_error_for_query_and_connection() {
        for source in ["KILL QUERY 7", "KILL CONNECTION 7"] {
            let control = crate::query_control::FrontendQueryControl::service();
            let requester = register_kill_session(&control, 8, 1, "alice");
            let _target = register_kill_session(&control, 7, 1, "bob");
            let connection_control =
                FixedConnectionControl::new(ClientConnectionTerminateOutcome::Requested);

            let error = execute_kill_statement(
                source,
                &parsed_kill(source),
                requester.token(),
                &control,
                Some(&connection_control),
            )
            .expect_err("cross-principal KILL must be denied");
            let user_error = error.user_error().expect("typed KILL error");

            assert_eq!(user_error.code().as_str(), "sql.admit.kill_denied");
            assert_eq!(user_error.phase(), novarocks_user_error::ErrorPhase::Admit);
            assert_eq!(
                user_error.location().map(|location| location.column()),
                Some(1)
            );
            assert!(
                connection_control
                    .calls
                    .lock()
                    .expect("connection control calls lock")
                    .is_empty()
            );
        }
    }

    #[derive(Default)]
    struct RecordingCoreCommand {
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
    }

    struct RejectingMutationEngine;

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

        fn finalize_delete(&self, _prepared: &dyn DeletePrepared) -> Result<(), String> {
            unreachable!("no-op DELETE must not finalize")
        }
    }

    impl CoreCommandRoute for RecordingCoreCommand {}

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
                    Arc::new(crate::connector::control_host::tests::test_control_binding(
                        1,
                    )),
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
        _command: &dyn CoreCommandRoute,
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
                .execute_delete(
                    delete_engine,
                    crate::query_execution::dml::delete::DeleteStatement::Predicate(statement),
                    sql,
                    context,
                    Some(&query_options),
                )
                .map(|()| StatementResult::Ok)
                .map_err(|error| error.to_string()),
            [Statement::Dml(DmlStatement::Update(_) | DmlStatement::Merge(_))] => mutation_engine
                .ok_or_else(|| "mutation engine is unavailable".to_string())
                .and_then(|engine| {
                    dml.try_execute_typed_mutation(
                        engine,
                        match &parsed[0] {
                            Statement::Dml(statement) => statement,
                            _ => unreachable!(),
                        },
                        sql,
                        context,
                        Some(&query_options),
                    )
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
        let command = RecordingCoreCommand::default();
        let dml = DmlService::new(Arc::new(crate::statistics::FrontendStatisticsService::new()));
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
        let command = RecordingCoreCommand::default();
        let dml = DmlService::new(Arc::new(crate::statistics::FrontendStatisticsService::new()));
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
        let command = RecordingCoreCommand::default();
        let dml = DmlService::new(Arc::new(crate::statistics::FrontendStatisticsService::new()));
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
    fn frontend_router_orders_ctas_before_truncate_and_fallback() {
        let insert = RecordingInsertEngine::default();
        let delete = RecordingDeleteEngine::default();
        let command = RecordingCoreCommand::default();
        let dml = DmlService::new(Arc::new(crate::statistics::FrontendStatisticsService::new()));
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
        let command = RecordingCoreCommand::default();
        let dml = DmlService::new(Arc::new(crate::statistics::FrontendStatisticsService::new()));
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
        let command = RecordingCoreCommand::default();
        let dml = DmlService::new(Arc::new(crate::statistics::FrontendStatisticsService::new()));
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
    fn session_setting_value_accepts_parser_owned_boolean_words() {
        let statements = novarocks_parser::parse("SET enable_eliminate_agg = on")
            .expect("SET boolean value must parse");
        let [ParsedStatement::Session(ast::SessionStatement::Set(statement))] =
            statements.as_slice()
        else {
            panic!("expected one SET statement");
        };
        assert_eq!(
            session_setting_value(&statement.assignments[0].value).expect("boolean value"),
            "on"
        );
    }

    #[test]
    fn autocommit_lowering_is_closed_over_truthful_boolean_spellings() {
        for (source, expected) in [
            ("SET autocommit = 1", AutocommitSetting::Enabled),
            ("SET autocommit = ON", AutocommitSetting::Enabled),
            ("SET autocommit = TRUE", AutocommitSetting::Enabled),
            ("SET autocommit = 0", AutocommitSetting::Disabled),
            ("SET autocommit = OFF", AutocommitSetting::Disabled),
            ("SET autocommit = FALSE", AutocommitSetting::Disabled),
        ] {
            let statements = novarocks_parser::parse(source).expect("SET must parse");
            let [ParsedStatement::Session(ast::SessionStatement::Set(statement))] =
                statements.as_slice()
            else {
                panic!("expected one SET statement");
            };
            assert_eq!(
                lower_autocommit_setting(&statement.assignments[0].value)
                    .expect("autocommit spelling must lower"),
                expected,
                "source={source}"
            );
        }
    }

    #[test]
    fn autocommit_lowering_rejects_values_outside_the_closed_boolean_surface() {
        for source in ["SET autocommit = 2", "SET autocommit = inherited"] {
            let statements = novarocks_parser::parse(source).expect("SET must parse");
            let [ParsedStatement::Session(ast::SessionStatement::Set(statement))] =
                statements.as_slice()
            else {
                panic!("expected one SET statement");
            };
            let error = lower_autocommit_setting(&statement.assignments[0].value)
                .expect_err("unrecognized autocommit value must be rejected");
            assert_eq!(error.kind(), QueryServiceErrorKind::InvalidValue);
            assert!(error.message().contains("invalid autocommit value"));
        }
    }

    #[test]
    fn autocommit_is_not_an_unknown_global_no_op() {
        assert!(is_known_session_setting("autocommit"));
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
    fn streaming_tail_detection_ignores_comments_and_rejects_a_real_statement() {
        let mut comments = SqlBatchCursor::new("SELECT 1; /* trailing comment */; -- done");
        assert_eq!(comments.next_fragment().unwrap(), Some("SELECT 1"));
        assert!(!comments.has_nonempty_remaining().unwrap());

        let mut statement = SqlBatchCursor::new("SELECT 1; /* comment */; SET query_timeout = 1");
        assert_eq!(statement.next_fragment().unwrap(), Some("SELECT 1"));
        assert!(statement.has_nonempty_remaining().unwrap());
    }

    #[test]
    fn streaming_batch_tail_check_ignores_empty_fragments_and_rejects_work() {
        let mut final_query = SqlBatchCursor::new("SELECT 1; ; -- trailing comment\n");
        assert_eq!(
            final_query.next_fragment().expect("first fragment"),
            Some("SELECT 1")
        );
        assert!(
            !final_query
                .has_nonempty_remaining()
                .expect("comment-only tail")
        );

        let mut followed = SqlBatchCursor::new("SELECT 1; SET query_timeout = 1");
        assert_eq!(
            followed.next_fragment().expect("first fragment"),
            Some("SELECT 1")
        );
        assert!(
            followed
                .has_nonempty_remaining()
                .expect("valid trailing statement")
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
    fn optimizer_session_settings_preserve_frontend_admission_contract() {
        let mut settings = SessionOptimizerSettings::default();
        apply_optimizer_session_set(&mut settings, "cbo_broadcast_node_mem_budget_bytes", "0")
            .expect("broadcast budget setting");
        apply_optimizer_session_set(
            &mut settings,
            "global_runtime_filter_probe_min_selectivity",
            "0.0",
        )
        .expect("runtime filter selectivity setting");
        apply_optimizer_session_set(&mut settings, "enable_common_subexpr_reuse", "false")
            .expect("cse setting");
        apply_optimizer_session_set(
            &mut settings,
            "enable_connector_static_predicate_pushdown",
            "false",
        )
        .expect("connector static predicate setting");
        apply_optimizer_session_set(&mut settings, "cbo_max_reorder_node_use_exhaustive", "2")
            .expect("join reorder setting");

        assert_eq!(settings.cbo_broadcast_node_mem_budget_bytes, Some(0.0));
        assert_eq!(settings.rf_probe_min_selectivity, Some(0.0));
        assert_eq!(settings.enable_common_subexpr_reuse, Some(false));
        assert_eq!(
            settings.enable_connector_static_predicate_pushdown,
            Some(false)
        );
        assert_eq!(settings.max_reorder_node_use_exhaustive, Some(2));
    }

    #[test]
    fn admin_raise_engine_error_keeps_the_engine_code_visible() {
        let error =
            admin_raise_engine_error("ADMIN RAISE ENGINE ERROR 'UnsupportedDistributedDmlShape'")
                .expect("parse command")
                .expect("recognized command");
        assert_eq!(error.kind(), QueryServiceErrorKind::Unsupported);
        assert!(
            error
                .to_string()
                .contains("[UnsupportedDistributedDmlShape]")
        );
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
