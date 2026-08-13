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

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use novarocks::common::app_config::ClusterRole;
use novarocks::common::engine_error::{EngineError, EngineErrorCode};
use novarocks::engine::add_files_engine::AddFilesEngine;
use novarocks::engine::ctas_engine::CtasEngine;
use novarocks::engine::delete_engine::DeleteEngine;
use novarocks::engine::insert_engine::InsertEngine;
use novarocks::engine::mutation_engine::MutationEngine;
use novarocks::engine::truncate_engine::TruncateEngine;
use novarocks::engine::{
    PreparedQueryOperation, SessionCatalogResolver, StandaloneCommandExecutor,
    StandaloneQueryCompiler, StatementResult, backend_command::BackendCommandExecutor,
    catalog_command::CatalogCommandExecutor, statistics_command::StatisticsCommandExecutor,
    view_command::ViewCommandExecutor,
};
use novarocks::query_execution::backend::BackendTopologyService;
use novarocks::query_execution::cancellation::QueryCancellationReason;
use novarocks::query_execution::control::{
    QueryCancelOutcome, QueryControlService, QuerySessionLease, SessionIdentity, SessionToken,
    StatementFinishOutcome,
};
use novarocks::query_execution::request_context::{
    RequestAdmission, RequestContext, SessionOptimizerSettings,
};
use novarocks::query_execution::service::QueryExecutionService;
use novarocks::query_execution::session::{
    QueryServiceError, QueryServiceErrorKind, QuerySession, QuerySessionFactory,
    QuerySessionOpenRequest, SessionExecutionSettings,
};
use novarocks::runtime::query_result::{QueryResult, QueryResultColumn, record_batch_to_chunk};
use novarocks_catalog::identifier::normalize_identifier;
use novarocks_catalog::memory::DEFAULT_DATABASE;
use novarocks_execution::runtime::query_options::QueryOptions;
use tokio::task;

use crate::dml::DmlService;

const DEFAULT_CATALOG: &str = "default_catalog";

pub trait CoreCommandRoute: Send + Sync {
    fn execute(
        &self,
        sql: &str,
        context: &RequestContext,
        query_options: QueryOptions,
    ) -> Result<StatementResult, String>;
}

#[derive(Clone)]
struct TypedThenLegacyCommand {
    catalog: CatalogCommandExecutor,
    statistics: StatisticsCommandExecutor,
    backend: BackendCommandExecutor,
    view: ViewCommandExecutor,
    legacy: StandaloneCommandExecutor,
}

impl TypedThenLegacyCommand {
    fn new(
        catalog: CatalogCommandExecutor,
        statistics: StatisticsCommandExecutor,
        backend: BackendCommandExecutor,
        view: ViewCommandExecutor,
        legacy: StandaloneCommandExecutor,
    ) -> Self {
        Self {
            catalog,
            statistics,
            backend,
            view,
            legacy,
        }
    }
}

impl CoreCommandRoute for TypedThenLegacyCommand {
    fn execute(
        &self,
        sql: &str,
        context: &RequestContext,
        query_options: QueryOptions,
    ) -> Result<StatementResult, String> {
        let connector_context = novarocks::connector::connector_request_context_for_query(
            Some(&query_options),
            context.execution().cancellation().clone(),
        )?;
        match self.catalog.try_execute(
            sql,
            context.session().current_catalog(),
            context.session().current_database(),
            &connector_context,
        )? {
            Some(result) => Ok(result),
            None => match self.statistics.try_execute(
                sql,
                context.session().current_catalog(),
                context.session().current_database(),
            )? {
                Some(result) => Ok(result),
                None => match self.backend.try_execute(sql, context.execution().role())? {
                    Some(result) => Ok(result),
                    None => match self.view.try_execute(
                        sql,
                        context.session().current_catalog(),
                        context.session().current_database(),
                        &connector_context,
                    )? {
                        Some(result) => Ok(result),
                        None => self.legacy.execute(sql, context, Some(query_options)),
                    },
                },
            },
        }
    }
}

fn execute_frontend_command<C, T, A>(
    dml: &DmlService,
    insert_engine: &dyn InsertEngine,
    delete_engine: &dyn DeleteEngine,
    mutation_engine: Option<&dyn MutationEngine>,
    ctas_route: C,
    truncate_route: T,
    add_files_route: A,
    command: &dyn CoreCommandRoute,
    sql: &str,
    context: &RequestContext,
    query_options: QueryOptions,
) -> Result<StatementResult, String>
where
    C: FnOnce(&str, &RequestContext, &QueryOptions) -> Result<Option<()>, crate::dml::DmlError>,
    T: FnOnce(&str, &RequestContext, &QueryOptions) -> Result<Option<()>, crate::dml::DmlError>,
    A: FnOnce(&str, &RequestContext, &QueryOptions) -> Result<Option<u32>, crate::dml::DmlError>,
{
    match dml.try_execute_insert(insert_engine, sql, context, Some(&query_options)) {
        Ok(Some(())) => Ok(StatementResult::Ok),
        Ok(None) => match dml.try_execute_delete(delete_engine, sql, context, Some(&query_options))
        {
            Ok(Some(())) => Ok(StatementResult::Ok),
            Ok(None) => match mutation_engine {
                Some(mutation_engine) => match dml.try_execute_update(
                    mutation_engine,
                    sql,
                    context,
                    Some(&query_options),
                ) {
                    Ok(Some(())) => Ok(StatementResult::Ok),
                    Ok(None) => match dml.try_execute_merge(
                        mutation_engine,
                        sql,
                        context,
                        Some(&query_options),
                    ) {
                        Ok(Some(())) => Ok(StatementResult::Ok),
                        Ok(None) => match ctas_route(sql, context, &query_options) {
                            Ok(Some(())) => Ok(StatementResult::Ok),
                            Ok(None) => match truncate_route(sql, context, &query_options) {
                                Ok(Some(())) => Ok(StatementResult::Ok),
                                Ok(None) => match add_files_route(sql, context, &query_options) {
                                    Ok(Some(file_count)) => {
                                        add_files_status(file_count).map(StatementResult::Query)
                                    }
                                    Ok(None) => command.execute(sql, context, query_options),
                                    Err(error) => Err(error.to_string()),
                                },
                                Err(error) => Err(error.to_string()),
                            },
                            Err(error) => Err(error.to_string()),
                        },
                        Err(error) => Err(error.to_string()),
                    },
                    Err(error) => Err(error.to_string()),
                },
                None => match ctas_route(sql, context, &query_options) {
                    Ok(Some(())) => Ok(StatementResult::Ok),
                    Ok(None) => match truncate_route(sql, context, &query_options) {
                        Ok(Some(())) => Ok(StatementResult::Ok),
                        Ok(None) => match add_files_route(sql, context, &query_options) {
                            Ok(Some(file_count)) => {
                                add_files_status(file_count).map(StatementResult::Query)
                            }
                            Ok(None) => command.execute(sql, context, query_options),
                            Err(error) => Err(error.to_string()),
                        },
                        Err(error) => Err(error.to_string()),
                    },
                    Err(error) => Err(error.to_string()),
                },
            },
            Err(error) => Err(error.to_string()),
        },
        Err(error) => Err(error.to_string()),
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

/// Design: ADR-0012 (docs/adr/ADR-0012-frontend-query-session-router.md)
#[derive(Clone)]
pub struct FrontendQueryService {
    session_catalog_resolver: SessionCatalogResolver,
    query_compiler: StandaloneQueryCompiler,
    command_executor: Arc<dyn CoreCommandRoute>,
    query_control: QueryControlService,
    query_execution: QueryExecutionService,
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
}

impl FrontendQueryService {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_with_recovery_bound(
        session_catalog_resolver: SessionCatalogResolver,
        query_compiler: StandaloneQueryCompiler,
        command_executor: StandaloneCommandExecutor,
        catalog_command_executor: CatalogCommandExecutor,
        statistics_command_executor: StatisticsCommandExecutor,
        backend_command_executor: BackendCommandExecutor,
        view_command_executor: ViewCommandExecutor,
        query_control: QueryControlService,
        query_execution: QueryExecutionService,
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
    ) -> Self {
        Self {
            session_catalog_resolver,
            query_compiler,
            command_executor: Arc::new(TypedThenLegacyCommand::new(
                catalog_command_executor,
                statistics_command_executor,
                backend_command_executor,
                view_command_executor,
                command_executor,
            )),
            query_control,
            query_execution,
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
        }
    }
}

impl QuerySessionFactory for FrontendQueryService {
    fn open_session(
        &self,
        request: QuerySessionOpenRequest,
    ) -> Result<Arc<dyn QuerySession>, QueryServiceError> {
        let lease = self
            .query_control
            .register_session(SessionIdentity::new(
                request.connection_id(),
                request.principal().to_string(),
            ))
            .map_err(|error| {
                QueryServiceError::new(
                    QueryServiceErrorKind::Internal,
                    format!("register frontend query session failed: {error:?}"),
                )
            })?;
        Ok(Arc::new(FrontendQuerySession {
            service: self.clone(),
            lease: Mutex::new(Some(lease)),
            state: Mutex::new(FrontendSessionState::default()),
        }))
    }

    fn cancel_all(&self, reason: QueryCancellationReason) {
        self.query_control.cancel_all(reason);
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
    ) -> Result<StatementResult, QueryServiceError> {
        let trimmed = strip_leading_line_comments(statement.trim().trim_end_matches(';').trim());
        if trimmed.is_empty() {
            return Ok(StatementResult::Ok);
        }
        if let Some(schema) = parse_use_database(trimmed) {
            self.init_database(schema).await?;
            return Ok(StatementResult::Ok);
        }
        if let Some(connection_id) = parse_kill_query(trimmed)? {
            let requester = self.token()?;
            return match self
                .service
                .query_control
                .kill_query(requester, connection_id)
            {
                QueryCancelOutcome::Requested | QueryCancelOutcome::AlreadyRequested(_) => {
                    Ok(StatementResult::Ok)
                }
                QueryCancelOutcome::NoActiveStatement => Err(QueryServiceError::new(
                    QueryServiceErrorKind::NoSuchSession,
                    format!("connection {connection_id} has no active query"),
                )),
                QueryCancelOutcome::UnknownSession => Err(QueryServiceError::new(
                    QueryServiceErrorKind::NoSuchSession,
                    format!("unknown connection {connection_id}"),
                )),
                QueryCancelOutcome::PermissionDenied => Err(QueryServiceError::new(
                    QueryServiceErrorKind::PermissionDenied,
                    "permission denied to kill query owned by another principal",
                )),
            };
        }
        if self.apply_session_set(trimmed).await? {
            return Ok(StatementResult::Ok);
        }
        if let Some(error) = admin_raise_engine_error(trimmed)? {
            return Err(error);
        }
        self.execute_admitted(trimmed.to_string()).await
    }

    async fn apply_session_set(&self, sql: &str) -> Result<bool, QueryServiceError> {
        let lower = sql.to_ascii_lowercase();
        if !lower.starts_with("set ") {
            return Ok(false);
        }
        let assignment = sql[4..].trim();
        if let Some(catalog) = assignment
            .strip_prefix("CATALOG ")
            .or_else(|| assignment.strip_prefix("catalog "))
        {
            let catalog =
                resolve_catalog_name(&self.service.session_catalog_resolver, catalog.trim())?;
            let mut state = self.state.lock().map_err(poisoned_state)?;
            state.current_catalog = catalog;
            return Ok(true);
        }
        let Some((raw_name, raw_value)) = assignment.split_once('=') else {
            return Ok(true);
        };
        if raw_name.trim().starts_with('@') && !raw_name.trim().starts_with("@@") {
            let value = if let Some(inner_query) = parenthesized_query(raw_value) {
                match self.execute_admitted(inner_query.to_string()).await? {
                    StatementResult::Query(result) => {
                        novarocks::runtime::user_variable::query_result_to_user_variable_literal(
                            &result,
                        )
                        .map_err(|message| {
                            QueryServiceError::new(QueryServiceErrorKind::Internal, message)
                        })?
                    }
                    StatementResult::Ok => {
                        return Err(QueryServiceError::new(
                            QueryServiceErrorKind::InvalidValue,
                            "user variable assignment query did not return a value",
                        ));
                    }
                }
            } else {
                raw_value.trim().to_string()
            };
            let mut state = self.state.lock().map_err(poisoned_state)?;
            state
                .user_variables
                .insert(raw_name.trim().to_ascii_lowercase(), value);
            return Ok(true);
        }
        let name = raw_name
            .trim()
            .trim_start_matches("@@")
            .to_ascii_lowercase();
        let value = raw_value.trim().trim_matches('\'').trim_matches('"');
        if name == "catalog" {
            let catalog = resolve_catalog_name(&self.service.session_catalog_resolver, value)?;
            let mut state = self.state.lock().map_err(poisoned_state)?;
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
            return Ok(true);
        }
        let mut state = self.state.lock().map_err(poisoned_state)?;
        match name.as_str() {
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
            _ => apply_optimizer_session_set(&mut state.optimizer_settings, &name, value)?,
        }
        Ok(true)
    }

    async fn execute_admitted(&self, sql: String) -> Result<StatementResult, QueryServiceError> {
        let state = self.state.lock().map_err(poisoned_state)?.clone();
        let assignments = state
            .user_variables
            .iter()
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect::<Vec<_>>();
        let sql = novarocks::sql::substitute_user_variables(&sql, &assignments)
            .map_err(classify_engine_error)?;
        let token = self.token()?;
        let mut active = self
            .service
            .query_control
            .begin_statement(token)
            .map_err(|error| {
                QueryServiceError::new(
                    QueryServiceErrorKind::Internal,
                    format!("begin statement failed: {error:?}"),
                )
            })?;
        let cancellation = active.cancellation().clone();
        let query_timeout_secs = state.execution_settings.query_timeout_secs();
        let deadline = match query_timeout_secs {
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
        let deadline = query_timeout_secs.map(|_| deadline);
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
        let mut query_options = state.execution_settings.query_options();
        query_options
            .set_allow_throw_exception(novarocks::sql::extract_allow_throw_exception_hint(&sql));
        let is_query = is_query_statement(&sql);
        let mut worker = task::spawn_blocking(move || {
            let result = if is_query {
                compiler
                    .prepare(&sql, &context, Some(query_options))
                    .and_then(|operation| execute_prepared_query(operation, &query_execution))
            } else {
                execute_frontend_command(
                    dml.as_ref(),
                    insert_engine.as_ref(),
                    delete_engine.as_ref(),
                    Some(mutation_engine.as_ref()),
                    |sql, context, query_options| {
                        dml.try_execute_ctas(
                            ctas_engine.as_ref(),
                            sql,
                            context,
                            Some(query_options),
                        )
                    },
                    |sql, context, query_options| {
                        dml.try_execute_truncate(
                            truncate_engine.as_ref(),
                            sql,
                            context,
                            Some(query_options),
                        )
                    },
                    |sql, context, query_options| {
                        dml.try_execute_add_files(
                            add_files_engine.as_ref(),
                            sql,
                            context,
                            Some(query_options),
                        )
                    },
                    command_executor.as_ref(),
                    &sql,
                    &context,
                    query_options,
                )
            };
            let completion = active.finish();
            (result, completion)
        });
        let result = if let Some(seconds) = query_timeout_secs {
            match tokio::time::timeout(Duration::from_secs(seconds), &mut worker).await {
                Ok(result) => result.map_err(|error| internal_error(error.to_string()))?,
                Err(_) => {
                    self.cancel_current(QueryCancellationReason::DeadlineExceeded {
                        timeout_ms: seconds.saturating_mul(1_000),
                    });
                    // A timeout is not complete until the worker releases the
                    // statement lease. Waiting here also fences Backend abort
                    // acknowledgement before this session admits its next SQL.
                    let _ = worker.await;
                    return Err(QueryServiceError::new(
                        QueryServiceErrorKind::Timeout,
                        format!("query timed out after {} ms", seconds.saturating_mul(1_000)),
                    ));
                }
            }
        } else {
            worker
                .await
                .map_err(|error| internal_error(error.to_string()))?
        };
        let (result, completion) = result;
        match completion {
            StatementFinishOutcome::Cancelled(reason) => Err(cancellation_error(reason)),
            StatementFinishOutcome::Stale if cancellation.is_cancelled() => Err(
                cancellation_error(cancellation.reason().expect("cancelled view has a reason")),
            ),
            StatementFinishOutcome::Completed | StatementFinishOutcome::Stale => {
                result.map_err(classify_engine_error)
            }
        }
    }
}

fn parenthesized_query(value: &str) -> Option<&str> {
    let trimmed = value.trim();
    let inner = trimmed.strip_prefix('(')?.strip_suffix(')')?.trim();
    let lower = inner.to_ascii_lowercase();
    (lower.starts_with("select ") || lower.starts_with("with ")).then_some(inner)
}

fn execute_prepared_query(
    operation: PreparedQueryOperation,
    query_execution: &QueryExecutionService,
) -> Result<StatementResult, String> {
    match operation {
        PreparedQueryOperation::Immediate(operation) => Ok(operation.into_result()),
        PreparedQueryOperation::Distributed(operation) => {
            let (request, completion) = operation.into_parts();
            let outcome = query_execution
                .execute(request)
                .map_err(|error| error.to_string())?;
            completion.complete(outcome)
        }
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
        let statements = split_sql_statements(sql)?;
        // Match the standalone MySQL session contract: a batch returns its
        // most recent result set even when subsequent DDL/session statements
        // complete successfully. In particular, all-in-one routes through
        // this frontend session before reaching the Stage/Start lifecycle.
        let mut last_query_result = None;
        for statement in statements {
            match self.execute_statement(&statement).await? {
                StatementResult::Query(result) => last_query_result = Some(result),
                StatementResult::Ok => {}
            }
        }
        Ok(last_query_result
            .map(StatementResult::Query)
            .unwrap_or(StatementResult::Ok))
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
    resolver: &SessionCatalogResolver,
    catalog: &str,
) -> Result<Option<String>, QueryServiceError> {
    let normalized = normalize_identifier(catalog).map_err(classify_engine_error)?;
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
                novarocks::catalog_application::CatalogApplicationErrorKind::Unavailable => {
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
            let database = normalize_identifier(database).map_err(classify_engine_error)?;
            match current_catalog {
                Some(catalog)
                    if resolver
                        .iceberg_namespace_exists(catalog, &database)
                        .map_err(classify_engine_error)? =>
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
                    .map_err(classify_engine_error)? =>
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
            let database = normalize_identifier(database).map_err(classify_engine_error)?;
            match catalog {
                Some(catalog)
                    if resolver
                        .iceberg_namespace_exists(&catalog, &database)
                        .map_err(classify_engine_error)? =>
                {
                    Ok(DatabaseContext {
                        catalog: Some(catalog),
                        database,
                    })
                }
                None if resolver
                    .database_exists(&database)
                    .map_err(classify_engine_error)? =>
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

fn split_sql_statements(sql: &str) -> Result<Vec<String>, QueryServiceError> {
    #[derive(Clone, Copy)]
    enum State {
        Normal,
        SingleQuote,
        DoubleQuote,
        Backtick,
        LineComment,
        BlockComment,
    }

    let mut statements = Vec::new();
    let mut start = 0;
    let bytes = sql.as_bytes();
    let mut index = 0;
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
                    let statement = sql[start..index].trim();
                    if !statement.is_empty() {
                        statements.push(statement.to_string());
                    }
                    start = index + 1;
                }
                _ => {}
            },
            State::SingleQuote if bytes[index] == b'\'' => state = State::Normal,
            State::DoubleQuote if bytes[index] == b'"' => state = State::Normal,
            State::Backtick if bytes[index] == b'`' => state = State::Normal,
            State::LineComment if bytes[index] == b'\n' => state = State::Normal,
            State::BlockComment if bytes[index] == b'*' && bytes.get(index + 1) == Some(&b'/') => {
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
        return Err(QueryServiceError::new(
            QueryServiceErrorKind::Parse,
            "unterminated quoted string in SQL batch",
        ));
    }
    let statement = sql[start..].trim();
    if !statement.is_empty() {
        statements.push(statement.to_string());
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

fn parse_use_database(sql: &str) -> Option<&str> {
    sql.strip_prefix("USE ")
        .or_else(|| sql.strip_prefix("use "))
        .map(str::trim)
        .filter(|schema| !schema.is_empty())
}

fn parse_kill_query(sql: &str) -> Result<Option<u32>, QueryServiceError> {
    let mut words = sql.split_whitespace();
    let Some(first) = words.next() else {
        return Ok(None);
    };
    if !first.eq_ignore_ascii_case("kill") {
        return Ok(None);
    }
    let second = words.next();
    let target = match second {
        Some(word) if word.eq_ignore_ascii_case("query") => words.next(),
        Some(word) => Some(word),
        None => None,
    };
    let target = target.ok_or_else(|| {
        QueryServiceError::new(
            QueryServiceErrorKind::Parse,
            "KILL requires a connection id",
        )
    })?;
    target.parse().map(Some).map_err(|_| {
        QueryServiceError::new(QueryServiceErrorKind::Parse, "invalid KILL connection id")
    })
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

fn is_query_statement(sql: &str) -> bool {
    let mut words = sql.split_whitespace();
    match words.next().map(|word| word.to_ascii_lowercase()) {
        Some(keyword) if matches!(keyword.as_str(), "select" | "with") => true,
        Some(keyword) if keyword == "explain" => {
            let mut target = words.next().map(|word| word.to_ascii_lowercase());
            while matches!(
                target.as_deref(),
                Some("analyze" | "verbose" | "costs" | "logical")
            ) {
                target = words.next().map(|word| word.to_ascii_lowercase());
            }
            matches!(target.as_deref(), Some("select" | "with"))
        }
        _ => false,
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

fn classify_engine_error(error: impl ToString) -> QueryServiceError {
    let message = error.to_string();
    let lower = message.to_ascii_lowercase();
    let kind = if lower.contains("unknown database") || lower.contains("unknown catalog") {
        QueryServiceErrorKind::BadDatabase
    } else if lower.contains("unsupported") {
        QueryServiceErrorKind::Unsupported
    } else if lower.contains("expected")
        || lower.contains("unterminated")
        || lower.contains("invalid")
    {
        QueryServiceErrorKind::Parse
    } else {
        QueryServiceErrorKind::Internal
    };
    QueryServiceError::new(kind, message)
}

fn cancellation_error(reason: QueryCancellationReason) -> QueryServiceError {
    let (kind, message) = match reason {
        QueryCancellationReason::DeadlineExceeded { timeout_ms } => (
            QueryServiceErrorKind::Timeout,
            format!("query timed out after {timeout_ms} ms"),
        ),
        QueryCancellationReason::ExplicitKill { .. } => (
            QueryServiceErrorKind::Interrupted,
            "Query execution was interrupted".to_string(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use novarocks::engine::delete_engine::{
        DeleteCommit, DeleteEngine, DeleteOperation, DeletePrepared, DeleteWriteReport,
        PrepareDeleteRequest, PreparedDelete,
    };
    use novarocks::engine::insert_engine::{
        IcebergInsertCommit, IcebergPreparedInsert, IcebergWriteReport, PrepareIcebergInsert,
        PreparedIcebergInsert, ResolveInsertTarget, ResolvedInsertTarget,
    };
    use novarocks::engine::mutation_engine::{
        MutationAbort, MutationCommit, MutationEngine, MutationPrepared, MutationStageOutcome,
        PrepareMutationRequest, PreparedMutation,
    };
    use novarocks::engine::statistics::{
        CollectedColumnStatistics, EmptyStatisticsService, StatisticsColumn, StatisticsEngine,
        StatisticsTableTarget,
    };
    use novarocks::query_execution::backend::BackendTopologySnapshot;
    use novarocks::query_execution::cancellation::QueryCancellationSource;
    use novarocks::query_execution::request_context::QueryExecutionContext;
    use novarocks_catalog::schema::ColumnDef;

    #[derive(Default)]
    struct RecordingCoreCommand {
        calls: AtomicUsize,
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
        ) -> Result<MutationStageOutcome, String> {
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
                    catalog: "ice".to_string(),
                    namespace: "db".to_string(),
                    table: "t".to_string(),
                    target_ref: "main".to_string(),
                    attempt_id: "test-delete".to_string(),
                    base_snapshot_id: None,
                },
                handle: Arc::new(TestDeletePrepared),
            })
        }

        fn run_delete(&self, _prepared: &dyn DeletePrepared) -> Result<DeleteWriteReport, String> {
            Ok(DeleteWriteReport::NoOp)
        }

        fn finalize_delete(&self, _prepared: &dyn DeletePrepared) -> Result<(), String> {
            unreachable!("no-op DELETE must not finalize")
        }
    }

    impl CoreCommandRoute for RecordingCoreCommand {
        fn execute(
            &self,
            _sql: &str,
            context: &RequestContext,
            _query_options: QueryOptions,
        ) -> Result<StatementResult, String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.contexts
                .lock()
                .expect("recorded command contexts")
                .push(context.execution().clone());
            Ok(StatementResult::Ok)
        }
    }

    #[derive(Default)]
    struct RecordingInsertEngine {
        resolve_contexts: Mutex<Vec<QueryExecutionContext>>,
    }

    impl StatisticsEngine for RecordingInsertEngine {
        fn resolve_table_columns(
            &self,
            _target: &StatisticsTableTarget,
        ) -> Result<Vec<StatisticsColumn>, String> {
            Ok(Vec::new())
        }

        fn resolve_local_table_columns(
            &self,
            _database: &str,
            _table: &str,
        ) -> Result<Option<Vec<StatisticsColumn>>, String> {
            Ok(None)
        }

        fn collect_table_statistics(
            &self,
            _target: &StatisticsTableTarget,
            _columns: &[String],
        ) -> Result<Vec<CollectedColumnStatistics>, String> {
            Ok(Vec::new())
        }
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
            ClusterRole::AllInOne,
            BackendTopologySnapshot::empty(topology_revision),
            Some(deadline),
            cancellation.view(),
            SessionOptimizerSettings::default(),
        ))
    }

    fn not_ctas(
        _sql: &str,
        _context: &RequestContext,
        _query_options: &QueryOptions,
    ) -> Result<Option<()>, crate::dml::DmlError> {
        Ok(None)
    }

    fn not_truncate(
        _sql: &str,
        _context: &RequestContext,
        _query_options: &QueryOptions,
    ) -> Result<Option<()>, crate::dml::DmlError> {
        Ok(None)
    }

    fn not_add_files(
        _sql: &str,
        _context: &RequestContext,
        _query_options: &QueryOptions,
    ) -> Result<Option<u32>, crate::dml::DmlError> {
        Ok(None)
    }

    #[test]
    fn sqlx2_application_frontend_router_handles_insert_before_core_command() {
        let engine = RecordingInsertEngine::default();
        let delete_engine = RecordingDeleteEngine::default();
        let command = RecordingCoreCommand::default();
        let dml = DmlService::compose(None, Arc::new(EmptyStatisticsService));
        let cancellation = QueryCancellationSource::new();
        let context =
            router_test_context(41, Instant::now() + Duration::from_secs(30), &cancellation);

        let error = execute_frontend_command(
            &dml,
            &engine,
            &delete_engine,
            None,
            not_ctas,
            not_truncate,
            not_add_files,
            &command,
            "INSERT INTO t VALUES (1)",
            &context,
            QueryOptions::default(),
        )
        .expect_err("Iceberg INSERT without StateStore must fail in DML");

        assert!(error.to_string().contains("state store is required"));
        assert_eq!(engine.resolve_contexts.lock().unwrap().len(), 1);
        assert_eq!(command.calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn sqlx2_application_frontend_router_passes_one_request_context_to_dml() {
        let engine = RecordingInsertEngine::default();
        let delete_engine = RecordingDeleteEngine::default();
        let command = RecordingCoreCommand::default();
        let dml = DmlService::compose(None, Arc::new(EmptyStatisticsService));
        let cancellation = QueryCancellationSource::new();
        let deadline = Instant::now() + Duration::from_secs(30);
        let context = router_test_context(73, deadline, &cancellation);

        let error = execute_frontend_command(
            &dml,
            &engine,
            &delete_engine,
            None,
            not_ctas,
            not_truncate,
            not_add_files,
            &command,
            "INSERT INTO t VALUES (1)",
            &context,
            QueryOptions::default(),
        )
        .expect_err("Iceberg INSERT without StateStore must fail in DML");
        assert!(error.to_string().contains("state store is required"));

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
        let dml = DmlService::new(Arc::new(
            crate::dml::journal::testing::InMemoryOperationJournal::default(),
        ));
        let cancellation = QueryCancellationSource::new();
        let deadline = Instant::now() + Duration::from_secs(30);
        let context = router_test_context(88, deadline, &cancellation);

        execute_frontend_command(
            &dml,
            &engine,
            &delete_engine,
            None,
            not_ctas,
            not_truncate,
            not_add_files,
            &command,
            "DELETE FROM t WHERE a = 1",
            &context,
            QueryOptions::default(),
        )
        // The statement is routed to the frontend DELETE owner and never falls
        // through to the core command. It then fails closed inside the DML
        // runner: CP-3B forbids dispatching a writer before an external
        // operation fence is established, and this focused-test service has no
        // coordination authority to mint one from.
        .expect_err("an unfenced DELETE must not dispatch");

        let executions = delete_engine.executions.lock().unwrap();
        assert_eq!(executions.len(), 1);
        assert_eq!(executions[0].topology().revision(), 88);
        assert_eq!(executions[0].deadline(), Some(deadline));
        assert_eq!(command.calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn non_insert_still_reaches_core_command_executor() {
        let engine = RecordingInsertEngine::default();
        let delete_engine = RecordingDeleteEngine::default();
        let command = RecordingCoreCommand::default();
        let dml = DmlService::compose(None, Arc::new(EmptyStatisticsService));
        let cancellation = QueryCancellationSource::new();
        let deadline = Instant::now() + Duration::from_secs(30);
        let context = router_test_context(91, deadline, &cancellation);

        execute_frontend_command(
            &dml,
            &engine,
            &delete_engine,
            None,
            not_ctas,
            not_truncate,
            not_add_files,
            &command,
            "CREATE DATABASE db2",
            &context,
            QueryOptions::default(),
        )
        .expect("core command route");

        assert!(engine.resolve_contexts.lock().unwrap().is_empty());
        assert_eq!(command.calls.load(Ordering::SeqCst), 1);
        let contexts = command.contexts.lock().unwrap();
        assert_eq!(contexts[0].topology().revision(), 91);
        assert_eq!(contexts[0].deadline(), Some(deadline));
    }

    #[test]
    fn frontend_router_orders_ctas_before_truncate_and_fallback() {
        let insert = RecordingInsertEngine::default();
        let delete = RecordingDeleteEngine::default();
        let command = RecordingCoreCommand::default();
        let dml = DmlService::compose(None, Arc::new(EmptyStatisticsService));
        let cancellation = QueryCancellationSource::new();
        let context =
            router_test_context(92, Instant::now() + Duration::from_secs(30), &cancellation);
        let ctas_calls = AtomicUsize::new(0);
        let truncate_calls = AtomicUsize::new(0);

        execute_frontend_command(
            &dml,
            &insert,
            &delete,
            None,
            |_, _, _| {
                ctas_calls.fetch_add(1, Ordering::SeqCst);
                Ok(Some(()))
            },
            |_, _, _| {
                truncate_calls.fetch_add(1, Ordering::SeqCst);
                Ok(None)
            },
            not_add_files,
            &command,
            "CREATE TABLE ice.db.dst AS SELECT 1",
            &context,
            QueryOptions::default(),
        )
        .expect("frontend CTAS route");

        assert_eq!(ctas_calls.load(Ordering::SeqCst), 1);
        assert_eq!(truncate_calls.load(Ordering::SeqCst), 0);
        assert_eq!(command.calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn frontend_router_target_errors_never_fall_back() {
        let insert = RecordingInsertEngine::default();
        let delete = RecordingDeleteEngine::default();
        let command = RecordingCoreCommand::default();
        let dml = DmlService::compose(None, Arc::new(EmptyStatisticsService));
        let cancellation = QueryCancellationSource::new();
        let context =
            router_test_context(93, Instant::now() + Duration::from_secs(30), &cancellation);
        let error = execute_frontend_command(
            &dml,
            &insert,
            &delete,
            None,
            |_, _, _| Err(crate::dml::DmlError::executor("CTAS failed")),
            |_, _, _| panic!("TRUNCATE route must not follow a CTAS error"),
            not_add_files,
            &command,
            "CREATE TABLE ice.db.dst AS SELECT 1",
            &context,
            QueryOptions::default(),
        )
        .unwrap_err();

        assert!(error.contains("CTAS failed"));
        assert_eq!(command.calls.load(Ordering::SeqCst), 0);

        let error = execute_frontend_command(
            &dml,
            &insert,
            &delete,
            None,
            |_, _, _| Ok(None),
            |_, _, _| Err(crate::dml::DmlError::executor("TRUNCATE failed")),
            not_add_files,
            &command,
            "TRUNCATE TABLE ice.db.dst",
            &context,
            QueryOptions::default(),
        )
        .unwrap_err();
        assert!(error.contains("TRUNCATE failed"));
        assert_eq!(command.calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn frontend_router_add_files_returns_status_and_never_falls_back() {
        let insert = RecordingInsertEngine::default();
        let delete = RecordingDeleteEngine::default();
        let command = RecordingCoreCommand::default();
        let dml = DmlService::compose(None, Arc::new(EmptyStatisticsService));
        let cancellation = QueryCancellationSource::new();
        let context =
            router_test_context(94, Instant::now() + Duration::from_secs(30), &cancellation);

        let result = execute_frontend_command(
            &dml,
            &insert,
            &delete,
            None,
            not_ctas,
            not_truncate,
            |_, _, _| Ok(Some(7)),
            &command,
            "ALTER TABLE ice.db.dst ADD FILES FROM 's3://warehouse/staged'",
            &context,
            QueryOptions::default(),
        )
        .expect("ADD FILES frontend route");
        assert!(matches!(result, StatementResult::Query(_)));
        assert_eq!(command.calls.load(Ordering::SeqCst), 0);

        let error = execute_frontend_command(
            &dml,
            &insert,
            &delete,
            None,
            not_ctas,
            not_truncate,
            |_, _, _| Err(crate::dml::DmlError::executor("ADD FILES failed")),
            &command,
            "ALTER TABLE ice.db.dst ADD FILES FROM 's3://warehouse/staged'",
            &context,
            QueryOptions::default(),
        )
        .unwrap_err();
        assert!(error.contains("ADD FILES failed"));
        assert_eq!(command.calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn frontend_router_recognized_mutations_never_fall_back_to_core() {
        let insert = RecordingInsertEngine::default();
        let delete = RecordingDeleteEngine::default();
        let mutation = RejectingMutationEngine;
        let command = RecordingCoreCommand::default();
        let dml = DmlService::new(Arc::new(
            crate::dml::journal::testing::InMemoryOperationJournal::default(),
        ));
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
                not_truncate,
                not_add_files,
                &command,
                sql,
                &context,
                QueryOptions::default(),
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
    fn frontend_router_runs_truncate_before_fallback() {
        let insert = RecordingInsertEngine::default();
        let delete = RecordingDeleteEngine::default();
        let command = RecordingCoreCommand::default();
        let dml = DmlService::compose(None, Arc::new(EmptyStatisticsService));
        let cancellation = QueryCancellationSource::new();
        let context =
            router_test_context(94, Instant::now() + Duration::from_secs(30), &cancellation);
        let ctas_calls = AtomicUsize::new(0);
        let truncate_calls = AtomicUsize::new(0);

        execute_frontend_command(
            &dml,
            &insert,
            &delete,
            None,
            |_, _, _| {
                ctas_calls.fetch_add(1, Ordering::SeqCst);
                Ok(None)
            },
            |_, _, _| {
                truncate_calls.fetch_add(1, Ordering::SeqCst);
                Ok(Some(()))
            },
            not_add_files,
            &command,
            "TRUNCATE TABLE ice.db.dst",
            &context,
            QueryOptions::default(),
        )
        .expect("frontend TRUNCATE route");

        assert_eq!(ctas_calls.load(Ordering::SeqCst), 1);
        assert_eq!(truncate_calls.load(Ordering::SeqCst), 1);
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
    fn kill_query_parser_accepts_explicit_and_short_forms() {
        assert_eq!(parse_kill_query("KILL QUERY 17").unwrap(), Some(17));
        assert_eq!(parse_kill_query("kill 18").unwrap(), Some(18));
        assert_eq!(parse_kill_query("SELECT 1").unwrap(), None);
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
    }

    #[test]
    fn parenthesized_query_accepts_scalar_selects_and_ctes() {
        assert_eq!(parenthesized_query("(SELECT 1)"), Some("SELECT 1"));
        assert_eq!(
            parenthesized_query(" (WITH values_cte AS (SELECT 1) SELECT * FROM values_cte) "),
            Some("WITH values_cte AS (SELECT 1) SELECT * FROM values_cte")
        );
    }

    #[test]
    fn parenthesized_query_rejects_non_query_expressions() {
        assert_eq!(parenthesized_query("(array[1, 2])"), None);
        assert_eq!(parenthesized_query("SELECT 1"), None);
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
    fn explain_refresh_is_a_command_but_explain_select_is_a_query() {
        assert!(is_query_statement("EXPLAIN VERBOSE SELECT 1"));
        assert!(is_query_statement(
            "EXPLAIN ANALYZE WITH cte AS (SELECT 1) SELECT * FROM cte"
        ));
        assert!(!is_query_statement("EXPLAIN REFRESH MATERIALIZED VIEW mv"));
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
}
