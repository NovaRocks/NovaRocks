// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain a copy of the
// License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Frontend-owned SQL admission and native assembly boundary.

use std::num::NonZeroUsize;
use std::sync::Arc;

use crate::native::fragment_encoder::encode_native_fragment_bundle;
use novarocks::catalog_application::information_schema;
use novarocks::catalog_application::query_materializer::build_catalog_service_provider;
use novarocks::catalog_application::virtual_table;
use novarocks::connector::connector_request_context_for_query;
use novarocks::mv::repository::MvRepository;
use novarocks::mv::storage_observation::MvStorageObservationPort;
use novarocks::query_execution::PreparedQueryOperation;
use novarocks::query_execution::compiler::{
    TableLookupMode, freeze_query_mv_rewrite_definition_index, query_catalog_service_snapshot,
    query_statistics_snapshot,
};
use novarocks::query_execution::kernels::{
    QueryPreparationKernel, SystemTableQueryKernel, ViewExecutionKernel,
};
use novarocks::query_execution::planning::sql_cancellation_observation;
use novarocks::query_execution::planning::time_travel::{
    has_time_travel_refs, rewrite_time_travel_refs,
};
use novarocks::query_execution::post_compile::{
    PostCompileIntent, prepare_compiled_distributed_query,
};
use novarocks::query_execution::request_context::{QueryExecutionContext, RequestContext};
use novarocks::view::ViewRequestContext;
use novarocks_protocol::lifecycle::QueryOptions;
use novarocks_sql::compiler::{
    ExplainLevel, SqlAnalyzeRequest, SqlCompileControl, SqlCompileIntent, SqlCompiler,
    SqlOptimizeRequest, SqlPlanningEnvironment, SqlSessionContext, SqlStatementInput,
    builtin_sql_function_catalog,
};
use novarocks_sql::syntax::{normalize_for_raw_parse, parse_normalized_sql_raw};

#[derive(Clone)]
pub(crate) struct FrontendQueryCompiler {
    query: QueryPreparationKernel,
    view: ViewExecutionKernel,
    system_tables: SystemTableQueryKernel,
    mv_repository: Arc<dyn MvRepository>,
    mv_storage_observation: Arc<dyn MvStorageObservationPort>,
}

impl FrontendQueryCompiler {
    pub(crate) fn new(
        query: QueryPreparationKernel,
        view: ViewExecutionKernel,
        system_tables: SystemTableQueryKernel,
        mv_repository: Arc<dyn MvRepository>,
        mv_storage_observation: Arc<dyn MvStorageObservationPort>,
    ) -> Self {
        Self {
            query,
            view,
            system_tables,
            mv_repository,
            mv_storage_observation,
        }
    }

    pub(crate) fn prepare(
        &self,
        sql: &str,
        context: &RequestContext,
        query_options: Option<QueryOptions>,
    ) -> Result<PreparedQueryOperation, String> {
        if !is_query_sql(sql) {
            return Err(
                "non-query statements must be executed through a typed command capability".into(),
            );
        }
        let connector_context = connector_request_context_for_query(
            query_options.as_ref(),
            context.execution().cancellation().clone(),
        )?;
        let current_catalog = context.session().current_catalog();
        let current_database = context.session().current_database();
        let normalized = normalize_for_raw_parse(sql)?;
        let (parse_sql, forced_explain_level, force_logical_explain) =
            if let Some((rewritten, level)) = split_explain_logical_sql(&normalized) {
                (rewritten, Some(level), true)
            } else if let Some((rewritten, level)) = split_explain_costs_sql(&normalized) {
                (rewritten, Some(level), false)
            } else {
                (normalized.clone(), None, false)
            };
        let statement = parse_normalized_sql_raw(&parse_sql)
            .map_err(|error| format_parser_error(&error.to_string()))?;

        match statement {
            sqlparser::ast::Statement::Explain {
                statement,
                verbose,
                analyze: false,
                ..
            } => {
                let sqlparser::ast::Statement::Query(ref query) = *statement else {
                    return Err("EXPLAIN only supports SELECT queries".to_string());
                };
                let query = self.prepare_explain_query(
                    query,
                    current_catalog,
                    current_database,
                    &connector_context,
                )?;
                let level = forced_explain_level.unwrap_or(if verbose {
                    ExplainLevel::Verbose
                } else {
                    ExplainLevel::Normal
                });
                let catalog_service = query_catalog_service_snapshot(&self.query);
                let materializer = build_catalog_service_provider(
                    current_catalog,
                    &catalog_service,
                    self.query.connector_control().as_ref(),
                    connector_context.clone(),
                    TableLookupMode::ExplainStats,
                    self.query.catalog_application().map(Arc::as_ref),
                );
                let mv_definitions = if force_logical_explain {
                    None
                } else {
                    Some(freeze_query_mv_rewrite_definition_index(
                        &self.query,
                        self.mv_repository.as_ref(),
                        self.mv_storage_observation.as_ref(),
                    )?)
                };
                let analyzed = SqlCompiler::analyze(self.analyze_request(
                    &query,
                    current_catalog,
                    current_database,
                    context.execution(),
                    &materializer,
                    mv_definitions.as_ref(),
                    if force_logical_explain {
                        SqlCompileIntent::LogicalOnly
                    } else {
                        SqlCompileIntent::Explain {
                            level,
                            analyze: false,
                        }
                    },
                )?)
                .map_err(|error| error.to_string())?;
                let output = if force_logical_explain {
                    analyzed
                        .into_complete()
                        .map_err(|error| error.to_string())?
                } else {
                    let analyzed = analyzed.into_pending().map_err(|error| error.to_string())?;
                    let statistics =
                        query_statistics_snapshot(&self.query, &materializer, &connector_context)?;
                    SqlCompiler::optimize(SqlOptimizeRequest::new(analyzed, &statistics))
                        .map_err(|error| error.to_string())?
                };
                PreparedQueryOperation::explain_lines(
                    output
                        .into_explain_lines(level, force_logical_explain)
                        .map_err(|error| error.to_string())?,
                )
            }
            sqlparser::ast::Statement::Explain {
                statement,
                analyze: true,
                ..
            } => {
                let sqlparser::ast::Statement::Query(ref query) = *statement else {
                    return Err("EXPLAIN ANALYZE only supports SELECT queries".to_string());
                };
                self.prepare_explain_analyze(
                    query,
                    current_catalog,
                    current_database,
                    query_options,
                    &connector_context,
                    context.execution(),
                )
            }
            sqlparser::ast::Statement::Query(ref query) => {
                if let Some(result) = information_schema::try_query_materialized_views(
                    self.system_tables.mv_repository().as_ref(),
                    query,
                )? {
                    return Ok(PreparedQueryOperation::immediate(result));
                }
                let query = self.prepare_query(
                    query.as_ref(),
                    current_catalog,
                    current_database,
                    &connector_context,
                )?;
                self.prepare_distributed_query(
                    &query,
                    current_catalog,
                    current_database,
                    query_options,
                    connector_context,
                    context.execution(),
                    SqlCompileIntent::Query,
                    true,
                    PostCompileIntent::Result,
                )
            }
            _ => Err("query compiler only supports SELECT and EXPLAIN statements".to_string()),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn prepare_distributed_query(
        &self,
        query: &sqlparser::ast::Query,
        current_catalog: Option<&str>,
        current_database: &str,
        query_options: Option<QueryOptions>,
        connector_context: novarocks_spi::connector::ConnectorRequestContext,
        execution: &QueryExecutionContext,
        intent: SqlCompileIntent,
        allow_mv_rewrite_candidates: bool,
        completion_intent: PostCompileIntent,
    ) -> Result<PreparedQueryOperation, String> {
        let catalog_service = query_catalog_service_snapshot(&self.query);
        let materializer = build_catalog_service_provider(
            current_catalog,
            &catalog_service,
            self.query.connector_control().as_ref(),
            connector_context.clone(),
            TableLookupMode::SchemaOnly,
            self.query.catalog_application().map(Arc::as_ref),
        );
        let mv_definitions =
            if allow_mv_rewrite_candidates && self.mv_repository.availability().is_available() {
                Some(freeze_query_mv_rewrite_definition_index(
                    &self.query,
                    self.mv_repository.as_ref(),
                    self.mv_storage_observation.as_ref(),
                )?)
            } else {
                None
            };
        let analyzed = SqlCompiler::analyze(self.analyze_request(
            query,
            current_catalog,
            current_database,
            execution,
            &materializer,
            mv_definitions.as_ref(),
            intent,
        )?)
        .map_err(|error| error.to_string())?
        .into_pending()
        .map_err(|error| error.to_string())?;
        let statistics = query_statistics_snapshot(&self.query, &materializer, &connector_context)?;
        let distributed_plan =
            SqlCompiler::optimize(SqlOptimizeRequest::new(analyzed, &statistics))
                .map_err(|error| error.to_string())?
                .into_distributed_plan()
                .map_err(|error| error.to_string())?;
        let (assembly, completion) = prepare_compiled_distributed_query(
            distributed_plan,
            &self.query,
            &materializer,
            &connector_context,
            query_options,
            execution,
            completion_intent,
        )?;
        let native_bundle = encode_native_fragment_bundle(assembly.encoding().encoding_view())?;
        assembly.into_operation(native_bundle, completion)
    }

    #[allow(clippy::too_many_arguments)]
    fn analyze_request<'a>(
        &self,
        query: &sqlparser::ast::Query,
        current_catalog: Option<&str>,
        current_database: &str,
        execution: &QueryExecutionContext,
        materializer: &'a dyn novarocks_sql::compiler::SqlCatalogSnapshot,
        mv_definitions: Option<&'a novarocks_sql::compiler::MvRewriteDefinitionIndex>,
        intent: SqlCompileIntent,
    ) -> Result<SqlAnalyzeRequest<'a>, String> {
        let backend_count =
            NonZeroUsize::new(execution.topology().targets().len()).ok_or_else(|| {
                "SQL compilation requires a non-empty admitted backend topology".to_string()
            })?;
        Ok(SqlAnalyzeRequest::new(
            SqlStatementInput::parsed_query(Box::new(query.clone())),
            intent,
            SqlSessionContext {
                current_catalog: current_catalog.map(str::to_string),
                current_database: current_database.to_string(),
                optimizer_settings: execution.optimizer_settings().clone(),
            },
            SqlPlanningEnvironment::Distributed { backend_count },
            materializer,
            builtin_sql_function_catalog(),
            mv_definitions,
            SqlCompileControl::new(
                execution.deadline(),
                sql_cancellation_observation(execution.cancellation().clone()),
            ),
        ))
    }

    fn prepare_query(
        &self,
        query: &sqlparser::ast::Query,
        current_catalog: Option<&str>,
        current_database: &str,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<sqlparser::ast::Query, String> {
        let mut prepared = self.prepare_explain_query(
            query,
            current_catalog,
            current_database,
            connector_context,
        )?;
        virtual_table::rewrite_query(
            self.system_tables.catalog_service(),
            self.system_tables.connector_control().as_ref(),
            self.system_tables.system_catalog().as_ref(),
            &mut prepared,
        )?;
        Ok(prepared)
    }

    fn prepare_explain_query(
        &self,
        query: &sqlparser::ast::Query,
        current_catalog: Option<&str>,
        current_database: &str,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<sqlparser::ast::Query, String> {
        let mut prepared = query.clone();
        self.view.view_service().rewrite_query(
            &self.view,
            &mut prepared,
            ViewRequestContext {
                current_catalog,
                current_database,
                connector_context: Some(connector_context),
            },
        )?;
        if has_time_travel_refs(&prepared) {
            rewrite_time_travel_refs(
                &self.query,
                current_catalog,
                current_database,
                &mut prepared,
                connector_context,
            )?;
        }
        Ok(prepared)
    }

    fn prepare_explain_analyze(
        &self,
        query: &sqlparser::ast::Query,
        current_catalog: Option<&str>,
        current_database: &str,
        query_options: Option<QueryOptions>,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
        execution: &QueryExecutionContext,
    ) -> Result<PreparedQueryOperation, String> {
        let query = self.prepare_explain_query(
            query,
            current_catalog,
            current_database,
            connector_context,
        )?;
        let planning_start = std::time::Instant::now();
        self.prepare_distributed_query(
            &query,
            current_catalog,
            current_database,
            Some(query_options_for_explain_analyze(query_options)),
            connector_context.clone(),
            execution,
            SqlCompileIntent::Explain {
                level: ExplainLevel::Analyze,
                analyze: true,
            },
            true,
            PostCompileIntent::Profile {
                planning_elapsed: planning_start.elapsed(),
                execution_started_at: std::time::Instant::now(),
            },
        )
    }
}

fn query_options_for_explain_analyze(query_options: Option<QueryOptions>) -> QueryOptions {
    let mut raw = query_options
        .as_ref()
        .map(|options| options.as_proto().clone())
        .unwrap_or_default();
    raw.enable_profile = true;
    QueryOptions::parse(raw).expect("enabling query profiling does not invalidate query options")
}

fn is_query_sql(sql: &str) -> bool {
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

fn format_parser_error(raw: &str) -> String {
    let mut out = format!("sql parser error: {raw}");
    if let Some(start) = raw.find("found: ") {
        let after = &raw[start + "found: ".len()..];
        let token = after
            .split(|character: char| character.is_whitespace() || character == ',')
            .next()
            .unwrap_or("")
            .trim()
            .trim_matches(|character: char| character == '`' || character == '"');
        if !token.is_empty() {
            out.push_str(&format!(" Unexpected input '{token}'."));
        }
    }
    out
}

fn split_explain_costs_sql(sql: &str) -> Option<(String, ExplainLevel)> {
    let body = consume_leading_keyword(consume_leading_keyword(sql, "EXPLAIN")?, "COSTS")?;
    Some((
        format!("EXPLAIN {}", body.trim_start()),
        ExplainLevel::Costs,
    ))
}

fn split_explain_logical_sql(sql: &str) -> Option<(String, ExplainLevel)> {
    let mut body = consume_leading_keyword(consume_leading_keyword(sql, "EXPLAIN")?, "LOGICAL")?;
    let mut level = ExplainLevel::Normal;
    for (keyword, candidate) in [
        ("VERBOSE", ExplainLevel::Verbose),
        ("COSTS", ExplainLevel::Costs),
    ] {
        if let Some(rest) = consume_leading_keyword(body, keyword) {
            level = candidate;
            body = rest;
            break;
        }
    }
    Some((format!("EXPLAIN {}", body.trim_start()), level))
}

fn consume_leading_keyword<'a>(sql: &'a str, keyword: &str) -> Option<&'a str> {
    let trimmed = sql.trim_start();
    let head = trimmed.as_bytes().get(..keyword.len())?;
    if !head.eq_ignore_ascii_case(keyword.as_bytes()) {
        return None;
    }
    let rest = &trimmed[keyword.len()..];
    if rest
        .chars()
        .next()
        .is_some_and(|character| !character.is_ascii_whitespace())
    {
        return None;
    }
    Some(rest)
}
