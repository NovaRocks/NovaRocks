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

//! Frontend-owned SQL admission and native assembly boundary.

use std::num::NonZeroUsize;
use std::sync::Arc;

use crate::catalog_application::information_schema;
use crate::catalog_application::query_materializer::build_catalog_service_provider;
use crate::catalog_application::virtual_table;
use crate::common::admitted_query_context::{QueryExecutionContext, RequestContext};
use crate::connector::connector_request_context_for_query;
use crate::mv::domain::repository::MvRepository;
use crate::native::fragment_encoder::encode_native_fragment_bundle;
use crate::query_execution::PreparedQueryOperation;
use crate::query_execution::compiler::{
    TableLookupMode, freeze_query_mv_rewrite_definition_index, query_catalog_service_snapshot,
    query_statistics_snapshot,
};
use crate::query_execution::kernels::{
    QueryPreparationKernel, SystemTableQueryKernel, ViewExecutionKernel,
};
use crate::query_execution::planning::sql_cancellation_observation;
use crate::query_execution::planning::time_travel::{
    TimeTravelRewriteError, has_time_travel_refs, rewrite_time_travel_refs,
};
use crate::query_execution::post_compile::{PostCompileIntent, prepare_compiled_distributed_query};
use crate::view::ViewRequestContext;
use novarocks_parser::ast::{ExplainFormat, ExplainQuery, Query, Statement};
use novarocks_proto::lifecycle::QueryOptions;
use novarocks_spi::connector::MvStorageObservationPort;
use novarocks_sql::analyze_error::AnalyzeError;
use novarocks_sql::compiler::{
    ExplainLevel, SqlAnalyzeRequest, SqlCompileControl, SqlCompileError, SqlCompileIntent,
    SqlCompiler, SqlOptimizeRequest, SqlPlanningEnvironment, SqlSessionContext, SqlStatementInput,
    builtin_sql_function_catalog,
};

/// Preserves SQL analyze-domain facts until the session still has the original
/// SQL source required to render a user location.
#[derive(Debug)]
pub(crate) enum FrontendQueryCompilerError {
    Engine(String),
    Analyze(AnalyzeError),
}

impl FrontendQueryCompilerError {
    fn from_compile(error: SqlCompileError) -> Self {
        match error {
            SqlCompileError::Analyze(error) => Self::Analyze(error),
            error => Self::Engine(error.to_string()),
        }
    }
}

impl From<String> for FrontendQueryCompilerError {
    fn from(error: String) -> Self {
        Self::Engine(error)
    }
}

impl From<TimeTravelRewriteError> for FrontendQueryCompilerError {
    fn from(error: TimeTravelRewriteError) -> Self {
        match error {
            TimeTravelRewriteError::Engine(error) => Self::Engine(error),
            TimeTravelRewriteError::Analyze(error) => Self::Analyze(error),
        }
    }
}

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

    pub(crate) fn prepare_statement(
        &self,
        statement: &Statement,
        context: &RequestContext,
        query_options: Option<QueryOptions>,
    ) -> Result<PreparedQueryOperation, FrontendQueryCompilerError> {
        let connector_context = connector_request_context_for_query(
            query_options.as_ref(),
            context.execution().cancellation().clone(),
        )?;
        let current_catalog = context.session().current_catalog();
        let current_database = context.session().current_database();

        match statement {
            Statement::ExplainQuery(explain) if explain.format != ExplainFormat::Analyze => {
                let (level, force_logical_explain) = explain_mode(explain);
                let query = self.prepare_explain_query(
                    explain.query.as_ref(),
                    current_catalog,
                    current_database,
                    &connector_context,
                )?;
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
                .map_err(FrontendQueryCompilerError::from_compile)?;
                let output = if force_logical_explain {
                    analyzed
                        .into_complete()
                        .map_err(FrontendQueryCompilerError::from_compile)?
                } else {
                    let analyzed = analyzed
                        .into_pending()
                        .map_err(FrontendQueryCompilerError::from_compile)?;
                    let statistics =
                        query_statistics_snapshot(&self.query, &materializer, &connector_context)?;
                    SqlCompiler::optimize(SqlOptimizeRequest::new(analyzed, &statistics))
                        .map_err(FrontendQueryCompilerError::from_compile)?
                };
                Ok(PreparedQueryOperation::explain_lines(
                    output
                        .into_explain_lines(level, force_logical_explain)
                        .map_err(FrontendQueryCompilerError::from_compile)?,
                )?)
            }
            Statement::ExplainQuery(explain) => self.prepare_explain_analyze(
                explain.query.as_ref(),
                current_catalog,
                current_database,
                query_options,
                &connector_context,
                context.execution(),
            ),
            Statement::Query(query) => {
                if let Some(result) = information_schema::try_query_materialized_views(
                    self.system_tables.mv_repository().as_ref(),
                    query,
                )? {
                    return Ok(PreparedQueryOperation::immediate(result));
                }
                let query = self.prepare_query(
                    query,
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
            _ => Err(FrontendQueryCompilerError::Engine(
                "query compiler only supports SELECT and EXPLAIN statements".to_string(),
            )),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn prepare_distributed_query(
        &self,
        query: &Query,
        current_catalog: Option<&str>,
        current_database: &str,
        query_options: Option<QueryOptions>,
        connector_context: novarocks_spi::connector::ConnectorRequestContext,
        execution: &QueryExecutionContext,
        intent: SqlCompileIntent,
        allow_mv_rewrite_candidates: bool,
        completion_intent: PostCompileIntent,
    ) -> Result<PreparedQueryOperation, FrontendQueryCompilerError> {
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
        .map_err(FrontendQueryCompilerError::from_compile)?
        .into_pending()
        .map_err(FrontendQueryCompilerError::from_compile)?;
        let statistics = query_statistics_snapshot(&self.query, &materializer, &connector_context)?;
        let distributed_plan =
            SqlCompiler::optimize(SqlOptimizeRequest::new(analyzed, &statistics))
                .map_err(FrontendQueryCompilerError::from_compile)?
                .into_distributed_plan()
                .map_err(FrontendQueryCompilerError::from_compile)?;
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
        Ok(assembly.into_operation(native_bundle, completion)?)
    }

    #[allow(clippy::too_many_arguments)]
    fn analyze_request<'a>(
        &self,
        query: &Query,
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
            crate::query_execution::constant_eval::constant_evaluator(),
            mv_definitions,
            SqlCompileControl::new(
                execution.deadline(),
                sql_cancellation_observation(execution.cancellation().clone()),
            ),
        ))
    }

    fn prepare_query(
        &self,
        query: &Query,
        current_catalog: Option<&str>,
        current_database: &str,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<Query, FrontendQueryCompilerError> {
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
        query: &Query,
        current_catalog: Option<&str>,
        current_database: &str,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<Query, FrontendQueryCompilerError> {
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
        query: &Query,
        current_catalog: Option<&str>,
        current_database: &str,
        query_options: Option<QueryOptions>,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
        execution: &QueryExecutionContext,
    ) -> Result<PreparedQueryOperation, FrontendQueryCompilerError> {
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
        .map(|options| *options.as_proto())
        .unwrap_or_default();
    raw.enable_profile = true;
    QueryOptions::parse(raw).expect("enabling query profiling does not invalidate query options")
}

pub(crate) fn explain_mode(explain: &ExplainQuery) -> (ExplainLevel, bool) {
    let level = match explain.format {
        ExplainFormat::Default => ExplainLevel::Normal,
        ExplainFormat::Verbose => ExplainLevel::Verbose,
        ExplainFormat::Costs => ExplainLevel::Costs,
        ExplainFormat::Logical => ExplainLevel::Normal,
        ExplainFormat::Analyze => ExplainLevel::Analyze,
    };
    (
        level,
        explain.logical || matches!(explain.format, ExplainFormat::Logical),
    )
}
