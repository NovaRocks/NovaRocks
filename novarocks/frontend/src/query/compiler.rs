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
use crate::catalog_application::query_bindings::QueryTableBindingStore;
use crate::catalog_application::query_materializer::{
    build_catalog_service_provider,
    build_catalog_service_provider_with_bindings_and_query_local_overlays,
};
use crate::catalog_application::virtual_table;
use crate::common::admitted_query_context::{
    QueryExecutionContext, RequestContext, StatementAdmissionContext,
};
use crate::common::statement_effect::StatementEffectTracker;
use crate::connector::connector_request_context_for_query;
use crate::mv::domain::readiness::MvReadinessPort;
use crate::native::fragment_encoder::encode_native_fragment_bundle;
use crate::query_execution::compiler::{
    TableLookupMode, freeze_query_mv_rewrite_definition_index, query_catalog_service_snapshot,
    query_statistics_snapshot,
};
use crate::query_execution::completion::{PreReadyRetryBoundary, PreparedDistributedRoundFactory};
use crate::query_execution::contract::{DistributedQueryError, DistributedQueryErrorKind};
use crate::query_execution::kernels::{
    QueryPreparationKernel, SystemTableQueryKernel, ViewExecutionKernel,
};
use crate::query_execution::planning::sql_cancellation_observation;
use crate::query_execution::planning::time_travel::{
    TimeTravelRewriteError, has_time_travel_refs, rewrite_time_travel_refs,
};
use crate::query_execution::post_compile::{PostCompileIntent, prepare_compiled_distributed_query};
use crate::query_execution::{PreparedQueryDistributedOperation, PreparedQueryOperation};
use crate::view::ViewRequestContext;
use novarocks_parser::ast::{ExplainFormat, ExplainQuery, Query, Statement};
use novarocks_proto_codec::lifecycle::QueryOptions;
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

/// A lake-native MV target is a physical provider table, but it is not an
/// ordinary external relation once this frontend has recorded it as an MV.
/// If startup quarantined that target, admitting its provider scan would let a
/// stale MV publication bypass the readiness boundary through plain SQL.
fn reject_quarantined_mv_targets(
    bindings: &QueryTableBindingStore,
    readiness: &MvReadinessPort,
) -> Result<(), FrontendQueryCompilerError> {
    for (_, binding) in bindings.captured_bindings() {
        let identity =
            novarocks_sql::planning::catalog::materialization_identity_facts(&binding.resolved);
        let target = crate::mv::domain::model::MvTarget {
            catalog: Some(identity.catalog().to_string()),
            database: identity.namespace().to_string(),
            name: identity.table().to_string(),
        };
        match readiness.load_ready(&target) {
            Ok(_) => {}
            Err(error)
                if error.kind()
                    == crate::mv::domain::repository::MvRepositoryErrorKind::Unavailable =>
            {
                return Err(FrontendQueryCompilerError::Engine(format!(
                    "unknown table: {}.{}",
                    identity.namespace(),
                    identity.table()
                )));
            }
            Err(error) => return Err(FrontendQueryCompilerError::Engine(error.to_string())),
        }
    }
    Ok(())
}

#[derive(Clone)]
pub(crate) struct FrontendQueryCompiler {
    query: QueryPreparationKernel,
    view: ViewExecutionKernel,
    system_tables: SystemTableQueryKernel,
    mv_readiness: Arc<MvReadinessPort>,
    mv_storage_observation: Arc<dyn MvStorageObservationPort>,
}

enum RetryCompletionTemplate {
    Result,
    Profile {
        planning_started_at: std::time::Instant,
    },
}

impl RetryCompletionTemplate {
    fn from_first_round(intent: &PostCompileIntent) -> Self {
        match intent {
            PostCompileIntent::Result => Self::Result,
            PostCompileIntent::Profile {
                planning_elapsed, ..
            } => Self::Profile {
                planning_started_at: std::time::Instant::now() - *planning_elapsed,
            },
        }
    }

    fn next_round_intent(&self) -> PostCompileIntent {
        match self {
            Self::Result => PostCompileIntent::Result,
            Self::Profile {
                planning_started_at,
            } => PostCompileIntent::Profile {
                planning_elapsed: planning_started_at.elapsed(),
                execution_started_at: std::time::Instant::now(),
            },
        }
    }
}

/// Owns only statement-stable SQL admission facts. Each call to `replan`
/// reconstructs analysis, optimizer, fragments, split assignment, and native
/// attachment from a new frozen topology; it has no old distributed artifact
/// to clone or patch.
struct FrontendDistributedRoundFactory {
    compiler: FrontendQueryCompiler,
    query: Query,
    statement: StatementAdmissionContext,
    current_catalog: Option<String>,
    current_database: String,
    query_options: Option<QueryOptions>,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
    catalog_service: crate::catalog_application::query_catalog::QueryCatalogService,
    bindings: Arc<QueryTableBindingStore>,
    mv_definitions: Option<novarocks_sql::compiler::MvRewriteDefinitionIndex>,
    intent: SqlCompileIntent,
    completion: RetryCompletionTemplate,
    effect_tracker: StatementEffectTracker,
}

impl FrontendDistributedRoundFactory {
    fn error(error: FrontendQueryCompilerError) -> DistributedQueryError {
        let message = match error {
            FrontendQueryCompilerError::Engine(message) => message,
            FrontendQueryCompilerError::Analyze(error) => error.to_string(),
        };
        DistributedQueryError::new(DistributedQueryErrorKind::Failed, message)
    }
}

impl PreparedDistributedRoundFactory for FrontendDistributedRoundFactory {
    fn replan(
        &mut self,
        topology: crate::common::backend_topology::BackendTopologySnapshot,
    ) -> Result<PreparedQueryDistributedOperation, DistributedQueryError> {
        if !self.bindings.is_sealed_for_topology_replan() {
            return Err(DistributedQueryError::new(
                DistributedQueryErrorKind::ContractViolation,
                "topology replan requires a sealed first-round semantic binding store",
            ));
        }
        let execution = self.statement.for_topology(topology);
        let materializer = build_catalog_service_provider_with_bindings_and_query_local_overlays(
            self.current_catalog.as_deref(),
            &self.catalog_service,
            self.compiler.query.connector_control().as_ref(),
            self.connector_context.clone(),
            Arc::clone(&self.bindings),
            Vec::new(),
            self.compiler.query.catalog_application().map(Arc::as_ref),
        );
        let analyzed = SqlCompiler::analyze(
            self.compiler
                .analyze_request(
                    &self.query,
                    self.current_catalog.as_deref(),
                    &self.current_database,
                    execution.execution(),
                    &materializer,
                    self.mv_definitions.as_ref(),
                    self.intent.clone(),
                )
                .map_err(|error| {
                    DistributedQueryError::new(DistributedQueryErrorKind::Failed, error)
                })?,
        )
        .map_err(|error| {
            FrontendDistributedRoundFactory::error(FrontendQueryCompilerError::from_compile(error))
        })?
        .into_pending()
        .map_err(|error| {
            FrontendDistributedRoundFactory::error(FrontendQueryCompilerError::from_compile(error))
        })?;
        let statistics =
            query_statistics_snapshot(&self.compiler.query, &materializer, &self.connector_context)
                .map_err(|error| {
                    DistributedQueryError::new(DistributedQueryErrorKind::Failed, error)
                })?;
        let distributed_plan =
            SqlCompiler::optimize(SqlOptimizeRequest::new(analyzed, &statistics))
                .map_err(|error| {
                    FrontendDistributedRoundFactory::error(
                        FrontendQueryCompilerError::from_compile(error),
                    )
                })?
                .into_distributed_plan()
                .map_err(|error| {
                    FrontendDistributedRoundFactory::error(
                        FrontendQueryCompilerError::from_compile(error),
                    )
                })?;
        let (assembly, completion) = prepare_compiled_distributed_query(
            distributed_plan,
            &self.compiler.query,
            &materializer,
            &self.connector_context,
            self.query_options.clone(),
            execution.execution(),
            self.completion.next_round_intent(),
        )
        .map_err(|error| DistributedQueryError::new(DistributedQueryErrorKind::Failed, error))?;
        let native_bundle = encode_native_fragment_bundle(assembly.encoding().encoding_view())
            .map_err(|error| {
                DistributedQueryError::new(DistributedQueryErrorKind::Failed, error)
            })?;
        let request = assembly.finish(native_bundle).map_err(|error| {
            DistributedQueryError::new(DistributedQueryErrorKind::Failed, error)
        })?;
        Ok(PreparedQueryDistributedOperation::new(request, completion))
    }
}

impl PreReadyRetryBoundary for FrontendDistributedRoundFactory {
    fn permit_pre_ready_retry(&self) -> Result<(), DistributedQueryError> {
        self.effect_tracker
            .issue_topology_retry_permit()
            .map(|_| ())
            .map_err(|error| {
                DistributedQueryError::new(
                    DistributedQueryErrorKind::Rejected,
                    format!("topology retry is not effect-free: {error:?}"),
                )
            })
    }

    fn close_after_control_ready(&self) {
        self.effect_tracker.close_after_control_ready();
    }

    fn close_after_stage_or_start(&self) {
        self.effect_tracker.close_after_stage_or_start();
    }
}

impl FrontendQueryCompiler {
    pub(crate) fn new(
        query: QueryPreparationKernel,
        view: ViewExecutionKernel,
        system_tables: SystemTableQueryKernel,
        mv_readiness: Arc<MvReadinessPort>,
        mv_storage_observation: Arc<dyn MvStorageObservationPort>,
    ) -> Self {
        Self {
            query,
            view,
            system_tables,
            mv_readiness,
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
                        self.mv_readiness.as_ref(),
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
                reject_quarantined_mv_targets(
                    materializer.query_table_bindings().as_ref(),
                    self.mv_readiness.as_ref(),
                )?;
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
                    self.system_tables.mv_readiness().as_ref(),
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
        let bindings = Arc::new(
            QueryTableBindingStore::try_new()
                .expect("query table binding scope allocation must not fail"),
        );
        let materializer = build_catalog_service_provider_with_bindings_and_query_local_overlays(
            current_catalog,
            &catalog_service,
            self.query.connector_control().as_ref(),
            connector_context.clone(),
            Arc::clone(&bindings),
            Vec::new(),
            self.query.catalog_application().map(Arc::as_ref),
        );
        let mv_definitions = if allow_mv_rewrite_candidates {
            Some(freeze_query_mv_rewrite_definition_index(
                &self.query,
                self.mv_readiness.as_ref(),
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
            intent.clone(),
        )?)
        .map_err(FrontendQueryCompilerError::from_compile)?
        .into_pending()
        .map_err(FrontendQueryCompilerError::from_compile)?;
        reject_quarantined_mv_targets(bindings.as_ref(), self.mv_readiness.as_ref())?;
        let statistics = query_statistics_snapshot(&self.query, &materializer, &connector_context)?;
        let distributed_plan =
            SqlCompiler::optimize(SqlOptimizeRequest::new(analyzed, &statistics))
                .map_err(FrontendQueryCompilerError::from_compile)?
                .into_distributed_plan()
                .map_err(FrontendQueryCompilerError::from_compile)?;
        let retry_completion = RetryCompletionTemplate::from_first_round(&completion_intent);
        let (assembly, completion) = prepare_compiled_distributed_query(
            distributed_plan,
            &self.query,
            &materializer,
            &connector_context,
            query_options.clone(),
            execution,
            completion_intent,
        )?;
        // Semantic admission is complete before native request construction.
        // A future topology-only round must reuse these exact bindings or fail
        // closed; it may never materialize a newer `Current` table here.
        materializer
            .query_table_bindings()
            .seal_for_topology_replan();
        let native_bundle = encode_native_fragment_bundle(assembly.encoding().encoding_view())?;
        let request = assembly.finish(native_bundle)?;
        drop(materializer);
        let factory = FrontendDistributedRoundFactory {
            compiler: self.clone(),
            query: query.clone(),
            statement: StatementAdmissionContext::new(
                current_catalog.map(str::to_string),
                current_database.to_string(),
                execution.role(),
                execution.deadline(),
                execution.cancellation().clone(),
                execution.optimizer_settings().clone(),
            ),
            current_catalog: current_catalog.map(str::to_string),
            current_database: current_database.to_string(),
            query_options,
            connector_context,
            catalog_service,
            bindings,
            mv_definitions,
            intent: intent.clone(),
            completion: retry_completion,
            effect_tracker: StatementEffectTracker::read_only(),
        };
        Ok(PreparedQueryOperation::Distributed(
            PreparedQueryDistributedOperation::new(request, completion)
                .with_round_factory(Box::new(factory)),
        ))
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
