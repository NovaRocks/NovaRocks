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

//! The real EXPLAIN path observes provider facts asynchronously and retains
//! the original pure SQL terminal and closed Profile computation creator.

use super::*;
use crate::query_execution::completion_facts::{
    CompletedReadCpuInput, FrontendPlanningCpuSource, LogicalFinishInput, ReadEncodingInput,
    ReadEncodingPurpose,
};
use novarocks_query_application::api::StatementNativeHealthFailure;

pub(crate) struct ExplainPreparationPorts<'a> {
    pub(crate) provider: &'a crate::query_execution::completion_facts::FrontendCompletionCustody,
    pub(crate) cpu: &'a FrontendPlanningCpuSource,
    pub(crate) io: Arc<dyn novarocks_spi::connector::ConnectorIoAdmissionPort>,
    pub(crate) state_store: Option<Arc<dyn novarocks_state_store_api::StateStoreExecutionRequests>>,
}

pub(crate) enum PreparedAsyncExplain {
    Lines(Vec<String>),
    Profile(Box<PreparedAsyncProfile>),
}

pub(crate) struct PreparedAsyncProfile {
    pub(crate) computation:
        Option<crate::query_execution::logical_execution::PreparedLogicalComputation>,
    pub(crate) output: crate::query_execution::internal_consumer::profile::ProfileComputationOutput,
    pub(crate) completion: crate::query_execution::completion::PreparedQueryCompletion,
}

impl FrontendQueryCompiler {
    pub(crate) async fn prepare_explain_async(
        &self,
        explain: &ExplainQuery,
        context: &RequestContext,
        query_options: Option<QueryOptions>,
        scope: &novarocks_workload_control::WorkScope,
        ports: ExplainPreparationPorts<'_>,
    ) -> Result<
        Result<PreparedAsyncExplain, FrontendQueryCompilerError>,
        StatementNativeHealthFailure,
    > {
        let planning_cpu = ports.cpu;
        let execution = context.execution();
        let current_catalog = context.session().current_catalog();
        let current_database = context.session().current_database();
        let metadata: Result<_, FrontendQueryCompilerError> = async {
            let connector_planning_context = connector_planning_context_for_query_on_runtime(
                self.connector_blocking_io.runtime(),
                query_options.as_ref(),
                context.execution().cancellation().clone(),
            )?;
            let connector_planning_context =
                novarocks_spi::connector::ConnectorPlanningContext::try_from_request(
                    connector_planning_context
                        .request()
                        .clone()
                        .with_io_admission(Arc::clone(&ports.io)),
                )
                .map_err(|error| FrontendQueryCompilerError::Engine(error.to_string()))?;
            let connector_context = connector_planning_context.request();
            let mut query = explain.query.as_ref().clone();
            self.view
                .view_service()
                .rewrite_query_async(
                    &crate::view::engine::FrontendViewEngine::new(self.view.clone()),
                    &mut query,
                    ViewRequestContext {
                        current_catalog,
                        current_database,
                        connector_context: Some(connector_context),
                    },
                    execution.sql_semantics(),
                )
                .await?;
            if has_time_travel_refs(&query) {
                crate::query_execution::planning::time_travel::rewrite_time_travel_refs_async(
                    &self.query,
                    current_catalog,
                    current_database,
                    &mut query,
                    connector_context,
                )
                .await?;
            }
            Ok((connector_planning_context, query))
        }
        .await;
        let (connector_planning_context, query) = match metadata {
            Ok(value) => value,
            Err(error) => return Ok(Err(error)),
        };
        let connector_context = connector_planning_context.request();
        let (level, logical_only) = explain_mode(explain);
        if explain.format == ExplainFormat::Analyze {
            return self
                .complete_async_profile(
                    &query,
                    context,
                    Some(query_options_for_explain_analyze(query_options)),
                    &connector_planning_context,
                    scope,
                    std::time::Instant::now(),
                    ports,
                )
                .await;
        }
        if logical_only {
            use novarocks_query_application::preparation::CatalogFactPort;
            let control = sql_compile_control_from_execution(execution);
            let logical = match planning_cpu
                .logical_catalog(
                    query,
                    SqlSessionContext {
                        sql_semantics: execution.sql_semantics().clone(),
                        current_catalog: current_catalog.map(str::to_owned),
                        current_database: current_database.to_owned(),
                        optimizer_settings: execution.optimizer_settings().clone(),
                    },
                    control,
                )
                .await?
            {
                Ok(logical) => logical,
                Err(error) => return Ok(Err(FrontendQueryCompilerError::from_compile(error))),
            };
            let facts: Result<_, FrontendQueryCompilerError> = async {
                let bindings = Arc::new(
                    QueryTableBindingStore::try_new()
                        .map_err(FrontendQueryCompilerError::Engine)?,
                );
                let scope =
                    crate::query_execution::completion_facts::StatementFactScope::governed_read_io_in(
                        bindings,
                        connector_context.clone(),
                        current_catalog,
                        scope,
                        execution.result_capacity().cloned().ok_or_else(|| {
                            FrontendQueryCompilerError::Engine(
                                "logical EXPLAIN requires the admitted result capacity".to_owned(),
                            )
                        })?,
                        ports.state_store.clone(),
                    )
                    .map_err(|error| FrontendQueryCompilerError::Engine(error.to_string()))?;
                let catalog = crate::query_execution::completion_facts::FrontendCatalogFacts::new(
                    self.completion_fact_owners(),
                    scope,
                );
                catalog
                    .resolve_relations(logical.needs())
                    .await
                    .map_err(FrontendQueryCompilerError::Engine)
            }
            .await;
            let facts = match facts {
                Ok(facts) => facts,
                Err(error) => return Ok(Err(error)),
            };
            let functions: Arc<dyn novarocks_sql::compiler::SqlFunctionCatalog> =
                self.functions.clone();
            return Ok(planning_cpu
                .logical_finish(LogicalFinishInput {
                    logical,
                    facts,
                    functions,
                    evaluator: crate::query_execution::constant_eval::constant_evaluator(),
                    policy: self.constant_policy(),
                    environment: SqlPlanningEnvironment::Distributed,
                    level,
                })
                .await?
                .map(PreparedAsyncExplain::Lines)
                .map_err(FrontendQueryCompilerError::from_compile));
        }
        self.complete_async_explain(
            &query,
            current_catalog,
            current_database,
            query_options,
            connector_planning_context,
            execution,
            scope,
            level,
            ports,
        )
        .await
    }

    async fn complete_async_explain(
        &self,
        query: &Query,
        current_catalog: Option<&str>,
        current_database: &str,
        query_options: Option<QueryOptions>,
        connector_planning_context: novarocks_spi::connector::ConnectorPlanningContext,
        execution: &QueryExecutionContext,
        scope: &novarocks_workload_control::WorkScope,
        level: ExplainLevel,
        ports: ExplainPreparationPorts<'_>,
    ) -> Result<
        Result<PreparedAsyncExplain, FrontendQueryCompilerError>,
        StatementNativeHealthFailure,
    > {
        let ExplainPreparationPorts {
            provider: provider_source,
            cpu: planning_cpu,
            state_store,
            ..
        } = ports;
        let request: Result<
            novarocks_sql::compiler::SqlFinalPlanCompileRequest,
            FrontendQueryCompilerError,
        > = (|| {
            let connector_context = connector_planning_context.request();
            let bindings = Arc::new(
                crate::catalog_application::query_bindings::QueryTableBindingStore::try_new()
                    .expect("query table binding scope allocation must not fail"),
            );
            crate::query_execution::completion_facts::install_frontend_fact_source(
                provider_source,
                self.completion_fact_owners(),
                crate::query_execution::completion_facts::StatementFactScope::governed_read_io_in(
                    Arc::clone(&bindings),
                    connector_context.clone(),
                    current_catalog,
                    scope,
                    execution.result_capacity().cloned().ok_or_else(|| {
                        FrontendQueryCompilerError::Engine(
                            "completion facts require the admitted result capacity".to_owned(),
                        )
                    })?,
                    state_store,
                )
                .map_err(|error| FrontendQueryCompilerError::Engine(error.to_string()))?,
                crate::query_execution::compiler::typed_connector_session()
                    .map_err(FrontendQueryCompilerError::Engine)?,
            )
            .map_err(FrontendQueryCompilerError::Engine)?;
            Ok(novarocks_sql::compiler::SqlFinalPlanCompileRequest::new(
                crate::query_execution::physical_encoding::mint_plan_version(),
                SqlStatementInput::parsed_query(Box::new(query.clone())),
                SqlCompileIntent::Explain {
                    level,
                    analyze: false,
                },
                SqlSessionContext {
                    sql_semantics: execution.sql_semantics().clone(),
                    current_catalog: current_catalog.map(str::to_string),
                    current_database: current_database.to_string(),
                    optimizer_settings: execution.optimizer_settings().clone(),
                },
                SqlPlanningEnvironment::Distributed,
                novarocks_sql::compiler::SqlFunctionCatalog::snapshot(self.functions.as_ref()),
                crate::query_execution::constant_eval::constant_evaluator(),
                self.constant_policy(),
                sql_compile_control_from_execution(execution),
                Self::pipeline_dop_domain(query_options.as_ref())?,
                Self::scan_read_budget(query_options.as_ref()),
                novarocks_sql::compiler::DEFAULT_COMPLETION_LIMITS,
            ))
        })();
        let request = match request {
            Ok(request) => request,
            Err(error) => return Ok(Err(error)),
        };
        let control = request.control().clone();
        provider_source
            .complete_in(request, scope, planning_cpu)
            .await?;
        if let Some(error) = provider_source.completed_error() {
            return Ok(Err(FrontendQueryCompilerError::from_completion(&error)));
        }
        if let Err(error) = provider_source.handoff_completed_in(
            planning_cpu,
            &mut Some(CompletedReadCpuInput::ExplainRender {
                budget: novarocks_sql::compiler::ExplainRenderBudget::default(),
                control,
            }),
        ) {
            return Ok(Err(FrontendQueryCompilerError::Engine(error.to_owned())));
        }
        Ok(planning_cpu
            .explain_render()
            .await?
            .map(PreparedAsyncExplain::Lines)
            .map_err(|error| FrontendQueryCompilerError::from_completion(&error)))
    }

    async fn complete_async_profile(
        &self,
        query: &Query,
        context: &RequestContext,
        query_options: Option<QueryOptions>,
        connector_planning_context: &novarocks_spi::connector::ConnectorPlanningContext,
        scope: &novarocks_workload_control::WorkScope,
        planning_started_at: std::time::Instant,
        ports: ExplainPreparationPorts<'_>,
    ) -> Result<
        Result<PreparedAsyncExplain, FrontendQueryCompilerError>,
        StatementNativeHealthFailure,
    > {
        let execution = context.execution();
        let current_catalog = context.session().current_catalog();
        let current_database = context.session().current_database();
        let family = *context.session().presentation();
        let ExplainPreparationPorts {
            provider: provider_source,
            cpu: planning_cpu,
            state_store,
            ..
        } = ports;
        let request: Result<
            (
                novarocks_sql::compiler::SqlFinalPlanCompileRequest,
                novarocks_physical_plan::PlanVersionId,
            ),
            FrontendQueryCompilerError,
        > = (|| {
            let bindings = Arc::new(
                QueryTableBindingStore::try_new()
                    .expect("query table binding scope allocation must not fail"),
            );
            crate::query_execution::completion_facts::install_frontend_fact_source(
                provider_source,
                self.completion_fact_owners(),
                crate::query_execution::completion_facts::StatementFactScope::governed_read_io_in(
                    Arc::clone(&bindings),
                    connector_planning_context.request().clone(),
                    current_catalog,
                    scope,
                    execution.result_capacity().cloned().ok_or_else(|| {
                        FrontendQueryCompilerError::Engine(
                            "completion facts require the admitted result capacity".to_owned(),
                        )
                    })?,
                    state_store,
                )
                .map_err(|error| FrontendQueryCompilerError::Engine(error.to_string()))?,
                crate::query_execution::compiler::typed_connector_session()
                    .map_err(FrontendQueryCompilerError::Engine)?,
            )
            .map_err(FrontendQueryCompilerError::Engine)?;
            let version = crate::query_execution::physical_encoding::mint_plan_version();
            Ok((
                novarocks_sql::compiler::SqlFinalPlanCompileRequest::new(
                    version,
                    SqlStatementInput::parsed_query(Box::new(query.clone())),
                    SqlCompileIntent::Explain {
                        level: ExplainLevel::Analyze,
                        analyze: true,
                    },
                    SqlSessionContext {
                        sql_semantics: execution.sql_semantics().clone(),
                        current_catalog: current_catalog.map(str::to_string),
                        current_database: current_database.to_string(),
                        optimizer_settings: execution.optimizer_settings().clone(),
                    },
                    SqlPlanningEnvironment::Distributed,
                    novarocks_sql::compiler::SqlFunctionCatalog::snapshot(self.functions.as_ref()),
                    crate::query_execution::constant_eval::constant_evaluator(),
                    self.constant_policy(),
                    sql_compile_control_from_execution(execution),
                    Self::pipeline_dop_domain(query_options.as_ref())?,
                    Self::scan_read_budget(query_options.as_ref()),
                    novarocks_sql::compiler::DEFAULT_COMPLETION_LIMITS,
                ),
                version,
            ))
        })();
        let (request, version) = match request {
            Ok(request) => request,
            Err(error) => return Ok(Err(error)),
        };
        let encoding_control = request.control().clone();
        provider_source
            .complete_in(request, scope, planning_cpu)
            .await?;
        if let Some(error) = provider_source.completed_error() {
            return Ok(Err(FrontendQueryCompilerError::from_completion(&error)));
        }
        if let Err(error) = provider_source.handoff_completed_in(
            planning_cpu,
            &mut Some(CompletedReadCpuInput::Encoding(Box::new(
                ReadEncodingInput {
                    purpose: ReadEncodingPurpose::Profile(planning_started_at),
                    version,
                    options: query_options,
                    family,
                    offset: 0,
                    carrier: self.query.static_plan_carrier().clone(),
                    policy: self.constant_policy(),
                    control: encoding_control,
                    #[cfg(novarocks_dev_controls)]
                    rewrite: None,
                },
            ))),
        ) {
            return Ok(Err(FrontendQueryCompilerError::Engine(error.to_owned())));
        }
        if let Err(error) = planning_cpu.prepare_profile_in().await? {
            return Ok(Err(error));
        }
        let mut profile = None;
        match planning_cpu.take_profile_in(&mut profile) {
            Ok(()) => Ok(Ok(PreparedAsyncExplain::Profile(
                profile.expect("same original Profile receiving output"),
            ))),
            Err(error) => Ok(Err(FrontendQueryCompilerError::Engine(error.to_owned()))),
        }
    }
}
