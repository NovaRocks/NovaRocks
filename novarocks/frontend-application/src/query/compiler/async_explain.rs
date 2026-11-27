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
    pub(crate) cpu: &'a Arc<FrontendPlanningCpuSource>,
    pub(crate) io: Arc<dyn novarocks_spi::connector::ConnectorIoAdmissionPort>,
    pub(crate) state_store: Option<Arc<dyn novarocks_state_store_api::StateStoreExecutionRequests>>,
    pub(crate) timing: Option<novarocks_query_application::session_control::ClientQueryTiming>,
    pub(crate) query_local_overlays:
        Vec<crate::catalog_application::query_materializer::QueryLocalTableOverlay>,
    pub(crate) original_context: Option<novarocks_spi::connector::ConnectorPlanningContext>,
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
    pub(in crate::query) async fn prepare_explain_async(
        &self,
        explain: ExplainInput,
        detail: novarocks_query_application::sql::session::ExplainDetail,
        context: &RequestContext,
        query_options: Option<QueryOptions>,
        scope: &novarocks_workload_control::WorkScope,
        mut ports: ExplainPreparationPorts<'_>,
    ) -> Result<
        Result<PreparedAsyncExplain, FrontendQueryCompilerError>,
        StatementNativeHealthFailure,
    > {
        let planning_cpu = ports.cpu;
        let execution = context.execution();
        let current_catalog = context.session().current_catalog();
        let current_database = context.session().current_database();
        let connector_planning_context = if let Some(original) = ports.original_context.take() {
            original
        } else {
            match self.connector_planning_context(
                execution,
                query_options.as_ref(),
                Arc::clone(&ports.io),
            ) {
                Ok(value) => value,
                Err(error) => return Ok(Err(error)),
            }
        };
        let connector_context = connector_planning_context.request();
        let analyze = explain.is_analyze();
        let (level, logical_only) = explain.display(detail);
        let query = *explain.query;
        if analyze {
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
            let control =
                observe_plan_timing(sql_compile_control_from_execution(execution), ports.timing);
            let bindings = match QueryTableBindingStore::try_new() {
                Ok(bindings) => Arc::new(bindings),
                Err(error) => return Ok(Err(FrontendQueryCompilerError::Engine(error))),
            };
            planning_cpu.install_catalog_qualification_source(
                Arc::clone(&bindings),
                self.query.catalog_application().cloned(),
            );
            let mut logical = match planning_cpu
                .logical_catalog_for_input(
                    execution.sql_statement_input(Box::new(query), true),
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
            let catalog: Result<_, FrontendQueryCompilerError> = async {
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
                    scope
                        .with_query_local_overlays(ports.query_local_overlays)
                        .with_snapshot_planning(
                            sql_compile_control_from_execution(execution),
                            execution.cancellation().clone(),
                            Arc::clone(planning_cpu),
                        ),
                );
                Ok(catalog)
            }
            .await;
            let catalog = match catalog {
                Ok(catalog) => catalog,
                Err(error) => return Ok(Err(error)),
            };
            while !logical.needs().is_empty() {
                let facts = match catalog.resolve_relations(logical.needs()).await {
                    Ok(facts) => facts,
                    Err(error) => return Ok(Err(FrontendQueryCompilerError::from_compile(error))),
                };
                logical = match planning_cpu
                    .logical_continue(logical, facts.into_boxed_slice())
                    .await?
                {
                    Ok(logical) => logical,
                    Err(error) => return Ok(Err(FrontendQueryCompilerError::from_compile(error))),
                };
            }
            let functions: Arc<dyn novarocks_sql::compiler::SqlFunctionCatalog> =
                self.functions.clone();
            let Some(table_argument_capacity) = execution.result_capacity().cloned() else {
                return Ok(Err(FrontendQueryCompilerError::Engine(
                    "logical EXPLAIN requires the admitted result capacity".into(),
                )));
            };
            return Ok(planning_cpu
                .logical_finish(LogicalFinishInput {
                    logical,
                    table_argument_capacity,
                    required_constants: Arc::new(crate::RequiredConstantInterpreter::new(
                        self.sealed_functions(),
                        self.constant_policy(),
                    )),
                    functions,
                    sealed_functions: self.sealed_functions(),
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
            timing,
            query_local_overlays,
            ..
        } = ports;
        let bindings = match QueryTableBindingStore::try_new() {
            Ok(bindings) => Arc::new(bindings),
            Err(error) => return Ok(Err(FrontendQueryCompilerError::Engine(error))),
        };
        let request: Result<
            novarocks_sql::compiler::SqlFinalPlanCompileRequest,
            FrontendQueryCompilerError,
        > = (|| {
            let connector_context = connector_planning_context.request();
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
                .map_err(|error| FrontendQueryCompilerError::Engine(error.to_string()))?
                .with_query_local_overlays(query_local_overlays)
                .with_snapshot_planning(
                    sql_compile_control_from_execution(execution),
                    execution.cancellation().clone(),
                    Arc::clone(planning_cpu),
                ),
                crate::query_execution::compiler::typed_connector_session()
                    .map_err(FrontendQueryCompilerError::Engine)?,
                Arc::clone(execution.session_property_inputs()),
            )
            .map_err(FrontendQueryCompilerError::Engine)?;
            Ok(novarocks_sql::compiler::SqlFinalPlanCompileRequest::new(
                crate::query_execution::physical_encoding::mint_plan_version(),
                execution.sql_statement_input(Box::new(query.clone()), true),
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
                self.sealed_functions(),
                self.constant_policy(),
                observe_plan_timing(sql_compile_control_from_execution(execution), timing),
                Self::pipeline_dop_domain(query_options.as_ref())?,
                novarocks_sql::compiler::DEFAULT_COMPLETION_LIMITS,
            ))
        })();
        let request = match request {
            Ok(request) => request,
            Err(error) => return Ok(Err(error)),
        };
        let control = request.control().clone();
        planning_cpu.install_catalog_qualification_source(
            Arc::clone(&bindings),
            self.query.catalog_application().cloned(),
        );
        provider_source
            .complete_in(request, scope, planning_cpu.as_ref(), &bindings)
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
        let family = context.session().presentation().copied();
        let ExplainPreparationPorts {
            provider: provider_source,
            cpu: planning_cpu,
            state_store,
            timing,
            ..
        } = ports;
        let bindings = match QueryTableBindingStore::try_new() {
            Ok(bindings) => Arc::new(bindings),
            Err(error) => return Ok(Err(FrontendQueryCompilerError::Engine(error))),
        };
        let request: Result<
            (
                novarocks_sql::compiler::SqlFinalPlanCompileRequest,
                novarocks_physical_plan::PlanVersionId,
            ),
            FrontendQueryCompilerError,
        > = (|| {
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
                .map_err(|error| FrontendQueryCompilerError::Engine(error.to_string()))?
                .with_snapshot_planning(
                    sql_compile_control_from_execution(execution),
                    execution.cancellation().clone(),
                    Arc::clone(planning_cpu),
                ),
                crate::query_execution::compiler::typed_connector_session()
                    .map_err(FrontendQueryCompilerError::Engine)?,
                Arc::clone(execution.session_property_inputs()),
            )
            .map_err(FrontendQueryCompilerError::Engine)?;
            let version = crate::query_execution::physical_encoding::mint_plan_version();
            Ok((
                novarocks_sql::compiler::SqlFinalPlanCompileRequest::new(
                    version,
                    execution.sql_statement_input(Box::new(query.clone()), true),
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
                    self.sealed_functions(),
                    self.constant_policy(),
                    observe_plan_timing(sql_compile_control_from_execution(execution), timing),
                    Self::pipeline_dop_domain(query_options.as_ref())?,
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
        planning_cpu.install_catalog_qualification_source(
            Arc::clone(&bindings),
            self.query.catalog_application().cloned(),
        );
        provider_source
            .complete_in(request, scope, planning_cpu.as_ref(), &bindings)
            .await?;
        if let Some(error) = provider_source.completed_error() {
            return Ok(Err(FrontendQueryCompilerError::from_completion(&error)));
        }
        if let Err(error) = provider_source.handoff_completed_in(
            planning_cpu,
            &mut Some(CompletedReadCpuInput::Encoding(Box::new(
                ReadEncodingInput {
                    capacity: execution
                        .result_capacity()
                        .cloned()
                        .expect("validated original statement capacity"),
                    purpose: ReadEncodingPurpose::Profile(planning_started_at),
                    version,
                    options: query_options,
                    family,
                    carrier: self.query.static_plan_carrier().clone(),
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
