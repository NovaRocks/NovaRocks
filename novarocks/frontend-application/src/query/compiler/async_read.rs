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

//! Async ordinary Read completion in the original statement source.
use super::*;

use crate::query_execution::completion_facts::{
    CompletedReadCpuInput, FrontendCompletionPorts, FrontendPlanningCpuSource, ReadEncodingInput,
    ReadEncodingPurpose,
};
use novarocks_query_application::api::StatementNativeHealthFailure;

struct ReadSqlInput {
    query: Query,
    current_catalog: Option<String>,
    current_database: String,
    family: novarocks_result_contract::ClientPresentationFamily,
}
struct ReadDistributedMetadata {
    query: Query,
    planning: novarocks_spi::connector::ConnectorPlanningContext,
}
enum ReadMetadataPrepared {
    Immediate(Box<PreparedQueryOperation>),
    Distributed(Box<ReadDistributedMetadata>),
}

impl FrontendQueryCompiler {
    pub(crate) fn new_planning_cpu_source(&self) -> Arc<FrontendPlanningCpuSource> {
        FrontendPlanningCpuSource::new(self.connector_blocking_io.clone())
    }

    pub(crate) async fn prepare_read_async(
        &self,
        statement: &Statement,
        purpose: FrontendQueryPurpose,
        context: &RequestContext,
        query_options: Option<QueryOptions>,
        scope: &novarocks_workload_control::WorkScope,
        ports: FrontendCompletionPorts<'_>,
        io: Arc<dyn novarocks_spi::connector::ConnectorIoAdmissionPort>,
        state_store: Option<Arc<dyn novarocks_state_store_api::StateStoreExecutionRequests>>,
        #[cfg(novarocks_dev_controls)] rewrite_statement: Option<
            crate::mv::test_controls::MvRewriteStatementTarget,
        >,
    ) -> Result<
        Result<PreparedQueryOperation, FrontendQueryCompilerError>,
        StatementNativeHealthFailure,
    > {
        let metadata: Result<ReadMetadataPrepared, FrontendQueryCompilerError> = async {
        purpose.validate(statement)?;
        let Statement::Query(original) = statement else {
            return Err(FrontendQueryCompilerError::Engine(
                "read preparation requires a query".into(),
            ));
        };
        if information_schema::is_local_materialized_views_query(original) {
            let requests = state_store.as_ref().ok_or_else(|| {
                FrontendQueryCompilerError::Application(
                    novarocks_query_application::session_error::QueryServiceError::from_failure_cause(
                        novarocks_failure_contract::SharedFailureCause::new(
                            novarocks_state_store_api::StateStoreOriginalUnsupported,
                        ),
                    ),
                )
            })?;
            if let Some(result) = information_schema::try_query_materialized_views_in(
                self.system_tables.mv_readiness().as_ref(),
                original,
                Arc::clone(requests),
            )
            .await
            .map_err(FrontendQueryCompilerError::Application)?
            {
                return Ok(ReadMetadataPrepared::Immediate(Box::new(PreparedQueryOperation::immediate(result))));
            }
        }
        let planning = connector_planning_context_for_query_on_runtime(
            self.connector_blocking_io.runtime(),
            query_options.as_ref(),
            context.execution().cancellation().clone(),
        )?;
        let planning = novarocks_spi::connector::ConnectorPlanningContext::try_from_request(
            planning.request().clone().with_io_admission(io),
        )
        .map_err(|e| FrontendQueryCompilerError::Engine(e.to_string()))?;
        let connector_context = planning.request();
        let current_catalog = context.session().current_catalog();
        let current_database = context.session().current_database();
        let mut query = original.clone();
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
                context.execution().sql_semantics(),
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
        novarocks_query_application::system_catalog_rewrite::rewrite_query_async(
            self.system_tables.facts_port().as_ref(),
            self.system_tables.system_catalog().as_ref(),
            connector_context,
            &mut query,
        )
        .await?;
            Ok(ReadMetadataPrepared::Distributed(Box::new(ReadDistributedMetadata { query, planning })))
        }.await;
        let (query, planning) = match metadata {
            Err(error) => return Ok(Err(error)),
            Ok(ReadMetadataPrepared::Immediate(operation)) => return Ok(Ok(*operation)),
            Ok(ReadMetadataPrepared::Distributed(prepared)) => {
                let ReadDistributedMetadata { query, planning } = *prepared;
                (query, planning)
            }
        };
        self.complete_async_read(
            purpose,
            ReadSqlInput {
                query,
                current_catalog: context.session().current_catalog().map(str::to_owned),
                current_database: context.session().current_database().to_owned(),
                family: *context.session().presentation(),
            },
            query_options,
            planning,
            context.execution(),
            ports,
            scope,
            state_store,
            #[cfg(novarocks_dev_controls)]
            rewrite_statement,
        )
        .await
    }
    async fn complete_async_read(
        &self,
        purpose: FrontendQueryPurpose,
        sql: ReadSqlInput,
        query_options: Option<QueryOptions>,
        connector_planning_context: novarocks_spi::connector::ConnectorPlanningContext,
        execution: &QueryExecutionContext,
        ports: FrontendCompletionPorts<'_>,
        scope: &novarocks_workload_control::WorkScope,
        state_store: Option<Arc<dyn novarocks_state_store_api::StateStoreExecutionRequests>>,
        #[cfg(novarocks_dev_controls)] rewrite_statement: Option<
            crate::mv::test_controls::MvRewriteStatementTarget,
        >,
    ) -> Result<
        Result<PreparedQueryOperation, FrontendQueryCompilerError>,
        StatementNativeHealthFailure,
    > {
        let FrontendCompletionPorts {
            provider: provider_source,
            cpu: planning_cpu,
        } = ports;
        let ReadSqlInput {
            query,
            current_catalog,
            current_database,
            family,
        } = sql;
        let request: Result<
            (
                novarocks_sql::compiler::SqlFinalPlanCompileRequest,
                novarocks_physical_plan::PlanVersionId,
            ),
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
                    current_catalog.as_deref(),
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
                    SqlCompileIntent::Query,
                    SqlSessionContext {
                        sql_semantics: execution.sql_semantics().clone(),
                        current_catalog: current_catalog.clone(),
                        current_database: current_database.clone(),
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
        #[cfg(novarocks_dev_controls)]
        let rewrite = self.mv_readiness.test_controls_owner().map(|controls| {
            crate::query_execution::completion_facts::ReadRewriteInput {
                controls,
                catalog: current_catalog,
                database: current_database,
                query,
                target: rewrite_statement,
                cancellation: execution.cancellation().clone(),
            }
        });
        if let Err(error) = provider_source.handoff_completed_in(
            planning_cpu,
            &mut Some(CompletedReadCpuInput::Encoding(Box::new(
                ReadEncodingInput {
                    purpose: ReadEncodingPurpose::Read(purpose),
                    version,
                    options: query_options,
                    family,
                    offset: match family {
                        novarocks_result_contract::ClientPresentationFamily::MysqlText => {
                            chrono::Local::now().offset().local_minus_utc()
                        }
                        novarocks_result_contract::ClientPresentationFamily::TrinoJson(_) => 0,
                    },
                    carrier: self.query.static_plan_carrier().clone(),
                    policy: self.constant_policy(),
                    control: encoding_control,
                    #[cfg(novarocks_dev_controls)]
                    rewrite,
                },
            ))),
        ) {
            return Ok(Err(FrontendQueryCompilerError::Engine(error.to_owned())));
        }
        if let Err(error) = planning_cpu.prepare_read_in().await? {
            return Ok(Err(error));
        }
        let mut operation = None;
        match planning_cpu.take_read_in(&mut operation) {
            Ok(()) => Ok(Ok(operation.expect("same original prepared Read"))),
            Err(error) => Ok(Err(FrontendQueryCompilerError::Engine(error.to_owned()))),
        }
    }
}
