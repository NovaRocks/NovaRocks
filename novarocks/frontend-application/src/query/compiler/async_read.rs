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

pub(crate) enum ReadCompilerInput {
    Query(Box<Query>),
    Show(Box<crate::query_execution::completion_facts::ShowInput>),
}

pub(crate) struct ReadStatementInput {
    pub(crate) query: ReadCompilerInput,
    pub(crate) principal: Option<Arc<str>>,
    pub(crate) timing: Option<novarocks_query_application::session_control::ClientQueryTiming>,
}

struct ReadSqlInput {
    statement: ReadStatementInput,
    current_catalog: Option<String>,
    current_database: String,
    family: Option<novarocks_result_contract::ClientPresentationFamily>,
}
impl FrontendQueryCompiler {
    pub(crate) fn new_planning_cpu_source(&self) -> Arc<FrontendPlanningCpuSource> {
        FrontendPlanningCpuSource::new(self.connector_blocking_io.clone())
    }

    pub(crate) async fn prepare_read_async(
        &self,
        statement: ReadStatementInput,
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
        let planning = match self.connector_planning_context(
            context.execution(),
            query_options.as_ref(),
            io,
        ) {
            Err(error) => return Ok(Err(error)),
            Ok(planning) => planning,
        };
        self.complete_async_read(
            purpose,
            ReadSqlInput {
                statement,
                current_catalog: context.session().current_catalog().map(str::to_owned),
                current_database: context.session().current_database().to_owned(),
                family: context.session().presentation().copied(),
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
            statement:
                ReadStatementInput {
                    query,
                    principal,
                    timing,
                },
            current_catalog,
            current_database,
            family,
        } = sql;
        let bindings = match QueryTableBindingStore::try_new() {
            Ok(bindings) => Arc::new(bindings),
            Err(error) => return Ok(Err(FrontendQueryCompilerError::Engine(error))),
        };
        let install = (|| {
            let connector_context = connector_planning_context.request();
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
                .map_err(|error| FrontendQueryCompilerError::Engine(error.to_string()))?
                .with_snapshot_planning(
                    sql_compile_control_from_execution(execution),
                    execution.cancellation().clone(),
                    Arc::clone(planning_cpu),
                )
                .with_query_observer_principal(principal),
                crate::query_execution::compiler::typed_connector_session()
                    .map_err(FrontendQueryCompilerError::Engine)?,
                Arc::clone(execution.session_property_inputs()),
            )
            .map_err(FrontendQueryCompilerError::Engine)
        })();
        if let Err(error) = install {
            return Ok(Err(error));
        }
        let (query, statement) = match query {
            ReadCompilerInput::Query(query) => {
                let statement = SqlStatementInput::query_with_admitted_session(query.clone());
                (query, statement)
            }
            ReadCompilerInput::Show(show) => {
                let prepared = provider_source
                    .prepare_show(
                        *show,
                        self.sealed_functions(),
                        execution.session_property_inputs(),
                    )
                    .await;
                let prepared = match prepared {
                    Ok(prepared) => prepared,
                    Err(error) => return Ok(Err(error)),
                };
                let statement = match prepared.private_relation {
                    Some((kind, declaration)) => {
                        match SqlStatementInput::query_with_statement_system_relation(
                            prepared.query.clone(),
                            kind,
                            declaration,
                        ) {
                            Ok(statement) => statement,
                            Err(error) => {
                                return Ok(Err(FrontendQueryCompilerError::from_compile(error)));
                            }
                        }
                    }
                    None => SqlStatementInput::query_with_admitted_session(prepared.query.clone()),
                };
                (prepared.query, statement)
            }
        };
        let pipeline_dop = match Self::pipeline_dop_domain(query_options.as_ref()) {
            Ok(dop) => dop,
            Err(error) => return Ok(Err(error)),
        };
        let version = crate::query_execution::physical_encoding::mint_plan_version();
        let request = novarocks_sql::compiler::SqlFinalPlanCompileRequest::new(
            version,
            statement,
            SqlCompileIntent::Query,
            SqlSessionContext {
                sql_semantics: execution.sql_semantics().clone(),
                current_catalog: current_catalog.clone(),
                current_database: current_database.clone(),
                optimizer_settings: execution.optimizer_settings().clone(),
            },
            SqlPlanningEnvironment::Distributed,
            novarocks_sql::compiler::SqlFunctionCatalog::snapshot(self.functions.as_ref()),
            self.sealed_functions(),
            self.constant_policy(),
            observe_plan_timing(sql_compile_control_from_execution(execution), timing),
            pipeline_dop,
            novarocks_sql::compiler::DEFAULT_COMPLETION_LIMITS,
        );
        #[cfg(not(novarocks_dev_controls))]
        drop(query);
        let encoding_control = request.control().clone();
        planning_cpu.install_catalog_qualification_source(
            bindings,
            self.query.catalog_application().cloned(),
        );
        provider_source
            .complete_in(request, scope, planning_cpu.as_ref())
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
                query: *query,
                target: rewrite_statement,
                cancellation: execution.cancellation().clone(),
            }
        });
        if let Err(error) = provider_source.handoff_completed_in(
            planning_cpu,
            &mut Some(CompletedReadCpuInput::Encoding(Box::new(
                ReadEncodingInput {
                    capacity: execution
                        .result_capacity()
                        .cloned()
                        .expect("validated original statement capacity"),
                    purpose: ReadEncodingPurpose::Read(purpose),
                    version,
                    options: query_options,
                    family,
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
