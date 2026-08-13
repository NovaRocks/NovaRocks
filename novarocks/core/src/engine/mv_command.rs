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

//! Closed typed executor for Iceberg MV statements.

use std::sync::Arc;

use crate::engine::StatementResult;
use crate::engine::domain::MvExecutionKernel;
use crate::mv::application::MvApplicationService;
use crate::query_execution::request_context::QueryExecutionContext;

#[derive(Clone)]
pub struct MvCommandExecutor {
    kernel: MvExecutionKernel,
}

impl MvCommandExecutor {
    pub(crate) fn new(kernel: MvExecutionKernel) -> Self {
        Self { kernel }
    }

    /// Execute exactly one MV statement through explicit ports. Refresh and
    /// repartition receive the request's already-admitted execution context;
    /// they never capture a second topology or cancellation scope.
    pub fn try_execute(
        &self,
        sql: &str,
        current_catalog: Option<&str>,
        current_database: &str,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
        execution: &QueryExecutionContext,
    ) -> Result<Option<StatementResult>, String> {
        let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)?;
        if crate::sql::parser::procedure::looks_like_call_procedure(&normalized) {
            let statement = crate::sql::parser::procedure::parse_call_procedure_sql(&normalized)?;
            if statement.procedure == crate::engine::mv::stateless_rebuild::PROCEDURE_NAME {
                return crate::engine::mv::stateless_rebuild::execute_novarocks_imv_stateless_rebuild(
                    self.kernel.connector_control().as_ref(),
                    self.kernel.storage_observation().as_ref(),
                    self.kernel.repository().as_ref(),
                    &statement,
                    current_database,
                    connector_context.clone(),
                )
                .map(Some);
            }
        }
        let mut statements = match crate::sql::parser::parse_sql(&normalized) {
            Ok(statements) => statements,
            Err(_) => return Ok(None),
        };
        if statements.len() != 1 {
            return Err("MV command accepts exactly one statement".to_string());
        }
        match statements.pop().expect("one checked statement") {
            crate::sql::parser::ast::Statement::CreateMaterializedView(statement) => {
                crate::engine::mv_flow::create_mv_with_kernel(
                    &self.kernel,
                    current_catalog,
                    current_database,
                    &statement,
                    connector_context,
                )
                .map(Some)
            }
            crate::sql::parser::ast::Statement::DropMaterializedView(statement) => {
                crate::engine::mv_flow::drop_mv_with_kernel(
                    &self.kernel,
                    current_catalog,
                    current_database,
                    &statement,
                    connector_context,
                )
                .map(Some)
            }
            crate::sql::parser::ast::Statement::AlterMaterializedView(statement)
                if !matches!(
                    &statement.action,
                    crate::sql::parser::ast::AlterMaterializedViewAction::Repartition(_)
                ) =>
            {
                crate::engine::mv_flow::alter_mv_with_kernel(
                    &self.kernel,
                    current_catalog,
                    current_database,
                    &statement,
                    connector_context,
                )
                .map(Some)
            }
            crate::sql::parser::ast::Statement::AlterMaterializedView(statement) => self
                .execute_repartition(
                    current_catalog,
                    current_database,
                    &statement,
                    connector_context,
                    execution,
                )
                .map(Some),
            crate::sql::parser::ast::Statement::RefreshMaterializedView(statement) => self
                .execute_refresh(
                    current_catalog,
                    current_database,
                    &statement,
                    connector_context,
                    execution,
                )
                .map(Some),
            crate::sql::parser::ast::Statement::ShowMaterializedViews(statement) => {
                crate::engine::mv_flow::list_mvs_with_kernel(
                    &self.kernel,
                    current_catalog,
                    &statement,
                )
                .map(Some)
            }
            _ => Ok(None),
        }
    }

    fn ports(&self) -> crate::engine::mv::iceberg_refresh::IcebergMvCorePorts {
        crate::engine::mv::iceberg_refresh::IcebergMvCorePorts::new(
            Arc::clone(self.kernel.catalog_service()),
            self.kernel.catalog_application().cloned(),
            Arc::clone(self.kernel.connector_control()),
            Arc::clone(self.kernel.repository()),
            Arc::clone(self.kernel.storage_observation()),
        )
    }

    fn execute_repartition(
        &self,
        current_catalog: Option<&str>,
        current_database: &str,
        statement: &crate::sql::parser::ast::AlterMaterializedViewStmt,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
        execution: &QueryExecutionContext,
    ) -> Result<StatementResult, String> {
        let crate::sql::parser::ast::AlterMaterializedViewAction::Repartition(fields) =
            &statement.action
        else {
            return Err("MV repartition executor received a non-repartition action".to_string());
        };
        let target = crate::engine::mv::iceberg_refresh::resolve_refresh_target(
            current_catalog,
            current_database,
            &statement.name,
        )?;
        let target = crate::mv::repository::MvTarget {
            catalog: Some(target.catalog),
            database: target.namespace,
            name: target.table,
        };
        let refresh_statement = crate::sql::parser::ast::RefreshMaterializedViewStmt {
            name: statement.name.clone(),
            full: false,
        };
        let ports = self.ports();
        let preparation =
            crate::engine::mv::iceberg_refresh::StandaloneMvRefreshPreparationService::new_repartition_with_ports(
                &ports,
                current_catalog,
                current_database,
                &refresh_statement,
                fields,
                connector_context,
            );
        self.kernel
            .application()
            .prepare_and_execute_refresh(
                &preparation,
                crate::mv::application::MvApplicationStatement::Refresh(
                    crate::sql::mv_refresh::MvRefreshStatement::from(&refresh_statement),
                ),
                target,
                connector_context.clone(),
                execution,
            )
            .map(statement_result)
            .map_err(|error| error.to_string())
    }

    fn execute_refresh(
        &self,
        current_catalog: Option<&str>,
        current_database: &str,
        statement: &crate::sql::parser::ast::RefreshMaterializedViewStmt,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
        execution: &QueryExecutionContext,
    ) -> Result<StatementResult, String> {
        let refresh_statement = crate::sql::mv_refresh::MvRefreshStatement::from(statement);
        refresh_statement.validate_supported()?;
        let target = crate::engine::mv::iceberg_refresh::resolve_refresh_target(
            current_catalog,
            current_database,
            &statement.name,
        )?;
        let requested_object = crate::mv::dependency::model::iceberg_mv_dependency_ref(
            &target.catalog,
            &target.namespace,
            &target.table,
        );
        let steps = crate::engine::mv::dependency::build_upstream_refresh_steps_with_repository(
            self.kernel.repository().as_ref(),
            &requested_object,
        )?;
        let mut last_result = None;
        for step in steps {
            if step.storage_engine != crate::mv::model::MvStorageEngine::Iceberg {
                return Err(format!(
                    "REFRESH MATERIALIZED VIEW is only supported for Iceberg-backed materialized views: {}",
                    step.object.display_name().trim_start_matches("mv:")
                ));
            }
            let target = step.target;
            let target_catalog = target.catalog.clone();
            let target_database = target.database.clone();
            let target_name = target.name.clone();
            let step_statement = crate::sql::parser::ast::RefreshMaterializedViewStmt {
                name: crate::sql::parser::ast::ObjectName {
                    parts: vec![target_database.clone(), target_name],
                },
                full: false,
            };
            let ports = self.ports();
            let preparation =
                crate::engine::mv::iceberg_refresh::StandaloneMvRefreshPreparationService::new_with_ports(
                    &ports,
                    target_catalog.as_deref(),
                    &target_database,
                    &step_statement,
                    connector_context,
                );
            last_result = Some(
                self.kernel
                    .application()
                    .prepare_and_execute_refresh(
                        &preparation,
                        crate::mv::application::MvApplicationStatement::Refresh(
                            crate::sql::mv_refresh::MvRefreshStatement::from(&step_statement),
                        ),
                        target,
                        connector_context.clone(),
                        execution,
                    )
                    .map(statement_result)
                    .map_err(|error| error.to_string())?,
            );
        }
        last_result.ok_or_else(|| "MV refresh dependency planner returned no steps".to_string())
    }
}

fn statement_result(result: crate::mv::application::MvStatementResult) -> StatementResult {
    match result {
        crate::mv::application::MvStatementResult::Ok => StatementResult::Ok,
        crate::mv::application::MvStatementResult::Query(result) => StatementResult::Query(result),
    }
}
