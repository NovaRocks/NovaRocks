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

use crate::common::admitted_query_context::QueryExecutionContext;
use crate::mv::domain::application::{MvApplicationService, MvStatementResult};
use crate::mv::domain::iceberg_backend::IcebergMvBackend;
use crate::mv::domain::iceberg_refresh::IcebergMvCorePorts;
use crate::mv::domain::repository::MvRepository;
use crate::runtime::statement_result::StatementResult;
use novarocks_spi::connector::MvStorageObservationPort;
use novarocks_sql::syntax::{
    AlterMaterializedViewAction, AlterMaterializedViewStmt, MvAdmittedStatement, ObjectName,
    RefreshMaterializedViewStmt, parse_call_procedure_sql, parse_mv_admitted_statement,
};

use super::FrontendMvService;
use crate::mv::domain::refresh::resolve_refresh_mv_target;
use crate::mv::domain::{
    PROCEDURE_NAME, alter_mv_with_ports, create_mv_with_ports, drop_mv_with_ports,
    execute_novarocks_imv_stateless_rebuild, list_mvs_with_backend,
};
use crate::runtime::query_result::build_string_query_result;
use std::sync::Arc;

#[derive(Clone)]
pub struct MvCommandExecutor {
    ports: IcebergMvCorePorts,
    create_application: Arc<dyn MvApplicationService>,
    refresh_service: Arc<FrontendMvService>,
    repository: Arc<dyn MvRepository>,
    storage_observation: Arc<dyn MvStorageObservationPort>,
    mv_backend: Arc<IcebergMvBackend>,
}

impl MvCommandExecutor {
    pub fn new(
        ports: IcebergMvCorePorts,
        create_application: Arc<dyn MvApplicationService>,
        refresh_service: Arc<FrontendMvService>,
        repository: Arc<dyn MvRepository>,
        storage_observation: Arc<dyn MvStorageObservationPort>,
        mv_backend: Arc<IcebergMvBackend>,
    ) -> Self {
        Self {
            ports,
            create_application,
            refresh_service,
            repository,
            storage_observation,
            mv_backend,
        }
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
        let normalized = novarocks_sql::syntax::normalize_for_raw_parse(sql)?;
        if let Some(parsed) = parse_explain_refresh_materialized_view(&normalized) {
            let (statement, level, analyze) = parsed?;
            if analyze {
                return Err(
                    "EXPLAIN ANALYZE REFRESH MATERIALIZED VIEW is not supported".to_string()
                );
            }
            let lines = crate::query_execution::mv_assembly::refresh_explain::explain_iceberg_mv_refresh_rewrite_plan_with_ports(
                &self.ports,
                current_catalog,
                current_database,
                &statement,
                level,
                connector_context,
            )?;
            return build_string_query_result("Explain String", lines)
                .map(StatementResult::Query)
                .map(Some);
        }
        if novarocks_sql::syntax::looks_like_call_procedure(&normalized) {
            let statement = parse_call_procedure_sql(&normalized)?;
            if statement.procedure == PROCEDURE_NAME {
                return execute_novarocks_imv_stateless_rebuild(
                    self.ports.connector_control(),
                    self.storage_observation.as_ref(),
                    self.repository.as_ref(),
                    &statement,
                    current_database,
                    connector_context.clone(),
                )
                .map(Some);
            }
        }
        let statement = match parse_mv_admitted_statement(&normalized) {
            Ok(statement) => statement,
            Err(_) => return Ok(None),
        };
        match statement {
            MvAdmittedStatement::Create(statement) => create_mv_with_ports(
                &self.ports,
                self.create_application.as_ref(),
                self.mv_backend.as_ref(),
                current_catalog,
                current_database,
                &statement,
                connector_context,
            )
            .map(Some),
            MvAdmittedStatement::Drop(statement) => drop_mv_with_ports(
                self.repository.as_ref(),
                self.mv_backend.as_ref(),
                current_catalog,
                current_database,
                &statement,
                connector_context,
            )
            .map(Some),
            MvAdmittedStatement::Alter(statement)
                if !matches!(
                    &statement.action,
                    AlterMaterializedViewAction::Repartition(_)
                ) =>
            {
                alter_mv_with_ports(
                    &self.ports,
                    current_catalog,
                    current_database,
                    &statement,
                    connector_context,
                )
                .map(Some)
            }
            MvAdmittedStatement::Alter(statement) => self
                .execute_repartition(
                    current_catalog,
                    current_database,
                    &statement,
                    connector_context,
                    execution,
                )
                .map(Some),
            MvAdmittedStatement::Refresh(statement) => self
                .execute_refresh(
                    current_catalog,
                    current_database,
                    &statement,
                    connector_context,
                    execution,
                )
                .map(Some),
            MvAdmittedStatement::Show(statement) => {
                list_mvs_with_backend(self.mv_backend.as_ref(), current_catalog, &statement)
                    .map(Some)
            }
        }
    }

    fn execute_repartition(
        &self,
        current_catalog: Option<&str>,
        current_database: &str,
        statement: &AlterMaterializedViewStmt,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
        execution: &QueryExecutionContext,
    ) -> Result<StatementResult, String> {
        let AlterMaterializedViewAction::Repartition(fields) = &statement.action else {
            return Err("MV repartition executor received a non-repartition action".to_string());
        };
        let target = resolve_refresh_mv_target(current_catalog, current_database, &statement.name)?;
        let refresh_statement = RefreshMaterializedViewStmt {
            name: statement.name.clone(),
            full: false,
        };
        let preparation =
            crate::query_execution::mv_assembly::refresh_preparation::StandaloneMvRefreshPreparationService::new_repartition_with_ports(
                &self.ports,
                current_catalog,
                current_database,
                &refresh_statement,
                fields,
                connector_context,
            );
        self.refresh_service
            .prepare_and_execute_refresh(
                &preparation,
                novarocks_sql::planning::mv::MvRefreshStatement::from(&refresh_statement),
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
        statement: &RefreshMaterializedViewStmt,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
        execution: &QueryExecutionContext,
    ) -> Result<StatementResult, String> {
        let refresh_statement = novarocks_sql::planning::mv::MvRefreshStatement::from(statement);
        refresh_statement.validate_supported()?;
        let target = resolve_refresh_mv_target(current_catalog, current_database, &statement.name)?;
        let target_catalog = target.catalog.as_deref().ok_or_else(|| {
            "REFRESH MATERIALIZED VIEW for an Iceberg MV requires current Iceberg catalog context"
                .to_string()
        })?;
        let requested_object = crate::mv::domain::dependency::model::iceberg_mv_dependency_ref(
            target_catalog,
            &target.database,
            &target.name,
        );
        let steps =
            crate::mv::domain::dependency::refresh::build_upstream_refresh_steps_with_repository(
                self.repository.as_ref(),
                &requested_object,
            )?;
        let mut last_result = None;
        for step in steps {
            if !step.is_iceberg() {
                return Err(format!(
                    "REFRESH MATERIALIZED VIEW is only supported for Iceberg-backed materialized views: {}",
                    step.display_name().trim_start_matches("mv:")
                ));
            }
            let target = step.into_target();
            let target_catalog = target.catalog.clone();
            let target_database = target.database.clone();
            let target_name = target.name.clone();
            let step_statement = RefreshMaterializedViewStmt {
                name: ObjectName {
                    parts: vec![target_database.clone(), target_name],
                },
                full: false,
            };
            let preparation =
                crate::query_execution::mv_assembly::refresh_preparation::StandaloneMvRefreshPreparationService::new_with_ports(
                    &self.ports,
                    target_catalog.as_deref(),
                    &target_database,
                    &step_statement,
                    connector_context,
                );
            last_result = Some(
                self.refresh_service
                    .prepare_and_execute_refresh(
                        &preparation,
                        novarocks_sql::planning::mv::MvRefreshStatement::from(&step_statement),
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

fn statement_result(result: MvStatementResult) -> StatementResult {
    match result {
        MvStatementResult::Ok => StatementResult::Ok,
        MvStatementResult::Query(result) => StatementResult::Query(result),
    }
}

fn parse_explain_refresh_materialized_view(
    sql: &str,
) -> Option<
    Result<
        (
            RefreshMaterializedViewStmt,
            novarocks_sql::compiler::ExplainLevel,
            bool,
        ),
        String,
    >,
> {
    let trimmed = sql.trim_start();
    let prefixes = [
        (
            "EXPLAIN ANALYZE REFRESH ",
            novarocks_sql::compiler::ExplainLevel::Analyze,
            true,
        ),
        (
            "EXPLAIN VERBOSE REFRESH ",
            novarocks_sql::compiler::ExplainLevel::Verbose,
            false,
        ),
        (
            "EXPLAIN COSTS REFRESH ",
            novarocks_sql::compiler::ExplainLevel::Costs,
            false,
        ),
        (
            "EXPLAIN REFRESH ",
            novarocks_sql::compiler::ExplainLevel::Normal,
            false,
        ),
    ];
    for (prefix, level, analyze) in prefixes {
        if trimmed
            .as_bytes()
            .get(..prefix.len())
            .is_some_and(|head| head.eq_ignore_ascii_case(prefix.as_bytes()))
        {
            let body = format!("REFRESH {}", trimmed[prefix.len()..].trim_start());
            let statement = match parse_mv_admitted_statement(&body) {
                Ok(statement) => statement,
                Err(error) => return Some(Err(error)),
            };
            let MvAdmittedStatement::Refresh(statement) = statement else {
                return Some(Err(
                    "EXPLAIN REFRESH only supports REFRESH MATERIALIZED VIEW".to_string(),
                ));
            };
            return Some(Ok((statement, level, analyze)));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::parse_explain_refresh_materialized_view;

    #[test]
    fn explain_refresh_parser_keeps_level_and_analyze_contract() {
        let verbose = parse_explain_refresh_materialized_view(
            "EXPLAIN VERBOSE REFRESH MATERIALIZED VIEW db.mv",
        )
        .expect("recognized")
        .expect("parsed");
        assert_eq!(verbose.0.name.parts, vec!["db", "mv"]);
        assert_eq!(verbose.1, novarocks_sql::compiler::ExplainLevel::Verbose);
        assert!(!verbose.2);

        let analyze =
            parse_explain_refresh_materialized_view("EXPLAIN ANALYZE REFRESH MATERIALIZED VIEW mv")
                .expect("recognized")
                .expect("parsed");
        assert_eq!(analyze.1, novarocks_sql::compiler::ExplainLevel::Analyze);
        assert!(analyze.2);
    }
}
