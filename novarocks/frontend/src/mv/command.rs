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
use crate::mv::activity::MvActivityOwner;
use crate::mv::domain::application::{
    MvAlterAction, MvAlterStatement, MvApplicationService, MvCreateDistribution,
    MvCreatePartitionField, MvCreateRefreshPolicy, MvCreateStatement, MvDropStatement,
    MvRefreshRequest, MvShowStatement, MvStatementResult,
};
use crate::mv::domain::iceberg_backend::IcebergMvBackend;
use crate::mv::domain::iceberg_refresh::IcebergMvCorePorts;
use crate::runtime::statement_result::StatementResult;
use novarocks_parser::ast::{
    CallStatement, Literal, LiteralKind, MaterializedViewAlterAction as TypedAlterAction,
    MaterializedViewExplainLevel, MaterializedViewPartitionArgument,
    MaterializedViewPartitionField, MaterializedViewRefreshMode,
    MaterializedViewRefreshPolicy as TypedRefreshPolicy, MaterializedViewStatement,
    ObjectName as TypedObjectName,
};
use novarocks_spi::connector::MvStorageObservationPort;
use novarocks_sql::semantic::IcebergPartitionFieldExpr;
use novarocks_types::naming::normalize_identifier;

use super::FrontendMvService;
use crate::mv::domain::refresh::resolve_refresh_mv_target;
use crate::mv::domain::{
    alter_mv_with_ports, create_mv_with_ports, drop_mv_with_ports,
    execute_typed_novarocks_imv_stateless_rebuild, list_mvs_with_backend,
};
use crate::runtime::query_result::build_string_query_result;
use std::sync::Arc;

#[derive(Clone)]
pub struct MvCommandExecutor {
    ports: IcebergMvCorePorts,
    create_application: Arc<dyn MvApplicationService>,
    refresh_service: Arc<FrontendMvService>,
    storage_observation: Arc<dyn MvStorageObservationPort>,
    mv_backend: Arc<IcebergMvBackend>,
}

impl MvCommandExecutor {
    pub fn new(
        ports: IcebergMvCorePorts,
        create_application: Arc<dyn MvApplicationService>,
        refresh_service: Arc<FrontendMvService>,
        storage_observation: Arc<dyn MvStorageObservationPort>,
        mv_backend: Arc<IcebergMvBackend>,
    ) -> Self {
        Self {
            ports,
            create_application,
            refresh_service,
            storage_observation,
            mv_backend,
        }
    }

    /// Executes one parser-admitted MV command through explicit ports. Refresh
    /// and repartition receive the request's already-admitted execution
    /// context; they never capture a second topology or cancellation scope.
    // Design: ADR-0088 (docs/adr/ADR-0088-domain-owned-sql-error-contracts.md)
    pub fn execute(
        &self,
        statement: &MaterializedViewStatement,
        current_catalog: Option<&str>,
        current_database: &str,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
        execution: &QueryExecutionContext,
    ) -> Result<StatementResult, String> {
        match statement {
            MaterializedViewStatement::Create(statement) => {
                let statement = lower_typed_create(statement)?;
                let target = resolve_refresh_mv_target(
                    current_catalog,
                    current_database,
                    &statement.name_parts,
                )?;
                self.refresh_service.execute_serialized(
                    &target,
                    MvActivityOwner::Create,
                    execution,
                    || {
                        create_mv_with_ports(
                            &self.ports,
                            self.create_application.as_ref(),
                            self.mv_backend.as_ref(),
                            current_catalog,
                            current_database,
                            &statement,
                            connector_context,
                        )
                    },
                )
            }
            MaterializedViewStatement::Drop(statement) => {
                let statement = MvDropStatement {
                    name_parts: lower_typed_name_parts(&statement.name)?,
                    if_exists: statement.if_exists,
                };
                let target = resolve_refresh_mv_target(
                    current_catalog,
                    current_database,
                    &statement.name_parts,
                )?;
                self.refresh_service.execute_serialized(
                    &target,
                    MvActivityOwner::Drop,
                    execution,
                    || {
                        drop_mv_with_ports(
                            self.ports.readiness().as_ref(),
                            self.mv_backend.as_ref(),
                            current_catalog,
                            current_database,
                            &statement,
                            connector_context,
                        )
                    },
                )
            }
            MaterializedViewStatement::Alter(statement)
                if !matches!(&statement.action, TypedAlterAction::Repartition(_)) =>
            {
                let statement = lower_typed_alter(statement)?;
                let target = resolve_refresh_mv_target(
                    current_catalog,
                    current_database,
                    &statement.name_parts,
                )?;
                self.refresh_service.execute_serialized(
                    &target,
                    MvActivityOwner::Alter,
                    execution,
                    || {
                        alter_mv_with_ports(
                            &self.ports,
                            current_catalog,
                            current_database,
                            &statement,
                            connector_context,
                        )
                    },
                )
            }
            MaterializedViewStatement::Alter(statement) => self.execute_repartition(
                current_catalog,
                current_database,
                &lower_typed_alter(statement)?,
                connector_context,
                execution,
            ),
            MaterializedViewStatement::Refresh(statement) => self.execute_refresh(
                current_catalog,
                current_database,
                &lower_typed_refresh(statement)?,
                connector_context,
                execution,
            ),
            MaterializedViewStatement::Show(statement) => list_mvs_with_backend(
                self.mv_backend.as_ref(),
                current_catalog,
                &lower_typed_show(statement)?,
            ),
            MaterializedViewStatement::ExplainRefresh(statement) => self.execute_explain_refresh(
                current_catalog,
                current_database,
                &lower_typed_refresh(&statement.refresh)?,
                lower_typed_explain_level(statement.level),
                connector_context,
            ),
        }
    }

    /// Executes the test-only stateless rebuild directly from parser-owned
    /// `CALL` syntax. Other procedures remain a route miss.
    pub fn try_execute_typed_call(
        &self,
        statement: &CallStatement,
        current_database: &str,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<Option<StatementResult>, String> {
        execute_typed_novarocks_imv_stateless_rebuild(
            self.ports.connector_control(),
            self.storage_observation.as_ref(),
            self.ports.readiness().as_ref(),
            statement,
            current_database,
            connector_context.clone(),
        )
    }

    fn execute_explain_refresh(
        &self,
        current_catalog: Option<&str>,
        current_database: &str,
        statement: &MvRefreshRequest,
        level: novarocks_sql::compiler::ExplainLevel,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<StatementResult, String> {
        let lines = crate::query_execution::mv_assembly::refresh_explain::explain_iceberg_mv_refresh_rewrite_plan_with_ports(
            &self.ports,
            current_catalog,
            current_database,
            statement,
            level,
            connector_context,
        )?;
        build_string_query_result("Explain String", lines).map(StatementResult::Query)
    }

    fn execute_repartition(
        &self,
        current_catalog: Option<&str>,
        current_database: &str,
        statement: &MvAlterStatement,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
        execution: &QueryExecutionContext,
    ) -> Result<StatementResult, String> {
        let MvAlterAction::Repartition(fields) = &statement.action else {
            return Err("MV repartition executor received a non-repartition action".to_string());
        };
        let target =
            resolve_refresh_mv_target(current_catalog, current_database, &statement.name_parts)?;
        let refresh_statement = MvRefreshRequest {
            name_parts: statement.name_parts.clone(),
            full: false,
        };
        let repartition_fields = fields
            .iter()
            .map(IcebergPartitionFieldExpr::from)
            .collect::<Vec<_>>();
        let preparation =
            crate::query_execution::mv_assembly::refresh_preparation::StandaloneMvRefreshPreparationService::new_repartition_with_ports(
                &self.ports,
                current_catalog,
                current_database,
                &refresh_statement,
                &repartition_fields,
                connector_context,
            );
        self.refresh_service
            .prepare_and_execute_refresh(
                &preparation,
                refresh_statement.sql_refresh_statement(),
                target,
                MvActivityOwner::Repartition,
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
        statement: &MvRefreshRequest,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
        execution: &QueryExecutionContext,
    ) -> Result<StatementResult, String> {
        let refresh_statement = statement.sql_refresh_statement();
        refresh_statement.validate_supported()?;
        let target =
            resolve_refresh_mv_target(current_catalog, current_database, &statement.name_parts)?;
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
            crate::mv::domain::dependency::refresh::build_upstream_refresh_steps_with_readiness(
                self.ports.readiness().as_ref(),
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
            let step_statement = MvRefreshRequest {
                name_parts: vec![target_database.clone(), target_name],
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
                        step_statement.sql_refresh_statement(),
                        target,
                        MvActivityOwner::ManualRefresh,
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

fn lower_typed_create(
    statement: &novarocks_parser::ast::CreateMaterializedView,
) -> Result<MvCreateStatement, String> {
    let distribution = statement.distribution.as_ref().ok_or_else(|| {
        "CREATE MATERIALIZED VIEW requires a DISTRIBUTED BY HASH(...) BUCKETS n clause".to_string()
    })?;
    let bucket_count = distribution
        .buckets
        .as_ref()
        .ok_or_else(|| {
            "CREATE MATERIALIZED VIEW requires a DISTRIBUTED BY HASH(...) BUCKETS n clause"
                .to_string()
        })
        .and_then(|value| lower_typed_u32(value, "BUCKETS count"))?;
    let primary_key = statement
        .primary_key
        .as_ref()
        .map(|columns| {
            let mut seen = std::collections::BTreeSet::new();
            let mut lowered = Vec::with_capacity(columns.len());
            for column in columns {
                if !seen.insert(column.value.to_ascii_lowercase()) {
                    return Err(format!(
                        "duplicate column `{}` in PRIMARY KEY clause",
                        column.value
                    ));
                }
                lowered.push(column.value.clone());
            }
            Ok(lowered)
        })
        .transpose()?;
    let select_query = statement.query.clone();

    let partition_by = statement
        .partition_by
        .as_ref()
        .map(|fields| {
            lower_typed_partition_fields(fields)?;
            Ok::<_, String>(fields.iter().map(MvCreatePartitionField::from).collect())
        })
        .transpose()?;

    Ok(MvCreateStatement {
        name_parts: lower_typed_name_parts(&statement.name)?,
        if_not_exists: statement.if_not_exists,
        partition_by,
        distribution: Some(MvCreateDistribution {
            hash_columns: distribution
                .hash_columns
                .iter()
                .map(|column| normalize_identifier(&column.value))
                .collect::<Result<Vec<_>, _>>()?,
            bucket_count: Some(bucket_count),
        }),
        refresh_policy: lower_typed_refresh_policy(
            statement
                .refresh
                .as_ref()
                .unwrap_or(&TypedRefreshPolicy::Manual { deferred: false }),
        )?,
        select_sql: novarocks_parser::printer::print_query(&select_query),
        select_query,
        properties: lower_typed_properties(&statement.properties)?,
        primary_key,
    })
}

fn lower_typed_alter(
    statement: &novarocks_parser::ast::AlterMaterializedView,
) -> Result<MvAlterStatement, String> {
    let action = match &statement.action {
        TypedAlterAction::SetRefresh(policy) => {
            MvAlterAction::SetRefresh(lower_typed_refresh_policy(policy)?)
        }
        TypedAlterAction::SetProperties(properties) => {
            let properties = lower_typed_properties(properties)?;
            if properties.is_empty() {
                return Err(
                    "ALTER MATERIALIZED VIEW SET TBLPROPERTIES requires at least one key=value pair"
                        .to_string(),
                );
            }
            let mut seen = std::collections::BTreeSet::new();
            for (key, _) in &properties {
                if !seen.insert(key.clone()) {
                    return Err(format!(
                        "duplicate key '{key}' in ALTER MATERIALIZED VIEW SET TBLPROPERTIES"
                    ));
                }
            }
            MvAlterAction::SetProperties(properties)
        }
        TypedAlterAction::PauseRefresh => MvAlterAction::PauseRefresh,
        TypedAlterAction::ResumeRefresh => MvAlterAction::ResumeRefresh,
        TypedAlterAction::Repartition(fields) => {
            MvAlterAction::Repartition(fields.iter().map(MvCreatePartitionField::from).collect())
        }
    };
    Ok(MvAlterStatement {
        name_parts: lower_typed_name_parts(&statement.name)?,
        action,
    })
}

fn lower_typed_refresh(
    statement: &novarocks_parser::ast::RefreshMaterializedView,
) -> Result<MvRefreshRequest, String> {
    if matches!(statement.mode, Some(MaterializedViewRefreshMode::Async)) {
        return Err(
            "REFRESH MATERIALIZED VIEW ... WITH ASYNC MODE is not supported yet".to_string(),
        );
    }
    Ok(MvRefreshRequest {
        name_parts: lower_typed_name_parts(&statement.name)?,
        full: statement.full,
    })
}

fn lower_typed_explain_level(
    level: MaterializedViewExplainLevel,
) -> novarocks_sql::compiler::ExplainLevel {
    match level {
        MaterializedViewExplainLevel::Default => novarocks_sql::compiler::ExplainLevel::Normal,
        MaterializedViewExplainLevel::Verbose => novarocks_sql::compiler::ExplainLevel::Verbose,
        MaterializedViewExplainLevel::Costs => novarocks_sql::compiler::ExplainLevel::Costs,
    }
}

fn lower_typed_show(
    statement: &novarocks_parser::ast::ShowMaterializedViews,
) -> Result<MvShowStatement, String> {
    let database = statement
        .database
        .as_ref()
        .map(|database| match database.parts.as_slice() {
            [database] => Ok(database.value.clone()),
            _ => Err("SHOW MATERIALIZED VIEWS FROM expects one database identifier".to_string()),
        })
        .transpose()?;
    Ok(MvShowStatement { database })
}

fn lower_typed_refresh_policy(
    policy: &TypedRefreshPolicy,
) -> Result<MvCreateRefreshPolicy, String> {
    match policy {
        TypedRefreshPolicy::Immediate => Err("REFRESH IMMEDIATE is not supported yet".to_string()),
        TypedRefreshPolicy::Manual { .. } => Ok(MvCreateRefreshPolicy::Manual),
        TypedRefreshPolicy::AsyncOnChange { .. } => Ok(MvCreateRefreshPolicy::AsyncOnChange),
        TypedRefreshPolicy::AsyncEvery { interval, unit, .. } => {
            let interval = lower_typed_u64(interval, "REFRESH ASYNC interval")?;
            if interval == 0 {
                return Err("REFRESH ASYNC interval must be positive".to_string());
            }
            let multiplier = match unit.value.to_ascii_uppercase().as_str() {
                "SECOND" | "SECONDS" => 1_000_u64,
                "MINUTE" | "MINUTES" => 60_000_u64,
                "HOUR" | "HOURS" => 3_600_000_u64,
                "DAY" | "DAYS" => 86_400_000_u64,
                _ => {
                    return Err(format!(
                        "unsupported REFRESH ASYNC interval unit `{}`; expected SECOND, MINUTE, HOUR, or DAY",
                        unit.value
                    ));
                }
            };
            let interval_ms = interval
                .checked_mul(multiplier)
                .ok_or_else(|| "REFRESH ASYNC interval is too large".to_string())?
                .try_into()
                .map_err(|_| "REFRESH ASYNC interval is too large".to_string())?;
            Ok(MvCreateRefreshPolicy::AsyncInterval { interval_ms })
        }
    }
}

fn lower_typed_partition_fields(
    fields: &[MaterializedViewPartitionField],
) -> Result<Vec<IcebergPartitionFieldExpr>, String> {
    fields.iter().map(lower_typed_partition_field).collect()
}

fn lower_typed_partition_field(
    field: &MaterializedViewPartitionField,
) -> Result<IcebergPartitionFieldExpr, String> {
    match field {
        MaterializedViewPartitionField::Identity(column) => {
            Ok(IcebergPartitionFieldExpr::Identity {
                column: normalize_identifier(&column.value)?,
            })
        }
        MaterializedViewPartitionField::Transform {
            name, arguments, ..
        } => {
            let transform = normalize_identifier(&name.value)?;
            let column = typed_partition_column(arguments, &transform)?;
            match transform.as_str() {
                "identity" => require_partition_argument_count(arguments, 1, "identity")
                    .map(|()| IcebergPartitionFieldExpr::Identity { column }),
                "year" => require_partition_argument_count(arguments, 1, "year")
                    .map(|()| IcebergPartitionFieldExpr::Year { column }),
                "month" => require_partition_argument_count(arguments, 1, "month")
                    .map(|()| IcebergPartitionFieldExpr::Month { column }),
                "day" => require_partition_argument_count(arguments, 1, "day")
                    .map(|()| IcebergPartitionFieldExpr::Day { column }),
                "hour" => require_partition_argument_count(arguments, 1, "hour")
                    .map(|()| IcebergPartitionFieldExpr::Hour { column }),
                "void" => require_partition_argument_count(arguments, 1, "void")
                    .map(|()| IcebergPartitionFieldExpr::Void { column }),
                "bucket" => Ok(IcebergPartitionFieldExpr::Bucket {
                    column,
                    num_buckets: typed_partition_positive_u32(arguments, "bucket count")?,
                }),
                "truncate" => Ok(IcebergPartitionFieldExpr::Truncate {
                    column,
                    width: typed_partition_positive_u32(arguments, "truncate width")?,
                }),
                _ => Err(format!(
                    "unsupported Iceberg partition transform `{}`",
                    name.value
                )),
            }
        }
    }
}

fn typed_partition_column(
    arguments: &[MaterializedViewPartitionArgument],
    transform: &str,
) -> Result<String, String> {
    let Some(MaterializedViewPartitionArgument::Ident(column)) = arguments.first() else {
        return Err(format!(
            "expected column argument for partition transform `{transform}`"
        ));
    };
    normalize_identifier(&column.value)
}

fn require_partition_argument_count(
    arguments: &[MaterializedViewPartitionArgument],
    expected: usize,
    transform: &str,
) -> Result<(), String> {
    if arguments.len() == expected {
        Ok(())
    } else if transform == "identity" {
        Err("identity() requires exactly one column argument: identity(column)".to_string())
    } else {
        Err(format!(
            "expected {expected} argument(s) for partition transform `{transform}`"
        ))
    }
}

fn typed_partition_positive_u32(
    arguments: &[MaterializedViewPartitionArgument],
    label: &str,
) -> Result<u32, String> {
    require_partition_argument_count(arguments, 2, label)?;
    let Some(MaterializedViewPartitionArgument::Literal(value)) = arguments.get(1) else {
        return Err(format!("expected numeric {label}"));
    };
    let value = lower_typed_u32(value, label)?;
    if value == 0 {
        return Err(format!("{label} must be positive"));
    }
    Ok(value)
}

fn lower_typed_properties(
    properties: &[novarocks_parser::ast::MaterializedViewProperty],
) -> Result<Vec<(String, String)>, String> {
    properties
        .iter()
        .map(|property| {
            Ok((
                lower_typed_string(&property.key, "MV property key")?,
                lower_typed_string(&property.value, "MV property value")?,
            ))
        })
        .collect()
}

fn lower_typed_name_parts(name: &TypedObjectName) -> Result<Vec<String>, String> {
    if name.parts.is_empty() {
        return Err("materialized view name must not be empty".to_string());
    }
    Ok(name.parts.iter().map(|part| part.value.clone()).collect())
}

fn lower_typed_u32(value: &Literal, context: &str) -> Result<u32, String> {
    lower_typed_u64(value, context)?
        .try_into()
        .map_err(|_| format!("{context} is too large"))
}

fn lower_typed_u64(value: &Literal, context: &str) -> Result<u64, String> {
    let LiteralKind::Number(value) = &value.kind else {
        return Err(format!("{context} expects a number"));
    };
    value
        .parse::<u64>()
        .map_err(|error| format!("invalid {context} `{value}`: {error}"))
}

fn lower_typed_string(value: &Literal, context: &str) -> Result<String, String> {
    let LiteralKind::String(value) = &value.kind else {
        return Err(format!("{context} expects a string"));
    };
    Ok(value.clone())
}

fn statement_result(result: MvStatementResult) -> StatementResult {
    match result {
        MvStatementResult::Ok => StatementResult::Ok,
        MvStatementResult::Query(result) => StatementResult::Query(result),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_parser::{
        ast::{MaterializedViewStatement, Statement},
        parse,
    };

    fn typed_mv(source: &str) -> MaterializedViewStatement {
        let statements = parse(source).expect("typed MV statement should parse");
        let [Statement::MaterializedView(statement)] = statements.as_slice() else {
            panic!("expected one typed MV statement for {source}");
        };
        statement.clone()
    }

    #[test]
    fn lowers_create_header_and_parses_only_embedded_query_slice() {
        let statement = typed_mv(
            "CREATE MATERIALIZED VIEW `analytics`.`orders_mv` \
             PARTITION BY (month(order_date)) \
             DISTRIBUTED BY HASH(order_id) BUCKETS 4 \
             REFRESH ASYNC EVERY INTERVAL 2 HOUR \
             PROPERTIES ('storage_engine' = 'iceberg') \
             AS SELECT order_id, order_date FROM ice.analytics.orders",
        );
        let MaterializedViewStatement::Create(create) = statement else {
            panic!("expected typed CREATE MATERIALIZED VIEW");
        };

        let lowered = lower_typed_create(&create).expect("typed create should lower");
        assert_eq!(lowered.name_parts, vec!["analytics", "orders_mv"]);
        assert_eq!(
            lowered.refresh_policy,
            MvCreateRefreshPolicy::AsyncInterval {
                interval_ms: 7_200_000
            }
        );
        assert!(matches!(
            lowered.partition_by.as_deref(),
            Some([MvCreatePartitionField::Month { column }]) if column == "order_date"
        ));
        assert_eq!(
            novarocks_parser::printer::print_query(&lowered.select_query),
            lowered.select_sql
        );
    }

    #[test]
    fn lowers_typed_alter_and_show_without_raw_command_parse() {
        let alter = typed_mv(
            "ALTER MATERIALIZED VIEW `analytics`.`orders_mv` \
             SET TBLPROPERTIES ('refresh_priority' = 'high')",
        );
        let MaterializedViewStatement::Alter(alter) = alter else {
            panic!("expected typed ALTER MATERIALIZED VIEW");
        };
        assert_eq!(
            lower_typed_alter(&alter)
                .expect("typed alter should lower")
                .action,
            MvAlterAction::SetProperties(vec![(
                "refresh_priority".to_string(),
                "high".to_string(),
            )])
        );

        let show = typed_mv("SHOW MATERIALIZED VIEWS FROM analytics");
        let MaterializedViewStatement::Show(show) = show else {
            panic!("expected typed SHOW MATERIALIZED VIEWS");
        };
        assert_eq!(
            lower_typed_show(&show).expect("typed show should lower"),
            MvShowStatement {
                database: Some("analytics".to_string())
            }
        );
    }

    #[test]
    fn typed_lowering_reapplies_legacy_mv_semantic_limits() {
        let immediate = typed_mv(
            "CREATE MATERIALIZED VIEW mv DISTRIBUTED BY HASH(k1) BUCKETS 1 \
             REFRESH IMMEDIATE AS SELECT k1 FROM ice.db.source",
        );
        let MaterializedViewStatement::Create(immediate) = immediate else {
            panic!("expected typed CREATE MATERIALIZED VIEW");
        };
        assert!(
            lower_typed_create(&immediate)
                .expect_err("legacy MV create rejects immediate refresh")
                .contains("IMMEDIATE")
        );

        let refresh = typed_mv("REFRESH MATERIALIZED VIEW db.mv WITH ASYNC MODE");
        let MaterializedViewStatement::Refresh(refresh) = refresh else {
            panic!("expected typed REFRESH MATERIALIZED VIEW");
        };
        assert!(
            lower_typed_refresh(&refresh)
                .expect_err("legacy refresh rejects async mode")
                .contains("ASYNC")
        );
    }

    #[test]
    fn typed_explain_refresh_preserves_requested_presentation_level() {
        for (sql, expected) in [
            (
                "EXPLAIN REFRESH MATERIALIZED VIEW db.mv",
                novarocks_sql::compiler::ExplainLevel::Normal,
            ),
            (
                "EXPLAIN VERBOSE REFRESH MATERIALIZED VIEW db.mv",
                novarocks_sql::compiler::ExplainLevel::Verbose,
            ),
            (
                "EXPLAIN COSTS REFRESH MATERIALIZED VIEW db.mv",
                novarocks_sql::compiler::ExplainLevel::Costs,
            ),
        ] {
            let statement = typed_mv(sql);
            let MaterializedViewStatement::ExplainRefresh(explain) = statement else {
                panic!("expected typed EXPLAIN REFRESH MATERIALIZED VIEW");
            };
            assert_eq!(lower_typed_explain_level(explain.level), expected);
        }
    }
}
