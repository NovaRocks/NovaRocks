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

//! Stateful materialized-view analysis and display adapter.

use arrow::datatypes::DataType;

use crate::mv::domain::analysis::{MvAnalysis, prepare_mv_select_for_catalog_provider};
use crate::mv::domain::application::MvShowStatement;
use crate::mv::domain::lifecycle::MvListRow;
use crate::mv::domain::model::MvStorageEngine;
use crate::mv::domain::readiness::MvReadinessPort;
use novarocks_mv_application::persistence::definition::{
    MvDesiredRefreshPolicy, StoredMvDefinition,
};
use novarocks_query_application::api::{QueryResult, build_utf8_table_query_result};

/// Lightweight projection of the iceberg base table that
/// `validate_ivm_primary_key` needs. Built once at the top of `create_mv`
/// from the loaded iceberg table; passing this struct keeps validation
/// pure and easy to unit-test.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BaseColumnDescriptor {
    pub name: String,
    pub data_type: DataType,
    /// Uppercased SQL type as the analyzer/iceberg-schema mapper produced
    /// it (e.g. `BIGINT`, `STRING`, `DECIMAL(18,2)`, `ARRAY<STRING>`).
    pub sql_type: String,
    pub nullable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BaseTableDescriptor {
    pub format_version: i32,
    pub columns: Vec<BaseColumnDescriptor>,
}

/// Validate that a parsed `PRIMARY KEY (col, ...)` clause on a CREATE
/// MATERIALIZED VIEW statement satisfies the IVM Phase-2 contract:
///
/// 1. The base table is iceberg format-version 2.
/// 2. Every PK column exists on the base table.
/// 3. Every PK column is NOT NULL on the base table.
/// 4. Every PK column has a hashable scalar type.
///
/// Errors fail fast in declared column order — the first mismatch wins.
/// Returns `Ok(())` on success and discards the PK list (PR-1 does not
/// persist it; PR-3 will).
pub(crate) fn validate_ivm_primary_key(
    pk_columns: &[String],
    base: &BaseTableDescriptor,
) -> Result<(), String> {
    // Messages are byte-identical to the provider's ChangeError Display, which
    // this used to borrow purely to render them; the only caller already
    // discarded the typed error via to_string().
    if base.format_version != 2 && base.format_version != 3 {
        return Err(format!(
            "iceberg base table format-version {} is not supported; IVM requires v2 or v3",
            base.format_version
        ));
    }
    for pk in pk_columns {
        let col = base
            .columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(pk))
            .ok_or_else(|| {
                format!("PRIMARY KEY column `{pk}` does not exist on the iceberg base table")
            })?;
        if col.nullable {
            return Err(format!(
                "PRIMARY KEY column `{}` must be NOT NULL on the iceberg base table",
                col.name
            ));
        }
        if !is_hashable_pk_type(&col.sql_type) {
            return Err(format!(
                "PRIMARY KEY column `{}` has unsupported type `{}`; only hashable scalar types are allowed",
                col.name, col.sql_type
            ));
        }
    }
    Ok(())
}

/// Hashable scalar-type predicate for IVM Phase-2 PRIMARY KEY columns.
/// Accepts: BIGINT, INT, SMALLINT, TINYINT, STRING, VARCHAR, DATE,
/// DATETIME, DECIMAL (with or without precision/scale).
/// Rejects: BOOLEAN, FLOAT, DOUBLE, ARRAY, MAP, STRUCT, JSON.
fn is_hashable_pk_type(sql_type: &str) -> bool {
    let upper = sql_type.to_ascii_uppercase();
    let head = upper.split(['(', '<']).next().unwrap_or("").trim();
    matches!(
        head,
        "BIGINT"
            | "INT"
            | "INTEGER"
            | "SMALLINT"
            | "TINYINT"
            | "STRING"
            | "VARCHAR"
            | "CHAR"
            | "DATE"
            | "DATETIME"
            | "TIMESTAMP"
            | "DECIMAL"
    )
}

/// List materialized views from the readiness-filtered Accelerator projection.
pub(crate) fn list_mv_rows_with_ports(
    readiness: &MvReadinessPort,
    current_catalog: Option<&str>,
    stmt: &MvShowStatement,
    storage_filter: Option<MvStorageEngine>,
) -> Result<Vec<MvListRow>, String> {
    let definitions = readiness
        .list_ready_projections()
        .map_err(|e| format!("load materialized view Accelerator projections failed: {e}"))?;

    let mut rows = Vec::new();
    for loaded in &definitions {
        let mv = &loaded.definition;
        if let Some(filter) = storage_filter
            && !mv.storage_engine.eq_ignore_ascii_case(filter.as_sql_str())
        {
            continue;
        }
        let engine = MvStorageEngine::from_sql_str(&mv.storage_engine)?;
        let refresh_state = refresh_status_for_mv(mv);
        if engine != MvStorageEngine::Iceberg {
            continue;
        }
        let Some(target_catalog) = mv.target_catalog.as_deref() else {
            continue;
        };
        if let Some(current_catalog) = current_catalog
            && !target_catalog.eq_ignore_ascii_case(current_catalog)
        {
            continue;
        };
        let Some(target_namespace) = mv.target_namespace.clone() else {
            continue;
        };
        if let Some(filter_db) = stmt.database.as_deref()
            && !target_namespace.eq_ignore_ascii_case(filter_db)
        {
            continue;
        }
        let Some(target_table) = mv.target_table.clone() else {
            continue;
        };
        rows.push(MvListRow {
            name: target_table,
            database: target_namespace,
            storage_engine: mv.storage_engine.clone(),
            refresh_mode: mv.refresh_policy.as_sql_str().to_string(),
            last_refresh_time: mv.last_refresh_ms.map(|value| value.to_string()),
            last_refresh_rows: mv.last_refresh_rows.map(|value| value.to_string()),
            base_tables: mv.base_table_refs.join(", "),
            select_text: mv.query_definition.raw_query_source.clone(),
            dependencies: dependency_display_for_mv_with_readiness(readiness, loaded)?,
            refresh_paused: mv.refresh_paused.to_string(),
            next_refresh_time: None,
            last_scheduler_error: None,
            max_staleness_ms: mv.max_staleness_ms.map(|value| value.to_string()),
            refresh_state,
            retry_after_time: None,
        });
    }
    Ok(rows)
}

fn refresh_status_for_mv(mv: &StoredMvDefinition) -> String {
    if mv.refresh_paused {
        return "PAUSED".to_string();
    }
    if matches!(mv.refresh_policy, MvDesiredRefreshPolicy::Manual) {
        "MANUAL".to_string()
    } else {
        "PENDING".to_string()
    }
}

/// Render the dependency-column text for a single MV row through the typed
/// repository boundary.
fn dependency_display_for_mv_with_readiness(
    readiness: &MvReadinessPort,
    projection: &novarocks_mv_application::repository::LoadedMvProjection,
) -> Result<String, String> {
    let dependencies = readiness
        .list_ready_dependencies_by_downstream(projection)
        .map_err(|e| format!("load MV dependencies for display failed: {e}"))?;
    Ok(dependencies
        .iter()
        .map(|dep| dep.upstream.display_name())
        .collect::<Vec<_>>()
        .join(", "))
}

/// Analyze an MV SELECT against an already-admitted query-local table provider.
///
/// The provider is built by the query-assembly owner that admitted the
/// request: the request-local catalog snapshot, the exact connector control
/// lease, and the catalog-application admission gate are all frozen into it
/// before analysis starts. This adapter contributes MV SELECT preparation and
/// analysis only; it never acquires catalog or connector authority itself.
pub fn analyze_mv_select_with_provider(
    current_catalog: Option<&str>,
    provider: &dyn novarocks_sql::planning::catalog::PlannerTableProvider,
    current_database: &str,
    query: &novarocks_parser::ast::Query,
    functions: &dyn novarocks_sql::compiler::SqlFunctionCatalog,
) -> Result<MvAnalysis, String> {
    let prepared =
        prepare_mv_select_for_catalog_provider(query, current_catalog, current_database)?;
    let catalog = novarocks_sql::compiler::SqlPlannerTableSnapshot::new(provider);
    let refresh_input = novarocks_sql::compiler::analyze_mv_refresh_input(
        novarocks_sql::compiler::SqlMvRefreshAnalysisContext {
            query: Box::new(prepared.query_for_analysis().clone()),
            current_database: current_database.to_string(),
            catalog: &catalog,
            functions,
        },
    )?;
    let output_columns = refresh_input.analysis_facts().output_columns;
    Ok(MvAnalysis {
        resolved_refs: prepared.resolved_refs().to_vec(),
        output_columns,
        refresh_input,
    })
}

pub(crate) fn build_mv_rows_result(rows: &[MvListRow]) -> Result<QueryResult, String> {
    const COLUMNS: &[(&str, bool)] = &[
        ("Name", false),
        ("Database", false),
        ("StorageEngine", false),
        ("RefreshMode", false),
        ("LastRefreshTime", true),
        ("LastRefreshRows", true),
        ("BaseTables", false),
        ("SelectText", false),
        ("Dependencies", false),
        ("RefreshPaused", false),
        ("NextRefreshTime", true),
        ("LastSchedulerError", true),
        ("MaxStalenessMs", true),
        ("RefreshState", false),
        ("RetryAfterTime", true),
    ];
    let rows = rows
        .iter()
        .map(|row| {
            vec![
                Some(row.name.clone()),
                Some(row.database.clone()),
                Some(row.storage_engine.clone()),
                Some(row.refresh_mode.clone()),
                row.last_refresh_time.clone(),
                row.last_refresh_rows.clone(),
                Some(row.base_tables.clone()),
                Some(row.select_text.clone()),
                Some(row.dependencies.clone()),
                Some(row.refresh_paused.clone()),
                row.next_refresh_time.clone(),
                row.last_scheduler_error.clone(),
                row.max_staleness_ms.clone(),
                Some(row.refresh_state.clone()),
                row.retry_after_time.clone(),
            ]
        })
        .collect();
    build_utf8_table_query_result(COLUMNS, rows)
        .map_err(|error| format!("build SHOW MATERIALIZED VIEWS batch failed: {error}"))
}
