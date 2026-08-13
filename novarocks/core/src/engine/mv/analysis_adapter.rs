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

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::catalog_application::CatalogApplicationPort;
use crate::engine::mv::lifecycle::MvListRow;
use crate::engine::query_planning::catalog_runtime::QueryCatalogService;
use crate::mv::analysis::{MvAnalysis, finish_mv_analysis, prepare_mv_select_for_catalog_provider};
use crate::mv::model::MvStorageEngine;
use crate::mv::persistence::definition::{StoredMvDefinition, StoredMvRefreshPolicy};
use crate::mv::persistence::refresh::MvRefreshState;
use crate::mv::repository::MvRepository;
use crate::runtime::query_result::{QueryResult, QueryResultColumn, record_batch_to_chunk};
use crate::sql::parser::ast::ShowMaterializedViewsStmt;
use novarocks_spi::connector::{ConnectorControlResolver, ConnectorRequestContext};

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

/// List materialized views from the explicit durable metadata boundary.
///
/// The caller supplies only the repository needed to derive stored refresh
/// state and dependency display text; this projection has no application-state
/// or connector ownership.
pub(crate) fn list_mv_rows_with_ports(
    repository: &dyn MvRepository,
    current_catalog: Option<&str>,
    stmt: &ShowMaterializedViewsStmt,
    storage_filter: Option<MvStorageEngine>,
) -> Result<Vec<MvListRow>, String> {
    let definitions = repository
        .list_definitions()
        .map_err(|e| format!("load materialized view definitions failed: {e}"))?;
    let now_ms = now_ms();

    let mut rows = Vec::new();
    for mv in &definitions {
        if let Some(filter) = storage_filter
            && !mv.storage_engine.eq_ignore_ascii_case(filter.as_sql_str())
        {
            continue;
        }
        let engine = MvStorageEngine::from_sql_str(&mv.storage_engine)?;
        let (refresh_state, retry_after_time) =
            refresh_status_for_mv_with_repository(repository, mv, now_ms)?;
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
            select_text: mv.select_sql.clone(),
            dependencies: dependency_display_for_mv_with_repository(repository, mv.mv_id)?,
            refresh_paused: mv.refresh_paused.to_string(),
            next_refresh_time: mv.next_refresh_after_ms.map(|value| value.to_string()),
            last_scheduler_error: mv.last_scheduler_error.clone(),
            max_staleness_ms: mv.max_staleness_ms.map(|value| value.to_string()),
            refresh_state,
            retry_after_time,
        });
    }
    Ok(rows)
}

fn refresh_status_for_mv_with_repository(
    repository: &dyn MvRepository,
    mv: &StoredMvDefinition,
    now_ms: i64,
) -> Result<(String, Option<String>), String> {
    let retry_after_time = mv
        .last_scheduler_error
        .as_ref()
        .and_then(|_| mv.next_refresh_after_ms)
        .filter(|next| *next > now_ms)
        .map(|value| value.to_string());
    if mv.refresh_paused {
        return Ok(("PAUSED".to_string(), retry_after_time));
    }
    if let Some(refresh_id) = mv.active_refresh_id {
        let refresh = repository
            .load_refresh(refresh_id)
            .map_err(|e| format!("load active MV refresh failed: {e}"))?;
        if refresh
            .as_ref()
            .map(|refresh| refresh.state == MvRefreshState::CommitUnknown)
            .unwrap_or(false)
        {
            return Ok(("BLOCKED_RECOVERY".to_string(), retry_after_time));
        }
        return Ok(("RUNNING".to_string(), retry_after_time));
    }
    if mv.refresh_in_progress {
        return Ok(("RUNNING".to_string(), retry_after_time));
    }
    if mv.last_scheduler_error.is_some() && mv.next_refresh_after_ms.is_none() {
        return Ok(("BLOCKED_SCHEDULER".to_string(), retry_after_time));
    }
    if mv.last_scheduler_error.is_some()
        && mv
            .next_refresh_after_ms
            .map(|next| next > now_ms)
            .unwrap_or(false)
    {
        return Ok(("FAILED_BACKOFF".to_string(), retry_after_time));
    }
    if matches!(mv.refresh_policy, StoredMvRefreshPolicy::Manual) {
        return Ok(("MANUAL".to_string(), retry_after_time));
    }
    if mv
        .next_refresh_after_ms
        .map(|next| next > now_ms)
        .unwrap_or(false)
    {
        Ok(("SUCCEEDED".to_string(), retry_after_time))
    } else {
        Ok(("PENDING".to_string(), retry_after_time))
    }
}

/// Render the dependency-column text for a single MV row through the typed
/// repository boundary.
fn dependency_display_for_mv_with_repository(
    repository: &dyn MvRepository,
    mv_id: i64,
) -> Result<String, String> {
    let dependencies = repository
        .list_dependencies_by_downstream(mv_id)
        .map_err(|e| format!("load MV dependencies for display failed: {e}"))?;
    Ok(dependencies
        .iter()
        .map(|dep| dep.upstream.display_name())
        .collect::<Vec<_>>()
        .join(", "))
}

/// Analyze an MV SELECT from explicit query-local catalog and connector ports.
///
/// `catalog_service` is expected to be a request-local snapshot captured by
/// the caller. The optional catalog application remains part of analysis so
/// external-table materialization is admitted only while the catalog is ready.
pub(crate) fn analyze_mv_select_with_ports(
    current_catalog: Option<&str>,
    catalog_service: &QueryCatalogService,
    catalog_application: Option<&dyn CatalogApplicationPort>,
    connector_control: &dyn ConnectorControlResolver,
    current_database: &str,
    query: &sqlparser::ast::Query,
    connector_context: &ConnectorRequestContext,
) -> Result<MvAnalysis, String> {
    let prepared =
        prepare_mv_select_for_catalog_provider(query, current_catalog, current_database)?;
    let provider = crate::engine::build_catalog_service_provider(
        current_catalog,
        catalog_service,
        connector_control,
        connector_context.clone(),
        crate::sql::catalog::TableLookupMode::SchemaOnly,
        catalog_application,
    );
    let (resolved, _, _) =
        crate::sql::analyzer::analyze(prepared.query_for_analysis(), &provider, current_database)?;
    Ok(finish_mv_analysis(prepared, resolved))
}

pub(crate) fn build_mv_rows_result(rows: &[MvListRow]) -> Result<QueryResult, String> {
    let columns = vec![
        QueryResultColumn {
            name: "Name".to_string(),
            data_type: DataType::Utf8,
            nullable: false,
            logical_type: None,
        },
        QueryResultColumn {
            name: "Database".to_string(),
            data_type: DataType::Utf8,
            nullable: false,
            logical_type: None,
        },
        QueryResultColumn {
            name: "StorageEngine".to_string(),
            data_type: DataType::Utf8,
            nullable: false,
            logical_type: None,
        },
        QueryResultColumn {
            name: "RefreshMode".to_string(),
            data_type: DataType::Utf8,
            nullable: false,
            logical_type: None,
        },
        QueryResultColumn {
            name: "LastRefreshTime".to_string(),
            data_type: DataType::Utf8,
            nullable: true,
            logical_type: None,
        },
        QueryResultColumn {
            name: "LastRefreshRows".to_string(),
            data_type: DataType::Utf8,
            nullable: true,
            logical_type: None,
        },
        QueryResultColumn {
            name: "BaseTables".to_string(),
            data_type: DataType::Utf8,
            nullable: false,
            logical_type: None,
        },
        QueryResultColumn {
            name: "SelectText".to_string(),
            data_type: DataType::Utf8,
            nullable: false,
            logical_type: None,
        },
        QueryResultColumn {
            name: "Dependencies".to_string(),
            data_type: DataType::Utf8,
            nullable: false,
            logical_type: None,
        },
        QueryResultColumn {
            name: "RefreshPaused".to_string(),
            data_type: DataType::Utf8,
            nullable: false,
            logical_type: None,
        },
        QueryResultColumn {
            name: "NextRefreshTime".to_string(),
            data_type: DataType::Utf8,
            nullable: true,
            logical_type: None,
        },
        QueryResultColumn {
            name: "LastSchedulerError".to_string(),
            data_type: DataType::Utf8,
            nullable: true,
            logical_type: None,
        },
        QueryResultColumn {
            name: "MaxStalenessMs".to_string(),
            data_type: DataType::Utf8,
            nullable: true,
            logical_type: None,
        },
        QueryResultColumn {
            name: "RefreshState".to_string(),
            data_type: DataType::Utf8,
            nullable: false,
            logical_type: None,
        },
        QueryResultColumn {
            name: "RetryAfterTime".to_string(),
            data_type: DataType::Utf8,
            nullable: true,
            logical_type: None,
        },
    ];

    let schema = Arc::new(Schema::new(vec![
        Field::new("Name", DataType::Utf8, false),
        Field::new("Database", DataType::Utf8, false),
        Field::new("StorageEngine", DataType::Utf8, false),
        Field::new("RefreshMode", DataType::Utf8, false),
        Field::new("LastRefreshTime", DataType::Utf8, true),
        Field::new("LastRefreshRows", DataType::Utf8, true),
        Field::new("BaseTables", DataType::Utf8, false),
        Field::new("SelectText", DataType::Utf8, false),
        Field::new("Dependencies", DataType::Utf8, false),
        Field::new("RefreshPaused", DataType::Utf8, false),
        Field::new("NextRefreshTime", DataType::Utf8, true),
        Field::new("LastSchedulerError", DataType::Utf8, true),
        Field::new("MaxStalenessMs", DataType::Utf8, true),
        Field::new("RefreshState", DataType::Utf8, false),
        Field::new("RetryAfterTime", DataType::Utf8, true),
    ]));
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| Some(row.name.clone()))
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| Some(row.database.clone()))
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| Some(row.storage_engine.clone()))
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| Some(row.refresh_mode.clone()))
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.last_refresh_time.clone())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.last_refresh_rows.clone())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| Some(row.base_tables.clone()))
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| Some(row.select_text.clone()))
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| Some(row.dependencies.clone()))
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| Some(row.refresh_paused.clone()))
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.next_refresh_time.clone())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.last_scheduler_error.clone())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.max_staleness_ms.clone())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| Some(row.refresh_state.clone()))
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.retry_after_time.clone())
                .collect::<Vec<_>>(),
        )),
    ];
    let batch = RecordBatch::try_new(schema, arrays)
        .map_err(|e| format!("build SHOW MATERIALIZED VIEWS batch failed: {e}"))?;
    Ok(QueryResult {
        columns,
        chunks: vec![record_batch_to_chunk(batch)?],
    })
}

pub(crate) fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
