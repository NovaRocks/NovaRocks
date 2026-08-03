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

use crate::engine::StandaloneState;
use crate::engine::mv::lifecycle::MvListRow;
use crate::engine::query_prep::drop_local_table_registration_if_exists;
use crate::meta::MetaReadTxn;
use crate::mv::analysis::{MvAnalysis, ResolvedTableRef, analyze_mv_select_with};
use crate::mv::model::MvStorageEngine;
use crate::mv::persistence::definition::{StoredMvDefinition, StoredMvRefreshPolicy};
use crate::mv::persistence::refresh::MvRefreshState;
use crate::runtime::query_result::{QueryResult, QueryResultColumn, record_batch_to_chunk};
use crate::sql::parser::ast::ShowMaterializedViewsStmt;

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
) -> Result<(), crate::connector::iceberg::changes::ChangeError> {
    use crate::connector::iceberg::changes::ChangeError;

    if base.format_version != 2 && base.format_version != 3 {
        return Err(ChangeError::IcebergFormatUnsupported {
            format_version: base.format_version,
        });
    }
    for pk in pk_columns {
        let col = base
            .columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(pk))
            .ok_or_else(|| ChangeError::PrimaryKeyMissingFromBase { pk_col: pk.clone() })?;
        if col.nullable {
            return Err(ChangeError::PrimaryKeyNullable {
                pk_col: col.name.clone(),
            });
        }
        if !is_hashable_pk_type(&col.sql_type) {
            return Err(ChangeError::PrimaryKeyTypeUnsupported {
                pk_col: col.name.clone(),
                ty: col.sql_type.clone(),
            });
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

/// Map an Arrow `DataType` to the SQL head token that
/// `is_hashable_pk_type` recognizes. Returns the token only — no
/// precision/scale or element-type tail. Anything not on the accepted
/// list falls through to the Arrow Debug form (e.g. `Float32`,
/// `List(...)`), which `is_hashable_pk_type` will then reject.
fn arrow_data_type_pk_head(dt: &arrow::datatypes::DataType) -> String {
    use arrow::datatypes::DataType;
    match dt {
        DataType::Int8 => "TINYINT".to_string(),
        DataType::Int16 => "SMALLINT".to_string(),
        DataType::Int32 => "INT".to_string(),
        DataType::Int64 => "BIGINT".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 => "STRING".to_string(),
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => "DECIMAL".to_string(),
        DataType::Date32 | DataType::Date64 => "DATE".to_string(),
        DataType::Timestamp(_, _) => "DATETIME".to_string(),
        // Explicitly unsupported as PK: floats (NaN equality), booleans
        // (degenerate cardinality), composites (no stable hash). Fall
        // through to Debug form so is_hashable_pk_type rejects them.
        other => format!("{other:?}"),
    }
}

/// Build the `BaseTableDescriptor` projection from an already-loaded
/// iceberg table. Used by `create_mv` and `create_iceberg_mv` before
/// invoking `validate_ivm_primary_key`.
pub(crate) fn descriptor_from_loaded(
    loaded: &crate::connector::iceberg::catalog::IcebergLoadedTable,
) -> BaseTableDescriptor {
    let format_version = loaded.table.metadata().format_version() as i32;
    let columns = loaded
        .columns
        .iter()
        .map(|col| BaseColumnDescriptor {
            name: col.name.clone(),
            data_type: col.data_type.clone(),
            sql_type: arrow_data_type_pk_head(&col.data_type),
            nullable: col.nullable,
        })
        .collect();
    BaseTableDescriptor {
        format_version,
        columns,
    }
}
pub(crate) fn list_mv_rows(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    stmt: &ShowMaterializedViewsStmt,
    storage_filter: Option<MvStorageEngine>,
) -> Result<Vec<MvListRow>, String> {
    let definitions = state
        .mv_repository
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
        let (refresh_state, retry_after_time) = refresh_status_for_mv(state, mv, now_ms)?;
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
            dependencies: dependency_display_for_mv(state, mv.mv_id)?,
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

fn refresh_status_for_mv(
    state: &Arc<StandaloneState>,
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
        let refresh = state
            .mv_repository
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
fn dependency_display_for_mv(state: &Arc<StandaloneState>, mv_id: i64) -> Result<String, String> {
    let dependencies = state
        .mv_repository
        .list_dependencies_by_downstream(mv_id)
        .map_err(|e| format!("load MV dependencies for display failed: {e}"))?;
    Ok(dependencies
        .iter()
        .map(|dep| dep.upstream.display_name())
        .collect::<Vec<_>>()
        .join(", "))
}

pub(crate) fn analyze_mv_select_with_connector_context(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &sqlparser::ast::Query,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<MvAnalysis, String> {
    analyze_mv_select_with(
        query,
        current_catalog,
        current_database,
        |resolved_refs| {
            register_iceberg_tables_for_mv_analysis(state, resolved_refs, connector_context)
        },
        |query_for_analysis| {
            let catalog = state
                .catalog_service
                .local()
                .read()
                .expect("standalone catalog read lock");
            let (resolved, _, _factory) =
                crate::sql::analyzer::analyze(query_for_analysis, &*catalog, current_database)?;
            drop(catalog);
            Ok(resolved)
        },
    )
}

fn register_iceberg_tables_for_mv_analysis(
    state: &Arc<StandaloneState>,
    resolved_refs: &[ResolvedTableRef],
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<(), String> {
    let connectors = state
        .connectors
        .read()
        .expect("standalone connector registry read lock")
        .clone();

    for table_ref in resolved_refs {
        let ResolvedTableRef::Iceberg {
            catalog,
            namespace,
            table,
        } = table_ref
        else {
            continue;
        };
        drop_local_table_registration_if_exists(state, namespace, table)?;
        let (mut table_def, _, _) = crate::connector::iceberg::provider::load_table_def_at(
            state.connector_control.as_ref(),
            connector_context.clone(),
            catalog,
            namespace,
            table,
            None,
            false,
        )
        .map_err(|err| format!("load iceberg table {catalog}.{namespace}.{table} failed: {err}"))?;
        table_def.name = table.clone();
        let mut local_catalog = state
            .catalog_service
            .local()
            .write()
            .map_err(|e| format!("standalone catalog write lock: {e}"))?;
        local_catalog.create_database(namespace)?;
        local_catalog.register(namespace, table_def)?;
    }
    Ok(())
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
