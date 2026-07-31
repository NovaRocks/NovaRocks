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

//! Synthetic/local query preparation for time-travel, delta scans, MV helpers,
//! ANALYZE schema materialization, and catalog-service table invalidation.
//! Ordinary SELECT external tables resolve through CatalogServiceProvider.

use std::sync::Arc;

use crate::engine::StandaloneState;
use crate::engine::StatementResult;
use crate::engine::backend_resolver::resolve_table_target;
use crate::engine::statement::parse_add_files_sql;
use crate::runtime::query_result::build_string_query_result;
use crate::sql::analyzer::iceberg_ref::resolve_read_binding;
use crate::sql::parser::ast::ObjectName;
use crate::sql::planner::table::{ScanSource, TableDef};
use novarocks_catalog::schema::ColumnDef;

#[derive(Clone, Debug)]
pub(crate) struct IcebergFileForQuery {
    pub(crate) path: String,
    pub(crate) size: i64,
    pub(crate) record_count: Option<i64>,
    pub(crate) partition_spec_id: Option<i32>,
    pub(crate) partition_key: Option<String>,
    pub(crate) first_row_id: Option<i64>,
    pub(crate) data_sequence_number: Option<i64>,
    pub(crate) change_op: Option<i8>,
    pub(crate) row_id_allow_list: Option<std::collections::BTreeSet<i64>>,
}

pub(crate) fn delete_temp_iceberg_file_for_query(
    path: String,
    size: i64,
    record_count: Option<i64>,
    change_op: Option<i8>,
) -> IcebergFileForQuery {
    IcebergFileForQuery {
        path,
        size,
        record_count,
        partition_spec_id: None,
        partition_key: None,
        first_row_id: None,
        data_sequence_number: None,
        change_op,
        row_id_allow_list: None,
    }
}

pub(crate) fn add_files(
    state: &Arc<StandaloneState>,
    sql: &str,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<StatementResult, String> {
    let (table_parts, s3_path) = parse_add_files_sql(sql)?;

    let (catalog_name, namespace, table_name) = match table_parts.len() {
        1 => {
            let cat =
                current_catalog.ok_or("ADD FILES requires a catalog context (use SET catalog)")?;
            (
                cat.to_string(),
                current_database.to_string(),
                table_parts[0].clone(),
            )
        }
        2 => {
            let cat = current_catalog.ok_or("ADD FILES requires a catalog context")?;
            (
                cat.to_string(),
                table_parts[0].clone(),
                table_parts[1].clone(),
            )
        }
        3 => (
            table_parts[0].clone(),
            table_parts[1].clone(),
            table_parts[2].clone(),
        ),
        _ => return Err("invalid table name in ADD FILES".to_string()),
    };

    let guard = state
        .iceberg_catalogs
        .read()
        .expect("iceberg catalog read lock");
    let entry = guard.get(&catalog_name)?;
    drop(guard);
    let target = crate::engine::backend_resolver::TargetBackend {
        backend_name: "iceberg",
        catalog: catalog_name.clone(),
        namespace: namespace.clone(),
        table: table_name.clone(),
    };
    crate::engine::mv::iceberg_guard::reject_if_iceberg_mv_table(
        state,
        &target,
        crate::engine::mv::iceberg_guard::IcebergMvUserMutation::Insert,
    )?;
    let count = crate::connector::iceberg::catalog::add_files::add_files(
        &entry,
        &namespace,
        &table_name,
        &s3_path,
    )?;
    entry.invalidate_table_cache(&namespace, &table_name);
    state
        .catalog_service
        .invalidate_table(&catalog_name, &namespace, &table_name)?;
    let msg = format!("Added {count} file(s)");
    build_string_query_result("status", vec![msg]).map(StatementResult::Query)
}

// ---------------------------------------------------------------------------
// Time-travel (FOR VERSION/TIMESTAMP AS OF) AST rewrite
// ---------------------------------------------------------------------------

/// Returns true if the query contains any `TableFactor::Table` node with a
/// `version: Some(...)` clause. Used as a cheap pre-check before cloning.
pub(crate) fn has_time_travel_refs(query: &sqlparser::ast::Query) -> bool {
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            if has_time_travel_in_set_expr(cte.query.body.as_ref()) {
                return true;
            }
        }
    }
    has_time_travel_in_set_expr(query.body.as_ref())
}

fn has_time_travel_in_set_expr(expr: &sqlparser::ast::SetExpr) -> bool {
    match expr {
        sqlparser::ast::SetExpr::Select(select) => {
            for tw in &select.from {
                if has_time_travel_in_factor(&tw.relation) {
                    return true;
                }
                for join in &tw.joins {
                    if has_time_travel_in_factor(&join.relation) {
                        return true;
                    }
                }
            }
            false
        }
        sqlparser::ast::SetExpr::SetOperation { left, right, .. } => {
            has_time_travel_in_set_expr(left) || has_time_travel_in_set_expr(right)
        }
        sqlparser::ast::SetExpr::Query(q) => has_time_travel_in_set_expr(q.body.as_ref()),
        _ => false,
    }
}

fn has_time_travel_in_factor(factor: &sqlparser::ast::TableFactor) -> bool {
    match factor {
        sqlparser::ast::TableFactor::Table { version, .. } => version.is_some(),
        sqlparser::ast::TableFactor::Derived { subquery, .. } => {
            has_time_travel_in_set_expr(subquery.body.as_ref())
        }
        _ => false,
    }
}

/// Walk the query AST in-place and rewrite each `TableFactor::Table` that has
/// a `version: Some(...)` clause:
///
/// 1. Resolve `version` → `snapshot_id` via `resolve_read_binding`.
/// 2. Build a synthetic `TableDef` for that snapshot and register it in the
///    local planner catalog under the name `<table>__at_<snapshot_id>`.
/// 3. Rewrite the `TableFactor::Table`:
///    - Replace `name` with `default_catalog.<namespace>.<synthetic name>` so
///      CatalogServiceProvider routes the synthetic table through the local
///      planner catalog even when the session has an Iceberg current catalog.
///    - Clear `version` (set to `None`).
///    - Preserve any existing alias; if none, set `alias` = original table name
///      so that `SELECT t.col FROM t FOR VERSION AS OF ...` resolves `t.col`.
///
/// Tables without a version clause are left untouched.
pub(crate) fn rewrite_time_travel_refs(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &mut sqlparser::ast::Query,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<(), String> {
    // Walk CTEs
    if let Some(with) = &mut query.with {
        for cte in &mut with.cte_tables {
            rewrite_time_travel_in_set_expr(
                state,
                current_catalog,
                current_database,
                cte.query.body.as_mut(),
                connector_context,
            )?;
        }
    }
    rewrite_time_travel_in_set_expr(
        state,
        current_catalog,
        current_database,
        query.body.as_mut(),
        connector_context,
    )
}

fn rewrite_time_travel_in_set_expr(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    expr: &mut sqlparser::ast::SetExpr,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<(), String> {
    match expr {
        sqlparser::ast::SetExpr::Select(select) => {
            for tw in &mut select.from {
                rewrite_time_travel_in_factor(
                    state,
                    current_catalog,
                    current_database,
                    &mut tw.relation,
                    connector_context,
                )?;
                for join in &mut tw.joins {
                    rewrite_time_travel_in_factor(
                        state,
                        current_catalog,
                        current_database,
                        &mut join.relation,
                        connector_context,
                    )?;
                }
            }
            Ok(())
        }
        sqlparser::ast::SetExpr::SetOperation { left, right, .. } => {
            rewrite_time_travel_in_set_expr(
                state,
                current_catalog,
                current_database,
                left.as_mut(),
                connector_context,
            )?;
            rewrite_time_travel_in_set_expr(
                state,
                current_catalog,
                current_database,
                right.as_mut(),
                connector_context,
            )
        }
        sqlparser::ast::SetExpr::Query(q) => rewrite_time_travel_in_set_expr(
            state,
            current_catalog,
            current_database,
            q.body.as_mut(),
            connector_context,
        ),
        _ => Ok(()),
    }
}

fn rewrite_time_travel_in_factor(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    factor: &mut sqlparser::ast::TableFactor,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<(), String> {
    match factor {
        sqlparser::ast::TableFactor::Table {
            name,
            version,
            alias,
            ..
        } if version.is_some() => {
            let version_clause = version.take().expect("checked is_some above");

            // Extract name parts for our ObjectName lookup
            let parts: Vec<String> = name
                .0
                .iter()
                .filter_map(|p| match p {
                    sqlparser::ast::ObjectNamePart::Identifier(ident) => {
                        Some(ident.value.to_ascii_lowercase())
                    }
                    _ => None,
                })
                .collect();

            if parts.is_empty() {
                return Err("iceberg time travel: table name has no identifier parts".to_string());
            }

            // Reject the combination of branch/tag suffix with FOR VERSION/TIMESTAMP AS OF.
            if let Some(last) = parts.last() {
                for prefix in &["branch_", "tag_"] {
                    if let Some(ref_name) = last.strip_prefix(prefix)
                        && !ref_name.is_empty()
                    {
                        return Err(format!(
                            "iceberg ref: branch suffix '.{}_{}' conflicts with FOR VERSION AS OF clause",
                            prefix.trim_end_matches('_'),
                            ref_name,
                        ));
                    }
                }
            }

            let our_name = ObjectName { parts };
            let target = resolve_table_target(state, &our_name, current_catalog, current_database)?;

            if target.backend_name != "iceberg" {
                return Err(format!(
                    "iceberg time travel: table '{}' is not an Iceberg table; time travel is only supported for Iceberg",
                    our_name.leaf()
                ));
            }

            // Load metadata to resolve the version clause
            let metadata = {
                let registry = state
                    .iceberg_catalogs
                    .read()
                    .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
                let entry = registry.get(&target.catalog)?;
                let loaded = crate::connector::iceberg::catalog::load_table(
                    &entry,
                    &target.namespace,
                    &target.table,
                )?;
                loaded.table.metadata().clone()
            };

            let fqn = format!("{}.{}.{}", target.catalog, target.namespace, target.table);
            let binding = resolve_read_binding(&version_clause, &metadata, &fqn)?;
            let snapshot_id = binding.snapshot_id;

            // Build and register the synthetic table def
            let synthetic_table_name = format!("{}__at_{}", target.table, snapshot_id);
            {
                let connectors = state
                    .connectors
                    .read()
                    .expect("standalone connector registry read lock")
                    .clone();
                let (table_def, _) = crate::connector::iceberg::provider::load_table_def_at(
                    state.connector_control.as_ref(),
                    connector_context.clone(),
                    &target.catalog,
                    &target.namespace,
                    &target.table,
                    Some(snapshot_id),
                    false,
                )?;
                // Build a new TableDef with the synthetic name
                let synthetic_def = TableDef {
                    name: synthetic_table_name.clone(),
                    ..table_def
                };
                register_local_table_registration(state, &target.namespace, synthetic_def)?;
            }

            // Rewrite the AST node in-place:
            // - Set alias to original table name if user didn't specify one
            // - Replace name with the synthetic name resolved against the target namespace
            // - version is already cleared (we took it above)
            if alias.is_none() {
                // Infer the original table alias from the last non-catalog part of the name
                let original_leaf = our_name.leaf().to_string();
                *alias = Some(sqlparser::ast::TableAlias {
                    name: sqlparser::ast::Ident::new(original_leaf),
                    columns: vec![],
                    explicit: false,
                });
            }

            // Replace with an explicit default_catalog-qualified synthetic
            // name. The synthetic table is registered in the local
            // InMemoryCatalog, and the catalog prefix prevents a session-level
            // Iceberg current catalog from routing `db.synthetic` back through
            // the Iceberg CatalogMgr entry.
            *name = sqlparser::ast::ObjectName(vec![
                sqlparser::ast::ObjectNamePart::Identifier(sqlparser::ast::Ident::new(
                    "default_catalog",
                )),
                sqlparser::ast::ObjectNamePart::Identifier(sqlparser::ast::Ident::new(
                    target.namespace.clone(),
                )),
                sqlparser::ast::ObjectNamePart::Identifier(sqlparser::ast::Ident::new(
                    synthetic_table_name,
                )),
            ]);

            Ok(())
        }
        sqlparser::ast::TableFactor::Table { .. } => Ok(()),
        sqlparser::ast::TableFactor::Derived { subquery, .. } => rewrite_time_travel_in_set_expr(
            state,
            current_catalog,
            current_database,
            subquery.body.as_mut(),
            connector_context,
        ),
        _ => Ok(()),
    }
}

/// Materialize a single external connector table into the standalone in-memory
/// catalog so that statement paths which do not run through the SELECT
/// query-prep flow (e.g. `ANALYZE TABLE` / `ANALYZE FULL TABLE`) can still
/// resolve its schema.
///
/// Already-materialized non-Iceberg sources are a no-op here. Unlike the
/// best-effort query-prep loop, an Iceberg load failure is surfaced as an
/// error: the table was named explicitly by the statement, so an unresolvable
/// name is a real error.
pub(crate) fn materialize_external_schema_table_for_statement(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    name: &ObjectName,
) -> Result<(), String> {
    let target = resolve_table_target(state, name, current_catalog, current_database)?;
    if target.backend_name != "iceberg" {
        // Non-Iceberg sources are already represented in the logical catalog.
        return Ok(());
    }
    // Synthetic time-travel tables live only in the in-memory catalog and are
    // unknown to the iceberg backend; never attempt to reload them.
    if is_synthetic_time_travel_table(&target.table) {
        return Ok(());
    }

    let connectors = state
        .connectors
        .read()
        .expect("standalone connector registry read lock")
        .clone();

    {
        let registry = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        let entry = registry.get(&target.catalog)?;
        entry.invalidate_table_cache(&target.namespace, &target.table);
    }
    drop_local_table_registration_if_exists(state, &target.namespace, &target.table)?;

    let (mut table_def, _) = crate::connector::iceberg::provider::load_schema_table_def(
        state.connector_control.as_ref(),
        crate::connector::connector_request_context(
            None,
            std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        )?,
        &target.catalog,
        &target.namespace,
        &target.table,
    )
    .map_err(|err| {
        format!(
            "load iceberg table {}.{}.{} failed: {err}",
            target.catalog, target.namespace, target.table
        )
    })?;
    table_def.name = target.table;
    register_local_table_registration(state, &target.namespace, table_def)
}

/// Returns true if `table_name` was produced by the time-travel rewriter.
/// Synthetic names follow the pattern `<original_table>__at_<snapshot_id>`
/// where `snapshot_id` is a decimal integer (i64).
fn is_synthetic_time_travel_table(table_name: &str) -> bool {
    if let Some(at_pos) = table_name.rfind("__at_") {
        let suffix = &table_name[at_pos + "__at_".len()..];
        !suffix.is_empty() && suffix.chars().all(|c| c.is_ascii_digit() || c == '-')
    } else {
        false
    }
}

fn register_local_table_registration(
    state: &Arc<StandaloneState>,
    namespace: &str,
    table_def: TableDef,
) -> Result<(), String> {
    let mut guard = state
        .catalog_service
        .local()
        .write()
        .expect("catalog service local write lock");
    guard.create_database(namespace).ok();
    guard
        .register(namespace, table_def)
        .map_err(|e| format!("register local table metadata: {e}"))
}

/// Register a synthetic `TableDef` into the standalone in-memory catalog so a
/// generated query can reference it by name (e.g. a single-file
/// `ExplicitFiles`-bound scan used by distributed COW UPDATE rewrites). Mirrors
/// the time-travel synthetic-table registration. Callers are responsible for
/// dropping the table via [`drop_local_table_registration_if_exists`] once the
/// query has run.
pub(crate) fn register_synthetic_table_for_query(
    state: &Arc<StandaloneState>,
    namespace: &str,
    table_def: TableDef,
) -> Result<(), String> {
    register_local_table_registration(state, namespace, table_def)
}

pub(crate) fn drop_local_table_registration_if_exists(
    state: &Arc<StandaloneState>,
    namespace: &str,
    table: &str,
) -> Result<(), String> {
    let mut guard = state
        .catalog_service
        .local()
        .write()
        .map_err(|e| format!("standalone catalog write lock: {e}"))?;
    match guard.drop_table(namespace, table) {
        Ok(()) => Ok(()),
        Err(err) if err.contains("unknown") => Ok(()),
        Err(err) => Err(format!("drop local table metadata: {err}")),
    }
}

/// IVM-A1 helper: build a local planner-catalog-compatible `TableDef` for the
/// base table of an MV refresh without registering any data files.
/// Advertises Iceberg v3 row-lineage virtual columns (`_row_id`, etc.) so
/// the analyzer can resolve apply-key references; the actual per-snapshot
/// files come from the `IcebergDeltaScan` operator at runtime.
pub(crate) fn build_iceberg_table_def_for_delta_scan(
    state: &Arc<StandaloneState>,
    catalog_name: &str,
    namespace: &str,
    table_name: &str,
) -> Result<TableDef, String> {
    let entry = {
        let registry = state
            .iceberg_catalogs
            .read()
            .expect("iceberg registry read lock");
        registry.get(catalog_name)?
    };
    let loaded = crate::connector::iceberg::catalog::load_table(&entry, namespace, table_name)?;
    crate::connector::iceberg::catalog::build_iceberg_table_def_for_delta_scan(
        catalog_name,
        namespace,
        table_name,
        loaded,
    )
}

pub(crate) fn build_iceberg_table_def_with_files(
    state: &Arc<StandaloneState>,
    catalog_name: &str,
    namespace: &str,
    table_name: &str,
    data_files: Vec<IcebergFileForQuery>,
) -> Result<TableDef, String> {
    let entry = {
        let registry = state
            .iceberg_catalogs
            .read()
            .expect("iceberg registry read lock");
        registry.get(catalog_name)?
    };
    let loaded = crate::connector::iceberg::catalog::load_table(&entry, namespace, table_name)?;
    let data_files = data_files
        .into_iter()
        .map(
            |file| crate::connector::iceberg::catalog::registry::DataFileWithStats {
                path: file.path,
                size: file.size,
                record_count: file.record_count,
                column_stats: None,
                partition_spec_id: file.partition_spec_id,
                partition_key: file.partition_key,
                partition_values: None,
                manifest_path: None,
                partition_field_values: vec![],
                first_row_id: file.first_row_id,
                data_sequence_number: file.data_sequence_number,
                delete_files: vec![],
            },
        )
        .collect();
    crate::connector::iceberg::catalog::build_iceberg_table_def_with_files(
        &entry,
        catalog_name,
        namespace,
        table_name,
        loaded,
        data_files,
    )
}

pub(crate) fn build_iceberg_delta_table_def_with_files(
    entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    catalog_name: &str,
    namespace: &str,
    table_name: &str,
    loaded: crate::connector::iceberg::catalog::IcebergLoadedTable,
    data_files: Vec<IcebergFileForQuery>,
) -> Result<TableDef, String> {
    let change_ops = validate_delta_file_change_ops(&data_files)?;
    let data_files = iceberg_files_for_query_to_stats(data_files);
    let mut table_def = crate::connector::iceberg::catalog::build_iceberg_table_def_with_files(
        entry,
        catalog_name,
        namespace,
        table_name,
        loaded,
        data_files,
    )?;
    stamp_delta_table_def_change_ops(&mut table_def, &change_ops)?;
    Ok(table_def)
}

fn iceberg_files_for_query_to_stats(
    data_files: Vec<IcebergFileForQuery>,
) -> Vec<crate::connector::iceberg::catalog::registry::DataFileWithStats> {
    data_files
        .into_iter()
        .map(
            |file| crate::connector::iceberg::catalog::registry::DataFileWithStats {
                path: file.path,
                size: file.size,
                record_count: file.record_count,
                column_stats: None,
                partition_spec_id: file.partition_spec_id,
                partition_key: file.partition_key,
                partition_values: None,
                manifest_path: None,
                partition_field_values: vec![],
                first_row_id: file.first_row_id,
                data_sequence_number: file.data_sequence_number,
                delete_files: vec![],
            },
        )
        .collect()
}

fn validate_delta_file_change_ops(data_files: &[IcebergFileForQuery]) -> Result<Vec<i8>, String> {
    data_files
        .iter()
        .enumerate()
        .map(|(idx, file)| {
            let op = file.change_op.ok_or_else(|| {
                format!(
                    "iceberg delta source file {} ({}) missing {}",
                    idx,
                    file.path,
                    crate::exec::change_op::CHANGE_OP_COLUMN
                )
            })?;
            crate::exec::change_op::validate_change_op_value(op)?;
            Ok(op)
        })
        .collect()
}

fn stamp_delta_table_def_change_ops(
    table_def: &mut TableDef,
    change_ops: &[i8],
) -> Result<(), String> {
    if table_def.columns.iter().any(|col| {
        col.name
            .eq_ignore_ascii_case(crate::exec::change_op::CHANGE_OP_COLUMN)
    }) {
        return Err(format!(
            "iceberg delta source base table already has reserved column {}",
            crate::exec::change_op::CHANGE_OP_COLUMN
        ));
    }
    if table_def
        .iceberg_row_lineage_metadata_columns
        .iter()
        .any(|col| {
            col.name
                .eq_ignore_ascii_case(crate::exec::change_op::CHANGE_OP_COLUMN)
        })
    {
        return Err(format!(
            "iceberg delta source metadata already contains reserved column {}",
            crate::exec::change_op::CHANGE_OP_COLUMN
        ));
    }

    let field = crate::exec::change_op::change_op_field();
    table_def
        .iceberg_row_lineage_metadata_columns
        .push(ColumnDef {
            name: field.name().clone(),
            data_type: field.data_type().clone(),
            nullable: field.is_nullable(),
            write_default: None,
            logical_type: None,
        });

    let ScanSource::IcebergDataFiles { files, .. } = &mut table_def.source else {
        return Err(
            "iceberg delta source requires Iceberg data-file storage for synthetic files"
                .to_string(),
        );
    };
    if files.len() != change_ops.len() {
        return Err(format!(
            "iceberg delta source file count mismatch: table storage has {}, input has {}",
            files.len(),
            change_ops.len()
        ));
    }
    for (file, op) in files.iter_mut().zip(change_ops.iter().copied()) {
        file.ivm_change_op = Some(op);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::connector::iceberg::scan_model::{IcebergSchemaDef, IcebergTableInfo};
    use crate::engine::query_prep::IcebergFileForQuery;
    use crate::sql::planner::table::{ScanSource, TableDef};

    fn test_iceberg_table_info() -> IcebergTableInfo {
        IcebergTableInfo {
            catalog: "test_catalog".to_string(),
            namespace: "test_db".to_string(),
            table: "test_table".to_string(),
            table_uuid: Some("00000000-0000-0000-0000-000000000001".to_string()),
            current_snapshot_id: Some(7),
            schema_id: 1,
            location: "file:///tmp/test_table".to_string(),
            schema: IcebergSchemaDef { fields: vec![] },
            serialized_metadata: None,
            serialized_metadata_rows: None,
        }
    }

    fn table_def_with_binding(
        table: &crate::connector::backend::ResolvedTable,
        binding: crate::connector::iceberg::scan_model::IcebergDataFileBinding,
    ) -> TableDef {
        let mut iceberg = test_iceberg_table_info();
        iceberg.catalog = table.catalog.clone();
        iceberg.namespace = table.namespace.clone();
        iceberg.table = table.table.clone();
        TableDef {
            name: table.table.clone(),
            columns: table.columns.clone(),
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::IcebergDataFiles {
                table: iceberg,
                files: vec![],
                cloud_properties: Default::default(),
                binding,
            },
        }
    }

    #[test]
    fn synthetic_query_table_registration_is_explicitly_local_and_scoped() {
        let state = std::sync::Arc::new(crate::engine::StandaloneState::default());
        let table = crate::connector::backend::ResolvedTable {
            catalog: "ice".to_string(),
            namespace: "scratch".to_string(),
            table: "rewrite_piece".to_string(),
            columns: vec![],
        };
        let table_def = table_def_with_binding(
            &table,
            crate::connector::iceberg::scan_model::IcebergDataFileBinding::ExplicitFiles,
        );

        super::register_synthetic_table_for_query(&state, "scratch", table_def)
            .expect("register synthetic table");

        let registered = state
            .catalog_service
            .local()
            .read()
            .expect("catalog read lock")
            .get("scratch", "rewrite_piece")
            .expect("synthetic table is visible in local catalog");
        assert_eq!(registered.name, "rewrite_piece");

        super::drop_local_table_registration_if_exists(&state, "scratch", "rewrite_piece")
            .expect("drop synthetic table");
        assert!(
            state
                .catalog_service
                .local()
                .read()
                .expect("catalog read lock")
                .get("scratch", "rewrite_piece")
                .is_err(),
            "synthetic table must not outlive the scoped query"
        );
        super::drop_local_table_registration_if_exists(&state, "scratch", "rewrite_piece")
            .expect("drop is idempotent for scoped cleanup");
    }

    fn parse_query_for_table_names(sql: &str) -> sqlparser::ast::Query {
        let stmt = crate::sql::parser::parse_sql_raw(sql).expect("parse sql");
        let sqlparser::ast::Statement::Query(query) = stmt else {
            panic!("expected query statement");
        };
        *query
    }

    fn file(change_op: Option<i8>) -> IcebergFileForQuery {
        IcebergFileForQuery {
            path: "file:///tmp/data.parquet".to_string(),
            size: 10,
            record_count: Some(1),
            partition_spec_id: None,
            partition_key: None,
            first_row_id: None,
            data_sequence_number: None,
            change_op,
            row_id_allow_list: None,
        }
    }

    #[test]
    fn delta_table_builder_rejects_untagged_file() {
        let err = super::validate_delta_file_change_ops(&[file(None)])
            .expect_err("untagged delta file must fail");

        assert!(err.contains("__change_op"));
        assert!(err.contains("missing"));
    }

    #[test]
    fn delta_table_builder_rejects_invalid_change_op() {
        let err = super::validate_delta_file_change_ops(&[file(Some(0))])
            .expect_err("invalid delta file must fail");

        assert!(err.contains("__change_op"));
        assert!(err.contains("invalid value 0"));
    }

    #[test]
    fn delta_table_builder_stamps_s3_files_and_adds_virtual_column() {
        let mut table_def = TableDef {
            name: "t".to_string(),
            columns: vec![],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::IcebergDataFiles {
                table: test_iceberg_table_info(),
                files: vec![crate::connector::iceberg::scan_model::IcebergDataFileInfo {
                    path: "file:///tmp/data.parquet".to_string(),
                    size: 10,
                    row_count: Some(1),
                    column_stats: None,
                    partition_spec_id: None,
                    partition_key: None,
                    first_row_id: None,
                    data_sequence_number: None,
                    ivm_change_op: None,
                    included_positions: None,
                    delete_files: vec![],
                    manifest_path: None,
                    partition_values: vec![],
                }],
                cloud_properties: Default::default(),
                binding:
                    crate::connector::iceberg::scan_model::IcebergDataFileBinding::ExplicitFiles,
            },
        };

        super::stamp_delta_table_def_change_ops(&mut table_def, &[1]).expect("stamp");

        assert_eq!(
            table_def
                .iceberg_row_lineage_metadata_columns
                .iter()
                .map(|col| (col.name.as_str(), &col.data_type, col.nullable))
                .collect::<Vec<_>>(),
            vec![("__change_op", &arrow::datatypes::DataType::Int8, false)]
        );
        let ScanSource::IcebergDataFiles { files, .. } = &table_def.source else {
            panic!("expected s3 parquet storage");
        };
        assert_eq!(files[0].ivm_change_op, Some(1));
    }

    #[test]
    fn delta_table_builder_preserves_row_lineage_metadata_and_adds_change_op() {
        let mut table_def = TableDef {
            name: "t".to_string(),
            columns: vec![],
            iceberg_row_lineage_metadata_columns: vec![
                novarocks_catalog::schema::ColumnDef {
                    name: "_file".to_string(),
                    data_type: arrow::datatypes::DataType::Utf8,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                },
                novarocks_catalog::schema::ColumnDef {
                    name: "_pos".to_string(),
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                },
                novarocks_catalog::schema::ColumnDef {
                    name: "_row_id".to_string(),
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                },
                novarocks_catalog::schema::ColumnDef {
                    name: "_last_updated_sequence_number".to_string(),
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                },
            ],
            source: ScanSource::IcebergDataFiles {
                table: test_iceberg_table_info(),
                files: vec![crate::connector::iceberg::scan_model::IcebergDataFileInfo {
                    path: "file:///tmp/data.parquet".to_string(),
                    size: 10,
                    row_count: Some(1),
                    column_stats: None,
                    partition_spec_id: None,
                    partition_key: None,
                    first_row_id: None,
                    data_sequence_number: None,
                    ivm_change_op: None,
                    included_positions: None,
                    delete_files: vec![],
                    manifest_path: None,
                    partition_values: vec![],
                }],
                cloud_properties: Default::default(),
                binding:
                    crate::connector::iceberg::scan_model::IcebergDataFileBinding::ExplicitFiles,
            },
        };

        super::stamp_delta_table_def_change_ops(&mut table_def, &[-1]).expect("stamp");

        assert_eq!(
            table_def
                .iceberg_row_lineage_metadata_columns
                .iter()
                .map(|col| (col.name.as_str(), &col.data_type, col.nullable))
                .collect::<Vec<_>>(),
            vec![
                ("_file", &arrow::datatypes::DataType::Utf8, false),
                ("_pos", &arrow::datatypes::DataType::Int64, false),
                ("_row_id", &arrow::datatypes::DataType::Int64, false),
                (
                    "_last_updated_sequence_number",
                    &arrow::datatypes::DataType::Int64,
                    false,
                ),
                ("__change_op", &arrow::datatypes::DataType::Int8, false),
            ]
        );
        let ScanSource::IcebergDataFiles { files, .. } = &table_def.source else {
            panic!("expected s3 parquet storage");
        };
        assert_eq!(files[0].ivm_change_op, Some(-1));
    }

    #[test]
    fn delta_table_builder_accepts_empty_iceberg_storage() {
        // The IVM-A1 delta source `stamp_delta_table_def_change_ops`
        // requires the base table to be backed by `IcebergDataFiles`
        // (real or synthetic). An empty Iceberg snapshot legitimately
        // produces `IcebergDataFiles { files: vec![] }` (see
        // `connector/iceberg/catalog/backend.rs::empty_iceberg_scan_source`);
        // ensure that path round-trips correctly when stamping with an
        // empty change-op slice.
        let mut table_def = TableDef {
            name: "t".to_string(),
            columns: vec![],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::IcebergDataFiles {
                table: test_iceberg_table_info(),
                files: Vec::new(),
                cloud_properties: Default::default(),
                binding:
                    crate::connector::iceberg::scan_model::IcebergDataFileBinding::ExplicitFiles,
            },
        };

        super::stamp_delta_table_def_change_ops(&mut table_def, &[])
            .expect("stamp empty delta over empty iceberg storage");

        assert_eq!(
            table_def
                .iceberg_row_lineage_metadata_columns
                .iter()
                .map(|col| (col.name.as_str(), &col.data_type, col.nullable))
                .collect::<Vec<_>>(),
            vec![("__change_op", &arrow::datatypes::DataType::Int8, false)]
        );
        let ScanSource::IcebergDataFiles { files, .. } = &table_def.source else {
            panic!("expected empty delta to use s3 parquet storage");
        };
        assert!(files.is_empty());
    }
}
