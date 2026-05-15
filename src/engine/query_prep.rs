//! Query preparation that materializes external connector tables into the
//! standalone in-memory catalog before planning.

use std::sync::Arc;

use crate::engine::StandaloneState;
use crate::engine::StatementResult;
use crate::engine::backend_resolver::resolve_table_target;
use crate::engine::build_string_query_result;
use crate::engine::statement::parse_add_files_sql;
use crate::sql::analyzer::iceberg_ref::resolve_read_binding;
use crate::sql::catalog::{ColumnDef, TableDef, TableStorage};
use crate::sql::parser::ast::ObjectName;
use crate::sql::parser::query_refs::{
    extract_table_names_from_query, extract_three_part_table_refs,
};

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
    let count = crate::connector::iceberg::catalog::add_files::add_files(
        &entry,
        &namespace,
        &table_name,
        &s3_path,
    )?;
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
///    in-memory catalog under the name `<table>__at_<snapshot_id>`.
/// 3. Rewrite the `TableFactor::Table`:
///    - Replace `name` with the synthetic 1-part name.
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
) -> Result<(), String> {
    let (catalog_backend, table_source) = {
        let registry = state
            .connectors
            .read()
            .expect("standalone connector registry read lock");
        (
            registry.catalog_backend("iceberg")?,
            registry.table_source("iceberg")?,
        )
    };

    // Walk CTEs
    if let Some(with) = &mut query.with {
        for cte in &mut with.cte_tables {
            rewrite_time_travel_in_set_expr(
                state,
                current_catalog,
                current_database,
                &catalog_backend,
                &table_source,
                cte.query.body.as_mut(),
            )?;
        }
    }
    rewrite_time_travel_in_set_expr(
        state,
        current_catalog,
        current_database,
        &catalog_backend,
        &table_source,
        query.body.as_mut(),
    )
}

fn rewrite_time_travel_in_set_expr(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    catalog_backend: &Arc<dyn crate::connector::backend::CatalogBackend>,
    table_source: &Arc<dyn crate::connector::backend::TableSource>,
    expr: &mut sqlparser::ast::SetExpr,
) -> Result<(), String> {
    match expr {
        sqlparser::ast::SetExpr::Select(select) => {
            for tw in &mut select.from {
                rewrite_time_travel_in_factor(
                    state,
                    current_catalog,
                    current_database,
                    catalog_backend,
                    table_source,
                    &mut tw.relation,
                )?;
                for join in &mut tw.joins {
                    rewrite_time_travel_in_factor(
                        state,
                        current_catalog,
                        current_database,
                        catalog_backend,
                        table_source,
                        &mut join.relation,
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
                catalog_backend,
                table_source,
                left.as_mut(),
            )?;
            rewrite_time_travel_in_set_expr(
                state,
                current_catalog,
                current_database,
                catalog_backend,
                table_source,
                right.as_mut(),
            )
        }
        sqlparser::ast::SetExpr::Query(q) => rewrite_time_travel_in_set_expr(
            state,
            current_catalog,
            current_database,
            catalog_backend,
            table_source,
            q.body.as_mut(),
        ),
        _ => Ok(()),
    }
}

fn rewrite_time_travel_in_factor(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    catalog_backend: &Arc<dyn crate::connector::backend::CatalogBackend>,
    table_source: &Arc<dyn crate::connector::backend::TableSource>,
    factor: &mut sqlparser::ast::TableFactor,
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
                        && !ref_name.is_empty() {
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
                let resolved = catalog_backend.load_table(
                    &target.catalog,
                    &target.namespace,
                    &target.table,
                )?;
                let table_def = table_source.build_table_def_at(&resolved, Some(snapshot_id))?;
                // Build a new TableDef with the synthetic name
                let synthetic_def = TableDef {
                    name: synthetic_table_name.clone(),
                    ..table_def
                };
                register_external_table(state, &target.namespace, synthetic_def)?;
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

            // Replace with a 2-part namespace-qualified synthetic name so the
            // rewritten query resolves correctly even when `current_database` is
            // empty or does not match the table's namespace.  The analyzer
            // accepts `<namespace>.<table>` 2-part references in the same way it
            // handles non-time-travel tables found via register_external_tables.
            *name = sqlparser::ast::ObjectName(vec![
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
            catalog_backend,
            table_source,
            subquery.body.as_mut(),
        ),
        _ => Ok(()),
    }
}

pub(crate) fn register_external_tables_for_query(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &sqlparser::ast::Query,
) -> Result<(), String> {
    register_external_tables_for_query_impl(state, current_catalog, current_database, query, false)
}

pub(crate) fn refresh_external_tables_for_query(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &sqlparser::ast::Query,
) -> Result<(), String> {
    register_external_tables_for_query_impl(state, current_catalog, current_database, query, true)
}

fn register_external_tables_for_query_impl(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &sqlparser::ast::Query,
    force_refresh: bool,
) -> Result<(), String> {
    let mut names = query_table_names(current_catalog, query);
    if names.is_empty() {
        return Ok(());
    }
    names.sort_by(|left, right| left.parts.cmp(&right.parts));
    names.dedup_by(|left, right| left.parts == right.parts);

    let (catalog, source) = {
        let registry = state
            .connectors
            .read()
            .expect("standalone connector registry read lock");
        (
            registry.catalog_backend("iceberg")?,
            registry.table_source("iceberg")?,
        )
    };

    for name in names {
        let Ok(target) = resolve_table_target(state, &name, current_catalog, current_database)
        else {
            continue;
        };
        if target.backend_name != "iceberg" {
            let local = state.catalog.read().expect("catalog read lock");
            if !force_refresh && local.get(&target.namespace, &target.table).is_ok() {
                continue;
            }
            continue;
        }
        // Skip synthetic time-travel tables registered by `rewrite_time_travel_refs`
        // (name pattern: `<table>__at_<snapshot_id>`).  These live only in the
        // InMemory catalog and must not be dropped or re-looked-up from the iceberg
        // catalog backend, which doesn't know about them.
        if is_synthetic_time_travel_table(&target.table) {
            continue;
        }
        {
            let registry = state
                .iceberg_catalogs
                .read()
                .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
            let entry = registry.get(&target.catalog)?;
            entry.invalidate_table_cache(&target.namespace, &target.table);
        }
        drop_registered_external_table(state, &target.namespace, &target.table)?;

        let resolved = catalog
            .load_table(&target.catalog, &target.namespace, &target.table)
            .map_err(|err| {
                format!(
                    "load iceberg table {}.{}.{} failed: {err}",
                    target.catalog, target.namespace, target.table
                )
            })?;
        let table_def = source.build_table_def(&resolved)?;
        register_external_table(state, &target.namespace, table_def)?;
    }

    Ok(())
}

fn query_table_names(
    current_catalog: Option<&str>,
    query: &sqlparser::ast::Query,
) -> Vec<ObjectName> {
    // Always collect fully-qualified 3-part references (including 4-part
    // __nr_meta_*__ forms reduced to 3-part). They register against the
    // catalog encoded in the name regardless of session catalog.
    let mut names: Vec<ObjectName> = extract_three_part_table_refs(query)
        .into_iter()
        .map(|(catalog, namespace, table)| ObjectName {
            parts: vec![catalog, namespace, table],
        })
        .collect();

    // When the session has a current catalog, also collect 1-part names so
    // that unqualified references in the query register through the session
    // catalog + current database.
    if current_catalog.is_some() {
        for table in extract_table_names_from_query(query) {
            names.push(ObjectName { parts: vec![table] });
        }
    }

    // Stable de-duplication on (parts) so the downstream registration loop
    // does not redundantly hit the iceberg backend for the same target.
    names.sort_by(|a, b| a.parts.cmp(&b.parts));
    names.dedup_by(|a, b| a.parts == b.parts);
    names
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

fn register_external_table(
    state: &Arc<StandaloneState>,
    namespace: &str,
    table_def: TableDef,
) -> Result<(), String> {
    let mut guard = state.catalog.write().expect("catalog write lock");
    guard.create_database(namespace).ok();
    guard
        .register(namespace, table_def)
        .map_err(|e| format!("register external table: {e}"))
}

pub(crate) fn drop_registered_external_table(
    state: &Arc<StandaloneState>,
    namespace: &str,
    table: &str,
) -> Result<(), String> {
    let mut guard = state
        .catalog
        .write()
        .map_err(|e| format!("standalone catalog write lock: {e}"))?;
    match guard.drop_table(namespace, table) {
        Ok(()) => Ok(()),
        Err(err) if err.contains("unknown") => Ok(()),
        Err(err) => Err(format!("drop registered external table: {err}")),
    }
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
        &entry, namespace, table_name, loaded, data_files,
    )
}

pub(crate) fn build_iceberg_delta_table_def_with_files(
    entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    namespace: &str,
    table_name: &str,
    loaded: crate::connector::iceberg::catalog::IcebergLoadedTable,
    data_files: Vec<IcebergFileForQuery>,
) -> Result<TableDef, String> {
    let change_ops = validate_delta_file_change_ops(&data_files)?;
    let data_files = iceberg_files_for_query_to_stats(data_files);
    let mut table_def = crate::connector::iceberg::catalog::build_iceberg_table_def_with_files(
        entry, namespace, table_name, loaded, data_files,
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
        });

    if change_ops.is_empty() && matches!(table_def.storage, TableStorage::LocalParquetFile { .. }) {
        table_def.storage = TableStorage::S3ParquetFiles {
            files: Vec::new(),
            cloud_properties: Default::default(),
        };
    }

    let TableStorage::S3ParquetFiles { files, .. } = &mut table_def.storage else {
        return Err(
            "iceberg delta source requires S3 parquet file storage for synthetic files".to_string(),
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
    use crate::engine::query_prep::IcebergFileForQuery;
    use crate::sql::catalog::{TableDef, TableStorage};

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
            iceberg_table: None,
            storage: TableStorage::S3ParquetFiles {
                files: vec![crate::sql::catalog::S3FileInfo {
                    path: "file:///tmp/data.parquet".to_string(),
                    size: 10,
                    row_count: Some(1),
                    column_stats: None,
                    partition_spec_id: None,
                    partition_key: None,
                    first_row_id: None,
                    data_sequence_number: None,
                    ivm_change_op: None,
                    delete_files: vec![],
                    manifest_path: None,
                    partition_values: vec![],
                }],
                cloud_properties: Default::default(),
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
        let TableStorage::S3ParquetFiles { files, .. } = &table_def.storage else {
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
                crate::sql::catalog::ColumnDef {
                    name: "_file".to_string(),
                    data_type: arrow::datatypes::DataType::Utf8,
                    nullable: false,
                    write_default: None,
                },
                crate::sql::catalog::ColumnDef {
                    name: "_pos".to_string(),
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: false,
                    write_default: None,
                },
                crate::sql::catalog::ColumnDef {
                    name: "_row_id".to_string(),
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: false,
                    write_default: None,
                },
                crate::sql::catalog::ColumnDef {
                    name: "_last_updated_sequence_number".to_string(),
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: false,
                    write_default: None,
                },
            ],
            iceberg_table: None,
            storage: TableStorage::S3ParquetFiles {
                files: vec![crate::sql::catalog::S3FileInfo {
                    path: "file:///tmp/data.parquet".to_string(),
                    size: 10,
                    row_count: Some(1),
                    column_stats: None,
                    partition_spec_id: None,
                    partition_key: None,
                    first_row_id: None,
                    data_sequence_number: None,
                    ivm_change_op: None,
                    delete_files: vec![],
                    manifest_path: None,
                    partition_values: vec![],
                }],
                cloud_properties: Default::default(),
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
        let TableStorage::S3ParquetFiles { files, .. } = &table_def.storage else {
            panic!("expected s3 parquet storage");
        };
        assert_eq!(files[0].ivm_change_op, Some(-1));
    }

    #[test]
    fn delta_table_builder_accepts_empty_local_storage() {
        let mut table_def = TableDef {
            name: "t".to_string(),
            columns: vec![],
            iceberg_row_lineage_metadata_columns: vec![],
            iceberg_table: None,
            storage: TableStorage::LocalParquetFile {
                path: std::env::temp_dir().join("empty-delta.parquet"),
            },
        };

        super::stamp_delta_table_def_change_ops(&mut table_def, &[]).expect("stamp empty delta");

        assert_eq!(
            table_def
                .iceberg_row_lineage_metadata_columns
                .iter()
                .map(|col| (col.name.as_str(), &col.data_type, col.nullable))
                .collect::<Vec<_>>(),
            vec![("__change_op", &arrow::datatypes::DataType::Int8, false)]
        );
        let TableStorage::S3ParquetFiles { files, .. } = &table_def.storage else {
            panic!("expected empty delta to use s3 parquet storage");
        };
        assert!(files.is_empty());
    }
}
