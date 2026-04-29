//! Materialized-view statement dispatch through `MvBackend`.

use std::sync::Arc;

use crate::engine::catalog::{InMemoryCatalog, normalize_identifier};
use crate::engine::{StandaloneState, StatementResult, execute_query};
use crate::runtime::query_result::QueryResult;
use crate::sql::parser::ast::{
    CreateMaterializedViewStmt, DropMaterializedViewStmt, RefreshMaterializedViewStmt,
    ShowMaterializedViewsStmt,
};
use crate::sql::parser::query_refs::{
    extract_three_part_table_refs, strip_catalog_from_three_part_names,
};

fn mv_backend(
    state: &Arc<StandaloneState>,
) -> Result<Arc<dyn crate::connector::backend::MvBackend>, String> {
    state
        .connectors
        .read()
        .expect("connector registry read")
        .mv_backend("managed")
}

pub(crate) fn create_mv(
    state: &Arc<StandaloneState>,
    db: &str,
    stmt: &CreateMaterializedViewStmt,
) -> Result<StatementResult, String> {
    mv_backend(state)?.create_mv(stmt, db)?;
    Ok(StatementResult::Ok)
}

pub(crate) fn drop_mv(
    state: &Arc<StandaloneState>,
    db: &str,
    stmt: &DropMaterializedViewStmt,
) -> Result<StatementResult, String> {
    mv_backend(state)?.drop_mv(stmt, db)?;
    Ok(StatementResult::Ok)
}

pub(crate) fn refresh_mv(
    state: &Arc<StandaloneState>,
    db: &str,
    stmt: &RefreshMaterializedViewStmt,
) -> Result<StatementResult, String> {
    mv_backend(state)?.refresh_mv(stmt, db)?;
    Ok(StatementResult::Ok)
}

pub(crate) fn list_mvs(
    state: &Arc<StandaloneState>,
    stmt: &ShowMaterializedViewsStmt,
) -> Result<StatementResult, String> {
    let result: QueryResult = mv_backend(state)?.list_mvs(stmt)?;
    Ok(StatementResult::Query(result))
}

pub(crate) fn execute_query_for_mv_refresh(
    state: &Arc<StandaloneState>,
    current_database: &str,
    sql: &str,
) -> Result<QueryResult, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)?;
    let statement = crate::sql::parser::parse_normalized_sql_raw(&normalized)
        .map_err(|e| format!("sql parser error: {e}"))?;
    let sqlparser::ast::Statement::Query(query) = statement else {
        return Err("REFRESH MATERIALIZED VIEW stored SQL must be a SELECT query".to_string());
    };

    let three_parts = extract_three_part_table_refs(&query);
    if !three_parts.is_empty() {
        crate::engine::query_prep::refresh_external_tables_for_query(
            state,
            None,
            current_database,
            &query,
        )?;
    }

    let mut executable = query.as_ref().clone();
    if !three_parts.is_empty() {
        strip_catalog_from_three_part_names(&mut executable);
    }
    let catalog = state.catalog.read().expect("standalone catalog read lock");
    execute_query(
        &executable,
        &catalog,
        current_database,
        state.exchange_port,
        None,
    )
}

fn normalize_incremental_mv_base_ref(
    base_ref: &crate::connector::starrocks::managed::store::IcebergTableRef,
) -> Result<(String, String, String), String> {
    Ok((
        normalize_identifier(&base_ref.catalog)?,
        normalize_identifier(&base_ref.namespace)?,
        normalize_identifier(&base_ref.table)?,
    ))
}

fn extract_three_part_table_ref_occurrences(
    query: &sqlparser::ast::Query,
) -> Vec<(String, String, String)> {
    let mut refs = Vec::new();
    extract_three_part_ref_occurrences_from_set_expr(query.body.as_ref(), &mut refs);
    refs
}

fn extract_three_part_ref_occurrences_from_set_expr(
    expr: &sqlparser::ast::SetExpr,
    refs: &mut Vec<(String, String, String)>,
) {
    match expr {
        sqlparser::ast::SetExpr::Select(select) => {
            for from in &select.from {
                extract_three_part_ref_occurrences_from_factor(&from.relation, refs);
                for join in &from.joins {
                    extract_three_part_ref_occurrences_from_factor(&join.relation, refs);
                }
            }
        }
        sqlparser::ast::SetExpr::SetOperation { left, right, .. } => {
            extract_three_part_ref_occurrences_from_set_expr(left, refs);
            extract_three_part_ref_occurrences_from_set_expr(right, refs);
        }
        sqlparser::ast::SetExpr::Query(query) => {
            extract_three_part_ref_occurrences_from_set_expr(query.body.as_ref(), refs);
        }
        _ => {}
    }
}

fn extract_three_part_ref_occurrences_from_factor(
    factor: &sqlparser::ast::TableFactor,
    refs: &mut Vec<(String, String, String)>,
) {
    match factor {
        sqlparser::ast::TableFactor::Table { name, .. } => {
            let parts: Vec<String> = name
                .0
                .iter()
                .filter_map(|part| match part {
                    sqlparser::ast::ObjectNamePart::Identifier(ident) => {
                        Some(ident.value.to_lowercase())
                    }
                    _ => None,
                })
                .collect();
            if parts.len() == 3 {
                refs.push((parts[0].clone(), parts[1].clone(), parts[2].clone()));
            }
        }
        sqlparser::ast::TableFactor::Derived { subquery, .. } => {
            extract_three_part_ref_occurrences_from_set_expr(subquery.body.as_ref(), refs);
        }
        _ => {}
    }
}

fn validate_incremental_mv_base_ref(
    query: &sqlparser::ast::Query,
    base_ref: &crate::connector::starrocks::managed::store::IcebergTableRef,
) -> Result<(String, String, String), String> {
    let refs = extract_three_part_table_ref_occurrences(query);
    if refs.len() != 1 {
        return Err(format!(
            "incremental MV refresh stored SQL must reference exactly one 3-part Iceberg table, got {}",
            refs.len()
        ));
    }

    let actual = {
        let (catalog, namespace, table) = &refs[0];
        (
            normalize_identifier(catalog).map_err(|e| {
                format!("incremental MV refresh stored SQL has invalid catalog reference: {e}")
            })?,
            normalize_identifier(namespace).map_err(|e| {
                format!("incremental MV refresh stored SQL has invalid namespace reference: {e}")
            })?,
            normalize_identifier(table).map_err(|e| {
                format!("incremental MV refresh stored SQL has invalid table reference: {e}")
            })?,
        )
    };
    let expected = normalize_incremental_mv_base_ref(base_ref)?;
    if actual != expected {
        return Err(format!(
            "incremental MV refresh stored SQL base table mismatch: expected {}.{}.{}, got {}.{}.{}",
            expected.0, expected.1, expected.2, actual.0, actual.1, actual.2
        ));
    }
    Ok(expected)
}

pub(crate) fn execute_query_for_mv_incremental_refresh(
    state: &Arc<StandaloneState>,
    current_database: &str,
    sql: &str,
    base_ref: &crate::connector::starrocks::managed::store::IcebergTableRef,
    delta_files: Vec<(String, i64, Option<i64>)>,
) -> Result<QueryResult, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)?;
    let statement = crate::sql::parser::parse_normalized_sql_raw(&normalized)
        .map_err(|e| format!("sql parser error: {e}"))?;
    let sqlparser::ast::Statement::Query(query) = statement else {
        return Err("REFRESH MATERIALIZED VIEW stored SQL must be a SELECT query".to_string());
    };

    let (catalog_name, namespace, table_name) = validate_incremental_mv_base_ref(&query, base_ref)?;
    let is_s3 = {
        let registry = state
            .iceberg_catalogs
            .read()
            .expect("iceberg registry read lock");
        registry.get(&catalog_name)?.is_s3()
    };
    if !is_s3 && delta_files.len() > 1 {
        return Err(
            "incremental MV refresh over local iceberg supports at most one delta file".to_string(),
        );
    }

    let table_def = crate::engine::query_prep::build_iceberg_table_def_with_files(
        state,
        &catalog_name,
        &namespace,
        &table_name,
        delta_files,
    )?;
    let mut incremental_catalog = InMemoryCatalog::default();
    incremental_catalog.create_database(&namespace)?;
    incremental_catalog
        .register(&namespace, table_def)
        .map_err(|e| format!("register incremental iceberg table: {e}"))?;

    let mut executable = query.as_ref().clone();
    strip_catalog_from_three_part_names(&mut executable);
    execute_query(
        &executable,
        &incremental_catalog,
        current_database,
        state.exchange_port,
        None,
    )
}

/// Run the MV's SELECT statement against a one-shot in-memory catalog
/// where the base table's storage is a single temp parquet file
/// containing the supplied deleted rows. Mirrors the insert-side
/// `execute_query_for_mv_incremental_refresh` but without iceberg-file
/// list construction — the caller has already projected the rows.
///
/// Returns the empty `QueryResult` when `deleted_rows` is empty.
pub(crate) fn execute_query_for_mv_incremental_deletes(
    state: &Arc<StandaloneState>,
    current_database: &str,
    sql: &str,
    base_ref: &crate::connector::starrocks::managed::store::IcebergTableRef,
    deleted_rows: Vec<arrow::record_batch::RecordBatch>,
) -> Result<QueryResult, String> {
    if deleted_rows.is_empty() {
        return Ok(QueryResult::empty());
    }

    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)?;
    let statement = crate::sql::parser::parse_normalized_sql_raw(&normalized)
        .map_err(|e| format!("sql parser error: {e}"))?;
    let sqlparser::ast::Statement::Query(query) = statement else {
        return Err("REFRESH MATERIALIZED VIEW stored SQL must be a SELECT query".to_string());
    };
    let (catalog_name, namespace, table_name) = validate_incremental_mv_base_ref(&query, base_ref)?;

    // Write the deleted rows to a temp parquet file. The temp directory
    // is OS-cleaned (or torn down by the next test run); we don't add
    // explicit cleanup logic — matches register_empty_iceberg_table.
    let dir = std::env::temp_dir().join(format!(
        "novarocks_mv_deletes_{}",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&dir)
        .map_err(|e| format!("create temp dir for delete-side mv refresh: {e}"))?;
    let path = dir.join(format!("{namespace}_{table_name}.parquet"));
    let schema = deleted_rows[0].schema();
    let file = std::fs::File::create(&path)
        .map_err(|e| format!("create temp parquet for delete-side mv refresh: {e}"))?;
    let mut writer = parquet::arrow::ArrowWriter::try_new(file, schema, None)
        .map_err(|e| format!("create temp parquet writer for delete-side mv refresh: {e}"))?;
    for batch in &deleted_rows {
        writer
            .write(batch)
            .map_err(|e| format!("write temp parquet batch for delete-side mv refresh: {e}"))?;
    }
    writer
        .close()
        .map_err(|e| format!("close temp parquet writer for delete-side mv refresh: {e}"))?;

    // Build a TableDef whose storage is the temp parquet file. Reuse
    // build_iceberg_table_def_with_files's column-shape logic by giving
    // it a one-element file list.
    let total_size: i64 = deleted_rows
        .iter()
        .map(|b| {
            // Best-effort byte estimate — used only for cardinality hints.
            // The query path doesn't depend on the exact value.
            let mut bytes: i64 = 0;
            for col in b.columns() {
                bytes += col.get_array_memory_size() as i64;
            }
            bytes
        })
        .sum();
    let total_rows: Option<i64> = Some(deleted_rows.iter().map(|b| b.num_rows() as i64).sum());
    let delete_files = vec![(format!("file://{}", path.display()), total_size, total_rows)];

    let table_def = crate::engine::query_prep::build_iceberg_table_def_with_files(
        state,
        &catalog_name,
        &namespace,
        &table_name,
        delete_files,
    )?;
    let mut incremental_catalog = InMemoryCatalog::default();
    incremental_catalog.create_database(&namespace)?;
    incremental_catalog
        .register(&namespace, table_def)
        .map_err(|e| format!("register delete-side iceberg table: {e}"))?;

    let mut executable = query.as_ref().clone();
    strip_catalog_from_three_part_names(&mut executable);
    execute_query(
        &executable,
        &incremental_catalog,
        current_database,
        state.exchange_port,
        None,
    )
}
