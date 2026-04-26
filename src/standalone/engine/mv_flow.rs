//! Materialized-view statement dispatch through `MvBackend`.

use std::sync::Arc;

use crate::runtime::query_result::QueryResult;
use crate::sql::parser::ast::{
    CreateMaterializedViewStmt, DropMaterializedViewStmt, RefreshMaterializedViewStmt,
    ShowMaterializedViewsStmt,
};
use crate::standalone::engine::catalog::{InMemoryCatalog, normalize_identifier};
use crate::standalone::engine::sqlparse::statement::{
    extract_three_part_table_refs, strip_catalog_from_three_part_names,
};
use crate::standalone::engine::{StandaloneState, StatementResult, execute_query};

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
        crate::standalone::engine::query_prep::refresh_external_tables_for_query(
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

    let table_def = crate::standalone::engine::query_prep::build_iceberg_table_def_with_files(
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
