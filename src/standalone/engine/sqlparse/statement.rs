//! DDL/DML statement handlers and sqlparser-AST analysis helpers.
//!
//! Two responsibilities:
//! - Top-level dispatchers (`execute_create_database_statement`,
//!   `execute_insert_statement`, etc.) that route statements to the local
//!   in-memory catalog, the iceberg registry, or the managed lake based on
//!   the parsed name and current catalog/database session context.
//! - Pure sqlparser AST utilities — table-name extraction for iceberg
//!   materialization, three-part name rewrites, and recognition for our
//!   bespoke `CREATE TABLE ... PROPERTIES("path"=...)` and
//!   `ALTER TABLE ... ADD FILES FROM` shorthands.

use std::sync::Arc;

use crate::sql::parser::ast::{
    CreateTableKind, Expr, GenerateSeriesSelect, InsertSource, Literal, ObjectName,
};
use crate::standalone::engine::catalog::normalize_identifier;
use crate::standalone::engine::insert::reorder_insert_rows;
use crate::standalone::engine::name_resolve::{
    resolve_iceberg_namespace_name, resolve_iceberg_table_name, resolve_local_table_name,
};
use crate::standalone::engine::{
    StandaloneState, StatementResult, delete_iceberg_catalog_if_needed,
    delete_iceberg_namespace_if_needed, delete_iceberg_table_if_needed,
    persist_iceberg_namespace_if_needed, persist_iceberg_table_if_needed,
};
use crate::standalone::iceberg::{
    create_namespace as create_iceberg_namespace, create_table as create_iceberg_table,
    drop_namespace as drop_iceberg_namespace, drop_table as drop_iceberg_table,
    insert_rows as insert_iceberg_rows, list_tables as list_iceberg_tables,
    namespace_exists as iceberg_namespace_exists,
};
use crate::standalone::lake::ddl::{
    create_managed_table, drop_managed_database_entry,
    drop_managed_table as drop_managed_lake_table,
    truncate_managed_table as truncate_managed_lake_table,
};

use super::expr::{sqlparser_expr_to_custom_expr, sqlparser_expr_to_literal};
use super::generate_series::{insert_generate_series_rows, parse_generate_series_function_expr};

fn convert_set_expr_to_insert_source(
    body: &sqlparser::ast::SetExpr,
) -> Result<InsertSource, String> {
    use sqlparser::ast as sqlast;
    match body {
        sqlast::SetExpr::Values(values) => {
            let mut rows = Vec::new();
            for row in &values.rows {
                let literal_row: Vec<Literal> = row
                    .iter()
                    .map(sqlparser_expr_to_literal)
                    .collect::<Result<_, _>>()?;
                rows.push(literal_row);
            }
            Ok(InsertSource::Values(rows))
        }
        sqlast::SetExpr::Select(select) => {
            if select.from.is_empty() {
                let row: Vec<Literal> = select
                    .projection
                    .iter()
                    .map(|item| match item {
                        sqlast::SelectItem::UnnamedExpr(expr) => sqlparser_expr_to_literal(expr),
                        _ => Err("INSERT SELECT source only supports unnamed expressions".into()),
                    })
                    .collect::<Result<_, _>>()?;
                Ok(InsertSource::SelectLiteralRow(row))
            } else if select.from.len() == 1 {
                let table_with_joins = &select.from[0];
                if table_with_joins.joins.is_empty() {
                    if let sqlparser::ast::TableFactor::TableFunction {
                        ref expr,
                        ref alias,
                    } = table_with_joins.relation
                    {
                        let (start, end, step) = parse_generate_series_function_expr(expr)?;
                        let column_name = alias
                            .as_ref()
                            .and_then(|a| a.columns.first().map(|c| c.name.value.clone()))
                            .unwrap_or_else(|| "generate_series".to_string());
                        let projection: Vec<Expr> = select
                            .projection
                            .iter()
                            .map(|item| match item {
                                sqlast::SelectItem::UnnamedExpr(expr) => {
                                    sqlparser_expr_to_custom_expr(expr)
                                }
                                _ => {
                                    Err("INSERT SELECT source only supports unnamed expressions"
                                        .into())
                                }
                            })
                            .collect::<Result<_, _>>()?;
                        Ok(InsertSource::GenerateSeriesSelect(GenerateSeriesSelect {
                            column_name,
                            start,
                            end,
                            step,
                            projection,
                        }))
                    } else {
                        Err("unsupported INSERT SELECT source".into())
                    }
                } else {
                    Err("INSERT SELECT with joins is not supported in this path".into())
                }
            } else {
                Err("INSERT SELECT with multiple tables is not supported".into())
            }
        }
        sqlast::SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } => {
            // Only UNION ALL is handled at this layer. UNION (distinct) would
            // need output-level dedup with the target schema's types, which we
            // don't have at parse time.
            if !matches!(op, sqlast::SetOperator::Union) {
                return Err("INSERT SELECT set operation is only UNION ALL here".into());
            }
            if !matches!(
                set_quantifier,
                sqlast::SetQuantifier::All | sqlast::SetQuantifier::AllByName
            ) {
                return Err(
                    "INSERT SELECT UNION requires UNION ALL (UNION/UNION DISTINCT unsupported)"
                        .into(),
                );
            }
            let mut parts = Vec::new();
            flatten_union_all(left, &mut parts)?;
            flatten_union_all(right, &mut parts)?;
            Ok(InsertSource::UnionAll(parts))
        }
        sqlast::SetExpr::Query(query) => convert_set_expr_to_insert_source(query.body.as_ref()),
        _ => Err("unsupported INSERT source".into()),
    }
}

fn flatten_union_all(
    body: &sqlparser::ast::SetExpr,
    out: &mut Vec<InsertSource>,
) -> Result<(), String> {
    use sqlparser::ast as sqlast;
    if let sqlast::SetExpr::SetOperation {
        op: sqlast::SetOperator::Union,
        set_quantifier: sqlast::SetQuantifier::All | sqlast::SetQuantifier::AllByName,
        left,
        right,
    } = body
    {
        flatten_union_all(left, out)?;
        flatten_union_all(right, out)?;
        Ok(())
    } else {
        out.push(convert_set_expr_to_insert_source(body)?);
        Ok(())
    }
}

/// Decide whether an INSERT's source Query should be executed via the full
/// plan pipeline (returning `InsertSource::FromQuery`) instead of being
/// collapsed into literal rows or a generate_series short-form.
///
/// We route via the full pipeline whenever the Query carries clauses the
/// literal fast path can't represent (WITH/ORDER BY/LIMIT/FETCH/locks), or
/// when the body is a SELECT that reads from at least one real relation
/// (i.e. something other than `TABLE(generate_series(...))`).
fn should_route_insert_via_from_query(query: &sqlparser::ast::Query) -> bool {
    if query.with.is_some()
        || query.order_by.is_some()
        || query.limit_clause.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
    {
        return true;
    }
    body_reads_from_real_relation(query.body.as_ref())
}

fn body_reads_from_real_relation(body: &sqlparser::ast::SetExpr) -> bool {
    use sqlparser::ast as sqlast;
    match body {
        sqlast::SetExpr::Select(select) => {
            if select.from.is_empty() {
                return false;
            }
            for table_with_joins in &select.from {
                if !table_with_joins.joins.is_empty() {
                    return true;
                }
                match &table_with_joins.relation {
                    sqlast::TableFactor::TableFunction { .. } => {
                        // generate_series is handled by the literal fast path.
                    }
                    _ => return true,
                }
            }
            false
        }
        sqlast::SetExpr::Query(inner) => should_route_insert_via_from_query(inner.as_ref()),
        sqlast::SetExpr::SetOperation { left, right, .. } => {
            body_reads_from_real_relation(left) || body_reads_from_real_relation(right)
        }
        _ => false,
    }
}

/// Convert a sqlparser INSERT AST to our custom InsertStmt.
/// Used for Iceberg tables which need the custom AST's InsertSource types.
pub(crate) fn convert_sqlparser_insert_to_custom(
    insert: &sqlparser::ast::Insert,
) -> Result<crate::sql::parser::ast::InsertStmt, String> {
    use sqlparser::ast as sqlast;

    let table = match &insert.table {
        sqlast::TableObject::TableName(name) => {
            crate::sql::parser::dialect::convert_object_name(name.clone())?
        }
        other => return Err(format!("unsupported INSERT target: {other}")),
    };
    let columns: Vec<String> = insert.columns.iter().map(|c| c.value.clone()).collect();
    let source_query = insert
        .source
        .as_ref()
        .ok_or_else(|| "INSERT requires a source".to_string())?;
    // If the body is a SELECT that reads from a real relation (not a
    // generate_series table function), or carries a WITH/ORDER BY/LIMIT that
    // the literal fast-path can't express, hand the whole Query to the
    // analyzer/planner/pipeline stack via `FromQuery`. This keeps the INSERT
    // entry point aligned with how StarRocks wraps INSERT ... SELECT as a
    // normal plan with a sink, rather than evaluating SELECT here.
    let source = if should_route_insert_via_from_query(source_query) {
        crate::sql::parser::ast::InsertSource::FromQuery(source_query.clone())
    } else {
        convert_set_expr_to_insert_source(source_query.body.as_ref())?
    };
    Ok(crate::sql::parser::ast::InsertStmt {
        table,
        columns,
        source,
    })
}

// ---------------------------------------------------------------------------
// DDL handlers
// ---------------------------------------------------------------------------

pub(crate) fn execute_create_database_statement(
    state: &Arc<StandaloneState>,
    name: &ObjectName,
    current_catalog: Option<&str>,
) -> Result<StatementResult, String> {
    if current_catalog.is_none() && name.parts.len() == 1 {
        let mut guard = state
            .catalog
            .write()
            .expect("standalone catalog write lock");
        guard.create_database(name.leaf())?;
        drop(guard);
        return Ok(StatementResult::Ok);
    }

    let resolved = resolve_iceberg_namespace_name(name.clone(), current_catalog)?;
    let guard = state
        .iceberg_catalogs
        .read()
        .expect("standalone iceberg catalog read lock");
    let entry = guard.get(&resolved.catalog)?;
    create_iceberg_namespace(&entry, &resolved.namespace)?;
    persist_iceberg_namespace_if_needed(state, &resolved.catalog, &resolved.namespace)?;
    Ok(StatementResult::Ok)
}

pub(crate) fn execute_create_table_statement(
    state: &Arc<StandaloneState>,
    stmt: crate::sql::parser::ast::CreateTableStmt,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<StatementResult, String> {
    match stmt.kind {
        CreateTableKind::Iceberg {
            columns,
            key_desc,
            bucket_count,
            properties,
        } => {
            // Two-part (or one-part) names in the default catalog land on
            // managed lake. The local parquet backend has been removed, so a
            // missing managed-lake config is a hard error rather than a
            // silent fallback.
            if current_catalog.is_none() && stmt.name.parts.len() <= 2 {
                if state.managed_lake_config.is_none() {
                    return Err(
                        "managed lake is not configured; set `warehouse_uri` to run CREATE TABLE"
                            .to_string(),
                    );
                }
                return create_managed_table(
                    state.as_ref(),
                    &stmt.name,
                    current_database,
                    &columns,
                    key_desc.as_ref(),
                    bucket_count,
                );
            }

            let resolved =
                resolve_iceberg_table_name(stmt.name, current_catalog, current_database)?;
            let guard = state
                .iceberg_catalogs
                .read()
                .expect("standalone iceberg catalog read lock");
            let entry = guard.get(&resolved.catalog)?;
            create_iceberg_table(
                &entry,
                &resolved.namespace,
                &resolved.table,
                &columns,
                key_desc.as_ref(),
                &properties,
            )?;
            persist_iceberg_table_if_needed(
                state,
                &resolved.catalog,
                &resolved.namespace,
                &resolved.table,
            )?;
            Ok(StatementResult::Ok)
        }
    }
}

pub(crate) fn execute_drop_catalog_statement(
    state: &Arc<StandaloneState>,
    catalog_name: &str,
    if_exists: bool,
) -> Result<StatementResult, String> {
    let mut guard = state
        .iceberg_catalogs
        .write()
        .expect("standalone iceberg catalog write lock");
    match guard.drop_catalog(catalog_name) {
        Ok(()) => {
            drop(guard);
            delete_iceberg_catalog_if_needed(state, &normalize_identifier(catalog_name)?)?;
            Ok(StatementResult::Ok)
        }
        Err(err) if if_exists && err.contains("unknown catalog") => Ok(StatementResult::Ok),
        Err(err) => Err(err),
    }
}

pub(crate) fn execute_drop_database_statement(
    state: &Arc<StandaloneState>,
    name: &ObjectName,
    current_catalog: Option<&str>,
    if_exists: bool,
    force: bool,
) -> Result<StatementResult, String> {
    if current_catalog.is_none() && name.parts.len() == 1 {
        let db_name = name.leaf();
        // With `FORCE`, cascade-drop every managed-lake table in this
        // database before we tear down the in-memory catalog entry.
        // Otherwise the managed catalog / sqlite / object store still hold
        // those tables, and the next `CREATE DATABASE`+`CREATE TABLE`
        // against the same name trips "table already exists".
        if force {
            let managed_tables = state
                .managed_lake
                .read()
                .expect("standalone managed lake read lock")
                .list_tables_in_database(db_name)
                .unwrap_or_default();
            for table_name in managed_tables {
                drop_managed_lake_table(state, db_name, &table_name)?;
            }
            // Remove the persisted `databases` row too, so a follow-up
            // `CREATE DATABASE <same>` gets a fresh `db_id` and the next
            // `CREATE TABLE <same>` doesn't collide with the DROPPING
            // tables the erase worker hasn't cleaned yet.
            if state.managed_lake_config.is_some() {
                drop_managed_database_entry(state, db_name)?;
            }
        }
        let mut guard = state
            .catalog
            .write()
            .expect("standalone catalog write lock");
        match guard.drop_database(db_name) {
            Ok(()) => {
                drop(guard);
                return Ok(StatementResult::Ok);
            }
            Err(err) if if_exists && err.contains("unknown database") => {
                return Ok(StatementResult::Ok);
            }
            Err(err) => return Err(err),
        }
    }

    let resolved = resolve_iceberg_namespace_name(name.clone(), current_catalog)?;
    let guard = state
        .iceberg_catalogs
        .read()
        .expect("standalone iceberg catalog read lock");
    let entry = guard.get(&resolved.catalog)?;
    let namespace_exists = iceberg_namespace_exists(&entry, &resolved.namespace)?;
    if !namespace_exists {
        return if if_exists {
            Ok(StatementResult::Ok)
        } else {
            Err(format!("unknown database `{}`", name.parts.join(".")))
        };
    }
    if force {
        for table_name in list_iceberg_tables(&entry, &resolved.namespace)? {
            drop_iceberg_table(&entry, &resolved.namespace, &table_name)?;
            delete_iceberg_table_if_needed(
                state,
                &resolved.catalog,
                &resolved.namespace,
                &table_name,
            )?;
        }
    }
    match drop_iceberg_namespace(&entry, &resolved.namespace) {
        Ok(()) => {
            delete_iceberg_namespace_if_needed(state, &resolved.catalog, &resolved.namespace)?;
            Ok(StatementResult::Ok)
        }
        Err(err) if if_exists && err.contains("namespace") => Ok(StatementResult::Ok),
        Err(err) => Err(err),
    }
}

pub(crate) fn execute_drop_table_statement(
    state: &Arc<StandaloneState>,
    name: &ObjectName,
    current_catalog: Option<&str>,
    current_database: &str,
    if_exists: bool,
    _force: bool,
) -> Result<StatementResult, String> {
    if current_catalog.is_none() && name.parts.len() <= 2 {
        let resolved = resolve_local_table_name(name, current_database)?;
        if state
            .managed_lake
            .read()
            .expect("standalone managed lake read lock")
            .contains_table(&resolved.database, &resolved.table)?
        {
            return drop_managed_lake_table(state, &resolved.database, &resolved.table);
        }
        // Not a managed table: still allow dropping a logical entry from the
        // in-memory catalog (used during iceberg materialization); otherwise
        // honour IF EXISTS.
        let mut guard = state
            .catalog
            .write()
            .expect("standalone catalog write lock");
        match guard.drop_table(&resolved.database, &resolved.table) {
            Ok(()) => Ok(StatementResult::Ok),
            Err(err) if if_exists && err.contains("unknown") => Ok(StatementResult::Ok),
            Err(err) => Err(err),
        }
    } else {
        let resolved = resolve_iceberg_table_name(name.clone(), current_catalog, current_database)?;
        let guard = state
            .iceberg_catalogs
            .read()
            .expect("standalone iceberg catalog read lock");
        let entry = guard.get(&resolved.catalog)?;
        match drop_iceberg_table(&entry, &resolved.namespace, &resolved.table) {
            Ok(()) => {
                delete_iceberg_table_if_needed(
                    state,
                    &resolved.catalog,
                    &resolved.namespace,
                    &resolved.table,
                )?;
                Ok(StatementResult::Ok)
            }
            Err(err) if if_exists && err.contains("table") => Ok(StatementResult::Ok),
            Err(err) => Err(err),
        }
    }
}

// ---------------------------------------------------------------------------
// DML handlers
// ---------------------------------------------------------------------------

pub(crate) fn execute_truncate_table_statement(
    state: &Arc<StandaloneState>,
    name: &ObjectName,
    current_database: &str,
) -> Result<StatementResult, String> {
    let resolved = resolve_local_table_name(name, current_database)?;
    if state
        .managed_lake
        .read()
        .expect("standalone managed lake read lock")
        .contains_table(&resolved.database, &resolved.table)?
    {
        return truncate_managed_lake_table(state, &resolved.database, &resolved.table);
    }
    Err(format!(
        "TRUNCATE TABLE only supports managed-lake tables: {}.{}",
        resolved.database, resolved.table
    ))
}

pub(crate) fn execute_insert_statement(
    state: &Arc<StandaloneState>,
    name: &ObjectName,
    columns: &[String],
    source: &InsertSource,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<StatementResult, String> {
    // Two-part names in the default catalog route to managed lake.
    if current_catalog.is_none() && name.parts.len() <= 2 {
        if let Ok(resolved) = resolve_local_table_name(name, current_database) {
            if state
                .managed_lake
                .read()
                .expect("standalone managed lake read lock")
                .contains_table(&resolved.database, &resolved.table)?
            {
                return crate::standalone::lake::txn::insert_into_managed_lake_table(
                    state,
                    name,
                    columns,
                    source,
                    current_database,
                );
            }
        }
    }

    let resolved = resolve_iceberg_table_name(name.clone(), current_catalog, current_database)?;
    let guard = state
        .iceberg_catalogs
        .read()
        .expect("standalone iceberg catalog read lock");
    let entry = guard.get(&resolved.catalog)?;
    let loaded =
        crate::standalone::iceberg::load_table(&entry, &resolved.namespace, &resolved.table)?;
    match source {
        InsertSource::Values(rows) => {
            let rows = reorder_insert_rows(rows, columns, &loaded.columns)?;
            insert_iceberg_rows(&entry, &resolved.namespace, &resolved.table, &rows)?;
        }
        InsertSource::SelectLiteralRow(row) => {
            let rows = reorder_insert_rows(std::slice::from_ref(row), columns, &loaded.columns)?;
            insert_iceberg_rows(&entry, &resolved.namespace, &resolved.table, &rows)?;
        }
        InsertSource::GenerateSeriesSelect(source) => {
            insert_generate_series_rows(
                &entry,
                &resolved.namespace,
                &resolved.table,
                source,
                columns,
                &loaded.columns,
            )?;
        }
        InsertSource::UnionAll(parts) => {
            // Iceberg expects a single insert per call; split into independent
            // inserts so each part writes its own append.
            for part in parts {
                execute_insert_statement(
                    state,
                    name,
                    columns,
                    part,
                    current_catalog,
                    current_database,
                )?;
            }
        }
        InsertSource::FromQuery(_) => {
            // Plan-pipeline INSERT ... SELECT is only implemented for
            // managed-lake tables today; iceberg writes still go through the
            // literal/generate_series fast paths.
            return Err("unsupported INSERT SELECT source for iceberg table".into());
        }
    }
    Ok(StatementResult::Ok)
}

// ---------------------------------------------------------------------------
// Query table name extraction (for Iceberg materialization)
// ---------------------------------------------------------------------------

/// Extract simple table names from a query AST (for Iceberg table materialization).
pub(crate) fn extract_table_names_from_query(query: &sqlparser::ast::Query) -> Vec<String> {
    let mut names = Vec::new();
    // Extract table names from CTEs (WITH clause)
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            extract_table_names_from_subquery(&cte.query, &mut names);
        }
    }
    extract_table_names_from_set_expr(query.body.as_ref(), &mut names);
    names.sort();
    names.dedup();
    names
}

fn extract_table_names_from_set_expr(expr: &sqlparser::ast::SetExpr, names: &mut Vec<String>) {
    match expr {
        sqlparser::ast::SetExpr::Select(s) => {
            for from in &s.from {
                extract_table_names_from_table_factor(&from.relation, names);
                for join in &from.joins {
                    extract_table_names_from_table_factor(&join.relation, names);
                }
            }
            // Also extract table names from subqueries in WHERE/HAVING/SELECT
            extract_table_names_from_expr_opt(s.selection.as_ref(), names);
            extract_table_names_from_expr_opt(s.having.as_ref(), names);
            for item in &s.projection {
                if let sqlparser::ast::SelectItem::UnnamedExpr(expr)
                | sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } = item
                {
                    extract_table_names_from_expr(expr, names);
                }
            }
        }
        sqlparser::ast::SetExpr::SetOperation { left, right, .. } => {
            extract_table_names_from_set_expr(left, names);
            extract_table_names_from_set_expr(right, names);
        }
        sqlparser::ast::SetExpr::Query(q) => {
            extract_table_names_from_set_expr(q.body.as_ref(), names);
        }
        _ => {}
    }
}

fn extract_table_names_from_table_factor(
    factor: &sqlparser::ast::TableFactor,
    names: &mut Vec<String>,
) {
    match factor {
        sqlparser::ast::TableFactor::Table { name, .. } => {
            // Take the last part as the table name (ignore catalog/db qualifiers)
            if let Some(last) = name.0.last() {
                let n = match last {
                    sqlparser::ast::ObjectNamePart::Identifier(ident) => ident.value.to_lowercase(),
                    other => other.to_string().to_lowercase(),
                };
                names.push(n);
            }
        }
        sqlparser::ast::TableFactor::Derived { subquery, .. } => {
            extract_table_names_from_set_expr(subquery.body.as_ref(), names);
        }
        _ => {}
    }
}

fn extract_table_names_from_expr_opt(expr: Option<&sqlparser::ast::Expr>, names: &mut Vec<String>) {
    if let Some(e) = expr {
        extract_table_names_from_expr(e, names);
    }
}

fn extract_table_names_from_expr(expr: &sqlparser::ast::Expr, names: &mut Vec<String>) {
    // Use the Display impl to get the SQL string, then recursively look for
    // subquery patterns. This is simpler than matching every AST variant.
    // For subquery extraction, we only need to find Subquery/Exists/InSubquery nodes.
    use sqlparser::ast::Expr;
    match expr {
        Expr::Subquery(q) | Expr::Exists { subquery: q, .. } => {
            extract_table_names_from_subquery(q, names);
        }
        Expr::InSubquery { subquery, expr, .. } => {
            extract_table_names_from_subquery(subquery, names);
            extract_table_names_from_expr(expr, names);
        }
        Expr::BinaryOp { left, right, .. } => {
            extract_table_names_from_expr(left, names);
            extract_table_names_from_expr(right, names);
        }
        Expr::UnaryOp { expr, .. } | Expr::Nested(expr) => {
            extract_table_names_from_expr(expr, names);
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            extract_table_names_from_expr(expr, names);
            extract_table_names_from_expr(low, names);
            extract_table_names_from_expr(high, names);
        }
        _ => {} // literals, column refs, functions, etc.
    }
}

fn extract_table_names_from_subquery(query: &sqlparser::ast::Query, names: &mut Vec<String>) {
    extract_table_names_from_set_expr(query.body.as_ref(), names);
}

// ---------------------------------------------------------------------------
// Three-part table name helpers (catalog.database.table)
// ---------------------------------------------------------------------------

/// Extract `(catalog, database, table)` triples from 3-part table references
/// in a query AST.
pub(crate) fn extract_three_part_table_refs(
    query: &sqlparser::ast::Query,
) -> Vec<(String, String, String)> {
    let mut refs = Vec::new();
    extract_three_part_refs_from_set_expr(query.body.as_ref(), &mut refs);
    refs.sort();
    refs.dedup();
    refs
}

fn extract_three_part_refs_from_set_expr(
    expr: &sqlparser::ast::SetExpr,
    refs: &mut Vec<(String, String, String)>,
) {
    match expr {
        sqlparser::ast::SetExpr::Select(s) => {
            for from in &s.from {
                extract_three_part_refs_from_factor(&from.relation, refs);
                for join in &from.joins {
                    extract_three_part_refs_from_factor(&join.relation, refs);
                }
            }
        }
        sqlparser::ast::SetExpr::SetOperation { left, right, .. } => {
            extract_three_part_refs_from_set_expr(left, refs);
            extract_three_part_refs_from_set_expr(right, refs);
        }
        sqlparser::ast::SetExpr::Query(q) => {
            extract_three_part_refs_from_set_expr(q.body.as_ref(), refs);
        }
        _ => {}
    }
}

fn extract_three_part_refs_from_factor(
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
            extract_three_part_refs_from_set_expr(subquery.body.as_ref(), refs);
        }
        _ => {}
    }
}

/// Rewrite a query AST in-place: convert all 3-part table references
/// `catalog.database.table` to 2-part `database.table` by stripping the
/// leading catalog element.
pub(crate) fn strip_catalog_from_three_part_names(query: &mut sqlparser::ast::Query) {
    strip_catalog_in_set_expr(query.body.as_mut());
}

fn strip_catalog_in_set_expr(expr: &mut sqlparser::ast::SetExpr) {
    match expr {
        sqlparser::ast::SetExpr::Select(s) => {
            for from in &mut s.from {
                strip_catalog_in_factor(&mut from.relation);
                for join in &mut from.joins {
                    strip_catalog_in_factor(&mut join.relation);
                }
            }
        }
        sqlparser::ast::SetExpr::SetOperation { left, right, .. } => {
            strip_catalog_in_set_expr(left.as_mut());
            strip_catalog_in_set_expr(right.as_mut());
        }
        sqlparser::ast::SetExpr::Query(q) => {
            strip_catalog_in_set_expr(q.body.as_mut());
        }
        _ => {}
    }
}

fn strip_catalog_in_factor(factor: &mut sqlparser::ast::TableFactor) {
    match factor {
        sqlparser::ast::TableFactor::Table { name, .. } => {
            if name.0.len() == 3 {
                name.0.remove(0);
            }
        }
        sqlparser::ast::TableFactor::Derived { subquery, .. } => {
            strip_catalog_in_set_expr(subquery.body.as_mut());
        }
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// ADD FILES SQL parsing
// ---------------------------------------------------------------------------

/// Check if SQL looks like ALTER TABLE ... ADD FILES FROM ...
pub(crate) fn looks_like_add_files(sql: &str) -> bool {
    let upper = sql.trim().to_ascii_uppercase();
    upper.starts_with("ALTER TABLE") && upper.contains("ADD FILES FROM")
}

/// Parse: ALTER TABLE [catalog.db.]table ADD FILES FROM 's3://...'
pub(crate) fn parse_add_files_sql(sql: &str) -> Result<(Vec<String>, String), String> {
    // Extract the part between ALTER TABLE and ADD FILES FROM
    let upper = sql.to_ascii_uppercase();
    let alter_idx = upper.find("ALTER TABLE").ok_or("missing ALTER TABLE")?;
    let add_files_idx = upper
        .find("ADD FILES FROM")
        .ok_or("missing ADD FILES FROM")?;

    let table_str = sql[alter_idx + 11..add_files_idx].trim();
    let table_parts: Vec<String> = table_str
        .split('.')
        .map(|s| s.trim().trim_matches('`').to_lowercase())
        .collect();

    // Extract the path after ADD FILES FROM
    let after_from = &sql[add_files_idx + 14..];
    let path = after_from
        .trim()
        .trim_end_matches(';')
        .trim()
        .trim_matches('\'')
        .trim_matches('"')
        .to_string();

    if path.is_empty() {
        return Err("ADD FILES FROM requires a path".to_string());
    }

    Ok((table_parts, path))
}
