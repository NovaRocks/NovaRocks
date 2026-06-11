//! Inline expansion of iceberg-catalog views referenced by SELECT queries.
//!
//! Runs after session-view expansion and before the analyzer. For every
//! table factor that resolves to a REST iceberg catalog the table is
//! probed first (matching StarRocks' table-then-view order); when no
//! table exists but a view does, the view's SQL representation is parsed
//! and spliced inline as a derived subquery. Bare names inside the view
//! body are qualified against the view's own catalog and stored
//! default-namespace — not the session database. Nested views expand
//! recursively with cycle detection.

use std::collections::HashSet;
use std::sync::Arc;

use sqlparser::ast as sqlast;

use crate::connector::backend::ResolvedView;
use crate::engine::StandaloneState;
use crate::engine::iceberg_view::{IcebergViewTarget, resolve_iceberg_view_target_parts};

type ViewKey = (String, String, String);

pub(crate) fn expand_iceberg_views_in_query(
    state: &Arc<StandaloneState>,
    query: &mut sqlast::Query,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<(), String> {
    let mut stack: Vec<ViewKey> = Vec::new();
    expand_query(state, query, current_catalog, current_database, &mut stack)
}

fn expand_query(
    state: &Arc<StandaloneState>,
    query: &mut sqlast::Query,
    current_catalog: Option<&str>,
    current_database: &str,
    stack: &mut Vec<ViewKey>,
) -> Result<(), String> {
    // CTE names shadow tables/views. Collect them up-front; the slight
    // over-shadowing (a later CTE name visible in an earlier body) only
    // suppresses expansion, never mis-expands.
    let mut cte_names: HashSet<String> = HashSet::new();
    if let Some(with_clause) = query.with.as_ref() {
        for cte in &with_clause.cte_tables {
            cte_names.insert(cte.alias.name.value.to_ascii_lowercase());
        }
    }
    if let Some(with_clause) = query.with.as_mut() {
        for cte in &mut with_clause.cte_tables {
            expand_query(
                state,
                cte.query.as_mut(),
                current_catalog,
                current_database,
                stack,
            )?;
        }
    }
    expand_set_expr(
        state,
        query.body.as_mut(),
        current_catalog,
        current_database,
        &cte_names,
        stack,
    )
}

fn expand_set_expr(
    state: &Arc<StandaloneState>,
    expr: &mut sqlast::SetExpr,
    current_catalog: Option<&str>,
    current_database: &str,
    cte_names: &HashSet<String>,
    stack: &mut Vec<ViewKey>,
) -> Result<(), String> {
    match expr {
        sqlast::SetExpr::Select(select) => {
            for twj in select.from.iter_mut() {
                expand_table_factor(
                    state,
                    &mut twj.relation,
                    current_catalog,
                    current_database,
                    cte_names,
                    stack,
                )?;
                for join in twj.joins.iter_mut() {
                    expand_table_factor(
                        state,
                        &mut join.relation,
                        current_catalog,
                        current_database,
                        cte_names,
                        stack,
                    )?;
                }
            }
            Ok(())
        }
        sqlast::SetExpr::Query(q) => {
            expand_query(state, q.as_mut(), current_catalog, current_database, stack)
        }
        sqlast::SetExpr::SetOperation { left, right, .. } => {
            expand_set_expr(
                state,
                left.as_mut(),
                current_catalog,
                current_database,
                cte_names,
                stack,
            )?;
            expand_set_expr(
                state,
                right.as_mut(),
                current_catalog,
                current_database,
                cte_names,
                stack,
            )
        }
        _ => Ok(()),
    }
}

fn expand_table_factor(
    state: &Arc<StandaloneState>,
    factor: &mut sqlast::TableFactor,
    current_catalog: Option<&str>,
    current_database: &str,
    cte_names: &HashSet<String>,
    stack: &mut Vec<ViewKey>,
) -> Result<(), String> {
    match factor {
        sqlast::TableFactor::Table { name, alias, .. } => {
            let parts: Vec<String> = name
                .0
                .iter()
                .filter_map(|part| match part {
                    sqlast::ObjectNamePart::Identifier(ident) => Some(ident.value.clone()),
                    _ => None,
                })
                .collect();
            if parts.len() == 1 && cte_names.contains(&parts[0].to_ascii_lowercase()) {
                return Ok(());
            }
            let Some(target) =
                rest_view_candidate(state, &parts, current_catalog, current_database)
            else {
                return Ok(());
            };
            // Table-first, matching StarRocks: a probe failure (connectivity
            // etc.) leaves the factor untouched so the analyzer surfaces the
            // canonical error for tables.
            if probe_table_exists(state, &target) {
                return Ok(());
            }
            let Some(view) = probe_load_view(state, &target)? else {
                return Ok(());
            };
            let key = (
                target.catalog.clone(),
                target.namespace.clone(),
                target.view.clone(),
            );
            if stack.contains(&key) {
                return Err(format!(
                    "circular view reference: {}.{}.{}",
                    key.0, key.1, key.2
                ));
            }
            let mut body = parse_view_sql(&view, &key)?;
            qualify_view_body_names(&mut body, &target.catalog, &view.default_namespace);
            stack.push(key);
            expand_query(
                state,
                &mut body,
                Some(&target.catalog),
                &view.default_namespace,
                stack,
            )?;
            stack.pop();

            let alias = alias.take().unwrap_or_else(|| sqlast::TableAlias {
                name: sqlast::Ident::new(parts.last().cloned().unwrap_or_default()),
                columns: Vec::new(),
                explicit: false,
            });
            *factor = sqlast::TableFactor::Derived {
                lateral: false,
                subquery: Box::new(body),
                alias: Some(alias),
                sample: None,
            };
            Ok(())
        }
        sqlast::TableFactor::Derived { subquery, .. } => expand_query(
            state,
            subquery.as_mut(),
            current_catalog,
            current_database,
            stack,
        ),
        sqlast::TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            expand_table_factor(
                state,
                &mut table_with_joins.relation,
                current_catalog,
                current_database,
                cte_names,
                stack,
            )?;
            for join in table_with_joins.joins.iter_mut() {
                expand_table_factor(
                    state,
                    &mut join.relation,
                    current_catalog,
                    current_database,
                    cte_names,
                    stack,
                )?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// Resolve to a target only when the name lands in a registered REST
/// iceberg catalog; all probe-ineligible names return None.
fn rest_view_candidate(
    state: &Arc<StandaloneState>,
    parts: &[String],
    current_catalog: Option<&str>,
    current_database: &str,
) -> Option<IcebergViewTarget> {
    let target = resolve_iceberg_view_target_parts(state, parts, current_catalog, current_database)
        .ok()
        .flatten()?;
    let registry = state.iceberg_catalogs.read().ok()?;
    let entry = registry.get(&target.catalog).ok()?;
    if !matches!(
        entry.kind,
        crate::connector::iceberg::catalog::registry::IcebergCatalogKind::Rest
    ) {
        return None;
    }
    Some(target)
}

fn probe_table_exists(state: &Arc<StandaloneState>, target: &IcebergViewTarget) -> bool {
    let Ok(backend) = state
        .connectors
        .read()
        .expect("connector registry read")
        .catalog_backend("iceberg")
    else {
        return true;
    };
    backend
        .table_exists(&target.catalog, &target.namespace, &target.view)
        .unwrap_or(true)
}

fn probe_load_view(
    state: &Arc<StandaloneState>,
    target: &IcebergViewTarget,
) -> Result<Option<ResolvedView>, String> {
    let backend = state
        .connectors
        .read()
        .expect("connector registry read")
        .catalog_backend("iceberg")?;
    match backend.load_view(&target.catalog, &target.namespace, &target.view) {
        Ok(view) => Ok(Some(view)),
        Err(err) if err.contains("unknown view") => Ok(None),
        Err(err) => Err(err),
    }
}

fn parse_view_sql(view: &ResolvedView, key: &ViewKey) -> Result<sqlast::Query, String> {
    let dialect = crate::sql::parser::dialect::StarRocksDialect;
    let mut parser = sqlparser::parser::Parser::new(&dialect)
        .try_with_sql(&view.sql)
        .map_err(|e| view_parse_error(key, &view.dialect, &e.to_string()))?;
    let stmt = parser
        .parse_statement()
        .map_err(|e| view_parse_error(key, &view.dialect, &e.to_string()))?;
    let sqlast::Statement::Query(query) = stmt else {
        return Err(format!(
            "iceberg view {}.{}.{} body is not a SELECT query",
            key.0, key.1, key.2
        ));
    };
    Ok(*query)
}

fn view_parse_error(key: &ViewKey, dialect: &str, err: &str) -> String {
    format!(
        "parse iceberg view {}.{}.{} (representation dialect `{dialect}`) failed: {err}",
        key.0, key.1, key.2
    )
}

/// Qualify bare/2-part table names inside a view body against the view's
/// catalog and default namespace. 3-part names and CTE references are left
/// untouched. Pure AST transform, unit-tested below.
pub(crate) fn qualify_view_body_names(
    query: &mut sqlast::Query,
    catalog: &str,
    default_namespace: &str,
) {
    let mut cte_names: HashSet<String> = HashSet::new();
    if let Some(with_clause) = query.with.as_ref() {
        for cte in &with_clause.cte_tables {
            cte_names.insert(cte.alias.name.value.to_ascii_lowercase());
        }
    }
    if let Some(with_clause) = query.with.as_mut() {
        for cte in &mut with_clause.cte_tables {
            qualify_view_body_names(cte.query.as_mut(), catalog, default_namespace);
        }
    }
    qualify_set_expr(query.body.as_mut(), catalog, default_namespace, &cte_names);
}

fn qualify_set_expr(
    expr: &mut sqlast::SetExpr,
    catalog: &str,
    default_namespace: &str,
    cte_names: &HashSet<String>,
) {
    match expr {
        sqlast::SetExpr::Select(select) => {
            for twj in select.from.iter_mut() {
                qualify_table_factor(&mut twj.relation, catalog, default_namespace, cte_names);
                for join in twj.joins.iter_mut() {
                    qualify_table_factor(&mut join.relation, catalog, default_namespace, cte_names);
                }
            }
        }
        sqlast::SetExpr::Query(q) => {
            qualify_view_body_names(q.as_mut(), catalog, default_namespace)
        }
        sqlast::SetExpr::SetOperation { left, right, .. } => {
            qualify_set_expr(left.as_mut(), catalog, default_namespace, cte_names);
            qualify_set_expr(right.as_mut(), catalog, default_namespace, cte_names);
        }
        _ => {}
    }
}

fn qualify_table_factor(
    factor: &mut sqlast::TableFactor,
    catalog: &str,
    default_namespace: &str,
    cte_names: &HashSet<String>,
) {
    match factor {
        sqlast::TableFactor::Table { name, .. } => {
            let ident_count = name
                .0
                .iter()
                .filter(|part| matches!(part, sqlast::ObjectNamePart::Identifier(_)))
                .count();
            match ident_count {
                1 => {
                    if let Some(sqlast::ObjectNamePart::Identifier(table)) = name.0.first() {
                        if cte_names.contains(&table.value.to_ascii_lowercase()) {
                            return;
                        }
                    }
                    let mut parts = vec![
                        sqlast::ObjectNamePart::Identifier(sqlast::Ident::new(catalog)),
                        sqlast::ObjectNamePart::Identifier(sqlast::Ident::new(default_namespace)),
                    ];
                    parts.append(&mut name.0);
                    name.0 = parts;
                }
                2 => {
                    name.0.insert(
                        0,
                        sqlast::ObjectNamePart::Identifier(sqlast::Ident::new(catalog)),
                    );
                }
                _ => {}
            }
        }
        sqlast::TableFactor::Derived { subquery, .. } => {
            qualify_view_body_names(subquery.as_mut(), catalog, default_namespace);
        }
        sqlast::TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            qualify_table_factor(
                &mut table_with_joins.relation,
                catalog,
                default_namespace,
                cte_names,
            );
            for join in table_with_joins.joins.iter_mut() {
                qualify_table_factor(&mut join.relation, catalog, default_namespace, cte_names);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod qualify_tests {
    use super::qualify_view_body_names;
    use crate::sql::parser::dialect::StarRocksDialect;
    use sqlparser::parser::Parser;

    fn parse_query(sql: &str) -> sqlparser::ast::Query {
        let dialect = StarRocksDialect;
        let mut parser = Parser::new(&dialect).try_with_sql(sql).expect("parser");
        let stmt = parser.parse_statement().expect("statement");
        let sqlparser::ast::Statement::Query(query) = stmt else {
            panic!("expected query");
        };
        *query
    }

    #[test]
    fn qualifies_bare_and_two_part_names() {
        let mut query = parse_query("SELECT a.id FROM t1 a JOIN db2.t2 b ON a.id = b.id");
        qualify_view_body_names(&mut query, "ice", "ns1");
        let rendered = query.to_string();
        assert!(rendered.contains("ice.ns1.t1"), "got: {rendered}");
        assert!(rendered.contains("ice.db2.t2"), "got: {rendered}");
    }

    #[test]
    fn leaves_three_part_names_and_ctes_alone() {
        let mut query = parse_query(
            "WITH c AS (SELECT id FROM t1) SELECT * FROM c JOIN other_cat.db.t3 ON true",
        );
        qualify_view_body_names(&mut query, "ice", "ns1");
        let rendered = query.to_string();
        assert!(rendered.contains("FROM c"), "got: {rendered}");
        assert!(rendered.contains("other_cat.db.t3"), "got: {rendered}");
        assert!(rendered.contains("ice.ns1.t1"), "got: {rendered}");
    }
}
