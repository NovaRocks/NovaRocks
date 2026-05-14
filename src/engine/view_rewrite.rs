//! Inline expansion of user-defined views.
//!
//! StarRocks's standalone server stores `CREATE VIEW` definitions on
//! `StandaloneState::views` as the underlying SELECT `Query` AST. This
//! module walks a query before it reaches the analyzer and replaces any
//! `TableFactor::Table` reference that resolves to a registered view
//! with a `TableFactor::Derived` subquery carrying the view's body.
//!
//! The walker is intentionally narrow: it only rewrites table references
//! in `FROM`/`JOIN` table-factor slots. Nested subqueries are walked too
//! so views can reference other views.

use std::collections::HashMap;
use std::sync::RwLock;

use sqlparser::ast as sqlast;

pub(crate) fn expand_views_in_query(
    query: &mut sqlast::Query,
    views: &RwLock<HashMap<(String, String), Box<sqlast::Query>>>,
    current_database: &str,
) {
    let registry = views.read().expect("view registry read lock");
    if registry.is_empty() {
        return;
    }
    expand_query(query, &registry, current_database);
}

fn expand_query(
    query: &mut sqlast::Query,
    registry: &HashMap<(String, String), Box<sqlast::Query>>,
    current_database: &str,
) {
    if let Some(with_clause) = query.with.as_mut() {
        for cte in &mut with_clause.cte_tables {
            expand_query(cte.query.as_mut(), registry, current_database);
        }
    }
    expand_set_expr(query.body.as_mut(), registry, current_database);
}

fn expand_set_expr(
    expr: &mut sqlast::SetExpr,
    registry: &HashMap<(String, String), Box<sqlast::Query>>,
    current_database: &str,
) {
    match expr {
        sqlast::SetExpr::Select(select) => expand_select(select, registry, current_database),
        sqlast::SetExpr::Query(q) => expand_query(q.as_mut(), registry, current_database),
        sqlast::SetExpr::SetOperation { left, right, .. } => {
            expand_set_expr(left.as_mut(), registry, current_database);
            expand_set_expr(right.as_mut(), registry, current_database);
        }
        _ => {}
    }
}

fn expand_select(
    select: &mut sqlast::Select,
    registry: &HashMap<(String, String), Box<sqlast::Query>>,
    current_database: &str,
) {
    for twj in select.from.iter_mut() {
        expand_table_factor(&mut twj.relation, registry, current_database);
        for join in twj.joins.iter_mut() {
            expand_table_factor(&mut join.relation, registry, current_database);
        }
    }
}

fn expand_table_factor(
    factor: &mut sqlast::TableFactor,
    registry: &HashMap<(String, String), Box<sqlast::Query>>,
    current_database: &str,
) {
    match factor {
        sqlast::TableFactor::Table { name, alias, .. } => {
            let parts: Vec<String> = name
                .0
                .iter()
                .filter_map(|p| match p {
                    sqlast::ObjectNamePart::Identifier(i) => Some(i.value.clone()),
                    _ => None,
                })
                .collect();
            let key = match parts.as_slice() {
                [v] => Some((
                    current_database.to_ascii_lowercase(),
                    v.to_ascii_lowercase(),
                )),
                [db, v] => Some((db.to_ascii_lowercase(), v.to_ascii_lowercase())),
                [_cat, db, v] => Some((db.to_ascii_lowercase(), v.to_ascii_lowercase())),
                _ => None,
            };
            let Some(key) = key else { return };
            let Some(view_query) = registry.get(&key) else {
                return;
            };
            // The body itself may reference further views — walk it first
            // so the substitution is fully resolved before it reaches the
            // analyzer.
            let mut expanded = view_query.as_ref().clone();
            expand_query(&mut expanded, registry, current_database);

            // Keep the user-supplied alias if any; otherwise use the view
            // name so downstream `SELECT v.col` references still resolve.
            let alias = alias.take().unwrap_or_else(|| sqlast::TableAlias {
                name: sqlast::Ident::new(parts.last().cloned().unwrap_or(key.1.clone())),
                columns: Vec::new(),
                explicit: false,
            });
            *factor = sqlast::TableFactor::Derived {
                lateral: false,
                subquery: Box::new(expanded),
                alias: Some(alias),
                sample: None,
            };
        }
        sqlast::TableFactor::Derived { subquery, .. } => {
            expand_query(subquery.as_mut(), registry, current_database);
        }
        sqlast::TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            expand_table_factor(&mut table_with_joins.relation, registry, current_database);
            for join in table_with_joins.joins.iter_mut() {
                expand_table_factor(&mut join.relation, registry, current_database);
            }
        }
        _ => {}
    }
}
