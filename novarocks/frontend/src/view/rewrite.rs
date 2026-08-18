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

use std::collections::{HashMap, HashSet};

use crate::view::{
    ExternalViewResolution, ResolvedExternalView, ViewEngine, ViewRequestContext, ViewSqlDialect,
    ViewTarget,
};
use sqlparser::ast as sqlast;
use sqlparser::parser::Parser;

use super::iceberg::resolve_external_target_parts;
use super::{DEFAULT_CATALOG, SessionViewKey, StoredView};

type ExternalViewKey = (String, String, String);

pub(super) fn expand_session_views(
    query: &mut sqlast::Query,
    registry: &HashMap<SessionViewKey, StoredView>,
    current_database: &str,
) {
    if registry.is_empty() {
        return;
    }
    expand_session_query(query, registry, current_database, &HashSet::new());
}

fn expand_session_query(
    query: &mut sqlast::Query,
    registry: &HashMap<SessionViewKey, StoredView>,
    current_database: &str,
    visible_ctes: &HashSet<String>,
) {
    let mut body_visible_ctes = visible_ctes.clone();
    if let Some(with_clause) = query.with.as_mut() {
        let recursive = with_clause.recursive;
        for cte in &mut with_clause.cte_tables {
            let name = cte.alias.name.value.to_ascii_lowercase();
            let mut cte_visible_ctes = body_visible_ctes.clone();
            if recursive {
                cte_visible_ctes.insert(name.clone());
            }
            expand_session_query(
                cte.query.as_mut(),
                registry,
                current_database,
                &cte_visible_ctes,
            );
            body_visible_ctes.insert(name);
        }
    }
    expand_session_set_expr(
        query.body.as_mut(),
        registry,
        current_database,
        &body_visible_ctes,
    );
}

fn expand_session_set_expr(
    expression: &mut sqlast::SetExpr,
    registry: &HashMap<SessionViewKey, StoredView>,
    current_database: &str,
    cte_names: &HashSet<String>,
) {
    match expression {
        sqlast::SetExpr::Select(select) => {
            for table_with_joins in &mut select.from {
                expand_session_table_factor(
                    &mut table_with_joins.relation,
                    registry,
                    current_database,
                    cte_names,
                );
                for join in &mut table_with_joins.joins {
                    expand_session_table_factor(
                        &mut join.relation,
                        registry,
                        current_database,
                        cte_names,
                    );
                }
            }
        }
        sqlast::SetExpr::Query(query) => {
            expand_session_query(query.as_mut(), registry, current_database, cte_names)
        }
        sqlast::SetExpr::SetOperation { left, right, .. } => {
            expand_session_set_expr(left.as_mut(), registry, current_database, cte_names);
            expand_session_set_expr(right.as_mut(), registry, current_database, cte_names);
        }
        _ => {}
    }
}

fn expand_session_table_factor(
    factor: &mut sqlast::TableFactor,
    registry: &HashMap<SessionViewKey, StoredView>,
    current_database: &str,
    cte_names: &HashSet<String>,
) {
    match factor {
        sqlast::TableFactor::Table { name, alias, .. } => {
            let parts = object_name_parts(name);
            if parts.len() == 1 && cte_names.contains(&parts[0].to_ascii_lowercase()) {
                return;
            }
            let key = match parts.as_slice() {
                [view] => session_key(DEFAULT_CATALOG, current_database, view),
                [database, view] => session_key(DEFAULT_CATALOG, database, view),
                [catalog, database, view] => session_key(catalog, database, view),
                _ => return,
            };
            let Some(stored) = registry.get(&key) else {
                return;
            };
            let mut expanded = stored.query.as_ref().clone();
            expand_session_query(&mut expanded, registry, &key.database, &HashSet::new());
            let alias = alias.take().unwrap_or_else(|| sqlast::TableAlias {
                name: sqlast::Ident::new(parts.last().cloned().unwrap_or_else(|| key.view.clone())),
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
            expand_session_query(subquery.as_mut(), registry, current_database, cte_names);
        }
        sqlast::TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            expand_session_table_factor(
                &mut table_with_joins.relation,
                registry,
                current_database,
                cte_names,
            );
            for join in &mut table_with_joins.joins {
                expand_session_table_factor(
                    &mut join.relation,
                    registry,
                    current_database,
                    cte_names,
                );
            }
        }
        _ => {}
    }
}

fn session_key(catalog: &str, database: &str, view: &str) -> SessionViewKey {
    SessionViewKey {
        catalog: catalog.to_ascii_lowercase(),
        database: database.to_ascii_lowercase(),
        view: view.to_ascii_lowercase(),
    }
}

pub(super) fn expand_external_views(
    engine: &dyn ViewEngine,
    query: &mut sqlast::Query,
    context: ViewRequestContext<'_>,
) -> Result<(), String> {
    let mut stack = Vec::new();
    expand_external_query(engine, query, context, &HashSet::new(), &mut stack)
}

fn expand_external_query(
    engine: &dyn ViewEngine,
    query: &mut sqlast::Query,
    context: ViewRequestContext<'_>,
    visible_ctes: &HashSet<String>,
    stack: &mut Vec<ExternalViewKey>,
) -> Result<(), String> {
    let mut body_visible_ctes = visible_ctes.clone();
    if let Some(with_clause) = query.with.as_mut() {
        let recursive = with_clause.recursive;
        for cte in &mut with_clause.cte_tables {
            let name = cte.alias.name.value.to_ascii_lowercase();
            let mut cte_visible_ctes = body_visible_ctes.clone();
            if recursive {
                cte_visible_ctes.insert(name.clone());
            }
            expand_external_query(
                engine,
                cte.query.as_mut(),
                context,
                &cte_visible_ctes,
                stack,
            )?;
            body_visible_ctes.insert(name);
        }
    }
    expand_external_set_expr(
        engine,
        query.body.as_mut(),
        context,
        &body_visible_ctes,
        stack,
    )
}

fn expand_external_set_expr(
    engine: &dyn ViewEngine,
    expression: &mut sqlast::SetExpr,
    context: ViewRequestContext<'_>,
    cte_names: &HashSet<String>,
    stack: &mut Vec<ExternalViewKey>,
) -> Result<(), String> {
    match expression {
        sqlast::SetExpr::Select(select) => {
            for table_with_joins in &mut select.from {
                expand_external_table_factor(
                    engine,
                    &mut table_with_joins.relation,
                    context,
                    cte_names,
                    stack,
                )?;
                for join in &mut table_with_joins.joins {
                    expand_external_table_factor(
                        engine,
                        &mut join.relation,
                        context,
                        cte_names,
                        stack,
                    )?;
                }
            }
            Ok(())
        }
        sqlast::SetExpr::Query(query) => {
            expand_external_query(engine, query.as_mut(), context, cte_names, stack)
        }
        sqlast::SetExpr::SetOperation { left, right, .. } => {
            expand_external_set_expr(engine, left.as_mut(), context, cte_names, stack)?;
            expand_external_set_expr(engine, right.as_mut(), context, cte_names, stack)
        }
        _ => Ok(()),
    }
}

fn expand_external_table_factor(
    engine: &dyn ViewEngine,
    factor: &mut sqlast::TableFactor,
    context: ViewRequestContext<'_>,
    cte_names: &HashSet<String>,
    stack: &mut Vec<ExternalViewKey>,
) -> Result<(), String> {
    match factor {
        sqlast::TableFactor::Table { name, alias, .. } => {
            let parts = object_name_parts(name);
            if parts.len() == 1 && cte_names.contains(&parts[0].to_ascii_lowercase()) {
                return Ok(());
            }
            let Some(target) = external_rewrite_candidate(engine, &parts, context) else {
                return Ok(());
            };
            let Some(connector_context) = context.connector_context else {
                return Err("external view rewrite requires connector request context".to_string());
            };
            let view = match engine.resolve_external_view(&target, connector_context) {
                Ok(ExternalViewResolution::Table | ExternalViewResolution::Missing) => {
                    return Ok(());
                }
                Ok(ExternalViewResolution::View(view)) => view,
                // A failed table probe historically left the relation for the
                // normal catalog path.  Preserve that behavior while making
                // an undeclared view capability an explicit admission error.
                Err(error) if !error.starts_with("Unsupported:") => return Ok(()),
                Err(error) => return Err(error),
            };
            let key = (
                target.catalog.clone(),
                target.database.clone(),
                target.view.clone(),
            );
            if stack.contains(&key) {
                return Err(format!(
                    "circular view reference: {}.{}.{}",
                    key.0, key.1, key.2
                ));
            }
            let mut body = parse_external_view_sql(&view, &key)?;
            qualify_view_body_names(&mut body, &target.catalog, &view.default_database);
            stack.push(key);
            expand_external_query(
                engine,
                &mut body,
                ViewRequestContext {
                    current_catalog: Some(&target.catalog),
                    current_database: &view.default_database,
                    connector_context: Some(connector_context),
                },
                &HashSet::new(),
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
        sqlast::TableFactor::Derived { subquery, .. } => {
            expand_external_query(engine, subquery.as_mut(), context, cte_names, stack)
        }
        sqlast::TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            expand_external_table_factor(
                engine,
                &mut table_with_joins.relation,
                context,
                cte_names,
                stack,
            )?;
            for join in &mut table_with_joins.joins {
                expand_external_table_factor(
                    engine,
                    &mut join.relation,
                    context,
                    cte_names,
                    stack,
                )?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn external_rewrite_candidate(
    engine: &dyn ViewEngine,
    parts: &[String],
    context: ViewRequestContext<'_>,
) -> Option<ViewTarget> {
    resolve_external_target_parts(engine, parts, context)
        .ok()
        .flatten()
}

fn parse_external_view_sql(
    view: &ResolvedExternalView,
    key: &ExternalViewKey,
) -> Result<sqlast::Query, String> {
    let mut parser = Parser::new(&ViewSqlDialect)
        .try_with_sql(&view.sql)
        .map_err(|error| external_view_parse_error(key, &view.dialect, &error.to_string()))?;
    let statement = parser
        .parse_statement()
        .map_err(|error| external_view_parse_error(key, &view.dialect, &error.to_string()))?;
    let sqlast::Statement::Query(query) = statement else {
        return Err(format!(
            "iceberg view {}.{}.{} body is not a SELECT query",
            key.0, key.1, key.2
        ));
    };
    Ok(*query)
}

fn external_view_parse_error(key: &ExternalViewKey, dialect: &str, error: &str) -> String {
    format!(
        "parse iceberg view {}.{}.{} (representation dialect `{dialect}`) failed: {error}",
        key.0, key.1, key.2
    )
}

fn qualify_view_body_names(query: &mut sqlast::Query, catalog: &str, default_database: &str) {
    qualify_view_body_query(query, catalog, default_database, &HashSet::new());
}

fn qualify_view_body_query(
    query: &mut sqlast::Query,
    catalog: &str,
    default_database: &str,
    visible_ctes: &HashSet<String>,
) {
    let mut body_visible_ctes = visible_ctes.clone();
    if let Some(with_clause) = query.with.as_mut() {
        let recursive = with_clause.recursive;
        for cte in &mut with_clause.cte_tables {
            let name = cte.alias.name.value.to_ascii_lowercase();
            let mut cte_visible_ctes = body_visible_ctes.clone();
            if recursive {
                cte_visible_ctes.insert(name.clone());
            }
            qualify_view_body_query(
                cte.query.as_mut(),
                catalog,
                default_database,
                &cte_visible_ctes,
            );
            body_visible_ctes.insert(name);
        }
    }
    qualify_set_expr(
        query.body.as_mut(),
        catalog,
        default_database,
        &body_visible_ctes,
    );
}

fn qualify_set_expr(
    expression: &mut sqlast::SetExpr,
    catalog: &str,
    default_database: &str,
    cte_names: &HashSet<String>,
) {
    match expression {
        sqlast::SetExpr::Select(select) => {
            for table_with_joins in &mut select.from {
                qualify_table_factor(
                    &mut table_with_joins.relation,
                    catalog,
                    default_database,
                    cte_names,
                );
                for join in &mut table_with_joins.joins {
                    qualify_table_factor(&mut join.relation, catalog, default_database, cte_names);
                }
            }
        }
        sqlast::SetExpr::Query(query) => {
            qualify_view_body_query(query.as_mut(), catalog, default_database, cte_names)
        }
        sqlast::SetExpr::SetOperation { left, right, .. } => {
            qualify_set_expr(left.as_mut(), catalog, default_database, cte_names);
            qualify_set_expr(right.as_mut(), catalog, default_database, cte_names);
        }
        _ => {}
    }
}

fn qualify_table_factor(
    factor: &mut sqlast::TableFactor,
    catalog: &str,
    default_database: &str,
    cte_names: &HashSet<String>,
) {
    match factor {
        sqlast::TableFactor::Table { name, .. } => {
            let identifier_count = name
                .0
                .iter()
                .filter(|part| matches!(part, sqlast::ObjectNamePart::Identifier(_)))
                .count();
            match identifier_count {
                1 => {
                    if let Some(sqlast::ObjectNamePart::Identifier(table)) = name.0.first()
                        && cte_names.contains(&table.value.to_ascii_lowercase())
                    {
                        return;
                    }
                    let mut parts = vec![
                        sqlast::ObjectNamePart::Identifier(sqlast::Ident::new(catalog)),
                        sqlast::ObjectNamePart::Identifier(sqlast::Ident::new(default_database)),
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
            qualify_view_body_query(subquery.as_mut(), catalog, default_database, cte_names);
        }
        sqlast::TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            qualify_table_factor(
                &mut table_with_joins.relation,
                catalog,
                default_database,
                cte_names,
            );
            for join in &mut table_with_joins.joins {
                qualify_table_factor(&mut join.relation, catalog, default_database, cte_names);
            }
        }
        _ => {}
    }
}

fn object_name_parts(name: &sqlast::ObjectName) -> Vec<String> {
    name.0
        .iter()
        .filter_map(|part| match part {
            sqlast::ObjectNamePart::Identifier(identifier) => Some(identifier.value.clone()),
            _ => None,
        })
        .collect()
}
