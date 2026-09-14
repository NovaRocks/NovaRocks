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
    ExternalViewResolution, ResolvedExternalView, ViewEngine, ViewRequestContext, ViewTarget,
};
use novarocks_parser::{
    Span,
    ast::{Ident, ObjectName, Query, SetExpr, Statement, TableAlias, TableFactor},
};

use crate::view_iceberg::resolve_external_target_parts;
use crate::view_service::{DEFAULT_CATALOG, SessionViewKey, StoredView};

type ExternalViewKey = (String, String, String);

pub(crate) fn expand_session_views(
    query: &mut Query,
    registry: &HashMap<SessionViewKey, StoredView>,
    current_database: &str,
) {
    if registry.is_empty() {
        return;
    }
    expand_session_query(query, registry, current_database, &HashSet::new());
}

fn expand_session_query(
    query: &mut Query,
    registry: &HashMap<SessionViewKey, StoredView>,
    current_database: &str,
    visible_ctes: &HashSet<String>,
) {
    let mut body_visible_ctes = visible_ctes.clone();
    if let Some(with_clause) = query.with.as_mut() {
        let recursive = with_clause.recursive;
        for cte in &mut with_clause.ctes {
            let name = cte.name.value.to_ascii_lowercase();
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
    expression: &mut SetExpr,
    registry: &HashMap<SessionViewKey, StoredView>,
    current_database: &str,
    cte_names: &HashSet<String>,
) {
    match expression {
        SetExpr::Select(select) => {
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
        SetExpr::Query(query) => {
            expand_session_query(query.as_mut(), registry, current_database, cte_names)
        }
        SetExpr::SetOperation(operation) => {
            expand_session_set_expr(
                operation.left.as_mut(),
                registry,
                current_database,
                cte_names,
            );
            expand_session_set_expr(
                operation.right.as_mut(),
                registry,
                current_database,
                cte_names,
            );
        }
        _ => {}
    }
}

fn expand_session_table_factor(
    factor: &mut TableFactor,
    registry: &HashMap<SessionViewKey, StoredView>,
    current_database: &str,
    cte_names: &HashSet<String>,
) {
    match factor {
        TableFactor::Table {
            name, alias, span, ..
        } => {
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
            qualify_view_body_names(
                &mut expanded,
                &stored.definition.resolution.default_catalog,
                &stored.definition.resolution.default_database,
            );
            expand_session_query(
                &mut expanded,
                registry,
                &stored.definition.resolution.default_database,
                &HashSet::new(),
            );
            let alias = alias.take().unwrap_or_else(|| TableAlias {
                name: synthetic_ident(
                    parts.last().cloned().unwrap_or_else(|| key.view.clone()),
                    *span,
                ),
                columns: Vec::new(),
                explicit_as: false,
                span: *span,
            });
            *factor = TableFactor::Derived {
                lateral: false,
                subquery: Box::new(expanded),
                hints: Vec::new(),
                alias: Some(alias),
                span: *span,
            };
        }
        TableFactor::Derived { subquery, .. } => {
            expand_session_query(subquery.as_mut(), registry, current_database, cte_names);
        }
        TableFactor::NestedJoin {
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

pub(crate) fn expand_external_views(
    engine: &dyn ViewEngine,
    query: &mut Query,
    context: ViewRequestContext<'_>,
) -> Result<(), String> {
    let mut stack = Vec::new();
    expand_external_query(engine, query, context, &HashSet::new(), &mut stack)
}

fn expand_external_query(
    engine: &dyn ViewEngine,
    query: &mut Query,
    context: ViewRequestContext<'_>,
    visible_ctes: &HashSet<String>,
    stack: &mut Vec<ExternalViewKey>,
) -> Result<(), String> {
    let mut body_visible_ctes = visible_ctes.clone();
    if let Some(with_clause) = query.with.as_mut() {
        let recursive = with_clause.recursive;
        for cte in &mut with_clause.ctes {
            let name = cte.name.value.to_ascii_lowercase();
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
    expression: &mut SetExpr,
    context: ViewRequestContext<'_>,
    cte_names: &HashSet<String>,
    stack: &mut Vec<ExternalViewKey>,
) -> Result<(), String> {
    match expression {
        SetExpr::Select(select) => {
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
        SetExpr::Query(query) => {
            expand_external_query(engine, query.as_mut(), context, cte_names, stack)
        }
        SetExpr::SetOperation(operation) => {
            expand_external_set_expr(engine, operation.left.as_mut(), context, cte_names, stack)?;
            expand_external_set_expr(engine, operation.right.as_mut(), context, cte_names, stack)
        }
        _ => Ok(()),
    }
}

fn expand_external_table_factor(
    engine: &dyn ViewEngine,
    factor: &mut TableFactor,
    context: ViewRequestContext<'_>,
    cte_names: &HashSet<String>,
    stack: &mut Vec<ExternalViewKey>,
) -> Result<(), String> {
    match factor {
        TableFactor::Table {
            name, alias, span, ..
        } => {
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
            qualify_view_body_names(
                &mut body,
                &view.definition.resolution.default_catalog,
                &view.definition.resolution.default_database,
            );
            stack.push(key);
            expand_external_query(
                engine,
                &mut body,
                ViewRequestContext {
                    current_catalog: Some(&view.definition.resolution.default_catalog),
                    current_database: &view.definition.resolution.default_database,
                    connector_context: Some(connector_context),
                },
                &HashSet::new(),
                stack,
            )?;
            stack.pop();

            let alias = alias.take().unwrap_or_else(|| TableAlias {
                name: synthetic_ident(parts.last().cloned().unwrap_or_default(), *span),
                columns: Vec::new(),
                explicit_as: false,
                span: *span,
            });
            *factor = TableFactor::Derived {
                lateral: false,
                subquery: Box::new(body),
                hints: Vec::new(),
                alias: Some(alias),
                span: *span,
            };
            Ok(())
        }
        TableFactor::Derived { subquery, .. } => {
            expand_external_query(engine, subquery.as_mut(), context, cte_names, stack)
        }
        TableFactor::NestedJoin {
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
) -> Result<Query, String> {
    let statements = novarocks_parser::parse(&view.definition.raw_query_source)
        .map_err(|error| external_view_parse_error(key, "starrocks", &error.to_string()))?;
    let [Statement::Query(query)] = statements.as_slice() else {
        return Err(format!(
            "iceberg view {}.{}.{} body is not a SELECT query",
            key.0, key.1, key.2
        ));
    };
    Ok(query.clone())
}

fn external_view_parse_error(key: &ExternalViewKey, dialect: &str, error: &str) -> String {
    format!(
        "parse iceberg view {}.{}.{} (representation dialect `{dialect}`) failed: {error}",
        key.0, key.1, key.2
    )
}

fn qualify_view_body_names(query: &mut Query, catalog: &str, default_database: &str) {
    qualify_view_body_query(query, catalog, default_database, &HashSet::new());
}

fn qualify_view_body_query(
    query: &mut Query,
    catalog: &str,
    default_database: &str,
    visible_ctes: &HashSet<String>,
) {
    let mut body_visible_ctes = visible_ctes.clone();
    if let Some(with_clause) = query.with.as_mut() {
        let recursive = with_clause.recursive;
        for cte in &mut with_clause.ctes {
            let name = cte.name.value.to_ascii_lowercase();
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
    expression: &mut SetExpr,
    catalog: &str,
    default_database: &str,
    cte_names: &HashSet<String>,
) {
    match expression {
        SetExpr::Select(select) => {
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
        SetExpr::Query(query) => {
            qualify_view_body_query(query.as_mut(), catalog, default_database, cte_names)
        }
        SetExpr::SetOperation(operation) => {
            qualify_set_expr(
                operation.left.as_mut(),
                catalog,
                default_database,
                cte_names,
            );
            qualify_set_expr(
                operation.right.as_mut(),
                catalog,
                default_database,
                cte_names,
            );
        }
        _ => {}
    }
}

fn qualify_table_factor(
    factor: &mut TableFactor,
    catalog: &str,
    default_database: &str,
    cte_names: &HashSet<String>,
) {
    match factor {
        TableFactor::Table { name, .. } => {
            let identifier_count = name.parts.len();
            match identifier_count {
                1 => {
                    if let Some(table) = name.parts.first()
                        && cte_names.contains(&table.value.to_ascii_lowercase())
                    {
                        return;
                    }
                    let mut parts = vec![
                        synthetic_ident(catalog, name.span),
                        synthetic_ident(default_database, name.span),
                    ];
                    parts.append(&mut name.parts);
                    name.parts = parts;
                }
                2 => {
                    name.parts.insert(0, synthetic_ident(catalog, name.span));
                }
                _ => {}
            }
        }
        TableFactor::Derived { subquery, .. } => {
            qualify_view_body_query(subquery.as_mut(), catalog, default_database, cte_names);
        }
        TableFactor::NestedJoin {
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

fn object_name_parts(name: &ObjectName) -> Vec<String> {
    name.parts
        .iter()
        .map(|identifier| identifier.value.clone())
        .collect()
}

fn synthetic_ident(value: impl Into<String>, span: Span) -> Ident {
    Ident {
        value: value.into(),
        quoted: false,
        quote_style: None,
        span,
    }
}
