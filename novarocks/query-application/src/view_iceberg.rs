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

use crate::persisted_query_definition::{PersistedQueryDefinition, PersistedQueryDialect};
use crate::view::{
    CreateExternalViewRequest, ViewEngine, ViewRequestContext, ViewStatementResult, ViewTarget,
};
use novarocks_parser::{
    ast::{CreateView, ObjectName},
    printer,
};
use novarocks_spi::connector::DropPolicy;
use novarocks_types::naming::normalize_identifier;

use crate::view_service::{DEFAULT_CATALOG, build_query_result, parse_query};

pub(crate) fn resolve_external_target_parts(
    _engine: &dyn ViewEngine,
    parts: &[String],
    context: ViewRequestContext<'_>,
) -> Result<Option<ViewTarget>, String> {
    let active_catalog = context
        .current_catalog
        .filter(|catalog| !catalog.eq_ignore_ascii_case(DEFAULT_CATALOG));
    let (catalog, database, view) = match parts {
        [catalog, database, view] => {
            if catalog.eq_ignore_ascii_case(DEFAULT_CATALOG) {
                return Ok(None);
            }
            (catalog.clone(), database.clone(), view.clone())
        }
        [database, view] => match active_catalog {
            Some(catalog) => (catalog.to_string(), database.clone(), view.clone()),
            None => return Ok(None),
        },
        [view] => match active_catalog {
            Some(catalog) => (
                catalog.to_string(),
                context.current_database.to_string(),
                view.clone(),
            ),
            None => return Ok(None),
        },
        _ => return Err(format!("invalid view name: {}", parts.join("."))),
    };
    let target = ViewTarget {
        catalog: normalize_identifier(&catalog)?,
        database: normalize_identifier(&database)?,
        view: normalize_identifier(&view)?,
    };
    Ok(Some(target))
}

pub(crate) fn create_external_view(
    engine: &dyn ViewEngine,
    target: ViewTarget,
    statement: &CreateView,
    context: ViewRequestContext<'_>,
) -> Result<ViewStatementResult, String> {
    let connector_context = context
        .connector_context
        .ok_or_else(|| "external view mutation requires connector request context".to_string())?;

    let definition = PersistedQueryDefinition::new(
        printer::print_query(&statement.query),
        PersistedQueryDialect::StarRocks,
        context.current_catalog.unwrap_or(DEFAULT_CATALOG),
        context.current_database,
    )?;
    let mut analyzed_query = parse_query(&definition.raw_query_source)?.as_ref().clone();
    crate::view_rewrite::expand_external_views(
        engine,
        &mut analyzed_query,
        ViewRequestContext {
            current_catalog: Some(&definition.resolution.default_catalog),
            current_database: &definition.resolution.default_database,
            connector_context: Some(connector_context),
        },
    )?;
    let mut columns = engine.analyze_external_view(
        &definition.resolution.default_catalog,
        &definition.resolution.default_database,
        &analyzed_query,
        connector_context,
    )?;
    if columns.is_empty() {
        return Err("CREATE VIEW: SELECT produced no output columns".to_string());
    }
    if !statement.columns.is_empty() && statement.columns.len() != columns.len() {
        return Err(format!(
            "view column list has {} names but the SELECT produces {} columns",
            statement.columns.len(),
            columns.len()
        ));
    }
    if !statement.columns.is_empty() {
        for (column, alias) in columns.iter_mut().zip(&statement.columns) {
            column.name = alias.value.clone();
        }
    }
    engine.create_external_view(
        CreateExternalViewRequest {
            target,
            columns,
            definition,
            comment: statement.comment.as_ref().map(literal_to_string),
            or_replace: statement.or_replace,
            if_not_exists: statement.if_not_exists,
            properties: Vec::new(),
        },
        connector_context,
    )?;
    Ok(ViewStatementResult::Ok)
}

fn literal_to_string(literal: &novarocks_parser::ast::Literal) -> String {
    match &literal.kind {
        novarocks_parser::ast::LiteralKind::String(value)
        | novarocks_parser::ast::LiteralKind::HexString(value)
        | novarocks_parser::ast::LiteralKind::Number(value) => value.clone(),
        novarocks_parser::ast::LiteralKind::Null => "NULL".to_string(),
        novarocks_parser::ast::LiteralKind::Boolean(value) => value.to_string(),
    }
}

pub(crate) fn drop_external_view(
    engine: &dyn ViewEngine,
    target: &ViewTarget,
    if_exists: bool,
    context: ViewRequestContext<'_>,
) -> Result<(), String> {
    let connector_context = context
        .connector_context
        .ok_or_else(|| "external view mutation requires connector request context".to_string())?;
    engine.drop_external_view(
        target,
        connector_context,
        if if_exists {
            DropPolicy::NoOpIfMissing
        } else {
            DropPolicy::FailIfMissing
        },
    )
}

pub(crate) fn show_create_view(
    engine: &dyn ViewEngine,
    name: &ObjectName,
    context: ViewRequestContext<'_>,
) -> Result<ViewStatementResult, String> {
    let parts = name
        .parts
        .iter()
        .map(|part| part.value.clone())
        .collect::<Vec<_>>();
    let Some(target) = resolve_external_target_parts(engine, &parts, context)? else {
        return Err("SHOW CREATE VIEW only supports views in iceberg catalogs".to_string());
    };
    let connector_context = context
        .connector_context
        .ok_or_else(|| "SHOW CREATE VIEW requires connector request context".to_string())?;
    let view = engine
        .load_external_view(&target, connector_context)?
        .ok_or_else(|| {
            format!(
                "unknown view: {}.{}.{}",
                target.catalog, target.database, target.view
            )
        })?;
    let columns = view
        .column_names
        .iter()
        .map(|name| format!("`{name}`"))
        .collect::<Vec<_>>()
        .join(", ");
    let mut ddl = format!(
        "CREATE VIEW `{}`.`{}`.`{}` ({})",
        target.catalog, target.database, target.view, columns
    );
    if let Some(comment) = &view.comment {
        ddl.push_str(&format!("\nCOMMENT \"{}\"", comment.replace('"', "\\\"")));
    }
    ddl.push_str(&format!("\nAS {};", view.definition.raw_query_source));
    Ok(ViewStatementResult::Query(build_query_result(vec![
        ("View".to_string(), vec![target.view]),
        ("Create View".to_string(), vec![ddl]),
    ])?))
}
