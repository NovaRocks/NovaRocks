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

use crate::view::{
    CreateExternalViewRequest, ViewEngine, ViewRequestContext, ViewSqlDialect, ViewStatementResult,
    ViewTarget,
};
use novarocks_catalog::identifier::normalize_identifier;
use novarocks_spi::connector::DropPolicy;
use sqlparser::ast::{CreateView, ObjectName, ObjectNamePart};
use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;

use super::{DEFAULT_CATALOG, build_query_result};

pub(super) fn resolve_external_target(
    engine: &dyn ViewEngine,
    name: &ObjectName,
    context: ViewRequestContext<'_>,
) -> Result<Option<ViewTarget>, String> {
    let parts = name
        .0
        .iter()
        .filter_map(|part| match part {
            ObjectNamePart::Identifier(identifier) => Some(identifier.value.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    resolve_external_target_parts(engine, &parts, context)
}

pub(super) fn resolve_external_target_parts(
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

pub(super) fn create_external_view(
    engine: &dyn ViewEngine,
    target: ViewTarget,
    statement: CreateView,
    context: ViewRequestContext<'_>,
) -> Result<ViewStatementResult, String> {
    if statement.materialized {
        return Err(
            "CREATE MATERIALIZED VIEW must go through the materialized-view DDL path".to_string(),
        );
    }
    let connector_context = context
        .connector_context
        .ok_or_else(|| "external view mutation requires connector request context".to_string())?;

    let view_sql = statement.query.to_string();
    let mut analyzed_query = statement.query.as_ref().clone();
    super::rewrite::expand_external_views(
        engine,
        &mut analyzed_query,
        ViewRequestContext {
            current_catalog: Some(&target.catalog),
            current_database: &target.database,
            connector_context: Some(connector_context),
        },
    )?;
    let mut columns = engine.analyze_external_view(
        &target.catalog,
        &target.database,
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
            column.name = alias.name.value.clone();
        }
    }
    engine.create_external_view(
        CreateExternalViewRequest {
            target,
            columns,
            sql: view_sql,
            comment: statement.comment,
            or_replace: statement.or_replace,
            if_not_exists: statement.if_not_exists,
            properties: Vec::new(),
        },
        connector_context,
    )?;
    Ok(ViewStatementResult::Ok)
}

pub(super) fn drop_external_view(
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

pub(super) fn show_create_view(
    engine: &dyn ViewEngine,
    sql: &str,
    context: ViewRequestContext<'_>,
) -> Result<ViewStatementResult, String> {
    let name = parse_show_create_view(sql)?;
    let Some(target) = resolve_external_target(engine, &name, context)? else {
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
    ddl.push_str(&format!("\nAS {};", view.sql));
    Ok(ViewStatementResult::Query(build_query_result(vec![
        ("View".to_string(), vec![target.view]),
        ("Create View".to_string(), vec![ddl]),
    ])?))
}

fn parse_show_create_view(sql: &str) -> Result<ObjectName, String> {
    let mut parser = Parser::new(&ViewSqlDialect)
        .try_with_sql(sql)
        .map_err(|error| format!("parse SHOW CREATE VIEW: {error}"))?;
    parser
        .expect_keyword(Keyword::SHOW)
        .map_err(|error| format!("parse SHOW CREATE VIEW: {error}"))?;
    parser
        .expect_keyword(Keyword::CREATE)
        .map_err(|error| format!("parse SHOW CREATE VIEW: {error}"))?;
    parser
        .expect_keyword(Keyword::VIEW)
        .map_err(|error| format!("parse SHOW CREATE VIEW: {error}"))?;
    parser
        .parse_object_name(false)
        .map_err(|error| format!("parse SHOW CREATE VIEW view name: {error}"))
}

pub(super) fn parse_show_views(sql: &str) -> Result<Option<String>, String> {
    let mut parser = Parser::new(&ViewSqlDialect)
        .try_with_sql(sql)
        .map_err(|error| format!("parse SHOW VIEWS: {error}"))?;
    parser
        .expect_keyword(Keyword::SHOW)
        .map_err(|error| format!("parse SHOW VIEWS: {error}"))?;
    parser
        .expect_keyword(Keyword::VIEWS)
        .map_err(|error| format!("parse SHOW VIEWS: {error}"))?;
    let database = if parser.parse_keyword(Keyword::FROM) {
        Some(
            parser
                .parse_identifier()
                .map_err(|error| format!("parse SHOW VIEWS database after FROM: {error}"))?
                .value,
        )
    } else {
        None
    };
    if parser.parse_keyword(Keyword::LIKE) || parser.parse_keyword(Keyword::WHERE) {
        return Err("SHOW VIEWS LIKE/WHERE is not supported".to_string());
    }
    Ok(database)
}
