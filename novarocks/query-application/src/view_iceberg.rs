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

use std::error::Error;

use crate::api::{CommandError, StatementExternalEffectDisposition};
use crate::persisted_query_definition::{PersistedQueryDefinition, PersistedQueryDialect};
use crate::sql::dml_admission::DmlAdmissionError;
use crate::view::{
    CreateExternalViewRequest, CreateViewCommand, ViewEngine, ViewRequestContext,
    ViewStatementResult, ViewTarget,
};
use novarocks_parser::{
    Span,
    ast::{ObjectName, TrinoDropView},
    build::{ident_from_catalog, object_name},
    printer::print_create_view_definition,
};
use novarocks_spi::connector::{
    ConnectorMutationFailure, ConnectorMutationFailureKind, ConnectorObjectKind, DropPolicy,
};
use novarocks_types::naming::normalize_identifier;

use crate::view_service::{DEFAULT_CATALOG, build_query_result};

pub(crate) fn require_mutable_view_target(
    parts: &[String],
    context: ViewRequestContext<'_>,
) -> Result<(), crate::api::CommandError> {
    let target = novarocks_types::naming::resolve_catalog_table_name(
        parts,
        Some(context.current_catalog.unwrap_or(DEFAULT_CATALOG)),
        context.current_database,
    )?;
    if novarocks_sql::system_table::is_system_table_name(&target.catalog, &target.namespace) {
        return Err(crate::api::CommandError::new(
            crate::api::CommandErrorKind::Unsupported,
            "system catalogs and information_schema are read-only",
        ));
    }
    Ok(())
}

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
    statement: CreateViewCommand<'_>,
    context: ViewRequestContext<'_>,
    local_views: &crate::view::LocalViewSnapshot,
) -> Result<ViewStatementResult, crate::api::CommandError> {
    let connector_context = context
        .connector_context
        .ok_or_else(|| "external view mutation requires connector request context".to_string())?;

    let definition = PersistedQueryDefinition::new(
        statement.raw_query,
        PersistedQueryDialect::Trino,
        context.current_catalog.unwrap_or(DEFAULT_CATALOG),
        context.current_database,
    )?;
    let columns = engine.analyze_view_definition(
        crate::view::ViewDefinitionAnalysisRequest {
            catalog: &definition.resolution.default_catalog,
            database: &definition.resolution.default_database,
            target: &target,
            query: statement.query,
            local_views,
        },
        connector_context,
    )?;
    let name = qualified_view_name(&target);
    engine
        .create_external_view(
            CreateExternalViewRequest {
                target,
                columns,
                definition,
                comment: statement.comment.map(str::to_owned),
                or_replace: statement.or_replace,
                if_not_exists: statement.if_not_exists,
                properties: Vec::new(),
            },
            connector_context,
        )
        .map_err(|error| {
            present_view_mutation_error(
                error,
                ViewMutation::Create,
                &name,
                statement.source,
                statement.span,
            )
        })?;
    Ok(ViewStatementResult::Ok)
}

pub(crate) fn drop_external_view(
    engine: &dyn ViewEngine,
    target: &ViewTarget,
    source: &str,
    statement: &TrinoDropView,
    context: ViewRequestContext<'_>,
) -> Result<(), crate::api::CommandError> {
    let connector_context = context
        .connector_context
        .ok_or_else(|| "external view mutation requires connector request context".to_string())?;
    engine
        .drop_external_view(
            target,
            connector_context,
            if statement.exists {
                DropPolicy::NoOpIfMissing
            } else {
                DropPolicy::FailIfMissing
            },
        )
        .map_err(|error| {
            present_view_mutation_error(
                error,
                ViewMutation::Drop,
                &qualified_view_name(target),
                source,
                statement.span,
            )
        })
}

fn qualified_view_name(target: &ViewTarget) -> String {
    format!("{}.{}.{}", target.catalog, target.database, target.view)
}

enum ViewMutation {
    Create,
    Drop,
}

fn present_view_mutation_error(
    error: CommandError,
    operation: ViewMutation,
    name: &str,
    source: &str,
    span: Span,
) -> CommandError {
    let disposition = error.external_effect_disposition();
    if disposition != Some(StatementExternalEffectDisposition::KnownUncommitted) {
        return error;
    }
    let Some(cause) = error.failure_cause() else {
        return error;
    };
    let mut source_error: &(dyn Error + 'static) = cause;
    let (failure, kind) = loop {
        if let Some(failure) = source_error.downcast_ref::<ConnectorMutationFailure>()
            && let Some(kind) = failure.existing_object()
        {
            break (failure.kind(), kind);
        }
        let Some(next) = source_error.source() else {
            return error;
        };
        source_error = next;
    };
    let (code, message) = match (operation, failure, kind) {
        (ViewMutation::Create, ConnectorMutationFailureKind::AlreadyExists, kind) => {
            let label = match kind {
                ConnectorObjectKind::Table => "Table",
                ConnectorObjectKind::View => "View",
                ConnectorObjectKind::MaterializedView { .. } => "Materialized view",
            };
            (
                DmlAdmissionError::TableAlreadyExists,
                format!("{label} already exists: '{name}'"),
            )
        }
        (
            ViewMutation::Drop,
            ConnectorMutationFailureKind::InvalidRequest,
            ConnectorObjectKind::MaterializedView { .. },
        ) => (
            DmlAdmissionError::MaterializedViewTarget,
            format!(
                "View '{name}' does not exist, but a materialized view with that name exists. Did you mean DROP MATERIALIZED VIEW {name}?"
            ),
        ),
        _ => return error,
    };
    CommandError::from_user_error_with_cause(
        code.to_user_error(source, span, message),
        cause.clone(),
    )
    .with_compile_control(error.compile_control_error())
    .with_external_effect_disposition(disposition)
}

pub(crate) fn show_create_view(
    engine: &dyn ViewEngine,
    name: &ObjectName,
    context: ViewRequestContext<'_>,
) -> Result<ViewStatementResult, crate::api::CommandError> {
    let parts = name
        .parts
        .iter()
        .map(|part| part.value.clone())
        .collect::<Vec<_>>();
    let Some(target) = resolve_external_target_parts(engine, &parts, context)? else {
        return Err("SHOW CREATE VIEW only supports views in iceberg catalogs"
            .to_string()
            .into());
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
    let name = object_name(
        [&target.catalog, &target.database, &target.view]
            .into_iter()
            .map(|part| ident_from_catalog(part.as_str()))
            .collect(),
    )
    .map_err(|error| error.to_string())?;
    let ddl = print_create_view_definition(
        &name,
        view.comment.as_deref(),
        &view.definition.raw_query_source,
    )
    .map_err(|error| error.to_string())?;
    Ok(ViewStatementResult::Query(build_query_result(vec![
        ("View".to_string(), vec![target.view]),
        ("Create View".to_string(), vec![ddl]),
    ])?))
}
