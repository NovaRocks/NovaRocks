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

//! Iceberg-catalog view DDL flows and name-target resolution.
//!
//! A view name routes to an iceberg catalog when it is a 3-part name
//! naming a registered iceberg catalog, or a 1/2-part name while a
//! session catalog (`SET CATALOG`) is active. Everything else stays a
//! session view in `StandaloneState::views`.

use std::sync::Arc;

use crate::connector::backend::CreateViewRequest;
use crate::engine::{StandaloneState, StatementResult};
use crate::sql::analysis::OutputColumn;
use crate::sql::parser::ast::TableColumnDef;
use novarocks_catalog::identifier::normalize_identifier;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct IcebergViewTarget {
    pub catalog: String,
    pub namespace: String,
    pub view: String,
}

/// Resolve a view name (already split into identifier parts) to an iceberg
/// target. `Ok(None)` means "session view" (default catalog). The catalog
/// must exist in the iceberg registry; an unknown catalog is an error.
pub(crate) fn resolve_iceberg_view_target_parts(
    state: &Arc<StandaloneState>,
    parts: &[String],
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<Option<IcebergViewTarget>, String> {
    let session_catalog =
        current_catalog.filter(|catalog| !catalog.eq_ignore_ascii_case("default_catalog"));
    let (catalog, namespace, view) = match parts {
        [catalog, db, view] => {
            if catalog.eq_ignore_ascii_case("default_catalog") {
                return Ok(None);
            }
            (catalog.clone(), db.clone(), view.clone())
        }
        [db, view] => match session_catalog {
            Some(catalog) => (catalog.to_string(), db.clone(), view.clone()),
            None => return Ok(None),
        },
        [view] => match session_catalog {
            Some(catalog) => (
                catalog.to_string(),
                current_database.to_string(),
                view.clone(),
            ),
            None => return Ok(None),
        },
        _ => return Err(format!("invalid view name: {}", parts.join("."))),
    };
    let target = IcebergViewTarget {
        catalog: normalize_identifier(&catalog)?,
        namespace: normalize_identifier(&namespace)?,
        view: normalize_identifier(&view)?,
    };
    // Validate catalog existence eagerly so DDL gets a clear error.
    state
        .iceberg_catalogs
        .read()
        .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?
        .get(&target.catalog)?;
    Ok(Some(target))
}

/// Helper for sqlparser names: extract identifier parts then resolve.
pub(crate) fn resolve_iceberg_view_target(
    state: &Arc<StandaloneState>,
    name: &sqlparser::ast::ObjectName,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<Option<IcebergViewTarget>, String> {
    let parts: Vec<String> = name
        .0
        .iter()
        .filter_map(|part| match part {
            sqlparser::ast::ObjectNamePart::Identifier(ident) => Some(ident.value.clone()),
            _ => None,
        })
        .collect();
    resolve_iceberg_view_target_parts(state, &parts, current_catalog, current_database)
}

/// Create (or replace) a view on an iceberg REST catalog. The original body is
/// persisted verbatim; an expanded copy is analyzed so views over views
/// type-check. Output columns come from the analyzer and may be renamed by the
/// optional column-alias list.
pub(crate) fn create_iceberg_view(
    state: &Arc<StandaloneState>,
    target: &IcebergViewTarget,
    stmt: sqlparser::ast::CreateView,
) -> Result<StatementResult, String> {
    if stmt.materialized {
        return Err(
            "CREATE MATERIALIZED VIEW must go through the materialized-view DDL path".to_string(),
        );
    }
    let backend = state
        .connectors
        .read()
        .expect("connector registry read")
        .catalog_backend("iceberg")?;

    // Views and tables share the namespace on iceberg catalogs; reject
    // shadowing instead of letting the REST server pick a winner.
    if backend.table_exists(&target.catalog, &target.namespace, &target.view)? {
        return Err(format!(
            "a table named {}.{}.{} already exists",
            target.catalog, target.namespace, target.view
        ));
    }
    if stmt.if_not_exists
        && backend.view_exists(&target.catalog, &target.namespace, &target.view)?
    {
        return Ok(StatementResult::Ok);
    }

    // Persist the original body; analyze an expanded copy so views over
    // views type-check. Bare names in the body resolve against the view's
    // own catalog/namespace — identical to read-time qualification.
    let view_sql = stmt.query.to_string();
    let mut analyzed_query = (*stmt.query).clone();
    crate::engine::iceberg_view_rewrite::expand_iceberg_views_in_query(
        state,
        &mut analyzed_query,
        Some(&target.catalog),
        &target.namespace,
    )?;
    let output_columns =
        analyze_view_query(state, &target.catalog, &target.namespace, &analyzed_query)?;
    let columns = view_columns(&output_columns, &stmt.columns)?;

    backend.create_view(CreateViewRequest {
        catalog: target.catalog.clone(),
        namespace: target.namespace.clone(),
        view: target.view.clone(),
        columns,
        view_sql,
        comment: stmt.comment.clone(),
        or_replace: stmt.or_replace,
        properties: vec![],
    })?;
    Ok(StatementResult::Ok)
}

/// Drop a view on an iceberg REST catalog. `IF EXISTS` swallows an unknown
/// view; dropping a name that is actually a table reports an explicit
/// type-mismatch error so callers reach for `DROP TABLE` instead.
pub(crate) fn drop_iceberg_view(
    state: &Arc<StandaloneState>,
    target: &IcebergViewTarget,
    if_exists: bool,
) -> Result<(), String> {
    let backend = state
        .connectors
        .read()
        .expect("connector registry read")
        .catalog_backend("iceberg")?;
    match backend.drop_view(&target.catalog, &target.namespace, &target.view) {
        Ok(()) => Ok(()),
        Err(err) if err.contains("unknown view") => {
            if if_exists {
                return Ok(());
            }
            if backend.table_exists(&target.catalog, &target.namespace, &target.view)? {
                return Err(format!(
                    "{}.{}.{} is a table, use DROP TABLE",
                    target.catalog, target.namespace, target.view
                ));
            }
            Err(err)
        }
        Err(err) => Err(err),
    }
}

/// Analyze the (already expanded) view body and return its user-visible output
/// columns, skipping optimizer-internal pseudo-columns.
fn analyze_view_query(
    state: &Arc<StandaloneState>,
    catalog: &str,
    namespace: &str,
    query: &sqlparser::ast::Query,
) -> Result<Vec<OutputColumn>, String> {
    let catalog_service_snapshot = crate::engine::catalog_service_snapshot(state);
    let connectors_snapshot = state
        .connectors
        .read()
        .expect("standalone connector registry read lock")
        .clone();
    let provider = crate::engine::build_catalog_service_provider(
        Some(catalog),
        &catalog_service_snapshot,
        &connectors_snapshot,
        crate::sql::catalog::TableLookupMode::SchemaOnly,
    );
    let (resolved, _ctes, _factory) = crate::sql::analyzer::analyze(query, &provider, namespace)
        .map_err(|e| format!("analyze view definition failed: {e}"))?;
    let columns: Vec<OutputColumn> = resolved
        .output_columns
        .into_iter()
        .filter(|column| !column.is_internal)
        .collect();
    if columns.is_empty() {
        return Err("CREATE VIEW: SELECT produced no output columns".to_string());
    }
    Ok(columns)
}

/// Map analyzed output columns to backend column definitions, applying the
/// optional column-alias list. The alias list, when present, must match the
/// number of output columns exactly.
fn view_columns(
    output: &[OutputColumn],
    aliases: &[sqlparser::ast::ViewColumnDef],
) -> Result<Vec<TableColumnDef>, String> {
    if !aliases.is_empty() && aliases.len() != output.len() {
        return Err(format!(
            "view column list has {} names but the SELECT produces {} columns",
            aliases.len(),
            output.len()
        ));
    }
    output
        .iter()
        .enumerate()
        .map(|(idx, column)| {
            let name = if aliases.is_empty() {
                column.name.clone()
            } else {
                aliases[idx].name.value.clone()
            };
            let data_type =
                crate::engine::iceberg_ctas::arrow_data_type_to_sql_type(&column.data_type)?;
            Ok(TableColumnDef {
                name,
                data_type,
                nullable: column.nullable,
                aggregation: None,
                default: None,
            })
        })
        .collect()
}
