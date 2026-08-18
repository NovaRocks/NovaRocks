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

//! information_schema AST rewriter for standalone queries.
//!
//! StarRocks exposes information_schema tables (`schemata`, `tables`, ...) as
//! real tables on the FE side. NovaRocks standalone delegates their schema and
//! row materialization to the frontend-injected `SystemCatalog` port.
//!
//! The rewriter replaces system-table references with VALUES-backed derived
//! tables, which the standard SQL pipeline handles like ordinary relations.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
    StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use sqlparser::ast as sqlast;

use crate::catalog_application::query_catalog::QueryCatalogService;
use crate::catalog_application::system_catalog::SystemCatalog;
use novarocks_catalog::schema::ColumnDef;
use novarocks_spi::connector::ConnectorControlRegistry;

pub(crate) const INFORMATION_SCHEMA_DB: &str = "information_schema";

// ---------------------------------------------------------------------------
// AST rewriter: substitute virtual-table refs with a VALUES derived table.
// ---------------------------------------------------------------------------
//
// StarRocks routes information_schema scans through a `SchemaScanNode` that
// produces rows at the BE; NovaRocks standalone has no equivalent BE-side
// generator, so we materialize rows here against the frontend-injected leaf
// ports
// and rewrite each `FROM information_schema.X` into a derived table backed by
// a VALUES expression. The standard SQL pipeline (analyzer → planner →
// codegen → pipeline) then handles projection / WHERE / aggregation / ORDER BY
// like any other base table.
//
// Hooked from `engine::mod::execute_statement` for `Statement::Query`, before
// `execute_query` runs. CTE bodies and subqueries are walked recursively.

/// Walk a query AST and replace virtual-table references with VALUES-backed
/// derived tables. Returns `Ok(())` even when no virtual tables are matched.
///
/// The three ports are taken individually because this rewrite is read-only
/// system-table materialization: it needs the local catalog snapshot for
/// default-catalog schema names, exact connector control for external
/// namespace facts, and the injected system catalog for row production.
pub fn rewrite_query(
    catalog_service: &QueryCatalogService,
    connector_control: &dyn ConnectorControlRegistry,
    system_catalog: &dyn SystemCatalog,
    query: &mut sqlast::Query,
) -> Result<(), String> {
    rewrite_query_inner(catalog_service, connector_control, system_catalog, query)
}

fn rewrite_query_inner(
    catalog_service: &QueryCatalogService,
    connector_control: &dyn ConnectorControlRegistry,
    system_catalog: &dyn SystemCatalog,
    query: &mut sqlast::Query,
) -> Result<(), String> {
    if let Some(with_clause) = query.with.as_mut() {
        for cte in with_clause.cte_tables.iter_mut() {
            rewrite_query_inner(
                catalog_service,
                connector_control,
                system_catalog,
                cte.query.as_mut(),
            )?;
        }
    }
    rewrite_set_expr(
        catalog_service,
        connector_control,
        system_catalog,
        query.body.as_mut(),
    )
}

fn rewrite_set_expr(
    catalog_service: &QueryCatalogService,
    connector_control: &dyn ConnectorControlRegistry,
    system_catalog: &dyn SystemCatalog,
    expr: &mut sqlast::SetExpr,
) -> Result<(), String> {
    match expr {
        sqlast::SetExpr::Select(select) => {
            for twj in select.from.iter_mut() {
                rewrite_table_factor(
                    catalog_service,
                    connector_control,
                    system_catalog,
                    &mut twj.relation,
                )?;
                for join in twj.joins.iter_mut() {
                    rewrite_table_factor(
                        catalog_service,
                        connector_control,
                        system_catalog,
                        &mut join.relation,
                    )?;
                }
            }
        }
        sqlast::SetExpr::Query(q) => rewrite_query_inner(
            catalog_service,
            connector_control,
            system_catalog,
            q.as_mut(),
        )?,
        sqlast::SetExpr::SetOperation { left, right, .. } => {
            rewrite_set_expr(
                catalog_service,
                connector_control,
                system_catalog,
                left.as_mut(),
            )?;
            rewrite_set_expr(
                catalog_service,
                connector_control,
                system_catalog,
                right.as_mut(),
            )?;
        }
        _ => {}
    }
    Ok(())
}

fn rewrite_table_factor(
    catalog_service: &QueryCatalogService,
    connector_control: &dyn ConnectorControlRegistry,
    system_catalog: &dyn SystemCatalog,
    factor: &mut sqlast::TableFactor,
) -> Result<(), String> {
    match factor {
        sqlast::TableFactor::Table { name, alias, .. } => {
            let parts = object_name_idents(name);
            // Recognize 2-part `information_schema.X` and 3-part
            // `<catalog>.information_schema.X`.
            //
            // For `default_catalog` (the local catalog), we look up the provider
            // in the registry and scan it against the local InMemoryCatalog.
            //
            // For any other 3-part name with an admitted external connector,
            // intercept `information_schema.schemata` through its exact control
            // generation.  This bypasses the provider table-load path, which
            // cannot represent a catalog namespace scan.
            //
            // We do NOT match plain 1-part references because the session's current
            // database may legitimately shadow them with a real table.
            let key: Option<(String, String)> = match parts.as_slice() {
                [db, tbl] => Some((db.clone(), tbl.clone())),
                [cat, db, tbl] if cat.eq_ignore_ascii_case("default_catalog") => {
                    Some((db.clone(), tbl.clone()))
                }
                [cat, db, tbl]
                    if db.eq_ignore_ascii_case(INFORMATION_SCHEMA_DB)
                        && tbl.eq_ignore_ascii_case("schemata") =>
                {
                    // External catalog 3-part name: `<cat>.information_schema.schemata`.
                    // Unknown catalogs remain untouched so downstream resolution
                    // preserves its normal error. Every successful admission keeps
                    // one lease for the complete namespace lookup.
                    let context = crate::connector::connector_request_context(
                        None,
                        Arc::new(AtomicBool::new(false)),
                    )?;
                    match crate::connector::acquire_metadata_planning_lease(connector_control, cat)
                    {
                        Ok(lease) => {
                            let namespaces =
                                crate::connector::metadata_list_namespaces_with_planning_lease(
                                    lease, context,
                                )?;
                            let mut databases = namespaces
                                .into_iter()
                                .map(|namespace| namespace.namespace.to_string())
                                .collect::<Vec<_>>();
                            databases.sort();
                            databases.dedup();
                            let inputs =
                                crate::catalog_application::system_catalog::SystemCatalogInputs {
                                    catalog_name: cat,
                                    schema_names: &databases,
                                };
                            let Some(data) = system_catalog.resolve(
                                INFORMATION_SCHEMA_DB,
                                "schemata",
                                &inputs,
                            )?
                            else {
                                return Ok(());
                            };
                            let tbl_name = tbl.clone();
                            let alias = alias.take().unwrap_or_else(|| sqlast::TableAlias {
                                explicit: false,
                                name: sqlast::Ident::new(tbl_name),
                                columns: Vec::new(),
                            });
                            *factor = derived_values_factor(&data.columns, &data.batches, alias)?;
                            return Ok(());
                        }
                        Err(_) => {
                            // Unknown catalog — leave untouched; downstream will produce
                            // a proper "unknown catalog" error.
                            return Ok(());
                        }
                    }
                }
                _ => None,
            };
            let Some((db, tbl)) = key else {
                return Ok(());
            };
            // Only information_schema hosts system tables; gate before gathering inputs so
            // ordinary table references never trigger a catalog read (behavior-preserving).
            if !db.eq_ignore_ascii_case(INFORMATION_SCHEMA_DB) {
                return Ok(());
            }
            let mut schema_names: Vec<String> = {
                let catalog = catalog_service
                    .local()
                    .read()
                    .expect("standalone catalog read lock");
                catalog.database_names().map(str::to_string).collect()
            };
            schema_names.sort();
            schema_names.dedup();
            let inputs = crate::catalog_application::system_catalog::SystemCatalogInputs {
                catalog_name: "default_catalog",
                schema_names: &schema_names,
            };
            let Some(data) = system_catalog.resolve(&db, &tbl, &inputs)? else {
                return Ok(());
            };

            let alias = alias.take().unwrap_or_else(|| sqlast::TableAlias {
                explicit: false,
                name: sqlast::Ident::new(tbl),
                columns: Vec::new(),
            });
            *factor = derived_values_factor(&data.columns, &data.batches, alias)?;
            Ok(())
        }
        sqlast::TableFactor::Derived { subquery, .. } => rewrite_query_inner(
            catalog_service,
            connector_control,
            system_catalog,
            subquery.as_mut(),
        ),
        sqlast::TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            rewrite_table_factor(
                catalog_service,
                connector_control,
                system_catalog,
                &mut table_with_joins.relation,
            )?;
            for join in table_with_joins.joins.iter_mut() {
                rewrite_table_factor(
                    catalog_service,
                    connector_control,
                    system_catalog,
                    &mut join.relation,
                )?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn object_name_idents(name: &sqlast::ObjectName) -> Vec<String> {
    name.0
        .iter()
        .filter_map(|p| match p {
            sqlast::ObjectNamePart::Identifier(i) => Some(i.value.clone()),
            _ => None,
        })
        .collect()
}

/// Build a `TableFactor::Derived` whose body is a VALUES expression carrying
/// `batches` and an alias declaring `columns` as the projected column names.
///
/// A provider returning zero rows is currently treated as a programmer error:
/// the only registered provider (`schemata`) always sees at least the
/// `default` database, and synthesizing a typed empty VALUES with a
/// `WHERE FALSE` wrapper is more code than it is worth before the second
/// provider lands.
fn derived_values_factor(
    columns: &[ColumnDef],
    batches: &[RecordBatch],
    alias: sqlast::TableAlias,
) -> Result<sqlast::TableFactor, String> {
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    let alias_columns: Vec<sqlast::TableAliasColumnDef> = columns
        .iter()
        .map(|c| sqlast::TableAliasColumnDef::from_name(c.name.clone()))
        .collect();

    if total_rows == 0 {
        return Err(format!(
            "virtual table `{}` returned zero rows; empty-result rewriting is not yet implemented",
            alias.name.value
        ));
    }

    let mut rows: Vec<Vec<sqlast::Expr>> = Vec::with_capacity(total_rows);
    for batch in batches {
        if batch.num_columns() != columns.len() {
            return Err(format!(
                "virtual table batch column count {} does not match provider schema {}",
                batch.num_columns(),
                columns.len()
            ));
        }
        for row_idx in 0..batch.num_rows() {
            let mut row = Vec::with_capacity(columns.len());
            for (col_idx, col_def) in columns.iter().enumerate() {
                let array = batch.column(col_idx);
                row.push(array_value_to_expr(
                    array.as_ref(),
                    row_idx,
                    &col_def.data_type,
                )?);
            }
            rows.push(row);
        }
    }

    let values_query = sqlast::Query {
        with: None,
        body: Box::new(sqlast::SetExpr::Values(sqlast::Values {
            explicit_row: false,
            value_keyword: false,
            rows,
        })),
        order_by: None,
        limit_clause: None,
        fetch: None,
        locks: Vec::new(),
        for_clause: None,
        settings: None,
        format_clause: None,
        pipe_operators: Vec::new(),
    };

    let alias = sqlast::TableAlias {
        explicit: alias.explicit,
        name: alias.name,
        columns: alias_columns,
    };
    Ok(sqlast::TableFactor::Derived {
        lateral: false,
        subquery: Box::new(values_query),
        alias: Some(alias),
        sample: None,
    })
}

fn array_value_to_expr(
    array: &dyn Array,
    row: usize,
    declared: &DataType,
) -> Result<sqlast::Expr, String> {
    if array.is_null(row) {
        return Ok(sqlast::Expr::Value(sqlast::Value::Null.with_empty_span()));
    }
    match declared {
        DataType::Utf8 | DataType::LargeUtf8 => {
            let s = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "expected Utf8 array".to_string())?
                .value(row)
                .to_string();
            Ok(sqlast::Expr::Value(
                sqlast::Value::SingleQuotedString(s).with_empty_span(),
            ))
        }
        DataType::Boolean => {
            let v = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| "expected Boolean array".to_string())?
                .value(row);
            Ok(sqlast::Expr::Value(
                sqlast::Value::Boolean(v).with_empty_span(),
            ))
        }
        DataType::Int8 => num_to_expr(
            array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| "expected Int8 array".to_string())?
                .value(row),
        ),
        DataType::Int16 => num_to_expr(
            array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| "expected Int16 array".to_string())?
                .value(row),
        ),
        DataType::Int32 => num_to_expr(
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "expected Int32 array".to_string())?
                .value(row),
        ),
        DataType::Int64 => num_to_expr(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "expected Int64 array".to_string())?
                .value(row),
        ),
        DataType::UInt8 => num_to_expr(
            array
                .as_any()
                .downcast_ref::<UInt8Array>()
                .ok_or_else(|| "expected UInt8 array".to_string())?
                .value(row),
        ),
        DataType::UInt16 => num_to_expr(
            array
                .as_any()
                .downcast_ref::<UInt16Array>()
                .ok_or_else(|| "expected UInt16 array".to_string())?
                .value(row),
        ),
        DataType::UInt32 => num_to_expr(
            array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| "expected UInt32 array".to_string())?
                .value(row),
        ),
        DataType::UInt64 => num_to_expr(
            array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| "expected UInt64 array".to_string())?
                .value(row),
        ),
        DataType::Float32 => {
            let v = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| "expected Float32 array".to_string())?
                .value(row);
            Ok(sqlast::Expr::Value(
                sqlast::Value::Number(format!("{v}"), false).with_empty_span(),
            ))
        }
        DataType::Float64 => {
            let v = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| "expected Float64 array".to_string())?
                .value(row);
            Ok(sqlast::Expr::Value(
                sqlast::Value::Number(format!("{v}"), false).with_empty_span(),
            ))
        }
        other => Err(format!(
            "virtual table column with arrow type {other:?} is not yet supported by the VALUES rewriter"
        )),
    }
}

fn num_to_expr<N: std::fmt::Display>(n: N) -> Result<sqlast::Expr, String> {
    Ok(sqlast::Expr::Value(
        sqlast::Value::Number(format!("{n}"), false).with_empty_span(),
    ))
}
