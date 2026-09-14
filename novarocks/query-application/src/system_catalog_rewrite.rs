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

//! Query-application-owned information_schema AST rewriter.
//!
//! StarRocks exposes information_schema tables (`schemata`, `tables`, ...) as
//! real tables on the FE side. NovaRocks delegates their schema and row
//! materialization to query-application ports.
//!
//! The rewriter replaces system-table references with VALUES-backed derived
//! tables, which the standard SQL pipeline handles like ordinary relations.

use arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
    StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use novarocks_parser::{Span, ast};
use novarocks_spi::connector::ConnectorRequestContext;

use crate::system_catalog::{SystemCatalog, SystemCatalogInputs};
use novarocks_types::schema::ColumnDef;

pub const INFORMATION_SCHEMA_DB: &str = "information_schema";

/// Immutable catalog names gathered by a role-local reader for one virtual
/// table materialization. Query application owns how the names become SQL
/// rows; role adapters own connector admission and metadata transport.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SystemCatalogFacts {
    pub catalog_name: String,
    pub schema_names: Vec<String>,
    pub table_names: Vec<(String, String)>,
}

impl SystemCatalogFacts {
    fn inputs(&self) -> SystemCatalogInputs<'_> {
        SystemCatalogInputs {
            catalog_name: &self.catalog_name,
            schema_names: &self.schema_names,
            table_names: &self.table_names,
        }
    }
}

/// Role-local source of catalog names needed by `information_schema`.
///
/// A successful external lookup is already admitted against one exact
/// connector generation. `None` deliberately preserves downstream unknown
/// catalog resolution instead of allowing the virtual-table path to invent a
/// catalog error.
pub trait SystemCatalogFactsPort: Send + Sync {
    fn local_system_catalog_facts(&self) -> Result<SystemCatalogFacts, String>;

    fn external_system_catalog_facts(
        &self,
        request: &ConnectorRequestContext,
        catalog_name: &str,
        include_table_names: bool,
    ) -> Result<Option<SystemCatalogFacts>, String>;
}

// ---------------------------------------------------------------------------
// AST rewriter: substitute virtual-table refs with a VALUES derived table.
// ---------------------------------------------------------------------------
//
// StarRocks routes information_schema scans through a dedicated scan node that
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
/// The facts port is role-local and read-only; it exposes neither a Frontend
/// aggregate nor catalog-control capability to the query application.
pub fn rewrite_query(
    facts_port: &dyn SystemCatalogFactsPort,
    system_catalog: &dyn SystemCatalog,
    connector_context: &ConnectorRequestContext,
    query: &mut ast::Query,
) -> Result<(), String> {
    rewrite_query_inner(facts_port, system_catalog, connector_context, query)
}

fn rewrite_query_inner(
    facts_port: &dyn SystemCatalogFactsPort,
    system_catalog: &dyn SystemCatalog,
    connector_context: &ConnectorRequestContext,
    query: &mut ast::Query,
) -> Result<(), String> {
    if let Some(with_clause) = query.with.as_mut() {
        for cte in with_clause.ctes.iter_mut() {
            rewrite_query_inner(
                facts_port,
                system_catalog,
                connector_context,
                cte.query.as_mut(),
            )?;
        }
    }
    rewrite_set_expr(
        facts_port,
        system_catalog,
        connector_context,
        query.body.as_mut(),
    )
}

fn rewrite_set_expr(
    facts_port: &dyn SystemCatalogFactsPort,
    system_catalog: &dyn SystemCatalog,
    connector_context: &ConnectorRequestContext,
    expr: &mut ast::SetExpr,
) -> Result<(), String> {
    match expr {
        ast::SetExpr::Select(select) => {
            for twj in select.from.iter_mut() {
                rewrite_table_factor(
                    facts_port,
                    system_catalog,
                    connector_context,
                    &mut twj.relation,
                )?;
                for join in twj.joins.iter_mut() {
                    rewrite_table_factor(
                        facts_port,
                        system_catalog,
                        connector_context,
                        &mut join.relation,
                    )?;
                }
            }
        }
        ast::SetExpr::Query(q) => {
            rewrite_query_inner(facts_port, system_catalog, connector_context, q.as_mut())?
        }
        ast::SetExpr::SetOperation(operation) => {
            rewrite_set_expr(
                facts_port,
                system_catalog,
                connector_context,
                operation.left.as_mut(),
            )?;
            rewrite_set_expr(
                facts_port,
                system_catalog,
                connector_context,
                operation.right.as_mut(),
            )?;
        }
        _ => {}
    }
    Ok(())
}

fn rewrite_table_factor(
    facts_port: &dyn SystemCatalogFactsPort,
    system_catalog: &dyn SystemCatalog,
    connector_context: &ConnectorRequestContext,
    factor: &mut ast::TableFactor,
) -> Result<(), String> {
    match factor {
        ast::TableFactor::Table { name, alias, .. } => {
            let parts = object_name_idents(name);
            // Recognize 2-part `information_schema.X` and 3-part
            // `<catalog>.information_schema.X`.
            //
            // For `default_catalog`, the role adapter snapshots local names.
            //
            // For any other 3-part name with an admitted external connector,
            // intercept `information_schema.{schemata,tables}` through its exact
            // control generation.  This bypasses the provider table-load path,
            // which cannot represent a catalog namespace or table scan.
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
                        && (tbl.eq_ignore_ascii_case("schemata")
                            || tbl.eq_ignore_ascii_case("tables")) =>
                {
                    // The adapter preserves normal unknown-catalog resolution
                    // by returning None and keeps exact connector admission
                    // outside this application domain.
                    let Some(facts) = facts_port.external_system_catalog_facts(
                        connector_context,
                        cat,
                        tbl.eq_ignore_ascii_case("tables"),
                    )?
                    else {
                        return Ok(());
                    };
                    let inputs = facts.inputs();
                    let Some(data) = system_catalog.resolve(INFORMATION_SCHEMA_DB, tbl, &inputs)?
                    else {
                        return Ok(());
                    };
                    let tbl_name = tbl.clone();
                    let alias = alias.take().unwrap_or_else(|| table_alias(&tbl_name));
                    *factor = derived_values_factor(&data.columns, &data.batches, alias)?;
                    return Ok(());
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
            let facts = facts_port.local_system_catalog_facts()?;
            let inputs = facts.inputs();
            let Some(data) = system_catalog.resolve(&db, &tbl, &inputs)? else {
                return Ok(());
            };

            let alias = alias.take().unwrap_or_else(|| table_alias(&tbl));
            *factor = derived_values_factor(&data.columns, &data.batches, alias)?;
            Ok(())
        }
        ast::TableFactor::Derived { subquery, .. } => rewrite_query_inner(
            facts_port,
            system_catalog,
            connector_context,
            subquery.as_mut(),
        ),
        ast::TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            rewrite_table_factor(
                facts_port,
                system_catalog,
                connector_context,
                &mut table_with_joins.relation,
            )?;
            for join in table_with_joins.joins.iter_mut() {
                rewrite_table_factor(
                    facts_port,
                    system_catalog,
                    connector_context,
                    &mut join.relation,
                )?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn object_name_idents(name: &ast::ObjectName) -> Vec<String> {
    name.parts.iter().map(|part| part.value.clone()).collect()
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
    alias: ast::TableAlias,
) -> Result<ast::TableFactor, String> {
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    let alias_columns: Vec<ast::Ident> = columns.iter().map(|c| ident(&c.name)).collect();

    if total_rows == 0 {
        return Err(format!(
            "virtual table `{}` returned zero rows; empty-result rewriting is not yet implemented",
            alias.name.value
        ));
    }

    let mut rows: Vec<Vec<ast::Expr>> = Vec::with_capacity(total_rows);
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

    let values_query = ast::Query {
        with: None,
        body: Box::new(ast::SetExpr::Values(ast::Values {
            explicit_row: false,
            rows,
            span: Span::new(0, 0),
        })),
        order_by: Vec::new(),
        limit: None,
        offset: None,
        limit_comma_offset: false,
        fetch: None,
        span: Span::new(0, 0),
    };

    let alias = ast::TableAlias {
        explicit_as: alias.explicit_as,
        name: alias.name,
        columns: alias_columns,
        span: Span::new(0, 0),
    };
    Ok(ast::TableFactor::Derived {
        lateral: false,
        subquery: Box::new(values_query),
        hints: Vec::new(),
        alias: Some(alias),
        span: Span::new(0, 0),
    })
}

fn array_value_to_expr(
    array: &dyn Array,
    row: usize,
    declared: &DataType,
) -> Result<ast::Expr, String> {
    if array.is_null(row) {
        return Ok(literal_expr(ast::LiteralKind::Null));
    }
    match declared {
        DataType::Utf8 | DataType::LargeUtf8 => {
            let s = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "expected Utf8 array".to_string())?
                .value(row)
                .to_string();
            Ok(literal_expr(ast::LiteralKind::String(s)))
        }
        DataType::Boolean => {
            let v = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| "expected Boolean array".to_string())?
                .value(row);
            Ok(literal_expr(ast::LiteralKind::Boolean(v)))
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
            Ok(literal_expr(ast::LiteralKind::Number(format!("{v}"))))
        }
        DataType::Float64 => {
            let v = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| "expected Float64 array".to_string())?
                .value(row);
            Ok(literal_expr(ast::LiteralKind::Number(format!("{v}"))))
        }
        other => Err(format!(
            "virtual table column with arrow type {other:?} is not yet supported by the VALUES rewriter"
        )),
    }
}

fn num_to_expr<N: std::fmt::Display>(n: N) -> Result<ast::Expr, String> {
    Ok(literal_expr(ast::LiteralKind::Number(format!("{n}"))))
}

fn ident(value: &str) -> ast::Ident {
    ast::Ident {
        value: value.to_string(),
        quoted: false,
        quote_style: None,
        span: Span::new(0, 0),
    }
}

fn table_alias(name: &str) -> ast::TableAlias {
    ast::TableAlias {
        name: ident(name),
        columns: Vec::new(),
        explicit_as: false,
        span: Span::new(0, 0),
    }
}

fn literal_expr(kind: ast::LiteralKind) -> ast::Expr {
    ast::Expr::Literal(ast::Literal {
        kind,
        span: Span::new(0, 0),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex, OnceLock};
    use std::time::{Duration, Instant};

    use super::*;
    use crate::system_catalog::SystemCatalogService;
    use novarocks_spi::connector::ConnectorCancellation;

    struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    fn connector_context() -> &'static ConnectorRequestContext {
        static CONTEXT: OnceLock<ConnectorRequestContext> = OnceLock::new();
        CONTEXT.get_or_init(|| {
            ConnectorRequestContext::try_new(
                Instant::now() + Duration::from_secs(300),
                Arc::new(NeverCancelled),
                novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
                novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
            )
            .expect("test connector context")
        })
    }

    struct Facts {
        external_requests: Mutex<Vec<(String, bool)>>,
        external: Option<SystemCatalogFacts>,
    }

    impl Facts {
        fn local() -> Self {
            Self {
                external_requests: Mutex::new(Vec::new()),
                external: None,
            }
        }

        fn external() -> Self {
            Self {
                external_requests: Mutex::new(Vec::new()),
                external: Some(SystemCatalogFacts {
                    catalog_name: "ice".to_string(),
                    schema_names: vec!["warehouse".to_string()],
                    table_names: vec![("warehouse".to_string(), "orders".to_string())],
                }),
            }
        }
    }

    impl SystemCatalogFactsPort for Facts {
        fn local_system_catalog_facts(&self) -> Result<SystemCatalogFacts, String> {
            Ok(SystemCatalogFacts {
                catalog_name: "default_catalog".to_string(),
                schema_names: vec!["default".to_string()],
                table_names: Vec::new(),
            })
        }

        fn external_system_catalog_facts(
            &self,
            _request: &ConnectorRequestContext,
            catalog_name: &str,
            include_table_names: bool,
        ) -> Result<Option<SystemCatalogFacts>, String> {
            self.external_requests
                .lock()
                .expect("external requests lock")
                .push((catalog_name.to_string(), include_table_names));
            Ok(self.external.clone())
        }
    }

    fn parsed_query(sql: &str) -> ast::Query {
        let mut statements = novarocks_parser::parse(sql).expect("parse query");
        let ast::Statement::Query(query) = statements.remove(0) else {
            panic!("expected query");
        };
        query
    }

    fn first_factor(query: &ast::Query) -> &ast::TableFactor {
        let ast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected select");
        };
        &select.from[0].relation
    }

    #[test]
    fn rewrites_local_schemata_from_read_only_facts() {
        let facts = Facts::local();
        let catalog = SystemCatalogService::with_defaults();
        let mut query = parsed_query("SELECT schema_name FROM information_schema.schemata");

        rewrite_query(&facts, &catalog, connector_context(), &mut query).expect("rewrite query");

        assert!(matches!(
            first_factor(&query),
            ast::TableFactor::Derived { .. }
        ));
        assert!(
            facts
                .external_requests
                .lock()
                .expect("external requests lock")
                .is_empty()
        );
    }

    #[test]
    fn external_tables_request_table_names_once_through_facts_port() {
        let facts = Facts::external();
        let catalog = SystemCatalogService::with_defaults();
        let mut query = parsed_query("SELECT table_name FROM ice.information_schema.tables");

        rewrite_query(&facts, &catalog, connector_context(), &mut query).expect("rewrite query");

        assert!(matches!(
            first_factor(&query),
            ast::TableFactor::Derived { .. }
        ));
        assert_eq!(
            *facts
                .external_requests
                .lock()
                .expect("external requests lock"),
            vec![("ice".to_string(), true)]
        );
    }

    #[test]
    fn unknown_external_catalog_remains_for_normal_resolution() {
        let facts = Facts::local();
        let catalog = SystemCatalogService::with_defaults();
        let mut query = parsed_query("SELECT schema_name FROM missing.information_schema.schemata");

        rewrite_query(&facts, &catalog, connector_context(), &mut query).expect("rewrite query");

        assert!(matches!(
            first_factor(&query),
            ast::TableFactor::Table { .. }
        ));
    }
}
