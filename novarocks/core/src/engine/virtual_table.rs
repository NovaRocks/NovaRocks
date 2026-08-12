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

use crate::engine::StandaloneState;
use novarocks_catalog::schema::ColumnDef;

pub(crate) const INFORMATION_SCHEMA_DB: &str = "information_schema";

// ---------------------------------------------------------------------------
// AST rewriter: substitute virtual-table refs with a VALUES derived table.
// ---------------------------------------------------------------------------
//
// StarRocks routes information_schema scans through a `SchemaScanNode` that
// produces rows at the BE; NovaRocks standalone has no equivalent BE-side
// generator, so we materialize rows here (against the live `StandaloneState`)
// and rewrite each `FROM information_schema.X` into a derived table backed by
// a VALUES expression. The standard SQL pipeline (analyzer → planner →
// codegen → pipeline) then handles projection / WHERE / aggregation / ORDER BY
// like any other base table.
//
// Hooked from `engine::mod::execute_statement` for `Statement::Query`, before
// `execute_query` runs. CTE bodies and subqueries are walked recursively.

/// Walk a query AST and replace virtual-table references with VALUES-backed
/// derived tables. Returns `Ok(())` even when no virtual tables are matched.
pub(crate) fn rewrite_query(
    state: &Arc<StandaloneState>,
    query: &mut sqlast::Query,
) -> Result<(), String> {
    rewrite_query_inner(state, query)
}

fn rewrite_query_inner(
    state: &Arc<StandaloneState>,
    query: &mut sqlast::Query,
) -> Result<(), String> {
    if let Some(with_clause) = query.with.as_mut() {
        for cte in with_clause.cte_tables.iter_mut() {
            rewrite_query_inner(state, cte.query.as_mut())?;
        }
    }
    rewrite_set_expr(state, query.body.as_mut())
}

fn rewrite_set_expr(
    state: &Arc<StandaloneState>,
    expr: &mut sqlast::SetExpr,
) -> Result<(), String> {
    match expr {
        sqlast::SetExpr::Select(select) => {
            for twj in select.from.iter_mut() {
                rewrite_table_factor(state, &mut twj.relation)?;
                for join in twj.joins.iter_mut() {
                    rewrite_table_factor(state, &mut join.relation)?;
                }
            }
        }
        sqlast::SetExpr::Query(q) => rewrite_query_inner(state, q.as_mut())?,
        sqlast::SetExpr::SetOperation { left, right, .. } => {
            rewrite_set_expr(state, left.as_mut())?;
            rewrite_set_expr(state, right.as_mut())?;
        }
        _ => {}
    }
    Ok(())
}

fn rewrite_table_factor(
    state: &Arc<StandaloneState>,
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
                    match crate::connector::acquire_metadata_planning_lease(
                        state.connector_control.as_ref(),
                        cat,
                    ) {
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
                            let inputs = crate::engine::system_catalog::SystemCatalogInputs {
                                catalog_name: cat,
                                schema_names: &databases,
                            };
                            let Some(data) = state.system_catalog.resolve(
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
                let catalog = state
                    .catalog_service
                    .local()
                    .read()
                    .expect("standalone catalog read lock");
                catalog.database_names().map(str::to_string).collect()
            };
            schema_names.sort();
            schema_names.dedup();
            let inputs = crate::engine::system_catalog::SystemCatalogInputs {
                catalog_name: "default_catalog",
                schema_names: &schema_names,
            };
            let Some(data) = state.system_catalog.resolve(&db, &tbl, &inputs)? else {
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
        sqlast::TableFactor::Derived { subquery, .. } => {
            rewrite_query_inner(state, subquery.as_mut())
        }
        sqlast::TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            rewrite_table_factor(state, &mut table_with_joins.relation)?;
            for join in table_with_joins.joins.iter_mut() {
                rewrite_table_factor(state, &mut join.relation)?;
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::engine::StandaloneState;
    use crate::engine::system_catalog::{SystemCatalog, SystemCatalogInputs, SystemTableData};
    use crate::sql::parser::dialect::StarRocksDialect;
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_catalog::schema::ColumnDef;
    use novarocks_spi::connector::{
        ConnectorControlBinding, ConnectorError, ConnectorErrorKind, ConnectorInstanceId,
        ConnectorListNamespacesRequest, ConnectorListTablesRequest, ConnectorMetadata,
        ConnectorNamespaceIdentity, ConnectorNamespaceRequest, ConnectorTableIdentity,
        ConnectorTableMetadata, ConnectorTableRequest,
    };
    use sqlparser::parser::Parser;

    #[derive(Default)]
    struct EchoSchemaNames {
        calls: AtomicUsize,
    }

    impl SystemCatalog for EchoSchemaNames {
        fn resolve(
            &self,
            db: &str,
            tbl: &str,
            inputs: &SystemCatalogInputs<'_>,
        ) -> Result<Option<SystemTableData>, String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if !(db.eq_ignore_ascii_case("information_schema")
                && tbl.eq_ignore_ascii_case("schemata"))
            {
                return Ok(None);
            }
            let col = ColumnDef {
                name: "schema_name".into(),
                data_type: DataType::Utf8,
                nullable: false,
                write_default: None,
                logical_type: None,
            };
            let arr = StringArray::from(inputs.schema_names.to_vec());
            let batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "schema_name",
                    DataType::Utf8,
                    false,
                )])),
                vec![Arc::new(arr)],
            )
            .map_err(|e| e.to_string())?;
            Ok(Some(SystemTableData {
                columns: vec![col],
                batches: vec![batch],
            }))
        }
    }

    /// Parse a SELECT query into a mutable `sqlparser::ast::Query`.
    fn parse_query(sql: &str) -> Box<sqlparser::ast::Query> {
        let dialect = StarRocksDialect;
        let stmt = Parser::new(&dialect)
            .try_with_sql(sql)
            .expect("lex")
            .parse_statement()
            .expect("parse");
        let sqlparser::ast::Statement::Query(q) = stmt else {
            panic!("expected Query statement")
        };
        q
    }

    struct NamespaceMetadata {
        instance_id: ConnectorInstanceId,
        namespaces: Arc<Mutex<Vec<String>>>,
    }

    impl ConnectorMetadata for NamespaceMetadata {
        fn instance_id(&self) -> &ConnectorInstanceId {
            &self.instance_id
        }

        fn list_namespaces(
            &self,
            request: ConnectorListNamespacesRequest,
        ) -> Result<Vec<ConnectorNamespaceIdentity>, ConnectorError> {
            if request.instance_id != self.instance_id {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "namespace fixture received another connector instance",
                ));
            }
            Ok(self
                .namespaces
                .lock()
                .expect("namespace fixture lock")
                .iter()
                .map(|namespace| ConnectorNamespaceIdentity {
                    instance_id: self.instance_id.clone(),
                    namespace: Arc::from(namespace.as_str()),
                })
                .collect())
        }

        fn namespace_exists(
            &self,
            _request: ConnectorNamespaceRequest,
        ) -> Result<bool, ConnectorError> {
            Err(unsupported_namespace_fixture_operation())
        }

        fn table_exists(&self, _request: ConnectorTableRequest) -> Result<bool, ConnectorError> {
            Err(unsupported_namespace_fixture_operation())
        }

        fn list_tables(
            &self,
            _request: ConnectorListTablesRequest,
        ) -> Result<Vec<ConnectorTableIdentity>, ConnectorError> {
            Err(unsupported_namespace_fixture_operation())
        }

        fn load_table(
            &self,
            _request: ConnectorTableRequest,
        ) -> Result<ConnectorTableMetadata, ConnectorError> {
            Err(unsupported_namespace_fixture_operation())
        }
    }

    fn unsupported_namespace_fixture_operation() -> ConnectorError {
        ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "namespace fixture only supports namespace enumeration",
        )
    }

    /// Build a minimal state whose opaque control binding exposes only the
    /// namespace facts consumed by this Core AST rewrite.
    fn state_with_namespace_catalog(
        catalog_name: &str,
        namespaces: Arc<Mutex<Vec<String>>>,
    ) -> Arc<StandaloneState> {
        let state = Arc::new(StandaloneState {
            system_catalog: Arc::new(EchoSchemaNames::default()),
            ..StandaloneState::default()
        });
        let fixture = crate::connector::scan_model::planned_files_fixture_binding(
            catalog_name,
            HashMap::new(),
            None,
        );
        let binding = ConnectorControlBinding::try_new(
            fixture.descriptor().clone(),
            fixture.incarnation(),
            Arc::new(NamespaceMetadata {
                instance_id: fixture.descriptor().instance_id.clone(),
                namespaces,
            }),
            Arc::clone(fixture.planning()),
            Arc::clone(fixture.execution_distribution()),
            None,
        )
        .expect("namespace fixture control binding");
        state
            .connector_control
            .register(binding)
            .expect("register namespace fixture control binding");
        state
    }

    // -----------------------------------------------------------------------
    // default_catalog regression test
    // -----------------------------------------------------------------------

    #[test]
    fn rewrite_default_catalog_information_schema_schemata() {
        // `default_catalog.information_schema.schemata` must be rewritten into
        // a VALUES-backed derived table even when the local in-memory catalog is
        // empty.
        let state = Arc::new(StandaloneState {
            system_catalog: Arc::new(EchoSchemaNames::default()),
            ..StandaloneState::default()
        });
        // Seed one database so the rewriter has at least one row to produce.
        {
            let mut cat = state
                .catalog_service
                .local()
                .write()
                .expect("catalog service local lock");
            cat.create_database("mydb").expect("create db");
        }
        let mut query =
            parse_query("SELECT schema_name FROM default_catalog.information_schema.schemata");
        super::rewrite_query(&state, &mut query).expect("rewrite_query");
        // After rewriting the FROM clause must be a Derived (VALUES) table, not a
        // plain Table reference.
        let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected Select body");
        };
        assert_eq!(select.from.len(), 1);
        assert!(
            matches!(
                select.from[0].relation,
                sqlparser::ast::TableFactor::Derived { .. }
            ),
            "expected Derived VALUES factor, got {:?}",
            select.from[0].relation
        );
    }

    // -----------------------------------------------------------------------
    // External Iceberg catalog: unknown catalog left untouched
    // -----------------------------------------------------------------------

    #[test]
    fn rewrite_unknown_catalog_information_schema_schemata_is_noop() {
        // A 3-part reference to an unregistered catalog must NOT be rewritten.
        // The rewriter leaves the AST untouched so downstream resolvers can
        // surface the proper "unknown catalog" error.
        let state = Arc::new(StandaloneState {
            system_catalog: Arc::new(EchoSchemaNames::default()),
            ..StandaloneState::default()
        });
        let mut query =
            parse_query("SELECT schema_name FROM no_such_cat.information_schema.schemata");
        super::rewrite_query(&state, &mut query).expect("rewrite_query returns Ok for unknown cat");
        // The FROM clause must still be a plain Table reference (not rewritten).
        let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected Select body");
        };
        assert!(
            matches!(
                select.from[0].relation,
                sqlparser::ast::TableFactor::Table { .. }
            ),
            "expected Table factor (not rewritten) for unknown catalog"
        );
    }

    // -----------------------------------------------------------------------
    // External Iceberg catalog: registered catalog rewrites to VALUES
    // -----------------------------------------------------------------------

    #[test]
    fn rewrite_registered_iceberg_catalog_information_schema_schemata() {
        let namespaces = Arc::new(Mutex::new(vec!["ns_alpha".to_string()]));
        let state = state_with_namespace_catalog("myice", Arc::clone(&namespaces));
        namespaces
            .lock()
            .expect("namespace fixture lock")
            .push("ns_live".to_string());
        let mut query = parse_query("SELECT schema_name FROM myice.information_schema.schemata");
        super::rewrite_query(&state, &mut query).expect("rewrite_query");
        assert!(format!("{query:?}").contains("ns_live"));

        // The FROM clause must now be a VALUES-backed Derived table (not a raw
        // Iceberg Table reference).
        let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected Select body");
        };
        assert!(
            matches!(
                select.from[0].relation,
                sqlparser::ast::TableFactor::Derived { .. }
            ),
            "expected Derived VALUES factor for registered external catalog, got {:?}",
            select.from[0].relation
        );
    }

    #[test]
    fn empty_system_catalog_leaves_schemata_untouched() {
        let state = Arc::new(StandaloneState::default());
        let mut query = parse_query("SELECT schema_name FROM information_schema.schemata");

        super::rewrite_query(&state, &mut query).expect("rewrite_query");

        let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected Select body");
        };
        assert!(
            matches!(
                select.from[0].relation,
                sqlparser::ast::TableFactor::Table { .. }
            ),
            "expected Table factor when the injected system catalog returns None"
        );
    }

    #[test]
    fn non_information_schema_reference_does_not_invoke_resolve() {
        let fake = Arc::new(EchoSchemaNames::default());
        let state = Arc::new(StandaloneState {
            system_catalog: fake.clone(),
            ..StandaloneState::default()
        });
        let mut query = parse_query("SELECT * FROM mydb.mytbl");

        super::rewrite_query(&state, &mut query).expect("rewrite_query");

        assert_eq!(fake.calls.load(Ordering::SeqCst), 0);
    }
}
