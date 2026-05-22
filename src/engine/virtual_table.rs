//! Virtual-table provider framework for standalone information_schema.
//!
//! StarRocks exposes information_schema tables (`schemata`, `tables`, ...) as
//! real tables on the FE side: each is a `SystemTable` subclass under
//! `InfoSchemaDb`, materialized through `SchemaScanNode` at query time. This
//! module mirrors that shape for NovaRocks standalone mode: each virtual table
//! provides its own column schema and a `scan` function that materializes rows
//! from the current `StandaloneState`.
//!
//! Rows are eagerly materialized at query-rewrite time and embedded as a
//! `TableStorage::SystemRows` variant in a cloned catalog snapshot, so the
//! standard SQL pipeline (analyzer / planner / optimizer / codegen / pipeline)
//! handles them like any other base table — no handler-style SQL-subset
//! interpretation required.
//!
//! Adding a new virtual table is one concrete `VirtualTableProvider` impl plus
//! a single `register` call in `VirtualTableRegistry::with_defaults`.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
    StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use sqlparser::ast as sqlast;

use crate::engine::StandaloneState;
use crate::sql::catalog::ColumnDef;

pub(crate) const INFORMATION_SCHEMA_DB: &str = "information_schema";

/// Implementation contract for a single information_schema virtual table.
pub(crate) trait VirtualTableProvider: Send + Sync {
    /// Lower-case database name this table lives under (typically
    /// `"information_schema"`).
    fn database(&self) -> &str;

    /// Lower-case table name (e.g. `"schemata"`).
    fn table(&self) -> &str;

    /// Column definitions, in the order they will appear in `SELECT *`. The
    /// Arrow data type here must match the schema of every `RecordBatch`
    /// returned by `scan`.
    fn columns(&self) -> Vec<ColumnDef>;

    /// Materialize rows from the current standalone state. Called once per
    /// query that references this virtual table.
    fn scan(&self, state: &StandaloneState) -> Result<Vec<RecordBatch>, String>;
}

/// Registry mapping `(database, table)` → provider.
pub(crate) struct VirtualTableRegistry {
    providers: HashMap<(String, String), Arc<dyn VirtualTableProvider>>,
}

impl VirtualTableRegistry {
    pub(crate) fn with_defaults() -> Self {
        let mut registry = Self {
            providers: HashMap::new(),
        };
        registry.register(Arc::new(super::information_schema::SchemataProvider));
        registry
    }

    fn register(&mut self, provider: Arc<dyn VirtualTableProvider>) {
        let key = (
            provider.database().to_ascii_lowercase(),
            provider.table().to_ascii_lowercase(),
        );
        self.providers.insert(key, provider);
    }

    pub(crate) fn lookup(
        &self,
        database: &str,
        table: &str,
    ) -> Option<Arc<dyn VirtualTableProvider>> {
        let key = (database.to_ascii_lowercase(), table.to_ascii_lowercase());
        self.providers.get(&key).cloned()
    }

    /// Returns every `(database, table)` pair owned by some provider. Used to
    /// pre-register placeholder databases (e.g. `information_schema`) in the
    /// catalog snapshot before per-query injection runs.
    pub(crate) fn databases(&self) -> impl Iterator<Item = &str> {
        self.providers.keys().map(|(db, _)| db.as_str())
    }
}

/// Build an Arrow `Schema` from a provider's `columns()` output. The field
/// names use the lower-case form (matching catalog identifier normalization)
/// so downstream codegen/lower can index by slot id without extra mapping.
pub(crate) fn arrow_schema_for_provider(provider: &dyn VirtualTableProvider) -> Arc<Schema> {
    use arrow::datatypes::Field;
    let fields: Vec<Field> = provider
        .columns()
        .into_iter()
        .map(|c| Field::new(c.name, c.data_type, c.nullable))
        .collect();
    Arc::new(Schema::new(fields))
}

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
            // For any other 3-part name where the catalog is a registered external
            // Iceberg catalog, we intercept `information_schema.schemata` and
            // enumerate that catalog's databases directly, bypassing the Iceberg
            // table load path that would otherwise fail with a "no metadata files"
            // error.
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
                    // Check whether <cat> is a registered Iceberg catalog; if so,
                    // enumerate its databases. If not, leave the reference alone so
                    // downstream resolvers can emit "unknown catalog".
                    let registry = state
                        .iceberg_catalogs
                        .read()
                        .expect("iceberg catalog registry read lock");
                    match registry.get(cat) {
                        Ok(entry) => {
                            let databases = crate::connector::iceberg::catalog::list_namespaces(&entry)?;
                            let schemata_cols = crate::engine::information_schema::schemata_columns();
                            let batches = crate::engine::information_schema::build_schemata_batch(cat, &databases)?;
                            let tbl_name = tbl.clone();
                            let alias = alias.take().unwrap_or_else(|| sqlast::TableAlias {
                                explicit: false,
                                name: sqlast::Ident::new(tbl_name),
                                columns: Vec::new(),
                            });
                            *factor = derived_values_factor(&schemata_cols, &batches, alias)?;
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
            let Some(provider) = state.virtual_tables.lookup(&db, &tbl) else {
                return Ok(());
            };

            let columns = provider.columns();
            let batches = provider.scan(state)?;
            let alias = alias.take().unwrap_or_else(|| sqlast::TableAlias {
                explicit: false,
                name: sqlast::Ident::new(tbl),
                columns: Vec::new(),
            });
            *factor = derived_values_factor(&columns, &batches, alias)?;
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
                row.push(array_value_to_expr(array.as_ref(), row_idx, &col_def.data_type)?);
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
        return Ok(sqlast::Expr::Value(
            sqlast::Value::Null.with_empty_span(),
        ));
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
    use std::sync::Arc;

    use crate::connector::iceberg::catalog::registry::build_catalog_entry;
    use crate::engine::StandaloneState;
    use crate::sql::parser::dialect::StarRocksDialect;
    use sqlparser::parser::Parser;

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

    /// Build a minimal `StandaloneState` with a local Iceberg (Hadoop) catalog
    /// registered under `catalog_name`, whose warehouse is `warehouse_path`.
    fn state_with_local_catalog(catalog_name: &str, warehouse_path: &str) -> Arc<StandaloneState> {
        let state = Arc::new(StandaloneState::default());
        let properties = vec![
            ("iceberg.catalog.warehouse".to_string(), warehouse_path.to_string()),
        ];
        let entry = build_catalog_entry(catalog_name, &properties).expect("build catalog entry");
        state
            .iceberg_catalogs
            .write()
            .expect("registry lock")
            .create_catalog(catalog_name, &properties)
            .expect("register catalog");
        let _ = entry;
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
        let state = Arc::new(StandaloneState::default());
        // Seed one database so the rewriter has at least one row to produce.
        {
            let mut cat = state.catalog.write().expect("catalog lock");
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
        let state = Arc::new(StandaloneState::default());
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
        // Create a temporary warehouse directory with one namespace subdirectory.
        let warehouse_dir = tempfile::tempdir().expect("tempdir");
        let ns_dir = warehouse_dir.path().join("ns_alpha");
        std::fs::create_dir_all(&ns_dir).expect("create namespace dir");

        let warehouse_path = warehouse_dir.path().to_str().unwrap();
        let state = state_with_local_catalog("myice", warehouse_path);

        let mut query =
            parse_query("SELECT schema_name FROM myice.information_schema.schemata");
        super::rewrite_query(&state, &mut query).expect("rewrite_query");

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
}
