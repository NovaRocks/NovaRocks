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

//! `CREATE TABLE [IF NOT EXISTS] <ident> [PARTITION BY (...)]
//!  [TBLPROPERTIES(...)] AS <select>` — Iceberg CTAS engine entry.
//!
//! Five steps (spec §5):
//!   A. Plan the SELECT and infer the iceberg schema.
//!   B. Catalog `create_table` (atomic point #1).
//!   C. Drive IcebergSinkPlan over the planned SELECT (atomic point #2).
//!   D. `run_iceberg_commit(FastAppendCommit)` (atomic point #3).
//!   E. On C / D failure, drop_table to roll back; on drop_table failure,
//!      return the documented combined error.
//!
//! Error quadrants (spec §5.3):
//!   - Step B fails (no rollback needed): "CTAS failed: cannot create table: <reason>"
//!   - Step C/D fails + drop succeeds: "CTAS failed during <kind>: <reason>; cleaned up"
//!     When kind is "commit", an orphan data files hint is appended.
//!   - Step C/D fails + drop also fails: "CTAS failed during data write/commit: <reason>;
//!     cleanup also failed: <drop_err>; table <ns>.<tbl> may exist as orphan, drop manually"
//!
//! Strict-default invariants: every CTAS-created table is
//! `format-version = 3` + `write.row-lineage = true`. The parser already
//! rejects user-supplied opt-out values; this module force-injects both
//! properties so an absent declaration still produces a v3 row-lineage
//! table.
//!
//! Phase-1 trade-off: the SELECT is executed twice — once here for schema
//! inference (Step A) and once inside `execute_iceberg_insert_or_overwrite`
//! for the actual data write (Steps C+D). This duplication avoids wiring
//! chunk re-use through the insert path; a future optimisation can buffer
//! the chunks from Step A and replay them without re-running the query.

use std::sync::Arc;

use arrow::datatypes::DataType;

use crate::connector::iceberg::catalog::registry::IcebergCatalogEntry;
use crate::engine::backend_resolver::TargetBackend;
use crate::engine::{StandaloneState, StatementResult};
use crate::runtime::query_result::QueryResultColumn;
use crate::sql::parser::ast::{
    CreateTableKind, CreateTableStmt, IcebergPartitionFieldExpr, InsertSource, OverwriteMode,
    SqlType, TableColumnDef,
};

/// Execute a CTAS statement. Caller has confirmed `stmt.as_select.is_some()`.
pub(crate) fn execute_iceberg_ctas(
    state: &Arc<StandaloneState>,
    stmt: CreateTableStmt,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<StatementResult, String> {
    debug_assert!(stmt.as_select.is_some(), "CTAS dispatch requires AS SELECT");

    // Resolve the target table name to (catalog, namespace, table).
    let target = crate::engine::backend_resolver::resolve_table_target(
        state,
        &stmt.name,
        current_catalog,
        current_database,
    )?;

    // Look up the iceberg catalog entry.
    let entry = {
        let registry = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        registry.get(&target.catalog)?
    };

    // IF NOT EXISTS / already-exists check.
    //
    // Hadoop catalog's create_table is non-strict (it silently overwrites the
    // metadata pointer; see hadoop_catalog.rs:230). To honor SQL semantics
    // where `CREATE TABLE t AS SELECT ...` must error when t already exists
    // (unless IF NOT EXISTS), we do the existence check explicitly here.
    let already_exists = table_exists(&entry, &target.namespace, &target.table)?;
    if already_exists {
        if stmt.if_not_exists {
            // IF NOT EXISTS: skip CTAS entirely. Do NOT execute SELECT.
            return Ok(StatementResult::Ok);
        }
        return Err(format!(
            "CTAS failed: cannot create table: table {}.{} already exists",
            target.namespace, target.table,
        ));
    }

    // Step A: plan SELECT, collect chunks + schema columns.
    // `run_select_to_chunks_and_schema` always returns the schema columns even
    // when the result set has zero rows.
    let query = stmt
        .as_select
        .as_ref()
        .expect("CTAS requires AS SELECT")
        .as_ref();
    let (_, schema_cols) =
        crate::engine::iceberg_writer::run_select_to_chunks_and_schema(state, &target, query)?;

    if schema_cols.is_empty() {
        return Err(
            "CTAS: SELECT produced no output columns; schema cannot be inferred".to_string(),
        );
    }

    // Convert the query output schema to TableColumnDefs for create_table.
    let columns = query_result_columns_to_table_column_defs(&schema_cols)?;

    // Unpack partition fields and properties from the statement kind.
    let CreateTableKind::Iceberg {
        partition_fields,
        key_desc,
        properties,
        ..
    } = &stmt.kind;

    // Validate that every PARTITION BY column name exists in the SELECT output.
    for field in partition_fields {
        let col_name = partition_field_column(field);
        if !columns.iter().any(|c| c.name == col_name) {
            return Err(format!(
                "partition column '{col_name}' not found in SELECT output"
            ));
        }
    }

    // Force v3 + row-lineage into properties (parser already rejected opt-out
    // values, so injecting here is always the safe path).
    let props_vec = inject_v3_row_lineage(properties);

    // Step B: catalog.create_table (atomic point #1).
    // Failure here means no table was created; no rollback needed.
    crate::connector::iceberg::catalog::registry::create_table(
        &entry,
        &target.namespace,
        &target.table,
        &columns,
        key_desc.as_ref(),
        partition_fields,
        &props_vec,
    )
    .map_err(|e| format!("CTAS failed: cannot create table: {e}"))?;

    // Steps C+D: drive sink + commit over the SELECT. On failure, roll back
    // by dropping the just-created table.
    //
    // Error quadrant comments are inline below (see module doc).
    let write_result = drive_data_write(state, &target, query);

    match write_result {
        Ok(()) => {
            // Invalidate caches so subsequent SELECTs see the new table and
            // its initial snapshot.
            let _ = crate::engine::iceberg_writer::invalidate_iceberg_caches(state, &target);
            Ok(StatementResult::Ok)
        }
        Err(write_err) => {
            // Step E: attempt drop_table rollback.
            let drop_result = crate::connector::iceberg::catalog::registry::drop_table(
                &entry,
                &target.namespace,
                &target.table,
            );
            match drop_result {
                Ok(_) => {
                    // Distinguish "commit failed" from "data write failed" by
                    // checking the error text. Commit failures leave orphan
                    // data files on disk.
                    let is_commit_failure = write_err.to_lowercase().contains("commit");
                    let kind = if is_commit_failure {
                        "commit"
                    } else {
                        "data write"
                    };
                    let orphan_hint = if is_commit_failure {
                        format!(
                            "; orphan data files may remain in <warehouse>/{}/data/",
                            target.table
                        )
                    } else {
                        String::new()
                    };
                    Err(format!(
                        "CTAS failed during {kind}: {write_err}; cleaned up{orphan_hint}"
                    ))
                }
                Err(drop_err) => Err(format!(
                    "CTAS failed during data write/commit: {write_err}; \
                     cleanup also failed: {drop_err}; \
                     table {}.{} may exist as orphan, drop manually",
                    target.namespace, target.table,
                )),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Check whether a table already exists in the given namespace via the
/// catalog's table-listing API. Returns `true` if the table is present.
fn table_exists(entry: &IcebergCatalogEntry, namespace: &str, table: &str) -> Result<bool, String> {
    let tables = crate::connector::iceberg::catalog::registry::list_tables(entry, namespace)?;
    let normalized = crate::engine::catalog::normalize_identifier(table)?;
    Ok(tables.iter().any(|t| t.eq_ignore_ascii_case(&normalized)))
}

/// Extract the source column name from a partition field expression.
fn partition_field_column(field: &IcebergPartitionFieldExpr) -> &str {
    match field {
        IcebergPartitionFieldExpr::Identity { column }
        | IcebergPartitionFieldExpr::Year { column }
        | IcebergPartitionFieldExpr::Month { column }
        | IcebergPartitionFieldExpr::Day { column }
        | IcebergPartitionFieldExpr::Hour { column }
        | IcebergPartitionFieldExpr::Bucket { column, .. }
        | IcebergPartitionFieldExpr::Truncate { column, .. }
        | IcebergPartitionFieldExpr::Void { column } => column.as_str(),
    }
}

/// Append `format-version=3` and `write.row-lineage=true` to the caller's
/// property list, overwriting any existing values. Returns a new vec.
fn inject_v3_row_lineage(base: &[(String, String)]) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = base
        .iter()
        .filter(|(k, _)| {
            !k.eq_ignore_ascii_case("format-version")
                && !k.eq_ignore_ascii_case("write.row-lineage")
        })
        .cloned()
        .collect();
    out.push(("format-version".to_string(), "3".to_string()));
    out.push(("write.row-lineage".to_string(), "true".to_string()));
    out
}

/// Convert `QueryResultColumn`s (which carry the Arrow DataType and nullability
/// from the planner) into `TableColumnDef`s suitable for `catalog::create_table`.
fn query_result_columns_to_table_column_defs(
    cols: &[QueryResultColumn],
) -> Result<Vec<TableColumnDef>, String> {
    cols.iter()
        .map(|c| {
            let data_type = arrow_data_type_to_sql_type(&c.data_type)?;
            Ok(TableColumnDef {
                name: c.name.clone(),
                data_type,
                nullable: c.nullable,
                aggregation: None,
                default: None,
            })
        })
        .collect()
}

/// Convert an Arrow `DataType` to a `SqlType` for use in CTAS schema creation.
///
/// Unsupported types (Float16, Interval, Duration, LargeList, FixedSizeList,
/// Decimal256, geometry / geography extensions) are rejected with a clear
/// error message directing users to the explicit CREATE TABLE + INSERT path.
///
/// Note: FixedSizeBinary(16) (LARGEINT) is accepted — it maps to
/// `SqlType::LargeInt` which the Iceberg writer stores as a BINARY(16) column
/// with a logical-type property.
pub(crate) fn arrow_schema_to_table_column_defs(
    schema: &arrow::datatypes::Schema,
) -> Result<Vec<TableColumnDef>, String> {
    schema
        .fields()
        .iter()
        .map(|field| {
            let data_type = arrow_data_type_to_sql_type(field.data_type())?;
            Ok(TableColumnDef {
                name: field.name().clone(),
                data_type,
                nullable: field.is_nullable(),
                aggregation: None,
                default: None,
            })
        })
        .collect()
}

/// Recursive Arrow DataType → SqlType conversion for CTAS schema inference.
pub(crate) fn arrow_data_type_to_sql_type(dt: &DataType) -> Result<SqlType, String> {
    use arrow::datatypes::TimeUnit;
    Ok(match dt {
        DataType::Boolean => SqlType::Boolean,
        DataType::Int8 => SqlType::TinyInt,
        DataType::Int16 => SqlType::SmallInt,
        DataType::Int32 => SqlType::Int,
        DataType::Int64 => SqlType::BigInt,
        DataType::Float32 => SqlType::Float,
        DataType::Float64 => SqlType::Double,
        DataType::Decimal128(precision, scale) => SqlType::Decimal {
            precision: *precision,
            scale: *scale,
        },
        DataType::Utf8 | DataType::LargeUtf8 => SqlType::String,
        DataType::Binary | DataType::LargeBinary => SqlType::Binary,
        // StarRocks LARGEINT is stored as a fixed 16-byte signed integer.
        DataType::FixedSizeBinary(w) if *w == crate::common::largeint::LARGEINT_BYTE_WIDTH => {
            SqlType::LargeInt
        }
        DataType::Date32 => SqlType::Date,
        DataType::Timestamp(_, _) => SqlType::DateTime,
        DataType::Time64(TimeUnit::Microsecond | TimeUnit::Nanosecond) => SqlType::Time,
        DataType::List(elem) => {
            SqlType::Array(Box::new(arrow_data_type_to_sql_type(elem.data_type())?))
        }
        DataType::Struct(fields) => SqlType::Struct(
            fields
                .iter()
                .map(|f| {
                    Ok((
                        f.name().clone(),
                        arrow_data_type_to_sql_type(f.data_type())?,
                    ))
                })
                .collect::<Result<Vec<_>, String>>()?,
        ),
        DataType::Map(entries, _) => {
            // Arrow MAP is encoded as List<Struct{key, value}>.
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(
                    "CTAS: MAP column has unexpected Arrow encoding (expected struct entries)"
                        .to_string(),
                );
            };
            let (_, key_field) = fields
                .find("key")
                .ok_or_else(|| "CTAS: MAP column missing 'key' field".to_string())?;
            let (_, val_field) = fields
                .find("value")
                .ok_or_else(|| "CTAS: MAP column missing 'value' field".to_string())?;
            SqlType::Map(
                Box::new(arrow_data_type_to_sql_type(key_field.data_type())?),
                Box::new(arrow_data_type_to_sql_type(val_field.data_type())?),
            )
        }
        other => {
            return Err(format!(
                "CTAS: arrow type {other:?} not supported; \
                 use CREATE TABLE then INSERT for variant/geometry/geography or \
                 unsupported numeric types (Float16, Decimal256, Interval, etc.)"
            ));
        }
    })
}

/// Drive Steps C + D: re-execute the SELECT and commit a FastAppend iceberg
/// transaction against the newly-created table.
///
/// This delegates entirely to `execute_iceberg_insert_or_overwrite` which
/// handles SELECT execution, writing data files, and committing the snapshot.
/// On failure the raw error string is returned so the caller can route it
/// through the rollback quadrant (Step E).
fn drive_data_write(
    state: &Arc<StandaloneState>,
    target: &TargetBackend,
    query: &sqlparser::ast::Query,
) -> Result<(), String> {
    // Load the just-created table via the connector registry's CatalogBackend
    // path — this is the same path used by run_insert and returns the
    // ResolvedTable that execute_iceberg_insert_or_overwrite expects.
    let resolved = {
        let reg = state.connectors.read().expect("connector registry read");
        let catalog_backend = reg.catalog_backend(target.backend_name)?;
        catalog_backend.load_table(&target.catalog, &target.namespace, &target.table)?
    };

    crate::engine::iceberg_writer::execute_iceberg_insert_or_overwrite(
        state,
        target,
        &resolved,
        &[], // no explicit insert column list — use schema order
        &InsertSource::FromQuery(Box::new(query.clone())),
        OverwriteMode::None,
        "main",
    )
    .map(|_| ())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

    use super::arrow_schema_to_table_column_defs;
    use crate::sql::parser::ast::SqlType;

    // ---------- basic scalar types ----------

    #[test]
    fn arrow_schema_to_table_column_defs_basic() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("amount", DataType::Decimal128(10, 2), true),
        ]);
        let cols = arrow_schema_to_table_column_defs(&schema).unwrap();
        assert_eq!(cols.len(), 3);

        assert_eq!(cols[0].name, "id");
        assert!(
            matches!(cols[0].data_type, SqlType::Int),
            "expected Int, got {:?}",
            cols[0].data_type
        );
        assert!(cols[0].nullable);
        assert!(cols[0].aggregation.is_none());
        assert!(cols[0].default.is_none());

        assert_eq!(cols[1].name, "name");
        assert!(
            matches!(cols[1].data_type, SqlType::String),
            "expected String, got {:?}",
            cols[1].data_type
        );

        assert_eq!(cols[2].name, "amount");
        assert!(
            matches!(
                cols[2].data_type,
                SqlType::Decimal {
                    precision: 10,
                    scale: 2
                }
            ),
            "expected Decimal(10,2), got {:?}",
            cols[2].data_type
        );
    }

    #[test]
    fn arrow_schema_to_table_column_defs_nullability_propagated() {
        let schema = Schema::new(vec![
            Field::new("required_col", DataType::Int64, false),
            Field::new("optional_col", DataType::Utf8, true),
        ]);
        let cols = arrow_schema_to_table_column_defs(&schema).unwrap();
        assert!(
            !cols[0].nullable,
            "Int64 NOT NULL field should not be nullable"
        );
        assert!(cols[1].nullable, "Utf8 NULL field should be nullable");
    }

    #[test]
    fn arrow_schema_to_table_column_defs_all_primitive_types() {
        let schema = Schema::new(vec![
            Field::new("b", DataType::Boolean, true),
            Field::new("i8", DataType::Int8, true),
            Field::new("i16", DataType::Int16, true),
            Field::new("i32", DataType::Int32, true),
            Field::new("i64", DataType::Int64, true),
            Field::new("f32", DataType::Float32, true),
            Field::new("f64", DataType::Float64, true),
            Field::new("s", DataType::Utf8, true),
            Field::new("ls", DataType::LargeUtf8, true),
            Field::new("bin", DataType::Binary, true),
            Field::new("lbin", DataType::LargeBinary, true),
            Field::new("d", DataType::Date32, true),
            Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new("t", DataType::Time64(TimeUnit::Microsecond), true),
        ]);
        let cols = arrow_schema_to_table_column_defs(&schema).unwrap();
        assert!(matches!(cols[0].data_type, SqlType::Boolean));
        assert!(matches!(cols[1].data_type, SqlType::TinyInt));
        assert!(matches!(cols[2].data_type, SqlType::SmallInt));
        assert!(matches!(cols[3].data_type, SqlType::Int));
        assert!(matches!(cols[4].data_type, SqlType::BigInt));
        assert!(matches!(cols[5].data_type, SqlType::Float));
        assert!(matches!(cols[6].data_type, SqlType::Double));
        assert!(matches!(cols[7].data_type, SqlType::String));
        assert!(matches!(cols[8].data_type, SqlType::String)); // LargeUtf8
        assert!(matches!(cols[9].data_type, SqlType::Binary));
        assert!(matches!(cols[10].data_type, SqlType::Binary)); // LargeBinary
        assert!(matches!(cols[11].data_type, SqlType::Date));
        assert!(matches!(cols[12].data_type, SqlType::DateTime));
        assert!(matches!(cols[13].data_type, SqlType::Time));
    }

    // ---------- unsupported types ----------

    #[test]
    fn arrow_schema_to_table_column_defs_rejects_unsupported() {
        let schema = Schema::new(vec![
            Field::new("e", DataType::Float16, true), // unsupported
        ]);
        let err = arrow_schema_to_table_column_defs(&schema).unwrap_err();
        assert!(
            err.to_lowercase().contains("not supported"),
            "expected 'not supported' in error, got: {err}"
        );
    }

    #[test]
    fn arrow_schema_to_table_column_defs_rejects_interval() {
        use arrow::datatypes::IntervalUnit;
        let schema = Schema::new(vec![Field::new(
            "iv",
            DataType::Interval(IntervalUnit::DayTime),
            true,
        )]);
        let err = arrow_schema_to_table_column_defs(&schema).unwrap_err();
        assert!(
            err.to_lowercase().contains("not supported"),
            "expected 'not supported' in error, got: {err}"
        );
    }

    // ---------- nested types ----------

    #[test]
    fn arrow_schema_to_table_column_defs_recurses_list() {
        let elem = Field::new("item", DataType::Int64, true);
        let schema = Schema::new(vec![Field::new(
            "ids",
            DataType::List(Arc::new(elem)),
            true,
        )]);
        let cols = arrow_schema_to_table_column_defs(&schema).unwrap();
        assert_eq!(cols.len(), 1);
        assert!(
            matches!(&cols[0].data_type, SqlType::Array(inner) if matches!(inner.as_ref(), SqlType::BigInt)),
            "expected Array(BigInt), got {:?}",
            cols[0].data_type
        );
    }

    #[test]
    fn arrow_schema_to_table_column_defs_recurses_struct_and_list() {
        // Struct{a: Int32, b: Utf8}
        let struct_field = Field::new(
            "meta",
            DataType::Struct(
                vec![
                    Field::new("a", DataType::Int32, true),
                    Field::new("b", DataType::Utf8, true),
                ]
                .into(),
            ),
            true,
        );
        // List<Int64>
        let list_field = Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            true,
        );
        let schema = Schema::new(vec![struct_field, list_field]);
        let cols = arrow_schema_to_table_column_defs(&schema).unwrap();
        assert_eq!(cols.len(), 2);

        // Verify struct
        let SqlType::Struct(fields) = &cols[0].data_type else {
            panic!("expected Struct, got {:?}", cols[0].data_type);
        };
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].0, "a");
        assert!(matches!(fields[0].1, SqlType::Int));
        assert_eq!(fields[1].0, "b");
        assert!(matches!(fields[1].1, SqlType::String));

        // Verify list
        let SqlType::Array(inner) = &cols[1].data_type else {
            panic!("expected Array, got {:?}", cols[1].data_type);
        };
        assert!(matches!(inner.as_ref(), SqlType::BigInt));
    }

    // ---------- IF NOT EXISTS parser test ----------

    #[test]
    fn parse_create_table_if_not_exists_sets_flag() {
        use crate::sql::parser::dialect::{
            StarRocksDialect, create_table::parse_create_table_statement,
        };

        let mut parser = sqlparser::parser::Parser::new(&StarRocksDialect)
            .try_with_sql("CREATE TABLE IF NOT EXISTS t AS SELECT 1 AS x")
            .expect("parser init");
        let stmt = parse_create_table_statement(&mut parser).expect("parse");
        assert!(
            stmt.if_not_exists,
            "IF NOT EXISTS must set the if_not_exists field to true"
        );
        assert!(stmt.as_select.is_some());
    }

    #[test]
    fn parse_create_table_without_if_not_exists_flag_is_false() {
        use crate::sql::parser::dialect::{
            StarRocksDialect, create_table::parse_create_table_statement,
        };

        let mut parser = sqlparser::parser::Parser::new(&StarRocksDialect)
            .try_with_sql("CREATE TABLE t AS SELECT 1 AS x")
            .expect("parser init");
        let stmt = parse_create_table_statement(&mut parser).expect("parse");
        assert!(
            !stmt.if_not_exists,
            "without IF NOT EXISTS the flag should be false"
        );
    }
}
