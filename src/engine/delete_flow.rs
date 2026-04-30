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

//! Standalone-mode `DELETE FROM iceberg ... WHERE ...` entry point.
//!
//! Phase 1 path:
//! 1. Resolve + load the iceberg table.
//! 2. Run pre-lowering validators and choose the Iceberg write mode.
//! 3. Translate the sqlparser WHERE into an iceberg [`Predicate`]. Phase 1
//!    supports comparison operators (`= != < <= > >=`), `IN (...)`, and
//!    `AND` / `OR` against primitive columns (int / long / string / bool).
//!    Other expressions are rejected with an explicit error.
//! 4. Build a [`TableScan`] with `_file`, `_pos`, and the primitive columns
//!    referenced by the WHERE expression. The iceberg `Predicate` is still
//!    passed into planning for manifest pruning.
//! 5. Drain the resulting Arrow stream, group `(file_path, pos)` pairs by
//!    `file_path`, and write one v2 position-delete Parquet file per group
//!    via [`write_position_delete_files`].
//! 6. Inject the resulting [`WrittenFile`]s into [`IcebergCommitCollector`]
//!    and dispatch to [`run_iceberg_commit`] (`op_kind = RowDelta`).

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, BooleanArray, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::DataType;
use futures::StreamExt;
use iceberg::Catalog;
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::expr::{Predicate, Reference};
use iceberg::spec::{Datum, PrimitiveType, Type};
use sqlparser::ast as sqlast;

use crate::connector::iceberg::catalog::registry::{block_on_iceberg, build_hadoop_catalog};
use crate::connector::iceberg::commit::{
    CommitOpKind, IcebergCommitCollector, IcebergWriteMode, PositionDeleteGroup, RunInput,
    ensure_iceberg_write_supported, ensure_no_equality_deletes, ensure_single_partition_spec,
    run_iceberg_commit, write_position_delete_files,
};
use crate::engine::backend_resolver::resolve_existing_table_target;
use crate::engine::{StandaloneState, StatementResult};
use crate::sql::parser::ast::DeleteStmt;

pub(crate) fn execute_delete_statement(
    state: &Arc<StandaloneState>,
    stmt: &DeleteStmt,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<StatementResult, String> {
    // 1. Resolve target.
    let target =
        resolve_existing_table_target(state, &stmt.table, current_catalog, current_database)?;
    if target.backend_name != "iceberg" {
        return Err(format!(
            "phase 1 DELETE only supports iceberg backends, got `{}`",
            target.backend_name
        ));
    }

    // 2. Build iceberg-rust catalog handle + load table.
    let entry = {
        let registry = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        registry.get(&target.catalog)?
    };
    let hadoop_catalog = build_hadoop_catalog(&entry)?;
    let catalog: Arc<dyn Catalog> = Arc::new(hadoop_catalog);
    let table_ident = iceberg::TableIdent::new(
        iceberg::NamespaceIdent::new(target.namespace.clone()),
        target.table.clone(),
    );
    let table = block_on_iceberg(async { catalog.load_table(&table_ident).await })?
        .map_err(|e| format!("load iceberg table {}: {e}", &table_ident))?;

    // 3. Validation.
    let write_mode = ensure_iceberg_write_supported(&table)?;
    ensure_single_partition_spec(&table)?;
    ensure_no_equality_deletes(&table)?;

    // 4. Validate WHERE → iceberg::Predicate to surface unsupported clauses
    //    early. The bound `Predicate` is also used for manifest-level pruning
    //    inside [`scan_for_position_deletes`].
    let schema = table.metadata().current_schema();
    let predicate = translate_where(&stmt.where_clause, schema.as_ref())?;

    // 5. Scan data files and collect (file, pos) pairs. This path still reads
    //    every physical row and applies the original sqlparser WHERE AST per
    //    row so the currently supported DELETE semantics stay unchanged while
    //    manifest pruning remains available through the iceberg Predicate.
    let groups = block_on_iceberg(async {
        scan_for_position_deletes(&table, predicate, &stmt.where_clause).await
    })??;

    // Empty result → no rows match the WHERE; return Ok without commit.
    if groups.iter().all(|g| g.positions.is_empty()) {
        return Ok(StatementResult::Ok);
    }

    let metadata = table.metadata();
    let staging_dir = format!(
        "{}/data/_staging/{}",
        metadata.location(),
        uuid::Uuid::new_v4()
    );
    let file_io = table.file_io().clone();

    match write_mode {
        IcebergWriteMode::LegacyPositionDeletes => {
            // 6. Write v2 Parquet position-delete files into staging.
            let written = block_on_iceberg(async {
                write_position_delete_files(
                    &file_io,
                    &staging_dir,
                    metadata.default_partition_spec_id(),
                    groups,
                )
                .await
            })??;

            // 7. Build collector + inject written files + commit via RowDeltaCommit.
            let collector = Arc::new(IcebergCommitCollector::new(
                CommitOpKind::RowDelta,
                table_ident,
                metadata.current_snapshot().map(|s| s.snapshot_id()),
                metadata.last_sequence_number(),
                metadata.current_schema().clone(),
                metadata.default_partition_spec().clone(),
                staging_dir.clone(),
                crate::common::types::UniqueId { hi: 0, lo: 0 },
            ));
            for wf in written {
                collector.inject_written_file(wf);
            }

            let fs = build_local_fs_operator()?;
            let _outcome = block_on_iceberg(async {
                run_iceberg_commit(RunInput {
                    collector: collector.clone(),
                    catalog: catalog.clone(),
                    table,
                    fs,
                    file_io,
                })
                .await
            })??;
        }
        IcebergWriteMode::RowLineageV3 => {
            // 6/7. Inject the grouped DELETE positions and let RowDeltaDvCommit
            //      build the merged Puffin deletion vectors at commit time.
            let collector = Arc::new(IcebergCommitCollector::new(
                CommitOpKind::RowDeltaDv,
                table_ident,
                metadata.current_snapshot().map(|s| s.snapshot_id()),
                metadata.last_sequence_number(),
                metadata.current_schema().clone(),
                metadata.default_partition_spec().clone(),
                staging_dir.clone(),
                crate::common::types::UniqueId { hi: 0, lo: 0 },
            ));
            for group in groups {
                collector.inject_delete_group(group);
            }

            let fs = build_local_fs_operator()?;
            let _outcome = block_on_iceberg(async {
                run_iceberg_commit(RunInput {
                    collector: collector.clone(),
                    catalog: catalog.clone(),
                    table,
                    fs,
                    file_io,
                })
                .await
            })??;
        }
    }

    // Invalidate caches so subsequent SELECTs see the new snapshot.
    crate::engine::iceberg_writer::invalidate_iceberg_caches(state, &target)?;

    Ok(StatementResult::Ok)
}

/// Translate a `sqlparser::ast::Expr` into an [`iceberg::expr::Predicate`].
///
/// Phase 1 supports the following node shapes; everything else is rejected
/// with an explicit error pointing at the unsupported construct so the caller
/// can rewrite the WHERE clause.
fn translate_where(
    expr: &sqlast::Expr,
    schema: &iceberg::spec::Schema,
) -> Result<Predicate, String> {
    match expr {
        sqlast::Expr::BinaryOp { left, op, right } => match op {
            sqlast::BinaryOperator::And => {
                let l = translate_where(left, schema)?;
                let r = translate_where(right, schema)?;
                Ok(l.and(r))
            }
            sqlast::BinaryOperator::Or => {
                let l = translate_where(left, schema)?;
                let r = translate_where(right, schema)?;
                Ok(l.or(r))
            }
            sqlast::BinaryOperator::Eq
            | sqlast::BinaryOperator::NotEq
            | sqlast::BinaryOperator::Lt
            | sqlast::BinaryOperator::LtEq
            | sqlast::BinaryOperator::Gt
            | sqlast::BinaryOperator::GtEq => {
                let (col_name, value_expr, flipped) = extract_comparison(left, right)?;
                let datum = literal_to_datum(value_expr, schema, &col_name)?;
                let term = Reference::new(col_name);
                let pred = match (op, flipped) {
                    (sqlast::BinaryOperator::Eq, _) => term.equal_to(datum),
                    (sqlast::BinaryOperator::NotEq, _) => term.not_equal_to(datum),
                    (sqlast::BinaryOperator::Lt, false) | (sqlast::BinaryOperator::Gt, true) => {
                        term.less_than(datum)
                    }
                    (sqlast::BinaryOperator::LtEq, false)
                    | (sqlast::BinaryOperator::GtEq, true) => term.less_than_or_equal_to(datum),
                    (sqlast::BinaryOperator::Gt, false) | (sqlast::BinaryOperator::Lt, true) => {
                        term.greater_than(datum)
                    }
                    (sqlast::BinaryOperator::GtEq, false)
                    | (sqlast::BinaryOperator::LtEq, true) => term.greater_than_or_equal_to(datum),
                    _ => unreachable!(),
                };
                Ok(pred)
            }
            other => Err(format!(
                "phase 1 DELETE WHERE does not support binary operator `{other:?}`"
            )),
        },
        sqlast::Expr::InList {
            expr,
            list,
            negated,
        } => {
            let col_name = expr_to_column_name(expr)?;
            let datums: Vec<Datum> = list
                .iter()
                .map(|lit| literal_to_datum(lit, schema, &col_name))
                .collect::<Result<_, _>>()?;
            let term = Reference::new(col_name);
            let pred = if *negated {
                term.is_not_in(datums)
            } else {
                term.is_in(datums)
            };
            Ok(pred)
        }
        sqlast::Expr::IsNull(inner) => {
            let col = expr_to_column_name(inner)?;
            Ok(Reference::new(col).is_null())
        }
        sqlast::Expr::IsNotNull(inner) => {
            let col = expr_to_column_name(inner)?;
            Ok(Reference::new(col).is_not_null())
        }
        sqlast::Expr::Nested(inner) => translate_where(inner, schema),
        other => Err(format!(
            "phase 1 DELETE WHERE supports comparison / IN / IS NULL / AND / OR \
             over primitive columns; rewrite this clause and retry. Unsupported: {other:?}"
        )),
    }
}

/// One side of a comparison must be a column reference and the other a literal.
/// Returns `(column_name, literal_expr, flipped)` where `flipped = true`
/// indicates the original was `<literal> <op> <column>`.
fn extract_comparison<'a>(
    left: &'a sqlast::Expr,
    right: &'a sqlast::Expr,
) -> Result<(String, &'a sqlast::Expr, bool), String> {
    if let Ok(name) = expr_to_column_name(left) {
        return Ok((name, right, false));
    }
    if let Ok(name) = expr_to_column_name(right) {
        return Ok((name, left, true));
    }
    Err(
        "phase 1 DELETE WHERE comparison must have exactly one side that is a \
         column reference (the other must be a literal)"
            .to_string(),
    )
}

fn expr_to_column_name(expr: &sqlast::Expr) -> Result<String, String> {
    match expr {
        sqlast::Expr::Identifier(ident) => Ok(ident.value.to_lowercase()),
        sqlast::Expr::CompoundIdentifier(parts) => {
            // a.b.c → take the last part (the column name); table-qualified
            // refs work because the Predicate is bound against the
            // single-table schema via TableScan.with_filter.
            parts
                .last()
                .map(|p| p.value.to_lowercase())
                .ok_or_else(|| "compound identifier has no parts".to_string())
        }
        other => Err(format!(
            "phase 1 DELETE WHERE expected a column identifier here, got {other:?}"
        )),
    }
}

fn literal_to_datum(
    expr: &sqlast::Expr,
    schema: &iceberg::spec::Schema,
    column_name: &str,
) -> Result<Datum, String> {
    let field = schema
        .as_struct()
        .fields()
        .iter()
        .find(|f| f.name.eq_ignore_ascii_case(column_name))
        .ok_or_else(|| format!("column `{column_name}` not found in iceberg table schema"))?;
    let prim = match &*field.field_type {
        Type::Primitive(p) => p,
        other => {
            return Err(format!(
                "phase 1 DELETE WHERE only supports primitive columns; column `{column_name}` is {other:?}"
            ));
        }
    };
    let lit_value = match expr {
        sqlast::Expr::Value(v) => v,
        sqlast::Expr::UnaryOp {
            op: sqlast::UnaryOperator::Minus,
            expr: inner,
        } => match inner.as_ref() {
            sqlast::Expr::Value(v) => v,
            other => {
                return Err(format!(
                    "phase 1 DELETE WHERE expects a literal value, got -{other:?}"
                ));
            }
        },
        other => {
            return Err(format!(
                "phase 1 DELETE WHERE expects a literal value, got {other:?}"
            ));
        }
    };
    let negate = matches!(
        expr,
        sqlast::Expr::UnaryOp {
            op: sqlast::UnaryOperator::Minus,
            ..
        }
    );
    let lit_str = match &lit_value.value {
        sqlast::Value::Number(s, _) => s.clone(),
        sqlast::Value::SingleQuotedString(s) | sqlast::Value::DoubleQuotedString(s) => s.clone(),
        sqlast::Value::Boolean(b) => b.to_string(),
        sqlast::Value::Null => {
            return Err(format!(
                "phase 1 DELETE WHERE does not support NULL literals; use IS NULL/IS NOT NULL instead \
                 (column `{column_name}`)"
            ));
        }
        other => {
            return Err(format!(
                "phase 1 DELETE WHERE literal value `{other:?}` is not yet supported"
            ));
        }
    };
    let owned;
    let lit_str = if negate {
        owned = format!("-{lit_str}");
        owned.as_str()
    } else {
        lit_str.as_str()
    };
    match prim {
        PrimitiveType::Int => lit_str
            .parse::<i32>()
            .map(Datum::int)
            .map_err(|e| format!("parse INT literal `{lit_str}` for column `{column_name}`: {e}")),
        PrimitiveType::Long => lit_str
            .parse::<i64>()
            .map(Datum::long)
            .map_err(|e| format!("parse LONG literal `{lit_str}` for column `{column_name}`: {e}")),
        PrimitiveType::String => Ok(Datum::string(lit_str)),
        PrimitiveType::Boolean => lit_str
            .parse::<bool>()
            .map(Datum::bool)
            .map_err(|e| format!("parse BOOL literal `{lit_str}` for column `{column_name}`: {e}")),
        other => Err(format!(
            "phase 1 DELETE WHERE primitive type {other:?} not yet supported (column `{column_name}`)"
        )),
    }
}

async fn scan_for_position_deletes(
    table: &iceberg::table::Table,
    predicate: Predicate,
    where_expr: &sqlast::Expr,
) -> Result<Vec<PositionDeleteGroup>, String> {
    let schema = table.metadata().current_schema();

    // Project `_file`, `_pos`, and every primitive top-level column the WHERE
    // clause may reference. Reading the whole row schema keeps the
    // per-row evaluator simple and matches the supported predicate set
    // (primitive comparisons + IN + IS NULL/IS NOT NULL + AND/OR).
    let mut select_cols: Vec<String> = vec!["_file".to_string(), "_pos".to_string()];
    for f in schema.as_struct().fields() {
        select_cols.push(f.name.clone());
    }

    // Pass `predicate` to the scan so manifest pruning still applies. We
    // strip it on each task below before handing the stream to
    // ArrowReader so no row_filter / row_selection is applied at decode
    // time.
    let scan = table
        .scan()
        .select(select_cols)
        .with_filter(predicate)
        .build()
        .map_err(|e| format!("build TableScan failed: {e}"))?;
    let task_stream = scan
        .plan_files()
        .await
        .map_err(|e| format!("TableScan::plan_files failed: {e}"))?;
    let cleaned_tasks = task_stream.map(|task_result| {
        task_result.map(|mut task| {
            task.deletes.clear();
            task.predicate = None;
            task
        })
    });

    // Build an ArrowReader with row_selection_enabled=false. Combined with
    // cleared `task.deletes` and `task.predicate`, this keeps the manual
    // sqlparser WHERE evaluator responsible for row-level DELETE matching.
    let arrow_reader = ArrowReaderBuilder::new(table.file_io().clone())
        .with_row_group_filtering_enabled(false)
        .with_row_selection_enabled(false)
        .build();
    let mut stream = arrow_reader
        .read(Box::pin(cleaned_tasks))
        .map_err(|e| format!("ArrowReader::read failed: {e}"))?;

    let mut by_file: BTreeMap<String, Vec<i64>> = BTreeMap::new();
    while let Some(batch_result) = stream.next().await {
        let batch = batch_result.map_err(|e| format!("scan stream error: {e}"))?;
        let file_idx = batch
            .schema()
            .index_of("_file")
            .map_err(|_| "scan batch missing `_file` column".to_string())?;
        let pos_idx = batch
            .schema()
            .index_of("_pos")
            .map_err(|_| "scan batch missing `_pos` column".to_string())?;
        // iceberg-rust encodes constant virtual columns (`_file`) as REE
        // (RunEndEncoded) arrays for memory efficiency. Cast to a plain
        // primitive type before downcasting — `arrow::compute::cast`
        // unwraps REE transparently.
        let file_col = arrow::compute::cast(batch.column(file_idx), &DataType::Utf8)
            .map_err(|e| format!("cast _file to Utf8 failed: {e}"))?;
        let pos_col = arrow::compute::cast(batch.column(pos_idx), &DataType::Int64)
            .map_err(|e| format!("cast _pos to Int64 failed: {e}"))?;
        let file_arr = file_col
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| "_file column not Utf8 after cast".to_string())?;
        let pos_arr = pos_col
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| "_pos column not Int64 after cast".to_string())?;
        for i in 0..batch.num_rows() {
            if file_arr.is_null(i) || pos_arr.is_null(i) {
                continue;
            }
            let matches = evaluate_where_at_row(where_expr, &batch, i, schema.as_ref())?;
            if !matches {
                continue;
            }
            let path = file_arr.value(i).to_string();
            let pos = pos_arr.value(i);
            by_file.entry(path).or_default().push(pos);
        }
    }

    Ok(by_file
        .into_iter()
        .map(|(referenced_data_file, positions)| PositionDeleteGroup {
            referenced_data_file,
            positions,
        })
        .collect())
}

/// Evaluate a Phase-1 supported WHERE expression against a single row of a
/// scanned [`RecordBatch`]. Mirrors the operator coverage of
/// [`translate_where`]; any clause this engine cannot map should already have
/// been rejected upstream during predicate translation.
fn evaluate_where_at_row(
    expr: &sqlast::Expr,
    batch: &RecordBatch,
    row: usize,
    schema: &iceberg::spec::Schema,
) -> Result<bool, String> {
    match expr {
        sqlast::Expr::BinaryOp { left, op, right } => match op {
            sqlast::BinaryOperator::And => Ok(evaluate_where_at_row(left, batch, row, schema)?
                && evaluate_where_at_row(right, batch, row, schema)?),
            sqlast::BinaryOperator::Or => Ok(evaluate_where_at_row(left, batch, row, schema)?
                || evaluate_where_at_row(right, batch, row, schema)?),
            sqlast::BinaryOperator::Eq
            | sqlast::BinaryOperator::NotEq
            | sqlast::BinaryOperator::Lt
            | sqlast::BinaryOperator::LtEq
            | sqlast::BinaryOperator::Gt
            | sqlast::BinaryOperator::GtEq => {
                let (col_name, value_expr, flipped) = extract_comparison(left, right)?;
                let cell = column_value_at_row(&col_name, batch, row, schema)?;
                let datum = literal_to_datum(value_expr, schema, &col_name)?;
                let cmp = match cell {
                    None => return Ok(false),
                    Some(v) => compare_cell_to_datum(&v, &datum, &col_name)?,
                };
                Ok(match (op, flipped) {
                    (sqlast::BinaryOperator::Eq, _) => cmp == std::cmp::Ordering::Equal,
                    (sqlast::BinaryOperator::NotEq, _) => cmp != std::cmp::Ordering::Equal,
                    (sqlast::BinaryOperator::Lt, false) | (sqlast::BinaryOperator::Gt, true) => {
                        cmp == std::cmp::Ordering::Less
                    }
                    (sqlast::BinaryOperator::LtEq, false)
                    | (sqlast::BinaryOperator::GtEq, true) => cmp != std::cmp::Ordering::Greater,
                    (sqlast::BinaryOperator::Gt, false) | (sqlast::BinaryOperator::Lt, true) => {
                        cmp == std::cmp::Ordering::Greater
                    }
                    (sqlast::BinaryOperator::GtEq, false)
                    | (sqlast::BinaryOperator::LtEq, true) => cmp != std::cmp::Ordering::Less,
                    _ => unreachable!("unsupported binary operator already rejected upstream"),
                })
            }
            other => Err(format!(
                "phase 1 DELETE WHERE evaluator does not support binary operator `{other:?}`"
            )),
        },
        sqlast::Expr::InList {
            expr,
            list,
            negated,
        } => {
            let col_name = expr_to_column_name(expr)?;
            let cell = column_value_at_row(&col_name, batch, row, schema)?;
            let cell = match cell {
                Some(v) => v,
                None => return Ok(false),
            };
            for lit in list {
                let datum = literal_to_datum(lit, schema, &col_name)?;
                if compare_cell_to_datum(&cell, &datum, &col_name)? == std::cmp::Ordering::Equal {
                    return Ok(!*negated);
                }
            }
            Ok(*negated)
        }
        sqlast::Expr::IsNull(inner) => {
            let col = expr_to_column_name(inner)?;
            Ok(column_value_at_row(&col, batch, row, schema)?.is_none())
        }
        sqlast::Expr::IsNotNull(inner) => {
            let col = expr_to_column_name(inner)?;
            Ok(column_value_at_row(&col, batch, row, schema)?.is_some())
        }
        sqlast::Expr::Nested(inner) => evaluate_where_at_row(inner, batch, row, schema),
        other => Err(format!(
            "phase 1 DELETE WHERE evaluator does not support {other:?}"
        )),
    }
}

/// Owned, evaluator-friendly view of a single row's column value.
#[derive(Debug, Clone)]
enum CellValue {
    Int(i64),
    Long(i64),
    String(String),
    Bool(bool),
}

fn column_value_at_row(
    col_name: &str,
    batch: &RecordBatch,
    row: usize,
    schema: &iceberg::spec::Schema,
) -> Result<Option<CellValue>, String> {
    let field = schema
        .as_struct()
        .fields()
        .iter()
        .find(|f| f.name.eq_ignore_ascii_case(col_name))
        .ok_or_else(|| format!("column `{col_name}` not found in iceberg schema"))?;
    let prim = match &*field.field_type {
        Type::Primitive(p) => p,
        other => {
            return Err(format!(
                "phase 1 DELETE WHERE evaluator only supports primitive columns; column `{col_name}` is {other:?}"
            ));
        }
    };
    let idx = batch
        .schema()
        .index_of(&field.name)
        .map_err(|_| format!("scan batch missing column `{col_name}`"))?;
    let column = batch.column(idx);
    if column.is_null(row) {
        return Ok(None);
    }
    let value = match prim {
        PrimitiveType::Int => {
            let arr = column
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| format!("column `{col_name}` is not Int32"))?;
            CellValue::Int(arr.value(row) as i64)
        }
        PrimitiveType::Long => {
            let arr = column
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| format!("column `{col_name}` is not Int64"))?;
            CellValue::Long(arr.value(row))
        }
        PrimitiveType::String => {
            let arr = column
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| format!("column `{col_name}` is not Utf8"))?;
            CellValue::String(arr.value(row).to_string())
        }
        PrimitiveType::Boolean => {
            let arr = column
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| format!("column `{col_name}` is not Boolean"))?;
            CellValue::Bool(arr.value(row))
        }
        other => {
            return Err(format!(
                "phase 1 DELETE WHERE evaluator does not yet support primitive type {other:?} (column `{col_name}`)"
            ));
        }
    };
    Ok(Some(value))
}

fn compare_cell_to_datum(
    cell: &CellValue,
    datum: &Datum,
    col_name: &str,
) -> Result<std::cmp::Ordering, String> {
    use iceberg::spec::PrimitiveLiteral;
    let lit = datum.literal();
    match (cell, lit) {
        (CellValue::Int(c), PrimitiveLiteral::Int(d)) => Ok(c.cmp(&(*d as i64))),
        (CellValue::Long(c), PrimitiveLiteral::Long(d)) => Ok(c.cmp(d)),
        (CellValue::String(c), PrimitiveLiteral::String(d)) => Ok(c.as_str().cmp(d.as_str())),
        (CellValue::Bool(c), PrimitiveLiteral::Boolean(d)) => Ok(c.cmp(d)),
        (cell, lit) => Err(format!(
            "phase 1 DELETE WHERE evaluator: column `{col_name}` and literal types disagree (cell={cell:?}, lit={lit:?})"
        )),
    }
}

fn build_local_fs_operator() -> Result<opendal::Operator, String> {
    let builder = opendal::services::Fs::default().root("/");
    opendal::Operator::new(builder)
        .map_err(|e| format!("build local-FS operator failed: {e}"))?
        .finish()
        .pipe(Ok)
}

trait Pipe: Sized {
    fn pipe<F, R>(self, f: F) -> R
    where
        F: FnOnce(Self) -> R,
    {
        f(self)
    }
}
impl<T: Sized> Pipe for T {}
