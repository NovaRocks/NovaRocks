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
//!    referenced by the WHERE expression.
//! 5. Drain the resulting Arrow stream, apply existing delete visibility,
//!    group `(file_path, pos)` pairs by `file_path`, and write one v2
//!    position-delete Parquet file per group via
//!    [`write_position_delete_files`].
//! 6. Inject the resulting [`WrittenFile`]s into [`IcebergCommitCollector`]
//!    and dispatch to [`run_iceberg_commit`] (`op_kind = RowDelta`).

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow::array::{Array, BooleanArray, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::DataType;
use futures::StreamExt;
use iceberg::Catalog;
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::expr::{Predicate, Reference};
use iceberg::spec::{Datum, PrimitiveType, Type};
use sqlparser::ast as sqlast;

use crate::connector::iceberg::catalog::registry::{self, block_on_iceberg, build_hadoop_catalog};
use crate::connector::iceberg::commit::{
    CommitOpKind, IcebergCommitCollector, IcebergSqlDeleteStrategy, PositionDeleteGroup, RunInput,
    classify_sql_delete_strategy, ensure_no_variant_columns_for_row_level_mutation,
    run_iceberg_commit, write_position_delete_files,
};
use crate::engine::backend_resolver::resolve_existing_table_target;
use crate::engine::{StandaloneState, StatementResult};
use crate::sql::analyzer::iceberg_ref::{IcebergRefSuffix, split_ref_suffix};
use crate::sql::parser::ast::{DeleteStmt, ObjectName};

pub(crate) fn execute_delete_statement(
    state: &Arc<StandaloneState>,
    stmt: &DeleteStmt,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<StatementResult, String> {
    // Detect branch/tag suffix in the target table name.
    let (stripped_parts, ref_suffix) = split_ref_suffix(&stmt.table.parts);
    let effective_name;
    let table_name: &ObjectName = match ref_suffix {
        Some(IcebergRefSuffix::Tag(ref tag_name)) => {
            return Err(format!(
                "iceberg ref: tag '{tag_name}' is read-only; use a branch as DML target"
            ));
        }
        Some(IcebergRefSuffix::Branch(_)) => {
            effective_name = ObjectName {
                parts: stripped_parts,
            };
            &effective_name
        }
        None => &stmt.table,
    };
    let target_ref = match &ref_suffix {
        Some(IcebergRefSuffix::Branch(b)) => b.clone(),
        _ => "main".to_string(),
    };

    // 1. Resolve target.
    let target =
        resolve_existing_table_target(state, table_name, current_catalog, current_database)?;
    if target.backend_name == "managed" {
        return execute_managed_delete_statement(state, &target, stmt, &target_ref);
    }
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

    // Branch writes require Iceberg v3 (row-lineage semantics).
    if target_ref != "main" {
        let fmt = table.metadata().format_version();
        if fmt != iceberg::spec::FormatVersion::V3 {
            return Err(format!(
                "iceberg ref: branch writes require Iceberg v3 tables (table {} is v{})",
                table_ident, fmt as u8,
            ));
        }
    }

    // 3. Validation.
    ensure_no_variant_columns_for_row_level_mutation(&table).map_err(|e| format!("DELETE: {e}"))?;
    let delete_strategy = classify_sql_delete_strategy(&table)?;
    // 4. Validate WHERE → iceberg::Predicate to surface unsupported clauses
    //    early. The bound `Predicate` is also used for manifest-level pruning
    //    inside [`scan_for_position_deletes`].
    let schema = table.metadata().current_schema();
    let predicate = translate_where(&stmt.where_clause, schema.as_ref())?;

    // For branch DML, resolve the branch head snapshot to read the base state
    // at that point rather than the table's current_snapshot.
    let read_snapshot_id: Option<i64> = if target_ref != "main" {
        resolve_branch_head_snapshot_id(table.metadata(), &target_ref)?
    } else {
        table.metadata().current_snapshot().map(|s| s.snapshot_id())
    };

    let existing_deletes_by_file = load_existing_delete_visibility_by_data_file_at(
        &table,
        read_snapshot_id,
        entry.object_store_config(),
    )?;
    let referenced_data_file_partitions =
        load_referenced_data_file_partitions_at(&table, read_snapshot_id)?;

    // 5. Scan data files and collect (file, pos) pairs. This path still reads
    //    every physical row and applies the original sqlparser WHERE AST per
    //    row so the currently supported DELETE semantics stay unchanged while
    //    existing row-level deletes remain visible to the write-side planner.
    let groups = block_on_iceberg(async {
        scan_for_position_deletes_at(
            &table,
            read_snapshot_id,
            predicate,
            &stmt.where_clause,
            &existing_deletes_by_file,
            &referenced_data_file_partitions,
        )
        .await
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

    // Determine the base snapshot for the commit. For branch DML, use the
    // branch head; for main/default, use the current snapshot.
    let base_snapshot_id = if target_ref != "main" {
        resolve_branch_head_snapshot_id(metadata, &target_ref)?
    } else {
        metadata.current_snapshot().map(|s| s.snapshot_id())
    };

    match delete_strategy {
        IcebergSqlDeleteStrategy::PositionDeleteFiles => {
            // 6. Write v2 Parquet position-delete files into staging.
            let written = block_on_iceberg(async {
                write_position_delete_files(&file_io, &staging_dir, groups).await
            })??;

            // 7. Build collector + inject written files + commit via RowDeltaCommit.
            let collector = Arc::new(IcebergCommitCollector::new(
                CommitOpKind::RowDelta,
                table_ident,
                base_snapshot_id,
                metadata.last_sequence_number(),
                metadata.current_schema().clone(),
                metadata.default_partition_spec().clone(),
                staging_dir.clone(),
                crate::common::types::UniqueId { hi: 0, lo: 0 },
            ));
            for wf in written {
                collector.inject_written_file(wf);
            }

            let abort_cleanup =
                crate::engine::iceberg_writer::build_abort_cleanup_for_catalog_entry(&entry)?;
            let _outcome = block_on_iceberg(async {
                run_iceberg_commit(RunInput {
                    collector: collector.clone(),
                    catalog: catalog.clone(),
                    table,
                    fs: abort_cleanup.fs,
                    file_io,
                    cleanup_path_mapper: abort_cleanup.path_mapper,
                    cow_update_rewrite: None,
                    target_ref: target_ref.clone(),
                    snapshot_properties: BTreeMap::new(),
                })
                .await
            })??;
        }
        IcebergSqlDeleteStrategy::DeletionVectors => {
            // 6/7. Inject the grouped DELETE positions and let RowDeltaDvCommit
            //      build the merged Puffin deletion vectors at commit time.
            let collector = Arc::new(IcebergCommitCollector::new(
                CommitOpKind::RowDeltaDv,
                table_ident,
                base_snapshot_id,
                metadata.last_sequence_number(),
                metadata.current_schema().clone(),
                metadata.default_partition_spec().clone(),
                staging_dir.clone(),
                crate::common::types::UniqueId { hi: 0, lo: 0 },
            ));
            for group in groups {
                collector.inject_delete_group(group);
            }

            let abort_cleanup =
                crate::engine::iceberg_writer::build_abort_cleanup_for_catalog_entry(&entry)?;
            let _outcome = block_on_iceberg(async {
                run_iceberg_commit(RunInput {
                    collector: collector.clone(),
                    catalog: catalog.clone(),
                    table,
                    fs: abort_cleanup.fs,
                    file_io,
                    cleanup_path_mapper: abort_cleanup.path_mapper,
                    cow_update_rewrite: None,
                    target_ref: target_ref.clone(),
                    snapshot_properties: BTreeMap::new(),
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

fn execute_managed_delete_statement(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    stmt: &DeleteStmt,
    target_ref: &str,
) -> Result<StatementResult, String> {
    if target_ref != "main" {
        return Err(format!(
            "DELETE: branch target `{target_ref}` is only supported for iceberg tables"
        ));
    }

    let (catalog, sink) = {
        let reg = state.connectors.read().expect("connector registry read");
        (
            reg.catalog_backend(target.backend_name)?,
            reg.table_sink(target.backend_name)?,
        )
    };
    let resolved = catalog.load_table(&target.catalog, &target.namespace, &target.table)?;
    let rewritten_sql = format!(
        "SELECT * FROM {} WHERE NOT COALESCE(({}), FALSE)",
        target.table, stmt.where_clause
    );
    let statement = crate::sql::parser::parse_sql_raw(&rewritten_sql)?;
    let sqlast::Statement::Query(query) = statement else {
        return Err("internal: managed DELETE rewrite did not parse as SELECT".to_string());
    };
    let batch = crate::engine::insert_flow::execute_insert_from_query_on_pipeline(
        state,
        target,
        &resolved,
        &[],
        query.as_ref(),
    )?;

    crate::connector::truncate_managed_table(state, &target.namespace, &target.table)?;
    if batch.num_rows() > 0 {
        sink.append_batch(&resolved, batch)?;
    }
    Ok(StatementResult::Ok)
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

async fn scan_for_position_deletes_at(
    table: &iceberg::table::Table,
    snapshot_id: Option<i64>,
    predicate: Predicate,
    where_expr: &sqlast::Expr,
    existing_deletes_by_file: &ExistingDeleteVisibilityByDataFile,
    referenced_data_file_partitions: &ReferencedDataFilePartitions,
) -> Result<Vec<PositionDeleteGroup>, String> {
    let schema = table.metadata().current_schema();

    let mut select_cols: Vec<String> = vec!["_file".to_string(), "_pos".to_string()];
    for f in schema.as_struct().fields() {
        select_cols.push(f.name.clone());
    }

    let _ = predicate;
    let mut scan_builder = table.scan().select(select_cols);
    if let Some(snap_id) = snapshot_id {
        scan_builder = scan_builder.snapshot_id(snap_id);
    }
    let scan = scan_builder
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
        collect_position_deletes_from_batch(
            &batch,
            where_expr,
            schema.as_ref(),
            existing_deletes_by_file,
            &mut by_file,
        )?;
    }

    Ok(by_file
        .into_iter()
        .map(|(referenced_data_file, positions)| {
            let partition = referenced_data_file_partitions
                .get(&referenced_data_file)
                .ok_or_else(|| {
                    format!(
                        "matched iceberg data file `{referenced_data_file}` is missing partition metadata"
                    )
                })?;
            Ok(PositionDeleteGroup {
                referenced_data_file,
                partition_spec_id: partition.partition_spec_id,
                partition_values: partition.partition_values.clone(),
                positions,
            })
        })
        .collect::<Result<Vec<_>, String>>()?)
}

async fn scan_for_position_deletes(
    table: &iceberg::table::Table,
    predicate: Predicate,
    where_expr: &sqlast::Expr,
    existing_deletes_by_file: &ExistingDeleteVisibilityByDataFile,
    referenced_data_file_partitions: &ReferencedDataFilePartitions,
) -> Result<Vec<PositionDeleteGroup>, String> {
    scan_for_position_deletes_at(
        table,
        None,
        predicate,
        where_expr,
        existing_deletes_by_file,
        referenced_data_file_partitions,
    )
    .await
}

pub(crate) struct ReferencedDataFilePartition {
    pub(crate) partition_spec_id: i32,
    pub(crate) partition_values: iceberg::spec::Struct,
}

pub(crate) type ReferencedDataFilePartitions = HashMap<String, ReferencedDataFilePartition>;

/// Resolve the snapshot id at the head of a named Iceberg branch.
///
/// Returns `None` when the branch exists but has never had a snapshot committed
/// to it (unborn branch). Returns an error when the ref does not exist in the
/// table metadata.
pub(crate) fn resolve_branch_head_snapshot_id(
    metadata: &iceberg::spec::TableMetadata,
    branch_name: &str,
) -> Result<Option<i64>, String> {
    match metadata.refs().get(branch_name) {
        Some(snap_ref) => Ok(Some(snap_ref.snapshot_id)),
        None => {
            if branch_name == "main" && metadata.current_snapshot().is_none() {
                // Unborn main branch — no snapshot yet; caller should treat as empty.
                Ok(None)
            } else {
                Err(format!(
                    "iceberg ref: branch '{branch_name}' not found in table metadata"
                ))
            }
        }
    }
}

/// Snapshot-aware version of [`load_referenced_data_file_partitions`].
///
/// Uses `snapshot_id` when `Some`, otherwise falls back to the current snapshot.
pub(crate) fn load_referenced_data_file_partitions_at(
    table: &iceberg::table::Table,
    snapshot_id: Option<i64>,
) -> Result<ReferencedDataFilePartitions, String> {
    let data_files = match snapshot_id {
        Some(id) => registry::extract_data_files_with_stats_at(table, id)?,
        None => registry::extract_data_files_with_stats(table)?,
    };
    let mut out = HashMap::with_capacity(data_files.len());
    for data_file in data_files {
        let partition_spec_id = data_file.partition_spec_id.ok_or_else(|| {
            format!(
                "iceberg data file `{}` missing partition spec id",
                data_file.path
            )
        })?;
        let partition_values = data_file.partition_values.ok_or_else(|| {
            format!(
                "iceberg data file `{}` missing partition values",
                data_file.path
            )
        })?;
        let partition = ReferencedDataFilePartition {
            partition_spec_id,
            partition_values,
        };
        insert_referenced_data_file_partition(&mut out, data_file.path, partition)?;
    }
    Ok(out)
}

pub(crate) fn load_referenced_data_file_partitions(
    table: &iceberg::table::Table,
) -> Result<ReferencedDataFilePartitions, String> {
    load_referenced_data_file_partitions_at(table, None)
}

fn insert_referenced_data_file_partition(
    partitions: &mut ReferencedDataFilePartitions,
    path: String,
    partition: ReferencedDataFilePartition,
) -> Result<(), String> {
    match partitions.entry(path) {
        std::collections::hash_map::Entry::Vacant(entry) => {
            entry.insert(partition);
        }
        std::collections::hash_map::Entry::Occupied(entry) => {
            let existing = entry.get();
            if existing.partition_spec_id == partition.partition_spec_id
                && existing.partition_values == partition.partition_values
            {
                return Ok(());
            }
            return Err(format!(
                "iceberg data file `{}` has conflicting partition metadata: old partition spec id {}, new partition spec id {}",
                entry.key(),
                existing.partition_spec_id,
                partition.partition_spec_id
            ));
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ExistingDeleteVisibility {
    pub(crate) deleted_positions: roaring::RoaringTreemap,
    pub(crate) equality_deletes: Vec<crate::connector::iceberg::equality_delete::EqualityDeleteSet>,
}

pub(crate) type ExistingDeleteVisibilityByDataFile = HashMap<String, ExistingDeleteVisibility>;

/// Snapshot-aware version of [`load_existing_delete_visibility_by_data_file`].
///
/// Uses `snapshot_id` when `Some`, otherwise falls back to the current snapshot.
pub(crate) fn load_existing_delete_visibility_by_data_file_at(
    table: &iceberg::table::Table,
    snapshot_id: Option<i64>,
    object_store_config: Option<&crate::fs::object_store::ObjectStoreConfig>,
) -> Result<ExistingDeleteVisibilityByDataFile, String> {
    let data_files = match snapshot_id {
        Some(id) => crate::connector::iceberg::catalog::registry::extract_data_files_with_stats_at(
            table, id,
        )?,
        None => crate::connector::iceberg::catalog::registry::extract_data_files_with_stats(table)?,
    };
    load_delete_visibility_from_data_files(data_files, object_store_config)
}

pub(crate) fn load_existing_delete_visibility_by_data_file(
    table: &iceberg::table::Table,
    object_store_config: Option<&crate::fs::object_store::ObjectStoreConfig>,
) -> Result<ExistingDeleteVisibilityByDataFile, String> {
    load_existing_delete_visibility_by_data_file_at(table, None, object_store_config)
}

fn load_delete_visibility_from_data_files(
    data_files: Vec<crate::connector::iceberg::catalog::registry::DataFileWithStats>,
    object_store_config: Option<&crate::fs::object_store::ObjectStoreConfig>,
) -> Result<ExistingDeleteVisibilityByDataFile, String> {
    let mut out: ExistingDeleteVisibilityByDataFile = HashMap::new();

    for data_file in data_files {
        if data_file.delete_files.is_empty() {
            continue;
        }

        let data_file_len = u64::try_from(data_file.size)
            .map_err(|_| format!("iceberg data file size is negative: {}", data_file.path))?;
        let mut loader_ranges = Vec::with_capacity(1 + data_file.delete_files.len());
        loader_ranges.push(crate::fs::scan_context::FileScanRange {
            path: data_file.path.clone(),
            file_len: data_file_len,
            offset: 0,
            length: data_file_len,
            scan_range_id: -1,
            first_row_id: data_file.first_row_id,
            data_sequence_number: data_file.data_sequence_number,
            ivm_change_op: None,
            external_datacache: None,
            delete_files: Vec::new(),
        });
        for delete_file in &data_file.delete_files {
            let delete_len_i64 = delete_file.length.unwrap_or(0);
            let delete_len = u64::try_from(delete_len_i64).map_err(|_| {
                format!("iceberg delete file size is negative: {}", delete_file.path)
            })?;
            loader_ranges.push(crate::fs::scan_context::FileScanRange {
                path: delete_file.path.clone(),
                file_len: delete_len,
                offset: 0,
                length: delete_len,
                scan_range_id: -1,
                first_row_id: None,
                data_sequence_number: None,
                ivm_change_op: None,
                external_datacache: None,
                delete_files: Vec::new(),
            });
        }

        let ctx = crate::fs::scan_context::FileScanContext::build(
            loader_ranges,
            None,
            object_store_config,
        )?;
        let normalized_delete_specs = ctx
            .ranges
            .iter()
            .skip(1)
            .zip(data_file.delete_files.iter())
            .map(|(resolved, original)| {
                let file_format = match original.file_format {
                    crate::sql::catalog::IcebergDeleteFileFormat::Parquet => {
                        crate::descriptors::THdfsFileFormat::PARQUET
                    }
                    crate::sql::catalog::IcebergDeleteFileFormat::Puffin => {
                        crate::descriptors::THdfsFileFormat::PARQUET
                    }
                };
                let file_content = match original.file_content {
                    crate::sql::catalog::IcebergDeleteFileContent::Position => {
                        crate::types::TIcebergFileContent::POSITION_DELETES
                    }
                    crate::sql::catalog::IcebergDeleteFileContent::Equality => {
                        crate::types::TIcebergFileContent::EQUALITY_DELETES
                    }
                };
                Ok(
                    crate::connector::iceberg::position_delete::IcebergDeleteFileSpec {
                        path: resolved.path.clone(),
                        file_format,
                        file_content,
                        length: original
                            .length
                            .map(u64::try_from)
                            .transpose()
                            .map_err(|_| {
                                format!("iceberg delete file size is negative: {}", original.path)
                            })?,
                        content_offset: original.content_offset,
                        content_size_in_bytes: original.content_size_in_bytes,
                    },
                )
            })
            .collect::<Result<Vec<_>, String>>()?;
        let deleted_positions = crate::connector::iceberg::position_delete::load_position_deletes(
            &normalized_delete_specs,
            &data_file.path,
            &ctx.factory,
        )?;
        let equality_deletes =
            crate::connector::iceberg::equality_delete::load_equality_delete_sets(
                &normalized_delete_specs,
                &ctx.factory,
            )?;
        if deleted_positions.is_empty() && equality_deletes.is_empty() {
            continue;
        }
        let visibility = ExistingDeleteVisibility {
            deleted_positions,
            equality_deletes,
        };
        if let Some(resolved_data_file) = ctx.ranges.first()
            && resolved_data_file.path != data_file.path
        {
            out.insert(resolved_data_file.path.clone(), visibility.clone());
        }
        out.insert(data_file.path, visibility);
    }

    Ok(out)
}

pub(crate) fn data_file_row_is_visible(
    batch: &RecordBatch,
    row: usize,
    file_path: &str,
    row_position: i64,
    existing_deletes_by_file: &ExistingDeleteVisibilityByDataFile,
) -> Result<bool, String> {
    let visibility = existing_deletes_by_file.get(file_path);
    if visibility
        .map(|state| state.deleted_positions.contains(row_position as u64))
        .unwrap_or(false)
    {
        return Ok(false);
    }
    let equality_deletes = visibility
        .map(|state| state.equality_deletes.as_slice())
        .unwrap_or(&[]);
    if crate::connector::iceberg::equality_delete::equality_delete_row_is_deleted(
        batch,
        row,
        equality_deletes,
    )? {
        return Ok(false);
    }
    Ok(true)
}

fn collect_position_deletes_from_batch(
    batch: &RecordBatch,
    where_expr: &sqlast::Expr,
    schema: &iceberg::spec::Schema,
    existing_deletes_by_file: &ExistingDeleteVisibilityByDataFile,
    by_file: &mut BTreeMap<String, Vec<i64>>,
) -> Result<(), String> {
    let file_idx = batch
        .schema()
        .index_of("_file")
        .map_err(|_| "scan batch missing `_file` column".to_string())?;
    let pos_idx = batch
        .schema()
        .index_of("_pos")
        .map_err(|_| "scan batch missing `_pos` column".to_string())?;
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
        let path = file_arr.value(i);
        if !data_file_row_is_visible(batch, i, path, pos_arr.value(i), existing_deletes_by_file)? {
            continue;
        }
        let matches = evaluate_where_at_row(where_expr, batch, i, schema)?;
        if !matches {
            continue;
        }
        by_file
            .entry(path.to_string())
            .or_default()
            .push(pos_arr.value(i));
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::fs;
    use std::sync::Arc;

    use arrow::array::{Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use arrow::record_batch::RecordBatch;
    use iceberg::spec::{Literal, NestedField, PrimitiveType, Struct, Type};
    use parquet::arrow::ArrowWriter;
    use sqlparser::ast as sqlast;

    use crate::connector::iceberg::position_delete::IcebergDeleteFileSpec;
    use crate::descriptors::THdfsFileFormat;
    use crate::fs::opendal::{OpendalRangeReaderFactory, build_fs_operator};
    use crate::types::TIcebergFileContent;

    fn temp_dir_for(name: &str) -> std::path::PathBuf {
        let mut dir = std::env::temp_dir();
        dir.push(format!(
            "novarocks_delete_flow_tests_{}_{}",
            name,
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("create tmp dir");
        dir
    }

    fn factory_for_dir(dir: &std::path::Path) -> OpendalRangeReaderFactory {
        let op = build_fs_operator(dir.to_str().expect("utf8 dir")).expect("operator");
        OpendalRangeReaderFactory::from_operator(op).expect("factory")
    }

    fn write_eq_delete_parquet(path: &std::path::Path) {
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![2]))])
            .expect("delete batch");
        let file = fs::File::create(path).expect("create delete file");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("writer");
        writer.write(&batch).expect("write delete file");
        writer.close().expect("close delete file");
    }

    fn iceberg_schema() -> iceberg::spec::Schema {
        iceberg::spec::Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Int),
                )),
                Arc::new(NestedField::required(
                    2,
                    "category",
                    Type::Primitive(PrimitiveType::String),
                )),
            ])
            .build()
            .expect("build iceberg schema")
    }

    fn delete_where_id_in_2_3() -> sqlast::Expr {
        sqlast::Expr::InList {
            expr: Box::new(sqlast::Expr::Identifier(sqlast::Ident::new("id"))),
            list: vec![
                sqlast::Expr::Value(sqlast::ValueWithSpan {
                    value: sqlast::Value::Number("2".to_string(), false),
                    span: sqlparser::tokenizer::Span::empty(),
                }),
                sqlast::Expr::Value(sqlast::ValueWithSpan {
                    value: sqlast::Value::Number("3".to_string(), false),
                    span: sqlparser::tokenizer::Span::empty(),
                }),
            ],
            negated: false,
        }
    }

    #[test]
    fn referenced_data_file_partition_insert_rejects_conflicting_duplicate_metadata() {
        let path = "/warehouse/db/t/data.parquet".to_string();
        let mut partitions = HashMap::new();
        super::insert_referenced_data_file_partition(
            &mut partitions,
            path.clone(),
            super::ReferencedDataFilePartition {
                partition_spec_id: 1,
                partition_values: Struct::from_iter([Some(Literal::int(10))]),
            },
        )
        .expect("insert first partition metadata");
        super::insert_referenced_data_file_partition(
            &mut partitions,
            path.clone(),
            super::ReferencedDataFilePartition {
                partition_spec_id: 1,
                partition_values: Struct::from_iter([Some(Literal::int(10))]),
            },
        )
        .expect("identical duplicate partition metadata");

        let err = super::insert_referenced_data_file_partition(
            &mut partitions,
            path.clone(),
            super::ReferencedDataFilePartition {
                partition_spec_id: 2,
                partition_values: Struct::from_iter([Some(Literal::int(10))]),
            },
        )
        .unwrap_err();

        assert!(err.contains(&path));
        assert!(err.contains("conflicting partition metadata"));
        assert!(err.contains("old partition spec id 1"));
        assert!(err.contains("new partition spec id 2"));
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[&path].partition_spec_id, 1);

        let mut partitions = HashMap::new();
        super::insert_referenced_data_file_partition(
            &mut partitions,
            path.clone(),
            super::ReferencedDataFilePartition {
                partition_spec_id: 1,
                partition_values: Struct::from_iter([Some(Literal::int(10))]),
            },
        )
        .expect("insert first partition metadata");
        let err = super::insert_referenced_data_file_partition(
            &mut partitions,
            path.clone(),
            super::ReferencedDataFilePartition {
                partition_spec_id: 1,
                partition_values: Struct::from_iter([Some(Literal::int(20))]),
            },
        )
        .unwrap_err();

        assert!(err.contains(&path));
        assert!(err.contains("conflicting partition metadata"));
        assert!(err.contains("old partition spec id 1"));
        assert!(err.contains("new partition spec id 1"));
    }

    #[test]
    fn position_delete_collection_skips_rows_hidden_by_equality_deletes() {
        let dir = temp_dir_for("equality_visibility");
        let delete_path = dir.join("eq-delete.parquet");
        write_eq_delete_parquet(&delete_path);
        let spec = IcebergDeleteFileSpec {
            path: delete_path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .to_string(),
            file_format: THdfsFileFormat::PARQUET,
            file_content: TIcebergFileContent::EQUALITY_DELETES,
            length: None,
            content_offset: None,
            content_size_in_bytes: None,
        };
        let equality_deletes =
            crate::connector::iceberg::equality_delete::load_equality_delete_sets(
                &[spec],
                &factory_for_dir(&dir),
            )
            .expect("load equality deletes");
        let mut equality_deletes_by_file = HashMap::new();
        equality_deletes_by_file.insert(
            "/warehouse/db/t/data.parquet".to_string(),
            super::ExistingDeleteVisibility {
                deleted_positions: roaring::RoaringTreemap::new(),
                equality_deletes,
            },
        );
        let schema = iceberg_schema();
        let batch_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("_file", DataType::Utf8, false),
            Field::new("_pos", DataType::Int64, false),
            Field::new("id", DataType::Int32, false),
            Field::new("category", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            batch_schema,
            vec![
                Arc::new(StringArray::from(vec![
                    "/warehouse/db/t/data.parquet",
                    "/warehouse/db/t/data.parquet",
                    "/warehouse/db/t/data.parquet",
                ])),
                Arc::new(Int64Array::from(vec![0, 1, 2])),
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .expect("scan batch");
        let mut by_file = BTreeMap::new();

        super::collect_position_deletes_from_batch(
            &batch,
            &delete_where_id_in_2_3(),
            &schema,
            &equality_deletes_by_file,
            &mut by_file,
        )
        .expect("collect positions");

        assert_eq!(
            by_file
                .get("/warehouse/db/t/data.parquet")
                .map(Vec::as_slice),
            Some(&[2][..])
        );
    }

    #[test]
    fn position_delete_collection_skips_rows_hidden_by_position_deletes() {
        let schema = iceberg_schema();
        let batch_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("_file", DataType::Utf8, false),
            Field::new("_pos", DataType::Int64, false),
            Field::new("id", DataType::Int32, false),
            Field::new("category", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            batch_schema,
            vec![
                Arc::new(StringArray::from(vec![
                    "/warehouse/db/t/data.parquet",
                    "/warehouse/db/t/data.parquet",
                    "/warehouse/db/t/data.parquet",
                ])),
                Arc::new(Int64Array::from(vec![0, 1, 2])),
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .expect("scan batch");
        let mut deleted_positions = roaring::RoaringTreemap::new();
        deleted_positions.insert(1);
        let mut visibility_by_file = HashMap::new();
        visibility_by_file.insert(
            "/warehouse/db/t/data.parquet".to_string(),
            super::ExistingDeleteVisibility {
                deleted_positions,
                equality_deletes: Vec::new(),
            },
        );
        let mut by_file = BTreeMap::new();

        super::collect_position_deletes_from_batch(
            &batch,
            &delete_where_id_in_2_3(),
            &schema,
            &visibility_by_file,
            &mut by_file,
        )
        .expect("collect positions");

        assert_eq!(
            by_file
                .get("/warehouse/db/t/data.parquet")
                .map(Vec::as_slice),
            Some(&[2][..])
        );
    }
}
