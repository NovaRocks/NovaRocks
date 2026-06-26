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
//! Distributed position-delete path:
//! 1. Resolve + load the iceberg table.
//! 2. Run pre-lowering validators and choose the Iceberg write mode.
//! 3. Translate the sqlparser WHERE into an iceberg [`Predicate`]. Phase 1
//!    supports comparison operators (`= != < <= > >=`), `IN (...)`, and
//!    `AND` / `OR` against primitive columns (int / long / string / bool / timestamp).
//!    Other expressions are rejected with an explicit error.
//! 4. Rewrite DELETE into a SELECT of `_file`, `_pos`, and partition source
//!    columns, then run it through the distributed `ICEBERG_DELETE_SINK`.
//! 5. Route the sink output through the Iceberg write transaction runner,
//!    which commits the generated position-delete files and drives
//!    finalization lifecycle.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow::array::{
    Array, BooleanArray, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use arrow::datatypes::DataType;
use chrono::NaiveDateTime;
use iceberg::expr::{Predicate, Reference};
use iceberg::spec::{Datum, PrimitiveType, Type};
use sqlparser::ast as sqlast;

use crate::connector::iceberg::catalog::registry::{self, block_on_iceberg, build_iceberg_catalog};
use crate::connector::iceberg::commit::{
    CommitOpKind, CommitOutcome, CommitServiceError, IcebergCommitCollector,
    IcebergSqlDeleteStrategy, classify_sql_delete_strategy,
};
use crate::engine::backend_resolver::{TargetBackend, resolve_existing_table_target};
use crate::engine::write_transaction::{
    IcebergWriteCommitExecutor, IcebergWriteCommitPolicy, IcebergWriteSource,
    IcebergWriteTransactionExecutor, IcebergWriteTransactionRunner, IcebergWriteTransactionSpec,
    IcebergWriteValidationPolicy, write_commit_has_files,
};
use crate::engine::{StandaloneState, StatementResult};
use crate::meta::repository::iceberg_operation::{IcebergOperationKind, IcebergOperationTarget};
use crate::runtime::coordinator::CoordinatedQueryResult;
use crate::runtime::write_coordinator::WriteCommitInput;
use crate::sql::analyzer::iceberg_ref::{IcebergRefSuffix, split_ref_suffix};
use crate::sql::catalog::ColumnDef;
use crate::sql::codegen::iceberg_write_sink::{IcebergWriteSinkMode, IcebergWriteSinkSpec};
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
    if target.backend_name == "starrocks" {
        return execute_starrocks_delete_statement(state, &target, stmt, &target_ref);
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
    let catalog = build_iceberg_catalog(&entry)?;
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
    let delete_strategy = classify_sql_delete_strategy(&table)?;
    // 4. Validate WHERE → iceberg::Predicate to surface unsupported clauses
    //    early. The distributed SELECT planner owns scan pruning and existing
    //    delete visibility from this point onward.
    let schema = table.metadata().current_schema();
    let _predicate = translate_where(&stmt.where_clause, schema.as_ref())?;

    let metadata = table.metadata();
    let base_snapshot_id = if target_ref != "main" {
        resolve_branch_head_snapshot_id(metadata, &target_ref)?
    } else {
        metadata.current_snapshot().map(|s| s.snapshot_id())
    };

    if matches!(delete_strategy, IcebergSqlDeleteStrategy::DeletionVectors) {
        run_delete_dv_write_transaction(
            state,
            &target,
            catalog,
            table,
            entry,
            base_snapshot_id,
            &target_ref,
            &stmt.where_clause,
        )?;
        return Ok(StatementResult::Ok);
    }

    let resolved = {
        let registry = state.connectors.read().expect("connector registry read");
        let backend = registry.catalog_backend("iceberg")?;
        backend.load_table(&target.catalog, &target.namespace, &target.table)?
    };
    let sink_spec = crate::engine::iceberg_writer::build_position_delete_sink_spec(
        &target, &resolved, &table, &entry,
    )?;
    let delete_query = build_delete_position_sink_query(
        &target,
        &stmt.where_clause,
        &sink_spec.target_columns,
        &target_ref,
    )?;

    let staging_dir = format!(
        "{}/data/_staging/{}",
        metadata.location(),
        uuid::Uuid::new_v4()
    );
    let collector = Arc::new(
        IcebergCommitCollector::new(
            CommitOpKind::RowDelta,
            table_ident,
            base_snapshot_id,
            metadata.last_sequence_number(),
            metadata.current_schema().clone(),
            metadata.default_partition_spec().clone(),
            staging_dir,
            crate::common::types::UniqueId { hi: 0, lo: 0 },
        )
        .with_table_metadata(metadata.clone()),
    );
    run_delete_write_transaction(
        state,
        &target,
        catalog,
        table,
        collector,
        entry,
        base_snapshot_id,
        &target_ref,
        delete_query,
        sink_spec,
    )?;

    Ok(StatementResult::Ok)
}

struct DistributedDeleteWriteExecutor {
    state: Arc<StandaloneState>,
    target: TargetBackend,
    delete_query: sqlparser::ast::Query,
    sink_spec: IcebergWriteSinkSpec,
    commit_executor: IcebergWriteCommitExecutor,
}

impl IcebergWriteTransactionExecutor for DistributedDeleteWriteExecutor {
    fn run_coordinated_write(
        &self,
        _spec: &IcebergWriteTransactionSpec,
    ) -> Result<CoordinatedQueryResult, String> {
        let mut result = crate::engine::execute_query_as_iceberg_write(
            &self.state,
            Some(&self.target.catalog),
            &self.target.namespace,
            &self.delete_query,
            self.sink_spec.clone(),
            None,
            None,
        )?;
        if result
            .write_commit
            .as_ref()
            .is_some_and(|commit| !write_commit_has_files(commit))
        {
            result.write_commit = None;
        }
        Ok(result)
    }

    fn commit(
        &self,
        _spec: &IcebergWriteTransactionSpec,
        write_commit: &WriteCommitInput,
    ) -> Result<CommitOutcome, CommitServiceError> {
        self.commit_executor.commit_write_input(write_commit)
    }

    fn finalize(&self, _spec: &IcebergWriteTransactionSpec) -> Result<(), String> {
        self.commit_executor.finalize()
    }
}

struct DistributedDvDeleteWriteExecutor {
    state: Arc<StandaloneState>,
    target: TargetBackend,
    delete_query: sqlparser::ast::Query,
    sink_spec: IcebergWriteSinkSpec,
    commit_executor: IcebergWriteCommitExecutor,
}

impl IcebergWriteTransactionExecutor for DistributedDvDeleteWriteExecutor {
    fn run_coordinated_write(
        &self,
        _spec: &IcebergWriteTransactionSpec,
    ) -> Result<CoordinatedQueryResult, String> {
        let mut result = crate::engine::execute_query_as_iceberg_write(
            &self.state,
            Some(&self.target.catalog),
            &self.target.namespace,
            &self.delete_query,
            self.sink_spec.clone(),
            None,
            Some(crate::engine::iceberg_write_shuffle_by_output_index(0)),
        )?;
        if result
            .write_commit
            .as_ref()
            .is_some_and(|commit| !write_commit_has_files(commit))
        {
            result.write_commit = None;
        }
        Ok(result)
    }

    fn commit(
        &self,
        _spec: &IcebergWriteTransactionSpec,
        write_commit: &WriteCommitInput,
    ) -> Result<CommitOutcome, CommitServiceError> {
        self.commit_executor.commit_write_input(write_commit)
    }

    fn finalize(&self, _spec: &IcebergWriteTransactionSpec) -> Result<(), String> {
        self.commit_executor.finalize()
    }
}

#[allow(clippy::too_many_arguments)]
fn run_delete_dv_write_transaction(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    catalog: Arc<dyn iceberg::Catalog>,
    table: iceberg::table::Table,
    entry: crate::connector::iceberg::catalog::IcebergCatalogEntry,
    base_snapshot_id: Option<i64>,
    target_ref: &str,
    where_clause: &sqlast::Expr,
) -> Result<(), String> {
    let resolved = {
        let registry = state.connectors.read().expect("connector registry read");
        let backend = registry.catalog_backend("iceberg")?;
        backend.load_table(&target.catalog, &target.namespace, &target.table)?
    };
    let mut sink_spec = crate::engine::iceberg_writer::build_position_delete_sink_spec(
        target, &resolved, &table, &entry,
    )?;
    sink_spec.mode = IcebergWriteSinkMode::DeletionVectors;
    sink_spec.set_planned_snapshot_id(base_snapshot_id)?;
    let delete_query = build_delete_position_sink_query(
        target,
        where_clause,
        &sink_spec.target_columns,
        target_ref,
    )?;

    let metadata = table.metadata();
    let table_ident = iceberg::TableIdent::new(
        iceberg::NamespaceIdent::new(target.namespace.clone()),
        target.table.clone(),
    );
    let staging_dir = format!(
        "{}/data/_staging/{}",
        metadata.location(),
        uuid::Uuid::new_v4()
    );
    let collector = Arc::new(
        IcebergCommitCollector::new(
            CommitOpKind::RowDeltaDvFromFiles,
            table_ident,
            base_snapshot_id,
            metadata.last_sequence_number(),
            metadata.current_schema().clone(),
            metadata.default_partition_spec().clone(),
            staging_dir,
            crate::common::types::UniqueId { hi: 0, lo: 0 },
        )
        .with_table_metadata(metadata.clone()),
    );
    let abort_cleanup =
        crate::engine::iceberg_writer::build_abort_cleanup_for_catalog_entry(&entry)?;
    let commit_executor = IcebergWriteCommitExecutor {
        state: Arc::clone(state),
        target: target.clone(),
        catalog,
        table: table.clone(),
        collector,
        fs: abort_cleanup.fs,
        cleanup_path_mapper: abort_cleanup.path_mapper,
        cow_update_rewrite: None,
        target_ref: target_ref.to_string(),
        snapshot_properties: BTreeMap::new(),
    };
    let spec = IcebergWriteTransactionSpec {
        target: IcebergOperationTarget {
            catalog: target.catalog.clone(),
            namespace: target.namespace.clone(),
            table: target.table.clone(),
            ref_name: (target_ref != "main").then(|| target_ref.to_string()),
        },
        operation_kind: IcebergOperationKind::RowDelta,
        attempt_id: format!(
            "{}.{}.{}:delete-dv:{}",
            target.catalog,
            target.namespace,
            target.table,
            uuid::Uuid::new_v4()
        ),
        commit: IcebergWriteCommitPolicy {
            commit_op_kind: CommitOpKind::RowDeltaDvFromFiles,
            base_snapshot_id,
            base_snapshot_map: BTreeMap::new(),
            target_ref: target_ref.to_string(),
            snapshot_properties: BTreeMap::new(),
        },
        validation: IcebergWriteValidationPolicy {
            require_v3_for_branch: target_ref != "main",
        },
        source: IcebergWriteSource::CoordinatedPlan,
    };
    let executor = DistributedDvDeleteWriteExecutor {
        state: Arc::clone(state),
        target: target.clone(),
        delete_query,
        sink_spec,
        commit_executor,
    };
    let runner = IcebergWriteTransactionRunner::new(Arc::clone(state), &executor);
    let _outcome = runner.run(spec)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_delete_write_transaction(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    catalog: Arc<dyn iceberg::Catalog>,
    table: iceberg::table::Table,
    collector: Arc<IcebergCommitCollector>,
    entry: crate::connector::iceberg::catalog::IcebergCatalogEntry,
    base_snapshot_id: Option<i64>,
    target_ref: &str,
    delete_query: sqlparser::ast::Query,
    sink_spec: IcebergWriteSinkSpec,
) -> Result<(), String> {
    let abort_cleanup =
        crate::engine::iceberg_writer::build_abort_cleanup_for_catalog_entry(&entry)?;
    let commit_executor = IcebergWriteCommitExecutor {
        state: Arc::clone(state),
        target: target.clone(),
        catalog,
        table: table.clone(),
        collector: Arc::clone(&collector),
        fs: abort_cleanup.fs,
        cleanup_path_mapper: abort_cleanup.path_mapper,
        cow_update_rewrite: None,
        target_ref: target_ref.to_string(),
        snapshot_properties: BTreeMap::new(),
    };
    let spec = IcebergWriteTransactionSpec {
        target: IcebergOperationTarget {
            catalog: target.catalog.clone(),
            namespace: target.namespace.clone(),
            table: target.table.clone(),
            ref_name: (target_ref != "main").then(|| target_ref.to_string()),
        },
        operation_kind: IcebergOperationKind::RowDelta,
        attempt_id: format!(
            "{}.{}.{}:delete:{}",
            target.catalog,
            target.namespace,
            target.table,
            uuid::Uuid::new_v4()
        ),
        commit: IcebergWriteCommitPolicy {
            commit_op_kind: CommitOpKind::RowDelta,
            base_snapshot_id,
            base_snapshot_map: BTreeMap::new(),
            target_ref: target_ref.to_string(),
            snapshot_properties: BTreeMap::new(),
        },
        validation: IcebergWriteValidationPolicy {
            require_v3_for_branch: target_ref != "main",
        },
        source: IcebergWriteSource::CoordinatedPlan,
    };
    let executor = DistributedDeleteWriteExecutor {
        state: Arc::clone(state),
        target: target.clone(),
        delete_query,
        sink_spec,
        commit_executor,
    };
    let runner = IcebergWriteTransactionRunner::new(Arc::clone(state), &executor);
    let _outcome = runner.run(spec)?;
    Ok(())
}

fn build_delete_position_sink_query(
    target: &TargetBackend,
    where_clause: &sqlast::Expr,
    sink_columns: &[ColumnDef],
    target_ref: &str,
) -> Result<sqlparser::ast::Query, String> {
    let projection = sink_columns
        .iter()
        .map(|column| sql_identifier(&column.name))
        .collect::<Vec<_>>()
        .join(", ");
    let version_clause = if target_ref == "main" {
        String::new()
    } else {
        format!(" FOR VERSION AS OF {}", sql_string_literal(target_ref))
    };
    let sql = format!(
        "SELECT {projection} FROM {}{version_clause} WHERE {where_clause}",
        qualify_iceberg_table(target)
    );
    parse_generated_query(&sql, "DELETE position-delete rewrite")
}

fn parse_generated_query(sql: &str, context: &str) -> Result<sqlparser::ast::Query, String> {
    match crate::sql::parser::parse_sql_raw(sql)? {
        sqlparser::ast::Statement::Query(query) => Ok(*query),
        other => Err(format!("{context}: generated non-query statement: {other}")),
    }
}

fn qualify_iceberg_table(target: &TargetBackend) -> String {
    format!(
        "{}.{}.{}",
        sql_identifier(&target.catalog),
        sql_identifier(&target.namespace),
        sql_identifier(&target.table)
    )
}

fn sql_identifier(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
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
                // Detect scalar_fn(col) <op> literal pattern first.
                // Function-call predicates cannot be pushed into Iceberg column
                // statistics (the function obscures the underlying column value),
                // so we return AlwaysTrue here to scan all files and leave
                // correctness to the per-row evaluator in evaluate_where_at_row.
                if extract_scalar_fn_comparison(left, right).is_some()
                    || extract_variant_get_comparison(left, right).is_some()
                {
                    return Ok(Predicate::AlwaysTrue);
                }
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

fn execute_starrocks_delete_statement(
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

    let table_info = resolve_starrocks_table_info(state, target)?;
    if table_info.is_materialized_view {
        return Err(format!(
            "The data of '{}' cannot be deleted because it is a materialized view; \
             the data of materialized view must be consistent with the base table.",
            target.table
        ));
    }

    if table_info.keys_type == "PRIMARY_KEYS" {
        return execute_starrocks_pk_delete(state, target, stmt, &table_info);
    }

    let keys_type =
        crate::engine::delete_predicate_translate::KeysType::from_meta_str(&table_info.keys_type)
            .ok_or_else(|| {
            format!(
                "DELETE not supported for StarRocks table keys_type `{}`",
                table_info.keys_type
            )
        })?;
    execute_starrocks_predicate_delete(state, target, stmt, &table_info, keys_type)
}

struct StarRocksTableInfo {
    keys_type: String,
    is_materialized_view: bool,
    columns: Vec<crate::engine::catalog::ColumnDef>,
    key_columns: Vec<String>,
}

fn resolve_starrocks_table_info(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
) -> Result<StarRocksTableInfo, String> {
    use crate::connector::starrocks::table::catalog::arrow_type_from_tablet_column;
    use crate::engine::catalog::ColumnDef;

    let starrocks = state
        .starrocks_table
        .read()
        .expect("standalone StarRocks table read lock");
    let runtime = starrocks.table(&target.namespace, &target.table)?;

    let key_columns: Vec<String> = runtime
        .columns
        .iter()
        .filter(|column| column.is_key)
        .map(|column| column.column_name.clone())
        .collect();

    let mut columns = Vec::with_capacity(runtime.columns.len());
    for column in &runtime.columns {
        if !column.visible {
            continue;
        }
        let schema_column = runtime
            .tablet_schema
            .column
            .iter()
            .find(|sc| {
                sc.name
                    .as_deref()
                    .is_some_and(|name: &str| name.eq_ignore_ascii_case(&column.column_name))
            })
            .ok_or_else(|| {
                format!(
                    "StarRocks table {}.{} missing tablet schema column `{}`",
                    runtime.database_name, runtime.table.name, column.column_name
                )
            })?;
        columns.push(ColumnDef {
            name: column.column_name.clone(),
            data_type: arrow_type_from_tablet_column(schema_column)?,
            nullable: column.nullable,
            write_default: None,
            logical_type: None,
        });
    }

    Ok(StarRocksTableInfo {
        keys_type: runtime.table.keys_type.clone(),
        is_materialized_view: matches!(
            runtime.table.kind,
            crate::connector::starrocks::table::model::StarRocksTableKind::MaterializedView
        ),
        columns,
        key_columns,
    })
}

fn execute_starrocks_predicate_delete(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    stmt: &DeleteStmt,
    table_info: &StarRocksTableInfo,
    keys_type: crate::engine::delete_predicate_translate::KeysType,
) -> Result<StatementResult, String> {
    use crate::connector::starrocks::lake::delete_predicate_proto::build_delete_predicate_pb;
    use crate::engine::delete_predicate_translate::translate_to_delete_predicate;

    let terms = translate_to_delete_predicate(
        &stmt.where_clause,
        &table_info.columns,
        &table_info.key_columns,
        keys_type,
    )?;

    // The version field is informational on the wire; backends derive the
    // actual rowset version from the txn publish. StarRocks emits the next
    // partition version here for parity, so do the same.
    let predicate_version = 0_i32;
    let predicate_pb = build_delete_predicate_pb(&terms, predicate_version);

    crate::connector::starrocks::table::txn::delete_starrocks_table_by_predicate(
        state,
        &target.namespace,
        &target.table,
        predicate_pb,
    )?;
    Ok(StatementResult::Ok)
}

/// Rewrite `DELETE FROM pk_t WHERE cond` into `SELECT <pk_cols> FROM pk_t
/// WHERE cond`, run it through the standalone pipeline, then route the
/// resulting chunks through the StarRocks table sink with `__op = 1` appended.
/// The sink's `parse_op_batch` recognizes the control column and emits a
/// `.del` file per tablet; the PK-applier consumes it at publish time.
///
/// WHERE accepts any plannable form (non-key columns, functions, joins,
/// subqueries) — same surface as StarRocks PK DELETE, no DeleteAnalyzer
/// restrictions.
fn execute_starrocks_pk_delete(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    stmt: &DeleteStmt,
    table_info: &StarRocksTableInfo,
) -> Result<StatementResult, String> {
    if table_info.key_columns.is_empty() {
        return Err(format!(
            "StarRocks table PRIMARY KEY table '{}' has no key columns",
            target.table
        ));
    }
    let pk_list = table_info.key_columns.join(", ");
    let qualified = qualify_starrocks_table(target);
    let where_sql = stmt.where_clause.to_string();
    let select_sql = format!("SELECT {pk_list} FROM {qualified} WHERE {where_sql}");

    let parsed = crate::sql::parser::parse_sql_raw(&select_sql)?;
    let sqlast::Statement::Query(query) = parsed else {
        return Err(format!(
            "internal: StarRocks PK DELETE rewrite did not parse as SELECT: {select_sql}"
        ));
    };

    // Run the SELECT through the standalone pipeline. Clone-then-release the
    // catalog read lock the same way insert_flow does, so the pipeline cannot
    // starve concurrent writers.
    let catalog_snapshot = state
        .catalog
        .read()
        .expect("standalone catalog read lock")
        .clone();
    let connectors_snapshot = state
        .connectors
        .read()
        .expect("standalone connector registry read lock")
        .clone();
    let query_result = crate::engine::execute_query(
        query.as_ref(),
        &catalog_snapshot,
        &connectors_snapshot,
        &target.namespace,
        state.exchange_port,
        None,
    )?;

    crate::connector::starrocks::table::txn::delete_starrocks_table_pk_rows(
        state,
        &target.namespace,
        &target.table,
        &query_result.chunks,
    )?;
    Ok(StatementResult::Ok)
}

fn qualify_starrocks_table(target: &crate::engine::backend_resolver::TargetBackend) -> String {
    if target.namespace.is_empty() {
        target.table.clone()
    } else {
        format!("{}.{}", target.namespace, target.table)
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

/// Attempt to detect a `scalar_fn(col_ref) <op> literal` pattern.
///
/// Returns `Some((fn_name, col_name, literal_expr, flipped))` when:
///   - One side is a single-argument function call whose sole argument is a
///     column reference.
///   - The other side is a value literal.
///   - The function name is in the supported deterministic string-function set.
///
/// `flipped = true` means the original was `literal <op> fn(col)`.
fn extract_scalar_fn_comparison<'a>(
    left: &'a sqlast::Expr,
    right: &'a sqlast::Expr,
) -> Option<(String, String, &'a sqlast::Expr, bool)> {
    if let Some((fn_name, col_name)) = expr_as_supported_scalar_fn_on_col(left) {
        if is_literal_expr(right) {
            return Some((fn_name, col_name, right, false));
        }
    }
    if let Some((fn_name, col_name)) = expr_as_supported_scalar_fn_on_col(right) {
        if is_literal_expr(left) {
            return Some((fn_name, col_name, left, true));
        }
    }
    None
}

/// Detect a `variant_get(col, 'path', 'type') <op> literal` predicate.
///
/// The generated DELETE rewrite runs the original WHERE clause through the
/// normal query pipeline, where `variant_get` is evaluated with full analyzer
/// and execution support. The Iceberg predicate translator only needs to
/// accept this shape and avoid unsafe file pruning, so callers treat it as
/// `AlwaysTrue`.
fn extract_variant_get_comparison<'a>(
    left: &'a sqlast::Expr,
    right: &'a sqlast::Expr,
) -> Option<(String, &'a sqlast::Expr, bool)> {
    if let Some(col_name) = expr_as_variant_get_on_col(left) {
        if is_literal_expr(right) {
            return Some((col_name, right, false));
        }
    }
    if let Some(col_name) = expr_as_variant_get_on_col(right) {
        if is_literal_expr(left) {
            return Some((col_name, left, true));
        }
    }
    None
}

fn expr_as_variant_get_on_col(expr: &sqlast::Expr) -> Option<String> {
    let sqlast::Expr::Function(func) = expr else {
        return None;
    };
    let name = func.name.to_string().to_ascii_lowercase();
    if !matches!(name.as_str(), "variant_get" | "try_variant_get") {
        return None;
    }
    let args = function_expr_args(func)?;
    if args.len() != 3 {
        return None;
    }
    let col_name = expr_to_column_name(args[0]).ok()?;
    extract_string_literal(args[1])?;
    extract_string_literal(args[2])?;
    Some(col_name)
}

/// Return `(fn_name_lowercase, col_name_lowercase)` when `expr` is a
/// single-argument function call over a bare column reference and the function
/// name is in the deterministic set we support for row-level evaluation.
fn expr_as_supported_scalar_fn_on_col(expr: &sqlast::Expr) -> Option<(String, String)> {
    let sqlast::Expr::Function(func) = expr else {
        return None;
    };
    let name = func.name.to_string().to_ascii_lowercase();
    if !is_supported_scalar_fn(&name) {
        return None;
    }
    let args = function_expr_args(func)?;
    if args.len() != 1 {
        return None;
    }
    let col_name = expr_to_column_name(args[0]).ok()?;
    Some((name, col_name))
}

fn function_expr_args(func: &sqlast::Function) -> Option<Vec<&sqlast::Expr>> {
    match &func.args {
        sqlast::FunctionArguments::List(list) => list
            .args
            .iter()
            .map(|arg| {
                if let sqlast::FunctionArg::Unnamed(sqlast::FunctionArgExpr::Expr(e)) = arg {
                    Some(e)
                } else {
                    None
                }
            })
            .collect(),
        _ => None,
    }
}

/// The set of deterministic, single-argument scalar functions that the phase-1
/// DELETE evaluator can apply per-row.  These functions cannot be pushed into
/// Iceberg column statistics (the predicate is treated as AlwaysTrue for file
/// skipping), but they are applied during the row-level filter pass.
fn is_supported_scalar_fn(name: &str) -> bool {
    matches!(
        name,
        "lower" | "upper" | "trim" | "ltrim" | "rtrim" | "length" | "char_length"
    )
}

/// Returns `true` when `expr` is a value literal (or a nested/negated literal)
/// that `literal_to_datum` can parse.
fn is_literal_expr(expr: &sqlast::Expr) -> bool {
    match expr {
        sqlast::Expr::Value(_) => true,
        sqlast::Expr::UnaryOp {
            op: sqlast::UnaryOperator::Minus,
            expr: inner,
        } => matches!(inner.as_ref(), sqlast::Expr::Value(_)),
        sqlast::Expr::Nested(inner) => is_literal_expr(inner),
        _ => false,
    }
}

/// Apply a supported scalar function to a `CellValue` and return the resulting
/// `CellValue`.  Returns an error for unsupported function / type combinations.
fn apply_scalar_fn_to_cell(fn_name: &str, cell: CellValue) -> Result<CellValue, String> {
    match fn_name {
        "lower" => match cell {
            CellValue::String(s) => Ok(CellValue::String(s.to_lowercase())),
            other => Err(format!("LOWER() requires a string column, got {other:?}")),
        },
        "upper" => match cell {
            CellValue::String(s) => Ok(CellValue::String(s.to_uppercase())),
            other => Err(format!("UPPER() requires a string column, got {other:?}")),
        },
        "trim" => match cell {
            CellValue::String(s) => Ok(CellValue::String(s.trim().to_string())),
            other => Err(format!("TRIM() requires a string column, got {other:?}")),
        },
        "ltrim" => match cell {
            CellValue::String(s) => Ok(CellValue::String(s.trim_start().to_string())),
            other => Err(format!("LTRIM() requires a string column, got {other:?}")),
        },
        "rtrim" => match cell {
            CellValue::String(s) => Ok(CellValue::String(s.trim_end().to_string())),
            other => Err(format!("RTRIM() requires a string column, got {other:?}")),
        },
        "length" | "char_length" => match cell {
            CellValue::String(s) => Ok(CellValue::Long(s.chars().count() as i64)),
            other => Err(format!("LENGTH() requires a string column, got {other:?}")),
        },
        other => Err(format!(
            "phase 1 DELETE WHERE: unsupported scalar function `{other}`"
        )),
    }
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
        PrimitiveType::Timestamp => {
            // SQL DATETIME literals arrive as 'YYYY-MM-DD HH:MM:SS[.ffffff]'.
            // Try sub-second precision first, then whole-second form.
            let micros = NaiveDateTime::parse_from_str(lit_str, "%Y-%m-%d %H:%M:%S%.f")
                .or_else(|_| NaiveDateTime::parse_from_str(lit_str, "%Y-%m-%d %H:%M:%S"))
                .map(|dt| dt.and_utc().timestamp_micros())
                .map_err(|e| {
                    format!("parse DATETIME literal `{lit_str}` for column `{column_name}`: {e}")
                })?;
            Ok(Datum::timestamp_micros(micros))
        }
        PrimitiveType::Timestamptz => {
            let micros = NaiveDateTime::parse_from_str(lit_str, "%Y-%m-%d %H:%M:%S%.f")
                .or_else(|_| NaiveDateTime::parse_from_str(lit_str, "%Y-%m-%d %H:%M:%S"))
                .map(|dt| dt.and_utc().timestamp_micros())
                .map_err(|e| {
                    format!("parse TIMESTAMPTZ literal `{lit_str}` for column `{column_name}`: {e}")
                })?;
            Ok(Datum::timestamptz_micros(micros))
        }
        other => Err(format!(
            "phase 1 DELETE WHERE primitive type {other:?} not yet supported (column `{column_name}`)"
        )),
    }
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
            included_positions: None,
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
                included_positions: None,
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
                        crate::thrift::descriptors::THdfsFileFormat::PARQUET
                    }
                    crate::sql::catalog::IcebergDeleteFileFormat::Puffin => {
                        crate::thrift::descriptors::THdfsFileFormat::PARQUET
                    }
                };
                let file_content = match original.file_content {
                    crate::sql::catalog::IcebergDeleteFileContent::Position => {
                        crate::thrift::types::TIcebergFileContent::POSITION_DELETES
                    }
                    crate::sql::catalog::IcebergDeleteFileContent::Equality => {
                        crate::thrift::types::TIcebergFileContent::EQUALITY_DELETES
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
                // Check for scalar_fn(col) <op> literal first.
                if let Some((fn_name, col_name, value_expr, flipped)) =
                    extract_scalar_fn_comparison(left, right)
                {
                    let raw_cell = column_value_at_row(&col_name, batch, row, schema)?;
                    let cell = match raw_cell {
                        None => return Ok(false),
                        Some(v) => apply_scalar_fn_to_cell(&fn_name, v)?,
                    };
                    // The datum must match the *result* type of the function (e.g.
                    // LOWER returns STRING, LENGTH returns LONG).  Build a synthetic
                    // string-typed schema using the transformed cell type so
                    // literal_to_datum can parse the literal correctly.
                    let cmp =
                        compare_cell_to_scalar_fn_literal(&cell, value_expr, &col_name, schema)?;
                    return Ok(match (op, flipped) {
                        (sqlast::BinaryOperator::Eq, _) => cmp == std::cmp::Ordering::Equal,
                        (sqlast::BinaryOperator::NotEq, _) => cmp != std::cmp::Ordering::Equal,
                        (sqlast::BinaryOperator::Lt, false)
                        | (sqlast::BinaryOperator::Gt, true) => cmp == std::cmp::Ordering::Less,
                        (sqlast::BinaryOperator::LtEq, false)
                        | (sqlast::BinaryOperator::GtEq, true) => {
                            cmp != std::cmp::Ordering::Greater
                        }
                        (sqlast::BinaryOperator::Gt, false)
                        | (sqlast::BinaryOperator::Lt, true) => cmp == std::cmp::Ordering::Greater,
                        (sqlast::BinaryOperator::GtEq, false)
                        | (sqlast::BinaryOperator::LtEq, true) => cmp != std::cmp::Ordering::Less,
                        _ => unreachable!("unsupported binary operator already rejected upstream"),
                    });
                }
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
    /// Microseconds since Unix epoch (matches Iceberg `Timestamp` and `Timestamptz`).
    Timestamp(i64),
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
        PrimitiveType::Timestamp | PrimitiveType::Timestamptz => {
            let arr = column
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| format!("column `{col_name}` is not TimestampMicrosecond"))?;
            CellValue::Timestamp(arr.value(row))
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
        // Iceberg Timestamp / Timestamptz both store microseconds-since-epoch as PrimitiveLiteral::Long.
        (CellValue::Timestamp(c), PrimitiveLiteral::Long(d)) => Ok(c.cmp(d)),
        (cell, lit) => Err(format!(
            "phase 1 DELETE WHERE evaluator: column `{col_name}` and literal types disagree (cell={cell:?}, lit={lit:?})"
        )),
    }
}

/// Compare a transformed `CellValue` (output of a scalar function) directly
/// against a SQL literal expression.  The comparison is done at the Rust level
/// without going through an Iceberg `Datum`, because the scalar-function output
/// type may differ from the underlying column type (e.g. LENGTH returns Long but
/// the column is String).
fn compare_cell_to_scalar_fn_literal(
    cell: &CellValue,
    literal_expr: &sqlast::Expr,
    col_name: &str,
    schema: &iceberg::spec::Schema,
) -> Result<std::cmp::Ordering, String> {
    match cell {
        CellValue::String(c) => {
            let s = extract_string_literal(literal_expr).ok_or_else(|| {
                format!(
                    "phase 1 DELETE WHERE: scalar function on column `{col_name}` returned STRING; \
                     expected a string literal on the other side of the comparison"
                )
            })?;
            Ok(c.as_str().cmp(s))
        }
        CellValue::Long(c) => {
            let n = extract_integer_literal(literal_expr).ok_or_else(|| {
                format!(
                    "phase 1 DELETE WHERE: scalar function on column `{col_name}` returned LONG; \
                     expected an integer literal on the other side of the comparison"
                )
            })?;
            Ok(c.cmp(&n))
        }
        // For Int/Bool/Timestamp results fall back to building a datum via the
        // underlying column type (these do not arise from the currently supported
        // scalar functions but are handled for completeness).
        _ => {
            let datum = literal_to_datum(literal_expr, schema, col_name)?;
            compare_cell_to_datum(cell, &datum, col_name)
        }
    }
}

/// Extract the string value from a SQL literal expression (`'...'` or `"..."`).
fn extract_string_literal(expr: &sqlast::Expr) -> Option<&str> {
    match expr {
        sqlast::Expr::Value(sqlast::ValueWithSpan { value, .. }) => match value {
            sqlast::Value::SingleQuotedString(s) | sqlast::Value::DoubleQuotedString(s) => {
                Some(s.as_str())
            }
            _ => None,
        },
        sqlast::Expr::Nested(inner) => extract_string_literal(inner),
        _ => None,
    }
}

/// Extract the integer value from a SQL literal expression (`123` or `-123`).
fn extract_integer_literal(expr: &sqlast::Expr) -> Option<i64> {
    match expr {
        sqlast::Expr::Value(sqlast::ValueWithSpan {
            value: sqlast::Value::Number(s, _),
            ..
        }) => s.parse::<i64>().ok(),
        sqlast::Expr::UnaryOp {
            op: sqlast::UnaryOperator::Minus,
            expr: inner,
        } => match inner.as_ref() {
            sqlast::Expr::Value(sqlast::ValueWithSpan {
                value: sqlast::Value::Number(s, _),
                ..
            }) => s.parse::<i64>().ok().map(|n| -n),
            _ => None,
        },
        sqlast::Expr::Nested(inner) => extract_integer_literal(inner),
        _ => None,
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
    use crate::fs::opendal::{OpendalRangeReaderFactory, build_fs_operator};
    use crate::thrift::descriptors::THdfsFileFormat;
    use crate::thrift::types::TIcebergFileContent;

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

    fn iceberg_schema_with_variant() -> iceberg::spec::Schema {
        iceberg::spec::Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Int),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "v",
                    Type::Primitive(PrimitiveType::Variant),
                )),
            ])
            .build()
            .expect("build iceberg schema with variant")
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

    fn column(name: &str, data_type: DataType) -> crate::sql::catalog::ColumnDef {
        crate::sql::catalog::ColumnDef {
            name: name.to_string(),
            data_type,
            nullable: false,
            write_default: None,
            logical_type: None,
        }
    }

    fn where_expr(sql: &str) -> sqlast::Expr {
        let statement = crate::sql::parser::parse_sql_raw(sql).expect("parse query");
        let sqlast::Statement::Query(query) = statement else {
            panic!("expected query");
        };
        let sqlast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected select");
        };
        select.selection.clone().expect("where clause")
    }

    #[test]
    fn delete_translate_accepts_variant_get_predicate_for_pipeline_filtering() {
        let where_clause =
            where_expr("SELECT 1 FROM orders WHERE try_variant_get(v, '$.a', 'bigint') = 2");
        super::translate_where(&where_clause, &iceberg_schema_with_variant())
            .expect("variant_get predicate should be delegated to the query pipeline");
    }

    #[test]
    fn delete_position_sink_query_projects_row_identity_and_partition_sources() {
        let target = crate::engine::backend_resolver::TargetBackend {
            backend_name: "iceberg",
            catalog: "ice".to_string(),
            namespace: "db".to_string(),
            table: "orders".to_string(),
        };
        let sink_columns = vec![
            column(
                crate::connector::iceberg::catalog::backend::ICEBERG_ROW_IDENTITY_FILE_COLUMN,
                DataType::Utf8,
            ),
            column(
                crate::connector::iceberg::catalog::backend::ICEBERG_ROW_IDENTITY_POS_COLUMN,
                DataType::Int64,
            ),
            column("region", DataType::Utf8),
        ];
        let where_clause = where_expr("SELECT 1 FROM orders WHERE region = 'east' AND amount = 10");

        let query =
            super::build_delete_position_sink_query(&target, &where_clause, &sink_columns, "main")
                .expect("rewrite query");
        let rendered = query.to_string();

        assert!(rendered.contains("`_file`"));
        assert!(rendered.contains("`_pos`"));
        assert!(rendered.contains("`region`"));
        assert!(rendered.contains("FROM `ice`.`db`.`orders`"));
        assert!(!rendered.contains("FOR VERSION AS OF"));
    }

    #[test]
    fn delete_position_sink_query_pins_branch_read_snapshot() {
        let target = crate::engine::backend_resolver::TargetBackend {
            backend_name: "iceberg",
            catalog: "ice".to_string(),
            namespace: "db".to_string(),
            table: "orders".to_string(),
        };
        let sink_columns = vec![
            column(
                crate::connector::iceberg::catalog::backend::ICEBERG_ROW_IDENTITY_FILE_COLUMN,
                DataType::Utf8,
            ),
            column(
                crate::connector::iceberg::catalog::backend::ICEBERG_ROW_IDENTITY_POS_COLUMN,
                DataType::Int64,
            ),
        ];
        let where_clause = where_expr("SELECT 1 FROM orders WHERE id = 1");

        let query =
            super::build_delete_position_sink_query(&target, &where_clause, &sink_columns, "dev")
                .expect("rewrite query");

        let rendered = query.to_string();
        assert!(rendered.contains("FROM `ice`.`db`.`orders`"));
        assert!(rendered.contains("FOR SYSTEM_TIME AS OF '__nr_ref:dev'"));
    }

    #[test]
    fn dv_delete_uses_distributed_dv_sink_not_local_collect() {
        let source = include_str!("delete_flow.rs");
        let branch = source
            .split("IcebergSqlDeleteStrategy::DeletionVectors")
            .nth(1)
            .expect("DV branch")
            .split("let resolved = {")
            .next()
            .expect("DV branch body");
        assert!(
            branch.contains("run_delete_dv_write_transaction"),
            "v3 DV-delete branch must route through the DV write transaction"
        );
        assert!(
            !branch.contains("scan_for_position_deletes_at"),
            "v3 DV-delete must not collect position deletes in the coordinator"
        );
        assert!(
            !branch.contains("load_existing_delete_visibility_by_data_file_at"),
            "v3 DV-delete must not preload existing delete visibility in the coordinator"
        );
        assert!(
            !branch.contains("load_referenced_data_file_partitions_at"),
            "v3 DV-delete must not preload referenced data-file partitions in the coordinator"
        );

        let transaction = source
            .split("fn run_delete_dv_write_transaction")
            .nth(1)
            .expect("DV transaction helper")
            .split("fn run_delete_write_transaction")
            .next()
            .expect("DV transaction helper body");
        assert!(
            transaction.contains("IcebergWriteSinkMode::DeletionVectors"),
            "v3 DV-delete transaction must use the DeletionVectors sink mode"
        );
        assert!(
            transaction.contains("set_planned_snapshot_id(base_snapshot_id)"),
            "v3 DV-delete transaction must pass the planned target snapshot to the sink"
        );
        assert!(
            transaction.contains("CommitOpKind::RowDeltaDvFromFiles"),
            "v3 DV-delete transaction must commit BE-written Puffin DV files"
        );
        assert!(
            transaction.contains("DistributedDvDeleteWriteExecutor"),
            "v3 DV-delete transaction must use the distributed executor"
        );
        assert!(
            !transaction.contains("inject_delete_group"),
            "v3 DV-delete transaction must not inject coordinator-local delete groups"
        );
        assert!(
            !branch.contains("InjectedDeleteGroupExecutor"),
            "v3 DV-delete must not use the local injected-delete-group executor"
        );

        let executor = source
            .split("struct DistributedDvDeleteWriteExecutor")
            .nth(1)
            .expect("DV distributed executor")
            .split("#[allow(clippy::too_many_arguments)]")
            .next()
            .expect("DV distributed executor body");
        assert!(
            executor.contains("execute_query_as_iceberg_write"),
            "v3 DV-delete executor must use the distributed iceberg write path"
        );
        assert!(
            executor.contains("iceberg_write_shuffle_by_output_index(0)"),
            "v3 DV-delete executor must shuffle by the first sink output column (_file)"
        );
    }

    #[test]
    fn dv_delete_local_scan_and_injected_executor_are_removed() {
        let source = include_str!("delete_flow.rs");
        let injected_executor = concat!("struct ", "InjectedDeleteGroupExecutor");
        let scan_at_helper = concat!("fn ", "scan_for_position_deletes_at");
        let scan_helper = concat!("fn ", "scan_for_position_deletes(");
        assert!(
            !source.contains(injected_executor),
            "v3 DV-delete must not keep the coordinator-local injected executor"
        );
        assert!(
            !source.contains(scan_at_helper),
            "v3 DV-delete must not keep the coordinator-local snapshot scan helper"
        );
        assert!(
            !source.contains(scan_helper),
            "v3 DV-delete must not keep the coordinator-local current-snapshot scan helper"
        );
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

    // --------------- Timestamp predicate tests ---------------

    fn iceberg_schema_with_timestamp() -> iceberg::spec::Schema {
        iceberg::spec::Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Int),
                )),
                Arc::new(NestedField::required(
                    2,
                    "ts",
                    Type::Primitive(PrimitiveType::Timestamp),
                )),
            ])
            .build()
            .expect("build iceberg schema with timestamp")
    }

    /// Build `WHERE ts = '<literal>'` as a sqlparser Expr.
    fn delete_where_ts_eq(literal: &str) -> sqlast::Expr {
        sqlast::Expr::BinaryOp {
            left: Box::new(sqlast::Expr::Identifier(sqlast::Ident::new("ts"))),
            op: sqlast::BinaryOperator::Eq,
            right: Box::new(sqlast::Expr::Value(sqlast::ValueWithSpan {
                value: sqlast::Value::SingleQuotedString(literal.to_string()),
                span: sqlparser::tokenizer::Span::empty(),
            })),
        }
    }

    #[test]
    fn literal_to_datum_parses_datetime_without_subseconds() {
        let schema = iceberg_schema_with_timestamp();
        let expr = sqlast::Expr::Value(sqlast::ValueWithSpan {
            value: sqlast::Value::SingleQuotedString("2020-01-01 00:00:00".to_string()),
            span: sqlparser::tokenizer::Span::empty(),
        });
        let datum = super::literal_to_datum(&expr, &schema, "ts").expect("parse datetime");
        // 2020-01-01 00:00:00 UTC == 1577836800 seconds == 1577836800_000000 microseconds
        use iceberg::spec::PrimitiveLiteral;
        assert!(
            matches!(datum.literal(), PrimitiveLiteral::Long(us) if *us == 1_577_836_800_000_000),
            "unexpected datum: {datum:?}"
        );
    }

    #[test]
    fn literal_to_datum_parses_datetime_with_subseconds() {
        let schema = iceberg_schema_with_timestamp();
        let expr = sqlast::Expr::Value(sqlast::ValueWithSpan {
            value: sqlast::Value::SingleQuotedString("2020-01-01 00:00:00.5".to_string()),
            span: sqlparser::tokenizer::Span::empty(),
        });
        let datum = super::literal_to_datum(&expr, &schema, "ts").expect("parse datetime .5");
        use iceberg::spec::PrimitiveLiteral;
        // .5 seconds == 500_000 microseconds
        assert!(
            matches!(datum.literal(), PrimitiveLiteral::Long(us) if *us == 1_577_836_800_500_000),
            "unexpected datum: {datum:?}"
        );
    }

    #[test]
    fn collect_position_deletes_finds_rows_matching_timestamp_predicate() {
        use arrow::array::TimestampMicrosecondArray;
        use arrow::datatypes::TimeUnit;

        let schema = iceberg_schema_with_timestamp();
        let batch_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("_file", DataType::Utf8, false),
            Field::new("_pos", DataType::Int64, false),
            Field::new("id", DataType::Int32, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
        ]));
        // Row 0: ts = 2020-01-01 00:00:00       (1577836800_000000 µs)
        // Row 1: ts = 2020-01-01 00:00:00.5     (1577836800_500000 µs)
        // Row 2: ts = 2020-01-02 00:00:00       (1577923200_000000 µs)
        let ts_values: Vec<i64> = vec![
            1_577_836_800_000_000,
            1_577_836_800_500_000,
            1_577_923_200_000_000,
        ];
        let batch = RecordBatch::try_new(
            batch_schema,
            vec![
                Arc::new(StringArray::from(vec![
                    "/wh/t/data.parquet",
                    "/wh/t/data.parquet",
                    "/wh/t/data.parquet",
                ])),
                Arc::new(Int64Array::from(vec![0, 1, 2])),
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(TimestampMicrosecondArray::from(ts_values)),
            ],
        )
        .expect("scan batch");

        // DELETE WHERE ts = '2020-01-01 00:00:00' — should match row 0 only.
        let where_expr = delete_where_ts_eq("2020-01-01 00:00:00");
        let empty_visibility = HashMap::new();
        let mut by_file = BTreeMap::new();
        super::collect_position_deletes_from_batch(
            &batch,
            &where_expr,
            &schema,
            &empty_visibility,
            &mut by_file,
        )
        .expect("collect positions");

        assert_eq!(
            by_file.get("/wh/t/data.parquet").map(Vec::as_slice),
            Some(&[0i64][..]),
            "expected only position 0 (row with ts=2020-01-01 00:00:00)"
        );
    }

    #[test]
    fn collect_position_deletes_finds_rows_matching_timestamp_with_subseconds() {
        use arrow::array::TimestampMicrosecondArray;
        use arrow::datatypes::TimeUnit;

        let schema = iceberg_schema_with_timestamp();
        let batch_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("_file", DataType::Utf8, false),
            Field::new("_pos", DataType::Int64, false),
            Field::new("id", DataType::Int32, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
        ]));
        let ts_values: Vec<i64> = vec![
            1_577_836_800_000_000,
            1_577_836_800_500_000,
            1_577_923_200_000_000,
        ];
        let batch = RecordBatch::try_new(
            batch_schema,
            vec![
                Arc::new(StringArray::from(vec![
                    "/wh/t/data.parquet",
                    "/wh/t/data.parquet",
                    "/wh/t/data.parquet",
                ])),
                Arc::new(Int64Array::from(vec![0, 1, 2])),
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(TimestampMicrosecondArray::from(ts_values)),
            ],
        )
        .expect("scan batch");

        // DELETE WHERE ts = '2020-01-01 00:00:00.5' — should match row 1 only.
        let where_expr = delete_where_ts_eq("2020-01-01 00:00:00.5");
        let empty_visibility = HashMap::new();
        let mut by_file = BTreeMap::new();
        super::collect_position_deletes_from_batch(
            &batch,
            &where_expr,
            &schema,
            &empty_visibility,
            &mut by_file,
        )
        .expect("collect positions");

        assert_eq!(
            by_file.get("/wh/t/data.parquet").map(Vec::as_slice),
            Some(&[1i64][..]),
            "expected only position 1 (row with ts=2020-01-01 00:00:00.5)"
        );
    }

    // --------------- Scalar-function predicate tests ---------------

    fn iceberg_schema_with_label() -> iceberg::spec::Schema {
        iceberg::spec::Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Int),
                )),
                Arc::new(NestedField::required(
                    2,
                    "label",
                    Type::Primitive(PrimitiveType::String),
                )),
            ])
            .build()
            .expect("build iceberg schema with label")
    }

    fn make_label_batch(labels: &[&str]) -> RecordBatch {
        let n = labels.len() as i64;
        let batch_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("_file", DataType::Utf8, false),
            Field::new("_pos", DataType::Int64, false),
            Field::new("id", DataType::Int32, false),
            Field::new("label", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            batch_schema,
            vec![
                Arc::new(StringArray::from(
                    (0..labels.len())
                        .map(|_| "/wh/t/data.parquet")
                        .collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from((0..n).collect::<Vec<_>>())),
                Arc::new(Int32Array::from(
                    (1..=labels.len() as i32).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(labels.to_vec())),
            ],
        )
        .expect("label batch")
    }

    fn delete_where_lower_label_eq(s: &str) -> sqlast::Expr {
        sqlast::Expr::BinaryOp {
            left: Box::new(sqlast::Expr::Function(sqlast::Function {
                name: sqlast::ObjectName::from(vec![sqlast::Ident::new("lower")]),
                args: sqlast::FunctionArguments::List(sqlast::FunctionArgumentList {
                    duplicate_treatment: None,
                    args: vec![sqlast::FunctionArg::Unnamed(sqlast::FunctionArgExpr::Expr(
                        sqlast::Expr::Identifier(sqlast::Ident::new("label")),
                    ))],
                    clauses: vec![],
                }),
                filter: None,
                null_treatment: None,
                over: None,
                within_group: vec![],
                parameters: sqlast::FunctionArguments::None,
                uses_odbc_syntax: false,
            })),
            op: sqlast::BinaryOperator::Eq,
            right: Box::new(sqlast::Expr::Value(sqlast::ValueWithSpan {
                value: sqlast::Value::SingleQuotedString(s.to_string()),
                span: sqlparser::tokenizer::Span::empty(),
            })),
        }
    }

    fn delete_where_upper_label_eq(s: &str) -> sqlast::Expr {
        sqlast::Expr::BinaryOp {
            left: Box::new(sqlast::Expr::Function(sqlast::Function {
                name: sqlast::ObjectName::from(vec![sqlast::Ident::new("upper")]),
                args: sqlast::FunctionArguments::List(sqlast::FunctionArgumentList {
                    duplicate_treatment: None,
                    args: vec![sqlast::FunctionArg::Unnamed(sqlast::FunctionArgExpr::Expr(
                        sqlast::Expr::Identifier(sqlast::Ident::new("label")),
                    ))],
                    clauses: vec![],
                }),
                filter: None,
                null_treatment: None,
                over: None,
                within_group: vec![],
                parameters: sqlast::FunctionArguments::None,
                uses_odbc_syntax: false,
            })),
            op: sqlast::BinaryOperator::Eq,
            right: Box::new(sqlast::Expr::Value(sqlast::ValueWithSpan {
                value: sqlast::Value::SingleQuotedString(s.to_string()),
                span: sqlparser::tokenizer::Span::empty(),
            })),
        }
    }

    #[test]
    fn scalar_fn_lower_matches_only_target_row() {
        // Rows: (id=1,label='X'), (id=2,label='Y'), (id=3,label='Z')
        // DELETE WHERE LOWER(label) = 'y' — should match position 1 (id=2) only.
        let schema = iceberg_schema_with_label();
        let batch = make_label_batch(&["X", "Y", "Z"]);
        let where_expr = delete_where_lower_label_eq("y");
        let empty_visibility = HashMap::new();
        let mut by_file = BTreeMap::new();

        super::collect_position_deletes_from_batch(
            &batch,
            &where_expr,
            &schema,
            &empty_visibility,
            &mut by_file,
        )
        .expect("collect positions with LOWER predicate");

        assert_eq!(
            by_file.get("/wh/t/data.parquet").map(Vec::as_slice),
            Some(&[1i64][..]),
            "expected only position 1 (label='Y', LOWER='y')"
        );
    }

    #[test]
    fn scalar_fn_upper_matches_only_target_row() {
        // DELETE WHERE UPPER(label) = 'Y' — should match position 1 (id=2) only.
        let schema = iceberg_schema_with_label();
        let batch = make_label_batch(&["X", "Y", "Z"]);
        let where_expr = delete_where_upper_label_eq("Y");
        let empty_visibility = HashMap::new();
        let mut by_file = BTreeMap::new();

        super::collect_position_deletes_from_batch(
            &batch,
            &where_expr,
            &schema,
            &empty_visibility,
            &mut by_file,
        )
        .expect("collect positions with UPPER predicate");

        assert_eq!(
            by_file.get("/wh/t/data.parquet").map(Vec::as_slice),
            Some(&[1i64][..]),
            "expected only position 1 (label='Y', UPPER='Y')"
        );
    }

    #[test]
    fn scalar_fn_lower_on_already_lowercase_matches_correctly() {
        // Rows all already lowercase: ('x','y','z').
        // DELETE WHERE LOWER(label) = 'y' — should match position 1.
        let schema = iceberg_schema_with_label();
        let batch = make_label_batch(&["x", "y", "z"]);
        let where_expr = delete_where_lower_label_eq("y");
        let empty_visibility = HashMap::new();
        let mut by_file = BTreeMap::new();

        super::collect_position_deletes_from_batch(
            &batch,
            &where_expr,
            &schema,
            &empty_visibility,
            &mut by_file,
        )
        .expect("collect positions");

        assert_eq!(
            by_file.get("/wh/t/data.parquet").map(Vec::as_slice),
            Some(&[1i64][..]),
        );
    }

    #[test]
    fn scalar_fn_lower_no_match_returns_empty() {
        // DELETE WHERE LOWER(label) = 'q' — no rows match.
        let schema = iceberg_schema_with_label();
        let batch = make_label_batch(&["X", "Y", "Z"]);
        let where_expr = delete_where_lower_label_eq("q");
        let empty_visibility = HashMap::new();
        let mut by_file = BTreeMap::new();

        super::collect_position_deletes_from_batch(
            &batch,
            &where_expr,
            &schema,
            &empty_visibility,
            &mut by_file,
        )
        .expect("collect positions");

        // No positions → map entry should be absent or empty.
        assert!(
            by_file
                .get("/wh/t/data.parquet")
                .map_or(true, |v| v.is_empty()),
            "expected no matching positions"
        );
    }

    /// Regression: plain col = literal still works correctly after the change.
    #[test]
    fn regression_plain_eq_still_works() {
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
                    "/wh/t/data.parquet",
                    "/wh/t/data.parquet",
                    "/wh/t/data.parquet",
                ])),
                Arc::new(Int64Array::from(vec![0, 1, 2])),
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .expect("batch");
        // DELETE WHERE id = 2
        let where_expr = sqlast::Expr::BinaryOp {
            left: Box::new(sqlast::Expr::Identifier(sqlast::Ident::new("id"))),
            op: sqlast::BinaryOperator::Eq,
            right: Box::new(sqlast::Expr::Value(sqlast::ValueWithSpan {
                value: sqlast::Value::Number("2".to_string(), false),
                span: sqlparser::tokenizer::Span::empty(),
            })),
        };
        let empty_visibility = HashMap::new();
        let mut by_file = BTreeMap::new();
        super::collect_position_deletes_from_batch(
            &batch,
            &where_expr,
            &schema,
            &empty_visibility,
            &mut by_file,
        )
        .expect("collect positions");
        assert_eq!(
            by_file.get("/wh/t/data.parquet").map(Vec::as_slice),
            Some(&[1i64][..]),
        );
    }

    /// Regression: col IN (...) still works correctly after the change.
    #[test]
    fn regression_in_list_still_works() {
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
                    "/wh/t/data.parquet",
                    "/wh/t/data.parquet",
                    "/wh/t/data.parquet",
                ])),
                Arc::new(Int64Array::from(vec![0, 1, 2])),
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .expect("batch");
        let where_expr = delete_where_id_in_2_3();
        let empty_visibility = HashMap::new();
        let mut by_file = BTreeMap::new();
        super::collect_position_deletes_from_batch(
            &batch,
            &where_expr,
            &schema,
            &empty_visibility,
            &mut by_file,
        )
        .expect("collect positions");
        // id IN (2, 3) → positions 1 and 2
        assert_eq!(
            by_file.get("/wh/t/data.parquet").map(Vec::as_slice),
            Some(&[1i64, 2][..]),
        );
    }
}
