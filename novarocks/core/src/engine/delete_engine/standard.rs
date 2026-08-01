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
use arrow::datatypes::{DataType, Field, Schema};
use bytes::Bytes;
use chrono::NaiveDateTime;
use iceberg::expr::{Predicate, Reference};
use iceberg::spec::{Datum, PrimitiveType, Type};
use sqlparser::ast as sqlast;

use crate::connector::iceberg::catalog::registry::{self, block_on_iceberg, build_iceberg_catalog};
use crate::connector::iceberg::commit::{
    CommitOpKind, CommitOutcome, CommitServiceError, DeletionVector, IcebergCommitCollector,
    IcebergSqlDeleteStrategy, classify_sql_delete_strategy,
};
#[cfg(test)]
use crate::connector::iceberg::delete_visibility::{
    ExistingDeleteVisibility, ReferencedDataFilePartition, ReferencedDataFilePartitions,
    insert_referenced_data_file_partition, load_existing_delete_visibility_by_data_file,
    load_existing_delete_visibility_by_data_file_at,
    load_existing_delete_visibility_from_descriptors, load_referenced_data_file_partitions,
    load_referenced_data_file_partitions_at,
};
use crate::connector::iceberg::delete_visibility::{
    ExistingDeleteVisibilityByDataFile, data_file_row_is_visible,
};
use crate::connector::iceberg::ref_snapshot::resolve_branch_head_snapshot_id;
use crate::connector::iceberg::sink::build_position_delete_data_file_partition_index;
use crate::connector::iceberg::sink_plan::IcebergSinkObjectStoreConfig;
use crate::connector::iceberg::write_contract::{
    encode_deletion_vector_sink_handle_payload, encode_position_delete_sink_handle_payload,
};
use crate::engine::StandaloneState;
use crate::engine::backend_resolver::{TargetBackend, resolve_existing_table_target};
use crate::engine::delete_engine::{
    DeleteOperation, PreparedDelete, PreparedDeleteExecution, prepared_delete,
};
use crate::engine::write_transaction::IcebergWriteCommitExecutor;
use crate::query_execution::outcome::QueryExecutionResult;
use crate::query_execution::request_context::QueryExecutionContext;
use crate::sql::analyzer::iceberg_ref::{IcebergRefSuffix, split_ref_suffix};
use crate::sql::parser::ast::{DeleteStmt, ObjectName};
use crate::sql::planner::distributed::write::sink::{IcebergWriteSinkMode, IcebergWriteSinkSpec};
use novarocks_catalog::schema::ColumnDef;
use novarocks_spi::connector::{ConnectorWriteIntent, ConnectorWriteOperationId};

pub(crate) fn prepare_delete_statement(
    state: &Arc<StandaloneState>,
    stmt: &DeleteStmt,
    current_catalog: Option<&str>,
    current_database: &str,
    execution: &QueryExecutionContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<PreparedDelete, String> {
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
    crate::engine::mv::iceberg_guard::reject_if_iceberg_mv_properties(
        &target,
        table.metadata().properties(),
        crate::engine::mv::iceberg_guard::IcebergMvUserMutation::Delete,
    )?;

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
        return prepare_delete_dv_write(
            state,
            &target,
            catalog,
            table,
            entry,
            base_snapshot_id,
            &target_ref,
            &stmt.where_clause,
            execution.clone(),
            connector_context,
        );
    }

    let resolved = {
        crate::connector::metadata_load_table(
            state.connector_control.as_ref(),
            connector_context.clone(),
            &target.catalog,
            &target.namespace,
            &target.table,
            novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
        )?
        .0
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
            novarocks_types::UniqueId::new(0, 0),
        )
        .with_table_metadata(metadata.clone()),
    );
    prepare_delete_write(
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
        execution.clone(),
        connector_context,
    )
}

struct DistributedDeleteWriteExecutor {
    state: Arc<StandaloneState>,
    target: TargetBackend,
    delete_query: sqlparser::ast::Query,
    sink_spec: IcebergWriteSinkSpec,
    commit_executor: Arc<IcebergWriteCommitExecutor>,
    execution: QueryExecutionContext,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
    connector_write: crate::query_execution::contract::ConnectorWritePlanningTemplate,
}

impl PreparedDeleteExecution for DistributedDeleteWriteExecutor {
    fn run(&self) -> Result<QueryExecutionResult, String> {
        let mut result = crate::engine::execute_query_as_iceberg_write_with_connector_context(
            &self.state,
            Some(&self.target.catalog),
            &self.target.namespace,
            &self.delete_query,
            self.sink_spec.clone(),
            None,
            None,
            Some(&self.execution),
            &self.connector_context,
            Some(self.connector_write.clone()),
        )?;
        Ok(result)
    }

    fn commit(
        &self,
        completion: &crate::query_execution::ConnectorWriteCompletion,
    ) -> Result<CommitOutcome, CommitServiceError> {
        crate::engine::iceberg_writer::commit_iceberg_connector_write(
            &self.commit_executor,
            completion,
        )
    }

    fn finalize(&self) -> Result<(), String> {
        self.commit_executor.finalize()
    }
}

struct DistributedDvDeleteWriteExecutor {
    state: Arc<StandaloneState>,
    target: TargetBackend,
    delete_query: sqlparser::ast::Query,
    sink_spec: IcebergWriteSinkSpec,
    commit_executor: Arc<IcebergWriteCommitExecutor>,
    execution: QueryExecutionContext,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
    connector_write: crate::query_execution::contract::ConnectorWritePlanningTemplate,
}

impl PreparedDeleteExecution for DistributedDvDeleteWriteExecutor {
    fn run(&self) -> Result<QueryExecutionResult, String> {
        let mut result = crate::engine::execute_query_as_iceberg_write_with_connector_context(
            &self.state,
            Some(&self.target.catalog),
            &self.target.namespace,
            &self.delete_query,
            self.sink_spec.clone(),
            None,
            Some(crate::engine::iceberg_write_shuffle_by_output_index(0)),
            Some(&self.execution),
            &self.connector_context,
            Some(self.connector_write.clone()),
        )?;
        Ok(result)
    }

    fn commit(
        &self,
        completion: &crate::query_execution::ConnectorWriteCompletion,
    ) -> Result<CommitOutcome, CommitServiceError> {
        crate::engine::iceberg_writer::commit_iceberg_connector_write(
            &self.commit_executor,
            completion,
        )
    }

    fn finalize(&self) -> Result<(), String> {
        self.commit_executor.finalize()
    }
}

#[allow(clippy::too_many_arguments)]
fn prepare_delete_dv_write(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    catalog: Arc<dyn iceberg::Catalog>,
    table: iceberg::table::Table,
    entry: crate::connector::iceberg::catalog::IcebergCatalogEntry,
    base_snapshot_id: Option<i64>,
    target_ref: &str,
    where_clause: &sqlast::Expr,
    execution: QueryExecutionContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<PreparedDelete, String> {
    let resolved = {
        crate::connector::metadata_load_table(
            state.connector_control.as_ref(),
            connector_context.clone(),
            &target.catalog,
            &target.namespace,
            &target.table,
            novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
        )?
        .0
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
    let writer_handle_payload =
        frozen_deletion_vector_handle_payload(&sink_spec, &table, &entry, base_snapshot_id)?;
    let input_schema = Arc::new(Schema::new(
        sink_spec
            .target_columns
            .iter()
            .take(2)
            .map(|column| Field::new(&column.name, column.data_type.clone(), column.nullable))
            .collect::<Vec<_>>(),
    ));
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
            novarocks_types::UniqueId::new(0, 0),
        )
        .with_table_metadata(metadata.clone()),
    );
    let abort_cleanup =
        crate::engine::iceberg_writer::build_abort_cleanup_for_catalog_entry(&entry)?;
    let commit_executor = Arc::new(IcebergWriteCommitExecutor {
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
    });
    let connector_operation_id = ConnectorWriteOperationId::new();
    let connector_write = crate::engine::iceberg_writer::register_iceberg_connector_write(
        state,
        target,
        target_ref,
        ConnectorWriteIntent::RowDelta,
        input_schema,
        writer_handle_payload,
        Arc::clone(&commit_executor),
        connector_operation_id,
        connector_context.clone(),
    )?;
    let executor = DistributedDvDeleteWriteExecutor {
        state: Arc::clone(state),
        target: target.clone(),
        delete_query,
        sink_spec,
        commit_executor,
        execution,
        connector_context: connector_context.clone(),
        connector_write,
    };
    Ok(prepared_delete(
        DeleteOperation {
            catalog: target.catalog.clone(),
            namespace: target.namespace.clone(),
            table: target.table.clone(),
            target_ref: target_ref.to_string(),
            attempt_id: connector_operation_id.to_string(),
            commit_op_kind: CommitOpKind::RowDeltaDvFromFiles,
            base_snapshot_id,
        },
        Arc::new(executor),
    ))
}

#[allow(clippy::too_many_arguments)]
fn prepare_delete_write(
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
    execution: QueryExecutionContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<PreparedDelete, String> {
    let metadata = table.metadata();
    let mut sink_spec = sink_spec;
    sink_spec.set_planned_snapshot_id(base_snapshot_id)?;
    let position_index_storage = position_delete_index_storage_config(&entry, metadata.location())?;
    let position_delete_partitions = build_position_delete_data_file_partition_index(
        metadata,
        base_snapshot_id,
        metadata.location(),
        position_index_storage.as_ref(),
    )?;
    let writer_handle_payload = encode_position_delete_sink_handle_payload(
        &sink_spec,
        metadata,
        &position_delete_partitions,
    )?;
    let input_schema = Arc::new(Schema::new(
        sink_spec
            .target_columns
            .iter()
            .take(2)
            .map(|column| Field::new(&column.name, column.data_type.clone(), column.nullable))
            .collect::<Vec<_>>(),
    ));
    let abort_cleanup =
        crate::engine::iceberg_writer::build_abort_cleanup_for_catalog_entry(&entry)?;
    let commit_executor = Arc::new(IcebergWriteCommitExecutor {
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
    });
    let connector_operation_id = ConnectorWriteOperationId::new();
    let connector_write = crate::engine::iceberg_writer::register_iceberg_connector_write(
        state,
        target,
        target_ref,
        ConnectorWriteIntent::RowDelta,
        input_schema,
        writer_handle_payload,
        Arc::clone(&commit_executor),
        connector_operation_id,
        connector_context.clone(),
    )?;
    let executor = DistributedDeleteWriteExecutor {
        state: Arc::clone(state),
        target: target.clone(),
        delete_query,
        sink_spec,
        commit_executor,
        execution,
        connector_context: connector_context.clone(),
        connector_write,
    };
    Ok(prepared_delete(
        DeleteOperation {
            catalog: target.catalog.clone(),
            namespace: target.namespace.clone(),
            table: target.table.clone(),
            target_ref: target_ref.to_string(),
            attempt_id: connector_operation_id.to_string(),
            commit_op_kind: CommitOpKind::RowDelta,
            base_snapshot_id,
        },
        Arc::new(executor),
    ))
}

/// Freeze all deletion-vector facts needed by a BE-only writer at the exact
/// base snapshot. Credentials and catalog clients stay on the FE.
pub(crate) fn frozen_deletion_vector_handle_payload(
    sink_spec: &IcebergWriteSinkSpec,
    table: &iceberg::table::Table,
    entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    base_snapshot_id: Option<i64>,
) -> Result<Bytes, String> {
    let metadata = table.metadata();
    let position_index_storage = position_delete_index_storage_config(entry, metadata.location())?;
    let position_delete_partitions = build_position_delete_data_file_partition_index(
        metadata,
        base_snapshot_id,
        metadata.location(),
        position_index_storage.as_ref(),
    )?;
    let existing_vectors = frozen_deletion_vectors_at_snapshot(table, base_snapshot_id, entry)?;
    encode_deletion_vector_sink_handle_payload(
        sink_spec,
        metadata,
        &position_delete_partitions,
        &existing_vectors,
    )
}

fn position_delete_index_storage_config(
    entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    table_location: &str,
) -> Result<Option<IcebergSinkObjectStoreConfig>, String> {
    let Some(bucket) =
        crate::connector::iceberg::changes::expected_object_store_bucket_from_location(
            table_location,
        )?
    else {
        return Ok(None);
    };
    let config = entry.object_store_config().ok_or_else(|| {
        format!(
            "Iceberg position-delete planning requires object-store credentials for bucket {bucket}"
        )
    })?;
    Ok(Some(IcebergSinkObjectStoreConfig {
        endpoint: config.endpoint.clone(),
        bucket,
        access_key_id: config.access_key_id.clone(),
        access_key_secret: config.access_key_secret.clone(),
        session_token: config.session_token.clone(),
        region: config.region.clone(),
        enable_path_style_access: config.enable_path_style_access,
        retry_max_times: config.retry_max_times,
        retry_min_delay_ms: config.retry_min_delay_ms,
        retry_max_delay_ms: config.retry_max_delay_ms,
        timeout_ms: config.timeout_ms,
        io_timeout_ms: config.io_timeout_ms,
    }))
}

fn frozen_deletion_vectors_at_snapshot(
    table: &iceberg::table::Table,
    snapshot_id: Option<i64>,
    entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
) -> Result<HashMap<String, DeletionVector>, String> {
    let Some(snapshot_id) = snapshot_id else {
        return Ok(HashMap::new());
    };
    let object_store_config = entry.object_store_config();
    let factory =
        crate::connector::iceberg::changes::build_factory_for_table(table, object_store_config)?;
    let expected_bucket =
        crate::connector::iceberg::changes::expected_object_store_bucket_for_table(table)?;
    let positions =
        crate::connector::iceberg::scan_deletes::previously_deleted_positions_at_snapshot(
            table,
            snapshot_id,
            &factory,
            &|path| {
                crate::connector::iceberg::changes::normalize_delete_projection_path(
                    path,
                    object_store_config,
                    expected_bucket.as_deref(),
                )
            },
            |_| true,
        )
        .map_err(|error| {
            format!(
                "read frozen Iceberg deletion-vector positions at snapshot {snapshot_id}: {error}"
            )
        })?;
    positions
        .into_iter()
        .map(|(path, positions)| {
            let mut vector = DeletionVector::new();
            for position in positions {
                vector.insert(position).map_err(|error| {
                    format!(
                        "encode frozen Iceberg deletion-vector position {position} for `{path}`: {error}"
                    )
                })?;
            }
            Ok((path, vector))
        })
        .collect()
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

    use crate::connector::iceberg::delete_file::{
        IcebergDeleteFileSpec, IcebergFileContent, IcebergFileFormat,
    };

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

    fn factory_for_dir(dir: &std::path::Path) -> novarocks_fs::FsAccessHandle {
        novarocks_fs::FsAccessResolver::new()
            .resolve_location(dir.join("__binding__").to_string_lossy(), None)
            .expect("access")
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

    fn column(name: &str, data_type: DataType) -> novarocks_catalog::schema::ColumnDef {
        novarocks_catalog::schema::ColumnDef {
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
            file_format: IcebergFileFormat::Parquet,
            file_content: IcebergFileContent::EqualityDeletes,
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
