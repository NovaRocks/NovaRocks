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

//! Standalone-mode iceberg INSERT INTO / INSERT OVERWRITE entry point.
//!
//! Routes from `insert_flow::run_insert` for iceberg targets whose source is
//! handled as one transaction. `UnionAll` remains split by `insert_flow` so
//! each part gets its own operation record.
//!
//! Phase 1 scope (per spec §0.4):
//! * `INSERT INTO iceberg ... SELECT ...` — handled here.
//! * `INSERT OVERWRITE iceberg ... SELECT ...` — handled here.
//! * `INSERT INTO iceberg VALUES (...)` — handled here.
//! * `INSERT OVERWRITE iceberg VALUES (...)` — handled here.

use std::collections::BTreeMap;
use std::sync::Arc;

use iceberg::Catalog;
use iceberg::spec::DataFile;
use iceberg::{NamespaceIdent, TableIdent};

use crate::connector::backend::ResolvedTable;
use crate::connector::iceberg::catalog::backend::{
    ICEBERG_ROW_IDENTITY_FILE_COLUMN, ICEBERG_ROW_IDENTITY_POS_COLUMN,
    iceberg_schema_def_for_codegen,
};
use crate::connector::iceberg::catalog::registry::{
    IcebergCatalogEntry, block_on_iceberg, build_iceberg_catalog,
};
use crate::connector::iceberg::commit::{
    CleanupPathMapper, CommitOpKind, CommitOutcome, CommitServiceError, EqualityDeleteColumn,
    IcebergCommitCollector, WrittenFile, ensure_iceberg_write_supported,
    ensure_no_equality_deletes, ensure_overwrite_single_partition_spec,
};
use crate::connector::iceberg::position_delete_descriptor::{
    ICEBERG_POSITION_DELETE_FILE_PATH_COLUMN, ICEBERG_POSITION_DELETE_FILE_PATH_FIELD_ID,
    ICEBERG_POSITION_DELETE_POS_COLUMN, ICEBERG_POSITION_DELETE_POS_FIELD_ID,
    PositionDeleteDescriptorInput, PositionDeleteOutputField, PositionDeletePartitionSourceField,
};
use crate::connector::iceberg::scan_model::{
    IcebergDataFileBinding, IcebergSchemaDef, IcebergSchemaFieldDef, IcebergTableInfo,
};
use crate::coordinator::execution::CoordinatedQueryResult;
use crate::coordinator::write::report::WriteCommitInput;
use crate::engine::backend_resolver::TargetBackend;
use crate::engine::mv::refresh_io::query_result_to_chunks;
use crate::engine::write_transaction::{
    IcebergWriteCommitExecutor, IcebergWriteCommitPolicy, IcebergWriteSource,
    IcebergWriteTransactionExecutor, IcebergWriteTransactionRunner, IcebergWriteTransactionSpec,
    IcebergWriteValidationPolicy,
};
use crate::engine::{StandaloneState, StatementResult};
use crate::exec::chunk::Chunk;
use crate::meta::repository::iceberg_operation::{IcebergOperationKind, IcebergOperationTarget};
use crate::sql::parser::ast::{InsertSource, Literal};
use crate::sql::planner::distributed::write::sink::{
    IcebergWriteFileCompression, IcebergWriteSinkMode, IcebergWriteSinkSpec,
    synthetic_iceberg_write_table_id, transform_to_sink_string,
};
use crate::sql::planner::table::{ScanSource, TableDef};
use novarocks_catalog::schema::ColumnDef;
use novarocks_catalog::schema::SqlType;

pub(crate) fn execute_iceberg_insert_or_overwrite(
    state: &Arc<StandaloneState>,
    target: &TargetBackend,
    resolved: &ResolvedTable,
    insert_columns: &[String],
    source: &InsertSource,
    overwrite_mode: crate::sql::parser::ast::OverwriteMode,
    target_ref: &str,
) -> Result<StatementResult, String> {
    use crate::sql::parser::ast::OverwriteMode;
    debug_assert_eq!(target.backend_name, "iceberg");

    let overwrite_full_table = matches!(overwrite_mode, OverwriteMode::FullTable);
    let overwrite_partitions = matches!(overwrite_mode, OverwriteMode::DynamicPartitions);

    // Reject UNION ALL on this path; caller enforces this for branch writes,
    // and OVERWRITE with this source is never valid.
    if matches!(source, InsertSource::UnionAll(_)) {
        return Err(
            "iceberg INSERT/OVERWRITE does not support UNION ALL sources on this path".to_string(),
        );
    }

    // 1. Resolve catalog entry + build iceberg-rust Catalog handle.
    let entry = {
        let registry = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        registry.get(&target.catalog)?
    };
    let catalog: Arc<dyn Catalog> = build_iceberg_catalog(&entry)?;
    let table_ident = TableIdent::new(
        NamespaceIdent::new(target.namespace.clone()),
        target.table.clone(),
    );
    let table =
        block_on_iceberg(async { catalog.load_table(&table_ident).await })?.map_err(|e| {
            format!(
                "load iceberg table {target_str}: {e}",
                target_str = target_string(target)
            )
        })?;

    // 2. Pre-lowering validators.
    let _write_mode = ensure_iceberg_write_supported(&table)?;
    if overwrite_full_table {
        ensure_overwrite_single_partition_spec(&table)?;
        ensure_no_equality_deletes(&table)?;
    }
    if overwrite_partitions {
        // v3 row-lineage + cross-historical-spec checks happen in
        // OverwritePartitionsCommit.
        if table.metadata().default_partition_spec().is_unpartitioned() {
            return Err(format!(
                "INSERT OVERWRITE PARTITIONS requires a partitioned table; \
                 table {} is unpartitioned (use OVERWRITE without PARTITIONS)",
                target_string(target),
            ));
        }
    }
    // Branch writes require Iceberg v3 (row-lineage semantics).
    if target_ref != "main" {
        let fmt = table.metadata().format_version();
        if fmt != iceberg::spec::FormatVersion::V3 {
            return Err(format!(
                "iceberg ref: branch writes require Iceberg v3 tables (table {} is v{})",
                target_string(target),
                fmt as u8,
            ));
        }
    }

    execute_iceberg_insert_distributed(
        state,
        target,
        resolved,
        insert_columns,
        source,
        overwrite_mode,
        target_ref,
        catalog,
        table,
        &entry,
        table_ident,
    )
}

#[allow(clippy::too_many_arguments)]
fn execute_iceberg_insert_distributed(
    state: &Arc<StandaloneState>,
    target: &TargetBackend,
    resolved: &ResolvedTable,
    insert_columns: &[String],
    source: &InsertSource,
    overwrite_mode: crate::sql::parser::ast::OverwriteMode,
    target_ref: &str,
    catalog: Arc<dyn Catalog>,
    table: iceberg::table::Table,
    entry: &IcebergCatalogEntry,
    table_ident: TableIdent,
) -> Result<StatementResult, String> {
    let metadata = table.metadata();
    let (query, sink_spec) =
        build_insert_write_plan(target, resolved, insert_columns, source, &table, entry)?;

    let commit_op_kind = commit_op_kind_for_overwrite_mode(overwrite_mode);
    let base_snapshot_id = write_base_snapshot_id(metadata, target_ref)?;
    let base_sequence_number = metadata.last_sequence_number();
    let current_schema = metadata.current_schema().clone();
    let default_partition_spec = metadata.default_partition_spec().clone();
    let staging_dir = format!(
        "{}/data/_staging/{}",
        metadata.location(),
        uuid::Uuid::new_v4()
    );
    let collector = Arc::new(
        IcebergCommitCollector::new(
            commit_op_kind,
            table_ident,
            base_snapshot_id,
            base_sequence_number,
            current_schema,
            default_partition_spec,
            staging_dir,
            crate::common::types::UniqueId { hi: 0, lo: 0 },
        )
        .with_table_metadata(metadata.clone()),
    );

    let abort_cleanup = build_abort_cleanup_for_catalog_entry(entry)?;
    let commit_executor = IcebergWriteCommitExecutor {
        state: Arc::clone(state),
        target: target.clone(),
        catalog,
        table,
        collector,
        fs: abort_cleanup.fs,
        cleanup_path_mapper: abort_cleanup.path_mapper,
        cow_update_rewrite: None,
        target_ref: target_ref.to_string(),
        snapshot_properties: BTreeMap::new(),
    };
    let executor = DistributedInsertWriteExecutor {
        state: Arc::clone(state),
        target: target.clone(),
        query,
        sink_spec,
        commit_executor,
    };
    let spec = IcebergWriteTransactionSpec {
        target: IcebergOperationTarget {
            catalog: target.catalog.clone(),
            namespace: target.namespace.clone(),
            table: target.table.clone(),
            ref_name: (target_ref != "main").then(|| target_ref.to_string()),
        },
        operation_kind: operation_kind_for_commit_op_kind(commit_op_kind),
        attempt_id: format!("{}:{}", target_string(target), uuid::Uuid::new_v4()),
        commit: IcebergWriteCommitPolicy {
            commit_op_kind,
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
    let runner = IcebergWriteTransactionRunner::new(Arc::clone(state), &executor);
    let _outcome = runner.run(spec)?;

    Ok(StatementResult::Ok)
}

struct DistributedInsertWriteExecutor {
    state: Arc<StandaloneState>,
    target: TargetBackend,
    query: sqlparser::ast::Query,
    sink_spec: IcebergWriteSinkSpec,
    commit_executor: IcebergWriteCommitExecutor,
}

impl IcebergWriteTransactionExecutor for DistributedInsertWriteExecutor {
    fn run_coordinated_write(
        &self,
        _spec: &IcebergWriteTransactionSpec,
    ) -> Result<CoordinatedQueryResult, String> {
        crate::engine::execute_query_as_iceberg_write(
            &self.state,
            Some(&self.target.catalog),
            &self.target.namespace,
            &self.query,
            self.sink_spec.clone(),
            None,
            None,
        )
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

/// Build the `(query, sink_spec)` pair for an iceberg INSERT/OVERWRITE write
/// without driving a transaction. The standalone INSERT path runs this through
/// its own runner; the folded MERGE not-matched INSERT branch runs the same
/// pair into a shared collector so the INSERT commits in the same snapshot as
/// the matched branch. Factored out of `execute_iceberg_insert_distributed` so
/// both callers share one query/sink construction (no semantic drift).
pub(crate) fn build_insert_write_plan(
    target: &TargetBackend,
    resolved: &ResolvedTable,
    insert_columns: &[String],
    source: &InsertSource,
    table: &iceberg::table::Table,
    entry: &IcebergCatalogEntry,
) -> Result<(sqlparser::ast::Query, IcebergWriteSinkSpec), String> {
    let write_columns = iceberg_insert_columns_from_schema(table.metadata().current_schema())?;
    let query = append_source_to_query_for_write(
        source,
        insert_columns,
        &resolved.columns,
        &write_columns,
    )?;
    let sink_spec = build_insert_write_sink_spec(target, resolved, table, entry, &write_columns)?;
    Ok((query, sink_spec))
}

pub(crate) fn build_insert_write_sink_spec(
    target: &TargetBackend,
    resolved: &ResolvedTable,
    table: &iceberg::table::Table,
    entry: &IcebergCatalogEntry,
    write_columns: &[ColumnDef],
) -> Result<IcebergWriteSinkSpec, String> {
    build_iceberg_write_sink_spec(
        target,
        resolved,
        table,
        entry,
        IcebergWriteSinkMode::Data,
        write_columns.to_vec(),
    )
}

pub(crate) fn build_position_delete_sink_spec(
    target: &TargetBackend,
    resolved: &ResolvedTable,
    table: &iceberg::table::Table,
    entry: &IcebergCatalogEntry,
) -> Result<IcebergWriteSinkSpec, String> {
    let target_columns = position_delete_sink_input_columns(table.metadata(), &resolved.columns)?;
    build_iceberg_write_sink_spec(
        target,
        resolved,
        table,
        entry,
        IcebergWriteSinkMode::PositionDeletes,
        target_columns,
    )
}

pub(crate) fn build_row_lineage_data_sink_spec(
    target: &TargetBackend,
    resolved: &ResolvedTable,
    table: &iceberg::table::Table,
    entry: &IcebergCatalogEntry,
) -> Result<IcebergWriteSinkSpec, String> {
    let target_columns = row_lineage_data_sink_input_columns(&resolved.columns);
    build_iceberg_write_sink_spec(
        target,
        resolved,
        table,
        entry,
        IcebergWriteSinkMode::RowLineageData,
        target_columns,
    )
}

pub(crate) fn build_equality_delete_sink_spec(
    target: &TargetBackend,
    resolved: &ResolvedTable,
    table: &iceberg::table::Table,
    entry: &IcebergCatalogEntry,
    equality_columns: &[EqualityDeleteColumn],
) -> Result<IcebergWriteSinkSpec, String> {
    if equality_columns.is_empty() {
        return Err(
            "iceberg equality-delete sink requires at least one equality column".to_string(),
        );
    }
    let target_columns = equality_columns
        .iter()
        .map(|column| ColumnDef {
            name: column.name.clone(),
            data_type: column.data_type.clone(),
            nullable: column.nullable,
            write_default: None,
            logical_type: None,
        })
        .collect::<Vec<_>>();
    build_iceberg_write_sink_spec(
        target,
        resolved,
        table,
        entry,
        IcebergWriteSinkMode::EqualityDeletes,
        target_columns,
    )
}

pub(crate) fn build_iceberg_write_sink_spec(
    target: &TargetBackend,
    resolved: &ResolvedTable,
    table: &iceberg::table::Table,
    entry: &IcebergCatalogEntry,
    mode: IcebergWriteSinkMode,
    target_columns: Vec<ColumnDef>,
) -> Result<IcebergWriteSinkSpec, String> {
    let metadata = table.metadata();
    let target_descriptor_columns =
        write_sink_target_descriptor_columns(mode, &resolved.columns, &target_columns)?;
    let iceberg_schema = match mode {
        IcebergWriteSinkMode::RowLineageData => {
            row_lineage_iceberg_schema_def_for_codegen(metadata.current_schema())
        }
        IcebergWriteSinkMode::Data
        | IcebergWriteSinkMode::PositionDeletes
        | IcebergWriteSinkMode::DeletionVectors
        | IcebergWriteSinkMode::EqualityDeletes => {
            iceberg_schema_def_for_codegen(metadata.current_schema())
        }
    };
    let iceberg = IcebergTableInfo {
        catalog: target.catalog.clone(),
        namespace: target.namespace.clone(),
        table: target.table.clone(),
        table_uuid: Some(metadata.uuid().to_string()),
        current_snapshot_id: metadata.current_snapshot_id(),
        schema_id: metadata.current_schema_id(),
        location: metadata.location().to_string(),
        schema: iceberg_schema,
        serialized_metadata: Some(
            serde_json::to_string(metadata)
                .map_err(|err| format!("serialize iceberg table metadata failed: {err}"))?,
        ),
        serialized_metadata_rows: None,
    };
    let cloud_properties = entry.cloud_properties_map();
    let target_table = TableDef {
        name: resolved.table.clone(),
        columns: target_descriptor_columns,
        iceberg_row_lineage_metadata_columns: Vec::new(),
        source: ScanSource::IcebergDataFiles {
            table: iceberg.clone(),
            files: Vec::new(),
            cloud_properties: cloud_properties.clone(),
            binding: IcebergDataFileBinding::CurrentSnapshot,
        },
    };
    let table_location = metadata.location().to_string();
    let data_location = metadata
        .properties()
        .get("write.data.path")
        .cloned()
        .unwrap_or_else(|| format!("{}/data", table_location.trim_end_matches('/')));
    let position_delete_output_descriptor = match mode {
        IcebergWriteSinkMode::PositionDeletes | IcebergWriteSinkMode::DeletionVectors => Some(
            build_position_delete_output_descriptor(metadata, &target_columns)?,
        ),
        IcebergWriteSinkMode::Data
        | IcebergWriteSinkMode::RowLineageData
        | IcebergWriteSinkMode::EqualityDeletes => None,
    };

    Ok(IcebergWriteSinkSpec {
        mode,
        target_table_id: synthetic_iceberg_write_table_id(),
        target_table,
        iceberg,
        target_columns,
        table_location,
        data_location,
        target_partition_spec_id: metadata.default_partition_spec_id(),
        cloud_properties,
        file_format: "parquet".to_string(),
        compression: IcebergWriteFileCompression::Snappy,
        position_delete_output_descriptor,
    })
}

fn row_lineage_data_sink_input_columns(target_columns: &[ColumnDef]) -> Vec<ColumnDef> {
    let mut columns = target_columns.to_vec();
    columns.push(ColumnDef {
        name: crate::exec::row_position::ICEBERG_ROW_ID_COL.to_string(),
        data_type: arrow::datatypes::DataType::Int64,
        nullable: false,
        write_default: None,
        logical_type: None,
    });
    columns.push(ColumnDef {
        name: crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL.to_string(),
        data_type: arrow::datatypes::DataType::Int64,
        nullable: true,
        write_default: None,
        logical_type: None,
    });
    columns
}

fn row_lineage_iceberg_schema_def_for_codegen(schema: &iceberg::spec::Schema) -> IcebergSchemaDef {
    let mut out = iceberg_schema_def_for_codegen(schema);
    out.fields.push(IcebergSchemaFieldDef {
        field_id: crate::exec::row_position::ICEBERG_RESERVED_FIELD_ID_ROW_ID,
        name: crate::exec::row_position::ICEBERG_ROW_ID_COL.to_string(),
        initial_default: None,
        write_default: None,
        initial_default_json: None,
        write_default_json: None,
        children: Vec::new(),
    });
    out.fields.push(IcebergSchemaFieldDef {
        field_id: crate::exec::row_position::ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
        name: crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL.to_string(),
        initial_default: None,
        write_default: None,
        initial_default_json: None,
        write_default_json: None,
        children: Vec::new(),
    });
    out
}

fn write_sink_target_descriptor_columns(
    mode: IcebergWriteSinkMode,
    resolved_columns: &[ColumnDef],
    sink_input_columns: &[ColumnDef],
) -> Result<Vec<ColumnDef>, String> {
    Ok(match mode {
        IcebergWriteSinkMode::PositionDeletes | IcebergWriteSinkMode::DeletionVectors => {
            resolved_columns.to_vec()
        }
        IcebergWriteSinkMode::Data | IcebergWriteSinkMode::RowLineageData => {
            sink_input_columns.to_vec()
        }
        IcebergWriteSinkMode::EqualityDeletes => sink_input_columns.to_vec(),
    })
}

fn position_delete_sink_input_columns(
    metadata: &iceberg::spec::TableMetadata,
    target_columns: &[ColumnDef],
) -> Result<Vec<ColumnDef>, String> {
    let mut columns = vec![
        ColumnDef {
            name: ICEBERG_ROW_IDENTITY_FILE_COLUMN.to_string(),
            data_type: arrow::datatypes::DataType::Utf8,
            nullable: false,
            write_default: None,
            logical_type: None,
        },
        ColumnDef {
            name: ICEBERG_ROW_IDENTITY_POS_COLUMN.to_string(),
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        },
    ];
    let schema = metadata.current_schema();
    for field in metadata.default_partition_spec().fields() {
        let source = schema.field_by_id(field.source_id).ok_or_else(|| {
            format!(
                "[UnsupportedPositionDeleteDescriptor] iceberg position-delete sink partition source field id {} not found",
                field.source_id
            )
        })?;
        let column = target_columns
            .iter()
            .find(|column| column.name.eq_ignore_ascii_case(&source.name))
            .ok_or_else(|| {
                format!(
                    "[UnsupportedPositionDeleteDescriptor] iceberg position-delete sink partition source column `{}` not found in target table",
                    source.name
                )
            })?;
        columns.push(column.clone());
    }
    Ok(columns)
}

fn build_position_delete_output_descriptor(
    metadata: &iceberg::spec::TableMetadata,
    target_columns: &[ColumnDef],
) -> Result<PositionDeleteDescriptorInput, String> {
    let schema = metadata.current_schema();
    let partition_source_fields = metadata
        .default_partition_spec()
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| {
            let source = schema.field_by_id(field.source_id).ok_or_else(|| {
                format!(
                    "[UnsupportedPositionDeleteDescriptor] iceberg position-delete sink partition source field id {} not found",
                    field.source_id
                )
            })?;
            let column = target_columns
                .iter()
                .find(|column| column.name.eq_ignore_ascii_case(&source.name))
                .ok_or_else(|| {
                    format!(
                        "[UnsupportedPositionDeleteDescriptor] iceberg position-delete sink partition source column `{}` not found in target table",
                        source.name
                    )
                })?;
            let output_expr_index = i32::try_from(idx + 2).map_err(|_| {
                "[UnsupportedPositionDeleteDescriptor] position-delete partition source index overflow"
                    .to_string()
            })?;
            Ok(PositionDeletePartitionSourceField {
                output_expr_index: usize::try_from(output_expr_index).map_err(|_| {
                    "[UnsupportedPositionDeleteDescriptor] position-delete partition source index overflow"
                        .to_string()
                })?,
                source_column_name: source.name.clone(),
                partition_field_name: field.name.clone(),
                transform_expr: transform_to_sink_string(&field.transform),
                source_field_id: field.source_id,
                data_type: column.data_type.clone(),
            })
        })
        .collect::<Result<Vec<_>, String>>()?;

    Ok(PositionDeleteDescriptorInput {
        file_path: PositionDeleteOutputField {
            output_expr_index: 0,
            name: ICEBERG_POSITION_DELETE_FILE_PATH_COLUMN.to_string(),
            data_type: arrow::datatypes::DataType::Utf8,
            field_id: ICEBERG_POSITION_DELETE_FILE_PATH_FIELD_ID,
        },
        pos: PositionDeleteOutputField {
            output_expr_index: 1,
            name: ICEBERG_POSITION_DELETE_POS_COLUMN.to_string(),
            data_type: arrow::datatypes::DataType::Int64,
            field_id: ICEBERG_POSITION_DELETE_POS_FIELD_ID,
        },
        partition_source_fields,
        target_partition_spec_id: metadata.default_partition_spec_id(),
    })
}

fn append_source_to_query(
    source: &InsertSource,
    insert_columns: &[String],
    target_columns: &[ColumnDef],
) -> Result<sqlparser::ast::Query, String> {
    append_source_to_query_for_write(source, insert_columns, target_columns, target_columns)
}

fn append_source_to_query_for_write(
    source: &InsertSource,
    insert_columns: &[String],
    source_columns: &[ColumnDef],
    write_columns: &[ColumnDef],
) -> Result<sqlparser::ast::Query, String> {
    match source {
        InsertSource::FromQuery(query)
            if insert_columns.is_empty() && same_column_sequence(source_columns, write_columns) =>
        {
            Ok((**query).clone())
        }
        InsertSource::FromQuery(query) => wrap_insert_query_with_write_projection(
            query,
            insert_columns,
            source_columns,
            write_columns,
        ),
        InsertSource::Values(rows) => values_append_source_to_query_for_write(
            rows,
            insert_columns,
            source_columns,
            write_columns,
        ),
        InsertSource::SelectLiteralRow(row) => values_append_source_to_query_for_write(
            std::slice::from_ref(row),
            insert_columns,
            source_columns,
            write_columns,
        ),
        InsertSource::UnionAll(_) => {
            Err("iceberg INSERT append does not support UNION ALL sources on this path".to_string())
        }
    }
}

fn wrap_insert_query_with_write_projection(
    query: &sqlparser::ast::Query,
    insert_columns: &[String],
    source_columns: &[ColumnDef],
    write_columns: &[ColumnDef],
) -> Result<sqlparser::ast::Query, String> {
    let insert_idx_by_target = if insert_columns.is_empty() {
        std::collections::HashMap::new()
    } else {
        insert_column_index_by_target_name(insert_columns, write_columns)?
    };
    let source_alias = "__nr_insert_src";
    let mut projection = Vec::with_capacity(write_columns.len());
    for (write_idx, column) in write_columns.iter().enumerate() {
        let target_name = novarocks_catalog::identifier::normalize_identifier(&column.name)?;
        let expr = if let Some(source_idx) = insert_idx_by_target.get(&target_name) {
            let source_expr = format!(
                "{}.{}",
                sql_identifier(source_alias),
                sql_identifier(&insert_columns[*source_idx])
            );
            target_cast_expr_sql(&source_expr, column)?
        } else if insert_columns.is_empty() {
            if let Some(source_idx) =
                source_index_for_write_column(column, write_idx, source_columns, write_columns)
            {
                let source_expr = format!(
                    "{}.{}",
                    sql_identifier(source_alias),
                    sql_identifier(&source_columns[source_idx].name)
                );
                target_cast_expr_sql(&source_expr, column)?
            } else {
                target_cast_expr_sql(&omitted_column_expr_sql(column)?, column)?
            }
        } else {
            target_cast_expr_sql(&omitted_column_expr_sql(column)?, column)?
        };
        projection.push(format!("{expr} AS {}", sql_identifier(&column.name)));
    }
    let alias_source_columns = if insert_columns.is_empty() {
        source_columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>()
    } else {
        insert_columns
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
    };
    let alias_columns = alias_source_columns
        .into_iter()
        .map(|column| sql_identifier(column))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT {} FROM ({}) AS {} ({})",
        projection.join(", "),
        query,
        sql_identifier(source_alias),
        alias_columns
    );
    parse_generated_query(&sql, "append INSERT SELECT projection")
}

fn values_append_source_to_query_for_write(
    rows: &[Vec<Literal>],
    insert_columns: &[String],
    source_columns: &[ColumnDef],
    write_columns: &[ColumnDef],
) -> Result<sqlparser::ast::Query, String> {
    let insert_idx_by_target = if insert_columns.is_empty() {
        std::collections::HashMap::new()
    } else {
        insert_column_index_by_target_name(insert_columns, write_columns)?
    };
    let rendered_rows = rows
        .iter()
        .map(|row| {
            if insert_columns.is_empty() {
                if row.len() != source_columns.len() {
                    return Err(format!(
                        "insert column count mismatch: expected {} values, got {}",
                        source_columns.len(),
                        row.len()
                    ));
                }
            } else if row.len() != insert_columns.len() {
                return Err(format!(
                    "insert column count mismatch: expected {} values for column list, got {}",
                    insert_columns.len(),
                    row.len()
                ));
            }
            let values = write_columns
                .iter()
                .enumerate()
                .map(|(write_idx, column)| {
                    if insert_columns.is_empty() {
                        if let Some(literal) = source_index_for_write_column(
                            column,
                            write_idx,
                            source_columns,
                            write_columns,
                        )
                        .and_then(|source_idx| row.get(source_idx))
                        {
                            target_literal_expr_sql(literal, column)
                        } else {
                            target_cast_expr_sql(&omitted_column_expr_sql(column)?, column)
                        }
                    } else {
                        let target_name =
                            novarocks_catalog::identifier::normalize_identifier(&column.name)?;
                        if let Some(literal) = insert_idx_by_target
                            .get(&target_name)
                            .and_then(|source_idx| row.get(*source_idx))
                        {
                            target_literal_expr_sql(literal, column)
                        } else {
                            target_cast_expr_sql(&omitted_column_expr_sql(column)?, column)
                        }
                    }
                })
                .collect::<Result<Vec<_>, _>>()?
                .join(", ");
            Ok(format!("({values})"))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let sql = format!("VALUES {}", rendered_rows.join(", "));
    parse_generated_query(&sql, "append INSERT VALUES")
}

fn same_column_sequence(left: &[ColumnDef], right: &[ColumnDef]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right.iter())
            .all(|(l, r)| l.name.eq_ignore_ascii_case(&r.name) && l.data_type == r.data_type)
}

fn source_index_for_write_column(
    write_column: &ColumnDef,
    write_idx: usize,
    source_columns: &[ColumnDef],
    write_columns: &[ColumnDef],
) -> Option<usize> {
    source_columns
        .iter()
        .position(|source| source.name.eq_ignore_ascii_case(&write_column.name))
        .or_else(|| {
            (source_columns.len() == write_columns.len() && write_idx < source_columns.len())
                .then_some(write_idx)
        })
}

pub(crate) fn iceberg_insert_columns_from_schema(
    schema: &iceberg::spec::Schema,
) -> Result<Vec<ColumnDef>, String> {
    let arrow_schema = iceberg::arrow::schema_to_arrow_schema(schema)
        .map_err(|e| format!("convert iceberg insert schema to arrow schema failed: {e}"))?;
    arrow_schema
        .fields()
        .iter()
        .map(|field| {
            let nested = schema
                .field_by_name(field.name())
                .ok_or_else(|| format!("iceberg column `{}` missing from schema", field.name()))?;
            let data_type = match nested.field_type.as_ref() {
                iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Variant) => {
                    arrow::datatypes::DataType::LargeBinary
                }
                iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Binary) => {
                    arrow::datatypes::DataType::Binary
                }
                _ => field.data_type().clone(),
            };
            Ok(ColumnDef {
                name: field.name().clone(),
                data_type,
                nullable: field.is_nullable(),
                write_default: nested
                    .write_default
                    .as_ref()
                    .map(|literal| {
                        crate::connector::iceberg::default_value::iceberg_literal_to_column_default(
                            literal,
                            nested.field_type.as_ref(),
                        )
                        .map_err(|e| {
                            format!(
                                "convert Iceberg insert write-default for column `{}` failed: {e}",
                                field.name()
                            )
                        })
                    })
                    .transpose()?,
                logical_type: None,
            })
        })
        .collect()
}

fn insert_column_index_by_target_name(
    insert_columns: &[String],
    target_columns: &[ColumnDef],
) -> Result<std::collections::HashMap<String, usize>, String> {
    let mut target_names = std::collections::HashSet::with_capacity(target_columns.len());
    for column in target_columns {
        target_names.insert(novarocks_catalog::identifier::normalize_identifier(
            &column.name,
        )?);
    }

    let mut mapping = std::collections::HashMap::with_capacity(insert_columns.len());
    for (idx, column) in insert_columns.iter().enumerate() {
        let normalized = novarocks_catalog::identifier::normalize_identifier(column)?;
        if !target_names.contains(&normalized) {
            return Err(format!("unknown INSERT column `{column}`"));
        }
        if mapping.insert(normalized.clone(), idx).is_some() {
            return Err(format!("duplicate INSERT column `{column}`"));
        }
    }
    Ok(mapping)
}

fn omitted_column_expr_sql(column: &ColumnDef) -> Result<String, String> {
    let Some(write_default) = &column.write_default else {
        return Ok("NULL".to_string());
    };
    let sql_type = arrow_data_type_to_sql_type(&column.data_type)?;
    let literal = crate::sql::literal::column_default_to_ast_literal(write_default, &sql_type)?;
    literal_to_sql_for_arrow_type(&literal, &column.data_type)
}

fn target_literal_expr_sql(literal: &Literal, column: &ColumnDef) -> Result<String, String> {
    target_cast_expr_sql(
        &literal_to_sql_for_arrow_type(literal, &column.data_type)?,
        column,
    )
}

pub(crate) fn target_cast_expr_sql(expr_sql: &str, column: &ColumnDef) -> Result<String, String> {
    Ok(format!(
        "CAST({expr_sql} AS {})",
        arrow_data_type_to_sql_type_name(&column.data_type)?
    ))
}

fn parse_generated_query(sql: &str, context: &str) -> Result<sqlparser::ast::Query, String> {
    match crate::sql::parser::parse_sql_raw(sql)? {
        sqlparser::ast::Statement::Query(query) => Ok(*query),
        other => Err(format!("{context}: generated non-query statement: {other}")),
    }
}

fn sql_identifier(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

fn literal_to_sql(literal: &Literal) -> Result<String, String> {
    Ok(match literal {
        Literal::Null => "NULL".to_string(),
        Literal::Bool(value) => {
            if *value {
                "TRUE".to_string()
            } else {
                "FALSE".to_string()
            }
        }
        Literal::Int(value) => value.to_string(),
        Literal::Float(value) => {
            if !value.is_finite() {
                return Err(format!(
                    "non-finite floating literal is not supported: {value}"
                ));
            }
            value.to_string()
        }
        Literal::String(value) | Literal::Date(value) => single_quoted_sql(value),
        Literal::Array(items) => format!(
            "[{}]",
            items
                .iter()
                .map(literal_to_sql)
                .collect::<Result<Vec<_>, _>>()?
                .join(", ")
        ),
        Literal::Map(entries) => {
            let mut args = Vec::with_capacity(entries.len() * 2);
            for (key, value) in entries {
                args.push(literal_to_sql(key)?);
                args.push(literal_to_sql(value)?);
            }
            format!("map({})", args.join(", "))
        }
        Literal::Struct(values) => format!(
            "row({})",
            values
                .iter()
                .map(literal_to_sql)
                .collect::<Result<Vec<_>, _>>()?
                .join(", ")
        ),
    })
}

pub(crate) fn literal_to_sql_for_arrow_type(
    literal: &Literal,
    data_type: &arrow::datatypes::DataType,
) -> Result<String, String> {
    use arrow::datatypes::DataType;

    match (literal, data_type) {
        (
            Literal::String(value) | Literal::Date(value),
            DataType::Binary | DataType::LargeBinary,
        ) => {
            let bytes = crate::sql::literal::latin1_string_to_bytes(value)?;
            Ok(format!("X'{}'", hex::encode_upper(bytes)))
        }
        (Literal::Array(items), DataType::List(item_field)) => {
            let values = items
                .iter()
                .map(|item| literal_to_sql_for_arrow_type(item, item_field.data_type()))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(format!("[{}]", values.join(", ")))
        }
        (Literal::Map(entries), DataType::Map(entries_field, _)) => {
            let DataType::Struct(fields) = entries_field.data_type() else {
                return literal_to_sql(literal);
            };
            if fields.len() != 2 {
                return literal_to_sql(literal);
            }
            let mut args = Vec::with_capacity(entries.len() * 2);
            for (key, value) in entries {
                args.push(literal_to_sql_for_arrow_type(key, fields[0].data_type())?);
                args.push(literal_to_sql_for_arrow_type(value, fields[1].data_type())?);
            }
            Ok(format!("map({})", args.join(", ")))
        }
        (Literal::Struct(values), DataType::Struct(fields)) if values.len() == fields.len() => {
            let values = values
                .iter()
                .zip(fields.iter())
                .map(|(value, field)| literal_to_sql_for_arrow_type(value, field.data_type()))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(format!("row({})", values.join(", ")))
        }
        _ => literal_to_sql(literal),
    }
}

fn single_quoted_sql(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len() + 2);
    for ch in value.chars() {
        match ch {
            '\'' => escaped.push_str("''"),
            '\\' => escaped.push_str(r"\\"),
            _ => escaped.push(ch),
        }
    }
    format!("'{escaped}'")
}

fn arrow_data_type_to_sql_type(dt: &arrow::datatypes::DataType) -> Result<SqlType, String> {
    use arrow::datatypes::{DataType, TimeUnit};
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
        DataType::Date32 => SqlType::Date,
        DataType::Timestamp(TimeUnit::Nanosecond, _) => SqlType::DateTimeNs,
        DataType::Timestamp(TimeUnit::Microsecond, _) => SqlType::DateTime,
        DataType::Time64(TimeUnit::Microsecond | TimeUnit::Nanosecond) => SqlType::Time,
        DataType::Binary => SqlType::Binary,
        DataType::LargeBinary => SqlType::Variant,
        DataType::List(element_field) => SqlType::Array(Box::new(arrow_data_type_to_sql_type(
            element_field.data_type(),
        )?)),
        DataType::Map(entries_field, _) => {
            let DataType::Struct(fields) = entries_field.data_type() else {
                return Err(format!("unsupported Arrow map entries type: {dt:?}"));
            };
            if fields.len() != 2 {
                return Err(format!("unsupported Arrow map entries field count: {dt:?}"));
            }
            SqlType::Map(
                Box::new(arrow_data_type_to_sql_type(fields[0].data_type())?),
                Box::new(arrow_data_type_to_sql_type(fields[1].data_type())?),
            )
        }
        DataType::Struct(fields) => SqlType::Struct(
            fields
                .iter()
                .map(|field| {
                    Ok((
                        field.name().clone(),
                        arrow_data_type_to_sql_type(field.data_type())?,
                    ))
                })
                .collect::<Result<Vec<_>, String>>()?,
        ),
        other => {
            return Err(format!(
                "unsupported Arrow type for INSERT default conversion: {other:?}"
            ));
        }
    })
}

fn arrow_data_type_to_sql_type_name(dt: &arrow::datatypes::DataType) -> Result<String, String> {
    sql_type_name(&arrow_data_type_to_sql_type(dt)?)
}

fn sql_type_name(sql_type: &SqlType) -> Result<String, String> {
    Ok(match sql_type {
        SqlType::TinyInt => "TINYINT".to_string(),
        SqlType::SmallInt => "SMALLINT".to_string(),
        SqlType::Int => "INT".to_string(),
        SqlType::BigInt => "BIGINT".to_string(),
        SqlType::LargeInt => "LARGEINT".to_string(),
        SqlType::Float => "FLOAT".to_string(),
        SqlType::Double => "DOUBLE".to_string(),
        SqlType::Decimal { precision, scale } => format!("DECIMAL({precision}, {scale})"),
        SqlType::String => "STRING".to_string(),
        SqlType::Json => "JSON".to_string(),
        SqlType::Binary => "VARBINARY".to_string(),
        SqlType::Bitmap => "BITMAP".to_string(),
        SqlType::Hll => "HLL".to_string(),
        SqlType::Boolean => "BOOLEAN".to_string(),
        SqlType::Date => "DATE".to_string(),
        SqlType::DateTime => "DATETIME".to_string(),
        SqlType::DateTimeNs => "DATETIME_NS".to_string(),
        SqlType::Time => "TIME".to_string(),
        SqlType::Array(inner) => format!("ARRAY<{}>", sql_type_name(inner)?),
        SqlType::Map(key, value) => {
            format!("MAP<{}, {}>", sql_type_name(key)?, sql_type_name(value)?)
        }
        SqlType::Struct(fields) => format!(
            "STRUCT<{}>",
            fields
                .iter()
                .map(|(name, ty)| Ok(format!("{} {}", sql_identifier(name), sql_type_name(ty)?)))
                .collect::<Result<Vec<_>, String>>()?
                .join(", ")
        ),
        SqlType::Variant => "VARIANT".to_string(),
    })
}

fn commit_op_kind_for_overwrite_mode(
    overwrite_mode: crate::sql::parser::ast::OverwriteMode,
) -> CommitOpKind {
    use crate::sql::parser::ast::OverwriteMode;
    match overwrite_mode {
        OverwriteMode::DynamicPartitions => CommitOpKind::OverwritePartitions,
        OverwriteMode::FullTable => CommitOpKind::Overwrite,
        OverwriteMode::None => CommitOpKind::FastAppend,
    }
}

fn operation_kind_for_commit_op_kind(kind: CommitOpKind) -> IcebergOperationKind {
    match kind {
        CommitOpKind::FastAppend => IcebergOperationKind::InsertAppend,
        CommitOpKind::Overwrite | CommitOpKind::OverwritePartitions => {
            IcebergOperationKind::InsertOverwrite
        }
        _ => IcebergOperationKind::Maintenance,
    }
}

fn write_base_snapshot_id(
    metadata: &iceberg::spec::TableMetadata,
    target_ref: &str,
) -> Result<Option<i64>, String> {
    if target_ref == "main" {
        return Ok(metadata.current_snapshot().map(|s| s.snapshot_id()));
    }
    metadata
        .refs()
        .get(target_ref)
        .map(|snapshot_ref| Some(snapshot_ref.snapshot_id))
        .ok_or_else(|| format!("iceberg ref: branch '{target_ref}' not found in table metadata"))
}

pub(crate) fn invalidate_iceberg_caches(
    state: &Arc<StandaloneState>,
    target: &TargetBackend,
) -> Result<(), String> {
    {
        let registry = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        let entry = registry.get(&target.catalog)?;
        entry.invalidate_table_cache(&target.namespace, &target.table);
    }
    state
        .catalog_service
        .invalidate_table(&target.catalog, &target.namespace, &target.table)
}

fn target_string(t: &TargetBackend) -> String {
    format!("{}.{}.{}", t.catalog, t.namespace, t.table)
}

pub(crate) fn data_file_to_written_file(
    df: &DataFile,
    partition_spec_id: i32,
) -> Result<WrittenFile, String> {
    Ok(WrittenFile {
        path: df.file_path().to_string(),
        format: df.file_format(),
        content: df.content_type(),
        partition_values: df.partition().clone(),
        partition_spec_id,
        record_count: df.record_count(),
        file_size_in_bytes: df.file_size_in_bytes(),
        split_offsets: df.split_offsets().map(|s| s.to_vec()).unwrap_or_default(),
        column_sizes: df.column_sizes().clone(),
        value_counts: df.value_counts().clone(),
        null_value_counts: df.null_value_counts().clone(),
        nan_value_counts: df.nan_value_counts().clone(),
        lower_bounds: df.lower_bounds().clone(),
        upper_bounds: df.upper_bounds().clone(),
        key_metadata: df.key_metadata().map(|s| s.to_vec()),
        referenced_data_file: df.referenced_data_file().map(|s| s.to_string()),
        equality_ids: df.equality_ids(),
        first_row_id: df.first_row_id(),
        content_offset: None,
        content_size_in_bytes: None,
        cardinality: None,
    })
}

pub(crate) fn run_select_to_chunks(
    state: &Arc<StandaloneState>,
    target: &TargetBackend,
    query: &sqlparser::ast::Query,
) -> Result<Vec<Chunk>, String> {
    // Pass `current_catalog` when the target is an iceberg table so that
    // 1-part and 2-part table references in the SELECT (e.g. `db.table`)
    // resolve against the active catalog.
    let current_catalog = if target.backend_name == "iceberg" && !target.catalog.is_empty() {
        Some(target.catalog.as_str())
    } else {
        None
    };

    let result = crate::engine::execute_query_with_catalog_service(
        state,
        current_catalog,
        &target.namespace,
        query,
        None,
    )?;
    query_result_to_chunks(result)
}

/// Like [`run_select_to_chunks`], but also returns the output schema columns
/// from the query plan. The schema is always populated even when the SELECT
/// produces zero rows — callers that need the column types for schema inference
/// (e.g. CTAS) should use this instead of `run_select_to_chunks`.
pub(crate) fn run_select_to_chunks_and_schema(
    state: &Arc<StandaloneState>,
    target: &TargetBackend,
    query: &sqlparser::ast::Query,
) -> Result<
    (
        Vec<Chunk>,
        Vec<crate::runtime::query_result::QueryResultColumn>,
    ),
    String,
> {
    // CTAS context: SELECT may reference iceberg tables (1-part or 2-part
    // names). Passing Some(target.catalog) routes unqualified refs to iceberg,
    // mirroring the standalone server's SELECT path.
    let current_catalog = if target.backend_name == "iceberg" && !target.catalog.is_empty() {
        Some(target.catalog.as_str())
    } else {
        None
    };
    let result = crate::engine::execute_query_with_catalog_service(
        state,
        current_catalog,
        &target.namespace,
        query,
        None,
    )?;
    let schema_cols = result.columns.clone();
    let chunks = query_result_to_chunks(result)?;
    Ok((chunks, schema_cols))
}

pub(crate) struct AbortCleanupOperator {
    pub(crate) fs: opendal::Operator,
    pub(crate) path_mapper: Option<CleanupPathMapper>,
}

pub(crate) fn build_abort_cleanup_for_catalog_entry(
    entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
) -> Result<AbortCleanupOperator, String> {
    if let Some(s3_config) = entry.object_store_config() {
        let access = crate::connector::iceberg::fs_io::resolve_access_for_location(
            &entry.warehouse_uri,
            Some(s3_config),
        )
        .map_err(|e| format!("resolve warehouse URI for iceberg abort cleanup: {e}"))?;
        let bucket = access
            .handle()
            .authority()
            .ok_or_else(|| {
                format!(
                    "resolve warehouse URI for iceberg abort cleanup missing bucket: {}",
                    entry.warehouse_uri
                )
            })?
            .to_string();
        let fs = access.operator();
        let mapper: CleanupPathMapper = Arc::new(move |path| {
            crate::fs::access::parse_object_store_path_parse_only(path)
                .ok()
                .and_then(|(actual_bucket, key)| {
                    if actual_bucket == bucket {
                        Some(key)
                    } else {
                        None
                    }
                })
                .unwrap_or_else(|| path.to_string())
        });
        return Ok(AbortCleanupOperator {
            fs,
            path_mapper: Some(mapper),
        });
    }

    let fs = crate::fs::local::build_fs_operator("/")
        .map_err(|e| format!("build local-FS operator failed: {e}"))?;
    let mapper: CleanupPathMapper =
        Arc::new(|path: &str| path.strip_prefix("file://").unwrap_or(path).to_string());
    Ok(AbortCleanupOperator {
        fs,
        path_mapper: Some(mapper),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Fields, TimeUnit};
    use sqlparser::ast as sqlast;

    use novarocks_catalog::schema::ColumnDefault;

    fn test_column(
        name: &str,
        data_type: DataType,
        write_default: Option<ColumnDefault>,
    ) -> novarocks_catalog::schema::ColumnDef {
        novarocks_catalog::schema::ColumnDef {
            name: name.to_string(),
            data_type,
            nullable: true,
            write_default,
            logical_type: None,
        }
    }

    fn parse_query(sql: &str) -> sqlast::Query {
        let stmt = crate::sql::parser::parse_sql_raw(sql).expect("parse query");
        let sqlast::Statement::Query(query) = stmt else {
            panic!("expected query statement");
        };
        *query
    }

    fn test_map_type(key: DataType, value: DataType) -> DataType {
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Arc::new(Field::new("key", key, false)),
                    Arc::new(Field::new("value", value, true)),
                ])),
                false,
            )),
            false,
        )
    }

    fn test_struct_type(fields: Vec<(&str, DataType)>) -> DataType {
        DataType::Struct(Fields::from(
            fields
                .into_iter()
                .map(|(name, data_type)| Arc::new(Field::new(name, data_type, true)))
                .collect::<Vec<_>>(),
        ))
    }

    fn test_iceberg_metadata_with_identity_partition(
        source_column_name: &str,
        source_field_id: i32,
        partition_spec_id: i32,
    ) -> iceberg::spec::TableMetadata {
        test_iceberg_metadata_with_partition(
            source_column_name,
            source_column_name,
            iceberg::spec::Transform::Identity,
            source_field_id,
            partition_spec_id,
        )
    }

    fn test_iceberg_metadata_with_partition(
        source_column_name: &str,
        partition_field_name: &str,
        transform: iceberg::spec::Transform,
        source_field_id: i32,
        partition_spec_id: i32,
    ) -> iceberg::spec::TableMetadata {
        let schema = iceberg::spec::Schema::builder()
            .with_fields(vec![
                Arc::new(iceberg::spec::NestedField::required(
                    source_field_id,
                    source_column_name,
                    iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Int),
                )),
                Arc::new(iceberg::spec::NestedField::optional(
                    source_field_id + 1,
                    "v",
                    iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::String),
                )),
            ])
            .build()
            .expect("schema");
        let partition_spec = iceberg::spec::PartitionSpec::builder(Arc::new(schema.clone()))
            .with_spec_id(partition_spec_id)
            .add_partition_field(source_column_name, partition_field_name, transform)
            .expect("partition field")
            .build()
            .expect("partition spec");
        let creation = iceberg::TableCreation::builder()
            .name("t".to_string())
            .location("file:///warehouse/db/t".to_string())
            .schema(schema)
            .partition_spec(partition_spec.into_unbound())
            .format_version(iceberg::spec::FormatVersion::V3)
            .build();
        let metadata = iceberg::spec::TableMetadataBuilder::from_table_creation(creation)
            .expect("metadata builder")
            .build()
            .expect("metadata")
            .metadata;
        retag_test_metadata_partition_source(metadata, source_field_id, partition_spec_id)
    }

    fn retag_test_metadata_partition_source(
        metadata: iceberg::spec::TableMetadata,
        source_field_id: i32,
        partition_spec_id: i32,
    ) -> iceberg::spec::TableMetadata {
        // TableMetadataBuilder::from_table_creation intentionally reassigns
        // field and spec ids; this fixture retags serialized metadata so tests
        // can assert planner descriptors carry target-table ids verbatim.
        let mut value = serde_json::to_value(metadata).expect("metadata json");
        let object = value.as_object_mut().expect("metadata object");
        object.insert(
            "default-spec-id".to_string(),
            serde_json::Value::from(partition_spec_id),
        );
        object.insert(
            "last-column-id".to_string(),
            serde_json::Value::from(source_field_id + 1),
        );

        let schemas = object
            .get_mut("schemas")
            .and_then(serde_json::Value::as_array_mut)
            .expect("schemas array");
        let schema_fields = schemas[0]
            .as_object_mut()
            .and_then(|schema| schema.get_mut("fields"))
            .and_then(serde_json::Value::as_array_mut)
            .expect("schema fields");
        schema_fields[0]
            .as_object_mut()
            .expect("first field")
            .insert("id".to_string(), serde_json::Value::from(source_field_id));
        schema_fields[1]
            .as_object_mut()
            .expect("second field")
            .insert(
                "id".to_string(),
                serde_json::Value::from(source_field_id + 1),
            );

        let specs = object
            .get_mut("partition-specs")
            .and_then(serde_json::Value::as_array_mut)
            .expect("partition specs");
        let spec = specs[0].as_object_mut().expect("partition spec object");
        spec.insert(
            "spec-id".to_string(),
            serde_json::Value::from(partition_spec_id),
        );
        let partition_fields = spec
            .get_mut("fields")
            .and_then(serde_json::Value::as_array_mut)
            .expect("partition fields");
        partition_fields[0]
            .as_object_mut()
            .expect("partition field")
            .insert(
                "source-id".to_string(),
                serde_json::Value::from(source_field_id),
            );

        serde_json::from_value(value).expect("retagged metadata")
    }

    fn test_iceberg_target() -> TargetBackend {
        TargetBackend {
            backend_name: "iceberg",
            catalog: "test_catalog".to_string(),
            namespace: "test_db".to_string(),
            table: "target_orders".to_string(),
        }
    }

    fn test_resolved_table(columns: Vec<ColumnDef>) -> ResolvedTable {
        ResolvedTable {
            catalog: "test_catalog".to_string(),
            namespace: "test_db".to_string(),
            table: "target_orders".to_string(),
            columns,
        }
    }

    fn test_iceberg_table(metadata: iceberg::spec::TableMetadata) -> iceberg::table::Table {
        iceberg::table::Table::builder()
            .identifier(TableIdent::new(
                NamespaceIdent::new("test_db".to_string()),
                "target_orders".to_string(),
            ))
            .file_io(iceberg::io::FileIO::new_with_fs())
            .metadata(metadata)
            .build()
            .expect("iceberg table")
    }

    fn test_iceberg_catalog_entry() -> IcebergCatalogEntry {
        let warehouse = tempfile::TempDir::new().expect("warehouse tempdir");
        crate::connector::iceberg::catalog::registry::build_catalog_entry(
            "test_catalog",
            &[
                ("type".to_string(), "iceberg".to_string()),
                (
                    "iceberg.catalog.warehouse".to_string(),
                    warehouse.path().display().to_string(),
                ),
            ],
        )
        .expect("iceberg catalog entry")
    }

    fn assert_position_delete_output_field(
        field: &crate::connector::iceberg::position_delete_descriptor::PositionDeleteOutputField,
        output_expr_index: i32,
        name: &str,
        data_type: &DataType,
        field_id: i32,
    ) {
        assert_eq!(field.output_expr_index, output_expr_index as usize);
        assert_eq!(field.name, name);
        assert_eq!(&field.data_type, data_type);
        assert_eq!(field.field_id, field_id);
    }

    fn assert_position_delete_descriptor_contract(
        desc: &crate::connector::iceberg::position_delete_descriptor::PositionDeleteDescriptorInput,
    ) {
        use crate::connector::iceberg::position_delete_descriptor::{
            ICEBERG_POSITION_DELETE_FILE_PATH_COLUMN, ICEBERG_POSITION_DELETE_FILE_PATH_FIELD_ID,
            ICEBERG_POSITION_DELETE_POS_COLUMN, ICEBERG_POSITION_DELETE_POS_FIELD_ID,
        };

        assert_eq!(desc.target_partition_spec_id, 7);
        assert_position_delete_output_field(
            &desc.file_path,
            0,
            ICEBERG_POSITION_DELETE_FILE_PATH_COLUMN,
            &DataType::Utf8,
            ICEBERG_POSITION_DELETE_FILE_PATH_FIELD_ID,
        );
        assert_position_delete_output_field(
            &desc.pos,
            1,
            ICEBERG_POSITION_DELETE_POS_COLUMN,
            &DataType::Int64,
            ICEBERG_POSITION_DELETE_POS_FIELD_ID,
        );
        let partition_field = desc
            .partition_source_fields
            .first()
            .expect("partition source field");
        assert_eq!(partition_field.output_expr_index, 2);
        assert_eq!(partition_field.source_column_name, "id");
        assert_eq!(partition_field.partition_field_name, "id_bucket");
        assert_eq!(partition_field.transform_expr, "bucket[8]");
        assert_eq!(partition_field.source_field_id, 42);
        assert_eq!(partition_field.data_type, DataType::Int32);
    }

    #[test]
    fn arrow_data_type_to_sql_type_accepts_time64_for_insert_defaults() {
        assert_eq!(
            arrow_data_type_to_sql_type(&DataType::Time64(TimeUnit::Microsecond)).expect("type"),
            novarocks_catalog::schema::SqlType::Time
        );
    }

    #[test]
    fn position_delete_sink_descriptor_columns_use_target_table_schema() {
        let resolved_columns = vec![
            test_column("id", DataType::Int32, None),
            test_column("v", DataType::Utf8, None),
        ];
        let sink_input_columns = vec![
            test_column("_file", DataType::Utf8, None),
            test_column("_pos", DataType::Int64, None),
        ];

        let descriptor_columns = write_sink_target_descriptor_columns(
            IcebergWriteSinkMode::PositionDeletes,
            &resolved_columns,
            &sink_input_columns,
        )
        .expect("descriptor columns");

        assert_eq!(
            descriptor_columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["id", "v"]
        );
    }

    #[test]
    fn position_delete_sink_spec_carries_descriptor() {
        let resolved_columns = vec![
            test_column("id", DataType::Int32, None),
            test_column("v", DataType::Utf8, None),
        ];
        let metadata = test_iceberg_metadata_with_partition(
            "id",
            "id_bucket",
            iceberg::spec::Transform::Bucket(8),
            42,
            7,
        );
        let target = test_iceberg_target();
        let resolved = test_resolved_table(resolved_columns);
        let table = test_iceberg_table(metadata);
        let entry = test_iceberg_catalog_entry();
        let spec = build_position_delete_sink_spec(&target, &resolved, &table, &entry)
            .expect("position delete sink spec");
        let desc = spec
            .position_delete_output_descriptor
            .as_ref()
            .expect("descriptor");
        assert_position_delete_descriptor_contract(desc);
    }

    #[test]
    fn position_delete_sink_spec_rejects_missing_partition_source() {
        let resolved_columns = vec![test_column("v", DataType::Utf8, None)];
        let metadata = test_iceberg_metadata_with_identity_partition("id", 42, 7);
        let target = test_iceberg_target();
        let resolved = test_resolved_table(resolved_columns);
        let table = test_iceberg_table(metadata);
        let entry = test_iceberg_catalog_entry();
        let err = build_position_delete_sink_spec(&target, &resolved, &table, &entry).unwrap_err();

        assert!(
            err.contains("[UnsupportedPositionDeleteDescriptor]"),
            "{err}"
        );
        assert!(err.contains("partition source column `id`"), "{err}");
    }

    #[test]
    fn row_lineage_data_sink_descriptor_columns_use_sink_input_schema() {
        let resolved_columns = vec![test_column("id", DataType::Int32, None)];
        let sink_input_columns = row_lineage_data_sink_input_columns(&resolved_columns);

        let descriptor_columns = write_sink_target_descriptor_columns(
            IcebergWriteSinkMode::RowLineageData,
            &resolved_columns,
            &sink_input_columns,
        )
        .expect("descriptor columns");

        assert_eq!(
            descriptor_columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["id", "_row_id", "_last_updated_sequence_number"]
        );
    }

    #[test]
    fn append_source_to_query_values_reorders_columns_and_fills_defaults() {
        let target_columns = vec![
            test_column("a", DataType::Int32, None),
            test_column("b", DataType::Int32, Some(ColumnDefault::Int32(5))),
            test_column("c", DataType::Int32, None),
        ];
        let source = InsertSource::Values(vec![vec![
            crate::sql::parser::ast::Literal::Int(30),
            crate::sql::parser::ast::Literal::Int(10),
        ]]);

        let query = append_source_to_query(
            &source,
            &["c".to_string(), "a".to_string()],
            &target_columns,
        )
        .expect("append source query");

        let sqlast::SetExpr::Values(values) = query.body.as_ref() else {
            panic!("expected VALUES query, got: {query}");
        };
        let row = values.rows.first().expect("one row");
        let rendered: Vec<String> = row.iter().map(ToString::to_string).collect();
        assert_eq!(
            rendered,
            vec!["CAST(10 AS INT)", "CAST(5 AS INT)", "CAST(30 AS INT)"]
        );
    }

    #[test]
    fn omitted_column_expr_characterizes_neutral_write_defaults() {
        let full_binary = (0_u16..=255).map(|byte| byte as u8).collect::<Vec<_>>();
        let full_binary_sql = format!(
            "X'{}'",
            (0_u16..=255)
                .map(|byte| format!("{byte:02X}"))
                .collect::<String>()
        );
        let cases = vec![
            (
                "integer",
                DataType::Int32,
                Some(ColumnDefault::Int32(5)),
                "5".to_string(),
            ),
            (
                "string",
                DataType::Utf8,
                Some(ColumnDefault::String("value".to_string())),
                "'value'".to_string(),
            ),
            (
                "decimal",
                DataType::Decimal128(10, 2),
                Some(ColumnDefault::Decimal {
                    unscaled: 12_345,
                    precision: 10,
                    scale: 2,
                }),
                "'123.45'".to_string(),
            ),
            (
                "date",
                DataType::Date32,
                Some(ColumnDefault::Date {
                    days_since_epoch: -1,
                }),
                "'1969-12-31'".to_string(),
            ),
            (
                "datetime",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                Some(ColumnDefault::TimestampMicros {
                    micros_since_epoch: 1_704_110_400_123_456,
                }),
                "'2024-01-01 12:00:00'".to_string(),
            ),
            (
                "datetime-ns",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                Some(ColumnDefault::TimestampNanos {
                    nanos_since_epoch: 1_704_164_645_123_456_789,
                }),
                "'2024-01-02 03:04:05.123456789'".to_string(),
            ),
            (
                "binary",
                DataType::Binary,
                Some(ColumnDefault::Binary(full_binary)),
                full_binary_sql,
            ),
            (
                "empty-array",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                Some(ColumnDefault::Array(Vec::new())),
                "[]".to_string(),
            ),
            (
                "empty-map",
                test_map_type(DataType::Int32, DataType::Utf8),
                Some(ColumnDefault::Map(Vec::new())),
                "map()".to_string(),
            ),
            ("missing", DataType::Int32, None, "NULL".to_string()),
        ];

        for (name, data_type, write_default, expected) in cases {
            let column = test_column(name, data_type, write_default);
            assert_eq!(
                omitted_column_expr_sql(&column),
                Ok(expected),
                "case={name}"
            );
        }
    }

    #[test]
    fn omitted_column_expr_characterizes_non_empty_collection_default_errors() {
        let list_column = test_column(
            "items",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            Some(ColumnDefault::Array(vec![ColumnDefault::Int32(1)])),
        );
        assert_eq!(
            omitted_column_expr_sql(&list_column).unwrap_err(),
            "non-empty ARRAY write-default is not yet supported (1 elements)"
        );

        let map_column = test_column(
            "attributes",
            test_map_type(DataType::Int32, DataType::Utf8),
            Some(ColumnDefault::Map(vec![(
                ColumnDefault::Int32(1),
                ColumnDefault::String("value".to_string()),
            )])),
        );
        assert_eq!(
            omitted_column_expr_sql(&map_column).unwrap_err(),
            "non-empty MAP write-default is not yet supported (1 entries)"
        );
    }

    #[test]
    fn append_source_to_query_values_casts_literals_to_target_types() {
        let target_columns = vec![
            test_column("id", DataType::Int64, None),
            test_column("region", DataType::Utf8, None),
            test_column("amount", DataType::Float64, None),
        ];
        let source = InsertSource::Values(vec![
            vec![
                crate::sql::parser::ast::Literal::Int(1),
                crate::sql::parser::ast::Literal::String("us".to_string()),
                crate::sql::parser::ast::Literal::Float(10.5),
            ],
            vec![
                crate::sql::parser::ast::Literal::Int(2),
                crate::sql::parser::ast::Literal::String("eu".to_string()),
                crate::sql::parser::ast::Literal::Float(20.0),
            ],
        ]);

        let query =
            append_source_to_query(&source, &[], &target_columns).expect("append source query");

        let sqlast::SetExpr::Values(values) = query.body.as_ref() else {
            panic!("expected VALUES query, got: {query}");
        };
        let first_row: Vec<String> = values.rows[0].iter().map(ToString::to_string).collect();
        let second_row: Vec<String> = values.rows[1].iter().map(ToString::to_string).collect();
        assert_eq!(
            first_row,
            vec![
                "CAST(1 AS BIGINT)",
                "CAST('us' AS STRING)",
                "CAST(10.5 AS DOUBLE)"
            ]
        );
        assert_eq!(
            second_row,
            vec![
                "CAST(2 AS BIGINT)",
                "CAST('eu' AS STRING)",
                "CAST(20 AS DOUBLE)"
            ]
        );
    }

    #[test]
    fn append_source_to_query_values_does_not_position_fill_added_middle_column() {
        let source_columns = vec![
            test_column("id", DataType::Int32, None),
            test_column("amount", DataType::Int32, None),
        ];
        let write_columns = vec![
            test_column("id", DataType::Int32, None),
            test_column("category", DataType::Utf8, None),
            test_column("amount", DataType::Int32, None),
        ];
        let source = InsertSource::Values(vec![vec![
            crate::sql::parser::ast::Literal::Int(1),
            crate::sql::parser::ast::Literal::Int(10),
        ]]);

        let query = append_source_to_query_for_write(&source, &[], &source_columns, &write_columns)
            .expect("append source query");

        let sqlast::SetExpr::Values(values) = query.body.as_ref() else {
            panic!("expected VALUES query, got: {query}");
        };
        let row: Vec<String> = values.rows[0].iter().map(ToString::to_string).collect();
        assert_eq!(
            row,
            vec!["CAST(1 AS INT)", "CAST(NULL AS STRING)", "CAST(10 AS INT)"]
        );
    }

    #[test]
    fn append_source_to_query_values_preserves_backslash_string_literals() {
        let target_columns = vec![test_column("region", DataType::Utf8, None)];
        let source = InsertSource::Values(vec![vec![crate::sql::parser::ast::Literal::String(
            r"e\f".to_string(),
        )]]);

        let query =
            append_source_to_query(&source, &[], &target_columns).expect("append source query");

        let sqlast::SetExpr::Values(values) = query.body.as_ref() else {
            panic!("expected VALUES query, got: {query}");
        };
        let sqlast::Expr::Cast { expr, .. } = &values.rows[0][0] else {
            panic!("expected CAST expression");
        };
        let sqlast::Expr::Value(value) = expr.as_ref() else {
            panic!("expected string literal inside CAST");
        };
        let sqlast::Value::SingleQuotedString(s) = &value.value else {
            panic!("expected single-quoted string");
        };
        assert_eq!(s, r"e\f");
    }

    #[test]
    fn append_source_to_query_values_renders_binary_literals_as_hex() {
        let target_columns = vec![test_column("payload", DataType::Binary, None)];
        let packed = crate::sql::literal::bytes_to_latin1_string(&[0xab, 0x01]);
        let source =
            InsertSource::Values(vec![vec![crate::sql::parser::ast::Literal::String(packed)]]);

        let query =
            append_source_to_query(&source, &[], &target_columns).expect("append source query");

        let sqlast::SetExpr::Values(values) = query.body.as_ref() else {
            panic!("expected VALUES query, got: {query}");
        };
        let sqlast::Expr::Cast { expr, .. } = &values.rows[0][0] else {
            panic!("expected CAST expression");
        };
        let sqlast::Expr::Value(value) = expr.as_ref() else {
            panic!("expected hex literal inside CAST");
        };
        let sqlast::Value::HexStringLiteral(s) = &value.value else {
            panic!("expected hex literal");
        };
        assert_eq!(s, "AB01");
    }

    #[test]
    fn target_cast_expr_sql_renders_large_binary_as_variant() {
        let column = test_column("v", DataType::LargeBinary, None);

        let sql = target_cast_expr_sql("X'AB01'", &column).expect("cast sql");

        assert_eq!(sql, "CAST(X'AB01' AS VARIANT)");
    }

    #[test]
    fn append_source_to_query_values_rejects_column_list_width_mismatch() {
        let target_columns = vec![
            test_column("a", DataType::Int32, None),
            test_column("b", DataType::Int32, None),
        ];
        let source = InsertSource::Values(vec![vec![
            crate::sql::parser::ast::Literal::Int(1),
            crate::sql::parser::ast::Literal::Int(2),
        ]]);

        let err = append_source_to_query(&source, &["a".to_string()], &target_columns)
            .expect_err("extra value must be rejected");
        assert!(
            err.contains("expected 1 values for column list, got 2"),
            "got: {err}"
        );
    }

    #[test]
    fn append_source_to_query_from_query_column_list_wraps_projection() {
        let target_columns = vec![
            test_column("a", DataType::Int32, None),
            test_column("b", DataType::Int32, Some(ColumnDefault::Int32(7))),
            test_column("c", DataType::Int32, None),
        ];
        let source = InsertSource::FromQuery(Box::new(parse_query("SELECT x, y FROM src")));

        let query = append_source_to_query(
            &source,
            &["c".to_string(), "a".to_string()],
            &target_columns,
        )
        .expect("append source query");

        let rendered = query.to_string();
        assert!(
            rendered.contains("FROM (SELECT x, y FROM src) AS `__nr_insert_src` (`c`, `a`)"),
            "derived query should carry source column aliases, got: {rendered}"
        );
        assert!(
            rendered.starts_with(
                "SELECT CAST(`__nr_insert_src`.`a` AS INT) AS `a`, CAST(7 AS INT) AS `b`, CAST(`__nr_insert_src`.`c` AS INT) AS `c`"
            ),
            "projection should target table column order, got: {rendered}"
        );
    }

    #[test]
    fn append_source_to_query_from_query_omitted_complex_columns_parse() {
        let target_columns = vec![
            test_column("k1", DataType::Int64, None),
            test_column(
                "c_map",
                test_map_type(DataType::Int32, DataType::Int32),
                None,
            ),
            test_column(
                "c_struct",
                test_struct_type(vec![("k1", DataType::Int32), ("k2", DataType::Int32)]),
                None,
            ),
        ];
        let source = InsertSource::FromQuery(Box::new(parse_query(
            "SELECT idx FROM row_util ORDER BY idx LIMIT 1000",
        )));

        let query = append_source_to_query(&source, &["k1".to_string()], &target_columns)
            .expect("append source query");
        let rendered = query.to_string();

        assert!(
            rendered.contains("CAST(NULL AS MAP"),
            "omitted map column should be cast from NULL once, got: {rendered}"
        );
        assert!(
            rendered.contains("CAST(NULL AS STRUCT"),
            "omitted struct column should be cast from NULL once, got: {rendered}"
        );
        assert!(
            !rendered.contains("CAST(CAST(NULL"),
            "omitted complex columns must not produce nested casts, got: {rendered}"
        );
    }

    #[test]
    fn insert_writer_columns_follow_fresh_iceberg_schema_after_external_evolution() {
        let schema = iceberg::spec::Schema::builder()
            .with_fields(vec![
                std::sync::Arc::new(iceberg::spec::NestedField::required(
                    1,
                    "amount",
                    iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long),
                )),
                std::sync::Arc::new(iceberg::spec::NestedField::required(
                    2,
                    "id",
                    iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Int),
                )),
                std::sync::Arc::new(iceberg::spec::NestedField::optional(
                    3,
                    "category",
                    iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::String),
                )),
            ])
            .build()
            .expect("schema");

        let columns = iceberg_insert_columns_from_schema(&schema).expect("columns");
        let names = columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>();
        let types = columns
            .iter()
            .map(|column| column.data_type.clone())
            .collect::<Vec<_>>();
        let nullable = columns
            .iter()
            .map(|column| column.nullable)
            .collect::<Vec<_>>();

        assert_eq!(names, vec!["amount", "id", "category"]);
        assert_eq!(
            types,
            vec![DataType::Int64, DataType::Int32, DataType::Utf8]
        );
        assert_eq!(nullable, vec![false, false, true]);
    }
}
