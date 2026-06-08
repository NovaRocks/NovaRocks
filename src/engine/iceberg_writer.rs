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
use std::future::Future;
use std::sync::Arc;

use iceberg::Catalog;
use iceberg::spec::DataFile;
use iceberg::{NamespaceIdent, TableIdent};

use crate::connector::backend::ResolvedTable;
use crate::connector::iceberg::catalog::registry::{block_on_iceberg, build_iceberg_catalog};
use crate::connector::iceberg::commit::{
    CleanupPathMapper, CommitOpKind, CommitOutcome, CommitServiceError, IcebergCommitCollector,
    WrittenFile, ensure_iceberg_write_supported, ensure_no_equality_deletes,
    ensure_no_variant_columns_for_row_level_mutation, ensure_overwrite_single_partition_spec,
};
use crate::connector::iceberg::data_writer::write_record_batches_as_data_files;
use crate::connector::starrocks::table::mv_refresh::query_result_to_chunks;
use crate::engine::backend_resolver::TargetBackend;
use crate::engine::write_transaction::{
    IcebergWriteCommitExecutor, IcebergWriteCommitPolicy, IcebergWriteSource,
    IcebergWriteTransactionExecutor, IcebergWriteTransactionRunner, IcebergWriteTransactionSpec,
    IcebergWriteValidationPolicy, synthetic_write_commit_input,
};
use crate::engine::{StandaloneState, StatementResult};
use crate::exec::chunk::Chunk;
use crate::meta::repository::iceberg_operation::{IcebergOperationKind, IcebergOperationTarget};
use crate::runtime::coordinator::CoordinatedQueryResult;
use crate::runtime::query_result::QueryResult;
use crate::runtime::write_coordinator::WriteCommitInput;
use crate::sql::parser::ast::InsertSource;

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
        ensure_no_variant_columns_for_row_level_mutation(&table)
            .map_err(|e| format!("INSERT OVERWRITE: {e}"))?;
        ensure_overwrite_single_partition_spec(&table)?;
        ensure_no_equality_deletes(&table)?;
    }
    if overwrite_partitions {
        // OVERWRITE PARTITIONS shares the variant-write restriction with
        // full-table OVERWRITE (#87 spec). Then check the partition-table
        // requirement; v3 row-lineage + cross-historical-spec checks happen
        // in OverwritePartitionsCommit.
        ensure_no_variant_columns_for_row_level_mutation(&table)
            .map_err(|e| format!("INSERT OVERWRITE PARTITIONS: {e}"))?;
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

    // 3. Produce chunks from the source.
    //    - FromQuery: execute the SELECT and collect the result chunks.
    //    - Values / SelectLiteralRow: build a RecordBatch from the literal rows
    //      using the iceberg table schema, then wrap it as a single Chunk.
    //      This supports branch-qualified INSERT INTO t.branch_dev VALUES (...).
    let chunks: Vec<Chunk> = match source {
        InsertSource::FromQuery(query) => run_select_to_chunks(state, target, query)?,
        InsertSource::Values(rows) => {
            let loaded = load_iceberg_table_for_literals(state, target)?;
            let batch =
                crate::connector::iceberg::catalog::registry::build_insert_batch(&loaded, rows)?;
            vec![crate::engine::record_batch_to_chunk(batch)?]
        }
        InsertSource::SelectLiteralRow(row) => {
            let loaded = load_iceberg_table_for_literals(state, target)?;
            let batch = crate::connector::iceberg::catalog::registry::build_insert_batch(
                &loaded,
                std::slice::from_ref(row),
            )?;
            vec![crate::engine::record_batch_to_chunk(batch)?]
        }
        InsertSource::UnionAll(_) => {
            unreachable!("rejected above")
        }
    };

    // 3.5. If the user specified an explicit column list, reorder columns and
    //      fill omitted columns with their write_default literal (or NULL).
    let chunks = if insert_columns.is_empty() {
        chunks
    } else {
        align_chunks_to_target_schema(chunks, insert_columns, &resolved.columns)?
    };

    // 4. Build the collector. The runner creates the operation record before
    //    the executor writes data files, then the executor injects files into
    //    this collector before commit.
    let metadata = table.metadata();
    let commit_op_kind = commit_op_kind_for_overwrite_mode(overwrite_mode);
    let base_snapshot_id = write_base_snapshot_id(metadata, target_ref)?;
    let base_sequence_number = metadata.last_sequence_number();
    let current_schema = metadata.current_schema().clone();
    let default_partition_spec = metadata.default_partition_spec().clone();
    let default_spec_id = metadata.default_partition_spec_id();
    let staging_dir = format!(
        "{}/data/_staging/{}",
        metadata.location(),
        uuid::Uuid::new_v4()
    );
    let collector = Arc::new(IcebergCommitCollector::new(
        commit_op_kind,
        table_ident.clone(),
        base_snapshot_id,
        base_sequence_number,
        current_schema,
        default_partition_spec,
        staging_dir,
        crate::common::types::UniqueId { hi: 0, lo: 0 },
    ));

    // 5. Build the OpenDAL Operator and transaction runner.
    let abort_cleanup = build_abort_cleanup_for_catalog_entry(&entry)?;
    let commit_executor = IcebergWriteCommitExecutor {
        state: Arc::clone(state),
        target: target.clone(),
        catalog: Arc::clone(&catalog),
        table: table.clone(),
        collector: Arc::clone(&collector),
        fs: abort_cleanup.fs,
        cleanup_path_mapper: abort_cleanup.path_mapper,
        cow_update_rewrite: None,
        target_ref: target_ref.to_string(),
        snapshot_properties: BTreeMap::new(),
    };
    let executor = InsertOrOverwriteWriteExecutor {
        commit_executor,
        table,
        chunks,
        collector,
        default_spec_id,
        should_commit_empty_input: !matches!(overwrite_mode, OverwriteMode::None),
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

struct InsertOrOverwriteWriteExecutor {
    commit_executor: IcebergWriteCommitExecutor,
    table: iceberg::table::Table,
    chunks: Vec<Chunk>,
    collector: Arc<IcebergCommitCollector>,
    default_spec_id: i32,
    should_commit_empty_input: bool,
}

impl IcebergWriteTransactionExecutor for InsertOrOverwriteWriteExecutor {
    fn run_coordinated_write(
        &self,
        _spec: &IcebergWriteTransactionSpec,
    ) -> Result<CoordinatedQueryResult, String> {
        let has_rows = self.chunks.iter().any(|c| c.batch.num_rows() > 0);
        if !has_rows && !self.should_commit_empty_input {
            return Ok(CoordinatedQueryResult {
                query_result: QueryResult::empty(),
                write_commit: None,
                write_abort: None,
            });
        }

        if has_rows {
            let write_table = self.table.clone();
            let write_chunks = self.chunks.clone();
            let data_files = run_data_file_write_phase_on_sink_io(
                write_chunks_as_iceberg_data_files_owned(write_table, write_chunks),
            )?;
            for df in data_files {
                let wf = data_file_to_written_file(&df, self.default_spec_id)?;
                self.collector.inject_written_file(wf);
            }
            inject_theta_sketches(&self.collector, &self.chunks);
        }

        Ok(CoordinatedQueryResult {
            query_result: QueryResult::empty(),
            write_commit: Some(synthetic_write_commit_input()),
            write_abort: None,
        })
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

fn inject_theta_sketches(collector: &IcebergCommitCollector, chunks: &[Chunk]) {
    // Compute Theta sketches from the source chunks and push them through the
    // collector so the commit action can register Puffin NDV statistics. We
    // emit one sketch set per chunk; StatsAssembler unions sketches across
    // sets per field id, so per-file attribution is not required to get
    // accurate aggregate NDV.
    for (idx, chunk) in chunks.iter().enumerate() {
        if chunk.batch.num_rows() == 0 {
            continue;
        }
        if let Some(sketches) =
            crate::connector::iceberg::sink::compute_theta_sketches_for_batch(&chunk.batch)
        {
            collector.inject_sketch_set(
                crate::connector::iceberg::stats_assembler::FileSketchSet {
                    file_path: format!("standalone_insert_chunk_{idx}"),
                    sketches,
                },
            );
        }
    }
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
    crate::engine::query_prep::invalidate_catalog_mgr_table(
        state,
        &target.catalog,
        &target.namespace,
        &target.table,
    )
}

fn target_string(t: &TargetBackend) -> String {
    format!("{}.{}.{}", t.catalog, t.namespace, t.table)
}

/// Load the iceberg table metadata as an `IcebergLoadedTable` for use by the
/// literal-row (VALUES) branch of the insert path. This provides the schema
/// information needed by `build_insert_batch`.
fn load_iceberg_table_for_literals(
    state: &Arc<StandaloneState>,
    target: &TargetBackend,
) -> Result<crate::connector::iceberg::catalog::registry::IcebergLoadedTable, String> {
    let registry = state
        .iceberg_catalogs
        .read()
        .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
    let entry = registry.get(&target.catalog)?;
    crate::connector::iceberg::catalog::registry::load_table(
        &entry,
        &target.namespace,
        &target.table,
    )
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
        lower_bounds: df.lower_bounds().clone(),
        upper_bounds: df.upper_bounds().clone(),
        key_metadata: df.key_metadata().map(|s| s.to_vec()),
        referenced_data_file: df.referenced_data_file().map(|s| s.to_string()),
        equality_ids: df.equality_ids(),
        first_row_id: df.first_row_id(),
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

    let result = crate::engine::execute_query_with_catalog_mgr(
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
    let result = crate::engine::execute_query_with_catalog_mgr(
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

fn align_chunks_to_target_schema(
    chunks: Vec<Chunk>,
    insert_columns: &[String],
    target_columns: &[crate::sql::catalog::ColumnDef],
) -> Result<Vec<Chunk>, String> {
    use crate::connector::iceberg::default_value::literal_to_constant_array;
    use crate::engine::catalog::normalize_identifier;
    use std::collections::HashMap;
    use std::sync::Arc;

    let normalized_insert: Vec<String> = insert_columns
        .iter()
        .map(|c| normalize_identifier(c))
        .collect::<Result<Vec<_>, _>>()?;
    let mut insert_idx_by_name: HashMap<String, usize> = HashMap::new();
    for (i, name) in normalized_insert.iter().enumerate() {
        if insert_idx_by_name.insert(name.clone(), i).is_some() {
            return Err(format!("duplicate INSERT column `{name}`"));
        }
    }

    let mut aligned = Vec::with_capacity(chunks.len());
    for chunk in chunks {
        let row_count = chunk.batch.num_rows();
        let source_schema = chunk.batch.schema();
        if source_schema.fields().len() != insert_columns.len() {
            return Err(format!(
                "INSERT column-list length {} does not match SELECT projection length {}",
                insert_columns.len(),
                source_schema.fields().len()
            ));
        }
        let mut columns: Vec<arrow::array::ArrayRef> = Vec::with_capacity(target_columns.len());
        let mut fields: Vec<arrow::datatypes::FieldRef> = Vec::with_capacity(target_columns.len());
        for column in target_columns {
            let normalized = normalize_identifier(&column.name)?;
            if let Some(insert_idx) = insert_idx_by_name.get(&normalized) {
                let field = source_schema.field(*insert_idx);
                columns.push(chunk.batch.column(*insert_idx).clone());
                fields.push(Arc::new(arrow::datatypes::Field::new(
                    column.name.clone(),
                    field.data_type().clone(),
                    field.is_nullable(),
                )));
            } else {
                let array = match &column.write_default {
                    Some(iceberg_lit) => {
                        literal_to_constant_array(iceberg_lit, &column.data_type, row_count)?
                    }
                    None => arrow::array::new_null_array(&column.data_type, row_count),
                };
                fields.push(Arc::new(arrow::datatypes::Field::new(
                    column.name.clone(),
                    column.data_type.clone(),
                    column.nullable,
                )));
                columns.push(array);
            }
        }
        let schema = Arc::new(arrow::datatypes::Schema::new(fields));
        let batch = arrow::record_batch::RecordBatch::try_new(schema, columns)
            .map_err(|e| format!("align INSERT batch: {e}"))?;
        aligned.push(crate::engine::record_batch_to_chunk(batch)?);
    }
    Ok(aligned)
}

pub(crate) fn build_abort_cleanup_for_catalog_entry(
    entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
) -> Result<AbortCleanupOperator, String> {
    if let Some(s3_config) = entry.object_store_config() {
        let fs = crate::fs::object_store::build_oss_operator(s3_config)
            .map_err(|e| format!("build S3 operator for iceberg abort cleanup: {e}"))?;
        let bucket = s3_config.bucket.clone();
        let mapper: CleanupPathMapper = Arc::new(move |path| {
            crate::connector::iceberg::catalog::add_files::parse_s3_path(path)
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

    let builder = opendal::services::Fs::default().root("/");
    let fs = opendal::Operator::new(builder)
        .map_err(|e| format!("build local-FS operator failed: {e}"))?
        .finish();
    let mapper: CleanupPathMapper =
        Arc::new(|path: &str| path.strip_prefix("file://").unwrap_or(path).to_string());
    Ok(AbortCleanupOperator {
        fs,
        path_mapper: Some(mapper),
    })
}

fn run_data_file_write_phase_on_sink_io<F, T>(future: F) -> Result<T, String>
where
    F: Future<Output = Result<T, String>> + Send + 'static,
    T: Send + 'static,
{
    let sink_io = crate::runtime::execution_services::execution_services()?
        .sink_io()
        .clone();
    let join = sink_io.spawn(future);
    futures::executor::block_on(join)
        .map_err(|e| format!("standalone iceberg data-file write task join failed: {e}"))?
}

async fn write_chunks_as_iceberg_data_files_owned(
    table: iceberg::table::Table,
    chunks: Vec<Chunk>,
) -> Result<Vec<DataFile>, String> {
    let batches = chunks.into_iter().map(|chunk| chunk.batch);
    write_record_batches_as_data_files(&table, batches).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_file_write_phase_helper_runs_on_sink_io_runtime() {
        let thread_name = run_data_file_write_phase_on_sink_io(async {
            Ok::<_, String>(
                std::thread::current()
                    .name()
                    .map(|s| s.to_string())
                    .unwrap_or_default(),
            )
        })
        .expect("sink_io write phase");

        assert!(
            thread_name.contains("novarocks-sink-io"),
            "data-file write phase ran on unexpected thread: {thread_name}"
        );
    }

    #[test]
    fn data_file_write_phase_helper_preserves_write_error() {
        let err = run_data_file_write_phase_on_sink_io(async {
            Err::<(), String>("write failed".into())
        })
        .expect_err("write error should propagate");

        assert_eq!(err, "write failed");
    }
}
