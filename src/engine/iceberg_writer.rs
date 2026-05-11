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
//! Routes from `insert_flow::run_insert` for any iceberg target whose source
//! is `FromQuery`, plus all iceberg targets when `overwrite = true`.
//!
//! Phase 1 scope (per spec §0.4):
//! * `INSERT INTO iceberg ... SELECT ...` — handled here.
//! * `INSERT OVERWRITE iceberg ... SELECT ...` — handled here.
//! * `INSERT INTO iceberg VALUES (...)` — keeps using the existing fast-append
//!   helper at `connector::iceberg::catalog::registry::insert_rows`.
//! * `INSERT OVERWRITE iceberg VALUES (...)` — rejected with a clear error;
//!   future Phase 1.x can lift this if the use case arises.

use std::sync::Arc;

use iceberg::Catalog;
use iceberg::spec::DataFile;
use iceberg::{NamespaceIdent, TableIdent};

use crate::connector::backend::ResolvedTable;
use crate::connector::iceberg::catalog::registry::{block_on_iceberg, build_iceberg_catalog};
use crate::connector::iceberg::commit::{
    CleanupPathMapper, CommitOpKind, IcebergCommitCollector, RunInput, WrittenFile,
    ensure_iceberg_write_supported, ensure_no_equality_deletes,
    ensure_no_variant_columns_for_row_level_mutation, ensure_overwrite_single_partition_spec,
    run_iceberg_commit,
};
use crate::connector::starrocks::managed::mv_refresh::query_result_to_chunks;
use crate::connector::starrocks::managed::mv_refresh_iceberg::write_chunks_as_iceberg_data_files;
use crate::engine::backend_resolver::TargetBackend;
use crate::engine::{StandaloneState, StatementResult};
use crate::exec::chunk::Chunk;
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

    // 4. Write data files. Empty input → no-op for INSERT INTO; for OVERWRITE
    //    an empty SELECT means "clear the table" so we still go through
    //    OverwriteCommit which handles the empty-written + non-empty-base case.
    let data_files: Vec<DataFile> = if chunks.iter().all(|c| c.batch.num_rows() == 0) {
        Vec::new()
    } else {
        block_on_iceberg(async { write_chunks_as_iceberg_data_files(&table, &chunks).await })??
    };

    // 5. Build the collector and inject every written file.
    let metadata = table.metadata();
    let staging_dir = format!(
        "{}/data/_staging/{}",
        metadata.location(),
        uuid::Uuid::new_v4()
    );
    let collector = Arc::new(IcebergCommitCollector::new(
        match overwrite_mode {
            OverwriteMode::DynamicPartitions => CommitOpKind::OverwritePartitions,
            OverwriteMode::FullTable => CommitOpKind::Overwrite,
            OverwriteMode::None => CommitOpKind::FastAppend,
        },
        table_ident,
        metadata.current_snapshot().map(|s| s.snapshot_id()),
        metadata.last_sequence_number(),
        metadata.current_schema().clone(),
        metadata.default_partition_spec().clone(),
        staging_dir,
        crate::common::types::UniqueId { hi: 0, lo: 0 },
    ));
    let default_spec_id = metadata.default_partition_spec_id();
    for df in data_files {
        let wf = data_file_to_written_file(&df, default_spec_id)?;
        collector.inject_written_file(wf);
    }

    // 6. Build the OpenDAL Operator + FileIO.
    let abort_cleanup = build_abort_cleanup_for_catalog_entry(&entry)?;
    let file_io = table.file_io().clone();

    // 7. Drive commit + abort cleanup on failure.
    let _outcome = block_on_iceberg(async {
        run_iceberg_commit(RunInput {
            collector: collector.clone(),
            catalog: catalog.clone(),
            table,
            fs: abort_cleanup.fs,
            file_io,
            cleanup_path_mapper: abort_cleanup.path_mapper,
            cow_update_rewrite: None,
            target_ref: target_ref.to_string(),
        })
        .await
    })??;

    // 8. Invalidate the iceberg entry's table cache so subsequent SELECTs
    //    see the new snapshot. The standalone catalog rebuilds its TableDef
    //    on the next register_iceberg_tables_for_query call.
    invalidate_iceberg_caches(state, target)?;

    Ok(StatementResult::Ok)
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
    {
        let mut local = state
            .catalog
            .write()
            .map_err(|e| format!("standalone catalog write lock: {e}"))?;
        let _ = local.drop_table(&target.namespace, &target.table);
    }
    Ok(())
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
    // Force-refresh every iceberg table referenced by the SELECT. The
    // simpler `register_iceberg_tables_for_query` skips already-registered
    // tables, but the table backing the INSERT may have been mutated by a
    // prior statement in the same session and the cached `TableDef` would
    // miss the new files. Refreshing here is mandatory before running the
    // SELECT so it sees all data files committed up to this point.
    crate::engine::query_prep::refresh_external_tables_for_query(
        state,
        None,
        &target.namespace,
        query,
    )?;

    // The SELECT may use 3-part `catalog.database.table` names (the INSERT
    // target itself uses one). Strip the catalog prefix before analysis so
    // we feed the analyzer 2-part names — it does not understand catalog-
    // qualified references on its own. This mirrors the standalone SELECT
    // dispatcher's handling of three-part names.
    let mut rewritten = query.clone();
    crate::sql::parser::query_refs::strip_catalog_from_three_part_names(&mut rewritten);

    // Clone-then-release: do not hold `state.catalog.read()` across
    // `execute_query`. The call drives a pipeline that may run for many
    // seconds; concurrent writers (e.g. INSERT cleanup taking
    // `state.catalog.write()` in `invalidate_iceberg_caches`) would
    // otherwise block indefinitely on the std::sync::RwLock writer queue.
    let catalog_snapshot = state
        .catalog
        .read()
        .expect("standalone catalog read lock")
        .clone();
    let result = crate::engine::execute_query(
        &rewritten,
        &catalog_snapshot,
        &target.namespace,
        state.exchange_port,
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
    // names) that need registration into the standalone in-memory catalog
    // before planning. resolve_table_target dispatches between iceberg and
    // managed-lake based on whether current_catalog is supplied: passing
    // Some(target.catalog) routes the unqualified refs to iceberg, mirroring
    // the standalone server's SELECT path (engine/mod.rs:611).
    let current_catalog = if target.backend_name == "iceberg" && !target.catalog.is_empty() {
        Some(target.catalog.as_str())
    } else {
        None
    };
    crate::engine::query_prep::refresh_external_tables_for_query(
        state,
        current_catalog,
        &target.namespace,
        query,
    )?;
    let mut rewritten = query.clone();
    crate::sql::parser::query_refs::strip_catalog_from_three_part_names(&mut rewritten);
    // Clone-then-release: same rationale as `run_select_to_chunks` above.
    let catalog_snapshot = state
        .catalog
        .read()
        .expect("standalone catalog read lock")
        .clone();
    let result = crate::engine::execute_query(
        &rewritten,
        &catalog_snapshot,
        &target.namespace,
        state.exchange_port,
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
