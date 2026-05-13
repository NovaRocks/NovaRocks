use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, Int64Array, StringArray};
use arrow::compute::{cast, concat_batches, filter_record_batch};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use iceberg::Catalog;
use iceberg::arrow::{ArrowReaderBuilder, schema_to_arrow_schema};

use crate::connector::iceberg::catalog::registry::{block_on_iceberg, build_hadoop_catalog};
use crate::connector::iceberg::commit::{
    CommitOpKind, CowUpdateRewriteSet, CowUpdateTouchedFile, IcebergCommitCollector,
    IcebergUpdateMode, RunInput, ensure_no_variant_columns_for_row_level_mutation,
    run_iceberg_commit, select_iceberg_update_mode,
};
use crate::connector::iceberg::data_writer::{RowLineageColumns, RowLineageWriteBatch};
use crate::engine::{StandaloneState, StatementResult};
use crate::sql::analyzer::iceberg_ref::{IcebergRefSuffix, split_ref_suffix};
use crate::sql::parser::ast::{
    MergeMatchedAction, MergeNotMatchedAction, MergeStmt, ObjectName, UpdateStmt,
};

pub(crate) fn execute_update_statement(
    state: &Arc<StandaloneState>,
    stmt: &UpdateStmt,
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

    let target = crate::engine::backend_resolver::resolve_existing_table_target(
        state,
        table_name,
        current_catalog,
        current_database,
    )?;
    if target.backend_name != "iceberg" {
        return Err(format!(
            "UPDATE only supports iceberg backends, got `{}`",
            target.backend_name
        ));
    }

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

    // Reject variant tables before any planning. Without this guard the
    // failure surfaces deep inside `materialize_update_matches` as a
    // planner error about the row-lineage `__nr_t` column.
    ensure_no_variant_columns_for_row_level_mutation(&table).map_err(|e| format!("UPDATE: {e}"))?;

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

    let target_columns = iceberg_table_columns(&table)?;
    let partition_columns = iceberg_partition_source_columns(&table)?;
    validate_update_assignments(&stmt.assignments, &target_columns, &partition_columns)?;

    let matched = materialize_update_matches(state, &target, stmt, current_catalog)?;
    if matched.row_ids.is_empty() {
        return Ok(StatementResult::Ok);
    }
    validate_unique_target_row_ids(&matched.row_ids)?;

    let mode = select_iceberg_update_mode(&table)?;
    match mode {
        IcebergUpdateMode::CopyOnWrite => execute_cow_update(
            state,
            &target,
            catalog,
            table_ident,
            table,
            &matched,
            entry,
            &target_ref,
        ),
        IcebergUpdateMode::MergeOnRead => execute_mor_update(
            state,
            &target,
            catalog,
            table_ident,
            table,
            &matched,
            entry,
            &target_ref,
        ),
    }
}

fn materialize_update_matches(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    stmt: &UpdateStmt,
    current_catalog: Option<&str>,
) -> Result<MatchedUpdateBatch, String> {
    let target_alias = stmt.alias.as_deref().unwrap_or("__nr_t");
    // The match SELECT runs against the standalone analyzer with
    // `current_database = target.namespace` (so 1-part target name resolves
    // to the iceberg target). Source relations may live in a different
    // namespace; `mutation_source_to_sql` qualifies them with their
    // namespace so the analyzer can find them.
    let target_sql = format!("{} AS {}", target.table, target_alias);
    let assignments_sql = stmt
        .assignments
        .iter()
        .map(|assignment| (assignment.column.as_str(), assignment.value.to_string()))
        .collect::<Vec<_>>();
    let assignments_sql = assignments_sql
        .iter()
        .map(|(column, expr)| (*column, expr.as_str()))
        .collect::<Vec<_>>();
    let where_sql = stmt.where_clause.as_ref().map(|expr| expr.to_string());
    let source_sql = mutation_source_to_sql(state, &stmt.source, current_catalog, target)?;
    let match_sql = build_update_match_query_sql(
        &target_sql,
        target_alias,
        source_sql.as_deref(),
        &assignments_sql,
        where_sql.as_deref(),
    );
    execute_update_match_query(state, Some(&target.catalog), &match_sql, &target.namespace)
}

fn mutation_source_to_sql(
    state: &Arc<StandaloneState>,
    source: &Option<crate::sql::parser::ast::MutationSource>,
    current_catalog: Option<&str>,
    target: &crate::engine::backend_resolver::TargetBackend,
) -> Result<Option<String>, String> {
    match source {
        None => Ok(None),
        Some(source) => {
            mutation_source_relation_to_sql(state, source, current_catalog, target).map(Some)
        }
    }
}

fn mutation_source_relation_to_sql(
    state: &Arc<StandaloneState>,
    source: &crate::sql::parser::ast::MutationSource,
    current_catalog: Option<&str>,
    target: &crate::engine::backend_resolver::TargetBackend,
) -> Result<String, String> {
    use crate::sql::parser::ast::MutationSource;
    match source {
        MutationSource::Table { name, alias } => {
            // The match SELECT runs with `current_database = target.namespace`
            // and `current_catalog = Some(target.catalog)`. Resolve the source
            // against the user's surface name to get its concrete (catalog,
            // namespace, table). Emit a 1-part name when the source shares the
            // target's namespace+catalog (lets refresh follow the
            // current-catalog path), and a 2-part `<namespace>.<table>` name
            // otherwise so the standalone analyzer can find it directly.
            let resolved = crate::engine::backend_resolver::resolve_existing_table_target(
                state,
                name,
                current_catalog,
                &target.namespace,
            )?;
            let mut sql =
                if resolved.catalog == target.catalog && resolved.namespace == target.namespace {
                    resolved.table.clone()
                } else {
                    format!("{}.{}", resolved.namespace, resolved.table)
                };
            if let Some(alias) = alias {
                sql.push_str(" AS ");
                sql.push_str(alias);
            }
            Ok(sql)
        }
        MutationSource::Query { query, alias } => {
            let alias = alias
                .as_deref()
                .ok_or_else(|| "MERGE/UPDATE subquery source requires an alias".to_string())?;
            Ok(format!("({query}) AS {alias}"))
        }
    }
}

fn execute_mor_update(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    catalog: Arc<dyn Catalog>,
    table_ident: iceberg::TableIdent,
    table: iceberg::table::Table,
    matched: &MatchedUpdateBatch,
    entry: crate::connector::iceberg::catalog::IcebergCatalogEntry,
    target_ref: &str,
) -> Result<StatementResult, String> {
    // For branch DML, read partition metadata at the branch head snapshot.
    let read_snapshot_id: Option<i64> = if target_ref != "main" {
        crate::engine::delete_flow::resolve_branch_head_snapshot_id(table.metadata(), target_ref)?
    } else {
        table.metadata().current_snapshot().map(|s| s.snapshot_id())
    };
    let referenced_partitions =
        crate::engine::delete_flow::load_referenced_data_file_partitions_at(
            &table,
            read_snapshot_id,
        )?;
    let delete_groups = build_position_delete_groups_from_matched(matched, &referenced_partitions)?;

    let metadata = table.metadata();
    let new_sequence_number = metadata.last_sequence_number() + 1;
    let runs = build_mor_update_runs(matched, new_sequence_number)?;

    let mut written_files: Vec<crate::connector::iceberg::commit::WrittenFile> = Vec::new();
    for run in &runs {
        let data_files = block_on_iceberg(async {
            crate::connector::iceberg::data_writer::write_row_lineage_batches_as_data_files(
                &table,
                std::slice::from_ref(&run.batch),
            )
            .await
        })??;
        if data_files.is_empty() {
            return Err(
                "MOR UPDATE produced no replacement data files for matched rows".to_string(),
            );
        }
        // Within each contiguous-row-id run we wrote rows in ascending row_id
        // order, so position 0 of the file maps to `run.first_row_id`. Stamp
        // `first_row_id` on the resulting WrittenFile so the manifest entry
        // records the correct lineage origin.
        let mut cursor = run.first_row_id;
        for df in data_files {
            let mut wf = crate::engine::iceberg_writer::data_file_to_written_file(
                &df,
                metadata.default_partition_spec_id(),
            )?;
            wf.first_row_id = Some(cursor);
            cursor = cursor.checked_add(wf.record_count as i64).ok_or_else(|| {
                "MOR UPDATE first_row_id cursor overflow when chaining rolling files".to_string()
            })?;
            written_files.push(wf);
        }
    }

    let staging_dir = format!(
        "{}/data/_staging/{}",
        metadata.location(),
        uuid::Uuid::new_v4()
    );
    let file_io = table.file_io().clone();
    let collector = Arc::new(IcebergCommitCollector::new(
        CommitOpKind::RowDeltaDv,
        table_ident,
        read_snapshot_id,
        metadata.last_sequence_number(),
        metadata.current_schema().clone(),
        metadata.default_partition_spec().clone(),
        staging_dir,
        crate::common::types::UniqueId { hi: 0, lo: 0 },
    ));
    for group in delete_groups {
        collector.inject_delete_group(group);
    }
    for wf in written_files {
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
            target_ref: target_ref.to_string(),
            snapshot_properties: BTreeMap::new(),
        })
        .await
    })??;

    crate::engine::iceberg_writer::invalidate_iceberg_caches(state, target)?;
    Ok(StatementResult::Ok)
}

struct MorUpdateRun {
    /// `_row_id` of the first row in the run. The data file written for this
    /// run has `first_row_id = Self::first_row_id`, and rows occupy positions
    /// `0..N` so the reader's `first_row_id + position` formula reconstructs
    /// each row's `_row_id`.
    first_row_id: i64,
    batch: RowLineageWriteBatch,
}

fn build_mor_update_runs(
    matched: &MatchedUpdateBatch,
    new_sequence_number: i64,
) -> Result<Vec<MorUpdateRun>, String> {
    if matched.row_ids.is_empty() {
        return Ok(Vec::new());
    }
    // Sort matched rows by row_id ascending so each contiguous run lays out
    // the new data file in row_id order (required for the read-side fallback
    // formula). Group rows whose row_ids form a contiguous integer sequence
    // into a single data file; non-contiguous gaps start a new file so each
    // file's stored `first_row_id` correctly identifies its starting row.
    let mut order: Vec<usize> = (0..matched.row_ids.len()).collect();
    order.sort_by_key(|&i| matched.row_ids[i]);

    let mut runs = Vec::new();
    let mut current_indices: Vec<usize> = Vec::new();
    let mut prev_row_id: Option<i64> = None;
    for &idx in &order {
        let row_id = matched.row_ids[idx];
        let starts_new_run = match prev_row_id {
            None => true,
            Some(prev)
                if row_id
                    == prev.checked_add(1).ok_or_else(|| {
                        "MOR UPDATE matched _row_id overflow while grouping contiguous runs"
                            .to_string()
                    })? =>
            {
                false
            }
            Some(_) => true,
        };
        if starts_new_run && !current_indices.is_empty() {
            runs.push(materialize_mor_update_run(
                matched,
                &current_indices,
                new_sequence_number,
            )?);
            current_indices.clear();
        }
        current_indices.push(idx);
        prev_row_id = Some(row_id);
    }
    if !current_indices.is_empty() {
        runs.push(materialize_mor_update_run(
            matched,
            &current_indices,
            new_sequence_number,
        )?);
    }
    Ok(runs)
}

fn materialize_mor_update_run(
    matched: &MatchedUpdateBatch,
    indices: &[usize],
    new_sequence_number: i64,
) -> Result<MorUpdateRun, String> {
    let user_batch = take_rows(&matched.new_rows, indices)?;
    let row_ids: Vec<i64> = indices.iter().map(|&i| matched.row_ids[i]).collect();
    let first_row_id = row_ids[0];
    let last_updated: Vec<Option<i64>> = (0..indices.len())
        .map(|_| Some(new_sequence_number))
        .collect();
    let lineage = RowLineageColumns {
        row_ids: Int64Array::from(row_ids),
        last_updated_sequence_numbers: Int64Array::from(last_updated),
    };
    Ok(MorUpdateRun {
        first_row_id,
        batch: RowLineageWriteBatch {
            user_batch,
            lineage,
        },
    })
}

fn take_rows(batch: &RecordBatch, indices: &[usize]) -> Result<RecordBatch, String> {
    if indices.is_empty() {
        return Ok(RecordBatch::new_empty(batch.schema()));
    }
    let idx_array =
        arrow::array::UInt32Array::from(indices.iter().map(|&i| i as u32).collect::<Vec<_>>());
    let mut new_columns = Vec::with_capacity(batch.num_columns());
    for col in batch.columns() {
        let taken = arrow::compute::take(col.as_ref(), &idx_array, None)
            .map_err(|e| format!("take MOR UPDATE rows failed: {e}"))?;
        new_columns.push(taken);
    }
    RecordBatch::try_new(batch.schema(), new_columns)
        .map_err(|e| format!("rebuild MOR UPDATE batch failed: {e}"))
}

fn build_position_delete_groups_from_matched(
    matched: &MatchedUpdateBatch,
    referenced_partitions: &crate::engine::delete_flow::ReferencedDataFilePartitions,
) -> Result<Vec<crate::connector::iceberg::commit::PositionDeleteGroup>, String> {
    let mut by_file: BTreeMap<String, Vec<i64>> = BTreeMap::new();
    for (path, pos) in matched.file_paths.iter().zip(matched.row_positions.iter()) {
        by_file.entry(path.clone()).or_default().push(*pos);
    }
    let mut out = Vec::with_capacity(by_file.len());
    for (file, positions) in by_file {
        let partition = referenced_partitions.get(&file).ok_or_else(|| {
            format!("matched iceberg data file `{file}` is missing partition metadata")
        })?;
        out.push(crate::connector::iceberg::commit::PositionDeleteGroup {
            referenced_data_file: file,
            partition_spec_id: partition.partition_spec_id,
            partition_values: partition.partition_values.clone(),
            positions,
        });
    }
    Ok(out)
}

fn execute_cow_update(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    catalog: Arc<dyn Catalog>,
    table_ident: iceberg::TableIdent,
    table: iceberg::table::Table,
    matched: &MatchedUpdateBatch,
    entry: crate::connector::iceberg::catalog::IcebergCatalogEntry,
    target_ref: &str,
) -> Result<StatementResult, String> {
    let (data_files, rewrite) = block_on_iceberg(async {
        write_cow_update_files(&table, matched, entry.object_store_config(), target_ref).await
    })??;

    if data_files.is_empty() {
        return Ok(StatementResult::Ok);
    }

    let metadata = table.metadata();
    // For branch DML, commit against the branch head snapshot.
    let base_snapshot_id: Option<i64> = if target_ref != "main" {
        crate::engine::delete_flow::resolve_branch_head_snapshot_id(metadata, target_ref)?
    } else {
        metadata.current_snapshot().map(|s| s.snapshot_id())
    };
    let staging_dir = format!(
        "{}/data/_staging/{}",
        metadata.location(),
        uuid::Uuid::new_v4()
    );
    let file_io = table.file_io().clone();
    let collector = Arc::new(IcebergCommitCollector::new(
        CommitOpKind::CowUpdate,
        table_ident,
        base_snapshot_id,
        metadata.last_sequence_number(),
        metadata.current_schema().clone(),
        metadata.default_partition_spec().clone(),
        staging_dir,
        crate::common::types::UniqueId { hi: 0, lo: 0 },
    ));
    for df in data_files {
        collector.inject_written_file(crate::engine::iceberg_writer::data_file_to_written_file(
            &df,
            metadata.default_partition_spec_id(),
        )?);
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
            cow_update_rewrite: Some(rewrite),
            target_ref: target_ref.to_string(),
            snapshot_properties: BTreeMap::new(),
        })
        .await
    })??;

    crate::engine::iceberg_writer::invalidate_iceberg_caches(state, target)?;
    Ok(StatementResult::Ok)
}

struct MatchedUpdateBatch {
    row_ids: Vec<i64>,
    file_paths: Vec<String>,
    row_positions: Vec<i64>,
    old_rows: RecordBatch,
    new_rows: RecordBatch,
}

fn execute_update_match_query(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    sql: &str,
    current_database: &str,
) -> Result<MatchedUpdateBatch, String> {
    let statement = crate::sql::parser::parse_sql_raw(sql)?;
    let sqlparser::ast::Statement::Query(query) = statement else {
        return Err("internal UPDATE match query was not a SELECT".to_string());
    };
    crate::engine::query_prep::refresh_external_tables_for_query(
        state,
        current_catalog,
        current_database,
        &query,
    )?;
    // Clone-then-release: pipeline execution must not hold
    // `state.catalog.read()`. See iceberg_writer::run_select_to_chunks.
    let catalog_snapshot = state
        .catalog
        .read()
        .expect("standalone catalog read lock")
        .clone();
    let result = crate::engine::execute_query(
        &query,
        &catalog_snapshot,
        current_database,
        state.exchange_port,
        None,
    )?;
    matched_update_batch_from_query_result(result)
}

fn matched_update_batch_from_query_result(
    result: crate::engine::QueryResult,
) -> Result<MatchedUpdateBatch, String> {
    let Some(first_chunk) = result.chunks.first() else {
        return empty_matched_update_batch();
    };
    let schema = first_chunk.batch.schema();
    let batches = result
        .chunks
        .iter()
        .map(|chunk| chunk.batch.clone())
        .collect::<Vec<_>>();
    let batch = concat_batches(&schema, batches.iter())
        .map_err(|e| format!("concatenate UPDATE match batches failed: {e}"))?;
    matched_update_batch_from_record_batch(&batch)
}

fn matched_update_batch_from_record_batch(
    batch: &RecordBatch,
) -> Result<MatchedUpdateBatch, String> {
    if batch.num_rows() == 0 {
        return empty_matched_update_batch();
    }

    let file_col = cast(required_column(batch, "__nr_file")?, &DataType::Utf8)
        .map_err(|e| format!("cast __nr_file to Utf8 failed: {e}"))?;
    let pos_col = cast(required_column(batch, "__nr_pos")?, &DataType::Int64)
        .map_err(|e| format!("cast __nr_pos to Int64 failed: {e}"))?;
    let row_id_col = cast(required_column(batch, "__nr_row_id")?, &DataType::Int64)
        .map_err(|e| format!("cast __nr_row_id to Int64 failed: {e}"))?;
    let file_arr = file_col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "__nr_file was not Utf8 after cast".to_string())?;
    let pos_arr = pos_col
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| "__nr_pos was not Int64 after cast".to_string())?;
    let row_id_arr = row_id_col
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| "__nr_row_id was not Int64 after cast".to_string())?;

    let mut file_paths = Vec::with_capacity(batch.num_rows());
    let mut row_positions = Vec::with_capacity(batch.num_rows());
    let mut row_ids = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        if file_arr.is_null(row) || pos_arr.is_null(row) || row_id_arr.is_null(row) {
            return Err("UPDATE match query produced null row identity columns".to_string());
        }
        file_paths.push(file_arr.value(row).to_string());
        row_positions.push(pos_arr.value(row));
        row_ids.push(row_id_arr.value(row));
    }

    let old_indices = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| !field.name().starts_with("__nr_"))
        .map(|(idx, _)| idx)
        .collect::<Vec<_>>();
    let old_fields = old_indices
        .iter()
        .map(|idx| batch.schema().field(*idx).clone())
        .collect::<Vec<_>>();
    let old_schema = Arc::new(Schema::new(old_fields));
    let old_columns = old_indices
        .iter()
        .map(|idx| batch.column(*idx).clone())
        .collect::<Vec<_>>();
    let old_rows = RecordBatch::try_new(old_schema.clone(), old_columns)
        .map_err(|e| format!("build UPDATE old-row batch failed: {e}"))?;

    let mut new_columns = Vec::with_capacity(old_schema.fields().len());
    for (old_idx, field) in old_indices.iter().zip(old_schema.fields().iter()) {
        let new_name = format!("__nr_new_{}", field.name());
        let column = match batch.schema().index_of(&new_name) {
            Ok(idx) => cast(batch.column(idx), field.data_type()).map_err(|e| {
                format!(
                    "cast UPDATE assignment column `{new_name}` to {:?} failed: {e}",
                    field.data_type()
                )
            })?,
            Err(_) => batch.column(*old_idx).clone(),
        };
        new_columns.push(column);
    }
    let new_rows = RecordBatch::try_new(old_schema, new_columns)
        .map_err(|e| format!("build UPDATE new-row batch failed: {e}"))?;

    Ok(MatchedUpdateBatch {
        row_ids,
        file_paths,
        row_positions,
        old_rows,
        new_rows,
    })
}

fn empty_matched_update_batch() -> Result<MatchedUpdateBatch, String> {
    let schema = Arc::new(Schema::empty());
    let empty = RecordBatch::new_empty(schema);
    Ok(MatchedUpdateBatch {
        row_ids: Vec::new(),
        file_paths: Vec::new(),
        row_positions: Vec::new(),
        old_rows: empty.clone(),
        new_rows: empty,
    })
}

fn required_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a ArrayRef, String> {
    let idx = batch
        .schema()
        .index_of(name)
        .map_err(|_| format!("UPDATE match query missing `{name}` column"))?;
    Ok(batch.column(idx))
}

async fn write_cow_update_files(
    table: &iceberg::table::Table,
    matched: &MatchedUpdateBatch,
    object_store_config: Option<&crate::fs::object_store::ObjectStoreConfig>,
    target_ref: &str,
) -> Result<(Vec<iceberg::spec::DataFile>, CowUpdateRewriteSet), String> {
    if matched.row_ids.is_empty() {
        return Ok((Vec::new(), empty_cow_rewrite(table, target_ref)?));
    }
    validate_unique_target_row_ids(&matched.row_ids)?;
    let rewrite_files = build_cow_rewrite_batches(table, matched, object_store_config).await?;
    let mut data_files = Vec::new();
    let mut data_files_by_old_file = Vec::with_capacity(rewrite_files.len());
    for rewrite in &rewrite_files {
        let written =
            crate::connector::iceberg::data_writer::write_row_lineage_batches_as_data_files(
                table,
                std::slice::from_ref(&rewrite.batch),
            )
            .await?;
        if written.is_empty() {
            return Err(format!(
                "COW UPDATE rewrite for data file `{}` produced no data files",
                rewrite.old_file
            ));
        }
        data_files.extend(written.clone());
        data_files_by_old_file.push((rewrite.old_file.clone(), written));
    }
    let rewrite = build_cow_rewrite_set(
        table,
        matched,
        &data_files_by_old_file,
        &rewrite_files,
        target_ref,
    )?;
    Ok((data_files, rewrite))
}

struct CowRewriteFile {
    old_file: String,
    batch: RowLineageWriteBatch,
}

#[derive(Default)]
struct CowRewriteAccumulator {
    pieces: Vec<RecordBatch>,
    row_ids: Vec<i64>,
    last_updated: Vec<Option<i64>>,
}

async fn build_cow_rewrite_batches(
    table: &iceberg::table::Table,
    matched: &MatchedUpdateBatch,
    object_store_config: Option<&crate::fs::object_store::ObjectStoreConfig>,
) -> Result<Vec<CowRewriteFile>, String> {
    let touched_files = matched.file_paths.iter().cloned().collect::<HashSet<_>>();
    let updated_by_row_id = matched
        .row_ids
        .iter()
        .enumerate()
        .map(|(idx, row_id)| (*row_id, idx))
        .collect::<HashMap<_, _>>();
    let existing_deletes_by_file =
        crate::engine::delete_flow::load_existing_delete_visibility_by_data_file(
            table,
            object_store_config,
        )?;
    let lineage_by_file = load_data_file_lineage(table)?;

    let user_schema = Arc::new(
        schema_to_arrow_schema(table.metadata().current_schema())
            .map_err(|e| format!("convert iceberg schema to arrow failed: {e}"))?,
    );
    let mut select_cols = vec!["_file".to_string(), "_pos".to_string()];
    for field in table.metadata().current_schema().as_struct().fields() {
        select_cols.push(field.name.clone());
    }
    let scan = table
        .scan()
        .select(select_cols)
        .build()
        .map_err(|e| format!("build COW UPDATE TableScan failed: {e}"))?;
    let task_stream = scan
        .plan_files()
        .await
        .map_err(|e| format!("COW UPDATE TableScan::plan_files failed: {e}"))?;
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
        .map_err(|e| format!("COW UPDATE ArrowReader::read failed: {e}"))?;

    let new_sequence_number = table.metadata().last_sequence_number() + 1;
    let mut accumulators = BTreeMap::<String, CowRewriteAccumulator>::new();
    let mut seen_updated_row_ids = HashSet::new();

    while let Some(batch_result) = stream.next().await {
        let batch = batch_result.map_err(|e| format!("COW UPDATE scan stream error: {e}"))?;
        collect_cow_rewrite_rows_from_batch(
            &batch,
            &user_schema,
            matched,
            &touched_files,
            &updated_by_row_id,
            &existing_deletes_by_file,
            &lineage_by_file,
            new_sequence_number,
            &mut accumulators,
            &mut seen_updated_row_ids,
        )?;
    }

    for row_id in &matched.row_ids {
        if !seen_updated_row_ids.contains(row_id) {
            return Err(format!(
                "UPDATE matched _row_id={row_id}, but the row was not visible during COW rewrite"
            ));
        }
    }
    if accumulators.is_empty() {
        return Err("COW UPDATE matched rows but produced no replacement rows".to_string());
    }
    let mut out = Vec::with_capacity(accumulators.len());
    for (old_file, acc) in accumulators {
        let user_batch = concat_batches(&user_schema, acc.pieces.iter()).map_err(|e| {
            format!("concatenate COW UPDATE replacement rows for `{old_file}` failed: {e}")
        })?;
        out.push(CowRewriteFile {
            old_file,
            batch: RowLineageWriteBatch {
                user_batch,
                lineage: RowLineageColumns {
                    row_ids: Int64Array::from(acc.row_ids),
                    last_updated_sequence_numbers: Int64Array::from(acc.last_updated),
                },
            },
        });
    }
    Ok(out)
}

#[allow(clippy::too_many_arguments)]
fn collect_cow_rewrite_rows_from_batch(
    batch: &RecordBatch,
    user_schema: &arrow::datatypes::SchemaRef,
    matched: &MatchedUpdateBatch,
    touched_files: &HashSet<String>,
    updated_by_row_id: &HashMap<i64, usize>,
    existing_deletes_by_file: &crate::engine::delete_flow::ExistingDeleteVisibilityByDataFile,
    lineage_by_file: &HashMap<String, DataFileLineage>,
    new_sequence_number: i64,
    accumulators: &mut BTreeMap<String, CowRewriteAccumulator>,
    seen_updated_row_ids: &mut HashSet<i64>,
) -> Result<(), String> {
    let file_col = cast(required_column(batch, "_file")?, &DataType::Utf8)
        .map_err(|e| format!("cast _file to Utf8 failed: {e}"))?;
    let pos_col = cast(required_column(batch, "_pos")?, &DataType::Int64)
        .map_err(|e| format!("cast _pos to Int64 failed: {e}"))?;
    let file_arr = file_col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "_file was not Utf8 after cast".to_string())?;
    let pos_arr = pos_col
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| "_pos was not Int64 after cast".to_string())?;
    let old_user_batch = user_batch_from_scan_batch(batch, user_schema)?;

    for row in 0..batch.num_rows() {
        if file_arr.is_null(row) || pos_arr.is_null(row) {
            return Err("COW UPDATE scan produced null row identity columns".to_string());
        }
        let file_path = file_arr.value(row);
        if !touched_files.contains(file_path) {
            continue;
        }
        if !crate::engine::delete_flow::data_file_row_is_visible(
            batch,
            row,
            file_path,
            pos_arr.value(row),
            existing_deletes_by_file,
        )? {
            continue;
        }
        let lineage = lineage_by_file.get(file_path).ok_or_else(|| {
            format!("COW UPDATE scan file `{file_path}` is missing row-lineage metadata")
        })?;
        let row_id = lineage
            .first_row_id
            .checked_add(pos_arr.value(row))
            .ok_or_else(|| format!("COW UPDATE row id overflow in `{file_path}`"))?;
        let piece = if let Some(matched_idx) = updated_by_row_id.get(&row_id).copied() {
            seen_updated_row_ids.insert(row_id);
            matched.new_rows.slice(matched_idx, 1)
        } else {
            old_user_batch.slice(row, 1)
        };
        let last_updated = if updated_by_row_id.contains_key(&row_id) {
            Some(new_sequence_number)
        } else {
            lineage.data_sequence_number
        };
        let acc = accumulators.entry(file_path.to_string()).or_default();
        acc.pieces.push(piece);
        acc.row_ids.push(row_id);
        acc.last_updated.push(last_updated);
    }
    Ok(())
}

fn user_batch_from_scan_batch(
    batch: &RecordBatch,
    user_schema: &arrow::datatypes::SchemaRef,
) -> Result<RecordBatch, String> {
    let mut columns = Vec::with_capacity(user_schema.fields().len());
    for field in user_schema.fields() {
        let idx = batch
            .schema()
            .index_of(field.name())
            .map_err(|_| format!("COW UPDATE scan missing `{}` column", field.name()))?;
        let column = cast(batch.column(idx), field.data_type()).map_err(|e| {
            format!(
                "cast COW UPDATE scan column `{}` to {:?} failed: {e}",
                field.name(),
                field.data_type()
            )
        })?;
        columns.push(column);
    }
    RecordBatch::try_new(user_schema.clone(), columns)
        .map_err(|e| format!("build COW UPDATE user batch failed: {e}"))
}

#[derive(Clone, Copy)]
struct DataFileLineage {
    first_row_id: i64,
    data_sequence_number: Option<i64>,
}

fn load_data_file_lineage(
    table: &iceberg::table::Table,
) -> Result<HashMap<String, DataFileLineage>, String> {
    let files = crate::connector::iceberg::catalog::registry::extract_data_files_with_stats(table)?;
    let mut out = HashMap::with_capacity(files.len());
    for file in files {
        let first_row_id = file.first_row_id.ok_or_else(|| {
            format!(
                "COW UPDATE requires first_row_id for iceberg data file `{}`",
                file.path
            )
        })?;
        out.insert(
            file.path,
            DataFileLineage {
                first_row_id,
                data_sequence_number: file.data_sequence_number,
            },
        );
    }
    Ok(out)
}

fn build_cow_rewrite_set(
    table: &iceberg::table::Table,
    matched: &MatchedUpdateBatch,
    data_files_by_old_file: &[(String, Vec<iceberg::spec::DataFile>)],
    rewrite_files: &[CowRewriteFile],
    target_ref: &str,
) -> Result<CowUpdateRewriteSet, String> {
    let metadata = table.metadata();
    // For branch DML, record the branch head snapshot as the rewrite base.
    // CowUpdateCommit checks that the base matches the parent snapshot, so
    // these must agree.
    let base_snapshot_id = if target_ref == "main" {
        metadata
            .current_snapshot()
            .map(|s| s.snapshot_id())
            .ok_or_else(|| "COW UPDATE requires a current snapshot".to_string())?
    } else {
        crate::engine::delete_flow::resolve_branch_head_snapshot_id(metadata, target_ref)?
            .ok_or_else(|| format!("COW UPDATE branch '{target_ref}' has no head snapshot"))?
    };
    let replacement_row_ids_by_old_file = rewrite_files
        .iter()
        .map(|rewrite| {
            let row_ids = (0..rewrite.batch.lineage.row_ids.len())
                .map(|idx| rewrite.batch.lineage.row_ids.value(idx))
                .collect::<Vec<_>>();
            (rewrite.old_file.clone(), row_ids)
        })
        .collect::<BTreeMap<_, _>>();
    let new_files_by_old_file = data_files_by_old_file
        .iter()
        .map(|(old_file, files)| {
            (
                old_file.clone(),
                files
                    .iter()
                    .map(|df| df.file_path().to_string())
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut rows_by_file = BTreeMap::<String, Vec<i64>>::new();
    for (file, row_id) in matched.file_paths.iter().zip(matched.row_ids.iter()) {
        rows_by_file.entry(file.clone()).or_default().push(*row_id);
    }
    let mut touched_data_files = Vec::with_capacity(rows_by_file.len());
    for (old_file, matched_row_ids) in rows_by_file {
        let new_files = new_files_by_old_file.get(&old_file).ok_or_else(|| {
            format!("COW UPDATE rewrite missing replacement data files for `{old_file}`")
        })?;
        let row_ids = replacement_row_ids_by_old_file
            .get(&old_file)
            .ok_or_else(|| {
                format!("COW UPDATE rewrite missing replacement row ids for `{old_file}`")
            })?;
        for row_id in &matched_row_ids {
            if !row_ids.contains(row_id) {
                return Err(format!(
                    "COW UPDATE replacement rows for `{old_file}` are missing updated row id {row_id}"
                ));
            }
        }
        touched_data_files.push(CowUpdateTouchedFile {
            old_file,
            new_files: new_files.clone(),
            row_ids: row_ids.clone(),
        });
    }
    Ok(CowUpdateRewriteSet {
        base_snapshot_id,
        target_table_uuid: table.metadata().uuid().to_string(),
        updated_row_ids: matched.row_ids.clone(),
        touched_data_files,
    })
}

fn empty_cow_rewrite(
    table: &iceberg::table::Table,
    target_ref: &str,
) -> Result<CowUpdateRewriteSet, String> {
    let metadata = table.metadata();
    let base_snapshot_id = if target_ref == "main" {
        metadata
            .current_snapshot()
            .map(|s| s.snapshot_id())
            .unwrap_or(0)
    } else {
        crate::engine::delete_flow::resolve_branch_head_snapshot_id(metadata, target_ref)?
            .unwrap_or(0)
    };
    Ok(CowUpdateRewriteSet {
        base_snapshot_id,
        target_table_uuid: metadata.uuid().to_string(),
        updated_row_ids: Vec::new(),
        touched_data_files: Vec::new(),
    })
}

fn iceberg_table_columns(
    table: &iceberg::table::Table,
) -> Result<Vec<crate::engine::catalog::ColumnDef>, String> {
    let arrow_schema = schema_to_arrow_schema(table.metadata().current_schema())
        .map_err(|e| format!("convert iceberg schema to arrow schema failed: {e}"))?;
    Ok(arrow_schema
        .fields()
        .iter()
        .map(|field| crate::engine::catalog::ColumnDef {
            name: field.name().clone(),
            data_type: field.data_type().clone(),
            nullable: field.is_nullable(),
            write_default: None,
        })
        .collect())
}

fn iceberg_partition_source_columns(table: &iceberg::table::Table) -> Result<Vec<String>, String> {
    let schema = table.metadata().current_schema();
    let mut out = Vec::new();
    for field in table.metadata().default_partition_spec().fields() {
        let source = schema.field_by_id(field.source_id).ok_or_else(|| {
            format!(
                "partition source field id {} is missing from iceberg schema",
                field.source_id
            )
        })?;
        out.push(source.name.clone());
    }
    Ok(out)
}

fn validate_update_assignments(
    assignments: &[crate::sql::parser::ast::UpdateAssignment],
    target_columns: &[crate::engine::catalog::ColumnDef],
    partition_columns: &[String],
) -> Result<(), String> {
    let target_names = target_columns
        .iter()
        .map(|c| c.name.to_ascii_lowercase())
        .collect::<std::collections::HashSet<_>>();
    let partition_names = partition_columns
        .iter()
        .map(|c| c.to_ascii_lowercase())
        .collect::<std::collections::HashSet<_>>();
    let mut seen = std::collections::HashSet::new();
    for assignment in assignments {
        let name = assignment.column.to_ascii_lowercase();
        if matches!(
            name.as_str(),
            "_row_id" | "_last_updated_sequence_number" | "_file" | "_pos"
        ) {
            return Err(format!(
                "UPDATE cannot assign reserved Iceberg metadata column `{}`",
                assignment.column
            ));
        }
        if !target_names.contains(&name) {
            return Err(format!(
                "UPDATE assignment references unknown target column `{}`",
                assignment.column
            ));
        }
        if partition_names.contains(&name) {
            return Err(format!(
                "UPDATE cannot modify Iceberg partition column `{}` in the first implementation",
                assignment.column
            ));
        }
        if !seen.insert(name) {
            return Err(format!(
                "UPDATE assignment lists target column `{}` more than once",
                assignment.column
            ));
        }
    }
    Ok(())
}

fn validate_unique_target_row_ids(row_ids: &[i64]) -> Result<(), String> {
    let mut seen = std::collections::HashSet::new();
    for row_id in row_ids {
        if !seen.insert(*row_id) {
            return Err(format!(
                "UPDATE source matched target row _row_id={} more than once; deduplicate the source before retrying",
                row_id
            ));
        }
    }
    Ok(())
}

fn build_update_match_query_sql(
    target_sql: &str,
    target_alias: &str,
    source_sql: Option<&str>,
    assignments_sql: &[(&str, &str)],
    where_sql: Option<&str>,
) -> String {
    let qualify = |column: &str| {
        if target_alias.is_empty() {
            column.to_string()
        } else {
            format!("{target_alias}.{column}")
        }
    };
    let star = if target_alias.is_empty() {
        "*".to_string()
    } else {
        format!("{target_alias}.*")
    };
    let mut select_items = vec![
        format!("{} AS __nr_file", qualify("_file")),
        format!("{} AS __nr_pos", qualify("_pos")),
        format!("{} AS __nr_row_id", qualify("_row_id")),
        format!(
            "{} AS __nr_last_updated_sequence_number",
            qualify("_last_updated_sequence_number")
        ),
        star,
    ];
    for (column, expr) in assignments_sql {
        select_items.push(format!("{expr} AS __nr_new_{column}"));
    }
    let mut sql = format!("SELECT {} FROM {target_sql}", select_items.join(", "));
    if let Some(source) = source_sql {
        sql.push_str(" CROSS JOIN ");
        sql.push_str(source);
    }
    if let Some(pred) = where_sql {
        sql.push_str(" WHERE ");
        sql.push_str(pred);
    }
    sql
}

// ---------------------------------------------------------------------------
// MERGE INTO
// ---------------------------------------------------------------------------

const MERGE_TARGET_DEFAULT_ALIAS: &str = "__nr_t";
const MERGE_SOURCE_DEFAULT_ALIAS: &str = "__nr_s";

pub(crate) fn execute_merge_statement(
    state: &Arc<StandaloneState>,
    stmt: &MergeStmt,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<StatementResult, String> {
    let target = crate::engine::backend_resolver::resolve_existing_table_target(
        state,
        &stmt.table,
        current_catalog,
        current_database,
    )?;
    if target.backend_name != "iceberg" {
        return Err(format!(
            "MERGE only supports iceberg backends, got `{}`",
            target.backend_name
        ));
    }

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

    // Reject variant tables before any planning (mirrors UPDATE entry).
    ensure_no_variant_columns_for_row_level_mutation(&table)
        .map_err(|e| format!("MERGE INTO: {e}"))?;

    let target_columns = iceberg_table_columns(&table)?;
    let partition_columns = iceberg_partition_source_columns(&table)?;

    // The match SELECT is built against the v3 row-lineage target so the
    // matched-side path can reuse the UPDATE executor. Validate the v3
    // requirement up front instead of letting the executor surface it.
    let _ = select_iceberg_update_mode(&table)?;

    if let Some(clause) = stmt.matched.as_ref() {
        if let MergeMatchedAction::Update { assignments } = &clause.action {
            validate_update_assignments(assignments, &target_columns, &partition_columns)?;
        }
    }
    let insert_columns_resolved = if let Some(clause) = stmt.not_matched.as_ref() {
        Some(resolve_merge_insert_columns(
            &clause.action,
            &target_columns,
        )?)
    } else {
        None
    };

    let match_rows = materialize_merge_match(
        state,
        &target,
        stmt,
        current_catalog,
        insert_columns_resolved.as_deref(),
    )?;

    let mut applied_change = false;
    if let Some(clause) = stmt.matched.as_ref() {
        let matched = matched_update_batch_from_record_batch(&match_rows.matched_batch()?)?;
        if !matched.row_ids.is_empty() {
            validate_unique_target_row_ids(&matched.row_ids)?;
            match &clause.action {
                MergeMatchedAction::Update { .. } => {
                    let mode = select_iceberg_update_mode(&table)?;
                    let table_for_op =
                        block_on_iceberg(async { catalog.load_table(&table_ident).await })?
                            .map_err(|e| format!("reload iceberg table {}: {e}", &table_ident))?;
                    match mode {
                        IcebergUpdateMode::CopyOnWrite => execute_cow_update(
                            state,
                            &target,
                            catalog.clone(),
                            table_ident.clone(),
                            table_for_op,
                            &matched,
                            entry.clone(),
                            "main",
                        )?,
                        IcebergUpdateMode::MergeOnRead => execute_mor_update(
                            state,
                            &target,
                            catalog.clone(),
                            table_ident.clone(),
                            table_for_op,
                            &matched,
                            entry.clone(),
                            "main",
                        )?,
                    };
                    applied_change = true;
                }
                MergeMatchedAction::Delete => {
                    let table_for_op =
                        block_on_iceberg(async { catalog.load_table(&table_ident).await })?
                            .map_err(|e| format!("reload iceberg table {}: {e}", &table_ident))?;
                    execute_merge_matched_delete(
                        state,
                        &target,
                        catalog.clone(),
                        table_ident.clone(),
                        table_for_op,
                        &matched,
                        entry.clone(),
                    )?;
                    applied_change = true;
                }
            }
        }
    }

    if let Some(clause) = stmt.not_matched.as_ref() {
        let insert_columns =
            insert_columns_resolved.expect("not_matched populated => insert columns resolved");
        let insert_batch = match_rows.unmatched_insert_batch(&target_columns, &insert_columns)?;
        if insert_batch.num_rows() > 0 {
            let table_for_op = block_on_iceberg(async { catalog.load_table(&table_ident).await })?
                .map_err(|e| format!("reload iceberg table {}: {e}", &table_ident))?;
            execute_merge_unmatched_insert(
                state,
                &target,
                catalog.clone(),
                table_ident.clone(),
                table_for_op,
                insert_batch,
                entry.clone(),
            )?;
            applied_change = true;
        }
        let _ = clause;
    }

    let _ = applied_change;
    Ok(StatementResult::Ok)
}

/// Resolved target column ordering for `WHEN NOT MATCHED INSERT`. Each entry
/// maps a target column name to either an explicit value expression (sourced
/// from `INSERT (cols) VALUES (exprs)`) or a `NULL` default when the user did
/// not list the column. Validates that every named column exists, that the
/// list has no duplicates, and that no reserved row-lineage column is named.
struct MergeInsertColumns {
    columns: Vec<MergeInsertColumn>,
}

struct MergeInsertColumn {
    name: String,
    /// `Some(idx)` when the user supplied a value for this target column at
    /// position `idx` in the `VALUES` tuple. `None` means "no value
    /// supplied"; we project a NULL of the column's type instead.
    value_index: Option<usize>,
}

impl std::ops::Deref for MergeInsertColumns {
    type Target = [MergeInsertColumn];
    fn deref(&self) -> &[MergeInsertColumn] {
        &self.columns
    }
}

fn resolve_merge_insert_columns(
    action: &MergeNotMatchedAction,
    target_columns: &[crate::engine::catalog::ColumnDef],
) -> Result<MergeInsertColumns, String> {
    let target_names_lower: Vec<String> = target_columns
        .iter()
        .map(|c| c.name.to_ascii_lowercase())
        .collect();

    // Empty `INSERT VALUES (...)` (no column list) means "values match target
    // schema in declaration order". Iceberg row-lineage columns (`_row_id`
    // etc.) are reserved/managed and never appear in the user-visible target
    // schema returned from `iceberg_table_columns`, so we don't have to
    // filter them here.
    if action.columns.is_empty() {
        if action.values.len() != target_columns.len() {
            return Err(format!(
                "MERGE WHEN NOT MATCHED INSERT VALUES count {} does not match target column count {}",
                action.values.len(),
                target_columns.len()
            ));
        }
        let columns = target_columns
            .iter()
            .enumerate()
            .map(|(idx, col)| MergeInsertColumn {
                name: col.name.clone(),
                value_index: Some(idx),
            })
            .collect();
        return Ok(MergeInsertColumns { columns });
    }

    let mut seen: HashSet<String> = HashSet::new();
    let mut by_target: HashMap<String, usize> = HashMap::new();
    for (idx, raw_name) in action.columns.iter().enumerate() {
        let lower = raw_name.to_ascii_lowercase();
        if matches!(
            lower.as_str(),
            "_row_id" | "_last_updated_sequence_number" | "_file" | "_pos"
        ) {
            return Err(format!(
                "MERGE INSERT cannot assign reserved Iceberg metadata column `{raw_name}`"
            ));
        }
        if !target_names_lower.contains(&lower) {
            return Err(format!(
                "MERGE INSERT references unknown target column `{raw_name}`"
            ));
        }
        if !seen.insert(lower.clone()) {
            return Err(format!(
                "MERGE INSERT lists target column `{raw_name}` more than once"
            ));
        }
        by_target.insert(lower, idx);
    }

    let columns = target_columns
        .iter()
        .map(|col| MergeInsertColumn {
            name: col.name.clone(),
            value_index: by_target.get(&col.name.to_ascii_lowercase()).copied(),
        })
        .collect();
    Ok(MergeInsertColumns { columns })
}

struct MergeMatchRows {
    /// The full RecordBatch from the MERGE match SELECT, with rows for both
    /// matched and unmatched cases. Filters for each side are derived from
    /// `__nr_match_kind` / `__nr_matched_apply` / `__nr_unmatched_apply`.
    full: RecordBatch,
}

impl MergeMatchRows {
    fn empty() -> Self {
        Self {
            full: RecordBatch::new_empty(Arc::new(Schema::empty())),
        }
    }

    fn matched_batch(&self) -> Result<RecordBatch, String> {
        if self.full.num_rows() == 0 {
            return Ok(self.full.clone());
        }
        let filter = self.row_filter("matched", "__nr_matched_apply")?;
        filter_record_batch(&self.full, &filter)
            .map_err(|e| format!("filter MERGE matched rows failed: {e}"))
    }

    fn unmatched_insert_batch(
        &self,
        target_columns: &[crate::engine::catalog::ColumnDef],
        insert_columns: &MergeInsertColumns,
    ) -> Result<RecordBatch, String> {
        let target_arrow_schema = arrow::datatypes::Schema::new(
            target_columns
                .iter()
                .map(|c| {
                    arrow::datatypes::Field::new(c.name.clone(), c.data_type.clone(), c.nullable)
                })
                .collect::<Vec<_>>(),
        );
        let target_arrow_schema = Arc::new(target_arrow_schema);
        if self.full.num_rows() == 0 {
            return Ok(RecordBatch::new_empty(target_arrow_schema));
        }
        let filter = self.row_filter("unmatched", "__nr_unmatched_apply")?;
        let filtered = filter_record_batch(&self.full, &filter)
            .map_err(|e| format!("filter MERGE unmatched rows failed: {e}"))?;
        if filtered.num_rows() == 0 {
            return Ok(RecordBatch::new_empty(target_arrow_schema));
        }

        let mut columns: Vec<ArrayRef> = Vec::with_capacity(target_columns.len());
        for (target_col, insert_entry) in target_columns.iter().zip(insert_columns.iter()) {
            debug_assert_eq!(target_col.name, insert_entry.name);
            let column = match insert_entry.value_index {
                Some(_) => {
                    let projected_name = format!("__nr_ins_{}", target_col.name);
                    let idx = filtered.schema().index_of(&projected_name).map_err(|_| {
                        format!("MERGE INSERT projection missing column `{projected_name}`")
                    })?;
                    cast(filtered.column(idx), &target_col.data_type).map_err(|e| {
                        format!(
                            "cast MERGE INSERT column `{}` to {:?} failed: {e}",
                            target_col.name, target_col.data_type
                        )
                    })?
                }
                None => arrow::array::new_null_array(&target_col.data_type, filtered.num_rows()),
            };
            columns.push(column);
        }
        RecordBatch::try_new(target_arrow_schema, columns)
            .map_err(|e| format!("build MERGE INSERT batch failed: {e}"))
    }

    fn row_filter(&self, kind: &str, apply_col: &str) -> Result<BooleanArray, String> {
        let kind_col = cast(
            required_column(&self.full, "__nr_match_kind")?,
            &DataType::Utf8,
        )
        .map_err(|e| format!("cast __nr_match_kind to Utf8 failed: {e}"))?;
        let kind_arr = kind_col
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| "__nr_match_kind was not Utf8 after cast".to_string())?;
        let apply_col = cast(required_column(&self.full, apply_col)?, &DataType::Boolean)
            .map_err(|e| format!("cast {apply_col} to Boolean failed: {e}"))?;
        let apply_arr = apply_col
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| "MERGE apply column was not Boolean after cast".to_string())?;

        let mut bits = Vec::with_capacity(self.full.num_rows());
        for row in 0..self.full.num_rows() {
            if kind_arr.is_null(row) {
                bits.push(false);
                continue;
            }
            let matches_kind = kind_arr.value(row) == kind;
            let applies = !apply_arr.is_null(row) && apply_arr.value(row);
            bits.push(matches_kind && applies);
        }
        Ok(BooleanArray::from(bits))
    }
}

fn materialize_merge_match(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    stmt: &MergeStmt,
    current_catalog: Option<&str>,
    insert_columns: Option<&[MergeInsertColumn]>,
) -> Result<MergeMatchRows, String> {
    let target_alias = stmt
        .target_alias
        .clone()
        .unwrap_or_else(|| MERGE_TARGET_DEFAULT_ALIAS.to_string());
    let target_sql = format!("{} AS {}", target.table, target_alias);

    let source_table_sql =
        mutation_source_relation_to_sql(state, &stmt.source, current_catalog, target)?;
    // `mutation_source_to_sql` preserves the user-provided alias when present.
    // When the source carries no alias, inject `__nr_s` so the projection /
    // ON predicate can reference source columns deterministically.
    let source_sql = match &stmt.source {
        crate::sql::parser::ast::MutationSource::Table { alias, .. }
        | crate::sql::parser::ast::MutationSource::Query { alias, .. } => {
            if alias.is_some() {
                source_table_sql
            } else {
                format!("{source_table_sql} AS {MERGE_SOURCE_DEFAULT_ALIAS}")
            }
        }
    };

    let on_sql = stmt.on.to_string();
    let matched_predicate_sql = stmt
        .matched
        .as_ref()
        .and_then(|c| c.predicate.as_ref())
        .map(|expr| expr.to_string());
    let not_matched_predicate_sql = stmt
        .not_matched
        .as_ref()
        .and_then(|c| c.predicate.as_ref())
        .map(|expr| expr.to_string());

    let matched_assignments_sql = match stmt.matched.as_ref().map(|c| &c.action) {
        Some(MergeMatchedAction::Update { assignments }) => assignments
            .iter()
            .map(|a| (a.column.clone(), a.value.to_string()))
            .collect::<Vec<_>>(),
        _ => Vec::new(),
    };
    let matched_assignments_sql_borrow: Vec<(&str, &str)> = matched_assignments_sql
        .iter()
        .map(|(c, e)| (c.as_str(), e.as_str()))
        .collect();

    let insert_values_sql: Vec<(String, String)> =
        match (insert_columns, stmt.not_matched.as_ref().map(|c| &c.action)) {
            (Some(cols), Some(action)) => cols
                .iter()
                .filter_map(|col| {
                    col.value_index
                        .map(|idx| (col.name.clone(), action.values[idx].to_string()))
                })
                .collect(),
            _ => Vec::new(),
        };
    let insert_values_sql_borrow: Vec<(&str, &str)> = insert_values_sql
        .iter()
        .map(|(c, e)| (c.as_str(), e.as_str()))
        .collect();

    let sql = build_merge_match_query_sql(
        &target_sql,
        &target_alias,
        &source_sql,
        &on_sql,
        matched_predicate_sql.as_deref(),
        not_matched_predicate_sql.as_deref(),
        &matched_assignments_sql_borrow,
        &insert_values_sql_borrow,
    );

    let result = execute_merge_match_query(state, Some(&target.catalog), &sql, &target.namespace)?;
    Ok(result)
}

fn execute_merge_match_query(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    sql: &str,
    current_database: &str,
) -> Result<MergeMatchRows, String> {
    let statement = crate::sql::parser::parse_sql_raw(sql)?;
    let sqlparser::ast::Statement::Query(query) = statement else {
        return Err("internal MERGE match query was not a SELECT".to_string());
    };
    crate::engine::query_prep::refresh_external_tables_for_query(
        state,
        current_catalog,
        current_database,
        &query,
    )?;
    // Clone-then-release: pipeline execution must not hold
    // `state.catalog.read()`. See iceberg_writer::run_select_to_chunks.
    let catalog_snapshot = state
        .catalog
        .read()
        .expect("standalone catalog read lock")
        .clone();
    let result = crate::engine::execute_query(
        &query,
        &catalog_snapshot,
        current_database,
        state.exchange_port,
        None,
    )?;
    let Some(first_chunk) = result.chunks.first() else {
        return Ok(MergeMatchRows::empty());
    };
    let schema = first_chunk.batch.schema();
    let batches = result
        .chunks
        .iter()
        .map(|c| c.batch.clone())
        .collect::<Vec<_>>();
    let full = concat_batches(&schema, batches.iter())
        .map_err(|e| format!("concatenate MERGE match batches failed: {e}"))?;
    Ok(MergeMatchRows { full })
}

fn build_merge_match_query_sql(
    target_sql: &str,
    target_alias: &str,
    source_sql: &str,
    on_sql: &str,
    matched_predicate_sql: Option<&str>,
    not_matched_predicate_sql: Option<&str>,
    matched_assignments_sql: &[(&str, &str)],
    insert_values_sql: &[(&str, &str)],
) -> String {
    let qualify = |column: &str| {
        if target_alias.is_empty() {
            column.to_string()
        } else {
            format!("{target_alias}.{column}")
        }
    };
    let star = if target_alias.is_empty() {
        "*".to_string()
    } else {
        format!("{target_alias}.*")
    };

    let mut select_items = vec![
        format!("{} AS __nr_file", qualify("_file")),
        format!("{} AS __nr_pos", qualify("_pos")),
        format!("{} AS __nr_row_id", qualify("_row_id")),
        format!(
            "{} AS __nr_last_updated_sequence_number",
            qualify("_last_updated_sequence_number")
        ),
        star,
        format!(
            "(CASE WHEN {} IS NOT NULL THEN 'matched' ELSE 'unmatched' END) AS __nr_match_kind",
            qualify("_row_id")
        ),
    ];
    select_items.push(format!(
        "(CASE WHEN ({}) THEN TRUE ELSE FALSE END) AS __nr_matched_apply",
        matched_predicate_sql.unwrap_or("TRUE")
    ));
    select_items.push(format!(
        "(CASE WHEN ({}) THEN TRUE ELSE FALSE END) AS __nr_unmatched_apply",
        not_matched_predicate_sql.unwrap_or("TRUE")
    ));
    for (column, expr) in matched_assignments_sql {
        select_items.push(format!("({expr}) AS __nr_new_{column}"));
    }
    for (column, expr) in insert_values_sql {
        select_items.push(format!("({expr}) AS __nr_ins_{column}"));
    }

    format!(
        "SELECT {} FROM {} LEFT JOIN {} ON {}",
        select_items.join(", "),
        source_sql,
        target_sql,
        on_sql
    )
}

fn execute_merge_matched_delete(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    catalog: Arc<dyn Catalog>,
    table_ident: iceberg::TableIdent,
    table: iceberg::table::Table,
    matched: &MatchedUpdateBatch,
    entry: crate::connector::iceberg::catalog::IcebergCatalogEntry,
) -> Result<StatementResult, String> {
    let referenced_partitions =
        crate::engine::delete_flow::load_referenced_data_file_partitions(&table)?;
    let delete_groups = build_position_delete_groups_from_matched(matched, &referenced_partitions)?;

    let metadata = table.metadata();
    let staging_dir = format!(
        "{}/data/_staging/{}",
        metadata.location(),
        uuid::Uuid::new_v4()
    );
    let file_io = table.file_io().clone();
    let collector = Arc::new(IcebergCommitCollector::new(
        CommitOpKind::RowDeltaDv,
        table_ident,
        metadata.current_snapshot().map(|s| s.snapshot_id()),
        metadata.last_sequence_number(),
        metadata.current_schema().clone(),
        metadata.default_partition_spec().clone(),
        staging_dir,
        crate::common::types::UniqueId { hi: 0, lo: 0 },
    ));
    for group in delete_groups {
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
            target_ref: "main".to_string(),
            snapshot_properties: BTreeMap::new(),
        })
        .await
    })??;

    crate::engine::iceberg_writer::invalidate_iceberg_caches(state, target)?;
    Ok(StatementResult::Ok)
}

fn execute_merge_unmatched_insert(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    catalog: Arc<dyn Catalog>,
    table_ident: iceberg::TableIdent,
    table: iceberg::table::Table,
    insert_batch: RecordBatch,
    entry: crate::connector::iceberg::catalog::IcebergCatalogEntry,
) -> Result<StatementResult, String> {
    let data_files = block_on_iceberg(async {
        crate::connector::iceberg::data_writer::write_record_batches_as_data_files(
            &table,
            std::iter::once(insert_batch),
        )
        .await
    })??;

    let metadata = table.metadata();
    let staging_dir = format!(
        "{}/data/_staging/{}",
        metadata.location(),
        uuid::Uuid::new_v4()
    );
    let file_io = table.file_io().clone();
    let collector = Arc::new(IcebergCommitCollector::new(
        CommitOpKind::FastAppend,
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
        let wf = crate::engine::iceberg_writer::data_file_to_written_file(&df, default_spec_id)?;
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
            target_ref: "main".to_string(),
            snapshot_properties: BTreeMap::new(),
        })
        .await
    })??;

    crate::engine::iceberg_writer::invalidate_iceberg_caches(state, target)?;
    Ok(StatementResult::Ok)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::catalog::ColumnDef;
    use arrow::datatypes::DataType;

    fn col(name: &str) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: true,
            write_default: None,
        }
    }

    #[test]
    fn reject_reserved_update_columns() {
        let err = validate_update_assignments(
            &[crate::sql::parser::ast::UpdateAssignment {
                column: "_row_id".to_string(),
                value: sqlparser::ast::Expr::Value(
                    sqlparser::ast::Value::Number("1".to_string(), false).into(),
                ),
            }],
            &[col("id"), col("v")],
            &[],
        )
        .expect_err("must reject");
        assert!(err.contains("reserved Iceberg metadata column"), "{err}");
    }

    #[test]
    fn reject_partition_column_update() {
        let err = validate_update_assignments(
            &[crate::sql::parser::ast::UpdateAssignment {
                column: "id".to_string(),
                value: sqlparser::ast::Expr::Value(
                    sqlparser::ast::Value::Number("1".to_string(), false).into(),
                ),
            }],
            &[col("id"), col("v")],
            &["id".to_string()],
        )
        .expect_err("must reject");
        assert!(err.contains("partition column"), "{err}");
    }

    #[test]
    fn duplicate_row_ids_are_rejected() {
        let err = validate_unique_target_row_ids(&[7, 8, 7]).expect_err("duplicate");
        assert!(err.contains("_row_id=7"), "{err}");
    }

    #[test]
    fn update_match_query_projects_identity_columns() {
        let sql = build_update_match_query_sql(
            "ice.db1.t AS t",
            "t",
            Some("staging.s AS s"),
            &[("v", "s.v")],
            Some("t.id = s.id"),
        );
        assert!(sql.contains("t._row_id AS __nr_row_id"), "{sql}");
        assert!(sql.contains("s.v AS __nr_new_v"), "{sql}");
        assert!(sql.contains("WHERE t.id = s.id"), "{sql}");
    }
}
