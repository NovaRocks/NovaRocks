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

//! Background worker and whole-table executor for standalone Iceberg OPTIMIZE.

use std::collections::BTreeMap;
use std::sync::{Arc, Weak};
use std::thread;
use std::time::Duration;

use iceberg::spec::DataFile;
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use sqlparser::ast::Statement;

use crate::common::types::UniqueId;
use crate::connector::iceberg::catalog::IcebergCatalogEntry;
use crate::connector::iceberg::catalog::registry::{block_on_iceberg, build_iceberg_catalog};
use crate::connector::iceberg::catalog::row_lineage_enabled;
use crate::connector::iceberg::commit::{
    AbortLog, CommitOpKind, IcebergCommitCollector, LiveFileMetrics, RunInput,
    current_live_file_metrics, run_iceberg_commit_typed,
};
use crate::connector::iceberg::data_writer::{
    RowLineageColumns, RowLineageWriteBatch, write_row_lineage_batches_as_data_files,
};
use crate::engine::StandaloneState;
use crate::engine::backend_resolver::TargetBackend;
use crate::engine::iceberg_writer::{
    build_abort_cleanup_for_catalog_entry, data_file_to_written_file, invalidate_iceberg_caches,
    run_select_to_chunks,
};
use crate::engine::mv::iceberg_refresh::write_chunks_as_iceberg_data_files;
use crate::exec::row_position::{ICEBERG_LAST_UPDATED_SEQ_COL, ICEBERG_ROW_ID_COL};
use crate::meta::repository::job::{
    IcebergOptimizeJobOutcome, IcebergOptimizeJobState, StoredIcebergOptimizeJob,
};

const OPTIMIZE_WORKER_POLL_INTERVAL: Duration = Duration::from_millis(500);

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct WholeTableRewriteTarget {
    pub(crate) catalog: String,
    pub(crate) namespace: String,
    pub(crate) table: String,
    pub(crate) base_snapshot_id: i64,
    pub(crate) job_id: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct WholeTableRewriteResult {
    pub(crate) optimize_outcome: IcebergOptimizeJobOutcome,
    pub(crate) before_metrics: LiveFileMetrics,
}

impl WholeTableRewriteTarget {
    fn from_job(job: &StoredIcebergOptimizeJob) -> Self {
        Self {
            catalog: job.catalog.clone(),
            namespace: job.namespace.clone(),
            table: job.table.clone(),
            base_snapshot_id: job.base_snapshot_id,
            job_id: Some(job.id),
        }
    }

    fn context(&self) -> String {
        match self.job_id {
            Some(job_id) => format!("optimize job {job_id}"),
            None => "rewrite_data_files".to_string(),
        }
    }
}

pub(crate) fn spawn_optimize_worker(state: Arc<StandaloneState>) {
    if state.metadata_provider.is_none() {
        return;
    }

    match reconcile_running_optimize_jobs_once(&state) {
        Ok(failed) if failed > 0 => {
            tracing::warn!(
                failed,
                "reconciled running iceberg optimize jobs on startup"
            );
        }
        Ok(_) => {}
        Err(err) => {
            tracing::warn!("failed to reconcile running iceberg optimize jobs on startup: {err}");
        }
    }

    let weak = Arc::downgrade(&state);
    if let Err(err) = thread::Builder::new()
        .name("iceberg-optimize-worker".to_string())
        .spawn(move || optimize_worker_loop(weak))
    {
        tracing::warn!("spawn iceberg optimize worker failed: {err}");
    }
}

fn optimize_worker_loop(state: Weak<StandaloneState>) {
    loop {
        let Some(strong) = state.upgrade() else {
            return;
        };
        if strong.metadata_provider.is_none() {
            return;
        }

        if let Err(err) = run_optimize_jobs_once(&strong) {
            tracing::warn!("iceberg optimize worker iteration failed: {err}");
        }
        drop(strong);
        thread::sleep(OPTIMIZE_WORKER_POLL_INTERVAL);
    }
}

pub(crate) fn run_optimize_jobs_once(state: &Arc<StandaloneState>) -> Result<(), String> {
    finish_recorded_running_outcomes_once(state)?;
    let jobs = list_pending_iceberg_optimize_jobs(state)?;
    for job in jobs {
        let running = match claim_iceberg_optimize_job(state, job.id) {
            Ok(running) => running,
            Err(err) => {
                tracing::warn!(
                    job_id = job.id,
                    catalog = job.catalog,
                    namespace = job.namespace,
                    table = job.table,
                    "skip iceberg optimize job that could not be claimed: {err}"
                );
                continue;
            }
        };
        match run_one_optimize_job(state, &running) {
            Ok(outcome) => {
                record_iceberg_optimize_job_outcome(state, running.id, outcome.clone()).map_err(
                    |err| {
                        format!(
                            "iceberg optimize job {} completed but persisting commit outcome failed: {err}",
                            running.id
                        )
                    },
                )?;
                finish_iceberg_optimize_job(state, running.id, outcome).map_err(|err| {
                    format!(
                        "iceberg optimize job {} completed but persisting FINISHED state failed: {err}",
                        running.id
                    )
                })?;
            }
            Err(err) => {
                tracing::warn!(
                    job_id = running.id,
                    catalog = running.catalog,
                    namespace = running.namespace,
                    table = running.table,
                    "iceberg optimize job failed: {err}"
                );
                fail_iceberg_optimize_job(state, running.id, err)?;
            }
        }
    }
    Ok(())
}

pub(crate) fn reconcile_running_optimize_jobs_once(
    state: &Arc<StandaloneState>,
) -> Result<usize, String> {
    let finished = finish_recorded_running_outcomes_once(state)?;
    let provider = state
        .metadata_provider
        .as_ref()
        .ok_or_else(|| "iceberg optimize metadata provider is not configured".to_string())?;
    let mut txn = provider
        .begin_write("fail running iceberg optimize jobs on startup")
        .map_err(|e| format!("open iceberg optimize startup transaction failed: {e}"))?;
    let failed = state
        .job_repo
        .fail_running_iceberg_optimize_jobs_on_startup(txn.as_mut(), now_ms())
        .map_err(|e| format!("fail running iceberg optimize jobs on startup failed: {e}"))?;
    txn.commit()
        .map_err(|e| format!("commit iceberg optimize startup transaction failed: {e}"))?;
    Ok(finished + failed)
}

fn finish_recorded_running_outcomes_once(state: &Arc<StandaloneState>) -> Result<usize, String> {
    let mut finished = 0usize;
    for job in show_iceberg_optimize_jobs(state)? {
        if job.state != IcebergOptimizeJobState::Running {
            continue;
        }
        if let Some(outcome) = job.outcome.clone() {
            finish_iceberg_optimize_job(state, job.id, outcome)?;
            finished += 1;
        }
    }
    Ok(finished)
}

fn list_pending_iceberg_optimize_jobs(
    state: &Arc<StandaloneState>,
) -> Result<Vec<StoredIcebergOptimizeJob>, String> {
    let provider = state
        .metadata_provider
        .as_ref()
        .ok_or_else(|| "iceberg optimize metadata provider is not configured".to_string())?;
    let read = provider
        .begin_read()
        .map_err(|e| format!("open iceberg optimize job read transaction failed: {e}"))?;
    state
        .job_repo
        .list_pending_iceberg_optimize_jobs(read.as_ref())
        .map_err(|e| format!("list pending iceberg optimize jobs failed: {e}"))
}

fn show_iceberg_optimize_jobs(
    state: &Arc<StandaloneState>,
) -> Result<Vec<StoredIcebergOptimizeJob>, String> {
    let provider = state
        .metadata_provider
        .as_ref()
        .ok_or_else(|| "iceberg optimize metadata provider is not configured".to_string())?;
    let read = provider
        .begin_read()
        .map_err(|e| format!("open iceberg optimize job show transaction failed: {e}"))?;
    state
        .job_repo
        .show_iceberg_optimize_jobs(read.as_ref())
        .map_err(|e| format!("show iceberg optimize jobs failed: {e}"))
}

fn claim_iceberg_optimize_job(
    state: &Arc<StandaloneState>,
    job_id: i64,
) -> Result<StoredIcebergOptimizeJob, String> {
    let provider = state
        .metadata_provider
        .as_ref()
        .ok_or_else(|| "iceberg optimize metadata provider is not configured".to_string())?;
    let mut txn = provider
        .begin_write("claim iceberg optimize job")
        .map_err(|e| format!("open iceberg optimize claim transaction failed: {e}"))?;
    let job = state
        .job_repo
        .claim_iceberg_optimize_job(txn.as_mut(), job_id, now_ms())
        .map_err(|e| format!("claim iceberg optimize job failed: {e}"))?;
    txn.commit()
        .map_err(|e| format!("commit iceberg optimize claim transaction failed: {e}"))?;
    Ok(job)
}

fn record_iceberg_optimize_job_outcome(
    state: &Arc<StandaloneState>,
    job_id: i64,
    outcome: IcebergOptimizeJobOutcome,
) -> Result<StoredIcebergOptimizeJob, String> {
    let provider = state
        .metadata_provider
        .as_ref()
        .ok_or_else(|| "iceberg optimize metadata provider is not configured".to_string())?;
    let mut txn = provider
        .begin_write("record iceberg optimize job outcome")
        .map_err(|e| format!("open iceberg optimize outcome transaction failed: {e}"))?;
    let job = state
        .job_repo
        .record_iceberg_optimize_job_outcome(txn.as_mut(), job_id, now_ms(), outcome)
        .map_err(|e| format!("record iceberg optimize job outcome failed: {e}"))?;
    txn.commit()
        .map_err(|e| format!("commit iceberg optimize outcome transaction failed: {e}"))?;
    Ok(job)
}

fn finish_iceberg_optimize_job(
    state: &Arc<StandaloneState>,
    job_id: i64,
    outcome: IcebergOptimizeJobOutcome,
) -> Result<StoredIcebergOptimizeJob, String> {
    let provider = state
        .metadata_provider
        .as_ref()
        .ok_or_else(|| "iceberg optimize metadata provider is not configured".to_string())?;
    let mut txn = provider
        .begin_write("finish iceberg optimize job")
        .map_err(|e| format!("open iceberg optimize finish transaction failed: {e}"))?;
    let job = state
        .job_repo
        .finish_iceberg_optimize_job(txn.as_mut(), job_id, now_ms(), outcome)
        .map_err(|e| format!("finish iceberg optimize job failed: {e}"))?;
    txn.commit()
        .map_err(|e| format!("commit iceberg optimize finish transaction failed: {e}"))?;
    Ok(job)
}

fn fail_iceberg_optimize_job(
    state: &Arc<StandaloneState>,
    job_id: i64,
    error_message: String,
) -> Result<StoredIcebergOptimizeJob, String> {
    let provider = state
        .metadata_provider
        .as_ref()
        .ok_or_else(|| "iceberg optimize metadata provider is not configured".to_string())?;
    let mut txn = provider
        .begin_write("fail iceberg optimize job")
        .map_err(|e| format!("open iceberg optimize fail transaction failed: {e}"))?;
    let job = state
        .job_repo
        .fail_iceberg_optimize_job(txn.as_mut(), job_id, now_ms(), error_message)
        .map_err(|e| format!("fail iceberg optimize job failed: {e}"))?;
    txn.commit()
        .map_err(|e| format!("commit iceberg optimize fail transaction failed: {e}"))?;
    Ok(job)
}

pub(crate) fn run_one_optimize_job(
    state: &Arc<StandaloneState>,
    job: &StoredIcebergOptimizeJob,
) -> Result<IcebergOptimizeJobOutcome, String> {
    execute_whole_table_rewrite(state, job)
}

pub(crate) fn execute_whole_table_rewrite(
    state: &Arc<StandaloneState>,
    job: &StoredIcebergOptimizeJob,
) -> Result<IcebergOptimizeJobOutcome, String> {
    let target = WholeTableRewriteTarget::from_job(job);
    execute_whole_table_rewrite_for_target(state, &target)
}

pub(crate) fn execute_whole_table_rewrite_for_target(
    state: &Arc<StandaloneState>,
    rewrite_target: &WholeTableRewriteTarget,
) -> Result<IcebergOptimizeJobOutcome, String> {
    execute_whole_table_rewrite_with_metrics_for_target(state, rewrite_target)
        .map(|result| result.optimize_outcome)
}

pub(crate) fn execute_whole_table_rewrite_with_metrics_for_target(
    state: &Arc<StandaloneState>,
    rewrite_target: &WholeTableRewriteTarget,
) -> Result<WholeTableRewriteResult, String> {
    let target = TargetBackend {
        backend_name: "iceberg",
        catalog: rewrite_target.catalog.clone(),
        namespace: rewrite_target.namespace.clone(),
        table: rewrite_target.table.clone(),
    };
    let entry = {
        let registry = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        registry.get(&rewrite_target.catalog)?
    };
    entry.invalidate_table_cache(&rewrite_target.namespace, &rewrite_target.table);

    let catalog: Arc<dyn Catalog> = build_iceberg_catalog(&entry)?;
    let table_ident = TableIdent::new(
        NamespaceIdent::new(rewrite_target.namespace.clone()),
        rewrite_target.table.clone(),
    );
    let table = load_current_table(catalog.as_ref(), &table_ident, rewrite_target)?;
    validate_base_snapshot(&table, rewrite_target)?;

    let initial_metrics = block_on_iceberg(current_live_file_metrics(&table, table.file_io()))??;
    if initial_metrics.data_files == 0 && initial_metrics.delete_files == 0 {
        tracing::info!(
            job_id = ?rewrite_target.job_id,
            catalog = rewrite_target.catalog,
            namespace = rewrite_target.namespace,
            table = rewrite_target.table,
            base_snapshot_id = rewrite_target.base_snapshot_id,
            "iceberg optimize no-op: table has no live files"
        );
        let optimize_outcome = IcebergOptimizeJobOutcome {
            target_snapshot_id: None,
            rewritten_data_files: 0,
            deleted_data_files: 0,
            added_data_files: 0,
            output_record_count: 0,
        };
        return Ok(WholeTableRewriteResult {
            optimize_outcome,
            before_metrics: initial_metrics,
        });
    }

    let preserve_row_lineage = row_lineage_enabled(table.metadata());
    // Tables that carry hidden NovaRocks MV internal columns (the IMV apply-key
    // column `__nova_base_row_id` and any declared aggregate-state columns) must
    // be rewritten through a direct physical read: those columns are real
    // physical Iceberg fields that the analyzer hides from `SELECT *`, so the
    // SQL-driven path below would omit them and the writer (which builds its
    // schema from the FULL physical `current_schema()`) would see fewer payload
    // columns than schema fields. The apply-key column is read BY STORED VALUE
    // during incremental refresh (`locate_target_rows_by_apply_key`) and cannot
    // be regenerated, so it must be carried through the rewrite verbatim at its
    // real field id. The gate reuses the exact table-property logic that drives
    // the analyzer's column hiding, so plain tables (no hidden columns) take the
    // unchanged SQL path below byte-for-byte.
    let hidden_internal_columns =
        crate::connector::iceberg::catalog::hidden_internal_column_names_from_metadata(
            table.metadata(),
        );
    let has_hidden_internal_columns = !hidden_internal_columns.is_empty();

    let (data_files, expected_rows) = if has_hidden_internal_columns {
        // Direct physical read path. `preserve_row_lineage` is always true for
        // NovaRocks MV storage tables (they are created with
        // write.row-lineage=true), but assert it explicitly: a hidden-column
        // table without row lineage cannot have its apply key preserved by the
        // row-lineage writer, and silently routing it elsewhere would risk
        // dropping the apply key.
        if !preserve_row_lineage {
            return Err(format!(
                "iceberg optimize cannot rewrite {}.{}.{}: table carries hidden internal columns \
                 ({}) but does not declare write.row-lineage=true, so the apply-key column cannot \
                 be preserved",
                rewrite_target.catalog,
                rewrite_target.namespace,
                rewrite_target.table,
                hidden_internal_columns.join(", ")
            ));
        }
        let batches = block_on_iceberg(read_full_physical_row_lineage_batches(&table))??;
        let rows = row_lineage_batches_row_count(&batches)?;
        let data_files = if rows == 0 {
            Vec::new()
        } else {
            block_on_iceberg(write_row_lineage_batches_as_data_files(&table, &batches))??
        };
        (data_files, rows)
    } else {
        let select_sql = if preserve_row_lineage {
            format!(
                "SELECT *, {ICEBERG_ROW_ID_COL}, {ICEBERG_LAST_UPDATED_SEQ_COL} FROM {}.{}.{}",
                quote_ident(&rewrite_target.catalog),
                quote_ident(&rewrite_target.namespace),
                quote_ident(&rewrite_target.table)
            )
        } else {
            format!(
                "SELECT * FROM {}.{}.{}",
                quote_ident(&rewrite_target.catalog),
                quote_ident(&rewrite_target.namespace),
                quote_ident(&rewrite_target.table)
            )
        };
        let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(&select_sql)?;
        let stmt = crate::sql::parser::parse_normalized_sql_raw(&normalized)
            .map_err(|e| format!("parse optimize SELECT failed: {e}"))?;
        let Statement::Query(query) = stmt else {
            return Err("internal optimize SELECT parser did not return a query".to_string());
        };
        let chunks = run_select_to_chunks(state, &target, query.as_ref())?;
        let visible_rows = chunk_row_count(&chunks)?;

        let data_files = if visible_rows == 0 {
            Vec::new()
        } else if preserve_row_lineage {
            let batches = chunks_to_row_lineage_batches(&chunks)?;
            block_on_iceberg(write_row_lineage_batches_as_data_files(&table, &batches))??
        } else {
            block_on_iceberg(write_chunks_as_iceberg_data_files(&table, &chunks))??
        };
        (data_files, visible_rows)
    };
    let output_record_count = data_file_record_count(&data_files)?;
    if output_record_count != expected_rows {
        return Err(format!(
            "iceberg optimize output row count mismatch: selected {expected_rows}, wrote {output_record_count}"
        ));
    }

    let post_write = (|| {
        let table = load_current_table(catalog.as_ref(), &table_ident, rewrite_target)?;
        validate_base_snapshot(&table, rewrite_target)?;
        let input_metrics = block_on_iceberg(current_live_file_metrics(&table, table.file_io()))??;
        Ok::<_, String>((table, input_metrics))
    })();
    let (table, input_metrics) = match post_write {
        Ok(value) => value,
        Err(err) => {
            return Err(cleanup_written_data_files_after_error(
                &entry,
                &data_files,
                err,
            ));
        }
    };

    let metadata = table.metadata();
    let staging_dir = format!(
        "{}/data/_staging/{}",
        metadata.location(),
        uuid::Uuid::new_v4()
    );
    let collector = Arc::new(
        IcebergCommitCollector::new(
            CommitOpKind::RewriteDataFiles,
            table_ident.clone(),
            metadata.current_snapshot().map(|s| s.snapshot_id()),
            metadata.last_sequence_number(),
            metadata.current_schema().clone(),
            metadata.default_partition_spec().clone(),
            staging_dir,
            UniqueId { hi: 0, lo: 0 },
        )
        .with_table_metadata(metadata.clone()),
    );
    if preserve_row_lineage {
        collector.mark_preserve_row_lineage();
    }
    let default_spec_id = metadata.default_partition_spec_id();
    for data_file in &data_files {
        collector.inject_written_file(data_file_to_written_file(data_file, default_spec_id)?);
    }

    let abort_cleanup = build_abort_cleanup_for_catalog_entry(&entry)?;
    let file_io = table.file_io().clone();
    let commit_outcome = block_on_iceberg(run_iceberg_commit_typed(RunInput {
        collector,
        catalog: Arc::clone(&catalog),
        table,
        fs: abort_cleanup.fs,
        file_io,
        cleanup_path_mapper: abort_cleanup.path_mapper,
        cow_update_rewrite: None,
        target_ref: "main".to_string(),
        snapshot_properties: BTreeMap::new(),
    }))
    .map_err(|err| format!("iceberg rewrite_data_files commit runtime failed: {err}"))?
    .map_err(compact_commit_error_to_user_message)?;

    invalidate_iceberg_caches(state, &target)?;

    // If the optimized table is an iceberg MV's own storage table, advance the
    // MV's recorded target snapshot to this pure-rewrite REPLACE snapshot so the
    // next incremental refresh does not reject the table as "modified outside
    // NovaRocks". No-op for plain (non-MV) tables. See
    // `MvRepository::adopt_target_compaction_snapshot` for the safety rules.
    adopt_mv_target_compaction_snapshot_if_present(
        state,
        rewrite_target,
        commit_outcome.new_snapshot_id,
    )?;

    tracing::info!(
        job_id = ?rewrite_target.job_id,
        catalog = rewrite_target.catalog,
        namespace = rewrite_target.namespace,
        table = rewrite_target.table,
        base_snapshot_id = rewrite_target.base_snapshot_id,
        target_snapshot_id = commit_outcome.new_snapshot_id,
        input_data_files = input_metrics.data_files,
        input_delete_files = input_metrics.delete_files,
        output_data_files = data_files.len(),
        output_record_count,
        "iceberg optimize finished"
    );

    let optimize_outcome = IcebergOptimizeJobOutcome {
        target_snapshot_id: Some(commit_outcome.new_snapshot_id),
        rewritten_data_files: input_metrics.data_files,
        deleted_data_files: input_metrics.delete_files,
        added_data_files: i64::try_from(data_files.len())
            .map_err(|_| "iceberg optimize output data file count overflow".to_string())?,
        output_record_count,
    };
    Ok(WholeTableRewriteResult {
        optimize_outcome,
        before_metrics: input_metrics,
    })
}

/// After a pure compaction rewrite, advance the recorded target snapshot of an
/// iceberg MV whose storage table was just optimized, so the next incremental
/// refresh does not fail `validate_target_snapshot`. This is a no-op for plain
/// (non-MV) tables: `adopt_target_compaction_snapshot` returns `false` when no
/// MV target-lookup record exists for the table, or when a refresh is in
/// progress / the recorded baseline no longer matches the optimized snapshot.
fn adopt_mv_target_compaction_snapshot_if_present(
    state: &Arc<StandaloneState>,
    rewrite_target: &WholeTableRewriteTarget,
    new_snapshot_id: i64,
) -> Result<(), String> {
    let Some(provider) = state.metadata_provider.as_ref() else {
        return Ok(());
    };
    let mut txn = provider
        .begin_write("adopt iceberg mv target compaction snapshot")
        .map_err(|e| format!("open mv compaction-snapshot adopt transaction failed: {e}"))?;
    let adopted = state
        .mv_repo
        .adopt_target_compaction_snapshot(
            txn.as_mut(),
            &rewrite_target.catalog,
            &rewrite_target.namespace,
            &rewrite_target.table,
            rewrite_target.base_snapshot_id,
            new_snapshot_id,
        )
        .map_err(|e| format!("adopt iceberg mv target compaction snapshot failed: {e}"))?;
    txn.commit()
        .map_err(|e| format!("commit mv compaction-snapshot adopt transaction failed: {e}"))?;
    if adopted {
        tracing::info!(
            catalog = rewrite_target.catalog,
            namespace = rewrite_target.namespace,
            table = rewrite_target.table,
            base_snapshot_id = rewrite_target.base_snapshot_id,
            new_snapshot_id,
            "adopted compaction snapshot as iceberg MV recorded target snapshot"
        );
    }
    Ok(())
}

fn load_current_table(
    catalog: &dyn Catalog,
    table_ident: &TableIdent,
    target: &WholeTableRewriteTarget,
) -> Result<iceberg::table::Table, String> {
    block_on_iceberg(async { catalog.load_table(table_ident).await })?.map_err(|e| {
        format!(
            "load iceberg table {}.{}.{} for {} failed: {e}",
            target.catalog,
            target.namespace,
            target.table,
            target.context()
        )
    })
}

fn validate_base_snapshot(
    table: &iceberg::table::Table,
    target: &WholeTableRewriteTarget,
) -> Result<(), String> {
    let current_snapshot_id = table
        .metadata()
        .current_snapshot()
        .map(|snapshot| snapshot.snapshot_id())
        .ok_or_else(|| {
            format!(
                "iceberg {} requires {}.{}.{} to have current snapshot {}",
                target.context(),
                target.catalog,
                target.namespace,
                target.table,
                target.base_snapshot_id
            )
        })?;
    if current_snapshot_id != target.base_snapshot_id {
        return Err(format!(
            "iceberg {} base snapshot mismatch for {}.{}.{}: expected {}, current {}",
            target.context(),
            target.catalog,
            target.namespace,
            target.table,
            target.base_snapshot_id,
            current_snapshot_id
        ));
    }
    Ok(())
}

fn chunk_row_count(chunks: &[crate::exec::chunk::Chunk]) -> Result<i64, String> {
    chunks.iter().try_fold(0_i64, |sum, chunk| {
        let rows = i64::try_from(chunk.batch.num_rows())
            .map_err(|_| "iceberg optimize selected row count overflow".to_string())?;
        sum.checked_add(rows)
            .ok_or_else(|| "iceberg optimize selected row count overflow".to_string())
    })
}

/// Split each chunk into (user-facing payload, row-lineage columns) so the
/// downstream `write_row_lineage_batches_as_data_files` writer can stamp
/// `_row_id` / `_last_updated_sequence_number` at their reserved field IDs
/// instead of allocating fresh row ids. The chunks come from the OPTIMIZE
/// `SELECT *, _row_id, _last_updated_sequence_number FROM …` and are
/// expected to carry both columns at the end of the schema in that order.
fn chunks_to_row_lineage_batches(
    chunks: &[crate::exec::chunk::Chunk],
) -> Result<Vec<RowLineageWriteBatch>, String> {
    use arrow::array::Int64Array;
    use arrow::datatypes::Schema;

    let mut batches = Vec::with_capacity(chunks.len());
    for chunk in chunks {
        if chunk.batch.num_rows() == 0 {
            continue;
        }
        let schema = chunk.batch.schema();
        let row_id_idx = schema
            .fields()
            .iter()
            .position(|f| f.name() == ICEBERG_ROW_ID_COL)
            .ok_or_else(|| {
                format!(
                    "iceberg optimize row-lineage SELECT did not return `{ICEBERG_ROW_ID_COL}` column"
                )
            })?;
        let last_updated_idx = schema
            .fields()
            .iter()
            .position(|f| f.name() == ICEBERG_LAST_UPDATED_SEQ_COL)
            .ok_or_else(|| {
                format!(
                    "iceberg optimize row-lineage SELECT did not return `{ICEBERG_LAST_UPDATED_SEQ_COL}` column"
                )
            })?;

        let row_ids = chunk
            .batch
            .column(row_id_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| format!("iceberg optimize `{ICEBERG_ROW_ID_COL}` column must be Int64"))?
            .clone();
        let last_updated = chunk
            .batch
            .column(last_updated_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                format!("iceberg optimize `{ICEBERG_LAST_UPDATED_SEQ_COL}` column must be Int64")
            })?
            .clone();

        // Strip the two trailing lineage columns from the user-facing
        // payload while keeping the rest of the schema (and field-id
        // metadata) intact. The downstream writer re-attaches the lineage
        // columns at their reserved field IDs.
        let mut keep: Vec<usize> = (0..schema.fields().len())
            .filter(|i| *i != row_id_idx && *i != last_updated_idx)
            .collect();
        keep.sort();
        let user_fields: Vec<_> = keep.iter().map(|i| schema.fields()[*i].clone()).collect();
        let user_columns: Vec<_> = keep
            .iter()
            .map(|i| chunk.batch.column(*i).clone())
            .collect();
        let user_schema = Arc::new(Schema::new_with_metadata(
            user_fields,
            schema.metadata().clone(),
        ));
        let user_batch = arrow::record_batch::RecordBatch::try_new(user_schema, user_columns)
            .map_err(|e| format!("iceberg optimize rebuild user batch failed: {e}"))?;

        batches.push(RowLineageWriteBatch {
            user_batch,
            lineage: RowLineageColumns {
                row_ids,
                last_updated_sequence_numbers: last_updated,
            },
        });
    }
    Ok(batches)
}

/// Sum the rows across a set of row-lineage write batches.
fn row_lineage_batches_row_count(batches: &[RowLineageWriteBatch]) -> Result<i64, String> {
    batches.iter().try_fold(0_i64, |sum, batch| {
        let rows = i64::try_from(batch.user_batch.num_rows())
            .map_err(|_| "iceberg optimize selected row count overflow".to_string())?;
        sum.checked_add(rows)
            .ok_or_else(|| "iceberg optimize selected row count overflow".to_string())
    })
}

/// Read the FULL physical payload of an Iceberg table (every column in
/// `current_schema()`, including columns that the analyzer hides from
/// `SELECT *` such as the IMV apply-key column) plus the V3 row-lineage
/// metadata columns, and split each batch into a [`RowLineageWriteBatch`] for
/// the row-lineage writer.
///
/// This bypasses the SQL analyzer entirely (which would hide MV-internal
/// columns) by scanning the `iceberg::table::Table` directly via the same
/// `scan().select(...)` + `ArrowReaderBuilder` mechanism that
/// `locate_target_rows_by_apply_key` uses. Crucially, and unlike the locator,
/// this path does NOT clear `task.deletes`: positional/equality deletes are
/// applied by the reader so deleted rows are not resurrected into the rewritten
/// files. The reader also synthesizes `_row_id` / `_last_updated_sequence_number`
/// per the Iceberg V3 spec (`first_row_id + _pos` fallback, stored value when
/// present), matching the regular scan path so row identity is preserved.
///
/// iceberg-rust 0.9 reads `first_row_id` directly from the data file's manifest
/// field and does NOT perform the V3 manifest-level `first_row_id` inheritance
/// (it leaves `FileScanTask.first_row_id = None` for files whose `first_row_id`
/// is inherited from the manifest rather than stored on the data file). The
/// regular NovaRocks scan path inherits it via `build_read_snapshot_at`, so to
/// match that behavior we precompute the inherited `first_row_id` per data-file
/// path and patch it onto each `FileScanTask` before reading. Without this, the
/// reader would fail with "_row_id metadata column was projected but
/// first_row_id is missing" for inherited-id files.
///
/// The physical columns are reassembled into the writer's user payload in
/// EXACTLY `current_schema()` field order (mapped by name, robust to the
/// reader's projection ordering), because the downstream `annotate_batch`
/// reannotates columns positionally against the writer schema. The two lineage
/// columns are routed into [`RowLineageColumns`] (NOT the user payload), so the
/// writer stamps them at their reserved field ids while the apply-key column
/// rides through as an ordinary physical column at its real field id.
async fn read_full_physical_row_lineage_batches(
    table: &iceberg::table::Table,
) -> Result<Vec<RowLineageWriteBatch>, String> {
    use std::collections::HashMap;

    use arrow::array::Int64Array;
    use arrow::datatypes::Schema;
    use futures::StreamExt;
    use iceberg::arrow::ArrowReaderBuilder;

    // Physical column names in `current_schema()` field order. The apply-key
    // column (and any hidden aggregate-state columns) are physical fields, so
    // they appear here and will be carried through verbatim.
    let physical_field_names: Vec<String> = table
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .map(|f| f.name.clone())
        .collect();
    if physical_field_names.is_empty() {
        return Err("iceberg optimize direct read: table has no physical columns".to_string());
    }

    // Precompute the V3-inherited `first_row_id` per data-file path, mirroring
    // the regular NovaRocks scan path (`build_read_snapshot_at`). iceberg-rust's
    // scan does not inherit `first_row_id` from the manifest, so we patch the
    // tasks below using this map.
    let Some(current_snapshot_id) = table.metadata().current_snapshot_id() else {
        return Err("iceberg optimize direct read: table has no current snapshot".to_string());
    };
    let first_row_id_by_path: HashMap<String, Option<i64>> =
        crate::connector::iceberg::catalog::registry::extract_data_files_with_stats_at(
            table,
            current_snapshot_id,
        )?
        .into_iter()
        .map(|f| (f.path, f.first_row_id))
        .collect();

    // Select every physical column plus the two V3 row-lineage metadata columns.
    let mut select_columns = physical_field_names.clone();
    select_columns.push(ICEBERG_ROW_ID_COL.to_string());
    select_columns.push(ICEBERG_LAST_UPDATED_SEQ_COL.to_string());

    let scan = table
        .scan()
        .select(select_columns)
        .build()
        .map_err(|e| format!("build iceberg optimize physical-read scan failed: {e}"))?;
    let task_stream = scan
        .plan_files()
        .await
        .map_err(|e| format!("plan iceberg optimize physical-read files failed: {e}"))?;
    // Patch the V3-inherited `first_row_id` onto each task (iceberg-rust does
    // not inherit it from the manifest), and leave `task.deletes` intact so the
    // reader applies them.
    let patched_tasks = task_stream.map(move |task_result| {
        let mut task = task_result?;
        if task.first_row_id.is_none()
            && let Some(Some(inherited)) = first_row_id_by_path.get(&task.data_file_path)
        {
            task.first_row_id = Some(*inherited);
        }
        Ok(task)
    });
    let arrow_reader = ArrowReaderBuilder::new(table.file_io().clone()).build();
    let mut stream = arrow_reader
        .read(Box::pin(patched_tasks))
        .map_err(|e| format!("read iceberg optimize physical-read scan failed: {e}"))?;

    let mut batches = Vec::new();
    while let Some(batch_result) = stream.next().await {
        let batch =
            batch_result.map_err(|e| format!("iceberg optimize physical-read scan error: {e}"))?;
        if batch.num_rows() == 0 {
            continue;
        }
        let schema = batch.schema();
        let column_index_by_name = |name: &str| -> Result<usize, String> {
            schema.index_of(name).map_err(|e| {
                format!("iceberg optimize physical-read scan missing column `{name}`: {e}")
            })
        };

        let row_id_idx = column_index_by_name(ICEBERG_ROW_ID_COL)?;
        let last_updated_idx = column_index_by_name(ICEBERG_LAST_UPDATED_SEQ_COL)?;
        let row_ids = batch
            .column(row_id_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| format!("iceberg optimize `{ICEBERG_ROW_ID_COL}` column must be Int64"))?
            .clone();
        let last_updated = batch
            .column(last_updated_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                format!("iceberg optimize `{ICEBERG_LAST_UPDATED_SEQ_COL}` column must be Int64")
            })?
            .clone();

        // Reassemble the physical payload in current_schema() field order,
        // mapping by name so we do not depend on the reader's projection order.
        let mut user_fields = Vec::with_capacity(physical_field_names.len());
        let mut user_columns = Vec::with_capacity(physical_field_names.len());
        for name in &physical_field_names {
            let idx = column_index_by_name(name)?;
            user_fields.push(schema.fields()[idx].clone());
            user_columns.push(batch.column(idx).clone());
        }
        let user_schema = Arc::new(Schema::new_with_metadata(
            user_fields,
            schema.metadata().clone(),
        ));
        let user_batch = arrow::record_batch::RecordBatch::try_new(user_schema, user_columns)
            .map_err(|e| format!("iceberg optimize rebuild physical user batch failed: {e}"))?;

        batches.push(RowLineageWriteBatch {
            user_batch,
            lineage: RowLineageColumns {
                row_ids,
                last_updated_sequence_numbers: last_updated,
            },
        });
    }
    Ok(batches)
}

fn data_file_record_count(data_files: &[DataFile]) -> Result<i64, String> {
    data_files.iter().try_fold(0_i64, |sum, data_file| {
        let rows = i64::try_from(data_file.record_count())
            .map_err(|_| "iceberg optimize output row count overflow".to_string())?;
        sum.checked_add(rows)
            .ok_or_else(|| "iceberg optimize output row count overflow".to_string())
    })
}

fn cleanup_written_data_files_after_error(
    entry: &IcebergCatalogEntry,
    data_files: &[DataFile],
    reason: String,
) -> String {
    if data_files.is_empty() {
        return reason;
    }
    let abort_cleanup = match build_abort_cleanup_for_catalog_entry(entry) {
        Ok(abort_cleanup) => abort_cleanup,
        Err(cleanup_err) => {
            return format!(
                "{reason}; failed to build cleanup operator for {} written optimize data file(s): {cleanup_err}",
                data_files.len()
            );
        }
    };
    let abort_log = AbortLog::new();
    for data_file in data_files {
        abort_log.record_data_file(data_file.file_path().to_string());
    }
    match block_on_iceberg(async {
        if let Some(mapper) = abort_cleanup.path_mapper {
            abort_log
                .cleanup_with_path_mapper(&abort_cleanup.fs, |path| mapper(path))
                .await
        } else {
            abort_log.cleanup(&abort_cleanup.fs).await
        }
    }) {
        Ok(cleanup_errors) if cleanup_errors.is_empty() => format!(
            "{reason}; cleaned {} written optimize data file(s)",
            data_files.len()
        ),
        Ok(cleanup_errors) => format!(
            "{reason}; attempted cleanup for {} written optimize data file(s), {} cleanup error(s)",
            data_files.len(),
            cleanup_errors.len()
        ),
        Err(cleanup_err) => format!(
            "{reason}; cleanup failed for {} written optimize data file(s): {cleanup_err}",
            data_files.len()
        ),
    }
}

fn compact_commit_error_to_user_message(
    err: crate::connector::iceberg::commit::CommitServiceError,
) -> String {
    crate::common::engine_error::EngineError::from(err).to_bracketed_user_message()
}

fn quote_ident(ident: &str) -> String {
    format!("`{}`", ident.replace('`', "``"))
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::connector::iceberg::commit::{
        CleanupAttempt, CommitOpKind, CommitServiceError, RecoveryEvidence,
    };
    use crate::engine::StandaloneState;
    use crate::meta::repository::job::{
        CreateIcebergOptimizeJobRequest, IcebergOptimizeJobOutcome, IcebergOptimizeJobState,
    };
    use crate::meta::{MetaStoreProvider, SqliteMetaStoreProvider};

    use super::{
        compact_commit_error_to_user_message, quote_ident, reconcile_running_optimize_jobs_once,
    };

    #[test]
    fn quote_ident_backtick_quotes_and_escapes_backticks() {
        assert_eq!(quote_ident("orders"), "`orders`");
        assert_eq!(quote_ident("line`item"), "`line``item`");
    }

    #[test]
    fn compact_formats_known_uncommitted_without_marking_unknown() {
        let err = CommitServiceError::known_uncommitted(
            "catalog commit conflict".to_string(),
            CleanupAttempt::completed(Vec::new()),
        );

        let message = compact_commit_error_to_user_message(err);

        assert_eq!(
            message,
            "[CommitKnownUncommitted] iceberg commit failed: catalog commit conflict; abort cleanup ran (0 error(s))"
        );
    }

    #[test]
    fn compact_formats_unknown_with_recovery_evidence() {
        let staging_dir = "s3://warehouse/optimize_t/_staging/rewrite".to_string();
        let err = CommitServiceError::unknown(
            "connection reset by peer".to_string(),
            RecoveryEvidence {
                table_ident: "ice.db.optimize_t".to_string(),
                op_kind: CommitOpKind::RewriteDataFiles,
                base_snapshot_id: Some(15),
                base_sequence_number: 4,
                staging_dir: staging_dir.clone(),
            },
        );

        let message = compact_commit_error_to_user_message(err);

        assert!(
            message.starts_with("[CommitUnknown] iceberg commit unknown"),
            "got: {message}"
        );
        assert!(
            message.contains(&staging_dir),
            "message should contain staging dir {staging_dir}, got: {message}"
        );
    }

    #[test]
    fn reconcile_running_optimize_jobs_finishes_recorded_outcome() {
        let dir = tempfile::tempdir().expect("tempdir");
        let provider = Arc::new(
            SqliteMetaStoreProvider::open(dir.path().join("metadata.sqlite"))
                .expect("open provider"),
        );
        let state = Arc::new(StandaloneState {
            metadata_provider: Some(provider.clone()),
            ..Default::default()
        });
        let mut txn = provider.begin_write("create optimize job").expect("write");
        let job = state
            .job_repo
            .create_iceberg_optimize_job(
                txn.as_mut(),
                CreateIcebergOptimizeJobRequest {
                    catalog: "ice".to_string(),
                    namespace: "ns".to_string(),
                    table: "orders".to_string(),
                    base_snapshot_id: 10,
                    now_ms: 1_000,
                },
            )
            .expect("create job");
        state
            .job_repo
            .claim_iceberg_optimize_job(txn.as_mut(), job.id, 1_100)
            .expect("claim job");
        let outcome = IcebergOptimizeJobOutcome {
            target_snapshot_id: Some(11),
            rewritten_data_files: 2,
            deleted_data_files: 1,
            added_data_files: 1,
            output_record_count: 7,
        };
        state
            .job_repo
            .record_iceberg_optimize_job_outcome(txn.as_mut(), job.id, 1_200, outcome.clone())
            .expect("record outcome");
        txn.commit().expect("commit seed");

        let changed = reconcile_running_optimize_jobs_once(&state).expect("reconcile");

        assert_eq!(changed, 1);
        let read = provider.begin_read().expect("read");
        let jobs = state
            .job_repo
            .show_iceberg_optimize_jobs(read.as_ref())
            .expect("show jobs");
        assert_eq!(jobs[0].state, IcebergOptimizeJobState::Finished);
        assert_eq!(jobs[0].outcome, Some(outcome));
    }
}
