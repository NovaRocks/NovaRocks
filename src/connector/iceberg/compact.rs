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
use crate::connector::iceberg::catalog::registry::{block_on_iceberg, build_hadoop_catalog};
use crate::connector::iceberg::catalog::row_lineage_enabled;
use crate::connector::iceberg::commit::{
    AbortLog, CommitOpKind, IcebergCommitCollector, RunInput, count_current_live_files,
    run_iceberg_commit,
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
    let target = TargetBackend {
        backend_name: "iceberg",
        catalog: job.catalog.clone(),
        namespace: job.namespace.clone(),
        table: job.table.clone(),
    };
    let entry = {
        let registry = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        registry.get(&job.catalog)?
    };
    entry.invalidate_table_cache(&job.namespace, &job.table);

    let hadoop_catalog = build_hadoop_catalog(&entry)?;
    let catalog: Arc<dyn Catalog> = Arc::new(hadoop_catalog);
    let table_ident = TableIdent::new(
        NamespaceIdent::new(job.namespace.clone()),
        job.table.clone(),
    );
    let table = load_current_table(catalog.as_ref(), &table_ident, job)?;
    validate_base_snapshot(&table, job)?;

    let (initial_data_files, initial_delete_files) =
        block_on_iceberg(count_current_live_files(&table, table.file_io()))??;
    if initial_data_files == 0 && initial_delete_files == 0 {
        tracing::info!(
            job_id = job.id,
            catalog = job.catalog,
            namespace = job.namespace,
            table = job.table,
            base_snapshot_id = job.base_snapshot_id,
            "iceberg optimize no-op: table has no live files"
        );
        return Ok(IcebergOptimizeJobOutcome {
            target_snapshot_id: None,
            rewritten_data_files: 0,
            deleted_data_files: 0,
            added_data_files: 0,
            output_record_count: 0,
        });
    }

    let preserve_row_lineage = row_lineage_enabled(table.metadata());
    let select_sql = if preserve_row_lineage {
        format!(
            "SELECT *, {ICEBERG_ROW_ID_COL}, {ICEBERG_LAST_UPDATED_SEQ_COL} FROM {}.{}.{}",
            quote_ident(&job.catalog),
            quote_ident(&job.namespace),
            quote_ident(&job.table)
        )
    } else {
        format!(
            "SELECT * FROM {}.{}.{}",
            quote_ident(&job.catalog),
            quote_ident(&job.namespace),
            quote_ident(&job.table)
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
    let output_record_count = data_file_record_count(&data_files)?;
    if output_record_count != visible_rows {
        return Err(format!(
            "iceberg optimize output row count mismatch: selected {visible_rows}, wrote {output_record_count}"
        ));
    }

    let post_write = (|| {
        let table = load_current_table(catalog.as_ref(), &table_ident, job)?;
        validate_base_snapshot(&table, job)?;
        let (input_data_files, input_delete_files) =
            block_on_iceberg(count_current_live_files(&table, table.file_io()))??;
        Ok::<_, String>((table, input_data_files, input_delete_files))
    })();
    let (table, input_data_files, input_delete_files) = match post_write {
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
    let collector = Arc::new(IcebergCommitCollector::new(
        CommitOpKind::RewriteDataFiles,
        table_ident,
        metadata.current_snapshot().map(|s| s.snapshot_id()),
        metadata.last_sequence_number(),
        metadata.current_schema().clone(),
        metadata.default_partition_spec().clone(),
        staging_dir,
        UniqueId { hi: 0, lo: 0 },
    ));
    if preserve_row_lineage {
        collector.mark_preserve_row_lineage();
    }
    let default_spec_id = metadata.default_partition_spec_id();
    for data_file in &data_files {
        collector.inject_written_file(data_file_to_written_file(data_file, default_spec_id)?);
    }

    let abort_cleanup = build_abort_cleanup_for_catalog_entry(&entry)?;
    let file_io = table.file_io().clone();
    let commit_outcome = block_on_iceberg(run_iceberg_commit(RunInput {
        collector,
        catalog: Arc::clone(&catalog),
        table,
        fs: abort_cleanup.fs,
        file_io,
        cleanup_path_mapper: abort_cleanup.path_mapper,
        cow_update_rewrite: None,
        target_ref: "main".to_string(),
        snapshot_properties: BTreeMap::new(),
    }))??;

    invalidate_iceberg_caches(state, &target)?;

    tracing::info!(
        job_id = job.id,
        catalog = job.catalog,
        namespace = job.namespace,
        table = job.table,
        base_snapshot_id = job.base_snapshot_id,
        target_snapshot_id = commit_outcome.new_snapshot_id,
        input_data_files,
        input_delete_files,
        output_data_files = data_files.len(),
        output_record_count,
        "iceberg optimize finished"
    );

    Ok(IcebergOptimizeJobOutcome {
        target_snapshot_id: Some(commit_outcome.new_snapshot_id),
        rewritten_data_files: input_data_files,
        deleted_data_files: input_delete_files,
        added_data_files: i64::try_from(data_files.len())
            .map_err(|_| "iceberg optimize output data file count overflow".to_string())?,
        output_record_count,
    })
}

fn load_current_table(
    catalog: &dyn Catalog,
    table_ident: &TableIdent,
    job: &StoredIcebergOptimizeJob,
) -> Result<iceberg::table::Table, String> {
    block_on_iceberg(async { catalog.load_table(table_ident).await })?.map_err(|e| {
        format!(
            "load iceberg table {}.{}.{} for optimize job {} failed: {e}",
            job.catalog, job.namespace, job.table, job.id
        )
    })
}

fn validate_base_snapshot(
    table: &iceberg::table::Table,
    job: &StoredIcebergOptimizeJob,
) -> Result<(), String> {
    let current_snapshot_id = table
        .metadata()
        .current_snapshot()
        .map(|snapshot| snapshot.snapshot_id())
        .ok_or_else(|| {
            format!(
                "iceberg optimize job {} requires {}.{}.{} to have current snapshot {}",
                job.id, job.catalog, job.namespace, job.table, job.base_snapshot_id
            )
        })?;
    if current_snapshot_id != job.base_snapshot_id {
        return Err(format!(
            "iceberg optimize job {} base snapshot mismatch for {}.{}.{}: expected {}, current {}",
            job.id,
            job.catalog,
            job.namespace,
            job.table,
            job.base_snapshot_id,
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

    use crate::engine::StandaloneState;
    use crate::meta::repository::job::{
        CreateIcebergOptimizeJobRequest, IcebergOptimizeJobOutcome, IcebergOptimizeJobState,
    };
    use crate::meta::{MetaStoreProvider, SqliteMetaStoreProvider};

    use super::{quote_ident, reconcile_running_optimize_jobs_once};

    #[test]
    fn quote_ident_backtick_quotes_and_escapes_backticks() {
        assert_eq!(quote_ident("orders"), "`orders`");
        assert_eq!(quote_ident("line`item"), "`line``item`");
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
