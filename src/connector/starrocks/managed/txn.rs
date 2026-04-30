use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{ArrayRef, UInt32Array, new_null_array};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::common::ids::SlotId;
use crate::connector::starrocks::ObjectStoreProfile;
use crate::connector::starrocks::lake::append_lake_txn_log_with_chunk_rowset;
use crate::connector::starrocks::lake::context::{
    PartialUpdateWritePolicy, TabletWriteContext, update_tablet_runtime_schema,
};
use crate::connector::starrocks::lake::transactions::publish_version;
use crate::connector::starrocks::lake::txn_log::append_lake_txn_log_empty_rowset;
use crate::connector::starrocks::managed::mv_agg_state::{self, AggregateMvLayout};
use crate::connector::starrocks::sink::routing::{
    build_unpartitioned_hash_routing, route_chunk_rows,
};
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::formats::starrocks::data::build_native_record_batch;
use crate::formats::starrocks::metadata::{load_bundle_segment_footers, load_tablet_snapshot};
use crate::formats::starrocks::plan::build_native_read_plan;
use crate::formats::starrocks::writer::StarRocksWriteFormat;
use crate::fs::path::{ScanPathScheme, classify_scan_paths};
use crate::runtime::query_result::QueryResult;
use crate::runtime::starlet_shard_registry::S3StoreConfig;
use crate::service::grpc_client::proto::starrocks::{
    KeysType, PublishVersionRequest, TabletSchemaPb,
};
use crate::sql::parser::ast::{InsertSource, Literal, ObjectName};

use super::catalog::register_managed_table_in_catalog;
use crate::engine::catalog::{ColumnDef, normalize_identifier};
use crate::engine::{
    ResolvedLocalTableName, StandaloneState, StatementResult, build_local_insert_batch,
    execute_query, insert_generate_series_rows_local, record_batch_to_chunk, reorder_insert_rows,
};

/// Insert rows into a standalone managed-lake table: prepare a txn in the
/// control plane, route rows across tablets, append native-format rowsets,
/// then publish_version and advance the visible partition version.
/// Expand an `InsertSource` into a flat `Vec<Vec<Literal>>` ready to pass to
/// `build_local_insert_batch`. Recursively unfolds UNION ALL chunks.
fn materialize_insert_rows(
    source: &InsertSource,
    insert_columns: &[String],
    target_columns: &[ColumnDef],
) -> Result<Vec<Vec<crate::sql::parser::ast::Literal>>, String> {
    match source {
        InsertSource::Values(rows) => reorder_insert_rows(rows, insert_columns, target_columns),
        InsertSource::SelectLiteralRow(row) => {
            reorder_insert_rows(std::slice::from_ref(row), insert_columns, target_columns)
        }
        InsertSource::GenerateSeriesSelect(gen_source) => {
            insert_generate_series_rows_local(gen_source, insert_columns, target_columns)
        }
        InsertSource::UnionAll(parts) => {
            let mut out = Vec::new();
            for part in parts {
                out.extend(materialize_insert_rows(
                    part,
                    insert_columns,
                    target_columns,
                )?);
            }
            Ok(out)
        }
        // FromQuery is handled separately at the INSERT entry point: it
        // drives the plan pipeline instead of producing literal rows here.
        InsertSource::FromQuery(_) => Err(
            "InsertSource::FromQuery must be dispatched via insert_from_query_into_managed_lake"
                .to_string(),
        ),
    }
}

pub(crate) fn insert_into_managed_lake_table(
    state: &Arc<StandaloneState>,
    name: &ObjectName,
    insert_columns: &[String],
    source: &InsertSource,
    current_database: &str,
) -> Result<StatementResult, String> {
    let resolved = resolve_managed_name(name, current_database)?;
    let plan = load_insert_plan(state, &resolved, PartitionTarget::Active)?;

    // INSERT ... SELECT from a real relation cannot be reduced to literal
    // rows in the parser. Dispatch it through the plan/pipeline executor to
    // stay aligned with how StarRocks wraps INSERT-SELECT (a normal SELECT
    // plan plus a table-writing sink), then hand the materialised result to
    // the same txn/write/publish sequence used by VALUES INSERT.
    if let InsertSource::FromQuery(query) = source {
        return insert_from_query_into_managed_lake(state, &resolved, &plan, insert_columns, query);
    }

    let rows = materialize_insert_rows(source, insert_columns, &plan.columns)?;
    if rows.is_empty() {
        return Ok(StatementResult::Ok);
    }

    let batch = build_local_insert_batch(&plan.columns, &rows)?;
    let chunk = build_chunk_for_insert(batch, plan.columns.len())?;
    write_chunks_into_managed_partition(state, plan, &[chunk])?;
    Ok(StatementResult::Ok)
}

pub(crate) fn insert_rows_into_managed_lake_table(
    state: &Arc<StandaloneState>,
    database: &str,
    table: &str,
    rows: &[Vec<Literal>],
) -> Result<(), String> {
    let resolved = ResolvedLocalTableName {
        database: normalize_identifier(database)?,
        table: normalize_identifier(table)?,
    };
    let plan = load_insert_plan(state, &resolved, PartitionTarget::Active)?;
    if rows.is_empty() {
        return Ok(());
    }
    let batch = build_local_insert_batch(&plan.columns, rows)?;
    let chunk = build_chunk_for_insert(batch, plan.columns.len())?;
    write_chunks_into_managed_partition(state, plan, &[chunk])?;
    Ok(())
}

pub(crate) fn insert_batch_into_managed_lake_table(
    state: &Arc<StandaloneState>,
    database: &str,
    table: &str,
    batch: RecordBatch,
) -> Result<(), String> {
    let resolved = ResolvedLocalTableName {
        database: normalize_identifier(database)?,
        table: normalize_identifier(table)?,
    };
    let plan = load_insert_plan(state, &resolved, PartitionTarget::Active)?;
    if batch.num_rows() == 0 {
        return Ok(());
    }
    let chunk = build_chunk_for_insert(batch, plan.columns.len())?;
    write_chunks_into_managed_partition(state, plan, &[chunk])?;
    Ok(())
}

#[derive(Clone, Debug)]
pub(crate) enum PartitionTarget {
    Active,
    Staged {
        partition_id: i64,
        index_id: i64,
        tablet_ids: Vec<i64>,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct ManagedInsertPlan {
    pub(crate) table_id: i64,
    pub(crate) db_id: i64,
    pub(crate) partition_id: i64,
    pub(crate) base_version: i64,
    pub(crate) columns: Vec<ColumnDef>,
    pub(crate) distributed_slot_ids: Vec<SlotId>,
    pub(crate) tablet_schema: TabletSchemaPb,
    pub(crate) tablets: Vec<ManagedInsertTablet>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ManagedInsertColumnMode {
    VisibleOnly,
    Physical,
}

#[derive(Clone, Debug)]
pub(crate) struct ManagedInsertTablet {
    pub(crate) tablet_id: i64,
    pub(crate) tablet_root_path: String,
}

#[derive(Clone, Debug)]
pub(crate) struct MvRefreshWriteMetadata {
    pub(crate) table_id: i64,
    pub(crate) previous_refresh_rows: i64,
    pub(crate) snapshots: BTreeMap<String, i64>,
}

pub(crate) fn load_insert_plan(
    state: &Arc<StandaloneState>,
    resolved: &ResolvedLocalTableName,
    target: PartitionTarget,
) -> Result<ManagedInsertPlan, String> {
    load_insert_plan_with_column_mode(
        state,
        resolved,
        target,
        ManagedInsertColumnMode::VisibleOnly,
    )
}

pub(crate) fn load_physical_insert_plan(
    state: &Arc<StandaloneState>,
    resolved: &ResolvedLocalTableName,
    target: PartitionTarget,
) -> Result<ManagedInsertPlan, String> {
    load_insert_plan_with_column_mode(state, resolved, target, ManagedInsertColumnMode::Physical)
}

fn load_insert_plan_with_column_mode(
    state: &Arc<StandaloneState>,
    resolved: &ResolvedLocalTableName,
    target: PartitionTarget,
    column_mode: ManagedInsertColumnMode,
) -> Result<ManagedInsertPlan, String> {
    let guard = state
        .managed_lake
        .read()
        .expect("standalone managed lake read lock");
    let runtime = guard.table(&resolved.database, &resolved.table)?;
    let (target_partition, _target_index, mut tablets) = match target {
        PartitionTarget::Active => {
            let partition = runtime
                .partitions
                .iter()
                .find(|partition| partition.state == super::store::ManagedPartitionState::Active)
                .cloned()
                .ok_or_else(|| {
                    format!(
                        "managed table {}.{} has no active partition",
                        resolved.database, resolved.table
                    )
                })?;
            let index = runtime
                .indexes
                .iter()
                .find(|index| {
                    index.partition_id == partition.partition_id
                        && index.state == super::store::ManagedIndexState::Active
                })
                .cloned()
                .ok_or_else(|| {
                    format!(
                        "managed table {}.{} has no active base index",
                        resolved.database, resolved.table
                    )
                })?;
            let tablets = runtime
                .tablets
                .iter()
                .filter(|tablet| {
                    tablet.index_id == index.index_id
                        && tablet.partition_id == partition.partition_id
                })
                .cloned()
                .collect::<Vec<_>>();
            (partition, index, tablets)
        }
        PartitionTarget::Staged {
            partition_id,
            index_id,
            tablet_ids,
        } => {
            let partition = runtime
                .partitions
                .iter()
                .find(|partition| partition.partition_id == partition_id)
                .cloned()
                .ok_or_else(|| {
                    format!(
                        "managed table {}.{} is missing staged partition {}",
                        resolved.database, resolved.table, partition_id
                    )
                })?;
            let index = runtime
                .indexes
                .iter()
                .find(|index| index.index_id == index_id && index.partition_id == partition_id)
                .cloned()
                .ok_or_else(|| {
                    format!(
                        "managed table {}.{} is missing staged index {}",
                        resolved.database, resolved.table, index_id
                    )
                })?;
            let tablets = runtime
                .tablets
                .iter()
                .filter(|tablet| {
                    tablet.partition_id == partition_id
                        && tablet.index_id == index_id
                        && tablet_ids.contains(&tablet.tablet_id)
                })
                .cloned()
                .collect::<Vec<_>>();
            if tablets.len() != tablet_ids.len() {
                return Err(format!(
                    "managed table {}.{} is missing staged tablets for partition {}",
                    resolved.database, resolved.table, partition_id
                ));
            }
            (partition, index, tablets)
        }
    };
    if tablets.is_empty() {
        return Err(format!(
            "managed table {}.{} has no tablets",
            resolved.database, resolved.table
        ));
    }
    tablets.sort_by_key(|tablet| tablet.bucket_seq);

    let columns = derive_column_defs_from_runtime(runtime, column_mode)?;
    let distributed_slot_ids = derive_distributed_slot_ids(
        &columns,
        runtime
            .columns
            .iter()
            .filter(|column| column_mode == ManagedInsertColumnMode::Physical || column.visible),
    );
    if distributed_slot_ids.is_empty() {
        if column_mode == ManagedInsertColumnMode::VisibleOnly
            && has_persisted_key_columns(runtime)
            && !selected_columns_include_persisted_key(&columns, runtime)
        {
            return Err(format!(
                "managed table {}.{} distribution key columns are hidden in visible insert mode; use physical insert plan",
                resolved.database, resolved.table
            ));
        }
        return Err(format!(
            "managed table {}.{} has no distribution key columns",
            resolved.database, resolved.table
        ));
    }

    Ok(ManagedInsertPlan {
        table_id: runtime.table.table_id,
        db_id: runtime.table.db_id,
        partition_id: target_partition.partition_id,
        base_version: target_partition.visible_version,
        columns,
        distributed_slot_ids,
        tablet_schema: runtime.tablet_schema.clone(),
        tablets: tablets
            .into_iter()
            .map(|tablet| ManagedInsertTablet {
                tablet_id: tablet.tablet_id,
                tablet_root_path: tablet.tablet_root_path,
            })
            .collect(),
    })
}

pub(crate) fn write_chunks_into_managed_partition(
    state: &Arc<StandaloneState>,
    plan: ManagedInsertPlan,
    chunks: &[Chunk],
) -> Result<i64, String> {
    write_chunks_into_managed_partition_inner(state, plan, chunks, VisibleCommitAction::Plain)
}

pub(crate) fn write_chunks_into_managed_partition_for_mv_refresh(
    state: &Arc<StandaloneState>,
    plan: ManagedInsertPlan,
    chunks: &[Chunk],
    metadata: MvRefreshWriteMetadata,
) -> Result<i64, String> {
    write_chunks_into_managed_partition_inner(
        state,
        plan,
        chunks,
        VisibleCommitAction::MvRefresh(metadata),
    )
}

pub(crate) fn read_active_managed_physical_chunks(
    state: &Arc<StandaloneState>,
    plan: &ManagedInsertPlan,
) -> Result<Vec<Chunk>, String> {
    let managed_config = state.managed_lake_config.as_ref().ok_or_else(|| {
        "standalone managed lake config is missing during physical read".to_string()
    })?;
    let output_schema = Arc::new(Schema::new(
        plan.columns
            .iter()
            .map(|column| {
                Field::new(
                    &column.name,
                    crate::engine::parquet::normalize_map_entries_nullability(&column.data_type),
                    column.nullable,
                )
            })
            .collect::<Vec<_>>(),
    ));

    let mut chunks = Vec::new();
    for tablet in &plan.tablets {
        let object_store_profile =
            object_store_profile_for_tablet_path(&tablet.tablet_root_path, &managed_config.s3)?;
        let snapshot = load_tablet_snapshot(
            tablet.tablet_id,
            plan.base_version,
            &tablet.tablet_root_path,
            object_store_profile.as_ref(),
        )?;
        let segment_footers = load_bundle_segment_footers(
            &snapshot,
            &tablet.tablet_root_path,
            object_store_profile.as_ref(),
        )?;
        let read_plan = build_native_read_plan(&snapshot, &segment_footers, &output_schema, None)?;
        let batch = build_native_record_batch(
            &read_plan,
            &segment_footers,
            &tablet.tablet_root_path,
            object_store_profile.as_ref(),
            &output_schema,
            &[],
        )?;
        if batch.num_rows() > 0 {
            chunks.push(record_batch_to_chunk(batch)?);
        }
    }
    Ok(chunks)
}

pub(crate) fn write_chunks_into_managed_partition_for_aggregate_mv_upsert(
    state: &Arc<StandaloneState>,
    plan: ManagedInsertPlan,
    delta_chunks: &[Chunk],
    layout: &AggregateMvLayout,
    metadata: MvRefreshWriteMetadata,
) -> Result<i64, String> {
    if plan.tablet_schema.keys_type != Some(KeysType::PrimaryKeys as i32) {
        return Err(
            "aggregate MV incremental upsert requires PRIMARY_KEYS physical table".to_string(),
        );
    }

    let old_chunks = read_active_managed_physical_chunks(state, &plan)?;
    let old_rows = mv_agg_state::build_old_state_map(&old_chunks, layout)?;
    let merged_chunks =
        mv_agg_state::merge_aggregate_state_batches(&old_rows, delta_chunks, layout)?;
    write_chunks_into_managed_partition_for_mv_refresh(state, plan, &merged_chunks, metadata)
}

enum VisibleCommitAction {
    Plain,
    MvRefresh(MvRefreshWriteMetadata),
}

fn write_chunks_into_managed_partition_inner(
    state: &Arc<StandaloneState>,
    plan: ManagedInsertPlan,
    chunks: &[Chunk],
    commit_action: VisibleCommitAction,
) -> Result<i64, String> {
    let total_rows = chunks_total_rows(chunks)?;
    let metadata_store = state
        .metadata_store
        .as_ref()
        .ok_or_else(|| "managed lake insert requires sqlite metadata store".to_string())?;
    let prepared =
        metadata_store.prepare_txn(plan.table_id, plan.partition_id, plan.base_version)?;

    let mut written_tablet_ids = Vec::new();
    let mut next_file_seq = 0_u64;
    for chunk in chunks {
        let write_outcome =
            write_routed_chunks(state, &plan, chunk, prepared.txn_id, &mut next_file_seq);
        let chunk_written_ids = match write_outcome {
            Ok(ids) => ids,
            Err(err) => {
                if let Err(abort_err) = metadata_store.mark_txn_aborted(prepared.txn_id) {
                    return Err(format!(
                        "managed-lake write failed: {err}; additionally mark_txn_aborted failed: {abort_err}"
                    ));
                }
                return Err(err);
            }
        };
        written_tablet_ids.extend(chunk_written_ids);
    }

    written_tablet_ids.sort_unstable();
    written_tablet_ids.dedup();

    let written: HashSet<i64> = written_tablet_ids.iter().copied().collect();
    if let Err(err) =
        append_empty_txn_logs_for_unwritten_tablets(state, &plan, prepared.txn_id, &written)
    {
        if let Err(abort_err) = metadata_store.mark_txn_aborted(prepared.txn_id) {
            return Err(format!(
                "managed-lake write failed: {err}; additionally mark_txn_aborted failed: {abort_err}"
            ));
        }
        return Err(err);
    }

    metadata_store.mark_txn_written(prepared.txn_id)?;

    publish_managed_txn(&plan, &prepared).map_err(|err| {
        if let Err(abort_err) = metadata_store.mark_txn_aborted(prepared.txn_id) {
            return format!(
                "managed-lake publish failed: {err}; additionally mark_txn_aborted failed: {abort_err}"
            );
        }
        format!("managed-lake publish failed: {err}")
    })?;

    match commit_action {
        VisibleCommitAction::Plain => {
            metadata_store.mark_txn_visible(prepared.txn_id, prepared.commit_version)?;
        }
        VisibleCommitAction::MvRefresh(metadata) => {
            let last_refresh_rows = metadata
                .previous_refresh_rows
                .checked_add(total_rows)
                .ok_or_else(|| {
                    format!(
                        "managed-lake mv refresh row count overflow: {} + {}",
                        metadata.previous_refresh_rows, total_rows
                    )
                })?;
            metadata_store.mark_txn_visible_with_mv_refresh_metadata(
                prepared.txn_id,
                prepared.commit_version,
                super::store::UpdateMvRefreshMetadataRequest {
                    table_id: metadata.table_id,
                    last_refresh_rows,
                    snapshots: metadata.snapshots,
                },
            )?;
        }
    }
    commit_catalog_visible_version(state, &plan, prepared.commit_version)?;

    Ok(total_rows)
}

fn chunks_total_rows(chunks: &[Chunk]) -> Result<i64, String> {
    chunks.iter().try_fold(0_i64, |acc, chunk| {
        let rows = i64::try_from(chunk.len())
            .map_err(|_| "managed-lake chunk row count overflow".to_string())?;
        acc.checked_add(rows)
            .ok_or_else(|| "managed-lake chunk row count overflow".to_string())
    })
}

fn derive_column_defs_from_runtime(
    runtime: &super::catalog::ManagedTableRuntime,
    mode: ManagedInsertColumnMode,
) -> Result<Vec<ColumnDef>, String> {
    runtime
        .columns
        .iter()
        .filter(|column| mode == ManagedInsertColumnMode::Physical || column.visible)
        .map(|column| {
            let schema_column = runtime
                .tablet_schema
                .column
                .iter()
                .find(|schema_column| {
                    schema_column
                        .name
                        .as_deref()
                        .is_some_and(|name| name.eq_ignore_ascii_case(&column.column_name))
                })
                .ok_or_else(|| {
                    format!(
                        "managed table {}.{} is missing tablet schema column `{}`",
                        runtime.database_name, runtime.table.name, column.column_name
                    )
                })?;
            Ok(ColumnDef {
                name: column.column_name.clone(),
                data_type:
                    crate::connector::starrocks::managed::catalog::arrow_type_from_tablet_column(
                        schema_column,
                    )?,
                nullable: column.nullable,
            })
        })
        .collect()
}

fn derive_distributed_slot_ids<'a>(
    columns: &[ColumnDef],
    stored_columns: impl IntoIterator<Item = &'a super::store::StoredManagedColumn>,
) -> Vec<SlotId> {
    let mut slot_ids = Vec::new();
    for column in stored_columns {
        if !column.is_key {
            continue;
        }
        if let Some(idx) = columns
            .iter()
            .position(|col| col.name.eq_ignore_ascii_case(&column.column_name))
        {
            slot_ids.push(SlotId::new(idx as u32 + 1));
        }
    }
    slot_ids
}

fn has_persisted_key_columns(runtime: &super::catalog::ManagedTableRuntime) -> bool {
    runtime.columns.iter().any(|column| column.is_key)
}

fn selected_columns_include_persisted_key(
    columns: &[ColumnDef],
    runtime: &super::catalog::ManagedTableRuntime,
) -> bool {
    runtime
        .columns
        .iter()
        .filter(|column| column.is_key)
        .any(|key_column| {
            columns
                .iter()
                .any(|column| column.name.eq_ignore_ascii_case(&key_column.column_name))
        })
}

fn build_chunk_for_insert(batch: RecordBatch, num_columns: usize) -> Result<Chunk, String> {
    let slot_ids = (1..=num_columns as u32)
        .map(SlotId::new)
        .collect::<Vec<_>>();
    let chunk_schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &slot_ids)?;
    Ok(Chunk::new_with_chunk_schema(batch, chunk_schema))
}

fn write_routed_chunks(
    state: &Arc<StandaloneState>,
    plan: &ManagedInsertPlan,
    chunk: &Chunk,
    txn_id: i64,
    next_file_seq: &mut u64,
) -> Result<Vec<i64>, String> {
    let tablet_ids = plan
        .tablets
        .iter()
        .map(|tablet| tablet.tablet_id)
        .collect::<Vec<_>>();
    let routing = build_unpartitioned_hash_routing(
        tablet_ids,
        plan.distributed_slot_ids.clone(),
        plan.partition_id,
    )?;

    let mut next_random_hash = 0_u32;
    let routed = route_chunk_rows(&routing, chunk, &mut next_random_hash)?;
    if !routed.rejections.is_empty() {
        return Err(format!(
            "managed-lake insert rejected {} rows during routing",
            routed.rejections.len()
        ));
    }

    let managed_config = state
        .managed_lake_config
        .as_ref()
        .ok_or_else(|| "standalone managed lake config is missing during insert".to_string())?
        .clone();

    let mut written_tablet_ids = Vec::new();
    for (tablet_idx, row_indices) in routed.per_tablet.iter().enumerate() {
        if row_indices.is_empty() {
            continue;
        }
        let tablet = &plan.tablets[tablet_idx];
        let routed_chunk = take_chunk_rows(chunk, row_indices)?;
        let write_ctx = TabletWriteContext {
            db_id: plan.db_id,
            table_id: plan.table_id,
            tablet_id: tablet.tablet_id,
            tablet_root_path: tablet.tablet_root_path.clone(),
            tablet_schema: plan.tablet_schema.clone(),
            s3_config: s3_config_for_tablet_path(&tablet.tablet_root_path, &managed_config.s3)?,
            partial_update: PartialUpdateWritePolicy::default(),
        };
        // Keep the tablet runtime's schema in lockstep with what we persist,
        // so concurrent readers/writers see the same logical shape.
        update_tablet_runtime_schema(tablet.tablet_id, &plan.tablet_schema)?;
        let file_seq = *next_file_seq;
        *next_file_seq = file_seq.saturating_add(1);
        append_lake_txn_log_with_chunk_rowset(
            &write_ctx,
            &routed_chunk,
            txn_id,
            0,
            file_seq,
            StarRocksWriteFormat::Native,
            plan.partition_id,
            None,
        )?;
        written_tablet_ids.push(tablet.tablet_id);
    }
    Ok(written_tablet_ids)
}

fn take_chunk_rows(chunk: &Chunk, row_indices: &[u32]) -> Result<Chunk, String> {
    if row_indices.len() == chunk.len() {
        return Ok(chunk.clone());
    }
    let indices = UInt32Array::from(row_indices.to_vec());
    let columns = chunk
        .batch
        .columns()
        .iter()
        .map(|column| arrow::compute::take(column.as_ref(), &indices, None))
        .collect::<arrow::error::Result<Vec<_>>>()
        .map_err(|e| format!("take routed rows failed: {e}"))?;
    let batch = RecordBatch::try_new(chunk.batch.schema(), columns)
        .map_err(|e| format!("build routed batch failed: {e}"))?;
    Ok(Chunk::new_with_chunk_schema(
        batch,
        chunk.chunk_schema_ref(),
    ))
}

fn publish_managed_txn(
    plan: &ManagedInsertPlan,
    prepared: &super::store::PreparedManagedTxn,
) -> Result<(), String> {
    // Publish the whole partition in one batch. Splitting written and empty
    // tablets into separate publish calls can make the second bundle write
    // synthesize siblings from the old base version and overwrite rowsets.
    let tablet_ids = plan
        .tablets
        .iter()
        .map(|tablet| tablet.tablet_id)
        .collect::<Vec<_>>();
    if !tablet_ids.is_empty() {
        publish_tablets_at_version(
            tablet_ids,
            prepared.txn_id,
            prepared.base_version,
            prepared.commit_version,
        )?;
    }
    Ok(())
}

fn append_empty_txn_logs_for_unwritten_tablets(
    state: &Arc<StandaloneState>,
    plan: &ManagedInsertPlan,
    txn_id: i64,
    written_tablet_ids: &HashSet<i64>,
) -> Result<(), String> {
    let managed_config = state
        .managed_lake_config
        .as_ref()
        .ok_or_else(|| "standalone managed lake config is missing during insert".to_string())?
        .clone();

    for tablet in &plan.tablets {
        if written_tablet_ids.contains(&tablet.tablet_id) {
            continue;
        }
        let write_ctx = TabletWriteContext {
            db_id: plan.db_id,
            table_id: plan.table_id,
            tablet_id: tablet.tablet_id,
            tablet_root_path: tablet.tablet_root_path.clone(),
            tablet_schema: plan.tablet_schema.clone(),
            s3_config: s3_config_for_tablet_path(&tablet.tablet_root_path, &managed_config.s3)?,
            partial_update: PartialUpdateWritePolicy::default(),
        };
        update_tablet_runtime_schema(tablet.tablet_id, &plan.tablet_schema)?;
        append_lake_txn_log_empty_rowset(&write_ctx, txn_id, plan.partition_id, None)?;
    }
    Ok(())
}

fn s3_config_for_tablet_path(
    tablet_root_path: &str,
    managed_s3: &S3StoreConfig,
) -> Result<Option<S3StoreConfig>, String> {
    match classify_scan_paths([tablet_root_path])? {
        ScanPathScheme::Local => Ok(None),
        ScanPathScheme::Oss => Ok(Some(managed_s3.clone())),
        ScanPathScheme::Hdfs => Err(format!(
            "managed-lake write does not support hdfs tablet path yet: {tablet_root_path}"
        )),
    }
}

fn object_store_profile_for_tablet_path(
    tablet_root_path: &str,
    managed_s3: &S3StoreConfig,
) -> Result<Option<ObjectStoreProfile>, String> {
    s3_config_for_tablet_path(tablet_root_path, managed_s3)?
        .as_ref()
        .map(ObjectStoreProfile::from_s3_store_config)
        .transpose()
}

/// Drive `publish_version` for a specific txn against the given tablet ids.
/// Also used by restart recovery to finish a `WRITTEN` txn whose rowsets are
/// already on object storage.
pub(crate) fn publish_tablets_at_version(
    tablet_ids: Vec<i64>,
    txn_id: i64,
    base_version: i64,
    commit_version: i64,
) -> Result<(), String> {
    let request = PublishVersionRequest {
        tablet_ids,
        txn_ids: vec![txn_id],
        base_version: Some(base_version),
        new_version: Some(commit_version),
        commit_time: None,
        timeout_ms: None,
        txn_infos: Vec::new(),
        rebuild_pindex_tablet_ids: Vec::new(),
        enable_aggregate_publish: None,
        resharding_tablet_infos: Vec::new(),
    };
    let response = publish_version(&request)?;
    if !response.failed_tablets.is_empty() {
        return Err(format!(
            "publish_version failed for tablets {:?}",
            response.failed_tablets
        ));
    }
    Ok(())
}

fn commit_catalog_visible_version(
    state: &Arc<StandaloneState>,
    plan: &ManagedInsertPlan,
    new_visible_version: i64,
) -> Result<(), String> {
    let mut managed = state
        .managed_lake
        .write()
        .expect("standalone managed lake write lock");
    let table_id = managed.advance_partition_version(plan.partition_id, new_visible_version)?;
    let runtime = managed
        .runtime_by_table_id(table_id)
        .cloned()
        .ok_or_else(|| format!("managed runtime missing for table_id={table_id}"))?;
    drop(managed);

    let mut catalog = state
        .catalog
        .write()
        .expect("standalone catalog write lock");
    register_managed_table_in_catalog(&mut catalog, &runtime)?;
    Ok(())
}

/// Plan-pipeline path for `INSERT INTO <managed_lake_table> SELECT ...`.
///
/// Matches the StarRocks FE shape of INSERT-SELECT: the SELECT is analyzed,
/// planned, optimised and executed through the normal query stack; the
/// collected output is projected/cast to the target table's column layout
/// and then handed to the managed-lake txn path that `VALUES` INSERT uses.
///
/// The output is materialised into a single Arrow batch before writing.
/// That is fine for the current target workload (INSERT ... SELECT of up to
/// a few hundred thousand rows) but is explicitly a single-node limitation
/// — a true streaming `ManagedLakeSink` operator will be needed once the
/// pipeline needs to run across multiple BEs.
fn insert_from_query_into_managed_lake(
    state: &Arc<StandaloneState>,
    resolved: &ResolvedLocalTableName,
    plan: &ManagedInsertPlan,
    insert_columns: &[String],
    query: &sqlparser::ast::Query,
) -> Result<StatementResult, String> {
    // Resolve SELECT against the target table's database so unqualified
    // references in the SELECT pick up the right schema; matches the INSERT
    // target namespace established by `resolve_managed_name`.
    let query_result = {
        let catalog = state.catalog.read().expect("standalone catalog read lock");
        execute_query(
            query,
            &catalog,
            &resolved.database,
            state.exchange_port,
            None,
        )?
    };

    let aligned = align_query_result_to_target(&query_result, insert_columns, &plan.columns)?;
    if aligned.num_rows() == 0 {
        return Ok(StatementResult::Ok);
    }
    let chunk = build_chunk_for_insert(aligned, plan.columns.len())?;

    write_chunks_into_managed_partition_inner(
        state,
        plan.clone(),
        &[chunk],
        VisibleCommitAction::Plain,
    )?;

    Ok(StatementResult::Ok)
}

/// Project/cast the SELECT output into the target table's schema and
/// concatenate all chunks into a single Arrow batch. Any target column that
/// the INSERT doesn't mention is filled with NULLs; all other columns are
/// placed in target order and cast to the target column's Arrow data type.
fn align_query_result_to_target(
    result: &QueryResult,
    insert_columns: &[String],
    target_columns: &[ColumnDef],
) -> Result<RecordBatch, String> {
    let mapping =
        build_target_column_mapping(insert_columns, target_columns, result.columns.len())?;

    let target_schema = Arc::new(Schema::new(
        target_columns
            .iter()
            .map(|c| {
                Field::new(
                    &c.name,
                    crate::engine::parquet::normalize_map_entries_nullability(&c.data_type),
                    c.nullable,
                )
            })
            .collect::<Vec<_>>(),
    ));

    let column_count = target_columns.len();
    let mut per_target_columns: Vec<Vec<ArrayRef>> = vec![Vec::new(); column_count];
    let mut total_rows = 0_usize;
    for chunk in &result.chunks {
        let batch = &chunk.batch;
        if batch.num_columns() < result.columns.len() {
            return Err(format!(
                "INSERT SELECT chunk has {} columns but query returns {}",
                batch.num_columns(),
                result.columns.len()
            ));
        }
        let chunk_rows = batch.num_rows();
        total_rows += chunk_rows;
        for (target_idx, source_idx) in mapping.iter().enumerate() {
            let target_column = &target_columns[target_idx];
            let target_type =
                crate::engine::parquet::normalize_map_entries_nullability(&target_column.data_type);
            let array: ArrayRef = match source_idx {
                Some(idx) => {
                    let src = batch.column(*idx);
                    if src.data_type() == &target_type {
                        src.clone()
                    } else {
                        arrow::compute::cast(src.as_ref(), &target_type).map_err(|e| {
                            format!(
                                "INSERT SELECT cannot cast column `{}` from {:?} to {:?}: {}",
                                target_column.name,
                                src.data_type(),
                                target_type,
                                e
                            )
                        })?
                    }
                }
                None => new_null_array(&target_type, chunk_rows),
            };
            per_target_columns[target_idx].push(array);
        }
    }

    let mut final_columns: Vec<ArrayRef> = Vec::with_capacity(column_count);
    for (target_idx, arrays) in per_target_columns.into_iter().enumerate() {
        let target_column = &target_columns[target_idx];
        let target_type =
            crate::engine::parquet::normalize_map_entries_nullability(&target_column.data_type);
        let merged: ArrayRef = if arrays.is_empty() {
            new_null_array(&target_type, 0)
        } else if arrays.len() == 1 {
            arrays.into_iter().next().unwrap()
        } else {
            let refs: Vec<&dyn arrow::array::Array> = arrays.iter().map(|a| a.as_ref()).collect();
            arrow::compute::concat(&refs).map_err(|e| {
                format!(
                    "INSERT SELECT failed to concat chunks for column `{}`: {e}",
                    target_column.name
                )
            })?
        };
        final_columns.push(merged);
    }

    if total_rows == 0 {
        return RecordBatch::try_new(target_schema, final_columns)
            .map_err(|e| format!("build empty INSERT SELECT batch failed: {e}"));
    }

    RecordBatch::try_new(target_schema, final_columns)
        .map_err(|e| format!("build INSERT SELECT batch failed: {e}"))
}

/// Produce a `target_index -> Option<source_index>` mapping. `insert_columns`
/// is the user-declared INSERT column list (possibly empty for positional
/// INSERT); `source_column_count` is the arity of the SELECT output.
fn build_target_column_mapping(
    insert_columns: &[String],
    target_columns: &[ColumnDef],
    source_column_count: usize,
) -> Result<Vec<Option<usize>>, String> {
    if insert_columns.is_empty() {
        if source_column_count != target_columns.len() {
            return Err(format!(
                "INSERT SELECT column count mismatch: target has {} columns, SELECT produces {}",
                target_columns.len(),
                source_column_count
            ));
        }
        return Ok((0..target_columns.len()).map(Some).collect());
    }

    if insert_columns.len() != source_column_count {
        return Err(format!(
            "INSERT SELECT column count mismatch: INSERT lists {} columns, SELECT produces {}",
            insert_columns.len(),
            source_column_count
        ));
    }

    let mut insert_index_by_name: HashMap<String, usize> =
        HashMap::with_capacity(insert_columns.len());
    for (idx, column) in insert_columns.iter().enumerate() {
        let key = normalize_identifier(column)?;
        if insert_index_by_name.insert(key, idx).is_some() {
            return Err(format!("duplicate INSERT column `{column}`"));
        }
    }

    let mut mapping = Vec::with_capacity(target_columns.len());
    for column in target_columns {
        let key = normalize_identifier(&column.name)?;
        mapping.push(insert_index_by_name.remove(&key));
    }
    if let Some((name, _)) = insert_index_by_name.into_iter().next() {
        return Err(format!(
            "unknown INSERT column `{name}` not found in target table"
        ));
    }
    Ok(mapping)
}

fn resolve_managed_name(
    name: &ObjectName,
    current_database: &str,
) -> Result<ResolvedLocalTableName, String> {
    use crate::engine::catalog::normalize_identifier;
    match name.parts.as_slice() {
        [table] => Ok(ResolvedLocalTableName {
            database: normalize_identifier(current_database)?,
            table: normalize_identifier(table)?,
        }),
        [database, table] => Ok(ResolvedLocalTableName {
            database: normalize_identifier(database)?,
            table: normalize_identifier(table)?,
        }),
        _ => Err(format!(
            "managed table name must be `<table>` or `<database>.<table>`, got `{}`",
            name.parts.join(".")
        )),
    }
}

#[cfg(test)]
mod mv_target_tests {
    use super::*;

    use crate::connector::starrocks::lake::context::{
        TabletWriteContext, lock_runtime_test_state, register_tablet_runtime,
    };
    use crate::connector::starrocks::lake::txn_log::read_txn_log_if_exists;
    use crate::connector::starrocks::managed::store::{
        ManagedGlobalMeta, ManagedIndexState, ManagedMvRefreshMode, ManagedMvStorageEngine,
        ManagedPartitionState, ManagedSnapshot, ManagedTableKind, ManagedTableState,
        SqliteMetadataStore, StoredManagedDatabase, StoredManagedIndex, StoredManagedPartition,
        StoredManagedSchema, StoredManagedTable, StoredManagedTablet, StoredMaterializedView,
    };
    use crate::connector::starrocks::managed::{
        ManagedLakeCatalog, ManagedLakeConfig, register_managed_tables_in_catalog,
    };
    use crate::engine::catalog::InMemoryCatalog;
    use crate::formats::starrocks::writer::bundle_meta::{
        empty_tablet_metadata, write_bundle_meta_file,
    };
    use crate::formats::starrocks::writer::layout::txn_log_file_path;
    use crate::runtime::starlet_shard_registry::S3StoreConfig;
    use crate::service::grpc_client::proto::starrocks::{ColumnPb, KeysType, TabletSchemaPb};
    use arrow::array::{Array, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use prost::Message;
    use std::net::ToSocketAddrs;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    #[test]
    fn write_chunks_into_managed_partition_routes_rows_to_staged_tablets() {
        let _guard = lock_runtime_test_state();
        let fixture = seed_state_with_staged_mv();
        let plan = load_insert_plan(
            &fixture.state,
            &ResolvedLocalTableName {
                database: "analytics".to_string(),
                table: "orders_mv".to_string(),
            },
            PartitionTarget::Staged {
                partition_id: 21,
                index_id: 31,
                tablet_ids: vec![41, 42],
            },
        )
        .expect("plan");

        assert_eq!(plan.partition_id, 21);
        assert_eq!(
            plan.tablets
                .iter()
                .map(|tablet| tablet.tablet_id)
                .collect::<Vec<_>>(),
            vec![41, 42],
        );
    }

    #[test]
    fn insert_plan_column_modes_split_visible_and_physical_columns() {
        let _guard = lock_runtime_test_state();
        let fixture = seed_state_with_hidden_physical_column_mv();
        let resolved = ResolvedLocalTableName {
            database: "analytics".to_string(),
            table: "orders_mv".to_string(),
        };

        let visible_plan =
            load_insert_plan(&fixture.state, &resolved, PartitionTarget::Active).expect("plan");
        assert_eq!(
            visible_plan
                .columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["k1", "total"],
        );
        assert_eq!(visible_plan.distributed_slot_ids, vec![SlotId::new(1)]);

        let physical_plan =
            load_physical_insert_plan(&fixture.state, &resolved, PartitionTarget::Active)
                .expect("physical plan");
        assert_eq!(
            physical_plan
                .columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["k1", "total", "__nr_shadow_total"],
        );
        assert_eq!(
            physical_plan.distributed_slot_ids,
            vec![SlotId::new(1), SlotId::new(3)],
        );

        let metadata_store = fixture.state.metadata_store.as_ref().expect("store");
        let prepared = metadata_store
            .prepare_txn(
                physical_plan.table_id,
                physical_plan.partition_id,
                physical_plan.base_version,
            )
            .expect("prepare txn");
        let chunk = physical_hidden_key_chunk(&[1, 2, 3], &[101, 102, 103]);
        let mut next_file_seq = 0_u64;
        let written_tablet_ids = write_routed_chunks(
            &fixture.state,
            &physical_plan,
            &chunk,
            prepared.txn_id,
            &mut next_file_seq,
        )
        .expect("write physical chunk");
        assert!(!written_tablet_ids.is_empty());
        assert!(
            written_tablet_ids
                .iter()
                .all(|tablet_id| *tablet_id == 40 || *tablet_id == 43)
        );
    }

    #[test]
    fn visible_insert_plan_reports_hidden_distribution_keys() {
        let _guard = lock_runtime_test_state();
        let fixture = seed_state_with_hidden_only_key_mv();
        let resolved = ResolvedLocalTableName {
            database: "analytics".to_string(),
            table: "orders_mv".to_string(),
        };

        let err =
            load_insert_plan(&fixture.state, &resolved, PartitionTarget::Active).expect_err("err");
        assert_eq!(
            err,
            "managed table analytics.orders_mv distribution key columns are hidden in visible insert mode; use physical insert plan",
        );

        let physical_plan =
            load_physical_insert_plan(&fixture.state, &resolved, PartitionTarget::Active)
                .expect("physical plan");
        assert_eq!(physical_plan.distributed_slot_ids, vec![SlotId::new(3)]);
    }

    #[test]
    fn mv_refresh_noop_chunks_updates_metadata_atomically() {
        let _guard = lock_runtime_test_state();
        let fixture = seed_state_with_active_mv();
        let plan = load_insert_plan(
            &fixture.state,
            &ResolvedLocalTableName {
                database: "analytics".to_string(),
                table: "orders_mv".to_string(),
            },
            PartitionTarget::Active,
        )
        .expect("plan");
        let mut snapshots = std::collections::BTreeMap::new();
        snapshots.insert("ice.ns.orders".to_string(), 42);

        let rows = write_chunks_into_managed_partition_for_mv_refresh(
            &fixture.state,
            plan,
            &[],
            MvRefreshWriteMetadata {
                table_id: 10,
                previous_refresh_rows: 3,
                snapshots: snapshots.clone(),
            },
        )
        .expect("write");
        assert_eq!(rows, 0);

        let store = fixture.state.metadata_store.as_ref().expect("store");
        let loaded = store.load_snapshot().expect("snapshot").managed;
        let mv = loaded
            .materialized_views
            .iter()
            .find(|mv| mv.mv_id == 10)
            .expect("mv");
        assert_eq!(mv.last_refresh_rows, Some(3));
        assert_eq!(mv.last_refresh_snapshots, snapshots);
    }

    #[test]
    fn mv_refresh_write_phase_persists_empty_logs_for_unwritten_tablets() {
        let _guard = lock_runtime_test_state();
        let fixture = seed_state_with_active_mv();
        let plan = load_insert_plan(
            &fixture.state,
            &ResolvedLocalTableName {
                database: "analytics".to_string(),
                table: "orders_mv".to_string(),
            },
            PartitionTarget::Active,
        )
        .expect("plan");
        assert_eq!(plan.tablets.len(), 2);

        let metadata_store = fixture.state.metadata_store.as_ref().expect("store");
        let prepared = metadata_store
            .prepare_txn(plan.table_id, plan.partition_id, plan.base_version)
            .expect("prepare txn");
        let chunk = single_i32_chunk("k1", &[1]);
        let mut next_file_seq = 0_u64;
        let mut written_tablet_ids = write_routed_chunks(
            &fixture.state,
            &plan,
            &chunk,
            prepared.txn_id,
            &mut next_file_seq,
        )
        .expect("write routed chunk");
        written_tablet_ids.sort_unstable();
        written_tablet_ids.dedup();
        assert_eq!(written_tablet_ids.len(), 1);

        let written = written_tablet_ids.iter().copied().collect::<HashSet<_>>();
        append_empty_txn_logs_for_unwritten_tablets(
            &fixture.state,
            &plan,
            prepared.txn_id,
            &written,
        )
        .expect("append empty logs");

        let mut row_counts = Vec::new();
        for tablet in &plan.tablets {
            let log_path =
                txn_log_file_path(&tablet.tablet_root_path, tablet.tablet_id, prepared.txn_id)
                    .expect("txn log path");
            let log = read_txn_log_if_exists(&log_path)
                .expect("read txn log")
                .expect("txn log exists before written boundary");
            let rows = log
                .op_write
                .as_ref()
                .and_then(|op| op.rowset.as_ref())
                .and_then(|rowset| rowset.num_rows)
                .unwrap_or(-1);
            row_counts.push(rows);
        }
        row_counts.sort_unstable();
        assert_eq!(row_counts, vec![0, 1]);
    }

    #[test]
    fn mv_refresh_writes_chunks_when_object_store_available() {
        let Some(config) = maybe_object_store_config() else {
            return;
        };
        let _guard = lock_runtime_test_state();
        let fixture =
            seed_state_with_active_mv_on_object_store(config).expect("object-store fixture");
        let plan = load_insert_plan(
            &fixture.state,
            &ResolvedLocalTableName {
                database: "analytics".to_string(),
                table: "orders_mv".to_string(),
            },
            PartitionTarget::Active,
        )
        .expect("plan");
        let chunk = single_i32_chunk("k1", &[1, 2, 3]);
        let mut snapshots = std::collections::BTreeMap::new();
        snapshots.insert("ice.ns.orders".to_string(), 42);

        let rows = write_chunks_into_managed_partition_for_mv_refresh(
            &fixture.state,
            plan,
            &[chunk],
            MvRefreshWriteMetadata {
                table_id: 10,
                previous_refresh_rows: 7,
                snapshots: snapshots.clone(),
            },
        )
        .expect("write");
        assert_eq!(rows, 3);

        let store = fixture.state.metadata_store.as_ref().expect("store");
        let loaded = store.load_snapshot().expect("snapshot").managed;
        let mv = loaded
            .materialized_views
            .iter()
            .find(|mv| mv.mv_id == 10)
            .expect("mv");
        assert_eq!(mv.last_refresh_rows, Some(10));
        assert_eq!(mv.last_refresh_snapshots, snapshots);
    }

    #[test]
    fn aggregate_mv_upsert_replaces_existing_primary_key_row() {
        let _guard = lock_runtime_test_state();
        let fixture = seed_state_with_aggregate_primary_key_mv();
        let (layout, old_chunks) = aggregate_physical_chunks(&[1], &[2], &[30]);
        let old_plan = load_physical_insert_plan(
            &fixture.state,
            &ResolvedLocalTableName {
                database: "analytics".to_string(),
                table: "orders_mv".to_string(),
            },
            PartitionTarget::Active,
        )
        .expect("old physical plan");
        write_chunks_into_managed_partition(&fixture.state, old_plan, &old_chunks)
            .expect("write old row");

        let delta_chunks = aggregate_physical_chunks(&[1], &[3], &[70]).1;
        let upsert_plan = load_physical_insert_plan(
            &fixture.state,
            &ResolvedLocalTableName {
                database: "analytics".to_string(),
                table: "orders_mv".to_string(),
            },
            PartitionTarget::Active,
        )
        .expect("upsert physical plan");
        write_chunks_into_managed_partition_for_aggregate_mv_upsert(
            &fixture.state,
            upsert_plan,
            &delta_chunks,
            &layout,
            MvRefreshWriteMetadata {
                table_id: 10,
                previous_refresh_rows: 0,
                snapshots: BTreeMap::new(),
            },
        )
        .expect("aggregate upsert");

        let read_plan = load_physical_insert_plan(
            &fixture.state,
            &ResolvedLocalTableName {
                database: "analytics".to_string(),
                table: "orders_mv".to_string(),
            },
            PartitionTarget::Active,
        )
        .expect("read physical plan");
        let active_chunks =
            read_active_managed_physical_chunks(&fixture.state, &read_plan).expect("read active");
        let total_rows = active_chunks.iter().map(Chunk::len).sum::<usize>();
        assert_eq!(total_rows, 1);

        let expected_row_id = delta_chunks[0]
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("delta row id")
            .value(0)
            .to_string();
        let mut seen = None;
        for chunk in active_chunks {
            let batch = chunk.batch;
            let row_ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("row id");
            let c = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("c");
            let s = batch
                .column(3)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("s");
            for row in 0..batch.num_rows() {
                seen = Some((row_ids.value(row).to_string(), c.value(row), s.value(row)));
            }
        }
        assert_eq!(seen, Some((expected_row_id, 5, 100)));

        let store = fixture.state.metadata_store.as_ref().expect("store");
        let loaded = store.load_snapshot().expect("snapshot").managed;
        let mv = loaded
            .materialized_views
            .iter()
            .find(|mv| mv.mv_id == 10)
            .expect("mv");
        assert_eq!(mv.last_refresh_rows, Some(1));
    }

    struct MvTestFixture {
        state: Arc<StandaloneState>,
        _metadata_dir: tempfile::TempDir,
    }

    fn seed_state_with_staged_mv() -> MvTestFixture {
        seed_state_with_mv_fixture(
            false,
            MvFixtureStorage::Local,
            MvFixtureKeyLayout::VisibleOnly,
        )
        .expect("local fixture")
    }

    fn seed_state_with_active_mv() -> MvTestFixture {
        seed_state_with_mv_fixture(
            true,
            MvFixtureStorage::Local,
            MvFixtureKeyLayout::VisibleOnly,
        )
        .expect("local fixture")
    }

    fn seed_state_with_hidden_physical_column_mv() -> MvTestFixture {
        seed_state_with_mv_fixture(
            true,
            MvFixtureStorage::Local,
            MvFixtureKeyLayout::VisibleAndHidden,
        )
        .expect("local fixture")
    }

    fn seed_state_with_hidden_only_key_mv() -> MvTestFixture {
        seed_state_with_mv_fixture(
            true,
            MvFixtureStorage::Local,
            MvFixtureKeyLayout::HiddenOnly,
        )
        .expect("local fixture")
    }

    fn seed_state_with_aggregate_primary_key_mv() -> MvTestFixture {
        let metadata_dir = tempfile::tempdir().expect("create tempdir");
        let metadata_root = metadata_dir.path().to_string_lossy().to_string();
        let config = ManagedLakeConfig {
            warehouse_uri: metadata_root.clone(),
            s3: S3StoreConfig {
                endpoint: "http://127.0.0.1:9000".to_string(),
                bucket: "bucket".to_string(),
                root: "warehouse".to_string(),
                access_key_id: "ak".to_string(),
                access_key_secret: "sk".to_string(),
                region: None,
                enable_path_style_access: Some(true),
            },
            mv_default_storage_engine: "managed_lake".to_string(),
            mv_iceberg_warehouse_location: None,
        };
        let active_tablet_root = format!("{metadata_root}/db_1/table_10/partition_20");
        let tablet_schema = aggregate_primary_key_tablet_schema();
        let columns = vec![
            crate::connector::starrocks::managed::store::StoredManagedColumn {
                schema_id: 100,
                ordinal: 0,
                column_name: "__row_id__".to_string(),
                logical_type: "VARCHAR".to_string(),
                nullable: false,
                visible: false,
                is_key: true,
            },
            crate::connector::starrocks::managed::store::StoredManagedColumn {
                schema_id: 100,
                ordinal: 1,
                column_name: "k1".to_string(),
                logical_type: "BIGINT".to_string(),
                nullable: false,
                visible: true,
                is_key: false,
            },
            crate::connector::starrocks::managed::store::StoredManagedColumn {
                schema_id: 100,
                ordinal: 2,
                column_name: "c".to_string(),
                logical_type: "BIGINT".to_string(),
                nullable: false,
                visible: true,
                is_key: false,
            },
            crate::connector::starrocks::managed::store::StoredManagedColumn {
                schema_id: 100,
                ordinal: 3,
                column_name: "s".to_string(),
                logical_type: "BIGINT".to_string(),
                nullable: true,
                visible: true,
                is_key: false,
            },
            crate::connector::starrocks::managed::store::StoredManagedColumn {
                schema_id: 100,
                ordinal: 4,
                column_name: "__agg_state_c".to_string(),
                logical_type: "BIGINT".to_string(),
                nullable: false,
                visible: false,
                is_key: false,
            },
            crate::connector::starrocks::managed::store::StoredManagedColumn {
                schema_id: 100,
                ordinal: 5,
                column_name: "__agg_state_s".to_string(),
                logical_type: "BIGINT".to_string(),
                nullable: true,
                visible: false,
                is_key: false,
            },
        ];
        let snapshot = ManagedSnapshot {
            global: ManagedGlobalMeta {
                warehouse_uri: config.warehouse_uri.clone(),
                next_db_id: 2,
                next_table_id: 11,
                next_partition_id: 21,
                next_index_id: 31,
                next_tablet_id: 44,
                next_txn_id: 100,
            },
            databases: vec![StoredManagedDatabase {
                db_id: 1,
                name: "analytics".to_string(),
            }],
            tables: vec![StoredManagedTable {
                table_id: 10,
                db_id: 1,
                name: "orders_mv".to_string(),
                keys_type: "PRIMARY_KEYS".to_string(),
                bucket_num: 2,
                current_schema_id: 100,
                state: ManagedTableState::Active,
                kind: ManagedTableKind::MaterializedView,
            }],
            schemas: vec![StoredManagedSchema {
                schema_id: 100,
                table_id: 10,
                schema_version: 0,
                tablet_schema_pb: tablet_schema.encode_to_vec(),
            }],
            columns,
            partitions: vec![StoredManagedPartition {
                partition_id: 20,
                table_id: 10,
                name: "p0".to_string(),
                visible_version: 1,
                next_version: 2,
                state: ManagedPartitionState::Active,
            }],
            indexes: vec![StoredManagedIndex {
                index_id: 30,
                table_id: 10,
                partition_id: 20,
                index_type: "BASE".to_string(),
                state: ManagedIndexState::Active,
            }],
            tablets: vec![
                StoredManagedTablet {
                    tablet_id: 40,
                    partition_id: 20,
                    index_id: 30,
                    bucket_seq: 0,
                    tablet_root_path: active_tablet_root.clone(),
                },
                StoredManagedTablet {
                    tablet_id: 43,
                    partition_id: 20,
                    index_id: 30,
                    bucket_seq: 1,
                    tablet_root_path: active_tablet_root.clone(),
                },
            ],
            txns: vec![],
            erase_jobs: vec![],
            materialized_views: vec![StoredMaterializedView {
                mv_id: 10,
                select_sql: "select k1, count(*) as c, sum(v1) as s from ice.ns.orders group by k1"
                    .to_string(),
                refresh_mode: ManagedMvRefreshMode::DeferredManual,
                base_table_refs: vec![],
                last_refresh_ms: None,
                last_refresh_rows: Some(1),
                last_refresh_snapshots: BTreeMap::new(),
                created_at_ms: 1,
                storage_engine: ManagedMvStorageEngine::ManagedLake,
                iceberg_table_identifier: None,
                last_refreshed_iceberg_snapshot_id: None,
                refresh_in_progress: false,
                refresh_target_snapshots: Default::default(),
            }],
        };

        let metadata_store =
            SqliteMetadataStore::open(format!("{metadata_root}/standalone.sqlite"))
                .expect("open store");
        metadata_store
            .replace_managed_snapshot(&snapshot)
            .expect("persist snapshot");

        for tablet_id in [40_i64, 43_i64] {
            let runtime_ctx = TabletWriteContext {
                db_id: 1,
                table_id: 10,
                tablet_id,
                tablet_root_path: active_tablet_root.clone(),
                tablet_schema: tablet_schema.clone(),
                s3_config: None,
                partial_update: Default::default(),
            };
            register_tablet_runtime(&runtime_ctx).expect("register runtime");
            let mut base_meta = empty_tablet_metadata(tablet_id);
            base_meta.version = Some(1);
            write_bundle_meta_file(
                &runtime_ctx.tablet_root_path,
                runtime_ctx.tablet_id,
                1,
                &runtime_ctx.tablet_schema,
                &base_meta,
            )
            .expect("write base tablet metadata");
        }

        let managed =
            ManagedLakeCatalog::rebuild(Some(config.clone()), snapshot).expect("rebuild managed");
        let mut catalog = InMemoryCatalog::default();
        catalog.create_database("analytics").expect("database");
        register_managed_tables_in_catalog(&mut catalog, &managed).expect("register catalog");

        MvTestFixture {
            state: Arc::new(StandaloneState {
                catalog: std::sync::RwLock::new(catalog),
                managed_lake: std::sync::RwLock::new(managed),
                managed_lake_config: Some(config),
                metadata_store: Some(metadata_store),
                ..Default::default()
            }),
            _metadata_dir: metadata_dir,
        }
    }

    fn aggregate_primary_key_tablet_schema() -> TabletSchemaPb {
        TabletSchemaPb {
            keys_type: Some(KeysType::PrimaryKeys as i32),
            column: vec![
                ColumnPb {
                    unique_id: 1,
                    name: Some("__row_id__".to_string()),
                    r#type: "VARCHAR".to_string(),
                    is_nullable: Some(false),
                    is_key: Some(true),
                    visible: Some(false),
                    ..Default::default()
                },
                ColumnPb {
                    unique_id: 2,
                    name: Some("k1".to_string()),
                    r#type: "BIGINT".to_string(),
                    is_nullable: Some(false),
                    is_key: Some(false),
                    visible: Some(true),
                    ..Default::default()
                },
                ColumnPb {
                    unique_id: 3,
                    name: Some("c".to_string()),
                    r#type: "BIGINT".to_string(),
                    is_nullable: Some(false),
                    is_key: Some(false),
                    visible: Some(true),
                    ..Default::default()
                },
                ColumnPb {
                    unique_id: 4,
                    name: Some("s".to_string()),
                    r#type: "BIGINT".to_string(),
                    is_nullable: Some(true),
                    is_key: Some(false),
                    visible: Some(true),
                    ..Default::default()
                },
                ColumnPb {
                    unique_id: 5,
                    name: Some("__agg_state_c".to_string()),
                    r#type: "BIGINT".to_string(),
                    is_nullable: Some(false),
                    is_key: Some(false),
                    visible: Some(false),
                    ..Default::default()
                },
                ColumnPb {
                    unique_id: 6,
                    name: Some("__agg_state_s".to_string()),
                    r#type: "BIGINT".to_string(),
                    is_nullable: Some(true),
                    is_key: Some(false),
                    visible: Some(false),
                    ..Default::default()
                },
            ],
            num_short_key_columns: Some(1),
            next_column_unique_id: Some(7),
            sort_key_idxes: vec![0],
            schema_version: Some(0),
            sort_key_unique_ids: vec![1],
            id: Some(100),
            ..Default::default()
        }
    }

    fn aggregate_physical_chunks(
        k1: &[i64],
        c: &[i64],
        s: &[i64],
    ) -> (
        crate::connector::starrocks::managed::mv_agg_state::AggregateMvLayout,
        Vec<Chunk>,
    ) {
        let shape = aggregate_mv_shape_for_txn_test();
        let output_columns = vec![
            crate::sql::analysis::OutputColumn {
                name: "k1".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
            crate::sql::analysis::OutputColumn {
                name: "c".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
            crate::sql::analysis::OutputColumn {
                name: "s".to_string(),
                data_type: DataType::Int64,
                nullable: true,
            },
        ];
        let layout = crate::connector::starrocks::managed::mv_agg_state::build_aggregate_mv_layout(
            &shape,
            &output_columns,
        )
        .expect("layout");
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k1", DataType::Int64, false),
                Field::new("c", DataType::Int64, false),
                Field::new("s", DataType::Int64, true),
            ])),
            vec![
                Arc::new(Int64Array::from(k1.to_vec())),
                Arc::new(Int64Array::from(c.to_vec())),
                Arc::new(Int64Array::from(s.to_vec())),
            ],
        )
        .expect("visible batch");
        let result = QueryResult {
            columns: Vec::new(),
            chunks: vec![record_batch_to_chunk(batch).expect("visible chunk")],
        };
        let chunks =
            crate::connector::starrocks::managed::mv_agg_state::materialize_aggregate_result_chunks(
                result, &layout, &shape,
            )
            .expect("physical chunks");
        (layout, chunks)
    }

    fn aggregate_mv_shape_for_txn_test()
    -> crate::connector::starrocks::managed::mv_shape::AggregateMvShape {
        let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(
            "select k1, count(*) as c, sum(v1) as s from ice.ns.orders group by k1",
        )
        .expect("normalize");
        let stmt = crate::sql::parser::parse_normalized_sql_raw(&normalized).expect("parse");
        let sqlparser::ast::Statement::Query(query) = stmt else {
            panic!("expected query");
        };
        let shape =
            crate::connector::starrocks::managed::mv_shape::classify_incremental_mv_query(&query)
                .expect("shape");
        let crate::connector::starrocks::managed::mv_shape::IncrementalMvShape::Aggregate(shape) =
            shape
        else {
            panic!("expected aggregate shape");
        };
        shape
    }

    fn seed_state_with_active_mv_on_object_store(
        config: ManagedLakeConfig,
    ) -> Result<MvTestFixture, String> {
        seed_state_with_mv_fixture(
            true,
            MvFixtureStorage::ObjectStore(config),
            MvFixtureKeyLayout::VisibleOnly,
        )
    }

    #[allow(clippy::large_enum_variant)]
    enum MvFixtureStorage {
        Local,
        ObjectStore(ManagedLakeConfig),
    }

    #[derive(Clone, Copy)]
    enum MvFixtureKeyLayout {
        VisibleOnly,
        VisibleAndHidden,
        HiddenOnly,
    }

    fn seed_state_with_mv_fixture(
        active_mv_metadata: bool,
        storage: MvFixtureStorage,
        key_layout: MvFixtureKeyLayout,
    ) -> Result<MvTestFixture, String> {
        let metadata_dir =
            tempfile::tempdir().map_err(|e| format!("create tempdir failed: {e}"))?;
        let metadata_root = metadata_dir.path().to_string_lossy().to_string();
        let (config, active_tablet_root, staged_tablet_root, tablet_s3_config) = match storage {
            MvFixtureStorage::Local => {
                let config = ManagedLakeConfig {
                    warehouse_uri: metadata_root.clone(),
                    s3: S3StoreConfig {
                        endpoint: "http://127.0.0.1:9000".to_string(),
                        bucket: "bucket".to_string(),
                        root: "warehouse".to_string(),
                        access_key_id: "ak".to_string(),
                        access_key_secret: "sk".to_string(),
                        region: None,
                        enable_path_style_access: Some(true),
                    },
                    mv_default_storage_engine: "managed_lake".to_string(),
                    mv_iceberg_warehouse_location: None,
                };
                (
                    config,
                    format!("{metadata_root}/db_1/table_10/partition_20"),
                    format!("{metadata_root}/db_1/table_10/partition_21"),
                    None,
                )
            }
            MvFixtureStorage::ObjectStore(config) => {
                let root = config.warehouse_uri.trim_end_matches('/').to_string();
                (
                    config.clone(),
                    format!("{root}/db_1/table_10/partition_20"),
                    format!("{root}/db_1/table_10/partition_21"),
                    Some(config.s3.clone()),
                )
            }
        };

        let mut tablet_schema = TabletSchemaPb {
            keys_type: Some(KeysType::DupKeys as i32),
            column: vec![
                ColumnPb {
                    unique_id: 1,
                    name: Some("k1".to_string()),
                    r#type: "INT".to_string(),
                    is_nullable: Some(false),
                    is_key: Some(true),
                    visible: Some(true),
                    ..Default::default()
                },
                ColumnPb {
                    unique_id: 2,
                    name: Some("total".to_string()),
                    r#type: "BIGINT".to_string(),
                    is_nullable: Some(true),
                    is_key: Some(false),
                    visible: Some(true),
                    ..Default::default()
                },
            ],
            num_short_key_columns: Some(1),
            next_column_unique_id: Some(3),
            sort_key_idxes: vec![0],
            schema_version: Some(0),
            sort_key_unique_ids: vec![1],
            id: Some(100),
            ..Default::default()
        };
        let include_hidden_physical_column = matches!(
            key_layout,
            MvFixtureKeyLayout::VisibleAndHidden | MvFixtureKeyLayout::HiddenOnly
        );
        let visible_key = matches!(
            key_layout,
            MvFixtureKeyLayout::VisibleOnly | MvFixtureKeyLayout::VisibleAndHidden
        );
        let hidden_key = include_hidden_physical_column;

        if include_hidden_physical_column {
            tablet_schema.column[0].is_key = Some(false);
            tablet_schema.column.push(ColumnPb {
                unique_id: 3,
                name: Some("__nr_shadow_total".to_string()),
                r#type: "BIGINT".to_string(),
                is_nullable: Some(true),
                is_key: Some(false),
                visible: Some(false),
                ..Default::default()
            });
            tablet_schema.next_column_unique_id = Some(4);
        }

        let mut columns = vec![
            crate::connector::starrocks::managed::store::StoredManagedColumn {
                schema_id: 100,
                ordinal: 0,
                column_name: "k1".to_string(),
                logical_type: "INT".to_string(),
                nullable: false,
                visible: true,
                is_key: visible_key,
            },
            crate::connector::starrocks::managed::store::StoredManagedColumn {
                schema_id: 100,
                ordinal: 1,
                column_name: "total".to_string(),
                logical_type: "BIGINT".to_string(),
                nullable: true,
                visible: true,
                is_key: false,
            },
        ];
        if include_hidden_physical_column {
            columns.push(
                crate::connector::starrocks::managed::store::StoredManagedColumn {
                    schema_id: 100,
                    ordinal: 2,
                    column_name: "__nr_shadow_total".to_string(),
                    logical_type: "BIGINT".to_string(),
                    nullable: true,
                    visible: false,
                    is_key: hidden_key,
                },
            );
        }

        let snapshot = ManagedSnapshot {
            global: ManagedGlobalMeta {
                warehouse_uri: config.warehouse_uri.clone(),
                next_db_id: 2,
                next_table_id: 11,
                next_partition_id: 22,
                next_index_id: 32,
                next_tablet_id: 44,
                next_txn_id: 100,
            },
            databases: vec![StoredManagedDatabase {
                db_id: 1,
                name: "analytics".to_string(),
            }],
            tables: vec![StoredManagedTable {
                table_id: 10,
                db_id: 1,
                name: "orders_mv".to_string(),
                keys_type: "DUP_KEYS".to_string(),
                bucket_num: 2,
                current_schema_id: 100,
                state: ManagedTableState::Active,
                kind: ManagedTableKind::MaterializedView,
            }],
            schemas: vec![StoredManagedSchema {
                schema_id: 100,
                table_id: 10,
                schema_version: 0,
                tablet_schema_pb: tablet_schema.encode_to_vec(),
            }],
            columns,
            partitions: vec![
                StoredManagedPartition {
                    partition_id: 20,
                    table_id: 10,
                    name: "p0".to_string(),
                    visible_version: 1,
                    next_version: 2,
                    state: ManagedPartitionState::Active,
                },
                StoredManagedPartition {
                    partition_id: 21,
                    table_id: 10,
                    name: "p0".to_string(),
                    visible_version: 1,
                    next_version: 2,
                    state: ManagedPartitionState::Creating,
                },
            ],
            indexes: vec![
                StoredManagedIndex {
                    index_id: 30,
                    table_id: 10,
                    partition_id: 20,
                    index_type: "BASE".to_string(),
                    state: ManagedIndexState::Active,
                },
                StoredManagedIndex {
                    index_id: 31,
                    table_id: 10,
                    partition_id: 21,
                    index_type: "BASE".to_string(),
                    state: ManagedIndexState::Creating,
                },
            ],
            tablets: vec![
                StoredManagedTablet {
                    tablet_id: 40,
                    partition_id: 20,
                    index_id: 30,
                    bucket_seq: 0,
                    tablet_root_path: active_tablet_root.clone(),
                },
                StoredManagedTablet {
                    tablet_id: 43,
                    partition_id: 20,
                    index_id: 30,
                    bucket_seq: 1,
                    tablet_root_path: active_tablet_root.clone(),
                },
                StoredManagedTablet {
                    tablet_id: 41,
                    partition_id: 21,
                    index_id: 31,
                    bucket_seq: 0,
                    tablet_root_path: staged_tablet_root.clone(),
                },
                StoredManagedTablet {
                    tablet_id: 42,
                    partition_id: 21,
                    index_id: 31,
                    bucket_seq: 1,
                    tablet_root_path: staged_tablet_root.clone(),
                },
            ],
            txns: vec![],
            erase_jobs: vec![],
            materialized_views: if active_mv_metadata {
                vec![StoredMaterializedView {
                    mv_id: 10,
                    select_sql: "select k1 from ice.ns.orders".to_string(),
                    refresh_mode: ManagedMvRefreshMode::DeferredManual,
                    base_table_refs: vec![],
                    last_refresh_ms: None,
                    last_refresh_rows: Some(0),
                    last_refresh_snapshots: std::collections::BTreeMap::new(),
                    created_at_ms: 1,
                    storage_engine: ManagedMvStorageEngine::ManagedLake,
                    iceberg_table_identifier: None,
                    last_refreshed_iceberg_snapshot_id: None,
                    refresh_in_progress: false,
                    refresh_target_snapshots: Default::default(),
                }]
            } else {
                vec![]
            },
        };

        let metadata_store =
            SqliteMetadataStore::open(format!("{metadata_root}/standalone.sqlite"))
                .map_err(|e| format!("open store failed: {e}"))?;
        metadata_store
            .replace_managed_snapshot(&snapshot)
            .map_err(|e| format!("persist snapshot failed: {e}"))?;

        for tablet_id in [40_i64, 43_i64] {
            let runtime_ctx = TabletWriteContext {
                db_id: 1,
                table_id: 10,
                tablet_id,
                tablet_root_path: active_tablet_root.clone(),
                tablet_schema: tablet_schema.clone(),
                s3_config: tablet_s3_config.clone(),
                partial_update: Default::default(),
            };
            register_tablet_runtime(&runtime_ctx)
                .map_err(|e| format!("register runtime failed: {e}"))?;
            let mut base_meta = empty_tablet_metadata(tablet_id);
            base_meta.version = Some(1);
            write_bundle_meta_file(
                &runtime_ctx.tablet_root_path,
                runtime_ctx.tablet_id,
                1,
                &runtime_ctx.tablet_schema,
                &base_meta,
            )
            .map_err(|e| format!("write base tablet metadata failed: {e}"))?;
        }

        let managed = ManagedLakeCatalog::rebuild(Some(config.clone()), snapshot)
            .map_err(|e| format!("rebuild managed failed: {e}"))?;
        let mut catalog = InMemoryCatalog::default();
        catalog
            .create_database("analytics")
            .map_err(|e| format!("create analytics failed: {e}"))?;
        register_managed_tables_in_catalog(&mut catalog, &managed)
            .map_err(|e| format!("register managed tables failed: {e}"))?;

        Ok(MvTestFixture {
            state: Arc::new(StandaloneState {
                catalog: std::sync::RwLock::new(catalog),
                managed_lake: std::sync::RwLock::new(managed),
                managed_lake_config: Some(config),
                metadata_store: Some(metadata_store),
                ..Default::default()
            }),
            _metadata_dir: metadata_dir,
        })
    }

    fn maybe_object_store_config() -> Option<ManagedLakeConfig> {
        let endpoint = std::env::var("AWS_S3_ENDPOINT")
            .unwrap_or_else(|_| "http://127.0.0.1:9000".to_string());
        if !managed_lake_endpoint_reachable(&endpoint) {
            eprintln!(
                "skipping mv object-store chunk write test: object store endpoint is unreachable: {endpoint}"
            );
            return None;
        }

        let access_key_id = std::env::var("AWS_S3_ACCESS_KEY_ID")
            .or_else(|_| std::env::var("MINIO_ROOT_USER"))
            .unwrap_or_else(|_| "admin".to_string());
        let access_key_secret = std::env::var("AWS_S3_SECRET_ACCESS_KEY")
            .or_else(|_| std::env::var("MINIO_ROOT_PASSWORD"))
            .unwrap_or_else(|_| "admin123".to_string());
        let bucket = std::env::var("AWS_S3_BUCKET").unwrap_or_else(|_| "novarocks".to_string());
        let root_prefix =
            std::env::var("AWS_S3_ROOT").unwrap_or_else(|_| "codex-managed-lake-tests".to_string());
        let run_id = format!(
            "mv_task4_{}_{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let root = if root_prefix.trim_matches('/').is_empty() {
            run_id
        } else {
            format!("{}/{}", root_prefix.trim_matches('/'), run_id)
        };
        Some(ManagedLakeConfig {
            warehouse_uri: format!("s3://{bucket}/{root}"),
            s3: S3StoreConfig {
                endpoint,
                bucket,
                root,
                access_key_id,
                access_key_secret,
                region: None,
                enable_path_style_access: Some(true),
            },
            mv_default_storage_engine: "managed_lake".to_string(),
            mv_iceberg_warehouse_location: None,
        })
    }

    fn managed_lake_endpoint_reachable(endpoint: &str) -> bool {
        let stripped = endpoint
            .split_once("://")
            .map(|(_, rest)| rest)
            .unwrap_or(endpoint);
        let authority = stripped.split('/').next().unwrap_or(stripped);
        let (host, port) = match authority.rsplit_once(':') {
            Some((host, port)) => match port.parse::<u16>() {
                Ok(port) => (host, port),
                Err(_) => return false,
            },
            None => {
                let default_port = if endpoint.starts_with("https://") {
                    443
                } else {
                    80
                };
                (authority, default_port)
            }
        };
        let Ok(addrs) = (host, port).to_socket_addrs() else {
            return false;
        };
        addrs
            .into_iter()
            .any(|addr| std::net::TcpStream::connect_timeout(&addr, Duration::from_secs(1)).is_ok())
    }

    fn single_i32_chunk(name: &str, values: &[i32]) -> Chunk {
        let schema = Arc::new(Schema::new(vec![
            Field::new(name, DataType::Int32, false),
            Field::new("total", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(values.to_vec())),
                Arc::new(Int64Array::from(vec![None; values.len()])),
            ],
        )
        .expect("batch");
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[SlotId::new(1), SlotId::new(2)],
        )
        .expect("chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    }

    fn physical_hidden_key_chunk(k1_values: &[i32], hidden_values: &[i64]) -> Chunk {
        assert_eq!(k1_values.len(), hidden_values.len());
        let schema = Arc::new(Schema::new(vec![
            Field::new("k1", DataType::Int32, false),
            Field::new("total", DataType::Int64, true),
            Field::new("__nr_shadow_total", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(k1_values.to_vec())),
                Arc::new(Int64Array::from(vec![None; k1_values.len()])),
                Arc::new(Int64Array::from(hidden_values.to_vec())),
            ],
        )
        .expect("batch");
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[SlotId::new(1), SlotId::new(2), SlotId::new(3)],
        )
        .expect("chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    }
}
