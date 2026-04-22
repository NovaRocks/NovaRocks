use std::sync::Arc;

use arrow::array::UInt32Array;
use arrow::record_batch::RecordBatch;

use crate::common::ids::SlotId;
use crate::connector::starrocks::lake::context::{
    PartialUpdateWritePolicy, TabletWriteContext, update_tablet_runtime_schema,
};
use crate::connector::starrocks::lake::{append_lake_txn_log_with_chunk_rowset, publish_version};
use crate::connector::starrocks::sink::routing::{
    build_unpartitioned_hash_routing, route_chunk_rows,
};
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::formats::starrocks::writer::StarRocksWriteFormat;
use crate::service::grpc_client::proto::starrocks::{PublishVersionRequest, TabletSchemaPb};
use crate::sql::parser::ast::{InsertSource, Literal, ObjectName};

use super::super::engine::local::ColumnDef;
use super::super::engine::{
    ResolvedLocalTableName, StandaloneState, StatementResult, build_local_insert_batch,
    insert_generate_series_rows_local, reorder_insert_rows,
};
use super::catalog::register_managed_table_in_catalog;

/// Insert rows into a standalone managed-lake table: prepare a txn in the
/// control plane, route rows across tablets, append native-format rowsets,
/// then publish_version and advance the visible partition version.
pub(crate) fn insert_into_managed_lake_table(
    state: &Arc<StandaloneState>,
    name: &ObjectName,
    insert_columns: &[String],
    source: &InsertSource,
    current_database: &str,
) -> Result<StatementResult, String> {
    let resolved = resolve_managed_name(name, current_database)?;
    let plan = load_insert_plan(state, &resolved)?;

    let rows = match source {
        InsertSource::Values(rows) => reorder_insert_rows(rows, insert_columns, &plan.columns)?,
        InsertSource::SelectLiteralRow(row) => {
            reorder_insert_rows(std::slice::from_ref(row), insert_columns, &plan.columns)?
        }
        InsertSource::GenerateSeriesSelect(gen_source) => {
            insert_generate_series_rows_local(gen_source, insert_columns, &plan.columns)?
        }
    };
    if rows.is_empty() {
        return Ok(StatementResult::Ok);
    }

    let batch = build_local_insert_batch(&plan.columns, &rows)?;
    let chunk = build_chunk_for_insert(batch, plan.columns.len())?;

    let metadata_store = state
        .metadata_store
        .as_ref()
        .ok_or_else(|| "managed lake insert requires sqlite metadata store".to_string())?;
    let prepared =
        metadata_store.prepare_txn(plan.table_id, plan.partition_id, plan.base_version)?;

    let write_outcome = write_routed_chunks(state, &plan, &chunk, prepared.txn_id);
    if let Err(err) = write_outcome {
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

    metadata_store.mark_txn_visible(prepared.txn_id, prepared.commit_version)?;

    commit_catalog_visible_version(state, &plan, prepared.commit_version)?;

    Ok(StatementResult::Ok)
}

#[derive(Clone, Debug)]
struct ManagedInsertPlan {
    table_id: i64,
    db_id: i64,
    partition_id: i64,
    base_version: i64,
    columns: Vec<ColumnDef>,
    distributed_slot_ids: Vec<SlotId>,
    tablet_schema: TabletSchemaPb,
    tablets: Vec<ManagedInsertTablet>,
}

#[derive(Clone, Debug)]
struct ManagedInsertTablet {
    tablet_id: i64,
    tablet_root_path: String,
}

fn load_insert_plan(
    state: &Arc<StandaloneState>,
    resolved: &ResolvedLocalTableName,
) -> Result<ManagedInsertPlan, String> {
    let guard = state
        .managed_lake
        .read()
        .expect("standalone managed lake read lock");
    let runtime = guard.table(&resolved.database, &resolved.table)?;
    let active_partition = runtime
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
    let active_index = runtime
        .indexes
        .iter()
        .find(|index| {
            index.partition_id == active_partition.partition_id
                && index.state == super::store::ManagedIndexState::Active
        })
        .cloned()
        .ok_or_else(|| {
            format!(
                "managed table {}.{} has no active base index",
                resolved.database, resolved.table
            )
        })?;
    let mut tablets = runtime
        .tablets
        .iter()
        .filter(|tablet| {
            tablet.index_id == active_index.index_id
                && tablet.partition_id == active_partition.partition_id
        })
        .cloned()
        .collect::<Vec<_>>();
    if tablets.is_empty() {
        return Err(format!(
            "managed table {}.{} has no tablets",
            resolved.database, resolved.table
        ));
    }
    tablets.sort_by_key(|tablet| tablet.bucket_seq);

    let columns = derive_column_defs(state, &resolved.database, &resolved.table)?;
    let distributed_slot_ids = derive_distributed_slot_ids(&runtime.tablet_schema, &columns)?;
    if distributed_slot_ids.is_empty() {
        return Err(format!(
            "managed table {}.{} has no distribution key columns",
            resolved.database, resolved.table
        ));
    }

    Ok(ManagedInsertPlan {
        table_id: runtime.table.table_id,
        db_id: runtime.table.db_id,
        partition_id: active_partition.partition_id,
        base_version: active_partition.visible_version,
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

fn derive_column_defs(
    state: &Arc<StandaloneState>,
    database: &str,
    table: &str,
) -> Result<Vec<ColumnDef>, String> {
    let catalog = state.catalog.read().expect("standalone catalog read lock");
    Ok(catalog.get(database, table)?.columns)
}

fn derive_distributed_slot_ids(
    tablet_schema: &TabletSchemaPb,
    columns: &[ColumnDef],
) -> Result<Vec<SlotId>, String> {
    let mut slot_ids = Vec::new();
    for column in &tablet_schema.column {
        if column.is_key != Some(true) {
            continue;
        }
        let Some(name) = column.name.as_deref() else {
            continue;
        };
        let lowered = name.to_ascii_lowercase();
        let idx = columns
            .iter()
            .position(|col| col.name.eq_ignore_ascii_case(&lowered))
            .ok_or_else(|| format!("distribution key `{name}` not found in logical column list"))?;
        slot_ids.push(SlotId::new(idx as u32 + 1));
    }
    Ok(slot_ids)
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
) -> Result<(), String> {
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
            s3_config: Some(managed_config.s3.clone()),
            partial_update: PartialUpdateWritePolicy::default(),
        };
        // Keep the tablet runtime's schema in lockstep with what we persist,
        // so concurrent readers/writers see the same logical shape.
        update_tablet_runtime_schema(tablet.tablet_id, &plan.tablet_schema)?;
        append_lake_txn_log_with_chunk_rowset(
            &write_ctx,
            &routed_chunk,
            txn_id,
            0,
            tablet_idx as u64,
            StarRocksWriteFormat::Native,
            plan.partition_id,
            None,
        )?;
    }
    Ok(())
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
    publish_tablets_at_version(
        plan.tablets.iter().map(|tablet| tablet.tablet_id).collect(),
        prepared.txn_id,
        prepared.base_version,
        prepared.commit_version,
    )
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

fn resolve_managed_name(
    name: &ObjectName,
    current_database: &str,
) -> Result<ResolvedLocalTableName, String> {
    match name.parts.as_slice() {
        [table] => Ok(ResolvedLocalTableName {
            database: super::super::engine::local::normalize_identifier(current_database)?,
            table: super::super::engine::local::normalize_identifier(table)?,
        }),
        [database, table] => Ok(ResolvedLocalTableName {
            database: super::super::engine::local::normalize_identifier(database)?,
            table: super::super::engine::local::normalize_identifier(table)?,
        }),
        _ => Err(format!(
            "managed table name must be `<table>` or `<database>.<table>`, got `{}`",
            name.parts.join(".")
        )),
    }
}
