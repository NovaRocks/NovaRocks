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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{ArrayRef, Int8Array, UInt32Array, new_null_array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::common::ids::SlotId;
use crate::connector::starrocks::ObjectStoreProfile;
use crate::connector::starrocks::fs_access::{
    object_store_profile_for_tablet_root, resolve_tablet_root,
};
use crate::connector::starrocks::lake::append_lake_txn_log_with_chunk_rowset;
use crate::connector::starrocks::lake::context::{
    PartialUpdateWritePolicy, TabletWriteContext, update_tablet_runtime_schema,
};
use crate::connector::starrocks::lake::transactions::publish_version;
use crate::connector::starrocks::lake::txn_log::append_lake_txn_log_empty_rowset;
use crate::connector::starrocks::schema::{StarRocksKeysType, StarRocksTabletSchema};
use crate::connector::starrocks::sink::routing::{
    build_unpartitioned_hash_routing, route_chunk_rows,
};
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::formats::starrocks::data::build_native_record_batch;
use crate::formats::starrocks::metadata::{load_bundle_segment_footers, load_tablet_snapshot};
use crate::formats::starrocks::plan::build_native_read_plan;
use crate::formats::starrocks::writer::StarRocksWriteFormat;
use crate::meta::repository::mv::UpdateStarRocksMvRefreshSummaryRequest;
use crate::meta::repository::starrocks_txn::StoredStarRocksTxn;
use crate::mv::aggregate_state::mv_agg_state::{self, AggregateMvLayout};
use crate::runtime::query_result::{QueryResult, record_batch_to_chunk};
use crate::runtime::starlet_shard_registry::S3StoreConfig;
use crate::service::grpc_client::proto::starrocks::{
    DeleteDataRequest, DeletePredicatePb, PublishVersionRequest, TableSchemaKeyPb,
};
use crate::sql::parser::ast::{InsertSource, Literal, ObjectName};

use super::catalog::register_starrocks_table_in_catalog;
use crate::engine::{
    StandaloneState, StatementResult, build_local_insert_batch, execute_query, reorder_insert_rows,
};
use crate::exec::expr::cast_with_special_rules;
use novarocks_catalog::identifier::LocalTableIdentity;
use novarocks_catalog::identifier::normalize_identifier;
use novarocks_catalog::schema::ColumnDef;

/// Insert rows into a standalone StarRocks table: prepare a txn in the
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
            "InsertSource::FromQuery must be dispatched via insert_from_query_into_starrocks_table"
                .to_string(),
        ),
    }
}

pub(crate) fn insert_into_starrocks_table(
    state: &Arc<StandaloneState>,
    name: &ObjectName,
    insert_columns: &[String],
    source: &InsertSource,
    current_database: &str,
) -> Result<StatementResult, String> {
    let resolved = resolve_starrocks_name(name, current_database)?;
    let plan = load_insert_plan(state, &resolved, PartitionTarget::Active)?;

    // INSERT ... SELECT from a real relation cannot be reduced to literal
    // rows in the parser. Dispatch it through the plan/pipeline executor to
    // stay aligned with how StarRocks wraps INSERT-SELECT (a normal SELECT
    // plan plus a table-writing sink), then hand the materialised result to
    // the same txn/write/publish sequence used by VALUES INSERT.
    if let InsertSource::FromQuery(query) = source {
        return insert_from_query_into_starrocks_table(
            state,
            &resolved,
            &plan,
            insert_columns,
            query,
        );
    }

    let rows = materialize_insert_rows(source, insert_columns, &plan.columns)?;
    if rows.is_empty() {
        return Ok(StatementResult::Ok);
    }

    let batch = build_local_insert_batch(&plan.columns, &rows)?;
    let chunk = build_chunk_for_insert(batch, plan.columns.len())?;
    write_chunks_into_starrocks_partition(state, plan, &[chunk])?;
    Ok(StatementResult::Ok)
}

pub(crate) fn insert_rows_into_starrocks_table(
    state: &Arc<StandaloneState>,
    database: &str,
    table: &str,
    rows: &[Vec<Literal>],
) -> Result<(), String> {
    let resolved = LocalTableIdentity {
        database: normalize_identifier(database)?,
        table: normalize_identifier(table)?,
    };
    let plan = load_insert_plan(state, &resolved, PartitionTarget::Active)?;
    if rows.is_empty() {
        return Ok(());
    }
    let batch = build_local_insert_batch(&plan.columns, rows)?;
    let chunk = build_chunk_for_insert(batch, plan.columns.len())?;
    write_chunks_into_starrocks_partition(state, plan, &[chunk])?;
    Ok(())
}

pub(crate) fn insert_batch_into_starrocks_table(
    state: &Arc<StandaloneState>,
    database: &str,
    table: &str,
    batch: RecordBatch,
) -> Result<(), String> {
    let resolved = LocalTableIdentity {
        database: normalize_identifier(database)?,
        table: normalize_identifier(table)?,
    };
    let plan = load_insert_plan(state, &resolved, PartitionTarget::Active)?;
    if batch.num_rows() == 0 {
        return Ok(());
    }
    let chunk = build_chunk_for_insert(batch, plan.columns.len())?;
    write_chunks_into_starrocks_partition(state, plan, &[chunk])?;
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
pub(crate) struct StarRocksInsertPlan {
    pub(crate) table_id: i64,
    pub(crate) db_id: i64,
    pub(crate) partition_id: i64,
    pub(crate) base_version: i64,
    pub(crate) columns: Vec<ColumnDef>,
    pub(crate) distributed_slot_ids: Vec<SlotId>,
    pub(crate) tablet_schema: StarRocksTabletSchema,
    pub(crate) tablets: Vec<StarRocksInsertTablet>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StarRocksInsertColumnMode {
    VisibleOnly,
    Physical,
}

#[derive(Clone, Debug)]
pub(crate) struct StarRocksInsertTablet {
    pub(crate) tablet_id: i64,
    pub(crate) tablet_root_path: String,
}

#[derive(Clone, Debug)]
pub(crate) struct MvRefreshWriteMetadata {
    pub(crate) table_id: i64,
    pub(crate) previous_refresh_rows: i64,
    pub(crate) snapshots: BTreeMap<String, i64>,
    pub(crate) table_uuids: BTreeMap<String, String>,
}

pub(crate) fn load_insert_plan(
    state: &Arc<StandaloneState>,
    resolved: &LocalTableIdentity,
    target: PartitionTarget,
) -> Result<StarRocksInsertPlan, String> {
    load_insert_plan_with_column_mode(
        state,
        resolved,
        target,
        StarRocksInsertColumnMode::VisibleOnly,
    )
}

pub(crate) fn load_physical_insert_plan(
    state: &Arc<StandaloneState>,
    resolved: &LocalTableIdentity,
    target: PartitionTarget,
) -> Result<StarRocksInsertPlan, String> {
    load_insert_plan_with_column_mode(state, resolved, target, StarRocksInsertColumnMode::Physical)
}

fn load_insert_plan_with_column_mode(
    state: &Arc<StandaloneState>,
    resolved: &LocalTableIdentity,
    target: PartitionTarget,
    column_mode: StarRocksInsertColumnMode,
) -> Result<StarRocksInsertPlan, String> {
    let guard = state
        .starrocks_table
        .read()
        .expect("standalone StarRocks table read lock");
    let runtime = guard.table(&resolved.database, &resolved.table)?;
    let (target_partition, _target_index, mut tablets) = match target {
        PartitionTarget::Active => {
            let partition = runtime
                .partitions
                .iter()
                .find(|partition| partition.state == super::model::StarRocksPartitionState::Active)
                .cloned()
                .ok_or_else(|| {
                    format!(
                        "StarRocks table {}.{} has no active partition",
                        resolved.database, resolved.table
                    )
                })?;
            let index = runtime
                .indexes
                .iter()
                .find(|index| {
                    index.partition_id == partition.partition_id
                        && index.state == super::model::StarRocksIndexState::Active
                })
                .cloned()
                .ok_or_else(|| {
                    format!(
                        "StarRocks table {}.{} has no active base index",
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
                        "StarRocks table {}.{} is missing staged partition {}",
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
                        "StarRocks table {}.{} is missing staged index {}",
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
                    "StarRocks table {}.{} is missing staged tablets for partition {}",
                    resolved.database, resolved.table, partition_id
                ));
            }
            (partition, index, tablets)
        }
    };
    if tablets.is_empty() {
        return Err(format!(
            "StarRocks table {}.{} has no tablets",
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
            .filter(|column| column_mode == StarRocksInsertColumnMode::Physical || column.visible),
    );
    if distributed_slot_ids.is_empty() {
        if column_mode == StarRocksInsertColumnMode::VisibleOnly
            && has_persisted_key_columns(runtime)
            && !selected_columns_include_persisted_key(&columns, runtime)
        {
            return Err(format!(
                "StarRocks table {}.{} distribution key columns are hidden in visible insert mode; use physical insert plan",
                resolved.database, resolved.table
            ));
        }
        return Err(format!(
            "StarRocks table {}.{} has no distribution key columns",
            resolved.database, resolved.table
        ));
    }

    Ok(StarRocksInsertPlan {
        table_id: runtime.table.table_id,
        db_id: runtime.table.db_id,
        partition_id: target_partition.partition_id,
        base_version: target_partition.visible_version,
        columns,
        distributed_slot_ids,
        tablet_schema: runtime.tablet_schema.clone(),
        tablets: tablets
            .into_iter()
            .map(|tablet| StarRocksInsertTablet {
                tablet_id: tablet.tablet_id,
                tablet_root_path: tablet.tablet_root_path,
            })
            .collect(),
    })
}

pub(crate) fn write_chunks_into_starrocks_partition(
    state: &Arc<StandaloneState>,
    plan: StarRocksInsertPlan,
    chunks: &[Chunk],
) -> Result<i64, String> {
    write_chunks_into_starrocks_partition_inner(state, plan, chunks, VisibleCommitAction::Plain)
}

#[allow(dead_code)]
pub(crate) fn write_chunks_into_starrocks_partition_for_mv_refresh(
    state: &Arc<StandaloneState>,
    plan: StarRocksInsertPlan,
    chunks: &[Chunk],
    metadata: MvRefreshWriteMetadata,
) -> Result<i64, String> {
    write_chunks_into_starrocks_partition_inner(
        state,
        plan,
        chunks,
        VisibleCommitAction::MvRefresh {
            metadata,
            row_delta: None,
        },
    )
}

pub(crate) fn write_chunks_into_starrocks_partition_for_mv_refresh_with_row_delta(
    state: &Arc<StandaloneState>,
    plan: StarRocksInsertPlan,
    chunks: &[Chunk],
    metadata: MvRefreshWriteMetadata,
    row_delta: i64,
) -> Result<i64, String> {
    write_chunks_into_starrocks_partition_inner(
        state,
        plan,
        chunks,
        VisibleCommitAction::MvRefresh {
            metadata,
            row_delta: Some(row_delta),
        },
    )
}

pub(crate) fn read_active_starrocks_physical_chunks(
    state: &Arc<StandaloneState>,
    plan: &StarRocksInsertPlan,
) -> Result<Vec<Chunk>, String> {
    let starrocks_table_config = state.starrocks_table_config.as_ref().ok_or_else(|| {
        "standalone StarRocks table config is missing during physical read".to_string()
    })?;
    let output_schema = Arc::new(Schema::new(
        plan.columns
            .iter()
            .map(|column| {
                Field::new(
                    &column.name,
                    crate::formats::parquet::local_io::normalize_map_entries_nullability(
                        &column.data_type,
                    ),
                    column.nullable,
                )
            })
            .collect::<Vec<_>>(),
    ));

    let mut chunks = Vec::new();
    for tablet in &plan.tablets {
        let object_store_profile = object_store_profile_for_tablet_path(
            &tablet.tablet_root_path,
            &starrocks_table_config.s3,
        )?;
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

pub(crate) fn write_chunks_into_starrocks_partition_for_aggregate_mv_upsert(
    state: &Arc<StandaloneState>,
    plan: StarRocksInsertPlan,
    delta_chunks: &[Chunk],
    layout: &AggregateMvLayout,
    metadata: MvRefreshWriteMetadata,
) -> Result<i64, String> {
    if plan.tablet_schema.keys_type != Some(StarRocksKeysType::Primary) {
        return Err(
            "aggregate MV incremental upsert requires PRIMARY_KEYS physical table".to_string(),
        );
    }

    let old_chunks = read_active_starrocks_physical_chunks(state, &plan)?;
    let old_rows = mv_agg_state::build_old_state_map(&old_chunks, layout)?;
    let merge_result = mv_agg_state::merge_aggregate_state_batches_with_retractions(
        &old_rows,
        delta_chunks,
        layout,
    )?;
    let mut publish_chunks = Vec::new();
    publish_chunks.extend(append_primary_key_op_column(
        &merge_result.upsert_chunks,
        STARROCKS_PK_OP_UPSERT,
    )?);
    publish_chunks.extend(append_primary_key_op_column(
        &merge_result.delete_chunks,
        STARROCKS_PK_OP_DELETE,
    )?);
    write_chunks_into_starrocks_partition_for_mv_refresh_with_row_delta(
        state,
        plan,
        &publish_chunks,
        metadata,
        merge_result.row_delta,
    )
}

const STARROCKS_PK_OP_COLUMN: &str = "__op";
const STARROCKS_PK_OP_UPSERT: i8 = 0;
const STARROCKS_PK_OP_DELETE: i8 = 1;

/// Drive a PRIMARY KEY DELETE through the existing StarRocks table sink path.
///
/// `pk_chunks` come from running `SELECT <pk_cols> FROM t WHERE cond` through
/// the standalone pipeline. Each chunk is appended a constant `__op = 1`
/// control column so the lake writer (`parse_op_batch`) classifies the rows
/// as deletes and emits a `.del` file via `encode_delete_keys_payload`.
///
/// Chunks coming out of the SELECT pipeline carry slot IDs that are
/// local to that plan, not to the table's column layout. The routing path
/// in `write_routed_chunks` resolves the distribution slot by looking up
/// `plan.distributed_slot_ids` (1-indexed positions in `plan.columns`) inside
/// the chunk schema, so we rebuild each chunk with slot IDs matching the
/// destination plan's column order before appending `__op`.
pub(crate) fn delete_starrocks_table_pk_rows(
    state: &Arc<StandaloneState>,
    database_name: &str,
    table_name: &str,
    pk_chunks: &[Chunk],
) -> Result<(), String> {
    let resolved = LocalTableIdentity {
        database: normalize_identifier(database_name)?,
        table: normalize_identifier(table_name)?,
    };
    let plan = load_physical_insert_plan(state, &resolved, PartitionTarget::Active)?;
    if plan.tablet_schema.keys_type != Some(StarRocksKeysType::Primary) {
        return Err(format!(
            "delete_starrocks_table_pk_rows called on non-PRIMARY_KEYS table {database_name}.{table_name}"
        ));
    }

    let mut rebuilt = Vec::with_capacity(pk_chunks.len());
    for chunk in pk_chunks {
        if chunk.is_empty() {
            continue;
        }
        rebuilt.push(rebuild_pk_chunk_for_plan(chunk, &plan)?);
    }
    if rebuilt.is_empty() {
        // No survivors matched the WHERE clause — nothing to delete.
        return Ok(());
    }
    let op_chunks = append_primary_key_op_column(&rebuilt, STARROCKS_PK_OP_DELETE)?;
    write_chunks_into_starrocks_partition(state, plan, &op_chunks)?;
    Ok(())
}

/// Reassign each column of `chunk` to its 1-indexed slot in `plan.columns`,
/// matching the slot layout that `derive_distributed_slot_ids` expects. The
/// chunk must contain only columns named in `plan.columns`; the order is
/// preserved as-is but slot IDs are remapped by name.
fn rebuild_pk_chunk_for_plan(chunk: &Chunk, plan: &StarRocksInsertPlan) -> Result<Chunk, String> {
    let batch_fields = chunk
        .batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    let mut slot_ids = Vec::with_capacity(batch_fields.len());
    for field in &batch_fields {
        let idx = plan
            .columns
            .iter()
            .position(|col| col.name.eq_ignore_ascii_case(field.name()))
            .ok_or_else(|| {
                format!(
                    "PK delete chunk column `{}` is not present in destination plan",
                    field.name()
                )
            })?;
        slot_ids.push(SlotId::new(idx as u32 + 1));
    }
    let chunk_schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(chunk.batch.schema().as_ref(), &slot_ids)?;
    Ok(Chunk::new_with_chunk_schema(
        chunk.batch.clone(),
        chunk_schema,
    ))
}

fn append_primary_key_op_column(chunks: &[Chunk], op: i8) -> Result<Vec<Chunk>, String> {
    chunks
        .iter()
        .filter(|chunk| !chunk.is_empty())
        .map(|chunk| {
            let mut fields = chunk
                .batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.as_ref().clone())
                .collect::<Vec<_>>();
            if fields
                .iter()
                .any(|field| field.name().eq_ignore_ascii_case(STARROCKS_PK_OP_COLUMN))
            {
                return Err(format!(
                    "aggregate MV incremental write result contains reserved column `{STARROCKS_PK_OP_COLUMN}`"
                ));
            }
            fields.push(Field::new(STARROCKS_PK_OP_COLUMN, DataType::Int8, false));
            let mut columns = chunk.batch.columns().to_vec();
            columns.push(Arc::new(Int8Array::from(vec![op; chunk.len()])) as ArrayRef);
            let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
                .map_err(|e| format!("append StarRocks PK op column failed: {e}"))?;
            let mut slot_ids = chunk
                .chunk_schema()
                .slots()
                .iter()
                .map(|slot| slot.slot_id())
                .collect::<Vec<_>>();
            let next_slot = slot_ids
                .iter()
                .map(|slot| slot.as_u32())
                .max()
                .unwrap_or(0)
                .saturating_add(1);
            slot_ids.push(SlotId::new(next_slot));
            let chunk_schema =
                ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &slot_ids)?;
            Ok(Chunk::new_with_chunk_schema(batch, chunk_schema))
        })
        .collect()
}

enum VisibleCommitAction {
    Plain,
    MvRefresh {
        metadata: MvRefreshWriteMetadata,
        row_delta: Option<i64>,
    },
}

fn write_chunks_into_starrocks_partition_inner(
    state: &Arc<StandaloneState>,
    mut plan: StarRocksInsertPlan,
    chunks: &[Chunk],
    commit_action: VisibleCommitAction,
) -> Result<i64, String> {
    let mut starrocks = state
        .starrocks_table
        .write()
        .expect("standalone StarRocks table write lock");
    revalidate_insert_plan_visible_version(&starrocks, &mut plan)?;

    let total_rows = chunks_total_rows(chunks)?;
    let prepared = prepare_starrocks_txn(state, &plan)?;

    let mut written_tablet_ids = Vec::new();
    let mut next_file_seq = 0_u64;
    for chunk in chunks {
        let write_outcome =
            write_routed_chunks(state, &plan, chunk, prepared.txn_id, &mut next_file_seq);
        let chunk_written_ids = match write_outcome {
            Ok(ids) => ids,
            Err(err) => {
                if let Err(abort_err) = mark_starrocks_txn_aborted(state, prepared.txn_id) {
                    return Err(format!(
                        "StarRocks table write failed: {err}; additionally mark_txn_aborted failed: {abort_err}"
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
        if let Err(abort_err) = mark_starrocks_txn_aborted(state, prepared.txn_id) {
            return Err(format!(
                "StarRocks table write failed: {err}; additionally mark_txn_aborted failed: {abort_err}"
            ));
        }
        return Err(err);
    }

    mark_starrocks_txn_written(state, prepared.txn_id)?;

    publish_starrocks_txn(&plan, &prepared).map_err(|err| {
        if let Err(abort_err) = mark_starrocks_txn_aborted(state, prepared.txn_id) {
            return format!(
                "StarRocks table publish failed: {err}; additionally mark_txn_aborted failed: {abort_err}"
            );
        }
        format!("StarRocks table publish failed: {err}")
    })?;

    match commit_action {
        VisibleCommitAction::Plain => {
            mark_starrocks_txn_visible(state, prepared.txn_id)?;
        }
        VisibleCommitAction::MvRefresh {
            metadata,
            row_delta,
        } => {
            let delta = row_delta.unwrap_or(total_rows);
            let last_refresh_rows = metadata
                .previous_refresh_rows
                .checked_add(delta)
                .ok_or_else(|| {
                    format!(
                        "StarRocks table mv refresh row count overflow: {} + {}",
                        metadata.previous_refresh_rows, delta
                    )
                })?;
            mark_starrocks_txn_visible_with_mv_refresh_metadata(
                state,
                prepared.txn_id,
                metadata.table_id,
                last_refresh_rows,
                metadata.snapshots,
                metadata.table_uuids,
            )?;
        }
    }
    commit_catalog_visible_version(state, &mut starrocks, &plan, prepared.commit_version)?;
    drop(starrocks);

    Ok(total_rows)
}

fn prepare_starrocks_txn(
    state: &Arc<StandaloneState>,
    plan: &StarRocksInsertPlan,
) -> Result<StoredStarRocksTxn, String> {
    let provider = state
        .metadata_provider
        .as_ref()
        .ok_or_else(|| "StarRocks table insert requires metadata provider".to_string())?;
    let mut txn = provider
        .begin_write("prepare StarRocks table txn")
        .map_err(|e| format!("open StarRocks txn prepare transaction failed: {e}"))?;
    let prepared = state
        .starrocks_txn_repo
        .prepare(
            &state.starrocks_table_repo,
            txn.as_mut(),
            plan.table_id,
            plan.partition_id,
        )
        .map_err(|e| format!("prepare StarRocks txn metadata failed: {e}"))?;
    if prepared.base_version != plan.base_version {
        return Err(format!(
            "StarRocks txn base version is {}, expected {}",
            prepared.base_version, plan.base_version
        ));
    }
    txn.commit()
        .map_err(|e| format!("commit StarRocks txn prepare metadata failed: {e}"))?;
    Ok(prepared)
}

fn mark_starrocks_txn_written(state: &Arc<StandaloneState>, txn_id: i64) -> Result<(), String> {
    let provider = state
        .metadata_provider
        .as_ref()
        .ok_or_else(|| "StarRocks table insert requires metadata provider".to_string())?;
    let mut txn = provider
        .begin_write("mark StarRocks table txn written")
        .map_err(|e| format!("open StarRocks txn written transaction failed: {e}"))?;
    state
        .starrocks_txn_repo
        .mark_written(txn.as_mut(), txn_id)
        .map_err(|e| format!("mark StarRocks txn written failed: {e}"))?;
    txn.commit()
        .map_err(|e| format!("commit StarRocks txn written metadata failed: {e}"))?;
    Ok(())
}

fn mark_starrocks_txn_visible(state: &Arc<StandaloneState>, txn_id: i64) -> Result<(), String> {
    let provider = state
        .metadata_provider
        .as_ref()
        .ok_or_else(|| "StarRocks table insert requires metadata provider".to_string())?;
    let mut txn = provider
        .begin_write("mark StarRocks table txn visible")
        .map_err(|e| format!("open StarRocks txn visible transaction failed: {e}"))?;
    state
        .starrocks_txn_repo
        .mark_visible(&state.starrocks_table_repo, txn.as_mut(), txn_id)
        .map_err(|e| format!("mark StarRocks txn visible failed: {e}"))?;
    txn.commit()
        .map_err(|e| format!("commit StarRocks txn visible metadata failed: {e}"))?;
    Ok(())
}

fn mark_starrocks_txn_visible_with_mv_refresh_metadata(
    state: &Arc<StandaloneState>,
    txn_id: i64,
    mv_id: i64,
    last_refresh_rows: i64,
    snapshots: BTreeMap<String, i64>,
    table_uuids: BTreeMap<String, String>,
) -> Result<(), String> {
    let provider = state
        .metadata_provider
        .as_ref()
        .ok_or_else(|| "StarRocks table insert requires metadata provider".to_string())?;
    let mut txn = provider
        .begin_write("mark StarRocks table mv refresh txn visible")
        .map_err(|e| format!("open StarRocks MV refresh visible transaction failed: {e}"))?;
    state
        .starrocks_txn_repo
        .mark_visible(&state.starrocks_table_repo, txn.as_mut(), txn_id)
        .map_err(|e| format!("mark StarRocks MV refresh txn visible failed: {e}"))?;
    state
        .mv_repo
        .update_starrocks_refresh_summary_if_present(
            txn.as_mut(),
            UpdateStarRocksMvRefreshSummaryRequest {
                mv_id,
                last_refresh_ms: current_time_ms(),
                last_refresh_rows,
                base_snapshots: snapshots,
                base_table_uuids: table_uuids,
            },
        )
        .map_err(|e| format!("update StarRocks MV refresh metadata failed: {e}"))?;
    txn.commit()
        .map_err(|e| format!("commit StarRocks MV refresh visible metadata failed: {e}"))?;
    Ok(())
}

fn mark_starrocks_txn_aborted(state: &Arc<StandaloneState>, txn_id: i64) -> Result<(), String> {
    let provider = state
        .metadata_provider
        .as_ref()
        .ok_or_else(|| "StarRocks table insert requires metadata provider".to_string())?;
    let mut txn = provider
        .begin_write("abort StarRocks table txn")
        .map_err(|e| format!("open StarRocks txn abort transaction failed: {e}"))?;
    state
        .starrocks_txn_repo
        .mark_aborted(txn.as_mut(), txn_id)
        .map_err(|e| format!("mark StarRocks txn aborted failed: {e}"))?;
    txn.commit()
        .map_err(|e| format!("commit StarRocks txn abort metadata failed: {e}"))?;
    Ok(())
}

fn current_time_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

fn revalidate_insert_plan_visible_version(
    starrocks: &super::catalog::StarRocksTableCatalog,
    plan: &mut StarRocksInsertPlan,
) -> Result<(), String> {
    let partition = starrocks
        .snapshot
        .partitions
        .iter()
        .find(|partition| {
            partition.table_id == plan.table_id && partition.partition_id == plan.partition_id
        })
        .ok_or_else(|| {
            format!(
                "StarRocks insert target partition {} for table {} no longer exists",
                plan.partition_id, plan.table_id
            )
        })?;
    if matches!(
        partition.state,
        super::model::StarRocksPartitionState::Retired
    ) {
        return Err(format!(
            "StarRocks insert target partition {} for table {} is no longer writable",
            plan.partition_id, plan.table_id
        ));
    }

    let current_tablet_ids = starrocks
        .snapshot
        .tablets
        .iter()
        .filter(|tablet| tablet.partition_id == plan.partition_id)
        .map(|tablet| tablet.tablet_id)
        .collect::<HashSet<_>>();
    if !plan
        .tablets
        .iter()
        .all(|tablet| current_tablet_ids.contains(&tablet.tablet_id))
    {
        return Err(format!(
            "StarRocks insert target tablets for partition {} changed before publish",
            plan.partition_id
        ));
    }

    plan.base_version = partition.visible_version;
    Ok(())
}

fn chunks_total_rows(chunks: &[Chunk]) -> Result<i64, String> {
    chunks.iter().try_fold(0_i64, |acc, chunk| {
        let rows = i64::try_from(chunk.len())
            .map_err(|_| "StarRocks table chunk row count overflow".to_string())?;
        acc.checked_add(rows)
            .ok_or_else(|| "StarRocks table chunk row count overflow".to_string())
    })
}

fn derive_column_defs_from_runtime(
    runtime: &super::catalog::StarRocksTableRuntime,
    mode: StarRocksInsertColumnMode,
) -> Result<Vec<ColumnDef>, String> {
    runtime
        .columns
        .iter()
        .filter(|column| mode == StarRocksInsertColumnMode::Physical || column.visible)
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
                        "StarRocks table {}.{} is missing tablet schema column `{}`",
                        runtime.database_name, runtime.table.name, column.column_name
                    )
                })?;
            Ok(ColumnDef {
                name: column.column_name.clone(),
                data_type:
                    crate::connector::starrocks::table::catalog::arrow_type_from_tablet_column(
                        schema_column,
                    )?,
                nullable: column.nullable,
                write_default: None,
                logical_type: None,
            })
        })
        .collect()
}

fn derive_distributed_slot_ids<'a>(
    columns: &[ColumnDef],
    stored_columns: impl IntoIterator<Item = &'a super::model::StoredStarRocksColumn>,
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

fn has_persisted_key_columns(runtime: &super::catalog::StarRocksTableRuntime) -> bool {
    runtime.columns.iter().any(|column| column.is_key)
}

fn selected_columns_include_persisted_key(
    columns: &[ColumnDef],
    runtime: &super::catalog::StarRocksTableRuntime,
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
    plan: &StarRocksInsertPlan,
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
            "StarRocks table insert rejected {} rows during routing",
            routed.rejections.len()
        ));
    }

    let starrocks_table_config = state
        .starrocks_table_config
        .as_ref()
        .ok_or_else(|| "standalone StarRocks table config is missing during insert".to_string())?
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
            s3_config: s3_config_for_tablet_path(
                &tablet.tablet_root_path,
                &starrocks_table_config.s3,
            )?,
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

/// Apply a `DeletePredicatePb` to every tablet of a DUP/UNIQUE/AGG StarRocks table
/// table's active partition. Writes one `op_write { rowset.delete_predicate }`
/// txn log per tablet via [`crate::connector::starrocks::lake::transactions::delete_data`],
/// then publishes the txn and refreshes the in-memory catalog.
///
/// PRIMARY_KEYS tables do not use this path: their DELETE is rewritten into
/// a `SELECT pk_cols, 1 AS __op` insert that flows through the regular sink
/// path so the PK-applier consumes `.del` files.
pub(crate) fn delete_starrocks_table_by_predicate(
    state: &Arc<StandaloneState>,
    database_name: &str,
    table_name: &str,
    delete_predicate_pb: DeletePredicatePb,
) -> Result<(), String> {
    // MV rejection before doing any txn work.
    {
        let starrocks = state
            .starrocks_table
            .read()
            .expect("standalone StarRocks table read lock");
        let runtime = starrocks.table(database_name, table_name)?;
        if matches!(
            runtime.table.kind,
            super::model::StarRocksTableKind::MaterializedView
        ) {
            return Err(format!(
                "The data of '{}.{}' cannot be deleted because it is a materialized view; \
                 the data of materialized view must be consistent with the base table.",
                database_name, table_name
            ));
        }
    }

    let resolved = LocalTableIdentity {
        database: normalize_identifier(database_name)?,
        table: normalize_identifier(table_name)?,
    };
    let plan = load_physical_insert_plan(state, &resolved, PartitionTarget::Active)?;
    let tablet_ids: Vec<i64> = plan.tablets.iter().map(|t| t.tablet_id).collect();
    if tablet_ids.is_empty() {
        return Err(format!(
            "StarRocks table {database_name}.{table_name} active partition has no tablets"
        ));
    }

    let prepared = prepare_starrocks_txn(state, &plan)?;

    let abort = |txn_id: i64, err: String| -> String {
        if let Err(abort_err) = mark_starrocks_txn_aborted(state, txn_id) {
            return format!(
                "StarRocks delete failed: {err}; additionally mark_starrocks_txn_aborted failed: {abort_err}"
            );
        }
        err
    };

    let request = DeleteDataRequest {
        tablet_ids: tablet_ids.clone(),
        txn_id: Some(prepared.txn_id),
        delete_predicate: Some(delete_predicate_pb),
        schema_key: Some(TableSchemaKeyPb {
            db_id: Some(plan.db_id),
            table_id: Some(plan.table_id),
            schema_id: plan.tablet_schema.id,
        }),
    };

    let response = crate::connector::starrocks::lake::transactions::delete_data(&request)
        .map_err(|e| abort(prepared.txn_id, e))?;
    if !response.failed_tablets.is_empty() {
        return Err(abort(
            prepared.txn_id,
            format!(
                "delete_data failed for tablets {:?}",
                response.failed_tablets
            ),
        ));
    }

    mark_starrocks_txn_written(state, prepared.txn_id)?;

    publish_tablets_at_version(
        tablet_ids,
        prepared.txn_id,
        prepared.base_version,
        prepared.commit_version,
    )
    .map_err(|e| {
        abort(
            prepared.txn_id,
            format!("StarRocks delete publish failed: {e}"),
        )
    })?;

    mark_starrocks_txn_visible(state, prepared.txn_id)?;

    let mut starrocks = state
        .starrocks_table
        .write()
        .expect("standalone StarRocks table write lock");
    commit_catalog_visible_version(state, &mut starrocks, &plan, prepared.commit_version)?;
    drop(starrocks);
    Ok(())
}

fn publish_starrocks_txn(
    plan: &StarRocksInsertPlan,
    prepared: &StoredStarRocksTxn,
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
    plan: &StarRocksInsertPlan,
    txn_id: i64,
    written_tablet_ids: &HashSet<i64>,
) -> Result<(), String> {
    let starrocks_table_config = state
        .starrocks_table_config
        .as_ref()
        .ok_or_else(|| "standalone StarRocks table config is missing during insert".to_string())?
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
            s3_config: s3_config_for_tablet_path(
                &tablet.tablet_root_path,
                &starrocks_table_config.s3,
            )?,
            partial_update: PartialUpdateWritePolicy::default(),
        };
        update_tablet_runtime_schema(tablet.tablet_id, &plan.tablet_schema)?;
        append_lake_txn_log_empty_rowset(&write_ctx, txn_id, plan.partition_id, None)?;
    }
    Ok(())
}

fn s3_config_for_tablet_path(
    tablet_root_path: &str,
    starrocks_s3: &S3StoreConfig,
) -> Result<Option<S3StoreConfig>, String> {
    if resolve_tablet_root(tablet_root_path, None).is_ok() {
        return Ok(None);
    }
    resolve_tablet_root(tablet_root_path, Some(starrocks_s3))
        .map_err(|err| format!("StarRocks table write invalid tablet path: {err}"))?;
    Ok(Some(starrocks_s3.clone()))
}

fn object_store_profile_for_tablet_path(
    tablet_root_path: &str,
    starrocks_s3: &S3StoreConfig,
) -> Result<Option<ObjectStoreProfile>, String> {
    if resolve_tablet_root(tablet_root_path, None).is_ok() {
        return Ok(None);
    }
    object_store_profile_for_tablet_root(tablet_root_path, Some(starrocks_s3))
        .map_err(|err| format!("StarRocks table write invalid tablet path: {err}"))
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
    starrocks: &mut super::catalog::StarRocksTableCatalog,
    plan: &StarRocksInsertPlan,
    new_visible_version: i64,
) -> Result<(), String> {
    let table_id = starrocks.advance_partition_version(plan.partition_id, new_visible_version)?;
    let runtime = starrocks
        .runtime_by_table_id(table_id)
        .cloned()
        .ok_or_else(|| format!("StarRocks runtime missing for table_id={table_id}"))?;

    let mut catalog = state
        .catalog_service
        .local()
        .write()
        .expect("standalone catalog write lock");
    register_starrocks_table_in_catalog(&mut catalog, &runtime)?;
    Ok(())
}

/// Plan-pipeline path for `INSERT INTO <starrocks_table> SELECT ...`.
///
/// Matches the StarRocks FE shape of INSERT-SELECT: the SELECT is analyzed,
/// planned, optimised and executed through the normal query stack; the
/// collected output is projected/cast to the target table's column layout
/// and then handed to the StarRocks table txn path that `VALUES` INSERT uses.
///
/// The output is materialised into a single Arrow batch before writing.
/// That is fine for the current target workload (INSERT ... SELECT of up to
/// a few hundred thousand rows) but is explicitly a single-node limitation
/// — a true streaming `StarRocksTableSink` operator will be needed once the
/// pipeline needs to run across multiple BEs.
fn insert_from_query_into_starrocks_table(
    state: &Arc<StandaloneState>,
    resolved: &LocalTableIdentity,
    plan: &StarRocksInsertPlan,
    insert_columns: &[String],
    query: &sqlparser::ast::Query,
) -> Result<StatementResult, String> {
    // Resolve SELECT against the target table's database so unqualified
    // references in the SELECT pick up the right schema; matches the INSERT
    // target namespace established by `resolve_starrocks_name`.
    let query_result = {
        let catalog = state
            .catalog_service
            .local()
            .read()
            .expect("standalone catalog read lock");
        let connectors_snapshot = state
            .connectors
            .read()
            .expect("standalone connector registry read lock")
            .clone();
        execute_query(
            query,
            &catalog,
            &connectors_snapshot,
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

    write_chunks_into_starrocks_partition_inner(
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
                    crate::formats::parquet::local_io::normalize_map_entries_nullability(
                        &c.data_type,
                    ),
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
            let target_type = crate::formats::parquet::local_io::normalize_map_entries_nullability(
                &target_column.data_type,
            );
            let array: ArrayRef = match source_idx {
                Some(idx) => {
                    let src = batch.column(*idx);
                    if src.data_type() == &target_type {
                        src.clone()
                    } else {
                        cast_with_special_rules(src, &target_type).map_err(|e| {
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
        let target_type = crate::formats::parquet::local_io::normalize_map_entries_nullability(
            &target_column.data_type,
        );
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

fn resolve_starrocks_name(
    name: &ObjectName,
    current_database: &str,
) -> Result<LocalTableIdentity, String> {
    use novarocks_catalog::identifier::normalize_identifier;
    match name.parts.as_slice() {
        [table] => Ok(LocalTableIdentity {
            database: normalize_identifier(current_database)?,
            table: normalize_identifier(table)?,
        }),
        [database, table] => Ok(LocalTableIdentity {
            database: normalize_identifier(database)?,
            table: normalize_identifier(table)?,
        }),
        _ => Err(format!(
            "StarRocks table name must be `<table>` or `<database>.<table>`, got `{}`",
            name.parts.join(".")
        )),
    }
}
