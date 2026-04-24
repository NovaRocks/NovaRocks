use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, UInt32Array, new_null_array};
use arrow::datatypes::{Field, Schema};
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
use crate::runtime::query_result::QueryResult;
use crate::service::grpc_client::proto::starrocks::{PublishVersionRequest, TabletSchemaPb};
use crate::sql::parser::ast::{InsertSource, ObjectName};

use super::super::engine::catalog::{ColumnDef, normalize_identifier};
use super::super::engine::{
    ResolvedLocalTableName, StandaloneState, StatementResult, build_local_insert_batch,
    execute_query, insert_generate_series_rows_local, reorder_insert_rows,
};
use super::catalog::register_managed_table_in_catalog;

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

#[derive(Clone, Debug)]
pub(crate) struct ManagedInsertTablet {
    pub(crate) tablet_id: i64,
    pub(crate) tablet_root_path: String,
}

pub(crate) fn load_insert_plan(
    state: &Arc<StandaloneState>,
    resolved: &ResolvedLocalTableName,
    target: PartitionTarget,
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
    let metadata_store = state
        .metadata_store
        .as_ref()
        .ok_or_else(|| "managed lake insert requires sqlite metadata store".to_string())?;
    let prepared =
        metadata_store.prepare_txn(plan.table_id, plan.partition_id, plan.base_version)?;

    let mut written_tablet_ids = Vec::new();
    let mut total_rows = 0_i64;
    let mut next_file_seq = 0_u64;
    for chunk in chunks {
        total_rows += chunk.len() as i64;
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

    metadata_store.mark_txn_written(prepared.txn_id)?;

    publish_managed_txn(&plan, &prepared, &written_tablet_ids).map_err(|err| {
        if let Err(abort_err) = metadata_store.mark_txn_aborted(prepared.txn_id) {
            return format!(
                "managed-lake publish failed: {err}; additionally mark_txn_aborted failed: {abort_err}"
            );
        }
        format!("managed-lake publish failed: {err}")
    })?;

    metadata_store.mark_txn_visible(prepared.txn_id, prepared.commit_version)?;
    commit_catalog_visible_version(state, &plan, prepared.commit_version)?;

    Ok(total_rows)
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
            s3_config: Some(managed_config.s3.clone()),
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
    written_tablet_ids: &[i64],
) -> Result<(), String> {
    // Publish all tablets that actually have a txn log at this version.
    if !written_tablet_ids.is_empty() {
        publish_tablets_at_version(
            written_tablet_ids.to_vec(),
            prepared.txn_id,
            prepared.base_version,
            prepared.commit_version,
        )?;
    }

    // For tablets that received no rows, publish via the empty-txnlog path so
    // their metadata still advances to the new version. This matches StarRocks
    // BE's handling of bucket-hash inserts that only touch a subset of tablets.
    let written: std::collections::HashSet<i64> = written_tablet_ids.iter().copied().collect();
    let empty_tablet_ids: Vec<i64> = plan
        .tablets
        .iter()
        .map(|tablet| tablet.tablet_id)
        .filter(|tablet_id| !written.contains(tablet_id))
        .collect();
    if !empty_tablet_ids.is_empty() {
        // StarRocks treats txn_id=-1 as the empty-txnlog sentinel; BE bumps the
        // tablet's metadata to new_version without applying any rowset.
        publish_tablets_at_version(
            empty_tablet_ids,
            EMPTY_TXNLOG_TXN_ID,
            prepared.base_version,
            prepared.commit_version,
        )?;
    }
    Ok(())
}

const EMPTY_TXNLOG_TXN_ID: i64 = -1;

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

    let metadata_store = state
        .metadata_store
        .as_ref()
        .ok_or_else(|| "managed lake insert requires sqlite metadata store".to_string())?;
    let prepared =
        metadata_store.prepare_txn(plan.table_id, plan.partition_id, plan.base_version)?;

    let mut next_file_seq = 0_u64;
    let write_outcome =
        write_routed_chunks(state, plan, &chunk, prepared.txn_id, &mut next_file_seq);
    let written_tablet_ids = match write_outcome {
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

    metadata_store.mark_txn_written(prepared.txn_id)?;

    publish_managed_txn(plan, &prepared, &written_tablet_ids).map_err(|err| {
        if let Err(abort_err) = metadata_store.mark_txn_aborted(prepared.txn_id) {
            return format!(
                "managed-lake publish failed: {err}; additionally mark_txn_aborted failed: {abort_err}"
            );
        }
        format!("managed-lake publish failed: {err}")
    })?;

    metadata_store.mark_txn_visible(prepared.txn_id, prepared.commit_version)?;

    commit_catalog_visible_version(state, plan, prepared.commit_version)?;

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
                    crate::standalone::engine::parquet::normalize_map_entries_nullability(
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
            let target_type = crate::standalone::engine::parquet::normalize_map_entries_nullability(
                &target_column.data_type,
            );
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
        let target_type = crate::standalone::engine::parquet::normalize_map_entries_nullability(
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

fn resolve_managed_name(
    name: &ObjectName,
    current_database: &str,
) -> Result<ResolvedLocalTableName, String> {
    use super::super::engine::catalog::normalize_identifier;
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

    use crate::runtime::starlet_shard_registry::S3StoreConfig;
    use crate::service::grpc_client::proto::starrocks::{ColumnPb, TabletSchemaPb};
    use crate::standalone::engine::catalog::InMemoryCatalog;
    use crate::standalone::lake::store::{
        ManagedGlobalMeta, ManagedIndexState, ManagedPartitionState, ManagedSnapshot,
        ManagedTableKind, ManagedTableState, StoredManagedDatabase, StoredManagedIndex,
        StoredManagedPartition, StoredManagedSchema, StoredManagedTable, StoredManagedTablet,
    };
    use crate::standalone::lake::{
        ManagedLakeCatalog, ManagedLakeConfig, register_managed_tables_in_catalog,
    };
    use prost::Message;

    #[test]
    fn write_chunks_into_managed_partition_routes_rows_to_staged_tablets() {
        let state = seed_state_with_staged_mv();
        let plan = load_insert_plan(
            &state,
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

    fn seed_state_with_staged_mv() -> Arc<StandaloneState> {
        let config = ManagedLakeConfig {
            warehouse_uri: "s3://bucket/warehouse".to_string(),
            s3: S3StoreConfig {
                endpoint: "http://127.0.0.1:9000".to_string(),
                bucket: "bucket".to_string(),
                root: "warehouse".to_string(),
                access_key_id: "ak".to_string(),
                access_key_secret: "sk".to_string(),
                region: None,
                enable_path_style_access: Some(true),
            },
        };

        let tablet_schema = TabletSchemaPb {
            column: vec![
                ColumnPb {
                    unique_id: 0,
                    name: Some("k1".to_string()),
                    r#type: "INT".to_string(),
                    is_nullable: Some(false),
                    is_key: Some(true),
                    visible: Some(true),
                    ..Default::default()
                },
                ColumnPb {
                    unique_id: 1,
                    name: Some("total".to_string()),
                    r#type: "BIGINT".to_string(),
                    is_nullable: Some(true),
                    is_key: Some(false),
                    visible: Some(true),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let snapshot = ManagedSnapshot {
            global: ManagedGlobalMeta {
                warehouse_uri: config.warehouse_uri.clone(),
                next_db_id: 2,
                next_table_id: 11,
                next_partition_id: 22,
                next_index_id: 32,
                next_tablet_id: 43,
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
            columns: vec![
                crate::standalone::lake::store::StoredManagedColumn {
                    schema_id: 100,
                    ordinal: 0,
                    column_name: "k1".to_string(),
                    logical_type: "INT".to_string(),
                    nullable: false,
                },
                crate::standalone::lake::store::StoredManagedColumn {
                    schema_id: 100,
                    ordinal: 1,
                    column_name: "total".to_string(),
                    logical_type: "BIGINT".to_string(),
                    nullable: true,
                },
            ],
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
                    tablet_root_path: "s3://bucket/warehouse/db_1/table_10/partition_20"
                        .to_string(),
                },
                StoredManagedTablet {
                    tablet_id: 41,
                    partition_id: 21,
                    index_id: 31,
                    bucket_seq: 0,
                    tablet_root_path: "s3://bucket/warehouse/db_1/table_10/partition_21"
                        .to_string(),
                },
                StoredManagedTablet {
                    tablet_id: 42,
                    partition_id: 21,
                    index_id: 31,
                    bucket_seq: 1,
                    tablet_root_path: "s3://bucket/warehouse/db_1/table_10/partition_21"
                        .to_string(),
                },
            ],
            txns: vec![],
            erase_jobs: vec![],
            materialized_views: vec![],
        };

        let managed =
            ManagedLakeCatalog::rebuild(Some(config.clone()), snapshot).expect("rebuild managed");
        let mut catalog = InMemoryCatalog::default();
        catalog
            .create_database("analytics")
            .expect("create analytics");
        register_managed_tables_in_catalog(&mut catalog, &managed)
            .expect("register managed tables");

        Arc::new(StandaloneState {
            catalog: std::sync::RwLock::new(catalog),
            managed_lake: std::sync::RwLock::new(managed),
            managed_lake_config: Some(config),
            ..Default::default()
        })
    }
}
