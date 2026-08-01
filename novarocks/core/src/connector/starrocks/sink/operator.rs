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

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Int8Array, Int16Array, Int32Array,
    Int64Array, LargeStringArray, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow::compute::{cast, concat_batches, filter_record_batch, take};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use chrono::{NaiveDate, NaiveDateTime};

use crate::common::ids::SlotId;
use crate::connector::starrocks::fe_v2_meta::{
    LakeTabletPartitionRef, resolve_tablet_paths_for_olap_sink,
};
use crate::connector::starrocks::lake::context::{AutoIncrementWritePolicy, get_tablet_runtime};
use crate::connector::starrocks::lake::txn_log::append_lake_txn_log_empty_rowset;
use crate::connector::starrocks::lake::{
    TabletWriteContext, append_lake_txn_log_with_chunk_rowset,
};
use crate::connector::starrocks::ports::{AutomaticPartitionRequest, SinkFrontendAddress};
use crate::connector::starrocks::schema::{StarRocksKeysType, StarRocksTabletSchema};
use crate::connector::starrocks::sink::auto_increment::allocate_auto_increment_ids;
use crate::connector::starrocks::sink::factory::{
    OlapTableSinkPlan, STARROCKS_DEFAULT_PARTITION_VALUE, SinkIndexWritePlan, TabletWriteTarget,
    automatic_partition_result_from_port, resolve_s3_for_sink_tablet,
};
use crate::connector::starrocks::sink::partition_key::{
    PartitionKeySource, PartitionMode, PartitionRoutingEntry, build_partition_key_arrays,
    partition_key_source_len, validate_partition_key_length,
};
use crate::connector::starrocks::sink::plan::{CreatePartitionResult, SinkPredicatePlan};
use crate::connector::starrocks::sink::routing::{
    RowRejectReason, RowRoutingPlan, route_chunk_rows,
};
use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSlotSchema};
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::novarocks_logging::{debug, info};
use crate::runtime::runtime_state::RuntimeState;
use crate::runtime::sink_commit::{TabletCommitInfo, TabletFailInfo};
use crate::runtime::starlet_shard_registry;

const LOAD_OP_COLUMN: &str = "__op";

pub(crate) struct OlapTableSinkOperator {
    name: String,
    plan: Arc<OlapTableSinkPlan>,
    finalize_shared: Arc<OlapSinkFinalizeSharedState>,
    driver_id: i32,
    file_seq: u64,
    next_random_hash: u32,
    pending_chunks: Vec<Chunk>,
    pending_input_rows: usize,
    pending_input_bytes: usize,
    row_routing: RowRoutingPlan,
    write_targets: HashMap<i64, TabletWriteTarget>,
    all_write_targets: HashMap<i64, TabletWriteTarget>,
    index_write_plans: Vec<SinkIndexWritePlan>,
    tablet_commit_infos: Vec<TabletCommitInfo>,
    seen_partition_values: HashSet<Vec<String>>,
    auto_partition_initialized: bool,
    auto_partition_debug_logged: bool,
    input_rows: i64,
    loaded_rows: i64,
    filtered_rows: i64,
    finished: bool,
    written_tablets: HashSet<i64>,
    dirty_partitions: HashSet<i64>,
}

#[derive(Default)]
pub(crate) struct OlapSinkFinalizeSharedState {
    registered_drivers: AtomicUsize,
    remaining_drivers: AtomicUsize,
    write_targets: Mutex<HashMap<i64, TabletWriteTarget>>,
    dirty_partitions: Mutex<HashSet<i64>>,
    written_tablets: Mutex<HashSet<i64>>,
    tablet_commit_infos: Mutex<Vec<TabletCommitInfo>>,
    first_error: Mutex<Option<String>>,
    fail_infos_reported: AtomicBool,
    commit_infos_reported: AtomicBool,
}

impl OlapSinkFinalizeSharedState {
    pub(crate) fn register_driver(&self) {
        self.registered_drivers.fetch_add(1, Ordering::AcqRel);
        self.remaining_drivers.fetch_add(1, Ordering::AcqRel);
    }

    fn record_progress(
        &self,
        write_targets: &HashMap<i64, TabletWriteTarget>,
        written_tablets: &HashSet<i64>,
        dirty_partitions: &HashSet<i64>,
        tablet_commit_infos: &[TabletCommitInfo],
    ) {
        if !write_targets.is_empty()
            && let Ok(mut guard) = self.write_targets.lock()
        {
            for (tablet_id, target) in write_targets {
                guard.entry(*tablet_id).or_insert_with(|| target.clone());
            }
        }
        if !written_tablets.is_empty()
            && let Ok(mut guard) = self.written_tablets.lock()
        {
            guard.extend(written_tablets.iter().copied());
        }
        if !dirty_partitions.is_empty()
            && let Ok(mut guard) = self.dirty_partitions.lock()
        {
            guard.extend(dirty_partitions.iter().copied());
        }
        if !tablet_commit_infos.is_empty()
            && let Ok(mut guard) = self.tablet_commit_infos.lock()
        {
            for info in tablet_commit_infos {
                let exists = guard.iter().any(|current| {
                    current.tablet_id == info.tablet_id && current.backend_id == info.backend_id
                });
                if !exists {
                    guard.push(info.clone());
                }
            }
        }
    }

    fn snapshot_progress(
        &self,
    ) -> (
        HashMap<i64, TabletWriteTarget>,
        HashSet<i64>,
        HashSet<i64>,
        Vec<TabletCommitInfo>,
    ) {
        let write_targets = self
            .write_targets
            .lock()
            .map(|guard| guard.clone())
            .unwrap_or_default();
        let dirty_partitions = self
            .dirty_partitions
            .lock()
            .map(|guard| guard.clone())
            .unwrap_or_default();
        let written_tablets = self
            .written_tablets
            .lock()
            .map(|guard| guard.clone())
            .unwrap_or_default();
        let tablet_commit_infos = self
            .tablet_commit_infos
            .lock()
            .map(|guard| guard.clone())
            .unwrap_or_default();
        (
            write_targets,
            dirty_partitions,
            written_tablets,
            tablet_commit_infos,
        )
    }

    fn record_error(&self, err: String) {
        if let Ok(mut guard) = self.first_error.lock()
            && guard.is_none()
        {
            *guard = Some(err);
        }
    }

    fn first_error(&self) -> Option<String> {
        self.first_error.lock().ok().and_then(|guard| guard.clone())
    }

    fn arrive_and_is_last(&self) -> bool {
        loop {
            let current = self.remaining_drivers.load(Ordering::Acquire);
            if current == 0 {
                return true;
            }
            if self
                .remaining_drivers
                .compare_exchange(current, current - 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return current == 1;
            }
        }
    }

    fn mark_fail_infos_reported(&self) -> bool {
        !self.fail_infos_reported.swap(true, Ordering::AcqRel)
    }

    fn mark_commit_infos_reported(&self) -> bool {
        !self.commit_infos_reported.swap(true, Ordering::AcqRel)
    }
}

struct TabletBufferedState {
    partition_id: i64,
    context: TabletWriteContext,
    request_batches: Vec<Chunk>,
    request_rows: usize,
    request_bytes: usize,
    memtable_batches: Vec<Chunk>,
    memtable_rows: usize,
    memtable_bytes: usize,
}

impl TabletBufferedState {
    fn new(partition_id: i64, context: TabletWriteContext) -> Self {
        Self {
            partition_id,
            context,
            request_batches: Vec::new(),
            request_rows: 0,
            request_bytes: 0,
            memtable_batches: Vec::new(),
            memtable_rows: 0,
            memtable_bytes: 0,
        }
    }

    fn push_request_batch(&mut self, batch: Chunk) {
        self.request_rows = self.request_rows.saturating_add(batch.len());
        self.request_bytes = self.request_bytes.saturating_add(batch.estimated_bytes());
        self.request_batches.push(batch);
    }

    fn should_seal_request_batch(&self, row_threshold: usize, bytes_threshold: usize) -> bool {
        self.request_rows >= row_threshold || self.request_bytes >= bytes_threshold
    }

    fn take_request_batch(&mut self) -> Result<Option<Chunk>, String> {
        let Some(batch) = concat_buffered_chunks(&mut self.request_batches)? else {
            return Ok(None);
        };
        self.request_rows = 0;
        self.request_bytes = 0;
        Ok(Some(batch))
    }

    fn push_memtable_batch(&mut self, batch: Chunk) {
        self.memtable_rows = self.memtable_rows.saturating_add(batch.len());
        self.memtable_bytes = self.memtable_bytes.saturating_add(batch.estimated_bytes());
        self.memtable_batches.push(batch);
    }

    fn should_flush_memtable(&self, bytes_threshold: usize) -> bool {
        self.memtable_bytes >= bytes_threshold
    }

    fn take_memtable_batch(&mut self) -> Result<Option<Chunk>, String> {
        let Some(batch) = concat_buffered_chunks(&mut self.memtable_batches)? else {
            return Ok(None);
        };
        self.memtable_rows = 0;
        self.memtable_bytes = 0;
        Ok(Some(batch))
    }
}

fn concat_buffered_chunks(batches: &mut Vec<Chunk>) -> Result<Option<Chunk>, String> {
    if batches.is_empty() {
        return Ok(None);
    }
    if batches.len() == 1 {
        return Ok(batches.pop());
    }
    let schema = batches
        .first()
        .map(|batch| batch.schema())
        .ok_or_else(|| "OLAP_TABLE_SINK buffered batches unexpectedly empty".to_string())?;
    let chunk_schema = batches
        .first()
        .map(|batch| batch.chunk_schema_ref())
        .ok_or_else(|| "OLAP_TABLE_SINK buffered chunks unexpectedly empty".to_string())?;
    let merged_batches = batches
        .iter()
        .map(|chunk| chunk.batch.clone())
        .collect::<Vec<_>>();
    let merged = concat_batches(&schema, merged_batches.as_slice())
        .map_err(|e| format!("OLAP_TABLE_SINK concat buffered batches failed: {e}"))?;
    batches.clear();
    Ok(Some(Chunk::try_new_with_chunk_schema(
        merged,
        chunk_schema,
    )?))
}

#[derive(Debug)]
struct FilteredBatch {
    batch: RecordBatch,
    rejected_rows: usize,
    tracking_logs: Vec<String>,
}

fn delete_row_mask_for_auto_increment(batch: &RecordBatch) -> Result<Option<Vec<bool>>, String> {
    let Some(op_idx) = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .find_map(|(idx, field)| (field.name() == LOAD_OP_COLUMN).then_some(idx))
    else {
        return Ok(None);
    };

    let op_col = batch.column(op_idx);
    match op_col.data_type() {
        DataType::Int8 => {
            let typed = op_col
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| "downcast __op Int8Array failed".to_string())?;
            let mut out = Vec::with_capacity(typed.len());
            for row_idx in 0..typed.len() {
                out.push(!typed.is_null(row_idx) && typed.value(row_idx) != 0);
            }
            Ok(Some(out))
        }
        DataType::Int32 => {
            let typed = op_col
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "downcast __op Int32Array failed".to_string())?;
            let mut out = Vec::with_capacity(typed.len());
            for row_idx in 0..typed.len() {
                out.push(!typed.is_null(row_idx) && typed.value(row_idx) != 0);
            }
            Ok(Some(out))
        }
        other => Err(format!(
            "OLAP_TABLE_SINK unsupported '{}' column type for auto_increment handling: {:?}",
            LOAD_OP_COLUMN, other
        )),
    }
}

fn materialize_auto_increment_for_sink_batch(
    batch: &RecordBatch,
    tablet_schema: &StarRocksTabletSchema,
    auto_increment: Option<&AutoIncrementWritePolicy>,
    rejected: &mut [bool],
    tracking_logs: &mut Vec<String>,
    table_id: i64,
) -> Result<RecordBatch, String> {
    let Some(auto_policy) = auto_increment else {
        return Ok(batch.clone());
    };
    let Some(auto_idx) = auto_policy.auto_increment_column_idx else {
        return Ok(batch.clone());
    };
    if auto_idx >= batch.num_columns() || auto_idx >= tablet_schema.column.len() {
        return Ok(batch.clone());
    }

    let auto_column = batch.column(auto_idx);
    if auto_column.null_count() == 0 {
        return Ok(batch.clone());
    }
    // When miss_auto_increment_column is true, the auto-increment column is a
    // placeholder NULL.  Do not allocate IDs here — the lake writer's partial
    // upsert path handles it: existing rows keep their value, new rows get a
    // fresh ID.  We must keep the NULLs so the non-nullable filter below
    // skips this column (handled by the auto-increment column exemption).
    if auto_policy.miss_auto_increment_column {
        return Ok(batch.clone());
    }
    let auto_col_name = auto_policy
        .auto_increment_column_name
        .as_deref()
        .filter(|name| !name.trim().is_empty())
        .or_else(|| {
            tablet_schema
                .column
                .get(auto_idx)
                .and_then(|column| column.name.as_deref())
                .filter(|name| !name.trim().is_empty())
        })
        .unwrap_or("<auto_increment>");

    // When null_expr_in_auto_increment is true, mark null rows as rejected and
    // return early.  The auto-increment counter was already advanced by the
    // pre-routing fill (fill_auto_increment_in_chunk_before_routing), so we
    // must NOT allocate again here.
    if auto_policy.null_expr_in_auto_increment {
        for (row_idx, is_rejected) in rejected.iter_mut().enumerate().take(batch.num_rows()) {
            if !auto_column.is_null(row_idx) || *is_rejected {
                continue;
            }
            *is_rejected = true;
            tracking_logs.push(format!(
                "Error: NULL value in auto increment column '{}'. Row: {}",
                auto_col_name,
                format_tracking_row(batch, row_idx)?
            ));
        }
        return Ok(batch.clone());
    }

    let delete_rows = if tablet_schema.keys_type == Some(StarRocksKeysType::Primary) {
        delete_row_mask_for_auto_increment(batch)?
    } else {
        None
    };
    let alloc_rows = (0..batch.num_rows())
        .filter(|row_idx| {
            auto_column.is_null(*row_idx)
                && !delete_rows
                    .as_ref()
                    .is_some_and(|mask| mask.get(*row_idx).copied().unwrap_or(false))
        })
        .count();
    let allocated_ids = if alloc_rows == 0 {
        Vec::new()
    } else {
        let fe_addr = auto_policy.fe_addr.as_ref().ok_or_else(|| {
            "OLAP_TABLE_SINK cannot allocate auto_increment id without FE address".to_string()
        })?;
        allocate_auto_increment_ids(
            auto_policy.frontend_provider.as_deref(),
            fe_addr,
            table_id,
            alloc_rows,
        )?
    };

    let mut next_alloc = 0usize;
    let filled_auto_column: ArrayRef = match auto_column.data_type() {
        DataType::Int64 => {
            let typed = auto_column
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "downcast Int64Array failed".to_string())?;
            let mut values = Vec::with_capacity(typed.len());
            for row_idx in 0..typed.len() {
                if typed.is_null(row_idx) {
                    let is_delete = delete_rows
                        .as_ref()
                        .is_some_and(|mask| mask.get(row_idx).copied().unwrap_or(false));
                    if is_delete {
                        values.push(0);
                    } else {
                        let auto_id = *allocated_ids.get(next_alloc).ok_or_else(|| {
                            "allocate_auto_increment_ids returned fewer ids than requested"
                                .to_string()
                        })?;
                        next_alloc = next_alloc.saturating_add(1);
                        values.push(auto_id);
                    }
                } else {
                    values.push(typed.value(row_idx));
                }
            }
            Arc::new(Int64Array::from(values))
        }
        DataType::Int32 => {
            let typed = auto_column
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "downcast Int32Array failed".to_string())?;
            let mut values = Vec::with_capacity(typed.len());
            for row_idx in 0..typed.len() {
                if typed.is_null(row_idx) {
                    let is_delete = delete_rows
                        .as_ref()
                        .is_some_and(|mask| mask.get(row_idx).copied().unwrap_or(false));
                    if is_delete {
                        values.push(0);
                    } else {
                        let auto_id = *allocated_ids.get(next_alloc).ok_or_else(|| {
                            "allocate_auto_increment_ids returned fewer ids than requested"
                                .to_string()
                        })?;
                        let casted = i32::try_from(auto_id).map_err(|_| {
                            format!(
                                "auto_increment value overflow for INT column '{}'",
                                auto_col_name
                            )
                        })?;
                        next_alloc = next_alloc.saturating_add(1);
                        values.push(casted);
                    }
                } else {
                    values.push(typed.value(row_idx));
                }
            }
            Arc::new(Int32Array::from(values))
        }
        other => {
            return Err(format!(
                "OLAP_TABLE_SINK unsupported auto_increment column type: column='{}' type={:?}",
                auto_col_name, other
            ));
        }
    };
    if next_alloc != allocated_ids.len() {
        return Err(format!(
            "allocate_auto_increment_ids returned unexpected id count: expected={} actual={}",
            next_alloc,
            allocated_ids.len()
        ));
    }

    let mut columns = batch.columns().to_vec();
    columns[auto_idx] = filled_auto_column;
    RecordBatch::try_new(batch.schema(), columns)
        .map_err(|e| format!("OLAP_TABLE_SINK build auto_increment batch failed: {e}"))
}

/// Fills auto-increment NULLs in a sink chunk BEFORE hash distribution, so that
/// IDs are assigned in the original INSERT row order. This matches StarRocks C++ BE
/// behavior where `_fill_auto_increment_id` is called before row routing.
///
/// When `miss_auto_increment_column` is true (partial update that does not include
/// the auto-increment column), NULLs are filled with 0 and real allocation is
/// deferred to the lake writer (DeltaWriter equivalent).
/// Merges multiple sink chunks into a single chunk so that auto-increment IDs
/// are allocated in one sequential batch. The FE may split a single INSERT
/// statement into multiple per-row chunks; merging them here ensures the
/// auto-increment counter advances in the original row order.
fn merge_sink_chunks_for_auto_increment(chunks: Vec<Chunk>) -> Result<Vec<Chunk>, String> {
    if chunks.len() <= 1 {
        return Ok(chunks);
    }
    // All chunks should share the same schema.
    let first = match chunks.first() {
        Some(c) if !c.is_empty() => c,
        _ => return Ok(chunks),
    };
    let schema = first.batch.schema();
    let chunk_schema = first.chunk_schema_ref();

    let merged = concat_batches(&schema, chunks.iter().map(|c| &c.batch))
        .map_err(|e| format!("merge sink chunks for auto_increment failed: {e}"))?;
    let merged_chunk = Chunk::try_new_with_chunk_schema(merged, chunk_schema)?;
    Ok(vec![merged_chunk])
}

fn fill_auto_increment_in_chunk_before_routing(
    chunk: &Chunk,
    plan: &OlapTableSinkPlan,
) -> Result<Chunk, String> {
    let Some(auto_slot_id) = plan.auto_increment_output_slot_id else {
        return Ok(chunk.clone());
    };
    if plan.miss_auto_increment_column {
        // Partial update without auto-increment column in input —
        // real allocation is deferred to the lake writer (DeltaWriter equivalent).
        return Ok(chunk.clone());
    }
    // Note: null_expr_in_auto_increment does NOT skip allocation here.
    // IDs must still be allocated to advance the counter, matching StarRocks
    // C++ BE where _fill_auto_increment_id always allocates and _validate_data
    // rejects null rows afterwards.
    let Some(&batch_idx) = chunk.slot_id_to_index().get(&auto_slot_id) else {
        return Ok(chunk.clone());
    };
    let auto_column = chunk.batch.column(batch_idx);
    if auto_column.null_count() == 0 {
        return Ok(chunk.clone());
    }

    // Build delete-row mask: delete rows get value 0 (same as C++ BE).
    let delete_rows = delete_row_mask_for_auto_increment(&chunk.batch)?;
    let alloc_rows = (0..chunk.batch.num_rows())
        .filter(|i| {
            auto_column.is_null(*i)
                && !delete_rows
                    .as_ref()
                    .is_some_and(|m| m.get(*i).copied().unwrap_or(false))
        })
        .count();

    // Allocate IDs from FE.
    let allocated_ids = if alloc_rows == 0 {
        Vec::new()
    } else {
        let fe_addr = plan
            .write_targets
            .values()
            .next()
            .and_then(|t| t.context.partial_update.auto_increment.fe_addr.as_ref())
            .ok_or_else(|| {
                "OLAP_TABLE_SINK cannot allocate auto_increment id without FE address".to_string()
            })?;
        let provider = plan.write_targets.values().next().and_then(|target| {
            target
                .context
                .partial_update
                .auto_increment
                .frontend_provider
                .as_deref()
        });
        allocate_auto_increment_ids(provider, fe_addr, plan.table_id, alloc_rows)?
    };

    // When null_expr_in_auto_increment is true, we consumed IDs from the counter
    // (above) but must NOT fill them into the column.  Keeping the NULLs allows
    // the per-tablet non-nullable check to reject these rows — matching StarRocks
    // C++ BE where _fill_auto_increment_id writes to the data column but the null
    // bitmap is preserved, and _validate_data rejects based on that bitmap.
    if plan.null_expr_in_auto_increment {
        return Ok(chunk.clone());
    }

    let mut next_alloc = 0usize;
    let filled: ArrayRef = match auto_column.data_type() {
        DataType::Int64 => {
            let typed = auto_column
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "downcast Int64Array failed".to_string())?;
            let values: Vec<i64> = (0..typed.len())
                .map(|i| {
                    if typed.is_null(i) {
                        let is_delete = delete_rows
                            .as_ref()
                            .is_some_and(|m| m.get(i).copied().unwrap_or(false));
                        if is_delete {
                            0i64
                        } else {
                            let v = allocated_ids.get(next_alloc).copied().unwrap_or(0);
                            next_alloc += 1;
                            v
                        }
                    } else {
                        typed.value(i)
                    }
                })
                .collect();
            Arc::new(Int64Array::from(values))
        }
        DataType::Int32 => {
            let typed = auto_column
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "downcast Int32Array failed".to_string())?;
            let values: Vec<i32> = (0..typed.len())
                .map(|i| {
                    if typed.is_null(i) {
                        let is_delete = delete_rows
                            .as_ref()
                            .is_some_and(|m| m.get(i).copied().unwrap_or(false));
                        if is_delete {
                            0i32
                        } else {
                            let v = allocated_ids.get(next_alloc).copied().unwrap_or(0);
                            next_alloc += 1;
                            v as i32
                        }
                    } else {
                        typed.value(i)
                    }
                })
                .collect();
            Arc::new(Int32Array::from(values))
        }
        _ => return Ok(chunk.clone()),
    };

    let mut columns = chunk.batch.columns().to_vec();
    columns[batch_idx] = filled;
    let new_batch = RecordBatch::try_new(chunk.batch.schema(), columns)
        .map_err(|e| format!("build auto_increment pre-routing batch failed: {e}"))?;
    Chunk::try_new_with_chunk_schema(new_batch, chunk.chunk_schema_ref())
}

fn filter_rows_for_tablet_schema(
    batch: &RecordBatch,
    tablet_schema: &StarRocksTabletSchema,
    auto_increment: Option<&AutoIncrementWritePolicy>,
    table_id: i64,
) -> Result<FilteredBatch, String> {
    if batch.num_rows() == 0 {
        return Ok(FilteredBatch {
            batch: batch.clone(),
            rejected_rows: 0,
            tracking_logs: Vec::new(),
        });
    }

    let mut rejected = vec![false; batch.num_rows()];
    let mut tracking_logs = Vec::new();
    let materialized_batch = materialize_auto_increment_for_sink_batch(
        batch,
        tablet_schema,
        auto_increment,
        rejected.as_mut_slice(),
        &mut tracking_logs,
        table_id,
    )?;
    let column_count = materialized_batch
        .num_columns()
        .min(tablet_schema.column.len());
    let auto_increment_schema_idx = auto_increment.and_then(|p| p.auto_increment_column_idx);
    for column_idx in 0..column_count {
        let schema_col = &tablet_schema.column[column_idx];
        if schema_col.is_nullable.unwrap_or(true) {
            continue;
        }
        // Skip non-nullable check for auto-increment column ONLY when
        // miss_auto_increment_column is true (the column has placeholder NULLs
        // that the lake writer will fill).  For normal INSERT paths, NULLs in
        // auto-increment columns are already filled by the pre-routing fill or
        // per-tablet materialize; remaining NULLs indicate data-level NULLs
        // (e.g. INSERT...SELECT from a nullable source) and should be rejected.
        if auto_increment_schema_idx == Some(column_idx)
            && auto_increment.is_some_and(|p| p.miss_auto_increment_column)
        {
            continue;
        }
        let column = materialized_batch.column(column_idx);
        if column.null_count() == 0 {
            continue;
        }
        let column_name = schema_col
            .name
            .as_deref()
            .filter(|name| !name.trim().is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| {
                materialized_batch
                    .schema()
                    .field(column_idx)
                    .name()
                    .to_string()
            });
        for (row_idx, is_rejected) in rejected
            .iter_mut()
            .enumerate()
            .take(materialized_batch.num_rows())
        {
            if !column.is_null(row_idx) || *is_rejected {
                continue;
            }
            *is_rejected = true;
            tracking_logs.push(format!(
                "Error: NULL value in non-nullable column '{}'. Row: {}",
                column_name,
                format_tracking_row(&materialized_batch, row_idx)?
            ));
        }
    }

    let rejected_rows = rejected.iter().filter(|flag| **flag).count();
    if rejected_rows == 0 {
        return Ok(FilteredBatch {
            batch: materialized_batch,
            rejected_rows: 0,
            tracking_logs,
        });
    }

    let kept_indices = rejected
        .iter()
        .enumerate()
        .filter_map(|(row_idx, rejected)| (!rejected).then_some(row_idx as u32))
        .collect::<Vec<_>>();
    let filtered_batch = take_batch_rows(&materialized_batch, &kept_indices)?;
    Ok(FilteredBatch {
        batch: filtered_batch,
        rejected_rows,
        tracking_logs,
    })
}

fn apply_index_where_clause(
    predicate_chunk: &Chunk,
    target_chunk: &Chunk,
    index_id: i64,
    where_clause: Option<&SinkPredicatePlan>,
) -> Result<Option<Chunk>, String> {
    let Some(where_clause) = where_clause else {
        return Ok(Some(target_chunk.clone()));
    };
    if predicate_chunk.is_empty() {
        return Ok(Some(target_chunk.clone()));
    }

    let predicate = where_clause
        .arena
        .eval(where_clause.expr_id, predicate_chunk)
        .map_err(|e| {
            format!(
                "OLAP_TABLE_SINK evaluate index where_clause failed: index_id={} rows={} error={}",
                index_id,
                predicate_chunk.len(),
                e
            )
        })?;
    let predicate_bool = if predicate.data_type() == &DataType::Boolean {
        predicate
    } else {
        cast(predicate.as_ref(), &DataType::Boolean).map_err(|e| {
            format!(
                "OLAP_TABLE_SINK cast index where_clause to boolean failed: index_id={} from={:?} error={}",
                index_id,
                predicate.data_type(),
                e
            )
        })?
    };
    let predicate_bool = predicate_bool
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| {
            format!(
                "OLAP_TABLE_SINK index where_clause did not produce boolean array: index_id={} result_type={:?}",
                index_id,
                predicate_bool.data_type()
            )
        })?;
    if predicate_bool.len() != predicate_chunk.len() || predicate_bool.len() != target_chunk.len() {
        return Err(format!(
            "OLAP_TABLE_SINK index where_clause row count mismatch: index_id={} predicate_rows={} target_rows={} actual_rows={}",
            index_id,
            predicate_chunk.len(),
            target_chunk.len(),
            predicate_bool.len()
        ));
    }

    let keep = (0..predicate_bool.len())
        .map(|row| !predicate_bool.is_null(row) && predicate_bool.value(row))
        .collect::<Vec<_>>();
    if keep.iter().all(|keep_row| *keep_row) {
        return Ok(Some(target_chunk.clone()));
    }
    if keep.iter().all(|keep_row| !*keep_row) {
        return Ok(None);
    }

    let mask = BooleanArray::from(keep);
    let filtered_batch = filter_record_batch(&target_chunk.batch, &mask).map_err(|e| {
        format!(
            "OLAP_TABLE_SINK apply index where_clause failed: index_id={} rows={} error={}",
            index_id,
            target_chunk.len(),
            e
        )
    })?;
    Ok(Some(Chunk::try_new_with_chunk_schema(
        filtered_batch,
        target_chunk.chunk_schema_ref(),
    )?))
}

fn format_tracking_row(batch: &RecordBatch, row_idx: usize) -> Result<String, String> {
    let mut values = Vec::new();
    for (col_idx, field) in batch.schema().fields().iter().enumerate() {
        if field.name() == LOAD_OP_COLUMN {
            continue;
        }
        let column = batch.column(col_idx);
        if column.is_null(row_idx) {
            values.push("NULL".to_string());
            continue;
        }
        let rendered = array_value_to_string(column.as_ref(), row_idx).map_err(|e| {
            format!(
                "OLAP_TABLE_SINK render tracking row failed: column_index={}, row_index={}, error={}",
                col_idx, row_idx, e
            )
        })?;
        values.push(rendered);
    }
    Ok(format!("[{}]", values.join(", ")))
}

fn format_partition_rejection(batch: &RecordBatch, row_idx: usize) -> Result<String, String> {
    Ok(format!(
        "Error: The row is out of partition ranges. Please add a new partition.. Row: {}",
        format_tracking_row(batch, row_idx)?
    ))
}

impl OlapTableSinkOperator {
    const FLUSH_RETRY_MAX_TIMES: usize = 3;
    const FLUSH_RETRY_BASE_BACKOFF_MS: u64 = 200;
    const FLUSH_RETRY_MAX_BACKOFF_MS: u64 = 5_000;
    const FLUSH_PENDING_ROWS_THRESHOLD: usize = 4_096;

    pub(crate) fn new_with_shared(
        name: String,
        plan: Arc<OlapTableSinkPlan>,
        driver_id: i32,
        finalize_shared: Arc<OlapSinkFinalizeSharedState>,
    ) -> Self {
        let row_routing = plan.row_routing.clone();
        let write_targets = plan.write_targets.clone();
        let mut index_write_plans = if plan.index_write_plans.is_empty() {
            vec![SinkIndexWritePlan {
                index_id: -1,
                schema_id: -1,
                row_routing: row_routing.clone(),
                write_targets: write_targets.clone(),
                schema_slot_bindings: plan.schema_slot_bindings.clone(),
                op_slot_id: plan.op_slot_id,
                where_clause: None,
            }]
        } else {
            plan.index_write_plans.clone()
        };
        if index_write_plans.is_empty() {
            index_write_plans.push(SinkIndexWritePlan {
                index_id: -1,
                schema_id: -1,
                row_routing: row_routing.clone(),
                write_targets: write_targets.clone(),
                schema_slot_bindings: plan.schema_slot_bindings.clone(),
                op_slot_id: plan.op_slot_id,
                where_clause: None,
            });
        }
        let mut all_write_targets = HashMap::new();
        for index_plan in &index_write_plans {
            for (tablet_id, target) in &index_plan.write_targets {
                all_write_targets
                    .entry(*tablet_id)
                    .or_insert_with(|| target.clone());
            }
        }
        if all_write_targets.is_empty() {
            all_write_targets = write_targets.clone();
        }
        let tablet_commit_infos = plan.tablet_commit_infos.clone();
        Self {
            name,
            plan,
            finalize_shared,
            driver_id,
            file_seq: 0,
            next_random_hash: driver_id.max(0) as u32,
            pending_chunks: Vec::new(),
            pending_input_rows: 0,
            pending_input_bytes: 0,
            row_routing,
            write_targets,
            all_write_targets,
            index_write_plans,
            tablet_commit_infos,
            seen_partition_values: HashSet::new(),
            auto_partition_initialized: false,
            auto_partition_debug_logged: false,
            input_rows: 0,
            loaded_rows: 0,
            filtered_rows: 0,
            finished: false,
            written_tablets: HashSet::new(),
            dirty_partitions: HashSet::new(),
        }
    }

    fn flush_pending_chunks_once(&mut self, state: &RuntimeState) -> Result<(), String> {
        if !self.pending_chunks.is_empty() {
            self.flush_real_data(state)?;
        }
        Ok(())
    }

    fn flush_pending_chunks_with_retry(&mut self, state: &RuntimeState) -> Result<(), String> {
        let mut retry_times = 0_usize;
        loop {
            match self.flush_pending_chunks_once(state) {
                Ok(()) => return Ok(()),
                Err(err) => {
                    if retry_times >= Self::FLUSH_RETRY_MAX_TIMES
                        || !is_retryable_sink_write_error(&err)
                    {
                        return Err(err);
                    }
                    let backoff_ms = retry_backoff_with_jitter_ms(self.driver_id, retry_times);
                    debug!(
                        target: "novarocks::starrocks::sink",
                        table_id = self.plan.table_id,
                        txn_id = self.plan.txn_id,
                        driver_id = self.driver_id,
                        retry_times = retry_times + 1,
                        backoff_ms,
                        error = %err,
                        "OLAP_TABLE_SINK retry flush/finalize due to temporary write error"
                    );
                    std::thread::sleep(Duration::from_millis(backoff_ms));
                    retry_times = retry_times.saturating_add(1);
                }
            }
        }
    }

    fn should_flush_pending_chunks(&self, state: &RuntimeState) -> bool {
        let write_buffer_size = crate::common::config::olap_sink_write_buffer_size_bytes().max(1);
        let row_threshold = state.chunk_size().max(Self::FLUSH_PENDING_ROWS_THRESHOLD);
        self.pending_input_bytes >= write_buffer_size || self.pending_input_rows >= row_threshold
    }

    fn append_tablet_rowset(
        &mut self,
        tablet_id: i64,
        partition_id: i64,
        context: &TabletWriteContext,
        batch: &Chunk,
    ) -> Result<(), String> {
        let file_seq = self.file_seq;
        self.file_seq = self.file_seq.saturating_add(1);

        append_lake_txn_log_with_chunk_rowset(
            context,
            batch,
            self.plan.txn_id,
            self.driver_id,
            file_seq,
            self.plan.write_format,
            partition_id,
            Some(&self.plan.load_id),
        )?;
        self.written_tablets.insert(tablet_id);
        self.dirty_partitions.insert(partition_id);
        Ok(())
    }

    fn all_tablet_fail_infos(&self) -> Vec<TabletFailInfo> {
        self.tablet_commit_infos
            .iter()
            .map(|info| TabletFailInfo {
                tablet_id: info.tablet_id,
                backend_id: info.backend_id,
            })
            .collect::<Vec<_>>()
    }

    fn report_fail_infos_once(&self, state: &RuntimeState) {
        if !self.finalize_shared.mark_fail_infos_reported() {
            return;
        }
        let fail_infos = self.all_tablet_fail_infos();
        state.add_tablet_fail_infos(fail_infos.clone());
        debug!(
            target: "novarocks::sink_commit",
            table_id = self.plan.table_id,
            db_name = ?self.plan.db_name,
            table_name = ?self.plan.table_name,
            tablet_fail_len = fail_infos.len(),
            "OLAP_TABLE_SINK report tablet fail infos"
        );
    }

    fn report_commit_infos_once(
        &self,
        state: &RuntimeState,
        merged_written_tablets: &HashSet<i64>,
        merged_tablet_commit_infos: &[TabletCommitInfo],
    ) {
        if !self.finalize_shared.mark_commit_infos_reported() {
            return;
        }
        let tablet_commit_infos = merged_tablet_commit_infos
            .iter()
            .filter(|info| merged_written_tablets.contains(&info.tablet_id))
            .cloned()
            .collect::<Vec<_>>();
        state.add_tablet_commit_infos(tablet_commit_infos.clone());
        debug!(
            target: "novarocks::sink_commit",
            table_id = self.plan.table_id,
            db_name = ?self.plan.db_name,
            table_name = ?self.plan.table_name,
            driver_id = self.driver_id,
            input_rows_local = self.input_rows,
            tablet_commit_len = tablet_commit_infos.len(),
            merged_written_tablet_len = merged_written_tablets.len(),
            "OLAP_TABLE_SINK report tablet commit infos"
        );
    }

    fn finalize_dirty_partition_tablets(
        &self,
        write_targets: &HashMap<i64, TabletWriteTarget>,
        dirty_partitions: &HashSet<i64>,
        written_tablets: &mut HashSet<i64>,
    ) -> Result<(), String> {
        if dirty_partitions.is_empty() {
            return Ok(());
        }
        let mut empty_targets = Vec::new();
        for target in write_targets.values() {
            if !dirty_partitions.contains(&target.partition_id) {
                continue;
            }
            if written_tablets.contains(&target.tablet_id) {
                continue;
            }
            empty_targets.push((
                target.tablet_id,
                target.partition_id,
                target.context.clone(),
            ));
        }
        if empty_targets.is_empty() {
            info!(
                target: "novarocks::starrocks::sink",
                table_id = self.plan.table_id,
                txn_id = self.plan.txn_id,
                dirty_partition_count = dirty_partitions.len(),
                written_tablet_count = written_tablets.len(),
                "OLAP_TABLE_SINK no empty-rowset finalize targets"
            );
            return Ok(());
        }
        info!(
            target: "novarocks::starrocks::sink",
            table_id = self.plan.table_id,
            txn_id = self.plan.txn_id,
            dirty_partition_count = dirty_partitions.len(),
            written_tablet_count = written_tablets.len(),
            empty_target_count = empty_targets.len(),
            "OLAP_TABLE_SINK finalize empty rowsets for untouched tablets"
        );

        let mut appended_tablets = Vec::with_capacity(empty_targets.len());
        let mut first_error = None::<String>;
        std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(empty_targets.len());
            for (tablet_id, partition_id, context) in empty_targets {
                let load_id = self.plan.load_id;
                let txn_id = self.plan.txn_id;
                handles.push(scope.spawn(move || -> Result<i64, String> {
                    append_lake_txn_log_empty_rowset(
                        &context,
                        txn_id,
                        partition_id,
                        Some(&load_id),
                    )?;
                    Ok(tablet_id)
                }));
            }
            for handle in handles {
                match handle.join() {
                    Ok(Ok(tablet_id)) => appended_tablets.push(tablet_id),
                    Ok(Err(err)) => {
                        if first_error.is_none() {
                            first_error = Some(err);
                        }
                    }
                    Err(_) => {
                        if first_error.is_none() {
                            first_error = Some(
                                "OLAP_TABLE_SINK finalize empty rowset worker panicked".to_string(),
                            );
                        }
                    }
                }
            }
        });
        if let Some(err) = first_error {
            return Err(err);
        }
        written_tablets.extend(appended_tablets);
        Ok(())
    }

    fn ensure_auto_partitions_for_chunks(&mut self, chunks: &[Chunk]) -> Result<(), String> {
        let Some(auto_partition) = self.plan.auto_partition.clone() else {
            return Ok(());
        };
        let mut to_create = BTreeSet::<Vec<String>>::new();
        for chunk in chunks {
            if chunk.is_empty() {
                continue;
            }
            let (source_kind, chunk_values) = match &auto_partition.partition_key_source {
                PartitionKeySource::Expr(_) => {
                    let arrays =
                        build_partition_key_arrays(&auto_partition.partition_key_source, chunk)?;
                    if arrays.len() != auto_partition.partition_column_names.len() {
                        return Err(format!(
                            "OLAP_TABLE_SINK automatic partition expression count mismatch: exprs={} partition_columns={}",
                            arrays.len(),
                            auto_partition.partition_column_names.len()
                        ));
                    }
                    (
                        "expr",
                        collect_partition_values_from_arrays(&arrays, chunk.len())?,
                    )
                }
                PartitionKeySource::SlotRefs(_) => (
                    "slot_refs",
                    collect_partition_values_from_chunk(
                        chunk,
                        &auto_partition.partition_slot_ids,
                        &auto_partition.partition_column_names,
                    )?,
                ),
                PartitionKeySource::None if !auto_partition.partition_slot_ids.is_empty() => (
                    "slot_refs_fallback",
                    collect_partition_values_from_chunk(
                        chunk,
                        &auto_partition.partition_slot_ids,
                        &auto_partition.partition_column_names,
                    )?,
                ),
                PartitionKeySource::None => ("none", BTreeSet::new()),
            };
            if !self.auto_partition_debug_logged {
                let field_summary = chunk
                    .chunk_schema()
                    .slots()
                    .iter()
                    .enumerate()
                    .map(|(idx, slot)| {
                        format!(
                            "{idx}:{}(slot={})",
                            chunk.schema().field(idx).name(),
                            slot.slot_id()
                        )
                    })
                    .collect::<Vec<_>>();
                info!(
                    target: "novarocks::starrocks::sink",
                    table_id = self.plan.table_id,
                    txn_id = self.plan.txn_id,
                    partition_key_source = source_kind,
                    partition_columns = ?auto_partition.partition_column_names,
                    configured_partition_slot_ids = ?auto_partition.partition_slot_ids,
                    chunk_fields = ?field_summary,
                    "OLAP_TABLE_SINK auto partition chunk field layout"
                );
                self.auto_partition_debug_logged = true;
            }
            for values in chunk_values {
                if self.seen_partition_values.contains(&values) {
                    continue;
                }
                to_create.insert(values);
            }
        }
        if to_create.is_empty() {
            return Ok(());
        }

        for partition_values in to_create {
            info!(
                target: "novarocks::starrocks::sink",
                table_id = self.plan.table_id,
                txn_id = self.plan.txn_id,
                partition_values = ?partition_values,
                "OLAP_TABLE_SINK runtime createPartition for stream load"
            );
            let response = auto_partition
                .frontend_provider
                .create_automatic_partitions(&AutomaticPartitionRequest {
                    frontend: SinkFrontendAddress {
                        host: auto_partition.fe_addr.hostname.clone(),
                        port: auto_partition.fe_addr.port,
                    },
                    db_id: auto_partition.db_id,
                    table_id: auto_partition.table_id,
                    txn_id: auto_partition.txn_id,
                    is_temp: auto_partition.dynamic_overwrite,
                    partition_values: vec![partition_values.clone()],
                })
                .map(automatic_partition_result_from_port)
                .map_err(|e| format!("OLAP_TABLE_SINK runtime automatic partition failed: {e}"))?;
            self.ingest_auto_partition_response(&auto_partition, response)?;
            self.seen_partition_values.insert(partition_values);
        }
        Ok(())
    }

    fn ingest_auto_partition_response(
        &mut self,
        auto_partition: &crate::connector::starrocks::sink::factory::AutomaticPartitionPlan,
        response: CreatePartitionResult,
    ) -> Result<(), String> {
        let partitions = response.partitions;
        let tablets = response.tablets;

        let primary_index_id = self
            .index_write_plans
            .first()
            .map(|plan| plan.index_id)
            .ok_or_else(|| "OLAP_TABLE_SINK has empty index_write_plans".to_string())?;
        let write_index_ids = self
            .index_write_plans
            .iter()
            .map(|plan| plan.index_id)
            .collect::<HashSet<_>>();
        let mut new_partitions_by_index = HashMap::<i64, Vec<PartitionRoutingEntry>>::new();
        let mut tablet_to_partition = HashMap::<i64, i64>::new();
        let partition_key_len = partition_key_source_len(&auto_partition.partition_key_source);
        for partition in partitions {
            if partition.is_shadow {
                continue;
            }
            validate_partition_key_length(
                partition.partition_id,
                partition_key_len,
                partition.start_key.as_deref(),
                partition.end_key.as_deref(),
                &partition.in_keys,
            )?;
            if !partition.in_keys.is_empty() && partition.end_key.is_some() {
                return Err(format!(
                    "OLAP_TABLE_SINK createPartition returned mixed range/list metadata for partition {}",
                    partition.partition_id
                ));
            }

            let partition_index_ids = partition
                .indexes
                .iter()
                .map(|index| index.index_id)
                .collect::<HashSet<_>>();
            for index_id in &write_index_ids {
                if !partition_index_ids.contains(index_id) {
                    return Err(format!(
                        "OLAP_TABLE_SINK createPartition returned partition {} without write index {} (write_index_ids={:?})",
                        partition.partition_id, index_id, write_index_ids
                    ));
                }
            }

            for index in partition
                .indexes
                .iter()
                .filter(|index| write_index_ids.contains(&index.index_id))
            {
                if index.tablet_ids.is_empty() {
                    return Err(format!(
                        "OLAP_TABLE_SINK createPartition returned partition {} index {} with empty tablet_ids",
                        partition.partition_id, index.index_id
                    ));
                }
                for tablet_id in &index.tablet_ids {
                    tablet_to_partition.insert(*tablet_id, partition.partition_id);
                }
                let already_present = self
                    .index_write_plans
                    .iter()
                    .find(|plan| plan.index_id == index.index_id)
                    .is_some_and(|plan| {
                        plan.row_routing
                            .partitions
                            .iter()
                            .any(|entry| entry.partition_id == partition.partition_id)
                    });
                if already_present {
                    continue;
                }
                new_partitions_by_index
                    .entry(index.index_id)
                    .or_default()
                    .push(PartitionRoutingEntry {
                        partition_id: partition.partition_id,
                        tablet_ids: index.tablet_ids.clone(),
                        start_key: partition.start_key.clone(),
                        end_key: partition.end_key.clone(),
                        in_keys: partition.in_keys.clone(),
                    });
            }
        }
        let has_new_partitions = new_partitions_by_index
            .values()
            .any(|entries| !entries.is_empty());
        if has_new_partitions {
            if !self.auto_partition_initialized {
                for index_plan in &mut self.index_write_plans {
                    index_plan.row_routing.partitions.clear();
                    index_plan.row_routing.tablet_ids.clear();
                    index_plan.row_routing.tablet_idx_by_id.clear();
                }
            }
            for index_plan in &mut self.index_write_plans {
                if let Some(new_partitions) = new_partitions_by_index.remove(&index_plan.index_id) {
                    index_plan.row_routing.partitions.extend(new_partitions);
                }
                rebuild_auto_partition_routing(
                    &mut index_plan.row_routing,
                    &auto_partition.partition_key_source,
                    partition_key_len,
                )?;
            }
            self.auto_partition_initialized = true;
        }

        if self.auto_partition_initialized {
            self.row_routing = self
                .index_write_plans
                .iter()
                .find(|plan| plan.index_id == primary_index_id)
                .map(|plan| plan.row_routing.clone())
                .ok_or_else(|| {
                    format!(
                        "OLAP_TABLE_SINK auto partition lost primary index routing: index_id={primary_index_id}"
                    )
                })?;
        }

        for tablet in tablets {
            let Some(backend_id) = tablet.node_ids.first().copied() else {
                return Err(format!(
                    "OLAP_TABLE_SINK createPartition returned tablet {} with empty node_ids",
                    tablet.tablet_id
                ));
            };
            let exists = self.tablet_commit_infos.iter().any(|current| {
                current.tablet_id == tablet.tablet_id && current.backend_id == backend_id
            });
            if !exists {
                self.tablet_commit_infos.push(TabletCommitInfo {
                    tablet_id: tablet.tablet_id,
                    backend_id,
                });
            }
        }

        let mut new_tablets_by_index = HashMap::<i64, Vec<i64>>::new();
        let mut new_tablets = BTreeSet::<i64>::new();
        for index_plan in &self.index_write_plans {
            for tablet_id in &index_plan.row_routing.tablet_ids {
                if !index_plan.write_targets.contains_key(tablet_id) {
                    new_tablets_by_index
                        .entry(index_plan.index_id)
                        .or_default()
                        .push(*tablet_id);
                    new_tablets.insert(*tablet_id);
                }
            }
        }
        let new_tablets = new_tablets.into_iter().collect::<Vec<_>>();
        if !new_tablets.is_empty() {
            let refs = new_tablets
                .iter()
                .map(|tablet_id| LakeTabletPartitionRef {
                    tablet_id: *tablet_id,
                })
                .collect::<Vec<_>>();
            let path_map = resolve_tablet_paths_for_olap_sink(
                None,
                &self.plan.table_identity,
                &refs,
                self.plan.starlet_metadata_provider.as_deref(),
            )?;
            let shard_infos = starlet_shard_registry::select_infos(&new_tablets);
            for index_plan in &mut self.index_write_plans {
                let Some(index_new_tablets) = new_tablets_by_index.get(&index_plan.index_id) else {
                    continue;
                };
                let template = index_plan
                    .write_targets
                    .values()
                    .next()
                    .map(|target| target.context.clone())
                    .ok_or_else(|| {
                        format!(
                            "OLAP_TABLE_SINK cannot build runtime write target: index_id={} has no template context",
                            index_plan.index_id
                        )
                    })?;
                for tablet_id in index_new_tablets {
                    let partition_id = index_plan
                        .row_routing
                        .partitions
                        .iter()
                        .find(|entry| entry.tablet_ids.contains(tablet_id))
                        .map(|entry| entry.partition_id)
                        .or_else(|| tablet_to_partition.get(tablet_id).copied())
                        .ok_or_else(|| {
                            format!(
                                "OLAP_TABLE_SINK cannot resolve partition for runtime tablet {} in index_id={}",
                                tablet_id, index_plan.index_id
                            )
                        })?;
                    let tablet_root_path = path_map.get(tablet_id).ok_or_else(|| {
                        format!(
                            "OLAP_TABLE_SINK missing resolved storage path for runtime tablet {}",
                            tablet_id
                        )
                    })?;
                    let from_shard = shard_infos.get(tablet_id).and_then(|info| info.s3.clone());
                    let from_runtime = if from_shard.is_none() {
                        get_tablet_runtime(*tablet_id)
                            .ok()
                            .and_then(|entry| entry.s3_config.clone())
                    } else {
                        None
                    };
                    let s3_config = resolve_s3_for_sink_tablet(
                        *tablet_id,
                        tablet_root_path,
                        from_shard,
                        from_runtime,
                    )?;

                    let mut context = template.clone();
                    context.db_id = self.plan.db_id;
                    context.table_id = self.plan.table_id;
                    context.tablet_id = *tablet_id;
                    context.tablet_root_path = tablet_root_path.clone();
                    context.s3_config = s3_config;

                    index_plan.write_targets.insert(
                        *tablet_id,
                        TabletWriteTarget {
                            tablet_id: *tablet_id,
                            partition_id,
                            context,
                        },
                    );
                }
            }
        }
        self.write_targets = self
            .index_write_plans
            .iter()
            .find(|plan| plan.index_id == primary_index_id)
            .map(|plan| plan.write_targets.clone())
            .ok_or_else(|| {
                format!(
                    "OLAP_TABLE_SINK auto partition lost primary index write targets: index_id={primary_index_id}"
                )
            })?;
        self.all_write_targets.clear();
        for index_plan in &self.index_write_plans {
            self.all_write_targets.extend(
                index_plan
                    .write_targets
                    .iter()
                    .map(|(tablet_id, target)| (*tablet_id, target.clone())),
            );
        }
        Ok(())
    }

    fn prepare_sink_chunks(&self, chunks: &[Chunk]) -> Result<Vec<Chunk>, String> {
        chunks
            .iter()
            .map(|chunk| self.project_chunk_for_sink_output(chunk))
            .collect()
    }

    fn project_chunk_for_sink_output(&self, chunk: &Chunk) -> Result<Chunk, String> {
        let Some(projection) = self.plan.output_projection.as_ref() else {
            return Ok(chunk.clone());
        };
        if projection.expr_ids.is_empty() {
            return Ok(chunk.clone());
        }
        if projection.expr_ids.len() != projection.output_slot_ids.len()
            || projection.expr_ids.len() != projection.output_field_names.len()
        {
            return Err(format!(
                "OLAP_TABLE_SINK output projection metadata mismatch: expr_ids={} output_slot_ids={} output_field_names={}",
                projection.expr_ids.len(),
                projection.output_slot_ids.len(),
                projection.output_field_names.len()
            ));
        }

        let mut projected_columns = Vec::with_capacity(projection.expr_ids.len());
        let mut projected_fields = Vec::with_capacity(projection.expr_ids.len());
        let mut projected_slots = Vec::with_capacity(projection.expr_ids.len());
        for (idx, expr_id) in projection.expr_ids.iter().enumerate() {
            let projected = projection.arena.eval(*expr_id, chunk).map_err(|e| {
                format!(
                    "OLAP_TABLE_SINK evaluate output_exprs[{}] failed: {}",
                    idx, e
                )
            })?;
            let slot_id = projection.output_slot_ids[idx];
            let field_name = projection
                .output_field_names
                .get(idx)
                .cloned()
                .unwrap_or_else(|| format!("col_{idx}"));
            let field = Field::new(field_name, projected.data_type().clone(), true);
            projected_slots.push(ChunkSlotSchema::new_with_field(
                slot_id,
                field.clone(),
                None,
                None,
            ));
            projected_fields.push(field);
            projected_columns.push(projected);
        }
        let _ = projected_fields;
        Chunk::try_new_with_columns(
            Arc::new(ChunkSchema::try_new(projected_slots)?),
            projected_columns,
        )
        .map_err(|e| format!("OLAP_TABLE_SINK build projected output chunk failed: {e}"))
    }
}

fn rebuild_auto_partition_routing(
    row_routing: &mut RowRoutingPlan,
    partition_key_source: &PartitionKeySource,
    partition_key_len: usize,
) -> Result<(), String> {
    row_routing.partition_key_source = partition_key_source.clone();
    row_routing.partition_key_len = partition_key_len;

    let has_any_in_keys = row_routing
        .partitions
        .iter()
        .any(|entry| !entry.in_keys.is_empty());
    let has_any_range_bound = row_routing
        .partitions
        .iter()
        .any(|entry| entry.start_key.is_some() || entry.end_key.is_some());
    row_routing.partition_mode = if row_routing.partition_key_len == 0
        || (!has_any_in_keys && !has_any_range_bound)
    {
        PartitionMode::Unpartitioned
    } else if has_any_in_keys {
        if row_routing
            .partitions
            .iter()
            .any(|entry| entry.in_keys.is_empty())
        {
            return Err(
                "OLAP_TABLE_SINK mixed list/range partitions are not supported in auto partition routing"
                    .to_string(),
            );
        }
        PartitionMode::List
    } else {
        if row_routing
            .partitions
            .iter()
            .any(|entry| entry.end_key.is_none())
        {
            return Err(
                "OLAP_TABLE_SINK auto partition range routing has partition without end key"
                    .to_string(),
            );
        }
        PartitionMode::Range
    };

    let mut all_tablets = BTreeSet::new();
    for entry in &row_routing.partitions {
        for tablet_id in &entry.tablet_ids {
            all_tablets.insert(*tablet_id);
        }
    }
    row_routing.tablet_ids = all_tablets.into_iter().collect::<Vec<_>>();
    row_routing.tablet_idx_by_id.clear();
    for (idx, tablet_id) in row_routing.tablet_ids.iter().enumerate() {
        row_routing.tablet_idx_by_id.insert(*tablet_id, idx);
    }
    Ok(())
}

impl OlapTableSinkOperator {
    fn flush_real_data(&mut self, state: &RuntimeState) -> Result<(), String> {
        if self.index_write_plans.is_empty() || self.all_write_targets.is_empty() {
            return Err("OLAP_TABLE_SINK has empty write_targets".to_string());
        }

        let request_rows_threshold = state.chunk_size().max(1);
        let request_bytes_threshold =
            crate::common::config::olap_sink_max_tablet_write_chunk_bytes().max(1);
        let write_buffer_size = crate::common::config::olap_sink_write_buffer_size_bytes().max(1);

        let flush_start_file_seq = self.file_seq;
        let flush_start_random_hash = self.next_random_hash;
        let flush_start_pending_input_rows = self.pending_input_rows;
        let flush_start_pending_input_bytes = self.pending_input_bytes;
        let flush_start_loaded_rows = self.loaded_rows;
        let flush_start_filtered_rows = self.filtered_rows;
        let pending_chunks = std::mem::take(&mut self.pending_chunks);
        self.pending_input_rows = 0;
        self.pending_input_bytes = 0;
        let mut flush_tracking_logs = Vec::new();
        let mut flush_loaded_rows_delta = 0_i64;
        let mut flush_filtered_rows_delta = 0_i64;
        let flush_result: Result<(), String> = (|| {
            let sink_chunks = self.prepare_sink_chunks(&pending_chunks)?;
            self.ensure_auto_partitions_for_chunks(&sink_chunks)?;
            let index_write_plans = self.index_write_plans.clone();
            // Merge all sink chunks into one so that auto-increment IDs are
            // allocated in a single sequential batch, matching the original
            // INSERT row order.  StarRocks C++ BE receives all rows in a single
            // chunk; NovaRocks may receive per-row chunks from the FE.
            let sink_chunks = merge_sink_chunks_for_auto_increment(sink_chunks)?;
            let mut buffered_by_tablet = BTreeMap::<i64, TabletBufferedState>::new();
            for sink_chunk in &sink_chunks {
                if sink_chunk.is_empty() {
                    continue;
                }
                // Fill auto-increment NULLs before hash distribution so that IDs
                // are assigned in the original INSERT row order (matching C++ BE).
                let sink_chunk =
                    &fill_auto_increment_in_chunk_before_routing(sink_chunk, &self.plan)?;
                let chunk_random_hash_seed = self.next_random_hash;
                let mut first_plan_next_random_hash = chunk_random_hash_seed;
                for (plan_idx, index_plan) in index_write_plans.iter().enumerate() {
                    let mut plan_random_hash = chunk_random_hash_seed;
                    let routed = route_chunk_rows(
                        &index_plan.row_routing,
                        sink_chunk,
                        &mut plan_random_hash,
                    )?;
                    if plan_idx == 0 {
                        first_plan_next_random_hash = plan_random_hash;
                        flush_filtered_rows_delta = flush_filtered_rows_delta
                            .saturating_add(routed.rejections.len() as i64);
                        for rejection in &routed.rejections {
                            let row_idx = rejection.row_index as usize;
                            let log = match rejection.reason {
                                RowRejectReason::OutOfPartitionRanges => {
                                    format_partition_rejection(&sink_chunk.batch, row_idx)?
                                }
                            };
                            flush_tracking_logs.push(log);
                        }
                    }
                    for (tablet_idx, row_indices) in routed.per_tablet.into_iter().enumerate() {
                        if row_indices.is_empty() {
                            continue;
                        }
                        let tablet_id = *index_plan
                            .row_routing
                            .tablet_ids
                            .get(tablet_idx)
                            .ok_or_else(|| {
                                format!(
                                    "OLAP_TABLE_SINK routing produced invalid tablet index {} for index_id={}",
                                    tablet_idx, index_plan.index_id
                                )
                            })?;
                        let target =
                            index_plan.write_targets.get(&tablet_id).ok_or_else(|| {
                                format!(
                                    "OLAP_TABLE_SINK routing resolved unknown tablet target {} for index_id={}",
                                    tablet_id, index_plan.index_id
                                )
                            })?;
                        let routed_chunk = if row_indices.len() == sink_chunk.len() {
                            sink_chunk.clone()
                        } else {
                            take_chunk_rows(sink_chunk, &row_indices)?
                        };
                        let Some(routed_chunk) = apply_index_where_clause(
                            &routed_chunk,
                            &routed_chunk,
                            index_plan.index_id,
                            index_plan.where_clause.as_ref(),
                        )?
                        else {
                            continue;
                        };
                        let is_primary_keys_table = target.context.tablet_schema.keys_type
                            == Some(StarRocksKeysType::Primary);
                        let routed_chunk = match align_chunk_to_schema_slot_bindings(
                            &routed_chunk,
                            &index_plan.schema_slot_bindings,
                            index_plan.op_slot_id,
                        ) {
                            Ok(aligned_chunk)
                                if !is_primary_keys_table
                                    && aligned_chunk.batch.num_columns()
                                        < routed_chunk.batch.num_columns() =>
                            {
                                let batch_fields = debug_chunk_fields(&routed_chunk);
                                info!(
                                    target: "novarocks::sink",
                                    table_id = self.plan.table_id,
                                    index_id = index_plan.index_id,
                                    schema_id = index_plan.schema_id,
                                    tablet_id = target.tablet_id,
                                    original_columns = routed_chunk.batch.num_columns(),
                                    aligned_columns = aligned_chunk.batch.num_columns(),
                                    batch_fields = ?batch_fields,
                                    "OLAP_TABLE_SINK skip write-slot alignment for non-primary key batch because alignment dropped columns"
                                );
                                routed_chunk
                            }
                            Ok(aligned_chunk) => aligned_chunk,
                            Err(err) if !is_primary_keys_table => {
                                let batch_fields = debug_chunk_fields(&routed_chunk);
                                info!(
                                    target: "novarocks::sink",
                                    table_id = self.plan.table_id,
                                    index_id = index_plan.index_id,
                                    schema_id = index_plan.schema_id,
                                    tablet_id = target.tablet_id,
                                    error = %err,
                                    original_columns = routed_chunk.batch.num_columns(),
                                    batch_fields = ?batch_fields,
                                    "OLAP_TABLE_SINK skip write-slot alignment for non-primary key batch because alignment failed"
                                );
                                routed_chunk
                            }
                            Err(err) => return Err(err),
                        };
                        let filtered_batch = filter_rows_for_tablet_schema(
                            &routed_chunk.batch,
                            &target.context.tablet_schema,
                            Some(&target.context.partial_update.auto_increment),
                            target.context.table_id,
                        )?;
                        if plan_idx == 0 {
                            flush_loaded_rows_delta = flush_loaded_rows_delta
                                .saturating_add(filtered_batch.batch.num_rows() as i64);
                            flush_filtered_rows_delta = flush_filtered_rows_delta
                                .saturating_add(filtered_batch.rejected_rows as i64);
                            flush_tracking_logs
                                .extend(filtered_batch.tracking_logs.iter().cloned());
                        }
                        let routed_chunk = {
                            let filtered_schema = routed_chunk.chunk_schema_ref();
                            Chunk::try_new_with_chunk_schema(filtered_batch.batch, filtered_schema)?
                        };
                        if routed_chunk.is_empty() {
                            continue;
                        }
                        buffered_by_tablet.entry(tablet_id).or_insert_with(|| {
                            TabletBufferedState::new(target.partition_id, target.context.clone())
                        });
                        let buffer = buffered_by_tablet.get_mut(&tablet_id).ok_or_else(|| {
                            format!(
                                "OLAP_TABLE_SINK tablet buffer missing after insert: tablet_id={}",
                                tablet_id
                            )
                        })?;
                        if buffer.partition_id != target.partition_id {
                            return Err(format!(
                                "OLAP_TABLE_SINK buffered partition mismatch for tablet {}: buffered_partition_id={} target_partition_id={}",
                                tablet_id, buffer.partition_id, target.partition_id
                            ));
                        }
                        buffer.push_request_batch(routed_chunk);
                        if buffer.should_seal_request_batch(
                            request_rows_threshold,
                            request_bytes_threshold,
                        ) && let Some(request_batch) = buffer.take_request_batch()?
                        {
                            buffer.push_memtable_batch(request_batch);
                        }
                        if buffer.should_flush_memtable(write_buffer_size)
                            && let Some(memtable_batch) = buffer.take_memtable_batch()?
                        {
                            self.append_tablet_rowset(
                                tablet_id,
                                buffer.partition_id,
                                &buffer.context,
                                &memtable_batch,
                            )?;
                        }
                    }
                }
                self.next_random_hash = first_plan_next_random_hash;
            }
            for (tablet_id, buffer) in &mut buffered_by_tablet {
                if let Some(request_batch) = buffer.take_request_batch()? {
                    buffer.push_memtable_batch(request_batch);
                }
                if let Some(memtable_batch) = buffer.take_memtable_batch()? {
                    self.append_tablet_rowset(
                        *tablet_id,
                        buffer.partition_id,
                        &buffer.context,
                        &memtable_batch,
                    )?;
                }
            }
            Ok(())
        })();

        if let Err(err) = flush_result {
            self.pending_chunks = pending_chunks;
            self.pending_input_rows = flush_start_pending_input_rows;
            self.pending_input_bytes = flush_start_pending_input_bytes;
            self.file_seq = flush_start_file_seq;
            self.next_random_hash = flush_start_random_hash;
            self.loaded_rows = flush_start_loaded_rows;
            self.filtered_rows = flush_start_filtered_rows;
            return Err(err);
        }

        self.loaded_rows = flush_start_loaded_rows.saturating_add(flush_loaded_rows_delta);
        self.filtered_rows = flush_start_filtered_rows.saturating_add(flush_filtered_rows_delta);
        let _ = flush_tracking_logs;

        Ok(())
    }
}

fn is_retryable_sink_write_error(err: &str) -> bool {
    let lower = err.to_ascii_lowercase();
    if lower.contains("temporary")
        || lower.contains("timeout")
        || lower.contains("timed out")
        || lower.contains("connection reset")
        || lower.contains("connection refused")
        || lower.contains("connection aborted")
        || lower.contains("broken pipe")
    {
        return true;
    }
    lower.contains("status: 5")
        || lower.contains("status=5")
        || lower.contains("status code: 5")
        || lower.contains("http status 5")
}

fn retry_backoff_with_jitter_ms(driver_id: i32, retry_times: usize) -> u64 {
    let exp = retry_times.min(6) as u32;
    let factor = 1_u64.checked_shl(exp).unwrap_or(u64::MAX);
    let base = OlapTableSinkOperator::FLUSH_RETRY_BASE_BACKOFF_MS
        .saturating_mul(factor)
        .min(OlapTableSinkOperator::FLUSH_RETRY_MAX_BACKOFF_MS);
    let seed = u64::from(driver_id.max(0) as u32)
        .wrapping_mul(131)
        .wrapping_add((retry_times as u64).wrapping_mul(17));
    let jitter = seed % 100;
    base.saturating_add(jitter)
}

impl Operator for OlapTableSinkOperator {
    fn name(&self) -> &str {
        &self.name
    }

    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }

    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }

    fn is_finished(&self) -> bool {
        self.finished
    }
}

impl ProcessorOperator for OlapTableSinkOperator {
    fn need_input(&self) -> bool {
        !self.finished
    }

    fn has_output(&self) -> bool {
        false
    }

    fn push_chunk(&mut self, state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
        if self.finished {
            return Ok(());
        }
        if chunk.is_empty() {
            return Ok(());
        }
        self.input_rows = self.input_rows.saturating_add(chunk.len() as i64);
        self.pending_input_rows = self.pending_input_rows.saturating_add(chunk.len());
        self.pending_input_bytes = self
            .pending_input_bytes
            .saturating_add(chunk.estimated_bytes());
        self.pending_chunks.push(chunk);
        if self.should_flush_pending_chunks(state) {
            self.flush_pending_chunks_with_retry(state)?;
        }
        Ok(())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        Ok(None)
    }

    fn set_finishing(&mut self, state: &RuntimeState) -> Result<(), String> {
        let flush_result = self.flush_pending_chunks_with_retry(state);
        if let Err(err) = &flush_result {
            self.finalize_shared.record_error(err.clone());
        } else if self.loaded_rows > 0 || self.filtered_rows > 0 {
            // FE derives loaded rows from reportExecStatus load counters.
            state.add_sink_load_stats(self.loaded_rows, 0, self.filtered_rows);
        }

        self.finalize_shared.record_progress(
            &self.all_write_targets,
            &self.written_tablets,
            &self.dirty_partitions,
            &self.tablet_commit_infos,
        );
        let is_last_driver = self.finalize_shared.arrive_and_is_last();

        let result = if is_last_driver {
            if let Some(err) = self.finalize_shared.first_error() {
                Err(err)
            } else {
                let (
                    merged_write_targets,
                    dirty_partitions,
                    mut merged_written_tablets,
                    merged_tablet_commit_infos,
                ) = self.finalize_shared.snapshot_progress();
                let mut merged_written_tablet_ids =
                    merged_written_tablets.iter().copied().collect::<Vec<_>>();
                merged_written_tablet_ids.sort_unstable();
                info!(
                    target: "novarocks::starrocks::sink",
                    table_id = self.plan.table_id,
                    txn_id = self.plan.txn_id,
                    merged_write_target_count = merged_write_targets.len(),
                    dirty_partition_count = dirty_partitions.len(),
                    merged_written_tablet_count = merged_written_tablets.len(),
                    merged_written_tablet_ids = ?merged_written_tablet_ids,
                    "OLAP_TABLE_SINK finalizing sink progress"
                );
                self.finalize_dirty_partition_tablets(
                    &merged_write_targets,
                    &dirty_partitions,
                    &mut merged_written_tablets,
                )?;
                self.report_commit_infos_once(
                    state,
                    &merged_written_tablets,
                    &merged_tablet_commit_infos,
                );
                Ok(())
            }
        } else if let Err(err) = flush_result {
            Err(err)
        } else if let Some(err) = self.finalize_shared.first_error() {
            Err(err)
        } else {
            Ok(())
        };

        self.finished = true;
        if let Err(err) = result {
            self.report_fail_infos_once(state);
            return Err(err);
        }
        Ok(())
    }
}

fn collect_partition_values_from_chunk(
    chunk: &Chunk,
    slot_ids: &[SlotId],
    column_names: &[String],
) -> Result<BTreeSet<Vec<String>>, String> {
    if slot_ids.is_empty() {
        return Ok(BTreeSet::new());
    }
    if slot_ids.len() != column_names.len() {
        return Err(format!(
            "OLAP_TABLE_SINK partition slot/name count mismatch: slot_ids={} column_names={}",
            slot_ids.len(),
            column_names.len()
        ));
    }

    let mut slot_to_index = HashMap::<SlotId, usize>::new();
    let mut name_to_index = HashMap::<String, usize>::new();
    let schema = chunk.batch.schema();
    for (idx, slot_schema) in chunk.chunk_schema().slots().iter().enumerate() {
        slot_to_index.entry(slot_schema.slot_id()).or_insert(idx);
        let field = schema.field(idx);
        let normalized_name = field.name().trim().to_ascii_lowercase();
        if !normalized_name.is_empty() {
            name_to_index.entry(normalized_name).or_insert(idx);
        }
    }

    let mut arrays = Vec::<ArrayRef>::with_capacity(slot_ids.len());
    for (slot_id, column_name) in slot_ids.iter().zip(column_names.iter()) {
        let normalized_name = column_name.trim().to_ascii_lowercase();
        let by_slot = slot_to_index.get(slot_id).copied();
        let by_name = name_to_index.get(&normalized_name).copied();
        let selected_idx = match (by_slot, by_name) {
            (Some(slot_idx), Some(name_idx)) => {
                if slot_idx == name_idx {
                    slot_idx
                } else {
                    name_idx
                }
            }
            (Some(slot_idx), None) => slot_idx,
            (None, Some(name_idx)) => name_idx,
            (None, None) => {
                return Err(format!(
                    "OLAP_TABLE_SINK partition column '{}' is not available in chunk by slot_id={} or field name",
                    column_name, slot_id
                ));
            }
        };
        arrays.push(
            chunk
                .batch
                .columns()
                .get(selected_idx)
                .cloned()
                .ok_or_else(|| {
                    format!(
                        "OLAP_TABLE_SINK partition column '{}' resolved invalid column index {}",
                        column_name, selected_idx
                    )
                })?,
        );
    }
    let mut out = BTreeSet::<Vec<String>>::new();
    for row in 0..chunk.len() {
        let mut values = Vec::with_capacity(arrays.len());
        for array in &arrays {
            if array.is_null(row) {
                values.push(STARROCKS_DEFAULT_PARTITION_VALUE.to_string());
            } else {
                values.push(partition_scalar_value_to_string(array.as_ref(), row)?);
            }
        }
        out.insert(values);
    }
    Ok(out)
}

fn collect_partition_values_from_arrays(
    arrays: &[ArrayRef],
    row_count: usize,
) -> Result<BTreeSet<Vec<String>>, String> {
    if arrays.is_empty() {
        return Ok(BTreeSet::new());
    }
    if arrays.iter().any(|array| array.len() < row_count) {
        return Err(format!(
            "OLAP_TABLE_SINK partition arrays are shorter than row_count: row_count={} min_array_len={}",
            row_count,
            arrays.iter().map(|array| array.len()).min().unwrap_or(0)
        ));
    }

    let mut out = BTreeSet::<Vec<String>>::new();
    for row in 0..row_count {
        let mut values = Vec::with_capacity(arrays.len());
        for array in arrays {
            if array.is_null(row) {
                // For expression-based range partitions, FE expects the literal string "NULL"
                // (handled by AnalyzerUtils.getAddPartitionClauseForRangePartition which maps
                // "NULL" → "0000-01-01"). This matches StarRocks C++ BE behavior where
                // NullableColumn::raw_item_value returns "NULL" via debug_item.
                // Note: list partitions use STARROCKS_DEFAULT_PARTITION_VALUE instead,
                // which is handled by PartitionValue.getValue() in a different FE code path.
                values.push("NULL".to_string());
            } else {
                values.push(partition_scalar_value_to_string(array.as_ref(), row)?);
            }
        }
        out.insert(values);
    }
    Ok(out)
}

fn partition_scalar_value_to_string(array: &dyn Array, row: usize) -> Result<String, String> {
    match array.data_type() {
        DataType::Utf8 => {
            let typed = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "downcast StringArray failed".to_string())?;
            Ok(normalize_text_partition_value(typed.value(row)))
        }
        DataType::LargeUtf8 => {
            let typed = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| "downcast LargeStringArray failed".to_string())?;
            Ok(normalize_text_partition_value(typed.value(row)))
        }
        DataType::Int8 => {
            let typed = array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| "downcast Int8Array failed".to_string())?;
            Ok(typed.value(row).to_string())
        }
        DataType::Int16 => {
            let typed = array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| "downcast Int16Array failed".to_string())?;
            Ok(typed.value(row).to_string())
        }
        DataType::Int32 => {
            let typed = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "downcast Int32Array failed".to_string())?;
            Ok(typed.value(row).to_string())
        }
        DataType::Int64 => {
            let typed = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "downcast Int64Array failed".to_string())?;
            Ok(typed.value(row).to_string())
        }
        DataType::UInt8 => {
            let typed = array
                .as_any()
                .downcast_ref::<UInt8Array>()
                .ok_or_else(|| "downcast UInt8Array failed".to_string())?;
            Ok(typed.value(row).to_string())
        }
        DataType::UInt16 => {
            let typed = array
                .as_any()
                .downcast_ref::<UInt16Array>()
                .ok_or_else(|| "downcast UInt16Array failed".to_string())?;
            Ok(typed.value(row).to_string())
        }
        DataType::UInt32 => {
            let typed = array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| "downcast UInt32Array failed".to_string())?;
            Ok(typed.value(row).to_string())
        }
        DataType::UInt64 => {
            let typed = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| "downcast UInt64Array failed".to_string())?;
            Ok(typed.value(row).to_string())
        }
        DataType::Boolean => {
            let typed = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| "downcast BooleanArray failed".to_string())?;
            Ok(if typed.value(row) { "TRUE" } else { "FALSE" }.to_string())
        }
        DataType::Date32 => {
            let typed = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| "downcast Date32Array failed".to_string())?;
            format_date32_for_partition_value(typed.value(row))
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let typed = array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .ok_or_else(|| "downcast TimestampSecondArray failed".to_string())?;
            format_timestamp_micros_for_partition_value(typed.value(row).saturating_mul(1_000_000))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let typed = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| "downcast TimestampMillisecondArray failed".to_string())?;
            format_timestamp_micros_for_partition_value(typed.value(row).saturating_mul(1_000))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let typed = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| "downcast TimestampMicrosecondArray failed".to_string())?;
            format_timestamp_micros_for_partition_value(typed.value(row))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let typed = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| "downcast TimestampNanosecondArray failed".to_string())?;
            format_timestamp_micros_for_partition_value(typed.value(row) / 1_000)
        }
        DataType::Decimal128(_, scale) => {
            let typed = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| "downcast Decimal128Array failed".to_string())?;
            format_decimal_for_partition_value(typed.value(row), *scale)
        }
        other => Err(format!(
            "unsupported automatic partition value data type: {:?}",
            other
        )),
    }
}

fn format_date32_for_partition_value(days_since_epoch: i32) -> Result<String, String> {
    let days_from_ce = 719_163_i32
        .checked_add(days_since_epoch)
        .ok_or_else(|| format!("date32 day overflow: {days_since_epoch}"))?;
    let date = NaiveDate::from_num_days_from_ce_opt(days_from_ce)
        .ok_or_else(|| format!("invalid date32 value: {days_since_epoch}"))?;
    Ok(date.format("%Y-%m-%d").to_string())
}

fn format_timestamp_micros_for_partition_value(micros_since_epoch: i64) -> Result<String, String> {
    let secs = micros_since_epoch.div_euclid(1_000_000);
    let micros = micros_since_epoch.rem_euclid(1_000_000) as u32;
    let dt = chrono::DateTime::from_timestamp(secs, micros.saturating_mul(1_000))
        .ok_or_else(|| format!("invalid timestamp micros: {micros_since_epoch}"))?;
    let base = dt.naive_utc().format("%Y-%m-%d %H:%M:%S").to_string();
    if micros == 0 {
        return Ok(base);
    }
    // Keep fixed-width micros to avoid FE datetime format probe failures
    // on values with short fractions (for example `...13.44`).
    Ok(format!("{base}.{micros:06}"))
}

fn normalize_text_partition_value(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return value.to_string();
    }

    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(trimmed) {
        return format_naive_datetime_partition_value(dt.naive_utc());
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%dT%H:%M:%S%.f") {
        return format_naive_datetime_partition_value(dt);
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%dT%H:%M:%S") {
        return format_naive_datetime_partition_value(dt);
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%d %H:%M:%S%.f") {
        return format_naive_datetime_partition_value(dt);
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%d %H:%M:%S") {
        return format_naive_datetime_partition_value(dt);
    }
    if let Ok(date) = NaiveDate::parse_from_str(trimmed, "%Y-%m-%d") {
        return date.format("%Y-%m-%d").to_string();
    }
    value.to_string()
}

fn format_naive_datetime_partition_value(dt: NaiveDateTime) -> String {
    let base = dt.format("%Y-%m-%d %H:%M:%S").to_string();
    let micros = dt.and_utc().timestamp_subsec_micros();
    if micros == 0 {
        base
    } else {
        // Keep a fixed-width fractional part to avoid FE datetime format probe failures
        // on strings like `...13.44`.
        format!("{base}.{micros:06}")
    }
}

fn format_decimal_for_partition_value(value: i128, scale: i8) -> Result<String, String> {
    if scale <= 0 {
        let zeros_i16 = i16::from(scale)
            .checked_neg()
            .ok_or_else(|| format!("invalid decimal scale: {scale}"))?;
        let zeros =
            usize::try_from(zeros_i16).map_err(|_| format!("invalid decimal scale: {scale}"))?;
        if zeros == 0 {
            return Ok(value.to_string());
        }
        return Ok(format!("{value}{}", "0".repeat(zeros)));
    }

    let negative = value < 0;
    let digits = value.unsigned_abs().to_string();
    let scale_usize =
        usize::try_from(scale).map_err(|_| format!("invalid decimal scale: {scale}"))?;
    let rendered = if digits.len() > scale_usize {
        let split = digits.len() - scale_usize;
        format!("{}.{}", &digits[..split], &digits[split..])
    } else {
        let mut s = String::with_capacity(scale_usize + 2);
        s.push_str("0.");
        for _ in 0..(scale_usize - digits.len()) {
            s.push('0');
        }
        s.push_str(&digits);
        s
    };
    if negative {
        Ok(format!("-{rendered}"))
    } else {
        Ok(rendered)
    }
}

fn take_batch_rows(batch: &RecordBatch, row_indices: &[u32]) -> Result<RecordBatch, String> {
    let index_array = UInt32Array::from(row_indices.to_vec());
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (col_idx, array) in batch.columns().iter().enumerate() {
        let taken = take(array.as_ref(), &index_array, None).map_err(|e| {
            format!(
                "OLAP_TABLE_SINK take rows for routed batch failed: column_index={}, error={}",
                col_idx, e
            )
        })?;
        columns.push(taken);
    }
    RecordBatch::try_new(batch.schema(), columns).map_err(|e| {
        format!(
            "OLAP_TABLE_SINK build routed record batch failed: rows={}, error={}",
            row_indices.len(),
            e
        )
    })
}

fn take_chunk_rows(chunk: &Chunk, row_indices: &[u32]) -> Result<Chunk, String> {
    let batch = take_batch_rows(&chunk.batch, row_indices)?;
    Chunk::try_new_with_chunk_schema(batch, chunk.chunk_schema_ref())
}

fn debug_chunk_fields(chunk: &Chunk) -> Vec<String> {
    chunk
        .chunk_schema()
        .slots()
        .iter()
        .enumerate()
        .map(|(idx, slot)| format!("{idx}:{}(slot={})", slot.name(), slot.slot_id()))
        .collect::<Vec<_>>()
}

fn align_chunk_to_schema_slot_bindings(
    chunk: &Chunk,
    schema_slot_bindings: &[Option<SlotId>],
    op_slot_id: Option<SlotId>,
) -> Result<Chunk, String> {
    if schema_slot_bindings.is_empty() && op_slot_id.is_none() {
        return Ok(chunk.clone());
    }

    let mut slot_to_data_index = HashMap::new();
    let mut slot_to_op_index = HashMap::new();
    for (idx, slot) in chunk.chunk_schema().slots().iter().enumerate() {
        if slot.name() == LOAD_OP_COLUMN {
            slot_to_op_index.entry(slot.slot_id()).or_insert(idx);
        } else {
            slot_to_data_index.entry(slot.slot_id()).or_insert(idx);
        }
    }

    let mut aligned_columns = Vec::new();
    let mut aligned_fields = Vec::new();
    let mut aligned_slots = Vec::new();
    let mut aligned_data_indexes = Vec::new();
    let mut aligned_slot_ids = HashSet::new();
    let mut next_synthetic_slot_id = u32::MAX;
    let mut assign_slot_id = |preferred: SlotId| {
        if aligned_slot_ids.insert(preferred) {
            return preferred;
        }
        loop {
            let synthetic = SlotId::new(next_synthetic_slot_id);
            next_synthetic_slot_id = next_synthetic_slot_id.saturating_sub(1);
            if aligned_slot_ids.insert(synthetic) {
                return synthetic;
            }
        }
    };
    for slot_id in schema_slot_bindings.iter().copied().flatten() {
        let Some(idx) = slot_to_data_index.get(&slot_id).copied() else {
            continue;
        };
        // Keep duplicated schema bindings (e.g. INSERT ... SELECT k1, k1, v)
        // so non-primary key writes preserve full target column count. ChunkSchema
        // still requires unique slot ids, so duplicated copies get synthetic ids.
        aligned_data_indexes.push(idx);
        let slot = &chunk.chunk_schema().slots()[idx];
        let aligned_slot_id = assign_slot_id(slot.slot_id());
        aligned_columns.push(chunk.batch.column(idx).clone());
        aligned_fields.push(chunk.batch.schema().field(idx).as_ref().clone());
        aligned_slots.push(slot.with_field_and_slot_id(
            aligned_slot_id,
            chunk.batch.schema().field(idx).as_ref().clone(),
        )?);
    }
    let resolved_op_index = if let Some(slot_id) = op_slot_id {
        if let Some(op_idx) = slot_to_op_index.get(&slot_id).copied() {
            Some(op_idx)
        } else {
            // Some plans materialize output fields as generic names (for example
            // "col_0"), but still carry the authoritative slot id for __op.
            // Recover __op from slot id only when this slot is not used as a
            // regular target column in schema bindings.
            let slot_bound_as_data = schema_slot_bindings
                .iter()
                .flatten()
                .copied()
                .any(|bound| bound == slot_id);
            if slot_bound_as_data {
                None
            } else {
                slot_to_data_index.get(&slot_id).copied()
            }
        }
    } else {
        chunk
            .chunk_schema()
            .slots()
            .iter()
            .enumerate()
            .find_map(|(idx, slot)| (slot.name() == LOAD_OP_COLUMN).then_some(idx))
    };
    if let Some(op_idx) = resolved_op_index {
        // Keep schema-slot aligned output order (including duplicated slots),
        // but append __op at most once to preserve control-column semantics.
        let op_already_selected = aligned_data_indexes.contains(&op_idx);
        if !op_already_selected {
            let op_slot = &chunk.chunk_schema().slots()[op_idx];
            let aligned_slot_id = assign_slot_id(op_slot.slot_id());
            aligned_columns.push(chunk.batch.column(op_idx).clone());
            let op_field = chunk.batch.schema().field(op_idx).as_ref().clone();
            let renamed_op_field = op_field.with_name(LOAD_OP_COLUMN.to_string());
            aligned_fields.push(renamed_op_field.clone());
            aligned_slots.push(op_slot.with_field_and_slot_id(aligned_slot_id, renamed_op_field)?);
        }
    }
    if aligned_columns.is_empty() {
        return Err(format!(
            "OLAP_TABLE_SINK cannot align routed batch with schema slot bindings: batch_columns={} schema_slot_bindings={} op_slot_id={:?}",
            chunk.batch.num_columns(),
            schema_slot_bindings.len(),
            op_slot_id
        ));
    }

    let aligned_schema = Arc::new(Schema::new(aligned_fields));
    let aligned_batch = RecordBatch::try_new(aligned_schema, aligned_columns)
        .map_err(|e| format!("OLAP_TABLE_SINK build write-slot aligned batch failed: {e}"))?;
    Chunk::try_new_with_chunk_schema(
        aligned_batch,
        Arc::new(ChunkSchema::try_new(aligned_slots)?),
    )
}
