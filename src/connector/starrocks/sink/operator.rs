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
use arrow::compute::{concat_batches, take};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{NaiveDate, NaiveDateTime};

use crate::common::ids::SlotId;
use crate::connector::starrocks::fe_v2_meta::{
    LakeTabletPartitionRef, resolve_tablet_paths_for_olap_sink,
};
use crate::connector::starrocks::lake::context::get_tablet_runtime;
use crate::connector::starrocks::lake::txn_log::append_lake_txn_log_empty_rowset;
use crate::connector::starrocks::lake::{TabletWriteContext, append_lake_txn_log_with_rowset};
use crate::connector::starrocks::sink::factory::{
    OlapTableSinkPlan, STARROCKS_DEFAULT_PARTITION_VALUE, SinkIndexWritePlan, TabletWriteTarget,
    create_automatic_partitions,
};
use crate::connector::starrocks::sink::partition_key::{
    PartitionKeySource, PartitionMode, PartitionRoutingEntry, build_partition_key_arrays,
    parse_partition_boundary_key, parse_partition_in_keys, partition_key_source_len,
    validate_partition_key_length,
};
use crate::connector::starrocks::sink::routing::{RowRoutingPlan, route_chunk_rows};
use crate::exec::chunk::{Chunk, field_slot_id, field_with_slot_id};
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::fs::path::{ScanPathScheme, classify_scan_paths};
use crate::novarocks_logging::{debug, info};
use crate::runtime::runtime_state::RuntimeState;
use crate::runtime::starlet_shard_registry;
use crate::service::grpc_client::proto::starrocks::KeysType;
use crate::types;

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
    tablet_commit_infos: Vec<types::TTabletCommitInfo>,
    seen_partition_values: HashSet<Vec<String>>,
    auto_partition_initialized: bool,
    auto_partition_debug_logged: bool,
    input_rows: i64,
    finished: bool,
    written_tablets: HashSet<i64>,
    dirty_partitions: HashSet<i64>,
    #[cfg(test)]
    append_call_count: usize,
    #[cfg(test)]
    fail_once_at_append_call: Option<usize>,
}

#[derive(Default)]
pub(crate) struct OlapSinkFinalizeSharedState {
    registered_drivers: AtomicUsize,
    remaining_drivers: AtomicUsize,
    dirty_partitions: Mutex<HashSet<i64>>,
    written_tablets: Mutex<HashSet<i64>>,
    tablet_commit_infos: Mutex<Vec<types::TTabletCommitInfo>>,
    first_error: Mutex<Option<String>>,
    fail_infos_reported: AtomicBool,
    commit_infos_reported: AtomicBool,
}

impl OlapSinkFinalizeSharedState {
    #[cfg(test)]
    pub(crate) fn new_single_driver() -> Self {
        let state = Self::default();
        state.registered_drivers.store(1, Ordering::Release);
        state.remaining_drivers.store(1, Ordering::Release);
        state
    }

    pub(crate) fn register_driver(&self) {
        self.registered_drivers.fetch_add(1, Ordering::AcqRel);
        self.remaining_drivers.fetch_add(1, Ordering::AcqRel);
    }

    fn record_progress(
        &self,
        written_tablets: &HashSet<i64>,
        dirty_partitions: &HashSet<i64>,
        tablet_commit_infos: &[types::TTabletCommitInfo],
    ) {
        if !written_tablets.is_empty() {
            if let Ok(mut guard) = self.written_tablets.lock() {
                guard.extend(written_tablets.iter().copied());
            }
        }
        if !dirty_partitions.is_empty() {
            if let Ok(mut guard) = self.dirty_partitions.lock() {
                guard.extend(dirty_partitions.iter().copied());
            }
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

    fn snapshot_progress(&self) -> (HashSet<i64>, HashSet<i64>, Vec<types::TTabletCommitInfo>) {
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
        (dirty_partitions, written_tablets, tablet_commit_infos)
    }

    fn record_error(&self, err: String) {
        if let Ok(mut guard) = self.first_error.lock() {
            if guard.is_none() {
                *guard = Some(err);
            }
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
    request_batches: Vec<RecordBatch>,
    request_rows: usize,
    request_bytes: usize,
    memtable_batches: Vec<RecordBatch>,
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

    fn push_request_batch(&mut self, batch: RecordBatch) {
        self.request_rows = self.request_rows.saturating_add(batch.num_rows());
        self.request_bytes = self
            .request_bytes
            .saturating_add(batch.get_array_memory_size());
        self.request_batches.push(batch);
    }

    fn should_seal_request_batch(&self, row_threshold: usize, bytes_threshold: usize) -> bool {
        self.request_rows >= row_threshold || self.request_bytes >= bytes_threshold
    }

    fn take_request_batch(&mut self) -> Result<Option<RecordBatch>, String> {
        let Some(batch) = concat_buffered_batches(&mut self.request_batches)? else {
            return Ok(None);
        };
        self.request_rows = 0;
        self.request_bytes = 0;
        Ok(Some(batch))
    }

    fn push_memtable_batch(&mut self, batch: RecordBatch) {
        self.memtable_rows = self.memtable_rows.saturating_add(batch.num_rows());
        self.memtable_bytes = self
            .memtable_bytes
            .saturating_add(batch.get_array_memory_size());
        self.memtable_batches.push(batch);
    }

    fn should_flush_memtable(&self, bytes_threshold: usize) -> bool {
        self.memtable_bytes >= bytes_threshold
    }

    fn take_memtable_batch(&mut self) -> Result<Option<RecordBatch>, String> {
        let Some(batch) = concat_buffered_batches(&mut self.memtable_batches)? else {
            return Ok(None);
        };
        self.memtable_rows = 0;
        self.memtable_bytes = 0;
        Ok(Some(batch))
    }
}

fn concat_buffered_batches(batches: &mut Vec<RecordBatch>) -> Result<Option<RecordBatch>, String> {
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
    let merged = concat_batches(&schema, batches.as_slice())
        .map_err(|e| format!("OLAP_TABLE_SINK concat buffered batches failed: {e}"))?;
    batches.clear();
    Ok(Some(merged))
}

impl OlapTableSinkOperator {
    const FLUSH_RETRY_MAX_TIMES: usize = 3;
    const FLUSH_RETRY_BASE_BACKOFF_MS: u64 = 200;
    const FLUSH_RETRY_MAX_BACKOFF_MS: u64 = 5_000;
    const FLUSH_PENDING_ROWS_THRESHOLD: usize = 4_096;

    #[cfg(test)]
    pub(crate) fn new(name: String, plan: Arc<OlapTableSinkPlan>, driver_id: i32) -> Self {
        Self::new_with_shared(
            name,
            plan,
            driver_id,
            Arc::new(OlapSinkFinalizeSharedState::new_single_driver()),
        )
    }

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
            finished: false,
            written_tablets: HashSet::new(),
            dirty_partitions: HashSet::new(),
            #[cfg(test)]
            append_call_count: 0,
            #[cfg(test)]
            fail_once_at_append_call: None,
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
        batch: &RecordBatch,
    ) -> Result<(), String> {
        let file_seq = self.file_seq;
        self.file_seq = self.file_seq.saturating_add(1);

        #[cfg(test)]
        {
            self.append_call_count = self.append_call_count.saturating_add(1);
            if self.fail_once_at_append_call == Some(self.append_call_count) {
                self.fail_once_at_append_call = None;
                return Err(format!(
                    "injected temporary append failure at call {}",
                    self.append_call_count
                ));
            }
        }

        append_lake_txn_log_with_rowset(
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

    fn all_tablet_fail_infos(&self) -> Vec<types::TTabletFailInfo> {
        self.tablet_commit_infos
            .iter()
            .map(|info| types::TTabletFailInfo::new(Some(info.tablet_id), Some(info.backend_id)))
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
        merged_tablet_commit_infos: &[types::TTabletCommitInfo],
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
        dirty_partitions: &HashSet<i64>,
        written_tablets: &mut HashSet<i64>,
    ) -> Result<(), String> {
        if dirty_partitions.is_empty() {
            return Ok(());
        }
        let mut empty_targets = Vec::new();
        for target in self.all_write_targets.values() {
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
                let load_id = self.plan.load_id.clone();
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
                    .batch
                    .schema()
                    .fields()
                    .iter()
                    .enumerate()
                    .map(|(idx, field)| {
                        let slot_id = field_slot_id(field.as_ref())
                            .ok()
                            .flatten()
                            .map(|slot| slot.to_string())
                            .unwrap_or_else(|| "none".to_string());
                        format!("{idx}:{}(slot={slot_id})", field.name())
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
            let response = create_automatic_partitions(
                &auto_partition.fe_addr,
                auto_partition.db_id,
                auto_partition.table_id,
                auto_partition.txn_id,
                vec![partition_values.clone()],
            )
            .map_err(|e| format!("OLAP_TABLE_SINK runtime automatic partition failed: {e}"))?;
            self.ingest_auto_partition_response(&auto_partition, response)?;
            self.seen_partition_values.insert(partition_values);
        }
        Ok(())
    }

    fn ingest_auto_partition_response(
        &mut self,
        auto_partition: &crate::connector::starrocks::sink::factory::AutomaticPartitionPlan,
        response: crate::frontend_service::TCreatePartitionResult,
    ) -> Result<(), String> {
        let partitions = response.partitions.unwrap_or_default();
        let tablets = response.tablets.unwrap_or_default();

        let mut new_partitions = Vec::<PartitionRoutingEntry>::new();
        let mut tablet_to_partition = HashMap::<i64, i64>::new();
        let partition_key_len = partition_key_source_len(&auto_partition.partition_key_source);
        for partition in partitions {
            if partition.is_shadow_partition.unwrap_or(false) {
                continue;
            }
            let index = partition
                .indexes
                .iter()
                .find(|idx| auto_partition.candidate_index_ids.contains(&idx.index_id))
                .ok_or_else(|| {
                    format!(
                        "OLAP_TABLE_SINK createPartition returned partition {} without routing index for schema_id={} (candidate_index_ids={:?})",
                        partition.id, auto_partition.schema_id, auto_partition.candidate_index_ids
                    )
                })?;
            if index.tablet_ids.is_empty() {
                return Err(format!(
                    "OLAP_TABLE_SINK createPartition returned partition {} with empty tablet_ids",
                    partition.id
                ));
            }
            let start_key = parse_partition_boundary_key(
                partition.start_keys.as_deref(),
                partition.start_key.as_ref(),
            )?;
            let end_key = parse_partition_boundary_key(
                partition.end_keys.as_deref(),
                partition.end_key.as_ref(),
            )?;
            let in_keys = parse_partition_in_keys(partition.in_keys.as_deref())?;
            validate_partition_key_length(
                partition.id,
                partition_key_len,
                start_key.as_deref(),
                end_key.as_deref(),
                &in_keys,
            )?;
            if !in_keys.is_empty() && end_key.is_some() {
                return Err(format!(
                    "OLAP_TABLE_SINK createPartition returned mixed range/list metadata for partition {}",
                    partition.id
                ));
            }
            for tablet_id in &index.tablet_ids {
                tablet_to_partition.insert(*tablet_id, partition.id);
            }
            if self
                .row_routing
                .partitions
                .iter()
                .any(|entry| entry.partition_id == partition.id)
            {
                continue;
            }
            new_partitions.push(PartitionRoutingEntry {
                partition_id: partition.id,
                tablet_ids: index.tablet_ids.clone(),
                start_key,
                end_key,
                in_keys,
            });
        }
        if !new_partitions.is_empty() {
            if !self.auto_partition_initialized {
                self.row_routing.partitions.clear();
                self.row_routing.tablet_ids.clear();
                self.row_routing.tablet_idx_by_id.clear();
            }
            self.row_routing.partitions.extend(new_partitions);
            self.auto_partition_initialized = true;
        }

        if self.auto_partition_initialized {
            self.row_routing.partition_key_source = auto_partition.partition_key_source.clone();
            self.row_routing.partition_key_len = partition_key_len;

            let has_any_in_keys = self
                .row_routing
                .partitions
                .iter()
                .any(|entry| !entry.in_keys.is_empty());
            let has_any_range_bound = self
                .row_routing
                .partitions
                .iter()
                .any(|entry| entry.start_key.is_some() || entry.end_key.is_some());
            self.row_routing.partition_mode = if self.row_routing.partition_key_len == 0
                || (!has_any_in_keys && !has_any_range_bound)
            {
                PartitionMode::Unpartitioned
            } else if has_any_in_keys {
                if self
                    .row_routing
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
                if self
                    .row_routing
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
            for entry in &self.row_routing.partitions {
                for tablet_id in &entry.tablet_ids {
                    all_tablets.insert(*tablet_id);
                }
            }
            self.row_routing.tablet_ids = all_tablets.into_iter().collect::<Vec<_>>();
            self.row_routing.tablet_idx_by_id.clear();
            for (idx, tablet_id) in self.row_routing.tablet_ids.iter().enumerate() {
                self.row_routing.tablet_idx_by_id.insert(*tablet_id, idx);
            }
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
                self.tablet_commit_infos.push(types::TTabletCommitInfo::new(
                    tablet.tablet_id,
                    backend_id,
                    Option::<Vec<String>>::None,
                    Option::<Vec<String>>::None,
                    Option::<Vec<i64>>::None,
                ));
            }
        }

        let new_tablets = self
            .row_routing
            .tablet_ids
            .iter()
            .copied()
            .filter(|tablet_id| !self.write_targets.contains_key(tablet_id))
            .collect::<Vec<_>>();
        if new_tablets.is_empty() {
            return Ok(());
        }
        let refs = new_tablets
            .iter()
            .map(|tablet_id| LakeTabletPartitionRef {
                tablet_id: *tablet_id,
            })
            .collect::<Vec<_>>();
        let path_map = resolve_tablet_paths_for_olap_sink(
            None,
            Some(&auto_partition.fe_addr),
            &self.plan.table_identity,
            &refs,
        )?;
        let shard_infos = starlet_shard_registry::select_infos(&new_tablets);
        let template = self
            .write_targets
            .values()
            .next()
            .map(|target| target.context.clone())
            .ok_or_else(|| {
                "OLAP_TABLE_SINK cannot build runtime write target: no template context available"
                    .to_string()
            })?;
        for tablet_id in new_tablets {
            let partition_id = self
                .row_routing
                .partitions
                .iter()
                .find(|entry| entry.tablet_ids.contains(&tablet_id))
                .map(|entry| entry.partition_id)
                .or_else(|| tablet_to_partition.get(&tablet_id).copied())
                .ok_or_else(|| {
                    format!(
                        "OLAP_TABLE_SINK cannot resolve partition for runtime tablet {}",
                        tablet_id
                    )
                })?;
            let tablet_root_path = path_map.get(&tablet_id).ok_or_else(|| {
                format!(
                    "OLAP_TABLE_SINK missing resolved storage path for runtime tablet {}",
                    tablet_id
                )
            })?;
            let scheme = classify_scan_paths([tablet_root_path.as_str()])?;
            let s3_config = match scheme {
                ScanPathScheme::Local => None,
                ScanPathScheme::Oss => {
                    let from_shard = shard_infos.get(&tablet_id).and_then(|info| info.s3.clone());
                    let from_runtime = if from_shard.is_none() {
                        get_tablet_runtime(tablet_id)
                            .ok()
                            .and_then(|entry| entry.s3_config.clone())
                    } else {
                        None
                    };
                    let inferred = if from_shard.is_none() && from_runtime.is_none() {
                        starlet_shard_registry::infer_s3_config_for_path(tablet_root_path)
                    } else {
                        None
                    };
                    Some(from_shard.or(from_runtime).or(inferred).ok_or_else(|| {
                        format!(
                            "OLAP_TABLE_SINK missing S3 config for runtime object-store tablet {} (path={})",
                            tablet_id, tablet_root_path
                        )
                    })?)
                }
                ScanPathScheme::Hdfs => {
                    return Err(format!(
                        "OLAP_TABLE_SINK does not support hdfs tablet path yet: tablet_id={} path={}",
                        tablet_id, tablet_root_path
                    ));
                }
            };

            let mut context = template.clone();
            context.db_id = self.plan.db_id;
            context.table_id = self.plan.table_id;
            context.tablet_id = tablet_id;
            context.tablet_root_path = tablet_root_path.clone();
            context.s3_config = s3_config;

            let target = TabletWriteTarget {
                tablet_id,
                partition_id,
                context,
            };
            self.write_targets.insert(tablet_id, target.clone());
            self.all_write_targets.insert(tablet_id, target.clone());
            if let Some(primary_index_plan) = self.index_write_plans.first_mut() {
                primary_index_plan.write_targets.insert(tablet_id, target);
            }
        }
        if let Some(primary_index_plan) = self.index_write_plans.first_mut() {
            primary_index_plan.row_routing = self.row_routing.clone();
        }
        Ok(())
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
            let field = field_with_slot_id(
                Field::new(field_name, projected.data_type().clone(), true),
                slot_id,
            );
            projected_fields.push(field);
            projected_columns.push(projected);
        }
        let projected_schema = Arc::new(Schema::new_with_metadata(
            projected_fields,
            chunk.schema().metadata().clone(),
        ));
        let projected_batch = RecordBatch::try_new(projected_schema, projected_columns)
            .map_err(|e| format!("OLAP_TABLE_SINK build projected output batch failed: {e}"))?;
        Chunk::try_new(projected_batch)
            .map_err(|e| format!("OLAP_TABLE_SINK build projected output chunk failed: {e}"))
    }

    fn flush_real_data(&mut self, state: &RuntimeState) -> Result<(), String> {
        if self.index_write_plans.is_empty() || self.all_write_targets.is_empty() {
            return Err("OLAP_TABLE_SINK has empty write_targets".to_string());
        }
        if self.plan.auto_partition.is_some() && self.index_write_plans.len() > 1 {
            return Err(
                "OLAP_TABLE_SINK automatic partition with multi-index sink is not supported yet"
                    .to_string(),
            );
        }

        let request_rows_threshold = state.chunk_size().max(1);
        let request_bytes_threshold =
            crate::common::config::olap_sink_max_tablet_write_chunk_bytes().max(1);
        let write_buffer_size = crate::common::config::olap_sink_write_buffer_size_bytes().max(1);

        let flush_start_file_seq = self.file_seq;
        let flush_start_random_hash = self.next_random_hash;
        let flush_start_pending_input_rows = self.pending_input_rows;
        let flush_start_pending_input_bytes = self.pending_input_bytes;
        let pending_chunks = std::mem::take(&mut self.pending_chunks);
        self.pending_input_rows = 0;
        self.pending_input_bytes = 0;
        let flush_result: Result<(), String> = (|| {
            self.ensure_auto_partitions_for_chunks(&pending_chunks)?;
            let index_write_plans = self.index_write_plans.clone();
            let mut buffered_by_tablet = BTreeMap::<i64, TabletBufferedState>::new();
            for chunk in &pending_chunks {
                if chunk.is_empty() {
                    continue;
                }
                let sink_chunk = self.project_chunk_for_sink_output(chunk)?;
                let chunk_random_hash_seed = self.next_random_hash;
                let mut first_plan_next_random_hash = chunk_random_hash_seed;
                for (plan_idx, index_plan) in index_write_plans.iter().enumerate() {
                    let mut plan_random_hash = chunk_random_hash_seed;
                    let routed = route_chunk_rows(
                        &index_plan.row_routing,
                        &sink_chunk,
                        &mut plan_random_hash,
                    )?;
                    if plan_idx == 0 {
                        first_plan_next_random_hash = plan_random_hash;
                    }
                    for (tablet_idx, row_indices) in routed.into_iter().enumerate() {
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
                        let routed_batch = if row_indices.len() == sink_chunk.len() {
                            sink_chunk.batch.clone()
                        } else {
                            take_batch_rows(&sink_chunk.batch, &row_indices)?
                        };
                        let is_primary_keys_table = target.context.tablet_schema.keys_type
                            == Some(KeysType::PrimaryKeys as i32);
                        let routed_batch = match align_batch_to_schema_slot_bindings(
                            &routed_batch,
                            &index_plan.schema_slot_bindings,
                            index_plan.op_slot_id,
                        ) {
                            Ok(aligned_batch)
                                if !is_primary_keys_table
                                    && aligned_batch.num_columns() < routed_batch.num_columns() =>
                            {
                                let batch_fields = routed_batch
                                    .schema()
                                    .fields()
                                    .iter()
                                    .enumerate()
                                    .map(|(idx, field)| {
                                        let slot_id = field_slot_id(field.as_ref())
                                            .ok()
                                            .flatten()
                                            .map(|slot| slot.to_string())
                                            .unwrap_or_else(|| "none".to_string());
                                        format!("{idx}:{}(slot={slot_id})", field.name())
                                    })
                                    .collect::<Vec<_>>();
                                info!(
                                    target: "novarocks::sink",
                                    table_id = self.plan.table_id,
                                    index_id = index_plan.index_id,
                                    schema_id = index_plan.schema_id,
                                    tablet_id = target.tablet_id,
                                    original_columns = routed_batch.num_columns(),
                                    aligned_columns = aligned_batch.num_columns(),
                                    batch_fields = ?batch_fields,
                                    "OLAP_TABLE_SINK skip write-slot alignment for non-primary key batch because alignment dropped columns"
                                );
                                routed_batch
                            }
                            Ok(aligned_batch) => aligned_batch,
                            Err(err) if !is_primary_keys_table => {
                                let batch_fields = routed_batch
                                    .schema()
                                    .fields()
                                    .iter()
                                    .enumerate()
                                    .map(|(idx, field)| {
                                        let slot_id = field_slot_id(field.as_ref())
                                            .ok()
                                            .flatten()
                                            .map(|slot| slot.to_string())
                                            .unwrap_or_else(|| "none".to_string());
                                        format!("{idx}:{}(slot={slot_id})", field.name())
                                    })
                                    .collect::<Vec<_>>();
                                info!(
                                    target: "novarocks::sink",
                                    table_id = self.plan.table_id,
                                    index_id = index_plan.index_id,
                                    schema_id = index_plan.schema_id,
                                    tablet_id = target.tablet_id,
                                    error = %err,
                                    original_columns = routed_batch.num_columns(),
                                    batch_fields = ?batch_fields,
                                    "OLAP_TABLE_SINK skip write-slot alignment for non-primary key batch because alignment failed"
                                );
                                routed_batch
                            }
                            Err(err) => return Err(err),
                        };
                        if !buffered_by_tablet.contains_key(&tablet_id) {
                            buffered_by_tablet.insert(
                                tablet_id,
                                TabletBufferedState::new(
                                    target.partition_id,
                                    target.context.clone(),
                                ),
                            );
                        }
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
                        buffer.push_request_batch(routed_batch);
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
            return Err(err);
        }

        Ok(())
    }

    #[cfg(test)]
    fn inject_temporary_failure_once_at_append_call(&mut self, append_call: usize) {
        self.fail_once_at_append_call = Some(append_call);
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
        } else if self.input_rows > 0 {
            // FE derives loaded rows from reportExecStatus load counters.
            state.add_sink_load_counters(self.input_rows, 0);
        }

        self.finalize_shared.record_progress(
            &self.written_tablets,
            &self.dirty_partitions,
            &self.tablet_commit_infos,
        );
        let is_last_driver = self.finalize_shared.arrive_and_is_last();

        let result = if is_last_driver {
            if let Some(err) = self.finalize_shared.first_error() {
                Err(err)
            } else {
                let (dirty_partitions, mut merged_written_tablets, merged_tablet_commit_infos) =
                    self.finalize_shared.snapshot_progress();
                let mut merged_written_tablet_ids =
                    merged_written_tablets.iter().copied().collect::<Vec<_>>();
                merged_written_tablet_ids.sort_unstable();
                info!(
                    target: "novarocks::starrocks::sink",
                    table_id = self.plan.table_id,
                    txn_id = self.plan.txn_id,
                    dirty_partition_count = dirty_partitions.len(),
                    merged_written_tablet_count = merged_written_tablets.len(),
                    merged_written_tablet_ids = ?merged_written_tablet_ids,
                    "OLAP_TABLE_SINK finalizing sink progress"
                );
                self.finalize_dirty_partition_tablets(
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
    for (idx, field) in chunk.batch.schema().fields().iter().enumerate() {
        if let Some(slot_id) = field_slot_id(field.as_ref())? {
            slot_to_index.entry(slot_id).or_insert(idx);
        }
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
                values.push(STARROCKS_DEFAULT_PARTITION_VALUE.to_string());
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
            Ok(if typed.value(row) { "1" } else { "0" }.to_string())
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

fn align_batch_to_schema_slot_bindings(
    batch: &RecordBatch,
    schema_slot_bindings: &[Option<SlotId>],
    op_slot_id: Option<SlotId>,
) -> Result<RecordBatch, String> {
    if schema_slot_bindings.is_empty() && op_slot_id.is_none() {
        return Ok(batch.clone());
    }

    let mut slot_to_data_index = HashMap::new();
    let mut slot_to_op_index = HashMap::new();
    for (idx, field) in batch.schema().fields().iter().enumerate() {
        if let Some(slot_id) = field_slot_id(field.as_ref())? {
            if field.name() == LOAD_OP_COLUMN {
                slot_to_op_index.entry(slot_id).or_insert(idx);
            } else {
                slot_to_data_index.entry(slot_id).or_insert(idx);
            }
        }
    }

    let mut aligned_columns = Vec::new();
    let mut aligned_fields = Vec::new();
    let mut aligned_data_indexes = Vec::new();
    for slot_id in schema_slot_bindings.iter().copied().flatten() {
        let Some(idx) = slot_to_data_index.get(&slot_id).copied() else {
            continue;
        };
        // Keep duplicated schema bindings (e.g. INSERT ... SELECT k1, k1, v)
        // so non-primary key writes preserve full target column count.
        aligned_data_indexes.push(idx);
        aligned_columns.push(batch.column(idx).clone());
        aligned_fields.push(batch.schema().field(idx).clone());
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
        batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .find_map(|(idx, field)| (field.name() == LOAD_OP_COLUMN).then_some(idx))
    };
    if let Some(op_idx) = resolved_op_index {
        // Keep schema-slot aligned output order (including duplicated slots),
        // but append __op at most once to preserve control-column semantics.
        let op_already_selected = aligned_data_indexes.contains(&op_idx);
        if !op_already_selected {
            aligned_columns.push(batch.column(op_idx).clone());
            let op_field = batch.schema().field(op_idx).as_ref().clone();
            aligned_fields.push(op_field.with_name(LOAD_OP_COLUMN.to_string()));
        }
    }
    if aligned_columns.is_empty() {
        return Err(format!(
            "OLAP_TABLE_SINK cannot align routed batch with schema slot bindings: batch_columns={} schema_slot_bindings={} op_slot_id={:?}",
            batch.num_columns(),
            schema_slot_bindings.len(),
            op_slot_id
        ));
    }

    let aligned_schema = Arc::new(Schema::new_with_metadata(
        aligned_fields,
        batch.schema().metadata().clone(),
    ));
    RecordBatch::try_new(aligned_schema, aligned_columns)
        .map_err(|e| format!("OLAP_TABLE_SINK build write-slot aligned batch failed: {e}"))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::{
        ArrayRef, Int8Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use tempfile::tempdir;

    use super::{
        LOAD_OP_COLUMN, OlapTableSinkOperator, align_batch_to_schema_slot_bindings,
        is_retryable_sink_write_error,
    };
    use crate::common::ids::SlotId;
    use crate::connector::starrocks::fe_v2_meta::LakeTableIdentity;
    use crate::connector::starrocks::lake::{TabletWriteContext, txn_log::read_txn_log_if_exists};
    use crate::connector::starrocks::sink::factory::{
        OlapTableSinkPlan, SinkOutputProjectionPlan, TabletWriteTarget,
    };
    use crate::connector::starrocks::sink::partition_key::{
        PartitionKeySource, PartitionMode, PartitionRoutingEntry,
    };
    use crate::connector::starrocks::sink::routing::RowRoutingPlan;
    use crate::exec::chunk::{Chunk, field_slot_id, field_with_slot_id};
    use crate::exec::expr::{ExprArena, ExprNode, LiteralValue};
    use crate::exec::pipeline::operator::ProcessorOperator;
    use crate::formats::starrocks::writer::{StarRocksWriteFormat, layout::txn_log_file_path};
    use crate::runtime::runtime_state::RuntimeState;
    use crate::service::grpc_client::proto::starrocks::{
        ColumnPb, KeysType, PUniqueId, TabletSchemaPb,
    };
    use crate::types;

    fn test_tablet_schema() -> TabletSchemaPb {
        TabletSchemaPb {
            keys_type: Some(KeysType::DupKeys as i32),
            column: vec![ColumnPb {
                unique_id: 1,
                name: Some("c1".to_string()),
                r#type: "BIGINT".to_string(),
                is_key: Some(true),
                aggregation: None,
                is_nullable: Some(false),
                default_value: None,
                precision: None,
                frac: None,
                length: None,
                index_length: None,
                is_bf_column: None,
                referenced_column_id: None,
                referenced_column: None,
                has_bitmap_index: None,
                visible: None,
                children_columns: Vec::new(),
                is_auto_increment: Some(false),
                agg_state_desc: None,
            }],
            num_short_key_columns: Some(1),
            num_rows_per_row_block: None,
            bf_fpp: None,
            next_column_unique_id: Some(2),
            deprecated_is_in_memory: None,
            deprecated_id: None,
            compression_type: None,
            sort_key_idxes: vec![0],
            schema_version: Some(0),
            sort_key_unique_ids: vec![1],
            table_indices: Vec::new(),
            compression_level: None,
            id: Some(5001),
        }
    }

    fn test_primary_key_tablet_schema() -> TabletSchemaPb {
        TabletSchemaPb {
            keys_type: Some(KeysType::PrimaryKeys as i32),
            column: vec![
                ColumnPb {
                    unique_id: 1,
                    name: Some("k1".to_string()),
                    r#type: "INT".to_string(),
                    is_key: Some(true),
                    aggregation: None,
                    is_nullable: Some(false),
                    default_value: None,
                    precision: None,
                    frac: None,
                    length: None,
                    index_length: None,
                    is_bf_column: None,
                    referenced_column_id: None,
                    referenced_column: None,
                    has_bitmap_index: None,
                    visible: None,
                    children_columns: Vec::new(),
                    is_auto_increment: Some(false),
                    agg_state_desc: None,
                },
                ColumnPb {
                    unique_id: 2,
                    name: Some("v1".to_string()),
                    r#type: "BIGINT".to_string(),
                    is_key: Some(false),
                    aggregation: None,
                    is_nullable: Some(true),
                    default_value: None,
                    precision: None,
                    frac: None,
                    length: None,
                    index_length: None,
                    is_bf_column: None,
                    referenced_column_id: None,
                    referenced_column: None,
                    has_bitmap_index: None,
                    visible: None,
                    children_columns: Vec::new(),
                    is_auto_increment: Some(false),
                    agg_state_desc: None,
                },
            ],
            num_short_key_columns: Some(1),
            num_rows_per_row_block: None,
            bf_fpp: None,
            next_column_unique_id: Some(3),
            deprecated_is_in_memory: None,
            deprecated_id: None,
            compression_type: None,
            sort_key_idxes: vec![0],
            schema_version: Some(0),
            sort_key_unique_ids: vec![1],
            table_indices: Vec::new(),
            compression_level: None,
            id: Some(5002),
        }
    }

    fn one_col_chunk(values: Vec<i64>) -> Chunk {
        let schema = Arc::new(Schema::new(vec![field_with_slot_id(
            Field::new("c1", DataType::Int64, false),
            SlotId::new(1),
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))])
            .expect("build record batch");
        Chunk::try_new(batch).expect("build chunk")
    }

    fn pk_mixed_op_chunk(keys: Vec<i32>, values: Vec<i64>, ops: Vec<i8>) -> Chunk {
        assert_eq!(keys.len(), values.len());
        assert_eq!(keys.len(), ops.len());
        let schema = Arc::new(Schema::new(vec![
            field_with_slot_id(Field::new("k1", DataType::Int32, false), SlotId::new(1)),
            field_with_slot_id(Field::new("v1", DataType::Int64, true), SlotId::new(2)),
            field_with_slot_id(Field::new("__op", DataType::Int8, false), SlotId::new(3)),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(keys)),
                Arc::new(Int64Array::from(values)),
                Arc::new(Int8Array::from(ops)),
            ],
        )
        .expect("build primary key mixed op record batch");
        Chunk::try_new(batch).expect("build chunk")
    }

    fn slot_bound_batch_with_op(include_op: bool) -> RecordBatch {
        let mut fields = Vec::new();
        let mut columns: Vec<ArrayRef> = Vec::new();
        for slot in 1_u32..=6 {
            fields.push(field_with_slot_id(
                Field::new(format!("c{slot}"), DataType::Int64, false),
                SlotId::new(slot),
            ));
            columns.push(Arc::new(Int64Array::from(vec![
                slot as i64,
                slot as i64 + 100,
            ])));
        }
        if include_op {
            fields.push(field_with_slot_id(
                Field::new(LOAD_OP_COLUMN, DataType::Int8, false),
                SlotId::new(9),
            ));
            columns.push(Arc::new(Int8Array::from(vec![0_i8, 1_i8])));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("build slot-bound record batch")
    }

    fn recursive_cte_schema_slot_bindings() -> Vec<Option<SlotId>> {
        vec![
            Some(SlotId::new(1)),
            Some(SlotId::new(2)),
            Some(SlotId::new(3)),
            Some(SlotId::new(4)),
            Some(SlotId::new(5)),
            Some(SlotId::new(2)),
            Some(SlotId::new(6)),
        ]
    }

    fn build_test_plan(
        tablet_root_path: String,
        table_id: i64,
        tablet_id: i64,
        partition_id: i64,
        tablet_schema: TabletSchemaPb,
    ) -> Arc<OlapTableSinkPlan> {
        let write_target = TabletWriteTarget {
            tablet_id,
            partition_id,
            context: TabletWriteContext {
                db_id: 6001,
                table_id,
                tablet_id,
                tablet_root_path,
                tablet_schema,
                s3_config: None,
                partial_update: Default::default(),
            },
        };
        let row_routing = RowRoutingPlan {
            tablet_ids: vec![tablet_id],
            tablet_idx_by_id: HashMap::from([(tablet_id, 0)]),
            distributed_slot_ids: Vec::new(),
            partition_key_source: PartitionKeySource::None,
            partition_key_len: 0,
            partition_mode: PartitionMode::Unpartitioned,
            partitions: vec![PartitionRoutingEntry {
                partition_id,
                tablet_ids: vec![tablet_id],
                start_key: None,
                end_key: None,
                in_keys: Vec::new(),
            }],
        };
        Arc::new(OlapTableSinkPlan {
            db_id: 6001,
            table_id,
            table_identity: LakeTableIdentity {
                catalog: "default_catalog".to_string(),
                db_name: "db1".to_string(),
                table_name: "t1".to_string(),
                db_id: 6001,
                table_id,
                schema_id: 5001,
            },
            db_name: Some("db1".to_string()),
            table_name: Some("t1".to_string()),
            txn_id: 8001,
            load_id: PUniqueId { hi: 11, lo: 22 },
            write_format: StarRocksWriteFormat::Native,
            tablet_commit_infos: vec![types::TTabletCommitInfo::new(
                tablet_id,
                1001,
                Option::<Vec<String>>::None,
                Option::<Vec<String>>::None,
                Option::<Vec<i64>>::None,
            )],
            write_targets: HashMap::from([(tablet_id, write_target)]),
            row_routing,
            schema_slot_bindings: Vec::new(),
            op_slot_id: None,
            index_write_plans: Vec::new(),
            output_projection: None,
            auto_partition: None,
        })
    }

    fn build_two_tablet_plan(
        tablet_a_root_path: String,
        tablet_b_root_path: String,
        table_id: i64,
        partition_id: i64,
        tablet_schema: TabletSchemaPb,
    ) -> Arc<OlapTableSinkPlan> {
        let tablet_a = 9910;
        let tablet_b = 9911;
        let write_target_a = TabletWriteTarget {
            tablet_id: tablet_a,
            partition_id,
            context: TabletWriteContext {
                db_id: 6001,
                table_id,
                tablet_id: tablet_a,
                tablet_root_path: tablet_a_root_path,
                tablet_schema: tablet_schema.clone(),
                s3_config: None,
                partial_update: Default::default(),
            },
        };
        let write_target_b = TabletWriteTarget {
            tablet_id: tablet_b,
            partition_id,
            context: TabletWriteContext {
                db_id: 6001,
                table_id,
                tablet_id: tablet_b,
                tablet_root_path: tablet_b_root_path,
                tablet_schema,
                s3_config: None,
                partial_update: Default::default(),
            },
        };
        let row_routing = RowRoutingPlan {
            tablet_ids: vec![tablet_a, tablet_b],
            tablet_idx_by_id: HashMap::from([(tablet_a, 0), (tablet_b, 1)]),
            distributed_slot_ids: Vec::new(),
            partition_key_source: PartitionKeySource::None,
            partition_key_len: 0,
            partition_mode: PartitionMode::Unpartitioned,
            partitions: vec![PartitionRoutingEntry {
                partition_id,
                tablet_ids: vec![tablet_a, tablet_b],
                start_key: None,
                end_key: None,
                in_keys: Vec::new(),
            }],
        };
        Arc::new(OlapTableSinkPlan {
            db_id: 6001,
            table_id,
            table_identity: LakeTableIdentity {
                catalog: "default_catalog".to_string(),
                db_name: "db1".to_string(),
                table_name: "t1".to_string(),
                db_id: 6001,
                table_id,
                schema_id: 5001,
            },
            db_name: Some("db1".to_string()),
            table_name: Some("t1".to_string()),
            txn_id: 8010,
            load_id: PUniqueId { hi: 33, lo: 44 },
            write_format: StarRocksWriteFormat::Native,
            tablet_commit_infos: vec![
                types::TTabletCommitInfo::new(
                    tablet_a,
                    1001,
                    Option::<Vec<String>>::None,
                    Option::<Vec<String>>::None,
                    Option::<Vec<i64>>::None,
                ),
                types::TTabletCommitInfo::new(
                    tablet_b,
                    1001,
                    Option::<Vec<String>>::None,
                    Option::<Vec<String>>::None,
                    Option::<Vec<i64>>::None,
                ),
            ],
            write_targets: HashMap::from([(tablet_a, write_target_a), (tablet_b, write_target_b)]),
            row_routing,
            schema_slot_bindings: Vec::new(),
            op_slot_id: None,
            index_write_plans: Vec::new(),
            output_projection: None,
            auto_partition: None,
        })
    }

    #[test]
    fn sink_retryable_error_detection_matches_temporary_and_5xx_patterns() {
        assert!(is_retryable_sink_write_error(
            "write object failed: Unexpected (temporary) at Writer::close"
        ));
        assert!(is_retryable_sink_write_error(
            "response: Parts { status: 502, version: HTTP/1.1 }"
        ));
        assert!(!is_retryable_sink_write_error(
            "invalid table_id for lake write: -1"
        ));
    }

    #[test]
    fn align_batch_preserves_duplicate_schema_slot_bindings() {
        let batch = slot_bound_batch_with_op(false);
        let aligned = align_batch_to_schema_slot_bindings(
            &batch,
            &recursive_cte_schema_slot_bindings(),
            None,
        )
        .expect("align batch with duplicate schema slot bindings");

        assert_eq!(aligned.num_columns(), 7);
        assert_eq!(aligned.schema().field(1).name(), "c2");
        assert_eq!(aligned.schema().field(5).name(), "c2");

        let c2 = aligned
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("c2 at index 1");
        let c2_dup = aligned
            .column(5)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("duplicated c2 at index 5");
        for row in 0..aligned.num_rows() {
            assert_eq!(c2.value(row), c2_dup.value(row));
        }
    }

    #[test]
    fn align_batch_duplicate_bindings_with_op_column() {
        let batch = slot_bound_batch_with_op(true);
        let aligned = align_batch_to_schema_slot_bindings(
            &batch,
            &recursive_cte_schema_slot_bindings(),
            Some(SlotId::new(9)),
        )
        .expect("align batch with duplicate schema slot bindings and __op");

        assert_eq!(aligned.num_columns(), 8);
        assert_eq!(aligned.schema().field(7).name(), LOAD_OP_COLUMN);
        assert_eq!(
            aligned
                .schema()
                .fields()
                .iter()
                .filter(|field| field.name() == LOAD_OP_COLUMN)
                .count(),
            1
        );

        let op = aligned
            .column(7)
            .as_any()
            .downcast_ref::<Int8Array>()
            .expect("op column");
        assert_eq!(op.value(0), 0);
        assert_eq!(op.value(1), 1);

        let c2 = aligned
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("c2 at index 1");
        let c2_dup = aligned
            .column(5)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("duplicated c2 at index 5");
        for row in 0..aligned.num_rows() {
            assert_eq!(c2.value(row), c2_dup.value(row));
        }
    }

    #[test]
    fn align_batch_does_not_alias_data_slot_to_op_column() {
        let batch = slot_bound_batch_with_op(false);
        let aligned = align_batch_to_schema_slot_bindings(
            &batch,
            &recursive_cte_schema_slot_bindings(),
            Some(SlotId::new(1)),
        )
        .expect("align batch should ignore misbound op slot id");

        assert_eq!(aligned.num_columns(), 7);
        assert_eq!(
            aligned
                .schema()
                .fields()
                .iter()
                .filter(|field| field.name() == LOAD_OP_COLUMN)
                .count(),
            0
        );
    }

    fn slot_bound_batch_with_unnamed_op_slot() -> RecordBatch {
        let mut fields = Vec::new();
        let mut columns: Vec<ArrayRef> = Vec::new();
        for slot in 1_u32..=6 {
            fields.push(field_with_slot_id(
                Field::new(format!("c{slot}"), DataType::Int64, false),
                SlotId::new(slot),
            ));
            columns.push(Arc::new(Int64Array::from(vec![
                slot as i64,
                slot as i64 + 100,
            ])));
        }
        fields.push(field_with_slot_id(
            Field::new("col_7", DataType::Int8, false),
            SlotId::new(9),
        ));
        columns.push(Arc::new(Int8Array::from(vec![0_i8, 1_i8])));
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("build slot-bound record batch with unnamed op slot")
    }

    #[test]
    fn align_batch_recovers_op_from_slot_when_name_is_generic() {
        let batch = slot_bound_batch_with_unnamed_op_slot();
        let aligned = align_batch_to_schema_slot_bindings(
            &batch,
            &recursive_cte_schema_slot_bindings(),
            Some(SlotId::new(9)),
        )
        .expect("align batch should recover __op from slot id");

        assert_eq!(aligned.num_columns(), 8);
        assert_eq!(aligned.schema().field(7).name(), LOAD_OP_COLUMN);
        let op = aligned
            .column(7)
            .as_any()
            .downcast_ref::<Int8Array>()
            .expect("op column");
        assert_eq!(op.value(0), 0);
        assert_eq!(op.value(1), 1);
    }

    #[test]
    fn output_projection_materializes_literal_op_column_before_routing() {
        let tmp = tempdir().expect("create tempdir");
        let tablet_id = 9904;
        let partition_id = 3004;
        let tablet_root_path = tmp.path().join("tablet_9904").to_string_lossy().to_string();
        let base_plan = build_test_plan(
            tablet_root_path,
            7004,
            tablet_id,
            partition_id,
            test_tablet_schema(),
        );
        let mut arena = ExprArena::default();
        let data_expr = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int64);
        let op_expr = arena.push_typed(ExprNode::Literal(LiteralValue::Int8(0)), DataType::Int8);
        let projection = SinkOutputProjectionPlan {
            arena: Arc::new(arena),
            expr_ids: vec![data_expr, op_expr],
            output_slot_ids: vec![SlotId::new(1), SlotId::new(8)],
            output_field_names: vec!["c1".to_string(), LOAD_OP_COLUMN.to_string()],
        };
        let plan = Arc::new(OlapTableSinkPlan {
            output_projection: Some(projection),
            ..(*base_plan).clone()
        });
        let op = OlapTableSinkOperator::new("OLAP_TABLE_SINK".to_string(), plan, 0);
        let projected = op
            .project_chunk_for_sink_output(&one_col_chunk(vec![10, 20]))
            .expect("project sink output");

        assert_eq!(projected.batch.num_columns(), 2);
        assert_eq!(projected.batch.schema().field(1).name(), LOAD_OP_COLUMN);
        assert_eq!(
            field_slot_id(projected.batch.schema().field(1).as_ref())
                .expect("read op slot id")
                .expect("op slot id"),
            SlotId::new(8)
        );
        let op_array = projected
            .batch
            .column(1)
            .as_any()
            .downcast_ref::<Int8Array>()
            .expect("op column should be Int8");
        assert_eq!(op_array.value(0), 0);
        assert_eq!(op_array.value(1), 0);
    }

    #[test]
    fn set_finishing_retries_temporary_append_failure_without_duplicate_rowset() {
        let tmp = tempdir().expect("create tempdir");
        let tablet_id = 9901;
        let partition_id = 3001;
        let tablet_root_path = tmp.path().join("tablet_9901").to_string_lossy().to_string();
        let plan = build_test_plan(
            tablet_root_path.clone(),
            7001,
            tablet_id,
            partition_id,
            test_tablet_schema(),
        );
        let mut op = OlapTableSinkOperator::new("OLAP_TABLE_SINK".to_string(), plan.clone(), 0);
        let state = RuntimeState::default();

        op.push_chunk(&state, one_col_chunk(vec![1, 2]))
            .expect("push first chunk");
        op.push_chunk(&state, one_col_chunk(vec![3]))
            .expect("push second chunk");

        // Fail the first append in the first flush attempt. Retry should replay the same
        // buffered payload and avoid duplicate rowset segments.
        op.inject_temporary_failure_once_at_append_call(1);
        op.set_finishing(&state)
            .expect("set finishing should retry and succeed");

        let log_path = txn_log_file_path(&tablet_root_path, tablet_id, plan.txn_id)
            .expect("build txn log path");
        let txn_log = read_txn_log_if_exists(&log_path)
            .expect("read txn log")
            .expect("txn log should exist");
        let rowset = txn_log
            .op_write
            .and_then(|op_write| op_write.rowset)
            .expect("rowset should exist");
        assert_eq!(rowset.num_rows, Some(3));
        assert_eq!(rowset.segments.len(), 1);
    }

    #[test]
    fn set_finishing_small_chunks_are_aggregated_before_append() {
        let tmp = tempdir().expect("create tempdir");
        let tablet_id = 9920;
        let partition_id = 3020;
        let tablet_root_path = tmp.path().join("tablet_9920").to_string_lossy().to_string();
        let plan = build_test_plan(
            tablet_root_path.clone(),
            7020,
            tablet_id,
            partition_id,
            test_tablet_schema(),
        );
        let mut op = OlapTableSinkOperator::new("OLAP_TABLE_SINK".to_string(), plan.clone(), 0);
        let state = RuntimeState::default();

        for value in 1..=12_i64 {
            op.push_chunk(&state, one_col_chunk(vec![value]))
                .expect("push tiny chunk");
        }
        op.set_finishing(&state)
            .expect("set finishing should flush aggregated chunks");

        let log_path = txn_log_file_path(&tablet_root_path, tablet_id, plan.txn_id)
            .expect("build txn log path");
        let txn_log = read_txn_log_if_exists(&log_path)
            .expect("read txn log")
            .expect("txn log should exist");
        let rowset = txn_log
            .op_write
            .and_then(|op_write| op_write.rowset)
            .expect("rowset should exist");
        assert_eq!(rowset.num_rows, Some(12));
        assert_eq!(rowset.segments.len(), 1);
    }

    #[test]
    fn set_finishing_multi_tablet_routing_keeps_tablet_boundaries() {
        let tmp = tempdir().expect("create tempdir");
        let tablet_a_root_path = tmp.path().join("tablet_9910").to_string_lossy().to_string();
        let tablet_b_root_path = tmp.path().join("tablet_9911").to_string_lossy().to_string();
        let plan = build_two_tablet_plan(
            tablet_a_root_path.clone(),
            tablet_b_root_path.clone(),
            7010,
            3010,
            test_tablet_schema(),
        );
        let mut op = OlapTableSinkOperator::new("OLAP_TABLE_SINK".to_string(), plan.clone(), 0);
        let state = RuntimeState::default();

        let values = (1..=16_i64).collect::<Vec<_>>();
        op.push_chunk(&state, one_col_chunk(values))
            .expect("push routed chunk");
        op.set_finishing(&state)
            .expect("set finishing should flush routed chunks");

        let log_a = txn_log_file_path(&tablet_a_root_path, 9910, plan.txn_id)
            .expect("build tablet a log path");
        let log_b = txn_log_file_path(&tablet_b_root_path, 9911, plan.txn_id)
            .expect("build tablet b log path");
        let rowset_a = read_txn_log_if_exists(&log_a)
            .expect("read tablet a txn log")
            .and_then(|log| log.op_write.and_then(|op| op.rowset))
            .expect("tablet a rowset should exist");
        let rowset_b = read_txn_log_if_exists(&log_b)
            .expect("read tablet b txn log")
            .and_then(|log| log.op_write.and_then(|op| op.rowset))
            .expect("tablet b rowset should exist");
        let rows_a = rowset_a.num_rows.unwrap_or(0);
        let rows_b = rowset_b.num_rows.unwrap_or(0);
        assert!(rows_a > 0);
        assert!(rows_b > 0);
        assert_eq!(rows_a + rows_b, 16);
        assert_eq!(rowset_a.segments.len(), 1);
        assert_eq!(rowset_b.segments.len(), 1);
    }

    #[test]
    fn set_finishing_non_primary_key_falls_back_when_alignment_fails() {
        let tmp = tempdir().expect("create tempdir");
        let tablet_id = 9903;
        let partition_id = 3003;
        let tablet_root_path = tmp.path().join("tablet_9903").to_string_lossy().to_string();
        let base_plan = build_test_plan(
            tablet_root_path.clone(),
            7003,
            tablet_id,
            partition_id,
            test_tablet_schema(),
        );
        let plan = Arc::new(OlapTableSinkPlan {
            schema_slot_bindings: vec![Some(SlotId::new(99))],
            ..(*base_plan).clone()
        });
        let mut op = OlapTableSinkOperator::new("OLAP_TABLE_SINK".to_string(), plan.clone(), 0);
        let state = RuntimeState::default();

        op.push_chunk(&state, one_col_chunk(vec![1, 2]))
            .expect("push chunk");
        op.set_finishing(&state)
            .expect("set finishing should fallback to original batch and succeed");

        let log_path = txn_log_file_path(&tablet_root_path, tablet_id, plan.txn_id)
            .expect("build txn log path");
        let txn_log = read_txn_log_if_exists(&log_path)
            .expect("read txn log")
            .expect("txn log should exist");
        let rowset = txn_log
            .op_write
            .and_then(|op_write| op_write.rowset)
            .expect("rowset should exist");
        assert_eq!(rowset.num_rows, Some(2));
        assert_eq!(rowset.segments.len(), 1);
    }

    #[test]
    fn set_finishing_primary_key_mixed_op_chunk_writes_rowset_and_del_file() {
        let tmp = tempdir().expect("create tempdir");
        let tablet_id = 9902;
        let partition_id = 3002;
        let tablet_root_path = tmp.path().join("tablet_9902").to_string_lossy().to_string();
        let plan = build_test_plan(
            tablet_root_path.clone(),
            7002,
            tablet_id,
            partition_id,
            test_primary_key_tablet_schema(),
        );
        let mut op = OlapTableSinkOperator::new("OLAP_TABLE_SINK".to_string(), plan.clone(), 0);
        let state = RuntimeState::default();

        op.push_chunk(
            &state,
            pk_mixed_op_chunk(vec![1, 2], vec![11, 22], vec![0, 1]),
        )
        .expect("push primary key mixed op chunk");
        op.set_finishing(&state)
            .expect("set finishing should flush mixed op chunk");

        let log_path = txn_log_file_path(&tablet_root_path, tablet_id, plan.txn_id)
            .expect("build txn log path");
        let txn_log = read_txn_log_if_exists(&log_path)
            .expect("read txn log")
            .expect("txn log should exist");
        let op_write = txn_log.op_write.expect("op_write should exist");
        let rowset = op_write.rowset.expect("rowset should exist");
        assert_eq!(rowset.num_rows, Some(1));
        assert_eq!(rowset.num_dels, Some(1));
        assert_eq!(rowset.segments.len(), 1);
        assert_eq!(op_write.dels.len(), 1);
    }

    #[test]
    fn align_batch_keeps_duplicate_slot_bindings() {
        let schema = Arc::new(Schema::new(vec![
            field_with_slot_id(Field::new("idx", DataType::Int64, false), SlotId::new(1)),
            field_with_slot_id(Field::new("v", DataType::Utf8, false), SlotId::new(2)),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(arrow::array::StringArray::from(vec!["a", "b"])),
            ],
        )
        .expect("build batch");

        let aligned = align_batch_to_schema_slot_bindings(
            &batch,
            &[
                Some(SlotId::new(1)),
                Some(SlotId::new(1)),
                Some(SlotId::new(2)),
            ],
            None,
        )
        .expect("align batch");

        assert_eq!(aligned.num_columns(), 3);
        let c0 = aligned
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("c0 int64");
        let c1 = aligned
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("c1 int64");
        assert_eq!(c0.value(0), 1);
        assert_eq!(c1.value(0), 1);
    }

    #[test]
    fn partition_scalar_value_normalizes_utf8_datetime_fraction() {
        let values = StringArray::from(vec![
            "2024-08-30 19:02:13.44",
            "2024-08-30 19:02:13",
            "2024-08-30T19:02:13.44Z",
            "not-a-datetime",
        ]);

        assert_eq!(
            super::partition_scalar_value_to_string(&values, 0).expect("normalize fraction"),
            "2024-08-30 19:02:13.440000"
        );
        assert_eq!(
            super::partition_scalar_value_to_string(&values, 1).expect("keep full seconds"),
            "2024-08-30 19:02:13"
        );
        assert_eq!(
            super::partition_scalar_value_to_string(&values, 2).expect("normalize rfc3339"),
            "2024-08-30 19:02:13.440000"
        );
        assert_eq!(
            super::partition_scalar_value_to_string(&values, 3).expect("keep plain text"),
            "not-a-datetime"
        );
    }

    #[test]
    fn partition_scalar_value_formats_timestamp_with_fixed_micros() {
        let micros =
            chrono::NaiveDateTime::parse_from_str("2024-08-30 19:02:13.44", "%Y-%m-%d %H:%M:%S%.f")
                .expect("parse datetime")
                .and_utc()
                .timestamp_micros();
        let values = TimestampMicrosecondArray::from(vec![micros]);
        assert_eq!(
            super::partition_scalar_value_to_string(&values, 0).expect("format timestamp"),
            "2024-08-30 19:02:13.440000"
        );
    }

    #[test]
    fn collect_partition_values_prefers_column_name_when_slot_binding_mismatches() {
        let schema = Arc::new(Schema::new(vec![
            field_with_slot_id(Field::new("ts", DataType::Utf8, true), SlotId::new(9)),
            field_with_slot_id(
                Field::new("original_ts", DataType::Utf8, true),
                SlotId::new(0),
            ),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["2024-08-30T19:02:13.44Z"])),
                Arc::new(StringArray::from(vec!["2024-08-22 23:48:13.44"])),
            ],
        )
        .expect("build partition value batch");
        let chunk = Chunk::try_new(batch).expect("build chunk");

        let values = super::collect_partition_values_from_chunk(
            &chunk,
            &[SlotId::new(0)],
            &[String::from("ts")],
        )
        .expect("collect partition values");
        assert_eq!(
            values.into_iter().collect::<Vec<_>>(),
            vec![vec!["2024-08-30 19:02:13.440000".to_string()]]
        );
    }
}
