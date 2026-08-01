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
//! Asynchronous scan runner for scan workers.
//!
//! Responsibilities:
//! - Executes scan tasks on background runtime and pushes produced chunks to scan buffers.
//! - Bridges connector scan APIs with pipeline-friendly push/pull chunk flow control.
//!
//! Key exported interfaces:
//! - Types: `ScanAsyncRunner`.
//! - Functions: `run_scan_worker`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use super::dispatch::ScanDispatchState;
use super::types::{NATIVE_ORDERED_LATE_PRUNED_UNITS, PushResult, ScanAsyncState};
use crate::common::failpoint;
use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSlotSchema, hydrate_dictionary_columns_except};
use crate::exec::expr::{ExprArena, ExprId};
use crate::exec::node::BoxedExecIter;
use crate::exec::node::scan::{ScanMorsel, ScanMorselPruneDecision, ScanNode, ScanOp};
use crate::exec::operators::FilterEncodingPolicy;
use crate::exec::operators::runtime_filter::{
    NativeOrderedLiveConsumerSet, RuntimeFilterConsumerSet,
};
use crate::exec::pipeline::schedule::observer::Observable;
use crate::exec::row_position::LakeRowPositionSpec;
use crate::exec::row_position::RowPositionSpec;
use crate::novarocks_logging::debug;
use crate::runtime::profile::{OperatorProfiles, ProfileUnit, clamp_u128_to_i64};
use arrow::array::{Array, ArrayRef, BooleanArray, Int32Array, Int64Array};
use arrow::compute::filter_record_batch;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const SLOW_SCAN_PROGRESS_THRESHOLD: Duration = Duration::from_secs(5);
const SLOW_SCAN_LOG_INTERVAL: Duration = Duration::from_secs(5);
const IO_TASK_EXEC_TIME: &str = "IOTaskExecTime";
const SCAN_TIME: &str = "ScanTime";
// These counters describe the core-owned residual scan conjunct. They are
// intentionally separate from runtime-filter counters because a connector may
// use the same source predicate for pruning while the core still evaluates it
// for correctness.
const SCAN_CONJUNCT_INPUT_ROWS: &str = "ScanConjunctInputRows";
const SCAN_CONJUNCT_OUTPUT_ROWS: &str = "ScanConjunctOutputRows";

type PositionedChunk = (Chunk, Option<Vec<i64>>);

struct IoExecScope {
    state: Arc<ScanAsyncState>,
    profiles: Option<OperatorProfiles>,
}

impl IoExecScope {
    fn new(state: Arc<ScanAsyncState>, profiles: Option<OperatorProfiles>) -> Self {
        let idle_ns = state.begin_io_task_exec();
        if idle_ns > 0
            && let Some(p) = profiles.as_ref()
        {
            p.unique.counter_add(
                "IOTaskWaitTime",
                ProfileUnit::TimeNs,
                clamp_u128_to_i64(idle_ns),
            );
        }
        Self { state, profiles }
    }
}

impl Drop for IoExecScope {
    fn drop(&mut self) {
        let elapsed_ns = self.state.end_io_task_exec();
        if elapsed_ns == 0 {
            return;
        }
        let Some(profiles) = self.profiles.as_ref() else {
            return;
        };
        let elapsed_ns = clamp_u128_to_i64(elapsed_ns);
        profiles
            .unique
            .counter_add(IO_TASK_EXEC_TIME, ProfileUnit::TimeNs, elapsed_ns);
        profiles
            .unique
            .counter_add(SCAN_TIME, ProfileUnit::TimeNs, elapsed_ns);
    }
}

/// Async scan runner that executes connector scan tasks and pushes produced chunks to scan buffers.
pub(super) struct ScanAsyncRunner {
    name: String,
    scan: ScanNode,
    /// Instance-materialized bound op. Morsel execution / iceberg-delete loads
    /// go through this op; `scan` supplies only static node config.
    op: Arc<dyn ScanOp>,
    dispatch: Arc<ScanDispatchState>,
    pub(super) morsel_iter: Option<BoxedExecIter>,
    pub(super) pending_chunk: Option<Chunk>,
    finished: bool,
    native_runtime_filter_consumers: Option<RuntimeFilterConsumerSet>,
    native_ordered_live_consumers: Option<NativeOrderedLiveConsumerSet>,
    conjunct_predicate: Option<ExprId>,
    conjunct_encoding_policy: Option<FilterEncodingPolicy>,
    arena: Arc<ExprArena>,
    profiles: Option<crate::runtime::profile::OperatorProfiles>,
    last_progress: Instant,
    last_log: Instant,
    current_morsel: Option<ScanMorsel>,
    driver_id: i32,
    row_position_state: Option<RowPositionState>,
    lake_row_position_state: Option<LakeRowPositionState>,
    late_pruned_units: u64,
}

struct RowPositionState {
    spec: RowPositionSpec,
    scan_range_id: i32,
}

struct LakeRowPositionState {
    spec: LakeRowPositionSpec,
    tablet_id: i64,
    range_idx: i32,
    next_row_offset: i64,
}

impl ScanAsyncRunner {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        name: String,
        scan: ScanNode,
        op: Arc<dyn ScanOp>,
        dispatch: Arc<ScanDispatchState>,
        native_runtime_filter_consumers: Option<RuntimeFilterConsumerSet>,
        native_ordered_live_consumers: Option<NativeOrderedLiveConsumerSet>,
        arena: Arc<ExprArena>,
        profiles: Option<crate::runtime::profile::OperatorProfiles>,
        driver_id: i32,
    ) -> Self {
        let conjunct_predicate = scan.conjunct_predicate();
        let conjunct_encoding_policy = conjunct_predicate
            .map(|predicate| FilterEncodingPolicy::from_predicate(&arena, predicate));
        Self {
            conjunct_predicate,
            conjunct_encoding_policy,
            name,
            scan,
            op,
            dispatch,
            morsel_iter: None,
            pending_chunk: None,
            finished: false,
            native_runtime_filter_consumers,
            native_ordered_live_consumers,
            arena,
            profiles,
            last_progress: Instant::now(),
            last_log: Instant::now(),
            current_morsel: None,
            driver_id,
            row_position_state: None,
            lake_row_position_state: None,
            late_pruned_units: 0,
        }
    }

    pub(super) fn set_native_ordered_live_consumers(
        &mut self,
        consumers: Option<NativeOrderedLiveConsumerSet>,
    ) {
        self.native_ordered_live_consumers = consumers;
    }

    pub(super) fn next_chunk(&mut self) -> Result<Option<Chunk>, String> {
        if let Some(chunk) = self.pending_chunk.take() {
            return Ok(Some(chunk));
        }
        if self.finished {
            return Ok(None);
        }

        let dispatch = Arc::clone(&self.dispatch);
        loop {
            self.maybe_log_stall("morsel");
            if self.morsel_iter.is_none() {
                let morsel = dispatch.pop_morsel();
                let Some(morsel) = morsel else {
                    self.finished = true;
                    self.current_morsel = None;
                    self.row_position_state = None;
                    self.lake_row_position_state = None;
                    self.last_progress = Instant::now();
                    return Ok(None);
                };
                let late_prune = ScanMorselPruneDecision::Keep;
                if late_prune == ScanMorselPruneDecision::Skip {
                    self.late_pruned_units = self.late_pruned_units.saturating_add(1);
                    if let Some(profiles) = self.profiles.as_ref() {
                        profiles.common.counter_add(
                            NATIVE_ORDERED_LATE_PRUNED_UNITS,
                            ProfileUnit::Unit,
                            1,
                        );
                    }
                    self.last_progress = Instant::now();
                    continue;
                }
                self.current_morsel = Some(morsel.clone());
                self.row_position_state = self.build_row_position_state(&morsel)?;
                self.lake_row_position_state = self.build_lake_row_position_state(&morsel)?;
                let start = Instant::now();
                // Preserve the old `ScanNode::execute_iter` behavior: an `Empty`
                // morsel yields an empty iterator without touching the op.
                let iter = if matches!(morsel, ScanMorsel::Empty) {
                    Box::new(std::iter::empty()) as crate::exec::node::BoxedExecIter
                } else {
                    self.op
                        .execute_iter(
                            morsel,
                            self.profiles.as_ref().map(|p| p.unique.clone()),
                            None,
                        )
                        .map_err(|e| e.to_string())?
                };
                self.morsel_iter = Some(iter);
                self.maybe_log_slow_call("morsel", "execute_iter", start);
                self.last_progress = Instant::now();
            }

            let iter = self.morsel_iter.as_mut().expect("morsel iter");
            let start = Instant::now();
            let next = iter.next();
            self.maybe_log_slow_call("morsel", "iter_next", start);
            match next {
                Some(Ok(chunk)) => {
                    self.last_progress = Instant::now();
                    if let Some(consumers) = self.native_ordered_live_consumers.as_ref() {
                        consumers.poll_updates()?;
                    }
                    failpoint::sleep_if_triggered(
                        failpoint::SCAN_CHUNK_SLEEP_AFTER_READ,
                        Duration::from_millis(25),
                    );
                    let chunk = self.append_row_position_columns(chunk)?;
                    let Some(chunk) = self.apply_conjunct_predicate(chunk)? else {
                        continue;
                    };
                    let Some(chunk) = (match self.native_ordered_live_consumers.as_ref() {
                        Some(consumers) => {
                            consumers.apply_latest_chunk_profiled(chunk, self.profiles.as_ref())?
                        }
                        None => Some(chunk),
                    }) else {
                        continue;
                    };
                    let Some(chunk) = (match self.native_runtime_filter_consumers.as_ref() {
                        Some(consumers) => {
                            consumers.apply_chunk_profiled(chunk, self.profiles.as_ref())?
                        }
                        None => Some(chunk),
                    }) else {
                        continue;
                    };
                    if !chunk.is_empty() {
                        // Check scan-level limit before returning chunk
                        if let Some(limit) = self.scan.limit() {
                            let rows = chunk.len();
                            let prev_rows = dispatch.fetch_add_output_rows(rows);
                            let total_rows = prev_rows + rows;

                            if prev_rows >= limit {
                                // Already exceeded limit, discard this chunk and stop
                                self.finished = true;
                                self.morsel_iter = None;
                                dispatch.set_reach_limit();
                                return Ok(None);
                            }

                            if total_rows >= limit {
                                // Just exceeded limit, set flag to stop picking up new morsels
                                dispatch.set_reach_limit();
                                // Still return this chunk (will be truncated by LimitOperator)
                            }
                        }
                        if let Some(profile) = self.profiles.as_ref() {
                            let rows = i64::try_from(chunk.len()).unwrap_or(i64::MAX);
                            profile
                                .unique
                                .counter_add("RowsRead", ProfileUnit::Unit, rows);
                        }
                        return Ok(Some(chunk));
                    }
                    continue;
                }
                Some(Err(err)) => {
                    self.finished = true;
                    self.last_progress = Instant::now();
                    return Err(err);
                }
                None => {
                    self.morsel_iter = None;
                    self.current_morsel = None;
                    self.row_position_state = None;
                    self.lake_row_position_state = None;
                    self.last_progress = Instant::now();
                    continue;
                }
            }
        }
    }

    #[cfg(test)]
    fn late_pruned_units_for_test(&self) -> u64 {
        self.late_pruned_units
    }

    fn apply_conjunct_predicate(&self, chunk: Chunk) -> Result<Option<Chunk>, String> {
        let Some(predicate) = self.conjunct_predicate else {
            return Ok(Some(chunk));
        };
        if chunk.is_empty() {
            return Ok(Some(chunk));
        }

        let input_rows = i64::try_from(chunk.len()).unwrap_or(i64::MAX);

        let chunk = if let Some(policy) = self.conjunct_encoding_policy.as_ref() {
            hydrate_dictionary_columns_except(&chunk, |slot_id, data_type| {
                policy.accepts_encoded_column(slot_id, data_type)
            })?
        } else {
            chunk
        };

        let predicate_array = self
            .arena
            .eval(predicate, &chunk)
            .map_err(|e| e.to_string())?;
        let filter_mask = predicate_array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| "scan conjunct predicate must return boolean array".to_string())?;
        let filtered_batch = filter_record_batch(&chunk.batch, filter_mask)
            .map_err(|e| format!("scan conjunct filter failed: {}", e))?;
        if let Some(profiles) = self.profiles.as_ref() {
            profiles
                .common
                .counter_add(SCAN_CONJUNCT_INPUT_ROWS, ProfileUnit::Unit, input_rows);
            profiles.common.counter_add(
                SCAN_CONJUNCT_OUTPUT_ROWS,
                ProfileUnit::Unit,
                i64::try_from(filtered_batch.num_rows()).unwrap_or(i64::MAX),
            );
        }
        if filtered_batch.num_rows() == 0 {
            return Ok(None);
        }
        Ok(Some(Chunk::new_like(filtered_batch, &chunk)))
    }

    fn build_row_position_state(
        &self,
        morsel: &ScanMorsel,
    ) -> Result<Option<RowPositionState>, String> {
        let Some(spec) = self.scan.row_position() else {
            return Ok(None);
        };
        if let Some(position) = morsel.connector_row_position() {
            return Ok(Some(RowPositionState {
                spec: spec.clone(),
                scan_range_id: position.scan_range_id,
            }));
        }
        Err("row position requires a connector split with provider-owned row identity".to_string())
    }

    fn build_lake_row_position_state(
        &self,
        morsel: &ScanMorsel,
    ) -> Result<Option<LakeRowPositionState>, String> {
        let Some(spec) = self.scan.lake_row_position() else {
            return Ok(None);
        };
        let (tablet_id, index) = match morsel {
            ScanMorsel::StarRocksRange { tablet_id, index } => (*tablet_id, *index),
            ScanMorsel::ConnectorSplit { index, .. } => {
                let Some(tablet_id) = self.op.storage_tablet_id(morsel)? else {
                    return Ok(None);
                };
                (tablet_id, *index)
            }
            _ => return Ok(None),
        };
        Ok(Some(LakeRowPositionState {
            spec: spec.clone(),
            tablet_id,
            range_idx: i32::try_from(index).unwrap_or(i32::MAX),
            next_row_offset: 0,
        }))
    }

    fn append_row_position_columns(&mut self, chunk: Chunk) -> Result<Chunk, String> {
        // Check lake GLM first (mutually exclusive with iceberg GLM)
        if self.row_position_state.is_none() {
            if let Some(state) = self.lake_row_position_state.as_mut() {
                return Self::append_lake_row_position_cols(state, chunk);
            }
            return Ok(chunk);
        }
        let Some(state) = self.row_position_state.as_mut() else {
            return Ok(chunk);
        };
        let row_count = chunk.len();
        if row_count == 0 {
            return Ok(chunk);
        }
        let backend_id = crate::runtime::backend_id::backend_id()
            .ok_or_else(|| "backend_id is not initialized for row position".to_string())?;
        let backend_id = i32::try_from(backend_id)
            .map_err(|_| format!("backend_id {} does not fit in int32", backend_id))?;

        let row_source_array = Arc::new(Int32Array::from(vec![backend_id; row_count])) as ArrayRef;
        let scan_range_array =
            Arc::new(Int32Array::from(vec![state.scan_range_id; row_count])) as ArrayRef;

        let row_id_array = chunk.column_by_slot_id(state.spec.row_id_slot)?;
        if row_id_array.data_type() != state.spec.row_id_field.data_type() {
            return Err(format!(
                "connector row id type {:?} does not match {:?}",
                row_id_array.data_type(),
                state.spec.row_id_field.data_type()
            ));
        }

        let mut field_map = HashMap::new();
        let chunk_schema = chunk.schema();
        for (idx, slot_schema) in chunk.chunk_schema().slots().iter().enumerate() {
            let field = chunk_schema.field(idx);
            field_map.insert(slot_schema.slot_id(), (field, slot_schema.clone()));
        }

        let output_chunk_schema = self.scan.output_chunk_schema();
        let output_slots = output_chunk_schema.slot_ids();
        let mut fields = Vec::with_capacity(output_slots.len());
        let mut columns = Vec::with_capacity(output_slots.len());
        let mut slot_schemas = Vec::with_capacity(output_slots.len());
        for slot_id in output_slots {
            if *slot_id == state.spec.row_source_slot {
                fields.push(state.spec.row_source_field.clone());
                columns.push(row_source_array.clone());
                slot_schemas.push(ChunkSlotSchema::new_with_field(
                    *slot_id,
                    state.spec.row_source_field.clone(),
                    None,
                    None,
                ));
                continue;
            }
            if *slot_id == state.spec.scan_range_slot {
                fields.push(state.spec.scan_range_field.clone());
                columns.push(scan_range_array.clone());
                slot_schemas.push(ChunkSlotSchema::new_with_field(
                    *slot_id,
                    state.spec.scan_range_field.clone(),
                    None,
                    None,
                ));
                continue;
            }
            if *slot_id == state.spec.row_id_slot {
                fields.push(state.spec.row_id_field.clone());
                columns.push(row_id_array.clone());
                slot_schemas.push(ChunkSlotSchema::new_with_field(
                    *slot_id,
                    state.spec.row_id_field.clone(),
                    None,
                    None,
                ));
                continue;
            }
            let (field, slot_schema) = field_map
                .get(slot_id)
                .ok_or_else(|| format!("missing field for slot_id {} in scan chunk", slot_id))?;
            let column = chunk.column_by_slot_id(*slot_id)?;
            fields.push(field.as_ref().clone());
            columns.push(column);
            slot_schemas.push(slot_schema.clone());
        }

        let _ = fields;
        Chunk::try_new_with_columns(Arc::new(ChunkSchema::try_new(slot_schemas)?), columns)
    }

    fn append_lake_row_position_cols(
        state: &mut LakeRowPositionState,
        chunk: Chunk,
    ) -> Result<Chunk, String> {
        let row_count = chunk.len();
        if row_count == 0 {
            return Ok(chunk);
        }
        let backend_id = crate::runtime::backend_id::backend_id()
            .ok_or_else(|| "backend_id is not initialized for lake row position".to_string())?;
        let source_id = i32::try_from(backend_id)
            .map_err(|_| format!("backend_id {} does not fit in int32", backend_id))?;

        let source_id_array = Arc::new(Int32Array::from(vec![source_id; row_count])) as ArrayRef;
        let tablet_id_array =
            Arc::new(Int64Array::from(vec![state.tablet_id; row_count])) as ArrayRef;
        let rss_id_array = Arc::new(Int32Array::from(vec![state.range_idx; row_count])) as ArrayRef;

        let start_offset = state.next_row_offset;
        let row_id_values: Vec<i64> = (0..row_count as i64).map(|i| start_offset + i).collect();
        state.next_row_offset += row_count as i64;
        let row_id_array = Arc::new(Int64Array::from(row_id_values)) as ArrayRef;

        let mut field_map = HashMap::new();
        let chunk_schema = chunk.schema();
        for (idx, slot_schema) in chunk.chunk_schema().slots().iter().enumerate() {
            let field = chunk_schema.field(idx);
            field_map.insert(slot_schema.slot_id(), (field, slot_schema.clone()));
        }

        let output_chunk_schema = chunk.chunk_schema().clone();
        // We need to use the scan's output schema, but we only have the chunk here.
        // Build output by scanning the output_chunk_schema of the ScanNode, but since we
        // don't have scan here, we reconstruct by appending virtual cols to existing cols.
        let existing_slots: Vec<_> = chunk.chunk_schema().slots().to_vec();

        let mut fields = Vec::new();
        let mut columns = Vec::new();
        let mut slot_schemas_out = Vec::new();

        // First output all existing storage columns
        for (idx, slot_schema) in existing_slots.iter().enumerate() {
            let field = chunk_schema.field(idx);
            fields.push(field.clone());
            columns.push(chunk.columns()[idx].clone());
            slot_schemas_out.push(slot_schema.clone());
        }

        // Then append the four lake virtual columns
        let spec = &state.spec;

        fields.push(spec.source_id_field.clone());
        columns.push(source_id_array);
        slot_schemas_out.push(ChunkSlotSchema::new_with_field(
            spec.source_id_slot,
            spec.source_id_field.clone(),
            None,
            None,
        ));

        fields.push(spec.tablet_id_field.clone());
        columns.push(tablet_id_array);
        slot_schemas_out.push(ChunkSlotSchema::new_with_field(
            spec.tablet_id_slot,
            spec.tablet_id_field.clone(),
            None,
            None,
        ));

        fields.push(spec.rss_id_field.clone());
        columns.push(rss_id_array);
        slot_schemas_out.push(ChunkSlotSchema::new_with_field(
            spec.rss_id_slot,
            spec.rss_id_field.clone(),
            None,
            None,
        ));

        fields.push(spec.row_id_field.clone());
        columns.push(row_id_array);
        slot_schemas_out.push(ChunkSlotSchema::new_with_field(
            spec.row_id_slot,
            spec.row_id_field.clone(),
            None,
            None,
        ));

        let _ = (fields, output_chunk_schema);
        Chunk::try_new_with_columns(Arc::new(ChunkSchema::try_new(slot_schemas_out)?), columns)
    }

    fn maybe_log_stall(&mut self, mode: &str) {
        let now = Instant::now();
        let stalled_for = now.duration_since(self.last_progress);
        if stalled_for < SLOW_SCAN_PROGRESS_THRESHOLD {
            return;
        }
        if now.duration_since(self.last_log) < SLOW_SCAN_LOG_INTERVAL {
            return;
        }
        let morsel = self.current_morsel.as_ref().map(|m| m.describe());
        match morsel {
            Some(morsel) => debug!(
                "scan_source stalled: name={} driver_id={} mode={} stalled_for={:?} morsel={}",
                self.name, self.driver_id, mode, stalled_for, morsel
            ),
            None => debug!(
                "scan_source stalled: name={} driver_id={} mode={} stalled_for={:?}",
                self.name, self.driver_id, mode, stalled_for
            ),
        }
        self.last_log = now;
    }

    fn maybe_log_slow_call(&mut self, mode: &str, action: &str, start: Instant) {
        let elapsed = start.elapsed();
        if elapsed < SLOW_SCAN_PROGRESS_THRESHOLD {
            return;
        }
        let now = Instant::now();
        if now.duration_since(self.last_log) < SLOW_SCAN_LOG_INTERVAL {
            return;
        }
        let morsel = self.current_morsel.as_ref().map(|m| m.describe());
        match morsel {
            Some(morsel) => debug!(
                "scan_source slow call: name={} driver_id={} mode={} action={} elapsed={:?} morsel={}",
                self.name, self.driver_id, mode, action, elapsed, morsel
            ),
            None => debug!(
                "scan_source slow call: name={} driver_id={} mode={} action={} elapsed={:?}",
                self.name, self.driver_id, mode, action, elapsed
            ),
        }
        self.last_log = now;
    }

    #[allow(dead_code)]
    pub(super) fn format_morsel(morsel: &ScanMorsel) -> String {
        morsel.describe()
    }
}

/// Run one scan worker loop that executes dispatched morsels and pushes produced chunks.
pub(super) fn run_scan_worker(
    state: Arc<ScanAsyncState>,
    runner_pool: Arc<Mutex<Vec<ScanAsyncRunner>>>,
    inflight: Arc<AtomicUsize>,
    inflight_observable: Arc<Observable>,
) {
    let runner = {
        let mut guard = runner_pool.lock().expect("scan runner lock");
        guard.pop()
    };
    let Some(mut runner) = runner else {
        inflight.fetch_sub(1, Ordering::AcqRel);
        let notify = inflight_observable.defer_notify();
        notify.arm();
        return;
    };
    let mut mark_finished_on_last = false;

    if state.is_canceled() {
        state.mark_finished();
        inflight.fetch_sub(1, Ordering::AcqRel);
        let notify = inflight_observable.defer_notify();
        notify.arm();
        return;
    }

    let _io_exec_scope = IoExecScope::new(Arc::clone(&state), runner.profiles.clone());

    let mut keep_runner = false;
    loop {
        if state.is_canceled() {
            state.mark_finished();
            break;
        }
        if !state.has_capacity() {
            keep_runner = true;
            break;
        }
        match runner.next_chunk() {
            Ok(Some(chunk)) => match state.push_chunk(chunk) {
                PushResult::Pushed => {}
                PushResult::Full(chunk) => {
                    keep_runner = true;
                    runner.pending_chunk = Some(chunk);
                    break;
                }
                PushResult::Canceled => {
                    state.mark_finished();
                    break;
                }
            },
            Ok(None) => {
                mark_finished_on_last = true;
                break;
            }
            Err(err) => {
                state.set_error(err);
                break;
            }
        }
    }

    if keep_runner {
        let mut guard = runner_pool.lock().expect("scan runner lock");
        guard.push(runner);
    }
    let remaining = inflight.fetch_sub(1, Ordering::AcqRel) - 1;
    if mark_finished_on_last && remaining == 0 {
        // A worker can observe queue exhaustion while other runners still keep
        // buffered chunks or an active morsel iterator in the idle runner pool.
        // Marking finished too early drops those buffered rows.
        let has_pending_runner_work = {
            let guard = runner_pool.lock().expect("scan runner lock");
            guard
                .iter()
                .any(|runner| runner.pending_chunk.is_some() || runner.morsel_iter.is_some())
        };
        if !has_pending_runner_work {
            state.mark_finished();
        }
    }
    // Wake idle drivers when inflight tasks change, so empty-range drivers can finish.
    let notify = inflight_observable.defer_notify();
    notify.arm();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSlotSchema};
    use crate::exec::expr::function::FunctionKind;
    use crate::exec::expr::{ExprArena, ExprNode, LiteralValue};
    use crate::exec::node::BoxedExecIter;
    use crate::exec::node::scan::{
        RuntimeFilterContext, ScanMorsel, ScanMorsels, ScanNode, ScanOp,
    };
    use crate::exec::operators::scan::dispatch::ScanDispatchState;
    use crate::exec::pipeline::scan::morsel::DynamicMorselQueue;
    use crate::exec::runtime_filter::{
        RUNTIME_FILTER_JOIN_MODE_BROADCAST, RuntimeBloomFilter, RuntimeEmptyFilter,
        RuntimeFilterType, RuntimeInFilter, RuntimeMembershipFilter, RuntimeMinMaxFilter,
    };
    use crate::runtime::profile::{OperatorProfiles, Profiler};
    use crate::runtime_filter::model::contract::{
        ArtifactCapability, ConsumerActivation, LateApplyGranularity,
    };
    use crate::runtime_filter::port::artifact::ArtifactBundle;
    use crate::runtime_filter::port::identity::LogicalVersion;
    use crate::runtime_filter::port::subscription::{LivePollOutcome, NonBlockingLiveSubscription};
    use arrow::array::{Array, DictionaryArray, Int8Array, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use arrow::record_batch::RecordBatch;
    use std::collections::{BTreeSet, HashMap};

    fn chunk_schema_of(schema: &Arc<Schema>, slot_ids: &[SlotId]) -> Arc<ChunkSchema> {
        ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), slot_ids)
            .expect("chunk schema")
    }

    #[derive(Clone)]
    struct EmptyScanOp;

    impl ScanOp for EmptyScanOp {
        fn execute_iter(
            &self,
            _morsel: ScanMorsel,
            _profile: Option<crate::runtime::profile::RuntimeProfile>,
            _runtime_filters: Option<&RuntimeFilterContext>,
        ) -> Result<BoxedExecIter, String> {
            Ok(Box::new(std::iter::empty()))
        }

        fn build_morsels(&self) -> Result<ScanMorsels, String> {
            Ok(ScanMorsels::new(Vec::new(), false))
        }
    }

    #[derive(Clone)]
    struct ValuesScanOp {
        values: Vec<i32>,
    }

    #[derive(Clone)]
    struct SingleChunkScanOp {
        chunk: Chunk,
    }

    #[derive(Clone)]
    struct RuntimeFilterRecordingScanOp {
        observed_min_max_counts: Arc<Mutex<Vec<usize>>>,
    }

    struct ControllableOrderedLiveSubscription {
        latest: Mutex<Option<Arc<ArtifactBundle>>>,
        polls: AtomicUsize,
    }

    struct PublishedBlockingSubscription(Arc<ArtifactBundle>);

    impl crate::runtime_filter::port::subscription::BlockingSnapshotSubscription
        for PublishedBlockingSubscription
    {
        fn acquire(
            &self,
            _timeout: Duration,
        ) -> crate::runtime_filter::port::subscription::ArtifactAcquireOutcome {
            crate::runtime_filter::port::subscription::ArtifactAcquireOutcome::Published(
                Arc::clone(&self.0),
            )
        }

        fn snapshot(&self) -> Option<Arc<ArtifactBundle>> {
            Some(Arc::clone(&self.0))
        }
    }

    impl ControllableOrderedLiveSubscription {
        fn new() -> Self {
            Self {
                latest: Mutex::new(None),
                polls: AtomicUsize::new(0),
            }
        }

        fn publish(&self, bundle: Arc<ArtifactBundle>) {
            *self.latest.lock().expect("ordered live latest lock") = Some(bundle);
        }

        fn poll_count(&self) -> usize {
            self.polls.load(Ordering::SeqCst)
        }
    }

    impl NonBlockingLiveSubscription for ControllableOrderedLiveSubscription {
        fn snapshot(&self) -> Option<Arc<ArtifactBundle>> {
            self.latest
                .lock()
                .expect("ordered live latest lock")
                .clone()
        }

        fn poll_after(&self, observed: Option<LogicalVersion>) -> LivePollOutcome {
            self.polls.fetch_add(1, Ordering::SeqCst);
            let latest = self
                .latest
                .lock()
                .expect("ordered live latest lock")
                .clone();
            match latest {
                Some(bundle) if observed.is_none_or(|observed| bundle.version() > observed) => {
                    LivePollOutcome::Updated {
                        bundle,
                        terminal: None,
                    }
                }
                Some(bundle) => LivePollOutcome::Idle {
                    latest_version: Some(bundle.version()),
                    terminal: None,
                },
                None => LivePollOutcome::Idle {
                    latest_version: None,
                    terminal: None,
                },
            }
        }
    }

    impl ScanOp for RuntimeFilterRecordingScanOp {
        fn execute_iter(
            &self,
            _morsel: ScanMorsel,
            _profile: Option<crate::runtime::profile::RuntimeProfile>,
            runtime_filters: Option<&RuntimeFilterContext>,
        ) -> Result<BoxedExecIter, String> {
            let runtime_filters =
                runtime_filters.ok_or_else(|| "missing runtime filter context".to_string())?;
            self.observed_min_max_counts
                .lock()
                .expect("observed min/max lock")
                .push(runtime_filters.min_max_filters().len());
            Ok(Box::new(std::iter::empty()))
        }

        fn build_morsels(&self) -> Result<ScanMorsels, String> {
            Ok(ScanMorsels::new(
                vec![ScanMorsel::FileRange {
                    path: "test".to_string(),
                    file_len: 0,
                    offset: 0,
                    length: 0,
                    scan_range_id: -1,
                    external_datacache: None,
                }],
                false,
            ))
        }
    }

    impl ScanOp for ValuesScanOp {
        fn execute_iter(
            &self,
            _morsel: ScanMorsel,
            _profile: Option<crate::runtime::profile::RuntimeProfile>,
            _runtime_filters: Option<&RuntimeFilterContext>,
        ) -> Result<BoxedExecIter, String> {
            let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
            let array = Arc::new(Int32Array::from(self.values.clone())) as arrow::array::ArrayRef;
            let batch = RecordBatch::try_new(schema, vec![array]).map_err(|e| e.to_string())?;
            Ok(Box::new(std::iter::once(Ok({
                let batch = batch;
                let chunk_schema =
                    crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                        batch.schema().as_ref(),
                        &[SlotId::new(1)],
                    )
                    .expect("chunk schema");
                Chunk::new_with_chunk_schema(batch, chunk_schema)
            }))))
        }

        fn build_morsels(&self) -> Result<ScanMorsels, String> {
            Ok(ScanMorsels::new(
                vec![ScanMorsel::FileRange {
                    path: "test".to_string(),
                    file_len: 0,
                    offset: 0,
                    length: 0,
                    scan_range_id: -1,
                    external_datacache: None,
                }],
                false,
            ))
        }
    }

    impl ScanOp for SingleChunkScanOp {
        fn execute_iter(
            &self,
            _morsel: ScanMorsel,
            _profile: Option<crate::runtime::profile::RuntimeProfile>,
            _runtime_filters: Option<&RuntimeFilterContext>,
        ) -> Result<BoxedExecIter, String> {
            Ok(Box::new(std::iter::once(Ok(self.chunk.clone()))))
        }

        fn build_morsels(&self) -> Result<ScanMorsels, String> {
            Ok(ScanMorsels::new(
                vec![ScanMorsel::FileRange {
                    path: "test".to_string(),
                    file_len: 0,
                    offset: 0,
                    length: 0,
                    scan_range_id: -1,
                    external_datacache: None,
                }],
                false,
            ))
        }
    }

    fn single_value_chunk(v: i32) -> Chunk {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let array = Arc::new(Int32Array::from(vec![v])) as arrow::array::ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![array]).expect("build test batch");
        {
            let batch = batch;
            let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                batch.schema().as_ref(),
                &[SlotId::new(1)],
            )
            .expect("chunk schema");
            Chunk::new_with_chunk_schema(batch, chunk_schema)
        }
    }

    fn int32_chunk(values: Vec<i32>) -> Chunk {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let array = Arc::new(Int32Array::from(values)) as arrow::array::ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![array]).expect("build test batch");
        let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[SlotId::new(1)],
        )
        .expect("chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    }

    fn ordered_live_spec(
        arena: &mut ExprArena,
        order: &Arc<crate::runtime_filter::port::ordered_bound::RuntimeOrderContract>,
        late_apply: LateApplyGranularity,
    ) -> crate::exec::node::runtime_filter::RuntimeFilterConsumerBinding {
        use crate::exec::node::runtime_filter::{
            RuntimeFilterConsumerBinding, RuntimeFilterExecutionContract,
            RuntimeFilterExecutionReduction,
        };

        let expr_id = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int64);
        RuntimeFilterConsumerBinding {
            binding_id: 2,
            channel_id: 7,
            expr_id,
            activation: ConsumerActivation::NonBlockingLive { late_apply },
            capabilities: BTreeSet::from([ArtifactCapability::OrderedRange]),
            contract: RuntimeFilterExecutionContract::Ordered {
                keys: order.keys().to_vec().into(),
                comparator_digest: order.plan_comparator_digest().get(),
                order_contract_digest: order.digest().bytes(),
            },
            reduction: RuntimeFilterExecutionReduction::TightenOrderedBound,
        }
    }

    fn ordered_live_bundle(
        order: Arc<crate::runtime_filter::port::ordered_bound::RuntimeOrderContract>,
        version: u64,
        bound: i64,
    ) -> Arc<ArtifactBundle> {
        crate::runtime_filter::exec::ordered_range_predicate::tests_support::bundle(
            order,
            Some(crate::runtime_filter::port::ordered_bound::OrderedScalar::Int64(bound)),
            LogicalVersion::new(version),
        )
    }

    fn ordered_file_morsel(path: &str) -> ScanMorsel {
        ScanMorsel::FileRange {
            path: path.to_string(),
            file_len: 1024,
            offset: 0,
            length: 1024,
            scan_range_id: -1,
            external_datacache: None,
        }
    }

    fn int64_values(chunk: &Chunk) -> Vec<i64> {
        chunk.columns()[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 values")
            .values()
            .to_vec()
    }

    fn dictionary_status_chunk(keys: Vec<Option<i32>>, values: Arc<StringArray>) -> Chunk {
        let dict = Arc::new(
            DictionaryArray::<Int32Type>::try_new(Int32Array::from(keys), values)
                .expect("build dictionary array"),
        ) as ArrayRef;
        let chunk_schema = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(1),
                Field::new("status", DataType::Utf8, true),
                None,
                None,
            )])
            .expect("chunk schema"),
        );
        Chunk::try_new_with_columns(chunk_schema, vec![dict]).expect("dictionary status chunk")
    }

    fn output_strings(chunk: &Chunk) -> Vec<Option<String>> {
        let flat = arrow::compute::cast(chunk.columns()[0].as_ref(), &DataType::Utf8)
            .expect("cast dictionary output to utf8");
        let strings = flat
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string output");
        (0..strings.len())
            .map(|idx| {
                if strings.is_null(idx) {
                    None
                } else {
                    Some(strings.value(idx).to_string())
                }
            })
            .collect()
    }

    fn in_filter(filter_id: i32, values: Vec<i32>) -> Vec<RuntimeInFilter> {
        let array = Arc::new(Int32Array::from(values)) as arrow::array::ArrayRef;
        let mut filter =
            RuntimeInFilter::new_for_test(filter_id, SlotId::new(1), array.data_type())
                .expect("in filter");
        filter
            .insert_array_for_test(&array)
            .expect("add build values");
        vec![filter]
    }

    fn string_in_filter(filter_id: i32, values: Vec<&str>) -> Vec<RuntimeInFilter> {
        let array = Arc::new(StringArray::from(values)) as arrow::array::ArrayRef;
        let mut filter =
            RuntimeInFilter::new_for_test(filter_id, SlotId::new(1), array.data_type())
                .expect("string in filter");
        filter
            .insert_array_for_test(&array)
            .expect("add string build values");
        vec![filter]
    }

    fn pruning_membership_filter(filter_id: i32, values: Vec<i32>) -> RuntimeMembershipFilter {
        let build_values = Arc::new(Int32Array::from(values)) as arrow::array::ArrayRef;
        RuntimeMembershipFilter::Bloom(
            RuntimeBloomFilter::build_from_array(
                filter_id,
                SlotId::new(1),
                RuntimeFilterType::Int32,
                &build_values,
                RUNTIME_FILTER_JOIN_MODE_BROADCAST,
            )
            .expect("build bloom filter"),
        )
    }

    fn pruning_string_membership_filter(
        filter_id: i32,
        values: Vec<&str>,
    ) -> RuntimeMembershipFilter {
        let build_values = Arc::new(StringArray::from(values)) as arrow::array::ArrayRef;
        RuntimeMembershipFilter::Bloom(
            RuntimeBloomFilter::build_from_array(
                filter_id,
                SlotId::new(1),
                RuntimeFilterType::Utf8,
                &build_values,
                RUNTIME_FILTER_JOIN_MODE_BROADCAST,
            )
            .expect("build string bloom filter"),
        )
    }

    fn passthrough_membership_filter(filter_id: i32) -> RuntimeMembershipFilter {
        let min_max =
            RuntimeMinMaxFilter::full_range(RuntimeFilterType::Int32).expect("min/max range");
        RuntimeMembershipFilter::Empty(RuntimeEmptyFilter::new(
            filter_id,
            SlotId::new(1),
            RuntimeFilterType::Int32,
            false,
            RUNTIME_FILTER_JOIN_MODE_BROADCAST,
            0,
            min_max,
        ))
    }

    fn pruning_min_max_filter(values: Vec<i32>) -> RuntimeMinMaxFilter {
        let array = Arc::new(Int32Array::from(values)) as arrow::array::ArrayRef;
        RuntimeMinMaxFilter::from_arrays(RuntimeFilterType::Int32, std::slice::from_ref(&array))
            .expect("min/max filter")
    }

    #[test]
    fn native_scan_ordered_live_polls_chunk_before_blocking_native_filter() {
        use crate::exec::node::runtime_filter::{
            RuntimeFilterConsumerBinding, RuntimeFilterExecutionContract,
            RuntimeFilterExecutionReduction,
        };
        use crate::runtime_filter::model::contract::{NullOrder, NullSemantics, SortDirection};
        use crate::runtime_filter::port::artifact::ArtifactMembershipSchema;
        use crate::runtime_filter::port::subscription::BlockingSnapshotSubscription;

        let mut arena = ExprArena::default();
        let expr_id = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let membership_schema =
            ArtifactMembershipSchema::new(&DataType::Int32, NullSemantics::NeverMatches)
                .expect("membership schema");
        let blocking_spec = RuntimeFilterConsumerBinding {
            binding_id: 11,
            channel_id: 7,
            expr_id,
            activation: ConsumerActivation::BlockingSnapshot,
            capabilities: BTreeSet::from([
                ArtifactCapability::Membership,
                ArtifactCapability::EmptyDomain,
            ]),
            contract: RuntimeFilterExecutionContract::Membership {
                canonical_schema: Arc::from(membership_schema.canonical_bytes()),
                schema_digest: membership_schema.digest().bytes(),
            },
            reduction: RuntimeFilterExecutionReduction::SetUnion,
        };
        let blocking_subscription: Arc<dyn BlockingSnapshotSubscription> =
            Arc::new(PublishedBlockingSubscription(
                crate::exec::operators::runtime_filter::tests_support::membership_bundle(&[]),
            ));
        let arena = Arc::new(arena);
        let blocking_consumers =
            crate::exec::operators::runtime_filter::RuntimeFilterConsumerSet::from_bound_for_test(
                vec![blocking_spec],
                Arc::clone(&arena),
                vec![blocking_subscription],
            );
        blocking_consumers
            .acquire_configured()
            .expect("acquire empty-domain blocking filter");

        let order = crate::runtime_filter::exec::ordered_range_predicate::tests_support::contract(
            DataType::Int32,
            SortDirection::Ascending,
            NullOrder::Last,
        );
        let ordered_spec = RuntimeFilterConsumerBinding {
            binding_id: 12,
            channel_id: 7,
            expr_id,
            activation: ConsumerActivation::NonBlockingLive {
                late_apply: LateApplyGranularity::Batch,
            },
            capabilities: BTreeSet::from([ArtifactCapability::OrderedRange]),
            contract: RuntimeFilterExecutionContract::Ordered {
                keys: order.keys().to_vec().into(),
                comparator_digest: order.plan_comparator_digest().get(),
                order_contract_digest: order.digest().bytes(),
            },
            reduction: RuntimeFilterExecutionReduction::TightenOrderedBound,
        };
        let live = Arc::new(ControllableOrderedLiveSubscription::new());
        let typed: Arc<dyn NonBlockingLiveSubscription> = live.clone();
        let ordered_consumers =
            crate::exec::operators::runtime_filter::NativeOrderedLiveConsumerSet::
                from_bound_for_test(
                    vec![ordered_spec],
                    Arc::clone(&arena),
                    vec![typed],
                );
        let op: Arc<dyn ScanOp> = Arc::new(ValuesScanOp { values: vec![1, 2] });
        let scan = ScanNode::new_for_test(Arc::clone(&op));
        let morsels = op.build_morsels().expect("build blocking-filter morsel");
        let dispatch = Arc::new(ScanDispatchState::new(DynamicMorselQueue::new(
            morsels.morsels,
            morsels.has_more,
        )));
        let mut runner = ScanAsyncRunner::new(
            "ordered-live-before-blocking".to_string(),
            scan,
            op,
            dispatch,
            Some(blocking_consumers),
            Some(ordered_consumers),
            arena,
            None,
            0,
        );

        assert!(
            runner
                .next_chunk()
                .expect("blocking native RF scan")
                .is_none(),
            "empty-domain blocking filter must remove the chunk"
        );
        assert!(
            live.poll_count() >= 1,
            "ordered live must poll for the new chunk before the blocking filter removes it"
        );
    }

    #[test]
    fn does_not_mark_finished_when_idle_pool_still_has_pending_runner_work() {
        let dispatch = Arc::new(ScanDispatchState::new(DynamicMorselQueue::new(
            Vec::new(),
            false,
        )));
        let scan_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let scan = ScanNode::new_for_test(Arc::new(EmptyScanOp))
            .with_node_id(1)
            .with_output_chunk_schema(chunk_schema_of(&scan_schema, &[SlotId::new(1)]));
        let arena = Arc::new(ExprArena::default());

        let mut pending_runner = ScanAsyncRunner::new(
            "scan".to_string(),
            scan.clone(),
            Arc::new(EmptyScanOp),
            Arc::clone(&dispatch),
            None,
            None,
            Arc::clone(&arena),
            None,
            0,
        );
        pending_runner.pending_chunk = Some(single_value_chunk(7));

        let empty_runner = ScanAsyncRunner::new(
            "scan".to_string(),
            scan,
            Arc::new(EmptyScanOp),
            Arc::clone(&dispatch),
            None,
            None,
            arena,
            None,
            1,
        );

        // Pop order is from vector tail, so put the empty runner at tail.
        let pool = Arc::new(Mutex::new(vec![pending_runner, empty_runner]));
        let state = Arc::new(ScanAsyncState::new(1, "runner-finish-test".to_string()));
        let inflight = Arc::new(AtomicUsize::new(1));
        let inflight_observable = Arc::new(Observable::new());

        run_scan_worker(
            Arc::clone(&state),
            Arc::clone(&pool),
            Arc::clone(&inflight),
            inflight_observable,
        );

        assert!(
            !state.is_finished(),
            "scan state should not finish while another runner still has pending work"
        );
        let guard = pool.lock().expect("scan runner pool lock");
        assert_eq!(guard.len(), 1);
        assert!(
            guard[0].pending_chunk.is_some(),
            "pending runner work should remain in the pool"
        );
    }

    #[test]
    fn applies_scan_conjunct_predicate_before_emitting_chunk() {
        let mut arena = ExprArena::default();
        let slot = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let literal = arena.push_typed(ExprNode::Literal(LiteralValue::Int32(3)), DataType::Int32);
        let predicate = arena.push_typed(ExprNode::Lt(slot, literal), DataType::Boolean);
        let arena = Arc::new(arena);

        let op: Arc<dyn ScanOp> = Arc::new(ValuesScanOp {
            values: vec![1, 3, 2, 4],
        });
        let scan = ScanNode::new_for_test(Arc::clone(&op))
            .with_node_id(1)
            .with_output_chunk_schema(chunk_schema_of(
                &Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)])),
                &[SlotId::new(1)],
            ))
            .with_conjunct_predicate(Some(predicate));
        let morsels = op.build_morsels().expect("build morsels");
        let dispatch = Arc::new(ScanDispatchState::new(DynamicMorselQueue::new(
            morsels.morsels,
            morsels.has_more,
        )));
        let profiler = Profiler::new("scan-conjunct-profile");
        let profiles = OperatorProfiles::new(profiler.child("SCAN (plan_node_id=1)"));

        let mut runner = ScanAsyncRunner::new(
            "scan".to_string(),
            scan,
            op,
            dispatch,
            None,
            None,
            arena,
            Some(profiles.clone()),
            0,
        );

        let chunk = runner
            .next_chunk()
            .expect("scan next chunk")
            .expect("scan chunk");
        let values = chunk
            .columns()
            .first()
            .expect("first column")
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 values");
        let actual = (0..values.len())
            .map(|idx| values.value(idx))
            .collect::<Vec<_>>();
        assert_eq!(actual, vec![1, 2]);
        assert_eq!(
            profiles.common.counter_value(SCAN_CONJUNCT_INPUT_ROWS),
            Some(4)
        );
        assert_eq!(
            profiles.common.counter_value(SCAN_CONJUNCT_OUTPUT_ROWS),
            Some(2)
        );
        assert!(
            runner.next_chunk().expect("scan eof").is_none(),
            "runner should reach EOF after single morsel"
        );
    }

    #[test]
    fn scan_conjunct_like_hydrates_dictionary_input() {
        let mut arena = ExprArena::default();
        let status = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Utf8);
        let pattern = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("P%".to_string())),
            DataType::Utf8,
        );
        let predicate = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Like,
                args: vec![status, pattern],
            },
            DataType::Boolean,
        );
        let arena = Arc::new(arena);

        let chunk = dictionary_status_chunk(
            vec![Some(0), Some(1), Some(2), None],
            Arc::new(StringArray::from(vec!["PAID", "PENDING", "web"])),
        );
        let scan_schema = Arc::new(Schema::new(vec![Field::new(
            "status",
            DataType::Utf8,
            true,
        )]));
        let op: Arc<dyn ScanOp> = Arc::new(SingleChunkScanOp { chunk });
        let scan = ScanNode::new_for_test(Arc::clone(&op))
            .with_node_id(1)
            .with_output_chunk_schema(chunk_schema_of(&scan_schema, &[SlotId::new(1)]))
            .with_conjunct_predicate(Some(predicate));
        let morsels = op.build_morsels().expect("build morsels");
        let dispatch = Arc::new(ScanDispatchState::new(DynamicMorselQueue::new(
            morsels.morsels,
            morsels.has_more,
        )));

        let mut runner = ScanAsyncRunner::new(
            "scan".to_string(),
            scan,
            op,
            dispatch,
            None,
            None,
            arena,
            None,
            0,
        );

        let output = runner
            .next_chunk()
            .expect("scan next chunk")
            .expect("scan chunk");

        assert_eq!(
            output_strings(&output),
            vec![Some("PAID".to_string()), Some("PENDING".to_string())]
        );
        assert_eq!(output.columns()[0].data_type(), &DataType::Utf8);
    }
}
