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
use super::types::{
    PushResult, ScanAsyncState, ScanRuntimeFilterProbe, SharedRuntimeFilterDecision,
};
use crate::common::failpoint;
use crate::connector::iceberg::equality_delete::{EqualityDeleteSet, equality_delete_keep_mask};
use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSlotSchema, hydrate_dictionary_columns_except};
use crate::exec::expr::{ExprArena, ExprId};
use crate::exec::node::BoxedExecIter;
use crate::exec::node::scan::{RuntimeFilterContext, ScanMorsel, ScanNode};
use crate::exec::operators::FilterEncodingPolicy;
use crate::exec::pipeline::schedule::observer::Observable;
use crate::exec::row_position::IcebergVirtualSpec;
use crate::exec::row_position::LakeRowPositionSpec;
use crate::exec::row_position::RowPositionSpec;
use crate::exec::runtime_filter::{
    RuntimeFilterDictionaryFoldCache, RuntimeInFilter, RuntimeMembershipFilter,
    filter_chunk_by_in_filters_with_exprs_and_dict_cache,
    filter_chunk_by_membership_filters_with_exprs_and_dict_cache,
    filter_chunk_by_min_max_filters_with_exprs_and_dict_cache,
};
use crate::novarocks_logging::debug;
use crate::runtime::profile::{OperatorProfiles, ProfileUnit, clamp_u128_to_i64};
use crate::runtime::runtime_filter_hub::{AcquiredRuntimeFilters, RuntimeFilterSnapshot};
use crate::runtime::runtime_filter_observability::RfLifecycleHandle;
use arrow::array::{Array, ArrayRef, BooleanArray, Int32Array, Int64Array, StringArray};
use arrow::compute::filter_record_batch;
use roaring::RoaringTreemap;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const SLOW_SCAN_PROGRESS_THRESHOLD: Duration = Duration::from_secs(5);
const SLOW_SCAN_LOG_INTERVAL: Duration = Duration::from_secs(5);
const JOIN_RUNTIME_FILTER_TIME: &str = "JoinRuntimeFilterTime";
const JOIN_RUNTIME_FILTER_HASH_TIME: &str = "JoinRuntimeFilterHashTime";
const JOIN_RUNTIME_FILTER_INPUT_ROWS: &str = "JoinRuntimeFilterInputRows";
const JOIN_RUNTIME_FILTER_OUTPUT_ROWS: &str = "JoinRuntimeFilterOutputRows";
const JOIN_RUNTIME_FILTER_EVALUATE: &str = "JoinRuntimeFilterEvaluate";
const RUNTIME_FILTER_NUM: &str = "RuntimeFilterNum";
const RUNTIME_IN_FILTER_NUM: &str = "RuntimeInFilterNum";
const RUNTIME_FILTER_PLANNED: &str = "RuntimeFilterPlanned";
const RUNTIME_FILTER_COMPLETE: &str = "RuntimeFilterComplete";
const RUNTIME_FILTER_UNAVAILABLE: &str = "RuntimeFilterUnavailable";
const IO_TASK_EXEC_TIME: &str = "IOTaskExecTime";
const SCAN_TIME: &str = "ScanTime";

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
    dispatch: Arc<ScanDispatchState>,
    pub(super) morsel_iter: Option<BoxedExecIter>,
    pub(super) pending_chunk: Option<Chunk>,
    finished: bool,
    shared_runtime_filter_decision: SharedRuntimeFilterDecision,
    runtime_filter_probe: Option<ScanRuntimeFilterProbe>,
    runtime_filter_exprs: HashMap<i32, ExprId>,
    runtime_filter_dict_fold_cache: RuntimeFilterDictionaryFoldCache,
    runtime_filters_expected: usize,
    runtime_filter_lifecycle_handles: HashMap<i32, RfLifecycleHandle>,
    acquired: Option<AcquiredRuntimeFilters>,
    runtime_filter_ctx: Option<Arc<RuntimeFilterContext>>,
    runtime_filters_loaded: bool,
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
    iceberg_virtual_state: Option<IcebergVirtualState>,
    iceberg_delete_filter_state: Option<IcebergDeleteFilterState>,
    iceberg_include_position_filter_state: Option<IcebergIncludePositionFilterState>,
}

struct RowPositionState {
    spec: RowPositionSpec,
    scan_range_id: i32,
    first_row_id: i64,
    next_row_offset: i64,
}

struct LakeRowPositionState {
    spec: LakeRowPositionSpec,
    tablet_id: i64,
    range_idx: i32,
    next_row_offset: i64,
}

/// Per-scan-range state that the Iceberg `_file` / `_pos` virtual columns
/// draw from while chunks stream out.
///
/// - `file_path`: copied from the current morsel's `path` — every row in this
///   scan range shares the same `_file` value (a parquet file produces one
///   morsel in NovaRocks today; splits would need per-morsel accumulation,
///   which this struct naturally gives because state is rebuilt per morsel).
/// - `next_row_offset`: absolute row position within the underlying parquet
///   file. Starts at `first_row_id` (0 when the morsel covers the whole file)
///   and grows by the number of rows materialized so far. Predicate filters
///   run later, so `_pos` captures the pre-filter position that row-level
///   DELETE readers rely on.
/// - `first_row_id`: manifest-derived row-id origin for V3 row-lineage synthesis
///   (`_row_id` virtual column). `None` when the morsel did not carry row-lineage
///   metadata (files-only path, e.g. MV refresh).
/// - `data_sequence_number`: manifest-derived data sequence number used as the
///   fallback value for `_last_updated_sequence_number`. `None` when absent from
///   the morsel.
struct IcebergVirtualState {
    spec: IcebergVirtualSpec,
    file_path: String,
    next_row_offset: i64,
    first_row_id: Option<i64>,
    data_sequence_number: Option<i64>,
    change_op: Option<i8>,
}

/// Iceberg v2 merge-on-read state owned by the scan runner.
///
/// - `deleted`: absolute row positions within the current data file that
///   prior DELETE / UPDATE / MERGE snapshots have retired, aggregated across
///   every position-delete file the FE attached to the morsel.
/// - `next_row_offset`: mirror of `IcebergVirtualState::next_row_offset` —
///   both advance by the pre-filter chunk size so they stay in sync even
///   when only one of them is active.
struct IcebergDeleteFilterState {
    deleted: RoaringTreemap,
    equality_deletes: Vec<EqualityDeleteSet>,
    next_row_offset: i64,
}

struct IcebergIncludePositionFilterState {
    included: RoaringTreemap,
    next_row_offset: i64,
}

/// Synthesize `_row_id` and `_last_updated_sequence_number` row-lineage column
/// values for one chunk.
///
/// For each row, stored column values (tagged with the Iceberg-spec reserved
/// parquet field ids) take precedence per row; NULL / absent stored values fall
/// back to the manifest-derived `first_row_id + scan_position_start + row_index`
/// and `data_sequence_number` respectively.
///
/// The optional `positions` parameter supports merge-on-read (MoR) paths where
/// rows are not contiguous. When `Some(pos)`, the fallback for row `i` uses
/// `first_row_id + pos[i]` (absolute data-file position); when `None`, the
/// sequential formula `first_row_id + scan_position_start + i` is used and
/// `positions` is ignored.
///
/// Returns two `Vec<i64>` in the order `(row_ids, seqs)`. Either vector is empty
/// when the corresponding `want_*` flag is false, which avoids allocations when
/// only one of the two columns is requested.
#[allow(clippy::too_many_arguments)]
fn synthesize_row_lineage_columns(
    schema: &arrow::datatypes::SchemaRef,
    columns: &[ArrayRef],
    num_rows: usize,
    first_row_id: i64,
    data_sequence_number: i64,
    scan_position_start: i64,
    positions: Option<&[i64]>,
    want_row_id: bool,
    want_last_updated_seq: bool,
) -> (Vec<i64>, Vec<i64>) {
    let stored_row_id_idx = if want_row_id {
        find_field_by_id(
            schema,
            crate::exec::row_position::ICEBERG_RESERVED_FIELD_ID_ROW_ID,
        )
    } else {
        None
    };
    let stored_seq_idx = if want_last_updated_seq {
        find_field_by_id(
            schema,
            crate::exec::row_position::ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
        )
    } else {
        None
    };

    let row_ids = if want_row_id {
        let stored =
            stored_row_id_idx.and_then(|idx| columns[idx].as_any().downcast_ref::<Int64Array>());
        (0..num_rows)
            .map(|i| match stored {
                Some(arr) if !arr.is_null(i) => arr.value(i),
                _ => match positions {
                    Some(pos) => first_row_id + pos[i],
                    None => first_row_id + scan_position_start + i as i64,
                },
            })
            .collect()
    } else {
        Vec::new()
    };

    let seqs = if want_last_updated_seq {
        let stored =
            stored_seq_idx.and_then(|idx| columns[idx].as_any().downcast_ref::<Int64Array>());
        (0..num_rows)
            .map(|i| match stored {
                Some(arr) if !arr.is_null(i) => arr.value(i),
                _ => data_sequence_number,
            })
            .collect()
    } else {
        Vec::new()
    };

    (row_ids, seqs)
}

/// Find the index of the parquet field with the given field-id metadata tag in
/// the schema. Returns `None` when no field carries that field-id.
fn find_field_by_id(schema: &arrow::datatypes::SchemaRef, target_id: i32) -> Option<usize> {
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    schema.fields().iter().position(|f| {
        f.metadata()
            .get(PARQUET_FIELD_ID_META_KEY)
            .and_then(|s| s.parse::<i32>().ok())
            == Some(target_id)
    })
}

impl ScanAsyncRunner {
    pub(super) fn new(
        name: String,
        scan: ScanNode,
        dispatch: Arc<ScanDispatchState>,
        shared_runtime_filter_decision: SharedRuntimeFilterDecision,
        runtime_filter_probe: Option<ScanRuntimeFilterProbe>,
        runtime_filter_exprs: HashMap<i32, ExprId>,
        runtime_filters_expected: usize,
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
            dispatch,
            morsel_iter: None,
            pending_chunk: None,
            finished: false,
            shared_runtime_filter_decision,
            runtime_filter_probe,
            runtime_filter_exprs,
            runtime_filter_dict_fold_cache: RuntimeFilterDictionaryFoldCache::default(),
            runtime_filters_expected,
            runtime_filter_lifecycle_handles: HashMap::new(),
            acquired: None,
            runtime_filter_ctx: None,
            runtime_filters_loaded: false,
            arena,
            profiles,
            last_progress: Instant::now(),
            last_log: Instant::now(),
            current_morsel: None,
            driver_id,
            row_position_state: None,
            lake_row_position_state: None,
            iceberg_virtual_state: None,
            iceberg_delete_filter_state: None,
            iceberg_include_position_filter_state: None,
        }
    }

    pub(super) fn set_runtime_filter_lifecycle_handles(
        &mut self,
        handles: HashMap<i32, RfLifecycleHandle>,
    ) {
        self.runtime_filter_lifecycle_handles = handles;
    }

    fn record_runtime_filter_acquired(&self, outcome: &str, latency_ns: i64) {
        for handle in self.runtime_filter_lifecycle_handles.values() {
            handle.acquired(outcome, latency_ns);
        }
    }

    fn record_runtime_filter_applied(&self, filter_id: i32, input_rows: usize, output_rows: usize) {
        if let Some(handle) = self.runtime_filter_lifecycle_handles.get(&filter_id) {
            handle.applied(input_rows as i64, output_rows as i64, 1);
        }
    }

    fn apply_complete_runtime_filters(
        &mut self,
        snapshot: &RuntimeFilterSnapshot,
        chunk: Chunk,
    ) -> Result<Option<Chunk>, String> {
        let mut current = Some(chunk);
        for filter in snapshot.membership_filters() {
            let Some(chunk) = current else {
                return Ok(None);
            };
            let input_rows = chunk.len();
            let filter_id = filter.filter_id();
            current = filter_chunk_by_membership_filters_with_exprs_and_dict_cache(
                &self.arena,
                &self.runtime_filter_exprs,
                std::slice::from_ref(filter),
                chunk,
                &mut self.runtime_filter_dict_fold_cache,
            )?;
            let output_rows = current.as_ref().map(|chunk| chunk.len()).unwrap_or(0);
            self.record_runtime_filter_applied(filter_id, input_rows, output_rows);
        }
        for filter in snapshot.in_filters() {
            let Some(chunk) = current else {
                return Ok(None);
            };
            let input_rows = chunk.len();
            let filter_id = filter.filter_id();
            current = filter_chunk_by_in_filters_with_exprs_and_dict_cache(
                &self.arena,
                &self.runtime_filter_exprs,
                std::slice::from_ref(filter),
                chunk,
                &mut self.runtime_filter_dict_fold_cache,
            )?;
            let output_rows = current.as_ref().map(|chunk| chunk.len()).unwrap_or(0);
            self.record_runtime_filter_applied(filter_id, input_rows, output_rows);
        }
        for (filter_id, filter) in snapshot.min_max_filters() {
            let Some(chunk) = current else {
                return Ok(None);
            };
            let input_rows = chunk.len();
            let filter_slice = [(*filter_id, Arc::clone(filter))];
            current = filter_chunk_by_min_max_filters_with_exprs_and_dict_cache(
                &self.arena,
                &self.runtime_filter_exprs,
                &filter_slice,
                chunk,
                &mut self.runtime_filter_dict_fold_cache,
            )?;
            let output_rows = current.as_ref().map(|chunk| chunk.len()).unwrap_or(0);
            self.record_runtime_filter_applied(*filter_id, input_rows, output_rows);
        }
        Ok(current)
    }

    pub(super) fn prepare_runtime_filters(&mut self, state: &ScanAsyncState) -> Result<(), String> {
        if self.runtime_filters_loaded {
            return Ok(());
        }
        let acquired = self.shared_runtime_filter_decision.get_or_acquire(
            state,
            self.runtime_filter_probe.as_ref(),
            self.runtime_filters_expected,
        )?;
        if state.is_canceled() {
            return Err("scan canceled while waiting for runtime filters".to_string());
        }
        self.install_acquired_runtime_filters(acquired)
    }

    fn install_acquired_runtime_filters(
        &mut self,
        acquired: Option<AcquiredRuntimeFilters>,
    ) -> Result<(), String> {
        let Some(acquired) = acquired else {
            self.acquired = None;
            self.runtime_filter_ctx = None;
            self.runtime_filters_loaded = true;
            return Ok(());
        };
        if let Some(profile) = self.profiles.as_ref() {
            profile.common.add_timer(JOIN_RUNTIME_FILTER_TIME);
            profile.common.add_timer(JOIN_RUNTIME_FILTER_HASH_TIME);
            profile
                .common
                .add_counter(JOIN_RUNTIME_FILTER_INPUT_ROWS, ProfileUnit::Unit);
            profile
                .common
                .add_counter(JOIN_RUNTIME_FILTER_OUTPUT_ROWS, ProfileUnit::Unit);
            profile
                .common
                .add_counter(JOIN_RUNTIME_FILTER_EVALUATE, ProfileUnit::Unit);
            profile
                .common
                .add_counter(RUNTIME_FILTER_NUM, ProfileUnit::Unit);
            profile
                .common
                .add_counter(RUNTIME_IN_FILTER_NUM, ProfileUnit::Unit);
        }
        if let AcquiredRuntimeFilters::Complete(snapshot) = &acquired {
            self.runtime_filter_dict_fold_cache.clear();
            self.runtime_filter_ctx = Some(Arc::new(RuntimeFilterContext::from_snapshot(
                snapshot.clone(),
            )));
            let ready_latency_ns = self
                .runtime_filter_probe
                .as_ref()
                .and_then(|rf| rf.mark_ready())
                .map(|elapsed| clamp_u128_to_i64(elapsed.as_nanos()));
            self.record_runtime_filter_acquired("complete", ready_latency_ns.unwrap_or(0));
            if let Some(latency_ns) = ready_latency_ns
                && let Some(profile) = self.profiles.as_ref()
            {
                for filter in snapshot.in_filters() {
                    let name = format!("JoinRuntimeFilter/{}/latency", filter.filter_id());
                    profile
                        .common
                        .counter_set(&name, ProfileUnit::TimeNs, latency_ns);
                }
                for filter in snapshot.membership_filters() {
                    let name = format!("JoinRuntimeFilter/{}/latency", filter.filter_id());
                    profile
                        .common
                        .counter_set(&name, ProfileUnit::TimeNs, latency_ns);
                }
            }
            self.log_runtime_filters_loaded(snapshot.in_filters(), snapshot.membership_filters());
            if let Some(profile) = self.profiles.as_ref() {
                profile.common.counter_set(
                    RUNTIME_FILTER_NUM,
                    ProfileUnit::Unit,
                    snapshot.membership_filters().len() as i64,
                );
                profile.common.counter_set(
                    RUNTIME_IN_FILTER_NUM,
                    ProfileUnit::Unit,
                    snapshot.in_filters().len() as i64,
                );
            }
        } else {
            if let AcquiredRuntimeFilters::Unavailable(reason) = &acquired {
                let outcome = format!("unavailable:{reason:?}");
                self.record_runtime_filter_acquired(&outcome, 0);
            }
            self.runtime_filter_ctx = None;
        }
        if let Some(profile) = self.profiles.as_ref() {
            profile
                .common
                .counter_set_unit(RUNTIME_FILTER_PLANNED, self.runtime_filters_expected as i64);
            match &acquired {
                AcquiredRuntimeFilters::Complete(snapshot) => {
                    let complete = snapshot.in_filters().len()
                        + snapshot.membership_filters().len()
                        + snapshot.min_max_filters().len();
                    profile
                        .common
                        .counter_set_unit(RUNTIME_FILTER_COMPLETE, complete as i64);
                    profile
                        .common
                        .counter_set_unit(RUNTIME_FILTER_UNAVAILABLE, 0);
                }
                AcquiredRuntimeFilters::Unavailable(_) => {
                    profile.common.counter_set_unit(RUNTIME_FILTER_COMPLETE, 0);
                    profile.common.counter_set_unit(
                        RUNTIME_FILTER_UNAVAILABLE,
                        self.runtime_filters_expected as i64,
                    );
                }
            }
        }
        self.acquired = Some(acquired);
        self.runtime_filters_loaded = true;
        Ok(())
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
                    self.iceberg_virtual_state = None;
                    self.iceberg_delete_filter_state = None;
                    self.iceberg_include_position_filter_state = None;
                    self.last_progress = Instant::now();
                    return Ok(None);
                };
                self.current_morsel = Some(morsel.clone());
                self.row_position_state = self.build_row_position_state(&morsel)?;
                self.lake_row_position_state = self.build_lake_row_position_state(&morsel);
                self.iceberg_virtual_state = self.build_iceberg_virtual_state(&morsel)?;
                self.iceberg_delete_filter_state =
                    self.build_iceberg_delete_filter_state(&morsel)?;
                self.iceberg_include_position_filter_state =
                    self.build_iceberg_include_position_filter_state(&morsel);
                let start = Instant::now();
                self.morsel_iter = Some(
                    self.scan
                        .execute_iter(
                            morsel,
                            self.profiles.as_ref().map(|p| p.unique.clone()),
                            self.runtime_filter_ctx.as_deref(),
                        )
                        .map_err(|e| e.to_string())?,
                );
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
                    failpoint::sleep_if_triggered(
                        failpoint::SCAN_CHUNK_SLEEP_AFTER_READ,
                        Duration::from_millis(25),
                    );
                    let Some((chunk, kept_positions)) =
                        self.apply_iceberg_position_delete_filter(chunk)?
                    else {
                        continue;
                    };
                    let Some((chunk, kept_positions)) =
                        self.apply_iceberg_include_position_filter(chunk, kept_positions)?
                    else {
                        continue;
                    };
                    let chunk =
                        self.append_iceberg_virtual_columns(chunk, kept_positions.as_deref())?;
                    let chunk = self.append_row_position_columns(chunk)?;
                    let Some(chunk) = self.apply_conjunct_predicate(chunk)? else {
                        continue;
                    };
                    if let Some(filtered) = self.apply_runtime_filters(chunk)?
                        && !filtered.is_empty()
                    {
                        // Check scan-level limit before returning chunk
                        if let Some(limit) = self.scan.limit() {
                            let rows = filtered.len();
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
                            let rows = i64::try_from(filtered.len()).unwrap_or(i64::MAX);
                            profile
                                .unique
                                .counter_add("RowsRead", ProfileUnit::Unit, rows);
                        }
                        return Ok(Some(filtered));
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
                    self.iceberg_virtual_state = None;
                    self.iceberg_delete_filter_state = None;
                    self.iceberg_include_position_filter_state = None;
                    self.last_progress = Instant::now();
                    continue;
                }
            }
        }
    }

    fn apply_conjunct_predicate(&self, chunk: Chunk) -> Result<Option<Chunk>, String> {
        let Some(predicate) = self.conjunct_predicate else {
            return Ok(Some(chunk));
        };
        if chunk.is_empty() {
            return Ok(Some(chunk));
        }

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
        let ScanMorsel::FileRange {
            scan_range_id,
            first_row_id,
            ..
        } = morsel
        else {
            return Err("row position requires file range morsels".to_string());
        };
        let first_row_id = first_row_id
            .ok_or_else(|| "row position requires first_row_id on scan range".to_string())?;
        Ok(Some(RowPositionState {
            spec: spec.clone(),
            scan_range_id: *scan_range_id,
            first_row_id,
            next_row_offset: 0,
        }))
    }

    fn build_lake_row_position_state(&self, morsel: &ScanMorsel) -> Option<LakeRowPositionState> {
        let spec = self.scan.lake_row_position()?;
        let ScanMorsel::StarRocksRange { tablet_id, index } = morsel else {
            return None;
        };
        Some(LakeRowPositionState {
            spec: spec.clone(),
            tablet_id: *tablet_id,
            range_idx: i32::try_from(*index).unwrap_or(i32::MAX),
            next_row_offset: 0,
        })
    }

    fn build_iceberg_virtual_state(
        &self,
        morsel: &ScanMorsel,
    ) -> Result<Option<IcebergVirtualState>, String> {
        let Some(spec) = self.scan.iceberg_virtual() else {
            return Ok(None);
        };
        let ScanMorsel::FileRange {
            path,
            first_row_id,
            data_sequence_number,
            ivm_change_op,
            ..
        } = morsel
        else {
            return Err("iceberg virtual columns require file range morsels".to_string());
        };
        Ok(Some(IcebergVirtualState {
            spec: spec.clone(),
            file_path: path.clone(),
            next_row_offset: 0,
            first_row_id: *first_row_id,
            data_sequence_number: *data_sequence_number,
            change_op: *ivm_change_op,
        }))
    }

    fn build_iceberg_delete_filter_state(
        &self,
        morsel: &ScanMorsel,
    ) -> Result<Option<IcebergDeleteFilterState>, String> {
        let ScanMorsel::FileRange { delete_files, .. } = morsel else {
            return Ok(None);
        };
        if delete_files.is_empty() {
            return Ok(None);
        }
        let deleted = self
            .scan
            .load_iceberg_position_deletes(morsel)?
            .unwrap_or_default();
        let equality_deletes = self
            .scan
            .load_iceberg_equality_deletes(morsel)?
            .unwrap_or_default();
        if deleted.is_empty() && equality_deletes.is_empty() {
            return Ok(None);
        }
        Ok(Some(IcebergDeleteFilterState {
            deleted,
            equality_deletes,
            next_row_offset: 0,
        }))
    }

    fn build_iceberg_include_position_filter_state(
        &self,
        morsel: &ScanMorsel,
    ) -> Option<IcebergIncludePositionFilterState> {
        let ScanMorsel::FileRange {
            included_positions, ..
        } = morsel
        else {
            return None;
        };
        let positions = included_positions.as_ref()?;
        let mut included = RoaringTreemap::new();
        for pos in positions {
            if *pos >= 0 {
                included.insert(*pos as u64);
            }
        }
        Some(IcebergIncludePositionFilterState {
            included,
            next_row_offset: 0,
        })
    }

    /// Apply Iceberg v2 merge-on-read filtering to the materialized chunk.
    ///
    /// Returns:
    /// - `Ok(None)` when the chunk is fully deleted by MoR; caller drops it.
    /// - `Ok(Some((chunk, None)))` when no MoR state is active — chunk
    ///   unchanged, no position list produced.
    /// - `Ok(Some((chunk, Some(kept_positions))))` when MoR filtered the
    ///   chunk; `kept_positions[i]` is the absolute data-file row position
    ///   of the `i`th surviving row, used by `_file` / `_pos` virtual column
    ///   synthesis.
    ///
    /// Advances both the MoR counter and the virtual-column counter by the
    /// pre-filter row count so that subsequent chunks remain correctly
    /// aligned with the data file even if the whole chunk is dropped.
    fn apply_iceberg_position_delete_filter(
        &mut self,
        chunk: Chunk,
    ) -> Result<Option<PositionedChunk>, String> {
        let row_count = chunk.len();
        if row_count == 0 {
            return Ok(Some((chunk, None)));
        }

        let Some(state) = self.iceberg_delete_filter_state.as_mut() else {
            // Keep the virtual-column counter in sync even when there is no
            // MoR state — done inside `append_iceberg_virtual_columns`.
            return Ok(Some((chunk, None)));
        };

        let start = state.next_row_offset;
        state.next_row_offset = state.next_row_offset.saturating_add(row_count as i64);

        // Build the boolean keep mask for the chunk. In the common case (no
        // row deleted) we can short-circuit and hand the chunk back untouched.
        let mut mask_values = Vec::with_capacity(row_count);
        for offset in 0..row_count as i64 {
            let pos = start + offset;
            let keep = pos < 0 || !state.deleted.contains(pos as u64);
            mask_values.push(keep);
        }
        if let Some(equality_keep) =
            equality_delete_keep_mask(&chunk.batch, &state.equality_deletes)?
        {
            for (keep, equality_keep) in mask_values.iter_mut().zip(equality_keep) {
                *keep = *keep && equality_keep;
            }
        }
        let kept_count = mask_values.iter().filter(|keep| **keep).count();

        if kept_count == row_count {
            // Chunk is untouched — return the original chunk but still feed
            // the kept positions to downstream virtual-column synthesis so
            // `_pos` matches the actual data-file positions.
            let kept_positions: Vec<i64> = (0..row_count as i64).map(|i| start + i).collect();
            return Ok(Some((chunk, Some(kept_positions))));
        }
        if kept_count == 0 {
            return Ok(None);
        }

        let mask = BooleanArray::from(mask_values.clone());
        let filtered_batch = filter_record_batch(&chunk.batch, &mask)
            .map_err(|e| format!("iceberg MoR filter failed: {e}"))?;
        let mut kept_positions = Vec::with_capacity(kept_count);
        for (i, keep) in mask_values.into_iter().enumerate() {
            if keep {
                kept_positions.push(start + i as i64);
            }
        }
        Ok(Some((
            Chunk::new_like(filtered_batch, &chunk),
            Some(kept_positions),
        )))
    }

    fn apply_iceberg_include_position_filter(
        &mut self,
        chunk: Chunk,
        kept_positions: Option<Vec<i64>>,
    ) -> Result<Option<PositionedChunk>, String> {
        let Some(state) = self.iceberg_include_position_filter_state.as_mut() else {
            return Ok(Some((chunk, kept_positions)));
        };
        apply_iceberg_include_position_filter(
            chunk,
            &state.included,
            &mut state.next_row_offset,
            kept_positions.as_deref(),
        )
    }

    fn append_iceberg_virtual_columns(
        &mut self,
        chunk: Chunk,
        kept_positions: Option<&[i64]>,
    ) -> Result<Chunk, String> {
        let Some(state) = self.iceberg_virtual_state.as_mut() else {
            return Ok(chunk);
        };
        let row_count = chunk.len();
        if row_count == 0 {
            return Ok(chunk);
        }

        // Pre-build the constant / row-indexed arrays up front so they can be
        // cheaply cloned into the output regardless of slot order.
        let file_path_array = state.spec.file_path_slot.map(|_| {
            Arc::new(StringArray::from(vec![state.file_path.as_str(); row_count])) as ArrayRef
        });
        let pos_array = state.spec.row_pos_slot.map(|_| {
            // When MoR has filtered the chunk, `kept_positions` holds the
            // absolute data-file position of every surviving row. Otherwise
            // the chunk is in raw file order starting at `next_row_offset`.
            if let Some(positions) = kept_positions {
                Arc::new(Int64Array::from(positions.to_vec())) as ArrayRef
            } else {
                let start = state.next_row_offset;
                let values: Vec<i64> = (0..row_count as i64).map(|i| start + i).collect();
                Arc::new(Int64Array::from(values)) as ArrayRef
            }
        });
        // `_pos` must capture the pre-filter absolute position, so advance the
        // counter by the pre-filter row count before any downstream predicates
        // drop more rows. When MoR supplied `kept_positions`, the counter has
        // already been advanced by the MoR filter — skip double-advancement in
        // that case.
        if kept_positions.is_none() {
            state.next_row_offset = state.next_row_offset.saturating_add(row_count as i64);
        }

        // V3 row-lineage synthesis: _row_id and _last_updated_sequence_number.
        // Build the value vectors before the slot-attach loop; each vector is
        // non-empty only when the corresponding slot is requested.
        //
        // MoR note: when MoR filtered the chunk, `kept_positions` holds the
        // absolute data-file row offsets of surviving rows. In that case we
        // compute per-row fallback values as `first_row_id + kept_positions[i]`
        // rather than using the sequential `scan_position_start + i` formula.
        // When no MoR is active, `next_row_offset` was already advanced by
        // `row_count` in the block above, so `scan_position_start` is
        // `next_row_offset - row_count`.
        let want_row_id = state.spec.row_id_slot.is_some();
        let want_last_updated_seq = state.spec.last_updated_seq_slot.is_some();
        let (row_ids_vec, seqs_vec) = if want_row_id || want_last_updated_seq {
            let first_row_id = if want_row_id {
                state.first_row_id.ok_or_else(|| {
                    "_row_id requested but morsel missing first_row_id; \
                 iceberg base table must be V3 row-lineage with manifest-derived ranges (not files-only path)"
                        .to_string()
                })?
            } else {
                0
            };
            let data_seq = if want_last_updated_seq {
                state.data_sequence_number.ok_or_else(|| {
                    "_last_updated_sequence_number requested but morsel missing data_sequence_number; \
                     iceberg base table must be V3 row-lineage with manifest-derived ranges (not files-only path)"
                        .to_string()
                })?
            } else {
                0
            };
            if let Some(positions) = kept_positions {
                // MoR case: pass absolute data-file positions so the helper uses
                // `first_row_id + positions[i]` as the per-row fallback.
                synthesize_row_lineage_columns(
                    &chunk.schema(),
                    chunk.columns(),
                    row_count,
                    first_row_id,
                    data_seq,
                    0, // unused when positions is Some
                    Some(positions),
                    want_row_id,
                    want_last_updated_seq,
                )
            } else {
                // Non-MoR case: rows are sequential; next_row_offset was already
                // advanced by row_count above.
                let scan_position_start = state.next_row_offset - row_count as i64;
                synthesize_row_lineage_columns(
                    &chunk.schema(),
                    chunk.columns(),
                    row_count,
                    first_row_id,
                    data_seq,
                    scan_position_start,
                    None,
                    want_row_id,
                    want_last_updated_seq,
                )
            }
        } else {
            (Vec::new(), Vec::new())
        };
        let row_id_array = state
            .spec
            .row_id_slot
            .map(|_| Arc::new(Int64Array::from(row_ids_vec)) as ArrayRef);
        let last_updated_seq_array = state
            .spec
            .last_updated_seq_slot
            .map(|_| Arc::new(Int64Array::from(seqs_vec)) as ArrayRef);
        let change_op_array = if state.spec.change_op_slot.is_some() {
            let value = state.change_op.ok_or_else(|| {
                format!(
                    "{} requested but morsel missing ivm_change_op",
                    crate::exec::row_position::CHANGE_OP_COL
                )
            })?;
            crate::exec::change_op::validate_change_op_value(value)?;
            let op = crate::exec::change_op::ChangeOp::from_i8(value)?;
            Some(crate::exec::change_op::change_op_array(op, row_count))
        } else {
            None
        };

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
            if Some(*slot_id) == state.spec.file_path_slot {
                let field = state
                    .spec
                    .file_path_field
                    .as_ref()
                    .ok_or_else(|| "iceberg _file slot missing field metadata".to_string())?;
                fields.push(field.clone());
                columns.push(
                    file_path_array
                        .as_ref()
                        .expect("file_path_array built when slot exists")
                        .clone(),
                );
                slot_schemas.push(ChunkSlotSchema::new_with_field(
                    *slot_id,
                    field.as_ref().clone(),
                    None,
                    None,
                ));
                continue;
            }
            if Some(*slot_id) == state.spec.row_pos_slot {
                let field = state
                    .spec
                    .row_pos_field
                    .as_ref()
                    .ok_or_else(|| "iceberg _pos slot missing field metadata".to_string())?;
                fields.push(field.clone());
                columns.push(
                    pos_array
                        .as_ref()
                        .expect("pos_array built when slot exists")
                        .clone(),
                );
                slot_schemas.push(ChunkSlotSchema::new_with_field(
                    *slot_id,
                    field.as_ref().clone(),
                    None,
                    None,
                ));
                continue;
            }
            if Some(*slot_id) == state.spec.row_id_slot {
                let field = state
                    .spec
                    .row_id_field
                    .as_ref()
                    .ok_or_else(|| "iceberg _row_id slot missing field metadata".to_string())?;
                fields.push(field.clone());
                columns.push(
                    row_id_array
                        .as_ref()
                        .expect("row_id_array built when slot exists")
                        .clone(),
                );
                slot_schemas.push(ChunkSlotSchema::new_with_field(
                    *slot_id,
                    field.as_ref().clone(),
                    None,
                    None,
                ));
                continue;
            }
            if Some(*slot_id) == state.spec.last_updated_seq_slot {
                let field = state.spec.last_updated_seq_field.as_ref().ok_or_else(|| {
                    "iceberg _last_updated_sequence_number slot missing field metadata".to_string()
                })?;
                fields.push(field.clone());
                columns.push(
                    last_updated_seq_array
                        .as_ref()
                        .expect("last_updated_seq_array built when slot exists")
                        .clone(),
                );
                slot_schemas.push(ChunkSlotSchema::new_with_field(
                    *slot_id,
                    field.as_ref().clone(),
                    None,
                    None,
                ));
                continue;
            }
            if Some(*slot_id) == state.spec.change_op_slot {
                let field =
                    state.spec.change_op_field.as_ref().ok_or_else(|| {
                        "iceberg __change_op slot missing field metadata".to_string()
                    })?;
                fields.push(field.clone());
                columns.push(
                    change_op_array
                        .as_ref()
                        .expect("change_op_array built when slot exists")
                        .clone(),
                );
                slot_schemas.push(ChunkSlotSchema::new_with_field(
                    *slot_id,
                    field.as_ref().clone(),
                    None,
                    None,
                ));
                continue;
            }
            let (field, slot_schema) = field_map.get(slot_id).ok_or_else(|| {
                format!(
                    "missing field for slot_id {} in iceberg virtual chunk assembly",
                    slot_id
                )
            })?;
            fields.push(field.as_ref().clone());
            columns.push(chunk.column_by_slot_id(*slot_id)?);
            slot_schemas.push(slot_schema.clone());
        }

        let _ = fields;
        Chunk::try_new_with_columns(Arc::new(ChunkSchema::try_new(slot_schemas)?), columns)
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

        let start_row_id = state.first_row_id + state.next_row_offset;
        let row_id_values = (0..row_count)
            .map(|idx| start_row_id + idx as i64)
            .collect::<Vec<_>>();
        // Row ids must be computed before runtime filters; downstream predicates will drop rows.
        state.next_row_offset = state.next_row_offset.saturating_add(row_count as i64);
        let row_id_array = Arc::new(Int64Array::from(row_id_values)) as ArrayRef;

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

    fn log_runtime_filters_loaded(
        &self,
        in_filters: &[Arc<RuntimeInFilter>],
        membership_filters: &[Arc<RuntimeMembershipFilter>],
    ) {
        let node_id = self.scan.node_id().unwrap_or(-1);
        debug!(
            "scan runtime filters loaded: node_id={} expected={} in_filters={} membership_filters={}",
            node_id,
            self.runtime_filters_expected,
            in_filters.len(),
            membership_filters.len()
        );
        for filter in in_filters {
            let filter = filter.as_ref();
            debug!(
                "scan runtime in filter: node_id={} filter_id={} slot_id={:?} empty={}",
                node_id,
                filter.filter_id(),
                filter.slot_id(),
                filter.is_empty()
            );
        }
        for filter in membership_filters {
            let filter = filter.as_ref();
            let kind = match filter {
                RuntimeMembershipFilter::Bloom(_) => "bloom",
                RuntimeMembershipFilter::Bitset(_) => "bitset",
                RuntimeMembershipFilter::Empty(_) => "empty",
            };
            debug!(
                "scan runtime membership filter: node_id={} filter_id={} kind={} slot_id={:?} ltype={:?} size={} has_null={} join_mode={} empty={}",
                node_id,
                filter.filter_id(),
                kind,
                filter.slot_id(),
                filter.ltype(),
                filter.size(),
                filter.has_null(),
                filter.join_mode(),
                filter.is_empty()
            );
        }
    }

    fn apply_runtime_filters(&mut self, chunk: Chunk) -> Result<Option<Chunk>, String> {
        let input_rows = chunk.len();
        let snapshot = match self.acquired.as_ref() {
            None => return Ok(Some(chunk)),
            Some(AcquiredRuntimeFilters::Unavailable(_)) => {
                if let Some(profile) = self.profiles.as_ref() {
                    profile.common.counter_add(
                        JOIN_RUNTIME_FILTER_INPUT_ROWS,
                        ProfileUnit::Unit,
                        input_rows as i64,
                    );
                    profile.common.counter_add(
                        JOIN_RUNTIME_FILTER_OUTPUT_ROWS,
                        ProfileUnit::Unit,
                        input_rows as i64,
                    );
                }
                return Ok(Some(chunk));
            }
            Some(AcquiredRuntimeFilters::Complete(snapshot)) => snapshot.clone(),
        };
        if snapshot.is_empty() {
            if let Some(profile) = self.profiles.as_ref() {
                profile.common.counter_add(
                    JOIN_RUNTIME_FILTER_INPUT_ROWS,
                    ProfileUnit::Unit,
                    input_rows as i64,
                );
                profile.common.counter_add(
                    JOIN_RUNTIME_FILTER_OUTPUT_ROWS,
                    ProfileUnit::Unit,
                    input_rows as i64,
                );
            }
            return Ok(Some(chunk));
        }
        let filters_len = (snapshot.in_filters().len()
            + snapshot.membership_filters().len()
            + snapshot.min_max_filters().len()) as i64;
        let result = if let Some(profile) = self.profiles.as_ref() {
            let _timer = profile.common.scoped_timer(JOIN_RUNTIME_FILTER_TIME);
            self.apply_complete_runtime_filters(&snapshot, chunk)
        } else {
            self.apply_complete_runtime_filters(&snapshot, chunk)
        }?;
        if let Some(profile) = self.profiles.as_ref() {
            let output_rows = result.as_ref().map(|c| c.len()).unwrap_or(0) as i64;
            profile.common.counter_add(
                JOIN_RUNTIME_FILTER_INPUT_ROWS,
                ProfileUnit::Unit,
                input_rows as i64,
            );
            profile.common.counter_add(
                JOIN_RUNTIME_FILTER_OUTPUT_ROWS,
                ProfileUnit::Unit,
                output_rows,
            );
            if filters_len > 0 {
                profile.common.counter_add(
                    JOIN_RUNTIME_FILTER_EVALUATE,
                    ProfileUnit::Unit,
                    filters_len,
                );
            }
        }
        Ok(result)
    }
}

fn apply_iceberg_include_position_filter(
    chunk: Chunk,
    included: &RoaringTreemap,
    next_row_offset: &mut i64,
    kept_positions: Option<&[i64]>,
) -> Result<Option<PositionedChunk>, String> {
    let row_count = chunk.len();
    if row_count == 0 {
        return Ok(Some((
            chunk,
            kept_positions.map(std::borrow::ToOwned::to_owned),
        )));
    }

    let positions = if let Some(positions) = kept_positions {
        if positions.len() != row_count {
            return Err(format!(
                "iceberg include-position filter positions length {} does not match chunk rows {}",
                positions.len(),
                row_count
            ));
        }
        positions.to_vec()
    } else {
        let start = *next_row_offset;
        *next_row_offset = next_row_offset.saturating_add(row_count as i64);
        (0..row_count as i64)
            .map(|offset| start + offset)
            .collect::<Vec<_>>()
    };

    let mut mask_values = Vec::with_capacity(row_count);
    let mut included_positions = Vec::new();
    for pos in positions {
        let keep = pos >= 0 && included.contains(pos as u64);
        mask_values.push(keep);
        if keep {
            included_positions.push(pos);
        }
    }
    if included_positions.is_empty() {
        return Ok(None);
    }
    if included_positions.len() == row_count {
        return Ok(Some((chunk, Some(included_positions))));
    }

    let mask = BooleanArray::from(mask_values);
    let filtered_batch = filter_record_batch(&chunk.batch, &mask)
        .map_err(|e| format!("iceberg include-position filter failed: {e}"))?;
    Ok(Some((
        Chunk::new_like(filtered_batch, &chunk),
        Some(included_positions),
    )))
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

    if let Err(err) = runner.prepare_runtime_filters(state.as_ref()) {
        if state.is_canceled() {
            state.mark_finished();
        } else {
            state.set_error(err);
        }
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
    use crate::exec::operators::scan::types::SharedRuntimeFilterDecision;
    use crate::exec::pipeline::dependency::DependencyManager;
    use crate::exec::pipeline::scan::morsel::DynamicMorselQueue;
    use crate::exec::row_position::IcebergVirtualSpec;
    use crate::exec::runtime_filter::{
        LocalRuntimeInFilterSet, RUNTIME_FILTER_JOIN_MODE_BROADCAST, RuntimeBloomFilter,
        RuntimeEmptyFilter, RuntimeFilterType, RuntimeInFilter, RuntimeMembershipFilter,
        RuntimeMinMaxFilter,
    };
    use crate::runtime::query_context::QueryId;
    use crate::runtime::runtime_filter_hub::RuntimeFilterHub;
    use crate::runtime::runtime_filter_observability::{QueryKey, RuntimeFilterLifecycleRegistry};
    use arrow::array::{Array, DictionaryArray, Int8Array, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use arrow::record_batch::RecordBatch;
    use std::collections::HashMap;
    use std::thread;

    /// Helper: call the production synthesis helper from a RecordBatch fixture.
    fn synthesize(
        batch: RecordBatch,
        first_row_id: i64,
        data_sequence_number: i64,
        spec: IcebergVirtualSpec,
        scan_position_start: i64,
    ) -> (Vec<i64>, Vec<i64>) {
        let schema = batch.schema();
        let columns: Vec<ArrayRef> = batch.columns().iter().cloned().collect();
        let num_rows = batch.num_rows();
        synthesize_row_lineage_columns(
            &schema,
            &columns,
            num_rows,
            first_row_id,
            data_sequence_number,
            scan_position_start,
            None, // no MoR positions in sequential-scan unit tests
            spec.row_id_slot.is_some(),
            spec.last_updated_seq_slot.is_some(),
        )
    }

    #[test]
    fn row_lineage_synthesis_falls_back_when_stored_columns_missing() {
        let id_field = Field::new("id", DataType::Int64, false);
        let schema = Arc::new(Schema::new(vec![id_field]));
        let id = Arc::new(Int64Array::from(vec![1_i64, 2, 3])) as ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![id]).unwrap();

        let mut spec = IcebergVirtualSpec::default();
        spec.row_id_slot = Some(SlotId::new(10));
        spec.last_updated_seq_slot = Some(SlotId::new(11));
        let (row_ids, seqs) = synthesize(batch, 100, 9, spec, 0);
        assert_eq!(row_ids, vec![100, 101, 102]);
        assert_eq!(seqs, vec![9, 9, 9]);
    }

    #[test]
    fn row_id_synthesis_uses_stored_when_all_non_null() {
        use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
        let id_field = Field::new("id", DataType::Int64, false);
        let stored_field =
            Field::new("_row_id", DataType::Int64, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                crate::exec::row_position::ICEBERG_RESERVED_FIELD_ID_ROW_ID.to_string(),
            )]));
        let schema = Arc::new(Schema::new(vec![id_field, stored_field]));
        let id = Arc::new(Int64Array::from(vec![1_i64, 2, 3])) as ArrayRef;
        let stored =
            Arc::new(Int64Array::from(vec![Some(700_i64), Some(800), Some(900)])) as ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![id, stored]).unwrap();

        let mut spec = IcebergVirtualSpec::default();
        spec.row_id_slot = Some(SlotId::new(10));
        let (row_ids, _seqs) = synthesize(batch, 100, 9, spec, 0);
        assert_eq!(row_ids, vec![700, 800, 900]);
    }

    #[test]
    fn row_id_synthesis_mixed_per_row_null() {
        use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
        let stored_field =
            Field::new("_row_id", DataType::Int64, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                crate::exec::row_position::ICEBERG_RESERVED_FIELD_ID_ROW_ID.to_string(),
            )]));
        let schema = Arc::new(Schema::new(vec![stored_field]));
        let stored = Arc::new(Int64Array::from(vec![Some(700_i64), None, Some(900)])) as ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![stored]).unwrap();

        let mut spec = IcebergVirtualSpec::default();
        spec.row_id_slot = Some(SlotId::new(10));
        let (row_ids, _seqs) = synthesize(batch, 100, 9, spec, 0);
        // index 1: 100 + scan_position_start(0) + i(1) = 101
        assert_eq!(row_ids, vec![700, 101, 900]);
    }

    #[test]
    fn last_updated_seq_synthesis_uses_stored_when_present() {
        use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
        let stored_field = Field::new("_last_updated_sequence_number", DataType::Int64, true)
            .with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                crate::exec::row_position::ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER
                    .to_string(),
            )]));
        let schema = Arc::new(Schema::new(vec![stored_field]));
        let stored = Arc::new(Int64Array::from(vec![Some(11_i64), Some(12), Some(13)])) as ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![stored]).unwrap();

        let mut spec = IcebergVirtualSpec::default();
        spec.last_updated_seq_slot = Some(SlotId::new(11));
        let (_row_ids, seqs) = synthesize(batch, 100, 9, spec, 0);
        assert_eq!(seqs, vec![11, 12, 13]);
    }

    #[test]
    fn last_updated_seq_synthesis_mixed_per_row_null() {
        use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
        let stored_field = Field::new("_last_updated_sequence_number", DataType::Int64, true)
            .with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                crate::exec::row_position::ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER
                    .to_string(),
            )]));
        let schema = Arc::new(Schema::new(vec![stored_field]));
        let stored = Arc::new(Int64Array::from(vec![Some(11_i64), None, Some(13)])) as ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![stored]).unwrap();

        let mut spec = IcebergVirtualSpec::default();
        spec.last_updated_seq_slot = Some(SlotId::new(11));
        let (_row_ids, seqs) = synthesize(batch, 100, 9, spec, 0);
        assert_eq!(seqs, vec![11, 9, 13]);
    }

    #[test]
    fn row_id_synthesis_advances_with_scan_position_start() {
        let id_field = Field::new("id", DataType::Int64, false);
        let schema = Arc::new(Schema::new(vec![id_field]));
        let id = Arc::new(Int64Array::from(vec![1_i64, 2])) as ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![id]).unwrap();

        let mut spec = IcebergVirtualSpec::default();
        spec.row_id_slot = Some(SlotId::new(10));
        // Same file, second chunk: scan_position_start = 7 (rows 0..7 already produced).
        let (row_ids, _seqs) = synthesize(batch, 100, 9, spec, 7);
        assert_eq!(row_ids, vec![107, 108]);
    }

    #[test]
    fn neither_slot_requested_yields_empty_vectors() {
        let id_field = Field::new("id", DataType::Int64, false);
        let schema = Arc::new(Schema::new(vec![id_field]));
        let id = Arc::new(Int64Array::from(vec![1_i64])) as ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![id]).unwrap();

        let spec = IcebergVirtualSpec::default();
        let (row_ids, seqs) = synthesize(batch, 100, 9, spec, 0);
        assert!(row_ids.is_empty());
        assert!(seqs.is_empty());
    }

    #[test]
    fn row_id_synthesis_uses_positions_for_mor_filtered_chunk() {
        let id_field = Field::new("id", DataType::Int64, false);
        let schema = Arc::new(Schema::new(vec![id_field]));
        let id = Arc::new(Int64Array::from(vec![1_i64, 2, 3])) as ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![id]).unwrap();
        let columns: Vec<ArrayRef> = batch.columns().iter().cloned().collect();

        // Simulate MoR: rows at parquet positions 5, 8, 12 survived.
        let positions = vec![5_i64, 8, 12];
        let (row_ids, _seqs) = synthesize_row_lineage_columns(
            &batch.schema(),
            &columns,
            batch.num_rows(),
            100,
            9,
            0, // unused when positions is Some
            Some(&positions),
            true,
            false,
        );
        assert_eq!(row_ids, vec![105, 108, 112]);
    }

    #[test]
    fn row_id_synthesis_stored_wins_over_positions_in_mor_path() {
        use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
        let stored_field =
            Field::new("_row_id", DataType::Int64, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                crate::exec::row_position::ICEBERG_RESERVED_FIELD_ID_ROW_ID.to_string(),
            )]));
        let schema = Arc::new(Schema::new(vec![stored_field]));
        let stored = Arc::new(Int64Array::from(vec![Some(700_i64), None, Some(900)])) as ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![stored]).unwrap();
        let columns: Vec<ArrayRef> = batch.columns().iter().cloned().collect();

        let positions = vec![5_i64, 8, 12];
        let (row_ids, _seqs) = synthesize_row_lineage_columns(
            &batch.schema(),
            &columns,
            batch.num_rows(),
            100,
            9,
            0, // unused when positions is Some
            Some(&positions),
            true,
            false,
        );
        // Row 0: stored 700 wins. Row 1: NULL -> fallback first_row_id + positions[1] = 108.
        // Row 2: stored 900 wins.
        assert_eq!(row_ids, vec![700, 108, 900]);
    }

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
        ivm_change_op: Option<i8>,
    }

    #[derive(Clone)]
    struct SingleChunkScanOp {
        chunk: Chunk,
    }

    #[derive(Clone)]
    struct RuntimeFilterRecordingScanOp {
        observed_min_max_counts: Arc<Mutex<Vec<usize>>>,
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
                .push(runtime_filters.snapshot().min_max_filters().len());
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
                    first_row_id: None,
                    data_sequence_number: None,
                    ivm_change_op: None,
                    included_positions: None,
                    external_datacache: None,
                    delete_files: Vec::new(),
                    iceberg_file_pruning: None,
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
                    first_row_id: None,
                    data_sequence_number: None,
                    ivm_change_op: self.ivm_change_op,
                    included_positions: None,
                    external_datacache: None,
                    delete_files: Vec::new(),
                    iceberg_file_pruning: None,
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
                    first_row_id: None,
                    data_sequence_number: None,
                    ivm_change_op: None,
                    included_positions: None,
                    external_datacache: None,
                    delete_files: Vec::new(),
                    iceberg_file_pruning: None,
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
        let spec = crate::exec::node::join::JoinRuntimeFilterSpec {
            filter_id,
            expr_order: 0,
            probe_slot_id: SlotId::new(1),
            build_data_type: DataType::Int32,
            merge_nodes: Vec::new(),
            has_remote_targets: false,
        };
        let array = Arc::new(Int32Array::from(values)) as arrow::array::ArrayRef;
        let mut set =
            LocalRuntimeInFilterSet::new(std::slice::from_ref(&spec), std::slice::from_ref(&array))
                .expect("in filter set");
        set.add_build_arrays(std::slice::from_ref(&array))
            .expect("add build values");
        set.into_filters()
    }

    fn string_in_filter(filter_id: i32, values: Vec<&str>) -> Vec<RuntimeInFilter> {
        let spec = crate::exec::node::join::JoinRuntimeFilterSpec {
            filter_id,
            expr_order: 0,
            probe_slot_id: SlotId::new(1),
            build_data_type: DataType::Utf8,
            merge_nodes: Vec::new(),
            has_remote_targets: false,
        };
        let array = Arc::new(StringArray::from(values)) as arrow::array::ArrayRef;
        let mut set =
            LocalRuntimeInFilterSet::new(std::slice::from_ref(&spec), std::slice::from_ref(&array))
                .expect("string in filter set");
        set.add_build_arrays(std::slice::from_ref(&array))
            .expect("add string build values");
        set.into_filters()
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

    fn scan_runtime_filter_runner(
        filter_id: i32,
        query_key: QueryKey,
        hub: &RuntimeFilterHub,
    ) -> (ScanAsyncRunner, crate::exec::expr::ExprId) {
        let mut arena = ExprArena::default();
        let expr = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let arena = Arc::new(arena);
        hub.register_probe_specs(
            42,
            &[crate::exec::node::RuntimeFilterProbeSpec {
                filter_id,
                expr_id: expr,
                slot_id: SlotId::new(1),
                data_type: DataType::Int32,
            }],
        );
        let scan_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let scan = ScanNode::new(Arc::new(EmptyScanOp))
            .with_node_id(42)
            .with_output_chunk_schema(chunk_schema_of(&scan_schema, &[SlotId::new(1)]));
        let dispatch = Arc::new(ScanDispatchState::new(DynamicMorselQueue::new(
            Vec::new(),
            false,
        )));
        let mut runner = ScanAsyncRunner::new(
            "scan".to_string(),
            scan,
            dispatch,
            SharedRuntimeFilterDecision::new(),
            Some(ScanRuntimeFilterProbe::new(hub.register_probe(42))),
            HashMap::from([(filter_id, expr)]),
            1,
            arena,
            None,
            0,
        );
        let recorder = RuntimeFilterLifecycleRegistry::global().recorder(query_key);
        runner.set_runtime_filter_lifecycle_handles(HashMap::from([(
            filter_id,
            recorder.filter(filter_id),
        )]));
        (runner, expr)
    }

    fn scan_runtime_filter_string_runner(
        filter_id: i32,
        query_key: QueryKey,
        hub: &RuntimeFilterHub,
    ) -> (ScanAsyncRunner, crate::exec::expr::ExprId) {
        let mut arena = ExprArena::default();
        let expr = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Utf8);
        let arena = Arc::new(arena);
        hub.register_probe_specs(
            42,
            &[crate::exec::node::RuntimeFilterProbeSpec {
                filter_id,
                expr_id: expr,
                slot_id: SlotId::new(1),
                data_type: DataType::Utf8,
            }],
        );
        let scan_schema = Arc::new(Schema::new(vec![Field::new(
            "status",
            DataType::Utf8,
            true,
        )]));
        let scan = ScanNode::new(Arc::new(EmptyScanOp))
            .with_node_id(42)
            .with_output_chunk_schema(chunk_schema_of(&scan_schema, &[SlotId::new(1)]));
        let dispatch = Arc::new(ScanDispatchState::new(DynamicMorselQueue::new(
            Vec::new(),
            false,
        )));
        let mut runner = ScanAsyncRunner::new(
            "scan".to_string(),
            scan,
            dispatch,
            SharedRuntimeFilterDecision::new(),
            Some(ScanRuntimeFilterProbe::new(hub.register_probe(42))),
            HashMap::from([(filter_id, expr)]),
            1,
            arena,
            None,
            0,
        );
        let recorder = RuntimeFilterLifecycleRegistry::global().recorder(query_key);
        runner.set_runtime_filter_lifecycle_handles(HashMap::from([(
            filter_id,
            recorder.filter(filter_id),
        )]));
        (runner, expr)
    }

    #[test]
    fn scan_runtime_filter_in_reuses_dictionary_fold_for_shared_values() {
        let query_key = QueryKey::from_hi_lo(20_050, 7);
        let registry = RuntimeFilterLifecycleRegistry::global();
        registry.remove_query(query_key);
        let hub = RuntimeFilterHub::new_for_query(
            DependencyManager::new(),
            QueryId {
                hi: query_key.hi,
                lo: query_key.lo,
            },
        );
        hub.set_wait_timeouts(Some(Duration::from_secs(60)), Some(Duration::from_secs(60)));
        let (mut runner, _) = scan_runtime_filter_string_runner(7, query_key, &hub);

        let in_filters = string_in_filter(7, vec!["PAID"]);
        runner.acquired = Some(AcquiredRuntimeFilters::Complete(
            RuntimeFilterSnapshot::from_filters(in_filters, Vec::new()),
        ));

        let values = Arc::new(StringArray::from(vec!["PAID", "NEW"]));
        let base = dictionary_status_chunk(
            vec![Some(0), Some(1), Some(1), Some(0)],
            Arc::clone(&values),
        );
        let out1 = runner
            .apply_runtime_filters(base.slice(0, 2))
            .expect("apply runtime filters")
            .expect("in filter should keep rows");
        let out2 = runner
            .apply_runtime_filters(base.slice(2, 2))
            .expect("apply runtime filters")
            .expect("in filter should keep rows");

        assert_eq!(output_strings(&out1), vec![Some("PAID".to_string())]);
        assert_eq!(output_strings(&out2), vec![Some("PAID".to_string())]);
        assert_eq!(
            runner.runtime_filter_dict_fold_cache.build_count_for_test(),
            1
        );
        registry.remove_query(query_key);
    }

    #[test]
    fn scan_runtime_filter_membership_reuses_dictionary_fold_for_shared_values() {
        let query_key = QueryKey::from_hi_lo(20_051, 7);
        let registry = RuntimeFilterLifecycleRegistry::global();
        registry.remove_query(query_key);
        let hub = RuntimeFilterHub::new_for_query(
            DependencyManager::new(),
            QueryId {
                hi: query_key.hi,
                lo: query_key.lo,
            },
        );
        hub.set_wait_timeouts(Some(Duration::from_secs(60)), Some(Duration::from_secs(60)));
        let (mut runner, _) = scan_runtime_filter_string_runner(7, query_key, &hub);

        runner.acquired = Some(AcquiredRuntimeFilters::Complete(
            RuntimeFilterSnapshot::from_filters(
                Vec::new(),
                vec![pruning_string_membership_filter(7, vec!["PAID"])],
            ),
        ));

        let values = Arc::new(StringArray::from(vec!["PAID", "NEW"]));
        let base = dictionary_status_chunk(
            vec![Some(0), Some(1), Some(1), Some(0)],
            Arc::clone(&values),
        );
        let out1 = runner
            .apply_runtime_filters(base.slice(0, 2))
            .expect("apply runtime filters")
            .expect("membership filter should keep rows");
        let out2 = runner
            .apply_runtime_filters(base.slice(2, 2))
            .expect("apply runtime filters")
            .expect("membership filter should keep rows");

        assert_eq!(output_strings(&out1), vec![Some("PAID".to_string())]);
        assert_eq!(output_strings(&out2), vec![Some("PAID".to_string())]);
        assert_eq!(
            runner.runtime_filter_dict_fold_cache.build_count_for_test(),
            1
        );
        registry.remove_query(query_key);
    }

    #[test]
    fn runtime_filters_use_acquired_snapshot_after_prepare() {
        let query_key = QueryKey::from_hi_lo(20_004, 7);
        let registry = RuntimeFilterLifecycleRegistry::global();
        registry.remove_query(query_key);
        let hub = RuntimeFilterHub::new_for_query(
            DependencyManager::new(),
            crate::runtime::query_context::QueryId {
                hi: query_key.hi,
                lo: query_key.lo,
            },
        );
        hub.set_wait_timeouts(Some(Duration::from_secs(60)), Some(Duration::from_secs(60)));

        let mut arena = ExprArena::default();
        let expr = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let arena = Arc::new(arena);
        hub.register_probe_specs(
            42,
            &[crate::exec::node::RuntimeFilterProbeSpec {
                filter_id: 7,
                expr_id: expr,
                slot_id: SlotId::new(1),
                data_type: DataType::Int32,
            }],
        );
        let probe = hub.register_probe(42);
        let build_values = Arc::new(Int32Array::from(vec![1, 2, 3])) as arrow::array::ArrayRef;
        let membership = RuntimeMembershipFilter::Bloom(
            RuntimeBloomFilter::build_from_array(
                7,
                SlotId::new(1),
                RuntimeFilterType::Int32,
                &build_values,
                RUNTIME_FILTER_JOIN_MODE_BROADCAST,
            )
            .expect("build bloom filter"),
        );
        hub.publish_filters(&[], &[membership]);

        let scan_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let scan = ScanNode::new(Arc::new(EmptyScanOp))
            .with_node_id(42)
            .with_output_chunk_schema(chunk_schema_of(&scan_schema, &[SlotId::new(1)]));
        let dispatch = Arc::new(ScanDispatchState::new(DynamicMorselQueue::new(
            Vec::new(),
            false,
        )));
        let mut runtime_filter_exprs = HashMap::new();
        runtime_filter_exprs.insert(7, expr);
        let profiles = crate::runtime::profile::OperatorProfiles::new(
            crate::runtime::profile::RuntimeProfile::new("scan"),
        );
        let mut runner = ScanAsyncRunner::new(
            "scan".to_string(),
            scan,
            dispatch,
            SharedRuntimeFilterDecision::new(),
            Some(ScanRuntimeFilterProbe::new(probe)),
            runtime_filter_exprs,
            1,
            arena,
            Some(profiles.clone()),
            0,
        );
        let recorder = registry.recorder(query_key);
        runner.set_runtime_filter_lifecycle_handles(HashMap::from([(7, recorder.filter(7))]));
        let state = ScanAsyncState::new(1, "runtime-filter-snapshot-test".to_string());
        runner
            .prepare_runtime_filters(&state)
            .expect("prepare runtime filters");
        assert_eq!(
            profiles.common.counter_value(RUNTIME_FILTER_PLANNED),
            Some(1)
        );
        assert_eq!(
            profiles.common.counter_value(RUNTIME_FILTER_COMPLETE),
            Some(1)
        );
        assert_eq!(
            profiles.common.counter_value(RUNTIME_FILTER_UNAVAILABLE),
            Some(0)
        );

        hub.publish_min_max_filter(
            7,
            RuntimeMinMaxFilter::empty_range(RuntimeFilterType::Int32)
                .expect("empty min/max filter"),
        );

        let filtered = runner
            .apply_runtime_filters(int32_chunk(vec![1, 2, 3, 4]))
            .expect("apply runtime filters")
            .expect("frozen snapshot should not include late filter");
        assert_eq!(filtered.len(), 3);

        let snapshot = registry.snapshot(query_key).expect("lifecycle snapshot");
        let filter = snapshot.filters.get(&7).expect("filter lifecycle");
        assert_eq!(
            filter.acquired.as_ref().map(|info| info.outcome.as_str()),
            Some("complete")
        );
        assert_eq!(filter.applied_input_rows(), 4);
        assert_eq!(filter.applied_output_rows(), 3);
        assert_eq!(filter.applied_evals(), 1);
        registry.remove_query(query_key);
    }

    #[test]
    fn runtime_filter_lifecycle_records_scan_membership_apply() {
        let query_key = QueryKey::from_hi_lo(20_005, 7);
        let registry = RuntimeFilterLifecycleRegistry::global();
        registry.remove_query(query_key);
        let hub = RuntimeFilterHub::new_for_query(
            DependencyManager::new(),
            QueryId {
                hi: query_key.hi,
                lo: query_key.lo,
            },
        );
        hub.set_wait_timeouts(Some(Duration::from_secs(60)), Some(Duration::from_secs(60)));
        let (mut runner, _) = scan_runtime_filter_runner(7, query_key, &hub);
        hub.publish_filters(&[], &[pruning_membership_filter(7, vec![1, 2, 3])]);

        let state = ScanAsyncState::new(1, "scan-membership-lifecycle-test".to_string());
        runner
            .prepare_runtime_filters(&state)
            .expect("prepare runtime filters");
        let filtered = runner
            .apply_runtime_filters(int32_chunk(vec![1, 2, 3, 4]))
            .expect("apply runtime filters")
            .expect("membership filter should keep rows");
        assert_eq!(filtered.len(), 3);

        let snapshot = registry.snapshot(query_key).expect("lifecycle snapshot");
        let filter = snapshot.filters.get(&7).expect("filter lifecycle");
        assert_eq!(filter.applied_input_rows(), 4);
        assert_eq!(filter.applied_output_rows(), 3);
        assert_eq!(filter.applied_evals(), 1);
        registry.remove_query(query_key);
    }

    #[test]
    fn runtime_filter_lifecycle_records_scan_in_apply() {
        let query_key = QueryKey::from_hi_lo(20_006, 8);
        let registry = RuntimeFilterLifecycleRegistry::global();
        registry.remove_query(query_key);
        let hub = RuntimeFilterHub::new_for_query(
            DependencyManager::new(),
            QueryId {
                hi: query_key.hi,
                lo: query_key.lo,
            },
        );
        hub.set_wait_timeouts(Some(Duration::from_secs(60)), Some(Duration::from_secs(60)));
        let (mut runner, _) = scan_runtime_filter_runner(8, query_key, &hub);
        let in_filters = in_filter(8, vec![1, 3]);
        hub.publish_filters(&in_filters, &[passthrough_membership_filter(8)]);

        let state = ScanAsyncState::new(1, "scan-in-lifecycle-test".to_string());
        runner
            .prepare_runtime_filters(&state)
            .expect("prepare runtime filters");
        let filtered = runner
            .apply_runtime_filters(int32_chunk(vec![1, 2, 3, 4]))
            .expect("apply runtime filters")
            .expect("in filter should keep rows");
        assert_eq!(filtered.len(), 2);

        let snapshot = registry.snapshot(query_key).expect("lifecycle snapshot");
        let filter = snapshot.filters.get(&8).expect("filter lifecycle");
        assert_eq!(filter.applied_input_rows(), 8);
        assert_eq!(filter.applied_output_rows(), 6);
        assert_eq!(filter.applied_evals(), 2);
        registry.remove_query(query_key);
    }

    #[test]
    fn runtime_filter_lifecycle_records_scan_min_max_apply() {
        let query_key = QueryKey::from_hi_lo(20_007, 9);
        let registry = RuntimeFilterLifecycleRegistry::global();
        registry.remove_query(query_key);
        let hub = RuntimeFilterHub::new_for_query(
            DependencyManager::new(),
            QueryId {
                hi: query_key.hi,
                lo: query_key.lo,
            },
        );
        hub.set_wait_timeouts(Some(Duration::from_secs(60)), Some(Duration::from_secs(60)));
        let (mut runner, _) = scan_runtime_filter_runner(9, query_key, &hub);
        hub.publish_min_max_filter(9, pruning_min_max_filter(vec![1, 3]));
        hub.publish_filters(&[], &[passthrough_membership_filter(9)]);

        let state = ScanAsyncState::new(1, "scan-min-max-lifecycle-test".to_string());
        runner
            .prepare_runtime_filters(&state)
            .expect("prepare runtime filters");
        let filtered = runner
            .apply_runtime_filters(int32_chunk(vec![1, 2, 3, 4]))
            .expect("apply runtime filters")
            .expect("min/max filter should keep rows");
        assert_eq!(filtered.len(), 3);

        let snapshot = registry.snapshot(query_key).expect("lifecycle snapshot");
        let filter = snapshot.filters.get(&9).expect("filter lifecycle");
        assert_eq!(filter.applied_input_rows(), 8);
        assert_eq!(filter.applied_output_rows(), 7);
        assert_eq!(filter.applied_evals(), 2);
        registry.remove_query(query_key);
    }

    #[test]
    fn runtime_filter_lifecycle_records_scan_unavailable_acquired() {
        let query_key = QueryKey::from_hi_lo(20_008, 10);
        let registry = RuntimeFilterLifecycleRegistry::global();
        registry.remove_query(query_key);
        let hub = RuntimeFilterHub::new_for_query(
            DependencyManager::new(),
            QueryId {
                hi: query_key.hi,
                lo: query_key.lo,
            },
        );
        hub.set_wait_timeouts(None, None);
        let (mut runner, _) = scan_runtime_filter_runner(10, query_key, &hub);

        let state = ScanAsyncState::new(1, "scan-unavailable-lifecycle-test".to_string());
        runner
            .prepare_runtime_filters(&state)
            .expect("prepare runtime filters");

        let snapshot = registry.snapshot(query_key).expect("lifecycle snapshot");
        let filter = snapshot.filters.get(&10).expect("filter lifecycle");
        assert_eq!(
            filter.acquired.as_ref().map(|info| info.outcome.as_str()),
            Some("unavailable:NoWaitConfigured")
        );
        assert_eq!(
            filter.acquired.as_ref().map(|info| info.latency_ns),
            Some(0)
        );
        registry.remove_query(query_key);
    }

    #[test]
    fn runtime_filter_lifecycle_preserves_first_scan_acquire_latency() {
        let query_key = QueryKey::from_hi_lo(20_009, 11);
        let registry = RuntimeFilterLifecycleRegistry::global();
        registry.remove_query(query_key);
        let hub = Arc::new(RuntimeFilterHub::new_for_query(
            DependencyManager::new(),
            QueryId {
                hi: query_key.hi,
                lo: query_key.lo,
            },
        ));
        hub.set_wait_timeouts(Some(Duration::from_secs(60)), Some(Duration::from_secs(60)));
        let (mut first_runner, _) = scan_runtime_filter_runner(11, query_key, &hub);
        let (mut later_runner, _) = scan_runtime_filter_runner(11, query_key, &hub);
        let publisher = {
            let hub = Arc::clone(&hub);
            thread::spawn(move || {
                thread::sleep(Duration::from_millis(20));
                hub.publish_filters(&[], &[pruning_membership_filter(11, vec![1, 2, 3])]);
            })
        };

        let state = ScanAsyncState::new(1, "scan-first-acquire-lifecycle-test".to_string());
        first_runner
            .prepare_runtime_filters(&state)
            .expect("prepare first runtime filters");
        publisher.join().expect("publisher");
        let first_latency = registry
            .snapshot(query_key)
            .expect("lifecycle snapshot")
            .filters
            .get(&11)
            .and_then(|filter| filter.acquired.as_ref())
            .map(|info| info.latency_ns)
            .expect("first acquire latency");
        assert!(first_latency > 0, "first acquire should have waited");

        later_runner
            .prepare_runtime_filters(&state)
            .expect("prepare later runtime filters");
        let snapshot = registry.snapshot(query_key).expect("lifecycle snapshot");
        let filter = snapshot.filters.get(&11).expect("filter lifecycle");
        assert_eq!(
            filter.acquired.as_ref().map(|info| info.latency_ns),
            Some(first_latency)
        );
        registry.remove_query(query_key);
    }

    #[test]
    fn connector_scan_receives_frozen_runtime_filter_context() {
        let hub = RuntimeFilterHub::new(DependencyManager::new());
        hub.set_wait_timeouts(Some(Duration::from_secs(60)), Some(Duration::from_secs(60)));

        let mut arena = ExprArena::default();
        let expr = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let arena = Arc::new(arena);
        hub.register_probe_specs(
            42,
            &[crate::exec::node::RuntimeFilterProbeSpec {
                filter_id: 7,
                expr_id: expr,
                slot_id: SlotId::new(1),
                data_type: DataType::Int32,
            }],
        );
        let probe = hub.register_probe(42);
        let build_values = Arc::new(Int32Array::from(vec![1, 2, 3])) as arrow::array::ArrayRef;
        let membership = RuntimeMembershipFilter::Bloom(
            RuntimeBloomFilter::build_from_array(
                7,
                SlotId::new(1),
                RuntimeFilterType::Int32,
                &build_values,
                RUNTIME_FILTER_JOIN_MODE_BROADCAST,
            )
            .expect("build bloom filter"),
        );
        hub.publish_filters(&[], &[membership]);

        let observed_min_max_counts = Arc::new(Mutex::new(Vec::new()));
        let scan_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let scan = ScanNode::new(Arc::new(RuntimeFilterRecordingScanOp {
            observed_min_max_counts: Arc::clone(&observed_min_max_counts),
        }))
        .with_node_id(42)
        .with_output_chunk_schema(chunk_schema_of(&scan_schema, &[SlotId::new(1)]));
        let morsels = scan.build_morsels().expect("build morsels");
        let dispatch = Arc::new(ScanDispatchState::new(DynamicMorselQueue::new(
            morsels.morsels,
            morsels.has_more,
        )));
        let mut runner = ScanAsyncRunner::new(
            "scan".to_string(),
            scan,
            dispatch,
            SharedRuntimeFilterDecision::new(),
            Some(ScanRuntimeFilterProbe::new(probe)),
            HashMap::from([(7, expr)]),
            1,
            arena,
            None,
            0,
        );
        let state = ScanAsyncState::new(1, "runtime-filter-context-test".to_string());
        runner
            .prepare_runtime_filters(&state)
            .expect("prepare runtime filters");

        hub.publish_min_max_filter(
            7,
            RuntimeMinMaxFilter::empty_range(RuntimeFilterType::Int32)
                .expect("empty min/max filter"),
        );

        assert!(
            runner.next_chunk().expect("scan next chunk").is_none(),
            "recording scan has no rows"
        );
        assert_eq!(
            *observed_min_max_counts
                .lock()
                .expect("observed min/max lock"),
            vec![0],
            "connector must see the frozen prepare-time snapshot, not late handle updates"
        );
    }

    #[test]
    fn runner_uses_cached_runtime_filter_decision_for_connector_context() {
        let hub = RuntimeFilterHub::new(DependencyManager::new());
        hub.set_wait_timeouts(Some(Duration::from_secs(60)), Some(Duration::from_secs(60)));

        let mut arena = ExprArena::default();
        let expr = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let arena = Arc::new(arena);
        hub.register_probe_specs(
            42,
            &[crate::exec::node::RuntimeFilterProbeSpec {
                filter_id: 7,
                expr_id: expr,
                slot_id: SlotId::new(1),
                data_type: DataType::Int32,
            }],
        );
        let source_probe = ScanRuntimeFilterProbe::new(hub.register_probe(42));
        hub.publish_filters(&[], &[pruning_membership_filter(7, vec![1, 2, 3])]);

        let shared_decision = SharedRuntimeFilterDecision::new();
        let state = ScanAsyncState::new(1, "shared-runtime-filter-decision-test".to_string());
        let cached = shared_decision
            .get_or_acquire(&state, Some(&source_probe), 1)
            .expect("warm shared runtime filter decision");
        assert!(
            matches!(cached, Some(AcquiredRuntimeFilters::Complete(_))),
            "shared decision should cache complete runtime filters"
        );

        let observed_min_max_counts = Arc::new(Mutex::new(Vec::new()));
        let scan_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let scan = ScanNode::new(Arc::new(RuntimeFilterRecordingScanOp {
            observed_min_max_counts: Arc::clone(&observed_min_max_counts),
        }))
        .with_node_id(42)
        .with_output_chunk_schema(chunk_schema_of(&scan_schema, &[SlotId::new(1)]));
        let morsels = scan.build_morsels().expect("build morsels");
        let dispatch = Arc::new(ScanDispatchState::new(DynamicMorselQueue::new(
            morsels.morsels,
            morsels.has_more,
        )));
        let mut runner = ScanAsyncRunner::new(
            "scan".to_string(),
            scan,
            dispatch,
            shared_decision,
            None,
            HashMap::from([(7, expr)]),
            1,
            arena,
            None,
            0,
        );
        runner
            .prepare_runtime_filters(&state)
            .expect("prepare runtime filters from cached decision");

        assert!(
            runner.next_chunk().expect("scan next chunk").is_none(),
            "recording scan has no rows"
        );
        assert_eq!(
            *observed_min_max_counts
                .lock()
                .expect("observed min/max lock"),
            vec![0],
            "connector scan should receive runtime filter context from the shared decision"
        );
    }

    #[test]
    fn include_position_filter_keeps_requested_positions() {
        let chunk = int32_chunk(vec![10, 11, 12, 13]);
        let mut included = RoaringTreemap::new();
        included.insert(1);
        included.insert(3);
        let mut next_row_offset = 0;

        let (filtered, positions) =
            apply_iceberg_include_position_filter(chunk, &included, &mut next_row_offset, None)
                .expect("include filter")
                .expect("some rows survive");

        let values = filtered
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 values");
        assert_eq!(values.values(), &[11, 13]);
        assert_eq!(positions, Some(vec![1, 3]));
        assert_eq!(next_row_offset, 4);
    }

    #[test]
    fn include_position_filter_intersects_existing_kept_positions() {
        let chunk = int32_chunk(vec![10, 11, 12]);
        let mut included = RoaringTreemap::new();
        included.insert(8);
        let mut next_row_offset = 0;

        let (filtered, positions) = apply_iceberg_include_position_filter(
            chunk,
            &included,
            &mut next_row_offset,
            Some(&[5, 8, 12]),
        )
        .expect("include filter")
        .expect("some rows survive");

        let values = filtered
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 values");
        assert_eq!(values.values(), &[11]);
        assert_eq!(positions, Some(vec![8]));
        assert_eq!(next_row_offset, 0);
    }

    #[test]
    fn does_not_mark_finished_when_idle_pool_still_has_pending_runner_work() {
        let dispatch = Arc::new(ScanDispatchState::new(DynamicMorselQueue::new(
            Vec::new(),
            false,
        )));
        let scan_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let scan = ScanNode::new(Arc::new(EmptyScanOp))
            .with_node_id(1)
            .with_output_chunk_schema(chunk_schema_of(&scan_schema, &[SlotId::new(1)]));
        let arena = Arc::new(ExprArena::default());

        let mut pending_runner = ScanAsyncRunner::new(
            "scan".to_string(),
            scan.clone(),
            Arc::clone(&dispatch),
            SharedRuntimeFilterDecision::new(),
            None,
            HashMap::new(),
            0,
            Arc::clone(&arena),
            None,
            0,
        );
        pending_runner.pending_chunk = Some(single_value_chunk(7));

        let empty_runner = ScanAsyncRunner::new(
            "scan".to_string(),
            scan,
            Arc::clone(&dispatch),
            SharedRuntimeFilterDecision::new(),
            None,
            HashMap::new(),
            0,
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

        let scan = ScanNode::new(Arc::new(ValuesScanOp {
            values: vec![1, 3, 2, 4],
            ivm_change_op: None,
        }))
        .with_node_id(1)
        .with_output_chunk_schema(chunk_schema_of(
            &Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)])),
            &[SlotId::new(1)],
        ))
        .with_conjunct_predicate(Some(predicate));
        let morsels = scan.build_morsels().expect("build morsels");
        let dispatch = Arc::new(ScanDispatchState::new(DynamicMorselQueue::new(
            morsels.morsels,
            morsels.has_more,
        )));

        let mut runner = ScanAsyncRunner::new(
            "scan".to_string(),
            scan,
            dispatch,
            SharedRuntimeFilterDecision::new(),
            None,
            HashMap::new(),
            0,
            arena,
            None,
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
        let scan = ScanNode::new(Arc::new(SingleChunkScanOp { chunk }))
            .with_node_id(1)
            .with_output_chunk_schema(chunk_schema_of(&scan_schema, &[SlotId::new(1)]))
            .with_conjunct_predicate(Some(predicate));
        let morsels = scan.build_morsels().expect("build morsels");
        let dispatch = Arc::new(ScanDispatchState::new(DynamicMorselQueue::new(
            morsels.morsels,
            morsels.has_more,
        )));

        let mut runner = ScanAsyncRunner::new(
            "scan".to_string(),
            scan,
            dispatch,
            SharedRuntimeFilterDecision::new(),
            None,
            HashMap::new(),
            0,
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

    #[test]
    fn appends_change_op_virtual_column_from_morsel_metadata() {
        let arena = Arc::new(ExprArena::default());
        let schema = Arc::new(Schema::new(vec![
            Field::new("v", DataType::Int32, false),
            Field::new("__change_op", DataType::Int8, false),
        ]));
        let mut spec = IcebergVirtualSpec::default();
        spec.change_op_slot = Some(SlotId::new(2));
        spec.change_op_field = Some(Field::new("__change_op", DataType::Int8, false));
        let scan = ScanNode::new(Arc::new(ValuesScanOp {
            values: vec![1, 2, 3],
            ivm_change_op: Some(crate::exec::change_op::CHANGE_OP_DELETE),
        }))
        .with_node_id(1)
        .with_output_chunk_schema(chunk_schema_of(&schema, &[SlotId::new(1), SlotId::new(2)]))
        .with_iceberg_virtual(Some(spec));
        let morsels = scan.build_morsels().expect("build morsels");
        let dispatch = Arc::new(ScanDispatchState::new(DynamicMorselQueue::new(
            morsels.morsels,
            morsels.has_more,
        )));

        let mut runner = ScanAsyncRunner::new(
            "scan".to_string(),
            scan,
            dispatch,
            SharedRuntimeFilterDecision::new(),
            None,
            HashMap::new(),
            0,
            arena,
            None,
            0,
        );

        let chunk = runner
            .next_chunk()
            .expect("scan next chunk")
            .expect("scan chunk");
        let change_op_column = chunk
            .column_by_slot_id(SlotId::new(2))
            .expect("change op column");
        let change_ops = change_op_column
            .as_any()
            .downcast_ref::<Int8Array>()
            .expect("int8 change op values");
        let actual = (0..change_ops.len())
            .map(|idx| change_ops.value(idx))
            .collect::<Vec<_>>();
        assert_eq!(actual, vec![-1, -1, -1]);
    }
}
