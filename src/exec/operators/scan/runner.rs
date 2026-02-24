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
use super::types::{PushResult, ScanAsyncState, ScanRuntimeFilterProbe};
use crate::exec::chunk::Chunk;
use crate::exec::chunk::field_slot_id;
use crate::exec::expr::{ExprArena, ExprId};
use crate::exec::node::BoxedExecIter;
use crate::exec::node::scan::{RuntimeFilterContext, ScanMorsel, ScanNode};
use crate::exec::pipeline::dependency::DependencyHandle;
use crate::exec::pipeline::schedule::observer::Observable;
use crate::exec::row_position::RowPositionSpec;
use crate::exec::runtime_filter::{
    RuntimeInFilter, RuntimeMembershipFilter, filter_chunk_by_in_filters_with_exprs,
    filter_chunk_by_membership_filters_with_exprs,
};
use crate::metrics;
use crate::novarocks_logging::debug;
use arrow::array::{ArrayRef, Int32Array, Int64Array};
use arrow::datatypes::Schema;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
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
const RUNTIME_FILTER_DEBUG_EVERY: u64 = 256;
const SCAN_ASYNC_WAIT_INTERVAL: Duration = Duration::from_millis(10);

fn wait_for_dependency(state: &ScanAsyncState, dep: &DependencyHandle) -> bool {
    if dep.is_ready() {
        return true;
    }
    let pair = Arc::new((Mutex::new(false), Condvar::new()));
    let pair_clone = Arc::clone(&pair);
    dep.add_waiter(Arc::new(move || {
        let (lock, cv) = &*pair_clone;
        let mut ready = lock.lock().expect("scan dependency wait lock");
        *ready = true;
        cv.notify_all();
    }));
    let (lock, cv) = &*pair;
    let mut ready = lock.lock().expect("scan dependency wait lock");
    while !*ready {
        if dep.is_ready() {
            return true;
        }
        if state.is_canceled() {
            return false;
        }
        let (guard, _) = cv
            .wait_timeout(ready, SCAN_ASYNC_WAIT_INTERVAL)
            .expect("scan dependency wait");
        ready = guard;
    }
    dep.is_ready()
}

/// Async scan runner that executes connector scan tasks and pushes produced chunks to scan buffers.
pub(super) struct ScanAsyncRunner {
    name: String,
    scan: ScanNode,
    dispatch: Arc<ScanDispatchState>,
    pub(super) morsel_iter: Option<BoxedExecIter>,
    pub(super) pending_chunk: Option<Chunk>,
    finished: bool,
    runtime_filter_probe: Option<ScanRuntimeFilterProbe>,
    runtime_filter_exprs: HashMap<i32, ExprId>,
    runtime_filters_expected: usize,
    runtime_filter_ctx: Option<Arc<RuntimeFilterContext>>,
    runtime_filters_loaded: bool,
    rf_debug_counter: u64,
    rf_debug_last_version: u64,
    arena: Arc<ExprArena>,
    profiles: Option<crate::runtime::profile::OperatorProfiles>,
    last_progress: Instant,
    last_log: Instant,
    current_morsel: Option<ScanMorsel>,
    driver_id: i32,
    row_position_state: Option<RowPositionState>,
}

struct RowPositionState {
    spec: RowPositionSpec,
    scan_range_id: i32,
    first_row_id: i64,
    next_row_offset: i64,
}

impl ScanAsyncRunner {
    pub(super) fn new(
        name: String,
        scan: ScanNode,
        dispatch: Arc<ScanDispatchState>,
        runtime_filter_probe: Option<ScanRuntimeFilterProbe>,
        runtime_filter_exprs: HashMap<i32, ExprId>,
        runtime_filters_expected: usize,
        arena: Arc<ExprArena>,
        profiles: Option<crate::runtime::profile::OperatorProfiles>,
        driver_id: i32,
    ) -> Self {
        Self {
            name,
            scan,
            dispatch,
            morsel_iter: None,
            pending_chunk: None,
            finished: false,
            runtime_filter_probe,
            runtime_filter_exprs,
            runtime_filters_expected,
            runtime_filter_ctx: None,
            runtime_filters_loaded: false,
            rf_debug_counter: 0,
            rf_debug_last_version: 0,
            arena,
            profiles,
            last_progress: Instant::now(),
            last_log: Instant::now(),
            current_morsel: None,
            driver_id,
            row_position_state: None,
        }
    }

    pub(super) fn prepare_runtime_filters(&mut self, state: &ScanAsyncState) -> Result<(), String> {
        if self.runtime_filters_loaded {
            return Ok(());
        }
        let Some(rf) = self.runtime_filter_probe.as_ref() else {
            self.runtime_filter_ctx = None;
            self.runtime_filters_loaded = true;
            return Ok(());
        };
        if self.runtime_filters_expected == 0 {
            self.runtime_filter_ctx = None;
            self.runtime_filters_loaded = true;
            return Ok(());
        }
        while let Some(dep) = rf.dependency_or_timeout() {
            if !wait_for_dependency(state, &dep) {
                return Err("scan canceled while waiting for runtime filters".to_string());
            }
        }
        if state.is_canceled() {
            return Err("scan canceled while waiting for runtime filters".to_string());
        }
        if let Some(profile) = self.profiles.as_ref() {
            profile.common.add_timer(JOIN_RUNTIME_FILTER_TIME);
            profile.common.add_timer(JOIN_RUNTIME_FILTER_HASH_TIME);
            profile
                .common
                .add_counter(JOIN_RUNTIME_FILTER_INPUT_ROWS, metrics::TUnit::UNIT);
            profile
                .common
                .add_counter(JOIN_RUNTIME_FILTER_OUTPUT_ROWS, metrics::TUnit::UNIT);
            profile
                .common
                .add_counter(JOIN_RUNTIME_FILTER_EVALUATE, metrics::TUnit::UNIT);
            profile
                .common
                .add_counter(RUNTIME_FILTER_NUM, metrics::TUnit::UNIT);
            profile
                .common
                .add_counter(RUNTIME_IN_FILTER_NUM, metrics::TUnit::UNIT);
        }
        let snapshot = rf.snapshot();
        if let Some(elapsed) = rf.mark_ready() {
            if let Some(profile) = self.profiles.as_ref() {
                let latency_ns = elapsed.as_nanos().min(i64::MAX as u128) as i64;
                for filter in snapshot.in_filters() {
                    let name = format!("JoinRuntimeFilter/{}/latency", filter.filter_id());
                    profile
                        .common
                        .counter_set(&name, metrics::TUnit::TIME_NS, latency_ns);
                }
                for filter in snapshot.membership_filters() {
                    let name = format!("JoinRuntimeFilter/{}/latency", filter.filter_id());
                    profile
                        .common
                        .counter_set(&name, metrics::TUnit::TIME_NS, latency_ns);
                }
            }
        }
        self.log_runtime_filters_loaded(snapshot.in_filters(), snapshot.membership_filters());
        self.runtime_filter_ctx = Some(Arc::new(RuntimeFilterContext::from_handle(rf.handle())));
        if let Some(profile) = self.profiles.as_ref() {
            let (filter_num, in_filter_num) = self
                .runtime_filter_ctx
                .as_ref()
                .map(|ctx| {
                    let snapshot = ctx.snapshot();
                    (
                        snapshot.membership_filters().len(),
                        snapshot.in_filters().len(),
                    )
                })
                .unwrap_or((0, 0));
            profile
                .common
                .counter_set(RUNTIME_FILTER_NUM, metrics::TUnit::UNIT, filter_num as i64);
            profile.common.counter_set(
                RUNTIME_IN_FILTER_NUM,
                metrics::TUnit::UNIT,
                in_filter_num as i64,
            );
        }
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
                    self.last_progress = Instant::now();
                    return Ok(None);
                };
                self.current_morsel = Some(morsel.clone());
                self.row_position_state = self.build_row_position_state(&morsel)?;
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
                    let chunk = self.append_row_position_columns(chunk)?;
                    if let Some(filtered) = self.apply_runtime_filters(chunk)? {
                        if !filtered.is_empty() {
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
                            return Ok(Some(filtered));
                        }
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
                    self.last_progress = Instant::now();
                    continue;
                }
            }
        }
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

    fn append_row_position_columns(&mut self, chunk: Chunk) -> Result<Chunk, String> {
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
        for field in chunk.schema().fields() {
            if let Some(slot_id) = field_slot_id(field)? {
                field_map.insert(slot_id, field.clone());
            }
        }

        let mut fields = Vec::with_capacity(self.scan.output_slots().len());
        let mut columns = Vec::with_capacity(self.scan.output_slots().len());
        for slot_id in self.scan.output_slots() {
            if *slot_id == state.spec.row_source_slot {
                fields.push(state.spec.row_source_field.clone());
                columns.push(row_source_array.clone());
                continue;
            }
            if *slot_id == state.spec.scan_range_slot {
                fields.push(state.spec.scan_range_field.clone());
                columns.push(scan_range_array.clone());
                continue;
            }
            if *slot_id == state.spec.row_id_slot {
                fields.push(state.spec.row_id_field.clone());
                columns.push(row_id_array.clone());
                continue;
            }
            let field = field_map
                .get(slot_id)
                .ok_or_else(|| format!("missing field for slot_id {} in scan chunk", slot_id))?;
            let column = chunk.column_by_slot_id(*slot_id)?;
            fields.push(field.as_ref().clone());
            columns.push(column);
        }

        let schema = Arc::new(Schema::new(fields));
        let batch = arrow::record_batch::RecordBatch::try_new(schema, columns)
            .map_err(|e| e.to_string())?;
        Chunk::try_new(batch)
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
        let expected_filters = self.runtime_filters_expected;
        let Some(rf) = self.runtime_filter_probe.as_ref() else {
            return Ok(Some(chunk));
        };
        if expected_filters == 0 {
            return Ok(Some(chunk));
        }
        let snapshot = rf.snapshot();
        self.rf_debug_counter = self.rf_debug_counter.wrapping_add(1);
        let version = rf.handle().version();
        if version != self.rf_debug_last_version
            || self.rf_debug_counter % RUNTIME_FILTER_DEBUG_EVERY == 0
        {
            let node_id = self.scan.node_id().unwrap_or(-1);
            debug!(
                "scan runtime filter progress: node_id={} driver_id={} version={} in_filters={} membership_filters={} expected={} counter={}",
                node_id,
                self.driver_id,
                version,
                snapshot.in_filters().len(),
                snapshot.membership_filters().len(),
                expected_filters,
                self.rf_debug_counter
            );
            self.rf_debug_last_version = version;
        }
        let input_rows = chunk.len();
        if snapshot.is_empty() {
            if let Some(profile) = self.profiles.as_ref() {
                profile.common.counter_add(
                    JOIN_RUNTIME_FILTER_INPUT_ROWS,
                    metrics::TUnit::UNIT,
                    input_rows as i64,
                );
                profile.common.counter_add(
                    JOIN_RUNTIME_FILTER_OUTPUT_ROWS,
                    metrics::TUnit::UNIT,
                    input_rows as i64,
                );
            }
            return Ok(Some(chunk));
        }
        let filters_len =
            (snapshot.in_filters().len() + snapshot.membership_filters().len()) as i64;
        let result = if let Some(profile) = self.profiles.as_ref() {
            let _timer = profile.common.scoped_timer(JOIN_RUNTIME_FILTER_TIME);
            let chunk = filter_chunk_by_membership_filters_with_exprs(
                &self.arena,
                &self.runtime_filter_exprs,
                snapshot.membership_filters(),
                chunk,
            )?;
            match chunk {
                Some(chunk) => filter_chunk_by_in_filters_with_exprs(
                    &self.arena,
                    &self.runtime_filter_exprs,
                    snapshot.in_filters(),
                    chunk,
                ),
                None => Ok(None),
            }
        } else {
            let chunk = filter_chunk_by_membership_filters_with_exprs(
                &self.arena,
                &self.runtime_filter_exprs,
                snapshot.membership_filters(),
                chunk,
            )?;
            match chunk {
                Some(chunk) => filter_chunk_by_in_filters_with_exprs(
                    &self.arena,
                    &self.runtime_filter_exprs,
                    snapshot.in_filters(),
                    chunk,
                ),
                None => Ok(None),
            }
        }?;
        if let Some(profile) = self.profiles.as_ref() {
            let output_rows = result.as_ref().map(|c| c.len()).unwrap_or(0) as i64;
            profile.common.counter_add(
                JOIN_RUNTIME_FILTER_INPUT_ROWS,
                metrics::TUnit::UNIT,
                input_rows as i64,
            );
            profile.common.counter_add(
                JOIN_RUNTIME_FILTER_OUTPUT_ROWS,
                metrics::TUnit::UNIT,
                output_rows,
            );
            if filters_len > 0 {
                profile.common.counter_add(
                    JOIN_RUNTIME_FILTER_EVALUATE,
                    metrics::TUnit::UNIT,
                    filters_len,
                );
            }
        }
        Ok(result)
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
    use crate::exec::chunk::{Chunk, field_with_slot_id};
    use crate::exec::expr::ExprArena;
    use crate::exec::node::BoxedExecIter;
    use crate::exec::node::scan::{
        RuntimeFilterContext, ScanMorsel, ScanMorsels, ScanNode, ScanOp,
    };
    use crate::exec::operators::scan::dispatch::ScanDispatchState;
    use crate::exec::pipeline::scan::morsel::DynamicMorselQueue;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::collections::HashMap;

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

    fn single_value_chunk(v: i32) -> Chunk {
        let schema = Arc::new(Schema::new(vec![field_with_slot_id(
            Field::new("v", DataType::Int32, false),
            SlotId::new(1),
        )]));
        let array = Arc::new(Int32Array::from(vec![v])) as arrow::array::ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![array]).expect("build test batch");
        Chunk::new(batch)
    }

    #[test]
    fn does_not_mark_finished_when_idle_pool_still_has_pending_runner_work() {
        let dispatch = Arc::new(ScanDispatchState::new(DynamicMorselQueue::new(
            Vec::new(),
            false,
        )));
        let scan = ScanNode::new(Arc::new(EmptyScanOp))
            .with_node_id(1)
            .with_output_slots(vec![SlotId::new(1)]);
        let arena = Arc::new(ExprArena::default());

        let mut pending_runner = ScanAsyncRunner::new(
            "scan".to_string(),
            scan.clone(),
            Arc::clone(&dispatch),
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
}
