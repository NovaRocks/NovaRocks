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
//! Scan source operator.
//!
//! Responsibilities:
//! - Pulls scanned chunks from async scan runners and emits them as pipeline source output.
//! - Coordinates driver blocking, scan completion, and runtime-filter probing.
//!
//! Key exported interfaces:
//! - Types: `ScanSourceFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use crate::common::config::{
    operator_buffer_chunks, scan_submit_fail_max, scan_submit_fail_timeout_ms,
};
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use crate::exec::node::scan::ScanNode;
use crate::exec::pipeline::dependency::DependencyHandle;
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::scan::morsel::DynamicMorselQueue;
use crate::exec::pipeline::schedule::observer::Observable;
use crate::runtime::runtime_filter_hub::RuntimeFilterHub;
use crate::runtime::runtime_state::RuntimeState;
use crate::runtime::scan_executor::scan_executor;
use crate::novarocks_logging::{debug, warn};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use super::dispatch::{ScanDispatchState, SharedScanState};
use super::runner::{ScanAsyncRunner, run_scan_worker};
use super::types::{ScanAsyncState, ScanRuntimeFilterProbe};

/// Factory for scan source operators that consume async scan output.
pub struct ScanSourceFactory {
    name: String,
    scan: ScanNode,
    state: SharedScanState,
    #[allow(dead_code)]
    runtime_filter_specs: Vec<crate::exec::node::RuntimeFilterProbeSpec>,
    runtime_filter_exprs: HashMap<i32, ExprId>,
    runtime_filters_expected: usize,
    local_rf_waiting_set: Vec<i32>,
    runtime_filter_hub: Arc<RuntimeFilterHub>,
    arena: Arc<ExprArena>,
}

impl ScanSourceFactory {
    pub(crate) fn new(
        scan: ScanNode,
        runtime_filter_hub: Arc<RuntimeFilterHub>,
        arena: Arc<ExprArena>,
    ) -> Self {
        let mut name = scan
            .profile_name()
            .unwrap_or_else(|| "ScanSource".to_string());
        if !name.contains("plan_node_id=") && !name.contains("(id=") {
            if let Some(node_id) = scan.node_id() {
                warn!(
                    "scan profile name missing plan_node_id, appending from scan node id: name={} node_id={}",
                    name, node_id
                );
                name = format!("{name} (id={node_id})");
            } else {
                warn!(
                    "scan profile name missing plan_node_id and node_id, using plan_node_id=-1: name={}",
                    name
                );
                name = format!("{name} (plan_node_id=-1)");
            }
        }
        let runtime_filter_specs = scan.runtime_filter_specs().to_vec();
        let runtime_filter_exprs = runtime_filter_specs
            .iter()
            .map(|spec| (spec.filter_id, spec.expr_id))
            .collect();
        let runtime_filters_expected = runtime_filter_specs.len();
        if runtime_filters_expected > 0 {
            if let Some(node_id) = scan.node_id() {
                runtime_filter_hub.register_probe_specs(node_id, &runtime_filter_specs);
            } else {
                warn!(
                    "scan runtime filter specs present but scan node_id is missing: name={}",
                    name
                );
            }
        }
        let local_rf_waiting_set = scan.local_rf_waiting_set().to_vec();
        Self {
            name,
            scan,
            state: SharedScanState::new(),
            runtime_filter_specs,
            runtime_filter_exprs,
            runtime_filters_expected,
            local_rf_waiting_set,
            runtime_filter_hub,
            arena,
        }
    }

    fn local_rf_deps(&self) -> Vec<DependencyHandle> {
        if self.local_rf_waiting_set.is_empty() {
            return Vec::new();
        }
        let mut deps = Vec::new();
        let mut seen = HashSet::new();
        for node_id in &self.local_rf_waiting_set {
            if seen.insert(*node_id) {
                if let Some(dep) = self.runtime_filter_hub.local_dependency_if_exists(*node_id) {
                    deps.push(dep);
                } else {
                    debug!(
                        "scan skip unknown local RF dependency: node_id={:?} waiting_build_node_id={}",
                        self.scan.node_id(),
                        node_id
                    );
                }
            }
        }
        deps
    }
}

impl OperatorFactory for ScanSourceFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, driver_id: i32) -> Box<dyn Operator> {
        let runtime_filter_probe = if self.runtime_filters_expected > 0 {
            self.scan.node_id().map(|node_id| {
                ScanRuntimeFilterProbe::new(self.runtime_filter_hub.register_probe(node_id))
            })
        } else {
            None
        };
        if !self.local_rf_waiting_set.is_empty() {
            debug!(
                "ScanSource local RF wait: node_id={:?} driver_id={} waiting_set={:?}",
                self.scan.node_id(),
                driver_id,
                self.local_rf_waiting_set
            );
        }
        let node_id = self.scan.node_id().unwrap_or(-1);
        let label = format!("scan_async_queue node={} driver={}", node_id, driver_id);
        Box::new(ScanSourceOperator {
            name: self.name.clone(),
            scan: self.scan.clone(),
            state: self.state.clone(),
            driver_id,
            dispatch: None,
            runtime_filter_probe,
            local_rf_deps: self.local_rf_deps(),
            runtime_filter_exprs: self.runtime_filter_exprs.clone(),
            runtime_filters_expected: self.runtime_filters_expected,
            arena: Arc::clone(&self.arena),
            profiles: None,
            async_state: ScanAsyncState::new(operator_buffer_chunks().max(1), label),
            async_runners: Arc::new(Mutex::new(Vec::new())),
            inflight_tasks: Arc::new(AtomicUsize::new(0)),
            max_io_tasks: AtomicUsize::new(1),
            waiting_on_capacity: AtomicBool::new(false),
            submit_failures: AtomicUsize::new(0),
            first_submit_failure_at: Mutex::new(None),
            row_position_registered: false,
        })
    }

    fn is_source(&self) -> bool {
        true
    }
}

struct ScanSourceOperator {
    name: String,
    scan: ScanNode,
    state: SharedScanState,
    driver_id: i32,
    dispatch: Option<Arc<ScanDispatchState>>,
    runtime_filter_probe: Option<ScanRuntimeFilterProbe>,
    local_rf_deps: Vec<DependencyHandle>,
    runtime_filter_exprs: HashMap<i32, ExprId>,
    runtime_filters_expected: usize,
    arena: Arc<ExprArena>,
    profiles: Option<crate::runtime::profile::OperatorProfiles>,
    async_state: Arc<ScanAsyncState>,
    async_runners: Arc<Mutex<Vec<ScanAsyncRunner>>>,
    // Tracks how many async scan tasks are currently submitted and not yet finished.
    inflight_tasks: Arc<AtomicUsize>,
    max_io_tasks: AtomicUsize,
    waiting_on_capacity: AtomicBool,
    submit_failures: AtomicUsize,
    first_submit_failure_at: Mutex<Option<Instant>>,
    row_position_registered: bool,
}

impl ScanSourceOperator {
    fn local_rf_dependency(&self) -> Option<DependencyHandle> {
        for dep in &self.local_rf_deps {
            if !dep.is_ready() {
                return Some(dep.clone());
            }
        }
        None
    }

    fn register_row_position(&mut self, state: &RuntimeState) -> Result<(), String> {
        if self.row_position_registered {
            return Ok(());
        }
        let Some(spec) = self.scan.row_position() else {
            self.row_position_registered = true;
            return Ok(());
        };
        let Some(ranges) = self.scan.row_position_ranges() else {
            return Err("row position ranges missing".to_string());
        };
        if ranges.is_empty() {
            self.row_position_registered = true;
            return Ok(());
        }
        let Some(scan_cfg) = self.scan.row_position_scan() else {
            return Err("row position scan config missing".to_string());
        };
        let Some(query_id) = state.query_id() else {
            return Err("row position requires query_id".to_string());
        };
        // Register ranges once so lookup RPCs can re-scan files by scan_range_id/row_id.
        crate::runtime::query_context::query_context_manager().register_glm_scan_ranges(
            query_id,
            spec.row_source_slot,
            scan_cfg.clone(),
            ranges.to_vec(),
        )?;
        self.row_position_registered = true;
        Ok(())
    }

    fn init_async_runners(&mut self, max_io_tasks: usize) -> Result<(), String> {
        if self.dispatch.is_none() {
            return Err("scan source operator not prepared".to_string());
        }
        let max_io_tasks = max_io_tasks.max(1);
        self.max_io_tasks.store(max_io_tasks, Ordering::Release);
        let mut guard = self.async_runners.lock().expect("scan runner lock");
        if !guard.is_empty() {
            return Ok(());
        }
        let dispatch = self.dispatch.as_ref().expect("scan dispatch");
        for _ in 0..max_io_tasks {
            let runner = ScanAsyncRunner::new(
                self.name.clone(),
                self.scan.clone(),
                Arc::clone(dispatch),
                self.runtime_filter_probe.clone(),
                self.runtime_filter_exprs.clone(),
                self.runtime_filters_expected,
                Arc::clone(&self.arena),
                self.profiles.clone(),
                self.driver_id,
            );
            guard.push(runner);
        }
        Ok(())
    }

    fn maybe_start_async_scan(&self) {
        // Scan tasks are short-lived: stop when the buffer is full and resume on demand.
        if self.async_state.is_canceled() || self.async_state.is_finished() {
            return;
        }
        if self.local_rf_dependency().is_some() {
            return;
        }
        if let Some(dispatch) = self.dispatch.as_ref() {
            let queue_empty = dispatch.queue_empty();
            if queue_empty && self.inflight_tasks.load(Ordering::Acquire) == 0 {
                let has_pending = {
                    let guard = self.async_runners.lock().expect("scan runner lock");
                    guard.iter().any(|runner| {
                        runner.pending_chunk.is_some() || runner.morsel_iter.is_some()
                    })
                };
                // Avoid leaving idle drivers stuck when all morsels are consumed.
                if !has_pending && !self.async_state.has_output() && !dispatch.has_more() {
                    let node_id = self.scan.node_id().unwrap_or(-1);
                    debug!(
                        "ScanSource mark_finished: node_id={} driver_id={} reason=dispatch_exhausted_no_pending",
                        node_id, self.driver_id
                    );
                    self.async_state.mark_finished();
                    return;
                }
            }
        }
        if let Some(rf) = self.runtime_filter_probe.as_ref() {
            if let Some(dep) = rf.dependency_or_timeout() {
                if !dep.is_ready() {
                    return;
                }
            }
        }
        if !self.async_state.has_capacity() {
            return;
        }
        let max_io_tasks = self.max_io_tasks.load(Ordering::Acquire).max(1);
        loop {
            if self.async_state.is_canceled() || self.async_state.is_finished() {
                return;
            }
            if !self.async_state.has_capacity() {
                return;
            }
            let has_runner = {
                let guard = self.async_runners.lock().expect("scan runner lock");
                !guard.is_empty()
            };
            if !has_runner {
                return;
            }
            if !self.try_acquire_inflight(max_io_tasks) {
                return;
            }
            let state = Arc::clone(&self.async_state);
            let runners = Arc::clone(&self.async_runners);
            let inflight = Arc::clone(&self.inflight_tasks);
            let inflight_observable = self
                .dispatch
                .as_ref()
                .expect("scan dispatch")
                .inflight_observable();
            let observable = self.async_state.observable();
            let submitted = scan_executor()
                .submit(move || run_scan_worker(state, runners, inflight, inflight_observable));
            if !submitted {
                self.inflight_tasks.fetch_sub(1, Ordering::AcqRel);
                if self
                    .waiting_on_capacity
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    scan_executor().register_capacity_waiter(&observable);
                }
                let failures = self.submit_failures.fetch_add(1, Ordering::AcqRel) + 1;
                let elapsed = {
                    let mut guard = self
                        .first_submit_failure_at
                        .lock()
                        .expect("scan submit failure lock");
                    let start = guard.get_or_insert_with(Instant::now);
                    start.elapsed()
                };
                // Clamp to avoid immediate fail-fast when config is zero.
                let fail_max = scan_submit_fail_max().max(1);
                let fail_timeout_ms = scan_submit_fail_timeout_ms().max(1);
                let fail_timeout = Duration::from_millis(fail_timeout_ms);
                // Fail fast if we cannot submit while the buffer is empty for too long.
                if (failures >= fail_max || elapsed >= fail_timeout)
                    && !self.async_state.has_output()
                {
                    let node_id = self.scan.node_id().unwrap_or(-1);
                    let err = format!(
                        "scan executor submit failed too long: node_id={} driver_id={} failures={} elapsed_ms={}",
                        node_id,
                        self.driver_id,
                        failures,
                        elapsed.as_millis()
                    );
                    warn!("{err}");
                    self.async_state.set_error(err);
                }
                return;
            }
            self.waiting_on_capacity.store(false, Ordering::Release);
            self.submit_failures.store(0, Ordering::Release);
            let mut guard = self
                .first_submit_failure_at
                .lock()
                .expect("scan submit failure lock");
            *guard = None;
        }
    }

    fn should_wait_runtime_filters(&self) -> bool {
        // Avoid blocking on runtime filters when this driver already has output
        // or has no remaining morsels to process.
        if self.async_state.is_finished() || self.async_state.has_output() {
            return false;
        }
        let Some(dispatch) = self.dispatch.as_ref() else {
            return true;
        };
        let queue_empty = dispatch.queue_empty();
        if !queue_empty {
            return true;
        }
        if dispatch.has_more() {
            return true;
        }
        if self.inflight_tasks.load(Ordering::Acquire) != 0 {
            return true;
        }
        let has_pending = {
            let guard = self.async_runners.lock().expect("scan runner lock");
            guard
                .iter()
                .any(|runner| runner.pending_chunk.is_some() || runner.morsel_iter.is_some())
        };
        has_pending
    }

    fn try_acquire_inflight(&self, max_io_tasks: usize) -> bool {
        let mut current = self.inflight_tasks.load(Ordering::Acquire);
        loop {
            if current >= max_io_tasks {
                return false;
            }
            match self.inflight_tasks.compare_exchange(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(next) => current = next,
            }
        }
    }
}

impl Operator for ScanSourceOperator {
    fn name(&self) -> &str {
        &self.name
    }

    fn set_profiles(&mut self, profiles: crate::runtime::profile::OperatorProfiles) {
        self.profiles = Some(profiles);
    }

    fn prepare(&mut self) -> Result<(), String> {
        if self.dispatch.is_some() {
            return Ok(());
        }

        let scan = self.scan.clone();
        let dispatch_result = self
            .state
            .dispatch
            .get_or_init(|| {
                let morsels = scan.build_morsels()?;
                if morsels.has_more {
                    let node_id = scan.node_id().unwrap_or(-1);
                    return Err(format!(
                        "scan node_id={} has incremental morsels which are not supported",
                        node_id
                    ));
                }
                let queue = DynamicMorselQueue::new(morsels.morsels, morsels.has_more);
                Ok(Arc::new(ScanDispatchState::new(queue)))
            })
            .clone();

        match dispatch_result {
            Ok(state) => {
                self.dispatch = Some(Arc::clone(&state));
                let max_io_tasks = {
                    let node_id = self.scan.node_id().unwrap_or(-1);
                    let tasks = self
                        .scan
                        .connector_io_tasks_per_scan_operator()
                        .unwrap_or(1);
                    if tasks <= 0 {
                        return Err(format!(
                            "invalid connector_io_tasks_per_scan_operator={} for scan node id={}",
                            tasks, node_id
                        ));
                    }
                    tasks as usize
                };
                self.init_async_runners(max_io_tasks)?;
                let async_obs = self.async_state.observable();
                let queue_obs = state.queue_observable();
                queue_obs.add_observer(Arc::new(move || {
                    let notify = async_obs.defer_notify();
                    notify.arm();
                }));
                let async_obs = self.async_state.observable();
                let inflight_obs = state.inflight_observable();
                inflight_obs.add_observer(Arc::new(move || {
                    let notify = async_obs.defer_notify();
                    notify.arm();
                }));
                let node_id = self.scan.node_id().unwrap_or(-1);
                debug!(
                    "ScanSource prepared: node_id={} driver_id={} original_morsels={} queue_empty={} has_more={} queue_observers={} inflight_observers={}",
                    node_id,
                    self.driver_id,
                    state.num_original_morsels(),
                    state.queue_empty(),
                    state.has_more(),
                    state.queue_observable().num_observers(),
                    state.inflight_observable().num_observers()
                );
                // Defer scheduling until the driver runs so downstream dependencies
                // (e.g., broadcast join build) can gate scan task submission.
                Ok(())
            }
            Err(err) => Err(err.clone()),
        }
    }

    fn cancel(&mut self) {
        self.async_state.cancel();
    }

    fn close(&mut self) -> Result<(), String> {
        self.async_state.cancel();
        Ok(())
    }

    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }

    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }

    fn is_finished(&self) -> bool {
        if self.async_state.is_finished() {
            return true;
        }
        if self.async_state.has_output() {
            return false;
        }
        let Some(dispatch) = self.dispatch.as_ref() else {
            return false;
        };
        if self.inflight_tasks.load(Ordering::Acquire) > 0 {
            return false;
        }
        if dispatch.has_more() || !dispatch.queue_empty() {
            return false;
        }
        let has_pending = {
            let guard = self.async_runners.lock().expect("scan runner lock");
            guard
                .iter()
                .any(|runner| runner.pending_chunk.is_some() || runner.morsel_iter.is_some())
        };
        if has_pending {
            return false;
        }
        let node_id = self.scan.node_id().unwrap_or(-1);
        debug!(
            "ScanSource mark_finished: node_id={} driver_id={} reason=is_finished_dispatch_exhausted",
            node_id, self.driver_id
        );
        self.async_state.mark_finished();
        true
    }

    fn pending_finish(&self) -> bool {
        false
    }
}

impl ProcessorOperator for ScanSourceOperator {
    fn need_input(&self) -> bool {
        false
    }

    fn has_output(&self) -> bool {
        self.maybe_start_async_scan();
        self.async_state.has_output()
    }

    fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> Result<(), String> {
        Err("scan source operator does not accept input".to_string())
    }

    fn pull_chunk(&mut self, state: &RuntimeState) -> Result<Option<Chunk>, String> {
        self.register_row_position(state)?;
        self.async_state.ensure_mem_tracker(state);
        let chunk = self.async_state.pop_chunk()?;
        self.maybe_start_async_scan();
        Ok(chunk)
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        Ok(())
    }

    fn source_observable(&self) -> Option<Arc<Observable>> {
        Some(self.async_state.observable())
    }

    fn precondition_dependency(
        &self,
    ) -> Option<crate::exec::pipeline::dependency::DependencyHandle> {
        // If the scan has already exhausted all morsels, do not block on
        // runtime filters or local RF. This avoids empty-range drivers
        // getting stuck behind dependencies and never sending EOS.
        if self.is_finished() {
            return None;
        }
        // 先等本地 RF（local build 端）
        if let Some(dep) = self.local_rf_dependency() {
            return Some(dep);
        }
        // 再等 RuntimeFilterHub 的依赖
        if let Some(rf) = self.runtime_filter_probe.as_ref() {
            if let Some(dep) = rf.dependency_or_timeout() {
                if !dep.is_ready() {
                    if !self.should_wait_runtime_filters() {
                        return None;
                    }
                    return Some(dep);
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Condvar, Mutex};

    use crate::runtime::runtime_state::RuntimeState;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use crate::common::ids::SlotId;
    use crate::exec::chunk::Chunk;
    use crate::exec::chunk::field_with_slot_id;
    use crate::exec::expr::ExprArena;
    use crate::exec::node::scan::{ScanMorsel, ScanMorsels, ScanNode, ScanOp};
    use crate::exec::pipeline::dependency::DependencyManager;
    use crate::exec::pipeline::operator_factory::OperatorFactory;
    use crate::runtime::io::io_executor;
    use crate::runtime::runtime_filter_hub::RuntimeFilterHub;

    use super::ScanSourceFactory;
    use std::thread;
    use std::time::{Duration, Instant};

    #[derive(Clone)]
    struct TestMorselScanOp {
        morsels: Vec<Vec<i32>>,
    }

    impl ScanOp for TestMorselScanOp {
        fn execute_iter(
            &self,
            morsel: ScanMorsel,
            _profile: Option<crate::runtime::profile::RuntimeProfile>,
            _runtime_filters: Option<&crate::exec::node::scan::RuntimeFilterContext>,
        ) -> Result<crate::exec::node::BoxedExecIter, String> {
            let ScanMorsel::FileRange { path, .. } = morsel else {
                return Err("test scan received unexpected morsel".to_string());
            };
            let idx: usize = path
                .strip_prefix("morsel:")
                .ok_or_else(|| "invalid morsel path".to_string())?
                .parse()
                .map_err(|_| "invalid morsel index".to_string())?;
            let data = self
                .morsels
                .get(idx)
                .cloned()
                .ok_or_else(|| "morsel index out of bounds".to_string())?;

            let schema = Arc::new(Schema::new(vec![field_with_slot_id(
                Field::new("v", DataType::Int32, false),
                SlotId::new(1),
            )]));
            let array = Arc::new(Int32Array::from(data)) as arrow::array::ArrayRef;
            let batch = RecordBatch::try_new(schema, vec![array]).map_err(|e| e.to_string())?;
            Ok(Box::new(std::iter::once(Ok(Chunk::new(batch)))))
        }

        fn build_morsels(&self) -> Result<ScanMorsels, String> {
            let morsels = (0..self.morsels.len())
                .map(|idx| ScanMorsel::FileRange {
                    path: format!("morsel:{idx}"),
                    file_len: 0,
                    offset: 0,
                    length: 0,
                    scan_range_id: -1,
                    first_row_id: None,
                    external_datacache: None,
                })
                .collect();
            Ok(ScanMorsels::new(morsels, false))
        }
    }

    #[test]
    fn scan_source_distributes_morsels_across_drivers() {
        let rt = RuntimeState::default();
        let op = TestMorselScanOp {
            morsels: vec![vec![1, 2], vec![3], vec![4, 5, 6], vec![7]],
        };
        let scan = ScanNode::new(Arc::new(op)).with_connector_io_tasks_per_scan_operator(Some(1));
        let runtime_filter_hub = Arc::new(RuntimeFilterHub::new(DependencyManager::new()));
        let arena = Arc::new(ExprArena::default());
        let factory = ScanSourceFactory::new(scan, runtime_filter_hub, arena);

        let dop = 4;
        let mut drivers: Vec<Box<dyn crate::exec::pipeline::operator::Operator>> = (0..dop)
            .map(|driver_id| factory.create(dop, driver_id))
            .collect();
        for d in drivers.iter_mut() {
            d.prepare().expect("prepare");
        }

        let mut values = Vec::new();
        let mut finished = vec![false; dop as usize];
        while finished.iter().any(|f| !*f) {
            for (idx, d) in drivers.iter_mut().enumerate() {
                if finished[idx] {
                    continue;
                }
                let Some(proc) = d.as_processor_mut() else {
                    panic!("missing processor");
                };
                match proc.pull_chunk(&rt) {
                    Ok(Some(chunk)) => {
                        let arr = chunk
                            .columns()
                            .get(0)
                            .expect("col0")
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .expect("int32");
                        for i in 0..arr.len() {
                            values.push(arr.value(i));
                        }
                    }
                    Ok(None) => {
                        if d.is_finished() {
                            finished[idx] = true;
                        } else {
                            thread::sleep(Duration::from_millis(1));
                        }
                    }
                    Err(e) => panic!("unexpected error: {e:?}"),
                }
            }
        }

        values.sort();
        assert_eq!(values, vec![1, 2, 3, 4, 5, 6, 7]);
    }

    #[test]
    fn scan_produces_output_when_io_executor_is_saturated() {
        let threads = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        let started = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new((Mutex::new(false), Condvar::new()));

        // Saturate IO executor threads to mimic exchange backpressure.
        for _ in 0..threads {
            let started = Arc::clone(&started);
            let gate = Arc::clone(&gate);
            io_executor().submit(move |_ctx| {
                started.fetch_add(1, Ordering::AcqRel);
                let (lock, cv) = &*gate;
                let mut ready = lock.lock().expect("io executor gate lock");
                while !*ready {
                    ready = cv.wait(ready).expect("io executor gate wait");
                }
            });
        }

        let wait_start = Instant::now();
        while started.load(Ordering::Acquire) < threads {
            if wait_start.elapsed() > Duration::from_secs(2) {
                break;
            }
            thread::sleep(Duration::from_millis(5));
        }

        let rt = RuntimeState::default();
        let op = TestMorselScanOp {
            morsels: vec![vec![1, 2, 3]],
        };
        let scan = ScanNode::new(Arc::new(op)).with_connector_io_tasks_per_scan_operator(Some(1));
        let runtime_filter_hub = Arc::new(RuntimeFilterHub::new(DependencyManager::new()));
        let arena = Arc::new(ExprArena::default());
        let factory = ScanSourceFactory::new(scan, runtime_filter_hub, arena);

        let mut driver = factory.create(2, 0);
        driver.prepare().expect("prepare scan source");
        let proc = driver.as_processor_mut().expect("scan source processor");

        let start = Instant::now();
        let mut output = None;
        while start.elapsed() < Duration::from_secs(1) {
            if proc.has_output() {
                output = proc.pull_chunk(&rt).expect("pull chunk");
                if output.as_ref().map(|c| !c.is_empty()).unwrap_or(false) {
                    break;
                }
            }
            thread::sleep(Duration::from_millis(5));
        }

        {
            let (lock, cv) = &*gate;
            let mut ready = lock.lock().expect("io executor gate lock");
            *ready = true;
            cv.notify_all();
        }

        assert!(
            output.is_some(),
            "scan should produce output even when io executor is saturated"
        );
    }

    #[test]
    fn scan_source_early_termination_with_limit() {
        let rt = RuntimeState::default();

        // Create 20 morsels, each with 10 rows, total 200 rows.
        // Set limit to 25 with DOP=4 and 1 io task per driver.
        // With parallel execution, we may read up to limit + (DOP * rows_per_morsel) rows
        // due to in-flight morsels, but should stop picking up new morsels after limit.
        let morsels: Vec<Vec<i32>> = (0..20).map(|i| (i * 10..i * 10 + 10).collect()).collect();

        let op = TestMorselScanOp { morsels };
        let scan = ScanNode::new(Arc::new(op))
            .with_connector_io_tasks_per_scan_operator(Some(1))
            .with_limit(Some(25));

        let runtime_filter_hub = Arc::new(RuntimeFilterHub::new(DependencyManager::new()));
        let arena = Arc::new(ExprArena::default());
        let factory = ScanSourceFactory::new(scan, runtime_filter_hub, arena);

        let dop = 4;
        let mut drivers: Vec<Box<dyn crate::exec::pipeline::operator::Operator>> = (0..dop)
            .map(|driver_id| factory.create(dop, driver_id))
            .collect();
        for d in drivers.iter_mut() {
            d.prepare().expect("prepare");
        }

        let mut total_rows = 0;
        let mut finished = vec![false; dop as usize];
        let mut iterations = 0;
        let max_iterations = 2000; // Prevent infinite loop in case of bugs

        while finished.iter().any(|f| !*f) && iterations < max_iterations {
            iterations += 1;
            for (idx, d) in drivers.iter_mut().enumerate() {
                if finished[idx] {
                    continue;
                }
                let Some(proc) = d.as_processor_mut() else {
                    panic!("missing processor");
                };
                match proc.pull_chunk(&rt) {
                    Ok(Some(chunk)) => {
                        total_rows += chunk.len();
                    }
                    Ok(None) => {
                        if d.is_finished() {
                            finished[idx] = true;
                        } else {
                            thread::sleep(Duration::from_millis(1));
                        }
                    }
                    Err(e) => panic!("unexpected error: {e:?}"),
                }
            }
        }

        // With early termination, we should read significantly fewer than all 200 rows.
        // Due to parallel execution, we may read up to limit + (DOP * rows_per_morsel) = 25 + 40 = 65.
        // Without early termination, we would read all 200 rows.
        assert!(
            total_rows >= 25 && total_rows < 100,
            "expected rows between 25 and 100 due to early termination (limit=25, dop=4), got {}. \
             Without early termination, would read all 200 rows.",
            total_rows
        );
    }

    #[test]
    fn scan_source_early_termination_few_drivers() {
        let rt = RuntimeState::default();

        // Create 10 morsels, each with 10 rows, total 100 rows.
        // With DOP=2 and limit=25, we should read at most 3-4 morsels (30-40 rows).
        let morsels: Vec<Vec<i32>> = (0..10).map(|i| (i * 10..i * 10 + 10).collect()).collect();

        let op = TestMorselScanOp { morsels };
        let scan = ScanNode::new(Arc::new(op))
            .with_connector_io_tasks_per_scan_operator(Some(1))
            .with_limit(Some(25));

        let runtime_filter_hub = Arc::new(RuntimeFilterHub::new(DependencyManager::new()));
        let arena = Arc::new(ExprArena::default());
        let factory = ScanSourceFactory::new(scan, runtime_filter_hub, arena);

        let dop = 2;
        let mut drivers: Vec<Box<dyn crate::exec::pipeline::operator::Operator>> = (0..dop)
            .map(|driver_id| factory.create(dop, driver_id))
            .collect();
        for d in drivers.iter_mut() {
            d.prepare().expect("prepare");
        }

        let mut total_rows = 0;
        let mut finished = vec![false; dop as usize];
        let mut iterations = 0;
        let max_iterations = 1000;

        while finished.iter().any(|f| !*f) && iterations < max_iterations {
            iterations += 1;
            for (idx, d) in drivers.iter_mut().enumerate() {
                if finished[idx] {
                    continue;
                }
                let Some(proc) = d.as_processor_mut() else {
                    panic!("missing processor");
                };
                match proc.pull_chunk(&rt) {
                    Ok(Some(chunk)) => {
                        total_rows += chunk.len();
                    }
                    Ok(None) => {
                        if d.is_finished() {
                            finished[idx] = true;
                        } else {
                            thread::sleep(Duration::from_millis(1));
                        }
                    }
                    Err(e) => panic!("unexpected error: {e:?}"),
                }
            }
        }

        // With early termination and DOP=2, we should read significantly less than all 100 rows.
        // We expect around 30-40 rows (3-4 morsels), definitely less than 70.
        assert!(
            total_rows < 70,
            "expected < 70 rows with early termination, got {} (limit=25, dop=2, all morsels=100 rows)",
            total_rows
        );
    }
}
