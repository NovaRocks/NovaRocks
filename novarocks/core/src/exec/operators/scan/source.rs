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
    connector_io_tasks_per_scan_operator_default, operator_buffer_chunks, scan_submit_fail_max,
    scan_submit_fail_timeout_ms,
};
use crate::exec::chunk::Chunk;
use crate::exec::expr::ExprArena;
use crate::exec::node::scan::{ScanNode, ScanOp};
use crate::exec::operators::runtime_filter::{
    NativeOrderedLiveConsumerSet, RuntimeFilterConsumerSet,
};
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::scan::morsel::DynamicMorselQueue;
use crate::exec::pipeline::schedule::observer::Observable;
use crate::novarocks_logging::{debug, warn};
use crate::runtime::runtime_state::RuntimeState;
use crate::runtime::scan_executor::scan_executor;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use super::dispatch::{ScanDispatchState, SharedScanState};
use super::runner::{ScanAsyncRunner, run_scan_worker};
use super::types::ScanAsyncState;

/// Factory for scan source operators that consume async scan output.
pub struct ScanSourceFactory {
    name: String,
    scan: ScanNode,
    /// Instance-materialized bound op (from the pipeline's `ScanBindings`).
    /// The static `scan.source()` produced this via `bind`; the operator and
    /// its async runners execute against this op, not against the node.
    op: Arc<dyn ScanOp>,
    state: SharedScanState,
    runtime_filter_execution: ScanSourceRuntimeFilterExecution,
    arena: Arc<ExprArena>,
}

struct ScanSourceRuntimeFilterExecution {
    blocking_consumers: RuntimeFilterConsumerSet,
    ordered_live_consumers: NativeOrderedLiveConsumerSet,
}

impl ScanSourceFactory {
    pub(crate) fn new_native(
        scan: ScanNode,
        op: Arc<dyn ScanOp>,
        arena: Arc<ExprArena>,
    ) -> Result<Self, String> {
        let mut membership_specs = Vec::new();
        let mut ordered_live_specs = Vec::new();
        let mut seen_bindings = HashSet::new();
        for spec in scan.native_runtime_filter_specs() {
            if !seen_bindings.insert(spec.binding_id) {
                return Err(format!(
                    "duplicate native scan runtime-filter consumer binding_id={}",
                    spec.binding_id
                ));
            }
            match &spec.contract {
                crate::exec::node::runtime_filter::RuntimeFilterExecutionContract::Membership {
                    ..
                } => membership_specs.push(spec.clone()),
                crate::exec::node::runtime_filter::RuntimeFilterExecutionContract::Ordered {
                    ..
                } => ordered_live_specs.push(spec.clone()),
            }
        }
        let blocking_consumers =
            RuntimeFilterConsumerSet::from_plan(&membership_specs, Arc::clone(&arena))?;
        let ordered_live_consumers =
            NativeOrderedLiveConsumerSet::from_plan(&ordered_live_specs, Arc::clone(&arena))?;
        Ok(Self::new_in_mode(
            scan,
            op,
            arena,
            ScanSourceRuntimeFilterExecution {
                blocking_consumers,
                ordered_live_consumers,
            },
        ))
    }

    #[cfg(test)]
    fn new_native_with_consumers_for_test(
        scan: ScanNode,
        op: Arc<dyn ScanOp>,
        arena: Arc<ExprArena>,
        consumers: RuntimeFilterConsumerSet,
    ) -> Self {
        let ordered_live_consumers =
            NativeOrderedLiveConsumerSet::from_plan(&[], Arc::clone(&arena))
                .expect("empty ordered live consumer set");
        Self::new_native_with_consumer_groups_for_test(
            scan,
            op,
            arena,
            consumers,
            ordered_live_consumers,
        )
    }

    #[cfg(test)]
    fn new_native_with_consumer_groups_for_test(
        scan: ScanNode,
        op: Arc<dyn ScanOp>,
        arena: Arc<ExprArena>,
        blocking_consumers: RuntimeFilterConsumerSet,
        ordered_live_consumers: NativeOrderedLiveConsumerSet,
    ) -> Self {
        Self::new_in_mode(
            scan,
            op,
            arena,
            ScanSourceRuntimeFilterExecution {
                blocking_consumers,
                ordered_live_consumers,
            },
        )
    }

    fn new_in_mode(
        scan: ScanNode,
        op: Arc<dyn ScanOp>,
        arena: Arc<ExprArena>,
        runtime_filter_execution: ScanSourceRuntimeFilterExecution,
    ) -> Self {
        let mut name = op
            .profile_name()
            .unwrap_or_else(|| "ScanSource".to_string());
        if !name.contains("plan_node_id=") && !name.contains("(id=") {
            if let Some(node_id) = scan.node_id() {
                // Most scan ops (including schema scan) don't carry plan_node_id in their
                // profile name template. Append it here to keep profile naming consistent
                // without log spam on normal queries.
                name = format!("{name} (plan_node_id={node_id})");
            } else {
                warn!(
                    "scan profile name missing plan_node_id and node_id, using plan_node_id=-1: name={}",
                    name
                );
                name = format!("{name} (plan_node_id=-1)");
            }
        }
        Self {
            name,
            scan,
            op,
            state: SharedScanState::new(),
            runtime_filter_execution,
            arena,
        }
    }
}

impl OperatorFactory for ScanSourceFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, driver_id: i32) -> Box<dyn Operator> {
        let node_id = self.scan.node_id().unwrap_or(-1);
        let label = format!("scan_async_queue node={} driver={}", node_id, driver_id);
        Box::new(ScanSourceOperator {
            name: self.name.clone(),
            scan: self.scan.clone(),
            op: Arc::clone(&self.op),
            state: self.state.clone(),
            driver_id,
            dispatch: None,
            arena: Arc::clone(&self.arena),
            native_runtime_filter_consumers: Some(
                self.runtime_filter_execution.blocking_consumers.clone(),
            ),
            native_ordered_live_consumers: Some(
                self.runtime_filter_execution.ordered_live_consumers.clone(),
            ),
            profiles: None,
            async_state: ScanAsyncState::new(operator_buffer_chunks().max(1), label),
            async_runners: Arc::new(Mutex::new(Vec::new())),
            inflight_tasks: Arc::new(AtomicUsize::new(0)),
            max_io_tasks: AtomicUsize::new(1),
            waiting_on_capacity: AtomicBool::new(false),
            submit_failures: AtomicUsize::new(0),
            first_submit_failure_at: Mutex::new(None),
            dispatch_observers_registered: AtomicBool::new(false),
            row_position_registered: false,
            lake_row_position_registered: false,
            incremental_registered: false,
        })
    }

    fn is_source(&self) -> bool {
        true
    }
}

struct ScanSourceOperator {
    name: String,
    scan: ScanNode,
    /// Instance-materialized bound op (see `ScanSourceFactory::op`). All
    /// morsel building / execution goes through this op; `scan` supplies only
    /// static node config (accept-empty gate, RF specs, node id, limit, ...).
    op: Arc<dyn ScanOp>,
    state: SharedScanState,
    driver_id: i32,
    dispatch: Option<Arc<ScanDispatchState>>,
    arena: Arc<ExprArena>,
    native_runtime_filter_consumers: Option<RuntimeFilterConsumerSet>,
    native_ordered_live_consumers: Option<NativeOrderedLiveConsumerSet>,
    profiles: Option<crate::runtime::profile::OperatorProfiles>,
    async_state: Arc<ScanAsyncState>,
    async_runners: Arc<Mutex<Vec<ScanAsyncRunner>>>,
    // Tracks how many async scan tasks are currently submitted and not yet finished.
    inflight_tasks: Arc<AtomicUsize>,
    max_io_tasks: AtomicUsize,
    waiting_on_capacity: AtomicBool,
    submit_failures: AtomicUsize,
    first_submit_failure_at: Mutex<Option<Instant>>,
    dispatch_observers_registered: AtomicBool,
    row_position_registered: bool,
    lake_row_position_registered: bool,
    incremental_registered: bool,
}

impl ScanSourceOperator {
    fn register_row_position(&mut self, state: &RuntimeState) -> Result<(), String> {
        if self.row_position_registered {
            return Ok(());
        }
        let Some(spec) = self.scan.row_position() else {
            self.row_position_registered = true;
            return Ok(());
        };
        if let Some(lookup) = self.scan.connector_row_position_lookup() {
            let query_id = state
                .query_id()
                .ok_or_else(|| "row position requires query_id".to_string())?;
            crate::runtime::query_context::query_context_manager().register_connector_glm(
                query_id,
                spec.row_source_slot,
                lookup.clone(),
            )?;
            self.row_position_registered = true;
            return Ok(());
        }
        Err("row position requires a connector read binding".to_string())
    }

    fn register_lake_row_position(&mut self, state: &RuntimeState) -> Result<(), String> {
        if self.lake_row_position_registered {
            return Ok(());
        }
        let Some(spec) = self.scan.lake_row_position() else {
            self.lake_row_position_registered = true;
            return Ok(());
        };
        let Some(info) = self.scan.lake_glm_info() else {
            return Err("lake_row_position set but lake_glm_info missing".to_string());
        };
        let Some(query_id) = state.query_id() else {
            return Err("lake row position requires query_id".to_string());
        };
        crate::runtime::query_context::query_context_manager().register_lake_glm(
            query_id,
            spec.source_id_slot,
            info.clone(),
        )?;
        self.lake_row_position_registered = true;
        Ok(())
    }

    fn register_incremental_dispatch(&mut self, state: &RuntimeState) -> Result<(), String> {
        if self.incremental_registered {
            return Ok(());
        }
        if !self.op.supports_incremental_scan_ranges() {
            self.incremental_registered = true;
            return Ok(());
        }
        let Some(finst_id) = state.fragment_instance_id() else {
            self.incremental_registered = true;
            return Ok(());
        };
        let Some(node_id) = self.scan.node_id() else {
            self.incremental_registered = true;
            return Ok(());
        };
        let Some(dispatch) = self.current_dispatch()? else {
            return Ok(());
        };
        crate::runtime::query_context::query_context_manager().register_incremental_scan_node(
            finst_id,
            node_id,
            Arc::clone(&self.op),
            dispatch,
        )?;
        self.incremental_registered = true;
        Ok(())
    }

    fn max_io_tasks_for_scan(&self) -> Result<usize, String> {
        let node_id = self.scan.node_id().unwrap_or(-1);
        let tasks = self
            .scan
            .connector_io_tasks_per_scan_operator()
            .unwrap_or_else(connector_io_tasks_per_scan_operator_default);
        if tasks <= 0 {
            return Err(format!(
                "invalid connector_io_tasks_per_scan_operator={} for scan node id={}",
                tasks, node_id
            ));
        }
        Ok(tasks as usize)
    }

    fn current_dispatch(&self) -> Result<Option<Arc<ScanDispatchState>>, String> {
        if let Some(dispatch) = self.dispatch.as_ref() {
            return Ok(Some(Arc::clone(dispatch)));
        }
        let Some(result) = self.state.dispatch.get() else {
            return Ok(None);
        };
        match result {
            Ok(dispatch) => Ok(Some(Arc::clone(dispatch))),
            Err(err) => Err(err.clone()),
        }
    }

    fn build_shared_dispatch(&self) -> Result<Arc<ScanDispatchState>, String> {
        let scan = self.scan.clone();
        let op = Arc::clone(&self.op);
        // Static gate lifted off the node; the bound op no longer knows it.
        let accept_empty = scan.accept_empty_scan_ranges();
        let dispatch_result = self
            .state
            .dispatch
            .get_or_init(move || {
                let mut morsels = op.build_morsels()?;
                morsels.ensure_non_empty(accept_empty);
                if morsels.has_more && !op.supports_incremental_scan_ranges() {
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
        dispatch_result.map_err(|err| err.clone())
    }

    fn ensure_dispatch_initialized(&self) -> Result<Option<Arc<ScanDispatchState>>, String> {
        if let Some(dispatch) = self.current_dispatch()? {
            self.configure_dispatch(Arc::clone(&dispatch))?;
            return Ok(Some(dispatch));
        }
        let dispatch = self.build_shared_dispatch()?;
        self.configure_dispatch(Arc::clone(&dispatch))?;
        Ok(Some(dispatch))
    }

    fn configure_dispatch(&self, dispatch: Arc<ScanDispatchState>) -> Result<(), String> {
        let max_io_tasks = self.max_io_tasks_for_scan()?;
        self.init_async_runners(Arc::clone(&dispatch), max_io_tasks);
        self.register_dispatch_observers(dispatch);
        Ok(())
    }

    fn init_async_runners(&self, dispatch: Arc<ScanDispatchState>, max_io_tasks: usize) {
        let max_io_tasks = max_io_tasks.max(1);
        self.max_io_tasks.store(max_io_tasks, Ordering::Release);
        let mut guard = self.async_runners.lock().expect("scan runner lock");
        if !guard.is_empty() {
            return;
        }
        for _ in 0..max_io_tasks {
            let runner = ScanAsyncRunner::new(
                self.name.clone(),
                self.scan.clone(),
                Arc::clone(&self.op),
                Arc::clone(&dispatch),
                self.native_runtime_filter_consumers.clone(),
                self.native_ordered_live_consumers.clone(),
                Arc::clone(&self.arena),
                self.profiles.clone(),
                self.driver_id,
            );
            guard.push(runner);
        }
    }

    fn register_dispatch_observers(&self, dispatch: Arc<ScanDispatchState>) {
        if self
            .dispatch_observers_registered
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let async_obs = self.async_state.observable();
        let queue_obs = dispatch.queue_observable();
        queue_obs.add_observer(Arc::new(move || {
            let notify = async_obs.defer_notify();
            notify.arm();
        }));
        let async_obs = self.async_state.observable();
        let inflight_obs = dispatch.inflight_observable();
        inflight_obs.add_observer(Arc::new(move || {
            let notify = async_obs.defer_notify();
            notify.arm();
        }));
    }

    fn propagate_native_ordered_live_consumers(&mut self) {
        let mut runners = self.async_runners.lock().expect("scan runner lock");
        for runner in runners.iter_mut() {
            runner.set_native_ordered_live_consumers(
                self.native_ordered_live_consumers
                    .as_ref()
                    .map(NativeOrderedLiveConsumerSet::clone),
            );
        }
    }

    fn maybe_start_async_scan(&self) {
        // Scan tasks are short-lived: stop when the buffer is full and resume on demand.
        if self.async_state.is_canceled() || self.async_state.is_finished() {
            return;
        }
        let dispatch = match self.ensure_dispatch_initialized() {
            Ok(Some(dispatch)) => dispatch,
            Ok(None) => return,
            Err(err) => {
                self.async_state.set_error(err);
                return;
            }
        };
        let queue_empty = dispatch.queue_empty();
        if queue_empty && self.inflight_tasks.load(Ordering::Acquire) == 0 {
            let has_pending = {
                let guard = self.async_runners.lock().expect("scan runner lock");
                guard
                    .iter()
                    .any(|runner| runner.pending_chunk.is_some() || runner.morsel_iter.is_some())
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
            if let Some(consumers) = self.native_runtime_filter_consumers.as_ref()
                && let Err(error) = consumers.acquire_configured()
            {
                self.async_state.set_error(error);
                return;
            }
            if !self.try_acquire_inflight(max_io_tasks) {
                return;
            }
            let state = Arc::clone(&self.async_state);
            let runners = Arc::clone(&self.async_runners);
            let inflight = Arc::clone(&self.inflight_tasks);
            let inflight_observable = dispatch.inflight_observable();
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

        let Some(state) = self.ensure_dispatch_initialized()? else {
            return Ok(());
        };
        self.dispatch = Some(Arc::clone(&state));
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

    fn bind_runtime_state(&mut self, state: &RuntimeState) -> Result<(), String> {
        if let Some(consumers) = self.native_runtime_filter_consumers.as_ref() {
            consumers.set_wait_timeout(
                state
                    .runtime_filter_scan_wait_timeout()
                    .or_else(|| state.runtime_filter_wait_timeout())
                    .unwrap_or(Duration::from_secs(1)),
            );
            consumers.bind(state)?;
        }
        if let Some(consumers) = self.native_ordered_live_consumers.as_ref() {
            consumers.bind(state)?;
        }
        self.propagate_native_ordered_live_consumers();
        self.register_incremental_dispatch(state)
    }

    fn cancel(&mut self) {
        self.async_state.cancel();
        let _ = self.op.terminate();
    }

    fn close(&mut self) -> Result<(), String> {
        self.async_state.cancel();
        // Each driver owns one operator instance, while a connector ScanOp
        // owns a fragment-wide reader group and a shared morsel queue.  A
        // normally exhausted driver must not terminate that group: sibling
        // drivers can still own queued splits.  Cancellation/error reaches
        // `cancel`, which invokes the terminal hook; normal readers close on
        // EOF or iterator Drop.
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
        let dispatch = match self.current_dispatch() {
            Ok(Some(dispatch)) => dispatch,
            Ok(None) => return false,
            Err(err) => {
                self.async_state.set_error(err);
                return false;
            }
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
        self.ensure_dispatch_initialized()?;
        self.register_incremental_dispatch(state)?;
        self.register_row_position(state)?;
        self.register_lake_row_position(state)?;
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
        None
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Condvar, Mutex};

    use crate::runtime::runtime_state::RuntimeState;
    use arrow::array::{Array, DictionaryArray, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use arrow::record_batch::RecordBatch;

    use crate::common::ids::SlotId;
    use crate::exec::chunk::Chunk;
    use crate::exec::expr::{ExprArena, ExprId};
    use crate::exec::node::scan::{ScanMorsel, ScanMorsels, ScanNode, ScanOp};
    use crate::exec::pipeline::dependency::DependencyManager;
    use crate::exec::pipeline::operator_factory::OperatorFactory;
    use crate::runtime::io::io_executor;

    use super::ScanSourceFactory;
    use std::thread;
    use std::time::{Duration, Instant};

    #[derive(Clone)]
    struct TestMorselScanOp {
        morsels: Vec<Vec<i32>>,
    }

    struct IdleOrderedLiveSubscription {
        polls: AtomicUsize,
    }

    impl crate::runtime_filter::port::subscription::NonBlockingLiveSubscription
        for IdleOrderedLiveSubscription
    {
        fn snapshot(&self) -> Option<Arc<crate::runtime_filter::port::artifact::ArtifactBundle>> {
            None
        }

        fn poll_after(
            &self,
            observed: Option<crate::runtime_filter::port::identity::LogicalVersion>,
        ) -> crate::runtime_filter::port::subscription::LivePollOutcome {
            self.polls.fetch_add(1, Ordering::SeqCst);
            crate::runtime_filter::port::subscription::LivePollOutcome::Idle {
                latest_version: observed,
                terminal: None,
            }
        }
    }

    fn test_file_morsel(path: impl Into<String>) -> ScanMorsel {
        ScanMorsel::FileRange {
            path: path.into(),
            file_len: 0,
            offset: 0,
            length: 0,
            scan_range_id: -1,
            external_datacache: None,
        }
    }

    struct PlainScanOp {
        plain_calls: AtomicUsize,
    }

    impl PlainScanOp {
        fn new() -> Self {
            Self {
                plain_calls: AtomicUsize::new(0),
            }
        }

        fn build_plain_calls(&self) -> usize {
            self.plain_calls.load(Ordering::Acquire)
        }
    }

    impl ScanOp for PlainScanOp {
        fn execute_iter(
            &self,
            _morsel: ScanMorsel,
            _profile: Option<crate::runtime::profile::RuntimeProfile>,
            _runtime_filters: Option<&crate::exec::node::scan::RuntimeFilterContext>,
        ) -> Result<crate::exec::node::BoxedExecIter, String> {
            Ok(Box::new(std::iter::empty()))
        }

        fn build_morsels(&self) -> Result<ScanMorsels, String> {
            self.plain_calls.fetch_add(1, Ordering::AcqRel);
            Ok(ScanMorsels::new(vec![test_file_morsel("plain")], false))
        }
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

            let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
            let array = Arc::new(Int32Array::from(data)) as arrow::array::ArrayRef;
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
            let morsels = (0..self.morsels.len())
                .map(|idx| ScanMorsel::FileRange {
                    path: format!("morsel:{idx}"),
                    file_len: 0,
                    offset: 0,
                    length: 0,
                    scan_range_id: -1,
                    external_datacache: None,
                })
                .collect();
            Ok(ScanMorsels::new(morsels, false))
        }
    }

    struct DictionaryScanOp;

    impl ScanOp for DictionaryScanOp {
        fn execute_iter(
            &self,
            _morsel: ScanMorsel,
            _profile: Option<crate::runtime::profile::RuntimeProfile>,
            _runtime_filters: Option<&crate::exec::node::scan::RuntimeFilterContext>,
        ) -> Result<crate::exec::node::BoxedExecIter, String> {
            let dictionary = Arc::new(
                vec![Some("one"), Some("two"), Some("three"), Some("four")]
                    .into_iter()
                    .collect::<DictionaryArray<Int32Type>>(),
            ) as arrow::array::ArrayRef;
            let logical_schema = Schema::new(vec![Field::new("v", DataType::Utf8, true)]);
            let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                &logical_schema,
                &[SlotId::new(1)],
            )?;
            let chunk = Chunk::try_new_with_columns(chunk_schema, vec![dictionary])?;
            Ok(Box::new(std::iter::once(Ok(chunk))))
        }

        fn build_morsels(&self) -> Result<ScanMorsels, String> {
            Ok(ScanMorsels::new(
                vec![ScanMorsel::FileRange {
                    path: "dictionary:test".to_string(),
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

    #[test]
    fn non_runtime_materialized_scan_keeps_plain_build_morsels() {
        let op = Arc::new(PlainScanOp::new());
        let scan = ScanNode::new_for_test(op.clone())
            .with_node_id(11)
            .with_connector_io_tasks_per_scan_operator(Some(1));
        let arena = Arc::new(ExprArena::default());
        let factory = ScanSourceFactory::new_native(scan, op.clone(), arena).unwrap();

        let mut source = factory.create(1, 0);
        source.prepare().expect("prepare source");

        assert_eq!(op.build_plain_calls(), 1);
    }

    #[test]
    fn scan_source_distributes_morsels_across_drivers() {
        let rt = RuntimeState::default();
        let op: Arc<dyn ScanOp> = Arc::new(TestMorselScanOp {
            morsels: vec![vec![1, 2], vec![3], vec![4, 5, 6], vec![7]],
        });
        let scan = ScanNode::new_for_test(Arc::clone(&op))
            .with_connector_io_tasks_per_scan_operator(Some(1));
        let arena = Arc::new(ExprArena::default());
        let factory = ScanSourceFactory::new_native(scan, op, arena).unwrap();

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
                            .first()
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
    fn native_scan_source_applies_the_shared_membership_mask() {
        let (consumers, arena) =
            crate::exec::operators::runtime_filter::tests_support::published_consumer_set(
                crate::exec::operators::runtime_filter::tests_support::membership_bundle(&[2, 4]),
            );
        let op: Arc<dyn ScanOp> = Arc::new(TestMorselScanOp {
            morsels: vec![vec![1, 2, 3, 4]],
        });
        let scan = ScanNode::new_for_test(Arc::clone(&op))
            .with_connector_io_tasks_per_scan_operator(Some(1));
        let factory =
            ScanSourceFactory::new_native_with_consumers_for_test(scan, op, arena, consumers);
        let state = RuntimeState::default();
        let mut source = factory.create(1, 0);
        source.prepare().unwrap();
        source.bind_runtime_state(&state).unwrap();

        let start = Instant::now();
        loop {
            let processor = source.as_processor_mut().unwrap();
            if let Some(chunk) = processor.pull_chunk(&state).unwrap() {
                let values = chunk.columns()[0]
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                assert_eq!(values.values(), &[2, 4]);
                break;
            }
            assert!(start.elapsed() < Duration::from_secs(2));
            thread::sleep(Duration::from_millis(1));
        }
    }

    #[test]
    fn native_scan_source_acquires_before_submitting_runner() {
        let source_thread = std::thread::current().id();
        let (consumers, arena, observer) =
            crate::exec::operators::runtime_filter::tests_support::observed_published_consumer_set(
                crate::exec::operators::runtime_filter::tests_support::membership_bundle(&[2, 4]),
            );
        let op: Arc<dyn ScanOp> = Arc::new(TestMorselScanOp {
            morsels: vec![vec![1, 2, 3, 4]],
        });
        let scan = ScanNode::new_for_test(Arc::clone(&op))
            .with_connector_io_tasks_per_scan_operator(Some(1));
        let factory =
            ScanSourceFactory::new_native_with_consumers_for_test(scan, op, arena, consumers);
        let state = RuntimeState::default();
        let mut source = factory.create(1, 0);
        source.prepare().unwrap();
        source.bind_runtime_state(&state).unwrap();
        assert_eq!(observer.calls(), 0, "bind must only subscribe");

        let start = Instant::now();
        loop {
            if source
                .as_processor_mut()
                .unwrap()
                .pull_chunk(&state)
                .unwrap()
                .is_some()
            {
                break;
            }
            assert!(start.elapsed() < Duration::from_secs(2));
            thread::sleep(Duration::from_millis(1));
        }

        assert_eq!(observer.calls(), 1);
        assert_eq!(observer.thread(), Some(source_thread));
    }

    #[test]
    fn native_scan_source_hydrates_dictionary_before_membership_apply() {
        let (consumers, arena) =
            crate::exec::operators::runtime_filter::tests_support::published_utf8_consumer_set(
                crate::exec::operators::runtime_filter::tests_support::utf8_membership_bundle(&[
                    "two", "four",
                ]),
            );
        let op: Arc<dyn ScanOp> = Arc::new(DictionaryScanOp);
        let scan = ScanNode::new_for_test(Arc::clone(&op))
            .with_connector_io_tasks_per_scan_operator(Some(1));
        let factory =
            ScanSourceFactory::new_native_with_consumers_for_test(scan, op, arena, consumers);
        let state = RuntimeState::default();
        let mut source = factory.create(1, 0);
        source.prepare().unwrap();
        source.bind_runtime_state(&state).unwrap();

        let start = Instant::now();
        loop {
            if let Some(chunk) = source
                .as_processor_mut()
                .unwrap()
                .pull_chunk(&state)
                .unwrap()
            {
                assert_eq!(chunk.columns()[0].data_type(), &DataType::Utf8);
                let values = chunk.columns()[0]
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                assert_eq!(values.value(0), "two");
                assert_eq!(values.value(1), "four");
                break;
            }
            assert!(start.elapsed() < Duration::from_secs(2));
            thread::sleep(Duration::from_millis(1));
        }
    }

    #[test]
    fn native_scan_runtime_filter_runs_before_scan_limit_accounting() {
        let (consumers, arena) =
            crate::exec::operators::runtime_filter::tests_support::published_consumer_set(
                crate::exec::operators::runtime_filter::tests_support::membership_bundle(&[3, 4]),
            );
        let op: Arc<dyn ScanOp> = Arc::new(TestMorselScanOp {
            morsels: vec![vec![1, 2], vec![3, 4]],
        });
        let scan = ScanNode::new_for_test(Arc::clone(&op))
            .with_limit(Some(2))
            .with_connector_io_tasks_per_scan_operator(Some(1));
        let factory =
            ScanSourceFactory::new_native_with_consumers_for_test(scan, op, arena, consumers);
        let state = RuntimeState::default();
        let mut source = factory.create(1, 0);
        source.prepare().unwrap();
        source.bind_runtime_state(&state).unwrap();

        let start = Instant::now();
        loop {
            if let Some(chunk) = source
                .as_processor_mut()
                .unwrap()
                .pull_chunk(&state)
                .unwrap()
            {
                let values = chunk.columns()[0]
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                assert_eq!(values.values(), &[3, 4]);
                break;
            }
            assert!(
                !source.is_finished(),
                "scan limit fired before native filtering"
            );
            assert!(start.elapsed() < Duration::from_secs(2));
            thread::sleep(Duration::from_millis(1));
        }
    }

    #[test]
    fn native_scan_ordered_live_factory_groups_nonblocking_ordered_specs() {
        use std::collections::BTreeSet;

        use crate::exec::expr::ExprNode;
        use crate::exec::node::runtime_filter::{
            RuntimeFilterConsumerBinding, RuntimeFilterExecutionContract,
            RuntimeFilterExecutionReduction,
        };
        use crate::runtime_filter::model::contract::{
            ArtifactCapability, ConsumerActivation, LateApplyGranularity, NullSemantics,
        };
        use crate::runtime_filter::port::artifact::ArtifactMembershipSchema;

        let (_, order) = crate::runtime_filter::service::NativeRuntimeFilterExecutionContext::
            installed_ordered_consumer_context_for_exec_test();
        let mut arena = ExprArena::default();
        let expr_id = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int64);
        let membership_schema =
            ArtifactMembershipSchema::new(&DataType::Int64, NullSemantics::NeverMatches)
                .expect("membership schema");
        let blocking = RuntimeFilterConsumerBinding {
            binding_id: 1,
            channel_id: 9,
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
        let ordered = RuntimeFilterConsumerBinding {
            binding_id: 2,
            channel_id: 1,
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
        let op: Arc<dyn ScanOp> = Arc::new(PlainScanOp::new());
        let mut scan = ScanNode::new_for_test(Arc::clone(&op))
            .with_connector_io_tasks_per_scan_operator(Some(1));
        scan.set_native_runtime_filter_specs(vec![blocking, ordered]);

        ScanSourceFactory::new_native(scan, op, Arc::new(arena))
            .expect("native scan factory must group blocking and ordered live specs separately");
    }

    #[test]
    fn native_scan_cycle_forced_membership_stays_in_join_consumer_group() {
        use std::collections::BTreeSet;

        use crate::exec::expr::ExprNode;
        use crate::exec::node::runtime_filter::{
            RuntimeFilterConsumerBinding, RuntimeFilterExecutionContract,
            RuntimeFilterExecutionReduction,
        };
        use crate::runtime_filter::model::contract::{
            ArtifactCapability, ConsumerActivation, LateApplyGranularity, NullSemantics,
        };
        use crate::runtime_filter::port::artifact::ArtifactMembershipSchema;

        let mut arena = ExprArena::default();
        let expr_id = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int64);
        let membership_schema =
            ArtifactMembershipSchema::new(&DataType::Int64, NullSemantics::NeverMatches)
                .expect("membership schema");
        let op: Arc<dyn ScanOp> = Arc::new(PlainScanOp::new());
        let mut scan = ScanNode::new_for_test(Arc::clone(&op))
            .with_connector_io_tasks_per_scan_operator(Some(1));
        scan.set_native_runtime_filter_specs(vec![RuntimeFilterConsumerBinding {
            binding_id: 4,
            channel_id: 2,
            expr_id,
            activation: ConsumerActivation::NonBlockingLive {
                late_apply: LateApplyGranularity::Batch,
            },
            capabilities: BTreeSet::from([
                ArtifactCapability::Membership,
                ArtifactCapability::EmptyDomain,
            ]),
            contract: RuntimeFilterExecutionContract::Membership {
                canonical_schema: Arc::from(membership_schema.canonical_bytes()),
                schema_digest: membership_schema.digest().bytes(),
            },
            reduction: RuntimeFilterExecutionReduction::SetUnion,
        }]);

        ScanSourceFactory::new_native(scan, op, Arc::new(arena))
            .expect("cycle-forced membership must remain a Join consumer");
    }

    #[test]
    fn native_scan_ordered_live_bind_propagates_bound_subscription_to_prepared_runners() {
        use std::collections::BTreeSet;

        use crate::exec::expr::ExprNode;
        use crate::exec::node::runtime_filter::{
            RuntimeFilterConsumerBinding, RuntimeFilterExecutionContract,
            RuntimeFilterExecutionReduction,
        };
        use crate::runtime_filter::model::contract::{
            ArtifactCapability, ConsumerActivation, LateApplyGranularity,
        };

        let (context, order) =
            crate::runtime_filter::service::NativeRuntimeFilterExecutionContext::
                installed_ordered_consumer_context_for_exec_test();
        let mut arena = ExprArena::default();
        let expr_id = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int64);
        let op: Arc<dyn ScanOp> = Arc::new(TestMorselScanOp {
            morsels: vec![vec![1, 2]],
        });
        let mut scan = ScanNode::new_for_test(Arc::clone(&op))
            .with_connector_io_tasks_per_scan_operator(Some(1));
        scan.set_native_runtime_filter_specs(vec![RuntimeFilterConsumerBinding {
            binding_id: 2,
            channel_id: 1,
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
        }]);
        let factory =
            ScanSourceFactory::new_native(scan, op, Arc::new(arena)).expect("native scan factory");
        let mut source = factory.create(1, 0);
        source
            .prepare()
            .expect("prepare creates scan runners before bind");
        let state = RuntimeState::default().with_native_runtime_filter_context(Some(context));
        source
            .bind_runtime_state(&state)
            .expect("bind ordered live subscription");

        let start = Instant::now();
        loop {
            if let Some(chunk) = source
                .as_processor_mut()
                .expect("scan processor")
                .pull_chunk(&state)
                .expect("prepared runner must receive bound ordered live consumer")
            {
                let values = chunk.columns()[0]
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("prepared runner int32 values");
                assert_eq!(values.values(), &[1, 2]);
                break;
            }
            assert!(start.elapsed() < Duration::from_secs(2));
            thread::sleep(Duration::from_millis(1));
        }
    }

    #[test]
    fn native_scan_ordered_live_factory_rejects_file_and_row_group_without_exact_pruner() {
        use std::collections::BTreeSet;

        use crate::exec::expr::ExprNode;
        use crate::exec::node::runtime_filter::{
            RuntimeFilterConsumerBinding, RuntimeFilterExecutionContract,
            RuntimeFilterExecutionReduction,
        };
        use crate::runtime_filter::model::contract::{
            ArtifactCapability, ConsumerActivation, LateApplyGranularity,
        };

        let (_, order) = crate::runtime_filter::service::NativeRuntimeFilterExecutionContext::
            installed_ordered_consumer_context_for_exec_test();
        for late_apply in [LateApplyGranularity::File, LateApplyGranularity::RowGroup] {
            let mut arena = ExprArena::default();
            let expr_id = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int64);
            let op: Arc<dyn ScanOp> = Arc::new(PlainScanOp::new());
            let mut scan = ScanNode::new_for_test(Arc::clone(&op));
            scan.set_native_runtime_filter_specs(vec![RuntimeFilterConsumerBinding {
                binding_id: 2,
                channel_id: 1,
                expr_id,
                activation: ConsumerActivation::NonBlockingLive { late_apply },
                capabilities: BTreeSet::from([ArtifactCapability::OrderedRange]),
                contract: RuntimeFilterExecutionContract::Ordered {
                    keys: order.keys().to_vec().into(),
                    comparator_digest: order.plan_comparator_digest().get(),
                    order_contract_digest: order.digest().bytes(),
                },
                reduction: RuntimeFilterExecutionReduction::TightenOrderedBound,
            }]);
            assert!(
                ScanSourceFactory::new_native(scan, op, Arc::new(arena))
                    .err()
                    .expect("unsupported late-apply granularity must fail")
                    .contains("unsupported late-apply granularity")
            );
        }
    }

    #[test]
    fn native_scan_ordered_live_coexists_with_blocking_join_without_live_wait() {
        use std::collections::BTreeSet;

        use crate::exec::node::runtime_filter::{
            RuntimeFilterConsumerBinding, RuntimeFilterExecutionContract,
            RuntimeFilterExecutionReduction,
        };
        use crate::runtime_filter::model::contract::{
            ArtifactCapability, ConsumerActivation, LateApplyGranularity, NullOrder, SortDirection,
        };
        use crate::runtime_filter::port::subscription::NonBlockingLiveSubscription;

        let (blocking_consumers, arena) =
            crate::exec::operators::runtime_filter::tests_support::published_consumer_set(
                crate::exec::operators::runtime_filter::tests_support::membership_bundle(&[3, 4]),
            );
        let order = crate::runtime_filter::exec::ordered_range_predicate::tests_support::contract(
            DataType::Int32,
            SortDirection::Ascending,
            NullOrder::Last,
        );
        let ordered_spec = RuntimeFilterConsumerBinding {
            binding_id: 12,
            channel_id: 7,
            expr_id: ExprId(0),
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
        let idle = Arc::new(IdleOrderedLiveSubscription {
            polls: AtomicUsize::new(0),
        });
        let typed: Arc<dyn NonBlockingLiveSubscription> = idle.clone();
        let ordered_live_consumers =
            crate::exec::operators::runtime_filter::NativeOrderedLiveConsumerSet::
                from_bound_for_test(
                    vec![ordered_spec],
                    Arc::clone(&arena),
                    vec![typed],
                );
        let op: Arc<dyn ScanOp> = Arc::new(TestMorselScanOp {
            morsels: vec![vec![1, 2, 3, 4]],
        });
        let scan = ScanNode::new_for_test(Arc::clone(&op))
            .with_connector_io_tasks_per_scan_operator(Some(1));
        let factory = ScanSourceFactory::new_native_with_consumer_groups_for_test(
            scan,
            op,
            arena,
            blocking_consumers,
            ordered_live_consumers,
        );
        let state = RuntimeState::default();
        let mut source = factory.create(1, 0);
        source.prepare().expect("prepare mixed native RF scan");
        source
            .bind_runtime_state(&state)
            .expect("bind mixed native RF scan");

        let start = Instant::now();
        loop {
            if let Some(chunk) = source
                .as_processor_mut()
                .expect("scan processor")
                .pull_chunk(&state)
                .expect("pull mixed native RF chunk")
            {
                let values = chunk.columns()[0]
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("mixed native RF int32 values");
                assert_eq!(values.values(), &[3, 4]);
                assert!(idle.polls.load(Ordering::SeqCst) > 0);
                break;
            }
            assert!(
                start.elapsed() < Duration::from_secs(2),
                "idle ordered live consumer must not block scan start"
            );
            thread::sleep(Duration::from_millis(1));
        }
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
        let op: Arc<dyn ScanOp> = Arc::new(TestMorselScanOp {
            morsels: vec![vec![1, 2, 3]],
        });
        let scan = ScanNode::new_for_test(Arc::clone(&op))
            .with_connector_io_tasks_per_scan_operator(Some(1));
        let arena = Arc::new(ExprArena::default());
        let factory = ScanSourceFactory::new_native(scan, op, arena).unwrap();

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

        let op: Arc<dyn ScanOp> = Arc::new(TestMorselScanOp { morsels });
        let scan = ScanNode::new_for_test(Arc::clone(&op))
            .with_connector_io_tasks_per_scan_operator(Some(1))
            .with_limit(Some(25));

        let arena = Arc::new(ExprArena::default());
        let factory = ScanSourceFactory::new_native(scan, op, arena).unwrap();

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
            (25..100).contains(&total_rows),
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

        let op: Arc<dyn ScanOp> = Arc::new(TestMorselScanOp { morsels });
        let scan = ScanNode::new_for_test(Arc::clone(&op))
            .with_connector_io_tasks_per_scan_operator(Some(1))
            .with_limit(Some(25));

        let arena = Arc::new(ExprArena::default());
        let factory = ScanSourceFactory::new_native(scan, op, arena).unwrap();

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
