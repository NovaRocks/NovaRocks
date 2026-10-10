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
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};

use std::sync::OnceLock;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, Instant};

use crate::runtime::cache::ExecutionCacheOptions;
use crate::runtime::mem_tracker::{MemTracker, process_mem_tracker, query_tracker_label};
use crate::runtime::profile::clamp_u128_to_i64;
use crate::runtime::query_options::QueryOptions;
use crate::runtime::{ExecutionRuntime, execution_services::IoExecutor};
use crate::runtime_filter::RuntimeFilterSessionRef;
use novarocks_types::QueryId;
use novarocks_types::UniqueId;

/// RuntimeState is a per-fragment-instance execution context, similar to StarRocks BE RuntimeState.
///
/// Today it mainly provides access to frequently used query options (e.g. `batch_size` / chunk size).
/// More execution-time parameters and state can be migrated here over time.
pub struct RuntimeState {
    query_options: Option<QueryOptions>,
    cache_options: Option<ExecutionCacheOptions>,
    error_state: std::sync::Arc<RuntimeErrorState>,
    last_report_exec_state_ns: AtomicI64,
    query_id: Option<QueryId>,
    fragment_instance_id: Option<UniqueId>,
    backend_num: Option<i32>,
    mem_tracker: Option<std::sync::Arc<MemTracker>>,
    runtime_filter_session: Option<RuntimeFilterSessionRef>,
    execution_runtime: Option<std::sync::Arc<ExecutionRuntime>>,
    query_memory: Option<crate::runtime::query_memory::QueryMemoryBinding>,
}

impl std::fmt::Debug for RuntimeState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RuntimeState")
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Default)]
pub struct RuntimeErrorState {
    error: std::sync::Mutex<Option<ExecutionFailure>>,
    stopped: std::sync::Condvar,
    #[cfg(test)]
    waiting: std::sync::atomic::AtomicUsize,
}

impl RuntimeErrorState {
    pub fn set_error(&self, err: impl Into<ExecutionFailure>) {
        let err = err.into();
        let mut guard = self.error.lock().expect("runtime error lock");
        if guard.is_none() {
            *guard = Some(err);
            self.stopped.notify_all();
        }
    }

    pub fn error(&self) -> Option<ExecutionFailure> {
        self.error.lock().expect("runtime error lock").clone()
    }

    /// Cooperative kernels need presence, not a cloned diagnostic payload.
    /// Keep the same lock and first-wins owner as the lossless error reader.
    pub(crate) fn is_stopped(&self) -> bool {
        self.error.lock().expect("runtime error lock").is_some()
    }

    #[cfg(test)]
    pub(crate) fn waiting_count(&self) -> usize {
        self.waiting.load(Ordering::Acquire)
    }

    /// Wait without holding execution after this exact fragment stops.
    /// The predicate and notification share the error lock, including an error
    /// published before registration and spurious condition-variable wakes.
    pub(crate) fn wait_interruptibly(&self, duration: std::time::Duration) -> ExecutionResult<()> {
        match self.wait_for_error(duration).as_ref() {
            Some(error) => Err(error.clone()),
            None => Ok(()),
        }
    }

    /// Read only the stop fact. The original Condvar still owns registration,
    /// notification and spurious-wake handling; no diagnostic is copied.
    pub(crate) fn wait_until_stopped(&self, duration: std::time::Duration) -> bool {
        self.wait_for_error(duration).is_some()
    }

    fn wait_for_error(
        &self,
        duration: std::time::Duration,
    ) -> std::sync::MutexGuard<'_, Option<ExecutionFailure>> {
        let guard = self.error.lock().expect("runtime error lock");
        #[cfg(test)]
        self.waiting.fetch_add(1, Ordering::Release);
        let (guard, _) = self
            .stopped
            .wait_timeout_while(guard, duration, |error| error.is_none())
            .expect("runtime stop wait");
        #[cfg(test)]
        self.waiting.fetch_sub(1, Ordering::Release);
        guard
    }
}

impl Default for RuntimeState {
    fn default() -> Self {
        Self {
            query_options: None,
            cache_options: None,
            error_state: std::sync::Arc::new(RuntimeErrorState::default()),
            // A newly admitted fragment starts its report interval now. Using
            // the process-relative zero would make the first poll fire
            // immediately once the process had been alive for one interval.
            last_report_exec_state_ns: AtomicI64::new(monotonic_now_ns()),
            query_id: None,
            fragment_instance_id: None,
            backend_num: None,
            mem_tracker: None,
            runtime_filter_session: None,
            execution_runtime: None,
            query_memory: None,
        }
    }
}

impl Clone for RuntimeState {
    fn clone(&self) -> Self {
        Self {
            query_options: self.query_options.clone(),
            cache_options: self.cache_options.clone(),
            error_state: std::sync::Arc::clone(&self.error_state),
            last_report_exec_state_ns: AtomicI64::new(
                self.last_report_exec_state_ns.load(Ordering::Acquire),
            ),
            query_id: self.query_id,
            fragment_instance_id: self.fragment_instance_id,
            backend_num: self.backend_num,
            mem_tracker: self.mem_tracker.clone(),
            runtime_filter_session: self.runtime_filter_session.clone(),
            execution_runtime: self.execution_runtime.clone(),
            query_memory: self.query_memory.clone(),
        }
    }
}

impl RuntimeState {
    pub fn new(
        query_options: Option<QueryOptions>,
        cache_options: Option<ExecutionCacheOptions>,
        query_id: Option<QueryId>,
        fragment_instance_id: Option<UniqueId>,
        backend_num: Option<i32>,
        mem_tracker: Option<std::sync::Arc<MemTracker>>,
        execution_runtime: Option<std::sync::Arc<ExecutionRuntime>>,
    ) -> Self {
        let mem_tracker = mem_tracker.or_else(|| {
            // An execution runtime still gates host-owned query accounting, so
            // a runtime-less RuntimeState stays untracked exactly as before.
            // Only the parent moves: the query subtree hangs off the single
            // process root, under the same label the Backend query-context
            // owner mints, so one query cannot end up as two subtrees whose
            // charges never meet.
            if execution_runtime.is_none() || (query_id.is_none() && fragment_instance_id.is_none())
            {
                return None;
            }
            let process = process_mem_tracker();
            let query_label = query_id
                .map(|id| query_tracker_label(id.high(), id.low()))
                .unwrap_or_else(|| "query_unknown".to_string());
            let query_tracker = MemTracker::new_child(query_label, &process);
            let fragment_label = fragment_instance_id
                .map(|id| format!("fragment_{:x}_{:x}", id.high(), id.low()))
                .unwrap_or_else(|| "fragment_unknown".to_string());
            Some(MemTracker::new_child(fragment_label, &query_tracker))
        });
        Self {
            query_options,
            cache_options,
            error_state: std::sync::Arc::new(RuntimeErrorState::default()),
            last_report_exec_state_ns: AtomicI64::new(monotonic_now_ns()),
            query_id,
            fragment_instance_id,
            backend_num,
            mem_tracker,
            runtime_filter_session: None,
            execution_runtime,
            query_memory: None,
        }
    }

    pub(crate) fn with_query_memory(
        mut self,
        binding: Option<crate::runtime::query_memory::QueryMemoryBinding>,
    ) -> Self {
        self.query_memory = binding;
        self
    }
    pub(crate) fn query_memory(&self) -> Option<&crate::runtime::query_memory::QueryMemoryBinding> {
        self.query_memory.as_ref()
    }
    pub fn with_runtime_filter_session(mut self, session: Option<RuntimeFilterSessionRef>) -> Self {
        self.runtime_filter_session = session;
        self
    }

    pub(crate) fn runtime_filter_session(&self) -> Option<&RuntimeFilterSessionRef> {
        self.runtime_filter_session.as_ref()
    }

    #[allow(dead_code)]
    pub fn query_options(&self) -> Option<&QueryOptions> {
        self.query_options.as_ref()
    }

    pub fn cache_options(&self) -> Option<&ExecutionCacheOptions> {
        self.cache_options.as_ref()
    }

    pub(crate) fn mem_tracker(&self) -> Option<std::sync::Arc<MemTracker>> {
        self.mem_tracker.clone()
    }

    pub fn backend_num(&self) -> Option<i32> {
        self.backend_num
    }

    pub(crate) fn runtime_filter_scan_wait_timeout(&self) -> Option<Duration> {
        let opts = self.query_options.as_ref();
        let scan_wait = opts
            .and_then(|opts| opts.runtime_filter_scan_wait_time_ms)
            .and_then(|v| (v >= 0).then_some(v));
        let override_scan = self
            .execution_runtime
            .as_ref()
            .and_then(|runtime| runtime.config().runtime_filter_scan_wait_time_ms_override)
            .and_then(|v| (v >= 0).then_some(v));
        let ms = override_scan.or(scan_wait)?;
        Some(Duration::from_millis(ms as u64))
    }

    pub(crate) fn runtime_filter_wait_timeout(&self) -> Option<Duration> {
        const DEFAULT_WAIT_TIMEOUT_MS: i64 = 1000;
        let opts = self.query_options.as_ref();
        let wait_timeout = opts
            .and_then(|opts| opts.runtime_filter_wait_timeout_ms)
            .and_then(|v| (v >= 0).then_some(v as i64));
        let override_wait = self
            .execution_runtime
            .as_ref()
            .and_then(|runtime| runtime.config().runtime_filter_wait_timeout_ms_override)
            .and_then(|v| (v >= 0).then_some(v));
        let ms = override_wait
            .or(wait_timeout)
            .unwrap_or(DEFAULT_WAIT_TIMEOUT_MS);
        Some(Duration::from_millis(ms as u64))
    }

    pub fn error_state(&self) -> std::sync::Arc<RuntimeErrorState> {
        std::sync::Arc::clone(&self.error_state)
    }

    /// Handle to the dedicated sink I/O execution service (IW-1).
    ///
    /// Operators reach this via `&RuntimeState` in `bind_runtime_state` /
    /// `push_chunk` so they never grab the shared `data_runtime` directly.
    pub fn sink_io_executor(&self) -> Result<IoExecutor, String> {
        self.execution_runtime
            .as_ref()
            .map(|runtime| runtime.services().sink_io().clone())
            .ok_or_else(|| "fragment runtime is unavailable".to_string())
    }

    pub fn execution_runtime(&self) -> Option<&ExecutionRuntime> {
        self.execution_runtime.as_deref()
    }

    pub fn error(&self) -> Option<ExecutionFailure> {
        self.error_state.error()
    }

    /// Return the maximum row count per in-memory chunk/RecordBatch.
    ///
    /// StarRocks BE uses query option `batch_size` (aka `RuntimeState::chunk_size()`).
    pub fn chunk_size(&self) -> usize {
        self.query_options
            .as_ref()
            .and_then(|opts| opts.batch_size)
            .filter(|v| *v > 0)
            .map(|v| v as usize)
            .unwrap_or(4096)
            .max(1)
    }

    pub fn runtime_profile_report_interval_ns(&self) -> Option<i64> {
        let opts = self.query_options.as_ref()?;
        if !opts.enable_profile {
            return None;
        }
        let interval_s = opts.runtime_profile_report_interval.unwrap_or(0);
        if interval_s <= 0 {
            return None;
        }
        Some(interval_s.saturating_mul(1_000_000_000))
    }

    pub fn should_report_exec_state(&self) -> bool {
        let Some(interval_ns) = self.runtime_profile_report_interval_ns() else {
            return false;
        };
        let now_ns = monotonic_now_ns();
        let last_ns = self.last_report_exec_state_ns.load(Ordering::Acquire);
        if now_ns.saturating_sub(last_ns) < interval_ns {
            return false;
        }
        self.last_report_exec_state_ns
            .compare_exchange(last_ns, now_ns, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }
}

fn monotonic_now_ns() -> i64 {
    static START: OnceLock<Instant> = OnceLock::new();
    let start = START.get_or_init(Instant::now);
    clamp_u128_to_i64(start.elapsed().as_nanos())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::execution_runtime::ExecutionRuntimeConfig;

    #[test]
    fn stop_presence_and_wait_keep_first_diagnostic_and_original_lossless_reader() {
        let state = RuntimeErrorState::default();
        assert!(!state.is_stopped());
        assert!(!state.wait_until_stopped(Duration::ZERO));
        let first = ExecutionFailure::from("original runtime diagnostic".repeat(1024));
        state.set_error(first.clone());
        state.set_error("later runtime diagnostic");
        assert!(state.is_stopped());
        assert!(state.wait_until_stopped(Duration::from_secs(30)));
        assert_eq!(state.error(), Some(first.clone()));
        assert_eq!(state.wait_interruptibly(Duration::ZERO), Err(first));
        assert_eq!(state.waiting_count(), 0);
    }

    #[test]
    fn stop_presence_wait_registration_is_woken_by_the_same_owner() {
        let state = std::sync::Arc::new(RuntimeErrorState::default());
        let reader = std::sync::Arc::clone(&state);
        let waiter = std::thread::spawn(move || reader.wait_until_stopped(Duration::from_secs(30)));
        let until = Instant::now() + Duration::from_secs(5);
        while state.waiting_count() == 0 && Instant::now() < until {
            std::thread::yield_now();
        }
        assert_eq!(state.waiting_count(), 1);
        state.set_error("actual runtime stop");
        assert!(waiter.join().expect("stop reader"));
        assert_eq!(state.waiting_count(), 0);
    }

    #[test]
    fn sink_io_executor_from_default_state_runs_on_sink_runtime() {
        let runtime = std::sync::Arc::new(
            ExecutionRuntime::new(
                ExecutionRuntimeConfig {
                    driver_threads: 1,
                    exchange_wait_ms: 120_000,
                    exchange_io_threads: 1,
                    exchange_io_max_inflight_bytes: 1,
                    exchange_max_transmit_batched_bytes: 1,
                    operator_buffer_chunks: 1,
                    local_exchange_buffer_mem_limit_per_driver: 1,
                    local_exchange_max_buffered_rows: 1,
                    runtime_filter_scan_wait_time_ms_override: None,
                    runtime_filter_wait_timeout_ms_override: None,
                    sink_io_worker_threads: 1,
                    sink_io_max_blocking_threads: 1,
                },
                crate::runtime::execution_runtime::test_execution_function_set(),
                crate::runtime::execution_runtime::test_memory_authority(),
            )
            .expect("test runtime"),
        );
        let state = RuntimeState::new(None, None, None, None, None, None, Some(runtime));
        let exec = state.sink_io_executor().expect("sink_io executor");
        let handle = exec.spawn(async {
            std::thread::current()
                .name()
                .map(|s| s.to_string())
                .unwrap_or_default()
        });
        let name = futures::executor::block_on(handle).expect("join");
        assert!(
            name.contains("novarocks-sink-io"),
            "sink_io task ran on unexpected thread: {name}"
        );
    }
}
