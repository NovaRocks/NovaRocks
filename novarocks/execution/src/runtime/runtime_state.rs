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
use crate::runtime::fragment::ExecutionFailure;

use std::sync::OnceLock;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, Instant};

use crate::runtime::cache::ExecutionCacheOptions;
#[cfg(test)]
use crate::runtime::fragment::ExecutionResult;
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
    #[cfg(novarocks_dev_controls)]
    exchange_test_observer: Option<std::sync::Arc<dyn crate::runtime::exchange::TestEventObserver>>,
}

impl std::fmt::Debug for RuntimeState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RuntimeState")
            .finish_non_exhaustive()
    }
}

/// Task work delegates to its exact accepted owner. Local execution explicitly
/// owns a separate failure domain and never synthesizes a Task identity.
pub struct RuntimeErrorState {
    control: RuntimeFailureControl,
}
enum RuntimeFailureControl {
    Local(Box<LocalFailureState>),
    Task(std::sync::Arc<crate::runtime::task_control::TaskExecutionControl>),
}
#[derive(Default)]
struct LocalCleanupFacts {
    facts: [Option<novarocks_failure_contract::FailureFact>; 4],
    dropped: u64,
}
impl LocalCleanupFacts {
    fn record(&mut self, fact: novarocks_failure_contract::FailureFact) {
        if let Some(slot) = self.facts.iter_mut().find(|slot| slot.is_none()) {
            *slot = Some(fact);
        } else {
            self.dropped = self.dropped.saturating_add(1);
        }
    }
}
#[derive(Default)]
struct LocalFailureState {
    // A local encoding/evaluation domain has no Task identity to invent.
    health: std::sync::Arc<crate::runtime::execution_health::ExecutionHealth>,
    cleanup: std::sync::Mutex<LocalCleanupFacts>,
    error: std::sync::Mutex<Option<ExecutionFailure>>,
    has_error: std::sync::atomic::AtomicBool,
    stop_observable: std::sync::Arc<crate::runtime::observable::Observable>,
    stopped: std::sync::Condvar,
    #[cfg(test)]
    waiting: std::sync::atomic::AtomicUsize,
}
impl Default for RuntimeErrorState {
    fn default() -> Self {
        Self {
            control: RuntimeFailureControl::Local(Box::default()),
        }
    }
}
impl std::fmt::Debug for RuntimeErrorState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeErrorState")
            .field("error", &self.error())
            .finish_non_exhaustive()
    }
}
impl crate::runtime::fragment::ExecutionFailureReporter for RuntimeErrorState {
    fn report_cleanup(&self, cause: &ExecutionFailure) {
        RuntimeErrorState::report_cleanup(self, cause);
    }
    fn report_unresolved_exit(&self, cause: &ExecutionFailure) {
        RuntimeErrorState::report_unresolved_exit(self, cause);
    }
    fn report_failure(&self, error: ExecutionFailure) {
        self.set_error(error);
    }
}
impl RuntimeErrorState {
    pub(crate) fn task_identity(&self) -> Option<novarocks_execution_contract::TaskIdentity> {
        match &self.control {
            RuntimeFailureControl::Task(control) => Some(control.identity()),
            RuntimeFailureControl::Local(_) => None,
        }
    }
    pub fn for_task(
        control: std::sync::Arc<crate::runtime::task_control::TaskExecutionControl>,
    ) -> Self {
        Self {
            control: RuntimeFailureControl::Task(control),
        }
    }
    /// Local callers supervise their explicitly local execution scope. Task
    /// work reports to its application's shared ExecutionHealth instead.
    pub(crate) fn exit_health_reporter(
        &self,
    ) -> crate::runtime::task_control::ExecutionExitReporter {
        match &self.control {
            RuntimeFailureControl::Task(owner) => owner.exit_health_reporter(),
            RuntimeFailureControl::Local(local) => {
                crate::runtime::task_control::ExecutionExitReporter::new(
                    local.health.clone(),
                    crate::runtime::execution_health::ExecutionExitOrigin::Local,
                )
            }
        }
    }
    pub fn local_exit_fault(&self) -> Option<crate::runtime::execution_health::ExecutionExitFault> {
        match &self.control {
            RuntimeFailureControl::Local(local) => local.health.failure(),
            RuntimeFailureControl::Task(_) => None,
        }
    }
    pub fn report_cleanup(&self, cause: &ExecutionFailure) {
        match &self.control {
            RuntimeFailureControl::Task(owner) => owner.report_cleanup(cause),
            RuntimeFailureControl::Local(local) => {
                use novarocks_failure_contract::FailureProjection;
                let fact = cause.failure_fact();
                local
                    .cleanup
                    .lock()
                    .expect("local cleanup facts lock")
                    .record(fact);
            }
        }
    }
    pub fn report_unresolved_exit(&self, cause: &ExecutionFailure) {
        match &self.control {
            RuntimeFailureControl::Task(owner) => owner.report_unresolved_exit(cause),
            RuntimeFailureControl::Local(local) => {
                use novarocks_failure_contract::FailureProjection;
                local.health.report_unresolved(
                    crate::runtime::execution_health::ExecutionExitOrigin::Local,
                    cause.failure_fact(),
                );
            }
        }
    }
    pub fn set_error(&self, err: impl Into<ExecutionFailure>) {
        let err = err.into();
        match &self.control {
            RuntimeFailureControl::Task(owner) => owner.report_failure(err),
            RuntimeFailureControl::Local(local) => {
                let accepted = {
                    let mut guard = local.error.lock().expect("local runtime error lock");
                    if guard.is_some() {
                        false
                    } else {
                        *guard = Some(err);
                        local.has_error.store(true, Ordering::Release);
                        true
                    }
                };
                if accepted {
                    local.stopped.notify_all();
                    local.stop_observable.notify_observers();
                }
            }
        }
    }
    pub(crate) fn stop_observable(&self) -> std::sync::Arc<crate::runtime::observable::Observable> {
        match &self.control {
            RuntimeFailureControl::Task(owner) => owner.stop_observable(),
            RuntimeFailureControl::Local(local) => local.stop_observable.clone(),
        }
    }
    /// A fragment outcome is a projection of the same Task receipt. Pure
    /// local execution has no Task authority to inspect or manufacture.
    pub(crate) fn task_conclusion(
        &self,
    ) -> Option<
        std::sync::Arc<novarocks_execution_contract::AcceptedTaskConclusion<ExecutionFailure>>,
    > {
        match &self.control {
            RuntimeFailureControl::Task(owner) => owner.accepted(),
            RuntimeFailureControl::Local(_) => None,
        }
    }

    pub fn error(&self) -> Option<ExecutionFailure> {
        match &self.control {
            RuntimeFailureControl::Task(owner) => owner.error(),
            RuntimeFailureControl::Local(local) => {
                if !local.has_error.load(Ordering::Acquire) {
                    return None;
                }
                local
                    .error
                    .lock()
                    .expect("local runtime error lock")
                    .clone()
            }
        }
    }
    /// Hot checkpoints read only the actual owner's presence flag.
    pub(crate) fn is_stopped(&self) -> bool {
        match &self.control {
            RuntimeFailureControl::Task(owner) => owner.is_stopped(),
            RuntimeFailureControl::Local(local) => local.has_error.load(Ordering::Acquire),
        }
    }
    #[cfg(test)]
    pub(crate) fn waiting_count(&self) -> usize {
        match &self.control {
            RuntimeFailureControl::Local(local) => local.waiting.load(Ordering::Acquire),
            RuntimeFailureControl::Task(_) => {
                panic!("Task wait registration belongs to its actual owner")
            }
        }
    }
    #[cfg(test)]
    pub(crate) fn wait_interruptibly(&self, duration: Duration) -> ExecutionResult<()> {
        match &self.control {
            RuntimeFailureControl::Task(owner) => owner.wait_interruptibly(duration),
            RuntimeFailureControl::Local(local) => match local.wait_for_error(duration).as_ref() {
                Some(error) => Err(error.clone()),
                None => Ok(()),
            },
        }
    }
    pub(crate) fn wait_until_stopped(&self, duration: Duration) -> bool {
        match &self.control {
            RuntimeFailureControl::Task(owner) => owner.wait_until_stopped(duration),
            RuntimeFailureControl::Local(local) => local.wait_for_error(duration).is_some(),
        }
    }
}
impl LocalFailureState {
    fn wait_for_error(
        &self,
        duration: Duration,
    ) -> std::sync::MutexGuard<'_, Option<ExecutionFailure>> {
        let guard = self.error.lock().expect("local runtime error lock");
        #[cfg(test)]
        self.waiting.fetch_add(1, Ordering::Release);
        let (guard, _) = self
            .stopped
            .wait_timeout_while(guard, duration, |error| error.is_none())
            .expect("local runtime stop wait");
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
            #[cfg(novarocks_dev_controls)]
            exchange_test_observer: None,
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
            #[cfg(novarocks_dev_controls)]
            exchange_test_observer: self.exchange_test_observer.clone(),
        }
    }
}

impl RuntimeState {
    #[cfg(novarocks_dev_controls)]
    pub fn with_exchange_test_observer(
        mut self,
        observer: Option<std::sync::Arc<dyn crate::runtime::exchange::TestEventObserver>>,
    ) -> Self {
        self.exchange_test_observer = observer;
        self
    }
    #[cfg(novarocks_dev_controls)]
    pub fn exchange_test_observer(
        &self,
    ) -> Option<std::sync::Arc<dyn crate::runtime::exchange::TestEventObserver>> {
        self.exchange_test_observer.clone()
    }

    pub fn for_local_execution(
        query_options: Option<QueryOptions>,
        cache_options: Option<ExecutionCacheOptions>,
        query_id: Option<QueryId>,
        fragment_instance_id: Option<UniqueId>,
        backend_num: Option<i32>,
        mem_tracker: Option<std::sync::Arc<MemTracker>>,
        execution_runtime: Option<std::sync::Arc<ExecutionRuntime>>,
    ) -> Self {
        Self::new(
            query_options,
            cache_options,
            query_id,
            fragment_instance_id,
            backend_num,
            mem_tracker,
            execution_runtime,
            std::sync::Arc::new(RuntimeErrorState::default()),
        )
    }

    pub fn new(
        query_options: Option<QueryOptions>,
        cache_options: Option<ExecutionCacheOptions>,
        query_id: Option<QueryId>,
        fragment_instance_id: Option<UniqueId>,
        backend_num: Option<i32>,
        mem_tracker: Option<std::sync::Arc<MemTracker>>,
        execution_runtime: Option<std::sync::Arc<ExecutionRuntime>>,
        error_state: std::sync::Arc<RuntimeErrorState>,
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
            error_state,
            last_report_exec_state_ns: AtomicI64::new(monotonic_now_ns()),
            query_id,
            fragment_instance_id,
            backend_num,
            mem_tracker,
            runtime_filter_session: None,
            execution_runtime,
            query_memory: None,
            #[cfg(novarocks_dev_controls)]
            exchange_test_observer: None,
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
    /// `StepContext` so they never grab the shared `data_runtime` directly.
    pub fn sink_io_executor(&self) -> Result<IoExecutor, RuntimeBindingFailure> {
        self.execution_runtime
            .as_ref()
            .map(|runtime| runtime.services().sink_io().clone())
            .ok_or(RuntimeBindingFailure::MissingExecutionRuntime)
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
        let first = ExecutionFailure::from(novarocks_functions::KernelFailure::Internal(
            novarocks_functions::KernelDiagnostic::new(
                &("original runtime diagnostic".repeat(1024)),
            ),
        ));
        state.set_error(first.clone());
        state.set_error(novarocks_functions::KernelFailure::Internal(
            novarocks_functions::KernelDiagnostic::new("later runtime diagnostic"),
        ));
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
        state.set_error(novarocks_functions::KernelFailure::Internal(
            novarocks_functions::KernelDiagnostic::new("actual runtime stop"),
        ));
        assert!(waiter.join().expect("stop reader"));
        assert_eq!(state.waiting_count(), 0);
    }

    #[test]
    fn sink_io_executor_from_default_state_runs_on_sink_runtime() {
        let runtime = std::sync::Arc::new(
            ExecutionRuntime::new(
                ExecutionRuntimeConfig {
                    driver_threads: 1,
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
        let state =
            RuntimeState::for_local_execution(None, None, None, None, None, None, Some(runtime));
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

/// An installed runtime capability is absent where execution requires it.
/// This is a binding invariant, distinct from a scheduler or memory refusal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeBindingFailure {
    MissingExecutionRuntime,
    MissingOperatorMemoryTracker {
        operator: novarocks_failure_contract::BoundedDetail,
    },
}
impl std::fmt::Display for RuntimeBindingFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingExecutionRuntime => f.write_str("fragment runtime is unavailable"),
            Self::MissingOperatorMemoryTracker { operator } => write!(
                f,
                "{operator} memory tracker must be bound before its first state"
            ),
        }
    }
}
impl std::error::Error for RuntimeBindingFailure {}
impl novarocks_failure_contract::FailureProjection for RuntimeBindingFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact};
        let reason = match self {
            Self::MissingExecutionRuntime => {
                crate::runtime::fragment::ExecutionReason::ExecutionRuntimeMissing
            }
            Self::MissingOperatorMemoryTracker { .. } => {
                crate::runtime::fragment::ExecutionReason::OperatorMemoryTrackerMissing
            }
        };
        FailureFact::new(FailureClass::Invariant, reason.reason())
    }
}
impl From<RuntimeBindingFailure> for ExecutionFailure {
    fn from(cause: RuntimeBindingFailure) -> Self {
        Self::from_domain(cause)
    }
}

#[cfg(test)]
mod secondary_exit_tests {
    use super::*;
    use novarocks_failure_contract::FailureProjection;

    #[test]
    fn local_cleanup_and_unknown_exit_cannot_create_or_replace_the_execution_error() {
        let state = RuntimeErrorState::default();
        let cause = ExecutionFailure::from(novarocks_functions::KernelFailure::ResourceExhausted);
        state.report_cleanup(&cause);
        state.report_unresolved_exit(&cause);
        assert!(state.error().is_none());
        assert!(!state.is_stopped());
        assert_eq!(
            state.local_exit_fault().unwrap().fact(),
            cause.failure_fact()
        );
        state.set_error(cause.clone());
        let before = state.error().unwrap();
        for _ in 0..8 {
            state.report_cleanup(&cause);
            state.report_unresolved_exit(&cause);
        }
        assert_eq!(state.error(), Some(before));
        let RuntimeFailureControl::Local(local) = &state.control else {
            unreachable!()
        };
        let facts = local.cleanup.lock().unwrap();
        assert!(
            facts
                .facts
                .iter()
                .all(|record| *record == Some(cause.failure_fact()))
        );
        assert_eq!(facts.dropped, 5);
        assert_eq!(
            state.local_exit_fault().unwrap().origin(),
            crate::runtime::execution_health::ExecutionExitOrigin::Local
        );
    }
}
