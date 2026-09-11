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
//! Pipeline driver execution loop.
//!
//! Responsibilities:
//! - Runs source/processor/sink operators with cooperative scheduling semantics.
//! - Tracks driver state transitions, blocking reasons, and execution quotas.
//!
//! Key exported interfaces:
//! - Types: `DriverState`, `DriverScheduleState`, `PipelineDriver`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use super::operator::{
    BlockedReason, DictionaryCarrierStats, DriverBlockDeadline, Operator, ProcessorOperator,
    dictionary_carrier_stats, hydrate_for_downstream,
};
use crate::exec::chunk::Chunk;
use crate::exec::pipeline::dependency::DependencyHandle;
use crate::exec::pipeline::schedule::observer::Observable;
use crate::runtime::fragment::io::{
    FragmentEvent, FragmentEventSink, FragmentProgress, NoopFragmentEventSink,
};
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::profile::Profiler;
use crate::runtime::profile::{CounterRef, OperatorProfiles, ProfileUnit, clamp_u128_to_i64};
use crate::runtime::runtime_state::RuntimeState;
use novarocks_types::{UniqueId, format_uuid};
use tracing::{debug, error};

#[derive(Clone, Debug, PartialEq, Eq)]
/// Runtime state for a single pipeline driver.
///
/// **State machine (high level)**
/// ```text
///              (scheduled)                 (time slice ends)
///   Ready ───────────────────► Running ─────────────────────► Ready
///                               │  │
///                               │  ├─ blocks on I/O/deps ───► Blocked(reason)
///                               │  │                         │
///                               │  │        (resumed)         │
///                               │  └─────────────────────────┘
///                               │
///                               ├─ completes normally ───────► Finished
///                               ├─ canceled ─────────────────► Canceled
///                               └─ fatal error ──────────────► Failed(err)
/// ```
pub enum DriverState {
    Ready,
    Running,
    Blocked(BlockedReason),
    PendingFinish,
    Finished,
    Canceled,
    Failed(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DriverBlockedKind {
    InputEmpty,
    OutputFull,
    Dependency,
}

enum WorkerBlockDecision {
    Runnable,
    NoBlocker,
    Retry,
    Blocked(Arc<Observable>, u64, Option<DriverBlockDeadline>),
}

enum StableObservableSnapshot {
    Stable(Arc<Observable>, u64),
    Changed,
    Missing,
}

fn stable_observable_snapshot(
    before: Option<Arc<Observable>>,
    generation: Option<u64>,
    after: Option<Arc<Observable>>,
) -> StableObservableSnapshot {
    match (before, generation, after) {
        (Some(before), Some(generation), Some(after))
            if Arc::ptr_eq(&before, &after)
                && before.generation() == generation
                && after.generation() == generation =>
        {
            StableObservableSnapshot::Stable(before, generation)
        }
        (None, None, None) => StableObservableSnapshot::Missing,
        _ => StableObservableSnapshot::Changed,
    }
}

#[derive(Debug)]
/// Scheduling metadata for one driver including blocking and requeue state.
pub(crate) struct DriverScheduleState {
    in_blocked: AtomicBool,
    source_observables: Mutex<Vec<Weak<Observable>>>,
    sink_observables: Mutex<Vec<Weak<Observable>>>,
}

impl DriverScheduleState {
    pub(crate) fn new() -> Self {
        Self {
            in_blocked: AtomicBool::new(false),
            source_observables: Mutex::new(Vec::new()),
            sink_observables: Mutex::new(Vec::new()),
        }
    }

    #[cfg(test)]
    pub(crate) fn is_in_blocked(&self) -> bool {
        self.in_blocked.load(Ordering::Acquire)
    }

    pub(crate) fn set_in_blocked(&self, value: bool) {
        self.in_blocked.store(value, Ordering::Release);
    }

    pub(crate) fn try_mark_source_observer_registered(&self, observable: &Arc<Observable>) -> bool {
        Self::try_mark_observer(&self.source_observables, observable)
    }

    pub(crate) fn try_mark_sink_observer_registered(&self, observable: &Arc<Observable>) -> bool {
        Self::try_mark_observer(&self.sink_observables, observable)
    }

    fn try_mark_observer(
        registered: &Mutex<Vec<Weak<Observable>>>,
        observable: &Arc<Observable>,
    ) -> bool {
        // A driver can expose different readiness observables as internal
        // processors enter and leave backpressure. Track each live identity,
        // but never keep an operator-owned observable alive from the scheduler.
        let candidate = Arc::downgrade(observable);
        let mut registered = registered.lock().expect("driver observable registry lock");
        registered.retain(|existing| existing.strong_count() != 0);
        if registered
            .iter()
            .any(|existing| Weak::ptr_eq(existing, &candidate))
        {
            return false;
        }
        registered.push(candidate);
        true
    }
}

fn is_scan_operator_name(name: &str) -> bool {
    name.contains("_SCAN")
}

fn is_exchange_receiver_operator_name(name: &str) -> bool {
    name.contains("EXCHANGE") && !is_exchange_sender_operator_name(name)
}

fn is_exchange_sender_operator_name(name: &str) -> bool {
    name.starts_with("DATA_STREAM_SINK") || name.starts_with("EXCHANGE_SINK")
}

/// Cooperative execution driver that runs source/processor/sink operators for one pipeline instance.
pub struct PipelineDriver {
    driver_id: i32,
    operators: Vec<Box<dyn Operator>>,
    profiler: Option<Profiler>,
    driver_total_time: Option<CounterRef>,
    driver_blocked_time: Option<CounterRef>,
    driver_input_empty_time: Option<CounterRef>,
    driver_output_full_time: Option<CounterRef>,
    driver_dependency_wait_time: Option<CounterRef>,
    operator_counters: Vec<OperatorCounters>,
    runtime_state: Arc<RuntimeState>,
    fragment_instance_id: Option<(i64, i64)>,
    event_sink: Arc<dyn FragmentEventSink>,
    state: DriverState,
    blocked_since: Option<(Instant, DriverBlockedKind)>,
    closed: bool,
    schedule_state: Arc<DriverScheduleState>,
    blocked_observable: Option<(Arc<Observable>, u64, Option<DriverBlockDeadline>)>,
    pending_finish_state: Option<DriverState>,
    operator_terminal_signal: Option<DriverState>,

    edge_chunks: Vec<Option<Chunk>>,
    edge_closed: Vec<bool>,
    operator_finishing_set: Vec<bool>,
    operator_mem_trackers: Vec<Option<Arc<MemTracker>>>,
    edge_mem_trackers: Vec<Option<Arc<MemTracker>>>,
}

/// Fragment-owned services bound before operator preparation.
///
/// Keeping these bindings together prevents driver construction from growing a
/// positional argument for every service that must be installed before an
/// asynchronous operator can start.
pub(crate) struct PipelineDriverBindings {
    event_sink: Arc<dyn FragmentEventSink>,
    prebound_operator_mem_trackers: Option<Vec<Option<Arc<MemTracker>>>>,
}

impl PipelineDriverBindings {
    pub(crate) fn new(
        event_sink: Arc<dyn FragmentEventSink>,
        prebound_operator_mem_trackers: Option<Vec<Option<Arc<MemTracker>>>>,
    ) -> Self {
        Self {
            event_sink,
            prebound_operator_mem_trackers,
        }
    }
}

#[derive(Clone)]
struct OperatorCounters {
    operator_total_time: CounterRef,
    push_total_time: CounterRef,
    pull_total_time: CounterRef,
    set_finishing_time: CounterRef,
    close_time: CounterRef,
    push_row_num: CounterRef,
    pull_row_num: CounterRef,
    mem_peak: CounterRef,
    mem_allocated: CounterRef,
    dict_input_rows: CounterRef,
    dict_input_columns: CounterRef,
    dict_kept_rows: CounterRef,
    dict_kept_columns: CounterRef,
    dict_hydrated_rows: CounterRef,
    dict_hydrated_columns: CounterRef,
    dict_unsupported_columns: CounterRef,
    io_task_exec_time: Option<CounterRef>,
    wait_time: Option<CounterRef>,
    receiver_process_total_time: Option<CounterRef>,
    network_time: Option<CounterRef>,
}

fn record_dictionary_carrier_stats(counters: &OperatorCounters, stats: DictionaryCarrierStats) {
    if !stats.has_input() {
        return;
    }
    counters.dict_input_rows.add(stats.input_rows);
    counters.dict_input_columns.add(stats.input_columns);
    counters.dict_kept_rows.add(stats.kept_rows);
    counters.dict_kept_columns.add(stats.kept_columns);
    counters.dict_hydrated_rows.add(stats.hydrated_rows);
    counters.dict_hydrated_columns.add(stats.hydrated_columns);
    counters
        .dict_unsupported_columns
        .add(stats.unsupported_columns);
}

impl PipelineDriver {
    pub fn new(
        driver_id: i32,
        operators: Vec<Box<dyn Operator>>,
        profiler: Option<Profiler>,
        operator_profiles: Vec<OperatorProfiles>,
        runtime_state: Arc<RuntimeState>,
        fragment_instance_id: Option<(i64, i64)>,
    ) -> Self {
        Self::new_with_event_sink(
            driver_id,
            operators,
            profiler,
            operator_profiles,
            runtime_state,
            fragment_instance_id,
            PipelineDriverBindings::new(Arc::new(NoopFragmentEventSink), None),
        )
    }

    pub(crate) fn new_with_event_sink(
        driver_id: i32,
        operators: Vec<Box<dyn Operator>>,
        profiler: Option<Profiler>,
        operator_profiles: Vec<OperatorProfiles>,
        runtime_state: Arc<RuntimeState>,
        fragment_instance_id: Option<(i64, i64)>,
        bindings: PipelineDriverBindings,
    ) -> Self {
        let PipelineDriverBindings {
            event_sink,
            prebound_operator_mem_trackers,
        } = bindings;
        let mut operators = operators;
        let operator_count = operators.len();
        let edge_count = operator_count.saturating_sub(1);
        let driver_total_time = profiler.as_ref().map(|p| p.add_timer("DriverTotalTime"));
        let driver_blocked_time = profiler.as_ref().map(|p| p.add_timer("DriverBlockedTime"));
        let driver_input_empty_time = profiler
            .as_ref()
            .map(|p| p.add_timer("DriverInputEmptyTime"));
        let driver_output_full_time = profiler
            .as_ref()
            .map(|p| p.add_timer("DriverOutputFullTime"));
        let driver_dependency_wait_time = profiler
            .as_ref()
            .map(|p| p.add_timer("DriverDependencyWaitTime"));
        let operator_counters = if profiler.is_some() {
            debug_assert_eq!(
                operators.len(),
                operator_profiles.len(),
                "operator_profiles must be created when profiler is enabled"
            );
            operators
                .iter()
                .map(|op| op.name().to_string())
                .zip(operator_profiles.iter())
                .map(|(name, p)| {
                    let io_task_exec_time = if is_scan_operator_name(&name) {
                        let _ = p.unique.add_timer("IOTaskWaitTime");
                        let _ = p.unique.add_timer("ScanTime");
                        Some(p.unique.add_timer("IOTaskExecTime"))
                    } else {
                        None
                    };
                    let wait_time = if is_exchange_receiver_operator_name(&name) {
                        Some(p.unique.add_timer("WaitTime"))
                    } else {
                        None
                    };
                    let receiver_process_total_time = if is_exchange_receiver_operator_name(&name) {
                        Some(p.unique.add_timer("ReceiverProcessTotalTime"))
                    } else {
                        None
                    };
                    let network_time = if is_exchange_sender_operator_name(&name) {
                        Some(p.unique.add_timer("NetworkTime"))
                    } else {
                        None
                    };
                    OperatorCounters {
                        operator_total_time: p.common.add_timer("OperatorTotalTime"),
                        push_total_time: p.common.add_timer("PushTotalTime"),
                        pull_total_time: p.common.add_timer("PullTotalTime"),
                        set_finishing_time: p.common.add_timer("SetFinishingTime"),
                        close_time: p.common.add_timer("CloseTime"),
                        push_row_num: p.common.add_counter("PushRowNum", ProfileUnit::Unit),
                        pull_row_num: p.common.add_counter("PullRowNum", ProfileUnit::Unit),
                        mem_peak: p
                            .common
                            .add_counter("OperatorPeakMemoryUsage", ProfileUnit::Bytes),
                        mem_allocated: p
                            .common
                            .add_counter("OperatorAllocatedMemoryUsage", ProfileUnit::Bytes),
                        dict_input_rows: p.unique.add_counter("DictInputRows", ProfileUnit::Unit),
                        dict_input_columns: p
                            .unique
                            .add_counter("DictInputColumns", ProfileUnit::Unit),
                        dict_kept_rows: p.unique.add_counter("DictKeptRows", ProfileUnit::Unit),
                        dict_kept_columns: p
                            .unique
                            .add_counter("DictKeptColumns", ProfileUnit::Unit),
                        dict_hydrated_rows: p
                            .unique
                            .add_counter("DictHydratedRows", ProfileUnit::Unit),
                        dict_hydrated_columns: p
                            .unique
                            .add_counter("DictHydratedColumns", ProfileUnit::Unit),
                        dict_unsupported_columns: p
                            .unique
                            .add_counter("DictUnsupportedColumns", ProfileUnit::Unit),
                        io_task_exec_time,
                        wait_time,
                        receiver_process_total_time,
                        network_time,
                    }
                })
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        let mem_root = runtime_state.mem_tracker();
        let operators_prebound = prebound_operator_mem_trackers.is_some();
        let operator_mem_trackers = prebound_operator_mem_trackers.unwrap_or_else(|| {
            if let Some(root) = mem_root.as_ref() {
                operators
                    .iter()
                    .enumerate()
                    .map(|(idx, op)| {
                        let label = format!("operator {}: {}", idx, op.name());
                        Some(MemTracker::new_child(label, root))
                    })
                    .collect()
            } else {
                vec![None; operator_count]
            }
        });
        assert_eq!(
            operator_mem_trackers.len(),
            operator_count,
            "prebound operator memory tracker count"
        );
        let edge_mem_trackers = if let Some(root) = mem_root.as_ref() {
            (0..edge_count)
                .map(|idx| {
                    let label = format!(
                        "edge {}: {} -> {}",
                        idx,
                        operators[idx].name(),
                        operators[idx + 1].name()
                    );
                    Some(MemTracker::new_child(label, root))
                })
                .collect()
        } else {
            vec![None; edge_count]
        };
        for (idx, op) in operators.iter_mut().enumerate() {
            if !operators_prebound
                && let Some(tracker) = operator_mem_trackers.get(idx).and_then(|v| v.as_ref())
            {
                op.set_mem_tracker(Arc::clone(tracker));
            }
            op.set_fragment_event_sink(Arc::clone(&event_sink));
        }
        Self {
            driver_id,
            operators,
            profiler,
            driver_total_time,
            driver_blocked_time,
            driver_input_empty_time,
            driver_output_full_time,
            driver_dependency_wait_time,
            operator_counters,
            runtime_state,
            fragment_instance_id,
            event_sink,
            state: DriverState::Ready,
            blocked_since: None,
            closed: false,
            schedule_state: Arc::new(DriverScheduleState::new()),
            blocked_observable: None,
            pending_finish_state: None,
            operator_terminal_signal: None,

            edge_chunks: vec![None; edge_count],
            edge_closed: vec![false; edge_count],
            operator_finishing_set: vec![false; operator_count],
            operator_mem_trackers,
            edge_mem_trackers,
        }
    }

    pub fn print_pipeline_structure(&self) {
        let op_names: Vec<String> = self
            .operators
            .iter()
            .map(|op| op.name().to_string())
            .collect();
        tracing::info!(
            "PipelineDriver structure: driver_id={} fragment_instance_id={:?} operators={:?}",
            self.driver_id,
            self.fragment_instance_id,
            op_names
        );
    }

    pub fn driver_id(&self) -> i32 {
        self.driver_id
    }

    pub(crate) fn fragment_instance_id(&self) -> Option<(i64, i64)> {
        self.fragment_instance_id
    }

    pub fn state(&self) -> &DriverState {
        &self.state
    }

    #[cfg(test)]
    pub(crate) fn schedule_state(&self) -> Arc<DriverScheduleState> {
        Arc::clone(&self.schedule_state)
    }

    pub(crate) fn try_mark_source_observer_registered(&self, observable: &Arc<Observable>) -> bool {
        self.schedule_state
            .try_mark_source_observer_registered(observable)
    }

    pub(crate) fn try_mark_sink_observer_registered(&self, observable: &Arc<Observable>) -> bool {
        self.schedule_state
            .try_mark_sink_observer_registered(observable)
    }

    pub(crate) fn set_in_blocked(&self, value: bool) {
        self.schedule_state.set_in_blocked(value);
    }

    pub(crate) fn has_pending_finish(&self) -> bool {
        self.operators.iter().any(|op| op.pending_finish())
    }

    fn cancel_operators(&mut self) {
        for op in self.operators.iter_mut() {
            op.cancel();
        }
    }

    /// Run terminal operator cancellation before an externally cancelled task
    /// is dropped by the global executor.
    ///
    /// The executor can observe a fragment abort while a driver is parked and
    /// discard that task without another `process` turn.  Cancellation must
    /// still reach source-owned resources such as connector reader groups.
    pub(crate) fn cancel_for_fragment_abort(&mut self) -> DriverState {
        // An externally aborted parked task does not receive another normal
        // `process` turn before the executor accounts it as complete. Route it
        // through the same PendingFinish latch as in-driver cancellation so an
        // async owner can finish its bounded cooperative abort first.
        self.finish_with_state(DriverState::Canceled)
    }

    fn fail_operators(&mut self) {
        for op in self.operators.iter_mut() {
            op.on_driver_failure();
        }
    }

    pub fn process(&mut self, time_slice: Duration) -> DriverState {
        let driver_start = Instant::now();
        if self.state == DriverState::Ready {
            // Print structure on first run or when explicitly needed.
            // To avoid spam, we might want to do this only once per driver.
            // But process() is called frequently.
            // Ideally we call this in Executor when creating the task.
            // For now let's just log it once in new() via a separate call or hack it here.
            // Actually, let's just log it in new() if possible, but new() is static.
            // So we'll rely on the user to check logs.
        }
        let state = self.process_inner(time_slice);
        let elapsed_ns = clamp_u128_to_i64(driver_start.elapsed().as_nanos());
        if let Some(counter) = self.driver_total_time.as_ref() {
            counter.add(elapsed_ns);
        }
        state
    }

    fn process_inner(&mut self, time_slice: Duration) -> DriverState {
        if let Some(err) = self.runtime_state.error() {
            return self.finish_with_state(DriverState::Failed(err));
        }
        if let Some(final_state) = self.pending_finish_state.clone() {
            if self.has_pending_finish() {
                self.state = DriverState::PendingFinish;
                return self.state.clone();
            }
            self.pending_finish_state = None;
            // An output-producing async processor can finish its background
            // work while still holding a final page for its downstream. Resume
            // the dataflow before completing a successful driver; terminal
            // failure/cancellation still keeps its original first-wins state.
            if matches!(final_state, DriverState::Finished) && !self.is_finished() {
                self.state = DriverState::Running;
            } else {
                return self.finish_with_state_after_operator_signal(final_state);
            }
        }

        let start = Instant::now();
        self.state = DriverState::Running;

        loop {
            if let Some(err) = self.runtime_state.error() {
                return self.finish_with_state(DriverState::Failed(err));
            }
            if start.elapsed() >= time_slice {
                self.state = DriverState::Ready;
                return self.state.clone();
            }

            if self.is_finished() || self.has_pending_finish() {
                return self.finish_with_state(DriverState::Finished);
            }

            if let Some(dep) = self.find_precondition_dependency() {
                return self.block_or_fail(BlockedReason::Dependency(dep));
            }

            let mut made_progress = false;

            if let Err(err) = self.propagate_edge_closure(&mut made_progress) {
                return self.finish_with_state(DriverState::Failed(err));
            }
            if let Err(err) = self.drive_set_finishing(&mut made_progress) {
                return self.finish_with_state(DriverState::Failed(err));
            }
            if let Err(err) = self.drive_dataflow(&mut made_progress) {
                return self.finish_with_state(DriverState::Failed(err));
            }

            if made_progress {
                continue;
            }

            // Readiness inspection below is intentionally worker-owned. Every
            // blocked decision brackets the active operator checks with the
            // exact observable identity and generation that can invalidate it.
            // The scheduler consumes only the frozen snapshot and never polls
            // operators.
            let has_pending_data = self.edge_chunks.iter().any(|chunk| chunk.is_some())
                || self.has_internal_output_on_worker();

            if !has_pending_data {
                match self.source_block_decision_on_worker() {
                    Ok(WorkerBlockDecision::Blocked(observable, generation, deadline)) => {
                        return self.block_on_observable(
                            BlockedReason::InputEmpty,
                            observable,
                            generation,
                            deadline,
                        );
                    }
                    Ok(WorkerBlockDecision::Retry) => continue,
                    Ok(WorkerBlockDecision::Runnable | WorkerBlockDecision::NoBlocker) => {}
                    Err(error) => {
                        return self.finish_with_state(DriverState::Failed(error));
                    }
                }
            }

            // A ready source does not imply that its first consumer can accept
            // another page. Check internal capacity after source readiness so a
            // source-ready/internal-full pipeline parks on the consumer event
            // instead of cycling through the global ready queue.
            match self.internal_sink_block_decision_on_worker() {
                Ok(WorkerBlockDecision::Blocked(observable, generation, deadline)) => {
                    return self.block_on_observable(
                        BlockedReason::OutputFull,
                        observable,
                        generation,
                        deadline,
                    );
                }
                Ok(WorkerBlockDecision::Retry) => continue,
                Ok(WorkerBlockDecision::Runnable | WorkerBlockDecision::NoBlocker) => {}
                Err(error) => {
                    return self.finish_with_state(DriverState::Failed(error));
                }
            }

            match self.terminal_sink_block_decision_on_worker() {
                Ok(WorkerBlockDecision::Blocked(observable, generation, deadline)) => {
                    return self.block_on_observable(
                        BlockedReason::OutputFull,
                        observable,
                        generation,
                        deadline,
                    );
                }
                Ok(WorkerBlockDecision::Retry) => continue,
                Ok(WorkerBlockDecision::Runnable | WorkerBlockDecision::NoBlocker) => {}
                Err(error) => return self.finish_with_state(DriverState::Failed(error)),
            }

            self.state = DriverState::Ready;
            return self.state.clone();
        }
    }

    fn find_precondition_dependency(&self) -> Option<DependencyHandle> {
        for op in &self.operators {
            if op.is_finished() {
                continue;
            }
            let Some(proc) = op.as_processor_ref() else {
                continue;
            };
            let Some(dep) = proc.precondition_dependency() else {
                continue;
            };
            if dep.is_ready() {
                continue;
            }
            return Some(dep);
        }
        None
    }

    fn source_observable_on_worker(&self) -> Option<Arc<Observable>> {
        let op = self.operators.first()?;
        let proc = op.as_processor_ref()?;
        proc.source_observable()
    }

    fn terminal_sink_observable_on_worker(&self) -> Option<Arc<Observable>> {
        let op = self.operators.last()?;
        let proc = op.as_processor_ref()?;
        proc.sink_observable()
    }

    pub(crate) fn blocked_observable_snapshot(
        &self,
    ) -> Option<(Arc<Observable>, u64, Option<DriverBlockDeadline>)> {
        self.blocked_observable
            .as_ref()
            .map(|(observable, generation, deadline)| {
                (Arc::clone(observable), *generation, *deadline)
            })
    }

    pub(crate) fn source_name(&self) -> &str {
        self.operators
            .first()
            .map(|op| op.name())
            .unwrap_or("unknown")
    }

    pub(crate) fn sink_name(&self) -> &str {
        self.operators
            .last()
            .map(|op| op.name())
            .unwrap_or("unknown")
    }

    fn source_ready_on_worker(&self) -> bool {
        let Some(op) = self.operators.first() else {
            return true;
        };
        if op.is_finished() {
            return true;
        }
        let Some(proc) = op.as_processor_ref() else {
            return true;
        };
        if proc.has_output() {
            return true;
        }
        // has_output may advance internal state (e.g. async scan completion); re-check finished.
        op.is_finished()
    }

    fn terminal_sink_ready_on_worker(&self) -> bool {
        let Some(op) = self.operators.last() else {
            return true;
        };
        if op.is_finished() {
            return true;
        }
        let Some(proc) = op.as_processor_ref() else {
            return true;
        };
        // A finishing operator can own output that only another driver turn
        // can push. This check stays inside the observable generation bracket
        // in terminal_sink_block_decision_on_worker, so an external event
        // that changes the answer cannot be lost between finishing and park.
        if proc.finishing_wait().can_progress() {
            return true;
        }
        if proc.need_input() {
            return true;
        }
        // need_input can flip to finished (e.g. when finishing drains); re-check finished.
        op.is_finished()
    }

    fn has_internal_output_on_worker(&self) -> bool {
        if self.operators.len() <= 2 {
            return false;
        }
        let end = self.operators.len().saturating_sub(1);
        self.operators[1..end].iter().any(|operator| {
            !operator.is_finished()
                && operator
                    .as_processor_ref()
                    .is_some_and(ProcessorOperator::has_output)
        })
    }

    fn source_block_decision_on_worker(&self) -> Result<WorkerBlockDecision, String> {
        let before = self.source_observable_on_worker();
        let generation = before.as_ref().map(|observable| observable.generation());
        if self.source_ready_on_worker() {
            return Ok(WorkerBlockDecision::Runnable);
        }
        let after = self.source_observable_on_worker();
        match stable_observable_snapshot(before, generation, after) {
            StableObservableSnapshot::Stable(observable, generation) => {
                let deadline = self
                    .operators
                    .first()
                    .and_then(|operator| operator.as_processor_ref())
                    .and_then(ProcessorOperator::source_block_deadline);
                Ok(WorkerBlockDecision::Blocked(
                    observable, generation, deadline,
                ))
            }
            StableObservableSnapshot::Changed => Ok(WorkerBlockDecision::Retry),
            StableObservableSnapshot::Missing => Err(format!(
                "pipeline source {} is blocked without a readiness observable",
                self.source_name()
            )),
        }
    }

    fn internal_sink_block_decision_on_worker(&self) -> Result<WorkerBlockDecision, String> {
        if self.operators.len() <= 2 {
            return Ok(WorkerBlockDecision::NoBlocker);
        }
        let end = self.operators.len().saturating_sub(1);
        for operator in self.operators[1..end].iter().rev() {
            let Some(processor) = operator.as_processor_ref() else {
                continue;
            };
            let before = processor.sink_observable();
            let generation = before.as_ref().map(|observable| observable.generation());
            if operator.is_finished() {
                continue;
            }
            if processor.finishing_wait().can_progress() {
                return Ok(WorkerBlockDecision::Runnable);
            }
            if processor.has_output() {
                return Ok(WorkerBlockDecision::Runnable);
            }
            if processor.need_input() {
                continue;
            }
            let after = processor.sink_observable();
            match stable_observable_snapshot(before, generation, after) {
                StableObservableSnapshot::Stable(observable, generation) => {
                    return Ok(WorkerBlockDecision::Blocked(observable, generation, None));
                }
                StableObservableSnapshot::Changed => return Ok(WorkerBlockDecision::Retry),
                StableObservableSnapshot::Missing => {
                    // A synchronous processor can transiently accept no input
                    // and expose no output while pipeline-owned finishing or a
                    // dependency publication converges (join probes are a
                    // common example). Without an observable there is no
                    // external capacity event for the scheduler to await, so
                    // this is not an event-driven blocker. Keep inspecting the
                    // remaining pipeline and leave the driver runnable.
                    continue;
                }
            }
        }
        Ok(WorkerBlockDecision::NoBlocker)
    }

    fn terminal_sink_block_decision_on_worker(&self) -> Result<WorkerBlockDecision, String> {
        let before = self.terminal_sink_observable_on_worker();
        let generation = before.as_ref().map(|observable| observable.generation());
        if self.terminal_sink_ready_on_worker() {
            return Ok(WorkerBlockDecision::Runnable);
        }
        let after = self.terminal_sink_observable_on_worker();
        match stable_observable_snapshot(before, generation, after) {
            StableObservableSnapshot::Stable(observable, generation) => {
                Ok(WorkerBlockDecision::Blocked(observable, generation, None))
            }
            StableObservableSnapshot::Changed => Ok(WorkerBlockDecision::Retry),
            StableObservableSnapshot::Missing => Err(format!(
                "pipeline sink {} is blocked without a readiness observable",
                self.sink_name()
            )),
        }
    }

    pub(crate) fn pending_finish_complete(&self) -> bool {
        debug_assert_eq!(self.state, DriverState::PendingFinish);
        !self.has_pending_finish()
    }

    pub(crate) fn set_ready(&mut self) {
        self.finish_blocked_interval();
        self.blocked_observable = None;
        self.state = DriverState::Ready;
    }

    fn is_finished(&self) -> bool {
        self.operators
            .last()
            .map(|op| op.is_finished())
            .unwrap_or(true)
    }

    pub(crate) fn report_exec_state_if_necessary(&self) {
        if self.is_finished() {
            return;
        }
        if !self.runtime_state.should_report_exec_state() {
            return;
        }
        let Some((hi, lo)) = self.fragment_instance_id else {
            return;
        };
        // Progress is observability only. A buggy role adapter must not be
        // able to terminate the driver or turn a successful fragment into a
        // failed one.
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.event_sink
                .record(FragmentEvent::Progress(FragmentProgress::new(
                    UniqueId::new(hi, lo),
                    0,
                    0,
                    0,
                )));
        }));
    }

    fn block_on_observable(
        &mut self,
        reason: BlockedReason,
        observable: Arc<Observable>,
        generation: u64,
        deadline: Option<DriverBlockDeadline>,
    ) -> DriverState {
        debug_assert!(matches!(
            reason,
            BlockedReason::InputEmpty | BlockedReason::OutputFull
        ));
        self.blocked_observable = Some((observable, generation, deadline));
        self.block_or_fail(reason)
    }

    fn block_or_fail(&mut self, reason: BlockedReason) -> DriverState {
        match reason {
            BlockedReason::Dependency(dep) => {
                self.blocked_observable = None;
                self.start_blocked_interval(DriverBlockedKind::Dependency);
                if let Some((hi, lo)) = self.fragment_instance_id {
                    debug!(
                        "Driver blocked on dependency: finst={} driver_id={} dep_name={}",
                        format_uuid(hi, lo),
                        self.driver_id,
                        dep.name()
                    );
                } else {
                    debug!(
                        "Driver blocked on dependency: driver_id={} dep_name={}",
                        self.driver_id,
                        dep.name()
                    );
                }
                self.state = DriverState::Blocked(BlockedReason::Dependency(dep));
                self.state.clone()
            }
            BlockedReason::InputEmpty => {
                debug_assert!(self.blocked_observable.is_some());
                self.start_blocked_interval(DriverBlockedKind::InputEmpty);
                self.state = DriverState::Blocked(BlockedReason::InputEmpty);
                self.state.clone()
            }
            BlockedReason::OutputFull => {
                debug_assert!(self.blocked_observable.is_some());
                self.start_blocked_interval(DriverBlockedKind::OutputFull);
                self.state = DriverState::Blocked(BlockedReason::OutputFull);
                self.state.clone()
            }
        }
    }

    fn start_blocked_interval(&mut self, kind: DriverBlockedKind) {
        if self.blocked_since.is_none() {
            self.blocked_since = Some((Instant::now(), kind));
        }
    }

    fn finish_blocked_interval(&mut self) {
        let Some((blocked_since, kind)) = self.blocked_since.take() else {
            return;
        };
        let elapsed_ns = clamp_u128_to_i64(blocked_since.elapsed().as_nanos());
        if let Some(counter) = self.driver_blocked_time.as_ref() {
            counter.add(elapsed_ns);
        }
        match kind {
            DriverBlockedKind::InputEmpty => {
                if let Some(counter) = self.driver_input_empty_time.as_ref() {
                    counter.add(elapsed_ns);
                }
            }
            DriverBlockedKind::OutputFull => {
                if let Some(counter) = self.driver_output_full_time.as_ref() {
                    counter.add(elapsed_ns);
                }
            }
            DriverBlockedKind::Dependency => {
                if let Some(counter) = self.driver_dependency_wait_time.as_ref() {
                    counter.add(elapsed_ns);
                }
            }
        }
    }

    fn finish_with_state(&mut self, state: DriverState) -> DriverState {
        // Failure/cancellation signals are first-wins and reach each operator
        // exactly once. A driver can be revisited while an asynchronous owner
        // is still pending, so neither the executor nor the poller may replay
        // these callbacks on every scheduling turn.
        let state = match (&self.operator_terminal_signal, &state) {
            (
                Some(existing),
                DriverState::Finished | DriverState::Canceled | DriverState::Failed(_),
            ) => existing.clone(),
            (None, DriverState::Canceled) => {
                self.cancel_operators();
                self.operator_terminal_signal = Some(state.clone());
                state
            }
            (None, DriverState::Failed(_)) => {
                self.fail_operators();
                self.operator_terminal_signal = Some(state.clone());
                state
            }
            _ => state,
        };
        self.finish_with_state_after_operator_signal(state)
    }

    fn finish_with_state_after_operator_signal(&mut self, state: DriverState) -> DriverState {
        self.finish_blocked_interval();
        if matches!(
            state,
            DriverState::Finished | DriverState::Canceled | DriverState::Failed(_)
        ) && self.has_pending_finish()
        {
            self.pending_finish_state = Some(state.clone());
            self.state = DriverState::PendingFinish;
            return self.state.clone();
        }
        match &state {
            DriverState::Finished => {
                let last_op = self
                    .operators
                    .last()
                    .map(|op| op.name())
                    .unwrap_or("unknown");
                if let Some((hi, lo)) = self.fragment_instance_id {
                    debug!(
                        "Driver finished: finst={} driver_id={} last_op={}",
                        format_uuid(hi, lo),
                        self.driver_id,
                        last_op
                    );
                } else {
                    debug!(
                        "Driver finished: driver_id={} last_op={}",
                        self.driver_id, last_op
                    );
                }
            }
            DriverState::Canceled => {
                if let Some((hi, lo)) = self.fragment_instance_id {
                    debug!(
                        "Driver canceled: finst={} driver_id={}",
                        format_uuid(hi, lo),
                        self.driver_id
                    );
                } else {
                    debug!("Driver canceled: driver_id={}", self.driver_id);
                }
            }
            DriverState::Failed(err) => {
                if let Some((hi, lo)) = self.fragment_instance_id {
                    error!(
                        "Driver failed: finst={} driver_id={} error={}",
                        format_uuid(hi, lo),
                        self.driver_id,
                        err
                    );
                } else {
                    error!("Driver failed: driver_id={} error={}", self.driver_id, err);
                }
            }
            _ => {}
        }
        match state {
            DriverState::Finished | DriverState::Canceled | DriverState::Failed(_) => {
                self.close_operators();
            }
            _ => {}
        }
        self.state = state;
        self.state.clone()
    }

    fn close_operators(&mut self) {
        if self.closed {
            return;
        }
        self.closed = true;
        self.release_edge_buffers();
        for idx in (0..self.operators.len()).rev() {
            let op = &mut self.operators[idx];
            let start = Instant::now();
            if let Err(err) = op.close() {
                error!("operator close failed: {}: {}", op.name(), err);
            }
            let elapsed = start.elapsed();
            let elapsed_ns = i64::try_from(elapsed.as_nanos()).unwrap_or(i64::MAX);

            if self.profiler.is_some() {
                let counters = &self.operator_counters[idx];
                counters.close_time.add(elapsed_ns);
                counters.operator_total_time.add(elapsed_ns);
            }
        }
        if self.profiler.is_some() {
            for idx in 0..self.operator_counters.len() {
                self.update_operator_mem_counters(idx);
            }
        }
    }

    fn release_edge_buffers(&mut self) {
        for idx in 0..self.edge_chunks.len() {
            let _ = self.edge_chunks[idx].take();
        }
    }

    fn update_operator_mem_counters(&self, operator_idx: usize) {
        if self.profiler.is_none() {
            return;
        }
        let Some(tracker) = self
            .operator_mem_trackers
            .get(operator_idx)
            .and_then(|v| v.as_ref())
        else {
            return;
        };
        if operator_idx >= self.operator_counters.len() {
            return;
        }
        let counters = &self.operator_counters[operator_idx];
        counters.mem_peak.set(tracker.peak());
        counters.mem_allocated.set(tracker.allocated());
    }

    fn drive_dataflow(&mut self, made_progress: &mut bool) -> Result<(), String> {
        let edge_count = self.edge_chunks.len();
        if edge_count == 0 {
            return Ok(());
        }

        self.drive_push_edges(made_progress)?;
        self.drive_pull_edges(made_progress)?;
        self.drive_push_edges(made_progress)?;
        Ok(())
    }

    fn drive_push_edges(&mut self, made_progress: &mut bool) -> Result<(), String> {
        let edge_count = self.edge_chunks.len();
        if edge_count == 0 {
            return Ok(());
        }

        for e in (0..edge_count).rev() {
            if self.edge_chunks[e].is_none() {
                continue;
            }
            let downstream_idx = e + 1;
            let downstream_name = {
                let Some(downstream_op) = self.operators.get(downstream_idx) else {
                    return Err("pipeline operator index out of bounds".to_string());
                };
                let downstream_name = downstream_op.name().to_string();
                let downstream = downstream_op.as_processor_ref().ok_or_else(|| {
                    format!(
                        "pipeline operator {} missing processor operator",
                        downstream_name
                    )
                })?;
                let chunk = self.edge_chunks[e].as_ref().expect("checked is_some");
                if !downstream.can_accept_input(chunk)? {
                    continue;
                }
                downstream_name
            };
            let chunk = self.edge_chunks[e].take().expect("checked is_some");
            let (mut chunk, dict_stats) = {
                let downstream_ref = self
                    .operators
                    .get(downstream_idx)
                    .and_then(|op| op.as_processor_ref())
                    .ok_or_else(|| {
                        format!(
                            "pipeline operator {} missing processor operator",
                            downstream_name
                        )
                    })?;
                let stats = if self.profiler.is_some() {
                    Some(dictionary_carrier_stats(&chunk, downstream_ref))
                } else {
                    None
                };
                let chunk = hydrate_for_downstream(&chunk, downstream_ref)?;
                (chunk, stats)
            };
            if let Some(tracker) = self
                .operator_mem_trackers
                .get(downstream_idx)
                .and_then(|v| v.as_ref())
            {
                // Ownership model: memory is charged to the current holder (queue/operator).
                // We transfer accounting at queue boundaries via release+consume on the same bytes.
                chunk.transfer_to(tracker);
                self.update_operator_mem_counters(downstream_idx);
            }
            let push_rows = chunk.len();
            let before_net = if self.profiler.is_some() {
                self.operator_counters[downstream_idx]
                    .network_time
                    .as_ref()
                    .map(|c| c.value())
                    .unwrap_or(0)
            } else {
                0
            };
            let start = Instant::now();
            let result = {
                let Some(downstream_op) = self.operators.get_mut(downstream_idx) else {
                    return Err("pipeline operator index out of bounds".to_string());
                };
                let downstream = downstream_op.as_processor_mut().ok_or_else(|| {
                    format!(
                        "pipeline operator {} missing processor operator",
                        downstream_name
                    )
                })?;
                downstream.push_chunk(self.runtime_state.as_ref(), chunk)
            };
            let elapsed = start.elapsed();
            if self.profiler.is_some() {
                let elapsed_ns = i64::try_from(elapsed.as_nanos()).unwrap_or(i64::MAX);
                let after_net = self.operator_counters[downstream_idx]
                    .network_time
                    .as_ref()
                    .map(|c| c.value())
                    .unwrap_or(before_net);
                let net_delta = after_net.saturating_sub(before_net);
                let active_ns = elapsed_ns.saturating_sub(net_delta);
                let counters = &self.operator_counters[downstream_idx];
                counters.push_total_time.add(elapsed_ns);
                counters.operator_total_time.add(active_ns);
                if result.is_ok()
                    && let Some(stats) = dict_stats
                {
                    record_dictionary_carrier_stats(counters, stats);
                }
                let pushed_rows = match &result {
                    Ok(()) => push_rows as i64,
                    _ => 0,
                };
                if pushed_rows > 0 {
                    counters.push_row_num.add(pushed_rows);
                }
            }
            match result {
                Ok(()) => {
                    *made_progress = true;
                }
                Err(err) => {
                    return Err(format!(
                        "pipeline push into operator {} (edge {} -> {}) failed: {}",
                        downstream_name, e, downstream_idx, err
                    ));
                }
            }
        }

        Ok(())
    }

    fn drive_pull_edges(&mut self, made_progress: &mut bool) -> Result<(), String> {
        let edge_count = self.edge_chunks.len();
        if edge_count == 0 {
            return Ok(());
        }

        for e in 0..edge_count {
            if self.edge_chunks[e].is_some() {
                continue;
            }
            let upstream_idx = e;
            let downstream_idx = e + 1;

            let (left, right) = self.operators.split_at_mut(downstream_idx);
            let upstream_op = &mut left[upstream_idx];
            let downstream_op = &mut right[0];

            let upstream_name = upstream_op.name().to_string();
            let upstream = upstream_op.as_processor_mut().ok_or_else(|| {
                format!(
                    "pipeline operator {} missing processor operator",
                    upstream_name
                )
            })?;
            let downstream_name = downstream_op.name().to_string();
            let downstream = downstream_op.as_processor_mut().ok_or_else(|| {
                format!(
                    "pipeline operator {} missing processor operator",
                    downstream_name
                )
            })?;

            if !upstream.has_output() || !downstream.need_input() {
                continue;
            }

            let (before_io, before_wait, before_recv_total, before_net) = if self.profiler.is_some()
            {
                (
                    self.operator_counters[upstream_idx]
                        .io_task_exec_time
                        .as_ref()
                        .map(|c| c.value())
                        .unwrap_or(0),
                    self.operator_counters[upstream_idx]
                        .wait_time
                        .as_ref()
                        .map(|c| c.value())
                        .unwrap_or(0),
                    self.operator_counters[upstream_idx]
                        .receiver_process_total_time
                        .as_ref()
                        .map(|c| c.value())
                        .unwrap_or(0),
                    self.operator_counters[upstream_idx]
                        .network_time
                        .as_ref()
                        .map(|c| c.value())
                        .unwrap_or(0),
                )
            } else {
                (0, 0, 0, 0)
            };
            let start = Instant::now();
            let maybe = upstream.pull_chunk(self.runtime_state.as_ref());
            let elapsed = start.elapsed();
            if self.profiler.is_some() {
                let elapsed_ns = i64::try_from(elapsed.as_nanos()).unwrap_or(i64::MAX);
                let after_io = self.operator_counters[upstream_idx]
                    .io_task_exec_time
                    .as_ref()
                    .map(|c| c.value())
                    .unwrap_or(before_io);
                let io_delta = after_io.saturating_sub(before_io);
                let after_wait = self.operator_counters[upstream_idx]
                    .wait_time
                    .as_ref()
                    .map(|c| c.value())
                    .unwrap_or(before_wait);
                let wait_delta = after_wait.saturating_sub(before_wait);
                let after_recv_total = self.operator_counters[upstream_idx]
                    .receiver_process_total_time
                    .as_ref()
                    .map(|c| c.value())
                    .unwrap_or(before_recv_total);
                let recv_total_delta = after_recv_total.saturating_sub(before_recv_total);
                let after_net = self.operator_counters[upstream_idx]
                    .network_time
                    .as_ref()
                    .map(|c| c.value())
                    .unwrap_or(before_net);
                let net_delta = after_net.saturating_sub(before_net);
                let mut active_ns = elapsed_ns
                    .saturating_sub(io_delta)
                    .saturating_sub(net_delta);
                if self.operator_counters[upstream_idx]
                    .receiver_process_total_time
                    .is_some()
                {
                    // Align with StarRocks: exchange receive-side work is accounted by dedicated
                    // receiver timers, not by OperatorTotalTime.
                    active_ns = active_ns.saturating_sub(recv_total_delta);
                } else {
                    active_ns = active_ns.saturating_sub(wait_delta);
                }
                let counters = &self.operator_counters[upstream_idx];
                counters.pull_total_time.add(elapsed_ns);
                counters.operator_total_time.add(active_ns);
            }
            let maybe = match maybe {
                Ok(value) => value,
                Err(err) => {
                    return Err(format!(
                        "pipeline pull from operator {} (edge {} -> {}) failed: {}",
                        upstream_name, upstream_idx, e, err
                    ));
                }
            };
            if let Some(mut chunk) = maybe {
                if self.profiler.is_some() {
                    let counters = &self.operator_counters[upstream_idx];
                    counters.pull_row_num.add(chunk.len() as i64);
                }
                if let Some(tracker) = self.edge_mem_trackers.get(e).and_then(|v| v.as_ref()) {
                    // Ownership model: memory is charged to the current holder (queue/operator).
                    // We transfer accounting at queue boundaries via release+consume on the same bytes.
                    chunk.transfer_to(tracker);
                }
                self.edge_chunks[e] = Some(chunk);
                *made_progress = true;
            }
        }

        Ok(())
    }

    fn propagate_edge_closure(&mut self, made_progress: &mut bool) -> Result<(), String> {
        for e in 0..self.edge_chunks.len() {
            if self.edge_closed[e] {
                continue;
            }
            if self.edge_chunks[e].is_some() {
                continue;
            }
            let upstream_finished = self
                .operators
                .get(e)
                .map(|op| op.is_finished())
                .unwrap_or(false);
            if upstream_finished {
                self.edge_closed[e] = true;
                let op_name = self
                    .operators
                    .get(e)
                    .map(|op| op.name())
                    .unwrap_or("unknown");
                debug!(
                    "Driver edge closed: driver_id={} edge={} upstream_op={}",
                    self.driver_id, e, op_name
                );
                *made_progress = true;
            }
        }
        Ok(())
    }

    fn drive_set_finishing(&mut self, made_progress: &mut bool) -> Result<(), String> {
        if self.operators.len() < 2 {
            return Ok(());
        }
        // debug!("Driver drive_set_finishing start: driver_id={} op_count={}", self.driver_id, self.operators.len());
        for idx in 1..self.operators.len() {
            if self.operator_finishing_set[idx] {
                continue;
            }
            let in_edge = idx - 1;
            if in_edge >= self.edge_closed.len() {
                continue;
            }
            if !self.edge_closed[in_edge] || self.edge_chunks[in_edge].is_some() {
                continue;
            }
            let op = self
                .operators
                .get_mut(idx)
                .ok_or_else(|| "pipeline operator index out of bounds".to_string())?;
            let op_name = op.name().to_string();
            let proc = op.as_processor_mut().ok_or_else(|| {
                format!("pipeline operator {} missing processor operator", op_name)
            })?;
            let start = Instant::now();
            let result = proc.set_finishing(self.runtime_state.as_ref());
            let elapsed = start.elapsed();
            if self.profiler.is_some() {
                let elapsed_ns = i64::try_from(elapsed.as_nanos()).unwrap_or(i64::MAX);
                let counters = &self.operator_counters[idx];
                counters.set_finishing_time.add(elapsed_ns);
                counters.operator_total_time.add(elapsed_ns);
            }
            if let Err(err) = result {
                return Err(format!(
                    "pipeline set_finishing on operator {} (idx {}) failed: {}",
                    op_name, idx, err
                ));
            }
            debug!(
                "Driver set_finishing: driver_id={} op_idx={} op_name={} success. edge_closed[{}]={}",
                self.driver_id, idx, op_name, in_edge, self.edge_closed[in_edge]
            );
            // Latch only once the operator says finishing is done. An operator
            // that is still waiting stays unlatched so a later turn retries it;
            // without this, `set_finishing` runs exactly once and an operator
            // that could not finish yet would never get another chance.
            let wait = proc.finishing_wait();
            self.operator_finishing_set[idx] = !wait.is_pending();
            // Owed output is work only this driver can perform. Treat it as
            // immediate progress so the loop calls set_finishing again;
            // parking on the sink observable here can miss an external event
            // that changed ExternalEvent -> OwedOutput during the call above.
            if !wait.is_pending() || wait.can_progress() {
                *made_progress = true;
            }
        }
        Ok(())
    }
}

impl Drop for PipelineDriver {
    fn drop(&mut self) {
        self.finish_blocked_interval();
        self.close_operators();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use super::{BlockedReason, DriverState, Observable, PipelineDriver};
    use crate::exec::chunk::Chunk;
    use crate::exec::pipeline::operator::{FinishingWait, Operator, ProcessorOperator};
    use crate::runtime::runtime_state::RuntimeState;

    /// A source that is finished before the driver's first turn, so the edge
    /// into the sink closes immediately and the driver goes straight to
    /// finishing it.
    struct FinishedSource;

    impl Operator for FinishedSource {
        fn name(&self) -> &str {
            "FinishedSource"
        }

        fn is_finished(&self) -> bool {
            true
        }

        fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
            Some(self)
        }

        fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
            Some(self)
        }
    }

    impl ProcessorOperator for FinishedSource {
        fn need_input(&self) -> bool {
            false
        }

        fn has_output(&self) -> bool {
            false
        }

        fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> Result<(), String> {
            Err("the finished source accepts no input".to_string())
        }

        fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
            Ok(None)
        }

        fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
            Ok(())
        }
    }

    /// A sink whose finishing wait is scripted, standing in for an exchange
    /// sink holding a payload behind a gated outbound edge.
    ///
    /// It mirrors the real sink where it matters: it refuses input while it
    /// owes output, so the driver parks on `OutputFull` rather than spinning,
    /// and it is not finished until its wait is `Complete`.
    struct ScriptedSink {
        wait: Arc<Mutex<FinishingWait>>,
        set_finishing_calls: Arc<AtomicUsize>,
        observable: Arc<Observable>,
        open_during_first_finish: bool,
        open_after_first_wait_check: bool,
        wait_checks: AtomicUsize,
    }

    impl ScriptedSink {
        fn new(wait: Arc<Mutex<FinishingWait>>) -> Self {
            Self {
                wait,
                set_finishing_calls: Arc::new(AtomicUsize::new(0)),
                observable: Arc::new(Observable::new()),
                open_during_first_finish: false,
                open_after_first_wait_check: false,
                wait_checks: AtomicUsize::new(0),
            }
        }

        fn opening_during_first_finish(wait: Arc<Mutex<FinishingWait>>) -> Self {
            Self {
                wait,
                set_finishing_calls: Arc::new(AtomicUsize::new(0)),
                observable: Arc::new(Observable::new()),
                open_during_first_finish: true,
                open_after_first_wait_check: false,
                wait_checks: AtomicUsize::new(0),
            }
        }

        fn opening_after_first_wait_check(wait: Arc<Mutex<FinishingWait>>) -> Self {
            Self {
                wait,
                set_finishing_calls: Arc::new(AtomicUsize::new(0)),
                observable: Arc::new(Observable::new()),
                open_during_first_finish: false,
                open_after_first_wait_check: true,
                wait_checks: AtomicUsize::new(0),
            }
        }

        fn wait(&self) -> FinishingWait {
            *self.wait.lock().expect("scripted wait lock")
        }
    }

    impl Operator for ScriptedSink {
        fn name(&self) -> &str {
            "ScriptedSink"
        }

        fn is_finished(&self) -> bool {
            !self.wait().is_pending()
        }

        fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
            Some(self)
        }

        fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
            Some(self)
        }
    }

    impl ProcessorOperator for ScriptedSink {
        fn need_input(&self) -> bool {
            !self.wait().is_pending()
        }

        fn has_output(&self) -> bool {
            false
        }

        fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> Result<(), String> {
            Ok(())
        }

        fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
            Ok(None)
        }

        fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
            let prior_calls = self.set_finishing_calls.fetch_add(1, Ordering::SeqCst);
            // The turn is what pushes owed output, exactly as the exchange
            // sink flushes its parked payload and sends its end-of-stream
            // inside this call. A wait for an external event is unchanged by
            // it.
            let mut wait = self.wait.lock().expect("scripted wait lock");
            if self.open_during_first_finish
                && prior_calls == 0
                && *wait == FinishingWait::ExternalEvent
            {
                // Reproduce the gate-open race: the notification happens
                // before the driver freezes its blocked generation, while the
                // sink returns owing output that only another call can push.
                *wait = FinishingWait::OwedOutput;
                self.observable.notify_observers();
                return Ok(());
            }
            if *wait == FinishingWait::OwedOutput {
                *wait = FinishingWait::Complete;
            }
            Ok(())
        }

        fn finishing_wait(&self) -> FinishingWait {
            let observed = self.wait();
            if self.open_after_first_wait_check
                && self.wait_checks.fetch_add(1, Ordering::SeqCst) == 0
                && observed == FinishingWait::ExternalEvent
            {
                *self.wait.lock().expect("scripted wait lock") = FinishingWait::OwedOutput;
                self.observable.notify_observers();
            }
            observed
        }

        fn sink_observable(&self) -> Option<Arc<Observable>> {
            Some(Arc::clone(&self.observable))
        }
    }

    /// The defect this catches: a finishing sink can move from waiting on an
    /// external event to owing output that only another driver turn can push.
    /// The event-driven driver must park on the sink's stable observable and
    /// resume from its generation change; it must neither poll the sink nor
    /// latch finishing before the owed output is sent.
    ///
    /// The consequence is a query that hangs with no failure anywhere. The one
    /// producer holding rows parks with the payload in hand, never sends its
    /// end of stream, the receiving exchange counts senders until the
    /// statement deadline, and the frontend's read completion -- which waits
    /// on the root task's FINISHED and its own end of stream, neither of which
    /// fails on absence -- waits with it. Measured on a real 1FE+3BE cluster
    /// as a distributed `SELECT ... ORDER BY` timing out after 120 s: two of
    /// three producers, both with no rows to send, sealed their stream; the
    /// third, with the two matching rows, parked and stayed parked.
    #[test]
    fn a_parked_driver_is_re_readied_once_its_sink_can_push_the_output_it_owes() {
        let runtime_state = Arc::new(RuntimeState::default());
        // The edge is closed: the sink is waiting for a decision only the
        // frontend can make.
        let wait = Arc::new(Mutex::new(FinishingWait::ExternalEvent));
        let sink = ScriptedSink::new(Arc::clone(&wait));
        let calls = Arc::clone(&sink.set_finishing_calls);
        let sink_observable = Arc::clone(&sink.observable);
        let mut driver = PipelineDriver::new(
            1,
            vec![Box::new(FinishedSource), Box::new(sink)],
            None,
            Vec::new(),
            runtime_state,
            None,
        );

        let state = driver.process(Duration::from_millis(10));
        assert!(
            matches!(state, DriverState::Blocked(BlockedReason::OutputFull)),
            "actual: {state:?}"
        );
        let turns_before_parking = calls.load(Ordering::SeqCst);
        assert!(
            turns_before_parking >= 1,
            "the driver gives finishing at least one turn before it parks"
        );
        let (blocked, generation, deadline) = driver
            .blocked_observable_snapshot()
            .expect("finishing wait must freeze the sink readiness event");
        assert!(Arc::ptr_eq(&blocked, &sink_observable));
        assert_eq!(generation, sink_observable.generation());
        assert!(deadline.is_none());

        // The edge opens. The sink publishes that the payload it parked is now
        // output only its own next turn can push, and the scheduler requeues
        // the driver from the generation change.
        *wait.lock().expect("scripted wait lock") = FinishingWait::OwedOutput;
        sink_observable.notify_observers();
        assert_ne!(generation, sink_observable.generation());
        driver.set_ready();

        // And the turn it is re-readied for is the one that pushes it.
        let state = driver.process(Duration::from_millis(10));
        assert!(matches!(state, DriverState::Finished), "actual: {state:?}");
        assert!(
            calls.load(Ordering::SeqCst) > turns_before_parking,
            "the re-readied turn is the one that finishes the sink"
        );
    }

    #[test]
    fn a_gate_open_during_finishing_cannot_park_output_owed_by_the_driver() {
        let wait = Arc::new(Mutex::new(FinishingWait::ExternalEvent));
        let sink = ScriptedSink::opening_during_first_finish(Arc::clone(&wait));
        let calls = Arc::clone(&sink.set_finishing_calls);
        let mut driver = PipelineDriver::new(
            2,
            vec![Box::new(FinishedSource), Box::new(sink)],
            None,
            Vec::new(),
            Arc::new(RuntimeState::default()),
            None,
        );

        let state = driver.process(Duration::from_millis(10));

        assert!(matches!(state, DriverState::Finished), "actual: {state:?}");
        assert_eq!(
            *wait.lock().expect("scripted wait lock"),
            FinishingWait::Complete
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn a_gate_open_after_finishing_check_cannot_park_output_owed_by_the_driver() {
        let wait = Arc::new(Mutex::new(FinishingWait::ExternalEvent));
        let sink = ScriptedSink::opening_after_first_wait_check(Arc::clone(&wait));
        let calls = Arc::clone(&sink.set_finishing_calls);
        let mut driver = PipelineDriver::new(
            3,
            vec![Box::new(FinishedSource), Box::new(sink)],
            None,
            Vec::new(),
            Arc::new(RuntimeState::default()),
            None,
        );

        let state = driver.process(Duration::from_millis(10));
        assert!(matches!(state, DriverState::Finished), "actual: {state:?}");
        assert_eq!(
            *wait.lock().expect("scripted wait lock"),
            FinishingWait::Complete
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}

#[cfg(test)]
mod terminal_signal_tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use super::*;

    struct ControlledReadinessOperator {
        name: &'static str,
        need_input: bool,
        has_output: bool,
        observable: Arc<Observable>,
    }

    struct DriverPolledSource {
        observable: Arc<Observable>,
    }

    struct BufferedOnceOperator {
        output: Option<Chunk>,
        observable: Arc<Observable>,
    }

    struct RecoveringTerminal {
        ready: Arc<AtomicBool>,
        pushed: Arc<AtomicUsize>,
        observable: Arc<Observable>,
    }

    struct GenerationChangingSource {
        changed: AtomicBool,
        observable: Arc<Observable>,
    }

    struct SynchronousJoinCompletion {
        phase: Arc<AtomicUsize>,
        pulls: Arc<AtomicUsize>,
    }

    struct DependencyBlockedJoin {
        dependency: DependencyHandle,
    }

    impl Operator for DriverPolledSource {
        fn name(&self) -> &str {
            "DRIVER_POLLED_SOURCE"
        }

        fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
            Some(self)
        }

        fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
            Some(self)
        }
    }

    impl ProcessorOperator for DriverPolledSource {
        fn need_input(&self) -> bool {
            false
        }

        fn has_output(&self) -> bool {
            panic!("active source polling must remain on a driver worker")
        }

        fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> Result<(), String> {
            Ok(())
        }

        fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
            Ok(None)
        }

        fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
            Ok(())
        }

        fn source_observable(&self) -> Option<Arc<Observable>> {
            Some(Arc::clone(&self.observable))
        }
    }

    impl Operator for BufferedOnceOperator {
        fn name(&self) -> &str {
            "BUFFERED_ONCE"
        }

        fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
            Some(self)
        }

        fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
            Some(self)
        }
    }

    impl ProcessorOperator for BufferedOnceOperator {
        fn need_input(&self) -> bool {
            self.output.is_none()
        }

        fn has_output(&self) -> bool {
            self.output.is_some()
        }

        fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
            self.output = Some(chunk);
            Ok(())
        }

        fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
            Ok(self.output.take())
        }

        fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
            Ok(())
        }

        fn sink_observable(&self) -> Option<Arc<Observable>> {
            Some(Arc::clone(&self.observable))
        }
    }

    impl Operator for RecoveringTerminal {
        fn name(&self) -> &str {
            "RECOVERING_TERMINAL"
        }

        fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
            Some(self)
        }

        fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
            Some(self)
        }
    }

    impl ProcessorOperator for RecoveringTerminal {
        fn need_input(&self) -> bool {
            self.ready.load(Ordering::Acquire)
        }

        fn has_output(&self) -> bool {
            false
        }

        fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> Result<(), String> {
            if !self.need_input() {
                return Err("recovering terminal received input while blocked".to_string());
            }
            self.pushed.fetch_add(1, Ordering::AcqRel);
            Ok(())
        }

        fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
            Ok(None)
        }

        fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
            Ok(())
        }

        fn sink_observable(&self) -> Option<Arc<Observable>> {
            Some(Arc::clone(&self.observable))
        }
    }

    impl Operator for GenerationChangingSource {
        fn name(&self) -> &str {
            "GENERATION_CHANGING_SOURCE"
        }

        fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
            Some(self)
        }

        fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
            Some(self)
        }
    }

    impl ProcessorOperator for GenerationChangingSource {
        fn need_input(&self) -> bool {
            false
        }

        fn has_output(&self) -> bool {
            if !self.changed.swap(true, Ordering::AcqRel) {
                self.observable.notify_observers();
            }
            false
        }

        fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> Result<(), String> {
            Ok(())
        }

        fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
            Ok(None)
        }

        fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
            Ok(())
        }

        fn source_observable(&self) -> Option<Arc<Observable>> {
            Some(Arc::clone(&self.observable))
        }
    }

    impl Operator for SynchronousJoinCompletion {
        fn name(&self) -> &str {
            "SYNCHRONOUS_JOIN_COMPLETION"
        }

        fn is_finished(&self) -> bool {
            self.phase.load(Ordering::Acquire) == 2
        }

        fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
            Some(self)
        }

        fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
            Some(self)
        }
    }

    impl ProcessorOperator for SynchronousJoinCompletion {
        fn need_input(&self) -> bool {
            false
        }

        fn has_output(&self) -> bool {
            self.phase.load(Ordering::Acquire) == 1
        }

        fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> Result<(), String> {
            Err("synchronous join completion does not accept input".to_string())
        }

        fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
            if self
                .phase
                .compare_exchange(1, 2, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.pulls.fetch_add(1, Ordering::AcqRel);
                return Ok(Some(Chunk::default()));
            }
            Ok(None)
        }

        fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
            Ok(())
        }
    }

    impl Operator for DependencyBlockedJoin {
        fn name(&self) -> &str {
            "DEPENDENCY_BLOCKED_JOIN"
        }

        fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
            Some(self)
        }

        fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
            Some(self)
        }
    }

    impl ProcessorOperator for DependencyBlockedJoin {
        fn need_input(&self) -> bool {
            false
        }

        fn has_output(&self) -> bool {
            false
        }

        fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> Result<(), String> {
            Err("dependency-blocked join cannot accept input".to_string())
        }

        fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
            Ok(None)
        }

        fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
            Ok(())
        }

        fn precondition_dependency(&self) -> Option<DependencyHandle> {
            Some(Arc::clone(&self.dependency))
        }
    }

    impl Operator for ControlledReadinessOperator {
        fn name(&self) -> &str {
            self.name
        }

        fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
            Some(self)
        }

        fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
            Some(self)
        }
    }

    impl ProcessorOperator for ControlledReadinessOperator {
        fn need_input(&self) -> bool {
            self.need_input
        }

        fn has_output(&self) -> bool {
            self.has_output
        }

        fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> Result<(), String> {
            Ok(())
        }

        fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
            Ok(None)
        }

        fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
            Ok(())
        }

        fn sink_observable(&self) -> Option<Arc<Observable>> {
            Some(Arc::clone(&self.observable))
        }

        fn source_observable(&self) -> Option<Arc<Observable>> {
            Some(Arc::clone(&self.observable))
        }
    }

    struct SignalCountingOperator {
        pending: Arc<AtomicBool>,
        cancel_count: Arc<AtomicUsize>,
        failure_count: Arc<AtomicUsize>,
    }

    impl Operator for SignalCountingOperator {
        fn name(&self) -> &str {
            "SIGNAL_COUNTING"
        }

        fn cancel(&mut self) {
            self.cancel_count.fetch_add(1, Ordering::SeqCst);
        }

        fn on_driver_failure(&mut self) {
            self.failure_count.fetch_add(1, Ordering::SeqCst);
        }

        fn pending_finish(&self) -> bool {
            self.pending.load(Ordering::SeqCst)
        }
    }

    fn signal_counting_driver(
        pending: Arc<AtomicBool>,
        cancel_count: Arc<AtomicUsize>,
        failure_count: Arc<AtomicUsize>,
    ) -> PipelineDriver {
        PipelineDriver::new(
            1,
            vec![Box::new(SignalCountingOperator {
                pending,
                cancel_count,
                failure_count,
            })],
            None,
            Vec::new(),
            Arc::new(RuntimeState::default()),
            None,
        )
    }

    #[test]
    fn downstream_output_is_runnable_before_an_upstream_capacity_blocker() {
        let upstream_observable = Arc::new(Observable::new());
        let downstream_observable = Arc::new(Observable::new());
        let terminal_observable = Arc::new(Observable::new());
        let driver = PipelineDriver::new(
            1,
            vec![
                Box::new(ControlledReadinessOperator {
                    name: "UPSTREAM_BLOCKED",
                    need_input: false,
                    has_output: false,
                    observable: upstream_observable,
                }),
                Box::new(ControlledReadinessOperator {
                    name: "DOWNSTREAM_OUTPUT",
                    need_input: false,
                    has_output: true,
                    observable: downstream_observable,
                }),
                Box::new(ControlledReadinessOperator {
                    name: "TERMINAL_READY",
                    need_input: true,
                    has_output: false,
                    observable: terminal_observable,
                }),
            ],
            None,
            Vec::new(),
            Arc::new(RuntimeState::default()),
            None,
        );

        assert!(matches!(
            driver
                .internal_sink_block_decision_on_worker()
                .expect("worker readiness decision"),
            WorkerBlockDecision::Runnable
        ));
    }

    #[test]
    fn retained_internal_output_resumes_from_the_downstream_generation() {
        let source_observable = Arc::new(Observable::new());
        let internal_observable = Arc::new(Observable::new());
        let terminal_observable = Arc::new(Observable::new());
        let terminal_ready = Arc::new(AtomicBool::new(false));
        let terminal_pushed = Arc::new(AtomicUsize::new(0));
        let mut driver = PipelineDriver::new(
            3,
            vec![
                Box::new(ControlledReadinessOperator {
                    name: "SOURCE_EMPTY",
                    need_input: false,
                    has_output: false,
                    observable: source_observable,
                }),
                Box::new(BufferedOnceOperator {
                    output: Some(Chunk::default()),
                    observable: internal_observable,
                }),
                Box::new(RecoveringTerminal {
                    ready: Arc::clone(&terminal_ready),
                    pushed: Arc::clone(&terminal_pushed),
                    observable: Arc::clone(&terminal_observable),
                }),
            ],
            None,
            Vec::new(),
            Arc::new(RuntimeState::default()),
            None,
        );

        assert_eq!(
            driver.process(Duration::from_secs(1)),
            DriverState::Blocked(BlockedReason::OutputFull)
        );
        let (blocked, generation, deadline) = driver
            .blocked_observable_snapshot()
            .expect("driver freezes its downstream wait target");
        assert!(Arc::ptr_eq(&blocked, &terminal_observable));
        assert_eq!(generation, terminal_observable.generation());
        assert!(deadline.is_none());

        terminal_ready.store(true, Ordering::Release);
        terminal_observable.notify_observers();
        assert_ne!(generation, terminal_observable.generation());
        driver.set_ready();
        let _ = driver.process(Duration::from_secs(1));
        assert_eq!(terminal_pushed.load(Ordering::Acquire), 1);
    }

    #[test]
    fn short_pipeline_has_no_internal_operator_range() {
        let observable = Arc::new(Observable::new());
        let driver = PipelineDriver::new(
            4,
            vec![Box::new(ControlledReadinessOperator {
                name: "ONLY_OPERATOR",
                need_input: true,
                has_output: false,
                observable,
            })],
            None,
            Vec::new(),
            Arc::new(RuntimeState::default()),
            None,
        );

        assert!(!driver.has_internal_output_on_worker());
        assert!(matches!(
            driver
                .internal_sink_block_decision_on_worker()
                .expect("short pipeline decision"),
            WorkerBlockDecision::NoBlocker
        ));
    }

    #[test]
    fn worker_retries_when_readiness_generation_changes_during_the_check() {
        let source_observable = Arc::new(Observable::new());
        let terminal_observable = Arc::new(Observable::new());
        let driver = PipelineDriver::new(
            5,
            vec![
                Box::new(GenerationChangingSource {
                    changed: AtomicBool::new(false),
                    observable: source_observable,
                }),
                Box::new(ControlledReadinessOperator {
                    name: "TERMINAL_READY",
                    need_input: true,
                    has_output: false,
                    observable: terminal_observable,
                }),
            ],
            None,
            Vec::new(),
            Arc::new(RuntimeState::default()),
            None,
        );

        assert!(matches!(
            driver
                .source_block_decision_on_worker()
                .expect("worker source decision"),
            WorkerBlockDecision::Retry
        ));
    }

    #[test]
    fn source_ready_internal_full_parks_on_internal_generation() {
        let source_observable = Arc::new(Observable::new());
        let internal_observable = Arc::new(Observable::new());
        let terminal_observable = Arc::new(Observable::new());
        let mut driver = PipelineDriver::new(
            6,
            vec![
                Box::new(ControlledReadinessOperator {
                    name: "SOURCE_READY",
                    need_input: false,
                    has_output: true,
                    observable: source_observable,
                }),
                Box::new(ControlledReadinessOperator {
                    name: "INTERNAL_FULL",
                    need_input: false,
                    has_output: false,
                    observable: Arc::clone(&internal_observable),
                }),
                Box::new(ControlledReadinessOperator {
                    name: "TERMINAL_READY",
                    need_input: true,
                    has_output: false,
                    observable: terminal_observable,
                }),
            ],
            None,
            Vec::new(),
            Arc::new(RuntimeState::default()),
            None,
        );

        assert_eq!(
            driver.process(Duration::from_secs(1)),
            DriverState::Blocked(BlockedReason::OutputFull)
        );
        let (blocked, generation, deadline) = driver
            .blocked_observable_snapshot()
            .expect("driver freezes the internal capacity wait target");
        assert!(Arc::ptr_eq(&blocked, &internal_observable));
        assert_eq!(generation, internal_observable.generation());
        assert!(deadline.is_none());
    }

    #[test]
    fn synchronous_join_completion_without_observable_stays_runnable_and_drains_output() {
        let source_observable = Arc::new(Observable::new());
        let terminal_observable = Arc::new(Observable::new());
        let join_phase = Arc::new(AtomicUsize::new(0));
        let join_pulls = Arc::new(AtomicUsize::new(0));
        let mut driver = PipelineDriver::new(
            7,
            vec![
                Box::new(ControlledReadinessOperator {
                    name: "SOURCE_READY",
                    need_input: false,
                    has_output: true,
                    observable: source_observable,
                }),
                Box::new(SynchronousJoinCompletion {
                    phase: Arc::clone(&join_phase),
                    pulls: Arc::clone(&join_pulls),
                }),
                Box::new(ControlledReadinessOperator {
                    name: "TERMINAL_READY",
                    need_input: true,
                    has_output: false,
                    observable: terminal_observable,
                }),
            ],
            None,
            Vec::new(),
            Arc::new(RuntimeState::default()),
            None,
        );

        assert_eq!(driver.process(Duration::from_secs(1)), DriverState::Ready);
        assert!(driver.blocked_observable_snapshot().is_none());

        // Join completion and unmatched-row emission are synchronous worker
        // work. They do not acquire an event observable merely because the
        // processor temporarily accepted no input before output became ready.
        join_phase.store(1, Ordering::Release);
        assert_eq!(driver.process(Duration::from_secs(1)), DriverState::Ready);
        assert_eq!(join_pulls.load(Ordering::Acquire), 1);
    }

    #[test]
    fn source_without_dependency_does_not_hide_internal_join_dependency() {
        let dependency =
            crate::exec::pipeline::dependency::DependencyManager::new().get_or_create("join");
        let source_observable = Arc::new(Observable::new());
        let terminal_observable = Arc::new(Observable::new());
        let mut driver = PipelineDriver::new(
            8,
            vec![
                Box::new(ControlledReadinessOperator {
                    name: "SOURCE_WITHOUT_DEPENDENCY",
                    need_input: false,
                    has_output: true,
                    observable: source_observable,
                }),
                Box::new(DependencyBlockedJoin {
                    dependency: Arc::clone(&dependency),
                }),
                Box::new(ControlledReadinessOperator {
                    name: "TERMINAL_READY",
                    need_input: true,
                    has_output: false,
                    observable: terminal_observable,
                }),
            ],
            None,
            Vec::new(),
            Arc::new(RuntimeState::default()),
            None,
        );

        assert_eq!(
            driver.process(Duration::from_secs(1)),
            DriverState::Blocked(BlockedReason::Dependency(Arc::clone(&dependency)))
        );
        assert!(driver.blocked_observable_snapshot().is_none());
        assert!(!dependency.is_ready());

        dependency.set_ready();
        assert!(dependency.is_ready());
    }

    #[test]
    fn scheduler_readiness_discovery_never_polls_an_active_source() {
        let source_observable = Arc::new(Observable::new());
        let internal_observable = Arc::new(Observable::new());
        let terminal_observable = Arc::new(Observable::new());
        let mut driver = PipelineDriver::new(
            2,
            vec![
                Box::new(DriverPolledSource {
                    observable: Arc::clone(&source_observable),
                }),
                Box::new(ControlledReadinessOperator {
                    name: "INTERNAL_READY",
                    need_input: true,
                    has_output: false,
                    observable: internal_observable,
                }),
                Box::new(ControlledReadinessOperator {
                    name: "TERMINAL_READY",
                    need_input: true,
                    has_output: false,
                    observable: terminal_observable,
                }),
            ],
            None,
            Vec::new(),
            Arc::new(RuntimeState::default()),
            None,
        );

        let generation = source_observable.generation();
        driver.blocked_observable = Some((Arc::clone(&source_observable), generation, None));
        driver.state = DriverState::Blocked(BlockedReason::InputEmpty);
        assert_eq!(generation, source_observable.generation());
        source_observable.notify_observers();
        assert_ne!(generation, source_observable.generation());
    }

    #[test]
    fn repeated_abort_signals_operators_once_while_finish_is_pending() {
        let pending = Arc::new(AtomicBool::new(true));
        let cancel_count = Arc::new(AtomicUsize::new(0));
        let failure_count = Arc::new(AtomicUsize::new(0));
        let mut driver = signal_counting_driver(
            Arc::clone(&pending),
            Arc::clone(&cancel_count),
            Arc::clone(&failure_count),
        );

        assert_eq!(
            driver.cancel_for_fragment_abort(),
            DriverState::PendingFinish
        );
        assert_eq!(
            driver.cancel_for_fragment_abort(),
            DriverState::PendingFinish
        );
        assert_eq!(cancel_count.load(Ordering::SeqCst), 1);
        assert_eq!(failure_count.load(Ordering::SeqCst), 0);

        pending.store(false, Ordering::SeqCst);
        assert_eq!(driver.process(Duration::ZERO), DriverState::Canceled);
        assert_eq!(cancel_count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn pending_failure_is_not_replaced_or_resignaled_by_abort() {
        let pending = Arc::new(AtomicBool::new(true));
        let cancel_count = Arc::new(AtomicUsize::new(0));
        let failure_count = Arc::new(AtomicUsize::new(0));
        let mut driver = signal_counting_driver(
            Arc::clone(&pending),
            Arc::clone(&cancel_count),
            Arc::clone(&failure_count),
        );

        assert_eq!(
            driver.finish_with_state(DriverState::Failed("first failure".to_string())),
            DriverState::PendingFinish
        );
        assert_eq!(
            driver.cancel_for_fragment_abort(),
            DriverState::PendingFinish
        );
        assert_eq!(failure_count.load(Ordering::SeqCst), 1);
        assert_eq!(cancel_count.load(Ordering::SeqCst), 0);

        pending.store(false, Ordering::SeqCst);
        assert_eq!(
            driver.cancel_for_fragment_abort(),
            DriverState::Failed("first failure".to_string())
        );
        assert_eq!(failure_count.load(Ordering::SeqCst), 1);
        assert_eq!(cancel_count.load(Ordering::SeqCst), 0);
    }
}
