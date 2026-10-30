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

#[path = "driver_progress.rs"]
mod progress;
use progress::ProgressEndpoint;

#[cfg(test)]
use crate::exec::chunk::Chunk;
#[cfg(test)]
use crate::exec::pipeline::dependency::DependencyHandle;
#[cfg(test)]
use crate::exec::pipeline::operator::OperatorLifecycle;
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult, PipelineOperation};

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use super::operator::{BlockedReason, DriverBlockDeadline, ExitProgress, FinishWatch, Operator};
use super::{
    driver_failure::DriverFailure,
    driver_retirement::{
        DriverExitCustody, EdgeRetirement, ProcessPanicCustody, RetainedDriverOwners,
    },
    input::RawInput,
    instance::OperatorInstance,
    shape::StepOperator,
    step::{AdmissionNext, InputResult, NextStep, RootPending, RootStep, UpstreamPull, WaitFact},
    step_context::{DEFAULT_TURN_WORK_UNITS, StepContext, TurnWork},
};
use crate::exec::pipeline::schedule::observer::Observable;
use crate::runtime::fragment::io::{
    FragmentEvent, FragmentEventSink, FragmentProgress, NoopFragmentEventSink, RootInputPermit,
};
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::profile::Profiler;
use crate::runtime::profile::{CounterRef, OperatorProfiles, ProfileUnit, clamp_u128_to_i64};
use crate::runtime::runtime_state::RuntimeState;
use novarocks_types::{UniqueId, format_uuid};
use std::num::NonZeroU64;
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
    Failed(ExecutionFailure),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DriverBlockedKind {
    InputEmpty,
    OutputFull,
    Dependency,
}

/// Driver-owned subscriptions leave shared control owners when this cover is
/// replaced or the driver exits. Callbacks contain only weak wake targets.
struct ControlWatch {
    sources: Vec<Arc<Observable>>,
    observable: Arc<Observable>,
    _subscriptions: Vec<crate::runtime::observable::ObserverSubscription>,
}
impl ControlWatch {
    fn covers(&self, source: &Arc<Observable>) -> bool {
        self.sources.iter().any(|old| Arc::ptr_eq(old, source))
    }
}
fn subscribe_control(
    source: &Arc<Observable>,
    target: &Arc<Observable>,
) -> crate::runtime::observable::ObserverSubscription {
    let target = Arc::downgrade(target);
    source.subscribe(Arc::new(move || {
        if let Some(target) = target.upgrade() {
            target.notify_observers();
        }
    }))
}

enum WorkerBlockDecision {
    Runnable,
    NoBlocker,
    Retry,
    Blocked(Arc<Observable>, u64, Option<DriverBlockDeadline>),
}

#[derive(Debug)]
/// Scheduling metadata for one driver including blocking and requeue state.
pub(crate) struct DriverScheduleState {
    in_blocked: AtomicBool,
    source_observables: Mutex<Vec<Weak<Observable>>>,
    sink_observables: Mutex<Vec<Weak<Observable>>>,
    finish_observables: Mutex<Vec<Weak<Observable>>>,
}

impl DriverScheduleState {
    pub(crate) fn new() -> Self {
        Self {
            in_blocked: AtomicBool::new(false),
            source_observables: Mutex::new(Vec::new()),
            sink_observables: Mutex::new(Vec::new()),
            finish_observables: Mutex::new(Vec::new()),
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

    pub(crate) fn try_mark_finish_observer_registered(&self, observable: &Arc<Observable>) -> bool {
        Self::try_mark_observer(&self.finish_observables, observable)
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

/// What a driver in `PendingFinish` waits for.
///
/// `generation` was sampled on the driver's finish observable before its
/// operators were asked about pending work, so a completion after that
/// question always shows up as a newer generation.
#[derive(Clone)]
pub(crate) struct PendingFinishWait {
    pub(crate) observable: Arc<Observable>,
    pub(crate) generation: u64,
    /// Earliest recheck asked for by an owner without a completion event.
    pub(crate) recheck_at: Option<Instant>,
}

/// Cooperative execution driver that runs source/processor/sink operators for one pipeline instance.
pub struct PipelineDriver {
    driver_id: i32,
    operators: Vec<OperatorInstance>,
    turn_work: TurnWork,
    // Design: ADR-0195 (docs/adr/ADR-0195-public-value-access-and-construction.md)
    value_conversions: novarocks_value_arrays::decode::ValueConversionContext,
    profiler: Option<Profiler>,
    driver_total_time: Option<CounterRef>,
    driver_blocked_time: Option<CounterRef>,
    driver_input_empty_time: Option<CounterRef>,
    driver_output_full_time: Option<CounterRef>,
    driver_dependency_wait_time: Option<CounterRef>,
    operator_counters: Vec<OperatorCounters>,
    runtime_state: Arc<RuntimeState>,
    query_memory: Option<crate::runtime::query_memory::QueryMemoryBinding>,
    fragment_instance_id: Option<(i64, i64)>,
    event_sink: Option<std::mem::ManuallyDrop<Arc<dyn FragmentEventSink>>>,
    progress: ProgressEndpoint,
    state: DriverState,
    blocked_since: Option<(Instant, DriverBlockedKind)>,
    closed: bool,
    exit_started: bool,
    cleanup_diagnostics: Vec<novarocks_execution_contract::SafeDetail>,
    cleanup_diagnostics_dropped: u64,
    activated: bool,
    schedule_state: Arc<DriverScheduleState>,
    blocked_observable: Option<(Arc<Observable>, u64, Option<DriverBlockDeadline>)>,
    control_wait_observables: Option<ControlWatch>,
    frontier_watch: Option<ControlWatch>,
    frontier_observable: Arc<Observable>,
    frontier_waits: Vec<WaitFact>,
    /// Terminal operator's early-finish observable and its generation, sampled
    /// before this turn last checked whether the pipeline was finished.
    blocked_terminal: Option<(Arc<Observable>, u64)>,
    /// Whether the downstream refuses the source's output: it had output but
    /// the first processor needed no input, or a pulled chunk stays on the
    /// first edge. Reported to the source when it changes.
    source_backpressure: bool,
    /// The last pull attempt found source output the downstream did not need.
    source_output_refused: bool,
    pending_finish_state: Option<DriverState>,
    /// Notified by every operator finish watch the driver has waited on.
    finish_observable: Arc<Observable>,
    /// Finish observables already forwarded to `finish_observable`.
    finish_watched: Vec<(
        Weak<Observable>,
        crate::runtime::observable::ObserverSubscription,
    )>,
    operator_terminal_signal: Option<DriverState>,

    edge_chunks: Vec<Option<RawInput>>,
    edge_retirement: Vec<EdgeRetirement>,
    exit_custody: Arc<DriverExitCustody>,
    process_panic: ProcessPanicCustody,
    // Before publication only: the producer borrows this grant across yields.
    // Once a Chunk exists, its RawInput owns the grant through edge custody.
    edge_root_input: Option<(usize, RootInputPermit)>,
    edge_closed: Vec<bool>,
    operator_finishing_set: Vec<bool>,
    /// Data-retired prefix; operator positions and physical exit owners remain
    /// stable until the complete driver retires.
    retired_prefix_len: usize,
    edge_mem_trackers: Vec<Option<Arc<MemTracker>>>,
}

/// Fragment-owned services bound before operator preparation.
///
/// Keeping these bindings together prevents driver construction from growing a
/// positional argument for every service that must be installed before an
/// asynchronous operator can start.
pub(crate) struct PipelineDriverBindings {
    query_memory: Option<crate::runtime::query_memory::QueryMemoryBinding>,
    event_sink: Arc<dyn FragmentEventSink>,
}

impl PipelineDriverBindings {
    pub(crate) fn new(event_sink: Arc<dyn FragmentEventSink>) -> Self {
        Self {
            event_sink,
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
    #[allow(
        dead_code,
        reason = "Formal invocation scope installation is a separate accepted slice."
    )]
    pub(crate) fn query_memory(&self) -> Option<&crate::runtime::query_memory::QueryMemoryBinding> {
        self.query_memory.as_ref()
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
    io_task_exec_time: Option<CounterRef>,
    wait_time: Option<CounterRef>,
    receiver_process_total_time: Option<CounterRef>,
    network_time: Option<CounterRef>,
}

impl PipelineDriver {
    #[allow(
        dead_code,
        reason = "Formal invocation scope installation is a separate accepted slice."
    )]
    pub(crate) fn query_memory(&self) -> Option<&crate::runtime::query_memory::QueryMemoryBinding> {
        self.query_memory.as_ref()
    }
    pub fn new(
        driver_id: i32,
        operators: Vec<Box<dyn Operator>>,
        profiler: Option<Profiler>,
        operator_profiles: Vec<OperatorProfiles>,
        runtime_state: Arc<RuntimeState>,
        fragment_instance_id: Option<(i64, i64)>,
    ) -> Self {
        let query_memory = runtime_state.query_memory().cloned();
        let event_sink: Arc<dyn FragmentEventSink> = Arc::new(NoopFragmentEventSink);
        let mem_root = runtime_state.mem_tracker();
        let operators = operators
            .into_iter()
            .enumerate()
            .map(|(ordinal, mut operator)| {
                let tracker = mem_root.as_ref().map(|root| {
                    MemTracker::new_child(
                        format!("operator {}: {}", ordinal, operator.name()),
                        root,
                    )
                });
                if let Some(tracker) = &tracker {
                    operator.set_mem_tracker(tracker.clone());
                }
                operator.set_fragment_event_sink(event_sink.clone());
                if let Some(profiles) = operator_profiles.get(ordinal) {
                    operator.set_profiles(profiles.clone());
                }
                OperatorInstance::new(ordinal, operator, tracker, runtime_state.error_state())
            })
            .collect();
        Self::from_prepared(
            driver_id,
            operators,
            profiler,
            operator_profiles,
            runtime_state,
            fragment_instance_id,
            PipelineDriverBindings::new(event_sink).with_query_memory(query_memory),
        )
    }

    pub(crate) fn from_prepared(
        driver_id: i32,
        operators: Vec<OperatorInstance>,
        profiler: Option<Profiler>,
        operator_profiles: Vec<OperatorProfiles>,
        runtime_state: Arc<RuntimeState>,
        fragment_instance_id: Option<(i64, i64)>,
        bindings: PipelineDriverBindings,
    ) -> Self {
        let PipelineDriverBindings {
            query_memory,
            event_sink,
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
        let progress = ProgressEndpoint::new(runtime_state.error_state());
        let turn_work = TurnWork::new(NonZeroU64::MIN, DEFAULT_TURN_WORK_UNITS);
        for op in &mut operators {
            op.bind_poll_budget(turn_work.poll_budget());
        }
        Self {
            driver_id,
            operators,
            turn_work,
            value_conversions: novarocks_value_arrays::decode::ValueConversionContext::default(),
            profiler,
            driver_total_time,
            driver_blocked_time,
            driver_input_empty_time,
            driver_output_full_time,
            driver_dependency_wait_time,
            operator_counters,
            runtime_state,
            query_memory,
            fragment_instance_id,
            event_sink: Some(std::mem::ManuallyDrop::new(event_sink)),
            progress,
            state: DriverState::Ready,
            blocked_since: None,
            closed: false,
            exit_started: false,
            cleanup_diagnostics: Vec::new(),
            cleanup_diagnostics_dropped: 0,
            activated: false,
            schedule_state: Arc::new(DriverScheduleState::new()),
            blocked_observable: None,
            control_wait_observables: None,
            frontier_watch: None,
            frontier_observable: Arc::new(Observable::new()),
            frontier_waits: Vec::with_capacity(operator_count.saturating_mul(2)),
            blocked_terminal: None,
            source_backpressure: false,
            source_output_refused: false,
            pending_finish_state: None,
            finish_observable: Arc::new(Observable::new()),
            finish_watched: Vec::new(),
            operator_terminal_signal: None,

            edge_chunks: (0..edge_count).map(|_| None).collect(),
            edge_retirement: (0..edge_count).map(|_| EdgeRetirement::new()).collect(),
            exit_custody: DriverExitCustody::new(),
            process_panic: ProcessPanicCustody::new(),
            edge_root_input: None,
            edge_closed: vec![false; edge_count],
            operator_finishing_set: vec![false; operator_count],
            retired_prefix_len: 0,
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

    /// Tells the source when the downstream starts or stops refusing its
    /// output. Called after dataflow. A source waiting for its own input has
    /// no output, so it never counts as refused.
    fn sync_source_backpressure(&mut self) {
        let held =
            self.source_output_refused || self.edge_chunks.first().is_some_and(Option::is_some);
        if held == self.source_backpressure {
            return;
        }
        self.source_backpressure = held;
        if let Some(source) = self.operators.first_mut() {
            source.on_downstream_backpressure(held);
        }
    }

    pub(crate) fn exit_custody(&self) -> Arc<DriverExitCustody> {
        self.exit_custody.clone()
    }

    pub(crate) fn has_pending_finish(&mut self) -> bool {
        let mut pending = !self.edge_exit_watches().is_empty();
        for index in 0..self.operators.len() {
            pending |= !self.observe_operator_exit(index).is_empty();
        }
        pending
    }

    /// Consume authored events once, while retaining every physical wait.
    /// A ready sibling may close without waiting for another unresolved owner.
    fn observe_operator_exit(&mut self, index: usize) -> Vec<FinishWatch> {
        let control = self.runtime_state.error_state();
        let mut watches = Vec::new();
        self.operators[index].visit_exits(
            &mut |observation| match observation.report_to(&control) {
                ExitProgress::Pending(watch) | ExitProgress::Unresolved(watch) => {
                    watches.push(watch)
                }
                ExitProgress::Exited => {}
            },
        );
        if (self.exit_started || self.operators[index].exit_has_started())
            && self.operators[index].owner_exit_observed()
            && !self.operators[index].close_was_started()
        {
            self.close_operator(index);
            // Close may initiate its own actual asynchronous exit. Its normal
            // return, including a returned diagnostic, is not exit evidence.
            self.operators[index].visit_exits(&mut |observation| match observation
                .report_to(&control)
            {
                ExitProgress::Pending(watch) | ExitProgress::Unresolved(watch) => {
                    watches.push(watch)
                }
                ExitProgress::Exited => {}
            });
        }
        if (self.exit_started || self.operators[index].exit_has_started())
            && self.operators[index].owner_exit_observed()
            && !self
                .edge_retirement
                .get(index)
                .is_some_and(EdgeRetirement::workspace_unresolved)
        {
            self.operators[index].dispose_after_observed_exit();
            // Destruction may itself lose physical exit. Observe its original
            // scope after the attempt; a caught destructor is no exit receipt.
            self.operators[index].visit_exits(&mut |observation| match observation
                .report_to(&control)
            {
                ExitProgress::Pending(watch) | ExitProgress::Unresolved(watch) => {
                    watches.push(watch)
                }
                ExitProgress::Exited => {}
            });
        }
        watches
    }

    pub(crate) fn try_mark_finish_observer_registered(&self, observable: &Arc<Observable>) -> bool {
        self.schedule_state
            .try_mark_finish_observer_registered(observable)
    }

    /// Whether the operators already received the driver's terminal signal
    /// (cancellation or failure). A driver past that point only waits for
    /// its asynchronous owners; another abort turn would change nothing.
    pub(crate) fn operator_terminal_signal_delivered(&self) -> bool {
        self.operator_terminal_signal.is_some()
    }

    /// Asks every operator about pending finish work and returns what the
    /// driver must wait for, or `None` when nothing is pending. Called when
    /// the driver parks; turns only ask [`Self::has_pending_finish`].
    ///
    /// The driver's finish observable generation is sampled before the first
    /// question. A finish observable seen for the first time is forwarded to
    /// the driver's before its owner is asked again, so a completion between
    /// the two questions is observed directly and one after them notifies.
    pub(crate) fn pending_finish_wait_on_worker(
        &mut self,
    ) -> Result<Option<PendingFinishWait>, crate::runtime::observable::ObservableCapacityError>
    {
        let generation = self.finish_observable.generation();
        let mut now = None;
        let mut pending = false;
        let mut recheck_at: Option<Instant> = None;
        // These are only cold exit-watch snapshots, never retained payloads.
        let mut watches = self.collect_exit_watches();
        loop {
            let mut added = false;
            for watch in &watches {
                if let FinishWatch::Notify(observable) = watch
                    && !self.is_finish_watched(observable)
                {
                    let target = Arc::downgrade(&self.finish_observable);
                    let subscription = observable.try_subscribe(Arc::new(move || {
                        if let Some(target) = target.upgrade() {
                            target.notify_observers();
                        }
                    }))?;
                    self.finish_watched
                        .push((Arc::downgrade(observable), subscription));
                    added = true;
                }
            }
            if !added {
                break;
            }
            watches = self.collect_exit_watches();
        }
        for watch in watches {
            pending = true;
            if let FinishWatch::RecheckAfter(interval) = watch {
                let at = *now.get_or_insert_with(Instant::now) + interval;
                recheck_at = Some(recheck_at.map_or(at, |earliest| earliest.min(at)));
            }
        }
        Ok(pending.then(|| PendingFinishWait {
            observable: Arc::clone(&self.finish_observable),
            generation,
            recheck_at,
        }))
    }

    fn collect_exit_watches(&mut self) -> Vec<FinishWatch> {
        let mut watches = self.edge_exit_watches();
        for index in 0..self.operators.len() {
            watches.extend(self.observe_operator_exit(index));
        }
        watches
    }

    fn is_finish_watched(&mut self, observable: &Arc<Observable>) -> bool {
        let candidate = Arc::downgrade(observable);
        self.finish_watched
            .retain(|(watched, _)| watched.strong_count() != 0);
        self.finish_watched
            .iter()
            .any(|(watched, _)| Weak::ptr_eq(watched, &candidate))
    }

    fn cancel_operators(&mut self) {
        self.release_edge_buffers();
        for op in &mut self.operators {
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

    /// The executor catches a panic around `process`. Route it through the
    /// same operator cleanup and PendingFinish latch as an ordinary failure.
    pub(crate) fn capture_process_panic(
        &mut self,
        payload: Box<dyn std::any::Any + Send>,
        publish: impl FnOnce(ExecutionFailure),
    ) -> DriverState {
        let control = self.runtime_state.error_state();
        let cause = self.process_panic.capture(payload, &control, publish);
        // The originating cause was already accepted before payload disposal.
        // Mark the terminal signal before foreign stop hooks; never re-enter
        // ordinary data work or publish the same caught event a second time.
        let state = self
            .operator_terminal_signal
            .get_or_insert_with(|| DriverState::Failed(cause))
            .clone();
        self.fail_operators();
        self.finish_with_state_after_operator_signal(state)
    }

    fn fail_operators(&mut self) {
        self.release_edge_buffers();
        for op in &mut self.operators {
            op.on_driver_failure();
        }
    }

    pub fn process(&mut self, time_slice: Duration) -> DriverState {
        let driver_start = Instant::now();
        if let Err(failure) = self.turn_work.begin_turn(DEFAULT_TURN_WORK_UNITS) {
            return self.finish_with_state(DriverState::Failed(failure));
        }
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
        if self.activated && self.pending_finish_state.is_some() {
            self.blocked_terminal = self.terminal_watch_on_worker();
            if let Err(error) = self.poll_operator_controls() {
                return self.finish_with_state(DriverState::Failed(error));
            }
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

        if !self.activated {
            // A queued driver has passed executor admission. Never activate a
            // prepared driver which was rejected, failed, or cancelled first.
            self.activated = true;
            for operator in &mut self.operators {
                if let Err(error) = operator.activate(&self.runtime_state) {
                    return self.finish_with_state(DriverState::Failed(error));
                }
            }
        }

        let start = Instant::now();
        self.state = DriverState::Running;

        loop {
            if let Some(err) = self.runtime_state.error() {
                return self.finish_with_state(DriverState::Failed(err));
            }
            // Sample before `is_finished`: an early finish published after this
            // point must still wake the driver if this turn parks below.
            self.blocked_terminal = self.terminal_watch_on_worker();
            if let Err(error) = self.poll_operator_controls() {
                return self.finish_with_state(DriverState::Failed(error));
            }
            if let Err(error) = self.retire_finished_prefix() {
                return self.finish_with_state(DriverState::Failed(error));
            }
            if self.is_finished() {
                return self.finish_with_state(DriverState::Finished);
            }
            // Observe exit failures promptly, but an outstanding physical
            // owner cannot freeze another operator's required data/EOS work.
            // Once the terminal sink finishes, the normal exit latch waits for
            // every remaining owner before reporting driver completion.
            self.has_pending_finish();
            if let Some(error) = self.runtime_state.error() {
                return self.finish_with_state(DriverState::Failed(error));
            }

            if start.elapsed() >= time_slice {
                self.state = DriverState::Ready;
                return self.state.clone();
            }

            let mut made_progress = false;

            if let Err(err) = self.propagate_edge_closure(&mut made_progress) {
                return self.finish_with_state(DriverState::Failed(err));
            }
            if let Err(err) = self.drive_set_finishing(&mut made_progress) {
                return self.finish_with_state(DriverState::Failed(err));
            }
            let root_yielded = match self.drive_dataflow(&mut made_progress) {
                Ok(yielded) => yielded,
                Err(err) => return self.finish_with_state(DriverState::Failed(err)),
            };
            if self
                .edge_retirement
                .iter()
                .any(EdgeRetirement::quantum_unresolved)
            {
                // Cleanup health is not an originating execution failure. Stop
                // data work while original edge/operator exit duties remain.
                return self.finish_with_state_after_operator_signal(DriverState::Finished);
            }
            self.sync_source_backpressure();
            if root_yielded {
                // A CPU quantum exhausted no external readiness condition.
                // Keep its original edge grant and schedule the next turn.
                self.state = DriverState::Ready;
                return self.state.clone();
            }

            if made_progress {
                continue;
            }

            let (reason, decision) = self.step_frontier_on_worker();
            match decision {
                WorkerBlockDecision::Blocked(observable, generation, deadline) => {
                    return self.block_on_observable(reason, observable, generation, deadline);
                }
                WorkerBlockDecision::Retry => continue,
                WorkerBlockDecision::Runnable | WorkerBlockDecision::NoBlocker => {
                    self.state = DriverState::Ready;
                    return self.state.clone();
                }
            }
        }
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

    /// The terminal early-finish observable a parked driver must also wake on.
    pub(crate) fn blocked_terminal_snapshot(&self) -> Option<(Arc<Observable>, u64)> {
        self.blocked_terminal
            .as_ref()
            .map(|(observable, generation)| (Arc::clone(observable), *generation))
    }

    /// Subscribe to every operator's control path before observing demand. A
    /// withdrawal racing with a data wait must invalidate that same wait.
    fn terminal_watch_on_worker(&mut self) -> Option<(Arc<Observable>, u64)> {
        let stop = self.runtime_state.error_state().stop_observable();
        let notices = || {
            self.operators
                .iter()
                .filter_map(|operator| operator.control_observable())
                .chain(std::iter::once(stop.clone()))
        };
        let same = self.control_wait_observables.as_ref().is_some_and(|old| {
            let mut current = notices();
            old.sources.iter().all(|old| {
                current
                    .next()
                    .is_some_and(|current| Arc::ptr_eq(old, &current))
            }) && current.next().is_none()
        });
        if !same {
            let sources = notices().collect::<Vec<_>>();
            drop(self.control_wait_observables.take());
            let observable = self.frontier_observable.clone();
            let mut subscriptions = Vec::with_capacity(sources.len());
            for (index, source) in sources.iter().enumerate() {
                if !sources[..index].iter().any(|old| Arc::ptr_eq(old, source)) {
                    subscriptions.push(subscribe_control(source, &observable));
                }
            }
            self.control_wait_observables = Some(ControlWatch {
                sources,
                observable,
                _subscriptions: subscriptions,
            });
        }
        let observable = self.frontier_observable.clone();
        let generation = observable.generation();
        Some((observable, generation))
    }

    fn poll_operator_controls(&mut self) -> ExecutionResult<()> {
        let control = self.runtime_state.error_state();
        let mut context =
            StepContext::new(&control, &mut self.turn_work, &mut self.value_conversions);
        for operator in &mut self.operators {
            operator.poll_control(&mut context)?;
        }
        Ok(())
    }

    pub(crate) fn source_name(&self) -> &str {
        self.operators
            .first()
            .map(|op| op.name())
            .unwrap_or("unknown")
    }

    #[cfg(novarocks_dev_controls)]
    pub(crate) fn sink_name(&self) -> &str {
        self.operators
            .last()
            .map(|op| op.name())
            .unwrap_or("unknown")
    }

    /// Wait only on operations which can advance an unoccupied output edge or
    /// an actual input delivery. Independent continuations contribute a union
    /// of wake sources; an empty source cannot hide accepted downstream work.
    fn step_frontier_on_worker(&mut self) -> (BlockedReason, WorkerBlockDecision) {
        self.frontier_waits.clear();
        let count = self.operators.len();
        let mut output_wait = false;
        for index in 0..count {
            let output_free = self.edge_chunks.get(index).is_none_or(Option::is_none);
            if !output_free {
                continue;
            }
            let operator = &self.operators[index];
            if index != 0 && self.edge_chunks[index - 1].is_some() {
                match operator.admission_next() {
                    Some(AdmissionNext::Wait(wait)) => {
                        output_wait = true;
                        self.frontier_waits.push(wait.clone());
                    }
                    Some(AdmissionNext::Yield) => {
                        self.frontier_watch = None;
                        return (BlockedReason::OutputFull, WorkerBlockDecision::Runnable);
                    }
                    Some(AdmissionNext::ResumeExisting) => {}
                    None if operator.need_input() => {
                        self.frontier_watch = None;
                        return (BlockedReason::OutputFull, WorkerBlockDecision::Runnable);
                    }
                    None => {}
                }
            }
            let downstream = self.operators.get(index + 1);
            if downstream.is_some_and(|next| !next.need_input() || next.is_finished()) {
                continue;
            }
            // Root credit can be a prerequisite for an otherwise ready producer.
            // The exact returned wait belongs to the receiver, not the source.
            if let Some(AdmissionNext::Wait(wait)) =
                downstream.and_then(OperatorInstance::admission_next)
                && !wait.is_ready()
            {
                output_wait = true;
                self.frontier_waits.push(wait.clone());
                continue;
            }
            match operator.work_next() {
                Some(NextStep::Wait(wait)) => {
                    // A retained root grant is still producing the receiver's
                    // input. Its workspace wait is input-empty at this edge;
                    // host capacity/finish waits remain output backpressure.
                    let root_workspace = self
                        .edge_root_input
                        .as_ref()
                        .is_some_and(|(edge, _)| *edge == index);
                    output_wait |= !root_workspace
                        && matches!(
                            wait.reason,
                            super::step::WaitReason::Output | super::step::WaitReason::Finish
                        );
                    self.frontier_waits.push(wait.clone());
                }
                Some(NextStep::Runnable | NextStep::Yield) => {
                    self.frontier_watch = None;
                    return (BlockedReason::InputEmpty, WorkerBlockDecision::Runnable);
                }
                Some(NextStep::NeedInput | NextStep::Finished) | None => {}
            }
        }
        if self.frontier_waits.iter().any(WaitFact::is_ready) {
            return (BlockedReason::InputEmpty, WorkerBlockDecision::Retry);
        }
        if self.frontier_waits.is_empty() {
            self.frontier_watch = None;
            return (BlockedReason::InputEmpty, WorkerBlockDecision::NoBlocker);
        }
        let reason = if output_wait {
            BlockedReason::OutputFull
        } else {
            BlockedReason::InputEmpty
        };
        let deadline = self
            .frontier_waits
            .iter()
            .filter_map(|wait| wait.deadline)
            .min_by_key(|deadline| deadline.at());
        // Reuse the existing control cover for a shared bounded event. Do not
        // consume a second slot merely because it also reports data readiness.
        for wait in &mut self.frontier_waits {
            if self
                .control_wait_observables
                .as_ref()
                .is_some_and(|watch| watch.covers(&wait.observable))
            {
                let (control, generation) = self
                    .blocked_terminal
                    .as_ref()
                    .expect("sampled control cover");
                wait.observable = control.clone();
                wait.observed_generation = *generation;
            }
        }
        let same = self.frontier_watch.as_ref().is_some_and(|watch| {
            watch.sources.iter().all(|source| {
                self.frontier_waits
                    .iter()
                    .any(|wait| Arc::ptr_eq(source, &wait.observable))
            }) && self
                .frontier_waits
                .iter()
                .all(|wait| watch.covers(&wait.observable))
        });
        if !same {
            drop(self.frontier_watch.take());
            // The scheduler sees only this driver-owned endpoint. Owner
            // subscriptions can then leave the active frontier without old
            // scheduler callbacks occupying their bounded notification slots.
            let observable = self.frontier_observable.clone();
            let mut sources = Vec::with_capacity(self.frontier_waits.len());
            let mut subscriptions = Vec::with_capacity(self.frontier_waits.len());
            for wait in &self.frontier_waits {
                if !sources
                    .iter()
                    .any(|source| Arc::ptr_eq(source, &wait.observable))
                {
                    // Control sources already reach this same stable driver
                    // endpoint. Registering it on itself would form a wake loop.
                    if !Arc::ptr_eq(&wait.observable, &observable) {
                        subscriptions.push(subscribe_control(&wait.observable, &observable));
                    }
                    sources.push(wait.observable.clone());
                }
            }
            self.frontier_watch = Some(ControlWatch {
                sources,
                observable,
                _subscriptions: subscriptions,
            });
        }
        let observable = self
            .frontier_watch
            .as_ref()
            .expect("installed data wait cover")
            .observable
            .clone();
        let generation = observable.generation();
        // Original generations are checked after subscribing. A publication
        // before the combined generation was sampled still invalidates its fact.
        let decision = if self.frontier_waits.iter().any(WaitFact::is_ready) {
            WorkerBlockDecision::Retry
        } else {
            WorkerBlockDecision::Blocked(observable, generation, deadline)
        };
        (reason, decision)
    }

    pub(crate) fn set_ready(&mut self) {
        self.finish_blocked_interval();
        self.blocked_observable = None;
        self.blocked_terminal = None;
        self.state = DriverState::Ready;
    }

    fn is_finished(&self) -> bool {
        self.operators
            .last()
            .map(|op| op.is_finished())
            .unwrap_or(true)
    }

    pub(crate) fn report_exec_state_if_necessary(&mut self) {
        if self.is_finished() {
            return;
        }
        if !self.runtime_state.should_report_exec_state() {
            return;
        }
        let Some((hi, lo)) = self.fragment_instance_id else {
            return;
        };
        let sink = self
            .event_sink
            .as_ref()
            .expect("the driver owns its progress holder");
        self.progress.record(
            (**sink).as_ref(),
            FragmentEvent::Progress(FragmentProgress::new(UniqueId::new(hi, lo), 0, 0, 0)),
        );
    }

    fn control_covers(&self, source: &Arc<Observable>) -> bool {
        self.control_wait_observables
            .as_ref()
            .is_some_and(|watch| watch.covers(source))
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
        let (observable, generation) = if self.control_covers(&observable) {
            let (control, observed) = self
                .blocked_terminal
                .as_ref()
                .expect("sampled control cover");
            // Both original conditions and every control fact were observed
            // under this bracket. A publication during data work retries the
            // worker; after parking the existing single forwarder wakes it.
            if observable.generation() != generation || control.generation() != *observed {
                self.set_ready();
                return self.state.clone();
            }
            (control.clone(), *observed)
        } else {
            (observable, generation)
        };
        self.blocked_observable = Some((observable, generation, deadline));
        self.block_or_fail(reason)
    }

    fn block_or_fail(&mut self, reason: BlockedReason) -> DriverState {
        if let Some(error) = self.runtime_state.error() {
            return self.finish_with_state(DriverState::Failed(error));
        }
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
        // is still pending, so a later scheduling turn must not replay these
        // callbacks.
        let state = match (&self.operator_terminal_signal, &state) {
            (
                Some(existing),
                DriverState::Finished | DriverState::Canceled | DriverState::Failed(_),
            ) => existing.clone(),
            (None, DriverState::Canceled) => {
                self.operator_terminal_signal = Some(state.clone());
                self.cancel_operators();
                state
            }
            (None, DriverState::Failed(error)) => {
                // Publish the originating failure before cleanup hooks can
                // fail, panic, or park on an asynchronous owner's actual exit.
                self.runtime_state.error_state().set_error(error.clone());
                self.operator_terminal_signal = Some(state.clone());
                self.fail_operators();
                state
            }
            _ => state,
        };
        self.finish_with_state_after_operator_signal(state)
    }

    fn finish_with_state_after_operator_signal(&mut self, mut state: DriverState) -> DriverState {
        self.finish_blocked_interval();
        if !self.exit_started {
            self.exit_started = true;
            self.value_conversions.clear();
            // No data work remains after accepting this terminal transition.
            // Retire its edge custody and data-only wake registrations before
            // starting physical exits; control/exit observation stays active.
            self.frontier_watch = None;
            self.frontier_waits.clear();
            self.release_edge_buffers();
            for index in 0..self.operators.len() {
                if let Err(error) = self.operators[index].begin_exit() {
                    self.runtime_state.error_state().report_cleanup(&error);
                }
            }
        }
        if matches!(
            state,
            DriverState::Finished | DriverState::Canceled | DriverState::Failed(_)
        ) && self.has_pending_finish()
        {
            self.pending_finish_state = Some(state.clone());
            self.state = DriverState::PendingFinish;
            return self.state.clone();
        }
        if matches!(state, DriverState::Finished)
            && let Some(error) = self.runtime_state.error()
        {
            state = DriverState::Failed(error);
        }
        self.close_operators();
        if matches!(state, DriverState::Finished)
            && let Some(error) = self.runtime_state.error()
        {
            state = DriverState::Failed(error);
        }
        // A synchronous hook cannot certify an asynchronous owner. Even an
        // exceptional hook which initiated exit remains owned until observed.
        if self.has_pending_finish() {
            self.pending_finish_state = Some(state);
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
        self.release_edge_buffers();
        // Each owner closes after its own real exit proof. A pending sibling
        // neither delays a safe close nor permits one across a live borrow.
        for idx in (0..self.operators.len()).rev() {
            self.observe_operator_exit(idx);
        }
        self.closed = self
            .operators
            .iter()
            .all(OperatorInstance::close_was_started);
        if self.profiler.is_some() {
            for idx in 0..self.operator_counters.len() {
                self.update_operator_mem_counters(idx);
            }
        }
    }
    fn close_operator(&mut self, idx: usize) {
        if self.operators[idx].close_was_started() {
            return;
        }
        let start = Instant::now();
        if let Err(error) = self.operators[idx].close() {
            let diagnostic = error.safe_detail();
            self.runtime_state.error_state().report_cleanup(&error);
            if self.cleanup_diagnostics.len() < 4 {
                self.cleanup_diagnostics.push(diagnostic);
            } else {
                self.cleanup_diagnostics_dropped =
                    self.cleanup_diagnostics_dropped.saturating_add(1);
            }
        }
        if self.profiler.is_some() {
            let elapsed_ns = i64::try_from(start.elapsed().as_nanos()).unwrap_or(i64::MAX);
            self.operator_counters[idx].close_time.add(elapsed_ns);
            self.operator_counters[idx]
                .operator_total_time
                .add(elapsed_ns);
        }
    }

    fn release_edge_buffers(&mut self) {
        // Move every original carrier into its already-installed exit slot
        // before invoking any workspace or input destructor.
        for (edge, input) in self.edge_retirement.iter_mut().zip(&mut self.edge_chunks) {
            edge.retain_raw(input);
        }
        if let Some((index, permit)) = self.edge_root_input.take() {
            self.edge_retirement[index].retain_upstream(permit);
            let control = self.runtime_state.error_state();
            self.edge_retirement[index]
                .retire_workspace(&control, || self.operators[index].release_root_workspace());
        }
        let control = self.runtime_state.error_state();
        for edge in &mut self.edge_retirement {
            edge.retire(&control);
        }
    }
    fn edge_exit_watches(&mut self) -> Vec<FinishWatch> {
        let control = self.runtime_state.error_state();
        let mut watches = Vec::new();
        self.process_panic
            .visit(&mut |observation| match observation.report_to(&control) {
                ExitProgress::Pending(watch) | ExitProgress::Unresolved(watch) => {
                    watches.push(watch)
                }
                ExitProgress::Exited => {}
            });
        for edge in &mut self.edge_retirement {
            edge.visit(
                &control,
                &mut |observation| match observation.report_to(&control) {
                    ExitProgress::Pending(watch) | ExitProgress::Unresolved(watch) => {
                        watches.push(watch)
                    }
                    ExitProgress::Exited => {}
                },
            );
        }
        watches
    }

    fn update_operator_mem_counters(&self, operator_idx: usize) {
        if self.profiler.is_none() {
            return;
        }
        let Some(tracker) = self
            .operators
            .get(operator_idx)
            .and_then(OperatorInstance::tracker)
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

    fn drive_dataflow(&mut self, made_progress: &mut bool) -> ExecutionResult<bool> {
        self.drive_push_edges(made_progress)?;
        let mut yielded = self.drive_terminal(made_progress)?;
        yielded |= self.drive_pull_edges(made_progress)?;
        if self
            .edge_retirement
            .iter()
            .any(EdgeRetirement::quantum_unresolved)
        {
            return Ok(yielded);
        }
        self.drive_push_edges(made_progress)?;
        yielded |= self.drive_terminal(made_progress)?;
        Ok(yielded)
    }

    fn retire_finished_prefix(&mut self) -> ExecutionResult<()> {
        let completed = self.operators[self.retired_prefix_len..]
            .iter()
            .rposition(OperatorInstance::is_finished)
            .map(|relative| self.retired_prefix_len + relative);
        match completed {
            Some(index) => self.retire_prefix_through(index),
            None => Ok(()),
        }
    }

    /// The completed operator needs no more input. Its outgoing last output is
    /// still demanded and must drain before EOS; all earlier edges are no
    /// longer demanded. Physical exits can progress beside the surviving suffix.
    fn retire_prefix_through(&mut self, index: usize) -> ExecutionResult<()> {
        let end = index + 1;
        if end <= self.retired_prefix_len {
            return Ok(());
        }
        let control = self.runtime_state.error_state();
        for edge in 0..index {
            self.edge_retirement[edge].retain_raw(&mut self.edge_chunks[edge]);
        }
        if self
            .edge_root_input
            .as_ref()
            .is_some_and(|(edge, _)| *edge < end)
        {
            let (edge, permit) = self.edge_root_input.take().expect("retired upstream grant");
            self.edge_retirement[edge].retain_upstream(permit);
            self.edge_retirement[edge]
                .retire_workspace(&control, || self.operators[edge].release_root_workspace());
            self.edge_retirement[edge].retire(&control);
        }
        for edge in 0..index {
            self.edge_retirement[edge].retire(&control);
        }
        let start = std::mem::replace(&mut self.retired_prefix_len, end);
        for ordinal in start..end {
            // Retired sources have no synthetic input EOS. Surviving receivers
            // still receive EOS only after their incoming last output drains.
            self.operator_finishing_set[ordinal] = true;
            if let Err(error) = self.operators[ordinal].retire_data() {
                let error = error.at_operator(ordinal, PipelineOperation::Control);
                self.runtime_state.error_state().report_cleanup(&error);
            }
        }
        Ok(())
    }

    fn drive_terminal(&mut self, made_progress: &mut bool) -> ExecutionResult<bool> {
        let Some((idx, terminal)) = self.operators.iter_mut().enumerate().last() else {
            return Ok(false);
        };
        if !terminal.has_continuation() {
            return Ok(false);
        }
        let control = self.runtime_state.error_state();
        let mut context =
            StepContext::new(&control, &mut self.turn_work, &mut self.value_conversions);
        let step = terminal
            .drive(&mut context)
            .map_err(|error| error.at_operator(idx, PipelineOperation::Pull))?;
        if step.output.is_some() {
            return Err(DriverFailure::TerminalOutput.into());
        }
        if matches!(step.next, NextStep::Finished) {
            self.retire_prefix_through(idx)?;
        }
        *made_progress = true;
        Ok(matches!(step.next, NextStep::Yield))
    }

    fn drive_push_edges(&mut self, made_progress: &mut bool) -> ExecutionResult<()> {
        let edge_count = self.edge_chunks.len();
        if edge_count == 0 {
            return Ok(());
        }

        for e in (0..edge_count).rev() {
            if self.edge_chunks[e].is_none() {
                continue;
            }
            let downstream_idx = e + 1;
            if self.operators[downstream_idx].is_finished() {
                self.retire_prefix_through(downstream_idx)?;
                continue;
            }
            if downstream_idx < edge_count && self.edge_chunks[downstream_idx].is_some() {
                continue;
            }
            let input = self.edge_chunks[e].take().expect("checked input edge");
            let push_rows = input.rows();
            let before_net = if self.profiler.is_some() {
                self.operator_counters[downstream_idx]
                    .network_time
                    .as_ref()
                    .map(|counter| counter.value())
                    .unwrap_or(0)
            } else {
                0
            };
            let start = Instant::now();
            let control = self.runtime_state.error_state();
            let mut context =
                StepContext::new(&control, &mut self.turn_work, &mut self.value_conversions);
            let result = self.operators[downstream_idx].try_push(&mut context, input);
            let accepted = matches!(result, InputResult::Accepted(_));
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
                let pushed_rows = if accepted { push_rows as i64 } else { 0 };
                if pushed_rows > 0 {
                    counters.push_row_num.add(pushed_rows);
                }
            }
            match result {
                InputResult::Deferred { input, .. } => {
                    self.edge_chunks[e] = Some(input);
                }
                InputResult::Rejected { input, failure } => {
                    self.edge_chunks[e] = Some(input);
                    return Err(failure.at_operator(downstream_idx, PipelineOperation::Admission));
                }
                InputResult::Accepted(Ok(step)) => {
                    if let Some(output) = step.output {
                        if downstream_idx >= edge_count {
                            return Err(DriverFailure::TerminalOutput.into());
                        }
                        self.edge_chunks[downstream_idx] = Some(RawInput::new(output, None));
                    }
                    *made_progress = true;
                }
                InputResult::Accepted(Err(failure)) => {
                    return Err(failure.at_operator(downstream_idx, PipelineOperation::Push));
                }
            }
            if self.operators[downstream_idx].is_finished() {
                self.retire_prefix_through(downstream_idx)?;
            }
        }

        Ok(())
    }

    fn drive_pull_edges(&mut self, made_progress: &mut bool) -> ExecutionResult<bool> {
        let edge_count = self.edge_chunks.len();
        if edge_count == 0 {
            return Ok(false);
        }

        let mut root_yielded = false;
        for e in 0..edge_count {
            if self.edge_chunks[e].is_some() {
                continue;
            }
            let upstream_idx = e;
            let downstream_idx = e + 1;

            let (left, right) = self.operators.split_at_mut(downstream_idx);
            let upstream_op = &mut left[upstream_idx];
            let downstream_op = &mut right[0];

            let upstream = upstream_op;
            let downstream = downstream_op;

            let retained_root_input = self
                .edge_root_input
                .as_ref()
                .is_some_and(|(edge, _)| *edge == e);
            // With no edge Chunk, a retained grant is precisely a yielded
            // CPU continuation. Ordinary has_output need not report a
            // complete Chunk before that continuation can run.
            let has_output = retained_root_input || upstream.has_continuation();
            let control = self.runtime_state.error_state();
            let mut context =
                StepContext::new(&control, &mut self.turn_work, &mut self.value_conversions);
            let admission = if !has_output {
                UpstreamPull::Busy
            } else if retained_root_input {
                if downstream.need_input() {
                    UpstreamPull::Uncovered
                } else {
                    UpstreamPull::Busy
                }
            } else {
                downstream.reserve_upstream(&mut context)?
            };
            let admitted = matches!(admission, UpstreamPull::Uncovered | UpstreamPull::Root(_));
            if e == 0 {
                self.source_output_refused = has_output && !admitted;
            }
            if !admitted {
                continue;
            }
            if let UpstreamPull::Root(input) = admission {
                if self.edge_root_input.is_some() {
                    return Err(DriverFailure::DuplicateRootGrant.into());
                }
                self.edge_root_input = Some((e, input));
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
            let control = self.runtime_state.error_state();
            let mut context = super::step_context::StepContext::new(
                &control,
                &mut self.turn_work,
                &mut self.value_conversions,
            );
            let step = match self.edge_root_input.as_ref() {
                Some((edge, input)) if *edge == e => upstream.drive_precovered(&mut context, input),
                _ => upstream.drive(&mut context).map(RootStep::Complete),
            };
            let (maybe, pending) = match step {
                Ok(RootStep::Complete(step)) => {
                    if matches!(step.next, NextStep::NeedInput | NextStep::Finished) {
                        *made_progress = true;
                    }
                    if matches!(step.next, NextStep::Yield) {
                        *made_progress = true;
                        root_yielded = true;
                    }
                    (Ok(step.output), None)
                }
                Ok(RootStep::Pending(pending)) => (Ok(None), Some(pending)),
                Err(error) => (Err(error), None),
            };
            if pending.is_none()
                && !matches!(&maybe, Ok(Some(_)))
                && self
                    .edge_root_input
                    .as_ref()
                    .is_some_and(|(edge, _)| *edge == e)
            {
                let (_, permit) = self
                    .edge_root_input
                    .take()
                    .expect("original unused Root grant");
                self.edge_retirement[e]
                    .finish_unused_quantum(&control, permit, || upstream.release_root_workspace());
            }
            if let Some(pending) = pending {
                if matches!(pending, RootPending::Yield) {
                    root_yielded = true;
                }
                if matches!(pending, RootPending::Runnable | RootPending::Yield) {
                    *made_progress = true;
                }
            }
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
                    return Err(err.at_operator(upstream_idx, PipelineOperation::Pull));
                }
            };
            if self.edge_retirement[e].is_unresolved() {
                return Ok(root_yielded);
            }
            if let Some(mut chunk) = maybe {
                if self.profiler.is_some() {
                    let counters = &self.operator_counters[upstream_idx];
                    counters.pull_row_num.add(chunk.len() as i64);
                }
                if !self
                    .edge_root_input
                    .as_ref()
                    .is_some_and(|(edge, _)| *edge == e)
                    && let Some(tracker) = self.edge_mem_trackers.get(e).and_then(|v| v.as_ref())
                {
                    // Ownership model: memory is charged to the current holder (queue/operator).
                    // We transfer accounting at queue boundaries via release+consume on the same bytes.
                    chunk.transfer_to(tracker);
                }
                let permit = if self
                    .edge_root_input
                    .as_ref()
                    .is_some_and(|(edge, _)| *edge == e)
                {
                    self.edge_root_input.take().map(|(_, permit)| permit)
                } else {
                    None
                };
                let root_output = permit.is_some();
                // Purely install both original owners before the first foreign
                // workspace action. An unwind cannot implicitly drop `chunk`.
                self.edge_chunks[e] = Some(RawInput::new(chunk, permit));
                if root_output
                    && !self.edge_retirement[e]
                        .finish_output_quantum(&control, || upstream.release_root_workspace())
                {
                    return Ok(root_yielded);
                }
                *made_progress = true;
            }
            if self.operators[upstream_idx].is_finished() {
                self.retire_prefix_through(upstream_idx)?;
            }
        }

        Ok(root_yielded)
    }

    fn propagate_edge_closure(&mut self, made_progress: &mut bool) -> ExecutionResult<()> {
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

    fn drive_set_finishing(&mut self, made_progress: &mut bool) -> ExecutionResult<()> {
        if self.operators.len() < 2 {
            return Ok(());
        }
        // debug!("Driver drive_set_finishing start: driver_id={} op_count={}", self.driver_id, self.operators.len());
        for idx in 1..self.operators.len() {
            if self.operator_finishing_set[idx]
                || (idx < self.edge_chunks.len() && self.edge_chunks[idx].is_some())
            {
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
                .ok_or(DriverFailure::MissingOperator { ordinal: idx })?;
            let op_name = op.name().to_string();
            let start = Instant::now();
            let control = self.runtime_state.error_state();
            let mut context =
                StepContext::new(&control, &mut self.turn_work, &mut self.value_conversions);
            let result = op.finish(&mut context);
            let elapsed = start.elapsed();
            if self.profiler.is_some() {
                let elapsed_ns = i64::try_from(elapsed.as_nanos()).unwrap_or(i64::MAX);
                let counters = &self.operator_counters[idx];
                counters.set_finishing_time.add(elapsed_ns);
                counters.operator_total_time.add(elapsed_ns);
            }
            result.map_err(|err| err.at_operator(idx, PipelineOperation::Finishing))?;
            debug!(
                "Driver set_finishing: driver_id={} op_idx={} op_name={} success. edge_closed[{}]={}",
                self.driver_id, idx, op_name, in_edge, self.edge_closed[in_edge]
            );
            // EOS is a once-only control signal. The receiver continuation
            // drains accepted input and finalization through admitted drive.
            self.operator_finishing_set[idx] = true;
            *made_progress = true;
        }
        Ok(())
    }
}

impl Drop for PipelineDriver {
    fn drop(&mut self) {
        self.value_conversions.clear();
        self.finish_blocked_interval();
        if !self.exit_started {
            self.exit_started = true;
            self.cancel_operators();
            for operator in &mut self.operators {
                if let Err(cause) = operator.begin_exit() {
                    self.runtime_state.error_state().report_cleanup(&cause);
                }
            }
        }
        self.close_operators();
        if self.has_pending_finish() {
            let control = self.runtime_state.error_state();
            let cause = DriverFailure::DroppedWithPendingExit.into();
            self.exit_custody.retain(RetainedDriverOwners {
                _process_panic: std::mem::take(&mut self.process_panic),
                _operators: std::mem::take(&mut self.operators),
                _edges: std::mem::take(&mut self.edge_retirement),
                _edge_inputs: std::mem::take(&mut self.edge_chunks),
                _upstream_grant: self.edge_root_input.take(),
                _runtime: self.runtime_state.clone(),
                _query_memory: self.query_memory.take(),
                _edge_trackers: std::mem::take(&mut self.edge_mem_trackers),
                _event_sink: std::mem::ManuallyDrop::into_inner(
                    self.event_sink
                        .take()
                        .expect("the original progress holder transfers with unresolved work"),
                ),
            });
            // Original resource custody is installed before any reporting.
            control.report_unresolved_exit(&cause);
            control.report_cleanup(&cause);
        }
        if let Some(sink) = self.event_sink.take() {
            self.progress
                .retire(std::mem::ManuallyDrop::into_inner(sink));
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::exec::pipeline::operator::OperatorLifecycle;
    use crate::runtime::fragment::ExecutionResult;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use super::{BlockedReason, DriverState, Observable, PipelineDriver};

    use crate::exec::pipeline::input::PreparedInput;
    use crate::exec::pipeline::operator::{FinishWatch, InputEncodingPolicy};
    use crate::exec::pipeline::shape::{
        SinkKernel, SinkStep, Source, SourceKernel, SourceStart, Transform,
    };
    use crate::exec::pipeline::step::{NextStep, StepResult, WaitFact, WaitReason};
    use crate::exec::pipeline::step_context::StepContext;

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum FinalizationState {
        Complete,
        OwedOutput,
        ExternalEvent,
    }
    use crate::runtime::runtime_state::RuntimeState;

    /// A source that is finished before the driver's first turn, so the edge
    /// into the sink closes immediately and the driver goes straight to
    /// finishing it.
    struct FinishedSource;

    impl OperatorLifecycle for FinishedSource {
        fn name(&self) -> &str {
            "FinishedSource"
        }
    }

    impl SourceKernel for FinishedSource {
        fn start(&self) -> SourceStart {
            SourceStart::Finished
        }
        fn poll_output(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
            panic!("finished source must not be polled")
        }
    }

    struct PendingOnceSource {
        checks: Arc<AtomicUsize>,
    }

    impl OperatorLifecycle for PendingOnceSource {
        fn name(&self) -> &str {
            "PendingOnceSource"
        }
        fn observe_exit(&mut self) -> super::super::operator::ExitObservation {
            if self.checks.fetch_add(1, Ordering::SeqCst) == 0 {
                super::super::operator::ExitObservation::pending(FinishWatch::RecheckAfter(
                    Duration::from_millis(1),
                ))
            } else {
                super::super::operator::ExitObservation::exited()
            }
        }
    }

    impl SourceKernel for PendingOnceSource {
        fn start(&self) -> SourceStart {
            SourceStart::Finished
        }
        fn poll_output(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
            panic!("finished source must not be polled")
        }
    }

    /// A sink whose finishing wait is scripted, standing in for an exchange
    /// sink holding a payload behind a gated outbound edge.
    ///
    /// It mirrors the real sink where it matters: it refuses input while it
    /// owes output, so the driver parks on `OutputFull` rather than spinning,
    /// and it is not finished until its wait is `Complete`.
    struct ScriptedSink {
        wait: Arc<Mutex<FinalizationState>>,
        finalization_steps: Arc<AtomicUsize>,
        finish_signals: AtomicUsize,
        observable: Arc<Observable>,
        open_during_first_finish: bool,
        open_after_first_wait_check: bool,
        wait_checks: AtomicUsize,
    }

    impl ScriptedSink {
        fn new(wait: Arc<Mutex<FinalizationState>>) -> Self {
            Self {
                wait,
                finalization_steps: Arc::new(AtomicUsize::new(0)),
                finish_signals: AtomicUsize::new(0),
                observable: Arc::new(Observable::new()),
                open_during_first_finish: false,
                open_after_first_wait_check: false,
                wait_checks: AtomicUsize::new(0),
            }
        }

        fn opening_during_first_finish(wait: Arc<Mutex<FinalizationState>>) -> Self {
            Self {
                wait,
                finalization_steps: Arc::new(AtomicUsize::new(0)),
                finish_signals: AtomicUsize::new(0),
                observable: Arc::new(Observable::new()),
                open_during_first_finish: true,
                open_after_first_wait_check: false,
                wait_checks: AtomicUsize::new(0),
            }
        }

        fn opening_after_first_wait_check(wait: Arc<Mutex<FinalizationState>>) -> Self {
            Self {
                wait,
                finalization_steps: Arc::new(AtomicUsize::new(0)),
                finish_signals: AtomicUsize::new(0),
                observable: Arc::new(Observable::new()),
                open_during_first_finish: false,
                open_after_first_wait_check: true,
                wait_checks: AtomicUsize::new(0),
            }
        }
    }

    impl OperatorLifecycle for ScriptedSink {
        fn name(&self) -> &str {
            "ScriptedSink"
        }
    }
    impl InputEncodingPolicy for ScriptedSink {}

    impl ScriptedSink {
        fn advance_finalization(&mut self) -> SinkStep {
            let generation = self.observable.generation();
            let prior_calls = self.finalization_steps.fetch_add(1, Ordering::SeqCst);
            let mut wait = self.wait.lock().expect("scripted wait lock");
            if self.open_during_first_finish
                && prior_calls == 0
                && *wait == FinalizationState::ExternalEvent
            {
                *wait = FinalizationState::OwedOutput;
                self.observable.notify_observers();
                return SinkStep::new(NextStep::Runnable, 0);
            }
            let observed = *wait;
            if self.open_after_first_wait_check
                && self.wait_checks.fetch_add(1, Ordering::SeqCst) == 0
                && observed == FinalizationState::ExternalEvent
            {
                *wait = FinalizationState::OwedOutput;
                self.observable.notify_observers();
            }
            match observed {
                FinalizationState::Complete => SinkStep::new(NextStep::Finished, 0),
                FinalizationState::OwedOutput => {
                    *wait = FinalizationState::Complete;
                    SinkStep::new(NextStep::Finished, 0)
                }
                FinalizationState::ExternalEvent => SinkStep::new(
                    NextStep::Wait(WaitFact {
                        reason: WaitReason::Output,
                        observable: self.observable.clone(),
                        observed_generation: generation,
                        deadline: None,
                    }),
                    0,
                ),
            }
        }
    }
    impl SinkKernel for ScriptedSink {
        fn consume(
            &mut self,
            _: &mut StepContext<'_>,
            _: PreparedInput,
        ) -> ExecutionResult<SinkStep> {
            panic!("scripted finalization sink has an empty input stream")
        }
        fn poll(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<SinkStep> {
            Ok(self.advance_finalization())
        }
        fn finish(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<SinkStep> {
            assert_eq!(
                self.finish_signals.fetch_add(1, Ordering::SeqCst),
                0,
                "EOS is signaled only once; later steps resume the continuation"
            );
            Ok(self.advance_finalization())
        }
    }

    struct HeldExitSource(Arc<std::sync::atomic::AtomicBool>);
    impl OperatorLifecycle for HeldExitSource {
        fn name(&self) -> &str {
            "HeldExitSource"
        }
        fn observe_exit(&mut self) -> super::super::operator::ExitObservation {
            if self.0.load(Ordering::SeqCst) {
                super::super::operator::ExitObservation::exited()
            } else {
                super::super::operator::ExitObservation::pending(FinishWatch::RecheckAfter(
                    Duration::ZERO,
                ))
            }
        }
    }
    impl SourceKernel for HeldExitSource {
        fn start(&self) -> SourceStart {
            SourceStart::Finished
        }
        fn poll_output(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
            panic!("already finished source")
        }
    }
    #[test]
    fn actual_driver_conversion_cache_survives_backpressure_and_clears_before_every_pending_exit() {
        use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSlotSchema};
        use crate::exec::pipeline::operator::{
            hydrate_for_downstream_with_factory,
            input_construction::try_runtime_input_construction_factory,
        };
        use arrow::array::{
            Array, ArrayRef, DictionaryArray, Int32Array, StringArray, StringViewArray,
        };
        use arrow::datatypes::{DataType, Field, Int32Type};
        use novarocks_type_contract::{
            FunctionValueType, ValueTypeStorageHold, ValueTypeStorageLifetime,
        };
        use novarocks_types::SlotId;
        struct SourceHold;
        impl ValueTypeStorageHold for SourceHold {}
        struct PlainReceiver;
        impl InputEncodingPolicy for PlainReceiver {}
        for terminal in 0..3 {
            let exited = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let wait = Arc::new(Mutex::new(FinalizationState::ExternalEvent));
            let runtime = Arc::new(RuntimeState::default());
            let sink = ScriptedSink::new(wait.clone());
            let sink_ready = sink.observable.clone();
            let mut driver = PipelineDriver::new(
                1,
                vec![
                    Box::new(Source::new(HeldExitSource(exited.clone()))),
                    Box::new(Transform::hydrating(sink)),
                ],
                None,
                Vec::new(),
                runtime.clone(),
                None,
            );
            let hold = Arc::new(SourceHold);
            let weak_hold = Arc::downgrade(&hold);
            let storage = ValueTypeStorageLifetime::held(hold);
            let values: ArrayRef = Arc::new(StringArray::from(vec![
                "the actual shared external payload",
            ]));
            let weak_values = Arc::downgrade(&values);
            let dictionary: ArrayRef = Arc::new(
                DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![0, 0]), values.clone())
                    .unwrap(),
            );
            let declaration = FunctionValueType::new(DataType::Utf8View, false);
            let slot = ChunkSlotSchema::try_new_typed(
                SlotId::new(1),
                Arc::new(Field::new("payload", dictionary.data_type().clone(), false)),
                declaration.typed_binding().clone(),
                None,
                storage.clone(),
            )
            .unwrap();
            let source = Chunk::try_new_typed_with_columns_owned_observed::<
                crate::runtime::fragment::ExecutionFailure,
                _,
            >(
                Arc::new(ChunkSchema::try_new_typed(vec![slot], storage.clone()).unwrap()),
                vec![dictionary],
                2,
                storage.clone(),
                |_| Ok(()),
            )
            .unwrap();
            let factory = try_runtime_input_construction_factory(
                crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
                crate::runtime::mem_tracker::MemTracker::new_root("driver conversion cache"),
                runtime.error_state(),
            )
            .unwrap();
            driver.value_conversions =
                novarocks_value_arrays::decode::ValueConversionContext::new(0);
            let bypassed = hydrate_for_downstream_with_factory(
                &source,
                &PlainReceiver,
                Some(factory.as_ref()),
                &mut driver.value_conversions,
            )
            .unwrap();
            assert_eq!(driver.value_conversions.stats().cache_skips, 1);
            assert_eq!(
                driver
                    .value_conversions
                    .storage_facts()
                    .owned_descriptor_bytes,
                0
            );
            assert_eq!(
                bypassed.columns()[0]
                    .as_any()
                    .downcast_ref::<StringViewArray>()
                    .unwrap()
                    .value(1),
                "the actual shared external payload"
            );
            drop(bypassed);
            driver.value_conversions =
                novarocks_value_arrays::decode::ValueConversionContext::default();
            let output = hydrate_for_downstream_with_factory(
                &source,
                &PlainReceiver,
                Some(factory.as_ref()),
                &mut driver.value_conversions,
            )
            .unwrap();
            let repeated = hydrate_for_downstream_with_factory(
                &source,
                &PlainReceiver,
                Some(factory.as_ref()),
                &mut driver.value_conversions,
            )
            .unwrap();
            assert_eq!(driver.value_conversions.stats().converted_values, 1);
            assert_eq!(driver.value_conversions.stats().cache_hits, 1);
            let other_values: ArrayRef = Arc::new(StringArray::from(vec![
                "the actual shared external payload",
            ]));
            let other_dictionary: ArrayRef = Arc::new(
                DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![0, 0]), other_values)
                    .unwrap(),
            );
            let other = Chunk::try_new_typed_with_columns_owned_observed::<
                crate::runtime::fragment::ExecutionFailure,
                _,
            >(
                source.chunk_schema_ref(),
                vec![other_dictionary],
                2,
                storage.clone(),
                |_| Ok(()),
            )
            .unwrap();
            let independent = hydrate_for_downstream_with_factory(
                &other,
                &PlainReceiver,
                Some(factory.as_ref()),
                &mut driver.value_conversions,
            )
            .unwrap();
            assert_eq!(
                driver.value_conversions.stats().cache_hits,
                1,
                "equal bytes from another storage must not hit"
            );
            assert_eq!(driver.value_conversions.stats().converted_values, 2);
            drop(independent);
            drop(other);
            drop(repeated);
            drop(source);
            drop(values);
            drop(storage);
            assert!(weak_values.upgrade().is_some());
            assert_eq!(
                driver.process(Duration::from_millis(10)),
                DriverState::Blocked(BlockedReason::OutputFull)
            );
            assert!(
                driver
                    .value_conversions
                    .storage_facts()
                    .owned_descriptor_bytes
                    > 0
            );
            let state = match terminal {
                0 => {
                    *wait.lock().unwrap() = FinalizationState::Complete;
                    sink_ready.notify_observers();
                    driver.set_ready();
                    driver.process(Duration::from_millis(10))
                }
                1 => driver.finish_with_state(DriverState::Canceled),
                _ => driver.finish_with_state(DriverState::Failed(
                    novarocks_functions::KernelFailure::ResourceExhausted.into(),
                )),
            };
            assert_eq!(state, DriverState::PendingFinish);
            assert!(
                weak_values.upgrade().is_none(),
                "cache source Arc retires before actual exit"
            );
            assert_eq!(
                driver
                    .value_conversions
                    .storage_facts()
                    .owned_descriptor_bytes,
                0
            );
            assert!(
                weak_hold.upgrade().is_some(),
                "published alias keeps actual source custody"
            );
            let view = output.columns()[0]
                .as_any()
                .downcast_ref::<StringViewArray>()
                .unwrap();
            assert_eq!(view.value(1), "the actual shared external payload");
            drop(output);
            assert!(
                weak_hold.upgrade().is_none(),
                "last published alias releases source custody"
            );
            exited.store(true, Ordering::SeqCst);
            let final_state = driver.process(Duration::from_millis(10));
            assert!(!matches!(final_state, DriverState::PendingFinish));
        }
    }

    /// An asynchronous source can stop being pending between the driver's
    /// first check and its terminal-state check. That transition cannot
    /// complete the driver while the exchange sink still owes its EOS.
    #[test]
    fn clearing_pending_finish_does_not_skip_the_terminal_sink() {
        let checks = Arc::new(AtomicUsize::new(0));
        let wait = Arc::new(Mutex::new(FinalizationState::ExternalEvent));
        let sink = ScriptedSink::new(wait);
        let finishing_calls = Arc::clone(&sink.finalization_steps);
        let mut driver = PipelineDriver::new(
            1,
            vec![
                Box::new(Source::new(PendingOnceSource {
                    checks: Arc::clone(&checks),
                })),
                Box::new(Transform::hydrating(sink)),
            ],
            None,
            Vec::new(),
            Arc::new(RuntimeState::default()),
            None,
        );

        assert_eq!(
            driver.process(Duration::from_millis(10)),
            DriverState::Blocked(BlockedReason::OutputFull)
        );
        assert!(checks.load(Ordering::SeqCst) >= 1);
        assert!(finishing_calls.load(Ordering::SeqCst) >= 1);

        let state = driver.process(Duration::from_millis(10));
        assert!(matches!(
            state,
            DriverState::Blocked(BlockedReason::OutputFull)
        ));
        assert!(finishing_calls.load(Ordering::SeqCst) >= 1);
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
        let wait = Arc::new(Mutex::new(FinalizationState::ExternalEvent));
        let sink = ScriptedSink::new(Arc::clone(&wait));
        let calls = Arc::clone(&sink.finalization_steps);
        let sink_observable = Arc::clone(&sink.observable);
        let mut driver = PipelineDriver::new(
            1,
            vec![
                Box::new(Source::new(FinishedSource)),
                Box::new(Transform::hydrating(sink)),
            ],
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
        assert_eq!(generation, blocked.generation());
        assert!(deadline.is_none());

        // The edge opens. The sink publishes that the payload it parked is now
        // output only its own next turn can push, and the scheduler requeues
        // the driver from the generation change.
        *wait.lock().expect("scripted wait lock") = FinalizationState::OwedOutput;
        sink_observable.notify_observers();
        assert_ne!(generation, blocked.generation());
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
        let wait = Arc::new(Mutex::new(FinalizationState::ExternalEvent));
        let sink = ScriptedSink::opening_during_first_finish(Arc::clone(&wait));
        let calls = Arc::clone(&sink.finalization_steps);
        let mut driver = PipelineDriver::new(
            2,
            vec![
                Box::new(Source::new(FinishedSource)),
                Box::new(Transform::hydrating(sink)),
            ],
            None,
            Vec::new(),
            Arc::new(RuntimeState::default()),
            None,
        );

        let state = driver.process(Duration::from_millis(10));

        assert!(matches!(state, DriverState::Finished), "actual: {state:?}");
        assert_eq!(
            *wait.lock().expect("scripted wait lock"),
            FinalizationState::Complete
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn a_gate_open_after_finishing_check_cannot_park_output_owed_by_the_driver() {
        let wait = Arc::new(Mutex::new(FinalizationState::ExternalEvent));
        let sink = ScriptedSink::opening_after_first_wait_check(Arc::clone(&wait));
        let calls = Arc::clone(&sink.finalization_steps);
        let mut driver = PipelineDriver::new(
            3,
            vec![
                Box::new(Source::new(FinishedSource)),
                Box::new(Transform::hydrating(sink)),
            ],
            None,
            Vec::new(),
            Arc::new(RuntimeState::default()),
            None,
        );

        let state = driver.process(Duration::from_millis(10));
        assert!(matches!(state, DriverState::Finished), "actual: {state:?}");
        assert_eq!(
            *wait.lock().expect("scripted wait lock"),
            FinalizationState::Complete
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}

#[cfg(test)]
mod terminal_signal_tests {
    use crate::exec::pipeline::step::{StepResult, WaitReason};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use super::*;

    struct CoveredWaitSink {
        event: Arc<Observable>,
    }
    impl OperatorLifecycle for CoveredWaitSink {
        fn name(&self) -> &str {
            "COVERED_CAPACITY_SINK"
        }
    }
    impl StepOperator for CoveredWaitSink {
        fn initial_step(&self) -> NextStep {
            NextStep::NeedInput
        }
        fn control_observable(&self) -> Option<Arc<Observable>> {
            Some(self.event.clone())
        }
        fn reserve_upstream(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<UpstreamPull> {
            Ok(UpstreamPull::Wait(WaitFact {
                reason: super::super::step::WaitReason::Output,
                observable: self.event.clone(),
                observed_generation: self.event.generation(),
                deadline: None,
            }))
        }
        fn try_push(&mut self, _: &mut StepContext<'_>, _: RawInput) -> InputResult {
            panic!("capacity has not accepted input")
        }
        fn drive(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
            panic!("capacity wait must precede data work")
        }
        fn finish(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<()> {
            Ok(())
        }
    }
    #[test]
    fn one_bounded_subscription_covers_a_shared_data_and_control_source() {
        let event = Arc::new(Observable::try_bounded(1).unwrap());
        let mut driver = PipelineDriver::new(
            1,
            vec![
                Box::new(ControlledReadinessOperator {
                    name: "READY_SOURCE",
                    next: FixtureStep::Output,
                    after_output: FixtureStep::Wait(WaitReason::Input),
                    observable: Arc::new(Observable::new()),
                }),
                Box::new(CoveredWaitSink {
                    event: event.clone(),
                }),
            ],
            None,
            Vec::new(),
            Arc::new(RuntimeState::default()),
            None,
        );
        let mut original = None;
        for _ in 0..2 {
            assert_eq!(
                driver.process(Duration::from_secs(1)),
                DriverState::Blocked(BlockedReason::OutputFull)
            );
            let (data, data_generation, _) = driver.blocked_observable_snapshot().unwrap();
            let (control, control_generation) = driver.blocked_terminal_snapshot().unwrap();
            assert!(Arc::ptr_eq(&data, &control));
            assert_eq!(data_generation, control_generation);
            assert!(
                matches!(
                    event.try_subscribe(Arc::new(|| {})),
                    Err(crate::runtime::observable::ObservableCapacityError::CapacityExceeded)
                ),
                "the only bounded slot belongs to the shared cover"
            );
            if let Some(old) = &original {
                assert!(Arc::ptr_eq(old, &data));
            } else {
                original = Some(data.clone());
            }
            driver.set_ready();
        }
        let target = original.unwrap();
        let before = target.generation();
        event.notify_observers();
        assert!(target.generation() > before);
        drop(driver);
        let _reclaimed = event
            .try_subscribe(Arc::new(|| {}))
            .expect("retirement returns the only bounded slot");
    }
    #[test]
    fn control_runnable_schedules_owed_finish_without_source_input_or_control_data_work() {
        use crate::exec::pipeline::shape::ControlStep;
        struct FinishingControlSink {
            event: Arc<Observable>,
            requested: Arc<AtomicBool>,
            scheduled: bool,
            calls: Arc<AtomicUsize>,
        }
        impl OperatorLifecycle for FinishingControlSink {
            fn name(&self) -> &str {
                "CONTROL_FINALIZATION_SINK"
            }
        }
        impl StepOperator for FinishingControlSink {
            fn initial_step(&self) -> NextStep {
                NextStep::NeedInput
            }
            fn control_observable(&self) -> Option<Arc<Observable>> {
                Some(self.event.clone())
            }
            fn poll_control(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<ControlStep> {
                if self.requested.load(Ordering::Acquire) && !self.scheduled {
                    self.scheduled = true;
                    Ok(ControlStep::Runnable)
                } else {
                    Ok(ControlStep::Active)
                }
            }
            fn try_push(&mut self, _: &mut StepContext<'_>, _: RawInput) -> InputResult {
                panic!("no input was produced")
            }
            fn drive(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
                assert!(
                    self.scheduled,
                    "only control made this finalization runnable"
                );
                self.calls.fetch_add(1, Ordering::AcqRel);
                Ok(StepResult::new(None, NextStep::Finished, 0))
            }
            fn finish(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<()> {
                panic!("upstream is still open and empty")
            }
        }
        let polls = Arc::new(AtomicUsize::new(0));
        let calls = Arc::new(AtomicUsize::new(0));
        let requested = Arc::new(AtomicBool::new(false));
        let event = Arc::new(Observable::new());
        let mut driver = PipelineDriver::new(
            1,
            vec![
                Box::new(ReportedWaitSource {
                    event: Arc::new(Observable::new()),
                    polls: polls.clone(),
                }),
                Box::new(FinishingControlSink {
                    event: event.clone(),
                    requested: requested.clone(),
                    scheduled: false,
                    calls: calls.clone(),
                }),
            ],
            None,
            vec![],
            Arc::new(RuntimeState::default()),
            None,
        );
        assert_eq!(
            driver.process(Duration::from_secs(1)),
            DriverState::Blocked(BlockedReason::InputEmpty)
        );
        assert_eq!(polls.load(Ordering::Acquire), 1);
        let (wake, generation) = driver.blocked_terminal_snapshot().unwrap();
        requested.store(true, Ordering::Release);
        event.notify_observers();
        assert_ne!(wake.generation(), generation);
        driver.set_ready();
        assert_eq!(driver.process(Duration::ZERO), DriverState::Ready);
        assert_eq!(
            calls.load(Ordering::Acquire),
            0,
            "control does no data work"
        );
        assert_eq!(
            driver.process(Duration::from_secs(1)),
            DriverState::Finished
        );
        assert_eq!(calls.load(Ordering::Acquire), 1);
        assert_eq!(
            polls.load(Ordering::Acquire),
            1,
            "finalization needs no new source input"
        );
    }

    struct ReportedWaitSource {
        event: Arc<Observable>,
        polls: Arc<AtomicUsize>,
    }
    impl OperatorLifecycle for ReportedWaitSource {
        fn name(&self) -> &str {
            "REPORTED_WAIT_SOURCE"
        }
    }
    impl StepOperator for ReportedWaitSource {
        fn try_push(&mut self, _: &mut StepContext<'_>, _: RawInput) -> InputResult {
            panic!("source cannot accept input")
        }
        fn drive(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
            self.polls.fetch_add(1, Ordering::AcqRel);
            Ok(StepResult::new(
                None,
                NextStep::Wait(WaitFact {
                    reason: super::super::step::WaitReason::Input,
                    observable: self.event.clone(),
                    observed_generation: self.event.generation(),
                    deadline: None,
                }),
                0,
            ))
        }
        fn finish(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<()> {
            panic!("source cannot receive input EOS")
        }
    }
    #[test]
    fn a_direct_step_source_parks_on_its_reported_wait_without_a_readiness_facade() {
        use crate::exec::pipeline::operator_factory::OperatorFactory;
        let event = Arc::new(Observable::new());
        let polls = Arc::new(AtomicUsize::new(0));
        let sink = crate::exec::operators::NoopSinkFactory::new();
        let mut driver = PipelineDriver::new(
            1,
            vec![
                Box::new(ReportedWaitSource {
                    event: event.clone(),
                    polls: polls.clone(),
                }),
                sink.create(1, 0),
            ],
            None,
            Vec::new(),
            Arc::new(RuntimeState::default()),
            None,
        );
        for turn in 0..2 {
            assert_eq!(
                driver.process(Duration::from_secs(1)),
                DriverState::Blocked(BlockedReason::InputEmpty)
            );
            let (wait, generation, _) = driver.blocked_observable_snapshot().unwrap();
            assert_eq!(generation, wait.generation());
            assert_eq!(polls.load(Ordering::Acquire), 1);
            if turn == 0 {
                driver.set_ready();
            }
        }
        let (endpoint, observed, _) = driver.blocked_observable_snapshot().unwrap();
        event.notify_observers();
        assert_ne!(endpoint.generation(), observed);
        assert_eq!(
            driver.process(Duration::from_secs(1)),
            DriverState::Blocked(BlockedReason::InputEmpty)
        );
        assert_eq!(polls.load(Ordering::Acquire), 2);
    }

    #[derive(Clone, Copy)]
    enum FixtureStep {
        NeedInput,
        Output,
        Wait(WaitReason),
        Finished,
    }
    struct ControlledReadinessOperator {
        name: &'static str,
        next: FixtureStep,
        after_output: FixtureStep,
        observable: Arc<Observable>,
    }

    struct DriverPolledSource {
        observable: Arc<Observable>,
    }

    struct BufferedOnceOperator {
        output: Option<Chunk>,
    }

    struct RecoveringTerminal {
        ready: Arc<AtomicBool>,
        pushed: Arc<AtomicUsize>,
        observable: Arc<Observable>,
    }

    struct GenerationChangingSource {
        changed: AtomicBool,
        polls: Arc<AtomicUsize>,
        observable: Arc<Observable>,
    }

    struct SynchronousJoinCompletion {
        phase: Arc<AtomicUsize>,
        pulls: Arc<AtomicUsize>,
    }

    struct DependencyBlockedJoin {
        dependency: DependencyHandle,
    }

    impl OperatorLifecycle for DriverPolledSource {
        fn name(&self) -> &str {
            "DRIVER_POLLED_SOURCE"
        }
    }

    impl StepOperator for DriverPolledSource {
        fn initial_step(&self) -> NextStep {
            NextStep::Wait(WaitFact {
                reason: WaitReason::Input,
                observable: self.observable.clone(),
                observed_generation: self.observable.generation(),
                deadline: None,
            })
        }
        fn try_push(&mut self, _: &mut StepContext<'_>, _: RawInput) -> InputResult {
            panic!("source has no input edge")
        }
        fn drive(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
            panic!("active source polling must remain on a driver worker")
        }
        fn finish(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<()> {
            panic!("source has no input EOS")
        }
    }

    impl OperatorLifecycle for BufferedOnceOperator {
        fn name(&self) -> &str {
            "BUFFERED_ONCE"
        }
    }

    impl crate::exec::pipeline::operator::InputEncodingPolicy for BufferedOnceOperator {}
    impl crate::exec::pipeline::shape::TransformKernel for BufferedOnceOperator {
        fn start(&self) -> crate::exec::pipeline::shape::TransformStart {
            if self.output.is_some() {
                crate::exec::pipeline::shape::TransformStart::Ready
            } else {
                crate::exec::pipeline::shape::TransformStart::NeedInput
            }
        }
        fn apply(
            &mut self,
            _: &mut StepContext<'_>,
            input: super::super::input::PreparedInput,
        ) -> ExecutionResult<StepResult> {
            self.output = Some(input.into_uncovered_chunk()?);
            Ok(StepResult::new(None, NextStep::Runnable, 0))
        }
        fn drive(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
            Ok(StepResult::new(self.output.take(), NextStep::NeedInput, 0))
        }
        fn finish(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
            assert!(self.output.is_none(), "buffered output drains before EOS");
            Ok(StepResult::new(None, NextStep::Finished, 0))
        }
    }

    impl OperatorLifecycle for RecoveringTerminal {
        fn name(&self) -> &str {
            "RECOVERING_TERMINAL"
        }
    }

    impl crate::exec::pipeline::operator::InputEncodingPolicy for RecoveringTerminal {}
    impl crate::exec::pipeline::shape::SinkKernel for RecoveringTerminal {
        fn admission(
            &self,
            _: &mut StepContext<'_>,
            _: &RawInput,
        ) -> ExecutionResult<Option<AdmissionNext>> {
            let generation = self.observable.generation();
            Ok((!self.ready.load(Ordering::Acquire)).then(|| {
                AdmissionNext::Wait(WaitFact {
                    reason: WaitReason::Output,
                    observable: self.observable.clone(),
                    observed_generation: generation,
                    deadline: None,
                })
            }))
        }
        fn consume(
            &mut self,
            _: &mut StepContext<'_>,
            input: super::super::input::PreparedInput,
        ) -> ExecutionResult<crate::exec::pipeline::shape::SinkStep> {
            let _chunk = input.into_uncovered_chunk()?;
            assert!(
                self.ready.load(Ordering::Acquire),
                "terminal consumed input while blocked"
            );
            self.pushed.fetch_add(1, Ordering::AcqRel);
            Ok(crate::exec::pipeline::shape::SinkStep::new(
                NextStep::NeedInput,
                0,
            ))
        }
    }

    impl OperatorLifecycle for GenerationChangingSource {
        fn name(&self) -> &str {
            "GENERATION_CHANGING_SOURCE"
        }
    }

    impl crate::exec::pipeline::shape::SourceKernel for GenerationChangingSource {
        fn poll_output(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
            let generation = self.observable.generation();
            self.polls.fetch_add(1, Ordering::AcqRel);
            if !self.changed.swap(true, Ordering::AcqRel) {
                self.observable.notify_observers();
            }
            Ok(StepResult::new(
                None,
                NextStep::Wait(WaitFact {
                    reason: WaitReason::Input,
                    observable: self.observable.clone(),
                    observed_generation: generation,
                    deadline: None,
                }),
                0,
            ))
        }
    }

    impl OperatorLifecycle for SynchronousJoinCompletion {
        fn name(&self) -> &str {
            "SYNCHRONOUS_JOIN_COMPLETION"
        }
    }

    impl StepOperator for SynchronousJoinCompletion {
        fn try_push(&mut self, _: &mut StepContext<'_>, input: RawInput) -> InputResult {
            InputResult::Deferred {
                input,
                next: AdmissionNext::ResumeExisting,
            }
        }
        fn drive(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
            if self
                .phase
                .compare_exchange(1, 2, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.pulls.fetch_add(1, Ordering::AcqRel);
                Ok(StepResult::new(
                    Some(Chunk::default()),
                    NextStep::Finished,
                    0,
                ))
            } else {
                Ok(StepResult::new(
                    None,
                    if self.phase.load(Ordering::Acquire) == 2 {
                        NextStep::Finished
                    } else {
                        NextStep::Yield
                    },
                    0,
                ))
            }
        }
        fn finish(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<()> {
            Ok(())
        }
    }

    impl OperatorLifecycle for DependencyBlockedJoin {
        fn name(&self) -> &str {
            "DEPENDENCY_BLOCKED_JOIN"
        }
    }

    impl StepOperator for DependencyBlockedJoin {
        fn try_push(&mut self, _: &mut StepContext<'_>, input: RawInput) -> InputResult {
            InputResult::Deferred {
                input,
                next: AdmissionNext::ResumeExisting,
            }
        }
        fn drive(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
            let event = self.dependency.observable();
            let generation = event.generation();
            Ok(StepResult::new(
                None,
                if self.dependency.is_ready() {
                    NextStep::Finished
                } else {
                    NextStep::Wait(WaitFact {
                        reason: WaitReason::Dependency,
                        observable: event,
                        observed_generation: generation,
                        deadline: None,
                    })
                },
                0,
            ))
        }
        fn finish(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<()> {
            Ok(())
        }
    }

    impl OperatorLifecycle for ControlledReadinessOperator {
        fn name(&self) -> &str {
            self.name
        }
    }

    impl ControlledReadinessOperator {
        fn bound(&self, next: FixtureStep) -> NextStep {
            match next {
                FixtureStep::Wait(reason) => NextStep::Wait(WaitFact {
                    reason,
                    observable: self.observable.clone(),
                    observed_generation: self.observable.generation(),
                    deadline: None,
                }),
                FixtureStep::NeedInput => NextStep::NeedInput,
                FixtureStep::Output => NextStep::Runnable,
                FixtureStep::Finished => NextStep::Finished,
            }
        }
    }
    impl StepOperator for ControlledReadinessOperator {
        fn initial_step(&self) -> NextStep {
            self.bound(self.next)
        }
        fn try_push(&mut self, _: &mut StepContext<'_>, input: RawInput) -> InputResult {
            match self.next {
                FixtureStep::NeedInput => {
                    // No columns are inspected by this scheduling fixture.
                    drop(input.accept(None).prepare_original());
                    InputResult::Accepted(Ok(StepResult::new(None, NextStep::NeedInput, 0)))
                }
                FixtureStep::Wait(_) => match self.bound(self.next) {
                    NextStep::Wait(wait) => InputResult::Deferred {
                        input,
                        next: AdmissionNext::Wait(wait),
                    },
                    _ => unreachable!(),
                },
                _ => InputResult::Deferred {
                    input,
                    next: AdmissionNext::ResumeExisting,
                },
            }
        }
        fn drive(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
            if matches!(self.next, FixtureStep::Output) {
                self.next = self.after_output;
                Ok(StepResult::new(
                    Some(Chunk::default()),
                    self.bound(self.next),
                    0,
                ))
            } else {
                Ok(StepResult::new(None, self.bound(self.next), 0))
            }
        }
        fn finish(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<()> {
            self.next = FixtureStep::Finished;
            Ok(())
        }
    }

    struct SignalCountingOperator {
        pending: Arc<AtomicBool>,
        cancel_count: Arc<AtomicUsize>,
        failure_count: Arc<AtomicUsize>,
    }

    impl OperatorLifecycle for SignalCountingOperator {
        fn name(&self) -> &str {
            "SIGNAL_COUNTING"
        }

        fn cancel(&mut self) {
            self.cancel_count.fetch_add(1, Ordering::SeqCst);
        }

        fn on_driver_failure(&mut self) {
            self.failure_count.fetch_add(1, Ordering::SeqCst);
        }

        fn pending_finish(&self) -> Option<FinishWatch> {
            self.pending
                .load(Ordering::SeqCst)
                .then_some(FinishWatch::RecheckAfter(Duration::from_millis(1)))
        }
    }

    impl crate::exec::pipeline::shape::SourceKernel for SignalCountingOperator {
        fn start(&self) -> crate::exec::pipeline::shape::SourceStart {
            crate::exec::pipeline::shape::SourceStart::Finished
        }
        fn poll_output(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
            panic!("finished lifecycle fixture must not produce data")
        }
    }
    fn signal_counting_driver(
        pending: Arc<AtomicBool>,
        cancel_count: Arc<AtomicUsize>,
        failure_count: Arc<AtomicUsize>,
    ) -> PipelineDriver {
        PipelineDriver::new(
            1,
            vec![Box::new(crate::exec::pipeline::shape::Source::new(
                SignalCountingOperator {
                    pending,
                    cancel_count,
                    failure_count,
                },
            ))],
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
        let mut driver = PipelineDriver::new(
            1,
            vec![
                Box::new(ControlledReadinessOperator {
                    name: "UPSTREAM_BLOCKED",
                    next: FixtureStep::Wait(WaitReason::Input),
                    after_output: FixtureStep::Wait(WaitReason::Input),
                    observable: upstream_observable,
                }),
                Box::new(ControlledReadinessOperator {
                    name: "DOWNSTREAM_OUTPUT",
                    next: FixtureStep::Output,
                    after_output: FixtureStep::NeedInput,
                    observable: downstream_observable,
                }),
                Box::new(ControlledReadinessOperator {
                    name: "TERMINAL_READY",
                    next: FixtureStep::NeedInput,
                    after_output: FixtureStep::Wait(WaitReason::Input),
                    observable: terminal_observable,
                }),
            ],
            None,
            Vec::new(),
            Arc::new(RuntimeState::default()),
            None,
        );

        assert!(matches!(
            driver.step_frontier_on_worker().1,
            WorkerBlockDecision::Runnable
        ));
    }

    struct NoOutputEndBuffered {
        output: Option<Chunk>,
        ends: Arc<AtomicUsize>,
    }
    impl OperatorLifecycle for NoOutputEndBuffered {
        fn name(&self) -> &str {
            "NO_OUTPUT_END_BUFFERED"
        }
    }
    impl crate::exec::pipeline::operator::InputEncodingPolicy for NoOutputEndBuffered {}
    impl crate::exec::pipeline::shape::TransformKernel for NoOutputEndBuffered {
        fn start(&self) -> crate::exec::pipeline::shape::TransformStart {
            crate::exec::pipeline::shape::TransformStart::Ready
        }
        fn apply(
            &mut self,
            _: &mut StepContext<'_>,
            _: super::super::input::PreparedInput,
        ) -> ExecutionResult<StepResult> {
            panic!("the source is already at EOS")
        }
        fn drive(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
            Ok(StepResult::new(self.output.take(), NextStep::NeedInput, 1))
        }
        fn end_input(
            &mut self,
            _: &mut StepContext<'_>,
        ) -> ExecutionResult<crate::exec::pipeline::shape::TransformInputEnd> {
            assert!(
                self.output.is_none(),
                "the computing continuation must drain first"
            );
            self.ends.fetch_add(1, Ordering::SeqCst);
            Ok(crate::exec::pipeline::shape::TransformInputEnd::Finished)
        }
        fn finish(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
            panic!("no-output EOS must not require a payload step")
        }
    }

    #[test]
    fn no_output_eos_control_preserves_the_last_real_edge_while_downstream_is_full() {
        let terminal_observable = Arc::new(Observable::new());
        let terminal_ready = Arc::new(AtomicBool::new(false));
        let terminal_pushed = Arc::new(AtomicUsize::new(0));
        let ends = Arc::new(AtomicUsize::new(0));
        let array = Arc::new(arrow::array::Int32Array::from(vec![7]));
        let actual_storage = Arc::downgrade(&array);
        let chunk = crate::exec::chunk::typed_fixture::columns(
            &["value"],
            &[novarocks_types::SlotId::new(1)],
            &[novarocks_type_contract::FunctionValueType::new(
                arrow::datatypes::DataType::Int32,
                false,
            )],
            vec![array],
            1,
        );
        let mut driver = PipelineDriver::new(
            3,
            vec![
                Box::new(ControlledReadinessOperator {
                    name: "SOURCE_EOS",
                    next: FixtureStep::Finished,
                    after_output: FixtureStep::Finished,
                    observable: Arc::new(Observable::new()),
                }),
                Box::new(crate::exec::pipeline::shape::Transform::hydrating(
                    NoOutputEndBuffered {
                        output: Some(chunk),
                        ends: ends.clone(),
                    },
                )),
                Box::new(crate::exec::pipeline::shape::Transform::hydrating(
                    RecoveringTerminal {
                        ready: terminal_ready.clone(),
                        pushed: terminal_pushed.clone(),
                        observable: terminal_observable.clone(),
                    },
                )),
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
        assert_eq!(ends.load(Ordering::SeqCst), 1);
        assert_eq!(terminal_pushed.load(Ordering::SeqCst), 0);
        assert!(
            driver.edge_chunks[1].is_some(),
            "the completed producer's last edge remains demanded"
        );
        assert!(
            actual_storage.upgrade().is_some(),
            "real Arrow storage remains owned by the edge"
        );
        terminal_ready.store(true, Ordering::Release);
        terminal_observable.notify_observers();
        driver.set_ready();
        assert_eq!(
            driver.process(Duration::from_secs(1)),
            DriverState::Finished
        );
        assert_eq!(terminal_pushed.load(Ordering::SeqCst), 1);
        assert_eq!(ends.load(Ordering::SeqCst), 1);
        assert!(driver.edge_chunks[1].is_none());
        assert!(
            actual_storage.upgrade().is_none(),
            "the actual consumed payload retires before Driver Drop"
        );
    }

    #[test]
    fn retained_internal_output_resumes_from_the_downstream_generation() {
        let source_observable = Arc::new(Observable::new());
        let terminal_observable = Arc::new(Observable::new());
        let terminal_ready = Arc::new(AtomicBool::new(false));
        let terminal_pushed = Arc::new(AtomicUsize::new(0));
        let mut driver = PipelineDriver::new(
            3,
            vec![
                Box::new(ControlledReadinessOperator {
                    name: "SOURCE_EMPTY",
                    next: FixtureStep::Wait(WaitReason::Input),
                    after_output: FixtureStep::Wait(WaitReason::Input),
                    observable: source_observable,
                }),
                Box::new(crate::exec::pipeline::shape::Transform::hydrating(
                    BufferedOnceOperator {
                        output: Some(Chunk::default()),
                    },
                )),
                Box::new(crate::exec::pipeline::shape::Transform::hydrating(
                    RecoveringTerminal {
                        ready: Arc::clone(&terminal_ready),
                        pushed: Arc::clone(&terminal_pushed),
                        observable: Arc::clone(&terminal_observable),
                    },
                )),
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
        assert_eq!(generation, blocked.generation());
        assert!(deadline.is_none());

        terminal_ready.store(true, Ordering::Release);
        terminal_observable.notify_observers();
        assert_ne!(generation, blocked.generation());
        driver.set_ready();
        let _ = driver.process(Duration::from_secs(1));
        assert_eq!(terminal_pushed.load(Ordering::Acquire), 1);
    }

    #[test]
    fn short_pipeline_has_no_internal_operator_range() {
        let observable = Arc::new(Observable::new());
        let mut driver = PipelineDriver::new(
            4,
            vec![Box::new(ControlledReadinessOperator {
                name: "ONLY_OPERATOR",
                next: FixtureStep::NeedInput,
                after_output: FixtureStep::NeedInput,
                observable,
            })],
            None,
            Vec::new(),
            Arc::new(RuntimeState::default()),
            None,
        );

        assert!(matches!(
            driver.step_frontier_on_worker().1,
            WorkerBlockDecision::NoBlocker
        ));
    }

    #[test]
    fn worker_retries_when_readiness_generation_changes_during_the_check() {
        let event = Arc::new(Observable::new());
        let polls = Arc::new(AtomicUsize::new(0));
        let mut driver = PipelineDriver::new(
            5,
            vec![Box::new(crate::exec::pipeline::shape::Source::new(
                GenerationChangingSource {
                    changed: AtomicBool::new(false),
                    polls: polls.clone(),
                    observable: event.clone(),
                },
            ))],
            None,
            Vec::new(),
            Arc::new(RuntimeState::default()),
            None,
        );
        assert_eq!(
            driver.process(Duration::from_secs(1)),
            DriverState::Blocked(BlockedReason::InputEmpty)
        );
        assert_eq!(
            polls.load(Ordering::Acquire),
            2,
            "the stale wait must be re-polled before the worker freezes a new block"
        );
        let (endpoint, generation, _) = driver.blocked_observable_snapshot().unwrap();
        assert_eq!(endpoint.generation(), generation);
        assert_eq!(event.generation(), 1);
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
                    next: FixtureStep::Output,
                    after_output: FixtureStep::Wait(WaitReason::Input),
                    observable: source_observable,
                }),
                Box::new(ControlledReadinessOperator {
                    name: "INTERNAL_FULL",
                    next: FixtureStep::Wait(WaitReason::Output),
                    after_output: FixtureStep::Wait(WaitReason::Input),
                    observable: Arc::clone(&internal_observable),
                }),
                Box::new(ControlledReadinessOperator {
                    name: "TERMINAL_READY",
                    next: FixtureStep::NeedInput,
                    after_output: FixtureStep::Wait(WaitReason::Input),
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
        assert_eq!(generation, blocked.generation());
        internal_observable.notify_observers();
        assert_ne!(generation, blocked.generation());
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
                    next: FixtureStep::Output,
                    after_output: FixtureStep::Wait(WaitReason::Input),
                    observable: source_observable,
                }),
                Box::new(SynchronousJoinCompletion {
                    phase: Arc::clone(&join_phase),
                    pulls: Arc::clone(&join_pulls),
                }),
                Box::new(ControlledReadinessOperator {
                    name: "TERMINAL_READY",
                    next: FixtureStep::NeedInput,
                    after_output: FixtureStep::Wait(WaitReason::Input),
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
        assert_eq!(
            driver.process(Duration::from_secs(1)),
            DriverState::Finished
        );
        assert_eq!(join_pulls.load(Ordering::Acquire), 1);
        assert_eq!(driver.process(Duration::ZERO), DriverState::Finished);
        assert_eq!(
            join_pulls.load(Ordering::Acquire),
            1,
            "unmatched output is delivered once"
        );
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
                    next: FixtureStep::Output,
                    after_output: FixtureStep::Wait(WaitReason::Input),
                    observable: source_observable,
                }),
                Box::new(DependencyBlockedJoin {
                    dependency: Arc::clone(&dependency),
                }),
                Box::new(ControlledReadinessOperator {
                    name: "TERMINAL_READY",
                    next: FixtureStep::NeedInput,
                    after_output: FixtureStep::Wait(WaitReason::Input),
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
            DriverState::Blocked(BlockedReason::InputEmpty)
        );
        let (endpoint, generation, _) = driver.blocked_observable_snapshot().unwrap();
        assert!(!dependency.is_ready());

        dependency.set_ready();
        assert!(dependency.is_ready());
        assert_ne!(
            generation,
            endpoint.generation(),
            "the internal dependency wakes the driver"
        );
    }

    #[test]
    fn scheduler_readiness_discovery_never_polls_an_active_source() {
        let event = Arc::new(Observable::new());
        let mut driver = PipelineDriver::new(
            2,
            vec![Box::new(DriverPolledSource {
                observable: event.clone(),
            })],
            None,
            Vec::new(),
            Arc::new(RuntimeState::default()),
            None,
        );
        assert_eq!(
            driver.process(Duration::from_secs(1)),
            DriverState::Blocked(BlockedReason::InputEmpty)
        );
        let (endpoint, generation, _) = driver.blocked_observable_snapshot().unwrap();
        event.notify_observers();
        assert_ne!(
            generation,
            endpoint.generation(),
            "notification forwards a wake without executing the active source"
        );
    }

    #[test]
    fn activation_runs_once_on_first_driver_turn_and_never_on_prestart_abort() {
        struct ActivationProbe(Arc<AtomicUsize>);

        impl OperatorLifecycle for ActivationProbe {
            fn name(&self) -> &str {
                "ACTIVATION_PROBE"
            }

            fn activate(&mut self, _state: &RuntimeState) -> ExecutionResult<()> {
                self.0.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }

        impl crate::exec::pipeline::shape::SourceKernel for ActivationProbe {
            fn poll_output(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
                Ok(StepResult::new(None, NextStep::Yield, 0))
            }
        }
        let count = Arc::new(AtomicUsize::new(0));
        let new_driver = || {
            PipelineDriver::new(
                1,
                vec![Box::new(crate::exec::pipeline::shape::Source::new(
                    ActivationProbe(Arc::clone(&count)),
                ))],
                None,
                Vec::new(),
                Arc::new(RuntimeState::default()),
                None,
            )
        };

        let mut admitted = new_driver();
        assert_eq!(count.load(Ordering::SeqCst), 0);
        assert_eq!(admitted.process(Duration::ZERO), DriverState::Ready);
        assert_eq!(admitted.process(Duration::ZERO), DriverState::Ready);
        assert_eq!(count.load(Ordering::SeqCst), 1);

        let mut rejected = new_driver();
        assert_eq!(rejected.cancel_for_fragment_abort(), DriverState::Canceled);
        assert_eq!(count.load(Ordering::SeqCst), 1);
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

        let original = ExecutionFailure::from(novarocks_functions::KernelFailure::Internal(
            novarocks_functions::KernelDiagnostic::new("first failure"),
        ));
        assert_eq!(
            driver.finish_with_state(DriverState::Failed(original.clone())),
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
            DriverState::Failed(original)
        );
        assert_eq!(failure_count.load(Ordering::SeqCst), 1);
        assert_eq!(cancel_count.load(Ordering::SeqCst), 0);
    }

    struct TypedErrorSink(ExecutionFailure, PipelineOperation);
    impl OperatorLifecycle for TypedErrorSink {
        fn name(&self) -> &str {
            "TypedErrorSink"
        }
    }
    impl crate::exec::pipeline::operator::InputEncodingPolicy for TypedErrorSink {}
    impl crate::exec::pipeline::shape::SinkKernel for TypedErrorSink {
        fn admission(
            &self,
            _: &mut crate::exec::pipeline::step_context::StepContext<'_>,
            _: &RawInput,
        ) -> ExecutionResult<Option<crate::exec::pipeline::step::AdmissionNext>> {
            if self.1 == PipelineOperation::Admission {
                Err(self.0.clone())
            } else {
                Ok(None)
            }
        }
        fn consume(
            &mut self,
            _: &mut crate::exec::pipeline::step_context::StepContext<'_>,
            input: crate::exec::pipeline::input::PreparedInput,
        ) -> ExecutionResult<crate::exec::pipeline::shape::SinkStep> {
            let chunk = input.into_uncovered_chunk()?;
            if self.1 == PipelineOperation::Push {
                Err(self.0.clone())
            } else {
                Ok(crate::exec::pipeline::shape::SinkStep::new(
                    NextStep::NeedInput,
                    chunk.len() as u64,
                ))
            }
        }
        fn finish(
            &mut self,
            _: &mut crate::exec::pipeline::step_context::StepContext<'_>,
        ) -> ExecutionResult<crate::exec::pipeline::shape::SinkStep> {
            Err(self.0.clone())
        }
    }

    #[test]
    fn typed_failure_push_admission_and_finishing_preserve_cause_and_exact_operator_context() {
        use crate::runtime::fragment::{
            ExecutionFailure, ExecutionFailureContext, PipelineOperation,
        };
        for operation in [
            PipelineOperation::Push,
            PipelineOperation::Admission,
            PipelineOperation::Finishing,
        ] {
            let original: ExecutionFailure =
                novarocks_functions::KernelFailure::ResourceExhausted.into();
            let mut driver = PipelineDriver::new(
                41,
                vec![
                    Box::new(crate::exec::pipeline::shape::Transform::hydrating(
                        TypedErrorSink(original.clone(), operation),
                    )),
                    Box::new(crate::exec::pipeline::shape::Transform::hydrating(
                        TypedErrorSink(original.clone(), operation),
                    )),
                ],
                None,
                vec![],
                Arc::new(RuntimeState::default()),
                None,
            );
            let mut progress = false;
            let error = if operation == PipelineOperation::Finishing {
                driver.edge_closed[0] = true;
                driver.drive_set_finishing(&mut progress).unwrap();
                driver.drive_terminal(&mut progress).unwrap_err()
            } else {
                driver.edge_chunks[0] = Some(RawInput::new(Chunk::default(), None));
                let error = if operation == PipelineOperation::Admission {
                    driver.drive_push_edges(&mut progress).unwrap_err()
                } else {
                    driver.drive_push_edges(&mut progress).unwrap();
                    driver.drive_terminal(&mut progress).unwrap_err()
                };
                assert_eq!(
                    driver.edge_chunks[0].is_some(),
                    operation == PipelineOperation::Admission
                );
                error
            };
            assert_eq!(error.cause(), original.cause());
            assert_eq!(
                error.context(),
                Some(ExecutionFailureContext {
                    operator_ordinal: 1,
                    operation
                })
            );
            assert_eq!(progress, operation != PipelineOperation::Admission);
        }
    }

    #[test]
    fn typed_failure_pending_finish_keeps_first_kernel_cause_and_single_signal() {
        use ExecutionFailure;
        let pending = Arc::new(AtomicBool::new(true));
        let cancel_count = Arc::new(AtomicUsize::new(0));
        let failure_count = Arc::new(AtomicUsize::new(0));
        let mut driver = signal_counting_driver(
            Arc::clone(&pending),
            Arc::clone(&cancel_count),
            Arc::clone(&failure_count),
        );
        let first: ExecutionFailure = novarocks_functions::KernelFailure::ResourceExhausted.into();
        assert_eq!(
            driver.finish_with_state(DriverState::Failed(first.clone())),
            DriverState::PendingFinish
        );
        assert_eq!(
            driver.finish_with_state(DriverState::Failed(
                novarocks_functions::KernelFailure::Cancelled.into()
            )),
            DriverState::PendingFinish
        );
        assert_eq!(
            driver.cancel_for_fragment_abort(),
            DriverState::PendingFinish
        );
        assert_eq!(failure_count.load(Ordering::SeqCst), 1);
        assert_eq!(cancel_count.load(Ordering::SeqCst), 0);
        pending.store(false, Ordering::SeqCst);
        assert_eq!(driver.process(Duration::ZERO), DriverState::Failed(first));
        assert_eq!(failure_count.load(Ordering::SeqCst), 1);
        assert_eq!(cancel_count.load(Ordering::SeqCst), 0);
    }

    struct RootCancelledDuringFailure(Arc<AtomicBool>);

    impl OperatorLifecycle for RootCancelledDuringFailure {
        fn name(&self) -> &str {
            "RootCancelledDuringFailure"
        }

        fn activate(&mut self, _: &RuntimeState) -> ExecutionResult<()> {
            if self.0.load(Ordering::SeqCst) {
                Err(novarocks_functions::KernelFailure::Cancelled.into())
            } else {
                Ok(())
            }
        }
    }

    impl crate::exec::pipeline::shape::SourceKernel for RootCancelledDuringFailure {
        fn poll_output(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
            Ok(StepResult::new(None, NextStep::Yield, 0))
        }
    }

    #[derive(Debug)]
    struct PeerCleanupObservation {
        before: Option<ExecutionResult<()>>,
        peer: DriverState,
        peer_conclusion: Option<ExecutionResult<()>>,
        after: Option<ExecutionResult<()>>,
    }

    struct RunPeerDuringFailure {
        cancelled: Arc<AtomicBool>,
        peer: Arc<Mutex<PipelineDriver>>,
        completion: Arc<super::super::global_driver_executor::FragmentCompletion>,
        observations: Arc<Mutex<Vec<PeerCleanupObservation>>>,
    }

    impl OperatorLifecycle for RunPeerDuringFailure {
        fn name(&self) -> &str {
            "RunPeerDuringFailure"
        }

        fn on_driver_failure(&mut self) {
            self.cancelled.store(true, Ordering::SeqCst);
            // Record facts only. The engine catches hook panics, so assertions
            // here could be swallowed without failing the test thread.
            let before = self.completion.conclusion();
            let peer = self.peer.lock().unwrap().process(Duration::ZERO);
            let peer_conclusion = match &peer {
                DriverState::Failed(error) => {
                    self.completion.fail(error.clone());
                    self.completion.conclusion()
                }
                _ => None,
            };
            self.observations
                .lock()
                .unwrap()
                .push(PeerCleanupObservation {
                    before,
                    peer,
                    peer_conclusion,
                    after: self.completion.conclusion(),
                });
        }
    }

    impl crate::exec::pipeline::shape::SourceKernel for RunPeerDuringFailure {
        fn poll_output(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
            Ok(StepResult::new(None, NextStep::Yield, 0))
        }
    }

    #[test]
    fn deferred_originating_failure_precedes_cleanup_induced_peer_failure() {
        use super::super::global_driver_executor::FragmentCompletion;

        use novarocks_execution_contract::{TaskConclusionPort, TaskFailurePhase};
        let owner = crate::runtime::task_control::test_support::DeferredOwner::new();
        let runtime = Arc::new(RuntimeState::new(
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Arc::new(crate::runtime::runtime_state::RuntimeErrorState::for_task(
                Arc::new(owner.view(TaskFailurePhase::Execution)),
            )),
        ));
        let cancelled = Arc::new(AtomicBool::new(false));
        let completion = FragmentCompletion::new(2, runtime.error_state());
        let observations = Arc::new(Mutex::new(Vec::new()));
        let peer = Arc::new(Mutex::new(PipelineDriver::new(
            2,
            vec![Box::new(crate::exec::pipeline::shape::Source::new(
                RootCancelledDuringFailure(Arc::clone(&cancelled)),
            ))],
            None,
            vec![],
            Arc::clone(&runtime),
            None,
        )));
        let mut origin = PipelineDriver::new(
            1,
            vec![Box::new(crate::exec::pipeline::shape::Source::new(
                RunPeerDuringFailure {
                    cancelled,
                    peer,
                    completion: Arc::clone(&completion),
                    observations: Arc::clone(&observations),
                },
            ))],
            None,
            vec![],
            Arc::clone(&runtime),
            None,
        );
        let original =
            ExecutionFailure::from(novarocks_functions::KernelFailure::ResourceExhausted)
                .at_operator(3, PipelineOperation::Push);
        assert_eq!(
            origin.finish_with_state(DriverState::Failed(original.clone())),
            DriverState::Failed(original.clone())
        );
        let observations = observations.lock().unwrap();
        assert_eq!(observations.len(), 1, "the actual cleanup hook ran once");
        let observed = &observations[0];
        assert!(matches!(observed.peer, DriverState::Failed(_)));
        assert!(observed.before.is_none());
        assert!(observed.peer_conclusion.is_none());
        assert!(observed.after.is_none());
        assert!(
            owner.accepted().is_none(),
            "both real failures remain unadjudicated"
        );
        assert!(owner.pending_len() >= 2);
        owner.advance_all();
        let accepted = owner.accepted().unwrap();
        assert!(
            accepted
                .originating_failure()
                .unwrap()
                .same_occurrence(&original),
            "the original proposal preceded cleanup, so its cause wins over the peer stop"
        );
        assert_eq!(accepted.neutral().generation().get(), 1);
        completion.fail(original.clone());
        owner.advance_all();
        assert!(Arc::ptr_eq(&accepted, &owner.accepted().unwrap()));
        assert!(runtime.error().unwrap().same_occurrence(&original));
        assert!(
            completion.stopped_fact().is_none(),
            "the test did not invent driver exit"
        );
    }
    #[test]
    fn originating_failure_precedes_cleanup_induced_peer_failure() {
        use super::super::global_driver_executor::FragmentCompletion;

        let runtime = Arc::new(RuntimeState::default());
        let cancelled = Arc::new(AtomicBool::new(false));
        let completion = FragmentCompletion::new(2, runtime.error_state());
        let observations = Arc::new(Mutex::new(Vec::new()));
        let peer = Arc::new(Mutex::new(PipelineDriver::new(
            2,
            vec![Box::new(crate::exec::pipeline::shape::Source::new(
                RootCancelledDuringFailure(Arc::clone(&cancelled)),
            ))],
            None,
            vec![],
            Arc::clone(&runtime),
            None,
        )));
        let mut origin = PipelineDriver::new(
            1,
            vec![Box::new(crate::exec::pipeline::shape::Source::new(
                RunPeerDuringFailure {
                    cancelled,
                    peer,
                    completion: Arc::clone(&completion),
                    observations: Arc::clone(&observations),
                },
            ))],
            None,
            vec![],
            Arc::clone(&runtime),
            None,
        );
        let original =
            ExecutionFailure::from(novarocks_functions::KernelFailure::ResourceExhausted)
                .at_operator(3, PipelineOperation::Push);
        assert_eq!(
            origin.finish_with_state(DriverState::Failed(original.clone())),
            DriverState::Failed(original.clone())
        );
        // These assertions execute outside the engine's catchable hook.
        let observed = observations.lock().unwrap();
        assert_eq!(
            observed.len(),
            1,
            "the real cleanup hook completed exactly once"
        );
        let observed = &observed[0];
        assert!(
            matches!(observed.peer, DriverState::Failed(_)),
            "{:?}",
            observed.peer
        );
        assert_eq!(observed.before, Some(Err(original.clone())));
        assert!(
            observed
                .before
                .as_ref()
                .unwrap()
                .as_ref()
                .unwrap_err()
                .same_occurrence(&original)
        );
        assert_eq!(observed.peer_conclusion, Some(Err(original.clone())));
        assert_eq!(observed.after, Some(Err(original.clone())));
        assert!(
            observed
                .after
                .as_ref()
                .unwrap()
                .as_ref()
                .unwrap_err()
                .same_occurrence(&original)
        );
        // The peer already reached completion, just as an executor worker can
        // while the originating worker remains in an operator callback.
        let previous = completion.conclusion().unwrap().unwrap_err();
        completion.fail(original.clone());
        assert!(
            completion
                .conclusion()
                .unwrap()
                .unwrap_err()
                .same_occurrence(&previous)
        );
        assert_eq!(completion.conclusion(), Some(Err(original.clone())));
        assert_eq!(runtime.error(), Some(original));
        assert!(completion.stopped_fact().is_none());
    }

    mod typed_construction_driver_tests {
        use super::*;
        use crate::exec::chunk::{ChunkSchema, ChunkSlotSchema};
        use crate::exec::pipeline::driver::PipelineDriverBindings;
        use crate::exec::pipeline::operator::input_construction::{
            InputConstructionFactory, InputConstructionProfile,
            try_runtime_input_construction_factory,
        };
        use crate::runtime::fragment::{ExecutionFailure, PipelineOperation};
        use crate::runtime::mem_tracker::MemTracker;
        use crate::runtime::runtime_state::RuntimeErrorState;
        use arrow::array::{
            Array, ArrayData, ArrayRef, DictionaryArray, Int32Array, LargeStringArray, ListArray,
            StringArray,
        };
        use arrow::buffer::{Buffer, NullBuffer, OffsetBuffer};
        use arrow::datatypes::{Field, Int32Type};
        use novarocks_type_contract::{
            TypedValueBinding, ValueKind, ValueLayout, ValueLogicalType, ValueTypeRef,
            ValueTypeStorageLifetime,
        };
        use novarocks_types::SlotId;

        struct RetainingSink {
            received: Arc<Mutex<Option<Chunk>>>,
            keep_encoding: bool,
        }
        impl OperatorLifecycle for RetainingSink {
            fn name(&self) -> &str {
                "RetainingTypedSink"
            }
        }
        impl crate::exec::pipeline::operator::InputEncodingPolicy for RetainingSink {
            fn retains_encoded_column(&self, _: SlotId, _: &arrow::datatypes::DataType) -> bool {
                self.keep_encoding
            }
        }
        impl crate::exec::pipeline::shape::SinkKernel for RetainingSink {
            fn consume(
                &mut self,
                _: &mut crate::exec::pipeline::step_context::StepContext<'_>,
                input: crate::exec::pipeline::input::PreparedInput,
            ) -> ExecutionResult<crate::exec::pipeline::shape::SinkStep> {
                let chunk = input.into_uncovered_chunk()?;
                let rows = chunk.len() as u64;
                *self.received.lock().unwrap() = Some(chunk);
                Ok(crate::exec::pipeline::shape::SinkStep::new(
                    NextStep::NeedInput,
                    rows,
                ))
            }
        }
        struct EmptySource;
        impl OperatorLifecycle for EmptySource {
            fn name(&self) -> &str {
                "EmptyTypedInputSource"
            }
        }
        impl crate::exec::pipeline::shape::SourceKernel for EmptySource {
            fn start(&self) -> crate::exec::pipeline::shape::SourceStart {
                crate::exec::pipeline::shape::SourceStart::Finished
            }
            fn poll_output(
                &mut self,
                _: &mut crate::exec::pipeline::step_context::StepContext<'_>,
            ) -> ExecutionResult<StepResult> {
                panic!("typed receiver fixture injects its original raw edge directly")
            }
        }
        fn source() -> Chunk {
            let storage = ValueTypeStorageLifetime::pure();
            let array: ArrayRef = Arc::new(
                [Some("{}"), None, Some("{}")]
                    .into_iter()
                    .collect::<DictionaryArray<Int32Type>>(),
            );
            let value = ValueTypeRef::try_new(
                ValueKind::Nominal(ValueLogicalType::Json),
                true,
                storage.clone(),
            )
            .unwrap();
            let binding = TypedValueBinding::try_new(
                value,
                ValueLayout::try_new(array.data_type(), storage.clone()).unwrap(),
            )
            .unwrap();
            let slot = ChunkSlotSchema::try_new_typed(
                SlotId::new(1),
                Arc::new(Field::new("payload", array.data_type().clone(), true)),
                binding,
                None,
                storage.clone(),
            )
            .unwrap();
            Chunk::try_new_typed_with_columns_owned_observed::<
                crate::runtime::fragment::ExecutionFailure,
                _,
            >(
                Arc::new(ChunkSchema::try_new_typed(vec![slot], storage.clone()).unwrap()),
                vec![array],
                3,
                storage,
                |_| Ok(()),
            )
            .unwrap()
        }
        fn required_list_source(
            offset: usize,
            len: usize,
        ) -> crate::runtime::fragment::ExecutionResult<Chunk> {
            // A NULL value deliberately retains nonempty payload bytes. The
            // typed source admits only its active parent/offset-visible rows.
            let values = ArrayData::builder(arrow::datatypes::DataType::Utf8)
                .len(2)
                .buffers(vec![
                    Buffer::from_slice_ref([0i32, 4, 10]),
                    Buffer::from(b"keptshadow".as_slice()),
                ])
                .nulls(Some(NullBuffer::from(vec![true, false])))
                .build()
                .unwrap();
            let dictionary: ArrayRef = Arc::new(
                DictionaryArray::<Int32Type>::try_new(
                    Int32Array::from(vec![1, 0, 1]),
                    Arc::new(StringArray::from(values)),
                )
                .unwrap(),
            );
            let data = ArrayData::builder(arrow::datatypes::DataType::List(Arc::new(Field::new(
                "item",
                dictionary.data_type().clone(),
                false,
            ))))
            .len(3)
            .buffers(vec![
                OffsetBuffer::new(vec![0i32, 1, 2, 3].into())
                    .inner()
                    .inner()
                    .clone(),
            ])
            .child_data(vec![dictionary.to_data()])
            .nulls(Some(NullBuffer::from(vec![false, true, true])))
            .build()
            .unwrap();
            data.validate_full().unwrap();
            let array: ArrayRef = Arc::new(ListArray::from(data));
            let array = array.slice(offset, len);
            let storage = ValueTypeStorageLifetime::pure();
            let binding = TypedValueBinding::try_new(
                ValueTypeRef::try_new(
                    ValueKind::List(
                        ValueTypeRef::try_new(ValueKind::String, false, storage.clone()).unwrap(),
                    ),
                    true,
                    storage.clone(),
                )
                .unwrap(),
                ValueLayout::try_new(array.data_type(), storage.clone()).unwrap(),
            )
            .unwrap();
            let slot = ChunkSlotSchema::try_new_typed(
                SlotId::new(1),
                Arc::new(Field::new("payload", array.data_type().clone(), true)),
                binding,
                None,
                storage.clone(),
            )
            .unwrap();
            Chunk::try_new_typed_with_columns_owned_observed::<
                crate::runtime::fragment::ExecutionFailure,
                _,
            >(
                Arc::new(ChunkSchema::try_new_typed(vec![slot], storage.clone()).unwrap()),
                vec![array],
                len,
                storage,
                |_| Ok(()),
            )
        }
        fn require_plain_list_at_actual_driver(offset: usize, len: usize) {
            let tracker = MemTracker::new_root("required inactive dictionary driver");
            let factory = factory(&tracker, Arc::new(RuntimeErrorState::default()));
            let received = Arc::new(Mutex::new(None));
            let source = required_list_source(offset, len).unwrap();
            let mut driver = driver(
                RetainingSink {
                    received: received.clone(),
                    keep_encoding: false,
                },
                Some(factory),
            );
            driver.edge_chunks[0] = Some(RawInput::new(source.clone(), None));
            let mut progress = false;
            // Desired plain/dictionary equivalence: inactive NULLs are not a
            // reason to reject an already validated immutable typed source.
            driver.drive_push_edges(&mut progress).unwrap();
            driver.drive_terminal(&mut progress).unwrap();
            assert!(progress);
            let output = received.lock().unwrap().take().unwrap();
            let list = output.columns()[0]
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            assert_eq!(list.len(), source.len());
            assert_eq!(list.values().data_type(), &arrow::datatypes::DataType::Utf8);
            let arrow::datatypes::DataType::List(item) = list.data_type() else {
                unreachable!()
            };
            assert!(!item.is_nullable());
            for row in 0..list.len() {
                assert_eq!(list.is_null(row), source.columns()[0].is_null(row));
                if !list.is_null(row) {
                    assert_eq!(
                        list.value(row)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap()
                            .value(0),
                        "kept"
                    );
                }
            }
            drop(driver);
            drop(output);
            assert_eq!(tracker.current(), 0);
        }
        #[test]
        fn required_dictionary_list_parent_null_remains_hydratable_at_actual_driver() {
            require_plain_list_at_actual_driver(0, 2);
        }
        #[test]
        fn required_dictionary_list_unreachable_slice_rows_remain_hydratable_at_actual_driver() {
            require_plain_list_at_actual_driver(1, 1);
        }
        #[test]
        fn locked_string_dictionary_decode_discards_nonempty_null_payload_ranges() {
            let source = required_list_source(0, 2).unwrap();
            let list = source.columns()[0]
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            let dictionary = list
                .values()
                .as_any()
                .downcast_ref::<DictionaryArray<Int32Type>>()
                .unwrap();
            let values = dictionary
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert!(values.is_null(1));
            assert_eq!(values.value_offsets(), &[0, 4, 10]);
            assert_eq!(values.value(1), "shadow");
            let decoded =
                arrow::compute::cast(dictionary, &arrow::datatypes::DataType::Utf8).unwrap();
            let decoded = decoded.as_any().downcast_ref::<StringArray>().unwrap();
            assert_eq!(decoded.value_offsets(), &[0, 0, 4, 4]);
            assert!(decoded.is_null(0));
            assert!(decoded.is_null(2));
            assert_eq!(decoded.value(0), "");
            assert_eq!(decoded.value(1), "kept");
            assert_eq!(decoded.value(2), "");
            let large_values = arrow::compute::cast(
                dictionary.values().as_ref(),
                &arrow::datatypes::DataType::LargeUtf8,
            )
            .unwrap();
            let large_values_array = large_values
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .unwrap();
            assert!(large_values_array.is_null(1));
            assert_eq!(large_values_array.value_offsets(), &[0i64, 4, 10]);
            assert_eq!(large_values_array.value(1), "shadow");
            let large_dictionary =
                DictionaryArray::<Int32Type>::try_new(dictionary.keys().clone(), large_values)
                    .unwrap();
            let decoded =
                arrow::compute::cast(&large_dictionary, &arrow::datatypes::DataType::LargeUtf8)
                    .unwrap();
            let decoded = decoded.as_any().downcast_ref::<LargeStringArray>().unwrap();
            assert_eq!(decoded.value_offsets(), &[0i64, 0, 4, 4]);
            assert!(decoded.is_null(0));
            assert!(decoded.is_null(2));
            assert_eq!(decoded.value(0), "");
            assert_eq!(decoded.value(1), "kept");
            assert_eq!(decoded.value(2), "");
        }
        #[test]
        fn required_dictionary_active_null_is_rejected_before_driver_hydration() {
            let failure = required_list_source(2, 1).unwrap_err();
            use novarocks_failure_contract::FailureProjection;
            assert_eq!(
                failure.failure_fact(),
                novarocks_value_arrays::ValueError::Invariant(
                    novarocks_value_arrays::ValueViolation::RequiredNull,
                )
                .failure_fact(),
            );
        }
        fn factory(
            tracker: &Arc<MemTracker>,
            error: Arc<RuntimeErrorState>,
        ) -> Arc<dyn InputConstructionFactory> {
            try_runtime_input_construction_factory(
                InputConstructionProfile {
                    max_requests: 2048,
                    max_request_bytes: i64::MAX as usize,
                    max_opaque_requests: 2048,
                    max_opaque_bytes: i64::MAX as usize,
                    max_work: usize::MAX,
                    max_depth: 64,
                },
                tracker.clone(),
                error,
            )
            .unwrap()
        }
        fn driver(
            sink: RetainingSink,
            factory: Option<Arc<dyn InputConstructionFactory>>,
        ) -> PipelineDriver {
            let state = Arc::new(RuntimeState::default());
            let source: Box<dyn Operator> =
                Box::new(crate::exec::pipeline::shape::Source::new(EmptySource));
            let mut sink = crate::exec::pipeline::shape::Transform::hydrating(sink);
            if let Some(factory) = factory {
                sink.set_construction_factory(factory);
            }
            PipelineDriver::from_prepared(
                49,
                vec![
                    OperatorInstance::new(0, source, None, state.error_state()),
                    OperatorInstance::new(1, Box::new(sink), None, state.error_state()),
                ],
                None,
                vec![],
                state,
                None,
                PipelineDriverBindings::new(Arc::new(
                    crate::runtime::fragment::io::NoopFragmentEventSink,
                )),
            )
        }
        #[test]
        fn driver_binding_decodes_and_retains_creator_charge_after_driver_and_factory_drop() {
            let tracker = MemTracker::new_root("typed driver construction");
            let error = Arc::new(RuntimeErrorState::default());
            let error_weak = Arc::downgrade(&error);
            let factory = factory(&tracker, error);
            let initial = tracker.current();
            let received = Arc::new(Mutex::new(None));
            let source = source();
            let mut driver = driver(
                RetainingSink {
                    received: received.clone(),
                    keep_encoding: false,
                },
                Some(factory.clone()),
            );
            driver.edge_chunks[0] = Some(RawInput::new(source.clone(), None));
            let mut progress = false;
            driver.drive_push_edges(&mut progress).unwrap();
            driver.drive_terminal(&mut progress).unwrap();
            assert!(progress);
            assert!(driver.edge_chunks[0].is_none());
            assert!(tracker.current() > initial);
            let retained = received.lock().unwrap().take().unwrap();
            assert_eq!(
                retained.record_batch().column(0).data_type(),
                &arrow::datatypes::DataType::Utf8
            );
            assert_eq!(retained.record_batch().column(0).null_count(), 1);
            assert_eq!(
                retained.chunk_schema().slots()[0]
                    .field_schema()
                    .logical_type(),
                Some(ValueLogicalType::Json)
            );
            assert!(matches!(
                source.record_batch().column(0).data_type(),
                arrow::datatypes::DataType::Dictionary(_, _)
            ));
            let clone = retained.clone();
            let scope = factory.begin_input_construction(&retained).unwrap();
            let slice = retained
                .slice_typed_observed(1, 2, scope.storage_lifetime(), |step| {
                    scope.observe(0, &[], step)
                })
                .unwrap();
            assert_eq!(slice.len(), 2);
            assert!(slice.record_batch().column(0).is_null(0));
            drop(scope);
            drop(factory);
            drop(driver);
            assert!(error_weak.upgrade().is_none());
            assert!(tracker.current() > 0);
            drop(retained);
            assert!(tracker.current() > 0);
            drop(clone);
            assert!(tracker.current() > 0);
            drop(slice);
            assert_eq!(tracker.current(), 0);
        }
        #[test]
        fn driver_preserves_original_construction_cancellation_cause_and_context() {
            let tracker = MemTracker::new_root("typed driver cancellation");
            let error = Arc::new(RuntimeErrorState::default());
            let factory = factory(&tracker, error.clone());
            let received = Arc::new(Mutex::new(None));
            let mut driver = driver(
                RetainingSink {
                    received: received.clone(),
                    keep_encoding: false,
                },
                Some(factory),
            );
            let original: ExecutionFailure =
                novarocks_functions::KernelFailure::DeadlineExceeded.into();
            let original = original.at_operator(7, PipelineOperation::Admission);
            error.set_error(original.clone());
            driver.edge_chunks[0] = Some(RawInput::new(source(), None));
            let mut progress = false;
            assert_eq!(
                {
                    driver.drive_push_edges(&mut progress).unwrap();
                    driver.drive_terminal(&mut progress).unwrap_err()
                },
                original
            );
            assert!(
                progress,
                "the raw input was accepted before receiver preparation failed"
            );
            assert!(received.lock().unwrap().is_none());
            drop(driver);
            assert_eq!(tracker.current(), 0);
        }
        #[test]
        fn driver_shares_accepted_encoding_without_new_scope_or_owner() {
            {
                let keep_encoding = true;
                let tracker = MemTracker::new_root("typed driver sharing");
                let received = Arc::new(Mutex::new(None));
                let factory = factory(&tracker, Arc::new(RuntimeErrorState::default()));
                let initial = tracker.current();
                let source = source();
                let mut driver = driver(
                    RetainingSink {
                        received: received.clone(),
                        keep_encoding,
                    },
                    Some(factory),
                );
                driver.edge_chunks[0] = Some(RawInput::new(source.clone(), None));
                let mut progress = false;
                driver.drive_push_edges(&mut progress).unwrap();
                driver.drive_terminal(&mut progress).unwrap();
                assert!(progress);
                assert_eq!(tracker.current(), initial);
                let retained = received.lock().unwrap().take().unwrap();
                assert!(Arc::ptr_eq(
                    source.record_batch().column(0),
                    retained.record_batch().column(0)
                ));
                drop(driver);
                assert_eq!(tracker.current(), 0);
                drop(retained);
            }
        }
    }
}

#[cfg(test)]
#[path = "driver_exit_tests.rs"]
mod exit_tests;

#[cfg(test)]
#[path = "driver_frontier_tests.rs"]
mod frontier_tests;

#[cfg(test)]
#[path = "driver_demand_tests.rs"]
mod demand_tests;

#[cfg(test)]
#[path = "shape/fanout_exit_tests.rs"]
mod fanout_exit_tests;

#[cfg(test)]
mod root_quantum_retirement_tests {
    use super::*;
    use crate::runtime::execution_health::ExecutionHealth;
    use crate::runtime::fragment::io::{
        ResultWriteCredit, RootInputAdmission, RootInputAuthority, RootInputReservationAdmission,
        RootResultWriteSpec, RootWorkExitProgress,
    };
    use std::mem::ManuallyDrop;
    use std::sync::atomic::AtomicUsize;

    struct Storage {
        bytes: Vec<u8>,
        drops: Arc<AtomicUsize>,
    }
    impl novarocks_type_contract::ValueTypeStorageHold for Storage {}
    impl Drop for Storage {
        fn drop(&mut self) {
            assert_eq!(self.bytes[0], 157);
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct Primary {
        data: ManuallyDrop<Arc<Storage>>,
        drops: Arc<[AtomicUsize; 2]>,
    }
    struct Secondary {
        _data: ManuallyDrop<Arc<Storage>>,
        drops: Arc<[AtomicUsize; 2]>,
    }
    impl Drop for Primary {
        fn drop(&mut self) {
            self.drops[0].fetch_add(1, Ordering::SeqCst);
            let data = unsafe { ManuallyDrop::take(&mut self.data) };
            std::panic::panic_any(Secondary {
                _data: ManuallyDrop::new(data),
                drops: self.drops.clone(),
            });
        }
    }
    impl Drop for Secondary {
        fn drop(&mut self) {
            self.drops[1].fetch_add(1, Ordering::SeqCst);
            panic!("unknown secondary must remain in its original bounded cell");
        }
    }
    struct Workspace {
        data: Option<Arc<Storage>>,
        fault: bool,
        opaque: Arc<[AtomicUsize; 2]>,
    }
    impl Drop for Workspace {
        fn drop(&mut self) {
            if self.fault {
                std::panic::panic_any(Primary {
                    data: ManuallyDrop::new(self.data.take().unwrap()),
                    drops: self.opaque.clone(),
                });
            }
        }
    }
    #[derive(Default)]
    struct Counters {
        pulls: AtomicUsize,
        workspace: AtomicUsize,
        credits: AtomicUsize,
        accepted: AtomicUsize,
        held: AtomicUsize,
        source_drop: AtomicUsize,
        sink_drop: AtomicUsize,
        begins: AtomicUsize,
        closes: AtomicUsize,
    }
    struct RootSource {
        counts: Arc<Counters>,
        storage_drops: Arc<AtomicUsize>,
        storage: Arc<Mutex<Vec<Weak<Storage>>>>,
        arrays: Arc<Mutex<Vec<Weak<arrow::array::Int32Array>>>>,
        workspace: Option<Workspace>,
        output: bool,
        fault: bool,
        opaque: Arc<[AtomicUsize; 2]>,
    }
    impl Drop for RootSource {
        fn drop(&mut self) {
            self.counts.source_drop.fetch_add(1, Ordering::SeqCst);
        }
    }
    impl OperatorLifecycle for RootSource {
        fn name(&self) -> &str {
            "REAL_ROOT_QUANTUM_SOURCE"
        }
        fn begin_exit(&mut self) -> ExecutionResult<()> {
            self.counts.begins.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        fn close(&mut self) -> ExecutionResult<()> {
            self.counts.closes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    impl StepOperator for RootSource {
        fn initial_step(&self) -> NextStep {
            NextStep::Runnable
        }
        fn try_push(&mut self, _: &mut StepContext<'_>, _: RawInput) -> InputResult {
            panic!("source input")
        }
        fn drive(
            &mut self,
            _: &mut StepContext<'_>,
        ) -> ExecutionResult<super::super::step::StepResult> {
            panic!("must borrow exact Root input")
        }
        fn finish(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<()> {
            panic!("source EOS")
        }
        fn drive_with_root_input(
            &mut self,
            _: &mut StepContext<'_>,
            permit: &RootInputPermit,
        ) -> ExecutionResult<RootStep> {
            let quantum = self.counts.pulls.fetch_add(1, Ordering::SeqCst);
            assert!(
                quantum < 2,
                "unknown quantum must not resume normal production"
            );
            assert_eq!(
                self.counts.held.load(Ordering::SeqCst),
                permit.retained_bytes()
            );
            let storage = Arc::new(Storage {
                bytes: vec![157; 64 * 1024],
                drops: self.storage_drops.clone(),
            });
            self.storage.lock().unwrap().push(Arc::downgrade(&storage));
            self.workspace = Some(Workspace {
                data: Some(storage.clone()),
                fault: self.fault,
                opaque: self.opaque.clone(),
            });
            let chunk = if self.output {
                let array = Arc::new(arrow::array::Int32Array::from(vec![1, 2, 3]));
                self.arrays.lock().unwrap().push(Arc::downgrade(&array));
                let schema = Arc::new(arrow::datatypes::Schema::new(vec![
                    arrow::datatypes::Field::new("value", arrow::datatypes::DataType::Int32, false),
                ]));
                let batch =
                    arrow::record_batch::RecordBatch::try_new(schema.clone(), vec![array]).unwrap();
                let chunk_schema =
                    crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                        &schema,
                        &[novarocks_types::SlotId::new(1)],
                    )
                    .unwrap();
                let hold = novarocks_type_contract::ValueTypeStorageLifetime::held(storage);
                Some(Chunk::new_with_chunk_schema(batch, chunk_schema).retaining_array_type_storage_observed::<novarocks_type_contract::owned_resources::layout::LayoutResourceError>(&hold, |_| Ok(())).unwrap())
            } else {
                None
            };
            Ok(RootStep::Complete(super::super::step::StepResult::new(
                chunk,
                if quantum == 1 {
                    NextStep::Finished
                } else {
                    NextStep::Runnable
                },
                0,
            )))
        }
        fn release_root_workspace(&mut self) {
            self.counts.workspace.fetch_add(1, Ordering::SeqCst);
            // The actual materializer takes before Drop as well. A caught
            // destructor cannot prove its captured data has exited.
            drop(self.workspace.take());
        }
    }
    struct RootReceiver {
        authority: RootInputAuthority,
        counts: Arc<Counters>,
        finishing: bool,
        generations: Arc<Mutex<Vec<u64>>>,
        credit_fault: bool,
        credit_data: Arc<Mutex<Vec<Weak<Storage>>>>,
        credit_opaque: Arc<[AtomicUsize; 2]>,
        storage_drops: Arc<AtomicUsize>,
    }
    impl Drop for RootReceiver {
        fn drop(&mut self) {
            self.counts.sink_drop.fetch_add(1, Ordering::SeqCst);
        }
    }
    impl OperatorLifecycle for RootReceiver {
        fn name(&self) -> &str {
            "REAL_ROOT_QUANTUM_RECEIVER"
        }
    }
    impl StepOperator for RootReceiver {
        fn initial_step(&self) -> NextStep {
            NextStep::NeedInput
        }
        fn reserve_upstream(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<UpstreamPull> {
            let RootInputReservationAdmission::Granted(mut reservation) =
                self.authority.try_reserve().unwrap()
            else {
                return Ok(UpstreamPull::Busy);
            };
            let bytes = self.authority.required_bytes();
            self.counts.held.fetch_add(bytes, Ordering::SeqCst);
            let counts = self.counts.clone();
            let fault = self.credit_fault;
            let data = fault.then(|| {
                let data = Arc::new(Storage {
                    bytes: vec![157; 64 * 1024],
                    drops: self.storage_drops.clone(),
                });
                self.credit_data.lock().unwrap().push(Arc::downgrade(&data));
                data
            });
            let data = Mutex::new(data);
            let opaque = self.credit_opaque.clone();
            let mut credit = Some(ResultWriteCredit::new(bytes, move |bytes| {
                counts.credits.fetch_add(1, Ordering::SeqCst);
                if fault {
                    std::panic::panic_any(Primary {
                        data: ManuallyDrop::new(data.lock().unwrap().take().unwrap()),
                        drops: opaque.clone(),
                    });
                }
                counts.held.fetch_sub(bytes, Ordering::SeqCst);
            }));
            let RootInputAdmission::Granted(permit) = reservation.try_bind(&mut credit).unwrap()
            else {
                panic!("reserved exact Root position must bind");
            };
            self.generations.lock().unwrap().push(permit.generation());
            Ok(UpstreamPull::Root(permit))
        }
        fn try_push(&mut self, _: &mut StepContext<'_>, input: RawInput) -> InputResult {
            assert!(
                self.authority.owns_original(input.permit().unwrap()),
                "output must retain its original unretired grant"
            );
            assert_eq!(
                self.counts.held.load(Ordering::SeqCst),
                self.authority.required_bytes()
            );
            let (chunk, mut permit) = input.into_edge_parts();
            drop(chunk);
            assert_eq!(
                permit.as_mut().unwrap().retire(),
                RootWorkExitProgress::Exited
            );
            drop(permit);
            self.counts.accepted.fetch_add(1, Ordering::SeqCst);
            InputResult::Accepted(Ok(super::super::step::StepResult::new(
                None,
                NextStep::NeedInput,
                0,
            )))
        }
        fn drive(
            &mut self,
            _: &mut StepContext<'_>,
        ) -> ExecutionResult<super::super::step::StepResult> {
            Ok(super::super::step::StepResult::new(
                None,
                if self.finishing {
                    NextStep::Finished
                } else {
                    NextStep::NeedInput
                },
                0,
            ))
        }
        fn finish(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<()> {
            self.finishing = true;
            Ok(())
        }
    }
    struct Fixture {
        driver: PipelineDriver,
        counts: Arc<Counters>,
        storage_drops: Arc<AtomicUsize>,
        storage: Arc<Mutex<Vec<Weak<Storage>>>>,
        arrays: Arc<Mutex<Vec<Weak<arrow::array::Int32Array>>>>,
        credit_data: Arc<Mutex<Vec<Weak<Storage>>>>,
        health: Arc<ExecutionHealth>,
        generations: Arc<Mutex<Vec<u64>>>,
        workspace_opaque: Arc<[AtomicUsize; 2]>,
        credit_opaque: Arc<[AtomicUsize; 2]>,
        authority: RootInputAuthority,
    }
    fn fixture(output: bool, workspace_fault: bool, credit_fault: bool) -> Fixture {
        use novarocks_types::{
            AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
        };
        let task = novarocks_execution_contract::TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).unwrap()).unwrap(),
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            BackendProcessId::new_v7(),
        );
        let spec = RootResultWriteSpec {
            task,
            contract: Arc::new(novarocks_result_contract::RootOutputContract::new(
                novarocks_result_contract::RootProfileId::V1,
                novarocks_result_contract::FrozenRootOutput::CountOnly,
            )),
        };
        let health = Arc::new(ExecutionHealth::new());
        let authority = RootInputAuthority::new(&spec, health.clone());
        let counts = Arc::new(Counters::default());
        let storage_drops = Arc::new(AtomicUsize::new(0));
        let storage = Arc::new(Mutex::new(Vec::new()));
        let arrays = Arc::new(Mutex::new(Vec::new()));
        let credit_data = Arc::new(Mutex::new(Vec::new()));
        let generations = Arc::new(Mutex::new(Vec::new()));
        let workspace_opaque = Arc::new(std::array::from_fn(|_| AtomicUsize::new(0)));
        let credit_opaque = Arc::new(std::array::from_fn(|_| AtomicUsize::new(0)));
        let driver = PipelineDriver::new(
            101,
            vec![
                Box::new(RootSource {
                    counts: counts.clone(),
                    storage_drops: storage_drops.clone(),
                    storage: storage.clone(),
                    arrays: arrays.clone(),
                    workspace: None,
                    output,
                    fault: workspace_fault,
                    opaque: workspace_opaque.clone(),
                }),
                Box::new(RootReceiver {
                    authority: authority.clone(),
                    counts: counts.clone(),
                    finishing: false,
                    generations: generations.clone(),
                    credit_fault,
                    credit_data: credit_data.clone(),
                    credit_opaque: credit_opaque.clone(),
                    storage_drops: storage_drops.clone(),
                }),
            ],
            None,
            Vec::new(),
            Arc::new(RuntimeState::default()),
            None,
        );
        Fixture {
            driver,
            counts,
            storage_drops,
            storage,
            arrays,
            credit_data,
            health,
            generations,
            workspace_opaque,
            credit_opaque,
            authority,
        }
    }
    #[test]
    fn two_healthy_root_quanta_release_each_workspace_and_only_retire_unused_or_delivered_grants() {
        for output in [false, true] {
            let mut f = fixture(output, false, false);
            let mut state = DriverState::Ready;
            for _ in 0..8 {
                state = f.driver.process(Duration::from_secs(1));
                if state == DriverState::Finished {
                    break;
                }
            }
            assert_eq!(state, DriverState::Finished);
            assert_eq!(f.counts.pulls.load(Ordering::SeqCst), 2);
            assert_eq!(f.counts.workspace.load(Ordering::SeqCst), 2);
            assert_eq!(f.counts.credits.load(Ordering::SeqCst), 2);
            assert_eq!(
                f.counts.accepted.load(Ordering::SeqCst),
                if output { 2 } else { 0 }
            );
            assert_eq!(f.generations.lock().unwrap().as_slice(), &[1, 2]);
            assert_eq!(f.storage_drops.load(Ordering::SeqCst), 2);
            assert!(
                f.storage
                    .lock()
                    .unwrap()
                    .iter()
                    .all(|data| data.upgrade().is_none())
            );
            assert!(
                f.arrays
                    .lock()
                    .unwrap()
                    .iter()
                    .all(|data| data.upgrade().is_none())
            );
            assert_eq!(f.counts.held.load(Ordering::SeqCst), 0);
            assert!(f.driver.runtime_state.error().is_none());
            assert!(
                f.driver
                    .runtime_state
                    .error_state()
                    .local_exit_fault()
                    .is_none()
            );
            assert!(f.health.is_healthy());
            drop(f.driver);
            assert_eq!(f.counts.source_drop.load(Ordering::SeqCst), 1);
            assert_eq!(f.counts.sink_drop.load(Ordering::SeqCst), 1);
        }
    }
    #[test]
    fn unknown_root_workspace_keeps_unsubmitted_chunk_grant_box_and_bounded_opaque_while_sibling_exits()
     {
        let mut f = fixture(true, true, false);
        let control = f.driver.runtime_state.error_state();
        assert_eq!(
            f.driver.process(Duration::from_secs(1)),
            DriverState::PendingFinish
        );
        assert!(control.error().is_none());
        assert!(control.local_exit_fault().is_some());
        assert_eq!(f.counts.accepted.load(Ordering::SeqCst), 0);
        assert_eq!(f.counts.pulls.load(Ordering::SeqCst), 1);
        assert_eq!(f.counts.workspace.load(Ordering::SeqCst), 1);
        assert_eq!(f.counts.credits.load(Ordering::SeqCst), 0);
        assert_eq!(
            f.counts.held.load(Ordering::SeqCst),
            f.authority.required_bytes()
        );
        assert_eq!(f.counts.source_drop.load(Ordering::SeqCst), 0);
        assert_eq!(f.counts.sink_drop.load(Ordering::SeqCst), 1);
        assert_eq!(f.counts.begins.load(Ordering::SeqCst), 1);
        assert_eq!(f.counts.closes.load(Ordering::SeqCst), 1);
        for _ in 0..2 {
            assert_eq!(f.driver.process(Duration::ZERO), DriverState::PendingFinish);
        }
        assert_eq!(f.workspace_opaque[0].load(Ordering::SeqCst), 1);
        assert_eq!(f.workspace_opaque[1].load(Ordering::SeqCst), 0);
        drop(f.driver);
        assert_eq!(f.counts.source_drop.load(Ordering::SeqCst), 0);
        assert!(
            f.arrays.lock().unwrap()[0].upgrade().is_some(),
            "the original unpublished output was installed before foreign release"
        );
        assert!(f.storage.lock().unwrap()[0].upgrade().is_some());
        assert_eq!(f.storage_drops.load(Ordering::SeqCst), 0);
        assert_eq!(
            f.counts.held.load(Ordering::SeqCst),
            f.authority.required_bytes()
        );
        assert!(control.error().is_none());
    }
    #[test]
    fn unknown_unused_root_permit_stops_data_without_task_failure_or_releasing_original_credit() {
        let mut f = fixture(false, false, true);
        let control = f.driver.runtime_state.error_state();
        assert_eq!(
            f.driver.process(Duration::from_secs(1)),
            DriverState::PendingFinish
        );
        assert!(control.error().is_none());
        assert!(f.health.failure().is_some());
        assert_eq!(f.counts.pulls.load(Ordering::SeqCst), 1);
        assert_eq!(f.counts.workspace.load(Ordering::SeqCst), 1);
        assert_eq!(f.counts.credits.load(Ordering::SeqCst), 1);
        assert_eq!(
            f.storage_drops.load(Ordering::SeqCst),
            1,
            "healthy workspace data really exited before exact permit retirement"
        );
        assert_eq!(f.counts.source_drop.load(Ordering::SeqCst), 1);
        assert_eq!(f.counts.sink_drop.load(Ordering::SeqCst), 1);
        for _ in 0..2 {
            assert_eq!(f.driver.process(Duration::ZERO), DriverState::PendingFinish);
        }
        assert_eq!(f.credit_opaque[0].load(Ordering::SeqCst), 1);
        assert_eq!(f.credit_opaque[1].load(Ordering::SeqCst), 0);
        drop(f.driver);
        assert_eq!(
            f.counts.held.load(Ordering::SeqCst),
            f.authority.required_bytes()
        );
        assert!(f.credit_data.lock().unwrap()[0].upgrade().is_some());
        assert!(!f.authority.is_available());
        assert!(control.error().is_none());
    }
}
