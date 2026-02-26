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
//! - Types: `DriverState`, `DriverScheduleState`, `ScheduleToken`, `PipelineDriver`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::time::{Duration, Instant};

use super::operator::{BlockedReason, Operator};
use crate::common::types::{UniqueId, format_uuid};
use crate::exec::chunk::Chunk;
use crate::exec::pipeline::dependency::DependencyHandle;
use crate::exec::pipeline::schedule::observer::Observable;
use crate::metrics;
use crate::novarocks_logging::{debug, error};
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::profile::Profiler;
use crate::runtime::profile::{CounterRef, OperatorProfiles, clamp_u128_to_i64};
use crate::runtime::runtime_state::RuntimeState;
use crate::service::fe_report;

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

#[derive(Debug)]
/// Scheduling metadata for one driver including blocking and requeue state.
pub(crate) struct DriverScheduleState {
    in_blocked: AtomicBool,
    need_check_reschedule: AtomicBool,
    schedule_token: AtomicBool,
    observer_mask: AtomicU8,
}

const OBSERVER_SOURCE: u8 = 1;
const OBSERVER_SINK: u8 = 1 << 1;

impl DriverScheduleState {
    pub(crate) fn new() -> Self {
        Self {
            in_blocked: AtomicBool::new(false),
            need_check_reschedule: AtomicBool::new(false),
            schedule_token: AtomicBool::new(false),
            observer_mask: AtomicU8::new(0),
        }
    }

    pub(crate) fn is_in_blocked(&self) -> bool {
        self.in_blocked.load(Ordering::Acquire)
    }

    pub(crate) fn set_in_blocked(&self, value: bool) {
        self.in_blocked.store(value, Ordering::Release);
    }

    #[allow(dead_code)]
    pub(crate) fn need_check_reschedule(&self) -> bool {
        self.need_check_reschedule.load(Ordering::Acquire)
    }

    pub(crate) fn set_need_check_reschedule(&self, value: bool) {
        self.need_check_reschedule.store(value, Ordering::Release);
    }

    pub(crate) fn try_mark_source_observer_registered(&self) -> bool {
        self.try_mark_observer(OBSERVER_SOURCE)
    }

    pub(crate) fn try_mark_sink_observer_registered(&self) -> bool {
        self.try_mark_observer(OBSERVER_SINK)
    }

    fn try_mark_observer(&self, mask: u8) -> bool {
        let mut current = self.observer_mask.load(Ordering::Acquire);
        loop {
            if (current & mask) != 0 {
                return false;
            }
            let next = current | mask;
            match self.observer_mask.compare_exchange(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }

    pub(crate) fn acquire_schedule_token(self: &Arc<Self>) -> ScheduleToken {
        let acquired = self
            .schedule_token
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok();
        ScheduleToken {
            state: Arc::clone(self),
            acquired,
        }
    }
}

/// Token passed through scheduler paths to preserve driver scheduling ownership.
pub(crate) struct ScheduleToken {
    state: Arc<DriverScheduleState>,
    acquired: bool,
}

impl ScheduleToken {
    pub(crate) fn acquired(&self) -> bool {
        self.acquired
    }
}

impl Drop for ScheduleToken {
    fn drop(&mut self) {
        if self.acquired {
            self.state.schedule_token.store(false, Ordering::Release);
        }
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
    operator_counters: Vec<OperatorCounters>,
    runtime_state: Arc<RuntimeState>,
    fragment_instance_id: Option<(i64, i64)>,
    state: DriverState,
    closed: bool,
    schedule_state: Arc<DriverScheduleState>,
    pending_finish_state: Option<DriverState>,

    edge_chunks: Vec<Option<Chunk>>,
    edge_closed: Vec<bool>,
    operator_finishing_set: Vec<bool>,
    operator_mem_trackers: Vec<Option<Arc<MemTracker>>>,
    edge_mem_trackers: Vec<Option<Arc<MemTracker>>>,
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
    pub fn new(
        driver_id: i32,
        operators: Vec<Box<dyn Operator>>,
        profiler: Option<Profiler>,
        operator_profiles: Vec<OperatorProfiles>,
        runtime_state: Arc<RuntimeState>,
        fragment_instance_id: Option<(i64, i64)>,
    ) -> Self {
        let mut operators = operators;
        let operator_count = operators.len();
        let edge_count = operator_count.saturating_sub(1);
        let driver_total_time = profiler.as_ref().map(|p| p.add_timer("DriverTotalTime"));
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
                        push_row_num: p.common.add_counter("PushRowNum", metrics::TUnit::UNIT),
                        pull_row_num: p.common.add_counter("PullRowNum", metrics::TUnit::UNIT),
                        mem_peak: p
                            .common
                            .add_counter("OperatorPeakMemoryUsage", metrics::TUnit::BYTES),
                        mem_allocated: p
                            .common
                            .add_counter("OperatorAllocatedMemoryUsage", metrics::TUnit::BYTES),
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
        let operator_mem_trackers = if let Some(root) = mem_root.as_ref() {
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
        };
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
            if let Some(tracker) = operator_mem_trackers.get(idx).and_then(|v| v.as_ref()) {
                op.set_mem_tracker(Arc::clone(tracker));
            }
        }
        Self {
            driver_id,
            operators,
            profiler,
            driver_total_time,
            operator_counters,
            runtime_state,
            fragment_instance_id,
            state: DriverState::Ready,
            closed: false,
            schedule_state: Arc::new(DriverScheduleState::new()),
            pending_finish_state: None,

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
        crate::novarocks_logging::info!(
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

    pub(crate) fn schedule_state(&self) -> Arc<DriverScheduleState> {
        Arc::clone(&self.schedule_state)
    }

    pub(crate) fn try_mark_source_observer_registered(&self) -> bool {
        self.schedule_state.try_mark_source_observer_registered()
    }

    pub(crate) fn try_mark_sink_observer_registered(&self) -> bool {
        self.schedule_state.try_mark_sink_observer_registered()
    }

    pub(crate) fn set_in_blocked(&self, value: bool) {
        self.schedule_state.set_in_blocked(value);
    }

    pub(crate) fn set_need_check_reschedule(&self, value: bool) {
        self.schedule_state.set_need_check_reschedule(value);
    }

    pub(crate) fn has_pending_finish(&self) -> bool {
        self.operators.iter().any(|op| op.pending_finish())
    }

    fn cancel_operators(&mut self) {
        for op in self.operators.iter_mut() {
            op.cancel();
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
        if let Some(final_state) = self.pending_finish_state.clone() {
            if self.has_pending_finish() {
                self.state = DriverState::PendingFinish;
                return self.state.clone();
            }
            self.pending_finish_state = None;
            return self.finish_with_state(final_state);
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

            if self.is_finished() {
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

            // If there is no buffered data, let source readiness decide first to avoid
            // blocking on a full sink when the driver has nothing to push.
            let has_buffered = self.edge_chunks.iter().any(|c| c.is_some());

            if !has_buffered {
                if let Some(source) = self.operators.first() {
                    if !source.is_finished() {
                        let Some(proc) = source.as_processor_ref() else {
                            return self.finish_with_state(DriverState::Failed(
                                "pipeline source missing processor operator".to_string(),
                            ));
                        };
                        if !proc.has_output() {
                            return self.block_or_fail(BlockedReason::InputEmpty);
                        }
                    }
                }
            }

            if let Some(sink) = self.operators.last() {
                if !sink.is_finished() {
                    let Some(proc) = sink.as_processor_ref() else {
                        return self.finish_with_state(DriverState::Failed(
                            "pipeline sink missing processor operator".to_string(),
                        ));
                    };
                    if !proc.need_input() {
                        return self.block_or_fail(BlockedReason::OutputFull);
                    }
                }
            }

            if has_buffered {
                if let Some(source) = self.operators.first() {
                    if !source.is_finished() {
                        let Some(proc) = source.as_processor_ref() else {
                            return self.finish_with_state(DriverState::Failed(
                                "pipeline source missing processor operator".to_string(),
                            ));
                        };
                        if !proc.has_output() {
                            return self.block_or_fail(BlockedReason::InputEmpty);
                        }
                    }
                }
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
            let proc = op.as_processor_ref()?;
            let dep = proc.precondition_dependency()?;
            if dep.is_ready() {
                continue;
            }
            return Some(dep);
        }
        None
    }

    pub(crate) fn source_observable(&self) -> Option<Arc<Observable>> {
        let op = self.operators.first()?;
        let proc = op.as_processor_ref()?;
        proc.source_observable()
    }

    pub(crate) fn sink_observable(&self) -> Option<Arc<Observable>> {
        let op = self.operators.last()?;
        let proc = op.as_processor_ref()?;
        proc.sink_observable()
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

    pub(crate) fn source_ready(&self) -> bool {
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

    pub(crate) fn sink_ready(&self) -> bool {
        let Some(op) = self.operators.last() else {
            return true;
        };
        if op.is_finished() {
            return true;
        }
        let Some(proc) = op.as_processor_ref() else {
            return true;
        };
        if proc.need_input() {
            return true;
        }
        // need_input can flip to finished (e.g. when finishing drains); re-check finished.
        op.is_finished()
    }

    pub(crate) fn check_is_ready(&self) -> bool {
        match &self.state {
            DriverState::Blocked(reason) => match reason {
                BlockedReason::InputEmpty => {
                    self.source_ready() || self.is_finished() || self.has_ready_finishing_work()
                }
                BlockedReason::OutputFull => {
                    self.sink_ready() || self.is_finished() || self.has_ready_finishing_work()
                }
                BlockedReason::Dependency(dep) => dep.is_ready(),
            },
            DriverState::PendingFinish => !self.has_pending_finish(),
            DriverState::Ready | DriverState::Running => true,
            DriverState::Finished | DriverState::Canceled | DriverState::Failed(_) => true,
        }
    }

    fn has_ready_finishing_work(&self) -> bool {
        if self.operators.len() < 2 {
            return false;
        }
        for idx in 1..self.operators.len() {
            if self.operator_finishing_set[idx] {
                continue;
            }
            let in_edge = idx - 1;
            if in_edge >= self.edge_closed.len() {
                continue;
            }
            if self.edge_closed[in_edge] && self.edge_chunks[in_edge].is_none() {
                return true;
            }
        }
        false
    }

    pub(crate) fn set_ready(&mut self) {
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
        fe_report::report_exec_state(UniqueId { hi, lo });
    }

    fn block_or_fail(&mut self, reason: BlockedReason) -> DriverState {
        match reason {
            BlockedReason::Dependency(dep) => {
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
                self.state = DriverState::Blocked(BlockedReason::InputEmpty);
                self.state.clone()
            }
            BlockedReason::OutputFull => {
                self.state = DriverState::Blocked(BlockedReason::OutputFull);
                self.state.clone()
            }
        }
    }

    fn finish_with_state(&mut self, state: DriverState) -> DriverState {
        if matches!(state, DriverState::Canceled | DriverState::Failed(_)) {
            self.cancel_operators();
        }
        if matches!(
            state,
            DriverState::Finished | DriverState::Canceled | DriverState::Failed(_)
        ) && self.pending_finish_state.is_none()
            && self.has_pending_finish()
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
            let (downstream_name, need_input) = {
                let Some(downstream_op) = self.operators.get_mut(downstream_idx) else {
                    return Err("pipeline operator index out of bounds".to_string());
                };
                let downstream_name = downstream_op.name().to_string();
                let downstream = downstream_op.as_processor_mut().ok_or_else(|| {
                    format!(
                        "pipeline operator {} missing processor operator",
                        downstream_name
                    )
                })?;
                (downstream_name, downstream.need_input())
            };
            if !need_input {
                continue;
            }
            let mut chunk = self.edge_chunks[e].take().expect("checked is_some");
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
                    return Err(err);
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
                    return Err(err);
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
                return Err(err);
            }
            debug!(
                "Driver set_finishing: driver_id={} op_idx={} op_name={} success. edge_closed[{}]={}",
                self.driver_id, idx, op_name, in_edge, self.edge_closed[in_edge]
            );
            self.operator_finishing_set[idx] = true;
            *made_progress = true;
        }
        Ok(())
    }
}

impl Drop for PipelineDriver {
    fn drop(&mut self) {
        self.close_operators();
    }
}
