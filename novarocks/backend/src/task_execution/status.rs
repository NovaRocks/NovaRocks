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

//! One task's status owner: the single serializer of its lifecycle.
//!
//! Every published version binds one complete immutable snapshot, frozen
//! here. Nothing re-renders an already published version from live metrics,
//! and a terminal snapshot is first-wins and never replaced.
//!
//! The split between [`TaskStatusReporter`] and [`TaskMetricsSink`] is the
//! point of this module's shape. Metrics are produced by whatever counts rows
//! and bytes, on whatever thread; lifecycle transitions are decided by the
//! task's owner. Handing out a sink that has no lifecycle method at all makes
//! "a metrics producer must not mutate lifecycle state" a property of the
//! type, not of a review comment.

use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use novarocks_execution_contract::task_execution::domain::{CodecOwnedContent, DomainVersion};
use novarocks_execution_contract::task_execution::identity::TaskIdentity;
use novarocks_execution_contract::task_execution::status::{
    AbortCause, CancelReason, DynamicFilterAdvertisement, FINAL_TASK_INFO_MAX_OPERATORS,
    FinalTaskInfo, OperatorStatistics, TaskFailure, TaskOutputFacts, TaskResourceFacts, TaskState,
    TaskStatus, TaskStatusError, TaskStatusVersion, TaskWriterFacts, TerminationDetail,
};
use novarocks_types::UniqueId;
use novarocks_worker::{
    MonotonicInstant, RootDrainAction, TaskTransition, classify_root_drain,
    classify_task_transition,
};

use super::clock::BackendMonotonicClock;
use super::host::TaskDynamicFilterRead;
use super::observation::TaskStatusSource;

/// The shortest interval between two metric-only status versions.
///
/// Lifecycle, terminal, failure, output-completion, and dynamic filter
/// advertisement changes ignore it and publish immediately; only a
/// republication that carries nothing but new counters is throttled. A quarter
/// second is short enough that a stuck task is visible well inside any
/// operator's patience and long enough that a task reporting per chunk cannot
/// turn the observation channel into a metrics firehose.
pub const METRIC_PUBLISH_MIN_INTERVAL: Duration = Duration::from_millis(250);

/// How one proposed status change was answered.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StatusAdvance {
    /// A lifecycle change published a new version.
    Published(TaskStatusVersion),
    /// The same state republished at a new version with fresh metrics.
    Republished(TaskStatusVersion),
    /// A metric-only change inside the throttle interval. It is retained and
    /// folded into the next publication.
    Throttled,
    /// The task is already terminal. The first terminal wins.
    AlreadyTerminal(TaskState),
    /// The move is not in the state machine.
    Illegal { from: TaskState, to: TaskState },
    /// The proposed snapshot is not a legal value.
    Rejected(TaskStatusError),
    /// This task published `u64::MAX` versions.
    VersionExhausted,
}

impl StatusAdvance {
    pub const fn published_version(&self) -> Option<TaskStatusVersion> {
        match self {
            Self::Published(version) | Self::Republished(version) => Some(*version),
            _ => None,
        }
    }
}

#[derive(Debug)]
struct OwnedStatus {
    current: TaskStatus,
    output: TaskOutputFacts,
    resources: TaskResourceFacts,
    writer: Option<TaskWriterFacts>,
    filters: Option<DynamicFilterAdvertisement>,
    filter_payload: Option<Arc<dyn CodecOwnedContent>>,
    operator_statistics: Vec<OperatorStatistics>,
    operator_statistics_truncated: bool,
    final_info: Option<FinalTaskInfo>,
    last_publication: MonotonicInstant,
    metrics_pending: bool,
    output_released: bool,
    /// Snapshots stay here until the creation transaction commits, so a
    /// creation that rolls back never leaves an observable task behind.
    released_to_observers: bool,
    buffered: Option<TaskStatus>,
}

/// The serializer of one task's status.
pub struct TaskStatusOwner {
    identity: TaskIdentity,
    source: Arc<TaskStatusSource>,
    clock: Arc<dyn BackendMonotonicClock>,
    throttle: Duration,
    state: Mutex<OwnedStatus>,
}

impl fmt::Debug for TaskStatusOwner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TaskStatusOwner")
            .field("identity", &self.identity)
            .finish_non_exhaustive()
    }
}

impl TaskStatusOwner {
    /// Freezes the first snapshot of a task under creation.
    ///
    /// The snapshot is version one, which is what the creation acknowledgement
    /// carries, but it is not visible to an observer until
    /// [`Self::release_to_observers`].
    pub fn new(
        identity: TaskIdentity,
        source: Arc<TaskStatusSource>,
        clock: Arc<dyn BackendMonotonicClock>,
        throttle: Duration,
    ) -> Self {
        let current = TaskStatus::created(identity);
        let now = clock.now();
        Self {
            identity,
            source,
            clock,
            throttle,
            state: Mutex::new(OwnedStatus {
                current: current.clone(),
                output: TaskOutputFacts::default(),
                resources: TaskResourceFacts::empty(),
                writer: None,
                filters: None,
                filter_payload: None,
                operator_statistics: Vec::new(),
                operator_statistics_truncated: false,
                final_info: None,
                last_publication: now,
                metrics_pending: false,
                output_released: false,
                released_to_observers: false,
                buffered: Some(current),
            }),
        }
    }

    /// Makes this task's snapshots observable. Called once, at the exact
    /// linearization point of its creation acknowledgement.
    pub fn release_to_observers(&self) {
        let mut state = self.state.lock().expect("task status lock");
        state.released_to_observers = true;
        if let Some(status) = state.buffered.take() {
            self.source.publish(status);
        }
    }

    pub const fn identity(&self) -> TaskIdentity {
        self.identity
    }

    pub fn current(&self) -> TaskStatus {
        self.state.lock().expect("task status lock").current.clone()
    }

    pub fn state(&self) -> TaskState {
        self.state.lock().expect("task status lock").current.state()
    }

    pub fn is_terminal(&self) -> bool {
        self.state
            .lock()
            .expect("task status lock")
            .current
            .is_terminal()
    }

    pub fn final_info(&self) -> Option<FinalTaskInfo> {
        self.state
            .lock()
            .expect("task status lock")
            .final_info
            .clone()
    }

    /// Whether this task's output responsibility has drained.
    ///
    /// It is a fact of its own, separate from the terminal state: a task can
    /// be `ABORTED` while its buffers are still being released, and a release
    /// must not linearize until they are.
    pub fn output_released(&self) -> bool {
        self.state.lock().expect("task status lock").output_released
    }

    pub fn dynamic_filters(&self) -> Option<TaskDynamicFilterRead> {
        let state = self.state.lock().expect("task status lock");
        let version = state.filters.map(DynamicFilterAdvertisement::version)?;
        let payload = state.filter_payload.as_ref()?;
        Some(TaskDynamicFilterRead::new(version, Arc::clone(payload)))
    }

    /// Applies one proposed lifecycle change.
    pub fn advance(
        &self,
        to: TaskState,
        termination: Option<TerminationDetail>,
        output: TaskOutputFacts,
    ) -> StatusAdvance {
        let now = self.clock.now();
        let mut state = self.state.lock().expect("task status lock");
        let from = state.current.state();
        if from.is_terminal() {
            return StatusAdvance::AlreadyTerminal(from);
        }
        match classify_task_transition(from, to) {
            TaskTransition::AlreadyTerminal => StatusAdvance::AlreadyTerminal(from),
            TaskTransition::Illegal => StatusAdvance::Illegal { from, to },
            TaskTransition::SameState => {
                state.output = output;
                self.republish_locked(&mut state, now, false)
            }
            TaskTransition::Apply => {
                let Some(version) = state.current.version().next() else {
                    return StatusAdvance::VersionExhausted;
                };
                state.output = output;
                let next = match self.compose(&state, version, to, termination) {
                    Ok(next) => next,
                    Err(error) => return StatusAdvance::Rejected(error),
                };
                if next.is_terminal() {
                    state.final_info = FinalTaskInfo::try_new(
                        self.identity,
                        next.clone(),
                        state.operator_statistics.clone(),
                        state.operator_statistics_truncated,
                    )
                    .ok();
                }
                self.commit_locked(&mut state, next, now);
                StatusAdvance::Published(version)
            }
        }
    }

    /// Records new metrics, publishing only when the throttle allows.
    fn report_metrics(
        &self,
        resources: TaskResourceFacts,
        writer: Option<TaskWriterFacts>,
    ) -> StatusAdvance {
        let now = self.clock.now();
        let mut state = self.state.lock().expect("task status lock");
        if state.current.is_terminal() {
            return StatusAdvance::AlreadyTerminal(state.current.state());
        }
        state.resources = resources;
        if writer.is_some() {
            state.writer = writer;
        }
        self.republish_locked(&mut state, now, false)
    }

    /// Publishes a retained metric-only change whose throttle interval has
    /// elapsed. Driven by the owner's deadline sweep so a throttled report is
    /// delayed, never lost.
    pub fn flush_throttled_metrics(&self) -> StatusAdvance {
        let now = self.clock.now();
        let mut state = self.state.lock().expect("task status lock");
        if !state.metrics_pending || state.current.is_terminal() {
            return StatusAdvance::Throttled;
        }
        self.republish_locked(&mut state, now, false)
    }

    /// Advertises a new dynamic filter version and retains its payload for a
    /// separate read.
    ///
    /// This publishes immediately: the advertisement is what tells a frontend
    /// there is something to fetch, so throttling it would throttle the fetch.
    pub fn advertise_dynamic_filters(
        &self,
        version: DomainVersion,
        domain_count: u32,
        payload: Arc<dyn CodecOwnedContent>,
    ) -> StatusAdvance {
        let now = self.clock.now();
        let mut state = self.state.lock().expect("task status lock");
        if state.current.is_terminal() {
            return StatusAdvance::AlreadyTerminal(state.current.state());
        }
        state.filters = Some(DynamicFilterAdvertisement::new(version, domain_count));
        state.filter_payload = Some(payload);
        self.republish_locked(&mut state, now, true)
    }

    /// Drives a task that ignored its stand-down request to a terminal
    /// status, and releases its output responsibility.
    ///
    /// Only the query context owner calls this, and only once the termination
    /// grace has elapsed: a task is asked to stand down first and given a
    /// bounded window to converge on its own outcome. Without this, one
    /// uncooperative task would pin a terminating context open forever, which
    /// is the shape of hang the whole lifecycle exists to avoid. A task that
    /// is already converging on its own failure keeps that failure as its
    /// cause; anything else is reported as forced.
    pub fn force_terminal(&self, cause: AbortCause) -> bool {
        let now = self.clock.now();
        let mut state = self.state.lock().expect("task status lock");
        if state.current.is_terminal() {
            return false;
        }
        let current = state.current.state();
        let steps: &[(TaskState, TerminationDetail)] = &if current.is_failure() {
            let Some(TerminationDetail::Failed(failure)) = state.current.termination().cloned()
            else {
                return false;
            };
            [(TaskState::Failed, TerminationDetail::Failed(failure))]
        } else {
            [(TaskState::Aborted, TerminationDetail::Aborted(cause))]
        };
        if !current.is_failure() && !current.is_abort() {
            // `ABORTING` is the only legal way into `ABORTED` from a state
            // that has not started terminating.
            let Some(version) = state.current.version().next() else {
                return false;
            };
            match self.compose(
                &state,
                version,
                TaskState::Aborting,
                Some(TerminationDetail::Aborted(cause)),
            ) {
                Ok(next) => self.commit_locked(&mut state, next, now),
                Err(_) => return false,
            }
        }
        for (task_state, detail) in steps {
            let Some(version) = state.current.version().next() else {
                return false;
            };
            let Ok(next) = self.compose(&state, version, *task_state, Some(detail.clone())) else {
                return false;
            };
            if next.is_terminal() {
                state.final_info = FinalTaskInfo::try_new(
                    self.identity,
                    next.clone(),
                    state.operator_statistics.clone(),
                    state.operator_statistics_truncated,
                )
                .ok();
            }
            self.commit_locked(&mut state, next, now);
        }
        state.output_released = true;
        self.source.note_progress();
        true
    }

    /// Records that the root result stream reached end of stream.
    ///
    /// This is the one moment a root task's output responsibility becomes
    /// complete: the pipeline closed the buffer, and the frontend has now
    /// consumed everything in it including the end-of-stream marker. Until it
    /// happens the root stays `FLUSHING`, because a drained pipeline that
    /// still owes packets has not finished its job.
    ///
    /// It publishes immediately rather than under the metric throttle: this is
    /// the fact the coordinator's completion is waiting for. It cannot turn a
    /// termination into a success — a task already standing down only records
    /// that its output drained.
    pub fn note_root_result_drained(&self) -> StatusAdvance {
        let now = self.clock.now();
        let mut state = self.state.lock().expect("task status lock");
        let from = state.current.state();
        let advance = match classify_root_drain(from) {
            RootDrainAction::AlreadyTerminal => return StatusAdvance::AlreadyTerminal(from),
            RootDrainAction::Illegal => {
                return StatusAdvance::Illegal {
                    from,
                    to: TaskState::Finished,
                };
            }
            RootDrainAction::RecordOnly => {
                // The buffered counts are deliberately left unreported rather
                // than set to zero: absence means "not reported", and this
                // path knows the stream drained, not what the task's other
                // output did.
                state.output = TaskOutputFacts::new(true);
                self.republish_locked(&mut state, now, true)
            }
            RootDrainAction::Finish => {
                let Some(version) = state.current.version().next() else {
                    return StatusAdvance::VersionExhausted;
                };
                state.output = TaskOutputFacts::new(true);
                let next = match self.compose(&state, version, TaskState::Finished, None) {
                    Ok(next) => next,
                    Err(error) => return StatusAdvance::Rejected(error),
                };
                state.final_info = FinalTaskInfo::try_new(
                    self.identity,
                    next.clone(),
                    state.operator_statistics.clone(),
                    state.operator_statistics_truncated,
                )
                .ok();
                self.commit_locked(&mut state, next, now);
                StatusAdvance::Published(version)
            }
        };
        state.output_released = true;
        drop(state);
        // No snapshot carries the release fact, so the owner is told directly.
        self.source.note_progress();
        advance
    }

    fn record_operator_statistics(&self, statistics: Vec<OperatorStatistics>) {
        let mut state = self.state.lock().expect("task status lock");
        let mut statistics = statistics;
        if statistics.len() > FINAL_TASK_INFO_MAX_OPERATORS {
            statistics.truncate(FINAL_TASK_INFO_MAX_OPERATORS);
            state.operator_statistics_truncated = true;
        }
        state.operator_statistics = statistics;
    }

    fn release_output(&self) {
        self.state.lock().expect("task status lock").output_released = true;
        // No snapshot carries this fact, so the owner is told directly.
        self.source.note_progress();
    }

    fn compose(
        &self,
        state: &OwnedStatus,
        version: TaskStatusVersion,
        task_state: TaskState,
        termination: Option<TerminationDetail>,
    ) -> Result<TaskStatus, TaskStatusError> {
        let mut status = TaskStatus::try_new(
            self.identity,
            version,
            task_state,
            termination,
            state.output,
        )?
        .with_resources(state.resources);
        if let Some(filters) = state.filters {
            status = status.with_dynamic_filters(filters);
        }
        if let Some(writer) = state.writer {
            status = status.with_writer(writer);
        }
        Ok(status)
    }

    /// Republishes the current state at a new version.
    ///
    /// `immediate` skips the throttle for a change that is not metric-only.
    fn republish_locked(
        &self,
        state: &mut OwnedStatus,
        now: MonotonicInstant,
        immediate: bool,
    ) -> StatusAdvance {
        if !immediate && now.saturating_duration_since(state.last_publication) < self.throttle {
            state.metrics_pending = true;
            return StatusAdvance::Throttled;
        }
        let Some(version) = state.current.version().next() else {
            return StatusAdvance::VersionExhausted;
        };
        let task_state = state.current.state();
        let termination = state.current.termination().cloned();
        let next = match self.compose(state, version, task_state, termination) {
            Ok(next) => next,
            Err(error) => return StatusAdvance::Rejected(error),
        };
        self.commit_locked(state, next, now);
        StatusAdvance::Republished(version)
    }

    fn commit_locked(&self, state: &mut OwnedStatus, next: TaskStatus, now: MonotonicInstant) {
        state.current = next.clone();
        state.metrics_pending = false;
        state.last_publication = now;
        if state.released_to_observers {
            self.source.publish(next);
            return;
        }
        // Pre-commit snapshots coalesce exactly the way the source's delivery
        // slot does: an older non-current snapshot no observer has seen may be
        // dropped, and a terminal one may not be superseded.
        let queued_terminal = state.buffered.as_ref().is_some_and(TaskStatus::is_terminal);
        if !queued_terminal {
            state.buffered = Some(next);
        }
    }
}

/// The lifecycle-writing handle of one task, held by its execution owner.
#[derive(Clone, Debug)]
pub struct TaskStatusReporter {
    owner: Arc<TaskStatusOwner>,
}

impl TaskStatusReporter {
    pub const fn new(owner: Arc<TaskStatusOwner>) -> Self {
        Self { owner }
    }

    pub fn identity(&self) -> TaskIdentity {
        self.owner.identity()
    }

    pub fn current(&self) -> TaskStatus {
        self.owner.current()
    }

    pub fn running(&self) -> StatusAdvance {
        self.owner
            .advance(TaskState::Running, None, TaskOutputFacts::default())
    }

    pub fn flushing(&self) -> StatusAdvance {
        self.owner
            .advance(TaskState::Flushing, None, TaskOutputFacts::default())
    }

    /// Finishes the task. `FINISHED` is only constructible with a complete
    /// output responsibility, so this cannot report success for a task that
    /// still owes output.
    pub fn finished(&self, output: TaskOutputFacts) -> StatusAdvance {
        self.owner.advance(TaskState::Finished, None, output)
    }

    pub fn canceling(&self, reason: CancelReason) -> StatusAdvance {
        self.owner.advance(
            TaskState::Canceling,
            Some(TerminationDetail::Canceled(reason)),
            TaskOutputFacts::default(),
        )
    }

    pub fn canceled(&self, reason: CancelReason) -> StatusAdvance {
        self.canceled_with_output(reason, TaskOutputFacts::default())
    }

    /// Completes a normal stand-down while preserving whether the fragment
    /// had already satisfied its output responsibility.
    ///
    /// The terminal remains `CANCELED` because the stand-down won the
    /// lifecycle race. The output fact is independent evidence used by
    /// consumers, such as the distributed-write commit gate, that must tell a
    /// completed sink from one that stopped before publishing all of its
    /// output.
    pub fn canceled_with_output(
        &self,
        reason: CancelReason,
        output: TaskOutputFacts,
    ) -> StatusAdvance {
        self.owner.advance(
            TaskState::Canceled,
            Some(TerminationDetail::Canceled(reason)),
            output,
        )
    }

    pub fn aborting(&self, cause: AbortCause) -> StatusAdvance {
        self.owner.advance(
            TaskState::Aborting,
            Some(TerminationDetail::Aborted(cause)),
            TaskOutputFacts::default(),
        )
    }

    pub fn aborted(&self, cause: AbortCause) -> StatusAdvance {
        self.owner.advance(
            TaskState::Aborted,
            Some(TerminationDetail::Aborted(cause)),
            TaskOutputFacts::default(),
        )
    }

    pub fn failing(&self, failure: TaskFailure) -> StatusAdvance {
        self.owner.advance(
            TaskState::Failing,
            Some(TerminationDetail::Failed(failure)),
            TaskOutputFacts::default(),
        )
    }

    pub fn failed(&self, failure: TaskFailure) -> StatusAdvance {
        self.owner.advance(
            TaskState::Failed,
            Some(TerminationDetail::Failed(failure)),
            TaskOutputFacts::default(),
        )
    }

    pub fn advertise_dynamic_filters(
        &self,
        version: DomainVersion,
        domain_count: u32,
        payload: Arc<dyn CodecOwnedContent>,
    ) -> StatusAdvance {
        self.owner
            .advertise_dynamic_filters(version, domain_count, payload)
    }

    pub fn record_operator_statistics(&self, statistics: Vec<OperatorStatistics>) {
        self.owner.record_operator_statistics(statistics);
    }

    /// Reports that this task's output buffers have drained.
    pub fn release_output(&self) {
        self.owner.release_output();
    }

    /// The metric-only handle. It has no lifecycle method at all.
    pub fn metrics(&self) -> TaskMetricsSink {
        TaskMetricsSink {
            owner: Arc::clone(&self.owner),
        }
    }
}

/// The metrics-writing handle of one task.
///
/// It can raise counters and nothing else: it cannot change a state, latch a
/// terminal, freeze a final info, or release output responsibility.
#[derive(Clone, Debug)]
pub struct TaskMetricsSink {
    owner: Arc<TaskStatusOwner>,
}

impl TaskMetricsSink {
    pub fn report(
        &self,
        resources: TaskResourceFacts,
        writer: Option<TaskWriterFacts>,
    ) -> StatusAdvance {
        self.owner.report_metrics(resources, writer)
    }
}

/// The result-plane binding of one live root task.
///
/// It pairs the execution kernel's buffer key with the status owner that must
/// hear about the drain, so a result poll cannot reach a buffer without also
/// being able to report what reaching its end means.
#[derive(Clone, Debug)]
pub struct RootResultBinding {
    kernel_key: UniqueId,
    status: Arc<TaskStatusOwner>,
}

impl RootResultBinding {
    pub const fn new(kernel_key: UniqueId, status: Arc<TaskStatusOwner>) -> Self {
        Self { kernel_key, status }
    }

    /// The execution kernel's key for this task's result buffer.
    pub const fn kernel_key(&self) -> UniqueId {
        self.kernel_key
    }

    pub fn identity(&self) -> TaskIdentity {
        self.status.identity()
    }

    /// Reports that this poll delivered the end-of-stream marker.
    pub fn note_result_stream_drained(&self) -> StatusAdvance {
        self.status.note_root_result_drained()
    }
}

/// Whether a root result poll may be served, and why not when it may not.
///
/// Owning a result buffer and owing the coordinator a result are two different
/// facts. Every task has a kernel key; only the task whose sink is the query's
/// result sink owes a result. A poll aimed at any other task is refused rather
/// than answered out of that task's buffer, which is what stops a mistaken or
/// forged identity from draining an exchange producer's output as if it were
/// the query's answer.
#[derive(Clone, Debug)]
pub enum RootResultRoute {
    /// This exact live task owns the query's client-visible result.
    Serve(RootResultBinding),
    /// No task of this identity exists on this exact backend process.
    UnknownTask,
    /// A live task, but its descriptor's sink is not the query's result sink.
    NotResultOwner,
    /// The creation transaction has not committed, so no buffer exists yet.
    Creating,
    /// The task already reached its terminal; its result buffer is gone.
    Terminal(TaskState),
    /// The retained terminal record was reclaimed.
    Gone,
}

impl RootResultRoute {
    /// A bounded, secret-free explanation of a refusal, for the poll's own
    /// error field.
    pub fn refusal_detail(&self) -> Option<String> {
        match self {
            Self::Serve(_) => None,
            Self::UnknownTask => {
                Some("result poll names a task this backend process does not own".to_owned())
            }
            Self::NotResultOwner => {
                Some("result poll names a task that does not own this query's result".to_owned())
            }
            Self::Creating => {
                Some("result poll names a task whose creation has not committed".to_owned())
            }
            Self::Terminal(state) => {
                Some(format!("result poll names a task that is already {state}"))
            }
            Self::Gone => Some("result poll reached a reclaimed task record".to_owned()),
        }
    }
}
