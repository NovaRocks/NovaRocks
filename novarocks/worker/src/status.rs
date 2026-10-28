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

use crate::task_conclusion::{ConclusionAcceptance, PreparedTaskConclusion, TaskConclusionState};
use crate::task_diagnostics::{
    DiagnosticRecordOutcome, PreparedCleanupDiagnostic, TaskSecondaryDiagnostics,
    TaskSecondaryDiagnosticsSnapshot,
};
use novarocks_execution::runtime::{
    execution_health::ExecutionHealth,
    fragment::ExecutionFailure,
    observable::Observable,
    task_control::{TaskExecutionControl, TaskExecutionPort},
};
use novarocks_execution_contract::{
    AcceptedTaskConclusion, ConclusionDecision, TaskConclusion, TaskConclusionPort,
    TaskConclusionProposal, TaskFailurePhase,
};
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::Duration;

use crate::{
    MonotonicInstant, RootDrainAction, TaskConvergence, TaskConvergenceAdvance,
    TaskConvergenceRejection, TaskConvergenceSnapshot, TaskDynamicFilterRead, TaskTransition,
    WorkerMonotonicClock, classify_root_drain, classify_task_transition,
};
use novarocks_execution_contract::task_execution::domain::{CodecOwnedContent, DomainVersion};
use novarocks_execution_contract::task_execution::identity::TaskIdentity;
use novarocks_execution_contract::task_execution::status::{
    AbortCause, CancelReason, DynamicFilterAdvertisement, FINAL_TASK_INFO_MAX_OPERATORS,
    FinalTaskInfo, OperatorStatistics, TaskOutputFacts, TaskResourceFacts, TaskState, TaskStatus,
    TaskStatusError, TaskStatusVersion, TaskWriterFacts, TerminationDetail,
};
use novarocks_execution_contract::task_execution::task_convergence::{
    TaskConvergenceReceipt, TaskConvergenceVersion,
};
use novarocks_types::UniqueId;

use crate::observation::TaskStatusSource;

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
#[expect(
    clippy::large_enum_variant,
    reason = "This Worker transition result carries the exact typed TaskStatusError; boxing would change the public contract and allocate on rejection."
)]
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
    /// An accepted conclusion kept its cause; this later proposal lost.
    KeptConclusion(TaskState),
    /// The move is not in the state machine.
    Illegal { from: TaskState, to: TaskState },
    /// The proposed snapshot is not a legal value.
    Rejected(TaskStatusError),
    /// This task published `u64::MAX` versions.
    VersionExhausted,
    /// A neutral failure summary cannot supply an originating Task cause.
    MissingFailureSource,
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
    conclusions: TaskConclusionState,
    secondary_diagnostics: TaskSecondaryDiagnostics,
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
    convergence: TaskConvergence,
    // Original runtime receipts remain independent from whole-Task publication.
    // Started opaque disposal may finish normally; never discard an arrived
    // real receipt or infer it from a timeout, terminal status, or health.
    runtime_actual_stopped_received: bool,
    runtime_resources_converged_received: bool,
    /// Snapshots stay here until the creation transaction commits, so a
    /// creation that rolls back never leaves an observable task behind.
    released_to_observers: bool,
    buffered: Option<TaskStatus>,
    buffered_convergence: Option<TaskConvergenceReceipt>,
    // A root/control transaction can accept without waking its producers
    // until its own closed facts are installed. Every contributor uses this
    // same fence; accepting a concurrent refinement never waits for it.
    publication_fences: usize,
    deferred_conclusion_notify: bool,
    deferred_status_notify: bool,
}

/// One status transaction. The only buffered snapshot is still owned by the
/// Task acceptance lock. The guard installs that immutable snapshot while
/// locked, then unlocks before waking an observer. Retirement can never read a
/// terminal owner ahead of its installed snapshot, and callbacks can reenter.
/// Early returns obey the same publication boundary.
struct StatusTransaction<'a> {
    owner: &'a TaskStatusOwner,
    state: Option<MutexGuard<'a, OwnedStatus>>,
    starting_generation: Option<std::num::NonZeroU64>,
}
impl std::ops::Deref for StatusTransaction<'_> {
    type Target = OwnedStatus;
    fn deref(&self) -> &OwnedStatus {
        self.state.as_deref().expect("live status transaction")
    }
}
impl std::ops::DerefMut for StatusTransaction<'_> {
    fn deref_mut(&mut self) -> &mut OwnedStatus {
        self.state.as_deref_mut().expect("live status transaction")
    }
}
/// Installation precedes notification. The enclosing owner may retain this
/// value across its own fence and publish only after releasing that fence.
pub(crate) struct PendingStatusPublication<'a> {
    owner: &'a TaskStatusOwner,
    conclusion_changed: bool,
    status_changed: bool,
    convergence: Option<novarocks_execution_contract::TaskConvergenceReceipt>,
}
fn publish_status(
    owner: &TaskStatusOwner,
    conclusion_changed: bool,
    status_changed: bool,
    convergence: Option<novarocks_execution_contract::TaskConvergenceReceipt>,
) {
    // Install every deferred owner fact before synchronous lifecycle
    // callbacks can reenter retirement. Status snapshots were installed
    // under the acceptance transaction; convergence is readiness-independent.
    if let Some(receipt) = convergence {
        owner.source.publish_task_convergence(receipt);
    }
    if conclusion_changed {
        owner.conclusion_changed.notify_all();
        owner.stop_observable.notify_observers();
    }
    if status_changed {
        owner.source.notify_status_publication();
    }
}
impl PendingStatusPublication<'_> {
    pub(crate) fn publish(self) {
        publish_status(
            self.owner,
            self.conclusion_changed,
            self.status_changed,
            self.convergence,
        );
    }
}
impl<'a> StatusTransaction<'a> {
    fn detach_publication(&mut self) -> PendingStatusPublication<'a> {
        let mut state = self.state.take().expect("live status transaction");
        let status_changed = state.released_to_observers
            && state
                .buffered
                .take()
                .is_some_and(|status| self.owner.source.install_status(status));
        let conclusion_changed = state.conclusions.generation() != self.starting_generation;
        if conclusion_changed {
            self.owner.conclusion_present.store(true, Ordering::Release);
        }
        state.deferred_conclusion_notify |= conclusion_changed;
        state.deferred_status_notify |= status_changed;
        let (conclusion_changed, status_changed, convergence) = if state.publication_fences == 0 {
            (
                std::mem::take(&mut state.deferred_conclusion_notify),
                std::mem::take(&mut state.deferred_status_notify),
                if state.released_to_observers {
                    state.buffered_convergence.take()
                } else {
                    None
                },
            )
        } else {
            (false, false, None)
        };
        drop(state);
        PendingStatusPublication {
            owner: self.owner,
            conclusion_changed,
            status_changed,
            convergence,
        }
    }
}
impl Drop for StatusTransaction<'_> {
    fn drop(&mut self) {
        if self.state.is_some() {
            self.detach_publication().publish();
        }
    }
}

/// Both notifications and rejected source destruction belong outside every
/// enclosing lifecycle fence. This value never publishes from Drop: dropping
/// it while unwinding a fenced mutation must not invoke a reentrant callback.
#[must_use = "publish the accepted conclusion after releasing enclosing lifecycle fences"]
pub(crate) struct PendingTaskConclusion<'a> {
    accepted: ConclusionAcceptance<ExecutionFailure>,
    discarded: Option<PreparedTaskConclusion>,
    publication: PendingStatusPublication<'a>,
}
impl PendingTaskConclusion<'_> {
    pub(crate) fn publish(self) -> ConclusionAcceptance<ExecutionFailure> {
        let Self {
            accepted,
            discarded,
            publication,
        } = self;
        publication.publish();
        drop(discarded);
        accepted
    }
}

/// An owned publication can leave the registry fence without borrowing its
/// task map. It holds no payload and makes no lifecycle decision of its own.
#[must_use = "publish after the registry/root closed facts are installed and unlocked"]
pub(crate) struct OwnedTaskConclusion {
    owner: Arc<TaskStatusOwner>,
    accepted: ConclusionAcceptance<ExecutionFailure>,
    discarded: Option<PreparedTaskConclusion>,
}
impl OwnedTaskConclusion {
    pub(crate) fn acceptance(&self) -> &ConclusionAcceptance<ExecutionFailure> {
        &self.accepted
    }
    pub(crate) fn publish(self) -> ConclusionAcceptance<ExecutionFailure> {
        let Self {
            owner,
            accepted,
            discarded,
        } = self;
        {
            let mut state = owner.transaction();
            state.publication_fences = state
                .publication_fences
                .checked_sub(1)
                .expect("owned Task publication releases its live fence exactly once");
        }
        // The last enclosing fence flushes all contributors' notifications
        // through StatusTransaction, after dropping the Task acceptance lock.
        drop(discarded);
        accepted
    }
}

/// The serializer of one task's status.
pub struct TaskStatusOwner {
    identity: TaskIdentity,
    source: Arc<TaskStatusSource>,
    clock: Arc<dyn WorkerMonotonicClock>,
    throttle: Duration,
    execution_health: Arc<ExecutionHealth>,
    preparation: Arc<crate::task_preparation::TaskPreparationScope>,
    state: Mutex<OwnedStatus>,
    conclusion_present: AtomicBool,
    conclusion_changed: Condvar,
    stop_observable: Arc<Observable>,
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
        clock: Arc<dyn WorkerMonotonicClock>,
        throttle: Duration,
        execution_health: Arc<ExecutionHealth>,
    ) -> Self {
        let preparation = Arc::new(crate::task_preparation::TaskPreparationScope::new(
            identity,
            Arc::clone(&execution_health),
        ));
        Self::from_preparation_scope(
            identity,
            source,
            clock,
            throttle,
            execution_health,
            preparation,
        )
    }

    pub(crate) fn from_preparation_scope(
        identity: TaskIdentity,
        source: Arc<TaskStatusSource>,
        clock: Arc<dyn WorkerMonotonicClock>,
        throttle: Duration,
        execution_health: Arc<ExecutionHealth>,
        preparation: Arc<crate::task_preparation::TaskPreparationScope>,
    ) -> Self {
        assert_eq!(
            preparation.identity(),
            identity,
            "one preparation reservation identity"
        );
        let current = TaskStatus::created(identity);
        let now = clock.now();
        Self {
            identity,
            source,
            clock,
            throttle,
            execution_health,
            preparation,
            state: Mutex::new(OwnedStatus {
                current: current.clone(),
                conclusions: TaskConclusionState::default(),
                secondary_diagnostics: TaskSecondaryDiagnostics::default(),
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
                convergence: TaskConvergence::new(),
                runtime_actual_stopped_received: false,
                runtime_resources_converged_received: false,
                released_to_observers: false,
                buffered: Some(current),
                buffered_convergence: None,
                publication_fences: 0,
                deferred_conclusion_notify: false,
                deferred_status_notify: false,
            }),
            conclusion_present: AtomicBool::new(false),
            conclusion_changed: Condvar::new(),
            stop_observable: Arc::new(Observable::new()),
        }
    }

    pub(crate) fn preparation_scope(&self) -> &crate::task_preparation::TaskPreparationScope {
        &self.preparation
    }
    pub(crate) fn preparation_scope_arc(
        &self,
    ) -> Arc<crate::task_preparation::TaskPreparationScope> {
        Arc::clone(&self.preparation)
    }

    /// Makes this task's snapshots observable. Called once, at the exact
    /// linearization point of its creation acknowledgement.
    pub fn release_to_observers(&self) {
        self.transaction().released_to_observers = true;
    }

    fn transaction(&self) -> StatusTransaction<'_> {
        let state = self.state.lock().expect("task status lock");
        let starting_generation = state.conclusions.generation();
        StatusTransaction {
            owner: self,
            state: Some(state),
            starting_generation,
        }
    }

    /// A phase-scoped view of this exact acceptance owner. No new latch is
    /// introduced, and neither a RuntimeState nor result payload is captured.
    pub fn execution_control(
        self: &Arc<Self>,
        phase: TaskFailurePhase,
    ) -> Arc<TaskExecutionControl> {
        Arc::new(
            TaskExecutionControl::new(self.identity, self.clone(), phase)
                .expect("Task control binds its exact status owner"),
        )
    }

    pub const fn identity(&self) -> TaskIdentity {
        self.identity
    }

    pub fn current(&self) -> TaskStatus {
        self.state.lock().expect("task status lock").current.clone()
    }

    /// Publishes the irreversible installation fact at the Context commit.
    /// This control fact bypasses metric throttling and remains present in
    /// every later snapshot, including terminal snapshots.
    pub fn note_installed(&self) -> StatusAdvance {
        let (result, publication) = self.prepare_installed();
        publication.publish();
        result
    }
    pub(crate) fn prepare_installed(&self) -> (StatusAdvance, PendingStatusPublication<'_>) {
        let now = self.clock.now();
        let mut state = self.transaction();
        let result = self.note_installed_locked(&mut state, now);
        let publication = state.detach_publication();
        (result, publication)
    }
    fn note_installed_locked(
        &self,
        state: &mut OwnedStatus,
        now: MonotonicInstant,
    ) -> StatusAdvance {
        if state.current.installed() {
            return StatusAdvance::Published(state.current.version());
        }
        if state.current.is_terminal() {
            return StatusAdvance::AlreadyTerminal(state.current.state());
        }
        // The Context caller's earlier stop observation is not an installation
        // fence. Serialize this irreversible fact with the accepted conclusion.
        if state.conclusions.current().is_some() {
            return StatusAdvance::KeptConclusion(state.current.state());
        }
        let Some(version) = state.current.version().next() else {
            return StatusAdvance::VersionExhausted;
        };
        let next = match self.compose(
            state,
            version,
            state.current.state(),
            state.current.termination().cloned(),
        ) {
            Ok(next) => next.with_installed(),
            Err(error) => return StatusAdvance::Rejected(error),
        };
        self.commit_locked(state, next, now);
        StatusAdvance::Published(version)
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
        self.convergence().output_released()
    }

    /// Returns the current Worker-local convergence facts. These facts remain
    /// mutable after the immutable task conclusion has been published.
    pub fn convergence(&self) -> TaskConvergenceSnapshot {
        self.state
            .lock()
            .expect("task status lock")
            .convergence
            .snapshot()
    }

    pub fn retirement_ready(&self) -> bool {
        self.convergence().retirement_ready()
    }

    /// Records the final Worker-owned retirement transition.
    pub fn retire(&self) -> Result<TaskConvergenceSnapshot, TaskConvergenceRejection> {
        let mut state = self.transaction();
        Ok(state.convergence.retire()?.snapshot())
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
        let mut state = self.transaction();
        self.advance_locked(&mut state, now, to, termination, output)
    }

    /// Concludes this task on the termination that wins, in one step.
    ///
    /// `proposal` competes under this owner's lock using the shared accepted
    /// conclusion rule. Cancellation, originating failure and nonderived abort
    /// keep their first accepted cause; only a derived peer placeholder may be
    /// refined by an originating failure. The accepted termination completes
    /// with `output` as its independent output fact.
    pub fn conclude_termination(
        &self,
        proposal: TerminationDetail,
        output: TaskOutputFacts,
    ) -> StatusAdvance {
        let now = self.clock.now();
        let mut state = self.transaction();
        let from = state.current.state();
        if from.is_terminal() {
            return StatusAdvance::AlreadyTerminal(from);
        }
        if failure_needs_source(&state, &proposal) {
            return StatusAdvance::MissingFailureSource;
        }
        let terminating = terminating_state_of(&proposal);
        if termination_decision(&state, &proposal)
            != novarocks_execution_contract::ConclusionDecision::Keep
        {
            let Some(version) = state.current.version().next() else {
                return StatusAdvance::VersionExhausted;
            };
            state.output = TaskOutputFacts::default();
            let next = match self.compose(&state, version, terminating, Some(proposal)) {
                Ok(next) => next,
                Err(error) => return StatusAdvance::Rejected(error),
            };
            self.commit_locked(&mut state, next, now);
        }
        self.complete_termination_locked(&mut state, now, output)
    }

    /// Finishes this task, unless a termination already won.
    ///
    /// A task that is already terminating completes that termination whatever
    /// its work did next, keeping `output` as the independent fact that its
    /// sink did run to completion. This is the same answer the execution owner
    /// gives when its own stand-down latch is set; deciding it here covers the
    /// window in which this owner already published the stand-down and the
    /// execution owner has not yet been told.
    pub fn conclude_success(&self, output: TaskOutputFacts) -> StatusAdvance {
        let now = self.clock.now();
        let mut state = self.transaction();
        let from = state.current.state();
        if from.is_terminal() {
            return StatusAdvance::AlreadyTerminal(from);
        }
        if terminal_state_of(from).is_some() {
            return self.complete_termination_locked(&mut state, now, output);
        }
        self.advance_locked(&mut state, now, TaskState::Finished, None, output)
    }

    /// Completes the termination this task is in, keeping its winning cause.
    fn complete_termination_locked(
        &self,
        state: &mut OwnedStatus,
        now: MonotonicInstant,
        output: TaskOutputFacts,
    ) -> StatusAdvance {
        let from = state.current.state();
        // Both callers enter only once the task is terminating: every
        // non-terminal, non-terminating state admits each terminating step.
        let Some(to) = terminal_state_of(from) else {
            return StatusAdvance::Illegal {
                from,
                to: TaskState::Aborted,
            };
        };
        let Some(version) = state.current.version().next() else {
            return StatusAdvance::VersionExhausted;
        };
        let termination = state.current.termination().cloned();
        state.output = output;
        let next = match self.compose(state, version, to, termination) {
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
        state
            .convergence
            .note_conclusion_stable()
            .expect("a live task accepts its stable conclusion");
        self.commit_locked(state, next, now);
        StatusAdvance::Published(version)
    }

    fn advance_locked(
        &self,
        state: &mut OwnedStatus,
        now: MonotonicInstant,
        to: TaskState,
        mut termination: Option<TerminationDetail>,
        output: TaskOutputFacts,
    ) -> StatusAdvance {
        let from = state.current.state();
        if from.is_terminal() {
            return StatusAdvance::AlreadyTerminal(from);
        }
        if termination
            .as_ref()
            .is_some_and(|detail| failure_needs_source(state, detail))
        {
            return StatusAdvance::MissingFailureSource;
        }
        if terminal_state_of(from) == Some(to) {
            termination = state.current.termination().cloned();
        } else if let Some(proposed) = termination.as_ref()
            && state.conclusions.current() != Some(&TaskConclusion::Terminated(proposed.clone()))
            && termination_decision(state, proposed)
                == novarocks_execution_contract::ConclusionDecision::Keep
        {
            return StatusAdvance::KeptConclusion(from);
        }
        match classify_task_transition(from, to) {
            TaskTransition::AlreadyTerminal => StatusAdvance::AlreadyTerminal(from),
            TaskTransition::Illegal => StatusAdvance::Illegal { from, to },
            TaskTransition::SameState => {
                state.output = output;
                self.republish_locked(state, now, false)
            }
            TaskTransition::Apply => {
                let Some(version) = state.current.version().next() else {
                    return StatusAdvance::VersionExhausted;
                };
                state.output = output;
                let next = match self.compose(state, version, to, termination) {
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
                    state
                        .convergence
                        .note_conclusion_stable()
                        .expect("a live task accepts its stable conclusion");
                }
                self.commit_locked(state, next, now);
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
        let mut state = self.transaction();
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
        let mut state = self.transaction();
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
        let mut state = self.transaction();
        if state.current.is_terminal() {
            return StatusAdvance::AlreadyTerminal(state.current.state());
        }
        state.filters = Some(DynamicFilterAdvertisement::new(version, domain_count));
        state.filter_payload = Some(payload);
        self.republish_locked(&mut state, now, true)
    }

    /// Fixes the stable conclusion of a task that ignored stand-down.
    ///
    /// Only the query context owner calls this, and only once the termination
    /// grace has elapsed: a task is asked to stand down first and given a
    /// bounded window to converge on its own outcome. This does not claim the
    /// execution stopped or release output or resources. Those facts require
    /// positive reports from the local execution owner. A task already
    /// converging on its own failure keeps that failure as its cause.
    pub fn force_conclusion(&self, cause: AbortCause) -> bool {
        let now = self.clock.now();
        let mut state = self.transaction();
        if state.current.is_terminal() {
            return false;
        }
        if state.current.termination().is_none() {
            let Some(version) = state.current.version().next() else {
                return false;
            };
            let Ok(next) = self.compose(
                &state,
                version,
                TaskState::Aborting,
                Some(TerminationDetail::Aborted(cause)),
            ) else {
                return false;
            };
            self.commit_locked(&mut state, next, now);
        }
        // Grace expiration fixes the accepted logical conclusion. It cannot
        // replace cancellation or an earlier abort, and proves no work exited.
        matches!(
            self.complete_termination_locked(&mut state, now, TaskOutputFacts::default()),
            StatusAdvance::Published(_)
        )
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
        let mut state = self.transaction();
        let from = state.current.state();
        let advance = match classify_root_drain(from) {
            // The stable conclusion does not close the convergence ledger.
            // A late drain still releases output responsibility even though
            // it cannot rewrite the terminal TaskStatus.
            RootDrainAction::AlreadyTerminal => StatusAdvance::AlreadyTerminal(from),
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
                state
                    .convergence
                    .note_conclusion_stable()
                    .expect("a live task accepts its stable conclusion");
                self.commit_locked(&mut state, next, now);
                StatusAdvance::Published(version)
            }
        };
        let progressed = matches!(
            state
                .convergence
                .note_output_released()
                .expect("a live task accepts output release"),
            TaskConvergenceAdvance::Advanced(_)
        );
        drop(state);
        if progressed {
            // No task-status snapshot carries this Worker-local convergence
            // fact yet, so wake the registry directly.
            self.source.note_progress();
        }
        advance
    }

    fn record_operator_statistics(&self, statistics: Vec<OperatorStatistics>) {
        let mut state = self.transaction();
        let mut statistics = statistics;
        if statistics.len() > FINAL_TASK_INFO_MAX_OPERATORS {
            statistics.truncate(FINAL_TASK_INFO_MAX_OPERATORS);
            state.operator_statistics_truncated = true;
        }
        state.operator_statistics = statistics;
    }

    fn release_output(&self) {
        let progressed = {
            let mut state = self.transaction();
            matches!(
                state
                    .convergence
                    .note_output_released()
                    .expect("a live task accepts output release"),
                TaskConvergenceAdvance::Advanced(_)
            )
        };
        if progressed {
            self.source.note_progress();
        }
    }

    fn apply_runtime_exit_receipts_locked(&self, state: &mut OwnedStatus) -> bool {
        if !self.preparation.allows_runtime_exit_publication() {
            return false;
        }
        if state.runtime_actual_stopped_received
            && let TaskConvergenceAdvance::Advanced(snapshot) = state
                .convergence
                .note_actual_stopped()
                .expect("a live task accepts original actual-stop evidence")
        {
            let version = TaskConvergenceVersion::new(snapshot.version())
                .expect("advanced convergence has a nonzero version");
            state.buffered_convergence = Some(TaskConvergenceReceipt::actual_stopped(
                self.identity,
                version,
            ));
        }
        state.runtime_resources_converged_received
            && matches!(
                state
                    .convergence
                    .note_resources_converged()
                    .expect("original runtime resources follow original actual stop"),
                TaskConvergenceAdvance::Advanced(_)
            )
    }
    fn note_actual_stopped(&self) {
        let resources_progressed = {
            let mut state = self.transaction();
            state.runtime_actual_stopped_received = true;
            self.apply_runtime_exit_receipts_locked(&mut state)
        };
        if resources_progressed {
            self.source.note_progress();
        }
    }
    fn note_resources_converged(&self) {
        let progressed = {
            let mut state = self.transaction();
            assert!(
                state.runtime_actual_stopped_received,
                "runtime resources require their original actual-stop receipt"
            );
            state.runtime_resources_converged_received = true;
            self.apply_runtime_exit_receipts_locked(&mut state)
        };
        if progressed {
            self.source.note_progress();
        }
    }
    pub(crate) fn runtime_actual_stopped_received(&self) -> bool {
        self.state
            .lock()
            .expect("task status lock")
            .runtime_actual_stopped_received
    }
    pub(crate) fn refresh_preparation_exit_receipts(&self) {
        let progressed = {
            let mut state = self.transaction();
            self.apply_runtime_exit_receipts_locked(&mut state)
        };
        if progressed {
            self.source.note_progress();
        }
    }

    fn compose(
        &self,
        state: &OwnedStatus,
        version: TaskStatusVersion,
        task_state: TaskState,
        termination: Option<TerminationDetail>,
    ) -> Result<TaskStatus, TaskStatusError> {
        let mut status = TaskStatus::try_new_with_installed(
            self.identity,
            version,
            task_state,
            termination,
            state.output,
            state.current.installed(),
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
        // Control and success enter the same receipt slot as rich failures.
        // A Failed projection is legal only after the actual source was
        // submitted through TaskConclusionPort under this same transaction.
        let projected = next
            .termination()
            .cloned()
            .map(TaskConclusion::Terminated)
            .or_else(|| (next.state() == TaskState::Finished).then_some(TaskConclusion::Succeeded));
        if let Some(projected) = projected
            && state.conclusions.current() != Some(&projected)
        {
            let proposal = match projected {
                TaskConclusion::Succeeded => TaskConclusionProposal::Succeeded,
                TaskConclusion::Terminated(TerminationDetail::Canceled(reason)) => {
                    TaskConclusionProposal::Canceled(reason)
                }
                TaskConclusion::Terminated(TerminationDetail::Aborted(cause)) => {
                    TaskConclusionProposal::Aborted(cause)
                }
                TaskConclusion::Terminated(TerminationDetail::Failed(_)) => {
                    panic!("Task failure projection requires its accepted original source")
                }
            };
            let (accepted, discarded) = state.conclusions.accept(
                self.identity,
                PreparedTaskConclusion::new(proposal),
                false,
            );
            assert_ne!(
                accepted.decision,
                ConclusionDecision::Keep,
                "status cannot overwrite an accepted Task conclusion"
            );
            assert!(discarded.is_none());
        }
        state.current = next.clone();
        state.metrics_pending = false;
        state.last_publication = now;
        // The transaction installs before unlocking and notifies afterwards.
        // Pre-creation snapshots remain buffered until the creation commits.
        // Snapshots no observer has seen may coalesce; a terminal may not be
        // superseded. The source rejects delayed older versions after unlock.
        let queued_terminal = state.buffered.as_ref().is_some_and(TaskStatus::is_terminal);
        if !queued_terminal {
            state.buffered = Some(next);
        }
    }
}

/// Evaluates a proposal from the immutable cause already held by this owner.
/// There is no second latch or priority policy in the lifecycle projection.
fn termination_decision(
    state: &OwnedStatus,
    proposed: &TerminationDetail,
) -> novarocks_execution_contract::ConclusionDecision {
    use novarocks_execution_contract::{TaskConclusion, decide_conclusion};
    decide_conclusion(
        state.conclusions.current(),
        &TaskConclusion::Terminated(proposed.clone()),
    )
}

/// Neutral status is an observation, never the missing original source.
fn failure_needs_source(state: &OwnedStatus, proposed: &TerminationDetail) -> bool {
    let proposed = TaskConclusion::Terminated(proposed.clone());
    matches!(
        &proposed,
        TaskConclusion::Terminated(TerminationDetail::Failed(_))
    ) && state.conclusions.current() != Some(&proposed)
        && novarocks_execution_contract::decide_conclusion(state.conclusions.current(), &proposed)
            != ConclusionDecision::Keep
}

impl TaskConclusionPort<ExecutionFailure> for TaskStatusOwner {
    fn identity(&self) -> TaskIdentity {
        self.identity
    }
    fn accepted(&self) -> Option<Arc<AcceptedTaskConclusion<ExecutionFailure>>> {
        self.state
            .lock()
            .expect("task status lock")
            .conclusions
            .accepted()
    }
    fn submit(&self, proposal: TaskConclusionProposal<ExecutionFailure>) {
        let candidate = PreparedTaskConclusion::new(proposal);
        self.submit_prepared(candidate, false).publish();
    }
}
impl TaskStatusOwner {
    /// Owner-internal settled read: submission has completed its transaction.
    /// Keep the two publications and the original output release ordering.
    fn conclude_failure(&self, cause: ExecutionFailure, phase: TaskFailurePhase) -> StatusAdvance {
        self.submit(TaskConclusionProposal::Failed { cause, phase });
        let accepted = self
            .accepted()
            .expect("failure submission has a winning conclusion");
        match accepted.neutral().conclusion() {
            TaskConclusion::Terminated(detail) => {
                self.conclude_termination(detail.clone(), TaskOutputFacts::default())
            }
            TaskConclusion::Succeeded => StatusAdvance::AlreadyTerminal(self.current().state()),
        }
    }

    /// Worker-local only: Task retirement export and wire projection are not
    /// connected to these secondary records. Snapshotting changes no lifecycle.
    pub fn secondary_diagnostics(&self) -> TaskSecondaryDiagnosticsSnapshot {
        self.state
            .lock()
            .expect("task status lock")
            .secondary_diagnostics
            .snapshot()
    }

    fn report_cleanup(&self, mut prepared: PreparedCleanupDiagnostic) -> DiagnosticRecordOutcome {
        let outcome = {
            let mut state = self.transaction();
            // Never submit/accept here, even if the Task has no conclusion or
            // only a derived peer placeholder. Cleanup cannot claim actual exit.
            state.secondary_diagnostics.record(prepared.record_mut())
        };
        drop(prepared);
        outcome
    }

    pub(crate) fn submit_prepared_owned(
        self: &Arc<Self>,
        candidate: PreparedTaskConclusion,
    ) -> OwnedTaskConclusion {
        let PendingTaskConclusion {
            accepted,
            discarded,
            publication,
        } = self.submit_prepared(candidate, true);
        // No notification escapes the Task-wide fence. The owned effect is
        // its release obligation, not a second source of acceptance rules.
        publication.publish();
        OwnedTaskConclusion {
            owner: Arc::clone(self),
            accepted,
            discarded,
        }
    }

    /// A preparation body's publication/disposal tail can outlive Installed.
    /// Prepare both bounded projections before locking, then choose the legal
    /// lifecycle phase at the same transaction that accepts its original cause.
    /// Execution means installation committed, never that a driver ran.
    pub(crate) fn report_preparation_body_failure(&self, cause: ExecutionFailure) {
        let preparation = PreparedTaskConclusion::new(TaskConclusionProposal::Failed {
            cause: cause.clone(),
            phase: TaskFailurePhase::Preparation,
        });
        let installed = PreparedTaskConclusion::new(TaskConclusionProposal::Failed {
            cause,
            phase: TaskFailurePhase::Execution,
        });
        let (pending, unused) =
            self.submit_prepared_with_installed(preparation, Some(installed), false);
        pending.publish();
        // The unselected rich projection and any losing source leave only
        // after the acceptance lock and its detached notification have exited.
        drop(unused);
    }

    /// Accept through the same Task transaction while a caller installs its
    /// own closed fact. Publish after all enclosing fences have been released.
    /// The caller retains this value outside that fenced scope, including its
    /// unwind path; no callback or rejected source is run under that fence.
    fn submit_prepared(
        &self,
        candidate: PreparedTaskConclusion,
        fence_publication: bool,
    ) -> PendingTaskConclusion<'_> {
        let (pending, unused) =
            self.submit_prepared_with_installed(candidate, None, fence_publication);
        debug_assert!(unused.is_none(), "no alternate lifecycle projection");
        pending
    }

    // Design: ADR-0187 (docs/adr/ADR-0187-receiver-owned-steps-and-single-task-conclusion.md)
    // Design: ADR-0193 (docs/adr/ADR-0193-task-conclusion-proposals-and-settled-points.md)
    fn submit_prepared_with_installed(
        &self,
        mut candidate: PreparedTaskConclusion,
        mut installed: Option<PreparedTaskConclusion>,
        fence_publication: bool,
    ) -> (PendingTaskConclusion<'_>, Option<PreparedTaskConclusion>) {
        // The caller evaluates producer facts/diagnostics before every
        // enclosing fence. Acceptance only installs that prepared candidate.
        let (accepted, discarded, publication) = {
            let mut state = self.transaction();
            if state.current.installed()
                && let Some(installed) = installed.as_mut()
            {
                std::mem::swap(&mut candidate, installed);
            }
            if fence_publication {
                state.publication_fences = state
                    .publication_fences
                    .checked_add(1)
                    .expect("live Task publication fence count");
            }
            if matches!(candidate.conclusion(), TaskConclusion::Succeeded)
                && state.conclusions.current().is_none()
            {
                assert!(
                    state.output.responsibility_complete()
                        && state.convergence.snapshot().actual_stopped(),
                    "Task success submission requires actual stopped and completed output facts"
                );
            }
            let sealed = state.current.is_terminal();
            let (accepted, mut discarded) =
                state.conclusions.accept(self.identity, candidate, sealed);
            // Only a losing originating proposal is secondary. Stop observations
            // carry no prepared record and cannot create a diagnostic storm.
            if accepted.decision == ConclusionDecision::Keep
                && let Some(candidate) = discarded.as_mut()
                && candidate.late_diagnostic_mut().is_some()
            {
                state
                    .secondary_diagnostics
                    .record(candidate.late_diagnostic_mut());
            }
            if accepted.decision != ConclusionDecision::Keep {
                let (to, detail, output) = match accepted.accepted.neutral().conclusion() {
                    TaskConclusion::Succeeded => (TaskState::Finished, None, state.output),
                    TaskConclusion::Terminated(detail) => (
                        terminating_state_of(detail),
                        Some(detail.clone()),
                        TaskOutputFacts::default(),
                    ),
                };
                let advance = self.advance_locked(&mut state, self.clock.now(), to, detail, output);
                assert!(
                    matches!(
                        advance,
                        StatusAdvance::Published(_)
                            | StatusAdvance::Republished(_)
                            | StatusAdvance::VersionExhausted
                    ),
                    "accepted Task conclusion must project to its lifecycle: {advance:?}"
                );
            }
            let publication = state.detach_publication();
            (accepted, discarded, publication)
        };
        (
            PendingTaskConclusion {
                accepted,
                discarded,
                publication,
            },
            installed,
        )
    }
}
impl TaskExecutionPort for TaskStatusOwner {
    fn execution_health(
        &self,
    ) -> Arc<novarocks_execution::runtime::execution_health::ExecutionHealth> {
        self.execution_health.clone()
    }
    fn report_cleanup(&self, cause: &ExecutionFailure, phase: TaskFailurePhase) {
        let prepared = PreparedCleanupDiagnostic::from_failure(cause, phase);
        let _ = TaskStatusOwner::report_cleanup(self, prepared);
    }
    fn report_unresolved_exit(
        &self,
        fact: novarocks_execution_contract::failure::FailureFact,
        phase: TaskFailurePhase,
    ) {
        self.execution_health.report_unresolved(
            novarocks_execution::runtime::execution_health::ExecutionExitOrigin::Task {
                identity: self.identity,
                phase,
            },
            fact,
        );
    }
    fn is_stopped(&self) -> bool {
        self.conclusion_present.load(Ordering::Acquire)
    }
    fn stop_observable(&self) -> Arc<Observable> {
        Arc::clone(&self.stop_observable)
    }
    fn wait_until_stopped(&self, duration: Duration) -> bool {
        let state = self.state.lock().expect("task status lock");
        let (state, _) = self
            .conclusion_changed
            .wait_timeout_while(state, duration, |state| {
                state.conclusions.current().is_none()
            })
            .expect("Task conclusion wait");
        state.conclusions.current().is_some()
    }
}

/// The terminating state a proposed termination enters first.
const fn terminating_state_of(detail: &TerminationDetail) -> TaskState {
    match detail {
        TerminationDetail::Canceled(_) => TaskState::Canceling,
        TerminationDetail::Aborted(_) => TaskState::Aborting,
        TerminationDetail::Failed(_) => TaskState::Failing,
    }
}

/// The terminal a terminating state completes to, or `None` for a state that
/// has not started terminating.
const fn terminal_state_of(state: TaskState) -> Option<TaskState> {
    match state {
        TaskState::Canceling => Some(TaskState::Canceled),
        TaskState::Aborting => Some(TaskState::Aborted),
        TaskState::Failing => Some(TaskState::Failed),
        _ => None,
    }
}

/// The lifecycle-writing handle of one Task's original admitted roles. Clones
/// share those positions and the one preissued completion exit notice.
#[derive(Clone)]
pub struct TaskStatusReporter {
    inner: Arc<ReporterBinding>,
}
struct ReporterBinding {
    owner: Arc<dyn crate::task_lifecycle::TaskLifecyclePort>,
    completion_exit: Mutex<Option<crate::task_lifecycle::CompletionExitNotice>>,
}
impl std::fmt::Debug for TaskStatusReporter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskStatusReporter")
            .field("identity", &self.identity())
            .finish_non_exhaustive()
    }
}
#[derive(Debug)]
pub struct CompletionExitAlreadyTaken;
impl std::fmt::Display for CompletionExitAlreadyTaken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the Task's original completion exit notice was already transferred")
    }
}
impl std::error::Error for CompletionExitAlreadyTaken {}
impl novarocks_execution_contract::failure::FailureProjection for CompletionExitAlreadyTaken {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::{FailureClass, FailureFact};
        FailureFact::new(
            FailureClass::Invariant,
            crate::failure_reason::WorkerFailureReason::CompletionDuplicate.reason(),
        )
    }
}
impl TaskStatusReporter {
    pub fn from_admitted(
        owner: Arc<dyn crate::task_lifecycle::TaskLifecyclePort>,
        completion_exit: crate::task_lifecycle::CompletionExitNotice,
    ) -> Result<
        Self,
        (
            novarocks_execution::runtime::task_control::TaskControlIdentityMismatch,
            crate::task_lifecycle::CompletionExitNotice,
        ),
    > {
        if owner.identity() != completion_exit.identity() {
            return Err((
                novarocks_execution::runtime::task_control::TaskControlIdentityMismatch,
                completion_exit,
            ));
        }
        Ok(Self {
            inner: Arc::new(ReporterBinding {
                owner,
                completion_exit: Mutex::new(Some(completion_exit)),
            }),
        })
    }
    pub fn identity(&self) -> TaskIdentity {
        self.inner.owner.identity()
    }
    pub fn current(&self) -> TaskStatus {
        self.inner.owner.current()
    }
    pub fn take_completion_exit_notice(
        &self,
    ) -> Result<crate::task_lifecycle::CompletionExitNotice, CompletionExitAlreadyTaken> {
        self.inner
            .completion_exit
            .lock()
            .expect("original completion exit authority")
            .take()
            .ok_or(CompletionExitAlreadyTaken)
    }
    /// The exact originating carrier transfers before its ordered finish fact.
    /// A neutral TaskFailure cannot replace this actual source.
    pub fn conclude_failure(&self, cause: ExecutionFailure, phase: TaskFailurePhase) {
        self.inner
            .owner
            .submit(TaskConclusionProposal::Failed { cause, phase });
        self.inner
            .owner
            .handoff(crate::task_lifecycle::TaskLifecycleEvent::FinishWithOutput { output: None });
    }
    pub fn report_cleanup(&self, prepared: PreparedCleanupDiagnostic) {
        self.inner.owner.handoff_cleanup(prepared);
    }
    pub fn secondary_diagnostics(&self) -> TaskSecondaryDiagnosticsSnapshot {
        self.inner.owner.secondary_diagnostics()
    }
    pub fn running(&self) {
        self.inner
            .owner
            .handoff(crate::task_lifecycle::TaskLifecycleEvent::Running);
    }
    pub fn flushing(&self) {
        self.inner
            .owner
            .handoff(crate::task_lifecycle::TaskLifecycleEvent::Flushing);
    }
    pub fn completion_intent(&self, completed_output: Option<TaskOutputFacts>) {
        self.inner.owner.handoff(
            crate::task_lifecycle::TaskLifecycleEvent::CompletionIntent { completed_output },
        );
    }
    pub fn finished(&self, output: TaskOutputFacts) {
        self.completion_intent(Some(output));
    }
    pub fn conclude_success(&self, output: TaskOutputFacts) {
        self.completion_intent(Some(output));
    }
    pub fn canceling(&self, reason: CancelReason) {
        self.inner
            .owner
            .submit(TaskConclusionProposal::Canceled(reason));
    }
    pub fn canceled(&self, reason: CancelReason) {
        self.canceled_with_output(reason, TaskOutputFacts::default());
    }
    pub fn canceled_with_output(&self, reason: CancelReason, output: TaskOutputFacts) {
        self.conclude_termination(
            crate::task_lifecycle::TaskStopIntent::Canceled(reason),
            output,
        );
    }
    pub fn aborting(&self, cause: AbortCause) {
        self.inner
            .owner
            .submit(TaskConclusionProposal::Aborted(cause));
    }
    pub fn aborted(&self, cause: AbortCause) {
        self.conclude_termination(
            crate::task_lifecycle::TaskStopIntent::Aborted(cause),
            TaskOutputFacts::default(),
        );
    }
    pub fn failing(&self, cause: ExecutionFailure, phase: TaskFailurePhase) {
        self.inner
            .owner
            .submit(TaskConclusionProposal::Failed { cause, phase });
    }
    pub fn failed(&self, cause: ExecutionFailure, phase: TaskFailurePhase) {
        self.conclude_failure(cause, phase);
    }
    pub fn conclude_termination(
        &self,
        intent: crate::task_lifecycle::TaskStopIntent,
        output: TaskOutputFacts,
    ) {
        match intent {
            crate::task_lifecycle::TaskStopIntent::Canceled(reason) => self.canceling(reason),
            crate::task_lifecycle::TaskStopIntent::Aborted(cause) => self.aborting(cause),
        }
        self.inner.owner.handoff(
            crate::task_lifecycle::TaskLifecycleEvent::FinishWithOutput {
                output: output.responsibility_complete().then_some(output),
            },
        );
    }
    pub fn advertise_dynamic_filters(
        &self,
        version: DomainVersion,
        domain_count: u32,
        payload: Arc<dyn CodecOwnedContent>,
    ) {
        self.inner
            .owner
            .handoff_dynamic_filters(version, domain_count, payload);
    }
    pub fn record_operator_statistics(&self, mut statistics: Vec<OperatorStatistics>) {
        let truncated = statistics.len() > FINAL_TASK_INFO_MAX_OPERATORS;
        statistics.truncate(FINAL_TASK_INFO_MAX_OPERATORS);
        self.inner.owner.handoff(
            crate::task_lifecycle::TaskLifecycleEvent::OperatorStatistics {
                statistics,
                truncated,
            },
        );
    }
    pub fn release_output(&self) {
        self.inner
            .owner
            .handoff(crate::task_lifecycle::TaskLifecycleEvent::OutputReleased);
    }
    /// Independent physical producers may report these original facts. The
    /// completion supervisor uses its separate unique exit notice instead.
    pub fn note_actual_stopped(&self) {
        self.inner
            .owner
            .handoff(crate::task_lifecycle::TaskLifecycleEvent::ActualStopped);
    }
    pub fn note_resources_converged(&self) {
        self.inner
            .owner
            .handoff(crate::task_lifecycle::TaskLifecycleEvent::ResourcesConverged);
    }
    pub fn convergence(&self) -> TaskConvergenceSnapshot {
        self.inner.owner.convergence()
    }
    pub fn metrics(&self) -> TaskMetricsSink {
        TaskMetricsSink {
            owner: self.inner.owner.clone(),
        }
    }
}

/// Counter-only view of the same original Task roles. It signs no new position.
#[derive(Clone)]
pub struct TaskMetricsSink {
    owner: Arc<dyn crate::task_lifecycle::TaskLifecyclePort>,
}
impl std::fmt::Debug for TaskMetricsSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskMetricsSink")
            .field("identity", &self.owner.identity())
            .finish_non_exhaustive()
    }
}
impl TaskMetricsSink {
    pub fn report(&self, resources: TaskResourceFacts, writer: Option<TaskWriterFacts>) {
        self.owner
            .handoff(crate::task_lifecycle::TaskLifecycleEvent::Metrics { resources, writer });
    }
}

/// One original result operation owns its preadmitted drain request. A route
/// lookup alone cannot manufacture an EOF proof or clone this request role.
#[derive(Debug)]
pub struct RootResultBinding {
    kernel_key: UniqueId,
    drain: crate::task_lifecycle::TaskRootDrainPermit,
}
impl RootResultBinding {
    pub const fn new(
        kernel_key: UniqueId,
        drain: crate::task_lifecycle::TaskRootDrainPermit,
    ) -> Self {
        Self { kernel_key, drain }
    }
    pub const fn kernel_key(&self) -> UniqueId {
        self.kernel_key
    }
    pub fn identity(&self) -> TaskIdentity {
        self.drain.identity()
    }
    pub fn note_result_stream_drained(self) -> crate::task_lifecycle::TaskRootDrainFuture {
        self.drain.submit()
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
#[derive(Debug)]
pub enum RootResultRoute {
    /// This exact live task owns the query's client-visible result.
    Serve(RootResultBinding),
    /// This exact result owner belongs to a context whose output was revoked
    /// by a latched termination. The accepted Task control stream supplies
    /// the originating cause; absence of its revoked buffer is not a refusal.
    AwaitTerminalControl,
    /// No task of this identity exists on this exact backend process.
    UnknownTask,
    /// A live task, but the sink its creation winner validated is not the
    /// query's result sink.
    NotResultOwner,
    /// The creation transaction has not committed, so no buffer exists yet.
    Creating,
    /// The task already reached its terminal and did not own a root result.
    Terminal(TaskState),
    /// A retired result owner may still replay its exact acknowledged EOS
    /// until the task fence or owning context is reclaimed.
    TerminalResultOwner(TaskState),
    /// The retained terminal record was reclaimed.
    Gone,
}

impl RootResultRoute {
    /// A bounded, secret-free explanation of a refusal, for the poll's own
    /// error field.
    pub fn refusal_detail(&self) -> Option<String> {
        match self {
            Self::Serve(_) | Self::AwaitTerminalControl => None,
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
            Self::TerminalResultOwner(state) => Some(format!(
                "result poll names a result-owning task that is already {state}"
            )),
            Self::Gone => Some("result poll reached a reclaimed task record".to_owned()),
        }
    }
}

#[cfg(test)]
mod task_convergence_tests {
    use super::*;
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };

    #[test]
    fn actual_stop_before_creation_commit_becomes_replayable_only_after_commit() {
        let execution = QueryExecutionId::new(
            QueryId::new(21, 22),
            AttemptId::new(1).expect("nonzero attempt"),
        )
        .expect("nonzero query id");
        let identity = TaskIdentity::new(
            execution,
            StageId::new(1).expect("nonzero stage"),
            TaskId::new(1).expect("nonzero task"),
            BackendProcessId::new_v7(),
        );
        let source = Arc::new(TaskStatusSource::new());
        let owner = TaskStatusOwner::new(
            identity,
            Arc::clone(&source),
            Arc::new(crate::ManualClock::new()),
            Duration::from_millis(250),
            Arc::new(novarocks_execution::runtime::execution_health::ExecutionHealth::default()),
        );

        owner.note_actual_stopped();
        assert_eq!(source.latest_task_convergence(identity), None);
        owner.release_to_observers();
        let receipt = source
            .latest_task_convergence(identity)
            .expect("committed task retains actual-stop fact");
        assert_eq!(receipt.identity(), identity);
        assert_eq!(receipt.version().get(), 1);
        owner.note_actual_stopped();
        assert_eq!(source.latest_task_convergence(identity), Some(receipt));
    }
}

#[cfg(test)]
mod conclusion_tests {
    use novarocks_execution_contract::TaskFailure;
    use novarocks_execution_contract::failure::FailureProjection;

    #[test]
    fn independent_constructed_metadata_does_not_retain_the_actual_worker_task() {
        use novarocks_execution::{
            exec::{
                chunk::{Chunk, ChunkSchema},
                pipeline::operator::input_construction::{
                    InputConstructionProfile, try_runtime_input_construction_factory,
                },
            },
            runtime::{mem_tracker::MemTracker, runtime_state::RuntimeErrorState},
        };
        use novarocks_type_contract::ValueTypeStorageLifetime;
        let owner = Arc::new(owner());
        let weak_task = Arc::downgrade(&owner);
        let control = Arc::new(RuntimeErrorState::for_task(
            owner.execution_control(TaskFailurePhase::Execution),
        ));
        let tracker = MemTracker::new_root("independent metadata has no Task capability");
        let factory = try_runtime_input_construction_factory(
            InputConstructionProfile {
                max_requests: 256,
                max_request_bytes: 1024 * 1024,
                max_opaque_requests: 256,
                max_opaque_bytes: 1024 * 1024,
                max_work: 1024 * 1024,
                max_depth: 64,
            },
            tracker.clone(),
            control,
        )
        .unwrap();
        let storage = ValueTypeStorageLifetime::pure();
        let input = Chunk::try_new_typed_with_columns_owned_observed::<ExecutionFailure, _>(
            Arc::new(ChunkSchema::try_new_typed(vec![], storage.clone()).unwrap()),
            vec![],
            1,
            storage,
            |_| Ok(()),
        )
        .unwrap();
        let scope = factory.begin_input_construction(&input).unwrap();
        let metadata = scope.storage_lifetime();
        drop(owner);
        drop(factory);
        assert!(
            weak_task.upgrade().is_some(),
            "actual construction scope still owns work control"
        );
        drop(scope);
        assert!(
            weak_task.upgrade().is_none(),
            "independent metadata must not retain Task control"
        );
        assert!(
            tracker.current() > 0,
            "independent metadata retains its own storage responsibility"
        );
        drop(metadata);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn core_runtime_delegates_to_the_actual_worker_task_and_keeps_its_original_source() {
        use novarocks_execution::runtime::runtime_state::RuntimeErrorState;
        use novarocks_execution_contract::failure::{FailureEvidence, FailureProjection};
        use std::sync::atomic::AtomicUsize;
        let owner = Arc::new(owner());
        owner.release_to_observers();
        let runtime = Arc::new(RuntimeErrorState::for_task(
            owner.execution_control(TaskFailurePhase::Execution),
        ));
        let callbacks = Arc::new(AtomicUsize::new(0));
        let count = callbacks.clone();
        let weak_runtime = Arc::downgrade(&runtime);
        let weak_owner = Arc::downgrade(&owner);
        owner.stop_observable().add_observer(Arc::new(move || {
            let owner = weak_owner.upgrade().unwrap();
            assert!(
                owner.state.try_lock().is_ok(),
                "runtime notification is outside the actual Task acceptance lock"
            );
            let runtime = weak_runtime.upgrade().unwrap();
            let observed = runtime.error().unwrap();
            let accepted = owner.accepted().unwrap();
            if let Some(original) = accepted.originating_failure() {
                assert!(std::ptr::eq(observed.cause(), original.cause()));
            } else {
                assert_eq!(
                    observed.failure_fact().evidence(),
                    FailureEvidence::StopObservation
                );
            }
            count.fetch_add(1, Ordering::Release);
        }));
        owner.submit(TaskConclusionProposal::Aborted(AbortCause::PeerTaskFailed));
        assert_eq!(owner.state(), TaskState::Aborting);
        runtime.set_error(runtime.error().unwrap());
        assert_eq!(callbacks.load(Ordering::Acquire), 1);
        let original: ExecutionFailure =
            novarocks_functions::KernelFailure::ResourceExhausted.into();
        runtime.set_error(original.clone());
        assert_eq!(callbacks.load(Ordering::Acquire), 2);
        assert_eq!(owner.state(), TaskState::Failing);
        assert!(std::ptr::eq(
            owner
                .accepted()
                .unwrap()
                .originating_failure()
                .unwrap()
                .cause(),
            original.cause()
        ));
        assert!(std::ptr::eq(
            runtime.error().unwrap().cause(),
            original.cause()
        ));
        assert!(
            !owner.convergence().actual_stopped(),
            "failure notification is not physical work exit"
        );
        runtime.set_error(novarocks_functions::KernelFailure::Cancelled);
        assert_eq!(callbacks.load(Ordering::Acquire), 2);
        assert!(std::ptr::eq(
            runtime.error().unwrap().cause(),
            original.cause()
        ));
    }

    #[test]
    fn core_runtime_cannot_replace_an_actual_worker_cancellation_with_a_later_failure() {
        use novarocks_execution::runtime::runtime_state::RuntimeErrorState;
        use novarocks_execution_contract::failure::{FailureEvidence, FailureProjection};
        let owner = Arc::new(owner());
        let runtime =
            RuntimeErrorState::for_task(owner.execution_control(TaskFailurePhase::Execution));
        owner.submit(TaskConclusionProposal::Canceled(
            CancelReason::UpstreamNoLongerNeeded,
        ));
        let canceled = owner.accepted().unwrap();
        runtime.set_error(novarocks_functions::KernelFailure::ResourceExhausted);
        assert!(Arc::ptr_eq(&canceled, &owner.accepted().unwrap()));
        assert!(owner.accepted().unwrap().originating_failure().is_none());
        assert_eq!(
            runtime.error().unwrap().failure_fact().evidence(),
            FailureEvidence::StopObservation
        );
        assert!(!owner.convergence().actual_stopped());
    }

    // These tests exercise the production TaskStatusOwner, not the Core view's
    // fixture owner. Its existing mutex owns receipt, lifecycle and stop view.
    #[test]
    fn accepted_task_freezes_exact_producer_facts_without_retaining_source_in_neutral_clone() {
        use novarocks_execution_contract::failure::*;
        use std::sync::atomic::{AtomicUsize, Ordering};
        #[derive(Debug)]
        struct ActualSource {
            fact: FailureFact,
            drops: Arc<AtomicUsize>,
            projections: Arc<AtomicUsize>,
        }
        impl std::fmt::Display for ActualSource {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("private-provider-credential")
            }
        }
        impl std::error::Error for ActualSource {}
        impl FailureProjection for ActualSource {
            fn failure_fact(&self) -> FailureFact {
                let call = self.projections.fetch_add(1, Ordering::AcqRel);
                assert_eq!(
                    call, 0,
                    "Task publication and diagnostics reuse one authored snapshot"
                );
                self.fact
            }
        }
        impl Drop for ActualSource {
            fn drop(&mut self) {
                self.drops.fetch_add(1, Ordering::AcqRel);
            }
        }
        let fact = FailureFact::new(
            FailureClass::Representation,
            FailureReason::new(
                std::num::NonZeroU16::new(65534).unwrap(),
                std::num::NonZeroU16::new(65535).unwrap(),
            ),
        )
        .with_row_position(FailureRowPosition {
            selected_ordinal: 0,
            batch_row: u64::MAX,
        })
        .with_resource(ResourceFacts {
            request_scope: Some(ResourceScope::Operator),
            constraint_scope: Some(ResourceScope::Process),
            dimension: ResourceDimension::Items,
            requested: Some(0),
            required_growth: None,
            limit: Some(u64::MAX),
            live: None,
            committed: Some(0),
        })
        .with_representation(RepresentationFacts {
            boundary: RepresentationBoundary::SingleOutput,
            layout: RepresentationLayout {
                domain: std::num::NonZeroU16::new(65535).unwrap(),
                code: std::num::NonZeroU16::new(1).unwrap(),
            },
            required: Some(0),
            extent_limit: None,
            representable_prefix: Some(0),
        });
        let drops = Arc::new(AtomicUsize::new(0));
        let projections = Arc::new(AtomicUsize::new(0));
        let source = ExecutionFailure::from_domain(ActualSource {
            fact,
            drops: drops.clone(),
            projections: projections.clone(),
        });
        let owner = owner();
        owner.submit(TaskConclusionProposal::Aborted(AbortCause::PeerTaskFailed));
        let peer = owner.accepted().unwrap();
        let before_accepted = owner.accepted();
        owner.submit(TaskConclusionProposal::Failed {
            cause: source.clone(),
            phase: TaskFailurePhase::Preparation,
        });
        let accepted = owner.accepted().unwrap();
        assert_eq!(
            accepted.neutral().generation().get(),
            before_accepted.unwrap().neutral().generation().get() + 1
        );
        assert_eq!(projections.load(Ordering::Acquire), 1);
        assert!(std::ptr::eq(
            accepted.originating_failure().unwrap().cause(),
            source.cause(),
        ));
        let TaskConclusion::Terminated(TerminationDetail::Failed(neutral)) =
            accepted.neutral().conclusion()
        else {
            panic!("authored failure")
        };
        let neutral = neutral.clone();
        assert_eq!(neutral.fact(), fact);
        assert_eq!(neutral.phase(), TaskFailurePhase::Preparation);
        assert!(
            !neutral
                .detail()
                .as_str()
                .contains("private-provider-credential")
        );
        owner.submit(TaskConclusionProposal::Canceled(
            CancelReason::UpstreamNoLongerNeeded,
        ));
        let canceled = owner.accepted().unwrap();
        assert!(Arc::ptr_eq(&accepted, &canceled));
        assert_eq!(drops.load(Ordering::Acquire), 0);
        drop(canceled);
        drop(accepted);
        drop(peer);
        drop(source);
        drop(owner);
        assert_eq!(
            drops.load(Ordering::Acquire),
            1,
            "only the original rich source owns provider state"
        );
        assert_eq!(
            neutral.fact(),
            fact,
            "neutral metadata survives actual source teardown"
        );
    }

    #[test]
    fn task_port_keeps_one_original_receipt_and_refines_only_a_live_peer_placeholder() {
        let owner = Arc::new(owner());
        let control = owner.execution_control(TaskFailurePhase::Preparation);
        owner.submit(TaskConclusionProposal::Aborted(AbortCause::PeerTaskFailed));
        let peer = owner.accepted().unwrap();
        assert_eq!(owner.state(), TaskState::Aborting);
        assert_eq!(peer.neutral().generation().get(), 1);
        let cleanup = ExecutionFailure::from(novarocks_functions::KernelFailure::ResourceExhausted);
        assert_eq!(
            TaskStatusReporter::new(owner.clone()).report_cleanup(
                PreparedCleanupDiagnostic::from_failure(&cleanup, TaskFailurePhase::Execution),
            ),
            DiagnosticRecordOutcome::Stored
        );
        assert!(Arc::ptr_eq(&peer, &owner.accepted().unwrap()));
        let observation = control.error().unwrap();
        let before_kept = control.accepted();
        control.report_failure(observation);
        let kept = control.accepted().unwrap();
        assert!(Arc::ptr_eq(&before_kept.unwrap(), &kept));
        assert!(Arc::ptr_eq(&peer, &kept));
        let original =
            ExecutionFailure::from(novarocks_functions::KernelFailure::ResourceExhausted);
        let before_refined = control.accepted();
        control.report_failure(original.clone());
        let refined = control.accepted().unwrap();
        assert_eq!(
            refined.neutral().generation().get(),
            before_refined.unwrap().neutral().generation().get() + 1
        );
        assert_eq!(refined.neutral().generation().get(), 2);
        assert!(std::ptr::eq(
            refined.originating_failure().unwrap().cause(),
            original.cause()
        ));
        assert!(std::ptr::eq(
            control.error().unwrap().cause(),
            original.cause()
        ));
        let secondary = owner.secondary_diagnostics();
        assert_eq!(
            secondary.entries().len(),
            1,
            "only explicit cleanup is secondary"
        );
        assert_eq!(
            secondary.entries().next().unwrap().role(),
            crate::TaskDiagnosticRole::Cleanup
        );
        assert_eq!(secondary.dropped(), 0);
        assert_eq!(owner.state(), TaskState::Failing);
        let TaskConclusion::Terminated(TerminationDetail::Failed(failure)) =
            refined.neutral().conclusion()
        else {
            panic!("original failure receipt required");
        };
        assert_eq!(failure.phase(), TaskFailurePhase::Preparation);
        assert_eq!(failure.category(), TaskFailureCategory::ResourceExhausted);
        assert!(!owner.convergence().actual_stopped());
        owner.force_conclusion(AbortCause::LeaseExpired);
        assert_eq!(owner.state(), TaskState::Failed);
        assert!(!owner.convergence().actual_stopped());
        assert!(Arc::ptr_eq(&refined, &owner.accepted().unwrap()));
    }

    #[test]
    fn task_port_defers_callbacks_until_the_enclosing_closed_fact_is_installed() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let owner = Arc::new(owner());
        owner.release_to_observers();
        let gate = Arc::new(std::sync::Mutex::new(false));
        let observed = Arc::new(AtomicUsize::new(0));
        let weak = Arc::downgrade(&owner);
        let (closed, callbacks) = (gate.clone(), observed.clone());
        owner.stop_observable().add_observer(Arc::new(move || {
            let owner = weak.upgrade().unwrap();
            assert!(
                owner.state.try_lock().is_ok(),
                "Task acceptance lock released"
            );
            assert!(*closed.try_lock().expect("outer fence released"));
            assert!(owner.accepted().unwrap().originating_failure().is_some());
            callbacks.fetch_add(1, Ordering::SeqCst);
        }));
        let candidate = PreparedTaskConclusion::new(TaskConclusionProposal::Failed {
            cause: novarocks_functions::KernelFailure::ResourceExhausted.into(),
            phase: TaskFailurePhase::Preparation,
        });
        // The retained publication lives outside the enclosing lock's scope.
        let pending = {
            let mut closed = gate.lock().unwrap();
            let pending = owner.submit_prepared_owned(candidate);
            assert!(owner.accepted().unwrap().originating_failure().is_some());
            assert_eq!(observed.load(Ordering::SeqCst), 0);
            *closed = true;
            pending
        };
        let accepted = pending.publish();
        assert_eq!(accepted.decision, ConclusionDecision::Accept);
        assert_eq!(observed.load(Ordering::SeqCst), 1);
        assert!(!owner.convergence().actual_stopped());
    }

    #[test]
    fn a_concurrent_originating_refinement_obeys_the_same_outer_publication_fence() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let owner = Arc::new(owner());
        owner.release_to_observers();
        let closed = Arc::new(std::sync::Mutex::new(false));
        let callbacks = Arc::new(AtomicUsize::new(0));
        let (read_closed, observed) = (closed.clone(), callbacks.clone());
        let weak = Arc::downgrade(&owner);
        owner.stop_observable().add_observer(Arc::new(move || {
            assert!(
                *read_closed
                    .try_lock()
                    .expect("all contributors wait for the outer fence")
            );
            let owner = weak.upgrade().unwrap();
            assert!(owner.accepted().unwrap().originating_failure().is_some());
            observed.fetch_add(1, Ordering::SeqCst);
        }));
        let candidate = PreparedTaskConclusion::new(TaskConclusionProposal::Aborted(
            AbortCause::PeerTaskFailed,
        ));
        let pending = {
            let mut closed = closed.lock().unwrap();
            let pending = owner.submit_prepared_owned(candidate);
            let previous = owner.accepted().unwrap();
            let driver = owner.clone();
            let refined = std::thread::spawn(move || {
                driver.submit(TaskConclusionProposal::Failed {
                    cause: novarocks_functions::KernelFailure::ResourceExhausted.into(),
                    phase: TaskFailurePhase::Execution,
                });
                driver.accepted().unwrap()
            })
            .join()
            .expect("the producer report itself remains nonblocking");
            assert_eq!(
                refined.neutral().generation().get(),
                previous.neutral().generation().get() + 1
            );
            assert!(Arc::ptr_eq(&refined, &owner.accepted().unwrap()));
            assert_eq!(callbacks.load(Ordering::SeqCst), 0);
            *closed = true;
            pending
        };
        pending.publish();
        assert_eq!(callbacks.load(Ordering::SeqCst), 1);
        assert!(!owner.convergence().actual_stopped());
    }

    #[test]
    fn nested_task_publication_fences_flush_only_after_the_last_owner_releases() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let owner = Arc::new(owner());
        owner.release_to_observers();
        let callbacks = Arc::new(AtomicUsize::new(0));
        let observed = callbacks.clone();
        owner.stop_observable().add_observer(Arc::new(move || {
            observed.fetch_add(1, Ordering::SeqCst);
        }));
        let first = owner.submit_prepared_owned(PreparedTaskConclusion::new(
            TaskConclusionProposal::Aborted(AbortCause::PeerTaskFailed),
        ));
        let second = owner.submit_prepared_owned(PreparedTaskConclusion::new(
            TaskConclusionProposal::Failed {
                cause: novarocks_functions::KernelFailure::ResourceExhausted.into(),
                phase: TaskFailurePhase::Execution,
            },
        ));
        assert_eq!(
            second.acceptance().decision,
            ConclusionDecision::RefineDerived
        );
        let current = owner.accepted().unwrap();
        let original = first.publish();
        assert_eq!(original.decision, ConclusionDecision::Accept);
        assert_eq!(callbacks.load(Ordering::SeqCst), 0);
        assert!(Arc::ptr_eq(&current, &owner.accepted().unwrap()));
        assert!(!owner.convergence().actual_stopped());
        second.publish();
        assert_eq!(callbacks.load(Ordering::SeqCst), 1);
        assert!(Arc::ptr_eq(&current, &owner.accepted().unwrap()));
        assert!(!owner.convergence().actual_stopped());
    }

    #[test]
    fn actual_stop_is_visible_to_its_owner_before_fenced_convergence_is_published() {
        let owner = Arc::new(owner());
        owner.release_to_observers();
        let pending = owner.submit_prepared_owned(PreparedTaskConclusion::new(
            TaskConclusionProposal::Aborted(AbortCause::PeerTaskFailed),
        ));
        let revision = owner.source.revision();
        let weak = Arc::downgrade(&owner);
        owner.stop_observable().add_observer(Arc::new(move || {
            // A reentrant lifecycle observer must see all deferred owner
            // facts, including actual-stop replay, before it may retire them.
            let owner = weak.upgrade().unwrap();
            assert!(owner.convergence().actual_stopped());
            assert!(
                owner.source.revision() > revision,
                "convergence is installed before lifecycle callbacks"
            );
        }));
        owner.note_actual_stopped();
        assert!(owner.convergence().actual_stopped());
        assert_eq!(owner.source.revision(), revision);
        pending.publish();
        assert!(owner.source.revision() > revision);
        let published = owner.source.revision();
        owner.note_actual_stopped();
        assert_eq!(owner.source.revision(), published);
    }

    #[test]
    fn task_port_acceptance_is_visible_while_notification_is_still_running() {
        use std::sync::Barrier;
        let owner = Arc::new(owner());
        owner.release_to_observers();
        let control = owner.execution_control(TaskFailurePhase::Execution);
        let entered = Arc::new(Barrier::new(2));
        let leave = Arc::new(Barrier::new(2));
        let weak = Arc::downgrade(&owner);
        let unlocked = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (a, b, checked) = (entered.clone(), leave.clone(), unlocked.clone());
        owner.stop_observable().add_observer(Arc::new(move || {
            let owner = weak.upgrade().unwrap();
            checked.store(
                owner.state.try_lock().is_ok(),
                std::sync::atomic::Ordering::Release,
            );
            a.wait();
            b.wait();
        }));
        let worker = owner.clone();
        let job = std::thread::spawn(move || {
            worker.submit(TaskConclusionProposal::Canceled(
                CancelReason::UpstreamNoLongerNeeded,
            ));
            worker.accepted().unwrap()
        });
        entered.wait();
        assert!(unlocked.load(std::sync::atomic::Ordering::Acquire));
        assert!(control.is_stopped());
        assert!(control.wait_until_stopped(Duration::ZERO));
        let first = control.accepted().unwrap();
        assert_eq!(
            owner.current().termination(),
            Some(&TerminationDetail::Canceled(
                CancelReason::UpstreamNoLongerNeeded
            ))
        );
        let before_later = control.accepted();
        control.report_failure(novarocks_functions::KernelFailure::ResourceExhausted.into());
        let later = control.accepted().unwrap();
        assert!(Arc::ptr_eq(&before_later.unwrap(), &later));
        assert!(Arc::ptr_eq(&first, &later));
        leave.wait();
        assert!(Arc::ptr_eq(&first, &job.join().unwrap()));
        assert!(!owner.convergence().actual_stopped());
    }

    #[derive(Debug)]
    struct TaskPortDropProbe {
        owner: std::sync::Weak<TaskStatusOwner>,
        dropped_unlocked: Arc<std::sync::atomic::AtomicBool>,
    }
    impl std::fmt::Display for TaskPortDropProbe {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("private resource diagnostic")
        }
    }
    impl std::error::Error for TaskPortDropProbe {}
    impl novarocks_execution_contract::failure::FailureProjection for TaskPortDropProbe {
        fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
            use novarocks_execution_contract::failure::*;
            FailureFact::new(
                FailureClass::Resource,
                FailureReason::new(domain::WORKER, std::num::NonZeroU16::new(65000).unwrap()),
            )
        }
    }
    impl Drop for TaskPortDropProbe {
        fn drop(&mut self) {
            let owner = self.owner.upgrade().unwrap();
            self.dropped_unlocked.store(
                owner.state.try_lock().is_ok(),
                std::sync::atomic::Ordering::Release,
            );
        }
    }
    #[derive(Debug)]
    struct EnclosingFenceProbe {
        inner: TaskPortDropProbe,
        gate: Arc<std::sync::Mutex<()>>,
    }
    impl std::fmt::Display for EnclosingFenceProbe {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            self.inner.fmt(f)
        }
    }
    impl std::error::Error for EnclosingFenceProbe {}
    impl novarocks_execution_contract::failure::FailureProjection for EnclosingFenceProbe {
        fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
            assert!(
                self.gate.try_lock().is_ok(),
                "projection runs outside the enclosing fence"
            );
            self.inner.failure_fact()
        }
    }
    impl Drop for EnclosingFenceProbe {
        fn drop(&mut self) {
            assert!(
                self.gate.try_lock().is_ok(),
                "discard runs outside the enclosing fence"
            );
        }
    }
    #[test]
    fn task_port_retains_a_losing_source_until_the_enclosing_fence_is_released() {
        for full in [false, true] {
            use std::sync::atomic::{AtomicBool, Ordering};
            let owner = Arc::new(owner());
            owner.submit(TaskConclusionProposal::Canceled(
                CancelReason::UpstreamNoLongerNeeded,
            ));
            let dropped = Arc::new(AtomicBool::new(false));
            let gate = Arc::new(std::sync::Mutex::new(()));
            if full {
                let reporter = TaskStatusReporter::new(owner.clone());
                for _ in 0..4 {
                    let cause = ExecutionFailure::from(
                        novarocks_functions::KernelFailure::ResourceExhausted,
                    );
                    assert_eq!(
                        reporter.report_cleanup(PreparedCleanupDiagnostic::from_failure(
                            &cause,
                            TaskFailurePhase::Execution,
                        )),
                        DiagnosticRecordOutcome::Stored
                    );
                }
            }
            let source = EnclosingFenceProbe {
                inner: TaskPortDropProbe {
                    owner: Arc::downgrade(&owner),
                    dropped_unlocked: dropped.clone(),
                },
                gate: gate.clone(),
            };
            let candidate = PreparedTaskConclusion::new(TaskConclusionProposal::Failed {
                cause: ExecutionFailure::from_domain(source),
                phase: TaskFailurePhase::Preparation,
            });
            let pending = {
                let _fence = gate.lock().unwrap();
                let pending = owner.submit_prepared_owned(candidate);
                assert_eq!(pending.acceptance().decision, ConclusionDecision::Keep);
                assert!(!dropped.load(Ordering::Acquire));
                pending
            };
            assert!(!dropped.load(Ordering::Acquire));
            let kept = pending.publish();
            assert_eq!(kept.decision, ConclusionDecision::Keep);
            assert!(dropped.load(Ordering::Acquire));
            let snapshot = owner.secondary_diagnostics();
            assert_eq!(snapshot.entries().len(), if full { 4 } else { 1 });
            assert_eq!(snapshot.dropped(), u64::from(full));
            assert!(!owner.convergence().actual_stopped());
        }
    }

    #[test]
    fn task_port_drops_a_losing_actual_source_after_unlocking() {
        let owner = Arc::new(owner());
        owner.submit(TaskConclusionProposal::Canceled(
            CancelReason::UpstreamNoLongerNeeded,
        ));
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let original = ExecutionFailure::from_domain(TaskPortDropProbe {
            owner: Arc::downgrade(&owner),
            dropped_unlocked: dropped.clone(),
        });
        let before = owner.accepted().unwrap();
        owner
            .execution_control(TaskFailurePhase::Execution)
            .report_failure(original);
        let held = owner.accepted().unwrap();
        assert!(Arc::ptr_eq(&before, &held));
        assert!(held.originating_failure().is_none());
        assert!(dropped.load(std::sync::atomic::Ordering::Acquire));
        let snapshot = owner.secondary_diagnostics();
        let diagnostic = snapshot.entries().next().unwrap();
        assert_eq!(diagnostic.role(), crate::TaskDiagnosticRole::LateFailure);
        assert_eq!(diagnostic.phase(), TaskFailurePhase::Execution);
        assert_eq!(
            diagnostic.fact().class(),
            novarocks_execution_contract::failure::FailureClass::Resource
        );
        assert_eq!(diagnostic.fact().reason().code(), 65000);
        assert_eq!(diagnostic.detail(), "Resource failure (reason 10:65000)");
        assert!(!diagnostic.detail().contains("private resource diagnostic"));
        assert_eq!(snapshot.dropped(), 0);
    }

    #[test]
    fn task_port_does_not_accept_a_neutral_failure_as_its_original_source() {
        let owner = owner();
        assert_eq!(
            owner.advance(
                TaskState::Failing,
                Some(failed("only a status summary")),
                TaskOutputFacts::default()
            ),
            StatusAdvance::MissingFailureSource
        );
        assert!(owner.accepted().is_none());
        assert_eq!(owner.state(), TaskState::Planned);
        assert!(!TaskExecutionPort::is_stopped(&owner));
    }

    use super::*;
    use novarocks_execution_contract::task_execution::status::{SafeDetail, TaskFailureCategory};
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };

    fn owner() -> TaskStatusOwner {
        let execution = QueryExecutionId::new(
            QueryId::new(23, 24),
            AttemptId::new(1).expect("nonzero attempt"),
        )
        .expect("nonzero query id");
        TaskStatusOwner::new(
            TaskIdentity::new(
                execution,
                StageId::new(1).expect("nonzero stage"),
                TaskId::new(1).expect("nonzero task"),
                BackendProcessId::new_v7(),
            ),
            Arc::new(TaskStatusSource::new()),
            Arc::new(crate::ManualClock::new()),
            Duration::from_millis(250),
            Arc::new(novarocks_execution::runtime::execution_health::ExecutionHealth::default()),
        )
    }

    // These tests protect the production synchronous owner, including both
    // terminating and terminal publication. The port itself allows delay.
    #[test]
    fn conclude_failure_preserves_the_winning_termination_publication_sequence() {
        for initial in [None, Some(false), Some(true)] {
            let owner = Arc::new(owner());
            owner.release_to_observers();
            match initial {
                None => {}
                Some(false) => owner.submit(TaskConclusionProposal::Canceled(
                    CancelReason::UpstreamNoLongerNeeded,
                )),
                Some(true) => {
                    owner.submit(TaskConclusionProposal::Aborted(AbortCause::PeerTaskFailed))
                }
            }
            let before = owner.current();
            let previous = owner.accepted();
            let notifications = Arc::new(Mutex::new(Vec::new()));
            let seen = notifications.clone();
            let weak = Arc::downgrade(&owner);
            owner.stop_observable().add_observer(Arc::new(move || {
                let owner = weak.upgrade().unwrap();
                assert!(
                    owner.state.try_lock().is_ok(),
                    "callbacks run outside the owner lock"
                );
                seen.lock().unwrap().push(owner.current());
            }));
            let original: ExecutionFailure =
                novarocks_functions::KernelFailure::ResourceExhausted.into();
            let reporter = TaskStatusReporter::new(owner.clone());
            let result = reporter.conclude_failure(original.clone(), TaskFailurePhase::Execution);
            assert!(matches!(result, StatusAdvance::Published(_)));
            let terminal = owner.current();
            assert_eq!(terminal.output(), TaskOutputFacts::default());
            assert_eq!(terminal.installed(), before.installed());
            assert!(
                !owner.convergence().actual_stopped(),
                "terminal publication is not physical exit"
            );
            let seen = notifications.lock().unwrap();
            if initial == Some(false) {
                assert!(
                    seen.is_empty(),
                    "losing failure publishes no new terminating version"
                );
                assert_eq!(terminal.state(), TaskState::Canceled);
                assert_eq!(terminal.version(), before.version().next().unwrap());
                assert_eq!(terminal.termination(), before.termination());
                assert!(Arc::ptr_eq(
                    previous.as_ref().unwrap(),
                    &owner.accepted().unwrap()
                ));
                assert_eq!(owner.secondary_diagnostics().entries().len(), 1);
            } else {
                assert_eq!(seen.len(), 1);
                let terminating = &seen[0];
                assert_eq!(terminating.state(), TaskState::Failing);
                assert_eq!(terminating.version(), before.version().next().unwrap());
                assert_eq!(terminal.version(), terminating.version().next().unwrap());
                assert_eq!(terminal.state(), TaskState::Failed);
                assert_eq!(terminal.termination(), terminating.termination());
                let accepted = owner.accepted().unwrap();
                assert!(std::ptr::eq(
                    accepted.originating_failure().unwrap().cause(),
                    original.cause()
                ));
                assert_eq!(
                    accepted.neutral().generation().get(),
                    if initial.is_some() { 2 } else { 1 }
                );
                assert_eq!(owner.secondary_diagnostics().entries().len(), 0);
            }
        }
    }

    fn failure(detail: &str) -> TaskFailure {
        TaskFailure::new(
            novarocks_execution_contract::failure::FailureFact::new(
                novarocks_execution_contract::failure::FailureClass::Data,
                novarocks_execution_contract::failure::FailureReason::new(
                    std::num::NonZeroU16::new(u16::MAX).unwrap(),
                    std::num::NonZeroU16::new(1).unwrap(),
                ),
            ),
            SafeDetail::new(detail).expect("bounded detail"),
        )
    }

    fn canceled(reason: CancelReason) -> TerminationDetail {
        TerminationDetail::Canceled(reason)
    }

    fn aborted(cause: AbortCause) -> TerminationDetail {
        TerminationDetail::Aborted(cause)
    }

    fn failed(detail: &str) -> TerminationDetail {
        TerminationDetail::Failed(failure(detail))
    }

    /// A proposal fixture owns its actual source from construction onward.
    /// Expected status is an oracle, never input for rebuilding that source.
    #[derive(Clone, Debug)]
    struct TerminationFixture {
        expected: TerminationDetail,
        source: Option<ExecutionFailure>,
    }
    impl TerminationFixture {
        fn command(expected: TerminationDetail) -> Self {
            assert!(!matches!(expected, TerminationDetail::Failed(_)));
            Self {
                expected,
                source: None,
            }
        }
        fn producer(source: novarocks_functions::KernelFailure) -> Self {
            let source = ExecutionFailure::from(source);
            let expected = TerminationDetail::Failed(TaskFailure::new_in_phase(
                source.failure_fact(),
                source.safe_detail(),
                TaskFailurePhase::Execution,
            ));
            Self {
                expected,
                source: Some(source),
            }
        }
        fn submit(&self, owner: &TaskStatusOwner) -> Arc<AcceptedTaskConclusion<ExecutionFailure>> {
            let proposal = match (&self.expected, &self.source) {
                (TerminationDetail::Canceled(reason), None) => {
                    TaskConclusionProposal::Canceled(*reason)
                }
                (TerminationDetail::Aborted(cause), None) => {
                    TaskConclusionProposal::Aborted(*cause)
                }
                (TerminationDetail::Failed(_), Some(cause)) => TaskConclusionProposal::Failed {
                    cause: cause.clone(),
                    phase: TaskFailurePhase::Execution,
                },
                _ => panic!("a fixture cannot reconstruct a cause from a summary"),
            };
            owner.submit(proposal);
            owner.accepted().unwrap()
        }
    }
    fn resource_fixture() -> TerminationFixture {
        TerminationFixture::producer(novarocks_functions::KernelFailure::ResourceExhausted)
    }
    fn internal_fixture() -> TerminationFixture {
        TerminationFixture::producer(novarocks_functions::KernelFailure::Internal(
            novarocks_functions::KernelDiagnostic::new("actual test producer failure"),
        ))
    }

    #[derive(Clone, Debug)]
    enum Start {
        /// Accepted but not installed: the shape a rolled-back creation has.
        Planned,
        Running,
        Terminating(Box<TerminationFixture>),
    }

    fn start(start: &Start) -> TaskStatusOwner {
        let owner = owner();
        let enter = |fixture: &TerminationFixture| {
            assert!(owner.accepted().is_none());
            let accepted = fixture.submit(&owner);
            assert_eq!(accepted.neutral().generation().get(), 1);
            assert_eq!(
                accepted.neutral().conclusion(),
                &TaskConclusion::Terminated(fixture.expected.clone())
            );
        };
        match start {
            Start::Planned => {}
            Start::Running => {
                owner.note_installed();
                assert!(matches!(
                    owner.advance(TaskState::Running, None, TaskOutputFacts::default()),
                    StatusAdvance::Published(_)
                ));
            }
            Start::Terminating(detail) => enter(detail),
        }
        owner
    }

    fn assert_concluded(
        owner: &TaskStatusOwner,
        state: TaskState,
        termination: Option<TerminationDetail>,
        context: &str,
    ) {
        let current = owner.current();
        assert_eq!(
            (current.state(), current.termination().cloned()),
            (state, termination),
            "{context}"
        );
        assert!(owner.convergence().conclusion_stable(), "{context}");
        assert_eq!(
            owner.final_info().map(|info| info.final_status().state()),
            Some(state),
            "{context}"
        );
    }

    /// Every starting state against every proposal. A proposal applies only
    /// where the state machine admits it; otherwise the termination already in
    /// progress completes with its own cause.
    #[test]
    fn termination_proposals_complete_the_stand_down_that_won() {
        let first_cancel =
            TerminationFixture::command(canceled(CancelReason::UpstreamNoLongerNeeded));
        let first_abort = TerminationFixture::command(aborted(AbortCause::LeaseExpired));
        let first_failure = resource_fixture();
        let late_cancel =
            TerminationFixture::command(canceled(CancelReason::UpstreamNoLongerNeeded));
        let late_abort = TerminationFixture::command(aborted(AbortCause::PeerTaskFailed));
        let late_failure = internal_fixture();
        let cases = [
            (Start::Planned, late_cancel.clone(), late_cancel.clone()),
            (Start::Planned, late_abort.clone(), late_abort.clone()),
            (Start::Planned, late_failure.clone(), late_failure.clone()),
            (Start::Running, late_cancel.clone(), late_cancel.clone()),
            (Start::Running, late_abort.clone(), late_abort.clone()),
            (Start::Running, late_failure.clone(), late_failure.clone()),
            (
                Start::Terminating(Box::new(first_cancel.clone())),
                late_cancel.clone(),
                first_cancel.clone(),
            ),
            (
                Start::Terminating(Box::new(first_cancel.clone())),
                late_abort.clone(),
                first_cancel.clone(),
            ),
            (
                Start::Terminating(Box::new(first_cancel.clone())),
                late_failure.clone(),
                first_cancel.clone(),
            ),
            (
                Start::Terminating(Box::new(first_abort.clone())),
                late_cancel.clone(),
                first_abort.clone(),
            ),
            (
                Start::Terminating(Box::new(first_abort.clone())),
                late_abort.clone(),
                first_abort.clone(),
            ),
            (
                Start::Terminating(Box::new(first_abort.clone())),
                late_failure.clone(),
                first_abort.clone(),
            ),
            (
                Start::Terminating(Box::new(first_failure.clone())),
                late_cancel,
                first_failure.clone(),
            ),
            (
                Start::Terminating(Box::new(first_failure.clone())),
                late_abort,
                first_failure.clone(),
            ),
            (
                Start::Terminating(Box::new(first_failure.clone())),
                late_failure,
                first_failure,
            ),
        ];
        for (start_state, proposal, winner) in cases {
            let context = format!("{start_state:?} + {proposal:?}");
            let owner = start(&start_state);
            proposal.submit(&owner);
            assert!(
                matches!(
                    owner.conclude_termination(
                        proposal.expected.clone(),
                        TaskOutputFacts::default()
                    ),
                    StatusAdvance::Published(_)
                ),
                "{context}"
            );
            let terminal =
                terminal_state_of(terminating_state_of(&winner.expected)).expect("terminal");
            assert_concluded(&owner, terminal, Some(winner.expected.clone()), &context);
            if let Some(source) = &winner.source {
                assert!(std::ptr::eq(
                    owner
                        .accepted()
                        .unwrap()
                        .originating_failure()
                        .unwrap()
                        .cause(),
                    source.cause()
                ));
            }
            let settled = owner.current();
            assert_eq!(
                owner.conclude_termination(proposal.expected, TaskOutputFacts::default()),
                StatusAdvance::AlreadyTerminal(terminal),
                "{context}: the first terminal wins"
            );
            assert_eq!(owner.current(), settled, "{context}");
        }
    }

    #[test]
    fn forced_logical_convergence_keeps_each_accepted_cause_without_claiming_actual_exit() {
        for winner in [
            TerminationFixture::command(canceled(CancelReason::UpstreamNoLongerNeeded)),
            TerminationFixture::command(aborted(AbortCause::QueryFailed)),
            TerminationFixture::command(aborted(AbortCause::PeerTaskFailed)),
            resource_fixture(),
        ] {
            let owner = start(&Start::Terminating(Box::new(winner.clone())));
            assert!(owner.force_conclusion(AbortCause::LeaseExpired));
            assert_concluded(
                &owner,
                terminal_state_of(terminating_state_of(&winner.expected)).unwrap(),
                Some(winner.expected.clone()),
                "forced convergence keeps acceptance",
            );
            assert!(!owner.convergence().actual_stopped());
            let stable = owner.current();
            assert!(!owner.force_conclusion(AbortCause::QueryFailed));
            assert_eq!(owner.current(), stable);
        }
    }

    #[test]
    fn only_an_originating_failure_refines_a_live_derived_placeholder() {
        let derived = aborted(AbortCause::PeerTaskFailed);
        let owner = start(&Start::Terminating(Box::new(TerminationFixture::command(
            derived.clone(),
        ))));
        for proposal in [
            canceled(CancelReason::UpstreamNoLongerNeeded),
            aborted(AbortCause::LeaseExpired),
        ] {
            assert_eq!(
                owner.advance(
                    terminating_state_of(&proposal),
                    Some(proposal),
                    TaskOutputFacts::default()
                ),
                StatusAdvance::KeptConclusion(TaskState::Aborting)
            );
            assert_eq!(owner.current().termination(), Some(&derived));
        }
        let original = resource_fixture();
        let previous = owner.accepted().unwrap();
        let refined = original.submit(&owner);
        assert_eq!(
            refined.neutral().generation().get(),
            previous.neutral().generation().get() + 1
        );
        let later = internal_fixture();
        let kept = later.submit(&owner);
        assert!(Arc::ptr_eq(&refined, &kept));
        owner.conclude_termination(later.expected, TaskOutputFacts::default());
        assert_concluded(
            &owner,
            TaskState::Failed,
            Some(original.expected),
            "originating refinement only",
        );
        assert!(std::ptr::eq(
            owner
                .accepted()
                .unwrap()
                .originating_failure()
                .unwrap()
                .cause(),
            original.source.unwrap().cause()
        ));
    }

    #[test]
    fn a_termination_conclusion_records_the_sink_output_fact() {
        let owner = start(&Start::Running);
        owner.conclude_termination(
            canceled(CancelReason::UpstreamNoLongerNeeded),
            TaskOutputFacts::new(true),
        );
        assert_eq!(owner.state(), TaskState::Canceled);
        assert!(owner.current().output().responsibility_complete());
    }

    #[test]
    fn success_finishes_only_a_task_that_is_not_terminating() {
        let owner = start(&Start::Running);
        assert!(matches!(
            owner.conclude_success(TaskOutputFacts::new(true)),
            StatusAdvance::Published(_)
        ));
        assert_concluded(&owner, TaskState::Finished, None, "running");
        assert!(owner.current().output().responsibility_complete());

        for winner in [
            TerminationFixture::command(canceled(CancelReason::UpstreamNoLongerNeeded)),
            TerminationFixture::command(aborted(AbortCause::PeerTaskFailed)),
            resource_fixture(),
        ] {
            let context = format!("{winner:?}");
            let owner = start(&Start::Running);
            winner.submit(&owner);
            assert!(
                matches!(
                    owner.conclude_success(TaskOutputFacts::new(true)),
                    StatusAdvance::Published(_)
                ),
                "{context}"
            );
            let terminal =
                terminal_state_of(terminating_state_of(&winner.expected)).expect("terminal");
            assert_concluded(&owner, terminal, Some(winner.expected), &context);
            assert!(
                owner.current().output().responsibility_complete(),
                "{context}: a sink that ran to success keeps that fact"
            );
        }
    }
    #[test]
    fn cleanup_records_borrow_real_sources_without_accepting_or_releasing_work() {
        use crate::TaskDiagnosticRole;
        use novarocks_execution_contract::failure::FailureProjection;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let check = |owner: &Arc<TaskStatusOwner>| {
            let before = owner.current();
            let before_convergence = owner.convergence();
            let accepted = owner.accepted();
            let callbacks = Arc::new(AtomicUsize::new(0));
            let count = callbacks.clone();
            owner.stop_observable().add_observer(Arc::new(move || {
                count.fetch_add(1, Ordering::Release);
            }));
            let dropped = Arc::new(AtomicBool::new(false));
            let source = ExecutionFailure::from_domain(TaskPortDropProbe {
                owner: Arc::downgrade(owner),
                dropped_unlocked: dropped.clone(),
            });
            let fact = source.failure_fact();
            let prepared =
                PreparedCleanupDiagnostic::from_failure(&source, TaskFailurePhase::Execution);
            assert_eq!(
                TaskStatusReporter::new(owner.clone()).report_cleanup(prepared),
                DiagnosticRecordOutcome::Stored
            );
            assert!(
                !dropped.load(Ordering::Acquire),
                "cleanup only borrowed its real source"
            );
            let snapshot = owner.secondary_diagnostics();
            let diagnostic = snapshot.entries().next().unwrap();
            assert_eq!(diagnostic.role(), TaskDiagnosticRole::Cleanup);
            assert_eq!(diagnostic.phase(), TaskFailurePhase::Execution);
            assert_eq!(diagnostic.fact(), fact);
            assert_eq!(diagnostic.detail(), source.safe_detail().as_str());
            assert!(!diagnostic.detail().contains("private resource diagnostic"));
            drop(source);
            assert!(
                dropped.load(Ordering::Acquire),
                "the neutral snapshot cannot retain the real source"
            );
            assert_eq!(owner.current(), before);
            assert_eq!(owner.convergence(), before_convergence);
            assert_eq!(callbacks.load(Ordering::Acquire), 0);
            match accepted {
                Some(accepted) => assert!(Arc::ptr_eq(&accepted, &owner.accepted().unwrap())),
                None => assert!(owner.accepted().is_none()),
            }
        };

        let empty = Arc::new(owner());
        check(&empty);
        assert!(
            !empty.is_stopped(),
            "cleanup cannot manufacture a Task stop"
        );

        let peer = Arc::new(owner());
        let control = peer.execution_control(TaskFailurePhase::Preparation);
        peer.submit(TaskConclusionProposal::Aborted(AbortCause::PeerTaskFailed));
        let receipt = peer.accepted().unwrap();
        check(&peer);
        for _ in 0..32 {
            let observation = control.error().unwrap();
            let before_kept = control.accepted();
            control.report_failure(observation);
            let kept = control.accepted().unwrap();
            assert!(Arc::ptr_eq(&before_kept.unwrap(), &kept));
            assert!(Arc::ptr_eq(&receipt, &kept));
        }
        assert_eq!(peer.secondary_diagnostics().entries().len(), 1);
        assert_eq!(
            peer.secondary_diagnostics().dropped(),
            0,
            "stop observations are not late origin reports"
        );
        let origin = ExecutionFailure::from(novarocks_functions::KernelFailure::ResourceExhausted);
        let before_refined = control.accepted();
        control.report_failure(origin.clone());
        let refined = control.accepted().unwrap();
        assert_eq!(
            refined.neutral().generation().get(),
            before_refined.unwrap().neutral().generation().get() + 1
        );
        assert_eq!(refined.neutral().generation().get(), 2);
        assert!(std::ptr::eq(
            refined.originating_failure().unwrap().cause(),
            origin.cause()
        ));
        assert_eq!(
            peer.secondary_diagnostics().entries().len(),
            1,
            "legal refinement is not secondary"
        );

        let canceled = Arc::new(owner());
        canceled.submit(TaskConclusionProposal::Canceled(
            CancelReason::UpstreamNoLongerNeeded,
        ));
        check(&canceled);

        let succeeded = Arc::new(start(&Start::Running));
        succeeded.note_actual_stopped();
        succeeded.conclude_success(TaskOutputFacts::new(true));
        assert_eq!(
            succeeded.accepted().unwrap().neutral().conclusion(),
            &TaskConclusion::Succeeded
        );
        check(&succeeded);
    }

    #[test]
    fn late_and_cleanup_reports_share_four_slots_without_changing_the_winner() {
        use crate::TaskDiagnosticRole;
        use novarocks_execution_contract::failure::FailureProjection;
        use std::sync::atomic::{AtomicUsize, Ordering};
        let owner = Arc::new(owner());
        owner.release_to_observers();
        owner.submit(TaskConclusionProposal::Canceled(
            CancelReason::UpstreamNoLongerNeeded,
        ));
        let first = owner.accepted().unwrap();
        let stable = owner.current();
        let convergence = owner.convergence();
        let callbacks = Arc::new(AtomicUsize::new(0));
        let count = callbacks.clone();
        owner.stop_observable().add_observer(Arc::new(move || {
            count.fetch_add(1, Ordering::Release);
        }));
        let reporter = TaskStatusReporter::new(owner.clone());
        for index in 0..6 {
            let source =
                ExecutionFailure::from(novarocks_functions::KernelFailure::ResourceExhausted);
            if index % 2 == 0 {
                let expected = if index < 4 {
                    DiagnosticRecordOutcome::Stored
                } else {
                    DiagnosticRecordOutcome::Dropped
                };
                assert_eq!(
                    reporter.report_cleanup(PreparedCleanupDiagnostic::from_failure(
                        &source,
                        TaskFailurePhase::Execution,
                    )),
                    expected
                );
            } else {
                owner
                    .execution_control(TaskFailurePhase::Preparation)
                    .report_failure(source);
                let kept = owner.accepted().unwrap();
                assert!(Arc::ptr_eq(&first, &kept));
            }
        }
        let snapshot = reporter.secondary_diagnostics();
        assert_eq!(snapshot.entries().len(), 4);
        assert_eq!(snapshot.dropped(), 2);
        let expected_fact =
            ExecutionFailure::from(novarocks_functions::KernelFailure::ResourceExhausted)
                .failure_fact();
        for (index, diagnostic) in snapshot.entries().enumerate() {
            assert_eq!(diagnostic.fact(), expected_fact);
            assert_eq!(
                diagnostic.role(),
                if index % 2 == 0 {
                    TaskDiagnosticRole::Cleanup
                } else {
                    TaskDiagnosticRole::LateFailure
                }
            );
            assert_eq!(
                diagnostic.phase(),
                if index % 2 == 0 {
                    TaskFailurePhase::Execution
                } else {
                    TaskFailurePhase::Preparation
                }
            );
        }
        assert_eq!(owner.current(), stable);
        assert_eq!(owner.convergence(), convergence);
        assert_eq!(callbacks.load(Ordering::Acquire), 0);
        assert!(Arc::ptr_eq(&first, &owner.accepted().unwrap()));
    }

    #[test]
    fn concurrent_cleanup_observations_only_consume_the_same_bounded_local_journal() {
        let owner = Arc::new(owner());
        owner.submit(TaskConclusionProposal::Canceled(
            CancelReason::UpstreamNoLongerNeeded,
        ));
        let accepted = owner.accepted().unwrap();
        let stable = owner.current();
        let convergence = owner.convergence();
        let jobs = (0..16)
            .map(|_| {
                let reporter = TaskStatusReporter::new(owner.clone());
                std::thread::spawn(move || {
                    let cause = ExecutionFailure::from(
                        novarocks_functions::KernelFailure::ResourceExhausted,
                    );
                    let prepared = PreparedCleanupDiagnostic::from_failure(
                        &cause,
                        TaskFailurePhase::Execution,
                    );
                    reporter.report_cleanup(prepared)
                })
            })
            .collect::<Vec<_>>();
        let stored = jobs
            .into_iter()
            .map(|job| job.join().unwrap())
            .filter(|outcome| *outcome == DiagnosticRecordOutcome::Stored)
            .count();
        assert_eq!(stored, 4);
        let snapshot = owner.secondary_diagnostics();
        assert_eq!(snapshot.entries().len(), 4);
        assert_eq!(snapshot.dropped(), 12);
        assert_eq!(owner.current(), stable);
        assert_eq!(owner.convergence(), convergence);
        assert!(Arc::ptr_eq(&accepted, &owner.accepted().unwrap()));
    }
}

#[cfg(test)]
mod publication_boundary_tests {
    use super::*;
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    use std::{
        future::Future,
        sync::atomic::{AtomicBool, AtomicUsize, Ordering},
        task::{Context, Poll, Wake, Waker},
    };

    struct ReentrantObserver {
        owner: Arc<TaskStatusOwner>,
        wakes: AtomicUsize,
        locked: AtomicBool,
    }
    impl Wake for ReentrantObserver {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.wakes.fetch_add(1, Ordering::Relaxed);
            // A notifier is allowed to synchronously schedule a callback.
            // Check the lock without risking a deadlocked test on regression.
            if self.owner.state.try_lock().is_err() {
                self.locked.store(true, Ordering::Release);
            }
        }
    }
    #[test]
    fn creation_lifecycle_and_metric_notifications_run_after_the_owner_unlocks() {
        let execution =
            QueryExecutionId::new(QueryId::new(3902, 1), AttemptId::new(1).unwrap()).unwrap();
        let identity = TaskIdentity::new(
            execution,
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            BackendProcessId::new_v7(),
        );
        let source = Arc::new(TaskStatusSource::new());
        let owner = Arc::new(TaskStatusOwner::new(
            identity,
            source.clone(),
            Arc::new(crate::ManualClock::new()),
            Duration::ZERO,
            Arc::new(novarocks_execution::runtime::execution_health::ExecutionHealth::default()),
        ));
        let observer = Arc::new(ReentrantObserver {
            owner: owner.clone(),
            wakes: AtomicUsize::new(0),
            locked: AtomicBool::new(false),
        });
        // Installation is required before RUNNING and is still withheld from
        // observers until this real creation transaction commits.
        assert!(matches!(
            owner.note_installed(),
            StatusAdvance::Published(_)
        ));
        let waker = Waker::from(observer.clone());
        let mut context = Context::from_waker(&waker);
        let mut pending = Box::pin(source.next_task_event_owned());
        assert!(matches!(pending.as_mut().poll(&mut context), Poll::Pending));
        owner.release_to_observers();
        assert!(matches!(
            pending.as_mut().poll(&mut context),
            Poll::Ready(Some(_))
        ));
        for _ in 0..2 {
            let mut pending = Box::pin(source.next_task_event_owned());
            assert!(matches!(pending.as_mut().poll(&mut context), Poll::Pending));
            if owner.state() == TaskState::Planned {
                assert!(matches!(
                    owner.advance(TaskState::Running, None, TaskOutputFacts::default()),
                    StatusAdvance::Published(_)
                ));
            } else {
                assert!(matches!(
                    owner.report_metrics(TaskResourceFacts::empty(), None),
                    StatusAdvance::Republished(_)
                ));
            }
            assert!(matches!(
                pending.as_mut().poll(&mut context),
                Poll::Ready(Some(_))
            ));
        }
        assert_eq!(observer.wakes.load(Ordering::Acquire), 3);
        assert!(!observer.locked.load(Ordering::Acquire));
    }
}

#[cfg(test)]
mod exit_health_port_tests {
    use super::*;
    use novarocks_execution::runtime::execution_health::ExecutionExitOrigin;
    use novarocks_execution_contract::failure::FailureProjection;

    #[test]
    fn execution_control_reports_to_the_injected_health_without_refining_peer_or_stopping_work() {
        let identity = TaskIdentity::new(
            novarocks_types::QueryExecutionId::new(
                novarocks_types::QueryId::new(5101, 5102),
                novarocks_types::AttemptId::new(1).unwrap(),
            )
            .unwrap(),
            novarocks_types::StageId::new(1).unwrap(),
            novarocks_types::TaskId::new(1).unwrap(),
            novarocks_types::BackendProcessId::new_v7(),
        );
        let health =
            Arc::new(novarocks_execution::runtime::execution_health::ExecutionHealth::default());
        let owner = Arc::new(TaskStatusOwner::new(
            identity,
            Arc::new(TaskStatusSource::new()),
            Arc::new(crate::ManualClock::new()),
            Duration::ZERO,
            health.clone(),
        ));
        owner.submit(TaskConclusionProposal::Aborted(AbortCause::PeerTaskFailed));
        let accepted = owner.accepted().unwrap();
        let before = owner.current();
        let convergence = owner.convergence();
        let cause = ExecutionFailure::from(novarocks_functions::KernelFailure::ResourceExhausted);
        let control = owner.execution_control(TaskFailurePhase::Execution);
        control.report_cleanup(&cause);
        control.report_unresolved_exit(&cause);
        let fault = health.failure().unwrap();
        assert_eq!(
            fault.origin(),
            ExecutionExitOrigin::Task {
                identity,
                phase: TaskFailurePhase::Execution
            }
        );
        assert_eq!(fault.fact(), cause.failure_fact());
        assert_eq!(owner.current(), before);
        assert_eq!(owner.convergence(), convergence);
        assert!(Arc::ptr_eq(&accepted, &owner.accepted().unwrap()));
        assert!(owner.accepted().unwrap().originating_failure().is_none());
        let diagnostics = owner.secondary_diagnostics();
        assert_eq!(diagnostics.entries().len(), 1);
        assert_eq!(
            diagnostics.entries().next().unwrap().role(),
            crate::TaskDiagnosticRole::Cleanup
        );
        let before_refined = control.accepted();
        control.report_failure(cause);
        let refined = control.accepted().unwrap();
        assert_eq!(
            refined.neutral().generation().get(),
            before_refined.unwrap().neutral().generation().get() + 1
        );
    }
}

#[cfg(test)]
mod preparation_runtime_receipt_tests {
    use super::*;
    use crate::task_preparation::PreparationUnwindRole;
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct PrimaryDrop {
        data: Arc<Vec<u8>>,
        entered: std::sync::mpsc::Sender<()>,
        release: std::sync::mpsc::Receiver<()>,
        secondary: bool,
        secondary_drops: Arc<AtomicUsize>,
    }
    struct SecondaryDrop {
        _data: Arc<Vec<u8>>,
        drops: Arc<AtomicUsize>,
    }
    impl Drop for SecondaryDrop {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::AcqRel);
        }
    }
    impl Drop for PrimaryDrop {
        fn drop(&mut self) {
            self.entered.send(()).unwrap();
            self.release.recv().unwrap();
            if self.secondary {
                std::panic::panic_any(SecondaryDrop {
                    _data: self.data.clone(),
                    drops: self.secondary_drops.clone(),
                });
            }
        }
    }
    fn actual_runtime_receipt_during_primary_drop(secondary: bool) {
        let execution =
            QueryExecutionId::new(QueryId::new(59_230, 1), AttemptId::new(1).unwrap()).unwrap();
        let owner = Arc::new(TaskStatusOwner::new(
            TaskIdentity::new(
                execution,
                StageId::new(1).unwrap(),
                TaskId::new(1).unwrap(),
                BackendProcessId::new_v7(),
            ),
            Arc::new(TaskStatusSource::new()),
            Arc::new(crate::ManualClock::new()),
            Duration::ZERO,
            Arc::new(ExecutionHealth::default()),
        ));
        owner.release_to_observers();
        let scope = owner.preparation_scope_arc();
        let data = Arc::new(vec![93u8; 16 * 1024]);
        let weak_body_data = Arc::downgrade(&data);
        let secondary_drops = Arc::new(AtomicUsize::new(0));
        let (entered, did_enter) = std::sync::mpsc::channel();
        let (release, released) = std::sync::mpsc::channel();
        scope.retain_body_panic(
            PreparationUnwindRole::Preparation,
            Box::new(PrimaryDrop {
                data,
                entered,
                release: released,
                secondary,
                secondary_drops: secondary_drops.clone(),
            }),
        );
        let disposer_scope = scope.clone();
        let disposer = std::thread::spawn(move || {
            disposer_scope.dispose_body_panic(PreparationUnwindRole::Preparation)
        });
        did_enter.recv().unwrap();
        let runtime_data = Arc::new(vec![94u8; 16 * 1024]);
        let weak_runtime_data = Arc::downgrade(&runtime_data);
        let reporter = TaskStatusReporter::new(owner.clone());
        let runtime = std::thread::spawn(move || {
            drop(runtime_data);
            reporter.note_actual_stopped();
            reporter.note_resources_converged();
            drop(reporter);
        });
        // Always release and join both real jobs before reporting an oracle
        // failure, so a failed receipt assertion cannot strand their gate.
        let runtime_result = runtime.join();
        let pending = owner.convergence();
        let receipt_retained = owner.runtime_actual_stopped_received();
        let no_public_receipt = owner
            .source
            .latest_task_convergence(owner.identity())
            .is_none();
        release.send(()).unwrap();
        let disposer_result = disposer.join();
        scope.body_exited();
        let finished = scope.complete_empty_after_actual_runtime_exit(
            owner.runtime_actual_stopped_received(),
            || owner.refresh_preparation_exit_receipts(),
        );
        assert!(runtime_result.is_ok() && disposer_result.is_ok());
        assert!(
            weak_runtime_data.upgrade().is_none(),
            "actual runtime data exited before its original receipt"
        );
        assert!(receipt_retained && no_public_receipt);
        assert!(
            !pending.actual_stopped() && !pending.resources_converged(),
            "Started opaque disposal is Pending rather than TaskStopped"
        );
        assert_eq!(secondary_drops.load(Ordering::Acquire), 0);
        if secondary {
            assert!(!finished);
            assert!(weak_body_data.upgrade().is_some());
            assert!(
                !owner.convergence().actual_stopped() && !owner.convergence().resources_converged()
            );
            assert!(
                owner
                    .source
                    .latest_task_convergence(owner.identity())
                    .is_none()
            );
            assert_eq!(
                owner
                    .execution_health
                    .failure()
                    .unwrap()
                    .fact()
                    .reason()
                    .code(),
                161
            );
        } else {
            assert!(finished);
            assert!(weak_body_data.upgrade().is_none());
            assert!(
                owner.convergence().actual_stopped() && owner.convergence().resources_converged()
            );
            assert!(
                owner
                    .source
                    .latest_task_convergence(owner.identity())
                    .is_some(),
                "original prep finish publishes the retained real receipt without replaying runtime cleanup"
            );
            assert!(owner.execution_health.is_healthy());
        }
    }
    #[test]
    fn a_real_runtime_receipt_waits_for_primary_payload_drop_then_publishes_once() {
        actual_runtime_receipt_during_primary_drop(false);
    }
    #[test]
    fn a_real_runtime_receipt_remains_retained_but_unpublished_after_secondary_drop_unresolved() {
        actual_runtime_receipt_during_primary_drop(true);
    }
}

#[cfg(test)]
mod preparation_body_phase_tests {
    use super::*;
    use novarocks_execution_contract::failure::{FailureFact, FailureProjection};
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    use std::sync::{Barrier, Weak};

    fn owner() -> Arc<TaskStatusOwner> {
        Arc::new(TaskStatusOwner::new(
            TaskIdentity::new(
                QueryExecutionId::new(QueryId::new(91, 147), AttemptId::new(1).unwrap()).unwrap(),
                StageId::new(1).unwrap(),
                TaskId::new(1).unwrap(),
                BackendProcessId::new_v7(),
            ),
            Arc::new(TaskStatusSource::new()),
            Arc::new(crate::ManualClock::new()),
            Duration::ZERO,
            Arc::new(ExecutionHealth::default()),
        ))
    }
    fn source() -> ExecutionFailure {
        ExecutionFailure::from(novarocks_functions::KernelFailure::ResourceExhausted)
    }
    fn assert_original_failure(
        owner: &TaskStatusOwner,
        installed: bool,
        expected_phase: TaskFailurePhase,
        expected: FailureFact,
    ) {
        let status = owner.current();
        assert_eq!(status.installed(), installed);
        assert_eq!(status.state(), TaskState::Failing);
        let Some(TerminationDetail::Failed(failure)) = status.termination() else {
            panic!("the original body failure remains its only conclusion")
        };
        assert_eq!(failure.phase(), expected_phase);
        assert_eq!(failure.fact(), expected);
        let accepted = owner.accepted().unwrap();
        assert_eq!(
            accepted.originating_failure().unwrap().failure_fact(),
            expected
        );
        let TaskConclusion::Terminated(TerminationDetail::Failed(failure)) =
            accepted.neutral().conclusion()
        else {
            panic!("the same Task accepts the original failure")
        };
        assert_eq!(failure.phase(), expected_phase);
        assert_eq!(failure.fact(), expected);
        // Neither lifecycle phase nor logical failure invents actual runtime exit.
        assert!(!owner.runtime_actual_stopped_received());
        assert!(!owner.convergence().actual_stopped());
        assert!(!owner.convergence().resources_converged());
    }
    #[test]
    fn original_body_failure_before_installation_keeps_preparation_and_blocks_late_installed() {
        let owner = owner();
        let original = source();
        let fact = original.failure_fact();
        let pointer = original.cause() as *const _;
        let before_accepted = owner.accepted();
        owner.report_preparation_body_failure(original);
        let accepted = owner.accepted().unwrap();
        assert!(before_accepted.is_none());
        assert_eq!(accepted.neutral().generation().get(), 1);
        assert_eq!(
            accepted.originating_failure().unwrap().cause() as *const _,
            pointer
        );
        let before = owner.current();
        // This is the real setter a caller can enter after its stale stop read.
        assert_eq!(
            owner.note_installed(),
            StatusAdvance::KeptConclusion(TaskState::Failing)
        );
        assert_eq!(owner.current(), before);
        assert_original_failure(&owner, false, TaskFailurePhase::Preparation, fact);
    }
    #[test]
    fn original_installed_before_body_failure_selects_execution_without_running_the_runtime() {
        let owner = owner();
        assert!(matches!(
            owner.note_installed(),
            StatusAdvance::Published(_)
        ));
        assert_eq!(owner.current().state(), TaskState::Planned);
        let original = source();
        let fact = original.failure_fact();
        let pointer = original.cause() as *const _;
        let before_accepted = owner.accepted();
        owner.report_preparation_body_failure(original);
        let accepted = owner.accepted().unwrap();
        assert!(before_accepted.is_none());
        assert_eq!(accepted.neutral().generation().get(), 1);
        assert_eq!(
            accepted.originating_failure().unwrap().cause() as *const _,
            pointer
        );
        assert_original_failure(&owner, true, TaskFailurePhase::Execution, fact);
        // Same-identity Installed replay retains its irreversible historical fact.
        assert_eq!(
            owner.note_installed(),
            StatusAdvance::Published(owner.current().version())
        );
    }
    #[test]
    fn concurrent_original_installed_and_body_failure_have_exactly_one_legal_linearization() {
        for _ in 0..64 {
            let owner = owner();
            let start = Arc::new(Barrier::new(2));
            let installer = owner.clone();
            let installer_start = start.clone();
            let installed = std::thread::spawn(move || {
                installer_start.wait();
                installer.note_installed()
            });
            let reporter = owner.clone();
            let original = source();
            let fact = original.failure_fact();
            let accepted = std::thread::spawn(move || {
                start.wait();
                reporter.report_preparation_body_failure(original);
                reporter.accepted().unwrap()
            });
            let installation = installed
                .join()
                .expect("the actual installation setter exits");
            let accepted = accepted.join().expect("the actual failure author exits");
            assert_eq!(accepted.neutral().generation().get(), 1);
            match installation {
                StatusAdvance::Published(_) => {
                    assert_original_failure(&owner, true, TaskFailurePhase::Execution, fact)
                }
                StatusAdvance::KeptConclusion(TaskState::Failing) => {
                    assert_original_failure(&owner, false, TaskFailurePhase::Preparation, fact)
                }
                other => panic!("unexpected actual installation result: {other:?}"),
            }
        }
    }
    #[test]
    fn earlier_cancellation_and_peer_placeholder_keep_the_original_priority_rules() {
        for installed in [false, true] {
            for peer in [false, true] {
                let owner = owner();
                if installed {
                    owner.note_installed();
                }
                if peer {
                    owner.submit(TaskConclusionProposal::Aborted(AbortCause::PeerTaskFailed))
                } else {
                    owner.submit(TaskConclusionProposal::Canceled(
                        CancelReason::UpstreamNoLongerNeeded,
                    ))
                };
                let first = owner.accepted().unwrap();
                let stable = owner.current();
                let original = source();
                let fact = original.failure_fact();
                let phase = if installed {
                    TaskFailurePhase::Execution
                } else {
                    TaskFailurePhase::Preparation
                };
                let before_late = owner.accepted();
                owner.report_preparation_body_failure(original);
                let late = owner.accepted().unwrap();
                if peer {
                    assert_eq!(
                        late.neutral().generation().get(),
                        before_late.unwrap().neutral().generation().get() + 1
                    );
                    assert_original_failure(&owner, installed, phase, fact);
                } else {
                    assert!(Arc::ptr_eq(&before_late.unwrap(), &late));
                    assert!(Arc::ptr_eq(&first, &late));
                    assert_eq!(owner.current(), stable);
                    let diagnostics = owner.secondary_diagnostics();
                    assert_eq!(diagnostics.entries().len(), 1);
                    let late = diagnostics.entries().next().unwrap();
                    assert_eq!(late.phase(), phase);
                    assert_eq!(late.fact(), fact);
                }
            }
        }
    }

    #[derive(Debug)]
    struct DropProbe {
        owner: Weak<TaskStatusOwner>,
        sample: Arc<Mutex<Option<bool>>>,
    }
    impl fmt::Display for DropProbe {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("actual losing source disposal probe")
        }
    }
    impl std::error::Error for DropProbe {}
    impl FailureProjection for DropProbe {
        fn failure_fact(&self) -> FailureFact {
            source().failure_fact()
        }
    }
    impl Drop for DropProbe {
        fn drop(&mut self) {
            let unlocked = self.owner.upgrade().unwrap().state.try_lock().is_ok();
            *self.sample.lock().unwrap() = Some(unlocked);
        }
    }
    #[test]
    fn unselected_and_losing_rich_candidates_destroy_their_original_source_after_unlock() {
        for installed in [false, true] {
            let owner = owner();
            if installed {
                owner.note_installed();
            }
            owner.submit(TaskConclusionProposal::Canceled(
                CancelReason::UpstreamNoLongerNeeded,
            ));
            let original = owner.accepted().unwrap();
            let sample = Arc::new(Mutex::new(None));
            let before_late = owner.accepted();
            owner.report_preparation_body_failure(ExecutionFailure::from_domain(DropProbe {
                owner: Arc::downgrade(&owner),
                sample: sample.clone(),
            }));
            let late = owner.accepted().unwrap();
            assert!(Arc::ptr_eq(&before_late.unwrap(), &late));
            assert!(Arc::ptr_eq(&original, &late));
            assert_eq!(*sample.lock().unwrap(), Some(true));
        }
    }
}
