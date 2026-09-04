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

//! Pure transitions: task states, the query context lifecycle, first-wins
//! termination, derived stage state, and the read and write completion
//! predicates.
//!
//! Everything here is a total function over values. Nothing reads a clock,
//! touches shared state, or knows which role it runs in, so the frontend and
//! the backend can classify the same fact identically without sharing a
//! registry, a timer, or a transition owner.

use std::fmt;

use crate::task_execution::status::{TaskState, TaskStatus, TerminationDetail};

/// How a backend must treat one proposed task state change.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum TaskTransition {
    /// A legal move: publish a new version.
    Apply,
    /// The same state again, which a metric-only update republishes at a new
    /// version.
    SameState,
    /// The task is already terminal. The first terminal wins and is never
    /// replaced.
    AlreadyTerminal,
    /// The move is not in the state machine.
    Illegal,
}

/// Classifies a proposed task state change.
pub fn classify_task_transition(from: TaskState, to: TaskState) -> TaskTransition {
    if from == to {
        return TaskTransition::SameState;
    }
    if from.is_terminal() {
        return TaskTransition::AlreadyTerminal;
    }
    let legal = match from {
        TaskState::Planned => matches!(
            to,
            TaskState::Running | TaskState::Canceling | TaskState::Aborting | TaskState::Failing
        ),
        // A task may reach `FINISHED` from `RUNNING` without publishing a
        // separate `FLUSHING` version. That is safe because a `FINISHED`
        // snapshot is only constructible once this task's output
        // responsibility is complete, so skipping the state cannot skip the
        // obligation.
        TaskState::Running => matches!(
            to,
            TaskState::Flushing
                | TaskState::Finished
                | TaskState::Canceling
                | TaskState::Aborting
                | TaskState::Failing
        ),
        TaskState::Flushing => matches!(
            to,
            TaskState::Finished | TaskState::Canceling | TaskState::Aborting | TaskState::Failing
        ),
        // A normal stand-down may still be escalated by a query failure or by
        // this task's own error.
        TaskState::Canceling => matches!(
            to,
            TaskState::Canceled | TaskState::Aborting | TaskState::Failing
        ),
        // Forced termination is already the strongest outcome, so it only
        // completes.
        TaskState::Aborting => matches!(to, TaskState::Aborted),
        TaskState::Failing => matches!(to, TaskState::Failed),
        TaskState::Finished | TaskState::Canceled | TaskState::Aborted | TaskState::Failed => false,
    };
    if legal {
        TaskTransition::Apply
    } else {
        TaskTransition::Illegal
    }
}

/// What an end-of-stream delivery on the root result plane means for the root
/// task's own status.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum RootDrainAction {
    /// Execution completed and the result stream is drained: this is the one
    /// moment a root task's output responsibility becomes complete, so publish
    /// `FINISHED`.
    Finish,
    /// The task is already terminating for another reason. Record that the
    /// output drained and publish it, but never turn a termination into a
    /// success.
    RecordOnly,
    /// The task is already terminal. The first terminal wins.
    AlreadyTerminal,
    /// A task that never started running cannot have produced a complete
    /// result stream. Fail loudly instead of inventing a `FINISHED`.
    Illegal,
}

/// Classifies an end-of-stream delivery against the root task's state.
///
/// This is what keeps the result data plane from becoming a second terminal
/// authority: end-of-stream is evidence about *output*, and it may only
/// complete a task that was still executing normally.
pub const fn classify_root_drain(state: TaskState) -> RootDrainAction {
    match state {
        TaskState::Running | TaskState::Flushing => RootDrainAction::Finish,
        TaskState::Canceling | TaskState::Aborting | TaskState::Failing => {
            RootDrainAction::RecordOnly
        }
        TaskState::Finished | TaskState::Canceled | TaskState::Aborted | TaskState::Failed => {
            RootDrainAction::AlreadyTerminal
        }
        TaskState::Planned => RootDrainAction::Illegal,
    }
}

/// A first-wins termination latch.
///
/// Explicit abort, lease expiry, and a task failure all race for the same
/// position. Exactly one of them wins, and the winner is the cause every
/// observer sees: a later reason never rewrites it, and the fan-out,
/// capability revocation, and resource cleanup run exactly once.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TerminationLatch {
    first: Option<TerminationDetail>,
}

/// The outcome of racing for a termination latch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LatchOutcome {
    /// This cause won and must drive termination.
    Won,
    /// Another cause already won; this one is reported but changes nothing.
    Lost(TerminationDetail),
    /// This cause replaced a derived placeholder. The attempt still terminates
    /// for the reason it already began terminating for, so this must not
    /// re-run the side effects of `Won`; only the reported cause improves.
    Refined(TerminationDetail),
}

impl TerminationLatch {
    pub const fn open() -> Self {
        Self { first: None }
    }

    pub const fn cause(&self) -> Option<&TerminationDetail> {
        self.first.as_ref()
    }

    pub const fn is_latched(&self) -> bool {
        self.first.is_some()
    }

    /// Races `cause` for the latch, reporting who won.
    ///
    /// One exception to first-wins: a derived cause is a placeholder, and an
    /// originating cause replaces it. Without that, a query context's
    /// "another task failed" -- which arrives first precisely because it is a
    /// reaction to the failure -- becomes the only thing the client is ever
    /// told, and the failing task's own message is dropped. The latch still
    /// holds exactly one cause; it holds the best one it has been offered.
    pub fn latch(&mut self, cause: TerminationDetail) -> LatchOutcome {
        match &self.first {
            Some(existing) if existing.is_derived() && !cause.is_derived() => {
                let replaced = existing.clone();
                self.first = Some(cause);
                LatchOutcome::Refined(replaced)
            }
            Some(existing) => LatchOutcome::Lost(existing.clone()),
            None => {
                self.first = Some(cause);
                LatchOutcome::Won
            }
        }
    }
}

/// Lifecycle state of one query context on one backend.
///
/// The context owns shared resources and the application lease; it never owns
/// a task's terminal outcome. There is deliberately no event meaning "every
/// task I currently know about is terminal": a backend must never guess that
/// its local task set is closed, because a legal `CreateTask` may still be in
/// flight.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum QueryContextState {
    #[default]
    Absent,
    Establishing,
    Active,
    Releasing,
    Aborting,
    TerminalRetained,
    Gone,
}

impl QueryContextState {
    pub const fn is_closed(self) -> bool {
        matches!(
            self,
            Self::Releasing | Self::Aborting | Self::TerminalRetained | Self::Gone
        )
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Absent => "ABSENT",
            Self::Establishing => "ESTABLISHING",
            Self::Active => "ACTIVE",
            Self::Releasing => "RELEASING",
            Self::Aborting => "ABORTING",
            Self::TerminalRetained => "TERMINAL_RETAINED",
            Self::Gone => "GONE",
        }
    }
}

impl fmt::Display for QueryContextState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// A lifecycle event of one query context.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum QueryContextEvent {
    /// An establish request won the creation gate.
    Establish,
    /// Shared domain materialization finished inside the initial lease.
    EstablishCompleted,
    /// The owner declared that no legal create can follow and every local
    /// task and output responsibility has drained.
    Release,
    /// Shared resources were released.
    ReleaseCompleted,
    /// An explicit abort, a lease expiry, or a task failure is terminating
    /// this context.
    Abort,
    /// Abort cleanup finished.
    AbortCompleted,
    /// Retention elapsed past the maximum legal request horizon.
    Reap,
}

/// How a backend must treat one query context lifecycle event.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ContextTransition {
    /// A legal move to this state.
    Apply(QueryContextState),
    /// The event has already been applied; report the retained receipt.
    Idempotent,
    /// Normal closure already won the race, so this abort only receives the
    /// normal terminal receipt and changes nothing.
    LostToRelease,
    /// The context is already terminal; report the retained cause.
    AlreadyTerminal,
    /// The event is not in the state machine.
    Illegal,
}

/// Classifies a query context lifecycle event.
pub fn classify_context_transition(
    state: QueryContextState,
    event: QueryContextEvent,
) -> ContextTransition {
    use QueryContextEvent as Event;
    use QueryContextState as State;

    match (state, event) {
        (State::Absent, Event::Establish) => ContextTransition::Apply(State::Establishing),
        // A pre-establish fence: aborting an absent context stops a late but
        // still legal establish or create from reviving the execution.
        (State::Absent, Event::Abort) => ContextTransition::Apply(State::TerminalRetained),

        (State::Establishing, Event::EstablishCompleted) => ContextTransition::Apply(State::Active),
        (State::Establishing, Event::Abort) => ContextTransition::Apply(State::Aborting),
        (State::Establishing, Event::Establish) => ContextTransition::Idempotent,

        (State::Active, Event::Release) => ContextTransition::Apply(State::Releasing),
        (State::Active, Event::Abort) => ContextTransition::Apply(State::Aborting),
        (State::Active, Event::Establish | Event::EstablishCompleted) => {
            ContextTransition::Idempotent
        }

        (State::Releasing, Event::ReleaseCompleted) => {
            ContextTransition::Apply(State::TerminalRetained)
        }
        (State::Releasing, Event::Release) => ContextTransition::Idempotent,
        (State::Releasing, Event::Abort) => ContextTransition::LostToRelease,

        (State::Aborting, Event::AbortCompleted) => {
            ContextTransition::Apply(State::TerminalRetained)
        }
        (State::Aborting, Event::Abort) => ContextTransition::Idempotent,

        (State::TerminalRetained, Event::Reap) => ContextTransition::Apply(State::Gone),
        (State::TerminalRetained, _) => ContextTransition::AlreadyTerminal,
        (State::Gone, _) => ContextTransition::AlreadyTerminal,

        _ => ContextTransition::Illegal,
    }
}

/// What kind of request is asking to be applied against a query context.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ContextOperationKind {
    /// A task creation, which may wait for the exact creation gate.
    CreateTask,
    /// A shared domain advance.
    AdvanceDomain,
    /// A lease renewal.
    RenewLease,
}

/// Whether a request may be applied.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum OperationAdmission {
    /// Apply it now.
    Admit,
    /// Wait for the exact creation gate, bounded by the operation's own
    /// deadline. Establishing may still fail, in which case the waiter fails
    /// with it.
    WaitForCreationGate,
    /// The context never existed and carries no retirement fence, so this is a
    /// fatal protocol error rather than something to wait for.
    NotEstablished,
    /// The context is closed. Nothing is applied and a terminal receipt is
    /// returned; this is not fatal on its own.
    TerminalReceipt,
    /// Retention elapsed. Inside the legal request horizon this means the
    /// attempt's integrity cannot be proven.
    Gone,
}

/// Classifies whether a request may be applied to a query context.
pub fn classify_operation_admission(
    state: QueryContextState,
    kind: ContextOperationKind,
) -> OperationAdmission {
    match state {
        // A create may legitimately arrive before its establish: the two are
        // sent concurrently, so the create waits for the gate rather than
        // forcing the frontend to serialize them.
        QueryContextState::Absent => match kind {
            ContextOperationKind::CreateTask => OperationAdmission::WaitForCreationGate,
            _ => OperationAdmission::NotEstablished,
        },
        QueryContextState::Establishing => match kind {
            ContextOperationKind::CreateTask => OperationAdmission::WaitForCreationGate,
            _ => OperationAdmission::NotEstablished,
        },
        QueryContextState::Active => OperationAdmission::Admit,
        QueryContextState::Releasing
        | QueryContextState::Aborting
        | QueryContextState::TerminalRetained => OperationAdmission::TerminalReceipt,
        QueryContextState::Gone => OperationAdmission::Gone,
    }
}

/// Derived aggregate state of one stage.
///
/// This is the frontend's observation over a stage's complete, frozen task
/// set. It is never a backend or wire authority.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum StageState {
    Running,
    Flushing,
    Finished,
    Canceled,
    Aborting,
    Aborted,
    Failed,
}

impl StageState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Running => "RUNNING",
            Self::Flushing => "FLUSHING",
            Self::Finished => "FINISHED",
            Self::Canceled => "CANCELED",
            Self::Aborting => "ABORTING",
            Self::Aborted => "ABORTED",
            Self::Failed => "FAILED",
        }
    }
}

impl fmt::Display for StageState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Derives a stage's state from its task states.
///
/// `scheduling_complete` means the stage's task set is frozen and every task
/// has been created. Until then no terminal or flushing aggregate may be
/// derived, because a task that does not exist yet cannot be observed. A
/// failure or an abort still propagates immediately: a stage does not have to
/// finish being built to be already lost.
///
/// Returns `None` for an empty task set, which is never a legal stage.
pub fn derive_stage_state(scheduling_complete: bool, tasks: &[TaskState]) -> Option<StageState> {
    if tasks.is_empty() {
        return None;
    }
    if tasks.contains(&TaskState::Failed) {
        return Some(StageState::Failed);
    }
    if tasks.contains(&TaskState::Aborting) {
        return Some(StageState::Aborting);
    }
    if tasks.contains(&TaskState::Aborted) {
        return Some(if tasks.iter().all(|state| state.is_terminal()) {
            StageState::Aborted
        } else {
            StageState::Aborting
        });
    }
    if !scheduling_complete {
        return Some(StageState::Running);
    }
    // `FAILING` and `CANCELING` are not terminal, so a stage holding one is
    // still active. This is what stops a single early finisher from dragging a
    // multi-task stage into `FLUSHING` and cancelling its children.
    if tasks.iter().any(|state| {
        matches!(
            state,
            TaskState::Planned | TaskState::Running | TaskState::Failing | TaskState::Canceling
        )
    }) {
        return Some(StageState::Running);
    }
    if tasks.iter().all(|state| *state == TaskState::Finished) {
        return Some(StageState::Finished);
    }
    let all_terminal = tasks.iter().all(|state| state.is_terminal());
    if all_terminal && tasks.contains(&TaskState::Canceled) {
        return Some(StageState::Canceled);
    }
    if tasks.iter().all(|state| {
        matches!(
            state,
            TaskState::Flushing | TaskState::Finished | TaskState::Canceled
        )
    }) && tasks.contains(&TaskState::Flushing)
    {
        return Some(StageState::Flushing);
    }
    Some(StageState::Running)
}

/// Whether a parent stage still needs its children's input.
///
/// Once a parent has stopped consuming, the frontend cancels the child stage's
/// non-root tasks with a normal reason and propagates that downward one layer
/// at a time.
pub fn parent_released_children(parent: StageState) -> bool {
    matches!(
        parent,
        StageState::Flushing
            | StageState::Finished
            | StageState::Canceled
            | StageState::Aborted
            | StageState::Failed
    )
}

/// The facts that decide a read query's client-visible completion.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct RootReadFacts {
    root_state: TaskState,
    root_eof_observed: bool,
    query_failure_latched: bool,
}

impl RootReadFacts {
    pub const fn new(
        root_state: TaskState,
        root_eof_observed: bool,
        query_failure_latched: bool,
    ) -> Self {
        Self {
            root_state,
            root_eof_observed,
            query_failure_latched,
        }
    }

    /// Whether the frontend may deliver end-of-stream to the client.
    ///
    /// This deliberately does not wait for upstream tasks to finish standing
    /// down. A root `CANCELED` is never success, and a latched failure or
    /// abort always wins.
    pub const fn client_visible_completion(self) -> bool {
        matches!(self.root_state, TaskState::Finished)
            && self.root_eof_observed
            && !self.query_failure_latched
    }
}

/// The facts that decide when an attempt has fully drained.
///
/// Draining is internal resource closure. It never gates a client-visible
/// read completion that has already been linearized.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct AttemptDrainFacts {
    all_tasks_terminal: bool,
    all_output_released: bool,
    all_contexts_released: bool,
}

impl AttemptDrainFacts {
    pub const fn new(
        all_tasks_terminal: bool,
        all_output_released: bool,
        all_contexts_released: bool,
    ) -> Self {
        Self {
            all_tasks_terminal,
            all_output_released,
            all_contexts_released,
        }
    }

    pub const fn drained(self) -> bool {
        self.all_tasks_terminal && self.all_output_released && self.all_contexts_released
    }

    pub const fn all_tasks_terminal(self) -> bool {
        self.all_tasks_terminal
    }

    pub const fn all_output_released(self) -> bool {
        self.all_output_released
    }

    pub const fn all_contexts_released(self) -> bool {
        self.all_contexts_released
    }
}

/// The facts that decide a distributed write's success.
///
/// A write does not get the read path's early completion. Every writer and
/// the root finish task must have finished, the prepared write set must be
/// complete, and the frontend's external commit must have succeeded. A
/// cancelled writer can never be counted as success.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct WriteCompletionFacts {
    all_writers_finished: bool,
    root_finish_task_finished: bool,
    prepared_write_set_complete: bool,
    external_commit_succeeded: bool,
    any_writer_canceled: bool,
}

impl WriteCompletionFacts {
    pub const fn new(
        all_writers_finished: bool,
        root_finish_task_finished: bool,
        prepared_write_set_complete: bool,
        external_commit_succeeded: bool,
        any_writer_canceled: bool,
    ) -> Self {
        Self {
            all_writers_finished,
            root_finish_task_finished,
            prepared_write_set_complete,
            external_commit_succeeded,
            any_writer_canceled,
        }
    }

    /// Whether the write may be reported to the client as successful.
    pub const fn client_visible_completion(self) -> bool {
        !self.any_writer_canceled
            && self.all_writers_finished
            && self.root_finish_task_finished
            && self.prepared_write_set_complete
            && self.external_commit_succeeded
    }
}

/// Whether a set of observed terminal statuses is compatible with success.
///
/// A non-root task that stood down normally is compatible; a failure or an
/// abort is not.
pub fn terminals_are_success_compatible<'a>(
    statuses: impl IntoIterator<Item = &'a TaskStatus>,
) -> bool {
    statuses
        .into_iter()
        .all(TaskStatus::is_success_compatible_terminal)
}

#[cfg(test)]
mod tests {
    use super::{
        AttemptDrainFacts, ContextOperationKind, ContextTransition, LatchOutcome,
        OperationAdmission, QueryContextEvent, QueryContextState, RootDrainAction, RootReadFacts,
        StageState, TaskTransition, TerminationLatch, WriteCompletionFacts,
        classify_context_transition, classify_operation_admission, classify_root_drain,
        classify_task_transition, derive_stage_state, parent_released_children,
    };
    use crate::task_execution::status::{
        AbortCause, CancelReason, SafeDetail, TaskFailure, TaskFailureCategory, TaskState,
        TerminationDetail,
    };

    const ALL_STATES: [TaskState; 10] = [
        TaskState::Planned,
        TaskState::Running,
        TaskState::Flushing,
        TaskState::Finished,
        TaskState::Canceling,
        TaskState::Canceled,
        TaskState::Aborting,
        TaskState::Aborted,
        TaskState::Failing,
        TaskState::Failed,
    ];

    fn failure() -> TerminationDetail {
        TerminationDetail::Failed(TaskFailure::new(
            TaskFailureCategory::Execution,
            SafeDetail::new("operator error").expect("fits"),
        ))
    }

    #[test]
    fn the_canonical_running_flushing_finished_path_is_legal() {
        assert_eq!(
            classify_task_transition(TaskState::Planned, TaskState::Running),
            TaskTransition::Apply
        );
        assert_eq!(
            classify_task_transition(TaskState::Running, TaskState::Flushing),
            TaskTransition::Apply
        );
        assert_eq!(
            classify_task_transition(TaskState::Flushing, TaskState::Finished),
            TaskTransition::Apply
        );
    }

    #[test]
    fn a_terminal_task_never_transitions_again() {
        for terminal in [
            TaskState::Finished,
            TaskState::Canceled,
            TaskState::Aborted,
            TaskState::Failed,
        ] {
            for target in ALL_STATES {
                let expected = if target == terminal {
                    TaskTransition::SameState
                } else {
                    TaskTransition::AlreadyTerminal
                };
                assert_eq!(
                    classify_task_transition(terminal, target),
                    expected,
                    "{terminal} -> {target}"
                );
            }
        }
    }

    #[test]
    fn illegal_task_transitions_are_rejected_rather_than_silently_allowed() {
        // No walking backwards out of flushing.
        assert_eq!(
            classify_task_transition(TaskState::Flushing, TaskState::Running),
            TaskTransition::Illegal
        );
        assert_eq!(
            classify_task_transition(TaskState::Flushing, TaskState::Planned),
            TaskTransition::Illegal
        );
        // A forced termination only completes; it cannot become a normal
        // cancellation or a plain failure.
        assert_eq!(
            classify_task_transition(TaskState::Aborting, TaskState::Canceled),
            TaskTransition::Illegal
        );
        assert_eq!(
            classify_task_transition(TaskState::Aborting, TaskState::Failing),
            TaskTransition::Illegal
        );
        assert_eq!(
            classify_task_transition(TaskState::Failing, TaskState::Canceled),
            TaskTransition::Illegal
        );
        // A planned task cannot skip straight to a terminal outcome.
        assert_eq!(
            classify_task_transition(TaskState::Planned, TaskState::Finished),
            TaskTransition::Illegal
        );
        assert_eq!(
            classify_task_transition(TaskState::Planned, TaskState::Flushing),
            TaskTransition::Illegal
        );
    }

    #[test]
    fn a_normal_stand_down_may_still_be_escalated() {
        assert_eq!(
            classify_task_transition(TaskState::Canceling, TaskState::Canceled),
            TaskTransition::Apply
        );
        assert_eq!(
            classify_task_transition(TaskState::Canceling, TaskState::Aborting),
            TaskTransition::Apply
        );
        assert_eq!(
            classify_task_transition(TaskState::Canceling, TaskState::Failing),
            TaskTransition::Apply
        );
    }

    #[test]
    fn same_state_is_a_metric_only_republish() {
        for state in ALL_STATES {
            assert_eq!(
                classify_task_transition(state, state),
                TaskTransition::SameState,
                "{state}"
            );
        }
    }

    #[test]
    fn termination_is_first_wins_and_the_first_cause_survives() {
        let mut latch = TerminationLatch::open();
        assert!(!latch.is_latched());

        assert_eq!(
            latch.latch(TerminationDetail::Aborted(AbortCause::LeaseExpired)),
            LatchOutcome::Won
        );
        assert!(latch.is_latched());
        assert_eq!(
            latch.cause(),
            Some(&TerminationDetail::Aborted(AbortCause::LeaseExpired))
        );

        assert_eq!(
            latch.latch(failure()),
            LatchOutcome::Lost(TerminationDetail::Aborted(AbortCause::LeaseExpired))
        );
        assert_eq!(
            latch.latch(TerminationDetail::Aborted(AbortCause::QueryFailed)),
            LatchOutcome::Lost(TerminationDetail::Aborted(AbortCause::LeaseExpired))
        );
        assert_eq!(
            latch.cause(),
            Some(&TerminationDetail::Aborted(AbortCause::LeaseExpired)),
            "a later cause must never rewrite the first"
        );
    }

    #[test]
    fn context_lifecycle_follows_the_narrow_state_machine() {
        use QueryContextEvent as Event;
        use QueryContextState as State;

        assert_eq!(
            classify_context_transition(State::Absent, Event::Establish),
            ContextTransition::Apply(State::Establishing)
        );
        assert_eq!(
            classify_context_transition(State::Establishing, Event::EstablishCompleted),
            ContextTransition::Apply(State::Active)
        );
        assert_eq!(
            classify_context_transition(State::Active, Event::Release),
            ContextTransition::Apply(State::Releasing)
        );
        assert_eq!(
            classify_context_transition(State::Releasing, Event::ReleaseCompleted),
            ContextTransition::Apply(State::TerminalRetained)
        );
        assert_eq!(
            classify_context_transition(State::TerminalRetained, Event::Reap),
            ContextTransition::Apply(State::Gone)
        );
    }

    #[test]
    fn abort_before_establish_builds_a_pre_establish_fence() {
        assert_eq!(
            classify_context_transition(QueryContextState::Absent, QueryContextEvent::Abort),
            ContextTransition::Apply(QueryContextState::TerminalRetained)
        );
        // Once the fence exists, a late establish or create finds a terminal
        // context rather than reviving the execution.
        assert_eq!(
            classify_context_transition(
                QueryContextState::TerminalRetained,
                QueryContextEvent::Establish
            ),
            ContextTransition::AlreadyTerminal
        );
        assert_eq!(
            classify_operation_admission(
                QueryContextState::TerminalRetained,
                ContextOperationKind::CreateTask
            ),
            OperationAdmission::TerminalReceipt
        );
    }

    #[test]
    fn establishing_may_abort_and_active_may_abort() {
        assert_eq!(
            classify_context_transition(QueryContextState::Establishing, QueryContextEvent::Abort),
            ContextTransition::Apply(QueryContextState::Aborting)
        );
        assert_eq!(
            classify_context_transition(QueryContextState::Active, QueryContextEvent::Abort),
            ContextTransition::Apply(QueryContextState::Aborting)
        );
        assert_eq!(
            classify_context_transition(
                QueryContextState::Aborting,
                QueryContextEvent::AbortCompleted
            ),
            ContextTransition::Apply(QueryContextState::TerminalRetained)
        );
    }

    #[test]
    fn release_beats_a_later_abort_but_not_an_earlier_one() {
        // Release linearized first, so the late abort only gets the normal
        // terminal receipt.
        assert_eq!(
            classify_context_transition(QueryContextState::Releasing, QueryContextEvent::Abort),
            ContextTransition::LostToRelease
        );
        // Abort linearized first, so release finds a terminating context.
        assert_eq!(
            classify_context_transition(QueryContextState::Aborting, QueryContextEvent::Release),
            ContextTransition::Illegal
        );
    }

    #[test]
    fn exact_retries_of_lifecycle_events_are_idempotent() {
        assert_eq!(
            classify_context_transition(
                QueryContextState::Establishing,
                QueryContextEvent::Establish
            ),
            ContextTransition::Idempotent
        );
        assert_eq!(
            classify_context_transition(QueryContextState::Active, QueryContextEvent::Establish),
            ContextTransition::Idempotent
        );
        assert_eq!(
            classify_context_transition(QueryContextState::Releasing, QueryContextEvent::Release),
            ContextTransition::Idempotent
        );
        assert_eq!(
            classify_context_transition(QueryContextState::Aborting, QueryContextEvent::Abort),
            ContextTransition::Idempotent
        );
    }

    #[test]
    fn there_is_no_event_that_releases_an_active_context_implicitly() {
        // The only way out of `Active` towards retention is an explicit
        // release or an abort. No enumerated event means "all known tasks are
        // terminal", which is exactly the guess a backend must never make.
        let reachable: Vec<QueryContextState> = [
            QueryContextEvent::Establish,
            QueryContextEvent::EstablishCompleted,
            QueryContextEvent::Release,
            QueryContextEvent::ReleaseCompleted,
            QueryContextEvent::Abort,
            QueryContextEvent::AbortCompleted,
            QueryContextEvent::Reap,
        ]
        .into_iter()
        .filter_map(
            |event| match classify_context_transition(QueryContextState::Active, event) {
                ContextTransition::Apply(state) => Some(state),
                _ => None,
            },
        )
        .collect();
        assert_eq!(
            reachable,
            vec![QueryContextState::Releasing, QueryContextState::Aborting]
        );
    }

    #[test]
    fn creates_wait_for_the_gate_while_other_operations_fail_closed() {
        for state in [QueryContextState::Absent, QueryContextState::Establishing] {
            assert_eq!(
                classify_operation_admission(state, ContextOperationKind::CreateTask),
                OperationAdmission::WaitForCreationGate,
                "{state}"
            );
            assert_eq!(
                classify_operation_admission(state, ContextOperationKind::AdvanceDomain),
                OperationAdmission::NotEstablished,
                "{state}"
            );
            assert_eq!(
                classify_operation_admission(state, ContextOperationKind::RenewLease),
                OperationAdmission::NotEstablished,
                "{state}"
            );
        }

        for kind in [
            ContextOperationKind::CreateTask,
            ContextOperationKind::AdvanceDomain,
            ContextOperationKind::RenewLease,
        ] {
            assert_eq!(
                classify_operation_admission(QueryContextState::Active, kind),
                OperationAdmission::Admit
            );
            for closed in [
                QueryContextState::Releasing,
                QueryContextState::Aborting,
                QueryContextState::TerminalRetained,
            ] {
                assert_eq!(
                    classify_operation_admission(closed, kind),
                    OperationAdmission::TerminalReceipt,
                    "{closed}"
                );
                assert!(closed.is_closed());
            }
            assert_eq!(
                classify_operation_admission(QueryContextState::Gone, kind),
                OperationAdmission::Gone
            );
        }
    }

    #[test]
    fn one_early_finisher_never_flushes_a_multi_task_stage() {
        let tasks = [TaskState::Finished, TaskState::Running];
        assert_eq!(
            derive_stage_state(true, &tasks),
            Some(StageState::Running),
            "a still-running sibling keeps the stage running"
        );
        assert!(!parent_released_children(StageState::Running));

        let flushing_and_running = [TaskState::Flushing, TaskState::Running];
        assert_eq!(
            derive_stage_state(true, &flushing_and_running),
            Some(StageState::Running)
        );

        let all_flushing = [TaskState::Flushing, TaskState::Finished];
        assert_eq!(
            derive_stage_state(true, &all_flushing),
            Some(StageState::Flushing)
        );
        assert!(parent_released_children(StageState::Flushing));
    }

    #[test]
    fn stage_derivation_priority_is_failure_then_abort_then_activity() {
        assert_eq!(
            derive_stage_state(true, &[TaskState::Failed, TaskState::Running]),
            Some(StageState::Failed)
        );
        assert_eq!(
            derive_stage_state(true, &[TaskState::Failed, TaskState::Aborted]),
            Some(StageState::Failed),
            "failure outranks abort"
        );
        assert_eq!(
            derive_stage_state(true, &[TaskState::Aborting, TaskState::Finished]),
            Some(StageState::Aborting)
        );
        assert_eq!(
            derive_stage_state(true, &[TaskState::Aborted, TaskState::Running]),
            Some(StageState::Aborting),
            "an abort is not complete while a sibling still runs"
        );
        assert_eq!(
            derive_stage_state(true, &[TaskState::Aborted, TaskState::Finished]),
            Some(StageState::Aborted)
        );
        assert_eq!(
            derive_stage_state(true, &[TaskState::Finished, TaskState::Finished]),
            Some(StageState::Finished)
        );
        assert_eq!(
            derive_stage_state(true, &[TaskState::Canceled, TaskState::Finished]),
            Some(StageState::Canceled)
        );
        assert_eq!(derive_stage_state(true, &[]), None);
    }

    #[test]
    fn a_terminal_aggregate_requires_a_frozen_task_set() {
        // Before scheduling is complete the observable tasks may all look
        // finished while more are still being created.
        assert_eq!(
            derive_stage_state(false, &[TaskState::Finished]),
            Some(StageState::Running)
        );
        assert_eq!(
            derive_stage_state(false, &[TaskState::Flushing]),
            Some(StageState::Running)
        );
        assert_eq!(
            derive_stage_state(true, &[TaskState::Finished]),
            Some(StageState::Finished)
        );
        // A failure or an abort still propagates without waiting for the
        // stage to finish being built.
        assert_eq!(
            derive_stage_state(false, &[TaskState::Failed]),
            Some(StageState::Failed)
        );
        assert_eq!(
            derive_stage_state(false, &[TaskState::Aborting]),
            Some(StageState::Aborting)
        );
    }

    #[test]
    fn a_failing_or_canceling_task_keeps_the_stage_active() {
        assert_eq!(
            derive_stage_state(true, &[TaskState::Failing, TaskState::Finished]),
            Some(StageState::Running),
            "FAILING is not terminal, so the stage is not yet failed"
        );
        assert_eq!(
            derive_stage_state(true, &[TaskState::Canceling, TaskState::Flushing]),
            Some(StageState::Running)
        );
    }

    #[test]
    fn client_visible_read_completion_does_not_wait_for_upstream_cancellation() {
        let complete = RootReadFacts::new(TaskState::Finished, true, false);
        assert!(complete.client_visible_completion());

        assert!(
            !RootReadFacts::new(TaskState::Finished, false, false).client_visible_completion(),
            "end-of-stream must have been observed"
        );
        assert!(
            !RootReadFacts::new(TaskState::Flushing, true, false).client_visible_completion(),
            "a flushing root still owes output"
        );
        assert!(
            !RootReadFacts::new(TaskState::Canceled, true, false).client_visible_completion(),
            "a cancelled root is never success"
        );
        assert!(
            !RootReadFacts::new(TaskState::Finished, true, true).client_visible_completion(),
            "a latched failure wins over an otherwise complete root"
        );
    }

    #[test]
    fn an_originating_failure_replaces_a_derived_placeholder_once() {
        // The defect this catches: a query context reports "another task
        // failed" as soon as it reacts to a failure, so it reaches the latch
        // first and, under strict first-wins, becomes the only thing the
        // client is ever told. Every engine-level message -- a CAST mismatch,
        // an array_map length error -- was replaced by PEER_TASK_FAILED, which
        // tells the user nothing they can act on.
        let mut latch = TerminationLatch::open();
        let derived = TerminationDetail::Aborted(AbortCause::PeerTaskFailed);
        let originating = TerminationDetail::Failed(TaskFailure::new(
            TaskFailureCategory::Execution,
            SafeDetail::truncating("array_map() element sizes differ"),
        ));

        assert!(matches!(latch.latch(derived.clone()), LatchOutcome::Won));
        assert!(matches!(
            latch.latch(originating.clone()),
            LatchOutcome::Refined(_)
        ));
        assert_eq!(latch.cause(), Some(&originating));

        // Once. A second originating cause does not keep rewriting the answer:
        // the attempt reports the first real explanation it was given.
        let second = TerminationDetail::Failed(TaskFailure::new(
            TaskFailureCategory::Execution,
            SafeDetail::truncating("a later, unrelated failure"),
        ));
        assert!(matches!(latch.latch(second), LatchOutcome::Lost(_)));
        assert_eq!(latch.cause(), Some(&originating));

        // And a derived cause never displaces a real one.
        let mut other = TerminationLatch::open();
        assert!(matches!(
            other.latch(originating.clone()),
            LatchOutcome::Won
        ));
        assert!(matches!(other.latch(derived), LatchOutcome::Lost(_)));
        assert_eq!(other.cause(), Some(&originating));
    }

    #[test]
    fn attempt_draining_is_separate_from_client_visible_completion() {
        assert!(AttemptDrainFacts::new(true, true, true).drained());
        assert!(!AttemptDrainFacts::new(true, true, false).drained());
        assert!(!AttemptDrainFacts::new(true, false, true).drained());
        assert!(!AttemptDrainFacts::new(false, true, true).drained());
    }

    #[test]
    fn a_write_needs_every_writer_the_root_the_set_and_the_commit() {
        assert!(
            WriteCompletionFacts::new(true, true, true, true, false).client_visible_completion()
        );
        for facts in [
            WriteCompletionFacts::new(false, true, true, true, false),
            WriteCompletionFacts::new(true, false, true, true, false),
            WriteCompletionFacts::new(true, true, false, true, false),
            WriteCompletionFacts::new(true, true, true, false, false),
        ] {
            assert!(!facts.client_visible_completion(), "{facts:?}");
        }
        assert!(
            !WriteCompletionFacts::new(true, true, true, true, true).client_visible_completion(),
            "a cancelled writer can never be counted as success"
        );
    }

    #[test]
    fn an_end_of_stream_completes_only_a_task_that_was_still_executing() {
        for state in [TaskState::Running, TaskState::Flushing] {
            assert_eq!(
                classify_root_drain(state),
                RootDrainAction::Finish,
                "{state}"
            );
        }
        // A task already standing down, being aborted, or converging on its own
        // failure must not be turned into a success by its buffer draining.
        for state in [
            TaskState::Canceling,
            TaskState::Aborting,
            TaskState::Failing,
        ] {
            assert_eq!(
                classify_root_drain(state),
                RootDrainAction::RecordOnly,
                "{state}"
            );
        }
        for state in [
            TaskState::Finished,
            TaskState::Canceled,
            TaskState::Aborted,
            TaskState::Failed,
        ] {
            assert_eq!(
                classify_root_drain(state),
                RootDrainAction::AlreadyTerminal,
                "{state}"
            );
        }
        assert_eq!(
            classify_root_drain(TaskState::Planned),
            RootDrainAction::Illegal,
            "a task that never ran cannot have drained a result stream"
        );
    }

    #[test]
    fn cancel_reason_is_the_only_success_compatible_cause() {
        assert!(
            TerminationDetail::Canceled(CancelReason::UpstreamNoLongerNeeded)
                .is_success_compatible()
        );
        assert!(!TerminationDetail::Aborted(AbortCause::QueryFailed).is_success_compatible());
        assert!(!failure().is_success_compatible());
    }
}
