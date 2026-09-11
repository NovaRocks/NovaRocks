// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use novarocks_execution_contract::{QueryContextState, TaskState, TerminationDetail};

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum TaskTransition {
    Apply,
    SameState,
    AlreadyTerminal,
    Illegal,
}

/// Classifies a proposed worker-local task state change.
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
        TaskState::Canceling => matches!(
            to,
            TaskState::Canceled | TaskState::Aborting | TaskState::Failing
        ),
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

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum RootDrainAction {
    Finish,
    RecordOnly,
    AlreadyTerminal,
    Illegal,
}

/// Converts root EOS into a worker action without making the data plane a
/// second terminal authority.
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

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TerminationLatch {
    first: Option<TerminationDetail>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LatchOutcome {
    Won,
    Lost(TerminationDetail),
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

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum QueryContextEvent {
    Establish,
    EstablishCompleted,
    Release,
    ReleaseCompleted,
    Abort,
    AbortCompleted,
    Reap,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ContextTransition {
    Apply(QueryContextState),
    Idempotent,
    LostToRelease,
    AlreadyTerminal,
    Illegal,
}

pub fn classify_context_transition(
    state: QueryContextState,
    event: QueryContextEvent,
) -> ContextTransition {
    use QueryContextEvent as Event;
    use QueryContextState as State;

    match (state, event) {
        (State::Absent, Event::Establish) => ContextTransition::Apply(State::Establishing),
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
        (State::TerminalRetained, _) | (State::Gone, _) => ContextTransition::AlreadyTerminal,
        _ => ContextTransition::Illegal,
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ContextOperationKind {
    CreateTask,
    AdvanceDomain,
    RenewLease,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum OperationAdmission {
    Admit,
    WaitForCreationGate,
    NotEstablished,
    TerminalReceipt,
    Gone,
}

pub fn classify_operation_admission(
    state: QueryContextState,
    kind: ContextOperationKind,
) -> OperationAdmission {
    match state {
        QueryContextState::Absent | QueryContextState::Establishing => match kind {
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

#[cfg(test)]
mod tests {
    use super::{
        ContextOperationKind, ContextTransition, LatchOutcome, OperationAdmission,
        QueryContextEvent, RootDrainAction, TaskTransition, TerminationLatch,
        classify_context_transition, classify_operation_admission, classify_root_drain,
        classify_task_transition,
    };
    use novarocks_execution_contract::{
        AbortCause, QueryContextState, SafeDetail, TaskFailure, TaskFailureCategory, TaskState,
        TerminationDetail,
    };

    #[test]
    fn task_transition_policy_preserves_terminal_monotonicity() {
        assert_eq!(
            classify_task_transition(TaskState::Running, TaskState::Finished),
            TaskTransition::Apply
        );
        assert_eq!(
            classify_task_transition(TaskState::Finished, TaskState::Running),
            TaskTransition::AlreadyTerminal
        );
        assert_eq!(
            classify_task_transition(TaskState::Planned, TaskState::Finished),
            TaskTransition::Illegal
        );
    }

    #[test]
    fn root_drain_records_evidence_without_overwriting_termination() {
        assert_eq!(
            classify_root_drain(TaskState::Running),
            RootDrainAction::Finish
        );
        assert_eq!(
            classify_root_drain(TaskState::Aborting),
            RootDrainAction::RecordOnly
        );
        assert_eq!(
            classify_root_drain(TaskState::Aborted),
            RootDrainAction::AlreadyTerminal
        );
    }

    #[test]
    fn originating_failure_refines_a_derived_termination_cause_once() {
        let mut latch = TerminationLatch::open();
        let derived = TerminationDetail::Aborted(AbortCause::PeerTaskFailed);
        assert_eq!(latch.latch(derived.clone()), LatchOutcome::Won);

        let failure = TerminationDetail::Failed(TaskFailure::new(
            TaskFailureCategory::Execution,
            SafeDetail::new("scan failed").expect("bounded detail"),
        ));
        assert_eq!(latch.latch(failure.clone()), LatchOutcome::Refined(derived));
        assert_eq!(latch.cause(), Some(&failure));
    }

    #[test]
    fn context_creation_gate_and_retirement_are_worker_decisions() {
        assert_eq!(
            classify_context_transition(QueryContextState::Absent, QueryContextEvent::Establish),
            ContextTransition::Apply(QueryContextState::Establishing)
        );
        assert_eq!(
            classify_operation_admission(
                QueryContextState::Establishing,
                ContextOperationKind::CreateTask
            ),
            OperationAdmission::WaitForCreationGate
        );
        assert_eq!(
            classify_operation_admission(
                QueryContextState::TerminalRetained,
                ContextOperationKind::RenewLease
            ),
            OperationAdmission::TerminalReceipt
        );
    }
}
