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

use novarocks_execution_contract::{ParticipationState, TaskState, TerminationDetail};

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
        TaskState::Canceling => matches!(to, TaskState::Canceled),
        // The owner additionally requires the accepted abort to be derived.
        // State shape alone never authorizes replacing an originating cause.
        TaskState::Aborting => matches!(to, TaskState::Aborted | TaskState::Failing),
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
            Some(existing)
                if existing.is_derived() && matches!(cause, TerminationDetail::Failed(_)) =>
            {
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
pub enum ParticipationEvent {
    Establish,
    EstablishCompleted,
    Quiesce,
    Release,
    ReleaseCompleted,
    Abort,
    AbortCompleted,
    Reap,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ParticipationTransition {
    Apply(ParticipationState),
    Idempotent,
    LostToRelease,
    AlreadyTerminal,
    Illegal,
}

pub fn classify_participation_transition(
    state: ParticipationState,
    event: ParticipationEvent,
) -> ParticipationTransition {
    use ParticipationEvent as Event;
    use ParticipationState as State;

    match (state, event) {
        (State::Absent, Event::Establish) => ParticipationTransition::Apply(State::Establishing),
        (State::Absent, Event::Abort) => ParticipationTransition::Apply(State::TerminalRetained),
        (State::Establishing, Event::EstablishCompleted) => {
            ParticipationTransition::Apply(State::Active)
        }
        (State::Absent | State::Establishing | State::Active, Event::Quiesce) => {
            ParticipationTransition::Apply(State::Quiescing)
        }
        (State::Quiescing, Event::Quiesce) => ParticipationTransition::Idempotent,
        (State::Quiescing, Event::Establish | Event::EstablishCompleted) => {
            ParticipationTransition::AlreadyTerminal
        }
        (State::Quiescing, Event::Release) => ParticipationTransition::Apply(State::Releasing),
        (State::Quiescing, Event::Abort) => ParticipationTransition::Apply(State::Aborting),
        (State::Establishing, Event::Abort) => ParticipationTransition::Apply(State::Aborting),
        (State::Establishing, Event::Establish) => ParticipationTransition::Idempotent,
        (State::Active, Event::Release) => ParticipationTransition::Apply(State::Releasing),
        (State::Active, Event::Abort) => ParticipationTransition::Apply(State::Aborting),
        (State::Active, Event::Establish | Event::EstablishCompleted) => {
            ParticipationTransition::Idempotent
        }
        (State::Releasing, Event::ReleaseCompleted) => {
            ParticipationTransition::Apply(State::TerminalRetained)
        }
        (State::Releasing, Event::Release) => ParticipationTransition::Idempotent,
        (State::Releasing, Event::Abort) => ParticipationTransition::LostToRelease,
        (State::Aborting, Event::AbortCompleted) => {
            ParticipationTransition::Apply(State::TerminalRetained)
        }
        (State::Aborting, Event::Abort) => ParticipationTransition::Idempotent,
        (State::TerminalRetained, Event::Reap) => ParticipationTransition::Apply(State::Gone),
        (State::TerminalRetained, _) | (State::Gone, _) => ParticipationTransition::AlreadyTerminal,
        _ => ParticipationTransition::Illegal,
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ParticipationOperationKind {
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
    state: ParticipationState,
    kind: ParticipationOperationKind,
) -> OperationAdmission {
    match state {
        ParticipationState::Absent | ParticipationState::Establishing => match kind {
            ParticipationOperationKind::CreateTask => OperationAdmission::WaitForCreationGate,
            _ => OperationAdmission::NotEstablished,
        },
        ParticipationState::Active => OperationAdmission::Admit,
        ParticipationState::Quiescing if kind == ParticipationOperationKind::RenewLease => {
            OperationAdmission::Admit
        }
        ParticipationState::Releasing
        | ParticipationState::Quiescing
        | ParticipationState::Aborting
        | ParticipationState::TerminalRetained => OperationAdmission::TerminalReceipt,
        ParticipationState::Gone => OperationAdmission::Gone,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        LatchOutcome, OperationAdmission, ParticipationEvent, ParticipationOperationKind,
        ParticipationTransition, RootDrainAction, TaskTransition, TerminationLatch,
        classify_operation_admission, classify_participation_transition, classify_root_drain,
        classify_task_transition,
    };
    use novarocks_execution_contract::{
        AbortCause, ParticipationState, SafeDetail, TaskFailure, TaskState, TerminationDetail,
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
            novarocks_execution_contract::failure::FailureFact::new(
                novarocks_execution_contract::failure::FailureClass::Data,
                novarocks_execution_contract::failure::FailureReason::new(
                    std::num::NonZeroU16::new(u16::MAX).unwrap(),
                    std::num::NonZeroU16::new(1).unwrap(),
                ),
            ),
            SafeDetail::new("scan failed").expect("bounded detail"),
        ));
        assert_eq!(latch.latch(failure.clone()), LatchOutcome::Refined(derived));
        assert_eq!(latch.cause(), Some(&failure));
    }

    #[test]
    fn peer_placeholder_is_not_refined_by_other_control_causes() {
        let mut latch = TerminationLatch::open();
        let peer = TerminationDetail::Aborted(AbortCause::PeerTaskFailed);
        latch.latch(peer.clone());
        for candidate in [
            TerminationDetail::Aborted(AbortCause::LeaseExpired),
            TerminationDetail::Canceled(
                novarocks_execution_contract::CancelReason::UpstreamNoLongerNeeded,
            ),
        ] {
            assert_eq!(latch.latch(candidate), LatchOutcome::Lost(peer.clone()));
            assert_eq!(latch.cause(), Some(&peer));
        }
    }

    #[test]
    fn participation_creation_gate_and_retirement_are_worker_decisions() {
        assert_eq!(
            classify_participation_transition(
                ParticipationState::Absent,
                ParticipationEvent::Establish
            ),
            ParticipationTransition::Apply(ParticipationState::Establishing)
        );
        assert_eq!(
            classify_operation_admission(
                ParticipationState::Establishing,
                ParticipationOperationKind::CreateTask
            ),
            OperationAdmission::WaitForCreationGate
        );
        assert_eq!(
            classify_operation_admission(
                ParticipationState::TerminalRetained,
                ParticipationOperationKind::RenewLease
            ),
            OperationAdmission::TerminalReceipt
        );
    }
}
