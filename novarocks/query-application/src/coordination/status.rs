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

use std::fmt;

use novarocks_execution_contract::{
    FinalTaskInfo, IdentityMismatch, TaskIdentity, TaskStatus, TaskStatusCursor, TaskStatusVersion,
};

/// One event delivered by query coordination's status-observation port.
///
/// Losing the observation stream is not an operation receipt and does not
/// change a Worker task. The coordinator resubscribes from its retained
/// cursors when it receives `ObservationLost`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StatusObservationPortEvent {
    Published(TaskStatus),
    Gone(TaskIdentity),
    ObservationLost,
}

impl StatusObservationPortEvent {
    pub const fn requires_resubscribe(&self) -> bool {
        matches!(self, Self::ObservationLost)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StatusObservation {
    Accept,
    Idempotent,
    Ignore,
    VersionConflict,
    IdentityMismatch(IdentityMismatch),
    TerminalOverwrite,
}

impl StatusObservation {
    pub const fn is_fatal(&self) -> bool {
        matches!(
            self,
            Self::VersionConflict | Self::IdentityMismatch(_) | Self::TerminalOverwrite
        )
    }
}

pub fn classify_observation(
    cursor: TaskStatusCursor,
    held: Option<&TaskStatus>,
    observed: &TaskStatus,
) -> StatusObservation {
    if let Err(mismatch) = cursor.identity().verify_matches(observed.identity()) {
        return StatusObservation::IdentityMismatch(mismatch);
    }
    let Some(current) = cursor.current_version() else {
        return StatusObservation::Accept;
    };
    if observed.version() == current {
        return match held {
            Some(held) if held == observed => StatusObservation::Idempotent,
            Some(_) => StatusObservation::VersionConflict,
            None => StatusObservation::Idempotent,
        };
    }
    if observed.version() < current {
        return StatusObservation::Ignore;
    }
    if held.is_some_and(TaskStatus::is_terminal) {
        return StatusObservation::TerminalOverwrite;
    }
    StatusObservation::Accept
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum GoneObservation {
    RetentionEnded,
    TerminalNeverObserved,
}

pub fn classify_gone(held: Option<&TaskStatus>) -> GoneObservation {
    if held.is_some_and(TaskStatus::is_terminal) {
        GoneObservation::RetentionEnded
    } else {
        GoneObservation::TerminalNeverObserved
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FinalInfoDisagreement {
    Identity(IdentityMismatch),
    TerminalVersion {
        observed: TaskStatusVersion,
        carried: TaskStatusVersion,
    },
    SnapshotConflict(TaskStatusVersion),
}

impl fmt::Display for FinalInfoDisagreement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Identity(mismatch) => write!(formatter, "final task info {mismatch}"),
            Self::TerminalVersion { observed, carried } => write!(
                formatter,
                "final task info carries terminal version {carried}, but version {observed} was observed"
            ),
            Self::SnapshotConflict(version) => write!(
                formatter,
                "final task info carries a different snapshot under terminal version {version}"
            ),
        }
    }
}

impl std::error::Error for FinalInfoDisagreement {}

pub fn verify_final_info(
    observed_terminal: &TaskStatus,
    info: &FinalTaskInfo,
) -> Result<(), FinalInfoDisagreement> {
    let carried = info.final_status();
    if let Err(mismatch) = observed_terminal
        .identity()
        .verify_matches(carried.identity())
    {
        return Err(FinalInfoDisagreement::Identity(mismatch));
    }
    if carried.version() != observed_terminal.version() {
        return Err(FinalInfoDisagreement::TerminalVersion {
            observed: observed_terminal.version(),
            carried: carried.version(),
        });
    }
    if carried != observed_terminal {
        return Err(FinalInfoDisagreement::SnapshotConflict(carried.version()));
    }
    Ok(())
}

/// Query-side validation of a newly observed task state.
///
/// This mirrors the protocol state machine but does not authorize Worker
/// mutation. It only decides whether the coordinator may adopt an observation.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ObservedTaskTransition {
    Advance,
    SameState,
    TerminalOverwrite,
    Illegal,
}

pub fn classify_observed_task_transition(
    from: novarocks_execution_contract::TaskState,
    to: novarocks_execution_contract::TaskState,
) -> ObservedTaskTransition {
    use novarocks_execution_contract::TaskState;

    if from.is_terminal() {
        return ObservedTaskTransition::TerminalOverwrite;
    }
    if from == to {
        return ObservedTaskTransition::SameState;
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
        ObservedTaskTransition::Advance
    } else {
        ObservedTaskTransition::Illegal
    }
}

#[cfg(test)]
mod tests {
    use novarocks_execution_contract::TaskState;

    use super::{
        ObservedTaskTransition, StatusObservationPortEvent, classify_observed_task_transition,
    };

    #[test]
    fn observation_loss_is_a_port_event_that_only_requests_resubscription() {
        assert!(StatusObservationPortEvent::ObservationLost.requires_resubscribe());
    }

    #[test]
    fn observed_transitions_do_not_authorize_worker_mutation() {
        assert_eq!(
            classify_observed_task_transition(TaskState::Running, TaskState::Finished),
            ObservedTaskTransition::Advance
        );
        assert_eq!(
            classify_observed_task_transition(TaskState::Finished, TaskState::Finished),
            ObservedTaskTransition::TerminalOverwrite
        );
        assert_eq!(
            classify_observed_task_transition(TaskState::Flushing, TaskState::Running),
            ObservedTaskTransition::Illegal
        );
    }
}
