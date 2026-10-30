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

use novarocks_execution_contract::{IdentityMismatch, TaskStatus, TaskStatusCursor};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StatusObservation {
    Accept,
    Idempotent,
    Ignore,
    VersionConflict,
    IdentityMismatch(IdentityMismatch),
    TerminalOverwrite,
    InstallationRegression,
    ConclusionOverwrite,
}

impl StatusObservation {
    pub const fn is_fatal(&self) -> bool {
        matches!(
            self,
            Self::VersionConflict
                | Self::IdentityMismatch(_)
                | Self::TerminalOverwrite
                | Self::InstallationRegression
                | Self::ConclusionOverwrite
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
    if held.is_some_and(|held| held.installed() && !observed.installed()) {
        return StatusObservation::InstallationRegression;
    }
    if held.is_some_and(TaskStatus::is_terminal) {
        return StatusObservation::TerminalOverwrite;
    }
    // A catch-up snapshot may skip lifecycle states, but cannot skip the
    // already accepted cause. This semantic check is independent of adjacent
    // version shape validation. Only a derived peer placeholder may refine.
    if let Some(existing) = held.and_then(TaskStatus::termination)
        && observed.termination() != Some(existing)
    {
        use novarocks_execution_contract::{ConclusionDecision, TaskConclusion, decide_conclusion};
        let current = TaskConclusion::Terminated(existing.clone());
        let proposed = observed
            .termination()
            .cloned()
            .map_or(TaskConclusion::Succeeded, TaskConclusion::Terminated);
        if decide_conclusion(Some(&current), &proposed) != ConclusionDecision::RefineDerived {
            return StatusObservation::ConclusionOverwrite;
        }
    }
    StatusObservation::Accept
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
        TaskState::Canceling => matches!(to, TaskState::Canceled),
        // The observation's accepted-cause check separately requires derived
        // refinement. A shape transition never authorizes a new first cause.
        TaskState::Aborting => matches!(to, TaskState::Aborted | TaskState::Failing),
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
mod accepted_cause_observation_tests {
    use super::*;
    use novarocks_execution_contract::{
        AbortCause, CancelReason, SafeDetail, TaskFailure, TaskIdentity, TaskOutputFacts,
        TaskState, TaskStatusVersion, TerminationDetail,
    };
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    fn identity() -> TaskIdentity {
        TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(3903, 1), AttemptId::new(1).unwrap()).unwrap(),
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            BackendProcessId::new_v7(),
        )
    }
    fn snapshot(
        identity: TaskIdentity,
        version: u64,
        cause: Option<TerminationDetail>,
        terminal: bool,
    ) -> TaskStatus {
        let state = match (&cause, terminal) {
            (Some(TerminationDetail::Canceled(_)), false) => TaskState::Canceling,
            (Some(TerminationDetail::Canceled(_)), true) => TaskState::Canceled,
            (Some(TerminationDetail::Aborted(_)), false) => TaskState::Aborting,
            (Some(TerminationDetail::Aborted(_)), true) => TaskState::Aborted,
            (Some(TerminationDetail::Failed(_)), false) => TaskState::Failing,
            (Some(TerminationDetail::Failed(_)), true) => TaskState::Failed,
            (None, false) => TaskState::Running,
            (None, true) => TaskState::Finished,
        };
        TaskStatus::try_new(
            identity,
            TaskStatusVersion::new(version).unwrap(),
            state,
            cause,
            TaskOutputFacts::new(terminal),
        )
        .unwrap()
    }
    fn failure(detail: &str) -> TerminationDetail {
        TerminationDetail::Failed(TaskFailure::new(
            novarocks_execution_contract::failure::FailureFact::new(
                novarocks_execution_contract::failure::FailureClass::Data,
                novarocks_execution_contract::failure::FailureReason::new(
                    std::num::NonZeroU16::new(u16::MAX).unwrap(),
                    std::num::NonZeroU16::new(1).unwrap(),
                ),
            ),
            SafeDetail::new(detail).unwrap(),
        ))
    }
    #[test]
    fn accepted_cancellation_and_original_causes_survive_adjacent_and_coalesced_observations() {
        let identity = identity();
        for first in [
            TerminationDetail::Canceled(CancelReason::UpstreamNoLongerNeeded),
            TerminationDetail::Aborted(AbortCause::LeaseExpired),
            failure("original"),
        ] {
            let held = snapshot(identity, 2, Some(first.clone()), false);
            let cursor = TaskStatusCursor::at(identity, held.version());
            for version in [3, 9] {
                for terminal in [false, true] {
                    let same = snapshot(identity, version, Some(first.clone()), terminal);
                    assert_eq!(
                        classify_observation(cursor, Some(&held), &same),
                        StatusObservation::Accept
                    );
                    for later in [
                        None,
                        Some(TerminationDetail::Canceled(
                            CancelReason::UpstreamNoLongerNeeded,
                        )),
                        Some(TerminationDetail::Aborted(AbortCause::QueryFailed)),
                        Some(failure("different failure")),
                    ] {
                        if later == Some(first.clone()) {
                            continue;
                        }
                        let observed = snapshot(identity, version, later, terminal);
                        let rejected = classify_observation(cursor, Some(&held), &observed);
                        assert_eq!(rejected, StatusObservation::ConclusionOverwrite);
                        assert!(rejected.is_fatal());
                    }
                }
            }
        }
    }
    #[test]
    fn derived_peer_placeholder_refines_only_to_originating_failure_across_a_gap() {
        let identity = identity();
        let held = snapshot(
            identity,
            2,
            Some(TerminationDetail::Aborted(AbortCause::PeerTaskFailed)),
            false,
        );
        let cursor = TaskStatusCursor::at(identity, held.version());
        for version in [3, 9] {
            for terminal in [false, true] {
                let original = snapshot(
                    identity,
                    version,
                    Some(failure("original producer")),
                    terminal,
                );
                assert_eq!(
                    classify_observation(cursor, Some(&held), &original),
                    StatusObservation::Accept
                );
                for later in [
                    Some(TerminationDetail::Canceled(
                        CancelReason::UpstreamNoLongerNeeded,
                    )),
                    Some(TerminationDetail::Aborted(AbortCause::LeaseExpired)),
                    None,
                ] {
                    assert_eq!(
                        classify_observation(
                            cursor,
                            Some(&held),
                            &snapshot(identity, version, later, terminal)
                        ),
                        StatusObservation::ConclusionOverwrite
                    );
                }
            }
        }
        let failure = snapshot(identity, 3, Some(failure("original producer")), false);
        assert_eq!(
            classify_observed_task_transition(held.state(), failure.state()),
            ObservedTaskTransition::Advance
        );
        let accepted_terminal = snapshot(identity, 3, held.termination().cloned(), true);
        assert_eq!(
            classify_observation(
                TaskStatusCursor::at(identity, accepted_terminal.version()),
                Some(&accepted_terminal),
                &snapshot(identity, 9, failure.termination().cloned(), true)
            ),
            StatusObservation::TerminalOverwrite
        );
    }
}

#[cfg(test)]
mod transition_tests {
    use super::*;
    use novarocks_execution_contract::TaskState;

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
