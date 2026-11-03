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

//! Typed task-protocol evidence produced from Worker-owned facts.
//!
//! A deployment decides whether and where this evidence is rendered.  The
//! Worker only classifies the settled receipt or lifecycle fact once, so an
//! adapter cannot accidentally reinterpret an idempotent replay as an apply.

use novarocks_execution_contract::task_execution::identity::{ParticipationRef, TaskIdentity};
use novarocks_execution_contract::task_execution::lease::LeaseSequence;
use novarocks_execution_contract::task_execution::operation::{OperationOutcome, ReleaseOutcome};
use novarocks_execution_contract::task_execution::status::{TaskState, TerminationDetail};

use crate::{CreateTaskOutcome, ParticipationOutcome, ReleaseParticipationOutcome};

/// The runtime-filter observation carried by an applied participation release.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeFilterReleaseObservation {
    Absent,
    Available,
    Unavailable,
}

/// One Worker-owned task-protocol fact for a process-local telemetry adapter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TaskProtocolEvent {
    ParticipationEstablishApplied {
        participation: ParticipationRef,
    },
    ParticipationEstablishIdempotent {
        participation: ParticipationRef,
    },
    ParticipationEstablishConflict {
        participation: ParticipationRef,
    },
    LeaseRenewed {
        participation: ParticipationRef,
        sequence: LeaseSequence,
    },
    TaskCreateApplied {
        identity: TaskIdentity,
    },
    TaskCreateIdempotent {
        identity: TaskIdentity,
    },
    ParticipationReleaseApplied {
        participation: ParticipationRef,
        runtime_filter: RuntimeFilterReleaseObservation,
    },
    ParticipationLeaseExpired {
        participation: ParticipationRef,
    },
    ParticipationTerminationCompleted {
        participation: ParticipationRef,
        cause: &'static str,
        retained_tasks: usize,
    },
    TaskTerminalRetained {
        identity: TaskIdentity,
        state: TaskState,
    },
    ParticipationAbortApplied {
        participation: ParticipationRef,
    },
}

impl TaskProtocolEvent {
    /// Maps an establish receipt to its observable domain progression.
    pub fn establish_participation(
        participation: ParticipationRef,
        receipt: &ParticipationOutcome,
    ) -> Option<Self> {
        match receipt.outcome() {
            OperationOutcome::Accepted => {
                Some(Self::ParticipationEstablishApplied { participation })
            }
            OperationOutcome::Idempotent => {
                Some(Self::ParticipationEstablishIdempotent { participation })
            }
            OperationOutcome::ParticipationConflict => {
                Some(Self::ParticipationEstablishConflict { participation })
            }
            _ => None,
        }
    }

    /// Maps a lease receipt to the one progression that extends the lease.
    pub fn renew_participation_lease(
        participation: ParticipationRef,
        sequence: LeaseSequence,
        receipt: &ParticipationOutcome,
    ) -> Option<Self> {
        (receipt.outcome() == OperationOutcome::Accepted).then_some(Self::LeaseRenewed {
            participation,
            sequence,
        })
    }

    /// Maps a create receipt to its observable domain progression.
    ///
    /// A create replay is decided by the task identity it names, so there is
    /// no third progression: a replay is idempotent whatever body it carried,
    /// and every refusal is the refusal it names rather than a comparison.
    pub fn create_task(identity: TaskIdentity, receipt: &CreateTaskOutcome) -> Option<Self> {
        match receipt.outcome() {
            OperationOutcome::Accepted => Some(Self::TaskCreateApplied { identity }),
            OperationOutcome::Idempotent => Some(Self::TaskCreateIdempotent { identity }),
            _ => None,
        }
    }

    /// Records the one release that actually moved an active participation to release.
    pub fn release_participation(
        participation: ParticipationRef,
        receipt: &ReleaseParticipationOutcome,
        runtime_filter: RuntimeFilterReleaseObservation,
    ) -> Option<Self> {
        (receipt.outcome() == OperationOutcome::Accepted
            && receipt.acknowledgement().map(|ack| ack.release()) == Some(ReleaseOutcome::Released))
        .then_some(Self::ParticipationReleaseApplied {
            participation,
            runtime_filter,
        })
    }

    pub const fn participation_lease_expired(participation: ParticipationRef) -> Self {
        Self::ParticipationLeaseExpired { participation }
    }

    pub fn participation_termination_completed(
        participation: ParticipationRef,
        cause: Option<&TerminationDetail>,
        retained_tasks: usize,
    ) -> Self {
        let cause = match cause {
            Some(TerminationDetail::Canceled(reason)) => reason.as_str(),
            Some(TerminationDetail::Aborted(cause)) => cause.as_str(),
            Some(TerminationDetail::Failed(_)) => "TASK_FAILED",
            None => "NONE",
        };
        Self::ParticipationTerminationCompleted {
            participation,
            cause,
            retained_tasks,
        }
    }

    pub const fn task_terminal_retained(identity: TaskIdentity, state: TaskState) -> Self {
        Self::TaskTerminalRetained { identity, state }
    }

    /// Maps an abort receipt to the one operation that delivered cancellation.
    pub fn abort_participation(
        participation: ParticipationRef,
        receipt: &ParticipationOutcome,
    ) -> Option<Self> {
        (receipt.outcome() == OperationOutcome::Accepted)
            .then_some(Self::ParticipationAbortApplied { participation })
    }
}

#[cfg(test)]
mod tests {
    use super::{RuntimeFilterReleaseObservation, TaskProtocolEvent};
    use crate::{
        CreateTaskOutcome, OperationReceipt, ParticipationOutcome, ReleaseAcknowledgement,
        ReleaseParticipationOutcome,
    };
    use novarocks_execution_contract::task_execution::identity::{
        ParticipationRef, TaskIdentity, TaskOperationId,
    };
    use novarocks_execution_contract::task_execution::operation::{
        OperationOutcome, ReleaseOutcome,
    };
    use novarocks_execution_contract::task_execution::transition::ParticipationState;
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };

    fn participation() -> ParticipationRef {
        ParticipationRef::new(
            QueryExecutionId::new(
                QueryId::new(7, 11),
                AttemptId::new(1).expect("attempt is nonzero"),
            )
            .expect("query id is nonzero"),
            FrontendProcessId::new_v7(),
            BackendProcessId::new_v7(),
        )
    }

    fn task(participation: ParticipationRef) -> TaskIdentity {
        TaskIdentity::new(
            participation.query_execution_id(),
            StageId::new(1).expect("stage is nonzero"),
            TaskId::new(1).expect("task is nonzero"),
            participation.backend_process_id(),
        )
    }

    #[test]
    fn only_domain_progressions_become_events() {
        let participation = participation();
        let establish: ParticipationOutcome = OperationReceipt::rejected(
            TaskOperationId::new_v7(),
            OperationOutcome::ParticipationConflict,
            "test",
        );
        assert!(matches!(
            TaskProtocolEvent::establish_participation(participation, &establish),
            Some(TaskProtocolEvent::ParticipationEstablishConflict { participation: actual }) if actual == participation
        ));

        let create: CreateTaskOutcome = OperationReceipt::rejected(
            TaskOperationId::new_v7(),
            OperationOutcome::Idempotent,
            "test",
        );
        assert!(matches!(
            TaskProtocolEvent::create_task(task(participation), &create),
            Some(TaskProtocolEvent::TaskCreateIdempotent { .. })
        ));

        let rejected: ParticipationOutcome = OperationReceipt::rejected(
            TaskOperationId::new_v7(),
            OperationOutcome::InvalidStateOrRequest,
            "test",
        );
        assert_eq!(
            TaskProtocolEvent::abort_participation(participation, &rejected),
            None
        );
    }

    #[test]
    fn release_event_requires_a_real_release() {
        let participation = participation();
        let receipt: ReleaseParticipationOutcome = OperationReceipt::acknowledged(
            TaskOperationId::new_v7(),
            OperationOutcome::Accepted,
            ReleaseAcknowledgement::new(
                participation,
                ReleaseOutcome::Released,
                ParticipationState::Releasing,
                None,
            ),
        );
        assert!(matches!(
            TaskProtocolEvent::release_participation(
                participation,
                &receipt,
                RuntimeFilterReleaseObservation::Available,
            ),
            Some(TaskProtocolEvent::ParticipationReleaseApplied {
                runtime_filter: RuntimeFilterReleaseObservation::Available,
                ..
            })
        ));
    }
}
