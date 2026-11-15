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

//! Pure projection from attempt-owned failure facts to shared passive values.

use crate::{
    error::{ParticipantObservationFailure, TaskExecutionError as ProtocolError},
    failure::{AttemptFailure, AttemptFailureSite, HostFailureRef, ProtocolFailure},
    operation::OperationDispatchResult,
};
use novarocks_coordination_contract::{
    ExecutionFailure as ProjectedAttemptFailure, ExecutionFailureInput, ExecutionFailureSite,
    PreReadyProcessReplacementWitness, QueryFailureReason,
};
use novarocks_execution_contract::{
    OperationKind, OperationOutcome, ParticipationRef, SafeDetail, TaskOperationId,
    failure::{FailureFact, FailureProjection},
    operation::OperationResult,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FailureProjectionError {
    MissingOriginalHostFailure(HostFailureRef),
    MissingSelectedTaskCause,
    InvalidReplacement,
}

/// Canonical typed Worker verdict projection, shared with the original host boundary.
pub fn operation_failure_fact(outcome: OperationOutcome) -> FailureFact {
    TaskProtocolFailureReason::terminal_verdict(outcome).failure_fact()
}

/// Preserve the producer fact, safe detail and exact original operation identity.
pub fn operation_failure_input(
    participation: ParticipationRef,
    operation_id: TaskOperationId,
    kind: OperationKind,
    rejection: &OperationResult<std::convert::Infallible>,
) -> ExecutionFailureInput {
    let site = ExecutionFailureSite::Operation {
        participation,
        operation_id,
        kind,
    };
    match rejection {
        OperationResult::DomainFailure { failure, .. } => {
            ExecutionFailureInput::new(failure.fact(), failure.detail().clone(), site)
        }
        OperationResult::Verdict {
            outcome, detail, ..
        } => ExecutionFailureInput::new(
            operation_failure_fact(*outcome),
            detail
                .clone()
                .unwrap_or_else(|| SafeDetail::truncating(&format!("{kind} returned {outcome:?}"))),
            site,
        ),
    }
}

pub fn project_failure(
    failure: &AttemptFailure,
    resolve_host: &mut impl FnMut(HostFailureRef) -> Option<ExecutionFailureInput>,
) -> Result<ProjectedAttemptFailure, FailureProjectionError> {
    let input = match failure {
        AttemptFailure::OwnerFailed => owner_failure(),
        AttemptFailure::Protocol(original) => protocol_failure(original),
        AttemptFailure::TaskProtocol(original) => project_task_failure(original, resolve_host)?,
        AttemptFailure::Host(original) => resolve_host(original.original).ok_or(
            FailureProjectionError::MissingOriginalHostFailure(original.original),
        )?,
        AttemptFailure::SelectedTask(original) => {
            ExecutionFailureInput::from_selected_task(original)
                .ok_or(FailureProjectionError::MissingSelectedTaskCause)?
        }
        AttemptFailure::EstablishReplayExhausted { participation, .. } => failure_input(
            QueryFailureReason::ObservationLost,
            "frontend task protocol could not continue",
            ExecutionFailureSite::Backend {
                process: participation.backend_process_id(),
            },
        ),
        AttemptFailure::BackendReplaced {
            original,
            replacement,
        } => {
            if original == replacement {
                return Err(FailureProjectionError::InvalidReplacement);
            }
            failure_input(
                QueryFailureReason::BackendLost,
                "the frozen pre-ready backend process was replaced",
                ExecutionFailureSite::Backend { process: *original },
            )
        }
        AttemptFailure::PreReadyReplacement {
            refusal,
            participation,
            operation_id,
            kind,
            replacement,
        } => {
            let input =
                replacement_refusal(refusal).ok_or(FailureProjectionError::InvalidReplacement)?;
            let mut projected = ProjectedAttemptFailure::from_input(input);
            if projected.input().site()
                == (ExecutionFailureSite::Operation {
                    participation: *participation,
                    operation_id: *operation_id,
                    kind: *kind,
                })
                && let Ok(witness) = PreReadyProcessReplacementWitness::try_new(
                    projected.input(),
                    *participation,
                    *operation_id,
                    *replacement,
                )
            {
                projected
                    .attach_pre_ready_replacement(witness)
                    .expect("same original refusal");
            }
            return Ok(projected);
        }
    };
    Ok(ProjectedAttemptFailure::from_input(input))
}

fn project_task_failure(
    original: &ProtocolError,
    resolve_host: &mut impl FnMut(HostFailureRef) -> Option<ExecutionFailureInput>,
) -> Result<ExecutionFailureInput, FailureProjectionError> {
    use ProtocolError as E;
    const DETAIL: &str = "frontend task protocol could not continue";
    let input = match original {
        E::Workload(host)
        | E::HostExecution(host)
        | E::SplitAssignment(host)
        | E::Actor(host)
        | E::NativeTransport(host)
        | E::Establish(host) => resolve_host(host.original).ok_or(
            FailureProjectionError::MissingOriginalHostFailure(host.original),
        )?,
        E::Execution(original) => protocol_failure(original),
        E::PreReadyEstablishRejected {
            participation,
            operation_id,
            rejection,
        } => operation_failure_input(
            *participation,
            *operation_id,
            OperationKind::UpdateParticipation,
            rejection,
        ),
        E::OperationFailed {
            participation,
            operation_id,
            kind,
            rejection,
            ..
        } => operation_failure_input(*participation, *operation_id, *kind, rejection),
        E::Descriptor(error) => ExecutionFailureInput::new(
            error.failure_fact(),
            SafeDetail::truncating("task descriptor violates its frozen contract"),
            ExecutionFailureSite::Coordinator,
        ),
        E::Request(error) => ExecutionFailureInput::new(
            error.failure_fact(),
            SafeDetail::truncating("task operation request violates its frozen contract"),
            ExecutionFailureSite::Coordinator,
        ),
        E::DomainRegression { conflict, .. } => ExecutionFailureInput::new(
            conflict.failure_fact(),
            SafeDetail::truncating(DETAIL),
            ExecutionFailureSite::Coordinator,
        ),
        E::Capacity(bound) => ExecutionFailureInput::new(
            bound.failure_fact(),
            SafeDetail::truncating("frontend task protocol capacity bound was exceeded"),
            ExecutionFailureSite::Coordinator,
        ),
        E::PreReadyEstablishTransportUnknown { backend }
        | E::ParticipantUnobservable {
            backend,
            state: ParticipantObservationFailure::Transport(_),
        } => failure_input(
            QueryFailureReason::ObservationLost,
            DETAIL,
            ExecutionFailureSite::Backend { process: *backend },
        ),
        E::ParticipantUnobservable {
            backend,
            state: ParticipantObservationFailure::ConnectionLost,
        } => failure_input(
            QueryFailureReason::BackendLost,
            DETAIL,
            ExecutionFailureSite::Backend { process: *backend },
        ),
        E::QueueResidenceExpired { .. } => failure_input(
            QueryFailureReason::TransportDeadline,
            DETAIL,
            ExecutionFailureSite::Coordinator,
        ),
        E::DispatchRejected {
            result: OperationDispatchResult::TransportUnknown,
            ..
        } => failure_input(
            QueryFailureReason::ObservationLost,
            DETAIL,
            ExecutionFailureSite::Coordinator,
        ),
        E::DispatchRejected {
            result: OperationDispatchResult::IngressRejected(_),
            ..
        } => failure_input(
            QueryFailureReason::TransportCapacity,
            DETAIL,
            ExecutionFailureSite::Coordinator,
        ),
        E::DispatchRejected {
            result: OperationDispatchResult::WorkerReceipt(receipt),
            ..
        } => ExecutionFailureInput::new(
            operation_failure_fact(receipt.outcome()),
            SafeDetail::truncating(DETAIL),
            ExecutionFailureSite::Coordinator,
        ),
        E::Schedule(_)
        | E::UnknownOperation
        | E::MissingReceipt(_)
        | E::DomainReceipt(_)
        | E::Identity(_)
        | E::EdgeAlreadyOpened(_)
        | E::Observation(_)
        | E::IllegalTaskTransition { .. }
        | E::FinalInfo(_)
        | E::ParticipantUnobservable {
            state: ParticipantObservationFailure::IdentityViolation(_),
            ..
        }
        | E::DispatchRejected {
            result: OperationDispatchResult::NonWorkerRejected,
            ..
        } => failure_input(
            QueryFailureReason::PreparationProtocol,
            DETAIL,
            ExecutionFailureSite::Coordinator,
        ),
    };
    Ok(input)
}

fn protocol_failure(original: &ProtocolFailure) -> ExecutionFailureInput {
    use AttemptFailureSite as S;
    let site = match original.site {
        S::Attempt => ExecutionFailureSite::Coordinator,
        S::Participation(participation) => ExecutionFailureSite::Backend {
            process: participation.backend_process_id(),
        },
        S::Task { identity, phase } => ExecutionFailureSite::Task { identity, phase },
        S::TaskControl { identity } => ExecutionFailureSite::TaskControl { identity },
        S::RootRead { root } => ExecutionFailureSite::RootRead { root },
        S::Backend { process } => ExecutionFailureSite::Backend { process },
        S::Operation {
            participation,
            operation_id,
            kind,
        } => ExecutionFailureSite::Operation {
            participation,
            operation_id,
            kind,
        },
    };
    ExecutionFailureInput::new(original.fact, original.detail.clone(), site)
}

pub fn failure_input(
    reason: QueryFailureReason,
    detail: &str,
    site: ExecutionFailureSite,
) -> ExecutionFailureInput {
    ExecutionFailureInput::new(reason.failure_fact(), SafeDetail::truncating(detail), site)
}

pub fn owner_failure() -> ExecutionFailureInput {
    failure_input(
        QueryFailureReason::NativeFuturePanicked,
        "Native attempt Core failed",
        ExecutionFailureSite::Coordinator,
    )
}

pub fn replacement_refusal(refusal: &AttemptFailure) -> Option<ExecutionFailureInput> {
    use crate::error::TaskExecutionError as ProtocolError;
    use novarocks_execution_contract::{
        OperationKind, OperationOutcome, operation::OperationResult,
    };

    let AttemptFailure::TaskProtocol(original) = refusal else {
        return None;
    };
    let (participation, operation_id, kind, rejection) = match original.as_ref() {
        ProtocolError::PreReadyEstablishRejected {
            participation,
            operation_id,
            rejection,
        } => (
            *participation,
            *operation_id,
            OperationKind::UpdateParticipation,
            rejection,
        ),
        ProtocolError::OperationFailed {
            task: Some(task),
            participation,
            operation_id,
            kind: OperationKind::CreateTask,
            rejection,
        } if task.backend_process_id() == participation.backend_process_id()
            && task.query_execution_id() == participation.query_execution_id() =>
        {
            (
                *participation,
                *operation_id,
                OperationKind::CreateTask,
                rejection,
            )
        }
        _ => return None,
    };
    if !matches!(
        rejection,
        OperationResult::Verdict {
            outcome: OperationOutcome::IdentityMismatch,
            ..
        }
    ) {
        return None;
    }
    Some(operation_failure_input(
        participation,
        operation_id,
        kind,
        rejection,
    ))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TaskProtocolFailureReason {
    OwnerFailed,
    OperationTimedOut,
    IdentityMismatch,
    CompatibilityMismatch,
    ParticipationNotEstablished,
    ParticipationConflict,
    DomainConflict,
    LeaseExpired,
    ParticipationTerminalReceipt,
    InvalidStateOrRequest,
    TerminalRejected,
    Gone,
    ResourceExhausted,
    NonterminalVerdictAtFailure,
}
impl TaskProtocolFailureReason {
    fn terminal_verdict(outcome: OperationOutcome) -> Self {
        match outcome {
            OperationOutcome::OwnerFailed => Self::OwnerFailed,
            OperationOutcome::OperationTimedOut => Self::OperationTimedOut,
            OperationOutcome::IdentityMismatch => Self::IdentityMismatch,
            OperationOutcome::CompatibilityMismatch => Self::CompatibilityMismatch,
            OperationOutcome::ParticipationNotEstablished => Self::ParticipationNotEstablished,
            OperationOutcome::ParticipationConflict => Self::ParticipationConflict,
            OperationOutcome::DomainConflict => Self::DomainConflict,
            OperationOutcome::LeaseExpired => Self::LeaseExpired,
            OperationOutcome::ParticipationTerminalReceipt => Self::ParticipationTerminalReceipt,
            OperationOutcome::InvalidStateOrRequest => Self::InvalidStateOrRequest,
            OperationOutcome::TerminalRejected => Self::TerminalRejected,
            OperationOutcome::Gone => Self::Gone,
            OperationOutcome::ResourceExhausted => Self::ResourceExhausted,
            // These are success/wait facts, not failure authors. Reaching a
            // terminal-only error branch is a local protocol invariant;
            // it cannot manufacture Resource, Task acceptance or BackendLost.
            OperationOutcome::Accepted
            | OperationOutcome::Idempotent
            | OperationOutcome::ReleaseNotReady
            | OperationOutcome::NotReady
            | OperationOutcome::PreparationBusy
            | OperationOutcome::AdmissionTicketStillActive => Self::NonterminalVerdictAtFailure,
        }
    }
}
impl novarocks_execution_contract::failure::FailureProjection for TaskProtocolFailureReason {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::{
            FailureClass, FailureFact, FailureReason, domain,
        };
        let (class, code) = match self {
            Self::OwnerFailed => (FailureClass::Invariant, 28),
            Self::OperationTimedOut => (FailureClass::Dependency, 14),
            Self::IdentityMismatch => {
                return FailureFact::new(
                    FailureClass::Invariant,
                    novarocks_coordination_contract::TASK_PROTOCOL_IDENTITY_MISMATCH_REASON,
                );
            }
            Self::CompatibilityMismatch => (FailureClass::Invariant, 16),
            Self::ParticipationNotEstablished => (FailureClass::Invariant, 17),
            Self::ParticipationConflict => (FailureClass::Invariant, 18),
            Self::DomainConflict => (FailureClass::Invariant, 19),
            Self::LeaseExpired => (FailureClass::Control, 20),
            Self::ParticipationTerminalReceipt => (FailureClass::Control, 21),
            Self::InvalidStateOrRequest => (FailureClass::Invariant, 22),
            Self::TerminalRejected => (FailureClass::Control, 23),
            Self::Gone => (FailureClass::Control, 24),
            Self::ResourceExhausted => (FailureClass::Resource, 25),
            Self::NonterminalVerdictAtFailure => (FailureClass::Invariant, 26),
        };
        FailureFact::new(
            class,
            FailureReason::new(domain::QUERY, std::num::NonZeroU16::new(code).unwrap()),
        )
    }
}

/// The same protocol reason author preserves a locally refused operation's
/// fact without claiming that a Worker sent a receipt.
pub fn local_protocol_refusal(
    kind: OperationKind,
    outcome: OperationOutcome,
) -> novarocks_coordination_contract::ExecutionFailureInput {
    use novarocks_coordination_contract::{ExecutionFailureInput, ExecutionFailureSite};
    use novarocks_execution_contract::{SafeDetail, failure::FailureProjection};
    ExecutionFailureInput::new(
        TaskProtocolFailureReason::terminal_verdict(outcome).failure_fact(),
        SafeDetail::truncating(&format!("{kind} returned {outcome:?}")),
        ExecutionFailureSite::Coordinator,
    )
}
