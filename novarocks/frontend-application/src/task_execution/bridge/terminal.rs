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

//! Normal terminal delivery to the original logical attempt.

use crate::task_execution::error::TaskExecutionError;
use novarocks_attempt_protocol::{
    error::{ParticipantObservationFailure, TaskExecutionError as ProtocolError},
    failure::{AttemptFailure, AttemptFailureSite, HostFailureRef, ProtocolFailure},
    operation::OperationDispatchResult,
};
use novarocks_coordination_contract::{
    AttemptActivationIdentity, AttemptTerminal, AttemptToLogical, AttemptToLogicalBody,
};
use novarocks_execution_contract::{SafeDetail, failure::FailureProjection};
use novarocks_query_application::{
    api::{
        NativeAttemptPreparationFailure, NativeAttemptTerminal, PreReadyProcessReplacementWitness,
    },
    execution_error::{ExecutionFailureInput, ExecutionFailureSite, QueryFailureReason},
};
use std::sync::{Arc, OnceLock};
use tokio::sync::Notify;

pub(crate) struct TerminalOutbox {
    activation: AttemptActivationIdentity,
    terminal: OnceLock<AttemptToLogical<AttemptFailure>>,
    ready: Notify,
}

impl TerminalOutbox {
    pub(crate) fn new(activation: AttemptActivationIdentity) -> Arc<Self> {
        Arc::new(Self {
            activation,
            terminal: OnceLock::new(),
            ready: Notify::new(),
        })
    }

    pub(crate) fn publish(
        &self,
        original: AttemptToLogical<AttemptFailure>,
    ) -> Result<(), AttemptToLogical<AttemptFailure>> {
        if original.activation != self.activation
            || !matches!(original.body, AttemptToLogicalBody::AttemptTerminal(_))
        {
            return Err(original);
        }
        self.terminal.set(original)?;
        self.ready.notify_one();
        Ok(())
    }

    /// Borrowing the outbox across a cancelled wait keeps the original result.
    /// The logical owner resolves any host reference against its original
    /// typed failure records before it asks QA to choose a recovery action.
    pub(crate) async fn terminal(
        &self,
        resolve_host: impl FnMut(HostFailureRef) -> Option<ExecutionFailureInput>,
    ) -> Result<NativeAttemptTerminal, TerminalProjectionError> {
        let port = loop {
            if let Some(port) = self.terminal.get() {
                break port;
            }
            self.ready.notified().await;
        };
        let AttemptToLogicalBody::AttemptTerminal(terminal) = &port.body else {
            unreachable!("validated original terminal publication");
        };
        project_terminal(terminal, resolve_host)
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum TerminalProjectionError {
    MissingOriginalHostFailure(HostFailureRef),
    MissingSelectedTaskCause,
    InvalidReplacement,
}

fn project_terminal(
    terminal: &AttemptTerminal<AttemptFailure>,
    mut resolve_host: impl FnMut(HostFailureRef) -> Option<ExecutionFailureInput>,
) -> Result<NativeAttemptTerminal, TerminalProjectionError> {
    let failure = match terminal {
        AttemptTerminal::Completed => return Ok(NativeAttemptTerminal::Completed),
        AttemptTerminal::Failed(failure) | AttemptTerminal::Stopped { failure, .. } => failure,
    };
    Ok(NativeAttemptTerminal::Failed(project_failure(
        failure,
        &mut resolve_host,
    )?))
}

fn project_failure(
    failure: &AttemptFailure,
    resolve_host: &mut impl FnMut(HostFailureRef) -> Option<ExecutionFailureInput>,
) -> Result<NativeAttemptPreparationFailure, TerminalProjectionError> {
    let input = match failure {
        AttemptFailure::OwnerFailed => owner_failure(),
        AttemptFailure::Protocol(original) => protocol_failure(original),
        AttemptFailure::TaskProtocol(original) => project_task_failure(original, resolve_host)?,
        AttemptFailure::Host(original) => resolve_host(original.original).ok_or(
            TerminalProjectionError::MissingOriginalHostFailure(original.original),
        )?,
        AttemptFailure::SelectedTask(original) => {
            ExecutionFailureInput::from_selected_task(original)
                .ok_or(TerminalProjectionError::MissingSelectedTaskCause)?
        }
        AttemptFailure::EstablishReplayExhausted { participation, .. } => {
            TaskExecutionError::PreReadyEstablishTransportUnknown {
                backend: participation.backend_process_id(),
            }
            .preparation_input()
        }
        AttemptFailure::BackendReplaced {
            original,
            replacement,
        } => {
            if original == replacement {
                return Err(TerminalProjectionError::InvalidReplacement);
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
                replacement_refusal(refusal).ok_or(TerminalProjectionError::InvalidReplacement)?;
            let mut projected = NativeAttemptPreparationFailure::from_input(input);
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
    Ok(NativeAttemptPreparationFailure::from_input(input))
}

fn project_task_failure(
    original: &ProtocolError,
    resolve_host: &mut impl FnMut(HostFailureRef) -> Option<ExecutionFailureInput>,
) -> Result<ExecutionFailureInput, TerminalProjectionError> {
    use ProtocolError as E;
    let input = match original {
        E::Workload(host)
        | E::HostExecution(host)
        | E::SplitAssignment(host)
        | E::Actor(host)
        | E::NativeTransport(host)
        | E::Establish(host) => resolve_host(host.original).ok_or(
            TerminalProjectionError::MissingOriginalHostFailure(host.original),
        )?,
        E::Execution(original) => protocol_failure(original),
        E::PreReadyEstablishRejected {
            participation,
            operation_id,
            rejection,
        } => TaskExecutionError::PreReadyEstablishRejected {
            participation: *participation,
            operation_id: *operation_id,
            rejection: rejection.clone(),
        }
        .preparation_input(),
        E::OperationFailed {
            participation,
            operation_id,
            kind,
            rejection,
            ..
        } => TaskExecutionError::OperationFailed {
            participation: *participation,
            operation_id: *operation_id,
            kind: *kind,
            rejection: rejection.clone(),
        }
        .preparation_input(),
        E::Descriptor(original) => {
            TaskExecutionError::Descriptor(original.clone()).preparation_input()
        }
        E::Request(original) => TaskExecutionError::Request(original.clone()).preparation_input(),
        E::DomainRegression {
            domain,
            token,
            conflict,
        } => TaskExecutionError::DomainRegression {
            domain,
            token: token.clone(),
            conflict: conflict.clone(),
        }
        .preparation_input(),
        E::Capacity(bound) => ExecutionFailureInput::new(
            bound.failure_fact(),
            SafeDetail::truncating("frontend task protocol capacity bound was exceeded"),
            ExecutionFailureSite::Coordinator,
        ),
        E::PreReadyEstablishTransportUnknown { backend } => {
            TaskExecutionError::PreReadyEstablishTransportUnknown { backend: *backend }
                .preparation_input()
        }
        E::ParticipantUnobservable {
            backend,
            state: ParticipantObservationFailure::ConnectionLost,
        } => failure_input(
            QueryFailureReason::BackendLost,
            "frontend task protocol could not continue",
            ExecutionFailureSite::Backend { process: *backend },
        ),
        E::ParticipantUnobservable {
            backend,
            state: ParticipantObservationFailure::Transport(_),
        } => failure_input(
            QueryFailureReason::ObservationLost,
            "frontend task protocol could not continue",
            ExecutionFailureSite::Backend { process: *backend },
        ),
        E::QueueResidenceExpired { .. } => failure_input(
            QueryFailureReason::TransportDeadline,
            "frontend task protocol could not continue",
            ExecutionFailureSite::Coordinator,
        ),
        E::DispatchRejected {
            result: OperationDispatchResult::TransportUnknown,
            ..
        } => failure_input(
            QueryFailureReason::ObservationLost,
            "frontend task protocol could not continue",
            ExecutionFailureSite::Coordinator,
        ),
        E::DispatchRejected {
            result: OperationDispatchResult::IngressRejected(_),
            ..
        } => failure_input(
            QueryFailureReason::TransportCapacity,
            "frontend task protocol could not continue",
            ExecutionFailureSite::Coordinator,
        ),
        E::DispatchRejected {
            kind,
            result: OperationDispatchResult::WorkerReceipt(receipt),
        } => ExecutionFailureInput::new(
            crate::task_execution::error::local_protocol_refusal(*kind, receipt.outcome()).fact(),
            SafeDetail::truncating("frontend task protocol could not continue"),
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
            "frontend task protocol could not continue",
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

fn failure_input(
    reason: QueryFailureReason,
    detail: &str,
    site: ExecutionFailureSite,
) -> ExecutionFailureInput {
    ExecutionFailureInput::new(reason.failure_fact(), SafeDetail::truncating(detail), site)
}

pub(crate) fn owner_failure() -> ExecutionFailureInput {
    failure_input(
        QueryFailureReason::NativeFuturePanicked,
        "Native attempt Core failed",
        ExecutionFailureSite::Coordinator,
    )
}

pub(super) fn replacement_refusal(
    refusal: &AttemptFailure,
) -> Option<novarocks_query_application::execution_error::ExecutionFailureInput> {
    use novarocks_attempt_protocol::error::TaskExecutionError as ProtocolError;
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
    Some(
        TaskExecutionError::OperationFailed {
            participation,
            operation_id,
            kind,
            rejection: rejection.clone(),
        }
        .preparation_input(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_coordination_contract::AttemptActionId;
    use novarocks_execution_contract::{
        OperationKind, OperationOutcome, ParticipationRef, TaskIdentity, TaskOperationId,
        operation::OperationResult,
    };
    use novarocks_query_application::api::NativeAttemptTopologyRequirement;
    use novarocks_types::{
        AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    use std::num::NonZeroU64;

    fn participation() -> ParticipationRef {
        ParticipationRef::new(
            QueryExecutionId::new(QueryId::new(0x7a5c, 6), AttemptId::new(1).unwrap()).unwrap(),
            FrontendProcessId::new_v7(),
            BackendProcessId::new_v7(),
        )
    }

    #[test]
    fn replacement_keeps_the_original_establish_or_create_refusal() {
        let original = participation();
        let operation_id = TaskOperationId::new_v7();
        let replacement = BackendProcessId::new_v7();
        let rejection = OperationResult::Verdict {
            outcome: OperationOutcome::IdentityMismatch,
            detail: Some(SafeDetail::truncating("original worker refusal")),
            acknowledgement: None,
        };
        let task = TaskIdentity::new(
            original.query_execution_id(),
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            original.backend_process_id(),
        );
        for (kind, refused) in [
            (
                OperationKind::UpdateParticipation,
                ProtocolError::PreReadyEstablishRejected {
                    participation: original,
                    operation_id,
                    rejection: rejection.clone(),
                },
            ),
            (
                OperationKind::CreateTask,
                ProtocolError::OperationFailed {
                    task: Some(task),
                    participation: original,
                    operation_id,
                    kind: OperationKind::CreateTask,
                    rejection: rejection.clone(),
                },
            ),
        ] {
            let refusal = AttemptFailure::TaskProtocol(Box::new(refused));
            let projected = replacement_refusal(&refusal).unwrap();
            let selected = |evidence_operation| {
                AttemptTerminal::Failed(AttemptFailure::PreReadyReplacement {
                    refusal: Box::new(refusal.clone()),
                    participation: original,
                    operation_id: evidence_operation,
                    kind,
                    replacement,
                })
            };
            let NativeAttemptTerminal::Failed(actual) =
                project_terminal(&selected(operation_id), |_| None).unwrap()
            else {
                panic!("the same refusal stays failed");
            };
            assert_eq!(actual.input(), &projected);
            assert_eq!(
                actual
                    .pre_ready_replacement()
                    .unwrap()
                    .replacement_process(),
                replacement
            );
            assert_eq!(
                actual.topology_requirement(),
                NativeAttemptTopologyRequirement::ExcludeProcess(original.backend_process_id())
            );
            let NativeAttemptTerminal::Failed(wrong) =
                project_terminal(&selected(TaskOperationId::new_v7()), |_| None).unwrap()
            else {
                panic!("the same refusal stays failed");
            };
            assert_eq!(wrong.input(), &projected);
            assert!(wrong.pre_ready_replacement().is_none());
        }
    }

    #[tokio::test]
    async fn terminal_reentry_uses_only_the_original_typed_host_failure() {
        let activation =
            AttemptActivationIdentity::from_parts(1, participation().query_execution_id(), 1);
        let reference = HostFailureRef(NonZeroU64::MIN);
        let input = owner_failure();
        let outbox = TerminalOutbox::new(activation);
        let port = AttemptToLogical {
            activation,
            action: AttemptActionId::new(NonZeroU64::MIN),
            body: AttemptToLogicalBody::AttemptTerminal(AttemptTerminal::Failed(
                AttemptFailure::Host(novarocks_attempt_protocol::failure::HostProjectedFailure {
                    original: reference,
                    kind: novarocks_attempt_protocol::failure::HostFailureKind::Execution,
                    class: input.attempt_class(),
                    site: AttemptFailureSite::Attempt,
                }),
            )),
        };
        assert!(outbox.publish(port.clone()).is_ok());
        assert!(outbox.publish(port).is_err());
        assert!(
            matches!(outbox.terminal(|_| None).await, Err(TerminalProjectionError::MissingOriginalHostFailure(original)) if original == reference)
        );
        let NativeAttemptTerminal::Failed(actual) = outbox
            .terminal(|original| {
                assert_eq!(original, reference);
                Some(input.clone())
            })
            .await
            .unwrap()
        else {
            panic!("the exact typed original failure");
        };
        assert_eq!(actual.input(), &input);
    }
}
