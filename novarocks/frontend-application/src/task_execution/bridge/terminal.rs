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

//! Terminal delivery to the original logical attempt.

use crate::control_runtime::membership::{MembershipErrors, ObservationErrors};
use crate::task_execution::{
    error::TaskExecutionError,
    host::{failure_bridge::FailureBridge, publication::OriginalHostFailure},
};
use novarocks_attempt_protocol::{
    error::{ParticipantObservationFailure, TaskExecutionError as ProtocolError},
    failure::{AttemptFailure, AttemptFailureSite, HostFailureRef, ProtocolFailure},
    operation::OperationDispatchResult,
};
use novarocks_coordination_contract::{
    AttemptActionId, AttemptActivationIdentity, AttemptConvergenceClosure,
    AttemptParticipationConvergence, AttemptTerminal, AttemptToLogical, AttemptToLogicalBody,
};
use novarocks_execution_contract::{SafeDetail, failure::FailureProjection};
use novarocks_query_application::{
    api::{
        NativeActiveConvergenceClosure, NativeActiveConvergenceRequest, NativeAttemptConvergence,
        NativeAttemptPreparationFailure, NativeAttemptTerminal, NativeParticipationConvergence,
        PreReadyProcessReplacementWitness, StatementNativeHealthFailure,
    },
    execution_error::{ExecutionFailureInput, ExecutionFailureSite, QueryFailureReason},
};
use std::sync::{Arc, OnceLock};
use tokio::sync::Notify;

/// The selected raw host error travels with the normal terminal that names it.
pub(crate) struct TerminalPublication {
    port: AttemptToLogical<AttemptFailure>,
    failure: Option<(HostFailureRef, OriginalHostFailure)>,
}

impl TerminalPublication {
    pub(crate) fn new(port: AttemptToLogical<AttemptFailure>) -> Self {
        Self {
            port,
            failure: None,
        }
    }

    pub(crate) fn host_reference(&self) -> Option<HostFailureRef> {
        let AttemptToLogicalBody::AttemptTerminal(
            AttemptTerminal::Failed(failure) | AttemptTerminal::Stopped { failure, .. },
        ) = &self.port.body
        else {
            return None;
        };
        host_failure_reference(failure)
    }

    pub(crate) fn attach_host_failure(
        &mut self,
        reference: HostFailureRef,
        failure: OriginalHostFailure,
    ) {
        assert_eq!(self.host_reference(), Some(reference));
        assert!(self.failure.is_none());
        self.failure = Some((reference, failure));
    }
}

/// The Core reports its local convergence cut independently of remote facts.
pub(crate) struct ConvergencePublication {
    pub(crate) activation: AttemptActivationIdentity,
    pub(crate) action: AttemptActionId,
    pub(crate) local_exited: bool,
    pub(crate) closure: Option<Arc<AttemptConvergenceClosure>>,
}

#[derive(Default)]
struct ConvergenceOutbox {
    local: OnceLock<AttemptActionId>,
    remote: OnceLock<Arc<AttemptConvergenceClosure>>,
    ready: Notify,
}

impl ConvergenceOutbox {
    fn publish(&self, original: ConvergencePublication) -> Result<(), ConvergencePublication> {
        if (!original.local_exited && original.closure.is_none())
            || (original.closure.is_none() && self.local.get().is_some())
        {
            return Err(original);
        }
        if let Some(closure) = &original.closure
            && self.remote.set(Arc::clone(closure)).is_err()
        {
            return Err(original);
        }
        if original.local_exited {
            self.local.get_or_init(|| original.action);
        }
        self.ready.notify_one();
        Ok(())
    }

    fn report(&self) -> Option<&Arc<AttemptConvergenceClosure>> {
        self.local.get()?;
        self.remote.get()
    }

    /// The Core cut is separate from removal of the actual Host registration.
    async fn wait(&self) -> Arc<AttemptConvergenceClosure> {
        loop {
            if let Some(closure) = self.report() {
                return Arc::clone(closure);
            }
            self.ready.notified().await;
        }
    }
}

pub(crate) fn host_failure_reference(failure: &AttemptFailure) -> Option<HostFailureRef> {
    match failure {
        AttemptFailure::Host(host)
        | AttemptFailure::EstablishReplayExhausted { original: host, .. } => Some(host.original),
        AttemptFailure::TaskProtocol(original) => match original.as_ref() {
            ProtocolError::Workload(host)
            | ProtocolError::HostExecution(host)
            | ProtocolError::SplitAssignment(host)
            | ProtocolError::Actor(host)
            | ProtocolError::NativeTransport(host)
            | ProtocolError::Establish(host) => Some(host.original),
            _ => None,
        },
        _ => None,
    }
}

pub(crate) struct TerminalOutbox {
    activation: AttemptActivationIdentity,
    terminal: OnceLock<Box<TerminalPublication>>,
    convergence: ConvergenceOutbox,
    ready: Notify,
}

impl TerminalOutbox {
    pub(crate) fn new(activation: AttemptActivationIdentity) -> Arc<Self> {
        Arc::new(Self {
            activation,
            terminal: OnceLock::new(),
            convergence: ConvergenceOutbox::default(),
            ready: Notify::new(),
        })
    }

    pub(crate) fn publish(
        &self,
        original: Box<TerminalPublication>,
    ) -> Result<(), Box<TerminalPublication>> {
        if original.port.activation != self.activation
            || !matches!(original.port.body, AttemptToLogicalBody::AttemptTerminal(_))
        {
            return Err(original);
        }
        self.terminal.set(original)?;
        self.ready.notify_one();
        Ok(())
    }

    pub(crate) fn publish_convergence(
        &self,
        original: ConvergencePublication,
    ) -> Result<(), ConvergencePublication> {
        if original.activation != self.activation {
            return Err(original);
        }
        self.convergence.publish(original)
    }

    pub(crate) async fn normal_convergence(&self) -> Arc<AttemptConvergenceClosure> {
        self.convergence.wait().await
    }

    /// Borrowing the outbox across a cancelled wait keeps the original result.
    pub(crate) async fn terminal(&self) -> Result<NativeAttemptTerminal, TerminalProjectionError> {
        let publication = loop {
            if let Some(publication) = self.terminal.get() {
                break publication;
            }
            self.ready.notified().await;
        };
        let AttemptToLogicalBody::AttemptTerminal(terminal) = &publication.port.body else {
            unreachable!("validated original terminal publication");
        };
        if let Some(reference) = publication.host_reference()
            && !publication
                .failure
                .as_ref()
                .is_some_and(|(original, _)| *original == reference)
        {
            return Err(TerminalProjectionError::MissingOriginalHostFailure(
                reference,
            ));
        }
        project_terminal(terminal, |reference| {
            let (original, failure) = publication.failure.as_ref()?;
            (*original == reference).then(|| failure.preparation_input())
        })
    }
}

/// One logical receiver selects a terminal while retaining the original
/// failure-convergence bridge even when a normal result arrives first.
pub(crate) struct RegisteredAttemptTerminal {
    normal: Arc<TerminalOutbox>,
    failure: FailureBridge,
    membership_errors: Option<MembershipErrors>,
    selected: Option<Result<NativeAttemptTerminal, TerminalProjectionError>>,
}

impl RegisteredAttemptTerminal {
    pub(crate) fn new(
        normal: Arc<TerminalOutbox>,
        failure: FailureBridge,
        membership_errors: Option<MembershipErrors>,
    ) -> Self {
        Self {
            normal,
            failure,
            membership_errors,
            selected: None,
        }
    }

    pub(crate) fn take_membership_errors(&self) -> ObservationErrors {
        self.membership_errors
            .as_ref()
            .map(MembershipErrors::take)
            .unwrap_or_default()
    }

    pub(crate) async fn terminal(
        &mut self,
    ) -> Result<NativeAttemptTerminal, TerminalProjectionError> {
        if let Some(original) = &self.selected {
            return original.clone();
        }
        let terminal = tokio::select! {
            biased;
            normal = self.normal.terminal() => normal,
            lost = self.failure.terminal() => Ok(lost),
        };
        self.selected = Some(terminal.clone());
        terminal
    }

    /// Wait for both protocol reports, or yield to the original loss path.
    /// This observation still leaves the caller-held positions live.
    pub(crate) async fn normal_convergence_ready(&mut self) -> bool {
        tokio::select! {
            biased;
            _ = self.failure.terminal() => false,
            _ = self.normal.normal_convergence() => true,
        }
    }

    /// Complete a submitted convergence through its original registered host.
    /// A late Core loss transfers the same remote responsibility to the failure
    /// bridge even if the normal protocol reports arrived first.
    pub(crate) async fn finish_convergence(
        &mut self,
        request: NativeActiveConvergenceRequest,
    ) -> Result<NativeActiveConvergenceClosure, StatementNativeHealthFailure> {
        let normal = self.normal_convergence_ready().await;
        self.request_host_close().map_err(|error| {
            StatementNativeHealthFailure::actual_exit_unproven(
                "Native convergence could not close its original host",
                Arc::new(error),
            )
        })?;
        if normal && let Some(closure) = self.normal_closure_after_exit().await? {
            return Ok(closure);
        }
        self.failure.converge(request).await
    }

    /// The application returns the original caller-held positions before
    /// asking this exact host to finish its Source and Scope retirement.
    pub(crate) fn request_host_close(
        &self,
    ) -> Result<(), novarocks_control_runtime::actor_turn::ActorTurnError> {
        self.failure.request_host_close()
    }

    /// Use the failure bridge's residual path when the original Core loses
    /// ownership before the actual host exits.
    pub(crate) async fn normal_closure_after_exit(
        &mut self,
    ) -> Result<Option<NativeActiveConvergenceClosure>, StatementNativeHealthFailure> {
        if !self.failure.normal_host_exit().await? {
            return Ok(None);
        }
        let remote = self.normal.convergence.report().ok_or_else(|| {
            StatementNativeHealthFailure::actual_exit_unproven(
                "Native host exited without its original normal convergence reports",
                Arc::new(MissingNormalConvergence),
            )
        })?;
        Ok(Some(project_convergence(remote)))
    }

    pub(crate) fn failure_bridge_mut(&mut self) -> &mut FailureBridge {
        &mut self.failure
    }
}

#[derive(Debug)]
struct MissingNormalConvergence;

impl std::fmt::Display for MissingNormalConvergence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("missing normal convergence reports at actual Host exit")
    }
}

impl std::error::Error for MissingNormalConvergence {}

fn project_convergence(original: &AttemptConvergenceClosure) -> NativeActiveConvergenceClosure {
    use AttemptParticipationConvergence as Fact;
    match original {
        AttemptConvergenceClosure::Evidence(rows) => {
            NativeActiveConvergenceClosure::Evidence(NativeAttemptConvergence::participations(
                rows.iter()
                    .map(|&(participation, fact)| match fact {
                        Fact::NeverEstablished => {
                            NativeParticipationConvergence::never_established(participation)
                        }
                        Fact::WorkerStoppedAndParticipationFenced => {
                            NativeParticipationConvergence::worker_stopped_and_participation_fenced(
                                participation,
                            )
                        }
                        Fact::WorkerProcessReplaced => {
                            NativeParticipationConvergence::worker_process_replaced(participation)
                        }
                    })
                    .collect::<Box<[_]>>(),
            ))
        }
        AttemptConvergenceClosure::RemoteTrackingEnded {
            exact_participations,
        } => NativeActiveConvergenceClosure::RemoteTrackingEnded {
            exact_participations: exact_participations.as_ref().into(),
        },
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
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

pub(super) fn project_failure(
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
        let raw = TaskExecutionError::Schedule("original frozen schedule failure".into());
        let input = raw.preparation_input();
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
        let missing = TerminalOutbox::new(activation);
        assert!(
            missing
                .publish(Box::new(TerminalPublication::new(port.clone())))
                .is_ok()
        );
        assert_eq!(
            missing.terminal().await,
            Err(TerminalProjectionError::MissingOriginalHostFailure(
                reference
            )),
        );

        let mut publication = Box::new(TerminalPublication::new(port.clone()));
        assert_eq!(publication.host_reference(), Some(reference));
        publication.attach_host_failure(reference, OriginalHostFailure::Task(raw));
        assert!(outbox.publish(publication).is_ok());
        assert!(
            outbox
                .publish(Box::new(TerminalPublication::new(port)))
                .is_err()
        );
        let NativeAttemptTerminal::Failed(actual) = outbox.terminal().await.unwrap() else {
            panic!("the exact typed original failure");
        };
        assert_eq!(actual.input(), &input);
    }
}
