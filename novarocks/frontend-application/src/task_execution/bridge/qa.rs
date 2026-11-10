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

//! The credentialed caller of the original QA attempt issue protocol.

use std::{
    collections::BTreeMap,
    num::NonZeroU64,
    sync::{Arc, Mutex},
};

use crate::task_execution::{error::TaskExecutionError, host::transport::AttemptEstablishMaterial};
use novarocks_attempt_protocol::{
    effect::OperationIntent,
    failure::{
        AttemptFailure, AttemptFailureSite, HostFailureKind, HostFailureRef, HostProjectedFailure,
    },
};
use novarocks_coordination_contract::{
    AttemptActionId, AttemptAuthorization, AttemptAuthorizationDecision,
    AttemptAuthorizationFailure, AttemptAuthorizationRef, AttemptIssueSettlement,
    AttemptRequestToken, AttemptToLogical, AttemptToLogicalBody, LogicalToAttempt,
    LogicalToAttemptBody,
};
use novarocks_execution_contract::{ParticipationRef, TaskOperationId};
use novarocks_query_application::coordination::{
    AdmissionIssueDisposition, AdmissionIssueReceipt, AdmissionIssueSettlement,
    EstablishIssueError, EstablishIssueSubmit, EstablishTransportAdmission,
    EstablishTransportReservation, EstablishTransportSink, EstablishTransportSubmission,
    LateEstablishWorkerSettlement, LogicalExecutionActorError, NativeAttemptDrive,
};
use novarocks_types::NativeCompatibilityId;

enum Guard {
    Current(EstablishTransportSubmission),
    Late(LateEstablishWorkerSettlement),
}

impl Guard {
    fn matches(&self, participation: ParticipationRef, operation: TaskOperationId) -> bool {
        let identity = match self {
            Self::Current(original) => original.identity(),
            Self::Late(original) => original.identity(),
        };
        identity.participation() == participation && identity.operation_id() == operation
    }
}

#[must_use = "publish the original reply and retain its typed failure if present"]
pub(crate) struct AttemptQaReply {
    pub(in crate::task_execution) reply: LogicalToAttempt<AttemptFailure>,
    pub(in crate::task_execution) failure: Option<(HostFailureRef, TaskExecutionError)>,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum AttemptQaPortError {
    WrongRequest,
    WrongGuard,
    DuplicateAuthorization,
}

/// One original activation owns this inventory. Its credentialed executor
/// serializes the QA calls. A reference points to a real guard only after the
/// original QA issue successfully crosses the transport fence.
pub(crate) struct AttemptQaBridge {
    drive: Arc<NativeAttemptDrive>,
    compatibility: NativeCompatibilityId,
    material: Arc<AttemptEstablishMaterial>,
    admissions: BTreeMap<ParticipationRef, (AttemptAuthorizationRef, AdmissionIssueReceipt)>,
    guards: BTreeMap<AttemptAuthorizationRef, Guard>,
    permission_error: Option<LogicalExecutionActorError>,
}

impl AttemptQaBridge {
    pub(in crate::task_execution) fn activation(
        &self,
    ) -> novarocks_coordination_contract::AttemptActivationIdentity {
        self.drive.identity()
    }

    pub(in crate::task_execution) fn new(
        drive: Arc<NativeAttemptDrive>,
        compatibility: NativeCompatibilityId,
        material: Arc<AttemptEstablishMaterial>,
    ) -> Self {
        Self {
            drive,
            compatibility,
            material,
            admissions: BTreeMap::new(),
            guards: BTreeMap::new(),
            permission_error: None,
        }
    }

    pub(in crate::task_execution) async fn authorize(
        &mut self,
        operation: &OperationIntent,
        port: &AttemptToLogical<AttemptFailure>,
    ) -> Result<AttemptQaReply, AttemptQaPortError> {
        if port.activation != self.drive.identity() {
            return Err(AttemptQaPortError::WrongRequest);
        }
        let AttemptToLogicalBody::AuthorizationRequest { token, request } = port.body else {
            return Err(AttemptQaPortError::WrongRequest);
        };
        if !matches!(request, AttemptAuthorization::Establish { .. }) {
            return Err(AttemptQaPortError::WrongRequest);
        }
        let original = self
            .material
            .for_authorization(operation, request)
            .map_err(|_| AttemptQaPortError::WrongRequest)?;
        self.issue_establish(port, token, request, original).await
    }

    pub(in crate::task_execution) async fn handle_port(
        &mut self,
        port: &AttemptToLogical<AttemptFailure>,
    ) -> Result<AttemptQaReply, AttemptQaPortError> {
        if port.activation != self.drive.identity() {
            return Err(AttemptQaPortError::WrongRequest);
        }
        if let AttemptToLogicalBody::PreReadyReplacementQuery { token, refusal } = &port.body {
            let failure = replacement_refusal(refusal).ok_or(AttemptQaPortError::WrongRequest)?;
            let decision = self.drive.may_observe_pre_ready_replacement(&failure).await;
            self.permission_error = decision.as_ref().err().copied();
            return Ok(AttemptQaReply {
                reply: LogicalToAttempt {
                    activation: port.activation,
                    body: LogicalToAttemptBody::PreReadyReplacementReply {
                        token: *token,
                        may_observe: decision.unwrap_or(false),
                    },
                },
                failure: None,
            });
        }
        let AttemptToLogicalBody::AuthorizationRequest { token, request } = port.body else {
            return self.settle(port);
        };
        let decision = match request {
            AttemptAuthorization::BeginAdmission(original) => {
                let participation = original.participation();
                if original.native_compatibility_id() != self.compatibility {
                    return Err(AttemptQaPortError::WrongRequest);
                }
                if self.admissions.contains_key(&participation) {
                    return Err(AttemptQaPortError::DuplicateAuthorization);
                }
                let reference = AttemptAuthorizationRef::new(NonZeroU64::new(token.get()).unwrap());
                self.drive
                    .begin_admission_issue(original)
                    .await
                    .map(|issued| {
                        assert_eq!(issued.participation(), participation);
                        assert_eq!(issued.operation_id(), original.envelope().operation_id());
                        assert!(
                            self.admissions
                                .insert(participation, (reference, issued))
                                .is_none()
                        );
                        AttemptAuthorizationDecision::AdmissionStarted(reference)
                    })
            }
            AttemptAuthorization::SettleAdmission {
                issue,
                participation,
                operation_id,
                outcome,
                receipt,
                prior_transport_unknown,
            } => {
                let &(reference, original) = self
                    .admissions
                    .get(&participation)
                    .filter(|(reference, original)| {
                        *reference == issue && original.operation_id() == operation_id
                    })
                    .ok_or(AttemptQaPortError::WrongGuard)?;
                let settlement = match (outcome, receipt, prior_transport_unknown) {
                    (Some(outcome), Some(receipt), false) => {
                        AdmissionIssueSettlement::applied(operation_id, outcome, receipt)
                    }
                    (Some(outcome), None, false) => {
                        AdmissionIssueSettlement::rejected(operation_id, outcome)
                    }
                    (None, None, unknown) => Ok(
                        AdmissionIssueSettlement::pre_worker_rejected_with_prior_unknown(
                            operation_id,
                            unknown,
                        ),
                    ),
                    _ => return Err(AttemptQaPortError::WrongRequest),
                };
                match settlement {
                    Ok(settlement) => self
                        .drive
                        .settle_admission_issue(original, settlement)
                        .await
                        .map(|disposition| {
                            assert_eq!(
                                self.admissions.remove(&participation),
                                Some((reference, original))
                            );
                            match disposition {
                                AdmissionIssueDisposition::Granted => {
                                    AttemptAuthorizationDecision::AdmissionGranted
                                }
                                AdmissionIssueDisposition::Retryable => {
                                    AttemptAuthorizationDecision::AdmissionRetryable
                                }
                            }
                        }),
                    Err(error) => Err(LogicalExecutionActorError::Establish(error)),
                }
            }
            AttemptAuthorization::ReauthorizeEstablish {
                participation,
                operation_id,
            } => {
                let original = self
                    .material
                    .for_reauthorization(participation, operation_id)
                    .map_err(|_| AttemptQaPortError::WrongRequest)?;
                return self.issue_establish(port, token, request, original).await;
            }
            AttemptAuthorization::Establish { .. } => return Err(AttemptQaPortError::WrongRequest),
        };
        let (decision, failure) = match decision {
            Ok(decision) => (Ok(decision), None),
            Err(error) => {
                let (failure, original) = Self::project_failure(
                    port.action,
                    HostFailureKind::LogicalActor,
                    TaskExecutionError::Actor(error),
                );
                (
                    Err(AttemptAuthorizationFailure::Rejected(failure)),
                    Some(original),
                )
            }
        };
        Ok(AttemptQaReply {
            reply: LogicalToAttempt {
                activation: port.activation,
                body: LogicalToAttemptBody::AuthorizationReply { token, decision },
            },
            failure,
        })
    }

    async fn issue_establish(
        &mut self,
        port: &AttemptToLogical<AttemptFailure>,
        token: AttemptRequestToken,
        request: AttemptAuthorization,
        original: Arc<novarocks_execution_contract::EstablishParticipation>,
    ) -> Result<AttemptQaReply, AttemptQaPortError> {
        let reference = AttemptAuthorizationRef::new(NonZeroU64::new(token.get()).unwrap());
        if self.guards.contains_key(&reference) {
            return Err(AttemptQaPortError::DuplicateAuthorization);
        }
        let permitted = match request {
            AttemptAuthorization::Establish { .. } => {
                self.drive
                    .authorize_establish(original.clone(), self.compatibility)
                    .await
            }
            AttemptAuthorization::ReauthorizeEstablish { participation, .. } => {
                self.drive.reauthorize_establish(participation).await
            }
            _ => unreachable!("validated Establish authorization request"),
        };
        let (decision, failure) = match permitted {
            Ok(permit) => {
                let identity = permit.identity();
                if identity.activation() != port.activation
                    || identity.participation() != original.participation()
                    || identity.operation_id() != original.envelope().operation_id()
                    || identity.ticket_id() != original.admission_ticket_id()
                    || identity.semantics() != original.semantic_identity()
                    || identity.native_compatibility_id() != self.compatibility
                {
                    return Err(AttemptQaPortError::WrongGuard);
                }
                let capture = EstablishSubmissionCapture::default();
                match permit.try_submit(&capture) {
                    Ok(EstablishIssueSubmit::Accepted) => {
                        let submission = capture.take().expect("original QA transport submission");
                        assert_eq!(submission.identity(), identity);
                        assert!(
                            self.guards
                                .insert(reference, Guard::Current(submission))
                                .is_none()
                        );
                        (
                            Ok(AttemptAuthorizationDecision::EstablishAuthorized(reference)),
                            None,
                        )
                    }
                    Ok(EstablishIssueSubmit::Backpressured) => {
                        unreachable!("prepaid Establish capture")
                    }
                    Err(error) => self.authorization_failure(
                        port.action,
                        LogicalExecutionActorError::Establish(error),
                    ),
                }
            }
            Err(error) => self.authorization_failure(port.action, error),
        };
        Ok(AttemptQaReply {
            reply: LogicalToAttempt {
                activation: port.activation,
                body: LogicalToAttemptBody::AuthorizationReply { token, decision },
            },
            failure,
        })
    }

    pub(in crate::task_execution) fn settle(
        &mut self,
        port: &AttemptToLogical<AttemptFailure>,
    ) -> Result<AttemptQaReply, AttemptQaPortError> {
        if port.activation != self.drive.identity() {
            return Err(AttemptQaPortError::WrongRequest);
        }
        let body = match port.body {
            AttemptToLogicalBody::EstablishSettlement {
                token,
                authorization,
                participation,
                operation_id,
                settlement,
            } => {
                let guard = self
                    .guards
                    .get(&authorization)
                    .ok_or(AttemptQaPortError::WrongGuard)?;
                if !guard.matches(participation, operation_id)
                    || settlement == AttemptIssueSettlement::TransportOwned
                    || matches!(guard, Guard::Late(_))
                        && !matches!(
                            settlement,
                            AttemptIssueSettlement::PreWorkerRejected
                                | AttemptIssueSettlement::Worker(_)
                        )
                {
                    return Err(AttemptQaPortError::WrongGuard);
                }
                let guard = self.guards.remove(&authorization).unwrap();
                let settled = match (guard, settlement) {
                    (Guard::Current(original), AttemptIssueSettlement::DefinitelyUnsent) => {
                        original.definitely_unsent()
                    }
                    (Guard::Current(original), AttemptIssueSettlement::TransportUnknown) => {
                        original.transport_unknown().map(|late| {
                            assert!(
                                self.guards
                                    .insert(authorization, Guard::Late(late))
                                    .is_none()
                            );
                        })
                    }
                    (Guard::Current(original), AttemptIssueSettlement::PreWorkerRejected) => {
                        original.pre_worker_rejected()
                    }
                    (Guard::Late(original), AttemptIssueSettlement::PreWorkerRejected) => {
                        original.pre_worker_rejected()
                    }
                    (Guard::Current(original), AttemptIssueSettlement::Worker(outcome)) => {
                        original.worker_settled(outcome)
                    }
                    (Guard::Late(original), AttemptIssueSettlement::Worker(outcome)) => {
                        original.worker_settled(outcome)
                    }
                    _ => unreachable!("validated original Establish settlement"),
                };
                let (result, failure) = match settled {
                    Ok(()) => (Ok(()), None),
                    Err(error) => {
                        let (failure, original) = Self::project_failure(
                            port.action,
                            HostFailureKind::EstablishAuthorization,
                            TaskExecutionError::Establish(error),
                        );
                        (Err(failure), Some(original))
                    }
                };
                return Ok(AttemptQaReply {
                    reply: LogicalToAttempt {
                        activation: port.activation,
                        body: LogicalToAttemptBody::EstablishSettlementReply {
                            token,
                            authorization,
                            result,
                        },
                    },
                    failure,
                });
            }
            AttemptToLogicalBody::DetachEstablish {
                token,
                authorization,
                participation,
                operation_id,
            } => {
                if !matches!(self.guards.get(&authorization), Some(guard @ Guard::Late(_)) if guard.matches(participation, operation_id))
                {
                    return Err(AttemptQaPortError::WrongGuard);
                }
                drop(self.guards.remove(&authorization));
                LogicalToAttemptBody::EstablishDetachReply {
                    token,
                    authorization,
                    result: Ok(()),
                }
            }
            _ => return Err(AttemptQaPortError::WrongRequest),
        };
        Ok(AttemptQaReply {
            reply: LogicalToAttempt {
                activation: port.activation,
                body,
            },
            failure: None,
        })
    }

    fn authorization_failure(
        &self,
        action: AttemptActionId,
        error: LogicalExecutionActorError,
    ) -> (
        Result<AttemptAuthorizationDecision, AttemptAuthorizationFailure<AttemptFailure>>,
        Option<(HostFailureRef, TaskExecutionError)>,
    ) {
        let (failure, original) = Self::project_failure(
            action,
            HostFailureKind::EstablishAuthorization,
            TaskExecutionError::Actor(error),
        );
        let disposition = match error {
            LogicalExecutionActorError::Establish(
                EstablishIssueError::AuthorizationBudgetExhausted,
            ) => AttemptAuthorizationFailure::EstablishBudgetExhausted(failure),
            _ => AttemptAuthorizationFailure::Rejected(failure),
        };
        (Err(disposition), Some(original))
    }

    fn project_failure(
        action: AttemptActionId,
        kind: HostFailureKind,
        error: TaskExecutionError,
    ) -> (AttemptFailure, (HostFailureRef, TaskExecutionError)) {
        let original = HostFailureRef(NonZeroU64::new(action.get()).unwrap());
        let class = error.preparation_input().attempt_class();
        let failure = AttemptFailure::Host(HostProjectedFailure {
            original,
            kind,
            class,
            site: AttemptFailureSite::Attempt,
        });
        (failure, (original, error))
    }
}

fn replacement_refusal(
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

#[derive(Debug, Default)]
pub(in crate::task_execution) struct EstablishSubmissionCapture {
    submission: Arc<Mutex<Option<EstablishTransportSubmission>>>,
}

impl EstablishSubmissionCapture {
    pub(in crate::task_execution) fn take(&self) -> Option<EstablishTransportSubmission> {
        self.submission
            .lock()
            .expect("Establish submission capture")
            .take()
    }
}

impl EstablishTransportSink for EstablishSubmissionCapture {
    fn try_reserve(
        &self,
        _identity: novarocks_query_application::coordination::EstablishIssueIdentity,
    ) -> EstablishTransportAdmission {
        EstablishTransportAdmission::Admitted(Box::new(EstablishSubmissionCaptureReservation {
            submission: Arc::clone(&self.submission),
        }))
    }
}

#[derive(Debug)]
struct EstablishSubmissionCaptureReservation {
    submission: Arc<Mutex<Option<EstablishTransportSubmission>>>,
}

impl EstablishTransportReservation for EstablishSubmissionCaptureReservation {
    fn submit(self: Box<Self>, submission: EstablishTransportSubmission) {
        let previous = self
            .submission
            .lock()
            .expect("Establish submission capture")
            .replace(submission);
        assert!(
            previous.is_none(),
            "one Establish authorization transfers exactly one submission guard"
        );
    }
}
