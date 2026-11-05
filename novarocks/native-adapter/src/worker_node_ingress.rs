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

//! Native mutation projection over the one actual Worker Node. Every reply
//! comes from its original admitted R; no later snapshot reconstructs an ACK.

use crate::{
    static_package_admission::StaticPackageAdmission,
    task_protocol::{TaskOperationBatchApplier, host_rejection_status},
};
use novarocks_execution_contract::{
    AdmissionTicketId, OperationOutcome, ParticipationRef, ReleaseOutcome, TaskOperationId,
    UpdateParticipation, operation::OperationResult,
};
use novarocks_proto_models::novarocks as proto;
use novarocks_task_codec::{
    operation::{
        DecodedOperation, DecodedUpdateParticipation, encode_abort_cause_field,
        encode_create_task_ack, encode_participation_ack,
        encode_participation_admission_ticket_ack, encode_quiesce_ack, encode_receipt,
        encode_release_ack,
    },
    status::encode_task_status,
};
use novarocks_types::NativeCompatibilityId;
use novarocks_worker::{
    NodeAdmissionTicketObservation, ReleasedParticipationEvidence, WorkerCancelOutcome,
    WorkerControlDelivery, WorkerControlReplyBody, WorkerCreateDelivery, WorkerCreateReply,
    WorkerIngressResponsibility, WorkerMonotonicInstant, WorkerParticipationDelivery,
    WorkerParticipationReplyBody, WorkerRequestError, WorkerRequestRefusal, WorkerRequestResult,
    WorkerTaskControl, WorkerTaskDelivery,
};
use std::{
    fmt,
    sync::{Arc, Mutex},
    time::Instant,
};
use tonic::Status;

type Ack = proto::task_operation_verdict::Ack;
type Receipt = proto::TaskOperationReceipt;

/// The same actual Node is shared with Root reads and observation adapters.
/// This object owns no alternate runtime, request index or recovery policy.
pub struct WorkerNodeTaskIngress {
    control: WorkerTaskControl,
    compatibility: NativeCompatibilityId,
    static_package_admission: Arc<StaticPackageAdmission>,
    #[cfg(novarocks_dev_controls)]
    test_controls: std::sync::OnceLock<Arc<crate::test_controls::NativeTestControls>>,
}
impl WorkerNodeTaskIngress {
    pub fn new(
        control: WorkerTaskControl,
        compatibility: NativeCompatibilityId,
        static_package_admission: Arc<StaticPackageAdmission>,
    ) -> Arc<Self> {
        Arc::new(Self {
            control,
            compatibility,
            static_package_admission,
            #[cfg(novarocks_dev_controls)]
            test_controls: std::sync::OnceLock::new(),
        })
    }
    #[cfg(novarocks_dev_controls)]
    pub fn install_test_controls(
        &self,
        controls: Arc<crate::test_controls::NativeTestControls>,
    ) -> Result<(), String> {
        self.test_controls
            .set(controls)
            .map_err(|_| "Native Node ingress test controls were already installed".into())
    }
    fn deadline(&self, original: Instant) -> WorkerMonotonicInstant {
        // Preserve the one original ingress deadline when moving between the
        // Native and Worker clocks. Never grant a fresh per-poll wait.
        self.control
            .now()
            .saturating_add(original.saturating_duration_since(Instant::now()))
    }
    fn compatibility_refusal(&self, request: &DecodedOperation) -> Option<Receipt> {
        let (operation, compatibility) = match request {
            DecodedOperation::AcquireParticipationAdmissionTicket(request) => (
                request.envelope().operation_id(),
                request.native_compatibility_id(),
            ),
            DecodedOperation::UpdateParticipation(DecodedUpdateParticipation::Establish(
                request,
            )) => (
                request.envelope().operation_id(),
                request.native_compatibility_id(),
            ),
            _ => return None,
        };
        (compatibility != self.compatibility)
            .then(|| verdict(operation, OperationOutcome::CompatibilityMismatch, None))
    }
}

#[tonic::async_trait]
impl TaskOperationBatchApplier for WorkerNodeTaskIngress {
    fn validate_ingress_ownership(
        &self,
        ownership: Option<&Arc<crate::native_ingress::NativeIngressOwnership>>,
    ) -> Result<(), Status> {
        ownership.map(|_| ()).ok_or_else(|| {
            Status::failed_precondition(
                "the actual Worker operation requires original Native ingress ownership",
            )
        })
    }

    async fn apply_task_operation(
        &self,
        operation: DecodedOperation,
        deadline: Instant,
        ownership: Option<Arc<crate::native_ingress::NativeIngressOwnership>>,
    ) -> Result<Receipt, Status> {
        let ownership = ownership.ok_or_else(|| {
            Status::failed_precondition(
                "the actual Worker operation requires original Native ingress ownership",
            )
        })?;
        if let Some(receipt) = self.compatibility_refusal(&operation) {
            return Ok(receipt);
        }
        let identity = operation.envelope().operation_id();
        let deadline = self.deadline(deadline);
        match operation {
            DecodedOperation::AcquireParticipationAdmissionTicket(request) => {
                // A new independent Native nonce, never a hash/reinterpretation
                // of the caller's operation ID. The Worker chooses replay.
                let ticket =
                    AdmissionTicketId::try_from_bytes(TaskOperationId::new_v7().to_bytes())
                        .map_err(|_| {
                            Status::internal("Native ticket nonce issuer produced zero")
                        })?;
                match self.control.acquire(request, ticket, deadline).await {
                    Ok(original) => encode_control(identity, original),
                    Err(error) => request_error(identity, error),
                }
            }
            DecodedOperation::CreateTask(decoded) => {
                #[cfg(novarocks_dev_controls)]
                if let Some(controls) = self.test_controls.get()
                    && crate::task_protocol_fault::restart_after_establish_participation_holds_task_creation(
                        controls, decoded.request().participation(),
                    )
                {
                    // The old hook blocks on runner-owned file/UDS waits.
                    // Reject the unported async rendezvous before Task R
                    // admission; never block the Native executor or pretend
                    // the selected process-loss boundary was exercised.
                    return Err(Status::unimplemented("the asynchronous restart-after-establish creation hold is not connected"));
                }
                let (request, input, retained) = decoded.into_parts_with_retained_bytes();
                #[cfg(novarocks_dev_controls)]
                let task_identity = request.identity();
                match self
                    .control
                    .create(
                        Arc::new(request),
                        input,
                        retained,
                        deadline,
                        WorkerIngressResponsibility::from_admitted(ownership),
                    )
                    .await
                {
                    Ok(original) => {
                        #[cfg(novarocks_dev_controls)]
                        let original_outcome = if let Some(controls) = self.test_controls.get() {
                            check_operation(identity, original.operation())?;
                            if let WorkerCreateReply::Participation(
                                WorkerParticipationReplyBody::Create { outcome, receipt },
                            ) = original.reply()
                            {
                                if receipt.identity() != task_identity {
                                    return Err(Status::internal(
                                        "original Create acknowledgement names another Task",
                                    ));
                                }
                                // Claim only after this original Task R carries
                                // the committed Worker ownership decision. The
                                // hook drops transport ACK, never admission or
                                // the later Installed/physical-exit facts.
                                crate::task_protocol_fault::create_task_ack_dropped(
                                    controls,
                                    task_identity,
                                    *outcome,
                                )?;
                                Some(*outcome)
                            } else {
                                None
                            }
                        } else {
                            None
                        };
                        let encoded = encode_create(identity, original)?;
                        #[cfg(novarocks_dev_controls)]
                        if let (Some(controls), Some(outcome)) =
                            (self.test_controls.get(), original_outcome)
                        {
                            let mut encoded = encoded;
                            // Forge only the wire answer after original ownership
                            // and exact operation/Task identity were verified.
                            // Identity goes first: rejection removes its ACK.
                            crate::task_protocol_fault::create_task_receipt_foreign_task(
                                controls,
                                task_identity,
                                outcome,
                                &mut encoded,
                            )?;
                            crate::task_protocol_fault::create_task_rejected_after_apply(
                                controls,
                                task_identity,
                                outcome,
                                &mut encoded,
                            )?;
                            return Ok(encoded);
                        }
                        Ok(encoded)
                    }
                    Err(error) => request_error(identity, error),
                }
            }
            DecodedOperation::UpdateParticipation(decoded) => {
                let Some(request) = decoded.as_neutral() else {
                    return Err(Status::internal(
                        "decoded Participation command has no neutral projection",
                    ));
                };
                let result = match request {
                    UpdateParticipation::Establish(request) => {
                        #[cfg(novarocks_dev_controls)]
                        if let Some(controls) = self.test_controls.get() {
                            crate::task_protocol_fault::arm_restart_after_establish_participation(
                                controls,
                                request.participation(),
                            )?;
                            if crate::task_protocol_fault::restart_after_establish_participation_holds_task_creation(controls, request.participation()) {
                                return Err(Status::unimplemented("the asynchronous restart-after-establish rendezvous is not connected"));
                            }
                        }
                        return match self.control.establish(request, deadline).await {
                            Ok(original) => encode_control(identity, original),
                            Err(error) => request_error(identity, error),
                        };
                    }
                    UpdateParticipation::RenewLease(request) => {
                        #[cfg(novarocks_dev_controls)]
                        if let Some(controls) = self.test_controls.get()
                            && let Some(mut claim) = controls
                                .claim_participation_owner_panic(
                                    request.participation(),
                                    request.envelope().operation_id(),
                                )
                                .map_err(Status::failed_precondition)?
                        {
                            // Authorization is consumed once before this SAME R
                            // is admitted. No fault owner or callback enters Core.
                            let deadline =
                                deadline.min(self.deadline(claim.cancellation().deadline()));
                            let result = self
                                .control
                                .renew_with_participation_owner_panic(request, deadline)
                                .await;
                            if let Ok(original) = &result
                                && original.body().matches(WorkerRequestResult::Outcome(
                                    OperationOutcome::OwnerFailed,
                                ))
                            {
                                claim.record_hit().map_err(Status::failed_precondition)?;
                            }
                            return match result {
                                Ok(original) => encode_participation(
                                    identity,
                                    original,
                                    ReplyKind::Participation,
                                ),
                                Err(error) => request_error(identity, error),
                            };
                        }
                        self.control.renew(request, deadline).await
                    }
                    UpdateParticipation::AdvanceDomain(request) => {
                        self.control
                            .advance_participation_domain(
                                request,
                                deadline,
                                WorkerIngressResponsibility::from_admitted(ownership),
                            )
                            .await
                    }
                };
                match result {
                    Ok(original) => {
                        encode_participation(identity, original, ReplyKind::Participation)
                    }
                    Err(error) => request_error(identity, error),
                }
            }
            DecodedOperation::CancelTask(request) => {
                match self.control.cancel(request, deadline).await {
                    Ok(original) => encode_cancel(identity, original),
                    Err(error) => request_error(identity, error),
                }
            }
            DecodedOperation::AbortParticipation(request) => {
                match self.control.abort(request, deadline).await {
                    Ok(original) => {
                        encode_participation(identity, original, ReplyKind::Participation)
                    }
                    Err(error) => request_error(identity, error),
                }
            }
            DecodedOperation::QuiesceParticipation(request) => {
                match self.control.quiesce(request, deadline).await {
                    Ok(original) => encode_participation(identity, original, ReplyKind::Quiesce),
                    Err(error) => request_error(identity, error),
                }
            }
            DecodedOperation::ReleaseParticipation(request) => {
                match self.control.release(request, deadline).await {
                    Ok(original) => encode_participation(identity, original, ReplyKind::Release),
                    Err(error) => request_error(identity, error),
                }
            }
            DecodedOperation::UpdateTask(decoded) => {
                let request = decoded.into_request();
                let task = request.identity();
                let participation = self.control.participation_for_task(task).ok_or_else(|| {
                    Status::failed_precondition("the Node does not hold this Task's Participation")
                })?;
                match self
                    .control
                    .update_task(
                        participation,
                        request,
                        deadline,
                        WorkerIngressResponsibility::from_admitted(ownership),
                    )
                    .await
                {
                    Ok(original) => encode_task_domain(identity, task, original),
                    Err(error) => request_error(identity, error),
                }
            }
        }
    }
    async fn observe_admission_ticket(
        &self,
        operation: TaskOperationId,
        ticket: AdmissionTicketId,
        participation: ParticipationRef,
        deadline: Instant,
    ) -> Result<NodeAdmissionTicketObservation, Status> {
        let original = self
            .control
            .observe_admission_ticket(operation, ticket, participation, self.deadline(deadline))
            .await
            .map_err(request_status)?;
        check_operation(operation, original.operation())?;
        match original.body() {
            WorkerControlReplyBody::TicketObservation(observation) => Ok(*observation),
            WorkerControlReplyBody::Verdict(WorkerRequestResult::TimeoutWhileAccepted) => Err(
                Status::deadline_exceeded("original admitted ticket observation exceeded its wait"),
            ),
            _ => Err(Status::internal(
                "original ticket observation returned no observation fact",
            )),
        }
    }
    async fn preflight_expired_establish_ticket(
        &self,
        operation: TaskOperationId,
        ticket: AdmissionTicketId,
        participation: ParticipationRef,
        compatibility: Option<&proto::NativeCompatibilityId>,
        deadline: Instant,
    ) -> Result<Option<Receipt>, Status> {
        let Some(compatibility) = compatibility else {
            return Ok(None);
        };
        let Ok(compatibility) = NativeCompatibilityId::try_from_slice(&compatibility.value) else {
            return Ok(None);
        };
        if compatibility != self.compatibility {
            return Ok(None);
        }
        let original = match self
            .control
            .preflight_expired_establish_ticket(
                operation,
                ticket,
                participation,
                self.deadline(deadline),
            )
            .await
        {
            Ok(original) => original,
            Err(error) => return request_error(operation, error).map(Some),
        };
        check_operation(operation, original.operation())?;
        match original.body() {
            WorkerControlReplyBody::PreflightExpired(outcome) => {
                Ok(outcome.map(|outcome| verdict(operation, outcome, None)))
            }
            WorkerControlReplyBody::Verdict(result) => encode_result(operation, *result).map(Some),
            _ => Err(Status::internal(
                "original expired-ticket preflight returned another reply shape",
            )),
        }
    }
    fn static_package_admission(&self) -> Option<&StaticPackageAdmission> {
        Some(&self.static_package_admission)
    }
}

#[derive(Clone, Copy)]
enum ReplyKind {
    Create,
    Participation,
    Quiesce,
    Release,
}
fn check_operation(expected: TaskOperationId, actual: TaskOperationId) -> Result<(), Status> {
    if expected == actual {
        Ok(())
    } else {
        Err(Status::internal(
            "original Worker reply names another operation",
        ))
    }
}
fn verdict(
    operation: TaskOperationId,
    outcome: OperationOutcome,
    acknowledgement: Option<Ack>,
) -> Receipt {
    encode_receipt(
        operation,
        OperationResult::Verdict {
            outcome,
            detail: None,
            acknowledgement,
        },
    )
}
fn encode_result(
    operation: TaskOperationId,
    result: WorkerRequestResult,
) -> Result<Receipt, Status> {
    match result {
        WorkerRequestResult::Outcome(outcome) => Ok(verdict(operation, outcome, None)),
        // The admitted action can still complete. The wire's OperationTimedOut
        // promises no creation/partial state, so it cannot represent this fact.
        WorkerRequestResult::TimeoutWhileAccepted => Err(Status::deadline_exceeded(
            "original admitted Worker request exceeded its reply wait",
        )),
    }
}
fn encode_create(
    operation: TaskOperationId,
    original: WorkerCreateDelivery,
) -> Result<Receipt, Status> {
    check_operation(operation, original.operation())?;
    match original.reply() {
        WorkerCreateReply::Participation(body) => {
            project_participation(operation, body, ReplyKind::Create, None)
        }
        WorkerCreateReply::Root(WorkerControlReplyBody::CreateParticipation(body)) => {
            project_participation(operation, body, ReplyKind::Create, None)
        }
        WorkerCreateReply::Root(WorkerControlReplyBody::Verdict(result)) => {
            encode_result(operation, *result)
        }
        _ => Err(Status::internal(
            "original Create reply has another body shape",
        )),
    }
}
fn encode_control(
    operation: TaskOperationId,
    original: WorkerControlDelivery,
) -> Result<Receipt, Status> {
    check_operation(operation, original.operation())?;
    match original.body() {
        WorkerControlReplyBody::Admission(Ok(receipt)) => Ok(verdict(
            operation,
            OperationOutcome::Accepted,
            Some(Ack::ParticipationAdmissionTicket(
                encode_participation_admission_ticket_ack(*receipt),
            )),
        )),
        WorkerControlReplyBody::Admission(Err(outcome)) => Ok(verdict(operation, *outcome, None)),
        WorkerControlReplyBody::Participation(ack) => {
            tracing::debug!(participation = ?ack.receipt.participation(), state = ?ack.receipt.state(), outcome = ?ack.outcome, "Worker returned original Participation acknowledgement");
            let body =
                encode_participation_ack(&ack.receipt, ack.termination_cause).ok_or_else(|| {
                    Status::internal("original Participation acknowledgement has no wire state")
                })?;
            Ok(verdict(
                operation,
                ack.outcome,
                Some(Ack::Participation(body)),
            ))
        }
        WorkerControlReplyBody::Domain(refusal) => Ok(encode_receipt(
            operation,
            OperationResult::DomainFailure {
                outcome: refusal.outcome,
                failure: refusal.failure.clone(),
            },
        )),
        WorkerControlReplyBody::Verdict(result) => encode_result(operation, *result),
        _ => Err(Status::internal(
            "original Worker control reply has an unexpected body",
        )),
    }
}
fn encode_participation(
    operation: TaskOperationId,
    original: WorkerParticipationDelivery,
    kind: ReplyKind,
) -> Result<Receipt, Status> {
    check_operation(operation, original.operation())?;
    project_participation(
        operation,
        original.body(),
        kind,
        original.released_evidence(),
    )
}
fn encode_cancel(
    operation: TaskOperationId,
    original: WorkerCancelOutcome,
) -> Result<Receipt, Status> {
    match original {
        WorkerCancelOutcome::Task(original) => encode_task(operation, original),
        WorkerCancelOutcome::Participation(original) => {
            check_operation(operation, original.operation())?;
            project_cancel(operation, original.body())
        }
        WorkerCancelOutcome::Refused {
            operation: actual,
            reason,
        } => {
            check_operation(operation, actual)?;
            Ok(encode_receipt(
                operation,
                OperationResult::Verdict {
                    outcome: reason.outcome(),
                    detail: Some(novarocks_execution_contract::SafeDetail::truncating(
                        reason.cancel_safe_detail(),
                    )),
                    acknowledgement: None,
                },
            ))
        }
    }
}
fn encode_task(
    operation: TaskOperationId,
    original: WorkerTaskDelivery,
) -> Result<Receipt, Status> {
    check_operation(operation, original.operation())?;
    project_cancel(operation, original.body())
}
fn project_cancel(
    operation: TaskOperationId,
    body: &WorkerParticipationReplyBody,
) -> Result<Receipt, Status> {
    match body {
        WorkerParticipationReplyBody::Task { outcome, status } => Ok(verdict(
            operation,
            *outcome,
            Some(Ack::CancelTask(encode_task_status(status))),
        )),
        WorkerParticipationReplyBody::Verdict(result) => encode_result(operation, *result),
        WorkerParticipationReplyBody::Domain(refusal) => Ok(encode_receipt(
            operation,
            OperationResult::DomainFailure {
                outcome: refusal.outcome,
                failure: refusal.failure.clone(),
            },
        )),
        _ => Err(Status::internal(
            "original Cancel reply has another body shape",
        )),
    }
}
fn project_participation(
    operation: TaskOperationId,
    original: &WorkerParticipationReplyBody,
    kind: ReplyKind,
    evidence: Option<&ReleasedParticipationEvidence>,
) -> Result<Receipt, Status> {
    let (outcome, ack) = match (kind, original) {
        (_, WorkerParticipationReplyBody::Verdict(result)) => {
            return encode_result(operation, *result);
        }
        (_, WorkerParticipationReplyBody::Domain(refusal)) => {
            return Ok(encode_receipt(
                operation,
                OperationResult::DomainFailure {
                    outcome: refusal.outcome,
                    failure: refusal.failure.clone(),
                },
            ));
        }
        (ReplyKind::Create, WorkerParticipationReplyBody::Create { outcome, receipt }) => (
            *outcome,
            Ack::CreateTask(encode_create_task_ack(receipt).ok_or_else(|| {
                Status::internal("original Create acknowledgement has no wire state")
            })?),
        ),
        (ReplyKind::Participation, WorkerParticipationReplyBody::Participation(ack))
        | (
            ReplyKind::Participation,
            WorkerParticipationReplyBody::ParticipationDomain {
                acknowledgement: ack,
                ..
            },
        ) => (
            ack.outcome,
            Ack::Participation(
                encode_participation_ack(&ack.receipt, ack.termination_cause).ok_or_else(|| {
                    Status::internal("original Participation acknowledgement has no wire state")
                })?,
            ),
        ),
        (ReplyKind::Quiesce, WorkerParticipationReplyBody::Quiesce { outcome, receipt }) => (
            *outcome,
            Ack::QuiesceParticipation(encode_quiesce_ack(receipt).ok_or_else(|| {
                Status::internal("original Quiesce acknowledgement has no wire state")
            })?),
        ),
        (
            ReplyKind::Release,
            WorkerParticipationReplyBody::Release {
                participation,
                release,
                state,
                termination_cause,
            },
        ) => {
            let runtime_filter = match release {
                ReleaseOutcome::NotReady => None,
                ReleaseOutcome::Released | ReleaseOutcome::AlreadyTerminal => {
                    let Some(evidence) = evidence else {
                        return Ok(verdict(operation, OperationOutcome::OwnerFailed, None));
                    };
                    crate::task_shared_facts::release_runtime_filter_telemetry(evidence)
                        .map_err(host_rejection_status)?
                }
            };
            let mut ack =
                encode_release_ack(*participation, *release, *state, runtime_filter.as_ref())
                    .ok_or_else(|| {
                        Status::internal("original Release acknowledgement has no wire state")
                    })?;
            ack.termination_cause = termination_cause.map(encode_abort_cause_field);
            let outcome = match release {
                ReleaseOutcome::NotReady => OperationOutcome::ReleaseNotReady,
                ReleaseOutcome::Released => OperationOutcome::Accepted,
                ReleaseOutcome::AlreadyTerminal => OperationOutcome::ParticipationTerminalReceipt,
            };
            (outcome, Ack::ReleaseParticipation(ack))
        }
        _ => {
            return Err(Status::internal(
                "original Worker Participation reply has another body shape",
            ));
        }
    };
    Ok(verdict(operation, outcome, Some(ack)))
}

/// Tonic's error source retains the same refused original, instead of
/// formatting away its ownership. Mutex adds no scheduling or new obligation.
struct RefusedHandoff(Mutex<WorkerRequestRefusal>);
impl fmt::Display for RefusedHandoff {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "original Worker handoff refused at {}",
            self.0.lock().expect("original Worker refusal").stage()
        )
    }
}
impl fmt::Debug for RefusedHandoff {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
impl std::error::Error for RefusedHandoff {}
pub(crate) fn request_status(error: WorkerRequestError) -> Status {
    match error {
        WorkerRequestError::OwnerFailed => {
            Status::internal("the original Worker request owner failed")
        }
        WorkerRequestError::HandoffRefused(original) => {
            tracing::warn!(stage = original.stage(), "Worker request handoff refused");
            let mut status = Status::internal("the original Worker handoff was refused");
            status.set_source(Arc::new(RefusedHandoff(Mutex::new(original))));
            status
        }
    }
}
fn request_error(operation: TaskOperationId, error: WorkerRequestError) -> Result<Receipt, Status> {
    match error {
        WorkerRequestError::OwnerFailed => {
            Ok(verdict(operation, OperationOutcome::OwnerFailed, None))
        }
        original @ WorkerRequestError::HandoffRefused(_) => Err(request_status(original)),
    }
}

#[tonic::async_trait]
impl crate::task_protocol::TaskExecutionIngress for WorkerNodeTaskIngress {
    #[cfg(novarocks_dev_controls)]
    fn with_registry_lock_for_test(
        &self,
        _callback: &mut dyn FnMut() -> Result<(), Status>,
    ) -> Result<(), Status> {
        Err(Status::unimplemented(
            "the Node owner has no synchronous registry lock rendezvous",
        ))
    }
    async fn apply_task_operations(
        &self,
        _request: proto::ApplyTaskOperationsRequest,
    ) -> Result<proto::ApplyTaskOperationsResponse, Status> {
        Err(Status::failed_precondition(
            "original Native ingress timing is required for Task operations",
        ))
    }
    async fn apply_task_operations_at(
        &self,
        request: proto::ApplyTaskOperationsRequest,
        timing: crate::task_protocol::TaskIngressTiming,
        ownership: Arc<crate::native_ingress::NativeIngressOwnership>,
    ) -> Result<proto::ApplyTaskOperationsResponse, Status> {
        crate::task_protocol::apply_task_operations_with_ownership_at(
            self,
            request,
            timing,
            Some(ownership),
        )
        .await
    }
    async fn apply_task_control_operations(
        &self,
        _request: proto::ApplyTaskControlOperationsRequest,
    ) -> Result<proto::ApplyTaskOperationsResponse, Status> {
        Err(Status::failed_precondition(
            "original Native ingress timing is required for Task control operations",
        ))
    }
    async fn apply_task_control_operations_at(
        &self,
        request: proto::ApplyTaskControlOperationsRequest,
        timing: crate::task_protocol::TaskIngressTiming,
        ownership: Arc<crate::native_ingress::NativeIngressOwnership>,
    ) -> Result<proto::ApplyTaskOperationsResponse, Status> {
        crate::task_protocol::apply_task_control_operations_with_ownership_at(
            self,
            request,
            timing,
            Some(ownership),
        )
        .await
    }
    fn subscribe_task_status(
        &self,
        request: proto::SubscribeTaskStatusRequest,
        deadline: Instant,
        ownership: Arc<crate::native_ingress::NativeIngressOwnership>,
    ) -> Result<crate::task_protocol::TaskStatusEventStream, Status> {
        crate::worker_node_observation::subscribe_task_status(
            &self.control,
            request,
            deadline,
            Some(ownership),
            #[cfg(novarocks_dev_controls)]
            self.test_controls.get().cloned(),
        )
    }
    async fn fetch_task_dynamic_filters(
        &self,
        request: proto::FetchTaskDynamicFiltersRequest,
        deadline: Instant,
    ) -> Result<proto::FetchTaskDynamicFiltersResponse, Status> {
        use novarocks_proto_codec::FieldPath;
        let request = novarocks_task_codec::operation::decode_fetch_dynamic_filters(
            &request,
            TaskOperationId::new_v7(),
            FieldPath::root("fetch_task_dynamic_filters"),
        )
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let participation = self
            .control
            .participation_for_task(request.identity())
            .ok_or_else(|| {
                Status::failed_precondition("the Node does not hold this Task's Participation")
            })?;
        let original = self
            .control
            .get_dynamic_filters(participation, request, self.deadline(deadline))
            .await
            .map_err(request_status)?;
        check_operation(request.envelope().operation_id(), original.operation())?;
        match original.body() {
            // This is the actual Core's no-payload decision, not a default for
            // an unprojectable or missing versioned payload.
            WorkerParticipationReplyBody::Verdict(WorkerRequestResult::Outcome(
                OperationOutcome::Idempotent,
            )) => crate::task_protocol::encode_dynamic_filter_read(request.identity(), None)
                .map_err(host_rejection_status),
            WorkerParticipationReplyBody::ReadPayload { version, .. } => {
                let (selected, payload) = original.dynamic_filter_payload().ok_or_else(|| {
                    Status::internal("the original selected dynamic-filter loan is missing")
                })?;
                if selected != *version {
                    return Err(Status::internal(
                        "the original dynamic-filter loan version differs from its Core reply",
                    ));
                }
                // Codec getters run on this original RPC thread, outside the
                // Participation/Task/store locks. `original` retains the same R pin.
                let domain = novarocks_task_codec::domain::encode_task_dynamic_filter_domain(
                    selected, payload,
                )
                .map_err(|error| Status::internal(error.to_string()))?;
                Ok(proto::FetchTaskDynamicFiltersResponse {
                    identity: Some(novarocks_task_codec::identity::encode_task_identity(
                        request.identity(),
                    )),
                    version: selected.get(),
                    domains: vec![domain],
                })
            }
            WorkerParticipationReplyBody::Verdict(WorkerRequestResult::Outcome(_))
            | WorkerParticipationReplyBody::Domain(_) => Err(Status::failed_precondition(
                "the original Worker refused the dynamic-filter read",
            )),
            WorkerParticipationReplyBody::Verdict(WorkerRequestResult::TimeoutWhileAccepted) => {
                Err(Status::deadline_exceeded(
                    "the original dynamic-filter request timed out while accepted",
                ))
            }
            _ => Err(Status::internal(
                "the original dynamic-filter reply has an unexpected body",
            )),
        }
    }
    async fn get_final_task_info(
        &self,
        request: proto::GetFinalTaskInfoRequest,
        deadline: Instant,
    ) -> Result<proto::GetFinalTaskInfoResponse, Status> {
        use novarocks_proto_codec::FieldPath;
        let request = novarocks_task_codec::operation::decode_get_final_task_info(
            &request,
            TaskOperationId::new_v7(),
            FieldPath::root("get_final_task_info"),
        )
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let participation = self
            .control
            .participation_for_task(request.identity())
            .ok_or_else(|| {
                Status::failed_precondition("the Node does not hold this Task's Participation")
            })?;
        let original = self
            .control
            .get_final_task_info(participation, request, self.deadline(deadline))
            .await
            .map_err(request_status)?;
        check_operation(request.envelope().operation_id(), original.operation())?;
        let result = match original.body() {
            WorkerParticipationReplyBody::FinalInfo(info) => {
                proto::get_final_task_info_response::Result::Info(
                    novarocks_task_codec::status::encode_final_task_info(info),
                )
            }
            WorkerParticipationReplyBody::Verdict(WorkerRequestResult::Outcome(outcome)) => {
                proto::get_final_task_info_response::Result::Unavailable(
                    novarocks_task_codec::operation::encode_operation_outcome(*outcome),
                )
            }
            WorkerParticipationReplyBody::Domain(refusal) => {
                proto::get_final_task_info_response::Result::Unavailable(
                    novarocks_task_codec::operation::encode_operation_outcome(
                        refusal.outcome.outcome(),
                    ),
                )
            }
            WorkerParticipationReplyBody::Verdict(WorkerRequestResult::TimeoutWhileAccepted) => {
                return Err(Status::deadline_exceeded(
                    "the original final-info request timed out while accepted",
                ));
            }
            _ => {
                return Err(Status::internal(
                    "the original final-info reply has an unexpected body",
                ));
            }
        };
        Ok(proto::GetFinalTaskInfoResponse {
            result: Some(result),
        })
    }
}

fn encode_task_domain(
    operation: TaskOperationId,
    task: novarocks_execution_contract::TaskIdentity,
    original: WorkerTaskDelivery,
) -> Result<Receipt, Status> {
    check_operation(operation, original.operation())?;
    match original.body() {
        WorkerParticipationReplyBody::TaskDomain { outcome, receipts } => {
            let accepted_domains = receipts
                .iter()
                .map(novarocks_task_codec::operation::encode_task_domain_receipt)
                .collect::<Option<Vec<_>>>()
                .ok_or_else(|| {
                    Status::internal("the original Task domain receipt has no wire representation")
                })?;
            Ok(verdict(
                operation,
                *outcome,
                Some(Ack::UpdateTask(proto::UpdateTaskAck {
                    identity: Some(novarocks_task_codec::identity::encode_task_identity(task)),
                    accepted_domains,
                })),
            ))
        }
        WorkerParticipationReplyBody::Verdict(result) => encode_result(operation, *result),
        WorkerParticipationReplyBody::Domain(refusal) => Ok(encode_receipt(
            operation,
            OperationResult::DomainFailure {
                outcome: refusal.outcome,
                failure: refusal.failure.clone(),
            },
        )),
        _ => Err(Status::internal(
            "the original Task domain reply has an unexpected body",
        )),
    }
}
