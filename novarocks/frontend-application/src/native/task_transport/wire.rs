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

//! Pure frontend Native wire projections. No scheduling, metrics or logging.

use super::*;

/// The five typed messages one establish carries.
///
/// Boxed inside [`OperationWireContent`]: an establish is by far the largest
/// request in the protocol, and every other variant would otherwise pay for
/// its size.
pub struct EstablishWireContent {
    pub catalog_set: CatalogSet,
    pub initial_runtime_filter: proto::RuntimeFilterContribution,
    pub initial_credential: proto::ParticipationCredentialDomain,
    pub query_options: proto::QueryOptions,
    pub native_compatibility_id: Option<proto::NativeCompatibilityId>,
}

/// Encodes one intent, projecting its own payloads back to the wire.
///
/// Nothing is handed in from outside: an intent carries its payloads behind a
/// fingerprint, and the codec that produced them is the only thing that can
/// recover them. Query options come from the immutable establish intent. The
/// compatibility identity still comes from the sink because it belongs to the
/// attempt-wide wire island rather than to the participation owner.
pub fn encode_operation(
    intent: &OperationIntent,
    attempt: &AttemptWireFacts,
) -> Result<proto::TaskOperation, String> {
    match intent {
        OperationIntent::AcquireParticipationAdmissionTicket(request) => {
            Ok(encode_acquire_participation_admission_ticket(*request))
        }
        OperationIntent::EstablishParticipation(request) => {
            encode_establish_participation_operation(request, attempt)
        }
        // The two carriers were frozen once; a send only wraps them in this
        // operation's envelope.
        OperationIntent::CreateTask(request) => Ok(encode_create_task(
            request.envelope(),
            request.parts().fragment().content(),
            request.parts().metadata(),
        )),
        OperationIntent::UpdateTask(request) => {
            let domains = encode_task_domains(request.domains())?;
            Ok(encode_update_task(request, domains))
        }
        OperationIntent::UpdateParticipation(request) => {
            encode_participation_operation(request, attempt)
        }
        OperationIntent::CancelTask(request) => Ok(encode_cancel_task(*request)),
        OperationIntent::QuiesceParticipation(request) => {
            Ok(codec::encode_quiesce_participation(*request))
        }
        OperationIntent::AbortParticipation(request) => Ok(encode_abort_participation(*request)),
        OperationIntent::ReleaseParticipation(request) => {
            Ok(encode_release_participation(*request))
        }
        // This read is its own RPC with its own response shape, so it has no
        // batch receipt to be answered by and can never be encoded as a batch item.
        #[cfg(test)]
        OperationIntent::FetchTaskDynamicFilters(_) => Err(format!(
            "{} is a separate RPC and has no operation-batch encoding",
            intent.kind()
        )),
    }
}

/// The attempt-level compatibility fact an establish carries outside its
/// neutral intent.
#[derive(Clone, Debug)]
pub struct AttemptWireFacts {
    pub native_compatibility_id: novarocks_types::NativeCompatibilityId,
}

pub fn encode_task_domains(
    domains: &[novarocks_execution::task_execution::operation::TaskDomainUpdate],
) -> Result<Vec<proto::TaskDomainUpdate>, String> {
    domains
        .iter()
        .map(|domain| {
            codec_domain::encode_neutral_task_domain(domain, FieldPath::root("initial_domains"))
                .map_err(|error| error.to_string())
        })
        .collect()
}

pub fn encode_participation_operation(
    request: &UpdateParticipation,
    attempt: &AttemptWireFacts,
) -> Result<proto::TaskOperation, String> {
    match request {
        UpdateParticipation::Establish(establish) => {
            let catalog_set = stored_message::<CatalogSet>(establish.catalog_binding().as_ref())
                .ok_or("establish catalog binding is not a codec-produced catalog set")?;
            let filter = stored_message::<proto::RuntimeFilterContribution>(
                establish.initial_runtime_filter().as_ref(),
            )
            .ok_or("establish runtime filter is not a codec-produced contribution")?;
            let query_options =
                stored_message::<proto::QueryOptions>(establish.query_options().as_ref())
                    .ok_or("establish query options are not codec-produced")?;
            let credential = stored_credential(establish.initial_credential().material().as_ref())
                .ok_or("establish credential is not codec-produced material")?;
            Ok(encode_establish_participation(
                establish,
                catalog_set.clone(),
                filter.clone(),
                proto::ParticipationCredentialDomain {
                    credential_domain_id: establish
                        .initial_credential()
                        .credential_domain_id()
                        .get(),
                    epoch: establish.initial_credential().epoch().get(),
                    descriptors: credential.descriptors().to_vec(),
                },
                *query_options,
                attempt.native_compatibility_id,
            ))
        }
        UpdateParticipation::AdvanceDomain(advance) => {
            let domain = codec_domain::encode_neutral_participation_domain(
                advance.domain(),
                FieldPath::root("advance_domain"),
            )
            .map_err(|error| error.to_string())?;
            Ok(encode_advance_participation_domain(advance, domain))
        }
        UpdateParticipation::RenewLease(renew) => Ok(encode_renew_lease(renew)),
    }
}

pub fn encode_establish_participation_operation(
    establish: &novarocks_execution::task_execution::EstablishParticipation,
    attempt: &AttemptWireFacts,
) -> Result<proto::TaskOperation, String> {
    let catalog_set = stored_message::<CatalogSet>(establish.catalog_binding().as_ref())
        .ok_or("establish catalog binding is not a codec-produced catalog set")?;
    let filter = stored_message::<proto::RuntimeFilterContribution>(
        establish.initial_runtime_filter().as_ref(),
    )
    .ok_or("establish runtime filter is not a codec-produced contribution")?;
    let query_options = stored_message::<proto::QueryOptions>(establish.query_options().as_ref())
        .ok_or("establish query options are not codec-produced")?;
    let credential = stored_credential(establish.initial_credential().material().as_ref())
        .ok_or("establish credential is not codec-produced material")?;
    Ok(encode_establish_participation(
        establish,
        catalog_set.clone(),
        filter.clone(),
        proto::ParticipationCredentialDomain {
            credential_domain_id: establish.initial_credential().credential_domain_id().get(),
            epoch: establish.initial_credential().epoch().get(),
            descriptors: credential.descriptors().to_vec(),
        },
        *query_options,
        attempt.native_compatibility_id,
    ))
}

/// Whether an owner consumes this kind's acknowledgement body.
///
/// `CancelTask` is deliberately absent even though its receipt carries a task
/// status on the wire. Per-task snapshots have exactly one publisher — the
/// status subscription — and reading the same fact from a second path would
/// let two producers publish status with no ordering between them, so a
/// cancel's status is observed through the subscription like every other.
pub(super) const fn consumes_ack_body(kind: OperationKind) -> bool {
    matches!(
        kind,
        OperationKind::AcquireParticipationAdmissionTicket
            | OperationKind::CreateTask
            | OperationKind::UpdateTask
            | OperationKind::UpdateParticipation
            | OperationKind::QuiesceParticipation
            | OperationKind::AbortParticipation
            | OperationKind::ReleaseParticipation
    )
}

pub(super) const fn is_applied(outcome: OperationOutcome) -> bool {
    matches!(
        outcome,
        OperationOutcome::Accepted | OperationOutcome::Idempotent
    )
}

/// Whether this exact operation result must carry its typed acknowledgement.
///
/// Abort differs from the other mutations: losing to an already-closing
/// participation is a successful stand-down observation even when the operation was
/// not newly applied. Its participation state is therefore part of the mandatory
/// receipt for every legal closing outcome.
pub(super) const fn consumes_ack_body_for_outcome(
    kind: OperationKind,
    outcome: OperationOutcome,
) -> bool {
    if matches!(kind, OperationKind::AbortParticipation) {
        return matches!(
            outcome,
            OperationOutcome::Accepted
                | OperationOutcome::Idempotent
                | OperationOutcome::ParticipationTerminalReceipt
                | OperationOutcome::LeaseExpired
                | OperationOutcome::Gone
        );
    }
    is_applied(outcome) && consumes_ack_body(kind)
}

/// Whether this intent is a lease renewal, decided from the neutral command
/// rather than from its kind, which an establish shares.
pub(super) fn is_lease_renewal(intent: &OperationIntent) -> bool {
    matches!(
        intent,
        OperationIntent::UpdateParticipation(request)
            if matches!(request.as_ref(), UpdateParticipation::RenewLease(_))
    )
}

/// One item this transport actually put on the wire.
#[derive(Copy, Clone, Debug)]
pub struct SentOperation {
    pub operation_id: TaskOperationId,
    pub kind: OperationKind,
    pub establish_participation: Option<ParticipationRef>,
    pub lease_renewal: bool,
    pub address: AckAddress,
}

/// What an acknowledgement body must be addressed to.
///
/// A receipt that agrees with itself proves nothing. This is the address the
/// frontend actually sent to, so a body naming a different task or participation is
/// refused rather than read as this request's proof.
#[derive(Clone, Copy, Debug)]
pub enum AckAddress {
    Admission(AcquireParticipationAdmissionTicket),
    Task(TaskIdentity),
    Participation(ParticipationRef),
    /// The kind carries no acknowledgement body.
    None,
}

impl AckAddress {
    pub fn of(intent: &OperationIntent) -> Self {
        match intent {
            OperationIntent::AcquireParticipationAdmissionTicket(request) => {
                Self::Admission(*request)
            }
            OperationIntent::EstablishParticipation(request) => {
                Self::Participation(request.participation())
            }
            OperationIntent::CreateTask(request) => Self::Task(request.identity()),
            OperationIntent::UpdateTask(request) => Self::Task(request.identity()),
            OperationIntent::CancelTask(request) => Self::Task(request.identity()),
            OperationIntent::QuiesceParticipation(request) => {
                Self::Participation(request.participation())
            }
            OperationIntent::UpdateParticipation(request) => {
                Self::Participation(request.participation())
            }
            OperationIntent::AbortParticipation(request) => {
                Self::Participation(request.participation())
            }
            OperationIntent::ReleaseParticipation(request) => {
                Self::Participation(request.participation())
            }
            #[cfg(test)]
            OperationIntent::FetchTaskDynamicFilters(_) => Self::None,
        }
    }
}

/// Reads one applied receipt's acknowledgement body.
///
/// Called only for an applied outcome of a kind whose owner consumes a
/// receipt. A body that does not match its kind, or that is addressed
/// elsewhere, is an error and never a silently empty payload: an applied
/// create with no receipt is a protocol violation, not a create that carried
/// nothing.
pub fn project_ack_payload(
    kind: OperationKind,
    address: AckAddress,
    receipt: &proto::TaskOperationReceipt,
) -> Result<AckPayloadProjection, String> {
    let path = || FieldPath::root("apply_task_operations").field("receipts");
    let Some(proto::task_operation_receipt::Result::Verdict(verdict)) = &receipt.result else {
        return Err(format!(
            "{kind} requires a protocol verdict acknowledgement"
        ));
    };
    let body = verdict
        .ack
        .as_ref()
        .ok_or_else(|| format!("{kind} requires an acknowledgement body for this outcome"))?;
    let mut termination = None;
    let payload = match (kind, body, address) {
        (
            OperationKind::AcquireParticipationAdmissionTicket,
            proto::task_operation_verdict::Ack::ParticipationAdmissionTicket(ack),
            AckAddress::Admission(expected),
        ) => codec::decode_participation_admission_ticket_ack(ack, expected, path())
            .map(AckPayload::AdmissionTicket)
            .map_err(|error| error.to_string()),
        (
            OperationKind::CreateTask,
            proto::task_operation_verdict::Ack::CreateTask(ack),
            AckAddress::Task(identity),
        ) => codec::decode_create_task_ack(ack, identity, path())
            .map(|receipt| AckPayload::Create(Box::new(receipt)))
            .map_err(|error| error.to_string()),
        (
            OperationKind::UpdateTask,
            proto::task_operation_verdict::Ack::UpdateTask(ack),
            AckAddress::Task(identity),
        ) => codec::decode_update_task_ack(ack, identity, path())
            .map(AckPayload::Update)
            .map_err(|error| error.to_string()),
        (
            OperationKind::UpdateParticipation | OperationKind::AbortParticipation,
            proto::task_operation_verdict::Ack::Participation(ack),
            AckAddress::Participation(participation),
        ) => codec::decode_participation_ack(ack, participation, path())
            .map(|(receipt, cause)| {
                termination = cause.map(|cause| AckTerminationDiagnostic {
                    participation,
                    cause,
                    release: false,
                });
                AckPayload::Participation(receipt)
            })
            .map_err(|error| error.to_string()),
        (
            OperationKind::QuiesceParticipation,
            proto::task_operation_verdict::Ack::QuiesceParticipation(ack),
            AckAddress::Participation(participation),
        ) => {
            let decoded =
                codec::decode_quiesce_ack(ack, path()).map_err(|error| error.to_string())?;
            if decoded.participation() != participation {
                return Err("a quiesce acknowledgement names a different participation".to_owned());
            }
            Ok(AckPayload::Quiesce(decoded))
        }
        (
            OperationKind::ReleaseParticipation,
            proto::task_operation_verdict::Ack::ReleaseParticipation(ack),
            AckAddress::Participation(participation),
        ) => {
            let decoded =
                codec::decode_release_ack(ack, path()).map_err(|error| error.to_string())?;
            termination = decoded
                .termination_cause
                .map(|cause| AckTerminationDiagnostic {
                    participation,
                    cause,
                    release: true,
                });
            release_payload(decoded, participation)
        }

        (kind, _, _) => Err(format!(
            "an applied {kind} carries an acknowledgement body of another kind"
        )),
    }?;
    Ok(AckPayloadProjection {
        payload,
        termination,
    })
}

pub(super) fn release_payload(
    decoded: codec::DecodedReleaseAck,
    participation: ParticipationRef,
) -> Result<AckPayload, String> {
    if decoded.participation != participation {
        return Err(
            "a release acknowledgement names a different participation than the request".to_owned(),
        );
    }
    Ok(AckPayload::Release {
        receipt: ParticipationReceipt::new(decoded.participation, decoded.state),
        outcome: decoded.outcome,
        runtime_filter: decoded.runtime_filter,
    })
}
pub fn project_ack_payload_owned(
    kind: OperationKind,
    address: AckAddress,
    receipt: proto::TaskOperationReceipt,
) -> Result<AckPayloadProjection, String> {
    if let (OperationKind::ReleaseParticipation, AckAddress::Participation(participation)) =
        (kind, address)
    {
        let Some(proto::task_operation_receipt::Result::Verdict(verdict)) = receipt.result else {
            return Err(format!(
                "{kind} requires a protocol verdict acknowledgement"
            ));
        };
        let Some(proto::task_operation_verdict::Ack::ReleaseParticipation(ack)) = verdict.ack
        else {
            return Err(format!(
                "an applied {kind} carries an acknowledgement body of another kind"
            ));
        };
        let decoded = codec::decode_release_ack_owned(
            ack,
            FieldPath::root("apply_task_operations").field("receipts"),
        )
        .map_err(|error| error.to_string())?;
        let termination = decoded
            .termination_cause
            .map(|cause| AckTerminationDiagnostic {
                participation,
                cause,
                release: true,
            });
        return release_payload(decoded, participation).map(|payload| AckPayloadProjection {
            payload,
            termination,
        });
    }
    project_ack_payload(kind, address, &receipt)
}

pub fn classify_apply_status(status: &tonic::Status) -> OperationDispatchResult {
    if novarocks_native_adapter::native_codec::is_operation_reply_preflight_failure(status) {
        // A reply arrived but its generated tree exceeded the original reply
        // grammar. This is a local refusal, never a Worker verdict or proof
        // of no remote effect. It must not authorize another dispatch.
        return OperationDispatchResult::NonWorkerRejected;
    }
    // Remote NO_ERROR GOAWAY leaves this immutable operation's outcome unknown.
    if status.code() == tonic::Code::Internal
        && task_operation_h2_source(status).is_some_and(|source| {
            source.is_go_away
                && source.is_remote
                && source.reason == Some(u32::from(h2::Reason::NO_ERROR))
        })
    {
        return OperationDispatchResult::TransportUnknown;
    }
    match status.code() {
        tonic::Code::Unavailable
        | tonic::Code::DeadlineExceeded
        | tonic::Code::Cancelled
        | tonic::Code::Unknown => OperationDispatchResult::TransportUnknown,
        // A gRPC failure is never a Worker receipt. Native ingress marks its
        // pre-admission category in metadata; an unmarked limit fails closed.
        tonic::Code::ResourceExhausted => OperationDispatchResult::IngressRejected(
            match status
                .metadata()
                .get("x-novarocks-ingress-rejection")
                .and_then(|value| value.to_str().ok())
            {
                Some("waiting_capacity") => {
                    crate::task_execution::operation::IngressRejection::WaitingCapacity
                }
                Some("body_limit") => crate::task_execution::operation::IngressRejection::BodyLimit,
                _ => crate::task_execution::operation::IngressRejection::UnclassifiedCapacity,
            },
        ),
        _ => OperationDispatchResult::NonWorkerRejected,
    }
}

pub(super) struct TaskOperationH2Source {
    pub(super) reason: Option<u32>,
    pub(super) is_go_away: bool,
    pub(super) is_reset: bool,
    pub(super) is_remote: bool,
    pub(super) is_library: bool,
    pub(super) is_io: bool,
    pub(super) io_kind: Option<std::io::ErrorKind>,
    pub(super) io_raw_os_error: Option<i32>,
}

impl fmt::Debug for TaskOperationH2Source {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TaskOperationH2Source")
            .field("reason", &self.reason)
            .field("is_go_away", &self.is_go_away)
            .field("is_reset", &self.is_reset)
            .field("is_remote", &self.is_remote)
            .field("is_library", &self.is_library)
            .field("is_io", &self.is_io)
            .field("io_kind", &self.io_kind)
            .field("io_raw_os_error", &self.io_raw_os_error)
            .finish()
    }
}

// Diagnostics borrow only the original typed source; messages grant no facts.
pub(super) fn task_operation_h2_source(status: &tonic::Status) -> Option<TaskOperationH2Source> {
    let mut source = std::error::Error::source(status);
    while let Some(error) = source {
        if let Some(error) = error.downcast_ref::<h2::Error>() {
            let io = error.get_io();
            return Some(TaskOperationH2Source {
                reason: error.reason().map(u32::from),
                is_go_away: error.is_go_away(),
                is_reset: error.is_reset(),
                is_remote: error.is_remote(),
                is_library: error.is_library(),
                is_io: error.is_io(),
                io_kind: io.map(std::io::Error::kind),
                io_raw_os_error: io.and_then(std::io::Error::raw_os_error),
            });
        }
        source = error.source();
    }
    None
}

/// Classifies a channel acquisition failure by type.
///
/// URI and connector construction are deterministic local failures that
/// resending cannot repair. A completed connector that then cannot dial or
/// establish its HTTP/2 stream leaves the remote outcome unknown.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChannelFailureClass {
    Fatal,
    RetryableNetwork,
}
pub fn classify_channel_failure(error: ChannelFailureClass) -> OperationDispatchResult {
    match error {
        ChannelFailureClass::Fatal => OperationDispatchResult::NonWorkerRejected,
        ChannelFailureClass::RetryableNetwork => OperationDispatchResult::TransportUnknown,
    }
}
pub(super) fn classify_channel_error(error: &ChannelAcquisitionError) -> OperationDispatchResult {
    classify_channel_failure(match error {
        ChannelAcquisitionError::Fatal(_) => ChannelFailureClass::Fatal,
        ChannelAcquisitionError::RetryableNetwork(_) => ChannelFailureClass::RetryableNetwork,
    })
}

pub(super) const fn worker_result(outcome: OperationOutcome) -> OperationDispatchResult {
    OperationDispatchResult::WorkerReceipt(WorkerReceiptOutcome::from_contract(outcome))
}

/// Whether a status stream rejection is fatal to the attempt.
///
/// A subscription is an observation channel: a lost stream is recovered by
/// resubscribing from the cursors, and only a typed rejection saying the
/// subscription itself is illegal or unauthorized can never be repaired that
/// way.
pub fn subscription_rejection_is_fatal(status: &tonic::Status) -> bool {
    matches!(
        status.code(),
        tonic::Code::InvalidArgument
            | tonic::Code::DataLoss
            | tonic::Code::Unimplemented
            | tonic::Code::PermissionDenied
            | tonic::Code::Unauthenticated
            | tonic::Code::FailedPrecondition
            | tonic::Code::NotFound
    )
}

pub enum EncodedMethodRequest {
    Ordinary(proto::ApplyTaskOperationsRequest),
    Control(proto::ApplyTaskControlOperationsRequest),
}

impl EncodedMethodRequest {
    pub(super) fn encoded_len(&self) -> usize {
        match self {
            Self::Ordinary(value) => prost::Message::encoded_len(value),
            Self::Control(value) => prost::Message::encoded_len(value),
        }
    }
}

pub struct EncodedMethodBatch {
    pub request: EncodedMethodRequest,
    pub items: usize,
    pub deadline: Duration,
}

impl EncodedMethodBatch {
    pub(super) fn encoded_len(&self) -> usize {
        match &self.request {
            EncodedMethodRequest::Ordinary(request) => prost::Message::encoded_len(request),
            EncodedMethodRequest::Control(request) => prost::Message::encoded_len(request),
        }
    }

    pub(super) fn expires_at(&self, submitted_at: tokio::time::Instant) -> tokio::time::Instant {
        submitted_at + self.deadline
    }
}

/// Preserve dispatcher order while directing each contiguous run to the
/// method whose closed input grammar accepts it. A run also shares one
/// absolute submit deadline: a longer item must not renew a shorter item.
pub fn encode_method_batches(
    operations: Vec<(bool, proto::TaskOperation)>,
    budget: TransportBudget,
) -> Result<Vec<EncodedMethodBatch>, novarocks_proto_codec::ProtocolError> {
    // This is also a direct internal test seam: apply the same source bound
    // before allocating any method-run carrier, even without a dispatcher.
    if operations.is_empty() || operations.len() > budget.max_batch_items() {
        return Err(novarocks_proto_codec::ProtocolError::new(
            FieldPath::root("task_operation_method_batches"),
            novarocks_proto_codec::ProtocolErrorKind::OutOfRange,
            "operation method batches exceed the original item bound",
        ));
    }
    fn key(item: &(bool, proto::TaskOperation)) -> (bool, Option<u64>) {
        (
            item.0,
            item.1
                .envelope
                .as_ref()
                .map(|envelope| envelope.max_wait_millis),
        )
    }
    let runs = 1 + operations
        .windows(2)
        .filter(|pair| key(&pair[0]) != key(&pair[1]))
        .count();
    let mut batches = Vec::with_capacity(runs);
    let mut iter = operations.into_iter();
    while let Some((control, first)) = iter.next() {
        let wait_millis = first
            .envelope
            .as_ref()
            .map(|envelope| envelope.max_wait_millis);
        let run_items = 1 + iter
            .as_slice()
            .iter()
            .take_while(|item| key(item) == (control, wait_millis))
            .count();
        let mut run = Vec::with_capacity(run_items);
        run.push(first);
        for (_, operation) in iter.by_ref().take(run_items - 1) {
            run.push(operation);
        }
        let deadline = Duration::from_millis(wait_millis.unwrap_or_default());
        let items = run.len();
        let request = if control {
            EncodedMethodRequest::Control(encode_control_operation_batch(run, budget)?)
        } else {
            EncodedMethodRequest::Ordinary(encode_operation_batch(run, budget)?)
        };
        batches.push(EncodedMethodBatch {
            request,
            items,
            deadline,
        });
    }
    let item_count = batches.iter().map(|batch| batch.items).sum();
    let encoded_bytes = batches.iter().fold(0usize, |total, batch| {
        total.saturating_add(batch.encoded_len())
    });
    if !budget.batch_fits(item_count, encoded_bytes) {
        return Err(novarocks_proto_codec::ProtocolError::new(
            FieldPath::root("task_operation_method_batches"),
            novarocks_proto_codec::ProtocolErrorKind::OutOfRange,
            "combined operation methods exceed the original batch budget",
        ));
    }
    Ok(batches)
}

pub struct CoveredRequestedVersions {
    pub(super) statuses: BTreeSet<TaskIdentity>,
    pub(super) task_convergence: BTreeSet<TaskIdentity>,
}

impl CoveredRequestedVersions {
    pub fn from_request(request: &DecodedCoveredSubscription) -> Self {
        Self {
            statuses: request
                .status_cursors
                .iter()
                .filter(|cursor| cursor.current_version().is_some())
                .map(|cursor| cursor.identity())
                .collect(),
            task_convergence: request
                .task_convergence_cursors
                .iter()
                .filter(|cursor| cursor.current_version().is_some())
                .map(|cursor| cursor.identity())
                .collect(),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum CoveredEventValidationError {
    ProcessMismatch,
    QueryMismatch,
    Protocol(&'static str),
}

impl CoveredEventValidationError {
    pub(super) const fn subscription_state(self) -> SubscriptionState {
        match self {
            Self::ProcessMismatch => SubscriptionState::ProcessMismatch,
            Self::QueryMismatch => SubscriptionState::QueryMismatch,
            Self::Protocol(_) => SubscriptionState::Rejected,
        }
    }
}

impl fmt::Display for CoveredEventValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ProcessMismatch => {
                formatter.write_str("covered observation addresses another backend process")
            }
            Self::QueryMismatch => {
                formatter.write_str("covered observation addresses another participation")
            }
            Self::Protocol(detail) => formatter.write_str(detail),
        }
    }
}

pub(super) fn validate_covered_task_participation(
    identity: TaskIdentity,
    participation: ParticipationRef,
) -> Result<(), CoveredEventValidationError> {
    if identity.query_execution_id() != participation.query_execution_id() {
        return Err(CoveredEventValidationError::QueryMismatch);
    }
    if identity.backend_process_id() != participation.backend_process_id() {
        return Err(CoveredEventValidationError::ProcessMismatch);
    }
    Ok(())
}

pub(super) fn validate_covered_receipt_participation(
    observed: ParticipationRef,
    expected: ParticipationRef,
) -> Result<(), CoveredEventValidationError> {
    if observed.query_execution_id() != expected.query_execution_id()
        || observed.frontend_process_id() != expected.frontend_process_id()
    {
        return Err(CoveredEventValidationError::QueryMismatch);
    }
    if observed.backend_process_id() != expected.backend_process_id() {
        return Err(CoveredEventValidationError::ProcessMismatch);
    }
    Ok(())
}

pub fn validate_covered_event_participation(
    request: &DecodedCoveredSubscription,
    requested_versions: &CoveredRequestedVersions,
    event: &CoveredStatusStreamEvent,
) -> Result<(), CoveredEventValidationError> {
    let participation = request.participation;
    // Classify identity before version prerequisites: a foreign Unchanged
    // frame is an identity violation even though no foreign cursor was sent.
    match &event.fact {
        CoveredStatusStreamFact::Status(status) => {
            validate_covered_task_participation(status.identity(), participation)?
        }
        CoveredStatusStreamFact::Gone(identity)
        | CoveredStatusStreamFact::Unknown(identity)
        | CoveredStatusStreamFact::StatusUnchanged(identity)
        | CoveredStatusStreamFact::TaskConvergenceUnchanged(identity) => {
            validate_covered_task_participation(*identity, participation)?
        }
        CoveredStatusStreamFact::TaskConvergence(receipt) => {
            validate_covered_task_participation(receipt.identity(), participation)?
        }
        CoveredStatusStreamFact::ParticipationConvergence(receipt) => {
            validate_covered_receipt_participation(receipt.participation(), participation)?
        }
        CoveredStatusStreamFact::Quiesce(receipt) => {
            validate_covered_receipt_participation(receipt.participation(), participation)?;
            for identity in receipt.accepted_tasks() {
                validate_covered_task_participation(*identity, participation)?;
            }
        }
        CoveredStatusStreamFact::CatchUpComplete(_) | CoveredStatusStreamFact::Bookmark(_) => {}
    }
    match &event.fact {
        CoveredStatusStreamFact::StatusUnchanged(identity)
            if !requested_versions.statuses.contains(identity) =>
        {
            return Err(CoveredEventValidationError::Protocol(
                "covered StatusUnchanged has no version in this subscription request",
            ));
        }
        CoveredStatusStreamFact::TaskConvergenceUnchanged(identity)
            if !requested_versions.task_convergence.contains(identity) =>
        {
            return Err(CoveredEventValidationError::Protocol(
                "covered TaskConvergenceUnchanged has no version in this subscription request",
            ));
        }
        CoveredStatusStreamFact::CatchUpComplete(marker)
            if marker.generation != request.generation.get() =>
        {
            return Err(CoveredEventValidationError::Protocol(
                "covered catch-up names another stream generation",
            ));
        }
        CoveredStatusStreamFact::Bookmark(marker)
            if marker.generation != request.generation.get() =>
        {
            return Err(CoveredEventValidationError::Protocol(
                "covered bookmark names another stream generation",
            ));
        }
        _ => {}
    }
    Ok(())
}

/// A passive diagnostic fact; only the executor logs it.
#[derive(Clone, Debug)]
pub struct AckTerminationDiagnostic {
    pub participation: ParticipationRef,
    pub cause: novarocks_execution_contract::task_execution::status::AbortCause,
    pub release: bool,
}
pub struct AckPayloadProjection {
    pub payload: AckPayload,
    pub termination: Option<AckTerminationDiagnostic>,
}
pub fn decode_ack(
    kind: OperationKind,
    address: AckAddress,
    receipt: &proto::TaskOperationReceipt,
) -> Result<AckPayload, String> {
    project_ack_payload(kind, address, receipt).map(|projection| projection.payload)
}
pub fn decode_ack_owned(
    kind: OperationKind,
    address: AckAddress,
    receipt: proto::TaskOperationReceipt,
) -> Result<AckPayload, String> {
    project_ack_payload_owned(kind, address, receipt).map(|projection| projection.payload)
}
/// Decode one covered frame with the same strict shared codec as production.
pub use novarocks_task_codec::operation::decode_covered_status_event;
/// Preserve request attribution and exact receipt order with the original codec.
pub fn decode_operation_receipts(
    response: &proto::ApplyTaskOperationsResponse,
    ids: &[TaskOperationId],
) -> Result<Vec<ReceiptHeader>, novarocks_proto_codec::ProtocolError> {
    decode_receipt_batch(
        response,
        ids,
        FieldPath::root("apply_task_operations_response"),
    )
}

pub struct AcknowledgementProjection {
    pub acknowledgement: OperationAcknowledgement,
    pub termination: Option<AckTerminationDiagnostic>,
    pub unreadable_ack: Option<String>,
}
impl AcknowledgementProjection {
    fn plain(acknowledgement: OperationAcknowledgement) -> Self {
        Self {
            acknowledgement,
            termination: None,
            unreadable_ack: None,
        }
    }
}
/// Builds one acknowledgement from one receipt, in its request item's order.
pub fn project_acknowledgement(
    item: &SentOperation,
    header: &ReceiptHeader,
    receipt: &proto::TaskOperationReceipt,
) -> AcknowledgementProjection {
    project_acknowledgement_with(item, header, || {
        project_ack_payload(item.kind, item.address, receipt)
    })
}
pub fn project_acknowledgement_owned(
    item: &SentOperation,
    header: &ReceiptHeader,
    receipt: proto::TaskOperationReceipt,
) -> AcknowledgementProjection {
    project_acknowledgement_with(item, header, || {
        project_ack_payload_owned(item.kind, item.address, receipt)
    })
}
fn project_acknowledgement_with(
    item: &SentOperation,
    header: &ReceiptHeader,
    decode: impl FnOnce() -> Result<AckPayloadProjection, String>,
) -> AcknowledgementProjection {
    if let novarocks_execution_contract::operation::OperationResult::DomainFailure {
        outcome,
        failure,
    } = header.result()
    {
        return AcknowledgementProjection::plain(OperationAcknowledgement::worker_domain_failure(
            item.operation_id,
            item.kind,
            *outcome,
            failure.clone(),
        ));
    }
    let outcome = header.outcome();
    if !consumes_ack_body_for_outcome(item.kind, outcome) {
        return AcknowledgementProjection::plain(
            OperationAcknowledgement::worker_receipt(
                item.operation_id,
                item.kind,
                outcome,
                AckPayload::None,
            )
            .with_detail(header.detail().cloned()),
        );
    }
    match decode() {
        Ok(projected) => AcknowledgementProjection {
            acknowledgement: OperationAcknowledgement::worker_receipt(
                item.operation_id,
                item.kind,
                outcome,
                projected.payload,
            )
            .with_detail(header.detail().cloned()),
            termination: projected.termination,
            unreadable_ack: None,
        },
        Err(detail) => AcknowledgementProjection {
            acknowledgement: OperationAcknowledgement::from_dispatch_result(
                item.operation_id,
                item.kind,
                OperationDispatchResult::NonWorkerRejected,
                AckPayload::None,
            ),
            termination: None,
            unreadable_ack: Some(detail),
        },
    }
}
