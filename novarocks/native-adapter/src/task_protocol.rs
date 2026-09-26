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

//! Native task-protocol RPC boundary.
//!
//! This is the adapter-owned contract between generated Native gRPC handlers
//! and a role-local task owner. The RPC service stays thin: it hands a wire
//! request to this port and encodes what comes back, so nothing above it
//! interprets a wire shape and nothing below it names one.
//!
//! Six entry points: ordinary and small-control mutation methods, one status
//! subscription, two typed observation reads, and the root result data plane.

use std::future::Future;
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytes::Bytes;
use novarocks_execution_contract::task_execution::identity::{
    AdmissionTicketId, QueryContextRef, TaskIdentity, TaskOperationId,
};
use novarocks_execution_contract::task_execution::operation::OperationKind;
use novarocks_execution_contract::task_execution::operation::{
    FetchTaskDynamicFilters, GetFinalTaskInfo, OperationOutcome, ResultByteLimit,
};
use novarocks_execution_contract::task_execution::status::{SafeDetail, TaskFailureCategory};
use novarocks_proto_codec::FieldPath;
use novarocks_proto_models::novarocks as proto;
use novarocks_task_codec::TransportBudget;
use novarocks_task_codec::identity::{decode_admission_ticket_id, decode_query_context_ref};
use novarocks_task_codec::operation::{
    CoveredCatchUpComplete, CoveredStatusStreamEvent, CoveredStatusStreamFact, DecodedOperation,
    NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES, decode_control_operation_batch,
    decode_covered_subscribe_task_status, decode_envelope, decode_fetch_dynamic_filters,
    decode_get_final_task_info, decode_ordinary_operation_batch,
    decode_ordinary_operation_batch_with_skip, encode_covered_status_event,
    encode_operation_outcome, encode_receipt,
};
use novarocks_task_codec::status::encode_final_task_info;
use novarocks_task_codec::{
    domain::encode_task_dynamic_filter_domain, identity::encode_task_identity,
};
use prost::Message;
use tokio_stream::Stream;

use crate::native_ingress::NativeIngressOwnership;
use crate::task_protocol_fault;
use novarocks_worker::{
    AdmissionTicketObservation, CoveredObservationFact, CoveredObservationFrame,
    CoveredSubscription, DynamicFilterReadOutcome, FinalTaskInfoOutcome, HostRejection,
    OperationReceipt, TaskDynamicFilterRead, TaskStatusSource,
};

/// The Tower-owned request clock carried into task operation dispatch.
#[derive(Clone, Copy, Debug)]
pub struct TaskIngressTiming {
    arrival: Instant,
    deadline: Instant,
}

impl TaskIngressTiming {
    pub fn new(arrival: Instant, deadline: Instant) -> Self {
        Self { arrival, deadline }
    }

    pub fn starting_now() -> Self {
        let arrival = Instant::now();
        Self::new(arrival, arrival + Duration::from_secs(300))
    }

    fn operation_deadline(self, max_wait: Duration) -> Instant {
        self.deadline.min(self.arrival + max_wait)
    }
}

/// Server-side status event stream of one logical query-by-backend
/// subscription.
pub type TaskStatusEventStream =
    Pin<Box<dyn Stream<Item = Result<proto::TaskStatusStreamEvent, tonic::Status>> + Send>>;

/// The acknowledgement body carried by one task-operation receipt.
pub type TaskOperationReceiptAck = proto::task_operation_receipt::Ack;

/// A validated root-result read passed from the Native wire adapter to the
/// role-local result owner.
#[derive(Clone, Copy, Debug)]
pub struct TaskResultReadRequest {
    identity: TaskIdentity,
    max_wait: Duration,
    acknowledged_packet_sequence: Option<i64>,
    max_result_bytes: ResultByteLimit,
}

impl TaskResultReadRequest {
    fn new(
        identity: TaskIdentity,
        max_wait: Duration,
        acknowledged_packet_sequence: Option<i64>,
        max_result_bytes: ResultByteLimit,
    ) -> Self {
        Self {
            identity,
            max_wait,
            acknowledged_packet_sequence,
            max_result_bytes,
        }
    }

    /// The exact task identity whose root-result responsibility is queried.
    pub const fn identity(self) -> TaskIdentity {
        self.identity
    }

    /// The bounded wait accepted by the Native request.
    pub const fn max_wait(self) -> Duration {
        self.max_wait
    }

    /// The packet acknowledged by this read, if any.
    pub const fn acknowledged_packet_sequence(self) -> Option<i64> {
        self.acknowledged_packet_sequence
    }

    /// The already validated payload ceiling for this response.
    pub const fn max_result_bytes(self) -> ResultByteLimit {
        self.max_result_bytes
    }
}

/// One role-local result-buffer observation, before Native response encoding.
#[derive(Debug)]
pub enum TaskResultRead {
    /// One retained payload packet, including the retained EOS packet.
    Ready {
        packet_sequence: i64,
        end_of_stream: bool,
        payload: Bytes,
    },
    /// The exact acknowledged EOS was consumed or replayed.
    EndOfStream { packet_sequence: i64 },
    /// No result is ready before the accepted read bound.
    NotReady,
    /// A settled, in-band failure or refusal from the role-local owner.
    Error { detail: String },
}

/// A role-local invariant failure while answering a validated result read.
#[derive(Debug)]
pub struct TaskResultReadError {
    detail: String,
}

impl TaskResultReadError {
    /// Builds an error whose detail is safe for the Native transport boundary.
    pub fn new(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
        }
    }

    /// The safe detail exposed as an internal Native status.
    pub fn detail(&self) -> &str {
        &self.detail
    }
}

/// Role-local authority over root-result routing, buffering, and drain facts.
#[tonic::async_trait]
pub trait TaskResultReader: Send + Sync {
    /// Reads one exact root-result packet after Native wire validation.
    async fn read_task_result(
        &self,
        request: TaskResultReadRequest,
        ownership: Option<Arc<NativeIngressOwnership>>,
    ) -> Result<TaskResultRead, TaskResultReadError>;
}

/// Role-local authority over task observation facts after Native request
/// validation. These reads never mutate registry state or renew a lease.
pub trait TaskObservationReader: Send + Sync {
    /// Reads a task's dynamic-filter advertisement from the role-local owner.
    fn read_task_dynamic_filters(
        &self,
        request: &FetchTaskDynamicFilters,
    ) -> DynamicFilterReadOutcome;

    /// Reads a task's terminal diagnostic information from the role-local owner.
    fn read_final_task_info(&self, request: &GetFinalTaskInfo) -> FinalTaskInfoOutcome;
}

/// Role-local authority over the observation channel for one held query
/// context. The channel and every retained status fact remain role-owned.
pub trait TaskStatusSubscriptionReader: Send + Sync {
    /// Returns the source only when this role currently holds the exact context.
    fn task_status_source(&self, context: QueryContextRef) -> Option<Arc<TaskStatusSource>>;
}

/// Role-local authority that linearly applies one already decoded task
/// operation. It never receives a raw Native request.
pub trait TaskOperationBatchApplier: Send + Sync {
    /// Applies one domain operation and returns its role-owned receipt.
    ///
    /// The operation is handed over by value because a create carries a
    /// short-lived creation input that is moved, never cloned, to whichever
    /// owner wins that identity's creation, and is otherwise dropped unread.
    fn apply_task_operation(
        &self,
        operation: DecodedOperation,
        local_wait_cap: Duration,
    ) -> Result<proto::TaskOperationReceipt, tonic::Status>;

    /// The Worker owner's context-bound, read-only ticket observation.
    fn observe_admission_ticket(
        &self,
        ticket_id: AdmissionTicketId,
        context: QueryContextRef,
    ) -> AdmissionTicketObservation;

    /// A narrow Worker-authored refusal only when an absent context would
    /// reach expiry redemption next. All ambiguous states return None.
    fn preflight_expired_establish_ticket(
        &self,
        operation_id: TaskOperationId,
        ticket_id: AdmissionTicketId,
        context: QueryContextRef,
        native_compatibility_id: Option<&proto::NativeCompatibilityId>,
    ) -> Result<Option<proto::TaskOperationReceipt>, tonic::Status>;
}

/// Decodes one bounded operation batch and delegates each item in request
/// order to its role-local owner.
///
/// There is no longer a confidentiality gate in front of this. It existed
/// because vended material crossed here and was legal only on an encrypted
/// transport; material no longer crosses at all, so the gate had nothing left
/// to guard (CAD-1 D1).
pub fn apply_task_operations(
    applier: &dyn TaskOperationBatchApplier,
    request: proto::ApplyTaskOperationsRequest,
) -> Result<proto::ApplyTaskOperationsResponse, tonic::Status> {
    apply_task_operations_at(applier, request, TaskIngressTiming::starting_now())
}

pub fn apply_task_operations_at(
    applier: &dyn TaskOperationBatchApplier,
    request: proto::ApplyTaskOperationsRequest,
    timing: TaskIngressTiming,
) -> Result<proto::ApplyTaskOperationsResponse, tonic::Status> {
    let root = FieldPath::root("apply_task_operations");
    let budget = TransportBudget::DEFAULT;
    if !budget.batch_fits(request.operations.len(), request.encoded_len())
        || request
            .operations
            .iter()
            .any(|item| item.envelope.is_none())
    {
        // The authoritative decoder supplies the exact rejection without
        // touching the Worker ticket owner.
        return match decode_ordinary_operation_batch(&request, budget, root) {
            Err(error) => Err(tonic::Status::invalid_argument(error.to_string())),
            Ok(_) => unreachable!("the batch or envelope precondition was false"),
        };
    }
    let mut envelopes = Vec::with_capacity(request.operations.len());
    for (index, item) in request.operations.iter().enumerate() {
        // `decode_envelope` validates identity and wait independently of the
        // kind; the full typed decoder supplies the real kind below.
        let envelope = decode_envelope(
            item.envelope.as_ref().expect("checked envelope"),
            OperationKind::UpdateQueryContext,
            FieldPath::root("apply_task_operations")
                .field("operations")
                .index(index)
                .field("envelope"),
        )
        .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
        envelopes.push(envelope);
    }
    // Read only the formal context and ticket identities before the full
    // Establish content is decoded. The observation is advisory: the Worker
    // still owns every final verdict and atomically redeems the ticket.
    let mut ticket_preflight = Vec::with_capacity(request.operations.len());
    let mut early_receipts = vec![None; request.operations.len()];
    for (index, item) in request.operations.iter().enumerate() {
        let Some(proto::task_operation::Operation::UpdateQueryContext(update)) = &item.operation
        else {
            ticket_preflight.push(None);
            continue;
        };
        let Some(proto::update_query_context_request::Command::Establish(establish)) =
            &update.command
        else {
            ticket_preflight.push(None);
            continue;
        };
        let Some(context) = &establish.query_context else {
            ticket_preflight.push(None);
            continue;
        };
        let Some(ticket_id) = &establish.admission_ticket_id else {
            ticket_preflight.push(None);
            continue;
        };
        let path = FieldPath::root("apply_task_operations")
            .field("operations")
            .index(index)
            .field("update_query_context")
            .field("establish");
        let context = decode_query_context_ref(context, path.clone().field("query_context"))
            .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
        let ticket_id = decode_admission_ticket_id(ticket_id, path.field("admission_ticket_id"))
            .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
        ticket_preflight.push(match applier.observe_admission_ticket(ticket_id, context) {
            AdmissionTicketObservation::Issued { remaining } => {
                Some((ticket_id, context, Instant::now() + remaining))
            }
            AdmissionTicketObservation::Expired
                if index == 0
                    && Instant::now()
                        < timing.operation_deadline(envelopes[index].max_wait().get()) =>
            {
                // The Worker supplies the only early verdict. The codec still
                // validates every other item before any item is applied.
                if let Some(receipt) = applier.preflight_expired_establish_ticket(
                    envelopes[index].operation_id(),
                    ticket_id,
                    context,
                    establish.native_compatibility_id.as_ref(),
                )? {
                    early_receipts[index] = Some(receipt);
                }
                None
            }
            _ => None,
        });
    }
    // Every non-skipped item is fully decoded and method-classified before
    // any operation is applied. Skipped Establish items have Worker receipts.
    let skip = early_receipts
        .iter()
        .map(Option::is_some)
        .collect::<Vec<_>>();
    let operations = decode_ordinary_operation_batch_with_skip(
        &request,
        TransportBudget::DEFAULT,
        &skip,
        FieldPath::root("apply_task_operations"),
    )
    .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
    // The neutral operation constructors currently carry their default wait.
    // The validated wire envelopes retain each caller's actual max_wait.
    let waits = request
        .operations
        .iter()
        .map(|item| {
            Duration::from_millis(
                item.envelope
                    .as_ref()
                    .expect("decoded envelope")
                    .max_wait_millis,
            )
        })
        .collect::<Vec<_>>();
    apply_decoded_operations(
        applier,
        operations,
        &early_receipts,
        &waits,
        &ticket_preflight,
        timing,
    )
}

/// Applies only the four closed control shapes through the existing owner.
/// The dedicated execution capacity for this method is installed later.
pub fn apply_task_control_operations(
    applier: &dyn TaskOperationBatchApplier,
    request: proto::ApplyTaskControlOperationsRequest,
) -> Result<proto::ApplyTaskOperationsResponse, tonic::Status> {
    apply_task_control_operations_at(applier, request, TaskIngressTiming::starting_now())
}

pub fn apply_task_control_operations_at(
    applier: &dyn TaskOperationBatchApplier,
    request: proto::ApplyTaskControlOperationsRequest,
    timing: TaskIngressTiming,
) -> Result<proto::ApplyTaskOperationsResponse, tonic::Status> {
    let operations = decode_control_operation_batch(
        &request,
        TransportBudget::DEFAULT,
        FieldPath::root("apply_task_control_operations"),
    )
    .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
    let waits = request
        .operations
        .iter()
        .map(|item| {
            Duration::from_millis(
                item.envelope
                    .as_ref()
                    .expect("decoded envelope")
                    .max_wait_millis,
            )
        })
        .collect::<Vec<_>>();
    let operations = operations.into_iter().map(Some).collect::<Vec<_>>();
    let early_receipts = vec![None; operations.len()];
    apply_decoded_operations(applier, operations, &early_receipts, &waits, &[], timing)
}

/// Applies each decoded item in request order.
///
/// The batch owns its decoded items, and each is handed to the applier by
/// value. An item that is refused here, before the applier, drops whatever it
/// carried unread -- including a create's input -- exactly as an item the
/// Worker answers from an existing identity does.
fn apply_decoded_operations(
    applier: &dyn TaskOperationBatchApplier,
    operations: Vec<Option<DecodedOperation>>,
    early_receipts: &[Option<proto::TaskOperationReceipt>],
    waits: &[Duration],
    ticket_preflight: &[Option<(AdmissionTicketId, QueryContextRef, Instant)>],
    timing: TaskIngressTiming,
) -> Result<proto::ApplyTaskOperationsResponse, tonic::Status> {
    let mut receipts = Vec::with_capacity(operations.len());
    for (index, (operation, max_wait)) in operations
        .into_iter()
        .zip(waits.iter().copied())
        .enumerate()
    {
        if let Some(receipt) = early_receipts[index].as_ref() {
            receipts.push(receipt.clone());
            continue;
        }
        let operation = operation.expect("unskipped item was decoded");
        let now = Instant::now();
        let operation_deadline = timing.operation_deadline(max_wait);
        if now >= operation_deadline {
            receipts.push(encode_receipt(
                operation.envelope().operation_id(),
                OperationOutcome::OperationTimedOut,
                "operation exceeded its original ingress deadline",
                None,
            ));
            continue;
        }
        let mut remaining = operation_deadline.duration_since(now);
        if let Some((ticket_id, context, first_deadline)) =
            ticket_preflight.get(index).copied().flatten()
            && let AdmissionTicketObservation::Issued {
                remaining: ticket_remaining,
            } = applier.observe_admission_ticket(ticket_id, context)
        {
            // The Worker owns the current state. Once redeemed, the original
            // issuance deadline no longer limits exact Establish replay.
            remaining = remaining.min(ticket_remaining);
            remaining = remaining.min(first_deadline.saturating_duration_since(now));
        }
        receipts.push(applier.apply_task_operation(operation, remaining)?);
    }
    Ok(proto::ApplyTaskOperationsResponse { receipts })
}

/// Decodes, captures, and starts one Native task-status subscription.
pub fn subscribe_task_status(
    reader: &dyn TaskStatusSubscriptionReader,
    request: proto::SubscribeTaskStatusRequest,
) -> Result<TaskStatusEventStream, tonic::Status> {
    let decoded =
        decode_covered_subscribe_task_status(&request, FieldPath::root("subscribe_task_status"))
            .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
    let source = reader.task_status_source(decoded.context).ok_or_else(|| {
        tonic::Status::failed_precondition(
            "subscribe names a query context this backend does not hold",
        )
    })?;
    let quiesce_cursor =
        decoded
            .quiesce_cursor
            .map(|cursor| novarocks_worker::QuiesceObservationCursor {
                context: cursor.context,
                fence_version: cursor.fence_version,
            });
    let covered = source
        .begin_covered_subscription_with_quiesce(
            decoded.context,
            decoded.generation.get(),
            &decoded.status_cursors,
            &decoded.task_convergence_cursors,
            decoded.context_cursor,
            quiesce_cursor,
            &decoded.required_identities,
            novarocks_task_codec::status::MAX_SUBSCRIPTION_CURSORS,
        )
        .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
    if task_protocol_fault::task_status_subscription_dropped(decoded.context)? {
        return Ok(Box::pin(tokio_stream::once(Err(
            tonic::Status::unavailable(
                "runner-owned task status stream dropped after the subscription was established",
            ),
        ))));
    }
    Ok(Box::pin(CoveredTaskStatusSubscription::new(
        decoded.context,
        decoded.generation.get(),
        covered,
    )))
}

/// Decodes, delegates, classifies, and encodes one dynamic-filter observation.
pub fn fetch_task_dynamic_filters(
    reader: &dyn TaskObservationReader,
    request: proto::FetchTaskDynamicFiltersRequest,
) -> Result<proto::FetchTaskDynamicFiltersResponse, tonic::Status> {
    // An observation read carries no envelope on the wire, so this Native
    // boundary mints its non-replayable operation identity after validation.
    let read = decode_fetch_dynamic_filters(
        &request,
        TaskOperationId::new_v7(),
        FieldPath::root("fetch_task_dynamic_filters"),
    )
    .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
    let identity = read.identity();
    let receipt = reader.read_task_dynamic_filters(&read);
    // `Accepted` carries a version newer than the caller acknowledged and
    // `Idempotent` says it is already current. This response has no outcome
    // field, so every other receipt is a refusal, not an empty answer.
    if !matches!(
        receipt.outcome(),
        OperationOutcome::Accepted | OperationOutcome::Idempotent
    ) {
        return Err(tonic::Status::failed_precondition(
            receipt.detail().map_or("", SafeDetail::as_str).to_owned(),
        ));
    }
    encode_dynamic_filter_read(identity, receipt.acknowledgement()).map_err(host_rejection_status)
}

/// Decodes, delegates, and encodes one final-task observation without
/// reclassifying the role owner's receipt.
pub fn get_final_task_info(
    reader: &dyn TaskObservationReader,
    request: proto::GetFinalTaskInfoRequest,
) -> Result<proto::GetFinalTaskInfoResponse, tonic::Status> {
    let read = decode_get_final_task_info(
        &request,
        TaskOperationId::new_v7(),
        FieldPath::root("get_final_task_info"),
    )
    .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
    Ok(encode_final_task_info_response(
        &reader.read_final_task_info(&read),
    ))
}

/// Decodes, delegates, and encodes one Native root-result read.
pub async fn fetch_task_result(
    reader: &dyn TaskResultReader,
    request: proto::FetchTaskResultRequest,
) -> Result<proto::FetchResultResponse, tonic::Status> {
    fetch_task_result_with_ownership(reader, request, None).await
}

/// Keeps the Tower slot with the synchronous root route check if the RPC
/// future is cancelled while the blocking pool is waiting for a worker.
pub async fn fetch_task_result_with_ownership(
    reader: &dyn TaskResultReader,
    request: proto::FetchTaskResultRequest,
    ownership: Option<Arc<NativeIngressOwnership>>,
) -> Result<proto::FetchResultResponse, tonic::Status> {
    use proto::fetch_result_response::Status as FetchStatus;

    let (identity, max_wait, acknowledged, max_result_bytes) =
        novarocks_task_codec::operation::decode_fetch_task_result(
            &request,
            novarocks_proto_codec::FieldPath::root("fetch_task_result"),
        )
        .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
    let acknowledged_packet_sequence = acknowledged
        .map(|sequence| {
            i64::try_from(sequence.get()).map_err(|_| {
                tonic::Status::invalid_argument(format!(
                    "acknowledged result packet sequence {} exceeds the wire response range",
                    sequence.get()
                ))
            })
        })
        .transpose()?;
    let read = reader
        .read_task_result(
            TaskResultReadRequest::new(
                identity,
                max_wait,
                acknowledged_packet_sequence,
                max_result_bytes,
            ),
            ownership,
        )
        .await
        .map_err(|error| tonic::Status::internal(error.detail().to_owned()))?;
    Ok(match read {
        TaskResultRead::Ready {
            packet_sequence,
            end_of_stream,
            payload,
        } => task_result_response(
            FetchStatus::Ready,
            String::new(),
            packet_sequence,
            end_of_stream,
            payload,
        ),
        TaskResultRead::EndOfStream { packet_sequence } => task_result_response(
            FetchStatus::Eof,
            String::new(),
            packet_sequence,
            true,
            Bytes::new(),
        ),
        TaskResultRead::NotReady => {
            task_result_response(FetchStatus::NotReady, String::new(), 0, false, Bytes::new())
        }
        TaskResultRead::Error { detail } => {
            task_result_response(FetchStatus::Error, detail, 0, false, Bytes::new())
        }
    })
}

fn task_result_response(
    status: proto::fetch_result_response::Status,
    message: String,
    packet_sequence: i64,
    end_of_stream: bool,
    result_arrow_ipc: Bytes,
) -> proto::FetchResultResponse {
    proto::FetchResultResponse {
        status: status as i32,
        message,
        packet_seq: packet_sequence,
        eos: end_of_stream,
        result_arrow_ipc,
    }
}

/// Maps a role-owner rejection onto the status code of a wire read that has no
/// in-band outcome field.
pub fn host_rejection_status(rejection: HostRejection) -> tonic::Status {
    let detail = rejection.detail().as_str().to_owned();
    match rejection.category() {
        TaskFailureCategory::Protocol => tonic::Status::invalid_argument(detail),
        TaskFailureCategory::ResourceExhausted => tonic::Status::resource_exhausted(detail),
        TaskFailureCategory::Execution
        | TaskFailureCategory::Exchange
        | TaskFailureCategory::Internal => tonic::Status::internal(detail),
    }
}

/// Projects one task's dynamic-filter advertisement onto its Native read
/// response without treating an unprojectable payload as an empty version.
pub fn encode_dynamic_filter_read(
    identity: TaskIdentity,
    read: Option<&TaskDynamicFilterRead>,
) -> Result<proto::FetchTaskDynamicFiltersResponse, HostRejection> {
    let (version, domains) = match read {
        Some(read) => {
            let domain = encode_task_dynamic_filter_domain(read.version(), read.payload().as_ref())
                .map_err(|error| {
                    HostRejection::new(
                        TaskFailureCategory::Internal,
                        format!(
                            "dynamic filter version {} cannot be projected: {error}",
                            read.version().get()
                        ),
                    )
                })?;
            (read.version().get(), vec![domain])
        }
        None => (0, Vec::new()),
    };
    Ok(proto::FetchTaskDynamicFiltersResponse {
        identity: Some(encode_task_identity(identity)),
        version,
        domains,
    })
}

/// Encodes a final-task observation without reclassifying the Worker receipt.
pub fn encode_final_task_info_response(
    receipt: &FinalTaskInfoOutcome,
) -> proto::GetFinalTaskInfoResponse {
    let result = match receipt.acknowledgement() {
        Some(info) => {
            proto::get_final_task_info_response::Result::Info(encode_final_task_info(info))
        }
        // Losing final info costs diagnostics only, so why it is missing is
        // reported as an outcome rather than as an error.
        None => proto::get_final_task_info_response::Result::Unavailable(encode_operation_outcome(
            receipt.outcome(),
        )),
    };
    proto::GetFinalTaskInfoResponse {
        result: Some(result),
    }
}

/// Encodes one role-owner receipt without inventing an acknowledgement whose
/// state the Native wire cannot represent.
pub fn encode_operation_receipt<T>(
    receipt: &OperationReceipt<T>,
    encode_ack: impl FnOnce(&T) -> Option<TaskOperationReceiptAck>,
) -> Result<proto::TaskOperationReceipt, tonic::Status> {
    let ack = match receipt.acknowledgement() {
        Some(body) => Some(encode_ack(body).ok_or_else(|| {
            tonic::Status::internal(
                "operation acknowledgement reports a state with no wire representation",
            )
        })?),
        None => None,
    };
    Ok(encode_receipt(
        receipt.operation_id(),
        receipt.outcome(),
        receipt.detail().map_or("", SafeDetail::as_str),
        ack,
    ))
}

const TASK_STATUS_BOOKMARK_INTERVAL: Duration = Duration::from_secs(1);

fn covered_stream_fact(fact: CoveredObservationFact) -> CoveredStatusStreamFact {
    match fact {
        CoveredObservationFact::Status(status) => CoveredStatusStreamFact::Status(status),
        CoveredObservationFact::StatusUnchanged(identity) => {
            CoveredStatusStreamFact::StatusUnchanged(identity)
        }
        CoveredObservationFact::Unknown(identity) => CoveredStatusStreamFact::Unknown(identity),
        CoveredObservationFact::Gone(identity) => CoveredStatusStreamFact::Gone(identity),
        CoveredObservationFact::TaskConvergence(receipt) => {
            CoveredStatusStreamFact::TaskConvergence(receipt)
        }
        CoveredObservationFact::TaskConvergenceUnchanged(identity) => {
            CoveredStatusStreamFact::TaskConvergenceUnchanged(identity)
        }
        CoveredObservationFact::ContextConvergence(receipt) => {
            CoveredStatusStreamFact::ContextConvergence(receipt)
        }
        CoveredObservationFact::Quiesce(receipt) => CoveredStatusStreamFact::Quiesce(receipt),
    }
}

fn encode_covered_frame(
    frame: CoveredObservationFrame,
    generation: u64,
) -> Result<proto::TaskStatusStreamEvent, tonic::Status> {
    let event = match frame {
        CoveredObservationFrame::CatchUp(fact) => CoveredStatusStreamEvent {
            fact: covered_stream_fact(fact),
            source_revision: None,
        },
        CoveredObservationFrame::CatchUpComplete { initial_cut } => CoveredStatusStreamEvent {
            fact: CoveredStatusStreamFact::CatchUpComplete(CoveredCatchUpComplete {
                generation,
                initial_cut,
            }),
            source_revision: None,
        },
        CoveredObservationFrame::Live { revision, fact } => CoveredStatusStreamEvent {
            fact: covered_stream_fact(fact),
            source_revision: Some(revision),
        },
    };
    encode_covered_status_event(&event).map_err(|error| tonic::Status::internal(error.to_string()))
}

fn covered_frame_task_identity(frame: &CoveredObservationFrame) -> Option<TaskIdentity> {
    let fact = match frame {
        CoveredObservationFrame::CatchUp(fact) | CoveredObservationFrame::Live { fact, .. } => fact,
        CoveredObservationFrame::CatchUpComplete { .. } => return None,
    };
    match fact {
        CoveredObservationFact::Status(status) => Some(status.identity()),
        CoveredObservationFact::StatusUnchanged(identity)
        | CoveredObservationFact::Unknown(identity)
        | CoveredObservationFact::Gone(identity)
        | CoveredObservationFact::TaskConvergenceUnchanged(identity) => Some(*identity),
        CoveredObservationFact::TaskConvergence(receipt) => Some(receipt.identity()),
        CoveredObservationFact::ContextConvergence(_) | CoveredObservationFact::Quiesce(_) => None,
    }
}

/// The covered subscription sends finite catch-up pages and live facts in one
/// ordered stream. A selected source frame becomes covered only when this
/// stream returns it to the transport; periodic bookmarks use the same order.
struct CoveredTaskStatusSubscription {
    generation: u64,
    position: Arc<CoveredSubscription>,
    wake: Option<Pin<Box<dyn Future<Output = u64> + Send>>>,
    bookmark_timer: Pin<Box<tokio::time::Sleep>>,
}

impl CoveredTaskStatusSubscription {
    fn new(context: QueryContextRef, generation: u64, position: CoveredSubscription) -> Self {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        context.hash(&mut hasher);
        let stagger = Duration::from_millis(hasher.finish() % 750);
        let first = tokio::time::Instant::now() + Duration::from_millis(250) + stagger;
        Self {
            generation,
            position: Arc::new(position),
            wake: None,
            bookmark_timer: Box::pin(tokio::time::sleep_until(first)),
        }
    }

    fn encode_bookmark(&self) -> Result<proto::TaskStatusStreamEvent, tonic::Status> {
        let bookmark = self.position.bookmark();
        encode_covered_status_event(&CoveredStatusStreamEvent {
            fact: CoveredStatusStreamFact::Bookmark(
                novarocks_task_codec::operation::CoveredObservationBookmark {
                    generation: bookmark.generation,
                    sequence: bookmark.sequence,
                    covered_prefix: bookmark.covered_prefix,
                    source_cut: bookmark.source_cut,
                },
            ),
            source_revision: None,
        })
        .map_err(|error| tonic::Status::internal(error.to_string()))
    }
}

impl Stream for CoveredTaskStatusSubscription {
    type Item = Result<proto::TaskStatusStreamEvent, tonic::Status>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if this.bookmark_timer.as_mut().poll(context).is_ready() {
                this.bookmark_timer
                    .as_mut()
                    .reset(tokio::time::Instant::now() + TASK_STATUS_BOOKMARK_INTERVAL);
                return Poll::Ready(Some(this.encode_bookmark()));
            }

            // Read the cut before selection. A publication after an empty
            // selection then makes wait_for_change return immediately.
            let observed_cut = this.position.source_cut();
            let mut encoded = None;
            let mut encode_error = None;
            let selected =
                this.position
                    .select_next(NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES, |frame| {
                        match encode_covered_frame(frame.clone(), this.generation) {
                            Ok(wire) => {
                                let bytes = wire.encoded_len();
                                encoded = Some(wire);
                                bytes
                            }
                            Err(error) => {
                                encode_error = Some(error);
                                NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES + 1
                            }
                        }
                    });
            if let Some(error) = encode_error {
                return Poll::Ready(Some(Err(error)));
            }
            match selected {
                Ok(Some(selected)) => {
                    let task_identity = covered_frame_task_identity(&selected.frame);
                    let mut wire = match encoded {
                        Some(wire) => wire,
                        None => match encode_covered_frame(selected.frame.clone(), this.generation)
                        {
                            Ok(wire) => wire,
                            Err(error) => return Poll::Ready(Some(Err(error))),
                        },
                    };
                    if wire.encoded_len() > NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES {
                        return Poll::Ready(Some(Err(tonic::Status::resource_exhausted(
                            "covered observation frame exceeds the native message bound",
                        ))));
                    }
                    if let Some(task_identity) = task_identity {
                        task_protocol_fault::task_status_foreign_process(task_identity, &mut wire);
                    }
                    if let Err(error) = this.position.note_delivered(selected.delivery_id) {
                        return Poll::Ready(Some(Err(tonic::Status::internal(error.to_string()))));
                    }
                    this.wake = None;
                    return Poll::Ready(Some(Ok(wire)));
                }
                Err(error) => {
                    return Poll::Ready(Some(Err(tonic::Status::resource_exhausted(
                        error.to_string(),
                    ))));
                }
                Ok(None) => {}
            }

            if this.wake.is_none() {
                let position = Arc::clone(&this.position);
                this.wake = Some(Box::pin(async move {
                    position.wait_for_change(observed_cut).await
                }));
            }
            let wake = this.wake.as_mut().expect("covered wait installed");
            match wake.as_mut().poll(context) {
                Poll::Ready(_) => {
                    this.wake = None;
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// The Native task-protocol ingress port.
///
/// Every method takes the wire request and returns the wire response, because
/// this is the wire boundary. A `tonic::Status` here means the request could
/// not be understood at all; a request that was understood and refused comes
/// back as a typed receipt or outcome inside a successful response, which is
/// what lets a frontend classify it without reading an error message.
#[tonic::async_trait]
pub trait TaskExecutionIngress: Send + Sync {
    /// Test-only rendezvous under the actual Worker registry mutex.
    #[cfg(debug_assertions)]
    fn with_registry_lock_for_test(
        &self,
        callback: &mut dyn FnMut() -> Result<(), tonic::Status>,
    ) -> Result<(), tonic::Status>;

    /// Applies a per-backend batch, one receipt per item in request order.
    ///
    /// A batch gives its items no atomicity and no shared verdict: a partial
    /// failure leaves every other item exactly as its own receipt reports.
    fn apply_task_operations(
        &self,
        request: proto::ApplyTaskOperationsRequest,
    ) -> Result<proto::ApplyTaskOperationsResponse, tonic::Status>;

    fn apply_task_operations_at(
        &self,
        request: proto::ApplyTaskOperationsRequest,
        timing: TaskIngressTiming,
    ) -> Result<proto::ApplyTaskOperationsResponse, tonic::Status>;

    /// Applies the closed, bounded control operation schema using the same
    /// per-item role owner and receipt semantics as the ordinary method.
    fn apply_task_control_operations(
        &self,
        request: proto::ApplyTaskControlOperationsRequest,
    ) -> Result<proto::ApplyTaskOperationsResponse, tonic::Status>;

    fn apply_task_control_operations_at(
        &self,
        request: proto::ApplyTaskControlOperationsRequest,
        timing: TaskIngressTiming,
    ) -> Result<proto::ApplyTaskOperationsResponse, tonic::Status>;

    /// Opens one logical subscription, resuming from the given per-task
    /// cursors.
    ///
    /// Observation only: it creates nothing, freezes no task set, and owns no
    /// admission, edge-open, terminal, or cancel authority.
    fn subscribe_task_status(
        &self,
        request: proto::SubscribeTaskStatusRequest,
    ) -> Result<TaskStatusEventStream, tonic::Status>;

    fn fetch_task_dynamic_filters(
        &self,
        request: proto::FetchTaskDynamicFiltersRequest,
    ) -> Result<proto::FetchTaskDynamicFiltersResponse, tonic::Status>;

    fn get_final_task_info(
        &self,
        request: proto::GetFinalTaskInfoRequest,
    ) -> Result<proto::GetFinalTaskInfoResponse, tonic::Status>;

    /// Polls the root task's result stream.
    ///
    /// Unlike the fragment-instance-addressed form it replaces, the request
    /// names an exact task, so it is fenced against a replaced backend
    /// process before it reaches a result buffer.
    async fn fetch_task_result(
        &self,
        request: proto::FetchTaskResultRequest,
    ) -> Result<proto::FetchResultResponse, tonic::Status>;

    async fn fetch_task_result_with_ownership(
        &self,
        request: proto::FetchTaskResultRequest,
        ownership: Option<Arc<NativeIngressOwnership>>,
    ) -> Result<proto::FetchResultResponse, tonic::Status> {
        let _ownership = ownership;
        self.fetch_task_result(request).await
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;
    use std::sync::Arc;
    use std::time::Duration;

    use prost::Message;
    use tokio_stream::StreamExt;

    use super::{
        Bytes, TaskResultRead, TaskResultReadError, TaskResultReadRequest, TaskResultReader,
        TaskStatusSubscriptionReader, encode_covered_frame, fetch_task_result, proto,
        subscribe_task_status, task_result_response,
    };
    use novarocks_execution_contract::task_execution::identity::{QueryContextRef, TaskIdentity};
    use novarocks_execution_contract::task_execution::operation::QuiesceQueryContextReceipt;
    use novarocks_execution_contract::task_execution::status::{
        TaskStatus, TaskStatusCursor, TaskStatusVersion,
    };
    use novarocks_execution_contract::task_execution::transition::QueryContextState;
    use novarocks_proto_codec::FieldPath;
    use novarocks_task_codec::operation::{
        CoveredStatusStreamFact, DecodedCoveredSubscription, MAX_FETCH_TASK_RESULT_PAYLOAD_BYTES,
        NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES, decode_covered_status_event,
        encode_covered_subscribe_task_status,
    };
    use novarocks_types::{
        AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    use novarocks_worker::{CoveredObservationFact, CoveredObservationFrame, TaskStatusSource};
    use proto::fetch_result_response::Status as FetchStatus;

    #[test]
    fn maximum_legal_root_result_fits_the_actual_grpc_response_envelope() {
        let payload_bytes = usize::try_from(MAX_FETCH_TASK_RESULT_PAYLOAD_BYTES)
            .expect("the Native result ceiling fits usize");
        let response = task_result_response(
            FetchStatus::Ready,
            String::new(),
            i64::MAX,
            true,
            Bytes::from(vec![0_u8; payload_bytes]),
        );
        let encoded_bytes = response.encoded_len();
        assert!(
            encoded_bytes <= NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES,
            "the maximum legal payload produces a {encoded_bytes}-byte response above the {}-byte decode ceiling",
            NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES
        );
    }

    #[test]
    fn root_result_reader_output_is_encoded_without_a_backend_type() {
        struct ReadyReader;

        #[tonic::async_trait]
        impl TaskResultReader for ReadyReader {
            async fn read_task_result(
                &self,
                request: TaskResultReadRequest,
                _ownership: Option<std::sync::Arc<crate::native_ingress::NativeIngressOwnership>>,
            ) -> Result<TaskResultRead, TaskResultReadError> {
                assert_eq!(request.max_wait(), std::time::Duration::from_millis(17));
                assert_eq!(request.acknowledged_packet_sequence(), Some(3));
                assert_eq!(request.max_result_bytes().get(), 4096);
                Ok(TaskResultRead::Ready {
                    packet_sequence: 4,
                    end_of_stream: false,
                    payload: Bytes::from_static(b"result"),
                })
            }
        }

        let identity = TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(7, 9), AttemptId::new(2).expect("attempt id"))
                .expect("execution id"),
            StageId::new(3).expect("stage id"),
            TaskId::new(4).expect("task id"),
            BackendProcessId::new_v7(),
        );
        let response = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("task result adapter runtime")
            .block_on(fetch_task_result(
                &ReadyReader,
                proto::FetchTaskResultRequest {
                    root_task: Some(novarocks_task_codec::identity::encode_task_identity(
                        identity,
                    )),
                    max_wait_millis: 17,
                    acknowledged_packet_sequence: Some(3),
                    max_result_bytes: 4096,
                },
            ))
            .expect("a valid role result is encoded");
        assert_eq!(response.status, FetchStatus::Ready as i32);
        assert_eq!(response.packet_seq, 4);
        assert!(!response.eos);
        assert_eq!(response.result_arrow_ipc, Bytes::from_static(b"result"));
    }

    struct CoveredReader {
        context: QueryContextRef,
        source: Arc<TaskStatusSource>,
    }

    impl TaskStatusSubscriptionReader for CoveredReader {
        fn task_status_source(&self, context: QueryContextRef) -> Option<Arc<TaskStatusSource>> {
            (context == self.context).then(|| Arc::clone(&self.source))
        }
    }

    fn covered_fixture() -> (CoveredReader, TaskIdentity, TaskIdentity) {
        let execution = QueryExecutionId::new(
            QueryId::new(31, 32),
            AttemptId::new(1).expect("nonzero attempt"),
        )
        .expect("nonzero query");
        let backend = BackendProcessId::new_v7();
        let context = QueryContextRef::new(execution, FrontendProcessId::new_v7(), backend);
        let identity = |task| {
            TaskIdentity::new(
                execution,
                StageId::new(1).expect("nonzero stage"),
                TaskId::new(task).expect("nonzero task"),
                backend,
            )
        };
        (
            CoveredReader {
                context,
                source: Arc::new(TaskStatusSource::new()),
            },
            identity(1),
            identity(2),
        )
    }

    fn covered_request(
        context: QueryContextRef,
        required: Vec<TaskIdentity>,
    ) -> proto::SubscribeTaskStatusRequest {
        encode_covered_subscribe_task_status(&DecodedCoveredSubscription {
            context,
            generation: NonZeroU64::new(7).expect("nonzero generation"),
            status_cursors: Vec::new(),
            task_convergence_cursors: Vec::new(),
            context_cursor: None,
            quiesce_cursor: None,
            required_identities: required,
        })
        .expect("exact covered request")
    }

    async fn next_covered(
        stream: &mut super::TaskStatusEventStream,
    ) -> novarocks_task_codec::operation::CoveredStatusStreamEvent {
        let wire = tokio::time::timeout(Duration::from_secs(3), stream.next())
            .await
            .expect("covered stream makes progress")
            .expect("covered stream remains open")
            .expect("covered frame is valid");
        decode_covered_status_event(&wire, FieldPath::root("covered_stream"))
            .expect("Native emits a valid covered frame")
    }

    #[tokio::test]
    async fn covered_stream_has_finite_catch_up_live_revision_and_honest_bookmark() {
        let (reader, a, b) = covered_fixture();
        reader.source.publish(TaskStatus::created(a));
        let mut stream =
            subscribe_task_status(&reader, covered_request(reader.context, vec![a, b]))
                .expect("covered subscription opens");
        let mut saw_status = false;
        let mut saw_unknown = false;
        for _ in 0..3 {
            let frame = next_covered(&mut stream).await;
            match frame.fact {
                CoveredStatusStreamFact::Status(status) => {
                    assert_eq!(status.identity(), a);
                    assert_eq!(frame.source_revision, None);
                    saw_status = true;
                }
                CoveredStatusStreamFact::Unknown(identity) => {
                    assert_eq!(identity, b);
                    assert_eq!(frame.source_revision, None);
                    saw_unknown = true;
                }
                CoveredStatusStreamFact::CatchUpComplete(complete) => {
                    assert_eq!(complete.generation, 7);
                    assert_eq!(complete.initial_cut, 1);
                    assert!(saw_status && saw_unknown);
                }
                other => panic!("unexpected catch-up fact: {other:?}"),
            }
        }
        reader.source.publish(TaskStatus::created(b));
        let mut live = None;
        for _ in 0..4 {
            let frame = next_covered(&mut stream).await;
            if matches!(&frame.fact, CoveredStatusStreamFact::Status(status) if status.identity() == b)
            {
                live = Some(frame);
                break;
            }
        }
        let live = live.expect("new task enters the same stream");
        assert_eq!(live.source_revision, Some(2));
        let mut bookmark = None;
        for _ in 0..4 {
            let frame = next_covered(&mut stream).await;
            if let CoveredStatusStreamFact::Bookmark(value) = frame.fact {
                bookmark = Some(value);
                break;
            }
        }
        let bookmark = bookmark.expect("quiet stream emits a periodic bookmark");
        assert_eq!(bookmark.generation, 7);
        assert!(bookmark.sequence > 0);
        assert_eq!(bookmark.covered_prefix, 2);
        assert_eq!(bookmark.source_cut, 2);
    }

    #[tokio::test]
    async fn covered_subscription_rejects_future_status_and_quiesce_cursors() {
        let (reader, a, _) = covered_fixture();
        reader.source.publish(TaskStatus::created(a));
        let mut status_future = covered_request(reader.context, vec![a]);
        status_future
            .cursors
            .push(novarocks_task_codec::status::encode_task_status_cursor(
                TaskStatusCursor::at(a, TaskStatusVersion::new(2).unwrap()),
            ));
        let error = match subscribe_task_status(&reader, status_future) {
            Ok(_) => panic!("future status cursor must fail closed"),
            Err(error) => error,
        };
        assert_eq!(error.code(), tonic::Code::InvalidArgument);

        let mut quiesce_future = covered_request(reader.context, vec![a]);
        quiesce_future.quiesce_cursor = Some(proto::QuiesceQueryContextCursor {
            query_context: Some(novarocks_task_codec::identity::encode_query_context_ref(
                reader.context,
            )),
            fence_version: 1,
        });
        let error = match subscribe_task_status(&reader, quiesce_future) {
            Ok(_) => panic!("future quiesce cursor must fail closed"),
            Err(error) => error,
        };
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn covered_rpc_rejects_legacy_mode_future_convergence_and_foreign_context() {
        let (reader, task, _) = covered_fixture();
        let mut missing = covered_request(reader.context, vec![task]);
        missing.generation = 0;
        let legacy = proto::SubscribeTaskStatusRequest {
            query_context: missing.query_context.clone(),
            ..Default::default()
        };
        let mut future_task = covered_request(reader.context, vec![task]);
        future_task.task_convergence_cursors = vec![proto::TaskConvergenceCursor {
            identity: Some(novarocks_task_codec::identity::encode_task_identity(task)),
            current_version: 1,
        }];
        let mut future_context = covered_request(reader.context, vec![task]);
        future_context.context_convergence_cursor = Some(proto::QueryContextConvergenceCursor {
            query_context: missing.query_context.clone(),
            current_version: 1,
        });
        let mut foreign_context = covered_request(reader.context, vec![task]);
        let foreign = QueryContextRef::new(
            reader.context.query_execution_id(),
            FrontendProcessId::new_v7(),
            reader.context.backend_process_id(),
        );
        foreign_context.context_convergence_cursor = Some(proto::QueryContextConvergenceCursor {
            query_context: Some(novarocks_task_codec::identity::encode_query_context_ref(
                foreign,
            )),
            current_version: 0,
        });
        for (name, request) in [
            ("missing generation", missing),
            ("legacy cursor-only", legacy),
            ("future Task convergence", future_task),
            ("future Context convergence", future_context),
            ("foreign Context", foreign_context),
        ] {
            match subscribe_task_status(&reader, request) {
                Ok(_) => panic!("{name} must not start a subscription"),
                Err(error) => assert_eq!(
                    error.code(),
                    tonic::Code::InvalidArgument,
                    "{name}: {error}"
                ),
            }
        }
    }

    #[test]
    fn covered_frame_respects_native_bound_and_rejects_oversized_membership() {
        let (reader, a, _) = covered_fixture();
        let members: Vec<_> = (1..=4096)
            .map(|task| {
                TaskIdentity::new(
                    a.query_execution_id(),
                    a.stage_id(),
                    TaskId::new(task).unwrap(),
                    a.backend_process_id(),
                )
            })
            .collect();
        let receipt = QuiesceQueryContextReceipt::new(
            reader.context,
            1,
            members.clone(),
            QueryContextState::Quiescing,
        );
        let wire = encode_covered_frame(
            CoveredObservationFrame::CatchUp(CoveredObservationFact::Quiesce(receipt)),
            7,
        )
        .expect("legal maximum membership encodes");
        assert!(wire.encoded_len() <= NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES);
        let mut too_many = members;
        too_many.push(TaskIdentity::new(
            a.query_execution_id(),
            a.stage_id(),
            TaskId::new(4097).unwrap(),
            a.backend_process_id(),
        ));
        let rejected = encode_covered_frame(
            CoveredObservationFrame::CatchUp(CoveredObservationFact::Quiesce(
                QuiesceQueryContextReceipt::new(
                    reader.context,
                    1,
                    too_many,
                    QueryContextState::Quiescing,
                ),
            )),
            7,
        );
        assert!(
            rejected.is_err(),
            "an oversized fact must not be silently skipped"
        );
    }
}
