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
//! Ordinary and small-control mutation methods, one status subscription, and
//! two typed observation reads. The bounded Root data plane has its own reader.

use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use novarocks_execution_contract::task_execution::identity::{
    AdmissionTicketId, QueryContextRef, TaskIdentity, TaskOperationId,
};
use novarocks_execution_contract::task_execution::operation::OperationKind;
use novarocks_execution_contract::task_execution::operation::OperationOutcome;
use novarocks_execution_contract::task_execution::status::{SafeDetail, TaskFailureCategory};
use novarocks_proto_codec::FieldPath;
use novarocks_proto_models::novarocks as proto;
use novarocks_task_codec::TransportBudget;
use novarocks_task_codec::identity::{decode_admission_ticket_id, decode_query_context_ref};
use novarocks_task_codec::operation::{
    DecodedOperation, decode_control_operation_batch, decode_envelope,
    decode_ordinary_operation_batch, decode_ordinary_operation_batch_with_skip, encode_receipt,
};
use novarocks_task_codec::{
    domain::encode_task_dynamic_filter_domain, identity::encode_task_identity,
};
use prost::Message;
use tokio_stream::Stream;

use crate::native_ingress::NativeIngressOwnership;
use crate::static_package_admission::StaticPackageAdmission;
use novarocks_worker::{
    HostRejection, NodeAdmissionTicketObservation, OperationReceipt, TaskDynamicFilterRead,
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
pub type TaskOperationReceiptAck = proto::task_operation_verdict::Ack;

/// Role-local authority that linearly applies one already decoded task
/// operation. It never receives a raw Native request.
#[tonic::async_trait]
pub trait TaskOperationBatchApplier: Send + Sync {
    /// Only owner-free codec recorders may accept the component helpers
    /// without a real Native permit. Production owners require that permit
    /// before any ticket preflight or operation handoff can occur.
    fn validate_ingress_ownership(
        &self,
        ownership: Option<&Arc<NativeIngressOwnership>>,
    ) -> Result<(), tonic::Status>;

    /// Applies one domain operation and returns its role-owned receipt.
    ///
    /// The operation is handed over by value because a create carries a
    /// short-lived creation input that is moved, never cloned, to whichever
    /// owner wins that identity's creation, and is otherwise dropped unread.
    /// `deadline` is the original ingress deadline, never a fresh wait granted
    /// when this future eventually gets polled.
    async fn apply_task_operation(
        &self,
        operation: DecodedOperation,
        deadline: Instant,
        ownership: Option<Arc<NativeIngressOwnership>>,
    ) -> Result<proto::TaskOperationReceipt, tonic::Status>;

    /// The Worker owner's context-bound, read-only ticket observation.
    async fn observe_admission_ticket(
        &self,
        operation: TaskOperationId,
        ticket_id: AdmissionTicketId,
        context: QueryContextRef,
        deadline: Instant,
    ) -> Result<NodeAdmissionTicketObservation, tonic::Status>;

    /// A narrow Worker-authored refusal only when an absent context would
    /// reach expiry redemption next. All ambiguous states return None.
    async fn preflight_expired_establish_ticket(
        &self,
        operation_id: TaskOperationId,
        ticket_id: AdmissionTicketId,
        context: QueryContextRef,
        native_compatibility_id: Option<&proto::NativeCompatibilityId>,
        deadline: Instant,
    ) -> Result<Option<proto::TaskOperationReceipt>, tonic::Status>;

    /// The compiled-package resource gate this process composed. A plan-tree
    /// process composes none, and its creates pass with their carrier unread.
    fn static_package_admission(&self) -> Option<&StaticPackageAdmission> {
        None
    }
}

/// Decodes one bounded operation batch and delegates each item in request
/// order to its role-local owner.
///
/// There is no longer a confidentiality gate in front of this. It existed
/// because vended material crossed here and was legal only on an encrypted
/// transport; material no longer crosses at all, so the gate had nothing left
/// to guard (CAD-1 D1).
pub async fn apply_task_operations(
    applier: &dyn TaskOperationBatchApplier,
    request: proto::ApplyTaskOperationsRequest,
) -> Result<proto::ApplyTaskOperationsResponse, tonic::Status> {
    apply_task_operations_at(applier, request, TaskIngressTiming::starting_now()).await
}

pub async fn apply_task_operations_at(
    applier: &dyn TaskOperationBatchApplier,
    request: proto::ApplyTaskOperationsRequest,
    timing: TaskIngressTiming,
) -> Result<proto::ApplyTaskOperationsResponse, tonic::Status> {
    apply_task_operations_with_ownership_at(applier, request, timing, None).await
}

pub(crate) async fn apply_task_operations_with_ownership_at(
    applier: &dyn TaskOperationBatchApplier,
    request: proto::ApplyTaskOperationsRequest,
    timing: TaskIngressTiming,
    ownership: Option<Arc<NativeIngressOwnership>>,
) -> Result<proto::ApplyTaskOperationsResponse, tonic::Status> {
    applier.validate_ingress_ownership(ownership.as_ref())?;
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
        ticket_preflight.push(
            match applier
                .observe_admission_ticket(
                    envelopes[index].operation_id(),
                    ticket_id,
                    context,
                    timing.operation_deadline(envelopes[index].max_wait().get()),
                )
                .await?
            {
                NodeAdmissionTicketObservation::Issued { remaining } => {
                    Some((ticket_id, context, Instant::now() + remaining))
                }
                NodeAdmissionTicketObservation::Expired
                    if index == 0
                        && Instant::now()
                            < timing.operation_deadline(envelopes[index].max_wait().get()) =>
                {
                    // The Worker supplies the only early verdict. The codec still
                    // validates every other item before any item is applied.
                    if let Some(receipt) = applier
                        .preflight_expired_establish_ticket(
                            envelopes[index].operation_id(),
                            ticket_id,
                            context,
                            establish.native_compatibility_id.as_ref(),
                            timing.operation_deadline(envelopes[index].max_wait().get()),
                        )
                        .await?
                    {
                        early_receipts[index] = Some(receipt);
                    }
                    None
                }
                _ => None,
            },
        );
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
    // A compiled-package process bounds every create's package here, before
    // any owner classifies its identity, so a replay is gated like a winner.
    // The batch decoder above is the only successful decode on this path:
    // the earlier one only renders a refusal.
    if let Some(admission) = applier.static_package_admission() {
        admission
            .admit_ordinary_batch(&operations, FieldPath::root("apply_task_operations"))
            .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
    }
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
        ownership,
    )
    .await
}

/// Validates the closed control schema and awaits each original owner in order.
/// Its Native ingress class remains independently admitted.
pub async fn apply_task_control_operations(
    applier: &dyn TaskOperationBatchApplier,
    request: proto::ApplyTaskControlOperationsRequest,
) -> Result<proto::ApplyTaskOperationsResponse, tonic::Status> {
    apply_task_control_operations_at(applier, request, TaskIngressTiming::starting_now()).await
}

pub async fn apply_task_control_operations_at(
    applier: &dyn TaskOperationBatchApplier,
    request: proto::ApplyTaskControlOperationsRequest,
    timing: TaskIngressTiming,
) -> Result<proto::ApplyTaskOperationsResponse, tonic::Status> {
    apply_task_control_operations_with_ownership_at(applier, request, timing, None).await
}

pub(crate) async fn apply_task_control_operations_with_ownership_at(
    applier: &dyn TaskOperationBatchApplier,
    request: proto::ApplyTaskControlOperationsRequest,
    timing: TaskIngressTiming,
    ownership: Option<Arc<NativeIngressOwnership>>,
) -> Result<proto::ApplyTaskOperationsResponse, tonic::Status> {
    applier.validate_ingress_ownership(ownership.as_ref())?;
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
    apply_decoded_operations(
        applier,
        operations,
        &early_receipts,
        &waits,
        &[],
        timing,
        ownership,
    )
    .await
}

/// Applies each decoded item in request order.
///
/// The batch owns its decoded items, and each is handed to the applier by
/// value. An item that is refused here, before the applier, drops whatever it
/// carried unread -- including a create's input -- exactly as an item the
/// Worker answers from an existing identity does.
async fn apply_decoded_operations(
    applier: &dyn TaskOperationBatchApplier,
    operations: Vec<Option<DecodedOperation>>,
    early_receipts: &[Option<proto::TaskOperationReceipt>],
    waits: &[Duration],
    ticket_preflight: &[Option<(AdmissionTicketId, QueryContextRef, Instant)>],
    timing: TaskIngressTiming,
    ownership: Option<Arc<NativeIngressOwnership>>,
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
                novarocks_execution_contract::operation::OperationResult::Verdict {
                    outcome: OperationOutcome::OperationTimedOut,
                    detail: Some(SafeDetail::truncating(
                        "operation exceeded its original ingress deadline",
                    )),
                    acknowledgement: None,
                },
            ));
            continue;
        }
        let mut deadline = operation_deadline;
        if let Some((ticket_id, context, first_deadline)) =
            ticket_preflight.get(index).copied().flatten()
            && let NodeAdmissionTicketObservation::Issued {
                remaining: ticket_remaining,
            } = applier
                .observe_admission_ticket(
                    operation.envelope().operation_id(),
                    ticket_id,
                    context,
                    operation_deadline,
                )
                .await?
        {
            // The Worker owns the current state. Once redeemed, the original
            // issuance deadline no longer limits exact Establish replay.
            deadline = deadline.min(now + ticket_remaining).min(first_deadline);
        }
        // Every item shares the batch's actual permit. The wrapper handed
        // to a rich Worker operation cannot mint another admission position.
        receipts.push(
            applier
                .apply_task_operation(operation, deadline, ownership.clone())
                .await?,
        );
    }
    Ok(proto::ApplyTaskOperationsResponse { receipts })
}

/// Maps a role-owner rejection onto the status code of a wire read that has no
/// in-band outcome field.
pub fn host_rejection_status(rejection: HostRejection) -> tonic::Status {
    let detail = rejection.detail().as_str().to_owned();
    match rejection.category() {
        TaskFailureCategory::Protocol => tonic::Status::invalid_argument(detail),
        TaskFailureCategory::ResourceExhausted => tonic::Status::resource_exhausted(detail),
        TaskFailureCategory::Unsupported => tonic::Status::unimplemented(detail),
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
                    HostRejection::from_domain(TaskFailureCategory::Internal, error)
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

/// Encodes one role-owner receipt without inventing an acknowledgement whose
/// state the Native wire cannot represent.
pub fn encode_operation_receipt<T>(
    receipt: &OperationReceipt<T>,
    encode_ack: impl FnOnce(&T) -> Option<TaskOperationReceiptAck>,
) -> Result<proto::TaskOperationReceipt, tonic::Status> {
    use novarocks_execution_contract::operation::OperationResult;
    let result = match receipt.result() {
        OperationResult::Verdict {
            outcome,
            detail,
            acknowledgement,
        } => {
            let acknowledgement = match acknowledgement {
                Some(body) => Some(encode_ack(body).ok_or_else(|| {
                    tonic::Status::internal(
                        "operation acknowledgement reports a state with no wire representation",
                    )
                })?),
                None => None,
            };
            OperationResult::Verdict {
                outcome: *outcome,
                detail: detail.clone(),
                acknowledgement,
            }
        }
        OperationResult::DomainFailure { outcome, failure } => OperationResult::DomainFailure {
            outcome: *outcome,
            failure: failure.clone(),
        },
    };
    Ok(encode_receipt(receipt.operation_id(), result))
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
    #[cfg(novarocks_dev_controls)]
    fn with_registry_lock_for_test(
        &self,
        callback: &mut dyn FnMut() -> Result<(), tonic::Status>,
    ) -> Result<(), tonic::Status>;

    /// Applies a per-backend batch, one receipt per item in request order.
    ///
    /// A batch gives its items no atomicity and no shared verdict: a partial
    /// failure leaves every other item exactly as its own receipt reports.
    async fn apply_task_operations(
        &self,
        request: proto::ApplyTaskOperationsRequest,
    ) -> Result<proto::ApplyTaskOperationsResponse, tonic::Status>;

    async fn apply_task_operations_at(
        &self,
        request: proto::ApplyTaskOperationsRequest,
        timing: TaskIngressTiming,
        ownership: Arc<NativeIngressOwnership>,
    ) -> Result<proto::ApplyTaskOperationsResponse, tonic::Status>;

    /// Applies the closed, bounded control operation schema using the same
    /// per-item role owner and receipt semantics as the ordinary method.
    async fn apply_task_control_operations(
        &self,
        request: proto::ApplyTaskControlOperationsRequest,
    ) -> Result<proto::ApplyTaskOperationsResponse, tonic::Status>;

    async fn apply_task_control_operations_at(
        &self,
        request: proto::ApplyTaskControlOperationsRequest,
        timing: TaskIngressTiming,
        ownership: Arc<NativeIngressOwnership>,
    ) -> Result<proto::ApplyTaskOperationsResponse, tonic::Status>;

    /// Opens one logical subscription, resuming from the given per-task
    /// cursors.
    ///
    /// Observation only: it creates nothing, freezes no task set, and owns no
    /// admission, edge-open, terminal, or cancel authority.
    fn subscribe_task_status(
        &self,
        request: proto::SubscribeTaskStatusRequest,
        deadline: Instant,
        ownership: Arc<NativeIngressOwnership>,
    ) -> Result<TaskStatusEventStream, tonic::Status>;

    async fn fetch_task_dynamic_filters(
        &self,
        request: proto::FetchTaskDynamicFiltersRequest,
        deadline: Instant,
    ) -> Result<proto::FetchTaskDynamicFiltersResponse, tonic::Status>;

    async fn get_final_task_info(
        &self,
        request: proto::GetFinalTaskInfoRequest,
        deadline: Instant,
    ) -> Result<proto::GetFinalTaskInfoResponse, tonic::Status>;
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;
    use std::sync::Arc;

    use super::proto;
    use crate::worker_node_projection::{encode_frame, observation_projection};
    use novarocks_execution_contract::task_execution::identity::{QueryContextRef, TaskIdentity};
    use novarocks_execution_contract::task_execution::operation::QuiesceQueryContextReceipt;
    use novarocks_execution_contract::task_execution::status::{
        TaskStatus, TaskStatusCursor, TaskStatusVersion,
    };
    use novarocks_execution_contract::task_execution::transition::QueryContextState;
    use novarocks_proto_codec::FieldPath;
    use novarocks_task_codec::operation::{
        CoveredObservationBookmark, CoveredStatusStreamEvent, CoveredStatusStreamFact,
        DecodedCoveredSubscription, MAX_NATIVE_ROOT_RESULT_PAYLOAD_BYTES,
        NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES, decode_covered_status_event,
        decode_covered_subscribe_task_status, encode_covered_status_event,
        encode_covered_subscribe_task_status,
    };
    use novarocks_types::{
        AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    use novarocks_worker_protocol::observation::{
        CoveredObservationFact, CoveredObservationFrame, CoveredSubscriptionRequest,
        ObservationSource, QuiesceObservationCursor, SubscriptionId,
    };
    use prost::Message;

    #[test]
    fn native_root_payload_ceiling_fits_the_actual_grpc_response_envelope() {
        let payload_bytes = usize::try_from(MAX_NATIVE_ROOT_RESULT_PAYLOAD_BYTES)
            .expect("the Native result ceiling fits usize");
        let response = proto::FetchRootResultResponse {
            root_task: Some(novarocks_task_codec::identity::encode_task_identity(
                TaskIdentity::new(
                    QueryExecutionId::new(
                        QueryId::new(i64::MIN, i64::MIN),
                        AttemptId::new(u64::MAX).unwrap(),
                    )
                    .unwrap(),
                    StageId::new(u32::MAX).unwrap(),
                    TaskId::new(u32::MAX).unwrap(),
                    BackendProcessId::new_v7(),
                ),
            )),
            profile_id: novarocks_result_contract::RootProfileId::V1.get(),
            output_kind: Some(novarocks_proto_codec::root_result::encode_kind(
                novarocks_result_contract::RootOutputKind::ClientRows,
            )),
            result: Some(proto::fetch_root_result_response::Result::Reply(
                proto::RootReadReceipt {
                    accepted_consumed_sequence: u64::MAX,
                    outcome: Some(proto::root_read_receipt::Outcome::Data(
                        novarocks_proto_models::result::RootData {
                            sequence: u64::MAX,
                            body: bytes::Bytes::from(vec![0_u8; payload_bytes]),
                            end_after_data: Some(novarocks_proto_models::result::RootEnd {
                                sequence: u64::MAX,
                                output_rows: u64::MAX,
                            }),
                        },
                    )),
                },
            )),
        };
        let encoded_bytes = response.encoded_len();
        assert!(
            encoded_bytes <= NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES,
            "the maximum legal payload produces a {encoded_bytes}-byte response above the {}-byte decode ceiling",
            NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES
        );
    }

    struct CoveredFixture {
        context: QueryContextRef,
        source: ObservationSource,
        next_subscription: u64,
    }

    impl CoveredFixture {
        fn publish(&mut self, status: TaskStatus) {
            let fact = CoveredObservationFact::Status(status);
            let bytes = (observation_projection().fact_bytes)(&fact).expect("bounded neutral fact");
            self.source.publish(fact, bytes).expect("valid publication");
            while !self.source.continue_publication(1).complete {}
        }

        // The wire oracle uses the production decoder, pure source and actual
        // Native projection. It creates no second production subscription port.
        fn subscribe(
            &mut self,
            request: proto::SubscribeTaskStatusRequest,
        ) -> Result<SubscriptionId, tonic::Status> {
            let decoded = decode_covered_subscribe_task_status(
                &request,
                FieldPath::root("subscribe_task_status"),
            )
            .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
            let id = SubscriptionId(self.next_subscription);
            self.next_subscription += 1;
            let position = CoveredSubscriptionRequest {
                status: decoded.status_cursors,
                convergence: decoded.task_convergence_cursors,
                context: decoded.context_cursor,
                quiesce: decoded
                    .quiesce_cursor
                    .map(|cursor| QuiesceObservationCursor {
                        context: cursor.context,
                        fence_version: cursor.fence_version,
                    }),
                required: decoded.required_identities,
            };
            self.source
                .begin_covered_subscription(id, decoded.generation.get(), position)
                .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
            loop {
                let progress = self
                    .source
                    .continue_subscribe(id, 1)
                    .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
                assert!(progress.work <= 1);
                if progress.complete {
                    return Ok(id);
                }
            }
        }

        fn next(&mut self, id: SubscriptionId) -> CoveredStatusStreamEvent {
            let selected = self
                .source
                .select_next(id)
                .expect("source selection")
                .expect("owed frame");
            let wire = encode_frame(&selected.frame, 7).expect("actual Native projection");
            assert!(wire.encoded_len() <= NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES);
            let event = decode_covered_status_event(&wire, FieldPath::root("covered_stream"))
                .expect("Native emits a valid covered frame");
            // Selection alone does not advance the source's delivered prefix.
            self.source
                .note_delivered(id, selected.delivery_id)
                .expect("ordered delivery");
            event
        }
    }

    fn covered_fixture() -> (CoveredFixture, TaskIdentity, TaskIdentity) {
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
        let projection = observation_projection();
        (
            CoveredFixture {
                context,
                source: ObservationSource::new(
                    context,
                    4096,
                    8,
                    projection.max_frame_bytes.get(),
                    projection.max_initial_bytes.get(),
                ),
                next_subscription: 1,
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

    #[test]
    fn covered_stream_has_finite_catch_up_live_revision_and_honest_bookmark() {
        let (mut fixture, a, b) = covered_fixture();
        fixture.publish(TaskStatus::created(a));
        let id = fixture
            .subscribe(covered_request(fixture.context, vec![a, b]))
            .expect("covered subscription opens");
        let mut saw_status = false;
        let mut saw_unknown = false;
        for _ in 0..3 {
            let frame = fixture.next(id);
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
        fixture.publish(TaskStatus::created(b));
        let live = fixture.next(id);
        assert!(
            matches!(&live.fact, CoveredStatusStreamFact::Status(status) if status.identity() == b)
        );
        assert_eq!(live.source_revision, Some(2));
        assert!(
            fixture
                .source
                .select_next(id)
                .expect("quiet source")
                .is_none()
        );
        let bookmark = fixture
            .source
            .bookmark(id)
            .expect("same-source quiet bookmark");
        let wire = encode_covered_status_event(&CoveredStatusStreamEvent {
            fact: CoveredStatusStreamFact::Bookmark(CoveredObservationBookmark {
                generation: bookmark.generation,
                sequence: bookmark.sequence,
                covered_prefix: bookmark.covered_prefix,
                source_cut: bookmark.source_cut,
            }),
            source_revision: None,
        })
        .expect("Native bookmark projection");
        let frame = decode_covered_status_event(&wire, FieldPath::root("covered_stream"))
            .expect("valid bookmark wire");
        let CoveredStatusStreamFact::Bookmark(bookmark) = frame.fact else {
            panic!("expected same-source bookmark");
        };
        assert_eq!(bookmark.generation, 7);
        assert!(bookmark.sequence > 0);
        assert_eq!(bookmark.covered_prefix, 2);
        assert_eq!(bookmark.source_cut, 2);
    }

    #[test]
    fn covered_subscription_rejects_future_status_and_quiesce_cursors() {
        let (mut fixture, a, _) = covered_fixture();
        fixture.publish(TaskStatus::created(a));
        let mut status_future = covered_request(fixture.context, vec![a]);
        status_future
            .cursors
            .push(novarocks_task_codec::status::encode_task_status_cursor(
                TaskStatusCursor::at(a, TaskStatusVersion::new(2).unwrap()),
            ));
        let error = fixture
            .subscribe(status_future)
            .expect_err("future status cursor must fail closed");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        let mut quiesce_future = covered_request(fixture.context, vec![a]);
        quiesce_future.quiesce_cursor = Some(proto::QuiesceQueryContextCursor {
            query_context: Some(novarocks_task_codec::identity::encode_query_context_ref(
                fixture.context,
            )),
            fence_version: 1,
        });
        let error = fixture
            .subscribe(quiesce_future)
            .expect_err("future quiesce cursor must fail closed");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn covered_rpc_rejects_legacy_mode_future_convergence_and_foreign_context() {
        let (mut fixture, task, _) = covered_fixture();
        let mut missing = covered_request(fixture.context, vec![task]);
        missing.generation = 0;
        let legacy = proto::SubscribeTaskStatusRequest {
            query_context: missing.query_context.clone(),
            ..Default::default()
        };
        let mut future_task = covered_request(fixture.context, vec![task]);
        future_task.task_convergence_cursors = vec![proto::TaskConvergenceCursor {
            identity: Some(novarocks_task_codec::identity::encode_task_identity(task)),
            current_version: 1,
        }];
        let mut future_context = covered_request(fixture.context, vec![task]);
        future_context.context_convergence_cursor = Some(proto::QueryContextConvergenceCursor {
            query_context: missing.query_context.clone(),
            current_version: 1,
        });
        let mut foreign_context = covered_request(fixture.context, vec![task]);
        let foreign = QueryContextRef::new(
            fixture.context.query_execution_id(),
            FrontendProcessId::new_v7(),
            fixture.context.backend_process_id(),
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
            match fixture.subscribe(request) {
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
        let (fixture, a, _) = covered_fixture();
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
            fixture.context,
            1,
            members.clone(),
            QueryContextState::Quiescing,
        );
        let wire = encode_frame(
            &CoveredObservationFrame::CatchUp(CoveredObservationFact::Quiesce(Arc::new(receipt))),
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
        let rejected = encode_frame(
            &CoveredObservationFrame::CatchUp(CoveredObservationFact::Quiesce(Arc::new(
                QuiesceQueryContextReceipt::new(
                    fixture.context,
                    1,
                    too_many,
                    QueryContextState::Quiescing,
                ),
            ))),
            7,
        );
        assert!(
            rejected.is_err(),
            "an oversized fact must not be silently skipped"
        );
    }
}

#[cfg(test)]
mod tests_batch;
