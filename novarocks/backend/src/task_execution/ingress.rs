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

//! The task protocol owner behind its RPC boundary.
//!
//! This is the only adapter between the wire messages and
//! [`TaskExecutionRegistry`]. It decodes, dispatches to exactly one neutral
//! entry point, and encodes what comes back; it decides nothing. Every
//! classification a frontend can act on — accepted, idempotent, conflicting,
//! terminal, reclaimed — is already in the receipt the owner returned, so this
//! adapter never invents an outcome and never reads a diagnostic string.
//!
//! A batch is walked in request order and produces one receipt per item. The
//! items share no verdict: each is dispatched on its own, and a refusal is that
//! item's receipt rather than the batch's error. Only two things fail the whole
//! call — a batch that cannot be decoded at all, and a receipt whose category
//! has no wire representation, which is an internal invariant violation rather
//! than something a caller can cause.
//!
//! # The one read this boundary cannot serve
//!
//! `FetchTaskDynamicFiltersResponse` carries the filter domains themselves,
//! but a retained payload reaches this adapter as
//! [`TaskDynamicFilterRead`](super::host::TaskDynamicFilterRead), whose
//! content sits behind `CodecOwnedContent` and exposes only a fingerprint and
//! a size — no wire projection. So a read that really has a payload is refused
//! as an internal invariant violation instead of being answered with an empty
//! domain list under a nonzero version, which would claim that the version
//! carries nothing. Nothing in this process advertises a payload yet, so the
//! refusal is unreachable today and becomes loud the moment a producer is
//! wired.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use novarocks_execution_contract::task_execution::identity::TaskOperationId;
use novarocks_execution_contract::task_execution::operation::{
    OperationOutcome, TaskDomainReceipt, UpdateQueryContext,
};
use novarocks_execution_contract::task_execution::status::{SafeDetail, TaskFailureCategory};
use novarocks_proto_codec::FieldPath;
use novarocks_proto_models::novarocks as proto;
use novarocks_task_codec::TransportBudget;
use novarocks_task_codec::domain::{
    ConfidentialTransport, refuse_confidential_material_in_the_clear,
};
use novarocks_task_codec::operation::{
    DecodedOperation, DecodedUpdateQueryContext, decode_fetch_dynamic_filters,
    decode_get_final_task_info, decode_operation_batch, decode_subscribe_task_status,
    encode_abort_cause_field, encode_create_task_ack, encode_operation_outcome,
    encode_query_context_ack, encode_query_context_admission_ticket_ack, encode_receipt,
    encode_release_ack, encode_status_event, encode_task_gone_event, encode_update_task_ack,
};
use novarocks_task_codec::status::{encode_final_task_info, encode_task_status};
use novarocks_types::NativeCompatibilityId;
use tokio_stream::Stream;

use super::fault;
use super::host::HostRejection;
use super::observation::{TaskStatusEvent, TaskStatusSource};
use super::receipt::OperationReceipt;
use super::registry::TaskExecutionRegistry;
use super::shared_facts::encode_dynamic_filter_read;
use crate::rpc::task_execution::{TaskExecutionIngress, TaskStatusEventStream};

type ReceiptAck = proto::task_operation_receipt::Ack;

/// The wire adapter of one backend's task protocol owner.
pub(crate) struct RegistryTaskExecutionIngress {
    registry: Arc<TaskExecutionRegistry>,
    native_compatibility_id: NativeCompatibilityId,
    native_transport_confidentiality: ConfidentialTransport,
}

impl RegistryTaskExecutionIngress {
    pub(crate) fn new(
        registry: Arc<TaskExecutionRegistry>,
        native_compatibility_id: NativeCompatibilityId,
        native_transport_confidentiality: ConfidentialTransport,
    ) -> Arc<Self> {
        Arc::new(Self {
            registry,
            native_compatibility_id,
            native_transport_confidentiality,
        })
    }

    /// Dispatches one decoded item and encodes its receipt.
    ///
    /// Nothing here is shared with the item before or after it: the owner
    /// linearizes each operation on its own, so this is a plain per-item call.
    fn apply_one(
        &self,
        operation: &DecodedOperation,
    ) -> Result<proto::TaskOperationReceipt, tonic::Status> {
        let compatibility = match operation {
            DecodedOperation::AcquireQueryContextAdmissionTicket(request) => Some((
                request.envelope().operation_id(),
                request.native_compatibility_id(),
            )),
            DecodedOperation::UpdateQueryContext(DecodedUpdateQueryContext::Establish(request)) => {
                Some((
                    request.envelope().operation_id(),
                    request.native_compatibility_id(),
                ))
            }
            _ => None,
        };
        if let Some((operation_id, compatibility_id)) = compatibility
            && compatibility_id != self.native_compatibility_id
        {
            return Ok(encode_receipt(
                operation_id,
                OperationOutcome::CompatibilityMismatch,
                "native compatibility identity does not match this backend process",
                None,
            ));
        }
        match operation {
            DecodedOperation::AcquireQueryContextAdmissionTicket(request) => {
                let receipt = self
                    .registry
                    .acquire_query_context_admission_ticket(*request);
                encode_item(&receipt, |ack| {
                    Some(ReceiptAck::QueryContextAdmissionTicket(
                        encode_query_context_admission_ticket_ack(*ack),
                    ))
                })
            }
            DecodedOperation::CreateTask(request) => {
                let identity = request.request().identity();
                let receipt = self.registry.create_task(request.request());
                // Claimed after the owner applied it: the task is admitted and
                // running, and only this answer is lost.
                fault::create_task_ack_dropped(identity, receipt.outcome())?;
                let mut encoded = encode_item(&receipt, |ack| {
                    encode_create_task_ack(ack).map(ReceiptAck::CreateTask)
                })?;
                // The two wire-value faults are claimed on the encoded answer,
                // because the value each one misstates exists only there: the
                // owner's receipt is a validated neutral value whose identity
                // and verdict cannot be made to disagree with each other.
                //
                // The identity forgery goes first. The conflict rewrite drops
                // the acknowledgement body, as a genuine rejection has none, so
                // the reverse order would leave the identity fault nothing to
                // forge and silently consume its arming.
                fault::create_task_receipt_foreign_task(identity, receipt.outcome(), &mut encoded)?;
                fault::create_task_conflict_after_apply(identity, receipt.outcome(), &mut encoded)?;
                Ok(encoded)
            }
            DecodedOperation::UpdateTask(request) => {
                let receipt = self.registry.update_task(request.request());
                // Read off the receipt, not the request: the condition is that
                // this backend durably accepted a delivery that both carried
                // splits and sealed its plan node, and the receipt's watermark
                // is where that fact lives. The request's payload would have to
                // be decoded again to learn the same thing.
                let terminal_nonempty = receipt.acknowledgement().is_some_and(|ack| {
                    ack.domains().iter().any(|domain| match domain {
                        TaskDomainReceipt::SplitAssignment { nodes, .. } => {
                            nodes.iter().any(|node| {
                                node.watermark().no_more_splits()
                                    && node.watermark().accepted_through().is_some()
                            })
                        }
                        _ => false,
                    })
                });
                fault::task_update_terminal_ack_dropped(
                    request.request().identity(),
                    receipt.outcome(),
                    terminal_nonempty,
                )?;
                encode_item(&receipt, |ack| {
                    encode_update_task_ack(ack).map(ReceiptAck::UpdateTask)
                })
            }
            DecodedOperation::UpdateQueryContext(request) => {
                let context = request.context();
                let Some(neutral) = request.as_neutral() else {
                    return Err(tonic::Status::internal(
                        "decoded query context command has no neutral projection",
                    ));
                };
                // Checked before the renewal is applied, not after: this
                // fault has to prevent the extension itself so the lease can
                // run out. Dropping the answer afterwards is the other fault,
                // and the frontend survives that one by resending.
                if matches!(&neutral, UpdateQueryContext::RenewLease(_))
                    && fault::lease_renewal_stopped(context)?
                {
                    return Err(tonic::Status::deadline_exceeded(
                        "runner-owned lease renewal refused so the lease expires",
                    ));
                }
                let receipt = self.registry.update_query_context(&neutral);
                match &neutral {
                    UpdateQueryContext::Establish(_) => {
                        // The rendezvous comes first: it holds an applied
                        // establish open so the harness can replace this exact
                        // process, and an answer dropped afterwards would
                        // belong to a process that no longer exists.
                        fault::restart_after_establish_context(context, receipt.outcome())?;
                        fault::establish_context_ack_dropped(context, receipt.outcome())?;
                    }
                    UpdateQueryContext::RenewLease(_) => {
                        fault::lease_renewal_ack_dropped(context, receipt.outcome())?;
                    }
                    UpdateQueryContext::AdvanceDomain(_) => {}
                }
                // Read after the operation: an establish that lost the latch
                // to an abort reports the cause that abort installed.
                let cause = self.registry.termination_cause(context);
                encode_item(&receipt, |ack| {
                    encode_query_context_ack(ack, cause).map(ReceiptAck::QueryContext)
                })
            }
            DecodedOperation::CancelTask(request) => {
                let receipt = self.registry.cancel_task(request);
                encode_item(&receipt, |ack| {
                    Some(ReceiptAck::CancelTask(encode_task_status(ack)))
                })
            }
            DecodedOperation::AbortQueryContext(request) => {
                let receipt = self.registry.abort_query_context(request);
                let cause = self.registry.termination_cause(request.context());
                encode_item(&receipt, |ack| {
                    encode_query_context_ack(ack, cause).map(ReceiptAck::QueryContext)
                })
            }
            DecodedOperation::ReleaseQueryContext(request) => {
                let receipt = self.registry.release_query_context(request);
                // Read after the release settled: the completion pass inside
                // it is what hands the shared facts back to the host and seals
                // this evidence.
                let evidence = self.registry.released_context_evidence(request.context());
                encode_item(&receipt, |ack| {
                    let mut encoded = encode_release_ack(
                        ack.context(),
                        ack.release(),
                        ack.state(),
                        evidence.runtime_filter(),
                    )?;
                    encoded.termination_cause =
                        ack.termination_cause().map(encode_abort_cause_field);
                    Some(ReceiptAck::ReleaseQueryContext(encoded))
                })
            }
        }
    }
}

/// Maps a host rejection onto a status code for a read that has no in-band
/// outcome field.
///
/// The category matters to the caller: a protocol refusal is the reader's own
/// request to fix, while anything else is this process failing to answer a
/// legal question and must not read as "your request was wrong".
fn rejection_status(rejection: HostRejection) -> tonic::Status {
    let detail = rejection.detail().as_str().to_owned();
    match rejection.category() {
        TaskFailureCategory::Protocol => tonic::Status::invalid_argument(detail),
        TaskFailureCategory::ResourceExhausted => tonic::Status::resource_exhausted(detail),
        TaskFailureCategory::Execution
        | TaskFailureCategory::Exchange
        | TaskFailureCategory::Internal => tonic::Status::internal(detail),
    }
}

/// Encodes one receipt, refusing to substitute anything the wire cannot say.
fn encode_item<T>(
    receipt: &OperationReceipt<T>,
    encode_ack: impl FnOnce(&T) -> Option<ReceiptAck>,
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

#[tonic::async_trait]
impl TaskExecutionIngress for RegistryTaskExecutionIngress {
    fn apply_task_operations(
        &self,
        request: proto::ApplyTaskOperationsRequest,
    ) -> Result<proto::ApplyTaskOperationsResponse, tonic::Status> {
        // Confidential bytes are refused on the raw request, before any
        // domain decoder can project or retain them and before any registry
        // operation can take effect. The fact comes from the same
        // Server-resolved transport mode that configured this BE listener.
        refuse_confidential_material_in_the_clear(
            &request,
            self.native_transport_confidentiality,
            FieldPath::root("apply_task_operations"),
        )
        .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
        // The transport budget is checked over the whole batch before any
        // item is decoded, so an oversized batch never reaches the owner.
        let operations = decode_operation_batch(
            &request,
            TransportBudget::DEFAULT,
            FieldPath::root("apply_task_operations"),
        )
        .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
        let mut receipts = Vec::with_capacity(operations.len());
        for operation in &operations {
            receipts.push(self.apply_one(operation)?);
        }
        Ok(proto::ApplyTaskOperationsResponse { receipts })
    }

    fn subscribe_task_status(
        &self,
        request: proto::SubscribeTaskStatusRequest,
    ) -> Result<TaskStatusEventStream, tonic::Status> {
        let (context, cursors) =
            decode_subscribe_task_status(&request, FieldPath::root("subscribe_task_status"))
                .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
        let source = self.registry.status_source(context).ok_or_else(|| {
            tonic::Status::failed_precondition(
                "subscribe names a query context this backend does not hold",
            )
        })?;
        // The catch-up frames are taken before the stream exists, so a cursor
        // that is behind cannot miss a version published between the two.
        let catch_up = source.subscribe(&cursors);
        if fault::task_status_subscription_dropped(context)? {
            // The subscription was established and is then torn down from the
            // stream body, which is what a lost stream looks like. Cursors are
            // read-only, so the resubscription loses no frame.
            return Ok(Box::pin(tokio_stream::once(Err(
                tonic::Status::unavailable(
                    "runner-owned task status stream dropped after the subscription was established",
                ),
            ))));
        }
        Ok(Box::pin(TaskStatusSubscription::new(source, catch_up)))
    }

    fn fetch_task_dynamic_filters(
        &self,
        request: proto::FetchTaskDynamicFiltersRequest,
    ) -> Result<proto::FetchTaskDynamicFiltersResponse, tonic::Status> {
        // An observation read carries no envelope on the wire, so its
        // operation identity is minted here and never replayed.
        let read = decode_fetch_dynamic_filters(
            &request,
            TaskOperationId::new_v7(),
            FieldPath::root("fetch_task_dynamic_filters"),
        )
        .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
        let identity = read.identity();
        let receipt = self.registry.fetch_task_dynamic_filters(&read);
        // `Accepted` carries a version newer than the caller acknowledged and
        // `Idempotent` says it is already current; both are settled answers,
        // with or without a payload. This response has no outcome field, so
        // anything else is a refusal that must not be dressed as an empty
        // answer.
        if !matches!(
            receipt.outcome(),
            OperationOutcome::Accepted | OperationOutcome::Idempotent
        ) {
            return Err(tonic::Status::failed_precondition(
                receipt.detail().map_or("", SafeDetail::as_str).to_owned(),
            ));
        }
        // A settled read with nothing advertised answers version zero, which
        // is this field family's "nothing": `DomainVersion` is nonzero, so it
        // cannot collide with a version a task published.
        encode_dynamic_filter_read(identity, receipt.acknowledgement()).map_err(rejection_status)
    }

    fn get_final_task_info(
        &self,
        request: proto::GetFinalTaskInfoRequest,
    ) -> Result<proto::GetFinalTaskInfoResponse, tonic::Status> {
        let read = decode_get_final_task_info(
            &request,
            TaskOperationId::new_v7(),
            FieldPath::root("get_final_task_info"),
        )
        .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
        let receipt = self.registry.get_final_task_info(&read);
        let result = match receipt.acknowledgement() {
            Some(info) => {
                proto::get_final_task_info_response::Result::Info(encode_final_task_info(info))
            }
            // Losing final info costs diagnostics only, so why it is missing
            // is reported as an outcome rather than as an error.
            None => proto::get_final_task_info_response::Result::Unavailable(
                encode_operation_outcome(receipt.outcome()),
            ),
        };
        Ok(proto::GetFinalTaskInfoResponse {
            result: Some(result),
        })
    }

    async fn fetch_task_result(
        &self,
        request: proto::FetchTaskResultRequest,
    ) -> Result<proto::FetchResultResponse, tonic::Status> {
        // The semantics live with the result plane, beside the buffer they
        // read. Two implementations of one RPC drift, and the one that drifts
        // is always the one nobody is looking at.
        crate::rpc::data_plane::fetch_task_result(&self.registry, request).await
    }
}

/// The server side of one logical status subscription.
///
/// It holds the context's observation channel and nothing else, so dropping it
/// — a client that went away, a coordinator that will resubscribe by cursor —
/// cancels no task, aborts no context, and changes no owner state. Waiting is
/// parked on the channel's own notify rather than sampled on a timer, because
/// terminal delivery is on the critical path of every query's completion.
///
/// It never completes on its own. Only the observer knows when it has seen
/// every terminal it was waiting for, so the server keeps the channel open
/// until the client closes it rather than guessing that a subscription is
/// finished.
struct TaskStatusSubscription {
    source: Arc<TaskStatusSource>,
    catch_up: VecDeque<TaskStatusEvent>,
    /// The parked wait for the next frame. It owns its own handle to the
    /// source, so polling never borrows across the await.
    pending: Option<Pin<Box<dyn Future<Output = Option<TaskStatusEvent>> + Send>>>,
}

impl TaskStatusSubscription {
    fn new(source: Arc<TaskStatusSource>, catch_up: Vec<TaskStatusEvent>) -> Self {
        Self {
            source,
            catch_up: catch_up.into(),
            pending: None,
        }
    }
}

impl Stream for TaskStatusSubscription {
    type Item = Result<proto::TaskStatusStreamEvent, tonic::Status>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if let Some(event) = this.catch_up.pop_front() {
            return Poll::Ready(Some(Ok(encode_event(&event))));
        }
        if this.pending.is_none() {
            let source = Arc::clone(&this.source);
            this.pending = Some(Box::pin(async move { source.next_event_owned().await }));
        }
        let pending = this.pending.as_mut().expect("a wait was just installed");
        match pending.as_mut().poll(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(event) => {
                this.pending = None;
                Poll::Ready(event.map(|event| Ok(encode_event(&event))))
            }
        }
    }
}

fn encode_event(event: &TaskStatusEvent) -> proto::TaskStatusStreamEvent {
    let (identity, mut encoded) = match event {
        TaskStatusEvent::Status(status) => (status.identity(), encode_status_event(status)),
        TaskStatusEvent::Gone(identity) => (*identity, encode_task_gone_event(*identity)),
    };
    // Claimed on the frame that is about to leave this process, which is the
    // only place the observation names a backend process the frontend will
    // check. Both the catch-up frames and the live ones are encoded here, so
    // no delivery path escapes it.
    fault::task_status_foreign_process(identity, &mut encoded);
    encoded
}

#[cfg(test)]
mod tests {
    //! The port driven over real wire messages against the real owner.
    //!
    //! Nothing here mocks the registry: a test builds the protobuf a frontend
    //! would send, hands it to the port, and reads the protobuf that comes
    //! back. The execution side is a fake because it is the only part these
    //! cases are not about.

    use std::sync::Mutex;
    use std::time::Duration;

    use novarocks_execution_contract::task_execution::descriptor::TaskDescriptor;
    use novarocks_execution_contract::task_execution::domain::{
        CodecOwnedContent, ContentFingerprint, DomainVersion,
    };
    use novarocks_execution_contract::task_execution::identity::{
        AdmissionTicketId, QueryContextRef, TaskIdentity,
    };
    use novarocks_execution_contract::task_execution::operation::{
        QueryContextDomainUpdate, TaskDomainUpdate,
    };
    use novarocks_execution_contract::task_execution::status::{
        AbortCause, CancelReason, TaskOutputFacts, TaskState, TaskStatus, TaskStatusCursor,
        TaskStatusVersion,
    };
    use novarocks_execution_contract::task_execution::transition::QueryContextState;
    use novarocks_proto_models::{catalog, common, plan};
    use novarocks_task_codec::identity::{
        encode_query_context_ref, encode_task_identity, encode_task_operation_id,
    };
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    use tokio_stream::StreamExt;

    use super::super::clock::{BackendMonotonicClock, ManualClock};
    use super::super::host::{
        HostRejection, QueryContextHost, ReleasedContextEvidence, RunnableTask, SharedFactsRequest,
        TaskExecutionHost,
    };
    use super::super::registry::TaskExecutionRegistryConfig;
    use super::super::status::TaskStatusReporter;
    use super::*;

    /// An execution side that accepts everything, so these cases fail only on
    /// the protocol boundary they are about.
    struct AcceptingContextHost;

    impl QueryContextHost for AcceptingContextHost {
        fn materialize(&self, _request: SharedFactsRequest<'_>) -> Result<(), HostRejection> {
            Ok(())
        }

        fn release(&self, _context: QueryContextRef) -> ReleasedContextEvidence {
            ReleasedContextEvidence::none()
        }

        fn advance_shared_domain(
            &self,
            _context: QueryContextRef,
            _domain: &QueryContextDomainUpdate,
        ) -> Result<(), HostRejection> {
            Ok(())
        }
    }

    #[derive(Debug)]
    struct InertRunnable;

    impl RunnableTask for InertRunnable {
        fn cancel(&self, _reason: CancelReason) {}

        fn abort(&self, _cause: AbortCause) {}
    }

    /// Content with no wire projection, which is what every codec-owned
    /// payload looks like from this boundary.
    #[derive(Debug)]
    struct OpaqueContent;

    impl CodecOwnedContent for OpaqueContent {
        fn fingerprint(&self) -> ContentFingerprint {
            ContentFingerprint::from_bytes([7u8; 16])
        }

        fn encoded_len(&self) -> usize {
            32
        }
    }

    #[derive(Default)]
    struct AcceptingTaskHost {
        /// Kept so a test can publish through the same handle a real task
        /// would use.
        reporters: Mutex<Vec<TaskStatusReporter>>,
    }

    impl AcceptingTaskHost {
        fn reporter(&self, identity: TaskIdentity) -> TaskStatusReporter {
            self.reporters
                .lock()
                .expect("reporters")
                .iter()
                .find(|reporter| reporter.identity() == identity)
                .expect("a submitted task has a reporter")
                .clone()
        }
    }

    impl TaskExecutionHost for AcceptingTaskHost {
        fn close_context_admission(&self, _context: QueryContextRef) {}

        fn forget_context_admission(&self, _context: QueryContextRef) {}

        fn install_receiver(&self, _descriptor: &TaskDescriptor) -> Result<(), HostRejection> {
            Ok(())
        }

        fn remove_receiver(&self, _descriptor: &TaskDescriptor) {}

        fn install_inbound_capability(
            &self,
            _descriptor: &TaskDescriptor,
        ) -> Result<(), HostRejection> {
            Ok(())
        }

        fn remove_inbound_capability(&self, _descriptor: &TaskDescriptor) {}

        fn submit_runnable(
            &self,
            _descriptor: &TaskDescriptor,
            reporter: TaskStatusReporter,
        ) -> Result<Arc<dyn RunnableTask>, HostRejection> {
            self.reporters.lock().expect("reporters").push(reporter);
            Ok(Arc::new(InertRunnable))
        }

        fn apply_task_domain(
            &self,
            _descriptor: &TaskDescriptor,
            _domain: &TaskDomainUpdate,
        ) -> Result<Option<u64>, HostRejection> {
            Ok(None)
        }
    }

    struct Fixture {
        ingress: Arc<RegistryTaskExecutionIngress>,
        registry: Arc<TaskExecutionRegistry>,
        task_host: Arc<AcceptingTaskHost>,
        backend: BackendProcessId,
        frontend: FrontendProcessId,
        native_compatibility_id: NativeCompatibilityId,
    }

    impl Fixture {
        fn new() -> Self {
            Self::with_transport(ConfidentialTransport::Plaintext)
        }

        fn with_transport(native_transport_confidentiality: ConfidentialTransport) -> Self {
            let backend = BackendProcessId::new_v7();
            let mut config = TaskExecutionRegistryConfig::for_process(backend);
            // Nothing here waits on a gate, and no case may depend on
            // elapsed wall time.
            config.gate_poll_interval = Duration::from_secs(3600);
            let task_host = Arc::new(AcceptingTaskHost::default());
            let native_compatibility_id = NativeCompatibilityId::new([0x71; 32]);
            let registry = TaskExecutionRegistry::new(
                config,
                Arc::new(ManualClock::new()) as Arc<dyn BackendMonotonicClock>,
                Arc::new(AcceptingContextHost),
                Arc::clone(&task_host) as Arc<dyn TaskExecutionHost>,
            );
            Self {
                ingress: RegistryTaskExecutionIngress::new(
                    Arc::clone(&registry),
                    native_compatibility_id,
                    native_transport_confidentiality,
                ),
                registry,
                task_host,
                backend,
                frontend: FrontendProcessId::new_v7(),
                native_compatibility_id,
            }
        }

        fn execution(&self) -> QueryExecutionId {
            QueryExecutionId::new(
                QueryId::new(17, 23),
                AttemptId::new(1).expect("nonzero attempt"),
            )
            .expect("nonzero query")
        }

        fn context(&self) -> QueryContextRef {
            QueryContextRef::new(self.execution(), self.frontend, self.backend)
        }

        fn other_context(&self) -> QueryContextRef {
            QueryContextRef::new(
                QueryExecutionId::new(
                    QueryId::new(18, 24),
                    AttemptId::new(1).expect("nonzero attempt"),
                )
                .expect("nonzero query"),
                self.frontend,
                self.backend,
            )
        }

        fn identity(&self, stage: u32, task: u32) -> TaskIdentity {
            self.identity_on(stage, task, self.backend)
        }

        fn identity_on(&self, stage: u32, task: u32, process: BackendProcessId) -> TaskIdentity {
            TaskIdentity::new(
                self.execution(),
                StageId::new(stage).expect("nonzero stage"),
                TaskId::new(task).expect("nonzero task"),
                process,
            )
        }

        fn apply(
            &self,
            operations: Vec<proto::TaskOperation>,
        ) -> proto::ApplyTaskOperationsResponse {
            self.ingress
                .apply_task_operations(proto::ApplyTaskOperationsRequest { operations })
                .expect("a well formed batch is answered with receipts")
        }

        fn acquire_ticket(&self, context: QueryContextRef) -> AdmissionTicketId {
            let response = self.apply(vec![acquire_ticket(
                context,
                TaskOperationId::new_v7(),
                self.native_compatibility_id,
                self.registry.admission_epoch_capability(),
            )]);
            let Some(ReceiptAck::QueryContextAdmissionTicket(ack)) = &response.receipts[0].ack
            else {
                panic!("ticket acquisition returns its grant");
            };
            let bytes: [u8; 16] = ack
                .ticket_id
                .as_ref()
                .expect("ticket acknowledgement names the nonce")
                .value
                .as_slice()
                .try_into()
                .expect("ticket nonce is exactly 16 bytes");
            AdmissionTicketId::try_from_bytes(bytes).expect("worker minted a nonzero nonce")
        }

        fn establish(
            &self,
            context: QueryContextRef,
            operation: TaskOperationId,
        ) -> proto::TaskOperation {
            establish_with_compatibility(
                context,
                operation,
                self.acquire_ticket(context),
                self.native_compatibility_id,
            )
        }
    }

    fn envelope(operation: TaskOperationId) -> proto::TaskOperationEnvelope {
        proto::TaskOperationEnvelope {
            operation_id: Some(encode_task_operation_id(operation)),
            max_wait_millis: 5_000,
        }
    }

    fn acquire_ticket(
        context: QueryContextRef,
        operation: TaskOperationId,
        native_compatibility_id: NativeCompatibilityId,
        admission_epoch_capability: novarocks_execution_contract::AdmissionEpochCapability,
    ) -> proto::TaskOperation {
        proto::TaskOperation {
            envelope: Some(envelope(operation)),
            operation: Some(
                proto::task_operation::Operation::AcquireQueryContextAdmissionTicket(
                    proto::AcquireQueryContextAdmissionTicketRequest {
                        query_context: Some(encode_query_context_ref(context)),
                        valid_for_millis: 10_000,
                        native_compatibility_id: Some(proto::NativeCompatibilityId {
                            value: native_compatibility_id.as_bytes().to_vec(),
                        }),
                        admission_epoch_capability: Some(proto::AdmissionEpochCapability {
                            value: admission_epoch_capability.to_bytes().to_vec(),
                        }),
                    },
                ),
            ),
        }
    }

    fn establish_with_compatibility(
        context: QueryContextRef,
        operation: TaskOperationId,
        admission_ticket_id: AdmissionTicketId,
        native_compatibility_id: NativeCompatibilityId,
    ) -> proto::TaskOperation {
        proto::TaskOperation {
            envelope: Some(envelope(operation)),
            operation: Some(proto::task_operation::Operation::UpdateQueryContext(
                proto::UpdateQueryContextRequest {
                    command: Some(proto::update_query_context_request::Command::Establish(
                        proto::EstablishQueryContextRequest {
                            query_context: Some(encode_query_context_ref(context)),
                            catalog_set: Some(catalog::CatalogSet::default()),
                            initial_runtime_filter: Some(
                                proto::RuntimeFilterContribution::default(),
                            ),
                            initial_credential: Some(proto::QueryContextCredentialDomain {
                                lease_id: 1,
                                epoch: 1,
                                descriptors: Vec::new(),
                                envelopes: Vec::new(),
                            }),
                            initial_lease: Some(proto::QueryExecutionLeaseGrant {
                                sequence: 0,
                                valid_for_millis: 30_000,
                            }),
                            query_options: Some(query_options()),
                            native_compatibility_id: Some(proto::NativeCompatibilityId {
                                value: native_compatibility_id.as_bytes().to_vec(),
                            }),
                            admission_ticket_id: Some(proto::AdmissionTicketId {
                                value: admission_ticket_id.to_bytes().to_vec(),
                            }),
                        },
                    )),
                },
            )),
        }
    }

    fn renew_lease(
        context: QueryContextRef,
        operation: TaskOperationId,
        sequence: u64,
    ) -> proto::TaskOperation {
        proto::TaskOperation {
            envelope: Some(envelope(operation)),
            operation: Some(proto::task_operation::Operation::UpdateQueryContext(
                proto::UpdateQueryContextRequest {
                    command: Some(proto::update_query_context_request::Command::RenewLease(
                        proto::RenewQueryExecutionLeaseRequest {
                            query_context: Some(encode_query_context_ref(context)),
                            lease: Some(proto::QueryExecutionLeaseGrant {
                                sequence,
                                valid_for_millis: 5_000,
                            }),
                        },
                    )),
                },
            )),
        }
    }

    fn release(context: QueryContextRef, operation: TaskOperationId) -> proto::TaskOperation {
        proto::TaskOperation {
            envelope: Some(envelope(operation)),
            operation: Some(proto::task_operation::Operation::ReleaseQueryContext(
                proto::ReleaseQueryContextRequest {
                    query_context: Some(encode_query_context_ref(context)),
                },
            )),
        }
    }

    fn cancel(identity: TaskIdentity, operation: TaskOperationId) -> proto::TaskOperation {
        proto::TaskOperation {
            envelope: Some(envelope(operation)),
            operation: Some(proto::task_operation::Operation::CancelTask(
                proto::CancelTaskRequest {
                    identity: Some(encode_task_identity(identity)),
                    reason: proto::TaskCancelReason::UpstreamNoLongerNeeded as i32,
                },
            )),
        }
    }

    fn query_options() -> proto::QueryOptions {
        proto::QueryOptions {
            pipeline_dop: 2,
            ..Default::default()
        }
    }

    fn unique(hi: i64, lo: i64) -> common::UniqueId {
        common::UniqueId { hi, lo }
    }

    /// A single-fragment root task: no exchange topology, one result sink.
    fn create_task(
        context: QueryContextRef,
        identity: TaskIdentity,
        operation: TaskOperationId,
    ) -> proto::TaskOperation {
        let finst = unique(41, 42);
        proto::TaskOperation {
            envelope: Some(envelope(operation)),
            operation: Some(proto::task_operation::Operation::CreateTask(
                proto::CreateTaskRequest {
                    query_context: Some(encode_query_context_ref(context)),
                    descriptor: Some(proto::TaskDescriptor {
                        identity: Some(encode_task_identity(identity)),
                        fragment_instance_id: Some(finst),
                        pipeline_dop: 2,
                        split_plan_nodes: Vec::new(),
                        topology: Some(proto::TaskExchangeTopology::default()),
                        fragment: Some(proto::TaskFragmentPlan {
                            plan: Some(plan::PlanFragment {
                                fragment_id: 1,
                                sink: Some(plan::DataSink {
                                    kind: Some(plan::data_sink::Kind::Result(true)),
                                }),
                                ..Default::default()
                            }),
                            instance_params: Some(proto::InstanceParams {
                                query_id: Some(unique(17, 23)),
                                fragment_instance_id: Some(finst),
                                query_options: Some(query_options()),
                                typed_result_sink: true,
                                ..Default::default()
                            }),
                        }),
                    }),
                    initial_domains: Vec::new(),
                },
            )),
        }
    }

    /// The same task with a data-stream sink: a legitimate exchange producer,
    /// which owns a result buffer but owes the coordinator no result.
    fn create_producer_task(
        context: QueryContextRef,
        identity: TaskIdentity,
        operation: TaskOperationId,
    ) -> proto::TaskOperation {
        let mut request = create_task(context, identity, operation);
        let Some(proto::task_operation::Operation::CreateTask(create)) = request.operation.as_mut()
        else {
            unreachable!("create_task builds a create");
        };
        let fragment = create
            .descriptor
            .as_mut()
            .and_then(|descriptor| descriptor.fragment.as_mut())
            .expect("the fixture carries a fragment");
        fragment
            .plan
            .as_mut()
            .expect("the fixture carries a plan")
            .sink = Some(plan::DataSink {
            kind: Some(plan::data_sink::Kind::DataStream(
                plan::DataStreamSink::default(),
            )),
        });
        fragment
            .instance_params
            .as_mut()
            .expect("the fixture carries instance params")
            .typed_result_sink = false;
        request
    }

    fn outcome_of(receipt: &proto::TaskOperationReceipt) -> proto::TaskOperationOutcome {
        proto::TaskOperationOutcome::try_from(receipt.outcome).expect("a known outcome")
    }

    #[test]
    fn a_foreign_compatibility_identity_is_rejected_before_context_side_effects() {
        let fixture = Fixture::new();
        let context = fixture.context();
        let response = fixture.apply(vec![acquire_ticket(
            context,
            TaskOperationId::new_v7(),
            NativeCompatibilityId::new([0x72; 32]),
            fixture.registry.admission_epoch_capability(),
        )]);

        assert_eq!(response.receipts.len(), 1);
        assert_eq!(
            outcome_of(&response.receipts[0]),
            proto::TaskOperationOutcome::CompatibilityMismatch
        );
        assert!(
            response.receipts[0].ack.is_none(),
            "a compatibility rejection cannot acknowledge an establishment"
        );
        assert_eq!(
            fixture.registry.context_state(context),
            QueryContextState::Absent
        );
        assert_eq!(
            fixture.registry.admission_reservation_count(),
            0,
            "compatibility must be rejected before the authority reserves capacity"
        );

        let response = fixture.apply(vec![renew_lease(context, TaskOperationId::new_v7(), 1)]);
        assert_eq!(
            outcome_of(&response.receipts[0]),
            proto::TaskOperationOutcome::ContextNotEstablished,
            "the rejected establish must leave no context for a later operation"
        );
        assert_eq!(
            fixture.registry.context_state(context),
            QueryContextState::Absent
        );
    }

    #[test]
    fn the_exact_native_compatibility_identity_establishes_the_context() {
        let fixture = Fixture::new();
        let context = fixture.context();
        let ticket_id = fixture.acquire_ticket(context);
        let response = fixture.apply(vec![establish_with_compatibility(
            context,
            TaskOperationId::new_v7(),
            ticket_id,
            fixture.native_compatibility_id,
        )]);

        assert_eq!(response.receipts.len(), 1);
        assert_eq!(
            outcome_of(&response.receipts[0]),
            proto::TaskOperationOutcome::Accepted
        );
        assert!(matches!(
            response.receipts[0].ack,
            Some(ReceiptAck::QueryContext(_))
        ));
        assert_eq!(
            fixture.registry.context_state(context),
            QueryContextState::Active
        );
    }

    #[test]
    fn a_compatibility_rejection_does_not_refuse_an_independent_valid_batch_item() {
        let fixture = Fixture::new();
        let rejected_context = fixture.context();
        let accepted_context = fixture.other_context();
        let accepted_ticket = fixture.acquire_ticket(accepted_context);
        let response = fixture.apply(vec![
            acquire_ticket(
                rejected_context,
                TaskOperationId::new_v7(),
                NativeCompatibilityId::new([0x72; 32]),
                fixture.registry.admission_epoch_capability(),
            ),
            establish_with_compatibility(
                accepted_context,
                TaskOperationId::new_v7(),
                accepted_ticket,
                fixture.native_compatibility_id,
            ),
        ]);

        assert_eq!(response.receipts.len(), 2);
        assert_eq!(
            outcome_of(&response.receipts[0]),
            proto::TaskOperationOutcome::CompatibilityMismatch
        );
        assert_eq!(
            outcome_of(&response.receipts[1]),
            proto::TaskOperationOutcome::Accepted,
            "a rejected item must not carry the valid establish down with it"
        );
        assert!(matches!(
            response.receipts[1].ack,
            Some(ReceiptAck::QueryContext(_))
        ));
        assert_eq!(
            fixture.registry.context_state(rejected_context),
            QueryContextState::Absent
        );
        assert_eq!(
            fixture.registry.context_state(accepted_context),
            QueryContextState::Active
        );
    }

    #[test]
    fn a_mixed_batch_answers_every_item_in_request_order() {
        let fixture = Fixture::new();
        let context = fixture.context();
        let ids = [
            TaskOperationId::new_v7(),
            TaskOperationId::new_v7(),
            TaskOperationId::new_v7(),
        ];
        let response = fixture.apply(vec![
            fixture.establish(context, ids[0]),
            renew_lease(context, ids[1], 1),
            release(context, ids[2]),
        ]);

        assert_eq!(response.receipts.len(), 3);
        for (receipt, expected) in response.receipts.iter().zip(ids) {
            assert_eq!(
                receipt
                    .operation_id
                    .as_ref()
                    .map(|id| id.value.clone())
                    .expect("a receipt carries its operation id"),
                encode_task_operation_id(expected).value,
                "receipts must arrive in request order"
            );
            assert_eq!(outcome_of(receipt), proto::TaskOperationOutcome::Accepted);
        }
        assert!(matches!(
            response.receipts[0].ack,
            Some(ReceiptAck::QueryContext(_))
        ));
        assert!(matches!(
            response.receipts[1].ack,
            Some(ReceiptAck::QueryContext(_))
        ));
        assert!(matches!(
            response.receipts[2].ack,
            Some(ReceiptAck::ReleaseQueryContext(_))
        ));
    }

    #[test]
    fn one_refused_item_leaves_every_other_receipt_untouched() {
        let fixture = Fixture::new();
        let context = fixture.context();
        let ids = [
            TaskOperationId::new_v7(),
            TaskOperationId::new_v7(),
            TaskOperationId::new_v7(),
        ];
        let response = fixture.apply(vec![
            fixture.establish(context, ids[0]),
            cancel(fixture.identity(9, 9), ids[1]),
            renew_lease(context, ids[2], 1),
        ]);

        assert_eq!(response.receipts.len(), 3);
        assert_eq!(
            outcome_of(&response.receipts[0]),
            proto::TaskOperationOutcome::Accepted
        );
        assert_eq!(
            outcome_of(&response.receipts[1]),
            proto::TaskOperationOutcome::InvalidStateOrRequest
        );
        assert!(
            response.receipts[1].ack.is_none(),
            "a refusal carries no acknowledgement"
        );
        assert!(
            !response.receipts[1].safe_detail.is_empty(),
            "a refusal carries its own redacted diagnostic"
        );
        assert_eq!(
            outcome_of(&response.receipts[2]),
            proto::TaskOperationOutcome::Accepted,
            "the refused cancel must not carry the renewal down with it"
        );
        assert_eq!(
            fixture.registry.context_state(context),
            QueryContextState::Active
        );
    }

    #[test]
    fn a_malformed_batch_is_refused_as_a_status_rather_than_a_receipt() {
        let fixture = Fixture::new();
        let error = fixture
            .ingress
            .apply_task_operations(proto::ApplyTaskOperationsRequest {
                operations: vec![proto::TaskOperation {
                    envelope: Some(envelope(TaskOperationId::new_v7())),
                    operation: None,
                }],
            })
            .expect_err("an operation without a command cannot be understood");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert!(
            error.message().contains("requires a command"),
            "unexpected message: {}",
            error.message()
        );
    }

    #[test]
    fn confidential_material_is_gated_before_domain_decode_or_apply() {
        let request = || proto::ApplyTaskOperationsRequest {
            operations: vec![proto::TaskOperation {
                // This is intentionally malformed. A confidential transport
                // reaches this later structural check; plaintext must stop at
                // the raw confidentiality gate first.
                envelope: None,
                operation: Some(proto::task_operation::Operation::UpdateQueryContext(
                    proto::UpdateQueryContextRequest {
                        command: Some(proto::update_query_context_request::Command::Establish(
                            proto::EstablishQueryContextRequest {
                                initial_credential: Some(proto::QueryContextCredentialDomain {
                                    lease_id: 1,
                                    epoch: 1,
                                    descriptors: Vec::new(),
                                    envelopes: vec![proto::CredentialLeaseSecretEnvelope::default()],
                                }),
                                ..Default::default()
                            },
                        )),
                    },
                )),
            }],
        };

        let plaintext = Fixture::with_transport(ConfidentialTransport::Plaintext);
        let plaintext_error = plaintext
            .ingress
            .apply_task_operations(request())
            .expect_err("plaintext must reject confidential bytes at raw ingress");
        assert_eq!(plaintext_error.code(), tonic::Code::InvalidArgument);
        assert!(
            plaintext_error
                .message()
                .contains("credential material requires a confidential native transport"),
            "unexpected plaintext rejection: {}",
            plaintext_error.message()
        );

        let confidential = Fixture::with_transport(ConfidentialTransport::Confidential);
        let confidential_error = confidential
            .ingress
            .apply_task_operations(request())
            .expect_err("the intentionally malformed request must reach structural decode");
        assert_eq!(confidential_error.code(), tonic::Code::InvalidArgument);
        assert!(
            confidential_error
                .message()
                .contains("requires an envelope"),
            "TLS admission did not reach structural decode: {}",
            confidential_error.message()
        );
    }

    #[test]
    fn an_oversized_batch_is_refused_before_any_item_is_walked() {
        let fixture = Fixture::new();
        let over_budget = TransportBudget::DEFAULT.max_batch_items() + 1;
        // Every item is also individually malformed, so the reported error
        // proves which check ran first.
        let operations = (0..over_budget)
            .map(|_| proto::TaskOperation {
                envelope: Some(envelope(TaskOperationId::new_v7())),
                operation: None,
            })
            .collect();
        let error = fixture
            .ingress
            .apply_task_operations(proto::ApplyTaskOperationsRequest { operations })
            .expect_err("a batch over its item budget is refused");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert!(
            error.message().contains("item or byte budget"),
            "unexpected message: {}",
            error.message()
        );
        assert!(
            !error.message().contains("requires a command"),
            "the budget must be checked before the items are decoded: {}",
            error.message()
        );
    }

    #[tokio::test]
    async fn a_subscription_delivers_its_catch_up_frame_and_then_a_later_event() {
        let fixture = Fixture::new();
        let context = fixture.context();
        fixture.apply(vec![fixture.establish(context, TaskOperationId::new_v7())]);
        let identity = fixture.identity(1, 1);
        let source = fixture
            .registry
            .status_source(context)
            .expect("an active context has an observation channel");

        let first = TaskStatus::try_new(
            identity,
            TaskStatusVersion::FIRST,
            TaskState::Running,
            None,
            TaskOutputFacts::default(),
        )
        .expect("a running status carries no termination");
        source.publish(first);
        // Taken by an earlier observer, which is exactly the situation the
        // catch-up frame exists for: the cursor is behind and no frame is
        // queued to bring it forward.
        assert!(source.next_event().is_some());
        assert_eq!(source.queued_frames(), 0);

        let mut stream = fixture
            .ingress
            .subscribe_task_status(proto::SubscribeTaskStatusRequest {
                query_context: Some(encode_query_context_ref(context)),
                cursors: vec![novarocks_task_codec::status::encode_task_status_cursor(
                    TaskStatusCursor::unobserved(identity),
                )],
            })
            .expect("an active context accepts a subscription");

        assert_eq!(next_status_version(&mut stream).await, 1);

        let second = TaskStatus::try_new(
            identity,
            TaskStatusVersion::new(2).expect("nonzero version"),
            TaskState::Flushing,
            None,
            TaskOutputFacts::default(),
        )
        .expect("a flushing status carries no termination");
        source.publish(second);
        assert_eq!(next_status_version(&mut stream).await, 2);
    }

    #[tokio::test]
    async fn dropping_a_subscription_leaves_its_task_live() {
        let fixture = Fixture::new();
        let context = fixture.context();
        let identity = fixture.identity(1, 1);
        let response = fixture.apply(vec![
            fixture.establish(context, TaskOperationId::new_v7()),
            create_task(context, identity, TaskOperationId::new_v7()),
        ]);
        assert_eq!(
            outcome_of(&response.receipts[1]),
            proto::TaskOperationOutcome::Accepted,
            "unexpected create refusal: {}",
            response.receipts[1].safe_detail
        );
        assert!(fixture.registry.has_live_task(identity));

        let mut stream = fixture
            .ingress
            .subscribe_task_status(proto::SubscribeTaskStatusRequest {
                query_context: Some(encode_query_context_ref(context)),
                cursors: vec![novarocks_task_codec::status::encode_task_status_cursor(
                    TaskStatusCursor::unobserved(identity),
                )],
            })
            .expect("an active context accepts a subscription");
        // Proves the stream was really live before it was dropped.
        assert_eq!(next_status_version(&mut stream).await, 1);
        drop(stream);

        assert!(
            fixture.registry.has_live_task(identity),
            "observation must not cancel, abort, or retire a task"
        );
        assert_eq!(
            fixture.registry.context_state(context),
            QueryContextState::Active
        );
    }

    #[test]
    fn a_subscription_for_an_absent_context_is_refused() {
        let fixture = Fixture::new();
        let Err(error) = fixture
            .ingress
            .subscribe_task_status(proto::SubscribeTaskStatusRequest {
                query_context: Some(encode_query_context_ref(fixture.context())),
                cursors: Vec::new(),
            })
        else {
            panic!("this process holds no such context");
        };
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn a_result_poll_for_a_foreign_task_is_refused_before_a_buffer() {
        let fixture = Fixture::new();
        let foreign = fixture.identity_on(1, 1, BackendProcessId::new_v7());
        let response = fixture
            .ingress
            .fetch_task_result(proto::FetchTaskResultRequest {
                root_task: Some(encode_task_identity(foreign)),
                max_wait_millis: 60_000,
                acknowledged_packet_sequence: None,
                max_result_bytes: 16 * 1024 * 1024,
            })
            .await
            .expect("a fenced poll is answered, not errored");
        assert_eq!(
            response.status,
            proto::fetch_result_response::Status::Error as i32
        );
        assert_eq!(
            response.message, "result poll names a task this backend process does not own",
            "the refusal must come from the process fence, not from a buffer"
        );
        assert_eq!(response.packet_seq, 0);
        assert!(!response.eos);
        assert!(response.result_arrow_ipc.is_empty());
    }

    #[test]
    fn final_info_that_is_not_available_comes_back_as_an_outcome() {
        let fixture = Fixture::new();
        let response = fixture
            .ingress
            .get_final_task_info(proto::GetFinalTaskInfoRequest {
                identity: Some(encode_task_identity(fixture.identity(4, 4))),
            })
            .expect("losing final info costs diagnostics only, so it is never an error");
        assert_eq!(
            response.result,
            Some(proto::get_final_task_info_response::Result::Unavailable(
                proto::TaskOperationOutcome::InvalidStateOrRequest as i32
            ))
        );
    }

    /// Takes the next frame, refusing to hang if the stream never wakes.
    async fn next_status_version(stream: &mut TaskStatusEventStream) -> u64 {
        let event = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("a published frame must wake the subscription")
            .expect("the stream is still open")
            .expect("an observation frame is never a status error");
        match event.event.expect("a frame carries a body") {
            proto::task_status_stream_event::Event::TaskStatus(status) => status.status_version,
            proto::task_status_stream_event::Event::TaskGone(_) => {
                panic!("expected a status frame, not a reclamation")
            }
        }
    }

    #[test]
    fn a_dynamic_filter_read_with_nothing_advertised_is_a_settled_empty_answer() {
        let fixture = Fixture::new();
        let context = fixture.context();
        let identity = fixture.identity(1, 1);
        fixture.apply(vec![
            fixture.establish(context, TaskOperationId::new_v7()),
            create_task(context, identity, TaskOperationId::new_v7()),
        ]);
        let response = fixture
            .ingress
            .fetch_task_dynamic_filters(proto::FetchTaskDynamicFiltersRequest {
                identity: Some(encode_task_identity(identity)),
                acknowledged_version: 0,
            })
            .expect("a live task with nothing advertised is a settled read");
        // Version zero cannot collide with a version a task published, so it
        // is the one honest way to say "nothing to fetch".
        assert_eq!(response.version, 0);
        assert!(response.domains.is_empty());
    }

    #[tokio::test]
    async fn a_result_poll_aimed_at_an_exchange_producer_is_refused() {
        // Every task owns a result buffer keyed by its kernel key, so routing
        // a poll by that key alone would hand back an exchange producer's
        // output as if it were the query's answer. Owning a buffer and owing
        // the coordinator a result are different facts.
        let fixture = Fixture::new();
        let context = fixture.context();
        let producer = fixture.identity(3, 3);
        fixture.apply(vec![
            fixture.establish(context, TaskOperationId::new_v7()),
            create_producer_task(context, producer, TaskOperationId::new_v7()),
        ]);
        assert!(fixture.registry.has_live_task(producer));

        let response = fixture
            .ingress
            .fetch_task_result(proto::FetchTaskResultRequest {
                root_task: Some(encode_task_identity(producer)),
                max_wait_millis: 60_000,
                acknowledged_packet_sequence: None,
                max_result_bytes: 16 * 1024 * 1024,
            })
            .await
            .expect("a fenced poll is answered, not errored");
        assert_eq!(
            response.status,
            proto::fetch_result_response::Status::Error as i32
        );
        assert_eq!(
            response.message, "result poll names a task that does not own this query's result",
            "the refusal must come from the ownership check, not from a buffer"
        );
        assert!(response.result_arrow_ipc.is_empty());
    }

    #[test]
    fn a_codec_produced_filter_payload_reaches_the_reader() {
        // The refusal path below only means something if the accepting path
        // works: a projection that always failed would satisfy that test while
        // making every real fetch useless.
        let fixture = Fixture::new();
        let context = fixture.context();
        let identity = fixture.identity(1, 1);
        fixture.apply(vec![
            fixture.establish(context, TaskOperationId::new_v7()),
            create_task(context, identity, TaskOperationId::new_v7()),
        ]);
        let envelope = novarocks_proto_models::filter::RuntimeFilterEnvelope {
            channel_id: 7,
            ..Default::default()
        };
        fixture
            .task_host
            .reporter(identity)
            .advertise_dynamic_filters(
                DomainVersion::new(4).expect("nonzero version"),
                2,
                Arc::new(novarocks_task_codec::domain::WireContent::new(
                    b"novarocks.task_execution.task_dynamic_filter.v1",
                    envelope.clone(),
                )) as Arc<dyn CodecOwnedContent>,
            );

        let response = fixture
            .ingress
            .fetch_task_dynamic_filters(proto::FetchTaskDynamicFiltersRequest {
                identity: Some(encode_task_identity(identity)),
                acknowledged_version: 0,
            })
            .expect("a codec-produced payload projects back");
        assert_eq!(response.version, 4);
        assert_eq!(response.domains.len(), 1);
        assert_eq!(response.domains[0].version, 4);
        assert_eq!(response.domains[0].envelope.as_ref(), Some(&envelope));

        // A caller that is already current is answered rather than refused.
        let current = fixture
            .ingress
            .fetch_task_dynamic_filters(proto::FetchTaskDynamicFiltersRequest {
                identity: Some(encode_task_identity(identity)),
                acknowledged_version: 4,
            })
            .expect("an already-current read is settled, not refused");
        assert_eq!(current.version, 4);
    }

    #[test]
    fn a_retained_filter_payload_is_refused_rather_than_reported_as_no_domains() {
        let fixture = Fixture::new();
        let context = fixture.context();
        let identity = fixture.identity(1, 1);
        fixture.apply(vec![
            fixture.establish(context, TaskOperationId::new_v7()),
            create_task(context, identity, TaskOperationId::new_v7()),
        ]);
        fixture
            .task_host
            .reporter(identity)
            .advertise_dynamic_filters(
                DomainVersion::new(3).expect("nonzero version"),
                2,
                Arc::new(OpaqueContent) as Arc<dyn CodecOwnedContent>,
            );

        let error = fixture
            .ingress
            .fetch_task_dynamic_filters(proto::FetchTaskDynamicFiltersRequest {
                identity: Some(encode_task_identity(identity)),
                acknowledged_version: 0,
            })
            .expect_err("this boundary cannot encode a codec-owned payload");
        assert_eq!(error.code(), tonic::Code::Internal);
        assert!(
            error.message().contains("version 3"),
            "the refusal must name what it could not encode: {}",
            error.message()
        );
    }

    #[test]
    fn a_dynamic_filter_read_for_an_unknown_task_is_refused() {
        let fixture = Fixture::new();
        let error = fixture
            .ingress
            .fetch_task_dynamic_filters(proto::FetchTaskDynamicFiltersRequest {
                identity: Some(encode_task_identity(fixture.identity(2, 2))),
                acknowledged_version: 0,
            })
            .expect_err("this response has no outcome field to refuse in");
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    }
}
