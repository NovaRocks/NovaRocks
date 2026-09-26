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
//! [`TaskDynamicFilterRead`](novarocks_worker::TaskDynamicFilterRead), whose
//! content sits behind `CodecOwnedContent` and exposes only a fingerprint and
//! a size — no wire projection. So a read that really has a payload is refused
//! as an internal invariant violation instead of being answered with an empty
//! domain list under a nonzero version, which would claim that the version
//! carries nothing. Nothing in this process advertises a payload yet, so the
//! refusal is unreachable today and becomes loud the moment a producer is
//! wired.

use std::sync::Arc;

use novarocks_execution_contract::task_execution::operation::{
    OperationOutcome, TaskDomainReceipt, UpdateQueryContext,
};
use novarocks_proto_models::novarocks as proto;
use novarocks_task_codec::TransportBudget;
use novarocks_task_codec::operation::{
    DecodedOperation, DecodedUpdateQueryContext, encode_abort_cause_field, encode_create_task_ack,
    encode_query_context_ack, encode_query_context_admission_ticket_ack, encode_receipt,
    encode_release_ack, encode_update_task_ack,
};
use novarocks_task_codec::status::encode_task_status;
use novarocks_types::NativeCompatibilityId;

use crate::task_protocol::{
    TaskExecutionIngress, TaskIngressTiming, TaskObservationReader, TaskOperationBatchApplier,
    TaskOperationReceiptAck as ReceiptAck, TaskResultRead, TaskResultReadError,
    TaskResultReadRequest, TaskResultReader, TaskStatusEventStream, TaskStatusSubscriptionReader,
    apply_task_control_operations, apply_task_control_operations_at, apply_task_operations,
    apply_task_operations_at, encode_operation_receipt, fetch_task_dynamic_filters,
    fetch_task_result, fetch_task_result_with_ownership, get_final_task_info,
    host_rejection_status, subscribe_task_status,
};
use crate::task_protocol_fault as fault;
use novarocks_worker::{
    AdmissionTicketObservation, RootResultRoute, StatusAdvance, TaskExecutionRegistry,
};

/// The wire adapter of one backend's task protocol owner.
pub struct RegistryTaskExecutionIngress {
    registry: Arc<TaskExecutionRegistry>,
    native_compatibility_id: NativeCompatibilityId,
}

impl RegistryTaskExecutionIngress {
    pub fn new(
        registry: Arc<TaskExecutionRegistry>,
        native_compatibility_id: NativeCompatibilityId,
    ) -> Arc<Self> {
        assert!(
            registry.config().max_tasks_per_context
                <= TransportBudget::DEFAULT.max_tasks_per_context(),
            "task registry context bound exceeds the native transport contract"
        );
        Arc::new(Self {
            registry,
            native_compatibility_id,
        })
    }

    /// Dispatches one decoded item and encodes its receipt.
    ///
    /// Nothing here is shared with the item before or after it: the owner
    /// linearizes each operation on its own, so this is a plain per-item call.
    /// The item is owned so that a create's input can be moved to the owner,
    /// which hands it to its creation winner or drops it unread.
    fn apply_one(
        &self,
        operation: DecodedOperation,
        local_wait_cap: std::time::Duration,
    ) -> Result<proto::TaskOperationReceipt, tonic::Status> {
        let compatibility = match &operation {
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
                    .acquire_query_context_admission_ticket(request);
                encode_operation_receipt(&receipt, |ack| {
                    Some(ReceiptAck::QueryContextAdmissionTicket(
                        encode_query_context_admission_ticket_ack(*ack),
                    ))
                })
            }
            DecodedOperation::CreateTask(decoded) => {
                // The request is what the owner decides on; the input is the
                // body only this identity's creation winner will interpret.
                let (request, input) = decoded.into_parts();
                let identity = request.identity();
                let receipt =
                    self.registry
                        .create_task_with_local_wait_cap(&request, input, local_wait_cap);
                // Claimed after the owner applied it: the task is admitted and
                // running, and only this answer is lost.
                fault::create_task_ack_dropped(identity, receipt.outcome())?;
                let mut encoded = encode_operation_receipt(&receipt, |ack| {
                    encode_create_task_ack(ack).map(ReceiptAck::CreateTask)
                })?;
                // The two wire-value faults are claimed on the encoded answer,
                // because the value each one misstates exists only there: the
                // owner's receipt is a validated neutral value whose identity
                // and verdict cannot be made to disagree with each other.
                //
                // The identity forgery goes first. The rejection rewrite drops
                // the acknowledgement body, as a genuine rejection has none, so
                // the reverse order would leave the identity fault nothing to
                // forge and silently consume its arming.
                fault::create_task_receipt_foreign_task(identity, receipt.outcome(), &mut encoded)?;
                fault::create_task_rejected_after_apply(identity, receipt.outcome(), &mut encoded)?;
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
                encode_operation_receipt(&receipt, |ack| {
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
                if matches!(&neutral, UpdateQueryContext::Establish(_)) {
                    // Arm before the registry releases its creation gate. A
                    // create may already be waiting there, and must observe
                    // the runner-owned process-loss rendezvous when it wakes.
                    fault::arm_restart_after_establish_context(context)?;
                }
                let receipt = self
                    .registry
                    .update_query_context_with_local_wait_cap(&neutral, local_wait_cap);
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
                encode_operation_receipt(&receipt, |ack| {
                    encode_query_context_ack(ack, cause).map(ReceiptAck::QueryContext)
                })
            }
            DecodedOperation::CancelTask(request) => {
                let receipt = self.registry.cancel_task(&request);
                encode_operation_receipt(&receipt, |ack| {
                    Some(ReceiptAck::CancelTask(encode_task_status(ack)))
                })
            }
            DecodedOperation::AbortQueryContext(request) => {
                let receipt = self.registry.abort_query_context(&request);
                let cause = self.registry.termination_cause(request.context());
                encode_operation_receipt(&receipt, |ack| {
                    encode_query_context_ack(ack, cause).map(ReceiptAck::QueryContext)
                })
            }
            DecodedOperation::ReleaseQueryContext(request) => {
                let receipt = self.registry.release_query_context(&request);
                // Read after the release settled: the completion pass inside
                // it is what hands the shared facts back to the host and seals
                // this evidence.
                let evidence = self.registry.released_context_evidence(request.context());
                let runtime_filter =
                    crate::task_shared_facts::release_runtime_filter_telemetry(&evidence)
                        .map_err(host_rejection_status)?;
                encode_operation_receipt(&receipt, |ack| {
                    let mut encoded = encode_release_ack(
                        ack.context(),
                        ack.release(),
                        ack.state(),
                        runtime_filter.as_ref(),
                    )?;
                    encoded.termination_cause =
                        ack.termination_cause().map(encode_abort_cause_field);
                    Some(ReceiptAck::ReleaseQueryContext(encoded))
                })
            }
        }
    }
}

#[tonic::async_trait]
impl TaskResultReader for RegistryTaskExecutionIngress {
    async fn read_task_result(
        &self,
        request: TaskResultReadRequest,
        ownership: Option<Arc<crate::native_ingress::NativeIngressOwnership>>,
    ) -> Result<TaskResultRead, TaskResultReadError> {
        use crate::task_result_diagnostics::emit_task_fetch_marker;
        use novarocks_worker::result_buffer::{
            TryFetchTypedResult, replays_task_terminal_ack, wait_fetch_task_typed,
        };
        use proto::fetch_result_response::Status as FetchStatus;

        let identity = request.identity();
        let acknowledged = request.acknowledged_packet_sequence();
        // The registry uses a synchronous mutex. Route only this narrow
        // lookup through the blocking pool; the result-buffer wait below
        // remains asynchronous on the listener runtime.
        let registry = Arc::clone(&self.registry);
        let queued_at = std::time::Instant::now();
        let route = tokio::task::spawn_blocking(move || {
            crate::backend_metrics::native_blocking_queue_wait(
                "fetch_task_result_route",
                queued_at.elapsed(),
            );
            let _ownership = ownership;
            registry.root_result_route(identity)
        })
        .await
        .map_err(|error| TaskResultReadError::new(format!("root result route failed: {error}")))?;
        let binding = match route {
            RootResultRoute::Serve(binding) => binding,
            RootResultRoute::TerminalResultOwner(_)
                if replays_task_terminal_ack(identity, acknowledged) =>
            {
                let packet_sequence =
                    acknowledged.expect("an exact terminal replay carries its sequence");
                emit_task_fetch_marker(identity, FetchStatus::Eof, packet_sequence, true, 0);
                return Ok(TaskResultRead::EndOfStream { packet_sequence });
            }
            route => {
                let detail = route
                    .refusal_detail()
                    .expect("only a served route has no refusal detail");
                // A refusal fails the frontend's read, and the frontend sees only this
                // text. Recording it here is what attributes it to a backend process
                // and to the route that produced it: a coordinator log alone cannot
                // say which of this backend's task states the poll landed in.
                tracing::warn!(
                    task = %identity,
                    route = ?route,
                    "root result poll refused"
                );
                emit_task_fetch_marker(identity, FetchStatus::Error, 0, false, 0);
                return Ok(TaskResultRead::Error { detail });
            }
        };
        Ok(
            match wait_fetch_task_typed(
                identity,
                acknowledged,
                request.max_wait(),
                request.max_result_bytes(),
            )
            .await
            {
                TryFetchTypedResult::Ready(result) => {
                    emit_task_fetch_marker(
                        identity,
                        FetchStatus::Ready,
                        result.packet_seq,
                        result.eos,
                        result.payload.len(),
                    );
                    TaskResultRead::Ready {
                        packet_sequence: result.packet_seq,
                        end_of_stream: result.eos,
                        payload: result.payload,
                    }
                }
                TryFetchTypedResult::EndAcknowledged => {
                    let advance = binding.note_result_stream_drained();
                    if matches!(
                        advance,
                        StatusAdvance::Illegal { .. }
                            | StatusAdvance::Rejected(_)
                            | StatusAdvance::VersionExhausted
                    ) {
                        return Err(TaskResultReadError::new(format!(
                            "root task {identity} could not record its acknowledged result drain: {advance:?}"
                        )));
                    }
                    let packet_sequence =
                        acknowledged.expect("an acknowledged end carries its packet sequence");
                    emit_task_fetch_marker(identity, FetchStatus::Eof, packet_sequence, true, 0);
                    TaskResultRead::EndOfStream { packet_sequence }
                }
                TryFetchTypedResult::NotReady => TaskResultRead::NotReady,
                TryFetchTypedResult::Error(error) => {
                    emit_task_fetch_marker(identity, FetchStatus::Error, 0, false, 0);
                    TaskResultRead::Error {
                        detail: error.message,
                    }
                }
            },
        )
    }
}

impl TaskObservationReader for RegistryTaskExecutionIngress {
    fn read_task_dynamic_filters(
        &self,
        request: &novarocks_execution_contract::task_execution::operation::FetchTaskDynamicFilters,
    ) -> novarocks_worker::DynamicFilterReadOutcome {
        self.registry.fetch_task_dynamic_filters(request)
    }

    fn read_final_task_info(
        &self,
        request: &novarocks_execution_contract::task_execution::operation::GetFinalTaskInfo,
    ) -> novarocks_worker::FinalTaskInfoOutcome {
        self.registry.get_final_task_info(request)
    }
}

impl TaskStatusSubscriptionReader for RegistryTaskExecutionIngress {
    fn task_status_source(
        &self,
        context: novarocks_execution_contract::task_execution::identity::QueryContextRef,
    ) -> Option<Arc<novarocks_worker::TaskStatusSource>> {
        self.registry.status_source(context)
    }
}

impl TaskOperationBatchApplier for RegistryTaskExecutionIngress {
    fn apply_task_operation(
        &self,
        operation: DecodedOperation,
        local_wait_cap: std::time::Duration,
    ) -> Result<proto::TaskOperationReceipt, tonic::Status> {
        self.apply_one(operation, local_wait_cap)
    }

    fn observe_admission_ticket(
        &self,
        ticket_id: novarocks_execution_contract::AdmissionTicketId,
        context: novarocks_execution_contract::QueryContextRef,
    ) -> AdmissionTicketObservation {
        self.registry.observe_admission_ticket(ticket_id, context)
    }

    fn preflight_expired_establish_ticket(
        &self,
        operation_id: novarocks_execution_contract::TaskOperationId,
        ticket_id: novarocks_execution_contract::AdmissionTicketId,
        context: novarocks_execution_contract::QueryContextRef,
        native_compatibility_id: Option<&proto::NativeCompatibilityId>,
    ) -> Result<Option<proto::TaskOperationReceipt>, tonic::Status> {
        let Some(native_compatibility_id) = native_compatibility_id else {
            return Ok(None);
        };
        let Ok(native_compatibility_id) =
            NativeCompatibilityId::try_from_slice(&native_compatibility_id.value)
        else {
            return Ok(None);
        };
        if native_compatibility_id != self.native_compatibility_id {
            // The existing compatibility verdict precedes Worker admission;
            // let the fully decoded path preserve that priority.
            return Ok(None);
        }
        self.registry
            .preflight_expired_establish_ticket(operation_id, context, ticket_id)
            .as_ref()
            .map(|receipt| encode_operation_receipt(receipt, |_| None))
            .transpose()
    }
}

#[tonic::async_trait]
impl TaskExecutionIngress for RegistryTaskExecutionIngress {
    #[cfg(debug_assertions)]
    fn with_registry_lock_for_test(
        &self,
        callback: &mut dyn FnMut() -> Result<(), tonic::Status>,
    ) -> Result<(), tonic::Status> {
        self.registry.with_registry_lock_for_test(callback)
    }

    fn apply_task_operations(
        &self,
        request: proto::ApplyTaskOperationsRequest,
    ) -> Result<proto::ApplyTaskOperationsResponse, tonic::Status> {
        apply_task_operations(self, request)
    }

    fn apply_task_operations_at(
        &self,
        request: proto::ApplyTaskOperationsRequest,
        timing: TaskIngressTiming,
    ) -> Result<proto::ApplyTaskOperationsResponse, tonic::Status> {
        apply_task_operations_at(self, request, timing)
    }

    fn apply_task_control_operations(
        &self,
        request: proto::ApplyTaskControlOperationsRequest,
    ) -> Result<proto::ApplyTaskOperationsResponse, tonic::Status> {
        apply_task_control_operations(self, request)
    }

    fn apply_task_control_operations_at(
        &self,
        request: proto::ApplyTaskControlOperationsRequest,
        timing: TaskIngressTiming,
    ) -> Result<proto::ApplyTaskOperationsResponse, tonic::Status> {
        apply_task_control_operations_at(self, request, timing)
    }

    fn subscribe_task_status(
        &self,
        request: proto::SubscribeTaskStatusRequest,
    ) -> Result<TaskStatusEventStream, tonic::Status> {
        subscribe_task_status(self, request)
    }

    fn fetch_task_dynamic_filters(
        &self,
        request: proto::FetchTaskDynamicFiltersRequest,
    ) -> Result<proto::FetchTaskDynamicFiltersResponse, tonic::Status> {
        fetch_task_dynamic_filters(self, request)
    }

    fn get_final_task_info(
        &self,
        request: proto::GetFinalTaskInfoRequest,
    ) -> Result<proto::GetFinalTaskInfoResponse, tonic::Status> {
        get_final_task_info(self, request)
    }

    async fn fetch_task_result(
        &self,
        request: proto::FetchTaskResultRequest,
    ) -> Result<proto::FetchResultResponse, tonic::Status> {
        fetch_task_result(self, request).await
    }

    async fn fetch_task_result_with_ownership(
        &self,
        request: proto::FetchTaskResultRequest,
        ownership: Option<Arc<crate::native_ingress::NativeIngressOwnership>>,
    ) -> Result<proto::FetchResultResponse, tonic::Status> {
        fetch_task_result_with_ownership(self, request, ownership).await
    }
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use novarocks_execution_contract::task_execution::context_convergence::{
        QueryContextConvergenceCursor, QueryContextConvergenceReceipt,
        QueryContextConvergenceState, QueryContextConvergenceVersion,
    };
    use novarocks_execution_contract::task_execution::creation::{
        PreparedTaskFacts, TaskCreationInput,
    };
    use novarocks_execution_contract::task_execution::descriptor::TaskDescriptor;
    use novarocks_execution_contract::task_execution::domain::{
        CodecOwnedContent, ContentFingerprint, DomainVersion,
    };
    use novarocks_execution_contract::task_execution::identity::{
        AdmissionTicketId, QueryContextRef, TaskIdentity, TaskOperationId,
    };
    use novarocks_execution_contract::task_execution::operation::{
        QueryContextDomainUpdate, TaskDomainUpdate,
    };
    use novarocks_execution_contract::task_execution::status::{
        AbortCause, CancelReason, TaskOutputFacts, TaskState, TaskStatus, TaskStatusCursor,
        TaskStatusVersion,
    };
    use novarocks_execution_contract::task_execution::transition::QueryContextState;
    use novarocks_proto_codec::FieldPath;
    use novarocks_proto_models::{catalog, common, plan};
    use novarocks_task_codec::identity::{
        encode_query_context_ref, encode_task_identity, encode_task_operation_id,
    };
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    use prost::Message;
    use tokio_stream::StreamExt;

    use super::*;
    use novarocks_worker::{
        HostRejection, ManualClock, QueryContextHost, ReleasedContextEvidence, RunnableTask,
        SharedFactsRequest, TaskExecutionHost, TaskStatusReporter, WorkerMonotonicClock,
    };
    use novarocks_worker::{TaskExecutionRegistryConfig, TaskStatusEvent};

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
        fn commit_creation(&self) {}

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

    /// An execution side that accepts every creation whose static plan the
    /// codec can read, so these cases fail only on the protocol boundary they
    /// are about.
    ///
    /// It interprets the winner's static fragment with the one codec decoder
    /// the real host uses, which is where the sink kind the owner acts on
    /// comes from. It also keeps a ledger of the static bytes it was handed,
    /// so a case can prove the host was reached once per winning round and
    /// never for a replay.
    #[derive(Default)]
    struct AcceptingTaskHost {
        /// Kept so a test can publish through the same handle a real task
        /// would use.
        reporters: Mutex<Vec<TaskStatusReporter>>,
        prepared: Mutex<Vec<bytes::Bytes>>,
        domains_applied: AtomicUsize,
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

        fn prepared(&self) -> Vec<bytes::Bytes> {
            self.prepared.lock().expect("prepared").clone()
        }
    }

    impl TaskExecutionHost for AcceptingTaskHost {
        fn close_context_admission(&self, _context: QueryContextRef) {}

        fn retire_context_execution(&self, _context: QueryContextRef) {}

        fn forget_context_admission(&self, _context: QueryContextRef) {}

        fn install_receiver(
            &self,
            _descriptor: &TaskDescriptor,
            input: TaskCreationInput,
        ) -> Result<PreparedTaskFacts, HostRejection> {
            let (fragment, _assignment) = input.into_parts();
            self.prepared
                .lock()
                .expect("prepared")
                .push(fragment.to_bytes());
            let decoded = novarocks_task_codec::creation::decode_static_fragment(
                &fragment,
                FieldPath::root("frozen_fragment"),
            )
            .map_err(|error| {
                HostRejection::new(
                    novarocks_execution_contract::task_execution::status::TaskFailureCategory::Protocol,
                    format!("static fragment is not decodable: {error}"),
                )
            })?;
            Ok(PreparedTaskFacts::new(decoded.sink_kind()))
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
            self.domains_applied.fetch_add(1, Ordering::SeqCst);
            Ok(None)
        }
    }

    struct Fixture {
        ingress: Arc<RegistryTaskExecutionIngress>,
        registry: Arc<TaskExecutionRegistry>,
        clock: Arc<ManualClock>,
        task_host: Arc<AcceptingTaskHost>,
        backend: BackendProcessId,
        frontend: FrontendProcessId,
        native_compatibility_id: NativeCompatibilityId,
    }

    impl Fixture {
        fn new() -> Self {
            let backend = BackendProcessId::new_v7();
            let mut config = TaskExecutionRegistryConfig::for_process(
                backend,
                novarocks_task_codec::TransportBudget::DEFAULT.max_tasks_per_context(),
                novarocks_task_codec::TransportBudget::DEFAULT.max_active_tasks_per_backend(),
            );
            // Nothing here waits on a gate, and no case may depend on
            // elapsed wall time.
            config.gate_poll_interval = Duration::from_secs(3600);
            let task_host = Arc::new(AcceptingTaskHost::default());
            let clock = Arc::new(ManualClock::new());
            let native_compatibility_id = NativeCompatibilityId::new([0x71; 32]);
            let registry = TaskExecutionRegistry::new(
                config,
                Arc::clone(&clock) as Arc<dyn WorkerMonotonicClock>,
                Arc::new(AcceptingContextHost),
                Arc::clone(&task_host) as Arc<dyn TaskExecutionHost>,
                crate::task_execution_observation::backend_task_execution_ports(),
            );
            Self {
                ingress: RegistryTaskExecutionIngress::new(
                    Arc::clone(&registry),
                    native_compatibility_id,
                ),
                registry,
                clock,
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
            let mut receipts = Vec::new();
            let mut pending = Vec::new();
            let mut pending_control = None;
            for operation in operations {
                let control = is_control_operation(&operation);
                if pending_control.is_some_and(|previous| previous != control) {
                    receipts.extend(
                        self.apply_group(std::mem::take(&mut pending), pending_control.unwrap())
                            .receipts,
                    );
                }
                pending_control = Some(control);
                pending.push(operation);
            }
            if let Some(control) = pending_control {
                receipts.extend(self.apply_group(pending, control).receipts);
            }
            proto::ApplyTaskOperationsResponse { receipts }
        }

        fn apply_group(
            &self,
            operations: Vec<proto::TaskOperation>,
            control: bool,
        ) -> proto::ApplyTaskOperationsResponse {
            if control {
                self.ingress
                    .apply_task_control_operations(proto::ApplyTaskControlOperationsRequest {
                        operations: operations.into_iter().map(to_control_operation).collect(),
                    })
                    .expect("a well formed control batch is answered with receipts")
            } else {
                self.ingress
                    .apply_task_operations(proto::ApplyTaskOperationsRequest { operations })
                    .expect("a well formed ordinary batch is answered with receipts")
            }
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

    fn is_control_operation(operation: &proto::TaskOperation) -> bool {
        match operation.operation.as_ref() {
            Some(proto::task_operation::Operation::UpdateQueryContext(update)) => matches!(
                update.command.as_ref(),
                Some(proto::update_query_context_request::Command::RenewLease(_))
            ),
            Some(proto::task_operation::Operation::CancelTask(_))
            | Some(proto::task_operation::Operation::AbortQueryContext(_))
            | Some(proto::task_operation::Operation::ReleaseQueryContext(_)) => true,
            _ => false,
        }
    }

    fn to_control_operation(operation: proto::TaskOperation) -> proto::TaskControlOperation {
        let control = match operation.operation.expect("a control command") {
            proto::task_operation::Operation::UpdateQueryContext(update) => {
                let Some(proto::update_query_context_request::Command::RenewLease(renew)) =
                    update.command
                else {
                    panic!("only renewal uses control UpdateQueryContext");
                };
                proto::task_control_operation::Control::RenewLease(renew)
            }
            proto::task_operation::Operation::CancelTask(cancel) => {
                proto::task_control_operation::Control::CancelTask(cancel)
            }
            proto::task_operation::Operation::AbortQueryContext(abort) => {
                proto::task_control_operation::Control::AbortQueryContext(abort)
            }
            proto::task_operation::Operation::ReleaseQueryContext(release) => {
                proto::task_control_operation::Control::ReleaseQueryContext(release)
            }
            _ => panic!("ordinary operation cannot be sent to control method"),
        };
        proto::TaskControlOperation {
            envelope: operation.envelope,
            control: Some(control),
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

    /// The two carriers of one create request, built the way the frontend
    /// builds them: the static fragment every task of a fragment shares, and
    /// the task-local creation metadata.
    #[derive(Clone)]
    struct CreateCarriers {
        frozen: proto::FrozenFragment,
        metadata: proto::CreationMetadata,
    }

    impl CreateCarriers {
        /// A single-fragment root task: no exchange topology, one result sink.
        fn root(context: QueryContextRef, identity: TaskIdentity) -> Self {
            Self {
                frozen: proto::FrozenFragment {
                    plan_version: vec![1; 16],
                    plan_contract_revision: 1,
                    fragment_contract_version: 1,
                    pipeline_dop_domain: Some(proto::PipelineDopDomain {
                        min: 2,
                        max: 2,
                        requires_power_of_two: false,
                    }),
                    plan: Some(plan::PlanFragment {
                        fragment_id: 1,
                        sink: Some(plan::DataSink {
                            kind: Some(plan::data_sink::Kind::Result(true)),
                        }),
                        ..Default::default()
                    }),
                },
                metadata: proto::CreationMetadata {
                    query_context: Some(encode_query_context_ref(context)),
                    descriptor: Some(proto::TaskDescriptor {
                        identity: Some(encode_task_identity(identity)),
                        fragment_instance_id: Some(unique(41, 42)),
                        pipeline_dop: 2,
                        split_plan_nodes: Vec::new(),
                        topology: Some(proto::TaskExchangeTopology::default()),
                    }),
                    initial_domains: Vec::new(),
                    assignment: Some(proto::TaskAssignment::default()),
                },
            }
        }

        /// The same task with a data-stream sink: a legitimate exchange
        /// producer, which owns a result buffer but owes the coordinator no
        /// result. Its one outbound edge carries the producer's position.
        fn producer(context: QueryContextRef, identity: TaskIdentity) -> Self {
            let mut carriers = Self::root(context, identity);
            carriers
                .frozen
                .plan
                .as_mut()
                .expect("the fixture carries a plan")
                .sink = Some(plan::DataSink {
                kind: Some(plan::data_sink::Kind::DataStream(plan::DataStreamSink {
                    dest_node_id: 17,
                    output_partition: Some(plan::DataPartition {
                        kind: plan::PartitionKind::Unpartitioned as i32,
                        exprs: Vec::new(),
                    }),
                    ..Default::default()
                })),
            });
            carriers
                .metadata
                .descriptor
                .as_mut()
                .and_then(|descriptor| descriptor.topology.as_mut())
                .expect("the fixture carries a topology")
                .outbound = vec![proto::TaskExchangeEdge {
                edge_id: 1,
                destination_node_id: 17,
                partitioning: proto::ExchangePartitioning::Unpartitioned as i32,
                destinations: vec![proto::TaskExchangeDestination {
                    task: Some(encode_task_identity(identity)),
                    fragment_instance_id: Some(unique(50, 51)),
                    endpoint: Some(proto::QueryControlEndpoint {
                        host: "be.local".to_owned(),
                        port: 8060,
                    }),
                    destination_node_id: 17,
                }],
                sender_ordinal: 0,
                sender_count: 1,
            }];
            carriers
                .metadata
                .assignment
                .as_mut()
                .expect("the fixture carries an assignment")
                .sink_edge_ids = vec![1];
            carriers
        }

        fn frozen_bytes(&self) -> bytes::Bytes {
            self.frozen.encode_to_vec().into()
        }

        fn operation(&self, operation: TaskOperationId) -> proto::TaskOperation {
            create_operation(
                operation,
                self.frozen_bytes(),
                self.metadata.encode_to_vec().into(),
            )
        }
    }

    fn create_operation(
        operation: TaskOperationId,
        frozen_fragment: bytes::Bytes,
        creation_metadata: bytes::Bytes,
    ) -> proto::TaskOperation {
        proto::TaskOperation {
            envelope: Some(envelope(operation)),
            operation: Some(proto::task_operation::Operation::CreateTask(
                proto::CreateTaskRequest {
                    frozen_fragment,
                    creation_metadata,
                },
            )),
        }
    }

    /// A single-fragment root task: no exchange topology, one result sink.
    fn create_task(
        context: QueryContextRef,
        identity: TaskIdentity,
        operation: TaskOperationId,
    ) -> proto::TaskOperation {
        CreateCarriers::root(context, identity).operation(operation)
    }

    /// The same task as an exchange producer.
    fn create_producer_task(
        context: QueryContextRef,
        identity: TaskIdentity,
        operation: TaskOperationId,
    ) -> proto::TaskOperation {
        CreateCarriers::producer(context, identity).operation(operation)
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
    fn ordinary_then_control_methods_preserve_receipt_order() {
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
    fn one_refused_control_item_leaves_renewal_receipt_untouched() {
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
    fn operation_waits_are_measured_from_tower_arrival_per_item() {
        let fixture = Fixture::new();
        let context = fixture.context();
        let capability = fixture.registry.admission_epoch_capability();
        let mut first = acquire_ticket(
            context,
            TaskOperationId::new_v7(),
            fixture.native_compatibility_id,
            capability,
        );
        first.envelope.as_mut().expect("envelope").max_wait_millis = 1_000;
        let mut second = acquire_ticket(
            context,
            TaskOperationId::new_v7(),
            fixture.native_compatibility_id,
            capability,
        );
        second.envelope.as_mut().expect("envelope").max_wait_millis = 30_000;
        let now = Instant::now();
        let response = fixture
            .ingress
            .apply_task_operations_at(
                proto::ApplyTaskOperationsRequest {
                    operations: vec![first, second],
                },
                TaskIngressTiming::new(now - Duration::from_secs(2), now + Duration::from_secs(20)),
            )
            .expect("valid batch has per-item receipts");
        assert_eq!(response.receipts.len(), 2);
        assert_eq!(
            outcome_of(&response.receipts[0]),
            proto::TaskOperationOutcome::OperationTimedOut
        );
        assert_eq!(
            outcome_of(&response.receipts[1]),
            proto::TaskOperationOutcome::Accepted
        );
        assert_eq!(fixture.registry.admission_reservation_count(), 1);
    }

    #[test]
    fn redeemed_ticket_expiry_does_not_block_exact_establish_replay() {
        let fixture = Fixture::new();
        let context = fixture.context();
        let ticket_id = fixture.acquire_ticket(context);
        let operation = establish_with_compatibility(
            context,
            TaskOperationId::new_v7(),
            ticket_id,
            fixture.native_compatibility_id,
        );
        let first = fixture.apply(vec![operation.clone()]);
        assert_eq!(
            outcome_of(&first.receipts[0]),
            proto::TaskOperationOutcome::Accepted
        );

        fixture.clock.advance(Duration::from_secs(11));
        let replay = fixture.apply(vec![operation]);
        assert_eq!(
            outcome_of(&replay.receipts[0]),
            proto::TaskOperationOutcome::Idempotent,
            "ticket issuance expiry cannot reject a redeemed exact replay"
        );
    }

    #[test]
    fn single_expired_establish_is_refused_before_deep_content_decode() {
        let fixture = Fixture::new();
        let context = fixture.context();
        let ticket_id = fixture.acquire_ticket(context);
        fixture.clock.advance(Duration::from_secs(11));
        let mut operation = establish_with_compatibility(
            context,
            TaskOperationId::new_v7(),
            ticket_id,
            fixture.native_compatibility_id,
        );
        let Some(proto::task_operation::Operation::UpdateQueryContext(update)) =
            &mut operation.operation
        else {
            panic!("fixture is an establish operation")
        };
        let Some(proto::update_query_context_request::Command::Establish(establish)) =
            &mut update.command
        else {
            panic!("fixture is an establish command")
        };
        establish
            .initial_lease
            .as_mut()
            .expect("fixture lease")
            .valid_for_millis = 0;
        let response = fixture
            .ingress
            .apply_task_operations(proto::ApplyTaskOperationsRequest {
                operations: vec![operation],
            })
            .expect("Worker can prove the expired ticket verdict first");
        assert_eq!(
            outcome_of(&response.receipts[0]),
            proto::TaskOperationOutcome::InvalidStateOrRequest
        );
        assert_eq!(
            fixture.registry.context_state(context),
            QueryContextState::Absent
        );
    }

    #[test]
    fn expired_establish_in_mixed_batch_keeps_whole_batch_validation() {
        let fixture = Fixture::new();
        let context = fixture.context();
        let ticket_id = fixture.acquire_ticket(context);
        fixture.clock.advance(Duration::from_secs(11));
        let error = fixture
            .ingress
            .apply_task_operations(proto::ApplyTaskOperationsRequest {
                operations: vec![
                    establish_with_compatibility(
                        context,
                        TaskOperationId::new_v7(),
                        ticket_id,
                        fixture.native_compatibility_id,
                    ),
                    proto::TaskOperation {
                        envelope: Some(envelope(TaskOperationId::new_v7())),
                        operation: None,
                    },
                ],
            })
            .expect_err("malformed mixed batch must fail before effects");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            fixture.registry.context_state(context),
            QueryContextState::Absent
        );
    }

    #[test]
    fn expired_establish_in_mixed_batch_keeps_other_item_and_receipt_order() {
        let fixture = Fixture::new();
        let context = fixture.context();
        let ticket_id = fixture.acquire_ticket(context);
        fixture.clock.advance(Duration::from_secs(11));
        let other = fixture.other_context();
        let capability = fixture.registry.admission_epoch_capability();
        let response = fixture
            .ingress
            .apply_task_operations(proto::ApplyTaskOperationsRequest {
                operations: vec![
                    establish_with_compatibility(
                        context,
                        TaskOperationId::new_v7(),
                        ticket_id,
                        fixture.native_compatibility_id,
                    ),
                    acquire_ticket(
                        other,
                        TaskOperationId::new_v7(),
                        fixture.native_compatibility_id,
                        capability,
                    ),
                ],
            })
            .expect("both items have independent receipts");
        assert_eq!(response.receipts.len(), 2);
        assert_eq!(
            outcome_of(&response.receipts[0]),
            proto::TaskOperationOutcome::InvalidStateOrRequest
        );
        assert_eq!(
            outcome_of(&response.receipts[1]),
            proto::TaskOperationOutcome::Accepted
        );
        assert_eq!(
            fixture.registry.context_state(context),
            QueryContextState::Absent
        );
        assert_eq!(fixture.registry.admission_reservation_count(), 1);
    }

    #[test]
    fn later_expired_establish_waits_for_earlier_context_effect() {
        let fixture = Fixture::new();
        let context = fixture.context();
        let expired_ticket = fixture.acquire_ticket(context);
        fixture.clock.advance(Duration::from_secs(11));
        let live_ticket = fixture.acquire_ticket(context);
        let response = fixture.apply(vec![
            establish_with_compatibility(
                context,
                TaskOperationId::new_v7(),
                live_ticket,
                fixture.native_compatibility_id,
            ),
            establish_with_compatibility(
                context,
                TaskOperationId::new_v7(),
                expired_ticket,
                fixture.native_compatibility_id,
            ),
        ]);
        assert_eq!(response.receipts.len(), 2);
        assert_eq!(
            outcome_of(&response.receipts[0]),
            proto::TaskOperationOutcome::Accepted
        );
        assert_eq!(
            outcome_of(&response.receipts[1]),
            proto::TaskOperationOutcome::ContextConflict,
            "the later request must see the context installed by the first item"
        );
    }

    #[test]
    fn expired_ticket_cannot_override_compatibility_or_operation_deadline() {
        let fixture = Fixture::new();
        let context = fixture.context();
        let ticket_id = fixture.acquire_ticket(context);
        fixture.clock.advance(Duration::from_secs(11));

        let foreign = establish_with_compatibility(
            context,
            TaskOperationId::new_v7(),
            ticket_id,
            NativeCompatibilityId::new([0x99; 32]),
        );
        let response = fixture.apply(vec![foreign]);
        assert_eq!(
            outcome_of(&response.receipts[0]),
            proto::TaskOperationOutcome::CompatibilityMismatch
        );

        let expired_operation = establish_with_compatibility(
            context,
            TaskOperationId::new_v7(),
            ticket_id,
            fixture.native_compatibility_id,
        );
        let now = Instant::now();
        let response = fixture
            .ingress
            .apply_task_operations_at(
                proto::ApplyTaskOperationsRequest {
                    operations: vec![expired_operation],
                },
                TaskIngressTiming::new(now - Duration::from_secs(6), now + Duration::from_secs(20)),
            )
            .expect("expired operation has a typed timeout receipt");
        assert_eq!(
            outcome_of(&response.receipts[0]),
            proto::TaskOperationOutcome::OperationTimedOut
        );
    }

    #[test]
    fn malformed_establish_wait_is_rejected_before_ticket_observation() {
        struct NoTicketAccess;

        impl TaskOperationBatchApplier for NoTicketAccess {
            fn apply_task_operation(
                &self,
                _operation: DecodedOperation,
                _local_wait_cap: Duration,
            ) -> Result<proto::TaskOperationReceipt, tonic::Status> {
                panic!("invalid wait must not reach the Worker")
            }

            fn observe_admission_ticket(
                &self,
                _ticket_id: AdmissionTicketId,
                _context: QueryContextRef,
            ) -> AdmissionTicketObservation {
                panic!("invalid wait must not acquire the ticket lock")
            }

            fn preflight_expired_establish_ticket(
                &self,
                _operation_id: TaskOperationId,
                _ticket_id: AdmissionTicketId,
                _context: QueryContextRef,
                _native_compatibility_id: Option<&proto::NativeCompatibilityId>,
            ) -> Result<Option<proto::TaskOperationReceipt>, tonic::Status> {
                panic!("invalid wait must not reach the Worker")
            }
        }

        let fixture = Fixture::new();
        let context = fixture.context();
        let ticket_id = fixture.acquire_ticket(context);
        for invalid_wait in [0, 300_001] {
            let mut operation = establish_with_compatibility(
                context,
                TaskOperationId::new_v7(),
                ticket_id,
                fixture.native_compatibility_id,
            );
            operation
                .envelope
                .as_mut()
                .expect("envelope")
                .max_wait_millis = invalid_wait;
            let error = apply_task_operations_at(
                &NoTicketAccess,
                proto::ApplyTaskOperationsRequest {
                    operations: vec![operation],
                },
                TaskIngressTiming::starting_now(),
            )
            .expect_err("invalid wait is a protocol error");
            assert_eq!(error.code(), tonic::Code::InvalidArgument);
        }
    }

    #[test]
    fn ordinary_method_rejects_control_before_registry_mutation() {
        let fixture = Fixture::new();
        let context = fixture.context();
        fixture.apply(vec![fixture.establish(context, TaskOperationId::new_v7())]);
        let error = fixture
            .ingress
            .apply_task_operations(proto::ApplyTaskOperationsRequest {
                operations: vec![renew_lease(context, TaskOperationId::new_v7(), 1)],
            })
            .expect_err("renewal must use the control method");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert!(error.message().contains("requires the control method"));
        assert_eq!(
            fixture.registry.context_state(context),
            QueryContextState::Active
        );
    }

    #[test]
    fn a_credential_announcement_reaches_structural_decode_on_any_transport() {
        // There used to be a confidentiality gate in front of this, because
        // the domain carried vended material and material was legal only on an
        // encrypted transport. Nothing carries material now (CAD-1 D1), so an
        // announcement must be treated identically everywhere and the ordinary
        // structural decoder must be the thing that answers.
        let request = proto::ApplyTaskOperationsRequest {
            operations: vec![proto::TaskOperation {
                // Intentionally malformed, so the reported error names the
                // check that actually ran.
                envelope: None,
                operation: Some(proto::task_operation::Operation::UpdateQueryContext(
                    proto::UpdateQueryContextRequest {
                        command: Some(proto::update_query_context_request::Command::Establish(
                            proto::EstablishQueryContextRequest {
                                initial_credential: Some(proto::QueryContextCredentialDomain {
                                    lease_id: 1,
                                    epoch: 1,
                                    descriptors: Vec::new(),
                                }),
                                ..Default::default()
                            },
                        )),
                    },
                )),
            }],
        };

        let error = Fixture::new()
            .ingress
            .apply_task_operations(request)
            .expect_err("the intentionally malformed request must reach structural decode");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert!(
            error.message().contains("requires an envelope"),
            "ingress did not reach structural decode: {}",
            error.message()
        );
        assert!(
            !error.message().contains("confidential"),
            "ingress still speaks of a transport gate that no longer exists: {}",
            error.message()
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
        let convergence = QueryContextConvergenceReceipt::new(
            context,
            QueryContextConvergenceVersion::FIRST,
            QueryContextConvergenceState::WorkerStoppedAndContextFenced,
        );
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
        source.publish_context_convergence(convergence);

        let mut stream = fixture
            .ingress
            .subscribe_task_status(proto::SubscribeTaskStatusRequest {
                query_context: Some(encode_query_context_ref(context)),
                cursors: vec![novarocks_task_codec::status::encode_task_status_cursor(
                    TaskStatusCursor::unobserved(identity),
                )],
                context_convergence_cursor: None,
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
        assert_eq!(
            source
                .subscribe_context_aware(
                    context,
                    &[],
                    Some(QueryContextConvergenceCursor::unobserved(context)),
                )
                .expect("an aware subscriber can still catch up"),
            vec![TaskStatusEvent::ContextConvergence(convergence)],
            "a legacy cursor-free stream must not consume context convergence"
        );
    }

    #[tokio::test]
    async fn overlapping_server_streams_observe_the_same_terminal_status() {
        let fixture = Fixture::new();
        let context = fixture.context();
        let identity = fixture.identity(1, 1);
        fixture.apply(vec![
            fixture.establish(context, TaskOperationId::new_v7()),
            create_task(context, identity, TaskOperationId::new_v7()),
        ]);

        let request = || proto::SubscribeTaskStatusRequest {
            query_context: Some(encode_query_context_ref(context)),
            cursors: vec![novarocks_task_codec::status::encode_task_status_cursor(
                TaskStatusCursor::unobserved(identity),
            )],
            context_convergence_cursor: None,
        };
        let mut old_stream = fixture
            .ingress
            .subscribe_task_status(request())
            .expect("the old stream opens");
        let mut replacement_stream = fixture
            .ingress
            .subscribe_task_status(request())
            .expect("the replacement stream opens");

        // Drain both retained catch-up frames before publishing the live
        // terminal. This is the overlap window created while HTTP/2
        // cancellation of the old handler is still in flight.
        assert_eq!(next_status_version(&mut old_stream).await, 1);
        assert_eq!(next_status_version(&mut replacement_stream).await, 1);

        let terminal = TaskStatus::try_new(
            identity,
            TaskStatusVersion::FIRST.next().expect("version two"),
            TaskState::Finished,
            None,
            TaskOutputFacts::new(true),
        )
        .expect("finished status carries complete output responsibility");
        let source = fixture
            .registry
            .status_source(context)
            .expect("the context retains its observation source");
        source.publish(terminal);
        source.mark_gone(identity);

        assert_eq!(next_status_version(&mut old_stream).await, 2);
        assert_eq!(
            next_status_version(&mut replacement_stream).await,
            2,
            "the old handler cannot consume the terminal retained for its replacement"
        );
        next_gone_identity(&mut old_stream, identity).await;
        next_gone_identity(&mut replacement_stream, identity).await;
    }

    #[tokio::test]
    async fn a_context_aware_subscription_catches_up_and_rejects_a_future_cursor() {
        let fixture = Fixture::new();
        let context = fixture.context();
        fixture.apply(vec![fixture.establish(context, TaskOperationId::new_v7())]);
        let source = fixture
            .registry
            .status_source(context)
            .expect("an active context has an observation channel");
        let receipt = QueryContextConvergenceReceipt::new(
            context,
            QueryContextConvergenceVersion::FIRST,
            QueryContextConvergenceState::WorkerStoppedAndContextFenced,
        );
        source.publish_context_convergence(receipt);

        let mut stream = fixture
            .ingress
            .subscribe_task_status(proto::SubscribeTaskStatusRequest {
                query_context: Some(encode_query_context_ref(context)),
                cursors: Vec::new(),
                context_convergence_cursor: Some(
                    novarocks_task_codec::context_convergence::encode_query_context_convergence_cursor(
                        QueryContextConvergenceCursor::unobserved(context),
                    ),
                ),
            })
            .expect("an unobserved context cursor opens a subscription");
        let event = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("the retained convergence frame is delivered")
            .expect("the stream remains open")
            .expect("the frame is valid");
        let proto::task_status_stream_event::Event::ContextConvergence(encoded) =
            event.event.expect("a frame carries a body")
        else {
            panic!("the context cursor must receive context convergence");
        };
        assert_eq!(
            novarocks_task_codec::context_convergence::decode_query_context_convergence_receipt(
                &encoded,
                FieldPath::root("context_convergence"),
            )
            .expect("the backend encoded its exact receipt"),
            receipt
        );
        drop(stream);

        let mut replacement = fixture
            .ingress
            .subscribe_task_status(proto::SubscribeTaskStatusRequest {
                query_context: Some(encode_query_context_ref(context)),
                cursors: Vec::new(),
                context_convergence_cursor: Some(
                    novarocks_task_codec::context_convergence::encode_query_context_convergence_cursor(
                        QueryContextConvergenceCursor::unobserved(context),
                    ),
                ),
            })
            .expect("a replacement stream owns an independent cursor");
        let event = tokio::time::timeout(Duration::from_secs(5), replacement.next())
            .await
            .expect("the replacement stream receives the retained receipt")
            .expect("the replacement stream remains open")
            .expect("the replacement frame is valid");
        assert!(matches!(
            event.event,
            Some(proto::task_status_stream_event::Event::ContextConvergence(
                _
            ))
        ));
        drop(replacement);

        let future = QueryContextConvergenceCursor::at(
            context,
            QueryContextConvergenceVersion::FIRST
                .next()
                .expect("version two"),
        );
        let Err(error) = fixture
            .ingress
            .subscribe_task_status(proto::SubscribeTaskStatusRequest {
            query_context: Some(encode_query_context_ref(context)),
            cursors: Vec::new(),
            context_convergence_cursor: Some(
                novarocks_task_codec::context_convergence::encode_query_context_convergence_cursor(
                    future,
                ),
            ),
        }) else {
            panic!("a future cursor must fail closed");
        };
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
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
                context_convergence_cursor: None,
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
                context_convergence_cursor: None,
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
            proto::task_status_stream_event::Event::ContextConvergence(_) => {
                panic!("expected a task status frame, not context convergence")
            }
        }
    }

    async fn next_gone_identity(stream: &mut TaskStatusEventStream, expected: TaskIdentity) {
        let event = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("a retained gone frame must wake the subscription")
            .expect("the stream is still open")
            .expect("an observation frame is never a status error");
        match novarocks_task_codec::operation::decode_context_aware_status_event(
            &event,
            FieldPath::root("task_status_stream_event"),
        )
        .expect("the backend encoded a valid observation")
        {
            novarocks_task_codec::operation::ContextAwareStatusStreamEvent::Gone(identity) => {
                assert_eq!(identity, expected);
            }
            novarocks_task_codec::operation::ContextAwareStatusStreamEvent::Status(_) => {
                panic!("expected a reclamation frame, not a status")
            }
            novarocks_task_codec::operation::ContextAwareStatusStreamEvent::ContextConvergence(
                _,
            ) => {
                panic!("expected a task reclamation frame, not context convergence")
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

    // ------------------------------------------------- create identity replay

    /// A create that names an existing identity is answered by that task.
    ///
    /// The replay below changes everything a create body can carry and still
    /// passes the codec: a static plan whose sink is an exchange producer, a
    /// different kernel key, parallelism and DOP domain, an outbound edge and
    /// its binding, another instance ordinal, an initial scan assignment, and
    /// an initial edge-open domain. It is answered with the original
    /// acknowledgement, correlated to its own operation id, and none of it
    /// reaches the execution host.
    #[test]
    fn a_create_replay_with_a_changed_body_is_answered_from_the_original_task() {
        let fixture = Fixture::new();
        let context = fixture.context();
        let identity = fixture.identity(5, 5);
        let original = CreateCarriers::root(context, identity);
        let response = fixture.apply(vec![
            fixture.establish(context, TaskOperationId::new_v7()),
            original.operation(TaskOperationId::new_v7()),
        ]);
        assert_eq!(
            outcome_of(&response.receipts[1]),
            proto::TaskOperationOutcome::Accepted,
            "{:?}",
            response.receipts[1]
        );
        let original_ack = response.receipts[1]
            .ack
            .clone()
            .expect("an accepted create is acknowledged");

        let mut changed = CreateCarriers::producer(context, identity);
        changed.frozen.pipeline_dop_domain = Some(proto::PipelineDopDomain {
            min: 1,
            max: 4,
            requires_power_of_two: false,
        });
        let descriptor = changed
            .metadata
            .descriptor
            .as_mut()
            .expect("the fixture carries a descriptor");
        descriptor.pipeline_dop = 3;
        descriptor.fragment_instance_id = Some(unique(77, 78));
        let assignment = changed
            .metadata
            .assignment
            .as_mut()
            .expect("the fixture carries an assignment");
        assignment.instance_ordinal = 7;
        assignment.initial_scan_ranges = vec![proto::TaskScanRanges {
            plan_node_id: 3,
            ranges: Vec::new(),
        }];
        changed.metadata.initial_domains = vec![proto::TaskDomainUpdate {
            domain: Some(proto::task_domain_update::Domain::OpenExchangeEdges(
                proto::OpenExchangeEdgesDomain {
                    version: 1,
                    edge_ids: vec![1],
                },
            )),
        }];
        let replay_operation = TaskOperationId::new_v7();
        let replay = fixture.apply(vec![changed.operation(replay_operation)]);
        assert_eq!(
            outcome_of(&replay.receipts[0]),
            proto::TaskOperationOutcome::Idempotent,
            "{:?}",
            replay.receipts[0]
        );
        assert_eq!(
            replay.receipts[0].ack.as_ref(),
            Some(&original_ack),
            "the replay is answered with the original entity receipt"
        );
        assert_eq!(
            replay.receipts[0].operation_id,
            Some(encode_task_operation_id(replay_operation)),
            "the answer correlates the replay's own operation"
        );

        assert_eq!(
            fixture.task_host.prepared(),
            vec![original.frozen_bytes()],
            "only the winner's static fragment was ever interpreted"
        );
        assert_eq!(
            fixture.task_host.domains_applied.load(Ordering::SeqCst),
            0,
            "the replay's initial domain was never applied"
        );
        let RootResultRoute::Serve(binding) = fixture.registry.root_result_route(identity) else {
            panic!("the original result owner still serves its result");
        };
        assert_eq!(
            binding.kernel_key(),
            novarocks_types::UniqueId::new(41, 42),
            "the original kernel key still governs the task"
        );
    }

    /// A static fragment is read only by the round that wins its identity.
    ///
    /// The ingress bounds the static carrier but does not decode it, so an
    /// unreadable one reaches the owner. As a first creation it is the
    /// winner's body and is refused, and the refused round leaves nothing
    /// behind; once a legal body has won, the same unreadable carrier is only
    /// a replay of that identity and is never read at all.
    #[test]
    fn an_unreadable_static_fragment_is_refused_only_when_it_would_be_interpreted() {
        let fixture = Fixture::new();
        let context = fixture.context();
        let identity = fixture.identity(6, 6);
        let legal = CreateCarriers::root(context, identity);
        let unreadable = || {
            create_operation(
                TaskOperationId::new_v7(),
                bytes::Bytes::from_static(&[0x0a, 0x80]),
                legal.metadata.encode_to_vec().into(),
            )
        };

        let refused = fixture.apply(vec![
            fixture.establish(context, TaskOperationId::new_v7()),
            unreadable(),
        ]);
        assert_eq!(
            outcome_of(&refused.receipts[1]),
            proto::TaskOperationOutcome::InvalidStateOrRequest,
            "{:?}",
            refused.receipts[1]
        );
        assert!(refused.receipts[1].ack.is_none());
        assert!(
            !fixture.registry.has_live_task(identity),
            "the refused first round was rolled back completely"
        );

        let accepted = fixture.apply(vec![legal.operation(TaskOperationId::new_v7())]);
        assert_eq!(
            outcome_of(&accepted.receipts[0]),
            proto::TaskOperationOutcome::Accepted,
            "{:?}",
            accepted.receipts[0]
        );

        let replay = fixture.apply(vec![unreadable()]);
        assert_eq!(
            outcome_of(&replay.receipts[0]),
            proto::TaskOperationOutcome::Idempotent,
            "{:?}",
            replay.receipts[0]
        );
        assert_eq!(replay.receipts[0].ack, accepted.receipts[0].ack);
        assert_eq!(
            fixture.task_host.prepared(),
            vec![
                bytes::Bytes::from_static(&[0x0a, 0x80]),
                legal.frozen_bytes()
            ],
            "each winning round was interpreted once and the replay never was"
        );
    }

    /// A replay is not a way past the codec: metadata that breaks its local
    /// structure is refused for every request, before any owner decides.
    #[test]
    fn a_replay_with_malformed_metadata_is_refused_without_touching_the_original_task() {
        let fixture = Fixture::new();
        let context = fixture.context();
        let identity = fixture.identity(7, 7);
        let original = CreateCarriers::root(context, identity);
        let response = fixture.apply(vec![
            fixture.establish(context, TaskOperationId::new_v7()),
            original.operation(TaskOperationId::new_v7()),
        ]);
        assert_eq!(
            outcome_of(&response.receipts[1]),
            proto::TaskOperationOutcome::Accepted
        );

        let mut malformed = original.clone();
        malformed
            .metadata
            .assignment
            .as_mut()
            .expect("the fixture carries an assignment")
            .sink_edge_ids = vec![9];
        let error = fixture
            .ingress
            .apply_task_operations(proto::ApplyTaskOperationsRequest {
                operations: vec![malformed.operation(TaskOperationId::new_v7())],
            })
            .expect_err("an assignment binding an edge the topology never froze is malformed");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert!(
            error.message().contains("sink_edge_ids"),
            "unexpected message: {}",
            error.message()
        );

        assert!(fixture.registry.has_live_task(identity));
        assert_eq!(fixture.task_host.prepared().len(), 1);
        let replay = fixture.apply(vec![original.operation(TaskOperationId::new_v7())]);
        assert_eq!(
            outcome_of(&replay.receipts[0]),
            proto::TaskOperationOutcome::Idempotent
        );
        assert_eq!(replay.receipts[0].ack, response.receipts[1].ack);
    }

    /// The receipt belongs to the exact context that created the task. The
    /// same task identity under a replaced frontend incarnation is fenced
    /// rather than answered with it.
    #[test]
    fn a_replay_under_another_frontend_process_reads_no_receipt() {
        let fixture = Fixture::new();
        let context = fixture.context();
        let identity = fixture.identity(8, 8);
        let response = fixture.apply(vec![
            fixture.establish(context, TaskOperationId::new_v7()),
            create_task(context, identity, TaskOperationId::new_v7()),
        ]);
        assert_eq!(
            outcome_of(&response.receipts[1]),
            proto::TaskOperationOutcome::Accepted
        );

        let replaced = QueryContextRef::new(
            fixture.execution(),
            FrontendProcessId::new_v7(),
            fixture.backend,
        );
        let fenced = fixture.apply(vec![create_task(
            replaced,
            identity,
            TaskOperationId::new_v7(),
        )]);
        assert_eq!(
            outcome_of(&fenced.receipts[0]),
            proto::TaskOperationOutcome::IdentityMismatch,
            "{:?}",
            fenced.receipts[0]
        );
        assert!(fenced.receipts[0].ack.is_none());
        assert_eq!(fixture.task_host.prepared().len(), 1);
    }
}
