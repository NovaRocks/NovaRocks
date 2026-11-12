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

//! Original supervised RPC execution and projection observations.

use super::*;

/// The original client and first-wins receipts; its fixed record owns reservations.
pub(super) struct ApplySend {
    pub(super) client: Client,
    pub(super) receipts: AcceptedOperationReceipts,
    pub(super) physical: Option<NativeCallSource>,
    pub(super) action_tail: Option<novarocks_resource_contract::ExistingOwnerRetentionPin>,
    pub(super) reply_plan: novarocks_task_codec::resource_preflight::OperationReplyDecodePlan,
}

/// First-wins settlement for every operation an accepted send owns.
///
/// Dropping the original send future is an unknown transport outcome, including
/// role shutdown and task abort. Its record waits for actual task join and
/// actual protected destructor join before returning process reservations. The guard publishes that fact for every operation
/// not already settled before releasing the process reservations, so an owner
/// can never remain in flight merely because its transport future disappeared.
pub(super) struct AcceptedOperationReceipts {
    pub(super) acks: TaskAckIntakeHandle,
    pub(super) pending: VecDeque<SentOperation>,
    pub(super) canonical: Option<crate::native::apply_send_owner::OriginalReplyPublisher>,
}

impl AcceptedOperationReceipts {
    pub(super) fn new(acks: TaskAckIntakeHandle, sent: Vec<SentOperation>) -> Self {
        Self {
            acks,
            pending: sent.into(),
            canonical: None,
        }
    }
    pub(super) fn new_canonical(
        acks: TaskAckIntakeHandle,
        sent: Vec<SentOperation>,
        canonical: crate::native::apply_send_owner::OriginalReplyPublisher,
    ) -> Self {
        Self {
            acks,
            pending: sent.into(),
            canonical: Some(canonical),
        }
    }
    pub(super) fn publish_canonical(&self, ack: OperationAcknowledgement) {
        let ack = match &self.canonical {
            Some(publisher) => publisher.canonicalize(ack),
            None => ack,
        };
        self.acks.publish(ack);
    }

    pub(super) fn prefix_ids(&self, count: usize) -> Vec<TaskOperationId> {
        assert!(
            count <= self.pending.len(),
            "prefix is part of the same accepted batch"
        );
        let mut ids = Vec::with_capacity(count);
        for item in self.pending.iter().take(count) {
            ids.push(item.operation_id);
        }
        ids
    }

    pub(super) fn prefix_establishes(
        &self,
        count: usize,
    ) -> Vec<(TaskOperationId, ParticipationRef)> {
        assert!(
            count <= self.pending.len(),
            "prefix is part of the same accepted batch"
        );
        let selected = self
            .pending
            .iter()
            .take(count)
            .filter(|item| item.establish_participation.is_some())
            .count();
        let mut establishes = Vec::with_capacity(selected);
        for item in self.pending.iter().take(count) {
            if let Some(participation) = item.establish_participation {
                establishes.push((item.operation_id, participation));
            }
        }
        establishes
    }

    pub(super) fn front(&self) -> Option<SentOperation> {
        self.pending.front().copied()
    }

    pub(super) fn publish_next(&mut self, ack: OperationAcknowledgement) {
        let item = self
            .pending
            .pop_front()
            .expect("an accepted response cannot outnumber its request");
        assert_eq!(
            item.operation_id,
            ack.operation_id(),
            "an accepted response must settle the request at the queue head"
        );
        self.publish_canonical(ack);
    }

    pub(super) fn publish_recorded_dispatch_rejection(&mut self) {
        assert!(
            self.canonical
                .as_ref()
                .is_some_and(|p| p.dispatch_rejected())
        );
        // The original typed fact precedes every acknowledgement and wake.
        self.publish_uniform(OperationDispatchResult::NonWorkerRejected);
    }

    pub(super) fn publish_uniform(&mut self, result: OperationDispatchResult) {
        while let Some(item) = self.pending.pop_front() {
            observe_dispatch_result(item.kind, item.lease_renewal, result);
            self.publish_canonical(OperationAcknowledgement::from_dispatch_result(
                item.operation_id,
                item.kind,
                result,
                AckPayload::None,
            ));
        }
    }

    pub(super) fn publish_prefix_uniform(&mut self, count: usize, result: OperationDispatchResult) {
        for _ in 0..count {
            let item = self
                .pending
                .pop_front()
                .expect("accepted batch count cannot exceed pending operations");
            observe_dispatch_result(item.kind, item.lease_renewal, result);
            self.publish_canonical(OperationAcknowledgement::from_dispatch_result(
                item.operation_id,
                item.kind,
                result,
                AckPayload::None,
            ));
        }
    }
}

impl Drop for AcceptedOperationReceipts {
    fn drop(&mut self) {
        if !self.pending.is_empty() {
            tracing::warn!(
                operations = self.pending.len(),
                "accepted task operation send dropped before settlement"
            );
            let result = if self
                .canonical
                .as_ref()
                .is_some_and(|p| p.dispatch_rejected())
            {
                // A wake/Drop unwind cannot erase the already installed
                // whole-batch no-RPC fact or authorize an Unknown replay.
                OperationDispatchResult::NonWorkerRejected
            } else {
                OperationDispatchResult::TransportUnknown
            };
            self.publish_uniform(result);
        }
    }
}

/// Sends one batch and publishes one acknowledgement per request item.
pub(super) async fn apply_operations(
    mut send: ApplySend,
    requests: Vec<EncodedMethodBatch>,
    submitted_at: tokio::time::Instant,
) {
    let last_expiry = requests
        .iter()
        .map(|batch| batch.expires_at(submitted_at))
        .max()
        .expect("the original Native Apply has at least one encoded method run");
    let frozen = {
        let publisher = send
            .receipts
            .canonical
            .as_ref()
            .expect("governed Native dispatch retains its original canonical backing");
        publisher.freeze_dispatch(last_expiry).await
    };
    if let Err(()) = frozen {
        send.receipts.publish_recorded_dispatch_rejection();
        return;
    }
    // The same original F crosses the atomic dispatch cut before any RPC
    // body poll. Every run keeps its own unchanged absolute deadline.
    for batch in requests {
        let expires_at = batch.expires_at(submitted_at);
        let establishes = send.receipts.prefix_establishes(batch.items);
        let response = match send_operations_in(
            &send.client,
            batch.request,
            expires_at,
            &send.receipts.acks,
            &establishes,
            &mut send.physical,
            &mut send.action_tail,
            send.reply_plan,
        )
        .await
        {
            Ok(response) => response,
            Err(result) => {
                for item in send.receipts.pending.iter().take(batch.items) {
                    tracing::warn!(
                        operation = ?item.operation_id,
                        kind = item.kind.as_str(),
                        address = ?item.address,
                        dispatch_result = ?result,
                        "task operation method run failed before receipt settlement"
                    );
                }
                send.receipts.publish_prefix_uniform(batch.items, result);
                continue;
            }
        };

        let ids = send.receipts.prefix_ids(batch.items);
        let headers = match decode_operation_receipts(&response, &ids) {
            Ok(headers) => headers,
            Err(error) => {
                // The answer arrived but cannot be attributed to the requests. A
                // short or reordered response is refused whole: guessing which
                // item a receipt belongs to could settle an operation the backend
                // never applied, and resending is not allowed once an answer has
                // been received.
                tracing::warn!(detail = %error, "task operation batch response is unusable");
                observe_refusal(REFUSAL_UNUSABLE_RESPONSE, ids.len());
                send.receipts.publish_prefix_uniform(
                    batch.items,
                    OperationDispatchResult::NonWorkerRejected,
                );
                continue;
            }
        };

        for (header, receipt) in headers.iter().zip(response.receipts) {
            let item = send
                .receipts
                .front()
                .expect("validated receipt count matches the accepted request");
            let ack = acknowledgement_owned(&item, header, receipt);
            send.receipts.publish_next(ack);
        }
    }
}

/// One `ApplyTaskOperations` round trip, classified by type.
///
/// The deadline is the shared absolute submit deadline of this run. A batch that
/// has not answered by then has a genuinely unknown outcome, which is exactly
/// what the retry rule exists for.
pub(super) async fn send_operations_in(
    client: &Client,
    request: EncodedMethodRequest,
    expires_at: tokio::time::Instant,
    intake: &TaskAckIntakeHandle,
    establishes: &[(TaskOperationId, ParticipationRef)],
    selected: &mut Option<NativeCallSource>,
    action_tail: &mut Option<novarocks_resource_contract::ExistingOwnerRetentionPin>,
    reply_plan: novarocks_task_codec::resource_preflight::OperationReplyDecodePlan,
) -> Result<proto::ApplyTaskOperationsResponse, OperationDispatchResult> {
    let method = match &request {
        EncodedMethodRequest::Ordinary(_) => NativeRpcMethod::ApplyTaskOperations,
        EncodedMethodRequest::Control(_) => NativeRpcMethod::ApplyTaskControlOperations,
    };
    let source = selected
        .take()
        .ok_or(OperationDispatchResult::NonWorkerRejected)?;
    let (reserved, acquired) = match source {
        NativeCallSource::Warm { acquired, call } => (call, acquired),
        NativeCallSource::Cold(handoff) => {
            let acquired = match handoff {
                crate::native::transport::NativeDialHandoff::Ready(acquired) => acquired,
                crate::native::transport::NativeDialHandoff::Reserved(dial) => {
                    let mut acquired = None;
                    tokio::time::timeout_at(expires_at, dial.connect_in(&mut acquired))
                        .await
                        .map_err(|_| OperationDispatchResult::TransportUnknown)?
                        .map_err(|error| classify_channel_error(&error))?;
                    acquired.ok_or(OperationDispatchResult::NonWorkerRejected)?
                }
            };
            // A fresh connection's first local stream/Buffer is attempted once.
            // No async wait holds an admitted Joint vector for another local
            // physical position. Refusal proves no Apply was dispatched.
            let call = std::future::poll_fn(|cx| {
                Poll::Ready(acquired.channel().select_local_call().try_reserve(cx))
            })
            .await;
            let call = match call {
                Poll::Ready(Ok(call)) => call,
                _ => return Err(OperationDispatchResult::NonWorkerRejected),
            };
            (call, acquired)
        }
    };
    let mut grpc = client.grpc_for_reserved_call(reserved, method);
    let remaining = expires_at.saturating_duration_since(tokio::time::Instant::now());
    if remaining.is_zero() {
        // Nothing was submitted, so nothing was applied. It is still reported
        // as unknown rather than as a rejection, because a caller may only
        // conclude "not applied" from an answer it received.
        client.invalidate_channel_if_current(method, &acquired);
        return Err(OperationDispatchResult::TransportUnknown);
    }
    // Reconstruct the pure plan from the now-formed DTO and require the same
    // source bound used before its first clone. No Joint refund occurs here.
    let encoding_plan =
        novarocks_native_adapter::native_codec::NativeUnaryEncodingPlan::for_original_operation(
            request.encoded_len(),
        )
        .expect("one nonempty original unary method run fits its source invoice");
    let call = async {
        match request {
            EncodedMethodRequest::Ordinary(request) => {
                let mut wire = tonic::Request::new(request);
                wire.set_timeout(remaining);
                wire.extensions_mut().insert(novarocks_native_adapter::native_lane::NativeRequestCustody::from_retained_action(
                    action_tail.take().expect("one original unary method run retains its action source")));
                let codec=novarocks_native_adapter::native_codec::NativeProstCodec::with_original_operation_plans(reply_plan, encoding_plan);
                let rpc = grpc.unary(
                    wire,
                    tonic::codegen::http::uri::PathAndQuery::from_static(
                        "/novarocks.NovaRocksGrpc/ApplyTaskOperations",
                    ),
                    codec,
                );
                tokio::pin!(rpc);
                let mut started = false;
                std::future::poll_fn(|cx| {
                    let result = rpc.as_mut().poll(cx);
                    if !started {
                        started = true;
                        for &(operation_id, participation) in establishes {
                            intake.publish_establish_send_started(operation_id, participation);
                        }
                    }
                    result
                })
                .await
            }
            EncodedMethodRequest::Control(request) => {
                let mut wire = tonic::Request::new(request);
                wire.set_timeout(remaining);
                wire.extensions_mut().insert(novarocks_native_adapter::native_lane::NativeRequestCustody::from_retained_action(
                    action_tail.take().expect("one original unary method run retains its action source")));
                let codec=novarocks_native_adapter::native_codec::NativeProstCodec::with_original_operation_plans(reply_plan, encoding_plan);
                let rpc = grpc.unary(
                    wire,
                    tonic::codegen::http::uri::PathAndQuery::from_static(
                        "/novarocks.NovaRocksGrpc/ApplyTaskControlOperations",
                    ),
                    codec,
                );
                tokio::pin!(rpc);
                let mut started = false;
                std::future::poll_fn(|cx| {
                    let result = rpc.as_mut().poll(cx);
                    if !started {
                        started = true;
                        for &(operation_id, participation) in establishes {
                            intake.publish_establish_send_started(operation_id, participation);
                        }
                    }
                    result
                })
                .await
            }
        }
    };
    let outcome = match tokio::time::timeout_at(expires_at, call).await {
        Ok(result) => result.map(tonic::Response::into_inner).map_err(|status| {
            let outcome = classify_apply_status(&status);
            tracing::warn!(
                method = ?method,
                channel_slot = ?acquired.slot(),
                h2_source = ?task_operation_h2_source(&status),
                code = ?status.code(),
                detail = status.message(),
                "task operation rpc failed"
            );
            outcome
        }),
        Err(_) => Err(OperationDispatchResult::TransportUnknown),
    };
    if matches!(outcome, Err(OperationDispatchResult::TransportUnknown)) {
        // An unknown outcome may have poisoned this stream. An old request
        // cannot evict a replacement channel already installed by another
        // attempt or by membership reconciliation.
        client.invalidate_channel_if_current(method, &acquired);
    }
    outcome
}

#[cfg(test)]
pub(super) async fn fixture_apply_request<T: prost::Message + Send + 'static>(
    grpc: &mut crate::native::transport::AuthenticatedOperationTestGrpc,
    request: tonic::Request<T>,
    method: NativeRpcMethod,
    original_count: usize,
    response_max: usize,
) -> Result<tonic::Response<proto::ApplyTaskOperationsResponse>, tonic::Status> {
    let reply = novarocks_task_codec::resource_preflight::OperationReplyDecodePlan::for_response(
        original_count,
        response_max,
    )
    .expect("the fixture retains its original request count and method limit");
    let encoding =
        novarocks_native_adapter::native_codec::NativeUnaryEncodingPlan::for_original_operation(
            request.get_ref().encoded_len(),
        )
        .expect("a fixture method request is nonempty");
    let path = match method {
        NativeRpcMethod::ApplyTaskOperations => "/novarocks.NovaRocksGrpc/ApplyTaskOperations",
        NativeRpcMethod::ApplyTaskControlOperations => {
            "/novarocks.NovaRocksGrpc/ApplyTaskControlOperations"
        }
        _ => unreachable!("only original Apply fixtures use this codec"),
    };
    grpc.ready()
        .await
        .map_err(|error| tonic::Status::unknown(format!("Service was not ready: {error}")))?;
    grpc.unary(
        request,
        tonic::codegen::http::uri::PathAndQuery::from_static(path),
        novarocks_native_adapter::native_codec::NativeProstCodec::with_original_operation_plans(
            reply, encoding,
        ),
    )
    .await
}

// Legacy wire-only fixtures exercise their original request/classifier
// contract directly. Production has no source-free call to this author.
#[cfg(test)]
pub(super) async fn send_operations(
    client: &Client,
    request: EncodedMethodRequest,
    expires_at: tokio::time::Instant,
    intake: &TaskAckIntakeHandle,
    establishes: &[(TaskOperationId, ParticipationRef)],
) -> Result<proto::ApplyTaskOperationsResponse, OperationDispatchResult> {
    let method = match &request {
        EncodedMethodRequest::Ordinary(_) => NativeRpcMethod::ApplyTaskOperations,
        EncodedMethodRequest::Control(_) => NativeRpcMethod::ApplyTaskControlOperations,
    };
    let (mut grpc, acquired) = tokio::time::timeout_at(
        expires_at,
        client.operation_test_grpc_with_channel_identity(method),
    )
    .await
    .map_err(|_| OperationDispatchResult::TransportUnknown)?
    .map_err(|error| {
        let outcome = classify_channel_error(&error);
        tracing::warn!(detail = %error, "task operation channel acquisition failed");
        outcome
    })?;
    let remaining = expires_at.saturating_duration_since(tokio::time::Instant::now());
    if remaining.is_zero() {
        // Nothing was submitted, so nothing was applied. It is still reported
        // as unknown rather than as a rejection, because a caller may only
        // conclude "not applied" from an answer it received.
        client.invalidate_channel_if_current(method, &acquired);
        return Err(OperationDispatchResult::TransportUnknown);
    }
    let response_max = client.method_message_limits(method).1;
    let call = async {
        match request {
            EncodedMethodRequest::Ordinary(request) => {
                let original_count = request.operations.len();
                let mut wire = tonic::Request::new(request);
                wire.set_timeout(remaining);
                let rpc =
                    fixture_apply_request(&mut grpc, wire, method, original_count, response_max);
                tokio::pin!(rpc);
                let mut started = false;
                std::future::poll_fn(|cx| {
                    let result = rpc.as_mut().poll(cx);
                    if !started {
                        started = true;
                        for &(operation_id, participation) in establishes {
                            intake.publish_establish_send_started(operation_id, participation);
                        }
                    }
                    result
                })
                .await
            }
            EncodedMethodRequest::Control(request) => {
                let original_count = request.operations.len();
                let mut wire = tonic::Request::new(request);
                wire.set_timeout(remaining);
                let rpc =
                    fixture_apply_request(&mut grpc, wire, method, original_count, response_max);
                tokio::pin!(rpc);
                let mut started = false;
                std::future::poll_fn(|cx| {
                    let result = rpc.as_mut().poll(cx);
                    if !started {
                        started = true;
                        for &(operation_id, participation) in establishes {
                            intake.publish_establish_send_started(operation_id, participation);
                        }
                    }
                    result
                })
                .await
            }
        }
    };
    let outcome = match tokio::time::timeout_at(expires_at, call).await {
        Ok(result) => result.map(tonic::Response::into_inner).map_err(|status| {
            let outcome = classify_apply_status(&status);
            tracing::warn!(
                code = ?status.code(),
                detail = status.message(),
                "task operation rpc failed"
            );
            outcome
        }),
        Err(_) => Err(OperationDispatchResult::TransportUnknown),
    };
    if matches!(outcome, Err(OperationDispatchResult::TransportUnknown)) {
        // An unknown outcome may have poisoned this stream. An old request
        // cannot evict a replacement channel already installed by another
        // attempt or by membership reconciliation.
        client.invalidate_channel_if_current(method, &acquired);
    }
    outcome
}

pub(super) fn acknowledgement(
    item: &SentOperation,
    header: &ReceiptHeader,
    receipt: &proto::TaskOperationReceipt,
) -> OperationAcknowledgement {
    observe_ack_projection(item, project_acknowledgement(item, header, receipt))
}
pub(super) fn acknowledgement_owned(
    item: &SentOperation,
    header: &ReceiptHeader,
    receipt: proto::TaskOperationReceipt,
) -> OperationAcknowledgement {
    observe_ack_projection(item, project_acknowledgement_owned(item, header, receipt))
}
pub(super) fn observe_ack_projection(
    item: &SentOperation,
    projection: AcknowledgementProjection,
) -> OperationAcknowledgement {
    if let Some(fact) = projection.termination {
        let participation = fact.participation;
        let cause = fact.cause;
        if fact.release {
            tracing::info!(%participation, ?cause, "release acknowledgement reports a termination cause");
        } else {
            tracing::info!(%participation, ?cause, "participation acknowledgement reports a termination cause");
        }
    }
    if let Some(detail) = projection.unreadable_ack {
        tracing::warn!(
            kind = item.kind.as_str(),
            detail,
            "task operation receipt has a required but unreadable acknowledgement"
        );
        observe_refusal(REFUSAL_UNUSABLE_ACK, 1);
        observe_dispatch_result(
            item.kind,
            item.lease_renewal,
            OperationDispatchResult::NonWorkerRejected,
        );
    } else if let Some(outcome) = projection.acknowledgement.worker_outcome() {
        observe_settled(item.kind, item.lease_renewal, outcome);
    }
    projection.acknowledgement
}
