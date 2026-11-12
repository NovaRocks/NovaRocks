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

//! Actual Native unary execution, copied from the original author. Cold dial
//! and local call custody stay on the same authenticated role client stack.
//! P05 classifies wire evidence; this module makes no retry/terminal decision.

use super::settlement::AcceptedReceipts;
use crate::native::{
    task_transport::{
        NativeCallSource,
        wire::{self, EncodedMethodRequest},
    },
    transport::{ChannelAcquisitionError, Client},
};
use crate::task_execution::{
    host::failure_lifecycle::LifecycleStarts, operation::OperationDispatchResult,
};
use novarocks_coordination_contract::MonotonicClock;
use novarocks_proto_codec::native_rpc::NativeRpcMethod;
use novarocks_proto_models::novarocks as proto;
use std::{future::Future, task::Poll};
pub(super) fn classify_channel_error(error: &ChannelAcquisitionError) -> OperationDispatchResult {
    wire::classify_channel_failure(match error {
        ChannelAcquisitionError::Fatal(_) => wire::ChannelFailureClass::Fatal,
        ChannelAcquisitionError::RetryableNetwork(_) => wire::ChannelFailureClass::RetryableNetwork,
    })
}
fn classify_apply_status(status: &tonic::Status) -> OperationDispatchResult {
    wire::classify_apply_status(status)
}
async fn observe_first_poll<F: Future>(
    rpc: F,
    receipts: &AcceptedReceipts,
    clock: &dyn MonotonicClock,
    lifecycle: Option<&LifecycleStarts>,
) -> F::Output {
    tokio::pin!(rpc);
    let mut started = false;
    std::future::poll_fn(|cx| {
        if started {
            return rpc.as_mut().poll(cx);
        }
        // Record the possibly transmitted operation even if the first poll
        // panics or the Core no longer accepts its progress input.
        if let Some(starts) = lifecycle {
            receipts.record_lifecycle_starts(starts);
        }
        let result = rpc.as_mut().poll(cx);
        receipts.report_establish_starts(clock);
        started = true;
        result
    })
    .await
}

pub(crate) async fn send_operations_in(
    client: &Client,
    request: EncodedMethodRequest,
    expires_at: tokio::time::Instant,
    receipts: &AcceptedReceipts,
    clock: &dyn MonotonicClock,
    lifecycle: Option<&LifecycleStarts>,
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
            match &request {
                EncodedMethodRequest::Ordinary(value) => prost::Message::encoded_len(value),
                EncodedMethodRequest::Control(value) => prost::Message::encoded_len(value),
            },
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
                observe_first_poll(rpc, receipts, clock, lifecycle).await
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
                observe_first_poll(rpc, receipts, clock, lifecycle).await
            }
        }
    };
    let outcome = match tokio::time::timeout_at(expires_at, call).await {
        Ok(result) => result.map(tonic::Response::into_inner).map_err(|status| {
            let outcome = classify_apply_status(&status);
            tracing::warn!(
                method = ?method,
                channel_slot = ?acquired.slot(),
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
