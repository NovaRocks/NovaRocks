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

//! A one-shot, non-waiting reservation of an original lane's local holders.
//!
//! `Pending` retains no waiter or partial reservation. The original bounded
//! service must reobserve its same request; it must not infer a durable wake
//! registration from this one poll. No request is dispatched by reservation.

use super::*;

/// One selected original connection. This is neither a Work grant nor a new
/// capacity author. The FE keeps its original cache slot and generation next
/// to this value, and rechecks that identity before a local offer is accepted.
///
/// This view deliberately has no Clone implementation. Its internal Channel
/// clone selects the original gate/Buffer once; attempts never choose a peer.
pub struct SelectedNativeCall {
    channel: NativeLaneChannel,
}

impl fmt::Debug for SelectedNativeCall {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SelectedNativeCall")
            .field("lane", &self.channel.lane())
            .finish_non_exhaustive()
    }
}

impl SelectedNativeCall {
    /// Attempt the same original stream and Tonic queue positions exactly
    /// once, without dispatch or an await. Call outside every ledger/cache/
    /// owner lock: Tonic readiness and temporary-holder Drop are foreign code.
    pub fn try_reserve(&self, cx: &mut Context<'_>) -> Poll<Result<ReservedNativeCall, StdError>> {
        self.channel.try_reserve_local(cx)
    }

    /// Keep this SAME Tonic clone/waiter through Pending and keep its actual
    /// Buffer qualification through Ready. This returns no sending authority,
    /// acquires no stream/Class/dial position and polls no RPC. The caller may
    /// only carry it between physical attempts of the SAME original WC ticket.
    /// Cancellation must destroy this source before reporting DefinitelyUnsent.
    pub fn poll_sdk_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), StdError>> {
        if let Some(admission) = &self.channel.outgoing_admission {
            loop {
                let changed = self
                    .channel
                    .outgoing_changed
                    .get_or_insert_with(|| Box::pin(admission.frontend_outgoing_notified()));
                if changed.as_mut().poll(cx).is_pending() {
                    break;
                }
                drop(self.channel.outgoing_changed.take());
                // A completed notifier is never polled again. Re-arm before
                // the closed/source check; actual events justify each pass.
            }
            if admission.frontend_outgoing_is_closed() {
                return Poll::Ready(Err("frontend outgoing native transport closed".into()));
            }
        }
        Service::poll_ready(&mut self.channel.channel, cx).map_err(Into::into)
    }
    /// Transfer the SAME SDK-ready clone into a local physical quote, never
    /// Drop/reacquire its Buffer qualification. A stream refusal retains only
    /// that selected SDK state; it holds no stream, Class, dial or Joint grant.
    /// Register stream_ready_notified BEFORE retrying this author.
    pub fn try_reserve_qualified_in(
        source: &mut Option<Self>,
        target: &mut Option<ReservedNativeCall>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StdError>> {
        if target.is_some() {
            return Poll::Ready(Err("native qualified call receiver is occupied".into()));
        }
        let Some(selected) = source.as_mut() else {
            return Poll::Ready(Err("native SDK source is absent".into()));
        };
        match selected.poll_sdk_ready(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Ready(Ok(())) => {}
        }
        let permit = match Arc::clone(&selected.channel.gate.semaphore).try_acquire_owned() {
            Ok(permit) => permit,
            Err(tokio::sync::TryAcquireError::NoPermits) => return Poll::Pending,
            Err(tokio::sync::TryAcquireError::Closed) => {
                return Poll::Ready(Err("native lane stream gate closed".into()));
            }
        };
        if selected
            .channel
            .outgoing_admission
            .as_ref()
            .is_some_and(NativeTransportAdmission::frontend_outgoing_is_closed)
        {
            drop(permit);
            return Poll::Ready(Err("frontend outgoing native transport closed".into()));
        }
        selected.channel.reserved = Some(permit);
        let selected = source.take().expect("same original SDK-qualified source");
        *target = Some(ReservedNativeCall {
            channel: Some(selected.channel),
        });
        Poll::Ready(Ok(()))
    }
    pub fn lane(&self) -> NativeLane {
        self.channel.lane()
    }
}

impl NativeLaneChannel {
    /// Freeze this original connection as the source for local attempts.
    /// The caller must keep/recheck the cache's original generation itself.
    pub fn select_local_call(&self) -> SelectedNativeCall {
        SelectedNativeCall {
            channel: self.clone(),
        }
    }

    /// Take this connection's original stream position and original Tonic
    /// Buffer position, or return with neither held by this attempt.
    ///
    /// `Pending` is a nonholding refusal, not an installed waiter. A bounded
    /// recheck of the same original request supplies progress if no other
    /// original source wakes it. Neither this method nor its return is Joint
    /// admission. Never call it under the workload State or a receiver lock.
    pub fn try_reserve_local(
        &self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<ReservedNativeCall, StdError>> {
        if self
            .outgoing_admission
            .as_ref()
            .is_some_and(NativeTransportAdmission::frontend_outgoing_is_closed)
        {
            return Poll::Ready(Err("frontend outgoing native transport closed".into()));
        }
        // Only this attempt owns any partial qualification. Dropping it also
        // drops a Buffer PollSemaphore's assigned-but-not-yet-returned permit.
        let mut attempt = self.clone();
        let permit = match Arc::clone(&attempt.gate.semaphore).try_acquire_owned() {
            Ok(permit) => permit,
            Err(tokio::sync::TryAcquireError::NoPermits) => return Poll::Pending,
            Err(tokio::sync::TryAcquireError::Closed) => {
                return Poll::Ready(Err("native lane stream gate closed".into()));
            }
        };
        attempt.reserved = Some(permit);
        match Service::poll_ready(&mut attempt.channel, cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => Poll::Ready(Err(error.into())),
            Poll::Ready(Ok(())) => {
                // Close can race readiness. The eventual call repeats the
                // original closed/register transaction too; no stale success
                // may bypass role close.
                if attempt
                    .outgoing_admission
                    .as_ref()
                    .is_some_and(NativeTransportAdmission::frontend_outgoing_is_closed)
                {
                    return Poll::Ready(Err("frontend outgoing native transport closed".into()));
                }
                Poll::Ready(Ok(ReservedNativeCall {
                    channel: Some(attempt),
                }))
            }
        }
    }
}

/// The exact stream+Buffer-ready Channel. MOVE this value into generated
/// Grpc/InterceptedService; cloning a Channel would lose its Buffer permit.
/// One call transfers the stream holder to the existing response future/body.
/// This carrier is not Clone and has no channel/permit extraction API.
pub struct ReservedNativeCall {
    channel: Option<NativeLaneChannel>,
}

impl fmt::Debug for ReservedNativeCall {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReservedNativeCall")
            .field("unconsumed", &self.channel.is_some())
            .finish_non_exhaustive()
    }
}

impl Service<hyper::http::Request<BoxBody>> for ReservedNativeCall {
    type Response = hyper::http::Response<BoxBody>;
    type Error = StdError;
    type Future = <NativeLaneChannel as Service<hyper::http::Request<BoxBody>>>::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let Some(channel) = self.channel.as_mut() else {
            return Poll::Ready(Err("native reserved call was already consumed".into()));
        };
        if channel
            .outgoing_admission
            .as_ref()
            .is_some_and(NativeTransportAdmission::frontend_outgoing_is_closed)
        {
            return Poll::Ready(Err("frontend outgoing native transport closed".into()));
        }
        // The same Buffer already has its permit. Tonic/Tower checks worker
        // liveness before returning that same qualification; no new pool is
        // acquired here. A closed worker returns its original error.
        Service::poll_ready(&mut channel.channel, cx).map_err(Into::into)
    }

    fn call(&mut self, request: hyper::http::Request<BoxBody>) -> Self::Future {
        let Some(mut channel) = self.channel.take() else {
            return Box::pin(async { Err("native reserved call was already consumed".into()) });
        };
        // This original author registers the real call, then transfers its
        // original permit to NativeLaneResponseFuture/NativeLaneStreamBody.
        Service::call(&mut channel, request)
    }
}
