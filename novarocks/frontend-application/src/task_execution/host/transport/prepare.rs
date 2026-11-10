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

//! One original WC ticket is reobserved after each failed local physical try.
//! The credentialed Source carrier polls this future before the GatedWork gate.

use super::{
    proposal_wait::ProposalWait, queue_input::SendInputLoan, release_wake::LedgerReleaseWake,
};
use crate::native::{
    apply_send_owner::{ApplySendPort, RegistrationFailure, SharedApplyPreparation},
    task_transport::{NativeCallSource, NativePhysicalOffer},
    transport::{Client, NativeChannelSource, NativeDialHandoff},
    transport_capacity::{NativeTransportCapacity, TransportCapacityLane},
};
use crate::task_execution::host::custody::{
    CustodyError, EncodingBinding, EncodingJob, TransportJoinRegistration,
};
use novarocks_control_runtime::{
    capacity_notification::CapacityRegistrant,
    obligation::StepRegistration,
    transaction::{RuntimeError, RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    identity::{Generation, ReservationId},
    scope::WorkKind,
};
use novarocks_proto_codec::native_rpc::NativeRpcMethod;
use novarocks_types::BackendProcessId;
use novarocks_workload_control::{
    WorkError,
    joint_admission::{
        JointLane, NativeAsyncActionAdmission, NativeAsyncActionAuthority, NativeAsyncActionQuote,
    },
};
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

pub(crate) enum PreparationFailure {
    Governance(WorkError),
    Runtime(RuntimeError),
    Role(RegistrationFailure),
    Channel(crate::native::transport::ChannelAcquisitionError),
    LocalCall(Box<dyn std::error::Error + Send + Sync>),
    GenerationRetired,
    Layout(crate::native::transport_capacity::TransportCapacityError),
}

/// No encoded bytes or child Work has started. A withdrawal destroys the
/// actual selected call before the outer GatedWork destroys captures and
/// revokes its original parent. Once attached, the same unused child guard
/// remains in the Native row until its credentialed physical disposal; this
/// preparation loan's Drop cannot certify that later boundary.
pub(crate) struct ReadyAction {
    pub(crate) call: Option<NativeCallSource>,
    pub(crate) publisher: Option<crate::native::apply_send_owner::OriginalReplyPublisher>,
    pub(crate) row: SharedApplyPreparation,
    encoding: Option<(TransportJoinRegistration, EncodingBinding)>,
}
impl ReadyAction {
    pub(crate) fn bind_encoding(
        &mut self,
        guard: StepRegistration,
    ) -> Result<EncodingJob, CustodyError> {
        let (source, binding) = self.encoding.take().expect("one original encoding binding");
        source.bind_encoding(guard, binding)
    }
}
impl Drop for ReadyAction {
    fn drop(&mut self) {
        drop(self.call.take());
    }
}

#[derive(Clone, Copy)]
enum NativeWait {
    None,
    Encoding,
    Cold,
    Sdk,
    Stream,
}
struct WarmAttempt {
    acquired: crate::native::data_runtime::CachedNativeChannel,
    sdk: Option<novarocks_native_adapter::native_lane::SelectedNativeCall>,
    stream_wait: Option<Pin<Box<tokio::sync::futures::OwnedNotified>>>,
}
pub(crate) struct PrepareSend {
    admission: NativeAsyncActionAdmission,
    proposal: ProposalWait,
    ledger: RuntimeLedger,
    scope: ScopeHandle,
    parent: ReservationId,
    generation: Generation,
    layout: NativeTransportCapacity,
    backend: BackendProcessId,
    lane: TransportCapacityLane,
    encoding_bytes: usize,
    method: NativeRpcMethod,
    client: Client,
    apply: ApplySendPort,
    inputs: Option<Vec<SendInputLoan>>,
    registration: TransportJoinRegistration,
    queue: ReservationId,
    registrant: Option<CapacityRegistrant>,
    items: usize,
    channel_source: Option<NativeChannelSource>,
    warm: Option<WarmAttempt>,
    native_wait: NativeWait,
    released: Arc<LedgerReleaseWake>,
    generation_wait: Option<Pin<Box<tokio::sync::futures::OwnedNotified>>>,
}
impl PrepareSend {
    /// Only preparation-private allocations. Native source pointees are
    /// invoiced once across all phases by their own author: a ReservedNativeDial
    /// Box can survive into the body, while its two notified layouts also cover
    /// the mutually exclusive warm generation/stream wait boxes.
    pub(crate) fn owned_wait_bytes() -> Result<usize, WorkError> {
        let release = LedgerReleaseWake::allocation_layout()
            .map_err(|_| WorkError::InvalidConfig("Native release-wake allocation layout"))?;
        release
            .size()
            .checked_add(ProposalWait::owned_future_layout().size())
            .ok_or(WorkError::ArithmeticOverflow)
    }
    #[allow(
        clippy::too_many_arguments,
        reason = "All arguments are frozen facts or original Source authorities; no defaults or identity reconstruction."
    )]
    pub(crate) fn new(
        authority: &NativeAsyncActionAuthority,
        quote: NativeAsyncActionQuote,
        ledger: RuntimeLedger,
        scope: ScopeHandle,
        parent: ReservationId,
        generation: Generation,
        layout: NativeTransportCapacity,
        backend: BackendProcessId,
        lane: TransportCapacityLane,
        encoding_bytes: usize,
        method: NativeRpcMethod,
        client: Client,
        apply: ApplySendPort,
        inputs: Vec<SendInputLoan>,
        registration: TransportJoinRegistration,
        queue: ReservationId,
    ) -> Result<Self, PreparationFailure> {
        let admission = authority
            .action_source(
                if lane == TransportCapacityLane::Control {
                    JointLane::Settlement
                } else {
                    JointLane::Foreground
                },
                quote,
            )
            .map_err(PreparationFailure::Governance)?
            .begin_when_available()
            .map_err(|failure| PreparationFailure::Governance(failure.error))?;
        let items = inputs.len();
        assert!(
            items != 0,
            "each original batch retains actual paid WC inputs"
        );
        Ok(Self {
            admission,
            proposal: ProposalWait::default(),
            ledger,
            scope,
            parent,
            generation,
            layout,
            backend,
            lane,
            encoding_bytes,
            method,
            client,
            apply,
            inputs: Some(inputs),
            registration,
            queue,
            registrant: None,
            items,
            channel_source: None,
            warm: None,
            native_wait: NativeWait::None,
            released: LedgerReleaseWake::new(),
            generation_wait: None,
        })
    }
}
impl Future for PrepareSend {
    type Output = Result<ReadyAction, PreparationFailure>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.released.refresh(cx.waker());
        if this.warm.is_some() {
            loop {
                if this.generation_wait.is_none() {
                    let mut wait = Box::pin(this.client.original_ready_notified());
                    wait.as_mut().enable();
                    this.generation_wait = Some(wait);
                }
                if this.warm.as_ref().unwrap().acquired.is_retired() {
                    return Poll::Ready(Err(PreparationFailure::GenerationRetired));
                }
                if this
                    .generation_wait
                    .as_mut()
                    .unwrap()
                    .as_mut()
                    .poll(cx)
                    .is_pending()
                {
                    break;
                }
                drop(this.generation_wait.take());
            }
        }
        // Native/WC refund wakes are not permission to retry the physical
        // author. First wait on the EXACT resource that blocked this source.
        match this.native_wait {
            NativeWait::None => {}
            NativeWait::Encoding => {
                if !this.released.take_release() {
                    return Poll::Pending;
                }
                this.native_wait = NativeWait::None;
            }
            NativeWait::Cold => {
                match NativeChannelSource::poll_selected_ready(&mut this.channel_source, cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(error)) => {
                        return Poll::Ready(Err(PreparationFailure::Channel(error)));
                    }
                    Poll::Ready(Ok(())) => this.native_wait = NativeWait::None,
                }
            }
            NativeWait::Sdk => {
                let warm = this.warm.as_mut().expect("same selected SDK source");
                if warm.acquired.is_retired() {
                    return Poll::Ready(Err(PreparationFailure::GenerationRetired));
                }
                match warm
                    .sdk
                    .as_mut()
                    .expect("same original Tonic clone")
                    .poll_sdk_ready(cx)
                {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(error)) => {
                        return Poll::Ready(Err(PreparationFailure::LocalCall(error)));
                    }
                    Poll::Ready(Ok(())) => this.native_wait = NativeWait::None,
                }
            }
            NativeWait::Stream => {
                let warm = this.warm.as_mut().expect("same SDK-qualified source");
                loop {
                    match warm.sdk.as_mut().unwrap().poll_sdk_ready(cx) {
                        Poll::Pending => {
                            this.native_wait = NativeWait::Sdk;
                            return Poll::Pending;
                        }
                        Poll::Ready(Err(error)) => {
                            return Poll::Ready(Err(PreparationFailure::LocalCall(error)));
                        }
                        Poll::Ready(Ok(())) => {}
                    }
                    if warm.acquired.is_retired() {
                        return Poll::Ready(Err(PreparationFailure::GenerationRetired));
                    }
                    if warm.stream_wait.is_none() {
                        let mut wait = Box::pin(warm.acquired.channel().stream_ready_notified());
                        wait.as_mut().enable();
                        warm.stream_wait = Some(wait);
                    }
                    if warm.acquired.channel().available_streams() != 0 {
                        drop(warm.stream_wait.take());
                        this.native_wait = NativeWait::None;
                        break;
                    }
                    if warm
                        .stream_wait
                        .as_mut()
                        .unwrap()
                        .as_mut()
                        .poll(cx)
                        .is_pending()
                    {
                        return Poll::Pending;
                    }
                    drop(warm.stream_wait.take());
                }
            }
        }
        if this.registrant.is_none() {
            this.registrant = Some(
                match this
                    .ledger
                    .capacity_registrant(std::task::Waker::from(Arc::clone(&this.released)))
                {
                    Ok(registrant) => registrant,
                    Err(error) => return Poll::Ready(Err(PreparationFailure::Runtime(error))),
                },
            );
        }
        this.released.arm(cx.waker());
        let mut offer = match this
            .admission
            .poll_local_offer(cx, &mut |signal, cx| this.proposal.poll(signal, cx))
        {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(failure)) => {
                return Poll::Ready(Err(PreparationFailure::Governance(failure.error)));
            }
            Poll::Ready(Ok(offer)) => offer,
        };
        let slot = match this.apply.reserve_original_slot(this.items) {
            Ok(slot) => slot,
            Err(failure) => {
                let notice = offer
                    .reobserve_after_drop(&mut None::<NativePhysicalOffer>)
                    .map_err(PreparationFailure::Governance);
                drop(offer);
                match notice {
                    Ok(notice) => notice.notify(),
                    Err(error) => return Poll::Ready(Err(error)),
                };
                return Poll::Ready(Err(PreparationFailure::Role(failure)));
            }
        };
        let mut requested =
            match this
                .layout
                .encoding_resources(this.backend, this.lane, this.encoding_bytes)
            {
                Ok(value) => value,
                Err(_) => {
                    drop(slot);
                    let notice = offer
                        .reobserve_after_drop(&mut None::<NativePhysicalOffer>)
                        .map_err(PreparationFailure::Governance);
                    drop(offer);
                    match notice {
                        Ok(notice) => notice.notify(),
                        Err(error) => return Poll::Ready(Err(error)),
                    };
                    return Poll::Ready(Err(PreparationFailure::Governance(WorkError::Capacity(
                        "Native original encoding bytes",
                    ))));
                }
            };
        // These are the actual one child report and capture record, bound only
        // when the parent's start gate wins. No additional byte/queue quota.
        requested.owned_slots.completions = 1;
        requested.owned_slots.captures = 1;
        let kind = if this.lane == TransportCapacityLane::Control {
            WorkKind::Settlement
        } else {
            WorkKind::Business
        };
        let encoding = match this.scope.reserve_child_next_or_wait(
            this.generation,
            this.parent,
            requested,
            kind,
            this.registrant.as_ref().unwrap(),
        ) {
            Ok(guard) => guard,
            Err(error) => {
                drop(slot);
                let notice = offer
                    .reobserve_after_drop(&mut None::<NativePhysicalOffer>)
                    .map_err(PreparationFailure::Governance);
                drop(offer);
                match notice {
                    Ok(notice) => notice.notify(),
                    Err(error) => return Poll::Ready(Err(error)),
                };
                return match error {
                    RuntimeError::CapacityDenied(denial) => {
                        match NativeTransportCapacity::classify_denial(denial) {
                            Ok(crate::native::transport_capacity::TransportCapacityRefusal::ProcessFull)=>{},
                            Ok(crate::native::transport_capacity::TransportCapacityRefusal::TargetFull(backend)) if backend==this.backend=>{},
                            Ok(_)=>return Poll::Ready(Err(PreparationFailure::Layout(crate::native::transport_capacity::TransportCapacityError::ForeignDimension))),
                            Err(error)=>return Poll::Ready(Err(PreparationFailure::Layout(error))),
                        }
                        this.native_wait = NativeWait::Encoding;
                        Poll::Pending
                    }
                    other => Poll::Ready(Err(PreparationFailure::Runtime(other))),
                };
            }
        };
        let physical = (|| {
            if this.warm.is_none() {
                if this.channel_source.is_none() {
                    this.channel_source = Some(
                        this.client
                            .prepare_channel_source(this.method)
                            .map_err(PreparationFailure::Channel)?,
                    );
                }
                let mut handoff = None;
                match NativeChannelSource::poll_reserve_in(
                    &mut this.channel_source,
                    &mut handoff,
                    cx,
                ) {
                    Poll::Pending => {
                        this.native_wait = NativeWait::Cold;
                        return Ok(None);
                    }
                    Poll::Ready(Err(error)) => return Err(PreparationFailure::Channel(error)),
                    Poll::Ready(Ok(())) => {}
                }
                match handoff
                    .take()
                    .expect("same original selected Native source")
                {
                    NativeDialHandoff::Ready(acquired) => {
                        let sdk = acquired.channel().select_local_call();
                        this.warm = Some(WarmAttempt {
                            acquired,
                            sdk: Some(sdk),
                            stream_wait: None,
                        });
                    }
                    cold @ NativeDialHandoff::Reserved(_) => {
                        return Ok(Some(NativePhysicalOffer {
                            encoding: None,
                            call: Some(NativeCallSource::Cold(cold)),
                        }));
                    }
                }
            }
            let warm = this
                .warm
                .as_mut()
                .expect("same warm source retained across Pending");
            if warm.acquired.is_retired() {
                return Err(PreparationFailure::GenerationRetired);
            }
            match warm.sdk.as_mut().unwrap().poll_sdk_ready(cx) {
                Poll::Pending => {
                    this.native_wait = NativeWait::Sdk;
                    return Ok(None);
                }
                Poll::Ready(Err(error)) => return Err(PreparationFailure::LocalCall(error)),
                Poll::Ready(Ok(())) => {}
            }
            // The SAME SDK qualification survives a stream refusal. It is
            // never dropped then reacquired behind other observers.
            if warm.stream_wait.is_none() {
                let mut wait = Box::pin(warm.acquired.channel().stream_ready_notified());
                wait.as_mut().enable();
                warm.stream_wait = Some(wait);
            }
            let mut call = None;
            match novarocks_native_adapter::native_lane::SelectedNativeCall::try_reserve_qualified_in(&mut warm.sdk,&mut call,cx){
                Poll::Pending=>{this.native_wait=NativeWait::Stream;return Ok(None);},
                Poll::Ready(Err(error))=>return Err(PreparationFailure::LocalCall(error)),
                Poll::Ready(Ok(()))=>{},
            }
            let WarmAttempt { acquired, .. } = this.warm.take().unwrap();
            let call = NativeCallSource::Warm {
                acquired,
                call: call.expect("same SDK-qualified local call"),
            };
            // None is only the retired TS member. The nonoptional original
            // ledger guard above owns all required encoding funding.
            Ok(Some(NativePhysicalOffer {
                encoding: None,
                call: Some(call),
            }))
        })();
        let mut physical = match physical {
            Ok(Some(physical)) => Some(physical),
            result => {
                drop(encoding);
                drop(slot);
                let notice = offer
                    .reobserve_after_drop(&mut None::<NativePhysicalOffer>)
                    .map_err(PreparationFailure::Governance);
                drop(offer);
                match notice {
                    Ok(notice) => notice.notify(),
                    Err(error) => return Poll::Ready(Err(error)),
                };
                return match result {
                    Ok(None) => Poll::Pending,
                    Err(error) => Poll::Ready(Err(error)),
                    Ok(Some(_)) => unreachable!(),
                };
            }
        };
        // Register the exact original row with the already-running Source Join
        // before an accepted physical object can require deferred retirement.
        let inputs = this
            .inputs
            .take()
            .expect("same original queued grants")
            .into_iter()
            .map(SendInputLoan::into_original)
            .collect();
        let row = match this
            .registration
            .attach_send(slot, inputs, encoding, this.queue)
        {
            Ok(row) => row,
            Err(error) => {
                let notification = offer
                    .reobserve_after_drop(&mut physical)
                    .map_err(PreparationFailure::Governance);
                drop(offer);
                match notification {
                    Ok(notification) => notification.notify(),
                    Err(error) => return Poll::Ready(Err(error)),
                }
                return Poll::Ready(Err(PreparationFailure::Role(error)));
            }
        };
        let accepted = match row.accept_native_in(&mut offer, &mut physical) {
            Ok(notice) => notice,
            Err(error) => {
                let notice = offer
                    .reobserve_after_drop(&mut physical)
                    .map_err(PreparationFailure::Governance);
                drop(offer);
                match notice {
                    Ok(notice) => notice.notify(),
                    Err(error) => return Poll::Ready(Err(error)),
                };
                return Poll::Ready(Err(PreparationFailure::Governance(error)));
            }
        };
        drop(offer);
        let (publisher, action, reply) = match row.prepare_native_in(&this.admission, this.items) {
            Ok(value) => value,
            Err(error) => {
                accepted.notify();
                return Poll::Ready(Err(PreparationFailure::Governance(error)));
            }
        };
        let mut physical = match row.take_native_physical() {
            Ok(value) => value,
            Err(error) => {
                for notice in [accepted, action, reply] {
                    notice.notify();
                }
                return Poll::Ready(Err(PreparationFailure::Governance(error)));
            }
        };
        let ready = ReadyAction {
            call: physical.call.take(),
            publisher: Some(publisher),
            row,
            encoding: Some((
                this.registration.clone(),
                EncodingBinding {
                    layout: this.layout.clone(),
                    backend: this.backend,
                    lane: this.lane,
                    quoted_bytes: this.encoding_bytes,
                },
            )),
        };
        drop(physical);
        drop(this.registrant.take());
        for notice in [accepted, action, reply] {
            notice.notify();
        }
        Poll::Ready(Ok(ready))
    }
}
