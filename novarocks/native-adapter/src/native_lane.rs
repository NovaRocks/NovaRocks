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

//! Native lanes and the stream positions they hold.
//!
//! A lane is one physical transport owner from the frozen method manifest
//! (spec v6 §5.4.5). Each HTTP/2 stream a lane opens or serves holds one
//! NovaRocks stream position from before the request is dispatched until the
//! response body's public exit: end of stream, an error/reset, or the drop of
//! the NovaRocks body wrapper. Neither a returned unary handler nor a resolved
//! Tonic response future returns the position (spec v6 §5.4.4).
//!
//! Positions are counts. The bytes Hyper, H2 and Tonic keep per stream are
//! bounded by their public configuration and verified by the transport
//! measurement gate; see `native_transport_geometry`.
//!
//! Design: ADR-0170 (docs/adr/ADR-0170-third-party-crates-are-bounded-by-public-configuration-not-forked.md)

mod request_custody;
use request_custody::StreamCustody;
pub use request_custody::{NativeRequestCustody, native_request_custody_layouts};

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};
use std::time::Duration;

use bytes::Bytes;
use hyper::body::Frame;
use novarocks_execution_contract::native_result_support::NativeResultSupportGeometry;
use novarocks_proto_codec::native_rpc::{FrontendNativeLane, NativeTrafficClass};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::PollSemaphore;
use tonic::body::BoxBody;
use tonic::codegen::{Body as HttpBody, Service, StdError};
use tonic::transport::Channel;

use crate::native_transport_admission::{
    FrontendOutgoingCallExit, NativeTransportAdmission, TransportClass, TransportRole,
};

/// One physical Native transport owner from the frozen method manifest.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum NativeLane {
    ResultData,
    Submission,
    Observation,
    LifecycleControl,
    Exchange,
    RuntimeFilter,
    Membership,
}

impl NativeLane {
    pub const COUNT: usize = 7;
    pub const ALL: [Self; Self::COUNT] = [
        Self::ResultData,
        Self::Submission,
        Self::Observation,
        Self::LifecycleControl,
        Self::Exchange,
        Self::RuntimeFilter,
        Self::Membership,
    ];

    /// The lane of a manifest traffic class; retired traffic has none.
    pub const fn of(traffic: NativeTrafficClass) -> Option<Self> {
        match traffic {
            NativeTrafficClass::Frontend(lane) => Some(Self::frontend(lane)),
            NativeTrafficClass::Exchange => Some(Self::Exchange),
            NativeTrafficClass::RuntimeFilter => Some(Self::RuntimeFilter),
            NativeTrafficClass::Membership => Some(Self::Membership),
            NativeTrafficClass::Retired => None,
        }
    }

    pub const fn frontend(lane: FrontendNativeLane) -> Self {
        match lane {
            FrontendNativeLane::ResultData => Self::ResultData,
            FrontendNativeLane::Submission => Self::Submission,
            FrontendNativeLane::Observation => Self::Observation,
            FrontendNativeLane::LifecycleControl => Self::LifecycleControl,
        }
    }

    pub const fn index(self) -> usize {
        match self {
            Self::ResultData => 0,
            Self::Submission => 1,
            Self::Observation => 2,
            Self::LifecycleControl => 3,
            Self::Exchange => 4,
            Self::RuntimeFilter => 5,
            Self::Membership => 6,
        }
    }

    /// Stable metric label.
    pub const fn label(self) -> &'static str {
        match self {
            Self::ResultData => "result_data",
            Self::Submission => "submission",
            Self::Observation => "observation",
            Self::LifecycleControl => "lifecycle_control",
            Self::Exchange => "exchange",
            Self::RuntimeFilter => "runtime_filter",
            Self::Membership => "membership",
        }
    }

    /// The connection admission class whose positions this lane's
    /// connections take. Only lifecycle control uses the Control class.
    pub const fn class(self) -> TransportClass {
        match self {
            Self::LifecycleControl => TransportClass::Control,
            _ => TransportClass::Data,
        }
    }
}

/// Which side of a stream a position belongs to.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum StreamDirection {
    /// A stream this process serves.
    Incoming,
    /// A stream this process opened as a client.
    Outgoing,
}

impl StreamDirection {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Incoming => "incoming",
            Self::Outgoing => "outgoing",
        }
    }
}

/// Which admission position a count describes.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum PositionKind {
    /// A live connection: from admission until its IO wrapper is dropped.
    Connection,
    /// A connection still bootstrapping (TLS, H2 preface, first request).
    Handshake,
}

impl PositionKind {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Connection => "connection",
            Self::Handshake => "handshake",
        }
    }
}

/// Role-owned publication of admission transitions. Every method is a
/// notification after the transition has happened; admission never reads
/// back from it, so metrics cannot change an admission decision.
pub trait NativeTransportObserver: Send + Sync + 'static {
    /// Positions of one class and kind currently in use, and their limit.
    fn positions(&self, _class: TransportClass, _kind: PositionKind, _used: usize, _limit: usize) {}
    /// One connection was refused for lack of a position.
    fn refused(&self, _class: TransportClass) {}
    /// A connection bound to a lane was established (+1) or exited (-1).
    fn lane_connections(&self, _lane: NativeLane, _delta: i64) {}
    /// A stream position was taken (+1) or returned (-1).
    fn lane_streams(&self, _lane: NativeLane, _direction: StreamDirection, _delta: i64) {}
    /// The fixed number of stream positions of one gate.
    fn lane_stream_limit(&self, _lane: NativeLane, _direction: StreamDirection, _limit: usize) {}
}

pub(crate) type SharedObserver = Option<Arc<dyn NativeTransportObserver>>;

/// A fixed number of stream positions for one lane and direction.
#[derive(Clone)]
pub struct NativeLaneStreamGate {
    semaphore: Arc<Semaphore>,
    lane: NativeLane,
    direction: StreamDirection,
    limit: usize,
    observer: SharedObserver,
}

impl fmt::Debug for NativeLaneStreamGate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeLaneStreamGate")
            .field("lane", &self.lane)
            .field("direction", &self.direction)
            .field("limit", &self.limit)
            .field("available", &self.available())
            .finish()
    }
}

impl NativeLaneStreamGate {
    pub(crate) fn new(
        lane: NativeLane,
        direction: StreamDirection,
        limit: usize,
        observer: SharedObserver,
    ) -> Self {
        if let Some(observer) = &observer {
            observer.lane_stream_limit(lane, direction, limit);
        }
        Self {
            semaphore: Arc::new(Semaphore::new(limit)),
            lane,
            direction,
            limit,
            observer,
        }
    }

    pub fn lane(&self) -> NativeLane {
        self.lane
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    pub fn available(&self) -> usize {
        self.semaphore.available_permits()
    }

    /// Take a position now or refuse; a refusal holds nothing.
    pub fn try_acquire(&self) -> Option<NativeLaneStreamPermit> {
        Arc::clone(&self.semaphore)
            .try_acquire_owned()
            .ok()
            .map(|permit| self.permit(permit))
    }

    fn permit(&self, permit: OwnedSemaphorePermit) -> NativeLaneStreamPermit {
        self.permit_with_exit(permit, None)
    }

    fn permit_with_exit(
        &self,
        permit: OwnedSemaphorePermit,
        outgoing_exit: Option<FrontendOutgoingCallExit>,
    ) -> NativeLaneStreamPermit {
        if let Some(observer) = &self.observer {
            observer.lane_streams(self.lane, self.direction, 1);
        }
        NativeLaneStreamPermit {
            permit: Some(permit),
            lane: self.lane,
            direction: self.direction,
            observer: self.observer.clone(),
            _outgoing_exit: outgoing_exit,
        }
    }
}

/// One held stream position. Dropping it is the stream's exit.
pub struct NativeLaneStreamPermit {
    permit: Option<OwnedSemaphorePermit>,
    lane: NativeLane,
    direction: StreamDirection,
    observer: SharedObserver,
    // Last: a body/future and its original permit exit before this observation.
    _outgoing_exit: Option<FrontendOutgoingCallExit>,
}

impl fmt::Debug for NativeLaneStreamPermit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeLaneStreamPermit")
            .field("lane", &self.lane)
            .field("direction", &self.direction)
            .finish()
    }
}

impl Drop for NativeLaneStreamPermit {
    fn drop(&mut self) {
        drop(self.permit.take());
        if let Some(observer) = &self.observer {
            observer.lane_streams(self.lane, self.direction, -1);
        }
    }
}

/// A response body that holds its stream position until its public exit:
/// the end of stream, an error (including a reset), or this wrapper's drop.
pub struct NativeLaneStreamBody {
    inner: Option<BoxBody>,
    permit: Option<StreamCustody>,
}

impl NativeLaneStreamBody {
    pub fn new(inner: BoxBody, permit: Option<NativeLaneStreamPermit>) -> Self {
        Self {
            inner: Some(inner),
            permit: permit.map(|permit| StreamCustody::Direct { _permit: permit }),
        }
    }
    fn boxed_with_custody(inner: BoxBody, permit: Option<StreamCustody>) -> BoxBody {
        tonic::body::boxed(Self {
            inner: Some(inner),
            permit,
        })
    }

    /// Box the wrapper as the Tonic body type, so the transport's response
    /// type does not change.
    pub fn boxed(inner: BoxBody, permit: Option<NativeLaneStreamPermit>) -> BoxBody {
        match permit {
            Some(permit) => tonic::body::boxed(Self::new(inner, Some(permit))),
            None => inner,
        }
    }
}

impl HttpBody for NativeLaneStreamBody {
    type Data = Bytes;
    type Error = tonic::Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let polled = Pin::new(self.inner.as_mut().expect("original response body")).poll_frame(cx);
        if matches!(polled, Poll::Ready(None | Some(Err(_)))) {
            // Legacy observers retain their original public-body semantics.
            // Paid calls retain the same source through actual inner Drop;
            // outgoing Bytes aliases independently keep their same tail.
            if matches!(self.permit, Some(StreamCustody::Direct { .. })) {
                drop(self.permit.take());
            }
        }
        polled
    }

    fn is_end_stream(&self) -> bool {
        self.inner.as_ref().is_none_or(HttpBody::is_end_stream)
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        self.inner
            .as_ref()
            .map_or_else(Default::default, HttpBody::size_hint)
    }
}
impl Drop for NativeLaneStreamBody {
    fn drop(&mut self) {
        let inner = self.inner.take();
        if let Err(cause) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(inner))) {
            std::panic::resume_unwind(Box::new(NativeStreamDropFailure {
                _cause: cause,
                _custody: self.permit.take(),
            }));
        }
        drop(self.permit.take());
    }
}
struct NativeStreamDropFailure {
    _cause: Box<dyn std::any::Any + Send>,
    _custody: Option<StreamCustody>,
}

/// One outgoing connection owner: a Tonic `Channel` over exactly one admitted
/// connection, plus the stream positions of that connection.
///
/// `poll_ready` takes a position before the request reaches Tonic's queue,
/// and the response body carries it to its exit. Clones share the positions;
/// a position reserved by `poll_ready` belongs to that clone only.
pub struct NativeLaneChannel {
    channel: Channel,
    gate: NativeLaneStreamGate,
    poll: PollSemaphore,
    reserved: Option<OwnedSemaphorePermit>,
    outgoing_admission: Option<NativeTransportAdmission>,
    outgoing_changed: Option<Pin<Box<tokio::sync::futures::OwnedNotified>>>,
}

impl fmt::Debug for NativeLaneChannel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeLaneChannel")
            .field("gate", &self.gate)
            .finish_non_exhaustive()
    }
}

impl Clone for NativeLaneChannel {
    fn clone(&self) -> Self {
        Self {
            channel: self.channel.clone(),
            gate: self.gate.clone(),
            poll: PollSemaphore::new(Arc::clone(&self.gate.semaphore)),
            reserved: None,
            outgoing_admission: self.outgoing_admission.clone(),
            outgoing_changed: None,
        }
    }
}

impl NativeLaneChannel {
    /// Bind `channel` to its lane with the frozen per-connection stream
    /// count. Outgoing transitions publish through `admission`'s observer.
    pub fn new(
        channel: Channel,
        lane: NativeLane,
        admission: Option<&NativeTransportAdmission>,
    ) -> Self {
        let mut channel = Self::with_streams(
            channel,
            lane,
            client_streams_per_connection(),
            admission.and_then(NativeTransportAdmission::observer),
        );
        channel.outgoing_admission = admission
            .filter(|admission| admission.role() == TransportRole::Frontend && lane.index() < 4)
            .cloned();
        channel
    }

    pub(crate) fn with_streams(
        channel: Channel,
        lane: NativeLane,
        streams: usize,
        observer: SharedObserver,
    ) -> Self {
        let gate = NativeLaneStreamGate::new(lane, StreamDirection::Outgoing, streams, observer);
        Self {
            channel,
            poll: PollSemaphore::new(Arc::clone(&gate.semaphore)),
            gate,
            reserved: None,
            outgoing_admission: None,
            outgoing_changed: None,
        }
    }

    pub fn lane(&self) -> NativeLane {
        self.gate.lane
    }

    pub fn stream_limit(&self) -> usize {
        self.gate.limit
    }

    pub fn available_streams(&self) -> usize {
        self.gate.available()
    }
}

impl Service<hyper::http::Request<BoxBody>> for NativeLaneChannel {
    type Response = hyper::http::Response<BoxBody>;
    type Error = StdError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if let Some(admission) = &self.outgoing_admission {
            // Register before checking closed. The same Notify wakes a waiter
            // already blocked on this clone's original stream reservation.
            let changed = self
                .outgoing_changed
                .get_or_insert_with(|| Box::pin(admission.frontend_outgoing_notified()));
            if changed.as_mut().poll(cx).is_ready() {
                *changed = Box::pin(admission.frontend_outgoing_notified());
                let _ = changed.as_mut().poll(cx);
            }
            if admission.frontend_outgoing_is_closed() {
                drop(self.reserved.take());
                // A pending acquire future may already own an assigned
                // qualification even though poll_acquire returned Pending.
                self.poll = PollSemaphore::new(Arc::clone(&self.gate.semaphore));
                return Poll::Ready(Err("frontend outgoing native transport closed".into()));
            }
        }
        if self.reserved.is_none() {
            // The semaphore is never closed; a closed gate is a broken owner.
            let Some(permit) = ready!(self.poll.poll_acquire(cx)) else {
                return Poll::Ready(Err("native lane stream gate closed".into()));
            };
            self.reserved = Some(permit);
        }
        Service::poll_ready(&mut self.channel, cx).map_err(Into::into)
    }

    fn call(&mut self, mut request: hyper::http::Request<BoxBody>) -> Self::Future {
        let Some(reserved) = self.reserved.take() else {
            return Box::pin(async { Err("native lane request was not ready".into()) });
        };
        // This check also covers a clone whose poll_ready succeeded before
        // role close. Registration and close share the original Core lock.
        let outgoing_exit = match self
            .outgoing_admission
            .as_ref()
            .map(|admission| admission.register_frontend_outgoing_call(self.gate.lane))
            .transpose()
        {
            Ok(exit) => exit,
            Err(error) => {
                self.poll = PollSemaphore::new(Arc::clone(&self.gate.semaphore));
                return Box::pin(async move { Err(error.into()) });
            }
        };
        let permit = self.gate.permit_with_exit(reserved, outgoing_exit);
        let permit = StreamCustody::install(permit, &mut request);
        let response = Service::call(&mut self.channel, request);
        Box::pin(NativeLaneResponseFuture {
            inner: Some(response),
            permit: Some(permit),
        })
    }
}

// The actual response future drops before the original stream holder if a
// caller abandons it. Headers hand that holder to the original response body.
struct NativeLaneResponseFuture {
    inner: Option<<Channel as Service<hyper::http::Request<BoxBody>>>::Future>,
    permit: Option<StreamCustody>,
}

impl Future for NativeLaneResponseFuture {
    type Output = Result<hyper::http::Response<BoxBody>, StdError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(
            self.inner
                .as_mut()
                .expect("original Native response future"),
        )
        .poll(cx)
        {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(response)) => {
                let permit = self.permit.take();
                Poll::Ready(Ok(
                    response.map(|body| NativeLaneStreamBody::boxed_with_custody(body, permit))
                ))
            }
            Poll::Ready(Err(error)) => {
                if matches!(self.permit, Some(StreamCustody::Direct { .. })) {
                    drop(self.permit.take());
                }
                Poll::Ready(Err(error.into()))
            }
        }
    }
}

impl Drop for NativeLaneResponseFuture {
    fn drop(&mut self) {
        let inner = self.inner.take();
        if let Err(cause) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(inner))) {
            std::panic::resume_unwind(Box::new(NativeStreamDropFailure {
                _cause: cause,
                _custody: self.permit.take(),
            }));
        }
        drop(self.permit.take());
    }
}

fn client_streams_per_connection() -> usize {
    usize::try_from(NativeResultSupportGeometry::V1.transport_streams_per_connection)
        .expect("validated Native geometry fits the target")
}

/// Public Tonic endpoint settings from the frozen geometry: connect deadline,
/// fixed receive windows, header list size, the Tonic request queue and the
/// per-connection request concurrency. Settings Tonic 0.12 does not expose
/// keep the Hyper/H2 defaults recorded in `native_transport_geometry`.
pub fn configure_native_endpoint(
    endpoint: tonic::transport::Endpoint,
) -> tonic::transport::Endpoint {
    let g = NativeResultSupportGeometry::V1;
    endpoint
        .tcp_keepalive(Some(Duration::from_secs(60)))
        .connect_timeout(Duration::from_millis(g.transport_connect_deadline_ms))
        .http2_adaptive_window(g.transport_h2_adaptive_window)
        .initial_stream_window_size(Some(
            u32::try_from(g.transport_h2_stream_receive_window_bytes)
                .expect("validated Native stream window fits HTTP/2"),
        ))
        .initial_connection_window_size(Some(
            u32::try_from(g.transport_h2_connection_receive_window_bytes)
                .expect("validated Native connection window fits HTTP/2"),
        ))
        .http2_max_header_list_size(
            u32::try_from(g.transport_h2_header_bytes)
                .expect("validated Native header list fits HTTP/2"),
        )
        .buffer_size(
            usize::try_from(g.transport_tonic_pending_per_connection)
                .expect("validated Native pending queue fits the target"),
        )
        .concurrency_limit(client_streams_per_connection())
}

/// The connection count a Frontend keeps per Backend process, endpoint and
/// lane, from the frozen geometry.
pub fn frontend_lane_connections(lane: FrontendNativeLane) -> usize {
    let g = NativeResultSupportGeometry::V1;
    let count = match lane {
        FrontendNativeLane::ResultData => g.transport_connections_per_frontend_backend_result,
        FrontendNativeLane::Submission => g.transport_connections_per_frontend_backend_submission,
        FrontendNativeLane::Observation => g.transport_connections_per_frontend_backend_observation,
        FrontendNativeLane::LifecycleControl => {
            g.transport_connections_per_frontend_backend_lifecycle_control
        }
    };
    usize::try_from(count).expect("validated Native lane connection count fits the target")
}

/// Which admission a dial attempt takes.
#[derive(Clone, Copy)]
#[expect(
    clippy::large_enum_variant,
    reason = "one Copy value per connector, captured once; the inline key avoids an allocation per dial"
)]
pub(crate) enum NativeDial {
    /// A Backend dial of `class`, with its exact peer/lane key when it has one.
    Backend(
        TransportClass,
        Option<crate::native_channel_identity::InlineNativeChannelIdentity>,
    ),
    /// A Frontend dial of one Backend lane.
    Frontend(FrontendNativeLane),
}

/// The transport connector preceded by dial admission. Admission is taken
/// again on every attempt, including Tonic's internal reconnect; a refused
/// attempt opens no socket and resolves no name, and the physical position of
/// an established connection follows its IO until the IO is dropped. The
/// handshake position covers DNS, TCP and TLS and returns once the IO exists.
pub(crate) fn admitted_connector_service(
    connector: novarocks_native_trust::NativeEndpointConnector,
    admission: Option<NativeTransportAdmission>,
    dial: NativeDial,
) -> impl Service<
    hyper::http::Uri,
    Response = hyper_util::rt::TokioIo<novarocks_transport_io::BoxedIo>,
    Error = std::io::Error,
    Future = impl Send,
> + Clone
+ Send
+ 'static {
    tower::service_fn(move |_| {
        let connector = connector.clone();
        let admission = admission.clone();
        async move {
            let dial = admission
                .as_ref()
                .map(|admission| match dial {
                    NativeDial::Backend(class, key) => admission.try_dial(class, key),
                    NativeDial::Frontend(lane) => admission.try_dial_lane(lane),
                })
                .transpose()
                .map_err(|error| {
                    std::io::Error::new(error.kind(), "native dial admission refused")
                })?;
            let io = connector.connect().await.map_err(std::io::Error::other)?;
            let io: novarocks_transport_io::BoxedIo = match dial {
                Some(dial) => Box::new(dial.established_io(io)?),
                None => io,
            };
            Ok(hyper_util::rt::TokioIo::new(io))
        }
    })
}

/// A Frontend's admitted connector for one Backend lane.
pub fn frontend_lane_connector(
    connector: novarocks_native_trust::NativeEndpointConnector,
    admission: NativeTransportAdmission,
    lane: FrontendNativeLane,
) -> impl Service<
    hyper::http::Uri,
    Response = hyper_util::rt::TokioIo<novarocks_transport_io::BoxedIo>,
    Error = std::io::Error,
    Future = impl Send,
> + Clone
+ Send
+ 'static {
    admitted_connector_service(connector, Some(admission), NativeDial::Frontend(lane))
}

mod cold_dial;
pub use cold_dial::FrontendDialReservation;

mod reserved_call;
pub use reserved_call::{ReservedNativeCall, SelectedNativeCall};

#[cfg(test)]
#[path = "native_lane_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "native_lane_outgoing_exit_tests.rs"]
mod outgoing_exit_tests;
