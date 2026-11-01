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

//! Per-listener admission before Tonic reads or decodes a request body.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::http::{Request, Response, header};
use bytes::Bytes;
use hyper::body::Frame;
use novarocks_proto_codec::native_rpc::{NativeBodyKind, NativeEndpointDomain, NativeRpcMethod};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tonic::codegen::Body as HttpBody;
use tonic::{Code, Status};
use tower::{Service, ServiceExt};

use crate::backend_metrics;
use crate::native_lane::NativeLaneStreamBody;
use crate::native_server::NativeIngressConfig;
use crate::native_transport_admission::NativeTransportAdmission;

const LOCAL_ENTRY_CAP: Duration = Duration::from_secs(300);
const GRPC_FRAME_HEADER_BYTES: usize = 5;

fn ingress_capacity_status(detail: &'static str, reason: &'static str) -> Status {
    let mut status = Status::new(Code::ResourceExhausted, detail);
    status.metadata_mut().insert(
        "x-novarocks-ingress-rejection",
        tonic::metadata::MetadataValue::from_static(reason),
    );
    status
}

#[derive(Debug)]
struct IngressFailure {
    status: Status,
    reason: Option<&'static str>,
}

impl IngressFailure {
    fn capacity(detail: &'static str, reason: &'static str) -> Self {
        Self {
            status: Status::new(Code::ResourceExhausted, detail),
            reason: Some(reason),
        }
    }

    fn into_http(self) -> Response<tonic::body::BoxBody> {
        let mut status = self.status;
        if let Some(reason) = self.reason {
            status.metadata_mut().insert(
                "x-novarocks-ingress-rejection",
                tonic::metadata::MetadataValue::from_static(reason),
            );
        }
        status.into_http()
    }
}

impl From<Status> for IngressFailure {
    fn from(status: Status) -> Self {
        Self {
            status,
            reason: None,
        }
    }
}

/// A request's original arrival and its locally bounded header deadline.
/// The permit follows every clone held by a handler, closure, or response.
pub struct NativeIngressOwnership {
    _permit: RunningPermit,
    arrival: Instant,
    deadline: Instant,
}

impl NativeIngressOwnership {
    pub fn arrival(&self) -> Instant {
        self.arrival
    }

    pub fn deadline(&self) -> Instant {
        self.deadline
    }
}

/// A real one-slot ingress owner for deterministic adapter cancellation tests.
#[cfg(test)]
pub(crate) async fn one_slot_test_ownership() -> (Arc<NativeIngressOwnership>, Arc<Semaphore>) {
    let gate = Gate::new(1, 0, "ordinary", false, 1024);
    let arrival = Instant::now();
    let deadline = arrival + LOCAL_ENTRY_CAP;
    let permit = gate
        .acquire(deadline)
        .await
        .expect("one available running slot");
    (
        Arc::new(NativeIngressOwnership {
            _permit: permit,
            arrival,
            deadline,
        }),
        gate.running,
    )
}

#[derive(Clone)]
struct Gate {
    running: Arc<Semaphore>,
    waiting: Arc<Semaphore>,
    class: &'static str,
    metrics: bool,
    wait_started: Arc<Mutex<BTreeMap<u64, i64>>>,
    next_wait_id: Arc<AtomicU64>,
}

impl Gate {
    fn new(
        running: usize,
        waiting: usize,
        class: &'static str,
        metrics: bool,
        request_limit: usize,
    ) -> Self {
        if metrics {
            backend_metrics::initialize_native_ingress_class(
                class,
                running,
                waiting,
                request_limit,
            );
        }
        Self {
            running: Arc::new(Semaphore::new(running)),
            waiting: Arc::new(Semaphore::new(waiting)),
            class,
            metrics,
            wait_started: Arc::new(Mutex::new(BTreeMap::new())),
            next_wait_id: Arc::new(AtomicU64::new(0)),
        }
    }

    async fn acquire(&self, deadline: Instant) -> Result<RunningPermit, IngressFailure> {
        if Instant::now() >= deadline {
            self.reject("waiting_deadline");
            return Err(
                Status::new(Code::DeadlineExceeded, "native ingress deadline elapsed").into(),
            );
        }
        if let Ok(permit) = Arc::clone(&self.running).try_acquire_owned() {
            if Instant::now() >= deadline {
                self.reject("waiting_deadline");
                return Err(
                    Status::new(Code::DeadlineExceeded, "native ingress deadline elapsed").into(),
                );
            }
            return Ok(RunningPermit::new(permit, self.class, self.metrics));
        }
        let wait_permit = Arc::clone(&self.waiting).try_acquire_owned().map_err(|_| {
            self.reject("waiting_capacity");
            IngressFailure::capacity(
                "native ingress waiting capacity exhausted",
                "waiting_capacity",
            )
        })?;
        let _wait = WaitingPermit::new(wait_permit, self.clone());
        let permit =
            tokio::time::timeout_at(deadline.into(), Arc::clone(&self.running).acquire_owned())
                .await
                .map_err(|_| {
                    self.reject("waiting_deadline");
                    Status::new(
                        Code::DeadlineExceeded,
                        "native ingress waiting deadline elapsed",
                    )
                })?
                .map_err(|_| {
                    self.reject("closed");
                    Status::new(Code::Unavailable, "native ingress admission closed")
                })?;
        if Instant::now() >= deadline {
            self.reject("waiting_deadline");
            return Err(
                Status::new(Code::DeadlineExceeded, "native ingress deadline elapsed").into(),
            );
        }
        drop(_wait);
        Ok(RunningPermit::new(permit, self.class, self.metrics))
    }

    fn reject(&self, reason: &'static str) {
        if self.metrics {
            backend_metrics::native_ingress_rejected(self.class, reason);
        }
    }
}

fn unix_time_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs().min(i64::MAX as u64) as i64)
}

struct RunningPermit {
    _permit: OwnedSemaphorePermit,
    class: &'static str,
    metrics: bool,
}

impl RunningPermit {
    fn new(permit: OwnedSemaphorePermit, class: &'static str, metrics: bool) -> Self {
        if metrics {
            backend_metrics::native_ingress_slot_change(class, "running", 1);
        }
        Self {
            _permit: permit,
            class,
            metrics,
        }
    }
}

impl Drop for RunningPermit {
    fn drop(&mut self) {
        if self.metrics {
            backend_metrics::native_ingress_slot_change(self.class, "running", -1);
        }
    }
}

struct WaitingPermit {
    _permit: OwnedSemaphorePermit,
    gate: Gate,
    id: u64,
    started: Instant,
}

impl WaitingPermit {
    fn new(permit: OwnedSemaphorePermit, gate: Gate) -> Self {
        let id = gate.next_wait_id.fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();
        if gate.metrics {
            let mut started_waits = gate.wait_started.lock().expect("Native metrics wait lock");
            started_waits.insert(id, unix_time_seconds());
            backend_metrics::native_ingress_slot_change(gate.class, "waiting", 1);
            let oldest = started_waits.values().copied().min().unwrap_or(0);
            backend_metrics::native_ingress_oldest_wait_since(gate.class, oldest);
        }
        Self {
            _permit: permit,
            gate,
            id,
            started,
        }
    }
}

impl Drop for WaitingPermit {
    fn drop(&mut self) {
        if self.gate.metrics {
            let mut started_waits = self
                .gate
                .wait_started
                .lock()
                .expect("Native metrics wait lock");
            started_waits.remove(&self.id);
            backend_metrics::native_ingress_slot_change(self.gate.class, "waiting", -1);
            let oldest = started_waits.values().copied().min().unwrap_or(0);
            backend_metrics::native_ingress_oldest_wait_since(self.gate.class, oldest);
            backend_metrics::native_ingress_waited(self.gate.class, self.started.elapsed());
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum MethodClass {
    Ordinary,
    RootResult,
    Control,
    Stream,
}

/// Sits after Native authentication and before the generated Tonic service.
///
/// On an admitted listener every request of a known lane first takes one of
/// that lane's served stream positions. The position is held by the response
/// body wrapper until the body ends, errors (including a client reset), or is
/// dropped -- never only until the handler returns. Server-streaming methods
/// hold theirs for the life of the stream. A refusal answers
/// `RESOURCE_EXHAUSTED` before any execution gate or decoder.
// Design: ADR-0157 (docs/adr/ADR-0157-native-rpc-ingress-cost-boundaries.md)
#[derive(Clone)]
pub struct NativeIngressService<S> {
    inner: S,
    ordinary: Gate,
    root_result: Gate,
    control: Gate,
    config: NativeIngressConfig,
    domain: NativeEndpointDomain,
    backend_metrics: bool,
    lane_streams: Option<NativeTransportAdmission>,
}

impl<S> NativeIngressService<S> {
    /// Install one explicit endpoint domain. The service name is diagnostic;
    /// only exact frozen manifest paths admit requests. Metrics never select
    /// admission behavior or the frontend bypass.
    pub fn new(
        inner: S,
        config: NativeIngressConfig,
        _service_name: &str,
        backend_metrics: bool,
        domain: NativeEndpointDomain,
    ) -> Self {
        Self {
            inner,
            ordinary: Gate::new(
                if domain == NativeEndpointDomain::BackendData {
                    config.ordinary_running
                } else {
                    0
                },
                if domain == NativeEndpointDomain::BackendData {
                    config.ordinary_waiting
                } else {
                    0
                },
                "ordinary",
                backend_metrics && domain == NativeEndpointDomain::BackendData,
                config.ordinary_request_max_bytes,
            ),
            root_result: Gate::new(
                if domain == NativeEndpointDomain::BackendData {
                    crate::native_transport_admission::root_result_ingress_owner_limit(
                        crate::native_transport_admission::TransportRole::Backend,
                        &novarocks_execution_contract::native_result_support::NativeResultSupportGeometry::V1,
                    ).expect("frozen root ingress geometry fits the target")
                } else {
                    0
                },
                0,
                "root_result",
                backend_metrics && domain == NativeEndpointDomain::BackendData,
                novarocks_result_contract::RootProfileV1::ENVELOPE_BYTES,
            ),
            control: Gate::new(
                if domain == NativeEndpointDomain::BackendControl {
                    config.control_running
                } else {
                    0
                },
                if domain == NativeEndpointDomain::BackendControl {
                    config.control_waiting
                } else {
                    0
                },
                "control",
                backend_metrics && domain == NativeEndpointDomain::BackendControl,
                config.control_request_max_bytes,
            ),
            config,
            domain,
            backend_metrics,
            lane_streams: None,
        }
    }

    /// Hold each request's lane stream position from `admission` until its
    /// response body exits.
    pub fn with_lane_streams(mut self, admission: Option<NativeTransportAdmission>) -> Self {
        if self.domain == NativeEndpointDomain::BackendData
            && let Some(admission) = &admission
        {
            self.root_result = Gate::new(
                admission.root_result_ingress_owner_limit(),
                0,
                "root_result",
                self.backend_metrics,
                novarocks_result_contract::RootProfileV1::ENVELOPE_BYTES,
            );
        }
        self.lane_streams = admission;
        self
    }

    fn classify(&self, method: NativeRpcMethod) -> MethodClass {
        if self.domain == NativeEndpointDomain::BackendControl {
            MethodClass::Control
        } else if method == NativeRpcMethod::FetchRootResult {
            MethodClass::RootResult
        } else if method.contract().body == NativeBodyKind::ServerStream {
            MethodClass::Stream
        } else {
            MethodClass::Ordinary
        }
    }
}

impl<S> Service<Request<Body>> for NativeIngressService<S>
where
    S: Service<Request<Body>, Response = Response<tonic::body::BoxBody>, Error = Infallible>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    type Response = Response<tonic::body::BoxBody>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        // Exact endpoint admission precedes every clone, gate, decoder, and
        // response-position claim. Authentication wraps this service.
        let Some(method) = NativeRpcMethod::from_path(request.uri().path())
            .filter(|method| method.is_allowed_at(self.domain))
        else {
            drop(request);
            let response = Status::new(
                Code::Unimplemented,
                "native RPC method is unavailable on this endpoint",
            )
            .into_http();
            return Box::pin(async move { Ok(response) });
        };
        // Record arrival synchronously: an async worker may not poll the
        // returned future immediately, and that delay consumes the request's
        // original ingress deadline, including response capacity preparation.
        let arrival = Instant::now();
        // The stream position precedes every execution gate and decoder; a
        // refusal holds nothing.
        let stream = match &self.lane_streams {
            Some(admission) => match admission.try_incoming_stream(method) {
                Some(stream) => stream,
                None => {
                    match self.classify(method) {
                        MethodClass::Control => self.control.reject("lane_streams"),
                        MethodClass::RootResult => self.root_result.reject("lane_streams"),
                        MethodClass::Ordinary | MethodClass::Stream => {
                            self.ordinary.reject("lane_streams")
                        }
                    }
                    drop(request);
                    let response = IngressFailure::capacity(
                        "native lane stream positions exhausted",
                        "lane_streams",
                    )
                    .into_http();
                    return Box::pin(async move { Ok(response) });
                }
            },
            None => None,
        };
        if self.domain == NativeEndpointDomain::FrontendMembership {
            // Membership has no BE task gate. Its transport stream position
            // still precedes dispatch and follows the response body's exit.
            let mut inner = self.inner.clone();
            return Box::pin(async move {
                let response = inner.ready().await?.call(request).await?;
                Ok(response.map(|body| NativeLaneStreamBody::boxed(body, stream)))
            });
        }
        let class = self.classify(method);
        let gate = match class {
            MethodClass::Control => self.control.clone(),
            MethodClass::RootResult => self.root_result.clone(),
            MethodClass::Ordinary | MethodClass::Stream => self.ordinary.clone(),
        };
        let config = self.config;
        let backend_metrics = self.backend_metrics;
        let mut inner = self.inner.clone();
        let admitted = async move {
            if backend_metrics {
                backend_metrics::native_async_first_poll_lag(gate.class, arrival.elapsed());
            }
            let deadline = match entry_deadline(request.headers(), arrival) {
                Ok(deadline) => deadline,
                Err(status) => {
                    gate.reject("invalid_deadline");
                    return Ok(status.into_http());
                }
            };
            let permit = match gate.acquire(deadline).await {
                Ok(permit) => permit,
                Err(failure) => return Ok(failure.into_http()),
            };
            let ownership = Arc::new(NativeIngressOwnership {
                _permit: permit,
                arrival,
                deadline,
            });
            let body_limit = match class {
                MethodClass::Control => Some(config.control_request_max_bytes),
                MethodClass::RootResult => {
                    Some(novarocks_result_contract::RootProfileV1::ENVELOPE_BYTES)
                }
                MethodClass::Ordinary => Some(config.ordinary_request_max_bytes),
                MethodClass::Stream => None,
            };
            let mut request = request;
            if let Some(limit) = body_limit {
                let Some(total_limit) = limit.checked_add(GRPC_FRAME_HEADER_BYTES) else {
                    return Ok(
                        Status::new(Code::Internal, "native ingress frame limit overflow")
                            .into_http(),
                    );
                };
                if request
                    .headers()
                    .get(header::CONTENT_LENGTH)
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| value.parse::<usize>().ok())
                    .is_some_and(|length| length > total_limit)
                {
                    gate.reject("body_limit");
                    return Ok(IngressFailure::capacity(
                        "native request body exceeds method limit",
                        "body_limit",
                    )
                    .into_http());
                }
                let (parts, body) = request.into_parts();
                request = Request::from_parts(
                    parts,
                    Body::new(LimitedRequestBody::new(
                        body,
                        total_limit,
                        gate.class,
                        backend_metrics,
                    )),
                );
            }
            request.extensions_mut().insert(Arc::clone(&ownership));
            // Bound the complete receive and unary dispatch, including a
            // client that stops sending body frames after acquiring a slot.
            let response = match tokio::time::timeout_at(deadline.into(), async {
                inner.ready().await?.call(request).await
            })
            .await
            {
                Ok(result) => result?,
                Err(_) => {
                    gate.reject("running_deadline");
                    return Ok(Status::new(
                        Code::DeadlineExceeded,
                        "native ingress deadline elapsed",
                    )
                    .into_http());
                }
            };
            if class == MethodClass::Stream {
                // Stream establishment is bounded, while its long-lived data
                // follows the Exchange/status owners' separate frame limits.
                return Ok(response);
            }
            Ok::<_, Infallible>(response.map(|body| {
                tonic::body::boxed(OwnedResponseBody::new(
                    body,
                    ownership,
                    gate.class,
                    backend_metrics,
                ))
            }))
        };
        Box::pin(async move {
            // Until the response exists the stream position is held by this
            // future; afterwards by the body, to the body's public exit.
            let response = admitted.await?;
            Ok(response.map(|body| NativeLaneStreamBody::boxed(body, stream)))
        })
    }
}

fn entry_deadline(headers: &axum::http::HeaderMap, arrival: Instant) -> Result<Instant, Status> {
    let mut timeouts = headers.get_all("grpc-timeout").iter();
    let timeout = match (timeouts.next(), timeouts.next()) {
        (None, None) => LOCAL_ENTRY_CAP,
        (Some(value), None) => parse_grpc_timeout(value)?.min(LOCAL_ENTRY_CAP),
        _ => {
            return Err(Status::new(
                Code::InvalidArgument,
                "duplicate grpc-timeout header",
            ));
        }
    };
    arrival.checked_add(timeout).ok_or_else(|| {
        Status::new(
            Code::InvalidArgument,
            "grpc-timeout exceeds local time range",
        )
    })
}

fn parse_grpc_timeout(value: &axum::http::HeaderValue) -> Result<Duration, Status> {
    let text = value
        .to_str()
        .map_err(|_| Status::new(Code::InvalidArgument, "invalid grpc-timeout header"))?;
    if !(2..=9).contains(&text.len()) {
        return Err(Status::new(
            Code::InvalidArgument,
            "invalid grpc-timeout header",
        ));
    }
    let (digits, unit) = text.split_at(text.len() - 1);
    if !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(Status::new(
            Code::InvalidArgument,
            "invalid grpc-timeout header",
        ));
    }
    let amount = digits
        .parse::<u64>()
        .map_err(|_| Status::new(Code::InvalidArgument, "invalid grpc-timeout header"))?;
    let nanos = match unit {
        "H" => 3_600_000_000_000_u64,
        "M" => 60_000_000_000_u64,
        "S" => 1_000_000_000_u64,
        "m" => 1_000_000_u64,
        "u" => 1_000_u64,
        "n" => 1_u64,
        _ => {
            return Err(Status::new(
                Code::InvalidArgument,
                "invalid grpc-timeout header",
            ));
        }
    };
    let duration = amount
        .checked_mul(nanos)
        .ok_or_else(|| Status::new(Code::InvalidArgument, "grpc-timeout is too large"))?;
    Ok(Duration::from_nanos(duration))
}

struct LimitedRequestBody {
    inner: Pin<Box<Body>>,
    remaining: usize,
    observed: usize,
    class: &'static str,
    metrics: bool,
    outcome: &'static str,
    terminal: bool,
}

impl LimitedRequestBody {
    fn new(inner: Body, limit: usize, class: &'static str, metrics: bool) -> Self {
        Self {
            inner: Box::pin(inner),
            remaining: limit,
            observed: 0,
            class,
            metrics,
            outcome: "dropped",
            terminal: false,
        }
    }
}

impl Drop for LimitedRequestBody {
    fn drop(&mut self) {
        if self.metrics {
            backend_metrics::native_ingress_request_body_bytes(
                self.class,
                self.outcome,
                self.observed,
            );
        }
    }
}

impl HttpBody for LimitedRequestBody {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        if self.terminal {
            return Poll::Ready(None);
        }
        match self.inner.as_mut().poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref() {
                    if data.len() > self.remaining {
                        self.outcome = "over_limit";
                        self.terminal = true;
                        if self.metrics {
                            backend_metrics::native_ingress_rejected(self.class, "body_limit");
                        }
                        return Poll::Ready(Some(Err(ingress_capacity_status(
                            "native request body exceeds method limit",
                            "body_limit",
                        ))));
                    }
                    self.remaining -= data.len();
                    self.observed += data.len();
                }
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(Some(Err(error))) => {
                self.outcome = "error";
                self.terminal = true;
                Poll::Ready(Some(Err(Status::internal(error.to_string()))))
            }
            Poll::Ready(None) => {
                self.outcome = "complete";
                self.terminal = true;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

struct OwnedResponseBody {
    inner: Pin<Box<tonic::body::BoxBody>>,
    ownership: Arc<NativeIngressOwnership>,
    class: &'static str,
    metrics: bool,
}

impl OwnedResponseBody {
    fn new(
        inner: tonic::body::BoxBody,
        ownership: Arc<NativeIngressOwnership>,
        class: &'static str,
        metrics: bool,
    ) -> Self {
        if metrics {
            backend_metrics::native_response_backing_change(class, "body", 1);
        }
        Self {
            inner: Box::pin(inner),
            ownership,
            class,
            metrics,
        }
    }
}

impl Drop for OwnedResponseBody {
    fn drop(&mut self) {
        if self.metrics {
            backend_metrics::native_response_backing_change(self.class, "body", -1);
        }
    }
}

impl HttpBody for OwnedResponseBody {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        match self.inner.as_mut().poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => match frame.into_data() {
                Ok(bytes) => Poll::Ready(Some(Ok(Frame::data(Bytes::from_owner(
                    OwnedResponseBytes::new(
                        bytes,
                        Arc::clone(&self.ownership),
                        self.class,
                        self.metrics,
                    ),
                ))))),
                Err(frame) => Poll::Ready(Some(Ok(frame))),
            },
            Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(error))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
    // Preserve the inner protocol's empty/trailers-only response. Ownership
    // still exits through Drop; inspecting these facts releases no permit.
    fn is_end_stream(&self) -> bool {
        self.inner.as_ref().get_ref().is_end_stream()
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        self.inner.as_ref().get_ref().size_hint()
    }
}

/// The h2 writer may retain a DATA `Bytes` after the response Body ends. The
/// owner follows the actual backing without copying the payload.
struct OwnedResponseBytes {
    bytes: Bytes,
    _ownership: Arc<NativeIngressOwnership>,
    class: &'static str,
    metrics: bool,
}

impl OwnedResponseBytes {
    fn new(
        bytes: Bytes,
        ownership: Arc<NativeIngressOwnership>,
        class: &'static str,
        metrics: bool,
    ) -> Self {
        if metrics {
            backend_metrics::native_response_backing_change(class, "data_backing", 1);
        }
        Self {
            bytes,
            _ownership: ownership,
            class,
            metrics,
        }
    }
}

impl AsRef<[u8]> for OwnedResponseBytes {
    fn as_ref(&self) -> &[u8] {
        self.bytes.as_ref()
    }
}

impl Drop for OwnedResponseBytes {
    fn drop(&mut self) {
        if self.metrics {
            backend_metrics::native_response_backing_change(self.class, "data_backing", -1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct OneDataFrame(Option<Bytes>);

    impl HttpBody for OneDataFrame {
        type Data = Bytes;
        type Error = Status;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            Poll::Ready(self.0.take().map(|bytes| Ok(Frame::data(bytes))))
        }
    }

    #[test]
    fn method_manifest_is_the_only_ingress_classification_source() {
        let data = NativeIngressService::new(
            (),
            NativeIngressConfig::default(),
            "Ignored",
            false,
            NativeEndpointDomain::BackendData,
        );
        let control = NativeIngressService::new(
            (),
            NativeIngressConfig::default(),
            "Ignored",
            false,
            NativeEndpointDomain::BackendControl,
        );
        for method in [
            NativeRpcMethod::Heartbeat,
            NativeRpcMethod::ApplyTaskControlOperations,
        ] {
            assert!(matches!(control.classify(method), MethodClass::Control));
        }
        for method in [
            NativeRpcMethod::ApplyTaskOperations,
            NativeRpcMethod::FetchTaskResult,
            NativeRpcMethod::DeliverExchangeUnit,
            NativeRpcMethod::RegisterExchangeDemand,
        ] {
            assert!(matches!(data.classify(method), MethodClass::Ordinary));
        }
        assert!(matches!(
            data.classify(NativeRpcMethod::FetchRootResult),
            MethodClass::RootResult
        ));
        assert!(matches!(
            data.classify(NativeRpcMethod::SubscribeTaskStatus),
            MethodClass::Stream
        ));
        assert!(matches!(
            data.classify(NativeRpcMethod::SubscribeExchangeGrants),
            MethodClass::Stream
        ));
        assert_eq!(NativeRpcMethod::from_path("/Other/Heartbeat"), None);
    }

    #[tokio::test]
    async fn root_ingress_progresses_past_200_without_borrowing_ordinary_or_control() {
        use crate::native_lane::NativeLane;
        let admission = NativeTransportAdmission::new().unwrap();
        let service = tower::service_fn(|_request: Request<Body>| async {
            Ok::<_, Infallible>(Response::new(tonic::body::boxed(OneDataFrame(Some(
                Bytes::from_static(b"payload"),
            )))))
        });
        let config = NativeIngressConfig {
            ordinary_running: 1,
            ordinary_waiting: 0,
            ..Default::default()
        };
        let data = NativeIngressService::new(
            service,
            config,
            "test",
            false,
            NativeEndpointDomain::BackendData,
        )
        .with_lane_streams(Some(admission.clone()));
        let request = |method: NativeRpcMethod| {
            Request::builder()
                .uri(method.contract().path)
                .body(Body::empty())
                .unwrap()
        };
        let ordinary = data
            .clone()
            .oneshot(request(NativeRpcMethod::ApplyTaskOperations))
            .await
            .unwrap();
        assert_eq!(data.ordinary.running.available_permits(), 0);
        let root_limit = data.root_result.running.available_permits();
        let stream_limit = admission.incoming_streams(NativeLane::ResultData).limit();
        let mut bodies = Vec::new();
        for _ in 0..256 {
            let response = data
                .clone()
                .oneshot(request(NativeRpcMethod::FetchRootResult))
                .await
                .unwrap();
            assert_eq!(response.status(), axum::http::StatusCode::OK);
            assert!(response.headers().get("grpc-status").is_none());
            bodies.push(response.into_body());
        }
        assert_eq!(
            data.root_result.running.available_permits(),
            root_limit - 256
        );
        assert_eq!(
            admission
                .incoming_streams(NativeLane::ResultData)
                .available(),
            stream_limit - 256
        );
        let control = NativeIngressService::new(
            service,
            config,
            "test",
            false,
            NativeEndpointDomain::BackendControl,
        )
        .with_lane_streams(Some(admission.clone()));
        let response = control
            .clone()
            .oneshot(request(NativeRpcMethod::Heartbeat))
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        assert_eq!(
            control.control.running.available_permits(),
            config.control_running - 1
        );
        let mut data_frames = Vec::new();
        for mut body in bodies {
            let frame = std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
                .await
                .unwrap()
                .unwrap();
            data_frames.push(frame);
            assert!(
                std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
                    .await
                    .is_none()
            );
            drop(body);
        }
        assert_eq!(
            admission
                .incoming_streams(NativeLane::ResultData)
                .available(),
            stream_limit
        );
        assert_eq!(
            data.root_result.running.available_permits(),
            root_limit - 256,
            "h2 DATA aliases still own the ingress positions after public stream EOF"
        );
        assert_eq!(data.ordinary.running.available_permits(), 0);
        drop(data_frames);
        assert_eq!(data.root_result.running.available_permits(), root_limit);
        drop(ordinary);
        drop(response);
        assert_eq!(data.ordinary.running.available_permits(), 1);
        assert_eq!(
            control.control.running.available_permits(),
            config.control_running
        );
    }

    #[tokio::test]
    async fn oversized_root_request_refuses_before_handler_dispatch() {
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        let service = tower::service_fn(move |_request: Request<Body>| {
            let calls = Arc::clone(&observed);
            async move {
                calls.fetch_add(1, Ordering::Relaxed);
                Ok::<_, Infallible>(Response::new(tonic::body::empty_body()))
            }
        });
        let ingress = NativeIngressService::new(
            service,
            NativeIngressConfig::default(),
            "test",
            false,
            NativeEndpointDomain::BackendData,
        );
        let available = ingress.root_result.running.available_permits();
        let response = ingress
            .clone()
            .oneshot(
                Request::builder()
                    .uri(NativeRpcMethod::FetchRootResult.contract().path)
                    .header(
                        header::CONTENT_LENGTH,
                        novarocks_result_contract::RootProfileV1::ENVELOPE_BYTES
                            + GRPC_FRAME_HEADER_BYTES
                            + 1,
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.headers().get("grpc-status").unwrap(), "8");
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        drop(response);
        assert_eq!(ingress.root_result.running.available_permits(), available);
    }

    #[test]
    fn grpc_timeout_parser_preserves_units_and_rejects_bad_headers() {
        assert_eq!(
            parse_grpc_timeout(&"10S".parse().unwrap()).unwrap(),
            Duration::from_secs(10)
        );
        assert_eq!(
            parse_grpc_timeout(&"250m".parse().unwrap()).unwrap(),
            Duration::from_millis(250)
        );
        assert!(parse_grpc_timeout(&"123456789S".parse().unwrap()).is_err());
        assert!(parse_grpc_timeout(&"10x".parse().unwrap()).is_err());
        assert!(parse_grpc_timeout(&"0".parse().unwrap()).is_err());
    }

    #[tokio::test]
    async fn expired_deadline_never_takes_an_available_running_slot() {
        let gate = Gate::new(1, 0, "ordinary", false, 1024);
        assert!(
            matches!(gate.acquire(Instant::now()).await, Err(failure) if failure.status.code() == tonic::Code::DeadlineExceeded)
        );
        assert_eq!(gate.running.available_permits(), 1);
    }

    #[tokio::test]
    async fn over_limit_request_body_stops_before_decoder() {
        let mut body = LimitedRequestBody::new(Body::from("123456"), 5, "ordinary", false);
        let first = std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await;
        assert!(
            matches!(first, Some(Err(status)) if status.code() == tonic::Code::ResourceExhausted)
        );
        let second = std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await;
        assert!(
            second.is_none(),
            "an over-limit body must not resume reading"
        );
    }

    #[tokio::test]
    async fn admitted_empty_grpc_refusal_ends_in_headers_without_data() {
        let inner = tower::service_fn(|_request: Request<Body>| async {
            Ok::<_, Infallible>(Status::not_found("unknown context root").into_http())
        });
        let ingress = NativeIngressService::new(
            inner,
            NativeIngressConfig::default(),
            "test",
            false,
            NativeEndpointDomain::BackendData,
        );
        let capacity = ingress.root_result.running.available_permits();
        let running = Arc::clone(&ingress.root_result.running);
        let service = hyper::service::service_fn(move |request: Request<hyper::body::Incoming>| {
            let ingress = ingress.clone();
            async move { ingress.oneshot(request.map(Body::new)).await }
        });
        let (client_io, server_io) = tokio::io::duplex(65536);
        let server = tokio::spawn(async move {
            hyper::server::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new())
                .serve_connection(hyper_util::rt::TokioIo::new(server_io), service)
                .await
        });
        let (mut client, connection) = h2::client::handshake(client_io).await.unwrap();
        let driver = tokio::spawn(connection);
        let request = Request::builder()
            .method("POST")
            .uri(NativeRpcMethod::FetchRootResult.contract().path)
            .header(header::CONTENT_TYPE, "application/grpc")
            .body(())
            .unwrap();
        let (response, _) = client.send_request(request, true).unwrap();
        let response = tokio::time::timeout(Duration::from_secs(2), response)
            .await
            .unwrap()
            .unwrap();
        let status = response.headers().get("grpc-status").unwrap().clone();
        let mut body = response.into_body();
        let ended_in_headers = body.is_end_stream();
        let data = tokio::time::timeout(Duration::from_secs(2), body.data())
            .await
            .unwrap();
        drop(body);
        drop(client);
        driver.abort();
        server.abort();
        let _ = driver.await;
        let _ = server.await;
        assert_eq!(status, "5");
        assert!(
            ended_in_headers,
            "empty gRPC refusal lost HEADERS END_STREAM through the owner wrapper"
        );
        assert!(data.is_none(), "empty gRPC refusal emitted a DATA frame");
        assert_eq!(
            running.available_permits(),
            capacity,
            "actual empty response exit must release its ingress permit"
        );
    }

    #[tokio::test]
    async fn response_data_backing_retains_permit_after_body_drop() {
        let gate = Gate::new(1, 0, "ordinary", false, 1024);
        let permit = gate
            .acquire(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        let ownership = Arc::new(NativeIngressOwnership {
            _permit: permit,
            arrival: Instant::now(),
            deadline: Instant::now() + Duration::from_secs(1),
        });
        let mut body = OwnedResponseBody::new(
            tonic::body::boxed(OneDataFrame(Some(Bytes::from_static(b"payload")))),
            Arc::clone(&ownership),
            "ordinary",
            false,
        );
        drop(ownership);
        let frame = std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
            .await
            .unwrap()
            .unwrap();
        drop(body);
        assert_eq!(
            gate.running.available_permits(),
            0,
            "h2 still owns the response DATA backing"
        );
        drop(frame);
        assert_eq!(gate.running.available_permits(), 1);
    }

    #[tokio::test]
    async fn current_gate_observation_tracks_holder_without_a_rejection() {
        let class = "test_current_holder";
        let gate = Gate::new(1, 0, class, true, 1024);
        let registry = backend_metrics::BackendMetricsRegistry::new().unwrap();
        let has_used = |rendered: &str, value: i64| {
            rendered.lines().any(|line| {
                line.starts_with("novarocks_backend_native_ingress_slots{")
                    && line.contains("class=\"test_current_holder\"")
                    && line.contains("dimension=\"used\"")
                    && line.contains("phase=\"running\"")
                    && line.ends_with(&format!(" {value}"))
            })
        };
        let rendered = backend_metrics::render_metrics(&registry).unwrap();
        assert!(has_used(&rendered, 0));
        let permit = gate
            .acquire(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        let rendered = backend_metrics::render_metrics(&registry).unwrap();
        assert!(has_used(&rendered, 1));
        drop(permit);
        let rendered = backend_metrics::render_metrics(&registry).unwrap();
        assert!(has_used(&rendered, 0));
    }

    #[tokio::test]
    async fn frontend_listener_bypasses_backend_task_gate() {
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        let service = tower::service_fn(move |_request: Request<Body>| {
            let observed = Arc::clone(&observed);
            async move {
                observed.fetch_add(1, Ordering::Relaxed);
                Ok::<_, Infallible>(Response::new(tonic::body::empty_body()))
            }
        });
        let config = NativeIngressConfig {
            ordinary_running: 0,
            control_running: 0,
            ..NativeIngressConfig::default()
        };
        let response = NativeIngressService::new(
            service,
            config,
            "Ignored",
            false,
            NativeEndpointDomain::FrontendMembership,
        )
        .oneshot(
            Request::builder()
                .uri(NativeRpcMethod::AnnounceBackend.contract().path)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn deadline_starts_at_call_and_stops_a_stalled_body() {
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        let service = tower::service_fn(move |request: Request<Body>| {
            let observed = Arc::clone(&observed);
            async move {
                observed.fetch_add(1, Ordering::Relaxed);
                let mut body = Box::pin(request.into_body());
                let _ = std::future::poll_fn(|cx| body.as_mut().poll_frame(cx)).await;
                Ok::<_, Infallible>(Response::new(tonic::body::empty_body()))
            }
        });
        let config = NativeIngressConfig::default();
        let mut request = Request::new(Body::new(PendingBody));
        *request.uri_mut() = NativeRpcMethod::ApplyTaskOperations
            .contract()
            .path
            .parse()
            .unwrap();
        request
            .headers_mut()
            .insert("grpc-timeout", "10m".parse().unwrap());
        let mut ingress = NativeIngressService::new(
            service,
            config,
            "Ignored",
            true,
            NativeEndpointDomain::BackendData,
        );
        let future = ingress.call(request);
        tokio::time::sleep(Duration::from_millis(20)).await;
        let response = tokio::time::timeout(Duration::from_millis(200), future)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            response.headers().get("grpc-status").unwrap(),
            "4",
            "an already-expired request must not reach the decoder"
        );
        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }

    struct PendingBody;

    impl HttpBody for PendingBody {
        type Data = Bytes;
        type Error = Infallible;

        fn poll_frame(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            Poll::Pending
        }
    }
}

#[cfg(test)]
#[path = "native_endpoint_domain_tests.rs"]
mod endpoint_domain_tests;
