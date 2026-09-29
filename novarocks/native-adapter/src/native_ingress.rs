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
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tonic::Status;
use tonic::codegen::Body as HttpBody;
use tower::{Service, ServiceExt};

use crate::backend_metrics;
use crate::native_server::NativeIngressConfig;

const LOCAL_ENTRY_CAP: Duration = Duration::from_secs(300);
const GRPC_FRAME_HEADER_BYTES: usize = 5;

fn ingress_capacity_status(detail: &'static str, reason: &'static str) -> Status {
    let mut status = Status::resource_exhausted(detail);
    status.metadata_mut().insert(
        "x-novarocks-ingress-rejection",
        tonic::metadata::MetadataValue::from_static(reason),
    );
    status
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

    async fn acquire(&self, deadline: Instant) -> Result<RunningPermit, Status> {
        if Instant::now() >= deadline {
            self.reject("waiting_deadline");
            return Err(Status::deadline_exceeded("native ingress deadline elapsed"));
        }
        if let Ok(permit) = Arc::clone(&self.running).try_acquire_owned() {
            if Instant::now() >= deadline {
                self.reject("waiting_deadline");
                return Err(Status::deadline_exceeded("native ingress deadline elapsed"));
            }
            return Ok(RunningPermit::new(permit, self.class, self.metrics));
        }
        let wait_permit = Arc::clone(&self.waiting).try_acquire_owned().map_err(|_| {
            self.reject("waiting_capacity");
            ingress_capacity_status(
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
                    Status::deadline_exceeded("native ingress waiting deadline elapsed")
                })?
                .map_err(|_| {
                    self.reject("closed");
                    Status::unavailable("native ingress admission closed")
                })?;
        if Instant::now() >= deadline {
            self.reject("waiting_deadline");
            return Err(Status::deadline_exceeded("native ingress deadline elapsed"));
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
    Control,
    Stream,
}

/// Sits after Native authentication and before the generated Tonic service.
// Design: ADR-0157 (docs/adr/ADR-0157-native-rpc-ingress-cost-boundaries.md)
#[derive(Clone)]
pub struct NativeIngressService<S> {
    inner: S,
    ordinary: Gate,
    control: Gate,
    config: NativeIngressConfig,
    control_path: String,
    exchange_path: String,
    subscribe_path: String,
    backend_metrics: bool,
}

impl<S> NativeIngressService<S> {
    pub fn new(
        inner: S,
        config: NativeIngressConfig,
        service_name: &str,
        backend_metrics: bool,
    ) -> Self {
        let prefix = format!("/{service_name}/");
        Self {
            inner,
            ordinary: Gate::new(
                config.ordinary_running,
                config.ordinary_waiting,
                "ordinary",
                backend_metrics,
                config.ordinary_request_max_bytes,
            ),
            control: Gate::new(
                config.control_running,
                config.control_waiting,
                "control",
                backend_metrics,
                config.control_request_max_bytes,
            ),
            config,
            control_path: format!("{prefix}ApplyTaskControlOperations"),
            exchange_path: format!("{prefix}Exchange"),
            subscribe_path: format!("{prefix}SubscribeTaskStatus"),
            backend_metrics,
        }
    }

    fn classify(&self, path: &str) -> MethodClass {
        if path == self.control_path {
            MethodClass::Control
        } else if path == self.exchange_path || path == self.subscribe_path {
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
        if !self.backend_metrics {
            // Frontend Native RPCs share the listener runtime sizing, but
            // BE-local task admission must not govern FE control traffic.
            let mut inner = self.inner.clone();
            return Box::pin(async move { inner.ready().await?.call(request).await });
        }
        // Record arrival synchronously: an async worker may not poll the
        // returned future immediately, and that delay consumes the request's
        // original ingress deadline.
        let arrival = Instant::now();
        let class = self.classify(request.uri().path());
        let gate = match class {
            MethodClass::Control => self.control.clone(),
            MethodClass::Ordinary | MethodClass::Stream => self.ordinary.clone(),
        };
        let config = self.config;
        let backend_metrics = self.backend_metrics;
        let mut inner = self.inner.clone();
        Box::pin(async move {
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
                Err(status) => return Ok(status.into_http()),
            };
            let ownership = Arc::new(NativeIngressOwnership {
                _permit: permit,
                arrival,
                deadline,
            });
            let body_limit = match class {
                MethodClass::Control => Some(config.control_request_max_bytes),
                MethodClass::Ordinary => Some(config.ordinary_request_max_bytes),
                MethodClass::Stream => None,
            };
            let mut request = request;
            if let Some(limit) = body_limit {
                let Some(total_limit) = limit.checked_add(GRPC_FRAME_HEADER_BYTES) else {
                    return Ok(Status::internal("native ingress frame limit overflow").into_http());
                };
                if request
                    .headers()
                    .get(header::CONTENT_LENGTH)
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| value.parse::<usize>().ok())
                    .is_some_and(|length| length > total_limit)
                {
                    gate.reject("body_limit");
                    return Ok(ingress_capacity_status(
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
                    return Ok(
                        Status::deadline_exceeded("native ingress deadline elapsed").into_http()
                    );
                }
            };
            if class == MethodClass::Stream {
                // Stream establishment is bounded, while its long-lived data
                // follows the Exchange/status owners' separate frame limits.
                return Ok(response);
            }
            Ok(response.map(|body| {
                tonic::body::boxed(OwnedResponseBody::new(
                    body,
                    ownership,
                    gate.class,
                    backend_metrics,
                ))
            }))
        })
    }
}

fn entry_deadline(headers: &axum::http::HeaderMap, arrival: Instant) -> Result<Instant, Status> {
    let timeout = match headers
        .get_all("grpc-timeout")
        .iter()
        .collect::<Vec<_>>()
        .as_slice()
    {
        [] => LOCAL_ENTRY_CAP,
        [value] => parse_grpc_timeout(value)?.min(LOCAL_ENTRY_CAP),
        _ => return Err(Status::invalid_argument("duplicate grpc-timeout header")),
    };
    arrival
        .checked_add(timeout)
        .ok_or_else(|| Status::invalid_argument("grpc-timeout exceeds local time range"))
}

fn parse_grpc_timeout(value: &axum::http::HeaderValue) -> Result<Duration, Status> {
    let text = value
        .to_str()
        .map_err(|_| Status::invalid_argument("invalid grpc-timeout header"))?;
    if !(2..=9).contains(&text.len()) {
        return Err(Status::invalid_argument("invalid grpc-timeout header"));
    }
    let (digits, unit) = text.split_at(text.len() - 1);
    if !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(Status::invalid_argument("invalid grpc-timeout header"));
    }
    let amount = digits
        .parse::<u64>()
        .map_err(|_| Status::invalid_argument("invalid grpc-timeout header"))?;
    let nanos = match unit {
        "H" => 3_600_000_000_000_u64,
        "M" => 60_000_000_000_u64,
        "S" => 1_000_000_000_u64,
        "m" => 1_000_000_u64,
        "u" => 1_000_u64,
        "n" => 1_u64,
        _ => return Err(Status::invalid_argument("invalid grpc-timeout header")),
    };
    let duration = amount
        .checked_mul(nanos)
        .ok_or_else(|| Status::invalid_argument("grpc-timeout is too large"))?;
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
            matches!(gate.acquire(Instant::now()).await, Err(status) if status.code() == tonic::Code::DeadlineExceeded)
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
        let response = NativeIngressService::new(service, config, "Test", false)
            .oneshot(Request::new(Body::empty()))
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
        *request.uri_mut() = "/Test/ApplyTaskOperations".parse().unwrap();
        request
            .headers_mut()
            .insert("grpc-timeout", "10m".parse().unwrap());
        let mut ingress = NativeIngressService::new(service, config, "Test", true);
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
