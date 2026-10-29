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

//! CR2 lane tests: served and client stream positions held to the response
//! body's public exit, bootstrap and physical positions over real sockets,
//! and dial admission on every connector attempt.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Mutex;
use std::time::Duration;

use axum::body::Body;
use hyper::http::{Request, Response};
use novarocks_native_trust::{NativeIncomingAdapter, NativeProcessIdentity, NativeTrust};
use novarocks_proto_codec::native_rpc::{NativeEndpointDomain, NativeRpcMethod};
use novarocks_proto_models::{catalog, filter, novarocks as proto};
use tokio::sync::mpsc;
use tower::ServiceExt;

use super::*;
use crate::generated::nova_rocks_grpc_server::NovaRocksGrpc;
use crate::native_ingress::NativeIngressService;
use crate::native_server::{NativeIngressConfig, NativeRpcServerHandle};
use crate::native_transport_admission::{AdmissionDimensions, TransportRole};

const WATCHDOG: Duration = Duration::from_secs(8);

async fn bounded<F: Future>(future: F) -> F::Output {
    tokio::time::timeout(WATCHDOG, future)
        .await
        .expect("lane fixture exceeded its failure watchdog")
}

async fn eventually(mut condition: impl FnMut() -> bool, what: &str) {
    tokio::time::timeout(WATCHDOG, async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting until {what}"));
}

/// Records every observer notification.
#[derive(Default)]
struct Recorder(Mutex<Vec<String>>);

impl Recorder {
    fn events(&self) -> Vec<String> {
        self.0.lock().unwrap().clone()
    }
}

impl NativeTransportObserver for Recorder {
    fn refused(&self, class: TransportClass) {
        self.0
            .lock()
            .unwrap()
            .push(format!("refused:{}", class.label()));
    }
    fn lane_connections(&self, lane: NativeLane, delta: i64) {
        self.0
            .lock()
            .unwrap()
            .push(format!("connections:{}:{delta}", lane.label()));
    }
    fn lane_streams(&self, lane: NativeLane, direction: StreamDirection, delta: i64) {
        self.0.lock().unwrap().push(format!(
            "streams:{}:{}:{delta}",
            lane.label(),
            direction.label()
        ));
    }
}

// ---------------------------------------------------------------------------
// Served stream positions through the ingress layer.
// ---------------------------------------------------------------------------

/// A response body whose frames the test sends, then ends or fails.
struct ChannelBody(mpsc::UnboundedReceiver<Result<Bytes, tonic::Status>>);

impl HttpBody for ChannelBody {
    type Data = Bytes;
    type Error = tonic::Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        self.0
            .poll_recv(cx)
            .map(|frame| frame.map(|frame| frame.map(Frame::data)))
    }
}

type Frames = mpsc::UnboundedSender<Result<Bytes, tonic::Status>>;

/// A handler that returns at once with a body the test drives; each call
/// hands its body sender to the test.
#[derive(Clone)]
struct DrivenHandler(mpsc::UnboundedSender<Frames>);

impl Service<Request<Body>> for DrivenHandler {
    type Response = Response<BoxBody>;
    type Error = Infallible;
    type Future = std::future::Ready<Result<Response<BoxBody>, Infallible>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _request: Request<Body>) -> Self::Future {
        let (frames_tx, frames) = mpsc::unbounded_channel();
        self.0.send(frames_tx).unwrap();
        std::future::ready(Ok(Response::new(tonic::body::boxed(ChannelBody(frames)))))
    }
}

fn ingress(
    admission: &NativeTransportAdmission,
) -> (
    NativeIngressService<DrivenHandler>,
    mpsc::UnboundedReceiver<Frames>,
) {
    let (senders_tx, senders) = mpsc::unbounded_channel();
    let service = NativeIngressService::new(
        DrivenHandler(senders_tx),
        NativeIngressConfig::default(),
        "lane-test",
        false,
        NativeEndpointDomain::BackendData,
    )
    .with_lane_streams(Some(admission.clone()));
    (service, senders)
}

#[tokio::test]
async fn membership_ingress_holds_stream_positions_through_eof_reset_drop_and_refuses_before_dispatch()
 {
    let recorder = Arc::new(Recorder::default());
    let admission = NativeTransportAdmission::frontend(Some(recorder.clone())).unwrap();
    let (senders_tx, mut senders) = mpsc::unbounded_channel();
    let service = NativeIngressService::new(
        DrivenHandler(senders_tx),
        NativeIngressConfig::default(),
        "membership",
        false,
        NativeEndpointDomain::FrontendMembership,
    )
    .with_lane_streams(Some(admission.clone()));
    let method = NativeRpcMethod::AnnounceBackend;
    let lane = NativeLane::Membership;
    for exit in ["eof", "reset", "drop"] {
        let response = bounded(service.clone().oneshot(request(method)))
            .await
            .unwrap();
        let frames = senders.recv().await.unwrap();
        assert_eq!(
            held(&admission, lane),
            1,
            "handler return cannot release the stream"
        );
        let mut body = response.into_body();
        match exit {
            "eof" => {
                drop(frames);
                assert!(next_frame(&mut body).await.is_none());
            }
            "reset" => {
                frames.send(Err(tonic::Status::cancelled("reset"))).unwrap();
                assert!(next_frame(&mut body).await.unwrap().is_err());
            }
            "drop" => drop(body),
            _ => unreachable!(),
        }
        assert_eq!(held(&admission, lane), 0, "{exit}");
    }
    let mut pending_service = service.clone();
    let pending = pending_service.call(request(method));
    assert_eq!(held(&admission, lane), 1);
    drop(pending);
    assert_eq!(held(&admission, lane), 0);
    let gate = admission.incoming_streams(lane);
    let positions: Vec<_> = (0..gate.limit())
        .map(|_| gate.try_acquire().unwrap())
        .collect();
    assert_eq!(positions.len(), 12_288);
    let response = bounded(service.oneshot(request(method))).await.unwrap();
    assert_eq!(response.headers()["grpc-status"], "8");
    assert_eq!(
        response.headers()["x-novarocks-ingress-rejection"],
        "lane_streams"
    );
    assert!(
        senders.try_recv().is_err(),
        "capacity refusal never reaches the handler"
    );
    assert_eq!(
        admission.available_positions(TransportClass::Data),
        admission.positions(TransportClass::Data)
    );
    drop(positions);
    assert_eq!(held(&admission, lane), 0);
    let events = recorder.events();
    assert!(events.contains(&"streams:membership:incoming:1".to_string()));
    assert!(events.contains(&"streams:membership:incoming:-1".to_string()));
}

fn request(method: NativeRpcMethod) -> Request<Body> {
    Request::builder()
        .uri(method.contract().path)
        .body(Body::empty())
        .unwrap()
}

async fn next_frame(body: &mut BoxBody) -> Option<Result<Frame<Bytes>, tonic::Status>> {
    bounded(std::future::poll_fn(|cx| {
        Pin::new(&mut *body).poll_frame(cx)
    }))
    .await
}

fn held(admission: &NativeTransportAdmission, lane: NativeLane) -> usize {
    let gate = admission.incoming_streams(lane);
    gate.limit() - gate.available()
}

#[tokio::test]
async fn served_stream_position_outlives_the_handler_and_returns_at_body_eof() {
    let recorder = Arc::new(Recorder::default());
    let admission = NativeTransportAdmission::with_parts(
        TransportRole::Backend,
        AdmissionDimensions::frozen().unwrap(),
        Some(recorder.clone()),
    )
    .unwrap();
    for (method, lane) in [
        (NativeRpcMethod::ApplyTaskOperations, NativeLane::Submission),
        (NativeRpcMethod::FetchRootResult, NativeLane::ResultData),
        (
            NativeRpcMethod::SubscribeTaskStatus,
            NativeLane::Observation,
        ),
        (NativeRpcMethod::ExchangeUnary, NativeLane::Exchange),
    ] {
        let (service, mut senders) = ingress(&admission);
        // The handler has returned and Tonic's response future resolved.
        let response = bounded(service.oneshot(request(method))).await.unwrap();
        let frames = senders.recv().await.unwrap();
        assert_eq!(
            held(&admission, lane),
            1,
            "{lane:?} returned at handler exit"
        );
        let mut body = response.into_body();
        frames.send(Ok(Bytes::from_static(b"payload"))).unwrap();
        assert!(next_frame(&mut body).await.unwrap().is_ok());
        assert_eq!(held(&admission, lane), 1, "{lane:?} returned before EOF");
        drop(frames);
        assert!(next_frame(&mut body).await.is_none());
        assert_eq!(held(&admission, lane), 0, "{lane:?} kept after EOF");
        drop(body);
        assert_eq!(held(&admission, lane), 0);
    }
    let events = recorder.events();
    for lane in ["submission", "result_data", "observation", "exchange"] {
        assert!(events.contains(&format!("streams:{lane}:incoming:1")));
        assert!(events.contains(&format!("streams:{lane}:incoming:-1")));
    }
}

#[tokio::test]
async fn served_stream_position_returns_on_reset_or_error_and_on_early_body_drop() {
    let admission = NativeTransportAdmission::new().unwrap();
    for method in [
        NativeRpcMethod::FetchRootResult,
        NativeRpcMethod::SubscribeTaskStatus,
    ] {
        let lane = NativeLane::of(method.contract().traffic).unwrap();
        // A reset surfaces to the body as an error frame.
        let (service, mut senders) = ingress(&admission);
        let response = bounded(service.oneshot(request(method))).await.unwrap();
        let frames = senders.recv().await.unwrap();
        let mut body = response.into_body();
        frames.send(Err(tonic::Status::cancelled("reset"))).unwrap();
        assert!(next_frame(&mut body).await.unwrap().is_err());
        assert_eq!(held(&admission, lane), 0, "{lane:?} kept after reset");
        drop((body, frames));

        // The connection or client went away before the body ended.
        let (service, mut senders) = ingress(&admission);
        let response = bounded(service.oneshot(request(method))).await.unwrap();
        let _frames = senders.recv().await.unwrap();
        assert_eq!(held(&admission, lane), 1);
        drop(response);
        assert_eq!(held(&admission, lane), 0, "{lane:?} kept after drop");

        // A call whose future is cancelled returns the position it took.
        let (mut service, _senders) = ingress(&admission);
        bounded(service.ready()).await.unwrap();
        let pending = service.call(request(method));
        assert_eq!(held(&admission, lane), 1);
        drop(pending);
        assert_eq!(held(&admission, lane), 0, "{lane:?} kept after cancel");
    }
}

#[tokio::test]
async fn exhausted_lane_refuses_before_execution_gates_without_borrowing_another_lane() {
    let admission = NativeTransportAdmission::new().unwrap();
    let gate = admission.incoming_streams(NativeLane::Submission);
    let taken: Vec<_> = (0..gate.limit())
        .map(|_| gate.try_acquire().unwrap())
        .collect();
    assert!(gate.try_acquire().is_none());
    let (service, mut senders) = ingress(&admission);
    let refused = bounded(
        service
            .clone()
            .oneshot(request(NativeRpcMethod::ApplyTaskOperations)),
    )
    .await
    .unwrap();
    assert_eq!(refused.headers()["grpc-status"], "8");
    assert_eq!(
        refused.headers()["x-novarocks-ingress-rejection"],
        "lane_streams"
    );
    assert!(
        senders.try_recv().is_err(),
        "a refused stream never reaches the handler"
    );
    // ResultData has its own positions.
    let response = bounded(
        service
            .clone()
            .oneshot(request(NativeRpcMethod::FetchRootResult)),
    )
    .await
    .unwrap();
    assert_eq!(held(&admission, NativeLane::ResultData), 1);
    drop(response);
    drop(taken);
    assert_eq!(held(&admission, NativeLane::Submission), 0);
}

// ---------------------------------------------------------------------------
// Client stream positions.
// ---------------------------------------------------------------------------

/// A plain HTTP/2 peer whose response bodies the test drives.
struct BodyServer {
    address: SocketAddr,
    bodies: mpsc::UnboundedReceiver<Frames>,
    task: tokio::task::JoinHandle<()>,
}

impl BodyServer {
    async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (bodies_tx, bodies) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            loop {
                let Ok((io, _)) = listener.accept().await else {
                    return;
                };
                let bodies_tx = bodies_tx.clone();
                let service =
                    hyper::service::service_fn(move |_request: Request<hyper::body::Incoming>| {
                        let (frames_tx, frames) = mpsc::unbounded_channel();
                        bodies_tx.send(frames_tx).unwrap();
                        async move { Ok::<_, Infallible>(Response::new(ChannelBody(frames))) }
                    });
                tokio::spawn(async move {
                    let _ = hyper::server::conn::http2::Builder::new(
                        hyper_util::rt::TokioExecutor::new(),
                    )
                    .serve_connection(hyper_util::rt::TokioIo::new(io), service)
                    .await;
                });
            }
        });
        Self {
            address,
            bodies,
            task,
        }
    }

    async fn channel(&self) -> Channel {
        bounded(
            tonic::transport::Endpoint::from_shared(format!("http://{}", self.address))
                .unwrap()
                .connect(),
        )
        .await
        .unwrap()
    }
}

impl Drop for BodyServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn client_request() -> Request<BoxBody> {
    Request::builder()
        .method("POST")
        .uri("/novarocks.NovaRocksGrpc/FetchRootResult")
        .body(tonic::body::empty_body())
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_lane_stream_position_is_held_until_the_response_body_ends_or_drops() {
    let mut server = BodyServer::start().await;
    let recorder: Arc<dyn NativeTransportObserver> = Arc::new(Recorder::default());
    let mut lane = NativeLaneChannel::with_streams(
        server.channel().await,
        NativeLane::ResultData,
        1,
        Some(recorder),
    );

    // EOF: the position is held after the response headers resolve and
    // returns when the body ends.
    let response = bounded(lane.ready().await.unwrap().call(client_request()))
        .await
        .unwrap();
    let frames = bounded(server.bodies.recv()).await.unwrap();
    assert_eq!(lane.available_streams(), 0, "returned at response headers");
    // The only position is held: the next call waits before reaching Tonic.
    let mut waiting = lane.clone();
    let mut ready = Box::pin(async move {
        waiting.ready().await.unwrap();
        waiting
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut ready)
            .await
            .is_err()
    );
    let mut body = response.into_body();
    frames.send(Ok(Bytes::from_static(b"row"))).unwrap();
    assert!(next_frame(&mut body).await.unwrap().is_ok());
    assert_eq!(lane.available_streams(), 0, "returned before EOF");
    drop(frames);
    // Hyper may surface END_STREAM as one empty DATA frame before the end.
    loop {
        match next_frame(&mut body).await {
            None => break,
            Some(Ok(frame)) => {
                assert!(frame.data_ref().is_some_and(Bytes::is_empty), "{frame:?}");
                assert_eq!(lane.available_streams(), 0, "returned before EOF");
            }
            Some(Err(error)) => panic!("unexpected body error: {error}"),
        }
    }
    let mut waiting = bounded(ready).await;
    // The waiter now owns the only position.
    assert_eq!(lane.available_streams(), 0);

    // Drop: the caller abandons the body early.
    let response = bounded(waiting.call(client_request())).await.unwrap();
    let _frames = bounded(server.bodies.recv()).await.unwrap();
    assert_eq!(lane.available_streams(), 0);
    drop(response);
    assert_eq!(lane.available_streams(), 1, "kept after body drop");
    drop(body);
}

// ---------------------------------------------------------------------------
// Real listener: bootstrap, physical positions and client reset.
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct LaneProbe;

type TaskStatusEvents = Pin<
    Box<
        dyn tokio_stream::Stream<Item = Result<proto::TaskStatusStreamEvent, tonic::Status>> + Send,
    >,
>;

#[tonic::async_trait]
impl NovaRocksGrpc for LaneProbe {
    type ExchangeStream = tokio_stream::Empty<Result<proto::ExchangeResponse, tonic::Status>>;
    type SubscribeTaskStatusStream = TaskStatusEvents;
    async fn exchange(
        &self,
        _request: tonic::Request<tonic::Streaming<proto::ExchangeRequest>>,
    ) -> Result<tonic::Response<Self::ExchangeStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("probe"))
    }
    async fn exchange_unary(
        &self,
        _request: tonic::Request<proto::ExchangeRequest>,
    ) -> Result<tonic::Response<proto::ExchangeResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("probe"))
    }
    async fn transmit_runtime_filter_envelope(
        &self,
        _request: tonic::Request<filter::RuntimeFilterEnvelope>,
    ) -> Result<tonic::Response<filter::RuntimeFilterEnvelopeResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("probe"))
    }
    async fn fetch_result(
        &self,
        _request: tonic::Request<proto::FetchResultRequest>,
    ) -> Result<tonic::Response<proto::FetchResultResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("probe"))
    }
    async fn prune_catalogs(
        &self,
        _request: tonic::Request<catalog::PruneCatalogsRequest>,
    ) -> Result<tonic::Response<catalog::PruneCatalogsResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("probe"))
    }
    async fn announce_backend(
        &self,
        _request: tonic::Request<proto::AnnounceBackendRequest>,
    ) -> Result<tonic::Response<proto::AnnounceBackendResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("probe"))
    }
    async fn heartbeat(
        &self,
        _request: tonic::Request<proto::HeartbeatRequest>,
    ) -> Result<tonic::Response<proto::HeartbeatResponse>, tonic::Status> {
        Ok(tonic::Response::new(proto::HeartbeatResponse::default()))
    }
    async fn apply_task_operations(
        &self,
        _request: tonic::Request<proto::ApplyTaskOperationsRequest>,
    ) -> Result<tonic::Response<proto::ApplyTaskOperationsResponse>, tonic::Status> {
        Ok(tonic::Response::new(
            proto::ApplyTaskOperationsResponse::default(),
        ))
    }
    async fn apply_task_control_operations(
        &self,
        _request: tonic::Request<proto::ApplyTaskControlOperationsRequest>,
    ) -> Result<tonic::Response<proto::ApplyTaskOperationsResponse>, tonic::Status> {
        Ok(tonic::Response::new(
            proto::ApplyTaskOperationsResponse::default(),
        ))
    }
    async fn subscribe_task_status(
        &self,
        _request: tonic::Request<proto::SubscribeTaskStatusRequest>,
    ) -> Result<tonic::Response<Self::SubscribeTaskStatusStream>, tonic::Status> {
        // One event, then a subscription that stays open.
        let events = tokio_stream::StreamExt::chain(
            tokio_stream::once(Ok(proto::TaskStatusStreamEvent::default())),
            tokio_stream::pending(),
        );
        Ok(tonic::Response::new(Box::pin(events)))
    }
    async fn fetch_task_dynamic_filters(
        &self,
        _request: tonic::Request<proto::FetchTaskDynamicFiltersRequest>,
    ) -> Result<tonic::Response<proto::FetchTaskDynamicFiltersResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("probe"))
    }
    async fn get_final_task_info(
        &self,
        _request: tonic::Request<proto::GetFinalTaskInfoRequest>,
    ) -> Result<tonic::Response<proto::GetFinalTaskInfoResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("probe"))
    }
    async fn fetch_task_result(
        &self,
        _request: tonic::Request<proto::FetchTaskResultRequest>,
    ) -> Result<tonic::Response<proto::FetchResultResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "FetchTaskResult is retired; use FetchRootResult",
        ))
    }
    async fn fetch_root_result(
        &self,
        _request: tonic::Request<proto::FetchRootResultRequest>,
    ) -> Result<tonic::Response<proto::FetchRootResultResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("probe"))
    }
}

fn frontend_trust() -> Arc<NativeTrust> {
    let trust = crate::backend_test_support::test_backend_native_trust();
    trust
        .bind_process_identity(NativeProcessIdentity::Frontend(
            novarocks_types::FrontendProcessId::new_v7(),
        ))
        .unwrap();
    trust
}

fn data_listener(
    admission: &NativeTransportAdmission,
    trust: &Arc<NativeTrust>,
) -> NativeRpcServerHandle {
    NativeRpcServerHandle::start_with_admission(
        "127.0.0.1",
        0,
        LaneProbe,
        trust.clone(),
        NativeIncomingAdapter::plaintext(),
        "lane-test",
        NativeEndpointDomain::BackendData,
        "native-lane-test",
        || {},
        || {},
        NativeIngressConfig {
            worker_threads: 1,
            control_worker_threads: 1,
            ..NativeIngressConfig::default()
        },
        admission.clone(),
        TransportClass::Data,
        None,
    )
    .unwrap()
}

fn membership_listener(
    admission: &NativeTransportAdmission,
    trust: &Arc<NativeTrust>,
) -> Result<NativeRpcServerHandle, String> {
    NativeRpcServerHandle::start_frontend_membership(
        "127.0.0.1",
        0,
        LaneProbe,
        trust.clone(),
        NativeIncomingAdapter::plaintext(),
        "membership-test",
        "native-membership-test",
        || {},
        || {},
        NativeIngressConfig {
            worker_threads: 1,
            control_worker_threads: 1,
            ..NativeIngressConfig::default()
        },
        admission.clone(),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn membership_listener_saturates_bootstrap_without_consuming_outgoing_lanes() {
    let trust = crate::backend_test_support::test_backend_native_trust();
    trust
        .bind_process_identity(NativeProcessIdentity::Backend(
            novarocks_types::BackendProcessId::new_v7(),
        ))
        .unwrap();
    assert!(
        membership_listener(&NativeTransportAdmission::new().unwrap(), &trust).is_err(),
        "a Backend admission cannot protect a Frontend listener"
    );
    let admission = NativeTransportAdmission::frontend(None).unwrap();
    let mut listener = membership_listener(&admission, &trust).unwrap();
    let class = TransportClass::Membership;
    let mut live = H2Client::connect(listener.bound_addr()).await;
    let response = bounded(live.send(&trust, NativeRpcMethod::AnnounceBackend).await)
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let header_status = response.headers().get("grpc-status").cloned();
    let mut body = response.into_body();
    while let Some(frame) = bounded(body.data()).await {
        let frame = frame.unwrap();
        body.flow_control().release_capacity(frame.len()).unwrap();
    }
    let trailers = bounded(body.trailers()).await.unwrap();
    drop(body);
    let status = header_status
        .or_else(|| trailers.and_then(|trailers| trailers.get("grpc-status").cloned()));
    assert_eq!(
        status.unwrap(),
        "12",
        "the authenticated announcement reaches the probe handler"
    );
    eventually(
        || admission.available_handshakes(class) == 32,
        "authenticated membership returns its bootstrap position",
    )
    .await;
    assert_eq!(admission.available_positions(class), 95);
    let mut pending = tokio::task::JoinSet::new();
    let opened = tokio::time::Instant::now();
    for _ in 0..32 {
        let address = listener.bound_addr();
        pending.spawn(async move { tokio::net::TcpStream::connect(address).await.unwrap() });
    }
    let mut silent = Vec::new();
    while let Some(socket) = bounded(pending.join_next()).await {
        silent.push(socket.unwrap());
    }
    assert!(
        opened.elapsed()
            < Duration::from_millis(
                NativeResultSupportGeometry::V1.transport_handshake_deadline_ms
            ),
        "the fixture must establish concurrent sockets before their bootstrap expires"
    );
    eventually(
        || admission.available_handshakes(class) == 0,
        "all membership bootstrap positions are held by real sockets",
    )
    .await;
    let mut refused = bounded(tokio::net::TcpStream::connect(listener.bound_addr()))
        .await
        .unwrap();
    let mut byte = [0; 1];
    use tokio::io::AsyncReadExt;
    match bounded(refused.read(&mut byte)).await {
        Ok(count) => assert_eq!(count, 0),
        Err(error) => assert_eq!(error.kind(), std::io::ErrorKind::ConnectionReset),
    }
    assert_eq!(
        admission.available_positions(TransportClass::Data),
        admission.positions(TransportClass::Data)
    );
    assert_eq!(
        admission.available_positions(TransportClass::Control),
        admission.positions(TransportClass::Control)
    );
    assert_eq!(admission.refused_connections(class), 1);
    eventually(
        || {
            admission.available_handshakes(class) == 32
                && admission.available_positions(class) == 95
        },
        "bootstrap deadlines close silent IO and return positions",
    )
    .await;
    drop(silent);
    let response = bounded(live.send(&trust, NativeRpcMethod::AnnounceBackend).await)
        .await
        .unwrap();
    drain(response.into_body()).await;
    assert_eq!(
        admission.available_positions(class),
        95,
        "authenticated connection survives the bootstrap deadline"
    );
    drop(live.sender);
    let _ = bounded(live.connection).await;
    eventually(
        || admission.available_positions(class) == 96,
        "authenticated IO exit returns the physical position",
    )
    .await;
    bounded(tokio::task::spawn_blocking(move || {
        listener.stop().unwrap()
    }))
    .await
    .unwrap();
}

struct H2Client {
    sender: h2::client::SendRequest<Bytes>,
    connection: tokio::task::JoinHandle<Result<(), h2::Error>>,
    address: SocketAddr,
}

impl H2Client {
    async fn connect(address: SocketAddr) -> Self {
        let stream = bounded(tokio::net::TcpStream::connect(address))
            .await
            .unwrap();
        let (sender, connection) = bounded(h2::client::handshake(stream)).await.unwrap();
        Self {
            sender,
            connection: tokio::spawn(connection),
            address,
        }
    }

    /// Send one authenticated request with an empty protobuf message.
    async fn send(
        &mut self,
        trust: &NativeTrust,
        method: NativeRpcMethod,
    ) -> h2::client::ResponseFuture {
        bounded(std::future::poll_fn(|cx| self.sender.poll_ready(cx)))
            .await
            .unwrap();
        let mut metadata = tonic::metadata::MetadataMap::new();
        trust.apply_client_authorization(&mut metadata).unwrap();
        let mut request = Request::builder()
            .method("POST")
            .uri(format!("http://{}{}", self.address, method.contract().path))
            .body(())
            .unwrap();
        *request.headers_mut() = metadata.into_headers();
        request.headers_mut().insert(
            "content-type",
            hyper::http::HeaderValue::from_static("application/grpc"),
        );
        request
            .headers_mut()
            .insert("te", hyper::http::HeaderValue::from_static("trailers"));
        let (response, mut body) = self.sender.send_request(request, false).unwrap();
        body.send_data(Bytes::from_static(b"\0\0\0\0\0"), true)
            .unwrap();
        response
    }
}

async fn drain(mut body: h2::RecvStream) {
    while let Some(frame) = bounded(body.data()).await {
        let frame = frame.unwrap();
        body.flow_control().release_capacity(frame.len()).unwrap();
    }
    let _ = bounded(body.trailers()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn first_authenticated_head_ends_bootstrap_and_a_silent_connection_closes_at_its_deadline() {
    let admission = NativeTransportAdmission::new().unwrap();
    let trust = frontend_trust();
    let mut listener = data_listener(&admission, &trust);
    let class = TransportClass::Data;
    let (positions, handshakes) = (
        admission.positions(class),
        admission.handshake_positions(class),
    );

    // A connection that completes H2 but never sends a request keeps its
    // handshake position until the bootstrap deadline closes it.
    let silent = H2Client::connect(listener.bound_addr()).await;
    eventually(
        || admission.available_handshakes(class) == handshakes - 1,
        "the silent connection holds a handshake position",
    )
    .await;
    let opened = tokio::time::Instant::now();

    // The first authenticated request ends another connection's bootstrap:
    // its handshake position returns while its physical position stays.
    let mut live = H2Client::connect(listener.bound_addr()).await;
    let response = live
        .send(&trust, NativeRpcMethod::ApplyTaskOperations)
        .await;
    let response = bounded(response).await.unwrap();
    assert_eq!(response.status(), 200);
    drain(response.into_body()).await;
    assert_eq!(admission.available_handshakes(class), handshakes - 1);
    assert_eq!(admission.available_positions(class), positions - 2);

    // The server closes the silent connection at the deadline.
    let closed = bounded(silent.connection).await.unwrap();
    let elapsed = opened.elapsed();
    assert!(
        elapsed
            >= Duration::from_millis(
                NativeResultSupportGeometry::V1.transport_handshake_deadline_ms
            )
            .saturating_sub(Duration::from_millis(500)),
        "closed after {elapsed:?}: {closed:?}"
    );
    let returned = tokio::time::timeout(WATCHDOG, async {
        while admission.available_handshakes(class) != handshakes
            || admission.available_positions(class) != positions - 1
        {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await;
    assert!(
        returned.is_ok(),
        "silent connection positions did not return: handshakes {}/{handshakes}, positions {}/{positions}",
        admission.available_handshakes(class),
        admission.available_positions(class)
    );

    // The authenticated connection outlives the bootstrap deadline.
    let response = live
        .send(&trust, NativeRpcMethod::ApplyTaskOperations)
        .await;
    let response = bounded(response).await.unwrap();
    assert_eq!(response.status(), 200);
    drain(response.into_body()).await;
    assert_eq!(admission.available_positions(class), positions - 1);

    // Dropping the live connection returns its physical position.
    drop(live.sender);
    let _ = bounded(live.connection).await;
    eventually(
        || admission.available_positions(class) == positions,
        "the live connection's physical position returns",
    )
    .await;
    bounded(tokio::task::spawn_blocking(move || {
        listener.stop().unwrap()
    }))
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn served_subscription_holds_its_position_until_the_client_resets_the_stream() {
    let admission = NativeTransportAdmission::new().unwrap();
    let trust = frontend_trust();
    let mut listener = data_listener(&admission, &trust);
    let mut client = H2Client::connect(listener.bound_addr()).await;
    let response = client
        .send(&trust, NativeRpcMethod::SubscribeTaskStatus)
        .await;
    let response = bounded(response).await.unwrap();
    assert_eq!(response.status(), 200);
    let mut body = response.into_body();
    let first = bounded(body.data()).await.unwrap().unwrap();
    body.flow_control().release_capacity(first.len()).unwrap();
    // The subscription is established and its handler long returned.
    assert_eq!(held(&admission, NativeLane::Observation), 1);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(held(&admission, NativeLane::Observation), 1);
    // Dropping every handle of an open stream resets it (RST_STREAM CANCEL).
    drop(body);
    eventually(
        || held(&admission, NativeLane::Observation) == 0,
        "the reset subscription returns its position",
    )
    .await;

    // A unary request on its own connection returns at EOF.
    let mut unary = H2Client::connect(listener.bound_addr()).await;
    let response = unary
        .send(&trust, NativeRpcMethod::ApplyTaskOperations)
        .await;
    drain(bounded(response).await.unwrap().into_body()).await;
    eventually(
        || held(&admission, NativeLane::Submission) == 0,
        "the unary position returns at EOF",
    )
    .await;
    drop((client.sender, unary.sender));
    bounded(tokio::task::spawn_blocking(move || {
        listener.stop().unwrap()
    }))
    .await
    .unwrap();
}

// ---------------------------------------------------------------------------
// Dial admission.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn every_frontend_dial_takes_admission_and_refusal_opens_no_socket() {
    let recorder = Arc::new(Recorder::default());
    let admission = NativeTransportAdmission::with_parts(
        TransportRole::Frontend,
        AdmissionDimensions {
            data_positions: 1,
            control_positions: 1,
            data_handshakes: 1,
            control_handshakes: 1,
        },
        Some(recorder.clone()),
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint =
        novarocks_types::NativeEndpoint::from_socket_addr(listener.local_addr().unwrap());
    let connector = frontend_lane_connector(
        novarocks_native_trust::NativeEndpointConnector::plaintext(endpoint),
        admission.clone(),
        FrontendNativeLane::ResultData,
    );
    let uri: hyper::http::Uri = "http://lane.invalid".parse().unwrap();
    let first = bounded(connector.clone().oneshot(uri.clone()))
        .await
        .unwrap();
    let (_accepted, _) = bounded(listener.accept()).await.unwrap();
    assert_eq!(admission.available_positions(TransportClass::Data), 0);
    assert_eq!(admission.available_handshakes(TransportClass::Data), 1);
    // A second attempt, as Tonic's reconnect would make, is refused before
    // any IO: no socket reaches the listener.
    let Err(refused) = bounded(connector.clone().oneshot(uri.clone())).await else {
        panic!("a dial without a position must be refused");
    };
    assert_eq!(refused.kind(), std::io::ErrorKind::WouldBlock);
    assert!(
        listener
            .poll_accept(&mut Context::from_waker(std::task::Waker::noop()))
            .is_pending()
    );
    assert_eq!(admission.refused_connections(TransportClass::Data), 1);
    // Lifecycle control uses its own class.
    assert_eq!(admission.available_positions(TransportClass::Control), 1);
    drop(first);
    assert_eq!(admission.available_positions(TransportClass::Data), 1);
    assert_eq!(
        recorder.events(),
        vec![
            "connections:result_data:1".to_owned(),
            "refused:data".to_owned(),
            "connections:result_data:-1".to_owned(),
        ]
    );
}

#[test]
fn lane_manifest_mapping_and_classes_are_exact() {
    use novarocks_proto_codec::native_rpc::NATIVE_METHODS;
    for contract in NATIVE_METHODS {
        let lane = NativeLane::of(contract.traffic);
        assert_eq!(
            lane.is_none(),
            contract.traffic == NativeTrafficClass::Retired,
            "{:?}",
            contract.method
        );
        if let Some(lane) = lane {
            assert_eq!(
                lane.class() == TransportClass::Control,
                contract.endpoint == NativeEndpointDomain::BackendControl,
                "{:?}",
                contract.method
            );
        }
    }
    let indices: std::collections::BTreeSet<_> =
        NativeLane::ALL.iter().map(|lane| lane.index()).collect();
    assert_eq!(indices.len(), NativeLane::COUNT);
    assert_eq!(frontend_lane_connections(FrontendNativeLane::ResultData), 4);
    assert_eq!(
        frontend_lane_connections(FrontendNativeLane::Observation),
        4
    );
    assert_eq!(frontend_lane_connections(FrontendNativeLane::Submission), 2);
    assert_eq!(
        frontend_lane_connections(FrontendNativeLane::LifecycleControl),
        1
    );
    let g = NativeResultSupportGeometry::V1;
    assert_eq!(
        crate::native_transport_admission::incoming_lane_stream_limit(
            TransportRole::Backend,
            NativeLane::ResultData,
            &g
        )
        .unwrap(),
        2 * 4 * 128
    );
    assert_eq!(
        crate::native_transport_admission::incoming_lane_stream_limit(
            TransportRole::Frontend,
            NativeLane::ResultData,
            &g
        )
        .unwrap(),
        0
    );
}
