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

//! Actual independent authenticated Data/Control listener actors sharing one
//! process admission. External client buffers/tasks are fixture-owned; these
//! tests do not establish BackendHost deployment.

use super::*;
use bytes::Bytes;
use hyper::http::Request;
use novarocks_native_trust::NativeTrust;
use novarocks_proto_codec::native_rpc::NativeRpcMethod;
use novarocks_proto_models::{catalog, filter, novarocks as proto};
use std::future::Future;
use std::io;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use tokio::io::AsyncReadExt;
use tonic::metadata::{Ascii, MetadataMap, MetadataValue};

const WATCHDOG: Duration = Duration::from_secs(8);
const ORIGINAL_FIELD: &str = "actual-authenticated-listener-original-field";

async fn bounded<F: Future>(future: F) -> F::Output {
    tokio::time::timeout(WATCHDOG, future)
        .await
        .expect("independent listener fixture exceeded its failure watchdog")
}

#[derive(Default)]
struct Observation {
    heartbeat: AtomicUsize,
    control: AtomicUsize,
    data: AtomicUsize,
    aliases: Mutex<Vec<MetadataValue<Ascii>>>,
}
#[derive(Clone)]
struct ProbeService(Arc<Observation>);
impl ProbeService {
    fn capture<T>(&self, request: &tonic::Request<T>) {
        assert!(
            request
                .extensions()
                .get::<Arc<crate::native_ingress::NativeIngressOwnership>>()
                .is_some()
        );
        let original = request.metadata().get("x-native-original").unwrap();
        assert_eq!(original.as_encoded_bytes(), ORIGINAL_FIELD.as_bytes());
        let alias = original.clone();
        assert_eq!(
            alias.as_encoded_bytes().as_ptr(),
            original.as_encoded_bytes().as_ptr()
        );
        self.0.aliases.lock().unwrap().push(alias);
    }
}

#[tonic::async_trait]
impl NovaRocksGrpc for ProbeService {
    type ExchangeStream = tokio_stream::Empty<Result<proto::ExchangeResponse, tonic::Status>>;

    async fn exchange(
        &self,
        _request: tonic::Request<tonic::Streaming<proto::ExchangeRequest>>,
    ) -> Result<tonic::Response<Self::ExchangeStream>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "Exchange is retired; use DeliverExchangeUnit",
        ))
    }

    type SubscribeExchangeGrantsStream = std::pin::Pin<
        Box<
            dyn tokio_stream::Stream<Item = Result<proto::ExchangeGrantBatch, tonic::Status>>
                + Send
                + 'static,
        >,
    >;

    async fn deliver_exchange_unit(
        &self,
        _request: tonic::Request<proto::DeliverExchangeUnitRequest>,
    ) -> Result<tonic::Response<proto::DeliverExchangeUnitResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "Exchange unit delivery is not installed",
        ))
    }

    async fn register_exchange_demand(
        &self,
        _request: tonic::Request<proto::RegisterExchangeDemandRequest>,
    ) -> Result<tonic::Response<proto::RegisterExchangeDemandResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "Exchange demand registration is not installed",
        ))
    }

    async fn subscribe_exchange_grants(
        &self,
        _request: tonic::Request<proto::SubscribeExchangeGrantsRequest>,
    ) -> Result<tonic::Response<Self::SubscribeExchangeGrantsStream>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "Exchange grant subscription is not installed",
        ))
    }

    type SubscribeTaskStatusStream =
        tokio_stream::Empty<Result<proto::TaskStatusStreamEvent, tonic::Status>>;

    async fn transmit_runtime_filter_envelope(
        &self,
        _request: tonic::Request<filter::RuntimeFilterEnvelope>,
    ) -> Result<tonic::Response<filter::RuntimeFilterEnvelopeResponse>, tonic::Status> {
        panic!("unexpected generated handler: transmit_runtime_filter_envelope")
    }
    async fn fetch_result(
        &self,
        _request: tonic::Request<proto::FetchResultRequest>,
    ) -> Result<tonic::Response<proto::FetchResultResponse>, tonic::Status> {
        panic!("unexpected generated handler: fetch_result")
    }
    async fn prune_catalogs(
        &self,
        _request: tonic::Request<catalog::PruneCatalogsRequest>,
    ) -> Result<tonic::Response<catalog::PruneCatalogsResponse>, tonic::Status> {
        panic!("unexpected generated handler: prune_catalogs")
    }
    async fn announce_backend(
        &self,
        _request: tonic::Request<proto::AnnounceBackendRequest>,
    ) -> Result<tonic::Response<proto::AnnounceBackendResponse>, tonic::Status> {
        panic!("unexpected generated handler: announce_backend")
    }
    async fn heartbeat(
        &self,
        request: tonic::Request<proto::HeartbeatRequest>,
    ) -> Result<tonic::Response<proto::HeartbeatResponse>, tonic::Status> {
        self.capture(&request);
        self.0.heartbeat.fetch_add(1, Ordering::SeqCst);
        Ok(tonic::Response::new(proto::HeartbeatResponse::default()))
    }
    async fn apply_task_operations(
        &self,
        request: tonic::Request<proto::ApplyTaskOperationsRequest>,
    ) -> Result<tonic::Response<proto::ApplyTaskOperationsResponse>, tonic::Status> {
        self.capture(&request);
        self.0.data.fetch_add(1, Ordering::SeqCst);
        Ok(tonic::Response::new(
            proto::ApplyTaskOperationsResponse::default(),
        ))
    }
    async fn apply_task_control_operations(
        &self,
        request: tonic::Request<proto::ApplyTaskControlOperationsRequest>,
    ) -> Result<tonic::Response<proto::ApplyTaskOperationsResponse>, tonic::Status> {
        self.capture(&request);
        self.0.control.fetch_add(1, Ordering::SeqCst);
        Ok(tonic::Response::new(
            proto::ApplyTaskOperationsResponse::default(),
        ))
    }
    async fn subscribe_task_status(
        &self,
        _request: tonic::Request<proto::SubscribeTaskStatusRequest>,
    ) -> Result<tonic::Response<Self::SubscribeTaskStatusStream>, tonic::Status> {
        panic!("unexpected generated handler: subscribe_task_status")
    }
    async fn fetch_task_dynamic_filters(
        &self,
        _request: tonic::Request<proto::FetchTaskDynamicFiltersRequest>,
    ) -> Result<tonic::Response<proto::FetchTaskDynamicFiltersResponse>, tonic::Status> {
        panic!("unexpected generated handler: fetch_task_dynamic_filters")
    }
    async fn get_final_task_info(
        &self,
        _request: tonic::Request<proto::GetFinalTaskInfoRequest>,
    ) -> Result<tonic::Response<proto::GetFinalTaskInfoResponse>, tonic::Status> {
        panic!("unexpected generated handler: get_final_task_info")
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
        panic!("unexpected generated handler: fetch_root_result")
    }
}

fn stock() -> (NativeTransportAdmission, (), ()) {
    (NativeTransportAdmission::new().unwrap(), (), ())
}
fn start(
    factory: &NativeTransportAdmission,
    trust: &Arc<NativeTrust>,
    observation: &Arc<Observation>,
    domain: NativeEndpointDomain,
) -> NativeRpcServerHandle {
    let class = match domain {
        NativeEndpointDomain::BackendData => TransportClass::Data,
        NativeEndpointDomain::BackendControl => TransportClass::Control,
        NativeEndpointDomain::FrontendMembership => unreachable!(),
    };
    NativeRpcServerHandle::start_with_admission(
        "127.0.0.1",
        0,
        ProbeService(observation.clone()),
        trust.clone(),
        NativeIncomingAdapter::plaintext(),
        "diagnostic-test-label",
        domain,
        "independent-native-listener-test",
        || {},
        || {},
        NativeIngressConfig {
            worker_threads: 1,
            control_worker_threads: 1,
            ..NativeIngressConfig::default()
        },
        factory.clone(),
        class,
        None,
    )
    .unwrap()
}

struct Client {
    sender: h2::client::SendRequest<Bytes>,
    task: tokio::task::JoinHandle<Result<(), h2::Error>>,
    address: SocketAddr,
}
impl Client {
    async fn connect(address: SocketAddr) -> Self {
        let stream = bounded(tokio::net::TcpStream::connect(address))
            .await
            .unwrap();
        let (sender, connection) = bounded(h2::client::handshake(stream)).await.unwrap();
        let task = tokio::spawn(connection);
        Self {
            sender,
            task,
            address,
        }
    }
    async fn rpc(
        &mut self,
        trust: Option<&NativeTrust>,
        method: NativeRpcMethod,
        payload: bool,
    ) -> (u16, String) {
        bounded(std::future::poll_fn(|cx| self.sender.poll_ready(cx)))
            .await
            .unwrap();
        let mut metadata = MetadataMap::new();
        if let Some(trust) = trust {
            trust.apply_client_authorization(&mut metadata).unwrap();
        }
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
        request.headers_mut().insert(
            "x-native-original",
            hyper::http::HeaderValue::from_static(ORIGINAL_FIELD),
        );
        let (response, mut body) = self.sender.send_request(request, false).unwrap();
        if payload {
            // Actual protobuf decoder receives one complete empty message.
            body.send_data(Bytes::from_static(b"\0\0\0\0\0"), true)
                .unwrap();
        }
        // With payload=false the peer never sends any body byte; domain/auth
        // refusal must complete while the request's decoder would be Pending.
        let response = bounded(response).await.unwrap();
        let http = response.status().as_u16();
        let header_status = response.headers().get("grpc-status").cloned();
        let mut response_body = response.into_body();
        let legal_unread_reset = |error: &h2::Error, response_bytes: usize| {
            !payload
                && response_bytes == 0
                && header_status.as_ref().is_some_and(|value| {
                    value
                        .to_str()
                        .ok()
                        .and_then(|text| text.parse::<u8>().ok())
                        .is_some_and(|code| code <= 16)
                })
                && error.is_remote()
                && error.is_reset()
                && error.reason() == Some(h2::Reason::NO_ERROR)
        };
        let mut response_bytes = 0_usize;
        let mut unread_reset = false;
        while let Some(bytes) = bounded(response_body.data()).await {
            match bytes {
                Ok(bytes) => {
                    response_bytes = response_bytes.checked_add(bytes.len()).unwrap();
                    response_body
                        .flow_control()
                        .release_capacity(bytes.len())
                        .unwrap();
                }
                Err(error) => {
                    // Hyper may close the deliberately unread request after
                    // publishing a complete trailers-only gRPC refusal. Only
                    // that remote NO_ERROR reset may finish this empty reply.
                    assert!(
                        legal_unread_reset(&error, response_bytes),
                        "unexpected response body error: {error}"
                    );
                    unread_reset = true;
                    break;
                }
            }
        }
        let trailers = if unread_reset {
            None
        } else {
            match bounded(response_body.trailers()).await {
                Ok(trailers) => trailers,
                Err(error) => {
                    assert!(
                        legal_unread_reset(&error, response_bytes),
                        "unexpected response trailers error: {error}"
                    );
                    None
                }
            }
        };
        let status = header_status
            .or_else(|| {
                trailers
                    .as_ref()
                    .and_then(|headers| headers.get("grpc-status").cloned())
            })
            .unwrap();
        drop((body, response_body));
        (http, status.to_str().unwrap().to_owned())
    }
    async fn join(self) {
        drop(self.sender);
        let _ = bounded(self.task).await.unwrap();
    }
}

async fn initial_settings(peer: &mut tokio::net::TcpStream) {
    let mut header = [0; 9];
    bounded(peer.read_exact(&mut header)).await.unwrap();
    assert_eq!(header[3], 4);
    assert_eq!(header[4], 0);
    let len =
        (usize::from(header[0]) << 16) | (usize::from(header[1]) << 8) | usize::from(header[2]);
    assert!(len <= 256);
    let mut payload = vec![0; len];
    bounded(peer.read_exact(&mut payload)).await.unwrap();
}
async fn half_open(address: SocketAddr) -> tokio::net::TcpStream {
    let mut peer = bounded(tokio::net::TcpStream::connect(address))
        .await
        .unwrap();
    initial_settings(&mut peer).await;
    peer
}
async fn acquisition_at(
    factory: &NativeTransportAdmission,
    class: TransportClass,
    expected: usize,
) {
    bounded(async {
        while factory.available_handshakes(class) != expected {
            tokio::task::yield_now().await;
        }
    })
    .await;
}
async fn positions_at(factory: &NativeTransportAdmission, class: TransportClass, expected: usize) {
    bounded(async {
        while factory.available_positions(class) != expected {
            tokio::task::yield_now().await;
        }
    })
    .await;
}
async fn peer_exited(mut peer: tokio::net::TcpStream) {
    let mut bytes = Vec::new();
    if let Err(error) = bounded(peer.read_to_end(&mut bytes)).await {
        assert_eq!(error.kind(), io::ErrorKind::ConnectionReset);
    }
}
async fn saturated(blocked: TransportClass, expected: usize) {
    let (factory, _, _) = stock();
    let trust = crate::backend_test_support::test_backend_native_trust();
    trust
        .bind_process_identity(novarocks_native_trust::NativeProcessIdentity::Frontend(
            novarocks_types::FrontendProcessId::new_v7(),
        ))
        .unwrap();
    let data_observation = Arc::new(Observation::default());
    let control_observation = Arc::new(Observation::default());
    let mut data = start(
        &factory,
        &trust,
        &data_observation,
        NativeEndpointDomain::BackendData,
    );
    let mut control = start(
        &factory,
        &trust,
        &control_observation,
        NativeEndpointDomain::BackendControl,
    );
    let mut data_client = Client::connect(data.bound_addr()).await;
    let mut control_client = Client::connect(control.bound_addr()).await;
    // Real outer authentication runs before the exact domain guard.
    assert_eq!(
        data_client
            .rpc(None, NativeRpcMethod::Heartbeat, false)
            .await,
        (200, "16".into())
    );
    assert_eq!(
        data_client
            .rpc(Some(&trust), NativeRpcMethod::Heartbeat, false)
            .await,
        (200, "12".into())
    );
    assert_eq!(
        control_client
            .rpc(Some(&trust), NativeRpcMethod::ApplyTaskOperations, false)
            .await,
        (200, "12".into())
    );
    assert_eq!(data_observation.data.load(Ordering::SeqCst), 0);
    assert_eq!(control_observation.heartbeat.load(Ordering::SeqCst), 0);
    assert_eq!(control_observation.control.load(Ordering::SeqCst), 0);
    // A connection's bootstrap ends with its first authenticated request on
    // its own endpoint; until then it holds a handshake position and is closed
    // at the bootstrap deadline. Each live client completes it now.
    assert_eq!(
        data_client
            .rpc(Some(&trust), NativeRpcMethod::ApplyTaskOperations, true)
            .await,
        (200, "0".into())
    );
    assert_eq!(
        control_client
            .rpc(Some(&trust), NativeRpcMethod::Heartbeat, true)
            .await,
        (200, "0".into())
    );
    acquisition_at(&factory, TransportClass::Data, 32).await;
    acquisition_at(&factory, TransportClass::Control, 8).await;

    let (address, opposite, live, observation) = match blocked {
        TransportClass::Data => (
            data.bound_addr(),
            TransportClass::Control,
            &mut control_client,
            &control_observation,
        ),
        TransportClass::Control => (
            control.bound_addr(),
            TransportClass::Data,
            &mut data_client,
            &data_observation,
        ),
        TransportClass::Membership => panic!("Backend has no membership listener"),
    };
    assert_eq!(factory.handshake_positions(blocked), expected);
    // Every position below is an actual accepted TCP socket withholding the
    // H2 preface, rather than an unpolled configuration standing in for IO.
    let mut pending = tokio::task::JoinSet::new();
    for _ in 0..expected {
        pending.spawn(half_open(address));
    }
    let mut peers = Vec::with_capacity(expected);
    while let Some(peer) = bounded(pending.join_next()).await {
        peers.push(peer.unwrap());
    }
    acquisition_at(&factory, blocked, 0).await;
    assert_eq!(
        factory.available_handshakes(opposite),
        factory.handshake_positions(opposite)
    );
    let mut refused = bounded(tokio::net::TcpStream::connect(address))
        .await
        .unwrap();
    let mut first = [0; 1];
    match bounded(refused.read(&mut first)).await {
        Ok(read) => assert_eq!(read, 0, "saturated acquisition refusal writes no SETTINGS"),
        Err(error) => assert_eq!(error.kind(), io::ErrorKind::ConnectionReset),
    }
    drop(refused);
    assert_eq!(factory.available_handshakes(blocked), 0);
    match opposite {
        TransportClass::Control => {
            assert_eq!(
                live.rpc(Some(&trust), NativeRpcMethod::Heartbeat, true)
                    .await,
                (200, "0".into())
            );
            assert_eq!(
                live.rpc(
                    Some(&trust),
                    NativeRpcMethod::ApplyTaskControlOperations,
                    true
                )
                .await,
                (200, "0".into())
            );
            assert_eq!(observation.heartbeat.load(Ordering::SeqCst), 2);
            assert_eq!(observation.control.load(Ordering::SeqCst), 1);
        }
        TransportClass::Data => {
            assert_eq!(
                live.rpc(Some(&trust), NativeRpcMethod::ApplyTaskOperations, true)
                    .await,
                (200, "0".into())
            );
            assert_eq!(observation.data.load(Ordering::SeqCst), 2);
        }
        TransportClass::Membership => panic!("Backend has no membership listener"),
    }
    // Both entrypoints receive their stop signal before either actor join.
    data.begin_stop();
    control.begin_stop();
    assert!(data.shutdown_tx.is_none() && control.shutdown_tx.is_none());
    bounded(tokio::task::spawn_blocking(move || {
        data.stop().unwrap();
        control.stop().unwrap();
    }))
    .await
    .unwrap();
    data_client.join().await;
    control_client.join().await;
    for peer in peers {
        peer_exited(peer).await;
    }
    acquisition_at(&factory, TransportClass::Data, 32).await;
    acquisition_at(&factory, TransportClass::Control, 8).await;
    positions_at(&factory, blocked, factory.positions(blocked)).await;
    positions_at(&factory, opposite, factory.positions(opposite)).await;
    assert_eq!(data_observation.heartbeat.load(Ordering::SeqCst), 0);
    assert_eq!(control_observation.data.load(Ordering::SeqCst), 0);
    data_observation.aliases.lock().unwrap().clear();
    control_observation.aliases.lock().unwrap().clear();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_data_32_half_open_saturation_preserves_authenticated_control_listener_progress() {
    saturated(TransportClass::Data, 32).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_control_8_half_open_saturation_refuses_without_borrowing_data_listener() {
    saturated(TransportClass::Control, 8).await;
}

#[test]
fn mismatched_domain_and_stock_class_refuse_before_address_resolution() {
    let (factory, _, _) = stock();
    let trust = crate::backend_test_support::test_backend_native_trust();
    for (domain, class) in [
        (NativeEndpointDomain::BackendData, TransportClass::Control),
        (NativeEndpointDomain::BackendControl, TransportClass::Data),
        (
            NativeEndpointDomain::FrontendMembership,
            TransportClass::Data,
        ),
    ] {
        let outcome = NativeRpcServerHandle::start_with_admission(
            "not-a-real-native-endpoint.invalid",
            0,
            ProbeService(Arc::new(Observation::default())),
            trust.clone(),
            NativeIncomingAdapter::plaintext(),
            "backend",
            domain,
            "unused-test-thread",
            || {},
            || {},
            NativeIngressConfig::default(),
            factory.clone(),
            class,
            None,
        );
        assert_eq!(
            outcome.err().unwrap(),
            "native endpoint domain and transport admission class disagree"
        );
    }
    assert_eq!(
        factory.available_positions(TransportClass::Data),
        factory.positions(TransportClass::Data)
    );
    assert_eq!(
        factory.available_positions(TransportClass::Control),
        factory.positions(TransportClass::Control)
    );
}
