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

//! Incoming connection sealing on a real admitted listener over TCP/H2.
//!
//! The first authenticated request on an endpoint method seals the connection
//! to that caller process and traffic class and ends its bootstrap. A later
//! request with another process or lane, or an authenticated caller without a
//! signed process, closes the connection before the route runs. Unknown
//! methods and failed authentication fall through to the routes unsealed.

use super::*;
use bytes::Bytes;
use hyper::http::{HeaderValue, Request, header::AUTHORIZATION};
use novarocks_native_trust::{
    DeploymentId, ManualClock, NativeCallerSubject, NativeProcessIdentity, NativeTransportMode,
    ValidatedSharedSecret,
};
use novarocks_secret::SecretValue;
use novarocks_types::{BackendProcessId, FrontendProcessId};
use std::sync::atomic::AtomicUsize;

const WATCHDOG: Duration = Duration::from_secs(5);

async fn bounded<F: std::future::Future>(future: F) -> F::Output {
    tokio::time::timeout(WATCHDOG, future)
        .await
        .expect("incoming TCP/H2 fixture must progress")
}

fn peer(frontend: bool, suffix: u8) -> NativeProcessIdentity {
    let mut bytes = [0; 16];
    bytes[6] = 0x70;
    bytes[8] = 0x80;
    bytes[15] = suffix;
    if frontend {
        NativeProcessIdentity::Frontend(FrontendProcessId::try_from_bytes(bytes).unwrap())
    } else {
        NativeProcessIdentity::Backend(BackendProcessId::try_from_bytes(bytes).unwrap())
    }
}

fn trust(identity: Option<NativeProcessIdentity>) -> NativeTrust {
    let trust = NativeTrust::new_with_clock(
        DeploymentId::parse("incoming-seal-proof").unwrap(),
        ValidatedSharedSecret::new(SecretValue::new("0123456789abcdef0123456789abcdef")).unwrap(),
        NativeCallerSubject::parse("fe@diagnostic-same-subject:9080").unwrap(),
        NativeTransportMode::Disabled,
        Arc::new(ManualClock::new(1_700_000_000)),
    );
    if let Some(identity) = identity {
        trust.bind_process_identity(identity).unwrap();
    }
    trust
}

fn authorization(trust: &NativeTrust) -> HeaderValue {
    let mut metadata = tonic::metadata::MetadataMap::new();
    trust.apply_client_authorization(&mut metadata).unwrap();
    HeaderValue::from_bytes(metadata.get("authorization").unwrap().as_encoded_bytes()).unwrap()
}

#[derive(Clone, Default)]
struct Counted(Arc<AtomicUsize>);

impl Service<axum::http::Request<axum::body::Body>> for Counted {
    type Response = axum::http::Response<tonic::body::BoxBody>;
    type Error = std::convert::Infallible;
    type Future = std::future::Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _request: axum::http::Request<axum::body::Body>) -> Self::Future {
        self.0.fetch_add(1, Ordering::SeqCst);
        std::future::ready(Ok(tonic::Status::ok("").into_http()))
    }
}

struct Fixture {
    admission: NativeTransportAdmission,
    calls: Arc<AtomicUsize>,
    address: SocketAddr,
    shutdown: watch::Sender<bool>,
    serving: tokio::task::JoinHandle<Result<(), String>>,
}

impl Fixture {
    async fn start(verifier: &NativeTrust) -> Self {
        let admission = NativeTransportAdmission::new().unwrap();
        let listener = TokioTcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let (shutdown, shutdown_rx) = watch::channel(false);
        let serving = tokio::spawn(serve_native_listener(
            listener,
            Counted(Arc::clone(&calls)),
            NativeIncomingAdapter::plaintext(),
            shutdown_rx,
            Arc::new(|| {}) as Arc<dyn Fn() + Send + Sync>,
            "incoming-seal-test",
            Some(ListenerAdmission {
                admission: admission.clone(),
                class: TransportClass::Data,
                root_results: None,
            }),
            Some((
                verifier.server_admission(),
                NativeEndpointDomain::BackendData,
            )),
        ));
        Self {
            admission,
            calls,
            address,
            shutdown,
            serving,
        }
    }

    async fn connect(&self) -> Client {
        let stream = bounded(tokio::net::TcpStream::connect(self.address))
            .await
            .unwrap();
        let (sender, connection) = bounded(h2::client::handshake(stream)).await.unwrap();
        Client {
            sender,
            driver: tokio::spawn(connection),
        }
    }

    async fn finish(self) {
        self.shutdown.send(true).unwrap();
        assert_eq!(bounded(self.serving).await.unwrap(), Ok(()));
        bounded(async {
            while self.admission.available_positions(TransportClass::Data)
                != self.admission.positions(TransportClass::Data)
                || self.admission.available_handshakes(TransportClass::Data)
                    != self.admission.handshake_positions(TransportClass::Data)
            {
                tokio::task::yield_now().await;
            }
        })
        .await;
    }
}

struct Client {
    sender: h2::client::SendRequest<Bytes>,
    driver: tokio::task::JoinHandle<Result<(), h2::Error>>,
}

impl Client {
    /// Send one request and return its grpc-status, or None when the
    /// connection was closed instead of answering.
    async fn rpc(&mut self, caller: Option<&NativeTrust>, path: &str) -> Option<String> {
        if bounded(std::future::poll_fn(|cx| self.sender.poll_ready(cx)))
            .await
            .is_err()
        {
            return None;
        }
        let mut request = Request::builder()
            .method("POST")
            .uri(path)
            .header("content-type", "application/grpc");
        if let Some(caller) = caller {
            request = request.header(AUTHORIZATION, authorization(caller));
        }
        let (response, _) = self
            .sender
            .send_request(request.body(()).unwrap(), true)
            .ok()?;
        let response = bounded(response).await.ok()?;
        response
            .headers()
            .get("grpc-status")
            .map(|status| status.to_str().unwrap().to_owned())
    }

    async fn closed(self) {
        drop(self.sender);
        let _ = bounded(self.driver).await;
    }
}

fn path(method: NativeRpcMethod) -> &'static str {
    method.contract().path
}

#[tokio::test]
async fn same_process_and_lane_share_one_sealed_connection() {
    let verifier = trust(None);
    let fixture = Fixture::start(&verifier).await;
    let frontend = trust(Some(peer(true, 1)));
    let mut client = fixture.connect().await;
    for method in [
        NativeRpcMethod::ApplyTaskOperations,
        NativeRpcMethod::PruneCatalogs,
        NativeRpcMethod::ApplyTaskOperations,
    ] {
        assert_eq!(
            client.rpc(Some(&frontend), path(method)).await.as_deref(),
            Some("0")
        );
    }
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 3);
    // The authenticated request ended the bootstrap.
    assert_eq!(
        fixture.admission.available_handshakes(TransportClass::Data),
        fixture.admission.handshake_positions(TransportClass::Data)
    );
    client.closed().await;
    fixture.finish().await;
}

#[tokio::test]
async fn another_process_or_lane_closes_the_connection_before_the_route() {
    let verifier = trust(None);
    for (second_caller, second_method) in [
        (peer(true, 2), NativeRpcMethod::ApplyTaskOperations),
        (peer(true, 1), NativeRpcMethod::FetchRootResult),
    ] {
        let fixture = Fixture::start(&verifier).await;
        let first = trust(Some(peer(true, 1)));
        let second = trust(Some(second_caller));
        let mut client = fixture.connect().await;
        assert_eq!(
            client
                .rpc(Some(&first), path(NativeRpcMethod::ApplyTaskOperations))
                .await
                .as_deref(),
            Some("0")
        );
        let refused = client.rpc(Some(&second), path(second_method)).await;
        assert_ne!(refused.as_deref(), Some("0"));
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
        // The conflicting connection is closed rather than kept usable.
        assert_eq!(
            client
                .rpc(Some(&first), path(NativeRpcMethod::ApplyTaskOperations))
                .await,
            None
        );
        client.closed().await;
        fixture.finish().await;
    }
}

#[tokio::test]
async fn authenticated_caller_without_a_signed_process_is_refused_and_closed() {
    let verifier = trust(None);
    let fixture = Fixture::start(&verifier).await;
    let legacy = trust(None);
    let mut client = fixture.connect().await;
    let refused = client
        .rpc(Some(&legacy), path(NativeRpcMethod::ApplyTaskOperations))
        .await;
    assert_ne!(refused.as_deref(), Some("0"));
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        client
            .rpc(Some(&legacy), path(NativeRpcMethod::ApplyTaskOperations))
            .await,
        None
    );
    client.closed().await;
    fixture.finish().await;
}

#[tokio::test]
async fn unknown_method_wrong_endpoint_and_bad_auth_fall_through_without_sealing() {
    let verifier = trust(None);
    let fixture = Fixture::start(&verifier).await;
    let frontend = trust(Some(peer(true, 1)));
    let other = trust(Some(peer(true, 2)));
    let mut client = fixture.connect().await;
    // None of these seal the connection or end its bootstrap.
    for (caller, path) in [
        (Some(&frontend), "/novarocks.NovaRocksGrpc/NotAMethod"),
        (Some(&frontend), path(NativeRpcMethod::Heartbeat)),
        (None, path(NativeRpcMethod::ApplyTaskOperations)),
    ] {
        assert_eq!(client.rpc(caller, path).await.as_deref(), Some("0"));
    }
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 3);
    assert!(
        fixture.admission.available_handshakes(TransportClass::Data)
            < fixture.admission.handshake_positions(TransportClass::Data)
    );
    // A different process may still seal it first.
    assert_eq!(
        client
            .rpc(Some(&other), path(NativeRpcMethod::ApplyTaskOperations))
            .await
            .as_deref(),
        Some("0")
    );
    assert_eq!(
        fixture.admission.available_handshakes(TransportClass::Data),
        fixture.admission.handshake_positions(TransportClass::Data)
    );
    client.closed().await;
    fixture.finish().await;
}
