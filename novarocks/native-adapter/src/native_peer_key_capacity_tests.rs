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

//! Per-key dial admission through the production connector. A key's
//! connecting position is claimed before the transport connector runs, so a
//! refused attempt opens no socket, and the position returns when the
//! attempt's connection is dropped.

use super::{NativeChannelKey, admitted_connector, native_endpoint};
use crate::backend_test_support::test_backend_data_runtime;
use crate::native_transport_admission::{NativeTransportAdmission, TransportClass};
use bytes::Bytes;
use hyper::body::Body;
use hyper::http::{Request, Response};
use novarocks_proto_codec::native_rpc::NativeRpcMethod;
use novarocks_types::{BackendProcessId, NativeEndpoint};
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Context;
use std::time::Duration;
use tokio::net::TcpListener;
use tower::ServiceExt;

const WATCHDOG: Duration = Duration::from_secs(5);

fn admitted() -> (crate::BackendDataRuntime, NativeTransportAdmission) {
    let admission = NativeTransportAdmission::new().unwrap();
    let runtime = test_backend_data_runtime()
        .with_transport_admission(admission.clone())
        .unwrap();
    (runtime, admission)
}

fn request(method: NativeRpcMethod) -> Request<tonic::body::BoxBody> {
    Request::builder()
        .uri(method.contract().path)
        .body(tonic::body::empty_body())
        .unwrap()
}

fn io_kind(mut cause: &(dyn std::error::Error + 'static)) -> io::ErrorKind {
    loop {
        if let Some(error) = cause.downcast_ref::<io::Error>() {
            return error.kind();
        }
        cause = cause
            .source()
            .expect("actual admission error must preserve its IO cause");
    }
}

async fn positions_returned(admission: &NativeTransportAdmission) {
    tokio::time::timeout(WATCHDOG, async {
        while admission.available_positions(TransportClass::Data)
            != admission.positions(TransportClass::Data)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("every admitted connection position must return");
}

#[tokio::test]
async fn exact_key_connecting_position_refuses_before_the_connector_runs() {
    let (runtime, admission) = admitted();
    let process = BackendProcessId::new_v7();
    // Nothing listens here: reaching the transport connector would fail with
    // a connection error rather than the admission refusal asserted below.
    let endpoint = NativeEndpoint::from_host_port("127.0.0.1", 1).unwrap();
    for (peer, method) in [
        (Some(process), NativeRpcMethod::DeliverExchangeUnit),
        (
            Some(process),
            NativeRpcMethod::TransmitRuntimeFilterEnvelope,
        ),
        (None, NativeRpcMethod::AnnounceBackend),
    ] {
        let key = NativeChannelKey::new(peer, endpoint.clone(), method).unwrap();
        let identity = key.inline_identity().unwrap();
        let blocker = admission
            .try_dial(TransportClass::Data, Some(identity))
            .unwrap();
        let connector =
            admitted_connector(&runtime, &endpoint, TransportClass::Data, Some(identity)).unwrap();
        let error = native_endpoint(&runtime, &endpoint)
            .unwrap()
            .connect_with_connector(connector)
            .await
            .unwrap_err();
        assert_eq!(io_kind(&error), io::ErrorKind::WouldBlock);
        drop(blocker);
    }
    positions_returned(&admission).await;
}

#[tokio::test]
async fn production_cache_miss_uses_same_key_gate_before_tcp_and_recovers_failed_leader() {
    let (runtime, admission) = admitted();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = NativeEndpoint::from_socket_addr(listener.local_addr().unwrap());
    let method = NativeRpcMethod::DeliverExchangeUnit;
    let key = NativeChannelKey::new(Some(BackendProcessId::new_v7()), endpoint, method).unwrap();
    let blocker = admission
        .try_dial(TransportClass::Data, Some(key.inline_identity().unwrap()))
        .unwrap();
    let refused = tokio::time::timeout(
        WATCHDOG,
        super::get_or_create_channel(&runtime, key.clone()),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(
        refused.starts_with("connect exchange endpoint failed:"),
        "must pass cache election and fail in its keyed dial admission: {refused}"
    );
    assert!(
        listener
            .poll_accept(&mut Context::from_waker(std::task::Waker::noop()))
            .is_pending(),
        "a refused production cache miss must not reach the real TCP listener"
    );
    drop(blocker);
    let accepted = Arc::new(AtomicUsize::new(0));
    let counted = accepted.clone();
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        counted.fetch_add(1, Ordering::SeqCst);
        let mut connection = h2::server::handshake(socket).await.unwrap();
        let (request, mut response) = connection.accept().await.unwrap().unwrap();
        drop(request);
        let mut stream = response.send_response(Response::new(()), false).unwrap();
        stream
            .send_data(Bytes::from_static(b"production keyed DATA"), true)
            .unwrap();
        drop(stream);
        // Wait for the client to close after cache removal and the last
        // Channel drop. No abort or sleep stands in for that exit.
        assert!(connection.accept().await.is_none());
    });
    let channel = tokio::time::timeout(
        WATCHDOG,
        super::get_or_create_channel(&runtime, key.clone()),
    )
    .await
    .unwrap()
    .unwrap();
    let mut response = tokio::time::timeout(WATCHDOG, channel.clone().oneshot(request(method)))
        .await
        .unwrap()
        .unwrap();
    let frame = tokio::time::timeout(
        WATCHDOG,
        std::future::poll_fn(|cx| Pin::new(response.body_mut()).poll_frame(cx)),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    assert_eq!(
        frame.into_data().unwrap().as_ref(),
        b"production keyed DATA"
    );
    assert_eq!(accepted.load(Ordering::SeqCst), 1);
    assert!(
        admission.available_positions(TransportClass::Data)
            < admission.positions(TransportClass::Data)
    );
    drop(response);
    drop(runtime.channels().remove(key.inline_identity().unwrap()));
    drop(channel);
    tokio::time::timeout(WATCHDOG, peer)
        .await
        .expect("actual peer must exit naturally after the client channel closes")
        .unwrap();
    drop(runtime);
    positions_returned(&admission).await;
}
