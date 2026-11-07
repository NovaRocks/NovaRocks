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

#![expect(
    clippy::disallowed_methods,
    reason = "Integration tests own and join their threads and runtime tasks; see ADR-0218."
)]

//! The public lane contract a Frontend consumer builds on: one lane channel
//! per connection, configured from the frozen geometry, whose stream position
//! follows each response body to its public exit.

use std::convert::Infallible;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use hyper::body::Frame;
use hyper::http::{Request, Response};
use novarocks_execution_contract::native_result_support::NativeResultSupportGeometry;
use novarocks_native_adapter::native_lane::{
    NativeLane, NativeLaneChannel, configure_native_endpoint,
};
use novarocks_native_adapter::native_transport_admission::NativeTransportAdmission;
use novarocks_native_adapter::native_transport_geometry::validate_native_transport_geometry;
use tokio::sync::mpsc;
use tonic::codegen::{Body as HttpBody, Service};
use tower::ServiceExt;

const WATCHDOG: Duration = Duration::from_secs(8);

type Frames = mpsc::UnboundedSender<Result<Bytes, Infallible>>;

struct DrivenBody(mpsc::UnboundedReceiver<Result<Bytes, Infallible>>);

impl HttpBody for DrivenBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        self.0
            .poll_recv(cx)
            .map(|frame| frame.map(|frame| frame.map(Frame::data)))
    }
}

async fn driven_server() -> (
    std::net::SocketAddr,
    mpsc::UnboundedReceiver<Frames>,
    tokio::task::JoinHandle<()>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (bodies_tx, bodies) = mpsc::unbounded_channel();
    let task = tokio::spawn(async move {
        while let Ok((io, _)) = listener.accept().await {
            let bodies_tx = bodies_tx.clone();
            let service =
                hyper::service::service_fn(move |_request: Request<hyper::body::Incoming>| {
                    let (frames_tx, frames) = mpsc::unbounded_channel();
                    bodies_tx.send(frames_tx).unwrap();
                    async move { Ok::<_, Infallible>(Response::new(DrivenBody(frames))) }
                });
            tokio::spawn(async move {
                let _ =
                    hyper::server::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new())
                        .serve_connection(hyper_util::rt::TokioIo::new(io), service)
                        .await;
            });
        }
    });
    (address, bodies, task)
}

fn request() -> Request<tonic::body::BoxBody> {
    Request::builder()
        .method("POST")
        .uri("/novarocks.NovaRocksGrpc/FetchRootResult")
        .body(tonic::body::empty_body())
        .unwrap()
}

async fn next_frame(
    body: &mut tonic::body::BoxBody,
) -> Option<Result<Frame<Bytes>, tonic::Status>> {
    tokio::time::timeout(
        WATCHDOG,
        std::future::poll_fn(|cx| Pin::new(&mut *body).poll_frame(cx)),
    )
    .await
    .expect("response body frame within the watchdog")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn result_lane_stream_position_follows_the_response_body_to_its_exit() {
    let (address, mut bodies, server) = driven_server().await;
    let admission = NativeTransportAdmission::frontend(None).unwrap();
    let channel = tokio::time::timeout(
        WATCHDOG,
        configure_native_endpoint(
            tonic::transport::Endpoint::from_shared(format!("http://{address}")).unwrap(),
        )
        .connect(),
    )
    .await
    .unwrap()
    .unwrap();
    let mut lane = NativeLaneChannel::new(channel, NativeLane::ResultData, Some(&admission));
    let limit =
        usize::try_from(NativeResultSupportGeometry::V1.transport_streams_per_connection).unwrap();
    assert_eq!(lane.stream_limit(), limit);

    // End of stream.
    let response = tokio::time::timeout(WATCHDOG, lane.ready().await.unwrap().call(request()))
        .await
        .unwrap()
        .unwrap();
    let frames = bodies.recv().await.unwrap();
    assert_eq!(lane.available_streams(), limit - 1);
    let mut body = response.into_body();
    frames.send(Ok(Bytes::from_static(b"segment"))).unwrap();
    assert_eq!(
        next_frame(&mut body)
            .await
            .unwrap()
            .unwrap()
            .into_data()
            .unwrap(),
        Bytes::from_static(b"segment")
    );
    assert_eq!(lane.available_streams(), limit - 1);
    drop(frames);
    while let Some(frame) = next_frame(&mut body).await {
        assert!(frame.unwrap().data_ref().is_some_and(Bytes::is_empty));
    }
    assert_eq!(lane.available_streams(), limit);

    // Early drop by the caller.
    let response = tokio::time::timeout(WATCHDOG, lane.ready().await.unwrap().call(request()))
        .await
        .unwrap()
        .unwrap();
    let _frames = bodies.recv().await.unwrap();
    assert_eq!(lane.available_streams(), limit - 1);
    drop(response);
    assert_eq!(lane.available_streams(), limit);

    // Clones share the connection's positions.
    let clone = lane.clone();
    let response = tokio::time::timeout(WATCHDOG, lane.ready().await.unwrap().call(request()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(clone.available_streams(), limit - 1);
    drop(response);
    server.abort();
}

#[test]
fn frozen_geometry_validates_and_reports_only_its_count_envelope() {
    let report = validate_native_transport_geometry(&NativeResultSupportGeometry::V1).unwrap();
    assert!(report.frontend.structural_bytes > 0);
    assert!(report.backend.structural_bytes > 0);
    assert_eq!(report.frontend.total_bytes().unwrap(), None);
    assert_eq!(report.backend.total_bytes().unwrap(), None);
}
