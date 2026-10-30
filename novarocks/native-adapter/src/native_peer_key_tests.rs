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

//! Actual cache-key routing through TCP, Hyper H2 and a raw Tonic Channel.
//! Ordinary fixtures use a runtime without dial admission; admitted fixtures
//! install the process transport admission and observe its positions. They do
//! not prove authentication or per-lane admission limits.

use super::{NativeChannelKey, get_or_create_channel};
use crate::backend_test_support::test_backend_data_runtime;
use crate::native_transport_admission::{NativeTransportAdmission, TransportClass};
use hyper::http::{Request, Response};
use hyper::rt::Executor;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use novarocks_proto_codec::native_rpc::NativeRpcMethod;
use novarocks_types::{BackendProcessId, NativeEndpoint};
use std::convert::Infallible;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::Barrier;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

use crate::native_lane::NativeLaneChannel as Channel;

const WATCHDOG: Duration = Duration::from_secs(5);

#[derive(Clone, Default)]
struct JoinedExecutor(Arc<Mutex<Vec<JoinHandle<()>>>>);
impl<F> Executor<F> for JoinedExecutor
where
    F: Future + Send + 'static,
    F::Output: Send,
{
    fn execute(&self, future: F) {
        self.0.lock().unwrap().push(tokio::spawn(async move {
            let _ = future.await;
        }));
    }
}
impl JoinedExecutor {
    async fn stop_and_join(&self) {
        loop {
            let tasks = std::mem::take(&mut *self.0.lock().unwrap());
            if tasks.is_empty() {
                break;
            }
            for task in &tasks {
                task.abort();
            }
            for task in tasks {
                let _ = tokio::time::timeout(WATCHDOG, task).await.unwrap();
            }
        }
    }
}

struct Server {
    endpoint: NativeEndpoint,
    accepted: Arc<AtomicUsize>,
    stop: CancellationToken,
    accept_task: JoinHandle<()>,
    executor: JoinedExecutor,
}
impl Server {
    async fn start(first_id: usize) -> Self {
        Self::start_inner(first_id).await
    }

    async fn start_inner(first_id: usize) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = NativeEndpoint::from_socket_addr(listener.local_addr().unwrap());
        let accepted = Arc::new(AtomicUsize::new(0));
        let observed = accepted.clone();
        let stop = CancellationToken::new();
        let stopping = stop.clone();
        let executor = JoinedExecutor::default();
        let serving = executor.clone();
        let accept_task = tokio::spawn(async move {
            loop {
                let (io, _) = tokio::select! {
                    biased;
                    _ = stopping.cancelled() => break,
                    accepted = listener.accept() => accepted.unwrap(),
                };
                let id = first_id + observed.fetch_add(1, Ordering::SeqCst);
                let builder = hyper::server::conn::http2::Builder::new(serving.clone());
                let service = service_fn(move |_: Request<hyper::body::Incoming>| async move {
                    Ok::<_, Infallible>(
                        Response::builder()
                            .header("x-connection-id", id.to_string())
                            .body(axum::body::Body::empty())
                            .unwrap(),
                    )
                });
                serving.execute(async move {
                    let _ = builder.serve_connection(TokioIo::new(io), service).await;
                });
            }
        });
        Self {
            endpoint,
            accepted,
            stop,
            accept_task,
            executor,
        }
    }

    async fn shutdown(self) {
        self.stop.cancel();
        tokio::time::timeout(WATCHDOG, self.accept_task)
            .await
            .unwrap()
            .unwrap();
        // Includes the actual connection future and Hyper's executor children.
        self.executor.stop_and_join().await;
    }
}

async fn connection_id(channel: Channel, method: NativeRpcMethod) -> usize {
    let request = Request::builder()
        .uri(method.contract().path)
        .body(tonic::body::empty_body())
        .unwrap();
    let response = tokio::time::timeout(WATCHDOG, channel.oneshot(request))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), 200);
    response.headers()["x-connection-id"]
        .to_str()
        .unwrap()
        .parse()
        .unwrap()
}

#[tokio::test]
async fn unit_methods_reuse_one_exact_peer_connection_without_changing_the_rpc_path() {
    let runtime = test_backend_data_runtime();
    let server = Server::start(1).await;
    let peer = BackendProcessId::new_v7();
    let methods = [
        NativeRpcMethod::DeliverExchangeUnit,
        NativeRpcMethod::RegisterExchangeDemand,
        NativeRpcMethod::SubscribeExchangeGrants,
    ];
    let mut first_key = None;
    for method in methods {
        let key = NativeChannelKey::new(Some(peer), server.endpoint.clone(), method).unwrap();
        if let Some(first) = &first_key {
            assert_eq!(&key, first);
        } else {
            first_key = Some(key.clone());
        }
        let channel = get_or_create_channel(&runtime, key).await.unwrap();
        assert_eq!(connection_id(channel, method).await, 1);
    }
    assert_eq!(server.accepted.load(Ordering::SeqCst), 1);
    runtime
        .channels()
        .remove(first_key.unwrap().inline_identity().unwrap());
    server.shutdown().await;
}

#[tokio::test]
async fn actual_cached_channels_separate_process_endpoint_and_manifest_method() {
    let runtime = test_backend_data_runtime();
    let first = Server::start(1).await;
    let other = Server::start(100).await;
    let old = BackendProcessId::new_v7();
    let replacement = BackendProcessId::new_v7();
    let exchange = NativeRpcMethod::DeliverExchangeUnit;
    let filter = NativeRpcMethod::TransmitRuntimeFilterEnvelope;
    let old_exchange = NativeChannelKey::new(Some(old), first.endpoint.clone(), exchange).unwrap();
    let new_exchange =
        NativeChannelKey::new(Some(replacement), first.endpoint.clone(), exchange).unwrap();
    let old_filter = NativeChannelKey::new(Some(old), first.endpoint.clone(), filter).unwrap();
    let other_endpoint =
        NativeChannelKey::new(Some(old), other.endpoint.clone(), exchange).unwrap();

    let original = get_or_create_channel(&runtime, old_exchange.clone())
        .await
        .unwrap();
    let original_id = connection_id(original.clone(), exchange).await;
    let replaced = get_or_create_channel(&runtime, new_exchange.clone())
        .await
        .unwrap();
    let replacement_id = connection_id(replaced.clone(), exchange).await;
    assert_ne!(original_id, replacement_id);
    let filtering = get_or_create_channel(&runtime, old_filter.clone())
        .await
        .unwrap();
    let filter_id = connection_id(filtering.clone(), filter).await;
    assert_ne!(original_id, filter_id);
    assert_ne!(replacement_id, filter_id);

    let repeated = get_or_create_channel(&runtime, old_exchange.clone())
        .await
        .unwrap();
    assert_eq!(connection_id(repeated.clone(), exchange).await, original_id);
    assert_eq!(first.accepted.load(Ordering::SeqCst), 3);

    let elsewhere = get_or_create_channel(&runtime, other_endpoint.clone())
        .await
        .unwrap();
    assert_eq!(connection_id(elsewhere.clone(), exchange).await, 100);
    assert_eq!(other.accepted.load(Ordering::SeqCst), 1);
    assert_eq!(connection_id(original.clone(), exchange).await, original_id);

    // Remove only this fixture's keys from the shared ordinary test runtime.
    // No global clear can invalidate another test's unrelated channel.
    for key in [old_exchange, new_exchange, old_filter, other_endpoint] {
        runtime.channels().remove(key.inline_identity().unwrap());
    }
    drop(original);
    drop(replaced);
    drop(filtering);
    drop(repeated);
    drop(elsewhere);
    first.shutdown().await;
    other.shutdown().await;
}

#[test]
fn channel_key_refuses_missing_or_foreign_peer_domains_and_frontend_methods() {
    let endpoint = NativeEndpoint::from_host_port("127.0.0.1", 9010).unwrap();
    let process = BackendProcessId::new_v7();
    assert!(
        NativeChannelKey::new(None, endpoint.clone(), NativeRpcMethod::DeliverExchangeUnit)
            .is_err()
    );
    assert!(
        NativeChannelKey::new(
            None,
            endpoint.clone(),
            NativeRpcMethod::TransmitRuntimeFilterEnvelope,
        )
        .is_err()
    );
    assert!(
        NativeChannelKey::new(
            Some(process),
            endpoint.clone(),
            NativeRpcMethod::AnnounceBackend,
        )
        .is_err()
    );
    for method in [
        NativeRpcMethod::FetchTaskResult,
        NativeRpcMethod::ApplyTaskOperations,
        NativeRpcMethod::Heartbeat,
        NativeRpcMethod::RetiredFetchResult,
    ] {
        assert!(NativeChannelKey::new(Some(process), endpoint.clone(), method).is_err());
        assert!(NativeChannelKey::new(None, endpoint.clone(), method).is_err());
    }
    assert!(NativeChannelKey::new(None, endpoint, NativeRpcMethod::AnnounceBackend).is_ok());
}

struct FundedRuntime {
    runtime: crate::BackendDataRuntime,
    admission: NativeTransportAdmission,
}
impl FundedRuntime {
    fn new() -> Self {
        let admission = NativeTransportAdmission::new().unwrap();
        let runtime = test_backend_data_runtime()
            .with_transport_admission(admission.clone())
            .unwrap();
        Self { runtime, admission }
    }
    fn held(&self) {
        assert!(
            self.admission.available_positions(TransportClass::Data)
                < self.admission.positions(TransportClass::Data)
        );
    }
    async fn finish_with_alias(self, escaped: Option<Channel>) {
        let Self { runtime, admission } = self;
        drop(runtime);
        // A connection's position follows its IO, not the Channel handle: once
        // the peer has closed it, an escaped Channel alias holds no position.
        drop(escaped);
        // Observe the connection's public exit: its IO wrapper is dropped when
        // the last Channel alias and its background tasks are gone.
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
}

#[tokio::test]
async fn funded_singleflight_nine_real_callers_share_one_dial_and_exact_peer_key() {
    let funded = FundedRuntime::new();
    let first = Server::start(1).await;
    let other = Server::start(100).await;
    let old = BackendProcessId::new_v7();
    let replacement = BackendProcessId::new_v7();
    let exchange = NativeRpcMethod::DeliverExchangeUnit;
    let filter = NativeRpcMethod::TransmitRuntimeFilterEnvelope;
    let key = NativeChannelKey::new(Some(old), first.endpoint.clone(), exchange).unwrap();
    let gate = Arc::new(Barrier::new(10));
    let tasks: Vec<_> = (0..9)
        .map(|_| {
            let runtime = funded.runtime.clone();
            let key = key.clone();
            let gate = gate.clone();
            tokio::spawn(async move {
                gate.wait().await;
                let channel = get_or_create_channel(&runtime, key).await.unwrap();
                let id = connection_id(channel.clone(), exchange).await;
                (channel, id)
            })
        })
        .collect();
    gate.wait().await;
    let mut aliases = Vec::new();
    for task in tasks {
        let (channel, id) = tokio::time::timeout(WATCHDOG, task).await.unwrap().unwrap();
        assert_eq!(id, 1);
        aliases.push(channel);
    }
    assert_eq!(first.accepted.load(Ordering::SeqCst), 1);
    let replacement_key =
        NativeChannelKey::new(Some(replacement), first.endpoint.clone(), exchange).unwrap();
    let filter_key = NativeChannelKey::new(Some(old), first.endpoint.clone(), filter).unwrap();
    let elsewhere_key = NativeChannelKey::new(Some(old), other.endpoint.clone(), exchange).unwrap();
    let replaced = get_or_create_channel(&funded.runtime, replacement_key.clone())
        .await
        .unwrap();
    let filtering = get_or_create_channel(&funded.runtime, filter_key.clone())
        .await
        .unwrap();
    let elsewhere = get_or_create_channel(&funded.runtime, elsewhere_key.clone())
        .await
        .unwrap();
    assert_eq!(connection_id(replaced.clone(), exchange).await, 2);
    assert_eq!(connection_id(filtering.clone(), filter).await, 3);
    assert_eq!(connection_id(elsewhere.clone(), exchange).await, 100);
    let repeated = get_or_create_channel(&funded.runtime, key.clone())
        .await
        .unwrap();
    assert_eq!(connection_id(repeated.clone(), exchange).await, 1);
    assert_eq!(first.accepted.load(Ordering::SeqCst), 3);
    assert_eq!(other.accepted.load(Ordering::SeqCst), 1);
    for key in [key, replacement_key, filter_key, elsewhere_key] {
        drop(
            funded
                .runtime
                .channels()
                .remove(key.inline_identity().unwrap()),
        );
    }
    // An escaped Channel survives cache removal and still has the original
    // startup family. There is no independent per-key/per-lane budget here.
    funded.held();
    let escaped = aliases.pop().unwrap();
    drop(aliases);
    drop(replaced);
    drop(filtering);
    drop(elsewhere);
    drop(repeated);
    first.shutdown().await;
    other.shutdown().await;
    funded.finish_with_alias(Some(escaped)).await;
}
