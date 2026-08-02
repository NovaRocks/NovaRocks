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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{Notify, mpsc};

use crate::native::runtime_filter_adapter::{
    decode_runtime_filter_envelope_response, encode_runtime_filter_envelope,
};
use crate::runtime_filter::router::remote::{
    RuntimeFilterEnvelopeSink, SinkCompletion, SinkSubmitOutcome, SinkTransportError,
};
use novarocks::novarocks_logging::error;
use novarocks::runtime::global_async_runtime::data_runtime_handle;
use novarocks::runtime_filter_transition::port::routing::RuntimeFilterRemoteRoute;
use novarocks::runtime_filter_transition::port::transport::{
    RuntimeFilterAcceptStatus, RuntimeFilterEnvelope, RuntimeFilterRouteIdentity,
    RuntimeFilterTransportEnvelope,
};
use novarocks::service::grpc_client::NovaRocksGrpcRemoteClient;

const LIVE_REQUEST_CAPACITY: usize = 1024;
const LIVE_COMPLETION_CAPACITY: usize = 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeFilterUnaryAck {
    identity: RuntimeFilterRouteIdentity,
    status: RuntimeFilterAcceptStatus,
}

impl RuntimeFilterUnaryAck {
    pub(crate) const fn new(
        identity: RuntimeFilterRouteIdentity,
        status: RuntimeFilterAcceptStatus,
    ) -> Self {
        Self { identity, status }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeFilterUnaryError {
    Transport(String),
    Contract(String),
}

impl RuntimeFilterUnaryError {
    fn transport(error: impl Into<String>) -> Self {
        Self::Transport(error.into())
    }

    fn contract(error: impl Into<String>) -> Self {
        Self::Contract(error.into())
    }
}

#[async_trait::async_trait]
pub(crate) trait RuntimeFilterEnvelopeUnaryClient: Send + Sync + 'static {
    async fn transmit(
        &self,
        route: RuntimeFilterRemoteRoute,
        envelope: Arc<RuntimeFilterEnvelope>,
        deadline: Duration,
    ) -> Result<RuntimeFilterUnaryAck, RuntimeFilterUnaryError>;
}

struct LiveRuntimeFilterEnvelopeUnaryClient;

#[async_trait::async_trait]
impl RuntimeFilterEnvelopeUnaryClient for LiveRuntimeFilterEnvelopeUnaryClient {
    async fn transmit(
        &self,
        route: RuntimeFilterRemoteRoute,
        envelope: Arc<RuntimeFilterEnvelope>,
        deadline: Duration,
    ) -> Result<RuntimeFilterUnaryAck, RuntimeFilterUnaryError> {
        // The endpoint is install-owned route authority. The raw backend index never
        // crosses this seam and cannot be confused with participant (+1) identity.
        let client = NovaRocksGrpcRemoteClient::new_runtime_endpoint(route.endpoint())
            .map_err(RuntimeFilterUnaryError::transport)?;
        // Deliberately encode at the unary boundary instead of retaining a second
        // protobuf copy beside the semantic envelope. The sink queues are bounded and
        // `run_worker` awaits one request at a time, so at most one transient protobuf
        // encoding is live per sink worker while retries keep sharing the same Arc.
        let response = client
            .transmit_runtime_filter_envelope_async(
                encode_runtime_filter_envelope(envelope.as_ref()),
                deadline,
            )
            .await
            .map_err(RuntimeFilterUnaryError::transport)?;
        decode_runtime_filter_unary_ack(response)
    }
}

fn decode_runtime_filter_unary_ack(
    response: novarocks::proto::filter::RuntimeFilterEnvelopeResponse,
) -> Result<RuntimeFilterUnaryAck, RuntimeFilterUnaryError> {
    decode_runtime_filter_envelope_response(response)
        .map(|(identity, status)| RuntimeFilterUnaryAck::new(identity, status))
        .map_err(RuntimeFilterUnaryError::contract)
}

struct SinkRequest {
    route: RuntimeFilterRemoteRoute,
    envelope: RuntimeFilterTransportEnvelope,
}

pub(crate) struct GrpcRuntimeFilterEnvelopeSink {
    requests: mpsc::Sender<SinkRequest>,
    completions: Mutex<mpsc::Receiver<SinkCompletion>>,
    shutdown: Arc<AtomicBool>,
    shutdown_notify: Arc<Notify>,
}

impl GrpcRuntimeFilterEnvelopeSink {
    pub(crate) fn new() -> Arc<Self> {
        Self::new_with_client_and_capacities(
            Arc::new(LiveRuntimeFilterEnvelopeUnaryClient),
            LIVE_REQUEST_CAPACITY,
            LIVE_COMPLETION_CAPACITY,
        )
    }

    #[cfg(test)]
    fn new_for_test(
        client: Arc<dyn RuntimeFilterEnvelopeUnaryClient>,
        request_capacity: usize,
        completion_capacity: usize,
    ) -> Result<Arc<Self>, String> {
        if request_capacity == 0 || completion_capacity == 0 {
            return Err("runtime filter sink capacities must be nonzero".to_string());
        }
        Ok(Self::new_with_client_and_capacities(
            client,
            request_capacity,
            completion_capacity,
        ))
    }

    fn new_with_client_and_capacities(
        client: Arc<dyn RuntimeFilterEnvelopeUnaryClient>,
        request_capacity: usize,
        completion_capacity: usize,
    ) -> Arc<Self> {
        let (request_tx, request_rx) = mpsc::channel(request_capacity);
        let (completion_tx, completion_rx) = mpsc::channel(completion_capacity);
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_notify = Arc::new(Notify::new());
        let sink = Arc::new(Self {
            requests: request_tx,
            completions: Mutex::new(completion_rx),
            shutdown: shutdown.clone(),
            shutdown_notify: shutdown_notify.clone(),
        });
        match data_runtime_handle() {
            Ok(runtime) => {
                runtime.spawn(run_worker(
                    request_rx,
                    completion_tx,
                    client,
                    shutdown,
                    shutdown_notify,
                ));
            }
            Err(runtime_error) => {
                sink.shutdown.store(true, Ordering::Release);
                error!(
                    error = %runtime_error,
                    "runtime filter envelope worker could not start"
                );
            }
        }
        sink
    }
}

impl RuntimeFilterEnvelopeSink for GrpcRuntimeFilterEnvelopeSink {
    fn try_send(
        &self,
        route: RuntimeFilterRemoteRoute,
        envelope: RuntimeFilterTransportEnvelope,
    ) -> SinkSubmitOutcome {
        if self.shutdown.load(Ordering::Acquire) {
            return SinkSubmitOutcome::Shutdown;
        }
        match self.requests.try_send(SinkRequest { route, envelope }) {
            Ok(()) => SinkSubmitOutcome::Submitted,
            Err(mpsc::error::TrySendError::Full(_)) => SinkSubmitOutcome::QueueFull,
            Err(mpsc::error::TrySendError::Closed(_)) => SinkSubmitOutcome::Shutdown,
        }
    }

    fn try_recv_completion(&self) -> Option<SinkCompletion> {
        self.completions
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .try_recv()
            .ok()
    }

    fn shutdown(&self) {
        if !self.shutdown.swap(true, Ordering::AcqRel) {
            // `notify_waiters` covers a worker already inside select; `notify_one`
            // stores one permit for the race where it has not entered select yet.
            self.shutdown_notify.notify_waiters();
            self.shutdown_notify.notify_one();
        }
    }
}

impl Drop for GrpcRuntimeFilterEnvelopeSink {
    fn drop(&mut self) {
        self.shutdown();
    }
}

async fn run_worker(
    mut requests: mpsc::Receiver<SinkRequest>,
    completions: mpsc::Sender<SinkCompletion>,
    client: Arc<dyn RuntimeFilterEnvelopeUnaryClient>,
    shutdown: Arc<AtomicBool>,
    shutdown_notify: Arc<Notify>,
) {
    loop {
        if shutdown.load(Ordering::Acquire) {
            break;
        }
        let request = tokio::select! {
            biased;
            _ = shutdown_notify.notified() => break,
            request = requests.recv() => match request {
                Some(request) => request,
                None => break,
            },
        };
        let (envelope, deadline) = request.envelope.into_parts();
        let requested_identity = envelope.route_identity().clone();
        let result = tokio::select! {
            biased;
            _ = shutdown_notify.notified() => break,
            result = client.transmit(request.route, envelope, deadline) => result,
        };
        let completion = match result {
            Ok(ack) if ack.identity == requested_identity => {
                SinkCompletion::Ack(ack.identity, ack.status)
            }
            Ok(ack) => SinkCompletion::TransportFailure(
                requested_identity.clone(),
                SinkTransportError::contract(format!(
                    "runtime filter ACK identity mismatch: requested={:?} acked={:?}",
                    requested_identity, ack.identity
                )),
            ),
            Err(RuntimeFilterUnaryError::Transport(error)) => SinkCompletion::TransportFailure(
                requested_identity,
                SinkTransportError::network(error),
            ),
            Err(RuntimeFilterUnaryError::Contract(error)) => SinkCompletion::TransportFailure(
                requested_identity,
                SinkTransportError::contract(error),
            ),
        };
        // A full completion queue retains this one completion in the worker future
        // and applies bounded backpressure to the request queue. Awaiting is async:
        // it never blocks an OS or Tokio worker thread, and shutdown interrupts it.
        tokio::select! {
            biased;
            _ = shutdown_notify.notified() => break,
            sent = completions.send(completion) => {
                if sent.is_err() {
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::sync::{Mutex, Semaphore, mpsc};

    use super::{
        GrpcRuntimeFilterEnvelopeSink, RuntimeFilterEnvelopeUnaryClient, RuntimeFilterUnaryAck,
        RuntimeFilterUnaryError, decode_runtime_filter_unary_ack,
    };
    use crate::runtime_filter::router::remote::{
        RuntimeFilterEnvelopeSink, SinkCompletion, SinkSubmitOutcome,
    };
    use novarocks::runtime::endpoint::RuntimeEndpoint;
    use novarocks::runtime::global_async_runtime::{data_block_on, data_runtime_handle};
    use novarocks::runtime_filter_transition::model::contract::{BindingId, ChannelId};
    use novarocks::runtime_filter_transition::port::identity::{
        DeploymentEpoch, ProducerSequence, RouteEdgeId, RuntimeFilterParticipantId,
    };
    use novarocks::runtime_filter_transition::port::routing::{
        RuntimeFilterRemoteRoute, RuntimeFilterRouteRole,
    };
    use novarocks::runtime_filter_transition::port::transport::{
        DeliveryRouteIdentity, RuntimeFilterAcceptStatus, RuntimeFilterEnvelope,
        RuntimeFilterEnvelopeKind, RuntimeFilterRouteIdentity, RuntimeFilterTransportEnvelope,
    };
    use novarocks_types::UniqueId;

    struct FakeUnaryClient {
        seen: mpsc::Sender<(RuntimeFilterRemoteRoute, Arc<RuntimeFilterEnvelope>)>,
        responses: Mutex<VecDeque<Result<RuntimeFilterUnaryAck, RuntimeFilterUnaryError>>>,
        gate: Option<Arc<Semaphore>>,
    }

    #[async_trait::async_trait]
    impl RuntimeFilterEnvelopeUnaryClient for FakeUnaryClient {
        async fn transmit(
            &self,
            route: RuntimeFilterRemoteRoute,
            envelope: Arc<RuntimeFilterEnvelope>,
            _deadline: Duration,
        ) -> Result<RuntimeFilterUnaryAck, RuntimeFilterUnaryError> {
            self.seen
                .send((route, envelope))
                .await
                .expect("bounded observation channel remains open");
            if let Some(gate) = &self.gate {
                gate.acquire()
                    .await
                    .expect("test gate remains open")
                    .forget();
            }
            self.responses
                .lock()
                .await
                .pop_front()
                .expect("fake response is configured")
        }
    }

    struct MalformedAckUnaryClient;

    #[async_trait::async_trait]
    impl RuntimeFilterEnvelopeUnaryClient for MalformedAckUnaryClient {
        async fn transmit(
            &self,
            _route: RuntimeFilterRemoteRoute,
            envelope: Arc<RuntimeFilterEnvelope>,
            _deadline: Duration,
        ) -> Result<RuntimeFilterUnaryAck, RuntimeFilterUnaryError> {
            let response = novarocks::proto::filter::RuntimeFilterEnvelopeResponse {
                acked_route_identity:
                    crate::native::runtime_filter_adapter::encode_runtime_filter_envelope(
                        envelope.as_ref(),
                    )
                    .route_identity,
                accept_status: novarocks::proto::filter::RuntimeFilterAcceptStatus::Unspecified
                    as i32,
                rejection_reason: String::new(),
            };
            decode_runtime_filter_unary_ack(response)
        }
    }

    fn route(edge: u32) -> RuntimeFilterRemoteRoute {
        RuntimeFilterRemoteRoute::new(
            RouteEdgeId::new(edge),
            RuntimeFilterParticipantId::new(8),
            RuntimeEndpoint::new("127.0.0.1", 19080).expect("endpoint"),
            RuntimeFilterRouteRole::Consumer(BindingId::new(31)),
        )
        .expect("remote route")
    }

    fn identity(edge: u32, sequence: u64) -> RuntimeFilterRouteIdentity {
        RuntimeFilterRouteIdentity::delivery(
            DeliveryRouteIdentity::try_new(RouteEdgeId::new(edge), ProducerSequence::new(sequence))
                .expect("delivery identity"),
        )
    }

    fn envelope(edge: u32, sequence: u64) -> RuntimeFilterTransportEnvelope {
        RuntimeFilterTransportEnvelope::new(
            Arc::new(
                RuntimeFilterEnvelope::try_new(
                    RuntimeFilterEnvelopeKind::Artifact,
                    UniqueId::new(11, 12),
                    ChannelId::new(13),
                    DeploymentEpoch::new(14),
                    identity(edge, sequence),
                    None,
                    None,
                    &[15; 32],
                    b"complete-domain-envelope".to_vec(),
                )
                .expect("domain envelope"),
            ),
            Duration::from_secs(2),
        )
    }

    fn recv_seen(
        receiver: &mut mpsc::Receiver<(RuntimeFilterRemoteRoute, Arc<RuntimeFilterEnvelope>)>,
    ) -> (RuntimeFilterRemoteRoute, Arc<RuntimeFilterEnvelope>) {
        data_block_on(async {
            tokio::time::timeout(Duration::from_secs(1), receiver.recv())
                .await
                .expect("fake unary client is invoked")
                .expect("observation channel remains open")
        })
        .expect("data runtime")
    }

    fn recv_completion(sink: &GrpcRuntimeFilterEnvelopeSink) -> SinkCompletion {
        data_block_on(async {
            tokio::time::timeout(Duration::from_secs(1), async {
                loop {
                    if let Some(completion) = sink.try_recv_completion() {
                        return completion;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("sink completion arrives")
        })
        .expect("data runtime")
    }

    #[test]
    fn live_sink_sends_complete_domain_envelope() {
        let expected_identity = identity(40, 7);
        let (seen_tx, mut seen_rx) = mpsc::channel(1);
        let client = Arc::new(FakeUnaryClient {
            seen: seen_tx,
            responses: Mutex::new(VecDeque::from([Ok(RuntimeFilterUnaryAck::new(
                expected_identity.clone(),
                RuntimeFilterAcceptStatus::Accepted,
            ))])),
            gate: None,
        });
        let sink = GrpcRuntimeFilterEnvelopeSink::new_for_test(client, 4, 4)
            .expect("live sink on shared data runtime");

        assert_eq!(
            sink.try_send(route(40), envelope(40, 7)),
            SinkSubmitOutcome::Submitted
        );
        let (seen_route, seen_envelope) = recv_seen(&mut seen_rx);
        assert_eq!(seen_route, route(40));
        assert_eq!(seen_envelope.query_id(), UniqueId::new(11, 12));
        assert_eq!(seen_envelope.channel_id(), ChannelId::new(13));
        assert_eq!(seen_envelope.deployment_epoch(), DeploymentEpoch::new(14));
        assert_eq!(seen_envelope.route_identity(), &expected_identity);
        assert_eq!(seen_envelope.schema_digest(), &[15; 32]);
        assert_eq!(seen_envelope.payload(), b"complete-domain-envelope");
        assert_eq!(
            recv_completion(&sink),
            SinkCompletion::Ack(expected_identity, RuntimeFilterAcceptStatus::Accepted)
        );
        sink.shutdown();
    }

    #[test]
    fn ack_identity_mismatch_is_contract_rejection() {
        let requested_identity = identity(44, 7);
        let mismatched_identity = identity(45, 7);
        let (seen_tx, mut seen_rx) = mpsc::channel(1);
        let client = Arc::new(FakeUnaryClient {
            seen: seen_tx,
            responses: Mutex::new(VecDeque::from([Ok(RuntimeFilterUnaryAck::new(
                mismatched_identity,
                RuntimeFilterAcceptStatus::Accepted,
            ))])),
            gate: None,
        });
        let sink = GrpcRuntimeFilterEnvelopeSink::new_for_test(client, 4, 4)
            .expect("live sink on shared data runtime");

        assert_eq!(
            sink.try_send(route(44), envelope(44, 7)),
            SinkSubmitOutcome::Submitted
        );
        let _ = recv_seen(&mut seen_rx);
        match recv_completion(&sink) {
            SinkCompletion::TransportFailure(identity, error) => {
                assert_eq!(identity, requested_identity);
                assert!(error.is_contract(), "{error}");
            }
            completion => panic!("expected contract rejection, got {completion:?}"),
        }
        sink.shutdown();
    }

    #[test]
    fn malformed_ack_status_is_contract_rejection() {
        let requested_identity = identity(46, 7);
        let sink =
            GrpcRuntimeFilterEnvelopeSink::new_for_test(Arc::new(MalformedAckUnaryClient), 4, 4)
                .expect("live sink on shared data runtime");

        assert_eq!(
            sink.try_send(route(46), envelope(46, 7)),
            SinkSubmitOutcome::Submitted
        );
        match recv_completion(&sink) {
            SinkCompletion::TransportFailure(identity, error) => {
                assert_eq!(identity, requested_identity);
                assert!(error.is_contract(), "{error}");
                assert!(
                    error
                        .to_string()
                        .contains("ACK accept status must be specified"),
                    "{error}"
                );
            }
            completion => panic!("expected malformed ACK contract rejection, got {completion:?}"),
        }
        sink.shutdown();
    }

    #[test]
    fn rpc_failure_remains_a_transport_failure() {
        let requested_identity = identity(47, 7);
        let (seen_tx, mut seen_rx) = mpsc::channel(1);
        let client = Arc::new(FakeUnaryClient {
            seen: seen_tx,
            responses: Mutex::new(VecDeque::from([Err(RuntimeFilterUnaryError::transport(
                "temporary peer outage",
            ))])),
            gate: None,
        });
        let sink = GrpcRuntimeFilterEnvelopeSink::new_for_test(client, 4, 4)
            .expect("live sink on shared data runtime");

        assert_eq!(
            sink.try_send(route(47), envelope(47, 7)),
            SinkSubmitOutcome::Submitted
        );
        let _ = recv_seen(&mut seen_rx);
        match recv_completion(&sink) {
            SinkCompletion::TransportFailure(identity, error) => {
                assert_eq!(identity, requested_identity);
                assert!(!error.is_contract(), "{error}");
            }
            completion => panic!("expected retryable transport failure, got {completion:?}"),
        }
        sink.shutdown();
    }

    #[test]
    fn request_queue_full_is_retryable_and_bounded() {
        let (seen_tx, mut seen_rx) = mpsc::channel(4);
        let gate = Arc::new(Semaphore::new(0));
        let client = Arc::new(FakeUnaryClient {
            seen: seen_tx,
            responses: Mutex::new(VecDeque::from([
                Ok(RuntimeFilterUnaryAck::new(
                    identity(41, 1),
                    RuntimeFilterAcceptStatus::Accepted,
                )),
                Ok(RuntimeFilterUnaryAck::new(
                    identity(42, 2),
                    RuntimeFilterAcceptStatus::Accepted,
                )),
            ])),
            gate: Some(gate.clone()),
        });
        let sink =
            GrpcRuntimeFilterEnvelopeSink::new_for_test(client, 1, 4).expect("bounded live sink");

        assert_eq!(
            sink.try_send(route(41), envelope(41, 1)),
            SinkSubmitOutcome::Submitted
        );
        let _ = recv_seen(&mut seen_rx);
        assert_eq!(
            sink.try_send(route(42), envelope(42, 2)),
            SinkSubmitOutcome::Submitted
        );
        assert_eq!(
            sink.try_send(route(43), envelope(43, 3)),
            SinkSubmitOutcome::QueueFull
        );

        gate.add_permits(2);
        sink.shutdown();
    }

    #[test]
    fn completion_queue_never_blocks_runtime_worker() {
        let (seen_tx, mut seen_rx) = mpsc::channel(4);
        let client = Arc::new(FakeUnaryClient {
            seen: seen_tx,
            responses: Mutex::new(VecDeque::from([
                Ok(RuntimeFilterUnaryAck::new(
                    identity(51, 1),
                    RuntimeFilterAcceptStatus::Accepted,
                )),
                Ok(RuntimeFilterUnaryAck::new(
                    identity(52, 2),
                    RuntimeFilterAcceptStatus::Duplicate,
                )),
            ])),
            gate: None,
        });
        let sink =
            GrpcRuntimeFilterEnvelopeSink::new_for_test(client, 4, 1).expect("bounded live sink");

        assert_eq!(
            sink.try_send(route(51), envelope(51, 1)),
            SinkSubmitOutcome::Submitted
        );
        let _ = recv_seen(&mut seen_rx);
        assert_eq!(
            sink.try_send(route(52), envelope(52, 2)),
            SinkSubmitOutcome::Submitted
        );
        let _ = recv_seen(&mut seen_rx);

        let (progress_tx, progress_rx) = tokio::sync::oneshot::channel();
        data_runtime_handle()
            .expect("shared data runtime")
            .spawn(async move {
                progress_tx
                    .send(())
                    .expect("progress observer remains open");
            });
        data_block_on(async {
            tokio::time::timeout(Duration::from_secs(1), progress_rx)
                .await
                .expect("independent runtime task is not blocked")
                .expect("progress sender remains open");
        })
        .expect("data runtime");

        assert_eq!(
            recv_completion(&sink),
            SinkCompletion::Ack(identity(51, 1), RuntimeFilterAcceptStatus::Accepted)
        );
        assert_eq!(
            recv_completion(&sink),
            SinkCompletion::Ack(identity(52, 2), RuntimeFilterAcceptStatus::Duplicate)
        );
        sink.shutdown();
    }

    #[test]
    fn shutdown_wakes_deferred_completion_and_stops_worker() {
        let (seen_tx, mut seen_rx) = mpsc::channel(4);
        let client = Arc::new(FakeUnaryClient {
            seen: seen_tx,
            responses: Mutex::new(VecDeque::from([
                Ok(RuntimeFilterUnaryAck::new(
                    identity(61, 1),
                    RuntimeFilterAcceptStatus::Accepted,
                )),
                Ok(RuntimeFilterUnaryAck::new(
                    identity(62, 2),
                    RuntimeFilterAcceptStatus::Accepted,
                )),
            ])),
            gate: None,
        });
        let client_weak = Arc::downgrade(&client);
        let sink = GrpcRuntimeFilterEnvelopeSink::new_for_test(client.clone(), 4, 1)
            .expect("bounded live sink");
        drop(client);

        assert_eq!(
            sink.try_send(route(61), envelope(61, 1)),
            SinkSubmitOutcome::Submitted
        );
        let _ = recv_seen(&mut seen_rx);
        assert_eq!(
            sink.try_send(route(62), envelope(62, 2)),
            SinkSubmitOutcome::Submitted
        );
        let _ = recv_seen(&mut seen_rx);

        sink.shutdown();
        drop(sink);
        data_block_on(async {
            tokio::time::timeout(Duration::from_secs(1), async {
                while client_weak.upgrade().is_some() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("shutdown releases the worker-owned client");
        })
        .expect("data runtime");
    }
}
