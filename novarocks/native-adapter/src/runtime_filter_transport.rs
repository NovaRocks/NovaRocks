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

//! Native gRPC sender for runtime-filter envelopes.
//!
//! The sender owns only bounded unary delivery. Route authority belongs to the
//! Worker participant domain and canonical contribution/artifact semantics
//! remain outside this module.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::{Notify, mpsc};
use tracing::warn;

use novarocks_proto_models::filter::RuntimeFilterEnvelopeResponse;

use crate::runtime_filter_rpc::{
    BackendNativeRouteIdentity, BackendNativeRuntimeFilterEnvelope,
    decode_runtime_filter_envelope_response, encode_runtime_filter_envelope,
    runtime_filter_envelope_encoded_len,
};
use crate::{BackendDataRuntime, NativeRpcClient};
use novarocks_worker::runtime_filter::domain::{BackendAcceptStatus, BackendRemoteRoute};
use novarocks_worker::{
    ReliableTransportFailOpenReason, ReliableTransportFailureOutcome, ReliableTransportPolicy,
    ReliableTransportSendOutcome, ReliableTransportState,
};

const LIVE_REQUEST_CAPACITY: usize = 1024;
const LIVE_COMPLETION_CAPACITY: usize = 1024;

#[cfg(test)]
fn test_backend_data_runtime() -> BackendDataRuntime {
    use novarocks_native_trust::{
        DeploymentId, NativeCallerSubject, NativeTransportMode, NativeTrust, ValidatedSharedSecret,
    };
    use novarocks_secret::SecretValue;

    static TEST_RUNTIME: std::sync::LazyLock<(tokio::runtime::Runtime, BackendDataRuntime)> =
        std::sync::LazyLock::new(|| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .worker_threads(1)
                .build()
                .expect("build Native Adapter test data runtime");
            let trust = Arc::new(NativeTrust::new(
                DeploymentId::parse("native-adapter-test").expect("valid deployment"),
                ValidatedSharedSecret::new(SecretValue::new("0123456789abcdef0123456789abcdef"))
                    .expect("valid secret"),
                NativeCallerSubject::parse("be@127.0.0.1:9070").expect("valid subject"),
                NativeTransportMode::Disabled,
            ));
            let adapter = BackendDataRuntime::new(
                runtime.handle().clone(),
                trust,
                crate::BackendNativeTransport::Plaintext,
            );
            (runtime, adapter)
        });
    TEST_RUNTIME.1.clone()
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackendRuntimeFilterUnaryAck {
    identity: BackendNativeRouteIdentity,
    status: BackendAcceptStatus,
}

impl BackendRuntimeFilterUnaryAck {
    pub(crate) const fn new(
        identity: BackendNativeRouteIdentity,
        status: BackendAcceptStatus,
    ) -> Self {
        Self { identity, status }
    }

    pub(crate) const fn identity(&self) -> BackendNativeRouteIdentity {
        self.identity
    }

    pub(crate) const fn status(&self) -> BackendAcceptStatus {
        self.status
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BackendRuntimeFilterUnaryError {
    Transport(String),
    Contract(String),
}

impl BackendRuntimeFilterUnaryError {
    fn transport(error: impl Into<String>) -> Self {
        Self::Transport(error.into())
    }

    fn contract(error: impl Into<String>) -> Self {
        Self::Contract(error.into())
    }
}

/// Frozen per-query policy for native runtime-filter unary delivery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BackendRuntimeFilterRetryPolicy {
    retry_interval: Duration,
    max_attempts: u32,
    deadline: Duration,
    max_pending_entries: usize,
    max_pending_bytes: usize,
}

impl BackendRuntimeFilterRetryPolicy {
    pub fn new(
        retry_interval: Duration,
        max_attempts: u32,
        deadline: Duration,
        max_pending_entries: usize,
        max_pending_bytes: usize,
    ) -> Result<Self, BackendRuntimeFilterUnaryError> {
        if retry_interval.is_zero()
            || max_attempts == 0
            || deadline.is_zero()
            || max_pending_entries == 0
            || max_pending_bytes == 0
        {
            return Err(BackendRuntimeFilterUnaryError::contract(
                "runtime filter retry policy values must be non-zero",
            ));
        }
        Ok(Self {
            retry_interval,
            max_attempts,
            deadline,
            max_pending_entries,
            max_pending_bytes,
        })
    }
}

impl ReliableTransportPolicy for BackendRuntimeFilterRetryPolicy {
    fn retry_interval(self) -> Duration {
        self.retry_interval
    }

    fn max_attempts(self) -> u32 {
        self.max_attempts
    }

    fn deadline(self) -> Duration {
        self.deadline
    }

    fn max_pending_entries(self) -> usize {
        self.max_pending_entries
    }

    fn max_pending_bytes(self) -> usize {
        self.max_pending_bytes
    }
}

#[derive(Clone, Debug)]
pub struct BackendNativeRuntimeFilterTransportEnvelope {
    envelope: Arc<BackendNativeRuntimeFilterEnvelope>,
    policy: BackendRuntimeFilterRetryPolicy,
}

impl BackendNativeRuntimeFilterTransportEnvelope {
    pub fn new(
        envelope: Arc<BackendNativeRuntimeFilterEnvelope>,
        policy: BackendRuntimeFilterRetryPolicy,
    ) -> Result<Self, BackendRuntimeFilterUnaryError> {
        Ok(Self { envelope, policy })
    }

    pub fn into_parts(
        self,
    ) -> (
        Arc<BackendNativeRuntimeFilterEnvelope>,
        BackendRuntimeFilterRetryPolicy,
    ) {
        (self.envelope, self.policy)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BackendRuntimeFilterSinkSubmitOutcome {
    Submitted,
    QueueFull,
    Shutdown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BackendRuntimeFilterSinkCompletion {
    Ack(BackendNativeRouteIdentity, BackendAcceptStatus),
    Retried(BackendNativeRouteIdentity),
    TransportFailure(
        BackendNativeRouteIdentity,
        BackendRuntimeFilterUnaryError,
        BackendRuntimeFilterTransportFailureReason,
    ),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackendRuntimeFilterTransportFailureReason {
    Deadline,
    AttemptsExhausted,
    ContractRejected,
}

/// Backend-native sink contract. It deliberately does not expose the old Core
/// router transport types; the Backend reliable transport integrates through
/// this port once it owns the physical route session.
pub trait BackendRuntimeFilterEnvelopeSink: Send + Sync {
    fn try_send(
        &self,
        route: BackendRemoteRoute,
        envelope: BackendNativeRuntimeFilterTransportEnvelope,
    ) -> BackendRuntimeFilterSinkSubmitOutcome;

    fn try_recv_completion(&self) -> Option<BackendRuntimeFilterSinkCompletion>;

    fn shutdown(&self);
}

#[async_trait::async_trait]
pub trait BackendRuntimeFilterEnvelopeUnaryClient: Send + Sync + 'static {
    async fn transmit(
        &self,
        route: BackendRemoteRoute,
        envelope: Arc<BackendNativeRuntimeFilterEnvelope>,
        deadline: Duration,
    ) -> Result<BackendRuntimeFilterUnaryAck, BackendRuntimeFilterUnaryError>;
}

struct LiveRuntimeFilterEnvelopeUnaryClient {
    runtime: BackendDataRuntime,
}

#[async_trait::async_trait]
impl BackendRuntimeFilterEnvelopeUnaryClient for LiveRuntimeFilterEnvelopeUnaryClient {
    async fn transmit(
        &self,
        route: BackendRemoteRoute,
        envelope: Arc<BackendNativeRuntimeFilterEnvelope>,
        deadline: Duration,
    ) -> Result<BackendRuntimeFilterUnaryAck, BackendRuntimeFilterUnaryError> {
        let client = NativeRpcClient::new_backend_endpoint(
            self.runtime.clone(),
            route.endpoint().native_endpoint().clone(),
            route.backend_process_id(),
        );
        let response = client
            .transmit_runtime_filter_envelope_async(
                encode_runtime_filter_envelope(envelope.as_ref()),
                deadline,
            )
            .await
            .map_err(BackendRuntimeFilterUnaryError::transport)?;
        // The peer's refusal reason exists only here. It never reaches the
        // completion channel, which carries a status and no text, and refusing
        // a runtime filter fails nothing downstream -- so without this line a
        // filter refused by every peer is indistinguishable from one that was
        // never produced, and the only symptom is a consumer waiting out its
        // whole wait cap.
        let rejection =
            (!response.rejection_reason.is_empty()).then(|| response.rejection_reason.clone());
        let ack = decode_runtime_filter_unary_ack(response)?;
        if ack.status() == BackendAcceptStatus::Rejected {
            warn!(
                target: "novarocks::runtime_filter",
                query_id = ?envelope.query_id(),
                deployment_epoch = envelope.deployment_epoch(),
                channel_id = envelope.channel_id().get(),
                kind = ?envelope.kind(),
                peer = %route.endpoint(),
                reason = rejection.as_deref().unwrap_or("<none>"),
                "runtime filter envelope refused by its peer backend"
            );
        }
        Ok(ack)
    }
}

fn decode_runtime_filter_unary_ack(
    response: RuntimeFilterEnvelopeResponse,
) -> Result<BackendRuntimeFilterUnaryAck, BackendRuntimeFilterUnaryError> {
    decode_runtime_filter_envelope_response(response)
        .map(|(identity, status)| BackendRuntimeFilterUnaryAck::new(identity, status))
        .map_err(BackendRuntimeFilterUnaryError::contract)
}

struct SinkRequest {
    route: BackendRemoteRoute,
    envelope: BackendNativeRuntimeFilterTransportEnvelope,
}

pub struct GrpcRuntimeFilterEnvelopeSink {
    requests: mpsc::Sender<SinkRequest>,
    completions: Mutex<mpsc::Receiver<BackendRuntimeFilterSinkCompletion>>,
    shutdown: Arc<AtomicBool>,
    shutdown_notify: Arc<Notify>,
    worker: Mutex<
        Option<
            tokio::task::JoinHandle<
                Result<(), novarocks_control_runtime::exit_work::ExitWorkError>,
            >,
        >,
    >,
}

#[allow(
    dead_code,
    reason = "Retained for target-specific native integration and regression coverage."
)]
impl GrpcRuntimeFilterEnvelopeSink {
    pub fn new(
        runtime: BackendDataRuntime,
        work: novarocks_worker::RuntimeFilterSenderWork,
    ) -> Arc<Self> {
        Self::new_with_client_and_capacities(
            runtime.clone(),
            work,
            Arc::new(LiveRuntimeFilterEnvelopeUnaryClient {
                runtime: runtime.clone(),
            }),
            LIVE_REQUEST_CAPACITY,
            LIVE_COMPLETION_CAPACITY,
        )
    }

    #[cfg(test)]
    fn new_for_test(
        client: Arc<dyn BackendRuntimeFilterEnvelopeUnaryClient>,
        request_capacity: usize,
        completion_capacity: usize,
        work: novarocks_worker::RuntimeFilterSenderWork,
    ) -> Result<Arc<Self>, String> {
        if request_capacity == 0 || completion_capacity == 0 {
            return Err("runtime filter sink capacities must be nonzero".to_string());
        }
        Ok(Self::new_with_client_and_capacities(
            test_backend_data_runtime(),
            work,
            client,
            request_capacity,
            completion_capacity,
        ))
    }

    fn new_with_client_and_capacities(
        runtime: BackendDataRuntime,
        work: novarocks_worker::RuntimeFilterSenderWork,
        client: Arc<dyn BackendRuntimeFilterEnvelopeUnaryClient>,
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
            shutdown: Arc::clone(&shutdown),
            shutdown_notify: Arc::clone(&shutdown_notify),
            worker: Mutex::new(None),
        });
        let worker = runtime.spawn_with_obligation(
            work,
            run_worker(request_rx, completion_tx, client, shutdown, shutdown_notify),
        );
        *sink
            .worker
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(worker);
        sink
    }
}

impl BackendRuntimeFilterEnvelopeSink for GrpcRuntimeFilterEnvelopeSink {
    fn try_send(
        &self,
        route: BackendRemoteRoute,
        envelope: BackendNativeRuntimeFilterTransportEnvelope,
    ) -> BackendRuntimeFilterSinkSubmitOutcome {
        if self.shutdown.load(Ordering::Acquire) {
            return BackendRuntimeFilterSinkSubmitOutcome::Shutdown;
        }
        match self.requests.try_send(SinkRequest { route, envelope }) {
            Ok(()) => BackendRuntimeFilterSinkSubmitOutcome::Submitted,
            Err(mpsc::error::TrySendError::Full(_)) => {
                BackendRuntimeFilterSinkSubmitOutcome::QueueFull
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                BackendRuntimeFilterSinkSubmitOutcome::Shutdown
            }
        }
    }

    fn try_recv_completion(&self) -> Option<BackendRuntimeFilterSinkCompletion> {
        self.completions
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .try_recv()
            .ok()
    }

    fn shutdown(&self) {
        if !self.shutdown.swap(true, Ordering::AcqRel) {
            self.shutdown_notify.notify_waiters();
            self.shutdown_notify.notify_one();
        }
        if let Some(worker) = self
            .worker
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
        {
            worker.abort();
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
    completions: mpsc::Sender<BackendRuntimeFilterSinkCompletion>,
    client: Arc<dyn BackendRuntimeFilterEnvelopeUnaryClient>,
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
        let Some(completion) =
            process_request(request, Arc::clone(&client), &completions, &shutdown_notify).await
        else {
            break;
        };
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

async fn process_request(
    request: SinkRequest,
    client: Arc<dyn BackendRuntimeFilterEnvelopeUnaryClient>,
    completions: &mpsc::Sender<BackendRuntimeFilterSinkCompletion>,
    shutdown_notify: &Notify,
) -> Option<BackendRuntimeFilterSinkCompletion> {
    let (envelope, policy) = request.envelope.into_parts();
    let identity = *envelope.route_identity();
    let started = Instant::now();
    let retained_bytes = match runtime_filter_envelope_encoded_len(envelope.as_ref()) {
        Ok(bytes) => bytes,
        Err(error) => {
            return Some(BackendRuntimeFilterSinkCompletion::TransportFailure(
                identity,
                BackendRuntimeFilterUnaryError::contract(error.to_string()),
                BackendRuntimeFilterTransportFailureReason::ContractRejected,
            ));
        }
    };
    let mut state = ReliableTransportState::new(policy);
    match state.send(identity, Arc::clone(&envelope), retained_bytes, started) {
        Ok(ReliableTransportSendOutcome::Buffered) => {}
        Ok(outcome) => {
            return Some(BackendRuntimeFilterSinkCompletion::TransportFailure(
                identity,
                BackendRuntimeFilterUnaryError::contract(format!(
                    "runtime filter transport admission failed: {outcome:?}"
                )),
                BackendRuntimeFilterTransportFailureReason::ContractRejected,
            ));
        }
        Err(error) => {
            return Some(BackendRuntimeFilterSinkCompletion::TransportFailure(
                identity,
                BackendRuntimeFilterUnaryError::contract(format!(
                    "runtime filter transport identity conflict: {error:?}"
                )),
                BackendRuntimeFilterTransportFailureReason::ContractRejected,
            ));
        }
    }
    let mut frame = envelope;
    loop {
        let now = Instant::now();
        let elapsed = now.saturating_duration_since(started);
        let Some(remaining) = policy.deadline().checked_sub(elapsed) else {
            return Some(fail_open_completion(
                &mut state,
                identity,
                ReliableTransportFailOpenReason::Deadline,
            ));
        };
        let result = tokio::select! {
            biased;
            _ = shutdown_notify.notified() => return None,
            result = client.transmit(request.route.clone(), Arc::clone(&frame), remaining) => result,
        };
        match result {
            Ok(ack) if ack.identity() == identity => {
                let _ = state.acknowledge(identity);
                return Some(BackendRuntimeFilterSinkCompletion::Ack(
                    ack.identity(),
                    ack.status(),
                ));
            }
            Ok(ack) => {
                let _ = state.acknowledge(identity);
                return Some(BackendRuntimeFilterSinkCompletion::TransportFailure(
                    identity,
                    BackendRuntimeFilterUnaryError::contract(format!(
                        "runtime filter ACK identity mismatch: requested={identity:?} acked={:?}",
                        ack.identity(),
                    )),
                    BackendRuntimeFilterTransportFailureReason::ContractRejected,
                ));
            }
            Err(BackendRuntimeFilterUnaryError::Contract(error)) => {
                let _ = state.acknowledge(identity);
                return Some(BackendRuntimeFilterSinkCompletion::TransportFailure(
                    identity,
                    BackendRuntimeFilterUnaryError::Contract(error),
                    BackendRuntimeFilterTransportFailureReason::ContractRejected,
                ));
            }
            Err(error @ BackendRuntimeFilterUnaryError::Transport(_)) => {
                match state.transport_failed(identity, Instant::now()) {
                    ReliableTransportFailureOutcome::RetryScheduled => {
                        tokio::select! {
                            biased;
                            _ = shutdown_notify.notified() => return None,
                            _ = tokio::time::sleep(policy.retry_interval()) => {}
                        }
                        let tick = state.drive(Instant::now());
                        if let Some((_, retried)) = tick.retried().first() {
                            tokio::select! {
                                biased;
                                _ = shutdown_notify.notified() => return None,
                                sent = completions.send(
                                    BackendRuntimeFilterSinkCompletion::Retried(identity)
                                ) => {
                                    if sent.is_err() {
                                        return None;
                                    }
                                }
                            }
                            frame = Arc::clone(retried);
                            continue;
                        }
                        let reason = tick
                            .failed_open()
                            .first()
                            .map(|(_, reason)| *reason)
                            .unwrap_or(ReliableTransportFailOpenReason::Deadline);
                        return Some(fail_open_completion(&mut state, identity, reason));
                    }
                    ReliableTransportFailureOutcome::FailedOpen(_, reason) => {
                        return Some(BackendRuntimeFilterSinkCompletion::TransportFailure(
                            identity,
                            error,
                            failure_reason(reason),
                        ));
                    }
                    ReliableTransportFailureOutcome::Unknown => {
                        return Some(BackendRuntimeFilterSinkCompletion::TransportFailure(
                            identity,
                            error,
                            BackendRuntimeFilterTransportFailureReason::ContractRejected,
                        ));
                    }
                }
            }
        }
    }
}

fn fail_open_completion(
    state: &mut ReliableTransportState<
        BackendNativeRouteIdentity,
        Arc<BackendNativeRuntimeFilterEnvelope>,
        BackendRuntimeFilterRetryPolicy,
    >,
    identity: BackendNativeRouteIdentity,
    reason: ReliableTransportFailOpenReason,
) -> BackendRuntimeFilterSinkCompletion {
    let _ = state.transport_failed(identity, Instant::now());
    // Failing open is correct -- the join is the authority and the result stays
    // correct -- which is exactly why it must be said out loud: the query
    // succeeds either way, and the only other trace is a consumer that waited
    // out its cap.
    warn!(
        target: "novarocks::runtime_filter",
        route = ?identity,
        reason = ?reason,
        "runtime filter envelope failed open after exhausting its transport budget"
    );
    BackendRuntimeFilterSinkCompletion::TransportFailure(
        identity,
        BackendRuntimeFilterUnaryError::transport("runtime filter retry budget exhausted"),
        failure_reason(reason),
    )
}

const fn failure_reason(
    reason: ReliableTransportFailOpenReason,
) -> BackendRuntimeFilterTransportFailureReason {
    match reason {
        ReliableTransportFailOpenReason::Deadline => {
            BackendRuntimeFilterTransportFailureReason::Deadline
        }
        ReliableTransportFailOpenReason::AttemptsExhausted => {
            BackendRuntimeFilterTransportFailureReason::AttemptsExhausted
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_proto_models::filter::{
        RuntimeFilterAcceptStatus, RuntimeFilterContributionRouteIdentity,
        RuntimeFilterEnvelopeResponse, RuntimeFilterRouteIdentity,
        runtime_filter_route_identity::Value,
    };
    use novarocks_worker::runtime_filter::domain::BackendAcceptStatus;
    use std::future::Future;
    use std::sync::atomic::AtomicUsize;
    use std::task::{Context, Poll, Waker};

    fn contribution_route() -> RuntimeFilterRouteIdentity {
        RuntimeFilterRouteIdentity {
            value: Some(Value::Contribution(
                RuntimeFilterContributionRouteIdentity {
                    producer_binding_id: 17,
                    task_key: Some(novarocks_proto_models::common::UniqueId { hi: 18, lo: 19 }),
                    partition_id: 0,
                    sequence: 0,
                },
            )),
        }
    }

    #[test]
    fn unary_ack_decode_retains_native_route_and_strict_status() {
        let ack = decode_runtime_filter_unary_ack(RuntimeFilterEnvelopeResponse {
            acked_route_identity: Some(contribution_route()),
            accept_status: RuntimeFilterAcceptStatus::Duplicate as i32,
            rejection_reason: String::new(),
        })
        .unwrap();
        assert_eq!(ack.status(), BackendAcceptStatus::Duplicate);
        assert!(ack.identity().as_contribution().is_some());
    }

    #[test]
    fn unary_ack_decode_rejects_success_with_rejection_reason() {
        let error = decode_runtime_filter_unary_ack(RuntimeFilterEnvelopeResponse {
            acked_route_identity: Some(contribution_route()),
            accept_status: RuntimeFilterAcceptStatus::Accepted as i32,
            rejection_reason: "unexpected".to_string(),
        })
        .unwrap_err();
        assert!(matches!(
            error,
            super::BackendRuntimeFilterUnaryError::Contract(_)
        ));
    }

    #[test]
    fn shutdown_retires_the_owned_runtime_filter_worker_after_actual_exit() {
        let runtime = test_backend_data_runtime();
        let mut owner = novarocks_worker::ParticipationWorkTestOwner::new();
        let sink =
            GrpcRuntimeFilterEnvelopeSink::new(runtime.clone(), owner.runtime_filter_sender());
        assert!(
            sink.worker
                .lock()
                .expect("runtime filter worker lock")
                .is_some()
        );
        assert!(
            !owner
                .poll_finish()
                .expect("the live sender remains supervised")
        );

        sink.shutdown();

        assert!(
            sink.worker
                .lock()
                .expect("runtime filter worker lock")
                .is_none()
        );
        drop(sink);
        runtime.block_on(owner.finish());
    }

    struct ScriptedClient {
        fail_transport: bool,
        calls: AtomicUsize,
    }
    #[async_trait::async_trait]
    impl BackendRuntimeFilterEnvelopeUnaryClient for ScriptedClient {
        async fn transmit(
            &self,
            _: BackendRemoteRoute,
            envelope: Arc<BackendNativeRuntimeFilterEnvelope>,
            _: Duration,
        ) -> Result<BackendRuntimeFilterUnaryAck, BackendRuntimeFilterUnaryError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fail_transport {
                Err(BackendRuntimeFilterUnaryError::transport("scripted retry"))
            } else {
                Ok(BackendRuntimeFilterUnaryAck::new(
                    *envelope.route_identity(),
                    BackendAcceptStatus::Accepted,
                ))
            }
        }
    }

    fn request() -> SinkRequest {
        use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
        use novarocks_worker::runtime_filter::domain::{
            BackendEnvelopeKind, BackendProducerOpenMetadata, BackendRouteEdgeId,
            BackendRouteEndpoint, BackendRoutePeer, BackendRouteRole, BackendRoutingChannel,
            BackendRoutingEdge, BackendRoutingShard, BackendTransportSequence,
        };
        let fixture =
            novarocks_worker::runtime_filter::fixture::BackendRuntimeFilterFixture::membership();
        let producer = fixture.producer_contract();
        let task = novarocks_types::UniqueId::new(18, 19);
        let source = BackendRouteRole::Producer(producer.binding_id());
        let edge = BackendRoutingEdge::new(
            BackendRouteEdgeId::new(1),
            BackendRouteEndpoint::new(1, source).unwrap(),
            BackendRouteEndpoint::new(2, BackendRouteRole::Aggregator).unwrap(),
            BackendRoutePeer::Remote {
                backend_process_id: novarocks_types::BackendProcessId::new_v7(),
                participant_id: 2,
                endpoint: RuntimeEndpoint::new("127.0.0.1", 9072).unwrap(),
            },
            [BackendEnvelopeKind::Contribution],
        )
        .unwrap();
        let routing = BackendRoutingShard::new(
            fixture.identity(),
            1,
            [BackendRoutingChannel::new(
                producer.channel_id(),
                [source],
                [],
                [edge],
                [((producer.binding_id(), task), 1)],
            )
            .unwrap()],
        )
        .unwrap();
        let decision = routing
            .route_producer(
                producer.channel_id(),
                producer.binding_id(),
                BackendEnvelopeKind::Contribution,
            )
            .unwrap();
        let contribution = fixture.membership_contribution();
        let envelope = BackendNativeRuntimeFilterEnvelope::new(
            BackendEnvelopeKind::Contribution,
            fixture.identity(),
            producer.channel_id(),
            BackendNativeRouteIdentity::contribution(
                crate::runtime_filter_rpc::BackendNativeContributionRouteIdentity::new(
                    producer.binding_id(),
                    task,
                    novarocks_execution::runtime_filter::PartitionId::new(0),
                    BackendTransportSequence::new(0),
                ),
            ),
            Some(BackendProducerOpenMetadata::try_new(1).unwrap()),
            None,
            contribution.contract_digest(),
            contribution.canonical_bytes().clone(),
        )
        .unwrap();
        SinkRequest {
            route: decision.remote_routes()[0].clone(),
            envelope: BackendNativeRuntimeFilterTransportEnvelope::new(
                Arc::new(envelope),
                BackendRuntimeFilterRetryPolicy::new(
                    Duration::from_millis(1),
                    3,
                    Duration::from_secs(10),
                    1,
                    4096,
                )
                .unwrap(),
            )
            .unwrap(),
        }
    }

    #[test]
    fn stop_exits_a_worker_blocked_on_its_full_completion_channel() {
        full_completion_channel_stops(false);
    }

    #[test]
    fn stop_exits_a_worker_blocked_on_its_full_retry_completion_channel() {
        full_completion_channel_stops(true);
    }

    fn poll_once<F: Future>(future: std::pin::Pin<&mut F>) -> Poll<F::Output> {
        future.poll(&mut Context::from_waker(Waker::noop()))
    }

    fn full_completion_channel_stops(retry: bool) {
        let runtime = test_backend_data_runtime();
        let mut owner = novarocks_worker::ParticipationWorkTestOwner::new();
        let worker = runtime.spawn_with_obligation(owner.runtime_filter_sender(), async move {
            let request = request();
            let identity = *request.envelope.envelope.route_identity();
            let (requests, receiver) = mpsc::channel(1);
            requests
                .try_send(request)
                .unwrap_or_else(|_| panic!("one paid request fits"));
            let (completions, _receiver) = mpsc::channel(1);
            completions
                .try_send(BackendRuntimeFilterSinkCompletion::Retried(identity))
                .unwrap();
            let client = Arc::new(ScriptedClient {
                fail_transport: retry,
                calls: AtomicUsize::new(0),
            });
            let shutdown = Arc::new(AtomicBool::new(false));
            let notify = Arc::new(Notify::new());
            let mut future = Box::pin(run_worker(
                receiver,
                completions,
                client.clone(),
                shutdown.clone(),
                notify.clone(),
            ));
            assert!(poll_once(future.as_mut()).is_pending());
            if retry {
                // The first poll armed the retry timer. Once it is due, this
                // poll reaches the full retry-completion send, not the timer.
                tokio::time::sleep(Duration::from_millis(5)).await;
                assert!(poll_once(future.as_mut()).is_pending());
            }
            assert_eq!(client.calls.load(Ordering::SeqCst), 1);
            shutdown.store(true, Ordering::Release);
            notify.notify_one();
            assert_eq!(poll_once(future.as_mut()), Poll::Ready(()));
            drop(future);
        });
        runtime.block_on(async {
            worker
                .await
                .expect("the sender task does not panic")
                .expect("the actual sender exits");
            owner.finish().await;
        });
    }
}
