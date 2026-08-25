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

//! Backend gRPC sender for runtime-filter envelopes.
//!
//! The sender owns only bounded unary delivery. Route authority belongs to the
//! Backend participant domain and canonical contribution/artifact semantics
//! remain outside this module.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::{Notify, mpsc};

use prost::Message;

use novarocks_proto::filter::RuntimeFilterEnvelopeResponse;

use crate::BackendDataRuntime;
use crate::rpc::client::BackendRpcClient;
use crate::runtime_filter::domain::{BackendAcceptStatus, BackendRemoteRoute};
use crate::runtime_filter::reliable_transport::{
    ReliableTransportFailOpenReason, ReliableTransportFailureOutcome, ReliableTransportPolicy,
    ReliableTransportSendOutcome, ReliableTransportState,
};
use crate::runtime_filter::rpc::{
    BackendNativeRouteIdentity, BackendNativeRuntimeFilterEnvelope,
    decode_runtime_filter_envelope_response, encode_runtime_filter_envelope,
};

const LIVE_REQUEST_CAPACITY: usize = 1024;
const LIVE_COMPLETION_CAPACITY: usize = 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BackendRuntimeFilterUnaryAck {
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
pub(crate) enum BackendRuntimeFilterUnaryError {
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
pub(crate) struct BackendRuntimeFilterRetryPolicy {
    retry_interval: Duration,
    max_attempts: u32,
    deadline: Duration,
    max_pending_entries: usize,
    max_pending_bytes: usize,
}

impl BackendRuntimeFilterRetryPolicy {
    pub(crate) fn new(
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
pub(crate) struct BackendNativeRuntimeFilterTransportEnvelope {
    envelope: Arc<BackendNativeRuntimeFilterEnvelope>,
    policy: BackendRuntimeFilterRetryPolicy,
}

impl BackendNativeRuntimeFilterTransportEnvelope {
    pub(crate) fn new(
        envelope: Arc<BackendNativeRuntimeFilterEnvelope>,
        policy: BackendRuntimeFilterRetryPolicy,
    ) -> Result<Self, BackendRuntimeFilterUnaryError> {
        Ok(Self { envelope, policy })
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        Arc<BackendNativeRuntimeFilterEnvelope>,
        BackendRuntimeFilterRetryPolicy,
    ) {
        (self.envelope, self.policy)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum BackendRuntimeFilterSinkSubmitOutcome {
    Submitted,
    QueueFull,
    Shutdown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum BackendRuntimeFilterSinkCompletion {
    Ack(BackendNativeRouteIdentity, BackendAcceptStatus),
    Retried(BackendNativeRouteIdentity),
    TransportFailure(
        BackendNativeRouteIdentity,
        BackendRuntimeFilterUnaryError,
        BackendRuntimeFilterTransportFailureReason,
    ),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BackendRuntimeFilterTransportFailureReason {
    Deadline,
    AttemptsExhausted,
    ContractRejected,
}

/// Backend-native sink contract. It deliberately does not expose the old Core
/// router transport types; the Backend reliable transport integrates through
/// this port once it owns the physical route session.
pub(crate) trait BackendRuntimeFilterEnvelopeSink: Send + Sync {
    fn try_send(
        &self,
        route: BackendRemoteRoute,
        envelope: BackendNativeRuntimeFilterTransportEnvelope,
    ) -> BackendRuntimeFilterSinkSubmitOutcome;

    fn try_recv_completion(&self) -> Option<BackendRuntimeFilterSinkCompletion>;

    fn shutdown(&self);
}

#[async_trait::async_trait]
pub(crate) trait BackendRuntimeFilterEnvelopeUnaryClient: Send + Sync + 'static {
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
        let client = BackendRpcClient::new_runtime_endpoint(self.runtime.clone(), route.endpoint())
            .map_err(BackendRuntimeFilterUnaryError::transport)?;
        let response = client
            .transmit_runtime_filter_envelope_async(
                encode_runtime_filter_envelope(envelope.as_ref()),
                deadline,
            )
            .await
            .map_err(BackendRuntimeFilterUnaryError::transport)?;
        decode_runtime_filter_unary_ack(response)
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

pub(crate) struct GrpcRuntimeFilterEnvelopeSink {
    requests: mpsc::Sender<SinkRequest>,
    completions: Mutex<mpsc::Receiver<BackendRuntimeFilterSinkCompletion>>,
    shutdown: Arc<AtomicBool>,
    shutdown_notify: Arc<Notify>,
    worker: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

#[allow(
    dead_code,
    reason = "Retained for target-specific native integration and regression coverage."
)]
impl GrpcRuntimeFilterEnvelopeSink {
    pub(crate) fn new(runtime: BackendDataRuntime) -> Arc<Self> {
        Self::new_with_client_and_capacities(
            runtime.clone(),
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
    ) -> Result<Arc<Self>, String> {
        if request_capacity == 0 || completion_capacity == 0 {
            return Err("runtime filter sink capacities must be nonzero".to_string());
        }
        Ok(Self::new_with_client_and_capacities(
            crate::rpc::runtime::test_backend_data_runtime(),
            client,
            request_capacity,
            completion_capacity,
        ))
    }

    fn new_with_client_and_capacities(
        runtime: BackendDataRuntime,
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
        let worker = runtime.handle().spawn(run_worker(
            request_rx,
            completion_tx,
            client,
            shutdown,
            shutdown_notify,
        ));
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
    let retained_bytes = encode_runtime_filter_envelope(envelope.as_ref()).encoded_len();
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
                            if completions
                                .send(BackendRuntimeFilterSinkCompletion::Retried(identity))
                                .await
                                .is_err()
                            {
                                return None;
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
    use super::{
        BackendRuntimeFilterEnvelopeSink, GrpcRuntimeFilterEnvelopeSink,
        decode_runtime_filter_unary_ack,
    };
    use crate::rpc::runtime::test_backend_data_runtime;
    use crate::runtime_filter::domain::BackendAcceptStatus;
    use novarocks_proto::filter::{
        RuntimeFilterAcceptStatus, RuntimeFilterContributionRouteIdentity,
        RuntimeFilterEnvelopeResponse, RuntimeFilterRouteIdentity,
        runtime_filter_route_identity::Value,
    };

    fn contribution_route() -> RuntimeFilterRouteIdentity {
        RuntimeFilterRouteIdentity {
            value: Some(Value::Contribution(
                RuntimeFilterContributionRouteIdentity {
                    producer_binding_id: 17,
                    fragment_instance_id: Some(novarocks_proto::common::UniqueId {
                        hi: 18,
                        lo: 19,
                    }),
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
    fn shutdown_aborts_the_owned_runtime_filter_worker() {
        let sink = GrpcRuntimeFilterEnvelopeSink::new(test_backend_data_runtime());
        assert!(
            sink.worker
                .lock()
                .expect("runtime filter worker lock")
                .is_some()
        );

        sink.shutdown();

        assert!(
            sink.worker
                .lock()
                .expect("runtime filter worker lock")
                .is_none()
        );
    }
}
