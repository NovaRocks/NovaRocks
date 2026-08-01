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
use std::net::{SocketAddr, TcpListener};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, mpsc};
use std::task::{Context, Poll};
use std::thread::JoinHandle;

use axum::Router;
use axum::http::{HeaderValue, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use novarocks_types::UniqueId;
use tokio::net::TcpListener as TokioTcpListener;
use tokio::sync::watch;
use tokio_stream::wrappers::ReceiverStream;
use tonic::body::boxed;
use tonic::codegen::Service;
use tonic::server::NamedService;
#[cfg(any(test, feature = "query-execution-contract-test-support"))]
use tonic::service::Routes;

#[cfg(any(test, feature = "query-execution-contract-test-support"))]
use crate::common::config::http_port;
use crate::common::types::format_uuid;
use crate::novarocks_logging::{error, info};
#[cfg(any(test, feature = "query-execution-contract-test-support"))]
use crate::query_execution::lifecycle::QueryLifecycleIngress;
use crate::query_execution::lifecycle::{
    QueryTerminalIngress, QueryTerminalReportOutcome, decode_query_terminal_snapshot,
};
use crate::runtime_filter::port::transport::RuntimeFilterEnvelopeIngress;
#[cfg(any(test, feature = "query-execution-contract-test-support"))]
use crate::service::grpc_query_lifecycle_adapter::{
    handle_abort_query, handle_init_query, handle_query_control_stream, handle_stage_fragments,
    handle_start_prepared_query,
};
use crate::service::grpc_runtime_filter_adapter::handle_runtime_filter_envelope;
use crate::service::internal_rpc;
use crate::service::metrics_http;
#[cfg(any(test, feature = "query-execution-contract-test-support"))]
use crate::service::native_fragment_ingress::{NativeFragmentCancelRequest, NativeFragmentIngress};
use crate::service::runtime_filter_envelope_ingress::query_scoped_runtime_filter_envelope_ingress;

pub use crate::proto;

const GRPC_MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;
static FETCH_RESULT_CALLS: AtomicUsize = AtomicUsize::new(0);
#[cfg(test)]
static PAUSE_STANDALONE_GRPC_STARTUP_AFTER_RESERVATION: AtomicBool = AtomicBool::new(false);
#[cfg(test)]
static STANDALONE_GRPC_STARTUP_RESERVATIONS: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
struct RejectingTestNativeFragmentIngress;

#[cfg(test)]
struct RejectingTestQueryLifecycleIngress;

#[cfg(test)]
pub(crate) fn rejecting_test_native_fragment_ingress() -> Arc<dyn NativeFragmentIngress> {
    Arc::new(RejectingTestNativeFragmentIngress)
}

#[cfg(test)]
pub(crate) fn rejecting_test_query_lifecycle_ingress() -> Arc<dyn QueryLifecycleIngress> {
    Arc::new(RejectingTestQueryLifecycleIngress)
}

#[cfg(test)]
impl QueryLifecycleIngress for RejectingTestQueryLifecycleIngress {
    fn bind_backend_identity(
        &self,
        _backend_id: u64,
    ) -> Result<(), crate::query_execution::lifecycle::QueryLifecycleError> {
        Ok(())
    }

    fn init_query(
        &self,
        request: crate::query_execution::lifecycle::QueryInitRequest,
    ) -> crate::query_execution::lifecycle::QueryInitAck {
        crate::query_execution::lifecycle::QueryInitAck::new(
            request.manifest().execution_id(),
            request.digest(),
            crate::query_execution::lifecycle::QueryInitOutcome::RejectedInvalidManifest,
        )
    }

    fn abort_query(
        &self,
        request: crate::query_execution::lifecycle::QueryAbortRequest,
    ) -> Result<
        crate::query_execution::lifecycle::QueryTerminationAck,
        crate::query_execution::lifecycle::QueryLifecycleError,
    > {
        Ok(crate::query_execution::lifecycle::QueryTerminationAck::new(
            request.execution_id(),
            crate::query_execution::lifecycle::QueryTerminationReason::CoordinatorAbort,
        ))
    }

    fn attach_control(
        &self,
        _attach: crate::query_execution::lifecycle::QueryControlAttach,
    ) -> Result<
        crate::query_execution::lifecycle::QueryControlAttachment,
        crate::query_execution::lifecycle::QueryLifecycleError,
    > {
        Err(crate::query_execution::lifecycle::QueryLifecycleError::new(
            crate::query_execution::lifecycle::QueryLifecycleErrorCode::Terminated,
            "test query lifecycle ingress rejects attach",
        ))
    }
}

#[cfg(test)]
impl NativeFragmentIngress for RejectingTestNativeFragmentIngress {
    fn cancel(
        &self,
        _request: NativeFragmentCancelRequest,
    ) -> Result<(), crate::service::native_fragment_ingress::NativeFragmentIngressError> {
        Ok(())
    }
}

fn status_from_lifecycle_error(
    error: crate::query_execution::lifecycle::QueryLifecycleError,
) -> tonic::Status {
    use crate::query_execution::lifecycle::QueryLifecycleErrorCode;

    let detail = error.detail().to_string();
    match error.code() {
        QueryLifecycleErrorCode::InvalidManifest => tonic::Status::invalid_argument(detail),
        QueryLifecycleErrorCode::Conflict => tonic::Status::already_exists(detail),
        QueryLifecycleErrorCode::StaleBackend | QueryLifecycleErrorCode::Terminated => {
            tonic::Status::failed_precondition(detail)
        }
        QueryLifecycleErrorCode::Capacity => tonic::Status::resource_exhausted(detail),
        QueryLifecycleErrorCode::Transport => tonic::Status::unavailable(detail),
        QueryLifecycleErrorCode::Internal => tonic::Status::internal(detail),
    }
}

#[derive(Default)]
struct GrpcServerState {
    started: bool,
    starting: bool,
    bound_port: Option<u16>,
    shutdown_tx: Option<watch::Sender<bool>>,
    join_handle: Option<JoinHandle<()>>,
    stop_requested: Option<Arc<AtomicBool>>,
    failure_rx: Option<mpsc::Receiver<String>>,
}

fn grpc_server_state() -> &'static Mutex<GrpcServerState> {
    static STATE: OnceLock<Mutex<GrpcServerState>> = OnceLock::new();
    STATE.get_or_init(|| Mutex::new(GrpcServerState::default()))
}

#[cfg(test)]
fn pause_standalone_grpc_startup_after_reservation() {
    if !PAUSE_STANDALONE_GRPC_STARTUP_AFTER_RESERVATION.load(Ordering::Acquire) {
        return;
    }
    STANDALONE_GRPC_STARTUP_RESERVATIONS.fetch_add(1, Ordering::AcqRel);
    while PAUSE_STANDALONE_GRPC_STARTUP_AFTER_RESERVATION.load(Ordering::Acquire) {
        std::thread::yield_now();
    }
}

#[derive(Clone)]
pub struct GrpcService {
    allow_local_execution: bool,
    #[cfg(any(test, feature = "query-execution-contract-test-support"))]
    native_fragment_ingress: Option<Arc<dyn NativeFragmentIngress>>,
    #[cfg(any(test, feature = "query-execution-contract-test-support"))]
    query_lifecycle_ingress: Option<Arc<dyn QueryLifecycleIngress>>,
    #[cfg(any(test, feature = "query-execution-contract-test-support"))]
    query_control_shutdown: Option<watch::Receiver<bool>>,
    terminal_ingress: Option<Arc<dyn QueryTerminalIngress>>,
    runtime_filter_envelope_ingress: Arc<dyn RuntimeFilterEnvelopeIngress>,
}

impl std::fmt::Debug for GrpcService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcService")
            .field("allow_local_execution", &self.allow_local_execution)
            .finish_non_exhaustive()
    }
}

impl GrpcService {
    #[cfg(any(test, feature = "query-execution-contract-test-support"))]
    pub fn with_fragment_execution(
        native_fragment_ingress: Arc<dyn NativeFragmentIngress>,
        query_lifecycle_ingress: Arc<dyn QueryLifecycleIngress>,
    ) -> Self {
        Self::with_handlers(
            true,
            Some(native_fragment_ingress),
            Some(query_lifecycle_ingress),
            query_scoped_runtime_filter_envelope_ingress(),
        )
    }

    /// Builds the neutral internal-RPC handler used by service hosts that
    /// execute fragments through an out-of-band ingress (for example BRPC).
    /// It owns no listener, HTTP router, Starlet adapter, or application state.
    pub fn internal_execution_without_native_fragment_ingress() -> Self {
        Self {
            allow_local_execution: true,
            #[cfg(any(test, feature = "query-execution-contract-test-support"))]
            native_fragment_ingress: None,
            #[cfg(any(test, feature = "query-execution-contract-test-support"))]
            query_lifecycle_ingress: None,
            #[cfg(any(test, feature = "query-execution-contract-test-support"))]
            query_control_shutdown: None,
            terminal_ingress: None,
            runtime_filter_envelope_ingress: query_scoped_runtime_filter_envelope_ingress(),
        }
    }

    pub fn terminal_ingress_only() -> Self {
        Self {
            allow_local_execution: false,
            #[cfg(any(test, feature = "query-execution-contract-test-support"))]
            native_fragment_ingress: None,
            #[cfg(any(test, feature = "query-execution-contract-test-support"))]
            query_lifecycle_ingress: None,
            #[cfg(any(test, feature = "query-execution-contract-test-support"))]
            query_control_shutdown: None,
            terminal_ingress: None,
            runtime_filter_envelope_ingress: query_scoped_runtime_filter_envelope_ingress(),
        }
    }

    #[cfg(any(test, feature = "query-execution-contract-test-support"))]
    fn with_handlers(
        allow_local_execution: bool,
        native_fragment_ingress: Option<Arc<dyn NativeFragmentIngress>>,
        query_lifecycle_ingress: Option<Arc<dyn QueryLifecycleIngress>>,
        runtime_filter_envelope_ingress: Arc<dyn RuntimeFilterEnvelopeIngress>,
    ) -> Self {
        Self {
            allow_local_execution,
            native_fragment_ingress,
            query_lifecycle_ingress,
            query_control_shutdown: None,
            terminal_ingress: None,
            runtime_filter_envelope_ingress,
        }
    }

    #[cfg(test)]
    pub(crate) fn full_execution_with_handlers(
        runtime_filter_envelope_ingress: Arc<dyn RuntimeFilterEnvelopeIngress>,
    ) -> Self {
        Self::with_handlers(
            true,
            Some(Arc::new(RejectingTestNativeFragmentIngress)),
            None,
            runtime_filter_envelope_ingress,
        )
    }

    #[cfg(test)]
    pub(crate) fn full_execution_with_runtime_filter_manager(
        manager: Arc<crate::runtime::query_context::QueryContextManager>,
    ) -> Self {
        let mut service = Self::with_handlers(
            true,
            Some(Arc::new(RejectingTestNativeFragmentIngress)),
            None,
            crate::service::runtime_filter_envelope_ingress::query_scoped_runtime_filter_envelope_ingress_with_manager(
                manager.clone(),
            ),
        );
        service
    }

    fn require_local_execution(&self, rpc_name: &str) -> Result<(), tonic::Status> {
        if self.allow_local_execution {
            Ok(())
        } else {
            Err(tonic::Status::failed_precondition(format!(
                "report-only NovaRocksGrpc endpoint rejects local execution RPC: {rpc_name}"
            )))
        }
    }

    #[cfg(any(test, feature = "query-execution-contract-test-support"))]
    fn require_query_lifecycle(
        &self,
        rpc_name: &str,
    ) -> Result<Arc<dyn QueryLifecycleIngress>, tonic::Status> {
        self.require_local_execution(rpc_name)?;
        self.query_lifecycle_ingress.clone().ok_or_else(|| {
            tonic::Status::failed_precondition("query lifecycle ingress is not configured")
        })
    }

    #[cfg(any(test, feature = "query-execution-contract-test-support"))]
    fn with_query_control_shutdown(mut self, shutdown: watch::Receiver<bool>) -> Self {
        self.query_control_shutdown = Some(shutdown);
        self
    }

    pub fn with_terminal_ingress(mut self, ingress: Arc<dyn QueryTerminalIngress>) -> Self {
        self.terminal_ingress = Some(ingress);
        self
    }
}

#[cfg(test)]
pub(crate) struct IndependentGrpcRuntimeFilterNode {
    endpoint: SocketAddr,
    resources: IndependentGrpcRuntimeFilterResources,
}

#[cfg(test)]
struct IndependentGrpcRuntimeFilterResources {
    manager: Arc<crate::runtime::query_context::QueryContextManager>,
    shutdown_tx: Option<watch::Sender<bool>>,
    server_handle: Option<JoinHandle<()>>,
    clean_handle: Option<JoinHandle<()>>,
}

#[cfg(test)]
impl IndependentGrpcRuntimeFilterResources {
    fn shutdown(&mut self, wait: std::time::Duration) -> Result<(), String> {
        self.manager.stop_clean_loop_for_test();
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(true);
        }
        let failures = join_independent_runtime_filter_threads(
            self.server_handle.take(),
            self.clean_handle.take(),
            wait,
        );
        if failures.is_empty() {
            Ok(())
        } else {
            Err(failures.join("; "))
        }
    }
}

#[cfg(test)]
impl Drop for IndependentGrpcRuntimeFilterResources {
    fn drop(&mut self) {
        let _ = self.shutdown(IndependentGrpcRuntimeFilterNode::WAIT);
    }
}

#[cfg(test)]
struct IndependentGrpcRuntimeFilterStartupGuard {
    resources: Option<IndependentGrpcRuntimeFilterResources>,
}

#[cfg(test)]
impl IndependentGrpcRuntimeFilterStartupGuard {
    fn new(resources: IndependentGrpcRuntimeFilterResources) -> Self {
        Self {
            resources: Some(resources),
        }
    }

    fn resources(&self) -> &IndependentGrpcRuntimeFilterResources {
        self.resources
            .as_ref()
            .expect("startup resources remain armed until readiness")
    }

    fn resources_mut(&mut self) -> &mut IndependentGrpcRuntimeFilterResources {
        self.resources
            .as_mut()
            .expect("startup resources remain armed until readiness")
    }

    fn disarm(mut self) -> IndependentGrpcRuntimeFilterResources {
        self.resources
            .take()
            .expect("successful startup transfers its resources to the live node")
    }
}

#[cfg(test)]
impl Drop for IndependentGrpcRuntimeFilterStartupGuard {
    fn drop(&mut self) {
        if let Some(resources) = &mut self.resources {
            let _ = resources.shutdown(IndependentGrpcRuntimeFilterNode::WAIT);
        }
    }
}

#[cfg(test)]
struct IndependentGrpcStartupProbe {
    manager: mpsc::SyncSender<std::sync::Weak<crate::runtime::query_context::QueryContextManager>>,
    server_exited: mpsc::SyncSender<()>,
    clean_exited: mpsc::SyncSender<()>,
    panic_before_ready: bool,
}

#[cfg(test)]
struct IndependentGrpcServerExitSignal(Option<mpsc::SyncSender<()>>);

#[cfg(test)]
impl Drop for IndependentGrpcServerExitSignal {
    fn drop(&mut self) {
        if let Some(exited) = self.0.take() {
            let _ = exited.send(());
        }
    }
}

#[cfg(test)]
impl IndependentGrpcRuntimeFilterNode {
    const WAIT: std::time::Duration = std::time::Duration::from_secs(5);

    pub(crate) fn start() -> Result<Self, String> {
        Self::start_with_probe(None)
    }

    fn start_with_probe(probe: Option<IndependentGrpcStartupProbe>) -> Result<Self, String> {
        let listener = bind_tcp_listener("127.0.0.1", 0, "independent runtime-filter gRPC")?;
        let endpoint = listener
            .local_addr()
            .map_err(|error| format!("read independent runtime-filter address failed: {error}"))?;
        let (manager, clean_handle) = if let Some(probe) = &probe {
            crate::runtime::query_context::QueryContextManager::new_for_live_test_with_exit_signal(
                Some(probe.clean_exited.clone()),
            )
        } else {
            crate::runtime::query_context::QueryContextManager::new_for_live_test()
        };
        if let Some(probe) = &probe {
            let _ = probe.manager.send(Arc::downgrade(&manager));
        }
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let resources = IndependentGrpcRuntimeFilterResources {
            manager,
            shutdown_tx: Some(shutdown_tx),
            server_handle: None,
            clean_handle: Some(clean_handle),
        };
        let mut startup = IndependentGrpcRuntimeFilterStartupGuard::new(resources);
        let service_manager = Arc::clone(&startup.resources().manager);
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let server_handle = std::thread::spawn(move || {
            let _exit = IndependentGrpcServerExitSignal(
                probe.as_ref().map(|probe| probe.server_exited.clone()),
            );
            if probe.as_ref().is_some_and(|probe| probe.panic_before_ready) {
                panic!("injected independent runtime-filter pre-ready server panic");
            }
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .worker_threads(2)
                .thread_stack_size(crate::runtime::global_async_runtime::WORKER_STACK_SIZE_BYTES)
                .build()
                .expect("build independent runtime-filter gRPC runtime");
            runtime.block_on(async move {
                let listener = TokioTcpListener::from_std(listener)
                    .expect("create independent runtime-filter Tokio listener");
                let service =
                    GrpcService::full_execution_with_runtime_filter_manager(service_manager);
                let service =
                    proto::novarocks::nova_rocks_grpc_server::NovaRocksGrpcServer::new(service)
                        .max_decoding_message_size(GRPC_MAX_MESSAGE_BYTES)
                        .max_encoding_message_size(GRPC_MAX_MESSAGE_BYTES);
                let app = build_novarocks_http_app(Routes::new(service));
                let server = axum::serve(listener, app).with_graceful_shutdown(async move {
                    while !*shutdown_rx.borrow() {
                        if shutdown_rx.changed().await.is_err() {
                            break;
                        }
                    }
                });
                let _ = ready_tx.send(());
                server
                    .await
                    .expect("independent runtime-filter gRPC server failed");
            });
        });
        startup.resources_mut().server_handle = Some(server_handle);
        ready_rx.recv_timeout(Self::WAIT).map_err(|error| {
            format!("independent runtime-filter gRPC server did not become ready: {error}")
        })?;
        Ok(Self {
            endpoint,
            resources: startup.disarm(),
        })
    }

    pub(crate) const fn endpoint(&self) -> SocketAddr {
        self.endpoint
    }

    pub(crate) fn manager(&self) -> &Arc<crate::runtime::query_context::QueryContextManager> {
        &self.resources.manager
    }

    pub(crate) fn shutdown(&mut self) -> Result<(), String> {
        self.resources.shutdown(Self::WAIT)
    }
}

#[cfg(test)]
impl Drop for IndependentGrpcRuntimeFilterNode {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

#[cfg(test)]
fn join_independent_runtime_filter_thread(
    handle: JoinHandle<()>,
    label: &'static str,
    timeout: std::time::Duration,
) -> Result<(), String> {
    let (joined_tx, joined_rx) = mpsc::sync_channel(1);
    std::thread::spawn(move || {
        let _ = joined_tx.send(handle.join());
    });
    match joined_rx.recv_timeout(timeout) {
        Ok(Ok(())) => Ok(()),
        Ok(Err(payload)) => {
            let detail = payload
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| {
                    payload
                        .downcast_ref::<&str>()
                        .map(|value| (*value).to_string())
                })
                .unwrap_or_else(|| "unknown panic payload".to_string());
            Err(format!(
                "independent runtime-filter {label} panicked: {detail}"
            ))
        }
        Err(error) => Err(format!(
            "independent runtime-filter {label} did not stop within {timeout:?}: {error}"
        )),
    }
}

#[cfg(test)]
fn join_independent_runtime_filter_threads(
    server_handle: Option<JoinHandle<()>>,
    clean_handle: Option<JoinHandle<()>>,
    wait: std::time::Duration,
) -> Vec<String> {
    let deadline_at = std::time::Instant::now() + wait;
    let mut failures = Vec::new();
    if let Some(handle) = server_handle
        && let Err(error) = join_independent_runtime_filter_thread(
            handle,
            "gRPC server",
            deadline_at.saturating_duration_since(std::time::Instant::now()),
        )
    {
        failures.push(error);
    }
    if let Some(handle) = clean_handle
        && let Err(error) = join_independent_runtime_filter_thread(
            handle,
            "manager clean loop",
            deadline_at.saturating_duration_since(std::time::Instant::now()),
        )
    {
        failures.push(error);
    }
    failures
}

#[tonic::async_trait]
impl proto::novarocks::nova_rocks_grpc_server::NovaRocksGrpc for GrpcService {
    type ExchangeStream = std::pin::Pin<
        Box<
            dyn tokio_stream::Stream<
                    Item = Result<proto::novarocks::ExchangeResponse, tonic::Status>,
                > + Send
                + 'static,
        >,
    >;
    type QueryControlStreamStream = std::pin::Pin<
        Box<
            dyn tokio_stream::Stream<
                    Item = Result<proto::novarocks::QueryControlResponse, tonic::Status>,
                > + Send
                + 'static,
        >,
    >;

    async fn exchange(
        &self,
        request: tonic::Request<tonic::Streaming<proto::novarocks::ExchangeRequest>>,
    ) -> Result<tonic::Response<Self::ExchangeStream>, tonic::Status> {
        use crate::novarocks_logging::debug;

        self.require_local_execution("Exchange")?;
        let mut inbound = request.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel::<
            Result<proto::novarocks::ExchangeResponse, tonic::Status>,
        >(4096);

        tokio::spawn(async move {
            loop {
                let req = match inbound.message().await {
                    Ok(Some(v)) => v,
                    Ok(None) => break,
                    Err(e) => {
                        let _ = tx
                            .send(Err(tonic::Status::internal(format!(
                                "exchange recv failed: {e}"
                            ))))
                            .await;
                        break;
                    }
                };

                let finst_id_hi = req.finst_id_hi;
                let finst_id_lo = req.finst_id_lo;
                let node_id = req.node_id;
                let sender_id = req.sender_id;
                let be_number = req.be_number;
                let eos = req.eos;
                let sequence = req.sequence;
                // handle_transmit_chunk includes Arrow IPC decoding which is CPU-intensive.
                // Offload to the blocking thread pool so async worker threads stay free for I/O.
                let result = match tokio::task::spawn_blocking(move || {
                    internal_rpc::handle_transmit_chunk(req)
                })
                .await
                {
                    Ok(r) => r,
                    Err(e) => {
                        let _ = tx
                            .send(Err(tonic::Status::internal(format!(
                                "exchange handler panicked: {e}"
                            ))))
                            .await;
                        break;
                    }
                };
                let ack = result;
                let handler_failed = ack.status.as_ref().is_some_and(|status| status.code != 0);
                debug!(
                    "exchange ack SEND: finst={} node_id={} sender_id={} be_number={} eos={} seq={}",
                    format_uuid(finst_id_hi, finst_id_lo),
                    node_id,
                    sender_id,
                    be_number,
                    eos,
                    sequence
                );

                if tx.send(Ok(ack)).await.is_err() {
                    break;
                }
                if handler_failed {
                    break;
                }
                debug!(
                    "exchange ack SENT: finst={} node_id={} sender_id={} be_number={} eos={} seq={}",
                    format_uuid(finst_id_hi, finst_id_lo),
                    node_id,
                    sender_id,
                    be_number,
                    eos,
                    sequence
                );
            }
        });

        Ok(tonic::Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn exchange_unary(
        &self,
        request: tonic::Request<proto::novarocks::ExchangeRequest>,
    ) -> Result<tonic::Response<proto::novarocks::ExchangeResponse>, tonic::Status> {
        self.require_local_execution("ExchangeUnary")?;
        let req = request.into_inner();
        let result = tokio::task::spawn_blocking(move || internal_rpc::handle_transmit_chunk(req))
            .await
            .map_err(|e| {
                tonic::Status::internal(format!("exchange_unary handler panicked: {e}"))
            })?;
        Ok(tonic::Response::new(result))
    }

    async fn transmit_runtime_filter_envelope(
        &self,
        request: tonic::Request<proto::filter::RuntimeFilterEnvelope>,
    ) -> Result<tonic::Response<proto::filter::RuntimeFilterEnvelopeResponse>, tonic::Status> {
        self.require_local_execution("TransmitRuntimeFilterEnvelope")?;
        let ingress = self.runtime_filter_envelope_ingress.clone();
        let request = request.into_inner();
        let response =
            tokio::task::spawn_blocking(move || handle_runtime_filter_envelope(ingress, request))
                .await
                .map_err(|error| {
                    tonic::Status::internal(format!(
                        "transmit_runtime_filter_envelope handler panicked: {error}"
                    ))
                })??;
        Ok(tonic::Response::new(response))
    }

    async fn lookup(
        &self,
        request: tonic::Request<proto::filter::LookupRequest>,
    ) -> Result<tonic::Response<proto::filter::LookupResponse>, tonic::Status> {
        self.require_local_execution("Lookup")?;
        Ok(tonic::Response::new(internal_rpc::handle_lookup(
            request.into_inner(),
        )))
    }

    async fn fetch_result(
        &self,
        request: tonic::Request<proto::novarocks::FetchResultRequest>,
    ) -> Result<tonic::Response<proto::novarocks::FetchResultResponse>, tonic::Status> {
        use proto::novarocks::fetch_result_response::Status as FetchStatus;

        self.require_local_execution("FetchResult")?;
        let req = request.into_inner();
        let finst_id = match req.finst_id {
            Some(id) => UniqueId::new(id.hi, id.lo),
            None => {
                return Ok(tonic::Response::new(
                    proto::novarocks::FetchResultResponse {
                        status: FetchStatus::Error as i32,
                        message: "missing finst_id in FetchResultRequest".to_string(),
                        packet_seq: 0,
                        eos: false,
                        result_arrow_ipc: vec![],
                    },
                ));
            }
        };
        let call_index = FETCH_RESULT_CALLS.fetch_add(1, Ordering::SeqCst) + 1;
        if crate::common::config::debug_fault_inject_fetch_not_ready_count()
            .is_some_and(|limit| call_index <= limit)
        {
            return Ok(tonic::Response::new(
                proto::novarocks::FetchResultResponse {
                    status: FetchStatus::NotReady as i32,
                    message: String::new(),
                    packet_seq: 0,
                    eos: false,
                    result_arrow_ipc: vec![],
                },
            ));
        }

        // wait_fetch_typed uses std::sync::Condvar::wait_timeout, which blocks
        // the OS thread for up to max_wait_ms. Offload to the blocking thread
        // pool so tonic worker threads remain free for I/O.
        use crate::runtime::result_buffer::{TryFetchTypedResult, wait_fetch_typed};
        let max_wait_ms = req.max_wait_ms;
        let fetch_result =
            tokio::task::spawn_blocking(move || wait_fetch_typed(finst_id, max_wait_ms))
                .await
                .map_err(|e| {
                    tonic::Status::internal(format!("fetch_result handler panicked: {e}"))
                })?;
        match fetch_result {
            TryFetchTypedResult::Ready(result) => {
                emit_grpc_typed_fetch_marker(FetchStatus::Ready as i32);
                Ok(tonic::Response::new(
                    proto::novarocks::FetchResultResponse {
                        status: FetchStatus::Ready as i32,
                        message: String::new(),
                        packet_seq: result.packet_seq,
                        eos: result.eos,
                        result_arrow_ipc: result.payload,
                    },
                ))
            }
            TryFetchTypedResult::NotReady => {
                emit_grpc_typed_fetch_marker(FetchStatus::NotReady as i32);
                Ok(tonic::Response::new(
                    proto::novarocks::FetchResultResponse {
                        status: FetchStatus::NotReady as i32,
                        message: String::new(),
                        packet_seq: 0,
                        eos: false,
                        result_arrow_ipc: vec![],
                    },
                ))
            }
            TryFetchTypedResult::Error(err) => {
                emit_grpc_typed_fetch_marker(FetchStatus::Error as i32);
                Ok(tonic::Response::new(
                    proto::novarocks::FetchResultResponse {
                        status: FetchStatus::Error as i32,
                        message: err.message,
                        packet_seq: 0,
                        eos: false,
                        result_arrow_ipc: vec![],
                    },
                ))
            }
        }
    }

    async fn ensure_connector_execution_binding(
        &self,
        request: tonic::Request<proto::novarocks::EnsureConnectorExecutionBindingRequest>,
    ) -> Result<
        tonic::Response<proto::novarocks::EnsureConnectorExecutionBindingResponse>,
        tonic::Status,
    > {
        #[cfg(any(test, feature = "query-execution-contract-test-support"))]
        {
            self.require_local_execution("EnsureConnectorExecutionBinding")?;
            let ingress = self.native_fragment_ingress.clone().ok_or_else(|| {
                tonic::Status::failed_precondition("connector binding ingress is not configured")
            })?;
            let request = request.into_inner();
            let result = tokio::task::spawn_blocking(move || {
                let (execution_id, declaration) =
                    crate::service::connector_binding::decode_ensure_request(request)
                        .map_err(|error| error.to_string())?;
                let context = crate::service::connector_binding::install_request_context()
                    .map_err(|error| error.to_string())?;
                ingress
                    .ensure_connector_execution_binding(execution_id, declaration, context)
                    .map_err(|error| error.to_string())
            })
            .await
            .map_err(|error| {
                tonic::Status::internal(format!(
                    "ensure_connector_execution_binding handler panicked: {error}"
                ))
            })?;
            let (status_code, message) = match result {
                Ok(()) => (0, String::new()),
                Err(error) => (1, error),
            };
            Ok(tonic::Response::new(
                proto::novarocks::EnsureConnectorExecutionBindingResponse {
                    status_code,
                    message,
                },
            ))
        }
        #[cfg(not(any(test, feature = "query-execution-contract-test-support")))]
        {
            let _ = request;
            Err(tonic::Status::failed_precondition(
                "core neutral NovaRocksGrpc handler rejects connector binding RPC",
            ))
        }
    }

    async fn retire_connector_execution_binding(
        &self,
        request: tonic::Request<proto::novarocks::RetireConnectorExecutionBindingRequest>,
    ) -> Result<
        tonic::Response<proto::novarocks::RetireConnectorExecutionBindingResponse>,
        tonic::Status,
    > {
        #[cfg(any(test, feature = "query-execution-contract-test-support"))]
        {
            self.require_local_execution("RetireConnectorExecutionBinding")?;
            let ingress = self.native_fragment_ingress.clone().ok_or_else(|| {
                tonic::Status::failed_precondition("connector binding ingress is not configured")
            })?;
            let request = request.into_inner();
            let result = tokio::task::spawn_blocking(move || {
                let key = crate::service::connector_binding::decode_retire_request(request)
                    .map_err(|error| error.to_string())?;
                ingress
                    .retire_connector_execution_binding(key)
                    .map_err(|error| error.to_string())
            })
            .await
            .map_err(|error| {
                tonic::Status::internal(format!(
                    "retire_connector_execution_binding handler panicked: {error}"
                ))
            })?;
            let (status_code, message) = match result {
                Ok(()) => (0, String::new()),
                Err(error) => (1, error),
            };
            Ok(tonic::Response::new(
                proto::novarocks::RetireConnectorExecutionBindingResponse {
                    status_code,
                    message,
                },
            ))
        }
        #[cfg(not(any(test, feature = "query-execution-contract-test-support")))]
        {
            let _ = request;
            Err(tonic::Status::failed_precondition(
                "core neutral NovaRocksGrpc handler rejects connector binding RPC",
            ))
        }
    }

    async fn heartbeat(
        &self,
        request: tonic::Request<proto::novarocks::HeartbeatRequest>,
    ) -> Result<tonic::Response<proto::novarocks::HeartbeatResponse>, tonic::Status> {
        #[cfg(any(test, feature = "query-execution-contract-test-support"))]
        {
            let req = request.into_inner();
            if let Some(ingress) = self.query_lifecycle_ingress.as_ref() {
                ingress
                    .bind_backend_identity(u64::from(req.assigned_be_id))
                    .map_err(status_from_lifecycle_error)?;
                crate::runtime::backend_id::set_backend_id(i64::from(req.assigned_be_id));
            }
            let num_cores = std::thread::available_parallelism()
                .map(|n| n.get() as u32)
                .unwrap_or(1);
            Ok(tonic::Response::new(proto::novarocks::HeartbeatResponse {
                start_epoch: crate::runtime::start_epoch::start_epoch(),
                version: crate::version::short_version().to_string(),
                num_cores,
                status_code: 0,
            }))
        }
        #[cfg(not(any(test, feature = "query-execution-contract-test-support")))]
        {
            let _ = request;
            Err(tonic::Status::failed_precondition(
                "core neutral NovaRocksGrpc handler rejects heartbeat RPC",
            ))
        }
    }

    async fn init_query(
        &self,
        request: tonic::Request<proto::novarocks::InitQueryRequest>,
    ) -> Result<tonic::Response<proto::novarocks::InitQueryResponse>, tonic::Status> {
        #[cfg(any(test, feature = "query-execution-contract-test-support"))]
        {
            let ingress = self.require_query_lifecycle("InitQuery")?;
            let request = request.into_inner();
            let response =
                tokio::task::spawn_blocking(move || handle_init_query(ingress.as_ref(), request))
                    .await
                    .map_err(|error| {
                        tonic::Status::internal(format!("init_query handler panicked: {error}"))
                    })??;
            Ok(tonic::Response::new(response))
        }
        #[cfg(not(any(test, feature = "query-execution-contract-test-support")))]
        {
            let _ = request;
            Err(tonic::Status::failed_precondition(
                "core neutral NovaRocksGrpc handler rejects query lifecycle RPC",
            ))
        }
    }

    async fn stage_fragments(
        &self,
        request: tonic::Request<proto::novarocks::StageFragmentsRequest>,
    ) -> Result<tonic::Response<proto::novarocks::StageFragmentsResponse>, tonic::Status> {
        #[cfg(any(test, feature = "query-execution-contract-test-support"))]
        {
            let ingress = self.require_query_lifecycle("StageFragments")?;
            let request = request.into_inner();
            let response = tokio::task::spawn_blocking(move || {
                handle_stage_fragments(ingress.as_ref(), request)
            })
            .await
            .map_err(|error| {
                tonic::Status::internal(format!("stage_fragments handler panicked: {error}"))
            })??;
            Ok(tonic::Response::new(response))
        }
        #[cfg(not(any(test, feature = "query-execution-contract-test-support")))]
        {
            let _ = request;
            Err(tonic::Status::failed_precondition(
                "core neutral NovaRocksGrpc handler rejects query lifecycle RPC",
            ))
        }
    }

    async fn start_prepared_query(
        &self,
        request: tonic::Request<proto::novarocks::StartPreparedQueryRequest>,
    ) -> Result<tonic::Response<proto::novarocks::StartPreparedQueryResponse>, tonic::Status> {
        #[cfg(any(test, feature = "query-execution-contract-test-support"))]
        {
            let ingress = self.require_query_lifecycle("StartPreparedQuery")?;
            let request = request.into_inner();
            let response = tokio::task::spawn_blocking(move || {
                handle_start_prepared_query(ingress.as_ref(), request)
            })
            .await
            .map_err(|error| {
                tonic::Status::internal(format!("start_prepared_query handler panicked: {error}"))
            })??;
            Ok(tonic::Response::new(response))
        }
        #[cfg(not(any(test, feature = "query-execution-contract-test-support")))]
        {
            let _ = request;
            Err(tonic::Status::failed_precondition(
                "core neutral NovaRocksGrpc handler rejects query lifecycle RPC",
            ))
        }
    }

    async fn abort_query(
        &self,
        request: tonic::Request<proto::novarocks::AbortQueryRequest>,
    ) -> Result<tonic::Response<proto::novarocks::AbortQueryResponse>, tonic::Status> {
        #[cfg(any(test, feature = "query-execution-contract-test-support"))]
        {
            let ingress = self.require_query_lifecycle("AbortQuery")?;
            let request = request.into_inner();
            let response =
                tokio::task::spawn_blocking(move || handle_abort_query(ingress.as_ref(), request))
                    .await
                    .map_err(|error| {
                        tonic::Status::internal(format!("abort_query handler panicked: {error}"))
                    })??;
            Ok(tonic::Response::new(response))
        }
        #[cfg(not(any(test, feature = "query-execution-contract-test-support")))]
        {
            let _ = request;
            Err(tonic::Status::failed_precondition(
                "core neutral NovaRocksGrpc handler rejects query lifecycle RPC",
            ))
        }
    }

    async fn query_control_stream(
        &self,
        request: tonic::Request<tonic::Streaming<proto::novarocks::QueryControlRequest>>,
    ) -> Result<tonic::Response<Self::QueryControlStreamStream>, tonic::Status> {
        #[cfg(any(test, feature = "query-execution-contract-test-support"))]
        {
            let ingress = self.require_query_lifecycle("QueryControlStream")?;
            let stream = handle_query_control_stream(
                ingress,
                request.into_inner(),
                self.query_control_shutdown.clone(),
            )
            .await?;
            Ok(tonic::Response::new(Box::pin(stream)))
        }
        #[cfg(not(any(test, feature = "query-execution-contract-test-support")))]
        {
            let _ = request;
            Err(tonic::Status::failed_precondition(
                "core neutral NovaRocksGrpc handler rejects query lifecycle RPC",
            ))
        }
    }

    async fn report_query_terminal(
        &self,
        request: tonic::Request<proto::novarocks::ReportQueryTerminalRequest>,
    ) -> Result<tonic::Response<proto::novarocks::ReportQueryTerminalResponse>, tonic::Status> {
        let Some(ingress) = self.terminal_ingress.clone() else {
            return Ok(tonic::Response::new(
                proto::novarocks::ReportQueryTerminalResponse {
                    outcome: proto::novarocks::ReportQueryTerminalOutcome::RejectedGone as i32,
                    detail: "query terminal ingress is not installed for this role".to_string(),
                },
            ));
        };
        let snapshot = request.into_inner().snapshot.ok_or_else(|| {
            tonic::Status::invalid_argument("ReportQueryTerminalRequest missing snapshot")
        })?;
        let snapshot =
            decode_query_terminal_snapshot(&snapshot).map_err(status_from_lifecycle_error)?;
        let ack = tokio::task::spawn_blocking(move || ingress.report_query_terminal(snapshot))
            .await
            .map_err(|error| {
                tonic::Status::internal(format!("query terminal ingress panicked: {error}"))
            })?
            .map_err(status_from_lifecycle_error)?;
        let outcome = match ack.outcome() {
            QueryTerminalReportOutcome::Accepted => {
                proto::novarocks::ReportQueryTerminalOutcome::Accepted
            }
            QueryTerminalReportOutcome::AlreadyAccepted => {
                proto::novarocks::ReportQueryTerminalOutcome::AlreadyAccepted
            }
            QueryTerminalReportOutcome::RejectedConflict => {
                proto::novarocks::ReportQueryTerminalOutcome::RejectedConflict
            }
            QueryTerminalReportOutcome::RejectedGone => {
                proto::novarocks::ReportQueryTerminalOutcome::RejectedGone
            }
        };
        Ok(tonic::Response::new(
            proto::novarocks::ReportQueryTerminalResponse {
                outcome: outcome as i32,
                detail: ack.detail().to_string(),
            },
        ))
    }
}

fn emit_grpc_typed_fetch_marker(status: i32) {
    if crate::common::config::debug_emit_grpc_fragment_marker() {
        println!("NOVAROCKS_GRPC_FETCH_TYPED status={status}");
        let _ = std::io::Write::flush(&mut std::io::stdout());
    }
}

async fn grpc_unimplemented_fallback() -> impl IntoResponse {
    (
        StatusCode::OK,
        [
            (tonic::Status::GRPC_STATUS, HeaderValue::from_static("12")),
            (
                axum::http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/grpc"),
            ),
        ],
    )
}

#[derive(Clone)]
struct AxumGrpcService<S> {
    inner: S,
}

impl<S> AxumGrpcService<S> {
    fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S> Service<axum::http::Request<axum::body::Body>> for AxumGrpcService<S>
where
    S: Service<
            axum::http::Request<tonic::body::BoxBody>,
            Response = axum::http::Response<tonic::body::BoxBody>,
            Error = std::convert::Infallible,
        > + Clone,
{
    type Response = axum::http::Response<tonic::body::BoxBody>;
    type Error = std::convert::Infallible;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: axum::http::Request<axum::body::Body>) -> Self::Future {
        self.inner.call(req.map(boxed))
    }
}

#[cfg(any(test, feature = "query-execution-contract-test-support"))]
fn build_novarocks_http_app(grpc_routes: Routes) -> Router {
    grpc_routes
        .into_axum_router()
        .route("/metrics", get(metrics_http::handle_metrics))
}

#[cfg(any(test, feature = "query-execution-contract-test-support"))]
pub fn start_grpc_server_with_native_fragment_ingress(
    host: &str,
    native_fragment_ingress: Arc<dyn NativeFragmentIngress>,
    query_lifecycle_ingress: Arc<dyn QueryLifecycleIngress>,
) -> Result<(), String> {
    start_grpc_http_server(
        host,
        http_port(),
        native_fragment_ingress,
        query_lifecycle_ingress,
    )
}

#[cfg(any(test, feature = "query-execution-contract-test-support"))]
fn start_grpc_http_server(
    host: &str,
    grpc_http_port: u16,
    native_fragment_ingress: Arc<dyn NativeFragmentIngress>,
    query_lifecycle_ingress: Arc<dyn QueryLifecycleIngress>,
) -> Result<(), String> {
    {
        let state = grpc_server_state()
            .lock()
            .map_err(|_| "lock grpc server state failed".to_string())?;
        if state.started {
            return Ok(());
        }
    }

    let host = host.to_string();
    let std_listener = bind_tcp_listener(&host, grpc_http_port, "novarocks grpc/http")?;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (failure_tx, failure_rx) = mpsc::channel();
    let stop_requested = Arc::new(AtomicBool::new(false));
    let stop_requested_for_thread = Arc::clone(&stop_requested);

    let join_handle = std::thread::spawn(move || {
        supervise_grpc_server_thread(stop_requested_for_thread, failure_tx, move || {
            info!(
                target: "novarocks::grpc",
                host = %host,
                http_port = grpc_http_port,
                "starting grpc server"
            );
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .worker_threads(8)
                .thread_stack_size(crate::runtime::global_async_runtime::WORKER_STACK_SIZE_BYTES)
                .build()
                .map_err(|error| format!("build grpc server runtime failed: {error}"))?;

            rt.block_on(async move {
                let listener = TokioTcpListener::from_std(std_listener)
                    .map_err(|error| format!("create grpc/http tokio listener failed: {error}"))?;
                let mut http_shutdown = shutdown_rx.clone();
                let query_control_shutdown = shutdown_rx.clone();

                let svc = GrpcService::with_fragment_execution(
                    native_fragment_ingress,
                    query_lifecycle_ingress,
                )
                .with_query_control_shutdown(query_control_shutdown);
                let svc = proto::novarocks::nova_rocks_grpc_server::NovaRocksGrpcServer::new(svc)
                    .max_decoding_message_size(GRPC_MAX_MESSAGE_BYTES)
                    .max_encoding_message_size(GRPC_MAX_MESSAGE_BYTES);
                let app = build_novarocks_http_app(Routes::new(svc));
                axum::serve(listener, app)
                    .with_graceful_shutdown(async move {
                        while !*http_shutdown.borrow() {
                            if http_shutdown.changed().await.is_err() {
                                break;
                            }
                        }
                    })
                    .await
                    .map_err(|error| format!("grpc/http serve future failed: {error}"))
            })
        });
    });

    let mut state = grpc_server_state()
        .lock()
        .map_err(|_| "lock grpc server state failed".to_string())?;
    if state.started {
        return Ok(());
    }
    state.started = true;
    state.bound_port = Some(grpc_http_port);
    state.shutdown_tx = Some(shutdown_tx);
    state.join_handle = Some(join_handle);
    state.stop_requested = Some(stop_requested);
    state.failure_rx = Some(failure_rx);
    Ok(())
}

fn supervise_grpc_server_thread<F>(
    stop_requested: Arc<AtomicBool>,
    failure_tx: mpsc::Sender<String>,
    run: F,
) where
    F: FnOnce() -> Result<(), String>,
{
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(run));
    if stop_requested.load(Ordering::Acquire) {
        return;
    }
    let detail = match outcome {
        Ok(Ok(())) => "serve future ended unexpectedly after readiness".to_string(),
        Ok(Err(error)) => format!("serve future failed after readiness: {error}"),
        Err(payload) => format!(
            "server thread panicked after readiness: {}",
            panic_payload_message(payload)
        ),
    };
    error!(
        target: "novarocks::grpc",
        error = %detail,
        "grpc server stopped unexpectedly"
    );
    let _ = failure_tx.send(format!("grpc server {detail}"));
}

fn panic_payload_message(payload: Box<dyn std::any::Any + Send>) -> String {
    payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| {
            payload
                .downcast_ref::<&str>()
                .map(|value| (*value).to_string())
        })
        .unwrap_or_else(|| "unknown panic payload".to_string())
}

pub fn grpc_server_bound_port() -> Result<u16, String> {
    if let Some(failure) = poll_grpc_server_failure()? {
        return Err(failure);
    }
    let state = grpc_server_state()
        .lock()
        .map_err(|_| "lock grpc server state failed".to_string())?;
    if !state.started {
        return Err("grpc server not started".to_string());
    }
    state
        .bound_port
        .ok_or_else(|| "grpc server bound port unavailable".to_string())
}

pub fn poll_grpc_server_failure() -> Result<Option<String>, String> {
    let mut state = grpc_server_state()
        .lock()
        .map_err(|_| "lock grpc server state failed".to_string())?;
    let Some(failure_rx) = state.failure_rx.as_ref() else {
        return Ok(None);
    };
    match failure_rx.try_recv() {
        Ok(failure) => {
            state.started = false;
            state.bound_port = None;
            state.failure_rx = None;
            Ok(Some(failure))
        }
        Err(mpsc::TryRecvError::Empty) => Ok(None),
        Err(mpsc::TryRecvError::Disconnected) => {
            let expected = state
                .stop_requested
                .as_ref()
                .is_some_and(|stop| stop.load(Ordering::Acquire));
            state.failure_rx = None;
            if expected {
                Ok(None)
            } else {
                state.started = false;
                state.bound_port = None;
                Ok(Some(
                    "grpc server supervisor exited unexpectedly after readiness".to_string(),
                ))
            }
        }
    }
}

fn join_grpc_server_thread(handle: JoinHandle<()>) -> Result<(), String> {
    handle.join().map_err(|payload| {
        let detail = payload
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| {
                payload
                    .downcast_ref::<&str>()
                    .map(|value| (*value).to_string())
            })
            .unwrap_or_else(|| "unknown panic payload".to_string());
        format!("grpc server thread panicked: {detail}")
    })
}

pub fn stop_grpc_server() -> Result<(), String> {
    let (shutdown_tx, join_handle, stop_requested, failure_rx) = {
        let mut state = grpc_server_state()
            .lock()
            .map_err(|_| "lock grpc server state failed".to_string())?;
        if !state.started
            && state.shutdown_tx.is_none()
            && state.join_handle.is_none()
            && state.failure_rx.is_none()
        {
            return Ok(());
        }
        state.started = false;
        state.bound_port = None;
        (
            state.shutdown_tx.take(),
            state.join_handle.take(),
            state.stop_requested.take(),
            state.failure_rx.take(),
        )
    };

    if let Some(stop_requested) = stop_requested {
        stop_requested.store(true, Ordering::Release);
    }
    if let Some(tx) = shutdown_tx {
        let _ = tx.send(true);
    }
    let join_result = match join_handle {
        Some(handle) => join_grpc_server_thread(handle),
        None => Ok(()),
    };

    let mut failures = Vec::new();
    if let Some(receiver) = failure_rx {
        failures.extend(receiver.try_iter());
    }
    if let Err(error) = join_result {
        failures.push(error);
    }
    if !failures.is_empty() {
        return Err(failures.join("; "));
    }
    Ok(())
}

/// Parse a gRPC bind address from a host string and port.
///
/// Handles bare IPv6 addresses (`::`, `::1`), bracketed IPv6 (`[::]`, `[::1]`),
/// and IPv4/hostname strings.  Bare and bracketed IPv6 forms are parsed via
/// `IpAddr` to avoid the `:::PORT` ambiguity that arises from naive
/// `format!("{host}:{port}")` string concatenation.
pub(crate) fn parse_grpc_bind_addr(host: &str, port: u16) -> Result<SocketAddr, String> {
    // Strip brackets from bracketed IPv6 literals, e.g. `[::1]` -> `::1`.
    let bare = if host.starts_with('[') && host.ends_with(']') {
        &host[1..host.len() - 1]
    } else {
        host
    };

    // If the bare string is a valid IP literal, build SocketAddr directly.
    if let Ok(ip) = bare.parse::<std::net::IpAddr>() {
        return Ok(SocketAddr::new(ip, port));
    }

    // Fallback for hostnames: use bracketed form for any host containing `:`.
    let formatted = if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    };
    formatted
        .parse::<SocketAddr>()
        .map_err(|e| format!("parse gRPC bind addr '{formatted}' failed: {e}"))
}

fn ensure_bindable(host: &str, port: u16, role: &str) -> Result<(), String> {
    drop(bind_tcp_listener(host, port, role)?);
    Ok(())
}

fn bind_tcp_listener(host: &str, port: u16, role: &str) -> Result<TcpListener, String> {
    let addr = parse_grpc_bind_addr(host, port)
        .map_err(|e| format!("parse {role} bind addr failed: {e}"))?;
    let listener = TcpListener::bind(addr)
        .map_err(|e| format!("failed to bind {role} listener on {addr}: {e}"))?;
    listener
        .set_nonblocking(true)
        .map_err(|e| format!("failed to configure {role} listener on {addr}: {e}"))?;
    Ok(listener)
}

#[derive(Clone)]
enum StandaloneGrpcMode {
    #[cfg(any(test, feature = "query-execution-contract-test-support"))]
    FullExecution(
        Arc<dyn NativeFragmentIngress>,
        Arc<dyn QueryLifecycleIngress>,
    ),
    TerminalIngressOnly,
}

impl std::fmt::Debug for StandaloneGrpcMode {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            #[cfg(any(test, feature = "query-execution-contract-test-support"))]
            Self::FullExecution(_, _) => formatter.write_str("FullExecution"),
            Self::TerminalIngressOnly => formatter.write_str("TerminalIngressOnly"),
        }
    }
}

impl StandaloneGrpcMode {
    fn service(self) -> GrpcService {
        match self {
            #[cfg(any(test, feature = "query-execution-contract-test-support"))]
            StandaloneGrpcMode::FullExecution(ingress, query_lifecycle_ingress) => {
                GrpcService::with_fragment_execution(ingress, query_lifecycle_ingress)
            }
            StandaloneGrpcMode::TerminalIngressOnly => GrpcService::terminal_ingress_only(),
        }
    }

    fn label(&self) -> &'static str {
        match self {
            #[cfg(any(test, feature = "query-execution-contract-test-support"))]
            StandaloneGrpcMode::FullExecution(_, _) => "standalone grpc exchange",
            StandaloneGrpcMode::TerminalIngressOnly => "standalone grpc terminal ingress",
        }
    }
}

/// Start a terminal-fallback endpoint for the standalone coordinator.
pub fn start_grpc_terminal_server(
    host: &str,
    port: u16,
    terminal_ingress: Arc<dyn QueryTerminalIngress>,
) -> Result<(), String> {
    start_standalone_grpc_server(
        host,
        port,
        StandaloneGrpcMode::TerminalIngressOnly,
        Some(terminal_ingress),
    )
}

/// Start a lightweight gRPC exchange server on a specific port.
///
/// This does not require global config to be initialised: the caller supplies
/// the bind address and native fragment ingress directly.
#[cfg(any(test, feature = "query-execution-contract-test-support"))]
pub fn start_grpc_exchange_server(
    host: &str,
    port: u16,
    native_fragment_ingress: Arc<dyn NativeFragmentIngress>,
    query_lifecycle_ingress: Arc<dyn QueryLifecycleIngress>,
) -> Result<(), String> {
    start_grpc_exchange_server_with_terminal_ingress(
        host,
        port,
        native_fragment_ingress,
        query_lifecycle_ingress,
        None,
    )
}

/// Starts a native fragment endpoint with an optional FE-owned terminal
/// fallback ingress.  Backend-only and compat composition deliberately pass
/// `None`, so their endpoint rejects `ReportQueryTerminal`.
#[cfg(any(test, feature = "query-execution-contract-test-support"))]
pub fn start_grpc_exchange_server_with_terminal_ingress(
    host: &str,
    port: u16,
    native_fragment_ingress: Arc<dyn NativeFragmentIngress>,
    query_lifecycle_ingress: Arc<dyn QueryLifecycleIngress>,
    terminal_ingress: Option<Arc<dyn QueryTerminalIngress>>,
) -> Result<(), String> {
    start_standalone_grpc_server(
        host,
        port,
        StandaloneGrpcMode::FullExecution(native_fragment_ingress, query_lifecycle_ingress),
        terminal_ingress,
    )
}

fn start_standalone_grpc_server(
    host: &str,
    port: u16,
    mode: StandaloneGrpcMode,
    terminal_ingress: Option<Arc<dyn QueryTerminalIngress>>,
) -> Result<(), String> {
    {
        let mut state = grpc_server_state()
            .lock()
            .map_err(|_| "lock grpc server state failed".to_string())?;
        if state.started {
            return Ok(());
        }
        if state.starting {
            return Err("grpc server startup already in progress".to_string());
        }
        state.starting = true;
    }
    #[cfg(test)]
    pause_standalone_grpc_startup_after_reservation();

    let host = host.to_string();
    let std_listener = match bind_tcp_listener(&host, port, mode.label()) {
        Ok(listener) => listener,
        Err(error) => {
            clear_grpc_server_startup_reservation();
            return Err(error);
        }
    };
    let bound_port = std_listener
        .local_addr()
        .map_err(|error| {
            clear_grpc_server_startup_reservation();
            format!("read {} bound address failed: {error}", mode.label())
        })?
        .port();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (failure_tx, failure_rx) = mpsc::channel();
    let stop_requested = Arc::new(AtomicBool::new(false));
    let stop_requested_for_thread = Arc::clone(&stop_requested);

    let join_handle = std::thread::spawn(move || {
        supervise_grpc_server_thread(stop_requested_for_thread, failure_tx, move || {
            info!(
                target: "novarocks::grpc",
                host = %host,
                port = bound_port,
                mode = ?mode,
                "starting standalone grpc server"
            );
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .worker_threads(8)
                .thread_stack_size(crate::runtime::global_async_runtime::WORKER_STACK_SIZE_BYTES)
                .build()
                .map_err(|error| format!("build standalone grpc server runtime failed: {error}"))?;

            rt.block_on(async move {
                let listener = TokioTcpListener::from_std(std_listener).map_err(|error| {
                    format!("create standalone grpc/http tokio listener failed: {error}")
                })?;
                let mut shutdown = shutdown_rx.clone();
                #[cfg(any(test, feature = "query-execution-contract-test-support"))]
                let query_control_shutdown = shutdown_rx.clone();

                let mut svc = mode.service();
                if let Some(terminal_ingress) = terminal_ingress {
                    svc = svc.with_terminal_ingress(terminal_ingress);
                }
                #[cfg(any(test, feature = "query-execution-contract-test-support"))]
                let svc = svc.with_query_control_shutdown(query_control_shutdown);
                let svc = proto::novarocks::nova_rocks_grpc_server::NovaRocksGrpcServer::new(svc)
                    .max_decoding_message_size(GRPC_MAX_MESSAGE_BYTES)
                    .max_encoding_message_size(GRPC_MAX_MESSAGE_BYTES);
                let grpc_path = format!(
                    "/{}/*rest",
                    <proto::novarocks::nova_rocks_grpc_server::NovaRocksGrpcServer<GrpcService> as NamedService>::NAME
                );
                let grpc_service = AxumGrpcService::new(svc);
                let app = Router::new()
                    .route_service(&grpc_path, grpc_service)
                    .route("/metrics", get(metrics_http::handle_metrics))
                    .fallback(grpc_unimplemented_fallback);
                axum::serve(listener, app)
                    .with_graceful_shutdown(async move {
                        while !*shutdown.borrow() {
                            if shutdown.changed().await.is_err() {
                                break;
                            }
                        }
                    })
                    .await
                    .map_err(|error| format!("standalone grpc serve future failed: {error}"))
            })
        });
    });

    let mut state = match grpc_server_state().lock() {
        Ok(state) => state,
        Err(_) => {
            stop_requested.store(true, Ordering::Release);
            let _ = shutdown_tx.send(true);
            let _ = join_grpc_server_thread(join_handle);
            clear_grpc_server_startup_reservation();
            return Err("lock grpc server state failed".to_string());
        }
    };
    debug_assert!(state.starting, "standalone grpc start lost its reservation");
    state.starting = false;
    state.started = true;
    state.bound_port = Some(bound_port);
    state.shutdown_tx = Some(shutdown_tx);
    state.join_handle = Some(join_handle);
    state.stop_requested = Some(stop_requested);
    state.failure_rx = Some(failure_rx);
    Ok(())
}

fn clear_grpc_server_startup_reservation() {
    if let Ok(mut state) = grpc_server_state().lock() {
        state.starting = false;
    }
}

#[cfg(test)]
mod tests {
    use super::{
        IndependentGrpcRuntimeFilterNode, IndependentGrpcStartupProbe, ensure_bindable,
        parse_grpc_bind_addr,
    };
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, TcpListener};
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::sync::mpsc;
    use std::time::Duration;
    use tracing_subscriber::layer::{Context as LayerContext, SubscriberExt};

    #[derive(Clone)]
    struct ErrorEventCounter {
        count: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl<S> tracing_subscriber::Layer<S> for ErrorEventCounter
    where
        S: tracing::Subscriber,
    {
        fn on_event(&self, event: &tracing::Event<'_>, _context: LayerContext<'_, S>) {
            if *event.metadata().level() == tracing::Level::ERROR {
                self.count.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct RetiredSubmitRequest {}

    #[derive(Clone, PartialEq, prost::Message)]
    struct RetiredSubmitResponse {}

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn generated_server_returns_unimplemented_for_retired_submit_method() {
        let node = IndependentGrpcRuntimeFilterNode::start().expect("start generated gRPC server");
        let channel =
            tonic::transport::Endpoint::from_shared(format!("http://{}", node.endpoint()))
                .expect("retired submit endpoint")
                .connect()
                .await
                .expect("connect retired submit client");
        let mut client = tonic::client::Grpc::new(channel);
        client.ready().await.expect("retired submit client ready");
        let error = client
            .unary(
                tonic::Request::new(RetiredSubmitRequest {}),
                tonic::codegen::http::uri::PathAndQuery::from_static(
                    "/novarocks.NovaRocksGrpc/SubmitFragment",
                ),
                tonic::codec::ProstCodec::<RetiredSubmitRequest, RetiredSubmitResponse>::default(),
            )
            .await
            .expect_err("retired SubmitFragment method must be unavailable");
        assert_eq!(error.code(), tonic::Code::Unimplemented);
    }

    #[test]
    fn independent_runtime_filter_start_failure_stops_all_started_threads() {
        let (manager_tx, manager_rx) = mpsc::sync_channel(1);
        let (server_exited_tx, server_exited_rx) = mpsc::sync_channel(1);
        let (clean_exited_tx, clean_exited_rx) = mpsc::sync_channel(1);
        let result =
            IndependentGrpcRuntimeFilterNode::start_with_probe(Some(IndependentGrpcStartupProbe {
                manager: manager_tx,
                server_exited: server_exited_tx,
                clean_exited: clean_exited_tx,
                panic_before_ready: true,
            }));
        assert!(result.is_err(), "injected pre-ready failure must surface");
        let manager = manager_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("startup publishes the manager weak reference");
        server_exited_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("server thread exits after injected startup failure");
        let clean_exited_before_cleanup = clean_exited_rx
            .recv_timeout(Duration::from_millis(100))
            .is_ok();
        let leaked_manager = manager.upgrade();
        if let Some(manager) = &leaked_manager {
            manager.stop_clean_loop_for_test();
            let _ = clean_exited_rx.recv_timeout(Duration::from_secs(1));
        }
        assert!(
            clean_exited_before_cleanup,
            "start returning Err must stop the manager clean loop"
        );
        assert!(
            leaked_manager.is_none(),
            "start returning Err must release the manager"
        );
    }

    #[test]
    fn independent_runtime_filter_shutdown_shares_one_deadline_across_threads() {
        let server = std::thread::spawn(|| {
            std::thread::sleep(Duration::from_millis(100));
        });
        let clean = std::thread::spawn(|| {
            std::thread::sleep(Duration::from_secs(1));
        });
        let wait = Duration::from_millis(150);
        let started_at = std::time::Instant::now();

        let failures =
            super::join_independent_runtime_filter_threads(Some(server), Some(clean), wait);
        let elapsed = started_at.elapsed();

        assert_eq!(failures.len(), 1, "{failures:?}");
        assert!(failures[0].contains("manager clean loop"), "{failures:?}");
        assert!(
            elapsed < Duration::from_millis(225),
            "server and clean loop each received a fresh deadline: elapsed={elapsed:?}"
        );
    }

    #[test]
    fn grpc_stop_join_propagates_server_thread_panic() {
        let handle = std::thread::spawn(|| panic!("injected grpc server panic"));
        let error = super::join_grpc_server_thread(handle)
            .expect_err("grpc server thread panic must reach stop caller");
        assert!(error.contains("panicked"), "{error}");
        assert!(error.contains("injected grpc server panic"), "{error}");
    }

    #[test]
    fn grpc_supervisor_reports_post_ready_serve_exit() {
        let (failure_tx, failure_rx) = mpsc::channel();
        let error_events = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let subscriber = tracing_subscriber::registry().with(ErrorEventCounter {
            count: Arc::clone(&error_events),
        });
        tracing::subscriber::with_default(subscriber, || {
            super::supervise_grpc_server_thread(
                std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
                failure_tx,
                || Ok(()),
            );
        });

        let failure = failure_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("unexpected post-ready serve exit must be reported");
        assert_eq!(
            error_events.load(Ordering::Relaxed),
            1,
            "unexpected supervisor exit must remain observable without polling"
        );
        assert!(failure.contains("grpc server"), "{failure}");
        assert!(
            failure.contains("ended unexpectedly after readiness"),
            "{failure}"
        );
    }

    #[test]
    fn grpc_supervisor_reports_post_ready_panic() {
        let (failure_tx, failure_rx) = mpsc::channel();
        super::supervise_grpc_server_thread(
            std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            failure_tx,
            || -> Result<(), String> { panic!("injected post-ready grpc panic") },
        );

        let failure = failure_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("post-ready panic must be reported");
        assert!(failure.contains("panicked after readiness"), "{failure}");
        assert!(
            failure.contains("injected post-ready grpc panic"),
            "{failure}"
        );
    }

    #[test]
    fn grpc_poll_reports_post_ready_supervisor_error() {
        let _test_guard = crate::engine::acquire_standalone_test_guard();
        let (failure_tx, failure_rx) = mpsc::channel();
        let stop_requested = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        {
            let mut state = super::grpc_server_state()
                .lock()
                .expect("lock grpc server state");
            *state = super::GrpcServerState {
                started: true,
                starting: false,
                bound_port: Some(19080),
                shutdown_tx: None,
                join_handle: None,
                stop_requested: Some(std::sync::Arc::clone(&stop_requested)),
                failure_rx: Some(failure_rx),
            };
        }

        super::supervise_grpc_server_thread(stop_requested, failure_tx, || {
            Err("injected runtime/serve failure".to_string())
        });

        let failure = super::poll_grpc_server_failure()
            .expect("poll grpc server failure")
            .expect("post-ready supervisor error must be observable");
        assert!(
            failure.contains("injected runtime/serve failure"),
            "{failure}"
        );

        *super::grpc_server_state()
            .lock()
            .expect("reset grpc server state") = super::GrpcServerState::default();
    }

    #[test]
    fn grpc_poll_reports_unexpected_post_ready_supervisor_exit() {
        let _test_guard = crate::engine::acquire_standalone_test_guard();
        let (failure_tx, failure_rx) = mpsc::channel();
        let stop_requested = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        {
            let mut state = super::grpc_server_state()
                .lock()
                .expect("lock grpc server state");
            *state = super::GrpcServerState {
                started: true,
                starting: false,
                bound_port: Some(19080),
                shutdown_tx: None,
                join_handle: None,
                stop_requested: Some(stop_requested),
                failure_rx: Some(failure_rx),
            };
        }
        drop(failure_tx);

        let failure = super::poll_grpc_server_failure()
            .expect("poll grpc server failure")
            .expect("unexpected post-ready exit must be observable");
        assert!(failure.contains("grpc server supervisor"), "{failure}");
        assert!(
            failure.contains("unexpectedly after readiness"),
            "{failure}"
        );

        *super::grpc_server_state()
            .lock()
            .expect("reset grpc server state") = super::GrpcServerState::default();
    }

    #[test]
    fn grpc_poll_ignores_requested_stop_after_supervisor_exit() {
        let _test_guard = crate::engine::acquire_standalone_test_guard();
        let (failure_tx, failure_rx) = mpsc::channel();
        let stop_requested = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
        {
            let mut state = super::grpc_server_state()
                .lock()
                .expect("lock grpc server state");
            *state = super::GrpcServerState {
                started: false,
                starting: false,
                bound_port: None,
                shutdown_tx: None,
                join_handle: None,
                stop_requested: Some(stop_requested),
                failure_rx: Some(failure_rx),
            };
        }
        drop(failure_tx);

        assert_eq!(
            super::poll_grpc_server_failure().expect("poll grpc server failure"),
            None,
            "requested stop must not be reported as a supervisor failure"
        );

        *super::grpc_server_state()
            .lock()
            .expect("reset grpc server state") = super::GrpcServerState::default();
    }

    #[test]
    fn concurrent_standalone_grpc_start_releases_the_losing_listener() {
        let _test_guard = crate::engine::acquire_standalone_test_guard();
        super::stop_grpc_server().expect("reset any prior standalone gRPC server");

        fn unused_port() -> u16 {
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
            let port = listener.local_addr().expect("read ephemeral port").port();
            drop(listener);
            port
        }

        let first_port = unused_port();
        let second_port = unused_port();
        super::STANDALONE_GRPC_STARTUP_RESERVATIONS.store(0, Ordering::Release);
        super::PAUSE_STANDALONE_GRPC_STARTUP_AFTER_RESERVATION.store(true, Ordering::Release);

        let first = std::thread::spawn(move || {
            super::start_grpc_exchange_server(
                "127.0.0.1",
                first_port,
                super::rejecting_test_native_fragment_ingress(),
                super::rejecting_test_query_lifecycle_ingress(),
            )
        });
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while super::STANDALONE_GRPC_STARTUP_RESERVATIONS.load(Ordering::Acquire) == 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "first startup did not reserve gRPC lifecycle ownership"
            );
            std::thread::yield_now();
        }

        let second = std::thread::spawn(move || {
            super::start_grpc_exchange_server(
                "127.0.0.1",
                second_port,
                super::rejecting_test_native_fragment_ingress(),
                super::rejecting_test_query_lifecycle_ingress(),
            )
        });
        let second_deadline = std::time::Instant::now() + Duration::from_millis(100);
        while super::STANDALONE_GRPC_STARTUP_RESERVATIONS.load(Ordering::Acquire) < 2
            && std::time::Instant::now() < second_deadline
        {
            std::thread::yield_now();
        }
        super::PAUSE_STANDALONE_GRPC_STARTUP_AFTER_RESERVATION.store(false, Ordering::Release);

        let first_result = first.join().expect("first startup thread panicked");
        let second_result = second.join().expect("second startup thread panicked");
        let successful_starts =
            usize::from(first_result.is_ok()) + usize::from(second_result.is_ok());
        let failed_starts =
            usize::from(first_result.is_err()) + usize::from(second_result.is_err());
        let rejected_start = first_result
            .as_ref()
            .err()
            .or_else(|| second_result.as_ref().err());
        let stop_result = super::stop_grpc_server();
        let first_released = TcpListener::bind(("127.0.0.1", first_port)).is_ok();
        let second_released = TcpListener::bind(("127.0.0.1", second_port)).is_ok();

        assert_eq!(successful_starts, 1, "only the owner may start a listener");
        assert_eq!(
            failed_starts, 1,
            "the concurrent non-owner must be rejected"
        );
        assert!(
            rejected_start
                .expect("concurrent non-owner must return an error")
                .contains("startup already in progress")
        );
        assert!(
            stop_result.is_ok(),
            "owner shutdown failed: {stop_result:?}"
        );
        assert!(first_released, "first listener was not released");
        assert!(second_released, "second listener was not released");
    }

    #[test]
    fn test_ensure_bindable_fails_for_occupied_port() {
        let occupied = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral test port");
        let occupied_port = occupied.local_addr().expect("get local addr").port();
        let err = ensure_bindable("127.0.0.1", occupied_port, "unit-test")
            .expect_err("expected bind failure");
        assert!(err.contains("failed to bind"));
        drop(occupied);
    }

    #[test]
    fn parse_grpc_bind_addr_bare_ipv6_wildcard() {
        let addr = parse_grpc_bind_addr("::", 9070).expect("parse :: wildcard");
        assert_eq!(addr.ip(), IpAddr::V6(Ipv6Addr::UNSPECIFIED));
        assert_eq!(addr.port(), 9070);
    }

    #[test]
    fn parse_grpc_bind_addr_bracketed_ipv6_wildcard() {
        let addr = parse_grpc_bind_addr("[::]", 9070).expect("parse [::] wildcard");
        assert_eq!(addr.ip(), IpAddr::V6(Ipv6Addr::UNSPECIFIED));
        assert_eq!(addr.port(), 9070);
    }

    #[test]
    fn parse_grpc_bind_addr_bracketed_ipv6_loopback() {
        let addr = parse_grpc_bind_addr("[::1]", 9070).expect("parse [::1]");
        assert_eq!(addr.ip(), IpAddr::V6(Ipv6Addr::LOCALHOST));
        assert_eq!(addr.port(), 9070);
    }

    #[test]
    fn parse_grpc_bind_addr_bare_ipv6_loopback() {
        let addr = parse_grpc_bind_addr("::1", 9070).expect("parse ::1");
        assert_eq!(addr.ip(), IpAddr::V6(Ipv6Addr::LOCALHOST));
        assert_eq!(addr.port(), 9070);
    }

    #[test]
    fn parse_grpc_bind_addr_ipv4() {
        let addr = parse_grpc_bind_addr("127.0.0.1", 9070).expect("parse 127.0.0.1");
        assert_eq!(addr.ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert_eq!(addr.port(), 9070);
    }

    #[test]
    fn parse_grpc_bind_addr_ipv4_wildcard() {
        let addr = parse_grpc_bind_addr("0.0.0.0", 9070).expect("parse 0.0.0.0");
        assert_eq!(addr.ip(), IpAddr::V4(Ipv4Addr::UNSPECIFIED));
        assert_eq!(addr.port(), 9070);
    }
}

#[cfg(test)]
mod pr3_tests {}
