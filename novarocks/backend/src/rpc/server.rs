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

//! Production Backend gRPC service and its instance-owned listener.
//!
//! The generated service is intentionally owned by `novarocks-backend`.  The
//! core service remains the compatibility-neutral implementation while the
//! closeout migrates individual execution adapters behind this backend entry
//! point; no process-global listener state is used here.

use std::net::{SocketAddr, TcpListener, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::task::{Context, Poll};
use std::thread::JoinHandle;

use crate::rpc::data_plane::BackendDataPlane;
use crate::rpc::task_execution::{TaskExecutionIngress, TaskStatusEventStream};
use crate::task_execution::TaskInboundCapabilities;
use axum::Router;
use axum::http::{HeaderValue, StatusCode};
use axum::response::IntoResponse;
use hyper::server::conn::http2;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use novarocks_execution::runtime::fragment::io::ExchangeReceiverPort;
use novarocks_native_trust::{NativeIncomingAdapter, NativeServerAdmission, NativeTrust};
use novarocks_proto_codec::catalog::{PruneCatalogsRequest, PruneCatalogsResponse};
use novarocks_proto_codec::membership::{
    BackendProcessDescriptor, BackendProcessId as ProtocolBackendProcessId,
};
use novarocks_proto_models::{catalog, filter, novarocks as proto};
use novarocks_types::BackendProcessId;
use tokio::net::TcpListener as TokioTcpListener;
use tokio::sync::watch;
use tokio_stream::wrappers::ReceiverStream;
use tonic::body::boxed;
use tonic::codegen::Service;
use tonic::server::NamedService;
use tower::ServiceExt;

use super::transport::nova_rocks_grpc_server::{NovaRocksGrpc, NovaRocksGrpcServer};
use crate::connector::catalog_manager::CatalogPruneResult;
use crate::drain::BackendDrainState;
use crate::rpc::runtime::BackendNativeTransport;
use crate::runtime_filter::rpc::{
    BackendRuntimeFilterEnvelopeIngress, handle_runtime_filter_envelope,
};

const GRPC_MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;

/// What a rejected catalog prune is allowed to say on the wire.
///
/// A catalog definition carries credential material. This detail is a fixed
/// string rather than anything derived from the handles involved, so no part
/// of a catalog's properties can reach an error, a log, or a status by being
/// interpolated into a rejection.
const CATALOG_PRUNE_STALE_SNAPSHOT_DETAIL: &str =
    "catalog reachability snapshot omits one or more live catalogs";

/// This process's owner of catalog reachability.
///
/// The frontend sends one complete reachability snapshot; the owner reconciles
/// its retained catalog runtimes against it and answers in its own vocabulary.
/// This port exists so the wire handler never holds a catalog registry of its
/// own: there is exactly one `CatalogManager` per process, and a second
/// reconciler would be a second authority over the same leases.
pub(crate) trait CatalogReachabilityAuthority: Send + Sync + 'static {
    fn prune_unreachable_catalogs(
        &self,
        reachable: std::collections::BTreeSet<novarocks_spi::connector::CatalogHandle>,
    ) -> CatalogPruneResult;
}

/// Everything a heartbeat answers with.
///
/// A heartbeat is a question about this process, not about any query: which
/// process is answering, what it immutably is, and whether it will still take
/// new work. All three travel together because they are one answer, and
/// because a reply that mixed one process's identity with another's drain
/// state would be worse than no reply at all.
#[derive(Clone)]
pub(crate) struct BackendProcessFacts {
    /// The identity minted by this process's composition root. A heartbeat
    /// naming a different one is a stale peer talking to a replaced process.
    pub(crate) process_id: BackendProcessId,
    pub(crate) descriptor: BackendProcessDescriptor,
    pub(crate) drain: Arc<BackendDrainState>,
}

/// Backend-owned production Tonic service. Domain owners contribute the narrow
/// ingress ports while this service composes them with `BackendDataPlane`.
#[derive(Clone)]
pub(crate) struct BackendRpcService {
    task_execution_ingress: Arc<dyn TaskExecutionIngress>,
    catalog_reachability: Arc<dyn CatalogReachabilityAuthority>,
    process: BackendProcessFacts,
    data_plane: BackendDataPlane,
    runtime_filter_ingress: Arc<dyn BackendRuntimeFilterEnvelopeIngress>,
}

impl BackendRpcService {
    pub(crate) fn new(
        task_execution_ingress: Arc<dyn TaskExecutionIngress>,
        catalog_reachability: Arc<dyn CatalogReachabilityAuthority>,
        runtime_filter_ingress: Arc<dyn BackendRuntimeFilterEnvelopeIngress>,
        exchange_receiver_port: Arc<dyn ExchangeReceiverPort>,
        task_inbound_capabilities: Arc<TaskInboundCapabilities>,
        process: BackendProcessFacts,
    ) -> Self {
        Self {
            task_execution_ingress,
            catalog_reachability,
            process,
            data_plane: BackendDataPlane::with_exchange_receiver_port(
                exchange_receiver_port,
                task_inbound_capabilities,
            ),
            runtime_filter_ingress,
        }
    }
}

#[tonic::async_trait]
impl NovaRocksGrpc for BackendRpcService {
    type ExchangeStream = std::pin::Pin<
        Box<
            dyn tokio_stream::Stream<Item = Result<proto::ExchangeResponse, tonic::Status>>
                + Send
                + 'static,
        >,
    >;
    type SubscribeTaskStatusStream = TaskStatusEventStream;
    // Named only because the generated trait requires a type here. The RPC it
    // belongs to is refused below, so this stream is never constructed.
    type QueryControlStreamStream = std::pin::Pin<
        Box<
            dyn tokio_stream::Stream<Item = Result<proto::QueryControlResponse, tonic::Status>>
                + Send
                + 'static,
        >,
    >;

    async fn announce_backend(
        &self,
        _request: tonic::Request<proto::AnnounceBackendRequest>,
    ) -> Result<tonic::Response<proto::AnnounceBackendResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "backend announce is accepted only by the frontend native ingress",
        ))
    }

    async fn exchange(
        &self,
        request: tonic::Request<tonic::Streaming<proto::ExchangeRequest>>,
    ) -> Result<tonic::Response<Self::ExchangeStream>, tonic::Status> {
        let mut inbound = request.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel(4096);
        let kernel = self.data_plane.clone();
        tokio::spawn(async move {
            loop {
                let request = match inbound.message().await {
                    Ok(Some(request)) => request,
                    Ok(None) => break,
                    Err(error) => {
                        let _ = tx
                            .send(Err(tonic::Status::internal(format!(
                                "exchange recv failed: {error}"
                            ))))
                            .await;
                        break;
                    }
                };
                let kernel = kernel.clone();
                let response =
                    match tokio::task::spawn_blocking(move || kernel.exchange(request)).await {
                        Ok(response) => response,
                        Err(error) => {
                            let _ = tx
                                .send(Err(tonic::Status::internal(format!(
                                    "exchange handler panicked: {error}"
                                ))))
                                .await;
                            break;
                        }
                    };
                let failed = response
                    .status
                    .as_ref()
                    .is_some_and(|status| status.code != 0);
                if tx.send(Ok(response)).await.is_err() || failed {
                    break;
                }
            }
        });
        Ok(tonic::Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn exchange_unary(
        &self,
        request: tonic::Request<proto::ExchangeRequest>,
    ) -> Result<tonic::Response<proto::ExchangeResponse>, tonic::Status> {
        let kernel = self.data_plane.clone();
        let response = tokio::task::spawn_blocking(move || kernel.exchange(request.into_inner()))
            .await
            .map_err(|error| {
                tonic::Status::internal(format!("exchange_unary handler panicked: {error}"))
            })?;
        Ok(tonic::Response::new(response))
    }

    async fn transmit_runtime_filter_envelope(
        &self,
        request: tonic::Request<filter::RuntimeFilterEnvelope>,
    ) -> Result<tonic::Response<filter::RuntimeFilterEnvelopeResponse>, tonic::Status> {
        let ingress = Arc::clone(&self.runtime_filter_ingress);
        let response = tokio::task::spawn_blocking(move || {
            handle_runtime_filter_envelope(ingress, request.into_inner())
        })
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
        request: tonic::Request<filter::LookupRequest>,
    ) -> Result<tonic::Response<filter::LookupResponse>, tonic::Status> {
        Ok(tonic::Response::new(
            self.data_plane.lookup(request.into_inner()),
        ))
    }

    async fn fetch_result(
        &self,
        request: tonic::Request<proto::FetchResultRequest>,
    ) -> Result<tonic::Response<proto::FetchResultResponse>, tonic::Status> {
        let kernel = self.data_plane.clone();
        let response =
            tokio::task::spawn_blocking(move || kernel.fetch_result(request.into_inner()))
                .await
                .map_err(|error| {
                    tonic::Status::internal(format!("fetch_result handler panicked: {error}"))
                })?;
        Ok(tonic::Response::new(response))
    }

    async fn prune_catalogs(
        &self,
        request: tonic::Request<catalog::PruneCatalogsRequest>,
    ) -> Result<tonic::Response<catalog::PruneCatalogsResponse>, tonic::Status> {
        let request = PruneCatalogsRequest::parse(request.into_inner())
            .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
        let reachable = request
            .reachable_catalogs()
            .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?
            .into_iter()
            .collect();
        let authority = Arc::clone(&self.catalog_reachability);
        let response = tokio::task::spawn_blocking(move || {
            match authority.prune_unreachable_catalogs(reachable) {
                CatalogPruneResult::Pruned { .. } => PruneCatalogsResponse::accepted(),
                // The rejected handles are deliberately not reported: naming
                // them would put catalog properties on the wire.
                CatalogPruneResult::Rejected { .. } => {
                    PruneCatalogsResponse::rejected(CATALOG_PRUNE_STALE_SNAPSHOT_DETAIL)
                        .expect("the fixed stale-snapshot detail is a bounded safe detail")
                }
            }
        })
        .await
        .map_err(|error| {
            tonic::Status::internal(format!("prune_catalogs handler panicked: {error}"))
        })?;
        Ok(tonic::Response::new(response.as_proto().clone()))
    }

    async fn heartbeat(
        &self,
        request: tonic::Request<proto::HeartbeatRequest>,
    ) -> Result<tonic::Response<proto::HeartbeatResponse>, tonic::Status> {
        let expected_process_id = request.into_inner().expected_process_id.ok_or_else(|| {
            tonic::Status::invalid_argument("heartbeat expected process id is required")
        })?;
        let expected_process_id = ProtocolBackendProcessId::parse(expected_process_id)
            .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?
            .domain()
            .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
        if expected_process_id != self.process.process_id {
            return Err(tonic::Status::failed_precondition(
                "heartbeat expected backend process id does not match this backend",
            ));
        }
        let num_cores = std::thread::available_parallelism()
            .map(|count| count.get() as u32)
            .unwrap_or(1);
        Ok(tonic::Response::new(proto::HeartbeatResponse {
            num_cores,
            descriptor: Some(self.process.descriptor.as_proto().clone()),
            reported_state: if self.process.drain.is_draining() {
                proto::BackendReportedState::Draining as i32
            } else {
                proto::BackendReportedState::Running as i32
            },
        }))
    }

    async fn apply_task_operations(
        &self,
        request: tonic::Request<proto::ApplyTaskOperationsRequest>,
    ) -> Result<tonic::Response<proto::ApplyTaskOperationsResponse>, tonic::Status> {
        let ingress = Arc::clone(&self.task_execution_ingress);
        let response = tokio::task::spawn_blocking(move || {
            ingress.apply_task_operations(request.into_inner())
        })
        .await
        .map_err(|error| {
            tonic::Status::internal(format!("apply_task_operations handler panicked: {error}"))
        })??;
        Ok(tonic::Response::new(response))
    }

    async fn subscribe_task_status(
        &self,
        request: tonic::Request<proto::SubscribeTaskStatusRequest>,
    ) -> Result<tonic::Response<Self::SubscribeTaskStatusStream>, tonic::Status> {
        // Opening a subscription only registers a cursor, so it does not need
        // the blocking pool the mutation path uses.
        let stream = self
            .task_execution_ingress
            .subscribe_task_status(request.into_inner())?;
        Ok(tonic::Response::new(stream))
    }

    async fn fetch_task_dynamic_filters(
        &self,
        request: tonic::Request<proto::FetchTaskDynamicFiltersRequest>,
    ) -> Result<tonic::Response<proto::FetchTaskDynamicFiltersResponse>, tonic::Status> {
        let ingress = Arc::clone(&self.task_execution_ingress);
        let response = tokio::task::spawn_blocking(move || {
            ingress.fetch_task_dynamic_filters(request.into_inner())
        })
        .await
        .map_err(|error| {
            tonic::Status::internal(format!(
                "fetch_task_dynamic_filters handler panicked: {error}"
            ))
        })??;
        Ok(tonic::Response::new(response))
    }

    async fn get_final_task_info(
        &self,
        request: tonic::Request<proto::GetFinalTaskInfoRequest>,
    ) -> Result<tonic::Response<proto::GetFinalTaskInfoResponse>, tonic::Status> {
        let ingress = Arc::clone(&self.task_execution_ingress);
        let response =
            tokio::task::spawn_blocking(move || ingress.get_final_task_info(request.into_inner()))
                .await
                .map_err(|error| {
                    tonic::Status::internal(format!(
                        "get_final_task_info handler panicked: {error}"
                    ))
                })??;
        Ok(tonic::Response::new(response))
    }

    async fn fetch_task_result(
        &self,
        request: tonic::Request<proto::FetchTaskResultRequest>,
    ) -> Result<tonic::Response<proto::FetchResultResponse>, tonic::Status> {
        let ingress = Arc::clone(&self.task_execution_ingress);
        let response =
            tokio::task::spawn_blocking(move || ingress.fetch_task_result(request.into_inner()))
                .await
                .map_err(|error| {
                    tonic::Status::internal(format!("fetch_task_result handler panicked: {error}"))
                })??;
        Ok(tonic::Response::new(response))
    }

    async fn report_query_terminal(
        &self,
        request: tonic::Request<proto::ReportQueryTerminalRequest>,
    ) -> Result<tonic::Response<proto::ReportQueryTerminalResponse>, tonic::Status> {
        let _ = request;
        Ok(tonic::Response::new(proto::ReportQueryTerminalResponse {
            outcome: proto::ReportQueryTerminalOutcome::RejectedGone as i32,
            detail: "query terminal reports are accepted only by the frontend report endpoint"
                .to_string(),
        }))
    }

    // The six RPCs below belonged to the retired fragment lifecycle. Nothing in
    // this process owns them any more: a query becomes work here through
    // `apply_task_operations`, and its participants are terminated through the
    // same owner. They stay only because the service definition still declares
    // them, and each refuses rather than answering, so a caller that still
    // dials one is told the truth instead of receiving a fabricated
    // acknowledgement it would treat as admission.
    async fn init_query(
        &self,
        request: tonic::Request<proto::InitQueryRequest>,
    ) -> Result<tonic::Response<proto::InitQueryResponse>, tonic::Status> {
        let _ = request;
        Err(retired_lifecycle_rpc("InitQuery"))
    }

    async fn stage_fragments(
        &self,
        request: tonic::Request<proto::StageFragmentsRequest>,
    ) -> Result<tonic::Response<proto::StageFragmentsResponse>, tonic::Status> {
        let _ = request;
        Err(retired_lifecycle_rpc("StageFragments"))
    }

    async fn start_prepared_query(
        &self,
        request: tonic::Request<proto::StartPreparedQueryRequest>,
    ) -> Result<tonic::Response<proto::StartPreparedQueryResponse>, tonic::Status> {
        let _ = request;
        Err(retired_lifecycle_rpc("StartPreparedQuery"))
    }

    async fn task_update(
        &self,
        request: tonic::Request<proto::TaskUpdateRequest>,
    ) -> Result<tonic::Response<proto::TaskUpdateResponse>, tonic::Status> {
        let _ = request;
        Err(retired_lifecycle_rpc("TaskUpdate"))
    }

    async fn abort_query(
        &self,
        request: tonic::Request<proto::AbortQueryRequest>,
    ) -> Result<tonic::Response<proto::AbortQueryResponse>, tonic::Status> {
        let _ = request;
        Err(retired_lifecycle_rpc("AbortQuery"))
    }

    async fn query_control_stream(
        &self,
        request: tonic::Request<tonic::Streaming<proto::QueryControlRequest>>,
    ) -> Result<tonic::Response<Self::QueryControlStreamStream>, tonic::Status> {
        let _ = request;
        Err(retired_lifecycle_rpc("QueryControlStream"))
    }
}

/// Refuses one RPC of the retired fragment lifecycle.
///
/// `Unimplemented` is the exact answer: the method is reachable because the
/// service definition still lists it, and this process implements no owner
/// behind it.
fn retired_lifecycle_rpc(method: &str) -> tonic::Status {
    tonic::Status::unimplemented(format!(
        "{method} belonged to the retired fragment query lifecycle and is not served by this backend"
    ))
}

/// A backend application owns exactly one native listener.  Unlike the legacy
/// core listener, this handle has no global reservation or shutdown state.
pub(crate) struct BackendRpcServerHandle {
    bound_addr: SocketAddr,
    shutdown_tx: Option<watch::Sender<bool>>,
    failure_rx: mpsc::Receiver<String>,
    join_handle: Option<JoinHandle<()>>,
    stop_requested: Arc<AtomicBool>,
}

impl BackendRpcServerHandle {
    pub(crate) fn start(
        host: &str,
        port: u16,
        service: BackendRpcService,
        native_trust: Arc<NativeTrust>,
        native_transport: BackendNativeTransport,
    ) -> Result<Self, String> {
        let address = (host, port)
            .to_socket_addrs()
            .map_err(|error| format!("resolve native backend gRPC address {host}:{port}: {error}"))?
            .next()
            .ok_or_else(|| {
                format!("resolve native backend gRPC address {host}:{port}: no address")
            })?;
        let listener = TcpListener::bind(address)
            .map_err(|error| format!("bind native backend gRPC address {address}: {error}"))?;
        listener
            .set_nonblocking(true)
            .map_err(|error| format!("set native backend gRPC listener nonblocking: {error}"))?;
        let bound_addr = listener
            .local_addr()
            .map_err(|error| format!("read native backend gRPC bound address: {error}"))?;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (failure_tx, failure_rx) = mpsc::channel();
        let stop_requested = Arc::new(AtomicBool::new(false));
        let thread_stop_requested = Arc::clone(&stop_requested);
        let join_handle = std::thread::Builder::new()
            .name("native-backend-grpc".to_string())
            .spawn(move || {
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let runtime = tokio::runtime::Builder::new_multi_thread()
                        .enable_all()
                        .worker_threads(8)
                        .thread_stack_size(novarocks_types::WORKER_STACK_SIZE_BYTES)
                        .build()
                        .map_err(|error| format!("build native backend gRPC runtime: {error}"))?;
                    runtime.block_on(async move {
                        let listener = TokioTcpListener::from_std(listener).map_err(|error| {
                            format!("create Tokio native backend gRPC listener: {error}")
                        })?;
                        let service = NovaRocksGrpcServer::new(service)
                            .max_decoding_message_size(GRPC_MAX_MESSAGE_BYTES)
                            .max_encoding_message_size(GRPC_MAX_MESSAGE_BYTES);
                        let grpc_path = format!(
                            "/{}/*rest",
                            <NovaRocksGrpcServer<BackendRpcService> as NamedService>::NAME
                        );
                        let app = tower::ServiceExt::<
                            axum::http::Request<axum::body::Body>,
                        >::map_response(
                            Router::new()
                                .route_service(&grpc_path, AxumGrpcService::new(service))
                                .fallback(grpc_unimplemented_fallback),
                            |response: axum::http::Response<axum::body::Body>| {
                                response.map(boxed)
                            },
                        );
                        let app =
                            BackendListenerAuthService::new(app, native_trust.server_admission());
                        serve_native_listener(
                            listener,
                            app,
                            native_transport.incoming_adapter(),
                            shutdown_rx,
                        )
                        .await
                    })
                }));
                if thread_stop_requested.load(Ordering::Acquire) {
                    return;
                }
                let error = match outcome {
                    Ok(Ok(())) => "native backend gRPC server exited unexpectedly".to_string(),
                    Ok(Err(error)) => error,
                    Err(payload) => payload
                        .downcast_ref::<String>()
                        .cloned()
                        .or_else(|| {
                            payload
                                .downcast_ref::<&str>()
                                .map(|value| (*value).to_string())
                        })
                        .unwrap_or_else(|| "native backend gRPC server panicked".to_string()),
                };
                let _ = failure_tx.send(error);
            })
            .map_err(|error| format!("spawn native backend gRPC server: {error}"))?;
        Ok(Self {
            bound_addr,
            shutdown_tx: Some(shutdown_tx),
            failure_rx,
            join_handle: Some(join_handle),
            stop_requested,
        })
    }

    pub(crate) const fn bound_addr(&self) -> SocketAddr {
        self.bound_addr
    }

    pub(crate) fn poll_failure(&mut self) -> Result<Option<String>, String> {
        match self.failure_rx.try_recv() {
            Ok(error) => Ok(Some(error)),
            Err(mpsc::TryRecvError::Empty) => Ok(None),
            Err(mpsc::TryRecvError::Disconnected) => Ok(None),
        }
    }

    pub(crate) fn stop(&mut self) -> Result<(), String> {
        self.stop_requested.store(true, Ordering::Release);
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(true);
        }
        if let Some(join_handle) = self.join_handle.take() {
            join_handle
                .join()
                .map_err(|_| "native backend gRPC server thread panicked".to_string())?;
        }
        Ok(())
    }
}

async fn serve_native_listener<S>(
    listener: TokioTcpListener,
    app: S,
    incoming: NativeIncomingAdapter,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<(), String>
where
    S: Service<
            axum::http::Request<axum::body::Body>,
            Response = axum::http::Response<tonic::body::BoxBody>,
            Error = std::convert::Infallible,
        > + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    loop {
        tokio::select! {
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    return Ok(());
                }
            }
            accepted = listener.accept() => {
                let (stream, _) = accepted
                    .map_err(|error| format!("accept native backend gRPC connection: {error}"))?;
                let app = app.clone();
                let incoming = incoming.clone();
                tokio::spawn(async move {
                    let stream = match incoming.accept(stream).await {
                        Ok(stream) => stream,
                        Err(_) => {
                            crate::metrics::record_backend_native_tls_handshake_failure();
                            return;
                        }
                    };
                    let service = service_fn(move |request: hyper::Request<hyper::body::Incoming>| {
                        let app = app.clone();
                        async move {
                            let response = app
                                .oneshot(request.map(axum::body::Body::new))
                                .await
                                .expect("backend Native route service is infallible");
                            Ok::<_, std::convert::Infallible>(response)
                        }
                    });
                    let _ = http2::Builder::new(TokioExecutor::new())
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        }
    }
}

#[derive(Clone)]
struct BackendListenerAuthService<S> {
    admission: NativeServerAdmission,
    inner: S,
}

impl<S> BackendListenerAuthService<S> {
    fn new(inner: S, admission: NativeServerAdmission) -> Self {
        Self { admission, inner }
    }
}

impl<S, Body> Service<axum::http::Request<Body>> for BackendListenerAuthService<S>
where
    S: Service<axum::http::Request<Body>, Response = axum::http::Response<tonic::body::BoxBody>>
        + Send,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
{
    type Response = axum::http::Response<tonic::body::BoxBody>;
    type Error = S::Error;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, request: axum::http::Request<Body>) -> Self::Future {
        if self.admission.admit_headers(request.headers()).is_err() {
            crate::metrics::record_backend_native_authentication_failure();
            return Box::pin(async {
                Ok(
                    tonic::Status::unauthenticated("native caller authentication failed")
                        .into_http(),
                )
            });
        }
        Box::pin(self.inner.call(request))
    }
}

impl Drop for BackendRpcServerHandle {
    fn drop(&mut self) {
        let _ = self.stop();
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

    fn call(&mut self, request: axum::http::Request<axum::body::Body>) -> Self::Future {
        self.inner.call(request.map(boxed))
    }
}
