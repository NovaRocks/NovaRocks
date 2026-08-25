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

use crate::metrics::handle_metrics;
use crate::rpc::data_plane::BackendDataPlane;
use axum::Router;
use axum::http::{HeaderValue, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use novarocks_execution::runtime::fragment::io::ExchangeReceiverPort;
use novarocks_proto::{filter, novarocks as proto};
use tokio::net::TcpListener as TokioTcpListener;
use tokio::sync::watch;
use tokio_stream::wrappers::ReceiverStream;
use tonic::body::boxed;
use tonic::codegen::Service;
use tonic::server::NamedService;

use super::transport::nova_rocks_grpc_server::{NovaRocksGrpc, NovaRocksGrpcServer};
use crate::connector::binding_decode;
use crate::fragment::ingress::NativeFragmentIngress;
use crate::query_lifecycle::QueryLifecycleIngress;
use crate::query_lifecycle::rpc::{
    QueryControlResponseStream, handle_abort_query, handle_init_query, handle_query_control_stream,
    handle_stage_fragments, handle_start_prepared_query, status_from_lifecycle_error,
};
use crate::runtime_filter::rpc::{
    BackendRuntimeFilterEnvelopeIngress, handle_runtime_filter_envelope,
};

const GRPC_MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;

/// Backend-owned production Tonic service. Domain owners contribute the narrow
/// ingress ports while this service composes them with `BackendDataPlane`.
#[derive(Clone)]
pub(crate) struct BackendRpcService {
    native_fragment_ingress: Arc<dyn NativeFragmentIngress>,
    query_lifecycle_ingress: Arc<dyn QueryLifecycleIngress>,
    query_control_shutdown: Option<watch::Receiver<bool>>,
    data_plane: BackendDataPlane,
    runtime_filter_ingress: Arc<dyn BackendRuntimeFilterEnvelopeIngress>,
}

impl BackendRpcService {
    pub(crate) fn new(
        native_fragment_ingress: Arc<dyn NativeFragmentIngress>,
        query_lifecycle_ingress: Arc<dyn QueryLifecycleIngress>,
        runtime_filter_ingress: Arc<dyn BackendRuntimeFilterEnvelopeIngress>,
        exchange_receiver_port: Arc<dyn ExchangeReceiverPort>,
    ) -> Self {
        Self {
            native_fragment_ingress,
            query_lifecycle_ingress,
            query_control_shutdown: None,
            data_plane: BackendDataPlane::with_exchange_receiver_port(exchange_receiver_port),
            runtime_filter_ingress,
        }
    }

    fn with_query_control_shutdown(mut self, shutdown: watch::Receiver<bool>) -> Self {
        self.query_control_shutdown = Some(shutdown);
        self
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
    type QueryControlStreamStream = std::pin::Pin<
        Box<
            dyn tokio_stream::Stream<Item = Result<proto::QueryControlResponse, tonic::Status>>
                + Send
                + 'static,
        >,
    >;

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

    async fn ensure_connector_execution_binding(
        &self,
        request: tonic::Request<proto::EnsureConnectorExecutionBindingRequest>,
    ) -> Result<tonic::Response<proto::EnsureConnectorExecutionBindingResponse>, tonic::Status>
    {
        let ingress = Arc::clone(&self.native_fragment_ingress);
        let result =
            tokio::task::spawn_blocking(move || {
                match binding_decode::decode_ensure_request(request.into_inner()) {
                    Ok((execution_id, declaration)) => {
                        ingress.ensure_connector_execution_binding(execution_id, declaration)
                    }
                    Err(rejection) => rejection,
                }
            })
            .await
            .map_err(|error| {
                tonic::Status::internal(format!(
                    "ensure_connector_execution_binding handler panicked: {error}"
                ))
            })?;
        Ok(tonic::Response::new(result.to_proto()))
    }

    async fn retire_connector_execution_binding(
        &self,
        request: tonic::Request<proto::RetireConnectorExecutionBindingRequest>,
    ) -> Result<tonic::Response<proto::RetireConnectorExecutionBindingResponse>, tonic::Status>
    {
        let ingress = Arc::clone(&self.native_fragment_ingress);
        let result =
            tokio::task::spawn_blocking(move || {
                match binding_decode::decode_retire_request(request.into_inner()) {
                    Ok(key) => ingress.retire_connector_execution_binding(key),
                    Err(outcome) => outcome,
                }
            })
            .await
            .map_err(|error| {
                tonic::Status::internal(format!(
                    "retire_connector_execution_binding handler panicked: {error}"
                ))
            })?;
        Ok(tonic::Response::new(result.to_proto()))
    }

    async fn heartbeat(
        &self,
        request: tonic::Request<proto::HeartbeatRequest>,
    ) -> Result<tonic::Response<proto::HeartbeatResponse>, tonic::Status> {
        let request = request.into_inner();
        self.query_lifecycle_ingress
            .bind_backend_identity(u64::from(request.assigned_be_id))
            .map_err(status_from_lifecycle_error)?;
        crate::runtime::backend_id::set_backend_id(i64::from(request.assigned_be_id));
        let num_cores = std::thread::available_parallelism()
            .map(|count| count.get() as u32)
            .unwrap_or(1);
        Ok(tonic::Response::new(proto::HeartbeatResponse {
            start_epoch: crate::runtime::start_epoch::start_epoch(),
            version: novarocks_version::native_build_identity().to_string(),
            num_cores,
            status_code: 0,
        }))
    }

    async fn init_query(
        &self,
        request: tonic::Request<proto::InitQueryRequest>,
    ) -> Result<tonic::Response<proto::InitQueryResponse>, tonic::Status> {
        let ingress = Arc::clone(&self.query_lifecycle_ingress);
        let response = tokio::task::spawn_blocking(move || {
            handle_init_query(ingress.as_ref(), request.into_inner())
        })
        .await
        .map_err(|error| {
            tonic::Status::internal(format!("init_query handler panicked: {error}"))
        })??;
        Ok(tonic::Response::new(response))
    }

    async fn stage_fragments(
        &self,
        request: tonic::Request<proto::StageFragmentsRequest>,
    ) -> Result<tonic::Response<proto::StageFragmentsResponse>, tonic::Status> {
        let ingress = Arc::clone(&self.query_lifecycle_ingress);
        let response = tokio::task::spawn_blocking(move || {
            handle_stage_fragments(ingress.as_ref(), request.into_inner())
        })
        .await
        .map_err(|error| {
            tonic::Status::internal(format!("stage_fragments handler panicked: {error}"))
        })??;
        Ok(tonic::Response::new(response))
    }

    async fn start_prepared_query(
        &self,
        request: tonic::Request<proto::StartPreparedQueryRequest>,
    ) -> Result<tonic::Response<proto::StartPreparedQueryResponse>, tonic::Status> {
        let ingress = Arc::clone(&self.query_lifecycle_ingress);
        let response = tokio::task::spawn_blocking(move || {
            handle_start_prepared_query(ingress.as_ref(), request.into_inner())
        })
        .await
        .map_err(|error| {
            tonic::Status::internal(format!("start_prepared_query handler panicked: {error}"))
        })??;
        Ok(tonic::Response::new(response))
    }

    async fn abort_query(
        &self,
        request: tonic::Request<proto::AbortQueryRequest>,
    ) -> Result<tonic::Response<proto::AbortQueryResponse>, tonic::Status> {
        let ingress = Arc::clone(&self.query_lifecycle_ingress);
        let response = tokio::task::spawn_blocking(move || {
            handle_abort_query(ingress.as_ref(), request.into_inner())
        })
        .await
        .map_err(|error| {
            tonic::Status::internal(format!("abort_query handler panicked: {error}"))
        })??;
        Ok(tonic::Response::new(response))
    }

    async fn query_control_stream(
        &self,
        request: tonic::Request<tonic::Streaming<proto::QueryControlRequest>>,
    ) -> Result<tonic::Response<Self::QueryControlStreamStream>, tonic::Status> {
        let stream: QueryControlResponseStream = handle_query_control_stream(
            Arc::clone(&self.query_lifecycle_ingress),
            request.into_inner(),
            self.query_control_shutdown.clone(),
        )
        .await?;
        Ok(tonic::Response::new(Box::pin(stream)))
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
    pub(crate) fn start(host: &str, port: u16, service: BackendRpcService) -> Result<Self, String> {
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
                        let service = NovaRocksGrpcServer::new(
                            service.with_query_control_shutdown(shutdown_rx.clone()),
                        )
                        .max_decoding_message_size(GRPC_MAX_MESSAGE_BYTES)
                        .max_encoding_message_size(GRPC_MAX_MESSAGE_BYTES);
                        let mut shutdown_rx = shutdown_rx;
                        let grpc_path = format!(
                            "/{}/*rest",
                            <NovaRocksGrpcServer<BackendRpcService> as NamedService>::NAME
                        );
                        let app = Router::new()
                            .route_service(&grpc_path, AxumGrpcService::new(service))
                            .route("/metrics", get(handle_metrics))
                            .fallback(grpc_unimplemented_fallback);
                        axum::serve(listener, app)
                            .with_graceful_shutdown(async move {
                                while !*shutdown_rx.borrow() {
                                    if shutdown_rx.changed().await.is_err() {
                                        break;
                                    }
                                }
                            })
                            .await
                            .map_err(|error| {
                                format!("native backend gRPC serve future failed: {error}")
                            })
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
