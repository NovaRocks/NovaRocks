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

//! Production native-BE gRPC service and its instance-owned listener.
//!
//! The generated service is intentionally owned by `novarocks-backend`.  The
//! core service remains the compatibility-neutral implementation while the
//! closeout migrates individual execution adapters behind this backend entry
//! point; no process-global listener state is used here.

use std::net::{SocketAddr, TcpListener, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::thread::JoinHandle;

use novarocks::query_execution::lifecycle::{
    QueryLifecycleIngress, QueryTerminalIngress, QueryTerminalReportOutcome,
    decode_query_terminal_snapshot,
};
use novarocks::runtime_filter_transition::port::transport::RuntimeFilterEnvelopeIngress;
use novarocks::service::native_data_plane::NativeDataPlaneKernel;
use novarocks_protocol::{filter, novarocks as proto};
use tokio::net::TcpListener as TokioTcpListener;
use tokio::sync::watch;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::wrappers::TcpListenerStream;

use super::ingress::NativeFragmentIngress;
use super::lifecycle_adapter::{
    QueryControlResponseStream, handle_abort_query, handle_init_query, handle_query_control_stream,
    handle_stage_fragments, handle_start_prepared_query, status_from_lifecycle_error,
};
use super::runtime_filter_adapter::handle_runtime_filter_envelope;
use super::transport::nova_rocks_grpc_server::{NovaRocksGrpc, NovaRocksGrpcServer};

const GRPC_MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;

/// Backend-owned production Tonic service.  Core contributes only the narrow
/// data-plane kernel and protocol-neutral lifecycle/report ports.
#[derive(Clone)]
pub(crate) struct NativeBackendGrpcService {
    native_fragment_ingress: Arc<dyn NativeFragmentIngress>,
    query_lifecycle_ingress: Arc<dyn QueryLifecycleIngress>,
    query_control_shutdown: Option<watch::Receiver<bool>>,
    terminal_ingress: Option<Arc<dyn QueryTerminalIngress>>,
    data_plane: NativeDataPlaneKernel,
    runtime_filter_ingress: Arc<dyn RuntimeFilterEnvelopeIngress>,
}

impl NativeBackendGrpcService {
    pub(crate) fn new(
        native_fragment_ingress: Arc<dyn NativeFragmentIngress>,
        query_lifecycle_ingress: Arc<dyn QueryLifecycleIngress>,
        terminal_ingress: Option<Arc<dyn QueryTerminalIngress>>,
        runtime_filter_ingress: Arc<dyn RuntimeFilterEnvelopeIngress>,
    ) -> Self {
        Self {
            native_fragment_ingress,
            query_lifecycle_ingress,
            query_control_shutdown: None,
            terminal_ingress,
            data_plane: NativeDataPlaneKernel::query_scoped(),
            runtime_filter_ingress,
        }
    }

    fn with_query_control_shutdown(mut self, shutdown: watch::Receiver<bool>) -> Self {
        self.query_control_shutdown = Some(shutdown);
        self
    }
}

#[tonic::async_trait]
impl NovaRocksGrpc for NativeBackendGrpcService {
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
        let result = tokio::task::spawn_blocking(move || {
            let (execution_id, declaration) =
                super::connector_binding::decode_ensure_request(request.into_inner())
                    .map_err(|error| error.to_string())?;
            let context = super::connector_binding::install_request_context()
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
        let (status_code, message) = result
            .map(|()| (0, String::new()))
            .unwrap_or_else(|error| (1, error));
        Ok(tonic::Response::new(
            proto::EnsureConnectorExecutionBindingResponse {
                status_code,
                message,
            },
        ))
    }

    async fn retire_connector_execution_binding(
        &self,
        request: tonic::Request<proto::RetireConnectorExecutionBindingRequest>,
    ) -> Result<tonic::Response<proto::RetireConnectorExecutionBindingResponse>, tonic::Status>
    {
        let ingress = Arc::clone(&self.native_fragment_ingress);
        let result = tokio::task::spawn_blocking(move || {
            let key = super::connector_binding::decode_retire_request(request.into_inner())
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
        let (status_code, message) = result
            .map(|()| (0, String::new()))
            .unwrap_or_else(|error| (1, error));
        Ok(tonic::Response::new(
            proto::RetireConnectorExecutionBindingResponse {
                status_code,
                message,
            },
        ))
    }

    async fn heartbeat(
        &self,
        request: tonic::Request<proto::HeartbeatRequest>,
    ) -> Result<tonic::Response<proto::HeartbeatResponse>, tonic::Status> {
        let request = request.into_inner();
        self.query_lifecycle_ingress
            .bind_backend_identity(u64::from(request.assigned_be_id))
            .map_err(status_from_lifecycle_error)?;
        novarocks::runtime::backend_id::set_backend_id(i64::from(request.assigned_be_id));
        let num_cores = std::thread::available_parallelism()
            .map(|count| count.get() as u32)
            .unwrap_or(1);
        Ok(tonic::Response::new(proto::HeartbeatResponse {
            start_epoch: novarocks::runtime::start_epoch::start_epoch(),
            version: novarocks::version::short_version().to_string(),
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
        let Some(ingress) = self.terminal_ingress.clone() else {
            return Ok(tonic::Response::new(proto::ReportQueryTerminalResponse {
                outcome: proto::ReportQueryTerminalOutcome::RejectedGone as i32,
                detail: "query terminal ingress is not installed for this role".to_string(),
            }));
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
            QueryTerminalReportOutcome::Accepted => proto::ReportQueryTerminalOutcome::Accepted,
            QueryTerminalReportOutcome::AlreadyAccepted => {
                proto::ReportQueryTerminalOutcome::AlreadyAccepted
            }
            QueryTerminalReportOutcome::RejectedConflict => {
                proto::ReportQueryTerminalOutcome::RejectedConflict
            }
            QueryTerminalReportOutcome::RejectedGone => {
                proto::ReportQueryTerminalOutcome::RejectedGone
            }
        };
        Ok(tonic::Response::new(proto::ReportQueryTerminalResponse {
            outcome: outcome as i32,
            detail: ack.detail().to_string(),
        }))
    }
}

/// A backend application owns exactly one native listener.  Unlike the legacy
/// core listener, this handle has no global reservation or shutdown state.
pub(crate) struct NativeGrpcServerHandle {
    bound_addr: SocketAddr,
    shutdown_tx: Option<watch::Sender<bool>>,
    failure_rx: mpsc::Receiver<String>,
    join_handle: Option<JoinHandle<()>>,
    stop_requested: Arc<AtomicBool>,
}

impl NativeGrpcServerHandle {
    pub(crate) fn start(
        host: &str,
        port: u16,
        service: NativeBackendGrpcService,
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
                        .thread_stack_size(
                            novarocks::runtime::global_async_runtime::WORKER_STACK_SIZE_BYTES,
                        )
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
                        tonic::transport::Server::builder()
                            .add_service(service)
                            .serve_with_incoming_shutdown(
                                TcpListenerStream::new(listener),
                                async move {
                                    while !*shutdown_rx.borrow() {
                                        if shutdown_rx.changed().await.is_err() {
                                            break;
                                        }
                                    }
                                },
                            )
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

impl Drop for NativeGrpcServerHandle {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}
