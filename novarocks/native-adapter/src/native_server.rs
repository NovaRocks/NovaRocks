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

//! Native inbound RPC listener over a Server-resolved transport capability.

use std::net::{SocketAddr, TcpListener, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
use std::task::{Context, Poll};
use std::thread::JoinHandle;
use std::time::Duration;

#[cfg(test)]
#[path = "native_control_listener_tests.rs"]
mod control_listener_tests;

#[cfg(test)]
#[path = "native_incoming_seal_tests.rs"]
mod incoming_seal_tests;

use axum::Router;
use hyper::server::conn::http2;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use novarocks_execution_contract::native_result_support::NativeResultSupportGeometry;
use novarocks_native_trust::{NativeIncomingAdapter, NativeServerAdmission, NativeTrust};
use novarocks_proto_codec::native_rpc::{NativeEndpointDomain, NativeRpcMethod};
use tokio::net::TcpListener as TokioTcpListener;
use tokio::sync::{Notify, watch};
use tonic::body::boxed;
use tonic::codegen::Service;
use tonic::server::NamedService;
use tower::{Layer, ServiceExt};

use crate::generated::nova_rocks_grpc_server::{NovaRocksGrpc, NovaRocksGrpcServer};
use crate::native_incoming_key_capacity::NativeIncomingKey;
use crate::native_ingress::NativeIngressService;
use crate::native_transport_admission::{
    NativeHandshakePermit, NativeIncomingConnectionBinding, NativeTransportAdmission,
    TransportClass, TransportRole,
};

/// How long a stopping listener lets its already-accepted connections finish.
///
/// This covers answering a request that was already dispatched -- the root
/// result poll waits at most 200ms, and task operations answer immediately.
/// It deliberately does not cover draining a long-lived stream: `Exchange` is
/// a bidirectional data pipe that GOAWAY does not end, and cutting it is what
/// cancelling it means. Anything still open when this expires is cancelled,
/// so the bound is what keeps one open stream from holding a stopping process.
const SHUTDOWN_DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);

/// How long to wait after a failed `accept` before trying again, so a full
/// descriptor table cannot turn this loop into a spin.
const ACCEPT_ERROR_BACKOFF: std::time::Duration = std::time::Duration::from_millis(100);

/// How many `accept` calls may fail in a row before the listener is reported
/// as broken. One transient refusal resets the count; a listener that can no
/// longer accept anything still reaches supervision.
const MAX_CONSECUTIVE_ACCEPT_ERRORS: u32 = 64;

const GRPC_MAX_MESSAGE_BYTES: usize =
    novarocks_task_codec::operation::NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES;

/// Server-validated limits for one Native listener. The listener owns only
/// local transport and scheduling capacity, not FE query admission or Worker
/// context reservations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NativeIngressConfig {
    pub worker_threads: usize,
    pub max_blocking_threads: usize,
    pub ordinary_running: usize,
    pub ordinary_waiting: usize,
    pub control_worker_threads: usize,
    pub control_running: usize,
    pub control_waiting: usize,
    pub ordinary_request_max_bytes: usize,
    pub ordinary_response_max_bytes: usize,
    pub control_request_max_bytes: usize,
    pub control_response_max_bytes: usize,
}

impl Default for NativeIngressConfig {
    fn default() -> Self {
        Self {
            worker_threads: 8,
            max_blocking_threads: 64,
            ordinary_running: 8,
            ordinary_waiting: 8,
            control_worker_threads: 4,
            control_running: 4,
            control_waiting: 4,
            ordinary_request_max_bytes: GRPC_MAX_MESSAGE_BYTES,
            ordinary_response_max_bytes: GRPC_MAX_MESSAGE_BYTES,
            control_request_max_bytes: 1024 * 1024,
            control_response_max_bytes: GRPC_MAX_MESSAGE_BYTES,
        }
    }
}

/// The admission class a listener serves. Each class has its own physical
/// connection and handshake positions; one class never borrows the other's.
#[derive(Clone)]
pub(crate) struct ListenerAdmission {
    pub(crate) admission: NativeTransportAdmission,
    pub(crate) class: TransportClass,
    /// The Backend data listener's bounded root reader, if it serves roots.
    pub(crate) root_results: Option<Arc<crate::root_result_reader::NativeRootResultReader>>,
}

/// Instance-owned native gRPC listener lifecycle.
///
/// The role provides its generated service and owns every domain handler. This
/// adapter owns only the generic socket, authenticated transport, h2 serving,
/// shutdown, and supervision mechanics.
pub struct NativeRpcListener {
    bound_addr: SocketAddr,
    shutdown_tx: Option<watch::Sender<bool>>,
    failure_rx: mpsc::Receiver<String>,
    join_handle: Option<JoinHandle<()>>,
    stop_requested: Arc<AtomicBool>,
}

impl NativeRpcListener {
    #[expect(
        clippy::too_many_arguments,
        reason = "role identity and its authentication metric remain explicit composition inputs"
    )]
    pub fn start_frontend_membership<S, F, H>(
        host: &str,
        port: u16,
        service: S,
        native_trust: Arc<NativeTrust>,
        incoming_adapter: NativeIncomingAdapter,
        role_label: &'static str,
        thread_name: &'static str,
        on_authentication_failure: F,
        on_transport_handshake_failure: H,
        ingress_config: NativeIngressConfig,
        admission: NativeTransportAdmission,
    ) -> Result<Self, String>
    where
        S: NovaRocksGrpc + Clone + Send + Sync + 'static,
        F: Fn() + Send + Sync + 'static,
        H: Fn() + Send + Sync + 'static,
    {
        Self::start_inner(
            host,
            port,
            service,
            native_trust,
            incoming_adapter,
            role_label,
            NativeEndpointDomain::FrontendMembership,
            thread_name,
            on_authentication_failure,
            on_transport_handshake_failure,
            ingress_config,
            ListenerAdmission {
                admission,
                class: TransportClass::Membership,
                root_results: None,
            },
        )
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "role identity and its authentication metric remain explicit composition inputs"
    )]
    pub(crate) fn start_with_admission<S, F, H>(
        host: &str,
        port: u16,
        service: S,
        native_trust: Arc<NativeTrust>,
        incoming_adapter: NativeIncomingAdapter,
        role_label: &'static str,
        domain: NativeEndpointDomain,
        thread_name: &'static str,
        on_authentication_failure: F,
        on_transport_handshake_failure: H,
        ingress_config: NativeIngressConfig,
        admission: NativeTransportAdmission,
        class: TransportClass,
        root_results: Option<Arc<crate::root_result_reader::NativeRootResultReader>>,
    ) -> Result<Self, String>
    where
        S: NovaRocksGrpc + Clone + Send + Sync + 'static,
        F: Fn() + Send + Sync + 'static,
        H: Fn() + Send + Sync + 'static,
    {
        Self::start_inner(
            host,
            port,
            service,
            native_trust,
            incoming_adapter,
            role_label,
            domain,
            thread_name,
            on_authentication_failure,
            on_transport_handshake_failure,
            ingress_config,
            ListenerAdmission {
                admission,
                class,
                root_results,
            },
        )
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "role identity and its authentication metric remain explicit composition inputs"
    )]
    #[expect(
        clippy::disallowed_methods,
        reason = "The native listener owns its runtime service task; see ADR-0218."
    )]
    fn start_inner<S, F, H>(
        host: &str,
        port: u16,
        service: S,
        native_trust: Arc<NativeTrust>,
        incoming_adapter: NativeIncomingAdapter,
        role_label: &'static str,
        domain: NativeEndpointDomain,
        thread_name: &'static str,
        on_authentication_failure: F,
        on_transport_handshake_failure: H,
        ingress_config: NativeIngressConfig,
        admission: ListenerAdmission,
    ) -> Result<Self, String>
    where
        S: NovaRocksGrpc + Clone + Send + Sync + 'static,
        F: Fn() + Send + Sync + 'static,
        H: Fn() + Send + Sync + 'static,
    {
        {
            let listener_admission = &admission;
            let compatible = matches!(
                (
                    domain,
                    listener_admission.class,
                    listener_admission.admission.role()
                ),
                (
                    NativeEndpointDomain::BackendData,
                    TransportClass::Data,
                    TransportRole::Backend
                ) | (
                    NativeEndpointDomain::BackendControl,
                    TransportClass::Control,
                    TransportRole::Backend
                ) | (
                    NativeEndpointDomain::FrontendMembership,
                    TransportClass::Membership,
                    TransportRole::Frontend
                )
            );
            if !compatible {
                return Err("native endpoint domain and transport admission class disagree".into());
            }
        }
        let descriptor_capacity =
            crate::native_fd_capacity::verify_native_file_descriptor_capacity(domain).map_err(
                |error| format!("verify native {role_label} descriptor capacity: {error}"),
            )?;
        tracing::info!(
            role = role_label,
            endpoint_domain = ?domain,
            descriptor_capacity = ?descriptor_capacity,
            "native listener descriptor baseline verified"
        );
        let address = (host, port)
            .to_socket_addrs()
            .map_err(|error| {
                format!("resolve native {role_label} gRPC address {host}:{port}: {error}")
            })?
            .next()
            .ok_or_else(|| {
                format!("resolve native {role_label} gRPC address {host}:{port}: no address")
            })?;
        let listener = TcpListener::bind(address)
            .map_err(|error| format!("bind native {role_label} gRPC address {address}: {error}"))?;
        listener.set_nonblocking(true).map_err(|error| {
            format!("set native {role_label} gRPC listener nonblocking: {error}")
        })?;
        let bound_addr = listener
            .local_addr()
            .map_err(|error| format!("read native {role_label} gRPC bound address: {error}"))?;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (failure_tx, failure_rx) = mpsc::channel();
        let stop_requested = Arc::new(AtomicBool::new(false));
        let thread_stop_requested = Arc::clone(&stop_requested);
        let authentication_failure = Arc::new(on_authentication_failure);
        let transport_handshake_failure = Arc::new(on_transport_handshake_failure);
        let join_handle = std::thread::Builder::new()
            .name(thread_name.to_string())
            .spawn(move || {
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let runtime = tokio::runtime::Builder::new_multi_thread()
                        .enable_all()
                        .worker_threads(match domain {
                            NativeEndpointDomain::BackendControl => ingress_config.control_worker_threads,
                            _ => ingress_config.worker_threads,
                        })
                        .max_blocking_threads(ingress_config.max_blocking_threads)
                        .thread_stack_size(novarocks_types::WORKER_STACK_SIZE_BYTES)
                        .build()
                        .map_err(|error| {
                            format!("build native {role_label} gRPC runtime: {error}")
                        })?;
                    let outcome = runtime.block_on(async move {
                        let listener = TokioTcpListener::from_std(listener).map_err(|error| {
                            format!("create Tokio native {role_label} gRPC listener: {error}")
                        })?;
                        // The explicit endpoint domain owns runtime admission
                        // and generated-service limits. Labels are diagnostic.
                        let (request_limit, response_limit) = match domain {
                            NativeEndpointDomain::BackendData => (
                                ingress_config.ordinary_request_max_bytes,
                                ingress_config.ordinary_response_max_bytes,
                            ),
                            NativeEndpointDomain::BackendControl => (
                                ingress_config.control_request_max_bytes,
                                ingress_config.control_response_max_bytes,
                            ),
                            NativeEndpointDomain::FrontendMembership => (
                                ingress_config.ordinary_request_max_bytes,
                                ingress_config.ordinary_response_max_bytes,
                            ),
                        };
                        let ordinary_service = NovaRocksGrpcServer::new(service.clone())
                            .max_decoding_message_size(request_limit)
                            .max_encoding_message_size(response_limit);
                        let control_service = NovaRocksGrpcServer::new(service)
                            .max_decoding_message_size(request_limit)
                            .max_encoding_message_size(response_limit);
                        let grpc_path = format!(
                            "/{}/*rest",
                            <NovaRocksGrpcServer<S> as NamedService>::NAME
                        );
                        let control_path = format!(
                            "/{}/ApplyTaskControlOperations",
                            <NovaRocksGrpcServer<S> as NamedService>::NAME
                        );
                        let mut router = Router::new()
                            .route_service(&control_path, AxumGrpcService::new(control_service));
                        // Bounded root reads bypass the generated service: the
                        // root reader keeps each response body's owner until
                        // the body exits.
                        if let Some(reader) = admission.root_results.clone()
                        {
                            router = router.route_service(
                                novarocks_proto_codec::native_rpc::NativeRpcMethod::FetchRootResult
                                    .contract()
                                    .path,
                                RootResultRoute { reader },
                            );
                        }
                        let app = tower::ServiceExt::<axum::http::Request<axum::body::Body>>::map_response(
                            router
                                .route_service(&grpc_path, AxumGrpcService::new(ordinary_service))
                                .fallback(grpc_unimplemented_fallback),
                            box_native_response as NativeResponseMapper,
                        );
                        let app = NativeIngressService::new(
                            app,
                            ingress_config,
                            <NovaRocksGrpcServer<S> as NamedService>::NAME,
                            domain != NativeEndpointDomain::FrontendMembership,
                            domain,
                        )
                        .with_lane_streams(
                            Some(admission.admission.clone()),
                        );
                        let app = native_trust
                            .server_admission()
                            .listener_layer()
                            .with_authentication_failure_observer(authentication_failure)
                            .layer(app);
                        serve_native_listener(
                            listener,
                            app,
                            incoming_adapter,
                            shutdown_rx,
                            transport_handshake_failure,
                            role_label,
                            Some(admission),
                            Some((native_trust.server_admission(), domain)),
                        )
                        .await
                    });
                    // Dropping this runtime cancels every connection task it
                    // still owns. Each one reports itself on the way out.
                    drop(runtime);
                    tracing::debug!(
                        role = role_label,
                        "native listener runtime dropped"
                    );
                    outcome
                }));
                if thread_stop_requested.load(Ordering::Acquire) {
                    return;
                }
                let error = match outcome {
                    Ok(Ok(())) => format!("native {role_label} gRPC server exited unexpectedly"),
                    Ok(Err(error)) => error,
                    Err(payload) => payload
                        .downcast_ref::<String>()
                        .cloned()
                        .or_else(|| payload.downcast_ref::<&str>().map(|value| (*value).to_string()))
                        .unwrap_or_else(|| format!("native {role_label} gRPC server panicked")),
                };
                let _ = failure_tx.send(error);
            })
            .map_err(|error| format!("spawn native {role_label} gRPC server: {error}"))?;
        Ok(Self {
            bound_addr,
            shutdown_tx: Some(shutdown_tx),
            failure_rx,
            join_handle: Some(join_handle),
            stop_requested,
        })
    }

    pub const fn bound_addr(&self) -> SocketAddr {
        self.bound_addr
    }

    pub fn poll_failure(&mut self) -> Result<Option<String>, String> {
        match self.failure_rx.try_recv() {
            Ok(error) => Ok(Some(error)),
            Err(mpsc::TryRecvError::Empty | mpsc::TryRecvError::Disconnected) => Ok(None),
        }
    }

    /// Close this listener's admission before waiting for its runtime to exit.
    /// Multiple independently owned listeners can all begin stopping first.
    pub fn begin_stop(&mut self) {
        self.stop_requested.store(true, Ordering::Release);
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(true);
        }
    }

    pub fn stop(&mut self) -> Result<(), String> {
        self.begin_stop();
        if let Some(join_handle) = self.join_handle.take() {
            join_handle
                .join()
                .map_err(|_| "native gRPC server thread panicked".to_string())?;
        }
        Ok(())
    }
}

impl Drop for NativeRpcListener {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

type NativeResponseMapper =
    fn(axum::http::Response<axum::body::Body>) -> axum::http::Response<tonic::body::BoxBody>;

fn box_native_response(
    response: axum::http::Response<axum::body::Body>,
) -> axum::http::Response<tonic::body::BoxBody> {
    response.map(boxed)
}

/// Apply the frozen public HTTP/2 limits to one Native connection, admitted or
/// not. These bound the counts and per-item sizes inside Hyper/H2; the bytes
/// those libraries allocate under them are covered by the transport
/// measurement gate.
fn configure_native_server(builder: &mut http2::Builder<TokioExecutor>) {
    let g = NativeResultSupportGeometry::V1;
    builder
        .initial_stream_window_size(g.transport_h2_stream_receive_window_bytes as u32)
        .initial_connection_window_size(g.transport_h2_connection_receive_window_bytes as u32)
        .adaptive_window(g.transport_h2_adaptive_window)
        .max_concurrent_streams(g.transport_streams_per_connection as u32)
        .max_pending_accept_reset_streams(g.transport_h2_pending_resets as usize)
        .max_local_error_reset_streams(g.transport_h2_pending_resets as usize)
        .max_frame_size(g.transport_h2_frame_bytes as u32)
        .max_header_list_size(g.transport_h2_header_bytes as u32)
        .max_send_buf_size(g.transport_h2_send_buffer_bytes as usize);
}

/// Per-connection request-head admission for admitted listeners.
///
/// The first request whose method belongs to this endpoint and whose caller
/// authenticates seals the connection to that caller process and traffic
/// class, and ends the connection's bootstrap: its handshake position is
/// returned. A later request with another key, or an authenticated caller
/// without a process identity, closes the connection. Unknown methods and
/// authentication failures are left to the routes behind this layer.
#[derive(Clone)]
struct NativeIncomingHeadAdmission {
    binding: NativeIncomingConnectionBinding,
    admission: NativeServerAdmission,
    domain: NativeEndpointDomain,
    handshake: NativeHandshakePermit,
    close: Arc<Notify>,
}

impl NativeIncomingHeadAdmission {
    fn admit(&self, uri: &axum::http::Uri, headers: &axum::http::HeaderMap) -> Result<(), ()> {
        let Some(method) = NativeRpcMethod::from_path(uri.path()) else {
            return Ok(());
        };
        if !method.is_allowed_at(self.domain) {
            return Ok(());
        }
        let Ok(caller) = self.admission.admit_headers(headers) else {
            return Ok(());
        };
        let sealed = caller
            .process_identity()
            .ok_or(())
            .and_then(|peer| {
                NativeIncomingKey::new(peer, self.domain, method.contract().traffic).map_err(|_| ())
            })
            .and_then(|key| self.binding.seal(key).map_err(|_| ()));
        match sealed {
            Ok(()) => {
                self.handshake.release();
                Ok(())
            }
            Err(()) => {
                self.close.notify_one();
                Err(())
            }
        }
    }
}

struct ConnectionInputs<S> {
    stream: tokio::net::TcpStream,
    app: S,
    incoming: NativeIncomingAdapter,
    admitted: Option<AdmittedConnection>,
    served: ServedConnection,
    drain: watch::Receiver<()>,
    on_transport_handshake_failure: Arc<dyn Fn() + Send + Sync>,
}

struct AdmittedConnection {
    connection: crate::native_transport_admission::NativeConnectionPermit,
    head: NativeIncomingHeadAdmission,
    bootstrap_deadline: tokio::time::Instant,
}

async fn serve_connection<S>(inputs: ConnectionInputs<S>)
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
    let ConnectionInputs {
        stream,
        app,
        incoming,
        admitted,
        mut served,
        mut drain,
        on_transport_handshake_failure,
    } = inputs;
    let deadline = admitted
        .as_ref()
        .map(|admitted| admitted.bootstrap_deadline);
    let accepted = match deadline {
        Some(deadline) => match tokio::time::timeout_at(deadline, incoming.accept(stream)).await {
            Ok(accepted) => accepted.map_err(|error| format!("{error:?}")),
            Err(_) => Err("native bootstrap deadline elapsed during TLS".to_owned()),
        },
        None => incoming
            .accept(stream)
            .await
            .map_err(|error| format!("{error:?}")),
    };
    let stream = match accepted {
        Ok(stream) => stream,
        Err(error) => {
            served.close("transport_handshake", &error);
            on_transport_handshake_failure();
            return;
        }
    };
    let (stream, head) = match admitted {
        Some(AdmittedConnection {
            connection, head, ..
        }) => {
            // The admission position follows the IO: it is returned only after
            // the socket and its TLS state have been destroyed.
            let stream = novarocks_transport_io::OwnedIo::with_guard(stream, connection);
            (stream, Some(head))
        }
        // Only private generic listener tests omit admission. Production
        // composition requires role-matched admission before listener startup.
        None => (novarocks_transport_io::OwnedIo::new(stream), None),
    };
    let mut builder = http2::Builder::new(TokioExecutor::new());
    configure_native_server(&mut builder);
    let close = head.as_ref().map(|head| Arc::clone(&head.close));
    let handshake = head.as_ref().map(|head| head.handshake.clone());
    let service = service_fn(move |request: hyper::Request<hyper::body::Incoming>| {
        let app = app.clone();
        let admitted = head
            .as_ref()
            .map(|head| head.admit(request.uri(), request.headers()));
        async move {
            if admitted == Some(Err(())) {
                return Ok::<_, std::convert::Infallible>(
                    tonic::Status::new(
                        tonic::Code::Unavailable,
                        "native connection is sealed to another caller or lane",
                    )
                    .into_http(),
                );
            }
            let response = app
                .oneshot(request.map(axum::body::Body::new))
                .await
                .expect("Native route service is infallible");
            Ok::<_, std::convert::Infallible>(response)
        }
    });
    let mut connection = std::pin::pin!(builder.serve_connection(TokioIo::new(stream), service));
    let mut winding_down = false;
    let outcome = loop {
        let bootstrap_pending = handshake
            .as_ref()
            .is_some_and(NativeHandshakePermit::is_held);
        tokio::select! {
            outcome = &mut connection => break Ok(outcome),
            // A closed channel means the listener is gone, which asks for the
            // same thing as an explicit signal.
            _ = drain.changed(), if !winding_down => {
                winding_down = true;
                // GOAWAY, then let the streams already in flight finish. This
                // is what makes a stopping backend answer its caller instead
                // of vanishing.
                connection.as_mut().graceful_shutdown();
            }
            _ = sleep_until_or_forever(deadline), if bootstrap_pending => {
                // The guard above was read when this wait began. A request
                // that authenticated meanwhile ended the bootstrap, and its
                // connection must keep serving past the deadline.
                if handshake
                    .as_ref()
                    .is_some_and(NativeHandshakePermit::is_held)
                {
                    break Err("native bootstrap deadline elapsed before an authenticated request");
                }
            }
            _ = notified_or_forever(close.as_deref()) => {
                break Err("native connection seal conflict");
            }
        }
    };
    match outcome {
        Ok(outcome) => served.close(
            "http2",
            &match outcome {
                Ok(()) => "ok".to_string(),
                Err(error) => format!("{error:?}"),
            },
        ),
        Err(reason) => {
            if reason.starts_with("native bootstrap") {
                on_transport_handshake_failure();
            }
            served.close("admission", reason);
        }
    }
}

async fn sleep_until_or_forever(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

async fn notified_or_forever(close: Option<&Notify>) {
    match close {
        Some(close) => close.notified().await,
        None => std::future::pending().await,
    }
}

#[expect(
    clippy::disallowed_methods,
    reason = "The original native listener admits and drains connection service tasks; see ADR-0218."
)]
async fn serve_native_listener<S>(
    listener: TokioTcpListener,
    app: S,
    incoming: NativeIncomingAdapter,
    mut shutdown_rx: watch::Receiver<bool>,
    on_transport_handshake_failure: Arc<dyn Fn() + Send + Sync>,
    role_label: &'static str,
    admission: Option<ListenerAdmission>,
    incoming_admission: Option<(NativeServerAdmission, NativeEndpointDomain)>,
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
    let bootstrap =
        Duration::from_millis(NativeResultSupportGeometry::V1.transport_handshake_deadline_ms);
    // Per listener rather than per process: one all-in-one process hosts both
    // role listeners, and a shared counter would report the other one's work.
    let live_connections = Arc::new(AtomicU64::new(0));
    let next_connection_id = Arc::new(AtomicU64::new(1));
    // Every connection holds a receiver for as long as it is serving, so
    // sending on this channel is how the listener asks them all to wind down
    // and `closed()` is how it learns that they have.
    let (drain_tx, drain_rx) = watch::channel(());
    let mut consecutive_accept_errors = 0_u32;
    loop {
        tokio::select! {
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    break;
                }
            }
            accepted = listener.accept() => {
                let (stream, peer) = match accepted {
                    Err(error) => {
                        // One refused connection is not a broken listener. A
                        // listener that is really gone still surfaces, through
                        // the consecutive count below, so supervision can act.
                        consecutive_accept_errors += 1;
                        tracing::warn!(
                            role = role_label,
                            %error,
                            consecutive_accept_errors,
                            "native listener could not accept a connection"
                        );
                        if consecutive_accept_errors >= MAX_CONSECUTIVE_ACCEPT_ERRORS {
                            return Err(format!(
                                "accept native gRPC connection failed \
                                 {consecutive_accept_errors} times in a row: {error}"
                            ));
                        }
                        // Descriptor exhaustion returns immediately and would
                        // otherwise spin this loop against a full table.
                        tokio::time::sleep(ACCEPT_ERROR_BACKOFF).await;
                        continue;
                    }
                    Ok(accepted) => accepted,
                };
                consecutive_accept_errors = 0;
                let accepted_at = tokio::time::Instant::now();
                let admitted = match (&admission, &incoming_admission) {
                    (Some(listener_admission), Some((server_admission, domain))) => {
                        match listener_admission.admission.try_accept(listener_admission.class) {
                            Ok(accepted) => Some(AdmittedConnection {
                                connection: accepted.connection,
                                head: NativeIncomingHeadAdmission {
                                    binding: accepted.binding,
                                    admission: server_admission.clone(),
                                    domain: *domain,
                                    handshake: accepted.handshake,
                                    close: Arc::new(Notify::new()),
                                },
                                bootstrap_deadline: accepted_at + bootstrap,
                            }),
                            Err(_) => {
                                // No position: close the socket now. Refusal is
                                // not a broken listener and leaves no pending work.
                                tracing::debug!(
                                    role = role_label,
                                    %peer,
                                    "native listener refused a connection without admission positions"
                                );
                                drop(stream);
                                continue;
                            }
                        }
                    }
                    (Some(_), None) => {
                        return Err("admitted native listener lacks incoming admission".to_owned());
                    }
                    (None, _) => None,
                };
                let served = ServedConnection::open(
                    role_label,
                    peer,
                    &next_connection_id,
                    &live_connections,
                );
                tokio::spawn(serve_connection(ConnectionInputs {
                    stream,
                    app: app.clone(),
                    incoming: incoming.clone(),
                    admitted,
                    served,
                    drain: drain_rx.clone(),
                    on_transport_handshake_failure: Arc::clone(&on_transport_handshake_failure),
                }));
            }
        }
    }

    // Stop accepting first, then let what is already in flight answer. The
    // listener's own receiver has to go before `closed()` can ever resolve.
    drop(drain_rx);
    let _ = drain_tx.send(());
    let live = live_connections.load(Ordering::Acquire);
    if live == 0 {
        tracing::debug!(role = role_label, "native listener stopped");
        return Ok(());
    }
    tracing::debug!(
        role = role_label,
        live_connections = live,
        "native listener is draining its connections"
    );
    if tokio::time::timeout(SHUTDOWN_DRAIN_TIMEOUT, drain_tx.closed())
        .await
        .is_err()
    {
        // Bounded on purpose: this runs inside process shutdown, and a peer
        // that never finishes its stream must not be able to hold the process
        // open. What is left is about to be cancelled, and each one says so.
        tracing::warn!(
            role = role_label,
            live_connections = live_connections.load(Ordering::Acquire),
            drain_timeout_ms = SHUTDOWN_DRAIN_TIMEOUT.as_millis(),
            "native listener stopped before every connection finished draining"
        );
    } else {
        tracing::debug!(role = role_label, "native listener drained and stopped");
    }
    Ok(())
}

/// One inbound connection this listener is serving, and the record of how it
/// ended.
///
/// A connection that reaches [`ServedConnection::close`] ended because its own
/// serving future resolved, and the reason is whatever hyper reported. A
/// connection whose guard drops first was ended by an owner outside it, which
/// means its peer saw the socket close with no HTTP/2 GOAWAY and, under TLS,
/// no `close_notify`. Without this record that difference is invisible from
/// both ends of the connection.
struct ServedConnection {
    role: &'static str,
    id: u64,
    peer: SocketAddr,
    live: Arc<AtomicU64>,
    closed: bool,
}

impl ServedConnection {
    fn open(
        role: &'static str,
        peer: SocketAddr,
        next_id: &Arc<AtomicU64>,
        live: &Arc<AtomicU64>,
    ) -> Self {
        let id = next_id.fetch_add(1, Ordering::Relaxed);
        let live_now = live.fetch_add(1, Ordering::AcqRel) + 1;
        tracing::debug!(
            role,
            connection_id = id,
            %peer,
            live_connections = live_now,
            "native listener accepted a connection"
        );
        Self {
            role,
            id,
            peer,
            live: Arc::clone(live),
            closed: false,
        }
    }

    fn close(&mut self, stage: &str, detail: &str) {
        self.closed = true;
        tracing::debug!(
            role = self.role,
            connection_id = self.id,
            peer = %self.peer,
            stage,
            detail,
            "native listener connection ended"
        );
    }
}

impl Drop for ServedConnection {
    fn drop(&mut self) {
        let live_now = self.live.fetch_sub(1, Ordering::AcqRel).saturating_sub(1);
        if self.closed {
            return;
        }
        // Unwinding means a handler panicked through the serving future; a
        // quiet drop means an owner outside the connection cancelled it, which
        // today is this listener's runtime being dropped.
        let cause = if std::thread::panicking() {
            "panic"
        } else {
            "cancelled"
        };
        tracing::warn!(
            role = self.role,
            connection_id = self.id,
            peer = %self.peer,
            cause,
            live_connections = live_now,
            "native listener connection was ended without finishing; its peer sees an abrupt close"
        );
    }
}

async fn grpc_unimplemented_fallback(
    request: axum::http::Request<axum::body::Body>,
) -> axum::http::Response<axum::body::Body> {
    drop(request);
    tonic::Status::new(tonic::Code::Unimplemented, "")
        .into_http()
        .map(axum::body::Body::new)
}

/// Serves `FetchRootResult` from the Backend's bounded root reader.
#[derive(Clone)]
struct RootResultRoute {
    reader: Arc<crate::root_result_reader::NativeRootResultReader>,
}

impl Service<axum::http::Request<axum::body::Body>> for RootResultRoute {
    type Response = axum::http::Response<tonic::body::BoxBody>;
    type Error = std::convert::Infallible;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: axum::http::Request<axum::body::Body>) -> Self::Future {
        let reader = Arc::clone(&self.reader);
        Box::pin(async move {
            Ok(
                crate::root_result_unary::root_result_unary(&reader, request)
                    .await
                    .map(boxed),
            )
        })
    }
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn authentication_refusal_answers_without_entering_the_route() {
        use std::sync::atomic::AtomicUsize;
        let entered = Arc::new(AtomicUsize::new(0));
        let failures = Arc::new(AtomicUsize::new(0));
        let observed = entered.clone();
        let inner = tower::service_fn(move |_request: axum::http::Request<axum::body::Body>| {
            observed.fetch_add(1, Ordering::SeqCst);
            async { Ok::<_, std::convert::Infallible>(tonic::Status::ok("").into_http()) }
        });
        let rejected = failures.clone();
        let mut service = crate::backend_test_support::test_backend_native_trust()
            .server_admission()
            .listener_layer()
            .with_authentication_failure_observer(Arc::new(move || {
                rejected.fetch_add(1, Ordering::SeqCst);
            }))
            .layer(inner);
        let response = service
            .call(axum::http::Request::new(axum::body::Body::empty()))
            .await
            .unwrap();
        assert_eq!(failures.load(Ordering::SeqCst), 1);
        assert_eq!(entered.load(Ordering::SeqCst), 0);
        assert_eq!(response.status(), 200);
        assert_eq!(response.headers()["grpc-status"], "16");
    }

    #[tokio::test]
    async fn unknown_route_answers_unimplemented() {
        let response =
            grpc_unimplemented_fallback(axum::http::Request::new(axum::body::Body::empty())).await;
        assert_eq!(response.status(), 200);
        assert_eq!(response.headers()["grpc-status"], "12");
        assert_eq!(response.headers()["content-type"], "application/grpc");
    }

    /// A service that reports when it has been entered and answers only when
    /// it is released, so a test can hold one request in flight.
    #[derive(Clone)]
    struct HeldService {
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
    }

    impl Service<axum::http::Request<axum::body::Body>> for HeldService {
        type Response = axum::http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
        >;

        fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _request: axum::http::Request<axum::body::Body>) -> Self::Future {
            let entered = Arc::clone(&self.entered);
            let release = Arc::clone(&self.release);
            Box::pin(async move {
                entered.notify_one();
                release.notified().await;
                Ok(axum::http::Response::builder()
                    .status(200)
                    .body(boxed(axum::body::Body::empty()))
                    .expect("held response"))
            })
        }
    }

    /// Stopping the listener must not cut a request that is already being
    /// served.
    ///
    /// The listener's runtime is dropped as soon as this function returns, and
    /// dropping it cancels every connection task it still owns. So "does not
    /// return while a request is in flight" is the whole of what keeps a
    /// stopping backend from closing its caller's socket with no GOAWAY.
    #[tokio::test(flavor = "multi_thread")]
    async fn stopping_waits_for_a_request_that_is_already_being_served() {
        let listener = TokioTcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test listener");
        let address = listener.local_addr().expect("test listener address");
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut serving = tokio::spawn(serve_native_listener(
            listener,
            HeldService {
                entered: Arc::clone(&entered),
                release: Arc::clone(&release),
            },
            NativeIncomingAdapter::plaintext(),
            shutdown_rx,
            Arc::new(|| {}) as Arc<dyn Fn() + Send + Sync>,
            "test",
            None,
            None,
        ));

        let stream = tokio::net::TcpStream::connect(address)
            .await
            .expect("connect to the test listener");
        let (mut sender, connection) = h2::client::handshake(stream)
            .await
            .expect("client handshake");
        let driver = tokio::spawn(async move {
            let _ = connection.await;
        });
        let request = axum::http::Request::builder()
            .method("POST")
            .uri("/held")
            .body(())
            .expect("held request");
        let (response, _body) = sender.send_request(request, true).expect("send request");
        entered.notified().await;

        shutdown_tx.send(true).expect("request shutdown");
        assert!(
            tokio::time::timeout(Duration::from_millis(250), &mut serving)
                .await
                .is_err(),
            "the listener returned while a request was still being served"
        );

        release.notify_one();
        assert_eq!(
            response.await.expect("held response arrives").status(),
            200,
            "a request already in flight must still be answered"
        );
        let served = tokio::time::timeout(Duration::from_secs(5), serving)
            .await
            .expect("the listener returns once its connections drained")
            .expect("listener task");
        assert_eq!(served, Ok(()));

        drop(sender);
        let _ = driver.await;
    }
}
