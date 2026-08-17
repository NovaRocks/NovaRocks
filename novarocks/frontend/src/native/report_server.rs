//! Frontend-owned report-only native endpoint.

use std::collections::BTreeMap;
use std::net::{SocketAddr, TcpListener};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::task::{Context, Poll};
use std::thread::JoinHandle;

use axum::Json;
use axum::Router;
use axum::http::{HeaderValue, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use novarocks::query_execution::lifecycle::{
    QueryLifecycleError, QueryLifecycleErrorCode, QueryTerminalIngress,
};
use novarocks_protocol::lifecycle::{
    ContractError, ContractErrorCode, ParticipantTerminalOutcome, QueryTerminalReportOutcome,
};
use novarocks_protocol::{filter, novarocks as proto};
use tokio::net::TcpListener as TokioTcpListener;
use tokio::sync::watch;
use tonic::body::boxed;
use tonic::codegen::Service;
use tonic::server::NamedService;

use crate::coordinator::{
    QueryLifecycleConvergenceErrorSource, QueryLifecycleConvergenceReader,
    QueryLifecycleConvergenceSnapshot,
};

use super::generated::nova_rocks_grpc_server::{NovaRocksGrpc, NovaRocksGrpcServer};

const GRPC_MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;

const LIFECYCLE_CONVERGENCE_DEBUG_PATH: &str = "/debug/query-lifecycle/latest";

fn lifecycle_convergence_debug_enabled() -> bool {
    cfg!(debug_assertions)
        && std::env::var_os("NOVAROCKS_SQL_TEST_QUERY_LIFECYCLE_FAULT_DIR").is_some()
}

#[derive(serde::Serialize)]
struct LifecycleConvergenceDebugSnapshot {
    execution_id: String,
    error_source: Option<&'static str>,
    participant_outcomes: Vec<LifecycleParticipantOutcomeDebug>,
    telemetry_unavailable: Vec<LifecycleTelemetryUnavailableDebug>,
    /// This endpoint intentionally exposes only query-scoped immutable
    /// terminal evidence. Process metrics are not an acceptable substitute.
    metrics: BTreeMap<String, i64>,
}

#[derive(serde::Serialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
enum LifecycleParticipantOutcomeDebug {
    Proof,
    Attestation { reason: String },
    NoOutcome,
}

#[derive(serde::Serialize)]
struct LifecycleTelemetryUnavailableDebug {
    scope: &'static str,
    stage: String,
    code: String,
}

async fn latest_lifecycle_convergence_snapshot(
    reader: Arc<dyn QueryLifecycleConvergenceReader>,
) -> axum::response::Response {
    let Some(snapshot) = reader.latest_convergence_snapshot() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    Json(lifecycle_convergence_debug_snapshot(snapshot)).into_response()
}

fn lifecycle_convergence_debug_snapshot(
    snapshot: QueryLifecycleConvergenceSnapshot,
) -> LifecycleConvergenceDebugSnapshot {
    let mut telemetry_unavailable = Vec::new();
    let mut participant_outcomes = snapshot
        .participant_outcomes
        .iter()
        .map(|outcome| {
            if let Some(snapshot) = outcome.snapshot() {
                let snapshot = snapshot.as_proto();
                if let Some(
                    proto::query_terminal_profile_contribution_telemetry::Telemetry::Unavailable(
                        reason,
                    ),
                ) = snapshot
                    .profile_contribution
                    .as_ref()
                    .and_then(|telemetry| telemetry.telemetry.as_ref())
                {
                    telemetry_unavailable.push(LifecycleTelemetryUnavailableDebug {
                        scope: "query",
                        stage: reason.stage.clone(),
                        code: reason.code.clone(),
                    });
                }
                for fragment in &snapshot.fragments {
                    if let Some(
                        proto::fragment_terminal_profile_telemetry::Telemetry::Unavailable(reason),
                    ) = fragment
                        .profile
                        .as_ref()
                        .and_then(|telemetry| telemetry.telemetry.as_ref())
                    {
                        telemetry_unavailable.push(LifecycleTelemetryUnavailableDebug {
                            scope: "fragment",
                            stage: reason.stage.clone(),
                            code: reason.code.clone(),
                        });
                    }
                }
                LifecycleParticipantOutcomeDebug::Proof
            } else if let Some(attestation) = outcome.negative_attestation() {
                LifecycleParticipantOutcomeDebug::Attestation {
                    reason: format!("{:?}", attestation.reason()),
                }
            } else {
                unreachable!("validated participant terminal outcome must be proof or attestation")
            }
        })
        .collect::<Vec<_>>();
    let error_source = snapshot.error_source.map(|source| match source {
        QueryLifecycleConvergenceErrorSource::BackendAttestation => "backend-attestation",
        QueryLifecycleConvergenceErrorSource::FrontendLiveness => "frontend-liveness",
        QueryLifecycleConvergenceErrorSource::NoOutcome => {
            participant_outcomes.push(LifecycleParticipantOutcomeDebug::NoOutcome);
            "no-outcome"
        }
    });
    LifecycleConvergenceDebugSnapshot {
        execution_id: format!(
            "{}:{}:{}",
            snapshot.execution_id.query_id().high(),
            snapshot.execution_id.query_id().low(),
            snapshot.execution_id.attempt_id().get()
        ),
        error_source,
        participant_outcomes,
        telemetry_unavailable,
        metrics: lifecycle_metric_map(snapshot.metrics),
    }
}

fn lifecycle_metric_map(
    metrics: novarocks::service::query_lifecycle_metrics::FrontendQueryLifecycleMetricsSnapshot,
) -> BTreeMap<String, i64> {
    [
        ("active_attempts", metrics.active_attempts as i64),
        ("init_applied", metrics.init_applied as i64),
        ("init_idempotent", metrics.init_idempotent as i64),
        ("init_failed", metrics.init_failed as i64),
        ("control_ready", metrics.control_ready as i64),
        ("attach_failed", metrics.attach_failed as i64),
        ("heartbeat_timeouts", metrics.heartbeat_timeouts as i64),
        ("coordinator_lost", metrics.coordinator_lost as i64),
        ("local_failures", metrics.local_failures as i64),
        (
            "backend_epoch_mismatches",
            metrics.backend_epoch_mismatches as i64,
        ),
        ("cleanup_failures", metrics.cleanup_failures as i64),
        (
            "terminal_locally_drained",
            metrics.terminal_locally_drained as i64,
        ),
        (
            "terminal_snapshots_accepted",
            metrics.terminal_snapshots_accepted as i64,
        ),
        (
            "terminal_snapshots_idempotent",
            metrics.terminal_snapshots_idempotent as i64,
        ),
        (
            "terminal_snapshot_conflicts",
            metrics.terminal_snapshot_conflicts as i64,
        ),
        (
            "terminal_finalize_failures",
            metrics.terminal_finalize_failures as i64,
        ),
    ]
    .into_iter()
    .map(|(name, value)| (name.to_string(), value))
    .collect()
}

#[derive(Clone)]
struct FrontendReportService {
    ingress: Arc<dyn QueryTerminalIngress>,
}

impl FrontendReportService {
    fn rejected(rpc_name: &str) -> tonic::Status {
        tonic::Status::failed_precondition(format!(
            "report-only NovaRocksGrpc endpoint rejects local execution RPC: {rpc_name}"
        ))
    }
}

#[tonic::async_trait]
impl NovaRocksGrpc for FrontendReportService {
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
        _request: tonic::Request<tonic::Streaming<proto::ExchangeRequest>>,
    ) -> Result<tonic::Response<Self::ExchangeStream>, tonic::Status> {
        Err(Self::rejected("Exchange"))
    }

    async fn exchange_unary(
        &self,
        _request: tonic::Request<proto::ExchangeRequest>,
    ) -> Result<tonic::Response<proto::ExchangeResponse>, tonic::Status> {
        Err(Self::rejected("ExchangeUnary"))
    }

    async fn transmit_runtime_filter_envelope(
        &self,
        _request: tonic::Request<filter::RuntimeFilterEnvelope>,
    ) -> Result<tonic::Response<filter::RuntimeFilterEnvelopeResponse>, tonic::Status> {
        Err(Self::rejected("TransmitRuntimeFilterEnvelope"))
    }

    async fn lookup(
        &self,
        _request: tonic::Request<filter::LookupRequest>,
    ) -> Result<tonic::Response<filter::LookupResponse>, tonic::Status> {
        Err(Self::rejected("Lookup"))
    }

    async fn fetch_result(
        &self,
        _request: tonic::Request<proto::FetchResultRequest>,
    ) -> Result<tonic::Response<proto::FetchResultResponse>, tonic::Status> {
        Err(Self::rejected("FetchResult"))
    }

    async fn ensure_connector_execution_binding(
        &self,
        _request: tonic::Request<proto::EnsureConnectorExecutionBindingRequest>,
    ) -> Result<tonic::Response<proto::EnsureConnectorExecutionBindingResponse>, tonic::Status>
    {
        Err(Self::rejected("EnsureConnectorExecutionBinding"))
    }

    async fn retire_connector_execution_binding(
        &self,
        _request: tonic::Request<proto::RetireConnectorExecutionBindingRequest>,
    ) -> Result<tonic::Response<proto::RetireConnectorExecutionBindingResponse>, tonic::Status>
    {
        Err(Self::rejected("RetireConnectorExecutionBinding"))
    }

    async fn heartbeat(
        &self,
        _request: tonic::Request<proto::HeartbeatRequest>,
    ) -> Result<tonic::Response<proto::HeartbeatResponse>, tonic::Status> {
        Err(Self::rejected("Heartbeat"))
    }

    async fn init_query(
        &self,
        _request: tonic::Request<proto::InitQueryRequest>,
    ) -> Result<tonic::Response<proto::InitQueryResponse>, tonic::Status> {
        Err(Self::rejected("InitQuery"))
    }

    async fn stage_fragments(
        &self,
        _request: tonic::Request<proto::StageFragmentsRequest>,
    ) -> Result<tonic::Response<proto::StageFragmentsResponse>, tonic::Status> {
        Err(Self::rejected("StageFragments"))
    }

    async fn start_prepared_query(
        &self,
        _request: tonic::Request<proto::StartPreparedQueryRequest>,
    ) -> Result<tonic::Response<proto::StartPreparedQueryResponse>, tonic::Status> {
        Err(Self::rejected("StartPreparedQuery"))
    }

    async fn abort_query(
        &self,
        _request: tonic::Request<proto::AbortQueryRequest>,
    ) -> Result<tonic::Response<proto::AbortQueryResponse>, tonic::Status> {
        Err(Self::rejected("AbortQuery"))
    }

    async fn query_control_stream(
        &self,
        _request: tonic::Request<tonic::Streaming<proto::QueryControlRequest>>,
    ) -> Result<tonic::Response<Self::QueryControlStreamStream>, tonic::Status> {
        Err(Self::rejected("QueryControlStream"))
    }

    async fn report_query_terminal(
        &self,
        request: tonic::Request<proto::ReportQueryTerminalRequest>,
    ) -> Result<tonic::Response<proto::ReportQueryTerminalResponse>, tonic::Status> {
        let outcome = request.into_inner().outcome.ok_or_else(|| {
            tonic::Status::invalid_argument("ReportQueryTerminalRequest missing outcome")
        })?;
        let outcome =
            ParticipantTerminalOutcome::parse(outcome).map_err(status_from_contract_error)?;
        let ingress = Arc::clone(&self.ingress);
        let ack = tokio::task::spawn_blocking(move || ingress.report_query_terminal(outcome))
            .await
            .map_err(|error| {
                tonic::Status::internal(format!("query terminal ingress panicked: {error}"))
            })?
            .map_err(status_from_lifecycle_error)?;
        let outcome = match ack.outcome().map_err(status_from_contract_error)? {
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
            QueryTerminalReportOutcome::Unspecified => {
                return Err(tonic::Status::internal(
                    "validated query terminal report acknowledgement has an unspecified outcome",
                ));
            }
        };
        Ok(tonic::Response::new(proto::ReportQueryTerminalResponse {
            outcome: outcome as i32,
            detail: ack.detail().to_string(),
        }))
    }
}

fn status_from_lifecycle_error(error: QueryLifecycleError) -> tonic::Status {
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

fn status_from_contract_error(error: ContractError) -> tonic::Status {
    let detail = error.detail().to_string();
    match error.code() {
        ContractErrorCode::InvalidValue | ContractErrorCode::VersionMismatch => {
            tonic::Status::invalid_argument(detail)
        }
        ContractErrorCode::Conflict | ContractErrorCode::DigestMismatch => {
            tonic::Status::already_exists(detail)
        }
        ContractErrorCode::Capacity => tonic::Status::resource_exhausted(detail),
    }
}

/// Instance-owned report listener. The host exposes only lifecycle methods,
/// never a Tonic service or a Core listener handle.
pub(crate) struct FrontendReportServerHandle {
    bound_addr: SocketAddr,
    shutdown_tx: Option<watch::Sender<bool>>,
    failure_rx: mpsc::Receiver<String>,
    join_handle: Option<JoinHandle<()>>,
    stop_requested: Arc<AtomicBool>,
}

impl FrontendReportServerHandle {
    pub(crate) fn start(
        host: &str,
        port: u16,
        ingress: Arc<dyn QueryTerminalIngress>,
        convergence_reader: Arc<dyn QueryLifecycleConvergenceReader>,
    ) -> Result<Self, String> {
        let address = parse_bind_addr(host, port)?;
        let listener = TcpListener::bind(address).map_err(|error| {
            format!("bind frontend report endpoint on {address} failed: {error}")
        })?;
        listener.set_nonblocking(true).map_err(|error| {
            format!("set frontend report endpoint on {address} nonblocking failed: {error}")
        })?;
        let bound_addr = listener.local_addr().map_err(|error| {
            format!("read frontend report endpoint bound address failed: {error}")
        })?;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (failure_tx, failure_rx) = mpsc::channel();
        let stop_requested = Arc::new(AtomicBool::new(false));
        let thread_stop_requested = Arc::clone(&stop_requested);
        let join_handle = std::thread::Builder::new()
            .name("frontend-report-grpc".to_string())
            .spawn(move || {
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let runtime = tokio::runtime::Builder::new_multi_thread()
                        .enable_all()
                        .worker_threads(8)
                        .thread_stack_size(
                            novarocks::runtime::global_async_runtime::WORKER_STACK_SIZE_BYTES,
                        )
                        .build()
                        .map_err(|error| {
                            format!("build frontend report endpoint runtime failed: {error}")
                        })?;
                    runtime.block_on(async move {
                        let listener = TokioTcpListener::from_std(listener).map_err(|error| {
                            format!("create frontend report Tokio listener failed: {error}")
                        })?;
                        let service = NovaRocksGrpcServer::new(FrontendReportService { ingress })
                            .max_decoding_message_size(GRPC_MAX_MESSAGE_BYTES)
                            .max_encoding_message_size(GRPC_MAX_MESSAGE_BYTES);
                        let grpc_path = format!(
                            "/{}/*rest",
                            <NovaRocksGrpcServer<FrontendReportService> as NamedService>::NAME
                        );
                        let app = Router::new()
                            .route_service(&grpc_path, AxumGrpcService::new(service))
                            .route("/metrics", get(novarocks::service::handle_metrics))
                            .fallback(grpc_unimplemented_fallback);
                        let app = if lifecycle_convergence_debug_enabled() {
                            let debug_reader = Arc::clone(&convergence_reader);
                            app.route(
                                LIFECYCLE_CONVERGENCE_DEBUG_PATH,
                                get(move || {
                                    latest_lifecycle_convergence_snapshot(Arc::clone(&debug_reader))
                                }),
                            )
                        } else {
                            app
                        };
                        let mut shutdown_rx = shutdown_rx;
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
                                format!("frontend report endpoint serve future failed: {error}")
                            })
                    })
                }));
                if thread_stop_requested.load(Ordering::Acquire) {
                    return;
                }
                let error = match outcome {
                    Ok(Ok(())) => "frontend report endpoint exited unexpectedly".to_string(),
                    Ok(Err(error)) => error,
                    Err(payload) => payload
                        .downcast_ref::<String>()
                        .cloned()
                        .or_else(|| {
                            payload
                                .downcast_ref::<&str>()
                                .map(|value| (*value).to_string())
                        })
                        .unwrap_or_else(|| "frontend report endpoint panicked".to_string()),
                };
                let _ = failure_tx.send(error);
            })
            .map_err(|error| format!("spawn frontend report endpoint: {error}"))?;
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
            Err(mpsc::TryRecvError::Empty) | Err(mpsc::TryRecvError::Disconnected) => Ok(None),
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
                .map_err(|_| "frontend report endpoint thread panicked".to_string())?;
        }
        Ok(())
    }
}

impl Drop for FrontendReportServerHandle {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

fn parse_bind_addr(host: &str, port: u16) -> Result<SocketAddr, String> {
    let bare = if host.starts_with('[') && host.ends_with(']') {
        &host[1..host.len() - 1]
    } else {
        host
    };
    if let Ok(ip) = bare.parse::<std::net::IpAddr>() {
        return Ok(SocketAddr::new(ip, port));
    }
    let formatted = if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    };
    formatted
        .parse::<SocketAddr>()
        .map_err(|error| format!("parse frontend report bind addr '{formatted}' failed: {error}"))
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
