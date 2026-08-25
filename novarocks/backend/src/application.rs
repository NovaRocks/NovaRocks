use std::fmt;
use std::future::Future;
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::time::{Duration, Instant};

use novarocks_execution::runtime::execution_runtime::{ExecutionRuntime, ExecutionRuntimeConfig};
use novarocks_proto::lifecycle::{
    QueryAbortRequest, QueryControlAttach, QueryInitAck, QueryInitRequest, QueryStageAck,
    QueryStageOutcome, QueryStageRequest, QueryStartAck, QueryStartRequest, QueryTerminationAck,
};
use novarocks_spi::connector::ConnectorExecutionInstaller;
use novarocks_types::AdvertiseEndpoint;

use crate::BackendDataRuntime;
use crate::connector::ConnectorRegistry;
use crate::exchange_receiver::BackendExchangeReceiverPort;
use crate::fragment::control::FragmentControlRegistry;
use crate::fragment::{
    NativeFragmentService, grpc_exchange_transmitter, grpc_fragment_lookup_client,
    native_result_writer,
};
use crate::metrics::MetricsHttpServer;
use crate::query_lifecycle::{
    NativeQueryLifecycleLocalRuntime, QueryControlAttachment, QueryLifecycleError,
    QueryLifecycleIngress, QueryLifecycleRegistry, QueryLifecycleRegistryConfig,
};
use crate::rpc::server::{BackendRpcServerHandle, BackendRpcService};
use crate::runtime_filter::rpc::BackendRuntimeFilterEnvelopeIngress;
use novarocks_execution::runtime::fragment::io::ExchangeReceiverPort;
use novarocks_spi::connector::WriteCommitEvidenceLimits;

const READINESS_TIMEOUT: Duration = Duration::from_secs(5);
const SUPERVISION_POLL_INTERVAL: Duration = Duration::from_millis(50);

pub struct BackendServerConfig {
    pub bind_host: String,
    pub grpc_port: u16,
    pub metrics_http_port: u16,
    pub advertise_endpoint: AdvertiseEndpoint,
    pub query_lifecycle_sweep_interval: Duration,
    pub query_lifecycle_config: QueryLifecycleRegistryConfig,
    /// Server-resolved per-fragment terminal write evidence budget.
    pub write_commit_evidence_limits: WriteCommitEvidenceLimits,
    pub execution_runtime_config: ExecutionRuntimeConfig,
    /// Provider-owned execution installers composed by the server role.
    ///
    /// Backend only owns registration and lifecycle of these contributions; it
    /// never constructs a provider-specific installer or catalog binding.
    pub execution_installers: Vec<Arc<dyn ConnectorExecutionInstaller>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackendApplicationErrorKind {
    Configuration,
    Start,
    Readiness,
    Supervision,
    Shutdown,
    Signal,
}

#[derive(Debug)]
pub struct BackendApplicationError {
    kind: BackendApplicationErrorKind,
    message: String,
}

impl BackendApplicationError {
    fn new(kind: BackendApplicationErrorKind, error: impl fmt::Display) -> Self {
        Self {
            kind,
            message: error.to_string(),
        }
    }

    fn with_cleanup_context(mut self, cleanup_error: impl fmt::Display) -> Self {
        self.message
            .push_str(&format!("; cleanup failed: {cleanup_error}"));
        self
    }

    pub const fn kind(&self) -> BackendApplicationErrorKind {
        self.kind
    }
}

impl fmt::Display for BackendApplicationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}: {}", self.kind, self.message)
    }
}

impl std::error::Error for BackendApplicationError {}

pub struct BackendApplicationHost {
    ready_marker: String,
    grpc_server: BackendRpcServerHandle,
    _native_fragment_service: Arc<NativeFragmentService>,
    _query_lifecycle_registry: Arc<QueryLifecycleRegistry>,
    execution_host: Arc<crate::ConnectorExecutionHost>,
    _execution_runtime: Arc<ExecutionRuntime>,
    query_lifecycle_sweep: QueryLifecycleSweepTask,
    metrics_http_server: MetricsHttpServer,
}

impl fmt::Debug for BackendApplicationHost {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BackendApplicationHost")
            .field("ready_marker", &self.ready_marker)
            .finish_non_exhaustive()
    }
}

struct BackendApplicationServices {
    native_fragment_service: Arc<NativeFragmentService>,
    query_lifecycle_registry: Arc<QueryLifecycleRegistry>,
    execution_host: Arc<crate::ConnectorExecutionHost>,
    execution_runtime: Arc<ExecutionRuntime>,
    exchange_receiver_port: Arc<dyn ExchangeReceiverPort>,
    query_lifecycle_ingress: Arc<dyn QueryLifecycleIngress>,
}

/// Backend composition root for the QLC-3 Stage/Start transaction.  The
/// registry owns lifecycle linearization while the fragment service owns
/// dormant local workers; neither exposes a direct production submit path.
struct BackendStageLifecycleIngress {
    registry: Arc<QueryLifecycleRegistry>,
    fragments: Arc<NativeFragmentService>,
}

impl QueryLifecycleIngress for BackendStageLifecycleIngress {
    fn bind_backend_identity(&self, backend_id: u64) -> Result<(), QueryLifecycleError> {
        self.registry.bind_backend_identity(backend_id)
    }

    fn init_query(&self, request: QueryInitRequest) -> QueryInitAck {
        self.registry.init_query(request)
    }

    fn stage_fragments(&self, request: QueryStageRequest) -> QueryStageAck {
        match self.registry.begin_stage(request.clone()) {
            crate::query_lifecycle::StageBuildDecision::Complete(ack) => ack,
            crate::query_lifecycle::StageBuildDecision::Build(permit) => {
                let execution_id = request.execution_id();
                let fragments = request.fragments();
                let build = self
                    .fragments
                    .stage_fragments(execution_id, &fragments, permit.gate());
                match build {
                    Ok(()) => permit.commit(),
                    Err(error) => QueryStageAck::new(
                        request.execution_id(),
                        request.digest_version(),
                        request.digest(),
                        QueryStageOutcome::RejectedLocalFailure,
                        error.to_string(),
                    )
                    .expect("validated Stage request has a valid failure acknowledgement"),
                }
            }
        }
    }

    fn start_prepared_query(&self, request: QueryStartRequest) -> QueryStartAck {
        self.registry.start_prepared_query(request)
    }

    fn abort_query(
        &self,
        request: QueryAbortRequest,
    ) -> Result<QueryTerminationAck, QueryLifecycleError> {
        self.registry.abort_query(request)
    }

    fn attach_control(
        &self,
        attach: QueryControlAttach,
    ) -> Result<QueryControlAttachment, QueryLifecycleError> {
        self.registry.attach_control(attach)
    }
}

struct QueryLifecycleSweepTask {
    stop_tx: Option<std::sync::mpsc::Sender<()>>,
    join_handle: Option<std::thread::JoinHandle<()>>,
}

impl QueryLifecycleSweepTask {
    fn start(registry: Arc<QueryLifecycleRegistry>, interval: Duration) -> Result<Self, String> {
        let (stop_tx, stop_rx) = std::sync::mpsc::channel();
        let join_handle = std::thread::Builder::new()
            .name("query-lifecycle-sweep".to_string())
            .spawn(move || {
                while let Err(std::sync::mpsc::RecvTimeoutError::Timeout) =
                    stop_rx.recv_timeout(interval)
                {
                    registry.sweep_expired(Instant::now());
                }
            })
            .map_err(|error| format!("spawn query lifecycle sweep task: {error}"))?;
        Ok(Self {
            stop_tx: Some(stop_tx),
            join_handle: Some(join_handle),
        })
    }

    fn stop(&mut self) -> Result<(), String> {
        if let Some(stop_tx) = self.stop_tx.take() {
            let _ = stop_tx.send(());
        }
        let Some(join_handle) = self.join_handle.take() else {
            return Ok(());
        };
        join_handle
            .join()
            .map_err(|_| "query lifecycle sweep task panicked".to_string())
    }
}

impl Drop for QueryLifecycleSweepTask {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

fn compose_backend_application_services(
    data_runtime: BackendDataRuntime,
    execution_runtime_config: ExecutionRuntimeConfig,
    query_lifecycle_config: QueryLifecycleRegistryConfig,
    write_commit_evidence_limits: WriteCommitEvidenceLimits,
    execution_installers: &[Arc<dyn ConnectorExecutionInstaller>],
) -> Result<BackendApplicationServices, BackendApplicationError> {
    let execution_runtime = Arc::new(ExecutionRuntime::new(execution_runtime_config).map_err(
        |error| BackendApplicationError::new(BackendApplicationErrorKind::Configuration, error),
    )?);
    let controls = Arc::new(FragmentControlRegistry::default());
    let exchange_receiver_port: Arc<dyn ExchangeReceiverPort> = Arc::new(
        BackendExchangeReceiverPort::new(Arc::clone(&execution_runtime)),
    );
    let execution_host = seal_connector_execution_host(execution_installers)?;
    let local_runtime = Arc::new(NativeQueryLifecycleLocalRuntime::new(
        Arc::clone(&controls),
        Arc::clone(&execution_host),
    ));
    let query_lifecycle_registry = QueryLifecycleRegistry::new_unbound_with_runtime(
        data_runtime.clone(),
        crate::runtime::start_epoch::start_epoch(),
        local_runtime,
        query_lifecycle_config,
    );
    let connector_registry = Arc::new(ConnectorRegistry::new());
    let native_fragment_service = Arc::new(
        NativeFragmentService::new_with_controls(
            grpc_exchange_transmitter(data_runtime.clone()),
            grpc_fragment_lookup_client(data_runtime),
            native_result_writer(),
            Arc::clone(&controls),
            Arc::clone(&query_lifecycle_registry),
            connector_registry,
            Arc::clone(&execution_host),
            Arc::clone(&execution_runtime),
        )
        .with_write_commit_evidence_limits(write_commit_evidence_limits)
        .with_exchange_receiver_port(Arc::clone(&exchange_receiver_port)),
    );
    controls.publish_resource_snapshot();
    execution_host.publish_resource_snapshot();
    crate::runtime::native_fragment_query::NativeFragmentQueryRuntime::global()
        .publish_resource_snapshot();
    let query_lifecycle_ingress: Arc<dyn QueryLifecycleIngress> =
        Arc::new(BackendStageLifecycleIngress {
            registry: Arc::clone(&query_lifecycle_registry),
            fragments: Arc::clone(&native_fragment_service),
        });
    Ok(BackendApplicationServices {
        native_fragment_service,
        query_lifecycle_registry,
        execution_host,
        execution_runtime,
        exchange_receiver_port,
        query_lifecycle_ingress,
    })
}

fn seal_connector_execution_host(
    execution_installers: &[Arc<dyn ConnectorExecutionInstaller>],
) -> Result<Arc<crate::ConnectorExecutionHost>, BackendApplicationError> {
    #[cfg(test)]
    if execution_installers.is_empty() {
        // Application tests exercise lifecycle wiring without a provider
        // composition root. Production startup never takes this branch.
        return Ok(Arc::new(crate::ConnectorExecutionHost::empty_for_tests()));
    }
    crate::ConnectorExecutionHost::try_new(execution_installers.iter().cloned())
        .map(Arc::new)
        .map_err(|error| {
            BackendApplicationError::new(
                BackendApplicationErrorKind::Configuration,
                format!("seal connector execution installer set: {error}"),
            )
        })
}

impl BackendApplicationHost {
    pub fn open(
        config: BackendServerConfig,
        data_runtime: BackendDataRuntime,
    ) -> Result<Self, BackendApplicationError> {
        Self::open_with_readiness_timeout(config, data_runtime, READINESS_TIMEOUT)
    }

    pub fn ready_marker(&self) -> &str {
        &self.ready_marker
    }

    /// Return the actual listener endpoint in a form a same-process frontend
    /// can dial.  A wildcard bind remains a listener concern; composition must
    /// use loopback rather than attempting to connect to `0.0.0.0` or `::`.
    pub fn connectable_native_endpoint(&self) -> SocketAddr {
        let bound = self.grpc_server.bound_addr();
        let ip = if bound.ip().is_unspecified() {
            match bound.ip() {
                std::net::IpAddr::V4(_) => std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
                std::net::IpAddr::V6(_) => std::net::IpAddr::V6(std::net::Ipv6Addr::LOCALHOST),
            }
        } else {
            bound.ip()
        };
        SocketAddr::new(ip, bound.port())
    }

    pub fn poll_failure(
        &mut self,
    ) -> Result<Option<BackendApplicationError>, BackendApplicationError> {
        self.grpc_server
            .poll_failure()
            .map_err(|error| {
                BackendApplicationError::new(BackendApplicationErrorKind::Supervision, error)
            })
            .map(|failure| {
                failure.map(|error| {
                    BackendApplicationError::new(BackendApplicationErrorKind::Supervision, error)
                })
            })
    }

    pub fn shutdown(mut self) -> Result<(), BackendApplicationError> {
        let listener_shutdown = self.grpc_server.stop();
        let execution_shutdown = self
            .execution_host
            .shutdown()
            .map_err(|error| error.to_string());
        let sweep_result = self.query_lifecycle_sweep.stop();
        let metrics_result = self.metrics_http_server.stop();
        combine_shutdown_results(listener_shutdown, sweep_result)
            .and(metrics_result)
            .and(execution_shutdown)
            .map_err(|error| {
                BackendApplicationError::new(BackendApplicationErrorKind::Shutdown, error)
            })
    }

    fn open_with_readiness_timeout(
        config: BackendServerConfig,
        data_runtime: BackendDataRuntime,
        readiness_timeout: Duration,
    ) -> Result<Self, BackendApplicationError> {
        let BackendServerConfig {
            bind_host,
            grpc_port,
            metrics_http_port,
            advertise_endpoint,
            query_lifecycle_sweep_interval,
            query_lifecycle_config,
            write_commit_evidence_limits,
            execution_runtime_config,
            execution_installers,
        } = config;
        let readiness_addr =
            advertised_probe_addr(&advertise_endpoint.host, advertise_endpoint.port).map_err(
                |error| {
                    BackendApplicationError::new(BackendApplicationErrorKind::Configuration, error)
                },
            )?;
        let services = compose_backend_application_services(
            data_runtime,
            execution_runtime_config,
            query_lifecycle_config,
            write_commit_evidence_limits,
            &execution_installers,
        )?;
        let metrics_http_server = if metrics_http_port == grpc_port {
            MetricsHttpServer::shared_with_grpc()
        } else {
            MetricsHttpServer::start(&bind_host, metrics_http_port).map_err(|error| {
                BackendApplicationError::new(BackendApplicationErrorKind::Start, error)
            })?
        };
        let native_fragment_service = Arc::clone(&services.native_fragment_service);
        let mut query_lifecycle_sweep = QueryLifecycleSweepTask::start(
            Arc::clone(&services.query_lifecycle_registry),
            query_lifecycle_sweep_interval,
        )
        .map_err(|error| BackendApplicationError::new(BackendApplicationErrorKind::Start, error))?;

        let runtime_filter_ingress: Arc<dyn BackendRuntimeFilterEnvelopeIngress> =
            services.query_lifecycle_registry.clone();
        let mut grpc_server = BackendRpcServerHandle::start(
            &bind_host,
            grpc_port,
            BackendRpcService::new(
                native_fragment_service.clone(),
                services.query_lifecycle_ingress.clone(),
                runtime_filter_ingress,
                Arc::clone(&services.exchange_receiver_port),
            ),
        )
        .map_err(|error| {
            let _ = query_lifecycle_sweep.stop();
            BackendApplicationError::new(
                BackendApplicationErrorKind::Start,
                format!("start native backend gRPC server on {bind_host}:{grpc_port}: {error}"),
            )
        })?;

        if let Err(error) = wait_for_tcp_ready(readiness_addr, readiness_timeout) {
            let listener_result = grpc_server.stop();
            let _ = query_lifecycle_sweep.stop();
            let _ = metrics_http_server.stop();
            let primary = BackendApplicationError::new(
                BackendApplicationErrorKind::Readiness,
                format!("advertised endpoint readiness failed: {error}"),
            );
            return Err(match listener_result {
                Ok(()) => primary,
                Err(cleanup_error) => primary.with_cleanup_context(cleanup_error),
            });
        }

        Ok(Self {
            ready_marker: format!(
                "NOVAROCKS_READY role=be grpc_port={grpc_port} advertise_host={} pid={}",
                advertise_endpoint.host,
                std::process::id()
            ),
            grpc_server,
            _native_fragment_service: native_fragment_service,
            _query_lifecycle_registry: services.query_lifecycle_registry,
            execution_host: services.execution_host,
            _execution_runtime: services.execution_runtime,
            query_lifecycle_sweep,
            metrics_http_server,
        })
    }
}

#[allow(
    dead_code,
    reason = "This library entrypoint is invoked by the backend server binary, not backend lib tests."
)]
pub fn run_backend_server(config: BackendServerConfig) -> Result<(), BackendApplicationError> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(novarocks_types::WORKER_STACK_SIZE_BYTES)
        .build()
        .map_err(|error| {
            BackendApplicationError::new(
                BackendApplicationErrorKind::Start,
                format!("build backend Tokio runtime failed: {error}"),
            )
        })?;
    let data_runtime = BackendDataRuntime::new(runtime.handle().clone());
    runtime.block_on(run_backend_server_until_signal(config, data_runtime))
}
pub async fn run_backend_server_until_shutdown<F>(
    config: BackendServerConfig,
    data_runtime: BackendDataRuntime,
    shutdown: F,
) -> Result<(), BackendApplicationError>
where
    F: Future<Output = ()> + Send,
{
    run_backend_server_until(config, data_runtime, async move {
        shutdown.await;
        Ok(())
    })
    .await
}

pub async fn run_backend_server_until_signal(
    config: BackendServerConfig,
    data_runtime: BackendDataRuntime,
) -> Result<(), BackendApplicationError> {
    #[cfg(unix)]
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .map_err(|error| {
        BackendApplicationError::new(
            BackendApplicationErrorKind::Signal,
            format!("install SIGINT listener failed: {error}"),
        )
    })?;

    run_backend_server_until(config, data_runtime, async {
        #[cfg(unix)]
        {
            // Register the OS handler before the host emits its ready marker.
            // A supervisor can otherwise deliver SIGINT in the narrow window
            // between readiness and the first poll of `tokio::signal::ctrl_c`.
            interrupt.recv().await;
            Ok(())
        }
        #[cfg(not(unix))]
        tokio::signal::ctrl_c().await.map_err(|error| {
            BackendApplicationError::new(
                BackendApplicationErrorKind::Signal,
                format!("Ctrl-C listener failed: {error}"),
            )
        })
    })
    .await
}

async fn run_backend_server_until<F>(
    config: BackendServerConfig,
    data_runtime: BackendDataRuntime,
    shutdown: F,
) -> Result<(), BackendApplicationError>
where
    F: Future<Output = Result<(), BackendApplicationError>> + Send,
{
    let mut host = BackendApplicationHost::open(config, data_runtime)?;
    println!("{}", host.ready_marker());
    tokio::pin!(shutdown);

    let primary = loop {
        tokio::select! {
            signal_result = &mut shutdown => break signal_result,
            _ = tokio::time::sleep(SUPERVISION_POLL_INTERVAL) => match host.poll_failure() {
                Ok(Some(error)) | Err(error) => break Err(error),
                Ok(None) => {}
            },
        }
    };

    let primary = match primary {
        Ok(()) => match host.poll_failure() {
            Ok(Some(error)) | Err(error) => Err(error),
            Ok(None) => Ok(()),
        },
        Err(error) => Err(error),
    };
    combine_primary_and_shutdown(primary, host.shutdown())
}

fn combine_primary_and_shutdown(
    primary: Result<(), BackendApplicationError>,
    shutdown: Result<(), BackendApplicationError>,
) -> Result<(), BackendApplicationError> {
    match (primary, shutdown) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(primary), Ok(())) => Err(primary),
        (Ok(()), Err(shutdown)) => Err(shutdown),
        (Err(primary), Err(shutdown)) => Err(primary.with_cleanup_context(shutdown)),
    }
}

fn combine_shutdown_results(
    listener: Result<(), String>,
    sweep: Result<(), String>,
) -> Result<(), String> {
    match (listener, sweep) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Err(sweep), Err(resources)) => Err(format!("{sweep}; {resources}")),
    }
}

fn advertised_probe_addr(host: &str, port: u16) -> Result<SocketAddr, String> {
    let host = host
        .trim()
        .trim_matches(|character| character == '[' || character == ']');
    (host, port)
        .to_socket_addrs()
        .map_err(|error| format!("resolve advertised endpoint {host}:{port}: {error}"))?
        .next()
        .ok_or_else(|| format!("advertised endpoint {host}:{port} resolved no addresses"))
}

fn wait_for_tcp_ready(addr: SocketAddr, timeout: Duration) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    let mut last_error = None;
    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let attempt_timeout = remaining.min(Duration::from_millis(100));
        match TcpStream::connect_timeout(&addr, attempt_timeout) {
            Ok(_) => return Ok(()),
            Err(error) => last_error = Some(error),
        }
        std::thread::sleep(remaining.min(Duration::from_millis(10)));
    }
    match last_error {
        Some(error) => Err(format!(
            "advertised endpoint {addr} did not become ready within {}ms: {error}",
            timeout.as_millis()
        )),
        None => Err(format!(
            "advertised endpoint {addr} did not become ready within {}ms",
            timeout.as_millis()
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::net::TcpListener;
    use std::sync::{Arc, LazyLock, Mutex};
    use std::time::Duration;

    use super::{
        BackendApplicationError, BackendApplicationErrorKind, BackendApplicationHost,
        BackendServerConfig, QueryLifecycleRegistryConfig, combine_primary_and_shutdown,
        compose_backend_application_services,
    };
    use crate::rpc::transport::nova_rocks_grpc_client::NovaRocksGrpcClient;
    use novarocks_execution::runtime::execution_runtime::{
        ExecutionRuntimeConfig, ExecutionSpillStorageConfig,
    };
    use novarocks_proto::lifecycle::{
        AttemptId, ParticipantBackendIdentity, ParticipantManifest, ParticipantManifestDigest,
        ParticipantRole, QueryAbortRequest, QueryControlEndpoint, QueryExecutionId,
        QueryInitRequest, QueryOptions, QueryTerminationReason,
    };
    use novarocks_proto::novarocks::{
        AbortQueryRequest as ProtoAbortQueryRequest, HeartbeatRequest,
        InitQueryRequest as ProtoInitQueryRequest, QueryControlAttach as ProtoQueryControlAttach,
        QueryControlRequest as ProtoQueryControlRequest,
    };
    use novarocks_proto::{lifecycle as protocol_lifecycle, novarocks as protocol};
    use novarocks_spi::connector::WriteCommitEvidenceLimits;
    use novarocks_types::AdvertiseEndpoint;
    use novarocks_types::QueryId;
    use novarocks_version::native_build_identity;
    use tokio_stream::wrappers::ReceiverStream;

    static LIVE_HOST_TEST: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    fn unused_port() -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
        let port = listener
            .local_addr()
            .expect("read ephemeral address")
            .port();
        drop(listener);
        port
    }

    fn execution_runtime_config() -> ExecutionRuntimeConfig {
        ExecutionRuntimeConfig {
            driver_threads: 1,
            scan_threads: 1,
            scan_queue_capacity: 1,
            spill_io_threads: 1,
            spill_io_queue_capacity: 1,
            spill_storage: ExecutionSpillStorageConfig::default(),
            exchange_wait_ms: 1,
            exchange_io_threads: 1,
            exchange_io_max_inflight_bytes: 1,
            exchange_max_transmit_batched_bytes: 1,
            operator_buffer_chunks: 1,
            local_exchange_buffer_mem_limit_per_driver: 1,
            local_exchange_max_buffered_rows: -1,
            connector_io_tasks_per_scan_operator: 1,
            scan_submit_fail_max: 1,
            scan_submit_fail_timeout_ms: 1,
            runtime_filter_scan_wait_time_ms_override: None,
            runtime_filter_wait_timeout_ms_override: None,
            sink_io_worker_threads: 1,
            sink_io_max_blocking_threads: 1,
        }
    }

    fn test_data_runtime() -> crate::BackendDataRuntime {
        crate::rpc::runtime::test_backend_data_runtime()
    }

    fn query_lifecycle_registry_config(
        heartbeat_timeout: Duration,
    ) -> QueryLifecycleRegistryConfig {
        QueryLifecycleRegistryConfig::new(
            4_096,
            16_384,
            Duration::from_millis(120_000),
            heartbeat_timeout,
            Duration::from_millis(30_000),
            256,
            32,
            48 * 1024 * 1024,
            256 * 1024 * 1024,
            512,
            48 * 1024 * 1024,
            Duration::from_millis(30_000),
            Duration::from_millis(5_000),
            Duration::from_millis(5_000),
            5,
            Duration::from_millis(100),
            Duration::from_millis(1_000),
            Duration::from_millis(120_000),
            4_096,
            256 * 1024 * 1024,
        )
    }

    fn backend_config(grpc_port: u16, advertise_port: u16) -> BackendServerConfig {
        BackendServerConfig {
            bind_host: "127.0.0.1".to_string(),
            grpc_port,
            metrics_http_port: grpc_port,
            advertise_endpoint: AdvertiseEndpoint {
                host: "127.0.0.1".to_string(),
                port: advertise_port,
            },
            query_lifecycle_sweep_interval: Duration::from_millis(1_000),
            query_lifecycle_config: query_lifecycle_registry_config(Duration::from_millis(5_000)),
            write_commit_evidence_limits: WriteCommitEvidenceLimits::default(),
            execution_runtime_config: execution_runtime_config(),
            execution_installers: Vec::new(),
        }
    }

    fn live_query_init_request(start_epoch: u64, query_low: i64) -> QueryInitRequest {
        let execution_id = QueryExecutionId::new(
            QueryId::new(0x514c_4302, query_low),
            AttemptId::new(1).expect("nonzero attempt"),
        )
        .expect("valid execution id");
        QueryInitRequest::from_manifest(
            ParticipantManifest::new(
                execution_id,
                ParticipantBackendIdentity::new(
                    7,
                    QueryControlEndpoint::new("127.0.0.1", 9030).expect("valid backend endpoint"),
                    start_epoch,
                )
                .expect("valid backend identity"),
                [ParticipantRole::FragmentExecutor],
                [novarocks_proto::common::UniqueId {
                    hi: query_low,
                    lo: 1,
                }],
                QueryOptions::parse(protocol::QueryOptions::default())
                    .expect("valid default query options"),
                10_000,
                [],
                None,
                std::time::Duration::from_secs(30),
                QueryControlEndpoint::new("127.0.0.1", 9031).expect("valid report endpoint"),
            )
            .expect("valid participant manifest"),
        )
    }

    fn protocol_init_request(request: &QueryInitRequest) -> protocol_lifecycle::QueryInitRequest {
        request.clone()
    }

    fn heartbeat_command(sequence: u64, sent_mono_ns: u64) -> ProtoQueryControlRequest {
        ProtoQueryControlRequest {
            command: Some(protocol::query_control_request::Command::Heartbeat(
                protocol::QueryControlHeartbeat {
                    sequence,
                    sent_mono_ns,
                },
            )),
        }
    }

    fn abort_command(reason: impl Into<String>) -> ProtoQueryControlRequest {
        ProtoQueryControlRequest {
            command: Some(protocol::query_control_request::Command::Abort(
                protocol::QueryControlAbort {
                    reason: reason.into(),
                },
            )),
        }
    }

    fn assert_event(
        event: protocol::QueryControlResponse,
        predicate: impl FnOnce(protocol::query_control_response::Event) -> bool,
    ) {
        assert!(predicate(event.event.expect("query control event")));
    }

    fn protocol_control_attach(
        init: &protocol_lifecycle::QueryInitRequest,
        frontend_owner_epoch: u64,
    ) -> ProtoQueryControlRequest {
        let manifest = init.manifest().expect("validated InitQuery has manifest");
        ProtoQueryControlRequest {
            command: Some(protocol::query_control_request::Command::Attach(
                ProtoQueryControlAttach {
                    execution_id: manifest.as_proto().execution_id,
                    init_digest: init
                        .digest()
                        .expect("validated InitQuery has digest")
                        .as_bytes()
                        .to_vec(),
                    frontend_owner_epoch,
                },
            )),
        }
    }

    fn protocol_abort_request(
        init: &protocol_lifecycle::QueryInitRequest,
        digest: &[u8],
        reason: impl Into<String>,
    ) -> ProtoAbortQueryRequest {
        let manifest = init.manifest().expect("validated InitQuery has manifest");
        ProtoAbortQueryRequest {
            execution_id: manifest.as_proto().execution_id,
            init_digest: digest.to_vec(),
            reason: reason.into(),
        }
    }

    async fn connect_live_client(grpc_port: u16) -> NovaRocksGrpcClient<tonic::transport::Channel> {
        NovaRocksGrpcClient::connect(format!("http://127.0.0.1:{grpc_port}"))
            .await
            .expect("connect native backend gRPC")
            .max_encoding_message_size(64 * 1024 * 1024)
            .max_decoding_message_size(64 * 1024 * 1024)
    }

    #[test]
    fn application_composition_owns_one_query_lifecycle_registry() {
        let services = compose_backend_application_services(
            test_data_runtime(),
            execution_runtime_config(),
            query_lifecycle_registry_config(Duration::from_millis(5_000)),
            WriteCommitEvidenceLimits::default(),
            &[],
        )
        .expect("compose backend application services");

        assert_eq!(
            Arc::strong_count(&services.query_lifecycle_registry),
            3,
            "application, Stage ingress, and fragment service must share exactly one registry"
        );
    }

    #[test]
    fn readiness_failure_stops_and_joins_started_listener() {
        let _live_host = LIVE_HOST_TEST.lock().expect("live host test lock");
        let grpc_port = unused_port();
        let mut config = backend_config(grpc_port, grpc_port);
        config.advertise_endpoint.host = "127.0.0.2".to_string();
        let error = BackendApplicationHost::open_with_readiness_timeout(
            config,
            test_data_runtime(),
            std::time::Duration::from_millis(25),
        )
        .expect_err("unreachable advertised endpoint must fail readiness");

        assert_eq!(error.kind(), BackendApplicationErrorKind::Readiness);
        TcpListener::bind(("127.0.0.1", grpc_port))
            .expect("readiness cleanup must release the started listener");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[expect(
        clippy::await_holding_lock,
        reason = "The mutex serializes loopback backend tests that bind listeners and must remain held for the full test."
    )]
    async fn application_query_control_attachment_live_loopback_round_trip() {
        let _live_host = LIVE_HOST_TEST.lock().expect("live host test lock");
        let grpc_port = unused_port();
        let host =
            BackendApplicationHost::open(backend_config(grpc_port, grpc_port), test_data_runtime())
                .expect("native backend host starts");
        let mut client = connect_live_client(grpc_port).await;
        let heartbeat = client
            .heartbeat(HeartbeatRequest {
                assigned_be_id: 7,
                fe_epoch: 1,
            })
            .await
            .expect("bind backend identity")
            .into_inner();
        assert_eq!(heartbeat.version, native_build_identity());
        let init = live_query_init_request(heartbeat.start_epoch, 901);
        let protocol_init = protocol_init_request(&init);
        client
            .init_query(protocol_init.as_proto().clone())
            .await
            .expect("InitQuery succeeds");

        let (commands, command_rx) = tokio::sync::mpsc::channel(4);
        commands
            .send(protocol_control_attach(&protocol_init, 9))
            .await
            .expect("send Attach");
        let mut events = client
            .query_control_stream(ReceiverStream::new(command_rx))
            .await
            .expect("attach QueryControlStream")
            .into_inner();
        assert_event(
            events
                .message()
                .await
                .expect("read ControlReady")
                .expect("ControlReady"),
            |event| {
                matches!(
                    event,
                    protocol::query_control_response::Event::ControlReady(_)
                )
            },
        );
        commands
            .send(heartbeat_command(77, 123))
            .await
            .expect("send heartbeat");
        assert_event(
            events
                .message()
                .await
                .expect("read HeartbeatAck")
                .expect("HeartbeatAck"),
            |event| {
                matches!(
                    event,
                    protocol::query_control_response::Event::HeartbeatAck(
                        protocol::QueryControlHeartbeatAck { sequence: 77 }
                    )
                )
            },
        );
        commands
            .send(abort_command("live loopback cancellation"))
            .await
            .expect("send Abort");
        assert_event(
            events
                .message()
                .await
                .expect("read TerminationAccepted")
                .expect("TerminationAccepted"),
            |event| {
                matches!(
                    event,
                    protocol::query_control_response::Event::TerminationAccepted(
                        protocol::QueryControlTerminationAccepted { reason }
                    ) if reason == QueryTerminationReason::CoordinatorAbort as i32
                )
            },
        );
        drop(events);
        drop(commands);
        host.shutdown().expect("native backend shutdown");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[expect(
        clippy::await_holding_lock,
        reason = "The mutex serializes loopback backend tests that bind listeners and must remain held for the full test."
    )]
    async fn application_query_control_heartbeat_timeout_fails_closed_with_open_socket() {
        let _live_host = LIVE_HOST_TEST.lock().expect("live host test lock");
        let grpc_port = unused_port();
        let mut config = backend_config(grpc_port, grpc_port);
        config.query_lifecycle_sweep_interval = Duration::from_millis(50);
        config.query_lifecycle_config = query_lifecycle_registry_config(Duration::from_millis(250));
        let host = BackendApplicationHost::open(config, test_data_runtime())
            .expect("native backend host starts");
        let mut client = connect_live_client(grpc_port).await;
        let heartbeat = client
            .heartbeat(HeartbeatRequest {
                assigned_be_id: 7,
                fe_epoch: 1,
            })
            .await
            .expect("bind backend identity")
            .into_inner();
        let init = live_query_init_request(heartbeat.start_epoch, 902);
        let protocol_init = protocol_init_request(&init);
        client
            .init_query(protocol_init.as_proto().clone())
            .await
            .expect("InitQuery succeeds");
        let (commands, command_rx) = tokio::sync::mpsc::channel(1);
        commands
            .send(protocol_control_attach(&protocol_init, 9))
            .await
            .expect("send Attach");
        let mut events = client
            .query_control_stream(ReceiverStream::new(command_rx))
            .await
            .expect("attach QueryControlStream")
            .into_inner();
        let _ = events
            .message()
            .await
            .expect("read ControlReady")
            .expect("ControlReady");

        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
        assert_event(
            tokio::time::timeout(std::time::Duration::from_secs(1), events.message())
                .await
                .expect("timeout termination event arrives")
                .expect("read timeout termination event")
                .expect("timeout TerminationAccepted"),
            |event| {
                matches!(
                    event,
                    protocol::query_control_response::Event::TerminationAccepted(
                        protocol::QueryControlTerminationAccepted { reason }
                    ) if reason == QueryTerminationReason::CoordinatorHeartbeatTimeout as i32
                )
            },
        );
        let termination = client
            .abort_query(protocol_abort_request(
                &protocol_init,
                protocol_init
                    .digest()
                    .expect("validated InitQuery has digest")
                    .as_bytes(),
                "probe latched timeout",
            ))
            .await
            .expect("AbortQuery observes termination")
            .into_inner();
        assert_eq!(
            termination.accepted_reason,
            novarocks_proto::novarocks::QueryTerminationReason::
                QueryTerminationCoordinatorHeartbeatTimeout as i32
        );

        drop(events);
        drop(commands);
        host.shutdown().expect("native backend shutdown");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[expect(
        clippy::await_holding_lock,
        reason = "The mutex serializes loopback backend tests that bind listeners and must remain held for the full test."
    )]
    async fn application_shutdown_closes_live_query_control_stream_and_fails_closed() {
        let _live_host = LIVE_HOST_TEST.lock().expect("live host test lock");
        let grpc_port = unused_port();
        let host =
            BackendApplicationHost::open(backend_config(grpc_port, grpc_port), test_data_runtime())
                .expect("native backend host starts");
        let registry = Arc::clone(&host._query_lifecycle_registry);
        let mut client = connect_live_client(grpc_port).await;
        let heartbeat = client
            .heartbeat(HeartbeatRequest {
                assigned_be_id: 7,
                fe_epoch: 1,
            })
            .await
            .expect("bind backend identity")
            .into_inner();
        let init = live_query_init_request(heartbeat.start_epoch, 903);
        let protocol_init = protocol_init_request(&init);
        client
            .init_query(protocol_init.as_proto().clone())
            .await
            .expect("InitQuery succeeds");
        let (commands, command_rx) = tokio::sync::mpsc::channel(1);
        commands
            .send(protocol_control_attach(&protocol_init, 9))
            .await
            .expect("send Attach");
        let mut events = client
            .query_control_stream(ReceiverStream::new(command_rx))
            .await
            .expect("attach QueryControlStream")
            .into_inner();
        let _ = events
            .message()
            .await
            .expect("read ControlReady")
            .expect("ControlReady");
        for sequence in 1..=17 {
            commands
                .send(heartbeat_command(sequence, sequence))
                .await
                .expect("send heartbeat without draining ACKs");
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        let (shutdown_tx, shutdown_rx) = std::sync::mpsc::sync_channel(1);
        let shutdown_thread = std::thread::spawn(move || {
            let _ = shutdown_tx.send(host.shutdown());
        });
        let early_shutdown = shutdown_rx.recv_timeout(std::time::Duration::from_millis(500));
        let returned_while_stream_live = early_shutdown.is_ok();

        // Always release the old implementation's graceful-shutdown wait so RED
        // leaves no global listener or detached thread behind.
        drop(events);
        drop(commands);
        let shutdown = match early_shutdown {
            Ok(result) => result,
            Err(_) => shutdown_rx
                .recv_timeout(std::time::Duration::from_secs(2))
                .expect("shutdown completes after releasing the stream"),
        };
        shutdown_thread.join().expect("join shutdown thread");
        shutdown.expect("native backend shutdown");
        assert!(
            returned_while_stream_live,
            "host shutdown must not wait indefinitely for a live bidi stream"
        );

        let termination = registry
            .abort_query(QueryAbortRequest::new(
                init.manifest()
                    .expect("validated init manifest")
                    .execution_id()
                    .expect("validated manifest execution id"),
                ParticipantManifestDigest::new(
                    *protocol_init
                        .digest()
                        .expect("validated InitQuery has digest")
                        .as_bytes(),
                ),
                "observe fail-closed shutdown",
            ))
            .expect("observe latched shutdown termination");
        assert_eq!(
            termination
                .accepted_reason()
                .expect("validated termination reason"),
            QueryTerminationReason::CoordinatorStreamLost
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[expect(
        clippy::await_holding_lock,
        reason = "The mutex serializes loopback backend tests that bind listeners and must remain held for the full test."
    )]
    async fn application_malformed_init_query_returns_invalid_argument() {
        let _live_host = LIVE_HOST_TEST.lock().expect("live host test lock");
        let grpc_port = unused_port();
        let host =
            BackendApplicationHost::open(backend_config(grpc_port, grpc_port), test_data_runtime())
                .expect("native backend host starts");
        let mut client = connect_live_client(grpc_port).await;

        let error = client
            .init_query(ProtoInitQueryRequest::default())
            .await
            .expect_err("malformed InitQuery must be a transport-visible error");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);

        host.shutdown().expect("native backend shutdown");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[expect(
        clippy::await_holding_lock,
        reason = "The mutex serializes loopback backend tests that bind listeners and must remain held for the full test."
    )]
    async fn application_malformed_abort_query_returns_invalid_argument() {
        let _live_host = LIVE_HOST_TEST.lock().expect("live host test lock");
        let grpc_port = unused_port();
        let host =
            BackendApplicationHost::open(backend_config(grpc_port, grpc_port), test_data_runtime())
                .expect("native backend host starts");
        let mut client = connect_live_client(grpc_port).await;

        let error = client
            .abort_query(ProtoAbortQueryRequest::default())
            .await
            .expect_err("malformed AbortQuery must be a transport-visible error");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);

        host.shutdown().expect("native backend shutdown");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[expect(
        clippy::await_holding_lock,
        reason = "The mutex serializes loopback backend tests that bind listeners and must remain held for the full test."
    )]
    async fn application_abort_digest_mismatch_is_rejected_without_terminating_entry() {
        let _live_host = LIVE_HOST_TEST.lock().expect("live host test lock");
        let grpc_port = unused_port();
        let host =
            BackendApplicationHost::open(backend_config(grpc_port, grpc_port), test_data_runtime())
                .expect("native backend host starts");
        let mut client = connect_live_client(grpc_port).await;
        let heartbeat = client
            .heartbeat(HeartbeatRequest {
                assigned_be_id: 7,
                fe_epoch: 1,
            })
            .await
            .expect("bind backend identity")
            .into_inner();
        let init = live_query_init_request(heartbeat.start_epoch, 904);
        let different = live_query_init_request(heartbeat.start_epoch, 905);
        let protocol_init = protocol_init_request(&init);
        let protocol_different = protocol_init_request(&different);
        client
            .init_query(protocol_init.as_proto().clone())
            .await
            .expect("InitQuery succeeds");

        let error = client
            .abort_query(protocol_abort_request(
                &protocol_init,
                protocol_different
                    .digest()
                    .expect("validated InitQuery has digest")
                    .as_bytes(),
                "mismatched digest",
            ))
            .await
            .expect_err("digest mismatch must be rejected");
        assert_eq!(error.code(), tonic::Code::AlreadyExists);

        let (commands, command_rx) = tokio::sync::mpsc::channel(1);
        commands
            .send(protocol_control_attach(&protocol_init, 9))
            .await
            .expect("send Attach");
        let mut events = client
            .query_control_stream(ReceiverStream::new(command_rx))
            .await
            .expect("mismatched abort leaves entry attachable")
            .into_inner();
        assert_event(
            events
                .message()
                .await
                .expect("read ControlReady")
                .expect("ControlReady"),
            |event| {
                matches!(
                    event,
                    protocol::query_control_response::Event::ControlReady(_)
                )
            },
        );

        drop(events);
        drop(commands);
        host.shutdown().expect("native backend shutdown");
    }

    #[test]
    fn supervision_error_remains_primary_when_shutdown_also_fails() {
        let error = combine_primary_and_shutdown(
            Err(BackendApplicationError::new(
                BackendApplicationErrorKind::Supervision,
                "gRPC server exited",
            )),
            Err(BackendApplicationError::new(
                BackendApplicationErrorKind::Shutdown,
                "gRPC join failed",
            )),
        )
        .expect_err("supervision failure must be returned");

        assert_eq!(error.kind(), BackendApplicationErrorKind::Supervision);
        assert!(
            error
                .to_string()
                .contains("cleanup failed: Shutdown: gRPC join failed")
        );
    }
}
