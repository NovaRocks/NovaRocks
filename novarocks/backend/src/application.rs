use std::fmt;
use std::future::Future;
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::time::{Duration, Instant};

use novarocks::common::app_config::{self, NovaRocksConfig};
use novarocks::common::network;
use novarocks::query_execution::report::{NativeReportHandler, NativeReportHandlerError};
use novarocks::service::{grpc_server, report_worker};

use crate::fragment::{
    NativeFragmentService, grpc_exchange_transmitter, grpc_fragment_lookup_client,
    native_fragment_event_sink, native_result_writer,
};

const READINESS_TIMEOUT: Duration = Duration::from_secs(5);
const SUPERVISION_POLL_INTERVAL: Duration = Duration::from_millis(50);
const BACKEND_REPORT_ROLE_REJECTION: &str =
    "native backend role does not own coordinator report ingress";

struct BackendNativeReportHandler;

impl NativeReportHandler for BackendNativeReportHandler {
    fn handle_native_report(
        &self,
        _report: novarocks::proto::novarocks::ExecStatusReport,
    ) -> Result<(), NativeReportHandlerError> {
        Err(NativeReportHandlerError::role_rejected(
            BACKEND_REPORT_ROLE_REJECTION,
        ))
    }
}

pub fn backend_native_report_handler() -> Arc<dyn NativeReportHandler> {
    Arc::new(BackendNativeReportHandler)
}

pub struct BackendServerConfig {
    pub config: NovaRocksConfig,
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

#[derive(Debug)]
pub struct BackendApplicationHost {
    ready_marker: String,
    _native_fragment_service: Arc<NativeFragmentService>,
}

impl BackendApplicationHost {
    pub fn open(config: BackendServerConfig) -> Result<Self, BackendApplicationError> {
        Self::open_with_readiness_timeout(config, READINESS_TIMEOUT)
    }

    /// Starts a native backend whose report ingress is owned by the supplied
    /// coordinator. This is used only by the all-in-one composition root.
    pub fn open_with_native_report_handler(
        config: BackendServerConfig,
        native_report_handler: Arc<dyn NativeReportHandler>,
    ) -> Result<Self, BackendApplicationError> {
        Self::open_with_readiness_timeout_and_report_handler(
            config,
            READINESS_TIMEOUT,
            native_report_handler,
        )
    }

    pub fn ready_marker(&self) -> &str {
        &self.ready_marker
    }

    pub fn poll_failure(
        &mut self,
    ) -> Result<Option<BackendApplicationError>, BackendApplicationError> {
        grpc_server::poll_grpc_server_failure()
            .map_err(|error| {
                BackendApplicationError::new(BackendApplicationErrorKind::Supervision, error)
            })
            .map(|failure| {
                failure.map(|error| {
                    BackendApplicationError::new(BackendApplicationErrorKind::Supervision, error)
                })
            })
    }

    pub fn shutdown(self) -> Result<(), BackendApplicationError> {
        stop_backend_resources().map_err(|error| {
            BackendApplicationError::new(BackendApplicationErrorKind::Shutdown, error)
        })
    }

    fn open_with_readiness_timeout(
        config: BackendServerConfig,
        readiness_timeout: Duration,
    ) -> Result<Self, BackendApplicationError> {
        Self::open_with_readiness_timeout_and_report_handler(
            config,
            readiness_timeout,
            backend_native_report_handler(),
        )
    }

    fn open_with_readiness_timeout_and_report_handler(
        config: BackendServerConfig,
        readiness_timeout: Duration,
        native_report_handler: Arc<dyn NativeReportHandler>,
    ) -> Result<Self, BackendApplicationError> {
        let config = config.config;
        app_config::install_preloaded_config(config.clone());

        let advertise_endpoint = network::standalone_advertise_endpoint_for_config(&config)
            .map_err(|error| {
                BackendApplicationError::new(BackendApplicationErrorKind::Configuration, error)
            })?;
        let readiness_addr =
            advertised_probe_addr(&advertise_endpoint.host, advertise_endpoint.port).map_err(
                |error| {
                    BackendApplicationError::new(BackendApplicationErrorKind::Configuration, error)
                },
            )?;
        let bind_host = config.server.host.clone();
        let grpc_port = config.server.grpc_port;
        let native_fragment_service = Arc::new(NativeFragmentService::new(
            grpc_exchange_transmitter(),
            grpc_fragment_lookup_client(),
            native_result_writer(),
            native_fragment_event_sink(),
        ));

        grpc_server::start_grpc_exchange_server(
            &bind_host,
            grpc_port,
            native_fragment_service.clone(),
            native_report_handler,
        )
        .map_err(|error| {
            BackendApplicationError::new(
                BackendApplicationErrorKind::Start,
                format!("start native backend gRPC server on {bind_host}:{grpc_port}: {error}"),
            )
        })?;

        if let Err(error) = wait_for_tcp_ready(readiness_addr, readiness_timeout) {
            return Err(cleanup_after_primary_error(BackendApplicationError::new(
                BackendApplicationErrorKind::Readiness,
                format!("advertised endpoint readiness failed: {error}"),
            )));
        }

        Ok(Self {
            ready_marker: format!(
                "NOVAROCKS_READY role=be grpc_port={grpc_port} advertise_host={} pid={}",
                advertise_endpoint.host,
                std::process::id()
            ),
            _native_fragment_service: native_fragment_service,
        })
    }
}

pub fn run_backend_server(config: BackendServerConfig) -> Result<(), BackendApplicationError> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(novarocks::runtime::global_async_runtime::WORKER_STACK_SIZE_BYTES)
        .build()
        .map_err(|error| {
            BackendApplicationError::new(
                BackendApplicationErrorKind::Start,
                format!("build backend Tokio runtime failed: {error}"),
            )
        })?;
    runtime.block_on(run_backend_server_until_signal(config))
}

pub async fn run_backend_server_until_shutdown<F>(
    config: BackendServerConfig,
    shutdown: F,
) -> Result<(), BackendApplicationError>
where
    F: Future<Output = ()> + Send,
{
    run_backend_server_until(config, async move {
        shutdown.await;
        Ok(())
    })
    .await
}

async fn run_backend_server_until_signal(
    config: BackendServerConfig,
) -> Result<(), BackendApplicationError> {
    run_backend_server_until(config, async {
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
    shutdown: F,
) -> Result<(), BackendApplicationError>
where
    F: Future<Output = Result<(), BackendApplicationError>> + Send,
{
    let mut host = BackendApplicationHost::open(config)?;
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

fn cleanup_after_primary_error(primary: BackendApplicationError) -> BackendApplicationError {
    match stop_backend_resources() {
        Ok(()) => primary,
        Err(cleanup_error) => primary.with_cleanup_context(cleanup_error),
    }
}

fn stop_backend_resources() -> Result<(), String> {
    let grpc_result = grpc_server::stop_grpc_server();
    report_worker::stop();
    grpc_result
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
    use std::sync::{LazyLock, Mutex};

    use super::{
        BackendApplicationError, BackendApplicationErrorKind, BackendApplicationHost,
        BackendServerConfig, combine_primary_and_shutdown,
    };
    use novarocks::common::app_config::NovaRocksConfig;
    use novarocks::proto::common::{Status, UniqueId};
    use novarocks::proto::novarocks::{ExecStatusReport, ReportExecStatusRequest};
    use novarocks::service::grpc_client::NovaRocksGrpcRemoteClient;

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

    fn backend_config(grpc_port: u16, advertise_port: u16) -> BackendServerConfig {
        let mut config = NovaRocksConfig::default();
        config.server.host = "127.0.0.1".to_string();
        config.server.grpc_port = grpc_port;
        config.cluster.advertise_host = "127.0.0.1".to_string();
        config.cluster.advertise_port = advertise_port;
        BackendServerConfig { config }
    }

    #[test]
    fn readiness_failure_stops_and_joins_started_listener() {
        let _live_host = LIVE_HOST_TEST.lock().expect("live host test lock");
        let grpc_port = unused_port();
        let mut config = backend_config(grpc_port, grpc_port);
        config.config.cluster.advertise_host = "127.0.0.2".to_string();
        let error = BackendApplicationHost::open_with_readiness_timeout(
            config,
            std::time::Duration::from_millis(25),
        )
        .expect_err("unreachable advertised endpoint must fail readiness");

        assert_eq!(error.kind(), BackendApplicationErrorKind::Readiness);
        TcpListener::bind(("127.0.0.1", grpc_port))
            .expect("readiness cleanup must release the started listener");
    }

    #[test]
    fn native_backend_rejects_coordinator_reports_with_role_error() {
        let _live_host = LIVE_HOST_TEST.lock().expect("live host test lock");
        let grpc_port = unused_port();
        let host = BackendApplicationHost::open(backend_config(grpc_port, grpc_port))
            .expect("native backend host starts");
        let client = NovaRocksGrpcRemoteClient::new(
            format!("127.0.0.1:{grpc_port}")
                .parse()
                .expect("backend address"),
        )
        .expect("gRPC client");

        let response = client
            .blocking_report_exec_status(ReportExecStatusRequest {
                report: Some(ExecStatusReport {
                    query_id: Some(UniqueId { hi: 41, lo: 73 }),
                    fragment_instance_id: Some(UniqueId { hi: 41, lo: 74 }),
                    status: Some(Status::default()),
                    done: true,
                    ..Default::default()
                }),
            })
            .expect("role rejection is returned as a business response");

        assert_eq!(response.status_code, 1);
        assert_eq!(response.error_code, "NativeReportRoleRejected");
        assert_eq!(
            response.message,
            "native backend role does not own coordinator report ingress"
        );
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
