use std::fmt;
use std::sync::Arc;
#[cfg(test)]
use std::sync::{Mutex, MutexGuard};
use std::thread;
use std::time::Duration;

use crate::thrift::{master_service, types};
use axum::Router;
use novarocks::common::app_config::{self, NovaRocksConfig};
use novarocks::common::network;
use novarocks::connector::starrocks::ports::LakeStorageDependencies;

use crate::backend_service::{self, BackendServiceHandle};
use crate::brpc;
use crate::control::FrontendControlState;
use crate::disk_report::{DiskReportSender, DiskReportWorker};
use crate::fragment::{
    CompatFragmentService, SyncFragmentExecutor, brpc_exchange_transmitter,
    brpc_fragment_lookup_client, compat_result_writer, lake_meta_storage_resolver,
};
use crate::frontend_rpc;
use crate::heartbeat_service::{self, HeartbeatServer};
use crate::lake_storage::CompatLakeStorageService;
use crate::listeners::{self, CompatListenerConfig, CompatListenerGroup};
use crate::load::{
    CompatLoadRegistry, CompatLoadService, LoadTrackingStore, router as load_router,
};
use crate::report::{CompatReportService, new_report_service_with_tracking};

const SUPERVISION_POLL_INTERVAL: Duration = Duration::from_millis(100);
#[cfg(test)]
static APPLICATION_TEST_LOCK: Mutex<()> = Mutex::new(());

struct CompatDiskReportSender;

impl DiskReportSender for CompatDiskReportSender {
    fn send_disk_report(
        &self,
        fe_addr: &types::TNetworkAddress,
        request: &master_service::TReportRequest,
    ) -> Result<(), String> {
        frontend_rpc::report_disk(fe_addr, request.clone())
    }
}

pub struct CompatServerConfig {
    pub config: NovaRocksConfig,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompatApplicationErrorKind {
    Configuration,
    ReportStart,
    GrpcStart,
    HeartbeatStart,
    BackendServiceStart,
    BrpcStart,
    Supervision,
    Shutdown,
}

#[derive(Debug)]
pub struct CompatApplicationError {
    kind: CompatApplicationErrorKind,
    message: String,
}

impl CompatApplicationError {
    fn new(kind: CompatApplicationErrorKind, error: impl fmt::Display) -> Self {
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

    pub const fn kind(&self) -> CompatApplicationErrorKind {
        self.kind
    }
}

impl fmt::Display for CompatApplicationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}: {}", self.kind, self.message)
    }
}

impl std::error::Error for CompatApplicationError {}

#[derive(Default)]
struct StartedResources {
    grpc: bool,
    heartbeat: bool,
    backend: bool,
    brpc: bool,
    report: bool,
    frontend_rpc: bool,
}

pub struct CompatApplicationHost {
    ports: Box<dyn CompatPorts>,
    started: StartedResources,
    fragment_service: Arc<CompatFragmentService>,
    report_service: Arc<CompatReportService>,
    load_service: Arc<CompatLoadService>,
    lake_storage_service: Arc<CompatLakeStorageService>,
    control: Arc<FrontendControlState>,
    disk_report_worker: Arc<DiskReportWorker>,
    tracking: Arc<LoadTrackingStore>,
    ready_marker: String,
    startup_summary: String,
    #[cfg(test)]
    _test_lifecycle_lock: MutexGuard<'static, ()>,
}

impl fmt::Debug for CompatApplicationHost {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CompatApplicationHost")
            .field("started_grpc", &self.started.grpc)
            .field("started_heartbeat", &self.started.heartbeat)
            .field("started_backend", &self.started.backend)
            .field("started_brpc", &self.started.brpc)
            .field("started_report", &self.started.report)
            .finish_non_exhaustive()
    }
}

impl Drop for CompatApplicationHost {
    fn drop(&mut self) {
        let _ = self.cleanup_started();
    }
}

impl CompatApplicationHost {
    pub fn open(config: CompatServerConfig) -> Result<Self, CompatApplicationError> {
        Self::open_with_ports(config, LiveCompatPorts::default())
    }

    pub fn ready_marker(&self) -> &str {
        &self.ready_marker
    }

    pub fn startup_summary(&self) -> &str {
        &self.startup_summary
    }

    pub fn poll_failure(
        &mut self,
    ) -> Result<Option<CompatApplicationError>, CompatApplicationError> {
        if let Some(error) = self.disk_report_worker.poll_failure() {
            return Ok(Some(CompatApplicationError::new(
                CompatApplicationErrorKind::Supervision,
                format!("disk report worker failed: {error}"),
            )));
        }
        let heartbeat_failure = self.ports.poll_heartbeat_failure().map_err(|error| {
            CompatApplicationError::new(
                CompatApplicationErrorKind::Supervision,
                format!("poll heartbeat supervisor failed: {error}"),
            )
        })?;
        if let Some(error) = heartbeat_failure {
            return Ok(Some(CompatApplicationError::new(
                CompatApplicationErrorKind::Supervision,
                error,
            )));
        }
        let backend_failure = self.ports.poll_backend_failure().map_err(|error| {
            CompatApplicationError::new(
                CompatApplicationErrorKind::Supervision,
                format!("poll backend supervisor failed: {error}"),
            )
        })?;
        if let Some(error) = backend_failure {
            return Ok(Some(CompatApplicationError::new(
                CompatApplicationErrorKind::Supervision,
                error,
            )));
        }
        let grpc_failure = self.ports.poll_grpc_failure().map_err(|error| {
            CompatApplicationError::new(
                CompatApplicationErrorKind::Supervision,
                format!("poll compat grpc supervisor failed: {error}"),
            )
        })?;
        if let Some(error) = grpc_failure {
            return Ok(Some(CompatApplicationError::new(
                CompatApplicationErrorKind::Supervision,
                error,
            )));
        }
        Ok(None)
    }

    pub fn shutdown(mut self) -> Result<(), CompatApplicationError> {
        self.cleanup_started().map_err(|error| {
            CompatApplicationError::new(CompatApplicationErrorKind::Shutdown, error)
        })
    }

    fn open_with_ports(
        config: CompatServerConfig,
        ports: impl CompatPorts + 'static,
    ) -> Result<Self, CompatApplicationError> {
        #[cfg(test)]
        let test_lifecycle_lock = APPLICATION_TEST_LOCK
            .lock()
            .expect("compat application test lifecycle lock");
        let config = config.config;
        let advertise_endpoint =
            network::advertise_endpoint_for_config(&config).map_err(|error| {
                CompatApplicationError::new(CompatApplicationErrorKind::Configuration, error)
            })?;
        let memory_limit = config
            .runtime
            .effective_be_mem_limit_bytes()
            .map_err(|error| {
                CompatApplicationError::new(CompatApplicationErrorKind::Configuration, error)
            })?;
        let query_threads = config
            .runtime
            .actual_internal_service_query_rpc_threads()
            .min(u32::MAX as usize) as u32;
        let log_level = compat_log_level(&config.log_level);
        app_config::install_preloaded_config(config.clone());

        let server = &config.server;
        let ready_marker = format!(
            "NOVAROCKS_READY role=compat-be heartbeat_port={} brpc_port={} grpc_port={} pid={}",
            server.heartbeat_port,
            server.brpc_port,
            server.grpc_port,
            std::process::id()
        );
        let startup_summary = format!(
            "novarocksd started (bind_host={}, advertise_host={}, advertise_port={}, heartbeat_port={}, be_port={}, brpc_port={}, http_port={}, grpc_port={}, starlet_port={})",
            server.host,
            advertise_endpoint.host,
            advertise_endpoint.port,
            server.heartbeat_port,
            server.be_port,
            server.brpc_port,
            server.http_port,
            server.grpc_port,
            server.starlet_port
        );
        let heartbeat_config = heartbeat_service::HeartbeatConfig {
            host: server.host.clone(),
            advertise_host: advertise_endpoint.host,
            heartbeat_port: server.heartbeat_port,
            be_port: server.be_port,
            brpc_port: server.brpc_port,
            http_port: server.http_port,
            starlet_port: advertise_endpoint.port,
            mem_limit_bytes: memory_limit,
        };
        let backend_config = backend_service::BackendServiceConfig {
            host: server.host.clone(),
            be_port: server.be_port,
        };
        let load_registry = Arc::new(CompatLoadRegistry::default());
        let tracking = Arc::new(LoadTrackingStore::default());
        let report_service = new_report_service_with_tracking(Arc::clone(&tracking));
        let control = Arc::new(FrontendControlState::new());
        let frontend_rpc_manager = frontend_rpc::create_manager_with_control(Arc::clone(&control));
        let disk_report_worker = Arc::new(DiskReportWorker::new(
            Arc::clone(&control),
            Arc::new(CompatDiskReportSender),
        ));
        let starlet_metadata_adapter = crate::starlet_metadata::starlet_metadata_adapter();
        let starlet_metadata_provider: Arc<
            dyn novarocks::connector::starrocks::ports::StarletMetadataProvider,
        > = starlet_metadata_adapter.clone();
        let storage_metadata_provider = crate::storage_wire::storage_metadata_provider();
        let lake_storage_service = Arc::new(CompatLakeStorageService::new(
            LakeStorageDependencies::with_providers(
                Arc::clone(&starlet_metadata_provider),
                Arc::clone(&storage_metadata_provider),
            ),
        ));
        let fragment_service = Arc::new(CompatFragmentService::new_with_connector_dependencies(
            novarocks::runtime::starrocks_fragment_query::StarRocksFragmentQueryRuntime::new(),
            brpc_exchange_transmitter(),
            brpc_fragment_lookup_client(),
            compat_result_writer(),
            Arc::clone(&report_service),
            Arc::clone(&load_registry),
            lake_meta_storage_resolver(
                Arc::clone(&starlet_metadata_provider),
                Arc::clone(&storage_metadata_provider),
            ),
            Some(crate::frontend_rpc::table_schema_provider()),
            Some(crate::schema_provider::schema_load_provider()),
            Some(crate::sink_frontend::sink_frontend_provider()),
            Some(Arc::clone(&starlet_metadata_provider)),
            Some(Arc::clone(&storage_metadata_provider)),
        ));
        let fragment_sync_executor: Arc<dyn SyncFragmentExecutor> = fragment_service.clone();
        let load_service = Arc::new(CompatLoadService::new(
            load_registry,
            fragment_sync_executor,
        ));
        let compat_routes = load_router(Arc::clone(&load_service), Arc::clone(&tracking));
        fragment_service
            .compose_connector_bindings(config.connector.object_store_config().map_err(
                |error| {
                    CompatApplicationError::new(
                        CompatApplicationErrorKind::Configuration,
                        format!("resolve connector startup object-store binding: {error}"),
                    )
                },
            )?)
            .map_err(|error| {
                CompatApplicationError::new(
                    CompatApplicationErrorKind::Configuration,
                    format!("compose connector instance bindings: {error}"),
                )
            })?;
        let brpc_config = brpc::CompatConfig {
            host: &server.host,
            heartbeat_port: server.heartbeat_port,
            brpc_port: server.brpc_port,
            internal_service_query_rpc_thread_num: query_threads,
            debug_exec_batch_plan_json: config.debug.exec_batch_plan_json,
            log_level,
            fragment_service_context: Arc::as_ptr(&fragment_service).cast(),
            lake_service_context: Arc::as_ptr(&lake_storage_service).cast(),
        };

        let mut host = Self {
            ports: Box::new(ports),
            started: StartedResources::default(),
            fragment_service,
            report_service,
            load_service,
            lake_storage_service,
            control: Arc::clone(&control),
            disk_report_worker: Arc::clone(&disk_report_worker),
            tracking,
            ready_marker,
            startup_summary,
            #[cfg(test)]
            _test_lifecycle_lock: test_lifecycle_lock,
        };
        if let Err(error) = frontend_rpc::install(frontend_rpc_manager) {
            return Err(host.start_failure(CompatApplicationErrorKind::Configuration, error));
        }
        host.started.frontend_rpc = true;
        if let Err(error) = host.report_service.start() {
            return Err(host.start_failure(
                CompatApplicationErrorKind::ReportStart,
                format!("start compat report service: {error}"),
            ));
        }
        host.started.report = true;
        if let Err(error) = host.ports.start_grpc(
            CompatListenerConfig {
                host: server.host.clone(),
                http_port: server.http_port,
                grpc_port: server.grpc_port,
                starlet_port: server.starlet_port,
            },
            compat_routes,
            starlet_metadata_adapter,
        ) {
            return Err(host.start_failure(
                CompatApplicationErrorKind::GrpcStart,
                format!("start grpc/http/starlet listeners: {error}"),
            ));
        }
        host.started.grpc = true;
        if let Err(error) = host.ports.start_heartbeat(
            heartbeat_config,
            Arc::clone(&control),
            Arc::clone(&disk_report_worker),
        ) {
            return Err(host.start_failure(
                CompatApplicationErrorKind::HeartbeatStart,
                format!("start heartbeat listener: {error}"),
            ));
        }
        host.started.heartbeat = true;
        let lake_agent_task_adapter =
            crate::lake_agent_tasks::lake_agent_task_adapter(storage_metadata_provider);
        if let Err(error) = host.ports.start_backend(
            backend_config,
            Arc::clone(&control),
            Arc::clone(&host.load_service),
            lake_agent_task_adapter,
        ) {
            return Err(host.start_failure(
                CompatApplicationErrorKind::BackendServiceStart,
                format!("start backend listener: {error}"),
            ));
        }
        host.started.backend = true;
        if let Err(error) = host.ports.start_brpc(&brpc_config) {
            return Err(host.start_failure(
                CompatApplicationErrorKind::BrpcStart,
                format!("start brpc listener: {error}"),
            ));
        }
        host.started.brpc = true;
        Ok(host)
    }

    fn start_failure(
        mut self,
        kind: CompatApplicationErrorKind,
        error: impl fmt::Display,
    ) -> CompatApplicationError {
        let primary = CompatApplicationError::new(kind, error);
        match self.cleanup_started() {
            Ok(()) => primary,
            Err(cleanup) => primary.with_cleanup_context(cleanup),
        }
    }

    fn cleanup_started(&mut self) -> Result<(), String> {
        let mut failures = Vec::new();
        if self.started.brpc {
            self.ports.stop_brpc();
            self.started.brpc = false;
        }
        self.load_service.begin_shutdown();
        if self.started.backend {
            if let Err(error) = self.ports.stop_backend() {
                failures.push(format!("stop backend service failed: {error}"));
            }
            self.started.backend = false;
        }
        if self.started.heartbeat {
            if let Err(error) = self.ports.stop_heartbeat() {
                failures.push(format!("stop heartbeat service failed: {error}"));
            }
            self.started.heartbeat = false;
        }
        if let Err(error) = self.disk_report_worker.shutdown() {
            failures.push(format!("stop disk report worker failed: {error}"));
        }
        if self.started.grpc {
            if let Err(error) = self.ports.stop_grpc() {
                failures.push(format!("stop grpc server failed: {error}"));
            }
            self.started.grpc = false;
        }
        self.load_service.finish_shutdown();
        self.tracking.clear();
        if self.started.report {
            self.report_service.stop();
            self.started.report = false;
        }
        if self.started.frontend_rpc {
            if let Err(error) = frontend_rpc::clear() {
                failures.push(format!("clear frontend RPC manager failed: {error}"));
            }
            self.started.frontend_rpc = false;
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(failures.join("; "))
        }
    }
}

pub fn run_compat_server_until_shutdown<F>(
    config: CompatServerConfig,
    shutdown_requested: F,
) -> Result<(), CompatApplicationError>
where
    F: FnMut() -> bool,
{
    run_compat_server_until_shutdown_with_ports(
        config,
        shutdown_requested,
        LiveCompatPorts::default(),
    )
}

fn run_compat_server_until_shutdown_with_ports<F>(
    config: CompatServerConfig,
    mut shutdown_requested: F,
    ports: impl CompatPorts + 'static,
) -> Result<(), CompatApplicationError>
where
    F: FnMut() -> bool,
{
    let mut host = CompatApplicationHost::open_with_ports(config, ports)?;
    println!("{}", host.ready_marker());
    println!("{}", host.startup_summary());

    let mut primary = Ok(());
    while !shutdown_requested() {
        match host.poll_failure() {
            Ok(Some(error)) | Err(error) => {
                primary = Err(error);
                break;
            }
            Ok(None) => {}
        }
        thread::sleep(SUPERVISION_POLL_INTERVAL);
    }
    if primary.is_ok() {
        match host.poll_failure() {
            Ok(Some(error)) | Err(error) => primary = Err(error),
            Ok(None) => {}
        }
    }
    combine_primary_and_shutdown(primary, host.shutdown())
}

fn combine_primary_and_shutdown(
    primary: Result<(), CompatApplicationError>,
    shutdown: Result<(), CompatApplicationError>,
) -> Result<(), CompatApplicationError> {
    match (primary, shutdown) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(primary), Ok(())) => Err(primary),
        (Ok(()), Err(shutdown)) => Err(shutdown),
        (Err(primary), Err(shutdown)) => Err(primary.with_cleanup_context(shutdown)),
    }
}

fn compat_log_level(level: &str) -> u8 {
    match level {
        "warn" => 1,
        "error" => 2,
        _ => 0,
    }
}

trait CompatPorts: Send {
    fn start_grpc(
        &mut self,
        config: CompatListenerConfig,
        compat_routes: Router,
        starlet_control: Arc<dyn listeners::StarletControl>,
    ) -> Result<(), String>;
    fn start_heartbeat(
        &mut self,
        config: heartbeat_service::HeartbeatConfig,
        control: Arc<FrontendControlState>,
        disk_report_worker: Arc<DiskReportWorker>,
    ) -> Result<(), String>;
    fn start_backend(
        &mut self,
        config: backend_service::BackendServiceConfig,
        control: Arc<FrontendControlState>,
        load_service: Arc<CompatLoadService>,
        lake_agent_task_adapter: Arc<crate::lake_agent_tasks::CompatLakeAgentTaskAdapter>,
    ) -> Result<(), String>;
    fn start_brpc(&mut self, config: &brpc::CompatConfig<'_>) -> Result<(), String>;
    fn poll_heartbeat_failure(&mut self) -> Result<Option<String>, String>;
    fn poll_backend_failure(&mut self) -> Result<Option<String>, String>;
    fn poll_grpc_failure(&mut self) -> Result<Option<String>, String>;
    fn stop_brpc(&mut self);
    fn stop_backend(&mut self) -> Result<(), String>;
    fn stop_heartbeat(&mut self) -> Result<(), String>;
    fn stop_grpc(&mut self) -> Result<(), String>;
}

#[derive(Default)]
struct LiveCompatPorts {
    heartbeat: Option<HeartbeatServer>,
    backend: Option<BackendServiceHandle>,
    listeners: Option<CompatListenerGroup>,
}

impl CompatPorts for LiveCompatPorts {
    fn start_grpc(
        &mut self,
        config: CompatListenerConfig,
        compat_routes: Router,
        starlet_control: Arc<dyn listeners::StarletControl>,
    ) -> Result<(), String> {
        self.listeners = Some(CompatListenerGroup::start(
            config,
            compat_routes,
            starlet_control,
        )?);
        Ok(())
    }

    fn start_heartbeat(
        &mut self,
        config: heartbeat_service::HeartbeatConfig,
        control: Arc<FrontendControlState>,
        disk_report_worker: Arc<DiskReportWorker>,
    ) -> Result<(), String> {
        self.heartbeat = Some(HeartbeatServer::start(config, control, disk_report_worker)?);
        Ok(())
    }

    fn start_backend(
        &mut self,
        config: backend_service::BackendServiceConfig,
        control: Arc<FrontendControlState>,
        load_service: Arc<CompatLoadService>,
        lake_agent_task_adapter: Arc<crate::lake_agent_tasks::CompatLakeAgentTaskAdapter>,
    ) -> Result<(), String> {
        self.backend = Some(backend_service::start_backend_service(
            config,
            control,
            load_service,
            lake_agent_task_adapter,
        )?);
        Ok(())
    }

    fn start_brpc(&mut self, config: &brpc::CompatConfig<'_>) -> Result<(), String> {
        brpc::start(config).map_err(|error| error.to_string())
    }

    fn poll_heartbeat_failure(&mut self) -> Result<Option<String>, String> {
        self.heartbeat
            .as_mut()
            .map_or(Ok(None), HeartbeatServer::poll_failure)
    }

    fn poll_backend_failure(&mut self) -> Result<Option<String>, String> {
        self.backend
            .as_ref()
            .map_or(Ok(None), BackendServiceHandle::poll_failure)
    }

    fn poll_grpc_failure(&mut self) -> Result<Option<String>, String> {
        self.listeners
            .as_ref()
            .map_or(Ok(None), CompatListenerGroup::poll_failure)
    }

    fn stop_brpc(&mut self) {
        brpc::stop();
    }

    fn stop_backend(&mut self) -> Result<(), String> {
        self.backend.take().map_or(Ok(()), |handle| handle.stop())
    }

    fn stop_heartbeat(&mut self) -> Result<(), String> {
        self.heartbeat
            .take()
            .map_or(Ok(()), |mut server| server.stop())
    }

    fn stop_grpc(&mut self) -> Result<(), String> {
        self.listeners
            .take()
            .map_or(Ok(()), |listeners| listeners.stop())
    }
}

#[cfg(test)]
#[path = "../tests/application_host.rs"]
mod tests;
